use diesel::{
    BoolExpressionMethods, ExpressionMethods, IntoSql, OptionalExtension, QueryDsl, sql_types::Bool,
};
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use validator::Validate;
use warp::{Rejection, Reply};

use super::{
    MAX_SELECTED_QUERY_TAGS, QueryParametersFilter,
    compiler::{self, dict::Scope},
    prepare_query_parameters,
};
use crate::{
    acquire_db_connection,
    error::Error,
    model::{
        NewStoredConstriction, NewStoredQuery, StoredConstriction, StoredQuery, StoredQueryScope,
        User,
    },
    run_retryable_transaction, run_serializable_transaction,
    schema::{stored_constriction, stored_query},
    util::{NOT_BLANK_REGEX, string_value_updated},
};

const RECENT_QUERY_LIMIT: i64 = 10;
const MAX_SAVED_QUERIES: i64 = 100;
const MAX_PERSONAL_CONSTRICTIONS_PER_SCOPE: i64 = 10;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum StoredQueryDefinition {
    Simple {
        text: String,
        tags: Vec<String>,
        sort: Option<QuerySort>,
    },
    Advanced {
        query: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuerySort {
    pub attribute: String,
    pub descending: bool,
}

#[derive(Deserialize)]
pub struct GetStoredQueriesFilter {
    pub scope: Option<StoredQueryScope>,
}

#[derive(Serialize)]
pub struct GetStoredQueriesResponse {
    pub saved_queries: Vec<StoredQuery>,
    pub recent_queries: Vec<StoredQuery>,
}

pub async fn get_stored_queries_handler(
    user: User,
    filter: GetStoredQueriesFilter,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;

    let mut saved_query = stored_query::table
        .filter(stored_query::fk_user.eq(user.pk))
        .filter(stored_query::is_saved.eq(true))
        .into_boxed();

    if let Some(scope) = filter.scope {
        saved_query = saved_query.filter(stored_query::scope.eq(scope));
    }

    let saved_queries = saved_query
        .order_by((
            stored_query::creation_timestamp.desc(),
            stored_query::pk.desc(),
        ))
        .load::<StoredQuery>(&mut connection)
        .await
        .map_err(Error::from)?;

    let mut recent_query = stored_query::table
        .filter(stored_query::fk_user.eq(user.pk))
        .filter(stored_query::is_saved.eq(false))
        .into_boxed();

    if let Some(scope) = filter.scope {
        recent_query = recent_query.filter(stored_query::scope.eq(scope));
    }

    let recent_queries = recent_query
        .order_by((
            stored_query::last_used_timestamp.desc(),
            stored_query::pk.desc(),
        ))
        .load::<StoredQuery>(&mut connection)
        .await
        .map_err(Error::from)?;

    Ok(warp::reply::json(&GetStoredQueriesResponse {
        saved_queries,
        recent_queries,
    }))
}

#[derive(Deserialize)]
pub struct RecordRecentQueryRequest {
    pub scope: StoredQueryScope,
    pub query_definition: StoredQueryDefinition,
}

pub async fn record_recent_query_handler(
    request: RecordRecentQueryRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    let query_definition =
        serialize_query_definition(&request.query_definition).map_err(warp::reject::custom)?;

    let mut connection = acquire_db_connection().await?;
    let stored_query = run_serializable_transaction(&mut connection, async |connection| {
        let stored_query = diesel::insert_into(stored_query::table)
            .values(&NewStoredQuery {
                fk_user: user.pk,
                scope: request.scope,
                query_definition: query_definition.clone(),
                is_saved: false,
                name: None,
            })
            .on_conflict(diesel::dsl::sql::<diesel::sql_types::Bool>(
                "(fk_user, scope, (md5(query_definition::text))) WHERE NOT is_saved",
            ))
            .do_update()
            .set(stored_query::last_used_timestamp.eq(diesel::dsl::now))
            .get_result::<StoredQuery>(connection)
            .await?;

        let obsolete_pks = stored_query::table
            .filter(stored_query::fk_user.eq(user.pk))
            .filter(stored_query::scope.eq(request.scope))
            .filter(stored_query::is_saved.eq(false))
            .order_by((
                stored_query::last_used_timestamp.desc(),
                stored_query::pk.desc(),
            ))
            .offset(RECENT_QUERY_LIMIT)
            .select(stored_query::pk)
            .load::<i64>(connection)
            .await?;

        if !obsolete_pks.is_empty() {
            diesel::delete(stored_query::table)
                .filter(stored_query::pk.eq_any(obsolete_pks))
                .execute(connection)
                .await?;
        }

        Ok(stored_query)
    })
    .await?;

    Ok(warp::reply::json(&stored_query))
}

#[derive(Deserialize, Validate)]
pub struct SaveQueryRequest {
    pub scope: StoredQueryScope,
    pub query_definition: StoredQueryDefinition,
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub name: String,
}

pub async fn save_query_handler(
    request: SaveQueryRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for SaveQueryRequest: {e}"
        )))
    })?;

    let query_definition =
        serialize_query_definition(&request.query_definition).map_err(warp::reject::custom)?;

    validate_advanced_query(&request.query_definition, request.scope, &user)
        .map_err(warp::reject::custom)?;

    let mut connection = acquire_db_connection().await?;
    let stored_query = run_serializable_transaction(&mut connection, async |connection| {
        let stored_query = diesel::insert_into(stored_query::table)
            .values(&NewStoredQuery {
                fk_user: user.pk,
                scope: request.scope,
                query_definition: query_definition.clone(),
                is_saved: true,
                name: Some(request.name.clone()),
            })
            .get_result::<StoredQuery>(connection)
            .await?;

        let count = stored_query::table
            .filter(stored_query::fk_user.eq(user.pk))
            .filter(stored_query::is_saved.eq(true))
            .count()
            .get_result::<i64>(connection)
            .await?;

        if count > MAX_SAVED_QUERIES {
            return Err(Error::TooManyResultsError(count as u32, MAX_SAVED_QUERIES as u32).into());
        }

        Ok(stored_query)
    })
    .await?;

    Ok(warp::reply::json(&stored_query))
}

#[derive(Deserialize, Validate)]
pub struct EditSavedQueryRequest {
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub name: Option<String>,
    pub query_definition: Option<StoredQueryDefinition>,
}

#[derive(AsChangeset)]
#[diesel(table_name = stored_query)]
pub struct StoredQueryUpdateOptional {
    pub name: Option<String>,
    pub query_definition: Option<serde_json::Value>,
}

pub struct StoredQueryUpdateFieldChanges {
    pub name_changed: bool,
    pub query_definition_changed: bool,
}

impl StoredQueryUpdateFieldChanges {
    pub fn has_changes(&self) -> bool {
        self.name_changed || self.query_definition_changed
    }
}

impl StoredQueryUpdateOptional {
    pub fn get_field_changes(&self, curr_value: &StoredQuery) -> StoredQueryUpdateFieldChanges {
        StoredQueryUpdateFieldChanges {
            name_changed: string_value_updated(curr_value.name.as_deref(), self.name.as_deref()),
            query_definition_changed: self
                .query_definition
                .as_ref()
                .map(|v| v != &curr_value.query_definition)
                .unwrap_or(false),
        }
    }

    pub fn has_changes(&self, curr_value: &StoredQuery) -> bool {
        self.get_field_changes(curr_value).has_changes()
    }
}

pub async fn edit_saved_query_handler(
    pk: i64,
    request: EditSavedQueryRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for EditSavedQueryRequest: {e}"
        )))
    })?;

    let query_definition = request
        .query_definition
        .as_ref()
        .map(serialize_query_definition)
        .transpose()
        .map_err(warp::reject::custom)?;

    let mut connection = acquire_db_connection().await?;

    let stored_query = run_retryable_transaction(&mut connection, async |connection| {
        let curr_value = stored_query::table
            .filter(stored_query::pk.eq(pk))
            .filter(stored_query::fk_user.eq(user.pk))
            .filter(stored_query::is_saved.eq(true))
            .for_update()
            .get_result::<StoredQuery>(connection)
            .await
            .optional()?
            .ok_or(Error::NotFoundError)?;

        let update = StoredQueryUpdateOptional {
            name: request.name.clone(),
            query_definition: query_definition.clone(),
        };

        let field_changes = update.get_field_changes(&curr_value);

        if !field_changes.has_changes() {
            return Ok(curr_value);
        }

        if field_changes.query_definition_changed
            && let Some(ref definition) = request.query_definition
        {
            validate_advanced_query(definition, curr_value.scope, &user)?;
        }

        let updated = diesel::update(stored_query::table)
            .filter(stored_query::pk.eq(pk))
            .filter(stored_query::fk_user.eq(user.pk))
            .filter(stored_query::is_saved.eq(true))
            .set(update)
            .get_result::<StoredQuery>(connection)
            .await?;

        Ok(updated)
    })
    .await?;

    Ok(warp::reply::json(&stored_query))
}

pub async fn delete_stored_query_handler(pk: i64, user: User) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;

    let deleted = diesel::delete(stored_query::table)
        .filter(stored_query::pk.eq(pk))
        .filter(stored_query::fk_user.eq(user.pk))
        .execute(&mut connection)
        .await
        .map_err(Error::from)?;

    if deleted == 0 {
        return Err(warp::reject::custom(Error::NotFoundError));
    }

    Ok(warp::reply::reply())
}

fn serialize_query_definition(
    definition: &StoredQueryDefinition,
) -> Result<serde_json::Value, Error> {
    match definition {
        StoredQueryDefinition::Advanced { query } => {
            if query.chars().count() > 1024 {
                return Err(Error::InvalidRequestInputError(String::from(
                    "Query exceeds maximum length of 1024 characters",
                )));
            }
        }
        StoredQueryDefinition::Simple { text, tags, sort } => {
            if text.chars().count() > 1024 {
                return Err(Error::InvalidRequestInputError(String::from(
                    "Search text exceeds maximum length of 1024 characters",
                )));
            }

            if tags.len() > MAX_SELECTED_QUERY_TAGS {
                return Err(Error::InvalidRequestInputError(format!(
                    "Cannot select more than {MAX_SELECTED_QUERY_TAGS} tags"
                )));
            }

            if tags.iter().any(|tag| !crate::tag::tag_is_valid(tag)) {
                return Err(Error::InvalidRequestInputError(String::from(
                    "Invalid selected tags",
                )));
            }

            if sort.as_ref().is_some_and(|sort| {
                sort.attribute.trim().is_empty() || sort.attribute.chars().count() > 255
            }) {
                return Err(Error::InvalidRequestInputError(String::from(
                    "Invalid sort attribute",
                )));
            }
        }
    }

    serde_json::to_value(definition).map_err(|e| Error::SerialisationError(e.to_string()))
}

fn validate_advanced_query(
    definition: &StoredQueryDefinition,
    stored_scope: StoredQueryScope,
    user: &User,
) -> Result<(), Error> {
    let StoredQueryDefinition::Advanced { query } = definition else {
        return Ok(());
    };

    let scope = stored_scope_to_compiler_scope(stored_scope);
    let user = Some(user.clone());
    let query_parameters =
        prepare_query_parameters(&QueryParametersFilter::default(), &user, &scope)?;

    compiler::compile_sql(Some(query.clone()), query_parameters, &scope, &user)?;

    Ok(())
}

fn stored_scope_to_compiler_scope(stored_scope: StoredQueryScope) -> Scope {
    match stored_scope {
        StoredQueryScope::Post => Scope::Post,
        StoredQueryScope::Collection => Scope::Collection,
        StoredQueryScope::CollectionItem => Scope::CollectionItem { collection_pk: -1 },
        StoredQueryScope::UserGroup => Scope::UserGroup,
    }
}

#[derive(Deserialize)]
pub struct GetStoredConstrictionsFilter {
    pub scope: Option<StoredQueryScope>,
}

pub async fn get_stored_constrictions_handler(
    user: Option<User>,
    filter: GetStoredConstrictionsFilter,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;

    let mut query = stored_constriction::table.into_boxed();

    query = if let Some(user) = user {
        query.filter(
            stored_constriction::fk_user
                .eq(user.pk)
                .or(stored_constriction::fk_user.is_null()),
        )
    } else {
        query.filter(stored_constriction::fk_user.is_null())
    };

    if let Some(scope) = filter.scope {
        query = query.filter(stored_constriction::scope.eq(scope));
    }

    let constrictions = query
        .order_by((
            stored_constriction::scope.asc(),
            stored_constriction::name.asc(),
            stored_constriction::pk.asc(),
        ))
        .load::<StoredConstriction>(&mut connection)
        .await
        .map_err(Error::from)?;

    Ok(warp::reply::json(&constrictions))
}

#[derive(Deserialize, Validate)]
pub struct CreateStoredConstrictionRequest {
    pub scope: StoredQueryScope,
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub name: String,
    #[validate(length(min = 1, max = 1024), regex(path = *NOT_BLANK_REGEX))]
    pub condition: String,
    #[serde(default)]
    pub is_global: bool,
}

pub async fn create_stored_constriction_handler(
    request: CreateStoredConstrictionRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreateStoredConstrictionRequest: {e}"
        )))
    })?;

    if request.is_global && !user.is_admin {
        return Err(warp::reject::custom(Error::UserNotAdmin));
    }

    validate_stored_constriction_condition(&request.condition, request.scope, &user)
        .map_err(warp::reject::custom)?;

    let mut connection = acquire_db_connection().await?;
    let stored_constriction = run_serializable_transaction(&mut connection, async |connection| {
        let stored_constriction = diesel::insert_into(stored_constriction::table)
            .values(&NewStoredConstriction {
                fk_user: if request.is_global {
                    None
                } else {
                    Some(user.pk)
                },
                scope: request.scope,
                name: request.name.clone(),
                condition: request.condition.clone(),
            })
            .get_result::<StoredConstriction>(connection)
            .await?;

        if !request.is_global {
            let count = stored_constriction::table
                .filter(stored_constriction::fk_user.eq(user.pk))
                .filter(stored_constriction::scope.eq(request.scope))
                .count()
                .get_result::<i64>(connection)
                .await?;

            if count > MAX_PERSONAL_CONSTRICTIONS_PER_SCOPE {
                return Err(Error::TooManyResultsError(
                    count as u32,
                    MAX_PERSONAL_CONSTRICTIONS_PER_SCOPE as u32,
                )
                .into());
            }
        }

        Ok(stored_constriction)
    })
    .await?;

    Ok(warp::reply::json(&stored_constriction))
}

#[derive(Deserialize, Validate)]
pub struct EditStoredConstrictionRequest {
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub name: Option<String>,
    #[validate(length(min = 1, max = 1024), regex(path = *NOT_BLANK_REGEX))]
    pub condition: Option<String>,
}

#[derive(AsChangeset)]
#[diesel(table_name = stored_constriction)]
pub struct StoredConstrictionUpdateOptional {
    pub name: Option<String>,
    pub condition: Option<String>,
}

pub struct StoredConstrictionUpdateFieldChanges {
    pub name_changed: bool,
    pub condition_changed: bool,
}

impl StoredConstrictionUpdateFieldChanges {
    pub fn has_changes(&self) -> bool {
        self.name_changed || self.condition_changed
    }
}

impl StoredConstrictionUpdateOptional {
    pub fn get_field_changes(
        &self,
        curr_value: &StoredConstriction,
    ) -> StoredConstrictionUpdateFieldChanges {
        StoredConstrictionUpdateFieldChanges {
            name_changed: string_value_updated(Some(&curr_value.name), self.name.as_deref()),
            condition_changed: string_value_updated(
                Some(&curr_value.condition),
                self.condition.as_deref(),
            ),
        }
    }

    pub fn has_changes(&self, curr_value: &StoredConstriction) -> bool {
        self.get_field_changes(curr_value).has_changes()
    }
}

pub async fn edit_stored_constriction_handler(
    pk: i64,
    request: EditStoredConstrictionRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for EditStoredConstrictionRequest: {e}"
        )))
    })?;

    let mut connection = acquire_db_connection().await?;
    let stored_constriction = run_retryable_transaction(&mut connection, async |connection| {
        let curr_value = stored_constriction::table
            .filter(stored_constriction::pk.eq(pk))
            .filter(
                stored_constriction::fk_user
                    .eq(user.pk)
                    .or(stored_constriction::fk_user
                        .is_null()
                        .and(user.is_admin.into_sql::<Bool>())),
            )
            .for_update()
            .get_result::<StoredConstriction>(connection)
            .await
            .optional()?
            .ok_or(Error::NotFoundError)?;

        let update = StoredConstrictionUpdateOptional {
            name: request.name.clone(),
            condition: request.condition.clone(),
        };

        let field_changes = update.get_field_changes(&curr_value);

        if !field_changes.has_changes() {
            return Ok(curr_value);
        }

        if field_changes.condition_changed
            && let Some(ref condition) = request.condition
        {
            validate_stored_constriction_condition(condition, curr_value.scope, &user)?;
        }

        let updated = diesel::update(stored_constriction::table)
            .filter(stored_constriction::pk.eq(pk))
            .set(update)
            .get_result::<StoredConstriction>(connection)
            .await?;

        Ok(updated)
    })
    .await?;

    Ok(warp::reply::json(&stored_constriction))
}

pub async fn delete_stored_constriction_handler(
    pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;

    let deleted = diesel::delete(stored_constriction::table)
        .filter(stored_constriction::pk.eq(pk))
        .filter(
            stored_constriction::fk_user
                .eq(user.pk)
                .or(stored_constriction::fk_user
                    .is_null()
                    .and(user.is_admin.into_sql::<Bool>())),
        )
        .execute(&mut connection)
        .await
        .map_err(Error::from)?;

    if deleted == 0 {
        return Err(warp::reject::custom(Error::NotFoundError));
    }

    Ok(warp::reply::reply())
}

fn validate_stored_constriction_condition(
    condition: &str,
    stored_scope: StoredQueryScope,
    user: &User,
) -> Result<(), Error> {
    let scope = stored_scope_to_compiler_scope(stored_scope);

    let mut log = compiler::Log::default();
    let ast = compiler::compile_ast(condition.to_string(), &mut log, true)?;

    if ast.node_type.statements.is_empty() {
        return Err(Error::InvalidRequestInputError(String::from(
            "Constriction must contain a boolean expression",
        )));
    }

    let user = Some(user.clone());

    let query_parameters = prepare_query_parameters(
        &QueryParametersFilter {
            constriction: Some(condition.to_string()),
            ..Default::default()
        },
        &user,
        &scope,
    )?;

    compiler::compile_sql(None, query_parameters, &scope, &user)?;

    Ok(())
}
