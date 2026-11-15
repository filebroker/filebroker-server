use diesel::{ExpressionMethods, OptionalExtension, QueryDsl};
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
    model::{NewStoredQuery, StoredQuery, StoredQueryScope, User},
    run_retryable_transaction, run_serializable_transaction,
    schema::stored_query,
    util::{NOT_BLANK_REGEX, string_value_updated},
};

const RECENT_QUERY_LIMIT: i64 = 10;
const MAX_SAVED_QUERIES: i64 = 100;

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

#[derive(Serialize)]
pub struct GetStoredQueriesResponse {
    pub saved_queries: Vec<StoredQuery>,
    pub recent_queries: Vec<StoredQuery>,
}

pub async fn get_stored_queries_handler(user: User) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;

    let saved_queries = stored_query::table
        .filter(stored_query::fk_user.eq(user.pk))
        .filter(stored_query::is_saved.eq(true))
        .order_by((
            stored_query::creation_timestamp.desc(),
            stored_query::pk.desc(),
        ))
        .load::<StoredQuery>(&mut connection)
        .await
        .map_err(Error::from)?;

    let recent_queries = stored_query::table
        .filter(stored_query::fk_user.eq(user.pk))
        .filter(stored_query::is_saved.eq(false))
        .order_by((
            stored_query::last_used_timestamp.desc(),
            stored_query::pk.desc(),
        ))
        .limit(RECENT_QUERY_LIMIT)
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

    let scope = match stored_scope {
        StoredQueryScope::Post => Scope::Post,
        StoredQueryScope::Collection => Scope::Collection,
        StoredQueryScope::CollectionItem => Scope::CollectionItem { collection_pk: -1 },
        StoredQueryScope::UserGroup => Scope::UserGroup,
    };

    let user = Some(user.clone());
    let query_parameters =
        prepare_query_parameters(&QueryParametersFilter::default(), &user, &scope)?;

    compiler::compile_sql(Some(query.clone()), query_parameters, &scope, &user)?;

    Ok(())
}
