use crate::diesel::OptionalExtension;
use crate::error::{Error, TransactionRuntimeError};
use crate::model::{
    ApplyAutoTagsTask, NewTagEditHistory, Tag, TagAlias, TagCategory, TagEdge, TagEditHistory,
    TagEditHistoryAlias, TagEditHistoryParent, User,
};
use crate::query::compiler::dict::Scope;
use crate::query::report_missing_pks;
use crate::schema::{
    tag, tag_alias, tag_category, tag_edge, tag_edit_history, tag_edit_history_alias,
    tag_edit_history_parent,
};
use crate::tag::auto_matching::{
    AutoMatchTarget, compile_auto_match_condition, compile_tag_auto_match_condition,
    create_apply_auto_tag_task, create_apply_tag_category_auto_tags_task,
    spawn_apply_auto_tags_task,
};
use crate::tag::{TagDetailed, get_tag_aliases, load_tag_detailed, validate_tag};
use crate::tag::{get_or_create_tag, get_tag_aliases_pks, get_tag_parents_pks, sanitize_tag};
use crate::util::{dedup_vec_optional, dedup_vecs_optional, string_value_updated};
use crate::{acquire_db_connection, retry_on_constraint_violation, run_serializable_transaction};
use chrono::{DateTime, Utc};
use diesel::{BelongingToDsl, BoolExpressionMethods, ExpressionMethods, QueryDsl};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};
use validator::Validate;
use warp::{Rejection, Reply};

#[derive(Deserialize, Validate)]
pub struct UpsertTagRequest {
    #[validate(custom(function = "validate_tag"))]
    pub tag_name: String,
    #[validate(length(min = 0, max = 25))]
    pub parent_pks: Option<Vec<i64>>,
    #[validate(length(min = 0, max = 25))]
    pub alias_pks: Option<Vec<i64>>,
    pub tag_category: Option<String>,
    #[validate(length(min = 0, max = 1000))]
    pub auto_match_condition_post: Option<String>,
    #[validate(length(min = 0, max = 1000))]
    pub auto_match_condition_collection: Option<String>,
}

#[derive(Serialize)]
pub struct UpsertTagResponse {
    pub inserted: bool,
    pub tag_detailed: TagDetailed,
}

/// Creates a tag with the given tag_name, parent and aliases. If the tag already exists, the existing tag is updated
/// and the given parents and aliases are added. Note that added aliases are removed from their pre-existing parent-child
/// hierarchy and added to the hierarchy of the given tag instead, setting all parents of the tag as the parents of all aliases
/// and setting all children of the tag as children of all aliases.
pub async fn upsert_tag_handler(
    mut upsert_tag_request: UpsertTagRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    upsert_tag_request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for UpsertTagRequest: {e}"
        )))
    })?;

    let tag_name = sanitize_tag(&upsert_tag_request.tag_name);

    if let Some(ref mut parent_pks) = upsert_tag_request.parent_pks {
        parent_pks.sort_unstable();
        parent_pks.dedup();
    }

    if let Some(ref mut alias_pks) = upsert_tag_request.alias_pks {
        alias_pks.sort_unstable();
        alias_pks.dedup();
    }

    let mut connection = acquire_db_connection().await?;

    if let Some(ref parent_pks) = upsert_tag_request.parent_pks {
        report_missing_pks!(tag, parent_pks, &mut connection)??;
    }

    if let Some(ref alias_pks) = upsert_tag_request.alias_pks {
        report_missing_pks!(tag, alias_pks, &mut connection)??;
    }

    let (inserted, tag_detailed, apply_auto_tags_task) =
        run_serializable_transaction(&mut connection, |connection| {
            async move {
                let (inserted, tag) = get_or_create_tag(&tag_name, &user, connection).await?;

                let (tag, _, apply_auto_tags_task) = if inserted {
                    if let Some(ref parent_pks) = upsert_tag_request.parent_pks {
                        if parent_pks.contains(&tag.pk) {
                            return Err(TransactionRuntimeError::Rollback(
                                Error::InvalidRequestInputError(format!(
                                    "Cannot set tag {} as its own parent",
                                    tag.pk
                                )),
                            ));
                        }

                        if !parent_pks.is_empty() {
                            if parent_pks.len() > 25 {
                                return Err(TransactionRuntimeError::Rollback(
                                    Error::BadRequestError(String::from(
                                        "Cannot set more than 25 parents",
                                    )),
                                ));
                            }
                            add_tag_parents(tag.pk, parent_pks, connection).await?;
                        }
                    }

                    if let Some(ref alias_pks) = upsert_tag_request.alias_pks {
                        if !alias_pks.is_empty() {
                            if alias_pks.contains(&tag.pk) {
                                return Err(TransactionRuntimeError::Rollback(
                                    Error::InvalidRequestInputError(format!(
                                        "Cannot set tag {} as an alias of itself",
                                        tag.pk
                                    )),
                                ));
                            }

                            if !alias_pks.is_empty() {
                                if alias_pks.len() > 25 {
                                    return Err(TransactionRuntimeError::Rollback(
                                        Error::BadRequestError(String::from(
                                            "Cannot set more than 25 aliases",
                                        )),
                                    ));
                                }

                                add_tag_aliases(&tag, alias_pks, connection).await?;
                            }
                        }
                    }

                    let tag_category = match upsert_tag_request.tag_category {
                        Some(tag_category) if !tag_category.is_empty() => {
                            let found_tag_category = tag_category::table
                                .filter(tag_category::id.eq(&tag_category))
                                .get_result::<TagCategory>(connection)
                                .await
                                .optional()?;
                            if let Some(found_tag_category) = found_tag_category {
                                Some(found_tag_category)
                            } else {
                                return Err(TransactionRuntimeError::Rollback(
                                    Error::InvalidEntityReferenceError(tag_category),
                                ));
                            }
                        }
                        _ => None,
                    };

                    let compiled_auto_match_condition_post = compile_auto_match_condition(
                        tag.tag_name.clone(),
                        upsert_tag_request.auto_match_condition_post.clone(),
                        tag_category
                            .as_ref()
                            .and_then(|tc| tc.auto_match_condition_post.clone()),
                        Scope::TagAutoMatchPost,
                    )?;
                    let compiled_auto_match_condition_collection = compile_auto_match_condition(
                        tag.tag_name.clone(),
                        upsert_tag_request.auto_match_condition_collection.clone(),
                        tag_category
                            .as_ref()
                            .and_then(|tc| tc.auto_match_condition_collection.clone()),
                        Scope::TagAutoMatchCollection,
                    )?;

                    let tag = diesel::update(tag::table)
                        .filter(tag::pk.eq(tag.pk))
                        .set((
                            tag::tag_category.eq(tag_category.as_ref().map(|tc| &tc.id)),
                            tag::auto_match_condition_post
                                .eq(&upsert_tag_request.auto_match_condition_post),
                            tag::auto_match_condition_collection
                                .eq(&upsert_tag_request.auto_match_condition_collection),
                            tag::compiled_auto_match_condition_post
                                .eq(&compiled_auto_match_condition_post),
                            tag::compiled_auto_match_condition_collection
                                .eq(&compiled_auto_match_condition_collection),
                        ))
                        .get_result::<Tag>(connection)
                        .await?;

                    let apply_auto_tag_task = if tag.compiled_auto_match_condition_post.is_some()
                        || tag.compiled_auto_match_condition_collection.is_some()
                    {
                        Some(create_apply_auto_tag_task(tag.pk, connection).await?)
                    } else {
                        None
                    };

                    (tag, tag_category, apply_auto_tag_task)
                } else {
                    update_tag(
                        tag.pk,
                        &user,
                        upsert_tag_request.parent_pks,
                        None,
                        None,
                        upsert_tag_request.alias_pks,
                        None,
                        None,
                        upsert_tag_request.tag_category,
                        upsert_tag_request.auto_match_condition_post,
                        upsert_tag_request.auto_match_condition_collection,
                        connection,
                    )
                    .await?
                };

                let tag_detailed = load_tag_detailed(tag, connection).await?;

                Ok((inserted, tag_detailed, apply_auto_tags_task))
            }
            .scope_boxed()
        })
        .await?;

    if let Some(apply_auto_tags_task) = apply_auto_tags_task {
        spawn_apply_auto_tags_task(apply_auto_tags_task);
    }

    Ok(warp::reply::json(&UpsertTagResponse {
        inserted,
        tag_detailed,
    }))
}

#[derive(Deserialize, Validate)]
pub struct UpdateTagRequest {
    #[validate(length(min = 0, max = 25))]
    pub added_parent_pks: Option<Vec<i64>>,
    #[validate(length(min = 0, max = 25))]
    pub parent_pks_overwrite: Option<Vec<i64>>,
    pub removed_parent_pks: Option<Vec<i64>>,
    #[validate(length(min = 0, max = 25))]
    pub added_alias_pks: Option<Vec<i64>>,
    #[validate(length(min = 0, max = 25))]
    pub alias_pks_overwrite: Option<Vec<i64>>,
    pub removed_alias_pks: Option<Vec<i64>>,
    pub tag_category: Option<String>,
    #[validate(length(min = 0, max = 1000))]
    pub auto_match_condition_post: Option<String>,
    #[validate(length(min = 0, max = 1000))]
    pub auto_match_condition_collection: Option<String>,
}

#[derive(AsChangeset)]
#[diesel(table_name = tag)]
pub struct TagUpdateOptional {
    pub tag_category: Option<String>,
    pub auto_match_condition_post: Option<String>,
    pub auto_match_condition_collection: Option<String>,
    pub compiled_auto_match_condition_post: Option<String>,
    pub compiled_auto_match_condition_collection: Option<String>,
    pub edit_timestamp: DateTime<Utc>,
    pub fk_edit_user: i64,
}

impl TagUpdateOptional {
    pub fn get_field_changes(&self, curr_value: &Tag) -> TagUpdateFieldChanges {
        TagUpdateFieldChanges {
            tag_category_changed: string_value_updated(
                curr_value.tag_category.as_deref(),
                self.tag_category.as_deref(),
            ),
            auto_match_condition_post_changed: string_value_updated(
                curr_value.auto_match_condition_post.as_deref(),
                self.auto_match_condition_post.as_deref(),
            ),
            auto_match_condition_collection_changed: string_value_updated(
                curr_value.auto_match_condition_collection.as_deref(),
                self.auto_match_condition_collection.as_deref(),
            ),
        }
    }
}

pub struct TagUpdateFieldChanges {
    pub tag_category_changed: bool,
    pub auto_match_condition_post_changed: bool,
    pub auto_match_condition_collection_changed: bool,
}

impl TagUpdateFieldChanges {
    pub fn has_changes(&self) -> bool {
        self.tag_category_changed
            || self.auto_match_condition_post_changed
            || self.auto_match_condition_collection_changed
    }
}

pub async fn update_tag_handler(
    tag_pk: i64,
    mut request: UpdateTagRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for UpdateTagRequest: {e}"
        )))
    })?;

    if (request.auto_match_condition_post.is_some()
        || request.auto_match_condition_collection.is_some())
        && !user.is_admin
    {
        return Err(warp::reject::custom(Error::UserNotAdmin));
    }

    dedup_vec_optional(&mut request.added_parent_pks);
    dedup_vec_optional(&mut request.parent_pks_overwrite);
    dedup_vec_optional(&mut request.removed_parent_pks);
    dedup_vec_optional(&mut request.added_alias_pks);
    dedup_vec_optional(&mut request.alias_pks_overwrite);
    dedup_vec_optional(&mut request.removed_alias_pks);
    dedup_vecs_optional(&mut request.added_parent_pks, &request.parent_pks_overwrite);
    dedup_vecs_optional(&mut request.added_alias_pks, &request.alias_pks_overwrite);

    let mut connection = acquire_db_connection().await?;
    let (tag_joined, apply_auto_tags_task) =
        run_serializable_transaction(&mut connection, |connection| {
            async move {
                let (tag, _, apply_auto_tags_task) = update_tag(
                    tag_pk,
                    &user,
                    request.added_parent_pks,
                    request.parent_pks_overwrite,
                    request.removed_parent_pks,
                    request.added_alias_pks,
                    request.alias_pks_overwrite,
                    request.removed_alias_pks,
                    request.tag_category,
                    request.auto_match_condition_post,
                    request.auto_match_condition_collection,
                    connection,
                )
                .await?;

                Ok((
                    load_tag_detailed(tag, connection).await?,
                    apply_auto_tags_task,
                ))
            }
            .scope_boxed()
        })
        .await?;

    if let Some(apply_auto_tags_task) = apply_auto_tags_task {
        spawn_apply_auto_tags_task(apply_auto_tags_task);
    }

    Ok(warp::reply::json(&tag_joined))
}

#[allow(clippy::too_many_arguments)]
pub async fn update_tag(
    tag_pk: i64,
    user: &User,
    mut added_parent_pks: Option<Vec<i64>>,
    mut parent_pks_overwrite: Option<Vec<i64>>,
    removed_parent_pks: Option<Vec<i64>>,
    mut added_alias_pks: Option<Vec<i64>>,
    mut alias_pks_overwrite: Option<Vec<i64>>,
    mut removed_alias_pks: Option<Vec<i64>>,
    tag_category: Option<String>,
    auto_match_condition_post: Option<String>,
    auto_match_condition_collection: Option<String>,
    connection: &mut AsyncPgConnection,
) -> Result<(Tag, Option<TagCategory>, Option<ApplyAutoTagsTask>), TransactionRuntimeError> {
    let (tag, category) = tag::table
        .left_join(tag_category::table)
        .filter(tag::pk.eq(tag_pk))
        .get_result::<(Tag, Option<TagCategory>)>(connection)
        .await
        .optional()?
        .ok_or(TransactionRuntimeError::Rollback(Error::NotFoundError))?;

    let mut parents_changed = false;
    let mut aliases_changed = false;

    let curr_parent_pks = get_tag_parents_pks(tag.pk, connection).await?;
    if let Some(ref mut parent_pks_overwrite) = parent_pks_overwrite {
        // add parents that aren't already set to added_parent_pks
        match added_parent_pks {
            Some(ref mut added_parent_pks) => {
                added_parent_pks.append(&mut parent_pks_overwrite.clone())
            }
            None => added_parent_pks = Some(parent_pks_overwrite.clone()),
        }
        // remove current parents that aren't in the added set
        let parents_to_remove = curr_parent_pks
            .iter()
            .filter(|tag_pk| !added_parent_pks.as_ref().unwrap().contains(tag_pk))
            .collect::<Vec<_>>();

        if !parents_to_remove.is_empty() {
            diesel::delete(tag_edge::table)
                .filter(
                    tag_edge::fk_child
                        .eq(tag.pk)
                        .and(tag_edge::fk_parent.eq_any(&parents_to_remove)),
                )
                .execute(connection)
                .await?;
            parents_changed = true;
        }
    }

    if let Some(ref mut added_parent_pks) = added_parent_pks {
        added_parent_pks.retain(|tag_pk| !curr_parent_pks.contains(tag_pk));
        if !added_parent_pks.is_empty() {
            if added_parent_pks.contains(&tag.pk) {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InvalidRequestInputError(format!(
                        "Cannot set tag {} as its own parent",
                        tag.pk
                    )),
                ));
            }
            report_missing_pks!(tag, &*added_parent_pks, connection)??;
            add_tag_parents(tag.pk, added_parent_pks, connection).await?;
            parents_changed = true;
        }
    }

    if let Some(ref removed_parent_pks) = removed_parent_pks {
        if !removed_parent_pks.is_empty() {
            diesel::delete(tag_edge::table)
                .filter(
                    tag_edge::fk_child
                        .eq(tag.pk)
                        .and(tag_edge::fk_parent.eq_any(removed_parent_pks)),
                )
                .execute(connection)
                .await?;
            parents_changed = true;
        }
    }

    if added_parent_pks
        .as_ref()
        .map(|v| !v.is_empty())
        .unwrap_or(false)
    {
        let curr_parents = get_tag_parents_pks(tag.pk, connection).await?;
        if curr_parents.len() > 25 {
            return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                String::from("Cannot set more than 25 parents"),
            )));
        }
    }

    let curr_alias_pks = get_tag_aliases_pks(tag.pk, connection).await?;
    if let Some(ref mut alias_pks_overwrite) = alias_pks_overwrite {
        // add aliases that aren't already set to added_alias_pks
        match added_alias_pks {
            Some(ref mut added_alias_pks) => {
                added_alias_pks.append(&mut alias_pks_overwrite.clone())
            }
            None => added_alias_pks = Some(alias_pks_overwrite.clone()),
        }

        // remove current aliases that aren't in the added set
        let aliases_to_remove = curr_alias_pks
            .iter()
            .filter(|tag_pk| !added_alias_pks.as_ref().unwrap().contains(tag_pk))
            .collect::<Vec<_>>();

        if !aliases_to_remove.is_empty() {
            // a tag cannot be the alias of another tag without also being an alias of all of that tag's other aliases
            // thus, removing an alias means that tag loses all its aliases
            diesel::delete(tag_alias::table)
                .filter(
                    tag_alias::fk_target
                        .eq_any(&aliases_to_remove)
                        .or(tag_alias::fk_source.eq_any(&aliases_to_remove)),
                )
                .execute(connection)
                .await?;
            aliases_changed = true;
        }
    }

    if let Some(ref mut added_alias_pks) = added_alias_pks {
        added_alias_pks.retain(|tag_pk| !curr_alias_pks.contains(tag_pk));
        if !added_alias_pks.is_empty() {
            if added_alias_pks.contains(&tag.pk) {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InvalidRequestInputError(format!(
                        "Cannot set tag {} as an alias of itself",
                        tag.pk
                    )),
                ));
            }
            report_missing_pks!(tag, &*added_alias_pks, connection)??;
            add_tag_aliases(&tag, added_alias_pks, connection).await?;
            aliases_changed = true;
            // remove aliases inherited from one of the added aliases that was not included in the overwrite request
            if alias_pks_overwrite.is_some() {
                let inserted_aliases = get_tag_aliases(tag.pk, connection).await?;
                let mut inherited_aliases = inserted_aliases
                    .iter()
                    .map(|t| t.pk)
                    .filter(|alias_pk| !added_alias_pks.contains(alias_pk))
                    .to_owned()
                    .collect::<Vec<_>>();
                match removed_alias_pks {
                    Some(ref mut removed_alias_pks) => {
                        removed_alias_pks.append(&mut inherited_aliases)
                    }
                    None => removed_alias_pks = Some(inherited_aliases),
                }
            }
        }
    }

    if let Some(ref removed_alias_pks) = removed_alias_pks {
        if !removed_alias_pks.is_empty() {
            // a tag cannot be the alias of another tag without also being an alias of all of that tag's other aliases
            // thus, removing an alias means that tag loses all its aliases
            diesel::delete(tag_alias::table)
                .filter(
                    tag_alias::fk_target
                        .eq_any(removed_alias_pks)
                        .or(tag_alias::fk_source.eq_any(removed_alias_pks)),
                )
                .execute(connection)
                .await?;
            aliases_changed = true;
        }
    }

    if added_alias_pks
        .as_ref()
        .map(|v| !v.is_empty())
        .unwrap_or(false)
    {
        let curr_aliases = get_tag_aliases_pks(tag.pk, connection).await?;
        if curr_aliases.len() > 25 {
            return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                String::from("Cannot set more than 25 aliases"),
            )));
        }
    }

    let mut update = TagUpdateOptional {
        tag_category: tag_category.clone(),
        auto_match_condition_post: auto_match_condition_post.clone(),
        auto_match_condition_collection: auto_match_condition_collection.clone(),
        compiled_auto_match_condition_post: None,
        compiled_auto_match_condition_collection: None,
        edit_timestamp: Utc::now(),
        fk_edit_user: user.pk,
    };
    let field_changes = update.get_field_changes(&tag);

    if parents_changed || aliases_changed || field_changes.has_changes() {
        // don't create a history entry if only auto_match_condition_post or auto_match_condition_collection have changed because they are not tracked by history
        if parents_changed || aliases_changed || field_changes.tag_category_changed {
            let tag_edit_history = diesel::insert_into(tag_edit_history::table)
                .values(NewTagEditHistory {
                    fk_tag: tag.pk,
                    fk_edit_user: tag.fk_edit_user,
                    edit_timestamp: tag.edit_timestamp,
                    tag_category: tag.tag_category,
                    tag_category_changed: field_changes.tag_category_changed,
                    parents_changed,
                    aliases_changed,
                })
                .get_result::<TagEditHistory>(connection)
                .await?;

            if parents_changed && !curr_parent_pks.is_empty() {
                diesel::insert_into(tag_edit_history_parent::table)
                    .values(
                        curr_parent_pks
                            .iter()
                            .map(|parent_pk| TagEditHistoryParent {
                                fk_tag_edit_history: tag_edit_history.pk,
                                fk_parent: *parent_pk,
                            })
                            .collect::<Vec<_>>(),
                    )
                    .execute(connection)
                    .await?;
            }

            if aliases_changed && !curr_alias_pks.is_empty() {
                diesel::insert_into(tag_edit_history_alias::table)
                    .values(
                        curr_alias_pks
                            .iter()
                            .map(|alias_pk| TagEditHistoryAlias {
                                fk_tag_edit_history: tag_edit_history.pk,
                                fk_alias: *alias_pk,
                            })
                            .collect::<Vec<_>>(),
                    )
                    .execute(connection)
                    .await?;
            }
        }

        let updated_category = if field_changes.tag_category_changed {
            match tag_category {
                Some(tag_category) if !tag_category.is_empty() => {
                    let found_tag_category = tag_category::table
                        .filter(tag_category::id.eq(&tag_category))
                        .get_result::<TagCategory>(connection)
                        .await
                        .optional()?;
                    if let Some(found_tag_category) = found_tag_category {
                        Some(found_tag_category)
                    } else {
                        return Err(TransactionRuntimeError::Rollback(
                            Error::InvalidEntityReferenceError(tag_category),
                        ));
                    }
                }
                _ => None,
            }
        } else {
            category
        };

        let compiled_auto_match_condition_post = compile_auto_match_condition(
            tag.tag_name.clone(),
            update.auto_match_condition_post.clone(),
            updated_category
                .as_ref()
                .and_then(|tc| tc.auto_match_condition_post.clone()),
            Scope::TagAutoMatchPost,
        )?;
        update.compiled_auto_match_condition_post =
            Some(compiled_auto_match_condition_post.unwrap_or_else(|| String::from("")));
        let compiled_auto_match_condition_collection = compile_auto_match_condition(
            tag.tag_name.clone(),
            update.auto_match_condition_collection.clone(),
            updated_category
                .as_ref()
                .and_then(|tc| tc.auto_match_condition_collection.clone()),
            Scope::TagAutoMatchCollection,
        )?;
        update.compiled_auto_match_condition_collection =
            Some(compiled_auto_match_condition_collection.unwrap_or_else(|| String::from("")));

        let updated_tag = diesel::update(tag::table)
            .filter(tag::pk.eq(tag.pk))
            .set(&update)
            .get_result::<Tag>(connection)
            .await?;

        let apply_auto_tag_task = if string_value_updated(
            tag.compiled_auto_match_condition_post.as_deref(),
            update.compiled_auto_match_condition_post.as_deref(),
        ) || string_value_updated(
            tag.compiled_auto_match_condition_collection.as_deref(),
            update.compiled_auto_match_condition_collection.as_deref(),
        ) {
            Some(create_apply_auto_tag_task(tag.pk, connection).await?)
        } else {
            None
        };

        Ok((updated_tag, updated_category, apply_auto_tag_task))
    } else {
        Ok((tag, category, None))
    }
}

pub async fn add_tag_aliases(
    tag: &Tag,
    alias_pks: &[i64],
    connection: &mut AsyncPgConnection,
) -> Result<(), TransactionRuntimeError> {
    let aliases_to_insert = alias_pks
        .iter()
        .map(|alias_pk| TagAlias {
            fk_source: tag.pk,
            fk_target: *alias_pk,
        })
        .collect::<Vec<_>>();

    diesel::insert_into(tag_alias::table)
        .values(aliases_to_insert)
        .on_conflict_do_nothing()
        .execute(connection)
        .await
        .map_err(retry_on_constraint_violation)?;

    Ok(())
}

pub async fn add_tag_parents(
    tag_pk: i64,
    parent_pks: &[i64],
    connection: &mut AsyncPgConnection,
) -> Result<(), TransactionRuntimeError> {
    let edges_to_insert = parent_pks
        .iter()
        .map(|parent_pk| TagEdge {
            fk_parent: *parent_pk,
            fk_child: tag_pk,
        })
        .collect::<Vec<_>>();

    diesel::insert_into(tag_edge::table)
        .values(edges_to_insert)
        .on_conflict_do_nothing()
        .execute(connection)
        .await
        .map_err(retry_on_constraint_violation)?;

    Ok(())
}

#[derive(AsChangeset, Deserialize, Identifiable, Validate)]
#[diesel(table_name = tag_category)]
#[diesel(primary_key(id))]
pub struct TagCategoryUpdateRequest {
    pub id: String,
    #[validate(length(min = 1, max = 255))]
    pub label: Option<String>,
    #[validate(length(max = 1000))]
    pub auto_match_condition_post: Option<String>,
    #[validate(length(max = 1000))]
    pub auto_match_condition_collection: Option<String>,
}

impl TagCategoryUpdateRequest {
    pub fn get_field_changes(&self, curr_value: &TagCategory) -> TagCategoryUpdateFieldChanges {
        TagCategoryUpdateFieldChanges {
            label_changed: string_value_updated(Some(&curr_value.label), self.label.as_deref()),
            auto_match_condition_post_changed: string_value_updated(
                curr_value.auto_match_condition_post.as_deref(),
                self.auto_match_condition_post.as_deref(),
            ),
            auto_match_condition_collection_changed: string_value_updated(
                curr_value.auto_match_condition_collection.as_deref(),
                self.auto_match_condition_collection.as_deref(),
            ),
        }
    }
}

pub struct TagCategoryUpdateFieldChanges {
    pub label_changed: bool,
    pub auto_match_condition_post_changed: bool,
    pub auto_match_condition_collection_changed: bool,
}

impl TagCategoryUpdateFieldChanges {
    pub fn has_changes(&self) -> bool {
        self.label_changed
            || self.auto_match_condition_post_changed
            || self.auto_match_condition_collection_changed
    }
}

pub async fn update_tag_category_handler(
    request: TagCategoryUpdateRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for TagCategoryUpdateRequest: {e}"
        )))
    })?;

    if !user.is_admin {
        return Err(warp::reject::custom(Error::UserNotAdmin));
    }

    // test if conditions compile
    if let Some(ref auto_match_condition_post) = request.auto_match_condition_post {
        compile_auto_match_condition(
            String::from("test"),
            None,
            Some(auto_match_condition_post.clone()),
            Scope::TagAutoMatchPost,
        )?;
    }
    if let Some(ref auto_match_condition_collection) = request.auto_match_condition_collection {
        compile_auto_match_condition(
            String::from("test"),
            None,
            Some(auto_match_condition_collection.clone()),
            Scope::TagAutoMatchCollection,
        )?;
    }

    let mut connection = acquire_db_connection().await?;
    let (updated_tag_category, apply_auto_tags_task) =
        run_serializable_transaction(&mut connection, |connection| {
            async {
                let tag_category = tag_category::table
                    .filter(tag_category::id.eq(&request.id))
                    .get_result::<TagCategory>(connection)
                    .await
                    .optional()?
                    .ok_or(Error::NotFoundError)?;

                let field_changes = request.get_field_changes(&tag_category);

                if field_changes.has_changes() {
                    let updated_tag_category = diesel::update(tag_category::table)
                        .filter(tag_category::id.eq(&tag_category.id))
                        .set(&request)
                        .get_result::<TagCategory>(connection)
                        .await?;

                    let apply_auto_tags_task = if field_changes.auto_match_condition_post_changed
                        || field_changes.auto_match_condition_collection_changed
                    {
                        let tags = Tag::belonging_to(&updated_tag_category)
                            .load::<Tag>(connection)
                            .await?;

                        if field_changes.auto_match_condition_post_changed {
                            for tag in tags.iter() {
                                let compiled_tag_auto_match_condition_post =
                                    compile_tag_auto_match_condition(
                                        tag.clone(),
                                        Some(updated_tag_category.clone()),
                                        AutoMatchTarget::Post,
                                    )?;

                                diesel::update(tag::table)
                                    .filter(tag::pk.eq(tag.pk))
                                    .set(
                                        tag::compiled_auto_match_condition_post
                                            .eq(compiled_tag_auto_match_condition_post
                                                .unwrap_or_else(|| String::from(""))),
                                    )
                                    .execute(connection)
                                    .await?;
                            }
                        }
                        if field_changes.auto_match_condition_collection_changed {
                            for tag in tags.iter() {
                                let compiled_tag_auto_match_condition_collection =
                                    compile_tag_auto_match_condition(
                                        tag.clone(),
                                        Some(updated_tag_category.clone()),
                                        AutoMatchTarget::Collection,
                                    )?;

                                diesel::update(tag::table)
                                    .filter(tag::pk.eq(tag.pk))
                                    .set(
                                        tag::auto_match_condition_collection
                                            .eq(compiled_tag_auto_match_condition_collection
                                                .unwrap_or_else(|| String::from(""))),
                                    )
                                    .execute(connection)
                                    .await?;
                            }
                        }

                        let apply_auto_tags_task = create_apply_tag_category_auto_tags_task(
                            tag_category.id.clone(),
                            connection,
                        )
                        .await?;
                        Some(apply_auto_tags_task)
                    } else {
                        None
                    };

                    Ok((updated_tag_category, apply_auto_tags_task))
                } else {
                    Ok((tag_category, None))
                }
            }
            .scope_boxed()
        })
        .await?;

    if let Some(apply_auto_tags_task) = apply_auto_tags_task {
        spawn_apply_auto_tags_task(apply_auto_tags_task);
    }

    Ok(warp::reply::json(&updated_tag_category))
}
