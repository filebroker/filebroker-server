use crate::error::{Error, TransactionRuntimeError};
use crate::model::{NewTag, Tag, TagCategory, User};
use crate::query::compiler::dict::Scope;
use crate::query::functions::lower;
use crate::query::{load_and_report_missing_pks, report_missing_pks};
use crate::schema::{tag, tag_category};
use crate::tag::auto_matching::compile_auto_match_condition;
use crate::tag::validate_tags;
use crate::tag::{filter_redundant_tags, sanitize_request_tags};
use crate::{
    acquire_db_connection, retry_on_constraint_violation, run_retryable_transaction, util,
};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use validator::Validate;
use warp::{Rejection, Reply};

#[derive(Deserialize, Validate)]
pub struct CreateTagsRequest {
    #[validate(custom(function = "validate_tags"))]
    pub tag_names: Vec<String>,
}

#[derive(Serialize)]
pub struct CreateTagsResponse {
    pub existing_tags: Vec<Tag>,
    pub inserted_tags: Vec<Tag>,
}

pub async fn create_tags_handler(
    create_tags_request: CreateTagsRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    create_tags_request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreateTagsRequest: {e}"
        )))
    })?;

    let mut tag_names = sanitize_request_tags(&create_tags_request.tag_names);
    util::dedup_vec(&mut tag_names);
    let mut connection = acquire_db_connection().await?;
    run_retryable_transaction(&mut connection, |connection| {
        async move {
            let (existing_tags, inserted_tags) =
                get_or_create_tags(connection, &tag_names, &user).await?;
            Ok(warp::reply::json(&CreateTagsResponse {
                existing_tags,
                inserted_tags,
            }))
        }
        .scope_boxed()
    })
    .await
    .map_err(warp::reject::custom)
}

/// Get and create all tags for the supplied tag names, returning a tuple of all existing and all created tags.
pub async fn get_or_create_tags(
    connection: &mut AsyncPgConnection,
    tags: &[String],
    user: &User,
) -> Result<(Vec<Tag>, Vec<Tag>), TransactionRuntimeError> {
    let lower_tags = tags.iter().map(|s| s.to_lowercase()).collect::<Vec<_>>();
    let existing_tags = tag::table
        .filter(lower(tag::tag_name).eq_any(&lower_tags))
        .load::<Tag>(connection)
        .await?;

    let mut existing_tag_map = HashMap::new();
    for existing_tag in existing_tags {
        existing_tag_map.insert(existing_tag.tag_name.to_lowercase(), existing_tag);
    }

    let mut new_tags = Vec::new();
    let mut set_tags = Vec::new();
    for tag in tags.iter() {
        match existing_tag_map.remove(&tag.to_lowercase()) {
            Some(existing_tag) => set_tags.push(existing_tag),
            None => new_tags.push(NewTag {
                tag_name: tag.clone(),
                fk_create_user: user.pk,
                tag_category: None,
                auto_match_condition_post: None,
                auto_match_condition_collection: None,
            }),
        }
    }

    let created_tags = diesel::insert_into(tag::table)
        .values(&new_tags)
        .get_results::<Tag>(connection)
        .await
        .map_err(retry_on_constraint_violation)?;

    Ok((set_tags, created_tags))
}

/// merge tags selected by name and tags selected by pk into a vector of loaded Tag structs,
/// saving all new tags for non-existing tag names
pub async fn handle_entered_and_selected_tags(
    selected_tags: &Option<Vec<i64>>,
    entered_tags: Option<Vec<String>>,
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<Tag>, TransactionRuntimeError> {
    // cannot use hashset because it is not supported as diesel expression
    let mut tags = entered_tags.as_deref().map(sanitize_request_tags);
    if let Some(selected_tags) = selected_tags {
        report_missing_pks!(tag, selected_tags, connection)??;
    }

    let mut set_tags = if let Some(ref mut tags) = tags {
        util::dedup_vec(tags);
        let (mut set_tags, created_tags) = get_or_create_tags(connection, tags, user).await?;
        set_tags.extend(created_tags);
        set_tags
    } else {
        Vec::new()
    };

    if let Some(selected_tags) = selected_tags {
        let loaded_selected_tags =
            load_and_report_missing_pks!(Tag, tag, selected_tags, connection)?;
        set_tags.extend(loaded_selected_tags);
    }

    if !set_tags.is_empty() {
        filter_redundant_tags(&mut set_tags, connection).await?;
        if set_tags.len() > 100 {
            return Err(TransactionRuntimeError::Rollback(
                Error::InvalidRequestInputError(format!(
                    "Cannot supply more than 100 tags, supplied: {}",
                    set_tags.len()
                )),
            ));
        }
    }

    Ok(set_tags)
}

pub async fn create_tag_category_handler(
    tag_category: TagCategory,
    user: User,
) -> Result<impl Reply, Rejection> {
    tag_category.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for TagCategory: {e}"
        )))
    })?;

    if !user.is_admin {
        return Err(warp::reject::custom(Error::UserNotAdmin));
    }

    // test if conditions compile
    if let Some(ref auto_match_condition_post) = tag_category.auto_match_condition_post {
        compile_auto_match_condition(
            String::from("test"),
            None,
            Some(auto_match_condition_post.clone()),
            Scope::TagAutoMatchPost,
        )?;
    }
    if let Some(ref auto_match_condition_collection) = tag_category.auto_match_condition_collection
    {
        compile_auto_match_condition(
            String::from("test"),
            None,
            Some(auto_match_condition_collection.clone()),
            Scope::TagAutoMatchCollection,
        )?;
    }

    let mut connection = acquire_db_connection().await?;
    let inserted_category = diesel::insert_into(tag_category::table)
        .values(&tag_category)
        .get_result::<TagCategory>(&mut connection)
        .await
        .map_err(Error::from)?;

    Ok(warp::reply::json(&inserted_category))
}
