use std::collections::HashMap;

use chrono::Utc;
use diesel::{
    dsl::exists,
    sql_types::{Array, Integer},
    BoolExpressionMethods, OptionalExtension,
};
use exec_rs::sync::MutexSync;
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError};
use warp::{Rejection, Reply};

use crate::{
    acquire_db_connection,
    diesel::{ExpressionMethods, QueryDsl, RunQueryDsl},
    error::{Error, TransactionRuntimeError},
    model::{
        NewPost, NewTag, NewTagClosureTable, Post, PostTag, Tag, TagAlias, TagClosureTable, User,
    },
    query::functions::*,
    retry_on_constraint_violation, run_retryable_transaction,
    schema::{post, post_tag, tag, tag_alias, tag_closure_table},
    DbConnection,
};

lazy_static! {
    static ref TAG_SYNC: MutexSync<String> = MutexSync::new();
}

#[derive(Deserialize, Validate)]
pub struct CreatePostRequest {
    #[validate(url)]
    pub data_url: String,
    #[validate(url)]
    pub source_url: Option<String>,
    pub title: Option<String>,
    #[validate(length(max = 100), custom = "validate_tags")]
    pub tags: Option<Vec<String>>,
    pub s3_object: Option<String>,
}

fn validate_tags(tags: &Vec<String>) -> Result<(), ValidationError> {
    for tag in tags {
        if !tag_is_valid(tag) {
            return Err(ValidationError::new(
                "Invalid tag, tags must be alphanumeric (or ['_', ''']) and 50 or less in length",
            ));
        }
    }

    Ok(())
}

fn validate_tag(tag: &str) -> Result<(), ValidationError> {
    if !tag_is_valid(tag) {
        return Err(ValidationError::new(
            "Invalid tag, tags must be 50 or less in length",
        ));
    }

    Ok(())
}

#[inline]
fn tag_is_valid(tag: &str) -> bool {
    tag.len() <= 50
}

lazy_static! {
    pub static ref WHITESPACE_REGEX: Regex = Regex::new("\\s+").unwrap();
}

#[inline]
fn sanitize_tag(tag: String) -> String {
    WHITESPACE_REGEX.replace_all(tag.trim(), " ").into_owned()
}

#[derive(Serialize)]
pub struct CreatePostResponse {
    pub post_id: i32,
}

fn sanitize_request_tags(request_tags: Vec<String>) -> Vec<String> {
    request_tags
        .into_iter()
        .unique()
        .map(sanitize_tag)
        .collect::<Vec<_>>()
}

pub async fn create_post_handler(
    create_post_request: CreatePostRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    create_post_request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreatePostRequest: {}",
            e
        )))
    })?;

    // cannot use hashset because it is not supported as diesel expression
    let tags = create_post_request.tags.map(sanitize_request_tags);

    let connection = acquire_db_connection()?;
    // run as repeatable read transaction and retry serialisation errors when a concurrent transaction
    // deletes or creates relevant tags
    run_retryable_transaction(&connection, || {
        let set_tags = if let Some(ref tags) = tags {
            let (mut set_tags, created_tags) = get_or_create_tags(&connection, tags)?;
            set_tags.extend(created_tags);

            filter_redundant_tags(&mut set_tags, &connection)?;
            Some(set_tags)
        } else {
            None
        };

        let post = diesel::insert_into(post::table)
            .values(NewPost {
                data_url: create_post_request.data_url.clone(),
                source_url: create_post_request.source_url.clone(),
                title: create_post_request.title.clone(),
                creation_timestamp: Utc::now(),
                fk_create_user: user.pk,
                score: 0,
                s3_object: create_post_request.s3_object.clone(),
            })
            .get_result::<Post>(&connection)?;

        if let Some(set_tags) = set_tags {
            let post_tags = set_tags
                .iter()
                .map(|tag| PostTag {
                    fk_post: post.pk,
                    fk_tag: tag.pk,
                })
                .collect::<Vec<_>>();

            diesel::insert_into(post_tag::table)
                .values(&post_tags)
                .execute(&connection)?;
        }

        Ok(warp::reply::json(&CreatePostResponse { post_id: post.pk }))
    })
    .map_err(warp::reject::custom)
}

#[derive(Deserialize, Validate)]
pub struct CreateTagsRequest {
    #[validate(custom = "validate_tags")]
    pub tag_names: Vec<String>,
}

#[derive(Serialize)]
pub struct CreateTagsResponse {
    pub existing_tags: Vec<Tag>,
    pub inserted_tags: Vec<Tag>,
}

pub async fn create_tags_handler(
    create_tags_request: CreateTagsRequest,
    _user: User,
) -> Result<impl Reply, Rejection> {
    create_tags_request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreateTagsRequest: {}",
            e
        )))
    })?;

    let tag_names = sanitize_request_tags(create_tags_request.tag_names);
    let connection = acquire_db_connection()?;
    run_retryable_transaction(&connection, || {
        let (existing_tags, inserted_tags) = get_or_create_tags(&connection, &tag_names)?;
        Ok(warp::reply::json(&CreateTagsResponse {
            existing_tags,
            inserted_tags,
        }))
    })
    .map_err(warp::reject::custom)
}

/// Get and create all tags for the supplied tag names, returning a tuple of all existing and all created tags.
pub fn get_or_create_tags(
    connection: &DbConnection,
    tags: &[String],
) -> Result<(Vec<Tag>, Vec<Tag>), TransactionRuntimeError> {
    let lower_tags = tags.iter().map(|s| s.to_lowercase()).collect::<Vec<_>>();
    let existing_tags = tag::table
        .filter(lower(tag::tag_name).eq_any(&lower_tags))
        .load::<Tag>(connection)?;

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
                creation_timestamp: Utc::now(),
            }),
        }
    }

    let created_tags = diesel::insert_into(tag::table)
        .values(&new_tags)
        .get_results::<Tag>(connection)
        .map_err(retry_on_constraint_violation)?;

    if !created_tags.is_empty() {
        // create self referencing closure table entries for all inserted tags
        let tag_closure_table_entries = created_tags
            .iter()
            .map(|created_tag| NewTagClosureTable {
                fk_parent: created_tag.pk,
                fk_child: created_tag.pk,
                depth: 0,
            })
            .collect::<Vec<_>>();

        diesel::insert_into(tag_closure_table::table)
            .values(&tag_closure_table_entries)
            .execute(connection)?;
    }

    Ok((set_tags, created_tags))
}

/// Filter redundant tags by removing tags that are a parent of another included tag or a shorter alias
/// for another included tag.
pub fn filter_redundant_tags(tags: &mut Vec<Tag>, connection: &DbConnection) -> Result<(), Error> {
    let mut selected_tags = Vec::new();
    for tag in tags.iter() {
        selected_tags.push(get_tag_hierarchy_information(tag, connection)?);
    }

    tags.retain(|tag| {
        selected_tags.iter().all(|other| {
            if tag.pk == other.tag.pk {
                return true;
            }

            let other_tag = &other.tag;
            let other_aliases = &other.tag_aliases;
            let is_parent = other.parent_depth_map.contains_key(&tag.pk);

            let is_shorter_alias = other_aliases
                .iter()
                .any(|alias| alias.pk == tag.pk && tag.tag_name.len() < other_tag.tag_name.len());

            !is_parent && !is_shorter_alias
        })
    });

    Ok(())
}

pub struct TagHierarchyInformation {
    pub tag: Tag,
    /// Mapping for the parent tag pks to their respective depth
    pub parent_depth_map: HashMap<i32, i32>,
    pub tag_aliases: Vec<Tag>,
}

pub fn get_tag_hierarchy_information(
    tag: &Tag,
    connection: &DbConnection,
) -> Result<TagHierarchyInformation, Error> {
    let tag_closure_table = tag_closure_table::table
        .filter(
            tag_closure_table::fk_child
                .eq(tag.pk)
                .and(tag_closure_table::depth.gt(0)),
        )
        .load::<TagClosureTable>(connection)?;

    let mut parent_depth_map = HashMap::new();
    for tag_closure in tag_closure_table.iter() {
        parent_depth_map.insert(tag_closure.fk_parent, tag_closure.depth);
    }

    let tag_aliases = tag::table
        .filter(exists(
            tag_alias::table.filter(
                (tag_alias::fk_source
                    .eq(tag.pk)
                    .and(tag_alias::fk_target.eq(tag::pk)))
                .or(tag_alias::fk_source
                    .eq(tag::pk)
                    .and(tag_alias::fk_target.eq(tag.pk))),
            ),
        ))
        .load::<Tag>(connection)?;

    Ok(TagHierarchyInformation {
        tag: tag.clone(),
        parent_depth_map,
        tag_aliases,
    })
}

#[derive(Deserialize, Validate)]
pub struct UpsertTagRequest {
    #[validate(custom = "validate_tag")]
    pub tag_name: String,
    pub parent_pk: Option<i32>,
    pub alias_pks: Option<Vec<i32>>,
}

#[derive(Serialize)]
pub struct UpsertTagResponse {
    pub inserted: bool,
}

/// Creates a tag with the given tag_name, parent and aliases. If the tag already exists, the existing tag is updated
/// with the giving parent and the provided aliases are added.
pub async fn upsert_tag_handler(
    upsert_tag_request: UpsertTagRequest,
    _user: User,
) -> Result<impl Reply, Rejection> {
    upsert_tag_request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for UpsertTagRequest: {}",
            e
        )))
    })?;

    let tag_name = sanitize_tag(upsert_tag_request.tag_name);

    let connection = acquire_db_connection()?;
    run_retryable_transaction(&connection, || {
        let (inserted, tag) = get_or_create_tag(&tag_name, &connection)?;

        if let Some(parent_pk) = upsert_tag_request.parent_pk {
            if parent_pk == tag.pk {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InvalidRequestInputError(format!(
                        "Cannot set tag {} as its own parent",
                        parent_pk
                    )),
                ));
            }
            set_tag_parent(&tag, parent_pk, &connection)?;
        }

        match upsert_tag_request.alias_pks {
            Some(ref alias_pks) if !alias_pks.is_empty() => {
                if alias_pks.iter().any(|alias_pk| *alias_pk == tag.pk) {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::InvalidRequestInputError(format!(
                            "Cannot set tag {} as an alias of itself",
                            tag.pk
                        )),
                    ));
                }

                // set parent of all added aliases to the parent of this tag
                if let Some(parent_pk) = get_tag_parent_pk(tag.pk, &connection)? {
                    set_tags_parent(alias_pks, parent_pk, &connection)?;
                } else {
                    remove_tags_from_ancestry(alias_pks, &connection)?;
                }

                let tag_aliases = alias_pks
                    .iter()
                    .map(|alias_pk| TagAlias {
                        fk_source: tag.pk,
                        fk_target: *alias_pk,
                    })
                    .collect::<Vec<_>>();

                // do nothing on conflict, i.e. if the alias already exists,
                // since the table has a bi-directional unique index this also works if one of the provided aliases already has this tag as an alias
                diesel::insert_into(tag_alias::table)
                    .values(&tag_aliases)
                    .on_conflict_do_nothing()
                    .execute(&connection)?;
            }
            _ => {}
        }

        Ok(warp::reply::json(&UpsertTagResponse { inserted }))
    })
    .map_err(warp::reject::custom)
}

pub fn get_or_create_tag(
    tag_name: &str,
    connection: &DbConnection,
) -> Result<(bool, Tag), TransactionRuntimeError> {
    let existing_tag = tag::table
        .filter(lower(tag::tag_name).eq(tag_name.to_lowercase()))
        .first::<Tag>(connection)
        .optional()?;

    let inserted = existing_tag.is_none();
    let tag = match existing_tag {
        Some(existing_tag) => existing_tag,
        None => diesel::insert_into(tag::table)
            .values(&NewTag {
                tag_name: String::from(tag_name),
                creation_timestamp: Utc::now(),
            })
            .get_result::<Tag>(connection)
            .map_err(retry_on_constraint_violation)?,
    };

    // Create self referencing closure tag entry for newly inserted tag
    if inserted {
        diesel::insert_into(tag_closure_table::table)
            .values(&NewTagClosureTable {
                fk_parent: tag.pk,
                fk_child: tag.pk,
                depth: 0,
            })
            .execute(connection)?;
    }

    Ok((inserted, tag))
}

pub fn get_tag_parent_pk(
    tag_pk: i32,
    connection: &DbConnection,
) -> Result<Option<i32>, diesel::result::Error> {
    tag_closure_table::table
        .select(tag_closure_table::fk_parent)
        .filter(
            tag_closure_table::fk_child
                .eq(tag_pk)
                .and(tag_closure_table::depth.eq(1)),
        )
        .get_result::<i32>(connection)
        .optional()
}

pub fn set_tag_parent(
    tag: &Tag,
    parent_pk: i32,
    connection: &DbConnection,
) -> Result<(), TransactionRuntimeError> {
    remove_tag_from_ancestry(tag, connection)?;
    diesel::sql_query(
        r#"
        INSERT INTO tag_closure_table(fk_parent, fk_child, depth)
        SELECT p.fk_parent, c.fk_child, p.depth + c.depth + 1
        FROM tag_closure_table p, tag_closure_table c
        WHERE p.fk_child = $1 AND c.fk_parent = $2
    "#,
    )
    .bind::<Integer, _>(parent_pk)
    .bind::<Integer, _>(tag.pk)
    .execute(connection)
    .map_err(retry_on_constraint_violation)?;
    Ok(())
}

pub fn remove_tag_from_ancestry(
    tag: &Tag,
    connection: &DbConnection,
) -> Result<(), TransactionRuntimeError> {
    // delete all links to node for all neighboring nodes (i.e. depth = 1)
    // keeping the self reference (depth = 0) since to node is not deleted, only removed from the current hierarchy
    diesel::sql_query(r#"
        DELETE FROM tag_closure_table WHERE pk IN(
            SELECT link.pk
            FROM tag_closure_table p, tag_closure_table link, tag_closure_table c, tag_closure_table to_delete
            WHERE p.fk_parent = link.fk_parent AND c.fk_child = link.fk_child
            AND p.fk_child  = to_delete.fk_parent AND c.fk_parent = to_delete.fk_child
            AND (to_delete.fk_parent = $1 OR to_delete.fk_child = $1)
            AND to_delete.depth = 1
        )
    "#)
    .bind::<Integer, _>(tag.pk)
    .execute(connection)
    .map_err(retry_on_constraint_violation)?;
    Ok(())
}

pub fn set_tags_parent(
    tag_pks: &Vec<i32>,
    parent_pk: i32,
    connection: &DbConnection,
) -> Result<(), TransactionRuntimeError> {
    remove_tags_from_ancestry(tag_pks, connection)?;
    diesel::sql_query(
        r#"
        INSERT INTO tag_closure_table(fk_parent, fk_child, depth)
        SELECT p.fk_parent, c.fk_child, p.depth + c.depth + 1
        FROM tag_closure_table p, tag_closure_table c
        WHERE p.fk_child = $1 AND c.fk_parent = ANY($2)
    "#,
    )
    .bind::<Integer, _>(parent_pk)
    .bind::<Array<Integer>, _>(tag_pks)
    .execute(connection)
    .map_err(retry_on_constraint_violation)?;
    Ok(())
}

pub fn remove_tags_from_ancestry(
    tag_pks: &Vec<i32>,
    connection: &DbConnection,
) -> Result<(), TransactionRuntimeError> {
    // delete all links to node for all neighboring nodes (i.e. depth = 1)
    // keeping the self reference (depth = 0) since to node is not deleted, only removed from the current hierarchy
    diesel::sql_query(r#"
        DELETE FROM tag_closure_table WHERE pk IN(
            SELECT link.pk
            FROM tag_closure_table p, tag_closure_table link, tag_closure_table c, tag_closure_table to_delete
            WHERE p.fk_parent = link.fk_parent AND c.fk_child = link.fk_child
            AND p.fk_child  = to_delete.fk_parent AND c.fk_parent = to_delete.fk_child
            AND (to_delete.fk_parent = ANY($1) OR to_delete.fk_child = ANY($1))
            AND to_delete.depth = 1
        )
    "#)
    .bind::<Array<Integer>, _>(tag_pks)
    .execute(connection)
    .map_err(retry_on_constraint_violation)?;
    Ok(())
}
