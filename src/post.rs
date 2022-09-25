use std::{collections::HashMap, fmt::Write};

use chrono::Utc;
use diesel::{
    dsl::{any, exists},
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
    model::{NewPost, NewTag, Post, PostTag, Tag, TagClosureTable, TagEdge, User},
    query::functions::*,
    retry_on_constraint_violation, run_retryable_transaction,
    schema::{post, post_tag, tag, tag_alias, tag_closure_table, tag_edge},
    DbConnection,
};

lazy_static! {
    static ref TAG_SYNC: MutexSync<String> = MutexSync::new();
}

#[derive(Deserialize, Validate)]
pub struct CreatePostRequest {
    #[validate(url)]
    pub data_url: Option<String>,
    #[validate(url)]
    pub source_url: Option<String>,
    pub title: Option<String>,
    #[validate(length(max = 100), custom = "validate_tags")]
    pub tags: Option<Vec<String>>,
    pub s3_object: Option<String>,
    #[validate(url)]
    pub thumbnail_url: Option<String>,
}

fn validate_tags(tags: &Vec<String>) -> Result<(), ValidationError> {
    for tag in tags {
        if !tag_is_valid(tag) {
            return Err(ValidationError::new(
                "Invalid tag, tags must be 50 or less in length",
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
                thumbnail_url: create_post_request.thumbnail_url.clone(),
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

        Ok(warp::reply::json(&post))
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
        .filter(lower(tag::tag_name).eq(any(&lower_tags)))
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

    let tag_aliases = get_tag_aliases(tag.pk, connection)?;

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
    #[validate(length(min = 0, max = 25))]
    pub parent_pks: Option<Vec<i32>>,
    #[validate(length(min = 0, max = 25))]
    pub alias_pks: Option<Vec<i32>>,
}

#[derive(Serialize)]
pub struct UpsertTagResponse {
    pub inserted: bool,
    pub tag_pk: i32,
}

macro_rules! report_missing_pks {
    ($tab:ident, $pks:expr, $connection:expr) => {
        $tab::table
            .select($tab::pk)
            .filter($tab::pk.eq(any($pks)))
            .load::<i32>($connection)
            .map(|found_pks| {
                let missing_pks = $pks
                    .iter()
                    .filter(|pk| !found_pks.contains(pk))
                    .collect::<Vec<_>>();
                if missing_pks.is_empty() {
                    Ok(())
                } else {
                    Err(crate::Error::InvalidEntityReferenceError(
                        itertools::Itertools::intersperse(
                            missing_pks.into_iter().map(i32::to_string),
                            String::from(", "),
                        )
                        .collect::<String>(),
                    ))
                }
            })
            .map_err(crate::Error::from)
    };
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

    if let Some(ref parent_pks) = upsert_tag_request.parent_pks {
        report_missing_pks!(tag, parent_pks, &connection)??;
    }

    if let Some(ref alias_pks) = upsert_tag_request.alias_pks {
        report_missing_pks!(tag, alias_pks, &connection)??;
    }

    run_retryable_transaction(&connection, || {
        let (inserted, tag) = get_or_create_tag(&tag_name, &connection)?;

        let parents_to_set = if let Some(ref parent_pks) = upsert_tag_request.parent_pks {
            if parent_pks.iter().any(|parent_pk| *parent_pk == tag.pk) {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InvalidRequestInputError(format!(
                        "Cannot set tag {} as its own parent",
                        tag.pk
                    )),
                ));
            }

            let curr_parents = get_tag_parents_pks(tag.pk, &connection)?;
            let parents_to_set = parent_pks
                .iter()
                .cloned()
                .filter(|parent_pk| !curr_parents.contains(parent_pk))
                .collect::<Vec<_>>();

            if curr_parents.len() + parents_to_set.len() > 25 {
                return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                    String::from("Cannot set more than 25 parents"),
                )));
            }

            add_tag_parents(tag.pk, &parents_to_set, &connection)?;
            Some(parents_to_set)
        } else {
            None
        };

        let aliases_to_set = match upsert_tag_request.alias_pks {
            Some(ref alias_pks) if !alias_pks.is_empty() => {
                if alias_pks.iter().any(|alias_pk| *alias_pk == tag.pk) {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::InvalidRequestInputError(format!(
                            "Cannot set tag {} as an alias of itself",
                            tag.pk
                        )),
                    ));
                }

                let curr_aliases = get_tag_aliases_pks(tag.pk, &connection)?;
                let aliases_to_set = alias_pks
                    .iter()
                    .cloned()
                    .filter(|alias_pk| !curr_aliases.contains(alias_pk))
                    .collect::<Vec<_>>();

                if curr_aliases.len() + aliases_to_set.len() > 25 {
                    return Err(TransactionRuntimeError::Rollback(Error::BadRequestError(
                        String::from("Cannot set more than 25 aliases"),
                    )));
                }

                // remove added tags from existing hierarchy to sync this this tag's hierarchy
                remove_tags_from_hierarchy(&aliases_to_set, &connection)?;
                add_tag_aliases(&tag, &aliases_to_set, &connection)?;
                Some(aliases_to_set)
            }
            _ => None,
        };

        if aliases_to_set.as_ref().map_or(false, |pks| !pks.is_empty())
            || parents_to_set.as_ref().map_or(false, |pks| !pks.is_empty())
        {
            log::debug!("syncing parents with aliases");
            let curr_aliases = get_tag_aliases(tag.pk, &connection)?;
            // add current parents of tag as parents for all current aliases if either changed
            let curr_parents = get_tag_parents_pks(tag.pk, &connection)?;
            log::debug!("curr parents: {:?}", &curr_parents);
            if !curr_parents.is_empty() {
                for alias in curr_aliases.iter() {
                    log::debug!(
                        "updating alias: setting {:?} as parents of alias {}",
                        &curr_parents,
                        alias.pk
                    );
                    add_tag_parents(alias.pk, &curr_parents, &connection)?;
                }
            }

            let curr_children = get_tag_children_pks(tag.pk, &connection)?;
            if !curr_children.is_empty() {
                let curr_alias_pks = curr_aliases
                    .iter()
                    .map(|alias| alias.pk)
                    .collect::<Vec<_>>();
                for child in curr_children {
                    log::debug!(
                        "updating child: setting curr aliases {:?} as parents of child {}",
                        &curr_alias_pks,
                        child
                    );
                    add_tag_parents(child, &curr_alias_pks, &connection)?;
                }
            }
        }

        Ok(warp::reply::json(&UpsertTagResponse {
            inserted,
            tag_pk: tag.pk,
        }))
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

    Ok((inserted, tag))
}

pub fn get_tag_parents_pks(
    tag_pk: i32,
    connection: &DbConnection,
) -> Result<Vec<i32>, diesel::result::Error> {
    tag_edge::table
        .select(tag_edge::fk_parent)
        .filter(tag_edge::fk_child.eq(tag_pk))
        .load::<i32>(connection)
}

pub fn get_tag_children_pks(
    tag_pk: i32,
    connection: &DbConnection,
) -> Result<Vec<i32>, diesel::result::Error> {
    tag_edge::table
        .select(tag_edge::fk_child)
        .filter(tag_edge::fk_parent.eq(tag_pk))
        .load::<i32>(connection)
}

pub fn get_tag_aliases_pks(
    tag_pk: i32,
    connection: &DbConnection,
) -> Result<Vec<i32>, diesel::result::Error> {
    tag::table
        .select(tag::pk)
        .filter(exists(
            tag_alias::table.filter(
                (tag_alias::fk_source
                    .eq(tag_pk)
                    .and(tag_alias::fk_target.eq(tag::pk)))
                .or(tag_alias::fk_source
                    .eq(tag::pk)
                    .and(tag_alias::fk_target.eq(tag_pk))),
            ),
        ))
        .load::<i32>(connection)
}

pub fn get_tag_aliases(
    tag_pk: i32,
    connection: &DbConnection,
) -> Result<Vec<Tag>, diesel::result::Error> {
    tag::table
        .filter(exists(
            tag_alias::table.filter(
                (tag_alias::fk_source
                    .eq(tag_pk)
                    .and(tag_alias::fk_target.eq(tag::pk)))
                .or(tag_alias::fk_source
                    .eq(tag::pk)
                    .and(tag_alias::fk_target.eq(tag_pk))),
            ),
        ))
        .load::<Tag>(connection)
}

pub fn add_tag_aliases(
    tag: &Tag,
    alias_pks: &Vec<i32>,
    connection: &DbConnection,
) -> Result<(), TransactionRuntimeError> {
    diesel::sql_query(
        r#"
        INSERT INTO tag_alias
        SELECT $1, pk
        FROM tag WHERE pk = ANY($2) OR EXISTS(
            SELECT * FROM tag_alias AS inner_alias
            WHERE fk_source = ANY($2) OR fk_target = ANY($2)
        )
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind::<Integer, _>(tag.pk)
    .bind::<Array<Integer>, _>(alias_pks)
    .execute(connection)
    .map_err(retry_on_constraint_violation)?;

    Ok(())
}

pub fn add_tag_parents(
    tag_pk: i32,
    parent_pks: &Vec<i32>,
    connection: &DbConnection,
) -> Result<(), TransactionRuntimeError> {
    // avoiding cycles:
    // for `tag`, remove all edges to parents that are children of any of the added parents (or an added parent itself, in case an added parent already is a direct parent)
    // for all added parents, remove edges to parents that are children of `tag`
    let mut delete_cyclic_edges_query = String::from(
        r#"
            DELETE FROM tag_edge
            WHERE (
                fk_child = $2
                AND fk_parent IN(SELECT fk_child FROM tag_closure_table WHERE fk_parent = ANY($1))
            )
        "#,
    );

    for parent_pk in parent_pks.iter() {
        // remove edges to parents that are a child of `tag_pk` (or `tag_pk` itself)
        write!(delete_cyclic_edges_query, " OR (fk_child = {parent_pk} AND fk_parent IN(SELECT fk_child FROM tag_closure_table WHERE fk_parent = {tag_pk}))").map_err(|e| Error::StdError(e.to_string()))?;
    }

    diesel::sql_query(delete_cyclic_edges_query)
        .bind::<Array<Integer>, _>(parent_pks)
        .bind::<Integer, _>(tag_pk)
        .execute(connection)
        .map_err(retry_on_constraint_violation)?;

    let edges_to_insert = parent_pks
        .iter()
        .map(|parent_pk| TagEdge {
            fk_parent: *parent_pk,
            fk_child: tag_pk,
        })
        .collect::<Vec<_>>();

    diesel::insert_into(tag_edge::table)
        .values(edges_to_insert)
        .execute(connection)
        .map_err(retry_on_constraint_violation)?;

    Ok(())
}

pub fn remove_tags_from_hierarchy(
    tags: &Vec<i32>,
    connection: &DbConnection,
) -> Result<usize, diesel::result::Error> {
    diesel::sql_query(
        r#"
        DELETE FROM tag_edge
        WHERE fk_parent = ANY($1)
        OR fk_child = ANY($1)
    "#,
    )
    .bind::<Array<Integer>, _>(tags)
    .execute(connection)
}
