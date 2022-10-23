use std::{collections::HashMap, fmt::Write};

use chrono::{DateTime, Utc};
use diesel::{
    connection::LoadConnection,
    dsl::{exists, not},
    pg::Pg,
    sql_types::{Array, Integer},
    BoolExpressionMethods, Connection, OptionalExtension,
};
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError};
use warp::{Rejection, Reply};

use crate::{
    acquire_db_connection,
    diesel::{
        ExpressionMethods, NullableExpressionMethods, QueryDsl, RunQueryDsl, TextExpressionMethods,
    },
    error::{Error, TransactionRuntimeError},
    model::{
        NewPost, NewTag, Post, PostGroupAccess, PostTag, PostUpdateOptional, S3Object, Tag,
        TagClosureTable, TagEdge, User, UserGroup,
    },
    perms,
    query::{functions::*, PostDetailed},
    retry_on_constraint_violation, run_retryable_transaction,
    schema::{
        post, post_group_access, post_tag, s3_object, tag, tag_alias, tag_closure_table, tag_edge,
        user_group, user_group_membership,
    },
};

macro_rules! report_missing_pks {
    ($tab:ident, $pks:expr, $connection:expr) => {
        $tab::table
            .select($tab::pk)
            .filter($tab::pk.eq_any($pks))
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

macro_rules! load_and_report_missing_pks {
    ($return_type:ident, $tab:ident, $pks:expr, $connection:expr) => {{
        let found = $tab::table
            .filter($tab::pk.eq_any($pks))
            .load::<$return_type>($connection)
            .map_err(crate::Error::from)?;

        let found_pks = found
            .iter()
            .map(|f| f.pk)
            .collect::<std::collections::HashSet<_>>();
        let missing_pks = $pks
            .iter()
            .filter(|pk| !found_pks.contains(pk))
            .collect::<Vec<_>>();
        if missing_pks.is_empty() {
            Ok(found)
        } else {
            Err(crate::Error::InvalidEntityReferenceError(
                itertools::Itertools::intersperse(
                    missing_pks.into_iter().map(i32::to_string),
                    String::from(", "),
                )
                .collect::<String>(),
            ))
        }
    }};
}

fn report_inaccessible_groups<C: Connection<Backend = Pg> + LoadConnection>(
    selected_group_access: &[GrantedPostGroupAccess],
    user: &User,
    connection: &mut C,
) -> Result<(), Error> {
    let group_pks = selected_group_access
        .iter()
        .map(|group_access| group_access.group_pk)
        .collect::<Vec<_>>();
    report_inaccessible_group_pks(&group_pks, user, connection)
}

fn report_inaccessible_group_pks<C: Connection<Backend = Pg> + LoadConnection>(
    group_pks: &[i32],
    user: &User,
    connection: &mut C,
) -> Result<(), Error> {
    let accessible_group_pks = user_group::table
        .select(user_group::pk)
        .filter(
            user_group::pk.eq_any(group_pks).and(
                user_group::fk_owner.nullable().eq(user.pk).or(exists(
                    user_group_membership::table.filter(
                        user_group_membership::fk_group
                            .eq(user_group::pk)
                            .and(user_group_membership::fk_user.nullable().eq(user.pk)),
                    ),
                )),
            ),
        )
        .load::<i32>(connection)
        .map_err(|e| Error::QueryError(e.to_string()))?;

    let missing_pks = group_pks
        .iter()
        .filter(|pk| !accessible_group_pks.contains(pk))
        .collect::<Vec<_>>();

    if !missing_pks.is_empty() {
        Err(Error::InvalidEntityReferenceError(
            itertools::Itertools::intersperse(
                missing_pks.into_iter().map(i32::to_string),
                String::from(", "),
            )
            .collect::<String>(),
        ))
    } else {
        Ok(())
    }
}

fn validate_tags(tags: &[String]) -> Result<(), ValidationError> {
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

fn dedup_vec<T: PartialEq + Ord>(vec: &mut Option<Vec<T>>) {
    if let Some(vec) = vec {
        vec.sort_unstable();
        vec.dedup();
    }
}

fn dedup_vecs<T: PartialEq + Ord>(v1: &mut Option<Vec<T>>, v2: &Option<Vec<T>>) {
    if let Some(v1) = v1 {
        if let Some(v2) = v2 {
            v1.retain(|e| !v2.contains(e));
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Copy, Eq)]
pub struct GrantedPostGroupAccess {
    pub group_pk: i32,
    pub write: bool,
}

impl PartialEq for GrantedPostGroupAccess {
    fn eq(&self, other: &Self) -> bool {
        self.group_pk == other.group_pk
    }
}

impl PartialOrd for GrantedPostGroupAccess {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.group_pk.partial_cmp(&other.group_pk)
    }
}

impl Ord for GrantedPostGroupAccess {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.group_pk.cmp(&other.group_pk)
    }
}

#[derive(Serialize)]
pub struct PostGroupAccessDetailed {
    pub fk_post: i32,
    pub write: bool,
    pub fk_granted_by: i32,
    pub creation_timestamp: DateTime<Utc>,
    pub granted_group: UserGroup,
}

#[derive(Deserialize, Validate)]
pub struct CreatePostRequest {
    #[validate(url)]
    pub data_url: Option<String>,
    #[validate(url)]
    pub source_url: Option<String>,
    #[validate(length(max = 300))]
    pub title: Option<String>,
    #[validate(length(max = 100), custom = "validate_tags")]
    pub entered_tags: Option<Vec<String>>,
    #[validate(length(max = 100))]
    pub selected_tags: Option<Vec<i32>>,
    pub s3_object: Option<String>,
    #[validate(url)]
    pub thumbnail_url: Option<String>,
    pub is_public: Option<bool>,
    pub public_edit: Option<bool>,
    #[validate(length(max = 50))]
    pub group_access: Option<Vec<GrantedPostGroupAccess>>,
    #[validate(length(max = 30000))]
    pub description: Option<String>,
}

pub async fn create_post_handler(
    mut create_post_request: CreatePostRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    create_post_request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreatePostRequest: {}",
            e
        )))
    })?;

    dedup_vec(&mut create_post_request.entered_tags);
    dedup_vec(&mut create_post_request.selected_tags);
    dedup_vec(&mut create_post_request.group_access);

    // cannot use hashset because it is not supported as diesel expression
    let tags = create_post_request.entered_tags.map(sanitize_request_tags);

    let mut connection = acquire_db_connection()?;

    if let Some(ref group_access) = create_post_request.group_access {
        if !group_access.is_empty() {
            report_inaccessible_groups(group_access, &user, &mut connection)?;
        }
    }

    // run as repeatable read transaction and retry serialisation errors when a concurrent transaction
    // deletes or creates relevant tags
    run_retryable_transaction(&mut connection, |connection| {
        if let Some(ref selected_tags) = create_post_request.selected_tags {
            report_missing_pks!(tag, selected_tags, connection)??;
        }

        let mut set_tags = if let Some(ref tags) = tags {
            let (mut set_tags, created_tags) = get_or_create_tags(connection, tags)?;
            set_tags.extend(created_tags);
            set_tags
        } else {
            Vec::new()
        };

        if let Some(ref selected_tags) = create_post_request.selected_tags {
            let loaded_selected_tags =
                load_and_report_missing_pks!(Tag, tag, selected_tags, connection)?;
            set_tags.extend(loaded_selected_tags);
        }

        if !set_tags.is_empty() {
            filter_redundant_tags(&mut set_tags, connection)?;
            if set_tags.len() > 100 {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InvalidRequestInputError(format!(
                        "Cannot supply more than 100 tags, supplied: {}",
                        set_tags.len()
                    )),
                ));
            }
        }

        let now = Utc::now();
        let post = diesel::insert_into(post::table)
            .values(NewPost {
                data_url: create_post_request.data_url.clone(),
                source_url: create_post_request.source_url.clone(),
                title: create_post_request.title.clone(),
                creation_timestamp: now,
                fk_create_user: user.pk,
                score: 0,
                s3_object: create_post_request.s3_object.clone(),
                thumbnail_url: create_post_request.thumbnail_url.clone(),
                public: create_post_request.is_public.unwrap_or(false),
                public_edit: create_post_request.public_edit.unwrap_or(false),
                description: create_post_request.description.clone(),
            })
            .get_result::<Post>(connection)?;

        let post_tags = set_tags
            .iter()
            .map(|tag| PostTag {
                fk_post: post.pk,
                fk_tag: tag.pk,
            })
            .collect::<Vec<_>>();

        if !post_tags.is_empty() {
            diesel::insert_into(post_tag::table)
                .values(&post_tags)
                .execute(connection)?;
        }

        if let Some(ref group_access) = create_post_request.group_access {
            if !group_access.is_empty() {
                let group_pks = group_access.iter().map(|g| g.group_pk).collect::<Vec<_>>();
                report_missing_pks!(user_group, &group_pks, connection)??;
                let post_group_access = group_access
                    .iter()
                    .map(|g| PostGroupAccess {
                        fk_post: post.pk,
                        fk_granted_group: g.group_pk,
                        write: g.write,
                        fk_granted_by: user.pk,
                        creation_timestamp: now,
                    })
                    .collect::<Vec<_>>();

                diesel::insert_into(post_group_access::table)
                    .values(&post_group_access)
                    .execute(connection)?;
            }
        }

        Ok(warp::reply::json(&post))
    })
    .map_err(warp::reject::custom)
}

#[derive(Deserialize, Validate)]
pub struct EditPostRequest {
    #[validate(length(max = 100), custom = "validate_tags")]
    pub tags_overwrite: Option<Vec<String>>,
    #[validate(length(max = 100))]
    pub tag_pks_overwrite: Option<Vec<i32>>,
    pub removed_tag_pks: Option<Vec<i32>>,
    #[validate(length(max = 100))]
    pub added_tag_pks: Option<Vec<i32>>,
    #[validate(length(max = 100), custom = "validate_tags")]
    pub added_tags: Option<Vec<String>>,
    #[validate(url)]
    pub data_url: Option<String>,
    #[validate(url)]
    pub source_url: Option<String>,
    #[validate(length(max = 300))]
    pub title: Option<String>,
    pub is_public: Option<bool>,
    pub public_edit: Option<bool>,
    #[validate(length(max = 30000))]
    pub description: Option<String>,
    #[validate(length(max = 50))]
    pub group_access_overwrite: Option<Vec<GrantedPostGroupAccess>>,
    #[validate(length(max = 50))]
    pub added_group_access: Option<Vec<GrantedPostGroupAccess>>,
    pub removed_group_access: Option<Vec<i32>>,
}

pub async fn edit_post_handler(
    mut request: EditPostRequest,
    post_pk: i32,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for EditPostRequest: {}",
            e
        )))
    })?;

    dedup_vec(&mut request.tags_overwrite);
    dedup_vec(&mut request.tag_pks_overwrite);
    dedup_vec(&mut request.removed_tag_pks);
    dedup_vec(&mut request.added_tag_pks);
    dedup_vec(&mut request.added_tags);
    dedup_vec(&mut request.removed_group_access);
    dedup_vec(&mut request.group_access_overwrite);
    dedup_vec(&mut request.added_group_access);
    dedup_vec(&mut request.removed_group_access);
    dedup_vecs(&mut request.added_tag_pks, &request.tag_pks_overwrite);
    dedup_vecs(&mut request.added_tags, &request.tags_overwrite);
    dedup_vecs(
        &mut request.added_group_access,
        &request.group_access_overwrite,
    );

    let mut connection = acquire_db_connection()?;

    if let Some(ref group_access) = request.group_access_overwrite {
        if !group_access.is_empty() {
            report_inaccessible_groups(group_access, &user, &mut connection)?;
        }
    }

    if let Some(ref group_access) = request.added_group_access {
        if !group_access.is_empty() {
            report_inaccessible_groups(group_access, &user, &mut connection)?;
        }
    }

    if let Some(ref removed_group_access) = request.removed_group_access {
        if !removed_group_access.is_empty() {
            report_inaccessible_group_pks(removed_group_access, &user, &mut connection)?;
        }
    }

    let post = run_retryable_transaction(&mut connection, |connection| {
        if !perms::is_post_editable(connection, Some(&user), post_pk)? {
            return Err(TransactionRuntimeError::Rollback(
                Error::InaccessibleObjectError(post_pk),
            ));
        }

        let mut added_tags = request.added_tags.clone();
        let mut added_tag_pks = request.added_tag_pks.clone();
        let mut removed_tag_pks = request.removed_tag_pks.clone();

        if request.tags_overwrite.is_some() || request.tag_pks_overwrite.is_some() {
            diesel::delete(post_tag::table.filter(post_tag::fk_post.eq(post_pk)))
                .execute(connection)?;
            if let Some(ref tags_overwrite) = request.tags_overwrite {
                match added_tags {
                    Some(ref mut added_tags) => added_tags.append(&mut tags_overwrite.clone()),
                    None => added_tags = Some(tags_overwrite.clone()),
                }
            }
            if let Some(ref tag_pks_overwrite) = request.tag_pks_overwrite {
                match added_tag_pks {
                    Some(ref mut added_tag_pks) => {
                        added_tag_pks.append(&mut tag_pks_overwrite.clone())
                    }
                    None => added_tag_pks = Some(tag_pks_overwrite.clone()),
                }
            }
        }

        if let Some(ref added_tags) = added_tags {
            let (existing_tags, created_tags) = get_or_create_tags(connection, added_tags)?;
            match added_tag_pks {
                Some(ref mut added_tag_pks) => {
                    existing_tags
                        .iter()
                        .for_each(|tag| added_tag_pks.push(tag.pk));
                    created_tags
                        .iter()
                        .for_each(|tag| added_tag_pks.push(tag.pk));
                }
                None => {
                    let mut vec = Vec::with_capacity(existing_tags.len() + created_tags.len());
                    existing_tags.iter().for_each(|tag| vec.push(tag.pk));
                    created_tags.iter().for_each(|tag| vec.push(tag.pk));
                    added_tag_pks = Some(vec);
                }
            }
        }

        if let Some(ref added_tag_pks) = added_tag_pks {
            if !added_tag_pks.is_empty() {
                let mut loaded_tags =
                    load_and_report_missing_pks!(Tag, tag, added_tag_pks, connection)?;
                let curr_tags = get_post_tags(post_pk, connection)?;
                let curr_tag_pks = curr_tags.iter().map(|tag| tag.pk).collect::<Vec<_>>();
                loaded_tags.extend(curr_tags);
                filter_redundant_tags(&mut loaded_tags, connection)?;

                // remove current tags that are now redundant, that means remove all currently set tags that have been removed by filter_redundant_tags
                let mut curr_tag_pks_to_remove = curr_tag_pks
                    .into_iter()
                    .filter(|tag_pk| !loaded_tags.iter().any(|tag| tag.pk == *tag_pk))
                    .collect::<Vec<_>>();

                if !curr_tag_pks_to_remove.is_empty() {
                    match removed_tag_pks {
                        Some(ref mut removed_tag_pks) => {
                            removed_tag_pks.append(&mut curr_tag_pks_to_remove)
                        }
                        None => removed_tag_pks = Some(curr_tag_pks_to_remove),
                    }
                }

                let new_post_tags = added_tag_pks
                    .iter()
                    .filter(|tag_pk| loaded_tags.iter().any(|tag| tag.pk == **tag_pk))
                    .map(|tag_pk| PostTag {
                        fk_post: post_pk,
                        fk_tag: *tag_pk,
                    })
                    .collect::<Vec<_>>();

                if !new_post_tags.is_empty() {
                    diesel::insert_into(post_tag::table)
                        .values(&new_post_tags)
                        .on_conflict_do_nothing()
                        .execute(connection)?;
                }
            }
        }

        if let Some(ref removed_tag_pks) = removed_tag_pks {
            if !removed_tag_pks.is_empty() {
                diesel::delete(
                    post_tag::table.filter(
                        post_tag::fk_post
                            .eq(post_pk)
                            .and(post_tag::fk_tag.eq_any(removed_tag_pks)),
                    ),
                )
                .execute(connection)?;
            }
        }

        if added_tag_pks
            .as_ref()
            .map(|v| !v.is_empty())
            .unwrap_or(false)
        {
            let curr_tag_count = post_tag::table
                .filter(post_tag::fk_post.eq(post_pk))
                .count()
                .get_result::<i64>(connection)?;

            if curr_tag_count > 100 {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InvalidRequestInputError(format!(
                        "Cannot supply more than 100 tags, supplied: {}",
                        curr_tag_count
                    )),
                ));
            }
        }

        if request.group_access_overwrite.is_some() || request.added_group_access.is_some() {
            let curr_group_access = post_group_access::table
                .filter(post_group_access::fk_post.eq(post_pk))
                .load::<PostGroupAccess>(connection)?;

            let mut added_group_access = request.added_group_access.clone().unwrap_or_default();

            if let Some(ref group_access_overwrite) = request.group_access_overwrite {
                let group_access_overwrite_pks = group_access_overwrite
                    .iter()
                    .map(|group_access| group_access.group_pk)
                    .collect::<Vec<_>>();
                let relevant_group_access_overwrite = group_access_overwrite
                    .iter()
                    .filter(|group_access| {
                        !curr_group_access.iter().any(|curr_group| {
                            curr_group.fk_granted_group == group_access.group_pk
                                && curr_group.write == group_access.write
                        })
                    })
                    .map(|group_access| group_access.group_pk)
                    .collect::<Vec<_>>();

                diesel::delete(
                    post_group_access::table.filter(
                        post_group_access::fk_post.eq(post_pk).and(
                            not(post_group_access::fk_granted_group
                                .eq_any(&group_access_overwrite_pks))
                            .or(post_group_access::fk_granted_group
                                .eq_any(&relevant_group_access_overwrite)),
                        ),
                    ),
                )
                .execute(connection)?;

                group_access_overwrite
                    .iter()
                    .filter(|group_access| {
                        relevant_group_access_overwrite.contains(&group_access.group_pk)
                    })
                    .for_each(|group_access| added_group_access.push(*group_access));
            }

            if !added_group_access.is_empty() {
                let group_pks = added_group_access
                    .iter()
                    .map(|g| g.group_pk)
                    .collect::<Vec<_>>();
                report_missing_pks!(user_group, &group_pks, connection)??;

                let new_post_group_access = added_group_access
                    .iter()
                    .map(|group_access| PostGroupAccess {
                        fk_post: post_pk,
                        fk_granted_group: group_access.group_pk,
                        write: group_access.write,
                        fk_granted_by: user.pk,
                        creation_timestamp: Utc::now(),
                    })
                    .collect::<Vec<_>>();

                diesel::insert_into(post_group_access::table)
                    .values(&new_post_group_access)
                    .on_conflict_do_nothing()
                    .execute(connection)?;
            }
        }

        if let Some(ref removed_group_access) = request.removed_group_access {
            if !removed_group_access.is_empty() {
                diesel::delete(
                    post_group_access::table.filter(
                        post_group_access::fk_post
                            .eq(post_pk)
                            .and(post_group_access::fk_granted_group.eq_any(removed_group_access)),
                    ),
                )
                .execute(connection)?;
            }
        }

        let update = PostUpdateOptional {
            data_url: request.data_url.clone(),
            source_url: request.source_url.clone(),
            title: request.title.clone(),
            public: request.is_public,
            public_edit: request.public_edit,
            description: request.description.clone(),
        };

        if update.has_changes() {
            let updated_post = diesel::update(post::table)
                .filter(post::pk.eq(post_pk))
                .set(PostUpdateOptional {
                    data_url: request.data_url.clone(),
                    source_url: request.source_url.clone(),
                    title: request.title.clone(),
                    public: request.is_public,
                    public_edit: request.public_edit,
                    description: request.description.clone(),
                })
                .get_result::<Post>(connection)?;

            Ok(updated_post)
        } else {
            let loaded_post = post::table
                .filter(post::pk.eq(post_pk))
                .get_result::<Post>(connection)?;

            Ok(loaded_post)
        }
    })?;

    let tags = get_post_tags(post_pk, &mut connection).map_err(Error::from)?;
    let group_access = get_post_group_access(post_pk, &mut connection).map_err(Error::from)?;
    let s3_object = if let Some(ref s3_object_key) = post.s3_object {
        Some(
            s3_object::table
                .filter(s3_object::object_key.eq(s3_object_key))
                .get_result::<S3Object>(&mut connection)
                .map_err(Error::from)?,
        )
    } else {
        None
    };

    Ok(warp::reply::json(&PostDetailed {
        pk: post.pk,
        data_url: post.data_url,
        source_url: post.source_url,
        title: post.title,
        creation_timestamp: post.creation_timestamp,
        fk_create_user: post.fk_create_user,
        score: post.score,
        s3_object,
        thumbnail_url: post.thumbnail_url,
        prev_post_pk: None,
        next_post_pk: None,
        public: post.public,
        public_edit: post.public_edit,
        description: post.description,
        is_editable: true,
        tags,
        group_access,
    }))
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
    let mut connection = acquire_db_connection()?;
    run_retryable_transaction(&mut connection, |connection| {
        let (existing_tags, inserted_tags) = get_or_create_tags(connection, &tag_names)?;
        Ok(warp::reply::json(&CreateTagsResponse {
            existing_tags,
            inserted_tags,
        }))
    })
    .map_err(warp::reject::custom)
}

/// Get and create all tags for the supplied tag names, returning a tuple of all existing and all created tags.
pub fn get_or_create_tags<C: Connection<Backend = Pg> + LoadConnection>(
    connection: &mut C,
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

    Ok((set_tags, created_tags))
}

/// Filter redundant tags by removing tags that are a parent of another included tag or a shorter alias
/// for another included tag.
pub fn filter_redundant_tags<C: Connection<Backend = Pg> + LoadConnection>(
    tags: &mut Vec<Tag>,
    connection: &mut C,
) -> Result<(), Error> {
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

pub fn get_tag_hierarchy_information<C: Connection<Backend = Pg> + LoadConnection>(
    tag: &Tag,
    connection: &mut C,
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

/// Creates a tag with the given tag_name, parent and aliases. If the tag already exists, the existing tag is updated
/// and the given parents and aliases are added. Note that added aliases are removed from their pre-existing parent-child
/// hierarchy and added to the hierarchy of the given tag instead, setting all parents of the tag as the parents of all aliases
/// and setting all children of the tag as children of all aliases.
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

    let mut connection = acquire_db_connection()?;

    if let Some(ref parent_pks) = upsert_tag_request.parent_pks {
        report_missing_pks!(tag, parent_pks, &mut connection)??;
    }

    if let Some(ref alias_pks) = upsert_tag_request.alias_pks {
        report_missing_pks!(tag, alias_pks, &mut connection)??;
    }

    run_retryable_transaction(&mut connection, |connection| {
        let (inserted, tag) = get_or_create_tag(&tag_name, connection)?;

        let parents_to_set = if let Some(ref parent_pks) = upsert_tag_request.parent_pks {
            if parent_pks.iter().any(|parent_pk| *parent_pk == tag.pk) {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InvalidRequestInputError(format!(
                        "Cannot set tag {} as its own parent",
                        tag.pk
                    )),
                ));
            }

            let curr_parents = get_tag_parents_pks(tag.pk, connection)?;
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

            add_tag_parents(tag.pk, &parents_to_set, connection)?;
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

                let curr_aliases = get_tag_aliases_pks(tag.pk, connection)?;
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
                remove_tags_from_hierarchy(&aliases_to_set, connection)?;
                add_tag_aliases(&tag, &aliases_to_set, connection)?;
                Some(aliases_to_set)
            }
            _ => None,
        };

        if aliases_to_set.as_ref().map_or(false, |pks| !pks.is_empty())
            || parents_to_set.as_ref().map_or(false, |pks| !pks.is_empty())
        {
            log::debug!("syncing parents with aliases");
            let curr_aliases = get_tag_aliases(tag.pk, connection)?;
            // add current parents of tag as parents for all current aliases if either changed
            let curr_parents = get_tag_parents_pks(tag.pk, connection)?;
            log::debug!("curr parents: {:?}", &curr_parents);
            if !curr_parents.is_empty() {
                for alias in curr_aliases.iter() {
                    log::debug!(
                        "updating alias: setting {:?} as parents of alias {}",
                        &curr_parents,
                        alias.pk
                    );
                    add_tag_parents(alias.pk, &curr_parents, connection)?;
                }
            }

            let curr_children = get_tag_children_pks(tag.pk, connection)?;
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
                    add_tag_parents(child, &curr_alias_pks, connection)?;
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

#[derive(Serialize)]
pub struct FindTagResponse {
    exact_match: Option<Tag>,
    suggestions: Vec<Tag>,
}

/// Find tags that match or start with the provided prefix tag_name. Returns the exact match and / or up to ten
/// suggestions starting with the provided prefix, ordered by length of tag_name.
pub async fn find_tag_handler(tag_name: String) -> Result<impl Reply, Rejection> {
    if tag_name.is_empty() {
        return Ok(warp::reply::json(&FindTagResponse {
            exact_match: None,
            suggestions: Vec::new(),
        }));
    }

    let tag_name = percent_encoding::percent_decode(tag_name.as_bytes())
        .decode_utf8()
        .map_err(|_| Error::UtfEncodingError)?;

    let mut connection = acquire_db_connection()?;
    let mut found_tags = tag::table
        .filter(lower(tag::tag_name).like(format!("{}%", tag_name.to_lowercase())))
        .order_by((char_length(tag::tag_name).asc(), tag::tag_name.asc()))
        .limit(10)
        .load::<Tag>(&mut connection)
        .map_err(Error::from)?;

    if found_tags.is_empty() {
        Ok(warp::reply::json(&FindTagResponse {
            exact_match: None,
            suggestions: found_tags,
        }))
    } else if found_tags[0].tag_name.len() == tag_name.len() {
        Ok(warp::reply::json(&FindTagResponse {
            exact_match: Some(found_tags.remove(0)),
            suggestions: found_tags,
        }))
    } else {
        Ok(warp::reply::json(&FindTagResponse {
            exact_match: None,
            suggestions: found_tags,
        }))
    }
}

pub fn get_or_create_tag<C: Connection<Backend = Pg> + LoadConnection>(
    tag_name: &str,
    connection: &mut C,
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

pub fn get_tag_parents_pks<C: Connection<Backend = Pg> + LoadConnection>(
    tag_pk: i32,
    connection: &mut C,
) -> Result<Vec<i32>, diesel::result::Error> {
    tag_edge::table
        .select(tag_edge::fk_parent)
        .filter(tag_edge::fk_child.eq(tag_pk))
        .load::<i32>(connection)
}

pub fn get_tag_children_pks<C: Connection<Backend = Pg> + LoadConnection>(
    tag_pk: i32,
    connection: &mut C,
) -> Result<Vec<i32>, diesel::result::Error> {
    tag_edge::table
        .select(tag_edge::fk_child)
        .filter(tag_edge::fk_parent.eq(tag_pk))
        .load::<i32>(connection)
}

pub fn get_tag_aliases_pks<C: Connection<Backend = Pg> + LoadConnection>(
    tag_pk: i32,
    connection: &mut C,
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

pub fn get_tag_aliases<C: Connection<Backend = Pg> + LoadConnection>(
    tag_pk: i32,
    connection: &mut C,
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

pub fn add_tag_aliases<C: Connection<Backend = Pg>>(
    tag: &Tag,
    alias_pks: &[i32],
    connection: &mut C,
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

pub fn add_tag_parents<C: Connection<Backend = Pg>>(
    tag_pk: i32,
    parent_pks: &[i32],
    connection: &mut C,
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

pub fn remove_tags_from_hierarchy<C: Connection<Backend = Pg>>(
    tags: &[i32],
    connection: &mut C,
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

pub fn get_post_tags<C: Connection<Backend = Pg> + LoadConnection>(
    post_pk: i32,
    connection: &mut C,
) -> Result<Vec<Tag>, diesel::result::Error> {
    tag::table
        .filter(exists(
            post_tag::table.filter(
                post_tag::fk_post
                    .eq(post_pk)
                    .and(post_tag::fk_tag.eq(tag::pk)),
            ),
        ))
        .load::<Tag>(connection)
}

pub fn get_post_group_access<C: Connection<Backend = Pg> + LoadConnection>(
    post_pk: i32,
    connection: &mut C,
) -> Result<Vec<PostGroupAccessDetailed>, diesel::result::Error> {
    post_group_access::table
        .inner_join(user_group::table)
        .filter(post_group_access::fk_post.eq(post_pk))
        .load::<(PostGroupAccess, UserGroup)>(connection)
        .map(|post_group_access_vec| {
            post_group_access_vec
                .into_iter()
                .map(|post_group_access| PostGroupAccessDetailed {
                    fk_post: post_group_access.0.fk_post,
                    write: post_group_access.0.write,
                    fk_granted_by: post_group_access.0.fk_granted_by,
                    creation_timestamp: post_group_access.0.creation_timestamp,
                    granted_group: post_group_access.1,
                })
                .collect::<Vec<_>>()
        })
}
