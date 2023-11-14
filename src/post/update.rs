use chrono::Utc;
use diesel::{
    dsl::{self, exists, not},
    sql_types::BigInt,
    upsert::excluded,
    BoolExpressionMethods, ExpressionMethods, NullableExpressionMethods, OptionalExtension,
    QueryDsl,
};
use diesel_async::{scoped_futures::ScopedFutureExt, RunQueryDsl};
use serde::Deserialize;
use validator::Validate;
use warp::{reject::Rejection, reply::Reply};

use crate::{
    acquire_db_connection,
    diesel::Table,
    error::{Error, TransactionRuntimeError},
    model::{
        NewPostCollectionItem, Post, PostCollection, PostCollectionGroupAccess, PostCollectionTag,
        PostCollectionUpdateOptional, PostGroupAccess, PostTag, PostUpdateOptional, S3Object, Tag,
        User,
    },
    perms,
    post::{load_post_collection_poster_object, report_inaccessible_post_pks},
    query::{self, load_and_report_missing_pks, PostCollectionDetailed, PostDetailed},
    retry_on_constraint_violation, run_serializable_transaction,
    schema::{
        post, post_collection, post_collection_group_access, post_collection_item,
        post_collection_tag, post_group_access, post_tag, registered_user, s3_object, tag,
        user_group, user_group_membership,
    },
    tags::{
        filter_redundant_tags, get_or_create_tags, get_source_object_tag, sanitize_request_tags,
        validate_tags,
    },
    util::{dedup_vec, dedup_vec_optional, dedup_vecs_optional},
};

use super::{
    get_post_collection_group_access, get_post_collection_tags, get_post_group_access,
    get_post_tags, report_inaccessible_group_pks, GroupAccessDefinition,
};

macro_rules! handle_object_tag_update {
    ($source_object_pk:expr, $tag_relation_entity:ident, $tag_relation_table:ident, $fk_source_object:ident, $tags_overwrite:expr, $tag_pks_overwrite:expr, $added_tags:expr, $added_tag_pks:expr, $removed_tag_pks:expr, $connection:expr) => {
        if $tags_overwrite.is_some() || $tag_pks_overwrite.is_some() {
            diesel::delete(
                $tag_relation_table::table.filter($tag_relation_table::$fk_source_object.eq($source_object_pk)),
            )
            .execute($connection)
            .await?;
            if let Some(ref tags_overwrite) = $tags_overwrite {
                match $added_tags {
                    Some(ref mut added_tags) => added_tags.append(&mut tags_overwrite.clone()),
                    None => $added_tags = Some(tags_overwrite.clone()),
                }
            }
            if let Some(ref tag_pks_overwrite) = $tag_pks_overwrite {
                match $added_tag_pks {
                    Some(ref mut added_tag_pks) => added_tag_pks.append(&mut tag_pks_overwrite.clone()),
                    None => $added_tag_pks = Some(tag_pks_overwrite.clone()),
                }
            }
        }

        if let Some(ref added_tags) = $added_tags {
            let mut tag_names = sanitize_request_tags(added_tags);
            dedup_vec(&mut tag_names);
            let (existing_tags, created_tags) = get_or_create_tags($connection, &tag_names).await?;
            match $added_tag_pks {
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
                    $added_tag_pks = Some(vec);
                }
            }
        }

        if let Some(ref added_tag_pks) = $added_tag_pks {
            if !added_tag_pks.is_empty() {
                let mut loaded_tags =
                    load_and_report_missing_pks!(Tag, tag, added_tag_pks, $connection)?;
                let curr_tags = get_source_object_tag!(
                    $tag_relation_table,
                    $source_object_pk,
                    $tag_relation_table::$fk_source_object,
                    $connection
                )
                .await?;
                let curr_tag_pks = curr_tags.iter().map(|tag| tag.pk).collect::<Vec<_>>();
                loaded_tags.extend(curr_tags);
                filter_redundant_tags(&mut loaded_tags, $connection).await?;

                // remove current tags that are now redundant, that means remove all currently set tags that have been removed by filter_redundant_tags
                let mut curr_tag_pks_to_remove = curr_tag_pks
                    .into_iter()
                    .filter(|tag_pk| !loaded_tags.iter().any(|tag| tag.pk == *tag_pk))
                    .collect::<Vec<_>>();

                if !curr_tag_pks_to_remove.is_empty() {
                    match $removed_tag_pks {
                        Some(ref mut removed_tag_pks) => {
                            removed_tag_pks.append(&mut curr_tag_pks_to_remove)
                        }
                        None => $removed_tag_pks = Some(curr_tag_pks_to_remove),
                    }
                }

                let new_object_tags = added_tag_pks
                    .iter()
                    .filter(|tag_pk| loaded_tags.iter().any(|tag| tag.pk == **tag_pk))
                    .map(|tag_pk| $tag_relation_entity {
                        $fk_source_object: $source_object_pk,
                        fk_tag: *tag_pk
                    })
                    .collect::<Vec<_>>();

                if !new_object_tags.is_empty() {
                    diesel::insert_into($tag_relation_table::table)
                        .values(&new_object_tags)
                        .on_conflict_do_nothing()
                        .execute($connection)
                        .await?;
                }
            }
        }

        if let Some(ref removed_tag_pks) = $removed_tag_pks {
            if !removed_tag_pks.is_empty() {
                diesel::delete(
                    $tag_relation_table::table.filter(
                        $tag_relation_table::$fk_source_object
                            .eq($source_object_pk)
                            .and($tag_relation_table::fk_tag.eq_any(removed_tag_pks)),
                    ),
                )
                .execute($connection)
                .await?;
            }
        }

        if $added_tag_pks
            .as_ref()
            .map(|v| !v.is_empty())
            .unwrap_or(false)
        {
            let curr_tag_count = $tag_relation_table::table
                .filter($tag_relation_table::$fk_source_object.eq($source_object_pk))
                .count()
                .get_result::<i64>($connection)
                .await?;

            if curr_tag_count > 100 {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InvalidRequestInputError(format!(
                        "Cannot supply more than 100 tags, supplied: {}",
                        curr_tag_count
                    )),
                ));
            }
        }
    };
}

macro_rules! handle_object_group_access_update {
    ($source_object_pk:expr, $group_access_relation_entity:ident, $group_access_relation_table:ident, $fk_source_object:ident, $group_access_overwrite:expr, $added_group_access:expr, $removed_group_access:expr, $user:expr, $connection:expr) => {
        let added_groups = if $group_access_overwrite.is_some()
                || $added_group_access.is_some()
            {
                // load groups that are visible to the current user and make sure to only overwrite those
                // otherwise, groups that are not visible to the client would get removed every time when overwriting the groups
                let curr_visible_group_access = $group_access_relation_table::table
                    .inner_join(user_group::table)
                    .select($group_access_relation_table::table::all_columns())
                    .filter($group_access_relation_table::$fk_source_object.eq($source_object_pk).and(
                        not(user_group::hidden).or(perms::get_group_membership_condition!($user.pk)),
                    ))
                    .load::<$group_access_relation_entity>($connection)
                    .await?;

                let mut added_group_access = $added_group_access.clone().unwrap_or_default();

                if let Some(ref group_access_overwrite) = $group_access_overwrite {
                    let curr_visible_group_access_pks = curr_visible_group_access
                        .iter()
                        .map(|group_access| group_access.fk_granted_group)
                        .collect::<Vec<_>>();
                    let group_access_overwrite_pks = group_access_overwrite
                        .iter()
                        .map(|group_access| group_access.group_pk)
                        .collect::<Vec<_>>();
                    let relevant_group_access_overwrite = group_access_overwrite
                        .iter()
                        .filter(|group_access| {
                            !curr_visible_group_access.iter().any(|curr_group| {
                                curr_group.fk_granted_group == group_access.group_pk
                                    && curr_group.write == group_access.write
                            })
                        })
                        .map(|group_access| group_access.group_pk)
                        .collect::<Vec<_>>();

                    diesel::delete(
                        $group_access_relation_table::table.filter(
                            $group_access_relation_table::$fk_source_object
                                .eq($source_object_pk)
                                .and(
                                    $group_access_relation_table::fk_granted_group
                                        .eq_any(&curr_visible_group_access_pks),
                                )
                                .and(
                                    not($group_access_relation_table::fk_granted_group
                                        .eq_any(&group_access_overwrite_pks))
                                    .or($group_access_relation_table::fk_granted_group
                                        .eq_any(&relevant_group_access_overwrite)),
                                ),
                        ),
                    )
                    .execute($connection)
                    .await?;

                    group_access_overwrite
                        .iter()
                        .filter(|group_access| {
                            relevant_group_access_overwrite.contains(&group_access.group_pk)
                        })
                        .for_each(|group_access| added_group_access.push(*group_access));
                }

                added_group_access.retain_mut(|group_access| {
                    !curr_visible_group_access.iter().any(|curr_group| {
                        curr_group.fk_granted_group == group_access.group_pk
                            && curr_group.write == group_access.write
                    })
                });

                if !added_group_access.is_empty() {
                    let group_pks = added_group_access
                        .iter()
                        .map(|g| g.group_pk)
                        .collect::<Vec<_>>();
                    report_inaccessible_group_pks(&group_pks, $user, $connection).await?;

                    let new_object_group_access = added_group_access
                        .iter()
                        .map(|group_access| $group_access_relation_entity {
                            $fk_source_object: $source_object_pk,
                            fk_granted_group: group_access.group_pk,
                            write: group_access.write,
                            fk_granted_by: $user.pk,
                            creation_timestamp: Utc::now(),
                        })
                        .collect::<Vec<_>>();

                    let res = diesel::insert_into($group_access_relation_table::table)
                        .values(&new_object_group_access)
                        .on_conflict((
                            $group_access_relation_table::$fk_source_object,
                            $group_access_relation_table::fk_granted_group,
                        ))
                        .do_update()
                        .set($group_access_relation_table::write.eq(excluded($group_access_relation_table::write)))
                        .execute($connection)
                        .await
                        .map_err(retry_on_constraint_violation)?;

                    res > 0
                } else {
                    false
                }
            } else {
                false
            };

            if let Some(ref removed_group_access) = $removed_group_access {
                if !removed_group_access.is_empty() {
                    diesel::delete(
                        $group_access_relation_table::table.filter(
                            $group_access_relation_table::$fk_source_object.eq($source_object_pk).and(
                                $group_access_relation_table::fk_granted_group.eq_any(removed_group_access),
                            ),
                        ),
                    )
                    .execute($connection)
                    .await?;
                }
            }

            if added_groups {
                let curr_group_count = $group_access_relation_table::table
                    .filter($group_access_relation_table::$fk_source_object.eq($source_object_pk))
                    .count()
                    .get_result::<i64>($connection)
                    .await?;

                if curr_group_count > 50 {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::InvalidRequestInputError(format!(
                            "Cannot supply more than 50 groups, supplied: {}",
                            curr_group_count
                        )),
                    ));
                }
            }
    };
}

#[derive(Deserialize, Validate)]
pub struct EditPostRequest {
    #[validate(length(max = 100), custom = "validate_tags")]
    pub tags_overwrite: Option<Vec<String>>,
    #[validate(length(max = 100))]
    pub tag_pks_overwrite: Option<Vec<i64>>,
    pub removed_tag_pks: Option<Vec<i64>>,
    #[validate(length(max = 100))]
    pub added_tag_pks: Option<Vec<i64>>,
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
    pub group_access_overwrite: Option<Vec<GroupAccessDefinition>>,
    #[validate(length(max = 50))]
    pub added_group_access: Option<Vec<GroupAccessDefinition>>,
    pub removed_group_access: Option<Vec<i64>>,
}

pub async fn edit_post_handler(
    mut request: EditPostRequest,
    post_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for EditPostRequest: {}",
            e
        )))
    })?;

    dedup_vec_optional(&mut request.tags_overwrite);
    dedup_vec_optional(&mut request.tag_pks_overwrite);
    dedup_vec_optional(&mut request.removed_tag_pks);
    dedup_vec_optional(&mut request.added_tag_pks);
    dedup_vec_optional(&mut request.added_tags);
    dedup_vec_optional(&mut request.removed_group_access);
    dedup_vec_optional(&mut request.group_access_overwrite);
    dedup_vec_optional(&mut request.added_group_access);
    dedup_vec_optional(&mut request.removed_group_access);
    dedup_vecs_optional(&mut request.added_tag_pks, &request.tag_pks_overwrite);
    dedup_vecs_optional(&mut request.added_tags, &request.tags_overwrite);
    dedup_vecs_optional(
        &mut request.added_group_access,
        &request.group_access_overwrite,
    );

    let mut connection = acquire_db_connection().await?;

    let post = run_serializable_transaction(&mut connection, |connection| {
        async {
            if !perms::is_post_editable(connection, Some(&user), post_pk).await? {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(post_pk),
                ));
            }

            let mut added_tags = request.added_tags.clone();
            let mut added_tag_pks = request.added_tag_pks.clone();
            let mut removed_tag_pks = request.removed_tag_pks.clone();

            handle_object_tag_update!(
                post_pk,
                PostTag,
                post_tag,
                fk_post,
                request.tags_overwrite,
                request.tag_pks_overwrite,
                added_tags,
                added_tag_pks,
                removed_tag_pks,
                connection
            );

            handle_object_group_access_update!(
                post_pk,
                PostGroupAccess,
                post_group_access,
                fk_post,
                request.group_access_overwrite,
                request.added_group_access,
                request.removed_group_access,
                &user,
                connection
            );

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
                    .set(update)
                    .get_result::<Post>(connection)
                    .await?;

                Ok(updated_post)
            } else {
                let loaded_post = post::table
                    .filter(post::pk.eq(post_pk))
                    .get_result::<Post>(connection)
                    .await?;

                Ok(loaded_post)
            }
        }
        .scope_boxed()
    })
    .await?;

    let tags = get_post_tags(post_pk, &mut connection)
        .await
        .map_err(Error::from)?;
    let group_access = get_post_group_access(post_pk, Some(&user), &mut connection)
        .await
        .map_err(Error::from)?;
    let create_user = registered_user::table
        .filter(registered_user::pk.eq(post.fk_create_user))
        .get_result::<User>(&mut connection)
        .await
        .map_err(Error::from)?;
    let is_editable = post.is_editable(Some(&user), &mut connection).await?;
    let is_deletable = post.is_deletable(Some(&user), &mut connection).await?;
    let s3_object = if let Some(ref s3_object_key) = post.s3_object {
        Some(
            s3_object::table
                .filter(s3_object::object_key.eq(s3_object_key))
                .get_result::<S3Object>(&mut connection)
                .await
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
        create_user,
        score: post.score,
        s3_object,
        thumbnail_url: post.thumbnail_url,
        prev_post: None,
        next_post: None,
        public: post.public,
        public_edit: post.public_edit,
        description: post.description,
        is_editable,
        is_deletable,
        tags,
        group_access,
        post_collection_item: None,
    }))
}

#[derive(Deserialize, Validate)]
pub struct EditPostCollectionRequest {
    #[validate(length(max = 100), custom = "validate_tags")]
    pub tags_overwrite: Option<Vec<String>>,
    #[validate(length(max = 100))]
    pub tag_pks_overwrite: Option<Vec<i64>>,
    pub removed_tag_pks: Option<Vec<i64>>,
    #[validate(length(max = 100))]
    pub added_tag_pks: Option<Vec<i64>>,
    #[validate(length(max = 100), custom = "validate_tags")]
    pub added_tags: Option<Vec<String>>,
    #[validate(length(max = 300))]
    pub title: Option<String>,
    pub is_public: Option<bool>,
    pub public_edit: Option<bool>,
    #[validate(length(max = 30000))]
    pub description: Option<String>,
    #[validate(length(max = 50))]
    pub group_access_overwrite: Option<Vec<GroupAccessDefinition>>,
    #[validate(length(max = 50))]
    pub added_group_access: Option<Vec<GroupAccessDefinition>>,
    pub removed_group_access: Option<Vec<i64>>,
    pub poster_object_key: Option<String>,
    #[validate(length(max = 10000))]
    pub post_pks_overwrite: Option<Vec<i64>>,
    #[validate(length(min = 0, max = 1024))]
    pub post_query_overwrite: Option<String>,
    #[validate(length(max = 10000))]
    pub added_post_pks: Option<Vec<i64>>,
    #[validate(length(min = 0, max = 1024))]
    pub added_post_query: Option<String>,
    pub removed_item_pks: Option<Vec<i64>>,
    pub duplicate_mode: Option<PostCollectionDuplicateMode>,
}

/// Changes the behaviour of adding duplicate posts to collections for the edit_post_collection_handler
///
/// `ignore`: Ignores the existence of duplicates and simply adds all specified posts
/// `skip`: Skips duplicates and only adds posts that aren't already contained in the collection
/// `reject`: Rejects the request with error_code 400018 if duplicates are detected for the specified posts
///
/// Input is expected to be lower case, default behaviour is `ignore`.
#[derive(Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PostCollectionDuplicateMode {
    Ignore,
    Skip,
    Reject,
}

pub async fn edit_post_collection_handler(
    mut request: EditPostCollectionRequest,
    post_collection_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for EditPostCollectionRequest: {}",
            e
        )))
    })?;

    dedup_vec_optional(&mut request.tags_overwrite);
    dedup_vec_optional(&mut request.tag_pks_overwrite);
    dedup_vec_optional(&mut request.removed_tag_pks);
    dedup_vec_optional(&mut request.added_tag_pks);
    dedup_vec_optional(&mut request.added_tags);
    dedup_vec_optional(&mut request.removed_group_access);
    dedup_vec_optional(&mut request.group_access_overwrite);
    dedup_vec_optional(&mut request.added_group_access);
    dedup_vec_optional(&mut request.removed_group_access);
    dedup_vecs_optional(&mut request.added_tag_pks, &request.tag_pks_overwrite);
    dedup_vecs_optional(&mut request.added_tags, &request.tags_overwrite);
    dedup_vecs_optional(
        &mut request.added_group_access,
        &request.group_access_overwrite,
    );

    let mut connection = acquire_db_connection().await?;

    let post_collection = run_serializable_transaction(&mut connection, |connection| {
        async {
            if !perms::is_post_collection_editable(connection, Some(&user), post_collection_pk)
                .await?
            {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(post_collection_pk),
                ));
            }

            let mut added_tags = request.added_tags.clone();
            let mut added_tag_pks = request.added_tag_pks.clone();
            let mut removed_tag_pks = request.removed_tag_pks.clone();
            let mut added_post_pks = request.added_post_pks.clone();
            let mut added_post_query = request.added_post_query.clone();

            if let Some(ref added_post_pks) = added_post_pks {
                report_inaccessible_post_pks(added_post_pks, &user, connection).await?
            }

            handle_object_tag_update!(
                post_collection_pk,
                PostCollectionTag,
                post_collection_tag,
                fk_post_collection,
                request.tags_overwrite,
                request.tag_pks_overwrite,
                added_tags,
                added_tag_pks,
                removed_tag_pks,
                connection
            );

            handle_object_group_access_update!(
                post_collection_pk,
                PostCollectionGroupAccess,
                post_collection_group_access,
                fk_post_collection,
                request.group_access_overwrite,
                request.added_group_access,
                request.removed_group_access,
                &user,
                connection
            );

            if request.post_pks_overwrite.is_some() || request.post_query_overwrite.is_some() {
                diesel::delete(post_collection_item::table)
                .filter(post_collection_item::fk_post_collection.eq(post_collection_pk))
                .execute(connection)
                .await?;

                if let Some(ref post_pks_overwrite) = request.post_pks_overwrite {
                    match added_post_pks {
                        Some(ref mut added_post_pks) => added_post_pks.append(&mut post_pks_overwrite.clone()),
                        None => added_post_pks = Some(post_pks_overwrite.clone())
                    }
                }
                if let Some(ref post_query_overwrite) = request.post_query_overwrite {
                    match added_post_query {
                        Some(_) => {
                            let found_posts = query::find_all_posts(post_query_overwrite.clone(), &Some(user.clone())).await?;
                            let mut found_post_pks = found_posts.iter().map(|post_query_object| post_query_object.pk).collect::<Vec<_>>();
                            match added_post_pks {
                                Some(ref mut added_post_pks) => added_post_pks.append(&mut found_post_pks),
                                None => added_post_pks = Some(found_post_pks)
                            }
                        }
                        None => added_post_query = Some(post_query_overwrite.clone())
                    }
                }
            }

            if let Some(ref added_post_query) = added_post_query {
                let found_posts = query::find_all_posts(added_post_query.clone(), &Some(user.clone())).await?;
                let mut found_post_pks = found_posts.iter().map(|post_query_object| post_query_object.pk).collect::<Vec<_>>();
                match added_post_pks {
                    Some(ref mut added_post_pks) => added_post_pks.append(&mut found_post_pks),
                    None => added_post_pks = Some(found_post_pks)
                }
            }

            if let Some(ref mut added_post_pks) = added_post_pks {
                if !added_post_pks.is_empty() {
                    match request.duplicate_mode {
                        Some(PostCollectionDuplicateMode::Ignore) | None => {}
                        Some(PostCollectionDuplicateMode::Reject) | Some(PostCollectionDuplicateMode::Skip) => {
                            let duplicate_posts = post_collection_item::table
                                .select(post_collection_item::fk_post)
                                .filter(post_collection_item::fk_post_collection.eq(post_collection_pk).and(post_collection_item::fk_post.eq_any(&*added_post_pks)))
                                .get_results::<i64>(connection)
                                .await?;

                            if request.duplicate_mode == Some(PostCollectionDuplicateMode::Reject) && !duplicate_posts.is_empty() {
                                return Err(TransactionRuntimeError::Rollback(Error::DuplicatePostCollectionItemError(post_collection_pk, duplicate_posts)));
                            } else {
                                added_post_pks.retain(|pk| !duplicate_posts.contains(pk));
                            }
                        }
                    }

                    let found_ordinal = post_collection_item::table
                        .select(dsl::max(post_collection_item::ordinal))
                        .filter(post_collection_item::fk_post_collection.eq(post_collection_pk))
                        .first::<Option<i32>>(connection)
                        .await
                        .optional()?;
                    let current_ordinal = match found_ordinal {
                        Some(Some(ordinal)) => ordinal + 1,
                        _ => 0
                    };

                    let post_collection_items = added_post_pks.iter().enumerate().map(|(idx, post_pk)| NewPostCollectionItem {
                        fk_post: *post_pk,
                        fk_post_collection: post_collection_pk,
                        fk_added_by: user.pk,
                        ordinal: current_ordinal + (idx as i32),
                    }).collect::<Vec<_>>();

                    // split items into chunks to avoid hitting the parameter limit
                    for item_chunk in post_collection_items.chunks(4096) {
                        diesel::insert_into(post_collection_item::table)
                            .values(item_chunk)
                            .execute(connection)
                            .await?;
                    }
                }
            }

            if let Some(ref removed_item_pks) = request.removed_item_pks {
                if !removed_item_pks.is_empty() {
                    diesel::delete(post_collection_item::table)
                        .filter(post_collection_item::pk.eq_any(removed_item_pks))
                        .execute(connection)
                        .await?;

                    // close resulting gaps in the ordinal sequence
                    diesel::sql_query(r#"
                        WITH post_collection_items_enumerated AS (
                            SELECT pk, row_number() OVER(ORDER BY ordinal) as row_idx
                            FROM post_collection_item
                            WHERE fk_post_collection = $1
                        )
                        UPDATE post_collection_item
                        SET ordinal = (SELECT row_idx - 1 FROM post_collection_items_enumerated WHERE pk = post_collection_item.pk)
                        WHERE fk_post_collection = $1
                    "#)
                    .bind::<BigInt, _>(post_collection_pk)
                    .execute(connection)
                    .await?;
                }
            }

            if added_post_pks
            .as_ref()
            .map(|v| !v.is_empty())
            .unwrap_or(false) {
                let post_collection_item_count = post_collection_item::table
                    .filter(post_collection_item::fk_post_collection.eq(post_collection_pk))
                    .count()
                    .get_result::<i64>(connection)
                    .await?;

                if post_collection_item_count > 10000 {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::TooManyResultsError(post_collection_item_count as u32, 10000)
                    ));
                }
            }

            let mut poster_object_key = request.poster_object_key.clone();
            if poster_object_key.is_none() && (
                added_post_pks
                    .as_ref()
                    .map(|v| !v.is_empty())
                    .unwrap_or(false)
                || request.removed_item_pks
                    .as_ref()
                    .map(|v| !v.is_empty())
                    .unwrap_or(false)
            ) {
                let poster_object = load_post_collection_poster_object(post_collection_pk, connection).await?;
                poster_object_key = poster_object.map(|o| o.object_key);
            }

            let update = PostCollectionUpdateOptional {
                title: request.title.clone(),
                public: request.is_public,
                public_edit: request.public_edit,
                poster_object_key,
                description: request.description.clone(),
            };

            if update.has_changes() {
                let updated_post_collection = diesel::update(post_collection::table)
                    .filter(post_collection::pk.eq(post_collection_pk))
                    .set(update)
                    .get_result::<PostCollection>(connection)
                    .await?;

                Ok(updated_post_collection)
            } else {
                let loaded_post_collection = post_collection::table
                    .filter(post_collection::pk.eq(post_collection_pk))
                    .get_result::<PostCollection>(connection)
                    .await?;

                Ok(loaded_post_collection)
            }
        }
        .scope_boxed()
    })
    .await?;

    let tags = get_post_collection_tags(post_collection_pk, &mut connection)
        .await
        .map_err(Error::from)?;
    let group_access =
        get_post_collection_group_access(post_collection_pk, Some(&user), &mut connection)
            .await
            .map_err(Error::from)?;
    let create_user = registered_user::table
        .filter(registered_user::pk.eq(post_collection.fk_create_user))
        .get_result::<User>(&mut connection)
        .await
        .map_err(Error::from)?;
    let is_editable = post_collection
        .is_editable(Some(&user), &mut connection)
        .await?;
    let is_deletable = post_collection
        .is_deletable(Some(&user), &mut connection)
        .await?;
    let poster_object = if let Some(ref poster_object_key) = post_collection.poster_object_key {
        Some(
            s3_object::table
                .filter(s3_object::object_key.eq(poster_object_key))
                .get_result::<S3Object>(&mut connection)
                .await
                .map_err(Error::from)?,
        )
    } else {
        None
    };

    Ok(warp::reply::json(&PostCollectionDetailed {
        pk: post_collection.pk,
        title: post_collection.title,
        create_user,
        creation_timestamp: post_collection.creation_timestamp,
        public: post_collection.public,
        public_edit: post_collection.public_edit,
        poster_object,
        poster_object_key: post_collection.poster_object_key,
        description: post_collection.description,
        is_editable,
        is_deletable,
        tags,
        group_access,
    }))
}
