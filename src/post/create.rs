use chrono::Utc;
use diesel::{JoinOnDsl, OptionalExtension, QueryDsl, Table};
use diesel_async::{scoped_futures::ScopedFutureExt, RunQueryDsl};
use serde::Deserialize;
use validator::Validate;
use warp::{reject::Rejection, reply::Reply};

use crate::{
    acquire_db_connection,
    diesel::ExpressionMethods,
    error::{Error, TransactionRuntimeError},
    model::{
        NewPost, NewPostCollection, NewPostCollectionItem, Post, PostCollection,
        PostCollectionGroupAccess, PostCollectionTag, PostGroupAccess, PostTag, S3Object,
        S3ObjectMetadata, User, UserPublic,
    },
    perms,
    query::{self, report_missing_pks, PostCollectionDetailed, PostDetailed},
    run_retryable_transaction, run_serializable_transaction,
    schema::{
        self, post, post_collection, post_collection_group_access, post_collection_item,
        post_collection_tag, post_group_access, post_tag, registered_user, s3_object,
        s3_object_metadata, user_group,
    },
    tags::{handle_entered_and_selected_tags, validate_tags},
    util::dedup_vec_optional,
};

use super::{
    get_post_collection_group_access, get_post_collection_tags, get_post_group_access,
    get_post_tags, load_post_collection_poster_object, report_inaccessible_groups,
    GroupAccessDefinition,
};

#[derive(Deserialize, Validate)]
pub struct CreatePostRequest {
    #[validate(url)]
    pub data_url: Option<String>,
    #[validate(url)]
    pub source_url: Option<String>,
    #[validate(length(max = 300))]
    pub title: Option<String>,
    #[validate(length(max = 100), custom(function = "validate_tags"))]
    pub entered_tags: Option<Vec<String>>,
    #[validate(length(max = 100))]
    pub selected_tags: Option<Vec<i64>>,
    pub s3_object: String,
    #[validate(url)]
    pub thumbnail_url: Option<String>,
    pub is_public: Option<bool>,
    pub public_edit: Option<bool>,
    #[validate(length(max = 50))]
    pub group_access: Option<Vec<GroupAccessDefinition>>,
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

    dedup_vec_optional(&mut create_post_request.entered_tags);
    dedup_vec_optional(&mut create_post_request.selected_tags);
    dedup_vec_optional(&mut create_post_request.group_access);

    let mut connection = acquire_db_connection().await?;

    if let Some(ref group_access) = create_post_request.group_access {
        if !group_access.is_empty() {
            report_inaccessible_groups(group_access, &user, &mut connection).await?;
        }
    }

    // run as repeatable read transaction and retry serialisation errors when a concurrent transaction
    // deletes or creates relevant tags
    run_serializable_transaction(&mut connection, |connection| {
        async move {
            let set_tags = handle_entered_and_selected_tags(
                &create_post_request.selected_tags,
                create_post_request.entered_tags,
                connection,
            )
            .await?;

            let object_key = &create_post_request.s3_object;
            let metadata_title: Option<String> = if create_post_request.title.is_none() {
                s3_object_metadata::table
                    .select(s3_object_metadata::title)
                    .filter(s3_object_metadata::object_key.eq(object_key))
                    .get_result::<Option<String>>(connection)
                    .await
                    .optional()?
                    .flatten()
            } else {
                None
            };

            let post = diesel::insert_into(post::table)
                .values(NewPost {
                    data_url: create_post_request.data_url.clone(),
                    source_url: create_post_request.source_url.clone(),
                    title: create_post_request.title.clone().or(metadata_title),
                    fk_create_user: user.pk,
                    s3_object: create_post_request.s3_object.clone(),
                    thumbnail_url: create_post_request.thumbnail_url.clone(),
                    public: create_post_request.is_public.unwrap_or(false),
                    public_edit: create_post_request.public_edit.unwrap_or(false),
                    description: create_post_request.description.clone(),
                })
                .get_result::<Post>(connection)
                .await?;

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
                    .execute(connection)
                    .await?;
            }

            if let Some(ref group_access) = create_post_request.group_access {
                if !group_access.is_empty() {
                    let group_pks = group_access.iter().map(|g| g.group_pk).collect::<Vec<_>>();
                    report_missing_pks!(user_group, &group_pks, connection)??;
                    let now = Utc::now();
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
                        .execute(connection)
                        .await?;
                }
            }

            let tags = get_post_tags(post.pk, connection)
                .await
                .map_err(Error::from)?;
            let group_access = get_post_group_access(post.pk, Some(&user), connection)
                .await
                .map_err(Error::from)?;
            let (create_user, edit_user) = diesel::alias!(
                schema::registered_user as create_user,
                schema::registered_user as edit_user,
            );
            let (create_user, edit_user) = post::table
                .inner_join(
                    create_user.on(post::fk_create_user.eq(create_user.field(registered_user::pk))),
                )
                .inner_join(
                    edit_user.on(post::fk_edit_user.eq(edit_user.field(registered_user::pk))),
                )
                .select((
                    create_user.fields(registered_user::table::all_columns()),
                    edit_user.fields(registered_user::table::all_columns()),
                ))
                .filter(post::pk.eq(post.pk))
                .get_result::<(UserPublic, UserPublic)>(connection)
                .await
                .map_err(Error::from)?;
            let is_editable = post.is_editable(Some(&user), connection).await?;
            let is_deletable = post.is_deletable(Some(&user), connection).await?;
            let (s3_object, s3_object_metadata) = s3_object::table
                .filter(s3_object::object_key.eq(&post.s3_object))
                .inner_join(s3_object_metadata::table)
                .get_result::<(S3Object, S3ObjectMetadata)>(connection)
                .await
                .map_err(Error::from)?;

            Ok(warp::reply::json(&PostDetailed {
                pk: post.pk,
                data_url: post.data_url,
                source_url: post.source_url,
                title: post.title,
                creation_timestamp: post.creation_timestamp,
                edit_timestamp: post.edit_timestamp,
                create_user,
                edit_user,
                score: post.score,
                s3_object,
                s3_object_metadata,
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
        .scope_boxed()
    })
    .await
    .map_err(warp::reject::custom)
}

#[derive(Deserialize, Validate)]
pub struct CreatePostCollectionRequest {
    #[validate(length(max = 300))]
    pub title: String,
    #[validate(length(max = 100), custom(function = "validate_tags"))]
    pub entered_tags: Option<Vec<String>>,
    #[validate(length(max = 100))]
    pub selected_tags: Option<Vec<i64>>,
    pub poster_object_key: Option<String>,
    pub is_public: Option<bool>,
    pub public_edit: Option<bool>,
    #[validate(length(max = 50))]
    pub group_access: Option<Vec<GroupAccessDefinition>>,
    #[validate(length(max = 30000))]
    pub description: Option<String>,
    #[validate(length(max = 10000))]
    pub post_pks: Option<Vec<i64>>,
    #[validate(length(min = 0, max = 1024))]
    pub post_query: Option<String>,
}

pub async fn create_post_collection_handler(
    mut request: CreatePostCollectionRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreatePostCollectionRequest: {}",
            e
        )))
    })?;

    dedup_vec_optional(&mut request.entered_tags);
    dedup_vec_optional(&mut request.selected_tags);
    dedup_vec_optional(&mut request.group_access);

    let mut connection = acquire_db_connection().await?;

    if let Some(ref group_access) = request.group_access {
        if !group_access.is_empty() {
            report_inaccessible_groups(group_access, &user, &mut connection).await?;
        }
    }

    run_retryable_transaction(&mut connection, |connection| {
        async move {
            let set_tags = handle_entered_and_selected_tags(
                &request.selected_tags,
                request.entered_tags,
                connection,
            )
            .await?;

            let post_collection = diesel::insert_into(post_collection::table)
                .values(NewPostCollection {
                    title: request.title,
                    fk_create_user: user.pk,
                    public: request.is_public.unwrap_or(false),
                    public_edit: request.public_edit.unwrap_or(false),
                    poster_object_key: request.poster_object_key,
                    description: request.description,
                })
                .get_result::<PostCollection>(connection)
                .await?;

            let post_collection_tags = set_tags
                .iter()
                .map(|tag| PostCollectionTag {
                    fk_post_collection: post_collection.pk,
                    fk_tag: tag.pk,
                })
                .collect::<Vec<_>>();

            if !post_collection_tags.is_empty() {
                diesel::insert_into(post_collection_tag::table)
                    .values(&post_collection_tags)
                    .execute(connection)
                    .await?;
            }

            if let Some(ref group_access) = request.group_access {
                if !group_access.is_empty() {
                    let group_pks = group_access.iter().map(|g| g.group_pk).collect::<Vec<_>>();
                    report_missing_pks!(user_group, &group_pks, connection)??;
                    let now = Utc::now();
                    let post_collection_group_access = group_access
                        .iter()
                        .map(|g| PostCollectionGroupAccess {
                            fk_post_collection: post_collection.pk,
                            fk_granted_group: g.group_pk,
                            write: g.write,
                            fk_granted_by: user.pk,
                            creation_timestamp: now,
                        })
                        .collect::<Vec<_>>();

                    diesel::insert_into(post_collection_group_access::table)
                        .values(&post_collection_group_access)
                        .execute(connection)
                        .await?;
                }
            }

            let current_ordinal = if let Some(ref post_pks) = request.post_pks {
                if !post_pks.is_empty() {
                    // load posts that are actually accessible and report missing or inaccessible pks as error
                    perms::load_posts_secured(post_pks, connection, Some(&user)).await?;
                    let post_collection_items = post_pks
                        .iter()
                        .enumerate()
                        .map(|(idx, post_pk)| NewPostCollectionItem {
                            fk_post: *post_pk,
                            fk_post_collection: post_collection.pk,
                            fk_added_by: user.pk,
                            ordinal: idx as i32,
                        })
                        .collect::<Vec<_>>();

                    // split items into chunks to avoid hitting the parameter limit
                    let mut insertion_count = 0;
                    for item_chunk in post_collection_items.chunks(4096) {
                        insertion_count += diesel::insert_into(post_collection_item::table)
                            .values(item_chunk)
                            .execute(connection)
                            .await?;
                    }
                    insertion_count
                } else {
                    0
                }
            } else {
                0
            };

            if let Some(query) = request.post_query {
                if !query.is_empty() {
                    let found_posts = query::find_all_posts(query, &Some(user.clone())).await?;
                    let post_collection_items = found_posts
                        .into_iter()
                        .enumerate()
                        .map(|(idx, post_query_object)| NewPostCollectionItem {
                            fk_post: post_query_object.pk,
                            fk_post_collection: post_collection.pk,
                            fk_added_by: user.pk,
                            ordinal: (current_ordinal + idx) as i32,
                        })
                        .collect::<Vec<_>>();
                    if current_ordinal + post_collection_items.len() > 10000 {
                        return Err(TransactionRuntimeError::Rollback(
                            Error::TooManyResultsError(
                                (current_ordinal + post_collection_items.len()) as u32,
                                10000,
                            ),
                        ));
                    }
                    if !post_collection_items.is_empty() {
                        // split items into chunks to avoid hitting the parameter limit
                        for item_chunk in post_collection_items.chunks(4096) {
                            diesel::insert_into(post_collection_item::table)
                                .values(item_chunk)
                                .execute(connection)
                                .await?;
                        }
                    }
                }
            }

            if post_collection.poster_object_key.is_none() {
                let thumbnail_object =
                    load_post_collection_poster_object(post_collection.pk, connection).await?;

                if let Some(thumbnail_object) = thumbnail_object {
                    diesel::update(post_collection::table)
                        .filter(post_collection::pk.eq(post_collection.pk))
                        .set(post_collection::poster_object_key.eq(&thumbnail_object.object_key))
                        .execute(connection)
                        .await?;
                }
            }

            let tags = get_post_collection_tags(post_collection.pk, connection)
                .await
                .map_err(Error::from)?;
            let group_access =
                get_post_collection_group_access(post_collection.pk, Some(&user), connection)
                    .await
                    .map_err(Error::from)?;
            let (create_user, edit_user) = diesel::alias!(
                schema::registered_user as create_user,
                schema::registered_user as edit_user,
            );
            let (create_user, edit_user) = post_collection::table
                .inner_join(
                    create_user
                        .on(post_collection::fk_create_user
                            .eq(create_user.field(registered_user::pk))),
                )
                .inner_join(
                    edit_user
                        .on(post_collection::fk_edit_user.eq(edit_user.field(registered_user::pk))),
                )
                .select((
                    create_user.fields(registered_user::table::all_columns()),
                    edit_user.fields(registered_user::table::all_columns()),
                ))
                .filter(post_collection::pk.eq(post_collection.pk))
                .get_result::<(UserPublic, UserPublic)>(connection)
                .await
                .map_err(Error::from)?;
            let is_editable = post_collection.is_editable(Some(&user), connection).await?;
            let is_deletable = post_collection
                .is_deletable(Some(&user), connection)
                .await?;
            let poster_object =
                if let Some(ref poster_object_key) = post_collection.poster_object_key {
                    Some(
                        s3_object::table
                            .filter(s3_object::object_key.eq(poster_object_key))
                            .get_result::<S3Object>(connection)
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
                edit_user,
                creation_timestamp: post_collection.creation_timestamp,
                edit_timestamp: post_collection.edit_timestamp,
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
        .scope_boxed()
    })
    .await
    .map_err(warp::reject::custom)
}
