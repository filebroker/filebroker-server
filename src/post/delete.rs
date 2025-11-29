use diesel::sql_types::Bool;
use diesel::{
    BoolExpressionMethods, ExpressionMethods, IntoSql, JoinOnDsl, NullableExpressionMethods,
    QueryDsl, Table,
    dsl::{exists, not},
};
use diesel_async::{AsyncPgConnection, RunQueryDsl, scoped_futures::ScopedFutureExt};
use serde::{Deserialize, Serialize};
use validator::Validate;
use warp::{reject::Rejection, reply::Reply};

use crate::{
    acquire_db_connection,
    error::{Error, TransactionRuntimeError},
    model::{DeferredS3ObjectDeletion, HlsStream, Post, PostCollection, S3Object, User},
    perms::{self, get_group_membership_administrator_condition},
    run_serializable_transaction,
    schema::{
        broker, broker_access, deferred_s3_object_deletion, hls_stream, post, post_collection,
        post_collection_group_access, post_collection_item, post_collection_tag, post_group_access,
        post_tag, s3_object,
    },
    util::dedup_vec,
};

#[derive(Deserialize, Validate)]
pub struct DeletePostsRequest {
    #[validate(length(max = 1000))]
    post_pks: Vec<i64>,
    inaccessible_post_mode: Option<DeleteInaccessiblePostMode>,
    delete_unreferenced_objects: Option<bool>,
}

/// Changes the behaviour when specifying a post for deletion where the create user
/// does not match the current user.
///
/// `skip` skips all posts that cannot be deleted
/// `reject` Rejects the request with a 403 error
///
/// Input is expected to be lower case, default behaviour is `skip`.
#[derive(Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DeleteInaccessiblePostMode {
    Skip,
    Reject,
}

#[derive(Serialize)]
pub struct DeletePostsResponse {
    deleted_posts: Vec<Post>,
    deleted_objects: Vec<S3Object>,
}

pub async fn delete_posts_handler(
    mut request: DeletePostsRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for DeletePostsRequest: {e}"
        )))
    })?;

    dedup_vec(&mut request.post_pks);

    let res = if !request.post_pks.is_empty() {
        let mut connection = acquire_db_connection().await?;
        run_serializable_transaction(&mut connection, |connection| {
            async {
                let mut post_pks = request.post_pks.clone();
                let inaccessible_posts =
                    post::table
                        .left_join(s3_object::table)
                        .left_join(broker::table.on(s3_object::fk_broker.eq(broker::pk)))
                        .select(post::pk)
                        .filter(post::pk.eq_any(&post_pks).and(not(
                            user.is_admin.into_sql::<Bool>().or(
                                post::fk_create_user.eq(user.pk).or(
                                    broker::fk_owner.eq(user.pk).or(exists(
                                        broker_access::table.filter(
                                            perms::get_broker_group_access_write_condition!(
                                                user.pk
                                            ),
                                        ),
                                    )),
                                ),
                            ),
                        )))
                        .get_results::<i64>(connection)
                        .await?;

                if let Some(DeleteInaccessiblePostMode::Reject) = request.inaccessible_post_mode
                    && !inaccessible_posts.is_empty()
                {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::InaccessibleObjectsError(inaccessible_posts),
                    ));
                }

                post_pks.retain(|pk| !inaccessible_posts.contains(pk));

                diesel::delete(post_collection_item::table)
                    .filter(post_collection_item::fk_post.eq_any(&post_pks))
                    .execute(connection)
                    .await?;

                diesel::delete(post_group_access::table)
                    .filter(post_group_access::fk_post.eq_any(&post_pks))
                    .execute(connection)
                    .await?;

                diesel::delete(post_tag::table)
                    .filter(post_tag::fk_post.eq_any(&post_pks))
                    .execute(connection)
                    .await?;

                let deleted_posts = diesel::delete(post::table)
                    .filter(post::pk.eq_any(&post_pks))
                    .get_results::<Post>(connection)
                    .await?;

                let deleted_objects = if request.delete_unreferenced_objects.unwrap_or(true) {
                    let s3_object_keys_to_del = deleted_posts
                        .iter()
                        .map(|post| &post.s3_object)
                        .collect::<Vec<_>>();
                    if !s3_object_keys_to_del.is_empty() {
                        delete_unreferenced_objects(&s3_object_keys_to_del, &user, connection)
                            .await?
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };

                Ok(DeletePostsResponse {
                    deleted_posts,
                    deleted_objects,
                })
            }
            .scope_boxed()
        })
        .await?
    } else {
        DeletePostsResponse {
            deleted_posts: Vec::new(),
            deleted_objects: Vec::new(),
        }
    };

    Ok(warp::reply::json(&res))
}

async fn delete_unreferenced_objects(
    object_keys: &[&String],
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<S3Object>, Error> {
    let object_keys_to_del = s3_object::table
        .select(s3_object::object_key)
        .filter(
            s3_object::object_key
                .eq_any(object_keys)
                .and(not(exists(
                    post::table.filter(post::s3_object.eq(s3_object::object_key)),
                )))
                .and(not(exists(post_collection::table.filter(
                    post_collection::poster_object_key.eq(s3_object::object_key.nullable()),
                )))),
        )
        .get_results::<String>(connection)
        .await?;

    delete_s3_objects(&object_keys_to_del, user, connection).await
}

pub async fn delete_s3_objects(
    object_keys: &[String],
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<S3Object>, Error> {
    let s3_objects = s3_object::table
        .inner_join(broker::table)
        .select(s3_object::table::all_columns())
        .filter(
            s3_object::object_key.eq_any(object_keys).and(
                user.is_admin
                    .into_sql::<Bool>()
                    .or(s3_object::fk_uploader
                        .eq(user.pk)
                        .or(broker::fk_owner
                            .eq(user.pk)
                            .or(exists(broker_access::table.filter(
                                perms::get_broker_group_access_write_condition!(user.pk),
                            ))))),
            ),
        )
        .get_results::<S3Object>(connection)
        .await?;

    let hls_master_playlists = s3_objects
        .iter()
        .filter_map(|s3_object| s3_object.hls_master_playlist.as_ref())
        .collect::<Vec<_>>();
    let hls_streams = diesel::delete(hls_stream::table)
        .filter(hls_stream::master_playlist.eq_any(&hls_master_playlists))
        .get_results::<HlsStream>(connection)
        .await?;

    let object_keys = s3_objects
        .iter()
        .map(|s3_object| &s3_object.object_key)
        .collect::<Vec<_>>();
    let mut deleted_objects = diesel::delete(s3_object::table)
        .filter(
            s3_object::object_key
                .eq_any(&object_keys)
                .or(s3_object::object_key
                    .eq_any(hls_streams.iter().map(|hls_stream| &hls_stream.stream_file)))
                .or(s3_object::object_key.eq_any(
                    hls_streams
                        .iter()
                        .map(|hls_stream| &hls_stream.stream_playlist),
                )),
        )
        .get_results::<S3Object>(connection)
        .await?;

    let thumbnails_to_del = deleted_objects
        .iter()
        .filter_map(|s3_object| s3_object.thumbnail_object_key.as_ref())
        .collect::<Vec<_>>();
    let hls_master_playlists_to_del = deleted_objects
        .iter()
        .filter_map(|s3_object| s3_object.hls_master_playlist.as_ref())
        .collect::<Vec<_>>();
    let mut deleted_related_objects: Vec<S3Object> = diesel::delete(s3_object::table)
        .filter(
            s3_object::object_key
                .eq_any(&thumbnails_to_del)
                .or(s3_object::object_key.eq_any(&hls_master_playlists_to_del)),
        )
        .get_results::<S3Object>(connection)
        .await?;

    deleted_objects.append(&mut deleted_related_objects);

    let deferred_s3_objects_deletions = deleted_objects
        .iter()
        .map(|deleted_object| DeferredS3ObjectDeletion {
            object_key: deleted_object.object_key.clone(),
            locked_at: None,
            fail_count: None,
            fk_broker: deleted_object.fk_broker,
        })
        .collect::<Vec<_>>();
    diesel::insert_into(deferred_s3_object_deletion::table)
        .values(deferred_s3_objects_deletions)
        .execute(connection)
        .await?;

    Ok(deleted_objects)
}

#[derive(Deserialize, Validate)]
pub struct DeletePostCollectionsRequest {
    #[validate(length(max = 1000))]
    post_collection_pks: Vec<i64>,
    inaccessible_post_collection_mode: Option<DeleteInaccessiblePostMode>,
}

#[derive(Serialize)]
pub struct DeletePostCollectionsResponse {
    deleted_post_collections: Vec<PostCollection>,
}

pub async fn delete_posts_collections_handler(
    mut request: DeletePostCollectionsRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for DeletePostCollectionsRequest: {e}"
        )))
    })?;

    dedup_vec(&mut request.post_collection_pks);

    let res = if !request.post_collection_pks.is_empty() {
        let mut connection = acquire_db_connection().await?;
        run_serializable_transaction(&mut connection, |connection| {
            async {
                let mut post_collection_pks = request.post_collection_pks.clone();
                let inaccessible_post_collections = post_collection::table
                    .select(post_collection::pk)
                    .filter(
                        post_collection::pk
                            .eq_any(&post_collection_pks)
                            .and(not(post_collection::fk_create_user.eq(user.pk))),
                    )
                    .get_results::<i64>(connection)
                    .await?;

                if let Some(DeleteInaccessiblePostMode::Reject) =
                    request.inaccessible_post_collection_mode
                    && !inaccessible_post_collections.is_empty()
                {
                    return Err(TransactionRuntimeError::Rollback(
                        Error::InaccessibleObjectsError(inaccessible_post_collections),
                    ));
                }

                post_collection_pks.retain(|pk| !inaccessible_post_collections.contains(pk));

                diesel::delete(post_collection_item::table)
                    .filter(post_collection_item::fk_post_collection.eq_any(&post_collection_pks))
                    .execute(connection)
                    .await?;

                diesel::delete(post_collection_group_access::table)
                    .filter(
                        post_collection_group_access::fk_post_collection
                            .eq_any(&post_collection_pks),
                    )
                    .execute(connection)
                    .await?;

                diesel::delete(post_collection_tag::table)
                    .filter(post_collection_tag::fk_post_collection.eq_any(&post_collection_pks))
                    .execute(connection)
                    .await?;

                let deleted_post_collections = diesel::delete(post_collection::table)
                    .filter(post_collection::pk.eq_any(&post_collection_pks))
                    .get_results::<PostCollection>(connection)
                    .await?;

                let poster_object_keys = deleted_post_collections
                    .iter()
                    .filter_map(|post_collection| post_collection.poster_object_key.as_ref())
                    .collect::<Vec<_>>();

                delete_unreferenced_objects(&poster_object_keys, &user, connection).await?;

                Ok(DeletePostCollectionsResponse {
                    deleted_post_collections,
                })
            }
            .scope_boxed()
        })
        .await?
    } else {
        DeletePostCollectionsResponse {
            deleted_post_collections: Vec::new(),
        }
    };

    Ok(warp::reply::json(&res))
}
