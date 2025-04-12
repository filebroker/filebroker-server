use chrono::{DateTime, Utc};
use diesel::{
    BelongingToDsl, BoolExpressionMethods, OptionalExtension, Table,
    dsl::{exists, not},
    upsert::excluded,
};
use diesel_async::{AsyncPgConnection, RunQueryDsl, scoped_futures::ScopedFutureExt};
use serde::Serialize;
use warp::{Reply, reject::Rejection};

use crate::{
    acquire_db_connection,
    diesel::{ExpressionMethods, NullableExpressionMethods, QueryDsl},
    error::{Error, TransactionRuntimeError},
    model::{
        NewPostCollectionEditHistory, NewPostEditHistory, Post, PostCollection,
        PostCollectionEditHistory, PostCollectionEditHistoryGroupAccess,
        PostCollectionEditHistoryTag, PostCollectionGroupAccess, PostCollectionTag,
        PostCollectionUpdateOptional, PostEditHistory, PostEditHistoryGroupAccess,
        PostEditHistoryTag, PostTag, PostUpdateOptional, Tag, User, UserGroup, UserPublic,
    },
    perms::{self, PostCollectionJoined, PostJoined},
    post::{
        PostCollectionGroupAccessDetailed, PostGroupAccess, PostGroupAccessDetailed,
        get_post_collection_group_access, get_post_collection_tags, get_post_group_access,
        get_post_tags,
    },
    query::PaginationQueryParams,
    run_serializable_transaction,
    schema::{
        post, post_collection, post_collection_edit_history,
        post_collection_edit_history_group_access, post_collection_edit_history_tag,
        post_collection_group_access, post_collection_tag, post_edit_history,
        post_edit_history_group_access, post_edit_history_tag, post_group_access, post_tag,
        registered_user, tag, user_group, user_group_membership,
    },
    util::vec_eq_sorted,
};

use super::{load_post_collection_detailed, load_post_detailed};

#[derive(Serialize)]
pub struct PostEditHistorySnapshot {
    pub pk: i64,
    pub fk_post: i64,
    pub edit_user: UserPublic,
    pub edit_timestamp: DateTime<Utc>,
    pub data_url: Option<String>,
    pub data_url_changed: bool,
    pub source_url: Option<String>,
    pub source_url_changed: bool,
    pub title: Option<String>,
    pub title_changed: bool,
    #[serde(rename = "is_public")]
    pub public: bool,
    pub public_changed: bool,
    pub public_edit: bool,
    pub public_edit_changed: bool,
    pub description: Option<String>,
    pub description_changed: bool,
    pub tags_changed: bool,
    pub group_access_changed: bool,
    pub tags: Vec<Tag>,
    pub group_access: Vec<PostGroupAccessDetailed>,
}

#[derive(Serialize)]
pub struct PostEditHistoryResponse {
    pub edit_timestamp: DateTime<Utc>,
    pub edit_user: UserPublic,
    pub total_snapshot_count: i64,
    pub snapshots: Vec<PostEditHistorySnapshot>,
}

pub async fn get_post_edit_history_handler(
    pagination: PaginationQueryParams,
    post_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;

    let PostJoined {
        post,
        create_user: _,
        s3_object: _,
        s3_object_metadata: _,
        edit_user,
    } = perms::load_post_secured(post_pk, &mut connection, Some(&user)).await?;
    if !post.is_editable(Some(&user), &mut connection).await? {
        return Err(warp::reject::custom(Error::InaccessibleObjectError(
            post_pk,
        )));
    }

    let total_snapshot_count = post_edit_history::table
        .filter(post_edit_history::fk_post.eq(post_pk))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .map_err(Error::from)?;
    let limit = pagination.limit.unwrap_or(10);
    let offset = limit * pagination.page.unwrap_or(0);
    let post_edit_history_snapshots = post_edit_history::table
        .inner_join(registered_user::table)
        .filter(post_edit_history::fk_post.eq(post_pk))
        .order(post_edit_history::pk.desc())
        .limit(limit.into())
        .offset(offset.into())
        .load::<(PostEditHistory, UserPublic)>(&mut connection)
        .await
        .map_err(Error::from)?;

    let mut snapshots = Vec::new();
    for (post_edit_history_snapshot, edit_user) in post_edit_history_snapshots {
        let tags =
            get_post_snapshot_tags(&post_edit_history_snapshot, &post, &mut connection).await?;
        let group_access = get_post_snapshot_group_access(
            &post_edit_history_snapshot,
            &post,
            &user,
            &mut connection,
        )
        .await?;

        snapshots.push(PostEditHistorySnapshot {
            pk: post_edit_history_snapshot.pk,
            fk_post: post_edit_history_snapshot.fk_post,
            edit_user,
            edit_timestamp: post_edit_history_snapshot.edit_timestamp,
            data_url: post_edit_history_snapshot.data_url,
            data_url_changed: post_edit_history_snapshot.data_url_changed,
            source_url: post_edit_history_snapshot.source_url,
            source_url_changed: post_edit_history_snapshot.source_url_changed,
            title: post_edit_history_snapshot.title,
            title_changed: post_edit_history_snapshot.title_changed,
            public: post_edit_history_snapshot.public,
            public_changed: post_edit_history_snapshot.public_changed,
            public_edit: post_edit_history_snapshot.public_edit,
            public_edit_changed: post_edit_history_snapshot.public_edit_changed,
            description: post_edit_history_snapshot.description,
            description_changed: post_edit_history_snapshot.description_changed,
            tags_changed: post_edit_history_snapshot.tags_changed,
            group_access_changed: post_edit_history_snapshot.group_access_changed,
            tags,
            group_access,
        });
    }

    Ok(warp::reply::json(&PostEditHistoryResponse {
        edit_timestamp: post.edit_timestamp,
        edit_user,
        total_snapshot_count,
        snapshots,
    }))
}

async fn get_post_snapshot_tags(
    post_edit_history_snapshot: &PostEditHistory,
    post: &Post,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<Tag>, Error> {
    // snaphots store the previous value and only store tags if they have changed,
    // thus we need to load the tags from the next snapshot where tags_changed is true,
    // or the post's current tags if no such snapshot exists
    let relevant_tag_snapshot = if post_edit_history_snapshot.tags_changed {
        Some(post_edit_history_snapshot.clone())
    } else {
        PostEditHistory::belonging_to(post)
            .filter(post_edit_history::pk.gt(post_edit_history_snapshot.pk))
            .filter(post_edit_history::tags_changed)
            .order(post_edit_history::pk.asc())
            .first::<PostEditHistory>(connection)
            .await
            .optional()
            .map_err(Error::from)?
    };
    let tags = if let Some(relevant_tag_snapshot) = relevant_tag_snapshot {
        PostEditHistoryTag::belonging_to(&relevant_tag_snapshot)
            .inner_join(tag::table)
            .select(tag::table::all_columns())
            .load::<Tag>(connection)
            .await
            .map_err(Error::from)?
    } else {
        get_post_tags(post.pk, connection)
            .await
            .map_err(Error::from)?
    };

    Ok(tags)
}

async fn get_post_snapshot_group_access(
    post_edit_history_snapshot: &PostEditHistory,
    post: &Post,
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<PostGroupAccessDetailed>, Error> {
    // same as above, but for group access
    let relevant_group_access_snapshot = if post_edit_history_snapshot.group_access_changed {
        Some(post_edit_history_snapshot.clone())
    } else {
        PostEditHistory::belonging_to(post)
            .filter(post_edit_history::pk.gt(post_edit_history_snapshot.pk))
            .filter(post_edit_history::group_access_changed)
            .order(post_edit_history::pk.asc())
            .first::<PostEditHistory>(connection)
            .await
            .optional()
            .map_err(Error::from)?
    };
    let group_access = if let Some(relevant_group_access_snapshot) = relevant_group_access_snapshot
    {
        PostEditHistoryGroupAccess::belonging_to(&relevant_group_access_snapshot)
            .inner_join(user_group::table)
            .filter(not(user_group::hidden).or(perms::get_group_membership_condition!(user.pk)))
            .load::<(PostEditHistoryGroupAccess, UserGroup)>(connection)
            .await
            .map_err(Error::from)?
            .into_iter()
            .map(|(group_access, user_group)| PostGroupAccessDetailed {
                fk_post: post.pk,
                write: group_access.write,
                fk_granted_by: group_access.fk_granted_by,
                creation_timestamp: group_access.creation_timestamp,
                granted_group: user_group,
            })
            .collect::<Vec<_>>()
    } else {
        get_post_group_access(post.pk, Some(user), connection)
            .await
            .map_err(Error::from)?
    };

    Ok(group_access)
}

#[derive(Serialize)]
pub struct PostCollectionEditHistorySnapshot {
    pub pk: i64,
    pub fk_post_collection: i64,
    pub edit_user: UserPublic,
    pub edit_timestamp: DateTime<Utc>,
    pub title: String,
    pub title_changed: bool,
    #[serde(rename = "is_public")]
    pub public: bool,
    pub public_changed: bool,
    pub public_edit: bool,
    pub public_edit_changed: bool,
    pub description: Option<String>,
    pub description_changed: bool,
    pub poster_object_key: Option<String>,
    pub poster_object_key_changed: bool,
    pub tags_changed: bool,
    pub group_access_changed: bool,
    pub tags: Vec<Tag>,
    pub group_access: Vec<PostCollectionGroupAccessDetailed>,
}

#[derive(Serialize)]
pub struct PostCollectionEditHistoryResponse {
    pub edit_timestamp: DateTime<Utc>,
    pub edit_user: UserPublic,
    pub total_snapshot_count: i64,
    pub snapshots: Vec<PostCollectionEditHistorySnapshot>,
}

pub async fn get_post_collection_edit_history_handler(
    pagination: PaginationQueryParams,
    post_collection_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;

    let PostCollectionJoined {
        post_collection,
        create_user: _,
        poster_object: _,
        edit_user,
    } = perms::load_post_collection_secured(post_collection_pk, &mut connection, Some(&user))
        .await?;
    if !post_collection
        .is_editable(Some(&user), &mut connection)
        .await?
    {
        return Err(warp::reject::custom(Error::InaccessibleObjectError(
            post_collection_pk,
        )));
    }

    let total_snapshot_count = post_collection_edit_history::table
        .filter(post_collection_edit_history::fk_post_collection.eq(post_collection_pk))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .map_err(Error::from)?;
    let limit = pagination.limit.unwrap_or(10);
    let offset = limit * pagination.page.unwrap_or(0);
    let post_collection_edit_history_snapshots = post_collection_edit_history::table
        .inner_join(registered_user::table)
        .filter(post_collection_edit_history::fk_post_collection.eq(post_collection_pk))
        .order(post_collection_edit_history::pk.desc())
        .limit(limit.into())
        .offset(offset.into())
        .load::<(PostCollectionEditHistory, UserPublic)>(&mut connection)
        .await
        .map_err(Error::from)?;

    let mut snapshots = Vec::new();
    for (post_collection_edit_history_snapshot, edit_user) in post_collection_edit_history_snapshots
    {
        let tags = get_post_collection_snapshot_tags(
            &post_collection_edit_history_snapshot,
            &post_collection,
            &mut connection,
        )
        .await?;
        let group_access = get_post_collection_snapshot_group_access(
            &post_collection_edit_history_snapshot,
            &post_collection,
            &user,
            &mut connection,
        )
        .await?;

        snapshots.push(PostCollectionEditHistorySnapshot {
            pk: post_collection_edit_history_snapshot.pk,
            fk_post_collection: post_collection_edit_history_snapshot.fk_post_collection,
            edit_user,
            edit_timestamp: post_collection_edit_history_snapshot.edit_timestamp,
            title: post_collection_edit_history_snapshot.title,
            title_changed: post_collection_edit_history_snapshot.title_changed,
            public: post_collection_edit_history_snapshot.public,
            public_changed: post_collection_edit_history_snapshot.public_changed,
            public_edit: post_collection_edit_history_snapshot.public_edit,
            public_edit_changed: post_collection_edit_history_snapshot.public_edit_changed,
            description: post_collection_edit_history_snapshot.description,
            description_changed: post_collection_edit_history_snapshot.description_changed,
            poster_object_key: post_collection_edit_history_snapshot.poster_object_key,
            poster_object_key_changed: post_collection_edit_history_snapshot
                .poster_object_key_changed,
            tags_changed: post_collection_edit_history_snapshot.tags_changed,
            group_access_changed: post_collection_edit_history_snapshot.group_access_changed,
            tags,
            group_access,
        });
    }

    Ok(warp::reply::json(&PostCollectionEditHistoryResponse {
        edit_timestamp: post_collection.edit_timestamp,
        edit_user,
        total_snapshot_count,
        snapshots,
    }))
}

async fn get_post_collection_snapshot_tags(
    post_collection_edit_history_snapshot: &PostCollectionEditHistory,
    post_collection: &PostCollection,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<Tag>, Error> {
    // snaphots store the previous value and only store tags if they have changed,
    // thus we need to load the tags from the next snapshot where tags_changed is true,
    // or the collection's current tags if no such snapshot exists
    let relevant_tag_snapshot = if post_collection_edit_history_snapshot.tags_changed {
        Some(post_collection_edit_history_snapshot.clone())
    } else {
        PostCollectionEditHistory::belonging_to(post_collection)
            .filter(post_collection_edit_history::pk.gt(post_collection_edit_history_snapshot.pk))
            .filter(post_collection_edit_history::tags_changed)
            .order(post_collection_edit_history::pk.asc())
            .first::<PostCollectionEditHistory>(connection)
            .await
            .optional()
            .map_err(Error::from)?
    };
    let tags = if let Some(relevant_tag_snapshot) = relevant_tag_snapshot {
        PostCollectionEditHistoryTag::belonging_to(&relevant_tag_snapshot)
            .inner_join(tag::table)
            .select(tag::table::all_columns())
            .load::<Tag>(connection)
            .await
            .map_err(Error::from)?
    } else {
        get_post_collection_tags(post_collection.pk, connection)
            .await
            .map_err(Error::from)?
    };

    Ok(tags)
}

async fn get_post_collection_snapshot_group_access(
    post_collection_edit_history_snapshot: &PostCollectionEditHistory,
    post_collection: &PostCollection,
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<PostCollectionGroupAccessDetailed>, Error> {
    // same as above, but for group access
    let relevant_group_access_snapshot = if post_collection_edit_history_snapshot
        .group_access_changed
    {
        Some(post_collection_edit_history_snapshot.clone())
    } else {
        PostCollectionEditHistory::belonging_to(post_collection)
            .filter(post_collection_edit_history::pk.gt(post_collection_edit_history_snapshot.pk))
            .filter(post_collection_edit_history::group_access_changed)
            .order(post_collection_edit_history::pk.asc())
            .first::<PostCollectionEditHistory>(connection)
            .await
            .optional()
            .map_err(Error::from)?
    };
    let group_access = if let Some(relevant_group_access_snapshot) = relevant_group_access_snapshot
    {
        PostCollectionEditHistoryGroupAccess::belonging_to(&relevant_group_access_snapshot)
            .inner_join(user_group::table)
            .filter(not(user_group::hidden).or(perms::get_group_membership_condition!(user.pk)))
            .load::<(PostCollectionEditHistoryGroupAccess, UserGroup)>(connection)
            .await
            .map_err(Error::from)?
            .into_iter()
            .map(
                |(group_access, user_group)| PostCollectionGroupAccessDetailed {
                    fk_post_collection: post_collection.pk,
                    write: group_access.write,
                    fk_granted_by: group_access.fk_granted_by,
                    creation_timestamp: group_access.creation_timestamp,
                    granted_group: user_group,
                },
            )
            .collect::<Vec<_>>()
    } else {
        get_post_collection_group_access(post_collection.pk, Some(user), connection)
            .await
            .map_err(Error::from)?
    };

    Ok(group_access)
}

pub async fn rewind_post_history_snapshot_handler(
    post_edit_history_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let post = run_serializable_transaction(&mut connection, |connection| {
        async {
            let post_snapshot = post_edit_history::table
                .find(post_edit_history_pk)
                .get_result::<PostEditHistory>(connection)
                .await
                .optional()
                .map_err(Error::from)?
                .ok_or(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(post_edit_history_pk),
                ))?;

            let PostJoined {
                post,
                create_user: _,
                s3_object: _,
                s3_object_metadata: _,
                edit_user: _,
            } = perms::load_post_secured(post_snapshot.fk_post, connection, Some(&user)).await?;
            if !post.is_editable(Some(&user), connection).await? {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(post_snapshot.fk_post),
                ));
            }

            let update = PostUpdateOptional {
                // `None` means "don't update this field", so if the field was empty in the snapshot, use an empty string to clear it
                data_url: Some(post_snapshot.data_url.clone().unwrap_or_default()),
                source_url: Some(post_snapshot.source_url.clone().unwrap_or_default()),
                title: Some(post_snapshot.title.clone().unwrap_or_default()),
                public: Some(post_snapshot.public),
                public_edit: Some(post_snapshot.public_edit),
                description: Some(post_snapshot.description.clone().unwrap_or_default()),
            };

            let mut curr_tags = get_post_tags(post.pk, connection).await?;
            let mut snapshot_tags =
                get_post_snapshot_tags(&post_snapshot, &post, connection).await?;
            let previous_tags = if !vec_eq_sorted(&mut curr_tags, &mut snapshot_tags) {
                diesel::delete(post_tag::table.filter(post_tag::fk_post.eq(post.pk)))
                    .execute(connection)
                    .await
                    .map_err(Error::from)?;
                diesel::insert_into(post_tag::table)
                    .values(
                        snapshot_tags
                            .iter()
                            .map(|tag| PostTag {
                                fk_post: post.pk,
                                fk_tag: tag.pk,
                            })
                            .collect::<Vec<_>>(),
                    )
                    .execute(connection)
                    .await
                    .map_err(Error::from)?;
                Some(curr_tags)
            } else {
                None
            };

            let curr_group_access = get_post_group_access(post.pk, Some(&user), connection).await?;
            let snapshot_group_access =
                get_post_snapshot_group_access(&post_snapshot, &post, &user, connection).await?;

            let mut group_access_to_remove = Vec::new();
            let mut group_access_to_add = Vec::new();
            for group_access in curr_group_access.iter() {
                if !snapshot_group_access.iter().any(|snapshot_group_access| {
                    snapshot_group_access.granted_group.pk == group_access.granted_group.pk
                        && snapshot_group_access.write == group_access.write
                }) {
                    group_access_to_remove.push(group_access.granted_group.pk);
                }
            }
            for group_access in snapshot_group_access {
                if !curr_group_access.iter().any(|curr_group_access| {
                    curr_group_access.granted_group.pk == group_access.granted_group.pk
                        && curr_group_access.write == group_access.write
                }) {
                    group_access_to_add.push(group_access);
                }
            }

            let mut removed_groups = 0;
            if !group_access_to_remove.is_empty() {
                removed_groups =
                    diesel::delete(post_group_access::table.filter(
                        post_group_access::fk_post.eq(post.pk).and(
                            post_group_access::fk_granted_group.eq_any(group_access_to_remove),
                        ),
                    ))
                    .execute(connection)
                    .await
                    .map_err(Error::from)?;
            }

            let mut added_groups = 0;
            if !group_access_to_add.is_empty() {
                added_groups = diesel::insert_into(post_group_access::table)
                    .values(
                        group_access_to_add
                            .iter()
                            .map(|group_access| PostGroupAccess {
                                fk_post: post.pk,
                                fk_granted_group: group_access.granted_group.pk,
                                write: group_access.write,
                                fk_granted_by: group_access.fk_granted_by,
                                creation_timestamp: group_access.creation_timestamp,
                            })
                            .collect::<Vec<_>>(),
                    )
                    .on_conflict((
                        post_group_access::fk_post,
                        post_group_access::fk_granted_group,
                    ))
                    .do_update()
                    .set(post_group_access::write.eq(excluded(post_group_access::write)))
                    .execute(connection)
                    .await
                    .map_err(Error::from)?;
            }

            let previous_group_access = if removed_groups > 0 || added_groups > 0 {
                Some(curr_group_access)
            } else {
                None
            };

            let update_field_changes = update.get_field_changes(&post);
            let ret = if update_field_changes.has_changes() {
                let updated_post = diesel::update(post::table)
                    .filter(post::pk.eq(post.pk))
                    .set(&update)
                    .get_result::<Post>(connection)
                    .await?;

                Ok(updated_post)
            } else {
                Ok(post.clone())
            };

            if update_field_changes.has_changes()
                || previous_tags.is_some()
                || previous_group_access.is_some()
            {
                let post_edit_history = diesel::insert_into(post_edit_history::table)
                    .values(NewPostEditHistory {
                        fk_post: post.pk,
                        fk_edit_user: post.fk_edit_user,
                        edit_timestamp: post.edit_timestamp,
                        data_url_changed: update_field_changes.data_url_changed,
                        data_url: post.data_url,
                        source_url_changed: update_field_changes.source_url_changed,
                        source_url: post.source_url,
                        title_changed: update_field_changes.title_changed,
                        title: post.title,
                        public_changed: update_field_changes.public_changed,
                        public: post.public,
                        public_edit_changed: update_field_changes.public_edit_changed,
                        public_edit: post.public_edit,
                        description_changed: update_field_changes.description_changed,
                        description: post.description,
                        tags_changed: previous_tags.is_some(),
                        group_access_changed: previous_group_access.is_some(),
                    })
                    .get_result::<PostEditHistory>(connection)
                    .await?;

                if let Some(previous_tags) = previous_tags {
                    diesel::insert_into(post_edit_history_tag::table)
                        .values(
                            previous_tags
                                .iter()
                                .map(|tag| PostEditHistoryTag {
                                    fk_post_edit_history: post_edit_history.pk,
                                    fk_tag: tag.pk,
                                })
                                .collect::<Vec<_>>(),
                        )
                        .execute(connection)
                        .await?;
                }

                if let Some(previous_group_access) = previous_group_access {
                    diesel::insert_into(post_edit_history_group_access::table)
                        .values(
                            previous_group_access
                                .iter()
                                .map(|group_access| PostEditHistoryGroupAccess {
                                    fk_post_edit_history: post_edit_history.pk,
                                    fk_granted_group: group_access.granted_group.pk,
                                    write: group_access.write,
                                    fk_granted_by: group_access.fk_granted_by,
                                    creation_timestamp: group_access.creation_timestamp,
                                })
                                .collect::<Vec<_>>(),
                        )
                        .execute(connection)
                        .await?;
                }

                diesel::update(post::table)
                    .filter(post::pk.eq(post.pk))
                    .set((
                        post::edit_timestamp.eq(Utc::now()),
                        post::fk_edit_user.eq(user.pk),
                    ))
                    .execute(connection)
                    .await?;
            }

            ret
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(
        &load_post_detailed(post, Some(&user), &mut connection).await?,
    ))
}

pub async fn rewind_post_collection_history_snapshot_handler(
    post_collection_edit_history_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let post_collection = run_serializable_transaction(&mut connection, |connection| {
        async {
            let post_collection_snapshot = post_collection_edit_history::table
                .find(post_collection_edit_history_pk)
                .get_result::<PostCollectionEditHistory>(connection)
                .await
                .optional()
                .map_err(Error::from)?
                .ok_or(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(post_collection_edit_history_pk),
                ))?;

            let PostCollectionJoined {
                post_collection,
                create_user: _,
                poster_object: _,
                edit_user: _,
            } = perms::load_post_collection_secured(
                post_collection_snapshot.fk_post_collection,
                connection,
                Some(&user),
            )
            .await?;
            if !post_collection.is_editable(Some(&user), connection).await? {
                return Err(TransactionRuntimeError::Rollback(
                    Error::InaccessibleObjectError(post_collection_snapshot.fk_post_collection),
                ));
            }

            let update = PostCollectionUpdateOptional {
                // `None` means "don't update this field", so if the field was empty in the snapshot, use an empty string to clear it
                title: Some(post_collection_snapshot.title.clone()),
                public: Some(post_collection_snapshot.public),
                public_edit: Some(post_collection_snapshot.public_edit),
                poster_object_key: Some(
                    post_collection_snapshot
                        .poster_object_key
                        .clone()
                        .unwrap_or_default(),
                ),
                description: Some(
                    post_collection_snapshot
                        .description
                        .clone()
                        .unwrap_or_default(),
                ),
            };

            let mut curr_tags = get_post_collection_tags(post_collection.pk, connection).await?;
            let mut snapshot_tags = get_post_collection_snapshot_tags(
                &post_collection_snapshot,
                &post_collection,
                connection,
            )
            .await?;
            let previous_tags = if !vec_eq_sorted(&mut curr_tags, &mut snapshot_tags) {
                diesel::delete(
                    post_collection_tag::table
                        .filter(post_collection_tag::fk_post_collection.eq(post_collection.pk)),
                )
                .execute(connection)
                .await
                .map_err(Error::from)?;
                diesel::insert_into(post_collection_tag::table)
                    .values(
                        snapshot_tags
                            .iter()
                            .map(|tag| PostCollectionTag {
                                fk_post_collection: post_collection.pk,
                                fk_tag: tag.pk,
                            })
                            .collect::<Vec<_>>(),
                    )
                    .execute(connection)
                    .await
                    .map_err(Error::from)?;
                Some(curr_tags)
            } else {
                None
            };

            let curr_group_access =
                get_post_collection_group_access(post_collection.pk, Some(&user), connection)
                    .await?;
            let snapshot_group_access = get_post_collection_snapshot_group_access(
                &post_collection_snapshot,
                &post_collection,
                &user,
                connection,
            )
            .await?;

            let mut group_access_to_remove = Vec::new();
            let mut group_access_to_add = Vec::new();
            for group_access in curr_group_access.iter() {
                if !snapshot_group_access.iter().any(|snapshot_group_access| {
                    snapshot_group_access.granted_group.pk == group_access.granted_group.pk
                        && snapshot_group_access.write == group_access.write
                }) {
                    group_access_to_remove.push(group_access.granted_group.pk);
                }
            }
            for group_access in snapshot_group_access {
                if !curr_group_access.iter().any(|curr_group_access| {
                    curr_group_access.granted_group.pk == group_access.granted_group.pk
                        && curr_group_access.write == group_access.write
                }) {
                    group_access_to_add.push(group_access);
                }
            }

            let mut removed_groups = 0;
            if !group_access_to_remove.is_empty() {
                removed_groups = diesel::delete(
                    post_collection_group_access::table.filter(
                        post_collection_group_access::fk_post_collection
                            .eq(post_collection.pk)
                            .and(
                                post_collection_group_access::fk_granted_group
                                    .eq_any(group_access_to_remove),
                            ),
                    ),
                )
                .execute(connection)
                .await
                .map_err(Error::from)?;
            }

            let mut added_groups = 0;
            if !group_access_to_add.is_empty() {
                added_groups = diesel::insert_into(post_collection_group_access::table)
                    .values(
                        group_access_to_add
                            .iter()
                            .map(|group_access| PostCollectionGroupAccess {
                                fk_post_collection: post_collection.pk,
                                fk_granted_group: group_access.granted_group.pk,
                                write: group_access.write,
                                fk_granted_by: group_access.fk_granted_by,
                                creation_timestamp: group_access.creation_timestamp,
                            })
                            .collect::<Vec<_>>(),
                    )
                    .on_conflict((
                        post_collection_group_access::fk_post_collection,
                        post_collection_group_access::fk_granted_group,
                    ))
                    .do_update()
                    .set(
                        post_collection_group_access::write
                            .eq(excluded(post_collection_group_access::write)),
                    )
                    .execute(connection)
                    .await
                    .map_err(Error::from)?;
            }

            let previous_group_access = if removed_groups > 0 || added_groups > 0 {
                Some(curr_group_access)
            } else {
                None
            };

            let update_field_changes = update.get_field_changes(&post_collection);
            let ret = if update_field_changes.has_changes() {
                let updated_post_collection = diesel::update(post_collection::table)
                    .filter(post_collection::pk.eq(post_collection.pk))
                    .set(&update)
                    .get_result::<PostCollection>(connection)
                    .await?;

                Ok(updated_post_collection)
            } else {
                Ok(post_collection.clone())
            };

            if update_field_changes.has_changes()
                || previous_tags.is_some()
                || previous_group_access.is_some()
            {
                let post_collection_edit_history =
                    diesel::insert_into(post_collection_edit_history::table)
                        .values(NewPostCollectionEditHistory {
                            fk_post_collection: post_collection.pk,
                            fk_edit_user: post_collection.fk_edit_user,
                            edit_timestamp: post_collection.edit_timestamp,
                            title_changed: update_field_changes.title_changed,
                            title: post_collection.title,
                            public_changed: update_field_changes.public_changed,
                            public: post_collection.public,
                            public_edit_changed: update_field_changes.public_edit_changed,
                            public_edit: post_collection.public_edit,
                            description_changed: update_field_changes.description_changed,
                            description: post_collection.description,
                            poster_object_key_changed: update_field_changes
                                .poster_object_key_changed,
                            poster_object_key: post_collection.poster_object_key,
                            tags_changed: previous_tags.is_some(),
                            group_access_changed: previous_group_access.is_some(),
                        })
                        .get_result::<PostCollectionEditHistory>(connection)
                        .await?;

                if let Some(previous_tags) = previous_tags {
                    diesel::insert_into(post_collection_edit_history_tag::table)
                        .values(
                            previous_tags
                                .iter()
                                .map(|tag| PostCollectionEditHistoryTag {
                                    fk_post_collection_edit_history: post_collection_edit_history
                                        .pk,
                                    fk_tag: tag.pk,
                                })
                                .collect::<Vec<_>>(),
                        )
                        .execute(connection)
                        .await?;
                }

                if let Some(previous_group_access) = previous_group_access {
                    diesel::insert_into(post_collection_edit_history_group_access::table)
                        .values(
                            previous_group_access
                                .iter()
                                .map(|group_access| PostCollectionEditHistoryGroupAccess {
                                    fk_post_collection_edit_history: post_collection_edit_history
                                        .pk,
                                    fk_granted_group: group_access.granted_group.pk,
                                    write: group_access.write,
                                    fk_granted_by: group_access.fk_granted_by,
                                    creation_timestamp: group_access.creation_timestamp,
                                })
                                .collect::<Vec<_>>(),
                        )
                        .execute(connection)
                        .await?;
                }

                diesel::update(post_collection::table)
                    .filter(post_collection::pk.eq(post_collection.pk))
                    .set((
                        post_collection::edit_timestamp.eq(Utc::now()),
                        post_collection::fk_edit_user.eq(user.pk),
                    ))
                    .execute(connection)
                    .await?;
            }

            ret
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(
        &load_post_collection_detailed(post_collection, Some(&user), &mut connection).await?,
    ))
}
