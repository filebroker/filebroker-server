use std::collections::HashSet;

use chrono::Utc;
use diesel::{
    dsl::{exists, not},
    JoinOnDsl, NullableExpressionMethods, QueryDsl,
};
use diesel_async::{scoped_futures::ScopedFutureExt, AsyncPgConnection, RunQueryDsl};
use serde::Deserialize;
use warp::{Rejection, Reply};

use crate::{
    acquire_db_connection,
    diesel::{BoolExpressionMethods, ExpressionMethods, OptionalExtension},
    error::{Error, TransactionRuntimeError},
    model::{
        Broker, NewUserGroup, Post, PostCollection, PostCollectionItem, S3Object, S3ObjectMetadata,
        User, UserGroup,
    },
    run_retryable_transaction,
    schema::{
        self, broker, broker_access, post, post_collection, post_collection_group_access,
        post_collection_item, post_group_access, registered_user, s3_object, s3_object_metadata,
        user_group, user_group_membership,
    },
};

pub fn append_secure_query_condition(
    where_expressions: &mut Vec<String>,
    user: &Option<User>,
    query_parameters: &QueryParameters,
) {
    let user_key = user
        .as_ref()
        .map(|u| u.pk.to_string())
        .unwrap_or_else(|| String::from("NULL"));

    let base_table_name = query_parameters.base_table_name;
    let public_edit_cond = if query_parameters.writable_only {
        format!("{base_table_name}.public_edit")
    } else {
        String::from("TRUE")
    };
    let group_access_write_cond = if query_parameters.writable_only {
        format!("{base_table_name}_group_access.write")
    } else {
        String::from("TRUE")
    };

    if user.is_some() {
        where_expressions.push(format!(
            r#"
            ({base_table_name}.fk_create_user = {user_key}
            OR ({base_table_name}.public AND {public_edit_cond})
            OR EXISTS(
                SELECT * FROM {base_table_name}_group_access
                WHERE {base_table_name}_group_access.fk_{base_table_name} = {base_table_name}.pk
                AND {group_access_write_cond}
                AND {base_table_name}_group_access.fk_granted_group IN(
                    SELECT pk FROM user_group
                    WHERE fk_owner = {user_key}
                    OR EXISTS(
                        SELECT * FROM user_group_membership
                        WHERE NOT revoked AND fk_user = {user_key} AND fk_group = user_group.pk
                    )
                )
            ))"#
        ));
    } else {
        where_expressions.push(format!(
            r#"
            ({base_table_name}.fk_create_user = {user_key}
            OR ({base_table_name}.public AND {public_edit_cond}))"#
        ));
    }
}

macro_rules! get_group_membership_condition {
    ($user_pk:expr) => {
        user_group::fk_owner.nullable().eq($user_pk).or(exists(
            user_group_membership::table.filter(
                user_group_membership::fk_group
                    .eq(user_group::pk)
                    .and(user_group_membership::fk_user.nullable().eq($user_pk))
                    .and(not(user_group_membership::revoked)),
            ),
        ))
    };
}

pub(crate) use get_group_membership_condition;

macro_rules! get_group_access_condition {
    ($fk:expr, $target:expr, $user_pk:expr, $table:ident) => {
        $fk.eq($target).and(
            $table::fk_granted_group.eq_any(
                user_group::table
                    .select(user_group::pk)
                    .filter(get_group_membership_condition!($user_pk)),
            ),
        )
    };
}

pub(crate) use get_group_access_condition;

macro_rules! get_group_access_or_public_condition {
    ($fk:expr, $target:expr, $user_pk:expr, $public_cond:expr, $group_fk:expr) => {
        $fk.eq($target).and(
            $public_cond.or($group_fk.eq_any(
                user_group::table
                    .select(user_group::pk)
                    .nullable()
                    .filter(get_group_membership_condition!($user_pk)),
            )),
        )
    };
}

pub(crate) use get_group_access_or_public_condition;

macro_rules! get_group_access_write_condition {
    ($fk:expr, $target:expr, $user_pk:expr, $table:ident) => {
        $fk.eq($target).and($table::write).and(
            $table::fk_granted_group.eq_any(
                user_group::table
                    .select(user_group::pk)
                    .filter(get_group_membership_condition!($user_pk)),
            ),
        )
    };
}

macro_rules! get_broker_group_access_write_condition {
    ($user_pk:expr) => {
        broker_access::fk_broker
            .eq(broker::pk)
            .and(broker_access::write)
            .and(
                broker_access::fk_granted_group
                    .is_null()
                    .or(broker_access::fk_granted_group.eq_any(
                        user_group::table
                            .select(user_group::pk)
                            .filter(get_group_membership_condition!($user_pk))
                            .nullable(),
                    )),
            )
    };
}

pub(crate) use get_broker_group_access_write_condition;

use crate::query::QueryParameters;
pub(crate) use get_group_access_write_condition;

pub struct PostJoined {
    pub post: Post,
    pub create_user: User,
    pub s3_object: S3Object,
    pub s3_object_metadata: S3ObjectMetadata,
}

pub struct PostCollectionJoined {
    pub post_collection: PostCollection,
    pub create_user: User,
    pub poster_object: Option<S3Object>,
}

pub struct PostCollectionItemJoined {
    pub post_collection_item: PostCollectionItem,
    pub added_by: User,
    pub post: PostJoined,
    pub post_collection: PostCollectionJoined,
}

pub async fn load_post_secured(
    post_pk: i64,
    connection: &mut AsyncPgConnection,
    user: Option<&User>,
) -> Result<PostJoined, Error> {
    let user_pk = user.map(|u| u.pk);
    post::table
        .inner_join(registered_user::table)
        .inner_join(s3_object::table)
        .inner_join(
            s3_object_metadata::table.on(s3_object::object_key.eq(s3_object_metadata::object_key)),
        )
        .filter(
            post::pk.eq(post_pk).and(
                post::fk_create_user
                    .nullable()
                    .eq(&user_pk)
                    .or(post::public)
                    .or(exists(post_group_access::table.filter(
                        get_group_access_condition!(
                            post_group_access::fk_post,
                            post::pk,
                            &user_pk,
                            post_group_access
                        ),
                    ))),
            ),
        )
        .get_result::<(Post, User, S3Object, S3ObjectMetadata)>(connection)
        .await
        .optional()?
        .ok_or(Error::InaccessibleObjectError(post_pk))
        .map(|tuple| PostJoined {
            post: tuple.0,
            create_user: tuple.1,
            s3_object: tuple.2,
            s3_object_metadata: tuple.3,
        })
}

pub async fn load_post_collection_item_secured(
    post_collection_pk: i64,
    post_collection_item_pk: i64,
    connection: &mut AsyncPgConnection,
    user: Option<&User>,
) -> Result<PostCollectionItemJoined, Error> {
    let user_pk = user.map(|u| u.pk);
    let (added_by_user, post_create_user, post_collection_create_user) = diesel::alias!(
        schema::registered_user as added_by_user,
        schema::registered_user as post_create_user,
        schema::registered_user as post_collection_create_user
    );
    let (post_s3_object, post_collection_poster_object) = diesel::alias!(
        schema::s3_object as post_s3_object,
        schema::s3_object as post_collection_poster_object
    );
    post_collection_item::table
        .inner_join(
            added_by_user
                .on(post_collection_item::fk_added_by.eq(added_by_user.field(registered_user::pk))),
        )
        .inner_join(post::table.on(post_collection_item::fk_post.eq(post::pk)))
        .inner_join(
            post_create_user
                .on(post::fk_create_user.eq(post_create_user.field(registered_user::pk))),
        )
        .inner_join(
            post_s3_object.on(post::s3_object.eq(post_s3_object.field(s3_object::object_key))),
        )
        .inner_join(
            post_collection::table
                .on(post_collection_item::fk_post_collection.eq(post_collection::pk)),
        )
        .inner_join(
            post_collection_create_user.on(post_collection::fk_create_user
                .eq(post_collection_create_user.field(registered_user::pk))),
        )
        .left_join(
            post_collection_poster_object.on(post_collection::poster_object_key.eq(
                post_collection_poster_object
                    .field(s3_object::object_key)
                    .nullable(),
            )),
        )
        .inner_join(
            s3_object_metadata::table.on(post_s3_object
                .field(s3_object::object_key)
                .eq(s3_object_metadata::object_key)),
        )
        .filter(
            post_collection_item::pk
                .eq(post_collection_item_pk)
                .and(post_collection::pk.eq(post_collection_pk))
                .and(
                    post::fk_create_user
                        .nullable()
                        .eq(&user_pk)
                        .or(post::public)
                        .or(exists(post_group_access::table.filter(
                            get_group_access_condition!(
                                post_group_access::fk_post,
                                post::pk,
                                &user_pk,
                                post_group_access
                            ),
                        ))),
                )
                .and(
                    post_collection::fk_create_user
                        .nullable()
                        .eq(&user_pk)
                        .or(post_collection::public)
                        .or(exists(post_collection_group_access::table.filter(
                            get_group_access_condition!(
                                post_collection_group_access::fk_post_collection,
                                post_collection::pk,
                                &user_pk,
                                post_collection_group_access
                            ),
                        ))),
                ),
        )
        .get_result::<(
            PostCollectionItem,
            User,
            Post,
            User,
            S3Object,
            PostCollection,
            User,
            Option<S3Object>,
            S3ObjectMetadata,
        )>(connection)
        .await
        .optional()?
        .ok_or(Error::InaccessibleObjectError(post_collection_item_pk))
        .map(|tuple| PostCollectionItemJoined {
            post_collection_item: tuple.0,
            added_by: tuple.1,
            post: PostJoined {
                post: tuple.2,
                create_user: tuple.3,
                s3_object: tuple.4,
                s3_object_metadata: tuple.8,
            },
            post_collection: PostCollectionJoined {
                post_collection: tuple.5,
                create_user: tuple.6,
                poster_object: tuple.7,
            },
        })
}

pub async fn load_posts_secured(
    post_pks: &[i64],
    connection: &mut AsyncPgConnection,
    user: Option<&User>,
) -> Result<Vec<PostJoined>, Error> {
    let user_pk = user.map(|u| u.pk);
    let results = post::table
        .inner_join(registered_user::table)
        .inner_join(s3_object::table)
        .inner_join(
            s3_object_metadata::table.on(s3_object::object_key.eq(s3_object_metadata::object_key)),
        )
        .filter(
            post::pk.eq_any(post_pks).and(
                post::fk_create_user
                    .nullable()
                    .eq(&user_pk)
                    .or(post::public)
                    .or(exists(post_group_access::table.filter(
                        get_group_access_condition!(
                            post_group_access::fk_post,
                            post::pk,
                            &user_pk,
                            post_group_access
                        ),
                    ))),
            ),
        )
        .load::<(Post, User, S3Object, S3ObjectMetadata)>(connection)
        .await?;

    let found_pks = results.iter().map(|res| res.0.pk).collect::<HashSet<_>>();
    let missing_pks = post_pks
        .iter()
        .filter(|pk| !found_pks.contains(pk))
        .cloned()
        .collect::<Vec<_>>();
    if missing_pks.is_empty() {
        Ok(results
            .into_iter()
            .map(|tuple| PostJoined {
                post: tuple.0,
                create_user: tuple.1,
                s3_object: tuple.2,
                s3_object_metadata: tuple.3,
            })
            .collect::<Vec<_>>())
    } else {
        Err(Error::InaccessibleObjectsError(missing_pks))
    }
}

pub async fn load_post_collection_secured(
    post_collection_pk: i64,
    connection: &mut AsyncPgConnection,
    user: Option<&User>,
) -> Result<PostCollectionJoined, Error> {
    let user_pk = user.map(|u| u.pk);
    post_collection::table
        .inner_join(registered_user::table)
        .left_join(s3_object::table)
        .filter(
            post_collection::pk.eq(post_collection_pk).and(
                post_collection::fk_create_user
                    .nullable()
                    .eq(&user_pk)
                    .or(post_collection::public)
                    .or(exists(post_collection_group_access::table.filter(
                        get_group_access_condition!(
                            post_collection_group_access::fk_post_collection,
                            post_collection::pk,
                            &user_pk,
                            post_collection_group_access
                        ),
                    ))),
            ),
        )
        .get_result::<(PostCollection, User, Option<S3Object>)>(connection)
        .await
        .optional()?
        .ok_or(Error::InaccessibleObjectError(post_collection_pk))
        .map(|tuple| PostCollectionJoined {
            post_collection: tuple.0,
            create_user: tuple.1,
            poster_object: tuple.2,
        })
}

pub struct PostJoinedS3Object {
    pub post: Post,
    pub create_user: User,
    pub s3_object: S3Object,
    pub s3_object_metadata: S3ObjectMetadata,
}

pub async fn load_s3_object_posts(
    s3_object_key: &str,
    user_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<PostJoinedS3Object>, Error> {
    post::table
        .inner_join(registered_user::table)
        .inner_join(s3_object::table)
        .inner_join(
            s3_object_metadata::table.on(s3_object::object_key.eq(s3_object_metadata::object_key)),
        )
        .filter(
            post::s3_object.eq(s3_object_key).and(
                post::fk_create_user
                    .nullable()
                    .eq(user_pk)
                    .or(post::public)
                    .or(exists(post_group_access::table.filter(
                        get_group_access_condition!(
                            post_group_access::fk_post,
                            post::pk,
                            user_pk,
                            post_group_access
                        ),
                    ))),
            ),
        )
        .load::<(Post, User, S3Object, S3ObjectMetadata)>(connection)
        .await
        .map_err(|e| Error::QueryError(e.to_string()))
        .map(|tuples| {
            tuples
                .into_iter()
                .map(|tuple| PostJoinedS3Object {
                    post: tuple.0,
                    create_user: tuple.1,
                    s3_object: tuple.2,
                    s3_object_metadata: tuple.3,
                })
                .collect::<Vec<_>>()
        })
}

pub async fn load_broker_secured(
    broker_pk: i64,
    connection: &mut AsyncPgConnection,
    user: Option<&User>,
) -> Result<Broker, Error> {
    let user_pk = user.map(|u| u.pk);
    broker::table
        .filter(
            broker::pk
                .eq(broker_pk)
                .and(broker::fk_owner.nullable().eq(&user_pk).or(exists(
                    broker_access::table.filter(get_group_access_or_public_condition!(
                        broker_access::fk_broker,
                        broker::pk,
                        &user_pk,
                        broker_access::fk_granted_group.is_null(),
                        broker_access::fk_granted_group
                    )),
                ))),
        )
        .get_result::<Broker>(connection)
        .await
        .optional()?
        .ok_or(Error::InaccessibleObjectError(broker_pk))
}

pub async fn get_brokers_secured(
    connection: &mut AsyncPgConnection,
    user: Option<&User>,
) -> Result<Vec<Broker>, Error> {
    let user_pk = user.map(|u| u.pk);
    broker::table
        .filter(
            broker::fk_owner
                .nullable()
                .eq(&user_pk)
                .or(exists(broker_access::table.filter(
                    get_group_access_or_public_condition!(
                        broker_access::fk_broker,
                        broker::pk,
                        &user_pk,
                        broker_access::fk_granted_group.is_null(),
                        broker_access::fk_granted_group
                    ),
                ))),
        )
        .load::<Broker>(connection)
        .await
        .map_err(|e| Error::QueryError(e.to_string()))
}

pub async fn get_user_groups_secured(
    connection: &mut AsyncPgConnection,
    user: Option<&User>,
) -> Result<Vec<UserGroup>, Error> {
    let user_pk = user.map(|u| u.pk);
    user_group::table
        .filter(not(user_group::hidden).or(get_group_membership_condition!(user_pk)))
        .load::<UserGroup>(connection)
        .await
        .map_err(|e| Error::QueryError(e.to_string()))
}

pub async fn get_current_user_groups(
    connection: &mut AsyncPgConnection,
    user: &User,
) -> Result<Vec<UserGroup>, Error> {
    user_group::table
        .filter(get_group_membership_condition!(user.pk))
        .load::<UserGroup>(connection)
        .await
        .map_err(|e| Error::QueryError(e.to_string()))
}

pub async fn is_post_editable(
    connection: &mut AsyncPgConnection,
    user: Option<&User>,
    post_pk: i64,
) -> Result<bool, Error> {
    let user_pk = user.map(|user| user.pk);
    post::table
        .filter(
            post::pk.eq(post_pk).and(
                post::public_edit
                    .or(post::fk_create_user.nullable().eq(user_pk))
                    .or(exists(post_group_access::table.filter(
                        get_group_access_write_condition!(
                            post_group_access::fk_post,
                            post::pk,
                            &user_pk,
                            post_group_access
                        ),
                    ))),
            ),
        )
        .get_result::<Post>(connection)
        .await
        .optional()
        .map_err(|e| Error::QueryError(e.to_string()))
        .map(|result| result.is_some())
}

pub async fn is_post_deletable(
    connection: &mut AsyncPgConnection,
    user: Option<&User>,
    post_pk: i64,
) -> Result<bool, Error> {
    let user_pk = user.map(|user| user.pk);
    post::table
        .left_join(s3_object::table)
        .left_join(broker::table.on(s3_object::fk_broker.eq(broker::pk)))
        .select(post::pk)
        .filter(
            post::pk
                .eq(post_pk)
                .and(
                    post::fk_create_user
                        .nullable()
                        .eq(user_pk)
                        .or(broker::fk_owner.nullable().eq(user_pk).or(exists(
                            broker_access::table
                                .filter(get_broker_group_access_write_condition!(user_pk)),
                        ))),
                ),
        )
        .get_result::<i64>(connection)
        .await
        .optional()
        .map_err(|e| Error::QueryError(e.to_string()))
        .map(|result| result.is_some())
}

pub async fn is_post_collection_editable(
    connection: &mut AsyncPgConnection,
    user: Option<&User>,
    post_collection_pk: i64,
) -> Result<bool, Error> {
    let user_pk = user.map(|user| user.pk);
    post_collection::table
        .filter(
            post_collection::pk.eq(post_collection_pk).and(
                post_collection::public_edit
                    .or(post_collection::fk_create_user.nullable().eq(user_pk))
                    .or(exists(post_collection_group_access::table.filter(
                        get_group_access_write_condition!(
                            post_collection_group_access::fk_post_collection,
                            post_collection::pk,
                            &user_pk,
                            post_collection_group_access
                        ),
                    ))),
            ),
        )
        .get_result::<PostCollection>(connection)
        .await
        .optional()
        .map_err(|e| Error::QueryError(e.to_string()))
        .map(|result| result.is_some())
}

pub async fn is_post_collection_deletable(
    connection: &mut AsyncPgConnection,
    user: Option<&User>,
    post_collection_pk: i64,
) -> Result<bool, Error> {
    let user_pk = user.map(|user| user.pk);
    post_collection::table
        .select(post_collection::pk)
        .filter(
            post_collection::pk
                .eq(post_collection_pk)
                .and(post_collection::fk_create_user.nullable().eq(user_pk)),
        )
        .get_result::<i64>(connection)
        .await
        .optional()
        .map_err(|e| Error::QueryError(e.to_string()))
        .map(|result| result.is_some())
}

#[derive(Deserialize)]
pub struct CreateUserGroupRequest {
    pub name: String,
    pub public: bool,
    pub hidden: bool,
}

pub async fn create_user_group_handler(
    request: CreateUserGroupRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let user_group = run_retryable_transaction(&mut connection, |connection| {
        async move {
            let current_groups = get_current_user_groups(connection, &user).await?;
            if current_groups.len() >= 250 {
                return Err(TransactionRuntimeError::from(Error::BadRequestError(
                    String::from("Cannot be a member of more than 250 groups"),
                )));
            }

            Ok(diesel::insert_into(user_group::table)
                .values(&NewUserGroup {
                    name: request.name,
                    public: request.public,
                    hidden: request.hidden,
                    fk_owner: user.pk,
                    creation_timestamp: Utc::now(),
                })
                .get_result::<UserGroup>(connection)
                .await?)
        }
        .scope_boxed()
    })
    .await
    .map_err(Error::from)?;

    Ok(warp::reply::json(&user_group))
}

pub async fn get_user_groups_handler(user: Option<User>) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    Ok(warp::reply::json(
        &get_user_groups_secured(&mut connection, user.as_ref()).await?,
    ))
}

pub async fn get_current_user_groups_handler(user: User) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    Ok(warp::reply::json(
        &get_current_user_groups(&mut connection, &user).await?,
    ))
}
