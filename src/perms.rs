use chrono::Utc;
use diesel::{
    connection::LoadConnection,
    dsl::{exists, not},
    pg::Pg,
    Connection, NullableExpressionMethods, QueryDsl,
};
use serde::Deserialize;
use warp::{Rejection, Reply};

use crate::{
    acquire_db_connection,
    diesel::{BoolExpressionMethods, ExpressionMethods, OptionalExtension, RunQueryDsl},
    error::Error,
    model::{Broker, NewUserGroup, Post, S3Object, User, UserGroup},
    schema::{
        broker, broker_access, post, post_group_access, s3_object, user_group,
        user_group_membership,
    },
};

pub fn append_secure_query_condition(where_expressions: &mut Vec<String>, user: &Option<User>) {
    let user_key = user
        .as_ref()
        .map(|u| u.pk.to_string())
        .unwrap_or_else(|| String::from("NULL"));

    if user.is_some() {
        where_expressions.push(format!(
            r#"
            (post.fk_create_user = {user_key}
            OR post.public
            OR EXISTS(
                SELECT * FROM post_group_access
                WHERE post_group_access.fk_post = post.pk 
                AND post_group_access.fk_granted_group IN(
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
            (post.fk_create_user = {user_key}
            OR post.public)"#
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

macro_rules! get_group_access_or_public_condition {
    ($fk:expr, $target:expr, $user_pk:expr, $table:ident) => {
        $fk.eq($target).and(
            $table::public.or($table::fk_granted_group.eq_any(
                user_group::table
                    .select(user_group::pk)
                    .nullable()
                    .filter(get_group_membership_condition!($user_pk)),
            )),
        )
    };
}

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

pub fn load_post_secured<C: Connection<Backend = Pg> + LoadConnection>(
    post_pk: i32,
    connection: &mut C,
    user: Option<&User>,
) -> Result<(Post, Option<S3Object>), Error> {
    let user_pk = user.map(|u| u.pk);
    post::table
        .left_join(s3_object::table)
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
        .get_result::<(Post, Option<S3Object>)>(connection)
        .optional()?
        .ok_or(Error::InaccessibleObjectError(post_pk))
}

pub fn load_s3_object_posts<C: Connection<Backend = Pg> + LoadConnection>(
    s3_object_key: &str,
    user_pk: i32,
    connection: &mut C,
) -> Result<Vec<(Post, S3Object)>, Error> {
    post::table
        .inner_join(s3_object::table)
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
        .load::<(Post, S3Object)>(connection)
        .map_err(|e| Error::QueryError(e.to_string()))
}

pub fn load_broker_secured<C: Connection<Backend = Pg> + LoadConnection>(
    broker_pk: i32,
    connection: &mut C,
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
                        broker_access
                    )),
                ))),
        )
        .get_result::<Broker>(connection)
        .optional()?
        .ok_or(Error::InaccessibleObjectError(broker_pk))
}

pub fn get_brokers_secured<C: Connection<Backend = Pg> + LoadConnection>(
    connection: &mut C,
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
                        broker_access
                    ),
                ))),
        )
        .load::<Broker>(connection)
        .map_err(|e| Error::QueryError(e.to_string()))
}

pub fn get_user_groups_secured<C: Connection<Backend = Pg> + LoadConnection>(
    connection: &mut C,
    user: Option<&User>,
) -> Result<Vec<UserGroup>, Error> {
    let user_pk = user.map(|u| u.pk);
    user_group::table
        .filter(
            not(user_group::hidden).or(user_group::fk_owner.nullable().eq(user_pk).or(exists(
                user_group_membership::table.filter(
                    user_group_membership::fk_group
                        .eq(user_group::pk)
                        .and(user_group_membership::fk_user.nullable().eq(user_pk)),
                ),
            ))),
        )
        .load::<UserGroup>(connection)
        .map_err(|e| Error::QueryError(e.to_string()))
}

pub fn get_current_user_groups<C: Connection<Backend = Pg> + LoadConnection>(
    connection: &mut C,
    user: &User,
) -> Result<Vec<UserGroup>, Error> {
    user_group::table
        .filter(
            user_group::fk_owner.nullable().eq(user.pk).or(exists(
                user_group_membership::table.filter(
                    user_group_membership::fk_group
                        .eq(user_group::pk)
                        .and(user_group_membership::fk_user.nullable().eq(user.pk)),
                ),
            )),
        )
        .load::<UserGroup>(connection)
        .map_err(|e| Error::QueryError(e.to_string()))
}

pub fn is_post_editable<C: Connection<Backend = Pg> + LoadConnection>(
    connection: &mut C,
    user: Option<&User>,
    post_pk: i32,
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
    let mut connection = acquire_db_connection()?;

    let user_group = connection
        .build_transaction()
        .run(|connection| {
            let current_groups = get_current_user_groups(connection, &user)?;
            if current_groups.len() >= 250 {
                return Err(Error::BadRequestError(String::from(
                    "Cannot be a member of more than 250 groups",
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
                .get_result::<UserGroup>(connection)?)
        })
        .map_err(Error::from)?;

    Ok(warp::reply::json(&user_group))
}

pub async fn get_user_groups_handler(user: Option<User>) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection()?;
    Ok(warp::reply::json(&get_user_groups_secured(
        &mut connection,
        user.as_ref(),
    )?))
}

pub async fn get_current_user_groups_handler(user: User) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection()?;
    Ok(warp::reply::json(&get_current_user_groups(
        &mut connection,
        &user,
    )?))
}
