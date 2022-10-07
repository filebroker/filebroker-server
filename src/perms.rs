use diesel::{
    dsl::{exists, not},
    NullableExpressionMethods, QueryDsl,
};

use crate::{
    diesel::{BoolExpressionMethods, ExpressionMethods, OptionalExtension, RunQueryDsl},
    error::Error,
    model::{Broker, Post, S3Object, User},
    schema::{
        broker, broker_access, post, post_group_access, s3_object, user_group,
        user_group_membership,
    },
    DbConnection,
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

macro_rules! get_group_access_read_condition {
    ($fk:expr, $target:expr, $user_pk:expr, $table:ident) => {
        $fk.eq($target).and(
            $table::fk_granted_group.eq_any(
                user_group::table.select(user_group::pk).filter(
                    user_group::fk_owner.nullable().eq($user_pk).or(exists(
                        user_group_membership::table.filter(
                            not(user_group_membership::revoked)
                                .and(user_group_membership::fk_user.nullable().eq($user_pk))
                                .and(user_group_membership::fk_group.eq(user_group::pk)),
                        ),
                    )),
                ),
            ),
        )
    };
}

macro_rules! get_group_access_or_public_read_condition {
    ($fk:expr, $target:expr, $user_pk:expr, $table:ident) => {
        $fk.eq($target).and(
            $table::public.or($table::fk_granted_group.eq_any(
                user_group::table.select(user_group::pk).nullable().filter(
                    user_group::fk_owner.nullable().eq($user_pk).or(exists(
                        user_group_membership::table.filter(
                            not(user_group_membership::revoked)
                                .and(user_group_membership::fk_user.nullable().eq($user_pk))
                                .and(user_group_membership::fk_group.eq(user_group::pk)),
                        ),
                    )),
                ),
            )),
        )
    };
}

pub fn load_post_secured(
    post_pk: i32,
    connection: &mut DbConnection,
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
                        get_group_access_read_condition!(
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

pub fn load_broker_secured(
    broker_pk: i32,
    connection: &mut DbConnection,
    user: Option<&User>,
) -> Result<Broker, Error> {
    let user_pk = user.map(|u| u.pk);
    broker::table
        .filter(
            broker::pk
                .eq(broker_pk)
                .and(broker::fk_owner.nullable().eq(&user_pk).or(exists(
                    broker_access::table.filter(get_group_access_or_public_read_condition!(
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

pub fn get_brokers_secured(
    connection: &mut DbConnection,
    user: Option<&User>,
) -> Result<Vec<Broker>, Error> {
    let user_pk = user.map(|u| u.pk);
    broker::table
        .filter(
            broker::fk_owner
                .nullable()
                .eq(&user_pk)
                .or(exists(broker_access::table.filter(
                    get_group_access_or_public_read_condition!(
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
