use diesel::{
    dsl::{any, exists, not},
    NullableExpressionMethods, QueryDsl,
};

use crate::{
    diesel::{BoolExpressionMethods, ExpressionMethods, OptionalExtension, RunQueryDsl},
    error::Error,
    model::{Broker, Post, S3Object, User},
    schema::{broker, permission_target, post, s3_object, user_group, user_group_membership},
    DbConnection,
};

pub fn append_secure_query_condition(where_expressions: &mut Vec<String>, user: &Option<User>) {
    let user_key = user
        .as_ref()
        .map(|u| u.pk.to_string())
        .unwrap_or_else(|| String::from("NULL"));
    where_expressions.push(format!(
        r#"
        post.fk_create_user = {user_key}
        OR EXISTS(
            SELECT pk FROM permission_target
            WHERE fk_post = post.pk 
            AND (
                permission_target.public
                OR permission_target.fk_granted_group IN(
                    SELECT pk FROM user_group
                    WHERE fk_owner = {user_key}
                    OR EXISTS(
                        SELECT * FROM user_group_membership
                        WHERE NOT revoked AND fk_user = {user_key} AND fk_group = user_group.pk
                    )
                )
            )
        )"#
    ));
}

macro_rules! get_permission_target_read_condition {
    ($fk:expr, $target:expr, $user_pk:expr) => {
        $fk.eq($target).and(
            permission_target::public.or(permission_target::fk_granted_group.eq(any(
                user_group::table.select(user_group::pk).nullable().filter(
                    user_group::fk_owner.nullable().eq($user_pk).or(exists(
                        user_group_membership::table.filter(
                            not(user_group_membership::revoked)
                                .and(user_group_membership::fk_user.nullable().eq($user_pk))
                                .and(user_group_membership::fk_group.eq(user_group::pk)),
                        ),
                    )),
                ),
            ))),
        )
    };
}

pub fn load_post_secured(
    post_pk: i32,
    connection: &DbConnection,
    user: Option<&User>,
) -> Result<(Post, Option<S3Object>), Error> {
    let user_pk = user.map(|u| u.pk);
    post::table
        .left_join(s3_object::table)
        .filter(
            post::pk
                .eq(post_pk)
                .and(post::fk_create_user.nullable().eq(&user_pk).or(exists(
                    permission_target::table.filter(get_permission_target_read_condition!(
                        permission_target::fk_post,
                        post::pk.nullable(),
                        &user_pk
                    )),
                ))),
        )
        .get_result::<(Post, Option<S3Object>)>(connection)
        .optional()?
        .ok_or(Error::InaccessibleObjectError(post_pk))
}

pub fn load_broker_secured(
    broker_pk: i32,
    connection: &DbConnection,
    user: Option<&User>,
) -> Result<Broker, Error> {
    let user_pk = user.map(|u| u.pk);
    broker::table
        .filter(
            broker::pk
                .eq(broker_pk)
                .and(broker::fk_owner.nullable().eq(&user_pk).or(exists(
                    permission_target::table.filter(get_permission_target_read_condition!(
                        permission_target::fk_broker,
                        broker::pk.nullable(),
                        &user_pk
                    )),
                ))),
        )
        .get_result::<Broker>(connection)
        .optional()?
        .ok_or(Error::InaccessibleObjectError(broker_pk))
}
