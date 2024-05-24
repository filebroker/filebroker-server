use chrono::{DateTime, Utc};
use diesel::{
    dsl::{exists, not},
    BoolExpressionMethods, JoinOnDsl, OptionalExtension, Table,
};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};

use crate::{
    diesel::{ExpressionMethods, NullableExpressionMethods, QueryDsl},
    error::Error,
    model::{PostCollectionGroupAccess, PostGroupAccess, S3Object, Tag, User, UserGroup},
    perms,
    schema::{
        post, post_collection_group_access, post_collection_item, post_collection_tag,
        post_group_access, post_tag, s3_object, tag, user_group, user_group_membership,
    },
};

pub mod create;
pub mod delete;
pub mod update;

async fn report_inaccessible_groups(
    selected_group_access: &[GroupAccessDefinition],
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<(), Error> {
    let group_pks = selected_group_access
        .iter()
        .map(|group_access| group_access.group_pk)
        .collect::<Vec<_>>();
    report_inaccessible_group_pks(&group_pks, user, connection).await
}

async fn report_inaccessible_post_pks(
    post_pks: &[i64],
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<(), Error> {
    perms::load_posts_secured(post_pks, connection, Some(user))
        .await
        .map(|_| ())
}

async fn report_inaccessible_group_pks(
    group_pks: &[i64],
    user: &User,
    connection: &mut AsyncPgConnection,
) -> Result<(), Error> {
    let accessible_group_pks = user_group::table
        .select(user_group::pk)
        .filter(
            user_group::pk
                .eq_any(group_pks)
                .and(perms::get_group_membership_condition!(user.pk)),
        )
        .load::<i64>(connection)
        .await
        .map_err(|e| Error::QueryError(e.to_string()))?;

    let missing_pks = group_pks
        .iter()
        .filter(|pk| !accessible_group_pks.contains(pk))
        .collect::<Vec<_>>();

    if !missing_pks.is_empty() {
        Err(Error::InvalidEntityReferenceError(
            itertools::Itertools::intersperse(
                missing_pks.into_iter().map(i64::to_string),
                String::from(", "),
            )
            .collect::<String>(),
        ))
    } else {
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Clone, Copy, Eq)]
pub struct GroupAccessDefinition {
    pub group_pk: i64,
    pub write: bool,
}

impl PartialEq for GroupAccessDefinition {
    fn eq(&self, other: &Self) -> bool {
        self.group_pk == other.group_pk
    }
}

impl PartialOrd for GroupAccessDefinition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GroupAccessDefinition {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.group_pk.cmp(&other.group_pk)
    }
}

#[derive(Serialize)]
pub struct PostGroupAccessDetailed {
    pub fk_post: i64,
    pub write: bool,
    pub fk_granted_by: i64,
    pub creation_timestamp: DateTime<Utc>,
    pub granted_group: UserGroup,
}

#[derive(Serialize)]
pub struct PostCollectionGroupAccessDetailed {
    pub fk_post_collection: i64,
    pub write: bool,
    pub fk_granted_by: i64,
    pub creation_timestamp: DateTime<Utc>,
    pub granted_group: UserGroup,
}

pub async fn get_post_tags(
    post_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<Tag>, diesel::result::Error> {
    post_tag::table
        .inner_join(tag::table)
        .select(tag::table::all_columns())
        .filter(post_tag::fk_post.eq(post_pk))
        .load::<Tag>(connection)
        .await
}

pub async fn get_post_collection_tags(
    post_collection_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<Tag>, diesel::result::Error> {
    post_collection_tag::table
        .inner_join(tag::table)
        .select(tag::table::all_columns())
        .filter(post_collection_tag::fk_post_collection.eq(post_collection_pk))
        .load::<Tag>(connection)
        .await
}

pub async fn get_post_group_access(
    post_pk: i64,
    user: Option<&User>,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<PostGroupAccessDetailed>, diesel::result::Error> {
    if let Some(user) = user {
        post_group_access::table
            .inner_join(user_group::table)
            .filter(
                post_group_access::fk_post.eq(post_pk).and(
                    not(user_group::hidden).or(perms::get_group_membership_condition!(user.pk)),
                ),
            )
            .load::<(PostGroupAccess, UserGroup)>(connection)
            .await
    } else {
        post_group_access::table
            .inner_join(user_group::table)
            .filter(
                post_group_access::fk_post
                    .eq(post_pk)
                    .and(not(user_group::hidden)),
            )
            .load::<(PostGroupAccess, UserGroup)>(connection)
            .await
    }
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

pub async fn get_post_collection_group_access(
    post_collection_pk: i64,
    user: Option<&User>,
    connection: &mut AsyncPgConnection,
) -> Result<Vec<PostCollectionGroupAccessDetailed>, diesel::result::Error> {
    if let Some(user) = user {
        post_collection_group_access::table
            .inner_join(user_group::table)
            .filter(
                post_collection_group_access::fk_post_collection
                    .eq(post_collection_pk)
                    .and(
                        not(user_group::hidden).or(perms::get_group_membership_condition!(user.pk)),
                    ),
            )
            .load::<(PostCollectionGroupAccess, UserGroup)>(connection)
            .await
    } else {
        post_collection_group_access::table
            .inner_join(user_group::table)
            .filter(
                post_collection_group_access::fk_post_collection
                    .eq(post_collection_pk)
                    .and(not(user_group::hidden)),
            )
            .load::<(PostCollectionGroupAccess, UserGroup)>(connection)
            .await
    }
    .map(|post_collection_group_access_vec| {
        post_collection_group_access_vec
            .into_iter()
            .map(
                |post_collection_group_access| PostCollectionGroupAccessDetailed {
                    fk_post_collection: post_collection_group_access.0.fk_post_collection,
                    write: post_collection_group_access.0.write,
                    fk_granted_by: post_collection_group_access.0.fk_granted_by,
                    creation_timestamp: post_collection_group_access.0.creation_timestamp,
                    granted_group: post_collection_group_access.1,
                },
            )
            .collect::<Vec<_>>()
    })
}

async fn load_post_collection_poster_object(
    post_collection_pk: i64,
    connection: &mut AsyncPgConnection,
) -> Result<Option<S3Object>, Error> {
    post_collection_item::table
        .inner_join(post::table)
        .inner_join(s3_object::table.on(s3_object::object_key.eq(post::s3_object)))
        .select(s3_object::table::all_columns())
        .filter(
            post_collection_item::fk_post_collection
                .eq(post_collection_pk)
                .and(s3_object::thumbnail_object_key.is_not_null()),
        )
        .order(post_collection_item::ordinal.asc())
        .first::<S3Object>(connection)
        .await
        .optional()
        .map_err(Error::from)
}
