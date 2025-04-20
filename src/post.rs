use std::collections::HashMap;

use chrono::{DateTime, Utc};
use diesel::{
    BoolExpressionMethods, JoinOnDsl, OptionalExtension, Table,
    dsl::{exists, not},
};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};

use crate::{
    diesel::{ExpressionMethods, NullableExpressionMethods, QueryDsl},
    error::Error,
    model::{
        Post, PostCollection, PostCollectionGroupAccess, PostGroupAccess, S3Object,
        S3ObjectMetadata, Tag, User, UserGroup, UserPublic,
    },
    perms,
    query::{PostCollectionDetailed, PostDetailed},
    schema::{
        self, post, post_collection, post_collection_group_access, post_collection_item,
        post_collection_tag, post_group_access, post_tag, registered_user, s3_object,
        s3_object_metadata, tag, user_group, user_group_membership,
    },
};

pub mod create;
pub mod delete;
pub mod history;
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

pub async fn get_posts_tags(
    post_pks: &[i64],
    connection: &mut AsyncPgConnection,
) -> Result<HashMap<i64, Vec<Tag>>, diesel::result::Error> {
    let mut post_tags_map = HashMap::new();
    let post_tags = post_tag::table
        .inner_join(tag::table)
        .select((post_tag::fk_post, tag::table::all_columns()))
        .filter(post_tag::fk_post.eq_any(post_pks))
        .load::<(i64, Tag)>(connection)
        .await?;
    for (post_pk, tag) in post_tags {
        post_tags_map
            .entry(post_pk)
            .or_insert_with(Vec::new)
            .push(tag);
    }
    Ok(post_tags_map)
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

pub async fn get_posts_group_access(
    post_pks: &[i64],
    user: Option<&User>,
    connection: &mut AsyncPgConnection,
) -> Result<HashMap<i64, Vec<PostGroupAccessDetailed>>, diesel::result::Error> {
    if let Some(user) = user {
        post_group_access::table
            .inner_join(user_group::table)
            .filter(
                post_group_access::fk_post.eq_any(post_pks).and(
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
                    .eq_any(post_pks)
                    .and(not(user_group::hidden)),
            )
            .load::<(PostGroupAccess, UserGroup)>(connection)
            .await
    }
    .map(|post_group_access_vec| {
        let mut post_group_access_map = HashMap::new();
        post_group_access_vec
            .into_iter()
            .map(|post_group_access| PostGroupAccessDetailed {
                fk_post: post_group_access.0.fk_post,
                write: post_group_access.0.write,
                fk_granted_by: post_group_access.0.fk_granted_by,
                creation_timestamp: post_group_access.0.creation_timestamp,
                granted_group: post_group_access.1,
            })
            .for_each(|post_group_access| {
                post_group_access_map
                    .entry(post_group_access.fk_post)
                    .or_insert_with(Vec::new)
                    .push(post_group_access);
            });
        post_group_access_map
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

pub async fn load_post_detailed(
    post: Post,
    user: Option<&User>,
    connection: &mut AsyncPgConnection,
) -> Result<PostDetailed, Error> {
    let tags = get_post_tags(post.pk, connection)
        .await
        .map_err(Error::from)?;
    let group_access = get_post_group_access(post.pk, user, connection)
        .await
        .map_err(Error::from)?;
    let (create_user, edit_user) = diesel::alias!(
        schema::registered_user as create_user,
        schema::registered_user as edit_user,
    );
    let (create_user, edit_user) = post::table
        .inner_join(create_user.on(post::fk_create_user.eq(create_user.field(registered_user::pk))))
        .inner_join(edit_user.on(post::fk_edit_user.eq(edit_user.field(registered_user::pk))))
        .select((
            create_user.fields(registered_user::table::all_columns()),
            edit_user.fields(registered_user::table::all_columns()),
        ))
        .filter(post::pk.eq(post.pk))
        .get_result::<(UserPublic, UserPublic)>(connection)
        .await
        .map_err(Error::from)?;
    let is_editable = post.is_editable(user, connection).await?;
    let is_deletable = post.is_deletable(user, connection).await?;
    let (s3_object, s3_object_metadata) = s3_object::table
        .filter(s3_object::object_key.eq(&post.s3_object))
        .inner_join(s3_object_metadata::table)
        .get_result::<(S3Object, S3ObjectMetadata)>(connection)
        .await
        .map_err(Error::from)?;

    Ok(PostDetailed {
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
    })
}

pub async fn load_post_collection_detailed(
    post_collection: PostCollection,
    user: Option<&User>,
    connection: &mut AsyncPgConnection,
) -> Result<PostCollectionDetailed, Error> {
    let tags = get_post_collection_tags(post_collection.pk, connection)
        .await
        .map_err(Error::from)?;
    let group_access = get_post_collection_group_access(post_collection.pk, user, connection)
        .await
        .map_err(Error::from)?;
    let (create_user, edit_user) = diesel::alias!(
        schema::registered_user as create_user,
        schema::registered_user as edit_user,
    );
    let (create_user, edit_user) = post_collection::table
        .inner_join(
            create_user
                .on(post_collection::fk_create_user.eq(create_user.field(registered_user::pk))),
        )
        .inner_join(
            edit_user.on(post_collection::fk_edit_user.eq(edit_user.field(registered_user::pk))),
        )
        .select((
            create_user.fields(registered_user::table::all_columns()),
            edit_user.fields(registered_user::table::all_columns()),
        ))
        .filter(post_collection::pk.eq(post_collection.pk))
        .get_result::<(UserPublic, UserPublic)>(connection)
        .await
        .map_err(Error::from)?;
    let is_editable = post_collection.is_editable(user, connection).await?;
    let is_deletable = post_collection.is_deletable(user, connection).await?;
    let poster_object = if let Some(ref poster_object_key) = post_collection.poster_object_key {
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

    Ok(PostCollectionDetailed {
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
