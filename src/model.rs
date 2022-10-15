#![allow(clippy::extra_unused_lifetimes)]

use chrono::{offset::Utc, DateTime};
use diesel::sql_types::{Int4, Int8, Nullable, Timestamptz, Varchar};
use diesel::{Associations, Identifiable, Insertable, Queryable};
use serde::Serialize;

use crate::schema::*;

#[derive(Identifiable, Queryable, Serialize)]
#[diesel(table_name = registered_user)]
#[diesel(primary_key(pk))]
pub struct User {
    pub pk: i32,
    pub user_name: String,
    pub password: String,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = registered_user)]
pub struct NewUser {
    pub user_name: String,
    pub password: String,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Identifiable, Queryable)]
#[diesel(belongs_to(User, foreign_key = fk_registered_user))]
#[diesel(table_name = refresh_token)]
#[diesel(primary_key(pk))]
pub struct RefreshToken {
    pub pk: i32,
    pub uuid: uuid::Uuid,
    pub expiry: DateTime<Utc>,
    pub invalidated: bool,
    pub fk_registered_user: i32,
}

#[derive(Insertable)]
#[diesel(table_name = refresh_token)]
pub struct NewRefreshToken {
    pub uuid: uuid::Uuid,
    pub expiry: DateTime<Utc>,
    pub invalidated: bool,
    pub fk_registered_user: i32,
}

#[derive(Associations, Identifiable, Queryable, QueryableByName, Serialize)]
#[diesel(table_name = post)]
#[diesel(primary_key(pk))]
#[diesel(belongs_to(User, foreign_key = fk_create_user))]
pub struct Post {
    pub pk: i32,
    pub data_url: Option<String>,
    pub source_url: Option<String>,
    pub title: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
    pub fk_create_user: i32,
    pub score: i32,
    pub s3_object: Option<String>,
    pub thumbnail_url: Option<String>,
    pub public: bool,
}

#[derive(Insertable)]
#[diesel(table_name = post)]
pub struct NewPost {
    pub data_url: Option<String>,
    pub source_url: Option<String>,
    pub title: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
    pub fk_create_user: i32,
    pub score: i32,
    pub s3_object: Option<String>,
    pub thumbnail_url: Option<String>,
    pub public: bool,
}

#[derive(Queryable, QueryableByName, Serialize)]
pub struct PostQueryObject {
    #[diesel(sql_type = Int4)]
    pub pk: i32,
    #[diesel(sql_type = Nullable<Varchar>)]
    pub data_url: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    pub source_url: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    pub title: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = Int4)]
    pub fk_create_user: i32,
    #[diesel(sql_type = Int4)]
    pub score: i32,
    #[diesel(sql_type = Nullable<Varchar>)]
    pub s3_object: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    pub thumbnail_url: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    pub thumbnail_object_key: Option<String>,
    #[serde(skip_serializing)]
    #[diesel(sql_type = Nullable<Int8>)]
    pub full_count: Option<i64>,
    #[serde(skip_serializing)]
    #[diesel(sql_type = Int4)]
    pub evaluated_limit: i32,
}

#[derive(Queryable, QueryableByName)]
pub struct PostWindowQueryObject {
    #[diesel(sql_type = Nullable<Int4>)]
    pub prev: Option<i32>,
    #[diesel(sql_type = Int4)]
    pub pk: i32,
    #[diesel(sql_type = Nullable<Int4>)]
    pub next: Option<i32>,
}

#[derive(Associations, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(table_name = post_tag)]
#[diesel(primary_key(fk_post, fk_tag))]
#[diesel(belongs_to(Post, foreign_key = fk_post))]
#[diesel(belongs_to(Tag, foreign_key = fk_tag))]
pub struct PostTag {
    pub fk_post: i32,
    pub fk_tag: i32,
}

#[derive(Clone, Identifiable, Queryable, Serialize)]
#[diesel(table_name = tag)]
#[diesel(primary_key(pk))]
pub struct Tag {
    pub pk: i32,
    pub tag_name: String,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = tag)]
pub struct NewTag {
    pub tag_name: String,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(table_name = tag_alias)]
#[diesel(primary_key(fk_source, fk_target))]
#[diesel(belongs_to(Tag, foreign_key = fk_source))]
pub struct TagAlias {
    pub fk_source: i32,
    pub fk_target: i32,
}

#[derive(Clone, Identifiable, Queryable, Serialize)]
#[diesel(table_name = tag_closure_table)]
#[diesel(primary_key(pk))]
pub struct TagClosureTable {
    pub pk: i32,
    pub fk_parent: i32,
    pub fk_child: i32,
    pub depth: i32,
}

#[derive(Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(table_name = tag_edge)]
#[diesel(primary_key(fk_parent, fk_child))]
pub struct TagEdge {
    pub fk_parent: i32,
    pub fk_child: i32,
}

#[derive(Associations, Identifiable, Queryable, Serialize)]
#[diesel(belongs_to(User, foreign_key = fk_owner))]
#[diesel(table_name = broker)]
#[diesel(primary_key(pk))]
pub struct Broker {
    pub pk: i32,
    pub name: String,
    pub bucket: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub is_aws_region: bool,
    pub remove_duplicate_files: bool,
    pub fk_owner: i32,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = broker)]
pub struct NewBroker {
    pub name: String,
    pub bucket: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub is_aws_region: bool,
    pub remove_duplicate_files: bool,
    pub fk_owner: i32,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(Broker, foreign_key = fk_broker))]
#[diesel(belongs_to(User, foreign_key = fk_uploader))]
#[diesel(belongs_to(S3Object, foreign_key = thumbnail_object_key))]
#[diesel(table_name = s3_object)]
#[diesel(primary_key(object_key))]
pub struct S3Object {
    pub object_key: String,
    pub sha256_hash: Option<String>,
    pub size_bytes: i64,
    pub mime_type: String,
    pub fk_broker: i32,
    pub fk_uploader: i32,
    pub thumbnail_object_key: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(User, foreign_key = fk_owner))]
#[diesel(table_name = user_group)]
#[diesel(primary_key(pk))]
pub struct UserGroup {
    pub pk: i32,
    pub name: String,
    pub public: bool,
    pub hidden: bool,
    pub fk_owner: i32,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = user_group)]
pub struct NewUserGroup {
    pub name: String,
    pub public: bool,
    pub hidden: bool,
    pub fk_owner: i32,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(UserGroup, foreign_key = fk_group))]
#[diesel(belongs_to(User, foreign_key = fk_user))]
#[diesel(table_name = user_group_membership)]
#[diesel(primary_key(fk_group, fk_user))]
pub struct UserGroupMembership {
    pub fk_group: i32,
    pub fk_user: i32,
    pub administrator: bool,
    pub revoked: bool,
    pub fk_granted_by: i32,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(User, foreign_key = fk_owner))]
#[diesel(table_name = post_collection)]
#[diesel(primary_key(pk))]
pub struct PostCollection {
    pub pk: i32,
    pub name: String,
    pub fk_owner: i32,
    pub creation_timestamp: DateTime<Utc>,
    pub public: bool,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(User, foreign_key = fk_added_by))]
#[diesel(table_name = post_collection_item)]
#[diesel(primary_key(fk_post, fk_post_collection))]
pub struct PostCollectionItem {
    pub fk_post: i32,
    pub fk_post_collection: i32,
    pub fk_added_by: i32,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(Post, foreign_key = fk_post))]
#[diesel(belongs_to(UserGroup, foreign_key = fk_granted_group))]
#[diesel(table_name = post_group_access)]
#[diesel(primary_key(fk_post, fk_granted_group))]
pub struct PostGroupAccess {
    pub fk_post: i32,
    pub fk_granted_group: i32,
    pub write: bool,
    pub fk_granted_by: i32,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(PostCollection, foreign_key = fk_post_collection))]
#[diesel(belongs_to(UserGroup, foreign_key = fk_granted_group))]
#[diesel(table_name = post_collection_group_access)]
#[diesel(primary_key(fk_post_collection, fk_granted_group))]
pub struct PostCollectionGroupAccess {
    pub fk_post_collection: i32,
    pub fk_granted_group: i32,
    pub write: bool,
    pub fk_granted_by: i32,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(Broker, foreign_key = fk_broker))]
#[diesel(belongs_to(UserGroup, foreign_key = fk_granted_group))]
#[diesel(table_name = broker_access)]
#[diesel(primary_key(pk))]
pub struct BrokerAccess {
    pub pk: i32,
    pub fk_broker: i32,
    pub fk_granted_group: Option<i32>,
    pub write: bool,
    pub public: bool,
    pub quota: Option<i64>,
    pub fk_granted_by: i32,
    pub creation_timestamp: DateTime<Utc>,
}
