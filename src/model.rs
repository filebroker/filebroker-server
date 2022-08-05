#![allow(clippy::extra_unused_lifetimes)]

use chrono::{offset::Utc, DateTime};
use diesel::{Associations, Identifiable, Insertable, Queryable};
use serde::Serialize;

use crate::schema::*;

#[derive(Associations, Identifiable, Queryable, Serialize)]
#[table_name = "registered_user"]
#[primary_key(pk)]
pub struct User {
    pub pk: i32,
    pub user_name: String,
    pub password: String,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Insertable)]
#[table_name = "registered_user"]
pub struct NewUser {
    pub user_name: String,
    pub password: String,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Identifiable, Queryable)]
#[belongs_to(User, foreign_key = "fk_registered_user")]
#[table_name = "refresh_token"]
#[primary_key(pk)]
pub struct RefreshToken {
    pub pk: i32,
    pub uuid: uuid::Uuid,
    pub expiry: DateTime<Utc>,
    pub invalidated: bool,
    pub fk_registered_user: i32,
}

#[derive(Insertable)]
#[table_name = "refresh_token"]
pub struct NewRefreshToken {
    pub uuid: uuid::Uuid,
    pub expiry: DateTime<Utc>,
    pub invalidated: bool,
    pub fk_registered_user: i32,
}

#[derive(Associations, Identifiable, Queryable, QueryableByName, Serialize)]
#[table_name = "post"]
#[primary_key(pk)]
#[belongs_to(User, foreign_key = "fk_create_user")]
pub struct Post {
    pub pk: i32,
    pub data_url: String,
    pub source_url: Option<String>,
    pub title: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
    pub fk_create_user: i32,
    pub score: i32,
    pub s3_object: Option<String>,
}

#[derive(Insertable)]
#[table_name = "post"]
pub struct NewPost {
    pub data_url: String,
    pub source_url: Option<String>,
    pub title: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
    pub fk_create_user: i32,
    pub score: i32,
    pub s3_object: Option<String>,
}

#[derive(Associations, Identifiable, Insertable, Queryable, Serialize)]
#[table_name = "post_tag"]
#[primary_key(fk_post, fk_tag)]
#[belongs_to(Post, foreign_key = "fk_post")]
#[belongs_to(Tag, foreign_key = "fk_tag")]
pub struct PostTag {
    pub fk_post: i32,
    pub fk_tag: i32,
}

#[derive(Associations, Clone, Identifiable, Queryable, Serialize)]
#[table_name = "tag"]
#[primary_key(pk)]
pub struct Tag {
    pub pk: i32,
    pub tag_name: String,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Insertable)]
#[table_name = "tag"]
pub struct NewTag {
    pub tag_name: String,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Identifiable, Insertable, Queryable, Serialize)]
#[table_name = "tag_alias"]
#[primary_key(fk_source, fk_target)]
#[belongs_to(Tag, foreign_key = "fk_source")]
pub struct TagAlias {
    pub fk_source: i32,
    pub fk_target: i32,
}

#[derive(Associations, Clone, Identifiable, Queryable, Serialize)]
#[table_name = "tag_closure_table"]
#[primary_key(pk)]
pub struct TagClosureTable {
    pub pk: i32,
    pub fk_parent: i32,
    pub fk_child: i32,
    pub depth: i32,
}

#[derive(Insertable)]
#[table_name = "tag_closure_table"]
pub struct NewTagClosureTable {
    pub fk_parent: i32,
    pub fk_child: i32,
    pub depth: i32,
}

#[derive(Associations, Identifiable, Queryable, Serialize)]
#[belongs_to(User, foreign_key = "fk_owner")]
#[table_name = "broker"]
#[primary_key(pk)]
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
}

#[derive(Insertable)]
#[table_name = "broker"]
pub struct NewBroker {
    pub name: String,
    pub bucket: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub is_aws_region: bool,
    pub remove_duplicate_files: bool,
    pub fk_owner: i32,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[belongs_to(Broker, foreign_key = "fk_broker")]
#[belongs_to(User, foreign_key = "fk_uploader")]
#[table_name = "s3_object"]
#[primary_key(object_key)]
pub struct S3Object {
    pub object_key: String,
    pub sha256_hash: Option<String>,
    pub size_bytes: i64,
    pub mime_type: String,
    pub fk_broker: i32,
    pub fk_uploader: i32,
}
