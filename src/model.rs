#![allow(clippy::extra_unused_lifetimes)]

use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

use crate::error::Error;
use crate::query::{SearchQueryResultObject, SearchResult};
use crate::util::string_value_updated;
use crate::{perms, schema::*};
use chrono::{DateTime, offset::Utc};
use diesel::data_types::PgInterval;
use diesel::deserialize::{self, FromSql, FromSqlRow};
use diesel::expression::AsExpression;
use diesel::pg::{Pg, PgValue};
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::{
    BigInt, Bool, Float8, Int4, Int8, Integer, Interval, Jsonb, Nullable, Timestamptz, Varchar,
};
use diesel::{Associations, Identifiable, Insertable, Queryable, QueryableByName};
use diesel_async::AsyncPgConnection;
use serde::{Deserialize, Serialize, Serializer};
use validator::{Validate, ValidationError};

#[derive(Identifiable, Queryable, QueryableByName, Serialize, Clone)]
#[diesel(table_name = registered_user)]
#[diesel(primary_key(pk))]
pub struct User {
    pub pk: i64,
    pub user_name: String,
    #[serde(skip_serializing)]
    pub password: String,
    pub email: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
    pub email_confirmed: bool,
    pub display_name: Option<String>,
    #[serde(skip_serializing)]
    pub jwt_version: i32,
    pub password_fail_count: i32,
    pub is_admin: bool,
    pub is_banned: bool,
    pub avatar_object_key: Option<String>,
}

pub fn get_system_user() -> User {
    User {
        pk: 0,
        user_name: "system".to_string(),
        password: "".to_string(),
        email: None,
        creation_timestamp: Default::default(),
        email_confirmed: false,
        display_name: None,
        jwt_version: 0,
        password_fail_count: 0,
        is_admin: true,
        is_banned: false,
        avatar_object_key: None,
    }
}

#[inline]
pub fn is_system_user(user: &User) -> bool {
    user.pk == 0
}

/// A struct representing a user that serializes public information only.
#[derive(Identifiable, Queryable, QueryableByName, Serialize, Clone)]
#[diesel(table_name = registered_user)]
#[diesel(primary_key(pk))]
pub struct UserPublic {
    #[diesel(sql_type = BigInt)]
    pub pk: i64,
    #[diesel(sql_type = Varchar)]
    pub user_name: String,
    #[diesel(sql_type = Varchar)]
    #[serde(skip_serializing)]
    pub password: String,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[serde(skip_serializing)]
    pub email: Option<String>,

    #[diesel(sql_type = Timestamptz)]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = Bool)]
    #[serde(skip_serializing)]
    pub email_confirmed: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    pub display_name: Option<String>,
    #[diesel(sql_type = Int4)]
    #[serde(skip_serializing)]
    pub jwt_version: i32,
    #[diesel(sql_type = Int4)]
    #[serde(skip_serializing)]
    pub password_fail_count: i32,
    #[diesel(sql_type = Bool)]
    pub is_admin: bool,
    #[diesel(sql_type = Bool)]
    pub is_banned: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    pub avatar_object_key: Option<String>,
}

impl From<User> for UserPublic {
    fn from(value: User) -> Self {
        Self {
            pk: value.pk,
            user_name: value.user_name,
            password: value.password,
            email: value.email,
            creation_timestamp: value.creation_timestamp,
            email_confirmed: value.email_confirmed,
            display_name: value.display_name,
            jwt_version: value.jwt_version,
            password_fail_count: value.password_fail_count,
            is_admin: value.is_admin,
            is_banned: value.is_banned,
            avatar_object_key: value.avatar_object_key,
        }
    }
}

impl From<UserPublic> for User {
    fn from(value: UserPublic) -> Self {
        Self {
            pk: value.pk,
            user_name: value.user_name,
            password: value.password,
            email: value.email,
            creation_timestamp: value.creation_timestamp,
            email_confirmed: value.email_confirmed,
            display_name: value.display_name,
            jwt_version: value.jwt_version,
            password_fail_count: value.password_fail_count,
            is_admin: value.is_admin,
            is_banned: value.is_banned,
            avatar_object_key: value.avatar_object_key,
        }
    }
}

#[derive(Clone, Insertable)]
#[diesel(table_name = registered_user)]
pub struct NewUser {
    pub user_name: String,
    pub password: String,
    pub email: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
    pub email_confirmed: bool,
    pub display_name: Option<String>,
}

#[derive(Associations, Identifiable, Insertable, Queryable)]
#[diesel(belongs_to(User, foreign_key = fk_user))]
#[diesel(table_name = refresh_token)]
#[diesel(primary_key(uuid))]
pub struct RefreshToken {
    pub uuid: uuid::Uuid,
    pub expiry: DateTime<Utc>,
    pub invalidated: bool,
    pub fk_user: i64,
}

#[derive(Associations, Clone, Identifiable, Queryable, QueryableByName, Serialize)]
#[diesel(table_name = post)]
#[diesel(primary_key(pk))]
#[diesel(belongs_to(User, foreign_key = fk_create_user))]
pub struct Post {
    pub pk: i64,
    pub data_url: Option<String>,
    pub source_url: Option<String>,
    pub title: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
    pub fk_create_user: i64,
    pub score: i32,
    pub s3_object: String,
    pub thumbnail_url: Option<String>,
    #[serde(rename = "is_public")]
    pub public: bool,
    pub public_edit: bool,
    pub description: Option<String>,
    pub edit_timestamp: DateTime<Utc>,
    pub fk_edit_user: i64,
}

impl Post {
    pub async fn is_editable(
        &self,
        user: Option<&User>,
        connection: &mut AsyncPgConnection,
    ) -> Result<bool, Error> {
        if self.public_edit {
            return Ok(true);
        }
        if let Some(user) = user
            && (user.is_admin || user.pk == self.fk_create_user)
        {
            return Ok(true);
        }

        perms::is_post_editable(connection, user, self.pk).await
    }

    pub async fn is_deletable(
        &self,
        user: Option<&User>,
        connection: &mut AsyncPgConnection,
    ) -> Result<bool, Error> {
        if let Some(user) = user
            && (user.is_admin || user.pk == self.fk_create_user)
        {
            return Ok(true);
        }

        perms::is_post_deletable(connection, user, self.pk).await
    }
}

#[derive(Insertable)]
#[diesel(table_name = post)]
pub struct NewPost {
    pub data_url: Option<String>,
    pub source_url: Option<String>,
    pub title: Option<String>,
    pub fk_create_user: i64,
    pub s3_object: String,
    pub thumbnail_url: Option<String>,
    pub public: bool,
    pub public_edit: bool,
    pub description: Option<String>,
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = registered_user)]
pub struct PostCreateUser {
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_create_user_pk")]
    pub pk: i64,
    #[diesel(sql_type = Varchar)]
    #[diesel(column_name = "post_create_user_user_name")]
    pub user_name: String,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_create_user_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_create_user_display_name")]
    pub display_name: Option<String>,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_create_user_is_admin")]
    pub is_admin: bool,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_create_user_is_banned")]
    pub is_banned: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_create_user_avatar_object_key")]
    pub avatar_object_key: Option<String>,
}

impl From<User> for PostCreateUser {
    fn from(value: User) -> Self {
        Self {
            pk: value.pk,
            user_name: value.user_name,
            creation_timestamp: value.creation_timestamp,
            display_name: value.display_name,
            is_admin: value.is_admin,
            is_banned: value.is_banned,
            avatar_object_key: value.avatar_object_key,
        }
    }
}

impl From<UserPublic> for PostCreateUser {
    fn from(value: UserPublic) -> Self {
        PostCreateUser::from(User::from(value))
    }
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = s3_object)]
pub struct PostS3Object {
    #[diesel(sql_type = Varchar)]
    #[diesel(column_name = "post_s3_object_object_key")]
    pub object_key: String,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_sha256_hash")]
    pub sha256_hash: Option<String>,
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_s3_object_size_bytes")]
    pub size_bytes: i64,
    #[diesel(sql_type = Varchar)]
    #[diesel(column_name = "post_s3_object_mime_type")]
    pub mime_type: String,
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_s3_object_fk_broker")]
    pub fk_broker: i64,
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_s3_object_fk_uploader")]
    pub fk_uploader: i64,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_thumbnail_object_key")]
    pub thumbnail_object_key: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_s3_object_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_filename")]
    pub filename: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_hls_master_playlist")]
    pub hls_master_playlist: Option<String>,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_s3_object_hls_disabled")]
    pub hls_disabled: bool,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    #[diesel(column_name = "post_s3_object_hls_locked_at")]
    pub hls_locked_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    #[diesel(column_name = "post_s3_object_thumbnail_locked_at")]
    pub thumbnail_locked_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Int4>)]
    #[diesel(column_name = "post_s3_object_hls_fail_count")]
    pub hls_fail_count: Option<i32>,
    #[diesel(sql_type = Nullable<Int4>)]
    #[diesel(column_name = "post_s3_object_thumbnail_fail_count")]
    pub thumbnail_fail_count: Option<i32>,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_s3_object_thumbnail_disabled")]
    pub thumbnail_disabled: bool,
}

impl From<S3Object> for PostS3Object {
    fn from(value: S3Object) -> Self {
        Self {
            object_key: value.object_key,
            sha256_hash: value.sha256_hash,
            size_bytes: value.size_bytes,
            mime_type: value.mime_type,
            fk_broker: value.fk_broker,
            fk_uploader: value.fk_uploader,
            thumbnail_object_key: value.thumbnail_object_key,
            creation_timestamp: value.creation_timestamp,
            filename: value.filename,
            hls_master_playlist: value.hls_master_playlist,
            hls_disabled: value.hls_disabled,
            hls_locked_at: value.hls_locked_at,
            thumbnail_locked_at: value.thumbnail_locked_at,
            hls_fail_count: value.hls_fail_count,
            thumbnail_fail_count: value.thumbnail_fail_count,
            thumbnail_disabled: value.thumbnail_disabled,
        }
    }
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = s3_object_metadata)]
pub struct PostS3ObjectMetadata {
    #[diesel(sql_type = Varchar)]
    #[diesel(column_name = "post_s3_object_metadata_object_key")]
    pub object_key: String,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_file_type")]
    pub file_type: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_file_type_extension")]
    pub file_type_extension: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_mime_type")]
    pub mime_type: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_title")]
    pub title: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_artist")]
    pub artist: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_album")]
    pub album: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_album_artist")]
    pub album_artist: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_composer")]
    pub composer: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_genre")]
    pub genre: Option<String>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    #[diesel(column_name = "post_s3_object_metadata_date")]
    pub date: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Integer>)]
    #[diesel(column_name = "post_s3_object_metadata_track_number")]
    pub track_number: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    #[diesel(column_name = "post_s3_object_metadata_track_count")]
    pub track_count: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    #[diesel(column_name = "post_s3_object_metadata_disc_number")]
    pub disc_number: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    #[diesel(column_name = "post_s3_object_metadata_disc_count")]
    pub disc_count: Option<i32>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_duration")]
    pub duration: Option<String>,
    #[diesel(sql_type = Nullable<Integer>)]
    #[diesel(column_name = "post_s3_object_metadata_width")]
    pub width: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    #[diesel(column_name = "post_s3_object_metadata_height")]
    pub height: Option<i32>,
    #[diesel(sql_type = Nullable<BigInt>)]
    #[diesel(column_name = "post_s3_object_metadata_size")]
    pub size: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    #[diesel(column_name = "post_s3_object_metadata_bit_rate")]
    pub bit_rate: Option<i64>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_format_name")]
    pub format_name: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_format_long_name")]
    pub format_long_name: Option<String>,
    #[diesel(sql_type = Integer)]
    #[diesel(column_name = "post_s3_object_metadata_video_stream_count")]
    pub video_stream_count: i32,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_video_codec_name")]
    pub video_codec_name: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_video_codec_long_name")]
    pub video_codec_long_name: Option<String>,
    #[diesel(sql_type = Nullable<Float8>)]
    #[diesel(column_name = "post_s3_object_metadata_video_frame_rate")]
    pub video_frame_rate: Option<f64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    #[diesel(column_name = "post_s3_object_metadata_video_bit_rate_max")]
    pub video_bit_rate_max: Option<i64>,
    #[diesel(sql_type = Integer)]
    #[diesel(column_name = "post_s3_object_metadata_audio_stream_count")]
    pub audio_stream_count: i32,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_audio_codec_name")]
    pub audio_codec_name: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_s3_object_metadata_audio_codec_long_name")]
    pub audio_codec_long_name: Option<String>,
    #[diesel(sql_type = Nullable<Float8>)]
    #[diesel(column_name = "post_s3_object_metadata_audio_sample_rate")]
    pub audio_sample_rate: Option<f64>,
    #[diesel(sql_type = Nullable<Integer>)]
    #[diesel(column_name = "post_s3_object_metadata_audio_channels")]
    pub audio_channels: Option<i32>,
    #[diesel(sql_type = Nullable<BigInt>)]
    #[diesel(column_name = "post_s3_object_metadata_audio_bit_rate_max")]
    pub audio_bit_rate_max: Option<i64>,
    #[diesel(sql_type = Jsonb)]
    #[diesel(column_name = "post_s3_object_metadata_raw")]
    pub raw: serde_json::Value,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_s3_object_metadata_loaded")]
    pub loaded: bool,
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = post)]
pub struct PostFull {
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_pk")]
    pub pk: i64,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_data_url")]
    pub data_url: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_source_url")]
    pub source_url: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_title")]
    pub title: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(embed)]
    pub create_user: PostCreateUser,
    #[diesel(sql_type = Int4)]
    #[diesel(column_name = "post_score")]
    pub score: i32,
    #[diesel(embed)]
    pub s3_object: PostS3Object,
    #[diesel(embed)]
    pub s3_object_metadata: PostS3ObjectMetadata,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_thumbnail_url")]
    pub thumbnail_url: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_thumbnail_object_key")]
    pub thumbnail_object_key: Option<String>,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_public")]
    #[serde(rename = "is_public")]
    pub public: bool,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_public_edit")]
    pub public_edit: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_description")]
    pub description: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_edit_timestamp")]
    pub edit_timestamp: DateTime<Utc>,
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = post)]
pub struct PostQueryObject {
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_pk")]
    pub pk: i64,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_data_url")]
    pub data_url: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_source_url")]
    pub source_url: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_title")]
    pub title: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(embed)]
    pub create_user: PostCreateUser,
    #[diesel(sql_type = Int4)]
    #[diesel(column_name = "post_score")]
    pub score: i32,
    #[diesel(embed)]
    pub s3_object: PostS3Object,
    #[diesel(embed)]
    pub s3_object_metadata: PostS3ObjectMetadata,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_thumbnail_url")]
    pub thumbnail_url: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_thumbnail_object_key")]
    pub thumbnail_object_key: Option<String>,
    #[serde(skip_serializing)]
    #[diesel(sql_type = Nullable<Int8>)]
    pub full_count: Option<i64>,
    #[serde(skip_serializing)]
    #[diesel(sql_type = Int4)]
    pub evaluated_limit: i32,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_public")]
    #[serde(rename = "is_public")]
    pub public: bool,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_public_edit")]
    pub public_edit: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_description")]
    pub description: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_edit_timestamp")]
    pub edit_timestamp: DateTime<Utc>,
}

impl SearchQueryResultObject for PostQueryObject {
    fn construct_search_result(
        full_count: Option<i64>,
        pages: Option<i64>,
        objects: Vec<Self>,
    ) -> SearchResult
    where
        Self: Sized,
    {
        SearchResult {
            full_count,
            pages,
            posts: Some(objects),
            collections: None,
            collection_items: None,
            user_groups: None,
        }
    }

    fn get_full_count(&self) -> Option<i64> {
        self.full_count
    }

    fn get_evaluated_limit(&self) -> i32 {
        self.evaluated_limit
    }
}

#[allow(dead_code)]
#[derive(Queryable, QueryableByName)]
pub struct PostWindowQueryObject {
    #[diesel(sql_type = BigInt)]
    pub row_number: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub prev: Option<i64>,
    #[diesel(sql_type = BigInt)]
    pub pk: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub next: Option<i64>,
    #[diesel(sql_type = Int4)]
    pub evaluated_limit: i32,
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = registered_user)]
pub struct PostCollectionCreateUser {
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_collection_create_user_pk")]
    pub pk: i64,
    #[diesel(sql_type = Varchar)]
    #[diesel(column_name = "post_collection_create_user_user_name")]
    pub user_name: String,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_collection_create_user_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_create_user_display_name")]
    pub display_name: Option<String>,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_collection_create_user_is_admin")]
    pub is_admin: bool,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_collection_create_user_is_banned")]
    pub is_banned: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_create_user_avatar_object_key")]
    pub avatar_object_key: Option<String>,
}

impl From<User> for PostCollectionCreateUser {
    fn from(value: User) -> Self {
        Self {
            pk: value.pk,
            user_name: value.user_name,
            creation_timestamp: value.creation_timestamp,
            display_name: value.display_name,
            is_admin: value.is_admin,
            is_banned: value.is_banned,
            avatar_object_key: value.avatar_object_key,
        }
    }
}

impl From<UserPublic> for PostCollectionCreateUser {
    fn from(value: UserPublic) -> Self {
        PostCollectionCreateUser::from(User::from(value))
    }
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = s3_object)]
pub struct PostCollectionPosterS3Object {
    #[diesel(sql_type = Varchar)]
    #[diesel(column_name = "post_collection_poster_object_key")]
    pub object_key: String,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_poster_sha256_hash")]
    pub sha256_hash: Option<String>,
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_collection_poster_size_bytes")]
    pub size_bytes: i64,
    #[diesel(sql_type = Varchar)]
    #[diesel(column_name = "post_collection_poster_mime_type")]
    pub mime_type: String,
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_collection_poster_fk_broker")]
    pub fk_broker: i64,
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_collection_poster_fk_uploader")]
    pub fk_uploader: i64,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_poster_thumbnail_object_key")]
    pub thumbnail_object_key: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_collection_poster_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_poster_filename")]
    pub filename: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_poster_hls_master_playlist")]
    pub hls_master_playlist: Option<String>,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_collection_poster_hls_disabled")]
    pub hls_disabled: bool,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    #[diesel(column_name = "post_collection_poster_hls_locked_at")]
    pub hls_locked_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    #[diesel(column_name = "post_collection_poster_thumbnail_locked_at")]
    pub thumbnail_locked_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Int4>)]
    #[diesel(column_name = "post_collection_poster_hls_fail_count")]
    pub hls_fail_count: Option<i32>,
    #[diesel(sql_type = Nullable<Int4>)]
    #[diesel(column_name = "post_collection_poster_thumbnail_fail_count")]
    pub thumbnail_fail_count: Option<i32>,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_collection_poster_thumbnail_disabled")]
    pub thumbnail_disabled: bool,
}

impl From<S3Object> for PostCollectionPosterS3Object {
    fn from(value: S3Object) -> Self {
        Self {
            object_key: value.object_key,
            sha256_hash: value.sha256_hash,
            size_bytes: value.size_bytes,
            mime_type: value.mime_type,
            fk_broker: value.fk_broker,
            fk_uploader: value.fk_uploader,
            thumbnail_object_key: value.thumbnail_object_key,
            creation_timestamp: value.creation_timestamp,
            filename: value.filename,
            hls_master_playlist: value.hls_master_playlist,
            hls_disabled: value.hls_disabled,
            hls_locked_at: value.hls_locked_at,
            thumbnail_locked_at: value.thumbnail_locked_at,
            hls_fail_count: value.hls_fail_count,
            thumbnail_fail_count: value.thumbnail_fail_count,
            thumbnail_disabled: value.thumbnail_disabled,
        }
    }
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = post_collection)]
pub struct PostCollectionFull {
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_collection_pk")]
    pub pk: i64,
    #[diesel(sql_type = Varchar)]
    #[diesel(column_name = "post_collection_title")]
    pub title: String,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_collection_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(embed)]
    pub create_user: PostCollectionCreateUser,
    #[diesel(embed)]
    pub poster_object: Option<PostCollectionPosterS3Object>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_thumbnail_object_key")]
    pub thumbnail_object_key: Option<String>,
    #[diesel(sql_type = Bool)]
    #[serde(rename = "is_public")]
    #[diesel(column_name = "post_collection_public")]
    pub public: bool,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_collection_public_edit")]
    pub public_edit: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_description")]
    pub description: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_collection_edit_timestamp")]
    pub edit_timestamp: DateTime<Utc>,
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = post_collection)]
pub struct PostCollectionQueryObject {
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_collection_pk")]
    pub pk: i64,
    #[diesel(sql_type = Varchar)]
    #[diesel(column_name = "post_collection_title")]
    pub title: String,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_collection_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(embed)]
    pub create_user: PostCollectionCreateUser,
    #[diesel(embed)]
    pub poster_object: Option<PostCollectionPosterS3Object>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_thumbnail_object_key")]
    pub thumbnail_object_key: Option<String>,
    #[serde(skip_serializing)]
    #[diesel(sql_type = Nullable<Int8>)]
    pub full_count: Option<i64>,
    #[serde(skip_serializing)]
    #[diesel(sql_type = Int4)]
    pub evaluated_limit: i32,
    #[diesel(sql_type = Bool)]
    #[serde(rename = "is_public")]
    #[diesel(column_name = "post_collection_public")]
    pub public: bool,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_collection_public_edit")]
    pub public_edit: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_description")]
    pub description: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_collection_edit_timestamp")]
    pub edit_timestamp: DateTime<Utc>,
}

impl SearchQueryResultObject for PostCollectionQueryObject {
    fn construct_search_result(
        full_count: Option<i64>,
        pages: Option<i64>,
        objects: Vec<Self>,
    ) -> SearchResult
    where
        Self: Sized,
    {
        SearchResult {
            full_count,
            pages,
            posts: None,
            collections: Some(objects),
            collection_items: None,
            user_groups: None,
        }
    }

    fn get_full_count(&self) -> Option<i64> {
        self.full_count
    }

    fn get_evaluated_limit(&self) -> i32 {
        self.evaluated_limit
    }
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = registered_user)]
pub struct PostCollectionItemAddedUser {
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_collection_item_added_user_pk")]
    pub pk: i64,
    #[diesel(sql_type = Varchar)]
    #[diesel(column_name = "post_collection_item_added_user_user_name")]
    pub user_name: String,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_collection_item_added_user_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_item_added_user_display_name")]
    pub display_name: Option<String>,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_collection_item_added_user_is_admin")]
    pub is_admin: bool,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_collection_item_added_user_is_banned")]
    pub is_banned: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_item_added_user_avatar_object_key")]
    pub avatar_object_key: Option<String>,
}

impl From<User> for PostCollectionItemAddedUser {
    fn from(value: User) -> Self {
        Self {
            pk: value.pk,
            user_name: value.user_name,
            creation_timestamp: value.creation_timestamp,
            display_name: value.display_name,
            is_admin: value.is_admin,
            is_banned: value.is_banned,
            avatar_object_key: value.avatar_object_key,
        }
    }
}

impl From<UserPublic> for PostCollectionItemAddedUser {
    fn from(value: UserPublic) -> Self {
        PostCollectionItemAddedUser::from(User::from(value))
    }
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = post_collection_item)]
pub struct PostCollectionItemQueryObject {
    #[diesel(embed)]
    pub post: PostFull,
    #[diesel(embed)]
    pub post_collection: PostCollectionFull,
    #[diesel(embed)]
    pub added_by: PostCollectionItemAddedUser,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_collection_item_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "post_collection_item_pk")]
    pub pk: i64,
    #[diesel(sql_type = Int4)]
    #[diesel(column_name = "post_collection_item_ordinal")]
    pub ordinal: i32,
    #[serde(skip_serializing)]
    #[diesel(sql_type = Nullable<Int8>)]
    pub full_count: Option<i64>,
    #[serde(skip_serializing)]
    #[diesel(sql_type = Int4)]
    pub evaluated_limit: i32,
}

impl SearchQueryResultObject for PostCollectionItemQueryObject {
    fn construct_search_result(
        full_count: Option<i64>,
        pages: Option<i64>,
        objects: Vec<Self>,
    ) -> SearchResult
    where
        Self: Sized,
    {
        SearchResult {
            full_count,
            pages,
            posts: None,
            collections: None,
            collection_items: Some(objects),
            user_groups: None,
        }
    }

    fn get_full_count(&self) -> Option<i64> {
        self.full_count
    }

    fn get_evaluated_limit(&self) -> i32 {
        self.evaluated_limit
    }
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = user_group)]
pub struct UserGroupQueryObject {
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "user_group_pk")]
    pub pk: i64,
    #[diesel(sql_type = Varchar)]
    #[diesel(column_name = "user_group_name")]
    pub name: String,
    #[serde(rename = "is_public")]
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "user_group_public")]
    pub public: bool,
    #[diesel(embed)]
    pub owner: UserGroupOwnerUser,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "user_group_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "user_group_description")]
    pub description: Option<String>,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "user_group_allow_member_invite")]
    pub allow_member_invite: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "user_group_avatar_object_key")]
    pub avatar_object_key: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "user_group_edit_timestamp")]
    pub edit_timestamp: DateTime<Utc>,
    #[serde(skip_serializing)]
    #[diesel(sql_type = Nullable<Int8>)]
    pub full_count: Option<i64>,
    #[serde(skip_serializing)]
    #[diesel(sql_type = Int4)]
    pub evaluated_limit: i32,
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = registered_user)]
pub struct UserGroupOwnerUser {
    #[diesel(sql_type = BigInt)]
    #[diesel(column_name = "user_group_owner_user_pk")]
    pub pk: i64,
    #[diesel(sql_type = Varchar)]
    #[diesel(column_name = "user_group_owner_user_user_name")]
    pub user_name: String,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "user_group_owner_user_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "user_group_owner_user_display_name")]
    pub display_name: Option<String>,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "user_group_owner_user_is_admin")]
    pub is_admin: bool,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "user_group_owner_user_is_banned")]
    pub is_banned: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "user_group_owner_user_avatar_object_key")]
    pub avatar_object_key: Option<String>,
}

impl From<User> for UserGroupOwnerUser {
    fn from(value: User) -> Self {
        Self {
            pk: value.pk,
            user_name: value.user_name,
            creation_timestamp: value.creation_timestamp,
            display_name: value.display_name,
            is_admin: value.is_admin,
            is_banned: value.is_banned,
            avatar_object_key: value.avatar_object_key,
        }
    }
}

impl From<UserPublic> for UserGroupOwnerUser {
    fn from(value: UserPublic) -> Self {
        UserGroupOwnerUser::from(User::from(value))
    }
}

impl SearchQueryResultObject for UserGroupQueryObject {
    fn construct_search_result(
        full_count: Option<i64>,
        pages: Option<i64>,
        objects: Vec<Self>,
    ) -> SearchResult
    where
        Self: Sized,
    {
        SearchResult {
            full_count,
            pages,
            posts: None,
            collections: None,
            collection_items: None,
            user_groups: Some(objects),
        }
    }

    fn get_full_count(&self) -> Option<i64> {
        self.full_count
    }

    fn get_evaluated_limit(&self) -> i32 {
        self.evaluated_limit
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = post)]
pub struct PostUpdateOptional {
    pub data_url: Option<String>,
    pub source_url: Option<String>,
    pub title: Option<String>,
    pub public: Option<bool>,
    pub public_edit: Option<bool>,
    pub description: Option<String>,
    pub edit_timestamp: DateTime<Utc>,
    pub fk_edit_user: i64,
}

impl PostUpdateOptional {
    pub fn get_field_changes(&self, curr_value: &Post) -> PostUpdateFieldChanges {
        PostUpdateFieldChanges {
            data_url_changed: string_value_updated(
                curr_value.data_url.as_deref(),
                self.data_url.as_deref(),
            ),
            source_url_changed: string_value_updated(
                curr_value.source_url.as_deref(),
                self.source_url.as_deref(),
            ),
            title_changed: string_value_updated(curr_value.title.as_deref(), self.title.as_deref()),
            public_changed: self.public.map(|v| v != curr_value.public).unwrap_or(false),
            public_edit_changed: self
                .public_edit
                .map(|v| v != curr_value.public_edit)
                .unwrap_or(false),
            description_changed: string_value_updated(
                curr_value.description.as_deref(),
                self.description.as_deref(),
            ),
        }
    }

    pub fn has_changes(&self, curr_value: &Post) -> bool {
        self.get_field_changes(curr_value).has_changes()
    }
}

pub struct PostUpdateFieldChanges {
    pub data_url_changed: bool,
    pub source_url_changed: bool,
    pub title_changed: bool,
    pub public_changed: bool,
    pub public_edit_changed: bool,
    pub description_changed: bool,
}

impl PostUpdateFieldChanges {
    pub fn has_changes(&self) -> bool {
        self.data_url_changed
            || self.source_url_changed
            || self.title_changed
            || self.public_changed
            || self.public_edit_changed
            || self.description_changed
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = post_collection)]
pub struct PostCollectionUpdateOptional {
    pub title: Option<String>,
    pub public: Option<bool>,
    pub public_edit: Option<bool>,
    pub poster_object_key: Option<String>,
    pub description: Option<String>,
    pub edit_timestamp: DateTime<Utc>,
    pub fk_edit_user: i64,
}

impl PostCollectionUpdateOptional {
    pub fn get_field_changes(
        &self,
        curr_value: &PostCollection,
    ) -> PostCollectionUpdateFieldChanges {
        PostCollectionUpdateFieldChanges {
            title_changed: string_value_updated(Some(&curr_value.title), self.title.as_deref()),
            public_changed: self.public.map(|v| v != curr_value.public).unwrap_or(false),
            public_edit_changed: self
                .public_edit
                .map(|v| v != curr_value.public_edit)
                .unwrap_or(false),
            poster_object_key_changed: string_value_updated(
                curr_value.poster_object_key.as_deref(),
                self.poster_object_key.as_deref(),
            ),
            description_changed: string_value_updated(
                curr_value.description.as_deref(),
                self.description.as_deref(),
            ),
        }
    }

    pub fn has_changes(&self, curr_value: &PostCollection) -> bool {
        self.get_field_changes(curr_value).has_changes()
    }
}

pub struct PostCollectionUpdateFieldChanges {
    pub title_changed: bool,
    pub public_changed: bool,
    pub public_edit_changed: bool,
    pub poster_object_key_changed: bool,
    pub description_changed: bool,
}

impl PostCollectionUpdateFieldChanges {
    pub fn has_changes(&self) -> bool {
        self.title_changed
            || self.public_changed
            || self.public_edit_changed
            || self.poster_object_key_changed
            || self.description_changed
    }
}

#[derive(Associations, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(table_name = post_tag)]
#[diesel(primary_key(fk_post, fk_tag))]
#[diesel(belongs_to(Post, foreign_key = fk_post))]
#[diesel(belongs_to(Tag, foreign_key = fk_tag))]
pub struct PostTag {
    pub fk_post: i64,
    pub fk_tag: i64,
    pub auto_matched: bool,
}

#[derive(Associations, Clone, Identifiable, Queryable, Serialize)]
#[diesel(table_name = tag)]
#[diesel(primary_key(pk))]
#[diesel(belongs_to(TagCategory, foreign_key = tag_category))]
pub struct Tag {
    pub pk: i64,
    pub tag_name: String,
    pub creation_timestamp: DateTime<Utc>,
    pub fk_create_user: i64,
    pub edit_timestamp: DateTime<Utc>,
    pub fk_edit_user: i64,
    pub tag_category: Option<String>,
    pub auto_match_condition_post: Option<String>,
    pub auto_match_condition_collection: Option<String>,
    #[serde(skip_serializing)]
    pub compiled_auto_match_condition_post: Option<String>,
    #[serde(skip_serializing)]
    pub compiled_auto_match_condition_collection: Option<String>,
}

#[derive(Clone, Deserialize, Identifiable, Insertable, Queryable, Serialize, Validate)]
#[diesel(table_name = tag_category)]
#[diesel(primary_key(id))]
pub struct TagCategory {
    #[validate(length(min = 1, max = 255), custom(function = "validate_tag_category"))]
    pub id: String,
    #[validate(length(min = 1, max = 255))]
    pub label: String,
    #[validate(length(max = 1000))]
    pub auto_match_condition_post: Option<String>,
    #[validate(length(max = 1000))]
    pub auto_match_condition_collection: Option<String>,
}

fn validate_tag_category(value: &str) -> Result<(), ValidationError> {
    if !value
        .chars()
        .all(|c| c == '_' || c == '-' || c.is_ascii_alphanumeric())
    {
        return Err(ValidationError::new(
            "Category id must contain only alphanumeric characters, underscores, or dashes",
        ));
    }
    Ok(())
}

impl PartialEq for Tag {
    fn eq(&self, other: &Self) -> bool {
        self.pk == other.pk
    }
}

impl Eq for Tag {}

impl Hash for Tag {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.pk.hash(state);
    }
}

impl PartialOrd for Tag {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Tag {
    fn cmp(&self, other: &Self) -> Ordering {
        self.pk.cmp(&other.pk)
    }
}

#[derive(Insertable)]
#[diesel(table_name = tag)]
pub struct NewTag {
    pub tag_name: String,
    pub fk_create_user: i64,
    pub tag_category: Option<String>,
    pub auto_match_condition_post: Option<String>,
    pub auto_match_condition_collection: Option<String>,
}

#[derive(Associations, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(table_name = tag_alias)]
#[diesel(primary_key(fk_source, fk_target))]
#[diesel(belongs_to(Tag, foreign_key = fk_source))]
pub struct TagAlias {
    pub fk_source: i64,
    pub fk_target: i64,
}

#[derive(Clone, Identifiable, Queryable, Serialize)]
#[diesel(table_name = tag_closure_table)]
#[diesel(primary_key(pk))]
pub struct TagClosureTable {
    pub pk: i64,
    pub fk_parent: i64,
    pub fk_child: i64,
    pub depth: i32,
}

#[derive(Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(table_name = tag_edge)]
#[diesel(primary_key(fk_parent, fk_child))]
pub struct TagEdge {
    pub fk_parent: i64,
    pub fk_child: i64,
}

#[derive(Associations, Identifiable, Queryable, Serialize, Clone)]
#[diesel(belongs_to(User, foreign_key = fk_owner))]
#[diesel(table_name = broker)]
#[diesel(primary_key(pk))]
pub struct Broker {
    pub pk: i64,
    pub name: String,
    pub bucket: String,
    pub endpoint: String,
    #[serde(skip_serializing)]
    pub access_key: String,
    #[serde(skip_serializing)]
    pub secret_key: String,
    pub is_aws_region: bool,
    pub remove_duplicate_files: bool,
    pub fk_owner: i64,
    pub creation_timestamp: DateTime<Utc>,
    pub hls_enabled: bool,
    pub enable_presigned_get: bool,
    pub is_system_bucket: bool,
    pub description: Option<String>,
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
    pub fk_owner: i64,
    pub hls_enabled: bool,
    pub enable_presigned_get: bool,
    pub is_system_bucket: bool,
    pub description: Option<String>,
}

#[derive(Clone, diesel_derive_enum::DbEnum, Debug, Serialize)]
#[ExistingTypePath = "crate::schema::sql_types::BrokerAuditAction"]
pub enum BrokerAuditAction {
    Edit,
    BucketConnectionEdit,
    AccessGranted,
    AccessRevoked,
    AccessQuotaEdit,
    AccessAdminPromote,
    AccessAdminDemote,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(Broker, foreign_key = fk_broker))]
#[diesel(table_name = broker_audit_log)]
#[diesel(primary_key(pk))]
pub struct BrokerAuditLog {
    pub pk: i64,
    pub fk_broker: i64,
    pub fk_user: i64,
    pub action: BrokerAuditAction,
    pub fk_target_group: Option<i64>,
    pub new_quota: Option<i64>,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = broker_audit_log)]
pub struct NewBrokerAuditLog {
    pub fk_broker: i64,
    pub fk_user: i64,
    pub action: BrokerAuditAction,
    pub fk_target_group: Option<i64>,
    pub new_quota: Option<i64>,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, QueryableByName, Serialize)]
#[diesel(belongs_to(Broker, foreign_key = fk_broker))]
#[diesel(belongs_to(User, foreign_key = fk_uploader))]
#[diesel(table_name = s3_object)]
#[diesel(primary_key(object_key))]
pub struct S3Object {
    pub object_key: String,
    pub sha256_hash: Option<String>,
    pub size_bytes: i64,
    pub mime_type: String,
    pub fk_broker: i64,
    pub fk_uploader: i64,
    pub thumbnail_object_key: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
    pub filename: Option<String>,
    pub hls_master_playlist: Option<String>,
    pub hls_disabled: bool,
    pub hls_locked_at: Option<DateTime<Utc>>,
    pub thumbnail_locked_at: Option<DateTime<Utc>>,
    pub hls_fail_count: Option<i32>,
    pub thumbnail_fail_count: Option<i32>,
    pub thumbnail_disabled: bool,
    pub metadata_locked_at: Option<DateTime<Utc>>,
    pub metadata_fail_count: Option<i32>,
}

#[derive(Associations, Debug, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(S3Object, foreign_key = stream_playlist))]
#[diesel(table_name = hls_stream)]
#[diesel(primary_key(stream_file))]
pub struct HlsStream {
    pub stream_playlist: String,
    pub stream_file: String,
    pub master_playlist: String,
    pub resolution: i32,
    pub x264_preset: String,
    pub target_bitrate: Option<String>,
    pub min_bitrate: Option<String>,
    pub max_bitrate: Option<String>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(User, foreign_key = fk_owner))]
#[diesel(table_name = user_group)]
#[diesel(primary_key(pk))]
pub struct UserGroup {
    pub pk: i64,
    pub name: String,
    #[serde(rename = "is_public")]
    pub public: bool,
    pub fk_owner: i64,
    pub creation_timestamp: DateTime<Utc>,
    pub description: Option<String>,
    pub allow_member_invite: bool,
    pub avatar_object_key: Option<String>,
    pub fk_create_user: i64,
    pub edit_timestamp: DateTime<Utc>,
    pub fk_edit_user: i64,
}

#[derive(Insertable)]
#[diesel(table_name = user_group)]
pub struct NewUserGroup {
    pub name: String,
    pub public: bool,
    pub fk_owner: i64,
    pub creation_timestamp: DateTime<Utc>,
    pub description: Option<String>,
    pub allow_member_invite: bool,
    pub avatar_object_key: Option<String>,
    pub fk_create_user: i64,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(UserGroup, foreign_key = fk_group))]
#[diesel(belongs_to(User, foreign_key = fk_user))]
#[diesel(table_name = user_group_membership)]
#[diesel(primary_key(fk_user, fk_group))]
pub struct UserGroupMembership {
    pub fk_group: i64,
    pub fk_user: i64,
    pub administrator: bool,
    pub revoked: bool,
    pub fk_granted_by: i64,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(table_name = user_group_tag)]
#[diesel(primary_key(fk_user_group, fk_tag))]
#[diesel(belongs_to(UserGroup, foreign_key = fk_user_group))]
#[diesel(belongs_to(Tag, foreign_key = fk_tag))]
pub struct UserGroupTag {
    pub fk_user_group: i64,
    pub fk_tag: i64,
    pub auto_matched: bool,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(UserGroup, foreign_key = fk_user_group))]
#[diesel(table_name = user_group_invite)]
#[diesel(primary_key(code))]
pub struct UserGroupInvite {
    pub code: String,
    pub fk_user_group: i64,
    pub fk_create_user: i64,
    pub fk_invited_user: Option<i64>,
    pub creation_timestamp: DateTime<Utc>,
    pub expiration_timestamp: Option<DateTime<Utc>>,
    pub last_used_timestamp: Option<DateTime<Utc>>,
    pub max_uses: Option<i32>,
    pub uses_count: i32,
    pub revoked: bool,
}

#[derive(Clone, diesel_derive_enum::DbEnum, Debug, Serialize)]
#[ExistingTypePath = "crate::schema::sql_types::UserGroupAuditAction"]
pub enum UserGroupAuditAction {
    Edit,
    Join,
    Invite,
    RevokeInvite,
    Leave,
    Kick,
    Ban,
    Unban,
    AdminPromote,
    AdminDemote,
    AvatarChange,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(UserGroup, foreign_key = fk_user_group))]
#[diesel(table_name = user_group_audit_log)]
#[diesel(primary_key(pk))]
pub struct UserGroupAuditLog {
    pub pk: i64,
    pub fk_user_group: i64,
    pub fk_user: i64,
    pub action: UserGroupAuditAction,
    pub fk_target_user: Option<i64>,
    pub invite_code: Option<String>,
    pub reason: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = user_group_audit_log)]
pub struct NewUserGroupAuditLog {
    pub fk_user_group: i64,
    pub fk_user: i64,
    pub action: UserGroupAuditAction,
    pub fk_target_user: Option<i64>,
    pub invite_code: Option<String>,
    pub reason: Option<String>,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(User, foreign_key = fk_create_user))]
#[diesel(table_name = post_collection)]
#[diesel(primary_key(pk))]
pub struct PostCollection {
    pub pk: i64,
    pub title: String,
    pub fk_create_user: i64,
    pub creation_timestamp: DateTime<Utc>,
    #[serde(rename = "is_public")]
    pub public: bool,
    pub public_edit: bool,
    pub poster_object_key: Option<String>,
    pub description: Option<String>,
    pub edit_timestamp: DateTime<Utc>,
    pub fk_edit_user: i64,
}

impl PostCollection {
    pub async fn is_editable(
        &self,
        user: Option<&User>,
        connection: &mut AsyncPgConnection,
    ) -> Result<bool, Error> {
        if self.public_edit {
            return Ok(true);
        }
        if let Some(user) = user
            && (user.is_admin || user.pk == self.fk_create_user)
        {
            return Ok(true);
        }

        perms::is_post_collection_editable(connection, user, self.pk).await
    }

    pub async fn is_deletable(
        &self,
        user: Option<&User>,
        connection: &mut AsyncPgConnection,
    ) -> Result<bool, Error> {
        if let Some(user) = user
            && (user.is_admin || user.pk == self.fk_create_user)
        {
            return Ok(true);
        }

        perms::is_post_collection_deletable(connection, user, self.pk).await
    }
}

#[derive(Insertable)]
#[diesel(table_name = post_collection)]
pub struct NewPostCollection {
    pub title: String,
    pub fk_create_user: i64,
    pub public: bool,
    pub public_edit: bool,
    pub poster_object_key: Option<String>,
    pub description: Option<String>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(User, foreign_key = fk_added_by))]
#[diesel(belongs_to(Post, foreign_key = fk_post))]
#[diesel(belongs_to(PostCollection, foreign_key = fk_post_collection))]
#[diesel(table_name = post_collection_item)]
#[diesel(primary_key(pk))]
pub struct PostCollectionItem {
    pub fk_post: i64,
    pub fk_post_collection: i64,
    pub fk_added_by: i64,
    pub creation_timestamp: DateTime<Utc>,
    pub pk: i64,
    pub ordinal: i32,
}

#[derive(Insertable)]
#[diesel(table_name = post_collection_item)]
pub struct NewPostCollectionItem {
    pub fk_post: i64,
    pub fk_post_collection: i64,
    pub fk_added_by: i64,
    pub ordinal: i32,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(Post, foreign_key = fk_post))]
#[diesel(belongs_to(UserGroup, foreign_key = fk_granted_group))]
#[diesel(table_name = post_group_access)]
#[diesel(primary_key(fk_post, fk_granted_group))]
pub struct PostGroupAccess {
    pub fk_post: i64,
    pub fk_granted_group: i64,
    pub write: bool,
    pub fk_granted_by: i64,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(PostCollection, foreign_key = fk_post_collection))]
#[diesel(belongs_to(UserGroup, foreign_key = fk_granted_group))]
#[diesel(table_name = post_collection_group_access)]
#[diesel(primary_key(fk_post_collection, fk_granted_group))]
pub struct PostCollectionGroupAccess {
    pub fk_post_collection: i64,
    pub fk_granted_group: i64,
    pub write: bool,
    pub fk_granted_by: i64,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(table_name = post_collection_tag)]
#[diesel(primary_key(fk_post_collection, fk_tag))]
#[diesel(belongs_to(PostCollection, foreign_key = fk_post_collection))]
#[diesel(belongs_to(Tag, foreign_key = fk_tag))]
pub struct PostCollectionTag {
    pub fk_post_collection: i64,
    pub fk_tag: i64,
    pub auto_matched: bool,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(Broker, foreign_key = fk_broker))]
#[diesel(belongs_to(UserGroup, foreign_key = fk_granted_group))]
#[diesel(table_name = broker_access)]
#[diesel(primary_key(pk))]
pub struct BrokerAccess {
    pub pk: i64,
    pub fk_broker: i64,
    pub fk_granted_group: Option<i64>,
    pub write: bool,
    pub quota: Option<i64>,
    pub fk_granted_by: i64,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Clone, Insertable)]
#[diesel(table_name = broker_access)]
pub struct NewBrokerAccess {
    pub fk_broker: i64,
    pub fk_granted_group: Option<i64>,
    pub write: bool,
    pub quota: Option<i64>,
    pub fk_granted_by: i64,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(User, foreign_key = fk_user))]
#[diesel(table_name = email_confirmation_token)]
#[diesel(primary_key(uuid))]
pub struct EmailConfirmationToken {
    pub uuid: uuid::Uuid,
    pub expiry: DateTime<Utc>,
    pub invalidated: bool,
    pub fk_user: i64,
}

#[derive(Associations, Identifiable, Insertable, Queryable)]
#[diesel(belongs_to(User, foreign_key = fk_user))]
#[diesel(table_name = one_time_password)]
#[diesel(primary_key(fk_user))]
pub struct OneTimePassword {
    pub password: String,
    pub expiry: DateTime<Utc>,
    pub invalidated: bool,
    pub fk_user: i64,
}

#[derive(Associations, Identifiable, Insertable, Queryable, QueryableByName)]
#[diesel(belongs_to(Broker, foreign_key = fk_broker))]
#[diesel(table_name = deferred_s3_object_deletion)]
#[diesel(primary_key(object_key))]
pub struct DeferredS3ObjectDeletion {
    pub object_key: String,
    pub locked_at: Option<DateTime<Utc>>,
    pub fail_count: Option<i32>,
    pub fk_broker: i64,
}

#[derive(AsChangeset, Identifiable, Insertable, Queryable, QueryableByName, Serialize)]
#[diesel(table_name = s3_object_metadata)]
#[diesel(primary_key(object_key))]
pub struct S3ObjectMetadata {
    pub object_key: String,
    pub file_type: Option<String>,
    pub file_type_extension: Option<String>,
    pub mime_type: Option<String>,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub album_artist: Option<String>,
    pub composer: Option<String>,
    pub genre: Option<String>,
    pub date: Option<DateTime<Utc>>,
    pub track_number: Option<i32>,
    pub disc_number: Option<i32>,
    pub duration: Option<PgIntervalWrapper>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub size: Option<i64>,
    pub bit_rate: Option<i64>,
    pub format_name: Option<String>,
    pub format_long_name: Option<String>,
    pub video_stream_count: i32,
    pub video_codec_name: Option<String>,
    pub video_codec_long_name: Option<String>,
    pub video_frame_rate: Option<f64>,
    pub video_bit_rate_max: Option<i64>,
    pub audio_stream_count: i32,
    pub audio_codec_name: Option<String>,
    pub audio_codec_long_name: Option<String>,
    pub audio_sample_rate: Option<f64>,
    pub audio_channels: Option<i32>,
    pub audio_bit_rate_max: Option<i64>,
    pub raw: serde_json::Value,
    pub loaded: bool,
    pub track_count: Option<i32>,
    pub disc_count: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsExpression, FromSqlRow)]
#[diesel(sql_type = Interval)]
pub struct PgIntervalWrapper(pub PgInterval);

impl Serialize for PgIntervalWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        pg_interval::Interval::new(self.0.months, self.0.days, self.0.microseconds)
            .to_postgres()
            .serialize(serializer)
    }
}

impl ToSql<Interval, Pg> for PgIntervalWrapper {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        <PgInterval as ToSql<Interval, Pg>>::to_sql(&self.0, out)
    }
}

impl FromSql<Interval, Pg> for PgIntervalWrapper {
    fn from_sql(value: PgValue<'_>) -> deserialize::Result<Self> {
        Ok(PgIntervalWrapper(
            <PgInterval as FromSql<Interval, Pg>>::from_sql(value)?,
        ))
    }
}

#[derive(QueryableByName)]
pub struct PgIntervalQuery {
    #[diesel(sql_type = Interval)]
    #[diesel(column_name = "pg_interval")]
    pub interval: PgInterval,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(Post, foreign_key = fk_post))]
#[diesel(belongs_to(User, foreign_key = fk_edit_user))]
#[diesel(table_name = post_edit_history)]
#[diesel(primary_key(pk))]
pub struct PostEditHistory {
    pub pk: i64,
    pub fk_post: i64,
    pub fk_edit_user: i64,
    pub edit_timestamp: DateTime<Utc>,
    pub data_url: Option<String>,
    pub data_url_changed: bool,
    pub source_url: Option<String>,
    pub source_url_changed: bool,
    pub title: Option<String>,
    pub title_changed: bool,
    pub public: bool,
    pub public_changed: bool,
    pub public_edit: bool,
    pub public_edit_changed: bool,
    pub description: Option<String>,
    pub description_changed: bool,
    pub tags_changed: bool,
    pub group_access_changed: bool,
}

#[derive(Clone, Insertable)]
#[diesel(table_name = post_edit_history)]
pub struct NewPostEditHistory {
    pub fk_post: i64,
    pub fk_edit_user: i64,
    pub edit_timestamp: DateTime<Utc>,
    pub data_url: Option<String>,
    pub data_url_changed: bool,
    pub source_url: Option<String>,
    pub source_url_changed: bool,
    pub title: Option<String>,
    pub title_changed: bool,
    pub public: bool,
    pub public_changed: bool,
    pub public_edit: bool,
    pub public_edit_changed: bool,
    pub description: Option<String>,
    pub description_changed: bool,
    pub tags_changed: bool,
    pub group_access_changed: bool,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(PostEditHistory, foreign_key = fk_post_edit_history))]
#[diesel(belongs_to(Tag, foreign_key = fk_tag))]
#[diesel(table_name = post_edit_history_tag)]
#[diesel(primary_key(fk_post_edit_history, fk_tag))]
pub struct PostEditHistoryTag {
    pub fk_post_edit_history: i64,
    pub fk_tag: i64,
    pub auto_matched: bool,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(PostEditHistory, foreign_key = fk_post_edit_history))]
#[diesel(belongs_to(UserGroup, foreign_key = fk_granted_group))]
#[diesel(table_name = post_edit_history_group_access)]
#[diesel(primary_key(fk_post_edit_history, fk_granted_group))]
pub struct PostEditHistoryGroupAccess {
    pub fk_post_edit_history: i64,
    pub fk_granted_group: i64,
    pub write: bool,
    pub fk_granted_by: i64,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(PostCollection, foreign_key = fk_post_collection))]
#[diesel(belongs_to(User, foreign_key = fk_edit_user))]
#[diesel(table_name = post_collection_edit_history)]
#[diesel(primary_key(pk))]
pub struct PostCollectionEditHistory {
    pub pk: i64,
    pub fk_post_collection: i64,
    pub fk_edit_user: i64,
    pub edit_timestamp: DateTime<Utc>,
    pub title: String,
    pub title_changed: bool,
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
}

#[derive(Clone, Insertable)]
#[diesel(table_name = post_collection_edit_history)]
pub struct NewPostCollectionEditHistory {
    pub fk_post_collection: i64,
    pub fk_edit_user: i64,
    pub edit_timestamp: DateTime<Utc>,
    pub title: String,
    pub title_changed: bool,
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
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(PostCollectionEditHistory, foreign_key = fk_post_collection_edit_history))]
#[diesel(belongs_to(Tag, foreign_key = fk_tag))]
#[diesel(table_name = post_collection_edit_history_tag)]
#[diesel(primary_key(fk_post_collection_edit_history, fk_tag))]
pub struct PostCollectionEditHistoryTag {
    pub fk_post_collection_edit_history: i64,
    pub fk_tag: i64,
    pub auto_matched: bool,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(PostCollectionEditHistory, foreign_key = fk_post_collection_edit_history))]
#[diesel(belongs_to(UserGroup, foreign_key = fk_granted_group))]
#[diesel(table_name = post_collection_edit_history_group_access)]
#[diesel(primary_key(fk_post_collection_edit_history, fk_granted_group))]
pub struct PostCollectionEditHistoryGroupAccess {
    pub fk_post_collection_edit_history: i64,
    pub fk_granted_group: i64,
    pub write: bool,
    pub fk_granted_by: i64,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(Tag, foreign_key = fk_tag))]
#[diesel(belongs_to(User, foreign_key = fk_edit_user))]
#[diesel(table_name = tag_edit_history)]
#[diesel(primary_key(pk))]
pub struct TagEditHistory {
    pub pk: i64,
    pub fk_tag: i64,
    pub fk_edit_user: i64,
    pub edit_timestamp: DateTime<Utc>,
    pub tag_category: Option<String>,
    pub tag_category_changed: bool,
    pub parents_changed: bool,
    pub aliases_changed: bool,
}

#[derive(Clone, Insertable)]
#[diesel(table_name = tag_edit_history)]
pub struct NewTagEditHistory {
    pub fk_tag: i64,
    pub fk_edit_user: i64,
    pub edit_timestamp: DateTime<Utc>,
    pub tag_category: Option<String>,
    pub tag_category_changed: bool,
    pub parents_changed: bool,
    pub aliases_changed: bool,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(TagEditHistory, foreign_key = fk_tag_edit_history))]
#[diesel(belongs_to(Tag, foreign_key = fk_parent))]
#[diesel(table_name = tag_edit_history_parent)]
#[diesel(primary_key(fk_tag_edit_history, fk_parent))]
pub struct TagEditHistoryParent {
    pub fk_tag_edit_history: i64,
    pub fk_parent: i64,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(TagEditHistory, foreign_key = fk_tag_edit_history))]
#[diesel(belongs_to(Tag, foreign_key = fk_alias))]
#[diesel(table_name = tag_edit_history_alias)]
#[diesel(primary_key(fk_tag_edit_history, fk_alias))]
pub struct TagEditHistoryAlias {
    pub fk_tag_edit_history: i64,
    pub fk_alias: i64,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(UserGroup, foreign_key = fk_user_group))]
#[diesel(belongs_to(User, foreign_key = fk_edit_user))]
#[diesel(table_name = user_group_edit_history)]
#[diesel(primary_key(pk))]
pub struct UserGroupEditHistory {
    pub pk: i64,
    pub fk_user_group: i64,
    pub fk_edit_user: i64,
    pub edit_timestamp: DateTime<Utc>,
    pub name: String,
    pub name_changed: bool,
    #[serde(rename = "is_public")]
    pub public: bool,
    pub public_changed: bool,
    pub description: Option<String>,
    pub description_changed: bool,
    pub allow_member_invite: bool,
    pub allow_member_invite_changed: bool,
    pub tags_changed: bool,
}

#[derive(Clone, Insertable)]
#[diesel(table_name = user_group_edit_history)]
pub struct NewUserGroupEditHistory {
    pub fk_user_group: i64,
    pub fk_edit_user: i64,
    pub edit_timestamp: DateTime<Utc>,
    pub name: String,
    pub name_changed: bool,
    pub public: bool,
    pub public_changed: bool,
    pub description: Option<String>,
    pub description_changed: bool,
    pub allow_member_invite: bool,
    pub allow_member_invite_changed: bool,
    pub tags_changed: bool,
}

#[derive(Associations, Clone, Identifiable, Insertable, Queryable, Serialize)]
#[diesel(belongs_to(UserGroupEditHistory, foreign_key = fk_user_group_edit_history))]
#[diesel(belongs_to(Tag, foreign_key = fk_tag))]
#[diesel(table_name = user_group_edit_history_tag)]
#[diesel(primary_key(fk_user_group_edit_history, fk_tag))]
pub struct UserGroupEditHistoryTag {
    pub fk_user_group_edit_history: i64,
    pub fk_tag: i64,
    pub auto_matched: bool,
}

#[derive(
    Associations, Clone, Debug, Identifiable, Insertable, Queryable, QueryableByName, Serialize,
)]
#[diesel(belongs_to(Tag, foreign_key = tag_to_apply))]
#[diesel(belongs_to(TagCategory, foreign_key = tag_category_to_apply))]
#[diesel(belongs_to(Post, foreign_key = post_to_apply))]
#[diesel(belongs_to(PostCollection, foreign_key = post_collection_to_apply))]
#[diesel(table_name = apply_auto_tags_task)]
#[diesel(primary_key(pk))]
pub struct ApplyAutoTagsTask {
    pub pk: i64,
    pub creation_timestamp: DateTime<Utc>,
    pub tag_to_apply: Option<i64>,
    pub tag_category_to_apply: Option<String>,
    pub post_to_apply: Option<i64>,
    pub post_collection_to_apply: Option<i64>,
    pub locked_at: Option<DateTime<Utc>>,
    pub fail_count: i32,
}

#[derive(Clone, Debug, Insertable)]
#[diesel(table_name = apply_auto_tags_task)]
pub struct NewApplyAutoTagsTask {
    pub tag_to_apply: Option<i64>,
    pub tag_category_to_apply: Option<String>,
    pub post_to_apply: Option<i64>,
    pub post_collection_to_apply: Option<i64>,
}
