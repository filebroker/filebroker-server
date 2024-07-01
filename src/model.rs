#![allow(clippy::extra_unused_lifetimes)]

use std::hash::{Hash, Hasher};

use chrono::{offset::Utc, DateTime};
use diesel::data_types::PgInterval;
use diesel::deserialize::{self, FromSql, FromSqlRow};
use diesel::expression::AsExpression;
use diesel::pg::{Pg, PgValue};
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::{
    BigInt, Bool, Float8, Int4, Int8, Integer, Interval, Jsonb, Nullable, Timestamptz, Varchar,
};
use diesel::{Associations, Identifiable, Insertable, Queryable};
use diesel_async::AsyncPgConnection;
use serde::{Serialize, Serializer};

use crate::error::Error;
use crate::query::{SearchQueryResultObject, SearchResult};
use crate::{perms, schema::*};

#[derive(Identifiable, Queryable, QueryableByName, Serialize, Clone)]
#[diesel(table_name = registered_user)]
#[diesel(primary_key(pk))]
pub struct User {
    #[diesel(sql_type = BigInt)]
    pub pk: i64,
    #[diesel(sql_type = Varchar)]
    pub user_name: String,
    #[diesel(sql_type = Varchar)]
    #[serde(skip_serializing)]
    pub password: String,
    #[diesel(sql_type = Nullable<Varchar>)]
    pub email: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    pub avatar_url: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = Bool)]
    pub email_confirmed: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    pub display_name: Option<String>,
    #[diesel(sql_type = Int4)]
    #[serde(skip_serializing)]
    pub jwt_version: i32,
    #[diesel(sql_type = Int4)]
    pub password_fail_count: i32,
}

#[derive(Clone, Insertable)]
#[diesel(table_name = registered_user)]
pub struct NewUser {
    pub user_name: String,
    pub password: String,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
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

#[derive(Associations, Identifiable, Queryable, QueryableByName, Serialize)]
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
        if let Some(user) = user {
            if user.pk == self.fk_create_user {
                return Ok(true);
            }
        }

        perms::is_post_editable(connection, user, self.pk).await
    }

    pub async fn is_deletable(
        &self,
        user: Option<&User>,
        connection: &mut AsyncPgConnection,
    ) -> Result<bool, Error> {
        if let Some(user) = user {
            if user.pk == self.fk_create_user {
                return Ok(true);
            }
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
    pub s3_object: Option<String>,
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
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_create_user_email")]
    pub email: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_create_user_avatar_url")]
    pub avatar_url: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_create_user_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_create_user_email_confirmed")]
    pub email_confirmed: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_create_user_display_name")]
    pub display_name: Option<String>,
    #[diesel(sql_type = Int4)]
    #[diesel(column_name = "post_create_user_password_fail_count")]
    pub password_fail_count: i32,
}

impl From<User> for PostCreateUser {
    fn from(value: User) -> Self {
        Self {
            pk: value.pk,
            user_name: value.user_name,
            email: value.email,
            avatar_url: value.avatar_url,
            creation_timestamp: value.creation_timestamp,
            email_confirmed: value.email_confirmed,
            display_name: value.display_name,
            password_fail_count: value.password_fail_count,
        }
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
    pub s3_object: Option<PostS3Object>,
    #[diesel(embed)]
    pub s3_object_metadata: Option<PostS3ObjectMetadata>,
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
    pub s3_object: Option<PostS3Object>,
    #[diesel(embed)]
    pub s3_object_metadata: Option<PostS3ObjectMetadata>,
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
        }
    }

    fn get_full_count(&self) -> Option<i64> {
        self.full_count
    }

    fn get_evaluated_limit(&self) -> i32 {
        self.evaluated_limit
    }
}

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
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_create_user_email")]
    pub email: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_create_user_avatar_url")]
    pub avatar_url: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_collection_create_user_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_collection_create_user_email_confirmed")]
    pub email_confirmed: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_create_user_display_name")]
    pub display_name: Option<String>,
    #[diesel(sql_type = Int4)]
    #[diesel(column_name = "post_collection_create_user_password_fail_count")]
    pub password_fail_count: i32,
}

impl From<User> for PostCollectionCreateUser {
    fn from(value: User) -> Self {
        Self {
            pk: value.pk,
            user_name: value.user_name,
            email: value.email,
            avatar_url: value.avatar_url,
            creation_timestamp: value.creation_timestamp,
            email_confirmed: value.email_confirmed,
            display_name: value.display_name,
            password_fail_count: value.password_fail_count,
        }
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
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_item_added_user_email")]
    pub email: Option<String>,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_item_added_user_avatar_url")]
    pub avatar_url: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    #[diesel(column_name = "post_collection_item_added_user_creation_timestamp")]
    pub creation_timestamp: DateTime<Utc>,
    #[diesel(sql_type = Bool)]
    #[diesel(column_name = "post_collection_item_added_user_email_confirmed")]
    pub email_confirmed: bool,
    #[diesel(sql_type = Nullable<Varchar>)]
    #[diesel(column_name = "post_collection_item_added_user_display_name")]
    pub display_name: Option<String>,
    #[diesel(sql_type = Int4)]
    #[diesel(column_name = "post_collection_item_added_user_password_fail_count")]
    pub password_fail_count: i32,
}

impl From<User> for PostCollectionItemAddedUser {
    fn from(value: User) -> Self {
        Self {
            pk: value.pk,
            user_name: value.user_name,
            email: value.email,
            avatar_url: value.avatar_url,
            creation_timestamp: value.creation_timestamp,
            email_confirmed: value.email_confirmed,
            display_name: value.display_name,
            password_fail_count: value.password_fail_count,
        }
    }
}

#[derive(Queryable, QueryableByName, Serialize)]
#[diesel(table_name = post_collection_item)]
pub struct PostCollectionItemFull {
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
}

impl PostUpdateOptional {
    pub fn has_changes(&self) -> bool {
        self.data_url.is_some()
            || self.source_url.is_some()
            || self.title.is_some()
            || self.public.is_some()
            || self.public_edit.is_some()
            || self.description.is_some()
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
}

impl PostCollectionUpdateOptional {
    pub fn has_changes(&self) -> bool {
        self.title.is_some()
            || self.public.is_some()
            || self.public_edit.is_some()
            || self.poster_object_key.is_some()
            || self.description.is_some()
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
}

#[derive(Clone, Identifiable, Queryable, Serialize)]
#[diesel(table_name = tag)]
#[diesel(primary_key(pk))]
pub struct Tag {
    pub pk: i64,
    pub tag_name: String,
    pub creation_timestamp: DateTime<Utc>,
}

impl PartialEq for Tag {
    fn eq(&self, other: &Self) -> bool {
        self.pk == other.pk
    }
}

impl Hash for Tag {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.pk.hash(state);
    }
}

#[derive(Insertable)]
#[diesel(table_name = tag)]
pub struct NewTag {
    pub tag_name: String,
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
    pub access_key: String,
    pub secret_key: String,
    pub is_aws_region: bool,
    pub remove_duplicate_files: bool,
    pub fk_owner: i64,
    pub creation_timestamp: DateTime<Utc>,
    pub hls_enabled: bool,
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
    pub creation_timestamp: DateTime<Utc>,
    pub hls_enabled: bool,
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
    pub hidden: bool,
    pub fk_owner: i64,
    pub creation_timestamp: DateTime<Utc>,
}

#[derive(Insertable)]
#[diesel(table_name = user_group)]
pub struct NewUserGroup {
    pub name: String,
    pub public: bool,
    pub hidden: bool,
    pub fk_owner: i64,
    pub creation_timestamp: DateTime<Utc>,
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
        if let Some(user) = user {
            if user.pk == self.fk_create_user {
                return Ok(true);
            }
        }

        perms::is_post_collection_editable(connection, user, self.pk).await
    }

    pub async fn is_deletable(
        &self,
        user: Option<&User>,
        connection: &mut AsyncPgConnection,
    ) -> Result<bool, Error> {
        if let Some(user) = user {
            if user.pk == self.fk_create_user {
                return Ok(true);
            }
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
