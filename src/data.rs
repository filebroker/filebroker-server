use chrono::Utc;
use diesel::QueryDsl;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use futures::{Stream, TryStreamExt};
use lazy_static::lazy_static;
use mime::Mime;
use mpart_async::server::MultipartStream;
use ring::digest;
use s3::{Bucket, Region, creds::Credentials};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use validator::Validate;
use warp::{
    Buf, Rejection, Reply,
    hyper::{self, Response},
    path::Peek,
};

use crate::model::{NewUserGroupAuditLog, UserGroup, UserGroupAuditAction, get_system_user};
use crate::post::delete::delete_s3_objects;
use crate::schema::{registered_user, user_group_audit_log};
use crate::user_group::load_user_group_detailed;
use crate::{
    acquire_db_connection,
    diesel::{ExpressionMethods, OptionalExtension},
    error::Error,
    model::{Broker, S3Object, S3ObjectMetadata, User},
    perms::{self, PostJoinedS3Object},
    post,
    query::PostDetailed,
    run_serializable_transaction,
    schema::{broker, s3_object, s3_object_metadata, user_group},
    util::NOT_BLANK_REGEX,
};

pub mod down;
pub mod encode;
pub mod s3utils;
pub mod up;

lazy_static! {
    pub static ref PRESIGNED_GET_EXPIRATION_SECS: u32 =
        std::env::var("FILEBROKER_PRESIGNED_GET_EXPIRATION_SECS")
            .map(|v| v
                .parse::<u32>()
                .expect("FILEBROKER_PRESIGNED_GET_EXPIRATION_SECS is not a valid u32"))
            .unwrap_or(28800);// 8 hours
}

#[derive(Serialize)]
pub struct UploadResponse {
    pub s3_object: S3Object,
    pub posts: Vec<PostDetailed>,
}

pub async fn upload_handler(
    broker_pk: i64,
    user: User,
    mime: Mime,
    upload_size: usize,
    disable_hls_transcoding: Option<bool>,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin,
) -> Result<impl Reply, Rejection> {
    let boundary = mime
        .get_param("boundary")
        .map(|v| v.to_string())
        .ok_or_else(|| Error::InvalidFileError(String::from("No mime boundary")))?;

    let mut connection = acquire_db_connection().await?;
    let broker = perms::load_broker_access_secured(broker_pk, &mut connection, Some(&user)).await?;
    if broker.fk_owner != user.pk {
        up::check_broker_quota_usage(&broker, &user, upload_size, &mut connection).await?;
    }
    drop(connection);

    let bucket = create_bucket(
        &broker.bucket,
        &broker.endpoint,
        &broker.access_key,
        &broker.secret_key,
        broker.is_aws_region,
    )?;

    let mut stream = MultipartStream::new(
        boundary,
        body.map_ok(|mut buf| buf.copy_to_bytes(buf.remaining())),
    );

    while let Ok(Some(field)) = stream.try_next().await {
        if field
            .name()
            .map_err(|e| Error::InvalidFileError(e.to_string()))?
            == "file"
        {
            let filename = field
                .filename()
                .map_err(|e| Error::InvalidFileError(e.to_string()))?
                .to_string();

            let content_type = field
                .content_type()
                .map_err(|e| Error::InvalidFileError(e.to_string()))?;

            let parsed_content_type = content_type
                .parse::<Mime>()
                .unwrap_or(mime::APPLICATION_OCTET_STREAM);

            let content_type_string = if parsed_content_type == mime::APPLICATION_OCTET_STREAM {
                mime_guess::from_path(&filename)
                    .first_or(mime::APPLICATION_OCTET_STREAM)
                    .to_string()
            } else {
                parsed_content_type.to_string()
            };

            let async_read = field.map_err(std::io::Error::other).into_async_read();

            let reader = s3utils::FileReader {
                async_read,
                hasher: digest::Context::new(&digest::SHA256),
                file_size: 0,
                upload_size,
            };

            let (s3_object, is_existing) = up::upload_file(
                &broker,
                &user,
                &bucket,
                reader,
                content_type_string,
                filename,
                disable_hls_transcoding,
            )
            .await?;

            let mut posts_detailed = Vec::new();
            if is_existing {
                let mut connection = acquire_db_connection().await?;
                let posts = perms::load_s3_object_posts(
                    &s3_object.object_key,
                    Some(&user),
                    &mut connection,
                )
                .await?;

                for PostJoinedS3Object {
                    post,
                    create_user,
                    s3_object,
                    s3_object_metadata,
                    edit_user,
                } in posts
                {
                    let is_editable = post.is_editable(Some(&user), &mut connection).await?;
                    let is_deletable = post.is_deletable(Some(&user), &mut connection).await?;
                    let tags = post::get_post_tags(post.pk, &mut connection)
                        .await
                        .map_err(Error::from)?;
                    let group_access =
                        post::get_post_group_access(post.pk, Some(&user), &mut connection)
                            .await
                            .map_err(Error::from)?;

                    posts_detailed.push(PostDetailed {
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
                        s3_object_presigned_url: None,
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
                    });
                }
            }

            return Ok(warp::reply::json(&UploadResponse {
                s3_object,
                posts: posts_detailed,
            }));
        }
    }

    Err(warp::reject::custom(Error::InvalidFileError(String::from(
        "No file specified, no multipart form field found for name 'file'",
    ))))
}

pub async fn get_object_metadata_handler(requested_path: Peek) -> Result<impl Reply, Rejection> {
    let object_key = requested_path.as_str();
    let mut connection = acquire_db_connection().await?;
    let metadata = s3_object_metadata::table
        .filter(s3_object_metadata::object_key.eq(object_key))
        .get_result::<S3ObjectMetadata>(&mut connection)
        .await
        .optional()
        .map_err(Error::from)?
        .ok_or_else(|| Error::InaccessibleObjectKeyError(String::from(object_key)))?;

    Ok(warp::reply::json(&metadata))
}

pub async fn get_object_handler(
    requested_path: Peek,
    range: Option<String>,
) -> Result<impl Reply, Rejection> {
    let object_key = requested_path.as_str();
    let mut connection = acquire_db_connection().await?;
    let (object, broker) = load_object(object_key, &mut connection).await?;
    drop(connection);

    let bucket = create_bucket(
        &broker.bucket,
        &broker.endpoint,
        &broker.access_key,
        &broker.secret_key,
        broker.is_aws_region,
    )?;

    let (sender, body) = hyper::Body::channel();

    let down::GetObjectResponse {
        response_status,
        content_type,
        content_length,
        content_range,
    } = down::get_object_response(range, object, bucket, Some(sender))?;

    let mut response_builder = Response::builder()
        .header("Content-Type", &content_type)
        .header("Accept-Ranges", "bytes")
        .header("Cache-Control", "max-age=31536000, immutable")
        .header("Content-Length", content_length);

    if let Some(ref content_range) = content_range {
        response_builder = response_builder.header("Content-Range", content_range);
    }

    log::debug!(
        "Streaming object {} body with Content-Type: '{}'; Content-Length: '{}'; Content-Range: '{:?}'",
        &object_key,
        &content_type,
        content_length,
        &content_range
    );

    Ok(response_builder
        .status(response_status)
        .body(body)
        .map_err(|e| Error::SerialisationError(e.to_string()))?)
}

pub async fn get_object_head_handler(
    requested_path: Peek,
    range: Option<String>,
) -> Result<impl Reply, Rejection> {
    let object_key = requested_path.as_str();
    let mut connection = acquire_db_connection().await?;
    let (object, broker) = load_object(object_key, &mut connection).await?;
    drop(connection);

    let bucket = create_bucket(
        &broker.bucket,
        &broker.endpoint,
        &broker.access_key,
        &broker.secret_key,
        broker.is_aws_region,
    )?;

    let (_, response_code) = bucket
        .head_object(&object.object_key)
        .await
        .map_err(Error::from)?;

    if response_code >= 300 {
        return Err(warp::reject::custom(Error::S3ResponseError(response_code)));
    }

    let down::GetObjectResponse {
        response_status,
        content_type,
        content_length,
        content_range,
    } = down::get_object_response(range, object, bucket, None)?;

    let mut response_builder = Response::builder()
        .status(200)
        .header("Content-Type", &content_type)
        .header("Accept-Ranges", "bytes")
        .header("Cache-Control", "max-age=31536000, immutable")
        .header("Content-Length", content_length);

    if let Some(ref content_range) = content_range {
        response_builder = response_builder.header("Content-Range", content_range);
    }

    Ok(response_builder
        .status(response_status)
        .body(hyper::Body::empty())
        .map_err(|e| Error::SerialisationError(e.to_string()))?)
}

pub async fn get_presigned_hls_playlist_handler(
    requested_path: Peek,
) -> Result<impl Reply, Rejection> {
    let object_key = requested_path.as_str();

    if !object_key.ends_with(".m3u8") {
        return Err(warp::reject::custom(Error::InvalidRequestInputError(
            format!("Invalid path: {object_key}, expected m3u8 file"),
        )));
    }

    let now = std::time::Instant::now();
    let mut connection = acquire_db_connection().await?;
    let (object, broker) = load_object(object_key, &mut connection).await?;
    drop(connection);

    if *PRESIGNED_GET_EXPIRATION_SECS == 0 || !broker.enable_presigned_get {
        return Err(warp::reject::custom(Error::BadRequestError(String::from(
            "Presigned get is disabled",
        ))));
    }

    // reject files larger than 16MB as this is unrealistic for real HLS playlists and,
    // since the whole file is loaded into memory, could be used as an attack vector
    if object.size_bytes > 16 << 20 {
        return Err(warp::reject::custom(Error::InternalError(String::from(
            "Size of HLS playlist exceeds maximum of 16MB",
        ))));
    }

    let bucket = create_bucket(
        &broker.bucket,
        &broker.endpoint,
        &broker.access_key,
        &broker.secret_key,
        broker.is_aws_region,
    )?;

    let object_response = bucket.get_object(object_key).await.map_err(Error::from)?;
    if object_response.status_code() >= 300 {
        return Err(warp::reject::custom(Error::S3ResponseError(
            object_response.status_code(),
        )));
    }

    let mut response_builder = Response::builder()
        .header("Content-Type", "application/vnd.apple.mpegurl")
        .header("Accept-Ranges", "none")
        .header("Cache-Control", "no-store");

    if m3u8_rs::is_master_playlist(object_response.bytes()) {
        // nothing needs to be done for master playlists as it only contains the relative file names for the media playlists,
        // which will then be retrieved from the same path leading to this endpoint
        log::debug!(
            "Called get_presigned_hls_playlist_handler for master playlist {object_key}, returning master playlist as is (duration {}ms)",
            now.elapsed().as_millis()
        );
        response_builder = response_builder.header("Content-Length", object_response.bytes().len());
        Ok(response_builder
            .body(Vec::from(object_response.bytes()))
            .map_err(|e| Error::HyperError(e.to_string()))?)
    } else {
        match m3u8_rs::parse_media_playlist(object_response.bytes()) {
            Ok((_, mut pl)) => {
                let mut presigned_url_map = HashMap::<String, String>::new();
                for segment in pl.segments.iter_mut() {
                    if presigned_url_map.contains_key(&segment.uri) {
                        segment.uri = presigned_url_map[&segment.uri].clone();
                    } else {
                        let object_path = if let Some(last_slash) = object_key.rfind('/') {
                            let dir = &object_key[..=last_slash];
                            format!("{}{}", dir, segment.uri)
                        } else {
                            segment.uri.to_string()
                        };
                        let presigned_url = bucket
                            .presign_get(&object_path, *PRESIGNED_GET_EXPIRATION_SECS, None)
                            .map_err(Error::from)?;
                        log::debug!(
                            "Generated presigned URL for {object_key} segment {object_path}: {presigned_url}"
                        );
                        presigned_url_map.insert(segment.uri.clone(), presigned_url.clone());
                        segment.uri = presigned_url;
                    }
                }

                let mut buff = Vec::with_capacity(object_response.bytes().len());
                pl.write_to(&mut buff)
                    .map_err(|e| Error::IoError(e.to_string()))?;

                log::debug!(
                    "Called get_presigned_hls_playlist_handler for media playlist {object_key}, returning media playlist with presigned URLs (duration {}ms)",
                    now.elapsed().as_millis()
                );

                response_builder = response_builder.header("Content-Length", buff.len());
                Ok(response_builder
                    .body(buff)
                    .map_err(|e| Error::HyperError(e.to_string()))?)
            }
            Err(e) => Err(warp::reject::custom(Error::M3U8ParseError(e.to_string()))),
        }
    }
}

#[derive(Deserialize, Validate)]
pub struct CreateUserAvatarRequest {
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub source_object_key: String,
    pub width: u32,
    pub height: u32,
    pub x: u32,
    pub y: u32,
}

pub async fn create_user_avatar_handler(
    request: CreateUserAvatarRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    let user_pk = user.pk;
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreateUserAvatarRequest: {e}"
        )))
    })?;

    let avatar_object = encode::generate_avatar(
        request.source_object_key,
        user.pk,
        request.width,
        request.height,
        request.x,
        request.y,
    )
    .await?;

    let mut connection = acquire_db_connection().await?;
    run_serializable_transaction(&mut connection, |connection| {
        async {
            let user = registered_user::table
                .filter(registered_user::pk.eq(user_pk))
                .get_result::<User>(connection)
                .await?;

            let s3_object = diesel::insert_into(s3_object::table)
                .values(&avatar_object)
                .get_result::<S3Object>(connection)
                .await?;

            diesel::update(registered_user::table.filter(registered_user::pk.eq(user_pk)))
                .set(registered_user::avatar_object_key.eq(&s3_object.object_key))
                .execute(connection)
                .await?;

            if let Some(avatar_object_key) = user.avatar_object_key {
                delete_s3_objects(&[avatar_object_key], &get_system_user(), connection).await?;
            }

            Ok(())
        }
        .scope_boxed()
    })
    .await?;

    let updated_user = registered_user::table
        .filter(registered_user::pk.eq(user.pk))
        .get_result::<User>(&mut connection)
        .await
        .map_err(Error::from)?;

    Ok(warp::reply::json(&updated_user))
}

pub async fn create_user_group_avatar_handler(
    request: CreateUserAvatarRequest,
    user_group_pk: i64,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreateUserAvatarRequest: {e}"
        )))
    })?;

    let mut connection = acquire_db_connection().await?;
    if !perms::is_user_group_editable(user_group_pk, Some(&user), &mut connection).await? {
        return Err(warp::reject::custom(Error::InaccessibleObjectError(
            user_group_pk,
        )));
    }
    drop(connection);

    let avatar_object = encode::generate_avatar(
        request.source_object_key,
        user.pk,
        request.width,
        request.height,
        request.x,
        request.y,
    )
    .await?;

    let mut connection = acquire_db_connection().await?;
    let updated_user_group = run_serializable_transaction(&mut connection, |connection| {
        async {
            let user_group = user_group::table
                .filter(user_group::pk.eq(user_group_pk))
                .get_result::<UserGroup>(connection)
                .await?;

            let s3_object = diesel::insert_into(s3_object::table)
                .values(&avatar_object)
                .get_result::<S3Object>(connection)
                .await?;

            let updated_user_group =
                diesel::update(user_group::table.filter(user_group::pk.eq(user_group_pk)))
                    .set(user_group::avatar_object_key.eq(&s3_object.object_key))
                    .get_result::<UserGroup>(connection)
                    .await?;

            diesel::insert_into(user_group_audit_log::table)
                .values(NewUserGroupAuditLog {
                    fk_user_group: user_group.pk,
                    fk_user: user.pk,
                    action: UserGroupAuditAction::AvatarChange,
                    fk_target_user: None,
                    invite_code: None,
                    reason: None,
                    creation_timestamp: Utc::now(),
                })
                .execute(connection)
                .await?;

            if let Some(avatar_object_key) = user_group.avatar_object_key {
                delete_s3_objects(&[avatar_object_key], &get_system_user(), connection).await?;
            }

            Ok(updated_user_group)
        }
        .scope_boxed()
    })
    .await?;

    let user_group_detailed =
        load_user_group_detailed(updated_user_group, Some(&user), &mut connection).await?;

    Ok(warp::reply::json(&user_group_detailed))
}

pub async fn load_object(
    object_key: &str,
    connection: &mut AsyncPgConnection,
) -> Result<(S3Object, Broker), Error> {
    s3_object::table
        .inner_join(broker::table)
        .filter(s3_object::object_key.eq(object_key))
        .get_result::<(S3Object, Broker)>(connection)
        .await
        .optional()
        .map_err(Error::from)?
        .ok_or_else(|| Error::InaccessibleObjectKeyError(String::from(object_key)))
}

pub fn create_bucket(
    bucket_name: &str,
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    is_aws_region: bool,
) -> Result<Bucket, Error> {
    let region = if is_aws_region {
        let parsed_endpoint = endpoint
            .parse::<Region>()
            .map_err(|e| Error::InvalidBucketError(e.to_string()));

        match parsed_endpoint {
            // handle new AWS region eu-central-2 in Zurich manually until rust-s3 adds support for that region
            Ok(Region::Custom { .. }) if endpoint == "eu-central-2" => Ok(Region::Custom {
                region: String::from("eu-central-2"),
                endpoint: String::from("s3.eu-central-2.amazonaws.com"),
            }),
            res => res,
        }
    } else {
        Ok(Region::Custom {
            region: String::from(""),
            endpoint: String::from(endpoint),
        })
    }?;

    let credentials = Credentials {
        access_key: Some(String::from(access_key)),
        secret_key: Some(String::from(secret_key)),
        security_token: None,
        session_token: None,
        expiration: None,
    };

    Bucket::new(bucket_name, region, credentials)
        .map_err(|e| Error::InvalidBucketError(e.to_string()))
        .map(|mut b| {
            if is_aws_region {
                b.set_request_timeout(None);
                b
            } else {
                b.set_request_timeout(None);
                b.with_path_style()
            }
        })
}

pub async fn get_system_bucket(
    connection: &mut AsyncPgConnection,
) -> Result<Option<(Broker, Bucket)>, Error> {
    let system_broker = broker::table
        .filter(broker::is_system_bucket.eq(true))
        .get_result::<Broker>(connection)
        .await
        .optional()?;

    match system_broker {
        Some(broker) => {
            let bucket = create_bucket(
                &broker.bucket,
                &broker.endpoint,
                &broker.access_key,
                &broker.secret_key,
                broker.is_aws_region,
            )?;
            Ok(Some((broker, bucket)))
        }
        None => Ok(None),
    }
}
