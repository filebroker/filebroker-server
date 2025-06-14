use std::collections::{HashMap, HashSet};

use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::Utc;
use diesel::{
    BoolExpressionMethods, JoinOnDsl, NullableExpressionMethods, QueryDsl,
    dsl::{exists, max, not, sum},
};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use futures::{Stream, TryStreamExt};
use mime::Mime;
use mpart_async::server::MultipartStream;
use ring::digest;
use s3::{Bucket, Region, creds::Credentials};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;
use validator::Validate;
use warp::{
    Buf, Rejection, Reply,
    hyper::{self, Response},
    path::Peek,
};

use crate::{
    acquire_db_connection,
    diesel::{ExpressionMethods, OptionalExtension},
    error::Error,
    model::{Broker, NewBroker, S3Object, S3ObjectMetadata, User},
    perms::{
        self, PostJoinedS3Object, get_group_access_or_public_condition,
        get_group_membership_condition,
    },
    post,
    query::PostDetailed,
    schema::{
        broker, broker_access, s3_object, s3_object_metadata, user_group, user_group_membership,
    },
    util::NOT_BLANK_REGEX,
};

pub mod down;
pub mod encode;
pub mod s3utils;
pub mod up;

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
    let broker = perms::load_broker_secured(broker_pk, &mut connection, Some(&user)).await?;
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
        .ok_or_else(|| Error::InaccessibleS3ObjectError(String::from(object_key)))?;

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

#[derive(Deserialize, Validate)]
pub struct CreateBrokerRequest {
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub name: String,
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub bucket: String,
    #[validate(length(min = 1, max = 2048), regex(path = *NOT_BLANK_REGEX))]
    pub endpoint: String,
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub access_key: String,
    #[validate(length(min = 1, max = 255), regex(path = *NOT_BLANK_REGEX))]
    pub secret_key: String,
    pub is_aws_region: bool,
    pub remove_duplicate_files: bool,
}

pub async fn create_broker_handler(
    create_broker_request: CreateBrokerRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    create_broker_request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreateBrokerRequest: {}",
            e
        )))
    })?;

    let bucket = create_bucket(
        &create_broker_request.bucket,
        &create_broker_request.endpoint,
        &create_broker_request.access_key,
        &create_broker_request.secret_key,
        create_broker_request.is_aws_region,
    )?;

    if let Err(e) = Url::parse(&bucket.url()) {
        return Err(warp::reject::custom(Error::InvalidBucketError(
            e.to_string(),
        )));
    }

    // test connection
    let mut test_path = Uuid::new_v4().to_string();
    test_path.insert_str(0, ".filebroker-test-");
    if let Err(e) = bucket.put_object(&test_path, &[]).await {
        return Err(warp::reject::custom(Error::InvalidBucketError(
            e.to_string(),
        )));
    }
    if let Err(e) = bucket.delete_object(&test_path).await {
        return Err(warp::reject::custom(Error::InvalidBucketError(
            e.to_string(),
        )));
    }

    let mut connection = acquire_db_connection().await?;
    let created_broker = diesel::insert_into(broker::table)
        .values(&NewBroker {
            name: create_broker_request.name,
            bucket: create_broker_request.bucket,
            endpoint: create_broker_request.endpoint,
            access_key: create_broker_request.access_key,
            secret_key: create_broker_request.secret_key,
            is_aws_region: create_broker_request.is_aws_region,
            remove_duplicate_files: create_broker_request.remove_duplicate_files,
            fk_owner: user.pk,
            creation_timestamp: Utc::now(),
            hls_enabled: false,
        })
        .get_result::<Broker>(&mut connection)
        .await
        .map_err(Error::from)?;

    Ok(warp::reply::json(&created_broker))
}

#[derive(Serialize)]
pub struct BrokerAvailability {
    pub broker: Broker,
    pub used_bytes: i64,
    pub quota_bytes: Option<i64>,
}

pub async fn get_brokers_handler(user: User) -> Result<impl Reply, Rejection> {
    let mut connection = acquire_db_connection().await?;
    let brokers = perms::get_brokers_secured(&mut connection, Some(&user)).await?;
    let broker_pks = brokers.iter().map(|b| b.pk).collect::<Vec<i64>>();

    let broker_usages = s3_object::table
        .group_by(s3_object::fk_broker)
        .select((s3_object::fk_broker, sum(s3_object::size_bytes)))
        .filter(
            s3_object::fk_uploader
                .eq(user.pk)
                .and(s3_object::fk_broker.eq_any(&broker_pks)),
        )
        .load::<(i64, Option<BigDecimal>)>(&mut connection)
        .await
        .map_err(Error::from)?
        .into_iter()
        .map(|(broker_pk, size_used)| (broker_pk, size_used.unwrap_or(BigDecimal::from(0))))
        .collect::<HashMap<i64, BigDecimal>>();

    let unlimited_brokers = broker::table
        .select(broker::pk)
        .filter(
            broker::pk.eq_any(&broker_pks).and(
                broker::fk_owner.eq(user.pk).or(exists(
                    broker_access::table.filter(
                        get_group_access_or_public_condition!(
                            broker_access::fk_broker,
                            broker::pk,
                            &Some(user.pk),
                            broker_access::fk_granted_group.is_null(),
                            broker_access::fk_granted_group
                        )
                        .and(broker_access::quota.is_null()),
                    ),
                )),
            ),
        )
        .load::<i64>(&mut connection)
        .await
        .map_err(Error::from)?
        .into_iter()
        .collect::<HashSet<i64>>();

    let broker_quotas = broker::table
        .inner_join(
            broker_access::table.on(get_group_access_or_public_condition!(
                broker_access::fk_broker,
                broker::pk,
                &Some(user.pk),
                broker_access::fk_granted_group.is_null(),
                broker_access::fk_granted_group
            )
            .and(broker_access::quota.is_not_null())),
        )
        .group_by(broker::pk)
        .select((broker::pk, max(broker_access::quota)))
        .filter(
            broker::pk
                .eq_any(&broker_pks)
                .and(not(broker::pk.eq_any(&unlimited_brokers))),
        )
        .load::<(i64, Option<i64>)>(&mut connection)
        .await
        .map_err(Error::from)?
        .into_iter()
        .collect::<HashMap<i64, Option<i64>>>();

    let mut broker_availabilities = Vec::new();
    for broker in brokers {
        let used_bytes = broker_usages
            .get(&broker.pk)
            .map(|usage_bytes| {
                usage_bytes.to_i64().ok_or_else(|| {
                    Error::InternalError(format!(
                        "Could not convert broker usage bytes {} to i64",
                        usage_bytes
                    ))
                })
            })
            .unwrap_or(Ok(0))?;
        let quota_bytes = if unlimited_brokers.contains(&broker.pk) {
            None
        } else {
            broker_quotas.get(&broker.pk).cloned().flatten()
        };

        broker_availabilities.push(BrokerAvailability {
            broker,
            used_bytes,
            quota_bytes,
        });
    }

    Ok(warp::reply::json(&broker_availabilities))
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
        .ok_or_else(|| Error::InaccessibleS3ObjectError(String::from(object_key)))
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
