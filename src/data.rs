use std::{ffi::OsStr, path::Path};

use diesel::QueryDsl;
use futures::{Stream, TryStreamExt};
use mime::Mime;
use mpart_async::server::MultipartStream;
use ring::digest;
use s3::{creds::Credentials, Bucket, Region};
use warp::{
    hyper::{self, Response},
    Buf, Rejection, Reply,
};

use crate::{
    acquire_db_connection,
    diesel::{ExpressionMethods, OptionalExtension, RunQueryDsl},
    error::Error,
    model::{Broker, S3Object, User},
    perms,
    schema::{broker, s3_object},
    DbConnection,
};

pub mod down;
pub mod s3utils;
pub mod up;

pub async fn upload_handler(
    broker_pk: i32,
    user: User,
    mime: Mime,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin,
) -> Result<impl Reply, Rejection> {
    let boundary = mime
        .get_param("boundary")
        .map(|v| v.to_string())
        .ok_or_else(|| Error::InvalidFileError(String::from("No mime boundary")))?;

    let connection = acquire_db_connection()?;
    let broker = perms::load_broker_secured(broker_pk, &connection, Some(&user))?;
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

            let uuid = uuid::Uuid::new_v4();
            let object_key =
                if let Some(extension) = Path::new(&filename).extension().and_then(OsStr::to_str) {
                    format!("{}.{}", &uuid, extension)
                } else {
                    uuid.to_string()
                };

            let content_type = field
                .content_type()
                .map_err(|e| Error::InvalidFileError(e.to_string()))?
                .to_string();

            let async_read = field
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                .into_async_read();

            let reader = s3utils::FileReader {
                async_read,
                hasher: digest::Context::new(&digest::SHA256),
                file_size: 0,
            };

            let s3_object = up::upload_file(
                &broker,
                &user,
                &bucket,
                reader,
                object_key.clone(),
                content_type,
                &uuid,
            )
            .await?;

            return Ok(warp::reply::json(&s3_object));
        }
    }

    Err(warp::reject::custom(Error::InvalidFileError(String::from(
        "No file specified, no multipart form field found for name 'file'",
    ))))
}

pub async fn get_object_handler(
    object_key: String,
    range: Option<String>,
) -> Result<impl Reply, Rejection> {
    let connection = acquire_db_connection()?;
    let (object, broker) = load_object(&object_key, &connection)?;
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
        .header("Cache-Control", "max-age=604800, immutable")
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
        .map_err(|_| Error::SerialisationError)?)
}

pub async fn get_object_head_handler(
    object_key: String,
    range: Option<String>,
) -> Result<impl Reply, Rejection> {
    let connection = acquire_db_connection()?;
    let (object, broker) = load_object(&object_key, &connection)?;
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
        .header("Cache-Control", "max-age=604800, immutable")
        .header("Content-Length", content_length);

    if let Some(ref content_range) = content_range {
        response_builder = response_builder.header("Content-Range", content_range);
    }

    Ok(response_builder
        .status(response_status)
        .body(hyper::Body::empty())
        .map_err(|_| Error::SerialisationError)?)
}

pub fn load_object(
    object_key: &str,
    connection: &DbConnection,
) -> Result<(S3Object, Broker), Error> {
    s3_object::table
        .inner_join(broker::table)
        .filter(s3_object::object_key.eq(object_key))
        .get_result::<(S3Object, Broker)>(connection)
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
        endpoint
            .parse::<Region>()
            .map_err(|e| Error::InvalidBucketError(e.to_string()))
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
