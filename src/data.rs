use std::{ffi::OsStr, path::Path, task::Poll};

use diesel::QueryDsl;
use futures::{ready, stream::IntoAsyncRead, Stream, TryStream, TryStreamExt};
use mime::Mime;
use mpart_async::server::MultipartStream;
use pin_project_lite::pin_project;
use ring::digest;
use s3::{creds::Credentials, Bucket, Region};
use warp::{Buf, Rejection, Reply};

use crate::{
    acquire_db_connection,
    diesel::{ExpressionMethods, OptionalExtension, RunQueryDsl},
    error::Error,
    model::{Broker, NewS3Object, S3Object, User},
    schema::{
        broker,
        s3_object::{self},
    },
    DbConnection,
};

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
    let broker = load_broker(broker_pk, &connection)?;
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

            let reader = FileReader {
                async_read,
                hasher: digest::Context::new(&digest::SHA256),
                file_size: 0,
            };

            let s3_object =
                upload_file(&broker, &user, &bucket, reader, object_key, content_type).await?;
            return Ok(warp::reply::json(&s3_object));
        }
    }

    Err(warp::reject::custom(Error::InvalidFileError(String::from(
        "No file specified, no multipart form field found for name 'file'",
    ))))
}

async fn upload_file<S>(
    broker: &Broker,
    user: &User,
    bucket: &Bucket,
    mut reader: FileReader<IntoAsyncRead<S>>,
    object_key: String,
    content_type: String,
) -> Result<S3Object, Error>
where
    S: TryStream<Error = std::io::Error> + Unpin,
    S::Ok: AsRef<[u8]>,
{
    let status = bucket
        .put_object_stream(&mut reader, &object_key)
        .await
        .map_err(|e| Error::S3Error(e.to_string()))?;
    if status >= 300 {
        return Err(Error::S3ResponseError(status));
    }

    let digest = reader.hasher.finish();
    let hash = data_encoding::HEXUPPER.encode(digest.as_ref());

    let mut connection = acquire_db_connection()?;

    if broker.remove_duplicate_files {
        let existing_object = s3_object::table
            .filter(s3_object::sha256_hash.eq(&hash))
            .order(s3_object::pk.desc())
            .limit(1)
            .get_result::<S3Object>(&connection)
            .optional()
            .map_err(|e| Error::QueryError(e.to_string()))?;

        if let Some(existing_object) = existing_object {
            // don't hold on to db connection while waiting for deletion
            drop(connection);
            match bucket.delete_object(&object_key).await {
                Ok(delete_response) => {
                    let status_code = delete_response.status_code();
                    if status_code < 300 {
                        // existing object found and deletion of new object succeeded -> return existing object
                        return Ok(existing_object);
                    } else {
                        log::error!(
                            "Deleting object {} for existing hash {} failed with status code {}. Going to use new object.",
                            &object_key,
                            &hash,
                            status_code
                        );
                    }
                }
                Err(e) => {
                    log::error!(
                        "Deleting object {} for existing hash {} failed with error {}. Going to use new object.",
                        &object_key,
                        &hash,
                        &e
                    );
                }
            }

            connection = acquire_db_connection()?;
        }
    }

    let s3_object = diesel::insert_into(s3_object::table)
        .values(&NewS3Object {
            object_key,
            sha256_hash: Some(hash),
            size_bytes: reader.file_size as i64,
            mime_type: content_type,
            fk_broker: broker.pk,
            fk_uploader: user.pk,
        })
        .get_result::<S3Object>(&connection)
        .map_err(|e| Error::QueryError(e.to_string()))?;

    Ok(s3_object)
}

pin_project! {
    pub struct FileReader<R> {
        #[pin]
        async_read: R,
        hasher: digest::Context,
        file_size: usize,
    }
}

impl<S> tokio::io::AsyncRead for FileReader<IntoAsyncRead<S>>
where
    S: TryStream<Error = std::io::Error> + Unpin,
    S::Ok: AsRef<[u8]>,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let slice = buf.initialize_unfilled();
        let this = self.project();
        let n = ready!(futures::io::AsyncRead::poll_read(
            this.async_read,
            cx,
            slice
        ))?;
        buf.advance(n);
        let filled = buf.filled();
        *this.file_size += n;
        this.hasher.update(filled);
        Poll::Ready(Ok(()))
    }
}

pub fn load_broker(broker_pk: i32, connection: &DbConnection) -> Result<Broker, Error> {
    broker::table
        .filter(broker::pk.eq(broker_pk))
        .first::<Broker>(connection)
        .optional()?
        .ok_or(Error::InaccessibleBrokerError(broker_pk))
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
        .map(|b| {
            if is_aws_region {
                b
            } else {
                b.with_path_style()
            }
        })
}
