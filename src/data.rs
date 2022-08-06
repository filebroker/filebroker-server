use std::{ffi::OsStr, path::Path, task::Poll};

use async_trait::async_trait;
use bytes::Bytes;
use diesel::QueryDsl;
use futures::io::BufReader;
use futures::AsyncReadExt;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::{ready, stream::IntoAsyncRead, Stream, TryStream, TryStreamExt};
use mime::Mime;
use mpart_async::server::MultipartStream;
use pin_project_lite::pin_project;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use ring::digest;
use s3::error::S3Error;
use s3::request_trait::Request;
use s3::{command::Command, creds::Credentials, request::Reqwest, Bucket, Region};
use uuid::Uuid;
use warp::{
    hyper::{self, Response},
    Buf, Rejection, Reply,
};

use crate::{
    acquire_db_connection,
    diesel::{BoolExpressionMethods, ExpressionMethods, OptionalExtension, RunQueryDsl},
    error::Error,
    model::{Broker, S3Object, User},
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

            let s3_object = upload_file(
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

async fn upload_file<S>(
    broker: &Broker,
    user: &User,
    bucket: &Bucket,
    mut reader: FileReader<IntoAsyncRead<S>>,
    object_key: String,
    content_type: String,
    file_id: &Uuid,
) -> Result<S3Object, Error>
where
    S: TryStream<Error = std::io::Error> + Unpin,
    S::Ok: AsRef<[u8]>,
{
    log::info!("Starting S3 upload for {}", &object_key);
    let status = bucket.put_object_stream(&mut reader, &object_key).await?;
    if status >= 300 {
        return Err(Error::S3ResponseError(status));
    }
    log::info!("Finished S3 upload for {}", &object_key);

    let digest = reader.hasher.finish();
    let hash = data_encoding::HEXUPPER.encode(digest.as_ref());

    let mut connection = acquire_db_connection()?;

    if broker.remove_duplicate_files {
        let existing_object = s3_object::table
            .filter(
                s3_object::sha256_hash
                    .eq(&hash)
                    .and(s3_object::fk_broker.eq(broker.pk)),
            )
            .limit(1)
            .get_result::<S3Object>(&connection)
            .optional()
            .map_err(|e| Error::QueryError(e.to_string()))?;

        if let Some(existing_object) = existing_object {
            // don't hold on to db connection while waiting for deletion
            drop(connection);
            log::info!("Found existing object {} with same hash as new object {}, going to delete new object", &existing_object.object_key, &object_key);
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

    let thumbnail =
        match generate_thumbnail(bucket, &object_key, file_id, &content_type, broker, user).await {
            Ok(thumbnail) => thumbnail,
            Err(e) => {
                log::error!("Failed to generate thumbnail: {}", e);
                None
            }
        };

    let s3_object = diesel::insert_into(s3_object::table)
        .values(&S3Object {
            object_key,
            sha256_hash: Some(hash),
            size_bytes: reader.file_size as i64,
            mime_type: content_type,
            fk_broker: broker.pk,
            fk_uploader: user.pk,
            thumbnail_object_key: thumbnail.map(|t| t.object_key),
        })
        .get_result::<S3Object>(&connection)
        .map_err(|e| Error::QueryError(e.to_string()))?;

    Ok(s3_object)
}

#[inline]
fn content_type_is_video(content_type: &str) -> bool {
    content_type.eq_ignore_ascii_case("video/mp4")
        || content_type.eq_ignore_ascii_case("video/webm")
        || content_type.eq_ignore_ascii_case("video/quicktime")
        || content_type.eq_ignore_ascii_case("video/x-msvideo")
        || content_type.eq_ignore_ascii_case("video/x-ms-wmv")
}

#[inline]
fn content_type_is_image(content_type: &str) -> bool {
    content_type.eq_ignore_ascii_case("image/png")
        || content_type.eq_ignore_ascii_case("image/jpeg")
        || content_type.eq_ignore_ascii_case("image/gif")
        || content_type.eq_ignore_ascii_case("image/webp")
        || content_type.eq_ignore_ascii_case("image/bmp")
}

async fn generate_thumbnail(
    bucket: &Bucket,
    path: &str,
    file_id: &Uuid,
    content_type: &str,
    broker: &Broker,
    user: &User,
) -> Result<Option<S3Object>, Error> {
    let presigned_get_object = bucket.presign_get(path, 1800, None)?;

    let content_type_is_video = content_type_is_video(content_type);
    let content_type_is_image = content_type_is_image(content_type);
    let thumbnail_extension;
    let thumbnail_content_type;

    if content_type_is_video || content_type_is_image {
        let args = if content_type_is_video {
            thumbnail_extension = "jpeg";
            thumbnail_content_type = String::from("image/jpeg");
            vec![
                String::from("-i"),
                presigned_get_object,
                String::from("-vf"),
                String::from(r"thumbnail,scale=iw*min(640/iw\,360/ih):ih*min(640/iw\,360/ih)"),
                String::from("-vframes"),
                String::from("1"),
                String::from("-f"),
                String::from("image2"),
                String::from("-v"),
                String::from("error"),
                String::from("pipe:1"),
            ]
        } else if content_type == "image/png" {
            thumbnail_extension = "png";
            thumbnail_content_type = String::from("image/png");
            vec![
                String::from("-i"),
                presigned_get_object,
                String::from("-pix_fmt"),
                String::from("rgba"),
                String::from("-vf"),
                String::from(
                    r"format=rgba,thumbnail,scale=iw*min(640/iw\,360/ih):ih*min(640/iw\,360/ih)",
                ),
                String::from("-f"),
                String::from("image2"),
                String::from("-codec"),
                String::from("png"),
                String::from("-v"),
                String::from("error"),
                String::from("pipe:1"),
            ]
        } else {
            thumbnail_extension = "jpeg";
            thumbnail_content_type = String::from("image/jpeg");
            vec![
                String::from("-i"),
                presigned_get_object,
                String::from("-vf"),
                String::from(r"thumbnail,scale=iw*min(640/iw\,360/ih):ih*min(640/iw\,360/ih)"),
                String::from("-f"),
                String::from("image2"),
                String::from("-v"),
                String::from("error"),
                String::from("pipe:1"),
            ]
        };

        log::info!("Spawning ffmpeg process to generate thumbnail for {}", path);
        let mut process = async_process::Command::new("ffmpeg")
            .args(args)
            .stdout(async_process::Stdio::piped())
            .spawn()
            .map_err(|e| Error::FfmpegProcessError(e.to_string()))?;

        let mut stdout = BufReader::new(process.stdout.take().ok_or_else(|| {
            Error::FfmpegProcessError(String::from("Could not get stdout of process"))
        })?);

        let mut buf: [u8; 1 << 14] = [0; 1 << 14];
        let mut thumb_bytes = Vec::new();
        loop {
            let n = stdout
                .read(&mut buf)
                .map_err(|e| Error::FfmpegProcessError(e.to_string()))
                .await?;
            if n == 0 {
                break;
            }

            thumb_bytes.extend_from_slice(&buf[0..n]);
            buf = [0; 1 << 14];
        }

        if thumb_bytes.is_empty() {
            log::warn!("Received 0 bytes for thumbnail {}", file_id);
            return Ok(None);
        }

        let thumb_path = format!("thumb_{}.{}", &file_id.to_string(), thumbnail_extension);
        log::info!("Storing thumbnail {} for object {}", &thumb_path, path);
        bucket.put_object(&thumb_path, &thumb_bytes).await?;

        let connection = acquire_db_connection()?;
        let s3_object = diesel::insert_into(s3_object::table)
            .values(&S3Object {
                object_key: thumb_path,
                sha256_hash: None,
                size_bytes: thumb_bytes.len() as i64,
                mime_type: thumbnail_content_type,
                fk_broker: broker.pk,
                fk_uploader: user.pk,
                thumbnail_object_key: None,
            })
            .get_result::<S3Object>(&connection)
            .map_err(|e| Error::QueryError(e.to_string()))?;

        Ok(Some(s3_object))
    } else {
        log::debug!(
            "Not creating thumbnail for unsupported content type '{}'",
            content_type
        );
        Ok(None)
    }
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

#[async_trait]
trait ObjectWriter {
    async fn write_bytes(&self, sender: hyper::body::Sender);
}

struct FullObjectWriter {
    bucket: Bucket,
    object: S3Object,
}

#[async_trait]
impl ObjectWriter for FullObjectWriter {
    async fn write_bytes(&self, mut sender: hyper::body::Sender) {
        let res = get_object_stream(&self.bucket, &self.object.object_key, &mut sender).await;

        match res {
            Ok(response_code) if response_code < 300 => {}
            Ok(response_code) => {
                sender.abort();
                log::error!(
                    "Non success response code {} reading object {}",
                    response_code,
                    &self.object.object_key
                );
            }
            Err(Error::HyperError(msg)) => {
                sender.abort();
                // sending probably failed due to broken pipe / connection disconnect, log as info
                log::debug!(
                    "Error occurred sending bytes to body for object {}: {}",
                    &self.object.object_key,
                    &msg
                );
            }
            Err(e) => {
                sender.abort();
                log::error!(
                    "Error occurred reading object {}: {}",
                    &self.object.object_key,
                    e
                );
            }
        }

        log::debug!("Writer exit for object {}", &self.object.object_key);
    }
}

struct ObjectRangeWriter {
    bucket: Bucket,
    object: S3Object,
    start: u64,
    end: u64,
}

#[async_trait]
impl ObjectWriter for ObjectRangeWriter {
    async fn write_bytes(&self, mut sender: hyper::body::Sender) {
        let start = self.start;
        let end = self.end;

        match get_object_range_stream(
            &self.bucket,
            &self.object.object_key,
            start,
            end,
            &mut sender,
        )
        .await
        .map_err(Error::from)
        {
            Ok(status_code) if status_code < 300 => {}
            Ok(status_code) => {
                sender.abort();
                log::error!(
                    "Non success response code {} reading object {}",
                    status_code,
                    &self.object.object_key
                );
            }
            Err(Error::HyperError(msg)) => {
                sender.abort();
                // sending probably failed due to broken pipe / connection disconnect, log as info
                log::debug!(
                    "Error occurred sending bytes to body for object {}: {}",
                    &self.object.object_key,
                    &msg
                );
            }
            Err(e) => {
                sender.abort();
                log::error!(
                    "Error occurred reading range {}-{} for object {}: {}",
                    start,
                    end,
                    &self.object.object_key,
                    e
                );
            }
        }

        log::debug!("Writer exit for object {}", &self.object.object_key);
    }
}

struct MultipartObjectWriter {
    parts: Vec<MultipartByteRange>,
    bucket: Bucket,
    object: S3Object,
    end_delimiter: String,
}

struct MultipartByteRange {
    start: u64,
    end: u64,
    fields: String,
}

impl MultipartByteRange {
    fn new(start: u64, end: u64, size: u64, content_type: &str, boundary: &str) -> Self {
        Self {
            start,
            end,
            fields: format!(
                "\r\n--{}\r\nContent-Type: {}\r\nContent-Range: bytes {}-{}/{}\r\n\r\n",
                boundary, content_type, start, end, size
            ),
        }
    }

    fn content_length(&self) -> u64 {
        self.end - self.start + 1 + (self.fields.len() as u64)
    }
}

#[async_trait]
impl ObjectWriter for MultipartObjectWriter {
    async fn write_bytes(&self, mut sender: hyper::body::Sender) {
        for part in self.parts.iter() {
            if let Err(e) = sender
                .send_data(Bytes::copy_from_slice(part.fields.as_bytes()))
                .await
            {
                log::debug!("Error writing part fields: {}", e);
                sender.abort();
                return;
            }

            let start = part.start;
            let end = part.end;
            match get_object_range_stream(
                &self.bucket,
                &self.object.object_key,
                start,
                end,
                &mut sender,
            )
            .await
            {
                Ok(status_code) if status_code < 300 => {}
                Ok(status_code) => {
                    sender.abort();
                    log::error!(
                        "Non success response code {} reading object {}",
                        status_code,
                        &self.object.object_key
                    );
                    return;
                }
                Err(Error::HyperError(msg)) => {
                    sender.abort();
                    // sending probably failed due to broken pipe / connection disconnect, log as info
                    log::debug!(
                        "Error occurred sending bytes to body for object {}: {}",
                        &self.object.object_key,
                        &msg
                    );
                    return;
                }
                Err(e) => {
                    sender.abort();
                    log::error!(
                        "Error occurred reading range {}-{} for object {}: {}",
                        start,
                        end,
                        &self.object.object_key,
                        e
                    );
                    return;
                }
            }
        }

        if let Err(e) = sender
            .send_data(Bytes::copy_from_slice(self.end_delimiter.as_bytes()))
            .await
        {
            log::debug!("Error writing end delimiter: {}", e);
            sender.abort();
            return;
        }

        log::debug!("Writer exit for object {}", &self.object.object_key);
    }
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

    let GetObjectResponse {
        response_status,
        content_type,
        content_length,
        content_range,
    } = get_object_response(range, object, bucket, Some(sender))?;

    let mut response_builder = Response::builder()
        .header("Content-Type", &content_type)
        .header("Accept-Ranges", "bytes")
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

    let GetObjectResponse {
        response_status,
        content_type,
        content_length,
        content_range,
    } = get_object_response(range, object, bucket, None)?;

    let mut response_builder = Response::builder()
        .header("Content-Type", &content_type)
        .header("Accept-Ranges", "bytes")
        .header("Content-Length", content_length);

    if let Some(ref content_range) = content_range {
        response_builder = response_builder.header("Content-Range", content_range);
    }

    Ok(response_builder
        .status(response_status)
        .body(hyper::Body::empty())
        .map_err(|_| Error::SerialisationError)?)
}

pub fn load_broker(broker_pk: i32, connection: &DbConnection) -> Result<Broker, Error> {
    broker::table
        .filter(broker::pk.eq(broker_pk))
        .first::<Broker>(connection)
        .optional()?
        .ok_or(Error::InaccessibleBrokerError(broker_pk))
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
        .ok_or_else(|| Error::InaccessibleObjectError(String::from(object_key)))
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

struct GetObjectResponse {
    response_status: u16,
    content_type: String,
    content_length: u64,
    content_range: Option<String>,
}

fn get_object_response(
    range: Option<String>,
    object: S3Object,
    bucket: Bucket,
    sender: Option<hyper::body::Sender>,
) -> Result<GetObjectResponse, Error> {
    let response_status;
    let content_type;
    let content_length;
    let content_range;

    if let Some(range) = range {
        let parsed_range = parse_range(&range, object.size_bytes as u64)?;
        // handle single range
        if parsed_range.len() == 1 {
            let parsed_range = parsed_range[0];
            let start = parsed_range.0;
            let end = parsed_range.1;

            response_status = 206;
            content_type = object.mime_type.clone();
            content_range = Some(format!("bytes {}-{}/{}", start, end, object.size_bytes));
            content_length = end - start + 1;

            if let Some(sender) = sender {
                let object_writer = ObjectRangeWriter {
                    bucket,
                    object,
                    start,
                    end,
                };

                tokio::spawn(async move {
                    object_writer.write_bytes(sender).await;
                });
            }
        } else {
            // handle multipart byteranges
            check_range_overlap(&parsed_range)?;
            let mut rng = thread_rng();
            let boundary: String = (&mut rng)
                .sample_iter(Alphanumeric)
                .take(60)
                .map(char::from)
                .collect();

            response_status = 206;
            content_type = format!("multipart/byteranges; boundary={}", &boundary);
            content_range = None;

            let parts = parsed_range
                .iter()
                .map(|range| {
                    MultipartByteRange::new(
                        range.0,
                        range.1,
                        object.size_bytes as u64,
                        &object.mime_type,
                        &boundary,
                    )
                })
                .collect::<Vec<_>>();

            let end_delimiter = format!("\r\n--{}--\r\n", &boundary);

            content_length = parts
                .iter()
                .map(MultipartByteRange::content_length)
                .sum::<u64>()
                + (end_delimiter.len() as u64);

            if let Some(sender) = sender {
                let object_writer = MultipartObjectWriter {
                    parts,
                    bucket,
                    object,
                    end_delimiter,
                };

                tokio::spawn(async move {
                    object_writer.write_bytes(sender).await;
                });
            }
        }
    } else {
        response_status = 200;
        content_type = object.mime_type.clone();
        content_length = object.size_bytes as u64;
        content_range = None;

        if let Some(sender) = sender {
            let object_writer = FullObjectWriter { bucket, object };

            tokio::spawn(async move {
                object_writer.write_bytes(sender).await;
            });
        }
    };

    Ok(GetObjectResponse {
        response_status,
        content_type,
        content_length,
        content_range,
    })
}

async fn get_object_range_stream(
    bucket: &Bucket,
    path: &str,
    start: u64,
    end: u64,
    sender: &mut hyper::body::Sender,
) -> Result<u16, Error> {
    let command = Command::GetObjectRange {
        start,
        end: Some(end),
    };
    get_command_stream(command, bucket, path, sender).await
}

async fn get_object_stream(
    bucket: &Bucket,
    path: &str,
    sender: &mut hyper::body::Sender,
) -> Result<u16, Error> {
    let command = Command::GetObject;
    get_command_stream(command, bucket, path, sender).await
}

async fn get_command_stream(
    command: s3::command::Command<'_>,
    bucket: &Bucket,
    path: &str,
    sender: &mut hyper::body::Sender,
) -> Result<u16, Error> {
    let request = Reqwest::new(bucket, path, command);
    let response = request.response().await?;
    let status = response.status();
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        sender.send_data(chunk.map_err(S3Error::from)?).await?;
    }

    Ok(status.as_u16())
}

fn parse_range(range: &str, size: u64) -> Result<Vec<(u64, u64)>, Error> {
    let byte_ranges = range
        .split("bytes=")
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    if byte_ranges.len() != 1 {
        return Err(Error::IllegalRangeError(format!(
            "Invalid range header '{}'",
            range
        )));
    }

    let byte_ranges = byte_ranges[0];
    byte_ranges
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|byte_range| {
            let parts = byte_range
                .split('-')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>();
            if !(parts.len() == 1 || parts.len() == 2) {
                return Err(Error::IllegalRangeError(format!(
                    "Invalid range '{}'",
                    byte_range
                )));
            }

            let start = parts[0]
                .parse::<u64>()
                .map_err(|e| Error::IllegalRangeError(e.to_string()))?;

            let end = if parts.len() == 2 {
                parts[1]
                    .parse::<u64>()
                    .map_err(|e| Error::IllegalRangeError(e.to_string()))?
            } else {
                size - 1
            };

            if start > size || end > size {
                return Err(Error::IllegalRangeError(format!(
                    "Range {} - {} invalid for size {}",
                    start, end, size
                )));
            } else if start > end {
                return Err(Error::IllegalRangeError(format!(
                    "Range {} - {} invalid",
                    start, end
                )));
            }

            Ok((start, end))
        })
        .collect::<Result<Vec<_>, Error>>()
}

fn check_range_overlap(ranges: &Vec<(u64, u64)>) -> Result<(), Error> {
    for i in 0..ranges.len() {
        for j in 0..ranges.len() {
            if i == j {
                continue;
            }

            let range_i = ranges[i];
            let range_j = ranges[j];

            if range_i.0 <= range_j.1 && range_i.1 >= range_j.0 {
                return Err(Error::IllegalRangeError(format!(
                    "Overlapping ranges {:?} and {:?}",
                    range_i, range_j
                )));
            }
        }
    }
    Ok(())
}
