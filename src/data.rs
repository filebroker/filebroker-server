use std::{ffi::OsStr, path::Path, task::Poll};

use async_trait::async_trait;
use bytes::BytesMut;
use diesel::QueryDsl;
use futures::{ready, stream::IntoAsyncRead, Stream, TryStream, TryStreamExt};
use mime::Mime;
use mpart_async::server::MultipartStream;
use pin_project_lite::pin_project;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use ring::digest;
use s3::{creds::Credentials, Bucket, Region};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use warp::{
    hyper::{self, Response},
    Buf, Rejection, Reply,
};

use crate::{
    acquire_db_connection,
    diesel::{BoolExpressionMethods, ExpressionMethods, OptionalExtension, RunQueryDsl},
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
    let status = bucket.put_object_stream(&mut reader, &object_key).await?;
    if status >= 300 {
        return Err(Error::S3ResponseError(status));
    }

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

const MAX_RANGE: u64 = 1 << 24;
const BUFFER_SIZE: usize = 1 << 16;

#[async_trait]
trait ObjectWriter {
    async fn write_bytes(&self, writer: &mut DuplexStream);
}

struct FullObjectWriter {
    bucket: Bucket,
    object: S3Object,
}

#[async_trait]
impl ObjectWriter for FullObjectWriter {
    async fn write_bytes(&self, writer: &mut DuplexStream) {
        let res = self
            .bucket
            .get_object_stream(&self.object.object_key, writer)
            .await;

        match res {
            Ok(response_code) if response_code < 300 => {}
            Ok(response_code) => {
                log::error!(
                    "Non success response code {} reading object pk {}",
                    response_code,
                    self.object.pk
                );
            }
            Err(e) => {
                log::error!("Error occurred reading object pk {}: {}", self.object.pk, e);
            }
        }

        log::debug!("Writer exit for object {}", self.object.pk);
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
    async fn write_bytes(&self, writer: &mut DuplexStream) {
        for chunk in split_range(self.start, self.end) {
            let start = chunk.0;
            let end = chunk.1;

            match self
                .bucket
                .get_object_range(&self.object.object_key, start, Some(end))
                .await
                .map_err(Error::from)
            {
                Ok(response_data) => {
                    let status_code = response_data.status_code();
                    if status_code < 300 {
                        let bytes = response_data.bytes();
                        log::debug!(
                            "Writing {} bytes for range {}-{} for object {}",
                            bytes.len(),
                            start,
                            end,
                            self.object.pk
                        );
                        if let Err(e) = writer.write_all(bytes).await {
                            log::error!("Failed to write file bytes: {}", e);
                            return;
                        }
                    } else {
                        log::error!("Error code sending bytes to body: {}", status_code);
                    }
                }
                Err(e) => {
                    log::error!("Error reading object range: {}", e);
                    return;
                }
            }
        }

        log::debug!("Writer exit for object {}", self.object.pk);
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
    async fn write_bytes(&self, writer: &mut DuplexStream) {
        for part in self.parts.iter() {
            if let Err(e) = writer.write_all(part.fields.as_bytes()).await {
                log::error!("Error writing part fields: {}", e);
                return;
            }

            for chunk in split_range(part.start, part.end) {
                let start = chunk.0;
                let end = chunk.1;
                match self
                    .bucket
                    .get_object_range(&self.object.object_key, start, Some(end))
                    .await
                {
                    Ok(response_data) => {
                        let response_code = response_data.status_code();
                        if response_code < 300 {
                            let bytes = response_data.bytes();
                            log::debug!(
                                "Writing {} bytes for range {}-{} for object {}",
                                bytes.len(),
                                start,
                                end,
                                self.object.pk
                            );
                            if let Err(e) = writer.write_all(bytes).await {
                                log::error!("Failed to write file bytes: {}", e);
                                return;
                            }
                        } else {
                            log::error!(
                                "Non success status code sending file bytes {}",
                                response_code
                            );
                            return;
                        }
                    }
                    Err(e) => {
                        log::error!("Error reading object range: {}", e);
                        return;
                    }
                }
            }
        }

        if let Err(e) = writer.write_all(self.end_delimiter.as_bytes()).await {
            log::error!("Error writing end delimiter: {}", e);
        }

        log::debug!("Writer exit for object {}", self.object.pk);
    }
}

pub async fn get_object_handler(
    s3_object_key: i32,
    range: Option<String>,
) -> Result<impl Reply, Rejection> {
    let connection = acquire_db_connection()?;
    let (object, broker) = load_object(s3_object_key, &connection)?;
    drop(connection);

    let bucket = create_bucket(
        &broker.bucket,
        &broker.endpoint,
        &broker.access_key,
        &broker.secret_key,
        broker.is_aws_region,
    )?;

    let (mut sender, body) = hyper::Body::channel();
    let (mut writer, mut reader) = tokio::io::duplex(BUFFER_SIZE);

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

            let object_writer = ObjectRangeWriter {
                bucket,
                object,
                start,
                end,
            };

            content_length = end - start + 1;

            tokio::spawn(async move {
                object_writer.write_bytes(&mut writer).await;
            });
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

            let object_writer = MultipartObjectWriter {
                parts,
                bucket,
                object,
                end_delimiter,
            };

            tokio::spawn(async move {
                object_writer.write_bytes(&mut writer).await;
            });
        }
    } else {
        response_status = 200;
        content_type = object.mime_type.clone();
        content_length = object.size_bytes as u64;
        content_range = None;

        let object_writer = FullObjectWriter { bucket, object };

        tokio::spawn(async move {
            object_writer.write_bytes(&mut writer).await;
        });
    };

    tokio::spawn(async move {
        let mut buf = BytesMut::with_capacity(BUFFER_SIZE);
        loop {
            match reader.read_buf(&mut buf).await {
                Ok(n) if n > 0 => {
                    let bytes = buf.copy_to_bytes(n);
                    log::debug!("sending {} bytes to body", bytes.len());
                    if let Err(e) = sender.send_data(bytes).await {
                        log::error!("Error sending bytes to body: {}", e);
                        sender.abort();
                        break;
                    }
                }
                Ok(_) => break,
                Err(e) => {
                    log::error!("Error reading file to buf: {}", e);
                    sender.abort();
                    break;
                }
            }
        }
        log::debug!("Reader exit for object {}", s3_object_key);
    });

    let mut response_builder = Response::builder()
        .header("Content-Type", &content_type)
        .header("Accept-Ranges", "bytes")
        .header("Content-Length", content_length);

    if let Some(ref content_range) = content_range {
        response_builder = response_builder.header("Content-Range", content_range);
    }

    log::debug!(
        "Streaming body with Content-Type: '{}'; Content-Length: '{}'; Content-Range: '{:?}'",
        &content_type,
        content_length,
        &content_range
    );

    Ok(response_builder
        .status(response_status)
        .body(body)
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
    s3_object_key: i32,
    connection: &DbConnection,
) -> Result<(S3Object, Broker), Error> {
    s3_object::table
        .inner_join(broker::table)
        .filter(s3_object::pk.eq(s3_object_key))
        .get_result::<(S3Object, Broker)>(connection)
        .optional()
        .map_err(Error::from)?
        .ok_or(Error::InaccessibleObjectError(s3_object_key))
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

/// To avoid loading large ranges into memory, ranges are split into chunks.
fn split_range(start: u64, end: u64) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();
    let mut curr_start = start;

    loop {
        let curr_end = curr_start + MAX_RANGE;
        if curr_end > end {
            ranges.push((curr_start, end));
            break;
        }

        ranges.push((curr_start, curr_end));
        curr_start = curr_end + 1;
    }

    ranges
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
