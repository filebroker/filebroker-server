use std::{task::Poll, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{ready, stream::IntoAsyncRead, StreamExt, TryStream};
use pin_project_lite::pin_project;
use ring::digest;
use s3::{command::Command, error::S3Error, request::Reqwest, request_trait::Request, Bucket};
use tokio::time::timeout;
use warp::hyper;

use crate::{error::Error, model::S3Object, util::format_duration};

pin_project! {
    pub struct FileReader<R> {
        #[pin]
        pub(crate) async_read: R,
        pub(crate) hasher: digest::Context,
        pub(crate) file_size: usize,
        pub(crate) upload_size: usize,
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
    ) -> Poll<std::io::Result<()>> {
        let slice = buf.initialize_unfilled();
        let this = self.project();
        let n = ready!(futures::io::AsyncRead::poll_read(
            this.async_read,
            cx,
            slice
        ))?;
        buf.advance(n);
        let filled = buf.filled();
        let len = filled.len();
        *this.file_size += n;
        if this.file_size > this.upload_size {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Read file size reached {}, exceeding provided upload size {}",
                    this.file_size, this.upload_size
                ),
            )));
        }
        this.hasher.update(&filled[len - n..len]);
        Poll::Ready(Ok(()))
    }
}

#[async_trait]
pub trait ObjectWriter {
    async fn write_bytes(&self, sender: hyper::body::Sender);
}

pub struct FullObjectWriter {
    pub(crate) bucket: Bucket,
    pub(crate) object: S3Object,
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

pub struct ObjectRangeWriter {
    pub(crate) bucket: Bucket,
    pub(crate) object: S3Object,
    pub(crate) start: u64,
    pub(crate) end: u64,
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
            Some(end),
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

pub struct MultipartObjectWriter {
    pub(crate) parts: Vec<MultipartByteRange>,
    pub(crate) bucket: Bucket,
    pub(crate) object: S3Object,
    pub(crate) end_delimiter: String,
}

pub struct MultipartByteRange {
    start: u64,
    end: u64,
    fields: String,
}

impl MultipartByteRange {
    pub fn new(start: u64, end: u64, size: u64, content_type: &str, boundary: &str) -> Self {
        Self {
            start,
            end,
            fields: format!(
                "\r\n--{}\r\nContent-Type: {}\r\nContent-Range: bytes {}-{}/{}\r\n\r\n",
                boundary, content_type, start, end, size
            ),
        }
    }

    pub fn content_length(&self) -> u64 {
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
                Some(end),
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

pub async fn get_object_range_stream(
    bucket: &Bucket,
    path: &str,
    mut start: u64,
    end: Option<u64>,
    sender: &mut hyper::body::Sender,
) -> Result<u16, Error> {
    loop {
        let command = Command::GetObjectRange { start, end };

        let res = get_command_stream(command, bucket, path, sender).await;

        match res {
            Ok(status) => return Ok(status),
            Err(S3CommandError::S3Error(e)) => return Err(e.into()),
            Err(S3CommandError::SendError(e)) => return Err(e.into()),
            Err(S3CommandError::SendTimeout { bytes_sent }) => {
                log::debug!(
                    "Received timeout trying to send S3 response to reader for range {start}-{end_str} of object {path}, going to retry with range {new_start}-{end_str} after reader becomes available",
                    new_start = start + bytes_sent as u64,
                    end_str = end.as_ref().map(u64::to_string).unwrap_or_default()
                );
                start += bytes_sent as u64;
                // wait for reader to be ready before requesting next range
                futures::future::poll_fn(|ctx| sender.poll_ready(ctx)).await?;
            }
        }
    }
}

pub async fn get_object_stream(
    bucket: &Bucket,
    path: &str,
    sender: &mut hyper::body::Sender,
) -> Result<u16, Error> {
    let command = Command::GetObject;
    let res = get_command_stream(command, bucket, path, sender).await;

    match res {
        Ok(status) => Ok(status),
        Err(S3CommandError::S3Error(e)) => Err(e.into()),
        Err(S3CommandError::SendError(e)) => Err(e.into()),
        Err(S3CommandError::SendTimeout { bytes_sent }) => {
            log::debug!("Received timeout trying to send S3 response to reader for object {path}, going to retry with range {bytes_sent}- after reader becomes available");
            futures::future::poll_fn(|ctx| sender.poll_ready(ctx)).await?;
            get_object_range_stream(bucket, path, bytes_sent as u64, None, sender).await
        }
    }
}

pub async fn get_command_stream(
    command: Command<'_>,
    bucket: &Bucket,
    path: &str,
    sender: &mut hyper::body::Sender,
) -> Result<u16, S3CommandError> {
    log::debug!("Executing S3 command streaming: {:?}", &command);
    let now = std::time::Instant::now();
    let request = Reqwest::new(bucket, path, command);
    let response = request.response().await?;
    let latency = now.elapsed();
    let status = response.status();
    let mut stream = response.bytes_stream();
    let mut total_bytes = 0;

    while let Some(chunk) = stream.next().await {
        let bytes = chunk.map_err(S3Error::from)?;
        let byte_len = bytes.len();
        #[cfg(debug_assertions)]
        let now = std::time::Instant::now();
        let res = timeout(Duration::from_secs(30), sender.send_data(bytes)).await;
        match res {
            Ok(Err(e)) => return Err(S3CommandError::SendError(e)),
            Err(_) => {
                return Err(S3CommandError::SendTimeout {
                    bytes_sent: total_bytes,
                })
            }
            Ok(Ok(_)) => {}
        }
        #[cfg(debug_assertions)]
        log::trace!(
            "Sent {} bytes to S3 response in {}",
            byte_len,
            format_duration(now.elapsed())
        );
        total_bytes += byte_len;
    }

    log::debug!(
        "Streamed {total_bytes} bytes from S3 in {} ({} request latency)",
        format_duration(now.elapsed()),
        format_duration(latency)
    );
    Ok(status.as_u16())
}

pub enum S3CommandError {
    S3Error(S3Error),
    SendError(hyper::Error),
    SendTimeout { bytes_sent: usize },
}

impl From<S3Error> for S3CommandError {
    fn from(value: S3Error) -> Self {
        Self::S3Error(value)
    }
}
