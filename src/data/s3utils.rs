use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{ready, stream::IntoAsyncRead, StreamExt, TryStream};
use pin_project_lite::pin_project;
use ring::digest;
use s3::{command::Command, error::S3Error, request::Reqwest, request_trait::Request, Bucket};
use warp::hyper;

use crate::{data::s3utils, error::Error, model::S3Object};

pin_project! {
    pub struct FileReader<R> {
        #[pin]
        pub(crate) async_read: R,
        pub(crate) hasher: digest::Context,
        pub(crate) file_size: usize,
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
        let len = filled.len();
        *this.file_size += n;
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
        let res =
            s3utils::get_object_stream(&self.bucket, &self.object.object_key, &mut sender).await;

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

        match s3utils::get_object_range_stream(
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
            match s3utils::get_object_range_stream(
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

pub async fn get_object_range_stream(
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

pub async fn get_object_stream(
    bucket: &Bucket,
    path: &str,
    sender: &mut hyper::body::Sender,
) -> Result<u16, Error> {
    let command = Command::GetObject;
    get_command_stream(command, bucket, path, sender).await
}

pub async fn get_command_stream(
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
