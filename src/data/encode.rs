use chrono::Utc;
use futures::{io::BufReader, AsyncReadExt, Future, TryFutureExt};
use rusty_pool::ThreadPool;
use s3::Bucket;
use uuid::Uuid;

use lazy_static::lazy_static;

use crate::{
    acquire_db_connection,
    diesel::{ExpressionMethods, RunQueryDsl},
    error::Error,
    model::{Broker, S3Object, User},
    schema::s3_object,
};

lazy_static! {
    pub static ref ENCODE_POOL: ThreadPool = rusty_pool::Builder::new()
        .name(String::from("encode_pool"))
        .build();
}

async fn submit_tokio_future_to_pool<R: Send + 'static>(
    future: impl Future<Output = Result<R, Error>> + 'static + Send,
) -> Result<R, Error> {
    let tokio_handle = tokio::runtime::Handle::current();
    let join_handle = ENCODE_POOL.spawn_await(async move {
        match tokio_handle.spawn(future).await {
            Ok(t) => t,
            Err(_) => Err(Error::CancellationError),
        }
    });

    match join_handle.receiver.await {
        Ok(t) => t,
        Err(_) => Err(Error::CancellationError),
    }
}

pub async fn generate_thumbnail(
    bucket: Bucket,
    source_object_key: String,
    file_id: Uuid,
    content_type: String,
    broker: Broker,
    user: User,
) -> Result<Option<S3Object>, Error> {
    submit_tokio_future_to_pool(async move {
        let presigned_get_object = bucket.presign_get(&source_object_key, 1800, None)?;

        let content_type_is_video = content_type_is_video(&content_type);
        let content_type_is_image = content_type_is_image(&content_type);
        let thumbnail_extension;
        let thumbnail_content_type;

        if content_type_is_video || content_type_is_image {
            let args = if content_type_is_video {
                thumbnail_extension = "webp";
                thumbnail_content_type = String::from("image/webp");
                vec![
                    String::from("-i"),
                    presigned_get_object,
                    String::from("-vf"),
                    String::from(r"thumbnail,scale=iw*min(640/iw\,360/ih):ih*min(640/iw\,360/ih)"),
                    String::from("-vframes"),
                    String::from("1"),
                    String::from("-f"),
                    String::from("image2"),
                    String::from("-codec"),
                    String::from("libwebp"),
                    String::from("-update"),
                    String::from("1"),
                    String::from("-quality"),
                    String::from("80"),
                    String::from("-v"),
                    String::from("error"),
                    String::from("pipe:1"),
                ]
            } else {
                thumbnail_extension = "webp";
                thumbnail_content_type = String::from("image/webp");
                vec![
                    String::from("-i"),
                    presigned_get_object,
                    String::from("-vf"),
                    String::from(r"thumbnail,scale=iw*min(640/iw\,360/ih):ih*min(640/iw\,360/ih)"),
                    String::from("-pix_fmt"),
                    String::from("bgra"),
                    String::from("-f"),
                    String::from("image2"),
                    String::from("-codec"),
                    String::from("libwebp"),
                    String::from("-quality"),
                    String::from("80"),
                    String::from("-update"),
                    String::from("1"),
                    String::from("-frames:v"),
                    String::from("1"),
                    String::from("-v"),
                    String::from("error"),
                    String::from("pipe:1"),
                ]
            };

            log::info!(
                "Spawning ffmpeg process to generate thumbnail for {}",
                source_object_key
            );
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
            log::info!(
                "Storing thumbnail {} for object {}",
                &thumb_path,
                source_object_key
            );
            bucket.put_object(&thumb_path, &thumb_bytes).await?;

            let mut connection = acquire_db_connection()?;
            let s3_object = diesel::insert_into(s3_object::table)
                .values(&S3Object {
                    object_key: thumb_path,
                    sha256_hash: None,
                    size_bytes: thumb_bytes.len() as i64,
                    mime_type: thumbnail_content_type,
                    fk_broker: broker.pk,
                    fk_uploader: user.pk,
                    thumbnail_object_key: None,
                    creation_timestamp: Utc::now(),
                    filename: None,
                })
                .get_result::<S3Object>(&mut connection)
                .map_err(|e| Error::QueryError(e.to_string()))?;

            diesel::update(s3_object::table)
                .filter(s3_object::object_key.eq(&source_object_key))
                .set(s3_object::thumbnail_object_key.eq(&s3_object.object_key))
                .execute(&mut connection)
                .map_err(|e| Error::QueryError(e.to_string()))?;

            Ok(Some(s3_object))
        } else {
            log::debug!(
                "Not creating thumbnail for unsupported content type '{}'",
                content_type
            );
            Ok(None)
        }
    })
    .await
}

#[inline]
fn content_type_is_video(content_type: &str) -> bool {
    content_type.starts_with("video/")
}

#[inline]
fn content_type_is_image(content_type: &str) -> bool {
    content_type.starts_with("image/")
}
