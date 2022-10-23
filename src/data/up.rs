use chrono::Utc;
use diesel::{OptionalExtension, QueryDsl};
use futures::{io::BufReader, stream::IntoAsyncRead, AsyncReadExt, TryFutureExt, TryStream};
use s3::Bucket;
use uuid::Uuid;

use crate::{
    acquire_db_connection,
    diesel::{BoolExpressionMethods, ExpressionMethods, RunQueryDsl},
    error::Error,
    model::{Broker, S3Object, User},
    schema::s3_object,
};

use super::s3utils;

pub async fn upload_file<S>(
    broker: &Broker,
    user: &User,
    bucket: &Bucket,
    mut reader: s3utils::FileReader<IntoAsyncRead<S>>,
    object_key: String,
    content_type: String,
    file_id: &Uuid,
) -> Result<(S3Object, bool), Error>
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
            .get_result::<S3Object>(&mut connection)
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
                        return Ok((existing_object, true));
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
            creation_timestamp: Utc::now(),
        })
        .get_result::<S3Object>(&mut connection)
        .map_err(|e| Error::QueryError(e.to_string()))?;

    Ok((s3_object, false))
}

#[inline]
fn content_type_is_video(content_type: &str) -> bool {
    content_type.eq_ignore_ascii_case("video/mp4")
        || content_type.eq_ignore_ascii_case("video/webm")
        || content_type.eq_ignore_ascii_case("video/quicktime")
        || content_type.eq_ignore_ascii_case("video/x-msvideo")
        || content_type.eq_ignore_ascii_case("video/x-ms-wmv")
        || content_type.eq_ignore_ascii_case("video/x-matroska")
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
            })
            .get_result::<S3Object>(&mut connection)
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
