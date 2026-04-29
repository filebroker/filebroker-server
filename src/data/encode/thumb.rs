use crate::data::encode::{
    content_type_is_audio, content_type_is_image, content_type_is_video, media_has_video,
    spawn_blocking,
};
use crate::error::{Error, TransactionRuntimeError};
use crate::model::{Broker, ObjectType, S3Object, User};
use crate::schema::s3_object;
use crate::task::LockedObjectsTaskSentinel;
use crate::util::join_api_url;
use crate::{acquire_db_connection, retry_on_serialization_failure, run_serializable_transaction};
use chrono::Utc;
use diesel::ExpressionMethods;
use diesel_async::RunQueryDsl;
use diesel_async::scoped_futures::ScopedFutureExt;
use s3::Bucket;
use std::process::{Command, Stdio};
use uuid::Uuid;

pub async fn generate_thumbnail(
    bucket: Bucket,
    source_object_key: String,
    file_id: Uuid,
    content_type: String,
    broker: Broker,
    user: User,
    thumbnail_lock_acquired: bool,
) -> Result<(), Error> {
    let object_url = join_api_url(["get-object", &source_object_key])?.to_string();

    let content_type_is_video = content_type_is_video(&content_type);
    let content_type_is_image = content_type_is_image(&content_type);
    let content_type_is_audio = content_type_is_audio(&content_type);
    let thumbnail_extension;
    let thumbnail_content_type;

    if content_type_is_video || content_type_is_image || content_type_is_audio {
        if content_type_is_audio && !media_has_video(&object_url).await? {
            log::info!(
                "Not creating thumbnail for audio object {} without video stream, marking as thumbnail_disabled",
                &source_object_key
            );
            let mut connection = acquire_db_connection().await?;
            diesel::update(s3_object::table)
                .filter(s3_object::object_key.eq(&source_object_key))
                .set(s3_object::thumbnail_disabled.eq(true))
                .execute(&mut connection)
                .await?;
            return Ok(());
        }

        let _locked_object_task_sentinel = if thumbnail_lock_acquired {
            None
        } else {
            match LockedObjectsTaskSentinel::acquire(
                "thumbnail_locked_at",
                "thumbnail_object_key",
                source_object_key.clone(),
            )
            .await?
            {
                Some(sentinel) => Some(sentinel),
                None => {
                    log::info!(
                        "Aborting thumbnail generation for object {} because it has already been locked",
                        &source_object_key
                    );
                    return Ok(());
                }
            }
        };

        // NOTE
        // ffmpeg does not support decoding animated webp, only decode: https://trac.ffmpeg.org/ticket/4907
        // hence thumbnails can not be generated in this case
        let args = if content_type_is_video || content_type_is_audio {
            thumbnail_extension = "webp";
            thumbnail_content_type = String::from("image/webp");
            vec![
                String::from("-i"),
                object_url,
                String::from("-vf"),
                String::from(r"thumbnail=300,scale=iw*min(640/iw\,360/ih):ih*min(640/iw\,360/ih)"),
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
                object_url,
                String::from("-vf"),
                String::from(r"thumbnail=300,scale=iw*min(640/iw\,360/ih):ih*min(640/iw\,360/ih)"),
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

        log::info!("Spawning ffmpeg process to generate thumbnail for {source_object_key}");
        let process = Command::new("ffmpeg")
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                Error::FfmpegProcessError(format!("Failed to spawn ffmpeg process: {e}"))
            })?;

        let process_output = spawn_blocking(|| {
            process.wait_with_output().map_err(|e| {
                Error::FfmpegProcessError(format!("Failed to get ffmpeg process output: {e}"))
            })
        })
        .await?;

        let thumb_bytes = process_output.stdout;
        if !process_output.status.success() || !process_output.stderr.is_empty() {
            let error_msg = String::from_utf8_lossy(&process_output.stderr);
            if process_output.status.success() {
                log::warn!(
                    "ffmpeg reported error generating thumbnail for {source_object_key}, going to check output for valid webp: {error_msg}"
                );
                if webp::Decoder::new(&thumb_bytes).decode().is_none() {
                    return Err(Error::FfmpegProcessError(format!(
                        "ffmpeg output contains invalid webp data for thumbnail of {source_object_key}"
                    )));
                }
            } else {
                return Err(Error::FfmpegProcessError(format!(
                    "ffmpeg for thumbnail of {} failed with status {}: {}",
                    &source_object_key, process_output.status, error_msg
                )));
            }
        }

        if thumb_bytes.is_empty() {
            log::warn!("Received 0 bytes for thumbnail {file_id}");
            return Ok(());
        }

        let thumb_path = format!("thumb_{}.{}", &file_id.to_string(), thumbnail_extension);
        log::info!(
            "Storing thumbnail {} for object {}",
            &thumb_path,
            source_object_key
        );
        bucket
            .put_object_with_content_type(&thumb_path, &thumb_bytes, &thumbnail_content_type)
            .await?;

        let mut connection = acquire_db_connection().await?;
        let persist_result = run_serializable_transaction(&mut connection, |connection| {
            async {
                let s3_object = diesel::insert_into(s3_object::table)
                    .values(&S3Object {
                        object_key: thumb_path.clone(),
                        sha256_hash: None,
                        size_bytes: thumb_bytes.len() as i64,
                        mime_type: thumbnail_content_type.clone(),
                        fk_broker: broker.pk,
                        fk_uploader: user.pk,
                        thumbnail_object_key: None,
                        creation_timestamp: Utc::now(),
                        filename: None,
                        hls_master_playlist: None,
                        hls_disabled: true,
                        hls_locked_at: None,
                        thumbnail_locked_at: None,
                        hls_fail_count: None,
                        thumbnail_fail_count: None,
                        thumbnail_disabled: true,
                        metadata_locked_at: None,
                        metadata_fail_count: None,
                        derived_from: Some(source_object_key.clone()),
                        object_type: ObjectType::Thumbnail,
                    })
                    .get_result::<S3Object>(connection)
                    .await
                    .map_err(retry_on_serialization_failure)?;

                let update_count = diesel::update(s3_object::table)
                    .filter(s3_object::object_key.eq(&source_object_key))
                    .set(s3_object::thumbnail_object_key.eq(&s3_object.object_key))
                    .execute(connection)
                    .await
                    .map_err(retry_on_serialization_failure)?;

                if update_count == 0 {
                    // object no longer exists, delete thumb
                    log::info!("Created thumbnail for object {source_object_key} that no longer exists, deleting created thumbnail {thumb_path}");
                    return Err(TransactionRuntimeError::Rollback(Error::QueryError(
                        format!("Source object {source_object_key} for thumbnail no longer exists"),
                    )));
                }

                Ok(())
            }
            .scope_boxed()
        })
            .await;
        drop(connection);

        if let Err(e) = persist_result {
            log::error!(
                "Failed to await and persist thumbnail object to db with error: {e}. Going to delete created object"
            );
            if let Err(e) = bucket.delete_object(&thumb_path).await {
                log::error!("Failed to delete obsolete thumbnail {thumb_path}: {e}");
            }
            return Err(e);
        }

        Ok(())
    } else {
        log::debug!("Not creating thumbnail for unsupported content type '{content_type}'");
        Ok(())
    }
}
