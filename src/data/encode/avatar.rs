use crate::acquire_db_connection;
use crate::data::encode::{media_is_animated, spawn_blocking};
use crate::data::get_system_bucket;
use crate::error::Error;
use crate::model::{ObjectType, S3Object};
use crate::util::join_api_url;
use chrono::Utc;
use std::process::{Command, Stdio};
use uuid::Uuid;

pub async fn generate_avatar(
    source_object_key: String,
    user_pk: i64,
    width: u32,
    height: u32,
    x: u32,
    y: u32,
) -> Result<S3Object, Error> {
    let mut connection = acquire_db_connection().await?;
    let system_bucket = get_system_bucket(&mut connection).await?;
    let (broker, bucket) = match system_bucket {
        Some(system_bucket) => system_bucket,
        None => return Err(Error::NoSystemBucketError),
    };
    drop(connection);

    let object_url = join_api_url(["get-object", &source_object_key])?.to_string();
    let is_animated = media_is_animated(&source_object_key).await?;
    let args = if is_animated {
        vec![
            String::from("-i"),
            object_url,
            String::from("-vf"),
            format!(
                "trim=duration=10,fps=15,crop={width}:{height}:{x}:{y},scale=256:256:flags=lanczos,format=yuva420p"
            ),
            String::from("-vframes"),
            String::from("150"),
            String::from("-c:v"),
            String::from("libwebp_anim"),
            String::from("-q:v"),
            String::from("40"),
            String::from("-loop"),
            String::from("0"),
            String::from("-preset"),
            String::from("default"),
            String::from("-vsync"),
            String::from("vfr"),
            String::from("-f"),
            String::from("webp"),
            String::from("-lossless"),
            String::from("0"),
            String::from("-v"),
            String::from("error"),
            String::from("pipe:1"),
        ]
    } else {
        vec![
            String::from("-i"),
            object_url,
            String::from("-vf"),
            format!("crop={width}:{height}:{x}:{y},scale=256:256"),
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
        "Spawning ffmpeg process to generate {}avatar for {source_object_key}",
        if is_animated { "animated " } else { "" }
    );
    let process = Command::new("ffmpeg")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::FfmpegProcessError(format!("Failed to spawn ffmpeg process: {e}")))?;

    let process_output = spawn_blocking(|| {
        process.wait_with_output().map_err(|e| {
            Error::FfmpegProcessError(format!("Failed to get ffmpeg process output: {e}"))
        })
    })
    .await?;

    let avatar_bytes = process_output.stdout;
    if !process_output.status.success() || !process_output.stderr.is_empty() {
        let error_msg = String::from_utf8_lossy(&process_output.stderr);
        if process_output.status.success() {
            log::warn!(
                "ffmpeg reported error generating avatar for {source_object_key}, going to check output for valid webp: {error_msg}"
            );
            if webp::Decoder::new(&avatar_bytes).decode().is_none() {
                return Err(Error::FfmpegProcessError(format!(
                    "ffmpeg output contains invalid webp data for avatar of {source_object_key}"
                )));
            }
        } else {
            return Err(Error::FfmpegProcessError(format!(
                "ffmpeg for avatar of {} failed with status {}: {}",
                &source_object_key, process_output.status, error_msg
            )));
        }
    }

    if avatar_bytes.is_empty() {
        return Err(Error::FfmpegProcessError(format!(
            "ffmpeg for avatar of {} received 0 bytes",
            &source_object_key
        )));
    }

    let object_id = source_object_key
        .split("/")
        .last()
        .unwrap()
        .split(".")
        .next()
        .unwrap();
    let avatar_id = Uuid::new_v4();
    let avatar_path = format!("{}/avatar_{}_{}.webp", user_pk, object_id, avatar_id);
    log::info!(
        "Storing avatar {} for object {}",
        &avatar_path,
        source_object_key
    );
    bucket
        .put_object_with_content_type(&avatar_path, &avatar_bytes, "image/webp")
        .await?;

    Ok(S3Object {
        object_key: avatar_path.clone(),
        sha256_hash: None,
        size_bytes: avatar_bytes.len() as i64,
        mime_type: String::from("image/webp"),
        fk_broker: broker.pk,
        fk_uploader: user_pk,
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
        derived_from: Some(source_object_key),
        object_type: ObjectType::Avatar,
    })
}
