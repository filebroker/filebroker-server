pub mod avatar;
pub mod hls;
pub mod metadata;
pub mod thumb;

use regex::Regex;
use rusty_pool::ThreadPool;
use std::ffi::OsStr;
use std::{
    collections::HashSet,
    process::{Command, Output, Stdio},
};
use tokio::sync::Semaphore;

use crate::{CONCURRENT_VIDEO_TRANSCODE_LIMIT, error::Error, util::join_api_url};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref ENCODE_POOL: ThreadPool = rusty_pool::Builder::new()
        .name(String::from("encode_pool"))
        .build();
    pub static ref VIDEO_TRANSCODE_SEMAPHORE: Semaphore = {
        let limit = CONCURRENT_VIDEO_TRANSCODE_LIMIT.unwrap_or_else(|| {
            let num_cpus = num_cpus::get();
            (num_cpus / 2).clamp(1, 8)
        });
        log::info!("CONCURRENT_VIDEO_TRANSCODE_LIMIT set to {limit}");
        Semaphore::new(limit)
    };
    pub static ref SUBMITTED_HLS_TRANSCODINGS: parking_lot::Mutex<HashSet<String>> =
        parking_lot::Mutex::new(HashSet::new());
    pub static ref EXIF_DATE_FORMAT_REGEX: Regex =
        Regex::new(r"(\d+):(\d+):(\d+) (\d+):(\d+)(:\d+)?(\+\d+:\d+)?")
            .expect("Failed to compile EXIF date format regex");
}

async fn spawn_blocking<R: Send + 'static>(
    task: impl FnOnce() -> Result<R, Error> + Send + 'static,
) -> Result<R, Error> {
    let join_handle = ENCODE_POOL.evaluate(task);

    match join_handle.receiver.await {
        Ok(t) => t,
        Err(_) => Err(Error::CancellationError),
    }
}

#[inline]
pub fn content_type_is_video(content_type: &str) -> bool {
    content_type.starts_with("video/")
}

#[inline]
pub fn content_type_is_image(content_type: &str) -> bool {
    content_type.starts_with("image/")
}

#[inline]
pub fn content_type_is_audio(content_type: &str) -> bool {
    content_type.starts_with("audio/")
}

pub async fn media_has_video(object_url: &str) -> Result<bool, Error> {
    media_has_stream("v", object_url).await
}

pub async fn media_has_audio(object_url: &str) -> Result<bool, Error> {
    media_has_stream("a", object_url).await
}

/*
Generating HLS playlists with subtitles is broken: https://trac.ffmpeg.org/ticket/9719#no1
async fn video_has_subtitles(object_url: &str) -> Result<bool, Error> {
    video_has_stream("s", object_url).await
}
*/

async fn media_has_stream(stream: &str, object_url: &str) -> Result<bool, Error> {
    let process_output = get_ffprobe_output([
        "-show_streams",
        "-select_streams",
        stream,
        "-v",
        "error",
        "-i",
        object_url,
    ])
    .await;
    match process_output {
        Ok(process_output) => {
            if !process_output.status.success() || !process_output.stderr.is_empty() {
                let error_msg = String::from_utf8_lossy(&process_output.stderr);
                if process_output.status.success() {
                    // if the exit code is successful then the selected streams should have been written to stdout
                    // it should be fine to ignore reported errors if the process exits successfully in this case
                    // (unlike ffmpeg video transcoding where the process exits successfully if the transcoding fails
                    // halfway through) but log the error as a warning just in case
                    log::warn!(
                        "ffprobe reported error selecting streams for {object_url} but the process finished successfully, proceeding: {error_msg}"
                    );
                } else {
                    return Err(Error::FfmpegProcessError(format!(
                        "ffprobe failed with status {}: {}",
                        process_output.status, error_msg
                    )));
                }
            }
            Ok(!process_output.stdout.is_empty())
        }
        Err(e) => Err(e),
    }
}

pub async fn get_ffprobe_output<I, S>(args: I) -> Result<Output, Error>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let ffprobe_process = Command::new("ffprobe")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::FfmpegProcessError(format!("Failed to spawn ffprobe process: {e}")))?;

    spawn_blocking(|| {
        ffprobe_process.wait_with_output().map_err(|e| {
            Error::FfmpegProcessError(format!("Failed to get ffprobe process output: {e}"))
        })
    })
    .await
}

pub async fn media_is_animated(object_key: &str) -> Result<bool, Error> {
    let object_url = join_api_url(["get-object", object_key])?.to_string();
    let process_output = get_ffprobe_output([
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=nb_frames",
        "-of",
        "csv=p=0",
        object_url.as_str(),
    ])
    .await;

    match process_output {
        Ok(process_output) => {
            if !process_output.status.success() || !process_output.stderr.is_empty() {
                let error_msg = String::from_utf8_lossy(&process_output.stderr);
                if process_output.status.success() {
                    log::warn!(
                        "ffprobe reported error selecting frame count for {object_key} but the process finished successfully, proceeding: {error_msg}"
                    );
                } else {
                    return Err(Error::FfmpegProcessError(format!(
                        "ffprobe failed with status {}: {}",
                        process_output.status, error_msg
                    )));
                }
            }

            let output_str = String::from_utf8_lossy(&process_output.stdout);
            let nb_frames = output_str.trim().parse::<u32>();
            if let Ok(nb_frames) = nb_frames {
                Ok(nb_frames > 1)
            } else {
                log::debug!(
                    "Expected nb_frames to be a u32 for {object_key} but got '{}', proceeding as non-animated",
                    output_str.trim()
                );
                Ok(false)
            }
        }
        Err(e) => Err(e),
    }
}
