use crate::data::encode::spawn_encode_task_blocking;
use crate::error::Error;
use crate::util::join_api_url;
use serde::Deserialize;
use std::ffi::OsStr;
use std::process::{Command, Output, Stdio};

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

    spawn_encode_task_blocking(|| {
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

#[derive(Debug, Deserialize)]
pub struct MediaProbe {
    #[serde(default)]
    pub streams: Vec<MediaStream>,
    pub format: MediaFormat,
}

#[derive(Debug, Deserialize)]
pub struct MediaFormat {
    pub duration: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MediaStream {
    pub index: i32,
    pub codec_type: StreamType,
    pub codec_name: Option<String>,
    pub channels: Option<i32>,
    #[serde(default)]
    pub tags: StreamTags,
    #[serde(default)]
    pub disposition: StreamDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StreamType {
    Video,
    Audio,
    Subtitle,
    #[serde(other)]
    Other,
}

#[derive(Debug, Default, Deserialize)]
pub struct StreamTags {
    pub language: Option<String>,
    pub title: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct StreamDisposition {
    #[serde(default, deserialize_with = "deserialize_ffmpeg_bool")]
    pub default: bool,
    #[serde(default, deserialize_with = "deserialize_ffmpeg_bool")]
    pub forced: bool,
}

fn deserialize_ffmpeg_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(u8::deserialize(deserializer)? != 0)
}

pub async fn probe_media(object_url: &str) -> Result<MediaProbe, Error> {
    let process_output = get_ffprobe_output([
        "-v",
        "error",
        "-show_streams",
        "-show_format",
        "-of",
        "json",
        "-i",
        object_url,
    ])
    .await?;

    if !process_output.status.success() {
        return Err(Error::FfmpegProcessError(format!(
            "ffprobe failed with status {}: {}",
            process_output.status,
            String::from_utf8_lossy(&process_output.stderr),
        )));
    }

    if !process_output.stderr.is_empty() {
        log::warn!(
            "ffprobe reported error while probing media {} but finished successfully, proceeding: {}",
            object_url,
            String::from_utf8_lossy(&process_output.stderr),
        );
    }

    serde_json::from_slice(&process_output.stdout)
        .map_err(|e| Error::FfmpegProcessError(format!("Failed to parse ffprobe output: {e}")))
}

pub struct ObjectDuration {
    pub duration_str: String,
    pub duration_sec: f32,
}

pub async fn get_object_duration(object_url: &str) -> Result<ObjectDuration, Error> {
    let process = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            object_url,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::FfmpegProcessError(format!("Failed to spawn ffprobe process: {e}")))?;
    let ffprobe_output = spawn_encode_task_blocking(|| {
        process
            .wait_with_output()
            .map_err(|e| Error::FfmpegProcessError(format!("ffprobe failed: {e}")))
    })
    .await?;

    if !ffprobe_output.status.success() {
        let error_msg = String::from_utf8_lossy(&ffprobe_output.stderr);
        return Err(Error::FfmpegProcessError(format!(
            "ffprobe failed with status {}: {error_msg}",
            ffprobe_output.status
        )));
    }

    let output_string = String::from_utf8_lossy(&ffprobe_output.stdout).into_owned();
    match output_string.trim().parse::<f32>() {
        Ok(secs) => Ok(ObjectDuration {
            duration_str: output_string,
            duration_sec: secs,
        }),
        Err(e) => Err(Error::FfmpegProcessError(format!(
            "Received invalid duration from ffprobe '{}', {e}",
            output_string.trim()
        ))),
    }
}

pub async fn get_object_start_time(object_url: &str) -> Result<f64, Error> {
    let process = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=start_time",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            object_url,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::FfmpegProcessError(format!("Failed to spawn ffprobe process: {e}")))?;

    let output = spawn_encode_task_blocking(|| {
        process
            .wait_with_output()
            .map_err(|e| Error::FfmpegProcessError(format!("ffprobe failed: {e}")))
    })
    .await?;

    if !output.status.success() {
        return Err(Error::FfmpegProcessError(format!(
            "ffprobe failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
        )));
    }

    let output_string = String::from_utf8_lossy(&output.stdout);

    let start_time = output_string.trim().parse::<f64>().map_err(|e| {
        Error::FfmpegProcessError(format!(
            "Received invalid start time from ffprobe '{}': {e}",
            output_string.trim(),
        ))
    })?;

    if !start_time.is_finite() || start_time < 0.0 {
        return Err(Error::FfmpegProcessError(format!(
            "ffprobe returned invalid start time '{}'",
            output_string.trim(),
        )));
    }

    Ok(start_time)
}

pub async fn get_video_resolution(
    source_object_key: &str,
    object_url: &str,
) -> Result<usize, Error> {
    let resolution_probe_process = Command::new("ffprobe")
        .args([
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=height",
            "-of",
            "csv=s=x:p=0",
            "-v",
            "error",
            object_url,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::FfmpegProcessError(format!("Failed to spawn ffprobe process: {e}")))?;

    let process_output = spawn_encode_task_blocking(|| {
        resolution_probe_process.wait_with_output().map_err(|e| {
            Error::FfmpegProcessError(format!("Failed to get ffprobe process output: {e}"))
        })
    })
    .await;
    match process_output {
        Ok(process_output) => {
            let resolution_string = String::from_utf8_lossy(&process_output.stdout)
                .trim()
                .to_string();
            if !process_output.status.success() || !process_output.stderr.is_empty() {
                let error_msg = String::from_utf8_lossy(&process_output.stderr);
                if process_output.status.success() {
                    log::warn!(
                        "ffprobe reported error while getting resolution for {source_object_key}, going to check output validity: {error_msg}"
                    );
                } else {
                    return Err(Error::FfmpegProcessError(format!(
                        "ffprobe failed with status {}: {}",
                        process_output.status, error_msg
                    )));
                }
            }

            resolution_string.trim().parse::<usize>().map_err(|_| {
                Error::FfmpegProcessError(format!(
                    "Invalid resolution from ffprobe for '{source_object_key}': {resolution_string}"
                ))
            })
        }
        Err(e) => Err(e),
    }
}
