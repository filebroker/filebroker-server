use crate::data::encode::{
    SUBMITTED_HLS_TRANSCODINGS, VIDEO_TRANSCODE_SEMAPHORE, output,
    output::{HlsOutputStream, S3UploadResult, UploadedHlsStream},
    probe::{self, MediaStream, StreamType},
    spawn_encode_task_blocking,
};
use crate::error::{Error, TransactionRuntimeError};
use crate::model::{
    Broker, HlsAudioStream, HlsStream, HlsSubtitleStream, ObjectType, S3Object, User,
};
use crate::schema::{hls_audio_stream, hls_stream, hls_subtitle_stream, s3_object};
use crate::task::LockedObjectsTaskSentinel;
use crate::util::{format_duration, join_api_url};
use crate::{acquire_db_connection, run_serializable_transaction};
use chrono::Utc;
use diesel::ExpressionMethods;
use diesel_async::RunQueryDsl;
use futures::future::try_join_all;
use itertools::Itertools;
use s3::Bucket;
use std::cmp::Reverse;
use std::fmt::Write;
use std::process::{Command, Stdio};
use tokio::task::JoinHandle;
use uuid::Uuid;

static VIDEO_TRANSCODE_RESOLUTIONS: [TranscodeResolution; 5] = [
    TranscodeResolution {
        resolution: 2160,
        target_bitrate: "27M",
        min_bitrate: "13M",
        max_bitrate: "39150K",
        downscale_target: true,
    },
    TranscodeResolution {
        resolution: 1440,
        target_bitrate: "13M",
        min_bitrate: "6750K",
        max_bitrate: "19575K",
        downscale_target: false,
    },
    TranscodeResolution {
        resolution: 1080,
        target_bitrate: "4500K",
        min_bitrate: "2250K",
        max_bitrate: "6525K",
        downscale_target: true,
    },
    TranscodeResolution {
        resolution: 720,
        target_bitrate: "2700K",
        min_bitrate: "1350K",
        max_bitrate: "3930K",
        downscale_target: true,
    },
    TranscodeResolution {
        resolution: 360,
        target_bitrate: "750K",
        min_bitrate: "384K",
        max_bitrate: "1200K",
        downscale_target: true,
    },
];

fn hls_audio_bitrate(channels: Option<i32>) -> &'static str {
    match channels {
        Some(1) => "64k",
        Some(2) => "128k",
        Some(3) => "192k",
        Some(4) => "256k",
        Some(5) => "320k",
        Some(6) => "384k", // 5.1
        Some(7) => "448k",
        Some(8) => "512k", // 7.1
        Some(_) => "512k",
        None => "128k",
    }
}

fn is_bitmap_subtitle_codec(codec_name: &str) -> bool {
    matches!(
        codec_name,
        "hdmv_pgs_subtitle" | "dvd_subtitle" | "dvb_subtitle" | "xsub"
    )
}

#[derive(Debug, Clone, Copy)]
struct TranscodeResolution {
    resolution: usize,
    target_bitrate: &'static str,
    min_bitrate: &'static str,
    max_bitrate: &'static str,
    downscale_target: bool,
}

/// Generate an HLS playlist for the given object. Videos are transcoded to h264 and split into up to 3 streams, the source resolution and
/// the two nearest lower resolutions.
///
/// `hls_lock_acquired` should only be `true` if the caller manages the `VIDEO_TRANSCODE_SEMAPHORE` and `hls_locked_at` timestamps
pub async fn generate_hls_playlist(
    bucket: Bucket,
    source_object_key: String,
    file_id: Uuid,
    broker: Broker,
    user: User,
    hls_lock_acquired: bool,
) -> Result<(), Error> {
    let _submitted_hls_transcoding_sentinel =
        SubmittedHlsTranscodingSentinel::new(&source_object_key);

    let (_locked_object_task_sentinel, _semaphore) = if hls_lock_acquired {
        (None, None)
    } else {
        log::debug!("Waiting to acquire permit to start HLS transcode for {source_object_key}");
        let semaphore = VIDEO_TRANSCODE_SEMAPHORE
            .acquire()
            .await
            .map_err(|_| Error::CancellationError)?;

        match LockedObjectsTaskSentinel::acquire(
            "hls_locked_at",
            "hls_master_playlist",
            source_object_key.clone(),
        )
        .await?
        {
            Some(sentinel) => (Some(sentinel), Some(semaphore)),
            None => {
                log::info!(
                    "Aborting HLS transcode for object {source_object_key} because it has already been locked"
                );
                return Ok(());
            }
        }
    };

    log::info!("Starting HLS transcode for {source_object_key}");
    let start_time = std::time::Instant::now();
    let object_url = join_api_url(["get-object", &source_object_key])?.to_string();

    let resolution = probe::get_video_resolution(&source_object_key, &object_url).await?;
    let media_probe = probe::probe_media(&object_url).await?;
    let audio_streams = media_probe
        .streams
        .iter()
        .filter(|stream| stream.codec_type == StreamType::Audio)
        .collect::<Vec<_>>();
    let subtitle_streams = media_probe
        .streams
        .iter()
        .filter(|stream| stream.codec_type == StreamType::Subtitle)
        .filter(|stream| match stream.codec_name.as_deref() {
            Some(codec_name) if !is_bitmap_subtitle_codec(codec_name) => true,
            Some(codec_name) => {
                log::warn!(
                    "Skipping unsupported bitmap subtitle stream {} with codec '{}'",
                    stream.index,
                    codec_name,
                );
                false
            }
            None => {
                log::warn!(
                    "Skipping subtitle stream {} because ffprobe returned no codec name",
                    stream.index,
                );
                false
            }
        })
        .collect::<Vec<_>>();
    let source_duration_str = media_probe.format.duration.as_deref().ok_or_else(|| {
        Error::FfmpegProcessError(format!(
            "ffprobe returned no duration for object '{source_object_key}'"
        ))
    })?;
    let source_duration_sec = source_duration_str.parse::<f32>().map_err(|e| {
        Error::FfmpegProcessError(format!(
            "Received invalid duration from ffprobe '{source_duration_str}': {e}"
        ))
    })?;
    if !source_duration_sec.is_finite() || source_duration_sec < 0.0 {
        return Err(Error::FfmpegProcessError(format!(
            "ffprobe returned invalid duration '{source_duration_str}' for object '{source_object_key}'"
        )));
    }

    let has_muxed_audio = audio_streams.len() == 1;
    let has_separate_audio = audio_streams.len() > 1;

    let mut video_transcode_resolutions = VIDEO_TRANSCODE_RESOLUTIONS;
    video_transcode_resolutions.sort_by_key(|t| Reverse(t.resolution));

    let target_bitrate = video_transcode_resolutions
        .into_iter()
        .find(|t| t.resolution <= resolution)
        .unwrap_or_else(|| *video_transcode_resolutions.last().unwrap());

    let downscaled_bitrates = video_transcode_resolutions
        .into_iter()
        .filter(|t| t.resolution < target_bitrate.resolution && t.downscale_target)
        .take(2)
        .collect::<Vec<_>>();

    let video_stream_count = downscaled_bitrates.len() + 1;
    let separate_audio_stream_count = if audio_streams.len() > 1 {
        audio_streams.len()
    } else {
        0
    };
    let subtitle_stream_count = subtitle_streams.len();
    let audio_variant_offset = video_stream_count;
    let subtitle_variant_offset = audio_variant_offset + separate_audio_stream_count;

    // generate string that splits the input video into separate streams for the source resolution and the two downscaled resolutions
    // e.g. [0:v]split=3[v1][v2][v3]; [v1]copy[v1out]; [v2]scale=w=1280:h=720[v2out]; [v3]scale=w=640:h=360[v3out]
    let mut split_string = String::from("[0:v]split=");
    split_string.push_str(&(downscaled_bitrates.len() + 1).to_string());
    split_string.push_str(&(0..=downscaled_bitrates.len()).fold(
        String::new(),
        |mut output, idx| {
            // writing to sting never fails
            write!(output, "[v{}]", idx + 1).unwrap();
            output
        },
    ));
    split_string.push_str("; [v1]format=yuv420p,fps=source_fps[v1out]");
    if !downscaled_bitrates.is_empty() {
        split_string.push_str("; ");
        let scale_string = downscaled_bitrates
            .iter()
            .enumerate()
            .map(|(i, bitrate)| {
                format!(
                    "[v{idx}]scale=w=-2:h={resolution},format=yuv420p,fps=source_fps[v{idx}out]",
                    idx = i + 2,
                    resolution = bitrate.resolution
                )
            })
            .join("; ");

        split_string.push_str(&scale_string);
    }

    let mut transcode_args = vec![
        String::from("-i"),
        object_url.clone(),
        String::from("-v"),
        String::from("error"),
        String::from("-filter_complex"),
        split_string,
    ];

    let mut output_reader_join_handles = Vec::new();

    #[cfg(unix)]
    let fifo_dir = tempfile::tempdir()
        .map_err(|e| Error::IoError(format!("Failed to create tempdir: {e}")))?;

    apply_video_transcode_args_and_spawn_output_reader(
        &mut transcode_args,
        video_stream_count,
        target_bitrate,
        &downscaled_bitrates,
        &fifo_dir,
        &bucket,
        &file_id,
        has_muxed_audio,
        &mut output_reader_join_handles,
    )?;

    apply_audio_transcode_args_and_spawn_output_reader(
        &mut transcode_args,
        video_stream_count,
        has_muxed_audio,
        has_separate_audio,
        &audio_streams,
        audio_variant_offset,
        &fifo_dir,
        &bucket,
        &file_id,
        &mut output_reader_join_handles,
    )?;

    transcode_args.push(String::from("-f"));
    transcode_args.push(String::from("hls"));
    transcode_args.push(String::from("-hls_time"));
    transcode_args.push(String::from("2"));
    transcode_args.push(String::from("-hls_playlist_type"));
    transcode_args.push(String::from("vod"));
    transcode_args.push(String::from("-hls_flags"));
    transcode_args.push(String::from("independent_segments+single_file"));
    transcode_args.push(String::from("-hls_segment_type"));
    transcode_args.push(String::from("mpegts"));
    transcode_args.push(String::from("-master_pl_name"));
    transcode_args.push(String::from("master.m3u8"));
    transcode_args.push(String::from("-var_stream_map"));
    transcode_args.push(build_var_stream_map(
        video_stream_count,
        has_muxed_audio,
        has_separate_audio,
        &audio_streams,
    ));
    #[cfg(unix)]
    transcode_args.push(format!("{}/stream_%v.m3u8", fifo_dir.path().display()));
    #[cfg(not(unix))]
    transcode_args.push(format!("{}_stream_%v.m3u8", &file_id));

    let master_playlist_join_handle = spawn_hls_master_playlist_reader(&fifo_dir)?;

    log::debug!("Spawning HLS transcode ffmpeg command with args {transcode_args:?}");
    let process = match Command::new("ffmpeg")
        .args(transcode_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::FfmpegProcessError(format!("Failed to spawn ffmpeg process: {e}")))
    {
        Ok(process) => process,
        Err(e) => {
            master_playlist_join_handle.abort();
            for handle in output_reader_join_handles {
                handle.abort();
            }
            return Err(Error::FfmpegProcessError(format!(
                "Error in ffmpeg process: {e}"
            )));
        }
    };

    let process_output = spawn_encode_task_blocking(|| {
        process.wait_with_output().map_err(|e| {
            Error::FfmpegProcessError(format!("Failed to get ffmpeg process output: {e}"))
        })
    })
    .await;
    let process_output = match process_output {
        Ok(process_output) if !process_output.status.success() => {
            master_playlist_join_handle.abort();

            for handle in &output_reader_join_handles {
                handle.abort();
            }

            // Ensure aborted uploads have actually stopped before deleting.
            let _ = master_playlist_join_handle.await;

            for handle in output_reader_join_handles {
                let _ = handle.await;
            }

            delete_hls_transcode_objects(
                &bucket,
                &file_id,
                video_stream_count,
                separate_audio_stream_count,
                subtitle_stream_count,
            )
            .await;

            let error_msg = String::from_utf8_lossy(&process_output.stderr);

            return Err(Error::FfmpegProcessError(format!(
                "ffmpeg for hls_transcoding of {source_object_key} failed with status {}: {error_msg}",
                process_output.status,
            )));
        }
        Err(e) => {
            master_playlist_join_handle.abort();
            for handle in output_reader_join_handles {
                handle.abort();
            }
            return Err(e);
        }
        Ok(process_output) => process_output,
    };

    let finalization_result = async {
        let (master_playlist_bytes, mut stream_results) = await_and_verify_results(
            master_playlist_join_handle,
            output_reader_join_handles,
            process_output.stderr,
            &source_object_key,
            source_duration_sec,
            &bucket,
        )
        .await?;

        let subtitle_results = generate_hls_subtitle_outputs(
            &object_url,
            &subtitle_streams,
            subtitle_variant_offset,
            source_duration_sec,
            &fifo_dir,
            &bucket,
            &file_id,
        )
        .await?;

        let final_master_playlist = finalize_hls_master_playlist(
            &master_playlist_bytes,
            video_stream_count,
            &stream_results,
            &subtitle_results,
        )?;

        let master_playlist_result =
            upload_hls_master_playlist(&bucket, &file_id, &final_master_playlist).await?;

        stream_results.extend(subtitle_results);

        persist_hls_transcode_results(
            &source_object_key,
            broker.pk,
            user.pk,
            master_playlist_result,
            stream_results,
        )
        .await
    }
    .await;

    if let Err(e) = finalization_result {
        log::error!(
            "Failed to finalise and persist HLS transcode results with error: {e}. Going to delete created objects"
        );

        delete_hls_transcode_objects(
            &bucket,
            &file_id,
            video_stream_count,
            separate_audio_stream_count,
            subtitle_stream_count,
        )
        .await;

        return Err(e);
    }

    log::info!(
        "Completed HLS transcoding for {source_object_key} after {}",
        format_duration(start_time.elapsed())
    );

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_video_transcode_args_and_spawn_output_reader(
    transcode_args: &mut Vec<String>,
    video_stream_count: usize,
    target_bitrate: TranscodeResolution,
    downscaled_bitrates: &[TranscodeResolution],
    fifo_dir: &tempfile::TempDir,
    bucket: &Bucket,
    file_id: &Uuid,
    has_muxed_audio: bool,
    output_reader_join_handles: &mut Vec<JoinHandle<Result<UploadedHlsStream, Error>>>,
) -> Result<(), Error> {
    for i in 0..video_stream_count {
        transcode_args.push(String::from("-map"));
        let is_target_resolution = i == 0;
        let bitrate = if is_target_resolution {
            target_bitrate
        } else {
            downscaled_bitrates[i - 1]
        };

        let preset = if is_target_resolution {
            "medium"
        } else {
            "fast"
        };

        transcode_args.push(format!("[v{}out]", i + 1));
        transcode_args.push(format!("-c:v:{i}"));
        transcode_args.push(String::from("libx264"));
        transcode_args.push(String::from("-x264-params"));
        transcode_args.push(String::from("nal-hrd=cbr:force-cfr=1"));
        transcode_args.push(format!("-b:v:{i}"));
        transcode_args.push(bitrate.target_bitrate.to_string());
        transcode_args.push(format!("-maxrate:v:{i}"));
        transcode_args.push(bitrate.max_bitrate.to_string());
        transcode_args.push(format!("-minrate:v:{i}"));
        transcode_args.push(bitrate.min_bitrate.to_string());
        transcode_args.push(format!("-bufsize:v:{i}"));
        transcode_args.push(bitrate.max_bitrate.to_string());
        transcode_args.push(String::from("-preset"));
        transcode_args.push(preset.to_string());
        transcode_args.push(format!("-profile:v:{i}"));
        transcode_args.push(String::from("high"));
        transcode_args.push(String::from("-g"));
        transcode_args.push(String::from("48"));
        transcode_args.push(String::from("-sc_threshold"));
        transcode_args.push(String::from("0"));
        transcode_args.push(String::from("-keyint_min"));
        transcode_args.push(String::from("48"));
        transcode_args.push(String::from("-movflags"));
        transcode_args.push(String::from("+faststart"));

        let output_reader_join_handle = spawn_hls_output_reader(
            #[cfg(unix)]
            fifo_dir,
            bucket.clone(),
            HlsOutputStream::Video(HlsStream {
                stream_playlist: format!("{file_id}/stream_{i}.m3u8"),
                stream_file: format!("{file_id}/stream_{i}.ts"),
                master_playlist: format!("{file_id}/master.m3u8"),
                resolution: bitrate.resolution as i32,
                x264_preset: String::from(preset),
                target_bitrate: Some(String::from(bitrate.target_bitrate)),
                min_bitrate: Some(String::from(bitrate.min_bitrate)),
                max_bitrate: Some(String::from(bitrate.max_bitrate)),
                has_muxed_audio,
            }),
        )?;

        output_reader_join_handles.push(output_reader_join_handle);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_audio_transcode_args_and_spawn_output_reader(
    transcode_args: &mut Vec<String>,
    video_stream_count: usize,
    has_muxed_audio: bool,
    has_separate_audio: bool,
    audio_streams: &[&MediaStream],
    audio_variant_offset: usize,
    fifo_dir: &tempfile::TempDir,
    bucket: &Bucket,
    file_id: &Uuid,
    output_reader_join_handles: &mut Vec<JoinHandle<Result<UploadedHlsStream, Error>>>,
) -> Result<(), Error> {
    if has_muxed_audio {
        let source_audio_stream = audio_streams[0];
        let audio_bitrate = hls_audio_bitrate(source_audio_stream.channels);

        for output_audio_index in 0..video_stream_count {
            transcode_args.push(String::from("-map"));
            transcode_args.push(format!("0:{}", source_audio_stream.index));

            transcode_args.push(format!("-c:a:{output_audio_index}"));
            transcode_args.push(String::from("aac"));

            transcode_args.push(format!("-b:a:{output_audio_index}"));
            transcode_args.push(audio_bitrate.to_owned());
        }
    } else if has_separate_audio {
        let default_audio_index = audio_streams
            .iter()
            .position(|stream| stream.disposition.default)
            .unwrap_or(0);
        for (audio_index, source_audio_stream) in audio_streams.iter().enumerate() {
            let audio_bitrate = hls_audio_bitrate(source_audio_stream.channels);

            transcode_args.push(String::from("-map"));
            transcode_args.push(format!("0:{}", source_audio_stream.index));

            transcode_args.push(format!("-c:a:{audio_index}"));
            transcode_args.push(String::from("aac"));

            transcode_args.push(format!("-b:a:{audio_index}"));
            transcode_args.push(audio_bitrate.to_owned());

            if let Some(ref language) = source_audio_stream.tags.language {
                transcode_args.push(format!("-metadata:s:a:{audio_index}"));
                transcode_args.push(format!("language={language}"));
            }

            let variant_index = audio_variant_offset + audio_index;

            let output_reader_join_handle = spawn_hls_output_reader(
                #[cfg(unix)]
                fifo_dir,
                bucket.clone(),
                HlsOutputStream::Audio(HlsAudioStream {
                    stream_playlist: format!("{file_id}/stream_{variant_index}.m3u8"),
                    stream_file: format!("{file_id}/stream_{variant_index}.ts"),
                    master_playlist: format!("{file_id}/master.m3u8"),
                    source_stream_index: source_audio_stream.index,
                    language: source_audio_stream.tags.language.clone(),
                    title: source_audio_stream.tags.title.clone(),
                    is_default: audio_index == default_audio_index,
                    autoselect: true,
                    source_codec: source_audio_stream.codec_name.clone(),
                    codec: String::from("aac"),
                    bitrate: audio_bitrate.to_owned(),
                    channels: source_audio_stream.channels,
                }),
            )?;

            output_reader_join_handles.push(output_reader_join_handle);
        }
    }

    Ok(())
}

#[cfg(unix)]
async fn generate_hls_subtitle_outputs(
    object_url: &str,
    subtitle_streams: &[&MediaStream],
    subtitle_variant_offset: usize,
    duration_sec: f32,
    fifo_dir: &tempfile::TempDir,
    bucket: &Bucket,
    file_id: &Uuid,
) -> Result<Vec<UploadedHlsStream>, Error> {
    use futures::future::try_join_all;
    use m3u8_rs::{MediaPlaylist, MediaPlaylistType, MediaSegment};

    if subtitle_streams.is_empty() {
        return Ok(Vec::new());
    }

    let video_stream_path = format!("{file_id}/stream_0.ts");
    let mpegts_timestamp = match async {
        let video_stream_url = bucket.presign_get(&video_stream_path, 300, None).await?;

        let start_time_sec = probe::get_object_start_time(&video_stream_url).await?;

        Ok::<_, Error>((start_time_sec * 90_000.0).round() as u64)
    }
    .await
    {
        Ok(mpegts_timestamp) => Some(mpegts_timestamp),
        Err(error) => {
            log::warn!(
                "Failed to determine MPEG-TS start time for HLS subtitles, subtitles will be uploaded without X-TIMESTAMP-MAP: {error}"
            );

            None
        }
    };

    let default_subtitle_index = subtitle_streams
        .iter()
        .position(|stream| stream.disposition.default);

    let mut transcode_args = vec![
        String::from("-y"),
        String::from("-nostdin"),
        String::from("-v"),
        String::from("error"),
        String::from("-i"),
        object_url.to_owned(),
    ];

    let mut output_reader_join_handles = Vec::with_capacity(subtitle_streams.len());

    for (subtitle_index, source_subtitle_stream) in subtitle_streams.iter().enumerate() {
        let variant_index = subtitle_variant_offset + subtitle_index;

        let stream_file = format!("{file_id}/stream_{variant_index}.vtt");

        let stream_playlist = format!("{file_id}/stream_{variant_index}.m3u8");

        let subtitle_playlist = MediaPlaylist {
            version: Some(3),
            target_duration: duration_sec.ceil() as u64,
            media_sequence: 0,
            segments: vec![MediaSegment {
                uri: format!("stream_{variant_index}.vtt"),
                duration: duration_sec,
                title: None,
                ..Default::default()
            }],
            end_list: true,
            playlist_type: Some(MediaPlaylistType::Vod),
            ..Default::default()
        };

        let hls_subtitle_stream = HlsSubtitleStream {
            stream_playlist,
            stream_file,
            master_playlist: format!("{file_id}/master.m3u8"),
            source_stream_index: source_subtitle_stream.index,
            language: source_subtitle_stream.tags.language.clone(),
            title: source_subtitle_stream.tags.title.clone(),
            is_default: Some(subtitle_index) == default_subtitle_index,
            autoselect: true,
            forced: source_subtitle_stream.disposition.forced,
            source_codec: source_subtitle_stream.codec_name.clone(),
            codec: String::from("webvtt"),
        };

        /*
         * Creates the VTT FIFO, starts its streaming S3 upload,
         * and prepares/uploads the generated subtitle playlist.
         */
        let (subtitle_fifo, output_reader_join_handle) = spawn_hls_subtitle_output_reader(
            fifo_dir,
            bucket.clone(),
            hls_subtitle_stream,
            subtitle_playlist,
            mpegts_timestamp,
        )?;

        output_reader_join_handles.push(output_reader_join_handle);

        transcode_args.push(String::from("-map"));
        transcode_args.push(format!("0:{}", source_subtitle_stream.index));

        transcode_args.push(String::from("-c:s"));
        transcode_args.push(String::from("webvtt"));

        transcode_args.push(String::from("-f"));
        transcode_args.push(String::from("webvtt"));

        transcode_args.push(subtitle_fifo.to_string_lossy().into_owned());
    }

    log::debug!("Spawning HLS subtitle ffmpeg command with args {transcode_args:?}");

    let process = match Command::new("ffmpeg")
        .args(transcode_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            Error::FfmpegProcessError(format!("Failed to spawn HLS subtitle ffmpeg process: {e}"))
        }) {
        Ok(process) => process,
        Err(e) => {
            for handle in &output_reader_join_handles {
                handle.abort();
            }

            for handle in output_reader_join_handles {
                let _ = handle.await;
            }

            return Err(e);
        }
    };

    let process_output = spawn_encode_task_blocking(|| {
        process.wait_with_output().map_err(|e| {
            Error::FfmpegProcessError(format!(
                "Failed to get HLS subtitle ffmpeg process output: {e}"
            ))
        })
    })
    .await;

    let process_output = match process_output {
        Ok(process_output) if !process_output.status.success() => {
            for handle in &output_reader_join_handles {
                handle.abort();
            }

            for handle in output_reader_join_handles {
                let _ = handle.await;
            }

            let error_msg = String::from_utf8_lossy(&process_output.stderr);

            return Err(Error::FfmpegProcessError(format!(
                "ffmpeg for HLS subtitle transcoding failed with status {}: {}",
                process_output.status, error_msg,
            )));
        }
        Err(e) => {
            for handle in &output_reader_join_handles {
                handle.abort();
            }

            for handle in output_reader_join_handles {
                let _ = handle.await;
            }

            return Err(e);
        }
        Ok(process_output) => process_output,
    };

    if !process_output.stderr.is_empty() {
        log::warn!(
            "ffmpeg reported errors while generating HLS subtitles: {}",
            String::from_utf8_lossy(&process_output.stderr),
        );
    }

    let subtitle_results = try_join_all(output_reader_join_handles)
        .await
        .map_err(|_| Error::CancellationError)?;

    subtitle_results.into_iter().collect()
}

fn build_var_stream_map(
    video_stream_count: usize,
    has_muxed_audio: bool,
    has_separate_audio: bool,
    audio_streams: &[&MediaStream],
) -> String {
    let mut variant_stream_map = Vec::new();

    for video_index in 0..video_stream_count {
        let mut entry = format!("v:{video_index}");

        if has_muxed_audio {
            entry.push_str(&format!(",a:{video_index}"));
        } else if has_separate_audio {
            entry.push_str(",agroup:audio");
        }

        variant_stream_map.push(entry);
    }

    if has_separate_audio {
        let default_audio_index = audio_streams
            .iter()
            .position(|stream| stream.disposition.default)
            .unwrap_or(0);

        for (audio_index, source_audio_stream) in audio_streams.iter().enumerate() {
            let mut entry = format!(
                "a:{audio_index},agroup:audio,default:{}",
                if audio_index == default_audio_index {
                    "yes"
                } else {
                    "no"
                },
            );

            if let Some(language) = source_audio_stream.tags.language.as_deref() {
                entry.push_str(&format!(",language:{language}"));
            }

            variant_stream_map.push(entry);
        }
    }

    variant_stream_map.join(" ")
}

fn finalize_hls_master_playlist(
    master_playlist_bytes: &[u8],
    video_stream_count: usize,
    stream_results: &[UploadedHlsStream],
    subtitle_results: &[UploadedHlsStream],
) -> Result<Vec<u8>, Error> {
    use m3u8_rs::{AlternativeMedia, AlternativeMediaType};

    const SUBTITLE_GROUP_ID: &str = "subtitles";

    let has_separate_audio = stream_results
        .iter()
        .any(|result| matches!(&result.hls_stream, HlsOutputStream::Audio(_)));

    if !has_separate_audio && subtitle_results.is_empty() {
        return Ok(master_playlist_bytes.to_vec());
    }

    let (_, mut master_playlist) =
        m3u8_rs::parse_master_playlist(master_playlist_bytes).map_err(|e| {
            Error::IoError(format!(
                "Failed to parse generated HLS master playlist: {e:?}"
            ))
        })?;

    for (audio_index, audio_stream) in stream_results
        .iter()
        .filter_map(|result| match &result.hls_stream {
            HlsOutputStream::Audio(stream) => Some(stream),
            _ => None,
        })
        .enumerate()
    {
        let playlist_uri = audio_stream
            .stream_playlist
            .rsplit('/')
            .next()
            .ok_or_else(|| {
                Error::IoError(format!(
                    "Invalid HLS audio playlist path '{}'",
                    audio_stream.stream_playlist,
                ))
            })?;

        let alternative = master_playlist
            .alternatives
            .iter_mut()
            .find(|alternative| {
                matches!(&alternative.media_type, AlternativeMediaType::Audio)
                    && alternative.uri.as_deref() == Some(playlist_uri)
            })
            .ok_or_else(|| {
                Error::IoError(format!(
                    "Generated HLS master playlist contains no audio rendition for '{playlist_uri}'"
                ))
            })?;

        alternative.name = hls_track_name(
            audio_stream.title.as_deref(),
            audio_stream.language.as_deref(),
            audio_index,
        );
    }

    /*
     * build_var_stream_map emits video variants first, followed by
     * any separate audio-only variants. Only video variants should
     * reference the subtitle group.
     */
    if !subtitle_results.is_empty() {
        for variant in master_playlist.variants.iter_mut().take(video_stream_count) {
            variant.subtitles = Some(SUBTITLE_GROUP_ID.to_owned());
        }
    }

    for (subtitle_index, subtitle_result) in subtitle_results.iter().enumerate() {
        let HlsOutputStream::Subtitle(subtitle_stream) = &subtitle_result.hls_stream else {
            return Err(Error::IoError(String::from(
                "Expected an HLS subtitle stream result",
            )));
        };

        let playlist_uri = subtitle_stream
            .stream_playlist
            .rsplit('/')
            .next()
            .ok_or_else(|| {
                Error::IoError(format!(
                    "Invalid HLS subtitle playlist path '{}'",
                    subtitle_stream.stream_playlist,
                ))
            })?
            .to_owned();

        let title = subtitle_stream
            .title
            .as_deref()
            .filter(|title| !title.is_empty());

        let language = subtitle_stream
            .language
            .as_deref()
            .filter(|language| !language.is_empty());

        /*
         * Mirrors VLC's track-title fallback without translating or
         * otherwise modifying the source language metadata.
         */
        let name = match (title, language) {
            (Some(title), Some(language)) => {
                format!("{title} - [{language}]")
            }
            (Some(title), None) => title.to_owned(),
            (None, Some(language)) => {
                format!("Track {subtitle_index} - [{language}]")
            }
            (None, None) => {
                format!("Track {subtitle_index}")
            }
        };

        master_playlist.alternatives.push(AlternativeMedia {
            media_type: AlternativeMediaType::Subtitles,
            uri: Some(playlist_uri),
            group_id: SUBTITLE_GROUP_ID.to_owned(),
            language: subtitle_stream.language.clone(),
            assoc_language: None,
            name,
            default: subtitle_stream.is_default,
            autoselect: subtitle_stream.autoselect,
            forced: subtitle_stream.forced,
            instream_id: None,
            characteristics: None,
            channels: None,
            other_attributes: None,
        });
    }

    let mut modified_master_playlist = Vec::new();

    master_playlist
        .write_to(&mut modified_master_playlist)
        .map_err(|e| {
            Error::IoError(format!(
                "Failed to serialize modified HLS master playlist: {e}"
            ))
        })?;

    Ok(modified_master_playlist)
}

fn hls_track_name(title: Option<&str>, language: Option<&str>, track_index: usize) -> String {
    let title = title.filter(|title| !title.is_empty());
    let language = language.filter(|language| !language.is_empty());

    match (title, language) {
        (Some(title), Some(language)) => {
            format!("{title} - [{language}]")
        }
        (Some(title), None) => title.to_owned(),
        (None, Some(language)) => {
            format!("Track {track_index} - [{language}]")
        }
        (None, None) => format!("Track {track_index}"),
    }
}

async fn await_and_verify_results(
    master_playlist_join_handle: JoinHandle<Result<Vec<u8>, Error>>,
    output_reader_join_handles: Vec<JoinHandle<Result<UploadedHlsStream, Error>>>,
    ffmpeg_stderr: Vec<u8>,
    source_object_key: &str,
    source_duration_sec: f32,
    bucket: &Bucket,
) -> Result<(Vec<u8>, Vec<UploadedHlsStream>), Error> {
    let (master_playlist_result, stream_results_result) = tokio::join!(
        async {
            master_playlist_join_handle
                .await
                .map_err(|_| Error::CancellationError)?
        },
        async {
            try_join_all(output_reader_join_handles)
                .await
                .map_err(|_| Error::CancellationError)?
                .into_iter()
                .collect::<Result<Vec<_>, Error>>()
        },
    );

    let master_playlist_bytes = master_playlist_result?;
    let stream_results = stream_results_result?;

    if !ffmpeg_stderr.is_empty() {
        let error_msg = String::from_utf8_lossy(&ffmpeg_stderr);
        log::warn!(
            "ffmpeg reported error during HLS transcoding of object '{source_object_key}', going to check if output video duration matches input: {error_msg}"
        );

        let video_stream = stream_results
            .iter()
            .find_map(|result| match &result.hls_stream {
                HlsOutputStream::Video(stream) => Some(stream),
                _ => None,
            })
            .ok_or_else(|| {
                Error::FfmpegProcessError(format!(
                    "HLS transcoding of '{source_object_key}' produced no video stream"
                ))
            })?;

        let hls_url = bucket
            .presign_get(&video_stream.stream_file, 300, None)
            .await?;

        let hls_duration = probe::get_object_duration(&hls_url).await?;

        if (source_duration_sec - hls_duration.duration_sec).abs() > 1.0 {
            return Err(Error::FfmpegProcessError(format!(
                "HLS video duration mismatch for object '{source_object_key}', expected {} but got {}. Reported error: {error_msg}",
                source_duration_sec, hls_duration.duration_str
            )));
        }
    }

    Ok((master_playlist_bytes, stream_results))
}

async fn persist_hls_transcode_results(
    source_object_key: &str,
    broker_pk: i64,
    user_pk: i64,
    master_playlist_result: S3UploadResult,
    stream_results: Vec<UploadedHlsStream>,
) -> Result<(), Error> {
    let mut s3_objects = vec![S3Object {
        object_key: master_playlist_result.path.clone(),
        sha256_hash: None,
        size_bytes: master_playlist_result.bytes_read as i64,
        mime_type: String::from("application/vnd.apple.mpegurl"),
        fk_broker: broker_pk,
        fk_uploader: user_pk,
        thumbnail_object_key: None,
        creation_timestamp: Utc::now(),
        filename: None,
        hls_master_playlist: Some(master_playlist_result.path.clone()),
        hls_disabled: true,
        hls_locked_at: None,
        thumbnail_locked_at: None,
        hls_fail_count: None,
        thumbnail_fail_count: None,
        thumbnail_disabled: true,
        metadata_locked_at: None,
        metadata_fail_count: None,
        derived_from: Some(source_object_key.to_string()),
        object_type: ObjectType::HlsPlaylist,
    }];

    let mut hls_streams = Vec::new();
    let mut hls_audio_streams = Vec::new();
    let mut hls_subtitle_streams = Vec::new();
    for stream_result in stream_results {
        let stream_mime_type = stream_result.hls_stream.stream_mime_type().to_owned();

        match stream_result.hls_stream {
            HlsOutputStream::Video(hls_stream) => hls_streams.push(hls_stream),
            HlsOutputStream::Audio(hls_audio_stream) => hls_audio_streams.push(hls_audio_stream),
            HlsOutputStream::Subtitle(hls_subtitle_stream) => {
                hls_subtitle_streams.push(hls_subtitle_stream)
            }
        }

        s3_objects.push(S3Object {
            object_key: stream_result.playlist_upload_result.path,
            sha256_hash: None,
            size_bytes: stream_result.playlist_upload_result.bytes_read as i64,
            mime_type: String::from("application/vnd.apple.mpegurl"),
            fk_broker: broker_pk,
            fk_uploader: user_pk,
            thumbnail_object_key: None,
            creation_timestamp: Utc::now(),
            filename: None,
            hls_master_playlist: Some(master_playlist_result.path.clone()),
            hls_disabled: true,
            hls_locked_at: None,
            thumbnail_locked_at: None,
            hls_fail_count: None,
            thumbnail_fail_count: None,
            thumbnail_disabled: true,
            metadata_locked_at: None,
            metadata_fail_count: None,
            derived_from: Some(source_object_key.to_string()),
            object_type: ObjectType::HlsPlaylist,
        });

        s3_objects.push(S3Object {
            object_key: stream_result.stream_upload_result.path,
            sha256_hash: None,
            size_bytes: stream_result.stream_upload_result.bytes_read as i64,
            mime_type: stream_mime_type,
            fk_broker: broker_pk,
            fk_uploader: user_pk,
            thumbnail_object_key: None,
            creation_timestamp: Utc::now(),
            filename: None,
            hls_master_playlist: Some(master_playlist_result.path.clone()),
            hls_disabled: true,
            hls_locked_at: None,
            thumbnail_locked_at: None,
            hls_fail_count: None,
            thumbnail_fail_count: None,
            thumbnail_disabled: true,
            metadata_locked_at: None,
            metadata_fail_count: None,
            derived_from: Some(source_object_key.to_string()),
            object_type: ObjectType::HlsSegment,
        });
    }

    let mut conn = acquire_db_connection().await?;
    run_serializable_transaction(&mut conn, async |conn| {
        diesel::insert_into(s3_object::table)
            .values(&s3_objects)
            .execute(conn)
            .await?;
        if !hls_streams.is_empty() {
            diesel::insert_into(hls_stream::table)
                .values(&hls_streams)
                .execute(conn)
                .await?;
        }
        if !hls_audio_streams.is_empty() {
            diesel::insert_into(hls_audio_stream::table)
                .values(&hls_audio_streams)
                .execute(conn)
                .await?;
        }
        if !hls_subtitle_streams.is_empty() {
            diesel::insert_into(hls_subtitle_stream::table)
                .values(&hls_subtitle_streams)
                .execute(conn)
                .await?;
        }

        let update_count = diesel::update(s3_object::table)
            .set(s3_object::hls_master_playlist.eq(&master_playlist_result.path))
            .filter(s3_object::object_key.eq(source_object_key))
            .execute(conn)
            .await?;

        if update_count == 0 {
            // source object no longer exists, delete HLS transcode
            return Err(TransactionRuntimeError::Rollback(Error::QueryError(
                format!("Source object {source_object_key} for HLS transcoding no longer exists"),
            )));
        }

        Ok(())
    })
    .await?;

    Ok(())
}

async fn delete_hls_transcode_objects(
    bucket: &Bucket,
    file_id: &Uuid,
    video_stream_count: usize,
    audio_stream_count: usize,
    subtitle_stream_count: usize,
) {
    async fn delete_object(bucket: &Bucket, object_key: String) {
        // Best-effort cleanup. The object may never have been created.
        let _ = bucket.delete_object(&object_key).await;
    }

    delete_object(bucket, format!("{file_id}/master.m3u8")).await;

    for video_index in 0..video_stream_count {
        delete_object(bucket, format!("{file_id}/stream_{video_index}.m3u8")).await;
        delete_object(bucket, format!("{file_id}/stream_{video_index}.ts")).await;
    }

    let audio_variant_offset = video_stream_count;

    for audio_index in 0..audio_stream_count {
        let variant_index = audio_variant_offset + audio_index;

        delete_object(bucket, format!("{file_id}/stream_{variant_index}.m3u8")).await;
        delete_object(bucket, format!("{file_id}/stream_{variant_index}.ts")).await;
    }

    let subtitle_variant_offset = audio_variant_offset + audio_stream_count;

    for subtitle_index in 0..subtitle_stream_count {
        let variant_index = subtitle_variant_offset + subtitle_index;

        delete_object(bucket, format!("{file_id}/stream_{variant_index}.m3u8")).await;
        delete_object(bucket, format!("{file_id}/stream_{variant_index}.vtt")).await;
    }
}

pub fn is_hls_supported_on_current_platform() -> bool {
    cfg!(unix)
}

#[cfg(unix)]
fn spawn_hls_output_reader(
    fifo_dir: &tempfile::TempDir,
    bucket: Bucket,
    hls_stream: HlsOutputStream,
) -> Result<JoinHandle<Result<UploadedHlsStream, Error>>, Error> {
    use futures::try_join;

    let hls_stream_pipe = fifo_dir
        .path()
        .join(hls_stream.stream_file().split('/').next_back().unwrap());
    nix::unistd::mkfifo(&hls_stream_pipe, nix::sys::stat::Mode::S_IRWXU)
        .map_err(|e| Error::IoError(format!("Failed mkfifo: {e}")))?;
    let hls_playlist_pipe = fifo_dir
        .path()
        .join(hls_stream.stream_playlist().split('/').next_back().unwrap());
    nix::unistd::mkfifo(&hls_playlist_pipe, nix::sys::stat::Mode::S_IRWXU)
        .map_err(|e| Error::IoError(format!("Failed mkfifo: {e}")))?;

    let join_handle = tokio::spawn(async move {
        let stream_file_target_path = hls_stream.stream_file().to_owned();
        let hls_stream_upload = upload_tokio_file(
            bucket.clone(),
            &hls_stream_pipe,
            stream_file_target_path,
            hls_stream.stream_mime_type().to_owned(),
        );

        let playlist_file_target_path = hls_stream.stream_playlist().to_owned();
        let hls_playlist_upload = upload_tokio_file(
            bucket,
            &hls_playlist_pipe,
            playlist_file_target_path,
            String::from("application/vnd.apple.mpegurl"),
        );

        let (stream_upload_result, playlist_upload_result) =
            try_join!(hls_stream_upload, hls_playlist_upload)?;

        if stream_upload_result.response_status >= 300 {
            return Err(Error::S3ResponseError(stream_upload_result.response_status));
        }
        if playlist_upload_result.response_status >= 300 {
            return Err(Error::S3ResponseError(
                playlist_upload_result.response_status,
            ));
        }

        Ok(UploadedHlsStream {
            playlist_upload_result,
            stream_upload_result,
            hls_stream,
        })
    });
    Ok(join_handle)
}

#[cfg(windows)]
fn spawn_hls_output_reader(
    _bucket: Bucket,
    _hls_stream: HlsStream,
) -> Result<JoinHandle<Result<UploadedHlsStream, Error>>, Error> {
    // TODO implement named pipes on windows
    Err(Error::FfmpegProcessError(String::from(
        "HLS transcoding not supported on current platform",
    )))
}

#[cfg(not(any(unix, windows)))]
fn spawn_hls_output_reader(
    _bucket: Bucket,
    _hls_stream: HlsStream,
) -> Result<JoinHandle<Result<UploadedHlsStream, Error>>, Error> {
    Err(Error::FfmpegProcessError(String::from(
        "HLS transcoding not supported on current platform",
    )))
}

#[cfg(unix)]
fn spawn_hls_master_playlist_reader(
    fifo_dir: &tempfile::TempDir,
) -> Result<JoinHandle<Result<Vec<u8>, Error>>, Error> {
    use tokio::io::AsyncReadExt;

    let master_playlist_pipe = fifo_dir.path().join("master.m3u8");

    nix::unistd::mkfifo(&master_playlist_pipe, nix::sys::stat::Mode::S_IRWXU)
        .map_err(|e| Error::IoError(format!("Failed mkfifo: {e}")))?;

    Ok(tokio::spawn(async move {
        let mut file = tokio::fs::File::open(&master_playlist_pipe)
            .await
            .map_err(|e| Error::IoError(format!("Failed to open HLS master playlist FIFO: {e}")))?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .await
            .map_err(|e| Error::IoError(format!("Failed to read HLS master playlist FIFO: {e}")))?;

        Ok(bytes)
    }))
}

#[cfg(windows)]
fn spawn_hls_master_playlist_reader(
    _bucket: Bucket,
    _master_playlist_path: String,
) -> Result<JoinHandle<Result<S3UploadResult, Error>>, Error> {
    // TODO implement named pipes on windows
    Err(Error::FfmpegProcessError(String::from(
        "HLS transcoding not supported on current platform",
    )))
}

#[cfg(not(any(unix, windows)))]
fn spawn_hls_master_playlist_reader(
    _bucket: Bucket,
    _master_playlist_path: String,
) -> Result<JoinHandle<Result<S3UploadResult, Error>>, Error> {
    Err(Error::FfmpegProcessError(String::from(
        "HLS transcoding not supported on current platform",
    )))
}

async fn upload_hls_master_playlist(
    bucket: &Bucket,
    file_id: &Uuid,
    playlist_bytes: &[u8],
) -> Result<S3UploadResult, Error> {
    let path = format!("{file_id}/master.m3u8");

    let response = bucket
        .put_object_with_content_type(&path, playlist_bytes, "application/vnd.apple.mpegurl")
        .await?;

    let response_status = response.status_code();

    if response_status >= 300 {
        return Err(Error::S3ResponseError(response_status));
    }

    Ok(S3UploadResult {
        path,
        bytes_read: playlist_bytes.len(),
        response_status,
    })
}

#[cfg(unix)]
fn spawn_hls_subtitle_output_reader(
    fifo_dir: &tempfile::TempDir,
    bucket: Bucket,
    hls_subtitle_stream: HlsSubtitleStream,
    subtitle_playlist: m3u8_rs::MediaPlaylist,
    mpegts_timestamp: Option<u64>,
) -> Result<
    (
        std::path::PathBuf,
        JoinHandle<Result<UploadedHlsStream, Error>>,
    ),
    Error,
> {
    use futures::try_join;

    let stream_file_target_path = hls_subtitle_stream.stream_file.clone();
    let playlist_file_target_path = hls_subtitle_stream.stream_playlist.clone();

    let subtitle_pipe = fifo_dir
        .path()
        .join(stream_file_target_path.split('/').next_back().unwrap());

    nix::unistd::mkfifo(&subtitle_pipe, nix::sys::stat::Mode::S_IRWXU)
        .map_err(|e| Error::IoError(format!("Failed to create HLS subtitle FIFO: {e}")))?;

    let mut playlist_bytes = Vec::new();
    subtitle_playlist
        .write_to(&mut playlist_bytes)
        .map_err(|e| Error::IoError(format!("Failed to serialise HLS subtitle playlist: {e}")))?;

    let ffmpeg_output_path = subtitle_pipe.clone();

    let join_handle = tokio::spawn(async move {
        let subtitle_upload = async {
            match mpegts_timestamp {
                Some(mpegts_timestamp) => {
                    upload_timestamp_mapped_webvtt(
                        bucket.clone(),
                        &subtitle_pipe,
                        stream_file_target_path,
                        mpegts_timestamp,
                    )
                    .await
                }
                None => {
                    upload_tokio_file(
                        bucket.clone(),
                        &subtitle_pipe,
                        stream_file_target_path,
                        String::from("text/vtt"),
                    )
                    .await
                }
            }
        };

        let playlist_upload = async {
            let response = bucket
                .put_object_with_content_type(
                    &playlist_file_target_path,
                    &playlist_bytes,
                    "application/vnd.apple.mpegurl",
                )
                .await
                .map_err(|e| {
                    Error::S3Error(format!(
                        "Failed to upload generated HLS subtitle playlist '{playlist_file_target_path}': {e}",
                    ))
                })?;

            Ok::<_, Error>(S3UploadResult {
                path: playlist_file_target_path,
                bytes_read: playlist_bytes.len(),
                response_status: response.status_code(),
            })
        };

        let (stream_upload_result, playlist_upload_result) =
            try_join!(subtitle_upload, playlist_upload)?;

        if stream_upload_result.response_status >= 300 {
            return Err(Error::S3ResponseError(stream_upload_result.response_status));
        }

        if playlist_upload_result.response_status >= 300 {
            return Err(Error::S3ResponseError(
                playlist_upload_result.response_status,
            ));
        }

        Ok(UploadedHlsStream {
            playlist_upload_result,
            stream_upload_result,
            hls_stream: HlsOutputStream::Subtitle(hls_subtitle_stream),
        })
    });

    Ok((ffmpeg_output_path, join_handle))
}

#[cfg(unix)]
fn upload_tokio_file(
    bucket: Bucket,
    file_path: impl AsRef<std::path::Path>,
    s3_path: String,
    content_type: String,
) -> impl futures::Future<Output = Result<S3UploadResult, Error>> {
    use futures::TryFutureExt;
    tokio::fs::File::open(file_path)
        .map_err(|e| Error::IoError(format!("Failed to open pipe file: {e}")))
        .and_then(|f| async move {
            let mut reader = output::ByteCountingReader::new(f);
            log::debug!("Beginning upload for HLS stream for file {s3_path}");
            let res = bucket
                .put_object_stream_builder(&s3_path)
                .with_content_type(&content_type)
                .with_max_concurrent_chunks(1)
                .execute_stream(&mut reader)
                .map_err(|e| Error::S3Error(format!("Failed to upload file '{s3_path}': {e}")))
                .await?;

            Ok(S3UploadResult {
                path: s3_path,
                bytes_read: reader.byte_count,
                response_status: res.status_code(),
            })
        })
}

#[cfg(unix)]
async fn upload_timestamp_mapped_webvtt(
    bucket: Bucket,
    file_path: impl AsRef<std::path::Path>,
    s3_path: String,
    mpegts_timestamp: u64,
) -> Result<S3UploadResult, Error> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

    let file = tokio::fs::File::open(file_path)
        .await
        .map_err(|e| Error::IoError(format!("Failed to open WebVTT pipe file: {e}")))?;

    let mut source_reader = BufReader::new(file);
    let mut first_line = String::new();

    let bytes_read = source_reader
        .read_line(&mut first_line)
        .await
        .map_err(|e| Error::IoError(format!("Failed to read WebVTT header: {e}")))?;

    let header_name = first_line
        .trim_end_matches(&['\r', '\n'][..])
        .trim_start_matches('\u{feff}');

    if bytes_read == 0 || header_name != "WEBVTT" {
        return Err(Error::FfmpegProcessError(format!(
            "Expected FFmpeg WebVTT output to start with 'WEBVTT', received '{header_name}'"
        )));
    }

    let line_ending = if first_line.ends_with("\r\n") {
        "\r\n"
    } else {
        "\n"
    };

    /*
     * The original blank line remains unread in source_reader.
     * Therefore, this header deliberately ends with only one line
     * ending rather than two.
     */
    let replacement_header = format!(
        "WEBVTT{line_ending}\
         X-TIMESTAMP-MAP=LOCAL:00:00:00.000,\
         MPEGTS:{mpegts_timestamp}{line_ending}"
    );

    let header_reader = std::io::Cursor::new(replacement_header.into_bytes());

    let combined_reader = header_reader.chain(source_reader);
    let mut counting_reader = output::ByteCountingReader::new(combined_reader);

    log::debug!("Beginning upload for timestamp-mapped HLS subtitle file {s3_path}");

    let response = bucket
        .put_object_stream_builder(&s3_path)
        .with_content_type("text/vtt")
        .with_max_concurrent_chunks(1)
        .execute_stream(&mut counting_reader)
        .await
        .map_err(|e| Error::S3Error(format!("Failed to upload file '{s3_path}': {e}")))?;

    Ok(S3UploadResult {
        path: s3_path,
        bytes_read: counting_reader.byte_count,
        response_status: response.status_code(),
    })
}

struct SubmittedHlsTranscodingSentinel<'a> {
    object_key: &'a str,
}

impl<'a> SubmittedHlsTranscodingSentinel<'a> {
    fn new(object_key: &'a str) -> Self {
        let mut submitted_hls_transcodings = SUBMITTED_HLS_TRANSCODINGS.lock();
        submitted_hls_transcodings.insert(String::from(object_key));
        SubmittedHlsTranscodingSentinel { object_key }
    }
}

impl Drop for SubmittedHlsTranscodingSentinel<'_> {
    fn drop(&mut self) {
        let mut submitted_hls_transcodings = SUBMITTED_HLS_TRANSCODINGS.lock();
        submitted_hls_transcodings.remove(self.object_key);
    }
}
