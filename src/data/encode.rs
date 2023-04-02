use std::cmp::Reverse;

use chrono::Utc;
use futures::{future::try_join_all, io::BufReader, ready, AsyncReadExt, Future, TryFutureExt};
use itertools::Itertools;
use pin_project_lite::pin_project;
use rusty_pool::ThreadPool;
use s3::Bucket;
use tokio::{sync::Semaphore, task::JoinHandle};
use uuid::Uuid;

use lazy_static::lazy_static;

use crate::{
    acquire_db_connection,
    diesel::{ExpressionMethods, RunQueryDsl},
    error::Error,
    model::{Broker, HlsStream, S3Object, User},
    schema::{hls_stream, s3_object},
    util::format_duration,
};

const CONCURRENT_VIDEO_TRANSCODE_LIMIT: usize = 4;
static VIDEO_TRANSCODE_SEMAPHORE: Semaphore =
    Semaphore::const_new(CONCURRENT_VIDEO_TRANSCODE_LIMIT);
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

#[derive(Debug, Clone, Copy)]
struct TranscodeResolution {
    resolution: usize,
    target_bitrate: &'static str,
    min_bitrate: &'static str,
    max_bitrate: &'static str,
    downscale_target: bool,
}

lazy_static! {
    pub static ref ENCODE_POOL: ThreadPool = rusty_pool::Builder::new()
        .name(String::from("encode_pool"))
        .build();
}

async fn spawn_blocking<R: Send + 'static>(
    future: impl Future<Output = Result<R, Error>> + 'static + Send,
) -> Result<R, Error> {
    let join_handle = ENCODE_POOL.complete(future);

    match join_handle.receiver.await {
        Ok(t) => t,
        Err(_) => Err(Error::CancellationError),
    }
}

async fn get_video_resolution(presigned_get_object: &str) -> Result<String, Error> {
    let mut resolution_probe_process = async_process::Command::new("ffprobe")
        .args([
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=height",
            "-of",
            "csv=s=x:p=0",
            "-v",
            "error",
            presigned_get_object,
        ])
        .stdout(async_process::Stdio::piped())
        .spawn()
        .map_err(|e| Error::FfmpegProcessError(e.to_string()))?;

    let mut resolution_probe_stdout =
        BufReader::new(resolution_probe_process.stdout.take().ok_or_else(|| {
            Error::FfmpegProcessError(String::from("Could not get stdout of process"))
        })?);

    spawn_blocking(async move {
        let mut resolution_string = String::new();
        resolution_probe_stdout
            .read_to_string(&mut resolution_string)
            .map_err(|e| Error::FfmpegProcessError(e.to_string()))
            .await?;
        Ok(resolution_string)
    })
    .await
}

async fn video_has_audio(presigned_get_object: &str) -> Result<bool, Error> {
    video_has_stream("a", presigned_get_object).await
}

/*
Generating HLS playlists with subtitles is broken: https://trac.ffmpeg.org/ticket/9719#no1
async fn video_has_subtitles(presigned_get_object: &str) -> Result<bool, Error> {
    video_has_stream("s", presigned_get_object).await
}
*/

async fn video_has_stream(stream: &str, presigned_get_object: &str) -> Result<bool, Error> {
    let mut audio_probe_process = async_process::Command::new("ffprobe")
        .args([
            "-show_streams",
            "-select_streams",
            stream,
            "-v",
            "error",
            "-i",
            presigned_get_object,
        ])
        .stdout(async_process::Stdio::piped())
        .spawn()
        .map_err(|e| Error::FfmpegProcessError(e.to_string()))?;

    let mut audio_probe_stdout =
        BufReader::new(audio_probe_process.stdout.take().ok_or_else(|| {
            Error::FfmpegProcessError(String::from("Could not get stdout of process"))
        })?);

    spawn_blocking(async move {
        let mut str = String::new();
        audio_probe_stdout
            .read_to_string(&mut str)
            .map_err(|e| Error::FfmpegProcessError(e.to_string()))
            .await?;
        Ok(!str.trim().is_empty())
    })
    .await
}

pub async fn generate_hls_playlist(
    bucket: Bucket,
    source_object_key: String,
    file_id: Uuid,
    broker: Broker,
    user: User,
) -> Result<(), Error> {
    log::debug!(
        "Waiting to acquire permit to start HLS transcode for {}",
        &source_object_key
    );
    let _semaphore = VIDEO_TRANSCODE_SEMAPHORE
        .acquire()
        .await
        .map_err(|_| Error::CancellationError)?;
    log::info!("Starting HLS transcode for {}", &source_object_key);
    let start_time = std::time::Instant::now();
    let presigned_get_object = bucket.presign_get(&source_object_key, 3600, None)?;

    let resolution_string = get_video_resolution(&presigned_get_object).await?;
    let has_audio = video_has_audio(&presigned_get_object).await?;

    let resolution = resolution_string.trim().parse::<usize>().map_err(|_| {
        Error::FfmpegProcessError(format!(
            "Invalid resolution from ffprobe for '{}': {}",
            &source_object_key, &resolution_string
        ))
    })?;

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

    // generate string that splits the input video into separate streams for the source resolution and the two downscaled resolutions
    // e.g. [0:v]split=3[v1][v2][v3]; [v1]copy[v1out]; [v2]scale=w=1280:h=720[v2out]; [v3]scale=w=640:h=360[v3out]
    let mut split_string = String::from("[0:v]split=");
    split_string.push_str(&(downscaled_bitrates.len() + 1).to_string());
    split_string.push_str(
        &(0..=downscaled_bitrates.len())
            .map(|idx| format!("[v{}]", idx + 1))
            .collect::<String>(),
    );
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
        presigned_get_object,
        String::from("-v"),
        String::from("error"),
        String::from("-filter_complex"),
        split_string,
    ];

    let mut output_reader_join_handles = Vec::new();

    #[cfg(unix)]
    let fifo_dir = tempfile::tempdir().map_err(|e| Error::IoError(e.to_string()))?;

    for i in 0..=downscaled_bitrates.len() {
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
            &fifo_dir,
            bucket.clone(),
            HlsStream {
                stream_playlist: format!("{}/stream_{i}.m3u8", &file_id),
                stream_file: format!("{}/stream_{i}.ts", &file_id),
                master_playlist: format!("{}/master.m3u8", &file_id),
                resolution: bitrate.resolution as i32,
                x264_preset: String::from(preset),
                target_bitrate: Some(String::from(bitrate.target_bitrate)),
                min_bitrate: Some(String::from(bitrate.min_bitrate)),
                max_bitrate: Some(String::from(bitrate.max_bitrate)),
            },
        )?;

        output_reader_join_handles.push(output_reader_join_handle);
    }

    if has_audio {
        for i in 0..=downscaled_bitrates.len() {
            transcode_args.push(String::from("-map"));
            transcode_args.push(String::from("a:0"));
            transcode_args.push(format!("-c:a:{i}"));
            transcode_args.push(String::from("aac"));
            transcode_args.push(format!("-b:a:{i}"));
            transcode_args.push(String::from("96k"));
            transcode_args.push(String::from("-ac"));
            transcode_args.push(String::from("2"));
        }
    }

    transcode_args.push(String::from("-f"));
    transcode_args.push(String::from("hls"));
    transcode_args.push(String::from("-hls_time"));
    transcode_args.push(String::from("2"));
    transcode_args.push(String::from("-hls_playlist_type"));
    transcode_args.push(String::from("vod"));
    transcode_args.push(String::from("-hls_flags"));
    transcode_args.push(String::from("independent_segments"));
    transcode_args.push(String::from("-hls_segment_type"));
    transcode_args.push(String::from("mpegts"));
    transcode_args.push(String::from("-hls_flags"));
    transcode_args.push(String::from("single_file"));
    transcode_args.push(String::from("-master_pl_name"));
    transcode_args.push(String::from("master.m3u8"));
    transcode_args.push(String::from("-var_stream_map"));
    transcode_args.push(
        (0..=downscaled_bitrates.len())
            .map(|idx| {
                let mut s = format!("v:{idx}");
                if has_audio {
                    s.push_str(&format!(",a:{idx}"));
                }
                s
            })
            .join(" "),
    );
    #[cfg(unix)]
    transcode_args.push(format!("{}/stream_%v.m3u8", fifo_dir.path().display()));
    #[cfg(not(unix))]
    transcode_args.push(format!("{}_stream_%v.m3u8", &file_id));

    let master_playlist_join_handle = spawn_hls_master_playlist_reader(
        #[cfg(unix)]
        &fifo_dir,
        bucket.clone(),
        format!("{}/master.m3u8", &file_id),
    )?;

    log::debug!(
        "Spawning HLS transcode ffmpeg command with args {:?}",
        &transcode_args
    );
    let process = match async_process::Command::new("ffmpeg")
        .args(transcode_args)
        .stdout(async_process::Stdio::piped())
        .stderr(async_process::Stdio::piped())
        .spawn()
        .map_err(|e| Error::FfmpegProcessError(e.to_string()))
    {
        Ok(process) => process,
        Err(e) => {
            master_playlist_join_handle.abort();
            for handle in output_reader_join_handles {
                handle.abort();
            }
            return Err(Error::FfmpegProcessError(e.to_string()));
        }
    };

    let process_output = spawn_blocking(
        process
            .output()
            .map_err(|e| Error::FfmpegProcessError(e.to_string())),
    )
    .await;
    match process_output {
        Ok(process_output) if !process_output.status.success() => {
            master_playlist_join_handle.abort();
            for handle in output_reader_join_handles {
                handle.abort();
            }
            let error_msg = std::str::from_utf8(&process_output.stderr)
                .unwrap_or("(stderr contains invalid utf8)");
            return Err(Error::FfmpegProcessError(format!(
                "ffmpeg for hls_transcoding of {} failed with status {}: {}",
                &source_object_key, process_output.status, error_msg
            )));
        }
        Err(e) => {
            master_playlist_join_handle.abort();
            for handle in output_reader_join_handles {
                handle.abort();
            }
            return Err(e);
        }
        _ => {}
    }

    if let Err(e) = persist_hls_transcode_results(
        &source_object_key,
        broker.pk,
        user.pk,
        master_playlist_join_handle,
        output_reader_join_handles,
    )
    .await
    {
        log::error!("Failed to await and persist HLS transcode results to db with error: {}. Going to delete created objects", e.to_string());
        // ignore deletion results for files that have never been created
        let _ = bucket
            .delete_object(&format!("{}/master.m3u8", &file_id))
            .await;
        for i in 0..=downscaled_bitrates.len() {
            let _ = bucket
                .delete_object(&format!("{}/stream_{i}.m3u8", &file_id))
                .await;
            let _ = bucket
                .delete_object(&format!("{}/stream_{i}.ts", &file_id))
                .await;
        }

        return Err(e);
    }

    log::info!(
        "Completed HLS transcoding for {} after {}",
        &source_object_key,
        format_duration(start_time.elapsed())
    );

    Ok(())
}

pub async fn generate_thumbnail(
    bucket: Bucket,
    source_object_key: String,
    file_id: Uuid,
    content_type: String,
    broker: Broker,
    user: User,
) -> Result<(), Error> {
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
            .stderr(async_process::Stdio::piped())
            .spawn()
            .map_err(|e| Error::FfmpegProcessError(e.to_string()))?;

        let mut stdout = BufReader::new(process.stdout.take().ok_or_else(|| {
            Error::FfmpegProcessError(String::from("Could not get stdout of process"))
        })?);

        let thumb_bytes = spawn_blocking(async move {
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

            Ok(thumb_bytes)
        })
        .await?;

        let process_output = process
            .output()
            .map_err(|e| Error::FfmpegProcessError(e.to_string()))
            .await;
        match process_output {
            Ok(process_output) if !process_output.status.success() => {
                let error_msg = std::str::from_utf8(&process_output.stderr)
                    .unwrap_or("(stderr contains invalid utf8)");
                return Err(Error::FfmpegProcessError(format!(
                    "ffmpeg for thumbnail of {} failed with status {}: {}",
                    &source_object_key, process_output.status, error_msg
                )));
            }
            Err(e) => {
                return Err(e);
            }
            _ => {}
        }

        if thumb_bytes.is_empty() {
            log::warn!("Received 0 bytes for thumbnail {}", file_id);
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
                hls_master_playlist: None,
                hls_disabled: true,
                hls_locked_at: Some(Utc::now()),
                thumbnail_locked_at: Some(Utc::now()),
                hls_fail_count: None,
                thumbnail_fail_count: None,
            })
            .get_result::<S3Object>(&mut connection)
            .map_err(|e| Error::QueryError(e.to_string()))?;

        diesel::update(s3_object::table)
            .filter(s3_object::object_key.eq(&source_object_key))
            .set(s3_object::thumbnail_object_key.eq(&s3_object.object_key))
            .execute(&mut connection)
            .map_err(|e| Error::QueryError(e.to_string()))?;

        Ok(())
    } else {
        log::debug!(
            "Not creating thumbnail for unsupported content type '{}'",
            content_type
        );
        Ok(())
    }
}

#[inline]
fn content_type_is_video(content_type: &str) -> bool {
    content_type.starts_with("video/")
}

#[inline]
fn content_type_is_image(content_type: &str) -> bool {
    content_type.starts_with("image/")
}

struct UploadedHlsStream {
    playlist_upload_result: S3UploadResult,
    stream_upload_result: S3UploadResult,
    hls_stream: HlsStream,
}

struct S3UploadResult {
    path: String,
    bytes_read: usize,
    #[cfg(any(unix))]
    response_status: u16,
}

pin_project! {
    struct ByteCountingTokioFileReader {
        #[pin]
        file: tokio::fs::File,
        byte_count: usize
    }
}

impl ByteCountingTokioFileReader {
    #[cfg(any(unix))]
    fn new(file: tokio::fs::File) -> Self {
        ByteCountingTokioFileReader {
            file,
            byte_count: 0,
        }
    }
}

impl tokio::io::AsyncRead for ByteCountingTokioFileReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.project();
        let curr_len = buf.filled().len();
        ready!(tokio::io::AsyncRead::poll_read(this.file, cx, buf))?;
        *this.byte_count += buf.filled().len() - curr_len;
        std::task::Poll::Ready(Ok(()))
    }
}

async fn persist_hls_transcode_results(
    source_object_key: &str,
    broker_pk: i32,
    user_pk: i32,
    master_playlist_join_handle: JoinHandle<Result<S3UploadResult, Error>>,
    output_reader_join_handles: Vec<JoinHandle<Result<UploadedHlsStream, Error>>>,
) -> Result<(), Error> {
    let master_playlist_result = master_playlist_join_handle
        .await
        .map_err(|_| Error::CancellationError)??;

    let stream_results = try_join_all(output_reader_join_handles)
        .await
        .map_err(|_| Error::CancellationError)?;

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
        hls_master_playlist: None,
        hls_disabled: true,
        hls_locked_at: Some(Utc::now()),
        thumbnail_locked_at: Some(Utc::now()),
        hls_fail_count: None,
        thumbnail_fail_count: None,
    }];

    let mut hls_streams = Vec::new();
    for stream_result in stream_results {
        let stream_result = stream_result?;

        hls_streams.push(stream_result.hls_stream);

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
            hls_master_playlist: None,
            hls_disabled: true,
            hls_locked_at: Some(Utc::now()),
            thumbnail_locked_at: Some(Utc::now()),
            hls_fail_count: None,
            thumbnail_fail_count: None,
        });

        s3_objects.push(S3Object {
            object_key: stream_result.stream_upload_result.path,
            sha256_hash: None,
            size_bytes: stream_result.stream_upload_result.bytes_read as i64,
            mime_type: String::from("video/mp2t"),
            fk_broker: broker_pk,
            fk_uploader: user_pk,
            thumbnail_object_key: None,
            creation_timestamp: Utc::now(),
            filename: None,
            hls_master_playlist: None,
            hls_disabled: true,
            hls_locked_at: Some(Utc::now()),
            thumbnail_locked_at: Some(Utc::now()),
            hls_fail_count: None,
            thumbnail_fail_count: None,
        });
    }

    let mut conn = acquire_db_connection()?;
    conn.build_transaction().run::<_, Error, _>(|conn| {
        diesel::insert_into(s3_object::table)
            .values(s3_objects)
            .execute(conn)?;
        diesel::insert_into(hls_stream::table)
            .values(hls_streams)
            .execute(conn)?;

        diesel::update(s3_object::table)
            .set(s3_object::hls_master_playlist.eq(master_playlist_result.path))
            .filter(s3_object::object_key.eq(source_object_key))
            .execute(conn)?;

        Ok(())
    })?;

    Ok(())
}

pub fn is_hls_supported_on_current_platform() -> bool {
    cfg!(unix)
}

#[cfg(unix)]
fn spawn_hls_output_reader(
    fifo_dir: &tempfile::TempDir,
    bucket: Bucket,
    hls_stream: HlsStream,
) -> Result<JoinHandle<Result<UploadedHlsStream, Error>>, Error> {
    use futures::try_join;

    let hls_stream_pipe = fifo_dir
        .path()
        .join(hls_stream.stream_file.split('/').last().unwrap());
    nix::unistd::mkfifo(&hls_stream_pipe, nix::sys::stat::Mode::S_IRWXU)
        .map_err(|e| Error::IoError(e.to_string()))?;
    let hls_playlist_pipe = fifo_dir
        .path()
        .join(hls_stream.stream_playlist.split('/').last().unwrap());
    nix::unistd::mkfifo(&hls_playlist_pipe, nix::sys::stat::Mode::S_IRWXU)
        .map_err(|e| Error::IoError(e.to_string()))?;

    let join_handle = tokio::spawn(async move {
        let stream_file_target_path = hls_stream.stream_file.clone();
        let hls_stream_upload = upload_tokio_file(
            bucket.clone(),
            &hls_stream_pipe,
            stream_file_target_path,
            String::from("video/mp2t"),
        );

        let playlist_file_target_path = hls_stream.stream_playlist.clone();
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
    bucket: Bucket,
    master_playlist_path: String,
) -> Result<JoinHandle<Result<S3UploadResult, Error>>, Error> {
    let master_playlist_pipe = fifo_dir
        .path()
        .join(master_playlist_path.split('/').last().unwrap());
    nix::unistd::mkfifo(&master_playlist_pipe, nix::sys::stat::Mode::S_IRWXU)
        .map_err(|e| Error::IoError(e.to_string()))?;

    let join_handle = tokio::spawn(async move {
        let res = upload_tokio_file(
            bucket,
            &master_playlist_pipe,
            master_playlist_path,
            String::from("application/vnd.apple.mpegurl"),
        )
        .await?;

        if res.response_status >= 300 {
            return Err(Error::S3ResponseError(res.response_status));
        }

        Ok(res)
    });
    Ok(join_handle)
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

#[cfg(any(unix))]
fn upload_tokio_file(
    bucket: Bucket,
    file_path: impl AsRef<std::path::Path>,
    s3_path: String,
    content_type: String,
) -> impl Future<Output = Result<S3UploadResult, Error>> {
    tokio::fs::File::open(file_path)
        .map_err(|e| Error::IoError(e.to_string()))
        .and_then(|f| async move {
            let mut reader = ByteCountingTokioFileReader::new(f);
            log::debug!("Beginning upload for HLS stream or file {}", &s3_path);
            let res = bucket
                .put_object_stream_with_content_type(&mut reader, &s3_path, &content_type)
                .map_err(|e| Error::S3Error(e.to_string()))
                .await?;

            Ok(S3UploadResult {
                path: s3_path,
                bytes_read: reader.byte_count,
                response_status: res,
            })
        })
}
