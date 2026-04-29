use crate::data::encode::{
    EXIF_DATE_FORMAT_REGEX, content_type_is_audio, content_type_is_image, content_type_is_video,
    spawn_blocking,
};
use crate::error::Error;
use crate::model::{PgIntervalQuery, PgIntervalWrapper, S3Object, S3ObjectMetadata};
use crate::query::functions::substring;
use crate::schema::{post, s3_object, s3_object_metadata};
use crate::tag::auto_matching::{create_apply_auto_tags_for_post_task, spawn_apply_auto_tags_task};
use crate::task::LockedObjectsTaskSentinel;
use crate::util::deserialize_string_from_number;
use crate::util::{DeStringOrArray, DeserializeOrDefault, join_api_url};
use crate::{acquire_db_connection, run_retryable_transaction};
use chrono::{DateTime, Utc};
use diesel::{
    BoolExpressionMethods, ExpressionMethods, NullableExpressionMethods, QueryDsl, sql_types::Text,
};
use diesel_async::RunQueryDsl;
use diesel_async::scoped_futures::ScopedFutureExt;
use lazy_static::lazy_static;
use regex::Regex;
use serde::Deserialize;
use std::process::{Command, Stdio};

#[derive(Deserialize)]
struct ExifToolOutput {
    #[serde(rename = "FileType")]
    file_type: DeserializeOrDefault<Option<String>>,
    #[serde(rename = "FileTypeExtension")]
    file_type_extension: DeserializeOrDefault<Option<String>>,
    #[serde(rename = "MIMEType")]
    mime_type: DeserializeOrDefault<Option<String>>,
    #[serde(rename = "Title")]
    title: DeserializeOrDefault<Option<String>>,
    #[serde(rename = "Artist")]
    artist: DeserializeOrDefault<Option<DeStringOrArray>>,
    #[serde(rename = "Album")]
    album: DeserializeOrDefault<Option<String>>,
    #[serde(rename = "Genre")]
    genre: DeserializeOrDefault<Option<DeStringOrArray>>,
    #[serde(rename = "CreateDate")]
    create_date: DeserializeOrDefault<Option<String>>,
    #[serde(rename = "DateTimeOriginal")]
    date_time_original: DeserializeOrDefault<Option<String>>,
    #[serde(rename = "Albumartist")]
    album_artist: DeserializeOrDefault<Option<String>>,
    #[serde(rename = "Composer")]
    composer: DeserializeOrDefault<Option<String>>,
    #[serde(
        default,
        rename = "TrackNumber",
        deserialize_with = "deserialize_string_from_number"
    )]
    track_number: Option<String>,
    #[serde(
        default,
        rename = "Discnumber",
        deserialize_with = "deserialize_string_from_number"
    )]
    disc_number: Option<String>,
    #[serde(rename = "Duration")]
    duration: DeserializeOrDefault<Option<String>>,
    #[serde(rename = "ImageWidth")]
    width: DeserializeOrDefault<Option<i32>>,
    #[serde(rename = "ImageHeight")]
    height: DeserializeOrDefault<Option<i32>>,
    #[serde(rename = "VideoFrameRate")]
    frame_rate: DeserializeOrDefault<Option<f64>>,
    #[serde(rename = "AudioSampleRate")]
    audio_sample_rate: DeserializeOrDefault<Option<f64>>,
    #[serde(rename = "SampleRate")]
    sample_rate: DeserializeOrDefault<Option<f64>>,
    #[serde(rename = "AudioChannels")]
    audio_channels: DeserializeOrDefault<Option<i32>>,
}

#[derive(Deserialize)]
struct FfprobeOutput {
    streams: Vec<FfprobeStream>,
    format: FfprobeFormat,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct FfprobeStream {
    index: usize,
    codec_type: DeserializeOrDefault<String>,
    codec_name: DeserializeOrDefault<Option<String>>,
    codec_long_name: DeserializeOrDefault<Option<String>>,
    display_aspect_ratio: DeserializeOrDefault<Option<String>>,
    #[serde(rename = "r_frame_rate")]
    frame_rate: DeserializeOrDefault<Option<String>>,
    width: DeserializeOrDefault<Option<i32>>,
    height: DeserializeOrDefault<Option<i32>>,
    duration: DeserializeOrDefault<Option<String>>,
    bit_rate: DeserializeOrDefault<Option<String>>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct FfprobeFormat {
    format_name: String,
    format_long_name: String,
    size: String,
    duration: Option<String>,
    bit_rate: Option<String>,
    #[serde(default)]
    tags: FfprobeTags,
}

#[derive(Clone, Default, Deserialize)]
struct FfprobeTags {
    creation_time: DeserializeOrDefault<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_string_from_number")]
    track: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_from_number")]
    disc: Option<String>,
    genre: DeserializeOrDefault<Option<String>>,
    title: DeserializeOrDefault<Option<String>>,
    composer: DeserializeOrDefault<Option<String>>,
    artist: DeserializeOrDefault<Option<String>>,
    album: DeserializeOrDefault<Option<String>>,
    album_artist: DeserializeOrDefault<Option<String>>,
}

pub async fn load_object_metadata(
    source_object_key: String,
    metadata_lock_acquired: bool,
) -> Result<(), Error> {
    let object_url = join_api_url(["get-object", &source_object_key])?.to_string();

    let _locked_object_task_sentinel = if metadata_lock_acquired {
        None
    } else {
        match LockedObjectsTaskSentinel::acquire_with_condition(
            "metadata_locked_at",
            "NOT EXISTS(SELECT * FROM s3_object_metadata WHERE object_key = s3_object.object_key AND loaded)",
            source_object_key.clone(),
        )
            .await?
        {
            Some(sentinel) => Some(sentinel),
            None => {
                log::info!(
                    "Aborting metadata extraction for object {} because it has already been locked",
                    &source_object_key
                );
                return Ok(());
            }
        }
    };

    log::info!(
        "Spawning exiftool process to extract metadata for {}",
        &source_object_key
    );

    let cloned_object_url = object_url.clone();
    let exif_proc_output = spawn_blocking(|| {
        let curl_proc = Command::new("curl")
            .arg("-s")
            .arg(cloned_object_url)
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| Error::ChildProcessError(format!("Failed to spawn curl process: {e}")))?;
        let exif_proc = Command::new("exiftool")
            .arg("-j")
            .arg("--struct")
            .arg("-fast")
            .arg("-api")
            .arg("largefilesupport=1")
            .arg("-")
            .stdin(Stdio::from(curl_proc.stdout.ok_or_else(|| {
                Error::ChildProcessError(String::from(
                    "Failed to get stdout of curl process for exiftool",
                ))
            })?))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                Error::ChildProcessError(format!("Failed to spawn exiftool process: {e}"))
            })?;

        exif_proc.wait_with_output().map_err(|e| {
            Error::ChildProcessError(format!("Failed to get exiftool process output: {e}"))
        })
    })
    .await?;

    if !exif_proc_output.status.success() {
        let error_msg = String::from_utf8_lossy(&exif_proc_output.stderr);
        return Err(Error::ChildProcessError(format!(
            "exiftool failed with status {}: {error_msg}",
            exif_proc_output.status
        )));
    }

    let exif_output_str = String::from_utf8_lossy(&exif_proc_output.stdout);
    let mut exif_output =
        serde_json::from_str::<Vec<ExifToolOutput>>(&exif_output_str).map_err(|e| {
            Error::ChildProcessError(format!("Failed to deserialize exiftool output: {e}"))
        })?;

    if exif_output.len() != 1 {
        return Err(Error::ChildProcessError(format!(
            "Expected exactly one exiftool output object, got {}",
            exif_output.len()
        )));
    }
    let exif_output = exif_output.pop().unwrap();

    let content_type_is_video = exif_output
        .mime_type
        .as_deref()
        .map(content_type_is_video)
        .unwrap_or_default();
    let content_type_is_image = exif_output
        .mime_type
        .as_deref()
        .map(content_type_is_image)
        .unwrap_or_default();
    let content_type_is_audio = exif_output
        .mime_type
        .as_deref()
        .map(content_type_is_audio)
        .unwrap_or_default();

    let FfprobeMediaMetadata {
        format_name,
        format_long_name,
        size,
        bit_rate,
        duration_secs,
        video_stream_count,
        video_codec_name,
        video_codec_long_name,
        video_bit_rate_max,
        video_frame_rate,
        audio_stream_count,
        audio_codec_name,
        audio_codec_long_name,
        audio_bit_rate_max,
        ffprobe_output_str,
        ffprobe_tags,
    } = if content_type_is_video || content_type_is_image || content_type_is_audio {
        match load_ffprobe_media_metadata(&object_url).await {
            Ok(metadata) => metadata,
            Err(e) => {
                log::error!(
                    "Failed to load ffprobe metadata for {}: {e}",
                    &source_object_key
                );
                FfprobeMediaMetadata::default()
            }
        }
    } else {
        FfprobeMediaMetadata::default()
    };

    let raw = if let Some(ffprobe_output_str) = ffprobe_output_str {
        serde_json::from_str(&format!(
            "{{\"exiftool\": {exif_output_str}, \"ffprobe\": {ffprobe_output_str}}}"
        ))
        .map_err(|e| Error::SerialisationError(format!("Failed to serialize raw metadate: {e}")))?
    } else {
        serde_json::from_str(&format!("{{\"exiftool\": {exif_output_str}}}")).map_err(|e| {
            Error::SerialisationError(format!("Failed to serialize raw metadata: {e}"))
        })?
    };

    let date_str = exif_output
        .create_date
        .into_inner()
        .or(exif_output.date_time_original.into_inner());
    let date: Result<Option<DateTime<Utc>>, chrono::ParseError> = date_str
        .as_ref()
        .map(|d| {
            let postgres_date = EXIF_DATE_FORMAT_REGEX
                .replace(d, |caps: &regex::Captures| {
                    // format as $1-$2-$3 $4:$5$6$7 with $6 falling back to :00 if missing and $7 falling back to timezone Z if missing
                    let year = &caps[1];
                    let month = &caps[2];
                    let day = &caps[3];
                    let hour = &caps[4];
                    let minute = &caps[5];
                    let second = caps.get(6).map_or(":00", |m| m.as_str()); // Fallback to ":00" if $6 / seconds is not present
                    let timezone = caps.get(7).map_or("Z", |m| m.as_str()); // Fallback to Z if $7 / timezone is not present
                    format!("{year}-{month}-{day} {hour}:{minute}{second}{timezone}")
                })
                .to_string();
            let parsed_date = DateTime::parse_from_str(&postgres_date, "%Y-%m-%d %H:%M:%S%.f%#z")?;
            Ok(parsed_date.with_timezone(&Utc))
        })
        .map_or(Ok(None), |v| v.map(Some));
    let date = match date {
        Ok(date) => date,
        Err(e) => {
            log::warn!(
                "Failed to parse '{:?}' as date from exiftool output for {}: {e}",
                &date_str,
                &source_object_key
            );
            None
        }
    };

    let date = date.or_else(|| {
        if let Some(ffprobe_date) = ffprobe_tags.creation_time.into_inner() {
            let parsed_date = DateTime::parse_from_rfc3339(&ffprobe_date);
            match parsed_date {
                Ok(parsed_date) => Some(parsed_date.with_timezone(&Utc)),
                Err(e) => {
                    log::warn!(
                        "Failed to parse '{:?}' as date from ffprobe output for {}: {e}",
                        &ffprobe_date,
                        &source_object_key
                    );
                    None
                }
            }
        } else {
            None
        }
    });

    let mut connection = acquire_db_connection().await?;

    let duration = if let Some(ref duration_str) = *exif_output.duration {
        let pg_interval = diesel::sql_query("SELECT $1::interval AS pg_interval")
            .bind::<Text, _>(duration_str)
            .get_result::<PgIntervalQuery>(&mut connection)
            .await;
        match pg_interval {
            Ok(pg_interval) => Some(pg_interval.interval),
            Err(e) => {
                if let Some(ffprobe_duration) = duration_secs {
                    let pg_interval = diesel::sql_query("SELECT $1::interval AS pg_interval")
                        .bind::<Text, _>(format!("{ffprobe_duration}s"))
                        .get_result::<PgIntervalQuery>(&mut connection)
                        .await;
                    match pg_interval {
                        Ok(pg_interval) => Some(pg_interval.interval),
                        Err(e) => {
                            log::warn!(
                                "Failed to cast exiftool duration '{:?}' and ffprobe duration '{}' to postgres interval for {}: {e}",
                                &exif_output.duration,
                                ffprobe_duration,
                                &source_object_key
                            );
                            None
                        }
                    }
                } else {
                    log::warn!(
                        "Failed to cast exiftool duration '{:?}' to postgres interval for {}: {e}",
                        &exif_output.duration,
                        &source_object_key
                    );
                    None
                }
            }
        }
    } else {
        None
    };

    fn parse_track_or_disc_number(
        track_or_disc_number: Option<String>,
        source_object_key: &str,
    ) -> (Option<i32>, Option<i32>) {
        track_or_disc_number
            .map(|track_number| {
                lazy_static! {
                    static ref EXIF_TRACK_NUMBER_REGEX: Regex =
                        Regex::new(r"^(\d+)\s*of\s*(\d+)$").unwrap();
                    static ref FFPROBE_TRACK_NUMBER_REGEX: Regex =
                        Regex::new(r"^(\d+)\s*/\s*(\d+)$").unwrap();
                }
                if let Ok(num) = track_number.trim().parse::<i32>() {
                    (Some(num), None)
                } else if let Some(captures) = EXIF_TRACK_NUMBER_REGEX.captures(&track_number) {
                    let track_number = captures.get(1).and_then(|m| m.as_str().parse::<i32>().ok());
                    let track_count = captures.get(2).and_then(|m| m.as_str().parse::<i32>().ok());
                    (track_number, track_count)
                } else if let Some(captures) = FFPROBE_TRACK_NUMBER_REGEX.captures(&track_number) {
                    let track_number = captures.get(1).and_then(|m| m.as_str().parse::<i32>().ok());
                    let track_count = captures.get(2).and_then(|m| m.as_str().parse::<i32>().ok());
                    (track_number, track_count)
                } else {
                    log::warn!(
                        "Cannot to parse track or disc number from '{}' for {}",
                        &track_number,
                        &source_object_key
                    );
                    (None, None)
                }
            })
            .unwrap_or_default()
    }
    let (track_number, track_count) = parse_track_or_disc_number(
        exif_output.track_number.or(ffprobe_tags.track),
        &source_object_key,
    );
    let (disc_number, disc_count) = parse_track_or_disc_number(
        exif_output.disc_number.or(ffprobe_tags.disc),
        &source_object_key,
    );

    let (s3_object_metadata, apply_auto_tags_tasks) =
        run_retryable_transaction(&mut connection, |connection| {
            async move {
                let object = s3_object::table
                    .filter(s3_object::object_key.eq(&source_object_key))
                    .get_result::<S3Object>(connection)
                    .await?;

                let metadata_to_insert = S3ObjectMetadata {
                    object_key: object.object_key,
                    file_type: exif_output.file_type.into_inner(),
                    file_type_extension: exif_output.file_type_extension.into_inner(),
                    mime_type: exif_output
                        .mime_type
                        .into_inner()
                        .or(Some(object.mime_type)),
                    title: exif_output
                        .title
                        .into_inner()
                        .or(ffprobe_tags.title.into_inner()),
                    artist: exif_output
                        .artist
                        .into_inner()
                        .map(DeStringOrArray::into_inner)
                        .or(ffprobe_tags.artist.into_inner()),
                    album: exif_output
                        .album
                        .into_inner()
                        .or(ffprobe_tags.album.into_inner()),
                    album_artist: exif_output
                        .album_artist
                        .into_inner()
                        .or(ffprobe_tags.album_artist.into_inner()),
                    composer: exif_output
                        .composer
                        .into_inner()
                        .or(ffprobe_tags.composer.into_inner()),
                    genre: exif_output
                        .genre
                        .into_inner()
                        .map(DeStringOrArray::into_inner)
                        .or(ffprobe_tags.genre.into_inner()),
                    date,
                    track_number,
                    disc_number,
                    duration: duration.map(PgIntervalWrapper),
                    width: *exif_output.width,
                    height: *exif_output.height,
                    size: size.or(Some(object.size_bytes)),
                    bit_rate,
                    format_name,
                    format_long_name,
                    video_stream_count,
                    video_codec_name,
                    video_codec_long_name,
                    video_frame_rate: (*exif_output.frame_rate).or(video_frame_rate),
                    video_bit_rate_max,
                    audio_stream_count,
                    audio_codec_name,
                    audio_codec_long_name,
                    audio_sample_rate: exif_output
                        .audio_sample_rate
                        .or(exif_output.sample_rate.into_inner()),
                    audio_channels: *exif_output.audio_channels,
                    audio_bit_rate_max,
                    raw,
                    loaded: true,
                    track_count,
                    disc_count,
                };

                let s3_object_metadata = diesel::insert_into(s3_object_metadata::table)
                    .values(&metadata_to_insert)
                    .on_conflict(s3_object_metadata::object_key)
                    .do_update()
                    .set(&metadata_to_insert)
                    .get_result::<S3ObjectMetadata>(connection)
                    .await?;

                if let Some(ref title) = s3_object_metadata.title {
                    let post_update_count = diesel::update(post::table)
                        .filter(
                            post::s3_object
                                .eq(&s3_object_metadata.object_key)
                                .and(post::title.is_null()),
                        )
                        .set(post::title.eq(substring(title, 1, 300).nullable()))
                        .execute(connection)
                        .await?;

                    if post_update_count > 0 {
                        log::info!(
                            "Updated {} posts with title from metadata for object {}",
                            post_update_count,
                            &s3_object_metadata.object_key
                        );
                    }
                }

                let mut apply_auto_tags_tasks = Vec::new();

                let related_posts = post::table
                    .select(post::pk)
                    .filter(post::s3_object.eq(&s3_object_metadata.object_key))
                    .load::<i64>(connection)
                    .await?;

                for post_pk in related_posts {
                    let apply_auto_tags_task =
                        create_apply_auto_tags_for_post_task(post_pk, connection).await?;
                    apply_auto_tags_tasks.push(apply_auto_tags_task);
                }
                Ok((s3_object_metadata, apply_auto_tags_tasks))
            }
            .scope_boxed()
        })
        .await?;

    log::info!(
        "Completed metadata extraction for {}",
        &s3_object_metadata.object_key
    );

    for apply_auto_tags_task in apply_auto_tags_tasks {
        spawn_apply_auto_tags_task(apply_auto_tags_task);
    }

    Ok(())
}

#[derive(Default)]
struct FfprobeMediaMetadata {
    format_name: Option<String>,
    format_long_name: Option<String>,
    size: Option<i64>,
    bit_rate: Option<i64>,
    duration_secs: Option<f32>,
    video_stream_count: i32,
    video_codec_name: Option<String>,
    video_codec_long_name: Option<String>,
    video_bit_rate_max: Option<i64>,
    video_frame_rate: Option<f64>,
    audio_stream_count: i32,
    audio_codec_name: Option<String>,
    audio_codec_long_name: Option<String>,
    audio_bit_rate_max: Option<i64>,
    ffprobe_output_str: Option<String>,
    ffprobe_tags: FfprobeTags,
}

async fn load_ffprobe_media_metadata(object_url: &str) -> Result<FfprobeMediaMetadata, Error> {
    let ffprobe_proc = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_streams",
            "-show_format",
            "-print_format",
            "json",
            object_url,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::FfmpegProcessError(format!("Failed to spawn ffprobe process: {e}")))?;

    let ffprobe_proc_output = spawn_blocking(|| {
        ffprobe_proc.wait_with_output().map_err(|e| {
            Error::FfmpegProcessError(format!("Failed to get ffprobe process output: {e}"))
        })
    })
    .await?;

    if !ffprobe_proc_output.status.success() {
        let error_msg = String::from_utf8_lossy(&ffprobe_proc_output.stderr);
        return Err(Error::FfmpegProcessError(format!(
            "ffprobe failed with status {}: {error_msg}",
            ffprobe_proc_output.status
        )));
    }

    let ffprobe_output_str = String::from_utf8_lossy(&ffprobe_proc_output.stdout).to_string();
    let ffprobe_output =
        serde_json::from_str::<FfprobeOutput>(&ffprobe_output_str).map_err(|e| {
            Error::FfmpegProcessError(format!("Failed to deserialize ffprobe output: {e}"))
        })?;

    let format_name = ffprobe_output.format.format_name;
    let format_long_name = ffprobe_output.format.format_long_name;
    let size = ffprobe_output.format.size.parse::<i64>().map_err(|e| {
        Error::FfmpegProcessError(format!("Failed to parse size from ffprobe output: {e}"))
    })?;
    let bit_rate = ffprobe_output
        .format
        .bit_rate
        .map(|b| b.parse::<i64>())
        .map_or(Ok(None), |v| v.map(Some))
        .map_err(|e| {
            Error::FfmpegProcessError(format!("Failed to parse bit rate from ffprobe output: {e}"))
        })?;
    let duration_secs = ffprobe_output
        .format
        .duration
        .map(|d| d.parse::<f32>())
        .map_or(Ok(None), |v| v.map(Some))
        .map_err(|e| {
            Error::FfmpegProcessError(format!("Failed to parse duration from ffprobe output: {e}"))
        })?;

    let mut video_stream_count = 0;
    let mut video_codec_name: Option<String> = None;
    let mut video_codec_long_name: Option<String> = None;
    let mut video_bit_rate_max: Option<i64> = None;
    let mut video_frame_rate: Option<f64> = None;
    let mut audio_stream_count = 0;
    let mut audio_codec_name: Option<String> = None;
    let mut audio_codec_long_name: Option<String> = None;
    let mut audio_bit_rate_max: Option<i64> = None;

    for stream in ffprobe_output.streams {
        if let Some(frame_rate) = stream.frame_rate.into_inner() {
            let parsed_frame_rate = match frame_rate.parse::<f64>() {
                Ok(frame_rate) => Some(frame_rate),
                Err(_) => {
                    let frame_rate_parts: Vec<&str> = frame_rate.split('/').collect();
                    if frame_rate_parts.len() == 2 {
                        let numerator = frame_rate_parts[0].parse::<f64>().ok();
                        let denominator = frame_rate_parts[1].parse::<f64>().ok();
                        if let (Some(numerator), Some(denominator)) = (numerator, denominator) {
                            Some((numerator / denominator * 100.0).round() / 100.0)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            };

            if let Some(frame_rate) = parsed_frame_rate {
                video_frame_rate = video_frame_rate
                    .map(|fr| fr.max(frame_rate))
                    .or(Some(frame_rate));
            } else {
                log::warn!(
                    "Failed to parse frame rate '{}' from ffprobe output for {}",
                    &frame_rate,
                    object_url,
                );
            }
        }

        if *stream.codec_type == "video" {
            video_stream_count += 1;
            video_codec_name = video_codec_name.or(stream.codec_name.into_inner());
            video_codec_long_name = video_codec_long_name.or(stream.codec_long_name.into_inner());
            if let Some(bit_rate) = stream.bit_rate.into_inner() {
                let bit_rate = bit_rate.parse::<i64>().map_err(|e| {
                    Error::FfmpegProcessError(format!(
                        "Failed to parse stream bit rate from ffprobe output: {e}"
                    ))
                })?;
                video_bit_rate_max = video_bit_rate_max
                    .map(|b| b.max(bit_rate))
                    .or(Some(bit_rate));
            }
        } else if *stream.codec_type == "audio" {
            audio_stream_count += 1;
            audio_codec_name = audio_codec_name.or(stream.codec_name.into_inner());
            audio_codec_long_name = audio_codec_long_name.or(stream.codec_long_name.into_inner());
            if let Some(bit_rate) = stream.bit_rate.into_inner() {
                let bit_rate = bit_rate.parse::<i64>().map_err(|e| {
                    Error::FfmpegProcessError(format!(
                        "Failed to parse stream bit rate from ffprobe output: {e}"
                    ))
                })?;
                audio_bit_rate_max = audio_bit_rate_max
                    .map(|b| b.max(bit_rate))
                    .or(Some(bit_rate));
            }
        }
    }

    Ok(FfprobeMediaMetadata {
        format_name: Some(format_name),
        format_long_name: Some(format_long_name),
        size: Some(size),
        bit_rate,
        duration_secs,
        video_stream_count,
        video_codec_name,
        video_codec_long_name,
        video_bit_rate_max,
        video_frame_rate,
        audio_stream_count,
        audio_codec_name,
        audio_codec_long_name,
        audio_bit_rate_max,
        ffprobe_output_str: Some(ffprobe_output_str),
        ffprobe_tags: ffprobe_output.format.tags,
    })
}
