pub mod avatar;
pub mod hls;
pub mod metadata;
pub mod output;
pub mod probe;
pub mod thumb;

use regex::Regex;
use rusty_pool::ThreadPool;
use std::collections::HashSet;
use tokio::sync::Semaphore;

use crate::{CONCURRENT_VIDEO_TRANSCODE_LIMIT, error::Error};
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
        Regex::new(r"(\d+):(\d+):(\d+) (\d+):(\d+)(:\d+(?:\.\d+)?)?((?:[+-]\d+:?\d+)|Z)?")
            .expect("Failed to compile EXIF date format regex");
}

pub async fn spawn_encode_task_blocking<R: Send + 'static>(
    task: impl FnOnce() -> Result<R, Error> + Send + 'static,
) -> Result<R, Error> {
    let join_handle = ENCODE_POOL.evaluate(task);

    join_handle
        .receiver
        .await
        .unwrap_or_else(|_| Err(Error::CancellationError))
}
