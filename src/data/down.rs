use std::cmp;

use rand::{Rng, distr::Alphanumeric};
use s3::Bucket;
use warp::hyper;

use crate::{error::Error, model::S3Object};

use super::s3utils::{self, ObjectWriter};

pub struct GetObjectResponse {
    pub(crate) response_status: u16,
    pub(crate) content_type: String,
    pub(crate) content_length: u64,
    pub(crate) content_range: Option<String>,
}

pub fn get_object_response(
    range: Option<String>,
    object: S3Object,
    bucket: Bucket,
    sender: Option<hyper::body::Sender>,
) -> Result<GetObjectResponse, Error> {
    let response_status;
    let content_type;
    let content_length;
    let content_range;

    let size = object.size_bytes as u64;
    if let Some(range) = range {
        let parsed_range = parse_range(&range, size)?;
        // handle single range
        if parsed_range.len() == 1 {
            let parsed_range = parsed_range[0];
            let start = parsed_range.0;
            let end = parsed_range.1;

            response_status = 206;
            content_type = object.mime_type.clone();
            content_range = Some(format!("bytes {}-{}/{}", start, end, object.size_bytes));
            content_length = end - start + 1;

            if let Some(sender) = sender {
                let object_writer = s3utils::ObjectRangeWriter {
                    bucket,
                    object,
                    start,
                    end,
                };

                tokio::spawn(async move {
                    object_writer.write_bytes(sender).await;
                });
            }
        } else {
            // handle multipart byteranges
            check_range_overlap(&parsed_range, size)?;
            let mut rng = rand::rng();
            let boundary: String = (&mut rng)
                .sample_iter(Alphanumeric)
                .take(60)
                .map(char::from)
                .collect();

            response_status = 206;
            content_type = format!("multipart/byteranges; boundary={}", &boundary);
            content_range = None;

            let parts = parsed_range
                .iter()
                .map(|range| {
                    s3utils::MultipartByteRange::new(
                        range.0,
                        range.1,
                        object.size_bytes as u64,
                        &object.mime_type,
                        &boundary,
                    )
                })
                .collect::<Vec<_>>();

            let end_delimiter = format!("\r\n--{}--\r\n", &boundary);

            content_length = parts
                .iter()
                .map(s3utils::MultipartByteRange::content_length)
                .sum::<u64>()
                + (end_delimiter.len() as u64);

            if let Some(sender) = sender {
                let object_writer = s3utils::MultipartObjectWriter {
                    parts,
                    bucket,
                    object,
                    end_delimiter,
                };

                tokio::spawn(async move {
                    object_writer.write_bytes(sender).await;
                });
            }
        }
    } else {
        response_status = 200;
        content_type = object.mime_type.clone();
        content_length = object.size_bytes as u64;
        content_range = None;

        if let Some(sender) = sender {
            let object_writer = s3utils::FullObjectWriter { bucket, object };

            tokio::spawn(async move {
                object_writer.write_bytes(sender).await;
            });
        }
    };

    Ok(GetObjectResponse {
        response_status,
        content_type,
        content_length,
        content_range,
    })
}

fn parse_range(range: &str, size: u64) -> Result<Vec<(u64, u64)>, Error> {
    let byte_ranges = range
        .split("bytes=")
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    if byte_ranges.len() != 1 {
        return Err(Error::IllegalRangeError(
            size,
            format!("Invalid range header '{range}'"),
        ));
    }

    let byte_ranges = byte_ranges[0];
    byte_ranges
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|byte_range| {
            let parts = byte_range
                .split('-')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>();
            if !(parts.len() == 1 || parts.len() == 2) {
                return Err(Error::IllegalRangeError(
                    size,
                    format!("Invalid range '{byte_range}'"),
                ));
            }

            let start = parts[0]
                .parse::<u64>()
                .map_err(|e| Error::IllegalRangeError(size, e.to_string()))?;

            let end = if parts.len() == 2 {
                parts[1]
                    .parse::<u64>()
                    .map(|end| cmp::min(end, size - 1))
                    .map_err(|e| Error::IllegalRangeError(size, e.to_string()))?
            } else {
                size - 1
            };

            if start >= size || end >= size {
                return Err(Error::IllegalRangeError(
                    size,
                    format!("Range {start} - {end} invalid for size {size}"),
                ));
            } else if start > end {
                return Err(Error::IllegalRangeError(
                    size,
                    format!("Range {start} - {end} invalid"),
                ));
            }

            Ok((start, end))
        })
        .collect::<Result<Vec<_>, Error>>()
}

fn check_range_overlap(ranges: &[(u64, u64)], size: u64) -> Result<(), Error> {
    for i in 0..ranges.len() {
        for j in 0..ranges.len() {
            if i == j {
                continue;
            }

            let range_i = ranges[i];
            let range_j = ranges[j];

            if range_i.0 <= range_j.1 && range_i.1 >= range_j.0 {
                return Err(Error::IllegalRangeError(
                    size,
                    format!("Overlapping ranges {range_i:?} and {range_j:?}"),
                ));
            }
        }
    }
    Ok(())
}
