use std::{fmt, net::SocketAddr, time::Duration};

use url::Url;

use crate::{error::Error, API_BASE_URL};

#[derive(Debug, Clone)]
pub struct FormattedDuration(Duration);

pub fn format_duration(val: Duration) -> FormattedDuration {
    FormattedDuration(val)
}

fn item_plural(f: &mut fmt::Formatter, started: &mut bool, name: &str, value: u64) -> fmt::Result {
    if value > 0 {
        if *started {
            f.write_str(" ")?;
        }
        write!(f, "{}{}", value, name)?;
        if value > 1 {
            f.write_str("s")?;
        }
        *started = true;
    }
    Ok(())
}

fn item(f: &mut fmt::Formatter, started: &mut bool, name: &str, value: u32) -> fmt::Result {
    if value > 0 {
        if *started {
            f.write_str(" ")?;
        }
        write!(f, "{}{}", value, name)?;
        *started = true;
    }
    Ok(())
}

fn item_if_not_started(
    f: &mut fmt::Formatter,
    started: &mut bool,
    name: &str,
    value: u32,
) -> fmt::Result {
    if !*started && value > 0 {
        if *started {
            f.write_str(" ")?;
        }
        write!(f, "{}{}", value, name)?;
        *started = true;
    }
    Ok(())
}

// based on https://docs.rs/humantime/latest/src/humantime/duration.rs.html#295-331
impl fmt::Display for FormattedDuration {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let secs = self.0.as_secs();
        let nanos = self.0.subsec_nanos();

        if secs == 0 && nanos == 0 {
            f.write_str("0s")?;
            return Ok(());
        }

        let years = secs / 31_557_600; // 365.25d
        let ydays = secs % 31_557_600;
        let months = ydays / 2_630_016; // 30.44d
        let mdays = ydays % 2_630_016;
        let days = mdays / 86400;
        let day_secs = mdays % 86400;
        let hours = day_secs / 3600;
        let minutes = day_secs % 3600 / 60;
        let seconds = day_secs % 60;

        let millis = nanos / 1_000_000;
        let micros = nanos / 1000 % 1000;
        let nanosec = nanos % 1000;

        let started = &mut false;
        item_plural(f, started, "year", years)?;
        item_plural(f, started, "month", months)?;
        item_plural(f, started, "day", days)?;
        item(f, started, "h", hours as u32)?;
        item(f, started, "m", minutes as u32)?;
        item(f, started, "s", seconds as u32)?;
        item_if_not_started(f, started, "ms", millis)?;
        item_if_not_started(f, started, "us", micros)?;
        item_if_not_started(f, started, "ns", nanosec)?;
        Ok(())
    }
}

pub struct OptFmt<T>(pub Option<T>);

impl<T: fmt::Display> fmt::Display for OptFmt<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref t) = self.0 {
            fmt::Display::fmt(t, f)
        } else {
            f.write_str("-")
        }
    }
}

pub fn join_api_url<'a>(segments: impl IntoIterator<Item = &'a str>) -> Result<Url, Error> {
    let mut url = API_BASE_URL.clone();
    join_url(&mut url, segments)?;
    Ok(url)
}

pub fn join_url<'a>(
    url: &mut Url,
    segments: impl IntoIterator<Item = &'a str>,
) -> Result<(), Error> {
    let mut path_segments = url
        .path_segments_mut()
        .map_err(|_| Error::InvalidUrlError(String::from("URL cannot be a base")))?;
    for segment in segments {
        path_segments.push(segment);
    }
    Ok(())
}

pub fn addr_to_ip_string(addr: &SocketAddr) -> String {
    match addr {
        SocketAddr::V4(addr) => addr.ip().to_string(),
        SocketAddr::V6(addr) => addr.ip().to_string(),
    }
}

pub fn dedup_vec_optional<T: PartialEq + Ord>(vec: &mut Option<Vec<T>>) {
    if let Some(vec) = vec {
        dedup_vec(vec);
    }
}

pub fn dedup_vecs_optional<T: PartialEq + Ord>(v1: &mut Option<Vec<T>>, v2: &Option<Vec<T>>) {
    if let Some(v1) = v1 {
        if let Some(v2) = v2 {
            dedup_vecs(v1, v2);
        }
    }
}

pub fn dedup_vec<T: PartialEq + Ord>(vec: &mut Vec<T>) {
    vec.sort_unstable();
    vec.dedup();
}

pub fn dedup_vecs<T: PartialEq + Ord>(v1: &mut Vec<T>, v2: &[T]) {
    v1.retain(|e| !v2.contains(e));
}
