use std::{
    fmt,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    time::Duration,
};

use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Deserializer};
use url::Url;

use crate::{API_BASE_URL, error::Error};

lazy_static! {
    pub static ref NOT_BLANK_REGEX: Regex = Regex::new(r".*\S.*").expect("Failed to compile regex");
}

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
        write!(f, "{value}{name}")?;
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
        write!(f, "{value}{name}")?;
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
        write!(f, "{value}{name}")?;
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

pub fn deserialize_string_from_number<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrNumber {
        String(String),
        Number(i64),
        Float(f64),
    }

    match Option::<StringOrNumber>::deserialize(deserializer)? {
        Some(StringOrNumber::String(s)) => Ok(Some(s)),
        Some(StringOrNumber::Number(i)) => Ok(Some(i.to_string())),
        Some(StringOrNumber::Float(f)) => Ok(Some(f.to_string())),
        None => Ok(None),
    }
}

/// Wrapper type used for deserializing a value using `Default` if the deserialization fails.
#[derive(Debug, Default)]
pub struct DeserializeOrDefault<T: Default>(pub T);

impl<T: Default> DeserializeOrDefault<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<'de, T: Default + Deserialize<'de>> Deserialize<'de> for DeserializeOrDefault<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let result = T::deserialize(deserializer);
        if let Err(ref e) = result {
            log::warn!("Failed to deserialize value: {e}");
        }
        result
            .or_else(|_| Ok(T::default()))
            .map(DeserializeOrDefault)
    }
}

impl<T> Clone for DeserializeOrDefault<T>
where
    T: Clone + Default,
{
    fn clone(&self) -> Self {
        DeserializeOrDefault(self.0.clone())
    }
}

impl<T> Deref for DeserializeOrDefault<T>
where
    T: Default,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for DeserializeOrDefault<T>
where
    T: Default,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Wrapper type to deserialize a string or an array of strings into a comma-separated string.
#[derive(Debug)]
pub struct DeStringOrArray(pub String);

impl DeStringOrArray {
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl<'de> Deserialize<'de> for DeStringOrArray {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StringOrArray {
            String(String),
            Array(Vec<String>),
        }

        match StringOrArray::deserialize(deserializer)? {
            StringOrArray::String(s) => Ok(DeStringOrArray(s)),
            StringOrArray::Array(a) => Ok(DeStringOrArray(a.join(", "))),
        }
    }
}

impl Clone for DeStringOrArray {
    fn clone(&self) -> Self {
        DeStringOrArray(self.0.clone())
    }
}

impl Deref for DeStringOrArray {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DeStringOrArray {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Returns true if the difference between curr_value and new_value will cause an update on the database.
///
/// new_value being `None` means no update, new_value being an empty string means the value will be
/// cleared / set to null causing an update if the curr_value is not None.
///
/// Returns true if new_value is present AND either a non-empty string different from the curr_value
/// or an empty string with curr_value being non-empty
#[inline]
pub fn string_value_updated(curr_value: Option<&str>, new_value: Option<&str>) -> bool {
    new_value
        .map(|v| v != curr_value.unwrap_or(""))
        .unwrap_or(false)
}

pub fn vec_eq_sorted<T: Ord>(v1: &mut Vec<T>, v2: &mut Vec<T>) -> bool {
    v1.sort_unstable();
    v2.sort_unstable();
    v1 == v2
}
