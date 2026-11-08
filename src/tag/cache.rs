//! In-memory tag lookup and topology cache.
//!
//! Cache invalidations are received over a dedicated LISTEN connection to the
//! writable PostgreSQL primary. Cache reads use the regular Diesel connection
//! pool and may be redirected to a read replica. The generation read and the
//! tag rows must come from the same repeatable-read snapshot.
//!
//! A full reload is performed after LISTEN has been registered, and again after
//! any interruption of the notification connection. Incremental notifications
//! are batched into one SQL query and published atomically using ArcSwap.

use std::{
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
    future::poll_fn,
    hash::{Hash, Hasher},
    sync::{Arc, LazyLock},
    time::Duration,
};

use arc_swap::ArcSwap;
use diesel::{
    QueryableByName, sql_query,
    sql_types::{Array, BigInt, Nullable, Text},
};
use diesel_async::RunQueryDsl;
use regex::Regex;
use serde::Deserialize;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, mpsc},
    time::{sleep, timeout},
};
use tokio_postgres::{AsyncMessage, Client, Connection, NoTls};

use crate::{DATABASE_URL, PG_ENABLE_SSL, acquire_db_connection, error::Error};

const SHARDS: usize = 256;
const PK_BLOCK_SIZE: i64 = 64;
const FULL_RELOAD_THRESHOLD: usize = 512;
const NOTIFICATION_CHANNEL: &str = "tag_cache_changed";
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
const BATCH_DELAY: Duration = Duration::from_millis(5);
const REPLICA_POLL_DELAY: Duration = Duration::from_millis(30);
const REPLICA_CATCHUP_TIMEOUT: Duration = Duration::from_secs(30);

// Serialize cache publications. An audit must not overwrite newer data
// published by the notification listener while the audit query is running.
static SNAPSHOT_REFRESH_LOCK: Mutex<()> = Mutex::const_new(());

/// Acquire one Arc to use for all tag lookups during a query compilation.
/// It remains internally consistent if a refresh happens concurrently.
pub static TAG_CACHE: LazyLock<ArcSwap<TagCacheSnapshot>> =
    LazyLock::new(|| ArcSwap::from_pointee(TagCacheSnapshot::empty()));

pub fn snapshot() -> Arc<TagCacheSnapshot> {
    TAG_CACHE.load_full()
}

pub static POPULATION_ESTIMATES: LazyLock<ArcSwap<PopulationEstimates>> =
    LazyLock::new(|| ArcSwap::from_pointee(PopulationEstimates::default()));

pub fn population_estimates() -> Arc<PopulationEstimates> {
    POPULATION_ESTIMATES.load_full()
}

#[derive(Debug, Clone, Default)]
pub struct PopulationEstimates {
    pub posts: Option<i64>,
    pub post_collections: Option<i64>,
    pub user_groups: Option<i64>,
}

const FUZZY_SIMILARITY_THRESHOLD: f32 = 0.5;

type Trigram = [char; 3];

// These expressions use the same normalization and word boundaries as
// trigram::similarity() and trigram::find_words_iter().
static TRIGRAM_NORMALIZATION: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^|$|\W+").unwrap());
static FUZZY_WORDS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\w+").unwrap());

/// Return the unique, normalized trigrams used by trigram::similarity().
/// Padding and the exclusion of trigrams ending with two spaces must match
/// the similarity function, otherwise candidate filtering can miss matches.
fn trigrams(text: &str) -> Vec<Trigram> {
    let normalized = TRIGRAM_NORMALIZATION.replace_all(text, "  ").to_lowercase();
    let chars: Vec<char> = normalized.chars().collect();
    let mut grams: Vec<Trigram> = chars
        .windows(3)
        .filter(|window| !(window[1] == ' ' && window[2] == ' '))
        .map(|window| [window[0], window[1], window[2]])
        .collect();
    grams.sort_unstable();
    grams.dedup();
    grams
}

/// Test the entire query against contiguous spans of complete words.
/// Unlike find_words_iter(), this also supports multiword search terms.
fn fuzzy_name_matches(query: &str, name: &str) -> bool {
    let words: Vec<_> = FUZZY_WORDS.find_iter(name).collect();
    for (i, first) in words.iter().enumerate() {
        for last in &words[i..] {
            if trigram::similarity(query, &name[first.start()..last.end()])
                > FUZZY_SIMILARITY_THRESHOLD
            {
                return true;
            }
        }
    }
    false
}

const FULL_QUERY: &str = r#"
WITH descendants AS (
    SELECT fk_parent AS tag_pk, array_agg(fk_child) AS descendant_pks
    FROM tag_closure_table
    WHERE depth > 0
    GROUP BY fk_parent
),
alias_links AS (
    SELECT fk_source AS tag_pk, fk_target AS alias_pk FROM tag_alias
    UNION ALL
    SELECT fk_target AS tag_pk, fk_source AS alias_pk FROM tag_alias
),
aliases AS (
    SELECT tag_pk, array_agg(alias_pk) AS alias_pks
    FROM alias_links
    GROUP BY tag_pk
)
SELECT tag.pk, tag.tag_name, tag.matching_post_count,
       tag.matching_post_collection_count, tag.matching_user_group_count,
       descendants.descendant_pks || aliases.alias_pks AS related_tag_pks
FROM tag
LEFT JOIN descendants ON descendants.tag_pk = tag.pk
LEFT JOIN aliases ON aliases.tag_pk = tag.pk
ORDER BY tag.pk
"#;

const INCREMENTAL_QUERY: &str = r#"
WITH requested_tags AS (
    SELECT DISTINCT unnest($1::BIGINT[]) AS pk
),
descendants AS (
    SELECT closure.fk_parent AS tag_pk,
           array_agg(closure.fk_child) AS descendant_pks
    FROM tag_closure_table AS closure
    INNER JOIN requested_tags AS requested ON requested.pk = closure.fk_parent
    WHERE closure.depth > 0
    GROUP BY closure.fk_parent
),
alias_links AS (
    SELECT aliases.fk_source AS tag_pk, aliases.fk_target AS alias_pk
    FROM tag_alias AS aliases
    INNER JOIN requested_tags AS requested ON requested.pk = aliases.fk_source
    UNION ALL
    SELECT aliases.fk_target AS tag_pk, aliases.fk_source AS alias_pk
    FROM tag_alias AS aliases
    INNER JOIN requested_tags AS requested ON requested.pk = aliases.fk_target
),
aliases AS (
    SELECT tag_pk, array_agg(alias_pk) AS alias_pks
    FROM alias_links
    GROUP BY tag_pk
)
SELECT tag.pk, tag.tag_name, tag.matching_post_count,
       tag.matching_post_collection_count, tag.matching_user_group_count,
       descendants.descendant_pks || aliases.alias_pks AS related_tag_pks
FROM tag
INNER JOIN requested_tags AS requested ON requested.pk = tag.pk
LEFT JOIN descendants ON descendants.tag_pk = tag.pk
LEFT JOIN aliases ON aliases.tag_pk = tag.pk
"#;

// reltuples is an inexpensive planner estimate, not an exact row count.
// Negative values indicate that statistics are not available yet.
const POPULATION_QUERY: &str = r#"
SELECT
    (SELECT CASE WHEN reltuples >= 0 THEN reltuples::BIGINT END
     FROM pg_catalog.pg_class WHERE oid = 'post'::regclass) AS posts,
    (SELECT CASE WHEN reltuples >= 0 THEN reltuples::BIGINT END
     FROM pg_catalog.pg_class WHERE oid = 'post_collection'::regclass) AS post_collections,
    (SELECT CASE WHEN reltuples >= 0 THEN reltuples::BIGINT END
     FROM pg_catalog.pg_class WHERE oid = 'user_group'::regclass) AS user_groups
"#;

#[derive(Debug, QueryableByName)]
struct DbPopulationEstimates {
    #[diesel(sql_type = Nullable<BigInt>)]
    posts: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    post_collections: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    user_groups: Option<i64>,
}

/// Refresh approximate table populations used by the tag selectivity heuristic.
pub async fn refresh_population_estimates() -> Result<(), Error> {
    let mut connection = acquire_cache_read_connection().await?;
    let row = sql_query(POPULATION_QUERY)
        .get_result::<DbPopulationEstimates>(&mut connection)
        .await
        .map_err(Error::from)?;

    let estimates = PopulationEstimates {
        posts: row.posts,
        post_collections: row.post_collections,
        user_groups: row.user_groups,
    };
    log::debug!("Refreshed tag population estimates: {estimates:?}");
    POPULATION_ESTIMATES.store(Arc::new(estimates));
    Ok(())
}

#[derive(Debug, QueryableByName)]
struct DbTag {
    #[diesel(sql_type = BigInt)]
    pk: i64,
    #[diesel(sql_type = Text)]
    tag_name: String,
    #[diesel(sql_type = BigInt)]
    matching_post_count: i64,
    #[diesel(sql_type = BigInt)]
    matching_post_collection_count: i64,
    #[diesel(sql_type = BigInt)]
    matching_user_group_count: i64,
    #[diesel(sql_type = Nullable<Array<BigInt>>)]
    related_tag_pks: Option<Vec<i64>>,
}

#[derive(QueryableByName)]
struct DbGeneration {
    #[diesel(sql_type = BigInt)]
    generation: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CachedTag {
    pub pk: i64,
    pub tag_name: String,
    pub matching_post_count: i64,
    pub matching_post_collection_count: i64,
    pub matching_user_group_count: i64,
    // Descendants and aliases, excluding the tag itself.
    pub related_tag_pks: Box<[i64]>,
}

impl CachedTag {
    /// The tag itself and all alias / descendant tag pks.
    pub fn effective_pks(&self) -> Vec<i64> {
        let mut pks = Vec::with_capacity(self.related_tag_pks.len() + 1);
        pks.push(self.pk);
        pks.extend_from_slice(&self.related_tag_pks);
        pks.sort_unstable();
        pks.dedup();
        pks
    }
}

impl From<DbTag> for CachedTag {
    fn from(row: DbTag) -> Self {
        // array_agg() does not guarantee element order. Normalize once so equivalent relationships
        // compare equal during consistency checks.
        let mut related_tag_pks = row.related_tag_pks.unwrap_or_default();
        related_tag_pks.sort_unstable();
        related_tag_pks.dedup();

        Self {
            pk: row.pk,
            tag_name: row.tag_name,
            matching_post_count: row.matching_post_count,
            matching_post_collection_count: row.matching_post_collection_count,
            matching_user_group_count: row.matching_user_group_count,
            related_tag_pks: related_tag_pks.into_boxed_slice(),
        }
    }
}

#[derive(Clone)]
pub struct TagCacheSnapshot {
    by_pk: Vec<Arc<HashMap<i64, Arc<CachedTag>>>>,
    by_name: Vec<Arc<HashMap<String, i64>>>,
    by_trigram: Vec<Arc<HashMap<Trigram, Arc<Vec<i64>>>>>,
    // A full reload or a successful audit certifies complete coverage through this generation. Incremental updates must not advance it.
    pub fully_loaded_generation: i64,
    // Highest DB generation read by an applied refresh. This is not a coverage guarantee, other notifications from that generation may remain.
    pub observed_generation: i64,
}

fn pk_shard(pk: i64) -> usize {
    pk.div_euclid(PK_BLOCK_SIZE).rem_euclid(SHARDS as i64) as usize
}

fn hash_shard<T: Hash + ?Sized>(value: &T) -> usize {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    (hasher.finish() % SHARDS as u64) as usize
}

fn name_shard(name: &str) -> usize {
    hash_shard(name)
}

fn trigram_shard(gram: &Trigram) -> usize {
    hash_shard(gram)
}

impl TagCacheSnapshot {
    fn empty() -> Self {
        Self {
            by_pk: (0..SHARDS).map(|_| Arc::new(HashMap::new())).collect(),
            by_name: (0..SHARDS).map(|_| Arc::new(HashMap::new())).collect(),
            by_trigram: (0..SHARDS).map(|_| Arc::new(HashMap::new())).collect(),
            fully_loaded_generation: 0,
            observed_generation: 0,
        }
    }

    fn from_rows(generation: i64, rows: Vec<DbTag>) -> Self {
        let mut result = Self::empty();
        result.fully_loaded_generation = generation;
        result.observed_generation = generation;
        for row in rows {
            result.insert(Arc::new(row.into()));
        }
        result
    }

    fn index_name(&mut self, pk: i64, name: &str) {
        let lowercase_name = name.to_lowercase();
        Arc::make_mut(&mut self.by_name[name_shard(&lowercase_name)]).insert(lowercase_name, pk);

        for gram in trigrams(name) {
            let postings = Arc::make_mut(&mut self.by_trigram[trigram_shard(&gram)])
                .entry(gram)
                .or_insert_with(|| Arc::new(Vec::new()));
            let pks = Arc::make_mut(postings);
            if let Err(position) = pks.binary_search(&pk) {
                pks.insert(position, pk);
            }
        }
    }

    fn unindex_name(&mut self, pk: i64, name: &str) {
        let lowercase_name = name.to_lowercase();
        Arc::make_mut(&mut self.by_name[name_shard(&lowercase_name)]).remove(&lowercase_name);

        for gram in trigrams(name) {
            let shard = Arc::make_mut(&mut self.by_trigram[trigram_shard(&gram)]);
            if let std::collections::hash_map::Entry::Occupied(mut entry) = shard.entry(gram) {
                let pks = Arc::make_mut(entry.get_mut());
                if let Ok(position) = pks.binary_search(&pk) {
                    pks.remove(position);
                }
                if pks.is_empty() {
                    entry.remove();
                }
            }
        }
    }

    fn insert(&mut self, tag: Arc<CachedTag>) {
        let previous = self.by_pk[pk_shard(tag.pk)].get(&tag.pk).cloned();
        if previous
            .as_ref()
            .is_none_or(|old| old.tag_name != tag.tag_name)
        {
            if let Some(old) = previous {
                self.unindex_name(old.pk, &old.tag_name);
            }
            self.index_name(tag.pk, &tag.tag_name);
        }
        Arc::make_mut(&mut self.by_pk[pk_shard(tag.pk)]).insert(tag.pk, tag);
    }

    fn remove(&mut self, pk: i64) {
        let old = Arc::make_mut(&mut self.by_pk[pk_shard(pk)]).remove(&pk);
        if let Some(old) = old {
            self.unindex_name(old.pk, &old.tag_name);
        }
    }

    fn with_updates(&self, pks: &HashSet<i64>, rows: Vec<DbTag>) -> Self {
        // Replacements with unchanged names only touch the PK map. Missing
        // requested rows are removals. Each modified shard is copied once.
        let mut updated = self.clone();
        let returned: HashSet<i64> = rows.iter().map(|row| row.pk).collect();
        for &pk in pks.difference(&returned) {
            updated.remove(pk);
        }
        for row in rows {
            updated.insert(Arc::new(row.into()));
        }
        updated
    }

    /// Compare the cache against a full database snapshot, ignoring generation
    /// metadata. Name indexes are compared independently of tag entries.
    fn differences_from(&self, database: &Self) -> CacheDifferences {
        let mut diff = CacheDifferences::default();
        for (cached_shard, db_shard) in self.by_pk.iter().zip(&database.by_pk) {
            for (&pk, db_tag) in db_shard.iter() {
                match cached_shard.get(&pk) {
                    None => {
                        diff.missing += 1;
                        diff.record(pk);
                    }
                    Some(cached_tag) if cached_tag != db_tag => {
                        diff.changed += 1;
                        diff.record(pk);
                    }
                    _ => {}
                }
            }
            for &pk in cached_shard.keys() {
                if !db_shard.contains_key(&pk) {
                    diff.unexpected += 1;
                    diff.record(pk);
                }
            }
        }
        diff.name_index_mismatch = self
            .by_name
            .iter()
            .zip(&database.by_name)
            .any(|(cached, db)| cached.as_ref() != db.as_ref());
        diff.trigram_index_mismatch = self
            .by_trigram
            .iter()
            .zip(&database.by_trigram)
            .any(|(cached, db)| cached.as_ref() != db.as_ref());
        diff
    }

    pub fn get(&self, pk: i64) -> Option<Arc<CachedTag>> {
        self.by_pk[pk_shard(pk)].get(&pk).cloned()
    }

    pub fn get_by_name(&self, name: &str) -> Option<Arc<CachedTag>> {
        let name = name.to_lowercase();
        let pk = self.by_name[name_shard(&name)].get(&name)?;
        self.get(*pk)
    }

    /// Find all tag names matching a fuzzy search, including multiword spans.
    /// For short terms, match the query compiler's exact-name fallback.
    pub fn find_fuzzy(&self, search_term: &str) -> Vec<Arc<CachedTag>> {
        let instant = std::time::Instant::now();
        let query = search_term.split_whitespace().collect::<Vec<_>>().join(" ");
        if query.chars().filter(|c| !c.is_whitespace()).count() < 3 {
            return self.get_by_name(&query).into_iter().collect();
        }

        let query_grams = trigrams(&query);
        // For Jaccard similarity > 0.5, the intersection must contain more than half the query trigrams (the union contains at least all of them).
        let minimum_overlap = query_grams.len() / 2 + 1;
        let mut overlap_counts: HashMap<i64, usize> = HashMap::new();
        for gram in query_grams {
            if let Some(pks) = self.by_trigram[trigram_shard(&gram)].get(&gram) {
                for &pk in pks.iter() {
                    *overlap_counts.entry(pk).or_default() += 1;
                }
            }
        }

        let overlap_counts_len = overlap_counts.len();
        let mut matching: Vec<_> = overlap_counts
            .into_iter()
            .filter(|&(_, overlap)| overlap >= minimum_overlap)
            .filter_map(|(pk, _)| self.get(pk))
            .filter(|tag| fuzzy_name_matches(&query, &tag.tag_name))
            .collect();
        matching.sort_unstable_by_key(|tag| tag.pk);
        log::debug!(
            "Tag fuzzy search: {} candidates, {} matches in {} microseconds",
            overlap_counts_len,
            matching.len(),
            instant.elapsed().as_micros()
        );
        matching
    }

    #[cfg(test)]
    pub fn fuzzy_effective_pks(&self, search_term: &str) -> Vec<i64> {
        let mut pks: Vec<_> = self
            .find_fuzzy(search_term)
            .into_iter()
            .flat_map(|tag| tag.effective_pks())
            .collect();
        pks.sort_unstable();
        pks.dedup();
        pks
    }

    #[cfg(test)]
    pub(crate) fn for_tests(tags: Vec<(i64, String, Vec<i64>)>) -> Self {
        let rows = tags
            .into_iter()
            .map(|(pk, tag_name, related_tag_pks)| DbTag {
                pk,
                tag_name,
                matching_post_count: 0,
                matching_post_collection_count: 0,
                matching_user_group_count: 0,
                related_tag_pks: Some(related_tag_pks),
            })
            .collect();

        Self::from_rows(1, rows)
    }
}

#[derive(Default, Debug)]
struct CacheDifferences {
    missing: usize,
    unexpected: usize,
    changed: usize,
    name_index_mismatch: bool,
    trigram_index_mismatch: bool,
    sample_pks: Vec<i64>,
}

impl CacheDifferences {
    fn record(&mut self, pk: i64) {
        if self.sample_pks.len() < 10 {
            self.sample_pks.push(pk);
        }
    }

    fn is_empty(&self) -> bool {
        self.missing == 0
            && self.unexpected == 0
            && self.changed == 0
            && !self.name_index_mismatch
            && !self.trigram_index_mismatch
    }
}

#[derive(Debug, Deserialize)]
struct NotificationPayload {
    generation: i64,
    #[serde(default)]
    tag_pks: Vec<i64>,
    #[serde(default)]
    reload: bool,
}

struct ListenerSession {
    // Keep the client alive for as long as the LISTEN connection is active.
    client: Client,
    notifications: mpsc::Receiver<String>,
}

fn drive_notifications<S, T>(mut connection: Connection<S, T>) -> mpsc::Receiver<String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel(4096);
    tokio::spawn(async move {
        loop {
            match poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(AsyncMessage::Notification(message)))
                    if message.channel() == NOTIFICATION_CHANNEL =>
                {
                    if tx.send(message.payload().to_owned()).await.is_err() {
                        break;
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => {
                    log::error!("Tag cache notification connection failed: {error}");
                    break;
                }
                None => break,
            }
        }
    });
    rx
}

async fn connect_listener() -> Result<ListenerSession, tokio_postgres::Error> {
    let (client, notifications) = if *PG_ENABLE_SSL {
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(crate::server::root_certs())
            .with_no_client_auth();
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(config);
        let (client, connection) = tokio_postgres::connect(&DATABASE_URL, tls).await?;
        (client, drive_notifications(connection))
    } else {
        let (client, connection) = tokio_postgres::connect(&DATABASE_URL, NoTls).await?;
        (client, drive_notifications(connection))
    };

    client.batch_execute("LISTEN tag_cache_changed").await?;
    Ok(ListenerSession {
        client,
        notifications,
    })
}

// The cache listens to changes from the primary DB but may read the current tag hierarchy from
// a read replica. Therefore, the cache must always use the tag topology generation from the notification
// payload and compare it against the primary DB generation loaded through here. If the generation loaded here
// is < the generation from the notification, then the generation has not reached the replica yet
// and the cache will try again after REPLICA_POLL_DELAY.
async fn primary_generation(session: &ListenerSession) -> Result<i64, tokio_postgres::Error> {
    let row = session
        .client
        .query_one(
            "SELECT generation FROM tag_cache_generation WHERE singleton",
            &[],
        )
        .await?;
    Ok(row.get(0))
}

async fn acquire_cache_read_connection() -> Result<crate::DbConnection, Error> {
    acquire_db_connection().await
}

/// Load tags and their generation from one consistent snapshot.
///
/// If the generation loaded from the DB is < `min_generation`, then the generation has not reached
/// the replica yet and the cache will try again after `REPLICA_POLL_DELAY`.
async fn read_rows(min_generation: i64, pks: Option<Vec<i64>>) -> Result<(i64, Vec<DbTag>), Error> {
    timeout(REPLICA_CATCHUP_TIMEOUT, async {
        loop {
            let mut connection = acquire_cache_read_connection().await?;
            let (generation, rows) = connection
                .build_transaction()
                .repeatable_read()
                .read_only()
                .run::<_, diesel::result::Error, _>(async |connection| {
                    let generation =
                        sql_query("SELECT generation FROM tag_cache_generation WHERE singleton")
                            .get_result::<DbGeneration>(connection)
                            .await?
                            .generation;

                    let rows = if generation < min_generation {
                        Vec::new()
                    } else if let Some(ref pks) = pks {
                        sql_query(INCREMENTAL_QUERY)
                            .bind::<Array<BigInt>, _>(pks.clone())
                            .load::<DbTag>(connection)
                            .await?
                    } else {
                        sql_query(FULL_QUERY).load::<DbTag>(connection).await?
                    };

                    Ok((generation, rows))
                })
                .await
                .map_err(Error::from)?;

            if generation >= min_generation {
                return Ok((generation, rows));
            }
            // Do not retain a repeatable-read snapshot while waiting for the replica,
            // a new transaction is required to observe WAL replay.
            drop(connection);
            sleep(REPLICA_POLL_DELAY).await;
        }
    })
    .await
    .map_err(|_| {
        Error::DatabaseConnectionError(format!(
            "Tag cache replica did not reach generation {min_generation} in time"
        ))
    })?
}

async fn reload_all(min_generation: i64) -> Result<(), Error> {
    let instant = std::time::Instant::now();

    loop {
        let required_generation = min_generation.max(ArcSwap::load(&TAG_CACHE).observed_generation);
        let (generation, rows) = read_rows(required_generation, None).await?;
        let count = rows.len();

        // Building the trigram index can be CPU-intensive. Do not block a Tokio runtime worker.
        let new_snapshot =
            tokio::task::spawn_blocking(move || TagCacheSnapshot::from_rows(generation, rows))
                .await
                .map_err(|error| {
                    Error::DatabaseConnectionError(format!(
                        "Failed to construct tag cache snapshot: {error}"
                    ))
                })?;

        // Acquire the lock only when publishing.
        let _guard = SNAPSHOT_REFRESH_LOCK.lock().await;

        let current_generation = ArcSwap::load(&TAG_CACHE).observed_generation;

        if generation < current_generation {
            log::debug!(
                "Discarding full tag cache snapshot at generation {generation}: current cache has reached generation {current_generation}"
            );

            // Drop the lock and retry with a newer database snapshot.
            continue;
        }

        TAG_CACHE.store(Arc::new(new_snapshot));

        log::info!(
            "Performed full tag cache reload with {count} tags at generation {generation} in {}ms",
            instant.elapsed().as_millis()
        );

        return Ok(());
    }
}

async fn refresh_changed(min_generation: i64, pks: HashSet<i64>) -> Result<(), Error> {
    if pks.is_empty() {
        return Ok(());
    }

    if pks.len() > FULL_RELOAD_THRESHOLD {
        return reload_all(min_generation).await;
    }

    let request: Vec<i64> = pks.iter().copied().collect();
    let mut required_generation = min_generation.max(TAG_CACHE.load_full().observed_generation);

    loop {
        // A full reload may already have covered these notifications.
        if min_generation <= TAG_CACHE.load_full().fully_loaded_generation {
            return Ok(());
        }

        let (generation, rows) = read_rows(required_generation, Some(request.clone())).await?;

        let _guard = SNAPSHOT_REFRESH_LOCK.lock().await;
        let previous = TAG_CACHE.load_full();

        // Recheck after acquiring the publication lock.
        if min_generation <= previous.fully_loaded_generation {
            return Ok(());
        }

        if generation < previous.observed_generation {
            required_generation = previous.observed_generation;
            continue;
        }

        let mut updated = previous.with_updates(&pks, rows);
        updated.observed_generation = generation;

        TAG_CACHE.store(Arc::new(updated));
        return Ok(());
    }
}

/// Compare a complete, transactionally consistent database view with the current cache.
/// Publish that view if the cache differs. This also recovers changes whose notifications were missed.
pub async fn verify_cache_consistency() -> Result<(), Error> {
    const MAX_AUDIT_ATTEMPTS: usize = 10;

    let instant = std::time::Instant::now();
    let min_generation = snapshot().observed_generation;

    // Load and construct a complete database snapshot without blocking incremental cache updates.
    let (generation, rows) = read_rows(min_generation, None).await?;
    let database = TagCacheSnapshot::from_rows(generation, rows);

    for attempt in 1..=MAX_AUDIT_ATTEMPTS {
        let existing = TAG_CACHE.load_full();

        // The database snapshot cannot safely replace a cache that has already observed a newer generation.
        if generation < existing.observed_generation {
            log::debug!(
                "Skipping tag cache consistency check: database generation {generation} is older than cached generation {}",
                existing.observed_generation
            );
            return Ok(());
        }

        // Perform the potentially expensive comparison without holding the publication lock.
        let differences = existing.differences_from(&database);

        // Only publication and the final snapshot identity check require synchronization.
        let _guard = SNAPSHOT_REFRESH_LOCK.lock().await;
        let current = TAG_CACHE.load_full();

        if !Arc::ptr_eq(&existing, &current) {
            if attempt == MAX_AUDIT_ATTEMPTS {
                log::warn!(
                    "Skipping tag cache consistency check after {MAX_AUDIT_ATTEMPTS} attempts due to concurrent cache updates ({}ms elapsed)",
                    instant.elapsed().as_millis()
                );
                return Ok(());
            }

            continue;
        }

        if differences.is_empty() {
            // A complete comparison certifies coverage through this generation, even if the cache was updated incrementally.
            if generation > existing.fully_loaded_generation {
                let mut verified = (*existing).clone();
                verified.fully_loaded_generation = generation;
                verified.observed_generation = generation;
                TAG_CACHE.store(Arc::new(verified));
            }

            log::info!(
                "Tag cache consistency verified at generation {generation} in {}ms (attempt {attempt})",
                instant.elapsed().as_millis()
            );
            return Ok(());
        }

        let details = format!(
            "{} missing, {} unexpected, {} changed; \
             name index differs: {}; trigram index differs: {}; \
             sample PKs: {:?}",
            differences.missing,
            differences.unexpected,
            differences.changed,
            differences.name_index_mismatch,
            differences.trigram_index_mismatch,
            differences.sample_pks
        );

        if generation <= existing.fully_loaded_generation {
            log::warn!(
                "Tag cache inconsistency detected at generation {generation}: {details}. Repairing cache"
            );
        } else {
            log::info!(
                "Tag cache audit found changes beyond fully loaded generation {}: {details}. Publishing complete snapshot",
                existing.fully_loaded_generation
            );
        }

        TAG_CACHE.store(Arc::new(database));

        return Ok(());
    }

    Ok(())
}

/// Perform periodic tag-cache verification and refresh selectivity estimates.
/// Both operations run even if one fails.
pub async fn run_maintenance() -> Result<(), Error> {
    let estimates_result = refresh_population_estimates().await;
    if let Err(error) = &estimates_result {
        log::error!("Failed to refresh tag population estimates: {error}");
    }
    let audit_result = verify_cache_consistency().await;
    if let Err(error) = &audit_result {
        log::error!("Failed to verify tag cache consistency: {error}");
    }
    estimates_result?;
    audit_result
}

async fn handle_batch(session: &mut ListenerSession, first: String) -> Result<(), Error> {
    let mut received = vec![first];
    // Adjacent notifications, often produced by recursive triggers in the same transaction,
    // are usually available within a few milliseconds.
    sleep(BATCH_DELAY).await;
    while let Ok(payload) = session.notifications.try_recv() {
        received.push(payload);
    }

    let covered_generation = snapshot().fully_loaded_generation;
    let mut min_generation = 0;
    let mut pks = HashSet::new();
    let mut reload = false;
    let mut invalid_notification = false;

    for json in received {
        match serde_json::from_str::<NotificationPayload>(&json) {
            Ok(event) if event.generation >= 0 => {
                // This check is safe only for a full snapshot. Incremental loads may observe higher
                // generations for some tags without having refreshed every tag modified in those generations.
                if event.generation <= covered_generation {
                    continue;
                }
                min_generation = min_generation.max(event.generation);
                reload |= event.reload || event.tag_pks.is_empty();
                pks.extend(event.tag_pks);
            }
            Ok(_) | Err(_) => {
                log::warn!("Invalid tag cache notification payload: {json}");
                reload = true;
                invalid_notification = true;
            }
        }
    }

    if invalid_notification {
        let barrier = primary_generation(session)
            .await
            .map_err(|error| Error::DatabaseConnectionError(error.to_string()))?;

        min_generation = min_generation.max(barrier);
    }

    if reload || pks.len() > FULL_RELOAD_THRESHOLD {
        reload_all(min_generation).await
    } else {
        let instant = std::time::Instant::now();
        let pk_count = pks.len();
        let res = refresh_changed(min_generation, pks).await;
        log::debug!(
            "Refreshed tag hierarchy cache with {pk_count} tags in {}ms",
            instant.elapsed().as_millis()
        );
        res
    }
}

/// Subscribe, load the initial snapshot, and spawn a reconnecting listener.
/// Call this during server startup before enabling cache-backed lookups.
pub async fn initialize() -> Result<(), Error> {
    // LISTEN is registered before loading the initial snapshot; notifications committed during the
    // full load remain queued and are handled afterward.
    let session = connect_listener()
        .await
        .map_err(|error| Error::DatabaseConnectionError(error.to_string()))?;
    let barrier = primary_generation(&session)
        .await
        .map_err(|error| Error::DatabaseConnectionError(error.to_string()))?;
    reload_all(barrier).await?;
    if let Err(error) = refresh_population_estimates().await {
        log::error!("Unable to initialize tag population estimates: {error}");
    }
    tokio::spawn(async move { listen_forever(session).await });
    Ok(())
}

async fn listen_forever(mut session: ListenerSession) {
    log::info!("Starting tag cache listener");
    loop {
        while let Some(message) = session.notifications.recv().await {
            if let Err(error) = handle_batch(&mut session, message).await {
                log::error!("Unable to refresh tag cache: {error}");
                // On any refresh failure, reconnect and rebuild the cache to avoid losing notifications that were already consumed.
                break;
            }
        }

        loop {
            sleep(RECONNECT_DELAY).await;
            let next = match connect_listener().await {
                Ok(session) => session,
                Err(error) => {
                    log::error!("Unable to reconnect tag cache listener: {error}");
                    continue;
                }
            };

            // Use a primary-side barrier so the full replica snapshot includes every commit from before the new LISTEN registration.
            let barrier = match primary_generation(&next).await {
                Ok(generation) => generation,
                Err(error) => {
                    log::error!("Unable to check primary tag generation: {error}");
                    continue;
                }
            };
            match reload_all(barrier).await {
                Ok(()) => {
                    session = next;
                    break;
                }
                Err(error) => {
                    log::error!("Unable to rebuild tag cache: {error}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pk: i64, name: &str, related: &[i64]) -> DbTag {
        DbTag {
            pk,
            tag_name: name.to_owned(),
            matching_post_count: 0,
            matching_post_collection_count: 0,
            matching_user_group_count: 0,
            related_tag_pks: Some(related.to_vec()),
        }
    }

    #[test]
    fn changing_one_tag_preserves_untouched_entries() {
        let initial =
            TagCacheSnapshot::from_rows(7, vec![row(1, "Audio", &[2]), row(2, "Video", &[])]);
        let unchanged = initial.get(2).unwrap();
        let updated = initial.with_updates(&HashSet::from([1]), vec![row(1, "Music", &[3])]);

        assert!(initial.get_by_name("audio").is_some());
        assert!(updated.get_by_name("audio").is_none());
        assert_eq!(
            updated.get_by_name("MUSIC").unwrap().effective_pks(),
            vec![1, 3]
        );
        assert!(Arc::ptr_eq(&unchanged, &updated.get(2).unwrap()));
        assert_eq!(updated.fully_loaded_generation, 7);
        assert_eq!(updated.observed_generation, 7);
    }

    #[test]
    fn audit_ignores_relation_array_order_and_duplicates() {
        let a = TagCacheSnapshot::from_rows(3, vec![row(1, "Audio", &[4, 2, 2])]);
        let b = TagCacheSnapshot::from_rows(3, vec![row(1, "Audio", &[2, 4])]);
        assert!(a.differences_from(&b).is_empty());
    }

    #[test]
    fn audit_detects_missing_changed_and_unexpected_tags() {
        let cache =
            TagCacheSnapshot::from_rows(9, vec![row(1, "Old", &[2]), row(3, "Removed", &[])]);
        let database =
            TagCacheSnapshot::from_rows(9, vec![row(1, "New", &[2]), row(2, "Created", &[])]);
        let diff = cache.differences_from(&database);
        assert_eq!((diff.missing, diff.unexpected, diff.changed), (1, 1, 1));
        assert!(diff.name_index_mismatch);
    }

    #[test]
    fn fuzzy_search_matches_words_and_multiword_spans() {
        let cache = TagCacheSnapshot::from_rows(
            1,
            vec![
                row(1, "audio", &[20, 21]),
                row(2, "progressive rock", &[]),
                row(3, "jazz fusion", &[]),
                row(4, "test", &[]),
            ],
        );
        assert!(cache.find_fuzzy("audi").iter().any(|tag| tag.pk == 1));
        assert!(cache.find_fuzzy("rock").iter().any(|tag| tag.pk == 2));
        assert!(!cache.find_fuzzy("rog").iter().any(|tag| tag.pk == 2));
        assert!(
            cache
                .find_fuzzy("progressive rock")
                .iter()
                .any(|tag| tag.pk == 2)
        );
        assert_eq!(cache.fuzzy_effective_pks("audi"), vec![1, 20, 21]);
        assert!(cache.find_fuzzy("t").is_empty());
        assert_eq!(cache.find_fuzzy("test")[0].pk, 4);
    }

    #[test]
    fn index_never_filters_out_a_matching_name() {
        let names = [
            "audio",
            "audi",
            "audio book",
            "progressive rock",
            "rock & roll",
            "heavy-metal",
            "metallica",
            "jazz fusion",
            "TEST",
            "testing",
            "café au lait",
            "über",
            "foo_bar",
            "foo-bar",
            "abc.def",
            "a",
            "a b",
            "a:b:c",
            "something entirely different",
            "pïng pong",
        ];
        let cache = TagCacheSnapshot::from_rows(
            1,
            names
                .iter()
                .enumerate()
                .map(|(i, name)| row(i as i64, name, &[]))
                .collect(),
        );
        for term in [
            "test",
            "audi",
            "rock",
            "progressive rock",
            "metal",
            "café",
            "foo",
            "foo bar",
            "über",
            "abc",
            "a",
        ] {
            let term = term.split_whitespace().collect::<Vec<_>>().join(" ");
            let mut expected: Vec<_> = names
                .iter()
                .enumerate()
                .filter(|(_, name)| {
                    if term.chars().filter(|c| !c.is_whitespace()).count() < 3 {
                        name.to_lowercase() == term.to_lowercase()
                    } else {
                        fuzzy_name_matches(&term, name)
                    }
                })
                .map(|(i, _)| i as i64)
                .collect();
            expected.sort_unstable();
            let actual: Vec<_> = cache.find_fuzzy(&term).iter().map(|tag| tag.pk).collect();
            assert_eq!(actual, expected, "query: {term}");
        }
    }

    #[test]
    fn count_only_updates_preserve_name_indices() {
        let cache = TagCacheSnapshot::from_rows(1, vec![row(1, "Audio", &[])]);
        let mut changed = row(1, "Audio", &[]);
        changed.matching_post_count = 42;
        let updated = cache.with_updates(&HashSet::from([1]), vec![changed]);
        assert_eq!(updated.get(1).unwrap().matching_post_count, 42);
        for i in 0..SHARDS {
            assert!(Arc::ptr_eq(&cache.by_name[i], &updated.by_name[i]));
            assert!(Arc::ptr_eq(&cache.by_trigram[i], &updated.by_trigram[i]));
        }
    }

    #[test]
    fn rename_and_delete_update_trigram_index() {
        let cache = TagCacheSnapshot::from_rows(1, vec![row(1, "Audio", &[])]);
        let renamed = cache.with_updates(&HashSet::from([1]), vec![row(1, "Music", &[])]);
        assert!(cache.find_fuzzy("audi").iter().any(|tag| tag.pk == 1));
        assert!(!renamed.find_fuzzy("audi").iter().any(|tag| tag.pk == 1));
        assert_eq!(renamed.get_by_name("music").unwrap().pk, 1);
        let deleted = renamed.with_updates(&HashSet::from([1]), vec![]);
        assert!(deleted.find_fuzzy("music").is_empty());
    }

    #[test]
    fn audit_detects_index_only_corruption() {
        let cache = TagCacheSnapshot::from_rows(1, vec![row(1, "audio", &[])]);
        let mut corrupted = cache.clone();
        let gram = trigrams("audio")[0];
        Arc::make_mut(&mut corrupted.by_trigram[trigram_shard(&gram)]).remove(&gram);
        let differences = corrupted.differences_from(&cache);
        assert!(differences.trigram_index_mismatch);
        assert_eq!(
            (
                differences.missing,
                differences.changed,
                differences.unexpected
            ),
            (0, 0, 0)
        );
    }

    #[test]
    fn deletion_removes_name_and_pk() {
        let initial = TagCacheSnapshot::from_rows(10, vec![row(4, "Deleted", &[])]);
        let updated = initial.with_updates(&HashSet::from([4]), vec![]);
        assert!(updated.get(4).is_none());
        assert!(updated.get_by_name("deleted").is_none());
    }

    #[test]
    fn partial_updates_at_the_same_generation_preserve_both_changes() {
        let initial =
            TagCacheSnapshot::from_rows(7, vec![row(1, "Audio", &[10]), row(2, "Video", &[20])]);

        let mut first = initial.with_updates(&HashSet::from([1]), vec![row(1, "Audio", &[11])]);
        first.observed_generation = 8;

        let mut second = first.with_updates(&HashSet::from([2]), vec![row(2, "Video", &[21])]);
        second.observed_generation = 8;

        assert_eq!(second.get(1).unwrap().effective_pks(), vec![1, 11]);
        assert_eq!(second.get(2).unwrap().effective_pks(), vec![2, 21]);
        assert_eq!(second.observed_generation, 8);
        assert_eq!(second.fully_loaded_generation, 7);

        // Neither incremental update modifies the original snapshot.
        assert_eq!(initial.get(1).unwrap().effective_pks(), vec![1, 10]);
        assert_eq!(initial.get(2).unwrap().effective_pks(), vec![2, 20]);
    }

    #[test]
    fn complete_snapshot_covers_other_changes_at_the_same_generation() {
        let initial =
            TagCacheSnapshot::from_rows(7, vec![row(1, "Audio", &[10]), row(2, "Video", &[20])]);

        let mut partial = initial.with_updates(&HashSet::from([1]), vec![row(1, "Audio", &[11])]);
        partial.observed_generation = 8;

        // A second notification for generation 8 hasn't been applied.
        let full =
            TagCacheSnapshot::from_rows(8, vec![row(1, "Audio", &[11]), row(2, "Video", &[21])]);

        assert_eq!(partial.observed_generation, full.observed_generation);
        assert_eq!(partial.fully_loaded_generation, 7);
        assert_eq!(full.fully_loaded_generation, 8);

        assert_eq!(full.get(2).unwrap().effective_pks(), vec![2, 21]);
        assert_eq!(partial.differences_from(&full).changed, 1);
        assert!(full.differences_from(&full).is_empty());
    }

    #[test]
    fn overlapping_fuzzy_matches_return_unique_sorted_effective_pks() {
        let cache = TagCacheSnapshot::from_rows(
            1,
            vec![
                row(11, "Audio", &[102, 101, 102]),
                row(12, "Audi", &[103, 102]),
            ],
        );

        assert_eq!(
            cache.fuzzy_effective_pks("audi"),
            vec![11, 12, 101, 102, 103]
        );
    }

    #[test]
    fn partial_rename_preserves_previous_snapshot_and_updates_both_indexes() {
        let before = TagCacheSnapshot::from_rows(1, vec![row(5, "Audio", &[6])]);

        let after = before.with_updates(&HashSet::from([5]), vec![row(5, "Music", &[7])]);

        assert_eq!(
            before.get_by_name("AUDIO").unwrap().effective_pks(),
            vec![5, 6]
        );
        assert_eq!(before.fuzzy_effective_pks("audi"), vec![5, 6]);

        assert!(after.get_by_name("audio").is_none());
        assert!(after.find_fuzzy("audi").is_empty());
        assert_eq!(
            after.get_by_name("MUSIC").unwrap().effective_pks(),
            vec![5, 7]
        );
    }

    #[test]
    fn audit_detects_count_and_topology_changes_without_name_index_changes() {
        let initial = TagCacheSnapshot::from_rows(1, vec![row(1, "Audio", &[2])]);

        let mut changed_row = row(1, "Audio", &[3]);
        changed_row.matching_post_count = 42;

        let changed = initial.with_updates(&HashSet::from([1]), vec![changed_row]);

        let diff = initial.differences_from(&changed);

        assert_eq!((diff.missing, diff.changed, diff.unexpected), (0, 1, 0));
        assert!(!diff.name_index_mismatch);
        assert!(!diff.trigram_index_mismatch);
    }

    #[test]
    fn audit_detects_name_index_corruption_without_tag_changes() {
        let original = TagCacheSnapshot::from_rows(1, vec![row(1, "Audio", &[])]);

        let mut corrupted = original.clone();
        let name = "audio";

        Arc::make_mut(&mut corrupted.by_name[name_shard(name)]).remove(name);

        let diff = corrupted.differences_from(&original);

        assert!(diff.name_index_mismatch);
        assert!(!diff.trigram_index_mismatch);
        assert_eq!((diff.missing, diff.changed, diff.unexpected), (0, 0, 0));
    }

    #[test]
    fn short_fuzzy_terms_use_case_insensitive_exact_name_matching() {
        let cache = TagCacheSnapshot::from_rows(
            1,
            vec![row(1, "A", &[]), row(2, "AB", &[]), row(3, "ABC", &[])],
        );

        assert_eq!(cache.find_fuzzy(" a ")[0].pk, 1);
        assert_eq!(cache.find_fuzzy("ab")[0].pk, 2);
        assert!(cache.find_fuzzy("bc").is_empty());
    }

    #[test]
    fn notification_payload_parses_partial_and_reload_forms() {
        let partial: NotificationPayload =
            serde_json::from_str(r#"{"generation":8,"tag_pks":[1,2]}"#).unwrap();

        assert_eq!(partial.generation, 8);
        assert_eq!(partial.tag_pks, vec![1, 2]);
        assert!(!partial.reload);

        let reload: NotificationPayload =
            serde_json::from_str(r#"{"generation":8,"reload":true}"#).unwrap();

        assert_eq!(reload.generation, 8);
        assert!(reload.tag_pks.is_empty());
        assert!(reload.reload);

        assert!(serde_json::from_str::<NotificationPayload>("not json").is_err());
    }

    #[test]
    fn pk_sharding_groups_consecutive_ids() {
        let block_start = 1024_i64;

        for offset in 0..PK_BLOCK_SIZE {
            assert_eq!(pk_shard(block_start), pk_shard(block_start + offset),);
        }

        assert_ne!(pk_shard(block_start), pk_shard(block_start + PK_BLOCK_SIZE),);

        // After 256 blocks, the shard assignment wraps around.
        assert_eq!(
            pk_shard(block_start),
            pk_shard(block_start + PK_BLOCK_SIZE * SHARDS as i64),
        );

        assert_eq!(pk_shard(-1), SHARDS - 1);
    }
}
