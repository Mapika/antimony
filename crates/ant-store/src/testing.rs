//! Test doubles and settings shared by the tests of every crate.

use std::sync::Arc;
use std::time::Duration;

use ant_core::{EncodedObject, ObjectId};
use async_trait::async_trait;
use parking_lot::Mutex;

use crate::memory::MemoryObjectStore;
use crate::{Backend, ObjectStore, Result};

/// The backend that tests otherwise run in memory use, chosen by
/// `ANT_TEST_BACKEND`: `memory` (default) or `local` (a fresh scratch local
/// store per call, see [`local_backend`]). Lets the property suites run
/// unchanged against local mode:
/// `ANT_TEST_BACKEND=local cargo nextest run -p ant-server`.
pub fn test_backend(label: &str) -> Backend {
    match std::env::var("ANT_TEST_BACKEND").as_deref() {
        Ok("local") => local_backend(label),
        Ok("memory") | Ok("") | Err(_) => Backend::in_memory(),
        Ok(other) => panic!("ANT_TEST_BACKEND={other}: expected `memory` or `local`"),
    }
}

/// A backend on a fresh local data directory under `$TMPDIR`, deleted with
/// the last handle to it (`LocalStore::open_scratch`).
pub fn local_backend(label: &str) -> Backend {
    match crate::local::LocalStore::open_scratch(label) {
        Ok(store) => store.backend(),
        Err(e) => panic!("opening a scratch local store: {e}"),
    }
}

/// Postgres URL of the test services: `ANT_TEST_PG_URL`, default the dev
/// services' database (`scripts/dev-services.sh start` or
/// `docker compose up -d`).
pub fn pg_url() -> String {
    std::env::var("ANT_TEST_PG_URL")
        .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into())
}

/// Scratch data left by tests that were killed before they could clean up
/// (SIGKILL, OOM, nextest timeouts) is swept once it is older than this
/// (`ANT_TEST_SWEEP_HOURS`, default 6; far longer than any test runs).
fn sweep_max_age() -> Duration {
    let hours = std::env::var("ANT_TEST_SWEEP_HOURS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(6.0);
    Duration::from_secs_f64(hours.max(0.0) * 3600.0)
}

pub(crate) fn unix_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// Run `fut` on its own thread and runtime (so it works from any runtime
/// flavour), waiting at most two minutes.
fn run_detached<F>(name: &str, fut: F)
where
    F: std::future::Future + Send + 'static,
{
    let work = move || {
        if let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() {
            rt.block_on(async {
                let _ = tokio::time::timeout(Duration::from_secs(120), fut).await;
            });
        }
    };
    if let Ok(h) = std::thread::Builder::new().name(name.into()).spawn(work) {
        let _ = h.join();
    }
}

/// A fresh scratch schema name `t_<label>_<unix secs>_<random>`. Every test
/// schema is named like this so the sweeper ([`sweep_schemas`]) can tell
/// test data (and its age) from anything else: it never touches a schema
/// that does not match. The first call in a process also sweeps stale
/// schemas of earlier, killed runs.
pub fn scratch_schema(label: &str) -> String {
    static SWEPT: std::sync::Once = std::sync::Once::new();
    SWEPT.call_once(|| {
        // Postgres not running (fast tier): the sweep just fails.
        let url = pg_url();
        run_detached("sweep-schemas", async move {
            let _ = sweep_schemas(&url, sweep_max_age()).await;
        });
    });
    let label = label.strip_prefix("t_").unwrap_or(label);
    format!("t_{label}_{}_{}", unix_secs(), rand::random::<u64>())
}

fn all_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// Creation time (Unix seconds) encoded in a scratch schema name
/// `t_<label>_<secs>_<digits>`.
fn schema_stamp(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("t_")?;
    let (rest, rand) = rest.rsplit_once('_')?;
    let (_, secs) = rest.rsplit_once('_')?;
    (all_digits(rand) && secs.len() == 10 && all_digits(secs)).then(|| secs.parse().ok())?
}

/// Creation time encoded in an S3 test prefix `<label>-t<secs>-<random>`
/// (or the older `<label>-t<secs>`).
fn prefix_stamp(prefix: &str) -> Option<u64> {
    let prefix = prefix.trim_end_matches('/');
    let stamp = |p: &str| {
        let (_, secs) = p.rsplit_once("-t")?;
        (secs.len() == 10 && all_digits(secs)).then(|| secs.parse().ok())?
    };
    match prefix.rsplit_once('-') {
        Some((rest, rand)) if all_digits(rand) && !rest.ends_with("-t") => {
            stamp(rest).or_else(|| stamp(prefix))
        }
        _ => stamp(prefix),
    }
}

/// Drop every test schema (`t_<label>_<secs>_<random>`, see
/// [`scratch_schema`]) older than `max_age`; returns the dropped names.
/// Other schemas are never touched. Failures on individual schemas (e.g. a
/// concurrent sweep got there first) are skipped.
pub async fn sweep_schemas(url: &str, max_age: Duration) -> Result<Vec<String>> {
    use sqlx::Connection;
    let be = |e: sqlx::Error| crate::StoreError::Backend(Box::new(e));
    let mut conn = sqlx::PgConnection::connect(url).await.map_err(be)?;
    let names: Vec<(String,)> = sqlx::query_as("SELECT nspname::text FROM pg_namespace")
        .fetch_all(&mut conn)
        .await
        .map_err(be)?;
    let _ = conn.close().await;
    let cutoff = unix_secs().saturating_sub(max_age.as_secs());
    let mut dropped = Vec::new();
    for (name,) in names {
        if schema_stamp(&name).is_some_and(|t| t <= cutoff)
            && crate::postgres::PgStore::drop_schema(url, &name).await.is_ok()
        {
            dropped.push(name);
        }
    }
    Ok(dropped)
}

/// Delete every object under `cfg.prefix` (`<prefix>/...`); returns how many.
/// SeaweedFS only returns the space when a whole bucket is deleted
/// (`scripts/clean-test-data.sh --drop-bucket`).
pub async fn delete_s3_prefix(cfg: &crate::s3::S3Config) -> usize {
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    let client = aws_sdk_s3::Client::from_conf(cfg.sdk_config().build());
    let (mut token, mut n) = (None, 0);
    loop {
        let Ok(page) = client
            .list_objects_v2()
            .bucket(&cfg.bucket)
            .prefix(format!("{}/", cfg.prefix))
            .set_continuation_token(token)
            .send()
            .await
        else {
            return n;
        };
        let ids: Vec<ObjectIdentifier> = page
            .contents()
            .iter()
            .filter_map(|o| o.key())
            .filter_map(|k| ObjectIdentifier::builder().key(k).build().ok())
            .collect();
        for chunk in ids.chunks(1000) {
            let Ok(del) = Delete::builder().set_objects(Some(chunk.to_vec())).quiet(true).build()
            else {
                continue;
            };
            if client.delete_objects().bucket(&cfg.bucket).delete(del).send().await.is_ok() {
                n += chunk.len();
            }
        }
        match page.next_continuation_token() {
            Some(t) if page.is_truncated() == Some(true) => token = Some(t.to_owned()),
            _ => return n,
        }
    }
}

/// Delete the S3 test prefixes (`<label>-t<secs>-<random>/`, see
/// [`crate::s3::S3Config::for_tests`]) of the shared test bucket that are
/// older than `max_age`; returns the swept prefixes.
pub async fn sweep_s3_prefixes(max_age: Duration) -> Vec<String> {
    let cfg = crate::s3::S3Config::for_tests_raw("sweep");
    let client = aws_sdk_s3::Client::from_conf(cfg.sdk_config().build());
    let cutoff = unix_secs().saturating_sub(max_age.as_secs());
    let (mut token, mut swept) = (None, Vec::new());
    loop {
        let Ok(page) = client
            .list_objects_v2()
            .bucket(&cfg.bucket)
            .delimiter("/")
            .set_continuation_token(token)
            .send()
            .await
        else {
            return swept;
        };
        for p in page.common_prefixes().iter().filter_map(|p| p.prefix()) {
            let prefix = p.trim_end_matches('/');
            if prefix_stamp(prefix).is_some_and(|t| t <= cutoff) {
                let old = crate::s3::S3Config { prefix: prefix.to_owned(), ..cfg.clone() };
                delete_s3_prefix(&old).await;
                swept.push(prefix.to_owned());
            }
        }
        match page.next_continuation_token() {
            Some(t) if page.is_truncated() == Some(true) => token = Some(t.to_owned()),
            _ => return swept,
        }
    }
}

/// Sweeps stale S3 test prefixes once per process (called when a test
/// builds its [`crate::s3::S3Config::for_tests`]).
pub(crate) fn sweep_s3_once() {
    static SWEPT: std::sync::Once = std::sync::Once::new();
    SWEPT.call_once(|| {
        run_detached("sweep-s3", async {
            let _ = sweep_s3_prefixes(sweep_max_age()).await;
        });
    });
}

/// Deletes an S3 test prefix when dropped, also on panic.
pub struct S3PrefixGuard(pub crate::s3::S3Config);

impl Drop for S3PrefixGuard {
    fn drop(&mut self) {
        let cfg = self.0.clone();
        run_detached("drop-s3-prefix", async move {
            delete_s3_prefix(&cfg).await;
        });
    }
}

/// An object store that records the ids of every `get_many` call before
/// forwarding it (by default to a fresh in-memory store), optionally after a
/// delay. Writes and `has_many` are forwarded unrecorded.
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    delay: Option<Duration>,
    reads: Mutex<Vec<Vec<ObjectId>>>,
}

impl Default for CountingStore {
    fn default() -> Self {
        Self::new(Arc::new(MemoryObjectStore::default()))
    }
}

impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner, delay: None, reads: Mutex::new(Vec::new()) }
    }

    /// Answer each `get_many` after `delay`.
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// The wrapped store (reads through it are not recorded).
    pub fn inner(&self) -> &dyn ObjectStore {
        self.inner.as_ref()
    }

    /// The ids of each `get_many` call since the last [`Self::reset`].
    pub fn reads(&self) -> Vec<Vec<ObjectId>> {
        self.reads.lock().clone()
    }

    /// `get_many` calls since the last [`Self::reset`].
    pub fn calls(&self) -> usize {
        self.reads.lock().len()
    }

    /// Ids asked for since the last [`Self::reset`], over all calls.
    pub fn ids(&self) -> usize {
        self.reads.lock().iter().map(Vec::len).sum()
    }

    /// Start recording from scratch.
    pub fn reset(&self) {
        self.reads.lock().clear();
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        self.reads.lock().push(ids.to_vec());
        if let Some(delay) = self.delay {
            tokio::time::sleep(delay).await;
        }
        self.inner.get_many(ids).await
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        self.inner.put_many(objects).await
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        self.inner.has_many(ids).await
    }
}

#[cfg(test)]
mod sweep_tests {
    use super::*;

    #[test]
    fn stamps_parse_only_test_names() {
        assert_eq!(schema_stamp("t_rp_1760000000_123"), Some(1760000000));
        assert_eq!(schema_stamp("t_1760000000_123"), None);
        assert_eq!(schema_stamp("t_rp_17600000_123"), None);
        assert_eq!(schema_stamp("public"), None);
        assert_eq!(schema_stamp("ant_1760000000_5"), None);
        assert_eq!(schema_stamp("t_rp_1760000000_x"), None);
        assert_eq!(prefix_stamp("rp9-t1760000000/"), Some(1760000000));
        assert_eq!(prefix_stamp("rp9-t1760000000-42/"), Some(1760000000));
        assert_eq!(prefix_stamp("rp9-t1760000000-42"), Some(1760000000));
        assert_eq!(prefix_stamp("e2e/abc"), None);
        assert_eq!(prefix_stamp("repo-t12"), None);
    }
}
