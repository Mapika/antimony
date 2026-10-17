//! S3-compatible `ObjectStore` for large objects.
//!
//! - Key layout: `{prefix}/{hex[0..2]}/{hex}` where hex is the object id.
//! - Values are the zstd-compressed (level 3) encoded object bytes. The hash
//!   is verified after decompressing on `get_many`; a mismatch is a
//!   `Backend` error.
//! - `get_many`/`has_many`/`put_many` fan out with bounded concurrency
//!   (`futures::stream::iter(..).buffered(64)`), preserving input order.
//! - `put_many` skips ids repeated within the batch; a plain PUT is
//!   idempotent (content is identical by construction).
//! - A missing key (`NoSuchKey` / 404) is `None`, not an error. `has_many`
//!   uses HEAD.
//! - `connect` builds a client with `force_path_style(true)` (SeaweedFS/MinIO
//!   need it), static credentials, the given region and endpoint.
//! - Every request is bounded: a connect timeout, a per-attempt timeout that
//!   covers the whole response body (`attempt + size / min_throughput`) and an
//!   overall operation timeout ([`S3Timeouts`]). Throttling (429, 503,
//!   `SlowDown`), 5xx, connection errors, timeouts and truncated or cut-off
//!   bodies are retried with exponential backoff and full jitter (a pause of
//!   up to `backoff_base * 2^n`, at most `backoff_cap`; a longer
//!   `Retry-After` is honoured) for as long as `retry_budget` allows,
//!   counted from the operation's first failure (and up to `max_attempts`,
//!   if set). GET, HEAD and PUT of content-addressed keys are idempotent.
//!   Client errors (403, 404, ...) and corrupt data fail at once. The SDK's
//!   own retries are disabled so attempts do not multiply. After giving up a
//!   `StoreError::Backend` names the operation, key and last failure.
//! - At most `max_concurrency` requests per store (clones share the limit)
//!   are in flight; more wait for a slot, so bursts (GC mark, repack, clone
//!   planning, checkout bundles) queue in the process instead of overrunning
//!   the backend, which throttles beyond some in-flight volume. A retry waits
//!   for a slot again; the backoff pause holds none.
//! - Process-wide counters ([`stats`]): retries by reason, operations that
//!   gave up, requests in flight and waiting for a slot.
//! - `ensure_bucket` creates the bucket if it does not exist ("already owned
//!   by you" counts as success).

use std::{
    collections::HashSet,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};

use ant_core::{EncodedObject, ObjectId};
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{
    config::{retry::RetryConfig, timeout::TimeoutConfig},
    error::{DisplayErrorContext, ProvideErrorMetadata, SdkError},
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use tokio::sync::Semaphore;

use crate::{BLOCKING_THRESHOLD, ObjectStore, Result, StoreError, ZSTD_LEVEL};

/// Maximum number of concurrent S3 requests per batch call.
const FANOUT: usize = 64;
/// Size assumed for the per-attempt timeout of a read whose size is unknown.
const UNKNOWN_SIZE: u64 = 16 * 1024 * 1024;
/// Longest `Retry-After` honoured.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);
/// Default [`S3Timeouts::max_concurrency`].
pub const DEFAULT_MAX_CONCURRENCY: usize = 32;

type HttpResponse = aws_sdk_s3::config::http::HttpResponse;

/// Timeout, retry and concurrency policy of the S3 client (see the module
/// docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3Timeouts {
    /// Establishing a TCP/TLS connection.
    pub connect: Duration,
    /// Fixed part of the per-attempt timeout (request sent, headers and the
    /// whole body received).
    pub attempt: Duration,
    /// Slowest acceptable transfer rate, bytes per second: an attempt moving
    /// `n` bytes may take `attempt + n / min_throughput`.
    pub min_throughput: u64,
    /// Everything about one operation, waiting for a slot, retries and
    /// pauses included.
    pub operation: Duration,
    /// Attempts per operation (1 = no retry, 0 = no limit: the retry budget
    /// decides).
    pub max_attempts: u32,
    /// How long after its first failed attempt an operation keeps retrying
    /// transient failures.
    pub retry_budget: Duration,
    /// The pause after the first failed attempt is drawn from `0..=base`,
    /// and the range doubles per attempt...
    pub backoff_base: Duration,
    /// ...up to `0..=cap`.
    pub backoff_cap: Duration,
    /// Requests in flight per store at most (shared by its clones).
    pub max_concurrency: usize,
}

impl Default for S3Timeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(5),
            attempt: Duration::from_secs(30),
            min_throughput: 10_000_000,
            operation: Duration::from_secs(300),
            max_attempts: 0,
            retry_budget: Duration::from_secs(60),
            backoff_base: Duration::from_millis(100),
            backoff_cap: Duration::from_secs(5),
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
        }
    }
}

impl S3Timeouts {
    /// Time one attempt moving `bytes` bytes may take.
    fn attempt_for(&self, bytes: u64) -> Duration {
        self.attempt + Duration::from_secs_f64(bytes as f64 / self.min_throughput.max(1) as f64)
    }

    /// Pause after failed attempt `n` (1-based): full jitter over
    /// `0..=min(cap, base * 2^(n-1))`.
    fn backoff(&self, n: u32) -> Duration {
        let ceiling = self
            .backoff_base
            .saturating_mul(1u32 << n.saturating_sub(1).min(20))
            .min(self.backoff_cap);
        ceiling.mul_f64(rand::random::<f64>())
    }
}

/// Why an attempt is retried (`ant_s3_retries_total{reason}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reason {
    /// 429, 503 or a `SlowDown` error code.
    Throttled = 0,
    /// Other 5xx, 408, 425.
    ServerError = 1,
    /// The request could not be sent or its response not read (connection
    /// refused or reset, malformed response).
    Network = 2,
    /// The response body broke off, came up short or failed mid-stream.
    Body = 3,
    /// The attempt timed out.
    Timeout = 4,
}

/// Retry reasons, in the order of [`S3Stats::retries`].
pub const RETRY_REASONS: [&str; 5] = ["throttled", "server_error", "network", "body", "timeout"];

static RETRIES: [AtomicU64; 5] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static GAVE_UP: AtomicU64 = AtomicU64::new(0);
/// Request kinds counted by [`S3Stats::requests`] (ranged reads count as `get`).
pub const REQUEST_OPS: [&str; 4] = ["get", "put", "head", "other"];
static REQUESTS: [AtomicU64; 4] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
static READ_BYTES: AtomicU64 = AtomicU64::new(0);
static IN_FLIGHT: AtomicI64 = AtomicI64::new(0);
static QUEUED: AtomicI64 = AtomicI64::new(0);

/// Process-wide S3 client counters (all stores), for metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct S3Stats {
    /// Attempts retried, per [`RETRY_REASONS`] (cumulative).
    pub retries: [u64; 5],
    /// Operations that failed after retrying: budget, attempts or operation
    /// timeout exhausted (cumulative).
    pub gave_up: u64,
    /// Requests in flight now.
    pub in_flight: i64,
    /// Requests waiting for a slot now (`max_concurrency`).
    pub queued: i64,
    /// Requests sent (attempts, retries included), per [`REQUEST_OPS`]
    /// (cumulative).
    pub requests: [u64; 4],
    /// Body bytes received by GETs (cumulative).
    pub read_bytes: u64,
}

/// The current [`S3Stats`].
pub fn stats() -> S3Stats {
    S3Stats {
        retries: std::array::from_fn(|i| RETRIES[i].load(Ordering::Relaxed)),
        gave_up: GAVE_UP.load(Ordering::Relaxed),
        in_flight: IN_FLIGHT.load(Ordering::Relaxed),
        queued: QUEUED.load(Ordering::Relaxed),
        requests: std::array::from_fn(|i| REQUESTS[i].load(Ordering::Relaxed)),
        read_bytes: READ_BYTES.load(Ordering::Relaxed),
    }
}

/// Adds one to a gauge while alive (also when its future is dropped).
struct GaugeGuard(&'static AtomicI64);

impl GaugeGuard {
    fn new(g: &'static AtomicI64) -> Self {
        g.fetch_add(1, Ordering::Relaxed);
        Self(g)
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
    pub timeouts: S3Timeouts,
}

impl S3Config {
    /// An SDK client configuration for this endpoint, bucket region and
    /// credentials (static credentials, path-style addressing, which
    /// SeaweedFS and MinIO need). [`S3ObjectStore::connect`] adds its
    /// timeouts and retry policy; tools that talk to the same store (e.g.
    /// to clean up after a benchmark) build their client from it as is.
    pub fn sdk_config(&self) -> aws_sdk_s3::config::Builder {
        let credentials = aws_sdk_s3::config::Credentials::new(
            &self.access_key,
            &self.secret_key,
            None,
            None,
            "ant-static",
        );
        aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new(self.region.clone()))
            .endpoint_url(&self.endpoint)
            .credentials_provider(credentials)
            .force_path_style(true)
    }

    /// Local S3 (SeaweedFS) from docker-compose.yml, overridable via `ANT_TEST_S3_*`.
    ///
    /// The prefix becomes `<prefix>-t<unix secs>-<random>` (unique per run; old ones
    /// are swept by `testing::sweep_s3_prefixes`); tests that write hold a
    /// `testing::S3PrefixGuard` on the returned config.
    pub fn for_tests(prefix: &str) -> Self {
        crate::testing::sweep_s3_once();
        Self::for_tests_raw(&format!(
            "{prefix}-t{}-{}",
            crate::testing::unix_secs(),
            rand::random::<u64>()
        ))
    }

    /// [`Self::for_tests`] with the prefix exactly as given.
    pub(crate) fn for_tests_raw(prefix: &str) -> Self {
        let prefix = prefix.to_string();
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        Self {
            endpoint: env("ANT_TEST_S3_ENDPOINT", "http://localhost:9009"),
            region: "us-east-1".into(),
            bucket: env("ANT_TEST_S3_BUCKET", "ant-test"),
            prefix,
            access_key: env("ANT_TEST_S3_ACCESS_KEY", "ant"),
            secret_key: env("ANT_TEST_S3_SECRET_KEY", "ant"),
            timeouts: S3Timeouts::default(),
        }
    }
}

/// A stored pack ([`S3ObjectStore::list_packs`]).
#[derive(Debug, Clone, Copy)]
pub(crate) struct PackInfo {
    pub id: [u8; 16],
    pub size: u64,
    /// Last modification, Unix seconds (S3 clock).
    pub modified: f64,
}

#[derive(Clone)]
pub struct S3ObjectStore {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    timeouts: S3Timeouts,
    /// Request slots (`max_concurrency`).
    slots: Arc<Semaphore>,
    /// Deletes the prefix's objects with the last clone (scratch stores only).
    _cleanup: Option<Arc<crate::testing::S3PrefixGuard>>,
}

/// Outcome of one failed attempt.
enum Attempt {
    /// Transient: try again, after `after` if the server asked for a pause.
    Retry {
        why: String,
        reason: Reason,
        after: Option<Duration>,
    },
    Fatal(StoreError),
}

impl Attempt {
    /// A failure while receiving the response body.
    fn body(why: String) -> Self {
        Attempt::Retry { why, reason: Reason::Body, after: None }
    }
}

fn retry_after(resp: &HttpResponse) -> Option<Duration> {
    let secs: f64 = resp.headers().get("retry-after")?.trim().parse().ok()?;
    (secs.is_finite() && secs >= 0.0).then(|| Duration::from_secs_f64(secs).min(MAX_RETRY_AFTER))
}

/// Sort an SDK failure into retryable and fatal.
fn classify<E>(op: &str, what: &str, e: SdkError<E, HttpResponse>) -> Attempt
where
    E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
{
    let status = e.raw_response().map(|r| r.status().as_u16());
    let after = e.raw_response().and_then(retry_after);
    let reason = match &e {
        SdkError::TimeoutError(_) => Some(Reason::Timeout),
        SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => Some(Reason::Network),
        SdkError::ServiceError(s) => {
            let code = s.err().code();
            if matches!(status, Some(429 | 503))
                || matches!(code, Some("SlowDown" | "Throttling" | "ThrottlingException"))
            {
                Some(Reason::Throttled)
            } else if status.is_some_and(|s| matches!(s, 408 | 425 | 500..=599)) {
                Some(Reason::ServerError)
            } else {
                None
            }
        }
        _ => None,
    };
    let why = DisplayErrorContext(&e).to_string();
    match reason {
        Some(reason) => Attempt::Retry { why, reason, after },
        None => Attempt::Fatal(backend_error(format!("s3 {op} {what}: {why}"))),
    }
}

/// A [`StoreError::Backend`] carrying `msg` (failed requests, corrupt bodies).
fn backend_error(msg: impl Into<String>) -> StoreError {
    StoreError::Backend(msg.into().into())
}

impl S3ObjectStore {
    pub async fn connect(cfg: &S3Config) -> Result<Self> {
        let conf = cfg
            .sdk_config()
            // Timeouts and retries are ours (`retrying`), around the body read.
            .retry_config(RetryConfig::disabled())
            .timeout_config(TimeoutConfig::builder().connect_timeout(cfg.timeouts.connect).build())
            .build();
        Ok(Self {
            client: aws_sdk_s3::Client::from_conf(conf),
            bucket: cfg.bucket.clone(),
            prefix: cfg.prefix.clone(),
            timeouts: cfg.timeouts,
            slots: Arc::new(Semaphore::new(cfg.timeouts.max_concurrency.max(1))),
            _cleanup: None,
        })
    }

    /// [`Self::connect`] to a throw-away prefix whose objects are deleted when
    /// the last clone of the store is dropped, even on panic. Meant for tests
    /// (pass a [`S3Config::for_tests`] config).
    pub async fn connect_scratch(cfg: &S3Config) -> Result<Self> {
        let mut store = Self::connect(cfg).await?;
        store._cleanup = Some(Arc::new(crate::testing::S3PrefixGuard(cfg.clone())));
        Ok(store)
    }

    /// Requests this store keeps in flight at most.
    pub fn max_concurrency(&self) -> usize {
        self.timeouts.max_concurrency.max(1)
    }

    /// Run `attempt` until it succeeds, fails for good, or the policy
    /// ([`S3Timeouts`]) is exhausted. Each attempt holds a request slot and,
    /// body included, is bounded by `attempt_timeout` (waiting for the slot
    /// is not); dropping a timed-out attempt cancels it, which releases
    /// whatever it held.
    async fn retrying<T, F, Fut>(
        &self,
        op: &'static str,
        what: &str,
        attempt_timeout: Duration,
        mut attempt: F,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, Attempt>>,
    {
        let t = self.timeouts;
        let started = tokio::time::Instant::now();
        let run = async {
            let mut n = 0u32;
            let mut first_failure: Option<tokio::time::Instant> = None;
            loop {
                n += 1;
                let outcome = {
                    let waiting = GaugeGuard::new(&QUEUED);
                    let _slot = self.slots.acquire().await.map_err(StoreError::backend)?;
                    drop(waiting);
                    let _running = GaugeGuard::new(&IN_FLIGHT);
                    let kind = match op {
                        "get" | "get_range" => 0,
                        "put" => 1,
                        "head" => 2,
                        _ => 3,
                    };
                    REQUESTS[kind].fetch_add(1, Ordering::Relaxed);
                    match tokio::time::timeout(attempt_timeout, attempt()).await {
                        Ok(r) => r,
                        Err(_) => Err(Attempt::Retry {
                            why: format!("attempt timed out after {attempt_timeout:?}"),
                            reason: Reason::Timeout,
                            after: None,
                        }),
                    }
                };
                let (why, reason, after) = match outcome {
                    Ok(v) => return Ok(v),
                    Err(Attempt::Fatal(e)) => return Err(e),
                    Err(Attempt::Retry { why, reason, after }) => (why, reason, after),
                };
                let failing = first_failure.get_or_insert_with(tokio::time::Instant::now).elapsed();
                let jitter = t.backoff(n);
                let pause = after.map_or(jitter, |a| a.max(jitter));
                let attempts_left = t.max_attempts == 0 || n < t.max_attempts;
                if !attempts_left
                    || failing + pause > t.retry_budget
                    || started.elapsed() + pause >= t.operation
                {
                    GAVE_UP.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(op, what, attempts = n, "s3 request failed, giving up: {why}");
                    return Err(backend_error(format!(
                        "s3 {op} {what}: gave up after {n} attempt(s) in {:?}: {why}",
                        started.elapsed()
                    )));
                }
                RETRIES[reason as usize].fetch_add(1, Ordering::Relaxed);
                // Under throttling many requests retry once or twice: log the
                // stubborn ones (the counters have them all).
                if n >= 3 {
                    tracing::warn!(
                        op,
                        what,
                        attempt = n,
                        ?pause,
                        "s3 request failed, retrying: {why}"
                    );
                } else {
                    tracing::debug!(
                        op,
                        what,
                        attempt = n,
                        ?pause,
                        "s3 request failed, retrying: {why}"
                    );
                }
                tokio::time::sleep(pause).await;
            }
        };
        match tokio::time::timeout(t.operation, run).await {
            Ok(r) => r,
            Err(_) => {
                GAVE_UP.fetch_add(1, Ordering::Relaxed);
                Err(backend_error(format!(
                    "s3 {op} {what}: operation timed out after {:?}",
                    t.operation
                )))
            }
        }
    }

    pub async fn ensure_bucket(&self) -> Result<()> {
        let (client, bucket) = (&self.client, &self.bucket);
        self.retrying("create_bucket", bucket, self.timeouts.attempt, || async move {
            match client.create_bucket().bucket(bucket).send().await {
                Ok(_) => Ok(()),
                // SeaweedFS (no identities configured) reports an existing bucket as
                // `BucketAlreadyExists` rather than `BucketAlreadyOwnedByYou`; both mean
                // the bucket is usable, so creation is idempotent.
                Err(e) => match e.as_service_error() {
                    Some(s)
                        if s.is_bucket_already_owned_by_you() || s.is_bucket_already_exists() =>
                    {
                        Ok(())
                    }
                    _ => Err(classify("create_bucket", bucket, e)),
                },
            }
        })
        .await
    }

    /// Readiness probe: `HEAD` on the bucket (cheap; no listing, no body).
    /// One attempt, bounded by the attempt timeout: a probe should report a
    /// struggling store, not retry through it.
    pub async fn ping(&self) -> Result<()> {
        let probe = self.client.head_bucket().bucket(&self.bucket).send();
        match tokio::time::timeout(self.timeouts.attempt, probe).await {
            Ok(r) => r.map(|_| ()).map_err(StoreError::backend),
            Err(_) => Err(backend_error(format!(
                "s3 head_bucket: timed out after {:?}",
                self.timeouts.attempt
            ))),
        }
    }

    /// Object key for `id`.
    pub fn key(&self, id: &ObjectId) -> String {
        let hex = id.to_hex();
        if self.prefix.is_empty() {
            format!("{}/{}", &hex[..2], hex)
        } else {
            format!("{}/{}/{}", self.prefix.trim_end_matches('/'), &hex[..2], hex)
        }
    }

    /// Key of pack `pack` (see [`crate::packed`]).
    pub(crate) fn pack_key(&self, pack: &[u8; 16]) -> String {
        format!("{}{}", self.packs_prefix(), hex::encode(pack))
    }

    /// Prefix of every pack key.
    fn packs_prefix(&self) -> String {
        if self.prefix.is_empty() {
            "packs/".to_owned()
        } else {
            format!("{}/packs/", self.prefix.trim_end_matches('/'))
        }
    }

    /// One page (up to 1000) of the stored packs, and the token of the next
    /// page. Keys under the packs prefix that are not pack ids are skipped.
    pub(crate) async fn list_packs(
        &self,
        token: Option<String>,
    ) -> Result<(Vec<PackInfo>, Option<String>)> {
        let prefix = self.packs_prefix();
        let (client, bucket, p) = (&self.client, &self.bucket, &prefix);
        let resp = self
            .retrying("list", &prefix, self.timeouts.attempt, || {
                let token = token.clone();
                async move {
                    client
                        .list_objects_v2()
                        .bucket(bucket)
                        .prefix(p)
                        .set_continuation_token(token)
                        .send()
                        .await
                        .map_err(|e| classify("list", p, e))
                }
            })
            .await?;
        let mut packs = Vec::new();
        for o in resp.contents() {
            let Some(hex) = o.key().and_then(|k| k.strip_prefix(prefix.as_str())) else {
                continue;
            };
            let mut id = [0u8; 16];
            if hex.len() != 32 || hex::decode_to_slice(hex, &mut id).is_err() {
                continue;
            }
            let modified = o.last_modified().map_or(0.0, |t| t.as_secs_f64());
            packs.push(PackInfo { id, size: o.size().unwrap_or(0).max(0) as u64, modified });
        }
        let next = if resp.is_truncated() == Some(true) {
            resp.next_continuation_token().map(str::to_owned)
        } else {
            None
        };
        Ok((packs, next))
    }

    /// Delete packs (one request per 1000).
    pub(crate) async fn delete_packs(&self, packs: &[[u8; 16]]) -> Result<()> {
        use aws_sdk_s3::types::{Delete, ObjectIdentifier};
        for chunk in packs.chunks(1000) {
            let objects = chunk
                .iter()
                .map(|p| ObjectIdentifier::builder().key(self.pack_key(p)).build())
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(StoreError::backend)?;
            let delete = Delete::builder()
                .set_objects(Some(objects))
                .quiet(true)
                .build()
                .map_err(StoreError::backend)?;
            // Deleting is idempotent: a retry after a lost response is harmless.
            let (client, bucket, delete) = (&self.client, &self.bucket, &delete);
            let resp = self
                .retrying("delete_objects", bucket, self.timeouts.attempt, || async move {
                    client
                        .delete_objects()
                        .bucket(bucket)
                        .delete(delete.clone())
                        .send()
                        .await
                        .map_err(|e| classify("delete_objects", bucket, e))
                })
                .await?;
            if let Some(e) = resp.errors().first() {
                return Err(backend_error(format!(
                    "deleting pack {}: {}",
                    e.key().unwrap_or("?"),
                    e.message().unwrap_or("error")
                )));
            }
        }
        Ok(())
    }

    /// Key of `rel` in the persistent server cache (`crate::server_cache`):
    /// `{prefix}/cache/{rel}`, beside (never inside) the object and pack
    /// keys.
    fn cache_key(&self, rel: &str) -> String {
        if self.prefix.is_empty() {
            format!("cache/{rel}")
        } else {
            format!("{}/cache/{rel}", self.prefix.trim_end_matches('/'))
        }
    }

    /// The body of cache key `rel`, `None` when absent (`crate::server_cache`).
    pub async fn cache_get(&self, rel: &str) -> Result<Option<Bytes>> {
        let (client, bucket) = (&self.client, &self.bucket);
        let key = self.cache_key(rel);
        let key = &key;
        self.retrying("get", key, self.timeouts.attempt_for(UNKNOWN_SIZE), || async move {
            let resp = match client.get_object().bucket(bucket).key(key).send().await {
                Ok(r) => r,
                Err(e) if e.as_service_error().is_some_and(|s| s.is_no_such_key()) => {
                    return Ok(None);
                }
                Err(e) => return Err(classify("get", key, e)),
            };
            let expected = resp.content_length().and_then(|n| u64::try_from(n).ok());
            let body = resp
                .body
                .collect()
                .await
                .map_err(|e| Attempt::body(format!("reading body: {e}")))?
                .into_bytes();
            READ_BYTES.fetch_add(body.len() as u64, Ordering::Relaxed);
            if expected.is_some_and(|n| n != body.len() as u64) {
                return Err(Attempt::body(format!("short read ({} bytes)", body.len())));
            }
            Ok(Some(body))
        })
        .await
    }

    /// Store `body` at cache key `rel` (`crate::server_cache`).
    pub async fn cache_put(&self, rel: &str, body: Bytes) -> Result<()> {
        self.put_bytes(&self.cache_key(rel), body).await
    }

    /// Every cache key under `rel_prefix`: (key relative to the cache
    /// prefix, size, last modified as Unix seconds).
    pub async fn cache_list(&self, rel_prefix: &str) -> Result<Vec<(String, u64, f64)>> {
        let prefix = self.cache_key(rel_prefix);
        let base = self.cache_key("");
        let (client, bucket, p) = (&self.client, &self.bucket, &prefix);
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let resp = self
                .retrying("list", p, self.timeouts.attempt, || {
                    let token = token.clone();
                    async move {
                        client
                            .list_objects_v2()
                            .bucket(bucket)
                            .prefix(p)
                            .set_continuation_token(token)
                            .send()
                            .await
                            .map_err(|e| classify("list", p, e))
                    }
                })
                .await?;
            for o in resp.contents() {
                let Some(rel) = o.key().and_then(|k| k.strip_prefix(base.as_str())) else {
                    continue;
                };
                let modified = o.last_modified().map_or(0.0, |t| t.as_secs_f64());
                out.push((rel.to_owned(), o.size().unwrap_or(0).max(0) as u64, modified));
            }
            token = match resp.is_truncated() {
                Some(true) => resp.next_continuation_token().map(str::to_owned),
                _ => None,
            };
            if token.is_none() {
                return Ok(out);
            }
        }
    }

    /// Delete cache keys (relative to the cache prefix), one request per
    /// 1000; absent keys are fine.
    pub async fn cache_delete(&self, rels: &[String]) -> Result<()> {
        use aws_sdk_s3::types::{Delete, ObjectIdentifier};
        for chunk in rels.chunks(1000) {
            let objects = chunk
                .iter()
                .map(|r| ObjectIdentifier::builder().key(self.cache_key(r)).build())
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(StoreError::backend)?;
            let delete = Delete::builder()
                .set_objects(Some(objects))
                .quiet(true)
                .build()
                .map_err(StoreError::backend)?;
            let (client, bucket, delete) = (&self.client, &self.bucket, &delete);
            self.retrying("delete_objects", bucket, self.timeouts.attempt, || async move {
                client
                    .delete_objects()
                    .bucket(bucket)
                    .delete(delete.clone())
                    .send()
                    .await
                    .map_err(|e| classify("delete_objects", bucket, e))
            })
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn put_raw(&self, key: String, body: Vec<u8>) -> Result<()> {
        self.put_bytes(&key, Bytes::from(body)).await
    }

    /// PUT `body` at `key`; retried, since a key holds one fixed content.
    async fn put_bytes(&self, key: &str, body: Bytes) -> Result<()> {
        let (client, bucket) = (&self.client, &self.bucket);
        let timeout = self.timeouts.attempt_for(body.len() as u64);
        self.retrying("put", key, timeout, || {
            let body = body.clone();
            async move {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(ByteStream::from(body))
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|e| classify("put", key, e))
            }
        })
        .await
    }

    /// `len` bytes at `offset` of object `key`. Bounded and retried as a
    /// whole (request, headers and body): a server that throttles or stalls
    /// mid-body costs one attempt, not the caller.
    pub(crate) async fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Bytes> {
        self.get_range_inner(key, offset, len, false).await
    }

    /// Like [`Self::get_range`], but a range reaching past the end of the
    /// object returns the bytes up to its end (`offset` must lie inside it).
    /// The length is checked against the object size the server reports
    /// (`Content-Range`), so a cut-off body is still retried, never taken
    /// for the end of the object.
    pub(crate) async fn get_range_upto(&self, key: &str, offset: u64, len: u64) -> Result<Bytes> {
        self.get_range_inner(key, offset, len, true).await
    }

    async fn get_range_inner(&self, key: &str, offset: u64, len: u64, upto: bool) -> Result<Bytes> {
        let (client, bucket) = (&self.client, &self.bucket);
        let range = format!("bytes={}-{}", offset, offset + len.max(1) - 1);
        let range = &range;
        let timeout = self.timeouts.attempt_for(len);
        self.retrying("get_range", key, timeout, || async move {
            let resp = client
                .get_object()
                .bucket(bucket)
                .key(key)
                .range(range)
                .send()
                .await
                .map_err(|e| classify("get_range", key, e))?;
            // The object's size, from `Content-Range: bytes a-b/size`.
            let size = resp
                .content_range()
                .and_then(|r| r.rsplit_once('/'))
                .and_then(|(_, size)| size.trim().parse::<u64>().ok());
            let expected = match (upto, size) {
                (true, Some(size)) => len.min(size.saturating_sub(offset)),
                _ => len,
            };
            let body = resp
                .body
                .collect()
                .await
                .map_err(|e| Attempt::body(format!("reading body: {e}")))?
                .into_bytes();
            READ_BYTES.fetch_add(body.len() as u64, Ordering::Relaxed);
            if body.len() as u64 != expected {
                return Err(Attempt::body(format!(
                    "short range read ({} of {expected} bytes)",
                    body.len()
                )));
            }
            Ok(body)
        })
        .await
    }

    /// Decompress and verify one stored body.
    pub(crate) fn decode_object(id: ObjectId, compressed: &[u8]) -> Result<EncodedObject> {
        let raw = zstd::stream::decode_all(compressed).map_err(StoreError::backend)?;
        let obj = EncodedObject::from_bytes(Bytes::from(raw))
            .map_err(|e| backend_error(format!("object {id}: {e}")))?;
        obj.verify(&id).map_err(|e| backend_error(format!("object {id}: {e}")))?;
        Ok(obj)
    }

    async fn get_one(&self, id: ObjectId) -> Result<Option<EncodedObject>> {
        let (client, bucket) = (&self.client, &self.bucket);
        let key = self.key(&id);
        let key = &key;
        let compressed: Option<Bytes> = self
            .retrying("get", key, self.timeouts.attempt_for(UNKNOWN_SIZE), || async move {
                let resp = match client.get_object().bucket(bucket).key(key).send().await {
                    Ok(r) => r,
                    Err(e) if e.as_service_error().is_some_and(|s| s.is_no_such_key()) => {
                        return Ok(None);
                    }
                    Err(e) => return Err(classify("get", key, e)),
                };
                let body = resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| Attempt::body(format!("reading body: {e}")))?
                    .into_bytes();
                READ_BYTES.fetch_add(body.len() as u64, Ordering::Relaxed);
                Ok(Some(body))
            })
            .await?;
        let Some(compressed) = compressed else {
            return Ok(None);
        };
        let compressed_len = compressed.len();
        let decode = move || Self::decode_object(id, &compressed);
        // Large bodies are CPU-heavy to decompress and hash; keep them off the
        // async worker threads.
        let obj = if compressed_len > BLOCKING_THRESHOLD {
            tokio::task::spawn_blocking(decode).await.map_err(StoreError::backend)??
        } else {
            decode()?
        };
        Ok(Some(obj))
    }

    async fn put_one(&self, id: ObjectId, obj: &EncodedObject) -> Result<()> {
        let compressed = if obj.as_bytes().len() > BLOCKING_THRESHOLD {
            let body = obj.clone();
            tokio::task::spawn_blocking(move || {
                zstd::stream::encode_all(&body.as_bytes()[..], ZSTD_LEVEL)
            })
            .await
            .map_err(StoreError::backend)?
            .map_err(StoreError::backend)?
        } else {
            zstd::stream::encode_all(&obj.as_bytes()[..], ZSTD_LEVEL)
                .map_err(StoreError::backend)?
        };
        self.put_bytes(&self.key(&id), Bytes::from(compressed)).await
    }

    async fn has_one(&self, id: ObjectId) -> Result<bool> {
        let (client, bucket) = (&self.client, &self.bucket);
        let key = self.key(&id);
        let key = &key;
        self.retrying("head", key, self.timeouts.attempt, || async move {
            match client.head_object().bucket(bucket).key(key).send().await {
                Ok(_) => Ok(true),
                Err(e) if e.as_service_error().is_some_and(|s| s.is_not_found()) => Ok(false),
                Err(e) => Err(classify("head", key, e)),
            }
        })
        .await
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        stream::iter(ids.iter().copied())
            .map(|id| self.get_one(id))
            .buffered(FANOUT)
            .try_collect()
            .await
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        // `EncodedObject` clones are cheap (shared `Bytes`); owning the pairs
        // keeps the buffered stream's closure free of higher-ranked borrows.
        let owned: Vec<EncodedObject> = objects.to_vec();
        let hash_all = move || {
            let mut seen = HashSet::with_capacity(owned.len());
            owned
                .into_iter()
                .filter_map(|o| {
                    let id = o.id();
                    seen.insert(id).then_some((id, o))
                })
                .collect::<Vec<(ObjectId, EncodedObject)>>()
        };
        // Hashing a big batch is CPU-heavy: keep it off the async worker threads.
        let unique =
            if objects.iter().map(|o| o.as_bytes().len()).sum::<usize>() > BLOCKING_THRESHOLD {
                tokio::task::spawn_blocking(hash_all).await.map_err(StoreError::backend)?
            } else {
                hash_all()
            };
        stream::iter(unique)
            .map(|(id, o)| async move { self.put_one(id, &o).await })
            .buffered(FANOUT)
            .try_collect::<Vec<()>>()
            .await?;
        Ok(())
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        stream::iter(ids.iter().copied())
            .map(|id| self.has_one(id))
            .buffered(FANOUT)
            .try_collect()
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use std::sync::atomic::Ordering;
    use std::time::Instant;

    use super::*;
    use crate::conformance;
    use crate::s3_fake::{Act, byte_at, config, fast_timeouts, spawn};

    async fn fresh() -> S3ObjectStore {
        let cfg = S3Config::for_tests(&format!("t{}", rand::random::<u64>()));
        let s = S3ObjectStore::connect_scratch(&cfg).await.expect("connect");
        s.ensure_bucket().await.expect("bucket (is `docker compose up -d` running?)");
        s
    }

    #[tokio::test]
    #[ignore = "needs s3: docker compose up -d"]
    async fn s3_object_store_conforms() {
        conformance::object_store(Arc::new(fresh().await)).await;
    }

    #[tokio::test]
    #[ignore = "needs s3: docker compose up -d"]
    async fn key_layout() {
        let s = fresh().await;
        let id = ObjectId([0xab; 32]);
        let k = s.key(&id);
        assert!(k.ends_with(&format!("/ab/{}", id.to_hex())), "{k}");
        assert!(k.starts_with(&s.prefix));
    }

    const LEN: u64 = 4096;

    fn expected(offset: u64) -> Vec<u8> {
        (offset..offset + LEN).map(byte_at).collect()
    }

    async fn store_for(script: Vec<Act>, attempts: u32) -> (S3ObjectStore, crate::s3_fake::Fake) {
        let fake = spawn(script).await;
        let s = S3ObjectStore::connect(&config(&fake, fast_timeouts(attempts))).await.unwrap();
        (s, fake)
    }

    #[tokio::test]
    async fn stalled_body_times_out_and_gives_up() {
        let (s, fake) = store_for(vec![Act::Stall], 3).await;
        let t = Instant::now();
        let err = s.get_range("k", 10, LEN).await.unwrap_err();
        // 3 attempts x 300 ms plus short backoffs; never a hang.
        assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
        assert_eq!(fake.hits.load(Ordering::SeqCst), 3);
        assert!(matches!(err, StoreError::Backend(_)));
        let msg = err.to_string();
        assert!(msg.contains("gave up after 3") && msg.contains("timed out"), "{msg}");
    }

    #[tokio::test]
    async fn stall_then_success_returns_correct_bytes() {
        let (s, fake) = store_for(vec![Act::Stall, Act::Stall, Act::Ok], 4).await;
        let got = s.get_range("k", 1000, LEN).await.unwrap();
        assert_eq!(&got[..], &expected(1000)[..]);
        assert_eq!(fake.hits.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn throttled_then_success() {
        let (s, fake) = store_for(vec![Act::Throttle(None), Act::Throttle(None), Act::Ok], 4).await;
        let got = s.get_range("k", 0, LEN).await.unwrap();
        assert_eq!(&got[..], &expected(0)[..]);
        assert_eq!(fake.hits.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_after_is_honoured() {
        let (s, fake) = store_for(vec![Act::Throttle(Some(1)), Act::Ok], 3).await;
        let t = Instant::now();
        let got = s.get_range("k", 7, LEN).await.unwrap();
        assert_eq!(&got[..], &expected(7)[..]);
        assert!(t.elapsed() >= Duration::from_secs(1), "{:?}", t.elapsed());
        assert_eq!(fake.hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn persistent_throttling_is_a_backend_error() {
        let (s, fake) = store_for(vec![Act::Throttle(None)], 3).await;
        let err = s.get_range("k", 0, LEN).await.unwrap_err();
        assert!(matches!(err, StoreError::Backend(_)));
        assert!(err.to_string().contains("gave up after 3"), "{err}");
        assert_eq!(fake.hits.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn truncated_body_is_retried() {
        let (s, fake) = store_for(vec![Act::Truncate, Act::Ok], 3).await;
        let got = s.get_range("k", 5, LEN).await.unwrap();
        assert_eq!(&got[..], &expected(5)[..]);
        assert_eq!(fake.hits.load(Ordering::SeqCst), 2);

        let (s, fake) = store_for(vec![Act::Truncate], 3).await;
        assert!(matches!(s.get_range("k", 5, LEN).await, Err(StoreError::Backend(_))));
        assert_eq!(fake.hits.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn ranges_past_the_end_stop_there_only_when_asked() {
        let want = |from: u64, to: u64| (from..to).map(byte_at).collect::<Vec<u8>>();
        // Up to the end: the bytes the object has.
        let (s, fake) = store_for(vec![Act::Sized(1000)], 3).await;
        assert_eq!(&s.get_range_upto("k", 900, 500).await.unwrap()[..], &want(900, 1000)[..]);
        assert_eq!(&s.get_range_upto("k", 0, 1000).await.unwrap()[..], &want(0, 1000)[..]);
        assert_eq!(&s.get_range_upto("k", 10, 20).await.unwrap()[..], &want(10, 30)[..]);
        assert_eq!(fake.hits.load(Ordering::SeqCst), 3);
        // A body cut off before the end the server named is retried, never
        // taken for the end of the object.
        let (s, fake) = store_for(vec![Act::SizedTruncate(1000), Act::Sized(1000)], 3).await;
        assert_eq!(&s.get_range_upto("k", 800, 500).await.unwrap()[..], &want(800, 1000)[..]);
        assert_eq!(fake.hits.load(Ordering::SeqCst), 2);
        let (s, _) = store_for(vec![Act::SizedTruncate(1000)], 2).await;
        assert!(matches!(s.get_range_upto("k", 800, 500).await, Err(StoreError::Backend(_))));
        // A plain ranged read still wants every byte it asked for.
        let (s, fake) = store_for(vec![Act::Sized(1000)], 2).await;
        assert!(matches!(s.get_range("k", 900, 500).await, Err(StoreError::Backend(_))));
        assert_eq!(fake.hits.load(Ordering::SeqCst), 2);
        // Without a size from the server, a short body is an error.
        let (s, _) = store_for(vec![Act::Truncate], 2).await;
        assert!(matches!(s.get_range_upto("k", 0, 100).await, Err(StoreError::Backend(_))));
    }

    #[tokio::test]
    async fn reset_connection_is_retried() {
        let (s, fake) = store_for(vec![Act::Reset, Act::Reset, Act::Ok], 4).await;
        let got = s.get_range("k", 3, LEN).await.unwrap();
        assert_eq!(&got[..], &expected(3)[..]);
        assert_eq!(fake.hits.load(Ordering::SeqCst), 3);

        let (s, fake) = store_for(vec![Act::Reset], 2).await;
        assert!(matches!(s.get_range("k", 3, LEN).await, Err(StoreError::Backend(_))));
        assert_eq!(fake.hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn client_errors_are_not_retried() {
        let (s, fake) = store_for(vec![Act::Status(403)], 5).await;
        let err = s.get_range("k", 0, LEN).await.unwrap_err();
        assert!(matches!(err, StoreError::Backend(_)));
        assert_eq!(fake.hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn operation_timeout_bounds_total_time() {
        let fake = spawn(vec![Act::Stall]).await;
        let mut t = fast_timeouts(1000);
        t.operation = Duration::from_millis(1200);
        let s = S3ObjectStore::connect(&config(&fake, t)).await.unwrap();
        let start = Instant::now();
        let err = s.get_range("k", 0, LEN).await.unwrap_err();
        assert!(start.elapsed() < Duration::from_secs(3), "{:?}", start.elapsed());
        assert!(matches!(err, StoreError::Backend(_)));
    }

    #[tokio::test]
    async fn put_and_head_are_retried() {
        let (s, fake) = store_for(vec![Act::Throttle(None), Act::Reset, Act::Ok], 4).await;
        s.put_raw("p/packs/x".into(), vec![1, 2, 3]).await.unwrap();
        assert_eq!(fake.hits.load(Ordering::SeqCst), 3);

        let (s, fake) = store_for(vec![Act::Throttle(None), Act::Ok], 3).await;
        let id = ObjectId([7; 32]);
        assert_eq!(s.has_many(&[id]).await.unwrap(), vec![true]);
        assert_eq!(fake.hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn ping_does_not_retry() {
        let (s, fake) = store_for(vec![Act::Throttle(None)], 5).await;
        let t = Instant::now();
        assert!(s.ping().await.is_err());
        assert!(t.elapsed() < Duration::from_secs(2));
        assert_eq!(fake.hits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn attempt_timeout_scales_with_size() {
        let t = S3Timeouts::default();
        assert_eq!(t.attempt_for(0), Duration::from_secs(30));
        assert_eq!(t.attempt_for(100_000_000), Duration::from_secs(40));
    }

    #[test]
    fn default_policy_rides_out_throttling() {
        let t = S3Timeouts::default();
        // Tens of seconds of transient failures, not ~1 s of attempts.
        assert!(t.retry_budget >= Duration::from_secs(30), "{t:?}");
        assert_eq!(t.max_attempts, 0, "the budget, not an attempt count, decides");
        assert!(t.backoff_cap >= Duration::from_secs(2) && t.backoff_cap <= t.retry_budget);
        assert!(t.operation > t.retry_budget);
        assert_eq!(t.max_concurrency, DEFAULT_MAX_CONCURRENCY);
    }

    #[test]
    fn backoff_is_full_jitter_below_a_cap() {
        let t = S3Timeouts::default();
        let mut short = 0;
        for n in 1..=40u32 {
            let ceiling = (t.backoff_base * 2u32.pow(n.min(20) - 1)).min(t.backoff_cap);
            for _ in 0..50 {
                let p = t.backoff(n);
                assert!(p <= ceiling, "attempt {n}: {p:?} > {ceiling:?}");
                if n >= 10 && p < t.backoff_cap / 2 {
                    short += 1;
                }
            }
        }
        // Full jitter: late pauses spread over the whole range, not its top.
        assert!(short > 0);
    }

    #[tokio::test]
    async fn throttling_storm_within_the_budget_succeeds() {
        // What SeaweedFS and S3 do under load: 429, 503 SlowDown, bodies
        // cut off mid-stream, resets and 5xx, for a while.
        let script = vec![
            Act::Throttle(None),
            Act::Status(503),
            Act::Truncate,
            Act::Reset,
            Act::Status(500),
            Act::Throttle(None),
            Act::Truncate,
            Act::Status(503),
            Act::Truncate,
            Act::Throttle(None),
            Act::Ok,
        ];
        let before = stats();
        let (s, fake) = store_for(script, 0).await;
        let got = s.get_range("k", 11, LEN).await.unwrap();
        assert_eq!(&got[..], &expected(11)[..]);
        assert_eq!(fake.hits.load(Ordering::SeqCst), 11);
        let after = stats();
        // Other tests of this process count too: at least ours.
        let delta = |i: usize| after.retries[i] - before.retries[i];
        assert!(delta(Reason::Throttled as usize) >= 5, "{after:?}");
        assert!(delta(Reason::Body as usize) >= 3, "{after:?}");
        assert!(delta(Reason::ServerError as usize) >= 1, "{after:?}");
        assert!(delta(Reason::Network as usize) >= 1, "{after:?}");
    }

    #[tokio::test]
    async fn permanent_throttling_gives_up_when_the_budget_is_spent() {
        let fake = spawn(vec![Act::Throttle(None)]).await;
        let mut t = fast_timeouts(0);
        t.retry_budget = Duration::from_secs(1);
        let s = S3ObjectStore::connect(&config(&fake, t)).await.unwrap();
        let before = stats().gave_up;
        let start = Instant::now();
        let err = s.get_range("k", 0, LEN).await.unwrap_err();
        let took = start.elapsed();
        assert!(matches!(err, StoreError::Backend(_)));
        assert!(err.to_string().contains("gave up after"), "{err}");
        // Retried for the budget (less at most one pause), then stopped.
        assert!(took >= Duration::from_millis(850), "{took:?}");
        assert!(took < Duration::from_secs(3), "{took:?}");
        assert!(fake.hits.load(Ordering::SeqCst) > 5);
        assert!(stats().gave_up > before);
    }

    #[tokio::test]
    async fn retry_after_beyond_the_budget_gives_up_at_once() {
        let fake = spawn(vec![Act::Throttle(Some(5)), Act::Ok]).await;
        let mut t = fast_timeouts(0);
        t.retry_budget = Duration::from_secs(1);
        let s = S3ObjectStore::connect(&config(&fake, t)).await.unwrap();
        let start = Instant::now();
        assert!(matches!(s.get_range("k", 0, LEN).await, Err(StoreError::Backend(_))));
        assert!(start.elapsed() < Duration::from_secs(1), "{:?}", start.elapsed());
        assert_eq!(fake.hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn not_found_is_not_retried() {
        let (s, fake) = store_for(vec![Act::Status(404)], 0).await;
        assert!(matches!(s.get_range("k", 0, LEN).await, Err(StoreError::Backend(_))));
        assert_eq!(fake.hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn requests_in_flight_are_bounded() {
        let fake = spawn(vec![Act::Slow(30)]).await;
        let mut t = fast_timeouts(1);
        t.max_concurrency = 4;
        let s = S3ObjectStore::connect(&config(&fake, t)).await.unwrap();
        assert_eq!(s.max_concurrency(), 4);
        // A burst far wider than the limit, through clones (they share it).
        let reads = (0..48u64).map(|i| {
            let s = s.clone();
            async move { s.get_range("k", i * 100, LEN).await.map(|b| (i, b)) }
        });
        for r in futures::future::join_all(reads).await {
            let (i, body) = r.unwrap();
            assert_eq!(&body[..], &expected(i * 100)[..]);
        }
        assert_eq!(fake.hits.load(Ordering::SeqCst), 48);
        let peak = fake.peak.load(Ordering::SeqCst);
        assert!(peak <= 4, "peak {peak}");
        assert!(peak >= 2, "the burst should run concurrently: peak {peak}");
    }

    #[tokio::test]
    async fn retries_wait_for_a_slot_again() {
        // Throttled requests do not keep their slot through the pause, and
        // never exceed the limit when they come back.
        let script = (0..16).map(|_| Act::Throttle(None)).chain([Act::Slow(10)]).collect();
        let fake = spawn(script).await;
        let mut t = fast_timeouts(0);
        t.max_concurrency = 2;
        let s = S3ObjectStore::connect(&config(&fake, t)).await.unwrap();
        let reads = (0..8u64).map(|i| {
            let s = s.clone();
            async move { s.get_range("k", i, LEN).await }
        });
        for r in futures::future::join_all(reads).await {
            r.unwrap();
        }
        assert!(fake.peak.load(Ordering::SeqCst) <= 2);
        assert_eq!(fake.hits.load(Ordering::SeqCst), 16 + 8);
    }
}
