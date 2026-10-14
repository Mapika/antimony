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
//!   `SlowDown`), 5xx, connection errors, timeouts and truncated bodies are
//!   retried with jittered exponential backoff (honouring `Retry-After`) up to
//!   `max_attempts`; GET, HEAD and PUT of content-addressed keys are
//!   idempotent. The SDK's own retries are disabled so attempts do not
//!   multiply. After the last attempt a `StoreError::Backend` names the
//!   operation, key and last failure.
//! - `ensure_bucket` creates the bucket if it does not exist ("already owned
//!   by you" counts as success).

use std::{collections::HashSet, future::Future, time::Duration};

use ant_core::{EncodedObject, ObjectId};
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{
    config::{retry::RetryConfig, timeout::TimeoutConfig},
    error::{DisplayErrorContext, SdkError},
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};

use crate::{BLOCKING_THRESHOLD, ObjectStore, Result, StoreError, ZSTD_LEVEL};

/// Maximum number of concurrent S3 requests per batch call.
const FANOUT: usize = 64;
/// Size assumed for the per-attempt timeout of a read whose size is unknown.
const UNKNOWN_SIZE: u64 = 16 * 1024 * 1024;
/// Longest pause between attempts, including a server's `Retry-After`.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

type HttpResponse = aws_sdk_s3::config::http::HttpResponse;

/// Timeout and retry policy of the S3 client (see the module docs).
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
    /// Everything about one operation, retries and pauses included.
    pub operation: Duration,
    /// Attempts per operation (1 = no retry).
    pub max_attempts: u32,
}

impl Default for S3Timeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(5),
            attempt: Duration::from_secs(30),
            min_throughput: 10_000_000,
            operation: Duration::from_secs(300),
            max_attempts: 5,
        }
    }
}

impl S3Timeouts {
    /// Time one attempt moving `bytes` bytes may take.
    fn attempt_for(&self, bytes: u64) -> Duration {
        self.attempt + Duration::from_secs_f64(bytes as f64 / self.min_throughput.max(1) as f64)
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
    pub fn for_tests(prefix: &str) -> Self {
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        Self {
            endpoint: env("ANT_TEST_S3_ENDPOINT", "http://localhost:9009"),
            region: "us-east-1".into(),
            bucket: env("ANT_TEST_S3_BUCKET", "ant-test"),
            prefix: prefix.to_string(),
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
}

/// Outcome of one failed attempt.
enum Attempt {
    /// Transient: try again, after `after` if the server asked for a pause.
    Retry {
        why: String,
        after: Option<Duration>,
    },
    Fatal(StoreError),
}

fn retry_after(resp: &HttpResponse) -> Option<Duration> {
    let secs: f64 = resp.headers().get("retry-after")?.trim().parse().ok()?;
    (secs.is_finite() && secs >= 0.0).then(|| Duration::from_secs_f64(secs).min(MAX_BACKOFF))
}

/// Sort an SDK failure into retryable and fatal.
fn classify<E>(op: &str, what: &str, e: SdkError<E, HttpResponse>) -> Attempt
where
    E: std::error::Error + Send + Sync + 'static,
{
    let status = e.raw_response().map(|r| r.status().as_u16());
    let after = e.raw_response().and_then(retry_after);
    let transient_status = |s: u16| matches!(s, 408 | 425 | 429 | 500..=504);
    let retry = match &e {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
            true
        }
        SdkError::ServiceError(_) => status.is_some_and(transient_status),
        _ => false,
    };
    let why = DisplayErrorContext(&e).to_string();
    if retry {
        Attempt::Retry { why, after }
    } else {
        Attempt::Fatal(corrupt(format!("s3 {op} {what}: {why}")))
    }
}

/// Pause after failed attempt `n`: 100 ms doubling to 5 s, jittered over its
/// upper half.
fn backoff(n: u32) -> Duration {
    let cap =
        Duration::from_millis(100u64 << n.saturating_sub(1).min(6)).min(Duration::from_secs(5));
    cap.mul_f64(0.5 + 0.5 * rand::random::<f64>())
}

fn corrupt(msg: impl Into<String>) -> StoreError {
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
        })
    }

    /// Run `attempt` until it succeeds, fails for good, or the policy
    /// ([`S3Timeouts`]) is exhausted. Each attempt, body included, is bounded
    /// by `attempt_timeout`; dropping a timed-out attempt cancels it, which
    /// releases whatever it held.
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
            loop {
                n += 1;
                let outcome = match tokio::time::timeout(attempt_timeout, attempt()).await {
                    Ok(r) => r,
                    Err(_) => Err(Attempt::Retry {
                        why: format!("attempt timed out after {attempt_timeout:?}"),
                        after: None,
                    }),
                };
                let (why, after) = match outcome {
                    Ok(v) => return Ok(v),
                    Err(Attempt::Fatal(e)) => return Err(e),
                    Err(Attempt::Retry { why, after }) => (why, after),
                };
                let pause = after.map_or_else(|| backoff(n), |a| a.max(backoff(n)));
                if n >= t.max_attempts.max(1) || started.elapsed() + pause >= t.operation {
                    return Err(corrupt(format!(
                        "s3 {op} {what}: gave up after {n} attempt(s) in {:?}: {why}",
                        started.elapsed()
                    )));
                }
                tracing::warn!(op, what, attempt = n, ?pause, "s3 request failed, retrying: {why}");
                tokio::time::sleep(pause).await;
            }
        };
        match tokio::time::timeout(t.operation, run).await {
            Ok(r) => r,
            Err(_) => {
                Err(corrupt(format!("s3 {op} {what}: operation timed out after {:?}", t.operation)))
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
            Err(_) => {
                Err(corrupt(format!("s3 head_bucket: timed out after {:?}", self.timeouts.attempt)))
            }
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
                return Err(corrupt(format!(
                    "deleting pack {}: {}",
                    e.key().unwrap_or("?"),
                    e.message().unwrap_or("error")
                )));
            }
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
            let body = resp
                .body
                .collect()
                .await
                .map_err(|e| Attempt::Retry { why: format!("reading body: {e}"), after: None })?
                .into_bytes();
            if body.len() as u64 != len {
                return Err(Attempt::Retry {
                    why: format!("short range read ({} of {len} bytes)", body.len()),
                    after: None,
                });
            }
            Ok(body)
        })
        .await
    }

    /// Decompress and verify one stored body.
    pub(crate) fn decode_object(id: ObjectId, compressed: &[u8]) -> Result<EncodedObject> {
        let raw = zstd::stream::decode_all(compressed).map_err(StoreError::backend)?;
        let obj = EncodedObject::from_bytes(Bytes::from(raw))
            .map_err(|e| corrupt(format!("object {id}: {e}")))?;
        obj.verify(&id).map_err(|e| corrupt(format!("object {id}: {e}")))?;
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
                let body = resp.body.collect().await.map_err(|e| Attempt::Retry {
                    why: format!("reading body: {e}"),
                    after: None,
                })?;
                Ok(Some(body.into_bytes()))
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
        let s = S3ObjectStore::connect(&cfg).await.expect("connect");
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
}
