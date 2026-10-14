//! Store flags shared by `ant-server` and the `ant` CLI.

use std::sync::Arc;

use ant_store::packed::PackedS3Store;
use ant_store::postgres::{PgOptions, PgStore};
use ant_store::s3::{S3Config, S3ObjectStore, S3Timeouts};
use ant_store::tiered::{DEFAULT_INLINE_THRESHOLD, TieredObjectStore};
use ant_store::{Backend, ObjectStore};
use anyhow::{Context, bail};

use crate::health::Probes;

/// Parse `500ms`, `10s`, `2m`, `1h` or a bare number of seconds (non-zero).
pub fn parse_duration(text: &str) -> Result<std::time::Duration, String> {
    let t = text.trim();
    let (num, unit_ms) = if let Some(n) = t.strip_suffix("ms") {
        (n, 1u64)
    } else if let Some(n) = t.strip_suffix('s') {
        (n, 1000)
    } else if let Some(n) = t.strip_suffix('m') {
        (n, 60_000)
    } else if let Some(n) = t.strip_suffix('h') {
        (n, 3_600_000)
    } else {
        (t, 1000)
    };
    let n: u64 = num.trim().parse().map_err(|_| format!("invalid duration {text:?}"))?;
    let ms = n.checked_mul(unit_ms).ok_or_else(|| format!("duration {text:?} too large"))?;
    if ms == 0 {
        return Err("duration must be greater than zero".into());
    }
    Ok(std::time::Duration::from_millis(ms))
}

/// Backend selection flags (`--memory` | `--pg-url` + optional `--s3-*`).
#[derive(clap::Args, Debug, Clone)]
pub struct StoreArgs {
    /// Use the in-memory backend (dev/testing). Exclusive with --pg-url.
    #[arg(long, env = "ANT_MEMORY")]
    memory: bool,

    /// Postgres URL for refs, metadata and small objects.
    #[arg(long, env = "ANT_PG_URL")]
    pg_url: Option<String>,

    /// Postgres schema holding the tables.
    #[arg(long, env = "ANT_PG_SCHEMA", default_value = "ant")]
    pg_schema: String,

    /// Postgres query-pool size per process. Replicas sharing a database
    /// need (max + lock connections) x replicas under its max_connections.
    #[arg(long, env = "ANT_PG_MAX_CONNECTIONS", default_value_t = 64)]
    pg_max_connections: u32,

    /// Postgres connections for per-ref land locks (refs landed on at once;
    /// 0 disables the locks, fine for a single server process).
    #[arg(long, env = "ANT_PG_LOCK_CONNECTIONS", default_value_t = 8)]
    pg_lock_connections: u32,

    /// Longest wait for a per-ref land lock (`10s`, `500ms`, `2m`; a bare
    /// number is seconds). On expiry the land falls back to a plain
    /// compare-and-swap. Ignored with `--pg-lock-connections 0`.
    #[arg(long, env = "ANT_PG_LOCK_TIMEOUT", default_value = "10s", value_parser = parse_duration)]
    pg_lock_timeout: std::time::Duration,

    /// Do not create or migrate the Postgres schema: fail unless it is
    /// already at this version's layout (`ant admin schema --migrate`).
    #[arg(long, env = "ANT_NO_MIGRATE")]
    no_migrate: bool,

    /// S3 endpoint for large objects.
    #[arg(long, env = "ANT_S3_ENDPOINT")]
    s3_endpoint: Option<String>,

    /// S3 bucket for large objects.
    #[arg(long, env = "ANT_S3_BUCKET")]
    s3_bucket: Option<String>,

    /// Key prefix inside the bucket.
    #[arg(long, env = "ANT_S3_PREFIX", default_value = "")]
    s3_prefix: String,

    /// S3 access key id.
    #[arg(long, env = "ANT_S3_ACCESS_KEY")]
    s3_access_key: Option<String>,

    /// S3 secret access key.
    #[arg(long, env = "ANT_S3_SECRET_KEY")]
    s3_secret_key: Option<String>,

    /// S3 region.
    #[arg(long, env = "ANT_S3_REGION", default_value = "us-east-1")]
    s3_region: String,

    /// S3 connect timeout (`5s`, `500ms`; a bare number is seconds).
    #[arg(long, env = "ANT_S3_CONNECT_TIMEOUT", default_value = "5s", value_parser = parse_duration)]
    s3_connect_timeout: std::time::Duration,

    /// Fixed part of the per-attempt S3 timeout, which covers the whole
    /// response body; the time for the bytes (--s3-min-throughput) is added.
    #[arg(long, env = "ANT_S3_TIMEOUT", default_value = "30s", value_parser = parse_duration)]
    s3_attempt_timeout: std::time::Duration,

    /// Slowest acceptable S3 transfer rate in bytes per second; sizes the
    /// per-attempt timeout of large reads and writes.
    #[arg(long, env = "ANT_S3_MIN_THROUGHPUT", default_value_t = 10_000_000)]
    s3_min_throughput: u64,

    /// Longest one S3 operation may take, retries included.
    #[arg(long, env = "ANT_S3_OPERATION_TIMEOUT", default_value = "5m", value_parser = parse_duration)]
    s3_operation_timeout: std::time::Duration,

    /// Attempts per S3 operation (1 = no retries). Throttling, 5xx,
    /// connection errors, timeouts and truncated bodies are retried.
    #[arg(long, env = "ANT_S3_MAX_ATTEMPTS", default_value_t = 5, value_parser = clap::value_parser!(u32).range(1..))]
    s3_max_attempts: u32,

    /// Objects whose encoded size is at most this many bytes live in Postgres;
    /// larger ones go to S3 (only with --s3-*).
    #[arg(long, env = "ANT_INLINE_THRESHOLD", default_value_t = DEFAULT_INLINE_THRESHOLD)]
    inline_threshold: usize,
}

impl StoreArgs {
    /// The Postgres query-pool size (also the basis of the default
    /// concurrency limits, `crate::limits`).
    pub fn pg_max_connections(&self) -> u32 {
        self.pg_max_connections
    }

    /// The Postgres URL, when a Postgres backend is selected.
    pub fn pg_url(&self) -> Option<&str> {
        self.pg_url.as_deref()
    }

    /// The Postgres schema holding the tables.
    pub fn pg_schema(&self) -> &str {
        &self.pg_schema
    }

    pub fn s3_config(&self) -> anyhow::Result<Option<S3Config>> {
        let any = self.s3_endpoint.is_some()
            || self.s3_bucket.is_some()
            || self.s3_access_key.is_some()
            || self.s3_secret_key.is_some();
        if !any {
            return Ok(None);
        }
        let (Some(endpoint), Some(bucket), Some(access_key), Some(secret_key)) = (
            self.s3_endpoint.clone(),
            self.s3_bucket.clone(),
            self.s3_access_key.clone(),
            self.s3_secret_key.clone(),
        ) else {
            bail!(
                "--s3-endpoint, --s3-bucket, --s3-access-key and --s3-secret-key must be given together"
            );
        };
        Ok(Some(S3Config {
            endpoint,
            region: self.s3_region.clone(),
            bucket,
            prefix: self.s3_prefix.clone(),
            access_key,
            secret_key,
            timeouts: S3Timeouts {
                connect: self.s3_connect_timeout,
                attempt: self.s3_attempt_timeout,
                min_throughput: self.s3_min_throughput,
                operation: self.s3_operation_timeout,
                max_attempts: self.s3_max_attempts,
            },
        }))
    }

    pub async fn backend(&self) -> anyhow::Result<Backend> {
        Ok(self.backend_with_probes().await?.0)
    }

    /// [`Self::backend`] plus the Postgres and S3 stores (pool metrics and
    /// readiness probes), where configured.
    pub async fn backend_with_probes(&self) -> anyhow::Result<(Backend, Probes)> {
        match (self.memory, self.pg_url.as_deref()) {
            (true, None) => {
                if self.s3_config()?.is_some() {
                    bail!("--s3-* flags require --pg-url, not --memory");
                }
                Ok((Backend::in_memory(), Probes::default()))
            }
            (false, Some(url)) => {
                let pg = Arc::new(
                    PgStore::connect_with(
                        url,
                        &self.pg_schema,
                        &PgOptions {
                            max_connections: self.pg_max_connections,
                            lock_connections: self.pg_lock_connections,
                            lock_timeout: self.pg_lock_timeout,
                            migrate: !self.no_migrate,
                        },
                    )
                    .await
                    .context("connecting to Postgres")?,
                );
                let mut s3_probe = None;
                let objects: Arc<dyn ObjectStore> = match self.s3_config()? {
                    None => pg.clone(),
                    Some(cfg) => {
                        let s3 = S3ObjectStore::connect(&cfg).await.context("configuring S3")?;
                        s3.ensure_bucket().await.context("ensuring S3 bucket")?;
                        // Large objects are bundled into S3 packs indexed in Postgres.
                        s3_probe = Some(s3.clone());
                        let packed = PackedS3Store::new(pg.as_ref().clone(), s3);
                        Arc::new(TieredObjectStore::new(
                            pg.clone(),
                            Arc::new(packed),
                            self.inline_threshold,
                        ))
                    }
                };
                Ok((
                    Backend { objects, refs: pg.clone(), meta: pg.clone() },
                    Probes { pg: Some(pg), s3: s3_probe },
                ))
            }
            (true, Some(_)) => bail!("--memory and --pg-url are mutually exclusive"),
            (false, None) => bail!("exactly one of --memory or --pg-url is required"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::parse_duration;

    #[test]
    fn durations_parse() {
        assert_eq!(parse_duration("10s"), Ok(Duration::from_secs(10)));
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(parse_duration("1h"), Ok(Duration::from_secs(3600)));
        assert_eq!(parse_duration("7"), Ok(Duration::from_secs(7)));
        for bad in ["", "s", "0", "0ms", "-1s", "1.5s", "abc", "99999999999999999999s"] {
            assert!(parse_duration(bad).is_err(), "{bad}");
        }
    }
}
