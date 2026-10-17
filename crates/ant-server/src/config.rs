//! Store flags shared by `ant-server` and the `ant` CLI.

use std::path::PathBuf;
use std::sync::Arc;

use ant_store::local::{LocalOptions, LocalStore, LocalSync};
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

/// Concurrency limits of a local-mode server are those of a server with a
/// pool of this many connections (64 reads, 32 writes, 256 lands in flight).
pub const LOCAL_LIMIT_POOL: u32 = 16;

/// Backend selection flags (`--memory` | `--local [<dir>]` | `--pg-url` +
/// optional `--s3-*`).
#[derive(clap::Args, Debug, Clone)]
pub struct StoreArgs {
    /// Use the in-memory backend (dev/testing). Exclusive with --pg-url.
    #[arg(long, env = "ANT_MEMORY")]
    memory: bool,

    /// Local mode: every store in one data directory (SQLite metadata and
    /// local pack files; no Postgres or S3). Without a value:
    /// `$ANT_LOCAL_DIR`, else `$XDG_DATA_HOME/antimony/local` (default
    /// `~/.local/share/antimony/local`). One process uses a directory at a
    /// time.
    #[arg(long, env = "ANT_LOCAL", num_args = 0..=1, default_missing_value = "",
          value_name = "DIR")]
    local: Option<PathBuf>,

    /// Local mode: SQLite reader connections (reads beyond wait).
    #[arg(long, env = "ANT_LOCAL_READERS", default_value_t = 4,
          value_parser = clap::value_parser!(u32).range(1..=64))]
    local_readers: u32,

    /// Local mode: decoded pack frames kept in memory, MiB.
    #[arg(long, env = "ANT_LOCAL_FRAME_CACHE_MIB", default_value_t = 16)]
    local_frame_cache_mib: usize,

    /// Local mode: `full` fsyncs every acknowledged write and land (the
    /// default); `normal` never fsyncs at commit: nothing is lost when the
    /// process crashes, but an OS crash or power loss can lose the last
    /// acknowledged lands.
    #[arg(long, env = "ANT_LOCAL_SYNC", default_value = "full", value_parser = ["full", "normal"])]
    local_sync: String,

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

    /// Longest one S3 operation may take, waiting for a request slot,
    /// retries and pauses included.
    #[arg(long, env = "ANT_S3_OPERATION_TIMEOUT", default_value = "5m", value_parser = parse_duration)]
    s3_operation_timeout: std::time::Duration,

    /// How long an S3 operation keeps retrying transient failures
    /// (throttling, 5xx, connection errors, timeouts, cut-off bodies),
    /// counted from its first failure. Pauses back off exponentially with
    /// full jitter, up to 5 s each (longer if the server sends Retry-After).
    #[arg(long, env = "ANT_S3_RETRY_BUDGET", default_value = "60s", value_parser = parse_duration)]
    s3_retry_budget: std::time::Duration,

    /// Attempts per S3 operation at most (1 = no retries; 0 = no limit, the
    /// retry budget decides).
    #[arg(long, env = "ANT_S3_MAX_ATTEMPTS", default_value_t = 0)]
    s3_max_attempts: u32,

    /// S3 requests in flight per process at most; more wait for a slot, so
    /// bursts queue here instead of overrunning (and being throttled by)
    /// the object store.
    #[arg(long, env = "ANT_S3_MAX_CONCURRENCY", default_value_t = ant_store::s3::DEFAULT_MAX_CONCURRENCY as u32, value_parser = clap::value_parser!(u32).range(1..))]
    s3_max_concurrency: u32,

    /// Objects whose encoded size is at most this many bytes live in Postgres;
    /// larger ones go to S3 (only with --s3-*).
    #[arg(long, env = "ANT_INLINE_THRESHOLD", default_value_t = DEFAULT_INLINE_THRESHOLD)]
    inline_threshold: usize,

    /// Compress similar large objects together in multi-object frames of S3
    /// packs (`false`: store each object alone, the format servers before
    /// 0.2.0 read; for the window of a rolling upgrade).
    #[arg(long, env = "ANT_PACK_FRAMES", default_value_t = true, action = clap::ArgAction::Set)]
    pack_frames: bool,

    /// Compressed S3 pack bytes kept in memory, MiB (0: off). Records are
    /// read as aligned 64 KiB segments, nearby ones with one ranged GET, and
    /// kept: a checkout or clone against a freshly started server reads
    /// each segment of the packs it needs about once, instead of one GET per
    /// object, while those packs fit.
    #[arg(long, env = "ANT_S3_PACK_CACHE_MIB", default_value_t = ant_store::packed::DEFAULT_SEGMENT_CACHE_BYTES >> 20)]
    s3_pack_cache_mib: usize,
}

impl StoreArgs {
    /// Whether local mode is selected (`--local`).
    pub fn is_local(&self) -> bool {
        self.local.is_some()
    }

    /// The local data directory, when local mode is selected.
    pub fn local_dir(&self) -> anyhow::Result<Option<PathBuf>> {
        match &self.local {
            None => Ok(None),
            Some(d) if !d.as_os_str().is_empty() => Ok(Some(d.clone())),
            Some(_) => match ant_store::local::default_dir() {
                Some(d) => Ok(Some(d)),
                None => bail!("--local: no directory given and $HOME is not set"),
            },
        }
    }

    /// Options of the local store (`--local`).
    pub fn local_options(&self) -> LocalOptions {
        LocalOptions {
            readers: self.local_readers as usize,
            inline_threshold: self.inline_threshold,
            frame_cache_bytes: self.local_frame_cache_mib.saturating_mul(1 << 20),
            pack_frames: self.pack_frames,
            migrate: !self.no_migrate,
            sync: if self.local_sync == "normal" { LocalSync::Normal } else { LocalSync::Full },
            ..LocalOptions::default()
        }
    }

    /// The basis of the default concurrency limits (`crate::limits`): the
    /// Postgres query-pool size, or [`LOCAL_LIMIT_POOL`] in local mode.
    pub fn limit_pool(&self) -> u32 {
        if self.is_local() { LOCAL_LIMIT_POOL } else { self.pg_max_connections }
    }

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
                retry_budget: self.s3_retry_budget,
                max_concurrency: self.s3_max_concurrency as usize,
                ..S3Timeouts::default()
            },
        }))
    }

    pub async fn backend(&self) -> anyhow::Result<Backend> {
        Ok(self.backend_with_probes().await?.0)
    }

    /// [`Self::backend`] plus the Postgres and S3 stores (pool metrics and
    /// readiness probes), where configured.
    pub async fn backend_with_probes(&self) -> anyhow::Result<(Backend, Probes)> {
        if let Some(dir) = self.local_dir()? {
            if self.memory || self.pg_url.is_some() {
                bail!("--local, --memory and --pg-url are mutually exclusive");
            }
            if self.s3_config()?.is_some() {
                bail!("--s3-* flags require --pg-url, not --local");
            }
            let options = self.local_options();
            let store = tokio::task::spawn_blocking(move || LocalStore::open(&dir, &options))
                .await
                .context("opening the local data directory")?
                .context("opening the local data directory")?;
            let backend = store.backend();
            return Ok((backend, Probes { local: Some(store), ..Probes::default() }));
        }
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
                        let mut packed = PackedS3Store::new(pg.as_ref().clone(), s3)
                            .with_segment_cache_bytes(
                                self.s3_pack_cache_mib.saturating_mul(1 << 20),
                            );
                        if !self.pack_frames {
                            packed = packed.with_frame_bytes(0);
                        }
                        Arc::new(TieredObjectStore::new(
                            pg.clone(),
                            Arc::new(packed),
                            self.inline_threshold,
                        ))
                    }
                };
                Ok((
                    Backend { objects, refs: pg.clone(), meta: pg.clone() },
                    Probes { pg: Some(pg), s3: s3_probe, ..Probes::default() },
                ))
            }
            (true, Some(_)) => bail!("--memory and --pg-url are mutually exclusive"),
            (false, None) => bail!("exactly one of --memory, --local or --pg-url is required"),
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
