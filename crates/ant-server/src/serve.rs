//! Server startup shared by the `ant-server` binary and `ant serve`, plus
//! [`spawn_local`] for in-process servers on ephemeral ports.
//!
//! Flags (clap, each also readable from env `ANT_*`):
//!   --grpc-listen <addr>   default 127.0.0.1:7420
//!   --http-listen <addr>   default 127.0.0.1:7421
//!   --metrics-listen <addr>  (ANT_METRICS_LISTEN) serve `/metrics` on its own
//!                          listener instead of the HTTP port
//!   --pg-lock-timeout <dur> (ANT_PG_LOCK_TIMEOUT) in the store flags, default 10s
//!   --log-format json|text (ANT_LOG_FORMAT) default: text on a terminal,
//!                          json otherwise; level via `RUST_LOG`
//!   --policy <file>        (ANT_POLICY) access-control policy (`ant_auth`);
//!                          without it the server runs in OPEN mode
//!   persistent caches of tree bundles and git base packs
//!   (`ant_store::server_cache`): `--cache-dir <dir>` (default in local mode:
//!   `<data dir>/cache`), `--no-cache-dir`, `--cache-dir-mib`,
//!   `--shared-cache`, `--shared-cache-mib`; warming at start: `--prewarm
//!   <ref>`, `--prewarm-git blobless|full` (env `ANT_CACHE_DIR`, ...)
//!   rate limits / load shedding (`crate::limits`): `--no-limits`,
//!   `--rate-{read,write,admin} <rps[:burst]|off>`,
//!   `--max-inflight-{read,write,land,admin} <n>`, `--shed-wait <dur>`
//!   (env `ANT_NO_LIMITS`, `ANT_RATE_*`, `ANT_MAX_INFLIGHT_*`, `ANT_SHED_WAIT`)
//!   plus the store flags of [`StoreArgs`] (`--memory` | `--pg-url` ...).

use std::io::IsTerminal;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use ant_store::Backend;
use anyhow::Context;
use clap::{Args, ValueEnum};
use tracing_subscriber::EnvFilter;

use crate::acl::Acl;
use crate::config::StoreArgs;
use crate::limits::{LimitArgs, LimitConfig, Limiter};
use crate::metrics::Metrics;
use crate::{http, rpc};

/// Log line format (`--log-format`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    /// Human-readable lines.
    Text,
    /// One JSON object per line, including the enclosing spans (and so the
    /// `request_id`) of each event.
    Json,
}

#[derive(Args, Debug)]
pub struct ServeArgs {
    /// gRPC listen address.
    #[arg(long, env = "ANT_GRPC_LISTEN", default_value = "127.0.0.1:7420")]
    pub grpc_listen: SocketAddr,

    /// Plain-HTTP object endpoint listen address.
    #[arg(long, env = "ANT_HTTP_LISTEN", default_value = "127.0.0.1:7421")]
    pub http_listen: SocketAddr,

    /// Serve Prometheus metrics (`GET /metrics`) on this address only,
    /// instead of on the HTTP port. The endpoint is unauthenticated.
    #[arg(long, env = "ANT_METRICS_LISTEN")]
    pub metrics_listen: Option<SocketAddr>,

    /// Log format: `text` or `json` (default: text on a terminal, json
    /// otherwise).
    #[arg(long, env = "ANT_LOG_FORMAT", value_enum)]
    pub log_format: Option<LogFormat>,

    /// Access-control policy file (TOML, see `ant_auth`). Without it every
    /// request is allowed.
    #[arg(long, env = "ANT_POLICY")]
    pub policy: Option<PathBuf>,

    /// Maximum glibc malloc arenas (default: the smaller of 16 and twice the
    /// CPU count; an explicit `MALLOC_ARENA_MAX` wins). Applied by
    /// [`configure_malloc`] before the runtime starts; ignored off glibc.
    /// Many arenas let freed memory pile up per thread (a git clone's peak
    /// RSS was 3x its live heap); few arenas contend on locks. 0 = leave
    /// glibc's default.
    #[arg(long, env = "ANT_MALLOC_ARENAS")]
    pub malloc_arenas: Option<usize>,

    /// Async worker threads (default: one per CPU; in local mode at most
    /// [`LOCAL_WORKER_THREADS`]).
    #[arg(long, env = "ANT_WORKER_THREADS", value_parser = clap::value_parser!(u32).range(1..=1024))]
    pub worker_threads: Option<u32>,

    /// Budget of the server's caches of trees, commits and changesets, MiB
    /// (default 104; 48 in local mode). Each cache may briefly hold twice
    /// its share.
    #[arg(long, env = "ANT_CACHE_MIB")]
    pub cache_mib: Option<usize>,

    /// Budget of the in-memory cache of served tree bundles (`GET /t/`),
    /// MiB; 0 turns it off (default 1024; 64 in local mode).
    #[arg(long, env = "ANT_BUNDLE_CACHE_MIB")]
    pub bundle_cache_mib: Option<usize>,

    /// Directory of the persistent server cache: tree bundles and git base
    /// packs survive restarts (one server process per directory; entries
    /// of different repositories are kept apart). Default: none, or
    /// `<data dir>/cache` in local mode.
    #[arg(long, env = "ANT_CACHE_DIR", value_name = "DIR")]
    pub cache_dir: Option<PathBuf>,

    /// No persistent disk cache, also in local mode.
    #[arg(long, env = "ANT_NO_CACHE_DIR", conflicts_with = "cache_dir")]
    pub no_cache_dir: bool,

    /// Budget of the persistent disk cache, MiB (least recently used
    /// entries are evicted; an entry over half of it is not kept). Default
    /// 4096; 1024 in local mode.
    #[arg(long, env = "ANT_CACHE_DIR_MIB")]
    pub cache_dir_mib: Option<u64>,

    /// Share the persistent cache between replicas: also keep entries in
    /// the store's S3 bucket, under `<s3 prefix>/cache/` (needs `--s3-*`).
    #[arg(long, env = "ANT_SHARED_CACHE")]
    pub shared_cache: bool,

    /// Budget of the shared cache in S3, MiB (oldest entries are deleted
    /// beyond it, checked every 10 minutes).
    #[arg(long, env = "ANT_SHARED_CACHE_MIB", default_value_t = 16384)]
    pub shared_cache_mib: u64,

    /// After start, bring the tree bundles of a checkout of these refs into
    /// the caches, in the background (repeat or comma-separate).
    #[arg(long, env = "ANT_PREWARM", value_delimiter = ',', value_name = "REF")]
    pub prewarm: Vec<String>,

    /// After start, load (or build) the git export's base pack for clones
    /// of all branches with this filter: `blobless` (`--filter=blob:none`)
    /// or `full` (repeat or comma-separate).
    #[arg(long, env = "ANT_PREWARM_GIT", value_delimiter = ',', value_enum)]
    pub prewarm_git: Vec<GitFilter>,

    /// Local mode: collect garbage every this long while serving (the
    /// first run one interval after start), like `ant admin gc --local`.
    #[arg(long, env = "ANT_GC_INTERVAL", value_parser = crate::config::parse_duration)]
    pub gc_interval: Option<std::time::Duration>,

    /// Grace period of the scheduled collections (`--gc-interval`): data
    /// written within it is kept. At least 1d.
    #[arg(long, env = "ANT_GC_GRACE", default_value = "7d", value_parser = crate::config::parse_duration)]
    pub gc_grace: std::time::Duration,

    /// Wait of the scheduled collections between condemning data and
    /// deleting it: longer than the caches' lifetime (10m) plus the longest
    /// request.
    #[arg(long, env = "ANT_GC_QUIESCE", default_value = "30m", value_parser = crate::config::parse_duration)]
    pub gc_quiesce: std::time::Duration,

    #[command(flatten)]
    pub limits: LimitArgs,

    #[command(flatten)]
    pub store: StoreArgs,
}

/// A clone filter of the git export (`--prewarm-git`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum GitFilter {
    /// `--filter=blob:none` clones.
    Blobless,
    /// Clones with every blob.
    Full,
}

/// Persistent disk cache budget, MiB (`--cache-dir-mib`).
pub const DEFAULT_CACHE_DIR_MIB: u64 = 4096;
/// Persistent disk cache budget in local mode, MiB.
pub const LOCAL_CACHE_DIR_MIB: u64 = 1024;
/// How often the shared cache is pruned to its budget.
const SHARED_PRUNE_EVERY: std::time::Duration = std::time::Duration::from_secs(600);

/// Async worker threads of a local-mode server at most (`--worker-threads`).
pub const LOCAL_WORKER_THREADS: usize = 4;
/// Blocking-pool threads of a local-mode server at most (SQLite reads and
/// pack-file I/O; idle ones exit after 10 s).
pub const LOCAL_BLOCKING_THREADS: usize = 64;
/// Server cache budget in local mode, MiB.
pub const LOCAL_CACHE_MIB: usize = 48;
/// Bundle cache budget in local mode, MiB (`--bundle-cache-mib`).
pub const LOCAL_BUNDLE_CACHE_MIB: usize = 64;

impl ServeArgs {
    /// Async worker threads: `--worker-threads`, else in local mode
    /// min([`LOCAL_WORKER_THREADS`], CPUs), else the runtime default.
    pub fn worker_threads(&self) -> Option<usize> {
        let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
        match self.worker_threads {
            Some(n) => Some(n as usize),
            None if self.store.is_local() => Some(LOCAL_WORKER_THREADS.min(cpus)),
            None => None,
        }
    }

    /// Malloc arenas to configure: `--malloc-arenas`, else in local mode
    /// twice the worker threads (fewer arenas, less retained memory).
    pub fn effective_malloc_arenas(&self) -> Option<usize> {
        self.malloc_arenas.or_else(|| {
            if self.store.is_local() {
                self.worker_threads().map(|n| (2 * n).clamp(2, 16))
            } else {
                None
            }
        })
    }

    /// The tokio runtime this server runs on (see [`Self::worker_threads`]).
    pub fn runtime(&self) -> std::io::Result<tokio::runtime::Runtime> {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.enable_all();
        if let Some(n) = self.worker_threads() {
            b.worker_threads(n.max(1));
        }
        if self.store.is_local() {
            b.max_blocking_threads(LOCAL_BLOCKING_THREADS);
        }
        b.build()
    }
}

/// Default arena cap: twice the CPU count, within 2..=16.
pub fn default_malloc_arenas() -> usize {
    let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
    (cpus * 2).clamp(2, 16)
}

/// Cap the glibc malloc arenas of a server process. Call it first thing in
/// `main`, before the tokio runtime (and so any worker thread) exists:
/// threads that already own an arena keep it. An explicit `MALLOC_ARENA_MAX`
/// environment variable is respected. Returns the cap applied, or `None`
/// when left alone (env var set, `0`, or not glibc).
pub fn configure_malloc(arenas: Option<usize>) -> Option<usize> {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        if std::env::var_os("MALLOC_ARENA_MAX").is_some() {
            return None;
        }
        let n = arenas.unwrap_or_else(default_malloc_arenas);
        if n == 0 {
            return None;
        }
        let n = n.min(i32::MAX as usize) as libc::c_int;
        // SAFETY: mallopt only sets allocator tunables.
        if unsafe { libc::mallopt(libc::M_ARENA_MAX, n) } == 1 {
            return Some(n as usize);
        }
        None
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    {
        let _ = arenas;
        None
    }
}

/// Install the global tracing subscriber (`RUST_LOG`, default info) writing to
/// stderr. An already installed subscriber is left alone.
pub fn init_logging(format: Option<LogFormat>) {
    let format = format.unwrap_or(if std::io::stderr().is_terminal() {
        LogFormat::Text
    } else {
        LogFormat::Json
    });
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr);
    let _ = match format {
        LogFormat::Text => builder.try_init(),
        LogFormat::Json => builder.json().try_init(),
    };
}

/// Run the server (gRPC and HTTP concurrently) until one of them fails.
/// Logs with tracing-subscriber (`RUST_LOG`, default info).
pub async fn run(args: ServeArgs) -> anyhow::Result<()> {
    init_logging(args.log_format);

    let cache_mib = args.cache_mib.or(args.store.is_local().then_some(LOCAL_CACHE_MIB));
    if let Some(mib) = cache_mib {
        crate::ops::set_cache_budget(mib.saturating_mul(1 << 20));
    }
    let bundle_mib =
        args.bundle_cache_mib.or(args.store.is_local().then_some(LOCAL_BUNDLE_CACHE_MIB));
    if let Some(mib) = bundle_mib {
        crate::bundle::set_cache_bytes(mib.saturating_mul(1 << 20));
    }
    let started = std::time::Instant::now();
    if args.gc_interval.is_some() && !args.store.is_local() {
        anyhow::bail!(
            "--gc-interval is for local mode (--local); collect Postgres stores with `ant admin gc`"
        );
    }
    let (backend, probes) = args.store.backend_with_probes().await?;
    // Local maintenance: the socket `ant admin gc|repack --local` talk to,
    // and scheduled collections. Both end with the server.
    let mut _maintenance = None;
    let mut _scheduled = None;
    if let Some(local) = &probes.local {
        tracing::info!(
            dir = %local.dir().display(),
            opened_ms = started.elapsed().as_millis() as u64,
            "local mode: SQLite metadata and pack files in one directory"
        );
        match ant_store::local::admin::serve(local.clone()) {
            Ok(s) => _maintenance = Some(s),
            Err(error) => tracing::warn!(
                %error,
                "no maintenance socket: `ant admin gc|repack --local` cannot reach this server"
            ),
        }
        if let Some(every) = args.gc_interval {
            let opts = ant_store::local::gc::GcOptions {
                grace: args.gc_grace,
                quiesce: args.gc_quiesce,
                ..ant_store::local::gc::GcOptions::default()
            };
            if opts.grace < ant_store::local::gc::MIN_GRACE {
                anyhow::bail!("--gc-grace must be at least 1d");
            }
            tracing::info!(every_secs = every.as_secs(), "scheduled garbage collection");
            _scheduled =
                Some(AbortOnDrop(ant_store::local::admin::schedule_gc(local.clone(), every, opts)));
        }
    }
    let metrics = Metrics::new();
    metrics.set_probes(probes);
    let backend = metrics.instrument(&backend);
    spawn_malloc_trim();
    let persist = open_server_cache(&args).await?;
    let caches = http::Caches::new(&backend, persist.clone());
    let acl = match &args.policy {
        Some(path) => {
            let acl = Acl::load(path)?;
            tracing::info!(policy = %path.display(), "access control enabled");
            acl
        }
        None => {
            tracing::warn!("no --policy given: OPEN mode, every request is allowed");
            Acl::open()
        }
    };

    let grpc = tokio::net::TcpListener::bind(args.grpc_listen)
        .await
        .with_context(|| format!("binding gRPC on {}", args.grpc_listen))?;
    let http_listener = tokio::net::TcpListener::bind(args.http_listen)
        .await
        .with_context(|| format!("binding HTTP on {}", args.http_listen))?;
    let metrics_listener = match args.metrics_listen {
        Some(addr) => Some(
            tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("binding metrics on {addr}"))?,
        ),
        None => None,
    };
    tracing::info!(
        grpc = %args.grpc_listen,
        http = %args.http_listen,
        metrics = %args.metrics_listen.unwrap_or(args.http_listen),
        "ant-server listening"
    );

    let limit_cfg = args.limits.config(args.store.limit_pool());
    if limit_cfg.enabled {
        tracing::info!(
            max_inflight = ?limit_cfg.max_inflight,
            shed_wait_ms = limit_cfg.shed_wait.as_millis() as u64,
            "rate limits and load shedding enabled (read, write, land, admin)"
        );
    } else {
        tracing::warn!("--no-limits: rate limiting and load shedding are off");
    }
    let limiter = Limiter::new(limit_cfg, acl.clone(), metrics.clone());
    let grpc_fut = rpc::serve_with_limiter(
        backend.clone(),
        acl.clone(),
        grpc,
        metrics.clone(),
        limiter.clone(),
    );
    let expose = metrics_listener.is_none();
    let http_metrics = metrics.clone();
    if !args.prewarm.is_empty() || !args.prewarm_git.is_empty() {
        let (b, a, c) = (backend.clone(), acl.clone(), caches.clone());
        tokio::spawn(prewarm(b, a, c, args.prewarm.clone(), args.prewarm_git.clone()));
    }
    let http_fut = async move {
        http::serve_with_caches(backend, acl, http_listener, http_metrics, expose, limiter, caches)
            .await
            .context("HTTP server failed")
    };
    let metrics_fut = async move {
        match metrics_listener {
            Some(listener) => {
                http::serve_metrics(metrics, listener).await.context("metrics server failed")
            }
            None => std::future::pending().await,
        }
    };
    let servers = async { tokio::try_join!(grpc_fut, http_fut, metrics_fut).map(|_| ()) };
    tokio::select! {
        r = servers => r,
        () = shutdown_signal() => {
            tracing::info!("SIGTERM or SIGINT: shutting down");
            Ok(())
        }
    }
}

/// Aborts a background task when dropped.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The persistent server cache the flags ask for (see [`ServeArgs::cache_dir`]),
/// if any. A disk cache that cannot be opened (in use, not writable) is
/// left out with a warning: the server runs without it.
async fn open_server_cache(
    args: &ServeArgs,
) -> anyhow::Result<Option<Arc<ant_store::server_cache::ServerCache>>> {
    use ant_store::server_cache::{DiskCache, ServerCache, SharedCache, namespace};
    let local = args.store.local_dir()?;
    let dir = match (&args.cache_dir, &local) {
        _ if args.no_cache_dir => None,
        (Some(dir), _) => Some(dir.clone()),
        (None, Some(local)) => Some(local.join("cache")),
        (None, None) => None,
    };
    let default_mib = if local.is_some() { LOCAL_CACHE_DIR_MIB } else { DEFAULT_CACHE_DIR_MIB };
    let budget = args.cache_dir_mib.unwrap_or(default_mib).saturating_mul(1 << 20);
    let ns = namespace(&args.store.identity()?);
    let disk = match dir {
        Some(dir) if budget > 0 => {
            let at = dir.clone();
            let ns = ns.clone();
            match tokio::task::spawn_blocking(move || DiskCache::open(&at, &ns, budget)).await? {
                Ok(disk) => {
                    let (entries, bytes) = disk.usage();
                    tracing::info!(
                        dir = %disk.dir().display(),
                        entries,
                        bytes,
                        budget_bytes = budget,
                        "persistent server cache"
                    );
                    Some(disk)
                }
                Err(e) => {
                    tracing::warn!(dir = %dir.display(), error = %e, "persistent server cache unavailable");
                    None
                }
            }
        }
        _ => None,
    };
    let shared = if args.shared_cache {
        let Some(cfg) = args.store.s3_config()? else {
            anyhow::bail!("--shared-cache needs the S3 store (--s3-*)");
        };
        let s3 = ant_store::s3::S3ObjectStore::connect(&cfg).await?;
        tracing::info!(bucket = %cfg.bucket, prefix = %cfg.prefix, "shared server cache in S3");
        Some(SharedCache::new(s3, args.shared_cache_mib.saturating_mul(1 << 20)))
    } else {
        None
    };
    let cache = ServerCache::new(disk, shared).map(Arc::new);
    if let Some(cache) = &cache {
        cache.spawn_pruner(SHARED_PRUNE_EVERY);
    }
    Ok(cache)
}

/// Warm the caches after start (`--prewarm`, `--prewarm-git`), one step at
/// a time: the bundles of each ref's tree, then each git base pack.
async fn prewarm(
    backend: Backend,
    acl: Acl,
    caches: http::Caches,
    refs: Vec<String>,
    git: Vec<GitFilter>,
) {
    let budget = match crate::bundle::BundleCache::default().budget() {
        0 => 256 << 20,
        n => n,
    };
    for name in refs {
        let started = std::time::Instant::now();
        let tree = async {
            let Some(commit) = backend.refs.get_ref(&name).await? else {
                return Ok::<_, ant_store::StoreError>(None);
            };
            Ok(backend.meta.get_commits(&[commit]).await?.pop().flatten().map(|n| n.tree))
        };
        let result = match tree.await {
            Ok(Some(tree)) => caches.prewarm_bundles(&backend, &acl, tree, budget).await,
            Ok(None) => {
                tracing::warn!(r#ref = name, "prewarm: no such ref (or commit)");
                continue;
            }
            Err(e) => Err(e),
        };
        match result {
            Ok((bundles, bytes)) => tracing::info!(
                r#ref = name,
                bundles,
                bytes,
                ms = started.elapsed().as_millis() as u64,
                "prewarm: tree bundles ready"
            ),
            Err(e) => tracing::warn!(r#ref = name, error = %e, "prewarm: bundles failed"),
        }
    }
    for filter in git {
        let started = std::time::Instant::now();
        let no_blobs = filter == GitFilter::Blobless;
        match caches.git().prewarm(no_blobs).await {
            Ok(fits) => tracing::info!(
                ?filter,
                base = fits,
                ms = started.elapsed().as_millis() as u64,
                "prewarm: git base pack ready"
            ),
            Err(e) => {
                tracing::warn!(?filter, error = format!("{e:#}"), "prewarm: git base pack failed")
            }
        }
    }
}

/// Resolves on SIGTERM or SIGINT (never, if the handlers cannot be set).
async fn shutdown_signal() {
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    let int = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        () = term => {}
        () = int => {}
    }
}

/// Addresses of a server started by [`spawn_local`].
#[derive(Debug, Clone, Copy)]
pub struct LocalServer {
    pub grpc: SocketAddr,
    pub http: SocketAddr,
}

impl LocalServer {
    pub fn grpc_url(&self) -> String {
        format!("http://{}", self.grpc)
    }

    pub fn http_url(&self) -> String {
        format!("http://{}", self.http)
    }
}

/// Serve gRPC and HTTP (object endpoint plus git export) for `backend` on
/// ephemeral 127.0.0.1 ports, in background tasks of the current runtime.
/// For tests, benchmarks and embedded use; logging is left to the caller.
pub async fn spawn_local(backend: Backend, acl: Acl) -> std::io::Result<LocalServer> {
    spawn_local_inner(backend, acl, Metrics::new()).await
}

/// [`spawn_local`] with the full production instrumentation: the stores are
/// wrapped with [`Metrics::instrument`] and every layer reports into
/// `metrics`, which the caller can render or scrape at `/metrics`. Callers
/// that also use `backend` directly bypass the store timing (and, since land
/// queues and caches are keyed by store identity, do not share them with the
/// server); read results through the server instead.
pub async fn spawn_local_with_metrics(
    backend: Backend,
    acl: Acl,
    metrics: Arc<Metrics>,
) -> std::io::Result<LocalServer> {
    let backend = metrics.instrument(&backend);
    spawn_local_inner(backend, acl, metrics).await
}

/// [`spawn_local_with_metrics`] with explicit rate-limit and load-shedding
/// settings (`crate::limits`; the defaults otherwise).
pub async fn spawn_local_with_limits(
    backend: Backend,
    acl: Acl,
    metrics: Arc<Metrics>,
    limits: LimitConfig,
) -> std::io::Result<LocalServer> {
    let backend = metrics.instrument(&backend);
    let limiter = Limiter::new(limits, acl.clone(), metrics.clone());
    spawn_local_with_limiter(backend, acl, metrics, limiter).await
}

async fn spawn_local_inner(
    backend: Backend,
    acl: Acl,
    metrics: Arc<Metrics>,
) -> std::io::Result<LocalServer> {
    let limiter = Limiter::new(LimitConfig::default(), acl.clone(), metrics.clone());
    spawn_local_with_limiter(backend, acl, metrics, limiter).await
}

/// [`spawn_local`] with a persistent server cache (`ant serve
/// --cache-dir`): also returns the server's caches (to prewarm or inspect).
pub async fn spawn_local_with_cache(
    backend: Backend,
    acl: Acl,
    persist: Option<Arc<ant_store::server_cache::ServerCache>>,
) -> std::io::Result<(LocalServer, http::Caches)> {
    let metrics = Metrics::new();
    let limiter = Limiter::new(LimitConfig::default(), acl.clone(), metrics.clone());
    let caches = http::Caches::new(&backend, persist);
    let server = spawn_local_with_caches(backend, acl, metrics, limiter, caches.clone()).await?;
    Ok((server, caches))
}

async fn spawn_local_with_limiter(
    backend: Backend,
    acl: Acl,
    metrics: Arc<Metrics>,
    limiter: Arc<Limiter>,
) -> std::io::Result<LocalServer> {
    let caches = http::Caches::new(&backend, None);
    spawn_local_with_caches(backend, acl, metrics, limiter, caches).await
}

async fn spawn_local_with_caches(
    backend: Backend,
    acl: Acl,
    metrics: Arc<Metrics>,
    limiter: Arc<Limiter>,
    caches: http::Caches,
) -> std::io::Result<LocalServer> {
    let grpc = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addrs = LocalServer { grpc: grpc.local_addr()?, http: http_listener.local_addr()? };
    tokio::spawn(rpc::serve_with_limiter(
        backend.clone(),
        acl.clone(),
        grpc,
        metrics.clone(),
        limiter.clone(),
    ));
    tokio::spawn(http::serve_with_caches(
        backend,
        acl,
        http_listener,
        metrics,
        true,
        limiter,
        caches,
    ));
    Ok(addrs)
}

/// Seconds between `malloc_trim` calls (`ANT_MALLOC_TRIM_SECS`, 0 = never).
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn spawn_malloc_trim() {
    let secs =
        std::env::var("ANT_MALLOC_TRIM_SECS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
    if secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs));
        loop {
            tick.tick().await;
            // SAFETY: malloc_trim only releases free heap memory to the OS.
            let _ = tokio::task::spawn_blocking(|| unsafe { libc::malloc_trim(0) }).await;
        }
    });
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn spawn_malloc_trim() {}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        serve: ServeArgs,
    }

    #[test]
    fn malloc_arenas_flag_parses_and_defaults_are_sane() {
        let cli = Cli::try_parse_from(["ant-server", "--memory", "--malloc-arenas", "4"]).unwrap();
        assert_eq!(cli.serve.malloc_arenas, Some(4));
        let cli = Cli::try_parse_from(["ant-server", "--memory"]).unwrap();
        assert_eq!(cli.serve.malloc_arenas, None);
        assert!((2..=16).contains(&default_malloc_arenas()));
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn configure_malloc_applies_cap() {
        // mallopt reports success; an explicit env var wins, 0 leaves glibc alone.
        assert_eq!(configure_malloc(Some(0)), None);
        if std::env::var_os("MALLOC_ARENA_MAX").is_some() {
            assert_eq!(configure_malloc(Some(7)), None);
        } else {
            assert_eq!(configure_malloc(Some(7)), Some(7));
            assert_eq!(configure_malloc(None), Some(default_malloc_arenas()));
        }
    }

    #[test]
    fn observability_flags_parse() {
        let cli = Cli::try_parse_from([
            "ant-server",
            "--memory",
            "--log-format",
            "json",
            "--metrics-listen",
            "127.0.0.1:9100",
        ])
        .unwrap();
        assert_eq!(cli.serve.log_format, Some(LogFormat::Json));
        assert_eq!(cli.serve.metrics_listen, Some("127.0.0.1:9100".parse().unwrap()));

        let cli = Cli::try_parse_from(["ant-server", "--memory"]).unwrap();
        assert_eq!(cli.serve.log_format, None);
        assert_eq!(cli.serve.metrics_listen, None);
        assert!(Cli::try_parse_from(["ant-server", "--memory", "--log-format", "xml"]).is_err());
    }

    #[test]
    fn cache_flags_parse() {
        let cli = Cli::try_parse_from([
            "ant-server",
            "--memory",
            "--cache-dir",
            "/tmp/x",
            "--cache-dir-mib",
            "10",
            "--prewarm",
            "main,dev",
            "--prewarm",
            "x",
            "--prewarm-git",
            "blobless,full",
        ])
        .unwrap();
        assert_eq!(cli.serve.cache_dir.as_deref(), Some(std::path::Path::new("/tmp/x")));
        assert_eq!(cli.serve.cache_dir_mib, Some(10));
        assert_eq!(cli.serve.prewarm, ["main", "dev", "x"]);
        assert_eq!(cli.serve.prewarm_git, [GitFilter::Blobless, GitFilter::Full]);
        assert!(!cli.serve.shared_cache);
        let cli = Cli::try_parse_from(["ant-server", "--memory"]).unwrap();
        assert!(cli.serve.cache_dir.is_none() && cli.serve.prewarm.is_empty());
        assert!(
            Cli::try_parse_from(["ant-server", "--memory", "--cache-dir", "x", "--no-cache-dir"])
                .is_err()
        );
        assert!(Cli::try_parse_from(["ant-server", "--memory", "--prewarm-git", "none"]).is_err());
    }

    #[test]
    fn pg_lock_timeout_flag_parses() {
        use std::time::Duration;
        // Defaults and overrides are checked through the clap command: the
        // field is private to `StoreArgs`, so assert parse success/failure.
        assert!(Cli::try_parse_from(["ant-server", "--memory"]).is_ok());
        assert!(
            Cli::try_parse_from(["ant-server", "--memory", "--pg-lock-timeout", "250ms"]).is_ok()
        );
        assert!(Cli::try_parse_from(["ant-server", "--memory", "--pg-lock-timeout", "0"]).is_err());
        assert!(Cli::try_parse_from(["ant-server", "--memory", "--pg-lock-timeout", "x"]).is_err());
        assert_eq!(crate::config::parse_duration("250ms"), Ok(Duration::from_millis(250)));
    }
}
