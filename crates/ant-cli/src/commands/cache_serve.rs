//! `ant cache-serve`: the cache tier process (`ant_cache`).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ant_cache::{AuthMode, CacheServe, Config};
use antd::disk::DiskObjectStore;
use antd::retry::RetryPolicy;
use anyhow::{Context as _, Result, bail};

use crate::args::{CacheAuth, CacheServeArgs};

/// How often the cache enforces its budget and refreshes its gauges.
const MAINTENANCE: Duration = Duration::from_secs(60);

pub async fn run(args: CacheServeArgs, cache: Option<PathBuf>, cache_bytes: u64) -> Result<()> {
    ant_server::serve::init_logging(None);
    // The default client cache holds whatever this user may read; a cache
    // tier must have a directory of its own.
    let Some(cache) = cache else {
        bail!("cache-serve needs its own cache directory: pass --cache <dir> (or set ANT_CACHE)");
    };
    let disk = Arc::new(
        DiskObjectStore::open(&cache, cache_bytes)
            .with_context(|| format!("opening cache {}", cache.display()))?,
    );
    let auth = match args.auth {
        CacheAuth::Forward => AuthMode::Forward,
        CacheAuth::Open => AuthMode::Open,
    };
    let serve = CacheServe::connect(Config {
        upstream: args.upstream.clone(),
        auth,
        disk,
        retry: RetryPolicy::default(),
    })
    .await?;
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    tracing::info!(
        listen = %args.listen,
        upstream = %args.upstream,
        auth = ?args.auth,
        cache = %cache.display(),
        cache_bytes,
        "cache-serve listening"
    );
    serve.spawn_maintenance(MAINTENANCE);
    serve.serve(listener).await.context("HTTP server failed")
}
