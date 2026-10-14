//! A minimal in-process shared HTTP cache (a stand-in for a CDN or caching
//! proxy) for benchmarks: `GET` requests are forwarded to the origin and
//! 200 responses whose `Cache-Control` says `public` (and neither `private`
//! nor `no-store`) are kept in memory forever, keyed by path and query.
//! Counts origin requests and cache hits.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use parking_lot::Mutex;

/// Response headers kept and replayed.
const KEPT: [HeaderName; 3] = [header::CONTENT_TYPE, header::CACHE_CONTROL, header::ETAG];

#[derive(Clone)]
struct Entry {
    headers: Vec<(HeaderName, axum::http::HeaderValue)>,
    body: Bytes,
}

#[derive(Default)]
pub struct Stats {
    pub origin_requests: AtomicU64,
    pub hits: AtomicU64,
}

#[derive(Clone)]
struct Proxy {
    origin: String,
    http: reqwest::Client,
    cache: Arc<Mutex<HashMap<String, Entry>>>,
    stats: Arc<Stats>,
}

pub struct Cdn {
    pub url: String,
    pub stats: Arc<Stats>,
    /// A real cache tier (`ant cache-serve`) instead of the toy proxy.
    pub tier: Option<Arc<ant_cache::CacheServe>>,
}

impl Cdn {
    pub fn origin_requests(&self) -> u64 {
        if let Some(t) = &self.tier {
            return t.metrics().upstream_total();
        }
        self.stats.origin_requests.load(Ordering::Relaxed)
    }

    pub fn hits(&self) -> u64 {
        if self.tier.is_some() {
            return 0;
        }
        self.stats.hits.load(Ordering::Relaxed)
    }
}

/// Start the cache on an ephemeral 127.0.0.1 port in front of `origin`.
pub async fn spawn(origin: &str) -> Result<Cdn> {
    let stats = Arc::new(Stats::default());
    let proxy = Proxy {
        origin: origin.trim_end_matches('/').to_owned(),
        http: reqwest::Client::new(),
        cache: Arc::default(),
        stats: stats.clone(),
    };
    let app = Router::new().fallback(handle).with_state(proxy);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("cdn: {e}");
        }
    });
    Ok(Cdn { url, stats, tier: None })
}

/// Start `ant_cache::CacheServe` (open mode, disk cache in `dir`) in front of
/// `origin` on an ephemeral port.
pub async fn spawn_tier(
    origin: &str,
    dir: &std::path::Path,
    auth: ant_cache::AuthMode,
    no_batch: bool,
) -> Result<Cdn> {
    let disk = Arc::new(antd::disk::DiskObjectStore::open(dir, u64::MAX / 2)?);
    let tier = ant_cache::CacheServe::connect(ant_cache::Config {
        upstream: origin.to_owned(),
        auth,
        disk,
        retry: antd::retry::RetryPolicy::none(),
        authz_ttl: ant_cache::DEFAULT_AUTHZ_TTL,
        authz_window: ant_cache::DEFAULT_AUTHZ_WINDOW,
    })
    .await?;
    if no_batch {
        tier.disable_batch_checks();
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let t2 = tier.clone();
    tokio::spawn(async move {
        if let Err(e) = t2.serve(listener).await {
            eprintln!("cache tier: {e}");
        }
    });
    Ok(Cdn { url, stats: Arc::default(), tier: Some(tier) })
}

fn shareable(headers: &HeaderMap) -> bool {
    let cc = headers
        .get(header::CACHE_CONTROL)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    cc.contains("public") && !cc.contains("private") && !cc.contains("no-store")
}

async fn handle(State(p): State<Proxy>, req: Request) -> Response {
    let key = req.uri().path_and_query().map(|pq| pq.as_str().to_owned()).unwrap_or_default();
    if let Some(e) = p.cache.lock().get(&key).cloned() {
        p.stats.hits.fetch_add(1, Ordering::Relaxed);
        let mut res = (StatusCode::OK, e.body).into_response();
        res.headers_mut().extend(e.headers);
        return res;
    }
    p.stats.origin_requests.fetch_add(1, Ordering::Relaxed);
    let mut up = p.http.get(format!("{}{key}", p.origin));
    if let Some(a) = req.headers().get(header::AUTHORIZATION) {
        up = up.header(header::AUTHORIZATION, a);
    }
    let resp = match up.send().await {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    };
    let status = resp.status();
    let headers: Vec<_> = resp
        .headers()
        .iter()
        .filter(|(k, _)| KEPT.contains(k) || k.as_str().starts_with("x-ant-"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let cacheable = status == StatusCode::OK && shareable(resp.headers());
    let body = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => return (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    };
    if cacheable {
        p.cache.lock().insert(key, Entry { headers: headers.clone(), body: body.clone() });
    }
    let mut res = (status, body).into_response();
    res.headers_mut().extend(headers);
    res
}
