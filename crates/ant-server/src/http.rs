//! Plain-HTTP immutable object endpoint, designed to sit behind any HTTP
//! cache or CDN.
//!
//! - `GET /o/{hex}` → 200, body = encoded object bytes,
//!   `Content-Type: application/x-ant-object`,
//!   `Cache-Control: public, max-age=31536000, immutable`, `ETag: "<hex>"`.
//!   `If-None-Match` matching the ETag → 304 with no body.
//! - Unknown id → 404 with `Cache-Control: no-store` (it may appear later).
//! - `GET /t/{hex}` → 200, a *tree bundle*: the tree, its blobs and whole
//!   subdirectories breadth first up to [`Limits`] (8 MiB, 8192 objects;
//!   blobs over 1 MiB are never bundled), framed by `ant_proto::bundle`
//!   (zstd), `Content-Type: application/x-ant-bundle`. Lets a client fetch a
//!   checkout with about one request per bundle instead of one per object,
//!   through the same caches as `/o/`: the content is a function of the
//!   tree id, so it is `Cache-Control: public, max-age=31536000, immutable`
//!   when not caller-specific (below). Unknown id or not a tree → 404
//!   (`no-store`). Every `/t/` response carries `x-ant-bundle: 1`, so a
//!   client tells this 404 from an older server without the route (whose
//!   404 lacks it) and falls back to `/o/`. Content rules: `crate::bundle`.
//! - `POST /o/check` → batch read-permission check (for cache tiers). Body: up
//!   to [`CHECK_MAX_IDS`] raw 32-byte object ids, concatenated (400 for a
//!   length that is not a multiple of 32, 413 for too many). Response 200:
//!   one byte per id, in order, `Content-Type: application/x-ant-check`,
//!   `Cache-Control: no-store`, bit 0 = `GET /o/{id}` would serve this
//!   caller the object (it exists and the caller may read it), bit 1 =
//!   it would be served `private` (outside the root zone; only set with
//!   bit 0). Authorization is exactly that of `GET /o/{id}` (same
//!   `crate::acl::read_access`): 401 without a valid token under a policy,
//!   and an object the caller may not read looks exactly like one that does
//!   not exist (both `0`), so the endpoint reveals no more than GET does.
//!   Costs one `read` rate-limit token per request, whatever the id count;
//!   absent on older servers (404/405), which clients detect and fall back
//!   from.
//! - Malformed id → 400.
//! - `GET /healthz` → 200 "ok" (liveness: static, touches nothing).
//! - `GET /readyz` → 200 `{"status":"ok","postgres":"ok","s3":"ok"}` when the
//!   configured dependencies answer (`crate::health`; in-memory: just
//!   `{"status":"ok"}`), else 503 with the failing ones as `down`/`timeout`.
//! - `GET /metrics` → 200 Prometheus/OpenMetrics text (`crate::metrics`); absent
//!   here when `--metrics-listen` serves it on its own listener.
//! - `GET /debug/stats` → 200 JSON of process-local cache, land queue and
//!   catch-up sizes (for soak monitoring). Under a policy it needs an admin
//!   token (401 without a valid one, 403 for a non-admin).
//!
//! Access control (`crate::acl`, only with a policy): `/o/{hex}` needs
//! `Authorization: Bearer <token>` (or Basic with the token as password),
//! else 401; unreadable objects get exactly the 404 of missing ones; objects
//! outside the root zone are served `Cache-Control: private, ...`. `/t/{hex}`
//! needs the same token and a readable tree (else 401 / 404). A `public`
//! bundle never holds an object outside the root zone (such objects, and
//! subdirectories listing one, are left out; the client fetches them by
//! `/o/` or their own bundle) and never depends on the caller. Whenever the
//! caller lacks read access to something it would hold, or the tree itself
//! lies outside the root zone, unreadable objects are left out and the
//! response is `private, no-store` (`bundle_visibility`). The git
//! export (`/repo.git/...`) takes Basic auth (401 carries
//! `WWW-Authenticate: Basic` so git asks its credential helpers) and needs
//! `can_read_everything`, else 403. `/healthz`, `/readyz` and `/metrics` stay open.
//!
//! Every response carries `x-request-id` (the request's own or a fresh ULID);
//! requests are counted and timed per route (`crate::observe`).

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

use ant_core::ObjectId;
use ant_store::{Backend, StoreError};
use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use bytes::Bytes;

use crate::acl::{self, Access, Acl, AuthError};
use crate::bundle::{self, Limits};
use crate::limits::{LimitConfig, Limiter, http_limit};
use crate::metrics::Metrics;
use crate::observe::{Observer, http_observe};

const CONTENT_TYPE: &str = "application/x-ant-object";
const CACHE_IMMUTABLE: &str = "public, max-age=31536000, immutable";
const CACHE_PRIVATE: &str = "private, max-age=31536000, immutable";
const CACHE_NO_STORE: &str = "no-store";
/// Bundles whose content depends on the caller: never stored by any cache.
const CACHE_PRIVATE_NO_STORE: &str = "private, no-store";
/// `WWW-Authenticate` challenge for the object and stats endpoints.
const BEARER_CHALLENGE: &str = "Bearer realm=\"antimony\"";

/// Serve the HTTP object endpoint plus the read-only git export
/// (`/repo.git`) on an already-bound listener until it fails, enforcing
/// `acl` and recording HTTP metrics, request ids and slow-request logs
/// (`crate::observe`) into `metrics`; `expose_metrics` serves `GET /metrics`
/// on this listener.
pub async fn serve_with_metrics(
    backend: Backend,
    acl: Acl,
    listener: tokio::net::TcpListener,
    metrics: Arc<Metrics>,
    expose_metrics: bool,
) -> std::io::Result<()> {
    let limiter = Limiter::new(LimitConfig::default(), acl.clone(), metrics.clone());
    serve_with_limiter(backend, acl, listener, metrics, expose_metrics, limiter).await
}

/// [`serve_with_metrics`] with an explicit [`Limiter`] (rate limits and load
/// shedding, `crate::limits`), shared with the gRPC front end. Rejections
/// are `429` / `503` with `Retry-After`.
pub async fn serve_with_limiter(
    backend: Backend,
    acl: Acl,
    listener: tokio::net::TcpListener,
    metrics: Arc<Metrics>,
    expose_metrics: bool,
    limiter: Arc<Limiter>,
) -> std::io::Result<()> {
    use axum::serve::ListenerExt;
    let app = app_with_metrics(backend, acl, metrics, expose_metrics, limiter);
    let listener = listener.tap_io(|tcp| {
        // Avoid Nagle/delayed-ACK stalls on small responses.
        let _ = tcp.set_nodelay(true);
    });
    axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>()).await
}

/// Serve only `GET /metrics` of `metrics` on a dedicated listener
/// (`--metrics-listen`).
pub async fn serve_metrics(
    metrics: Arc<Metrics>,
    listener: tokio::net::TcpListener,
) -> std::io::Result<()> {
    let app = Router::new().route("/metrics", get(scrape)).with_state(metrics);
    axum::serve(listener, app).await
}

/// Object endpoint plus git export, enforcing `acl`, with the observation
/// layer (metrics, request ids, slow logs); `expose_metrics` adds
/// `GET /metrics`.
fn app_with_metrics(
    backend: Backend,
    acl: Acl,
    metrics: Arc<Metrics>,
    expose_metrics: bool,
    limiter: Arc<Limiter>,
) -> Router {
    let observer = Arc::new(Observer::new(metrics.clone(), acl.clone()));
    let git = ant_gitserve::router(backend.clone());
    let mut app = router_with_state(backend, acl.clone(), metrics, expose_metrics).merge(git);
    if !acl.is_open() {
        // A layer on the git router itself would also wrap its fallback and
        // answer 401 for every unknown path (e.g. a `/metrics` moved to
        // `--metrics-listen`); guarding by prefix keeps those a plain 404.
        app = app.layer(axum::middleware::from_fn_with_state(acl, git_auth));
    }
    // Limits sit inside the observer so rejections are counted and timed.
    app.layer(axum::middleware::from_fn_with_state(limiter, http_limit))
        .layer(axum::middleware::from_fn_with_state(observer, http_observe))
}

/// `GET /metrics`: the OpenMetrics text of the scrape.
async fn scrape(State(metrics): State<Arc<Metrics>>) -> Response {
    let body = metrics.render();
    ([(header::CONTENT_TYPE, crate::metrics::CONTENT_TYPE)], body).into_response()
}

/// The object endpoint, health, readiness and stats routes (plus
/// `/metrics` when `expose_metrics`), without the git export.
fn router_with_state(
    backend: Backend,
    acl: Acl,
    metrics: Arc<Metrics>,
    expose_metrics: bool,
) -> Router {
    let mut router = Router::new()
        .route("/o/check", post(check_objects))
        .route("/o/{hex}", get(get_object))
        .route("/t/{hex}", get(get_tree_bundle))
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .route("/debug/stats", get(debug_stats))
        .with_state(HttpState {
            bundle_reads: Arc::new(bundle::Coalescer::new(backend.objects.clone())),
            backend,
            acl,
            probes: metrics.probes(),
            bundles: Arc::new(bundle::BundleCache::default()),
        });
    if expose_metrics {
        router = router.merge(Router::new().route("/metrics", get(scrape)).with_state(metrics));
    }
    router
}

/// `GET /readyz`: 200 when every configured dependency answers its probe,
/// else 503; the JSON body names each dependency's state. Unauthenticated.
async fn readyz(State(state): State<HttpState>) -> Response {
    let readiness = state.probes.check().await;
    let status =
        if readiness.is_ready() { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (
        status,
        [
            (header::CONTENT_TYPE, HeaderValue::from_static("application/json")),
            (header::CACHE_CONTROL, HeaderValue::from_static(CACHE_NO_STORE)),
        ],
        readiness.json(),
    )
        .into_response()
}

/// `GET /debug/stats`: process-local sizes as one JSON line. Under a policy
/// only admin principals may read it.
async fn debug_stats(State(state): State<HttpState>, headers: HeaderMap) -> Response {
    match state.acl.authenticate_header(authorization(&headers)) {
        Err(e) => return unauthorized(BEARER_CHALLENGE, e),
        Ok(Some(access)) if !access.is_admin() => {
            return (StatusCode::FORBIDDEN, "admin token required").into_response();
        }
        Ok(_) => {}
    }
    ([(header::CONTENT_TYPE, "application/json")], stats_json(&state.bundles)).into_response()
}

/// Cache entries and weights, store registrations, land queues and running
/// catch-ups, and the tree-bundle cache. Built by hand: the shape is fixed
/// and no JSON crate is needed.
fn stats_json(bundles: &bundle::BundleCache) -> String {
    let (caches, objects, metas) = crate::ops::cache::cache_stats();
    let caches: Vec<String> = caches
        .iter()
        .map(|c| {
            format!(
                r#"{{"name":"{}","entries":{},"weight_bytes":{},"budget_bytes":{}}}"#,
                c.name, c.entries, c.weight_bytes, c.budget_bytes
            )
        })
        .collect();
    let (queues, requests) = crate::ops::land::queue_stats();
    let (entries, bytes) = bundles.usage();
    let (hits, misses) = bundle::cache_lookups();
    format!(
        r#"{{"caches":[{}],"cache_stores":{{"objects":{objects},"meta":{metas}}},"land_queues":{{"queues":{queues},"requests":{requests}}},"catch_ups":{},"bundle_cache":{{"entries":{entries},"bytes":{bytes},"budget_bytes":{},"hits":{hits},"misses":{misses}}}}}"#,
        caches.join(","),
        crate::ops::history::running_catch_ups(),
        bundles.budget(),
    )
}

#[derive(Clone)]
struct HttpState {
    backend: Backend,
    acl: Acl,
    probes: crate::health::Probes,
    /// Recently served tree bundles (`crate::bundle::BundleCache`).
    bundles: Arc<bundle::BundleCache>,
    /// The store as bundle builds read it (`crate::bundle::Coalescer`).
    bundle_reads: Arc<bundle::Coalescer>,
}

fn authorization(headers: &HeaderMap) -> Option<&str> {
    headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok())
}

fn unauthorized(challenge: &'static str, e: AuthError) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [
            (header::WWW_AUTHENTICATE, HeaderValue::from_static(challenge)),
            (header::CACHE_CONTROL, HeaderValue::from_static(CACHE_NO_STORE)),
        ],
        e.to_string(),
    )
        .into_response()
}

fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        [(header::CACHE_CONTROL, HeaderValue::from_static(CACHE_NO_STORE))],
        "object not found",
    )
        .into_response()
}

fn backend_error(e: impl std::fmt::Display, id: &ObjectId) -> Response {
    tracing::error!(error = %e, %id, "object read failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [(header::CACHE_CONTROL, HeaderValue::from_static(CACHE_NO_STORE))],
        "storage backend error",
    )
        .into_response()
}

/// Git export guard: Basic (or Bearer) auth, and full read access because
/// the export cannot filter trees without changing ids.
async fn git_auth(
    State(acl): State<Acl>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = request.uri().path();
    if path != "/repo.git" && !path.starts_with("/repo.git/") {
        return next.run(request).await;
    }
    match acl.authenticate_header(authorization(request.headers())) {
        Err(e) => unauthorized("Basic realm=\"antimony\"", e),
        Ok(Some(access)) if !access.can_read_everything() => {
            (StatusCode::FORBIDDEN, "git export requires read access to the whole repository")
                .into_response()
        }
        Ok(_) => next.run(request).await,
    }
}

async fn get_object(
    State(HttpState { backend, acl, .. }): State<HttpState>,
    Path(hex): Path<String>,
    headers: HeaderMap,
) -> Response {
    let access = match acl.authenticate_header(authorization(&headers)) {
        Ok(a) => a,
        Err(e) => return unauthorized(BEARER_CHALLENGE, e),
    };
    let id = match ObjectId::from_str(&hex) {
        Ok(id) => id,
        Err(_) => return (StatusCode::BAD_REQUEST, "malformed object id").into_response(),
    };
    let etag = format!("\"{}\"", id.to_hex());

    let mut cache = CACHE_IMMUTABLE;
    match acl::read_access(&backend, access.as_ref(), &[id]).await {
        Ok(r) => match r.first() {
            Some(a) if a.readable => {
                if a.private {
                    cache = CACHE_PRIVATE;
                }
            }
            _ => return not_found(),
        },
        Err(e) => return backend_error(e, &id),
    }

    // Immutable content: a matching ETag means the client already holds
    // exactly these bytes, so answer without touching the store.
    if if_none_match_matches(&headers, &etag) {
        return Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header(header::ETAG, etag)
            .header(header::CACHE_CONTROL, cache)
            .body(axum::body::Body::empty())
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    match backend.objects.get_many(std::slice::from_ref(&id)).await {
        Ok(mut found) => match found.pop().flatten() {
            Some(obj) => (
                [
                    (header::CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE)),
                    (header::CACHE_CONTROL, HeaderValue::from_static(cache)),
                    (
                        header::ETAG,
                        HeaderValue::from_str(&etag).unwrap_or(HeaderValue::from_static("\"\"")),
                    ),
                ],
                obj.into_bytes(),
            )
                .into_response(),
            None => not_found(),
        },
        Err(e) => backend_error(e, &id),
    }
}

/// Most ids one `POST /o/check` may carry.
pub const CHECK_MAX_IDS: usize = 4096;
/// `Content-Type` of a `POST /o/check` response.
pub const CHECK_CONTENT_TYPE: &str = "application/x-ant-check";
/// Response flag: the object would be served to this caller.
pub const CHECK_READABLE: u8 = 1;
/// Response flag: it would be served `private`.
pub const CHECK_PRIVATE: u8 = 2;

/// `POST /o/check`: which of the posted objects `GET /o/{id}` would serve
/// this caller (see the module docs).
async fn check_objects(
    State(HttpState { backend, acl, .. }): State<HttpState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let access = match acl.authenticate_header(authorization(&headers)) {
        Ok(a) => a,
        Err(e) => return unauthorized(BEARER_CHALLENGE, e),
    };
    if !body.len().is_multiple_of(32) {
        return (StatusCode::BAD_REQUEST, "body must be a multiple of 32 bytes").into_response();
    }
    if body.len() / 32 > CHECK_MAX_IDS {
        return (StatusCode::PAYLOAD_TOO_LARGE, "too many ids in one check").into_response();
    }
    let ids: Vec<ObjectId> =
        body.chunks_exact(32).filter_map(|c| <[u8; 32]>::try_from(c).ok()).map(ObjectId).collect();
    // Existence is part of the answer (a hidden object must look like a
    // missing one), and is always looked up so the work does not depend on
    // the decision.
    let (access_res, exists) = tokio::join!(
        acl::read_access(&backend, access.as_ref(), &ids),
        backend.objects.has_many(&ids)
    );
    let (decisions, exists) = match (access_res, exists) {
        (Ok(d), Ok(e)) => (d, e),
        (Err(e), _) => return check_error(e),
        (_, Err(e)) => return check_error(e),
    };
    let flags: Vec<u8> = decisions
        .iter()
        .zip(&exists)
        .map(|(d, e)| match (d.readable && *e, d.private) {
            (true, true) => CHECK_READABLE | CHECK_PRIVATE,
            (true, false) => CHECK_READABLE,
            (false, _) => 0,
        })
        .collect();
    (
        [
            (header::CONTENT_TYPE, HeaderValue::from_static(CHECK_CONTENT_TYPE)),
            (header::CACHE_CONTROL, HeaderValue::from_static(CACHE_NO_STORE)),
            (HeaderName::from_static("x-ant-check"), HeaderValue::from_static("1")),
        ],
        flags,
    )
        .into_response()
}

fn check_error(e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, "object check failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [(header::CACHE_CONTROL, HeaderValue::from_static(CACHE_NO_STORE))],
        "storage backend error",
    )
        .into_response()
}

/// `GET /t/{hex}`: the tree bundle of `hex` (see the module docs).
async fn get_tree_bundle(
    State(HttpState { backend, acl, bundles, bundle_reads, .. }): State<HttpState>,
    Path(hex): Path<String>,
    headers: HeaderMap,
) -> Response {
    let mut res = tree_bundle(backend, acl, &bundles, &bundle_reads, &hex, &headers).await;
    res.headers_mut().insert(ant_proto::bundle::HEADER, HeaderValue::from_static("1"));
    res
}

fn bundle_response(body: Bytes, cache: &'static str) -> Response {
    (
        [
            (header::CONTENT_TYPE, HeaderValue::from_static(ant_proto::bundle::CONTENT_TYPE)),
            (header::CACHE_CONTROL, HeaderValue::from_static(cache)),
        ],
        body,
    )
        .into_response()
}

async fn tree_bundle(
    backend: Backend,
    acl: Acl,
    cached: &bundle::BundleCache,
    reads: &bundle::Coalescer,
    hex: &str,
    headers: &HeaderMap,
) -> Response {
    let access = match acl.authenticate_header(authorization(headers)) {
        Ok(a) => a,
        Err(e) => return unauthorized(BEARER_CHALLENGE, e),
    };
    let id = match ObjectId::from_str(hex) {
        Ok(id) => id,
        Err(_) => return (StatusCode::BAD_REQUEST, "malformed object id").into_response(),
    };
    if let Some(access) = &access {
        match acl::readable(&backend, &access.policy, &access.principal, &[id]).await {
            Ok(r) if r.first().copied().unwrap_or(false) => {}
            Ok(_) => return not_found(),
            Err(e) => return backend_error(e, &id),
        }
    }
    // A cached bundle hid nothing: serve it to whoever may read all of it.
    if let Some(hit) = cached.get(&id) {
        let whole = match &access {
            None => true,
            Some(access) => match bundle_visibility(&backend, access, id, &hit.ids).await {
                Ok((hidden, cache)) => hidden.is_empty() && cache == CACHE_IMMUTABLE,
                Err(e) => return backend_error(e, &id),
            },
        };
        if whole {
            return bundle_response(hit.body.clone(), CACHE_IMMUTABLE);
        }
    }
    let units = match bundle::collect(reads, id, Limits::default()).await {
        Ok(Some(units)) => units,
        Ok(None) => return not_found(),
        Err(e) => return backend_error(e, &id),
    };
    let ids = bundle::visibility_ids(&units);
    let (hidden, cache) = match &access {
        None => (HashSet::new(), CACHE_IMMUTABLE),
        Some(access) => match bundle_visibility(&backend, access, id, &ids).await {
            Ok(v) => v,
            Err(e) => return backend_error(e, &id),
        },
    };
    let objects = bundle::select(&units, &hidden);
    drop(units);
    let body = tokio::task::spawn_blocking(move || {
        ant_proto::bundle::encode(objects.iter().map(|(id, o)| (id, o.as_bytes().as_ref())))
    })
    .await;
    match body {
        Ok(Ok(body)) => {
            let body = Bytes::from(body);
            if hidden.is_empty() && cache == CACHE_IMMUTABLE {
                cached.insert(id, body.clone(), ids);
            }
            bundle_response(body, cache)
        }
        Ok(Err(e)) => backend_error(e, &id),
        Err(e) => backend_error(e, &id),
    }
}

/// Which objects of a bundle `access` may receive, and how the response may
/// be cached. Shared caches serve a `public` response to anyone, so it must
/// equal what every caller gets and hold only objects `GET /o/` would also
/// serve `public`:
/// - The requested tree is private (outside the root zone): everything the
///   caller cannot read is hidden, and the response is `private, no-store`.
/// - Otherwise private objects are always hidden (independent of the
///   caller). If the caller cannot read some remaining object, that is
///   hidden too and the response, now caller-specific, is
///   `private, no-store`; else it is `public` and immutable.
///
/// `ids`: [`bundle::visibility_ids`] of the bundle's units.
async fn bundle_visibility(
    backend: &Backend,
    access: &Access,
    root: ObjectId,
    ids: &[ObjectId],
) -> Result<(HashSet<ObjectId>, &'static str), StoreError> {
    let policy = &access.policy;
    let private = acl::private_many(backend, policy, ids).await?;
    let root_private = ids.iter().zip(&private).any(|(id, p)| *id == root && *p);
    let mut hidden: HashSet<ObjectId> = if root_private {
        HashSet::new()
    } else {
        ids.iter().zip(&private).filter(|(_, p)| **p).map(|(id, _)| *id).collect()
    };
    let readable = acl::readable(backend, policy, &access.principal, ids).await?;
    let mut shared = !root_private;
    for (id, ok) in ids.iter().zip(readable) {
        if !ok && hidden.insert(*id) {
            shared = false;
        }
    }
    Ok((hidden, if shared { CACHE_IMMUTABLE } else { CACHE_PRIVATE_NO_STORE }))
}

/// True if any `If-None-Match` entry equals `etag` (weak prefixes ignored)
/// or is `*`.
fn if_none_match_matches(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .any(|candidate| {
            candidate == "*" || candidate.strip_prefix("W/").unwrap_or(candidate) == etag
        })
}

#[cfg(test)]
mod tests {
    use super::stats_json;

    #[test]
    fn stats_json_has_expected_shape() {
        let json = stats_json(&crate::bundle::BundleCache::new(1024));
        assert!(json.starts_with(r#"{"caches":[{"name":"trees""#), "{json}");
        assert!(json.contains(r#""land_queues":{"queues":"#), "{json}");
        assert!(
            json.contains(r#""bundle_cache":{"entries":0,"bytes":0,"budget_bytes":1024,"#),
            "{json}"
        );
        assert!(json.ends_with('}'), "{json}");
        assert!(!json.contains('\n'));
    }
}
