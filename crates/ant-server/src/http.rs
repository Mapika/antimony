//! Plain-HTTP immutable object endpoint, designed to sit behind any HTTP
//! cache or CDN.
//!
//! - `GET /o/{hex}` → 200, body = encoded object bytes,
//!   `Content-Type: application/x-ant-object`,
//!   `Cache-Control: public, max-age=31536000, immutable`, `ETag: "<hex>"`.
//!   `If-None-Match` matching the ETag → 304 with no body.
//! - Unknown id → 404 with `Cache-Control: no-store` (it may appear later).
//! - Malformed id → 400.
//! - `GET /healthz` → 200 "ok".
//! - `GET /debug/stats` → 200 JSON of process-local cache, land queue and
//!   catch-up sizes (for soak monitoring).
//!
//! Access control (`crate::acl`, only with a policy): `/o/{hex}` needs
//! `Authorization: Bearer <token>` (or Basic with the token as password),
//! else 401; unreadable objects get exactly the 404 of missing ones; objects
//! outside the root zone are served `Cache-Control: private, ...`. The git
//! export (`/repo.git/...`) takes Basic auth (401 carries
//! `WWW-Authenticate: Basic` so git asks its credential helpers) and needs
//! `can_read_everything`, else 403. `/healthz` and `/debug/stats` stay open.

use std::str::FromStr;

use ant_core::ObjectId;
use ant_store::Backend;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

use crate::acl::{self, Acl, AuthError};

const CONTENT_TYPE: &str = "application/x-ant-object";
const CACHE_IMMUTABLE: &str = "public, max-age=31536000, immutable";
const CACHE_PRIVATE: &str = "private, max-age=31536000, immutable";
const CACHE_NO_STORE: &str = "no-store";

/// Serve the HTTP object endpoint plus the read-only git export
/// (`/repo.git`) on an already-bound listener until it fails.
pub async fn serve_on(backend: Backend, listener: tokio::net::TcpListener) -> std::io::Result<()> {
    serve_with_acl(backend, Acl::open(), listener).await
}

/// [`serve_on`] enforcing `acl` (object endpoint and git export).
pub async fn serve_with_acl(
    backend: Backend,
    acl: Acl,
    listener: tokio::net::TcpListener,
) -> std::io::Result<()> {
    use axum::serve::ListenerExt;
    let app = app_with_acl(backend, acl);
    let listener = listener.tap_io(|tcp| {
        // Avoid Nagle/delayed-ACK stalls on small responses.
        let _ = tcp.set_nodelay(true);
    });
    axum::serve(listener, app).await
}

/// Object endpoint plus git export, enforcing `acl`.
pub fn app_with_acl(backend: Backend, acl: Acl) -> Router {
    let git = ant_gitserve::router(backend.clone());
    let git = if acl.is_open() {
        git
    } else {
        git.layer(axum::middleware::from_fn_with_state(acl.clone(), git_auth))
    };
    router_with_acl(backend, acl).merge(git)
}

pub fn router(backend: Backend) -> Router {
    router_with_acl(backend, Acl::open())
}

/// The object endpoint (`/o/{hex}`, `/healthz`) enforcing `acl`.
pub fn router_with_acl(backend: Backend, acl: Acl) -> Router {
    Router::new()
        .route("/o/{hex}", get(get_object))
        .route("/healthz", get(|| async { "ok" }))
        .route("/debug/stats", get(debug_stats))
        .with_state(HttpState { backend, acl })
}

/// `GET /debug/stats`: process-local sizes as one JSON line.
async fn debug_stats() -> Response {
    ([(header::CONTENT_TYPE, "application/json")], stats_json()).into_response()
}

/// Cache entries and weights, store registrations, land queues and running
/// catch-ups. Built by hand: the shape is fixed and no JSON crate is needed.
fn stats_json() -> String {
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
    format!(
        r#"{{"caches":[{}],"cache_stores":{{"objects":{objects},"meta":{metas}}},"land_queues":{{"queues":{queues},"requests":{requests}}},"catch_ups":{}}}"#,
        caches.join(","),
        crate::ops::history::running_catch_ups(),
    )
}

#[derive(Clone)]
struct HttpState {
    backend: Backend,
    acl: Acl,
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
    State(HttpState { backend, acl }): State<HttpState>,
    Path(hex): Path<String>,
    headers: HeaderMap,
) -> Response {
    let access = match acl.authenticate_header(authorization(&headers)) {
        Ok(a) => a,
        Err(e) => return unauthorized("Bearer realm=\"antimony\"", e),
    };
    let id = match ObjectId::from_str(&hex) {
        Ok(id) => id,
        Err(_) => return (StatusCode::BAD_REQUEST, "malformed object id").into_response(),
    };
    let etag = format!("\"{}\"", id.to_hex());

    let mut cache = CACHE_IMMUTABLE;
    if let Some(access) = &access {
        let policy = &access.policy;
        match acl::readable(&backend, policy, &access.principal, &[id]).await {
            Ok(r) if r.first().copied().unwrap_or(false) => {}
            Ok(_) => return not_found(),
            Err(e) => return backend_error(e, &id),
        }
        match acl::is_private(&backend, policy, id).await {
            Ok(true) => cache = CACHE_PRIVATE,
            Ok(false) => {}
            Err(e) => return backend_error(e, &id),
        }
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
        let json = stats_json();
        assert!(json.starts_with(r#"{"caches":[{"name":"trees""#), "{json}");
        assert!(json.contains(r#""land_queues":{"queues":"#), "{json}");
        assert!(json.ends_with('}'), "{json}");
        assert!(!json.contains('\n'));
    }
}
