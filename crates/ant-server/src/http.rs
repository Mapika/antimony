//! Plain-HTTP immutable object endpoint, designed to sit behind any HTTP
//! cache or CDN.
//!
//! TASK (transport): implement `router`.
//!
//! - `GET /o/{hex}` → 200, body = encoded object bytes,
//!   `Content-Type: application/x-ant-object`,
//!   `Cache-Control: public, max-age=31536000, immutable`, `ETag: "<hex>"`.
//!   `If-None-Match` matching the ETag → 304 with no body.
//! - Unknown id → 404 with `Cache-Control: no-store` (it may appear later).
//! - Malformed id → 400.
//! - `GET /healthz` → 200 "ok".

use std::str::FromStr;

use ant_core::ObjectId;
use ant_store::Backend;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

const CONTENT_TYPE: &str = "application/x-ant-object";
const CACHE_IMMUTABLE: &str = "public, max-age=31536000, immutable";
const CACHE_NO_STORE: &str = "no-store";

/// Serve the HTTP object endpoint plus the read-only git export
/// (`/repo.git`) on an already-bound listener until it fails.
pub async fn serve_on(backend: Backend, listener: tokio::net::TcpListener) -> std::io::Result<()> {
    use axum::serve::ListenerExt;
    let app = router(backend.clone()).merge(ant_gitserve::router(backend));
    let listener = listener.tap_io(|tcp| {
        // Avoid Nagle/delayed-ACK stalls on small responses.
        let _ = tcp.set_nodelay(true);
    });
    axum::serve(listener, app).await
}

pub fn router(backend: Backend) -> Router {
    Router::new()
        .route("/o/{hex}", get(get_object))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(backend)
}

async fn get_object(
    State(backend): State<Backend>,
    Path(hex): Path<String>,
    headers: HeaderMap,
) -> Response {
    let id = match ObjectId::from_str(&hex) {
        Ok(id) => id,
        Err(_) => return (StatusCode::BAD_REQUEST, "malformed object id").into_response(),
    };
    let etag = format!("\"{}\"", id.to_hex());

    // Immutable content: a matching ETag means the client already holds
    // exactly these bytes, so answer without touching the store.
    if if_none_match_matches(&headers, &etag) {
        return Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header(header::ETAG, etag)
            .header(header::CACHE_CONTROL, CACHE_IMMUTABLE)
            .body(axum::body::Body::empty())
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    match backend.objects.get_many(std::slice::from_ref(&id)).await {
        Ok(mut found) => match found.pop().flatten() {
            Some(obj) => (
                [
                    (header::CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE)),
                    (header::CACHE_CONTROL, HeaderValue::from_static(CACHE_IMMUTABLE)),
                    (
                        header::ETAG,
                        HeaderValue::from_str(&etag).unwrap_or(HeaderValue::from_static("\"\"")),
                    ),
                ],
                obj.into_bytes(),
            )
                .into_response(),
            None => (
                StatusCode::NOT_FOUND,
                [(header::CACHE_CONTROL, HeaderValue::from_static(CACHE_NO_STORE))],
                "object not found",
            )
                .into_response(),
        },
        Err(e) => {
            tracing::error!(error = %e, %id, "object read failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CACHE_CONTROL, HeaderValue::from_static(CACHE_NO_STORE))],
                "storage backend error",
            )
                .into_response()
        }
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
