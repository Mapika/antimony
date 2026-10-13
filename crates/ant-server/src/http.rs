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

use ant_store::Backend;

pub fn router(backend: Backend) -> axum::Router {
    let _ = backend;
    todo!("transport")
}
