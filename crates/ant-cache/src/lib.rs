//! The cache tier: `ant cache-serve`, a caching proxy for the server's
//! immutable HTTP object surface.
//!
//! It serves `GET /o/{id}` and `GET /t/{tree}` (see `ant-server`'s `http`
//! module for the formats) from a local disk cache in the client cache
//! format (`antd::disk`), filling misses from an upstream: the origin server
//! or another cache-serve. Clients use it by pointing `--http` / `ANT_HTTP`
//! at it; their gRPC connection keeps going to the origin.
//!
//! - **Fill**: a miss fetches the object from the upstream, verifies its
//!   hash against the requested id (an upstream cannot poison the cache),
//!   stores it, and answers. Concurrent misses for one id share one upstream
//!   request (`joined`).
//! - **Bundles**: a bundle is stored as its objects plus a small index
//!   ([`index`]); a hit re-encodes it from the disk cache with no upstream
//!   request. A bundle whose objects were partly evicted is a miss.
//! - **Disk budget**: the cache is an `antd::disk::DiskObjectStore`, so the
//!   size budget, LRU eviction and `ant cache stats` / `ant cache gc` apply
//!   unchanged; [`CacheServe::spawn_maintenance`] enforces the budget while
//!   idle and prunes the bundle index.
//! - **Metrics**: `GET /metrics`, see [`metrics`].
//! - **Misses are not stored**: a 404 is never cached (the object may be
//!   written later).
//!
//! # Access control
//!
//! The origin decides who may read what (path and ref rules, zone tags;
//! `ant-server`'s `acl`), and its answers depend on the caller, so a shared
//! cache that answered by object id alone would hand an object to a
//! principal the origin would refuse. Two modes, chosen by the operator:
//!
//! - [`AuthMode::Forward`] (default, safe for any upstream): the cache never
//!   answers a request on its own authority. The caller's `Authorization`
//!   header is forwarded to the upstream on every request.
//!   - `/o/{id}` held locally: the cache sends the upstream a conditional
//!     `GET /o/{id}` with `If-None-Match: "<id>"` and the caller's
//!     credentials. The origin authorizes it exactly as it would a read and,
//!     if allowed, answers `304` without touching its stores; the cache then
//!     serves the bytes from disk (`revalidated`). Any other answer (401,
//!     403, 404, 5xx, transport error) is passed through and nothing is
//!     served: it fails closed. The saving is the object bytes and the origin's
//!     store read, not the request.
//!   - `/o/{id}` not held: forwarded with the caller's credentials; the
//!     response is stored (the disk is private to the cache; serving is always
//!     gated as above). Concurrent misses by the *same* credentials share one
//!     request; a concurrent miss by *different* credentials first passes the
//!     conditional check above and only then takes the bytes of the in-flight
//!     fetch, so waiting never bypasses authorization.
//!   - `/t/{tree}`: a bundle's content depends on the caller (objects they
//!     cannot read are left out), so it is forwarded uncached with the
//!     caller's credentials and returned with the origin's own headers
//!     (`private, no-store` when caller-specific). The verified objects in
//!     it are also stored, which makes later `/o/` revalidations for them
//!     hits.
//! - [`AuthMode::Open`]: for an upstream that enforces no access policy
//!   (a server started without `--policy`, or a cache-serve in open mode in
//!   front of one). Every object is then readable by everyone, so the cache
//!   serves hits with no upstream request and builds bundles from its
//!   disk. `Authorization` headers are ignored and never forwarded. At
//!   startup the cache checks that the upstream really is open (an
//!   unauthenticated `GET /debug/stats` must answer `200`; servers with a
//!   policy answer `401`, a forward-mode cache-serve `404`) and refuses to
//!   start otherwise, so an open cache cannot be put in front of a
//!   protected origin by accident. An open cache-serve exposes `GET
//!   /debug/stats` itself, so caches chain (`client -> regional -> node ->
//!   origin`, each open); forward-mode caches may chain too.
//!
//! The cache's own HTTP listener has no authentication of its own: bind it
//! where the clients that may read the data can reach it (an open cache
//! serves *everything* to whoever connects).

pub mod index;
pub mod metrics;
mod serve;

pub use serve::{AuthMode, CacheServe, Config};
