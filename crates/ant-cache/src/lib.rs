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
//!   - `/o/{id}` held locally: the cache asks the upstream whether the caller
//!     may read it (see "Permission checks" below) and serves the bytes from
//!     disk only on a clear yes (`revalidated`). Any other answer (401, 403,
//!     404 or a no, 5xx, transport error) is passed through or turned into
//!     the origin's own 404, and nothing is served: it fails closed.
//!   - `/o/{id}` not held: forwarded with the caller's credentials; the
//!     response is stored (the disk is private to the cache; serving is always
//!     gated as above). Concurrent misses by the *same* credentials share one
//!     request; a concurrent miss by *different* credentials first passes a
//!     conditional read at the origin and only then takes the bytes of the in-flight
//!     fetch, so waiting never bypasses authorization.
//!   - `/t/{tree}`: see "Bundles" below.
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
//! # Permission checks (forward mode)
//!
//! A hit costs the origin one question, not one object: `POST /o/check`
//! (`ant-server`'s `http` module) takes up to 4096 object ids with the
//! caller's `Authorization` and answers, per id, whether `GET /o/{id}` would
//! serve that caller (and whether `private`). It runs the very same
//! authorization code as `GET /o/{id}` (`acl::read_access`), and a hidden
//! object is indistinguishable from a missing one in the answer.
//!
//! - **Batching**: questions from concurrent requests with the same
//!   credentials wait `authz_window` (default 2 ms) and go out as one
//!   request, in chunks of 4096. A request for many objects at once (a
//!   bundle) is sent immediately.
//! - **Decision cache**: an *allowed* answer is reused for the same
//!   credentials and object for at most `authz_ttl` (default 30 s) from the
//!   moment the question was sent; then the origin is asked again. Denials
//!   and errors are never remembered, so a grant works at once. A successful
//!   fetch of an object by a caller counts as a decision for it. Decisions
//!   are keyed by a SHA-256 of the whole `Authorization` header value (the
//!   token itself is not kept, logged or exported) and are bounded in
//!   number. **The TTL is the revocation window**: a principal whose access
//!   was revoked at the origin (policy change, re-tagging) can still read
//!   objects this cache holds for up to `authz_ttl` more. `authz_ttl = 0`
//!   asks on every request (still batched), which restores immediate
//!   revocation at the price of one origin request per batch. Another
//!   principal never benefits from a decision made for someone else.
//! - **Older origins**: a `404` or `405` for `POST /o/check` means the origin
//!   predates it. The cache then authorizes each hit with a conditional
//!   `GET /o/{id}` (`If-None-Match`; the origin answers `304` for readable
//!   objects) as before, keeps no decisions, forwards bundles, and probes the
//!   endpoint again after a minute. Any other failure of the check (5xx,
//!   malformed answer, transport error) is not a reason to fall back: it
//!   serves nothing.
//!
//! # Bundles
//!
//! A bundle depends on the caller, so it is never answered from stored bytes
//! alone. Instead of revalidating bundle bodies (which would still cost an
//! origin request per bundle and a store per caller) the cache *rebuilds*
//! them: when the origin serves a bundle `public` (the same for every
//! caller: nothing outside the root zone in it) the cache indexes the
//! objects it lists. A later request for that tree, from any caller, is
//! answered by checking all listed ids with batched `POST /o/check`s and
//! encoding exactly the objects that caller may read, tree first, from the
//! disk cache: `public, immutable` if the caller may read all of them,
//! `private, no-store` if some were left out (as the origin does). That is
//! authorized object by object by the origin's own rules, so the result
//! never contains anything a `GET /o/{id}` by that caller would refuse. If
//! the tree is not indexed, an object was evicted, the tree itself is not
//! readable by the caller, the origin has no `/o/check` or anything fails,
//! the request is forwarded to the origin and its answer returned as before.
//! Bundles the origin serves caller-specific (`private, no-store`, e.g. a
//! tree outside the root zone) are only forwarded.
//!
//! The cache's own HTTP listener has no authentication of its own: bind it
//! where the clients that may read the data can reach it (an open cache
//! serves *everything* to whoever connects).

mod authz;
pub mod index;
pub mod metrics;
mod serve;

pub use serve::{AuthMode, CacheServe, Config, DEFAULT_AUTHZ_TTL, DEFAULT_AUTHZ_WINDOW};
