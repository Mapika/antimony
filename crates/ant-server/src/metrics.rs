//! Observability: Prometheus metrics, structured logs and request ids.
//!
//! STATUS: contract for the next implementation round; not yet wired in.
//! (This file is not compiled until `pub mod metrics;` is added to lib.rs.)
//!
//! # Metrics
//! Exposed in Prometheus text format at `GET /metrics` on the HTTP port.
//! Unauthenticated, but `--metrics-listen <addr>` moves it to a separate
//! listener for deployments that must not expose it publicly. Use the
//! `prometheus-client` crate (no global registry; one `Metrics` value created
//! in `serve::run` and passed to rpc/http/ops via `Arc`).
//!
//! Required series (labels in braces):
//! - `ant_rpc_requests_total{method,code}` and
//!   `ant_rpc_duration_seconds{method}` (histogram) for every gRPC method,
//!   recorded by a tower layer so handlers stay untouched.
//! - `ant_http_requests_total{route,status}`,
//!   `ant_http_duration_seconds{route}`; routes: `objects`, `git`, `healthz`.
//! - Landing: `ant_land_batch_size` (histogram), `ant_land_outcomes_total
//!   {outcome=landed|rebased|conflict|aborted|denied}`,
//!   `ant_land_cas_retries_total`, `ant_land_queue_depth{ref}` (gauge; cap
//!   label cardinality at the 64 most recently active refs, fold the rest
//!   into `ref="other"`).
//! - Stores: `ant_store_op_duration_seconds{store=objects|refs|meta,op}`
//!   via wrapper types like ant-bench's timed stores; `ant_store_errors_total
//!   {store,op}`.
//! - Caches (ops/cache.rs): `ant_cache_hits_total{cache}`,
//!   `ant_cache_misses_total{cache}`, `ant_cache_bytes{cache}`.
//! - History index: `ant_history_lag_commits{ref}`,
//!   `ant_history_catch_up_seconds` (histogram).
//! - Process: build info (`ant_build_info{version,git}` = 1), Postgres pool
//!   size/idle/in-use gauges.
//!
//! # Logs
//! `--log-format json|text` (default text on a TTY, json otherwise). Every
//! RPC/HTTP request gets a request id (honour an incoming `x-request-id`, else
//! generate a ULID), attached as a tracing span field and echoed in the
//! response header. Log level via `RUST_LOG`. Slow requests (> 1 s) logged at
//! WARN with method, principal (if authenticated) and duration.
//!
//! # Tests
//! - `/metrics` parses with `prometheus-parse` (dev-dep) and contains every
//!   required metric family after a short scripted session (create, land,
//!   conflict, object reads, git clone).
//! - Counters move as expected (e.g. one conflicting land →
//!   `ant_land_outcomes_total{outcome="conflict"}` +1).
//! - Overhead: the in-memory `ant-bench fleet` lands/s drops by < 3%.
//! - `x-request-id` round-trips through gRPC and HTTP.
