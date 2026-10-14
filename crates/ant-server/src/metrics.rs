//! Observability: Prometheus metrics (this module), request ids, slow-request
//! logs and the transport layers that record them (`crate::observe`).
//!
//! # Metrics
//! Exposed in the OpenMetrics text format (Prometheus scrapes it natively) at
//! `GET /metrics` on the HTTP port. Unauthenticated, but
//! `--metrics-listen <addr>` moves it to a separate listener (and removes it
//! from the main HTTP port) for deployments that must not expose it publicly.
//!
//! There is no global registry: [`Metrics`] is created once in `serve::run`
//! and shared via `Arc` with the gRPC service, the HTTP routers and the store
//! wrappers ([`Metrics::instrument`]). State that already lives in
//! process-wide statics (cache sizes and lookup counters, land queues) is
//! read when the endpoint is scraped ([`Metrics::render`]); a `Metrics`
//! reports cache lookups that happened since it was created.
//!
//! Families (labels in braces):
//! - `ant_rpc_requests_total{method,code}`, `ant_rpc_duration_seconds{method}`
//!   (histogram): every gRPC method, recorded by a tower layer around the
//!   whole service. `code` is the snake_case gRPC status (`ok`,
//!   `not_found`, ...; `cancelled` when the client went away before the
//!   response finished). The duration runs until the response body ends, so
//!   streaming RPCs include the stream. Methods outside the proto service are
//!   folded into `unknown`.
//! - `ant_http_requests_total{route,status}`,
//!   `ant_http_duration_seconds{route}`: routes `objects`, `git`, `healthz`, `readyz`,
//!   plus `metrics` and `other` (`/debug/stats`, 404s) so the label set stays
//!   closed. The duration runs until the response headers are ready.
//! - `ant_land_batch_size` (histogram, requests per group-commit batch),
//!   `ant_land_outcomes_total{outcome=landed|rebased|conflict|aborted|denied}`
//!   (one per land request), `ant_land_cas_retries_total` (batches that lost
//!   the ref CAS and replayed), `ant_land_queue_depth{ref}` (requests waiting
//!   now).
//! - `ant_store_op_duration_seconds{store=objects|refs|meta,op}` (histogram)
//!   and `ant_store_errors_total{store,op}`, via [`Metrics::instrument`].
//! - `ant_cache_hits_total{cache}`, `ant_cache_misses_total{cache}`,
//!   `ant_cache_bytes{cache}` for `trees`, `commits`, `nodes`, `changesets`.
//! - `ant_history_lag_commits{ref}` (commits the path-history index is behind
//!   the ref, as of the last catch-up step) and `ant_history_catch_up_seconds`
//!   (histogram, catch-ups that indexed something).
//! - `ant_build_info{version,git}` = 1 and, with Postgres,
//!   `ant_pg_pool_size`, `ant_pg_pool_idle`, `ant_pg_pool_in_use`,
//!   `ant_pg_pool_max` (0 for the in-memory backend).
//!
//! # Cardinality
//! Every label value comes from a closed set (methods, routes, status codes,
//! store ops, cache names) except `ref`. Per-ref series (`ant_land_queue_depth`,
//! `ant_history_lag_commits`) cover the [`MAX_REF_LABELS`] most recently
//! active refs (touched by a land or a catch-up); everything else is folded
//! into `ref="other"` (queue depths are summed, lag is last-writer). Refs
//! leaving the recent set drop their lag series.
//!
//! # Logs and request ids
//! See `crate::observe` and `serve::LogFormat`: `--log-format json|text`,
//! `x-request-id` (honoured or ULID-generated, a tracing span field, echoed on
//! the response), and WARN logs for requests slower than one second.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ant_core::{EncodedObject, ObjectId, RepoPath};
use ant_store::postgres::PgStore;
use ant_store::{
    Backend, CasOutcome, CommitNode, HistoryRow, MetaStore, ObjectStore, PathChange, RefLock,
    RefStore, Result as StoreResult,
};
use async_trait::async_trait;
use bytes::Bytes;
use prometheus_client::encoding::{EncodeLabelSet, text::encode};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

use crate::ops::OpError;
use crate::ops::cache::{CACHE_NAMES, cache_stats, lookup_counters};
use crate::ops::land::{LandOutcome, queue_depths};

/// Content type of the `/metrics` response.
pub const CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// Refs that get their own `ref` label value (see the module docs).
pub const MAX_REF_LABELS: usize = 64;

/// `ref` label value for everything outside the recent set.
const OTHER: &str = "other";

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RpcLabels {
    method: &'static str,
    code: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct MethodLabels {
    method: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct HttpLabels {
    route: &'static str,
    status: u16,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RouteLabels {
    route: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OutcomeLabels {
    outcome: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RefLabels {
    r#ref: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StoreOpLabels {
    store: &'static str,
    op: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct CacheLabels {
    cache: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BuildLabels {
    version: &'static str,
    git: &'static str,
}

type HistFamily<L> = Family<L, Histogram, fn() -> Histogram>;

fn request_histogram() -> Histogram {
    // 0.5 ms .. ~65 s.
    Histogram::new(exponential_buckets(0.0005, 2.0, 18))
}

fn store_histogram() -> Histogram {
    // 0.1 ms .. ~13 s.
    Histogram::new(exponential_buckets(0.0001, 2.0, 18))
}

fn hist_family<L: Clone + std::hash::Hash + Eq>(f: fn() -> Histogram) -> HistFamily<L> {
    Family::new_with_constructor(f)
}

/// The 64 most recently active refs, newest first.
#[derive(Default)]
struct RecentRefs(VecDeque<String>);

impl RecentRefs {
    /// Mark `name` active. Returns the label to use plus the ref pushed out of
    /// the set, if any.
    fn touch(&mut self, name: &str) -> (String, Option<String>) {
        if let Some(pos) = self.0.iter().position(|r| r == name) {
            if pos != 0
                && let Some(r) = self.0.remove(pos)
            {
                self.0.push_front(r);
            }
            return (name.to_owned(), None);
        }
        self.0.push_front(name.to_owned());
        let evicted = if self.0.len() > MAX_REF_LABELS { self.0.pop_back() } else { None };
        (name.to_owned(), evicted)
    }

    fn label(&self, name: &str) -> String {
        if self.0.iter().any(|r| r == name) { name.to_owned() } else { OTHER.to_owned() }
    }
}

/// All server metrics. Create one per server with [`Metrics::new`].
pub struct Metrics {
    registry: Registry,
    rpc_requests: Family<RpcLabels, Counter>,
    rpc_duration: HistFamily<MethodLabels>,
    http_requests: Family<HttpLabels, Counter>,
    http_duration: HistFamily<RouteLabels>,
    land_batch_size: Histogram,
    land_outcomes: Family<OutcomeLabels, Counter>,
    land_cas_retries: Counter,
    land_queue_depth: Family<RefLabels, Gauge>,
    store_duration: HistFamily<StoreOpLabels>,
    store_errors: Family<StoreOpLabels, Counter>,
    cache_hits: Family<CacheLabels, Counter>,
    cache_misses: Family<CacheLabels, Counter>,
    cache_bytes: Family<CacheLabels, Gauge>,
    history_lag: Family<RefLabels, Gauge>,
    history_catch_up: Histogram,
    pg_size: Gauge,
    pg_idle: Gauge,
    pg_in_use: Gauge,
    pg_max: Gauge,
    recent_refs: Mutex<RecentRefs>,
    pg: Mutex<Option<Arc<PgStore>>>,
    probes: Mutex<crate::health::Probes>,
    /// Cache lookup totals already folded into the counters (starts at the
    /// process-wide values when this `Metrics` is created).
    cache_synced: Mutex<[(u64, u64); 4]>,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let mut registry = Registry::default();

        let rpc_requests = Family::<RpcLabels, Counter>::default();
        registry.register(
            "ant_rpc_requests",
            "gRPC requests by method and status",
            rpc_requests.clone(),
        );
        let rpc_duration = hist_family::<MethodLabels>(request_histogram);
        registry.register(
            "ant_rpc_duration_seconds",
            "gRPC request duration until the response body ends",
            rpc_duration.clone(),
        );
        let http_requests = Family::<HttpLabels, Counter>::default();
        registry.register(
            "ant_http_requests",
            "HTTP requests by route and status",
            http_requests.clone(),
        );
        let http_duration = hist_family::<RouteLabels>(request_histogram);
        registry.register(
            "ant_http_duration_seconds",
            "HTTP request duration until the response headers are ready",
            http_duration.clone(),
        );

        let land_batch_size = Histogram::new(exponential_buckets(1.0, 2.0, 7));
        registry.register(
            "ant_land_batch_size",
            "Land requests per group-commit batch",
            land_batch_size.clone(),
        );
        let land_outcomes = Family::<OutcomeLabels, Counter>::default();
        registry.register("ant_land_outcomes", "Land requests by outcome", land_outcomes.clone());
        let land_cas_retries = Counter::default();
        registry.register(
            "ant_land_cas_retries",
            "Land batches replayed after losing the ref CAS",
            land_cas_retries.clone(),
        );
        let land_queue_depth = Family::<RefLabels, Gauge>::default();
        registry.register(
            "ant_land_queue_depth",
            "Land requests waiting per ref (most recently active refs, rest as other)",
            land_queue_depth.clone(),
        );

        let store_duration = hist_family::<StoreOpLabels>(store_histogram);
        registry.register(
            "ant_store_op_duration_seconds",
            "Store call duration by store and operation",
            store_duration.clone(),
        );
        let store_errors = Family::<StoreOpLabels, Counter>::default();
        registry.register("ant_store_errors", "Failed store calls", store_errors.clone());

        let cache_hits = Family::<CacheLabels, Counter>::default();
        registry.register(
            "ant_cache_hits",
            "Cache lookups served from the cache",
            cache_hits.clone(),
        );
        let cache_misses = Family::<CacheLabels, Counter>::default();
        registry.register(
            "ant_cache_misses",
            "Cache lookups that went to the store",
            cache_misses.clone(),
        );
        let cache_bytes = Family::<CacheLabels, Gauge>::default();
        registry.register(
            "ant_cache_bytes",
            "Approximate bytes held per cache",
            cache_bytes.clone(),
        );

        let history_lag = Family::<RefLabels, Gauge>::default();
        registry.register(
            "ant_history_lag_commits",
            "Commits the path-history index is behind the ref",
            history_lag.clone(),
        );
        let history_catch_up = Histogram::new(exponential_buckets(0.001, 2.0, 16));
        registry.register(
            "ant_history_catch_up_seconds",
            "Duration of history catch-ups that indexed commits",
            history_catch_up.clone(),
        );

        let build = Family::<BuildLabels, Gauge>::default();
        registry.register("ant_build_info", "Build information (value is always 1)", build.clone());
        build
            .get_or_create(&BuildLabels {
                version: env!("CARGO_PKG_VERSION"),
                git: option_env!("ANT_GIT_SHA").unwrap_or("unknown"),
            })
            .set(1);

        let pg_size = Gauge::default();
        registry.register(
            "ant_pg_pool_size",
            "Open Postgres query-pool connections",
            pg_size.clone(),
        );
        let pg_idle = Gauge::default();
        registry.register(
            "ant_pg_pool_idle",
            "Idle Postgres query-pool connections",
            pg_idle.clone(),
        );
        let pg_in_use = Gauge::default();
        registry.register(
            "ant_pg_pool_in_use",
            "Postgres query-pool connections in use",
            pg_in_use.clone(),
        );
        let pg_max = Gauge::default();
        registry.register("ant_pg_pool_max", "Configured Postgres query-pool size", pg_max.clone());

        // Closed label sets appear (as zeros) before their first event.
        for outcome in ["landed", "rebased", "conflict", "aborted", "denied"] {
            let _ = land_outcomes.get_or_create(&OutcomeLabels { outcome });
        }
        for cache in CACHE_NAMES {
            let _ = cache_hits.get_or_create(&CacheLabels { cache });
            let _ = cache_misses.get_or_create(&CacheLabels { cache });
            let _ = cache_bytes.get_or_create(&CacheLabels { cache });
        }
        let _ = land_queue_depth.get_or_create(&RefLabels { r#ref: OTHER.to_owned() });
        let _ = history_lag.get_or_create(&RefLabels { r#ref: OTHER.to_owned() });

        Arc::new(Self {
            registry,
            rpc_requests,
            rpc_duration,
            http_requests,
            http_duration,
            land_batch_size,
            land_outcomes,
            land_cas_retries,
            land_queue_depth,
            store_duration,
            store_errors,
            cache_hits,
            cache_misses,
            cache_bytes,
            history_lag,
            history_catch_up,
            pg_size,
            pg_idle,
            pg_in_use,
            pg_max,
            recent_refs: Mutex::new(RecentRefs::default()),
            pg: Mutex::new(None),
            probes: Mutex::new(crate::health::Probes::default()),
            cache_synced: Mutex::new(lookup_counters()),
        })
    }

    /// Report the Postgres pool of `pg` in the `ant_pg_pool_*` gauges.
    pub fn set_pg(&self, pg: Arc<PgStore>) {
        *self.pg.lock().unwrap_or_else(|e| e.into_inner()) = Some(pg.clone());
        self.probes.lock().unwrap_or_else(|e| e.into_inner()).pg = Some(pg);
    }

    /// Register the dependencies `GET /readyz` probes (also sets the
    /// Postgres pool reported in the gauges).
    pub fn set_probes(&self, probes: crate::health::Probes) {
        if let Some(pg) = &probes.pg {
            *self.pg.lock().unwrap_or_else(|e| e.into_inner()) = Some(pg.clone());
        }
        *self.probes.lock().unwrap_or_else(|e| e.into_inner()) = probes;
    }

    /// The dependencies `GET /readyz` probes.
    pub fn probes(&self) -> crate::health::Probes {
        self.probes.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    // -- transports -------------------------------------------------------

    pub(crate) fn record_rpc(&self, method: &'static str, code: &'static str, secs: f64) {
        self.rpc_requests.get_or_create(&RpcLabels { method, code }).inc();
        self.rpc_duration.get_or_create(&MethodLabels { method }).observe(secs);
    }

    pub(crate) fn record_http(&self, route: &'static str, status: u16, secs: f64) {
        self.http_requests.get_or_create(&HttpLabels { route, status }).inc();
        self.http_duration.get_or_create(&RouteLabels { route }).observe(secs);
    }

    // -- landing ----------------------------------------------------------

    /// Count one land request by its result.
    pub(crate) fn record_land(&self, result: &Result<LandOutcome, OpError>) {
        let outcome = match result {
            Ok(LandOutcome::Landed { rebased: false, .. }) => "landed",
            Ok(LandOutcome::Landed { rebased: true, .. }) => "rebased",
            Ok(LandOutcome::Conflict { .. }) => "conflict",
            Err(OpError::Aborted(_)) => "aborted",
            Err(OpError::PermissionDenied(_)) => "denied",
            Err(_) => return,
        };
        self.land_outcomes.get_or_create(&OutcomeLabels { outcome }).inc();
    }

    pub(crate) fn record_land_batch(&self, size: usize) {
        self.land_batch_size.observe(size as f64);
    }

    pub(crate) fn record_land_cas_retry(&self) {
        self.land_cas_retries.inc();
    }

    // -- refs and history -------------------------------------------------

    /// Mark `ref_name` recently active (it keeps its own `ref` label).
    pub(crate) fn touch_ref(&self, ref_name: &str) {
        let (_, evicted) =
            self.recent_refs.lock().unwrap_or_else(|e| e.into_inner()).touch(ref_name);
        if let Some(evicted) = evicted {
            self.history_lag.remove(&RefLabels { r#ref: evicted });
        }
    }

    pub(crate) fn set_history_lag(&self, ref_name: &str, commits: usize) {
        self.touch_ref(ref_name);
        let label = self.recent_refs.lock().unwrap_or_else(|e| e.into_inner()).label(ref_name);
        self.history_lag.get_or_create(&RefLabels { r#ref: label }).set(commits as i64);
    }

    pub(crate) fn record_catch_up(&self, secs: f64) {
        self.history_catch_up.observe(secs);
    }

    // -- stores -----------------------------------------------------------

    fn store_op(&self, store: &'static str, op: &'static str) -> StoreOp {
        let labels = StoreOpLabels { store, op };
        StoreOp {
            duration: self.store_duration.get_or_create(&labels).clone(),
            errors: self.store_errors.get_or_create(&labels).clone(),
        }
    }

    /// Wrap every store of `backend` so each call is timed and its failures
    /// counted (`ant_store_op_duration_seconds`, `ant_store_errors_total`).
    /// Use the returned backend everywhere the server uses stores: caches and
    /// land queues are keyed by store identity.
    pub fn instrument(&self, backend: &Backend) -> Backend {
        Backend {
            objects: Arc::new(TimedObjects {
                inner: backend.objects.clone(),
                get_many: self.store_op("objects", "get_many"),
                put_many: self.store_op("objects", "put_many"),
                has_many: self.store_op("objects", "has_many"),
            }),
            refs: Arc::new(TimedRefs {
                inner: backend.refs.clone(),
                get_ref: self.store_op("refs", "get_ref"),
                cas_ref: self.store_op("refs", "cas_ref"),
                list_refs: self.store_op("refs", "list_refs"),
                lock_ref: self.store_op("refs", "lock_ref"),
            }),
            meta: Arc::new(TimedMeta {
                inner: backend.meta.clone(),
                put_commit: self.store_op("meta", "put_commit"),
                put_commits: self.store_op("meta", "put_commits"),
                get_commits: self.store_op("meta", "get_commits"),
                get_changeset: self.store_op("meta", "get_changeset"),
                get_changesets: self.store_op("meta", "get_changesets"),
                first_parent_chain: self.store_op("meta", "first_parent_chain"),
                tail: self.store_op("meta", "tail"),
                get_derived: self.store_op("meta", "get_derived"),
                get_derived_many: self.store_op("meta", "get_derived_many"),
                put_derived: self.store_op("meta", "put_derived"),
                put_derived_many: self.store_op("meta", "put_derived_many"),
                index_history: self.store_op("meta", "index_history"),
                query_history: self.store_op("meta", "query_history"),
                history_watermark: self.store_op("meta", "history_watermark"),
                set_history_watermark: self.store_op("meta", "set_history_watermark"),
                clear_history: self.store_op("meta", "clear_history"),
            }),
        }
    }

    // -- scrape -----------------------------------------------------------

    /// Refresh the gauges that mirror process state and encode everything in
    /// the OpenMetrics text format ([`CONTENT_TYPE`]).
    pub fn render(&self) -> String {
        self.refresh();
        let mut out = String::new();
        // Encoding into a String cannot fail.
        let _ = encode(&mut out, &self.registry);
        out
    }

    fn refresh(&self) {
        // Land queues: summed per label, recomputed from scratch so drained
        // queues disappear.
        let mut depth: std::collections::BTreeMap<String, i64> = Default::default();
        {
            let recent = self.recent_refs.lock().unwrap_or_else(|e| e.into_inner());
            for (name, len) in queue_depths() {
                *depth.entry(recent.label(&name)).or_default() += len as i64;
            }
        }
        depth.entry(OTHER.to_owned()).or_default();
        self.land_queue_depth.clear();
        for (r#ref, n) in depth {
            self.land_queue_depth.get_or_create(&RefLabels { r#ref }).set(n);
        }

        // Caches: sizes are gauges, lookups are folded in as counter deltas.
        let (stats, _, _) = cache_stats();
        for s in &stats {
            self.cache_bytes
                .get_or_create(&CacheLabels { cache: s.name })
                .set(s.weight_bytes as i64);
        }
        let totals = lookup_counters();
        let mut synced = self.cache_synced.lock().unwrap_or_else(|e| e.into_inner());
        for (i, cache) in CACHE_NAMES.into_iter().enumerate() {
            let labels = CacheLabels { cache };
            self.cache_hits.get_or_create(&labels).inc_by(totals[i].0.saturating_sub(synced[i].0));
            self.cache_misses
                .get_or_create(&labels)
                .inc_by(totals[i].1.saturating_sub(synced[i].1));
        }
        *synced = totals;
        drop(synced);

        let pg = self.pg.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(pg) = pg {
            let s = pg.pool_stats();
            self.pg_size.set(i64::from(s.size));
            self.pg_idle.set(i64::from(s.idle));
            self.pg_in_use.set(i64::from(s.in_use()));
            self.pg_max.set(i64::from(s.max));
        }
    }
}

// ---------------------------------------------------------------------------
// store wrappers
// ---------------------------------------------------------------------------

/// Pre-resolved handles for one store operation (no label lookup per call).
struct StoreOp {
    duration: Histogram,
    errors: Counter,
}

impl StoreOp {
    async fn time<T>(&self, fut: impl Future<Output = StoreResult<T>>) -> StoreResult<T> {
        let start = Instant::now();
        let out = fut.await;
        self.duration.observe(start.elapsed().as_secs_f64());
        if out.is_err() {
            self.errors.inc();
        }
        out
    }
}

struct TimedObjects {
    inner: Arc<dyn ObjectStore>,
    get_many: StoreOp,
    put_many: StoreOp,
    has_many: StoreOp,
}

#[async_trait]
impl ObjectStore for TimedObjects {
    async fn get_many(&self, ids: &[ObjectId]) -> StoreResult<Vec<Option<EncodedObject>>> {
        self.get_many.time(self.inner.get_many(ids)).await
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> StoreResult<()> {
        self.put_many.time(self.inner.put_many(objects)).await
    }

    async fn has_many(&self, ids: &[ObjectId]) -> StoreResult<Vec<bool>> {
        self.has_many.time(self.inner.has_many(ids)).await
    }
}

struct TimedRefs {
    inner: Arc<dyn RefStore>,
    get_ref: StoreOp,
    cas_ref: StoreOp,
    list_refs: StoreOp,
    lock_ref: StoreOp,
}

#[async_trait]
impl RefStore for TimedRefs {
    async fn get_ref(&self, name: &str) -> StoreResult<Option<ObjectId>> {
        self.get_ref.time(self.inner.get_ref(name)).await
    }

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ObjectId>,
        new: Option<ObjectId>,
    ) -> StoreResult<CasOutcome> {
        self.cas_ref.time(self.inner.cas_ref(name, expected, new)).await
    }

    async fn list_refs(&self, prefix: &str) -> StoreResult<Vec<(String, ObjectId)>> {
        self.list_refs.time(self.inner.list_refs(prefix)).await
    }

    async fn lock_ref(&self, name: &str) -> StoreResult<RefLock> {
        self.lock_ref.time(self.inner.lock_ref(name)).await
    }
}

struct TimedMeta {
    inner: Arc<dyn MetaStore>,
    put_commit: StoreOp,
    put_commits: StoreOp,
    get_commits: StoreOp,
    get_changeset: StoreOp,
    get_changesets: StoreOp,
    first_parent_chain: StoreOp,
    tail: StoreOp,
    get_derived: StoreOp,
    get_derived_many: StoreOp,
    put_derived: StoreOp,
    put_derived_many: StoreOp,
    index_history: StoreOp,
    query_history: StoreOp,
    history_watermark: StoreOp,
    set_history_watermark: StoreOp,
    clear_history: StoreOp,
}

// Every method is forwarded, including the batched ones that have trait
// defaults: the defaults would turn one store round trip into one per item.
#[async_trait]
impl MetaStore for TimedMeta {
    async fn put_commit(&self, node: CommitNode, changes: Vec<PathChange>) -> StoreResult<u64> {
        self.put_commit.time(self.inner.put_commit(node, changes)).await
    }

    async fn get_commits(&self, ids: &[ObjectId]) -> StoreResult<Vec<Option<CommitNode>>> {
        self.get_commits.time(self.inner.get_commits(ids)).await
    }

    async fn get_changeset(&self, id: &ObjectId) -> StoreResult<Option<Vec<PathChange>>> {
        self.get_changeset.time(self.inner.get_changeset(id)).await
    }

    async fn get_changesets(&self, ids: &[ObjectId]) -> StoreResult<Vec<Option<Vec<PathChange>>>> {
        self.get_changesets.time(self.inner.get_changesets(ids)).await
    }

    async fn first_parent_chain(
        &self,
        tip: ObjectId,
        stop: Option<ObjectId>,
        limit: usize,
    ) -> StoreResult<Vec<CommitNode>> {
        self.first_parent_chain.time(self.inner.first_parent_chain(tip, stop, limit)).await
    }

    async fn tail(&self, after: u64, limit: usize) -> StoreResult<Vec<(u64, ObjectId)>> {
        self.tail.time(self.inner.tail(after, limit)).await
    }

    async fn get_derived(&self, kind: &str, key: &[u8]) -> StoreResult<Option<Bytes>> {
        self.get_derived.time(self.inner.get_derived(kind, key)).await
    }

    async fn put_derived(&self, kind: &str, key: &[u8], value: Bytes) -> StoreResult<()> {
        self.put_derived.time(self.inner.put_derived(kind, key, value)).await
    }

    async fn index_history(&self, ref_name: &str, rows: &[HistoryRow]) -> StoreResult<()> {
        self.index_history.time(self.inner.index_history(ref_name, rows)).await
    }

    async fn query_history(
        &self,
        ref_name: &str,
        prefix: &RepoPath,
        max_generation: u64,
        limit: usize,
    ) -> StoreResult<Vec<(u64, ObjectId)>> {
        self.query_history
            .time(self.inner.query_history(ref_name, prefix, max_generation, limit))
            .await
    }

    async fn history_watermark(&self, ref_name: &str) -> StoreResult<Option<ObjectId>> {
        self.history_watermark.time(self.inner.history_watermark(ref_name)).await
    }

    async fn set_history_watermark(
        &self,
        ref_name: &str,
        expected: Option<ObjectId>,
        new: ObjectId,
    ) -> StoreResult<bool> {
        self.set_history_watermark
            .time(self.inner.set_history_watermark(ref_name, expected, new))
            .await
    }

    async fn clear_history(&self, ref_name: &str) -> StoreResult<()> {
        self.clear_history.time(self.inner.clear_history(ref_name)).await
    }

    async fn get_derived_many(
        &self,
        kind: &str,
        keys: &[&[u8]],
    ) -> StoreResult<Vec<Option<Bytes>>> {
        self.get_derived_many.time(self.inner.get_derived_many(kind, keys)).await
    }

    async fn put_commits(
        &self,
        commits: Vec<(CommitNode, Vec<PathChange>)>,
    ) -> StoreResult<Vec<u64>> {
        self.put_commits.time(self.inner.put_commits(commits)).await
    }

    async fn put_derived_many(
        &self,
        kind: &str,
        entries: Vec<(Vec<u8>, Bytes)>,
    ) -> StoreResult<()> {
        self.put_derived_many.time(self.inner.put_derived_many(kind, entries)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recent_refs_cap_and_fold() {
        let mut r = RecentRefs::default();
        for i in 0..MAX_REF_LABELS {
            let (_, evicted) = r.touch(&format!("r{i}"));
            assert!(evicted.is_none());
        }
        // Touching an old ref keeps it; a new one pushes out the oldest.
        r.touch("r0");
        let (_, evicted) = r.touch("new");
        assert_eq!(evicted.as_deref(), Some("r1"));
        assert_eq!(r.label("r0"), "r0");
        assert_eq!(r.label("r1"), OTHER);
        assert_eq!(r.0.len(), MAX_REF_LABELS);
    }

    #[test]
    fn render_lists_closed_label_sets_and_build_info() {
        let text = Metrics::new().render();
        assert!(text.contains("ant_build_info{"), "{text}");
        assert!(text.contains(r#"ant_land_outcomes_total{outcome="denied"} 0"#), "{text}");
        assert!(text.contains(r#"ant_cache_hits_total{cache="trees"}"#), "{text}");
        assert!(text.trim_end().ends_with("# EOF"));
    }
}
