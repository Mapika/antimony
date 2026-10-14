//! Prometheus metrics of the cache tier (`GET /metrics`, OpenMetrics text),
//! in the style of `ant-server`'s: one `Metrics` per process, closed label
//! sets, counters named `*_total` in the exposition.
//!
//! - `ant_cacheserve_requests_total{route,result}`: `route` is `objects` or
//!   `bundles`; `result` is
//!   - `hit`: served from the local disk cache (open mode),
//!   - `revalidated`: served from the local disk cache after the upstream
//!     confirmed this caller may read it (forward mode),
//!   - `miss`: fetched from upstream by this request,
//!   - `joined`: waited for an upstream fetch another request had in flight,
//!   - `passthrough`: forwarded to the upstream uncached (forward-mode bundles),
//!   - `refused`: upstream refused the caller (401/403/404 on a forward
//!     revalidation or fetch),
//!   - `not_found`: upstream has no such object,
//!   - `error`: upstream or local failure (4xx/5xx, bad data).
//!
//!   The hit ratio of a tier is `(hit + revalidated) / sum`.
//! - `ant_cacheserve_response_bytes_total{route}`: body bytes sent.
//! - `ant_cacheserve_upstream_requests_total{route,kind,status}`: requests to
//!   the upstream; `kind` is `fetch` or `revalidate`; `status` is the HTTP
//!   status, or `0` for a transport error.
//! - `ant_cacheserve_upstream_bytes_total{route}`: body bytes received.
//! - `ant_cacheserve_inflight_fetches`: upstream fetches in flight.
//! - `ant_cacheserve_disk_bytes`, `ant_cacheserve_disk_budget_bytes`: the
//!   cache's tracked size (shared counter, no scan) and its budget;
//!   `ant_cacheserve_index_bytes`: the bundle index.
//! - `ant_cacheserve_build_info{version}` = 1.

use std::sync::Arc;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

/// `Content-Type` of the scrape.
pub const CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RequestLabels {
    route: &'static str,
    result: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RouteLabels {
    route: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct UpstreamLabels {
    route: &'static str,
    kind: &'static str,
    status: u16,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BuildLabels {
    version: &'static str,
}

pub struct Metrics {
    registry: Registry,
    upstream_total: std::sync::atomic::AtomicU64,
    requests: Family<RequestLabels, Counter>,
    response_bytes: Family<RouteLabels, Counter>,
    upstream_requests: Family<UpstreamLabels, Counter>,
    upstream_bytes: Family<RouteLabels, Counter>,
    inflight: Gauge,
    disk_bytes: Gauge,
    disk_budget: Gauge,
    index_bytes: Gauge,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let mut registry = Registry::default();
        let requests = Family::<RequestLabels, Counter>::default();
        registry.register(
            "ant_cacheserve_requests",
            "Requests by route and how they were served",
            requests.clone(),
        );
        let response_bytes = Family::<RouteLabels, Counter>::default();
        registry.register(
            "ant_cacheserve_response_bytes",
            "Response body bytes sent",
            response_bytes.clone(),
        );
        let upstream_requests = Family::<UpstreamLabels, Counter>::default();
        registry.register(
            "ant_cacheserve_upstream_requests",
            "Requests to the upstream by route, kind and status (0: transport error)",
            upstream_requests.clone(),
        );
        let upstream_bytes = Family::<RouteLabels, Counter>::default();
        registry.register(
            "ant_cacheserve_upstream_bytes",
            "Body bytes received from the upstream",
            upstream_bytes.clone(),
        );
        let inflight = Gauge::default();
        registry.register(
            "ant_cacheserve_inflight_fetches",
            "Upstream fetches in flight",
            inflight.clone(),
        );
        let disk_bytes = Gauge::default();
        registry.register(
            "ant_cacheserve_disk_bytes",
            "Bytes in the disk cache (shared counter)",
            disk_bytes.clone(),
        );
        let disk_budget = Gauge::default();
        registry.register(
            "ant_cacheserve_disk_budget_bytes",
            "Size budget of the disk cache",
            disk_budget.clone(),
        );
        let index_bytes = Gauge::default();
        registry.register(
            "ant_cacheserve_index_bytes",
            "Bytes of the bundle index",
            index_bytes.clone(),
        );
        let build = Family::<BuildLabels, Gauge>::default();
        registry.register("ant_cacheserve_build_info", "Build information", build.clone());
        build.get_or_create(&BuildLabels { version: env!("CARGO_PKG_VERSION") }).set(1);
        Arc::new(Self {
            registry,
            upstream_total: Default::default(),
            requests,
            response_bytes,
            upstream_requests,
            upstream_bytes,
            inflight,
            disk_bytes,
            disk_budget,
            index_bytes,
        })
    }

    pub fn request(&self, route: &'static str, result: &'static str) {
        self.requests.get_or_create(&RequestLabels { route, result }).inc();
    }

    pub fn response_bytes(&self, route: &'static str, n: usize) {
        self.response_bytes.get_or_create(&RouteLabels { route }).inc_by(n as u64);
    }

    pub fn upstream(&self, route: &'static str, kind: &'static str, status: u16, bytes: usize) {
        self.upstream_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.upstream_requests.get_or_create(&UpstreamLabels { route, kind, status }).inc();
        self.upstream_bytes.get_or_create(&RouteLabels { route }).inc_by(bytes as u64);
    }

    /// Requests made to the upstream so far (all routes and kinds).
    pub fn upstream_total(&self) -> u64 {
        self.upstream_total.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn inflight(&self, delta: i64) {
        self.inflight.inc_by(delta);
    }

    pub fn set_disk(&self, bytes: Option<u64>, budget: u64, index: u64) {
        if let Some(b) = bytes {
            self.disk_bytes.set(b.min(i64::MAX as u64) as i64);
        }
        self.disk_budget.set(budget.min(i64::MAX as u64) as i64);
        self.index_bytes.set(index.min(i64::MAX as u64) as i64);
    }

    /// The scrape text.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let _ = encode(&mut out, &self.registry);
        out
    }
}
