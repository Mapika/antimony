//! Per-principal rate limits and global load shedding, in front of the
//! handlers of both transports (a tower layer for gRPC, an axum middleware
//! for HTTP), inside the observation layers so rejections show up in the
//! `ant_rpc_*` / `ant_http_*` metrics.
//!
//! # Why
//! Thousands of agents retry with backoff. One runaway principal, or a
//! thundering herd after an outage, must not saturate the Postgres pool and
//! starve everyone else. Two independent mechanisms:
//!
//! 1. **Token buckets** per caller and rate class. Callers are principals
//!    under a policy (the bearer token's principal) and client IPs in open
//!    mode or for unknown tokens. Exceeding a bucket is rejected up front:
//!    gRPC `RESOURCE_EXHAUSTED` / HTTP `429`.
//! 2. **Concurrency gates** per class with a bounded queue. A request takes a
//!    slot (held until its response body ends), waits in the queue for at
//!    most `shed_wait` when none is free, and is *shed* when the queue is
//!    full or the wait expires: gRPC `UNAVAILABLE` / HTTP `503`.
//!
//! Every rejection happens before the handler runs, carries a retry hint
//! (gRPC trailer metadata `retry-after-ms`; HTTP `Retry-After` in seconds
//! plus `retry-after-ms`) and increments
//! `ant_ratelimit_rejected_total{class,reason}`. Because the handler never
//! ran, a client may retry *any* RPC that comes back with the hint, including
//! non-idempotent ones; `antd`'s client does exactly that and nothing else.
//!
//! # Classes
//! | class | gRPC methods / HTTP routes | rate bucket |
//! |---|---|---|
//! | `read` | `GetObjects HasObjects GetRef ListRefs ResolveRev ReadPath Log Diff Blame MergeBase IsAncestor LogRange LandedAs`, `/o/`, `/t/`, `/repo.git` | read |
//! | `write` | `PutObjects UpdateRef CreateChangeset` | write |
//! | `land` | `Land` | write |
//! | `admin` | `/debug/stats` | admin |
//!
//! `land` has its own concurrency gate but shares the `write` token bucket.
//! Lands on one ref are queued and group-committed by `ops::land`; a land
//! mostly *waits* there rather than using a Postgres connection, so its gate
//! is deliberately wide (16x the pool by default) and never the bottleneck
//! for batching. `/healthz`, `/readyz` and `/metrics` are never limited.
//!
//! # Defaults
//! Concurrency limits derive from the Postgres query-pool size `P`
//! (`--pg-max-connections`, default 64): `read` 4P, `write` 2P, `land` 16P,
//! `admin` 8; the queue holds as many waiters as the gate has slots; `shed_wait`
//! is 2 s. Rates default to `read` 2000/s (burst 4000), `write` 500/s (1000),
//! `admin` 20/s (40) *per principal* and are unlimited for open-mode/IP
//! callers unless `--rate-*` is given explicitly (an explicit flag applies to
//! every caller). A policy's `[principal.limits]` overrides per principal.
//! The in-memory fleet benchmark (16 agents, open mode) is unaffected.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ant_auth::RateSpec;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures::future::BoxFuture;
use http_body::{Body, Frame};
use pin_project_lite::pin_project;
use prometheus_client::metrics::gauge::Gauge;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower::{Layer, Service};

use crate::acl::Acl;
use crate::metrics::Metrics;

/// Trailer metadata / HTTP header carrying the server's retry hint in
/// milliseconds.
pub const RETRY_AFTER_MS: &str = "retry-after-ms";

/// Largest retry hint ever sent.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

/// Request class (see the module docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Read,
    Write,
    Land,
    Admin,
}

impl Class {
    pub const ALL: [Class; 4] = [Class::Read, Class::Write, Class::Land, Class::Admin];

    pub fn name(self) -> &'static str {
        match self {
            Class::Read => "read",
            Class::Write => "write",
            Class::Land => "land",
            Class::Admin => "admin",
        }
    }

    fn index(self) -> usize {
        self as usize
    }

    /// Index into the three rate buckets (read, write, admin).
    fn rate_index(self) -> usize {
        match self {
            Class::Read => 0,
            Class::Write | Class::Land => 1,
            Class::Admin => 2,
        }
    }
}

/// The class of a gRPC method name (`None`: not limited / unknown).
pub fn grpc_class(method: &str) -> Option<Class> {
    Some(match method {
        "GetObjects" | "HasObjects" | "GetRef" | "ListRefs" | "ResolveRev" | "ReadPath" | "Log"
        | "Diff" | "Blame" | "MergeBase" | "IsAncestor" | "LogRange" | "LandedAs" => Class::Read,
        "PutObjects" | "UpdateRef" | "CreateChangeset" => Class::Write,
        "Land" => Class::Land,
        _ => return None,
    })
}

/// The class of an HTTP path (`None`: health, readiness, metrics, 404s).
pub fn http_class(path: &str) -> Option<Class> {
    if path.starts_with("/o/")
        || path.starts_with("/t/")
        || path == "/repo.git"
        || path.starts_with("/repo.git/")
    {
        Some(Class::Read)
    } else if path == "/debug/stats" {
        Some(Class::Admin)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// configuration
// ---------------------------------------------------------------------------

/// Limiter settings. `None` rates and `0` limits mean unlimited.
#[derive(Clone, Debug)]
pub struct LimitConfig {
    /// Master switch; `false` disables both mechanisms.
    pub enabled: bool,
    /// Default rates (read, write, admin) of authenticated principals that
    /// have no policy override.
    pub principal: [Option<RateSpec>; 3],
    /// Rates of open-mode and unauthenticated callers (keyed by client IP).
    pub anonymous: [Option<RateSpec>; 3],
    /// Concurrency slots per class (`Class::index` order: read, write, land,
    /// admin); 0 = unlimited.
    pub max_inflight: [usize; 4],
    /// Waiters allowed per class beyond the slots; 0 sheds immediately.
    pub queue: [usize; 4],
    /// Longest a request waits for a slot before it is shed.
    pub shed_wait: Duration,
}

impl LimitConfig {
    /// Defaults for a Postgres query pool of `pg_max_connections`.
    pub fn for_pool(pg_max_connections: u32) -> Self {
        let p = pg_max_connections.max(1) as usize;
        let max_inflight = [4 * p, 2 * p, 16 * p, 8];
        Self {
            enabled: true,
            principal: [
                Some(RateSpec { rps: 2000.0, burst: 4000.0 }),
                Some(RateSpec { rps: 500.0, burst: 1000.0 }),
                Some(RateSpec { rps: 20.0, burst: 40.0 }),
            ],
            anonymous: [None; 3],
            max_inflight,
            queue: max_inflight,
            shed_wait: Duration::from_secs(2),
        }
    }

    /// No limiting at all.
    pub fn disabled() -> Self {
        Self { enabled: false, ..Self::default() }
    }
}

impl Default for LimitConfig {
    fn default() -> Self {
        Self::for_pool(64)
    }
}

/// Command-line flags (each also `ANT_*`), shared by `ant serve` and
/// `ant-server`.
#[derive(clap::Args, Debug, Clone, Default)]
pub struct LimitArgs {
    /// Disable rate limiting and load shedding altogether.
    #[arg(long, env = "ANT_NO_LIMITS")]
    pub no_limits: bool,

    /// Read-class rate per caller: `<rps>[:<burst>]` or `off`. When given it
    /// also applies to open-mode callers (keyed by client IP).
    #[arg(long, env = "ANT_RATE_READ", value_parser = RateSpec::parse)]
    pub rate_read: Option<RateSpec>,

    /// Write-class (changesets, lands, ref updates, uploads) rate per caller.
    #[arg(long, env = "ANT_RATE_WRITE", value_parser = RateSpec::parse)]
    pub rate_write: Option<RateSpec>,

    /// Admin-class rate per caller.
    #[arg(long, env = "ANT_RATE_ADMIN", value_parser = RateSpec::parse)]
    pub rate_admin: Option<RateSpec>,

    /// Concurrent read requests (0 = unlimited; default 4 x pg pool).
    #[arg(long, env = "ANT_MAX_INFLIGHT_READ")]
    pub max_inflight_read: Option<usize>,

    /// Concurrent write requests (default 2 x pg pool).
    #[arg(long, env = "ANT_MAX_INFLIGHT_WRITE")]
    pub max_inflight_write: Option<usize>,

    /// Concurrent Land requests, queued ones included (default 16 x pg pool).
    #[arg(long, env = "ANT_MAX_INFLIGHT_LAND")]
    pub max_inflight_land: Option<usize>,

    /// Concurrent admin requests (default 8).
    #[arg(long, env = "ANT_MAX_INFLIGHT_ADMIN")]
    pub max_inflight_admin: Option<usize>,

    /// Longest a request queues for a concurrency slot before being shed
    /// with UNAVAILABLE / 503 (`2s`, `500ms`).
    #[arg(long, env = "ANT_SHED_WAIT", value_parser = crate::config::parse_duration)]
    pub shed_wait: Option<Duration>,
}

impl LimitArgs {
    /// The settings for a Postgres pool of `pg_max_connections`.
    pub fn config(&self, pg_max_connections: u32) -> LimitConfig {
        let mut cfg = LimitConfig::for_pool(pg_max_connections);
        cfg.enabled = !self.no_limits;
        for (i, flag) in [self.rate_read, self.rate_write, self.rate_admin].into_iter().enumerate()
        {
            if let Some(spec) = flag {
                let spec = (!spec.is_unlimited()).then_some(spec);
                cfg.principal[i] = spec;
                cfg.anonymous[i] = spec;
            }
        }
        let flags = [
            self.max_inflight_read,
            self.max_inflight_write,
            self.max_inflight_land,
            self.max_inflight_admin,
        ];
        for (i, flag) in flags.into_iter().enumerate() {
            if let Some(n) = flag {
                cfg.max_inflight[i] = n;
                cfg.queue[i] = n;
            }
        }
        if let Some(w) = self.shed_wait {
            cfg.shed_wait = w;
        }
        cfg
    }
}

// ---------------------------------------------------------------------------
// token buckets
// ---------------------------------------------------------------------------

/// A token bucket: refills at `spec.rps` tokens per second up to
/// `spec.burst`; a request takes one token.
#[derive(Clone, Debug)]
pub struct Bucket {
    tokens: f64,
    last: Instant,
    spec: RateSpec,
}

impl Bucket {
    /// A full bucket at `now`.
    pub fn new(spec: RateSpec, now: Instant) -> Self {
        Self { tokens: spec.burst, last: now, spec }
    }

    fn refill(&mut self, now: Instant) {
        if now > self.last {
            let dt = now.duration_since(self.last).as_secs_f64();
            self.tokens = (self.tokens + dt * self.spec.rps).min(self.spec.burst);
            self.last = now;
        }
    }

    /// Take a token, or return how long until one is available.
    pub fn take(&mut self, now: Instant) -> Result<(), Duration> {
        self.refill(now);
        if self.tokens >= 1.0 - 1e-9 {
            self.tokens = (self.tokens - 1.0).max(0.0);
            Ok(())
        } else {
            let secs = (1.0 - self.tokens) / self.spec.rps;
            Err(Duration::from_secs_f64(secs.min(MAX_RETRY_AFTER.as_secs_f64())))
        }
    }

    /// Whether the bucket is full as of `now` (it can be forgotten: a new
    /// one starts full too).
    fn is_idle(&self, now: Instant) -> bool {
        let dt = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens + dt * self.spec.rps >= self.spec.burst
    }
}

#[derive(Default)]
struct Entry {
    buckets: [Option<Bucket>; 3],
}

impl Entry {
    fn take(&mut self, idx: usize, spec: RateSpec, now: Instant) -> Result<(), Duration> {
        self.buckets[idx].get_or_insert_with(|| Bucket::new(spec, now)).take(now)
    }

    fn is_idle(&self, now: Instant) -> bool {
        self.buckets.iter().flatten().all(|b| b.is_idle(now))
    }
}

/// Idle entries are dropped once a table grows past this many (doubling).
const PRUNE_AT: usize = 1024;

struct Table<K> {
    map: HashMap<K, Entry>,
    prune_at: usize,
}

impl<K: std::hash::Hash + Eq> Table<K> {
    fn new() -> Self {
        Self { map: HashMap::new(), prune_at: PRUNE_AT }
    }

    fn prune(&mut self, now: Instant) {
        if self.map.len() >= self.prune_at {
            self.map.retain(|_, e| !e.is_idle(now));
            self.prune_at = PRUNE_AT.max(self.map.len() * 2);
        }
    }
}

struct Shard {
    principals: Table<String>,
    ips: Table<IpAddr>,
    anon: Entry,
}

/// Who a request is accounted to.
#[derive(Clone, Copy, Debug)]
enum Who<'a> {
    Principal(&'a str),
    Ip(IpAddr),
    Anon,
}

const SHARDS: usize = 16;

/// Sharded buckets per caller and rate class.
struct RateTable {
    hasher: RandomState,
    shards: Vec<Mutex<Shard>>,
}

impl RateTable {
    fn new() -> Self {
        let shards = (0..SHARDS)
            .map(|_| {
                Mutex::new(Shard {
                    principals: Table::new(),
                    ips: Table::new(),
                    anon: Entry::default(),
                })
            })
            .collect();
        Self { hasher: RandomState::new(), shards }
    }

    fn check(
        &self,
        who: Who<'_>,
        idx: usize,
        spec: RateSpec,
        now: Instant,
    ) -> Result<(), Duration> {
        let shard = match who {
            Who::Principal(name) => self.hasher.hash_one(name),
            Who::Ip(ip) => self.hasher.hash_one(ip),
            Who::Anon => 0,
        } as usize
            % SHARDS;
        let mut shard = crate::lock(&self.shards[shard]);
        match who {
            Who::Principal(name) => {
                if !shard.principals.map.contains_key(name) {
                    shard.principals.prune(now);
                    shard.principals.map.insert(name.to_owned(), Entry::default());
                }
                match shard.principals.map.get_mut(name) {
                    Some(e) => e.take(idx, spec, now),
                    None => Ok(()),
                }
            }
            Who::Ip(ip) => {
                if !shard.ips.map.contains_key(&ip) {
                    shard.ips.prune(now);
                }
                shard.ips.map.entry(ip).or_default().take(idx, spec, now)
            }
            Who::Anon => shard.anon.take(idx, spec, now),
        }
    }
}

// ---------------------------------------------------------------------------
// concurrency gates
// ---------------------------------------------------------------------------

struct Gate {
    sem: Arc<Semaphore>,
    queue: usize,
    waiting: AtomicUsize,
    queued: Gauge,
}

/// Decrements the waiter count when a queued request stops waiting
/// (admitted, shed or cancelled).
struct Waiting<'a>(&'a Gate);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.waiting.fetch_sub(1, Ordering::AcqRel);
        self.0.queued.dec();
    }
}

impl Gate {
    async fn acquire(&self, wait: Duration) -> Result<OwnedSemaphorePermit, RejectKind> {
        if let Ok(p) = self.sem.clone().try_acquire_owned() {
            return Ok(p);
        }
        if self.waiting.fetch_add(1, Ordering::AcqRel) >= self.queue {
            self.waiting.fetch_sub(1, Ordering::AcqRel);
            return Err(RejectKind::QueueFull);
        }
        self.queued.inc();
        let _waiting = Waiting(self);
        match tokio::time::timeout(wait, self.sem.clone().acquire_owned()).await {
            Ok(Ok(p)) => Ok(p),
            _ => Err(RejectKind::QueueTimeout),
        }
    }
}

/// An admitted request's concurrency slot; dropped when its response body
/// ends (or the request is abandoned).
pub struct Permit {
    _slot: Option<OwnedSemaphorePermit>,
    inflight: Option<Gauge>,
}

impl Permit {
    fn none() -> Self {
        Self { _slot: None, inflight: None }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(g) = &self.inflight {
            g.dec();
        }
    }
}

// ---------------------------------------------------------------------------
// limiter
// ---------------------------------------------------------------------------

/// Why a request was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectKind {
    /// The caller's token bucket is empty (gRPC `RESOURCE_EXHAUSTED`, 429).
    Rate,
    /// The class's queue is full (`UNAVAILABLE`, 503).
    QueueFull,
    /// No slot freed up within `shed_wait` (`UNAVAILABLE`, 503).
    QueueTimeout,
}

impl RejectKind {
    pub fn reason(self) -> &'static str {
        match self {
            RejectKind::Rate => "rate",
            RejectKind::QueueFull => "queue_full",
            RejectKind::QueueTimeout => "queue_timeout",
        }
    }
}

/// A rejected request.
#[derive(Clone, Debug)]
pub struct Reject {
    pub class: Class,
    pub kind: RejectKind,
    /// When the caller should try again.
    pub retry_after: Duration,
}

impl Reject {
    fn message(&self) -> String {
        match self.kind {
            RejectKind::Rate => format!(
                "rate limit exceeded for {} requests; retry in {} ms",
                self.class.name(),
                self.retry_ms()
            ),
            _ => format!(
                "server overloaded ({} requests {}); retry in {} ms",
                self.class.name(),
                if self.kind == RejectKind::QueueFull { "queue full" } else { "queue timeout" },
                self.retry_ms()
            ),
        }
    }

    /// The hint in whole milliseconds, at least 1.
    pub fn retry_ms(&self) -> u64 {
        (self.retry_after.as_millis() as u64).max(1)
    }

    /// gRPC status: `RESOURCE_EXHAUSTED` for rate, `UNAVAILABLE` for shed,
    /// with `retry-after-ms` metadata.
    pub fn status(&self) -> tonic::Status {
        let mut status = match self.kind {
            RejectKind::Rate => tonic::Status::resource_exhausted(self.message()),
            _ => tonic::Status::unavailable(self.message()),
        };
        status.metadata_mut().insert(RETRY_AFTER_MS, self.retry_ms().into());
        status
    }

    /// HTTP response: 429 for rate, 503 for shed, with `Retry-After`
    /// (seconds, rounded up) and `retry-after-ms`; never cacheable.
    pub fn response(&self) -> Response {
        let code = match self.kind {
            RejectKind::Rate => StatusCode::TOO_MANY_REQUESTS,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        };
        let ms = self.retry_ms();
        let secs = ms.div_ceil(1000).max(1);
        (
            code,
            [
                (header::RETRY_AFTER, HeaderValue::from(secs)),
                (header::HeaderName::from_static(RETRY_AFTER_MS), HeaderValue::from(ms)),
                (header::CACHE_CONTROL, HeaderValue::from_static("no-store")),
            ],
            self.message(),
        )
            .into_response()
    }
}

/// The shared limiter: one per server, used by the gRPC and HTTP front ends.
pub struct Limiter {
    cfg: LimitConfig,
    acl: Acl,
    metrics: Arc<Metrics>,
    rates: RateTable,
    gates: [Option<Gate>; 4],
    inflight: [Gauge; 4],
}

impl Limiter {
    pub fn new(cfg: LimitConfig, acl: Acl, metrics: Arc<Metrics>) -> Arc<Self> {
        let gates = Class::ALL.map(|c| {
            let limit = cfg.max_inflight[c.index()];
            metrics.set_inflight_limit(c.name(), if cfg.enabled { limit } else { 0 });
            (cfg.enabled && limit > 0).then(|| Gate {
                sem: Arc::new(Semaphore::new(limit)),
                queue: cfg.queue[c.index()],
                waiting: AtomicUsize::new(0),
                queued: metrics.queued_gauge(c.name()),
            })
        });
        let inflight = Class::ALL.map(|c| metrics.inflight_gauge(c.name()));
        Arc::new(Self { cfg, acl, metrics, rates: RateTable::new(), gates, inflight })
    }

    /// A limiter that never rejects.
    pub fn disabled(acl: Acl, metrics: Arc<Metrics>) -> Arc<Self> {
        Self::new(LimitConfig::disabled(), acl, metrics)
    }

    fn reject(&self, class: Class, kind: RejectKind, retry_after: Duration) -> Reject {
        self.metrics.record_rejected(class.name(), kind.reason());
        Reject { class, kind, retry_after }
    }

    /// Retry hint for a shed request: long enough for the queue to drain a
    /// little, short enough to stay responsive.
    fn shed_hint(&self) -> Duration {
        (self.cfg.shed_wait / 2).clamp(Duration::from_millis(50), Duration::from_secs(5))
    }

    /// Admit a request of `class` made with the given `Authorization` header
    /// value and peer address, or say why not. The returned [`Permit`] must
    /// live as long as the request.
    pub async fn admit(
        &self,
        class: Class,
        authorization: Option<&str>,
        remote: Option<IpAddr>,
    ) -> Result<Permit, Reject> {
        if !self.cfg.enabled {
            return Ok(Permit::none());
        }
        self.admit_at(class, authorization, remote, Instant::now()).await
    }

    async fn admit_at(
        &self,
        class: Class,
        authorization: Option<&str>,
        remote: Option<IpAddr>,
        now: Instant,
    ) -> Result<Permit, Reject> {
        let idx = class.rate_index();
        let (who, spec) = match self.acl.principal_for_header(authorization) {
            Some(p) => {
                let overridden = self
                    .acl
                    .policy()
                    .and_then(|pol| pol.rate_overrides(&p.name))
                    .and_then(|o| [o.read, o.write, o.admin][idx]);
                let spec = overridden.or(self.cfg.principal[idx]).filter(|s| !s.is_unlimited());
                (Who::Principal(&p.name), spec)
            }
            None => (
                remote.map_or(Who::Anon, Who::Ip),
                self.cfg.anonymous[idx].filter(|s| !s.is_unlimited()),
            ),
        };
        if let Some(spec) = spec
            && let Err(wait) = self.rates.check(who, idx, spec, now)
        {
            return Err(self.reject(class, RejectKind::Rate, wait));
        }
        let slot = match &self.gates[class.index()] {
            Some(gate) => match gate.acquire(self.cfg.shed_wait).await {
                Ok(p) => Some(p),
                Err(kind) => return Err(self.reject(class, kind, self.shed_hint())),
            },
            None => None,
        };
        let gauge = self.inflight[class.index()].clone();
        gauge.inc();
        Ok(Permit { _slot: slot, inflight: Some(gauge) })
    }
}

// ---------------------------------------------------------------------------
// transports
// ---------------------------------------------------------------------------

pin_project! {
    /// Response body that keeps the request's [`Permit`] until it is dropped
    /// (the transport drops it when the response has been sent).
    pub struct LimitedBody<B> {
        #[pin]
        inner: B,
        permit: Option<Permit>,
    }
}

impl<B: Default> Default for LimitedBody<B> {
    fn default() -> Self {
        Self { inner: B::default(), permit: None }
    }
}

impl<B: Body> Body for LimitedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let polled = this.inner.poll_frame(cx);
        // Free the slot as soon as the body is over, not when hyper gets
        // around to dropping it.
        if matches!(polled, Poll::Ready(None | Some(Err(_)))) {
            *this.permit = None;
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

fn authorization_of<B>(req: &http::Request<B>) -> Option<&str> {
    req.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok())
}

/// Tower layer around the whole gRPC service.
#[derive(Clone)]
pub struct GrpcLimitLayer {
    limiter: Arc<Limiter>,
}

impl GrpcLimitLayer {
    pub fn new(limiter: Arc<Limiter>) -> Self {
        Self { limiter }
    }
}

impl<S> Layer<S> for GrpcLimitLayer {
    type Service = GrpcLimit<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcLimit { inner, limiter: self.limiter.clone() }
    }
}

#[derive(Clone)]
pub struct GrpcLimit<S> {
    inner: S,
    limiter: Arc<Limiter>,
}

impl<S, B, ResB> Service<http::Request<B>> for GrpcLimit<S>
where
    S: Service<http::Request<B>, Response = http::Response<ResB>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
    ResB: Body + Default + Send + 'static,
{
    type Response = http::Response<LimitedBody<ResB>>;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let limiter = self.limiter.clone();
        let class = req
            .uri()
            .path()
            .strip_prefix("/ant.v1.Ant/")
            .and_then(grpc_class)
            .filter(|_| limiter.cfg.enabled);
        let Some(class) = class else {
            let fut = inner.call(req);
            return Box::pin(async move {
                Ok(fut.await?.map(|inner| LimitedBody { inner, permit: None }))
            });
        };
        let remote = req
            .extensions()
            .get::<tonic::transport::server::TcpConnectInfo>()
            .and_then(|i| i.remote_addr)
            .map(|a| a.ip());
        let auth = authorization_of(&req).map(str::to_owned);
        Box::pin(async move {
            match limiter.admit(class, auth.as_deref(), remote).await {
                Ok(permit) => {
                    let res = inner.call(req).await?;
                    Ok(res.map(|inner| LimitedBody { inner, permit: Some(permit) }))
                }
                Err(reject) => Ok(reject.status().into_http()),
            }
        })
    }
}

/// axum middleware (`from_fn_with_state`) for the HTTP front end.
pub async fn http_limit(State(limiter): State<Arc<Limiter>>, req: Request, next: Next) -> Response {
    let Some(class) = http_class(req.uri().path()).filter(|_| limiter.cfg.enabled) else {
        return next.run(req).await;
    };
    let remote = req.extensions().get::<ConnectInfo<SocketAddr>>().map(|c| c.0.ip());
    match limiter.admit(class, authorization_of(&req), remote).await {
        Ok(permit) => {
            let res = next.run(req).await;
            let (parts, body) = res.into_parts();
            Response::from_parts(
                parts,
                axum::body::Body::new(LimitedBody { inner: body, permit: Some(permit) }),
            )
        }
        Err(reject) => reject.response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> Instant {
        Instant::now()
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn bucket_allows_burst_then_refills_at_rate() {
        let spec = RateSpec { rps: 10.0, burst: 5.0 };
        let start = t0();
        let mut b = Bucket::new(spec, start);
        for _ in 0..5 {
            assert!(b.take(start).is_ok());
        }
        // Empty: the next token is 100 ms away.
        let wait = b.take(start).unwrap_err();
        assert!((wait.as_secs_f64() - 0.1).abs() < 1e-9, "{wait:?}");
        // Half a token's worth later: still rejected, 50 ms to go.
        let wait = b.take(start + ms(50)).unwrap_err();
        assert!((wait.as_secs_f64() - 0.05).abs() < 1e-9, "{wait:?}");
        // A full interval after the start: exactly one token.
        assert!(b.take(start + ms(100)).is_ok());
        assert!(b.take(start + ms(100)).is_err());
        // Long idle: capped at the burst, not unbounded.
        let later = start + Duration::from_secs(3600);
        for _ in 0..5 {
            assert!(b.take(later).is_ok());
        }
        assert!(b.take(later).is_err());
    }

    #[test]
    fn bucket_sustained_rate_matches_spec() {
        let spec = RateSpec { rps: 100.0, burst: 1.0 };
        let start = t0();
        let mut b = Bucket::new(spec, start);
        let mut ok = 0;
        // 10 s of attempts every millisecond.
        for i in 0..10_000u64 {
            if b.take(start + ms(i)).is_ok() {
                ok += 1;
            }
        }
        assert!((990..=1001).contains(&ok), "{ok}");
    }

    #[test]
    fn bucket_ignores_time_going_backwards() {
        let spec = RateSpec { rps: 1.0, burst: 1.0 };
        let start = t0() + Duration::from_secs(10);
        let mut b = Bucket::new(spec, start);
        assert!(b.take(start).is_ok());
        assert!(b.take(start - Duration::from_secs(5)).is_err());
    }

    #[test]
    fn bucket_retry_hint_is_capped() {
        let spec = RateSpec { rps: 0.0001, burst: 1.0 };
        let start = t0();
        let mut b = Bucket::new(spec, start);
        assert!(b.take(start).is_ok());
        assert_eq!(b.take(start).unwrap_err(), MAX_RETRY_AFTER);
    }

    #[test]
    fn table_isolates_callers_and_classes_and_prunes_idle() {
        let table = RateTable::new();
        let spec = RateSpec { rps: 1.0, burst: 2.0 };
        let now = t0();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        for who in [Who::Principal("a"), Who::Ip(ip), Who::Anon] {
            assert!(table.check(who, 0, spec, now).is_ok());
            assert!(table.check(who, 0, spec, now).is_ok());
            assert!(table.check(who, 0, spec, now).is_err(), "{who:?}");
            // Another class of the same caller has its own bucket.
            assert!(table.check(who, 1, spec, now).is_ok(), "{who:?}");
        }
        assert!(table.check(Who::Principal("b"), 0, spec, now).is_ok());
        // Many one-off IPs do not grow the table without bound once idle.
        let later = now + Duration::from_secs(60);
        for i in 0..10_000u32 {
            let ip = IpAddr::from(std::net::Ipv4Addr::from(i));
            assert!(table.check(Who::Ip(ip), 0, spec, later).is_ok());
        }
        let later2 = later + Duration::from_secs(60);
        for i in 20_000..40_000u32 {
            let ip = IpAddr::from(std::net::Ipv4Addr::from(i));
            assert!(table.check(Who::Ip(ip), 0, spec, later2).is_ok());
        }
        let total: usize = table.shards.iter().map(|s| crate::lock(s).ips.map.len()).sum();
        assert!(total < 20_000 + SHARDS * PRUNE_AT, "{total}");
    }

    fn limiter(cfg: LimitConfig) -> Arc<Limiter> {
        Limiter::new(cfg, Acl::open(), Metrics::new())
    }

    #[tokio::test]
    async fn rate_rejection_carries_a_hint_and_is_counted() {
        let mut cfg = LimitConfig::default();
        cfg.anonymous[0] = Some(RateSpec { rps: 2.0, burst: 2.0 });
        let l = limiter(cfg);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let now = t0();
        for _ in 0..2 {
            l.admit_at(Class::Read, None, Some(ip), now).await.unwrap();
        }
        let r = l.admit_at(Class::Read, None, Some(ip), now).await.err().unwrap();
        assert_eq!(r.kind, RejectKind::Rate);
        assert_eq!(r.retry_ms(), 500);
        assert_eq!(r.status().code(), tonic::Code::ResourceExhausted);
        assert_eq!(r.status().metadata().get(RETRY_AFTER_MS).unwrap(), "500");
        let res = r.response();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(res.headers().get(header::RETRY_AFTER).unwrap(), "1");
        assert_eq!(res.headers().get(RETRY_AFTER_MS).unwrap(), "500");
        // Another IP and another class are unaffected.
        let other: IpAddr = "127.0.0.2".parse().unwrap();
        l.admit_at(Class::Read, None, Some(other), now).await.unwrap();
        l.admit_at(Class::Write, None, Some(ip), now).await.unwrap();
        let text = l.metrics.render();
        assert!(
            text.contains(r#"ant_ratelimit_rejected_total{class="read",reason="rate"} 1"#),
            "{text}"
        );
    }

    #[tokio::test]
    async fn gate_queues_then_sheds_and_frees_slots() {
        let mut cfg = LimitConfig::default();
        cfg.max_inflight[0] = 2;
        cfg.queue[0] = 1;
        cfg.shed_wait = ms(150);
        let l = limiter(cfg);
        let a = l.admit(Class::Read, None, None).await.unwrap();
        let _b = l.admit(Class::Read, None, None).await.unwrap();
        assert_eq!(l.inflight[0].get(), 2);
        // One waiter fits in the queue; the second is shed at once.
        let l2 = l.clone();
        let waiter = tokio::spawn(async move { l2.admit(Class::Read, None, None).await });
        tokio::time::sleep(ms(30)).await;
        assert_eq!(l.gates[0].as_ref().unwrap().queued.get(), 1);
        let full = l.admit(Class::Read, None, None).await.err().unwrap();
        assert_eq!(full.kind, RejectKind::QueueFull);
        assert_eq!(full.status().code(), tonic::Code::Unavailable);
        assert!(full.status().metadata().get(RETRY_AFTER_MS).is_some());
        assert_eq!(full.response().status(), StatusCode::SERVICE_UNAVAILABLE);
        // Freeing a slot admits the waiter.
        drop(a);
        let c = waiter.await.unwrap().unwrap();
        assert_eq!(l.gates[0].as_ref().unwrap().queued.get(), 0);
        // With the pool full again a waiter times out.
        let t = l.admit(Class::Read, None, None).await.err().unwrap();
        assert_eq!(t.kind, RejectKind::QueueTimeout);
        drop(c);
        // Other classes have their own gates.
        l.admit(Class::Write, None, None).await.unwrap();
        // A cancelled wait does not leak queue capacity.
        let _d = l.admit(Class::Read, None, None).await.unwrap();
        for _ in 0..5 {
            let _ = tokio::time::timeout(ms(10), l.admit(Class::Read, None, None)).await;
        }
        assert_eq!(l.gates[0].as_ref().unwrap().waiting.load(Ordering::Acquire), 0);
        assert_eq!(l.gates[0].as_ref().unwrap().queued.get(), 0);
    }

    #[tokio::test]
    async fn disabled_limiter_admits_everything() {
        let l = Limiter::disabled(Acl::open(), Metrics::new());
        let mut held = Vec::new();
        for _ in 0..10_000 {
            held.push(l.admit(Class::Land, None, None).await.unwrap());
        }
    }

    #[test]
    fn classes_cover_every_rpc() {
        let proto = include_str!("../../../proto/ant.proto");
        for m in proto
            .lines()
            .filter_map(|l| l.trim().strip_prefix("rpc "))
            .filter_map(|l| l.split('(').next())
        {
            assert!(grpc_class(m).is_some(), "{m} has no limiter class");
        }
        assert_eq!(grpc_class("Land"), Some(Class::Land));
        assert_eq!(grpc_class("CreateChangeset"), Some(Class::Write));
        assert_eq!(grpc_class("Nope"), None);
        assert_eq!(http_class("/o/ab"), Some(Class::Read));
        assert_eq!(http_class("/t/ab"), Some(Class::Read));
        assert_eq!(http_class("/repo.git/info/refs"), Some(Class::Read));
        assert_eq!(http_class("/debug/stats"), Some(Class::Admin));
        for open in ["/healthz", "/readyz", "/metrics", "/other"] {
            assert_eq!(http_class(open), None, "{open}");
        }
    }

    #[test]
    fn flags_override_defaults() {
        use clap::Parser;
        #[derive(Parser)]
        struct Cli {
            #[command(flatten)]
            l: LimitArgs,
        }
        let cfg = Cli::parse_from(["x"]).l.config(10);
        assert!(cfg.enabled);
        assert_eq!(cfg.max_inflight, [40, 20, 160, 8]);
        assert!(cfg.anonymous.iter().all(Option::is_none));
        assert_eq!(cfg.principal[0].unwrap().rps, 2000.0);
        let cfg = Cli::parse_from([
            "x",
            "--rate-read",
            "50:75",
            "--rate-write",
            "off",
            "--max-inflight-land",
            "7",
            "--shed-wait",
            "250ms",
        ])
        .l
        .config(10);
        assert_eq!(cfg.anonymous[0], Some(RateSpec { rps: 50.0, burst: 75.0 }));
        assert_eq!(cfg.principal[0], cfg.anonymous[0]);
        assert_eq!((cfg.principal[1], cfg.anonymous[1]), (None, None));
        assert_eq!((cfg.max_inflight[2], cfg.queue[2]), (7, 7));
        assert_eq!(cfg.shed_wait, ms(250));
        assert!(!Cli::parse_from(["x", "--no-limits"]).l.config(10).enabled);
        assert!(Cli::try_parse_from(["x", "--rate-read", "fast"]).is_err());
    }
}
