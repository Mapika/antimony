//! The HTTP front of the cache tier (see the crate docs for the design).

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ant_core::{EncodedObject, ObjectId};
use ant_store::ObjectStore;
use antd::disk::DiskObjectStore;
use antd::retry::{RETRY_AFTER_MS, RetryPolicy, send_get};
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use parking_lot::Mutex;

use crate::authz::{AuthzState, CredKey, Outcome, Pending, Slot, Verdict, cred_key};
use crate::index::Index;
use crate::metrics::{CONTENT_TYPE as METRICS_CONTENT_TYPE, Metrics};

const OBJECT_CONTENT_TYPE: &str = "application/x-ant-object";
const CACHE_IMMUTABLE: &str = "public, max-age=31536000, immutable";
const CACHE_PRIVATE: &str = "private, max-age=31536000, immutable";
const CACHE_NO_STORE: &str = "no-store";
const CACHE_PRIVATE_NO_STORE: &str = "private, no-store";
/// Header marking a `POST /o/check` answer (`ant-server`'s `http`).
const CHECK_HEADER: &str = "x-ant-check";
/// Response header telling how the cache served a request.
pub const CACHE_HEADER: &str = "x-ant-cache";
/// Largest decompressed bundle accepted from an upstream.
const MAX_BUNDLE_BYTES: usize = 256 * 1024 * 1024;
/// Most object ids sent in one `POST /o/check` (the origin's limit).
const CHECK_MAX_IDS: usize = 4096;
/// Background bundle ingestions at once (forward mode).
const INGEST_PARALLELISM: usize = 4;
/// Upstream headers copied into responses.
const PASS_HEADERS: [&str; 8] = [
    CHECK_HEADER,
    "content-type",
    "cache-control",
    "etag",
    "www-authenticate",
    ant_proto::bundle::HEADER,
    "retry-after",
    RETRY_AFTER_MS,
];

/// How the cache decides who may read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    /// Authorize every request with the upstream (safe for any upstream).
    Forward,
    /// The upstream enforces no policy: serve everyone from the cache.
    Open,
}

pub struct Config {
    /// Base URL of the upstream (`http://host:port`).
    pub upstream: String,
    pub auth: AuthMode,
    pub disk: Arc<DiskObjectStore>,
    /// Retry policy for upstream requests that carry a retry hint.
    pub retry: RetryPolicy,
    /// Forward mode: how long an allowed read decision for a principal is
    /// reused without asking the upstream again (`ZERO`: always ask). It
    /// bounds how long a revoked principal can still read objects this cache
    /// holds; see the crate docs.
    pub authz_ttl: Duration,
    /// Forward mode: how long permission checks wait to be batched into one
    /// request to the upstream.
    pub authz_window: Duration,
}

/// Default [`Config::authz_ttl`].
pub const DEFAULT_AUTHZ_TTL: Duration = Duration::from_secs(30);
/// Default [`Config::authz_window`].
pub const DEFAULT_AUTHZ_WINDOW: Duration = Duration::from_millis(2);

/// A buffered upstream response.
#[derive(Debug)]
pub(crate) struct Reply {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

/// The answer of `/o/check` for a list of ids.
enum Checked {
    Flags(Vec<u8>),
    Refused(Arc<Reply>),
    Unsupported,
}

/// What the upstream says about a set of ids for one caller.
pub(crate) enum Authz {
    /// One verdict per id, in order.
    Verdicts(Vec<Verdict>),
    /// The upstream refused the request: pass its response on.
    Refused(Arc<Reply>),
    /// The upstream has no batch check: authorize per object.
    Unsupported,
}

/// The answer of an object flight that found the object already on disk
/// (a fetch never sends a condition, so the upstream does not answer 304).
const ON_DISK: StatusCode = StatusCode::NOT_MODIFIED;

type FlightOut = Result<Arc<Reply>, Arc<String>>;
type FlightFut = Shared<BoxFuture<'static, FlightOut>>;

struct Flight {
    /// The credentials the fetch runs under (forward mode).
    auth: Option<String>,
    fut: FlightFut,
}

enum Role {
    /// This request started the fetch.
    Led,
    /// Same credentials as the fetch in flight: share its answer.
    Joined,
    /// A fetch under other credentials is in flight: authorize first.
    Foreign,
}

pub struct CacheServe {
    upstream: String,
    auth: AuthMode,
    disk: Arc<DiskObjectStore>,
    index: Index,
    http: reqwest::Client,
    retry: RetryPolicy,
    metrics: Arc<Metrics>,
    objects: Mutex<HashMap<ObjectId, Flight>>,
    bundles: Mutex<HashMap<ObjectId, Flight>>,
    ingest: tokio::sync::Semaphore,
    authz: AuthzState,
}

impl CacheServe {
    /// Open the cache directory's bundle index and, in open mode, check that
    /// the upstream enforces no policy.
    pub async fn connect(cfg: Config) -> anyhow::Result<Arc<Self>> {
        let index = Index::open(cfg.disk.root())?;
        let me = Arc::new(Self {
            upstream: cfg.upstream.trim_end_matches('/').to_owned(),
            auth: cfg.auth,
            disk: cfg.disk,
            index,
            http: reqwest::Client::new(),
            retry: cfg.retry,
            metrics: Metrics::new(),
            objects: Mutex::default(),
            bundles: Mutex::default(),
            ingest: tokio::sync::Semaphore::new(INGEST_PARALLELISM),
            authz: AuthzState::new(cfg.authz_ttl, cfg.authz_window),
        });
        if me.auth == AuthMode::Open {
            me.check_open_upstream().await?;
        }
        Ok(me)
    }

    async fn check_open_upstream(&self) -> anyhow::Result<()> {
        let url = format!("{}/debug/stats", self.upstream);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("cannot reach the upstream {}: {e}", self.upstream))?;
        if resp.status() != reqwest::StatusCode::OK {
            anyhow::bail!(
                "upstream {} did not answer an unauthenticated GET /debug/stats with 200 (got {}): \
                 it enforces an access policy (or is not an open antimony server), so an open \
                 cache would not be safe; use `--auth forward`",
                self.upstream,
                resp.status()
            );
        }
        Ok(())
    }

    /// Behave as against an origin without `POST /o/check` (per-object
    /// conditional reads, forwarded bundles). For benchmarks and tests that
    /// compare with the batch endpoint.
    #[doc(hidden)]
    pub fn disable_batch_checks(&self) {
        self.authz.disable();
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// Enforce the disk budget, prune the bundle index and refresh the disk
    /// gauges, now.
    pub async fn maintain(&self) {
        let index = self.index.clone();
        let budget = self.disk.budget();
        let _ = tokio::task::spawn_blocking(move || index.prune((budget / 50).max(16 << 20))).await;
        if let Err(e) = self.disk.maintain().await {
            tracing::warn!("cache maintenance failed: {e}");
        }
        self.refresh_gauges().await;
    }

    /// Run [`maintain`](Self::maintain) every `every` until the process ends.
    pub fn spawn_maintenance(self: &Arc<Self>, every: Duration) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            loop {
                tick.tick().await;
                let Some(me) = weak.upgrade() else { return };
                me.maintain().await;
            }
        })
    }

    async fn refresh_gauges(&self) {
        let index = self.index.clone();
        let bytes = tokio::task::spawn_blocking(move || index.bytes()).await.unwrap_or(0);
        self.metrics.set_disk(self.disk.tracked_usage(), self.disk.budget(), bytes);
    }

    /// The HTTP routes.
    pub fn router(self: &Arc<Self>) -> Router {
        let mut router = Router::new()
            .route("/o/{hex}", get(get_object))
            .route("/t/{hex}", get(get_bundle))
            .route("/healthz", get(|| async { "ok" }))
            .route("/readyz", get(|| async { "ok" }))
            .route("/metrics", get(scrape));
        if self.auth == AuthMode::Open {
            // Lets an open cache sit in front of this one (see the crate docs).
            router = router.route("/debug/stats", get(stats));
        }
        router.with_state(self.clone())
    }

    /// Serve on `listener` until it fails.
    pub async fn serve(self: Arc<Self>, listener: tokio::net::TcpListener) -> std::io::Result<()> {
        use axum::serve::ListenerExt;
        let app = self.router();
        let listener = listener.tap_io(|tcp| {
            let _ = tcp.set_nodelay(true);
        });
        axum::serve(listener, app).await
    }

    // -- upstream ---------------------------------------------------------

    /// The credentials to forward for a request: the caller's in forward
    /// mode, none in open mode.
    fn credentials(&self, headers: &HeaderMap) -> Option<String> {
        match self.auth {
            AuthMode::Open => None,
            AuthMode::Forward => {
                headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).map(str::to_owned)
            }
        }
    }

    /// One buffered `GET {upstream}{path}`.
    async fn upstream_get(
        &self,
        path: &str,
        route: &'static str,
        kind: &'static str,
        auth: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<Reply, String> {
        let mut req = self.http.get(format!("{}{path}", self.upstream));
        if let Some(a) = auth {
            req = req.header(header::AUTHORIZATION, a);
        }
        if let Some(e) = if_none_match {
            req = req.header(header::IF_NONE_MATCH, e);
        }
        let result = async { collect_reply(send_get(req, &self.retry).await?).await }.await;
        match result {
            Ok(reply) => {
                self.metrics.upstream(route, kind, reply.status.as_u16(), reply.body.len());
                Ok(reply)
            }
            Err(e) => {
                self.metrics.upstream(route, kind, 0, 0);
                Err(format!("upstream request failed: {e}"))
            }
        }
    }

    // -- permission checks (forward mode) ----------------------------------

    /// One `POST /o/check` for at most [`CHECK_MAX_IDS`] ids.
    async fn check_chunk(
        &self,
        route: &'static str,
        auth: Option<&str>,
        ids: &[ObjectId],
    ) -> Result<Checked, String> {
        let body: Vec<u8> = ids.iter().flat_map(|id| id.0).collect();
        let mut req = self
            .http
            .post(format!("{}/o/check", self.upstream))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(body);
        if let Some(a) = auth {
            req = req.header(header::AUTHORIZATION, a);
        }
        let reply = match async { collect_reply(req.send().await?).await }.await {
            Ok(r) => r,
            Err(e) => {
                self.metrics.upstream(route, "check", 0, 0);
                return Err(format!("upstream request failed: {e}"));
            }
        };
        self.metrics.upstream(route, "check", reply.status.as_u16(), reply.body.len());
        match reply.status {
            StatusCode::OK => {
                let marked = reply.headers.get(CHECK_HEADER).is_some_and(|v| v == "1");
                if marked && reply.body.len() == ids.len() {
                    Ok(Checked::Flags(reply.body.to_vec()))
                } else {
                    Err("upstream sent a malformed check response".to_owned())
                }
            }
            // An origin without the endpoint (the route exists for GET only,
            // or not at all): fall back to per-object conditional reads.
            StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED => {
                self.authz.mark_unsupported(Instant::now());
                Ok(Checked::Unsupported)
            }
            _ => Ok(Checked::Refused(Arc::new(reply))),
        }
    }

    /// Ask the upstream about `ids` for the caller `auth`, reusing allowed
    /// decisions younger than the TTL. `coalesce`: wait a few milliseconds
    /// so concurrent callers' questions share one request (single objects);
    /// otherwise ask at once (a caller that already has a large list).
    /// Anything but a clear answer is an error or a refusal: the caller
    /// serves nothing from disk then.
    pub(crate) async fn authorize(
        self: &Arc<Self>,
        auth: Option<&str>,
        ids: &[ObjectId],
        route: &'static str,
        coalesce: bool,
    ) -> Result<Authz, String> {
        let now = Instant::now();
        if self.authz.is_unsupported(now) {
            self.metrics.authz("fallback", ids.len());
            return Ok(Authz::Unsupported);
        }
        let key = cred_key(auth);
        let mut out: Vec<Option<Verdict>> =
            ids.iter().map(|id| self.authz.lookup(&key, id, now)).collect();
        let missing: Vec<usize> = (0..ids.len()).filter(|i| out[*i].is_none()).collect();
        self.metrics.authz("cached", ids.len() - missing.len());
        if missing.is_empty() {
            return Ok(Authz::Verdicts(out.into_iter().flatten().collect()));
        }
        self.metrics.authz("checked", missing.len());
        if coalesce {
            let receivers: Vec<_> =
                missing.iter().map(|i| self.enqueue_check(key, auth, ids[*i], route)).collect();
            for (i, rx) in missing.iter().zip(receivers) {
                match rx.await {
                    Ok(Ok(Outcome::Verdict(v))) => out[*i] = Some(v),
                    Ok(Ok(Outcome::Refused(r))) => return Ok(Authz::Refused(r)),
                    Ok(Ok(Outcome::Unsupported)) => return Ok(Authz::Unsupported),
                    Ok(Err(e)) => return Err((*e).clone()),
                    Err(_) => return Err("permission check abandoned".to_owned()),
                }
            }
        } else {
            let asked = Instant::now();
            for chunk in missing.chunks(CHECK_MAX_IDS) {
                let chunk_ids: Vec<ObjectId> = chunk.iter().map(|i| ids[*i]).collect();
                match self.check_chunk(route, auth, &chunk_ids).await? {
                    Checked::Flags(flags) => {
                        let verdicts: Vec<Verdict> = flags.iter().map(|f| verdict_of(*f)).collect();
                        self.authz.remember(&key, asked, allowed_of(&chunk_ids, &verdicts));
                        for (i, v) in chunk.iter().zip(verdicts) {
                            out[*i] = Some(v);
                        }
                    }
                    Checked::Refused(r) => return Ok(Authz::Refused(r)),
                    Checked::Unsupported => return Ok(Authz::Unsupported),
                }
            }
        }
        out.into_iter()
            .collect::<Option<Vec<_>>>()
            .map(Authz::Verdicts)
            .ok_or_else(|| "incomplete permission check".to_owned())
    }

    /// Queue one object for the next batched check of `key`'s credentials.
    fn enqueue_check(
        self: &Arc<Self>,
        key: CredKey,
        auth: Option<&str>,
        id: ObjectId,
        route: &'static str,
    ) -> tokio::sync::oneshot::Receiver<Slot> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let first = {
            let mut pending = self.authz.pending.lock();
            let first = !pending.contains_key(&key);
            pending
                .entry(key)
                .or_insert_with(|| Pending { creds: auth.map(str::to_owned), waiters: Vec::new() })
                .waiters
                .push((id, tx));
            first
        };
        if first {
            let me = self.clone();
            tokio::spawn(async move {
                if !me.authz.window.is_zero() {
                    tokio::time::sleep(me.authz.window).await;
                }
                me.flush_checks(key, route).await;
            });
        }
        rx
    }

    /// Send the queued questions of `key` (in chunks) and answer the waiters.
    async fn flush_checks(&self, key: CredKey, route: &'static str) {
        let Some(batch) = self.authz.pending.lock().remove(&key) else { return };
        let mut ids: Vec<ObjectId> = batch.waiters.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        ids.dedup();
        let mut answers: HashMap<ObjectId, Slot> = HashMap::with_capacity(ids.len());
        for chunk in ids.chunks(CHECK_MAX_IDS) {
            let asked = Instant::now();
            match self.check_chunk(route, batch.creds.as_deref(), chunk).await {
                Ok(Checked::Flags(flags)) => {
                    let verdicts: Vec<Verdict> = flags.iter().map(|f| verdict_of(*f)).collect();
                    self.authz.remember(&key, asked, allowed_of(chunk, &verdicts));
                    for (id, v) in chunk.iter().zip(verdicts) {
                        answers.insert(*id, Ok(Outcome::Verdict(v)));
                    }
                }
                Ok(Checked::Refused(r)) => {
                    for id in chunk {
                        answers.insert(*id, Ok(Outcome::Refused(r.clone())));
                    }
                }
                Ok(Checked::Unsupported) => {
                    for id in chunk {
                        answers.insert(*id, Ok(Outcome::Unsupported));
                    }
                }
                Err(e) => {
                    let e = Arc::new(e);
                    for id in chunk {
                        answers.insert(*id, Err(e.clone()));
                    }
                }
            }
        }
        for (id, tx) in batch.waiters {
            let slot = answers
                .get(&id)
                .cloned()
                .unwrap_or_else(|| Err(Arc::new("permission check lost an id".to_owned())));
            let _ = tx.send(slot);
        }
    }

    /// Fetch, verify and store one object under `auth`.
    async fn fetch_object(&self, id: ObjectId, auth: Option<String>) -> FlightOut {
        let path = format!("/o/{}", id.to_hex());
        let reply = self
            .upstream_get(&path, "objects", "fetch", auth.as_deref(), None)
            .await
            .map_err(Arc::new)?;
        if reply.status == StatusCode::OK {
            let body = reply.body.clone();
            let verified = tokio::task::spawn_blocking(move || {
                let obj = EncodedObject::from_bytes(body).map_err(|e| e.to_string())?;
                obj.verify(&id).map_err(|e| e.to_string())?;
                Ok::<_, String>(obj)
            })
            .await
            .map_err(|e| Arc::new(e.to_string()))?;
            let obj =
                verified.map_err(|e| Arc::new(format!("upstream sent a corrupt object: {e}")))?;
            if let Err(e) = self.disk.put_many_trusted(vec![(id, obj)]).await {
                tracing::warn!(%id, "storing object failed: {e}");
            }
            if self.auth == AuthMode::Forward {
                // The origin just served this caller: that is a fresh decision.
                let private = reply
                    .headers
                    .get(header::CACHE_CONTROL)
                    .and_then(|v| v.to_str().ok())
                    .is_some_and(|v| v.contains("private"));
                self.authz.remember(
                    &cred_key(auth.as_deref()),
                    Instant::now(),
                    std::iter::once((id, private)),
                );
            }
        }
        Ok(Arc::new(reply))
    }

    /// Join the fetch of `id` in flight, or start one under `auth`.
    ///
    /// With `recheck`, a new flight first looks at the disk cache and, when
    /// the object is there, answers [`ON_DISK`] without asking the upstream.
    fn object_flight(
        self: &Arc<Self>,
        id: ObjectId,
        auth: Option<String>,
        recheck: bool,
    ) -> (Role, FlightFut) {
        let mut map = self.objects.lock();
        if let Some(f) = map.get(&id) {
            let role = if f.auth == auth { Role::Joined } else { Role::Foreign };
            return (role, f.fut.clone());
        }
        let me = self.clone();
        let creds = auth.clone();
        let fut: FlightFut = async move {
            let on_disk = recheck
                && matches!(
                    me.disk
                        .get_many(std::slice::from_ref(&id))
                        .await
                        .map(|mut v| v.pop().flatten()),
                    Ok(Some(_))
                );
            let out = if on_disk {
                Ok(Arc::new(Reply {
                    status: ON_DISK,
                    headers: HeaderMap::new(),
                    body: Bytes::new(),
                }))
            } else {
                me.metrics.inflight(1);
                let out = me.fetch_object(id, creds).await;
                me.metrics.inflight(-1);
                out
            };
            me.objects.lock().remove(&id);
            out
        }
        .boxed()
        .shared();
        map.insert(id, Flight { auth, fut: fut.clone() });
        (Role::Led, fut)
    }

    /// Check with the upstream that the caller may read `id`: a conditional
    /// request that the origin answers `304` for readable objects without
    /// reading them.
    async fn revalidate(&self, id: &ObjectId, auth: Option<&str>) -> Result<Reply, String> {
        let etag = format!("\"{}\"", id.to_hex());
        self.upstream_get(
            &format!("/o/{}", id.to_hex()),
            "objects",
            "revalidate",
            auth,
            Some(&etag),
        )
        .await
    }

    // -- bundles ----------------------------------------------------------

    /// Decode, verify and store a bundle body (and index it for open mode).
    async fn ingest_bundle(&self, tree: ObjectId, body: Bytes, index: bool) -> Result<(), String> {
        let decoded = tokio::task::spawn_blocking(move || {
            let frames =
                ant_proto::bundle::decode(&body, MAX_BUNDLE_BYTES).map_err(|e| e.to_string())?;
            if frames.first().map(|(id, _)| *id) != Some(tree) {
                return Err("bundle does not start with the requested tree".to_owned());
            }
            frames
                .into_iter()
                .map(|(id, data)| {
                    let obj = EncodedObject::from_bytes(data).map_err(|e| e.to_string())?;
                    obj.verify(&id).map_err(|e| e.to_string())?;
                    Ok((id, obj))
                })
                .collect::<Result<Vec<_>, String>>()
        })
        .await
        .map_err(|e| e.to_string())??;
        let ids: Vec<ObjectId> = decoded.iter().map(|(id, _)| *id).collect();
        self.disk.put_many_trusted(decoded).await.map_err(|e| e.to_string())?;
        if index {
            let ix = self.index.clone();
            tokio::task::spawn_blocking(move || ix.write(&tree, &ids))
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Open mode: the bundle of `tree` from the upstream, stored and indexed.
    async fn fetch_bundle(&self, tree: ObjectId) -> FlightOut {
        let path = format!("/t/{}", tree.to_hex());
        let reply =
            self.upstream_get(&path, "bundles", "fetch", None, None).await.map_err(Arc::new)?;
        if reply.status == StatusCode::OK {
            self.ingest_bundle(tree, reply.body.clone(), true)
                .await
                .map_err(|e| Arc::new(format!("unusable bundle from upstream: {e}")))?;
        }
        Ok(Arc::new(reply))
    }

    fn bundle_flight(self: &Arc<Self>, tree: ObjectId) -> (Role, FlightFut) {
        let mut map = self.bundles.lock();
        if let Some(f) = map.get(&tree) {
            return (Role::Joined, f.fut.clone());
        }
        let me = self.clone();
        let fut: FlightFut = async move {
            me.metrics.inflight(1);
            let out = me.fetch_bundle(tree).await;
            me.metrics.inflight(-1);
            me.bundles.lock().remove(&tree);
            out
        }
        .boxed()
        .shared();
        map.insert(tree, Flight { auth: None, fut: fut.clone() });
        (Role::Led, fut)
    }

    /// Open mode: re-encode `tree`'s indexed bundle from the disk cache;
    /// `None` when not indexed or any object is gone.
    async fn bundle_from_disk(&self, tree: ObjectId) -> Option<Vec<u8>> {
        let ix = self.index.clone();
        let ids = tokio::task::spawn_blocking(move || ix.read(&tree)).await.ok()??;
        let objects = self.disk.get_many(&ids).await.ok()?;
        let objects: Vec<(ObjectId, EncodedObject)> =
            ids.into_iter().zip(objects).map(|(id, o)| Some((id, o?))).collect::<Option<_>>()?;
        tokio::task::spawn_blocking(move || {
            ant_proto::bundle::encode(objects.iter().map(|(id, o)| (id, o.as_bytes().as_ref())))
        })
        .await
        .ok()?
        .ok()
    }
}

/// Buffer an upstream response, keeping the headers in [`PASS_HEADERS`].
async fn collect_reply(resp: reqwest::Response) -> reqwest::Result<Reply> {
    let status = resp.status();
    let mut headers = HeaderMap::new();
    for name in PASS_HEADERS {
        if let Some(v) = resp.headers().get(name) {
            headers.insert(HeaderName::from_static(name), v.clone());
        }
    }
    let body = resp.bytes().await?;
    Ok(Reply { status, headers, body })
}

/// Decode one flag byte of a `/o/check` answer.
fn verdict_of(flag: u8) -> Verdict {
    if flag & 1 != 0 { Verdict::Allowed { private: flag & 2 != 0 } } else { Verdict::Denied }
}

/// The allowed ids among `ids` with their private flags.
fn allowed_of<'a>(
    ids: &'a [ObjectId],
    verdicts: &'a [Verdict],
) -> impl Iterator<Item = (ObjectId, bool)> + 'a {
    ids.iter().zip(verdicts).filter_map(|(id, v)| match v {
        Verdict::Allowed { private } => Some((*id, *private)),
        Verdict::Denied => None,
    })
}

fn cache_control_of(private: bool) -> &'static str {
    if private { CACHE_PRIVATE } else { CACHE_IMMUTABLE }
}

// ---------------------------------------------------------------------------
// responses
// ---------------------------------------------------------------------------

fn text(status: StatusCode, msg: &'static str) -> Response {
    (status, [(header::CACHE_CONTROL, HeaderValue::from_static(CACHE_NO_STORE))], msg)
        .into_response()
}

fn bad_gateway(msg: &str) -> Response {
    tracing::warn!("{msg}");
    text(StatusCode::BAD_GATEWAY, "upstream unavailable")
}

/// A response made of an upstream reply (status, passed headers, body).
fn from_reply(reply: &Reply, tag: &'static str) -> Response {
    let mut res = Response::new(Body::from(reply.body.clone()));
    *res.status_mut() = reply.status;
    for (k, v) in &reply.headers {
        res.headers_mut().insert(k.clone(), v.clone());
    }
    res.headers_mut().insert(CACHE_HEADER, HeaderValue::from_static(tag));
    res
}

fn tag_for(reply: &Reply, forward: bool) -> &'static str {
    match reply.status.as_u16() {
        200 => "miss",
        401 | 403 => "refused",
        404 if forward => "refused",
        404 => "not_found",
        _ => "error",
    }
}

fn if_none_match_matches(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .any(|c| c == "*" || c.strip_prefix("W/").unwrap_or(c) == etag)
}

/// A local object as a response (`cache`: the `Cache-Control` to send).
fn object_response(
    me: &CacheServe,
    id: &ObjectId,
    obj: EncodedObject,
    cache: &str,
    tag: &'static str,
    request: &HeaderMap,
) -> Response {
    let etag = format!("\"{}\"", id.to_hex());
    let cache = HeaderValue::from_str(cache).unwrap_or(HeaderValue::from_static(CACHE_PRIVATE));
    if if_none_match_matches(request, &etag) {
        me.metrics.request("objects", tag);
        let mut res = Response::new(Body::empty());
        *res.status_mut() = StatusCode::NOT_MODIFIED;
        res.headers_mut().insert(header::CACHE_CONTROL, cache);
        if let Ok(v) = HeaderValue::from_str(&etag) {
            res.headers_mut().insert(header::ETAG, v);
        }
        res.headers_mut().insert(CACHE_HEADER, HeaderValue::from_static(tag));
        return res;
    }
    let bytes = obj.into_bytes();
    me.metrics.request("objects", tag);
    me.metrics.response_bytes("objects", bytes.len());
    let mut res = Response::new(Body::from(bytes));
    let h = res.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static(OBJECT_CONTENT_TYPE));
    h.insert(header::CACHE_CONTROL, cache);
    if let Ok(v) = HeaderValue::from_str(&etag) {
        h.insert(header::ETAG, v);
    }
    h.insert(CACHE_HEADER, HeaderValue::from_static(tag));
    res
}

/// Count and build the response of a fetched (or refused) upstream reply.
fn reply_response(
    me: &CacheServe,
    route: &'static str,
    reply: &Reply,
    tag: &'static str,
) -> Response {
    me.metrics.request(route, tag);
    me.metrics.response_bytes(route, reply.body.len());
    from_reply(reply, tag)
}

// ---------------------------------------------------------------------------
// handlers
// ---------------------------------------------------------------------------

async fn scrape(State(me): State<Arc<CacheServe>>) -> Response {
    me.refresh_gauges().await;
    ([(header::CONTENT_TYPE, METRICS_CONTENT_TYPE)], me.metrics.render()).into_response()
}

async fn stats(State(me): State<Arc<CacheServe>>) -> Response {
    let usage = me.disk.tracked_usage().map_or("null".to_owned(), |u| u.to_string());
    (
        [(header::CONTENT_TYPE, "application/json")],
        format!(
            r#"{{"cache_serve":true,"auth":"open","disk_bytes":{usage},"budget_bytes":{}}}"#,
            me.disk.budget()
        ),
    )
        .into_response()
}

async fn get_object(
    State(me): State<Arc<CacheServe>>,
    Path(hex): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Ok(id) = ObjectId::from_str(&hex) else {
        return text(StatusCode::BAD_REQUEST, "malformed object id");
    };
    serve_object(&me, id, &headers, true).await
}

/// `GET /o/<id>`. With `recheck`, a miss whose object lands on disk before
/// its fetch starts (another request's fetch just finished) is served from
/// disk again instead of being fetched twice.
async fn serve_object(
    me: &Arc<CacheServe>,
    id: ObjectId,
    headers: &HeaderMap,
    recheck: bool,
) -> Response {
    let auth = me.credentials(headers);
    let local = match me.disk.get_many(std::slice::from_ref(&id)).await {
        Ok(mut v) => v.pop().flatten(),
        Err(e) => {
            tracing::warn!(%id, "cache read failed: {e}");
            None
        }
    };
    match (me.auth, local) {
        (AuthMode::Open, Some(obj)) => {
            object_response(me, &id, obj, CACHE_IMMUTABLE, "hit", headers)
        }
        (AuthMode::Open, None) => match fill_object(me, id, None, recheck).await {
            Some(res) => res,
            None if recheck => Box::pin(serve_object(me, id, headers, false)).await,
            None => bad_gateway("object vanished from the cache while being served"),
        },
        (AuthMode::Forward, Some(obj)) => {
            // Never serve on our own authority: the origin must confirm,
            // by a batched permission check or, for an origin without one,
            // a conditional read.
            match me.authorize(auth.as_deref(), &[id], "objects", true).await {
                Ok(Authz::Verdicts(v)) => match v.first() {
                    Some(Verdict::Allowed { private }) => object_response(
                        me,
                        &id,
                        obj,
                        cache_control_of(*private),
                        "revalidated",
                        headers,
                    ),
                    _ => {
                        // Exactly the origin's answer for a hidden object.
                        me.metrics.request("objects", "refused");
                        let mut res = text(StatusCode::NOT_FOUND, "object not found");
                        res.headers_mut().insert(CACHE_HEADER, HeaderValue::from_static("refused"));
                        res
                    }
                },
                Ok(Authz::Refused(r)) => {
                    let tag = tag_for(&r, true);
                    reply_response(me, "objects", &r, tag)
                }
                Ok(Authz::Unsupported) => {
                    revalidated_hit(me, &id, obj, auth.as_deref(), headers).await
                }
                Err(e) => {
                    me.metrics.request("objects", "error");
                    bad_gateway(&e)
                }
            }
        }
        (AuthMode::Forward, None) => match fill_object(me, id, auth, recheck).await {
            Some(res) => res,
            None if recheck => Box::pin(serve_object(me, id, headers, false)).await,
            None => bad_gateway("object vanished from the cache while being served"),
        },
    }
}

/// Forward-mode hit against an origin without batch checks: one conditional
/// read per object, answered `304` for readable ones without reading them.
async fn revalidated_hit(
    me: &Arc<CacheServe>,
    id: &ObjectId,
    obj: EncodedObject,
    auth: Option<&str>,
    headers: &HeaderMap,
) -> Response {
    match me.revalidate(id, auth).await {
        Ok(r) if r.status == StatusCode::NOT_MODIFIED || r.status == StatusCode::OK => {
            let cache = r
                .headers
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok())
                .unwrap_or(CACHE_PRIVATE)
                .to_owned();
            object_response(me, id, obj, &cache, "revalidated", headers)
        }
        Ok(r) => {
            let tag = tag_for(&r, true);
            reply_response(me, "objects", &r, tag)
        }
        Err(e) => {
            me.metrics.request("objects", "error");
            bad_gateway(&e)
        }
    }
}

/// Local miss: fetch under the caller's credentials (sharing a fetch in
/// flight where that is safe).
async fn fill_object(
    me: &Arc<CacheServe>,
    id: ObjectId,
    auth: Option<String>,
    recheck: bool,
) -> Option<Response> {
    let forward = me.auth == AuthMode::Forward;
    let (role, fut) = me.object_flight(id, auth.clone(), recheck);
    if matches!(role, Role::Foreign) {
        // Someone else's fetch is in flight: the caller must first prove
        // access to the origin, then may take the bytes from that fetch.
        match me.revalidate(&id, auth.as_deref()).await {
            Ok(r) if r.status == StatusCode::NOT_MODIFIED => {
                let done = fut.await;
                if done.as_ref().is_ok_and(|d| d.status == ON_DISK) {
                    // It was already on disk: serve it the normal way.
                    return None;
                }
                if let Ok(done) = done
                    && done.status == StatusCode::OK
                    && let Ok(Some(obj)) =
                        me.disk.get_many(&[id]).await.map(|mut v| v.pop().flatten())
                {
                    let cache = done
                        .headers
                        .get(header::CACHE_CONTROL)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or(CACHE_PRIVATE)
                        .to_owned();
                    return Some(object_response(
                        me,
                        &id,
                        obj,
                        &cache,
                        "joined",
                        &HeaderMap::new(),
                    ));
                }
                // The shared fetch failed: fetch under our own credentials.
                return Some(match me.fetch_object(id, auth).await {
                    Ok(r) => {
                        let tag = tag_for(&r, forward);
                        reply_response(me, "objects", &r, tag)
                    }
                    Err(e) => {
                        me.metrics.request("objects", "error");
                        bad_gateway(&e)
                    }
                });
            }
            Ok(r) if r.status == StatusCode::OK => {
                // Origin sent the bytes itself (it ignored the condition).
                let tag = tag_for(&r, forward);
                return Some(reply_response(me, "objects", &r, tag));
            }
            Ok(r) => {
                let tag = tag_for(&r, forward);
                return Some(reply_response(me, "objects", &r, tag));
            }
            Err(e) => {
                me.metrics.request("objects", "error");
                return Some(bad_gateway(&e));
            }
        }
    }
    Some(match fut.await {
        Ok(r) if r.status == ON_DISK => return None,
        Ok(r) => {
            let tag = if matches!(role, Role::Joined) && r.status == StatusCode::OK {
                "joined"
            } else {
                tag_for(&r, forward)
            };
            reply_response(me, "objects", &r, tag)
        }
        Err(e) => {
            me.metrics.request("objects", "error");
            bad_gateway(&e)
        }
    })
}

async fn get_bundle(
    State(me): State<Arc<CacheServe>>,
    Path(hex): Path<String>,
    headers: HeaderMap,
) -> Response {
    let mut res = bundle_response(&me, &hex, &headers).await;
    res.headers_mut()
        .insert(HeaderName::from_static(ant_proto::bundle::HEADER), HeaderValue::from_static("1"));
    res
}

fn is_public(headers: &HeaderMap) -> bool {
    headers.get(header::CACHE_CONTROL).and_then(|v| v.to_str().ok()).is_some_and(|v| {
        let v = v.to_ascii_lowercase();
        v.contains("public") && !v.contains("private") && !v.contains("no-store")
    })
}

/// Forward mode: the bundle of `tree` for the caller `auth`, rebuilt from
/// the disk cache, or `None` to forward the request.
///
/// The index holds the objects of a bundle the origin served `public`
/// (outside-the-root-zone objects left out, the same for every caller).
/// Whether *this* caller may read each of them is asked of the origin with
/// batched `POST /o/check`s; the bundle holds exactly the objects the origin
/// would serve this caller individually, tree first. If the caller lacks any
/// of them the result is caller-specific and sent `private, no-store`, as
/// the origin does. Any doubt (not indexed, an object evicted, the tree
/// itself not readable, no batch endpoint, a refusal or error) returns
/// `None`, and the origin decides.
async fn rebuilt_bundle(
    me: &Arc<CacheServe>,
    tree: ObjectId,
    auth: Option<&str>,
) -> Option<Response> {
    let ix = me.index.clone();
    let ids = tokio::task::spawn_blocking(move || ix.read(&tree)).await.ok()??;
    if ids.first() != Some(&tree) {
        return None;
    }
    let verdicts = match me.authorize(auth, &ids, "bundles", false).await {
        Ok(Authz::Verdicts(v)) => v,
        _ => return None,
    };
    if verdicts.first() != Some(&Verdict::Allowed { private: false }) {
        return None;
    }
    let keep: Vec<ObjectId> = ids
        .iter()
        .zip(&verdicts)
        .filter(|(_, v)| matches!(v, Verdict::Allowed { private: false }))
        .map(|(id, _)| *id)
        .collect();
    let shared = !verdicts.contains(&Verdict::Denied);
    let objects = me.disk.get_many(&keep).await.ok()?;
    let objects: Vec<(ObjectId, EncodedObject)> =
        keep.into_iter().zip(objects).map(|(id, o)| Some((id, o?))).collect::<Option<_>>()?;
    let body = tokio::task::spawn_blocking(move || {
        ant_proto::bundle::encode(objects.iter().map(|(id, o)| (id, o.as_bytes().as_ref())))
    })
    .await
    .ok()?
    .ok()?;
    me.metrics.request("bundles", "revalidated");
    me.metrics.response_bytes("bundles", body.len());
    let mut res = Response::new(Body::from(body));
    let h = res.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static(ant_proto::bundle::CONTENT_TYPE));
    h.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(if shared { CACHE_IMMUTABLE } else { CACHE_PRIVATE_NO_STORE }),
    );
    h.insert(CACHE_HEADER, HeaderValue::from_static("revalidated"));
    Some(res)
}

async fn bundle_response(me: &Arc<CacheServe>, hex: &str, headers: &HeaderMap) -> Response {
    let Ok(tree) = ObjectId::from_str(hex) else {
        return text(StatusCode::BAD_REQUEST, "malformed object id");
    };
    let path = format!("/t/{hex}");
    if me.auth == AuthMode::Forward {
        // A bundle depends on the caller. When the tree's public bundle is
        // known, rebuild it from disk once the upstream has confirmed, in
        // batches, which of its objects this caller may read; otherwise
        // forward (never answer from disk on our own authority).
        let auth = me.credentials(headers);
        if let Some(res) = rebuilt_bundle(me, tree, auth.as_deref()).await {
            return res;
        }
        return match me.upstream_get(&path, "bundles", "fetch", auth.as_deref(), None).await {
            Ok(reply) => {
                // A response every caller would get (`public`) lists the
                // tree's objects for later rebuilds; waiting for an ingestion
                // slot (back-pressure on the clients) keeps a cold fill from
                // skipping most bundles. Other bundles are kept only when a
                // slot is free.
                let index = reply.status == StatusCode::OK && is_public(&reply.headers);
                let permit = if reply.status != StatusCode::OK {
                    None
                } else if index {
                    me.ingest.acquire().await.ok()
                } else {
                    me.ingest.try_acquire().ok()
                };
                if let Some(permit) = permit {
                    // Keep the verified objects for later revalidated reads.
                    permit.forget();
                    let (me2, body) = (me.clone(), reply.body.clone());
                    tokio::spawn(async move {
                        if let Err(e) = me2.ingest_bundle(tree, body, index).await {
                            tracing::debug!(%tree, "bundle not kept: {e}");
                        }
                        me2.ingest.add_permits(1);
                    });
                }
                let tag = if reply.status == StatusCode::OK {
                    "passthrough"
                } else {
                    tag_for(&reply, true)
                };
                reply_response(me, "bundles", &reply, tag)
            }
            Err(e) => {
                me.metrics.request("bundles", "error");
                bad_gateway(&e)
            }
        };
    }

    if let Some(body) = me.bundle_from_disk(tree).await {
        me.metrics.request("bundles", "hit");
        me.metrics.response_bytes("bundles", body.len());
        let mut res = Response::new(Body::from(body));
        let h = res.headers_mut();
        h.insert(header::CONTENT_TYPE, HeaderValue::from_static(ant_proto::bundle::CONTENT_TYPE));
        h.insert(header::CACHE_CONTROL, HeaderValue::from_static(CACHE_IMMUTABLE));
        h.insert(CACHE_HEADER, HeaderValue::from_static("hit"));
        return res;
    }
    let (role, fut) = me.bundle_flight(tree);
    match fut.await {
        Ok(r) => {
            let tag = if matches!(role, Role::Joined) && r.status == StatusCode::OK {
                "joined"
            } else {
                tag_for(&r, false)
            };
            reply_response(me, "bundles", &r, tag)
        }
        Err(e) => {
            me.metrics.request("bundles", "error");
            bad_gateway(&e)
        }
    }
}
