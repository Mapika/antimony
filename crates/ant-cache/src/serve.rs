//! The HTTP front of the cache tier (see the crate docs for the design).

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

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

use crate::index::Index;
use crate::metrics::{CONTENT_TYPE as METRICS_CONTENT_TYPE, Metrics};

const OBJECT_CONTENT_TYPE: &str = "application/x-ant-object";
const CACHE_IMMUTABLE: &str = "public, max-age=31536000, immutable";
const CACHE_PRIVATE: &str = "private, max-age=31536000, immutable";
const CACHE_NO_STORE: &str = "no-store";
/// Response header telling how the cache served a request.
pub const CACHE_HEADER: &str = "x-ant-cache";
/// Largest decompressed bundle accepted from an upstream.
const MAX_BUNDLE_BYTES: usize = 256 * 1024 * 1024;
/// Background bundle ingestions at once (forward mode).
const INGEST_PARALLELISM: usize = 4;
/// Upstream headers copied into responses.
const PASS_HEADERS: [&str; 7] = [
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
}

/// A buffered upstream response.
#[derive(Debug)]
struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

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
        let result = async {
            let resp = send_get(req, &self.retry).await?;
            let status = resp.status();
            let mut headers = HeaderMap::new();
            for name in PASS_HEADERS {
                if let Some(v) = resp.headers().get(name) {
                    headers.insert(HeaderName::from_static(name), v.clone());
                }
            }
            let body = resp.bytes().await?;
            Ok::<_, reqwest::Error>(Reply { status, headers, body })
        }
        .await;
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
        }
        Ok(Arc::new(reply))
    }

    /// Join the fetch of `id` in flight, or start one under `auth`.
    fn object_flight(self: &Arc<Self>, id: ObjectId, auth: Option<String>) -> (Role, FlightFut) {
        let mut map = self.objects.lock();
        if let Some(f) = map.get(&id) {
            let role = if f.auth == auth { Role::Joined } else { Role::Foreign };
            return (role, f.fut.clone());
        }
        let me = self.clone();
        let creds = auth.clone();
        let fut: FlightFut = async move {
            me.metrics.inflight(1);
            let out = me.fetch_object(id, creds).await;
            me.metrics.inflight(-1);
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
    let auth = me.credentials(&headers);
    let local = match me.disk.get_many(std::slice::from_ref(&id)).await {
        Ok(mut v) => v.pop().flatten(),
        Err(e) => {
            tracing::warn!(%id, "cache read failed: {e}");
            None
        }
    };
    match (me.auth, local) {
        (AuthMode::Open, Some(obj)) => {
            object_response(&me, &id, obj, CACHE_IMMUTABLE, "hit", &headers)
        }
        (AuthMode::Open, None) => fill_object(&me, id, None).await,
        (AuthMode::Forward, Some(obj)) => {
            // Never serve on our own authority: the origin must confirm.
            match me.revalidate(&id, auth.as_deref()).await {
                Ok(r) if r.status == StatusCode::NOT_MODIFIED || r.status == StatusCode::OK => {
                    let cache = r
                        .headers
                        .get(header::CACHE_CONTROL)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or(CACHE_PRIVATE)
                        .to_owned();
                    object_response(&me, &id, obj, &cache, "revalidated", &headers)
                }
                Ok(r) => {
                    let tag = tag_for(&r, true);
                    reply_response(&me, "objects", &r, tag)
                }
                Err(e) => {
                    me.metrics.request("objects", "error");
                    bad_gateway(&e)
                }
            }
        }
        (AuthMode::Forward, None) => fill_object(&me, id, auth).await,
    }
}

/// Local miss: fetch under the caller's credentials (sharing a fetch in
/// flight where that is safe).
async fn fill_object(me: &Arc<CacheServe>, id: ObjectId, auth: Option<String>) -> Response {
    let forward = me.auth == AuthMode::Forward;
    let (role, fut) = me.object_flight(id, auth.clone());
    if matches!(role, Role::Foreign) {
        // Someone else's fetch is in flight: the caller must first prove
        // access to the origin, then may take the bytes from that fetch.
        match me.revalidate(&id, auth.as_deref()).await {
            Ok(r) if r.status == StatusCode::NOT_MODIFIED => {
                if let Ok(done) = fut.await
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
                    return object_response(me, &id, obj, &cache, "joined", &HeaderMap::new());
                }
                // The shared fetch failed: fetch under our own credentials.
                return match me.fetch_object(id, auth).await {
                    Ok(r) => {
                        let tag = tag_for(&r, forward);
                        reply_response(me, "objects", &r, tag)
                    }
                    Err(e) => {
                        me.metrics.request("objects", "error");
                        bad_gateway(&e)
                    }
                };
            }
            Ok(r) if r.status == StatusCode::OK => {
                // Origin sent the bytes itself (it ignored the condition).
                let tag = tag_for(&r, forward);
                return reply_response(me, "objects", &r, tag);
            }
            Ok(r) => {
                let tag = tag_for(&r, forward);
                return reply_response(me, "objects", &r, tag);
            }
            Err(e) => {
                me.metrics.request("objects", "error");
                return bad_gateway(&e);
            }
        }
    }
    match fut.await {
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
    }
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

async fn bundle_response(me: &Arc<CacheServe>, hex: &str, headers: &HeaderMap) -> Response {
    let Ok(tree) = ObjectId::from_str(hex) else {
        return text(StatusCode::BAD_REQUEST, "malformed object id");
    };
    let path = format!("/t/{hex}");
    if me.auth == AuthMode::Forward {
        // A bundle depends on the caller: forward, never answer from disk.
        let auth = me.credentials(headers);
        return match me.upstream_get(&path, "bundles", "fetch", auth.as_deref(), None).await {
            Ok(reply) => {
                if reply.status == StatusCode::OK
                    && let Ok(permit) = me.ingest.try_acquire()
                {
                    // Keep the verified objects for later revalidated reads.
                    permit.forget();
                    let (me2, body) = (me.clone(), reply.body.clone());
                    tokio::spawn(async move {
                        if let Err(e) = me2.ingest_bundle(tree, body, false).await {
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
