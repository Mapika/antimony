//! Forward-mode permission checks of the cache tier: the batched
//! `POST /o/check`, per-principal decisions with a TTL, the fallback to
//! conditional reads against an origin without the endpoint, failing closed,
//! and bundles rebuilt from disk.
//!
//! Most tests run against a scriptable mock origin (so grants, revocations,
//! outages and an older origin can be staged exactly); the last ones use the
//! real `ant-server`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ant_auth::{Policy, hash_token};
use ant_cache::{AuthMode, CacheServe, Config};
use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, Tree, TreeEntry};
use ant_server::acl::{self, Acl};
use ant_server::metrics::Metrics;
use ant_server::serve::spawn_local_with_metrics;
use ant_store::graph::{build_tree, write_commit};
use ant_store::{Backend, ObjectStore};
use antd::disk::DiskObjectStore;
use antd::retry::RetryPolicy;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use parking_lot::Mutex;

// ---------------------------------------------------------------------------
// a scriptable origin
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    /// Implements `POST /o/check`.
    Batch,
    /// An older origin: `POST /o/check` is a 405.
    Legacy,
    /// `POST /o/check` fails with a 500.
    Broken,
}

#[derive(Default)]
struct Counters {
    full_gets: AtomicU64,
    conditional_gets: AtomicU64,
    checks: AtomicU64,
    checked_ids: AtomicU64,
    bundles: AtomicU64,
}

struct MockInner {
    /// `objs[0]` is the tree holding the others.
    objs: Vec<(ObjectId, EncodedObject)>,
    /// Token -> ids it may not read. A token not in the map is unknown (401).
    tokens: Mutex<HashMap<String, HashSet<ObjectId>>>,
    mode: Mutex<Mode>,
    c: Counters,
}

#[derive(Clone)]
struct Mock(Arc<MockInner>);

impl Mock {
    fn start_tokens(&self, tokens: &[&str]) {
        let mut t = self.0.tokens.lock();
        for tok in tokens {
            t.insert((*tok).to_owned(), HashSet::new());
        }
    }

    fn deny(&self, token: &str, ids: &[ObjectId]) {
        self.0.tokens.lock().get_mut(token).unwrap().extend(ids.iter().copied());
    }

    fn set_mode(&self, mode: Mode) {
        *self.0.mode.lock() = mode;
    }

    fn blobs(&self) -> Vec<ObjectId> {
        self.0.objs[1..].iter().map(|(id, _)| *id).collect()
    }

    fn tree(&self) -> ObjectId {
        self.0.objs[0].0
    }

    fn requests(&self) -> u64 {
        let c = &self.0.c;
        c.full_gets.load(Ordering::SeqCst)
            + c.conditional_gets.load(Ordering::SeqCst)
            + c.checks.load(Ordering::SeqCst)
            + c.bundles.load(Ordering::SeqCst)
    }

    fn checks(&self) -> u64 {
        self.0.c.checks.load(Ordering::SeqCst)
    }

    fn checked_ids(&self) -> u64 {
        self.0.c.checked_ids.load(Ordering::SeqCst)
    }

    fn conditional_gets(&self) -> u64 {
        self.0.c.conditional_gets.load(Ordering::SeqCst)
    }

    fn bundles(&self) -> u64 {
        self.0.c.bundles.load(Ordering::SeqCst)
    }

    /// `None`: unknown token. Else the ids it is denied.
    fn denied(&self, headers: &HeaderMap) -> Option<HashSet<ObjectId>> {
        let tok = headers.get(header::AUTHORIZATION)?.to_str().ok()?.strip_prefix("Bearer ")?;
        self.0.tokens.lock().get(tok).cloned()
    }
}

fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, [(header::CACHE_CONTROL, "no-store")], "unauthorized")
        .into_response()
}

async fn mock_get(State(m): State<Mock>, Path(hex): Path<String>, headers: HeaderMap) -> Response {
    let Some(denied) = m.denied(&headers) else { return unauthorized() };
    let id: ObjectId = hex.parse().unwrap();
    let Some((_, obj)) = m.0.objs.iter().find(|(i, _)| *i == id) else {
        return (StatusCode::NOT_FOUND, [(header::CACHE_CONTROL, "no-store")], "object not found")
            .into_response();
    };
    if denied.contains(&id) {
        return (StatusCode::NOT_FOUND, [(header::CACHE_CONTROL, "no-store")], "object not found")
            .into_response();
    }
    let cc = [(header::CACHE_CONTROL, "public, max-age=31536000, immutable")];
    if headers.contains_key(header::IF_NONE_MATCH) {
        m.0.c.conditional_gets.fetch_add(1, Ordering::SeqCst);
        return (StatusCode::NOT_MODIFIED, cc).into_response();
    }
    m.0.c.full_gets.fetch_add(1, Ordering::SeqCst);
    (cc, obj.as_bytes().to_vec()).into_response()
}

async fn mock_check(State(m): State<Mock>, headers: HeaderMap, body: Bytes) -> Response {
    match *m.0.mode.lock() {
        Mode::Legacy => return StatusCode::METHOD_NOT_ALLOWED.into_response(),
        Mode::Broken => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        Mode::Batch => {}
    }
    let Some(denied) = m.denied(&headers) else { return unauthorized() };
    m.0.c.checks.fetch_add(1, Ordering::SeqCst);
    m.0.c.checked_ids.fetch_add(body.len() as u64 / 32, Ordering::SeqCst);
    let flags: Vec<u8> = body
        .chunks_exact(32)
        .map(|c| {
            let id = ObjectId(c.try_into().unwrap());
            let exists = m.0.objs.iter().any(|(i, _)| *i == id);
            u8::from(exists && !denied.contains(&id))
        })
        .collect();
    ([("x-ant-check", HeaderValue::from_static("1"))], flags).into_response()
}

async fn mock_bundle(
    State(m): State<Mock>,
    Path(hex): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Some(denied) = m.denied(&headers) else { return unauthorized() };
    m.0.c.bundles.fetch_add(1, Ordering::SeqCst);
    let id: ObjectId = hex.parse().unwrap();
    let marker = [("x-ant-bundle", "1")];
    if id != m.tree() || denied.contains(&id) {
        return (StatusCode::NOT_FOUND, marker, "object not found").into_response();
    }
    let objs: Vec<_> = m.0.objs.iter().filter(|(i, _)| !denied.contains(i)).collect();
    let cc = if objs.len() == m.0.objs.len() {
        "public, max-age=31536000, immutable"
    } else {
        "private, no-store"
    };
    let body =
        ant_proto::bundle::encode(objs.iter().map(|(i, o)| (i, o.as_bytes().as_ref()))).unwrap();
    (
        marker,
        [(header::CONTENT_TYPE, "application/x-ant-bundle"), (header::CACHE_CONTROL, cc)],
        body,
    )
        .into_response()
}

/// A mock origin holding a tree of `n` blobs; tokens `a`, `b`, `c` known.
async fn mock(n: usize) -> (Mock, String) {
    let mut objs = Vec::new();
    let mut entries = Vec::new();
    for i in 0..n {
        let blob = EncodedObject::new(ObjectKind::Blob, format!("blob {i}\n").as_bytes());
        entries.push(TreeEntry { mode: EntryMode::File, name: format!("f{i:03}"), id: blob.id() });
        objs.push(blob);
    }
    let tree = EncodedObject::new(ObjectKind::Tree, &Tree::new(entries).unwrap().encode());
    let mut all = vec![(tree.id(), tree)];
    all.extend(objs.into_iter().map(|o| (o.id(), o)));
    let m = Mock(Arc::new(MockInner {
        objs: all,
        tokens: Mutex::default(),
        mode: Mutex::new(Mode::Batch),
        c: Counters::default(),
    }));
    m.start_tokens(&["a", "b", "c", "d"]);
    let app = Router::new()
        .route("/o/check", axum::routing::post(mock_check))
        .route("/o/{hex}", get(mock_get))
        .route("/t/{hex}", get(mock_bundle))
        .with_state(m.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    (m, url)
}

// ---------------------------------------------------------------------------
// the tier
// ---------------------------------------------------------------------------

struct Tier {
    url: String,
    serve: Arc<CacheServe>,
    disk: Arc<DiskObjectStore>,
    _dir: tempfile::TempDir,
}

async fn tier(upstream: &str, ttl: Duration, window: Duration) -> Tier {
    let dir = tempfile::tempdir().unwrap();
    let disk = Arc::new(DiskObjectStore::open(dir.path(), u64::MAX / 2).unwrap());
    let serve = CacheServe::connect(Config {
        upstream: upstream.to_owned(),
        auth: AuthMode::Forward,
        disk: disk.clone(),
        retry: RetryPolicy::none(),
        authz_ttl: ttl,
        authz_window: window,
    })
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve.clone().serve(listener));
    Tier { url, serve, disk, _dir: dir }
}

async fn get_with(url: &str, token: Option<&str>) -> reqwest::Response {
    let mut req = reqwest::Client::new().get(url);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    req.send().await.unwrap()
}

fn tag(r: &reqwest::Response) -> String {
    r.headers().get("x-ant-cache").map(|v| v.to_str().unwrap().to_owned()).unwrap_or_default()
}

fn obj_url(t: &Tier, id: &ObjectId) -> String {
    format!("{}/o/{}", t.url, id.to_hex())
}

const LONG: Duration = Duration::from_secs(30);

fn bundle_ids(body: &[u8]) -> Vec<ObjectId> {
    ant_proto::bundle::decode(body, 64 << 20).unwrap().into_iter().map(|(id, _)| id).collect()
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

/// Concurrent hits by one principal share one `POST /o/check`; within the TTL
/// nothing reaches the origin; another principal is asked separately.
#[tokio::test]
async fn concurrent_hits_share_one_check_and_ttl_hits_cost_nothing() {
    let (m, url) = mock(40).await;
    let t = tier(&url, LONG, Duration::from_millis(50)).await;
    let blobs = m.blobs();
    // Warm the disk through principal a (a fetch: one GET each, no checks).
    for id in &blobs {
        assert_eq!(get_with(&obj_url(&t, id), Some("a")).await.status(), 200);
    }
    assert_eq!(m.checks(), 0);

    // b was never asked: its 40 concurrent reads become one check of 40 ids.
    let reads: Vec<_> =
        blobs.iter().map(|id| async { get_with(&obj_url(&t, id), Some("b")).await }).collect();
    for r in futures::future::join_all(reads).await {
        assert_eq!(r.status(), 200);
        assert_eq!(tag(&r), "revalidated");
    }
    assert_eq!((m.checks(), m.checked_ids()), (1, 40));
    assert_eq!(m.conditional_gets(), 0);

    // Within the TTL: no origin request at all, for b or for a.
    let before = m.requests();
    for tok in ["a", "b"] {
        for id in &blobs {
            let r = get_with(&obj_url(&t, id), Some(tok)).await;
            assert_eq!((r.status().as_u16(), tag(&r)), (200, "revalidated".to_owned()));
        }
    }
    assert_eq!(m.requests(), before, "decisions are reused");

    let text = t.serve.metrics().render();
    assert!(text.contains(r#"ant_cacheserve_authz_total{result="checked"} 40"#), "{text}");
    assert!(text.contains(r#"kind="check""#), "{text}");
    // Metrics and the exposition carry no credentials.
    assert!(!text.contains("Bearer") && !text.contains(r#"="a""#));
}

/// The TTL contract: an allowed decision is reused for at most the TTL, after
/// which the origin is asked again; a decision is per credential and never
/// serves another principal; a denial is never remembered.
#[tokio::test]
async fn revocation_takes_effect_within_the_ttl_and_never_leaks_across_principals() {
    let ttl = Duration::from_millis(1500);
    let (m, url) = mock(3).await;
    let t = tier(&url, ttl, Duration::from_millis(1)).await;
    let id = m.blobs()[0];
    let u = obj_url(&t, &id);

    // a is served (the fetch counts as a decision), and so is b after a check.
    assert_eq!(get_with(&u, Some("a")).await.status(), 200);
    let started = Instant::now();
    assert_eq!(get_with(&u, Some("b")).await.status(), 200);
    assert_eq!(m.checks(), 1);

    // Revoke b at the origin; c (denied from the start) has no decision.
    m.deny("b", &[id]);
    m.deny("c", &[id]);
    // c never gets the bytes the cache holds, however warm a and b are.
    for _ in 0..3 {
        let r = get_with(&u, Some("c")).await;
        assert_eq!(r.status(), 404);
        assert!(!r.text().await.unwrap().contains("blob"));
    }
    assert_eq!(get_with(&u, None).await.status(), 401);
    assert_eq!(get_with(&u, Some("nobody")).await.status(), 401);
    // Credentials are compared whole: a header that merely resembles a's.
    let r = reqwest::Client::new()
        .get(&u)
        .header(header::AUTHORIZATION, "bearer a")
        .send()
        .await
        .unwrap();
    assert_ne!(r.status(), 200, "a different header is a different principal");

    // Inside the TTL a revoked principal may still be served: that is the
    // documented window (only asserted if the machine was fast enough).
    if started.elapsed() < ttl - Duration::from_millis(300) {
        assert_eq!(get_with(&u, Some("b")).await.status(), 200, "decision still valid");
    }
    // After the TTL the origin is asked again and refuses.
    tokio::time::sleep(ttl.saturating_sub(started.elapsed()) + Duration::from_millis(100)).await;
    let checks = m.checks();
    let r = get_with(&u, Some("b")).await;
    assert_eq!(r.status(), 404);
    assert!(!r.text().await.unwrap().contains("blob"));
    assert!(m.checks() > checks, "expired decision: asked the origin");
    // A denial is not remembered: re-granting works at once.
    m.0.tokens.lock().get_mut("b").unwrap().clear();
    assert_eq!(get_with(&u, Some("b")).await.status(), 200);
}

/// `ttl = 0` asks the origin every time: revocation is immediate.
#[tokio::test]
async fn zero_ttl_checks_every_request() {
    let (m, url) = mock(2).await;
    let t = tier(&url, Duration::ZERO, Duration::ZERO).await;
    let id = m.blobs()[0];
    let u = obj_url(&t, &id);
    assert_eq!(get_with(&u, Some("a")).await.status(), 200);
    for n in 1..=3 {
        assert_eq!(get_with(&u, Some("a")).await.status(), 200);
        assert_eq!(m.checks(), n);
    }
    m.deny("a", &[id]);
    assert_eq!(get_with(&u, Some("a")).await.status(), 404);
}

/// An origin without `POST /o/check` (404/405): per-object conditional reads
/// as before, and the missing endpoint is probed once, not per request.
#[tokio::test]
async fn an_older_origin_falls_back_to_conditional_reads() {
    let (m, url) = mock(3).await;
    m.set_mode(Mode::Legacy);
    let t = tier(&url, LONG, Duration::from_millis(1)).await;
    let blobs = m.blobs();
    for id in &blobs {
        assert_eq!(get_with(&obj_url(&t, id), Some("a")).await.status(), 200);
    }
    // b: not asked before, so it needs a check; the origin has none.
    for (n, id) in blobs.iter().enumerate() {
        let r = get_with(&obj_url(&t, id), Some("b")).await;
        assert_eq!((r.status().as_u16(), tag(&r)), (200, "revalidated".to_owned()));
        assert_eq!(m.conditional_gets(), n as u64 + 1);
    }
    // And again: still conditional reads (no decisions are kept without a
    // check endpoint, as before).
    let r = get_with(&obj_url(&t, &blobs[0]), Some("b")).await;
    assert_eq!(r.status(), 200);
    assert_eq!(m.conditional_gets(), 4);
    let text = t.serve.metrics().render();
    assert!(text.contains(r#"status="405""#), "the probe was a 405");
    assert!(!text.contains(r#"status="405"} 2"#), "probed once, then remembered: {text}");
    // Refusals still come from the origin's own answer.
    m.deny("b", &[blobs[0]]);
    let r = get_with(&obj_url(&t, &blobs[0]), Some("b")).await;
    assert_eq!(r.status(), 404);
    assert!(!r.text().await.unwrap().contains("blob"));
    assert_eq!(get_with(&obj_url(&t, &blobs[1]), Some("unknown")).await.status(), 401);
    // Bundles are forwarded to such an origin, every time.
    let before = m.bundles();
    for _ in 0..2 {
        assert_eq!(
            get_with(&format!("{}/t/{}", t.url, m.tree().to_hex()), Some("a")).await.status(),
            200
        );
    }
    assert_eq!(m.bundles(), before + 2);
}

/// Anything but a clear answer serves nothing from disk.
#[tokio::test]
async fn failures_of_the_check_fail_closed() {
    let (m, url) = mock(2).await;
    let t = tier(&url, LONG, Duration::ZERO).await;
    let id = m.blobs()[0];
    let u = obj_url(&t, &id);
    assert_eq!(get_with(&u, Some("a")).await.status(), 200);

    // The origin's check errors: its failure and no bytes for b.
    m.set_mode(Mode::Broken);
    let r = get_with(&u, Some("b")).await;
    assert_eq!(r.status(), 500, "the origin's failure, passed on");
    assert!(!r.text().await.unwrap().contains("blob"));
    m.set_mode(Mode::Batch);

    // A refused credential gets the origin's refusal, not the object.
    let r = get_with(&u, Some("nobody")).await;
    assert_eq!(r.status(), 401);
    assert!(!r.text().await.unwrap().contains("blob"));

    // The origin is gone altogether: a cached object is not served to
    // anyone without a decision (b has none; a's is cached for the TTL).
    let dead = tier("http://127.0.0.1:1", LONG, Duration::ZERO).await;
    let obj = m.0.objs[1].1.clone();
    dead.disk.put_many_trusted(vec![(id, obj)]).await.unwrap();
    for tok in [Some("a"), Some("b"), None] {
        let r = get_with(&obj_url(&dead, &id), tok).await;
        assert_eq!(r.status(), 502, "{tok:?}");
        assert!(!r.text().await.unwrap().contains("blob"));
    }
}

/// A public bundle seen once is rebuilt from disk for later callers, after
/// one batched check for the whole bundle, and holds exactly what the origin
/// would serve each caller by id.
#[tokio::test]
async fn bundles_are_rebuilt_from_disk_after_a_batched_check() {
    let (m, url) = mock(30).await;
    let t = tier(&url, LONG, Duration::from_millis(1)).await;
    let burl = format!("{}/t/{}", t.url, m.tree().to_hex());
    let all: HashSet<ObjectId> = m.0.objs.iter().map(|(i, _)| *i).collect();

    let r = get_with(&burl, Some("a")).await;
    assert_eq!((r.status().as_u16(), tag(&r)), (200, "passthrough".to_owned()));
    assert_eq!(bundle_ids(&r.bytes().await.unwrap()).len(), 31);
    // The cache keeps the bundle's objects and its listing in the background.
    for _ in 0..300 {
        let held = t.disk.has_many(&m.0.objs.iter().map(|(i, _)| *i).collect::<Vec<_>>()).await;
        if held.unwrap().iter().all(|h| *h) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(m.bundles(), 1);

    // b may read everything: rebuilt, public, one check of 31 ids.
    let r = get_with(&burl, Some("b")).await;
    assert_eq!((r.status().as_u16(), tag(&r)), (200, "revalidated".to_owned()));
    assert_eq!(r.headers().get("cache-control").unwrap(), "public, max-age=31536000, immutable");
    assert_eq!(r.headers().get("x-ant-bundle").unwrap(), "1");
    let ids = bundle_ids(&r.bytes().await.unwrap());
    assert_eq!(ids[0], m.tree());
    assert_eq!(ids.iter().copied().collect::<HashSet<_>>(), all);
    assert_eq!((m.bundles(), m.checks(), m.checked_ids()), (1, 1, 31));
    // Again within the TTL: no origin request.
    let before = m.requests();
    assert_eq!(get_with(&burl, Some("b")).await.status(), 200);
    assert_eq!(m.requests(), before);

    // c may not read one blob: the bundle omits it, as the origin's would,
    // and is not shareable.
    let hidden = m.blobs()[7];
    m.deny("c", &[hidden]);
    let direct = get_with(&format!("{url}/t/{}", m.tree().to_hex()), Some("c")).await;
    let direct_ids = bundle_ids(&direct.bytes().await.unwrap());
    let r = get_with(&burl, Some("c")).await;
    assert_eq!((r.status().as_u16(), tag(&r)), (200, "revalidated".to_owned()));
    assert_eq!(r.headers().get("cache-control").unwrap(), "private, no-store");
    let ids = bundle_ids(&r.bytes().await.unwrap());
    assert!(!ids.contains(&hidden));
    assert_eq!(ids, direct_ids);

    // A caller who may not read the tree itself, or is unknown, is sent to
    // the origin and gets its answer.
    m.deny("d", &[m.tree()]);
    let before = m.bundles();
    let r = get_with(&burl, Some("d")).await;
    assert_eq!(r.status(), 404);
    assert_eq!(m.bundles(), before + 1);
    assert_eq!(get_with(&burl, Some("nobody")).await.status(), 401);
    assert_eq!(get_with(&burl, None).await.status(), 401);
}

// ---------------------------------------------------------------------------
// against the real server
// ---------------------------------------------------------------------------

const READER: &str = "t-reader";
const WRITER: &str = "t-writer";
const SECRET: &str = "TOPSECRET\n";

fn hex(b: [u8; 32]) -> String {
    ObjectId(b).to_hex()
}

fn policy() -> Policy {
    Policy::parse(&format!(
        r#"
        [[principal]]
        name = "reader"
        token_sha256 = "{}"
        groups = ["agents"]

        [[principal]]
        name = "writer"
        token_sha256 = "{}"
        groups = ["agents", "security"]

        [[path]]
        prefix = ""
        read = ["*"]
        write = ["group:agents"]

        [[path]]
        prefix = "secrets"
        read = ["group:security"]
        write = ["group:security"]
        "#,
        hex(hash_token(READER)),
        hex(hash_token(WRITER)),
    ))
    .unwrap()
}

fn object_checks(metrics: &Metrics) -> u64 {
    metrics
        .render()
        .lines()
        .filter(|l| {
            l.starts_with("ant_http_requests_total{") && l.contains(r#"route="object_checks""#)
        })
        .map(|l| l.rsplit(' ').next().unwrap().parse::<u64>().unwrap())
        .sum()
}

/// With the real origin: a warm cache answers a principal's reads with
/// batched checks, never serves the secret to the reader even though the
/// writer's decision for it is cached, and rebuilds the root bundle for the
/// reader exactly as the origin would build it.
#[tokio::test]
async fn real_origin_checks_and_bundles() {
    let backend = Backend::in_memory();
    let files: Vec<(&str, &[u8])> = vec![
        ("src/a.rs", b"pub fn a() {}\n"),
        ("README", b"readme\n"),
        ("secrets/key", SECRET.as_bytes()),
    ];
    let root = build_tree(backend.objects.as_ref(), &files).await.unwrap();
    let sig = ant_core::Signature {
        name: "a".into(),
        email: "a@x".into(),
        time: 1_700_000_000,
        tz_offset_minutes: 0,
    };
    let commit = write_commit(
        &backend,
        &ant_core::Commit {
            tree: root,
            parents: vec![],
            author: sig.clone(),
            committer: sig,
            extra_headers: vec![],
            message: "c\n".into(),
        },
    )
    .await
    .unwrap();
    let policy = policy();
    acl::reindex(&backend, &policy, commit).await.unwrap();
    let metrics = Metrics::new();
    let origin =
        spawn_local_with_metrics(backend.clone(), Acl::new(policy), metrics.clone()).await.unwrap();
    let t = tier(&origin.http_url(), LONG, Duration::from_millis(5)).await;
    let secret = EncodedObject::new(ObjectKind::Blob, SECRET.as_bytes()).id();
    let readme = EncodedObject::new(ObjectKind::Blob, b"readme\n").id();

    // The writer's root bundle is public (secrets are left out of it for
    // everyone) and indexed; its objects are kept.
    let burl = format!("{}/t/{}", t.url, root.to_hex());
    let w = get_with(&burl, Some(WRITER)).await;
    assert_eq!(w.status(), 200);
    assert_eq!(w.headers().get("cache-control").unwrap(), "public, max-age=31536000, immutable");
    drop(w);
    for _ in 0..300 {
        if t.disk.has_many(&[readme]).await.unwrap()[0] {
            tokio::time::sleep(Duration::from_millis(100)).await;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The writer reads the secret (a fetch: decision cached for the writer).
    assert_eq!(get_with(&obj_url(&t, &secret), Some(WRITER)).await.status(), 200);

    // The reader's bundle: rebuilt, equal to the origin's own.
    let direct =
        get_with(&format!("{}/t/{}", origin.http_url(), root.to_hex()), Some(READER)).await;
    let direct_cc = direct.headers().get("cache-control").cloned();
    let direct_ids = bundle_ids(&direct.bytes().await.unwrap());
    let r = get_with(&burl, Some(READER)).await;
    assert_eq!((r.status().as_u16(), tag(&r)), (200, "revalidated".to_owned()));
    assert_eq!(r.headers().get("cache-control").cloned(), direct_cc);
    let ids = bundle_ids(&r.bytes().await.unwrap());
    assert_eq!(ids, direct_ids);
    assert!(!ids.contains(&secret));
    assert!(object_checks(&metrics) >= 1);

    // Reader reads: public objects from disk after one batched check; the
    // secret, which the writer's cached decision covers, is a 404.
    let r = get_with(&obj_url(&t, &readme), Some(READER)).await;
    assert_eq!((r.status().as_u16(), tag(&r)), (200, "revalidated".to_owned()));
    let r = get_with(&obj_url(&t, &secret), Some(READER)).await;
    assert_eq!(r.status(), 404);
    assert!(!r.text().await.unwrap().contains("TOPSECRET"));
    // Same answer as asking the origin directly.
    let direct =
        get_with(&format!("{}/o/{}", origin.http_url(), secret.to_hex()), Some(READER)).await;
    assert_eq!(direct.status(), 404);
}
