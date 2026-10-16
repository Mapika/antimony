//! The cache tier (`ant_cache`) in front of a real `ant-server`: forward-mode
//! access control (an object the origin refuses is never served from the
//! cache, whatever the cache holds), open mode, fills, dedup, bounded disk
//! and client checkouts through it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ant_auth::{Policy, hash_token};
use ant_cache::{AuthMode, CacheServe, Config};
use ant_core::{
    Commit, EncodedObject, EntryMode, ObjectId, ObjectKind, Signature, Tree, TreeEntry,
};
use ant_server::acl::{self, Acl};
use ant_server::metrics::Metrics;
use ant_server::serve::{LocalServer, spawn_local, spawn_local_with_metrics};
use ant_store::graph::{build_tree, write_commit};
use ant_store::{Backend, ObjectStore, ObjectStoreExt};
use antd::disk::DiskObjectStore;
use antd::readthrough::ReadThrough;
use antd::remote::RemoteObjectStore;
use antd::retry::RetryPolicy;
use antd::workspace::{Sparse, Workspace};

const READER: &str = "t-reader";
const WRITER: &str = "t-writer";
const SECRET: &str = "TOPSECRET\n";

fn hex(b: [u8; 32]) -> String {
    ObjectId(b).to_hex()
}

/// Everyone reads everything but `secrets/`, which only the security group
/// (the writer) reads.
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

        [[ref]]
        pattern = "main"
        update = []
        land = ["group:agents"]
        "#,
        hex(hash_token(READER)),
        hex(hash_token(WRITER)),
    ))
    .unwrap()
}

fn blob_id(content: &str) -> ObjectId {
    EncodedObject::new(ObjectKind::Blob, content.as_bytes()).id()
}

async fn tree_at(b: &Backend, root: ObjectId, path: &str) -> ObjectId {
    let mut id = root;
    for name in path.split('/').filter(|s| !s.is_empty()) {
        let obj = b.objects.get(&id).await.unwrap().unwrap();
        let tree = Tree::decode(obj.body()).unwrap();
        id = tree.entries().iter().find(|e| e.name == name).unwrap().id;
    }
    id
}

struct Protected {
    origin: LocalServer,
    root: ObjectId,
    secrets_dir: ObjectId,
    secret: ObjectId,
    public: ObjectId,
}

/// A server with a policy and a tagged commit with public and secret files.
async fn protected() -> Protected {
    let backend = Backend::in_memory();
    let files: Vec<(&str, &[u8])> = vec![
        ("src/a.rs", b"pub fn a() {}\n"),
        ("README", b"readme\n"),
        ("secrets/key", SECRET.as_bytes()),
        ("secrets/dir/x", b"nested secret\n"),
    ];
    let root = build_tree(backend.objects.as_ref(), &files).await.unwrap();
    let sig = Signature {
        name: "a".into(),
        email: "a@x".into(),
        time: 1_700_000_000,
        tz_offset_minutes: 0,
    };
    let commit = write_commit(
        &backend,
        &Commit {
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
    let secrets_dir = tree_at(&backend, root, "secrets").await;
    let origin = spawn_local(backend, Acl::new(policy)).await.unwrap();
    Protected { origin, root, secrets_dir, secret: blob_id(SECRET), public: blob_id("readme\n") }
}

struct Tier {
    url: String,
    serve: Arc<CacheServe>,
    disk: Arc<DiskObjectStore>,
    _dir: tempfile::TempDir,
}

async fn tier_with(upstream: &str, auth: AuthMode, budget: u64) -> anyhow::Result<Tier> {
    let dir = tempfile::tempdir().unwrap();
    let disk = Arc::new(DiskObjectStore::open(dir.path(), budget).unwrap());
    let serve = CacheServe::connect(Config {
        upstream: upstream.to_owned(),
        auth,
        disk: disk.clone(),
        retry: RetryPolicy::none(),
        authz_ttl: ant_cache::DEFAULT_AUTHZ_TTL,
        authz_window: ant_cache::DEFAULT_AUTHZ_WINDOW,
    })
    .await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(serve.clone().serve(listener));
    Ok(Tier { url, serve, disk, _dir: dir })
}

async fn tier(upstream: &str, auth: AuthMode) -> Tier {
    tier_with(upstream, auth, u64::MAX / 2).await.unwrap()
}

async fn get(url: &str, token: Option<&str>) -> reqwest::Response {
    let mut req = reqwest::Client::new().get(url);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    req.send().await.unwrap()
}

fn cache_tag(r: &reqwest::Response) -> String {
    r.headers().get("x-ant-cache").map(|v| v.to_str().unwrap().to_owned()).unwrap_or_default()
}

/// Sum of the origin's `ant_http_requests_total` for the object routes.
fn origin_requests(metrics: &Metrics) -> u64 {
    metrics
        .render()
        .lines()
        .filter(|l| {
            l.starts_with("ant_http_requests_total{")
                && (l.contains(r#"route="objects""#) || l.contains(r#"route="tree_bundles""#))
        })
        .map(|l| l.rsplit(' ').next().unwrap().parse::<u64>().unwrap())
        .sum()
}

fn upstream_fetches(serve: &CacheServe, kind: &str) -> u64 {
    serve
        .metrics()
        .render()
        .lines()
        .filter(|l| {
            l.starts_with("ant_cacheserve_upstream_requests_total{")
                && l.contains(&format!(r#"kind="{kind}""#))
        })
        .map(|l| l.rsplit(' ').next().unwrap().parse::<u64>().unwrap())
        .sum()
}

fn encoded(content: &str) -> Vec<u8> {
    EncodedObject::new(ObjectKind::Blob, content.as_bytes()).as_bytes().to_vec()
}

fn bundle_ids(body: &[u8]) -> Vec<ObjectId> {
    ant_proto::bundle::decode(body, 64 << 20).unwrap().into_iter().map(|(id, _)| id).collect()
}

// ---------------------------------------------------------------------------
// access control
// ---------------------------------------------------------------------------

/// The mandatory non-leak test: whatever the cache holds, a principal the
/// origin refuses gets what the origin gives it (a 404, a 401), never bytes.
#[tokio::test]
async fn forward_mode_never_serves_what_the_origin_refuses() {
    let p = protected().await;
    let t = tier(&p.origin.http_url(), AuthMode::Forward).await;
    let url = |id: &ObjectId| format!("{}/o/{}", t.url, id.to_hex());

    // Cold: the writer may read the secret; the cache fetches and stores it.
    let r = get(&url(&p.secret), Some(WRITER)).await;
    assert_eq!(r.status(), 200);
    assert_eq!(cache_tag(&r), "miss");
    assert_eq!(r.bytes().await.unwrap().as_ref(), encoded(SECRET).as_slice());
    assert!(t.disk.has_many(&[p.secret]).await.unwrap()[0], "now cached");

    // Warm: the reader, the unauthenticated and the unknown token all get
    // the origin's refusal, byte for byte what direct requests get.
    for token in [Some(READER), None, Some("t-nobody")] {
        let direct = get(&format!("{}/o/{}", p.origin.http_url(), p.secret.to_hex()), token).await;
        let via = get(&url(&p.secret), token).await;
        assert_ne!(via.status(), 200, "{token:?}");
        assert_eq!(via.status(), direct.status(), "{token:?}");
        assert_eq!(via.headers().get("cache-control"), direct.headers().get("cache-control"));
        let (a, b) = (via.bytes().await.unwrap(), direct.bytes().await.unwrap());
        assert_eq!(a, b);
        assert!(!a.windows(SECRET.len()).any(|w| w == SECRET.as_bytes()));
    }
    // A matching If-None-Match does not help either.
    let r = reqwest::Client::new()
        .get(url(&p.secret))
        .bearer_auth(READER)
        .header("if-none-match", format!("\"{}\"", p.secret.to_hex()))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    // The writer is still served, from disk, after the origin confirmed it.
    let r = get(&url(&p.secret), Some(WRITER)).await;
    assert_eq!(r.status(), 200);
    assert_eq!(cache_tag(&r), "revalidated");
    assert_eq!(r.bytes().await.unwrap().as_ref(), encoded(SECRET).as_slice());
    // Public objects are served to the reader from disk, once cached.
    assert_eq!(get(&url(&p.public), Some(READER)).await.status(), 200);
    let r = get(&url(&p.public), Some(READER)).await;
    assert_eq!((r.status().as_u16(), cache_tag(&r)), (200, "revalidated".to_owned()));
    // No token at all gets no object, cached or not.
    assert_eq!(get(&url(&p.public), None).await.status(), 401);
}

/// Requests for one cold secret by a principal that may read it and one that
/// may not, racing through the cache's shared fetch, never leak it.
#[tokio::test]
async fn racing_principals_do_not_share_a_fetch_across_access() {
    let p = protected().await;
    for _ in 0..15 {
        let t = tier(&p.origin.http_url(), AuthMode::Forward).await;
        let url = format!("{}/o/{}", t.url, p.secret.to_hex());
        let (w, r1, r2, w2) = tokio::join!(
            get(&url, Some(WRITER)),
            get(&url, Some(READER)),
            get(&url, Some(READER)),
            get(&url, Some(WRITER)),
        );
        assert_eq!(w.status(), 200);
        assert_eq!(w2.status(), 200);
        for r in [r1, r2] {
            assert_eq!(r.status(), 404);
            assert!(
                !r.bytes().await.unwrap().windows(SECRET.len()).any(|w| w == SECRET.as_bytes())
            );
        }
    }
}

/// Bundles are caller-specific: the cache forwards them, and the objects it
/// keeps from a writer's private bundle stay behind the same checks.
#[tokio::test]
async fn bundles_are_forwarded_and_what_they_leave_behind_stays_gated() {
    let p = protected().await;
    let t = tier(&p.origin.http_url(), AuthMode::Forward).await;
    let bundle = |tree: &ObjectId, token| {
        let url = format!("{}/t/{}", t.url, tree.to_hex());
        async move { get(&url, token).await }
    };
    // The writer's bundle of secrets/ holds the secrets; the reader's is a 404.
    let w = bundle(&p.secrets_dir, Some(WRITER)).await;
    assert_eq!(w.status(), 200);
    assert_eq!(w.headers().get("x-ant-bundle").unwrap(), "1");
    assert_eq!(w.headers().get("cache-control").unwrap(), "private, no-store");
    let ids = bundle_ids(&w.bytes().await.unwrap());
    assert!(ids.contains(&p.secret));
    let r = bundle(&p.secrets_dir, Some(READER)).await;
    assert_eq!(r.status(), 404);
    assert!(r.headers().contains_key("x-ant-bundle"));
    assert_eq!(bundle(&p.secrets_dir, None).await.status(), 401);

    // The cache keeps the writer's objects (in the background)...
    for _ in 0..200 {
        if t.disk.has_many(&[p.secret]).await.unwrap()[0] {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(t.disk.has_many(&[p.secret]).await.unwrap()[0], "bundle objects were kept");
    // ...but never hands them to the reader, bundle or object.
    let url = format!("{}/o/{}", t.url, p.secret.to_hex());
    assert_eq!(get(&url, Some(READER)).await.status(), 404);
    assert_eq!(bundle(&p.secrets_dir, Some(READER)).await.status(), 404);

    // The root bundle: the reader's equals the origin's, the writer's too.
    for token in [READER, WRITER] {
        let direct =
            get(&format!("{}/t/{}", p.origin.http_url(), p.root.to_hex()), Some(token)).await;
        let via = bundle(&p.root, Some(token)).await;
        assert_eq!(via.status(), direct.status());
        assert_eq!(
            via.headers().get("cache-control"),
            direct.headers().get("cache-control"),
            "origin's caching headers pass through"
        );
        assert_eq!(
            bundle_ids(&via.bytes().await.unwrap()),
            bundle_ids(&direct.bytes().await.unwrap())
        );
    }
    let reader_ids = bundle_ids(&bundle(&p.root, Some(READER)).await.bytes().await.unwrap());
    assert!(!reader_ids.contains(&p.secret) && !reader_ids.contains(&p.secrets_dir));
}

#[tokio::test]
async fn open_mode_refuses_a_protected_upstream() {
    let p = protected().await;
    let err = tier_with(&p.origin.http_url(), AuthMode::Open, u64::MAX / 2).await.err().unwrap();
    assert!(err.to_string().contains("--auth forward"), "{err}");
    // An open server is fine, and so is a cache-serve in front of it.
    let open = spawn_local(Backend::in_memory(), Acl::open()).await.unwrap();
    let t = tier_with(&open.http_url(), AuthMode::Open, u64::MAX / 2).await.unwrap();
    tier_with(&t.url, AuthMode::Open, u64::MAX / 2).await.unwrap();
    // A forward-mode cache is not open.
    let f = tier(&open.http_url(), AuthMode::Forward).await;
    assert!(tier_with(&f.url, AuthMode::Open, u64::MAX / 2).await.is_err());
}

// ---------------------------------------------------------------------------
// open mode
// ---------------------------------------------------------------------------

struct Open {
    origin: LocalServer,
    metrics: Arc<Metrics>,
    backend: Backend,
}

async fn open_origin() -> Open {
    let backend = Backend::in_memory();
    let metrics = Metrics::new();
    let origin =
        spawn_local_with_metrics(backend.clone(), Acl::open(), metrics.clone()).await.unwrap();
    Open { origin, metrics, backend }
}

#[tokio::test]
async fn open_mode_fills_once_then_serves_from_disk() {
    let o = open_origin().await;
    let blob = EncodedObject::new(ObjectKind::Blob, b"hello cache\n");
    o.backend.objects.put(blob.clone()).await.unwrap();
    let t = tier(&o.origin.http_url(), AuthMode::Open).await;
    let url = format!("{}/o/{}", t.url, blob.id().to_hex());

    let r = get(&url, None).await;
    assert_eq!((r.status().as_u16(), cache_tag(&r)), (200, "miss".to_owned()));
    assert_eq!(r.headers().get("content-type").unwrap(), "application/x-ant-object");
    assert_eq!(r.headers().get("cache-control").unwrap(), "public, max-age=31536000, immutable");
    assert_eq!(r.bytes().await.unwrap().as_ref(), blob.as_bytes().as_ref());
    let before = origin_requests(&o.metrics);
    for _ in 0..5 {
        let r = get(&url, Some("ignored-token")).await;
        assert_eq!((r.status().as_u16(), cache_tag(&r)), (200, "hit".to_owned()));
        assert_eq!(
            r.headers().get("etag").unwrap().to_str().unwrap(),
            format!("\"{}\"", blob.id().to_hex())
        );
        assert_eq!(r.bytes().await.unwrap().as_ref(), blob.as_bytes().as_ref());
    }
    assert_eq!(origin_requests(&o.metrics), before, "hits never reach the origin");
    // If-None-Match.
    let r = reqwest::Client::new()
        .get(&url)
        .header("if-none-match", format!("\"{}\"", blob.id().to_hex()))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 304);
    // Unknown and malformed ids; 404s are not cached.
    let missing = ObjectId([9; 32]);
    let miss_url = format!("{}/o/{}", t.url, missing.to_hex());
    assert_eq!(get(&miss_url, None).await.status(), 404);
    assert_eq!(get(&miss_url, None).await.status(), 404);
    o.backend.objects.put(EncodedObject::new(ObjectKind::Blob, b"x")).await.unwrap();
    assert_eq!(get(&format!("{}/o/nothex", t.url), None).await.status(), 400);
    let later = EncodedObject::new(ObjectKind::Blob, b"appears later");
    let later_url = format!("{}/o/{}", t.url, later.id().to_hex());
    assert_eq!(get(&later_url, None).await.status(), 404);
    o.backend.objects.put(later.clone()).await.unwrap();
    assert_eq!(get(&later_url, None).await.status(), 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_misses_share_one_upstream_fetch() {
    let o = open_origin().await;
    let blob = EncodedObject::new(ObjectKind::Blob, &vec![7u8; 100_000]);
    o.backend.objects.put(blob.clone()).await.unwrap();
    let t = tier(&o.origin.http_url(), AuthMode::Open).await;
    let url = format!("{}/o/{}", t.url, blob.id().to_hex());
    let reqs: Vec<_> = (0..40).map(|_| get(&url, None)).collect();
    for r in futures::future::join_all(reqs).await {
        assert_eq!(r.status(), 200);
        assert_eq!(r.bytes().await.unwrap().as_ref(), blob.as_bytes().as_ref());
    }
    assert_eq!(upstream_fetches(&t.serve, "fetch"), 1);
    assert_eq!(origin_requests(&o.metrics), 1, "one origin request for 40 clients");
}

/// A bundle the cache has once is re-encoded from disk: same objects, no
/// upstream request, also when it was filled by per-object reads.
#[tokio::test]
async fn open_mode_bundles_are_cached() {
    let o = open_origin().await;
    let files: Vec<(String, Vec<u8>)> =
        (0..60).map(|i| (format!("d{}/f{i}", i % 4), format!("file {i}\n").into_bytes())).collect();
    let refs: Vec<(&str, &[u8])> = files.iter().map(|(p, c)| (p.as_str(), c.as_slice())).collect();
    let root = build_tree(o.backend.objects.as_ref(), &refs).await.unwrap();
    let t = tier(&o.origin.http_url(), AuthMode::Open).await;
    let url = format!("{}/t/{}", t.url, root.to_hex());

    let direct = get(&format!("{}/t/{}", o.origin.http_url(), root.to_hex()), None).await;
    let direct_ids = bundle_ids(&direct.bytes().await.unwrap());
    assert!(direct_ids.len() > 60);

    let r = get(&url, None).await;
    assert_eq!((r.status().as_u16(), cache_tag(&r)), (200, "miss".to_owned()));
    assert_eq!(r.headers().get("x-ant-bundle").unwrap(), "1");
    assert_eq!(bundle_ids(&r.bytes().await.unwrap()), direct_ids);
    let fetches = upstream_fetches(&t.serve, "fetch");
    let r = get(&url, None).await;
    assert_eq!((r.status().as_u16(), cache_tag(&r)), (200, "hit".to_owned()));
    assert_eq!(bundle_ids(&r.bytes().await.unwrap()), direct_ids, "same objects, same order");
    // The objects of the bundle are individually served from disk too.
    let r = get(&format!("{}/o/{}", t.url, direct_ids[5].to_hex()), None).await;
    assert_eq!(cache_tag(&r), "hit");
    assert_eq!(upstream_fetches(&t.serve, "fetch"), fetches);
    // 404 for a non-tree keeps the marker header.
    let r = get(&format!("{}/t/{}", t.url, ObjectId([3; 32]).to_hex()), None).await;
    assert_eq!(r.status(), 404);
    assert!(r.headers().contains_key("x-ant-bundle"));

    // Losing an object of the bundle makes it a miss that heals itself.
    t.disk.evict_to(0).await.unwrap();
    let r = get(&url, None).await;
    assert_eq!(cache_tag(&r), "miss");
    assert_eq!(bundle_ids(&r.bytes().await.unwrap()), direct_ids);
    assert_eq!(cache_tag(&get(&url, None).await), "hit");
}

/// An upstream (or a man in the middle) cannot poison the cache.
#[tokio::test]
async fn corrupt_upstream_objects_are_rejected_and_not_stored() {
    use axum::routing::get as route;
    let liar = EncodedObject::new(ObjectKind::Blob, b"not what you asked for");
    let body = liar.as_bytes().to_vec();
    let app = axum::Router::new()
        .route(
            "/o/{hex}",
            route(move || {
                let body = body.clone();
                async move { body }
            }),
        )
        .route("/debug/stats", route(|| async { "{}" }));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let up = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await });
    let t = tier(&up, AuthMode::Open).await;
    let wanted = ObjectId([5; 32]);
    let r = get(&format!("{}/o/{}", t.url, wanted.to_hex()), None).await;
    assert_eq!(r.status(), 502);
    assert!(!t.disk.has_many(&[wanted, liar.id()]).await.unwrap().iter().any(|p| *p));
}

/// The disk budget holds, and evicted objects are simply fetched again.
#[tokio::test]
async fn disk_stays_within_budget() {
    let o = open_origin().await;
    let objs: Vec<EncodedObject> = (0..60u32)
        .map(|i| {
            // Incompressible: the budget counts compressed bytes.
            let mut x = (u64::from(i) + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let body: Vec<u8> = (0..20_000)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    (x >> 24) as u8
                })
                .collect();
            EncodedObject::new(ObjectKind::Blob, &body)
        })
        .collect();
    o.backend.objects.put_many(&objs).await.unwrap();
    let budget = 200_000;
    let t = tier_with(&o.origin.http_url(), AuthMode::Open, budget).await.unwrap();
    for round in 0..2 {
        for obj in &objs {
            let r = get(&format!("{}/o/{}", t.url, obj.id().to_hex()), None).await;
            assert_eq!(r.status(), 200, "round {round}");
            assert_eq!(r.bytes().await.unwrap().as_ref(), obj.as_bytes().as_ref());
        }
    }
    t.serve.maintain().await;
    let stats = t.disk.stats().await.unwrap();
    assert!(stats.bytes <= budget, "{} > {budget}", stats.bytes);
    assert!(stats.objects > 0 && (stats.objects as usize) < objs.len());
}

#[tokio::test]
async fn metrics_report_tiers() {
    let o = open_origin().await;
    let blob = EncodedObject::new(ObjectKind::Blob, b"metered");
    o.backend.objects.put(blob.clone()).await.unwrap();
    let t = tier(&o.origin.http_url(), AuthMode::Open).await;
    let url = format!("{}/o/{}", t.url, blob.id().to_hex());
    get(&url, None).await.bytes().await.unwrap();
    for _ in 0..3 {
        get(&url, None).await.bytes().await.unwrap();
    }
    let text = get(&format!("{}/metrics", t.url), None).await.text().await.unwrap();
    let value = |needle: &str| -> u64 {
        text.lines()
            .find(|l| l.starts_with(needle))
            .unwrap_or_else(|| panic!("{needle} in\n{text}"))
            .rsplit(' ')
            .next()
            .unwrap()
            .parse()
            .unwrap()
    };
    assert_eq!(value(r#"ant_cacheserve_requests_total{route="objects",result="hit"}"#), 3);
    assert_eq!(value(r#"ant_cacheserve_requests_total{route="objects",result="miss"}"#), 1);
    assert_eq!(
        value(
            r#"ant_cacheserve_upstream_requests_total{route="objects",kind="fetch",status="200"}"#
        ),
        1
    );
    assert!(value(r#"ant_cacheserve_response_bytes_total{route="objects"}"#) > 0);
    assert!(text.contains("ant_cacheserve_disk_budget_bytes"));
    assert!(text.trim_end().ends_with("# EOF"));
}

// ---------------------------------------------------------------------------
// clients
// ---------------------------------------------------------------------------

async fn build_files(objects: &dyn ObjectStore, n: usize) -> ObjectId {
    #[derive(Default)]
    struct Dir {
        entries: Vec<TreeEntry>,
        dirs: BTreeMap<String, Dir>,
    }
    fn store(dir: Dir, out: &mut Vec<EncodedObject>) -> ObjectId {
        let mut entries = dir.entries;
        for (name, sub) in dir.dirs {
            entries.push(TreeEntry { mode: EntryMode::Tree, name, id: store(sub, out) });
        }
        let obj = EncodedObject::new(ObjectKind::Tree, &Tree::new(entries).unwrap().encode());
        let id = obj.id();
        out.push(obj);
        id
    }
    let mut root = Dir::default();
    let mut objs = Vec::new();
    for i in 0..n {
        let d = root.dirs.entry(format!("d{}", i % 13)).or_default();
        let d = d.dirs.entry(format!("s{}", i % 7)).or_default();
        let blob = EncodedObject::new(
            ObjectKind::Blob,
            format!("file {i}\n").repeat(i % 30 + 1).as_bytes(),
        );
        d.entries.push(TreeEntry {
            mode: EntryMode::File,
            name: format!("f{i}.txt"),
            id: blob.id(),
        });
        objs.push(blob);
    }
    let id = store(root, &mut objs);
    objects.put_many(&objs).await.unwrap();
    id
}

async fn checkout(grpc: &str, http: &str, tree: ObjectId, dir: &std::path::Path) {
    let remote = RemoteObjectStore::connect(grpc, Some(http)).await.unwrap();
    let cache = tempfile::tempdir().unwrap();
    let disk = Arc::new(DiskObjectStore::open(cache.path(), u64::MAX / 2).unwrap());
    let rt = ReadThrough::over_disk(disk, Arc::new(remote.clone()))
        .verified_upstream()
        .with_tree_bundles(remote.clone());
    Workspace::materialize_with(&rt, Some(&rt), dir, ObjectId([0xc0; 32]), tree, Sparse::all())
        .await
        .unwrap();
}

fn tree_listing(dir: &std::path::Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(root: &std::path::Path, dir: &std::path::Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let e = e.unwrap();
            let name = e.file_name().to_string_lossy().into_owned();
            if name == ".ant" {
                continue;
            }
            if e.file_type().unwrap().is_dir() {
                walk(root, &e.path(), out);
            } else {
                let rel = e.path().strip_prefix(root).unwrap().to_string_lossy().into_owned();
                out.insert(rel, std::fs::read(e.path()).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

/// Two cold clients check out through one cache: equal to a direct checkout,
/// and the second one costs the origin nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkouts_through_the_cache_equal_direct_and_warm_ones_skip_the_origin() {
    let o = open_origin().await;
    let tree = build_files(o.backend.objects.as_ref(), 600).await;
    let t = tier(&o.origin.http_url(), AuthMode::Open).await;

    let direct = tempfile::tempdir().unwrap();
    checkout(&o.origin.grpc_url(), &o.origin.http_url(), tree, direct.path()).await;
    let expected = tree_listing(direct.path());
    assert_eq!(expected.len(), 600);

    let first = tempfile::tempdir().unwrap();
    checkout(&o.origin.grpc_url(), &t.url, tree, first.path()).await;
    assert_eq!(tree_listing(first.path()), expected);

    let after_first = origin_requests(&o.metrics);
    let second = tempfile::tempdir().unwrap();
    checkout(&o.origin.grpc_url(), &t.url, tree, second.path()).await;
    assert_eq!(tree_listing(second.path()), expected);
    assert_eq!(origin_requests(&o.metrics), after_first, "warm cache: no origin requests");
}
