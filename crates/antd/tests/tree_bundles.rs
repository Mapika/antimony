//! Checkouts over HTTP tree bundles (`GET /t/{id}`) against a real
//! `ant-server`: byte-for-byte equal to gRPC checkouts, with fallback to
//! per-object reads for older servers and unusable bundles.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, Tree, TreeEntry};
use ant_server::acl::Acl;
use ant_server::serve::spawn_local;
use ant_store::{Backend, ObjectStore, ObjectStoreExt};
use antd::disk::DiskObjectStore;
use antd::readthrough::ReadThrough;
use antd::remote::{HttpRequests, RemoteObjectStore};
use antd::workspace::{Sparse, Workspace};
use axum::Router;
use axum::extract::{Path as UrlPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};

const COMMIT: ObjectId = ObjectId([0xc0; 32]);
/// Over the server's 1 MiB bundle limit: always fetched with `GET /o/`.
const HUGE: usize = 1536 * 1024;

/// Store a tree of `(path, mode, content)` leaves (gitlinks: content is the
/// hex id) and return its id.
async fn build(objects: &dyn ObjectStore, leaves: &[(String, EntryMode, Vec<u8>)]) -> ObjectId {
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
    for (path, mode, content) in leaves {
        let mut parts: Vec<&str> = path.split('/').collect();
        let name = parts.pop().unwrap().to_string();
        let mut d = &mut root;
        for p in parts {
            d = d.dirs.entry(p.to_string()).or_default();
        }
        let id = if *mode == EntryMode::Gitlink {
            ObjectId::from_str(std::str::from_utf8(content).unwrap()).unwrap()
        } else {
            let blob = EncodedObject::new(ObjectKind::Blob, content);
            let id = blob.id();
            objs.push(blob);
            id
        };
        d.entries.push(TreeEntry { mode: *mode, name, id });
    }
    let id = store(root, &mut objs);
    objects.put_many(&objs).await.unwrap();
    id
}

/// A few hundred files in nested directories, plus an executable, a
/// symlink, a gitlink, a blob too large to bundle and a blob at two paths.
fn leaves() -> Vec<(String, EntryMode, Vec<u8>)> {
    let mut v: Vec<(String, EntryMode, Vec<u8>)> = (0..300)
        .map(|i| {
            let path = format!("d{}/s{}/f{i}.txt", i % 17, i % 5);
            (path, EntryMode::File, format!("file {i}\n").repeat(i % 40 + 1).into_bytes())
        })
        .collect();
    v.push(("bin/tool".into(), EntryMode::Executable, b"#!/bin/sh\necho hi\n".to_vec()));
    v.push(("link".into(), EntryMode::Symlink, b"d0/s0/f0.txt".to_vec()));
    v.push(("module".into(), EntryMode::Gitlink, ObjectId([0x5a; 32]).to_hex().into_bytes()));
    v.push(("big/huge.bin".into(), EntryMode::File, vec![7u8; HUGE]));
    v.push(("dup/a".into(), EntryMode::File, b"same\n".to_vec()));
    v.push(("dup2/deeper/a".into(), EntryMode::File, b"same\n".to_vec()));
    v
}

struct Fixture {
    backend: Backend,
    grpc_url: String,
    http_url: String,
    tree: ObjectId,
}

async fn fixture() -> Fixture {
    let backend = Backend::in_memory();
    let tree = build(backend.objects.as_ref(), &leaves()).await;
    let server = spawn_local(backend.clone(), Acl::open()).await.unwrap();
    Fixture { backend, grpc_url: server.grpc_url(), http_url: server.http_url(), tree }
}

/// Materialize `tree` into `dir` with a fresh cache through `remote`.
async fn checkout(remote: &RemoteObjectStore, tree: ObjectId, dir: &Path, sparse: Sparse) {
    let cache = tempfile::tempdir().unwrap();
    let disk = Arc::new(DiskObjectStore::open(cache.path(), u64::MAX / 2).unwrap());
    let rt = ReadThrough::over_disk(disk, Arc::new(remote.clone()))
        .verified_upstream()
        .with_tree_bundles(remote.clone());
    Workspace::materialize_with(&rt, Some(&rt), dir, COMMIT, tree, sparse).await.unwrap();
}

/// Everything under `dir`: kind, permission bits and content (link target
/// for symlinks), plus the index without its stat columns.
/// Path → (kind, permission bits, content or link target).
type Files = BTreeMap<String, (char, u32, Vec<u8>)>;

fn snapshot(dir: &Path) -> (Files, Vec<String>) {
    fn walk(root: &Path, dir: &Path, out: &mut Files) {
        for e in std::fs::read_dir(dir).unwrap() {
            let e = e.unwrap();
            let path = e.path();
            let rel = path.strip_prefix(root).unwrap().to_str().unwrap().to_string();
            if rel == ".ant" {
                continue;
            }
            let meta = std::fs::symlink_metadata(&path).unwrap();
            let mode = meta.permissions().mode() & 0o777;
            if meta.file_type().is_symlink() {
                let target = std::fs::read_link(&path).unwrap();
                out.insert(rel, ('l', 0, target.to_str().unwrap().as_bytes().to_vec()));
            } else if meta.is_dir() {
                out.insert(rel, ('d', 0, Vec::new()));
                walk(root, &path, out);
            } else {
                out.insert(rel, ('f', mode, std::fs::read(&path).unwrap()));
            }
        }
    }
    let mut files = BTreeMap::new();
    walk(dir, dir, &mut files);
    let index = std::fs::read_to_string(dir.join(".ant/index")).unwrap();
    let index =
        index.lines().map(|l| l.split('\t').take(3).collect::<Vec<_>>().join("\t")).collect();
    (files, index)
}

fn delta(after: HttpRequests, before: HttpRequests) -> HttpRequests {
    HttpRequests {
        objects: after.objects - before.objects,
        bundles: after.bundles - before.bundles,
    }
}

#[tokio::test]
async fn http_bundle_checkout_equals_grpc_checkout() {
    let f = fixture().await;
    let grpc = RemoteObjectStore::connect(&f.grpc_url, None).await.unwrap();
    let http = RemoteObjectStore::connect(&f.grpc_url, Some(&f.http_url)).await.unwrap();
    for sparse in [
        Sparse::all(),
        Sparse::new(vec!["d3".into(), "bin/tool".into()]).unwrap(),
        Sparse::new(vec!["d1/s1".into(), "**/a".into()]).unwrap(),
    ] {
        let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        checkout(&grpc, f.tree, a.path(), sparse.clone()).await;
        let before = http.http_requests();
        checkout(&http, f.tree, b.path(), sparse.clone()).await;
        let used = delta(http.http_requests(), before);
        let (want, got) = (snapshot(a.path()), snapshot(b.path()));
        assert!(!want.0.is_empty());
        assert_eq!(want, got, "sparse {:?}", sparse.patterns);
        assert_eq!(grpc.http_requests(), HttpRequests::default());
        if sparse.patterns.is_empty() {
            // The whole (small) tree fits one bundle; only the huge blob
            // is fetched by id.
            assert_eq!(used, HttpRequests { objects: 1, bundles: 1 }, "full checkout");
        } else {
            assert!(used.bundles >= 1, "{used:?} for {:?}", sparse.patterns);
        }
    }
}

/// An HTTP front end of an older server: `GET /o/{id}` only. `/t/` answers
/// with a plain 404, or 405 when `t_status` says so.
async fn old_server(backend: Backend, t_status: StatusCode) -> String {
    async fn object(State(b): State<Backend>, UrlPath(hex): UrlPath<String>) -> Response {
        let Ok(id) = ObjectId::from_str(&hex) else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        match b.objects.get(&id).await.unwrap() {
            Some(o) => o.into_bytes().into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }
    let mut app = Router::new().route("/o/{hex}", get(object));
    if t_status == StatusCode::METHOD_NOT_ALLOWED {
        app = app.route("/t/{hex}", post(|| async { "" }));
    }
    serve(app.with_state(backend)).await
}

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

#[tokio::test]
async fn older_servers_fall_back_to_per_object_reads() {
    let f = fixture().await;
    let grpc = RemoteObjectStore::connect(&f.grpc_url, None).await.unwrap();
    let want = tempfile::tempdir().unwrap();
    checkout(&grpc, f.tree, want.path(), Sparse::all()).await;
    let want = snapshot(want.path());
    for status in [StatusCode::NOT_FOUND, StatusCode::METHOD_NOT_ALLOWED] {
        let base = old_server(f.backend.clone(), status).await;
        let remote = RemoteObjectStore::connect(&f.grpc_url, Some(&base)).await.unwrap();
        assert!(remote.tree_bundles_enabled());
        let dir = tempfile::tempdir().unwrap();
        checkout(&remote, f.tree, dir.path(), Sparse::all()).await;
        assert_eq!(snapshot(dir.path()), want, "{status}");
        // One probe, then bundles stay off for this store and its clones.
        let used = remote.http_requests();
        assert_eq!(used.bundles, 1, "{status}");
        assert!(used.objects > 300, "{used:?}");
        assert!(!remote.clone().tree_bundles_enabled());
        let dir = tempfile::tempdir().unwrap();
        checkout(&remote, f.tree, dir.path(), Sparse::all()).await;
        assert_eq!(remote.http_requests().bundles, 1);
    }
}

#[tokio::test]
async fn unusable_bundles_are_discarded() {
    let f = fixture().await;
    let grpc = RemoteObjectStore::connect(&f.grpc_url, None).await.unwrap();
    let want = tempfile::tempdir().unwrap();
    checkout(&grpc, f.tree, want.path(), Sparse::all()).await;
    let want = snapshot(want.path());

    // `/t/` answers 200 with bodies that must not be trusted: a bundle whose
    // first frame is the requested id with someone else's bytes, a bundle
    // starting with another object, and a body that is no bundle at all.
    // `/o/` is served from the real store.
    #[derive(Clone)]
    struct Bad {
        backend: Backend,
        kind: u8,
    }
    async fn object(State(b): State<Bad>, UrlPath(hex): UrlPath<String>) -> Response {
        let id = ObjectId::from_str(&hex).unwrap();
        match b.backend.objects.get(&id).await.unwrap() {
            Some(o) => o.into_bytes().into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }
    async fn bundle(State(b): State<Bad>, UrlPath(hex): UrlPath<String>) -> Response {
        let id = ObjectId::from_str(&hex).unwrap();
        let other = EncodedObject::new(ObjectKind::Blob, b"forged\n");
        let body = match b.kind {
            0 => ant_proto::bundle::encode([(&id, other.as_bytes().as_ref())]).unwrap(),
            1 => ant_proto::bundle::encode([(&other.id(), other.as_bytes().as_ref())]).unwrap(),
            _ => b"garbage".to_vec(),
        };
        body.into_response()
    }
    for kind in 0..3u8 {
        let app = Router::new()
            .route("/o/{hex}", get(object))
            .route("/t/{hex}", get(bundle))
            .with_state(Bad { backend: f.backend.clone(), kind });
        let base = serve(app).await;
        let remote = RemoteObjectStore::connect(&f.grpc_url, Some(&base)).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        checkout(&remote, f.tree, dir.path(), Sparse::all()).await;
        assert_eq!(snapshot(dir.path()), want, "kind {kind}");
        // Still on: a bad bundle is not an old server.
        assert!(remote.tree_bundles_enabled());
        assert!(remote.http_requests().bundles > 1, "kind {kind}");
    }
}

/// Local store that records the ids of each `put_many`, in order.
#[derive(Default)]
struct RecordingStore {
    inner: ant_store::memory::MemoryObjectStore,
    puts: std::sync::Mutex<Vec<Vec<ObjectId>>>,
}

#[async_trait::async_trait]
impl ObjectStore for RecordingStore {
    async fn get_many(&self, ids: &[ObjectId]) -> ant_store::Result<Vec<Option<EncodedObject>>> {
        self.inner.get_many(ids).await
    }
    async fn put_many(&self, objects: &[EncodedObject]) -> ant_store::Result<()> {
        self.puts.lock().unwrap().push(objects.iter().map(EncodedObject::id).collect());
        self.inner.put_many(objects).await
    }
    async fn has_many(&self, ids: &[ObjectId]) -> ant_store::Result<Vec<bool>> {
        self.inner.has_many(ids).await
    }
}

/// The tree a bundle was fetched for is stored after everything else the
/// bundle delivered: another process that finds it in the shared cache
/// walks on below it at once (without waiting for the fetcher), so the
/// subtrees and blobs must already be there, or it fetches them again.
#[tokio::test]
async fn a_bundles_own_tree_is_stored_last() {
    let f = fixture().await;
    let remote = RemoteObjectStore::connect(&f.grpc_url, Some(&f.http_url)).await.unwrap();
    let local = Arc::new(RecordingStore::default());
    let rt = ReadThrough::new(local.clone(), Arc::new(remote.clone()))
        .verified_upstream()
        .with_tree_bundles(remote);
    let got = rt.get_trees_bundled(&[f.tree]).await.unwrap();
    assert!(got[0].is_some());
    let puts = local.puts.lock().unwrap().clone();
    let (last, earlier) = puts.split_last().expect("bundle stored");
    assert_eq!(last, &vec![f.tree], "the bundle's tree comes last, alone");
    let below: usize = earlier.iter().map(Vec::len).sum();
    assert!(below > 100, "subtrees and blobs stored before it: {below}");
    assert!(earlier.iter().flatten().all(|id| *id != f.tree));
}
