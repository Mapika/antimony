mod support;
use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, TreeEntry};
use ant_store::server_cache::{DiskCache, ServerCache};
use ant_store::{
    Backend, ObjectStoreExt,
    graph::{build_tree, write_commit, write_tree},
};
use std::{collections::HashSet, path::Path, time::Duration};
use support::{commit_of, git_command, pack_bytes, packed_objects};
use tempfile::TempDir;

async fn git_result(dir: &Path, args: &[&str]) -> std::process::Output {
    tokio::time::timeout(
        Duration::from_secs(30),
        git_command(dir)
            .args(["-c", "protocol.version=2", "-c", "fetch.unpackLimit=1"])
            .args(args)
            .output(),
    )
    .await
    .expect("git timed out")
    .unwrap()
}

async fn git(dir: &Path, args: &[&str]) -> String {
    let result = git_result(dir, args).await;
    assert!(result.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&result.stderr));
    String::from_utf8(result.stdout).unwrap().trim().to_string()
}

/// Serve `b`'s git export on an ephemeral port: the clone URL and the server
/// task (abort it to stop).
async fn serve(b: Backend) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/repo.git", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, ant_gitserve::router(b)).await.unwrap();
    });
    (url, task)
}

async fn commit(b: &Backend, parents: Vec<ObjectId>, version: u8) -> ObjectId {
    let nested = build_tree(b.objects.as_ref(), &[("deep/file", b"nested\n")]).await.unwrap();
    let mut entries = vec![TreeEntry { mode: EntryMode::Tree, name: "dir".into(), id: nested }];
    for (name, mode, content) in [
        ("readme", EntryMode::File, vec![version; 10]),
        ("run", EntryMode::Executable, b"#!/bin/sh\necho hello\n".to_vec()),
        ("link", EntryMode::Symlink, b"dir/deep/file".to_vec()),
    ] {
        let id = b.objects.put(EncodedObject::new(ObjectKind::Blob, &content)).await.unwrap();
        entries.push(TreeEntry { mode, name: name.into(), id });
    }
    let tree = write_tree(b.objects.as_ref(), entries).await.unwrap();
    let commit = commit_of(
        tree,
        parents,
        ("Test", "test@example.com"),
        i64::from(version),
        format!("version {version}\n"),
    );
    write_commit(b, &commit).await.unwrap()
}

struct Fixture {
    b: Backend,
    ids: Vec<ObjectId>,
    url: String,
    task: tokio::task::JoinHandle<()>,
    temp: TempDir,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Fixture {
    async fn new() -> Self {
        let b = Backend::in_memory();
        let mut ids = Vec::new();
        for v in 1..=3 {
            ids.push(commit(&b, ids.last().copied().into_iter().collect(), v).await);
        }
        b.refs.cas_ref("main", None, Some(ids[2])).await.unwrap();
        b.refs.cas_ref("topic", None, Some(ids[1])).await.unwrap();
        let (url, task) = serve(b.clone()).await;
        Self { b, ids, url, task, temp: tempfile::tempdir().unwrap() }
    }

    async fn clone_into(&self, name: &str, options: &[&str]) -> std::path::PathBuf {
        let mut args = vec!["clone"];
        args.extend_from_slice(options);
        args.extend([&self.url, name]);
        git(self.temp.path(), &args).await;
        self.temp.path().join(name)
    }
}

fn assert_files(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(std::fs::read(dir.join("readme")).unwrap(), vec![3; 10]);
    assert_eq!(std::fs::read(dir.join("dir/deep/file")).unwrap(), b"nested\n");
    assert_eq!(std::fs::read(dir.join("run")).unwrap(), b"#!/bin/sh\necho hello\n");
    assert_ne!(std::fs::metadata(dir.join("run")).unwrap().permissions().mode() & 0o111, 0);
    assert_eq!(std::fs::read_link(dir.join("link")).unwrap(), Path::new("dir/deep/file"));
}

#[tokio::test]
async fn full_clone_and_push_rejected() {
    let f = Fixture::new().await;
    let dir = f.clone_into("full", &[]).await;
    assert_files(&dir);
    assert_eq!(
        git(&dir, &["log", "--format=%H"]).await,
        f.ids.iter().rev().map(ToString::to_string).collect::<Vec<_>>().join("\n")
    );
    assert_eq!(git(&dir, &["rev-parse", "origin/topic"]).await, f.ids[1].to_string());
    assert_eq!(
        git(&dir, &["symbolic-ref", "refs/remotes/origin/HEAD"]).await,
        "refs/remotes/origin/main"
    );
    git(&dir, &["fsck", "--full"]).await;
    let push = git_result(&dir, &["push", "origin", "HEAD:forbidden"]).await;
    assert!(!push.status.success());
    assert!(String::from_utf8_lossy(&push.stderr).contains("403"));
    assert_eq!(f.b.refs.list_refs("").await.unwrap().len(), 2);
    let result =
        reqwest::Client::new().post(format!("{}/git-receive-pack", f.url)).send().await.unwrap();
    assert_eq!(result.status(), 403);
}

#[tokio::test]
async fn partial_clone_fetches_blobs_on_checkout() {
    let f = Fixture::new().await;
    let dir = f.clone_into("partial", &["--filter=blob:none", "--no-checkout"]).await;
    let objects =
        git(&dir, &["cat-file", "--batch-all-objects", "--batch-check=%(objecttype)"]).await;
    assert!(!objects.lines().any(|line| line == "blob"));
    assert!(objects.lines().any(|line| line == "commit"));
    git(&dir, &["checkout", "main"]).await;
    assert_files(&dir);
    let objects =
        git(&dir, &["cat-file", "--batch-all-objects", "--batch-check=%(objecttype)"]).await;
    assert_eq!(objects.lines().filter(|line| *line == "blob").count(), 4);
    let checked_out = f.clone_into("partial-checkout", &["--filter=blob:none"]).await;
    assert_files(&checked_out);
}

/// A pack that fails while it is built (here: a want the store does not
/// have) reaches git as a remote error, not as a truncated pack.
#[tokio::test]
async fn failed_pack_is_reported_to_git() {
    let f = Fixture::new().await;
    let dir = f.clone_into("clone", &[]).await;
    let missing = EncodedObject::new(ObjectKind::Blob, b"never stored").id().to_string();
    let result = git_result(&dir, &["fetch", "origin", &missing]).await;
    assert!(!result.status.success());
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains(&format!("unknown want {missing}")), "{stderr}");
}

#[tokio::test]
async fn shallow_clone() {
    let f = Fixture::new().await;
    let dir = f.clone_into("shallow", &["--depth", "1"]).await;
    assert_files(&dir);
    assert_eq!(git(&dir, &["rev-list", "--count", "HEAD"]).await, "1");
    assert_eq!(git(&dir, &["rev-parse", "--is-shallow-repository"]).await, "true");
}

async fn objects(dir: &Path) -> HashSet<String> {
    git(dir, &["cat-file", "--batch-all-objects", "--batch-check=%(objectname)"])
        .await
        .lines()
        .map(str::to_owned)
        .collect()
}

#[tokio::test]
async fn incremental_fetch_sends_exactly_new_objects() {
    let f = Fixture::new().await;
    let dir = f.clone_into("incremental", &[]).await;
    let old = objects(&dir).await;
    let next = commit(&f.b, vec![f.ids[2]], 4).await;
    f.b.refs.cas_ref("main", Some(f.ids[2]), Some(next)).await.unwrap();
    git(&dir, &["fetch", "origin"]).await;
    assert_eq!(git(&dir, &["rev-parse", "origin/main"]).await, next.to_string());
    let new = objects(&dir).await;
    assert_eq!(new.difference(&old).count(), 3); // commit, root tree, changed blob
    // Inspect every pack, including duplicates: counting the object database
    // alone would miss the server resending old objects in a second pack.
    let packed = packed_objects(&dir.join(".git/objects/pack")).await;
    assert_eq!(packed.len(), new.len(), "incremental pack repeated objects");
    git(&dir, &["fsck", "--full"]).await;
}

/// git gzips large upload-pack requests; a lazy checkout of a partial clone
/// with many missing blobs must still work.
#[tokio::test]
async fn partial_clone_lazy_fetch_of_many_blobs() {
    let b = Backend::in_memory();
    let files: Vec<(String, Vec<u8>)> = (0..300)
        .map(|i| (format!("d{}/f{i:03}.txt", i % 7), format!("content {i}\n").into_bytes()))
        .collect();
    let refs: Vec<(&str, &[u8])> = files.iter().map(|(p, c)| (p.as_str(), c.as_slice())).collect();
    let tree = build_tree(b.objects.as_ref(), &refs).await.unwrap();
    let c =
        write_commit(&b, &commit_of(tree, vec![], ("t", "t@t"), 1, "many\n".into())).await.unwrap();
    b.refs.cas_ref("main", None, Some(c)).await.unwrap();

    let (url, _task) = serve(b).await;
    let tmp = tempfile::tempdir().unwrap();
    git(tmp.path(), &["clone", "-q", "--filter=blob:none", &url, "p"]).await;
    let dir = tmp.path().join("p");
    for (path, content) in &files {
        assert_eq!(&std::fs::read(dir.join(path)).unwrap(), content, "{path}");
    }
}

/// Merge-heavy history over real HTTP: blobless clone with a lazy checkout,
/// fsck, and a pack well below the undeltified size of its trees and commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merged_history_blobless_clone_and_checkout() {
    let b = Backend::in_memory();
    let tip = support::merge_history(&b, 300).await;
    b.refs.cas_ref("main", None, Some(tip)).await.unwrap();
    let (url, task) = serve(b).await;
    let temp = tempfile::tempdir().unwrap();
    git(temp.path(), &["clone", "--filter=blob:none", "--no-checkout", &url, "merged"]).await;
    let dir = temp.path().join("merged");
    let packed = pack_bytes(&dir.join(".git/objects/pack"));
    git(&dir, &["checkout", "main"]).await;
    git(&dir, &["fsck", "--full"]).await;
    assert_eq!(git(&dir, &["rev-parse", "HEAD"]).await, tip.to_string());
    assert_eq!(git(&dir, &["status", "--porcelain"]).await, "");
    let merges = git(&dir, &["rev-list", "--merges", "--count", "HEAD"]).await;
    assert!(merges.parse::<usize>().unwrap() > 30, "{merges} merges");
    task.abort();
    // 300 commits plus topics; the single-base encoder needed ~70% more.
    eprintln!("merged history over HTTP: blobless pack {packed} bytes");
    assert!(packed < 320_000, "blobless pack {packed} bytes (measured 254k)");
}

/// Includes the in-memory store in RSS; reports the pre-clone resident set
/// separately so fixture storage is not confused with pack working memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_blobless_history_is_delta_compressed() {
    use std::{sync::atomic::Ordering, time::Instant};
    const REVISIONS: usize = 2000;
    let (b, undeltified) = support::history(REVISIONS, 2000).await;
    let (url, task) = serve(b).await;
    let temp = tempfile::tempdir().unwrap();
    let before = support::rss_kib();
    let (peak, monitor) = support::watch_peak_rss(before);
    let start = Instant::now();
    git(temp.path(), &["clone", "--filter=blob:none", "--no-checkout", &url, "large"]).await;
    let elapsed = start.elapsed();
    monitor.abort();
    let dir = temp.path().join("large");
    let packed = pack_bytes(&dir.join(".git/objects/pack"));
    eprintln!(
        "synthetic {REVISIONS} x 2000: undeltified={undeltified} pack={packed} clone={elapsed:?} RSS before={before} KiB peak={} KiB",
        peak.load(Ordering::Relaxed)
    );
    git(&dir, &["fsck", "--full"]).await;
    assert_eq!(git(&dir, &["rev-list", "--count", "HEAD"]).await, REVISIONS.to_string());
    task.abort();
    assert!(packed * 100 <= undeltified * 30, "pack {packed} exceeds 30% of {undeltified}");
}

#[tokio::test]
async fn delta_chains_path_type_changes_and_incremental_fetch() {
    let f = Fixture::new().await;
    let tip = support::revisions(&f.b, Some(f.ids[2]), 0..125).await;
    f.b.refs.cas_ref("main", Some(f.ids[2]), Some(tip)).await.unwrap();
    let dir = f.clone_into("chains", &["--no-checkout"]).await;
    git(&dir, &["fsck", "--full"]).await;
    let old = objects(&dir).await;
    let pack_dir = dir.join(".git/objects/pack");
    let packed = packed_objects(&pack_dir).await;
    let deltas = |kind: &str| packed.iter().filter(|(k, d)| k == kind && d.is_some()).count();
    let max_depth = packed.iter().filter_map(|(_, d)| *d).max().unwrap_or(0);
    assert!(deltas("tree") > 50 && deltas("blob") > 50);
    assert!((2..=50).contains(&max_depth));
    let next = support::revisions(&f.b, Some(tip), 125..135).await;
    f.b.refs.cas_ref("main", Some(tip), Some(next)).await.unwrap();
    git(&dir, &["fetch", "origin"]).await;
    git(&dir, &["fsck", "--full"]).await;
    let new = objects(&dir).await;
    assert_eq!(new.difference(&old).count(), 30);
    let packed_count = packed_objects(&pack_dir).await.len();
    assert_eq!(packed_count, new.len(), "incremental pack repeated known objects");
}

/// The bytes of the one pack of a bare clone.
fn single_pack(dir: &Path) -> Vec<u8> {
    let packs: Vec<_> = std::fs::read_dir(dir.join("objects/pack"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "pack"))
        .collect();
    assert_eq!(packs.len(), 1, "{packs:?}");
    std::fs::read(&packs[0]).unwrap()
}

/// Clones reuse the pack of the first clone (the cached base pack): clones
/// arriving while it is built wait for it and receive the same bytes; after
/// new commits a clone receives the base entries followed by exactly the new
/// objects; after a ref moves back, a clone is built from scratch again.
/// Every clone has exactly the objects of a clone from a fresh server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clones_reuse_the_cached_base_pack() {
    let f = Fixture::new().await;
    let tip = support::revisions(&f.b, Some(f.ids[2]), 0..20).await;
    f.b.refs.cas_ref("main", Some(f.ids[2]), Some(tip)).await.unwrap();
    for (n, filter) in [&[][..], &["--filter=blob:none"][..]].into_iter().enumerate() {
        let clone = |name: String| {
            let f = &f;
            async move {
                let mut options = vec!["--bare"];
                options.extend_from_slice(filter);
                f.clone_into(&name, &options).await
            }
        };
        // A fresh server for every comparison clone: nothing cached.
        let fresh = |name: String| {
            let b = f.b.clone();
            let temp = f.temp.path().to_path_buf();
            async move {
                let (url, task) = serve(b).await;
                let mut args = vec!["clone", "--bare"];
                args.extend_from_slice(filter);
                args.extend([url.as_str(), name.as_str()]);
                git(&temp, &args).await;
                task.abort();
                objects(&temp.join(&name)).await
            }
        };
        let concurrent =
            futures::future::join_all((0..4).map(|i| clone(format!("first-{n}-{i}")))).await;
        let first = single_pack(&concurrent[0]);
        for dir in &concurrent {
            assert!(single_pack(dir) == first, "concurrent clones received different packs");
            git(dir, &["fsck", "--full", "--strict"]).await;
        }
        assert_eq!(objects(&concurrent[0]).await, fresh(format!("fresh-{n}-a")).await);

        let next = support::revisions(&f.b, Some(tip), 20 + n * 5..25 + n * 5).await;
        f.b.refs.cas_ref("main", Some(tip), Some(next)).await.unwrap();
        let after = clone(format!("after-{n}")).await;
        let pack = single_pack(&after);
        assert!(pack[12..first.len() - 32] == first[12..first.len() - 32], "base not reused");
        assert!(pack.len() > first.len());
        git(&after, &["fsck", "--full", "--strict"]).await;
        let expected = fresh(format!("fresh-{n}-b")).await;
        assert_eq!(objects(&after).await, expected);
        assert_eq!(packed_objects(&after.join("objects/pack")).await.len(), expected.len());

        // Back to `tip`: the base (built for `tip`... or `next`) is refused
        // or reused, and the clone is exact either way.
        f.b.refs.cas_ref("main", Some(next), Some(tip)).await.unwrap();
        let back = clone(format!("back-{n}")).await;
        git(&back, &["fsck", "--full", "--strict"]).await;
        assert_eq!(objects(&back).await, fresh(format!("fresh-{n}-c")).await);
    }
}

/// Serve an export with a persistent server cache in `dir` (disk tier).
async fn serve_persisted(
    b: Backend,
    dir: &Path,
) -> (String, tokio::task::JoinHandle<()>, ant_gitserve::Export, std::sync::Arc<ServerCache>) {
    let disk = DiskCache::open(dir, "test", 1 << 30).unwrap();
    let cache = std::sync::Arc::new(ServerCache::new(Some(disk), None).unwrap());
    let export = ant_gitserve::Export::new(b, Some(cache.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/repo.git", listener.local_addr().unwrap());
    let router = export.router();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (url, task, export, cache)
}

/// Wait (bounded) until the base of a filter is persisted in `cache`.
async fn persisted(cache: &ServerCache, name: &str) -> String {
    for _ in 0..3000 {
        if let Some(key) = cache.pointer(name).await {
            return key;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("base {name} was not persisted");
}

/// A copy of a cache directory, as a restarted server finds it.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// A base pack written to the server cache is loaded by the next server
/// (a restart: the same directory) before its first clone, which then
/// sends the same pack without building it; a damaged entry is dropped and
/// the clone built from scratch. `prewarm` loads (or builds) the base at
/// start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persisted_base_pack_survives_a_restart() {
    let f = Fixture::new().await;
    let tip = support::revisions(&f.b, Some(f.ids[2]), 0..20).await;
    f.b.refs.cas_ref("main", Some(f.ids[2]), Some(tip)).await.unwrap();
    let dirs = tempfile::tempdir().unwrap();
    let clone = |url: String, name: &str| {
        let dest = f.temp.path().join(name);
        let temp = f.temp.path().to_path_buf();
        async move {
            let args = ["clone", "--bare", "--filter=blob:none", &url, dest.to_str().unwrap()];
            git(&temp, &args).await;
            git(&dest, &["fsck", "--full", "--strict"]).await;
            single_pack(&dest)
        }
    };

    let first_dir = dirs.path().join("first");
    let (url, task, _, cache) = serve_persisted(f.b.clone(), &first_dir).await;
    let first = clone(url, "first").await;
    let key = persisted(&cache, "git-blobless").await;
    assert!(key.starts_with("blobless-f1-"), "{key}");
    task.abort();

    // Restarted: the base is read from disk, not built.
    let restarted = dirs.path().join("restarted");
    copy_dir(&first_dir, &restarted);
    let (url, task, _, cache) = serve_persisted(f.b.clone(), &restarted).await;
    let again = clone(url, "again").await;
    assert!(again == first, "the persisted base is sent as is");
    assert_eq!(cache.stats().disk_hits, 1);
    task.abort();

    // Damaged on disk: dropped, and the clone is built (and exact) anyway.
    let damaged = dirs.path().join("damaged");
    copy_dir(&first_dir, &damaged);
    let entry = damaged.join("v1/test/git").join(&key[..2]).join(&key);
    let mut bytes = std::fs::read(&entry).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x55;
    std::fs::write(&entry, bytes).unwrap();
    let (url, task, _, cache) = serve_persisted(f.b.clone(), &damaged).await;
    let rebuilt = clone(url, "rebuilt").await;
    assert_eq!(cache.stats().disk_hits, 0);
    assert_eq!(cache.stats().corrupt, 1);
    assert_eq!(
        objects(&f.temp.path().join("rebuilt")).await,
        objects(&f.temp.path().join("first")).await
    );
    assert_eq!(rebuilt.len(), first.len());
    task.abort();

    // Prewarm: loads the persisted base, or builds one when there is none.
    let warm = dirs.path().join("warm");
    copy_dir(&first_dir, &warm);
    let (_, task, export, cache) = serve_persisted(f.b.clone(), &warm).await;
    assert!(export.prewarm(true).await.unwrap());
    assert_eq!(cache.stats().disk_hits, 1);
    task.abort();
    let (url, task, export, cache) = serve_persisted(f.b.clone(), &dirs.path().join("cold")).await;
    assert!(export.prewarm(true).await.unwrap());
    assert_eq!(persisted(&cache, "git-blobless").await, key);
    // (Its pack lists the wants in another order than a clone does: same
    // objects, other layout.)
    clone(url, "prewarmed").await;
    assert_eq!(
        objects(&f.temp.path().join("prewarmed")).await,
        objects(&f.temp.path().join("first")).await
    );
    assert_eq!(cache.stats().disk_hits, 0, "the prewarmed base was used, not loaded");
    task.abort();
}
