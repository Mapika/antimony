mod support;
use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, TreeEntry};
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
