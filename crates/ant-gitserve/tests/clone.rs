use ant_core::{Commit, EncodedObject, EntryMode, ObjectId, ObjectKind, Signature, TreeEntry};
use ant_store::{
    Backend, ObjectStoreExt,
    graph::{build_tree, write_commit, write_tree},
};
use std::{collections::HashSet, path::Path, time::Duration};
use tempfile::TempDir;

async fn git_result(dir: &Path, args: &[&str]) -> std::process::Output {
    tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new("git")
            .current_dir(dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_TERMINAL_PROMPT", "0")
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
    assert!(
        result.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).unwrap().trim().to_string()
}

async fn commit(b: &Backend, parents: Vec<ObjectId>, version: u8) -> ObjectId {
    let nested = build_tree(b.objects.as_ref(), &[("deep/file", b"nested\n")])
        .await
        .unwrap();
    let mut entries = vec![TreeEntry {
        mode: EntryMode::Tree,
        name: "dir".into(),
        id: nested,
    }];
    for (name, mode, content) in [
        ("readme", EntryMode::File, vec![version; 10]),
        (
            "run",
            EntryMode::Executable,
            b"#!/bin/sh\necho hello\n".to_vec(),
        ),
        ("link", EntryMode::Symlink, b"dir/deep/file".to_vec()),
    ] {
        let id = b
            .objects
            .put(EncodedObject::new(ObjectKind::Blob, &content))
            .await
            .unwrap();
        entries.push(TreeEntry {
            mode,
            name: name.into(),
            id,
        });
    }
    let tree = write_tree(b.objects.as_ref(), entries).await.unwrap();
    let sig = Signature {
        name: "Test".into(),
        email: "test@example.com".into(),
        time: i64::from(version),
        tz_offset_minutes: 0,
    };
    write_commit(
        b,
        &Commit {
            tree,
            parents,
            author: sig.clone(),
            committer: sig,
            extra_headers: vec![],
            message: format!("version {version}\n"),
        },
    )
    .await
    .unwrap()
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/repo.git", listener.local_addr().unwrap());
        let app = ant_gitserve::router(b.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            b,
            ids,
            url,
            task,
            temp: tempfile::tempdir().unwrap(),
        }
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
    assert_eq!(
        std::fs::read(dir.join("dir/deep/file")).unwrap(),
        b"nested\n"
    );
    assert_eq!(
        std::fs::read(dir.join("run")).unwrap(),
        b"#!/bin/sh\necho hello\n"
    );
    assert_ne!(
        std::fs::metadata(dir.join("run"))
            .unwrap()
            .permissions()
            .mode()
            & 0o111,
        0
    );
    assert_eq!(
        std::fs::read_link(dir.join("link")).unwrap(),
        Path::new("dir/deep/file")
    );
}

#[tokio::test]
async fn full_clone_and_push_rejected() {
    let f = Fixture::new().await;
    let dir = f.clone_into("full", &[]).await;
    assert_files(&dir);
    assert_eq!(
        git(&dir, &["log", "--format=%H"]).await,
        f.ids
            .iter()
            .rev()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert_eq!(
        git(&dir, &["rev-parse", "origin/topic"]).await,
        f.ids[1].to_string()
    );
    assert_eq!(
        git(&dir, &["symbolic-ref", "refs/remotes/origin/HEAD"]).await,
        "refs/remotes/origin/main"
    );
    git(&dir, &["fsck", "--full"]).await;
    let push = git_result(&dir, &["push", "origin", "HEAD:forbidden"]).await;
    assert!(!push.status.success());
    assert!(String::from_utf8_lossy(&push.stderr).contains("403"));
    assert_eq!(f.b.refs.list_refs("").await.unwrap().len(), 2);
    let result = reqwest::Client::new()
        .post(format!("{}/git-receive-pack", f.url))
        .send()
        .await
        .unwrap();
    assert_eq!(result.status(), 403);
}

#[tokio::test]
async fn partial_clone_fetches_blobs_on_checkout() {
    let f = Fixture::new().await;
    let dir = f
        .clone_into("partial", &["--filter=blob:none", "--no-checkout"])
        .await;
    let objects = git(
        &dir,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objecttype)",
        ],
    )
    .await;
    assert!(!objects.lines().any(|line| line == "blob"));
    assert!(objects.lines().any(|line| line == "commit"));
    git(&dir, &["checkout", "main"]).await;
    assert_files(&dir);
    let objects = git(
        &dir,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objecttype)",
        ],
    )
    .await;
    assert_eq!(objects.lines().filter(|line| *line == "blob").count(), 4);
    let checked_out = f
        .clone_into("partial-checkout", &["--filter=blob:none"])
        .await;
    assert_files(&checked_out);
}

#[tokio::test]
async fn shallow_clone() {
    let f = Fixture::new().await;
    let dir = f.clone_into("shallow", &["--depth", "1"]).await;
    assert_files(&dir);
    assert_eq!(git(&dir, &["rev-list", "--count", "HEAD"]).await, "1");
    assert_eq!(
        git(&dir, &["rev-parse", "--is-shallow-repository"]).await,
        "true"
    );
}

async fn objects(dir: &Path) -> HashSet<String> {
    git(
        dir,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname)",
        ],
    )
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
    f.b.refs
        .cas_ref("main", Some(f.ids[2]), Some(next))
        .await
        .unwrap();
    git(&dir, &["fetch", "origin"]).await;
    assert_eq!(
        git(&dir, &["rev-parse", "origin/main"]).await,
        next.to_string()
    );
    let new = objects(&dir).await;
    assert_eq!(new.difference(&old).count(), 3); // commit, root tree, changed blob
    // Inspect every pack, including duplicates: counting the object database
    // alone would miss the server resending old objects in a second pack.
    let mut packed = Vec::new();
    for entry in std::fs::read_dir(dir.join(".git/objects/pack")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "idx") {
            let listing = git(&dir, &["verify-pack", "-v", path.to_str().unwrap()]).await;
            packed.extend(listing.lines().filter_map(|line| {
                let id = line.split_whitespace().next()?;
                (id.len() == 64).then(|| id.to_owned())
            }));
        }
    }
    assert_eq!(packed.len(), new.len(), "incremental pack repeated objects");
    git(&dir, &["fsck", "--full"]).await;
}
