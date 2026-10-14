use ant_core::{Commit, EncodedObject, EntryMode, Object, ObjectKind, TreeEntry};
use ant_store::graph::{build_tree, lookup_path, write_commit};
use ant_store::{CasOutcome, ObjectStoreExt};

use super::*;
use crate::ops::test_support::sig;

async fn snapshot(b: &Backend, parent: Option<ObjectId>, files: &[(&str, &[u8])]) -> ObjectId {
    let tree = build_tree(b.objects.as_ref(), files).await.unwrap();
    write_commit(
        b,
        &Commit {
            tree,
            parents: parent.into_iter().collect(),
            author: sig(),
            committer: sig(),
            extra_headers: vec![],
            message: "snapshot\n".into(),
        },
    )
    .await
    .unwrap()
}

async fn contents(b: &Backend, commit: ObjectId, path: &str) -> Option<Vec<u8>> {
    let tree = b.objects.read_commit(&commit).await.unwrap().tree;
    let (_, id) =
        lookup_path(b.objects.as_ref(), tree, &RepoPath::new(path).unwrap()).await.unwrap()?;
    Some(b.objects.read_blob(&id).await.unwrap().to_vec())
}

#[tokio::test]
async fn fast_forward_and_validation() {
    let b = Backend::in_memory();
    let missing = ObjectId([19; 32]);
    assert!(matches!(land(&b, "main", missing).await, Err(OpError::NotFound(_))));
    assert!(matches!(rebase(&b, missing, missing).await, Err(OpError::NotFound(_))));
    let root = snapshot(&b, None, &[("f", b"one")]).await;
    assert!(matches!(rebase(&b, root, root).await, Err(OpError::InvalidArgument(_))));
    assert_eq!(
        land(&b, "main", root).await.unwrap(),
        LandOutcome::Landed { new_tip: root, rebased: false }
    );
    let next = snapshot(&b, Some(root), &[("f", b"two")]).await;
    assert_eq!(
        land(&b, "main", next).await.unwrap(),
        LandOutcome::Landed { new_tip: next, rebased: false }
    );
    assert_eq!(
        land(&b, "main", root).await.unwrap(),
        LandOutcome::Landed { new_tip: next, rebased: false }
    );
    assert!(matches!(rebase(&b, next, missing).await, Err(OpError::NotFound(_))));
}

#[tokio::test]
async fn text_merge_preserves_metadata_and_mode() {
    let b = Backend::in_memory();
    let base = snapshot(&b, None, &[("f", b"a\nb\nc\nd\ne\n")]).await;
    let ours = snapshot(&b, Some(base), &[("f", b"A\nb\nc\nd\ne\n")]).await;
    let theirs = snapshot(&b, Some(base), &[("f", b"a\nb\nc\nd\nE\n")]).await;
    let mut original = b.objects.read_commit(&theirs).await.unwrap();
    let mut tree = b.objects.read_tree(&original.tree).await.unwrap();
    let mut file = tree.get("f").unwrap().clone();
    file.mode = EntryMode::Executable;
    tree.upsert(file).unwrap();
    original.tree = b.objects.put(Object::Tree(tree).encode()).await.unwrap();
    original.committer.time += 42;
    original.message = "verbatim\n\nmessage".into();
    let theirs = write_commit(&b, &original).await.unwrap();
    let merged = rebase(&b, theirs, ours).await.unwrap().unwrap();
    assert_eq!(contents(&b, merged, "f").await.unwrap(), b"A\nb\nc\nd\nE\n");
    let actual = b.objects.read_commit(&merged).await.unwrap();
    assert_eq!(actual.author, original.author);
    assert_eq!(actual.committer, original.committer);
    assert_eq!(actual.message, original.message);
    assert_eq!(actual.parents, [ours]);
    assert_eq!(
        b.objects.read_tree(&actual.tree).await.unwrap().get("f").unwrap().mode,
        EntryMode::Executable
    );
}

#[tokio::test]
async fn sorted_conflicts_leave_ref_and_log_unchanged() {
    let b = Backend::in_memory();
    let base = snapshot(&b, None, &[("z", b"old"), ("bin", b"\xff"), ("p/q", b"q")]).await;
    let ours = snapshot(&b, Some(base), &[("z", b"ours"), ("bin", b"\xfe"), ("p", b"file")]).await;
    let theirs = snapshot(
        &b,
        Some(base),
        &[("z", b"theirs"), ("bin", b"\xfd"), ("p/q", b"q"), ("p/new", b"new")],
    )
    .await;
    land(&b, "main", ours).await.unwrap();
    let log = b.meta.tail(0, 100).await.unwrap();
    assert_eq!(
        land(&b, "main", theirs).await.unwrap(),
        LandOutcome::Conflict {
            tip: ours,
            paths: ["bin", "p/new", "z"].map(|p| RepoPath::new(p).unwrap()).to_vec(),
        }
    );
    assert_eq!(b.refs.get_ref("main").await.unwrap(), Some(ours));
    assert_eq!(b.meta.tail(0, 100).await.unwrap(), log);
}

#[tokio::test]
async fn replacements_deletions_and_identical_changes() {
    let b = Backend::in_memory();
    let base = snapshot(&b, None, &[("p", b"file"), ("d/q", b"q"), ("same", b"old")]).await;
    let ours = snapshot(
        &b,
        Some(base),
        &[("p", b"file"), ("d/q", b"q"), ("same", b"new"), ("other", b"ours")],
    )
    .await;
    let theirs =
        snapshot(&b, Some(base), &[("p/q", b"nested"), ("d", b"file"), ("same", b"new")]).await;
    let merged = rebase(&b, theirs, ours).await.unwrap().unwrap();
    for (path, expected) in [("p/q", "nested"), ("d", "file"), ("same", "new"), ("other", "ours")] {
        assert_eq!(contents(&b, merged, path).await.unwrap(), expected.as_bytes());
    }
    let tree = b.objects.read_commit(&merged).await.unwrap().tree;
    assert!(
        lookup_path(b.objects.as_ref(), tree, &RepoPath::new("d/q").unwrap())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_disjoint_lands() {
    let b = Backend::in_memory();
    let root = snapshot(&b, None, &[("seed", b"s")]).await;
    land(&b, "main", root).await.unwrap();
    let mut commits = Vec::new();
    for i in 0..24 {
        commits.push(
            snapshot(&b, Some(root), &[("seed", b"s"), (&format!("agent{i}/out"), b"done")]).await,
        );
    }
    let mut tasks = Vec::new();
    for id in commits {
        let b = b.clone();
        tasks.push(tokio::spawn(async move { land(&b, "main", id).await }));
    }
    for task in tasks {
        assert!(matches!(task.await.unwrap().unwrap(), LandOutcome::Landed { .. }));
    }
    let tip = crate::ops::test_support::tip(&b).await;
    for i in 0..24 {
        assert_eq!(contents(&b, tip, &format!("agent{i}/out")).await.unwrap(), b"done");
    }
    assert_eq!(b.meta.get_commits(&[tip]).await.unwrap()[0].as_ref().unwrap().generation, 25);
}

#[tokio::test]
async fn symlink_and_simultaneous_mode_changes_conflict() {
    for mode in [EntryMode::Symlink, EntryMode::Executable] {
        let b = Backend::in_memory();
        let base = snapshot(&b, None, &[("f", b"a\nb\nc\nd\ne\n")]).await;
        let mut sides = Vec::new();
        for text in [b"A\nb\nc\nd\ne\n", b"a\nb\nc\nd\nE\n"] {
            let blob = EncodedObject::new(ObjectKind::Blob, text);
            let id = blob.id();
            b.objects.put(blob).await.unwrap();
            let tree = ant_store::graph::write_tree(
                b.objects.as_ref(),
                vec![TreeEntry { name: "f".into(), mode, id }],
            )
            .await
            .unwrap();
            sides.push(
                write_commit(
                    &b,
                    &Commit {
                        tree,
                        parents: vec![base],
                        author: sig(),
                        committer: sig(),
                        extra_headers: vec![],
                        message: "side".into(),
                    },
                )
                .await
                .unwrap(),
            );
        }
        assert_eq!(
            rebase(&b, sides[0], sides[1]).await.unwrap(),
            Err(vec![RepoPath::new("f").unwrap()])
        );
    }
}

struct ContendedRefs {
    attempts: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl ant_store::RefStore for ContendedRefs {
    async fn get_ref(&self, _: &str) -> ant_store::Result<Option<ObjectId>> {
        Ok(None)
    }
    async fn cas_ref(
        &self,
        _: &str,
        _: Option<ObjectId>,
        _: Option<ObjectId>,
    ) -> ant_store::Result<CasOutcome> {
        self.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(CasOutcome::Mismatch { actual: None })
    }
    async fn list_refs(&self, _: &str) -> ant_store::Result<Vec<(String, ObjectId)>> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn aborts_after_exact_retry_limit() {
    let mut b = Backend::in_memory();
    let refs = std::sync::Arc::new(ContendedRefs { attempts: 0.into() });
    b.refs = refs.clone();
    let root = snapshot(&b, None, &[]).await;
    assert!(matches!(land(&b, "main", root).await, Err(OpError::Aborted(_))));
    assert_eq!(refs.attempts.load(std::sync::atomic::Ordering::SeqCst), MAX_LAND_ATTEMPTS);
}
