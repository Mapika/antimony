use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ant_store::{CasOutcome, ChangeKind, ObjectStoreExt, RefStore};
use tokio::sync::{Notify, Semaphore};
use tokio::time::timeout;

use super::queue::{MAX_BATCH, enqueue};
use super::*;
use crate::ops::test_support::{commit, read, write};

const DEADLINE: Duration = Duration::from_secs(20);

struct ObservedRefs {
    inner: Arc<dyn RefStore>,
    calls: AtomicUsize,
    block: AtomicBool,
    entered: Notify,
    release: Semaphore,
    move_to: Mutex<Option<ObjectId>>,
    always_mismatch: AtomicBool,
}

impl ObservedRefs {
    fn install(b: &mut Backend) -> Arc<Self> {
        let refs = Arc::new(Self {
            inner: b.refs.clone(),
            calls: AtomicUsize::new(0),
            block: AtomicBool::new(false),
            entered: Notify::new(),
            release: Semaphore::new(0),
            move_to: Mutex::new(None),
            always_mismatch: AtomicBool::new(false),
        });
        b.refs = refs.clone();
        refs
    }
}

#[async_trait::async_trait]
impl RefStore for ObservedRefs {
    async fn get_ref(&self, name: &str) -> ant_store::Result<Option<ObjectId>> {
        self.inner.get_ref(name).await
    }

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ObjectId>,
        new: Option<ObjectId>,
    ) -> ant_store::Result<CasOutcome> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.always_mismatch.load(Ordering::SeqCst) {
            return Ok(CasOutcome::Mismatch { actual: expected });
        }
        if self.block.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        let move_to = self.move_to.lock().unwrap().take();
        if let Some(id) = move_to {
            assert_eq!(self.inner.cas_ref(name, expected, Some(id)).await?, CasOutcome::Updated);
        }
        self.inner.cas_ref(name, expected, new).await
    }

    async fn list_refs(&self, prefix: &str) -> ant_store::Result<Vec<(String, ObjectId)>> {
        self.inner.list_refs(prefix).await
    }
}

// Enqueue synchronously on a current-thread runtime, making batch membership
// and arrival order deterministic without sleeps or timing assumptions.
async fn batch(b: &Backend, commits: &[ObjectId]) -> Vec<OpResult<LandOutcome>> {
    let replies: Vec<_> = commits.iter().map(|id| enqueue(b, "main", *id)).collect();
    timeout(DEADLINE, futures::future::join_all(replies))
        .await
        .unwrap()
        .into_iter()
        .map(Result::unwrap)
        .collect()
}

async fn history(b: &Backend, root: ObjectId) -> Vec<ObjectId> {
    let mut current = b.refs.get_ref("main").await.unwrap().unwrap();
    let mut chain = Vec::new();
    while current != root {
        assert!(!chain.contains(&current));
        chain.push(current);
        let node = b.objects.read_commit(&current).await.unwrap();
        assert_eq!(node.parents.len(), 1);
        current = node.parents[0];
    }
    chain.reverse();
    chain
}

#[tokio::test]
async fn middle_conflict_uses_one_cas_and_keeps_own_tips() {
    let mut b = Backend::in_memory();
    let root = commit(&b, None, vec![write("f", "base\n")]).await.unwrap();
    land(&b, "main", root).await.unwrap();
    let a = commit(&b, Some(root), vec![write("f", "a\n")]).await.unwrap();
    let conflict =
        commit(&b, Some(root), vec![write("f", "b\n"), write("leak", "bad")]).await.unwrap();
    let c = commit(&b, Some(root), vec![write("c", "c")]).await.unwrap();
    let refs = ObservedRefs::install(&mut b);
    let outcomes = batch(&b, &[a, conflict, c]).await;
    assert_eq!(outcomes[0].as_ref().unwrap(), &LandOutcome::Landed { new_tip: a, rebased: false });
    assert_eq!(
        outcomes[1].as_ref().unwrap(),
        &LandOutcome::Conflict { tip: a, paths: vec![RepoPath::new("f").unwrap()] }
    );
    let LandOutcome::Landed { new_tip, rebased: true } = outcomes[2].as_ref().unwrap() else {
        panic!()
    };
    assert_eq!(history(&b, root).await, [a, *new_tip]);
    assert_eq!(read(&b, *new_tip, "f").await.as_deref(), Some("a\n"));
    assert_eq!(read(&b, *new_tip, "c").await.as_deref(), Some("c"));
    assert_eq!(read(&b, *new_tip, "leak").await, None);
    assert_eq!(refs.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn two_hundred_concurrent_lands_have_exact_history_and_counts() {
    let mut b = Backend::in_memory();
    let root = commit(&b, None, vec![write("seed", "s")]).await.unwrap();
    land(&b, "main", root).await.unwrap();
    let mut commits = Vec::new();
    for i in 0..200 {
        commits.push(commit(&b, Some(root), vec![write(&format!("f{i}"), "done")]).await.unwrap());
    }
    let refs = ObservedRefs::install(&mut b);
    let outcomes = timeout(
        DEADLINE,
        futures::future::join_all(commits.iter().map(|id| land(&b, "main", *id))),
    )
    .await
    .unwrap();
    let tips: Vec<_> = outcomes
        .into_iter()
        .map(|outcome| match outcome.unwrap() {
            LandOutcome::Landed { new_tip, .. } => new_tip,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(tips.len(), 200);
    assert_eq!(tips.iter().collect::<BTreeSet<_>>().len(), 200);
    assert_eq!(history(&b, root).await, tips);
    assert_eq!(refs.calls.load(Ordering::SeqCst), 200_usize.div_ceil(MAX_BATCH));
    let tip = *tips.last().unwrap();
    let tree = b.objects.read_tree(&b.objects.read_commit(&tip).await.unwrap().tree).await.unwrap();
    assert_eq!(tree.entries().len(), 201);
    for i in 0..200 {
        assert_eq!(read(&b, tip, &format!("f{i}")).await.as_deref(), Some("done"));
    }
    // Every original can be retried, without extending the history.
    for outcome in batch(&b, &commits).await {
        assert_eq!(outcome.unwrap(), LandOutcome::Landed { new_tip: tip, rebased: false });
    }
    assert_eq!(history(&b, root).await.len(), 200);
}

#[tokio::test]
async fn dropping_leader_caller_mid_cas_does_not_strand_followers() {
    let mut b = Backend::in_memory();
    let root = commit(&b, None, vec![]).await.unwrap();
    land(&b, "main", root).await.unwrap();
    let a = commit(&b, Some(root), vec![write("a", "a")]).await.unwrap();
    let c = commit(&b, Some(root), vec![write("c", "c")]).await.unwrap();
    let d = commit(&b, Some(root), vec![write("d", "d")]).await.unwrap();
    let refs = ObservedRefs::install(&mut b);
    refs.block.store(true, Ordering::SeqCst);
    let backend = b.clone();
    let caller = tokio::spawn(async move { land(&backend, "main", a).await });
    timeout(DEADLINE, refs.entered.notified()).await.unwrap();
    let c_reply = enqueue(&b, "main", c);
    let d_reply = enqueue(&b, "main", d);
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    refs.release.add_permits(1);
    for reply in [c_reply, d_reply] {
        assert!(matches!(
            timeout(DEADLINE, reply).await.unwrap().unwrap().unwrap(),
            LandOutcome::Landed { .. }
        ));
    }
    assert_eq!(history(&b, root).await.len(), 3);
    let tip = b.refs.get_ref("main").await.unwrap().unwrap();
    for path in ["a", "c", "d"] {
        assert_eq!(read(&b, tip, path).await.as_deref(), Some(path));
    }
}

#[tokio::test]
async fn duplicate_and_overlapping_stacks_share_tentative_mappings() {
    let b = Backend::in_memory();
    let root = commit(&b, None, vec![]).await.unwrap();
    let moved = commit(&b, Some(root), vec![write("moved", "m")]).await.unwrap();
    land(&b, "main", moved).await.unwrap();
    let lower = commit(&b, Some(root), vec![write("lower", "l")]).await.unwrap();
    let upper = commit(&b, Some(lower), vec![write("upper", "u")]).await.unwrap();
    let outcomes = batch(&b, &[lower, lower, upper, lower, upper]).await;
    assert!(outcomes.iter().all(|o| matches!(o, Ok(LandOutcome::Landed { .. }))));
    let chain = history(&b, moved).await;
    assert_eq!(chain.len(), 2);
    assert_eq!(
        outcomes[1].as_ref().unwrap(),
        &LandOutcome::Landed { new_tip: chain[0], rebased: false }
    );
    assert_eq!(
        outcomes[3].as_ref().unwrap(),
        &LandOutcome::Landed { new_tip: chain[1], rebased: false }
    );
    assert_eq!(read(&b, chain[1], "lower").await.as_deref(), Some("l"));
    assert_eq!(read(&b, chain[1], "upper").await.as_deref(), Some("u"));
    assert_eq!(
        land(&b, "main", lower).await.unwrap(),
        LandOutcome::Landed { new_tip: chain[1], rebased: false }
    );
}

#[tokio::test]
async fn conflicting_stack_does_not_publish_its_prefix() {
    let b = Backend::in_memory();
    let root = commit(&b, None, vec![write("f", "base\n")]).await.unwrap();
    let moved = commit(&b, Some(root), vec![write("f", "ours\n")]).await.unwrap();
    land(&b, "main", moved).await.unwrap();
    let lower = commit(&b, Some(root), vec![write("lower", "l")]).await.unwrap();
    let upper = commit(&b, Some(lower), vec![write("f", "theirs\n")]).await.unwrap();
    let other = commit(&b, Some(root), vec![write("other", "o")]).await.unwrap();
    let outcomes = batch(&b, &[upper, other]).await;
    assert!(matches!(outcomes[0], Ok(LandOutcome::Conflict { .. })));
    let tip = b.refs.get_ref("main").await.unwrap().unwrap();
    assert_eq!(history(&b, moved).await.len(), 1);
    assert_eq!(read(&b, tip, "lower").await, None);
    assert!(b.meta.get_derived(LANDED_AS, lower.as_bytes()).await.unwrap().is_none());
    assert!(matches!(
        land(&b, "main", lower).await.unwrap(),
        LandOutcome::Landed { rebased: true, .. }
    ));
    assert_eq!(history(&b, moved).await.len(), 2);
}

#[tokio::test]
async fn cas_mismatch_recomputes_every_outcome() {
    let mut b = Backend::in_memory();
    let root = commit(&b, None, vec![write("f", "base\n")]).await.unwrap();
    land(&b, "main", root).await.unwrap();
    let a = commit(&b, Some(root), vec![write("f", "a\n")]).await.unwrap();
    let c = commit(&b, Some(root), vec![write("c", "c")]).await.unwrap();
    let external = commit(&b, Some(root), vec![write("f", "external\n")]).await.unwrap();
    let refs = ObservedRefs::install(&mut b);
    *refs.move_to.lock().unwrap() = Some(external);
    let outcomes = batch(&b, &[a, c]).await;
    assert_eq!(
        outcomes[0].as_ref().unwrap(),
        &LandOutcome::Conflict { tip: external, paths: vec![RepoPath::new("f").unwrap()] }
    );
    let LandOutcome::Landed { new_tip, rebased: true } = outcomes[1].as_ref().unwrap() else {
        panic!()
    };
    assert_eq!(history(&b, root).await, [external, *new_tip]);
    assert_eq!(refs.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        b.meta.get_derived(LANDED_AS, c.as_bytes()).await.unwrap().unwrap().as_ref(),
        new_tip.as_bytes()
    );
}

#[tokio::test]
async fn retry_exhaustion_aborts_the_entire_batch() {
    let mut b = Backend::in_memory();
    let root = commit(&b, None, vec![]).await.unwrap();
    land(&b, "main", root).await.unwrap();
    let a = commit(&b, Some(root), vec![write("a", "a")]).await.unwrap();
    let c = commit(&b, Some(root), vec![write("c", "c")]).await.unwrap();
    let refs = ObservedRefs::install(&mut b);
    refs.always_mismatch.store(true, Ordering::SeqCst);
    for outcome in batch(&b, &[a, c]).await {
        assert!(matches!(outcome, Err(OpError::Aborted(_))));
    }
    assert_eq!(refs.calls.load(Ordering::SeqCst), MAX_LAND_ATTEMPTS);
    assert_eq!(b.refs.get_ref("main").await.unwrap(), Some(root));
    assert!(b.meta.get_derived(LANDED_AS, c.as_bytes()).await.unwrap().is_none());
    refs.always_mismatch.store(false, Ordering::SeqCst);
    assert!(matches!(land(&b, "main", c).await.unwrap(), LandOutcome::Landed { .. }));
}

#[tokio::test]
async fn invalid_request_does_not_block_valid_neighbors() {
    let b = Backend::in_memory();
    let root = commit(&b, None, vec![]).await.unwrap();
    let next = commit(&b, Some(root), vec![write("a", "a")]).await.unwrap();
    let outcomes = batch(&b, &[root, ObjectId([9; 32]), next]).await;
    assert!(matches!(outcomes[0], Ok(LandOutcome::Landed { .. })));
    assert!(matches!(outcomes[1], Err(OpError::NotFound(_))));
    assert!(matches!(outcomes[2], Ok(LandOutcome::Landed { .. })));
    assert_eq!(history(&b, root).await, [next]);
}

#[tokio::test]
async fn rebased_changesets_match_tree_diff_including_directory_replacement() {
    use crate::ops::test_support::sig;
    use ant_store::graph::{build_tree, diff_trees, write_commit};
    let b = Backend::in_memory();
    let root = commit(&b, None, vec![write("d/f", "f"), write("same", "s")]).await.unwrap();
    let moved = commit(&b, Some(root), vec![write("other", "o")]).await.unwrap();
    let tree = build_tree(b.objects.as_ref(), &[("d", b"file"), ("same", b"s")]).await.unwrap();
    let original = write_commit(
        &b,
        &ant_core::Commit {
            tree,
            parents: vec![root],
            author: sig(),
            committer: sig(),
            extra_headers: vec![],
            message: "replace directory".into(),
        },
    )
    .await
    .unwrap();
    let rebased = rebase(&b, original, moved).await.unwrap().unwrap();
    let old_tree = b.objects.read_commit(&moved).await.unwrap().tree;
    let new_tree = b.objects.read_commit(&rebased).await.unwrap().tree;
    let changes = b.meta.get_changeset(&rebased).await.unwrap().unwrap();
    assert_eq!(
        changes,
        diff_trees(b.objects.as_ref(), Some(old_tree), Some(new_tree)).await.unwrap()
    );
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].old, None);
    assert_eq!(changes[0].kind, ChangeKind::Added);
    // Reapplying identical changes records an empty changeset.
    let again = rebase(&b, original, rebased).await.unwrap().unwrap();
    assert!(b.meta.get_changeset(&again).await.unwrap().unwrap().is_empty());
}
