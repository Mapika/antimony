//! Lock turns: a worker keeps the ref lock past a turn while nobody else
//! waits for it, and hands it over within a turn when someone does.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ant_store::{CasOutcome, RefLock, RefStore};
use tokio::sync::Notify;
use tokio::time::timeout;

use super::queue::enqueue;
use super::*;
use crate::ops::test_support::{DEADLINE, commit, history, replies, write};

/// Forwards to a ref store with real locks, counting lock grants and
/// slowing every CAS down so a long queue spans several turns.
struct SlowLockedRefs {
    inner: Arc<dyn RefStore>,
    grants: AtomicUsize,
    granted: Notify,
}

#[async_trait::async_trait]
impl RefStore for SlowLockedRefs {
    async fn get_ref(&self, name: &str) -> ant_store::Result<Option<ObjectId>> {
        self.inner.get_ref(name).await
    }

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ObjectId>,
        new: Option<ObjectId>,
    ) -> ant_store::Result<CasOutcome> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.inner.cas_ref(name, expected, new).await
    }

    async fn list_refs(&self, prefix: &str) -> ant_store::Result<Vec<(String, ObjectId)>> {
        self.inner.list_refs(prefix).await
    }

    async fn lock_ref(&self, name: &str) -> ant_store::Result<RefLock> {
        let lock = self.inner.lock_ref(name).await?;
        self.grants.fetch_add(1, Ordering::SeqCst);
        self.granted.notify_one();
        Ok(lock)
    }
}

/// A landed root and `n` commits on it touching distinct paths, with the
/// backend's ref store wrapped (which also gives it its own land queue).
async fn setup(n: usize) -> (Backend, Arc<SlowLockedRefs>, ObjectId, Vec<ObjectId>) {
    let mut b = Backend::in_memory();
    let root = commit(&b, None, vec![write("seed", "s")]).await.unwrap();
    land(&b, "main", root).await.unwrap();
    let mut commits = Vec::with_capacity(n);
    for i in 0..n {
        commits.push(commit(&b, Some(root), vec![write(&format!("f{i}"), "x")]).await.unwrap());
    }
    let refs = Arc::new(SlowLockedRefs {
        inner: b.refs.clone(),
        grants: AtomicUsize::new(0),
        granted: Notify::new(),
    });
    b.refs = refs.clone();
    (b, refs, root, commits)
}

#[tokio::test]
async fn uncontended_lock_is_kept_across_turns() {
    // 10 batches of 64, each publish taking >= 5 ms: several turns long.
    let (b, refs, root, commits) = setup(640).await;
    let start = std::time::Instant::now();
    let pending: Vec<_> = commits.iter().map(|id| enqueue(&b, "main", *id)).collect();
    for outcome in timeout(DEADLINE, replies(pending)).await.unwrap() {
        assert!(matches!(outcome, Ok(LandOutcome::Landed { .. })), "{outcome:?}");
    }
    assert!(start.elapsed() > Duration::from_millis(40), "the run must span several turns");
    assert_eq!(refs.grants.load(Ordering::SeqCst), 1, "nobody waited: one hold of the lock");
    assert_eq!(history(&b, root).await.len(), 640);
}

#[tokio::test]
async fn waiting_writer_gets_the_lock_within_a_turn() {
    let (b, refs, root, commits) = setup(1280).await;
    let pending: Vec<_> = commits.iter().map(|id| enqueue(&b, "main", *id)).collect();
    timeout(DEADLINE, refs.granted.notified()).await.unwrap();
    // Another writer (a replica, an import publishing the ref) asks for the
    // lock while the worker lands: it gets it long before the queue is done.
    let waiter = {
        let refs = refs.clone();
        tokio::spawn(async move {
            let lock = refs.inner.lock_ref("main").await.unwrap();
            let tip = refs.inner.get_ref("main").await.unwrap();
            lock.release().await.unwrap();
            tip
        })
    };
    let tip_seen = timeout(DEADLINE, waiter).await.unwrap().unwrap();
    let outcomes = timeout(DEADLINE, replies(pending)).await.unwrap();
    let last = match outcomes.last() {
        Some(Ok(LandOutcome::Landed { new_tip, .. })) => *new_tip,
        other => panic!("{other:?}"),
    };
    assert_ne!(tip_seen, Some(last), "the waiter must not wait for the whole queue");
    assert!(refs.grants.load(Ordering::SeqCst) >= 2, "the worker took the lock again");
    assert_eq!(history(&b, root).await.len(), 1280);
}
