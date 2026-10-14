//! Pipelined batches: a batch replayed and written while its predecessor's
//! publish is in flight is published only after it, is redone when the
//! predecessor's CAS mismatches or fails, and sees the predecessor's
//! unpublished landed-as mappings.

use std::sync::atomic::Ordering;

use tokio::time::timeout;

use super::queue::enqueue;
use super::*;
use crate::ops::test_support::{DEADLINE, GatedRefs, commit, history, read, replies, write};

type Receiver = tokio::sync::oneshot::Receiver<OpResult<LandOutcome>>;

/// Land `first` as one batch and `then` as the batch pipelined behind it:
/// the first batch's ref read is held until `then` is queued.
async fn pipelined(
    b: &Backend,
    refs: &GatedRefs,
    first: &[ObjectId],
    then: &[ObjectId],
) -> (Vec<Receiver>, Vec<Receiver>) {
    refs.hold_get.store(true, Ordering::SeqCst);
    let first: Vec<_> = first.iter().map(|id| enqueue(b, "main", *id)).collect();
    timeout(DEADLINE, refs.get_entered.notified()).await.unwrap();
    let then: Vec<_> = then.iter().map(|id| enqueue(b, "main", *id)).collect();
    refs.get_release.add_permits(1);
    (first, then)
}

/// Whether some recorded commit has exactly `parents` (evidence that a
/// speculative replay wrote a commit onto a tentative tip).
async fn recorded_with_parents(b: &Backend, parents: &[ObjectId]) -> bool {
    let ids: Vec<_> = b.meta.tail(0, 10_000).await.unwrap().into_iter().map(|(_, id)| id).collect();
    b.meta.get_commits(&ids).await.unwrap().into_iter().flatten().any(|n| n.parents == parents)
}

fn landed(outcome: &OpResult<LandOutcome>) -> ObjectId {
    match outcome {
        Ok(LandOutcome::Landed { new_tip, .. }) => *new_tip,
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn speculative_batch_is_redone_after_predecessor_cas_mismatch() {
    let mut b = Backend::in_memory();
    let root = commit(&b, None, vec![write("seed", "s")]).await.unwrap();
    land(&b, "main", root).await.unwrap();
    let a = commit(&b, Some(root), vec![write("a", "a")]).await.unwrap();
    let c = commit(&b, Some(root), vec![write("c", "c")]).await.unwrap();
    let d = commit(&b, Some(root), vec![write("d", "d")]).await.unwrap();
    let external = commit(&b, Some(root), vec![write("x", "x")]).await.unwrap();
    let refs = GatedRefs::install(&mut b);
    *refs.move_to.lock().unwrap() = Some(external);
    let (first, then) = pipelined(&b, &refs, &[a], &[c, d]).await;
    let first = replies(first).await;
    let then = replies(then).await;
    // [c, d] were replayed onto the tentative tip `a` (a fast-forward)...
    assert!(recorded_with_parents(&b, &[a]).await);
    // ...but that speculation was discarded: everything lands on `external`.
    let chain = history(&b, root).await;
    assert_eq!(chain.len(), 4);
    assert_eq!(chain[0], external);
    assert_eq!(landed(&first[0]), chain[1]);
    assert_eq!(landed(&then[0]), chain[2]);
    assert_eq!(landed(&then[1]), chain[3]);
    for path in ["a", "c", "d", "x"] {
        assert_eq!(read(&b, chain[3], path).await.as_deref(), Some(path));
    }
    // Mismatch, retry of [a], then [c, d].
    assert_eq!(refs.cas_calls.load(Ordering::SeqCst), 3);
    // Mappings point at the published commits, not the speculative ones.
    for (original, published) in [(c, chain[2]), (d, chain[3])] {
        let mapped = b.meta.get_derived(LANDED_AS, original.as_bytes()).await.unwrap().unwrap();
        assert_eq!(mapped.as_ref(), published.as_bytes());
    }
}

#[tokio::test]
async fn speculative_batch_lands_after_predecessor_publish_fails() {
    let mut b = Backend::in_memory();
    let root = commit(&b, None, vec![]).await.unwrap();
    land(&b, "main", root).await.unwrap();
    let a = commit(&b, Some(root), vec![write("a", "a")]).await.unwrap();
    let c = commit(&b, Some(root), vec![write("c", "c")]).await.unwrap();
    let d = commit(&b, Some(root), vec![write("d", "d")]).await.unwrap();
    let refs = GatedRefs::install(&mut b);
    refs.fail_cas.store(true, Ordering::SeqCst);
    let (first, then) = pipelined(&b, &refs, &[a], &[c, d]).await;
    assert!(matches!(replies(first).await[0], Err(OpError::Aborted(_))));
    assert!(recorded_with_parents(&b, &[a]).await);
    // The speculative batch is requeued, not failed with its predecessor.
    let then = replies(then).await;
    let chain = history(&b, root).await;
    assert_eq!(chain.len(), 2);
    assert_eq!([landed(&then[0]), landed(&then[1])], [chain[0], chain[1]]);
    assert_eq!(read(&b, chain[1], "a").await, None);
    assert!(b.meta.get_derived(LANDED_AS, a.as_bytes()).await.unwrap().is_none());
    // The failed request can simply be retried.
    assert!(matches!(land(&b, "main", a).await.unwrap(), LandOutcome::Landed { .. }));
    assert_eq!(history(&b, root).await.len(), 3);
}

#[tokio::test]
async fn speculative_batch_sees_unpublished_mappings_of_its_predecessor() {
    let mut b = Backend::in_memory();
    let root = commit(&b, None, vec![]).await.unwrap();
    let moved = commit(&b, Some(root), vec![write("moved", "m")]).await.unwrap();
    land(&b, "main", moved).await.unwrap();
    let lower = commit(&b, Some(root), vec![write("lower", "l")]).await.unwrap();
    let upper = commit(&b, Some(lower), vec![write("upper", "u")]).await.unwrap();
    let refs = GatedRefs::install(&mut b);
    // Hold the predecessor's CAS so the next batch replays before `lower`'s
    // mapping is published.
    refs.hold_cas.store(true, Ordering::SeqCst);
    let (first, then) = pipelined(&b, &refs, &[lower], &[upper, lower]).await;
    timeout(DEADLINE, refs.cas_entered.notified()).await.unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    refs.cas_release.add_permits(1);
    let first = replies(first).await;
    let then = replies(then).await;
    // `upper` replays alone on top of `lower`'s rebase; `lower` is not
    // replayed again, by either request.
    let chain = history(&b, moved).await;
    assert_eq!(chain.len(), 2);
    assert_eq!(landed(&first[0]), chain[0]);
    assert_eq!(landed(&then[0]), chain[1]);
    assert_eq!(
        then[1].as_ref().unwrap(),
        &LandOutcome::Landed { new_tip: chain[1], rebased: false }
    );
    assert_eq!(read(&b, chain[1], "lower").await.as_deref(), Some("l"));
    assert_eq!(read(&b, chain[1], "upper").await.as_deref(), Some("u"));
    assert_eq!(refs.cas_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn speculative_replies_wait_for_the_predecessor_publish() {
    let mut b = Backend::in_memory();
    let root = commit(&b, None, vec![]).await.unwrap();
    land(&b, "main", root).await.unwrap();
    let a = commit(&b, Some(root), vec![write("a", "a")]).await.unwrap();
    let refs = GatedRefs::install(&mut b);
    refs.hold_cas.store(true, Ordering::SeqCst);
    // `root` is already landed: the next batch moves nothing and could reply
    // at once, but its answer (tip `a`) is not published yet.
    let (first, mut then) = pipelined(&b, &refs, &[a], &[root]).await;
    timeout(DEADLINE, refs.cas_entered.notified()).await.unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(then[0].try_recv().is_err());
    assert_eq!(b.refs.get_ref("main").await.unwrap(), Some(root));
    refs.cas_release.add_permits(1);
    assert_eq!(landed(&replies(first).await[0]), a);
    assert_eq!(
        replies(then).await[0].as_ref().unwrap(),
        &LandOutcome::Landed { new_tip: a, rebased: false }
    );
    assert_eq!(refs.cas_calls.load(Ordering::SeqCst), 1);
}
