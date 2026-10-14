use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ant_core::ObjectId;
use ant_store::feed::{MoveCause, RefFeed, RefMove, RefNotice};
use ant_store::{Backend, CasOutcome, RefStore};
use futures::{Stream, StreamExt};
use tokio::time::timeout;

use super::*;
use crate::ops::land::{self, LandOutcome};
use crate::ops::test_support::{DEADLINE, commit, history, write};

type Events = Pin<Box<dyn Stream<Item = OpResult<RefEvent>> + Send>>;

async fn watch(b: &Backend, from: Option<ObjectId>, buffer: usize) -> Events {
    Box::pin(watch_ref(b, None, "main", from, WatchOptions { buffer }, None).await.unwrap())
}

async fn next(events: &mut Events) -> RefEvent {
    timeout(DEADLINE, events.next()).await.expect("no event in time").unwrap().unwrap()
}

/// Events until one moves the ref to `end`; asserts they chain from `start`
/// and returns them.
async fn until(events: &mut Events, start: Option<ObjectId>, end: ObjectId) -> Vec<RefEvent> {
    let mut out = Vec::new();
    let mut at = start;
    while at != Some(end) {
        let e = next(events).await;
        assert_eq!(e.old, at, "events must chain: {out:?} then {e:?}");
        at = e.new;
        out.push(e);
    }
    out
}

fn landed(events: &[RefEvent]) -> Vec<ObjectId> {
    events.iter().flat_map(|e| e.landed.iter().copied()).collect()
}

/// `main` at a root commit.
async fn repo() -> (Backend, ObjectId) {
    let b = Backend::in_memory();
    let root = commit(&b, None, vec![write("README", "r\n")]).await.unwrap();
    assert_eq!(b.refs.cas_ref("main", None, Some(root)).await.unwrap(), CasOutcome::Updated);
    (b, root)
}

async fn land_change(b: &Backend, base: ObjectId, path: &str) -> ObjectId {
    let c = commit(b, Some(base), vec![write(path, &format!("{path}\n"))]).await.unwrap();
    match land::land(b, "main", c).await.unwrap() {
        LandOutcome::Landed { new_tip, .. } => new_tip,
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn every_land_is_an_event_in_order() {
    let (b, root) = repo().await;
    let mut events = watch(&b, None, WATCH_BUFFER).await;
    let mut tips = Vec::new();
    for i in 0..5 {
        tips.push(land_change(&b, root, &format!("f{i}")).await);
    }
    let got = until(&mut events, Some(root), tips[4]).await;
    assert_eq!(got.len(), 5);
    for (e, tip) in got.iter().zip(&tips) {
        assert_eq!(
            (e.kind, e.coalesced, e.landed.as_slice()),
            (EventKind::Land, false, &[*tip][..])
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_lands_each_appear_in_landing_order() {
    let (b, root) = repo().await;
    let mut events = watch(&b, None, WATCH_BUFFER).await;
    let mut changes = Vec::new();
    for i in 0..48 {
        changes.push(commit(&b, Some(root), vec![write(&format!("b{i}"), "x\n")]).await.unwrap());
    }
    let lands = changes.iter().map(|&c| {
        let b = b.clone();
        tokio::spawn(async move { land::land(&b, "main", c).await.unwrap() })
    });
    for l in futures::future::join_all(lands).await {
        assert!(matches!(l.unwrap(), LandOutcome::Landed { .. }));
    }
    let chain = history(&b, root).await;
    let got = until(&mut events, Some(root), *chain.last().unwrap()).await;
    assert!(got.iter().all(|e| e.kind == EventKind::Land && !e.coalesced));
    assert_eq!(landed(&got), chain, "every land, in landing order");
    // Each event is one publish: it ends at the commit its batch landed last.
    assert!(got.iter().all(|e| e.landed.last().copied() == e.new));
}

#[tokio::test]
async fn updates_rewinds_creations_and_deletions() {
    let (b, root) = repo().await;
    let mut events = watch(&b, None, WATCH_BUFFER).await;
    let c1 = commit(&b, Some(root), vec![write("a", "1\n")]).await.unwrap();
    let c2 = commit(&b, Some(c1), vec![write("a", "2\n")]).await.unwrap();
    // A fast-forward by UpdateRef: both commits, kind update.
    b.refs.cas_ref("main", Some(root), Some(c2)).await.unwrap();
    let e = next(&mut events).await;
    assert_eq!(
        e,
        RefEvent {
            old: Some(root),
            new: Some(c2),
            landed: vec![c1, c2],
            kind: EventKind::Update,
            coalesced: false
        }
    );
    // A rewind and a move to unrelated history: force, nothing landed.
    b.refs.cas_ref("main", Some(c2), Some(c1)).await.unwrap();
    let other = commit(&b, None, vec![write("o", "o\n")]).await.unwrap();
    b.refs.cas_ref("main", Some(c1), Some(other)).await.unwrap();
    for (old, new) in [(c2, c1), (c1, other)] {
        let e = next(&mut events).await;
        assert_eq!((e.old, e.new, e.kind), (Some(old), Some(new), EventKind::Force));
        assert!(e.landed.is_empty());
    }
    // Deleted, then created again: the creation lists the whole history.
    b.refs.cas_ref("main", Some(other), None).await.unwrap();
    b.refs.cas_ref("main", None, Some(c2)).await.unwrap();
    let e = next(&mut events).await;
    assert_eq!((e.old, e.new, e.kind), (Some(other), None, EventKind::Force));
    let e = next(&mut events).await;
    assert_eq!((e.old, e.new, e.kind), (None, Some(c2), EventKind::Update));
    assert_eq!(e.landed, vec![root, c1, c2]);
}

#[tokio::test]
async fn catch_up_from_an_old_commit_then_live() {
    let (b, root) = repo().await;
    let mut tips = vec![root];
    for i in 0..4 {
        tips.push(land_change(&b, *tips.last().unwrap(), &format!("c{i}")).await);
    }
    let mut events = watch(&b, Some(tips[1]), WATCH_BUFFER).await;
    let e = next(&mut events).await;
    assert_eq!((e.old, e.new), (Some(tips[1]), Some(tips[4])));
    assert_eq!(
        (e.kind, e.coalesced, e.landed.clone()),
        (EventKind::Update, true, tips[2..].to_vec())
    );
    let live = land_change(&b, tips[4], "live").await;
    let e = next(&mut events).await;
    assert_eq!(
        (e.old, e.new, e.kind, e.coalesced),
        (Some(tips[4]), Some(live), EventKind::Land, false)
    );

    // From the tip itself: no catch-up event, straight to live events.
    let mut events = watch(&b, Some(live), WATCH_BUFFER).await;
    let after = land_change(&b, live, "after").await;
    let e = next(&mut events).await;
    assert_eq!((e.old, e.new, e.kind), (Some(live), Some(after), EventKind::Land));

    // From a commit the ref does not contain: one force event.
    let side = commit(&b, Some(root), vec![write("side", "s\n")]).await.unwrap();
    let mut events = watch(&b, Some(side), WATCH_BUFFER).await;
    let e = next(&mut events).await;
    assert_eq!(
        (e.old, e.new, e.kind, e.coalesced),
        (Some(side), Some(after), EventKind::Force, true)
    );
    assert!(e.landed.is_empty());

    // Unknown commits and bad names are refused.
    let unknown = ObjectId([9; 32]);
    let r = watch_ref(&b, None, "main", Some(unknown), WatchOptions::default(), None).await;
    assert!(matches!(r, Err(OpError::NotFound(_))));
    let r = watch_ref(&b, None, "bad name", None, WatchOptions::default(), None).await;
    assert!(r.is_err());
}

#[tokio::test]
async fn long_catch_ups_are_split_into_bounded_events() {
    let (b, root) = repo().await;
    let mut tip = root;
    let mut all = Vec::new();
    // Recorded without landing: a long first-parent chain.
    for i in 0..(MAX_EVENT_COMMITS + 10) {
        tip = commit(&b, Some(tip), vec![write("n", &format!("{i}\n"))]).await.unwrap();
        all.push(tip);
    }
    b.refs.cas_ref("main", Some(root), Some(tip)).await.unwrap();
    let mut events = watch(&b, Some(root), WATCH_BUFFER).await;
    let got = until(&mut events, Some(root), tip).await;
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].landed.len(), MAX_EVENT_COMMITS);
    assert_eq!(landed(&got), all);
}

#[tokio::test]
async fn a_slow_watcher_gets_coalesced_events() {
    let (b, root) = repo().await;
    // A ring of 4 events; the watcher pulls nothing while 30 lands publish.
    let mut events = watch(&b, None, 4).await;
    let mut tip = root;
    let mut tips = Vec::new();
    for i in 0..30 {
        tip = land_change(&b, tip, &format!("s{i}")).await;
        tips.push(tip);
    }
    // A new watcher waits until the hub has published up to the tip.
    let _caught_up = watch(&b, None, 4).await;
    let got = until(&mut events, Some(root), tip).await;
    assert_eq!(landed(&got), tips, "nothing lost");
    assert!(got[0].coalesced && got[0].landed.len() >= 26, "{:?}", got[0]);
    assert!(got.len() <= 5, "at most the ring after the coalesced event: {}", got.len());
}

/// A ref store whose moves are announced only while `announce` is set, on a
/// feed the test can also send to.
struct Quiet {
    inner: Arc<dyn RefStore>,
    feed: RefFeed,
    announce: AtomicBool,
}

#[async_trait::async_trait]
impl RefStore for Quiet {
    async fn get_ref(&self, name: &str) -> ant_store::Result<Option<ObjectId>> {
        self.inner.get_ref(name).await
    }

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ObjectId>,
        new: Option<ObjectId>,
    ) -> ant_store::Result<CasOutcome> {
        let out = self.inner.cas_ref(name, expected, new).await?;
        if out == CasOutcome::Updated && self.announce.load(Ordering::SeqCst) {
            self.feed.moved(RefMove {
                name: name.into(),
                old: expected,
                new,
                cause: ant_store::feed::move_cause(),
                nonce: ant_store::feed::nonce(),
            });
        }
        Ok(out)
    }

    async fn list_refs(&self, prefix: &str) -> ant_store::Result<Vec<(String, ObjectId)>> {
        self.inner.list_refs(prefix).await
    }

    fn watch_refs(&self) -> Option<tokio::sync::broadcast::Receiver<RefNotice>> {
        Some(self.feed.subscribe())
    }
}

async fn quiet_repo() -> (Backend, Arc<Quiet>, ObjectId) {
    let (mut b, root) = repo().await;
    let quiet = Arc::new(Quiet {
        inner: b.refs.clone(),
        feed: RefFeed::default(),
        announce: AtomicBool::new(true),
    });
    b.refs = quiet.clone();
    (b, quiet, root)
}

#[tokio::test]
async fn missed_notices_are_filled_from_the_ref() {
    let (b, quiet, root) = quiet_repo().await;
    let mut events = watch(&b, None, WATCH_BUFFER).await;
    // Three silent lands, then a resync (a listener reconnect).
    quiet.announce.store(false, Ordering::SeqCst);
    let mut tip = root;
    let mut tips = Vec::new();
    for i in 0..3 {
        tip = land_change(&b, tip, &format!("q{i}")).await;
        tips.push(tip);
    }
    quiet.feed.send(RefNotice::Resync);
    let e = next(&mut events).await;
    assert_eq!(
        (e.old, e.new, e.coalesced, e.kind),
        (Some(root), Some(tip), true, EventKind::Update)
    );
    assert_eq!(e.landed, tips);

    // A silent land followed by an announced one: the announced notice does
    // not start at the hub's tip, so the gap is filled from the ref.
    let silent = land_change(&b, tip, "silent").await;
    quiet.announce.store(true, Ordering::SeqCst);
    let loud = land_change(&b, silent, "loud").await;
    let e = next(&mut events).await;
    assert_eq!((e.old, e.new, e.coalesced), (Some(tip), Some(loud), true));
    assert_eq!(e.landed, vec![silent, loud]);

    // Duplicates (a relayed copy of a notice) and stale notices are skipped.
    let dup = RefMove {
        name: "main".into(),
        old: Some(silent),
        new: Some(loud),
        cause: MoveCause::Land,
        nonce: 42,
    };
    quiet.feed.send(RefNotice::Moved(Arc::new(dup.clone())));
    quiet.feed.send(RefNotice::Moved(Arc::new(dup)));
    let last = land_change(&b, loud, "last").await;
    let e = next(&mut events).await;
    assert_eq!(
        (e.old, e.new, e.coalesced, e.kind),
        (Some(loud), Some(last), false, EventKind::Land)
    );
}

#[tokio::test]
async fn hubs_are_shared_and_stop_with_their_last_watcher() {
    let (b, root) = repo().await;
    let a = watch(&b, None, WATCH_BUFFER).await;
    let mut c = watch(&b, Some(root), WATCH_BUFFER).await;
    let key = HubKey { refs: Arc::as_ptr(&b.refs).cast::<()>() as usize, name: "main".into() };
    let live = || crate::lock(&HUBS).get(&key).is_some_and(|w| w.strong_count() > 0);
    assert!(live());
    drop(a);
    let tip = land_change(&b, root, "x").await;
    assert_eq!(next(&mut c).await.new, Some(tip));
    drop(c);
    assert!(!live());
}
