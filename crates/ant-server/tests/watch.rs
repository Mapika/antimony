//! Ref event streams (`ops::watch`) against in-memory and Postgres stores:
//! lands (including group-commit batches), UpdateRef and force moves,
//! catch-up, replicas landing concurrently while another replica is
//! watched, listener reconnects and slow watchers. Every stream must chain
//! (each event's old tip is the previous event's new tip) and list every
//! landed commit exactly once, in order.
//!
//! The Postgres variants run with `--run-ignored all`.

mod common;

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ant_core::ObjectId;
use ant_server::ops::OpResult;
use ant_server::ops::changeset::{ChangesetRequest, create_changeset};
use ant_server::ops::land::{LandOutcome, land};
use ant_server::ops::watch::{EventKind, RefEvent, WatchOptions, watch_ref};
use ant_store::memory::{MemoryMetaStore, MemoryObjectStore, MemoryRefStore};
use ant_store::postgres::{PgStore, listener_application_name};
use ant_store::{Backend, ObjectStoreExt};
use common::pg::{self, CountingRefs, RefCounts};
use common::{sig, write};
use futures::{Stream, StreamExt};

const DEADLINE: Duration = Duration::from_secs(60);

type Events = Pin<Box<dyn Stream<Item = OpResult<RefEvent>> + Send>>;

async fn watch(b: &Backend, from: Option<ObjectId>, buffer: usize) -> Events {
    Box::pin(watch_ref(b, None, "main", from, WatchOptions { buffer }, None).await.unwrap())
}

async fn next(events: &mut Events) -> RefEvent {
    tokio::time::timeout(DEADLINE, events.next()).await.expect("no event in time").unwrap().unwrap()
}

/// Events until one moves the ref to `end`, checking that they chain from
/// `start`.
async fn until(events: &mut Events, start: ObjectId, end: ObjectId) -> Vec<RefEvent> {
    let mut out = Vec::new();
    let mut at = Some(start);
    while at != Some(end) {
        let e = next(events).await;
        assert_eq!(e.old, at, "events must chain: {:?}", out.last());
        at = e.new;
        out.push(e);
    }
    out
}

fn landed(events: &[RefEvent]) -> Vec<ObjectId> {
    events.iter().flat_map(|e| e.landed.iter().copied()).collect()
}

async fn change(b: &Backend, base: Option<ObjectId>, path: &str) -> ObjectId {
    let req = ChangesetRequest {
        base,
        ops: vec![write(path, format!("{path}\n"))],
        author: sig(),
        committer: None,
        message: format!("{path}\n"),
    };
    create_changeset(b, req).await.unwrap()
}

async fn land_tip(b: &Backend, commit: ObjectId) -> ObjectId {
    match land(b, "main", commit).await.unwrap() {
        LandOutcome::Landed { new_tip, .. } => new_tip,
        other => panic!("{other:?}"),
    }
}

/// `main`'s first-parent commits after `root`, oldest first.
async fn history(b: &Backend, root: ObjectId) -> Vec<ObjectId> {
    let mut cur = b.refs.get_ref("main").await.unwrap().unwrap();
    let mut out = Vec::new();
    while cur != root {
        out.push(cur);
        cur = b.objects.read_commit(&cur).await.unwrap().parents[0];
    }
    out.reverse();
    out
}

async fn seeded(b: &Backend) -> ObjectId {
    let root = change(b, None, "README").await;
    land_tip(b, root).await
}

/// Lands one by one and in concurrent batches, an UpdateRef fast-forward, a
/// force move, then a second watcher catching up from an old commit.
async fn lands_updates_force_and_catch_up(b: Backend) {
    let root = seeded(&b).await;
    let mut events = watch(&b, None, 256).await;
    let mut tip = root;
    for i in 0..3 {
        tip = land_tip(&b, change(&b, Some(tip), &format!("one/{i}")).await).await;
    }
    let mut changes = Vec::new();
    for i in 0..40 {
        changes.push(change(&b, Some(root), &format!("batch/{i}")).await);
    }
    let lands = changes.into_iter().map(|c| {
        let b = b.clone();
        tokio::spawn(async move { land_tip(&b, c).await })
    });
    for l in futures::future::join_all(lands).await {
        l.unwrap();
    }
    let chain = history(&b, root).await;
    let tip = *chain.last().unwrap();
    let got = until(&mut events, root, tip).await;
    assert_eq!(landed(&got), chain, "every land once, in landing order");
    // Each one publish, unless a resync (a listener connecting) coalesced.
    assert!(got.iter().all(|e| e.kind == EventKind::Land || e.coalesced), "{got:?}");

    // UpdateRef: a fast-forward of two commits, then a rewind.
    let c1 = change(&b, Some(tip), "u1").await;
    let c2 = change(&b, Some(c1), "u2").await;
    b.refs.cas_ref("main", Some(tip), Some(c2)).await.unwrap();
    let e = next(&mut events).await;
    assert_eq!(
        (e.old, e.new, e.kind, e.landed.clone()),
        (Some(tip), Some(c2), EventKind::Update, vec![c1, c2])
    );
    b.refs.cas_ref("main", Some(c2), Some(root)).await.unwrap();
    let e = next(&mut events).await;
    assert_eq!((e.old, e.new, e.kind), (Some(c2), Some(root), EventKind::Force));
    assert!(e.landed.is_empty());
    b.refs.cas_ref("main", Some(root), Some(c2)).await.unwrap();

    // Catch-up from an old commit, then live.
    let mut late = watch(&b, Some(chain[2]), 256).await;
    let e = next(&mut late).await;
    assert!(e.coalesced);
    assert_eq!((e.old, e.new), (Some(chain[2]), Some(c2)));
    let mut expected = chain[3..].to_vec();
    expected.extend([c1, c2]);
    assert_eq!(e.landed, expected);
    let live = land_tip(&b, change(&b, Some(c2), "live").await).await;
    // The first watcher also sees the rewind undone.
    for (events, from) in [(&mut events, root), (&mut late, c2)] {
        let got = until(events, from, live).await;
        assert_eq!(got.last().unwrap().kind, EventKind::Land);
    }
}

/// Lands through `replicas[1..]` while `replicas[0]` is watched: no gaps,
/// nothing twice, landing order.
async fn replicas_land_while_watched(replicas: Vec<Backend>, lands_per_replica: usize) {
    let counts = Arc::new(RefCounts::default());
    let replicas: Vec<Backend> =
        replicas.into_iter().map(|b| CountingRefs::wrap(b, &counts, None)).collect();
    let root = seeded(&replicas[0]).await;
    let mut events = watch(&replicas[0], None, 256).await;
    let mut tasks = Vec::new();
    for (r, b) in replicas.iter().enumerate().skip(1) {
        for i in 0..lands_per_replica {
            let b = b.clone();
            tasks.push(tokio::spawn(async move {
                let c = change(&b, Some(root), &format!("r{r}/f{i}")).await;
                land_tip(&b, c).await
            }));
        }
    }
    for t in tasks {
        t.await.unwrap();
    }
    let chain = history(&replicas[0], root).await;
    assert_eq!(chain.len(), (replicas.len() - 1) * lands_per_replica);
    let got = until(&mut events, root, *chain.last().unwrap()).await;
    assert_eq!(landed(&got), chain, "no gaps, nothing twice");
}

/// A watcher that pulls nothing while 30 lands publish gets them coalesced.
async fn slow_watcher(b: Backend) {
    let root = seeded(&b).await;
    let mut events = watch(&b, None, 4).await;
    let mut tip = root;
    for i in 0..30 {
        tip = land_tip(&b, change(&b, Some(tip), &format!("slow/{i}")).await).await;
    }
    // A new watcher waits until the hub has published up to the tip.
    let _caught_up = watch(&b, None, 4).await;
    let got = until(&mut events, root, tip).await;
    assert_eq!(landed(&got), history(&b, root).await);
    assert!(got[0].coalesced, "{:?}", got[0]);
    assert!(got.len() <= 5, "{} events", got.len());
}

fn memory_replicas(n: usize) -> Vec<Backend> {
    let objects = Arc::new(MemoryObjectStore::default());
    let refs = Arc::new(MemoryRefStore::default());
    let meta = Arc::new(MemoryMetaStore::default());
    (0..n)
        .map(|_| Backend { objects: objects.clone(), refs: refs.clone(), meta: meta.clone() })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_lands_updates_force_and_catch_up() {
    lands_updates_force_and_catch_up(ant_store::testing::test_backend("watch")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn memory_replicas_land_while_watched() {
    replicas_land_while_watched(memory_replicas(4), 32).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_slow_watcher() {
    slow_watcher(Backend::in_memory()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_lands_updates_force_and_catch_up() {
    lands_updates_force_and_catch_up(ant_store::testing::local_backend("watch")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_slow_watcher() {
    slow_watcher(ant_store::testing::local_backend("watch_slow")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs postgres: docker compose up -d"]
async fn pg_lands_updates_force_and_catch_up() {
    lands_updates_force_and_catch_up(pg::scratch_backend("t_watch").await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs postgres: docker compose up -d"]
async fn pg_four_replicas_land_while_one_is_watched() {
    let (_, stores) = pg::replica_stores("t_watch_replicas", 4).await;
    replicas_land_while_watched(stores.into_iter().map(pg::backend).collect(), 40).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs postgres: docker compose up -d"]
async fn pg_slow_watcher() {
    slow_watcher(pg::scratch_backend("t_watch_slow").await).await;
}

async fn listener_pids(schema: &str) -> Vec<i32> {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&pg::pg_url()).await.unwrap();
    sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE application_name = $1")
        .bind(listener_application_name(schema))
        .fetch_all(&mut conn)
        .await
        .unwrap()
}

/// A ref move nobody announced (raw SQL) is found when the listener
/// reconnects: one coalesced event.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs postgres: docker compose up -d"]
async fn pg_listener_reconnect_fills_the_gap() {
    let (schema, stores) = pg::replica_stores("t_watch_reconnect", 2).await;
    let stores: Vec<Arc<PgStore>> = stores;
    let (a, b) = (pg::backend(stores[0].clone()), pg::backend(stores[1].clone()));
    let root = seeded(&b).await;
    let mut events = watch(&a, None, 256).await;
    let started = std::time::Instant::now();
    let pids = loop {
        let pids = listener_pids(&schema).await;
        if !pids.is_empty() {
            break pids;
        }
        assert!(started.elapsed() < DEADLINE, "listener never connected");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // Recorded commits, moved to by a statement that notifies nobody.
    let c1 = change(&b, Some(root), "silent/1").await;
    let c2 = change(&b, Some(c1), "silent/2").await;
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&pg::pg_url()).await.unwrap();
    let stmt = format!("UPDATE \"{schema}\".refs SET target = $1 WHERE name = 'main'");
    sqlx::query(sqlx::AssertSqlSafe(stmt))
        .bind(c2.as_bytes().as_slice())
        .execute(&mut conn)
        .await
        .unwrap();
    for pid in pids {
        sqlx::query("SELECT pg_terminate_backend($1)").bind(pid).execute(&mut conn).await.unwrap();
    }
    let e = next(&mut events).await;
    assert_eq!(
        (e.old, e.new, e.coalesced, e.landed.clone()),
        (Some(root), Some(c2), true, vec![c1, c2])
    );
    // Live again afterwards.
    let tip = land_tip(&b, change(&b, Some(c2), "after").await).await;
    let e = next(&mut events).await;
    assert_eq!((e.old, e.new, e.kind, e.coalesced), (Some(c2), Some(tip), EventKind::Land, false));
}

/// The value of an unlabelled sample in a metrics rendering.
fn sample(text: &str, name: &str) -> f64 {
    text.lines()
        .find_map(|l| l.strip_prefix(name).and_then(|v| v.strip_prefix(' ')))
        .unwrap_or_else(|| panic!("no {name} in\n{text}"))
        .trim()
        .parse()
        .unwrap()
}

/// Over gRPC: watchers are counted while open, events and coalesced events
/// as they are sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rpc_stream_and_metrics() {
    use ant_proto::v1 as pb;
    let metrics = ant_server::metrics::Metrics::new();
    let b = Backend::in_memory();
    let acl = ant_server::acl::Acl::open();
    let server =
        ant_server::serve::spawn_local_with_metrics(b.clone(), acl, metrics.clone()).await.unwrap();
    let client = pb::ant_client::AntClient::connect(server.grpc_url()).await.unwrap();
    let land_rpc = |c: ObjectId| {
        let mut client = client.clone();
        async move {
            let r = client.land(pb::LandRequest { ref_name: "main".into(), commit: c.0.to_vec() });
            match r.await.unwrap().into_inner().outcome {
                Some(pb::land_response::Outcome::Landed(l)) => {
                    ObjectId::from_bytes(&l.new_tip).unwrap()
                }
                other => panic!("{other:?}"),
            }
        }
    };
    // Commits are created directly; lands go through the server.
    let root = land_rpc(change(&b, None, "README").await).await;
    let t1 = land_rpc(change(&b, Some(root), "a").await).await;
    let req = pb::WatchRefRequest { name: "main".into(), from: Some(root.0.to_vec()) };
    let mut events = client.clone().watch_ref(req).await.unwrap().into_inner();
    let e = tokio::time::timeout(DEADLINE, events.message()).await.unwrap().unwrap().unwrap();
    assert_eq!((e.old_tip, e.new_tip), (Some(root.0.to_vec()), Some(t1.0.to_vec())));
    assert_eq!((e.kind, e.coalesced), (pb::RefEventKind::Update as i32, true));
    let t2 = land_rpc(change(&b, Some(t1), "b").await).await;
    let e = tokio::time::timeout(DEADLINE, events.message()).await.unwrap().unwrap().unwrap();
    assert_eq!(e.landed, vec![t2.0.to_vec()]);
    assert_eq!((e.kind, e.coalesced), (pb::RefEventKind::Land as i32, false));
    let text = metrics.render();
    assert_eq!(sample(&text, "ant_ref_watchers"), 1.0);
    assert_eq!(sample(&text, "ant_ref_watch_events_total"), 2.0);
    assert_eq!(sample(&text, "ant_ref_watch_coalesced_events_total"), 1.0);
    // The server notices the client leaving.
    drop(events);
    let started = std::time::Instant::now();
    while sample(&metrics.render(), "ant_ref_watchers") != 0.0 {
        assert!(started.elapsed() < DEADLINE, "watcher still counted");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
