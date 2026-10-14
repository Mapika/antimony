//! Landing on Postgres through the ref lock's session: batches are written
//! with deferred durability and published (ref move + landed-as mappings) in
//! one synchronous commit on the lock's session. If that session dies (as on
//! any database crash), nothing is published and the requests are aborted.
//!
//! Runs with `--run-ignored all`.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ant_core::{EntryMode, ObjectId};
use ant_server::ops::OpError;
use ant_server::ops::changeset::{ChangesetRequest, create_changeset};
use ant_server::ops::land::{LandOutcome, land};
use ant_store::{Backend, ObjectStoreExt};
use common::pg::{self, CountingRefs, OnLock, RefCounts};
use common::*;

/// Kill the session holding the ref lock, as a database crash would.
async fn terminate_lock_session(schema: &str, name: &str) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&pg::pg_url()).await.unwrap();
    let killed: bool = sqlx::query_scalar(
        "SELECT pg_terminate_backend(pid) FROM pg_locks \
         WHERE locktype = 'advisory' AND granted AND objsubid = 1 \
           AND ((classid::bigint << 32) | objid::bigint) = hashtextextended($1, 0)",
    )
    .bind(format!("ant/{schema}/{name}"))
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert!(killed);
}

/// `n` replicas whose ref stores count CASes and publishes; the next lock
/// granted after `kill_next_lock` is set has its session terminated.
async fn replicas(
    n: usize,
    counts: &Arc<RefCounts>,
    kill_next_lock: &Arc<AtomicBool>,
) -> Vec<Backend> {
    let (schema, stores) = pg::replica_stores("t_land_pg", n).await;
    let kill = kill_next_lock.clone();
    let on_lock: OnLock = Arc::new(move |name| {
        let (kill, schema) = (kill.clone(), schema.clone());
        Box::pin(async move {
            if kill.swap(false, Ordering::SeqCst) {
                terminate_lock_session(&schema, &name).await;
            }
        })
    });
    stores
        .into_iter()
        .map(|pg| CountingRefs::wrap(pg::backend(pg), counts, Some(on_lock.clone())))
        .collect()
}

async fn change(b: &Backend, base: ObjectId, path: &str) -> ObjectId {
    let req = ChangesetRequest {
        base: Some(base),
        ops: vec![write(path, format!("{path}\n"))],
        author: sig(),
        committer: None,
        message: format!("{path}\n"),
    };
    create_changeset(b, req).await.unwrap()
}

async fn history_len(b: &Backend, root: ObjectId) -> usize {
    let mut cur = b.refs.get_ref("main").await.unwrap().unwrap();
    let mut n = 0;
    while cur != root {
        let commit = b.objects.read_commit(&cur).await.unwrap();
        assert_eq!(commit.parents.len(), 1);
        cur = commit.parents[0];
        n += 1;
    }
    n
}

async fn seed(b: &Backend) -> ObjectId {
    let mut model = Model::new();
    model.insert("README".into(), (EntryMode::File, b"seed\n".to_vec()));
    let c0 = base_commit(b, store_model(b, &model).await).await;
    assert!(matches!(land(b, "main", c0).await.unwrap(), LandOutcome::Landed { .. }));
    c0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs postgres: docker compose up -d"]
async fn pg_replicas_land_by_publishing_on_the_lock_session() {
    let counts = Arc::new(RefCounts::default());
    let replicas = replicas(4, &counts, &Arc::default()).await;
    let c0 = seed(&replicas[0]).await;
    let mut tasks = Vec::new();
    for (r, b) in replicas.iter().enumerate() {
        for i in 0..32 {
            let b = b.clone();
            tasks.push(tokio::spawn(async move {
                let commit = change(&b, c0, &format!("r{r}/f{i}")).await;
                let out = land(&b, "main", commit).await.unwrap();
                assert!(matches!(out, LandOutcome::Landed { .. }), "{out:?}");
                (commit, out)
            }));
        }
    }
    let mut landed = Vec::new();
    for t in tasks {
        landed.push(t.await.unwrap());
    }
    assert_eq!(history_len(&replicas[0], c0).await, 128);
    // Every batch went through the lock's session, and the lock serialized
    // the replicas so well that no publish mismatched.
    assert_eq!(counts.cas.load(Ordering::SeqCst), 0);
    assert!(counts.publishes.load(Ordering::SeqCst) > 0);
    assert_eq!(counts.publish_mismatches.load(Ordering::SeqCst), 0);
    // The mappings were published with the ref: retries are no-ops on any
    // replica.
    let tip = replicas[0].refs.get_ref("main").await.unwrap().unwrap();
    for (i, (commit, _)) in landed.iter().enumerate() {
        let b = &replicas[i % replicas.len()];
        assert_eq!(
            land(b, "main", *commit).await.unwrap(),
            LandOutcome::Landed { new_tip: tip, rebased: false }
        );
    }
    assert_eq!(history_len(&replicas[0], c0).await, 128);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs postgres: docker compose up -d"]
async fn pg_land_publishes_nothing_when_the_lock_session_dies() {
    let counts = Arc::new(RefCounts::default());
    let kill_next_lock = Arc::new(AtomicBool::new(false));
    let b = replicas(1, &counts, &kill_next_lock).await.remove(0);
    let c0 = seed(&b).await;
    let a = change(&b, c0, "a").await;
    let c = change(&b, c0, "c").await;
    kill_next_lock.store(true, Ordering::SeqCst);
    // The batch is replayed and written under the dead session; its publish
    // must fail rather than make anything reachable.
    let (ra, rc) = tokio::join!(land(&b, "main", a), land(&b, "main", c));
    for r in [&ra, &rc] {
        assert!(matches!(r, Err(OpError::Aborted(_))), "{r:?}");
    }
    assert_eq!(b.refs.get_ref("main").await.unwrap(), Some(c0));
    assert_eq!(history_len(&b, c0).await, 0);
    // Retrying lands both (a new lock, a new session).
    for commit in [a, c] {
        assert!(matches!(land(&b, "main", commit).await.unwrap(), LandOutcome::Landed { .. }));
    }
    assert_eq!(history_len(&b, c0).await, 2);
    let tip = b.refs.get_ref("main").await.unwrap().unwrap();
    let files = contents(&b, b.objects.read_commit(&tip).await.unwrap().tree).await;
    assert!(files.contains_key("a") && files.contains_key("c"));
    assert_eq!(counts.cas.load(Ordering::SeqCst), 0);
}
