//! Several server replicas sharing one store: K backends (separate land
//! queues, and for Postgres separate pools and caches, as in separate
//! processes) × M concurrent lands each on one ref. Every land must succeed
//! with a linear history of exactly K×M new commits, and the ref lock must
//! take turns so well that no CAS ever mismatches.
//!
//! The Postgres variant runs with `--run-ignored all`.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ant_core::EntryMode;
use ant_server::ops::changeset::{ChangesetRequest, create_changeset};
use ant_server::ops::land::{LandOutcome, land};
use ant_store::memory::{MemoryMetaStore, MemoryObjectStore, MemoryRefStore};
use ant_store::{Backend, ObjectStoreExt};
use common::pg::{self, CountingRefs, RefCounts};
use common::*;

/// K replicas × M concurrent lands; returns CAS mismatches seen.
async fn replicas_land(replicas: Vec<Backend>, lands_per_replica: usize) -> usize {
    let counts = Arc::new(RefCounts::default());
    let replicas: Vec<Backend> =
        replicas.into_iter().map(|b| CountingRefs::wrap(b, &counts, None)).collect();
    let first = &replicas[0];
    let mut seed = Model::new();
    seed.insert("README".into(), (EntryMode::File, b"seed\n".to_vec()));
    let c0 = base_commit(first, store_model(first, &seed).await).await;
    assert_eq!(
        land(first, "main", c0).await.unwrap(),
        LandOutcome::Landed { new_tip: c0, rebased: false }
    );
    counts.cas_mismatches.store(0, Ordering::SeqCst);
    counts.publish_mismatches.store(0, Ordering::SeqCst);

    let mut tasks = Vec::new();
    for (r, b) in replicas.iter().enumerate() {
        for i in 0..lands_per_replica {
            let b = b.clone();
            tasks.push(tokio::spawn(async move {
                // Every change starts from the seed, so all but the first
                // land of the batch rebase server-side.
                let path = format!("r{r}/f{i}");
                let req = ChangesetRequest {
                    base: Some(c0),
                    ops: vec![write(&path, format!("{r}/{i}\n"))],
                    author: sig(),
                    committer: None,
                    message: format!("replica {r} change {i}\n"),
                };
                let commit = create_changeset(&b, req).await.unwrap();
                let out = land(&b, "main", commit).await.unwrap();
                assert!(matches!(out, LandOutcome::Landed { .. }), "{path}: {out:?}");
                path
            }));
        }
    }
    let mut paths = Vec::new();
    for t in tasks {
        paths.push(t.await.unwrap());
    }

    // Linear history of exactly K×M commits on top of the seed.
    let tip = first.refs.get_ref("main").await.unwrap().unwrap();
    let mut cur = tip;
    let mut count = 0;
    while cur != c0 {
        let commit = first.objects.read_commit(&cur).await.unwrap();
        assert_eq!(commit.parents.len(), 1, "history must stay linear");
        cur = commit.parents[0];
        count += 1;
        assert!(count <= paths.len(), "seed not reached within {} commits", paths.len());
    }
    assert_eq!(count, paths.len(), "one commit per land");

    // Every change is in the final tree.
    let tree = first.objects.read_commit(&tip).await.unwrap().tree;
    let files: BTreeMap<_, _> = contents(first, tree).await;
    for p in &paths {
        assert!(files.contains_key(p), "{p} missing at the tip");
    }
    assert_eq!(files.len(), paths.len() + 1);
    counts.mismatches()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn memory_replicas_take_turns() {
    let objects = Arc::new(MemoryObjectStore::default());
    let refs = Arc::new(MemoryRefStore::default());
    let meta = Arc::new(MemoryMetaStore::default());
    // Distinct ref-store wrappers give each replica its own land queue.
    let replicas = (0..4).map(|_| Backend {
        objects: objects.clone(),
        refs: refs.clone(),
        meta: meta.clone(),
    });
    let mismatches = replicas_land(replicas.collect(), 24).await;
    assert_eq!(mismatches, 0, "the ref lock must serialize replicas");
}

async fn pg_replicas(n: usize) -> Vec<Backend> {
    let (_, stores) = pg::replica_stores("t_replicas", n).await;
    stores.into_iter().map(pg::backend).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs postgres: docker compose up -d"]
async fn pg_replicas_take_turns() {
    let mismatches = replicas_land(pg_replicas(4).await, 32).await;
    assert_eq!(mismatches, 0, "the ref lock must serialize replicas");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs postgres: docker compose up -d"]
async fn pg_eight_replicas_many_lands() {
    let mismatches = replicas_land(pg_replicas(8).await, 64).await;
    assert_eq!(mismatches, 0, "the ref lock must serialize replicas");
}
