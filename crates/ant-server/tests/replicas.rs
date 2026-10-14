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
use std::sync::atomic::{AtomicUsize, Ordering};

use ant_core::{EntryMode, ObjectId, RepoPath};
use ant_server::ops::changeset::{ChangesetRequest, FileOp, WriteContent, create_changeset};
use ant_server::ops::land::{LandOutcome, land};
use ant_store::memory::{MemoryMetaStore, MemoryObjectStore, MemoryRefStore};
use ant_store::postgres::{PgOptions, PgStore};
use ant_store::{Backend, CasOutcome, ObjectStoreExt, RefLock, RefLockRelease, RefStore};
use async_trait::async_trait;
use bytes::Bytes;
use common::*;

/// Forwards to a shared ref store (lock included) and counts CAS and publish
/// mismatches.
struct CountingRefs {
    inner: Arc<dyn RefStore>,
    mismatches: Arc<AtomicUsize>,
}

#[async_trait]
impl RefStore for CountingRefs {
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
        if matches!(out, CasOutcome::Mismatch { .. }) {
            self.mismatches.fetch_add(1, Ordering::Relaxed);
        }
        Ok(out)
    }

    async fn list_refs(&self, prefix: &str) -> ant_store::Result<Vec<(String, ObjectId)>> {
        self.inner.list_refs(prefix).await
    }

    async fn lock_ref(&self, name: &str) -> ant_store::Result<RefLock> {
        let lock = self.inner.lock_ref(name).await?;
        if !lock.is_held() {
            return Ok(lock);
        }
        Ok(RefLock::held(CountingLock { inner: lock, mismatches: self.mismatches.clone() }))
    }
}

/// Forwards to a held lock and counts mismatched publishes (lands move the
/// ref through the lock's publish when the store supports it).
struct CountingLock {
    inner: RefLock,
    mismatches: Arc<AtomicUsize>,
}

#[async_trait]
impl RefLockRelease for CountingLock {
    async fn release(self: Box<Self>) -> ant_store::Result<()> {
        self.inner.release().await
    }

    fn can_publish(&self) -> bool {
        self.inner.can_publish()
    }

    async fn publish(
        &mut self,
        expected: Option<ObjectId>,
        new: ObjectId,
        kind: &str,
        derived: Vec<(Vec<u8>, Bytes)>,
    ) -> ant_store::Result<CasOutcome> {
        let out = self.inner.publish(expected, new, kind, derived).await?;
        if matches!(out, CasOutcome::Mismatch { .. }) {
            self.mismatches.fetch_add(1, Ordering::Relaxed);
        }
        Ok(out)
    }
}

fn counted(b: Backend, mismatches: &Arc<AtomicUsize>) -> Backend {
    Backend { refs: Arc::new(CountingRefs { inner: b.refs, mismatches: mismatches.clone() }), ..b }
}

fn write(path: &str, content: String) -> FileOp {
    FileOp::Write {
        path: RepoPath::new(path).unwrap(),
        mode: EntryMode::File,
        content: WriteContent::Inline(Bytes::from(content)),
    }
}

/// K replicas × M concurrent lands; returns CAS mismatches seen.
async fn replicas_land(replicas: Vec<Backend>, lands_per_replica: usize) -> usize {
    let mismatches = Arc::new(AtomicUsize::new(0));
    let replicas: Vec<Backend> = replicas.into_iter().map(|b| counted(b, &mismatches)).collect();
    let first = &replicas[0];
    let mut seed = Model::new();
    seed.insert("README".into(), (EntryMode::File, b"seed\n".to_vec()));
    let c0 = base_commit(first, store_model(first, &seed).await).await;
    assert_eq!(
        land(first, "main", c0).await.unwrap(),
        LandOutcome::Landed { new_tip: c0, rebased: false }
    );
    mismatches.store(0, Ordering::Relaxed);

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
    mismatches.load(Ordering::Relaxed)
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

fn pg_url() -> String {
    std::env::var("ANT_TEST_PG_URL")
        .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into())
}

async fn pg_replicas(n: usize) -> Vec<Backend> {
    let schema = format!("t_replicas_{}", rand::random::<u64>());
    // Small pools, as replicas sharing a database must use: 8 x 64 would
    // exceed max_connections.
    let options = PgOptions {
        max_connections: 12,
        lock_connections: 2,
        // A lock wait that times out falls back to CAS, which this test
        // counts as a failure; allow for stalls of a loaded shared database.
        lock_timeout: std::time::Duration::from_secs(120),
    };
    let mut out = Vec::new();
    for i in 0..n {
        // The first replica drops the schema (all replicas are idle by then).
        let pg = Arc::new(if i == 0 {
            PgStore::connect_scratch_with(&pg_url(), &schema, &options).await.unwrap()
        } else {
            PgStore::connect_with(&pg_url(), &schema, &options).await.unwrap()
        });
        out.push(Backend { objects: pg.clone(), refs: pg.clone(), meta: pg });
    }
    out
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
