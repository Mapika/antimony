//! Landing on Postgres through the ref lock's session: batches are written
//! with deferred durability and published (ref move + landed-as mappings) in
//! one synchronous commit on the lock's session. If that session dies (as on
//! any database crash), nothing is published and the requests are aborted.
//!
//! Runs with `--run-ignored all`.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ant_core::{EntryMode, ObjectId, RepoPath};
use ant_server::ops::OpError;
use ant_server::ops::changeset::{ChangesetRequest, FileOp, WriteContent, create_changeset};
use ant_server::ops::land::{LandOutcome, land};
use ant_store::postgres::{PgOptions, PgStore};
use ant_store::{Backend, CasOutcome, ObjectStoreExt, RefLock, RefLockRelease, RefStore};
use async_trait::async_trait;
use bytes::Bytes;
use common::*;

fn pg_url() -> String {
    std::env::var("ANT_TEST_PG_URL")
        .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into())
}

#[derive(Default)]
struct Counts {
    cas: AtomicUsize,
    publishes: AtomicUsize,
    mismatches: AtomicUsize,
    /// Terminate the next lock's session right after it is granted.
    kill_next_lock: AtomicBool,
}

/// Forwards to Postgres, counting plain CASes and lock publishes.
struct Observed {
    inner: Arc<PgStore>,
    schema: String,
    counts: Arc<Counts>,
}

/// A granted lock, forwarding publishes and counting their outcomes.
struct CountingLock {
    inner: RefLock,
    counts: Arc<Counts>,
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
        self.counts.publishes.fetch_add(1, Ordering::SeqCst);
        let out = self.inner.publish(expected, new, kind, derived).await?;
        if matches!(out, CasOutcome::Mismatch { .. }) {
            self.counts.mismatches.fetch_add(1, Ordering::SeqCst);
        }
        Ok(out)
    }
}

#[async_trait]
impl RefStore for Observed {
    async fn get_ref(&self, name: &str) -> ant_store::Result<Option<ObjectId>> {
        self.inner.get_ref(name).await
    }

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ObjectId>,
        new: Option<ObjectId>,
    ) -> ant_store::Result<CasOutcome> {
        self.counts.cas.fetch_add(1, Ordering::SeqCst);
        self.inner.cas_ref(name, expected, new).await
    }

    async fn list_refs(&self, prefix: &str) -> ant_store::Result<Vec<(String, ObjectId)>> {
        self.inner.list_refs(prefix).await
    }

    async fn lock_ref(&self, name: &str) -> ant_store::Result<RefLock> {
        let lock = self.inner.lock_ref(name).await?;
        if self.counts.kill_next_lock.swap(false, Ordering::SeqCst) {
            terminate_lock_session(&self.schema, name).await;
        }
        Ok(RefLock::held(CountingLock { inner: lock, counts: self.counts.clone() }))
    }
}

/// Kill the session holding the ref lock, as a database crash would.
async fn terminate_lock_session(schema: &str, name: &str) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&pg_url()).await.unwrap();
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

async fn replicas(n: usize, counts: &Arc<Counts>) -> Vec<Backend> {
    let schema = format!("t_land_pg_{}", rand::random::<u64>());
    let options = PgOptions {
        max_connections: 12,
        lock_connections: 2,
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
        let refs = Arc::new(Observed {
            inner: pg.clone(),
            schema: schema.clone(),
            counts: counts.clone(),
        });
        out.push(Backend { objects: pg.clone(), refs, meta: pg });
    }
    out
}

async fn change(b: &Backend, base: ObjectId, path: &str) -> ObjectId {
    let req = ChangesetRequest {
        base: Some(base),
        ops: vec![FileOp::Write {
            path: RepoPath::new(path).unwrap(),
            mode: EntryMode::File,
            content: WriteContent::Inline(Bytes::from(format!("{path}\n"))),
        }],
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
    let counts = Arc::new(Counts::default());
    let replicas = replicas(4, &counts).await;
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
    assert_eq!(counts.mismatches.load(Ordering::SeqCst), 0);
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
    let counts = Arc::new(Counts::default());
    let b = replicas(1, &counts).await.remove(0);
    let c0 = seed(&b).await;
    let a = change(&b, c0, "a").await;
    let c = change(&b, c0, "c").await;
    counts.kill_next_lock.store(true, Ordering::SeqCst);
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
