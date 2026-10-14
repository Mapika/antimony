//! Postgres scratch stores (tests run with `--run-ignored all`) and a ref
//! store wrapper that counts CASes and lock publishes.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ant_core::ObjectId;
use ant_store::postgres::{PgOptions, PgStore};
use ant_store::{Backend, CasOutcome, RefLock, RefLockRelease, RefStore};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;

pub use ant_store::testing::pg_url;

/// A fresh schema name `<prefix>_<random>`.
pub fn scratch_schema(prefix: &str) -> String {
    format!("{prefix}_{}", rand::random::<u64>())
}

/// Every store of a backend on one Postgres store.
pub fn backend(pg: Arc<PgStore>) -> Backend {
    Backend { objects: pg.clone(), refs: pg.clone(), meta: pg }
}

/// A backend on a fresh schema that is dropped with the store.
pub async fn scratch_backend(prefix: &str) -> Backend {
    let pg = PgStore::connect_scratch(&pg_url(), &scratch_schema(prefix))
        .await
        .expect("connect (is `docker compose up -d` running?)");
    backend(Arc::new(pg))
}

/// `n` stores on one fresh schema, opened as separate server replicas would
/// (own pools and caches). Small pools, as replicas sharing a database must
/// use: 8 x 64 would exceed max_connections. The first store drops the
/// schema (all replicas are idle by then).
pub async fn replica_stores(prefix: &str, n: usize) -> (String, Vec<Arc<PgStore>>) {
    let schema = scratch_schema(prefix);
    let options = PgOptions {
        max_connections: 12,
        lock_connections: 2,
        // A lock wait that times out falls back to CAS, which the replica
        // tests count as a failure; allow for stalls of a loaded shared
        // database.
        lock_timeout: std::time::Duration::from_secs(120),
        ..PgOptions::default()
    };
    let mut out = Vec::new();
    for i in 0..n {
        let pg = if i == 0 {
            PgStore::connect_scratch_with(&pg_url(), &schema, &options).await.unwrap()
        } else {
            PgStore::connect_with(&pg_url(), &schema, &options).await.unwrap()
        };
        out.push(Arc::new(pg));
    }
    (schema, out)
}

/// What [`CountingRefs`] saw.
#[derive(Default)]
pub struct RefCounts {
    pub cas: AtomicUsize,
    pub cas_mismatches: AtomicUsize,
    pub publishes: AtomicUsize,
    pub publish_mismatches: AtomicUsize,
}

impl RefCounts {
    /// Mismatched CASes plus mismatched publishes.
    pub fn mismatches(&self) -> usize {
        self.cas_mismatches.load(Ordering::SeqCst) + self.publish_mismatches.load(Ordering::SeqCst)
    }
}

/// Runs after a held ref lock is granted, with the ref name.
pub type OnLock = Arc<dyn Fn(String) -> BoxFuture<'static, ()> + Send + Sync>;

/// Forwards to a ref store (lock included), counting CASes, lock publishes
/// and their mismatches.
pub struct CountingRefs {
    pub inner: Arc<dyn RefStore>,
    pub counts: Arc<RefCounts>,
    pub on_lock: Option<OnLock>,
}

impl CountingRefs {
    /// `b` with its ref store wrapped (a distinct wrapper per backend also
    /// gives each its own land queue).
    pub fn wrap(b: Backend, counts: &Arc<RefCounts>, on_lock: Option<OnLock>) -> Backend {
        let refs = Arc::new(CountingRefs { inner: b.refs, counts: counts.clone(), on_lock });
        Backend { refs, ..b }
    }
}

fn count_mismatch(out: &CasOutcome, counter: &AtomicUsize) {
    if matches!(out, CasOutcome::Mismatch { .. }) {
        counter.fetch_add(1, Ordering::SeqCst);
    }
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
        self.counts.cas.fetch_add(1, Ordering::SeqCst);
        let out = self.inner.cas_ref(name, expected, new).await?;
        count_mismatch(&out, &self.counts.cas_mismatches);
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
        if let Some(on_lock) = &self.on_lock {
            on_lock(name.to_owned()).await;
        }
        Ok(RefLock::held(CountingLock { inner: lock, counts: self.counts.clone() }))
    }
}

/// A held lock, forwarding publishes and counting their outcomes.
struct CountingLock {
    inner: RefLock,
    counts: Arc<RefCounts>,
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
        count_mismatch(&out, &self.counts.publish_mismatches);
        Ok(out)
    }
}
