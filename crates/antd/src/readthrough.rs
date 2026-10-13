//! Read-through cache: local store first, misses fetched from upstream in
//! one batch, verified, and written back locally.
//!
//! TASK (antd-cache): implement everything marked `todo!()`.
//!
//! - `get_many`: one `local.get_many(ids)`; collect misses (deduplicated);
//!   one `upstream.get_many(misses)`; `verify` each fetched object against
//!   its requested id (mismatch → `StoreError::Backend`); one
//!   `local.put_many(fetched)`; return merged results in input order.
//! - `put_many`: write to upstream first, then local (write-through).
//! - `has_many`: local first, upstream for misses.
//! - In-flight dedup: concurrent `get_many` calls that miss on the same id
//!   should trigger only ONE upstream fetch for it. Implement with a
//!   `parking_lot::Mutex<HashMap<ObjectId, Shared future / watch channel>>`
//!   or similar. (Tested below with a counting upstream.)

use std::collections::HashMap;
use std::sync::Arc;

use ant_core::{EncodedObject, ObjectId};
use ant_store::{ObjectStore, Result, StoreError};
use async_trait::async_trait;
use futures::future::{BoxFuture, FutureExt, Shared};
use parking_lot::Mutex;

type Fetched = std::result::Result<Arc<HashMap<ObjectId, EncodedObject>>, Arc<String>>;
type InFlight = Shared<BoxFuture<'static, Fetched>>;

pub struct ReadThrough {
    local: Arc<dyn ObjectStore>,
    upstream: Arc<dyn ObjectStore>,
    inflight: Arc<Mutex<HashMap<ObjectId, InFlight>>>,
}

impl ReadThrough {
    pub fn new(local: Arc<dyn ObjectStore>, upstream: Arc<dyn ObjectStore>) -> Self {
        Self { local, upstream, inflight: Default::default() }
    }

    /// Fetch `ids` from upstream, verify, and fill the local store. Re-checks
    /// local first: a previous fetch may have landed after our local miss.
    async fn fetch(
        local: Arc<dyn ObjectStore>,
        upstream: Arc<dyn ObjectStore>,
        ids: Vec<ObjectId>,
    ) -> Result<HashMap<ObjectId, EncodedObject>> {
        let mut out = HashMap::with_capacity(ids.len());
        let mut missing = Vec::new();
        for (id, o) in ids.iter().zip(local.get_many(&ids).await?) {
            match o {
                Some(o) => {
                    out.insert(*id, o);
                }
                None => missing.push(*id),
            }
        }
        if missing.is_empty() {
            return Ok(out);
        }
        let mut fetched = Vec::new();
        for (id, o) in missing.iter().zip(upstream.get_many(&missing).await?) {
            if let Some(o) = o {
                o.verify(id).map_err(StoreError::backend)?;
                fetched.push(o.clone());
                out.insert(*id, o);
            }
        }
        local.put_many(&fetched).await?;
        Ok(out)
    }
}

#[async_trait]
impl ObjectStore for ReadThrough {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        let mut out = self.local.get_many(ids).await?;
        let mut misses: Vec<ObjectId> =
            ids.iter().zip(&out).filter(|(_, o)| o.is_none()).map(|(id, _)| *id).collect();
        if misses.is_empty() {
            return Ok(out);
        }
        misses.sort_unstable();
        misses.dedup();

        // Join fetches already in flight; start one batch for the rest.
        let mut waits: Vec<InFlight> = Vec::new();
        let mut led = Vec::new();
        {
            let mut map = self.inflight.lock();
            let mut mine = Vec::new();
            for id in &misses {
                match map.get(id) {
                    Some(f) => {
                        if !waits.iter().any(|w| w.ptr_eq(f)) {
                            waits.push(f.clone());
                        }
                    }
                    None => mine.push(*id),
                }
            }
            if !mine.is_empty() {
                let (local, upstream, batch) = (self.local.clone(), self.upstream.clone(), mine.clone());
                let fut: InFlight = async move {
                    Self::fetch(local, upstream, batch)
                        .await
                        .map(Arc::new)
                        .map_err(|e| Arc::new(e.to_string()))
                }
                .boxed()
                .shared();
                for id in &mine {
                    map.insert(*id, fut.clone());
                }
                waits.push(fut);
                led = mine;
            }
        }

        let results = futures::future::join_all(waits).await;
        if !led.is_empty() {
            let mut map = self.inflight.lock();
            for id in &led {
                map.remove(id);
            }
        }
        let mut found: HashMap<ObjectId, EncodedObject> = HashMap::new();
        for r in results {
            let r = r.map_err(|e| StoreError::backend(std::io::Error::other(e.to_string())))?;
            found.extend(r.iter().map(|(k, v)| (*k, v.clone())));
        }
        for (slot, id) in out.iter_mut().zip(ids) {
            if slot.is_none() {
                *slot = found.get(id).cloned();
            }
        }
        Ok(out)
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        self.upstream.put_many(objects).await?;
        self.local.put_many(objects).await
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        let mut out = self.local.has_many(ids).await?;
        let misses: Vec<usize> = (0..ids.len()).filter(|&i| !out[i]).collect();
        if !misses.is_empty() {
            let miss_ids: Vec<ObjectId> = misses.iter().map(|&i| ids[i]).collect();
            for (i, present) in misses.into_iter().zip(self.upstream.has_many(&miss_ids).await?) {
                out[i] = present;
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use ant_core::ObjectKind;
    use ant_store::memory::MemoryObjectStore;
    use ant_store::{ObjectStoreExt, conformance};

    /// Upstream that counts how many ids it was asked for and is slow.
    #[derive(Default)]
    struct Counting {
        inner: MemoryObjectStore,
        fetched: AtomicUsize,
    }

    #[async_trait]
    impl ObjectStore for Counting {
        async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
            self.fetched.fetch_add(ids.len(), Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            self.inner.get_many(ids).await
        }
        async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
            self.inner.put_many(objects).await
        }
        async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
            self.inner.has_many(ids).await
        }
    }

    /// Upstream that returns a different object than requested.
    struct Lying(EncodedObject);

    #[async_trait]
    impl ObjectStore for Lying {
        async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
            Ok(ids.iter().map(|_| Some(self.0.clone())).collect())
        }
        async fn put_many(&self, _: &[EncodedObject]) -> Result<()> {
            Ok(())
        }
        async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
            Ok(vec![true; ids.len()])
        }
    }

    #[tokio::test]
    async fn readthrough_conforms() {
        let rt = ReadThrough::new(
            Arc::new(MemoryObjectStore::default()),
            Arc::new(MemoryObjectStore::default()),
        );
        conformance::object_store(Arc::new(rt)).await;
    }

    #[tokio::test]
    async fn misses_fill_local() {
        let local = Arc::new(MemoryObjectStore::default());
        let up = Arc::new(Counting::default());
        let o = EncodedObject::new(ObjectKind::Blob, b"remote");
        up.inner.put(o.clone()).await.unwrap();
        let rt = ReadThrough::new(local.clone(), up.clone());
        assert_eq!(rt.get(&o.id()).await.unwrap(), Some(o.clone()));
        assert_eq!(local.get(&o.id()).await.unwrap(), Some(o.clone()));
        assert_eq!(rt.get(&o.id()).await.unwrap(), Some(o));
        assert_eq!(up.fetched.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_misses_dedup() {
        let up = Arc::new(Counting::default());
        let o = EncodedObject::new(ObjectKind::Blob, b"hot");
        up.inner.put(o.clone()).await.unwrap();
        let rt = Arc::new(ReadThrough::new(Arc::new(MemoryObjectStore::default()), up.clone()));
        let mut tasks = Vec::new();
        for _ in 0..20 {
            let rt = rt.clone();
            let id = o.id();
            tasks.push(tokio::spawn(async move { rt.get(&id).await }));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap().unwrap(), Some(o.clone()));
        }
        assert_eq!(up.fetched.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rejects_hash_mismatch() {
        let liar = EncodedObject::new(ObjectKind::Blob, b"not what you asked for");
        let rt = ReadThrough::new(Arc::new(MemoryObjectStore::default()), Arc::new(Lying(liar)));
        assert!(rt.get(&ObjectId([3; 32])).await.is_err());
    }
}
