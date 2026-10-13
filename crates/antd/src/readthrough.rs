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

use std::sync::Arc;

use ant_core::{EncodedObject, ObjectId};
use ant_store::{ObjectStore, Result};
use async_trait::async_trait;

pub struct ReadThrough {
    #[allow(dead_code)]
    local: Arc<dyn ObjectStore>,
    #[allow(dead_code)]
    upstream: Arc<dyn ObjectStore>,
}

impl ReadThrough {
    pub fn new(local: Arc<dyn ObjectStore>, upstream: Arc<dyn ObjectStore>) -> Self {
        let _ = (&local, &upstream);
        todo!("antd-cache")
    }
}

#[async_trait]
impl ObjectStore for ReadThrough {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        let _ = ids;
        todo!("antd-cache")
    }
    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        let _ = objects;
        todo!("antd-cache")
    }
    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        let _ = ids;
        todo!("antd-cache")
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
