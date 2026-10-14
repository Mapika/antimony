//! Helpers shared by the crate's unit tests.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ant_core::{EncodedObject, ObjectId};
use ant_store::ObjectStore;
use ant_store::memory::MemoryObjectStore;
use async_trait::async_trait;

/// An in-memory store that counts `get_many` calls and the ids they ask
/// for, optionally answering each call after `delay`.
#[derive(Default)]
pub(crate) struct CountingStore {
    pub(crate) inner: MemoryObjectStore,
    pub(crate) calls: AtomicUsize,
    pub(crate) ids: AtomicUsize,
    pub(crate) delay: Option<Duration>,
}

impl CountingStore {
    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub(crate) fn ids(&self) -> usize {
        self.ids.load(Ordering::SeqCst)
    }

    /// Start counting from zero.
    pub(crate) fn reset(&self) {
        self.calls.store(0, Ordering::SeqCst);
        self.ids.store(0, Ordering::SeqCst);
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn get_many(&self, ids: &[ObjectId]) -> ant_store::Result<Vec<Option<EncodedObject>>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.ids.fetch_add(ids.len(), Ordering::SeqCst);
        if let Some(delay) = self.delay {
            tokio::time::sleep(delay).await;
        }
        self.inner.get_many(ids).await
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> ant_store::Result<()> {
        self.inner.put_many(objects).await
    }

    async fn has_many(&self, ids: &[ObjectId]) -> ant_store::Result<Vec<bool>> {
        self.inner.has_many(ids).await
    }
}
