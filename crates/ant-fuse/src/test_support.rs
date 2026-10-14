//! Helpers shared by the crate's unit tests.

use ant_core::{EncodedObject, ObjectId};
use ant_store::ObjectStore;
use ant_store::memory::MemoryObjectStore;
use async_trait::async_trait;
use parking_lot::Mutex;

/// The multi-thread runtime the mounts and blocking state operations need.
pub(crate) fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
}

/// An in-memory store that records the ids of every `get_many` call.
#[derive(Default)]
pub(crate) struct RecordingStore {
    pub(crate) inner: MemoryObjectStore,
    pub(crate) reads: Mutex<Vec<Vec<ObjectId>>>,
}

impl RecordingStore {
    /// Ids read since the last [`Self::forget_reads`], over all calls.
    pub(crate) fn ids_read(&self) -> usize {
        self.reads.lock().iter().map(Vec::len).sum()
    }

    pub(crate) fn forget_reads(&self) {
        self.reads.lock().clear();
    }
}

#[async_trait]
impl ObjectStore for RecordingStore {
    async fn get_many(&self, ids: &[ObjectId]) -> ant_store::Result<Vec<Option<EncodedObject>>> {
        self.reads.lock().push(ids.to_vec());
        self.inner.get_many(ids).await
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> ant_store::Result<()> {
        self.inner.put_many(objects).await
    }

    async fn has_many(&self, ids: &[ObjectId]) -> ant_store::Result<Vec<bool>> {
        self.inner.has_many(ids).await
    }
}
