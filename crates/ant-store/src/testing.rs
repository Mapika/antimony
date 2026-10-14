//! Test doubles and settings shared by the tests of every crate.

use std::sync::Arc;
use std::time::Duration;

use ant_core::{EncodedObject, ObjectId};
use async_trait::async_trait;
use parking_lot::Mutex;

use crate::memory::MemoryObjectStore;
use crate::{ObjectStore, Result};

/// Postgres URL of the test services: `ANT_TEST_PG_URL`, default the dev
/// services' database (`scripts/dev-services.sh start` or
/// `docker compose up -d`).
pub fn pg_url() -> String {
    std::env::var("ANT_TEST_PG_URL")
        .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into())
}

/// An object store that records the ids of every `get_many` call before
/// forwarding it (by default to a fresh in-memory store), optionally after a
/// delay. Writes and `has_many` are forwarded unrecorded.
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    delay: Option<Duration>,
    reads: Mutex<Vec<Vec<ObjectId>>>,
}

impl Default for CountingStore {
    fn default() -> Self {
        Self::new(Arc::new(MemoryObjectStore::default()))
    }
}

impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner, delay: None, reads: Mutex::new(Vec::new()) }
    }

    /// Answer each `get_many` after `delay`.
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// The wrapped store (reads through it are not recorded).
    pub fn inner(&self) -> &dyn ObjectStore {
        self.inner.as_ref()
    }

    /// The ids of each `get_many` call since the last [`Self::reset`].
    pub fn reads(&self) -> Vec<Vec<ObjectId>> {
        self.reads.lock().clone()
    }

    /// `get_many` calls since the last [`Self::reset`].
    pub fn calls(&self) -> usize {
        self.reads.lock().len()
    }

    /// Ids asked for since the last [`Self::reset`], over all calls.
    pub fn ids(&self) -> usize {
        self.reads.lock().iter().map(Vec::len).sum()
    }

    /// Start recording from scratch.
    pub fn reset(&self) {
        self.reads.lock().clear();
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        self.reads.lock().push(ids.to_vec());
        if let Some(delay) = self.delay {
            tokio::time::sleep(delay).await;
        }
        self.inner.get_many(ids).await
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        self.inner.put_many(objects).await
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        self.inner.has_many(ids).await
    }
}
