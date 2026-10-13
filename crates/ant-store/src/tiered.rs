//! Routes small objects to one store (e.g. Postgres) and large ones to
//! another (e.g. S3). Most trees and source files are small, and S3's
//! per-request latency would dominate if every tiny object went there.
//!
//! - `put_many`: objects whose *encoded* length is `<= threshold` go to
//!   `small`, the rest to `large`. One `put_many` call per tier.
//! - `get_many`: one `get_many` on `small` for all ids, then one on `large`
//!   for only the misses; merge preserving input order.
//! - `has_many`: same two-phase shape.

use std::sync::Arc;

use ant_core::{EncodedObject, ObjectId};
use async_trait::async_trait;

use crate::{ObjectStore, Result};

pub const DEFAULT_INLINE_THRESHOLD: usize = 4096;

pub struct TieredObjectStore {
    small: Arc<dyn ObjectStore>,
    large: Arc<dyn ObjectStore>,
    threshold: usize,
}

impl TieredObjectStore {
    pub fn new(small: Arc<dyn ObjectStore>, large: Arc<dyn ObjectStore>, threshold: usize) -> Self {
        Self { small, large, threshold }
    }
}

#[async_trait]
impl ObjectStore for TieredObjectStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        let mut out = self.small.get_many(ids).await?;
        let misses: Vec<usize> =
            out.iter().enumerate().filter(|(_, o)| o.is_none()).map(|(i, _)| i).collect();
        if !misses.is_empty() {
            let miss_ids: Vec<ObjectId> = misses.iter().map(|&i| ids[i]).collect();
            let found = self.large.get_many(&miss_ids).await?;
            for (i, o) in misses.into_iter().zip(found) {
                out[i] = o;
            }
        }
        Ok(out)
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        let mut small = Vec::new();
        let mut large = Vec::new();
        for o in objects {
            if o.as_bytes().len() <= self.threshold {
                small.push(o.clone());
            } else {
                large.push(o.clone());
            }
        }
        // The tiers are independent: write them concurrently.
        let put_small =
            async { if small.is_empty() { Ok(()) } else { self.small.put_many(&small).await } };
        let put_large =
            async { if large.is_empty() { Ok(()) } else { self.large.put_many(&large).await } };
        futures::try_join!(put_small, put_large)?;
        Ok(())
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        let mut out = self.small.has_many(ids).await?;
        let misses: Vec<usize> =
            out.iter().enumerate().filter(|(_, present)| !**present).map(|(i, _)| i).collect();
        if !misses.is_empty() {
            let miss_ids: Vec<ObjectId> = misses.iter().map(|&i| ids[i]).collect();
            let found = self.large.has_many(&miss_ids).await?;
            for (i, present) in misses.into_iter().zip(found) {
                out[i] = present;
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryObjectStore;
    use crate::{ObjectStoreExt, conformance};
    use ant_core::ObjectKind;

    #[tokio::test]
    async fn tiered_conforms() {
        let s = TieredObjectStore::new(
            Arc::new(MemoryObjectStore::default()),
            Arc::new(MemoryObjectStore::default()),
            DEFAULT_INLINE_THRESHOLD,
        );
        conformance::object_store(Arc::new(s)).await;
    }

    #[tokio::test]
    async fn routes_by_size() {
        let small = Arc::new(MemoryObjectStore::default());
        let large = Arc::new(MemoryObjectStore::default());
        let s = TieredObjectStore::new(small.clone(), large.clone(), 100);
        let a = EncodedObject::new(ObjectKind::Blob, &[1; 10]);
        let b = EncodedObject::new(ObjectKind::Blob, &[2; 1000]);
        s.put_many(&[a.clone(), b.clone()]).await.unwrap();
        assert_eq!(small.len(), 1);
        assert_eq!(large.len(), 1);
        assert!(small.get(&a.id()).await.unwrap().is_some());
        assert!(large.get(&b.id()).await.unwrap().is_some());
        assert_eq!(s.get(&b.id()).await.unwrap(), Some(b));
    }
}
