//! In-memory reference implementations. Used by tests everywhere and as the
//! behavioral reference for the persistent stores.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use ant_core::{EncodedObject, ObjectId};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;

use ant_core::RepoPath;

use crate::{
    CasOutcome, CommitNode, HistoryRow, MetaStore, ObjectStore, PathChange, RefLock,
    RefLockRelease, RefStore, Result, validate_ref_name,
};

#[derive(Default)]
pub struct MemoryObjectStore {
    objects: Mutex<HashMap<ObjectId, EncodedObject>>,
}

impl MemoryObjectStore {
    pub fn len(&self) -> usize {
        self.objects.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl ObjectStore for MemoryObjectStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        let m = self.objects.lock();
        Ok(ids.iter().map(|id| m.get(id).cloned()).collect())
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        let mut m = self.objects.lock();
        for o in objects {
            m.entry(o.id()).or_insert_with(|| o.clone());
        }
        Ok(())
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        let m = self.objects.lock();
        Ok(ids.iter().map(|id| m.contains_key(id)).collect())
    }
}

/// Per-ref writer locks of a [`MemoryRefStore`]: in-process only, since a
/// memory store is never shared between processes. Entries exist only while
/// a lock is held or awaited.
type RefLocks = Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>;

#[derive(Default)]
pub struct MemoryRefStore {
    refs: Mutex<BTreeMap<String, ObjectId>>,
    locks: RefLocks,
}

struct MemoryRefLock {
    locks: RefLocks,
    name: String,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for MemoryRefLock {
    fn drop(&mut self) {
        let mut locks = self.locks.lock();
        drop(self.guard.take());
        // Waiters hold clones of the entry; remove it only when none remain.
        if locks.get(&self.name).is_some_and(|m| Arc::strong_count(m) == 1) {
            locks.remove(&self.name);
        }
    }
}

#[async_trait]
impl RefLockRelease for MemoryRefLock {
    async fn release(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl RefStore for MemoryRefStore {
    async fn get_ref(&self, name: &str) -> Result<Option<ObjectId>> {
        validate_ref_name(name)?;
        Ok(self.refs.lock().get(name).copied())
    }

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ObjectId>,
        new: Option<ObjectId>,
    ) -> Result<CasOutcome> {
        validate_ref_name(name)?;
        let mut m = self.refs.lock();
        let actual = m.get(name).copied();
        if actual != expected {
            return Ok(CasOutcome::Mismatch { actual });
        }
        match new {
            Some(id) => m.insert(name.to_string(), id),
            None => m.remove(name),
        };
        Ok(CasOutcome::Updated)
    }

    async fn list_refs(&self, prefix: &str) -> Result<Vec<(String, ObjectId)>> {
        Ok(self
            .refs
            .lock()
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), *v))
            .collect())
    }

    async fn lock_ref(&self, name: &str) -> Result<RefLock> {
        validate_ref_name(name)?;
        let mutex = self.locks.lock().entry(name.to_string()).or_default().clone();
        let mut lock = MemoryRefLock { locks: self.locks.clone(), name: name.to_string(), guard: None };
        // `lock` owns the entry from here, so a cancelled wait still cleans up.
        lock.guard = Some(mutex.lock_owned().await);
        Ok(RefLock::held(lock))
    }
}

#[derive(Default)]
struct MetaInner {
    commits: HashMap<ObjectId, (u64, CommitNode, Vec<PathChange>)>,
    log: Vec<(u64, ObjectId)>,
    derived: HashMap<(String, Vec<u8>), Bytes>,
    /// ref -> (path, generation, commit) rows, deduplicated.
    history: HashMap<String, std::collections::BTreeSet<(String, u64, ObjectId)>>,
    watermarks: HashMap<String, ObjectId>,
}

#[derive(Default)]
pub struct MemoryMetaStore {
    inner: Mutex<MetaInner>,
}

#[async_trait]
impl MetaStore for MemoryMetaStore {
    async fn put_commit(&self, node: CommitNode, changes: Vec<PathChange>) -> Result<u64> {
        let mut m = self.inner.lock();
        if let Some((seq, _, _)) = m.commits.get(&node.id) {
            return Ok(*seq);
        }
        let seq = m.log.last().map_or(1, |(s, _)| s + 1);
        m.log.push((seq, node.id));
        m.commits.insert(node.id, (seq, node, changes));
        Ok(seq)
    }

    async fn get_commits(&self, ids: &[ObjectId]) -> Result<Vec<Option<CommitNode>>> {
        let m = self.inner.lock();
        Ok(ids.iter().map(|id| m.commits.get(id).map(|(_, n, _)| n.clone())).collect())
    }

    async fn get_changeset(&self, id: &ObjectId) -> Result<Option<Vec<PathChange>>> {
        Ok(self.inner.lock().commits.get(id).map(|(_, _, c)| c.clone()))
    }

    async fn tail(&self, after: u64, limit: usize) -> Result<Vec<(u64, ObjectId)>> {
        let m = self.inner.lock();
        let start = m.log.partition_point(|(s, _)| *s <= after);
        Ok(m.log[start..].iter().take(limit).copied().collect())
    }

    async fn get_derived(&self, kind: &str, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.inner.lock().derived.get(&(kind.to_string(), key.to_vec())).cloned())
    }

    async fn put_derived(&self, kind: &str, key: &[u8], value: Bytes) -> Result<()> {
        self.inner.lock().derived.insert((kind.to_string(), key.to_vec()), value);
        Ok(())
    }

    async fn index_history(&self, ref_name: &str, rows: &[HistoryRow]) -> Result<()> {
        let mut m = self.inner.lock();
        let set = m.history.entry(ref_name.to_string()).or_default();
        for r in rows {
            set.insert((r.path.as_str().to_string(), r.generation, r.commit));
        }
        Ok(())
    }

    async fn query_history(
        &self,
        ref_name: &str,
        prefix: &RepoPath,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<(u64, ObjectId)>> {
        let m = self.inner.lock();
        let mut hits: Vec<(u64, ObjectId)> = m
            .history
            .get(ref_name)
            .into_iter()
            .flatten()
            .filter(|(p, g, _)| {
                *g <= max_generation
                    && RepoPath::new(p.clone()).is_ok_and(|p| p.starts_with(prefix))
            })
            .map(|(_, g, c)| (*g, *c))
            .collect();
        hits.sort_unstable_by(|a, b| b.cmp(a));
        hits.dedup();
        hits.truncate(limit);
        Ok(hits)
    }

    async fn history_watermark(&self, ref_name: &str) -> Result<Option<ObjectId>> {
        Ok(self.inner.lock().watermarks.get(ref_name).copied())
    }

    async fn set_history_watermark(
        &self,
        ref_name: &str,
        expected: Option<ObjectId>,
        new: ObjectId,
    ) -> Result<bool> {
        let mut m = self.inner.lock();
        if m.watermarks.get(ref_name).copied() != expected {
            return Ok(false);
        }
        m.watermarks.insert(ref_name.to_string(), new);
        Ok(true)
    }

    async fn clear_history(&self, ref_name: &str) -> Result<()> {
        let mut m = self.inner.lock();
        m.history.remove(ref_name);
        m.watermarks.remove(ref_name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::conformance;

    #[tokio::test]
    async fn memory_object_store_conforms() {
        conformance::object_store(Arc::new(MemoryObjectStore::default())).await;
    }

    #[tokio::test]
    async fn memory_ref_store_conforms() {
        conformance::ref_store(Arc::new(MemoryRefStore::default())).await;
        let s: Arc<dyn RefStore> = Arc::new(MemoryRefStore::default());
        conformance::ref_lock(s.clone(), s).await;
    }

    #[tokio::test]
    async fn memory_meta_store_conforms() {
        conformance::meta_store(Arc::new(MemoryMetaStore::default())).await;
        conformance::history_index(Arc::new(MemoryMetaStore::default())).await;
    }

    #[tokio::test]
    async fn memory_meta_batch_conforms() {
        conformance::meta_store_batch(Arc::new(MemoryMetaStore::default())).await;
    }
}
