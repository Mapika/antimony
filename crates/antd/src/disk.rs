//! On-disk content-addressed object cache.
//!
//! TASK (antd-cache): implement everything marked `todo!()`.
//!
//! - Layout: `{root}/objects/{hex[0..2]}/{hex}`; file content is the
//!   zstd-compressed (level 1) encoded object. Temp files go in `{root}/tmp`
//!   (same filesystem) and are `rename`d into place — never write the final
//!   path directly. If the final path already exists, skip the write.
//! - `get_many`: missing file → `None`. A corrupt file (bad zstd / framing)
//!   is deleted and reported as `None` (it's a cache). On hit, bump the
//!   file's mtime to "now" (`libc::utimensat` or `std::fs::File::set_modified`)
//!   so eviction is LRU-ish; failures to bump are ignored.
//! - Use `tokio::task::spawn_blocking` for filesystem work; process a batch
//!   in one blocking task (or a few), not one task per object.
//! - `gc()`: if the total size of `objects/` exceeds `max_bytes`, delete
//!   oldest-mtime files until it is under 90% of `max_bytes`. Returns bytes
//!   freed. Concurrent readers may race with deletion; that just produces a
//!   miss.
//! - `ObjectStore::put_many` writes every object (no size check);
//!   `gc` is the only place that evicts.

use std::path::{Path, PathBuf};

use ant_core::{EncodedObject, ObjectId};
use ant_store::{ObjectStore, Result};
use async_trait::async_trait;

pub struct DiskObjectStore {
    #[allow(dead_code)]
    root: PathBuf,
    #[allow(dead_code)]
    max_bytes: u64,
}

impl DiskObjectStore {
    /// Open (creating directories as needed).
    pub fn open(root: impl AsRef<Path>, max_bytes: u64) -> std::io::Result<Self> {
        let _ = (root.as_ref(), max_bytes);
        todo!("antd-cache")
    }

    pub fn path_for(&self, id: &ObjectId) -> PathBuf {
        let _ = id;
        todo!("antd-cache")
    }

    /// Total bytes on disk under `objects/`.
    pub async fn disk_usage(&self) -> std::io::Result<u64> {
        todo!("antd-cache")
    }

    /// Evict least-recently-used objects if over budget. Returns bytes freed.
    pub async fn gc(&self) -> std::io::Result<u64> {
        todo!("antd-cache")
    }
}

#[async_trait]
impl ObjectStore for DiskObjectStore {
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
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use ant_core::ObjectKind;
    use ant_store::{ObjectStoreExt, conformance};

    #[tokio::test]
    async fn disk_store_conforms() {
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        conformance::object_store(Arc::new(s)).await;
    }

    #[tokio::test]
    async fn layout_and_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let o = EncodedObject::new(ObjectKind::Blob, b"persist me");
        {
            let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
            s.put(o.clone()).await.unwrap();
            let p = s.path_for(&o.id());
            assert!(p.starts_with(dir.path().join("objects")));
            assert!(p.ends_with(format!("{}/{}", &o.id().to_hex()[..2], o.id().to_hex())));
            assert!(p.exists());
        }
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        assert_eq!(s.get(&o.id()).await.unwrap(), Some(o));
    }

    #[tokio::test]
    async fn corrupt_file_is_a_miss() {
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        let o = EncodedObject::new(ObjectKind::Blob, b"x");
        s.put(o.clone()).await.unwrap();
        std::fs::write(s.path_for(&o.id()), b"garbage").unwrap();
        assert_eq!(s.get(&o.id()).await.unwrap(), None);
        assert!(!s.path_for(&o.id()).exists());
    }

    #[tokio::test]
    async fn gc_evicts_least_recently_used() {
        let dir = tempfile::tempdir().unwrap();
        // Incompressible-ish payloads so sizes are predictable.
        let mk = |seed: u8| {
            let body: Vec<u8> = (0..20_000u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8 ^ seed).collect();
            EncodedObject::new(ObjectKind::Blob, &body)
        };
        let objs: Vec<_> = (0..10).map(mk).collect();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        for o in &objs {
            s.put(o.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        // Touch the oldest so it becomes most-recently-used.
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(s.get(&objs[0].id()).await.unwrap().is_some());

        let total = s.disk_usage().await.unwrap();
        let s = DiskObjectStore::open(dir.path(), total / 2).unwrap();
        let freed = s.gc().await.unwrap();
        assert!(freed > 0);
        assert!(s.disk_usage().await.unwrap() <= (total / 2) * 9 / 10 + 1);
        assert!(s.get(&objs[0].id()).await.unwrap().is_some(), "recently used survived");
        assert!(s.get(&objs[1].id()).await.unwrap().is_none(), "oldest evicted");
    }

    #[tokio::test]
    async fn concurrent_writers_two_handles() {
        let dir = tempfile::tempdir().unwrap();
        let a = Arc::new(DiskObjectStore::open(dir.path(), u64::MAX).unwrap());
        let b = Arc::new(DiskObjectStore::open(dir.path(), u64::MAX).unwrap());
        let objs: Vec<_> = (0..200).map(|i| EncodedObject::new(ObjectKind::Blob, format!("{i}").as_bytes())).collect();
        let (oa, ob) = (objs.clone(), objs.clone());
        let ta = tokio::spawn(async move { a.put_many(&oa).await });
        let tb = tokio::spawn(async move { b.put_many(&ob).await });
        ta.await.unwrap().unwrap();
        tb.await.unwrap().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        let ids: Vec<_> = objs.iter().map(|o| o.id()).collect();
        assert!(s.get_many(&ids).await.unwrap().iter().all(Option::is_some));
        // No temp files left behind.
        assert_eq!(std::fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
    }
}
