//! On-disk content-addressed object cache.
//!
//! - Layout: `{root}/objects/{hex[0..2]}/{hex}`; file content is the
//!   zstd-compressed (level 1) encoded object. Temp files go in `{root}/tmp`
//!   (same filesystem) and are `rename`d into place, never written at the
//!   final path. If the final path already exists, the write is skipped.
//! - `get_many`: missing file → `None`. A corrupt file (bad zstd / framing)
//!   is deleted and reported as `None` (it's a cache). A hit bumps the
//!   file's mtime to "now" (`File::set_modified`) so eviction is LRU-ish;
//!   failures to bump are ignored.
//! - Filesystem work runs in `spawn_blocking`, one blocking task per batch.
//! - `gc()`: if the total size of `objects/` exceeds `max_bytes`, deletes
//!   oldest-mtime files until it is under 90% of `max_bytes`. Returns bytes
//!   freed. Concurrent readers may race with deletion; that just produces a
//!   miss.
//! - `ObjectStore::put_many` writes every object (no size check);
//!   `gc` is the only place that evicts.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use ant_core::{EncodedObject, ObjectId};
use ant_store::{ObjectStore, Result, StoreError};
use async_trait::async_trait;
use bytes::Bytes;

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct DiskObjectStore {
    root: PathBuf,
    max_bytes: u64,
}

impl DiskObjectStore {
    /// Open (creating directories as needed).
    pub fn open(root: impl AsRef<Path>, max_bytes: u64) -> std::io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(root.join("objects"))?;
        std::fs::create_dir_all(root.join("tmp"))?;
        Ok(Self { root, max_bytes })
    }

    pub fn path_for(&self, id: &ObjectId) -> PathBuf {
        object_path(&self.root, id)
    }

    /// Total bytes on disk under `objects/`.
    pub async fn disk_usage(&self) -> std::io::Result<u64> {
        let root = self.root.clone();
        blocking(move || Ok(scan(&root)?.iter().map(|(_, size, _)| size).sum())).await
    }

    /// Evict least-recently-used objects if over budget. Returns bytes freed.
    pub async fn gc(&self) -> std::io::Result<u64> {
        let root = self.root.clone();
        let max = self.max_bytes;
        blocking(move || {
            let mut files = scan(&root)?;
            let mut total: u64 = files.iter().map(|(_, size, _)| size).sum();
            if total <= max {
                return Ok(0);
            }
            let target = max / 10 * 9 + (max % 10) * 9 / 10;
            files.sort_by_key(|(_, _, mtime)| *mtime);
            let mut freed = 0;
            for (path, size, _) in files {
                if total <= target {
                    break;
                }
                match std::fs::remove_file(&path) {
                    Ok(()) => {
                        total -= size;
                        freed += size;
                    }
                    Err(e) if e.kind() == ErrorKind::NotFound => total -= size,
                    Err(e) => return Err(e),
                }
            }
            Ok(freed)
        })
        .await
    }
}

fn object_path(root: &Path, id: &ObjectId) -> PathBuf {
    let hex = id.to_hex();
    root.join("objects").join(&hex[..2]).join(hex)
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> std::io::Result<T> {
    tokio::task::spawn_blocking(f).await.map_err(std::io::Error::other)?
}

/// (path, size, mtime) of every cached object.
fn scan(root: &Path) -> std::io::Result<Vec<(PathBuf, u64, SystemTime)>> {
    let mut out = Vec::new();
    for shard in std::fs::read_dir(root.join("objects"))? {
        let shard = shard?;
        if !shard.file_type()?.is_dir() {
            continue;
        }
        for f in std::fs::read_dir(shard.path())? {
            let f = f?;
            // Racing deletions just shrink the result.
            let Ok(meta) = f.metadata() else { continue };
            if meta.is_file() {
                out.push((f.path(), meta.len(), meta.modified()?));
            }
        }
    }
    Ok(out)
}

fn read_one(path: &Path) -> Option<EncodedObject> {
    let compressed = match std::fs::read(path) {
        Ok(c) => c,
        Err(_) => return None,
    };
    let decoded = zstd::decode_all(&compressed[..])
        .ok()
        .and_then(|raw| EncodedObject::from_bytes(Bytes::from(raw)).ok());
    match decoded {
        Some(o) => {
            if let Ok(f) = std::fs::File::open(path) {
                let _ = f.set_modified(SystemTime::now());
            }
            Some(o)
        }
        None => {
            // Corrupt cache entry: drop it and report a miss.
            let _ = std::fs::remove_file(path);
            None
        }
    }
}

fn write_one(root: &Path, id: &ObjectId, obj: &EncodedObject) -> std::io::Result<()> {
    let dest = object_path(root, id);
    if dest.exists() {
        return Ok(());
    }
    let compressed = zstd::encode_all(&obj.as_bytes()[..], 1)?;
    let tmp = root.join("tmp").join(format!(
        "{}.{}.{}",
        id.to_hex(),
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&tmp, compressed)?;
    let renamed = match std::fs::rename(&tmp, &dest) {
        Err(e) if e.kind() == ErrorKind::NotFound => {
            std::fs::create_dir_all(dest.parent().unwrap_or(root))?;
            std::fs::rename(&tmp, &dest)
        }
        r => r,
    };
    if renamed.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    renamed
}

#[async_trait]
impl ObjectStore for DiskObjectStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let root = self.root.clone();
        let ids = ids.to_vec();
        blocking(move || Ok(ids.iter().map(|id| read_one(&object_path(&root, id))).collect()))
            .await
            .map_err(StoreError::backend)
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }
        let root = self.root.clone();
        let objects = objects.to_vec();
        blocking(move || {
            for o in &objects {
                write_one(&root, &o.id(), o)?;
            }
            Ok(())
        })
        .await
        .map_err(StoreError::backend)
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        let root = self.root.clone();
        let ids = ids.to_vec();
        blocking(move || Ok(ids.iter().map(|id| object_path(&root, id).exists()).collect()))
            .await
            .map_err(StoreError::backend)
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
            let body: Vec<u8> =
                (0..20_000u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8 ^ seed).collect();
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
        let objs: Vec<_> = (0..200)
            .map(|i| EncodedObject::new(ObjectKind::Blob, format!("{i}").as_bytes()))
            .collect();
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
