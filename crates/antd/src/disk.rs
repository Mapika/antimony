//! On-disk content-addressed object cache.
//!
//! - Layout: `{root}/objects/{hex[0..2]}/{hex}`; file content is the
//!   zstd-compressed (level 1) encoded object. Writes go to a `.tmp.*` file
//!   in the same shard directory (a same-directory `rename` is cheap and
//!   parallel; one across directories serializes on a filesystem-wide lock)
//!   and are `rename`d into place, never written at the final path. Readers
//!   and `scan` ignore `.tmp.*`; `{root}/tmp` is kept for older clients. If
//!   the final path already exists, the write is skipped.
//! - `get_many`: missing file → `None`. A corrupt file (bad zstd / framing)
//!   is deleted and reported as `None` (it's a cache). A hit bumps the
//!   file's mtime to "now" (`File::set_modified`) so eviction is LRU-ish;
//!   failures to bump are ignored.
//! - Filesystem work runs in `spawn_blocking`, one blocking task per batch.
//!   Inside a batch, objects are read/written by several threads
//!   ([`crate::par::par_map`]); zstd contexts are thread-local and reused. Objects are
//!   compressed with the content size in the frame header so reads allocate
//!   once (older entries without it still decode).
//! - `gc()`: if the total size of `objects/` exceeds `max_bytes`, deletes
//!   oldest-mtime files until it is under 90% of `max_bytes`. Returns bytes
//!   freed. Concurrent readers may race with deletion; that just produces a
//!   miss. Eviction runs only when a caller invokes `gc()`; nothing in this
//!   crate runs it automatically, so `max_bytes` is not enforced on writes.
//! - `ObjectStore::put_many` writes every object (no size check);
//!   `gc` is the only place that evicts.
//! - Format version: `{root}/format` holds [`CACHE_FORMAT`]. `open` writes
//!   it when missing (a cache from before the marker is version 1) and
//!   refuses a cache whose version is newer (`ant_core::NewerFormat`).

use std::cell::RefCell;
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use ant_core::{EncodedObject, ObjectId};
use ant_store::{ObjectStore, Result, StoreError};
use async_trait::async_trait;
use bytes::Bytes;

use crate::par::par_map;

/// Prefix of in-progress writes inside a shard directory.
const TMP_PREFIX: &str = ".tmp.";

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Largest decompressed size trusted from a frame header (guards allocation
/// against a corrupt header; larger objects take the streaming path).
const MAX_PREALLOC: u64 = 1 << 30;

thread_local! {
    static DECOMPRESSOR: RefCell<Option<zstd::bulk::Decompressor<'static>>> =
        const { RefCell::new(None) };
    static COMPRESSOR: RefCell<Option<zstd::bulk::Compressor<'static>>> =
        const { RefCell::new(None) };
}

/// Version of the cache layout, recorded in `{root}/format` (`"1\n"`). A
/// cache without the file was written before it existed: version 1.
pub const CACHE_FORMAT: u64 = 1;
const FORMAT_FILE: &str = "format";

/// Refuse a cache written by a newer version; record the version in a cache
/// that has no marker yet.
fn check_cache_format(root: &Path) -> std::io::Result<()> {
    let path = root.join(FORMAT_FILE);
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            let found = ant_core::version::parse_format_marker(&text).ok_or_else(|| {
                std::io::Error::new(
                    ErrorKind::InvalidData,
                    format!("corrupt cache format marker {}: {text:?}", path.display()),
                )
            })?;
            ant_core::version::check_format(
                || format!("disk cache {} format", root.display()),
                found,
                CACHE_FORMAT,
            )
            .map_err(std::io::Error::other)
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            // Unique temp name: concurrent opens each rename a whole file.
            let tmp = root.join(format!(
                ".{FORMAT_FILE}{TMP_PREFIX}{}.{}",
                std::process::id(),
                TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::write(&tmp, format!("{CACHE_FORMAT}\n"))?;
            std::fs::rename(&tmp, &path).inspect_err(|_| {
                let _ = std::fs::remove_file(&tmp);
            })
        }
        Err(e) => Err(e),
    }
}

pub struct DiskObjectStore {
    root: PathBuf,
    max_bytes: u64,
}

impl DiskObjectStore {
    /// Open (creating directories and the format marker as needed). A cache
    /// written by a newer version is refused: an `io::Error` wrapping
    /// [`ant_core::NewerFormat`].
    pub fn open(root: impl AsRef<Path>, max_bytes: u64) -> std::io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(root.join("objects"))?;
        std::fs::create_dir_all(root.join("tmp"))?;
        check_cache_format(&root)?;
        Ok(Self { root, max_bytes })
    }

    /// Store objects whose ids the caller has already established (e.g.
    /// verified on fetch), skipping the re-hash `put_many` would do. Ids that
    /// do not match their object poison the cache; callers must have checked.
    pub async fn put_many_trusted(&self, objects: Vec<(ObjectId, EncodedObject)>) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }
        self.write_batch(objects, |root, (id, o)| write_one(root, id, o)).await
    }

    /// Write `items` with `write` on the blocking pool, several at a time.
    async fn write_batch<T: Send + Sync + 'static>(
        &self,
        items: Vec<T>,
        write: fn(&Path, &T) -> std::io::Result<()>,
    ) -> Result<()> {
        let root = self.root.clone();
        blocking(move || par_map(&items, 16, |item| write(&root, item)).into_iter().collect())
            .await
            .map_err(StoreError::backend)
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
            if f.file_name().as_encoded_bytes().starts_with(TMP_PREFIX.as_bytes()) {
                continue; // in-progress write
            }
            let Ok(meta) = f.metadata() else { continue };
            if meta.is_file() {
                out.push((f.path(), meta.len(), meta.modified()?));
            }
        }
    }
    Ok(out)
}

fn decompress(compressed: &[u8]) -> Option<Vec<u8>> {
    match zstd::zstd_safe::get_frame_content_size(compressed) {
        Ok(Some(n)) if n <= MAX_PREALLOC => DECOMPRESSOR.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                *slot = zstd::bulk::Decompressor::new().ok();
            }
            slot.as_mut()?.decompress(compressed, n as usize).ok()
        }),
        _ => zstd::decode_all(compressed).ok(),
    }
}

fn compress(raw: &[u8]) -> std::io::Result<Vec<u8>> {
    COMPRESSOR.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = Some(zstd::bulk::Compressor::new(1)?);
        }
        match slot.as_mut() {
            Some(c) => c.compress(raw),
            None => Err(std::io::Error::other("zstd compressor unavailable")),
        }
    })
}

fn read_one(path: &Path) -> Option<EncodedObject> {
    let mut file = std::fs::File::open(path).ok()?;
    let mut compressed = Vec::new();
    if let Ok(meta) = file.metadata() {
        compressed.reserve_exact(meta.len() as usize);
    }
    file.read_to_end(&mut compressed).ok()?;
    let decoded =
        decompress(&compressed).and_then(|raw| EncodedObject::from_bytes(Bytes::from(raw)).ok());
    match decoded {
        Some(o) => {
            let _ = file.set_modified(SystemTime::now());
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
    let compressed = compress(&obj.as_bytes()[..])?;
    // Temp file beside its destination: a rename within one directory only
    // locks that directory, whereas a cross-directory rename takes a
    // filesystem-wide lock that serializes every concurrent writer.
    let tmp = dest.with_file_name(format!(
        "{TMP_PREFIX}{}.{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let write = |tmp: &Path| -> std::io::Result<()> {
        let mut f = std::fs::File::create(tmp)?;
        f.write_all(&compressed)
    };
    let written = match write(&tmp) {
        Err(e) if e.kind() == ErrorKind::NotFound => {
            std::fs::create_dir_all(dest.parent().unwrap_or(root))?;
            write(&tmp)
        }
        r => r,
    };
    let renamed = written.and_then(|()| std::fs::rename(&tmp, &dest));
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
        blocking(move || Ok(par_map(&ids, 32, |id| read_one(&object_path(&root, id)))))
            .await
            .map_err(StoreError::backend)
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }
        self.write_batch(objects.to_vec(), |root, o| write_one(root, &o.id(), o)).await
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        let root = self.root.clone();
        let ids = ids.to_vec();
        blocking(move || Ok(par_map(&ids, 256, |id| object_path(&root, id).exists())))
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
            let p = object_path(dir.path(), &o.id());
            assert!(p.starts_with(dir.path().join("objects")));
            assert!(p.ends_with(format!("{}/{}", &o.id().to_hex()[..2], o.id().to_hex())));
            assert!(p.exists());
        }
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        assert_eq!(s.get(&o.id()).await.unwrap(), Some(o));
    }

    #[tokio::test]
    async fn format_marker_is_written_and_newer_caches_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        // A cache written before the marker existed: objects, no `format`.
        let o = EncodedObject::new(ObjectKind::Blob, b"old cache");
        DiskObjectStore::open(dir.path(), u64::MAX).unwrap().put(o.clone()).await.unwrap();
        std::fs::remove_file(dir.path().join("format")).unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        assert_eq!(s.get(&o.id()).await.unwrap(), Some(o.clone()));
        assert_eq!(std::fs::read_to_string(dir.path().join("format")).unwrap(), "1\n");
        // No temp files next to it.
        let names: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().all(|n| !n.contains(TMP_PREFIX)), "{names:?}");
        // A newer version is refused with a recognizable error.
        std::fs::write(dir.path().join("format"), "2\n").unwrap();
        let err = DiskObjectStore::open(dir.path(), u64::MAX).err().unwrap();
        let newer = err.get_ref().and_then(|e| e.downcast_ref::<ant_core::NewerFormat>());
        assert_eq!(newer.map(|n| (n.found, n.supported)), Some((2, CACHE_FORMAT)), "{err}");
        // Garbage is corruption, not a newer version.
        std::fs::write(dir.path().join("format"), "x").unwrap();
        let err = DiskObjectStore::open(dir.path(), u64::MAX).err().unwrap();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn corrupt_file_is_a_miss() {
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        let o = EncodedObject::new(ObjectKind::Blob, b"x");
        s.put(o.clone()).await.unwrap();
        std::fs::write(object_path(dir.path(), &o.id()), b"garbage").unwrap();
        assert_eq!(s.get(&o.id()).await.unwrap(), None);
        assert!(!object_path(dir.path(), &o.id()).exists());
    }

    #[tokio::test]
    async fn reads_frames_without_content_size() {
        // Entries written by older versions used the streaming encoder.
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        let o = EncodedObject::new(ObjectKind::Blob, &vec![7u8; 10_000]);
        let path = object_path(dir.path(), &o.id());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, zstd::encode_all(&o.as_bytes()[..], 1).unwrap()).unwrap();
        assert_eq!(s.get(&o.id()).await.unwrap(), Some(o));
    }

    #[tokio::test]
    async fn large_batches_roundtrip_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        let objs: Vec<_> = (0..1000)
            .map(|i| EncodedObject::new(ObjectKind::Blob, format!("object {i}").as_bytes()))
            .collect();
        s.put_many(&objs).await.unwrap();
        let mut ids: Vec<_> = objs.iter().map(|o| o.id()).collect();
        ids.insert(500, ObjectId([9; 32]));
        let got = s.get_many(&ids).await.unwrap();
        assert!(got[500].is_none());
        for (i, o) in objs.iter().enumerate() {
            assert_eq!(got[if i < 500 { i } else { i + 1 }].as_ref(), Some(o));
        }
        // No in-progress files are left in shard directories.
        assert!(s.disk_usage().await.unwrap() > 0);
        for shard in std::fs::read_dir(dir.path().join("objects")).unwrap() {
            for f in std::fs::read_dir(shard.unwrap().path()).unwrap() {
                assert!(!f.unwrap().file_name().to_string_lossy().starts_with(".tmp."));
            }
        }
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
