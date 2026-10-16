//! Pack files of a local data directory (`<dir>/packs/<32 hex>.pack`), the
//! [`PackFiles`] of the local store's [`crate::packed::PackedStore`].
//!
//! - **Format.** An 8-byte header `ANTPACK1`, then records exactly as in S3
//!   packs (`crate::packed::frame`: single-object records, `AFR1` frames,
//!   `ADF1` delta frames), each self-contained. The index (SQLite
//!   `object_index`) holds `id -> (pack, offset, length)`; a pack file has
//!   no index of its own.
//! - **Appends.** One *active* pack per process receives every write,
//!   appended under a lock; at [`ROLL_BYTES`] it is fsynced and closed and
//!   the next write starts a new pack with a random id. A pack is never
//!   appended to after the process that created it stops (a crash can only
//!   leave a torn tail that no index row points at).
//! - **Durability.** `write_packs` returns only after its bytes are on disk
//!   (`fdatasync`), and the index rows are written after it returns, so an
//!   index row never points at bytes a crash can lose. Concurrent writers
//!   share fsyncs: one `fdatasync` covers every append made before it
//!   started. A new pack's directory entry is fsynced when it is created.
//! - **Reads** are `pread`s on the blocking pool through a small cache of
//!   open file handles.

use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;

use crate::packed::PackFiles;
use crate::{Result, StoreError};

/// Magic of a pack file (its first 8 bytes); the version is the digit.
pub const PACK_MAGIC: &[u8; 8] = b"ANTPACK1";
/// A pack is closed once it reaches this size.
pub const ROLL_BYTES: u64 = 64 * 1024 * 1024;
/// Open read handles kept.
const OPEN_FILES: usize = 64;

fn io(e: std::io::Error) -> StoreError {
    StoreError::backend(e)
}

/// The file name of pack `id`.
pub fn pack_file_name(id: &[u8; 16]) -> String {
    format!("{}.pack", hex::encode(id))
}

/// The pack id of a file name made by [`pack_file_name`].
pub fn parse_pack_file_name(name: &str) -> Option<[u8; 16]> {
    let hex = name.strip_suffix(".pack")?;
    let bytes = hex::decode(hex).ok()?;
    bytes.try_into().ok()
}

struct Active {
    id: [u8; 16],
    file: Arc<File>,
    len: u64,
}

struct Inner {
    dir: PathBuf,
    /// The pack being appended to, and the number of appends ever made
    /// (all packs; the sequence fsyncs are counted in).
    active: Mutex<(Option<Active>, u64)>,
    /// Serializes fsyncs of the active pack.
    syncing: Mutex<()>,
    /// Appends `<=` this are durable.
    synced: AtomicU64,
    open: Mutex<lru::LruCache<[u8; 16], Arc<File>>>,
}

/// Pack files under one directory (see the module docs). Clones share state.
#[derive(Clone)]
pub struct LocalPacks {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for LocalPacks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalPacks").field("dir", &self.inner.dir).finish()
    }
}

/// fsync a directory, making the entries created in it durable.
pub(crate) fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

impl LocalPacks {
    /// Packs under `dir` (created if missing).
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(io)?;
        let open = lru::LruCache::new(
            std::num::NonZeroUsize::new(OPEN_FILES).unwrap_or(std::num::NonZeroUsize::MIN),
        );
        Ok(Self {
            inner: Arc::new(Inner {
                dir: dir.to_owned(),
                active: Mutex::new((None, 0)),
                syncing: Mutex::new(()),
                synced: AtomicU64::new(0),
                open: Mutex::new(open),
            }),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.inner.dir
    }

    /// Path of pack `id`.
    pub fn path_of(&self, id: &[u8; 16]) -> PathBuf {
        self.inner.dir.join(pack_file_name(id))
    }

    /// Start a new pack: header written, directory entry durable.
    fn create(&self) -> Result<Active> {
        let id: [u8; 16] = rand::random();
        let path = self.path_of(&id);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(io)?;
        file.write_all_at(PACK_MAGIC, 0).map_err(io)?;
        sync_dir(&self.inner.dir).map_err(io)?;
        Ok(Active { id, file: Arc::new(file), len: PACK_MAGIC.len() as u64 })
    }

    /// Append `bodies` (blocking); returns where each went and the append
    /// sequence number to sync up to.
    fn append(&self, bodies: &[Vec<u8>]) -> Result<(Vec<([u8; 16], u64)>, u64)> {
        let mut guard = self.inner.active.lock();
        let (active, seq) = &mut *guard;
        let mut placed = Vec::with_capacity(bodies.len());
        for body in bodies {
            let full = active.as_ref().is_some_and(|a| {
                a.len > PACK_MAGIC.len() as u64 && a.len + body.len() as u64 > ROLL_BYTES
            });
            if full && let Some(old) = active.take() {
                // Closing a pack: make it durable now, so only the active
                // pack ever needs a sync.
                old.file.sync_data().map_err(io)?;
                self.inner.synced.fetch_max(*seq, Ordering::SeqCst);
            }
            if active.is_none() {
                *active = Some(self.create()?);
            }
            let Some(a) = active.as_mut() else {
                return Err(StoreError::InvalidArgument("no active pack".into()));
            };
            if let Err(e) = a.file.write_all_at(body, a.len) {
                // A torn append: never append to this pack again.
                *active = None;
                return Err(io(e));
            }
            placed.push((a.id, a.len));
            a.len += body.len() as u64;
            *seq += 1;
        }
        Ok((placed, *seq))
    }

    /// Make every append up to `seq` durable (blocking), sharing fsyncs
    /// between concurrent callers.
    fn sync_upto(&self, seq: u64) -> Result<()> {
        if self.inner.synced.load(Ordering::SeqCst) >= seq {
            return Ok(());
        }
        let _one = self.inner.syncing.lock();
        if self.inner.synced.load(Ordering::SeqCst) >= seq {
            return Ok(());
        }
        // Appends are made under the `active` lock, so every append counted
        // in `upto` is complete; earlier packs were synced when closed.
        let (file, upto) = {
            let guard = self.inner.active.lock();
            (guard.0.as_ref().map(|a| a.file.clone()), guard.1)
        };
        if let Some(file) = file {
            file.sync_data().map_err(io)?;
        }
        self.inner.synced.fetch_max(upto, Ordering::SeqCst);
        Ok(())
    }

    /// An open handle on pack `id` (blocking).
    fn handle(&self, id: &[u8; 16]) -> Result<Arc<File>> {
        if let Some(f) = self.inner.open.lock().get(id) {
            return Ok(f.clone());
        }
        let file = Arc::new(File::open(self.path_of(id)).map_err(|e| {
            StoreError::backend(std::io::Error::new(
                e.kind(),
                format!("pack {}: {e}", hex::encode(id)),
            ))
        })?);
        self.inner.open.lock().put(*id, file.clone());
        Ok(file)
    }

    /// Forget open handles (a pack was deleted or replaced).
    pub(crate) fn forget(&self, ids: &[[u8; 16]]) {
        let mut open = self.inner.open.lock();
        for id in ids {
            open.pop(id);
        }
    }

    /// Every pack file in the directory with its size (blocking).
    pub(crate) fn list(&self) -> Result<HashMap<[u8; 16], u64>> {
        let mut out = HashMap::new();
        for entry in std::fs::read_dir(&self.inner.dir).map_err(io)? {
            let entry = entry.map_err(io)?;
            let Some(id) = entry.file_name().to_str().and_then(parse_pack_file_name) else {
                continue;
            };
            out.insert(id, entry.metadata().map_err(io)?.len());
        }
        Ok(out)
    }

    /// Close the active pack (durably): the next write starts a new one.
    pub(crate) fn seal(&self) -> Result<()> {
        let mut guard = self.inner.active.lock();
        if let Some(old) = guard.0.take() {
            old.file.sync_data().map_err(io)?;
            let seq = guard.1;
            self.inner.synced.fetch_max(seq, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Read a range (blocking).
    pub(crate) fn read_blocking(&self, pack: &[u8; 16], offset: u64, len: u64) -> Result<Vec<u8>> {
        let file = self.handle(pack)?;
        let mut buf = vec![0u8; len as usize];
        file.read_exact_at(&mut buf, offset).map_err(|e| {
            StoreError::backend(std::io::Error::new(
                e.kind(),
                format!("pack {} at {offset}+{len}: {e}", hex::encode(pack)),
            ))
        })?;
        Ok(buf)
    }

    /// Append and sync (blocking).
    pub(crate) fn write_blocking(&self, bodies: &[Vec<u8>]) -> Result<Vec<([u8; 16], u64)>> {
        let (placed, seq) = self.append(bodies)?;
        self.sync_upto(seq)?;
        Ok(placed)
    }
}

#[async_trait]
impl PackFiles for LocalPacks {
    async fn write_packs(&self, bodies: Vec<Vec<u8>>) -> Result<Vec<([u8; 16], u64)>> {
        if bodies.is_empty() {
            return Ok(Vec::new());
        }
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.write_blocking(&bodies))
            .await
            .map_err(StoreError::backend)?
    }

    async fn read_range(&self, pack: &[u8; 16], offset: u64, len: u64) -> Result<Bytes> {
        let this = self.clone();
        let pack = *pack;
        let body = tokio::task::spawn_blocking(move || this.read_blocking(&pack, offset, len))
            .await
            .map_err(StoreError::backend)??;
        Ok(Bytes::from(body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn appends_are_placed_read_back_and_shared_by_concurrent_writers() {
        let dir = tempfile::tempdir().unwrap();
        let packs = LocalPacks::open(dir.path()).unwrap();
        let placed = packs.write_packs(vec![b"hello".to_vec(), b"world!".to_vec()]).await.unwrap();
        assert_eq!(placed[0].0, placed[1].0, "one active pack");
        assert_eq!(placed[0].1, 8);
        assert_eq!(placed[1].1, 13);
        assert_eq!(&packs.read_range(&placed[1].0, 13, 6).await.unwrap()[..], b"world!");
        let mut tasks = Vec::new();
        for i in 0..32u8 {
            let packs = packs.clone();
            tasks.push(tokio::spawn(async move {
                let body = vec![i; 1000 + i as usize];
                let at = packs.write_packs(vec![body.clone()]).await.unwrap()[0];
                (at, body)
            }));
        }
        for t in tasks {
            let ((pack, off), body) = t.await.unwrap();
            let got = packs.read_range(&pack, off, body.len() as u64).await.unwrap();
            assert_eq!(&got[..], &body[..]);
        }
        // Every append is durable once written.
        let seq = packs.inner.active.lock().1;
        assert!(packs.inner.synced.load(Ordering::SeqCst) >= seq);
        let file = std::fs::read(packs.path_of(&placed[0].0)).unwrap();
        assert_eq!(&file[..8], PACK_MAGIC);
    }

    #[tokio::test]
    async fn full_packs_roll_over_and_sealing_starts_a_new_pack() {
        let dir = tempfile::tempdir().unwrap();
        let packs = LocalPacks::open(dir.path()).unwrap();
        let big = vec![1u8; (ROLL_BYTES / 2) as usize + 1];
        let a = packs.write_packs(vec![big.clone(), big.clone(), b"x".to_vec()]).await.unwrap();
        assert_ne!(a[0].0, a[1].0, "the second half does not fit");
        assert_eq!(a[1].0, a[2].0);
        packs.seal().unwrap();
        let b = packs.write_packs(vec![b"y".to_vec()]).await.unwrap();
        assert_ne!(b[0].0, a[2].0);
        assert_eq!(packs.list().unwrap().len(), 3);
        assert_eq!(&packs.read_range(&a[2].0, a[2].1, 1).await.unwrap()[..], b"x");
        assert!(packs.read_range(&[9; 16], 8, 1).await.is_err(), "missing pack");
        assert!(packs.read_range(&b[0].0, 8, 100).await.is_err(), "short read");
    }
}
