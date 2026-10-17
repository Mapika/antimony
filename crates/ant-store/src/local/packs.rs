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
//! - **Busy packs.** A pack is *busy* while it may still gain index rows
//!   from anyone but the maintenance run deciding about it: while it is the
//!   active pack, while it holds an append whose index rows have not been
//!   written yet ([`PackFiles::settle`] reports them written), and while a
//!   pack maintenance writes is pinned ([`PackWriter`], until its rows are
//!   switched). Maintenance (`local::gc`, `local::repack`) never deletes or
//!   compacts a busy pack ([`LocalPacks::busy`]). A pack is busy from
//!   before its file exists: the active pack is created, appended to and
//!   registered under the `pending` lock that [`LocalPacks::busy`] takes,
//!   and a maintenance pack is pinned in the critical section that creates
//!   it. A pack that is not busy never becomes busy again (pack ids are
//!   random and new, and a pack never becomes active again), so no writer
//!   appends to it any more.
//! - **Deletion** (`local::gc::delete_packs`, the only caller of
//!   [`LocalPacks::delete`]). Invariant: *a pack file is unlinked only if,
//!   at one instant on the store's writer thread after the file was
//!   created, the pack was not busy and no index row pointed into it.*
//!   Both are checked in one job on the writer thread, which also deletes
//!   the pack's `packs` row; the file is unlinked once that job committed.
//!   No row can point into the pack afterwards: an ordinary writer writes
//!   rows only into packs it appended to, and settles an append only after
//!   its rows committed, so a pack that is not busy already has every row
//!   a writer will ever give it, committed before the job ran on the same
//!   thread, where the job sees it. Maintenance runs one at a time and
//!   writes rows only into its own pinned packs; nothing of a run outlives
//!   it but writer jobs it already queued (a cancelled run's row switch),
//!   and those run before any job queued later, the check included.
//!   Earlier snapshots (the directory listing, the busy set, the rows a run
//!   read) can therefore be stale without harm: they only nominate
//!   candidates. (They once decided
//!   alone: a pack created by a writer between the busy snapshot and the
//!   listing was listed, not busy and, its rows not written yet,
//!   unreferenced, so it was deleted while it was the active pack: its
//!   rows, and every append after them, pointed into a deleted file.)
//! - **Readers.** A reader that looked up an old location just before its
//!   rows moved may still open the file: [`LocalPacks::delete`] bumps a
//!   generation counter before unlinking, and `PackedStore` repeats a
//!   failed read with a fresh index lookup when the generation changed
//!   meanwhile ([`PackFiles::generation`]). Handles already open keep
//!   reading the unlinked file.

use std::collections::{HashMap, HashSet};
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

/// Where a body went: its pack and the offset of its first byte.
type Placed = ([u8; 16], u64);

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
    /// Appends (pack, offset of the body) whose index rows are not known to
    /// be written yet.
    pending: Mutex<HashMap<[u8; 16], HashSet<u64>>>,
    /// Bumped before every pack deletion (see the module docs).
    generation: AtomicU64,
    /// Test hook, called at each [`HookPoint`].
    #[cfg(test)]
    hook: Mutex<Option<Hook>>,
}

/// A test hook (see [`LocalPacks::set_hook`]).
#[cfg(test)]
pub(crate) type Hook = Arc<dyn Fn(HookPoint) + Send + Sync>;

/// Points of maintenance's pack handling where tests interleave other work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HookPoint {
    /// Between listing the pack files and taking the busy set
    /// (`local::gc::pack_files`).
    Snapshot,
    /// A pack of maintenance's own was created and pinned
    /// ([`LocalPacks::writer`], on a blocking thread).
    MaintenancePack,
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
                pending: Mutex::new(HashMap::new()),
                generation: AtomicU64::new(0),
                #[cfg(test)]
                hook: Mutex::new(None),
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
    fn append(&self, bodies: &[Vec<u8>]) -> Result<(Vec<Placed>, u64)> {
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

    /// Append and sync (blocking). The appends count as pending until
    /// [`PackFiles::settle`] reports their index rows written.
    pub(crate) fn write_blocking(&self, bodies: &[Vec<u8>]) -> Result<Vec<([u8; 16], u64)>> {
        let (placed, seq) = {
            // Registered before the bytes exist anywhere a scan could see
            // them: a pack is busy from its first append on.
            let mut pending = self.inner.pending.lock();
            let (placed, seq) = self.append(bodies)?;
            for (pack, off) in &placed {
                pending.entry(*pack).or_default().insert(*off);
            }
            (placed, seq)
        };
        self.sync_upto(seq)?;
        Ok(placed)
    }

    /// Packs that may still gain index rows from writers: the active pack
    /// and those holding pending appends (see the module docs).
    pub(crate) fn busy(&self) -> HashSet<[u8; 16]> {
        let pending = self.inner.pending.lock();
        let mut out: HashSet<[u8; 16]> =
            pending.iter().filter(|(_, offs)| !offs.is_empty()).map(|(p, _)| *p).collect();
        if let Some(a) = &self.inner.active.lock().0 {
            out.insert(a.id);
        }
        out
    }

    /// Start a new pack of maintenance's own (blocking): never the active
    /// pack; busy (pinned) until the [`PackPin`] that [`PackWriter::finish`]
    /// returns is dropped, so that a later maintenance run cannot delete it
    /// before its rows are written.
    pub(crate) fn writer(&self) -> Result<PackWriter> {
        // Created and pinned in one critical section of `pending`: busy
        // from before its file exists (see the module docs).
        let mut pending = self.inner.pending.lock();
        let a = self.create()?;
        pending.entry(a.id).or_default().insert(PIN);
        drop(pending);
        let pin = PackPin { packs: self.clone(), pack: a.id };
        self.hook(HookPoint::MaintenancePack);
        Ok(PackWriter { id: a.id, file: a.file, len: a.len, pin })
    }

    /// Install (or remove) the test hook.
    #[cfg(test)]
    pub(crate) fn set_hook(&self, hook: Option<Hook>) {
        *self.inner.hook.lock() = hook;
    }

    /// Run the test hook at `point` (nothing outside tests).
    #[cfg(test)]
    pub(crate) fn hook(&self, point: HookPoint) {
        let hook = self.inner.hook.lock().clone();
        if let Some(h) = hook {
            h(point);
        }
    }

    #[cfg(not(test))]
    #[inline(always)]
    pub(crate) fn hook(&self, _point: HookPoint) {}

    /// Unlink packs (blocking) that `local::gc::delete_packs` found not busy
    /// and without index rows on the writer thread (the invariant of the
    /// module docs). The generation moves first, so readers that looked up
    /// a location in one of them retry.
    pub(crate) fn delete(&self, ids: &[[u8; 16]]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        self.inner.generation.fetch_add(1, Ordering::SeqCst);
        self.forget(ids);
        for pack in ids {
            match std::fs::remove_file(self.path_of(pack)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io(e)),
            }
        }
        sync_dir(&self.inner.dir).map_err(io)
    }
}

/// The pending "offset" a pin registers (never a real append's offset).
const PIN: u64 = u64::MAX;

/// Keeps a pack busy (see [`LocalPacks::writer`]).
pub(crate) struct PackPin {
    packs: LocalPacks,
    pack: [u8; 16],
}

impl Drop for PackPin {
    fn drop(&mut self) {
        self.packs.settle(&[(self.pack, PIN)]);
    }
}

/// A pack written by maintenance (compaction, repacking): appended to, then
/// made durable by [`PackWriter::finish`] before any index row points into it.
pub(crate) struct PackWriter {
    id: [u8; 16],
    file: Arc<File>,
    len: u64,
    pin: PackPin,
}

impl PackWriter {
    /// Append `body` (blocking); returns its offset.
    pub(crate) fn append(&mut self, body: &[u8]) -> Result<u64> {
        let at = self.len;
        self.file.write_all_at(body, at).map_err(io)?;
        self.len += body.len() as u64;
        Ok(at)
    }

    /// Make the pack durable (blocking); it stays busy while the returned
    /// pin lives.
    pub(crate) fn finish(self) -> Result<([u8; 16], PackPin)> {
        self.file.sync_data().map_err(io)?;
        Ok((self.id, self.pin))
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

    fn settle(&self, placed: &[([u8; 16], u64)]) {
        let mut pending = self.inner.pending.lock();
        for (pack, off) in placed {
            if let Some(offs) = pending.get_mut(pack) {
                offs.remove(off);
                if offs.is_empty() {
                    pending.remove(pack);
                }
            }
        }
    }

    fn generation(&self) -> u64 {
        self.inner.generation.load(Ordering::SeqCst)
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
