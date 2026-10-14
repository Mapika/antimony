//! The copy-on-write layer: path/inode bookkeeping and every mutation.
//!
//! Namespace operations run under the `overlay` mutex against the in-memory
//! base tree and whiteouts (loaded once at mount); mutations also hold the
//! state lock so `status`/`reset` in other processes see whole operations.
//! Reads and writes on open upper files use only the `handles` map.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::SystemTime;

use ant_core::{EntryMode, ObjectId, ObjectKind, Tree, TreeEntry};
use fuser::*;
use parking_lot::{Condvar, Mutex, RwLock};

use crate::filesystem::{State, View};

use super::journal::{COMPACT_BYTES, Journal, Record, compact, load};
use super::state::*;
use super::{error, file_kind, fuse_error, hidden, metadata, mode, other, under};

pub(super) static PROBE: [AtomicU64; 6] = [const { AtomicU64::new(0) }; 6];
struct Probe(std::time::Instant, bool);
impl Drop for Probe {
    fn drop(&mut self) {
        let i = if self.1 { 2 } else { 3 };
        PROBE[i].fetch_add(self.0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        PROBE[i + 2].fetch_add(1, Ordering::Relaxed);
    }
}
pub(super) struct Overlay {
    pub(super) paths: HashMap<INodeNo, String>,
    pub(super) directories: HashMap<u64, Vec<(INodeNo, FileType, String)>>,
    root: ObjectId,
    deleted: BTreeSet<String>,
    /// Journal size as of this mount's last read or append; a different
    /// size on disk means another process (`reset`) appended.
    journal_len: u64,
}

pub(super) enum OpenFile {
    Upper(File),
    Lower(ObjectId),
}
pub(super) struct Handle {
    pub(super) file: OpenFile,
    pub(super) writable: bool,
    /// The inode left the namespace (unlinked or replaced): never reattach.
    pub(super) detached: AtomicBool,
    /// Write ordering for the inode (upper files only).
    pub(super) pending: Option<Arc<Pending>>,
}

/// Writes are acknowledged before they reach the upper file, so the next
/// request's trip through the kernel (and several writes' copies into the
/// page cache) overlap. Every upper handle of an inode shares one `Pending`
/// tracking the byte ranges in flight: a write waits only for overlapping
/// in-flight writes (so rewrites of a region apply in order), and anything
/// that observes the file's content or size (reads, attributes, flush,
/// fsync, truncate) waits until nothing is in flight, so no one can see a
/// write missing. An I/O error after the reply is kept and reported by the
/// next write, flush or fsync (like a writeback error). A daemon crash can
/// lose such writes unless the file was closed (flushed) or synced since:
/// unsynced data, as documented in `rw`.
#[derive(Default)]
pub(super) struct Pending {
    state: Mutex<InFlight>,
    idle: Condvar,
}
#[derive(Default)]
struct InFlight {
    ranges: Vec<(u64, u64)>,
    error: Option<io::Error>,
}
impl Pending {
    /// Register a write of `len` bytes at `offset`, after overlapping ones.
    pub(super) fn begin(&self, offset: u64, len: usize) -> io::Result<()> {
        let end = offset.saturating_add(len as u64);
        let mut s = self.state.lock();
        if let Some(e) = s.error.take() {
            return Err(e);
        }
        while s.ranges.iter().any(|&(a, b)| a < end && offset < b) {
            self.idle.wait(&mut s);
        }
        s.ranges.push((offset, end));
        Ok(())
    }
    pub(super) fn end(&self, offset: u64, len: usize, result: io::Result<()>) {
        let end = offset.saturating_add(len as u64);
        let mut s = self.state.lock();
        if let Some(i) = s.ranges.iter().position(|&r| r == (offset, end)) {
            s.ranges.swap_remove(i);
        }
        if let Err(e) = result {
            s.error.get_or_insert(e);
        }
        drop(s);
        self.idle.notify_all();
    }
    /// Wait until no acknowledged write is in flight.
    pub(super) fn settle(&self) {
        let mut s = self.state.lock();
        while !s.ranges.is_empty() {
            self.idle.wait(&mut s);
        }
    }
    /// Settle and report (once) a write error that happened after its reply.
    pub(super) fn written(&self) -> io::Result<()> {
        let mut s = self.state.lock();
        while !s.ranges.is_empty() {
            self.idle.wait(&mut s);
        }
        s.error.take().map_or(Ok(()), Err)
    }
    pub(super) fn fail(&self, e: io::Error) {
        self.state.lock().error.get_or_insert(e);
    }
}

pub(super) struct Writable {
    pub(super) _mount_lock: MountGuard,
    pub(super) lower: View,
    pub(super) dir: PathBuf,
    pub(super) overlay: Mutex<Overlay>,
    pub(super) handles: RwLock<HashMap<u64, Arc<Handle>>>,
    files: Mutex<HashMap<INodeNo, std::sync::Weak<Pending>>>,
    next_handle: AtomicU64,
    pub(super) journal: Arc<Journal>,
    flusher: Mutex<Option<std::thread::JoinHandle<()>>>,
}
pub(super) struct Operation<'a> {
    pub(super) view: &'a Writable,
    pub(super) paths: &'a mut HashMap<INodeNo, String>,
    pub(super) directories: &'a mut HashMap<u64, Vec<(INodeNo, FileType, String)>>,
    pub(super) cache: &'a mut State,
    pub(super) root: ObjectId,
    pub(super) deleted: &'a mut BTreeSet<String>,
    journal_len: &'a mut u64,
}

impl Writable {
    /// Recover the state directory, fold the journal into the snapshots and
    /// load base and whiteouts. The caller holds the exclusive mount guard.
    pub(super) fn open(mount_lock: MountGuard, lower: View, dir: PathBuf) -> io::Result<Self> {
        let journal = Journal::open(&dir)?;
        let persisted = {
            let _lock = journal.lock()?;
            recover(&dir)?;
            compact(&dir, journal.file())?
        };
        let journal = Arc::new(journal);
        let flusher = journal.spawn_flusher()?;
        Ok(Self {
            _mount_lock: mount_lock,
            lower,
            overlay: Mutex::new(Overlay {
                paths: HashMap::from([(INodeNo(1), String::new())]),
                directories: HashMap::new(),
                root: persisted.tree,
                deleted: persisted.deleted,
                journal_len: persisted.len,
            }),
            dir,
            handles: RwLock::new(HashMap::new()),
            files: Mutex::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
            journal,
            flusher: Mutex::new(Some(flusher)),
        })
    }

    /// A namespace operation that changes nothing on disk.
    pub(super) fn run<T>(
        &self,
        f: impl FnOnce(&mut Operation<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        self.operate(false, f)
    }
    /// A namespace operation that may change the state directory.
    pub(super) fn run_mut<T>(
        &self,
        f: impl FnOnce(&mut Operation<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        self.operate(true, f)
    }
    fn operate<T>(
        &self,
        exclusive: bool,
        f: impl FnOnce(&mut Operation<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        let t0 = std::time::Instant::now();
        let mut overlay = self.overlay.lock();
        let t1 = std::time::Instant::now();
        let _lock = if exclusive { Some(self.journal.lock()?) } else { None };
        let t2 = std::time::Instant::now();
        let _probe = Probe(t1, exclusive);
        PROBE[0].fetch_add((t1 - t0).as_nanos() as u64, Ordering::Relaxed);
        PROBE[1].fetch_add((t2 - t1).as_nanos() as u64, Ordering::Relaxed);
        if self.journal.size()? != overlay.journal_len {
            // `reset` replaced the base (or a crashed writer left a tail).
            let _relock = if exclusive { None } else { Some(self.journal.lock()?) };
            recover(&self.dir)?;
            let p = load(&self.dir)?;
            overlay.root = p.tree;
            overlay.deleted = p.deleted;
            overlay.journal_len = p.len;
        }
        let Overlay { paths, directories, root, deleted, journal_len } = &mut *overlay;
        let result = f(&mut Operation {
            view: self,
            paths,
            directories,
            cache: &mut self.lower.state.lock(),
            root: *root,
            deleted,
            journal_len,
        });
        if exclusive && overlay.journal_len > COMPACT_BYTES {
            compact(&self.dir, self.journal.file())?;
            self.journal.compacted();
            overlay.journal_len = 0;
        }
        result
    }

    pub(super) fn probe_report() {
        let v: Vec<u64> = PROBE.iter().map(|a| a.load(Ordering::Relaxed)).collect();
        eprintln!(
            "PROBE wait_ms={} flock_ms={} hold_excl_ms={} hold_ro_ms={} n_excl={} n_ro={}",
            v[0] / 1000000,
            v[1] / 1000000,
            v[2] / 1000000,
            v[3] / 1000000,
            v[4],
            v[5]
        );
    }
    pub(super) fn handle(&self, fh: FileHandle) -> io::Result<Arc<Handle>> {
        self.handles.read().get(&fh.0).cloned().ok_or_else(|| error(libc::EBADF))
    }
    pub(super) fn insert_handle(&self, ino: INodeNo, file: OpenFile, writable: bool) -> FileHandle {
        let pending = match file {
            OpenFile::Upper(_) => {
                let mut files = self.files.lock();
                let slot = files.entry(ino).or_default();
                Some(slot.upgrade().unwrap_or_else(|| {
                    let p = Arc::new(Pending::default());
                    *slot = Arc::downgrade(&p);
                    p
                }))
            }
            OpenFile::Lower(_) => None,
        };
        let fh = self.next_handle.fetch_add(1, Ordering::Relaxed);
        let handle = Arc::new(Handle { file, writable, detached: AtomicBool::new(false), pending });
        self.handles.write().insert(fh, handle);
        FileHandle(fh)
    }
    pub(super) fn release_handle(&self, ino: INodeNo, fh: FileHandle) {
        let handle = self.handles.write().remove(&fh.0);
        if handle.is_some_and(|h| h.pending.is_some()) {
            let mut files = self.files.lock();
            if files.get(&ino).is_some_and(|w| w.strong_count() == 0) {
                files.remove(&ino);
            }
        }
    }
    /// Wait until acknowledged writes to `ino` have reached its upper file.
    pub(super) fn settle(&self, ino: INodeNo) {
        let pending = self.files.lock().get(&ino).and_then(std::sync::Weak::upgrade);
        if let Some(p) = pending {
            p.settle();
        }
    }
    pub(super) fn next_handle(&self) -> u64 {
        self.next_handle.fetch_add(1, Ordering::Relaxed)
    }

    /// Writes to an open upper file normally touch only the file. `reset`
    /// may prune an upper file equal to the base while it is open; the
    /// writes then land in the unlinked inode. Detected after each write
    /// (and on flush/fsync/truncate) by `nlink == 0`, the file is published
    /// again at its path, with everything written to it.
    pub(super) fn check_attached(
        &self,
        ino: INodeNo,
        fh: FileHandle,
        handle: &Arc<Handle>,
        write: Option<(u64, &[u8])>,
    ) -> io::Result<()> {
        let OpenFile::Upper(f) = &handle.file else {
            return Ok(());
        };
        if !handle.writable || handle.detached.load(Ordering::Relaxed) || f.metadata()?.nlink() > 0
        {
            return Ok(());
        }
        self.run_mut(|o| o.reattach(ino, fh, handle, write))
    }

    /// Final compaction and sync at unmount.
    pub(super) fn shutdown(&self) -> io::Result<()> {
        Self::probe_report();
        self.journal.stop();
        if let Some(flusher) = self.flusher.lock().take() {
            let _ = flusher.join();
        }
        let mut overlay = self.overlay.lock();
        let _lock = self.journal.lock()?;
        recover(&self.dir)?;
        compact(&self.dir, self.journal.file())?;
        self.journal.compacted();
        overlay.journal_len = 0;
        self.journal.sync()
    }
}
impl Drop for Writable {
    fn drop(&mut self) {
        self.journal.stop();
        if let Some(flusher) = self.flusher.lock().take() {
            let _ = flusher.join();
        }
    }
}

impl Operation<'_> {
    pub(super) fn path(&self, ino: INodeNo) -> io::Result<String> {
        self.paths.get(&ino).cloned().ok_or_else(|| error(libc::ENOENT))
    }
    pub(super) fn active(&self, mut ino: INodeNo) -> bool {
        while ino != INodeNo(1) {
            let Some(path) = self.paths.get(&ino) else {
                return false;
            };
            let parent = self.cache.nodes[(ino.0 - 1) as usize].parent;
            let name = path.rsplit('/').next().unwrap_or("");
            if self.cache.paths.get(&(parent, name.into())) != Some(&ino) {
                return false;
            }
            ino = parent;
        }
        true
    }
    pub(super) fn child(&self, parent: INodeNo, name: &OsStr) -> io::Result<String> {
        let name = name.to_str().ok_or_else(|| error(libc::EINVAL))?;
        if name.is_empty() || name.contains(['/', '\n']) || name == "." || name == ".." {
            return Err(error(libc::EINVAL));
        }
        let path = self.path(parent)?;
        Ok(if path.is_empty() { name.into() } else { format!("{path}/{name}") })
    }
    pub(super) fn upper(&self, path: &str) -> PathBuf {
        self.view.dir.join("upper").join(path)
    }
    pub(super) fn tree(&mut self, id: ObjectId) -> io::Result<Arc<Tree>> {
        self.view.lower.cached_tree(self.cache, id).map_err(fuse_error)
    }
    pub(super) fn lower(&mut self, path: &str) -> io::Result<Option<TreeEntry>> {
        let mut entry = TreeEntry { name: String::new(), mode: EntryMode::Tree, id: self.root };
        for part in path.split('/').filter(|s| !s.is_empty()) {
            if entry.mode != EntryMode::Tree {
                return Ok(None);
            }
            let tree = self.tree(entry.id)?;
            let Some(next) = tree.get(part) else {
                return Ok(None);
            };
            entry = next.clone();
        }
        Ok(Some(entry))
    }
    /// Whether `path` currently shows (or would show) a lower-layer entry,
    /// i.e. whether removing it needs a whiteout.
    fn lower_visible(&mut self, path: &str) -> io::Result<bool> {
        Ok(!hidden(self.deleted, path) && self.lower(path)?.is_some())
    }
    fn upper_entry(path: &str, m: &fs::Metadata) -> TreeEntry {
        TreeEntry {
            name: path.rsplit('/').next().unwrap_or("").into(),
            mode: mode(m),
            id: ObjectId([0; 32]),
        }
    }
    pub(super) fn entry(&mut self, path: &str) -> io::Result<TreeEntry> {
        if let Some(m) = metadata(&self.upper(path))? {
            return Ok(Self::upper_entry(path, &m));
        }
        self.lower_entry(path)
    }
    fn lower_entry(&mut self, path: &str) -> io::Result<TreeEntry> {
        if hidden(self.deleted, path) {
            return Err(error(libc::ENOENT));
        }
        self.lower(path)?.ok_or_else(|| error(libc::ENOENT))
    }
    pub(super) fn inode(&mut self, parent: INodeNo, name: &OsStr) -> io::Result<INodeNo> {
        if name == "." {
            return Ok(parent);
        }
        if name == ".." {
            return Ok(self.cache.nodes[(parent.0 - 1) as usize].parent);
        }
        let path = self.child(parent, name)?;
        let entry = self.entry(&path)?;
        Ok(self.bind(parent, path, &entry))
    }
    fn bind(&mut self, parent: INodeNo, path: String, entry: &TreeEntry) -> INodeNo {
        if let Some(ino) = self.cache.paths.get(&(parent, entry.name.clone()))
            && file_kind(self.cache.nodes[(ino.0 - 1) as usize].mode) != file_kind(entry.mode)
        {
            self.cache.paths.remove(&(parent, entry.name.clone()));
        }
        let ino = View::inode(self.cache, parent, entry);
        self.paths.insert(ino, path);
        ino
    }
    pub(super) fn blob(&mut self, id: ObjectId) -> io::Result<bytes::Bytes> {
        self.view.lower.blob(self.cache, id).map_err(fuse_error)
    }
    pub(super) fn attr(&mut self, ino: INodeNo) -> io::Result<FileAttr> {
        let path = self.path(ino)?;
        self.view.settle(ino);
        let m = metadata(&self.upper(&path))?;
        let entry = match &m {
            Some(m) => Self::upper_entry(&path, m),
            None => self.lower_entry(&path)?,
        };
        let size = if let Some(m) = &m {
            m.len()
        } else if entry.mode.is_blob() {
            self.blob(entry.id)?.len() as u64
        } else {
            0
        };
        Ok(self.file_attr(ino, &entry, m.as_ref(), size))
    }
    fn file_attr(
        &self,
        ino: INodeNo,
        entry: &TreeEntry,
        m: Option<&fs::Metadata>,
        size: u64,
    ) -> FileAttr {
        let time = SystemTime::UNIX_EPOCH;
        FileAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime: m.and_then(|m| m.accessed().ok()).unwrap_or(time),
            mtime: m.and_then(|m| m.modified().ok()).unwrap_or(time),
            ctime: m.and_then(|m| m.modified().ok()).unwrap_or(time),
            crtime: time,
            kind: file_kind(entry.mode),
            perm: m.map(|m| (m.mode() & 0o7777) as u16).unwrap_or(match entry.mode {
                EntryMode::File => 0o644,
                EntryMode::Symlink => 0o777,
                _ => 0o755,
            }),
            nlink: if file_kind(entry.mode) == FileType::Directory { 2 } else { 1 },
            // SAFETY: these calls have no preconditions and cannot fail.
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }
    pub(super) fn children(&mut self, path: &str) -> io::Result<BTreeMap<String, EntryMode>> {
        if file_kind(self.entry(path)?.mode) != FileType::Directory {
            return Err(error(libc::ENOTDIR));
        }
        let mut out = BTreeMap::new();
        if !hidden(self.deleted, path)
            && let Some(entry) = self.lower(path)?
            && entry.mode == EntryMode::Tree
        {
            let tree = self.tree(entry.id)?;
            for e in tree.entries() {
                let child =
                    if path.is_empty() { e.name.clone() } else { format!("{path}/{}", e.name) };
                if !hidden(self.deleted, &child) {
                    out.insert(e.name.clone(), e.mode);
                }
            }
        }
        if let Some(m) = metadata(&self.upper(path))?
            && m.is_dir()
        {
            for e in fs::read_dir(self.upper(path))? {
                let e = e?;
                let name = e.file_name().into_string().map_err(|_| error(libc::EINVAL))?;
                out.insert(name, mode(&fs::symlink_metadata(e.path())?));
            }
        }
        Ok(out)
    }

    /// Order an upper-directory change after pending journal records.
    fn physical(&self) -> io::Result<()> {
        self.view.journal.before_physical()
    }
    fn dirty(&self, path: &Path) {
        self.view.journal.dirty(path);
    }
    fn record(&mut self, record: Record) -> io::Result<()> {
        self.view.journal.append(self.journal_len, &record)
    }

    /// Publish `data` at `full` only once it is complete and durable: a crash
    /// leaves either no upper file (the lower one shows) or the whole copy,
    /// never a prefix shadowing the lower file.
    fn publish(&self, full: &Path, data: &[u8], perm: u32) -> io::Result<()> {
        let temp = self.view.dir.join("copy.tmp");
        let mut f = File::create(&temp)?;
        f.write_all(data)?;
        f.set_permissions(fs::Permissions::from_mode(perm))?;
        f.sync_all()?;
        fs::rename(temp, full)?;
        self.dirty(full);
        Ok(())
    }
    pub(super) fn copy_up(&mut self, path: &str) -> io::Result<PathBuf> {
        let full = self.upper(path);
        if metadata(&full)?.is_some() {
            return Ok(full);
        }
        let entry = self.entry(path)?;
        if let Some((parent, _)) = path.rsplit_once('/') {
            self.copy_up(parent)?;
        }
        self.physical()?;
        match entry.mode {
            EntryMode::Tree | EntryMode::Gitlink => {
                fs::create_dir(&full)?;
                self.dirty(&full);
            }
            EntryMode::Symlink => {
                symlink(OsStr::from_bytes(&self.blob(entry.id)?), &full)?;
                self.dirty(&full);
            }
            _ => {
                let data = self.blob(entry.id)?;
                self.publish(&full, &data, exec_perm(entry.mode))?;
            }
        }
        Ok(full)
    }
    pub(super) fn recursive_copy(&mut self, path: &str) -> io::Result<()> {
        if let Some((parent, _)) = path.rsplit_once('/') {
            self.copy_up(parent)?;
        }
        let mut todo = vec![path.to_owned()];
        let mut files = Vec::new();
        while !todo.is_empty() {
            let mut ids = Vec::new();
            for path in &todo {
                if !hidden(self.deleted, path)
                    && let Some(e) = self.lower(path)?
                    && e.mode == EntryMode::Tree
                {
                    ids.push(e.id);
                }
            }
            self.view.lower.cache_trees(self.cache, &ids).map_err(fuse_error)?;
            let mut next = Vec::new();
            for path in todo {
                let entry = self.entry(&path)?;
                if file_kind(entry.mode) == FileType::Directory {
                    self.copy_up(&path)?;
                    for name in self.children(&path)?.keys() {
                        next.push(format!("{path}/{name}"));
                    }
                } else if metadata(&self.upper(&path))?.is_none() {
                    files.push((path, entry));
                }
            }
            todo = next;
        }
        self.physical()?;
        for batch in files.chunks(512) {
            let ids = batch.iter().map(|(_, e)| e.id).collect::<Vec<_>>();
            let objects = self.view.lower.batch_objects(&ids).map_err(fuse_error)?;
            for ((path, entry), object) in batch.iter().zip(objects) {
                if object.kind() != ObjectKind::Blob {
                    return Err(other("expected blob during copy-up"));
                }
                let full = self.upper(path);
                if entry.mode == EntryMode::Symlink {
                    symlink(OsStr::from_bytes(object.body()), &full)?;
                    self.dirty(&full);
                } else {
                    self.publish(&full, object.body(), exec_perm(entry.mode))?;
                }
            }
        }
        Ok(())
    }
    pub(super) fn detach(&mut self, parent: INodeNo, name: &OsStr) {
        if let Some(name) = name.to_str() {
            self.cache.paths.remove(&(parent, name.into()));
        }
    }
    pub(super) fn remove(
        &mut self,
        parent: INodeNo,
        name: &OsStr,
        directory: bool,
    ) -> io::Result<()> {
        let path = self.child(parent, name)?;
        let is_dir = file_kind(self.entry(&path)?.mode) == FileType::Directory;
        if directory != is_dir {
            return Err(error(if directory { libc::ENOTDIR } else { libc::EISDIR }));
        }
        if is_dir && !self.children(&path)?.is_empty() {
            return Err(error(libc::ENOTEMPTY));
        }
        // Whiteout first: a crash in between leaves the upper copy, which
        // wins over the whiteout, i.e. the state before the removal.
        if self.lower_visible(&path)? {
            self.record(Record::Whiteout(path.clone()))?;
            self.deleted.insert(path.clone());
        }
        let full = self.upper(&path);
        if let Some(m) = metadata(&full)? {
            self.physical()?;
            if m.is_dir() {
                fs::remove_dir(&full)?;
            } else {
                fs::remove_file(&full)?;
            }
            self.dirty(&full);
        }
        self.detach(parent, name);
        Ok(())
    }
    pub(super) fn create(
        &mut self,
        parent: INodeNo,
        name: &OsStr,
        perm: u32,
        target: Option<&Path>,
        directory: bool,
    ) -> io::Result<INodeNo> {
        Ok(self.create_entry(parent, name, perm, target, directory)?.0)
    }
    /// Create a file, directory or symlink. A new regular file is returned
    /// open for reading and writing.
    pub(super) fn create_entry(
        &mut self,
        parent: INodeNo,
        name: &OsStr,
        perm: u32,
        target: Option<&Path>,
        directory: bool,
    ) -> io::Result<(INodeNo, Option<File>, fs::Metadata)> {
        let path = self.child(parent, name)?;
        // An existing upper entry fails the exclusive create below (EEXIST).
        match self.lower_entry(&path) {
            Ok(_) => return Err(error(libc::EEXIST)),
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {}
            Err(e) => return Err(e),
        }
        self.copy_up(&self.path(parent)?)?;
        self.physical()?;
        let full = self.upper(&path);
        let (file, m) = if let Some(target) = target {
            symlink(target, &full)?;
            (None, fs::symlink_metadata(&full)?)
        } else if directory {
            fs::create_dir(&full)?;
            fs::set_permissions(&full, fs::Permissions::from_mode(perm))?;
            (None, fs::symlink_metadata(&full)?)
        } else {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(perm)
                .open(&full)?;
            let mut m = f.metadata()?;
            if m.mode() & 0o7777 != perm & 0o7777 {
                // The daemon's umask narrowed the mode.
                f.set_permissions(fs::Permissions::from_mode(perm))?;
                m = f.metadata()?;
            }
            (Some(f), m)
        };
        self.dirty(&full);
        let ino = self.bind(parent, path.clone(), &Self::upper_entry(&path, &m));
        Ok((ino, file, m))
    }
    pub(super) fn created_attr(&self, ino: INodeNo, m: &fs::Metadata) -> io::Result<FileAttr> {
        let path = self.path(ino)?;
        Ok(self.file_attr(ino, &Self::upper_entry(&path, m), Some(m), m.len()))
    }
    pub(super) fn open(&mut self, ino: INodeNo, flags: i32) -> io::Result<FileHandle> {
        let path = self.path(ino)?;
        let entry = self.entry(&path)?;
        if file_kind(entry.mode) != FileType::RegularFile {
            return Err(error(libc::EISDIR));
        }
        let write = flags & libc::O_ACCMODE != libc::O_RDONLY;
        let file = if write || metadata(&self.upper(&path))?.is_some() {
            let full = self.copy_up(&path)?;
            let f = OpenOptions::new()
                .read(true)
                .write(write)
                .truncate(write && flags & libc::O_TRUNC != 0)
                .open(full)?;
            OpenFile::Upper(f)
        } else {
            OpenFile::Lower(entry.id)
        };
        Ok(self.view.insert_handle(ino, file, write))
    }
    /// See [`Writable::check_attached`].
    fn reattach(
        &mut self,
        ino: INodeNo,
        fh: FileHandle,
        handle: &Arc<Handle>,
        write: Option<(u64, &[u8])>,
    ) -> io::Result<()> {
        let current = self.view.handles.read().get(&fh.0).cloned();
        let Some(current) = current else {
            return Ok(());
        };
        if !Arc::ptr_eq(&current, handle) {
            // Another thread already reattached: repeat this write there.
            if let (OpenFile::Upper(f), Some((offset, data))) = (&current.file, write) {
                f.write_all_at(data, offset)?;
            }
            return Ok(());
        }
        let OpenFile::Upper(orphan) = &handle.file else {
            return Ok(());
        };
        // Copy the orphan only once concurrent acknowledged writes are in.
        if let Some(p) = &handle.pending {
            p.settle();
        }
        if !self.active(ino) {
            handle.detached.store(true, Ordering::Relaxed);
            return Ok(());
        }
        let path = self.path(ino)?;
        let full = self.upper(&path);
        let replaced = metadata(&full)?.is_some();
        if !replaced {
            if let Some((parent, _)) = path.rsplit_once('/') {
                self.copy_up(parent)?;
            }
            self.physical()?;
            let m = orphan.metadata()?;
            let mut data = vec![0; usize::try_from(m.len()).map_err(other)?];
            let mut read = 0;
            while read < data.len() {
                match orphan.read_at(&mut data[read..], read as u64)? {
                    0 => break,
                    n => read += n,
                }
            }
            data.truncate(read);
            self.publish(&full, &data, m.mode() & 0o7777)?;
        }
        let file = OpenOptions::new().read(true).write(true).open(&full)?;
        if replaced && let Some((offset, data)) = write {
            file.write_all_at(data, offset)?;
        }
        let new = Arc::new(Handle {
            file: OpenFile::Upper(file),
            writable: true,
            detached: AtomicBool::new(false),
            pending: handle.pending.clone(),
        });
        self.view.handles.write().insert(fh.0, new);
        Ok(())
    }
    pub(super) fn rename(
        &mut self,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
    ) -> io::Result<()> {
        if flags.bits() & !1 != 0 {
            return Err(error(libc::EINVAL));
        }
        let old = self.child(parent, name)?;
        let new = self.child(newparent, newname)?;
        let src = self.entry(&old)?;
        if old == new {
            return Ok(());
        }
        if under(&new, &old) {
            return Err(error(libc::EINVAL));
        }
        match self.entry(&new) {
            Ok(dst) => {
                if flags.bits() & 1 != 0 {
                    return Err(error(libc::EEXIST));
                }
                let a = file_kind(src.mode) == FileType::Directory;
                let b = file_kind(dst.mode) == FileType::Directory;
                if a != b {
                    return Err(error(if a { libc::ENOTDIR } else { libc::EISDIR }));
                }
                if b && !self.children(&new)?.is_empty() {
                    return Err(error(libc::ENOTEMPTY));
                }
            }
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {}
            Err(e) => return Err(e),
        }
        let ino = self.inode(parent, name)?;
        // A source showing lower content needs whiteouts and a full copy;
        // an upper-only source (new files, build outputs) is a plain rename:
        // the destination's upper entry shadows any lower one, and an empty
        // destination directory has all its lower children hidden already.
        let lower = self.lower_visible(&old)?;
        if lower {
            self.recursive_copy(&old)?;
        }
        self.copy_up(&self.path(newparent)?)?;
        let (from, to) = (self.upper(&old), self.upper(&new));
        if lower {
            self.record(Record::Rename { old: old.clone(), new: new.clone() })?;
            self.deleted.insert(old.clone());
            self.deleted.insert(new.clone());
        }
        self.physical()?;
        fs::rename(&from, &to)?;
        self.dirty(&from);
        self.dirty(&to);
        if lower {
            self.record(Record::Done)?;
        }
        self.detach(parent, name);
        self.detach(newparent, newname);
        self.cache.paths.insert((newparent, newname.to_string_lossy().into_owned()), ino);
        self.cache.nodes[(ino.0 - 1) as usize].parent = newparent;
        for path in self.paths.values_mut() {
            if under(path, &old) {
                *path = format!("{new}{}", &path[old.len()..]);
            }
        }
        Ok(())
    }
}

fn exec_perm(mode: EntryMode) -> u32 {
    if mode == EntryMode::Executable { 0o755 } else { 0o644 }
}
