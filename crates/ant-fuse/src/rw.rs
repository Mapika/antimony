//! Writable lazy workspaces: a copy-on-write layer implemented inside our
//! FUSE filesystem (no kernel overlayfs, no privileges needed).
//!
//! TASK (rw-fuse): implement `mount_rw` and `changes`, then add
//! `ant ws new --lazy <dir>` (mounts in a detached background process that
//! survives the CLI; `ant ws unmount <dir>`), and make `status`/`submit`/`land`
//! work on lazy workspaces via `changes` (land's reset = replace `base_tree`
//! and clear the upper layer entries that now match the new base).
//!
//! Layout of the upper (state) directory, which must live OUTSIDE the
//! mountpoint (e.g. `~/.cache/antimony/ws/<hash of mountpoint>/`):
//! - `upper/` mirrors the repo tree for every file/dir created or modified
//!   (copy-up on first write: whole file copied from the lower layer).
//! - `whiteouts`: newline-separated repo paths deleted from the lower layer
//!   (a deleted dir hides everything below it). Persist durably (write temp +
//!   rename) after each mutation; tolerate crashes between ops.
//! - `base`: `base_commit=<hex>\nbase_tree=<hex>\n`.
//!
//! Semantics (POSIX enough for compilers, editors, git-like tools):
//! - Reads: upper wins, whiteouts hide lower, else lower (lazy, as in the
//!   read-only view; reuse its inode/caching code).
//! - Supported mutations: create, write, truncate/setattr size, chmod (only
//!   the exec bit is meaningful; store others), mkdir, rmdir (ENOTEMPTY
//!   honored across layers), unlink, rename (files and directories; a lower
//!   directory rename may copy up recursively in v1), symlink, readlink,
//!   fsync, utimens (mtime stored in upper). Hard links → EPERM.
//! - Editor pattern "write temp + rename over original" must work.
//! - Inode numbers stay stable across copy-up for the life of the mount.
//! - Concurrency: many processes reading/writing different files at once.
//!
//! `changes(objects, state_dir)` → `Vec<antd::workspace::LocalChange>`-shaped
//! results (path, kind, new (mode, blob id)), sorted by path, computed from the
//! upper dir + whiteouts only — cost proportional to the number of changed
//! paths, never to repo size. Files copied up but byte-identical to base (and
//! same mode) are NOT changes. New blob contents are put into `objects`.
//!
//! Tests (real FUSE mounts, multi-thread runtime, like `lib.rs` tests):
//! read-through of untouched files; edit-in-place; temp+rename; delete file;
//! delete dir then recreate a file inside; rename lower dir; chmod +x;
//! symlink create; `changes` matches the expected list in each case; a
//! 20k-file base tree where `changes` after editing 2 files reads ≤ a few
//! dozen objects (count via a wrapping ObjectStore); remount from the same
//! state dir preserves edits; concurrent writers (16 threads × 50 files).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, RepoPath, Tree, TreeEntry};
use ant_store::ObjectStore;
use antd::workspace::{LocalChange, LocalChangeKind};
use fuser::*;
use parking_lot::Mutex;

use crate::MountOptions;
use crate::filesystem::{State, View};

fn error(code: i32) -> io::Error {
    io::Error::from_raw_os_error(code)
}
fn fuse_error(e: Errno) -> io::Error {
    error(e.code())
}
fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

// Separate open file descriptions make flock serialize the daemon, status and
// reset, including callers in the same process. Never held on a Tokio worker.
struct StateLock(File);
impl StateLock {
    fn take(dir: &Path) -> io::Result<Self> {
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("lock"))?;
        // SAFETY: flock only consumes the valid descriptor, with no pointers.
        if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(f))
    }
}
impl Drop for StateLock {
    fn drop(&mut self) {
        // SAFETY: the file is still alive here.
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

// Exclusive "this state dir is mounted" marker. Within a process an exact
// registry decides, so the answer is correct the instant a previous mount's
// filesystem is dropped (i.e. once `umount_and_join` returns). Across processes
// a POSIX record lock decides. Deliberately not flock/OFD: those belong to the
// open file description, and another thread that forks (to spawn fusermount, or
// anything else) holds a copy of that description until it execs, so the lock
// would spuriously still look held right after unmount. Record locks are never
// inherited by a forked child. They are dropped when any descriptor of the file
// closes in this process, which is why only this type ever opens `mount.lock`.
static MOUNTED: std::sync::LazyLock<Mutex<std::collections::HashSet<PathBuf>>> =
    std::sync::LazyLock::new(Default::default);
struct MountGuard {
    file: Option<File>,
    dir: PathBuf,
}
impl MountGuard {
    fn take(dir: &Path) -> io::Result<Self> {
        let busy = || other("lazy workspace is already mounted");
        if !MOUNTED.lock().insert(dir.to_owned()) {
            return Err(busy());
        }
        // From here on the guard owns the registry entry, released on drop.
        let mut guard = Self {
            file: None,
            dir: dir.to_owned(),
        };
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("mount.lock"))?;
        // SAFETY: an all-zero flock is valid; the fields we need are set below.
        let mut lock: libc::flock = unsafe { std::mem::zeroed() };
        lock.l_type = libc::F_WRLCK as _;
        lock.l_whence = libc::SEEK_SET as _;
        // SAFETY: the descriptor is valid and `lock` outlives the call.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &lock) } != 0 {
            return Err(match io::Error::last_os_error().raw_os_error() {
                Some(libc::EACCES | libc::EAGAIN) => busy(),
                _ => io::Error::last_os_error(),
            });
        }
        guard.file = Some(file);
        Ok(guard)
    }
}
impl Drop for MountGuard {
    fn drop(&mut self) {
        // Release the record lock before the registry entry, so the next
        // mount never sees a stale lock.
        self.file = None;
        MOUNTED.lock().remove(&self.dir);
    }
}

fn atomic(dir: &Path, name: &str, data: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!(".{name}.tmp"));
    let mut f = File::create(&tmp)?;
    f.write_all(data)?;
    f.sync_all()?;
    fs::rename(tmp, dir.join(name))?;
    File::open(dir)?.sync_all()
}

/// Read the persisted base (commit, tree).
pub fn base(state: &Path) -> io::Result<(ObjectId, ObjectId)> {
    let text = fs::read_to_string(state.join("base"))?;
    let get = |key: &str| -> io::Result<ObjectId> {
        text.lines()
            .find_map(|l| l.strip_prefix(key))
            .ok_or_else(|| other("invalid lazy workspace base"))?
            .parse()
            .map_err(other)
    };
    Ok((get("base_commit=")?, get("base_tree=")?))
}

fn save_base(state: &Path, commit: ObjectId, tree: ObjectId) -> io::Result<()> {
    atomic(
        state,
        "base",
        format!("base_commit={commit}\nbase_tree={tree}\n").as_bytes(),
    )
}

fn whiteouts(state: &Path) -> io::Result<BTreeSet<String>> {
    let text = fs::read_to_string(state.join("whiteouts"))?;
    text.lines()
        .map(|p| {
            RepoPath::new(p).map_err(other)?;
            Ok(p.to_owned())
        })
        .collect()
}
fn save_whiteouts(state: &Path, paths: &BTreeSet<String>) -> io::Result<()> {
    let mut text = paths.iter().cloned().collect::<Vec<_>>().join("\n");
    if !text.is_empty() {
        text.push('\n');
    }
    atomic(state, "whiteouts", text.as_bytes())
}
// A small intent record closes the crash window between changing whiteouts
// and removing/renaming an upper entry. Recovery rolls the operation forward.
fn recover(state: &Path) -> io::Result<()> {
    let record = match fs::read_to_string(state.join("intent")) {
        Ok(s) => s,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let fields: Vec<_> = record.lines().collect();
    let (operation, paths) = fields
        .split_first()
        .ok_or_else(|| other("empty mutation intent"))?;
    if !matches!((*operation, paths.len()), ("delete", 1) | ("rename", 2)) {
        return Err(other("invalid mutation intent"));
    }
    for path in paths {
        if RepoPath::new(*path).map_err(other)?.is_root() {
            return Err(other("root mutation intent"));
        }
    }
    let mut deleted = whiteouts(state)?;
    deleted.extend(paths.iter().map(|p| p.to_string()));
    save_whiteouts(state, &deleted)?;
    let old = state.join("upper").join(paths[0]);
    if let Some(m) = metadata(&old)? {
        if *operation == "rename" {
            let new = state.join("upper").join(paths[1]);
            fs::rename(&old, &new)?;
            sync_parent(&new)?;
        } else if m.is_dir() {
            fs::remove_dir(&old)?;
        } else {
            fs::remove_file(&old)?;
        }
        sync_parent(&old)?;
    }
    fs::remove_file(state.join("intent"))?;
    File::open(state)?.sync_all()
}
fn sync_parent(path: &Path) -> io::Result<()> {
    File::open(path.parent().ok_or_else(|| other("path has no parent"))?)?.sync_all()
}
fn mutate(state: &Path, record: &str) -> io::Result<()> {
    atomic(state, "intent", record.as_bytes())?;
    recover(state)
}

fn under(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|s| s.starts_with('/'))
}
fn hidden(paths: &BTreeSet<String>, path: &str) -> bool {
    paths.contains(path)
        || path
            .match_indices('/')
            .any(|(i, _)| paths.contains(&path[..i]))
}
fn metadata(path: &Path) -> io::Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(m) => Ok(Some(m)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
fn mode(m: &fs::Metadata) -> EntryMode {
    if m.is_dir() {
        EntryMode::Tree
    } else if m.file_type().is_symlink() {
        EntryMode::Symlink
    } else if m.mode() & 0o111 != 0 {
        EntryMode::Executable
    } else {
        EntryMode::File
    }
}
fn file_kind(m: EntryMode) -> FileType {
    match m {
        EntryMode::Tree | EntryMode::Gitlink => FileType::Directory,
        EntryMode::Symlink => FileType::Symlink,
        _ => FileType::RegularFile,
    }
}

/// Mount a persistent copy-on-write workspace. `state_dir` must be outside
/// `mountpoint`. Existing state wins over the supplied initial base. Like
/// `mount`, this blocks and requires a live multi-thread Tokio runtime.
///
/// Do not `fork` (spawn helper processes) from the same process while its own
/// threads do I/O on this mount: the kernel can hand a concurrent direct-I/O
/// read stale buffer contents (see `KERNEL_GATE`). Daemon and clients in
/// separate processes, as the CLI does it, are unaffected.
pub fn mount_rw(
    objects: Arc<dyn ObjectStore>,
    base_commit: ObjectId,
    base_tree: ObjectId,
    mountpoint: &Path,
    state_dir: &Path,
    rt: tokio::runtime::Handle,
    opts: MountOptions,
) -> io::Result<BackgroundSession> {
    if rt.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
        return Err(other("ant-fuse requires a multi-threaded Tokio runtime"));
    }
    fs::create_dir_all(state_dir)?;
    let state_dir = fs::canonicalize(state_dir)?;
    if state_dir.starts_with(fs::canonicalize(mountpoint)?) {
        return Err(other("state directory must be outside mountpoint"));
    }
    let mount_lock = MountGuard::take(&state_dir)?;
    let lock = StateLock::take(&state_dir)?;
    fs::create_dir_all(state_dir.join("upper"))?;
    if !state_dir.join("base").exists() {
        save_base(&state_dir, base_commit, base_tree)?;
    }
    if !state_dir.join("whiteouts").exists() {
        save_whiteouts(&state_dir, &BTreeSet::new())?;
    }
    recover(&state_dir)?;
    let (_, root) = base(&state_dir)?;
    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::RW,
        MountOption::DefaultPermissions,
        MountOption::FSName(opts.fs_name.clone()),
    ];
    let view = Writable {
        _mount_lock: mount_lock,
        lower: View::new(objects, root, rt, opts),
        dir: state_dir,
        overlay: Mutex::new(Overlay {
            paths: HashMap::from([(INodeNo(1), String::new())]),
            handles: HashMap::new(),
            directories: HashMap::new(),
            next_handle: 1,
        }),
    };
    drop(lock);
    fuser::spawn_mount(view, mountpoint, &config)
}

struct Overlay {
    paths: HashMap<INodeNo, String>,
    handles: HashMap<u64, OpenFile>,
    directories: HashMap<u64, Vec<(INodeNo, FileType, String)>>,
    next_handle: u64,
}
enum OpenFile {
    Upper(File),
    Lower(ObjectId),
}
struct Writable {
    _mount_lock: MountGuard,
    lower: View,
    dir: PathBuf,
    overlay: Mutex<Overlay>,
}
struct Operation<'a> {
    view: &'a Writable,
    overlay: &'a mut Overlay,
    cache: &'a mut State,
    root: ObjectId,
    deleted: BTreeSet<String>,
}
impl Writable {
    fn run<T>(&self, f: impl FnOnce(&mut Operation<'_>) -> io::Result<T>) -> io::Result<T> {
        let mut overlay = self.overlay.lock();
        let _lock = StateLock::take(&self.dir)?;
        recover(&self.dir)?;
        let (_, root) = base(&self.dir)?;
        let deleted = whiteouts(&self.dir)?;
        f(&mut Operation {
            view: self,
            overlay: &mut overlay,
            cache: &mut self.lower.state.lock(),
            root,
            deleted,
        })
    }
}
impl Operation<'_> {
    fn path(&self, ino: INodeNo) -> io::Result<String> {
        self.overlay
            .paths
            .get(&ino)
            .cloned()
            .ok_or_else(|| error(libc::ENOENT))
    }
    fn active(&self, mut ino: INodeNo) -> bool {
        while ino != INodeNo(1) {
            let Some(path) = self.overlay.paths.get(&ino) else {
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
    fn child(&self, parent: INodeNo, name: &OsStr) -> io::Result<String> {
        let name = name.to_str().ok_or_else(|| error(libc::EINVAL))?;
        if name.is_empty() || name.contains(['/', '\n']) || name == "." || name == ".." {
            return Err(error(libc::EINVAL));
        }
        let path = self.path(parent)?;
        Ok(if path.is_empty() {
            name.into()
        } else {
            format!("{path}/{name}")
        })
    }
    fn upper(&self, path: &str) -> PathBuf {
        self.view.dir.join("upper").join(path)
    }
    fn tree(&mut self, id: ObjectId) -> io::Result<Arc<Tree>> {
        self.view
            .lower
            .cached_tree(self.cache, id)
            .map_err(fuse_error)
    }
    fn lower(&mut self, path: &str) -> io::Result<Option<TreeEntry>> {
        let mut entry = TreeEntry {
            name: String::new(),
            mode: EntryMode::Tree,
            id: self.root,
        };
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
    fn entry(&mut self, path: &str) -> io::Result<TreeEntry> {
        if let Some(m) = metadata(&self.upper(path))? {
            return Ok(TreeEntry {
                name: path.rsplit('/').next().unwrap_or("").into(),
                mode: mode(&m),
                id: ObjectId([0; 32]),
            });
        }
        if hidden(&self.deleted, path) {
            return Err(error(libc::ENOENT));
        }
        self.lower(path)?.ok_or_else(|| error(libc::ENOENT))
    }
    fn inode(&mut self, parent: INodeNo, name: &OsStr) -> io::Result<INodeNo> {
        if name == "." {
            return Ok(parent);
        }
        if name == ".." {
            return Ok(self.cache.nodes[(parent.0 - 1) as usize].parent);
        }
        let path = self.child(parent, name)?;
        let entry = self.entry(&path)?;
        if let Some(ino) = self.cache.paths.get(&(parent, entry.name.clone()))
            && file_kind(self.cache.nodes[(ino.0 - 1) as usize].mode) != file_kind(entry.mode)
        {
            self.cache.paths.remove(&(parent, entry.name.clone()));
        }
        let ino = View::inode(self.cache, parent, &entry);
        self.overlay.paths.insert(ino, path);
        Ok(ino)
    }
    fn blob(&mut self, id: ObjectId) -> io::Result<bytes::Bytes> {
        self.view.lower.blob(self.cache, id).map_err(fuse_error)
    }
    fn attr(&mut self, ino: INodeNo) -> io::Result<FileAttr> {
        let path = self.path(ino)?;
        let entry = self.entry(&path)?;
        let m = metadata(&self.upper(&path))?;
        let size = if let Some(m) = &m {
            m.len()
        } else if entry.mode.is_blob() {
            self.blob(entry.id)?.len() as u64
        } else {
            0
        };
        let time = SystemTime::UNIX_EPOCH;
        Ok(FileAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime: m.as_ref().and_then(|m| m.accessed().ok()).unwrap_or(time),
            mtime: m.as_ref().and_then(|m| m.modified().ok()).unwrap_or(time),
            ctime: m.as_ref().and_then(|m| m.modified().ok()).unwrap_or(time),
            crtime: time,
            kind: file_kind(entry.mode),
            perm: m
                .as_ref()
                .map(|m| (m.mode() & 0o7777) as u16)
                .unwrap_or(match entry.mode {
                    EntryMode::File => 0o644,
                    EntryMode::Symlink => 0o777,
                    _ => 0o755,
                }),
            nlink: if file_kind(entry.mode) == FileType::Directory {
                2
            } else {
                1
            },
            // SAFETY: these calls have no preconditions and cannot fail.
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 4096,
            flags: 0,
        })
    }
    fn children(&mut self, path: &str) -> io::Result<BTreeMap<String, EntryMode>> {
        if file_kind(self.entry(path)?.mode) != FileType::Directory {
            return Err(error(libc::ENOTDIR));
        }
        let mut out = BTreeMap::new();
        if !hidden(&self.deleted, path)
            && let Some(entry) = self.lower(path)?
            && entry.mode == EntryMode::Tree
        {
            let tree = self.tree(entry.id)?;
            for e in tree.entries() {
                let child = if path.is_empty() {
                    e.name.clone()
                } else {
                    format!("{path}/{}", e.name)
                };
                if !hidden(&self.deleted, &child) {
                    out.insert(e.name.clone(), e.mode);
                }
            }
        }
        if let Some(m) = metadata(&self.upper(path))?
            && m.is_dir()
        {
            for e in fs::read_dir(self.upper(path))? {
                let e = e?;
                let name = e
                    .file_name()
                    .into_string()
                    .map_err(|_| error(libc::EINVAL))?;
                out.insert(name, mode(&fs::symlink_metadata(e.path())?));
            }
        }
        Ok(out)
    }
    fn copy_up(&mut self, path: &str) -> io::Result<PathBuf> {
        let full = self.upper(path);
        if metadata(&full)?.is_some() {
            return Ok(full);
        }
        let entry = self.entry(path)?;
        if let Some((parent, _)) = path.rsplit_once('/') {
            self.copy_up(parent)?;
        }
        match entry.mode {
            EntryMode::Tree | EntryMode::Gitlink => fs::create_dir(&full)?,
            EntryMode::Symlink => symlink(OsStr::from_bytes(&self.blob(entry.id)?), &full)?,
            _ => {
                // Publish a complete copy only after data and permissions are durable.
                let temp = self.view.dir.join("copy.tmp");
                let mut f = File::create(&temp)?;
                f.write_all(&self.blob(entry.id)?)?;
                f.set_permissions(fs::Permissions::from_mode(
                    if entry.mode == EntryMode::Executable {
                        0o755
                    } else {
                        0o644
                    },
                ))?;
                f.sync_all()?;
                fs::rename(temp, &full)?;
            }
        }
        sync_parent(&full)?;
        Ok(full)
    }
    fn recursive_copy(&mut self, path: &str) -> io::Result<()> {
        if let Some((parent, _)) = path.rsplit_once('/') {
            self.copy_up(parent)?;
        }
        let mut todo = vec![path.to_owned()];
        let mut files = Vec::new();
        while !todo.is_empty() {
            let mut ids = Vec::new();
            for path in &todo {
                if !hidden(&self.deleted, path)
                    && let Some(e) = self.lower(path)?
                    && e.mode == EntryMode::Tree
                {
                    ids.push(e.id);
                }
            }
            self.view
                .lower
                .cache_trees(self.cache, &ids)
                .map_err(fuse_error)?;
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
        for batch in files.chunks(512) {
            let ids = batch.iter().map(|(_, e)| e.id).collect::<Vec<_>>();
            let objects = self.view.lower.batch_objects(&ids).map_err(fuse_error)?;
            for ((path, entry), object) in batch.iter().zip(objects) {
                if object.kind() != ObjectKind::Blob {
                    return Err(other("expected blob during copy-up"));
                }
                let full = self.upper(path);
                if entry.mode == EntryMode::Symlink {
                    symlink(OsStr::from_bytes(object.body()), full)?;
                } else {
                    let temp = self.view.dir.join("copy.tmp");
                    let mut f = File::create(&temp)?;
                    f.write_all(object.body())?;
                    f.set_permissions(fs::Permissions::from_mode(
                        if entry.mode == EntryMode::Executable {
                            0o755
                        } else {
                            0o644
                        },
                    ))?;
                    f.sync_all()?;
                    fs::rename(temp, full)?;
                }
                sync_parent(&self.upper(path))?;
            }
        }
        Ok(())
    }
    fn detach(&mut self, parent: INodeNo, name: &OsStr) {
        if let Some(name) = name.to_str() {
            self.cache.paths.remove(&(parent, name.into()));
        }
    }
    fn remove(&mut self, parent: INodeNo, name: &OsStr, directory: bool) -> io::Result<()> {
        let path = self.child(parent, name)?;
        let is_dir = file_kind(self.entry(&path)?.mode) == FileType::Directory;
        if directory != is_dir {
            return Err(error(if directory {
                libc::ENOTDIR
            } else {
                libc::EISDIR
            }));
        }
        if is_dir && !self.children(&path)?.is_empty() {
            return Err(error(libc::ENOTEMPTY));
        }
        mutate(&self.view.dir, &format!("delete\n{path}\n"))?;
        self.deleted.insert(path);
        self.detach(parent, name);
        Ok(())
    }
    fn create(
        &mut self,
        parent: INodeNo,
        name: &OsStr,
        perm: u32,
        target: Option<&Path>,
        directory: bool,
    ) -> io::Result<INodeNo> {
        let path = self.child(parent, name)?;
        match self.entry(&path) {
            Ok(_) => return Err(error(libc::EEXIST)),
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {}
            Err(e) => return Err(e),
        }
        self.copy_up(&self.path(parent)?)?;
        let full = self.upper(&path);
        if let Some(target) = target {
            symlink(target, &full)?;
        } else if directory {
            fs::create_dir(&full)?;
            fs::set_permissions(&full, fs::Permissions::from_mode(perm))?;
        } else {
            let f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&full)?;
            f.set_permissions(fs::Permissions::from_mode(perm))?;
        }
        sync_parent(&full)?;
        self.inode(parent, name)
    }
    fn open(&mut self, ino: INodeNo, flags: i32) -> io::Result<FileHandle> {
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
        let handle = self.overlay.next_handle;
        self.overlay.next_handle += 1;
        self.overlay.handles.insert(handle, file);
        Ok(FileHandle(handle))
    }
    fn refresh_writer(&mut self, ino: INodeNo, fh: FileHandle) -> io::Result<()> {
        if !self.active(ino) {
            return Ok(());
        }
        if let Some(OpenFile::Upper(f)) = self.overlay.handles.get(&fh.0)
            && f.metadata()?.nlink() == 0
        {
            // Reset prunes matching copies. Reattach a still-open writer to the
            // current path so subsequent writes remain part of the workspace.
            let path = self.path(ino)?;
            let full = self.copy_up(&path)?;
            let file = OpenOptions::new().read(true).write(true).open(full)?;
            self.overlay.handles.insert(fh.0, OpenFile::Upper(file));
        }
        Ok(())
    }
    fn rename(
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
        self.recursive_copy(&old)?;
        self.copy_up(&self.path(newparent)?)?;
        mutate(&self.view.dir, &format!("rename\n{old}\n{new}\n"))?;
        self.deleted.insert(old.clone());
        self.deleted.insert(new.clone());
        self.detach(parent, name);
        self.detach(newparent, newname);
        self.cache
            .paths
            .insert((newparent, newname.to_string_lossy().into_owned()), ino);
        self.cache.nodes[(ino.0 - 1) as usize].parent = newparent;
        for path in self.overlay.paths.values_mut() {
            if under(path, &old) {
                *path = format!("{new}{}", &path[old.len()..]);
            }
        }
        Ok(())
    }
}

// Zero TTL and direct I/O let base replacement become visible without stale
// kernel attributes or pages, and keep status synchronized with completed writes.
const TTL: Duration = Duration::ZERO;
impl Filesystem for Writable {
    fn lookup(&self, _: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        match self.run(|o| {
            let ino = o.inode(parent, name)?;
            o.attr(ino)
        }) {
            Ok(a) => reply.entry(&TTL, &a, Generation(0)),
            Err(e) => reply.error(e.into()),
        }
    }
    fn getattr(&self, _: &Request, ino: INodeNo, _: Option<FileHandle>, reply: ReplyAttr) {
        match self.run(|o| o.attr(ino)) {
            Ok(a) => reply.attr(&TTL, &a),
            Err(e) => reply.error(e.into()),
        }
    }
    fn opendir(&self, _: &Request, ino: INodeNo, _: OpenFlags, reply: ReplyOpen) {
        let result = self.run(|o| {
            let path = o.path(ino)?;
            let children = o.children(&path)?;
            let mut entries = vec![
                (ino, FileType::Directory, ".".into()),
                (
                    o.cache.nodes[(ino.0 - 1) as usize].parent,
                    FileType::Directory,
                    "..".into(),
                ),
            ];
            for (name, mode) in children {
                entries.push((o.inode(ino, OsStr::new(&name))?, file_kind(mode), name));
            }
            let handle = o.overlay.next_handle;
            o.overlay.next_handle += 1;
            o.overlay.directories.insert(handle, entries);
            Ok(FileHandle(handle))
        });
        match result {
            Ok(h) => reply.opened(h, FopenFlags::empty()),
            Err(e) => reply.error(e.into()),
        }
    }
    fn readdir(
        &self,
        _: &Request,
        _: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        // Snapshot each directory stream: deleting entries while traversing a
        // wide directory must not shift the offsets and make rm -r skip files.
        let overlay = self.overlay.lock();
        let Some(entries) = overlay.directories.get(&fh.0) else {
            reply.error(Errno::EBADF);
            return;
        };
        for (i, (ino, kind, name)) in entries
            .iter()
            .enumerate()
            .skip(usize::try_from(offset).unwrap_or(usize::MAX))
        {
            if reply.add(*ino, (i + 1) as u64, *kind, name) {
                break;
            }
        }
        reply.ok();
    }
    fn releasedir(&self, _: &Request, _: INodeNo, fh: FileHandle, _: OpenFlags, reply: ReplyEmpty) {
        self.overlay.lock().directories.remove(&fh.0);
        reply.ok();
    }
    fn open(&self, _: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        match self.run(|o| o.open(ino, flags.0)) {
            Ok(h) => reply.opened(h, FopenFlags::FOPEN_DIRECT_IO),
            Err(e) => reply.error(e.into()),
        }
    }
    fn read(
        &self,
        _: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _: OpenFlags,
        _: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let result = self.run(|o| {
            let handle = o
                .overlay
                .handles
                .get(&fh.0)
                .ok_or_else(|| error(libc::EBADF))?;
            let lower = match handle {
                OpenFile::Lower(id) => Some(*id),
                _ => None,
            };
            let mut out = vec![0; size as usize];
            if let OpenFile::Upper(f) = handle {
                let n = f.read_at(&mut out, offset)?;
                out.truncate(n);
                return Ok(bytes::Bytes::from(out));
            }
            // A lower file may have been copied up since this read handle opened.
            let path = o.path(ino)?;
            if o.active(ino)
                && let Some(m) = metadata(&o.upper(&path))?
                && m.is_file()
            {
                let n = File::open(o.upper(&path))?.read_at(&mut out, offset)?;
                out.truncate(n);
                return Ok(bytes::Bytes::from(out));
            }
            let data = o.blob(lower.ok_or_else(|| error(libc::EBADF))?)?;
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(data.len());
            Ok(data.slice(start..start.saturating_add(size as usize).min(data.len())))
        });
        match result {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(e.into()),
        }
    }
    fn write(
        &self,
        _: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _: WriteFlags,
        _: OpenFlags,
        _: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        match self.run(|o| {
            o.refresh_writer(ino, fh)?;
            match o.overlay.handles.get(&fh.0) {
                Some(OpenFile::Upper(f)) => {
                    f.write_all_at(data, offset)?;
                    Ok(data.len() as u32)
                }
                _ => Err(error(libc::EBADF)),
            }
        }) {
            Ok(n) => reply.written(n),
            Err(e) => reply.error(e.into()),
        }
    }
    fn release(
        &self,
        _: &Request,
        _: INodeNo,
        fh: FileHandle,
        _: OpenFlags,
        _: Option<LockOwner>,
        _: bool,
        reply: ReplyEmpty,
    ) {
        self.overlay.lock().handles.remove(&fh.0);
        reply.ok();
    }
    fn flush(&self, _: &Request, _: INodeNo, _: FileHandle, _: LockOwner, reply: ReplyEmpty) {
        reply.ok();
    }
    fn fsync(&self, _: &Request, _: INodeNo, fh: FileHandle, datasync: bool, reply: ReplyEmpty) {
        let result = self.run(|o| match o.overlay.handles.get(&fh.0) {
            Some(OpenFile::Upper(f)) => {
                if datasync {
                    f.sync_data()
                } else {
                    f.sync_all()
                }
            }
            Some(OpenFile::Lower(_)) => Ok(()),
            _ => Err(error(libc::EBADF)),
        });
        match result {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
    fn fsyncdir(&self, _: &Request, ino: INodeNo, _: FileHandle, _: bool, reply: ReplyEmpty) {
        match self.run(|o| {
            let p = o.copy_up(&o.path(ino)?)?;
            File::open(p)?.sync_all()
        }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
    fn create(
        &self,
        _: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        match self.run(|o| {
            let ino = o.create(parent, name, mode & !umask, None, false)?;
            let h = o.open(ino, flags)?;
            Ok((o.attr(ino)?, h))
        }) {
            Ok((a, h)) => reply.created(&TTL, &a, Generation(0), h, FopenFlags::FOPEN_DIRECT_IO),
            Err(e) => reply.error(e.into()),
        }
    }
    fn mkdir(
        &self,
        _: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        match self.run(|o| {
            let ino = o.create(parent, name, mode & !umask, None, true)?;
            o.attr(ino)
        }) {
            Ok(a) => reply.entry(&TTL, &a, Generation(0)),
            Err(e) => reply.error(e.into()),
        }
    }
    fn symlink(
        &self,
        _: &Request,
        parent: INodeNo,
        name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        match self.run(|o| {
            let ino = o.create(parent, name, 0o777, Some(target), false)?;
            o.attr(ino)
        }) {
            Ok(a) => reply.entry(&TTL, &a, Generation(0)),
            Err(e) => reply.error(e.into()),
        }
    }
    fn readlink(&self, _: &Request, ino: INodeNo, reply: ReplyData) {
        match self.run(|o| {
            let path = o.path(ino)?;
            if metadata(&o.upper(&path))?.is_some() {
                return Ok(fs::read_link(o.upper(&path))?
                    .as_os_str()
                    .as_bytes()
                    .to_vec());
            }
            let entry = o.entry(&path)?;
            if entry.mode != EntryMode::Symlink {
                return Err(error(libc::EINVAL));
            }
            Ok(o.blob(entry.id)?.to_vec())
        }) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(e.into()),
        }
    }
    fn unlink(&self, _: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        match self.run(|o| o.remove(parent, name, false)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
    fn rmdir(&self, _: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        match self.run(|o| o.remove(parent, name, true)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
    fn rename(
        &self,
        _: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        match self.run(|o| o.rename(parent, name, newparent, newname, flags)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
    fn link(&self, _: &Request, _: INodeNo, _: INodeNo, _: &OsStr, reply: ReplyEntry) {
        reply.error(Errno::EPERM);
    }
    fn setattr(
        &self,
        _: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _: Option<SystemTime>,
        fh: Option<FileHandle>,
        _: Option<SystemTime>,
        _: Option<SystemTime>,
        _: Option<SystemTime>,
        _: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let result = self.run(|o| {
            if uid.is_some() || gid.is_some() {
                return Err(error(libc::EPERM));
            }
            if size.is_some()
                && let Some(fh) = fh
            {
                o.refresh_writer(ino, fh)?;
            }
            let path = o.copy_up(&o.path(ino)?)?;
            if let Some(size) = size {
                if let Some(OpenFile::Upper(f)) = fh.and_then(|h| o.overlay.handles.get(&h.0)) {
                    f.set_len(size)?;
                } else {
                    OpenOptions::new().write(true).open(&path)?.set_len(size)?;
                }
            }
            if let Some(mode) = mode {
                fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;
            }
            if atime.is_some() || mtime.is_some() {
                let ts = |t: Option<TimeOrNow>| -> io::Result<libc::timespec> {
                    let Some(t) = t else {
                        return Ok(libc::timespec {
                            tv_sec: 0,
                            tv_nsec: libc::UTIME_OMIT,
                        });
                    };
                    let t = match t {
                        TimeOrNow::Now => SystemTime::now(),
                        TimeOrNow::SpecificTime(t) => t,
                    };
                    let d = t.duration_since(SystemTime::UNIX_EPOCH).map_err(other)?;
                    Ok(libc::timespec {
                        tv_sec: d.as_secs().try_into().map_err(other)?,
                        tv_nsec: d.subsec_nanos().into(),
                    })
                };
                let times = [ts(atime)?, ts(mtime)?];
                let p = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(other)?;
                // SAFETY: p is NUL terminated and times contains two timespecs.
                if unsafe {
                    libc::utimensat(
                        libc::AT_FDCWD,
                        p.as_ptr(),
                        times.as_ptr(),
                        libc::AT_SYMLINK_NOFOLLOW,
                    )
                } != 0
                {
                    return Err(io::Error::last_os_error());
                }
            }
            o.attr(ino)
        });
        match result {
            Ok(a) => reply.attr(&TTL, &a),
            Err(e) => reply.error(e.into()),
        }
    }
}

struct Upper {
    files: BTreeMap<String, (EntryMode, ObjectId)>,
    dirs: BTreeSet<String>,
    blobs: Vec<EncodedObject>,
}
fn scan_upper(state: &Path) -> io::Result<Upper> {
    let root = state.join("upper");
    let mut out = Upper {
        files: BTreeMap::new(),
        dirs: BTreeSet::new(),
        blobs: Vec::new(),
    };
    let mut todo = vec![String::new()];
    while let Some(dir) = todo.pop() {
        for entry in fs::read_dir(root.join(&dir))? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| other("non-UTF8 repo path"))?;
            if dir.is_empty() && name == ".ant" {
                continue;
            }
            let path = if dir.is_empty() {
                name
            } else {
                format!("{dir}/{name}")
            };
            RepoPath::new(&path).map_err(other)?;
            let m = fs::symlink_metadata(entry.path())?;
            if m.is_dir() {
                out.dirs.insert(path.clone());
                todo.push(path);
            } else {
                let data = if m.file_type().is_symlink() {
                    fs::read_link(entry.path())?.as_os_str().as_bytes().to_vec()
                } else {
                    fs::read(entry.path())?
                };
                let blob = EncodedObject::new(ObjectKind::Blob, &data);
                out.files.insert(path, (mode(&m), blob.id()));
                out.blobs.push(blob);
            }
        }
    }
    Ok(out)
}

// Walk only branches intersecting upper entries or whiteouts, breadth-first.
// All trees at a depth are fetched together. Never fetch base blobs.
async fn affected_base(
    objects: &dyn ObjectStore,
    root: ObjectId,
    upper: &Upper,
    deleted: &BTreeSet<String>,
) -> io::Result<BTreeMap<String, (EntryMode, ObjectId)>> {
    let mut exact = upper.dirs.clone();
    exact.extend(upper.files.keys().cloned());
    exact.extend(deleted.iter().cloned());
    if exact.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut branches = BTreeSet::new();
    for path in &exact {
        for (i, _) in path.match_indices('/') {
            branches.insert(path[..i].to_string());
        }
    }
    let mut out = BTreeMap::new();
    let mut todo = vec![(String::new(), root, false)];
    while !todo.is_empty() {
        let mut next = Vec::new();
        for batch in todo.chunks(512) {
            let ids = batch.iter().map(|(_, id, _)| *id).collect::<Vec<_>>();
            let found = objects.get_many(&ids).await.map_err(other)?;
            if found.len() != ids.len() {
                return Err(other("misaligned tree batch"));
            }
            for ((prefix, _, inherited), object) in batch.iter().zip(found) {
                let object = object.ok_or_else(|| other("missing base tree"))?;
                if object.kind() != ObjectKind::Tree {
                    return Err(other("base object is not a tree"));
                }
                let tree = Tree::decode(object.body()).map_err(other)?;
                for e in tree.entries() {
                    let path = if prefix.is_empty() {
                        e.name.clone()
                    } else {
                        format!("{prefix}/{}", e.name)
                    };
                    let all =
                        *inherited || deleted.contains(&path) || upper.files.contains_key(&path);
                    if !all && !exact.contains(&path) && !branches.contains(&path) {
                        continue;
                    }
                    if e.mode == EntryMode::Tree {
                        next.push((path, e.id, all));
                    } else {
                        out.insert(path, (e.mode, e.id));
                    }
                }
            }
        }
        todo = next;
    }
    Ok(out)
}

fn compare(
    upper: &Upper,
    deleted: &BTreeSet<String>,
    lower: &BTreeMap<String, (EntryMode, ObjectId)>,
) -> io::Result<Vec<LocalChange>> {
    let mut paths: BTreeSet<_> = upper.files.keys().cloned().collect();
    paths.extend(lower.keys().cloned());
    let mut out = Vec::new();
    for path in paths {
        let old = lower.get(&path).copied();
        let new = upper.files.get(&path).copied().or_else(|| {
            if hidden(deleted, &path)
                || upper.dirs.contains(&path)
                || path
                    .match_indices('/')
                    .any(|(i, _)| upper.files.contains_key(&path[..i]))
            {
                None
            } else {
                old
            }
        });
        if old == new {
            continue;
        }
        let kind = match (old, new) {
            (None, Some(_)) => LocalChangeKind::Added,
            (Some(_), None) => LocalChangeKind::Deleted,
            _ => LocalChangeKind::Modified,
        };
        out.push(LocalChange {
            path: RepoPath::new(path).map_err(other)?,
            kind,
            new,
        });
    }
    Ok(out)
}

/// Compute sorted changes without walking untouched base subtrees. Blocking
/// filesystem work and state locking run on a blocking thread. New blobs are
/// inserted in batches into `objects`.
pub async fn changes(
    objects: Arc<dyn ObjectStore>,
    state_dir: &Path,
) -> io::Result<Vec<LocalChange>> {
    let state = state_dir.to_path_buf();
    let rt = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let _lock = StateLock::take(&state)?;
        recover(&state)?;
        let (_, root) = base(&state)?;
        let deleted = whiteouts(&state)?;
        let upper = scan_upper(&state)?;
        let lower = rt.block_on(affected_base(objects.as_ref(), root, &upper, &deleted))?;
        let result = compare(&upper, &deleted, &lower)?;
        let changed: BTreeSet<_> = result
            .iter()
            .filter_map(|c| c.new.map(|(_, id)| id))
            .collect();
        let blobs = upper
            .blobs
            .into_iter()
            .filter(|b| changed.contains(&b.id()))
            .collect::<Vec<_>>();
        for batch in blobs.chunks(512) {
            rt.block_on(objects.put_many(batch)).map_err(other)?;
        }
        Ok(result)
    })
    .await
    .map_err(other)?
}

/// Replace the base while preserving edits that differ from the new tree.
/// Matching upper files and obsolete whiteouts are removed. A live mount
/// observes the replacement on its next operation, preserving inode numbers.
pub async fn reset(
    objects: Arc<dyn ObjectStore>,
    state_dir: &Path,
    commit: ObjectId,
    tree: ObjectId,
) -> io::Result<()> {
    let state = state_dir.to_path_buf();
    let rt = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let _lock = StateLock::take(&state)?;
        recover(&state)?;
        let upper = scan_upper(&state)?;
        let mut deleted = whiteouts(&state)?;
        let lower = rt.block_on(affected_base(objects.as_ref(), tree, &upper, &deleted))?;
        let (_, old_tree) = base(&state)?;
        let old_lower = rt.block_on(affected_base(objects.as_ref(), old_tree, &upper, &deleted))?;
        // Publish first: interruption leaves redundant copies, never loses edits.
        save_base(&state, commit, tree)?;
        deleted = lower
            .keys()
            .filter(|path| hidden(&deleted, path) && !upper.files.contains_key(*path))
            .cloned()
            .collect();
        save_whiteouts(&state, &deleted)?;
        for (path, value) in &upper.files {
            if lower.get(path) == Some(value) || old_lower.get(path) == Some(value) {
                fs::remove_file(state.join("upper").join(path))?;
            }
        }
        // Remove empty copy-up directories so upstream removals become visible.
        let mut dirs: Vec<_> = upper.dirs.into_iter().collect();
        dirs.sort_by_key(|p| std::cmp::Reverse(p.len()));
        for path in dirs {
            match fs::remove_dir(state.join("upper").join(path)) {
                Ok(()) => {}
                Err(e) if e.raw_os_error() == Some(libc::ENOTEMPTY) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    })
    .await
    .map_err(other)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_store::{ObjectStoreExt, graph::build_tree, memory::MemoryObjectStore};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    }
    struct Fixture {
        session: Option<BackgroundSession>,
        mount: PathBuf,
        state: PathBuf,
        objects: Arc<MemoryObjectStore>,
        tree: ObjectId,
        rt: tokio::runtime::Runtime,
        _tmp: tempfile::TempDir,
        // Last field: released only after the unmount in `Drop`. See `KERNEL_GATE`.
        _gate: parking_lot::MutexGuard<'static, ()>,
    }
    impl Fixture {
        fn new() -> Self {
            let gate = crate::KERNEL_GATE.lock();
            let rt = runtime();
            let objects = Arc::new(MemoryObjectStore::default());
            let tree = rt
                .block_on(build_tree(
                    objects.as_ref(),
                    &[
                        ("file", b"original"),
                        ("dir/a", b"a"),
                        ("dir/sub/b", b"b"),
                        ("untouched", b"lazy"),
                    ],
                ))
                .unwrap();
            let tmp = tempfile::tempdir().unwrap();
            let mount = tmp.path().join("mount");
            let state = tmp.path().join("state");
            fs::create_dir(&mount).unwrap();
            let session = mount_rw(
                objects.clone(),
                ObjectId([1; 32]),
                tree,
                &mount,
                &state,
                rt.handle().clone(),
                MountOptions::default(),
            )
            .unwrap();
            Self {
                session: Some(session),
                mount,
                state,
                objects,
                tree,
                rt,
                _tmp: tmp,
                _gate: gate,
            }
        }
        fn changes(&self) -> Vec<LocalChange> {
            self.rt
                .block_on(changes(self.objects.clone(), &self.state))
                .unwrap()
        }
        fn summary(&self) -> Vec<(String, LocalChangeKind)> {
            self.changes()
                .into_iter()
                .map(|c| (c.path.to_string(), c.kind))
                .collect()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            if let Some(s) = self.session.take() {
                s.umount_and_join().unwrap();
            }
        }
    }
    use LocalChangeKind::{Added, Deleted, Modified};

    #[test]
    fn read_through_edit_in_place_and_identical_copy() {
        let f = Fixture::new();
        assert_eq!(fs::read(f.mount.join("untouched")).unwrap(), b"lazy");
        assert!(!f.state.join("upper/untouched").exists());
        assert!(f.changes().is_empty());
        let ino = fs::metadata(f.mount.join("file")).unwrap().ino();
        let file = OpenOptions::new()
            .write(true)
            .open(f.mount.join("file"))
            .unwrap();
        file.write_all_at(b"OR", 0).unwrap();
        file.sync_all().unwrap();
        assert_eq!(fs::read(f.mount.join("file")).unwrap(), b"ORiginal");
        assert_eq!(fs::metadata(f.mount.join("file")).unwrap().ino(), ino);
        assert_eq!(f.summary(), [("file".into(), Modified)]);
        fs::write(f.mount.join("file"), b"original").unwrap();
        assert!(f.changes().is_empty());
        file.set_len(3).unwrap();
        assert_eq!(fs::read(f.mount.join("file")).unwrap(), b"ori");
        assert_eq!(f.summary(), [("file".into(), Modified)]);
    }
    #[test]
    fn editor_temp_rename_and_delete() {
        let f = Fixture::new();
        fs::write(f.mount.join("temp"), b"replacement").unwrap();
        fs::rename(f.mount.join("temp"), f.mount.join("file")).unwrap();
        assert_eq!(fs::read(f.mount.join("file")).unwrap(), b"replacement");
        assert_eq!(f.summary(), [("file".into(), Modified)]);
        fs::remove_file(f.mount.join("file")).unwrap();
        assert_eq!(f.summary(), [("file".into(), Deleted)]);
    }
    #[test]
    fn delete_dir_recreate_and_rename_lower_dir() {
        let f = Fixture::new();
        assert_eq!(
            fs::remove_dir(f.mount.join("dir"))
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ENOTEMPTY)
        );
        fs::remove_dir_all(f.mount.join("dir")).unwrap();
        fs::create_dir(f.mount.join("dir")).unwrap();
        fs::write(f.mount.join("dir/new"), b"new").unwrap();
        assert!(!f.mount.join("dir/a").exists());
        assert_eq!(
            f.summary(),
            [
                ("dir/a".into(), Deleted),
                ("dir/new".into(), Added),
                ("dir/sub/b".into(), Deleted)
            ]
        );
        // One mount at a time (see `KERNEL_GATE`); `f`'s edits are checked.
        drop(f);
        let g = Fixture::new();
        let ino = fs::metadata(g.mount.join("dir/sub/b")).unwrap().ino();
        fs::rename(g.mount.join("dir"), g.mount.join("moved")).unwrap();
        assert_eq!(fs::read(g.mount.join("moved/sub/b")).unwrap(), b"b");
        assert_eq!(
            fs::metadata(g.mount.join("moved/sub/b")).unwrap().ino(),
            ino
        );
        assert!(!g.mount.join("dir").exists());
        assert_eq!(
            g.summary(),
            [
                ("dir/a".into(), Deleted),
                ("dir/sub/b".into(), Deleted),
                ("moved/a".into(), Added),
                ("moved/sub/b".into(), Added)
            ]
        );
    }
    #[test]
    fn chmod_symlink_times_and_hardlink() {
        let f = Fixture::new();
        fs::set_permissions(f.mount.join("file"), fs::Permissions::from_mode(0o751)).unwrap();
        assert_eq!(
            fs::metadata(f.mount.join("file")).unwrap().mode() & 0o777,
            0o751
        );
        symlink("file", f.mount.join("link")).unwrap();
        assert_eq!(
            fs::read_link(f.mount.join("link")).unwrap(),
            PathBuf::from("file")
        );
        assert_eq!(fs::read(f.mount.join("link")).unwrap(), b"original");
        assert_eq!(
            f.summary(),
            [("file".into(), Modified), ("link".into(), Added)]
        );
        let changes = f.changes();
        assert_eq!(changes[0].new.unwrap().0, EntryMode::Executable);
        assert_eq!(changes[1].new.unwrap().0, EntryMode::Symlink);
        assert_eq!(
            fs::hard_link(f.mount.join("file"), f.mount.join("hard"))
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EPERM)
        );
        let time = SystemTime::UNIX_EPOCH + Duration::from_secs(1234567);
        File::open(f.mount.join("file"))
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(time))
            .unwrap();
        assert_eq!(
            fs::metadata(f.mount.join("file"))
                .unwrap()
                .modified()
                .unwrap(),
            time
        );
    }
    #[test]
    fn remount_preserves_edits() {
        let mut f = Fixture::new();
        fs::write(f.mount.join("file"), b"edited").unwrap();
        fs::remove_file(f.mount.join("dir/a")).unwrap();
        let expected = f.changes();
        f.session.take().unwrap().umount_and_join().unwrap();
        f.session = Some(
            mount_rw(
                f.objects.clone(),
                ObjectId([9; 32]),
                f.tree,
                &f.mount,
                &f.state,
                f.rt.handle().clone(),
                MountOptions::default(),
            )
            .unwrap(),
        );
        assert_eq!(fs::read(f.mount.join("file")).unwrap(), b"edited");
        assert!(!f.mount.join("dir/a").exists());
        assert_eq!(f.changes(), expected);
        assert_eq!(base(&f.state).unwrap().0, ObjectId([1; 32]));
    }
    #[test]
    fn concurrent_writers() {
        let f = Fixture::new();
        let directory = f.mount.join("many");
        fs::create_dir(&directory).unwrap();
        std::thread::scope(|scope| {
            for t in 0..16 {
                let mount = &directory;
                scope.spawn(move || {
                    for i in 0..50 {
                        let text = format!("thread {t} file {i}");
                        let path = mount.join(format!("new-{t:02}-{i:02}"));
                        fs::write(&path, &text).unwrap();
                        assert_eq!(fs::read(&path).unwrap(), text.as_bytes(), "{path:?}");
                    }
                });
            }
        });
        let changes = f.changes();
        assert_eq!(changes.len(), 800);
        assert!(changes.iter().all(|c| c.kind == Added));
        assert!(changes.windows(2).all(|c| c[0].path < c[1].path));
        fs::remove_dir_all(directory).unwrap();
        assert!(f.changes().is_empty());
    }
    #[test]
    fn concurrent_mixed_operations_read_back_exact_bytes() {
        // Threads create, read back (own and neighbours' files), rename over
        // each other, edit lower files in place and unlink, all at once.
        let f = Fixture::new();
        let root = f.mount.clone();
        std::thread::scope(|scope| {
            for t in 0..12usize {
                let root = &root;
                scope.spawn(move || {
                    let dir = root.join(format!("w{t:02}"));
                    fs::create_dir(&dir).unwrap();
                    let neighbour = root.join(format!("w{:02}", (t + 1) % 12));
                    for i in 0..25usize {
                        let body = format!("t{t} i{i} {}", "x".repeat(i * 37));
                        let a = dir.join(format!("a{i}"));
                        let b = dir.join(format!("b{i}"));
                        fs::write(&a, &body).unwrap();
                        assert_eq!(fs::read(&a).unwrap(), body.as_bytes(), "{a:?}");
                        fs::rename(&a, &b).unwrap();
                        assert!(!a.exists());
                        assert_eq!(fs::read(&b).unwrap(), body.as_bytes(), "{b:?}");
                        // Lower file, shared by every thread.
                        assert_eq!(fs::read(root.join("untouched")).unwrap(), b"lazy");
                        assert_eq!(fs::read(root.join("dir/sub/b")).unwrap(), b"b");
                        // Neighbours' files are either absent yet or exact.
                        for e in fs::read_dir(&neighbour).into_iter().flatten().flatten() {
                            let name = e.file_name().into_string().unwrap();
                            if let Some(n) = name.strip_prefix('b').and_then(|n| n.parse::<usize>().ok())
                                && let Ok(got) = fs::read(e.path())
                            {
                                let want = format!(
                                    "t{} i{n} {}",
                                    (t + 1) % 12,
                                    "x".repeat(n * 37)
                                );
                                assert_eq!(got, want.as_bytes(), "{:?}", e.path());
                            }
                        }
                        if i % 3 == 0 {
                            fs::remove_file(&b).unwrap();
                            assert!(!b.exists());
                        }
                    }
                });
            }
        });
        let kept = f.changes();
        assert_eq!(kept.len(), 12 * (25 - 9));
        assert!(kept.iter().all(|c| c.kind == Added));
        assert_eq!(fs::read(f.mount.join("file")).unwrap(), b"original");
    }
    #[test]
    fn mount_guard_is_exclusive_and_released_immediately() {
        // Spawns a process, so it must not overlap kernel I/O (see `KERNEL_GATE`).
        let _gate = crate::KERNEL_GATE.lock();
        let tmp = tempfile::tempdir().unwrap();
        let first = MountGuard::take(tmp.path()).unwrap();
        let err = MountGuard::take(tmp.path()).err().unwrap();
        assert!(err.to_string().contains("already mounted"), "{err}");
        // A forked child (e.g. a concurrent fusermount spawn) must not keep the
        // lock alive after the guard is gone.
        let child = std::process::Command::new("sleep").arg("0.3").spawn().unwrap();
        drop(first);
        MountGuard::take(tmp.path()).unwrap();
        drop(child);
    }
    #[test]
    fn live_reset_cleans_landed_entries_and_preserves_other_edits() {
        let f = Fixture::new();
        fs::write(f.mount.join("file"), b"landed").unwrap();
        fs::write(f.mount.join("local"), b"unsubmitted").unwrap();
        fs::remove_file(f.mount.join("dir/a")).unwrap();
        let tree =
            f.rt.block_on(build_tree(
                f.objects.as_ref(),
                &[
                    ("file", b"landed"),
                    ("dir/sub/b", b"b"),
                    ("upstream", b"new"),
                ],
            ))
            .unwrap();
        f.rt.block_on(reset(f.objects.clone(), &f.state, ObjectId([2; 32]), tree))
            .unwrap();
        assert_eq!(fs::read(f.mount.join("file")).unwrap(), b"landed");
        assert_eq!(fs::read(f.mount.join("upstream")).unwrap(), b"new");
        assert!(!f.mount.join("untouched").exists());
        assert!(!f.mount.join("dir/a").exists());
        assert_eq!(f.summary(), [("local".into(), Added)]);
        assert!(!f.state.join("upper/file").exists());
    }

    #[derive(Default)]
    pub(super) struct CountingStore {
        pub(super) inner: MemoryObjectStore,
        pub(super) reads: AtomicUsize,
    }
    type StoreFuture<'a, T> =
        std::pin::Pin<Box<dyn std::future::Future<Output = ant_store::Result<T>> + Send + 'a>>;
    impl ObjectStore for CountingStore {
        fn get_many<'a, 'b, 'f>(
            &'a self,
            ids: &'b [ObjectId],
        ) -> StoreFuture<'f, Vec<Option<EncodedObject>>>
        where
            'a: 'f,
            'b: 'f,
            Self: 'f,
        {
            Box::pin(async move {
                self.reads.fetch_add(ids.len(), Ordering::Relaxed);
                self.inner.get_many(ids).await
            })
        }
        fn put_many<'a, 'b, 'f>(&'a self, objects: &'b [EncodedObject]) -> StoreFuture<'f, ()>
        where
            'a: 'f,
            'b: 'f,
            Self: 'f,
        {
            Box::pin(async move { self.inner.put_many(objects).await })
        }
        fn has_many<'a, 'b, 'f>(&'a self, ids: &'b [ObjectId]) -> StoreFuture<'f, Vec<bool>>
        where
            'a: 'f,
            'b: 'f,
            Self: 'f,
        {
            Box::pin(async move { self.inner.has_many(ids).await })
        }
    }
    #[test]
    fn changes_only_reads_affected_branches_of_twenty_thousand_files() {
        let _gate = crate::KERNEL_GATE.lock();
        let rt = runtime();
        let objects = Arc::new(CountingStore::default());
        let paths = (0..20_000)
            .map(|i| format!("d{:03}/f{:03}", i / 100, i % 100))
            .collect::<Vec<_>>();
        let entries = paths
            .iter()
            .map(|p| (p.as_str(), b"base".as_slice()))
            .collect::<Vec<_>>();
        let tree = rt.block_on(build_tree(objects.as_ref(), &entries)).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let mount = tmp.path().join("mount");
        let state = tmp.path().join("state");
        fs::create_dir(&mount).unwrap();
        let session = mount_rw(
            objects.clone(),
            ObjectId([1; 32]),
            tree,
            &mount,
            &state,
            rt.handle().clone(),
            MountOptions::default(),
        )
        .unwrap();
        fs::write(mount.join("d001/f001"), b"one").unwrap();
        fs::write(mount.join("d199/f099"), b"two").unwrap();
        objects.reads.store(0, Ordering::Relaxed);
        let changes = rt.block_on(changes(objects.clone(), &state)).unwrap();
        assert_eq!(changes.len(), 2);
        assert!(
            objects.reads.load(Ordering::Relaxed) <= 24,
            "too many base reads"
        );
        assert_eq!(
            &rt.block_on(objects.read_blob(&changes[0].new.unwrap().1))
                .unwrap()[..],
            b"one"
        );
        session.umount_and_join().unwrap();
    }
}

#[cfg(test)]
mod operation_tests {
    use super::*;
    use ant_store::{graph::build_tree, memory::MemoryObjectStore};

    // Exercise the exact operation implementation even in a sandbox without
    // /dev/fuse; the mount tests above independently cover kernel integration.
    #[test]
    fn mutations_and_reset_without_kernel() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let objects = Arc::new(MemoryObjectStore::default());
        let tree = rt
            .block_on(build_tree(
                objects.as_ref(),
                &[("file", b"original"), ("dir/a", b"a"), ("dir/sub/b", b"b")],
            ))
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path();
        fs::create_dir(state.join("upper")).unwrap();
        save_base(state, ObjectId([1; 32]), tree).unwrap();
        save_whiteouts(state, &BTreeSet::new()).unwrap();
        let view = Writable {
            _mount_lock: MountGuard::take(state).unwrap(),
            lower: View::new(
                objects.clone(),
                tree,
                rt.handle().clone(),
                MountOptions::default(),
            ),
            dir: state.to_path_buf(),
            overlay: Mutex::new(Overlay {
                paths: HashMap::from([(INodeNo(1), String::new())]),
                handles: HashMap::new(),
                directories: HashMap::new(),
                next_handle: 1,
            }),
        };
        let root = INodeNo(1);
        view.run(|o| {
            let ino = o.inode(root, OsStr::new("file"))?;
            assert_eq!(o.attr(ino)?.size, 8);
            let p = o.copy_up("file")?;
            fs::write(p, "edited")?;
            assert_eq!(o.inode(root, OsStr::new("file"))?, ino);
            let dir = o.inode(root, OsStr::new("dir"))?;
            let sub = o.inode(dir, OsStr::new("sub"))?;
            let b = o.inode(sub, OsStr::new("b"))?;
            o.rename(
                root,
                OsStr::new("dir"),
                root,
                OsStr::new("moved"),
                RenameFlags::empty(),
            )?;
            assert_eq!(o.path(b)?, "moved/sub/b");
            assert_eq!(fs::read(o.upper("moved/sub/b"))?, b"b");
            assert!(o.entry("dir").is_err());
            o.remove(root, OsStr::new("file"), false)?;
            let dir = o.create(root, OsStr::new("dir"), 0o755, None, true)?;
            o.create(dir, OsStr::new("new"), 0o644, None, false)?;
            assert_eq!(o.children("dir")?.len(), 1);
            o.create(
                root,
                OsStr::new("link"),
                0o777,
                Some(Path::new("dir/new")),
                false,
            )?;
            Ok(())
        })
        .unwrap();
        let changes = rt.block_on(changes(objects.clone(), state)).unwrap();
        let summary = changes
            .iter()
            .map(|c| (c.path.as_str(), c.kind))
            .collect::<Vec<_>>();
        use LocalChangeKind::{Added, Deleted};
        assert_eq!(
            summary,
            [
                ("dir/a", Deleted),
                ("dir/new", Added),
                ("dir/sub/b", Deleted),
                ("file", Deleted),
                ("link", Added),
                ("moved/a", Added),
                ("moved/sub/b", Added)
            ]
        );
        let new_tree = rt
            .block_on(build_tree(
                objects.as_ref(),
                &[("dir/new", b""), ("upstream", b"new")],
            ))
            .unwrap();
        rt.block_on(reset(objects.clone(), state, ObjectId([2; 32]), new_tree))
            .unwrap();
        view.run(|o| {
            assert_eq!(o.children("dir")?.len(), 1);
            assert_eq!(o.entry("upstream")?.mode, EntryMode::File);
            assert!(!o.deleted.contains("dir"));
            Ok(())
        })
        .unwrap();
        assert!(!state.join("upper/dir/new").exists());
        assert_eq!(
            rt.block_on(super::changes(objects, state)).unwrap().len(),
            3
        );
    }

    #[test]
    fn status_scales_with_changed_paths_without_kernel() {
        use std::sync::atomic::Ordering;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let objects = Arc::new(super::tests::CountingStore::default());
        let paths = (0..20_000)
            .map(|i| format!("d{:03}/f{:03}", i / 100, i % 100))
            .collect::<Vec<_>>();
        let entries = paths
            .iter()
            .map(|p| (p.as_str(), b"base".as_slice()))
            .collect::<Vec<_>>();
        let tree = rt.block_on(build_tree(objects.as_ref(), &entries)).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path();
        fs::create_dir_all(state.join("upper/d001")).unwrap();
        fs::create_dir_all(state.join("upper/d199")).unwrap();
        fs::write(state.join("upper/d001/f001"), b"one").unwrap();
        fs::write(state.join("upper/d199/f099"), b"two").unwrap();
        save_base(state, ObjectId([1; 32]), tree).unwrap();
        save_whiteouts(state, &BTreeSet::new()).unwrap();
        objects.reads.store(0, Ordering::Relaxed);
        let changes = rt.block_on(super::changes(objects.clone(), state)).unwrap();
        assert_eq!(changes.len(), 2);
        assert_eq!(objects.reads.load(Ordering::Relaxed), 3);
        assert_eq!(changes[0].path.as_str(), "d001/f001");
        assert_eq!(changes[1].path.as_str(), "d199/f099");
    }

    #[test]
    fn interrupted_mutations_roll_forward() {
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path();
        fs::create_dir(state.join("upper")).unwrap();
        save_whiteouts(state, &BTreeSet::new()).unwrap();
        fs::write(state.join("upper/old"), b"new data").unwrap();
        fs::write(state.join("upper/new"), b"old data").unwrap();
        // Crash just after journaling, before whiteouts or rename.
        atomic(state, "intent", b"rename\nold\nnew\n").unwrap();
        recover(state).unwrap();
        assert_eq!(fs::read(state.join("upper/new")).unwrap(), b"new data");
        assert!(!state.join("upper/old").exists());
        assert_eq!(
            whiteouts(state).unwrap(),
            BTreeSet::from(["old".into(), "new".into()])
        );
        // Crash after the physical rename, before clearing the intent.
        atomic(state, "intent", b"rename\nold\nnew\n").unwrap();
        recover(state).unwrap();
        assert_eq!(fs::read(state.join("upper/new")).unwrap(), b"new data");
        atomic(state, "intent", b"delete\nnew\n").unwrap();
        recover(state).unwrap();
        assert!(!state.join("upper/new").exists());
        assert!(!state.join("intent").exists());
    }

    #[test]
    fn reset_drops_unchanged_copies_and_splits_directory_whiteouts() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let objects = Arc::new(MemoryObjectStore::default());
        let old = rt
            .block_on(build_tree(
                objects.as_ref(),
                &[("untouched", b"old"), ("dir/a", b"old"), ("dir/b", b"old")],
            ))
            .unwrap();
        let new = rt
            .block_on(build_tree(
                objects.as_ref(),
                &[
                    ("untouched", b"upstream"),
                    ("dir/a", b"landed"),
                    ("dir/b", b"old"),
                ],
            ))
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let state = temp.path();
        fs::create_dir_all(state.join("upper/dir")).unwrap();
        fs::write(state.join("upper/untouched"), b"old").unwrap();
        fs::write(state.join("upper/dir/a"), b"landed").unwrap();
        save_base(state, ObjectId([1; 32]), old).unwrap();
        save_whiteouts(state, &BTreeSet::from(["dir".into()])).unwrap();
        rt.block_on(reset(objects.clone(), state, ObjectId([2; 32]), new))
            .unwrap();
        assert!(!state.join("upper/untouched").exists());
        assert!(!state.join("upper/dir/a").exists());
        assert_eq!(whiteouts(state).unwrap(), BTreeSet::from(["dir/b".into()]));
        let changes = rt.block_on(changes(objects, state)).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].path.as_str(), "dir/b");
        assert_eq!(changes[0].kind, LocalChangeKind::Deleted);
    }
}
