//! The copy-on-write layer: path/inode bookkeeping and every mutation, run
//! under the state lock against the current base and whiteouts.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use ant_core::{EntryMode, ObjectId, ObjectKind, Tree, TreeEntry};
use fuser::*;
use parking_lot::Mutex;

use crate::filesystem::{State, View};

use super::state::*;
use super::{error, file_kind, fuse_error, hidden, metadata, mode, other, under};

pub(super) struct Overlay {
    pub(super) paths: HashMap<INodeNo, String>,
    pub(super) handles: HashMap<u64, OpenFile>,
    pub(super) directories: HashMap<u64, Vec<(INodeNo, FileType, String)>>,
    pub(super) next_handle: u64,
}
pub(super) enum OpenFile {
    Upper(File),
    Lower(ObjectId),
}
pub(super) struct Writable {
    pub(super) _mount_lock: MountGuard,
    pub(super) lower: View,
    pub(super) dir: PathBuf,
    pub(super) overlay: Mutex<Overlay>,
}
pub(super) struct Operation<'a> {
    pub(super) view: &'a Writable,
    pub(super) overlay: &'a mut Overlay,
    pub(super) cache: &'a mut State,
    pub(super) root: ObjectId,
    pub(super) deleted: BTreeSet<String>,
}
impl Writable {
    pub(super) fn run<T>(
        &self,
        f: impl FnOnce(&mut Operation<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
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
    pub(super) fn path(&self, ino: INodeNo) -> io::Result<String> {
        self.overlay.paths.get(&ino).cloned().ok_or_else(|| error(libc::ENOENT))
    }
    pub(super) fn active(&self, mut ino: INodeNo) -> bool {
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
    pub(super) fn entry(&mut self, path: &str) -> io::Result<TreeEntry> {
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
    pub(super) fn inode(&mut self, parent: INodeNo, name: &OsStr) -> io::Result<INodeNo> {
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
    pub(super) fn blob(&mut self, id: ObjectId) -> io::Result<bytes::Bytes> {
        self.view.lower.blob(self.cache, id).map_err(fuse_error)
    }
    pub(super) fn attr(&mut self, ino: INodeNo) -> io::Result<FileAttr> {
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
            perm: m.as_ref().map(|m| (m.mode() & 0o7777) as u16).unwrap_or(match entry.mode {
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
        })
    }
    pub(super) fn children(&mut self, path: &str) -> io::Result<BTreeMap<String, EntryMode>> {
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
                let child =
                    if path.is_empty() { e.name.clone() } else { format!("{path}/{}", e.name) };
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
                let name = e.file_name().into_string().map_err(|_| error(libc::EINVAL))?;
                out.insert(name, mode(&fs::symlink_metadata(e.path())?));
            }
        }
        Ok(out)
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
        match entry.mode {
            EntryMode::Tree | EntryMode::Gitlink => fs::create_dir(&full)?,
            EntryMode::Symlink => symlink(OsStr::from_bytes(&self.blob(entry.id)?), &full)?,
            _ => {
                // Publish a complete copy only after data and permissions are durable.
                let temp = self.view.dir.join("copy.tmp");
                let mut f = File::create(&temp)?;
                f.write_all(&self.blob(entry.id)?)?;
                f.set_permissions(fs::Permissions::from_mode(
                    if entry.mode == EntryMode::Executable { 0o755 } else { 0o644 },
                ))?;
                f.sync_all()?;
                fs::rename(temp, &full)?;
            }
        }
        sync_parent(&full)?;
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
                if !hidden(&self.deleted, path)
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
                        if entry.mode == EntryMode::Executable { 0o755 } else { 0o644 },
                    ))?;
                    f.sync_all()?;
                    fs::rename(temp, full)?;
                }
                sync_parent(&self.upper(path))?;
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
        mutate(&self.view.dir, &format!("delete\n{path}\n"))?;
        self.deleted.insert(path);
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
            let f = OpenOptions::new().write(true).create_new(true).open(&full)?;
            f.set_permissions(fs::Permissions::from_mode(perm))?;
        }
        sync_parent(&full)?;
        self.inode(parent, name)
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
        let handle = self.overlay.next_handle;
        self.overlay.next_handle += 1;
        self.overlay.handles.insert(handle, file);
        Ok(FileHandle(handle))
    }
    pub(super) fn refresh_writer(&mut self, ino: INodeNo, fh: FileHandle) -> io::Result<()> {
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
        self.recursive_copy(&old)?;
        self.copy_up(&self.path(newparent)?)?;
        mutate(&self.view.dir, &format!("rename\n{old}\n{new}\n"))?;
        self.deleted.insert(old.clone());
        self.deleted.insert(new.clone());
        self.detach(parent, name);
        self.detach(newparent, newname);
        self.cache.paths.insert((newparent, newname.to_string_lossy().into_owned()), ino);
        self.cache.nodes[(ino.0 - 1) as usize].parent = newparent;
        for path in self.overlay.paths.values_mut() {
            if under(path, &old) {
                *path = format!("{new}{}", &path[old.len()..]);
            }
        }
        Ok(())
    }
}
