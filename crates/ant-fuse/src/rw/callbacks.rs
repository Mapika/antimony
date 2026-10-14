//! `fuser::Filesystem` callbacks for writable workspaces: thin wrappers
//! running [`super::overlay::Operation`]s and replying.

use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::Path;
use std::time::{Duration, SystemTime};

use ant_core::EntryMode;
use fuser::*;

use super::overlay::*;
use super::{error, file_kind, metadata, other};

/// Wait for the handle's inode's acknowledged writes; report a deferred
/// write error.
fn written(handle: &Handle) -> io::Result<()> {
    handle.pending.as_ref().map_or(Ok(()), |p| p.written())
}

impl Writable {
    fn read_lower(
        &self,
        o: &mut Operation<'_>,
        ino: INodeNo,
        id: ant_core::ObjectId,
        offset: u64,
        size: u32,
    ) -> io::Result<bytes::Bytes> {
        // A lower file may have been copied up since this read handle opened.
        let path = o.path(ino)?;
        self.settle(ino);
        if o.active(ino)
            && let Some(m) = metadata(&o.upper(&path))?
            && m.is_file()
        {
            let mut out = vec![0; size as usize];
            let n = File::open(o.upper(&path))?.read_at(&mut out, offset)?;
            out.truncate(n);
            return Ok(bytes::Bytes::from(out));
        }
        let data = o.blob(id)?;
        let start = usize::try_from(offset).unwrap_or(usize::MAX).min(data.len());
        Ok(data.slice(start..start.saturating_add(size as usize).min(data.len())))
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
                (o.cache.nodes[(ino.0 - 1) as usize].parent, FileType::Directory, "..".into()),
            ];
            for (name, mode) in children {
                entries.push((o.inode(ino, OsStr::new(&name))?, file_kind(mode), name));
            }
            let handle = o.view.next_handle();
            o.directories.insert(handle, entries);
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
        for (i, (ino, kind, name)) in
            entries.iter().enumerate().skip(usize::try_from(offset).unwrap_or(usize::MAX))
        {
            if reply.add(*ino, (i + 1) as u64, *kind, name) {
                break;
            }
        }
        reply.ok();
    }
    fn init(&mut self, _: &Request, config: &mut KernelConfig) -> io::Result<()> {
        // Lookups and readdir in one directory may run concurrently; the
        // overlay serializes what needs it.
        let _ = config.add_capabilities(InitFlags::FUSE_PARALLEL_DIROPS);
        Ok(())
    }
    fn destroy(&mut self) {
        if let Err(error) = self.shutdown() {
            tracing::error!(%error, "lazy workspace shutdown failed");
        }
    }
    fn releasedir(&self, _: &Request, _: INodeNo, fh: FileHandle, _: OpenFlags, reply: ReplyEmpty) {
        self.overlay.lock().directories.remove(&fh.0);
        reply.ok();
    }
    fn open(&self, _: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let result = if flags.0 & libc::O_ACCMODE != libc::O_RDONLY {
            self.run_mut(|o| o.open(ino, flags.0))
        } else {
            self.run(|o| o.open(ino, flags.0))
        };
        match result {
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
        let result = self.handle(fh).and_then(|handle| {
            let lower = match &handle.file {
                OpenFile::Upper(f) => {
                    // Fast path: positional read, no overlay or state lock.
                    if let Some(p) = &handle.pending {
                        p.settle();
                    }
                    let mut out = vec![0; size as usize];
                    let n = f.read_at(&mut out, offset)?;
                    out.truncate(n);
                    return Ok(bytes::Bytes::from(out));
                }
                OpenFile::Lower(id) => *id,
            };
            self.run(|o| self.read_lower(o, ino, lower, offset, size))
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
        // Fast path: positional write on the handle's own file, no overlay or
        // state lock; different files proceed in parallel. Acknowledged
        // before the copy into the upper file (see `Pending`).
        let handle = match self.handle(fh) {
            Ok(h) => h,
            Err(e) => return reply.error(e.into()),
        };
        let (OpenFile::Upper(f), Some(pending)) = (&handle.file, &handle.pending) else {
            return reply.error(Errno::EBADF);
        };
        if let Err(e) = pending.begin(offset, data.len()) {
            return reply.error(e.into());
        }
        reply.written(data.len() as u32);
        let result = f.write_all_at(data, offset);
        pending.end(offset, data.len(), result);
        if let Err(e) = self.check_attached(ino, fh, &handle, Some((offset, data))) {
            pending.fail(e);
        }
    }
    fn release(
        &self,
        _: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _: OpenFlags,
        _: Option<LockOwner>,
        _: bool,
        reply: ReplyEmpty,
    ) {
        self.release_handle(ino, fh);
        reply.ok();
    }
    fn flush(&self, _: &Request, ino: INodeNo, fh: FileHandle, _: LockOwner, reply: ReplyEmpty) {
        let result = self.handle(fh).and_then(|h| {
            written(&h)?;
            self.check_attached(ino, fh, &h, None)
        });
        match result {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
    fn fsync(&self, _: &Request, ino: INodeNo, fh: FileHandle, datasync: bool, reply: ReplyEmpty) {
        let result = self.handle(fh).and_then(|h| {
            written(&h)?;
            self.check_attached(ino, fh, &h, None)?;
            if let OpenFile::Upper(f) = &self.handle(fh)?.file {
                if datasync { f.sync_data()? } else { f.sync_all()? }
            }
            // Also make the file's creation and earlier metadata durable.
            self.journal.sync()
        });
        match result {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
    fn fsyncdir(&self, _: &Request, ino: INodeNo, _: FileHandle, _: bool, reply: ReplyEmpty) {
        match self.run_mut(|o| {
            let p = o.copy_up(&o.path(ino)?)?;
            File::open(p)?.sync_all()
        }) {
            Ok(()) => match self.journal.sync() {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(e.into()),
            },
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
        match self.run_mut(|o| {
            let (ino, file, m) = o.create_entry(parent, name, mode & !umask, None, false)?;
            let file = file.ok_or_else(|| other("created file not open"))?;
            let attr = o.created_attr(ino, &m)?;
            let write = flags & libc::O_ACCMODE != libc::O_RDONLY;
            Ok((attr, self.insert_handle(ino, OpenFile::Upper(file), write)))
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
        match self.run_mut(|o| {
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
        match self.run_mut(|o| {
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
                return Ok(fs::read_link(o.upper(&path))?.as_os_str().as_bytes().to_vec());
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
        match self.run_mut(|o| o.remove(parent, name, false)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
    fn rmdir(&self, _: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        match self.run_mut(|o| o.remove(parent, name, true)) {
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
        match self.run_mut(|o| o.rename(parent, name, newparent, newname, flags)) {
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
        if uid.is_some() || gid.is_some() {
            reply.error(Errno::EPERM);
            return;
        }
        if size.is_some()
            && let Some(fh) = fh
            && let Err(e) = self.handle(fh).and_then(|h| self.check_attached(ino, fh, &h, None))
        {
            reply.error(e.into());
            return;
        }
        let result = self.run_mut(|o| {
            let path = o.copy_up(&o.path(ino)?)?;
            self.settle(ino);
            if let Some(size) = size {
                let handle = fh.and_then(|h| self.handles.read().get(&h.0).cloned());
                if let Some(OpenFile::Upper(f)) = handle.as_ref().map(|h| &h.file) {
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
                        return Ok(libc::timespec { tv_sec: 0, tv_nsec: libc::UTIME_OMIT });
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
