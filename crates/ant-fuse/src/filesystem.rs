use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use ant_core::{EntryMode, ObjectId, ObjectKind, Tree, TreeEntry};
use ant_store::{ObjectStore, ObjectStoreExt, StoreError};
use bytes::Bytes;
use fuser::*;
use parking_lot::Mutex;

use crate::MountOptions;

type Result<T> = std::result::Result<T, Errno>;

fn io_error(error: impl std::fmt::Display) -> Errno {
    tracing::error!(%error, "FUSE object fetch failed");
    Errno::EIO
}

fn kind(mode: EntryMode) -> FileType {
    match mode.class() {
        EntryMode::Tree | EntryMode::Gitlink => FileType::Directory,
        EntryMode::Symlink => FileType::Symlink,
        _ => FileType::RegularFile,
    }
}

#[derive(Clone, Copy)]
pub(super) struct Node {
    pub(super) parent: INodeNo,
    pub(super) mode: EntryMode,
    pub(super) id: ObjectId,
}

#[derive(Default)]
struct BlobCache {
    data: HashMap<ObjectId, (Bytes, u128)>,
    order: BTreeMap<u128, ObjectId>,
    clock: u128,
    bytes: usize,
}

impl BlobCache {
    fn get(&mut self, id: &ObjectId) -> Option<Bytes> {
        let (data, stamp) = self.data.get_mut(id)?;
        self.order.remove(stamp);
        self.clock += 1;
        *stamp = self.clock;
        self.order.insert(*stamp, *id);
        Some(data.clone())
    }

    fn insert(&mut self, id: ObjectId, data: Bytes, limit: usize) {
        if data.len() > limit || limit == 0 {
            return;
        }
        if let Some((old, stamp)) = self.data.remove(&id) {
            self.bytes -= old.len();
            self.order.remove(&stamp);
        }
        while self.bytes > limit - data.len() {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            if let Some((old, _)) = self.data.remove(&oldest) {
                self.bytes -= old.len();
            }
        }
        self.clock += 1;
        self.bytes += data.len();
        self.order.insert(self.clock, id);
        self.data.insert(id, (data, self.clock));
    }
}

pub(super) struct State {
    pub(super) nodes: Vec<Node>,
    pub(super) paths: HashMap<(INodeNo, String), INodeNo>,
    pub(super) trees: HashMap<ObjectId, Arc<Tree>>,
    prefetched: HashSet<ObjectId>,
    sizes: HashMap<ObjectId, u64>,
    blobs: BlobCache,
}

pub(crate) struct View {
    objects: Arc<dyn ObjectStore>,
    rt: tokio::runtime::Handle,
    opts: MountOptions,
    uid: u32,
    gid: u32,
    // Callbacks run on fuser's OS threads, never Tokio workers. Serializing
    // cache misses also coalesces concurrent requests for the same object.
    pub(super) state: Mutex<State>,
}

impl View {
    pub(crate) fn new(
        objects: Arc<dyn ObjectStore>,
        root: ObjectId,
        rt: tokio::runtime::Handle,
        opts: MountOptions,
    ) -> Self {
        Self {
            objects,
            rt,
            opts,
            // SAFETY: these libc calls take no pointers and cannot fail.
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            state: Mutex::new(State {
                nodes: vec![Node { parent: INodeNo(1), mode: EntryMode::Tree, id: root }],
                paths: HashMap::new(),
                trees: HashMap::new(),
                prefetched: HashSet::new(),
                sizes: HashMap::new(),
                blobs: BlobCache::default(),
            }),
        }
    }

    fn node(s: &State, ino: INodeNo) -> Result<Node> {
        let index =
            ino.0.checked_sub(1).and_then(|n| usize::try_from(n).ok()).ok_or(Errno::ENOENT)?;
        s.nodes.get(index).copied().ok_or(Errno::ENOENT)
    }

    pub(super) fn inode(s: &mut State, parent: INodeNo, entry: &TreeEntry) -> INodeNo {
        *s.paths.entry((parent, entry.name.clone())).or_insert_with(|| {
            s.nodes.push(Node { parent, mode: entry.mode.class(), id: entry.id });
            INodeNo(s.nodes.len() as u64)
        })
    }

    fn tree(&self, s: &mut State, ino: INodeNo) -> Result<Arc<Tree>> {
        let node = Self::node(s, ino)?;
        if node.mode == EntryMode::Gitlink {
            return Ok(Arc::new(Tree::empty()));
        }
        if node.mode != EntryMode::Tree {
            return Err(Errno::ENOTDIR);
        }
        self.cached_tree(s, node.id)
    }

    pub(super) fn cached_tree(&self, s: &mut State, id: ObjectId) -> Result<Arc<Tree>> {
        if let Some(tree) = s.trees.get(&id) {
            return Ok(tree.clone());
        }
        let tree = Arc::new(self.rt.block_on(self.objects.read_tree(&id)).map_err(io_error)?);
        s.trees.insert(id, tree.clone());
        Ok(tree)
    }

    pub(super) fn batch_objects(&self, ids: &[ObjectId]) -> Result<Vec<ant_core::EncodedObject>> {
        let objects = self.rt.block_on(self.objects.get_many(ids)).map_err(io_error)?;
        if objects.len() != ids.len() {
            return Err(io_error("misaligned object batch"));
        }
        ids.iter()
            .zip(objects)
            .map(|(id, o)| o.ok_or_else(|| io_error(StoreError::NotFound(*id))))
            .collect()
    }

    pub(super) fn cache_trees(&self, s: &mut State, ids: &[ObjectId]) -> Result<()> {
        let mut ids: Vec<_> = ids.iter().filter(|id| !s.trees.contains_key(id)).copied().collect();
        ids.sort_unstable();
        ids.dedup();
        for batch in ids.chunks(512) {
            for (id, object) in batch.iter().zip(self.batch_objects(batch)?) {
                if object.kind() != ObjectKind::Tree {
                    return Err(io_error("expected tree"));
                }
                s.trees.insert(*id, Arc::new(Tree::decode(object.body()).map_err(io_error)?));
            }
        }
        Ok(())
    }

    fn prefetch(&self, s: &mut State, ino: INodeNo, tree: &Tree) -> Result<()> {
        let node = Self::node(s, ino)?;
        if node.mode == EntryMode::Gitlink || s.prefetched.contains(&node.id) {
            return Ok(());
        }
        let mut ids: Vec<_> = tree
            .entries()
            .iter()
            .filter(|e| e.mode.is_tree() && !s.trees.contains_key(&e.id))
            .map(|e| e.id)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        if !ids.is_empty() {
            let objects = self.rt.block_on(self.objects.get_many(&ids)).map_err(io_error)?;
            if objects.len() != ids.len() {
                return Err(io_error("misaligned get_many response"));
            }
            for (id, object) in ids.into_iter().zip(objects) {
                let object = object.ok_or_else(|| io_error(StoreError::NotFound(id)))?;
                if object.kind() != ObjectKind::Tree {
                    return Err(io_error(StoreError::WrongKind {
                        id,
                        expected: ObjectKind::Tree,
                        actual: object.kind(),
                    }));
                }
                s.trees.insert(id, Arc::new(Tree::decode(object.body()).map_err(io_error)?));
            }
        }
        s.prefetched.insert(node.id);
        Ok(())
    }

    pub(super) fn blob(&self, s: &mut State, id: ObjectId) -> Result<Bytes> {
        if let Some(data) = s.blobs.get(&id) {
            return Ok(data);
        }
        let data = self.rt.block_on(self.objects.read_blob(&id)).map_err(io_error)?;
        s.sizes.insert(id, data.len() as u64);
        s.blobs.insert(id, data.clone(), self.opts.blob_cache_bytes);
        Ok(data)
    }

    fn attr(&self, s: &mut State, ino: INodeNo) -> Result<FileAttr> {
        let node = Self::node(s, ino)?;
        let size = if node.mode.is_blob() {
            match s.sizes.get(&node.id) {
                Some(size) => *size,
                None => self.blob(s, node.id)?.len() as u64,
            }
        } else {
            0
        };
        Ok(FileAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime: self.opts.mtime,
            mtime: self.opts.mtime,
            ctime: self.opts.mtime,
            crtime: self.opts.mtime,
            kind: kind(node.mode),
            perm: match node.mode {
                EntryMode::File => 0o444,
                EntryMode::Symlink => 0o777,
                _ => 0o555,
            },
            nlink: if kind(node.mode) == FileType::Directory { 2 } else { 1 },
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        })
    }

    fn lookup_inode(&self, s: &mut State, parent: INodeNo, name: &OsStr) -> Result<INodeNo> {
        let tree = self.tree(s, parent)?;
        if name == "." {
            return Ok(parent);
        }
        if name == ".." {
            return Ok(Self::node(s, parent)?.parent);
        }
        let entry = name.to_str().and_then(|name| tree.get(name)).ok_or(Errno::ENOENT)?;
        Ok(Self::inode(s, parent, entry))
    }
    fn read_range(&self, s: &mut State, ino: INodeNo, offset: u64, size: u32) -> Result<Bytes> {
        let node = Self::node(s, ino)?;
        if kind(node.mode) == FileType::Directory {
            return Err(Errno::EISDIR);
        }
        if kind(node.mode) != FileType::RegularFile {
            return Err(Errno::EINVAL);
        }
        let data = self.blob(s, node.id)?;
        let start = usize::try_from(offset).unwrap_or(usize::MAX).min(data.len());
        let end = start.saturating_add(size as usize).min(data.len());
        Ok(data.slice(start..end))
    }

    fn directory_page(
        &self,
        s: &mut State,
        ino: INodeNo,
        offset: u64,
        mut add: impl FnMut(INodeNo, u64, FileType, &str) -> bool,
    ) -> Result<()> {
        let tree = self.tree(s, ino)?;
        self.prefetch(s, ino, &tree)?;
        let parent = Self::node(s, ino)?.parent;
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        for index in start..tree.entries().len().saturating_add(2) {
            let (entry_ino, entry_kind, name) = match index {
                0 => (ino, FileType::Directory, "."),
                1 => (parent, FileType::Directory, ".."),
                _ => {
                    let entry = &tree.entries()[index - 2];
                    (Self::inode(s, ino, entry), kind(entry.mode), entry.name.as_str())
                }
            };
            if add(entry_ino, (index + 1) as u64, entry_kind, name) {
                break;
            }
        }
        Ok(())
    }
}

impl Filesystem for View {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let mut s = self.state.lock();
        match self.lookup_inode(&mut s, parent, name).and_then(|ino| self.attr(&mut s, ino)) {
            Ok(attr) => reply.entry(&self.opts.attr_ttl, &attr, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.attr(&mut self.state.lock(), ino) {
            Ok(attr) => reply.attr(&self.opts.attr_ttl, &attr),
            Err(e) => reply.error(e),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        if flags.0 & (libc::O_ACCMODE | libc::O_TRUNC | libc::O_APPEND) != 0 {
            reply.error(Errno::EROFS);
            return;
        }
        match Self::node(&self.state.lock(), ino) {
            Ok(node) if kind(node.mode) == FileType::RegularFile => {
                reply.opened(FileHandle(0), FopenFlags::FOPEN_KEEP_CACHE)
            }
            Ok(node) if kind(node.mode) == FileType::Directory => reply.error(Errno::EISDIR),
            Ok(_) => reply.error(Errno::EINVAL),
            Err(e) => reply.error(e),
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        match self.read_range(&mut self.state.lock(), ino, offset, size) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(e),
        }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        if flags.0 & (libc::O_ACCMODE | libc::O_TRUNC | libc::O_APPEND) != 0 {
            reply.error(Errno::EROFS);
            return;
        }
        match Self::node(&self.state.lock(), ino) {
            Ok(node) if kind(node.mode) == FileType::Directory => {
                reply.opened(FileHandle(0), FopenFlags::empty())
            }
            Ok(_) => reply.error(Errno::ENOTDIR),
            Err(e) => reply.error(e),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let mut s = self.state.lock();
        match Self::node(&s, ino).and_then(|node| {
            if node.mode != EntryMode::Symlink {
                return Err(Errno::EINVAL);
            }
            self.blob(&mut s, node.id)
        }) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(e),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let result =
            self.directory_page(&mut self.state.lock(), ino, offset, |ino, offset, kind, name| {
                reply.add(ino, offset, kind, name)
            });
        match result {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        reply.error(Errno::EROFS);
    }

    fn mknod(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(Errno::EROFS);
    }

    fn mkdir(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        reply.error(Errno::EROFS);
    }

    fn unlink(&self, _req: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::EROFS);
    }

    fn rmdir(&self, _req: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::EROFS);
    }

    fn symlink(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _link_name: &OsStr,
        _target: &Path,
        reply: ReplyEntry,
    ) {
        reply.error(Errno::EROFS);
    }

    fn rename(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _newparent: INodeNo,
        _newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::EROFS);
    }

    fn link(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _newparent: INodeNo,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(Errno::EROFS);
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _offset: u64,
        _data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        reply.error(Errno::EROFS);
    }

    fn setxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::EROFS);
    }

    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::EROFS);
    }

    fn create(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        reply.error(Errno::EROFS);
    }

    fn fallocate(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _offset: u64,
        _length: u64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::EROFS);
    }

    fn copy_file_range(
        &self,
        _req: &Request,
        _ino_in: INodeNo,
        _fh_in: FileHandle,
        _offset_in: u64,
        _ino_out: INodeNo,
        _fh_out: FileHandle,
        _offset_out: u64,
        _len: u64,
        _flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        reply.error(Errno::EROFS);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_core::EncodedObject;
    use ant_store::graph::build_tree;
    use ant_store::memory::MemoryObjectStore;
    use std::future::Future;
    use std::pin::Pin;

    #[derive(Default)]
    struct RecordingStore {
        inner: MemoryObjectStore,
        reads: Mutex<Vec<Vec<ObjectId>>>,
    }

    // Spell out async_trait's boxed futures to avoid adding a dependency.
    type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = ant_store::Result<T>> + Send + 'a>>;
    impl ObjectStore for RecordingStore {
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
                self.reads.lock().push(ids.to_vec());
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
    fn lru_eviction_and_disabled_cache() {
        let mut cache = BlobCache::default();
        let a = ObjectId([1; 32]);
        let b = ObjectId([2; 32]);
        let c = ObjectId([3; 32]);
        cache.insert(a, Bytes::from_static(b"aa"), 4);
        cache.insert(b, Bytes::from_static(b"bb"), 4);
        assert!(cache.get(&a).is_some());
        cache.insert(c, Bytes::from_static(b"cc"), 4);
        assert!(cache.get(&b).is_none());
        assert_eq!(cache.bytes, 4);
        cache.insert(b, Bytes::from_static(b"too large"), 4);
        assert!(cache.get(&b).is_none());
        cache.insert(b, Bytes::new(), 0);
        assert!(cache.get(&b).is_none());
        cache.insert(a, Bytes::from_static(b"a"), 4);
        assert_eq!(cache.bytes, 3);
    }

    #[test]
    fn lazy_batched_trees_and_distinct_stable_inodes() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = Arc::new(RecordingStore::default());
        let root = rt
            .block_on(build_tree(
                store.as_ref(),
                &[("a/x", b"same"), ("b/y", b"same"), ("c/x", b"same")],
            ))
            .unwrap();
        let view = View::new(store.clone(), root, rt.handle().clone(), MountOptions::default());
        assert!(store.reads.lock().is_empty());
        let mut s = view.state.lock();
        let tree = view.tree(&mut s, INodeNo(1)).unwrap();
        assert_eq!(store.reads.lock().len(), 1);
        view.prefetch(&mut s, INodeNo(1), &tree).unwrap();
        assert_eq!(store.reads.lock()[1].len(), 2); // a and c share a tree.
        view.prefetch(&mut s, INodeNo(1), &tree).unwrap();
        let a = view.lookup_inode(&mut s, INodeNo(1), OsStr::new("a")).unwrap();
        let c = view.lookup_inode(&mut s, INodeNo(1), OsStr::new("c")).unwrap();
        assert_ne!(a, c);
        let ax = view.lookup_inode(&mut s, a, OsStr::new("x")).unwrap();
        let cx = view.lookup_inode(&mut s, c, OsStr::new("x")).unwrap();
        assert_ne!(ax, cx);
        assert_eq!(view.lookup_inode(&mut s, a, OsStr::new("x")).unwrap(), ax);
        assert_eq!(view.lookup_inode(&mut s, a, OsStr::new("..")).unwrap(), INodeNo(1));
        assert_eq!(store.reads.lock().len(), 2);
        assert_eq!(view.attr(&mut s, ax).unwrap().size, 4);
        assert_eq!(view.attr(&mut s, cx).unwrap().size, 4);
        assert_eq!(store.reads.lock().len(), 3);
        assert_eq!(view.lookup_inode(&mut s, ax, OsStr::new("x")), Err(Errno::ENOTDIR));
        assert_eq!(view.lookup_inode(&mut s, a, OsStr::new("missing")), Err(Errno::ENOENT));
    }

    #[test]
    fn missing_wrong_kind_gitlinks_and_cached_sizes() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = Arc::new(RecordingStore::default());
        let blob = rt.block_on(store.put(EncodedObject::new(ObjectKind::Blob, b"hello"))).unwrap();
        let opts = MountOptions { blob_cache_bytes: 0, ..MountOptions::default() };
        let view = View::new(store.clone(), blob, rt.handle().clone(), opts);
        let mut s = view.state.lock();
        assert_eq!(view.tree(&mut s, INodeNo(1)).unwrap_err(), Errno::EIO);
        let missing = ObjectId([7; 32]);
        let gitlink = View::inode(
            &mut s,
            INodeNo(1),
            &TreeEntry { mode: EntryMode::Gitlink, id: missing, name: "vendor".into() },
        );
        let reads = store.reads.lock().len();
        assert!(view.tree(&mut s, gitlink).unwrap().is_empty());
        let attr = view.attr(&mut s, gitlink).unwrap();
        assert_eq!((attr.perm, attr.nlink), (0o555, 2));
        assert_eq!(store.reads.lock().len(), reads);
        assert_eq!(view.blob(&mut s, missing), Err(Errno::EIO));
        let file = View::inode(
            &mut s,
            INodeNo(1),
            &TreeEntry { mode: EntryMode::File, id: blob, name: "file".into() },
        );
        assert_eq!(view.attr(&mut s, file).unwrap().size, 5);
        let reads = store.reads.lock().len();
        assert_eq!(view.attr(&mut s, file).unwrap().size, 5);
        assert_eq!(store.reads.lock().len(), reads);
        assert!(s.blobs.data.is_empty());
        assert!(View::node(&s, INodeNo(0)).is_err());
        assert!(View::node(&s, INodeNo(u64::MAX)).is_err());
    }

    #[test]
    fn concurrent_lookup_reuses_inode_and_fetch() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = Arc::new(RecordingStore::default());
        let root = rt.block_on(build_tree(store.as_ref(), &[("file", b"data")])).unwrap();
        let view = View::new(store.clone(), root, rt.handle().clone(), MountOptions::default());
        std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        let mut s = view.state.lock();
                        let ino =
                            view.lookup_inode(&mut s, INodeNo(1), OsStr::new("file")).unwrap();
                        let attr = view.attr(&mut s, ino).unwrap();
                        assert_eq!(attr.size, 4);
                        ino
                    })
                })
                .collect();
            for thread in threads {
                assert_eq!(thread.join().unwrap(), INodeNo(2));
            }
        });
        assert_eq!(store.reads.lock().len(), 2);
    }

    #[test]
    fn reads_at_eof_and_extreme_offsets() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = Arc::new(RecordingStore::default());
        let root = rt.block_on(build_tree(store.as_ref(), &[("file", b"abcdef")])).unwrap();
        let view = View::new(store.clone(), root, rt.handle().clone(), MountOptions::default());
        let mut s = view.state.lock();
        let ino = view.lookup_inode(&mut s, INodeNo(1), OsStr::new("file")).unwrap();
        assert_eq!(view.read_range(&mut s, ino, 2, 2).unwrap().as_ref(), b"cd");
        assert_eq!(view.read_range(&mut s, ino, 4, u32::MAX).unwrap().as_ref(), b"ef");
        assert!(view.read_range(&mut s, ino, 6, 1).unwrap().is_empty());
        assert!(view.read_range(&mut s, ino, u64::MAX, u32::MAX).unwrap().is_empty());
        assert!(view.read_range(&mut s, ino, 0, 0).unwrap().is_empty());
        assert_eq!(view.read_range(&mut s, INodeNo(1), 0, 1), Err(Errno::EISDIR));
        assert_eq!(store.reads.lock().len(), 2);
    }

    #[test]
    fn directory_pages_preserve_offsets_and_dot_entries() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = Arc::new(RecordingStore::default());
        let names: Vec<_> = (0..1500).map(|i| format!("f{i:04}")).collect();
        let files: Vec<_> = names.iter().map(|n| (n.as_str(), b"x".as_slice())).collect();
        let root = rt.block_on(build_tree(store.as_ref(), &files)).unwrap();
        let view = View::new(store.clone(), root, rt.handle().clone(), MountOptions::default());
        let mut s = view.state.lock();
        let mut offset = 0;
        let mut listing = Vec::new();
        loop {
            let mut page = Vec::new();
            view.directory_page(&mut s, INodeNo(1), offset, |ino, next, kind, name| {
                if page.len() == 17 {
                    return true;
                }
                page.push((ino, next, kind, name.to_owned()));
                false
            })
            .unwrap();
            if page.is_empty() {
                break;
            }
            offset = page.last().unwrap().1;
            listing.extend(page);
        }
        assert_eq!(listing.len(), 1502);
        assert_eq!((listing[0].0, listing[0].3.as_str()), (INodeNo(1), "."));
        assert_eq!((listing[1].0, listing[1].3.as_str()), (INodeNo(1), ".."));
        for (entry, name) in listing[2..].iter().zip(names) {
            assert_eq!(entry.3, name);
            assert_eq!(entry.0, view.lookup_inode(&mut s, INodeNo(1), OsStr::new(&name)).unwrap());
        }
        view.directory_page(&mut s, INodeNo(1), u64::MAX, |_, _, _, _| panic!("past EOF")).unwrap();
        assert_eq!(store.reads.lock().len(), 1); // Listing never fetches file blobs.
    }

    #[test]
    fn rejects_current_thread_runtime_before_mounting() {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let result = crate::mount(
            Arc::new(MemoryObjectStore::default()),
            ObjectId::empty_tree(),
            Path::new("/unused"),
            rt.handle().clone(),
            MountOptions::default(),
        );
        assert_eq!(result.err().unwrap().kind(), std::io::ErrorKind::InvalidInput);
    }
}
