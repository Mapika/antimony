//! Lazy, read-only FUSE view of a tree. Nothing is downloaded until it is
//! looked up or read, so an agent can "check out" a huge monorepo instantly
//! and only pay for what it touches.
//!
//! [`mount`] uses `fuser` without libfuse (mounting goes through
//! `fusermount3`); [`mount_rw`] adds a copy-on-write layer (see [`rw`]).
//!
//! - Inodes: root = 1; numbers are assigned on first lookup and stay stable
//!   for the life of the mount. Identical objects at different paths get
//!   different inodes (no hard-link semantics).
//! - Directories 0o555, files 0o444, executables 0o555, symlinks 0o777,
//!   gitlinks appear as empty directories. `nlink` is 1 for files and 2 for
//!   directories; uid/gid are the mounting process's; mtime is UNIX_EPOCH or
//!   the timestamp in [`MountOptions`].
//! - `getattr` reports exact sizes; a blob is fetched (and its size cached)
//!   to learn its size.
//! - Read-only mounts answer every mutation, and opening for write, with
//!   `EROFS`.
//! - FUSE callbacks run on fuser's threads and block on the given Tokio
//!   runtime handle, which must belong to a multi-thread runtime.
//! - Caches: decoded trees (unbounded) and blob contents (LRU bounded by
//!   `MountOptions::blob_cache_bytes`). Listing a directory prefetches its
//!   subtrees with one `get_many`; `readdir` supports offsets.
//! - Store errors become `EIO` (logged with tracing).
//! - Dropping the returned `BackgroundSession` unmounts.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use ant_core::ObjectId;
use ant_store::ObjectStore;

mod filesystem;
pub mod rw;
pub use rw::mount_rw;

#[derive(Debug, Clone)]
pub struct MountOptions {
    pub attr_ttl: Duration,
    pub blob_cache_bytes: usize,
    pub mtime: SystemTime,
    pub fs_name: String,
}

impl Default for MountOptions {
    fn default() -> Self {
        Self {
            attr_ttl: Duration::from_secs(3600),
            blob_cache_bytes: 256 * 1024 * 1024,
            mtime: SystemTime::UNIX_EPOCH,
            fs_name: "antimony".into(),
        }
    }
}

/// Mount `root_tree` read-only at `mountpoint`. Requires `rt` to be a
/// multi-threaded runtime, kept alive until the session is dropped.
/// This function and session drop perform blocking mount/unmount I/O; async
/// callers should use `spawn_blocking` for both.
pub fn mount(
    objects: Arc<dyn ObjectStore>,
    root_tree: ObjectId,
    mountpoint: &Path,
    rt: tokio::runtime::Handle,
    opts: MountOptions,
) -> std::io::Result<fuser::BackgroundSession> {
    if rt.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "ant-fuse requires a multi-threaded Tokio runtime",
        ));
    }
    let mut config = fuser::Config::default();
    config.mount_options = vec![
        fuser::MountOption::RO,
        fuser::MountOption::DefaultPermissions,
        fuser::MountOption::FSName(opts.fs_name.clone()),
    ];
    fuser::spawn_mount(filesystem::View::new(objects, root_tree, rt, opts), mountpoint, &config)
}

/// Serializes every in-process kernel mount test.
///
/// Mounting and unmounting spawn `fusermount3` through a real `fork` (fuser
/// clears CLOEXEC in a `pre_exec` hook, which rules out `posix_spawn`). A test
/// process is both FUSE client and daemon, so another test may be inside a
/// direct-I/O `read` whose destination pages the kernel has grabbed with
/// `get_user_pages`. If a sibling thread forks then, those pages become
/// copy-on-write: the kernel fills the pre-fork copy, the reader's mapping
/// moves to a fresh one, and the read returns success with stale heap bytes.
/// Real deployments do not hit this because the daemon and its clients are
/// separate processes; only tests that mix both in one process need the gate.
#[cfg(test)]
pub(crate) static KERNEL_GATE: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::PathBuf;

    use super::*;
    use ant_core::{EncodedObject, EntryMode, Object, ObjectKind, Tree, TreeEntry};
    use ant_store::ObjectStoreExt;
    use ant_store::graph::build_tree;
    use ant_store::memory::MemoryObjectStore;

    async fn fixture() -> (Arc<MemoryObjectStore>, ObjectId) {
        let s = Arc::new(MemoryObjectStore::default());
        let sub = build_tree(s.as_ref(), &[("lib.rs", b"pub fn x() {}\n"), ("deep/er/f", b"deep")])
            .await
            .unwrap();
        let exe =
            s.put(EncodedObject::new(ObjectKind::Blob, b"#!/bin/sh\necho hi\n")).await.unwrap();
        let link = s.put(EncodedObject::new(ObjectKind::Blob, b"src/lib.rs")).await.unwrap();
        let big: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        let big_id = s.put(EncodedObject::new(ObjectKind::Blob, &big)).await.unwrap();
        let mut entries = vec![
            TreeEntry { mode: EntryMode::Tree, name: "src".into(), id: sub },
            TreeEntry { mode: EntryMode::Executable, name: "run.sh".into(), id: exe },
            TreeEntry { mode: EntryMode::Symlink, name: "link".into(), id: link },
            TreeEntry { mode: EntryMode::Gitlink, name: "vendor".into(), id: ObjectId([1; 32]) },
            TreeEntry { mode: EntryMode::File, name: "big.bin".into(), id: big_id },
        ];
        // Wide directory to exercise readdir offsets.
        let wide = build_tree(
            s.as_ref(),
            &(0..1500)
                .map(|i| (format!("f{i:04}"), b"w".as_slice()))
                .collect::<Vec<_>>()
                .iter()
                .map(|(n, c)| (n.as_str(), *c))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        entries.push(TreeEntry { mode: EntryMode::Tree, name: "wide".into(), id: wide });
        let root = s.put(Object::Tree(Tree::new(entries).unwrap()).encode()).await.unwrap();
        (s, root)
    }

    fn mount_fixture(
        rt: &tokio::runtime::Runtime,
    ) -> (parking_lot::MutexGuard<'static, ()>, tempfile::TempDir, PathBuf, fuser::BackgroundSession)
    {
        let gate = crate::KERNEL_GATE.lock();
        let (s, root) = rt.block_on(fixture());
        let tmp = tempfile::tempdir().unwrap();
        let mnt = tmp.path().join("mnt");
        std::fs::create_dir(&mnt).unwrap();
        let sess = mount(s, root, &mnt, rt.handle().clone(), MountOptions::default()).unwrap();
        (gate, tmp, mnt, sess)
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
    }

    #[test]
    fn reads_files_dirs_links() {
        let rt = rt();
        let (_gate, _tmp, mnt, _sess) = mount_fixture(&rt);
        assert_eq!(std::fs::read_to_string(mnt.join("src/lib.rs")).unwrap(), "pub fn x() {}\n");
        assert_eq!(std::fs::read_to_string(mnt.join("src/deep/er/f")).unwrap(), "deep");
        assert_eq!(std::fs::read_link(mnt.join("link")).unwrap(), PathBuf::from("src/lib.rs"));
        assert_eq!(std::fs::read_to_string(mnt.join("link")).unwrap(), "pub fn x() {}\n");
        assert!(mnt.join("vendor").is_dir());
        assert_eq!(std::fs::read_dir(mnt.join("vendor")).unwrap().count(), 0);

        let mut names: Vec<_> = std::fs::read_dir(&mnt)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, ["big.bin", "link", "run.sh", "src", "vendor", "wide"]);

        let m = std::fs::metadata(mnt.join("run.sh")).unwrap();
        assert_eq!(m.permissions().mode() & 0o777, 0o555);
        assert_eq!(m.len(), 18);
        let m = std::fs::metadata(mnt.join("src/lib.rs")).unwrap();
        assert_eq!(m.permissions().mode() & 0o777, 0o444);
        // Stable inodes.
        let i1 = std::fs::metadata(mnt.join("src/lib.rs")).unwrap().ino();
        let i2 = std::fs::metadata(mnt.join("src/lib.rs")).unwrap().ino();
        assert_eq!(i1, i2);
    }

    #[test]
    fn large_file_and_wide_dir() {
        let rt = rt();
        let (_gate, _tmp, mnt, _sess) = mount_fixture(&rt);
        let big = std::fs::read(mnt.join("big.bin")).unwrap();
        assert_eq!(big.len(), 3_000_000);
        assert!(big.iter().enumerate().all(|(i, &b)| b == (i % 251) as u8));
        assert_eq!(std::fs::read_dir(mnt.join("wide")).unwrap().count(), 1500);
    }

    #[test]
    fn read_only() {
        let rt = rt();
        let (_gate, _tmp, mnt, _sess) = mount_fixture(&rt);
        let e = std::fs::write(mnt.join("new"), b"x").unwrap_err();
        assert_eq!(e.raw_os_error(), Some(libc::EROFS));
        let e = std::fs::OpenOptions::new().write(true).open(mnt.join("src/lib.rs")).unwrap_err();
        assert_eq!(e.raw_os_error(), Some(libc::EROFS));
        assert!(std::fs::remove_file(mnt.join("run.sh")).is_err());
        assert!(std::fs::create_dir(mnt.join("d")).is_err());
        assert!(std::fs::metadata(mnt.join("missing")).is_err());
    }
}
