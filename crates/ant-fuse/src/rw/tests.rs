use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt, symlink};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use ant_core::{EncodedObject, EntryMode, ObjectId};
use ant_store::graph::build_tree;
use ant_store::memory::MemoryObjectStore;
use ant_store::{ObjectStore, ObjectStoreExt};
use antd::workspace::{LocalChange, LocalChangeKind};
use fuser::*;

use super::*;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
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
        Self { session: Some(session), mount, state, objects, tree, rt, _tmp: tmp, _gate: gate }
    }
    fn changes(&self) -> Vec<LocalChange> {
        self.rt.block_on(changes(self.objects.clone(), &self.state)).unwrap()
    }
    fn summary(&self) -> Vec<(String, LocalChangeKind)> {
        self.changes().into_iter().map(|c| (c.path.to_string(), c.kind)).collect()
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
    let file = OpenOptions::new().write(true).open(f.mount.join("file")).unwrap();
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
        fs::remove_dir(f.mount.join("dir")).unwrap_err().raw_os_error(),
        Some(libc::ENOTEMPTY)
    );
    fs::remove_dir_all(f.mount.join("dir")).unwrap();
    fs::create_dir(f.mount.join("dir")).unwrap();
    fs::write(f.mount.join("dir/new"), b"new").unwrap();
    assert!(!f.mount.join("dir/a").exists());
    assert_eq!(
        f.summary(),
        [("dir/a".into(), Deleted), ("dir/new".into(), Added), ("dir/sub/b".into(), Deleted)]
    );
    // One mount at a time (see `KERNEL_GATE`); `f`'s edits are checked.
    drop(f);
    let g = Fixture::new();
    let ino = fs::metadata(g.mount.join("dir/sub/b")).unwrap().ino();
    fs::rename(g.mount.join("dir"), g.mount.join("moved")).unwrap();
    assert_eq!(fs::read(g.mount.join("moved/sub/b")).unwrap(), b"b");
    assert_eq!(fs::metadata(g.mount.join("moved/sub/b")).unwrap().ino(), ino);
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
    assert_eq!(fs::metadata(f.mount.join("file")).unwrap().mode() & 0o777, 0o751);
    symlink("file", f.mount.join("link")).unwrap();
    assert_eq!(fs::read_link(f.mount.join("link")).unwrap(), PathBuf::from("file"));
    assert_eq!(fs::read(f.mount.join("link")).unwrap(), b"original");
    assert_eq!(f.summary(), [("file".into(), Modified), ("link".into(), Added)]);
    let changes = f.changes();
    assert_eq!(changes[0].new.unwrap().0, EntryMode::Executable);
    assert_eq!(changes[1].new.unwrap().0, EntryMode::Symlink);
    assert_eq!(
        fs::hard_link(f.mount.join("file"), f.mount.join("hard")).unwrap_err().raw_os_error(),
        Some(libc::EPERM)
    );
    let time = SystemTime::UNIX_EPOCH + Duration::from_secs(1234567);
    File::open(f.mount.join("file"))
        .unwrap()
        .set_times(fs::FileTimes::new().set_modified(time))
        .unwrap();
    assert_eq!(fs::metadata(f.mount.join("file")).unwrap().modified().unwrap(), time);
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
                        if let Some(n) =
                            name.strip_prefix('b').and_then(|n| n.parse::<usize>().ok())
                            && let Ok(got) = fs::read(e.path())
                        {
                            let want = format!("t{} i{n} {}", (t + 1) % 12, "x".repeat(n * 37));
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
            &[("file", b"landed"), ("dir/sub/b", b"b"), ("upstream", b"new")],
        ))
        .unwrap();
    f.rt.block_on(reset(f.objects.clone(), &f.state, ObjectId([2; 32]), tree)).unwrap();
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
    let paths = (0..20_000).map(|i| format!("d{:03}/f{:03}", i / 100, i % 100)).collect::<Vec<_>>();
    let entries = paths.iter().map(|p| (p.as_str(), b"base".as_slice())).collect::<Vec<_>>();
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
    assert!(objects.reads.load(Ordering::Relaxed) <= 24, "too many base reads");
    assert_eq!(&rt.block_on(objects.read_blob(&changes[0].new.unwrap().1)).unwrap()[..], b"one");
    session.umount_and_join().unwrap();
}
