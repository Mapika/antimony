use std::collections::{BTreeSet, HashMap};
use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use ant_core::{EntryMode, ObjectId};
use ant_store::graph::build_tree;
use ant_store::memory::MemoryObjectStore;
use antd::workspace::LocalChangeKind;
use fuser::*;
use parking_lot::Mutex;

use super::state::*;
use super::*;

// Exercise the exact operation implementation even in a sandbox without
// /dev/fuse; the mount tests in `tests.rs` independently cover kernel integration.
#[test]
fn mutations_and_reset_without_kernel() {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
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
        lower: View::new(objects.clone(), tree, rt.handle().clone(), MountOptions::default()),
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
        o.rename(root, OsStr::new("dir"), root, OsStr::new("moved"), RenameFlags::empty())?;
        assert_eq!(o.path(b)?, "moved/sub/b");
        assert_eq!(fs::read(o.upper("moved/sub/b"))?, b"b");
        assert!(o.entry("dir").is_err());
        o.remove(root, OsStr::new("file"), false)?;
        let dir = o.create(root, OsStr::new("dir"), 0o755, None, true)?;
        o.create(dir, OsStr::new("new"), 0o644, None, false)?;
        assert_eq!(o.children("dir")?.len(), 1);
        o.create(root, OsStr::new("link"), 0o777, Some(Path::new("dir/new")), false)?;
        Ok(())
    })
    .unwrap();
    let changes = rt.block_on(changes(objects.clone(), state)).unwrap();
    let summary = changes.iter().map(|c| (c.path.as_str(), c.kind)).collect::<Vec<_>>();
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
        .block_on(build_tree(objects.as_ref(), &[("dir/new", b""), ("upstream", b"new")]))
        .unwrap();
    rt.block_on(reset(objects.clone(), state, ObjectId([2; 32]), new_tree)).unwrap();
    view.run(|o| {
        assert_eq!(o.children("dir")?.len(), 1);
        assert_eq!(o.entry("upstream")?.mode, EntryMode::File);
        assert!(!o.deleted.contains("dir"));
        Ok(())
    })
    .unwrap();
    assert!(!state.join("upper/dir/new").exists());
    assert_eq!(rt.block_on(super::changes(objects, state)).unwrap().len(), 3);
}

#[test]
fn status_scales_with_changed_paths_without_kernel() {
    use std::sync::atomic::Ordering;
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let objects = Arc::new(super::tests::CountingStore::default());
    let paths = (0..20_000).map(|i| format!("d{:03}/f{:03}", i / 100, i % 100)).collect::<Vec<_>>();
    let entries = paths.iter().map(|p| (p.as_str(), b"base".as_slice())).collect::<Vec<_>>();
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
    assert_eq!(whiteouts(state).unwrap(), BTreeSet::from(["old".into(), "new".into()]));
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
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
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
            &[("untouched", b"upstream"), ("dir/a", b"landed"), ("dir/b", b"old")],
        ))
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path();
    fs::create_dir_all(state.join("upper/dir")).unwrap();
    fs::write(state.join("upper/untouched"), b"old").unwrap();
    fs::write(state.join("upper/dir/a"), b"landed").unwrap();
    save_base(state, ObjectId([1; 32]), old).unwrap();
    save_whiteouts(state, &BTreeSet::from(["dir".into()])).unwrap();
    rt.block_on(reset(objects.clone(), state, ObjectId([2; 32]), new)).unwrap();
    assert!(!state.join("upper/untouched").exists());
    assert!(!state.join("upper/dir/a").exists());
    assert_eq!(whiteouts(state).unwrap(), BTreeSet::from(["dir/b".into()]));
    let changes = rt.block_on(changes(objects, state)).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].path.as_str(), "dir/b");
    assert_eq!(changes[0].kind, LocalChangeKind::Deleted);
}
