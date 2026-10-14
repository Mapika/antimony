use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use ant_core::{EntryMode, ObjectId};
use ant_store::graph::build_tree;
use ant_store::memory::MemoryObjectStore;
use antd::workspace::LocalChangeKind;
use fuser::*;

use super::state::*;
use super::*;
use crate::test_support::{CountingStore, runtime};

// Exercise the exact operation implementation even in a sandbox without
// /dev/fuse; the mount tests in `tests.rs` independently cover kernel integration.
#[test]
fn mutations_and_reset_without_kernel() {
    let rt = runtime();
    let (objects, tree, temp) = workspace::<MemoryObjectStore>(
        &rt,
        &[("file", b"original"), ("dir/a", b"a"), ("dir/sub/b", b"b")],
    );
    let state = temp.path();
    let view = Writable::open(
        MountGuard::take(state).unwrap(),
        View::new(objects.clone(), tree, rt.handle().clone(), MountOptions::default()),
        state.to_path_buf(),
    )
    .unwrap();
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
    let rt = runtime();
    let paths = (0..20_000).map(|i| format!("d{:03}/f{:03}", i / 100, i % 100)).collect::<Vec<_>>();
    let entries = paths.iter().map(|p| (p.as_str(), b"base".as_slice())).collect::<Vec<_>>();
    let (objects, _tree, temp) = workspace::<CountingStore>(&rt, &entries);
    let state = temp.path();
    fs::create_dir_all(state.join("upper/d001")).unwrap();
    fs::create_dir_all(state.join("upper/d199")).unwrap();
    fs::write(state.join("upper/d001/f001"), b"one").unwrap();
    fs::write(state.join("upper/d199/f099"), b"two").unwrap();
    objects.reset();
    let changes = rt.block_on(super::changes(objects.clone(), state)).unwrap();
    assert_eq!(changes.len(), 2);
    assert_eq!(objects.ids(), 3);
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
    let rt = runtime();
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

/// A state directory (empty upper layer, no whiteouts) on a base tree of
/// `files`, stored in a new `S`.
fn workspace<S: ObjectStore + Default>(
    rt: &tokio::runtime::Runtime,
    files: &[(&str, &[u8])],
) -> (Arc<S>, ObjectId, tempfile::TempDir) {
    let objects = Arc::new(S::default());
    let tree = rt.block_on(build_tree(objects.as_ref(), files)).unwrap();
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir(temp.path().join("upper")).unwrap();
    save_base(temp.path(), ObjectId([1; 32]), tree).unwrap();
    save_whiteouts(temp.path(), &BTreeSet::new()).unwrap();
    (objects, tree, temp)
}

#[test]
fn whiteouts_are_journaled_and_compacted_past_the_threshold() {
    use super::journal::JOURNAL;
    let rt = runtime();
    let names = (0..300).map(|i| format!("f{i:03}")).collect::<Vec<_>>();
    let paths = names.iter().map(|n| format!("d/{n}")).collect::<Vec<_>>();
    let files = paths.iter().map(|p| (p.as_str(), b"x".as_slice())).collect::<Vec<_>>();
    let (objects, tree, temp) = workspace::<MemoryObjectStore>(&rt, &files);
    let state = temp.path();
    let open = || {
        Writable::open(
            MountGuard::take(state).unwrap(),
            View::new(objects.clone(), tree, rt.handle().clone(), MountOptions::default()),
            state.to_path_buf(),
        )
        .unwrap()
    };
    let mut view = open();
    view.compact_bytes = 2048;
    let mut largest = 0;
    for name in &names {
        view.run_mut(|o| {
            let d = o.inode(INodeNo(1), OsStr::new("d"))?;
            o.remove(d, OsStr::new(name), false)
        })
        .unwrap();
        largest = largest.max(fs::metadata(state.join(JOURNAL)).unwrap().len());
        // No per-operation rewrite of the snapshot.
        assert!(snapshot_whiteouts(state).unwrap().len() <= 300);
    }
    assert!(largest <= 2048 + 64, "journal grew to {largest}");
    // Compaction happened: the snapshot holds most deletions, and snapshot
    // plus journal hold all of them.
    assert!(snapshot_whiteouts(state).unwrap().len() >= 200);
    let all: BTreeSet<String> = paths.iter().cloned().collect();
    assert_eq!(whiteouts(state).unwrap(), all);
    let changes = rt.block_on(super::changes(objects.clone(), state)).unwrap();
    assert_eq!(changes.len(), 300);
    assert!(changes.iter().all(|c| c.kind == LocalChangeKind::Deleted));
    // Survives reopening (mount-time recovery and compaction).
    drop(view);
    let view = open();
    assert_eq!(fs::metadata(state.join(JOURNAL)).unwrap().len(), 0);
    assert_eq!(snapshot_whiteouts(state).unwrap(), all);
    view.run(|o| {
        assert!(o.children("d")?.is_empty());
        Ok(())
    })
    .unwrap();
}

#[test]
fn trailing_journal_rename_rolls_forward_once() {
    use super::journal::{JOURNAL, Record, append_durably, parse};
    let rt = runtime();
    let (_objects, _tree, temp) = workspace::<MemoryObjectStore>(&rt, &[("old", b"lower")]);
    let state = temp.path();
    fs::write(state.join("upper/old"), b"copied up").unwrap();
    // Crash after journaling the rename, before moving the upper copy.
    append_durably(state, &Record::Rename { old: "old".into(), new: "new".into() }).unwrap();
    recover(state).unwrap();
    assert_eq!(fs::read(state.join("upper/new")).unwrap(), b"copied up");
    assert!(!state.join("upper/old").exists());
    assert_eq!(whiteouts(state).unwrap(), BTreeSet::from(["old".into(), "new".into()]));
    let (records, _) = parse(&fs::read(state.join(JOURNAL)).unwrap()).unwrap();
    assert_eq!(records.last(), Some(&Record::Done));
    // Completed: a later upper "old" is never moved again.
    fs::write(state.join("upper/old"), b"recreated").unwrap();
    recover(state).unwrap();
    assert_eq!(fs::read(state.join("upper/old")).unwrap(), b"recreated");
    assert_eq!(fs::read(state.join("upper/new")).unwrap(), b"copied up");
}

#[test]
fn torn_journal_tail_is_dropped_before_appending() {
    use super::journal::{JOURNAL, Record, append_durably, parse};
    let rt = runtime();
    let (_objects, _tree, temp) = workspace::<MemoryObjectStore>(&rt, &[("a", b"a"), ("b", b"b")]);
    let state = temp.path();
    append_durably(state, &Record::Whiteout("a".into())).unwrap();
    let good = fs::metadata(state.join(JOURNAL)).unwrap().len();
    // A crash in the middle of the next append.
    let torn = Record::Whiteout("b".into()).frame();
    let mut f = fs::OpenOptions::new().append(true).open(state.join(JOURNAL)).unwrap();
    std::io::Write::write_all(&mut f, &torn[..torn.len() - 3]).unwrap();
    drop(f);
    assert_eq!(whiteouts(state).unwrap(), BTreeSet::from(["a".into()]));
    recover(state).unwrap();
    assert_eq!(fs::metadata(state.join(JOURNAL)).unwrap().len(), good);
    append_durably(state, &Record::Whiteout("b".into())).unwrap();
    let (records, len) = parse(&fs::read(state.join(JOURNAL)).unwrap()).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(len as u64, fs::metadata(state.join(JOURNAL)).unwrap().len());
    assert_eq!(whiteouts(state).unwrap(), BTreeSet::from(["a".into(), "b".into()]));
}
