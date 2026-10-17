use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use super::*;
use crate::test_support::CountingStore;
use ant_core::{EncodedObject, Object, ObjectKind, Tree, TreeEntry};
use ant_store::ObjectStoreExt;
use ant_store::graph::build_tree;
use ant_store::memory::MemoryObjectStore;

fn p(s: &str) -> RepoPath {
    RepoPath::new(s).unwrap()
}

const C: ObjectId = ObjectId([0xc0; 32]);
const C2: ObjectId = ObjectId([0xc2; 32]);

#[test]
fn sparse_matching() {
    let s = Sparse::new(vec!["src/foo".into(), "docs/*.md".into(), "**/BUILD".into()]).unwrap();
    assert!(s.matches(&p("src/foo")));
    assert!(s.matches(&p("src/foo/a/b.rs")));
    assert!(!s.matches(&p("src/foobar/x")));
    assert!(s.matches(&p("docs/a.md")));
    assert!(!s.matches(&p("docs/sub/a.md")));
    assert!(s.matches(&p("x/y/BUILD")));
    assert!(Sparse::all().matches(&p("anything")));

    let prefix_only = Sparse::new(vec!["src/foo".into()]).unwrap();
    assert!(prefix_only.may_contain_under(&p("src")));
    assert!(prefix_only.may_contain_under(&p("src/foo/bar")));
    assert!(!prefix_only.may_contain_under(&p("lib")));
    assert!(!prefix_only.may_contain_under(&p("src/foobar")));

    // Directories wholly in scope (fetched with their contents as bundles).
    let m = prefix_only.compile().unwrap();
    assert!(m.covers_all_under(&p("src/foo")) && m.covers_all_under(&p("src/foo/x")));
    assert!(!m.covers_all_under(&p("src")) && !m.covers_all_under(&RepoPath::root()));
    assert!(!m.covers_all_under(&p("src/foobar")));
    assert!(Sparse::all().compile().unwrap().covers_all_under(&RepoPath::root()));
    let globs = Sparse::new(vec!["**/BUILD".into()]).unwrap().compile().unwrap();
    assert!(!globs.covers_all_under(&p("x")));
}

#[tokio::test]
async fn materialize_full_and_status_clean() {
    let objs = MemoryObjectStore::default();
    let t = build_tree(&objs, &[("a.txt", b"A"), ("d/e/f.rs", b"F"), ("d/g", b"G")]).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let ws = Workspace::materialize(&objs, &dir, C, t, Sparse::all()).await.unwrap();
    assert_eq!(std::fs::read(dir.join("d/e/f.rs")).unwrap(), b"F");
    assert!(ws.status(&objs).await.unwrap().is_empty());

    let reopened = Workspace::open(&dir).unwrap();
    assert_eq!(reopened.base_commit, C);
    assert_eq!(reopened.base_tree, t);
    assert!(reopened.status(&objs).await.unwrap().is_empty());
}

/// Workspaces from before the format line keep working; a newer format
/// is refused with a recognizable error.
#[tokio::test]
async fn workspace_format_versions() {
    let objs = MemoryObjectStore::default();
    let t = build_tree(&objs, &[("a.txt", b"A")]).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let mut ws =
        Workspace::materialize(&objs, &dir, C, t, Sparse::new(vec!["a.txt".into()]).unwrap())
            .await
            .unwrap();
    let config = dir.join(".ant/workspace");
    let written = fs::read_to_string(&config).unwrap();
    assert!(written.starts_with(&format!("format={WORKSPACE_FORMAT}\n")), "{written}");

    // The layout written before versioning: no format line.
    fs::write(&config, format!("base_commit={C}\nbase_tree={t}\nsparse=a.txt\n")).unwrap();
    let old = Workspace::open(&dir).unwrap();
    assert_eq!((old.base_commit, old.base_tree), (C, t));
    assert_eq!(old.sparse.patterns, vec!["a.txt".to_string()]);
    assert!(old.status(&objs).await.unwrap().is_empty());
    // Rewritten state records the version.
    ws.reset_to(&objs, C2, t).await.unwrap();
    assert!(fs::read_to_string(&config).unwrap().starts_with("format=1\n"));

    // Newer: refused before anything else in the file is interpreted.
    fs::write(&config, "format=2\nbase_commit=not-an-id-in-v1\n").unwrap();
    let err = Workspace::open(&dir).err().unwrap();
    let newer = err.downcast_ref::<ant_core::NewerFormat>().unwrap();
    assert_eq!((newer.found, newer.supported), (2, WORKSPACE_FORMAT));
    fs::write(&config, format!("format=x\nbase_commit={C}\nbase_tree={t}\n")).unwrap();
    let err = Workspace::open(&dir).err().unwrap();
    assert!(err.downcast_ref::<ant_core::NewerFormat>().is_none());
    assert!(err.to_string().contains("corrupt workspace format"), "{err}");
}

#[tokio::test]
async fn sparse_skips_unneeded_subtrees() {
    let objs = CountingStore::default();
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    for i in 0..50 {
        files.push((format!("other{i}/x/y.rs"), b"o".to_vec()));
    }
    files.push(("want/a.rs".into(), b"w".to_vec()));
    let refs: Vec<(&str, &[u8])> = files.iter().map(|(a, b)| (a.as_str(), b.as_slice())).collect();
    let t = build_tree(objs.inner(), &refs).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    Workspace::materialize(&objs, &dir, C, t, Sparse::new(vec!["want".into()]).unwrap())
        .await
        .unwrap();
    assert!(dir.join("want/a.rs").exists());
    assert!(!dir.join("other0").exists());
    // root tree level + "want" level + one blob batch; definitely not ~150.
    assert!(objs.calls() <= 4, "calls = {}", objs.calls());
}

#[tokio::test]
async fn modes_symlinks_and_gitlinks() {
    let objs = MemoryObjectStore::default();
    let exe = objs.put(EncodedObject::new(ObjectKind::Blob, b"#!/bin/sh\n")).await.unwrap();
    let target = objs.put(EncodedObject::new(ObjectKind::Blob, b"run.sh")).await.unwrap();
    let tree = Object::Tree(
        Tree::new(vec![
            TreeEntry { mode: EntryMode::Executable, name: "run.sh".into(), id: exe },
            TreeEntry { mode: EntryMode::Symlink, name: "link".into(), id: target },
            TreeEntry { mode: EntryMode::Gitlink, name: "sub".into(), id: ObjectId([5; 32]) },
        ])
        .unwrap(),
    )
    .encode();
    let t = objs.put(tree).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let ws = Workspace::materialize(&objs, &dir, C, t, Sparse::all()).await.unwrap();
    let m = std::fs::metadata(dir.join("run.sh")).unwrap().permissions().mode();
    assert_eq!(m & 0o111, 0o111);
    assert_eq!(std::fs::read_link(dir.join("link")).unwrap(), PathBuf::from("run.sh"));
    assert!(dir.join("sub").is_dir());
    assert!(ws.status(&objs).await.unwrap().is_empty());
}

/// Trees from old histories (`100664`, `100775`, zero-padded `040000`)
/// check out like their semantic class and show a clean status.
#[tokio::test]
async fn legacy_modes_check_out_as_their_class() {
    let legacy = |m: &str| EntryMode::from_octal(m.as_bytes()).unwrap();
    let objs = MemoryObjectStore::default();
    let plain = objs.put(EncodedObject::new(ObjectKind::Blob, b"plain\n")).await.unwrap();
    let exe = objs.put(EncodedObject::new(ObjectKind::Blob, b"#!/bin/sh\n")).await.unwrap();
    let inner = Object::Tree(
        Tree::new(vec![TreeEntry { mode: legacy("100664"), name: "f".into(), id: plain }]).unwrap(),
    )
    .encode();
    let inner = objs.put(inner).await.unwrap();
    let tree = Object::Tree(
        Tree::new(vec![
            TreeEntry { mode: legacy("100640"), name: "plain".into(), id: plain },
            TreeEntry { mode: legacy("100775"), name: "run.sh".into(), id: exe },
            TreeEntry { mode: legacy("040000"), name: "dir".into(), id: inner },
        ])
        .unwrap(),
    )
    .encode();
    let t = objs.put(tree).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let ws = Workspace::materialize(&objs, &dir, C, t, Sparse::all()).await.unwrap();
    let mode = |p: &str| std::fs::metadata(dir.join(p)).unwrap().permissions().mode();
    assert_eq!(mode("plain") & 0o111, 0);
    assert_eq!(mode("run.sh") & 0o111, 0o111);
    assert_eq!(std::fs::read(dir.join("dir/f")).unwrap(), b"plain\n");
    assert!(ws.status(&objs).await.unwrap().is_empty());
}

#[tokio::test]
async fn status_detects_changes() {
    let objs = MemoryObjectStore::default();
    let t = build_tree(
        &objs,
        &[("keep", b"k"), ("mod", b"m"), ("del", b"d"), ("in/scope", b"s"), ("out/side", b"o")],
    )
    .await
    .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let sparse = Sparse::new(vec!["keep".into(), "mod".into(), "del".into(), "in".into()]).unwrap();
    let ws = Workspace::materialize(&objs, &dir, C, t, sparse).await.unwrap();
    assert!(!dir.join("out").exists());

    std::fs::write(dir.join("mod"), b"m2").unwrap();
    std::fs::remove_file(dir.join("del")).unwrap();
    std::fs::create_dir_all(dir.join("new/dir")).unwrap();
    std::fs::write(dir.join("new/dir/f"), b"n").unwrap();
    std::fs::create_dir_all(dir.join("empty")).unwrap();
    // Same size, different content, and force same mtime: must still be
    // caught because we compare inode/mtime/size and rehash on mismatch —
    // here the content changes size-preservingly, so rewrite via a new inode.
    std::fs::write(dir.join("in/scope.tmp"), b"S").unwrap();
    std::fs::rename(dir.join("in/scope.tmp"), dir.join("in/scope")).unwrap();
    std::fs::set_permissions(dir.join("keep"), std::fs::Permissions::from_mode(0o755)).unwrap();

    let st = ws.status(&objs).await.unwrap();
    let summary: Vec<_> = st.iter().map(|c| (c.path.as_str(), c.kind)).collect();
    assert_eq!(
        summary,
        [
            ("del", LocalChangeKind::Deleted),
            ("in/scope", LocalChangeKind::Modified),
            ("keep", LocalChangeKind::Modified),
            ("mod", LocalChangeKind::Modified),
            ("new/dir/f", LocalChangeKind::Added),
        ]
    );
    let keep = st.iter().find(|c| c.path.as_str() == "keep").unwrap();
    assert_eq!(keep.new.unwrap().0, EntryMode::Executable);
    let m = st.iter().find(|c| c.path.as_str() == "mod").unwrap();
    assert_eq!(&objs.read_blob(&m.new.unwrap().1).await.unwrap()[..], b"m2");
}

#[tokio::test]
async fn parallel_scan_many_dirs_and_deletions() {
    let objs = MemoryObjectStore::default();
    let files: Vec<(String, Vec<u8>)> =
        (0..400).map(|i| (format!("d{}/s{}/f{i}", i % 20, i % 7), vec![b'x'; i % 50])).collect();
    let refs: Vec<(&str, &[u8])> = files.iter().map(|(a, b)| (a.as_str(), b.as_slice())).collect();
    let t = build_tree(&objs, &refs).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let ws = Workspace::materialize(&objs, &dir, C, t, Sparse::all()).await.unwrap();
    assert!(ws.status(&objs).await.unwrap().is_empty());
    std::fs::remove_file(dir.join(&files[3].0)).unwrap();
    std::fs::remove_file(dir.join(&files[399].0)).unwrap();
    std::fs::write(dir.join("d1/new"), b"n").unwrap();
    let st = ws.status(&objs).await.unwrap();
    let summary: Vec<_> = st.iter().map(|c| (c.path.as_str().to_string(), c.kind)).collect();
    let mut expect = vec![
        (files[3].0.clone(), LocalChangeKind::Deleted),
        (files[399].0.clone(), LocalChangeKind::Deleted),
        ("d1/new".to_string(), LocalChangeKind::Added),
    ];
    expect.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(summary, expect);
}

/// Directories are created by the walk: exactly those on the way to a file
/// in scope, also when only a deeper level has files, and never one whose
/// files are all out of scope (a glob pattern does not prune the walk).
#[tokio::test]
async fn only_directories_with_files_in_scope_are_created() {
    let objs = MemoryObjectStore::default();
    let mut files: Vec<(String, Vec<u8>)> = vec![
        ("a/b/c/d/BUILD".into(), b"1".to_vec()),
        ("a/x.rs".into(), b"2".to_vec()),
        ("e/f/g.rs".into(), b"3".to_vec()),
        ("BUILD".into(), b"4".to_vec()),
    ];
    // Wide and deep enough for many concurrent expansions and writers.
    for i in 0..3000 {
        files.push((format!("w{}/v{}/u{}/f{i}", i % 13, i % 29, i % 5), vec![b'z'; i % 300]));
    }
    files.push(("w0/v0/u0/BUILD".into(), b"5".to_vec()));
    let refs: Vec<(&str, &[u8])> = files.iter().map(|(a, b)| (a.as_str(), b.as_slice())).collect();
    let t = build_tree(&objs, &refs).await.unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("globbed");
    let ws =
        Workspace::materialize(&objs, &dir, C, t, Sparse::new(vec!["**/BUILD".into()]).unwrap())
            .await
            .unwrap();
    let mut dirs = Vec::new();
    let mut stack = vec![dir.clone()];
    while let Some(d) = stack.pop() {
        for e in fs::read_dir(&d).unwrap() {
            let e = e.unwrap();
            if e.file_type().unwrap().is_dir() && e.file_name() != ".ant" {
                dirs.push(e.path().strip_prefix(&dir).unwrap().to_str().unwrap().to_string());
                stack.push(e.path());
            }
        }
    }
    dirs.sort();
    assert_eq!(dirs, ["a", "a/b", "a/b/c", "a/b/c/d", "w0", "w0/v0", "w0/v0/u0"]);
    assert_eq!(fs::read(dir.join("a/b/c/d/BUILD")).unwrap(), b"1");
    assert!(ws.status(&objs).await.unwrap().is_empty());

    let dir = tmp.path().join("full");
    let ws = Workspace::materialize(&objs, &dir, C, t, Sparse::all()).await.unwrap();
    for (path, body) in &files {
        assert_eq!(&fs::read(dir.join(path)).unwrap(), body, "{path}");
    }
    assert!(ws.status(&objs).await.unwrap().is_empty());
}

#[tokio::test]
async fn reset_to_new_base() {
    let objs = Arc::new(MemoryObjectStore::default());
    let t1 = build_tree(objs.as_ref(), &[("a", b"1"), ("b/c", b"2")]).await.unwrap();
    let t2 = build_tree(objs.as_ref(), &[("a", b"1-mine"), ("d", b"upstream")]).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let mut ws = Workspace::materialize(objs.as_ref(), &dir, C, t1, Sparse::all()).await.unwrap();
    // Local change that is identical in t2 (we "landed" it).
    std::fs::write(dir.join("a"), b"1-mine").unwrap();
    ws.reset_to(objs.as_ref(), C2, t2).await.unwrap();
    assert_eq!(std::fs::read(dir.join("d")).unwrap(), b"upstream");
    assert!(!dir.join("b").exists());
    assert!(ws.status(objs.as_ref()).await.unwrap().is_empty());
    assert_eq!(Workspace::open(&dir).unwrap().base_commit, C2);

    // Dirty change not in the new tree → refuse.
    std::fs::write(dir.join("d"), b"local edit").unwrap();
    assert!(ws.reset_to(objs.as_ref(), C, t1).await.is_err());
}

#[tokio::test]
async fn reset_to_indexes_landed_out_of_scope_add() {
    let objs = Arc::new(MemoryObjectStore::default());
    let t1 = build_tree(objs.as_ref(), &[("a", b"1"), ("b/c", b"2")]).await.unwrap();
    let t2 =
        build_tree(objs.as_ref(), &[("a", b"1"), ("b/c", b"2"), ("x", b"landed")]).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let sparse = Sparse::new(vec!["a".into()]).unwrap();
    let mut ws = Workspace::materialize(objs.as_ref(), &dir, C, t1, sparse).await.unwrap();
    // Added outside the sparse scope, identical to the landed tree.
    std::fs::write(dir.join("x"), b"landed").unwrap();
    ws.reset_to(objs.as_ref(), C2, t2).await.unwrap();
    // It must now be indexed, so the workspace is clean and not resent.
    assert!(ws.status(objs.as_ref()).await.unwrap().is_empty());
    assert!(!dir.join("b").exists());
}
fn summary(changes: &[LocalChange]) -> Vec<(String, LocalChangeKind)> {
    changes.iter().map(|c| (c.path.as_str().to_string(), c.kind)).collect()
}

#[tokio::test]
async fn update_carries_local_edits_onto_new_base() {
    use LocalChangeKind::*;
    let objs = Arc::new(MemoryObjectStore::default());
    let files1: &[(&str, &[u8])] = &[
        ("both", b"1\n2\n3\n4\n5\n"),
        ("clash", b"1\n2\n3\n"),
        ("gone", b"g"),
        ("keep", b"k"),
        ("local", b"l"),
        ("up", b"u"),
        ("moddel", b"m"),
    ];
    let files2: &[(&str, &[u8])] = &[
        ("both", b"one\n2\n3\n4\n5\n"),
        ("clash", b"1\nup\n3\n"),
        ("keep", b"k"),
        ("local", b"l"),
        ("up", b"u2"),
        ("new/file", b"n"),
    ];
    let t1 = build_tree(objs.as_ref(), files1).await.unwrap();
    let t2 = build_tree(objs.as_ref(), files2).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let mut ws = Workspace::materialize(objs.as_ref(), &dir, C, t1, Sparse::all()).await.unwrap();
    std::fs::write(dir.join("both"), b"1\n2\n3\n4\nfive\n").unwrap();
    std::fs::write(dir.join("clash"), b"1\nmine\n3\n").unwrap();
    std::fs::write(dir.join("local"), b"l2").unwrap();
    std::fs::write(dir.join("moddel"), b"m2").unwrap();
    std::fs::write(dir.join("added"), b"a").unwrap();
    std::fs::remove_file(dir.join("keep")).unwrap();
    let plan = ws.update(objs.as_ref(), C2, t2).await.unwrap();
    assert_eq!(ws.base_commit, C2);
    let conflicts: Vec<&str> =
        plan.iter().filter(|u| u.conflict).map(|u| u.path.as_str()).collect();
    assert_eq!(conflicts, ["clash", "moddel"]);
    assert_eq!(read_conflicts(&conflicts_file(&dir)).unwrap(), [p("clash"), p("moddel")]);
    let read = |f: &str| std::fs::read(dir.join(f)).unwrap();
    assert_eq!(read("both"), b"one\n2\n3\n4\nfive\n");
    assert_eq!(
        read("clash"),
        b"1\n<<<<<<< upstream\nup\n||||||| base\n2\n=======\nmine\n>>>>>>> local\n3\n"
    );
    assert_eq!(read("up"), b"u2");
    assert_eq!(read("new/file"), b"n");
    assert_eq!(read("local"), b"l2");
    assert_eq!(read("moddel"), b"m2");
    assert!(!dir.join("gone").exists());
    assert!(!dir.join("keep").exists());
    let reopened = Workspace::open(&dir).unwrap();
    assert_eq!((reopened.base_commit, reopened.base_tree), (C2, t2));
    assert_eq!(
        summary(&ws.status(objs.as_ref()).await.unwrap()),
        [
            ("added".into(), Added),
            ("both".into(), Modified),
            ("clash".into(), Modified),
            ("keep".into(), Deleted),
            ("local".into(), Modified),
            ("moddel".into(), Added),
        ]
    );
    // Nothing left to carry: a second update is a no-op.
    let again = ws.update(objs.as_ref(), C2, t2).await.unwrap();
    assert!(again.iter().all(|u| u.target == crate::update::Target::Local));
    write_conflicts(&conflicts_file(&dir), &[]).unwrap();
    assert!(read_conflicts(&conflicts_file(&dir)).unwrap().is_empty());
}

#[tokio::test]
async fn update_respects_sparse_scope() {
    let objs = Arc::new(MemoryObjectStore::default());
    let t1 = build_tree(objs.as_ref(), &[("in/a", b"1"), ("out/b", b"2")]).await.unwrap();
    let t2 = build_tree(
        objs.as_ref(),
        &[("in/a", b"3"), ("in/n", b"4"), ("out/b", b"5"), ("out/c", b"6")],
    )
    .await
    .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let sparse = Sparse::new(vec!["in".into()]).unwrap();
    let mut ws = Workspace::materialize(objs.as_ref(), &dir, C, t1, sparse).await.unwrap();
    // A local file outside the scope, identical to upstream's new file.
    std::fs::create_dir_all(dir.join("out")).unwrap();
    std::fs::write(dir.join("out/c"), b"6").unwrap();
    let plan = ws.update(objs.as_ref(), C2, t2).await.unwrap();
    assert!(plan.iter().all(|u| !u.conflict));
    assert_eq!(std::fs::read(dir.join("in/a")).unwrap(), b"3");
    assert_eq!(std::fs::read(dir.join("in/n")).unwrap(), b"4");
    assert!(!dir.join("out/b").exists());
    let st = ws.status(objs.as_ref()).await.unwrap();
    assert!(st.is_empty(), "{st:?}");
}

fn kinds(changes: &[LocalChange]) -> Vec<(String, LocalChangeKind)> {
    changes.iter().map(|c| (c.path.to_string(), c.kind)).collect()
}

fn put(dir: &Path, path: &str, body: &str) {
    let full = dir.join(path);
    fs::create_dir_all(full.parent().unwrap()).unwrap();
    fs::write(full, body).unwrap();
}

#[tokio::test]
async fn ignored_untracked_files_are_not_reported() {
    let objs = MemoryObjectStore::default();
    let t = build_tree(
        &objs,
        &[
            (".gitignore", b"target/\n*.log\n!keep.log\n/rootonly\n"),
            ("src/lib.rs", b"L"),
            ("src/.gitignore", b"gen_*\n"),
        ],
    )
    .await
    .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let ws = Workspace::materialize(&objs, &dir, C, t, Sparse::all()).await.unwrap();
    // Build-like output.
    put(&dir, "target/debug/app", "bin");
    put(&dir, "crates/x/target/out.o", "o");
    put(&dir, "a.log", "l");
    put(&dir, "keep.log", "k");
    put(&dir, "rootonly", "r");
    put(&dir, "sub/rootonly", "not anchored here");
    put(&dir, "src/gen_a.rs", "g");
    put(&dir, "other/gen_a.rs", "not covered by src/.gitignore");
    // A file named `target` is not a directory: dir-only pattern.
    put(&dir, "docs/target", "file");
    // Workspace-local rules.
    put(&dir, ".ant/ignore", "scratch/\n");
    put(&dir, "scratch/notes", "n");

    let (changes, ignored) = ws.status_with_ignored(&objs).await.unwrap();
    let got: Vec<String> = kinds(&changes).into_iter().map(|(p, _)| p).collect();
    assert_eq!(got, ["docs/target", "keep.log", "other/gen_a.rs", "sub/rootonly"]);
    assert!(changes.iter().all(|c| c.kind == LocalChangeKind::Added));
    assert_eq!(
        ignored,
        ["a.log", "crates/x/target/", "rootonly", "scratch/", "src/gen_a.rs", "target/"]
    );
    // `status` agrees with the first half.
    assert_eq!(ws.status(&objs).await.unwrap(), changes);
}

#[tokio::test]
async fn tracked_files_stay_tracked_despite_ignore_rules() {
    use LocalChangeKind::{Deleted, Modified};
    let objs = MemoryObjectStore::default();
    let t = build_tree(
        &objs,
        &[
            (".gitignore", b"*.lock\nvendor/\n"),
            ("Cargo.lock", b"v1"),
            ("vendor/lib/a.c", b"A"),
            ("vendor/b.c", b"B"),
        ],
    )
    .await
    .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let ws = Workspace::materialize(&objs, &dir, C, t, Sparse::all()).await.unwrap();
    assert!(ws.status(&objs).await.unwrap().is_empty());
    put(&dir, "Cargo.lock", "v2");
    put(&dir, "vendor/lib/a.c", "A2");
    fs::remove_file(dir.join("vendor/b.c")).unwrap();
    // Untracked neighbours inside the tracked-but-ignored directory stay hidden.
    put(&dir, "vendor/lib/new.o", "x");
    put(&dir, "vendor/newdir/x", "x");
    let (changes, ignored) = ws.status_with_ignored(&objs).await.unwrap();
    assert_eq!(
        kinds(&changes),
        [
            ("Cargo.lock".to_string(), Modified),
            ("vendor/b.c".to_string(), Deleted),
            ("vendor/lib/a.c".to_string(), Modified),
        ]
    );
    assert_eq!(ignored, ["vendor/lib/new.o", "vendor/newdir/"]);
}

#[tokio::test]
async fn gitignore_files_are_ordinary_files_and_edits_take_effect() {
    let objs = MemoryObjectStore::default();
    let t = build_tree(&objs, &[("a", b"A")]).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let ws = Workspace::materialize(&objs, &dir, C, t, Sparse::all()).await.unwrap();
    put(&dir, ".gitignore", "out/\n");
    put(&dir, "out/x", "x");
    let changes = ws.status(&objs).await.unwrap();
    assert_eq!(kinds(&changes), [(".gitignore".to_string(), LocalChangeKind::Added)]);
    // A pattern can ignore the rule file itself, like in git.
    put(&dir, ".gitignore", "out/\n.gitignore\n");
    assert!(ws.status(&objs).await.unwrap().is_empty());
    // A negation in a nested file re-includes within its subtree only.
    put(&dir, ".gitignore", "*.bin\n");
    put(&dir, "keep/.gitignore", "!*.bin\n");
    put(&dir, "keep/a.bin", "1");
    put(&dir, "drop/a.bin", "2");
    let got: Vec<String> =
        ws.status(&objs).await.unwrap().iter().map(|c| c.path.to_string()).collect();
    assert_eq!(got, [".gitignore", "keep/.gitignore", "keep/a.bin", "out/x"]);
}

/// An ignored directory is not entered: 50k files below it cost nothing.
#[tokio::test]
async fn ignored_directory_is_not_scanned() {
    use std::time::{Duration, Instant};
    let objs = MemoryObjectStore::default();
    let t = build_tree(&objs, &[(".gitignore", b"target/\n"), ("src/a.rs", b"A")]).await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("ws");
    let ws = Workspace::materialize(&objs, &dir, C, t, Sparse::all()).await.unwrap();
    let measure = || async {
        let mut best = Duration::MAX;
        for _ in 0..5 {
            let start = Instant::now();
            assert!(ws.status(&objs).await.unwrap().is_empty());
            best = best.min(start.elapsed());
        }
        best
    };
    let before = measure().await;
    for d in 0..50 {
        let sub = dir.join(format!("target/d{d}"));
        fs::create_dir_all(&sub).unwrap();
        for f in 0..1000 {
            fs::write(sub.join(format!("f{f}")), b"x").unwrap();
        }
    }
    let after = measure().await;
    eprintln!("status with 50k ignored files: {after:?} (without: {before:?})");
    // Generous bound against noise: scanning 50k files takes far longer.
    assert!(after < before + Duration::from_millis(50), "{after:?} vs {before:?}");
}
