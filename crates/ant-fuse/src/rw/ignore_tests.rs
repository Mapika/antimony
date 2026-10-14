//! Ignore rules in the upper-layer scan, driven through a state directory
//! (no kernel mount needed).

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ant_core::ObjectId;
use ant_store::graph::build_tree;
use ant_store::memory::MemoryObjectStore;
use antd::workspace::LocalChangeKind::{self, Added, Deleted, Modified};

use super::state::*;
use super::*;
use crate::test_support::runtime;

fn workspace(
    rt: &tokio::runtime::Runtime,
    files: &[(&str, &[u8])],
) -> (Arc<MemoryObjectStore>, tempfile::TempDir) {
    let objects = Arc::new(MemoryObjectStore::default());
    let tree = rt.block_on(build_tree(objects.as_ref(), files)).unwrap();
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir(temp.path().join("upper")).unwrap();
    save_base(temp.path(), ObjectId([1; 32]), tree).unwrap();
    save_whiteouts(temp.path(), &BTreeSet::new()).unwrap();
    (objects, temp)
}

fn put(state: &Path, path: &str, body: &str) {
    let p = state.join("upper").join(path);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, body).unwrap();
}

fn status(
    rt: &tokio::runtime::Runtime,
    objects: &Arc<MemoryObjectStore>,
    state: &Path,
) -> (Vec<(String, LocalChangeKind)>, Vec<String>) {
    let (changes, ignored) = rt.block_on(changes_with_ignored(objects.clone(), state)).unwrap();
    (changes.into_iter().map(|c| (c.path.to_string(), c.kind)).collect(), ignored)
}

fn k(path: &str, kind: LocalChangeKind) -> (String, LocalChangeKind) {
    (path.to_string(), kind)
}

#[test]
fn base_and_upper_gitignore_files_hide_untracked_paths() {
    let rt = runtime();
    let (objects, temp) = workspace(
        &rt,
        &[
            (".gitignore", b"target/\n*.log\n!keep.log\n"),
            ("src/lib.rs", b"lib"),
            // Only in the base layer, never copied up.
            ("src/.gitignore", b"gen_*\n"),
            ("build.log", b"tracked"),
            ("vendor/a.c", b"A"),
            ("vendor/.gitignore", b"*.o\n"),
        ],
    );
    let state = temp.path();
    put(state, "target/debug/app", "bin");
    put(state, "a.log", "l");
    put(state, "keep.log", "k");
    put(state, "src/gen_a.rs", "g");
    put(state, "gen_b.rs", "not covered");
    put(state, "build.log", "edited");
    put(state, "vendor/x.o", "o");
    put(state, "vendor/new.c", "c");
    // The local ignore file lives in meta/.
    fs::create_dir_all(state.join("meta")).unwrap();
    fs::write(state.join("meta/ignore"), "scratch/\n").unwrap();
    put(state, "scratch/n", "n");

    let (changes, ignored) = status(&rt, &objects, state);
    assert_eq!(
        changes,
        [
            k("build.log", Modified),
            k("gen_b.rs", Added),
            k("keep.log", Added),
            k("vendor/new.c", Added)
        ]
    );
    assert_eq!(ignored, ["a.log", "scratch/", "src/gen_a.rs", "target/", "vendor/x.o"]);

    // An upper `.gitignore` shadows the base one, and is itself reported.
    put(state, "src/.gitignore", "other_*\n");
    let (changes, ignored) = status(&rt, &objects, state);
    assert!(changes.contains(&k("src/.gitignore", Modified)), "{changes:?}");
    assert!(changes.contains(&k("src/gen_a.rs", Added)), "{changes:?}");
    assert!(!ignored.contains(&"src/gen_a.rs".to_string()));
}

#[test]
fn tracked_files_below_ignored_paths_are_still_compared() {
    let rt = runtime();
    let (objects, temp) = workspace(
        &rt,
        &[
            (".gitignore", b"vendor/\n*.lock\n"),
            ("vendor/lib/a.c", b"A"),
            ("vendor/b.c", b"B"),
            ("Cargo.lock", b"v1"),
        ],
    );
    let state = temp.path();
    // Copy-ups of tracked files: one edited, one identical.
    put(state, "vendor/lib/a.c", "A2");
    put(state, "vendor/b.c", "B");
    put(state, "Cargo.lock", "v2");
    // Untracked neighbour in the ignored directory.
    put(state, "vendor/lib/new.o", "x");
    let (changes, ignored) = status(&rt, &objects, state);
    assert_eq!(changes, [k("Cargo.lock", Modified), k("vendor/lib/a.c", Modified)]);
    // The directory holds tracked files: it is not listed, nor its contents.
    assert!(ignored.is_empty(), "{ignored:?}");

    // A tracked file deleted under an ignored directory is a deletion.
    let mut gone = BTreeSet::new();
    gone.insert("vendor/b.c".to_string());
    fs::remove_file(state.join("upper/vendor/b.c")).unwrap();
    save_whiteouts(state, &gone).unwrap();
    let (changes, _) = status(&rt, &objects, state);
    assert_eq!(
        changes,
        [k("Cargo.lock", Modified), k("vendor/b.c", Deleted), k("vendor/lib/a.c", Modified)]
    );
}

/// 50k files in an ignored upper directory are never read or hashed.
#[test]
fn ignored_directory_is_not_scanned() {
    let rt = runtime();
    let (objects, temp) = workspace(&rt, &[(".gitignore", b"target/\n"), ("src/a.rs", b"A")]);
    let state = temp.path();
    put(state, "src/a.rs", "A edited");
    let measure = || {
        let mut best = Duration::MAX;
        for _ in 0..5 {
            let start = Instant::now();
            let (changes, _) = status(&rt, &objects, state);
            assert_eq!(changes, [k("src/a.rs", Modified)]);
            best = best.min(start.elapsed());
        }
        best
    };
    let before = measure();
    for d in 0..50 {
        let sub = state.join(format!("upper/target/d{d}"));
        fs::create_dir_all(&sub).unwrap();
        for f in 0..1000 {
            fs::write(sub.join(format!("f{f}")), b"x").unwrap();
        }
    }
    let after = measure();
    eprintln!("lazy status with 50k ignored files: {after:?} (without: {before:?})");
    assert!(after < before + Duration::from_millis(50), "{after:?} vs {before:?}");
}
