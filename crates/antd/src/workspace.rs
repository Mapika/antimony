//! Sparse working copies.
//!
//! TASK (antd-workspace): implement everything marked `todo!()`.
//!
//! On-disk state lives in `{dir}/.ant/` (never reported as a change):
//! - `.ant/workspace`: `base_commit=<hex>`, `base_tree=<hex>`, and one
//!   `sparse=<pattern>` line per pattern. Plain text, one `key=value` per line.
//! - `.ant/index`: one record per materialized non-tree entry: path, mode,
//!   object id, size, mtime (ns), inode. Format is yours (keep it simple and
//!   fast to parse; tab-separated text is fine).
//!
//! Sparse patterns (`Sparse::matches`):
//! - Empty pattern list = everything.
//! - A pattern with no glob metacharacters (`*?[{`) is a path prefix:
//!   `src/foo` matches `src/foo` and everything under `src/foo/`.
//! - Otherwise it's a `globset` glob matched against the full path, with
//!   `literal_separator(true)` (so `*` doesn't cross `/`; use `**`).
//!
//! `materialize`:
//! - `dir` must not exist or be empty (else `AlreadyExists` io error).
//! - Walk the tree breadth-first, fetching each level's subtrees with ONE
//!   `get_many` call (not one per tree). Skip subtrees that cannot contain a
//!   match (`Sparse::may_contain_under(dir_path)`: true if any prefix pattern
//!   starts with or is under that dir, or any glob pattern exists — globs are
//!   not pruned).
//! - Fetch blobs in batches of 512 ids per `get_many`. Write files with the
//!   right mode (0o755 for Executable, 0o644 otherwise), symlinks via
//!   `std::os::unix::fs::symlink`, gitlinks as empty directories (not
//!   indexed).
//! - Record every written entry in the index with its post-write stat.
//!
//! `status` (no network; uses only the index + local hashing):
//! - Walk the workspace (skip `.ant`). For each file/symlink:
//!   indexed and (size, mtime_ns, ino, mode) unchanged → unchanged, without
//!   reading the file; otherwise read + hash as a blob and compare ids/mode.
//! - Indexed paths missing on disk → `Deleted`.
//! - Unindexed files anywhere (even outside the sparse patterns) → `Added`
//!   (it's the agent's new file — never ignore it).
//! - Empty directories are ignored. Output sorted by path.
//! - New/modified blob contents are stored into `objects` (so `submit` can
//!   upload them by id) — `LocalChange.new` carries their ids.
//!
//! `reset_to(objects, new_commit, new_tree)`: move the workspace to a new base
//! (e.g. after landing). Requires `status()` to be empty except for changes
//! that are already identical in `new_tree` (the just-landed changes).
//! Otherwise returns an error naming the dirty paths. Applies
//! `diff_trees(old_base_tree, new_tree)` to the in-scope files on disk
//! (writes, deletes, removes now-empty dirs), then rewrites the index and
//! config.

use std::path::{Path, PathBuf};

use ant_core::{EntryMode, ObjectId, RepoPath};
use ant_store::ObjectStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sparse {
    pub patterns: Vec<String>,
}

impl Sparse {
    pub fn all() -> Self {
        Self { patterns: vec![] }
    }

    pub fn new(patterns: Vec<String>) -> anyhow::Result<Self> {
        let _ = patterns;
        todo!("antd-workspace")
    }

    pub fn matches(&self, path: &RepoPath) -> bool {
        let _ = path;
        todo!("antd-workspace")
    }

    /// Could any path under directory `dir` match?
    pub fn may_contain_under(&self, dir: &RepoPath) -> bool {
        let _ = dir;
        todo!("antd-workspace")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalChangeKind {
    Added,
    Modified,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalChange {
    pub path: RepoPath,
    pub kind: LocalChangeKind,
    /// New entry for Added/Modified; blob is present in the local store.
    pub new: Option<(EntryMode, ObjectId)>,
}

#[derive(Debug)]
pub struct Workspace {
    #[allow(dead_code)]
    dir: PathBuf,
    pub base_commit: ObjectId,
    pub base_tree: ObjectId,
    pub sparse: Sparse,
}

impl Workspace {
    pub async fn materialize(
        objects: &dyn ObjectStore,
        dir: &Path,
        base_commit: ObjectId,
        base_tree: ObjectId,
        sparse: Sparse,
    ) -> anyhow::Result<Self> {
        let _ = (objects, dir, base_commit, base_tree, sparse);
        todo!("antd-workspace")
    }

    pub fn open(dir: &Path) -> anyhow::Result<Self> {
        let _ = dir;
        todo!("antd-workspace")
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub async fn status(&self, objects: &dyn ObjectStore) -> anyhow::Result<Vec<LocalChange>> {
        let _ = objects;
        todo!("antd-workspace")
    }

    pub async fn reset_to(
        &mut self,
        objects: &dyn ObjectStore,
        new_commit: ObjectId,
        new_tree: ObjectId,
    ) -> anyhow::Result<()> {
        let _ = (objects, new_commit, new_tree);
        todo!("antd-workspace")
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use ant_core::{EncodedObject, Object, ObjectKind, Tree, TreeEntry};
    use ant_store::graph::build_tree;
    use ant_store::memory::MemoryObjectStore;
    use ant_store::{ObjectStoreExt, Result as SResult};
    use async_trait::async_trait;

    fn p(s: &str) -> RepoPath {
        RepoPath::new(s).unwrap()
    }

    const C: ObjectId = ObjectId([0xc0; 32]);
    const C2: ObjectId = ObjectId([0xc2; 32]);

    #[derive(Default)]
    struct CountingStore {
        inner: MemoryObjectStore,
        calls: AtomicUsize,
    }
    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn get_many(&self, ids: &[ObjectId]) -> SResult<Vec<Option<EncodedObject>>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get_many(ids).await
        }
        async fn put_many(&self, o: &[EncodedObject]) -> SResult<()> {
            self.inner.put_many(o).await
        }
        async fn has_many(&self, ids: &[ObjectId]) -> SResult<Vec<bool>> {
            self.inner.has_many(ids).await
        }
    }

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

    #[tokio::test]
    async fn sparse_skips_unneeded_subtrees() {
        let objs = CountingStore::default();
        let mut files: Vec<(String, Vec<u8>)> = Vec::new();
        for i in 0..50 {
            files.push((format!("other{i}/x/y.rs"), b"o".to_vec()));
        }
        files.push(("want/a.rs".into(), b"w".to_vec()));
        let refs: Vec<(&str, &[u8])> = files.iter().map(|(a, b)| (a.as_str(), b.as_slice())).collect();
        let t = build_tree(&objs.inner, &refs).await.unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        Workspace::materialize(&objs, &dir, C, t, Sparse::new(vec!["want".into()]).unwrap()).await.unwrap();
        assert!(dir.join("want/a.rs").exists());
        assert!(!dir.join("other0").exists());
        // root tree level + "want" level + one blob batch; definitely not ~150.
        assert!(objs.calls.load(Ordering::SeqCst) <= 4, "calls = {}", objs.calls.load(Ordering::SeqCst));
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

    #[tokio::test]
    async fn status_detects_changes() {
        let objs = MemoryObjectStore::default();
        let t = build_tree(&objs, &[("keep", b"k"), ("mod", b"m"), ("del", b"d"), ("in/scope", b"s"), ("out/side", b"o")])
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
}
