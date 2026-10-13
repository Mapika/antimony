//! Server-side commit creation from file-level operations.
//!
//! TASK (server-changeset): implement `create_changeset`.
//!
//! Semantics:
//! - Start from `base`'s tree (or the empty tree when `base` is `None`).
//!   `base`, if given, must be a *recorded* commit (`MetaStore`), else
//!   `NotFound`.
//! - Apply every op. Ops may come in any order but at most one op per path
//!   (duplicates → `InvalidArgument`).
//! - `Write` creates intermediate directories as needed. Writing where an
//!   ancestor path component is a non-tree (file/symlink/gitlink), or where
//!   the path itself is currently a tree → `InvalidArgument` (in a single
//!   request, deleting `p` and writing `p/q` IS allowed and must work).
//! - `Write` mode must be `File`, `Executable` or `Symlink`, else
//!   `InvalidArgument`. `WriteContent::Blob(id)` must exist in the object
//!   store and be a blob, else `NotFound`/`InvalidArgument`.
//! - `Delete` of a path that is not a non-tree entry in the base →
//!   `NotFound`. Directories left empty by deletes are removed (git cannot
//!   represent empty trees except the root).
//! - Only rewrite trees along modified paths; untouched subtrees keep their
//!   ids. Store all new blobs/trees with as few `put_many` calls as
//!   practical (one per batch, not one per object).
//! - Build the `Commit` (parents = `[base]` or `[]`, committer defaults to
//!   author, message verbatim), then persist via
//!   `ant_store::graph::write_commit`, which also records generation and
//!   changeset. Return the commit id.
//! - An empty ops list is allowed (produces a commit with the same tree).
//!
//! Performance target: O(changed paths × depth) object reads/writes,
//! independent of repo size.

use std::collections::HashSet;

use ant_core::{
    Commit, EncodedObject, EntryMode, Object, ObjectId, ObjectKind, RepoPath, Signature, Tree,
    TreeEntry,
};
use ant_store::{Backend, graph};
use bytes::Bytes;

use super::{OpError, OpResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteContent {
    Inline(Bytes),
    Blob(ObjectId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileOp {
    Write { path: RepoPath, mode: EntryMode, content: WriteContent },
    Delete { path: RepoPath },
}

impl FileOp {
    pub fn path(&self) -> &RepoPath {
        match self {
            FileOp::Write { path, .. } | FileOp::Delete { path } => path,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChangesetRequest {
    pub base: Option<ObjectId>,
    pub ops: Vec<FileOp>,
    pub author: Signature,
    pub committer: Option<Signature>,
    pub message: String,
}

pub async fn create_changeset(b: &Backend, req: ChangesetRequest) -> OpResult<ObjectId> {
    let base_tree = match req.base {
        Some(id) => {
            let node = b.meta.get_commits(&[id]).await?.pop().flatten();
            Some(node.ok_or_else(|| OpError::NotFound(format!("commit {id}")))?.tree)
        }
        None => None,
    };

    // Validate ops and resolve their content into (mode, blob id).
    let mut seen = HashSet::new();
    let mut new_objects = Vec::new();
    let mut referenced = Vec::new();
    let mut edits: Vec<(Vec<&str>, Edit)> = Vec::with_capacity(req.ops.len());
    for op in &req.ops {
        let path = op.path();
        if path.is_root() {
            return Err(OpError::InvalidArgument("op on repository root".into()));
        }
        if !seen.insert(path.clone()) {
            return Err(OpError::InvalidArgument(format!("duplicate op for {path}")));
        }
        let edit = match op {
            FileOp::Delete { .. } => Edit::Delete,
            FileOp::Write { mode, content, .. } => {
                if !mode.is_blob() {
                    return Err(OpError::InvalidArgument(format!(
                        "{path}: cannot write mode {mode:?}"
                    )));
                }
                let id = match content {
                    WriteContent::Inline(data) => {
                        let obj = EncodedObject::new(ObjectKind::Blob, data);
                        let id = obj.id();
                        new_objects.push(obj);
                        id
                    }
                    WriteContent::Blob(id) => {
                        referenced.push(*id);
                        *id
                    }
                };
                Edit::Write(*mode, id)
            }
        };
        edits.push((path.components().collect(), edit));
    }
    check_blobs(b, &referenced).await?;

    edits.sort_by(|a, b| a.0.cmp(&b.0));
    let mut directories = match base_tree {
        Some(root) => {
            let paths: Vec<_> = req.ops.iter().map(|op| op.path().clone()).collect();
            let [dirs] = super::land::load_directories(b, [root], &paths).await?;
            dirs
        }
        None => Default::default(),
    };
    let root = apply(&mut directories, base_tree, &edits, 0, &mut new_objects)?;
    let tree = match root {
        Some(id) => id,
        None => {
            let empty = Object::Tree(Tree::empty()).encode();
            let id = empty.id();
            new_objects.push(empty);
            id
        }
    };
    b.objects.put_many(&new_objects).await?;

    let commit = Commit {
        tree,
        parents: req.base.into_iter().collect(),
        committer: req.committer.unwrap_or_else(|| req.author.clone()),
        author: req.author,
        extra_headers: vec![],
        message: req.message,
    };
    Ok(graph::write_commit(b, &commit).await?)
}

#[derive(Debug, Clone, Copy)]
enum Edit {
    Write(EntryMode, ObjectId),
    Delete,
}

/// Referenced blobs must exist and be blobs.
async fn check_blobs(b: &Backend, ids: &[ObjectId]) -> OpResult<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let found = b.objects.get_many(ids).await?;
    for (id, o) in ids.iter().zip(found) {
        match o {
            None => return Err(OpError::not_found_object(*id)),
            Some(o) if o.kind() != ObjectKind::Blob => {
                return Err(OpError::InvalidArgument(format!(
                    "{id} is a {:?}, not a blob",
                    o.kind()
                )));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// Apply `edits` (sorted by components, all sharing the first `depth`
/// components) to the tree `base`. Returns the new tree id, or `None` if the
/// directory ends up empty. New trees are appended to `out`.
fn apply(
    directories: &mut super::land::Directories,
    base: Option<ObjectId>,
    edits: &[(Vec<&str>, Edit)],
    depth: usize,
    out: &mut Vec<EncodedObject>,
) -> OpResult<Option<ObjectId>> {
    let mut tree = match base {
        Some(_) => {
            let path = RepoPath::new(
                edits.first().map(|(parts, _)| parts[..depth].join("/")).unwrap_or_default(),
            )?;
            directories
                .remove(&path)
                .ok_or_else(|| OpError::InvalidArgument(format!("missing directory {path}")))?
        }
        None => Tree::empty(),
    };
    let mut i = 0;
    while i < edits.len() {
        let name = edits[i].0[depth];
        let end = i + edits[i..].iter().take_while(|(c, _)| c[depth] == name).count();
        let group = &edits[i..end];
        i = end;

        // Sorting puts the edit for `name` itself before edits beneath it.
        let (own, nested) = match group.first() {
            Some((c, e)) if c.len() == depth + 1 => (Some(*e), &group[1..]),
            _ => (None, group),
        };
        let existing = tree.get(name).cloned();
        let display = || group[0].0[..=depth].join("/");

        if nested.is_empty() {
            match (own, existing) {
                (Some(Edit::Write(..)), Some(e)) if e.mode.is_tree() => {
                    return Err(OpError::InvalidArgument(format!("{} is a directory", display())));
                }
                (Some(Edit::Write(mode, id)), _) => {
                    tree.upsert(TreeEntry { mode, name: name.to_string(), id })?;
                }
                (Some(Edit::Delete), Some(e)) if !e.mode.is_tree() => {
                    tree.remove(name);
                }
                (Some(Edit::Delete), _) => {
                    return Err(OpError::NotFound(format!("file {}", display())));
                }
                (None, _) => unreachable!("group has at least one edit"),
            }
            continue;
        }

        let sub_base = match (own, existing) {
            (Some(Edit::Write(..)), _) => {
                return Err(OpError::InvalidArgument(format!(
                    "{} is written as a file and used as a directory",
                    display()
                )));
            }
            (Some(Edit::Delete), Some(e)) if !e.mode.is_tree() => None,
            (Some(Edit::Delete), _) => {
                return Err(OpError::NotFound(format!("file {}", display())));
            }
            (None, Some(e)) if e.mode.is_tree() => Some(e.id),
            // Below a file: writing there is a path conflict, while
            // deleting there targets a file that does not exist.
            (None, Some(_)) if nested.iter().all(|(_, e)| matches!(e, Edit::Delete)) => {
                return Err(OpError::NotFound(format!("{} is not a directory", display())));
            }
            (None, Some(_)) => {
                return Err(OpError::InvalidArgument(format!("{} is not a directory", display())));
            }
            (None, None) => None,
        };
        match apply(directories, sub_base, nested, depth + 1, out)? {
            Some(id) => {
                tree.upsert(TreeEntry { mode: EntryMode::Tree, name: name.to_string(), id })?
            }
            None => {
                tree.remove(name);
            }
        }
    }
    if tree.is_empty() {
        return Ok(None);
    }
    let obj = Object::Tree(tree).encode();
    let id = obj.id();
    out.push(obj);
    Ok(Some(id))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::ops::OpError;
    use ant_core::{EncodedObject, ObjectKind};
    use ant_store::graph::{diff_trees, lookup_path};
    use ant_store::{ChangeKind, ObjectStoreExt};

    pub fn sig() -> Signature {
        Signature { name: "agent".into(), email: "a@x".into(), time: 1_700_000_000, tz_offset_minutes: 0 }
    }

    pub fn write(path: &str, content: &str) -> FileOp {
        FileOp::Write {
            path: RepoPath::new(path).unwrap(),
            mode: EntryMode::File,
            content: WriteContent::Inline(Bytes::copy_from_slice(content.as_bytes())),
        }
    }

    pub fn delete(path: &str) -> FileOp {
        FileOp::Delete { path: RepoPath::new(path).unwrap() }
    }

    pub async fn commit(b: &Backend, base: Option<ObjectId>, ops: Vec<FileOp>) -> OpResult<ObjectId> {
        create_changeset(
            b,
            ChangesetRequest { base, ops, author: sig(), committer: None, message: "msg\n".into() },
        )
        .await
    }

    pub async fn read(b: &Backend, commit_id: ObjectId, path: &str) -> Option<String> {
        let c = b.objects.read_commit(&commit_id).await.unwrap();
        let (mode, id) =
            lookup_path(b.objects.as_ref(), c.tree, &RepoPath::new(path).unwrap()).await.unwrap()?;
        assert!(mode.is_blob(), "{path} is not a blob");
        Some(String::from_utf8(b.objects.read_blob(&id).await.unwrap().to_vec()).unwrap())
    }

    #[tokio::test]
    async fn root_commit_and_nested_writes() {
        let b = Backend::in_memory();
        let c = commit(&b, None, vec![write("a/b/c.txt", "hi"), write("top", "t")]).await.unwrap();
        assert_eq!(read(&b, c, "a/b/c.txt").await.as_deref(), Some("hi"));
        assert_eq!(read(&b, c, "top").await.as_deref(), Some("t"));
        let commit_obj = b.objects.read_commit(&c).await.unwrap();
        assert!(commit_obj.parents.is_empty());
        assert_eq!(commit_obj.committer, sig());
        assert_eq!(commit_obj.message, "msg\n");
        // Recorded in meta with generation 1 and a full changeset.
        let node = b.meta.get_commits(&[c]).await.unwrap().pop().unwrap().unwrap();
        assert_eq!(node.generation, 1);
        assert_eq!(b.meta.get_changeset(&c).await.unwrap().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn modify_delete_and_prune_empty_dirs() {
        let b = Backend::in_memory();
        let c1 = commit(&b, None, vec![write("d/x", "1"), write("d/y", "2"), write("e/z", "3")])
            .await
            .unwrap();
        let c2 = commit(&b, Some(c1), vec![write("d/x", "11"), delete("e/z")]).await.unwrap();
        assert_eq!(read(&b, c2, "d/x").await.as_deref(), Some("11"));
        assert_eq!(read(&b, c2, "d/y").await.as_deref(), Some("2"));
        let tree = b.objects.read_commit(&c2).await.unwrap().tree;
        // "e" became empty and must be gone entirely.
        assert!(lookup_path(b.objects.as_ref(), tree, &RepoPath::new("e").unwrap()).await.unwrap().is_none());

        let cs = b.meta.get_changeset(&c2).await.unwrap().unwrap();
        let kinds: Vec<_> = cs.iter().map(|c| (c.path.as_str(), c.kind)).collect();
        assert_eq!(kinds, [("d/x", ChangeKind::Modified), ("e/z", ChangeKind::Deleted)]);
        let node = b.meta.get_commits(&[c2]).await.unwrap().pop().unwrap().unwrap();
        assert_eq!(node.generation, 2);
        assert_eq!(node.parents, vec![c1]);
    }

    #[tokio::test]
    async fn untouched_subtrees_keep_ids() {
        let b = Backend::in_memory();
        let c1 = commit(&b, None, vec![write("big/a", "a"), write("small/b", "b")]).await.unwrap();
        let c2 = commit(&b, Some(c1), vec![write("small/b", "bb")]).await.unwrap();
        let t1 = b.objects.read_commit(&c1).await.unwrap().tree;
        let t2 = b.objects.read_commit(&c2).await.unwrap().tree;
        let big = RepoPath::new("big").unwrap();
        assert_eq!(
            lookup_path(b.objects.as_ref(), t1, &big).await.unwrap(),
            lookup_path(b.objects.as_ref(), t2, &big).await.unwrap()
        );
        assert_eq!(diff_trees(b.objects.as_ref(), Some(t1), Some(t2)).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn blob_reference_and_modes() {
        let b = Backend::in_memory();
        let blob = EncodedObject::new(ObjectKind::Blob, b"uploaded");
        let blob_id = b.objects.put(blob).await.unwrap();
        let c = commit(
            &b,
            None,
            vec![
                FileOp::Write {
                    path: RepoPath::new("u").unwrap(),
                    mode: EntryMode::Executable,
                    content: WriteContent::Blob(blob_id),
                },
                FileOp::Write {
                    path: RepoPath::new("l").unwrap(),
                    mode: EntryMode::Symlink,
                    content: WriteContent::Inline(Bytes::from_static(b"u")),
                },
            ],
        )
        .await
        .unwrap();
        let tree = b.objects.read_commit(&c).await.unwrap().tree;
        let t = b.objects.read_tree(&tree).await.unwrap();
        assert_eq!(t.get("u").unwrap().mode, EntryMode::Executable);
        assert_eq!(t.get("u").unwrap().id, blob_id);
        assert_eq!(t.get("l").unwrap().mode, EntryMode::Symlink);
    }

    #[tokio::test]
    async fn replace_file_with_directory_in_one_request() {
        let b = Backend::in_memory();
        let c1 = commit(&b, None, vec![write("p", "file")]).await.unwrap();
        let c2 = commit(&b, Some(c1), vec![delete("p"), write("p/q", "nested")]).await.unwrap();
        assert_eq!(read(&b, c2, "p/q").await.as_deref(), Some("nested"));
    }

    #[tokio::test]
    async fn empty_ops_same_tree() {
        let b = Backend::in_memory();
        let c1 = commit(&b, None, vec![write("f", "x")]).await.unwrap();
        let c2 = commit(&b, Some(c1), vec![]).await.unwrap();
        assert_ne!(c1, c2);
        assert_eq!(
            b.objects.read_commit(&c1).await.unwrap().tree,
            b.objects.read_commit(&c2).await.unwrap().tree
        );
    }

    #[tokio::test]
    async fn errors() {
        let b = Backend::in_memory();
        let c1 = commit(&b, None, vec![write("f", "x"), write("d/g", "y")]).await.unwrap();

        let e = commit(&b, Some(c1), vec![write("f/sub", "z")]).await.unwrap_err();
        assert!(matches!(e, OpError::InvalidArgument(_)), "file as dir: {e}");
        let e = commit(&b, Some(c1), vec![write("d", "z")]).await.unwrap_err();
        assert!(matches!(e, OpError::InvalidArgument(_)), "overwrite dir: {e}");
        let e = commit(&b, Some(c1), vec![write("a", "1"), write("a", "2")]).await.unwrap_err();
        assert!(matches!(e, OpError::InvalidArgument(_)), "dup path: {e}");
        let e = commit(&b, Some(c1), vec![delete("nope")]).await.unwrap_err();
        assert!(matches!(e, OpError::NotFound(_)), "delete missing: {e}");
        let e = commit(&b, Some(c1), vec![delete("d")]).await.unwrap_err();
        assert!(matches!(e, OpError::NotFound(_)), "delete dir: {e}");
        let e = commit(&b, Some(ObjectId([9; 32])), vec![]).await.unwrap_err();
        assert!(matches!(e, OpError::NotFound(_)), "bad base: {e}");
        let e = commit(
            &b,
            Some(c1),
            vec![FileOp::Write {
                path: RepoPath::new("m").unwrap(),
                mode: EntryMode::Tree,
                content: WriteContent::Inline(Bytes::new()),
            }],
        )
        .await
        .unwrap_err();
        assert!(matches!(e, OpError::InvalidArgument(_)), "tree mode: {e}");
        let e = commit(
            &b,
            Some(c1),
            vec![FileOp::Write {
                path: RepoPath::new("m").unwrap(),
                mode: EntryMode::File,
                content: WriteContent::Blob(ObjectId([8; 32])),
            }],
        )
        .await
        .unwrap_err();
        assert!(matches!(e, OpError::NotFound(_)), "missing blob: {e}");
        let e = commit(&b, Some(c1), vec![write("", "root")]).await.unwrap_err();
        assert!(matches!(e, OpError::InvalidArgument(_)), "root path: {e}");
    }

    #[tokio::test]
    async fn many_files_wide_directory() {
        let b = Backend::in_memory();
        let ops: Vec<_> = (0..2000).map(|i| write(&format!("w/{:04}/f.rs", i % 100 * 100 + i / 100), "x")).collect();
        let c = commit(&b, None, ops).await.unwrap();
        assert_eq!(b.meta.get_changeset(&c).await.unwrap().unwrap().len(), 2000);
    }
}
