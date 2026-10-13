//! Server-side commit creation from file-level operations.
//!
//! [`create_changeset`] semantics:
//! - Starts from `base`'s tree (or the empty tree when `base` is `None`).
//!   `base`, if given, must be a *recorded* commit (`MetaStore`), else
//!   `NotFound`.
//! - Applies every op. Ops may come in any order but at most one op per path
//!   (duplicates → `InvalidArgument`).
//! - `Write` creates intermediate directories as needed. Writing where an
//!   ancestor path component is a non-tree (file/symlink/gitlink), or where
//!   the path itself is currently a tree → `InvalidArgument` (in a single
//!   request, deleting `p` and writing `p/q` is allowed).
//! - `Write` mode must be `File`, `Executable` or `Symlink`, else
//!   `InvalidArgument`. `WriteContent::Blob(id)` must exist in the object
//!   store and be a blob, else `NotFound`/`InvalidArgument`.
//! - `Delete` of a path that is not a non-tree entry in the base →
//!   `NotFound`. Directories left empty by deletes are removed (git cannot
//!   represent empty trees except the root).
//! - Only trees along modified paths are rewritten; untouched subtrees keep
//!   their ids. Directories are loaded per depth (`tree_edit`), and every
//!   new blob, tree and the commit go out in one `put_many`.
//! - The `Commit` (parents = `[base]` or `[]`, committer defaults to author,
//!   message verbatim) is recorded with the generation and changeset
//!   `ant_store::graph::write_commit` would compute; the changeset comes
//!   from the ops instead of re-reading the trees just built.
//! - An empty ops list is allowed (produces a commit with the same tree).
//!
//! Cost: O(changed paths × depth) object reads/writes, independent of repo
//! size.

use std::collections::HashSet;
use std::sync::Arc;

use ant_core::{
    Commit, EncodedObject, EntryMode, Object, ObjectId, ObjectKind, RepoPath, Signature, Tree,
    TreeEntry,
};
use ant_store::{Backend, ChangeKind, CommitNode, PathChange};
use bytes::Bytes;

use super::{OpError, OpResult, cache};
use crate::acl::{self, Access};

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
    create_changeset_as(b, None, req).await
}

/// [`create_changeset`] on behalf of `access` (`None` = open mode, no
/// checks). Every op path must be writable (`PermissionDenied`), and blobs
/// referenced by id must be readable by, or uploaded by, the caller —
/// otherwise they are reported exactly like missing blobs (`NotFound`).
/// Under a policy, written blobs and rewritten trees are zone-tagged
/// (`acl::tag_written`) before they are stored.
pub async fn create_changeset_as(
    b: &Backend,
    access: Option<&Access>,
    req: ChangesetRequest,
) -> OpResult<ObjectId> {
    if let Some(access) = access {
        access.check_writable(req.ops.iter().map(FileOp::path))?;
    }
    let base = match req.base {
        Some(id) => {
            let node = cache::get_nodes(b, &[id]).await?.pop().flatten();
            Some(node.ok_or_else(|| OpError::NotFound(format!("commit {id}")))?)
        }
        None => None,
    };
    let base_tree = base.as_ref().map(|n| n.tree);

    // Validate ops and resolve their content into (mode, blob id).
    let mut seen = HashSet::new();
    let mut new_objects = Vec::new();
    let mut referenced = Vec::new();
    let mut blob_writes = Vec::new();
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
                blob_writes.push((path.clone(), id));
                Edit::Write(*mode, id)
            }
        };
        edits.push((path.components().collect(), edit));
    }
    if let Some(access) = access
        && !access.is_admin()
        && !referenced.is_empty()
    {
        let allowed = acl::can_reference(b, access, &referenced).await?;
        if let Some((id, _)) = referenced.iter().zip(allowed).find(|(_, ok)| !ok) {
            return Err(OpError::not_found_object(*id));
        }
    }
    check_blobs(b, &referenced).await?;

    let mut directories = match base_tree {
        Some(root) => {
            let paths: Vec<_> = req.ops.iter().map(|op| op.path().clone()).collect();
            let [dirs] = super::tree_edit::load_directories(b, [root], &paths).await?;
            dirs
        }
        None => Default::default(),
    };
    // The changeset is exactly the ops that change a leaf: `apply` rejects
    // every op that would replace or remove a directory, and pruning only
    // removes directories. Computed from the base before `apply` consumes it.
    let mut changes: Vec<PathChange> = req
        .ops
        .iter()
        .zip(&edits)
        .filter_map(|(op, (_, edit))| {
            let path = op.path();
            let old =
                super::tree_edit::entry(&directories, path).filter(|(mode, _)| !mode.is_tree());
            let new = match *edit {
                Edit::Write(mode, id) => Some((mode, id)),
                Edit::Delete => None,
            };
            let kind = match (old, new) {
                _ if old == new => return None,
                (None, _) => ChangeKind::Added,
                (_, None) => ChangeKind::Deleted,
                _ => ChangeKind::Modified,
            };
            Some(PathChange { path: path.clone(), kind, old, new })
        })
        .collect();
    changes.sort_by(|a, b| a.path.cmp(&b.path));
    edits.sort_by(|a, b| a.0.cmp(&b.0));

    let mut trees = Vec::new();
    let root = apply(&mut directories, base_tree, &edits, 0, &mut new_objects, &mut trees)?;
    let tree = match root {
        Some(id) => id,
        None => {
            let empty = Object::Tree(Tree::empty()).encode();
            let id = empty.id();
            new_objects.push(empty);
            trees.push((RepoPath::root(), id, Arc::new(Tree::empty())));
            id
        }
    };
    if let Some(access) = access {
        let mut writes: Vec<_> = trees.iter().map(|(path, id, _)| (path.clone(), *id)).collect();
        writes.extend(blob_writes);
        acl::tag_written(b, &access.policy, &writes).await?;
    }

    let commit = Commit {
        tree,
        parents: req.base.into_iter().collect(),
        committer: req.committer.unwrap_or_else(|| req.author.clone()),
        author: req.author,
        extra_headers: vec![],
        message: req.message.into_bytes(),
    };
    let encoded = EncodedObject::new(ObjectKind::Commit, &commit.encode());
    let id = encoded.id();
    new_objects.push(encoded);
    let node = CommitNode {
        id,
        tree,
        parents: commit.parents.clone(),
        generation: base.map_or(0, |n| n.generation) + 1,
        committer_time: commit.committer.time,
    };
    let changes_weight = cache::changeset_weight(&changes);
    let recorded = Arc::new(changes.clone());
    // One write for every new object (blobs, trees, the commit) and, at the
    // same time, the commit record with the changeset computed above: no
    // tree is read back. The id is returned only once both are durable; a
    // record whose objects failed to write is unreachable, and a retry of
    // the same request completes it.
    futures::try_join!(async { b.objects.put_many(&new_objects).await }, async {
        b.meta.put_commit(node.clone(), changes).await
    },)?;

    let objects = cache::objects(&b.objects);
    objects.trees.insert_many(trees.into_iter().map(|(_, id, tree)| {
        let weight = cache::tree_weight(&tree);
        (id, tree, weight)
    }));
    let commit_weight = cache::commit_weight(&commit);
    objects.commits.insert(id, Arc::new(commit), commit_weight);
    let meta = cache::meta(&b.meta);
    meta.nodes.insert(id, node, cache::NODE_WEIGHT);
    meta.changesets.insert(id, recorded, changes_weight);
    Ok(id)
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
    directories: &mut super::tree_edit::Directories,
    base: Option<ObjectId>,
    edits: &[(Vec<&str>, Edit)],
    depth: usize,
    out: &mut Vec<EncodedObject>,
    trees: &mut Vec<(RepoPath, ObjectId, Arc<Tree>)>,
) -> OpResult<Option<ObjectId>> {
    let dir = RepoPath::new(
        edits.first().map(|(parts, _)| parts[..depth].join("/")).unwrap_or_default(),
    )?;
    let mut tree = match base {
        Some(_) => directories
            .remove(&dir)
            .ok_or_else(|| OpError::InvalidArgument(format!("missing directory {dir}")))?,
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
        match apply(directories, sub_base, nested, depth + 1, out, trees)? {
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
    let obj = EncodedObject::new(ObjectKind::Tree, &tree.encode());
    let id = obj.id();
    out.push(obj);
    trees.push((dir, id, Arc::new(tree)));
    Ok(Some(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::OpError;
    use crate::ops::test_support::{commit, delete, read, sig, write};
    use ant_core::{EncodedObject, ObjectKind};
    use ant_store::graph::{diff_trees, lookup_path};
    use ant_store::{ChangeKind, ObjectStoreExt};

    #[tokio::test]
    async fn root_commit_and_nested_writes() {
        let b = Backend::in_memory();
        let c = commit(&b, None, vec![write("a/b/c.txt", "hi"), write("top", "t")]).await.unwrap();
        assert_eq!(read(&b, c, "a/b/c.txt").await.as_deref(), Some("hi"));
        assert_eq!(read(&b, c, "top").await.as_deref(), Some("t"));
        let commit_obj = b.objects.read_commit(&c).await.unwrap();
        assert!(commit_obj.parents.is_empty());
        assert_eq!(commit_obj.committer, sig());
        assert_eq!(commit_obj.message, b"msg\n");
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
        assert!(
            lookup_path(b.objects.as_ref(), tree, &RepoPath::new("e").unwrap())
                .await
                .unwrap()
                .is_none()
        );

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
        let ops: Vec<_> = (0..2000)
            .map(|i| write(&format!("w/{:04}/f.rs", i % 100 * 100 + i / 100), "x"))
            .collect();
        let c = commit(&b, None, ops).await.unwrap();
        assert_eq!(b.meta.get_changeset(&c).await.unwrap().unwrap().len(), 2000);
    }
}
