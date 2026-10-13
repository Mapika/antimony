//! Commit-graph primitives shared by the server, git bridge, and tests:
//! tree diffing, commit recording, and generation-number ancestry queries.

use std::collections::{BinaryHeap, HashSet};

use ant_core::{
    Commit, EncodedObject, EntryMode, Object, ObjectId, RepoPath, Tree, TreeEntry,
};

use crate::{
    Backend, ChangeKind, CommitNode, ObjectStore, ObjectStoreExt, PathChange, Result, StoreError,
};

/// Non-tree paths that differ between two trees (`None` = empty tree),
/// sorted by path. Identical subtrees are skipped by id without being read.
pub async fn diff_trees(
    objects: &dyn ObjectStore,
    old: Option<ObjectId>,
    new: Option<ObjectId>,
) -> Result<Vec<PathChange>> {
    let mut out = Vec::new();
    // Explicit stack instead of async recursion.
    let mut stack = vec![(RepoPath::root(), old, new)];
    while let Some((dir, old, new)) = stack.pop() {
        if old == new {
            continue;
        }
        let old_t = match old {
            Some(id) => objects.read_tree(&id).await?,
            None => Tree::empty(),
        };
        let new_t = match new {
            Some(id) => objects.read_tree(&id).await?,
            None => Tree::empty(),
        };
        let mut names: Vec<&str> = old_t
            .entries()
            .iter()
            .chain(new_t.entries())
            .map(|e| e.name.as_str())
            .collect();
        names.sort_unstable();
        names.dedup();
        for name in names {
            let o = old_t.get(name);
            let n = new_t.get(name);
            if o == n {
                continue;
            }
            let path = dir.join(name)?;
            let o_tree = o.filter(|e| e.mode.is_tree()).map(|e| e.id);
            let n_tree = n.filter(|e| e.mode.is_tree()).map(|e| e.id);
            let o_leaf = o.filter(|e| !e.mode.is_tree()).map(|e| (e.mode, e.id));
            let n_leaf = n.filter(|e| !e.mode.is_tree()).map(|e| (e.mode, e.id));
            if o_tree.is_some() || n_tree.is_some() {
                stack.push((path.clone(), o_tree, n_tree));
            }
            if o_leaf != n_leaf {
                let kind = match (o_leaf, n_leaf) {
                    (None, Some(_)) => ChangeKind::Added,
                    (Some(_), None) => ChangeKind::Deleted,
                    _ => ChangeKind::Modified,
                };
                out.push(PathChange { path, kind, old: o_leaf, new: n_leaf });
            }
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// Look up the entry at `path` within `tree` (root path returns a synthetic
/// tree entry for `tree` itself).
pub async fn lookup_path(
    objects: &dyn ObjectStore,
    tree: ObjectId,
    path: &RepoPath,
) -> Result<Option<(EntryMode, ObjectId)>> {
    let mut cur = (EntryMode::Tree, tree);
    for comp in path.components() {
        if !cur.0.is_tree() {
            return Ok(None);
        }
        match objects.read_tree(&cur.1).await?.get(comp) {
            Some(e) => cur = (e.mode, e.id),
            None => return Ok(None),
        }
    }
    Ok(Some(cur))
}

/// Record an already-stored commit in the meta store: computes generation
/// and the changeset vs. first parent. Parents must already be recorded.
/// Idempotent; returns the log sequence number.
pub async fn record_commit(b: &Backend, id: ObjectId) -> Result<u64> {
    if let Some(Some(_)) = b.meta.get_commits(&[id]).await?.pop() {
        // Already recorded: put_commit is idempotent and returns the seq.
        let node = b.meta.get_commits(&[id]).await?.pop().flatten().unwrap();
        return b.meta.put_commit(node, vec![]).await;
    }
    let commit = b.objects.read_commit(&id).await?;
    let parents = b.meta.get_commits(&commit.parents).await?;
    let mut generation = 0;
    for (pid, p) in commit.parents.iter().zip(&parents) {
        let p = p.as_ref().ok_or_else(|| {
            StoreError::InvalidArgument(format!("parent {pid} of {id} is not recorded"))
        })?;
        generation = generation.max(p.generation);
    }
    let first_parent_tree = parents.first().and_then(|p| p.as_ref()).map(|p| p.tree);
    let changes = diff_trees(b.objects.as_ref(), first_parent_tree, Some(commit.tree)).await?;
    let node = CommitNode {
        id,
        tree: commit.tree,
        parents: commit.parents.clone(),
        generation: generation + 1,
        committer_time: commit.committer.time,
    };
    b.meta.put_commit(node, changes).await
}

/// Store a commit object and record it. Returns the commit id.
pub async fn write_commit(b: &Backend, commit: &Commit) -> Result<ObjectId> {
    let id = b.objects.put(Object::Commit(commit.clone()).encode()).await?;
    record_commit(b, id).await?;
    Ok(id)
}

/// Encode and store a tree, returning its id.
pub async fn write_tree(objects: &dyn ObjectStore, entries: Vec<TreeEntry>) -> Result<ObjectId> {
    let obj = Object::Tree(Tree::new(entries)?).encode();
    let id = obj.id();
    objects.put_many(&[obj]).await?;
    Ok(id)
}

async fn node(b: &Backend, id: ObjectId) -> Result<CommitNode> {
    b.meta
        .get_commits(&[id])
        .await?
        .pop()
        .flatten()
        .ok_or_else(|| StoreError::InvalidArgument(format!("commit {id} is not recorded")))
}

/// True if `ancestor` is reachable from `descendant` (a commit is its own
/// ancestor). Prunes using generation numbers.
pub async fn is_ancestor(b: &Backend, ancestor: ObjectId, descendant: ObjectId) -> Result<bool> {
    if ancestor == descendant {
        return Ok(true);
    }
    let target_gen = node(b, ancestor).await?.generation;
    let mut seen = HashSet::new();
    let mut stack = vec![descendant];
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let n = node(b, id).await?;
        if n.generation <= target_gen {
            continue;
        }
        for p in n.parents {
            if p == ancestor {
                return Ok(true);
            }
            stack.push(p);
        }
    }
    Ok(false)
}

/// A best common ancestor of `a` and `b` (highest generation), if any.
pub async fn merge_base(b: &Backend, x: ObjectId, y: ObjectId) -> Result<Option<ObjectId>> {
    // Walk both sides in generation order, painting reachability.
    const X: u8 = 1;
    const Y: u8 = 2;
    let mut paint: std::collections::HashMap<ObjectId, u8> = Default::default();
    let mut heap = BinaryHeap::new();
    for (id, flag) in [(x, X), (y, Y)] {
        let n = node(b, id).await?;
        *paint.entry(id).or_default() |= flag;
        heap.push((n.generation, id));
    }
    let mut queued: HashSet<ObjectId> = [x, y].into();
    while let Some((_, id)) = heap.pop() {
        let flags = paint[&id];
        if flags == X | Y {
            return Ok(Some(id));
        }
        for p in node(b, id).await?.parents {
            *paint.entry(p).or_default() |= flags;
            if queued.insert(p) {
                heap.push((node(b, p).await?.generation, p));
            } else if paint[&p] == X | Y {
                // Re-push so it is examined with its final paint.
                heap.push((node(b, p).await?.generation, p));
            }
        }
    }
    Ok(None)
}

/// Helper for tests and tools: build a tree from `(path, content)` pairs of
/// regular files, storing all blobs and trees. Returns the root tree id.
pub async fn build_tree(objects: &dyn ObjectStore, files: &[(&str, &[u8])]) -> Result<ObjectId> {
    use std::collections::BTreeMap;
    #[derive(Default)]
    struct Dir {
        files: BTreeMap<String, ObjectId>,
        dirs: BTreeMap<String, Dir>,
    }
    let mut root = Dir::default();
    let mut blobs = Vec::new();
    for (path, content) in files {
        let p = RepoPath::new(*path)?;
        let (parent, name) = p
            .split_last()
            .ok_or_else(|| StoreError::InvalidArgument("empty path".into()))?;
        let obj = EncodedObject::new(ant_core::ObjectKind::Blob, content);
        let mut d = &mut root;
        for c in parent.components() {
            d = d.dirs.entry(c.to_string()).or_default();
        }
        d.files.insert(name.to_string(), obj.id());
        blobs.push(obj);
    }
    objects.put_many(&blobs).await?;

    fn encode(d: &Dir, out: &mut Vec<EncodedObject>) -> Result<ObjectId> {
        let mut entries: Vec<TreeEntry> = d
            .files
            .iter()
            .map(|(n, id)| TreeEntry { mode: EntryMode::File, name: n.clone(), id: *id })
            .collect();
        for (n, sub) in &d.dirs {
            entries.push(TreeEntry { mode: EntryMode::Tree, name: n.clone(), id: encode(sub, out)? });
        }
        let obj = Object::Tree(Tree::new(entries)?).encode();
        let id = obj.id();
        out.push(obj);
        Ok(id)
    }
    let mut trees = Vec::new();
    let id = encode(&root, &mut trees)?;
    objects.put_many(&trees).await?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_core::Signature;

    fn sig(t: i64) -> Signature {
        Signature { name: "t".into(), email: "t@t".into(), time: t, tz_offset_minutes: 0 }
    }

    async fn commit(b: &Backend, tree: ObjectId, parents: Vec<ObjectId>, t: i64) -> ObjectId {
        write_commit(
            b,
            &Commit {
                tree,
                parents,
                author: sig(t),
                committer: sig(t),
                extra_headers: vec![],
                message: format!("c{t}\n"),
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn diff_and_lookup() {
        let b = Backend::in_memory();
        let o = b.objects.as_ref();
        let t1 = build_tree(o, &[("a/x", b"1"), ("a/y", b"2"), ("b", b"3")]).await.unwrap();
        let t2 = build_tree(o, &[("a/x", b"1"), ("a/z", b"4"), ("b", b"33"), ("c/d/e", b"5")])
            .await
            .unwrap();
        let d = diff_trees(o, Some(t1), Some(t2)).await.unwrap();
        let summary: Vec<_> = d.iter().map(|c| (c.path.as_str(), c.kind)).collect();
        assert_eq!(
            summary,
            [
                ("a/y", ChangeKind::Deleted),
                ("a/z", ChangeKind::Added),
                ("b", ChangeKind::Modified),
                ("c/d/e", ChangeKind::Added),
            ]
        );
        assert_eq!(diff_trees(o, None, Some(t1)).await.unwrap().len(), 3);
        assert!(diff_trees(o, Some(t1), Some(t1)).await.unwrap().is_empty());

        let (mode, _) = lookup_path(o, t2, &RepoPath::new("c/d").unwrap()).await.unwrap().unwrap();
        assert_eq!(mode, EntryMode::Tree);
        assert!(lookup_path(o, t2, &RepoPath::new("b/nope").unwrap()).await.unwrap().is_none());
        assert_eq!(
            lookup_path(o, t2, &RepoPath::root()).await.unwrap(),
            Some((EntryMode::Tree, t2))
        );
    }

    #[tokio::test]
    async fn file_to_dir_replacement_diffs_both() {
        let b = Backend::in_memory();
        let o = b.objects.as_ref();
        let t1 = build_tree(o, &[("p", b"file")]).await.unwrap();
        let t2 = build_tree(o, &[("p/q", b"nested")]).await.unwrap();
        let d = diff_trees(o, Some(t1), Some(t2)).await.unwrap();
        let summary: Vec<_> = d.iter().map(|c| (c.path.as_str(), c.kind)).collect();
        assert_eq!(summary, [("p", ChangeKind::Deleted), ("p/q", ChangeKind::Added)]);
    }

    #[tokio::test]
    async fn generations_ancestry_merge_base() {
        //   r - a1 - a2
        //    \        \
        //     b1 ----- m
        let b = Backend::in_memory();
        let o = b.objects.as_ref();
        let t = build_tree(o, &[("f", b"x")]).await.unwrap();
        let r = commit(&b, t, vec![], 1).await;
        let a1 = commit(&b, t, vec![r], 2).await;
        let a2 = commit(&b, t, vec![a1], 3).await;
        let b1 = commit(&b, t, vec![r], 4).await;
        let m = commit(&b, t, vec![b1, a2], 5).await;

        let gens: Vec<u64> = b
            .meta
            .get_commits(&[r, a1, a2, b1, m])
            .await
            .unwrap()
            .into_iter()
            .map(|n| n.unwrap().generation)
            .collect();
        assert_eq!(gens, [1, 2, 3, 2, 4]);

        assert!(is_ancestor(&b, r, m).await.unwrap());
        assert!(is_ancestor(&b, a1, m).await.unwrap());
        assert!(is_ancestor(&b, m, m).await.unwrap());
        assert!(!is_ancestor(&b, b1, a2).await.unwrap());
        assert!(!is_ancestor(&b, m, r).await.unwrap());

        assert_eq!(merge_base(&b, a2, b1).await.unwrap(), Some(r));
        assert_eq!(merge_base(&b, a1, m).await.unwrap(), Some(a1));
        assert_eq!(merge_base(&b, a2, a2).await.unwrap(), Some(a2));

        // Root commit changeset is relative to the empty tree.
        assert_eq!(b.meta.get_changeset(&r).await.unwrap().unwrap().len(), 1);
        assert!(b.meta.get_changeset(&a1).await.unwrap().unwrap().is_empty());
    }

    #[tokio::test]
    async fn record_requires_parents() {
        let b = Backend::in_memory();
        let t = build_tree(b.objects.as_ref(), &[("f", b"x")]).await.unwrap();
        let orphan = Commit {
            tree: t,
            parents: vec![ObjectId([7; 32])],
            author: sig(1),
            committer: sig(1),
            extra_headers: vec![],
            message: "m".into(),
        };
        let id = b.objects.put(Object::Commit(orphan).encode()).await.unwrap();
        assert!(matches!(record_commit(&b, id).await, Err(StoreError::InvalidArgument(_))));
    }
}
