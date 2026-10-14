//! Path-limited directory loading and editing shared by changeset creation
//! and landing: only the directories on changed paths are read (one batch
//! per depth), edited in memory, and re-encoded deepest first.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use ant_core::{EntryMode, ObjectId, RepoPath, Tree, TreeEntry};
use ant_store::{Backend, ChangeKind, PathChange, StoreError};

use super::{OpResult, cache};

pub(super) type Entry = (EntryMode, ObjectId);
pub(super) type Directories = BTreeMap<RepoPath, Tree>;

/// Load only the directories on `paths` under each root, one batch per
/// depth: first from `local` (trees built but not yet written), then the
/// store-scoped tree cache; only misses hit the store, with at most one
/// `get_many` per depth.
pub(super) async fn load_directories<const N: usize>(
    b: &Backend,
    local: &HashMap<ObjectId, Arc<Tree>>,
    roots: [ObjectId; N],
    paths: &[RepoPath],
) -> OpResult<[Directories; N]> {
    let mut needed = BTreeSet::from([RepoPath::root()]);
    for path in paths {
        let mut path = path.clone();
        needed.insert(path.clone());
        while let Some((parent, _)) = path.split_last() {
            needed.insert(parent.clone());
            path = parent;
        }
    }
    let mut pending: Vec<_> =
        roots.into_iter().enumerate().map(|(i, root)| (i, RepoPath::root(), root)).collect();
    let mut dirs: [Directories; N] = std::array::from_fn(|_| BTreeMap::new());
    while !pending.is_empty() {
        let mut trees: Vec<Option<Arc<Tree>>> =
            pending.iter().map(|(_, _, id)| local.get(id).cloned()).collect();
        let lookups: Vec<_> = pending
            .iter()
            .zip(&trees)
            .filter(|(_, t)| t.is_none())
            .map(|((_, _, id), _)| *id)
            .collect();
        if !lookups.is_empty() {
            let mut read = cache::read_trees(b, &lookups).await?.into_iter();
            for slot in trees.iter_mut().filter(|t| t.is_none()) {
                *slot = read.next();
            }
        }
        let mut next = Vec::new();
        for ((i, path, id), tree) in pending.into_iter().zip(trees) {
            let tree = tree.ok_or(StoreError::NotFound(id))?;
            for entry in tree.entries().iter().filter(|e| e.mode.is_tree()) {
                let child = path.join(&entry.name)?;
                if needed.contains(&child) {
                    next.push((i, child, entry.id));
                }
            }
            dirs[i].insert(path, Arc::unwrap_or_clone(tree));
        }
        pending = next;
    }
    Ok(dirs)
}

/// The leaf or directory entry at `path`, with legacy modes reduced to their
/// semantic class (so merges and recorded changes never see legacy spellings).
pub(super) fn entry(dirs: &Directories, path: &RepoPath) -> Option<Entry> {
    let (parent, name) = path.split_last()?;
    dirs.get(&parent)?.get(name).map(|e| (e.mode.class(), e.id))
}

/// The changeset entry for setting the leaf at `path` in `dirs` to `new`
/// (`None` = delete), or `None` when that changes nothing. Changesets
/// describe leaves, so a directory at `path` counts as absent.
pub(super) fn leaf_change(
    dirs: &Directories,
    path: &RepoPath,
    new: Option<Entry>,
) -> Option<PathChange> {
    let old = entry(dirs, path).filter(|(mode, _)| !mode.is_tree());
    let kind = match (old, new) {
        _ if old == new => return None,
        (None, _) => ChangeKind::Added,
        (_, None) => ChangeKind::Deleted,
        _ => ChangeKind::Modified,
    };
    Some(PathChange { path: path.clone(), kind, old, new })
}

pub(super) fn deepest_first(dirs: &Directories) -> Vec<RepoPath> {
    let mut paths: Vec<_> = dirs.keys().cloned().collect();
    paths.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
    paths
}

pub(super) fn prune_empty(dirs: &mut Directories) {
    for path in deepest_first(dirs) {
        if let Some((parent, name)) = path.split_last()
            && dirs.get(&path).is_some_and(Tree::is_empty)
        {
            dirs.remove(&path);
            if let Some(tree) = dirs.get_mut(&parent) {
                tree.remove(name);
            }
        }
    }
}

pub(super) fn apply_write(dirs: &mut Directories, path: &RepoPath, value: Entry) -> OpResult<bool> {
    let Some((parent, name)) = path.split_last() else {
        return Ok(false);
    };
    let mut dir = RepoPath::root();
    for component in parent.components() {
        let child = dir.join(component)?;
        let tree = dirs.entry(dir).or_default();
        if let Some(existing) = tree.get(component) {
            if !existing.mode.is_tree() {
                return Ok(false);
            }
        } else {
            tree.upsert(TreeEntry {
                name: component.into(),
                mode: EntryMode::Tree,
                id: ObjectId::empty_tree(),
            })?;
        }
        dirs.entry(child.clone()).or_default();
        dir = child;
    }
    let tree = dirs.entry(parent).or_default();
    if tree.get(name).is_some_and(|e| e.mode.is_tree()) {
        return Ok(false);
    }
    tree.upsert(TreeEntry { name: name.into(), mode: value.0, id: value.1 })?;
    Ok(true)
}
