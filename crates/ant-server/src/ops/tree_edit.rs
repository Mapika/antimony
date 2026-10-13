//! Path-limited directory loading and editing shared by changeset creation
//! and landing: only the directories on changed paths are read (one batch
//! per depth), edited in memory, and re-encoded deepest first.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use ant_core::{EntryMode, ObjectId, ObjectKind, RepoPath, Tree, TreeEntry};
use ant_store::{Backend, StoreError};

use super::{OpResult, cache};

pub(super) type Entry = (EntryMode, ObjectId);
pub(super) type Directories = BTreeMap<RepoPath, Tree>;

// Load only directories on changed paths, one batch per depth.
pub(super) async fn load_directories<const N: usize>(
    b: &Backend,
    roots: [ObjectId; N],
    paths: &[RepoPath],
) -> OpResult<[Directories; N]> {
    load_directories_with(b, &HashMap::new(), roots, paths).await
}

/// [`load_directories`] that first consults `local` (trees built but not yet
/// written) and then the store-scoped tree cache; only misses hit the store,
/// with at most one `get_many` per depth.
pub(super) async fn load_directories_with<const N: usize>(
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
    let caches = cache::objects(&b.objects);
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
            let mut hits = caches.trees.get_many(&lookups).into_iter();
            let mut misses = Vec::new();
            for ((_, _, id), slot) in pending.iter().zip(&mut trees) {
                if slot.is_none() {
                    *slot = hits.next().flatten();
                    if slot.is_none() {
                        misses.push(*id);
                    }
                }
            }
            if !misses.is_empty() {
                misses.sort_unstable();
                misses.dedup();
                let mut fetched = HashMap::with_capacity(misses.len());
                for (id, object) in misses.iter().zip(b.objects.get_many(&misses).await?) {
                    let object = object.ok_or(StoreError::NotFound(*id))?;
                    if object.kind() != ObjectKind::Tree {
                        return Err(StoreError::WrongKind {
                            id: *id,
                            expected: ObjectKind::Tree,
                            actual: object.kind(),
                        }
                        .into());
                    }
                    fetched.insert(*id, Arc::new(Tree::decode(object.body())?));
                }
                caches.trees.insert_many(
                    fetched.iter().map(|(id, t)| (*id, t.clone(), cache::tree_weight(t))),
                );
                for ((_, _, id), slot) in pending.iter().zip(&mut trees) {
                    if slot.is_none() {
                        *slot = fetched.get(id).cloned();
                    }
                }
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

pub(super) fn entry(dirs: &Directories, path: &RepoPath) -> Option<Entry> {
    let (parent, name) = path.split_last()?;
    dirs.get(&parent)?.get(name).map(|e| (e.mode, e.id))
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
