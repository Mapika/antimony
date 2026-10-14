//! Breadth-first walks restricted to a few branches of a tree: the reader
//! behind [`crate::update::plan`]'s base lookups and the lazy workspace's
//! status (`ant_fuse::rw`). Every tree of a depth is read with batched
//! `get_many` calls ([`BATCH`] ids each); blobs are never read.

use std::collections::{BTreeMap, HashMap};

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, Tree, TreeEntry};
use ant_store::{ObjectStore, StoreError};

/// Objects per `get_many` / `put_many` call in client batch work.
pub const BATCH: usize = 512;

/// What a walk does with one entry of a tree it read.
pub enum Visit {
    /// Ignore the entry (and everything below it).
    Skip,
    /// Record the entry.
    Leaf,
    /// Read the entry as a subtree (trees only); the flag is passed to the
    /// visits of its entries.
    Descend(bool),
}

/// Why a walk failed. Callers turn it into their own error.
#[derive(Debug)]
pub enum WalkError {
    Store(StoreError),
    /// `get_many` returned a different number of results than ids.
    Misaligned,
    Missing(ObjectId),
    NotTree(ObjectId),
    Decode(ant_core::Error),
}

/// Walk `root`, asking `visit(path, entry, flag)` about every entry of every
/// tree read (`flag` comes from the `Descend` that led there; `false` at the
/// root). Returns the entries visited as `Leaf`, by full path.
pub async fn walk(
    objects: &dyn ObjectStore,
    root: ObjectId,
    visit: impl FnMut(&str, &TreeEntry, bool) -> Visit,
) -> Result<BTreeMap<String, (EntryMode, ObjectId)>, WalkError> {
    walk_cached(objects, root, &HashMap::new(), visit).await
}

/// [`walk`] that takes the trees in `known` (by id) without reading them
/// from `objects`.
pub async fn walk_cached(
    objects: &dyn ObjectStore,
    root: ObjectId,
    known: &HashMap<ObjectId, EncodedObject>,
    mut visit: impl FnMut(&str, &TreeEntry, bool) -> Visit,
) -> Result<BTreeMap<String, (EntryMode, ObjectId)>, WalkError> {
    let mut out = BTreeMap::new();
    let mut level = vec![(String::new(), root, false)];
    while !level.is_empty() {
        let mut next = Vec::new();
        for chunk in level.chunks(BATCH) {
            let ids: Vec<ObjectId> =
                chunk.iter().map(|(_, id, _)| *id).filter(|id| !known.contains_key(id)).collect();
            let found = if ids.is_empty() {
                Vec::new()
            } else {
                objects.get_many(&ids).await.map_err(WalkError::Store)?
            };
            if found.len() != ids.len() {
                return Err(WalkError::Misaligned);
            }
            let mut found = found.into_iter();
            for (prefix, id, flag) in chunk {
                let obj = match known.get(id) {
                    Some(obj) => obj.clone(),
                    None => found.next().flatten().ok_or(WalkError::Missing(*id))?,
                };
                if obj.kind() != ObjectKind::Tree {
                    return Err(WalkError::NotTree(*id));
                }
                for e in Tree::decode(obj.body()).map_err(WalkError::Decode)?.entries() {
                    let path = if prefix.is_empty() {
                        e.name.clone()
                    } else {
                        format!("{prefix}/{}", e.name)
                    };
                    match visit(&path, e, *flag) {
                        Visit::Skip => {}
                        Visit::Leaf => {
                            out.insert(path, (e.mode.class(), e.id));
                        }
                        Visit::Descend(flag) => next.push((path, e.id, flag)),
                    }
                }
            }
        }
        level = next;
    }
    Ok(out)
}
