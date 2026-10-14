//! Tree bundles: the content of `GET /t/{tree}` (see `crate::http`).
//!
//! A bundle holds a tree, its blobs, and then whole subdirectories in
//! breadth-first order while they fit the [`Limits`]. The unit of inclusion
//! below the requested tree is a *directory*: a subtree object together with
//! every blob it lists that is at most `max_blob` bytes. A directory is
//! included completely or not at all, and the walk stops at the first one
//! that does not fit, so a client that holds a directory's tree from a
//! bundle also holds its small blobs. Blobs larger than `max_blob` are never
//! bundled (clients fetch them with `GET /o/{id}`). Blobs of the requested
//! tree itself are added in entry order while they fit (skipping ones that
//! don't). The content is a function of the tree id and the limits only.
//!
//! [`select`] then removes objects a response must not carry (see the
//! safety argument in `crate::http`): a hidden blob of the requested tree is
//! left out; a subdirectory with any hidden object (its tree or any blob it
//! lists) is left out together with everything below it, so the client asks
//! for that directory with its own request.

use std::collections::{HashMap, HashSet};

use ant_core::{EncodedObject, ObjectId, ObjectKind, Tree};
use ant_store::{ObjectStore, StoreError};

/// Size limits of one bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest blob (encoded bytes) put into a bundle.
    pub max_blob: usize,
    /// Total encoded bytes of all objects in a bundle (the requested tree
    /// always counts and is always included).
    pub max_bytes: usize,
    /// Objects in a bundle.
    pub max_objects: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self { max_blob: 1 << 20, max_bytes: 8 << 20, max_objects: 8192 }
    }
}

/// Subtrees decoded per store call while walking.
const TREE_CHUNK: usize = 64;
/// Blob ids per store call while walking (also bounds over-reading past the
/// budget to about one call).
const BLOB_CHUNK: usize = 512;

/// One directory of a bundle.
#[derive(Debug)]
pub struct Unit {
    /// Index of the directory this one was reached from (`None`: the
    /// requested tree).
    pub parent: Option<usize>,
    pub tree: (ObjectId, EncodedObject),
    /// Blobs first bundled by this directory.
    pub blobs: Vec<(ObjectId, EncodedObject)>,
    /// Every bundleable blob the tree lists (also those an earlier
    /// directory already bundled): all must be visible for it to be kept.
    pub refs: Vec<ObjectId>,
}

impl Unit {
    pub fn ids(&self) -> impl Iterator<Item = &ObjectId> {
        std::iter::once(&self.tree.0).chain(self.blobs.iter().map(|(id, _)| id))
    }
}

/// Candidate content of the bundle for `root`; `None` if `root` is missing
/// or not a tree.
pub async fn collect(
    objects: &dyn ObjectStore,
    root: ObjectId,
    limits: Limits,
) -> Result<Option<Vec<Unit>>, StoreError> {
    let Some(root_obj) = objects.get_many(&[root]).await?.pop().flatten() else {
        return Ok(None);
    };
    if root_obj.kind() != ObjectKind::Tree {
        return Ok(None);
    }
    let tree = Tree::decode(root_obj.body())?;
    let mut bytes = root_obj.as_bytes().len();
    let mut count = 1usize;
    let mut bundled: HashSet<ObjectId> = HashSet::from([root]);

    // The requested tree's own blobs: as many as fit, in entry order.
    let (refs, subtrees) = split_entries(&tree);
    let mut blobs = Vec::new();
    for chunk in refs.chunks(BLOB_CHUNK) {
        let wanted: Vec<ObjectId> =
            chunk.iter().filter(|id| !bundled.contains(id)).copied().collect();
        for (id, obj) in wanted.iter().zip(objects.get_many(&wanted).await?) {
            let Some(obj) = obj else { continue };
            let len = obj.as_bytes().len();
            if len > limits.max_blob
                || bytes + len > limits.max_bytes
                || count >= limits.max_objects
                || !bundled.insert(*id)
            {
                continue;
            }
            bytes += len;
            count += 1;
            blobs.push((*id, obj));
        }
    }
    let mut units = vec![Unit { parent: None, tree: (root, root_obj), blobs, refs }];

    // Then whole subdirectories, breadth first, until one does not fit.
    let mut seen: HashSet<ObjectId> = HashSet::from([root]);
    let mut queue: Vec<(usize, ObjectId)> = Vec::new();
    for id in subtrees {
        if seen.insert(id) {
            queue.push((0, id));
        }
    }
    let mut next = 0;
    'walk: while next < queue.len() {
        let batch: Vec<(usize, ObjectId)> =
            queue[next..(next + TREE_CHUNK).min(queue.len())].to_vec();
        next += batch.len();
        let ids: Vec<ObjectId> = batch.iter().map(|(_, id)| *id).collect();
        let trees = objects.get_many(&ids).await?;
        let mut decoded = Vec::with_capacity(batch.len());
        for ((parent, id), obj) in batch.into_iter().zip(trees) {
            // A missing or non-tree subtree cannot be bundled; stop here so
            // the client asks for it (and gets the real error) itself.
            let Some(obj) = obj.filter(|o| o.kind() == ObjectKind::Tree) else { break 'walk };
            let tree = Tree::decode(obj.body())?;
            decoded.push((parent, id, obj, tree));
        }

        // Fetch the blobs of a group of directories at a time.
        let mut group_start = 0;
        while group_start < decoded.len() {
            let mut group_end = group_start;
            let mut entries = 0;
            while group_end < decoded.len() && (group_end == group_start || entries < BLOB_CHUNK) {
                entries += decoded[group_end].3.entries().len();
                group_end += 1;
            }
            let group = &decoded[group_start..group_end];
            group_start = group_end;

            let mut wanted: Vec<ObjectId> = Vec::new();
            let mut listed: Vec<(Vec<ObjectId>, Vec<ObjectId>)> = Vec::new();
            for (_, _, _, tree) in group {
                let (refs, subtrees) = split_entries(tree);
                if refs.len() + 1 > limits.max_objects {
                    break 'walk;
                }
                wanted.extend(refs.iter().filter(|id| !bundled.contains(id)));
                listed.push((refs, subtrees));
            }
            wanted.sort_unstable();
            wanted.dedup();
            let found: HashMap<ObjectId, EncodedObject> = wanted
                .iter()
                .copied()
                .zip(objects.get_many(&wanted).await?)
                .filter_map(|(id, o)| Some((id, o?)))
                .collect();

            for ((parent, id, obj, _), (refs, subtrees)) in group.iter().zip(listed) {
                let mut unit_bytes = obj.as_bytes().len();
                let mut own = Vec::new();
                let mut fresh: HashSet<ObjectId> = HashSet::new();
                for r in &refs {
                    if bundled.contains(r) || !fresh.insert(*r) {
                        continue;
                    }
                    // A listed blob the store lacks: the directory cannot be
                    // complete, so the walk ends before it.
                    let Some(o) = found.get(r) else { break 'walk };
                    let len = o.as_bytes().len();
                    if len > limits.max_blob {
                        continue;
                    }
                    unit_bytes += len;
                    own.push((*r, o.clone()));
                }
                if bytes + unit_bytes > limits.max_bytes
                    || count + 1 + own.len() > limits.max_objects
                {
                    break 'walk;
                }
                bytes += unit_bytes;
                count += 1 + own.len();
                bundled.insert(*id);
                bundled.extend(own.iter().map(|(id, _)| *id));
                let index = units.len();
                units.push(Unit {
                    parent: Some(*parent),
                    tree: (*id, obj.clone()),
                    blobs: own,
                    refs,
                });
                for s in subtrees {
                    if seen.insert(s) {
                        queue.push((index, s));
                    }
                }
            }
        }
    }
    Ok(Some(units))
}

/// Bundleable blob ids (deduplicated, entry order) and subtree ids of `tree`.
/// Gitlinks are not objects of this repository and are skipped.
fn split_entries(tree: &Tree) -> (Vec<ObjectId>, Vec<ObjectId>) {
    let mut seen = HashSet::new();
    let mut blobs = Vec::new();
    let mut subtrees = Vec::new();
    for e in tree.entries() {
        if e.mode.is_tree() {
            subtrees.push(e.id);
        } else if e.mode.is_blob() && seen.insert(e.id) {
            blobs.push(e.id);
        }
    }
    (blobs, subtrees)
}

/// The objects of `units` a response may carry when `hidden` objects must
/// not appear: hidden blobs of the requested tree are dropped, and so is
/// every subdirectory with a hidden tree or blob, with everything reached
/// through it. The requested tree comes first. Callers check the requested
/// tree itself before (it is always included).
pub fn select(units: &[Unit], hidden: &HashSet<ObjectId>) -> Vec<(ObjectId, EncodedObject)> {
    let mut dropped = vec![false; units.len()];
    let mut out = Vec::new();
    for (i, u) in units.iter().enumerate() {
        match u.parent {
            None => {
                out.push(u.tree.clone());
                out.extend(u.blobs.iter().filter(|(id, _)| !hidden.contains(id)).cloned());
            }
            Some(p) => {
                dropped[i] = dropped[p]
                    || hidden.contains(&u.tree.0)
                    || u.refs.iter().any(|id| hidden.contains(id));
                if !dropped[i] {
                    out.push(u.tree.clone());
                    out.extend(u.blobs.iter().cloned());
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_core::{EntryMode, TreeEntry};
    use ant_store::ObjectStoreExt;
    use ant_store::memory::MemoryObjectStore;

    fn blob(s: &str) -> EncodedObject {
        EncodedObject::new(ObjectKind::Blob, s.as_bytes())
    }

    fn tree(entries: &[(&str, EntryMode, ObjectId)]) -> EncodedObject {
        let es: Vec<TreeEntry> = entries
            .iter()
            .map(|(n, m, id)| TreeEntry { mode: *m, name: (*n).to_string(), id: *id })
            .collect();
        EncodedObject::new(ObjectKind::Tree, &Tree::new(es).unwrap().encode())
    }

    /// root: a.txt, big.bin, sub/{b.txt, deep/{c.txt}}, other/{a.txt (same
    /// blob as root's), d.txt}, a gitlink.
    async fn fixture() -> (MemoryObjectStore, ObjectId, HashMap<&'static str, ObjectId>) {
        let store = MemoryObjectStore::default();
        let a = blob("a");
        let big = EncodedObject::new(ObjectKind::Blob, &vec![7u8; 5000]);
        let b = blob("b");
        let c = blob("c");
        let d = blob("d");
        let deep = tree(&[("c.txt", EntryMode::File, c.id())]);
        let sub = tree(&[("b.txt", EntryMode::File, b.id()), ("deep", EntryMode::Tree, deep.id())]);
        let other = tree(&[("a.txt", EntryMode::File, a.id()), ("d.txt", EntryMode::File, d.id())]);
        let root = tree(&[
            ("a.txt", EntryMode::File, a.id()),
            ("big.bin", EntryMode::File, big.id()),
            ("sub", EntryMode::Tree, sub.id()),
            ("other", EntryMode::Tree, other.id()),
            ("module", EntryMode::Gitlink, ObjectId([9; 32])),
        ]);
        let ids = HashMap::from([
            ("a", a.id()),
            ("big", big.id()),
            ("b", b.id()),
            ("c", c.id()),
            ("d", d.id()),
            ("deep", deep.id()),
            ("sub", sub.id()),
            ("other", other.id()),
        ]);
        let root_id = root.id();
        store.put_many(&[a, big, b, c, d, deep, sub, other, root]).await.unwrap();
        (store, root_id, ids)
    }

    fn all_ids(units: &[Unit]) -> Vec<ObjectId> {
        units.iter().flat_map(|u| u.ids().copied().collect::<Vec<_>>()).collect()
    }

    #[tokio::test]
    async fn bundles_everything_that_fits() {
        let (store, root, ids) = fixture().await;
        let units = collect(&store, root, Limits::default()).await.unwrap().unwrap();
        let got = all_ids(&units);
        assert_eq!(got[0], root);
        for name in ["a", "big", "b", "c", "d", "deep", "sub", "other"] {
            assert_eq!(got.iter().filter(|id| **id == ids[name]).count(), 1, "{name}");
        }
        assert_eq!(got.len(), 9, "the shared blob is bundled once; the gitlink never");
        // Every bundled object is what the store holds under its id.
        for u in &units {
            for (id, obj) in std::iter::once(&u.tree).chain(&u.blobs) {
                obj.verify(id).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn large_blobs_are_left_out_and_directories_stay_whole() {
        let (store, root, ids) = fixture().await;
        let limits = Limits { max_blob: 1000, ..Limits::default() };
        let got = all_ids(&collect(&store, root, limits).await.unwrap().unwrap());
        assert!(!got.contains(&ids["big"]));
        assert!(got.contains(&ids["c"]));

        // Room for the root unit plus little else: the walk stops at the
        // first directory that does not fit, never splitting one.
        let root_len = store.get(&root).await.unwrap().unwrap().as_bytes().len();
        let limits = Limits { max_blob: 1000, max_bytes: root_len + 20, max_objects: 100 };
        let units = collect(&store, root, limits).await.unwrap().unwrap();
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].blobs.len(), 1, "a.txt fits");
        // A tiny object budget: the requested tree alone.
        let limits = Limits { max_objects: 1, ..Limits::default() };
        let units = collect(&store, root, limits).await.unwrap().unwrap();
        assert_eq!(all_ids(&units), vec![root]);
    }

    #[tokio::test]
    async fn missing_or_non_tree_roots_are_none() {
        let (store, _, ids) = fixture().await;
        assert!(collect(&store, ObjectId([1; 32]), Limits::default()).await.unwrap().is_none());
        assert!(collect(&store, ids["a"], Limits::default()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn select_drops_hidden_directories_with_their_descendants() {
        let (store, root, ids) = fixture().await;
        let units = collect(&store, root, Limits::default()).await.unwrap().unwrap();
        let ids_of = |hidden: &[ObjectId]| -> Vec<ObjectId> {
            select(&units, &hidden.iter().copied().collect())
                .into_iter()
                .map(|(id, _)| id)
                .collect()
        };
        // Hidden blob directly in the requested tree: only it goes.
        let got = ids_of(&[ids["big"]]);
        assert_eq!(got.len(), 8);
        assert!(!got.contains(&ids["big"]));
        // Hidden blob in sub/: sub and sub/deep go, other/ stays.
        let got = ids_of(&[ids["b"]]);
        for gone in ["b", "sub", "deep", "c"] {
            assert!(!got.contains(&ids[gone]), "{gone}");
        }
        assert!(got.contains(&ids["other"]) && got.contains(&ids["d"]));
        // A blob bundled at the root but also listed in other/: hiding it
        // drops other/ even though other/ does not carry it itself.
        let got = ids_of(&[ids["a"]]);
        assert!(!got.contains(&ids["other"]) && !got.contains(&ids["d"]));
        assert!(got.contains(&ids["sub"]));
        // Hidden subtree object.
        let got = ids_of(&[ids["deep"]]);
        assert!(!got.contains(&ids["deep"]) && !got.contains(&ids["c"]));
        assert!(got.contains(&ids["sub"]) && got.contains(&ids["b"]));
        assert_eq!(ids_of(&[])[0], root);
    }
}
