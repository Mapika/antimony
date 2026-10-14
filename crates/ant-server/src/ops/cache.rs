//! Process-wide caches of immutable store data, scoped per store instance.
//!
//! Everything cached here is immutable once it exists: trees and commits are
//! content-addressed, and a recorded commit's node and changeset never change
//! (`put_commit` is idempotent). Entries are only inserted after they were
//! read from, or durably written to, the store they are scoped to, so a hit
//! never masks a missing object or an unrecorded commit.
//!
//! Scoping holds a `Weak` to the store, which pins its allocation: a store
//! dropped and replaced by another at the same address can never inherit the
//! old store's cache.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, LazyLock, Mutex, Weak};

use ant_core::{Commit, Object, ObjectId, ObjectKind, Tree};
use ant_store::{Backend, CommitNode, MetaStore, ObjectStore, PathChange, StoreError};

/// Two-generation cache: inserts go to `cur`; when `cur` exceeds its budget it
/// becomes `prev` (dropping the old `prev`). Hits in `prev` are promoted.
/// Approximates LRU with O(1) operations and a hard memory bound of roughly
/// twice the budget.
struct Generations<K, V> {
    cur: HashMap<K, (V, usize)>,
    prev: HashMap<K, (V, usize)>,
    weight: usize,
    budget: usize,
}

impl<K: Hash + Eq, V: Clone> Generations<K, V> {
    fn new(budget: usize) -> Self {
        Self { cur: HashMap::new(), prev: HashMap::new(), weight: 0, budget }
    }

    fn get(&mut self, key: &K) -> Option<V>
    where
        K: Clone,
    {
        if let Some((v, _)) = self.cur.get(key) {
            return Some(v.clone());
        }
        let (v, w) = self.prev.remove(key)?;
        self.insert(key.clone(), v.clone(), w);
        Some(v)
    }

    fn insert(&mut self, key: K, value: V, weight: usize) {
        if self.cur.insert(key, (value, weight)).is_none() {
            self.weight += weight;
        }
        if self.weight > self.budget {
            self.prev = std::mem::take(&mut self.cur);
            self.weight = 0;
        }
    }

    /// `(entries, weight)` over both generations.
    fn stats(&self) -> (usize, usize) {
        let prev_weight: usize = self.prev.values().map(|(_, w)| *w).sum();
        (self.cur.len() + self.prev.len(), self.weight + prev_weight)
    }
}

/// A cache keyed by `K`, safe to share between tasks.
pub(crate) struct Cache<K, V>(Mutex<Generations<K, V>>);

impl<K: Hash + Eq + Clone, V: Clone> Cache<K, V> {
    fn new(budget: usize) -> Self {
        Self(Mutex::new(Generations::new(budget)))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Generations<K, V>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn get(&self, key: &K) -> Option<V> {
        self.lock().get(key)
    }

    /// Index-aligned lookup under one lock.
    pub(crate) fn get_many<'a>(&self, keys: impl IntoIterator<Item = &'a K>) -> Vec<Option<V>>
    where
        K: 'a,
    {
        let mut g = self.lock();
        keys.into_iter().map(|k| g.get(k)).collect()
    }

    pub(crate) fn insert(&self, key: K, value: V, weight: usize) {
        self.lock().insert(key, value, weight);
    }

    pub(crate) fn insert_many(&self, entries: impl IntoIterator<Item = (K, V, usize)>) {
        let mut g = self.lock();
        for (k, v, w) in entries {
            g.insert(k, v, w);
        }
    }

    /// `(entries, weight)` currently held.
    pub(crate) fn stats(&self) -> (usize, usize) {
        self.lock().stats()
    }

    fn budget(&self) -> usize {
        self.lock().budget
    }
}

/// Approximate weight of a decoded tree in bytes.
pub(crate) fn tree_weight(tree: &Tree) -> usize {
    64 + tree.entries().iter().map(|e| 72 + e.name.len()).sum::<usize>()
}

/// Caches scoped to one `ObjectStore`.
pub(crate) struct ObjectCaches {
    pub trees: Cache<ObjectId, Arc<Tree>>,
    pub commits: Cache<ObjectId, Arc<Commit>>,
}

/// Caches scoped to one `MetaStore`: recorded commits only.
pub(crate) struct MetaCaches {
    pub nodes: Cache<ObjectId, CommitNode>,
    pub changesets: Cache<ObjectId, Arc<Vec<PathChange>>>,
}

const TREE_BUDGET: usize = 64 << 20;
const COMMIT_BUDGET: usize = 8 << 20;
const NODE_BUDGET: usize = 16 << 20;
const CHANGESET_BUDGET: usize = 16 << 20;

pub(crate) const NODE_WEIGHT: usize = 160;

pub(crate) fn changeset_weight(changes: &[PathChange]) -> usize {
    32 + changes.iter().map(|c| 160 + c.path.as_str().len()).sum::<usize>()
}

pub(crate) fn commit_weight(commit: &Commit) -> usize {
    256 + commit.message.len() + commit.parents.len() * 32
}

/// Totals for one named cache, summed over every live store instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheStat {
    pub name: &'static str,
    pub entries: usize,
    pub weight_bytes: usize,
    pub budget_bytes: usize,
}

type Registry<S, C> = Mutex<Vec<(Weak<S>, Arc<C>)>>;

static OBJECTS: LazyLock<Registry<dyn ObjectStore, ObjectCaches>> = LazyLock::new(Default::default);
static META: LazyLock<Registry<dyn MetaStore, MetaCaches>> = LazyLock::new(Default::default);

fn scoped<S: ?Sized, C>(
    registry: &Registry<S, C>,
    store: &Arc<S>,
    make: impl FnOnce() -> C,
) -> Arc<C> {
    let mut entries = registry.lock().unwrap_or_else(|e| e.into_inner());
    entries.retain(|(weak, _)| weak.strong_count() > 0);
    let weak = Arc::downgrade(store);
    if let Some((_, caches)) = entries.iter().find(|(w, _)| Weak::ptr_eq(w, &weak)) {
        return caches.clone();
    }
    let caches = Arc::new(make());
    entries.push((weak, caches.clone()));
    caches
}

pub(crate) fn objects(store: &Arc<dyn ObjectStore>) -> Arc<ObjectCaches> {
    scoped(&OBJECTS, store, || ObjectCaches {
        trees: Cache::new(TREE_BUDGET),
        commits: Cache::new(COMMIT_BUDGET),
    })
}

pub(crate) fn meta(store: &Arc<dyn MetaStore>) -> Arc<MetaCaches> {
    scoped(&META, store, || MetaCaches {
        nodes: Cache::new(NODE_BUDGET),
        changesets: Cache::new(CHANGESET_BUDGET),
    })
}

/// Clones the caches of every live registration, releasing the lock first.
fn live<S: ?Sized, C>(registry: &Registry<S, C>) -> Vec<Arc<C>> {
    let entries = registry.lock().unwrap_or_else(|e| e.into_inner());
    entries.iter().filter(|(w, _)| w.strong_count() > 0).map(|(_, c)| c.clone()).collect()
}

/// Adds one cache's `(entries, weight, budget)` into `acc`.
fn add<K: Hash + Eq + Clone, V: Clone>(acc: &mut (usize, usize, usize), cache: &Cache<K, V>) {
    let (entries, weight) = cache.stats();
    acc.0 += entries;
    acc.1 += weight;
    acc.2 += cache.budget();
}

/// Process-wide cache sizes for diagnostics: per-cache totals over the live
/// stores (`trees`, `commits`, `nodes`, `changesets`, in that order), plus the
/// number of live object-store and meta-store registrations.
pub fn cache_stats() -> (Vec<CacheStat>, usize, usize) {
    let objects = live(&OBJECTS);
    let metas = live(&META);
    let mut totals = [(0, 0, 0); 4];
    for c in &objects {
        add(&mut totals[0], &c.trees);
        add(&mut totals[1], &c.commits);
    }
    for c in &metas {
        add(&mut totals[2], &c.nodes);
        add(&mut totals[3], &c.changesets);
    }
    const NAMES: [&str; 4] = ["trees", "commits", "nodes", "changesets"];
    let stats = NAMES
        .iter()
        .zip(totals)
        .map(|(name, (entries, weight_bytes, budget_bytes))| CacheStat {
            name,
            entries,
            weight_bytes,
            budget_bytes,
        })
        .collect();
    (stats, objects.len(), metas.len())
}

/// Index-aligned `get_commits` through the node cache: one store round trip
/// for the misses, none when everything is cached.
pub(crate) async fn get_nodes(
    b: &Backend,
    ids: &[ObjectId],
) -> ant_store::Result<Vec<Option<CommitNode>>> {
    let caches = meta(&b.meta);
    let mut out = caches.nodes.get_many(ids);
    let missing: Vec<ObjectId> =
        ids.iter().zip(&out).filter(|(_, n)| n.is_none()).map(|(id, _)| *id).collect();
    if missing.is_empty() {
        return Ok(out);
    }
    let fetched = b.meta.get_commits(&missing).await?;
    let found: HashMap<ObjectId, CommitNode> =
        fetched.into_iter().flatten().map(|n| (n.id, n)).collect();
    caches.nodes.insert_many(found.values().map(|n| (n.id, n.clone(), NODE_WEIGHT)));
    for (id, slot) in ids.iter().zip(&mut out) {
        if slot.is_none() {
            *slot = found.get(id).cloned();
        }
    }
    Ok(out)
}

/// Index-aligned `get_changesets` through the changeset cache.
pub(crate) async fn get_changesets(
    b: &Backend,
    ids: &[ObjectId],
) -> ant_store::Result<Vec<Option<Arc<Vec<PathChange>>>>> {
    let caches = meta(&b.meta);
    let mut out = caches.changesets.get_many(ids);
    let missing: Vec<ObjectId> =
        ids.iter().zip(&out).filter(|(_, c)| c.is_none()).map(|(id, _)| *id).collect();
    if missing.is_empty() {
        return Ok(out);
    }
    let fetched = b.meta.get_changesets(&missing).await?;
    let mut found = HashMap::with_capacity(missing.len());
    for (id, changes) in missing.into_iter().zip(fetched) {
        if let Some(changes) = changes {
            let weight = changeset_weight(&changes);
            let changes = Arc::new(changes);
            caches.changesets.insert(id, changes.clone(), weight);
            found.insert(id, changes);
        }
    }
    for (id, slot) in ids.iter().zip(&mut out) {
        if slot.is_none() {
            *slot = found.get(id).cloned();
        }
    }
    Ok(out)
}

/// Index-aligned commit-object reads through the commit cache. Missing
/// objects are `NotFound`; non-commits are `WrongKind`.
pub(crate) async fn read_commits(
    b: &Backend,
    ids: &[ObjectId],
) -> ant_store::Result<Vec<Arc<Commit>>> {
    let caches = objects(&b.objects);
    let cached = caches.commits.get_many(ids);
    let missing: Vec<ObjectId> =
        ids.iter().zip(&cached).filter(|(_, c)| c.is_none()).map(|(id, _)| *id).collect();
    let mut found = HashMap::with_capacity(missing.len());
    if !missing.is_empty() {
        for (id, object) in missing.iter().zip(b.objects.get_many(&missing).await?) {
            let object = object.ok_or(StoreError::NotFound(*id))?;
            let Object::Commit(commit) = object.decode()? else {
                return Err(StoreError::WrongKind {
                    id: *id,
                    expected: ObjectKind::Commit,
                    actual: object.kind(),
                });
            };
            let weight = commit_weight(&commit);
            let commit = Arc::new(commit);
            caches.commits.insert(*id, commit.clone(), weight);
            found.insert(*id, commit);
        }
    }
    ids.iter()
        .zip(cached)
        .map(|(id, c)| match c {
            Some(c) => Ok(c),
            None => found.get(id).cloned().ok_or(StoreError::NotFound(*id)),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generations_bound_and_promote() {
        let mut g = Generations::new(3);
        for i in 0..3 {
            g.insert(i, i, 1);
        }
        assert_eq!(g.get(&0), Some(0));
        // Exceeding the budget retires the current generation.
        g.insert(3, 3, 1);
        assert!(g.cur.is_empty());
        assert_eq!(g.prev.len(), 4);
        // A hit in the old generation is promoted.
        assert_eq!(g.get(&1), Some(1));
        assert!(g.cur.contains_key(&1));
        for i in 10..14 {
            g.insert(i, i, 1);
        }
        // The un-promoted old entries are gone; memory stays bounded.
        assert_eq!(g.get(&2), None);
        assert!(g.cur.len() + g.prev.len() <= 8);
    }

    #[test]
    fn generations_stats_count_both_generations() {
        let mut g = Generations::new(10);
        for i in 0..3 {
            g.insert(i, i, 1);
        }
        assert_eq!(g.stats(), (3, 3));

        // Budget 3: the fourth insert retires the first four into `prev`.
        let mut g = Generations::new(3);
        for i in 0..4 {
            g.insert(i, i, 1);
        }
        assert_eq!(g.stats(), (4, 4));
    }

    #[test]
    fn cache_stats_reports_live_node_cache() {
        let a: Arc<dyn MetaStore> = Arc::new(ant_store::memory::MemoryMetaStore::default());
        let id = ObjectId([9; 32]);
        let node = CommitNode { id, tree: id, parents: vec![], generation: 1, committer_time: 0 };
        meta(&a).nodes.insert(id, node, NODE_WEIGHT);
        let (stats, _, metas) = cache_stats();
        let names: Vec<&str> = stats.iter().map(|s| s.name).collect();
        assert_eq!(names, ["trees", "commits", "nodes", "changesets"]);
        let nodes = stats.iter().find(|s| s.name == "nodes").expect("nodes entry");
        assert!(nodes.entries >= 1);
        assert!(nodes.budget_bytes >= NODE_BUDGET);
        assert!(metas >= 1);
    }

    #[test]
    fn scoping_is_per_store_instance() {
        let a: Arc<dyn MetaStore> = Arc::new(ant_store::memory::MemoryMetaStore::default());
        let b: Arc<dyn MetaStore> = Arc::new(ant_store::memory::MemoryMetaStore::default());
        let id = ObjectId([7; 32]);
        let node = CommitNode { id, tree: id, parents: vec![], generation: 1, committer_time: 0 };
        meta(&a).nodes.insert(id, node.clone(), NODE_WEIGHT);
        assert_eq!(meta(&a).nodes.get(&id), Some(node));
        assert_eq!(meta(&b).nodes.get(&id), None);
    }
}
