//! Commit-graph primitives shared by the server, git bridge, and tests:
//! tree diffing, commit recording, and generation-number ancestry queries.

use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use ant_core::{
    Commit, EncodedObject, EntryMode, Object, ObjectId, ObjectKind, RepoPath, Tree, TreeEntry,
};
use async_trait::async_trait;

use crate::{
    Backend, ChangeKind, CommitNode, ObjectStore, ObjectStoreExt, PathChange, Result, StoreError,
};

/// Where [`diff_trees_with`] reads trees from: one index-aligned batch per
/// call. Every [`ObjectStore`] is one (a single `get_many` per batch); the
/// server layers its tree cache on top.
#[async_trait]
pub trait TreeSource: Send + Sync {
    /// Decoded trees, index-aligned with `ids`. Missing objects are
    /// `NotFound`; non-trees are `WrongKind`.
    async fn read_trees(&self, ids: &[ObjectId]) -> Result<Vec<Arc<Tree>>>;
}

#[async_trait]
impl<T: ObjectStore + ?Sized> TreeSource for T {
    async fn read_trees(&self, ids: &[ObjectId]) -> Result<Vec<Arc<Tree>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        ids.iter()
            .zip(self.get_many(ids).await?)
            .map(|(id, o)| {
                let o = o.ok_or(StoreError::NotFound(*id))?;
                if o.kind() != ObjectKind::Tree {
                    return Err(StoreError::WrongKind {
                        id: *id,
                        expected: ObjectKind::Tree,
                        actual: o.kind(),
                    });
                }
                Ok(Arc::new(Tree::decode(o.body())?))
            })
            .collect()
    }
}

/// Non-tree paths that differ between two trees (`None` = empty tree),
/// sorted by path. Identical subtrees are skipped by id without being read.
pub async fn diff_trees(
    objects: &dyn ObjectStore,
    old: Option<ObjectId>,
    new: Option<ObjectId>,
) -> Result<Vec<PathChange>> {
    diff_trees_with(objects, old, new).await
}

/// [`diff_trees`] reading trees from `source`: breadth first, with one
/// `read_trees` batch per depth (the distinct trees of every differing
/// directory pair at that depth), so the round trips are bounded by the
/// depth of the changed paths, not their number.
pub async fn diff_trees_with(
    source: &(impl TreeSource + ?Sized),
    old: Option<ObjectId>,
    new: Option<ObjectId>,
) -> Result<Vec<PathChange>> {
    let mut out = Vec::new();
    let mut level = vec![(RepoPath::root(), old, new)];
    let empty = Arc::new(Tree::empty());
    while !level.is_empty() {
        level.retain(|(_, o, n)| o != n);
        let mut ids: Vec<ObjectId> =
            level.iter().flat_map(|(_, o, n)| [*o, *n]).flatten().collect();
        ids.sort_unstable();
        ids.dedup();
        let trees: HashMap<ObjectId, Arc<Tree>> =
            ids.iter().copied().zip(source.read_trees(&ids).await?).collect();
        let tree = |id: Option<ObjectId>| match id {
            // `ids` covers every `Some` in `level`; the fallback is unreachable.
            Some(id) => trees.get(&id).cloned().ok_or(StoreError::NotFound(id)),
            None => Ok(empty.clone()),
        };
        let mut next = Vec::new();
        for (dir, old, new) in level {
            let (old_t, new_t) = (tree(old)?, tree(new)?);
            // Union by name: a file and a directory of the same name sort
            // apart in git order, so pair entries by name explicitly.
            let mut names: BTreeMap<&str, (Option<&TreeEntry>, Option<&TreeEntry>)> =
                BTreeMap::new();
            for e in old_t.entries() {
                names.entry(e.name.as_str()).or_default().0 = Some(e);
            }
            for e in new_t.entries() {
                names.entry(e.name.as_str()).or_default().1 = Some(e);
            }
            for (name, (o, n)) in names {
                if o == n {
                    continue;
                }
                let path = dir.join(name)?;
                let o_tree = o.filter(|e| e.mode.is_tree()).map(|e| e.id);
                let n_tree = n.filter(|e| e.mode.is_tree()).map(|e| e.id);
                let o_leaf = o.filter(|e| !e.mode.is_tree()).map(|e| (e.mode.class(), e.id));
                let n_leaf = n.filter(|e| !e.mode.is_tree()).map(|e| (e.mode.class(), e.id));
                if o_tree.is_some() || n_tree.is_some() {
                    next.push((path.clone(), o_tree, n_tree));
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
        level = next;
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
            Some(e) => cur = (e.mode.class(), e.id),
            None => return Ok(None),
        }
    }
    Ok(Some(cur))
}

/// Record an already-stored commit in the meta store: computes generation
/// and the changeset vs. first parent. Parents must already be recorded.
/// Idempotent; returns the log sequence number.
pub async fn record_commit(b: &Backend, id: ObjectId) -> Result<u64> {
    if let Some(node) = b.meta.get_commits(&[id]).await?.pop().flatten() {
        // Already recorded: put_commit is idempotent and returns the seq.
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

// ---------------------------------------------------------------------------
// Ancestry queries
// ---------------------------------------------------------------------------

/// First `first_parent_chain` read of a walk; later reads double up to
/// [`MAX_CHAIN_READ`], so short walks read little and long ones few times.
const FIRST_CHAIN_READ: usize = 256;
/// Largest `first_parent_chain` read of a walk.
const MAX_CHAIN_READ: usize = 8192;
/// Missing nodes fetched as chains in parallel; more than this are fetched
/// with one `get_commits` instead (wide frontiers of merge-heavy history).
const PARALLEL_CHAINS: usize = 8;

fn not_recorded(id: ObjectId) -> StoreError {
    StoreError::InvalidArgument(format!("commit {id} is not recorded"))
}

/// Commit-graph nodes loaded on demand for one walk. A missing node is read
/// together with the first-parent chain below it (`first_parent_chain`, one
/// server-side traversal in persistent stores), so a walk costs about one
/// round trip per chain segment it enters (plus a few for long segments),
/// not one per commit.
struct Nodes<'a> {
    meta: &'a dyn crate::MetaStore,
    nodes: HashMap<ObjectId, CommitNode>,
    chunk: usize,
}

impl<'a> Nodes<'a> {
    /// `first_read`: the most nodes the first chain read may return.
    fn new(b: &'a Backend, first_read: usize) -> Self {
        Self { meta: b.meta.as_ref(), nodes: HashMap::new(), chunk: first_read }
    }

    /// Load `ids` with one `get_commits` (no chains).
    async fn load_exact(&mut self, ids: &[ObjectId]) -> Result<()> {
        for (id, n) in ids.iter().zip(self.meta.get_commits(ids).await?) {
            self.nodes.insert(*id, n.ok_or_else(|| not_recorded(*id))?);
        }
        Ok(())
    }

    fn get(&self, id: &ObjectId) -> Result<&CommitNode> {
        self.nodes.get(id).ok_or_else(|| not_recorded(*id))
    }

    /// Load every `(id, useful)` not loaded yet, in one round of store calls;
    /// `useful` bounds how many first-parent nodes from `id` down can matter
    /// to the caller (at least `id` itself is read). Unrecorded ids are an
    /// error.
    async fn load(&mut self, want: &[(ObjectId, usize)]) -> Result<()> {
        let mut seen = HashSet::new();
        let want: Vec<(ObjectId, usize)> = want
            .iter()
            .copied()
            .filter(|(id, _)| !self.nodes.contains_key(id) && seen.insert(*id))
            .collect();
        if want.is_empty() {
            return Ok(());
        }
        let chunk = self.chunk;
        self.chunk = (chunk * 2).min(MAX_CHAIN_READ);
        if want.len() > PARALLEL_CHAINS {
            let ids: Vec<ObjectId> = want.iter().map(|(id, _)| *id).collect();
            return self.load_exact(&ids).await;
        }
        let meta = self.meta;
        let chains = futures::future::try_join_all(
            want.iter()
                .map(|(id, useful)| meta.first_parent_chain(*id, None, (*useful).clamp(1, chunk))),
        )
        .await?;
        for ((id, _), chain) in want.iter().zip(chains) {
            if chain.first().map(|n| n.id) != Some(*id) {
                return Err(not_recorded(*id));
            }
            for n in chain {
                self.nodes.entry(n.id).or_insert(n);
            }
        }
        Ok(())
    }
}

/// Commits to visit in decreasing generation order. A commit whose node is
/// not loaded yet is queued under an upper bound of its generation (its
/// child's minus one); popping such an entry first loads every such
/// pending commit in one [`Nodes::load`] round and requeues them under
/// their real generations. Bounds never undercut real generations, so the
/// order stays exact.
struct Queue {
    heap: BinaryHeap<(u64, ObjectId)>,
    /// Queued under a bound, not loaded: id → bound.
    pending: HashMap<ObjectId, u64>,
    queued: HashSet<ObjectId>,
    popped: HashSet<ObjectId>,
}

impl Queue {
    fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            pending: HashMap::new(),
            queued: HashSet::new(),
            popped: HashSet::new(),
        }
    }

    /// Queue `id` (once); `bound` = an upper bound of its generation.
    fn push(&mut self, nodes: &Nodes<'_>, id: ObjectId, bound: u64) {
        if !self.queued.insert(id) {
            return;
        }
        match nodes.nodes.get(&id) {
            Some(n) => self.heap.push((n.generation, id)),
            None => {
                self.pending.insert(id, bound);
                self.heap.push((bound, id));
            }
        }
    }

    /// The next commit (loaded), highest generation first, ties by id.
    /// `useful(bound)` = how many first-parent nodes below a pending commit
    /// with that generation bound are worth reading.
    async fn pop(
        &mut self,
        nodes: &mut Nodes<'_>,
        useful: impl Fn(u64) -> usize,
    ) -> Result<Option<ObjectId>> {
        while let Some((g, id)) = self.heap.pop() {
            if self.pending.contains_key(&id) {
                let want: Vec<(ObjectId, usize)> =
                    self.pending.iter().map(|(id, bound)| (*id, useful(*bound))).collect();
                nodes.load(&want).await?;
                for (id, _) in self.pending.drain() {
                    self.heap.push((nodes.get(&id)?.generation, id));
                }
                continue;
            }
            // Bound entries of commits loaded meanwhile are stale (or, when
            // the bound was exact, duplicates).
            if nodes.get(&id)?.generation == g && self.popped.insert(id) {
                return Ok(Some(id));
            }
        }
        Ok(None)
    }
}

/// True if `ancestor` is reachable from `descendant` (a commit is its own
/// ancestor). Walks from `descendant` in generation order and never below
/// `ancestor`'s generation; nodes are read a chain segment per store call
/// (bounded by the generation gap), not one commit per call. Unrecorded
/// commits are `InvalidArgument`.
pub async fn is_ancestor(b: &Backend, ancestor: ObjectId, descendant: ObjectId) -> Result<bool> {
    if ancestor == descendant {
        return Ok(true);
    }
    // The generation gap bounds every chain read: start with the largest.
    let mut nodes = Nodes::new(b, MAX_CHAIN_READ);
    nodes.load_exact(&[ancestor, descendant]).await?;
    let target = nodes.get(&ancestor)?.generation;
    let gap = |g: u64| usize::try_from(g.saturating_sub(target)).unwrap_or(usize::MAX);
    let top = nodes.get(&descendant)?.generation;
    if top <= target {
        return Ok(false);
    }
    let mut queue = Queue::new();
    queue.push(&nodes, descendant, top);
    while let Some(id) = queue.pop(&mut nodes, gap).await? {
        let n = nodes.get(&id)?;
        if n.generation <= target {
            continue;
        }
        let (g, parents) = (n.generation, n.parents.clone());
        for p in parents {
            if p == ancestor {
                return Ok(true);
            }
            // A parent's generation is below its child's; at or below the
            // target's it cannot reach the target.
            if g - 1 > target {
                queue.push(&nodes, p, g - 1);
            }
        }
    }
    Ok(false)
}

/// True if `ancestor` lies on the first-parent chain of `descendant`
/// (inclusive). Unrecorded commits are simply not ancestors. Reads the
/// chain in chunks bounded by the generation gap.
pub async fn is_first_parent_ancestor(
    b: &Backend,
    ancestor: ObjectId,
    descendant: ObjectId,
) -> Result<bool> {
    if ancestor == descendant {
        return Ok(true);
    }
    let [Some(target), Some(desc)] =
        <[_; 2]>::try_from(b.meta.get_commits(&[ancestor, descendant]).await?)
            .unwrap_or([None, None])
    else {
        return Ok(false);
    };
    // Generations strictly decrease along a chain: a newer `ancestor` needs
    // no walk, and otherwise the walk is at most the generation gap long.
    if desc.generation <= target.generation {
        return Ok(false);
    }
    let gap = usize::try_from(desc.generation - target.generation).unwrap_or(usize::MAX);
    let mut from = descendant;
    loop {
        let chunk =
            b.meta.first_parent_chain(from, Some(ancestor), MAX_CHAIN_READ.min(gap)).await?;
        let Some(last) = chunk.last() else { return Ok(false) };
        match last.parents.first() {
            Some(p) if *p == ancestor => return Ok(true),
            Some(p)
                if chunk.len() == MAX_CHAIN_READ.min(gap)
                    && last.generation > target.generation + 1 =>
            {
                from = *p
            }
            _ => return Ok(false),
        }
    }
}

/// A best common ancestor of `x` and `y`: the one with the highest
/// generation (no other common ancestor descends from it), ties broken by
/// the larger id. `None` for unrelated histories. Paints both sides'
/// ancestry walking in generation order; nodes are read a chain segment per
/// store call. Unrecorded commits are `InvalidArgument`.
pub async fn merge_base(b: &Backend, x: ObjectId, y: ObjectId) -> Result<Option<ObjectId>> {
    const X: u8 = 1;
    const Y: u8 = 2;
    let mut nodes = Nodes::new(b, FIRST_CHAIN_READ);
    nodes.load(&[(x, MAX_CHAIN_READ), (y, MAX_CHAIN_READ)]).await?;
    if x == y {
        return Ok(Some(x));
    }
    let mut paint: HashMap<ObjectId, u8> = HashMap::from([(x, X), (y, Y)]);
    let mut queue = Queue::new();
    let gx = nodes.get(&x)?.generation;
    let gy = nodes.get(&y)?.generation;
    queue.push(&nodes, x, gx);
    queue.push(&nodes, y, gy);
    let chunk = |_| MAX_CHAIN_READ;
    while let Some(id) = queue.pop(&mut nodes, chunk).await? {
        let flags = paint[&id];
        if flags == X | Y {
            return Ok(Some(id));
        }
        // Every descendant of a commit (higher generation) is popped before
        // it, so its paint is final here: the first commit painted by both
        // sides is a common ancestor of the highest generation.
        let n = nodes.get(&id)?;
        let (g, parents) = (n.generation, n.parents.clone());
        for p in parents {
            *paint.entry(p).or_default() |= flags;
            queue.push(&nodes, p, g - 1);
        }
    }
    Ok(None)
}

/// The newest commit on `to`'s first-parent chain that is reachable from
/// `from` (`None`: no commit of that chain is). Walks both in generation
/// order: `from`'s whole ancestry (painted "excluded") and `to`'s first
/// parents, until the chain meets a painted commit or `from`'s ancestry is
/// exhausted. `log from..to` with first parents only is the chain from
/// `to` down to, excluding, this commit. Unrecorded commits are
/// `InvalidArgument`.
pub async fn first_parent_range_end(
    b: &Backend,
    from: ObjectId,
    to: ObjectId,
) -> Result<Option<ObjectId>> {
    const CHAIN: u8 = 1;
    const EXCLUDED: u8 = 2;
    let mut nodes = Nodes::new(b, FIRST_CHAIN_READ);
    nodes.load(&[(to, MAX_CHAIN_READ), (from, MAX_CHAIN_READ)]).await?;
    if from == to {
        return Ok(Some(to));
    }
    let mut paint: HashMap<ObjectId, u8> = HashMap::from([(to, CHAIN), (from, EXCLUDED)]);
    let mut queue = Queue::new();
    let (gt, gf) = (nodes.get(&to)?.generation, nodes.get(&from)?.generation);
    queue.push(&nodes, to, gt);
    queue.push(&nodes, from, gf);
    // Queued commits painted excluded; none left = the rest of the chain is in range.
    let mut excluded = 1usize;
    let chunk = |_| MAX_CHAIN_READ;
    while let Some(id) = queue.pop(&mut nodes, chunk).await? {
        let flags = paint[&id];
        let n = nodes.get(&id)?;
        let (g, parents) = (n.generation, n.parents.clone());
        if flags & EXCLUDED != 0 {
            if flags & CHAIN != 0 {
                return Ok(Some(id));
            }
            excluded -= 1;
            for p in parents {
                let before = paint.get(&p).copied().unwrap_or(0);
                paint.insert(p, before | EXCLUDED);
                if before & EXCLUDED == 0 {
                    excluded += 1;
                    queue.push(&nodes, p, g - 1);
                }
            }
        } else {
            if excluded == 0 {
                return Ok(None);
            }
            let Some(p) = parents.first().copied() else { return Ok(None) };
            let before = paint.get(&p).copied().unwrap_or(0);
            paint.insert(p, before | CHAIN);
            queue.push(&nodes, p, g - 1);
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
        let (parent, name) =
            p.split_last().ok_or_else(|| StoreError::InvalidArgument("empty path".into()))?;
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
            entries.push(TreeEntry {
                mode: EntryMode::Tree,
                name: n.clone(),
                id: encode(sub, out)?,
            });
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
                message: format!("c{t}\n").into_bytes(),
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

    /// Counts `get_many` calls and the ids they asked for.
    struct Counting {
        inner: crate::memory::MemoryObjectStore,
        calls: std::sync::atomic::AtomicUsize,
        ids: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl ObjectStore for Counting {
        async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
            use std::sync::atomic::Ordering::Relaxed;
            self.calls.fetch_add(1, Relaxed);
            self.ids.fetch_add(ids.len(), Relaxed);
            self.inner.get_many(ids).await
        }
        async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
            self.inner.put_many(objects).await
        }
        async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
            self.inner.has_many(ids).await
        }
    }

    /// Many changed directories cost one batched read per depth, and only
    /// the differing trees are read (each distinct tree once).
    #[tokio::test]
    async fn diff_reads_one_batch_per_depth() {
        use std::sync::atomic::Ordering::Relaxed;
        let o = Counting {
            inner: Default::default(),
            calls: Default::default(),
            ids: Default::default(),
        };
        let paths: Vec<String> =
            (0..256).map(|i| format!("agents/{:02}/{i}/f.txt", i % 16)).collect();
        // Distinct content per file: no two agent directories share a tree.
        let olds: Vec<Vec<u8>> = paths.iter().map(|p| format!("{p} old").into_bytes()).collect();
        let news: Vec<Vec<u8>> = paths.iter().map(|p| format!("{p} new").into_bytes()).collect();
        let old: Vec<(&str, &[u8])> =
            paths.iter().zip(&olds).map(|(p, c)| (p.as_str(), c.as_slice())).collect();
        let mut new = old.clone();
        // 100 changed agent directories, one deleted file, one added file,
        // and a file replaced by a directory.
        for ((_, content), n) in new.iter_mut().zip(&news).take(100) {
            *content = n;
        }
        new.retain(|(p, _)| *p != paths[200]);
        new.push(("agents/99/x/f.txt", b"old"));
        let old_t = build_tree(&o, &old).await.unwrap();
        let mut with_file = old.clone();
        with_file.push(("p", b"file"));
        let old_t2 = build_tree(&o, &with_file).await.unwrap();
        new.push(("p/q", b"nested"));
        let new_t = build_tree(&o, &new).await.unwrap();
        o.calls.store(0, Relaxed);

        let d = diff_trees(&o, Some(old_t), Some(new_t)).await.unwrap();
        // root, agents, shards, agent dirs (`p` adds one level below root).
        assert_eq!(o.calls.load(Relaxed), 4);
        let mut want: Vec<(String, ChangeKind)> =
            paths[..100].iter().map(|p| (p.clone(), ChangeKind::Modified)).collect();
        want.push((paths[200].clone(), ChangeKind::Deleted));
        want.push(("agents/99/x/f.txt".into(), ChangeKind::Added));
        want.push(("p/q".into(), ChangeKind::Added));
        want.sort_by(|a, b| a.0.cmp(&b.0));
        let got: Vec<(String, ChangeKind)> =
            d.iter().map(|c| (c.path.as_str().to_string(), c.kind)).collect();
        assert_eq!(got, want);
        // Differing trees only: 2 roots, 2 `agents`, 2 × 16 shards (all
        // touched), 2 × 100 changed agent dirs, the emptied (so removed)
        // agent dir's old side, 1 new shard, 1 new agent dir, and `p`.
        assert_eq!(o.ids.load(Relaxed), 2 + 2 + 32 + 200 + 1 + 1 + 1 + 1);

        let d = diff_trees(&o, Some(old_t2), Some(new_t)).await.unwrap();
        let p: Vec<_> = d.iter().filter(|c| c.path.as_str().starts_with('p')).collect();
        assert_eq!(p.len(), 2);
        assert_eq!((p[0].path.as_str(), p[0].kind), ("p", ChangeKind::Deleted));
        assert_eq!((p[1].path.as_str(), p[1].kind), ("p/q", ChangeKind::Added));
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

    /// Counts the graph reads of a `MemoryMetaStore` (every method forwarded).
    #[derive(Default)]
    struct CountingMeta {
        inner: crate::memory::MemoryMetaStore,
        get_commits: std::sync::atomic::AtomicUsize,
        chains: std::sync::atomic::AtomicUsize,
    }

    impl CountingMeta {
        fn calls(&self) -> usize {
            use std::sync::atomic::Ordering::Relaxed;
            self.get_commits.load(Relaxed) + self.chains.load(Relaxed)
        }
        fn reset(&self) {
            use std::sync::atomic::Ordering::Relaxed;
            self.get_commits.store(0, Relaxed);
            self.chains.store(0, Relaxed);
        }
    }

    #[async_trait]
    impl crate::MetaStore for CountingMeta {
        async fn put_commit(&self, node: CommitNode, changes: Vec<PathChange>) -> Result<u64> {
            self.inner.put_commit(node, changes).await
        }
        async fn get_commits(&self, ids: &[ObjectId]) -> Result<Vec<Option<CommitNode>>> {
            self.get_commits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.get_commits(ids).await
        }
        async fn get_changeset(&self, id: &ObjectId) -> Result<Option<Vec<PathChange>>> {
            self.inner.get_changeset(id).await
        }
        async fn first_parent_chain(
            &self,
            tip: ObjectId,
            stop: Option<ObjectId>,
            limit: usize,
        ) -> Result<Vec<CommitNode>> {
            self.chains.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.first_parent_chain(tip, stop, limit).await
        }
        async fn tail(&self, after: u64, limit: usize) -> Result<Vec<(u64, ObjectId)>> {
            self.inner.tail(after, limit).await
        }
        async fn get_derived(&self, kind: &str, key: &[u8]) -> Result<Option<bytes::Bytes>> {
            self.inner.get_derived(kind, key).await
        }
        async fn put_derived(&self, kind: &str, key: &[u8], value: bytes::Bytes) -> Result<()> {
            self.inner.put_derived(kind, key, value).await
        }
        async fn index_history(&self, ref_name: &str, rows: &[crate::HistoryRow]) -> Result<()> {
            self.inner.index_history(ref_name, rows).await
        }
        async fn query_history(
            &self,
            ref_name: &str,
            prefix: &RepoPath,
            max_generation: u64,
            limit: usize,
        ) -> Result<Vec<(u64, ObjectId)>> {
            self.inner.query_history(ref_name, prefix, max_generation, limit).await
        }
        async fn history_watermark(&self, ref_name: &str) -> Result<Option<ObjectId>> {
            self.inner.history_watermark(ref_name).await
        }
        async fn set_history_watermark(
            &self,
            ref_name: &str,
            expected: Option<ObjectId>,
            new: ObjectId,
        ) -> Result<bool> {
            self.inner.set_history_watermark(ref_name, expected, new).await
        }
        async fn clear_history(&self, ref_name: &str) -> Result<()> {
            self.inner.clear_history(ref_name).await
        }
    }

    /// Graph-only commits (no objects): ids derived from a counter.
    struct Dag {
        b: Backend,
        meta: Arc<CountingMeta>,
        next: u64,
    }

    impl Dag {
        fn new() -> Self {
            let meta = Arc::new(CountingMeta::default());
            let mut b = Backend::in_memory();
            b.meta = meta.clone();
            Self { b, meta, next: 0 }
        }

        async fn add(&mut self, parents: &[ObjectId]) -> ObjectId {
            use crate::MetaStore;
            self.next += 1;
            let id = EncodedObject::new(ObjectKind::Blob, &self.next.to_le_bytes()).id();
            let pg = self.meta.inner.get_commits(parents).await.unwrap();
            let generation =
                1 + pg.iter().map(|n| n.as_ref().unwrap().generation).max().unwrap_or(0);
            let node = CommitNode {
                id,
                tree: ObjectId([0; 32]),
                parents: parents.to_vec(),
                generation,
                committer_time: 0,
            };
            self.meta.inner.put_commit(node, vec![]).await.unwrap();
            id
        }

        async fn chain(&mut self, from: Option<ObjectId>, n: usize) -> Vec<ObjectId> {
            let mut out = Vec::with_capacity(n);
            let mut prev = from;
            for _ in 0..n {
                let id = self.add(prev.as_slice()).await;
                out.push(id);
                prev = Some(id);
            }
            out
        }
    }

    /// Long linear histories cost a handful of store calls, not one per
    /// commit (the walks read whole first-parent segments at a time).
    #[tokio::test]
    async fn ancestry_over_long_histories_is_batched() {
        let mut d = Dag::new();
        let main = d.chain(None, 20_000).await;
        let side = d.chain(Some(main[10_000]), 50).await;
        let b = d.b.clone();
        let (tip, side_tip) = (main[19_999], side[49]);

        d.meta.reset();
        assert_eq!(merge_base(&b, side_tip, tip).await.unwrap(), Some(main[10_000]));
        let calls = d.meta.calls();
        assert!(calls <= 8, "merge_base took {calls} store calls");

        d.meta.reset();
        assert!(is_ancestor(&b, main[100], tip).await.unwrap());
        let calls = d.meta.calls();
        assert!(calls <= 4, "is_ancestor took {calls} store calls");

        d.meta.reset();
        assert!(!is_ancestor(&b, side[0], tip).await.unwrap());
        let calls = d.meta.calls();
        assert!(calls <= 4, "is_ancestor (false) took {calls} store calls");

        d.meta.reset();
        assert!(is_first_parent_ancestor(&b, main[100], tip).await.unwrap());
        assert!(d.meta.calls() <= 4, "{}", d.meta.calls());

        d.meta.reset();
        assert_eq!(first_parent_range_end(&b, side_tip, tip).await.unwrap(), Some(main[10_000]));
        let calls = d.meta.calls();
        assert!(calls <= 8, "first_parent_range_end took {calls} store calls");

        // Ancestor newer than the descendant: no walk at all.
        d.meta.reset();
        assert!(!is_ancestor(&b, tip, main[5]).await.unwrap());
        assert_eq!(d.meta.calls(), 1);
    }

    /// Merge commits (imported history): second parents are walked too.
    #[tokio::test]
    async fn ancestry_through_merges() {
        //  r - a1 - a2 - m1 - a3 ----- m2
        //   \           /             /
        //    b1 ------ b2 - b3 - b4 -
        //                \
        //                 c1
        let mut d = Dag::new();
        let r = d.add(&[]).await;
        let a = d.chain(Some(r), 2).await;
        let bs = d.chain(Some(r), 2).await;
        let m1 = d.add(&[a[1], bs[1]]).await;
        let a3 = d.add(&[m1]).await;
        let b34 = d.chain(Some(bs[1]), 2).await;
        let m2 = d.add(&[a3, b34[1]]).await;
        let c1 = d.add(&[bs[1]]).await;
        let b = d.b.clone();

        assert!(is_ancestor(&b, bs[0], m2).await.unwrap());
        assert!(is_ancestor(&b, b34[0], m2).await.unwrap());
        assert!(!is_ancestor(&b, c1, m2).await.unwrap());
        assert!(!is_first_parent_ancestor(&b, bs[1], m2).await.unwrap());
        assert!(is_first_parent_ancestor(&b, a[0], m2).await.unwrap());
        assert_eq!(merge_base(&b, c1, m2).await.unwrap(), Some(bs[1]));
        assert_eq!(merge_base(&b, c1, a3).await.unwrap(), Some(bs[1]));
        assert_eq!(merge_base(&b, a[1], b34[0]).await.unwrap(), Some(r));
        assert_eq!(merge_base(&b, m1, m2).await.unwrap(), Some(m1));
        // m2's first-parent chain is m2 a3 m1 a2 a1 r; from c1 it meets r
        // (b2 is reachable from c1 but not on that chain).
        assert_eq!(first_parent_range_end(&b, c1, m2).await.unwrap(), Some(r));
        assert_eq!(first_parent_range_end(&b, a[0], m2).await.unwrap(), Some(a[0]));
        assert_eq!(first_parent_range_end(&b, m2, a3).await.unwrap(), Some(a3));
        assert_eq!(first_parent_range_end(&b, m1, m1).await.unwrap(), Some(m1));

        // Unrelated histories.
        let other = d.chain(None, 3).await;
        assert_eq!(merge_base(&b, other[2], m2).await.unwrap(), None);
        assert_eq!(first_parent_range_end(&b, other[2], m2).await.unwrap(), None);
        assert!(!is_ancestor(&b, other[0], m2).await.unwrap());

        // Unrecorded commits are invalid arguments.
        let ghost = ObjectId([9; 32]);
        assert!(matches!(merge_base(&b, ghost, m2).await, Err(StoreError::InvalidArgument(_))));
        assert!(matches!(is_ancestor(&b, ghost, m2).await, Err(StoreError::InvalidArgument(_))));
        assert!(!is_first_parent_ancestor(&b, ghost, m2).await.unwrap());
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
