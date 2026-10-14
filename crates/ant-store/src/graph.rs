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

mod skip;
pub use skip::{SKIP_KIND, fill as fill_skip_index};

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
/// Generations below its position that a DAG walk's first
/// `ancestors_above` read covers; doubles with every read. Also the
/// generation gap above which `is_ancestor` and `merge_base` first try the
/// first-parent skip index (below it the walk takes a read or two).
const FIRST_WINDOW: u64 = 256;
/// Most commits one `ancestors_above` read returns.
const MAX_ANCESTRY_READ: usize = 1 << 17;
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
    /// Commits loaded by a complete [`Nodes::load_above`] read → its floor:
    /// their parents that read did not return lie at or below it.
    floors: HashMap<ObjectId, u64>,
}

impl<'a> Nodes<'a> {
    /// `first_read`: the most nodes the first chain read may return.
    fn new(b: &'a Backend, first_read: usize) -> Self {
        Self {
            meta: b.meta.as_ref(),
            nodes: HashMap::new(),
            chunk: first_read,
            floors: HashMap::new(),
        }
    }

    /// Load `ids` with one `get_commits` (no chains).
    async fn load_exact(&mut self, ids: &[ObjectId]) -> Result<()> {
        for (id, n) in ids.iter().zip(self.meta.get_commits(ids).await?) {
            self.nodes.insert(*id, n.ok_or_else(|| not_recorded(*id))?);
        }
        Ok(())
    }

    /// Load the ancestry of `tips` above generation `floor` (as far as one
    /// `ancestors_above` read returns) with one store call.
    async fn load_above(&mut self, tips: &[ObjectId], floor: u64) -> Result<()> {
        let limit = MAX_ANCESTRY_READ.max(tips.len());
        let read = self.meta.ancestors_above(tips, floor, limit).await?;
        let complete = read.len() < limit;
        for n in read {
            if complete {
                self.floors.insert(n.id, floor);
            }
            self.nodes.entry(n.id).or_insert(n);
        }
        Ok(())
    }

    fn get(&self, id: &ObjectId) -> Result<&CommitNode> {
        self.nodes.get(id).ok_or_else(|| not_recorded(*id))
    }

    /// An upper bound of the generations of `child`'s parents (loaded):
    /// below its own, and at or below the floor of the read that loaded it
    /// for parents that read did not return. In merge-heavy history the
    /// latter keeps old second parents (long-lived topic branches) from
    /// being read before the walk gets down to them.
    fn parent_bound(&self, child: &ObjectId) -> Result<u64> {
        let g = self.get(child)?.generation - 1;
        Ok(self.floors.get(child).map_or(g, |f| g.min(*f)))
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
/// pending commit in one round of store calls and requeues them under
/// their real generations. Bounds never undercut real generations, so the
/// order stays exact.
///
/// Two ways to load: [`Queue::pop`] reads first-parent chain segments
/// below the pending commits ([`Nodes::load`]; for walks that mostly follow
/// first parents), [`Queue::pop_above`] reads their whole ancestry down to
/// a generation floor ([`Nodes::load_above`]; for walks over the whole
/// DAG, where in merge-heavy history nearly every first-parent commit has
/// a second parent to visit: one round per merge otherwise).
struct Queue {
    heap: BinaryHeap<(u64, ObjectId)>,
    /// Queued under a bound, not loaded: id → bound.
    pending: HashMap<ObjectId, u64>,
    queued: HashSet<ObjectId>,
    popped: HashSet<ObjectId>,
    /// [`Queue::pop_above`]: generations the next read covers, and the one
    /// after it (doubling from there on).
    window: u64,
    next_window: u64,
}

impl Queue {
    /// `first_window`: generations the first [`Queue::pop_above`] read
    /// covers; later ones cover [`FIRST_WINDOW`] doubling.
    fn new(first_window: u64) -> Self {
        Self {
            heap: BinaryHeap::new(),
            pending: HashMap::new(),
            queued: HashSet::new(),
            popped: HashSet::new(),
            window: first_window.max(1),
            next_window: FIRST_WINDOW,
        }
    }

    /// The next commit of generation above `stop` (loaded), highest
    /// generation first, ties by id; `None` when none is left. Popping a
    /// pending commit loads the ancestry of all pending commits down to a
    /// window of generations below it, which doubles every time (one
    /// `ancestors_above` read: a walk costs about one store call per
    /// doubling of its depth, not one per merge it passes). Pending commits
    /// that read does not return lie at or below its floor: they are
    /// requeued under the floor, or dropped when that is at or below `stop`
    /// (with `stop` 0 every recorded commit lies above it: such a commit is
    /// unrecorded, an error).
    async fn pop_above(&mut self, nodes: &mut Nodes<'_>, stop: u64) -> Result<Option<ObjectId>> {
        while let Some((g, id)) = self.heap.pop() {
            if g <= stop {
                return Ok(None);
            }
            if let Some(&bound) = self.pending.get(&id) {
                if bound != g {
                    continue; // requeued under a lower bound since
                }
                let floor = g.saturating_sub(self.window).max(stop);
                self.window = self.next_window;
                self.next_window = self.next_window.saturating_mul(2);
                let tips: Vec<ObjectId> = self.pending.keys().copied().collect();
                nodes.load_above(&tips, floor).await?;
                for (id, bound) in std::mem::take(&mut self.pending) {
                    match nodes.nodes.get(&id) {
                        Some(n) => self.heap.push((n.generation, id)),
                        None if floor > stop => {
                            let bound = bound.min(floor);
                            self.pending.insert(id, bound);
                            self.heap.push((bound, id));
                        }
                        None if stop == 0 => return Err(not_recorded(id)),
                        None => {}
                    }
                }
                continue;
            }
            // Entries of commits loaded under a bound are stale (or, when
            // the bound was exact, duplicates); those of dropped commits are
            // not loaded.
            match nodes.nodes.get(&id) {
                Some(n) if n.generation == g && self.popped.insert(id) => return Ok(Some(id)),
                _ => {}
            }
        }
        Ok(None)
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
/// ancestor). When `ancestor` is more than [`FIRST_WINDOW`] generations
/// older, first checks `descendant`'s first-parent chain through the skip
/// index (O(log n) reads: the usual fast-forward check of a ref update).
/// Otherwise, or when it is not on that chain, walks from `descendant` in
/// generation order and never below `ancestor`'s generation. A walk in
/// generation order visits everything above the generation where it finds
/// `ancestor` anyway, so it reads all of that in one store call
/// ([`Queue::pop_above`] with the whole gap as its window; more only past
/// [`MAX_ANCESTRY_READ`] commits). Unrecorded commits are
/// `InvalidArgument`.
pub async fn is_ancestor(b: &Backend, ancestor: ObjectId, descendant: ObjectId) -> Result<bool> {
    if ancestor == descendant {
        return Ok(true);
    }
    let mut nodes = Nodes::new(b, MAX_CHAIN_READ);
    nodes.load_exact(&[ancestor, descendant]).await?;
    let target = nodes.get(&ancestor)?.generation;
    let top = nodes.get(&descendant)?.generation;
    if top <= target {
        return Ok(false);
    }
    if top - target > FIRST_WINDOW
        && skip::on_first_parent_chain(b, nodes.get(&ancestor)?, descendant).await? == Some(true)
    {
        return Ok(true);
    }
    let mut queue = Queue::new(top - target);
    queue.push(&nodes, descendant, top);
    while let Some(id) = queue.pop_above(&mut nodes, target).await? {
        let parents = nodes.get(&id)?.parents.clone();
        let bound = nodes.parent_bound(&id)?;
        for p in parents {
            if p == ancestor {
                return Ok(true);
            }
            // Commits at or below the target's generation, which cannot
            // reach it, are never popped.
            queue.push(&nodes, p, bound);
        }
    }
    Ok(false)
}

/// True if `ancestor` lies on the first-parent chain of `descendant`
/// (inclusive). Unrecorded commits are simply not ancestors. Reads the
/// chain itself (in chunks bounded by the generation gap) when the gap is
/// at most [`FIRST_WINDOW`], else descends the skip index (O(log n) reads).
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
    if desc.generation - target.generation > FIRST_WINDOW
        && let Some(found) = skip::on_first_parent_chain(b, &target, descendant).await?
    {
        return Ok(found);
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
/// the larger id. `None` for unrelated histories. When one is more than
/// [`FIRST_WINDOW`] generations older and on the other's first-parent
/// chain (skip index, O(log n) reads), it is the answer. Otherwise paints
/// both sides' ancestry walking in generation order, reading the walk's
/// ancestry a doubling window of generations per store call
/// ([`Queue::pop_above`], the first down to just below the older side): the
/// walk visits the commits above the answer's generation, at about one
/// store call per doubling of its depth below the older side.
/// Unrecorded commits are `InvalidArgument`.
pub async fn merge_base(b: &Backend, x: ObjectId, y: ObjectId) -> Result<Option<ObjectId>> {
    const X: u8 = 1;
    const Y: u8 = 2;
    let mut nodes = Nodes::new(b, FIRST_CHAIN_READ);
    nodes.load_exact(&[x, y]).await?;
    if x == y {
        return Ok(Some(x));
    }
    let gx = nodes.get(&x)?.generation;
    let gy = nodes.get(&y)?.generation;
    // An ancestor of the other is the answer (no other common ancestor
    // has its generation); a first-parent one is found without a walk.
    let (old, new) = if gx < gy { (x, y) } else { (y, x) };
    if gx.abs_diff(gy) > FIRST_WINDOW
        && skip::on_first_parent_chain(b, nodes.get(&old)?, new).await? == Some(true)
    {
        return Ok(Some(old));
    }
    let mut paint: HashMap<ObjectId, u8> = HashMap::from([(x, X), (y, Y)]);
    // The walk pops everything from the newer side down to the older one's
    // generation before it can find a common ancestor: the first read
    // covers that and [`FIRST_WINDOW`] below.
    let mut queue = Queue::new(gx.abs_diff(gy).saturating_add(FIRST_WINDOW));
    queue.push(&nodes, x, gx);
    queue.push(&nodes, y, gy);
    while let Some(id) = queue.pop_above(&mut nodes, 0).await? {
        let flags = paint[&id];
        if flags == X | Y {
            return Ok(Some(id));
        }
        // Every descendant of a commit (higher generation) is popped before
        // it, so its paint is final here: the first commit painted by both
        // sides is a common ancestor of the highest generation.
        let parents = nodes.get(&id)?.parents.clone();
        let bound = nodes.parent_bound(&id)?;
        for p in parents {
            *paint.entry(p).or_default() |= flags;
            queue.push(&nodes, p, bound);
        }
    }
    Ok(None)
}

/// `id`'s `n`-th first-parent ancestor (`id~n`; `None` when its chain is
/// shorter). Up to [`FIRST_WINDOW`] steps read the chain (one store call);
/// more descend the skip index (O(log n) reads), or read the chain in
/// chunks when it cannot be indexed. An unrecorded `id` is
/// `InvalidArgument`.
pub async fn first_parent_ancestor(
    b: &Backend,
    id: ObjectId,
    n: usize,
) -> Result<Option<ObjectId>> {
    if n as u64 > FIRST_WINDOW
        && let Some(found) = skip::nth_first_parent(b, id, n as u64).await?
    {
        return Ok(found);
    }
    let (mut from, mut left) = (id, n);
    loop {
        let want = left.saturating_add(1).min(MAX_CHAIN_READ);
        let chain = b.meta.first_parent_chain(from, None, want).await?;
        if chain.is_empty() && from == id {
            return Err(not_recorded(id));
        }
        if let Some(n) = chain.get(left) {
            return Ok(Some(n.id));
        }
        match chain.last().and_then(|n| n.parents.first()) {
            Some(p) if chain.len() == want => {
                left -= chain.len();
                from = *p;
            }
            _ => return Ok(None),
        }
    }
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
    let mut queue = Queue::new(FIRST_WINDOW);
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
        let obj = EncodedObject::new(ObjectKind::Blob, content);
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

    /// Many changed directories cost one batched read per depth, and only
    /// the differing trees are read (each distinct tree once).
    #[tokio::test]
    async fn diff_reads_one_batch_per_depth() {
        let o = crate::testing::CountingStore::default();
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
        o.reset();

        let d = diff_trees(&o, Some(old_t), Some(new_t)).await.unwrap();
        // root, agents, shards, agent dirs (`p` adds one level below root).
        assert_eq!(o.calls(), 4);
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
        assert_eq!(o.ids(), 2 + 2 + 32 + 200 + 1 + 1 + 1 + 1);

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

    /// Counts the graph reads (`calls`) and the derived-data calls
    /// (`derived_calls`: the skip index) of a `MemoryMetaStore` (every
    /// method forwarded).
    #[derive(Default)]
    struct CountingMeta {
        inner: crate::memory::MemoryMetaStore,
        get_commits: std::sync::atomic::AtomicUsize,
        chains: std::sync::atomic::AtomicUsize,
        ancestry: std::sync::atomic::AtomicUsize,
        derived: std::sync::atomic::AtomicUsize,
        /// Commits returned by graph reads.
        nodes: std::sync::atomic::AtomicUsize,
    }

    impl CountingMeta {
        fn calls(&self) -> usize {
            use std::sync::atomic::Ordering::Relaxed;
            self.get_commits.load(Relaxed) + self.chains.load(Relaxed) + self.ancestry.load(Relaxed)
        }
        fn derived_calls(&self) -> usize {
            self.derived.load(std::sync::atomic::Ordering::Relaxed)
        }
        fn nodes(&self) -> usize {
            self.nodes.load(std::sync::atomic::Ordering::Relaxed)
        }
        fn reset(&self) {
            use std::sync::atomic::Ordering::Relaxed;
            self.get_commits.store(0, Relaxed);
            self.chains.store(0, Relaxed);
            self.ancestry.store(0, Relaxed);
            self.derived.store(0, Relaxed);
            self.nodes.store(0, Relaxed);
        }
        fn count<T>(&self, counter: &std::sync::atomic::AtomicUsize, nodes: &[T]) {
            use std::sync::atomic::Ordering::Relaxed;
            counter.fetch_add(1, Relaxed);
            self.nodes.fetch_add(nodes.len(), Relaxed);
        }
    }

    #[async_trait]
    impl crate::MetaStore for CountingMeta {
        async fn put_commit(&self, node: CommitNode, changes: Vec<PathChange>) -> Result<u64> {
            self.inner.put_commit(node, changes).await
        }
        async fn get_commits(&self, ids: &[ObjectId]) -> Result<Vec<Option<CommitNode>>> {
            let out = self.inner.get_commits(ids).await?;
            self.count(&self.get_commits, &out);
            Ok(out)
        }
        async fn ancestors_above(
            &self,
            tips: &[ObjectId],
            min_generation: u64,
            limit: usize,
        ) -> Result<Vec<CommitNode>> {
            let out = self.inner.ancestors_above(tips, min_generation, limit).await?;
            self.count(&self.ancestry, &out);
            Ok(out)
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
            let out = self.inner.first_parent_chain(tip, stop, limit).await?;
            self.count(&self.chains, &out);
            Ok(out)
        }
        async fn tail(&self, after: u64, limit: usize) -> Result<Vec<(u64, ObjectId)>> {
            self.inner.tail(after, limit).await
        }
        async fn get_derived(&self, kind: &str, key: &[u8]) -> Result<Option<bytes::Bytes>> {
            self.derived.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.get_derived(kind, key).await
        }
        async fn put_derived(&self, kind: &str, key: &[u8], value: bytes::Bytes) -> Result<()> {
            self.derived.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.put_derived(kind, key, value).await
        }
        async fn get_derived_many(
            &self,
            kind: &str,
            keys: &[&[u8]],
        ) -> Result<Vec<Option<bytes::Bytes>>> {
            self.derived.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.get_derived_many(kind, keys).await
        }
        async fn put_derived_many(
            &self,
            kind: &str,
            entries: Vec<(Vec<u8>, bytes::Bytes)>,
        ) -> Result<()> {
            self.derived.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.put_derived_many(kind, entries).await
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

    /// A merge-heavy history shaped like rust-lang/rust's: every
    /// first-parent commit merges a short topic branch based a few commits
    /// back (bors), some topics are merges themselves (rollups), and a few
    /// are long-lived (based far back). Returns the first-parent chain,
    /// oldest first, and every topic commit with the main index it was
    /// merged at.
    async fn merge_heavy(
        d: &mut Dag,
        merges: usize,
        mut seed: u64,
    ) -> (Vec<ObjectId>, Vec<(ObjectId, usize)>) {
        let mut next = move |m: usize| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            ((seed >> 33) as usize) % m
        };
        let mut main = vec![d.add(&[]).await];
        let mut topics = Vec::new();
        for i in 0..merges {
            let back = if next(64) == 0 { 500 + next(2000) } else { next(8) };
            let base = main[main.len() - 1 - back.min(main.len() - 1)];
            let topic = d.chain(Some(base), 1 + next(4)).await;
            let mut tip = topic[topic.len() - 1];
            topics.extend(topic.iter().map(|c| (*c, i + 1)));
            if next(10) == 0 {
                let other = d.chain(Some(base), 1 + next(3)).await;
                topics.extend(other.iter().map(|c| (*c, i + 1)));
                tip = d.add(&[tip, other[other.len() - 1]]).await;
            }
            let m = d.add(&[main[main.len() - 1], tip]).await;
            main.push(m);
        }
        (main, topics)
    }

    /// Run `f` and report its store calls (graph reads, derived calls,
    /// commits read) and time.
    async fn measured<T>(
        d: &Dag,
        what: &str,
        f: impl std::future::Future<Output = Result<T>>,
    ) -> (T, usize, usize, usize) {
        d.meta.reset();
        let start = std::time::Instant::now();
        let out = f.await.unwrap();
        let (calls, derived, nodes) = (d.meta.calls(), d.meta.derived_calls(), d.meta.nodes());
        eprintln!(
            "{what}: {:?}, {calls} graph reads ({nodes} commits), {derived} skip-index calls",
            start.elapsed()
        );
        (out, calls, derived, nodes)
    }

    /// Merge-heavy history (~115k commits): the queries cost a few store
    /// calls however many merges they pass, and first-parent questions
    /// (indexed) read no commits beyond their arguments.
    #[tokio::test]
    async fn ancestry_over_merge_heavy_histories_is_bounded() {
        let mut d = Dag::new();
        // ANT_GRAPH_MERGES=90000 (~345k commits) measures at rust-lang/rust size.
        let merges = std::env::var("ANT_GRAPH_MERGES").ok().and_then(|v| v.parse().ok());
        let (main, topics) = merge_heavy(&mut d, merges.unwrap_or(30_000).max(30_000), 1).await;
        let b = d.b.clone();
        let tip = main[main.len() - 1];
        let at = |back: usize| main[main.len() - 1 - back];
        let total = d.next as usize;
        assert!(total > 100_000, "{total} commits");

        // Cold skip index: the first query fills it in along main (the
        // arguments, then chain reads of 64, 2048 and 32768 commits), then
        // descends it.
        let fill_reads = 2 + main.len().div_ceil(32_768);
        let (mb, calls, derived, _) =
            measured(&d, "merge_base(main, main~2000) cold", merge_base(&b, tip, at(2000))).await;
        assert_eq!(mb, Some(at(2000)));
        assert!(calls <= 1 + fill_reads, "{calls}");
        assert!(derived <= 80, "{derived}");

        // Indexed: no walk at all.
        for back in [2000, 25_000, 29_999] {
            let (mb, calls, derived, _) = measured(
                &d,
                &format!("merge_base(main, main~{back})"),
                merge_base(&b, tip, at(back)),
            )
            .await;
            assert_eq!(mb, Some(at(back)));
            assert_eq!(calls, 1);
            assert!(derived <= 64, "{derived}");
            let (yes, calls, derived, _) = measured(
                &d,
                &format!("is_ancestor(main~{back}, main)"),
                is_ancestor(&b, at(back), tip),
            )
            .await;
            assert!(yes);
            assert_eq!(calls, 1);
            assert!(derived <= 64, "{derived}");
            let (yes, calls, derived, _) = measured(
                &d,
                &format!("is_first_parent_ancestor(main~{back}, main)"),
                is_first_parent_ancestor(&b, at(back), tip),
            )
            .await;
            assert!(yes);
            assert_eq!(calls, 1);
            assert!(derived <= 64, "{derived}");
        }

        // A topic commit merged ~25000 merges ago: reachable only through a
        // second parent, so the walk reads everything above it, in a few
        // calls.
        let (old_topic, merged_at) =
            topics.iter().copied().find(|(_, i)| *i == main.len() - 1 - 25_000).unwrap();
        assert_eq!(merged_at, main.len() - 1 - 25_000);
        let (yes, calls, _, _) =
            measured(&d, "is_ancestor(topic of main~25000, main)", is_ancestor(&b, old_topic, tip))
                .await;
        assert!(yes);
        assert!(calls <= 6, "{calls}");
        assert!(!is_first_parent_ancestor(&b, old_topic, tip).await.unwrap());

        // Not merged: a branch off main~2000.
        let feature = d.chain(Some(at(2000)), 3).await;
        let (no, calls, _, _) = measured(
            &d,
            "is_ancestor(feature off main~2000, main)",
            is_ancestor(&b, feature[0], tip),
        )
        .await;
        assert!(!no);
        assert!(calls <= 4, "{calls}");
        let (mb, calls, _, nodes) = measured(
            &d,
            "merge_base(feature off main~2000, main)",
            merge_base(&b, feature[2], tip),
        )
        .await;
        assert_eq!(mb, Some(at(2000)));
        assert!(calls <= 4, "{calls}");
        assert!(nodes < 20 * 2000, "{nodes}");
        // ... and its first-parent range.
        let (end, calls, _, _) = measured(
            &d,
            "first_parent_range_end(feature, main)",
            first_parent_range_end(&b, feature[2], tip),
        )
        .await;
        assert_eq!(end, Some(at(2000)));
        assert!(calls <= 8, "{calls}");

        // Freshly landed commits on the indexed chain: filled in with one
        // short read.
        let landed = d.add(&[tip, feature[2]]).await;
        let (mb, calls, derived, _) =
            measured(&d, "merge_base(landed, main~2000)", merge_base(&b, landed, at(2000))).await;
        assert_eq!(mb, Some(at(2000)));
        assert!(calls <= 2, "{calls}");
        assert!(derived <= 70, "{derived}");
    }

    /// The skip index agrees with walking first parents, whatever was
    /// filled in before (partial fills, chains that cannot be indexed, several
    /// roots), its entries are consistent, and a descent costs O(log n)
    /// reads.
    #[tokio::test]
    async fn skip_index_matches_first_parent_chains() {
        let mut d = Dag::new();
        let mut seed = 7u64;
        let mut next = move |m: usize| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            ((seed >> 33) as usize) % m
        };
        // Node i's parents, as indices.
        let mut parents: Vec<Vec<usize>> = Vec::new();
        let mut ids: Vec<ObjectId> = Vec::new();
        for i in 0..3000 {
            let ps: Vec<usize> = match i {
                0 | 1500 => vec![], // two roots
                _ if next(20) == 0 => vec![next(i)],
                _ if next(3) == 0 => vec![i - 1, next(i)],
                _ => vec![i - 1],
            };
            let mut ps = ps;
            ps.dedup();
            let pid: Vec<ObjectId> = ps.iter().map(|&p| ids[p]).collect();
            ids.push(d.add(&pid).await);
            parents.push(ps);
        }
        let b = d.b.clone();
        let chain = |mut i: usize| {
            let mut out = vec![i];
            while let Some(&p) = parents[i].first() {
                out.push(p);
                i = p;
            }
            out
        };
        // A chain reaching an unrecorded commit cannot be indexed, and
        // nothing is written.
        let ghost = ObjectId([9; 32]);
        let orphan = CommitNode {
            id: ObjectId([8; 32]),
            tree: ObjectId([0; 32]),
            parents: vec![ghost],
            generation: 5,
            committer_time: 0,
        };
        b.meta.put_commit(orphan.clone(), vec![]).await.unwrap();
        let on_orphan = d.add(&[orphan.id]).await;
        assert_eq!(skip::entry(&b, on_orphan).await.unwrap(), None);
        assert_eq!(skip::entry(&b, ghost).await.unwrap(), None);
        assert_eq!(b.meta.get_derived(SKIP_KIND, on_orphan.as_bytes()).await.unwrap(), None);
        assert_eq!(skip::nth_first_parent(&b, on_orphan, 1).await.unwrap(), None);
        // Partial fills first.
        for i in [10, 700, 701, 1499, 1600, 2400] {
            assert!(skip::entry(&b, ids[i]).await.unwrap().is_some());
        }
        // Just above an indexed commit.
        assert!(skip::entry(&b, ids[2401]).await.unwrap().is_some());
        let nodes = b.meta.get_commits(&ids).await.unwrap();
        for (i, n) in nodes.iter().enumerate().step_by(7) {
            let n = n.as_ref().unwrap();
            let e = skip::entry(&b, ids[i]).await.unwrap().unwrap();
            let c = chain(i);
            assert_eq!(e.depth as usize, c.len() - 1);
            assert_eq!(e.generation, n.generation);
            assert_eq!(e.parent, parents[i].first().map(|&p| ids[p]));
            let j = c[c.len() - 1 - e.jump_depth as usize];
            assert_eq!(e.jump, ids[j]);
            assert_eq!(e.jump_generation, nodes[j].as_ref().unwrap().generation);
            assert!(e.jump_depth < e.depth || e.depth == 0);
        }
        let mut max_reads = 0;
        for _ in 0..3000 {
            let (a, x) = (next(3000), next(3000));
            // Mostly pairs on one chain, some not.
            let a = if next(4) == 0 { a } else { *chain(x).get(next(chain(x).len())).unwrap() };
            let target = nodes[a].as_ref().unwrap();
            d.meta.reset();
            let got = skip::on_first_parent_chain(&b, target, ids[x]).await.unwrap();
            assert_eq!(got, Some(chain(x).contains(&a)), "{a} on the chain of {x}");
            max_reads = max_reads.max(d.meta.derived_calls());
            let c = chain(x);
            let n = next(c.len() + 2);
            d.meta.reset();
            let got = skip::nth_first_parent(&b, ids[x], n as u64).await.unwrap();
            assert_eq!(got, Some(c.get(n).map(|&i| ids[i])), "{x}~{n}");
            max_reads = max_reads.max(d.meta.derived_calls());
            let got = first_parent_ancestor(&b, ids[x], n).await.unwrap();
            assert_eq!(got, c.get(n).map(|&i| ids[i]), "first_parent_ancestor {x}~{n}");
            let short = n % 300;
            let got = first_parent_ancestor(&b, on_orphan, short).await.unwrap();
            assert_eq!(got, [on_orphan, orphan.id].get(short).copied(), "orphan~{short}");
        }
        for n in [3, 1000] {
            let got = first_parent_ancestor(&b, ghost, n).await;
            assert!(matches!(got, Err(StoreError::InvalidArgument(_))), "{got:?}");
        }
        // log2(3000) ~ 11.6.
        assert!(max_reads <= 40, "{max_reads} reads for one descent");
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
