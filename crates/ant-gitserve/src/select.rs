//! Reachability planning retains IDs, not object contents. Tree entries let us
//! include/exclude blobs without downloading their bodies during planning.
use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashSet, VecDeque},
    sync::Arc,
};

use ant_core::{EntryMode, ObjectId, ObjectKind};
use ant_store::{Backend, CommitNode};
use anyhow::{Result, anyhow, bail, ensure};

use crate::cache::Base;

/// Ids per store call when reading commit metadata and checking `have`s.
pub const BATCH: usize = 512;
/// Trees per read, and reads in flight, while planning.
const TREE_BATCH: usize = 256;
const TREE_READS: usize = 8;

#[derive(Default)]
pub struct Fetch {
    pub wants: Vec<ObjectId>,
    pub haves: Vec<ObjectId>,
    pub shallow: HashSet<ObjectId>,
    pub depth: Option<usize>,
    pub no_blobs: bool,
    pub done: bool,
    pub ofs_delta: bool,
}

impl Fetch {
    pub fn parse(args: &[&str]) -> Result<Self> {
        let mut fetch = Self::default();
        for line in args {
            if let Some(id) = line.strip_prefix("want ") {
                fetch.wants.push(id.parse()?);
            } else if let Some(id) = line.strip_prefix("have ") {
                fetch.haves.push(id.parse()?);
            } else if let Some(id) = line.strip_prefix("shallow ") {
                fetch.shallow.insert(id.parse()?);
            } else if let Some(depth) = line.strip_prefix("deepen ") {
                let depth = depth.parse()?;
                ensure!(depth > 0 && fetch.depth.is_none(), "invalid depth");
                fetch.depth = Some(depth);
            } else if *line == "filter blob:none" {
                fetch.no_blobs = true;
            } else if *line == "ofs-delta" {
                fetch.ofs_delta = true;
            } else if *line == "done" {
                fetch.done = true;
            } else if !matches!(
                *line,
                "thin-pack" | "no-progress" | "include-tag" | "wait-for-done"
            ) {
                bail!("unsupported fetch argument: {line}");
            }
        }
        ensure!(!fetch.wants.is_empty(), "missing wants");
        Ok(fetch)
    }

    /// A complete clone that can use, or become, a cached base pack (see
    /// `cache.rs`): nothing to subtract, no shallow boundary, deltas taken.
    pub fn is_clone(&self) -> bool {
        self.done
            && self.haves.is_empty()
            && self.shallow.is_empty()
            && self.depth.is_none()
            && self.ofs_delta
    }
}

/// Objects the client has: a set, and/or everything in a cached base pack.
#[derive(Clone, Default)]
pub struct Excluded {
    pub set: HashSet<ObjectId>,
    pub base: Option<Arc<Base>>,
}

impl Excluded {
    pub fn contains(&self, id: &ObjectId) -> bool {
        self.set.contains(id) || self.base.as_ref().is_some_and(|base| base.contains(id))
    }
}

/// What a fetch sends, before any tree is read: the export walk
/// (`walk.rs`) expands `roots` once, skipping `excluded` objects.
#[derive(Clone, Default)]
pub struct Selection {
    /// Selected commits, sent first.
    pub commits: Vec<ObjectId>,
    /// Wanted ids without commit metadata: blobs are sent as they are (even
    /// under `blob:none`), trees are walked like `roots`.
    pub direct: Vec<ObjectId>,
    /// Root trees of the selected commits (repeats allowed).
    pub roots: Vec<ObjectId>,
    /// Objects the client has (reachable from its `have`s), or that the
    /// cached base pack sent ahead of this selection already holds.
    pub excluded: Excluded,
    pub no_blobs: bool,
    pub shallow: Vec<ObjectId>,
    pub unshallow: Vec<ObjectId>,
    /// For a clone (`Fetch::is_clone`): the wanted commits that are not
    /// ancestors of other wanted commits. The pack is then exactly what
    /// they reach and can be cached as a base for them.
    pub tips: Vec<ObjectId>,
}

/// Lazily paint have ancestry in generation order. To test a candidate at
/// generation G, there is no reason to walk known commits below G: parents
/// always have strictly smaller generations. This also handles divergent
/// branches without mistaking equal generations for common ancestry.
#[derive(Default)]
struct Known {
    pending: BTreeMap<u64, Vec<CommitNode>>,
    ids: HashSet<ObjectId>,
    roots: Vec<ObjectId>,
}

impl Known {
    async fn add(&mut self, b: &Backend, ids: &[ObjectId]) -> Result<()> {
        for chunk in ids.chunks(BATCH) {
            for node in b.meta.get_commits(chunk).await?.into_iter().flatten() {
                if self.ids.insert(node.id) {
                    self.roots.push(node.tree);
                    self.pending.entry(node.generation).or_default().push(node);
                }
            }
        }
        Ok(())
    }

    async fn advance_to(
        &mut self,
        b: &Backend,
        generation: u64,
        shallow: &HashSet<ObjectId>,
    ) -> Result<()> {
        while self.pending.last_key_value().is_some_and(|(g, _)| *g > generation) {
            let mut parents = HashSet::new();
            let mut count = 0;
            while count < BATCH {
                let Some(mut entry) = self.pending.last_entry() else {
                    break;
                };
                if *entry.key() <= generation {
                    break;
                }
                let group = entry.get_mut();
                let take = (BATCH - count).min(group.len());
                for node in group.drain(group.len() - take..) {
                    count += 1;
                    if !shallow.contains(&node.id) {
                        parents
                            .extend(node.parents.into_iter().filter(|id| !self.ids.contains(id)));
                    }
                }
                if group.is_empty() {
                    entry.remove();
                }
            }
            self.add(b, &parents.into_iter().collect::<Vec<_>>()).await?;
        }
        Ok(())
    }
}

/// Entries of a canonical tree body without allocating names: (mode, name, id).
pub(crate) fn entries(
    mut body: &[u8],
) -> impl Iterator<Item = Result<(EntryMode, &[u8], ObjectId)>> {
    std::iter::from_fn(move || {
        if body.is_empty() {
            return None;
        }
        let entry = (|| {
            let space = body.iter().position(|&b| b == b' ').ok_or_else(|| anyhow!("bad tree"))?;
            let mode = EntryMode::from_octal(&body[..space])?;
            let rest = &body[space + 1..];
            let nul = rest.iter().position(|&b| b == 0).ok_or_else(|| anyhow!("bad tree"))?;
            ensure!(rest.len() >= nul + 1 + ObjectId::LEN, "truncated tree entry");
            let id = ObjectId::from_bytes(&rest[nul + 1..nul + 1 + ObjectId::LEN])?;
            body = &rest[nul + 1 + ObjectId::LEN..];
            Ok((mode, &rest[..nul], id))
        })();
        if entry.is_err() {
            body = &[];
        }
        Some(entry)
    })
}

/// Every tree (and blob, unless `no_blobs`) reachable from `roots`: the
/// objects a client already has. Ids only; nothing is retained per object but
/// the set. Store reads are pipelined: `TREE_READS` batches stay in flight
/// while the previous one is parsed on the blocking pool.
async fn reachable(b: &Backend, roots: Vec<ObjectId>, no_blobs: bool) -> Result<HashSet<ObjectId>> {
    let mut found: HashSet<ObjectId> = HashSet::new();
    let mut queue: VecDeque<ObjectId> = roots.into_iter().filter(|id| found.insert(*id)).collect();
    let mut reads = VecDeque::new();
    loop {
        while reads.len() < TREE_READS && !queue.is_empty() {
            let batch: Vec<_> = queue.drain(..queue.len().min(TREE_BATCH)).collect();
            let b = b.clone();
            reads.push_back(Read(tokio::spawn(async move {
                let objects = b.objects.get_many(&batch).await?;
                Ok((batch, objects))
            })));
        }
        let Some(mut read) = reads.pop_front() else { break };
        let (batch, objects) = (&mut read.0).await??;
        // Parsing large trees is CPU work, too. Do not stall HTTP executors.
        (found, queue) = tokio::task::spawn_blocking(move || -> Result<_> {
            ensure!(objects.len() == batch.len(), "incomplete tree batch");
            for (id, object) in batch.into_iter().zip(objects) {
                let object = object.ok_or_else(|| anyhow!("missing tree {id}"))?;
                ensure!(object.kind() == ObjectKind::Tree, "expected tree {id}");
                for entry in entries(object.body()) {
                    let (mode, _, id) = entry?;
                    let wanted = mode.is_tree() || mode.is_blob() && !no_blobs;
                    if wanted && found.insert(id) && mode.is_tree() {
                        queue.push_back(id);
                    }
                }
            }
            Ok((found, queue))
        })
        .await??;
    }
    Ok(found)
}

/// Aborts a spawned read when the walk stops early (error or disconnect).
pub(crate) struct Read<T>(pub tokio::task::JoinHandle<Result<T>>);

impl<T> Drop for Read<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Where a selected commit (and its root tree) goes in the pack: newest
/// first by generation, then by committer time, then by id.
///
/// The walk from the wants is level by level (distance from the wants), but
/// commits and root trees are read in this order instead. Consecutive root
/// trees are then close in history, which keeps the export walk's runs of
/// versions small (see `walk.rs`), and it is the order `ant admin repack`
/// stores commits (and every path's versions) in: reading in level order
/// jumps between the delta frames of a repacked store on merge-heavy
/// histories (topic branches are many levels deep but old) and decodes each
/// frame many times over (the Linux kernel: ~1.5M commits read 256 at a
/// time, a 64 MiB cache of decoded frames).
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Order {
    generation: Reverse<u64>,
    time: Reverse<i64>,
    id: ObjectId,
    tree: ObjectId,
}

impl Order {
    fn of(node: &CommitNode) -> Self {
        Self {
            generation: Reverse(node.generation),
            time: Reverse(node.committer_time),
            id: node.id,
            tree: node.tree,
        }
    }

    /// The commits and their root trees, in pack order.
    fn sorted(mut selected: Vec<Self>) -> (Vec<ObjectId>, Vec<ObjectId>) {
        selected.sort_unstable();
        selected.into_iter().map(|order| (order.id, order.tree)).unzip()
    }
}

/// Plan the pack for `fetch`: walk the commits from the wants (level by
/// level, `BATCH` at a time, up to `depth` for shallow fetches, recording the
/// new shallow and unshallow boundaries) and collect everything the client
/// already has (reachable from its `have`s), so that the export walk
/// (`walk.rs`) sends exactly what it lacks: the selected commits, the direct
/// wants, and every tree and blob (unless `blob:none`) reachable from the
/// selected root trees and not excluded. No object body but the client's
/// trees is read here.
pub async fn select(b: &Backend, fetch: &Fetch) -> Result<Selection> {
    if fetch.haves.is_empty() && fetch.shallow.is_empty() && fetch.depth.is_none() {
        return select_all(b, fetch).await;
    }
    let mut known = Known::default();
    known.add(b, &fetch.haves).await?;
    let mut selected = Vec::new();
    let mut direct = Vec::new();
    let mut shallow = Vec::new();
    let mut unshallow = Vec::new();
    let mut seen = HashSet::new();
    let mut frontier = fetch.wants.clone();
    let mut level = 1;
    // Clones note their tips: wanted commits no other commit reaches.
    let clone = fetch.is_clone();
    let wanted: HashSet<ObjectId> =
        if clone { fetch.wants.iter().copied().collect() } else { HashSet::new() };
    let mut tips = Vec::new();
    let mut parents = HashSet::new();
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for chunk in frontier.chunks(BATCH) {
            let metadata = b.meta.get_commits(chunk).await?;
            if let Some(generation) = metadata.iter().flatten().map(|node| node.generation).min() {
                known.advance_to(b, generation, &fetch.shallow).await?;
            }
            for (id, node) in chunk.iter().zip(metadata) {
                if !seen.insert(*id) {
                    continue;
                }
                let Some(node) = node else {
                    // A blob or tree (or an unknown id): classified by the
                    // export walk, which reads it anyway.
                    direct.push(*id);
                    continue;
                };
                if known.ids.contains(id) && !(fetch.depth.is_some() && fetch.shallow.contains(id))
                {
                    continue;
                }
                selected.push(Order::of(&node));
                if clone {
                    if wanted.contains(id) {
                        tips.push(*id);
                    }
                    parents.extend(node.parents.iter().copied());
                }
                if fetch.depth.is_some_and(|depth| level >= depth) {
                    if !node.parents.is_empty() {
                        shallow.push(*id);
                    }
                } else if fetch.depth.is_none() && fetch.shallow.contains(id) {
                    // Preserve existing client boundaries unless explicitly deepening.
                } else {
                    if fetch.shallow.contains(id) {
                        unshallow.push(*id);
                    }
                    next.extend(node.parents);
                }
            }
        }
        frontier = next;
        level += 1;
    }
    drop(seen);
    tips.retain(|id| !parents.contains(id));
    drop(parents);
    let mut excluded = HashSet::new();
    if !selected.is_empty() || !direct.is_empty() {
        // Exact subtraction includes historical trees, including restored files.
        known.advance_to(b, 0, &fetch.shallow).await?;
        let known_roots = std::mem::take(&mut known.roots);
        drop(known);
        excluded = reachable(b, known_roots, fetch.no_blobs).await?;
    }
    let (commits, roots) = Order::sorted(selected);
    Ok(Selection {
        commits,
        direct,
        roots,
        excluded: Excluded { set: excluded, base: None },
        no_blobs: fetch.no_blobs,
        shallow,
        unshallow,
        tips,
    })
}

/// [`select`] of a fetch with nothing to subtract and no depth (a clone, or a
/// fetch without `have`s): every commit the wants reach, read with one
/// ancestry query (`MetaStore::ancestors_above`, a single server-side
/// traversal in Postgres) instead of one store call per level of history
/// (the Linux kernel's history is ~1M levels deep, merge-heavy histories
/// average only a few commits per level).
async fn select_all(b: &Backend, fetch: &Fetch) -> Result<Selection> {
    let mut wanted = Vec::new();
    let mut direct = Vec::new();
    let mut seen = HashSet::new();
    for chunk in fetch.wants.chunks(BATCH) {
        for (id, node) in chunk.iter().zip(b.meta.get_commits(chunk).await?) {
            if !seen.insert(*id) {
                continue;
            }
            match node {
                Some(_) => wanted.push(*id),
                // A blob or tree (or an unknown id): classified by the
                // export walk, which reads it anyway.
                None => direct.push(*id),
            }
        }
    }
    drop(seen);
    let nodes = b.meta.ancestors_above(&wanted, 0, usize::MAX).await?;
    let mut tips = Vec::new();
    if fetch.is_clone() {
        let parents: HashSet<ObjectId> =
            nodes.iter().flat_map(|node| node.parents.iter().copied()).collect();
        tips = wanted.iter().copied().filter(|id| !parents.contains(id)).collect();
    }
    let (commits, roots) = Order::sorted(nodes.iter().map(Order::of).collect());
    drop(nodes);
    Ok(Selection {
        commits,
        direct,
        roots,
        excluded: Excluded::default(),
        no_blobs: fetch.no_blobs,
        tips,
        ..Default::default()
    })
}

/// Plan a clone on top of a cached `base`: the commits the wants reach
/// beyond it (the walk stops at commits in the base) and their trees, with
/// the base's objects excluded. `None` when the base does not fit: a want
/// is not a commit the store has, the wants do not reach every tip of the base (the pack
/// would carry objects the client did not ask for), or they reach more than
/// `limit` commits beyond it (a new base is due).
pub async fn select_after(
    b: &Backend,
    fetch: &Fetch,
    base: &Arc<Base>,
    limit: usize,
) -> Result<Option<Selection>> {
    // Every want must be a commit the store still has, also one the base
    // holds (the walk below does not read those): a base outlives garbage
    // collection (in memory, or persisted), and only what live wants reach
    // is live (`cache.rs`).
    for chunk in fetch.wants.chunks(BATCH) {
        if b.meta.get_commits(chunk).await?.iter().any(Option::is_none) {
            return Ok(None);
        }
    }
    let base_tips: HashSet<ObjectId> = base.tips.iter().copied().collect();
    let mut reached_tips = HashSet::new();
    let mut selected = Vec::new();
    let mut seen = HashSet::new();
    let mut frontier = fetch.wants.clone();
    while !frontier.is_empty() {
        let mut next = Vec::new();
        let mut new = Vec::new();
        for id in frontier {
            if !seen.insert(id) {
                continue;
            }
            if base.contains(&id) {
                // Tips form an antichain: a reachable tip is reached
                // before any other commit of the base below it.
                if base_tips.contains(&id) {
                    reached_tips.insert(id);
                }
            } else {
                new.push(id);
            }
        }
        for chunk in new.chunks(BATCH) {
            for node in b.meta.get_commits(chunk).await? {
                let Some(node) = node else { return Ok(None) };
                selected.push(Order::of(&node));
                next.extend(node.parents);
            }
        }
        if selected.len() > limit {
            return Ok(None);
        }
        frontier = next;
    }
    if reached_tips.len() != base_tips.len() {
        return Ok(None);
    }
    let (commits, roots) = Order::sorted(selected);
    Ok(Some(Selection {
        commits,
        roots,
        excluded: Excluded { set: HashSet::new(), base: Some(base.clone()) },
        no_blobs: fetch.no_blobs,
        ..Default::default()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_core::{Commit, EncodedObject, Signature};
    use ant_store::{
        ObjectStoreExt,
        graph::{build_tree, write_commit},
    };

    /// Everything the export walk sends for `fetch`, in pack order.
    async fn ids(b: &Backend, fetch: Fetch) -> Vec<ObjectId> {
        crate::walk::ids(b, select(b, &fetch).await.unwrap()).await.unwrap()
    }

    async fn commit(b: &Backend, parents: Vec<ObjectId>, content: &[u8], time: i64) -> ObjectId {
        let tree = build_tree(b.objects.as_ref(), &[("file", content)]).await.unwrap();
        let sig = Signature {
            name: "test".into(),
            email: "test@test".into(),
            time,
            tz_offset_minutes: 0,
        };
        write_commit(
            b,
            &Commit {
                tree,
                parents,
                author: sig.clone(),
                committer: sig,
                extra_headers: vec![],
                message: format!("{time}\n").into_bytes(),
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn generation_cutoff_and_divergent_history() {
        let b = Backend::in_memory();
        let root = commit(&b, vec![], b"root", 1).await;
        let left = commit(&b, vec![root], b"left", 2).await;
        let right = commit(&b, vec![root], b"right", 3).await;
        let tip = commit(&b, vec![left], b"tip", 4).await;
        let mut known = Known::default();
        known.add(&b, &[tip]).await.unwrap();
        known.advance_to(&b, 2, &HashSet::new()).await.unwrap();
        assert!(known.ids.contains(&left));
        assert!(!known.ids.contains(&root), "walk must stop at the generation floor");
        assert!(!known.ids.contains(&right), "equal generation is not equal ancestry");
        let selection =
            ids(&b, Fetch { wants: vec![right], haves: vec![tip], done: true, ..Fetch::default() })
                .await;
        assert_eq!(selection.len(), 3);
        assert!(selection.contains(&right));
        assert!(!selection.contains(&root));
        let empty =
            ids(&b, Fetch { wants: vec![left], haves: vec![tip], done: true, ..Fetch::default() })
                .await;
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn reintroduced_blobs_and_direct_blob_wants() {
        let b = Backend::in_memory();
        let root = commit(&b, vec![], b"old", 1).await;
        let middle = commit(&b, vec![root], b"new", 2).await;
        let tip = commit(&b, vec![middle], b"old", 3).await;
        let selection = ids(
            &b,
            Fetch { wants: vec![tip], haves: vec![middle], done: true, ..Fetch::default() },
        )
        .await;
        assert_eq!(selection, [tip], "old tree and blob already belong to the client");
        let blob = b.objects.put(EncodedObject::new(ObjectKind::Blob, b"lazy")).await.unwrap();
        let selection = ids(
            &b,
            Fetch { wants: vec![blob, blob], no_blobs: true, done: true, ..Fetch::default() },
        )
        .await;
        assert_eq!(selection, [blob], "explicit wants override blob filtering");
    }

    #[tokio::test]
    async fn shallow_have_does_not_imply_its_ancestors() {
        let b = Backend::in_memory();
        let root = commit(&b, vec![], b"old", 1).await;
        let tip = commit(&b, vec![root], b"new", 2).await;
        let selection = ids(
            &b,
            Fetch {
                wants: vec![root],
                haves: vec![tip],
                shallow: [tip].into(),
                done: true,
                ..Fetch::default()
            },
        )
        .await;
        assert_eq!(selection.len(), 3);
        assert!(selection.contains(&root));
    }

    #[test]
    fn delta_capability_is_explicit() {
        let want = format!("want {}", ObjectId::empty_tree());
        assert!(!Fetch::parse(&[&want, "thin-pack", "done"]).unwrap().ofs_delta);
        assert!(Fetch::parse(&[&want, "ofs-delta", "done"]).unwrap().ofs_delta);
    }

    #[tokio::test]
    async fn explicit_blob_and_reachable_blob_are_sent_once() {
        let b = Backend::in_memory();
        let tip = commit(&b, vec![], b"body", 1).await;
        let blob = EncodedObject::new(ObjectKind::Blob, b"body").id();
        let selection =
            ids(&b, Fetch { wants: vec![tip, blob], done: true, ..Default::default() }).await;
        assert_eq!(selection.len(), 3);
        assert_eq!(selection.iter().filter(|id| **id == blob).count(), 1);
        let kinds = b.objects.get_many(&selection).await.unwrap();
        assert_eq!(kinds.iter().flatten().filter(|o| o.kind() == ObjectKind::Blob).count(), 1);
    }

    #[test]
    fn invalid_fetch_arguments() {
        for args in [vec!["want bad"], vec!["deepen 0"], vec!["filter blob:limit=10"], vec!["done"]]
        {
            assert!(Fetch::parse(&args).is_err());
        }
    }
}
