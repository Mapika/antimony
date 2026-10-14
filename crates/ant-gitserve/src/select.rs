//! Reachability planning retains IDs, not object contents. Tree entries let us
//! include/exclude blobs without downloading their bodies during planning.
use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
};

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind};
use ant_store::{Backend, CommitNode};
use anyhow::{Result, anyhow, bail, ensure};

/// Ids per store call when reading commit metadata and checking `have`s.
pub const BATCH: usize = 512;
/// Trees per read, and reads in flight, while planning.
const TREE_BATCH: usize = 256;
const TREE_READS: usize = 8;
/// Path fingerprint of commits and directly wanted objects (sorted first).
const NO_PATH: u64 = 0;
/// Path fingerprint of root trees (children hash their names into it).
const ROOT_PATH: u64 = 1;

/// One object of the pack plan: its id, a fingerprint of the path it was
/// first reached at, and the uncompressed size when planning read the body
/// (trees and directly wanted blobs; 0 for commits and blobs reached through
/// trees, which planning never reads).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Planned {
    pub id: ObjectId,
    pub path: u64,
    pub size: u32,
    pub blob: bool,
}

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
}

pub struct Selection {
    pub objects: Vec<Planned>,
    pub shallow: Vec<ObjectId>,
    pub unshallow: Vec<ObjectId>,
}

#[cfg(test)]
impl Selection {
    pub fn ids(&self) -> Vec<ObjectId> {
        self.objects.iter().map(|p| p.id).collect()
    }
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
fn entries(mut body: &[u8]) -> impl Iterator<Item = Result<(EntryMode, &[u8], ObjectId)>> {
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

/// Breadth-first tree walk state; moved to the blocking pool per batch.
struct Walk {
    excluded: HashSet<ObjectId>,
    found: HashSet<ObjectId>,
    ordered: Vec<Planned>,
    queue: VecDeque<(ObjectId, u64)>,
    no_blobs: bool,
}

impl Walk {
    fn visit(
        &mut self,
        batch: Vec<(ObjectId, u64)>,
        objects: Vec<Option<EncodedObject>>,
    ) -> Result<()> {
        ensure!(objects.len() == batch.len(), "incomplete tree batch");
        for ((id, path), object) in batch.into_iter().zip(objects) {
            let object = object.ok_or_else(|| anyhow!("missing tree {id}"))?;
            ensure!(object.kind() == ObjectKind::Tree, "expected tree {id}");
            let size = u32::try_from(object.body().len()).unwrap_or(u32::MAX);
            self.ordered.push(Planned { id, path, size, blob: false });
            for entry in entries(object.body()) {
                let (mode, name, id) = entry?;
                if !(mode == EntryMode::Tree || mode.is_blob() && !self.no_blobs) {
                    continue;
                }
                // Mark on enqueue, not dequeue: shared subtrees must not
                // accumulate millions of duplicate pending references.
                if self.excluded.contains(&id) || !self.found.insert(id) {
                    continue;
                }
                let mut hash = DefaultHasher::new();
                path.hash(&mut hash);
                name.hash(&mut hash);
                let path = hash.finish();
                if mode == EntryMode::Tree {
                    self.queue.push_back((id, path));
                } else {
                    self.ordered.push(Planned { id, path, size: 0, blob: true });
                }
            }
        }
        Ok(())
    }
}

/// Aborts a spawned read when planning stops early (error or disconnect).
struct Read(tokio::task::JoinHandle<Result<TreeBatch>>);

type TreeBatch = (Vec<(ObjectId, u64)>, Vec<Option<EncodedObject>>);

impl Drop for Read {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Expand each distinct tree once, recording compact path fingerprints rather
/// than allocating full paths for millions of historical objects. Collisions
/// only affect compression: the encoder checks kinds and encodes actual bytes.
/// Store reads are pipelined: `TREE_READS` batches stay in flight while the
/// previous one is parsed on the blocking pool.
async fn trees(
    b: &Backend,
    roots: Vec<ObjectId>,
    excluded: HashSet<ObjectId>,
    no_blobs: bool,
) -> Result<Vec<Planned>> {
    let mut walk = Walk {
        excluded,
        found: HashSet::new(),
        ordered: Vec::new(),
        queue: VecDeque::new(),
        no_blobs,
    };
    for id in roots {
        if !walk.excluded.contains(&id) && walk.found.insert(id) {
            walk.queue.push_back((id, ROOT_PATH));
        }
    }
    let mut reads = VecDeque::new();
    loop {
        while reads.len() < TREE_READS && !walk.queue.is_empty() {
            let batch: Vec<_> = walk.queue.drain(..walk.queue.len().min(TREE_BATCH)).collect();
            let b = b.clone();
            reads.push_back(Read(tokio::spawn(async move {
                let ids: Vec<_> = batch.iter().map(|(id, _)| *id).collect();
                let objects = b.objects.get_many(&ids).await?;
                Ok((batch, objects))
            })));
        }
        let Some(mut read) = reads.pop_front() else { break };
        let (batch, objects) = (&mut read.0).await??;
        // Parsing large trees is CPU work, too. Do not stall HTTP executors.
        walk = tokio::task::spawn_blocking(move || -> Result<Walk> {
            walk.visit(batch, objects)?;
            Ok(walk)
        })
        .await??;
    }
    Ok(walk.ordered)
}

/// Plan the pack for `fetch`: walk the commits from the wants (level by
/// level, `BATCH` at a time, up to `depth` for shallow fetches, recording the
/// new shallow and unshallow boundaries), then every tree and blob (unless
/// `blob:none`) reachable from their root trees and not from the client's
/// `have`s, so the client receives exactly what it lacks. Objects come out
/// grouped by path fingerprint, largest first within a path, for the delta
/// search.
pub async fn select(b: &Backend, fetch: &Fetch) -> Result<Selection> {
    let mut known = Known::default();
    known.add(b, &fetch.haves).await?;
    let mut selected = HashSet::new();
    let mut direct_blobs = HashMap::new();
    let mut roots = Vec::new();
    let mut shallow = Vec::new();
    let mut unshallow = Vec::new();
    let mut seen = HashSet::new();
    let mut frontier = fetch.wants.clone();
    let mut level = 1;
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for chunk in frontier.chunks(BATCH) {
            let metadata = b.meta.get_commits(chunk).await?;
            if let Some(generation) = metadata.iter().flatten().map(|node| node.generation).min() {
                known.advance_to(b, generation, &fetch.shallow).await?;
            }
            let mut direct = Vec::new();
            for (id, node) in chunk.iter().zip(metadata) {
                if !seen.insert(*id) {
                    continue;
                }
                let Some(node) = node else {
                    direct.push(*id);
                    continue;
                };
                if known.ids.contains(id) && !(fetch.depth.is_some() && fetch.shallow.contains(id))
                {
                    continue;
                }
                selected.insert(*id);
                roots.push(node.tree);
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
            if !direct.is_empty() {
                for (id, object) in direct.iter().zip(b.objects.get_many(&direct).await?) {
                    let object = object.ok_or_else(|| anyhow!("unknown want {id}"))?;
                    match object.kind() {
                        ObjectKind::Blob => {
                            selected.insert(*id);
                            let size = u32::try_from(object.body().len()).unwrap_or(u32::MAX);
                            direct_blobs.insert(*id, size);
                        }
                        ObjectKind::Tree => roots.push(*id),
                        ObjectKind::Commit => bail!("commit {id} has no graph metadata"),
                    }
                }
            }
        }
        frontier = next;
        level += 1;
    }
    drop(seen);
    let mut entries: Vec<_> = selected
        .iter()
        .map(|id| match direct_blobs.get(id) {
            Some(size) => Planned { id: *id, path: NO_PATH, size: *size, blob: true },
            None => Planned { id: *id, path: NO_PATH, size: 0, blob: false },
        })
        .collect();
    drop(direct_blobs);
    if !roots.is_empty() {
        // Exact subtraction includes historical trees, including restored files.
        known.advance_to(b, 0, &fetch.shallow).await?;
        let known_roots = std::mem::take(&mut known.roots);
        drop(known);
        let known_objects = trees(b, known_roots, HashSet::new(), fetch.no_blobs)
            .await?
            .into_iter()
            .map(|planned| planned.id)
            .collect();
        entries.extend(
            trees(b, roots, known_objects, fetch.no_blobs)
                .await?
                .into_iter()
                .filter(|planned| !selected.contains(&planned.id)),
        );
    }
    drop(selected);
    // Group by path, largest first within a path (as git's own packer does):
    // deltas then mostly delete from a bigger, usually newer base, and chains
    // fan out instead of growing one step per revision. The stable sort keeps
    // traversal order (newest first) among equal sizes.
    let objects = tokio::task::spawn_blocking(move || {
        entries.sort_by_key(|planned| (planned.path, Reverse(planned.size)));
        entries
    })
    .await?;
    Ok(Selection { objects, shallow, unshallow })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_core::{Commit, EncodedObject, Signature};
    use ant_store::{
        ObjectStoreExt,
        graph::{build_tree, write_commit},
    };

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
        let selection = select(
            &b,
            &Fetch { wants: vec![right], haves: vec![tip], done: true, ..Fetch::default() },
        )
        .await
        .unwrap();
        assert_eq!(selection.ids().len(), 3);
        assert!(selection.ids().contains(&right));
        assert!(!selection.ids().contains(&root));
        let empty = select(
            &b,
            &Fetch { wants: vec![left], haves: vec![tip], done: true, ..Fetch::default() },
        )
        .await
        .unwrap();
        assert!(empty.objects.is_empty());
    }

    #[tokio::test]
    async fn reintroduced_blobs_and_direct_blob_wants() {
        let b = Backend::in_memory();
        let root = commit(&b, vec![], b"old", 1).await;
        let middle = commit(&b, vec![root], b"new", 2).await;
        let tip = commit(&b, vec![middle], b"old", 3).await;
        let selection = select(
            &b,
            &Fetch { wants: vec![tip], haves: vec![middle], done: true, ..Fetch::default() },
        )
        .await
        .unwrap();
        assert_eq!(selection.ids(), [tip], "old tree and blob already belong to the client");
        let blob = b.objects.put(EncodedObject::new(ObjectKind::Blob, b"lazy")).await.unwrap();
        let selection = select(
            &b,
            &Fetch { wants: vec![blob, blob], no_blobs: true, done: true, ..Fetch::default() },
        )
        .await
        .unwrap();
        assert_eq!(selection.ids(), [blob], "explicit wants override blob filtering");
    }

    #[tokio::test]
    async fn shallow_have_does_not_imply_its_ancestors() {
        let b = Backend::in_memory();
        let root = commit(&b, vec![], b"old", 1).await;
        let tip = commit(&b, vec![root], b"new", 2).await;
        let selection = select(
            &b,
            &Fetch {
                wants: vec![root],
                haves: vec![tip],
                shallow: [tip].into(),
                done: true,
                ..Fetch::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(selection.ids().len(), 3);
        assert!(selection.ids().contains(&root));
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
            select(&b, &Fetch { wants: vec![tip, blob], done: true, ..Default::default() })
                .await
                .unwrap();
        assert_eq!(selection.ids().len(), 3);
        assert_eq!(selection.ids().iter().filter(|id| **id == blob).count(), 1);
        assert_eq!(selection.objects.iter().filter(|p| p.blob).count(), 1);
    }

    #[test]
    fn invalid_fetch_arguments() {
        for args in [vec!["want bad"], vec!["deepen 0"], vec!["filter blob:limit=10"], vec!["done"]]
        {
            assert!(Fetch::parse(&args).is_err());
        }
    }
}
