//! Reachability planning retains IDs, not object contents. Tree entries let us
//! include/exclude blobs without downloading their bodies during planning.
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
};

use ant_core::{EntryMode, ObjectId, ObjectKind, Tree};
use ant_store::{Backend, CommitNode};
use anyhow::{Result, anyhow, bail, ensure};

pub const BATCH: usize = 512;

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
    pub ids: Vec<ObjectId>,
    pub paths: Vec<u64>,
    pub shallow: Vec<ObjectId>,
    pub unshallow: Vec<ObjectId>,
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

/// Expand each distinct tree once, recording compact path fingerprints rather
/// than allocating full paths for millions of historical objects. Collisions
/// only affect compression: the encoder checks kinds and encodes actual bytes.
async fn trees(
    b: &Backend,
    roots: Vec<ObjectId>,
    excluded: HashSet<ObjectId>,
    no_blobs: bool,
) -> Result<Vec<(ObjectId, u64)>> {
    let mut excluded = excluded;
    let mut found = HashSet::new();
    let mut ordered = Vec::new();
    let mut queue = VecDeque::new();
    for id in roots {
        if !excluded.contains(&id) && found.insert(id) {
            queue.push_back((id, 0));
        }
    }
    while !queue.is_empty() {
        let batch: Vec<_> = queue.drain(..queue.len().min(BATCH)).collect();
        let ids: Vec<_> = batch.iter().map(|(id, _)| *id).collect();
        let objects = b.objects.get_many(&ids).await?;
        ensure!(objects.len() == ids.len(), "incomplete tree batch");
        // Parsing large trees is CPU work, too. Do not stall HTTP executors.
        let (next_found, next_ordered, next_queue, next_excluded) =
            tokio::task::spawn_blocking(move || -> Result<_> {
                for ((id, path), object) in batch.into_iter().zip(objects) {
                    let object = object.ok_or_else(|| anyhow!("missing tree {id}"))?;
                    ensure!(object.kind() == ObjectKind::Tree, "expected tree {id}");
                    ordered.push((id, path));
                    for entry in Tree::decode(object.body())?.entries() {
                        if !(entry.mode == EntryMode::Tree || entry.mode.is_blob() && !no_blobs) {
                            continue;
                        }
                        // Mark on enqueue, not dequeue: shared subtrees must not
                        // accumulate millions of duplicate pending references.
                        if excluded.contains(&entry.id) || !found.insert(entry.id) {
                            continue;
                        }
                        let mut hash = DefaultHasher::new();
                        path.hash(&mut hash);
                        entry.name.hash(&mut hash);
                        let child_path = hash.finish();
                        if entry.mode == EntryMode::Tree {
                            queue.push_back((entry.id, child_path));
                        } else {
                            ordered.push((entry.id, child_path));
                        }
                    }
                }
                Ok((found, ordered, queue, excluded))
            })
            .await??;
        found = next_found;
        ordered = next_ordered;
        queue = next_queue;
        excluded = next_excluded;
    }
    Ok(ordered)
}

pub async fn select(b: &Backend, fetch: &Fetch) -> Result<Selection> {
    let mut known = Known::default();
    known.add(b, &fetch.haves).await?;
    let mut selected = HashSet::new();
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
    let mut entries: Vec<_> = selected.iter().map(|id| (*id, 0)).collect();
    if !roots.is_empty() {
        // Exact subtraction includes historical trees, including restored files.
        known.advance_to(b, 0, &fetch.shallow).await?;
        let known_roots = std::mem::take(&mut known.roots);
        drop(known);
        let known_objects = trees(b, known_roots, HashSet::new(), fetch.no_blobs)
            .await?
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        entries.extend(
            trees(b, roots, known_objects, fetch.no_blobs)
                .await?
                .into_iter()
                .filter(|(id, _)| !selected.contains(id)),
        );
    }
    drop(selected);
    // Stable grouping preserves traversal (newest to oldest) within each path.
    // This lets streaming retain just ONE previous body, not a per-path cache.
    let (ids, paths) = tokio::task::spawn_blocking(move || {
        entries.sort_by_key(|(_, path)| *path);
        entries.into_iter().unzip()
    })
    .await?;
    Ok(Selection { ids, paths, shallow, unshallow })
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
        assert_eq!(selection.ids.len(), 3);
        assert!(selection.ids.contains(&right));
        assert!(!selection.ids.contains(&root));
        let empty = select(
            &b,
            &Fetch { wants: vec![left], haves: vec![tip], done: true, ..Fetch::default() },
        )
        .await
        .unwrap();
        assert!(empty.ids.is_empty());
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
        assert_eq!(selection.ids, [tip], "old tree and blob already belong to the client");
        let blob = b.objects.put(EncodedObject::new(ObjectKind::Blob, b"lazy")).await.unwrap();
        let selection = select(
            &b,
            &Fetch { wants: vec![blob, blob], no_blobs: true, done: true, ..Fetch::default() },
        )
        .await
        .unwrap();
        assert_eq!(selection.ids, [blob], "explicit wants override blob filtering");
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
        assert_eq!(selection.ids.len(), 3);
        assert!(selection.ids.contains(&root));
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
        assert_eq!(selection.ids.len(), 3);
        assert_eq!(selection.ids.iter().filter(|id| **id == blob).count(), 1);
        assert_eq!(selection.ids.len(), selection.paths.len());
    }

    #[test]
    fn invalid_fetch_arguments() {
        for args in [vec!["want bad"], vec!["deepen 0"], vec!["filter blob:limit=10"], vec!["done"]]
        {
            assert!(Fetch::parse(&args).is_err());
        }
    }
}
