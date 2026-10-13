//! Reachability planning retains IDs, not object contents. Tree entries let us
//! include/exclude blobs without downloading their bodies during planning.
use std::collections::{BTreeMap, HashSet, VecDeque};

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
            } else if *line == "done" {
                fetch.done = true;
            } else if !matches!(
                *line,
                "thin-pack" | "ofs-delta" | "no-progress" | "include-tag" | "wait-for-done"
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

/// Expand tree IDs in bounded batches. Blob bodies and gitlinks are never read.
async fn trees(
    b: &Backend,
    roots: Vec<ObjectId>,
    excluded: &HashSet<ObjectId>,
    no_blobs: bool,
) -> Result<HashSet<ObjectId>> {
    let mut found = HashSet::new();
    let mut queue: VecDeque<_> = roots.into();
    while !queue.is_empty() {
        let mut batch = Vec::new();
        while batch.len() < BATCH {
            let Some(id) = queue.pop_front() else { break };
            if !excluded.contains(&id) && found.insert(id) {
                batch.push(id);
            }
        }
        if batch.is_empty() {
            continue;
        }
        let objects = b.objects.get_many(&batch).await?;
        for (id, object) in batch.into_iter().zip(objects) {
            let object = object.ok_or_else(|| anyhow!("missing tree {id}"))?;
            ensure!(object.kind() == ObjectKind::Tree, "expected tree {id}");
            for entry in Tree::decode(object.body())?.entries() {
                if entry.mode == EntryMode::Tree {
                    queue.push_back(entry.id);
                } else if entry.mode.is_blob() && !no_blobs && !excluded.contains(&entry.id) {
                    found.insert(entry.id);
                }
            }
        }
    }
    Ok(found)
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
    if !roots.is_empty() {
        // Exact object subtraction must include historical trees: a new
        // commit can restore a blob absent from the client's current tip.
        known.advance_to(b, 0, &fetch.shallow).await?;
        let known_objects = trees(b, known.roots, &HashSet::new(), fetch.no_blobs).await?;
        selected.extend(trees(b, roots, &known_objects, fetch.no_blobs).await?);
    }
    let mut ids: Vec<_> = selected.into_iter().collect();
    ids.sort_unstable();
    Ok(Selection { ids, shallow, unshallow })
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
    fn invalid_fetch_arguments() {
        for args in [vec!["want bad"], vec!["deepen 0"], vec!["filter blob:limit=10"], vec!["done"]]
        {
            assert!(Fetch::parse(&args).is_err());
        }
    }
}
