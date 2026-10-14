//! The commit graph of one import: discovery, already-imported detection and
//! the parents-first conversion order, all in compact index-based arrays.

use std::collections::HashMap;

use ant_core::ObjectId;
use ant_store::Backend;
use anyhow::{Context as _, anyhow};

use crate::commit::Oid;
use crate::memo::{DigestMap, Sha1};
use crate::read::{Reader, oid};

/// Derived-data kind mapping a source commit id to its antimony commit id.
pub(crate) const COMMIT_KIND: &str = "git-import/commit/v1";
/// Commits per incremental pre-check query: starts small, doubles up to the cap.
const PRECHECK_START: usize = 64;
const PRECHECK_MAX: usize = 8192;

/// A commit already imported by an earlier run.
#[derive(Clone, Copy)]
pub(crate) struct Known {
    pub ant: ObjectId,
    pub generation: u64,
}

/// What discovery reads from each unknown commit.
struct Head {
    tree: Sha1,
    parents: Vec<Sha1>,
}

pub(crate) struct Graph {
    /// Source id of each commit; index 0 is the tip.
    pub ids: Vec<Sha1>,
    /// Source root tree of each commit read by discovery (zero for known ones).
    pub trees: Vec<Sha1>,
    par_off: Vec<u32>,
    par: Vec<u32>,
    /// Commits found already imported (their parents are never expanded).
    pub known: HashMap<u32, Known>,
    /// Commits still to convert, parents first.
    pub order: Vec<u32>,
}

impl Graph {
    pub fn parents(&self, idx: u32) -> &[u32] {
        let i = idx as usize;
        &self.par[self.par_off[i] as usize..self.par_off[i + 1] as usize]
    }

    /// Root tree of commit `idx`, reading the commit if discovery did not.
    pub fn tree_of(&self, idx: u32, reader: &mut Reader) -> anyhow::Result<Sha1> {
        if !self.known.contains_key(&idx) {
            return Ok(self.trees[idx as usize]);
        }
        let id = oid(&self.ids[idx as usize]);
        let raw = reader.read(&id)?;
        Ok(parse_head(&id, &raw)?.tree)
    }
}

fn hex20(v: &[u8]) -> Option<Sha1> {
    let mut out = [0u8; 20];
    hex::decode_to_slice(v, &mut out).ok()?;
    Some(out)
}

/// Only the `tree` and `parent` headers of a raw commit.
fn parse_head(id: &Oid, raw: &[u8]) -> anyhow::Result<Head> {
    let bad = |what: &str| anyhow!("commit {id}: {what}");
    let mut lines = raw.split(|&b| b == b'\n');
    let tree = lines
        .next()
        .and_then(|l| l.strip_prefix(b"tree "))
        .and_then(hex20)
        .ok_or_else(|| bad("missing or bad tree"))?;
    let mut parents = Vec::new();
    for l in lines {
        match l.strip_prefix(b"parent ") {
            Some(h) => parents.push(hex20(h).ok_or_else(|| bad("bad parent id"))?),
            None => break,
        }
    }
    Ok(Head { tree, parents })
}

fn read_heads(
    repo: &gix::ThreadSafeRepository,
    ids: &[Oid],
    threads: usize,
) -> anyhow::Result<Vec<Head>> {
    let per = ids.len().div_ceil(threads.max(1)).max(256);
    if ids.len() <= per {
        let mut reader = Reader::new(repo.to_thread_local());
        return ids.iter().map(|id| parse_head(id, &reader.read(id)?)).collect();
    }
    let parts: Vec<anyhow::Result<Vec<Head>>> = std::thread::scope(|s| {
        let handles: Vec<_> = ids
            .chunks(per)
            .map(|chunk| {
                s.spawn(move || {
                    let mut reader = Reader::new(repo.to_thread_local());
                    let mut out = Vec::with_capacity(chunk.len());
                    for (i, id) in chunk.iter().enumerate() {
                        out.push(parse_head(id, &reader.read(id)?)?);
                        // Commits are scattered over the whole pack: release
                        // the mapped pages as we go.
                        if i % 256 == 255 {
                            crate::rss::trim_mapped_packs(64 << 20);
                        }
                    }
                    Ok(out)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Err(anyhow!("commit reader panicked"))))
            .collect()
    });
    let mut out = Vec::with_capacity(ids.len());
    for p in parts {
        out.extend(p?);
    }
    Ok(out)
}

/// Find the commits reachable from `tip`, detect those earlier imports already
/// converted (their ancestors are never read), and order the rest parents
/// first. Reads ahead in batches: commits are checked with one
/// `get_derived_many` per chunk, not one lookup each, and read in parallel.
pub(crate) fn discover(
    repo: &gix::ThreadSafeRepository,
    backend: &Backend,
    handle: &tokio::runtime::Handle,
    tip: Oid,
    threads: usize,
) -> anyhow::Result<Graph> {
    let mut ids: Vec<Sha1> = vec![crate::read::sha(&tip)];
    let mut trees: Vec<Sha1> = vec![[0; 20]];
    let mut index: DigestMap<u32> = DigestMap::default();
    index.insert(ids[0], 0);
    let mut par_off: Vec<u32> = Vec::new();
    let mut par: Vec<u32> = Vec::new();
    let mut known: HashMap<u32, Known> = HashMap::new();

    let mut next = 0usize;
    let mut chunk = PRECHECK_START;
    while next < ids.len() {
        let end = (next + chunk).min(ids.len());
        let keys: Vec<&[u8]> = ids[next..end].iter().map(|s| s.as_slice()).collect();
        let found = handle.block_on(backend.meta.get_derived_many(COMMIT_KIND, &keys))?;
        let mut unknown = Vec::with_capacity(end - next);
        for (i, value) in (next..end).zip(found) {
            match value {
                Some(bytes) => {
                    known.insert(
                        i as u32,
                        Known { ant: ObjectId::from_bytes(&bytes)?, generation: 0 },
                    );
                }
                None => unknown.push(oid(&ids[i])),
            }
        }
        let mut heads = read_heads(repo, &unknown, threads)?.into_iter();
        crate::rss::trim_mapped_packs(64 << 20);
        for i in next..end {
            par_off.push(par.len() as u32);
            if known.contains_key(&(i as u32)) {
                continue;
            }
            let head = heads.next().ok_or_else(|| anyhow!("commit reader lost a commit"))?;
            trees[i] = head.tree;
            for p in head.parents {
                let idx = *index.entry(p).or_insert_with(|| {
                    ids.push(p);
                    trees.push([0; 20]);
                    (ids.len() - 1) as u32
                });
                par.push(idx);
            }
        }
        next = end;
        chunk = (chunk * 2).min(PRECHECK_MAX);
    }
    par_off.push(par.len() as u32);
    drop(index);

    let mut graph = Graph { ids, trees, par_off, par, known, order: Vec::new() };
    graph.order = topological_order(&graph);
    load_known_generations(&mut graph, backend, handle)?;
    Ok(graph)
}

/// Post-order DFS from the tip, first parent first: parents precede children
/// and each first-parent chain stays contiguous.
fn topological_order(g: &Graph) -> Vec<u32> {
    let n = g.ids.len();
    let mut seen = vec![false; n];
    let mut order = Vec::with_capacity(n);
    if g.known.contains_key(&0) {
        return order;
    }
    seen[0] = true;
    let mut stack: Vec<(u32, u32)> = vec![(0, 0)];
    while let Some((node, k)) = stack.last_mut() {
        let parents = g.parents(*node);
        if (*k as usize) < parents.len() {
            let p = parents[*k as usize];
            *k += 1;
            if !seen[p as usize] {
                seen[p as usize] = true;
                if !g.known.contains_key(&p) {
                    stack.push((p, 0));
                }
            }
        } else {
            order.push(*node);
            stack.pop();
        }
    }
    order
}

/// Generation numbers of the known commits the new ones hang off.
fn load_known_generations(
    g: &mut Graph,
    backend: &Backend,
    handle: &tokio::runtime::Handle,
) -> anyhow::Result<()> {
    let mut wanted: Vec<u32> = Vec::new();
    for &c in &g.order {
        for &p in g.parents(c) {
            if g.known.contains_key(&p) {
                wanted.push(p);
            }
        }
    }
    wanted.sort_unstable();
    wanted.dedup();
    if wanted.is_empty() {
        return Ok(());
    }
    let ants: Vec<ObjectId> = wanted.iter().map(|p| g.known[p].ant).collect();
    for (p, node) in wanted.iter().zip(handle.block_on(backend.meta.get_commits(&ants))?) {
        let node =
            node.with_context(|| format!("imported commit {} is not recorded", g.known[p].ant))?;
        if let Some(k) = g.known.get_mut(p) {
            k.generation = node.generation;
        }
    }
    Ok(())
}
