//! The parallel half of the producer.
//!
//! Workers take chunks of the conversion order and, for each commit, do all the
//! work that needs only *source* objects: read and parse the commit, diff its
//! tree against its parents' trees, read and hash the blobs it introduces, and
//! parse the trees that changed. The sequential converter then only maps ids
//! through its memo and hashes the (small) rewritten trees.
//!
//! Everything a worker returns is keyed by source object id, so it is a pure
//! hint: if the converter finds something missing (or already converted) it
//! falls back to reading it itself, and the result is identical either way.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Condvar, Mutex};

use ant_core::{EncodedObject, EntryMode, ObjectId, RepoPath};
use ant_store::ChangeKind;

use crate::commit::{Oid, RawCommit, parse_commit};
use crate::graph::Graph;
use crate::memo::{DigestMap, Sha1};
use crate::read::{Reader, oid, sha};
use crate::tree::{Ent, Parsed, TreeCache, parse_tree};

/// Commits per unit of work.
const CHUNK: usize = 16;
/// Prepared bytes a worker may run ahead before waiting for the converter.
const AHEAD_BYTES: usize = 96 << 20;
/// Parsed-tree cache per worker (bytes per generation).
const WORKER_TREE_CACHE: usize = 6 << 20;

/// A leaf change in source ids; the converter maps them to antimony ids.
pub(crate) struct SrcChange {
    pub path: RepoPath,
    pub kind: ChangeKind,
    pub old: Option<(EntryMode, Sha1)>,
    pub new: Option<(EntryMode, Sha1)>,
}

/// Everything prepared for one commit.
pub(crate) struct Prep {
    pub idx: u32,
    pub source: Oid,
    pub raw: RawCommit,
    /// Parsed trees that are new relative to the commit's parents.
    pub trees: DigestMap<Arc<Parsed>>,
    /// Blobs that are new relative to the commit's parents, converted.
    pub blobs: DigestMap<(ObjectId, EncodedObject)>,
    /// Non-tree differences from the first parent, sorted by path.
    pub changes: Vec<SrcChange>,
    pub bytes: usize,
}

struct Frame {
    path: String,
    new: Option<Sha1>,
    /// This directory's tree id in each parent (`[None]` for a root commit).
    olds: Vec<Option<Sha1>>,
    /// Everything below equals what another parent already has.
    known: bool,
}

pub(crate) struct Prepper<'g> {
    graph: &'g Graph,
    reader: Reader,
    cache: TreeCache,
}

impl<'g> Prepper<'g> {
    pub fn new(graph: &'g Graph, repo: &gix::ThreadSafeRepository) -> Self {
        Self {
            graph,
            reader: Reader::new(repo.to_thread_local()),
            cache: TreeCache::new(WORKER_TREE_CACHE),
        }
    }

    fn tree(&mut self, id: &Sha1, path: &str, commit: &Oid) -> anyhow::Result<Arc<Parsed>> {
        if let Some(t) = self.cache.get(id) {
            return Ok(t);
        }
        let source = oid(id);
        let raw = self.reader.read(&source)?;
        let parsed = Arc::new(parse_tree(&source, raw, path, Some(commit))?);
        self.cache.insert(*id, parsed.clone());
        Ok(parsed)
    }

    pub fn prepare(&mut self, idx: u32) -> anyhow::Result<Prep> {
        let source = oid(&self.graph.ids[idx as usize]);
        let raw = parse_commit(source, &self.reader.read(&source)?)?;
        let root = sha(&raw.tree);

        let parents = self.graph.parents(idx);
        let mut olds: Vec<Option<Sha1>> = Vec::with_capacity(parents.len().max(1));
        for p in parents {
            olds.push(Some(self.graph.tree_of(*p, &mut self.reader)?));
        }
        if olds.is_empty() {
            olds.push(None);
        }

        let mut trees: DigestMap<Arc<Parsed>> = DigestMap::default();
        let mut wanted: HashSet<Sha1, crate::memo::DigestBuild> = HashSet::default();
        let mut changes: Vec<SrcChange> = Vec::new();
        let empty = Parsed::empty();

        let mut stack = vec![Frame { path: String::new(), new: Some(root), olds, known: false }];
        while let Some(f) = stack.pop() {
            if f.new == f.olds[0] {
                continue;
            }
            let known = f.known || (f.new.is_some() && f.olds[1..].contains(&f.new));
            let newp = match &f.new {
                Some(id) => self.tree(id, &f.path, &source)?,
                None => empty.clone(),
            };
            let oldp = match &f.olds[0] {
                Some(id) => self.tree(id, &f.path, &source)?,
                None => empty.clone(),
            };
            if let (Some(id), false) = (&f.new, known) {
                trees.insert(*id, newp.clone());
            }
            // Other parents' versions of this directory, aligned with `f.olds[1..]`.
            let mut others: Vec<Option<Arc<Parsed>>> = Vec::new();
            if !known {
                for o in &f.olds[1..] {
                    others.push(match o {
                        Some(id) => Some(self.tree(id, &f.path, &source)?),
                        None => None,
                    });
                }
            }

            let (mut i, mut j) = (0usize, 0usize);
            while i < newp.ents.len() || j < oldp.ents.len() {
                let (n, o): (Option<&Ent>, Option<&Ent>) =
                    match (newp.ents.get(i), oldp.ents.get(j)) {
                        (Some(a), Some(b)) => {
                            match ant_core::git_name_cmp(
                                newp.name_bytes(a),
                                a.mode.is_tree(),
                                oldp.name_bytes(b),
                                b.mode.is_tree(),
                            ) {
                                std::cmp::Ordering::Less => {
                                    i += 1;
                                    (Some(a), None)
                                }
                                std::cmp::Ordering::Greater => {
                                    j += 1;
                                    (None, Some(b))
                                }
                                std::cmp::Ordering::Equal => {
                                    i += 1;
                                    j += 1;
                                    (Some(a), Some(b))
                                }
                            }
                        }
                        (Some(a), None) => {
                            i += 1;
                            (Some(a), None)
                        }
                        (None, Some(b)) => {
                            j += 1;
                            (None, Some(b))
                        }
                        (None, None) => break,
                    };
                if let (Some(n), Some(o)) = (n, o)
                    && n.mode == o.mode
                    && n.id == o.id
                {
                    continue;
                }
                let name_bytes = match (n, o) {
                    (Some(n), _) => newp.name_bytes(n),
                    (None, Some(o)) => oldp.name_bytes(o),
                    (None, None) => continue,
                };
                let name = std::str::from_utf8(name_bytes).unwrap_or_default();
                let n_tree = n.filter(|e| e.mode.is_tree()).map(|e| e.id);
                let o_tree = o.filter(|e| e.mode.is_tree()).map(|e| e.id);
                let n_leaf = n.filter(|e| !e.mode.is_tree()).map(|e| (e.mode.class(), e.id));
                let o_leaf = o.filter(|e| !e.mode.is_tree()).map(|e| (e.mode.class(), e.id));
                let child =
                    if f.path.is_empty() { name.to_string() } else { format!("{}/{name}", f.path) };

                if n_tree.is_some() || o_tree.is_some() {
                    let mut olds = Vec::with_capacity(f.olds.len());
                    olds.push(o_tree);
                    if known {
                        olds.resize(f.olds.len(), None);
                    } else {
                        for other in &others {
                            olds.push(
                                other.as_ref().and_then(|p| p.find(name_bytes, true)).map(|e| e.id),
                            );
                        }
                    }
                    stack.push(Frame { path: child.clone(), new: n_tree, olds, known });
                }
                if o_leaf != n_leaf {
                    let kind = match (o_leaf, n_leaf) {
                        (None, Some(_)) => ChangeKind::Added,
                        (Some(_), None) => ChangeKind::Deleted,
                        _ => ChangeKind::Modified,
                    };
                    if let Some((mode, id)) = n_leaf
                        && mode.is_blob()
                        && !known
                        && !others.iter().flatten().any(|p| {
                            p.find(name_bytes, false)
                                .is_some_and(|e| e.mode.class() == mode && e.id == id)
                        })
                    {
                        wanted.insert(id);
                    }
                    changes.push(SrcChange {
                        path: RepoPath::new(child)?,
                        kind,
                        old: o_leaf,
                        new: n_leaf,
                    });
                }
            }
        }
        changes.sort_by(|a, b| a.path.cmp(&b.path));

        let mut blobs: DigestMap<(ObjectId, EncodedObject)> = DigestMap::default();
        let mut bytes = 0usize;
        for id in wanted {
            let obj = self.reader.read_blob(&oid(&id))?;
            bytes += obj.as_bytes().len();
            blobs.insert(id, (obj.id(), obj));
        }
        bytes += trees.values().map(|t| t.weight()).sum::<usize>();
        Ok(Prep { idx, source, raw, trees, blobs, changes, bytes })
    }
}

struct State {
    next_chunk: usize,
    consumed: usize,
    held_bytes: usize,
    stop: bool,
    done: BTreeMap<usize, anyhow::Result<Vec<Prep>>>,
}

/// Hands chunks of prepared commits from the workers to the converter, in order.
pub(crate) struct Pipeline<'g> {
    graph: &'g Graph,
    total: usize,
    window: usize,
    state: Mutex<State>,
    work: Condvar,
    ready: Condvar,
}

impl<'g> Pipeline<'g> {
    pub fn new(graph: &'g Graph, workers: usize) -> Self {
        Self {
            graph,
            total: graph.order.len().div_ceil(CHUNK),
            window: workers * 3 + 2,
            state: Mutex::new(State {
                next_chunk: 0,
                consumed: 0,
                held_bytes: 0,
                stop: false,
                done: BTreeMap::new(),
            }),
            work: Condvar::new(),
            ready: Condvar::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn stop(&self) {
        self.lock().stop = true;
        self.work.notify_all();
        self.ready.notify_all();
    }

    /// Worker loop; returns when all chunks are taken or the pipeline stops.
    pub fn work(&self, repo: &gix::ThreadSafeRepository) {
        let mut prepper = Prepper::new(self.graph, repo);
        loop {
            let chunk = {
                let mut st = self.lock();
                loop {
                    if st.stop || st.next_chunk >= self.total {
                        return;
                    }
                    let c = st.next_chunk;
                    if c == st.consumed
                        || (c < st.consumed + self.window && st.held_bytes < AHEAD_BYTES)
                    {
                        st.next_chunk += 1;
                        break c;
                    }
                    st = self.work.wait(st).unwrap_or_else(|e| e.into_inner());
                }
            };
            let lo = chunk * CHUNK;
            let hi = (lo + CHUNK).min(self.graph.order.len());
            let result: anyhow::Result<Vec<Prep>> =
                self.graph.order[lo..hi].iter().map(|&i| prepper.prepare(i)).collect();
            let bytes = result.as_ref().map_or(0, |v| v.iter().map(|p| p.bytes).sum());
            let mut st = self.lock();
            st.held_bytes += bytes;
            st.done.insert(chunk, result);
            self.ready.notify_all();
        }
    }

    /// The next chunk in conversion order, waiting for it; `None` at the end.
    pub fn next(&self) -> Option<anyhow::Result<Vec<Prep>>> {
        let mut st = self.lock();
        loop {
            let c = st.consumed;
            if let Some(r) = st.done.remove(&c) {
                st.consumed += 1;
                if let Ok(v) = &r {
                    st.held_bytes = st.held_bytes.saturating_sub(v.iter().map(|p| p.bytes).sum());
                }
                self.work.notify_all();
                return Some(r);
            }
            if st.consumed >= self.total || st.stop {
                return None;
            }
            st = self.ready.wait(st).unwrap_or_else(|e| e.into_inner());
        }
    }
}
