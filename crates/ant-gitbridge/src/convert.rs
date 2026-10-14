//! The sequential half of the producer: rewrites trees and commits into
//! antimony objects, in conversion order, from what the workers prepared.
//!
//! Only cheap, order-dependent work happens here (memo lookups, encoding and
//! hashing the rewritten trees, assigning generations). Anything the workers
//! did not supply (or the bounded memo forgot) is read again on the spot, so
//! the output never depends on timing or on the memo size.

use std::sync::Arc;

use ant_core::{Commit, EncodedObject, EntryMode, ObjectId, ObjectKind, Tree, TreeEntry};
use ant_store::{CommitNode, PathChange};
use anyhow::{anyhow, bail};
use tokio::sync::mpsc;

use crate::commit::Oid;
use crate::graph::Graph;
use crate::memo::{Memo, Sha1};
use crate::prep::Prep;
use crate::read::{Reader, oid};
use crate::tree::{Parsed, TreeCache, parse_tree};
use crate::{Msg, NewCommit};

/// Cut a batch of objects at this many bytes even if `batch_objects` is not reached.
const BATCH_BYTES: usize = 16 << 20;
/// Parsed-tree cache for the fallback path (bytes per generation).
const FALLBACK_TREE_CACHE: usize = 32 << 20;

/// How often the fallback paths ran (a measure of memo and worker misses).
#[derive(Default, Debug, Clone, Copy)]
pub(crate) struct ConvStats {
    pub commits: u64,
    pub fallback_trees: u64,
    pub fallback_blobs: u64,
    pub fallback_ids: u64,
}

/// Sends converted objects and commits to the writer in batches.
pub(crate) struct Sink {
    tx: mpsc::Sender<Msg>,
    batch_objects: usize,
    objects: Vec<EncodedObject>,
    object_bytes: usize,
    commits: Vec<NewCommit>,
    /// The target already holds earlier imports (its objects may exist).
    pub incremental: bool,
}

impl Sink {
    pub fn new(tx: mpsc::Sender<Msg>, batch_objects: usize) -> Self {
        Self {
            tx,
            batch_objects,
            objects: Vec::new(),
            object_bytes: 0,
            commits: Vec::new(),
            incremental: false,
        }
    }

    pub fn send(&self, msg: Msg) -> anyhow::Result<()> {
        self.tx.blocking_send(msg).map_err(|_| anyhow!("import writer stopped"))
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        if self.objects.is_empty() && self.commits.is_empty() {
            return Ok(());
        }
        let objects = std::mem::take(&mut self.objects);
        let bytes = std::mem::take(&mut self.object_bytes);
        let commits = std::mem::take(&mut self.commits);
        self.send(Msg::Batch { objects, bytes, commits, check_present: self.incremental })
    }

    fn push(&mut self, obj: EncodedObject) -> anyhow::Result<()> {
        self.object_bytes += obj.as_bytes().len();
        self.objects.push(obj);
        if self.objects.len() >= self.batch_objects || self.object_bytes >= BATCH_BYTES {
            self.flush()?;
        }
        Ok(())
    }
}

/// What the workers prepared for the commit being converted.
struct View {
    trees: crate::memo::DigestMap<Arc<Parsed>>,
    blobs: crate::memo::DigestMap<(ObjectId, EncodedObject)>,
}

pub(crate) struct Converter<'g> {
    graph: &'g Graph,
    reader: Reader,
    pub memo: Memo,
    cache: TreeCache,
    /// Antimony id of each converted (or already imported) commit, by graph index.
    ant: Vec<ObjectId>,
    generation: Vec<u32>,
    sink: Sink,
    /// Directory being converted, for error messages.
    path: String,
    current: Option<Oid>,
    pub stats: ConvStats,
}

impl<'g> Converter<'g> {
    pub fn new(graph: &'g Graph, reader: Reader, memo: Memo, sink: Sink) -> Self {
        let mut ant = vec![ObjectId([0; 32]); graph.ids.len()];
        let mut generation = vec![0u32; graph.ids.len()];
        for (&i, k) in &graph.known {
            ant[i as usize] = k.ant;
            generation[i as usize] = k.generation as u32;
        }
        Self {
            graph,
            reader,
            memo,
            cache: TreeCache::new(FALLBACK_TREE_CACHE),
            ant,
            generation,
            sink,
            path: String::new(),
            current: None,
            stats: ConvStats::default(),
        }
    }

    pub fn into_parts(self) -> (Memo, Sink) {
        (self.memo, self.sink)
    }

    pub fn sink(&mut self) -> &mut Sink {
        &mut self.sink
    }

    /// Antimony id of the commit with graph index `idx` (converted or known).
    pub fn commit_id(&self, idx: u32) -> Option<ObjectId> {
        let id = self.ant[idx as usize];
        (id != ObjectId([0; 32])).then_some(id)
    }

    pub fn convert(&mut self, prep: Prep) -> anyhow::Result<()> {
        let Prep { idx, source, raw, trees, blobs, changes, .. } = prep;
        self.current = Some(source);
        self.path.clear();
        let mut view = View { trees, blobs };

        let tree = self.convert_tree(&crate::read::sha(&raw.tree), &mut view)?;
        let mut parents = Vec::with_capacity(raw.parents.len());
        let mut generation = 0u32;
        for &p in self.graph.parents(idx) {
            let a =
                self.commit_id(p).ok_or_else(|| anyhow!("parent of {source} was not converted"))?;
            generation = generation.max(self.generation[p as usize]);
            parents.push(a);
        }
        generation += 1;

        let mut out = Vec::with_capacity(changes.len());
        for c in changes {
            let old = match c.old {
                Some((m, id)) => Some((m, self.leaf(m, &id, &mut view, false)?)),
                None => None,
            };
            let new = match c.new {
                Some((m, id)) => Some((m, self.leaf(m, &id, &mut view, false)?)),
                None => None,
            };
            out.push(PathChange { path: c.path, kind: c.kind, old, new });
        }

        let committer_time = raw.committer.time;
        let commit = Commit {
            tree,
            parents: parents.clone(),
            author: raw.author,
            committer: raw.committer,
            extra_headers: raw.extra_headers,
            message: raw.message,
        };
        let obj = EncodedObject::new(ObjectKind::Commit, &commit.encode());
        let id = obj.id();
        self.ant[idx as usize] = id;
        self.generation[idx as usize] = generation;
        let node =
            CommitNode { id, tree, parents, generation: u64::from(generation), committer_time };
        self.sink.commits.push(NewCommit { node, changes: out, git: source });
        self.stats.commits += 1;
        self.sink.push(obj)
    }

    /// Parsed entries of source tree `id` when the workers did not supply them.
    fn fallback_tree(&mut self, id: &Sha1) -> anyhow::Result<Arc<Parsed>> {
        if let Some(t) = self.cache.get(id) {
            return Ok(t);
        }
        self.stats.fallback_trees += 1;
        let source = oid(id);
        let raw = self.reader.read(&source)?;
        let parsed = Arc::new(parse_tree(&source, raw, &self.path, self.current.as_ref())?);
        self.cache.insert(*id, parsed.clone());
        Ok(parsed)
    }

    fn convert_tree(&mut self, id: &Sha1, view: &mut View) -> anyhow::Result<ObjectId> {
        if let Some(a) = self.memo.get(id) {
            return Ok(a);
        }
        let parsed = match view.trees.get(id) {
            Some(t) => t.clone(),
            None => self.fallback_tree(id)?,
        };
        let mut body: Vec<u8> = Vec::with_capacity(parsed.ents.len() * 56);
        let mut kids: Vec<ObjectId> = Vec::with_capacity(parsed.ents.len());
        let mut canonical = true;
        let mut prev: Option<&crate::tree::Ent> = None;
        for e in &parsed.ents {
            let name = parsed.name_bytes(e);
            let child = match e.mode.class() {
                EntryMode::Tree => {
                    let len = self.path.len();
                    if len > 0 {
                        self.path.push('/');
                    }
                    self.path.push_str(parsed.name(e));
                    let c = self.convert_tree(&e.id, view);
                    self.path.truncate(len);
                    c?
                }
                m => self.leaf(m, &e.id, view, true)?,
            };
            if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') {
                canonical = false;
            }
            if let Some(p) = prev
                && ant_core::git_name_cmp(
                    parsed.name_bytes(p),
                    p.mode.is_tree(),
                    name,
                    e.mode.is_tree(),
                ) != std::cmp::Ordering::Less
            {
                canonical = false;
            }
            prev = Some(e);
            if canonical {
                body.extend_from_slice(e.mode.as_octal().as_bytes());
                body.push(b' ');
                body.extend_from_slice(name);
                body.push(0);
                body.extend_from_slice(child.as_bytes());
            }
            kids.push(child);
        }
        if !canonical {
            // Unusual source tree (unsorted, duplicate or invalid names): let
            // ant-core sort or reject it.
            let entries = parsed
                .ents
                .iter()
                .zip(&kids)
                .map(|(e, k)| TreeEntry { mode: e.mode, name: parsed.name(e).to_string(), id: *k })
                .collect();
            body = Tree::new(entries)?.encode();
        }
        let obj = EncodedObject::new(ObjectKind::Tree, &body);
        let ant = obj.id();
        self.memo.insert(id, ant);
        self.sink.push(obj)?;
        Ok(ant)
    }

    /// Antimony id of a non-tree entry. `emit`: the object may not be stored
    /// yet (a tree entry being converted); otherwise it was stored when it was
    /// first converted and only its id is needed.
    fn leaf(
        &mut self,
        mode: EntryMode,
        id: &Sha1,
        view: &mut View,
        emit: bool,
    ) -> anyhow::Result<ObjectId> {
        match mode.class() {
            // Submodule pointer: carried through, never fetched. The foreign
            // commit id is zero-padded to 32 bytes (lossless for SHA-1/256).
            EntryMode::Gitlink => {
                let mut raw = [0u8; 32];
                raw[..20].copy_from_slice(id);
                Ok(ObjectId(raw))
            }
            EntryMode::Tree => bail!("tree entry where a blob was expected"),
            _ => {
                if let Some(a) = self.memo.get(id) {
                    return Ok(a);
                }
                if let Some((aid, _)) = view.blobs.get(id) {
                    let aid = *aid;
                    if emit && let Some((_, obj)) = view.blobs.remove(id) {
                        self.sink.push(obj)?;
                    }
                    self.memo.insert(id, aid);
                    return Ok(aid);
                }
                let obj = self.reader.read_blob(&oid(id))?;
                let aid = obj.id();
                if emit {
                    self.stats.fallback_blobs += 1;
                    self.sink.push(obj)?;
                } else {
                    self.stats.fallback_ids += 1;
                }
                self.memo.insert(id, aid);
                Ok(aid)
            }
        }
    }
}
