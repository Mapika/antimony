//! The export walk: reads every object of a fetch exactly once and hands it
//! to the delta encoder (`pack.rs`) in pack order.
//!
//! Planning (`select.rs`) settles the commits, the direct wants and the
//! objects the client has, from metadata alone. This walk then expands the
//! root trees *path by path*: all versions of one path form a group, and a
//! group is complete as soon as its parent path's group has been read (a path
//! fingerprint is a hash of the parent's fingerprint and the entry name).
//! Groups are read in breadth-first order, several store reads in flight.
//!
//! Tree versions of one path go out largest first, as git's own packer
//! orders them: on merge-heavy histories traversal order interleaves
//! branches, and size order brings near versions together (git/git's
//! blobless pack is 4x larger without it). Sizes are only known once bodies
//! are read, and one path can have GiBs of versions (the root directory of
//! git/git), so a group is sorted externally: each store read sorts its
//! versions of a path into a *run* and stores it as a chain of deltas against
//! the previous run entry (consecutive versions by size are near-identical:
//! a few hundred bytes each instead of ~30 KB), and the complete group is
//! a k-way merge of its runs, rebuilding each body with one delta
//! application. A group read by one store call is sorted in place, without
//! deltas. Runs of one group are capped at `STASH_BYTES`; past that the group
//! is merged in parts (correct, slightly worse compression).
//!
//! Blob versions keep traversal order (their sizes are not read either); a
//! blob is passed on as it arrives.
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
    mem,
};

use ant_core::{EncodedObject, ObjectId, ObjectKind, delta};
use ant_store::Backend;
use anyhow::{Result, anyhow, bail, ensure};
use bytes::BytesMut;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::select::{BATCH, Read, Selection, entries};

/// Objects in pack order, each with its path fingerprint.
pub type Batch = Vec<(u64, EncodedObject)>;

/// Path fingerprint of commits and directly wanted objects (sent first).
pub const NO_PATH: u64 = 0;
/// Path fingerprint of root trees (children hash their names into it).
const ROOT_PATH: u64 = 1;
/// One store read takes up to this many trees or commits; a blob, whose size
/// is not known before reading it, costs `BLOB_COST` of it.
const READ_COST: usize = 256;
const BLOB_COST: usize = 16;
const READS_IN_FLIGHT: usize = 8;
/// Bytes of runs (deltas and whole first entries) held for one group.
const STASH_BYTES: usize = 32 << 20;
/// Body bytes per batch of merged output.
const OUTPUT_BYTES: usize = 4 << 20;
/// Merged bodies are rebuilt into shared buffers of this size.
const SLAB_BYTES: usize = 1 << 20;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Commit,
    Tree,
    Blob,
}

/// One object to read. Groups (one kind at one path) are contiguous in the
/// queue; `first` and `last` mark their ends, `seq` is the position in the
/// group (traversal order, the tie-breaker of the size sort).
#[derive(Clone, Copy, Debug)]
struct Item {
    id: ObjectId,
    path: u64,
    seq: u32,
    kind: Kind,
    first: bool,
    last: bool,
}

/// Versions of one path from one store read, largest first.
enum Run {
    /// The whole group: sorted objects.
    Whole(Vec<EncodedObject>),
    /// Part of a group: entry `n` is a delta against entry `n - 1` (or whole
    /// where that does not pay), its bytes end at `ends[n]` in `data`.
    Chain { keys: Vec<(u32, u32)>, whole: Vec<bool>, ends: Vec<usize>, data: Vec<u8> },
}

impl Run {
    fn new(mut objects: Vec<(u32, EncodedObject)>, whole_group: bool) -> Self {
        objects.sort_by_key(|(seq, object)| (Reverse(object.body().len()), *seq));
        if whole_group {
            return Self::Whole(objects.into_iter().map(|(_, object)| object).collect());
        }
        let mut keys = Vec::with_capacity(objects.len());
        let mut whole = Vec::with_capacity(objects.len());
        let mut ends = Vec::with_capacity(objects.len());
        let mut data = Vec::new();
        let mut previous: Option<&[u8]> = None;
        let mut table = Vec::new();
        for (seq, object) in &objects {
            let body = object.body();
            let size = u32::try_from(body.len()).unwrap_or(u32::MAX);
            let delta = previous.and_then(|base| {
                let index = delta::Index::reuse(base, mem::take(&mut table))?;
                let delta = delta::encode_indexed(&index, base, body, body.len());
                table = index.into_table();
                delta
            });
            whole.push(delta.is_none());
            data.extend_from_slice(delta.as_deref().unwrap_or(body));
            keys.push((size, *seq));
            ends.push(data.len());
            previous = Some(body);
        }
        data.shrink_to_fit();
        Self::Chain { keys, whole, ends, data }
    }

    fn bytes(&self) -> usize {
        match self {
            Self::Whole(objects) => objects.iter().map(|o| o.as_bytes().len()).sum(),
            Self::Chain { data, ends, .. } => data.len() + ends.len() * 24,
        }
    }
}

/// A complete group (or a capped part of one), merged by the consumer.
pub struct Merge {
    path: u64,
    runs: Vec<Run>,
}

impl Merge {
    /// Rebuild the versions in size order (descending, then traversal
    /// order) into `tx`, `OUTPUT_BYTES` at a time.
    fn run(self, tx: Sender<Batch>) -> Result<()> {
        let Merge { path, runs } = self;
        struct Cursor {
            next: usize,
            start: usize,
            body: Vec<u8>,
            scratch: Vec<u8>,
        }
        let mut cursors: Vec<_> = runs
            .iter()
            .map(|_| Cursor { next: 0, start: 0, body: Vec::new(), scratch: Vec::new() })
            .collect();
        let key = |run: &Run, n: usize| match run {
            Run::Whole(objects) => objects.get(n).map(|o| (o.body().len() as u64, 0)),
            Run::Chain { keys, .. } => keys.get(n).map(|(size, seq)| (u64::from(*size), *seq)),
        };
        let mut heap = BinaryHeap::new();
        for (r, run) in runs.iter().enumerate() {
            if let Some((size, seq)) = key(run, 0) {
                heap.push((size, Reverse(seq), Reverse(r)));
            }
        }
        let mut out = Vec::new();
        let mut bytes = 0;
        let mut slab = BytesMut::new();
        while let Some((_, _, Reverse(r))) = heap.pop() {
            let cursor = &mut cursors[r];
            let n = cursor.next;
            let object = match &runs[r] {
                Run::Whole(objects) => objects[n].clone(),
                Run::Chain { whole, ends, data, .. } => {
                    let stored = &data[cursor.start..ends[n]];
                    cursor.start = ends[n];
                    if whole[n] {
                        cursor.body.clear();
                        cursor.body.extend_from_slice(stored);
                    } else {
                        delta::apply(&cursor.body, stored, &mut cursor.scratch)
                            .ok_or_else(|| anyhow!("corrupt export run"))?;
                        mem::swap(&mut cursor.body, &mut cursor.scratch);
                    }
                    // Rebuilt bodies share slabs: one allocation per
                    // `SLAB_BYTES` rather than one per version.
                    let header = format!("tree {}\0", cursor.body.len());
                    let need = header.len() + cursor.body.len();
                    if slab.capacity() - slab.len() < need {
                        slab = BytesMut::with_capacity(SLAB_BYTES.max(need));
                    }
                    slab.extend_from_slice(header.as_bytes());
                    slab.extend_from_slice(&cursor.body);
                    EncodedObject::from_bytes(slab.split().freeze())?
                }
            };
            cursor.next += 1;
            if let Some((size, seq)) = key(&runs[r], n + 1) {
                heap.push((size, Reverse(seq), Reverse(r)));
            }
            bytes += object.body().len();
            out.push((path, object));
            if bytes >= OUTPUT_BYTES {
                tx.blocking_send(mem::take(&mut out))
                    .map_err(|_| anyhow!("pack encoder stopped"))?;
                bytes = 0;
            }
        }
        if !out.is_empty() {
            tx.blocking_send(out).map_err(|_| anyhow!("pack encoder stopped"))?;
        }
        Ok(())
    }

    /// Objects of a group that fit one run, without a merge.
    fn into_whole(mut self) -> Result<Batch, Self> {
        if let [Run::Whole(_)] = &self.runs[..]
            && let Some(Run::Whole(objects)) = self.runs.pop()
        {
            return Ok(objects.into_iter().map(|object| (self.path, object)).collect());
        }
        Err(self)
    }
}

/// Walk output, in pack order.
pub enum Part {
    Objects(Batch),
    Merge(Merge),
}

impl Part {
    /// The objects of this part, in order; merges run on the blocking pool.
    pub fn objects(self) -> (Receiver<Batch>, Option<tokio::task::JoinHandle<Result<()>>>) {
        let (tx, rx) = mpsc::channel(2);
        match self {
            Part::Objects(batch) => {
                // Capacity 2: never blocks.
                let _ = tx.try_send(batch);
                (rx, None)
            }
            Part::Merge(merge) => (rx, Some(tokio::task::spawn_blocking(move || merge.run(tx)))),
        }
    }
}

/// One store read, prepared on the blocking pool in parallel with others:
/// tree entries listed (deduplicated within the read) and runs built.
struct Fetched {
    items: Vec<Item>,
    /// Commits and blobs, by item.
    objects: Vec<Option<EncodedObject>>,
    /// Children (fingerprint, tree?, id) of tree items; item `n`'s end at
    /// `ends[n]`.
    children: Vec<(u64, bool, ObjectId)>,
    ends: Vec<usize>,
    /// Runs, each with the index of its last item.
    runs: Vec<(usize, Run)>,
}

fn prepare(
    items: Vec<Item>,
    objects: Vec<Option<EncodedObject>>,
    excluded: &HashSet<ObjectId>,
    no_blobs: bool,
) -> Result<Fetched> {
    ensure!(objects.len() == items.len(), "incomplete store batch");
    let mut fetched = Fetched {
        objects: Vec::with_capacity(items.len()),
        children: Vec::new(),
        ends: Vec::with_capacity(items.len()),
        runs: Vec::new(),
        items: Vec::new(),
    };
    let mut seen = HashSet::new();
    let mut run = Vec::new();
    let mut run_first = false;
    for (n, (item, object)) in items.iter().zip(objects).enumerate() {
        let object = object.ok_or_else(|| anyhow!("missing object {}", item.id))?;
        if item.kind != Kind::Tree {
            fetched.objects.push(Some(object));
            fetched.ends.push(fetched.children.len());
            continue;
        }
        ensure!(object.kind() == ObjectKind::Tree, "expected tree {}", item.id);
        for entry in entries(object.body()) {
            let (mode, name, id) = entry?;
            if !(mode.is_tree() || mode.is_blob() && !no_blobs) {
                continue;
            }
            // Versions of a directory share most entries: list each once.
            if excluded.contains(&id) || !seen.insert(id) {
                continue;
            }
            let mut hash = DefaultHasher::new();
            item.path.hash(&mut hash);
            name.hash(&mut hash);
            fetched.children.push((hash.finish(), mode.is_tree(), id));
        }
        fetched.objects.push(None);
        fetched.ends.push(fetched.children.len());
        if run.is_empty() {
            run_first = item.first;
        }
        run.push((item.seq, object));
        if item.last || n + 1 == items.len() || items[n + 1].first {
            let whole = run_first && item.last;
            fetched.runs.push((n, Run::new(mem::take(&mut run), whole)));
        }
    }
    fetched.items = items;
    Ok(fetched)
}

/// Walk state; moved to the blocking pool for each read.
struct Walk {
    excluded: std::sync::Arc<HashSet<ObjectId>>,
    found: HashSet<ObjectId>,
    no_blobs: bool,
    queue: VecDeque<Item>,
    /// Child groups of the group being read, in discovery order.
    children: Vec<(u64, Kind, Vec<ObjectId>)>,
    child_index: HashMap<(u64, bool), usize>,
    /// Runs of the tree group being read.
    stash: Vec<Run>,
    stash_bytes: usize,
}

impl Walk {
    fn enqueue(&mut self, path: u64, kind: Kind, ids: Vec<ObjectId>) {
        let count = ids.len();
        self.queue.extend(ids.into_iter().enumerate().map(|(n, id)| Item {
            id,
            path,
            seq: u32::try_from(n).unwrap_or(u32::MAX),
            kind,
            first: n == 0,
            last: n + 1 == count,
        }));
    }

    fn emit(parts: &mut Vec<Part>, path: u64, object: EncodedObject) {
        if let Some(Part::Objects(batch)) = parts.last_mut() {
            batch.push((path, object));
        } else {
            parts.push(Part::Objects(vec![(path, object)]));
        }
    }

    fn merge(&mut self, path: u64, parts: &mut Vec<Part>) {
        if self.stash.is_empty() {
            return;
        }
        if self.stash.len() > 1 {
            tracing::debug!(
                path,
                runs = self.stash.len(),
                stash_bytes = self.stash_bytes,
                "export: merging versions of one path"
            );
        }
        self.stash_bytes = 0;
        let merge = Merge { path, runs: mem::take(&mut self.stash) };
        match merge.into_whole() {
            Ok(objects) => {
                for (path, object) in objects {
                    Self::emit(parts, path, object);
                }
            }
            Err(merge) => parts.push(Part::Merge(merge)),
        }
    }

    fn visit(&mut self, fetched: Fetched) -> Vec<Part> {
        let Fetched { items, objects, children, ends, runs } = fetched;
        let mut parts = Vec::new();
        let mut runs = runs.into_iter().peekable();
        let mut start = 0;
        for (n, (item, object)) in items.into_iter().zip(objects).enumerate() {
            if let Some(object) = object {
                Self::emit(&mut parts, item.path, object);
            }
            for &(path, tree, id) in &children[start..ends[n]] {
                // Mark on discovery: shared subtrees are read once, at the
                // first path they are reached at.
                if !self.found.insert(id) {
                    continue;
                }
                let index = *self.child_index.entry((path, tree)).or_insert_with(|| {
                    let kind = if tree { Kind::Tree } else { Kind::Blob };
                    self.children.push((path, kind, Vec::new()));
                    self.children.len() - 1
                });
                self.children[index].2.push(id);
            }
            start = ends[n];
            if let Some((_, run)) = runs.next_if(|(end, _)| *end == n) {
                self.stash_bytes += run.bytes();
                self.stash.push(run);
                if self.stash_bytes >= STASH_BYTES && !item.last {
                    self.merge(item.path, &mut parts);
                }
            }
            if item.last {
                self.merge(item.path, &mut parts);
                self.child_index.clear();
                for (path, kind, ids) in mem::take(&mut self.children) {
                    self.enqueue(path, kind, ids);
                }
            }
        }
        parts
    }
}

/// Classify the direct wants (one read each): blobs are sent, largest first,
/// and marked found so the walk does not send them again; trees join the
/// roots. Commits without metadata cannot be served.
async fn direct(
    b: &Backend,
    found: &mut HashSet<ObjectId>,
    ids: &[ObjectId],
    roots: &mut Vec<ObjectId>,
) -> Result<Batch> {
    let mut blobs = Vec::new();
    for chunk in ids.chunks(BATCH) {
        for (id, object) in chunk.iter().zip(b.objects.get_many(chunk).await?) {
            let object = object.ok_or_else(|| anyhow!("unknown want {id}"))?;
            match object.kind() {
                ObjectKind::Blob => {
                    if found.insert(*id) {
                        blobs.push((NO_PATH, object));
                    }
                }
                ObjectKind::Tree => roots.push(*id),
                ObjectKind::Commit => bail!("commit {id} has no graph metadata"),
            }
        }
    }
    blobs.sort_by_key(|(_, object)| Reverse(object.body().len()));
    Ok(blobs)
}

/// Read every object of `selection` once and send it to `tx` in pack order:
/// direct blobs, commits, then trees and blobs path by path. Returns the
/// number of objects sent.
pub async fn walk(b: Backend, selection: Selection, tx: Sender<Part>) -> Result<u64> {
    let Selection { commits, direct: wants, mut roots, excluded, no_blobs, .. } = selection;
    let mut found = HashSet::new();
    let first = direct(&b, &mut found, &wants, &mut roots).await?;
    let mut sent = first.len() as u64;
    if !first.is_empty() {
        tx.send(Part::Objects(first)).await.map_err(|_| anyhow!("pack encoder stopped"))?;
    }
    let roots: Vec<_> =
        roots.into_iter().filter(|id| !excluded.contains(id) && found.insert(*id)).collect();
    let mut walk = Walk {
        excluded: std::sync::Arc::new(excluded),
        found,
        no_blobs,
        queue: VecDeque::new(),
        children: Vec::new(),
        child_index: HashMap::new(),
        stash: Vec::new(),
        stash_bytes: 0,
    };
    walk.enqueue(NO_PATH, Kind::Commit, commits);
    walk.enqueue(ROOT_PATH, Kind::Tree, roots);
    let mut reads = VecDeque::new();
    loop {
        while reads.len() < READS_IN_FLIGHT && !walk.queue.is_empty() {
            let mut cost = 0;
            let mut items = Vec::new();
            while let Some(item) = walk.queue.front() {
                let item_cost = if item.kind == Kind::Blob { BLOB_COST } else { 1 };
                if cost + item_cost > READ_COST && !items.is_empty() {
                    break;
                }
                cost += item_cost;
                items.extend(walk.queue.pop_front());
            }
            let b = b.clone();
            let excluded = walk.excluded.clone();
            let no_blobs = walk.no_blobs;
            reads.push_back(Read(tokio::spawn(async move {
                let ids: Vec<_> = items.iter().map(|item| item.id).collect();
                let objects = b.objects.get_many(&ids).await?;
                // Parsing and delta-chaining are CPU work: keep them off the
                // async executors, in parallel across reads.
                tokio::task::spawn_blocking(move || prepare(items, objects, &excluded, no_blobs))
                    .await?
            })));
        }
        let Some(mut read) = reads.pop_front() else { break };
        let fetched = (&mut read.0).await??;
        sent += fetched.items.len() as u64;
        let (state, parts) = tokio::task::spawn_blocking(move || {
            let parts = walk.visit(fetched);
            (walk, parts)
        })
        .await?;
        walk = state;
        for part in parts {
            tx.send(part).await.map_err(|_| anyhow!("pack encoder stopped"))?;
        }
    }
    ensure!(walk.stash.is_empty() && walk.children.is_empty(), "unfinished export walk");
    Ok(sent)
}

#[cfg(test)]
pub async fn ids(b: &Backend, selection: Selection) -> Result<Vec<ObjectId>> {
    let (tx, mut rx) = mpsc::channel(4);
    let walker = tokio::spawn(walk(b.clone(), selection, tx));
    let mut ids = Vec::new();
    while let Some(part) = rx.recv().await {
        let (mut objects, merge) = part.objects();
        while let Some(batch) = objects.recv().await {
            ids.extend(batch.iter().map(|(_, object)| object.id()));
        }
        if let Some(merge) = merge {
            merge.await??;
        }
    }
    walker.await??;
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// External sort: runs of consecutive versions (delta-chained, as for
    /// groups larger than one store read) merge into exactly the order of
    /// one global sort by size (descending, then traversal order), with
    /// every body rebuilt byte for byte.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runs_merge_into_global_size_order() {
        let mut state = 0x9e37_79b9_u32;
        let mut random = move |n: u32| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state % n
        };
        // A directory listing that mostly grows, with edits and equal sizes.
        let mut body: Vec<u8> = (0..4000).map(|_| random(256) as u8).collect();
        let mut versions = Vec::new();
        for seq in 0..1000_u32 {
            match random(4) {
                0 => body.extend((0..random(64)).map(|_| random(256) as u8)),
                1 => body.truncate(body.len().saturating_sub(random(48) as usize)),
                _ => {
                    let at = random(body.len() as u32) as usize;
                    body[at] = random(256) as u8;
                }
            }
            versions.push((seq, EncodedObject::new(ObjectKind::Tree, &body)));
        }
        let mut expected = versions.clone();
        expected.sort_by_key(|(seq, o)| (Reverse(o.body().len()), *seq));
        let runs: Vec<_> = versions.chunks(97).map(|c| Run::new(c.to_vec(), false)).collect();
        let raw: usize = versions.iter().map(|(_, o)| o.body().len()).sum();
        let stored: usize = runs.iter().map(Run::bytes).sum();
        assert!(stored * 5 < raw, "runs hold {stored} of {raw} bytes");
        let (tx, mut rx) = mpsc::channel(2);
        let merge = tokio::task::spawn_blocking(move || Merge { path: 7, runs }.run(tx));
        let mut merged = Vec::new();
        while let Some(batch) = rx.recv().await {
            merged.extend(batch);
        }
        merge.await.unwrap().unwrap();
        assert!(merged.iter().all(|(path, _)| *path == 7));
        let merged: Vec<_> = merged.into_iter().map(|(_, o)| o).collect();
        let expected: Vec<_> = expected.into_iter().map(|(_, o)| o).collect();
        assert!(merged == expected, "merge order or bodies differ");
        // A group read by one store call is sorted in place.
        let whole = Merge { path: 3, runs: vec![Run::new(versions[..50].to_vec(), true)] };
        let objects = whole.into_whole().ok().unwrap();
        let mut sorted = versions[..50].to_vec();
        sorted.sort_by_key(|(seq, o)| (Reverse(o.body().len()), *seq));
        assert!(objects.into_iter().map(|(_, o)| o).eq(sorted.into_iter().map(|(_, o)| o)));
    }
}
