//! Import existing git repositories (SHA-1 or SHA-256) into antimony.
//!
//! TASK (gitbridge): implement `import`.
//!
//! - Open the repo with `gix`. For each `(git_ref, ant_ref)` in
//!   `opts.refs`, collect all commits reachable from the git ref that are
//!   not yet imported, and convert them parents-first (topological order).
//! - Conversion rewrites object ids to SHA-256 git objects
//!   (`ant_core::{Tree, Commit}`) while preserving everything else byte for
//!   byte: tree entry names/modes, commit author/committer/time zones,
//!   extra headers (e.g. `encoding`, `mergetag`, `gpgsig`) and messages
//!   (raw bytes: names and messages need not be UTF-8). The result MUST
//!   equal what `git fast-export | git fast-import` into a
//!   `--object-format=sha256` repo produces for unsigned commits (the test
//!   checks every commit id).
//! - Memoize SHA-1→SHA-256 for trees and blobs within a run (identical
//!   subtrees are converted once). Persist the commit mapping in
//!   `MetaStore` derived data, kind `"git-import/commit/v1"`, key = source
//!   commit id bytes, value = 32-byte antimony id — this makes re-imports
//!   incremental (already-imported commits are skipped without reading
//!   their trees).
//! - Write objects in batches (`opts.batch_objects` per `put_many`, several
//!   batches in flight); record each batch's commits (generation and
//!   changeset computed by the producer from its own conversion state, no
//!   tree re-reads) with one `put_commits` once the batch's objects and
//!   those of all earlier batches are stored.
//! - Finally point each `ant_ref` at the converted tip via `cas_ref`
//!   (expected = current value; it's an import tool, not concurrent).
//! - gix is synchronous: do git reading inside `spawn_blocking`, streaming
//!   converted objects to the async writer through a bounded channel, so
//!   memory stays bounded on big repos.
//! - Non-UTF-8 tree entry names → error naming the path and commit. Commit
//!   text is carried as raw bytes.
//! - Return stats counting objects actually written (not skipped). When no
//!   previously imported commit is found the target is assumed empty and the
//!   existence check is skipped (re-puts are idempotent), so stats then count
//!   every converted object.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context as _};
use ant_core::{
    Commit, EncodedObject, EntryMode, ObjectId, ObjectKind, RepoPath, Signature, Tree, TreeEntry,
};
use ant_store::{Backend, CasOutcome, ChangeKind, CommitNode, PathChange};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Derived-data kind mapping a source commit id to its antimony commit id.
const COMMIT_KIND: &str = "git-import/commit/v1";

/// Batches of objects stored concurrently (commits are still recorded in order).
const BATCHES_IN_FLIGHT: usize = 4;
/// Rotate the parsed-tree cache after this many entries per generation.
const TREE_CACHE_ENTRIES: usize = 400_000;
/// Commits per incremental pre-check query: starts small, doubles up to the cap.
const PRECHECK_START: usize = 64;
const PRECHECK_MAX: usize = 8192;

#[derive(Debug, Clone)]
pub struct ImportOptions {
    /// (git ref, antimony ref), e.g. ("refs/heads/main", "main").
    pub refs: Vec<(String, String)>,
    pub batch_objects: usize,
}

impl ImportOptions {
    pub fn single(git_ref: &str, ant_ref: &str) -> Self {
        Self { refs: vec![(git_ref.into(), ant_ref.into())], batch_objects: 1024 }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportStats {
    pub commits: u64,
    pub trees: u64,
    pub blobs: u64,
}

/// A converted commit ready to be recorded.
struct NewCommit {
    node: CommitNode,
    changes: Vec<PathChange>,
    /// Source commit id (key of the derived mapping).
    git: gix::ObjectId,
}

/// Producer → writer messages. Objects always precede the commits that
/// reference them, so the writer can store a batch and then record its
/// commits parents-first.
enum Msg {
    Batch {
        objects: Vec<EncodedObject>,
        commits: Vec<NewCommit>,
        /// Ask the store which objects exist (needed for exact stats on
        /// incremental imports); skipped on fresh imports.
        check_present: bool,
    },
    Tip { ant_ref: String, id: ObjectId },
}

pub async fn import(repo: &Path, backend: &Backend, opts: &ImportOptions) -> anyhow::Result<ImportStats> {
    let batch_objects = opts.batch_objects.max(1);
    let (tx, rx) = mpsc::channel::<Msg>(4);
    let producer = {
        let repo = repo.to_path_buf();
        let backend = backend.clone();
        let refs = opts.refs.clone();
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || produce(&repo, backend, handle, &refs, batch_objects, tx))
    };
    let mut stats = ImportStats::default();
    let written = write(backend, rx, &mut stats).await;
    let produced = producer.await?;
    written?;
    produced?;
    tracing::debug!(?stats, "git import finished");
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Writer (async side)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Counts {
    trees: u64,
    blobs: u64,
}

type InFlight = VecDeque<(JoinHandle<anyhow::Result<Counts>>, Vec<NewCommit>)>;

async fn write(backend: &Backend, mut rx: mpsc::Receiver<Msg>, stats: &mut ImportStats) -> anyhow::Result<()> {
    let mut inflight: InFlight = VecDeque::new();
    while let Some(msg) = rx.recv().await {
        match msg {
            Msg::Batch { objects, commits, check_present } => {
                let b = backend.clone();
                let task = tokio::spawn(async move { store_objects(&b, objects, check_present).await });
                inflight.push_back((task, commits));
                while inflight.len() > BATCHES_IN_FLIGHT {
                    finish_batch(backend, &mut inflight, stats).await?;
                }
            }
            Msg::Tip { ant_ref, id } => {
                while !inflight.is_empty() {
                    finish_batch(backend, &mut inflight, stats).await?;
                }
                let expected = backend.refs.get_ref(&ant_ref).await?;
                match backend.refs.cas_ref(&ant_ref, expected, Some(id)).await? {
                    CasOutcome::Updated => {}
                    CasOutcome::Mismatch { actual } => {
                        bail!("ref {ant_ref} changed during import (now {actual:?})")
                    }
                }
            }
        }
    }
    while !inflight.is_empty() {
        finish_batch(backend, &mut inflight, stats).await?;
    }
    Ok(())
}

/// Wait for the oldest batch's objects, then record its commits. Batches
/// complete strictly in order, so a commit is only recorded once everything it
/// can reference is stored.
async fn finish_batch(backend: &Backend, inflight: &mut InFlight, stats: &mut ImportStats) -> anyhow::Result<()> {
    let Some((task, commits)) = inflight.pop_front() else {
        return Ok(());
    };
    let counts = task.await.context("object writer task")??;
    stats.trees += counts.trees;
    stats.blobs += counts.blobs;
    if commits.is_empty() {
        return Ok(());
    }
    let mut mapping = Vec::with_capacity(commits.len());
    let mut records = Vec::with_capacity(commits.len());
    for c in commits {
        mapping.push((c.git.as_slice().to_vec(), c.node.id.as_bytes().to_vec().into()));
        records.push((c.node, c.changes));
    }
    stats.commits += records.len() as u64;
    backend.meta.put_commits(records).await?;
    // After the commits: a mapping must never point at an unrecorded commit.
    backend.meta.put_derived_many(COMMIT_KIND, mapping).await?;
    Ok(())
}

/// Store the objects the target does not already hold; only those are counted.
async fn store_objects(backend: &Backend, objects: Vec<EncodedObject>, check_present: bool) -> anyhow::Result<Counts> {
    let mut counts = Counts::default();
    if objects.is_empty() {
        return Ok(counts);
    }
    let missing = if check_present {
        let ids: Vec<ObjectId> = objects.iter().map(EncodedObject::id).collect();
        let present = backend.objects.has_many(&ids).await?;
        objects.into_iter().zip(present).filter_map(|(o, p)| (!p).then_some(o)).collect()
    } else {
        objects
    };
    for obj in &missing {
        match obj.kind() {
            ObjectKind::Blob => counts.blobs += 1,
            ObjectKind::Tree => counts.trees += 1,
            ObjectKind::Commit => {}
        }
    }
    if !missing.is_empty() {
        backend.objects.put_many(&missing).await?;
    }
    Ok(counts)
}

// ---------------------------------------------------------------------------
// Producer (blocking side)
// ---------------------------------------------------------------------------

fn produce(
    repo_path: &Path,
    backend: Backend,
    handle: tokio::runtime::Handle,
    refs: &[(String, String)],
    batch_objects: usize,
    tx: mpsc::Sender<Msg>,
) -> anyhow::Result<()> {
    let repo = gix::open(repo_path).with_context(|| format!("opening git repo {}", repo_path.display()))?;
    let mut p = Producer {
        repo,
        backend,
        handle,
        tx,
        batch_objects,
        objects: Vec::new(),
        commits: Vec::new(),
        memo: HashMap::new(),
        infos: HashMap::new(),
        trees: TreeCache::default(),
        incremental: false,
        current: None,
        empty: Arc::new(Vec::new()),
    };
    for (git_ref, ant_ref) in refs {
        let tip = p.resolve(git_ref)?;
        p.discover(tip)?;
        p.walk(tip)?;
        let id = p
            .known_commit(tip)
            .ok_or_else(|| anyhow!("{git_ref} was not converted"))?;
        p.send_batch()?;
        p.send(Msg::Tip { ant_ref: ant_ref.clone(), id })?;
    }
    p.send_batch()
}

/// One parsed tree entry (names already checked to be UTF-8).
struct RawEntry {
    mode: EntryMode,
    name: String,
    child: gix::ObjectId,
}

type Entries = Arc<Vec<RawEntry>>;

/// Two-generation cache of parsed source trees, bounded by entry count.
#[derive(Default)]
struct TreeCache {
    hot: HashMap<gix::ObjectId, Entries>,
    cold: HashMap<gix::ObjectId, Entries>,
    hot_entries: usize,
}

impl TreeCache {
    fn get(&mut self, id: &gix::ObjectId) -> Option<Entries> {
        if let Some(e) = self.hot.get(id) {
            return Some(e.clone());
        }
        let e = self.cold.remove(id)?;
        self.insert(*id, e.clone());
        Some(e)
    }

    fn insert(&mut self, id: gix::ObjectId, entries: Entries) {
        if self.hot_entries >= TREE_CACHE_ENTRIES {
            self.cold = std::mem::take(&mut self.hot);
            self.hot_entries = 0;
        }
        self.hot_entries += entries.len() + 1;
        self.hot.insert(id, entries);
    }
}

/// What the producer remembers about a converted (or known) commit.
#[derive(Clone, Copy)]
struct CommitInfo {
    /// Source root tree.
    git_tree: gix::ObjectId,
    generation: u64,
}

struct Producer {
    repo: gix::Repository,
    backend: Backend,
    handle: tokio::runtime::Handle,
    tx: mpsc::Sender<Msg>,
    batch_objects: usize,
    /// Converted objects not yet sent to the writer.
    objects: Vec<EncodedObject>,
    /// Converted commits not yet sent to the writer (parents-first order).
    commits: Vec<NewCommit>,
    /// SHA-1 (source) → SHA-256 (antimony) for trees, blobs and commits
    /// converted during this run, plus commits found already imported.
    memo: HashMap<gix::ObjectId, ObjectId>,
    /// Source commit → tree and generation, for first-parent changesets.
    infos: HashMap<gix::ObjectId, CommitInfo>,
    trees: TreeCache,
    /// A previously imported commit was found: the target is not empty.
    incremental: bool,
    /// Commit being converted (for error messages).
    current: Option<gix::ObjectId>,
    empty: Entries,
}

/// A commit parsed from git's raw encoding.
struct RawCommit {
    tree: gix::ObjectId,
    parents: Vec<gix::ObjectId>,
    author: Signature,
    committer: Signature,
    extra_headers: Vec<(String, Vec<u8>)>,
    message: Vec<u8>,
}

enum Frame {
    Enter(gix::ObjectId),
    /// Only the id: the commit is re-read when its turn comes, so the DFS
    /// stack never holds the parsed metadata of many commits at once.
    Exit(gix::ObjectId),
}

impl Producer {
    fn resolve(&self, name: &str) -> anyhow::Result<gix::ObjectId> {
        let mut reference = self.repo.find_reference(name).with_context(|| format!("resolving {name}"))?;
        Ok(reference.peel_to_id()?.detach())
    }

    fn read(&self, id: gix::ObjectId) -> anyhow::Result<Vec<u8>> {
        Ok(self.repo.find_object(id).with_context(|| format!("reading object {id}"))?.detach().data)
    }

    /// Antimony id of an already converted commit (this run, or found by
    /// [`Self::discover`] from an earlier import).
    fn known_commit(&self, id: gix::ObjectId) -> Option<ObjectId> {
        self.memo.get(&id).copied()
    }

    /// Find the commits reachable from `tip` that earlier imports already
    /// converted, and put them in `memo`. Reads ahead in batches (commits are
    /// checked with one `get_derived_many` per chunk, not one lookup each);
    /// everything below a known commit is known too and is never expanded.
    fn discover(&mut self, tip: gix::ObjectId) -> anyhow::Result<()> {
        let mut seen = HashSet::from([tip]);
        let mut queue = VecDeque::from([tip]);
        let mut chunk = PRECHECK_START;
        while !queue.is_empty() {
            let mut batch = Vec::with_capacity(chunk.min(queue.len()));
            while batch.len() < chunk {
                match queue.pop_front() {
                    Some(id) if !self.memo.contains_key(&id) => batch.push(id),
                    Some(_) => {}
                    None => break,
                }
            }
            let keys: Vec<&[u8]> = batch.iter().map(|id| id.as_slice()).collect();
            let found = self.handle.block_on(self.backend.meta.get_derived_many(COMMIT_KIND, &keys))?;
            for (id, value) in batch.into_iter().zip(found) {
                match value {
                    Some(bytes) => {
                        self.memo.insert(id, ObjectId::from_bytes(&bytes)?);
                        self.incremental = true;
                    }
                    None => {
                        for p in parse_commit(id, &self.read(id)?)?.parents {
                            if seen.insert(p) {
                                queue.push_back(p);
                            }
                        }
                    }
                }
            }
            chunk = (chunk * 2).min(PRECHECK_MAX);
        }
        Ok(())
    }

    fn send(&self, msg: Msg) -> anyhow::Result<()> {
        self.tx.blocking_send(msg).map_err(|_| anyhow!("import writer stopped"))
    }

    fn send_batch(&mut self) -> anyhow::Result<()> {
        if self.objects.is_empty() && self.commits.is_empty() {
            return Ok(());
        }
        let objects = std::mem::take(&mut self.objects);
        let commits = std::mem::take(&mut self.commits);
        self.send(Msg::Batch { objects, commits, check_present: self.incremental })
    }

    fn push_object(&mut self, obj: EncodedObject) -> anyhow::Result<()> {
        self.objects.push(obj);
        if self.objects.len() >= self.batch_objects {
            self.send_batch()?;
        }
        Ok(())
    }

    /// Post-order walk from `tip` that converts parents before children and
    /// stops at commits that were already imported.
    fn walk(&mut self, tip: gix::ObjectId) -> anyhow::Result<()> {
        let mut visited = HashSet::new();
        let mut stack = vec![Frame::Enter(tip)];
        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Enter(id) => {
                    if !visited.insert(id) || self.memo.contains_key(&id) {
                        continue;
                    }
                    let raw = parse_commit(id, &self.read(id)?)?;
                    stack.push(Frame::Exit(id));
                    for p in raw.parents.into_iter().rev() {
                        stack.push(Frame::Enter(p));
                    }
                }
                Frame::Exit(id) => self.convert_commit(id)?,
            }
        }
        Ok(())
    }

    /// Root tree and generation of an already converted parent commit.
    fn parent_info(&mut self, id: gix::ObjectId) -> anyhow::Result<CommitInfo> {
        if let Some(i) = self.infos.get(&id) {
            return Ok(*i);
        }
        // Imported by an earlier run: ask the store (once per boundary commit).
        let ant = self
            .known_commit(id)
            .ok_or_else(|| anyhow!("parent {id} was not converted"))?;
        let node = self
            .handle
            .block_on(self.backend.meta.get_commits(&[ant]))?
            .pop()
            .flatten()
            .ok_or_else(|| anyhow!("imported commit {ant} (source {id}) is not recorded"))?;
        let git_tree = parse_commit(id, &self.read(id)?)?.tree;
        let info = CommitInfo { git_tree, generation: node.generation };
        self.infos.insert(id, info);
        Ok(info)
    }

    fn convert_commit(&mut self, id: gix::ObjectId) -> anyhow::Result<()> {
        self.current = Some(id);
        let raw = parse_commit(id, &self.read(id)?)?;
        let tree = self.convert_tree(raw.tree, "")?;
        let mut parents = Vec::with_capacity(raw.parents.len());
        let mut generation = 0;
        let mut first = None;
        for (i, p) in raw.parents.iter().enumerate() {
            let a = self
                .known_commit(*p)
                .ok_or_else(|| anyhow!("parent {p} of {id} was not converted"))?;
            let info = self.parent_info(*p)?;
            generation = generation.max(info.generation);
            if i == 0 {
                first = Some(info.git_tree);
            }
            parents.push(a);
        }
        let changes = self.changes(first, raw.tree)?;
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
        let ant = obj.id();
        self.memo.insert(id, ant);
        self.infos.insert(id, CommitInfo { git_tree: raw.tree, generation: generation + 1 });
        let node = CommitNode { id: ant, tree, parents, generation: generation + 1, committer_time };
        self.commits.push(NewCommit { node, changes, git: id });
        self.push_object(obj)
    }

    /// Parsed entries of source tree `id` (cached). `path` is the directory,
    /// only used in error messages.
    fn tree_entries(&mut self, id: gix::ObjectId, path: &str) -> anyhow::Result<Entries> {
        if let Some(e) = self.trees.get(&id) {
            return Ok(e);
        }
        let raw = self.read(id)?;
        let hash_len = self.repo.object_hash().len_in_bytes();
        let mut entries = Vec::new();
        let mut rest = &raw[..];
        while !rest.is_empty() {
            let malformed = || anyhow!("malformed tree {id}");
            let sp = rest.iter().position(|&b| b == b' ').ok_or_else(malformed)?;
            let nul = sp + 1 + rest[sp + 1..].iter().position(|&b| b == 0).ok_or_else(malformed)?;
            let oid_end = nul + 1 + hash_len;
            if rest.len() < oid_end {
                bail!("malformed tree {id}");
            }
            let name = match std::str::from_utf8(&rest[sp + 1..nul]) {
                Ok(n) => n.to_string(),
                Err(_) => {
                    let commit = self.current.map_or_else(|| "?".to_string(), |c| c.to_string());
                    bail!(
                        "non-UTF-8 file name {:?} in directory {:?} (tree {id}, commit {commit}): \
                         antimony tree entry names must be UTF-8",
                        String::from_utf8_lossy(&rest[sp + 1..nul]),
                        if path.is_empty() { "/" } else { path },
                    );
                }
            };
            let mode = EntryMode::from_octal_lenient(&rest[..sp])?;
            let child = gix::ObjectId::from_bytes_or_panic(&rest[nul + 1..oid_end]);
            entries.push(RawEntry { mode, name, child });
            rest = &rest[oid_end..];
        }
        let entries = Arc::new(entries);
        self.trees.insert(id, entries.clone());
        Ok(entries)
    }

    fn convert_tree(&mut self, id: gix::ObjectId, path: &str) -> anyhow::Result<ObjectId> {
        if let Some(a) = self.memo.get(&id) {
            return Ok(*a);
        }
        let raw = self.tree_entries(id, path)?;
        let mut entries = Vec::with_capacity(raw.len());
        for e in raw.iter() {
            let child_id = match e.mode {
                EntryMode::Tree => {
                    let sub = if path.is_empty() { e.name.clone() } else { format!("{path}/{}", e.name) };
                    self.convert_tree(e.child, &sub)?
                }
                _ => self.leaf_id(e.mode, e.child)?,
            };
            entries.push(TreeEntry { mode: e.mode, name: e.name.clone(), id: child_id });
        }
        let tree = Tree::new(entries)?;
        let obj = EncodedObject::new(ObjectKind::Tree, &tree.encode());
        let ant = obj.id();
        self.memo.insert(id, ant);
        self.push_object(obj)?;
        Ok(ant)
    }

    /// Antimony id of a non-tree entry.
    fn leaf_id(&mut self, mode: EntryMode, child: gix::ObjectId) -> anyhow::Result<ObjectId> {
        match mode {
            // Submodule pointer: carried through, never fetched. The foreign
            // commit id is zero-padded to 32 bytes (lossless for SHA-1/256).
            EntryMode::Gitlink => {
                let mut raw = [0u8; 32];
                let n = child.as_bytes().len().min(32);
                raw[..n].copy_from_slice(&child.as_bytes()[..n]);
                Ok(ObjectId(raw))
            }
            _ => self.convert_blob(child),
        }
    }

    fn convert_blob(&mut self, id: gix::ObjectId) -> anyhow::Result<ObjectId> {
        if let Some(a) = self.memo.get(&id) {
            return Ok(*a);
        }
        let data = self.read(id)?;
        let obj = EncodedObject::new(ObjectKind::Blob, &data);
        let ant = obj.id();
        self.memo.insert(id, ant);
        self.push_object(obj)?;
        Ok(ant)
    }

    /// Non-tree paths that differ between two source trees (`old` = `None`
    /// for root commits), sorted by path: the same result as
    /// `ant_store::graph::diff_trees` on the converted trees, computed from the
    /// producer's own state. Identical subtrees (equal source ids) are skipped
    /// unread, and nothing is fetched from the store.
    fn changes(&mut self, old: Option<gix::ObjectId>, new: gix::ObjectId) -> anyhow::Result<Vec<PathChange>> {
        let mut out = Vec::new();
        let mut stack = vec![(RepoPath::root(), old, Some(new))];
        while let Some((dir, old, new)) = stack.pop() {
            if old == new {
                continue;
            }
            let old_e = match old {
                Some(id) => self.tree_entries(id, dir.as_str())?,
                None => self.empty.clone(),
            };
            let new_e = match new {
                Some(id) => self.tree_entries(id, dir.as_str())?,
                None => self.empty.clone(),
            };
            let old_m: HashMap<&str, &RawEntry> = old_e.iter().map(|e| (e.name.as_str(), e)).collect();
            let new_m: HashMap<&str, &RawEntry> = new_e.iter().map(|e| (e.name.as_str(), e)).collect();
            let mut names: Vec<&str> = old_m.keys().chain(new_m.keys()).copied().collect();
            names.sort_unstable();
            names.dedup();
            for name in names {
                let o = old_m.get(name).copied();
                let n = new_m.get(name).copied();
                if o.map(|e| (e.mode, e.child)) == n.map(|e| (e.mode, e.child)) {
                    continue;
                }
                let path = dir.join(name)?;
                let o_tree = o.filter(|e| e.mode.is_tree()).map(|e| e.child);
                let n_tree = n.filter(|e| e.mode.is_tree()).map(|e| e.child);
                let o_leaf = o.filter(|e| !e.mode.is_tree()).map(|e| (e.mode, e.child));
                let n_leaf = n.filter(|e| !e.mode.is_tree()).map(|e| (e.mode, e.child));
                if o_tree.is_some() || n_tree.is_some() {
                    stack.push((path.clone(), o_tree, n_tree));
                }
                if o_leaf != n_leaf {
                    let kind = match (o_leaf, n_leaf) {
                        (None, Some(_)) => ChangeKind::Added,
                        (Some(_), None) => ChangeKind::Deleted,
                        _ => ChangeKind::Modified,
                    };
                    let old = o_leaf.map(|(m, c)| Ok::<_, anyhow::Error>((m, self.leaf_id(m, c)?))).transpose()?;
                    let new = n_leaf.map(|(m, c)| Ok::<_, anyhow::Error>((m, self.leaf_id(m, c)?))).transpose()?;
                    out.push(PathChange { path, kind, old, new });
                }
            }
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Raw git commit parsing
// ---------------------------------------------------------------------------

fn parse_commit(id: gix::ObjectId, raw: &[u8]) -> anyhow::Result<RawCommit> {
    let bad = |what: &str| anyhow!("commit {id}: {what}");
    let (head, message) = match raw.windows(2).position(|w| w == b"\n\n") {
        Some(i) => (&raw[..i], &raw[i + 2..]),
        None => (raw, &raw[raw.len()..]),
    };

    // Fold continuation lines into the previous header, as ant-core does.
    let mut fields: Vec<(&[u8], Vec<u8>)> = Vec::new();
    for line in head.split(|&b| b == b'\n') {
        if let Some(cont) = line.strip_prefix(b" ") {
            let last = fields.last_mut().ok_or_else(|| bad("orphan continuation line"))?;
            last.1.push(b'\n');
            last.1.extend_from_slice(cont);
        } else {
            let (k, v) = match line.iter().position(|&b| b == b' ') {
                Some(i) => (&line[..i], &line[i + 1..]),
                None => (line, &b""[..]),
            };
            fields.push((k, v.to_vec()));
        }
    }

    let hex_id = |v: &[u8], what: &str| -> anyhow::Result<gix::ObjectId> {
        gix::ObjectId::from_hex(v).map_err(|_| bad(what))
    };
    let mut it = fields.into_iter().peekable();
    let tree = match it.next() {
        Some((b"tree", v)) => hex_id(&v, "bad tree id")?,
        _ => return Err(bad("missing tree")),
    };
    let mut parents = Vec::new();
    while matches!(it.peek(), Some((b"parent", _))) {
        if let Some((_, v)) = it.next() {
            parents.push(hex_id(&v, "bad parent id")?);
        }
    }
    let author = match it.next() {
        Some((b"author", v)) => parse_signature(id, &v)?,
        _ => return Err(bad("missing author")),
    };
    let committer = match it.next() {
        Some((b"committer", v)) => parse_signature(id, &v)?,
        _ => return Err(bad("missing committer")),
    };
    let extra_headers = it
        .map(|(k, v)| {
            let k = std::str::from_utf8(k).map_err(|_| bad("non-UTF-8 header name"))?;
            Ok((k.to_string(), v))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RawCommit { tree, parents, author, committer, extra_headers, message: message.to_vec() })
}

/// `Name <email> <unix-seconds> <+hhmm>`, same rules as `Signature` decoding in
/// ant-core, but name and email stay raw bytes.
fn parse_signature(id: gix::ObjectId, s: &[u8]) -> anyhow::Result<Signature> {
    let bad = || anyhow!("commit {id}: bad signature {:?}", String::from_utf8_lossy(s));
    let lt = s.iter().position(|&b| b == b'<').ok_or_else(bad)?;
    let gt = s.iter().rposition(|&b| b == b'>').ok_or_else(bad)?;
    if gt < lt {
        bail!(bad());
    }
    let mut name_end = lt;
    while name_end > 0 && s[name_end - 1] == b' ' {
        name_end -= 1;
    }
    let name = s[..name_end].to_vec();
    let email = s[lt + 1..gt].to_vec();
    let tail = std::str::from_utf8(&s[gt + 1..]).map_err(|_| bad())?;
    let mut rest = tail.split_whitespace();
    let time: i64 = rest.next().and_then(|t| t.parse().ok()).ok_or_else(bad)?;
    let tz = rest.next().ok_or_else(bad)?;
    if tz.len() != 5 || !tz.is_ascii() || !matches!(&tz[..1], "+" | "-") {
        bail!(bad());
    }
    let hh: i32 = tz[1..3].parse().map_err(|_| bad())?;
    let mm: i32 = tz[3..5].parse().map_err(|_| bad())?;
    let off = if &tz[..1] == "-" { -(hh * 60 + mm) } else { hh * 60 + mm };
    Ok(Signature { name, email, time, tz_offset_minutes: off })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::process::Command;

    use super::*;
    use ant_core::{ObjectId, RepoPath};
    use ant_store::graph::lookup_path;
    use ant_store::ObjectStoreExt;

    fn git(dir: &Path, args: &[&str]) -> String {
        git_env(dir, args, &[])
    }

    /// Like [`git`], with extra (raw-byte) environment overriding the defaults.
    fn git_env(dir: &Path, args: &[&str], extra: &[(&str, &[u8])]) -> String {
        use std::os::unix::ffi::OsStrExt;
        let out = Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_AUTHOR_NAME", "Ada")
            .env("GIT_AUTHOR_EMAIL", "ada@example.com")
            .env("GIT_COMMITTER_NAME", "Bob")
            .env("GIT_COMMITTER_EMAIL", "bob@example.com")
            .env("GIT_AUTHOR_DATE", "1700000000 +0530")
            .env("GIT_COMMITTER_DATE", "1700000100 -0700")
            .envs(extra.iter().map(|(k, v)| (*k, std::ffi::OsStr::from_bytes(v))))
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8(out.stdout).unwrap()
    }

    /// History with nested dirs, exec bit, symlink, rename, delete, a merge.
    fn make_repo(root: &Path) -> PathBuf {
        let r = root.join("src");
        std::fs::create_dir(&r).unwrap();
        git(&r, &["init", "-q", "-b", "main"]);
        std::fs::create_dir_all(r.join("a/b")).unwrap();
        std::fs::write(r.join("a/b/c.txt"), "c\n").unwrap();
        std::fs::write(r.join("run.sh"), "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(r.join("run.sh"), std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
        std::os::unix::fs::symlink("run.sh", r.join("link")).unwrap();
        git(&r, &["add", "-A"]);
        git(&r, &["commit", "-qm", "first\n\nbody line"]);
        git(&r, &["checkout", "-qb", "side"]);
        std::fs::write(r.join("side.txt"), "s\n").unwrap();
        git(&r, &["add", "-A"]);
        git(&r, &["commit", "-qm", "side work"]);
        git(&r, &["checkout", "-q", "main"]);
        git(&r, &["mv", "a/b/c.txt", "a/c.txt"]);
        git(&r, &["rm", "-q", "link"]);
        git(&r, &["commit", "-qm", "move and delete"]);
        git(&r, &["merge", "-q", "--no-edit", "side"]);
        r
    }

    /// git's own SHA-256 conversion as an oracle: commit subject → id.
    fn oracle(root: &Path, src: &Path) -> Vec<(String, String)> {
        oracle_with(root, src, &["fast-export", "--all"])
    }

    fn oracle_with(root: &Path, src: &Path, export_args: &[&str]) -> Vec<(String, String)> {
        let dst = root.join("oracle");
        std::fs::create_dir(&dst).unwrap();
        git(&dst, &["init", "-q", "-b", "main", "--object-format=sha256"]);
        let export = Command::new("git").args(export_args).current_dir(src).output().unwrap();
        assert!(export.status.success(), "{}", String::from_utf8_lossy(&export.stderr));
        let mut child = Command::new("git")
            .args(["fast-import", "--quiet"])
            .current_dir(&dst)
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write;
        child.stdin.take().unwrap().write_all(&export.stdout).unwrap();
        assert!(child.wait().unwrap().success());
        git(&dst, &["log", "--format=%H %s", "main"])
            .lines()
            .map(|l| {
                let (h, s) = l.split_once(' ').unwrap();
                (h.to_string(), s.to_string())
            })
            .collect()
    }

    #[tokio::test]
    async fn import_matches_git_sha256_conversion() {
        let tmp = tempfile::tempdir().unwrap();
        let src = make_repo(tmp.path());
        let expected = oracle(tmp.path(), &src);
        assert_eq!(expected.len(), 4);

        let b = Backend::in_memory();
        let stats = import(&src, &b, &ImportOptions::single("refs/heads/main", "main")).await.unwrap();
        assert_eq!(stats.commits, 4);

        let tip = b.refs.get_ref("main").await.unwrap().unwrap();
        assert_eq!(tip.to_hex(), expected[0].0, "tip id must equal git's sha256 conversion");
        for (hex, subject) in &expected {
            let id: ObjectId = hex.parse().unwrap();
            let c = b.objects.read_commit(&id).await.unwrap_or_else(|e| panic!("{subject}: {e}"));
            assert!(c.message.starts_with(subject.as_bytes()));
            assert!(b.meta.get_commits(&[id]).await.unwrap()[0].is_some(), "{subject} recorded");
        }

        let c = b.objects.read_commit(&tip).await.unwrap();
        assert_eq!(c.author.tz_offset_minutes, 330);
        assert_eq!(c.committer.tz_offset_minutes, -420);
        let f = lookup_path(b.objects.as_ref(), c.tree, &RepoPath::new("a/c.txt").unwrap()).await.unwrap().unwrap();
        assert_eq!(&b.objects.read_blob(&f.1).await.unwrap()[..], b"c\n");
        let node = b.meta.get_commits(&[tip]).await.unwrap().pop().unwrap().unwrap();
        assert_eq!(node.parents.len(), 2);
        assert_eq!(node.generation, 3);
    }

    #[tokio::test]
    async fn reimport_is_incremental() {
        let tmp = tempfile::tempdir().unwrap();
        let src = make_repo(tmp.path());
        let b = Backend::in_memory();
        let opts = ImportOptions::single("refs/heads/main", "main");
        import(&src, &b, &opts).await.unwrap();
        let again = import(&src, &b, &opts).await.unwrap();
        assert_eq!(again, ImportStats::default());

        std::fs::write(src.join("new.txt"), "n\n").unwrap();
        git(&src, &["add", "-A"]);
        git(&src, &["commit", "-qm", "later"]);
        let inc = import(&src, &b, &opts).await.unwrap();
        assert_eq!(inc.commits, 1);
        assert_eq!(inc.blobs, 1);
        let tip = b.refs.get_ref("main").await.unwrap().unwrap();
        assert!(b.objects.read_commit(&tip).await.unwrap().message.starts_with(b"later"));
    }

    /// Real histories carry latin1 names and non-UTF-8 messages (with an
    /// `encoding` header); they must import byte-identically.
    #[tokio::test]
    async fn non_utf8_commit_text_matches_git_sha256_conversion() {
        let tmp = tempfile::tempdir().unwrap();
        let r = tmp.path().join("src");
        std::fs::create_dir(&r).unwrap();
        git(&r, &["init", "-q", "-b", "main"]);
        let name: &[u8] = b"J\xf6rg M\xfcller";
        let email: &[u8] = b"j\xf6rg@example.com";
        let msg: &[u8] = b"caf\xe9 \xff subject\n\nbody \xe4\xf6\xfc\n";
        std::fs::write(r.join("f.txt"), "one\n").unwrap();
        git(&r, &["add", "-A"]);
        std::fs::write(tmp.path().join("msg"), msg).unwrap();
        let msg_file = tmp.path().join("msg");
        let msg_file = msg_file.to_str().unwrap();
        let env: &[(&str, &[u8])] = &[
            ("GIT_AUTHOR_NAME", name),
            ("GIT_AUTHOR_EMAIL", email),
            ("GIT_COMMITTER_NAME", b"Committer \xe9"),
        ];
        let commit = |dir: &Path| {
            git_env(dir, &["-c", "i18n.commitEncoding=ISO-8859-1", "commit", "-q", "-F", msg_file], env);
        };
        commit(&r);
        std::fs::write(r.join("f.txt"), "two\n").unwrap();
        git(&r, &["add", "-A"]);
        commit(&r);

        let expected = oracle_with(tmp.path(), &r, &["fast-export", "--all", "--reencode=no"]);
        assert_eq!(expected.len(), 2);

        let b = Backend::in_memory();
        import(&r, &b, &ImportOptions::single("refs/heads/main", "main")).await.unwrap();
        let tip = b.refs.get_ref("main").await.unwrap().unwrap();
        assert_eq!(tip.to_hex(), expected[0].0, "tip id must equal git's sha256 conversion");
        let c = b.objects.read_commit(&tip).await.unwrap();
        assert_eq!(c.author.name, name);
        assert_eq!(c.author.email, email);
        assert_eq!(c.committer.name, b"Committer \xe9");
        assert_eq!(c.message, msg);
        assert!(c.extra_headers.iter().any(|(k, v)| k == "encoding" && v == b"ISO-8859-1"));
        let parent: ObjectId = expected[1].0.parse().unwrap();
        assert!(b.objects.read_commit(&parent).await.is_ok());
    }

    #[tokio::test]
    async fn non_utf8_file_name_is_rejected_with_path_and_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let r = tmp.path().join("src");
        std::fs::create_dir_all(r.join("dir")).unwrap();
        git(&r, &["init", "-q", "-b", "main"]);
        use std::os::unix::ffi::OsStrExt;
        std::fs::write(r.join("dir").join(std::ffi::OsStr::from_bytes(b"bad-\xe9.txt")), "x").unwrap();
        git(&r, &["add", "-A"]);
        git(&r, &["commit", "-qm", "bad name"]);
        let head = git(&r, &["rev-parse", "HEAD"]);

        let b = Backend::in_memory();
        let err = import(&r, &b, &ImportOptions::single("refs/heads/main", "main")).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("non-UTF-8 file name"), "{msg}");
        assert!(msg.contains("bad-"), "{msg}");
        assert!(msg.contains("dir"), "{msg}");
        assert!(msg.contains(head.trim()), "{msg}");
    }

    /// The producer-computed generation and changeset equal what
    /// `graph::diff_trees` derives by re-reading the trees.
    #[tokio::test]
    async fn recorded_changesets_match_tree_diffs() {
        let tmp = tempfile::tempdir().unwrap();
        let src = make_repo(tmp.path());
        // Tiny batches: commits and their objects land in different batches.
        let mut opts = ImportOptions::single("refs/heads/main", "main");
        opts.batch_objects = 2;
        let b = Backend::in_memory();
        import(&src, &b, &opts).await.unwrap();

        let log = b.meta.tail(0, 100).await.unwrap();
        assert_eq!(log.len(), 4);
        for (_, id) in &log {
            let c = b.objects.read_commit(id).await.unwrap();
            let node = b.meta.get_commits(&[*id]).await.unwrap().pop().unwrap().unwrap();
            let first = match c.parents.first() {
                Some(p) => Some(b.objects.read_commit(p).await.unwrap().tree),
                None => None,
            };
            let expected = ant_store::graph::diff_trees(b.objects.as_ref(), first, Some(c.tree)).await.unwrap();
            assert_eq!(b.meta.get_changeset(id).await.unwrap().unwrap(), expected, "changeset of {id}");
            let parent_gens = b.meta.get_commits(&c.parents).await.unwrap();
            let parent_gen = parent_gens.iter().map(|p| p.as_ref().unwrap().generation).max().unwrap_or(0);
            assert_eq!(node.generation, parent_gen + 1);
            assert_eq!(node.committer_time, c.committer.time);
        }
    }
}
