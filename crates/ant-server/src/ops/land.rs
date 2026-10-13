//! Landing: advance a ref to include a commit, rebasing server-side when the
//! ref moved. This replaces a merge queue — conflicts are rare at file
//! granularity and are reported back to the agent to fix and retry.
//!
//! TASK (server-land): implement `land` and `rebase`.
//!
//! `rebase(b, commit, onto)`:
//! - `commit` must be recorded and have exactly one parent `P`; merge
//!   commits → `InvalidArgument`.
//! - Take `commit`'s recorded changeset (vs `P`). For each change at path:
//!   base = entry at path in P's tree, ours = entry in `onto`'s tree,
//!   theirs = change.new.
//!   * ours == base            → apply theirs (write or delete)
//!   * ours == theirs          → already applied, skip
//!   * base, ours, theirs all regular files (File/Executable) with
//!     `ours.mode == base.mode` or `theirs.mode == base.mode` (pick the
//!     changed mode), and all three blobs valid UTF-8 → 3-way text merge
//!     with `diffy::merge(base, ours, theirs)`; clean → write merged blob;
//!     conflict → path conflicts.
//!   * anything else (modify/delete, add/add with different content,
//!     symlinks, binary) → path conflicts.
//! - Structural conflicts when applying onto `onto` (e.g. `onto` turned a
//!   parent dir into a file) → path conflicts too.
//! - If any conflicts: return `Err(paths)` sorted, write nothing that
//!   matters (unreferenced objects are fine).
//! - Otherwise create the rebased commit with parent `[onto]`, the original
//!   author, committer and message preserved verbatim, via
//!   `changeset::create_changeset` (or equivalent tree building + write_commit).
//!   Return `Ok(new_commit_id)`.
//!
//! `land(b, ref_name, commit)`:
//! - `commit` must be recorded (else `NotFound`).
//! - ref absent → create it at `commit` (CAS expected=None).
//! - `commit` already an ancestor of tip → `Landed { new_tip: tip, rebased: false }`.
//! - tip is an ancestor of `commit` → fast-forward CAS, `rebased: false`.
//! - else `rebase(commit, tip)`; conflict → `Conflict { tip, paths }`;
//!   success → CAS tip→rebased, `rebased: true`.
//! - Concurrent requests are queued per backend/ref. A detached worker replays
//!   up to 64 in arrival order, then publishes their final tip with one CAS.
//!   Conflicts do not advance the tentative tip; replies retain each request's
//!   own resulting commit (or the tip against which it conflicted).
//! - On CAS mismatch, replay the whole batch from the new tip (rebasing the
//!   ORIGINAL commits again); after 32 failed attempts → `Aborted` for all.
//! - Landed-as mappings become durable after CAS and before replying or
//!   processing the next batch. Caller cancellation cannot cancel the worker.
//! - Must be safe under heavy concurrency: N concurrent lands of
//!   non-conflicting commits must all land, each exactly once.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};

use tokio::sync::oneshot;

use ant_core::{EncodedObject, EntryMode, Object, ObjectId, ObjectKind, RepoPath, Tree, TreeEntry};
#[cfg(test)]
use ant_store::graph::write_commit;
use ant_store::{
    Backend, CasOutcome, ChangeKind, CommitNode, ObjectStoreExt, PathChange, StoreError,
};

use super::{OpError, OpResult};
use crate::acl::{self, Access};
use ant_auth::Policy;
use ant_store::graph;

type Entry = (EntryMode, ObjectId);
pub(super) type Directories = BTreeMap<RepoPath, Tree>;

// Load only directories on changed paths, one batch per depth.
pub(super) async fn load_directories<const N: usize>(
    b: &Backend,
    roots: [ObjectId; N],
    paths: &[RepoPath],
) -> OpResult<[Directories; N]> {
    let mut needed = BTreeSet::from([RepoPath::root()]);
    for path in paths {
        let mut path = path.clone();
        needed.insert(path.clone());
        while let Some((parent, _)) = path.split_last() {
            needed.insert(parent.clone());
            path = parent;
        }
    }
    let mut pending: Vec<_> =
        roots.into_iter().enumerate().map(|(i, root)| (i, RepoPath::root(), root)).collect();
    let mut dirs: [Directories; N] = std::array::from_fn(|_| BTreeMap::new());
    while !pending.is_empty() {
        let ids: Vec<_> = pending.iter().map(|(_, _, id)| *id).collect();
        let objects = b.objects.get_many(&ids).await?;
        let mut next = Vec::new();
        for ((i, path, id), object) in pending.into_iter().zip(objects) {
            let object = object.ok_or(StoreError::NotFound(id))?;
            if object.kind() != ObjectKind::Tree {
                return Err(StoreError::WrongKind {
                    id,
                    expected: ObjectKind::Tree,
                    actual: object.kind(),
                }
                .into());
            }
            let tree = Tree::decode(object.body())?;
            for entry in tree.entries().iter().filter(|e| e.mode.is_tree()) {
                let child = path.join(&entry.name)?;
                if needed.contains(&child) {
                    next.push((i, child, entry.id));
                }
            }
            dirs[i].insert(path, tree);
        }
        pending = next;
    }
    Ok(dirs)
}

fn entry(dirs: &Directories, path: &RepoPath) -> Option<Entry> {
    let (parent, name) = path.split_last()?;
    dirs.get(&parent)?.get(name).map(|e| (e.mode, e.id))
}

fn deepest_first(dirs: &Directories) -> Vec<RepoPath> {
    let mut paths: Vec<_> = dirs.keys().cloned().collect();
    paths.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
    paths
}

fn prune_empty(dirs: &mut Directories) {
    for path in deepest_first(dirs) {
        if let Some((parent, name)) = path.split_last()
            && dirs.get(&path).is_some_and(Tree::is_empty)
        {
            dirs.remove(&path);
            if let Some(tree) = dirs.get_mut(&parent) {
                tree.remove(name);
            }
        }
    }
}

fn apply_write(dirs: &mut Directories, path: &RepoPath, value: Entry) -> OpResult<bool> {
    let Some((parent, name)) = path.split_last() else {
        return Ok(false);
    };
    let mut dir = RepoPath::root();
    for component in parent.components() {
        let child = dir.join(component)?;
        let tree = dirs.entry(dir).or_default();
        if let Some(existing) = tree.get(component) {
            if !existing.mode.is_tree() {
                return Ok(false);
            }
        } else {
            tree.upsert(TreeEntry {
                name: component.into(),
                mode: EntryMode::Tree,
                id: ObjectId::empty_tree(),
            })?;
        }
        dirs.entry(child.clone()).or_default();
        dir = child;
    }
    let tree = dirs.entry(parent).or_default();
    if tree.get(name).is_some_and(|e| e.mode.is_tree()) {
        return Ok(false);
    }
    tree.upsert(TreeEntry { name: name.into(), mode: value.0, id: value.1 })?;
    Ok(true)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LandOutcome {
    Landed { new_tip: ObjectId, rebased: bool },
    Conflict { tip: ObjectId, paths: Vec<RepoPath> },
}

pub const MAX_LAND_ATTEMPTS: usize = 32;

pub async fn rebase(
    b: &Backend,
    commit: ObjectId,
    onto: ObjectId,
) -> OpResult<Result<ObjectId, Vec<RepoPath>>> {
    rebase_tagged(b, None, commit, onto).await
}

/// [`rebase`]; under a policy, merged blobs and rebuilt trees are
/// zone-tagged (`acl::tag_written`) before they are stored.
async fn rebase_tagged(
    b: &Backend,
    policy: Option<&Policy>,
    commit: ObjectId,
    onto: ObjectId,
) -> OpResult<Result<ObjectId, Vec<RepoPath>>> {
    let nodes = b.meta.get_commits(&[commit, onto]).await?;
    let original = nodes[0].as_ref().ok_or_else(|| OpError::not_found_object(commit))?;
    if original.parents.len() != 1 {
        return Err(OpError::InvalidArgument("rebase requires exactly one parent".into()));
    }
    let target = nodes[1].as_ref().ok_or_else(|| OpError::not_found_object(onto))?;
    let parent = original.parents[0];
    let parent_node = b
        .meta
        .get_commits(&[parent])
        .await?
        .pop()
        .flatten()
        .ok_or_else(|| OpError::not_found_object(parent))?;
    let changes =
        b.meta.get_changeset(&commit).await?.ok_or_else(|| OpError::not_found_object(commit))?;
    let paths: Vec<_> = changes.iter().map(|c| c.path.clone()).collect();
    let [base_dirs, mut dirs] =
        load_directories(b, [parent_node.tree, target.tree], &paths).await?;
    let mut conflicts = BTreeSet::new();
    let mut edits = BTreeMap::new();
    let mut merges = Vec::new();
    let mut blob_ids = BTreeSet::new();
    for change in &changes {
        let base = entry(&base_dirs, &change.path);
        let ours = entry(&dirs, &change.path);
        let theirs = change.new;
        if ours == theirs {
            continue;
        }
        if ours == base {
            edits.insert(change.path.clone(), theirs);
            continue;
        }
        if let (Some(base), Some(ours), Some(theirs)) = (base, ours, theirs)
            && [base.0, ours.0, theirs.0]
                .into_iter()
                .all(|m| matches!(m, EntryMode::File | EntryMode::Executable))
            && (ours.0 == base.0 || theirs.0 == base.0)
        {
            blob_ids.extend([base.1, ours.1, theirs.1]);
            merges.push((change.path.clone(), base, ours, theirs));
        } else {
            conflicts.insert(change.path.clone());
        }
    }
    let blob_ids: Vec<_> = blob_ids.into_iter().collect();
    let mut blobs = BTreeMap::new();
    if !blob_ids.is_empty() {
        for (id, object) in blob_ids.iter().copied().zip(b.objects.get_many(&blob_ids).await?) {
            let object = object.ok_or(StoreError::NotFound(id))?;
            if object.kind() != ObjectKind::Blob {
                return Err(StoreError::WrongKind {
                    id,
                    expected: ObjectKind::Blob,
                    actual: object.kind(),
                }
                .into());
            }
            blobs.insert(id, object);
        }
    }
    let mut objects = Vec::new();
    // (path, id) of every new blob/tree, for zone tagging.
    let mut written = Vec::new();
    for (path, base, ours, theirs) in merges {
        let texts = [base.1, ours.1, theirs.1]
            .map(|id| blobs.get(&id).and_then(|blob| std::str::from_utf8(blob.body()).ok()));
        if let [Some(base_text), Some(ours_text), Some(theirs_text)] = texts
            && let Ok(merged) = diffy::merge(base_text, ours_text, theirs_text)
        {
            let object = EncodedObject::new(ObjectKind::Blob, merged.as_bytes());
            let mode = if ours.0 == base.0 { theirs.0 } else { ours.0 };
            written.push((path.clone(), object.id()));
            edits.insert(path, Some((mode, object.id())));
            objects.push(object);
        } else {
            conflicts.insert(path);
        }
    }
    let recorded_changes: Vec<_> = edits
        .iter()
        .filter_map(|(path, new)| {
            // Changesets describe leaves; replacing a pruned directory adds
            // a leaf here and records its children as separate deletions.
            let old = entry(&dirs, path).filter(|(mode, _)| !mode.is_tree());
            if old == *new {
                return None;
            }
            let kind = match (old, new) {
                (None, Some(_)) => ChangeKind::Added,
                (Some(_), None) => ChangeKind::Deleted,
                _ => ChangeKind::Modified,
            };
            Some(PathChange { path: path.clone(), kind, old, new: *new })
        })
        .collect();
    // Deleting first permits file/directory replacements within one changeset.
    for (path, value) in &edits {
        if value.is_none()
            && let Some((parent, name)) = path.split_last()
            && let Some(tree) = dirs.get_mut(&parent)
        {
            tree.remove(name);
        }
    }
    prune_empty(&mut dirs);
    for (path, value) in &edits {
        if let Some(value) = value
            && !apply_write(&mut dirs, path, *value)?
        {
            conflicts.insert(path.clone());
        }
    }
    if !conflicts.is_empty() {
        return Ok(Err(conflicts.into_iter().collect()));
    }
    let mut root = target.tree;
    for path in deepest_first(&dirs) {
        let tree = dirs
            .remove(&path)
            .ok_or_else(|| OpError::InvalidArgument(format!("missing directory {path}")))?;
        let object = Object::Tree(tree).encode();
        let id = object.id();
        if let Some((parent, name)) = path.split_last() {
            let parent_tree = dirs
                .get_mut(&parent)
                .ok_or_else(|| OpError::InvalidArgument(format!("missing directory {parent}")))?;
            if parent_tree.get(name).is_some_and(|e| e.id == id) {
                continue;
            }
            parent_tree.upsert(TreeEntry { name: name.into(), mode: EntryMode::Tree, id })?;
        } else {
            root = id;
            if root == target.tree {
                continue;
            }
        }
        written.push((path, id));
        objects.push(object);
    }
    let mut rebased = b.objects.read_commit(&commit).await?;
    rebased.tree = root;
    rebased.parents = vec![onto];
    let generation = target.generation + 1;
    let committer_time = rebased.committer.time;
    let object = Object::Commit(rebased).encode();
    let id = object.id();
    objects.push(object);
    if let Some(policy) = policy {
        acl::tag_written(b, policy, &written).await?;
    }
    b.objects.put_many(&objects).await?;
    b.meta
        .put_commit(
            CommitNode { id, tree: root, parents: vec![onto], generation, committer_time },
            recorded_changes,
        )
        .await?;
    Ok(Ok(id))
}

/// Derived-data kind mapping a commit id to the id it was landed as after a
/// server-side rebase. Makes retries of a land idempotent.
const LANDED_AS: &str = "land/landed-as/v1";

/// Longest unlanded stack `land` will replay.
pub const MAX_STACK: usize = 256;

/// Ancestry nodes are immutable. Keep them for one batch so each tentative
/// tip does not require rereading the preceding requests' entire history.
#[derive(Default)]
struct BatchState {
    nodes: HashMap<ObjectId, CommitNode>,
    mappings: HashMap<ObjectId, Option<bytes::Bytes>>,
    pending: BTreeMap<ObjectId, ObjectId>,
}

impl BatchState {
    async fn nodes(&mut self, b: &Backend, ids: &[ObjectId]) -> OpResult<()> {
        let missing: Vec<_> =
            ids.iter().copied().filter(|id| !self.nodes.contains_key(id)).collect();
        if !missing.is_empty() {
            for (id, node) in missing.iter().zip(b.meta.get_commits(&missing).await?) {
                self.nodes.insert(*id, node.ok_or_else(|| OpError::not_found_object(*id))?);
            }
        }
        Ok(())
    }

    async fn ancestor(
        &mut self,
        b: &Backend,
        ancestor: ObjectId,
        descendant: ObjectId,
    ) -> OpResult<bool> {
        if ancestor == descendant {
            return Ok(true);
        }
        self.nodes(b, &[ancestor]).await?;
        let generation = self.nodes[&ancestor].generation;
        let mut seen = HashSet::from([descendant]);
        let mut frontier = vec![descendant];
        while !frontier.is_empty() {
            self.nodes(b, &frontier).await?;
            let mut next = Vec::new();
            for id in frontier {
                let node = &self.nodes[&id];
                if node.generation <= generation {
                    continue;
                }
                for parent in &node.parents {
                    if *parent == ancestor {
                        return Ok(true);
                    }
                    if seen.insert(*parent) {
                        next.push(*parent);
                    }
                }
            }
            frontier = next;
        }
        Ok(false)
    }

    async fn already_landed(&mut self, b: &Backend, id: ObjectId, tip: ObjectId) -> OpResult<bool> {
        if self.ancestor(b, id, tip).await? {
            return Ok(true);
        }
        if let Some(landed) = self.pending.get(&id).copied() {
            return self.ancestor(b, landed, tip).await;
        }
        if let std::collections::hash_map::Entry::Vacant(slot) = self.mappings.entry(id) {
            let mapped = b.meta.get_derived(LANDED_AS, id.as_bytes()).await?;
            slot.insert(mapped);
        }
        let mapped = self.mappings[&id].as_deref().map(ObjectId::from_bytes).transpose()?;
        match mapped {
            Some(landed) => self.ancestor(b, landed, tip).await,
            None => Ok(false),
        }
    }

    /// The unlanded first-parent stack ending at `commit`, bottom first.
    async fn stack(
        &mut self,
        b: &Backend,
        commit: ObjectId,
        tip: ObjectId,
    ) -> OpResult<Vec<ObjectId>> {
        let mut stack = vec![commit];
        let mut cur = commit;
        loop {
            self.nodes(b, &[cur]).await?;
            let [parent] = self.nodes[&cur].parents[..] else {
                break;
            };
            if self.already_landed(b, parent, tip).await? {
                break;
            }
            if stack.len() >= MAX_STACK {
                return Err(OpError::InvalidArgument(format!(
                    "{commit} sits on more than {MAX_STACK} unlanded commits"
                )));
            }
            stack.push(parent);
            cur = parent;
        }
        stack.reverse();
        Ok(stack)
    }

    /// Decide one request against the tentative `tip`. `verified` is the
    /// request's own permission pre-check; anything it does not cover fails
    /// this request only, before it has any effect on the batch.
    async fn apply(
        &mut self,
        b: &Backend,
        commit: ObjectId,
        tip: Option<ObjectId>,
        verified: &Verified,
        policy: Option<&Policy>,
    ) -> OpResult<LandOutcome> {
        self.nodes(b, &[commit]).await?;
        let Some(tip) = tip else {
            // Creating a ref: no previous content to protect.
            return Ok(LandOutcome::Landed { new_tip: commit, rebased: false });
        };
        if self.already_landed(b, commit, tip).await? {
            return Ok(LandOutcome::Landed { new_tip: tip, rebased: false });
        }
        if self.ancestor(b, tip, commit).await? {
            verified.allows_fast_forward(tip)?;
            return Ok(LandOutcome::Landed { new_tip: commit, rebased: false });
        }
        let stack = self.stack(b, commit, tip).await?;
        verified.allows_replay(&stack)?;
        let mut onto = tip;
        let mut pairs = Vec::new();
        for original in stack {
            match rebase_tagged(b, policy, original, onto).await? {
                Ok(id) => {
                    pairs.push((original, id));
                    onto = id;
                }
                Err(paths) => return Ok(LandOutcome::Conflict { tip, paths }),
            }
        }
        // A conflicting stack must not make its successfully replayed prefix
        // visible to later requests or publish that prefix's mappings.
        self.pending.extend(pairs);
        Ok(LandOutcome::Landed { new_tip: onto, rebased: true })
    }
}

const MAX_BATCH: usize = 64;
type Reply = oneshot::Sender<OpResult<LandOutcome>>;
struct Request {
    commit: ObjectId,
    verified: Verified,
    policy: Option<Arc<Policy>>,
    reply: Reply,
}

/// What a land request's permission pre-check (run against the ref tip seen
/// before joining a batch) proved the caller may do.
#[derive(Debug, Clone)]
enum Verified {
    /// Open mode or admin.
    All,
    /// The ref did not exist: only creating it was checked.
    Creation,
    /// Every changeset on `commit`'s first-parent chain down to (excluding)
    /// `stop`, the first commit already landed (in the ref's history or mapped
    /// to a landed rebase), is writable by the caller. Covers replaying any
    /// of them, and fast-forwarding from any tip on that chain.
    Chain { set: HashSet<ObjectId>, stop: Option<ObjectId> },
    /// The tree diff from `from` to `commit` is writable (fast-forward only;
    /// e.g. merges whose first-parent changesets carry other people's work).
    Exact { from: ObjectId },
}

const RETRY: &str = "ref moved while checking permissions; retry";

impl Verified {
    fn allows_fast_forward(&self, tip: ObjectId) -> OpResult<()> {
        match self {
            Verified::All => Ok(()),
            Verified::Chain { set, stop } if *stop == Some(tip) || set.contains(&tip) => Ok(()),
            Verified::Exact { from } if *from == tip => Ok(()),
            _ => Err(OpError::Aborted(RETRY.into())),
        }
    }

    fn allows_replay(&self, stack: &[ObjectId]) -> OpResult<()> {
        match self {
            Verified::All => Ok(()),
            Verified::Chain { set, .. } if stack.iter().all(|c| set.contains(c)) => Ok(()),
            // Includes `Exact` (only a fast-forward was verified): a retry
            // re-verifies against the new tip.
            _ => Err(OpError::Aborted(RETRY.into())),
        }
    }
}

/// Per-request permission pre-check for `land_as`, run before the request
/// joins a batch (so a denied request never touches anyone else's).
async fn verify_land(
    b: &Backend,
    access: &Access,
    ref_name: &str,
    commit: ObjectId,
) -> OpResult<Verified> {
    if !access.policy.can_land(&access.principal, ref_name) {
        return Err(OpError::PermissionDenied(format!("cannot land onto {ref_name}")));
    }
    if access.is_admin() {
        return Ok(Verified::All);
    }
    let mut state = BatchState::default();
    state.nodes(b, &[commit]).await?;
    let Some(tip) = b.refs.get_ref(ref_name).await? else {
        return Ok(Verified::Creation);
    };
    let mut chain = Vec::new();
    let mut stop = None;
    let mut complete = true;
    let mut cur = Some(commit);
    while let Some(c) = cur {
        if state.already_landed(b, c, tip).await? {
            stop = Some(c);
            break;
        }
        if chain.len() >= MAX_STACK {
            complete = false;
            break;
        }
        state.nodes(b, &[c]).await?;
        cur = state.nodes[&c].parents.first().copied();
        chain.push(c);
    }
    let mut verdict =
        Err(OpError::PermissionDenied(format!("more than {MAX_STACK} commits to verify")));
    if complete {
        verdict = Ok(());
        let sets = b.meta.get_changesets(&chain).await?;
        for (id, set) in chain.iter().zip(sets) {
            let set = set.ok_or_else(|| OpError::not_found_object(*id))?;
            verdict = access.check_writable(set.iter().map(|c| &c.path));
            if verdict.is_err() {
                break;
            }
        }
    }
    if !state.ancestor(b, tip, commit).await? {
        // Will be replayed: every replayed changeset was checked above.
        verdict?;
        return Ok(Verified::Chain { set: chain.into_iter().collect(), stop });
    }
    if verdict.is_ok() && (stop == Some(tip) || chain.contains(&tip)) {
        // Fast-forward along the first-parent chain: its changesets compose
        // to exactly the tree diff.
        return Ok(Verified::Chain { set: chain.into_iter().collect(), stop });
    }
    // Fast-forward through history the changesets do not describe relative
    // to `tip` (tip reached via a merge's second parent, or merges carrying
    // other people's changes): what changes is the trees' net diff.
    state.nodes(b, &[tip]).await?;
    let (old, new) = (state.nodes[&tip].tree, state.nodes[&commit].tree);
    let changes = graph::diff_trees(b.objects.as_ref(), Some(old), Some(new)).await?;
    access.check_writable(changes.iter().map(|c| &c.path))?;
    Ok(Verified::Exact { from: tip })
}

// Include all backend identities: independent repositories named "main" must
// never share requests. The worker owns the Backend while the key is live.
#[derive(Clone, Hash, PartialEq, Eq)]
struct QueueKey(usize, usize, usize, String);
type Queues = HashMap<QueueKey, VecDeque<Request>>;
static QUEUES: LazyLock<Mutex<Queues>> = LazyLock::new(Default::default);

struct Worker {
    key: QueueKey,
    active: bool,
}

impl Drop for Worker {
    fn drop(&mut self) {
        if self.active {
            // Panic/runtime shutdown closes every queued reply. In-flight
            // replies are owned by the task and close when its future drops.
            QUEUES.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.key);
        }
    }
}

impl Worker {
    async fn run(mut self, b: Backend) {
        // Coalesce callers that became runnable together, without a timer.
        tokio::task::yield_now().await;
        loop {
            let batch: Vec<_> = {
                let mut queues = QUEUES.lock().unwrap_or_else(|e| e.into_inner());
                let Some(queue) = queues.get_mut(&self.key) else {
                    return;
                };
                if queue.is_empty() {
                    queues.remove(&self.key);
                    self.active = false;
                    return;
                }
                let count = queue.len().min(MAX_BATCH);
                queue.drain(..count).collect()
            };
            match land_batch(&b, &self.key.3, &batch).await {
                Ok(outcomes) => {
                    for (request, outcome) in batch.into_iter().zip(outcomes) {
                        let _ = request.reply.send(outcome);
                    }
                }
                Err(error) => {
                    let message = error.to_string();
                    for request in batch {
                        let _ = request.reply.send(Err(OpError::Aborted(message.clone())));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
fn enqueue(b: &Backend, ref_name: &str, commit: ObjectId) -> oneshot::Receiver<OpResult<LandOutcome>> {
    enqueue_checked(b, ref_name, commit, Verified::All, None)
}

/// Queue a request already checked by `verify_land` (or `All` in open mode).
fn enqueue_checked(
    b: &Backend,
    ref_name: &str,
    commit: ObjectId,
    verified: Verified,
    policy: Option<Arc<Policy>>,
) -> oneshot::Receiver<OpResult<LandOutcome>> {
    let key = QueueKey(
        Arc::as_ptr(&b.objects) as *const () as usize,
        Arc::as_ptr(&b.refs) as *const () as usize,
        Arc::as_ptr(&b.meta) as *const () as usize,
        ref_name.to_owned(),
    );
    let (reply, receiver) = oneshot::channel();
    let mut queues = QUEUES.lock().unwrap_or_else(|e| e.into_inner());
    let lead = !queues.contains_key(&key);
    queues.entry(key.clone()).or_default().push_back(Request { commit, verified, policy, reply });
    if lead {
        tokio::spawn(Worker { key, active: true }.run(b.clone()));
    }
    receiver
}

async fn land_batch(
    b: &Backend,
    ref_name: &str,
    batch: &[Request],
) -> OpResult<Vec<OpResult<LandOutcome>>> {
    let mut start = b.refs.get_ref(ref_name).await?;
    let mut state = BatchState::default();
    let ids: Vec<_> = batch.iter().map(|r| r.commit).collect();
    for node in b.meta.get_commits(&ids).await?.into_iter().flatten() {
        state.nodes.insert(node.id, node);
    }
    let keys: Vec<_> = ids.iter().map(|id| id.as_bytes().as_slice()).collect();
    for _ in 0..MAX_LAND_ATTEMPTS {
        for (id, mapped) in ids.iter().zip(b.meta.get_derived_many(LANDED_AS, &keys).await?) {
            state.mappings.insert(*id, mapped);
        }
        let mut tip = start;
        let mut outcomes = Vec::with_capacity(batch.len());
        for request in batch {
            let outcome = state
                .apply(b, request.commit, tip, &request.verified, request.policy.as_deref())
                .await;
            if let Ok(LandOutcome::Landed { new_tip, .. }) = &outcome {
                tip = Some(*new_tip);
            }
            outcomes.push(outcome);
        }
        // Read-only batches may linearize at the initial read. Otherwise even
        // conflict replies wait for the CAS: their tentative tips must exist
        // in the published history.
        if tip == start {
            return Ok(outcomes);
        }
        match b.refs.cas_ref(ref_name, start, tip).await? {
            CasOutcome::Updated => {
                // MetaStore has no bulk derived-data write. Bound concurrent
                // writes and finish all of them before replying/starting a
                // new batch, even if one write fails.
                use futures::{StreamExt, stream};
                let writes: Vec<_> = stream::iter(state.pending)
                    .map(|(original, landed)| async move {
                        b.meta
                            .put_derived(
                                LANDED_AS,
                                original.as_bytes(),
                                landed.as_bytes().to_vec().into(),
                            )
                            .await
                    })
                    .buffer_unordered(16)
                    .collect()
                    .await;
                for result in writes {
                    result?;
                }
                return Ok(outcomes);
            }
            CasOutcome::Mismatch { actual } => {
                start = actual;
                state.pending.clear();
                // Another process may have published new landed-as mappings.
                state.mappings.clear();
            }
        }
    }
    Err(OpError::Aborted(format!(
        "ref {ref_name} changed during {MAX_LAND_ATTEMPTS} land attempts"
    )))
}

pub async fn land(b: &Backend, ref_name: &str, commit: ObjectId) -> OpResult<LandOutcome> {
    land_as(b, None, ref_name, commit).await
}

/// [`land`] on behalf of `access` (`None` = open mode). The caller must be
/// allowed to land onto `ref_name` and to write every path changed by every
/// commit that gets replayed (or, for a fast-forward, by the net diff).
/// Checked per request before it joins a group-commit batch.
pub async fn land_as(
    b: &Backend,
    access: Option<&Access>,
    ref_name: &str,
    commit: ObjectId,
) -> OpResult<LandOutcome> {
    ant_store::validate_ref_name(ref_name)?;
    let (verified, policy) = match access {
        Some(a) => (verify_land(b, a, ref_name, commit).await?, Some(a.policy.clone())),
        None => (Verified::All, None),
    };
    enqueue_checked(b, ref_name, commit, verified, policy)
        .await
        .map_err(|_| OpError::Aborted("land worker stopped before replying".into()))?
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::ops::OpError;
    use crate::ops::changeset::tests::{commit, delete, read, write};
    use ant_store::graph::is_ancestor;
    use ant_store::ObjectStoreExt;

    async fn tip(b: &Backend) -> ObjectId {
        b.refs.get_ref("main").await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn create_and_fast_forward() {
        let b = Backend::in_memory();
        let c1 = commit(&b, None, vec![write("f", "1")]).await.unwrap();
        assert_eq!(land(&b, "main", c1).await.unwrap(), LandOutcome::Landed { new_tip: c1, rebased: false });
        let c2 = commit(&b, Some(c1), vec![write("f", "2")]).await.unwrap();
        assert_eq!(land(&b, "main", c2).await.unwrap(), LandOutcome::Landed { new_tip: c2, rebased: false });
        // Re-landing an ancestor is a no-op.
        assert_eq!(land(&b, "main", c1).await.unwrap(), LandOutcome::Landed { new_tip: c2, rebased: false });
        assert_eq!(tip(&b).await, c2);
    }

    #[tokio::test]
    async fn rebase_disjoint_paths() {
        let b = Backend::in_memory();
        let base = commit(&b, None, vec![write("a", "a"), write("b", "b")]).await.unwrap();
        land(&b, "main", base).await.unwrap();
        let x = commit(&b, Some(base), vec![write("a", "A")]).await.unwrap();
        let y = commit(&b, Some(base), vec![write("b", "B"), write("new/file", "n")]).await.unwrap();
        land(&b, "main", x).await.unwrap();
        let out = land(&b, "main", y).await.unwrap();
        let LandOutcome::Landed { new_tip, rebased: true } = out else { panic!("{out:?}") };
        assert_eq!(read(&b, new_tip, "a").await.as_deref(), Some("A"));
        assert_eq!(read(&b, new_tip, "b").await.as_deref(), Some("B"));
        assert_eq!(read(&b, new_tip, "new/file").await.as_deref(), Some("n"));
        let rebased = b.objects.read_commit(&new_tip).await.unwrap();
        let orig = b.objects.read_commit(&y).await.unwrap();
        assert_eq!(rebased.parents, vec![x]);
        assert_eq!(rebased.author, orig.author);
        assert_eq!(rebased.message, orig.message);
        assert!(is_ancestor(&b, x, new_tip).await.unwrap());
    }

    #[tokio::test]
    async fn three_way_text_merge_same_file() {
        let b = Backend::in_memory();
        let text = "l1\nl2\nl3\nl4\nl5\nl6\n";
        let base = commit(&b, None, vec![write("f", text)]).await.unwrap();
        land(&b, "main", base).await.unwrap();
        let x = commit(&b, Some(base), vec![write("f", "L1\nl2\nl3\nl4\nl5\nl6\n")]).await.unwrap();
        let y = commit(&b, Some(base), vec![write("f", "l1\nl2\nl3\nl4\nl5\nL6\n")]).await.unwrap();
        land(&b, "main", x).await.unwrap();
        let LandOutcome::Landed { new_tip, .. } = land(&b, "main", y).await.unwrap() else { panic!() };
        assert_eq!(read(&b, new_tip, "f").await.as_deref(), Some("L1\nl2\nl3\nl4\nl5\nL6\n"));
    }

    #[tokio::test]
    async fn conflicts_are_reported_not_landed() {
        let b = Backend::in_memory();
        let base = commit(&b, None, vec![write("f", "x\n"), write("g", "g\n"), write("h", "h\n")]).await.unwrap();
        land(&b, "main", base).await.unwrap();
        let x = commit(&b, Some(base), vec![write("f", "ours\n"), delete("g"), write("h", "same\n")]).await.unwrap();
        let y = commit(&b, Some(base), vec![write("f", "theirs\n"), write("g", "mod\n"), write("h", "same\n")])
            .await
            .unwrap();
        land(&b, "main", x).await.unwrap();
        let out = land(&b, "main", y).await.unwrap();
        let paths: Vec<_> = match out {
            LandOutcome::Conflict { tip: t, paths } => {
                assert_eq!(t, x);
                paths.iter().map(|p| p.as_str().to_string()).collect()
            }
            other => panic!("{other:?}"),
        };
        // h: both sides made the identical change → not a conflict.
        assert_eq!(paths, ["f", "g"]);
        assert_eq!(tip(&b).await, x);
    }

    #[tokio::test]
    async fn structural_conflict() {
        let b = Backend::in_memory();
        let base = commit(&b, None, vec![write("p", "file")]).await.unwrap();
        land(&b, "main", base).await.unwrap();
        let x = commit(&b, Some(base), vec![delete("p"), write("p/q", "dir now")]).await.unwrap();
        let y = commit(&b, Some(base), vec![write("p2", "unrelated"), write("p", "edited")]).await.unwrap();
        land(&b, "main", x).await.unwrap();
        assert!(matches!(land(&b, "main", y).await.unwrap(), LandOutcome::Conflict { .. }));
    }

    #[tokio::test]
    async fn rejects_unrecorded_and_merge_commits() {
        let b = Backend::in_memory();
        assert!(matches!(land(&b, "main", ObjectId([1; 32])).await, Err(OpError::NotFound(_))));
        let r = commit(&b, None, vec![write("f", "1")]).await.unwrap();
        let a = commit(&b, Some(r), vec![write("a", "1")]).await.unwrap();
        let c = commit(&b, Some(r), vec![write("c", "1")]).await.unwrap();
        let m = ant_store::graph::write_commit(
            &b,
            &ant_core::Commit {
                tree: b.objects.read_commit(&a).await.unwrap().tree,
                parents: vec![a, c],
                author: crate::ops::changeset::tests::sig(),
                committer: crate::ops::changeset::tests::sig(),
                extra_headers: vec![],
                message: "merge".into(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(rebase(&b, m, r).await, Err(OpError::InvalidArgument(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_lands_all_succeed() {
        let b = Backend::in_memory();
        let base = commit(&b, None, vec![write("seed", "s")]).await.unwrap();
        land(&b, "main", base).await.unwrap();
        const N: usize = 24;
        let mut commits = Vec::new();
        for i in 0..N {
            commits.push(commit(&b, Some(base), vec![write(&format!("agent{i}/out"), "done")]).await.unwrap());
        }
        let b = Arc::new(b);
        let mut tasks = Vec::new();
        for c in commits {
            let b = b.clone();
            tasks.push(tokio::spawn(async move { land(&b, "main", c).await }));
        }
        for t in tasks {
            assert!(matches!(t.await.unwrap().unwrap(), LandOutcome::Landed { .. }));
        }
        let t = tip(&b).await;
        for i in 0..N {
            assert_eq!(read(&b, t, &format!("agent{i}/out")).await.as_deref(), Some("done"), "agent{i}");
        }
        // Linear first-parent history of N+1 commits.
        let node = b.meta.get_commits(&[t]).await.unwrap().pop().unwrap().unwrap();
        assert_eq!(node.generation as usize, N + 1);
    }
}

#[cfg(test)]
mod independent_tests {
    use super::*;
    use crate::ops::changeset::tests::sig;
    use ant_core::Commit;
    use ant_store::graph::{build_tree, lookup_path};

    async fn snapshot(b: &Backend, parent: Option<ObjectId>, files: &[(&str, &[u8])]) -> ObjectId {
        let tree = build_tree(b.objects.as_ref(), files).await.unwrap();
        write_commit(
            b,
            &Commit {
                tree,
                parents: parent.into_iter().collect(),
                author: sig(),
                committer: sig(),
                extra_headers: vec![],
                message: "snapshot\n".into(),
            },
        )
        .await
        .unwrap()
    }

    async fn contents(b: &Backend, commit: ObjectId, path: &str) -> Option<Vec<u8>> {
        let tree = b.objects.read_commit(&commit).await.unwrap().tree;
        let (_, id) =
            lookup_path(b.objects.as_ref(), tree, &RepoPath::new(path).unwrap()).await.unwrap()?;
        Some(b.objects.read_blob(&id).await.unwrap().to_vec())
    }

    #[tokio::test]
    async fn fast_forward_and_validation() {
        let b = Backend::in_memory();
        let missing = ObjectId([19; 32]);
        assert!(matches!(land(&b, "main", missing).await, Err(OpError::NotFound(_))));
        assert!(matches!(rebase(&b, missing, missing).await, Err(OpError::NotFound(_))));
        let root = snapshot(&b, None, &[("f", b"one")]).await;
        assert!(matches!(rebase(&b, root, root).await, Err(OpError::InvalidArgument(_))));
        assert_eq!(
            land(&b, "main", root).await.unwrap(),
            LandOutcome::Landed { new_tip: root, rebased: false }
        );
        let next = snapshot(&b, Some(root), &[("f", b"two")]).await;
        assert_eq!(
            land(&b, "main", next).await.unwrap(),
            LandOutcome::Landed { new_tip: next, rebased: false }
        );
        assert_eq!(
            land(&b, "main", root).await.unwrap(),
            LandOutcome::Landed { new_tip: next, rebased: false }
        );
        assert!(matches!(rebase(&b, next, missing).await, Err(OpError::NotFound(_))));
    }

    #[tokio::test]
    async fn text_merge_preserves_metadata_and_mode() {
        let b = Backend::in_memory();
        let base = snapshot(&b, None, &[("f", b"a\nb\nc\nd\ne\n")]).await;
        let ours = snapshot(&b, Some(base), &[("f", b"A\nb\nc\nd\ne\n")]).await;
        let theirs = snapshot(&b, Some(base), &[("f", b"a\nb\nc\nd\nE\n")]).await;
        let mut original = b.objects.read_commit(&theirs).await.unwrap();
        let mut tree = b.objects.read_tree(&original.tree).await.unwrap();
        let mut file = tree.get("f").unwrap().clone();
        file.mode = EntryMode::Executable;
        tree.upsert(file).unwrap();
        original.tree = b.objects.put(Object::Tree(tree).encode()).await.unwrap();
        original.committer.time += 42;
        original.message = "verbatim\n\nmessage".into();
        let theirs = write_commit(&b, &original).await.unwrap();
        let merged = rebase(&b, theirs, ours).await.unwrap().unwrap();
        assert_eq!(contents(&b, merged, "f").await.unwrap(), b"A\nb\nc\nd\nE\n");
        let actual = b.objects.read_commit(&merged).await.unwrap();
        assert_eq!(actual.author, original.author);
        assert_eq!(actual.committer, original.committer);
        assert_eq!(actual.message, original.message);
        assert_eq!(actual.parents, [ours]);
        assert_eq!(
            b.objects.read_tree(&actual.tree).await.unwrap().get("f").unwrap().mode,
            EntryMode::Executable
        );
    }

    #[tokio::test]
    async fn sorted_conflicts_leave_ref_and_log_unchanged() {
        let b = Backend::in_memory();
        let base = snapshot(&b, None, &[("z", b"old"), ("bin", b"\xff"), ("p/q", b"q")]).await;
        let ours =
            snapshot(&b, Some(base), &[("z", b"ours"), ("bin", b"\xfe"), ("p", b"file")]).await;
        let theirs = snapshot(
            &b,
            Some(base),
            &[("z", b"theirs"), ("bin", b"\xfd"), ("p/q", b"q"), ("p/new", b"new")],
        )
        .await;
        land(&b, "main", ours).await.unwrap();
        let log = b.meta.tail(0, 100).await.unwrap();
        assert_eq!(
            land(&b, "main", theirs).await.unwrap(),
            LandOutcome::Conflict {
                tip: ours,
                paths: ["bin", "p/new", "z"].map(|p| RepoPath::new(p).unwrap()).to_vec(),
            }
        );
        assert_eq!(b.refs.get_ref("main").await.unwrap(), Some(ours));
        assert_eq!(b.meta.tail(0, 100).await.unwrap(), log);
    }

    #[tokio::test]
    async fn replacements_deletions_and_identical_changes() {
        let b = Backend::in_memory();
        let base = snapshot(&b, None, &[("p", b"file"), ("d/q", b"q"), ("same", b"old")]).await;
        let ours = snapshot(
            &b,
            Some(base),
            &[("p", b"file"), ("d/q", b"q"), ("same", b"new"), ("other", b"ours")],
        )
        .await;
        let theirs =
            snapshot(&b, Some(base), &[("p/q", b"nested"), ("d", b"file"), ("same", b"new")]).await;
        let merged = rebase(&b, theirs, ours).await.unwrap().unwrap();
        for (path, expected) in
            [("p/q", "nested"), ("d", "file"), ("same", "new"), ("other", "ours")]
        {
            assert_eq!(contents(&b, merged, path).await.unwrap(), expected.as_bytes());
        }
        let tree = b.objects.read_commit(&merged).await.unwrap().tree;
        assert!(
            lookup_path(b.objects.as_ref(), tree, &RepoPath::new("d/q").unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_disjoint_lands() {
        let b = Backend::in_memory();
        let root = snapshot(&b, None, &[("seed", b"s")]).await;
        land(&b, "main", root).await.unwrap();
        let mut commits = Vec::new();
        for i in 0..24 {
            commits.push(
                snapshot(&b, Some(root), &[("seed", b"s"), (&format!("agent{i}/out"), b"done")])
                    .await,
            );
        }
        let mut tasks = Vec::new();
        for id in commits {
            let b = b.clone();
            tasks.push(tokio::spawn(async move { land(&b, "main", id).await }));
        }
        for task in tasks {
            assert!(matches!(task.await.unwrap().unwrap(), LandOutcome::Landed { .. }));
        }
        let tip = b.refs.get_ref("main").await.unwrap().unwrap();
        for i in 0..24 {
            assert_eq!(contents(&b, tip, &format!("agent{i}/out")).await.unwrap(), b"done");
        }
        assert_eq!(b.meta.get_commits(&[tip]).await.unwrap()[0].as_ref().unwrap().generation, 25);
    }

    #[tokio::test]
    async fn symlink_and_simultaneous_mode_changes_conflict() {
        for mode in [EntryMode::Symlink, EntryMode::Executable] {
            let b = Backend::in_memory();
            let base = snapshot(&b, None, &[("f", b"a\nb\nc\nd\ne\n")]).await;
            let mut sides = Vec::new();
            for text in [b"A\nb\nc\nd\ne\n", b"a\nb\nc\nd\nE\n"] {
                let blob = EncodedObject::new(ObjectKind::Blob, text);
                let id = blob.id();
                b.objects.put(blob).await.unwrap();
                let tree = ant_store::graph::write_tree(
                    b.objects.as_ref(),
                    vec![TreeEntry { name: "f".into(), mode, id }],
                )
                .await
                .unwrap();
                sides.push(
                    write_commit(
                        &b,
                        &Commit {
                            tree,
                            parents: vec![base],
                            author: sig(),
                            committer: sig(),
                            extra_headers: vec![],
                            message: "side".into(),
                        },
                    )
                    .await
                    .unwrap(),
                );
            }
            assert_eq!(
                rebase(&b, sides[0], sides[1]).await.unwrap(),
                Err(vec![RepoPath::new("f").unwrap()])
            );
        }
    }

    struct ContendedRefs {
        attempts: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ant_store::RefStore for ContendedRefs {
        async fn get_ref(&self, _: &str) -> ant_store::Result<Option<ObjectId>> {
            Ok(None)
        }
        async fn cas_ref(
            &self,
            _: &str,
            _: Option<ObjectId>,
            _: Option<ObjectId>,
        ) -> ant_store::Result<CasOutcome> {
            self.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(CasOutcome::Mismatch { actual: None })
        }
        async fn list_refs(&self, _: &str) -> ant_store::Result<Vec<(String, ObjectId)>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn aborts_after_exact_retry_limit() {
        let mut b = Backend::in_memory();
        let refs = std::sync::Arc::new(ContendedRefs { attempts: 0.into() });
        b.refs = refs.clone();
        let root = snapshot(&b, None, &[]).await;
        assert!(matches!(land(&b, "main", root).await, Err(OpError::Aborted(_))));
        assert_eq!(refs.attempts.load(std::sync::atomic::Ordering::SeqCst), MAX_LAND_ATTEMPTS);
    }
}

#[cfg(test)]
mod group_tests {
    use super::*;
    use crate::ops::changeset::tests::{commit, read, write};
    use ant_store::RefStore;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{Notify, Semaphore};
    use tokio::time::timeout;

    const DEADLINE: Duration = Duration::from_secs(20);

    struct ObservedRefs {
        inner: Arc<dyn RefStore>,
        calls: AtomicUsize,
        block: AtomicBool,
        entered: Notify,
        release: Semaphore,
        move_to: Mutex<Option<ObjectId>>,
        always_mismatch: AtomicBool,
    }

    impl ObservedRefs {
        fn install(b: &mut Backend) -> Arc<Self> {
            let refs = Arc::new(Self {
                inner: b.refs.clone(),
                calls: AtomicUsize::new(0),
                block: AtomicBool::new(false),
                entered: Notify::new(),
                release: Semaphore::new(0),
                move_to: Mutex::new(None),
                always_mismatch: AtomicBool::new(false),
            });
            b.refs = refs.clone();
            refs
        }
    }

    #[async_trait::async_trait]
    impl RefStore for ObservedRefs {
        async fn get_ref(&self, name: &str) -> ant_store::Result<Option<ObjectId>> {
            self.inner.get_ref(name).await
        }

        async fn cas_ref(
            &self,
            name: &str,
            expected: Option<ObjectId>,
            new: Option<ObjectId>,
        ) -> ant_store::Result<CasOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.always_mismatch.load(Ordering::SeqCst) {
                return Ok(CasOutcome::Mismatch { actual: expected });
            }
            if self.block.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.acquire().await.unwrap().forget();
            }
            let move_to = self.move_to.lock().unwrap().take();
            if let Some(id) = move_to {
                assert_eq!(
                    self.inner.cas_ref(name, expected, Some(id)).await?,
                    CasOutcome::Updated
                );
            }
            self.inner.cas_ref(name, expected, new).await
        }

        async fn list_refs(&self, prefix: &str) -> ant_store::Result<Vec<(String, ObjectId)>> {
            self.inner.list_refs(prefix).await
        }
    }

    // Enqueue synchronously on a current-thread runtime, making batch membership
    // and arrival order deterministic without sleeps or timing assumptions.
    async fn batch(b: &Backend, commits: &[ObjectId]) -> Vec<OpResult<LandOutcome>> {
        let replies: Vec<_> = commits.iter().map(|id| enqueue(b, "main", *id)).collect();
        timeout(DEADLINE, futures::future::join_all(replies))
            .await
            .unwrap()
            .into_iter()
            .map(Result::unwrap)
            .collect()
    }

    async fn history(b: &Backend, root: ObjectId) -> Vec<ObjectId> {
        let mut current = b.refs.get_ref("main").await.unwrap().unwrap();
        let mut chain = Vec::new();
        while current != root {
            assert!(!chain.contains(&current));
            chain.push(current);
            let node = b.objects.read_commit(&current).await.unwrap();
            assert_eq!(node.parents.len(), 1);
            current = node.parents[0];
        }
        chain.reverse();
        chain
    }

    #[tokio::test]
    async fn middle_conflict_uses_one_cas_and_keeps_own_tips() {
        let mut b = Backend::in_memory();
        let root = commit(&b, None, vec![write("f", "base\n")]).await.unwrap();
        land(&b, "main", root).await.unwrap();
        let a = commit(&b, Some(root), vec![write("f", "a\n")]).await.unwrap();
        let conflict =
            commit(&b, Some(root), vec![write("f", "b\n"), write("leak", "bad")]).await.unwrap();
        let c = commit(&b, Some(root), vec![write("c", "c")]).await.unwrap();
        let refs = ObservedRefs::install(&mut b);
        let outcomes = batch(&b, &[a, conflict, c]).await;
        assert_eq!(
            outcomes[0].as_ref().unwrap(),
            &LandOutcome::Landed { new_tip: a, rebased: false }
        );
        assert_eq!(
            outcomes[1].as_ref().unwrap(),
            &LandOutcome::Conflict { tip: a, paths: vec![RepoPath::new("f").unwrap()] }
        );
        let LandOutcome::Landed { new_tip, rebased: true } = outcomes[2].as_ref().unwrap() else {
            panic!()
        };
        assert_eq!(history(&b, root).await, [a, *new_tip]);
        assert_eq!(read(&b, *new_tip, "f").await.as_deref(), Some("a\n"));
        assert_eq!(read(&b, *new_tip, "c").await.as_deref(), Some("c"));
        assert_eq!(read(&b, *new_tip, "leak").await, None);
        assert_eq!(refs.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn two_hundred_concurrent_lands_have_exact_history_and_counts() {
        let mut b = Backend::in_memory();
        let root = commit(&b, None, vec![write("seed", "s")]).await.unwrap();
        land(&b, "main", root).await.unwrap();
        let mut commits = Vec::new();
        for i in 0..200 {
            commits
                .push(commit(&b, Some(root), vec![write(&format!("f{i}"), "done")]).await.unwrap());
        }
        let refs = ObservedRefs::install(&mut b);
        let outcomes = timeout(
            DEADLINE,
            futures::future::join_all(commits.iter().map(|id| land(&b, "main", *id))),
        )
        .await
        .unwrap();
        let tips: Vec<_> = outcomes
            .into_iter()
            .map(|outcome| match outcome.unwrap() {
                LandOutcome::Landed { new_tip, .. } => new_tip,
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(tips.len(), 200);
        assert_eq!(tips.iter().collect::<BTreeSet<_>>().len(), 200);
        assert_eq!(history(&b, root).await, tips);
        assert_eq!(refs.calls.load(Ordering::SeqCst), 200_usize.div_ceil(MAX_BATCH));
        let tip = *tips.last().unwrap();
        let tree =
            b.objects.read_tree(&b.objects.read_commit(&tip).await.unwrap().tree).await.unwrap();
        assert_eq!(tree.entries().len(), 201);
        for i in 0..200 {
            assert_eq!(read(&b, tip, &format!("f{i}")).await.as_deref(), Some("done"));
        }
        // Every original can be retried, without extending the history.
        for outcome in batch(&b, &commits).await {
            assert_eq!(outcome.unwrap(), LandOutcome::Landed { new_tip: tip, rebased: false });
        }
        assert_eq!(history(&b, root).await.len(), 200);
    }

    #[tokio::test]
    async fn dropping_leader_caller_mid_cas_does_not_strand_followers() {
        let mut b = Backend::in_memory();
        let root = commit(&b, None, vec![]).await.unwrap();
        land(&b, "main", root).await.unwrap();
        let a = commit(&b, Some(root), vec![write("a", "a")]).await.unwrap();
        let c = commit(&b, Some(root), vec![write("c", "c")]).await.unwrap();
        let d = commit(&b, Some(root), vec![write("d", "d")]).await.unwrap();
        let refs = ObservedRefs::install(&mut b);
        refs.block.store(true, Ordering::SeqCst);
        let backend = b.clone();
        let caller = tokio::spawn(async move { land(&backend, "main", a).await });
        timeout(DEADLINE, refs.entered.notified()).await.unwrap();
        let c_reply = enqueue(&b, "main", c);
        let d_reply = enqueue(&b, "main", d);
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        refs.release.add_permits(1);
        for reply in [c_reply, d_reply] {
            assert!(matches!(
                timeout(DEADLINE, reply).await.unwrap().unwrap().unwrap(),
                LandOutcome::Landed { .. }
            ));
        }
        assert_eq!(history(&b, root).await.len(), 3);
        let tip = b.refs.get_ref("main").await.unwrap().unwrap();
        for path in ["a", "c", "d"] {
            assert_eq!(read(&b, tip, path).await.as_deref(), Some(path));
        }
    }

    #[tokio::test]
    async fn duplicate_and_overlapping_stacks_share_tentative_mappings() {
        let b = Backend::in_memory();
        let root = commit(&b, None, vec![]).await.unwrap();
        let moved = commit(&b, Some(root), vec![write("moved", "m")]).await.unwrap();
        land(&b, "main", moved).await.unwrap();
        let lower = commit(&b, Some(root), vec![write("lower", "l")]).await.unwrap();
        let upper = commit(&b, Some(lower), vec![write("upper", "u")]).await.unwrap();
        let outcomes = batch(&b, &[lower, lower, upper, lower, upper]).await;
        assert!(outcomes.iter().all(|o| matches!(o, Ok(LandOutcome::Landed { .. }))));
        let chain = history(&b, moved).await;
        assert_eq!(chain.len(), 2);
        assert_eq!(
            outcomes[1].as_ref().unwrap(),
            &LandOutcome::Landed { new_tip: chain[0], rebased: false }
        );
        assert_eq!(
            outcomes[3].as_ref().unwrap(),
            &LandOutcome::Landed { new_tip: chain[1], rebased: false }
        );
        assert_eq!(read(&b, chain[1], "lower").await.as_deref(), Some("l"));
        assert_eq!(read(&b, chain[1], "upper").await.as_deref(), Some("u"));
        assert_eq!(
            land(&b, "main", lower).await.unwrap(),
            LandOutcome::Landed { new_tip: chain[1], rebased: false }
        );
    }

    #[tokio::test]
    async fn conflicting_stack_does_not_publish_its_prefix() {
        let b = Backend::in_memory();
        let root = commit(&b, None, vec![write("f", "base\n")]).await.unwrap();
        let moved = commit(&b, Some(root), vec![write("f", "ours\n")]).await.unwrap();
        land(&b, "main", moved).await.unwrap();
        let lower = commit(&b, Some(root), vec![write("lower", "l")]).await.unwrap();
        let upper = commit(&b, Some(lower), vec![write("f", "theirs\n")]).await.unwrap();
        let other = commit(&b, Some(root), vec![write("other", "o")]).await.unwrap();
        let outcomes = batch(&b, &[upper, other]).await;
        assert!(matches!(outcomes[0], Ok(LandOutcome::Conflict { .. })));
        let tip = b.refs.get_ref("main").await.unwrap().unwrap();
        assert_eq!(history(&b, moved).await.len(), 1);
        assert_eq!(read(&b, tip, "lower").await, None);
        assert!(b.meta.get_derived(LANDED_AS, lower.as_bytes()).await.unwrap().is_none());
        assert!(matches!(
            land(&b, "main", lower).await.unwrap(),
            LandOutcome::Landed { rebased: true, .. }
        ));
        assert_eq!(history(&b, moved).await.len(), 2);
    }

    #[tokio::test]
    async fn cas_mismatch_recomputes_every_outcome() {
        let mut b = Backend::in_memory();
        let root = commit(&b, None, vec![write("f", "base\n")]).await.unwrap();
        land(&b, "main", root).await.unwrap();
        let a = commit(&b, Some(root), vec![write("f", "a\n")]).await.unwrap();
        let c = commit(&b, Some(root), vec![write("c", "c")]).await.unwrap();
        let external = commit(&b, Some(root), vec![write("f", "external\n")]).await.unwrap();
        let refs = ObservedRefs::install(&mut b);
        *refs.move_to.lock().unwrap() = Some(external);
        let outcomes = batch(&b, &[a, c]).await;
        assert_eq!(
            outcomes[0].as_ref().unwrap(),
            &LandOutcome::Conflict { tip: external, paths: vec![RepoPath::new("f").unwrap()] }
        );
        let LandOutcome::Landed { new_tip, rebased: true } = outcomes[1].as_ref().unwrap() else {
            panic!()
        };
        assert_eq!(history(&b, root).await, [external, *new_tip]);
        assert_eq!(refs.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            b.meta.get_derived(LANDED_AS, c.as_bytes()).await.unwrap().unwrap().as_ref(),
            new_tip.as_bytes()
        );
    }

    #[tokio::test]
    async fn retry_exhaustion_aborts_the_entire_batch() {
        let mut b = Backend::in_memory();
        let root = commit(&b, None, vec![]).await.unwrap();
        land(&b, "main", root).await.unwrap();
        let a = commit(&b, Some(root), vec![write("a", "a")]).await.unwrap();
        let c = commit(&b, Some(root), vec![write("c", "c")]).await.unwrap();
        let refs = ObservedRefs::install(&mut b);
        refs.always_mismatch.store(true, Ordering::SeqCst);
        for outcome in batch(&b, &[a, c]).await {
            assert!(matches!(outcome, Err(OpError::Aborted(_))));
        }
        assert_eq!(refs.calls.load(Ordering::SeqCst), MAX_LAND_ATTEMPTS);
        assert_eq!(b.refs.get_ref("main").await.unwrap(), Some(root));
        assert!(b.meta.get_derived(LANDED_AS, c.as_bytes()).await.unwrap().is_none());
        refs.always_mismatch.store(false, Ordering::SeqCst);
        assert!(matches!(land(&b, "main", c).await.unwrap(), LandOutcome::Landed { .. }));
    }

    #[tokio::test]
    async fn invalid_request_does_not_block_valid_neighbors() {
        let b = Backend::in_memory();
        let root = commit(&b, None, vec![]).await.unwrap();
        let next = commit(&b, Some(root), vec![write("a", "a")]).await.unwrap();
        let outcomes = batch(&b, &[root, ObjectId([9; 32]), next]).await;
        assert!(matches!(outcomes[0], Ok(LandOutcome::Landed { .. })));
        assert!(matches!(outcomes[1], Err(OpError::NotFound(_))));
        assert!(matches!(outcomes[2], Ok(LandOutcome::Landed { .. })));
        assert_eq!(history(&b, root).await, [next]);
    }

    #[tokio::test]
    async fn rebased_changesets_match_tree_diff_including_directory_replacement() {
        use crate::ops::changeset::tests::sig;
        use ant_store::graph::{build_tree, diff_trees, write_commit};
        let b = Backend::in_memory();
        let root = commit(&b, None, vec![write("d/f", "f"), write("same", "s")]).await.unwrap();
        let moved = commit(&b, Some(root), vec![write("other", "o")]).await.unwrap();
        let tree = build_tree(b.objects.as_ref(), &[("d", b"file"), ("same", b"s")]).await.unwrap();
        let original = write_commit(
            &b,
            &ant_core::Commit {
                tree,
                parents: vec![root],
                author: sig(),
                committer: sig(),
                extra_headers: vec![],
                message: "replace directory".into(),
            },
        )
        .await
        .unwrap();
        let rebased = rebase(&b, original, moved).await.unwrap().unwrap();
        let old_tree = b.objects.read_commit(&moved).await.unwrap().tree;
        let new_tree = b.objects.read_commit(&rebased).await.unwrap().tree;
        let changes = b.meta.get_changeset(&rebased).await.unwrap().unwrap();
        assert_eq!(
            changes,
            diff_trees(b.objects.as_ref(), Some(old_tree), Some(new_tree)).await.unwrap()
        );
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].old, None);
        assert_eq!(changes[0].kind, ChangeKind::Added);
        // Reapplying identical changes records an empty changeset.
        let again = rebase(&b, original, rebased).await.unwrap().unwrap();
        assert!(b.meta.get_changeset(&again).await.unwrap().unwrap().is_empty());
    }
}
