//! In-memory batch replay: ancestry checks, stack discovery and rebases
//! against a tentative tip, with every new object and commit record queued
//! until the batch is flushed before its CAS.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use ant_auth::Policy;
use ant_core::merge::{self, EntryMerge, TextMerge};
use ant_core::{EncodedObject, EntryMode, Object, ObjectId, ObjectKind, RepoPath, Tree, TreeEntry};
use ant_store::{Backend, CommitNode, PathChange, StoreError};

use super::verify::Verified;
use super::{LANDED_AS, LandOutcome, MAX_STACK};
use crate::acl;
use crate::ops::tree_edit::{
    Directories, apply_write, deepest_first, entry, leaf_change, load_directories, prune_empty,
};
use crate::ops::{OpError, OpResult, cache};

/// A new blob or tree and the path it is written at (for zone tagging).
type TagWrite = (RepoPath, ObjectId);

/// First-parent nodes fetched in one round trip when an ancestry walk reaches
/// a commit that is not cached (e.g. history landed by another process).
const CHAIN_PREFETCH: usize = 128;

/// Objects and commit records produced by in-memory rebases, not yet written.
/// They are written together (objects first) before the batch's CAS.
#[derive(Default)]
pub(super) struct Writes {
    objects: Vec<EncodedObject>,
    commits: Vec<(CommitNode, Vec<PathChange>)>,
    trees: Vec<(ObjectId, Arc<Tree>)>,
    /// Zone tags owed (under a policy) for new blobs/trees: (path, id).
    tags: Vec<(Arc<Policy>, RepoPath, ObjectId)>,
}

impl Writes {
    fn mark(&self) -> [usize; 4] {
        [self.objects.len(), self.commits.len(), self.trees.len(), self.tags.len()]
    }

    fn truncate(&mut self, [objects, commits, trees, tags]: [usize; 4]) {
        self.objects.truncate(objects);
        self.commits.truncate(commits);
        self.trees.truncate(trees);
        self.tags.truncate(tags);
    }
}

/// State for replaying one batch in memory. Ancestry nodes, trees and blobs
/// are immutable, so everything read or built stays valid across CAS retries;
/// commits and trees built here are visible to later requests of the batch
/// before they are written.
#[derive(Default)]
pub(super) struct BatchState {
    pub(super) nodes: HashMap<ObjectId, CommitNode>,
    pub(super) mappings: HashMap<ObjectId, Option<bytes::Bytes>>,
    pub(super) pending: BTreeMap<ObjectId, ObjectId>,
    /// Trees and merged blobs built by this batch (written or not).
    pub(super) trees: HashMap<ObjectId, Arc<Tree>>,
    pub(super) blobs: HashMap<ObjectId, EncodedObject>,
    pub(super) writes: Writes,
}

impl BatchState {
    /// Load nodes into `self.nodes` without failing on unrecorded ids.
    pub(super) async fn prefetch(&mut self, b: &Backend, ids: &[ObjectId]) -> OpResult<()> {
        let missing: Vec<_> =
            ids.iter().copied().filter(|id| !self.nodes.contains_key(id)).collect();
        if !missing.is_empty() {
            for node in cache::get_nodes(b, &missing).await?.into_iter().flatten() {
                self.nodes.insert(node.id, node);
            }
        }
        Ok(())
    }

    pub(super) async fn nodes(&mut self, b: &Backend, ids: &[ObjectId]) -> OpResult<()> {
        self.prefetch(b, ids).await?;
        match ids.iter().find(|id| !self.nodes.contains_key(id)) {
            Some(id) => Err(OpError::not_found_object(*id)),
            None => Ok(()),
        }
    }

    /// Like [`Self::nodes`], but a single uncached commit (the common case
    /// while walking linear history) prefetches its first-parent chain.
    pub(super) async fn walk_nodes(&mut self, b: &Backend, ids: &[ObjectId]) -> OpResult<()> {
        if let [id] = ids
            && !self.nodes.contains_key(id)
        {
            let meta = cache::meta(&b.meta);
            if meta.nodes.get(id).is_none() {
                let chain = b.meta.first_parent_chain(*id, None, CHAIN_PREFETCH).await?;
                meta.nodes.insert_many(chain.iter().map(|n| (n.id, n.clone(), cache::NODE_WEIGHT)));
                for node in chain {
                    self.nodes.entry(node.id).or_insert(node);
                }
            }
        }
        self.nodes(b, ids).await
    }

    pub(super) async fn ancestor(
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
            self.walk_nodes(b, &frontier).await?;
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

    pub(super) async fn already_landed(
        &mut self,
        b: &Backend,
        id: ObjectId,
        tip: ObjectId,
    ) -> OpResult<bool> {
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
    pub(super) async fn stack(
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
    pub(super) async fn apply(
        &mut self,
        b: &Backend,
        commit: ObjectId,
        tip: Option<ObjectId>,
        verified: &Verified,
        policy: Option<&Arc<Policy>>,
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
        let mark = self.writes.mark();
        // One round trip each for the whole stack's changesets and messages.
        cache::get_changesets(b, &stack).await?;
        cache::read_commits(b, &stack).await?;
        for original in stack {
            match self.rebase(b, policy, original, onto).await {
                Ok(Ok(id)) => {
                    pairs.push((original, id));
                    onto = id;
                }
                // A conflicting stack must not make its successfully replayed
                // prefix visible to later requests, publish that prefix's
                // mappings, or record its commits.
                Ok(Err(paths)) => {
                    self.writes.truncate(mark);
                    return Ok(LandOutcome::Conflict { tip, paths });
                }
                Err(error) => {
                    self.writes.truncate(mark);
                    return Err(error);
                }
            }
        }
        self.pending.extend(pairs);
        Ok(LandOutcome::Landed { new_tip: onto, rebased: true })
    }

    /// The blobs `ids`, from this batch's blobs, the blob cache or one
    /// `get_many`.
    async fn blobs(
        &self,
        b: &Backend,
        ids: BTreeSet<ObjectId>,
    ) -> OpResult<BTreeMap<ObjectId, EncodedObject>> {
        let mut blobs = BTreeMap::new();
        let unbuilt: Vec<_> = ids
            .into_iter()
            .filter(|id| match self.blobs.get(id) {
                Some(blob) => {
                    blobs.insert(*id, blob.clone());
                    false
                }
                None => true,
            })
            .collect();
        let cached = cache::objects(&b.objects).blobs.get_many(&unbuilt);
        let mut fetch = Vec::new();
        for (id, blob) in unbuilt.into_iter().zip(cached) {
            match blob {
                Some(blob) => {
                    blobs.insert(id, blob);
                }
                None => fetch.push(id),
            }
        }
        if !fetch.is_empty() {
            let fetched = b.objects.get_many(&fetch).await?;
            cache::remember_blobs(b, fetched.iter().flatten());
            for (id, object) in fetch.iter().copied().zip(fetched) {
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
        Ok(blobs)
    }

    /// Rebase `commit` onto `onto` in memory; on success the new trees,
    /// blobs and commit are queued in `self.writes` (see [`Self::flush`]).
    pub(super) async fn rebase(
        &mut self,
        b: &Backend,
        policy: Option<&Arc<Policy>>,
        commit: ObjectId,
        onto: ObjectId,
    ) -> OpResult<Result<ObjectId, Vec<RepoPath>>> {
        self.prefetch(b, &[commit, onto]).await?;
        let original = self.nodes.get(&commit).ok_or_else(|| OpError::not_found_object(commit))?;
        if original.parents.len() != 1 {
            return Err(OpError::InvalidArgument("rebase requires exactly one parent".into()));
        }
        let parent = original.parents[0];
        let target = self.nodes.get(&onto).ok_or_else(|| OpError::not_found_object(onto))?.clone();
        self.nodes(b, &[parent]).await?;
        let parent_tree = self.nodes[&parent].tree;
        let changes = cache::get_changesets(b, &[commit])
            .await?
            .pop()
            .flatten()
            .ok_or_else(|| OpError::not_found_object(commit))?;
        let paths: Vec<_> = changes.iter().map(|c| c.path.clone()).collect();
        let [base_dirs, mut dirs] =
            load_directories(b, &self.trees, [parent_tree, target.tree], &paths).await?;
        let mut conflicts = BTreeSet::new();
        let mut edits = BTreeMap::new();
        let mut merges = Vec::new();
        let mut blob_ids = BTreeSet::new();
        for change in changes.iter() {
            let base = entry(&base_dirs, &change.path);
            let ours = entry(&dirs, &change.path);
            let theirs = change.new;
            match merge::entries(base, ours, theirs) {
                EntryMerge::Same | EntryMerge::Ours => {}
                EntryMerge::Theirs => {
                    edits.insert(change.path.clone(), theirs);
                }
                EntryMerge::Text { mode, base, ours, theirs } => {
                    blob_ids.extend([base, ours, theirs]);
                    merges.push((change.path.clone(), mode, [base, ours, theirs]));
                }
                EntryMerge::Conflict => {
                    conflicts.insert(change.path.clone());
                }
            }
        }
        let blobs = self.blobs(b, blob_ids).await?;
        let mut objects = Vec::new();
        // (path, id) of every new blob/tree, for zone tagging.
        let mut written = Vec::new();
        for (path, mode, ids) in merges {
            let bodies = ids.map(|id| blobs.get(&id).map(|blob| blob.body()));
            if let [Some(base), Some(ours), Some(theirs)] = bodies
                && let TextMerge::Clean(merged) = merge::text(base, ours, theirs)
            {
                let object = EncodedObject::new(ObjectKind::Blob, &merged);
                written.push((path.clone(), object.id()));
                edits.insert(path, Some((mode, object.id())));
                objects.push(object);
            } else {
                conflicts.insert(path);
            }
        }
        let recorded_changes: Vec<_> = edits
            .iter()
            // Changesets describe leaves; replacing a pruned directory adds a
            // leaf here and records its children as separate deletions.
            .filter_map(|(path, new)| leaf_change(&dirs, path, *new))
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
        let mut new_trees = Vec::new();
        let root = encode_changed(dirs, target.tree, &mut objects, &mut written, &mut new_trees)?;
        let original = cache::read_commits(b, &[commit]).await?;
        let mut rebased = original
            .first()
            .map(|c| (**c).clone())
            .ok_or_else(|| OpError::not_found_object(commit))?;
        rebased.tree = root;
        rebased.parents = vec![onto];
        let generation = target.generation + 1;
        let committer_time = rebased.committer.time;
        let object = Object::Commit(rebased).encode();
        let id = object.id();
        objects.push(object);
        let node = CommitNode { id, tree: root, parents: vec![onto], generation, committer_time };
        for object in &objects {
            if object.kind() == ObjectKind::Blob {
                self.blobs.insert(object.id(), object.clone());
            }
        }
        for (id, tree) in &new_trees {
            self.trees.insert(*id, tree.clone());
        }
        self.nodes.insert(id, node.clone());
        self.writes.objects.extend(objects);
        self.writes.trees.extend(new_trees);
        if let Some(policy) = policy {
            self.writes
                .tags
                .extend(written.into_iter().map(|(path, id)| (policy.clone(), path, id)));
        }
        self.writes.commits.push((node, recorded_changes));
        Ok(Ok(id))
    }

    /// Start from what `predecessor` read and built (nodes, trees, blobs):
    /// a batch replayed on an unpublished predecessor's tentative tip needs
    /// its new commits and trees before they are written (and cached).
    pub(super) fn seed(&mut self, predecessor: &BatchState) {
        for (id, node) in &predecessor.nodes {
            self.nodes.entry(*id).or_insert_with(|| node.clone());
        }
        for (id, tree) in &predecessor.trees {
            self.trees.entry(*id).or_insert_with(|| tree.clone());
        }
        for (id, blob) in &predecessor.blobs {
            self.blobs.entry(*id).or_insert_with(|| blob.clone());
        }
    }

    /// Write queued objects and record queued commits (parents first), then
    /// publish both to the store-scoped caches.
    pub(super) async fn flush(&mut self, b: &Backend) -> OpResult<()> {
        self.take_writes().write(b).await
    }

    /// The queued objects and commit records, to write with [`Writes::write`].
    pub(super) fn take_writes(&mut self) -> Writes {
        std::mem::take(&mut self.writes)
    }
}

impl Writes {
    pub(super) fn is_empty(&self) -> bool {
        self.commits.is_empty() && self.objects.is_empty()
    }

    /// See [`BatchState::flush`].
    pub(super) async fn write(self, b: &Backend) -> OpResult<()> {
        let writes = self;
        if writes.is_empty() {
            return Ok(());
        }
        // Tags must be recorded before the objects are stored, one call per
        // distinct policy (normally all requests share one).
        let mut by_policy: Vec<(Arc<Policy>, Vec<TagWrite>)> = Vec::new();
        for (policy, path, id) in writes.tags {
            match by_policy.iter_mut().find(|(p, _)| Arc::ptr_eq(p, &policy)) {
                Some((_, group)) => group.push((path, id)),
                None => by_policy.push((policy, vec![(path, id)])),
            }
        }
        for (policy, group) in &by_policy {
            acl::tag_written(b, policy, group).await?;
        }
        let entries: Vec<_> = writes
            .commits
            .iter()
            .map(|(node, changes)| {
                (node.clone(), Arc::new(changes.clone()), cache::changeset_weight(changes))
            })
            .collect();
        // Objects and commit records go out concurrently: both finish before
        // the caller's CAS makes anything reachable, and a recorded commit
        // whose objects failed to write is unreachable (its id was never
        // returned) and is completed by any retry, which rebuilds it
        // identically.
        futures::try_join!(async { b.objects.put_many(&writes.objects).await }, async {
            b.meta.put_commits(writes.commits).await
        },)?;
        cache::objects(&b.objects).trees.insert_many(writes.trees.into_iter().map(|(id, tree)| {
            let weight = cache::tree_weight(&tree);
            (id, tree, weight)
        }));
        let meta = cache::meta(&b.meta);
        meta.nodes
            .insert_many(entries.iter().map(|(n, _, _)| (n.id, n.clone(), cache::NODE_WEIGHT)));
        meta.changesets.insert_many(entries.into_iter().map(|(n, c, w)| (n.id, c, w)));
        Ok(())
    }
}

/// Encode the edited directories deepest first into a new root tree,
/// skipping trees whose id did not change (`root` is the unedited root).
/// New trees go to `objects`, `written` (with their path) and `new_trees`
/// (decoded, for the caches); returns the new root.
fn encode_changed(
    mut dirs: Directories,
    root: ObjectId,
    objects: &mut Vec<EncodedObject>,
    written: &mut Vec<(RepoPath, ObjectId)>,
    new_trees: &mut Vec<(ObjectId, Arc<Tree>)>,
) -> OpResult<ObjectId> {
    let mut new_root = root;
    for path in deepest_first(&dirs) {
        let tree = dirs
            .remove(&path)
            .ok_or_else(|| OpError::InvalidArgument(format!("missing directory {path}")))?;
        let object = EncodedObject::new(ObjectKind::Tree, &tree.encode());
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
            new_root = id;
            if new_root == root {
                continue;
            }
        }
        objects.push(object);
        written.push((path, id));
        new_trees.push((id, Arc::new(tree)));
    }
    Ok(new_root)
}
