//! Storage traits and implementations.
//!
//! Three independent stores make up a [`Backend`]:
//! - [`ObjectStore`]: immutable, content-addressed encoded objects.
//! - [`RefStore`]: mutable named pointers, updated only by compare-and-swap.
//! - [`MetaStore`]: commit graph, per-commit changesets (the write-ahead log),
//!   and a generic key/value space for derived data.
//!
//! Every implementation must pass the suites in [`conformance`].

pub mod conformance;
pub mod graph;
pub mod memory;
pub mod postgres;
pub mod s3;
pub mod tiered;

use std::sync::Arc;

use ant_core::{Commit, EncodedObject, EntryMode, ObjectId, ObjectKind, RepoPath, Tree};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("object not found: {0}")]
    NotFound(ObjectId),
    #[error("expected {expected:?} object at {id}, found {actual:?}")]
    WrongKind { id: ObjectId, expected: ObjectKind, actual: ObjectKind },
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error(transparent)]
    Core(#[from] ant_core::Error),
    #[error("backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl StoreError {
    pub fn backend(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        StoreError::Backend(Box::new(e))
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

// ---------------------------------------------------------------------------
// ObjectStore
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    /// Fetch encoded objects. The output is index-aligned with `ids`; missing
    /// objects are `None`. Duplicate ids are allowed.
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>>;

    /// Store objects under their own hash. Idempotent: re-putting an existing
    /// object succeeds and is a no-op.
    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()>;

    /// Index-aligned existence check.
    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>>;
}

/// Convenience helpers available on every `ObjectStore` (including `dyn`).
#[async_trait]
pub trait ObjectStoreExt: ObjectStore {
    async fn get(&self, id: &ObjectId) -> Result<Option<EncodedObject>> {
        Ok(self.get_many(std::slice::from_ref(id)).await?.pop().flatten())
    }

    async fn get_required(&self, id: &ObjectId) -> Result<EncodedObject> {
        self.get(id).await?.ok_or(StoreError::NotFound(*id))
    }

    async fn put(&self, object: EncodedObject) -> Result<ObjectId> {
        let id = object.id();
        self.put_many(std::slice::from_ref(&object)).await?;
        Ok(id)
    }

    async fn read_tree(&self, id: &ObjectId) -> Result<Tree> {
        let o = self.get_required(id).await?;
        expect_kind(id, &o, ObjectKind::Tree)?;
        Ok(Tree::decode(o.body())?)
    }

    async fn read_commit(&self, id: &ObjectId) -> Result<Commit> {
        let o = self.get_required(id).await?;
        expect_kind(id, &o, ObjectKind::Commit)?;
        Ok(Commit::decode(o.body())?)
    }

    async fn read_blob(&self, id: &ObjectId) -> Result<Bytes> {
        let o = self.get_required(id).await?;
        expect_kind(id, &o, ObjectKind::Blob)?;
        Ok(o.as_bytes().slice(o.as_bytes().len() - o.body().len()..))
    }
}

impl<T: ObjectStore + ?Sized> ObjectStoreExt for T {}

fn expect_kind(id: &ObjectId, o: &EncodedObject, expected: ObjectKind) -> Result<()> {
    if o.kind() != expected {
        return Err(StoreError::WrongKind { id: *id, expected, actual: o.kind() });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// RefStore
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasOutcome {
    Updated,
    /// The ref did not hold `expected`; nothing was written.
    Mismatch {
        actual: Option<ObjectId>,
    },
}

#[async_trait]
pub trait RefStore: Send + Sync + 'static {
    async fn get_ref(&self, name: &str) -> Result<Option<ObjectId>>;

    /// Atomically set `name` to `new` iff it currently holds `expected`.
    /// `expected = None` means "must not exist"; `new = None` deletes.
    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ObjectId>,
        new: Option<ObjectId>,
    ) -> Result<CasOutcome>;

    /// All refs whose name starts with `prefix`, sorted by name.
    async fn list_refs(&self, prefix: &str) -> Result<Vec<(String, ObjectId)>>;
}

/// Ref names: `/`-separated, each component non-empty, no `.`/`..`, no
/// whitespace, NUL, `~^:?*[\`. E.g. `main`, `release/1.2`, `ws/agent-7/fix`.
pub fn validate_ref_name(name: &str) -> Result<()> {
    let bad = name.is_empty()
        || name.len() > 255
        || name.split('/').any(|c| c.is_empty() || c == "." || c == "..")
        || name.chars().any(|c| c.is_whitespace() || c.is_control() || "~^:?*[\\".contains(c));
    if bad {
        return Err(StoreError::InvalidArgument(format!("invalid ref name {name:?}")));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// MetaStore
// ---------------------------------------------------------------------------

/// Commit-graph node. `generation` is 1 for root commits, else
/// `1 + max(parent generations)`; it lets ancestry queries prune early.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitNode {
    pub id: ObjectId,
    pub tree: ObjectId,
    pub parents: Vec<ObjectId>,
    pub generation: u64,
    pub committer_time: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    Added,
    Modified,
    Deleted,
}

/// One non-tree path changed by a commit relative to its first parent (or to
/// the empty tree for root commits). Mode-only changes count as `Modified`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathChange {
    pub path: RepoPath,
    pub kind: ChangeKind,
    pub old: Option<(EntryMode, ObjectId)>,
    pub new: Option<(EntryMode, ObjectId)>,
}

/// One row of a ref's path-history index: `commit` (on the ref's first-parent
/// chain) changed `path` (a non-tree path, as in its changeset).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRow {
    pub path: RepoPath,
    pub generation: u64,
    pub commit: ObjectId,
}

#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// Record a commit and its changeset (sorted by path) atomically, and
    /// append it to the log. Idempotent: re-recording an existing commit
    /// returns its original sequence number and changes nothing.
    async fn put_commit(&self, node: CommitNode, changes: Vec<PathChange>) -> Result<u64>;

    /// Index-aligned lookup.
    async fn get_commits(&self, ids: &[ObjectId]) -> Result<Vec<Option<CommitNode>>>;

    async fn get_changeset(&self, id: &ObjectId) -> Result<Option<Vec<PathChange>>>;

    /// Index-aligned batch of [`Self::get_changeset`]. Persistent stores
    /// should override this with a single round trip.
    async fn get_changesets(&self, ids: &[ObjectId]) -> Result<Vec<Option<Vec<PathChange>>>> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            out.push(self.get_changeset(id).await?);
        }
        Ok(out)
    }

    /// The first-parent chain starting at `tip` (inclusive), newest first, at
    /// most `limit` nodes, ending before `stop` (exclusive) when it is reached.
    /// Stops early at a root or an unrecorded commit. The default walks with
    /// one `get_commits` per commit; persistent stores should override this
    /// with a single server-side traversal.
    async fn first_parent_chain(
        &self,
        tip: ObjectId,
        stop: Option<ObjectId>,
        limit: usize,
    ) -> Result<Vec<CommitNode>> {
        let mut out = Vec::new();
        let mut next = Some(tip);
        while let Some(id) = next.filter(|id| Some(*id) != stop) {
            if out.len() >= limit {
                break;
            }
            let Some(node) = self.get_commits(&[id]).await?.pop().flatten() else { break };
            next = node.parents.first().copied();
            out.push(node);
        }
        Ok(out)
    }

    /// Log entries with sequence number `> after`, ascending, at most `limit`.
    /// Sequence numbers start at 1 and strictly increase (gaps allowed).
    async fn tail(&self, after: u64, limit: usize) -> Result<Vec<(u64, ObjectId)>>;

    /// Generic derived-data space. `kind` names the derivation (e.g.
    /// `"path-history/v1"`). Values are immutable once written: a second put
    /// of the same key overwrites only if the value is identical (no-op) and
    /// otherwise is allowed to either keep the first value or replace it.
    async fn get_derived(&self, kind: &str, key: &[u8]) -> Result<Option<Bytes>>;
    async fn put_derived(&self, kind: &str, key: &[u8], value: Bytes) -> Result<()>;

    /// Add rows to `ref_name`'s path-history index. Idempotent.
    async fn index_history(&self, ref_name: &str, rows: &[HistoryRow]) -> Result<()>;

    /// Distinct commits in `ref_name`'s index having a row whose path equals
    /// `prefix` or lies under it (root prefix = every row), with generation
    /// `<= max_generation`; ordered by generation descending, at most `limit`.
    async fn query_history(
        &self,
        ref_name: &str,
        prefix: &RepoPath,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<(u64, ObjectId)>>;

    /// Last commit indexed for `ref_name` (the indexer's watermark).
    async fn history_watermark(&self, ref_name: &str) -> Result<Option<ObjectId>>;

    /// Atomically move the watermark from `expected` to `new` (CAS, like
    /// refs). Returns false on mismatch.
    async fn set_history_watermark(
        &self,
        ref_name: &str,
        expected: Option<ObjectId>,
        new: ObjectId,
    ) -> Result<bool>;

    /// Drop all rows and the watermark for `ref_name` (used when a ref is
    /// moved non-fast-forward and its index must be rebuilt).
    async fn clear_history(&self, ref_name: &str) -> Result<()>;

    /// Index-aligned batch of [`Self::get_derived`]. Persistent stores should
    /// override this with a single round trip.
    async fn get_derived_many(&self, kind: &str, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>> {
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            out.push(self.get_derived(kind, k).await?);
        }
        Ok(out)
    }

    /// Record many commits (parents before children) with their changesets.
    /// Equivalent to calling [`Self::put_commit`] for each entry in order;
    /// returns the sequence numbers index-aligned with the input. Persistent
    /// stores should override this with one transaction.
    async fn put_commits(&self, commits: Vec<(CommitNode, Vec<PathChange>)>) -> Result<Vec<u64>> {
        let mut out = Vec::with_capacity(commits.len());
        for (node, changes) in commits {
            out.push(self.put_commit(node, changes).await?);
        }
        Ok(out)
    }

    /// Batch of [`Self::put_derived`] under one `kind`. Persistent stores
    /// should override this with a single round trip.
    async fn put_derived_many(&self, kind: &str, entries: Vec<(Vec<u8>, Bytes)>) -> Result<()> {
        for (k, v) in entries {
            self.put_derived(kind, &k, v).await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Backend {
    pub objects: Arc<dyn ObjectStore>,
    pub refs: Arc<dyn RefStore>,
    pub meta: Arc<dyn MetaStore>,
}

impl Backend {
    pub fn in_memory() -> Self {
        Self {
            objects: Arc::new(memory::MemoryObjectStore::default()),
            refs: Arc::new(memory::MemoryRefStore::default()),
            meta: Arc::new(memory::MemoryMetaStore::default()),
        }
    }
}
