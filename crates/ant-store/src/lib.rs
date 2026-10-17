//! Storage traits and implementations.
//!
//! Three independent stores make up a [`Backend`]:
//! - [`ObjectStore`]: immutable, content-addressed encoded objects.
//! - [`RefStore`]: mutable named pointers, updated only by compare-and-swap.
//! - [`MetaStore`]: commit graph, per-commit changesets (the write-ahead log),
//!   and a generic key/value space for derived data.
//!
//! Every implementation must pass the suites in [`conformance`].

mod changeset_codec;
pub mod conformance;
pub mod derive;
pub mod feed;
pub mod graph;
pub mod local;
pub mod memory;
pub mod packed;
pub mod postgres;
pub mod s3;
#[cfg(test)]
mod s3_fake;
pub mod server_cache;
pub mod testing;
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
    /// The store was written by a newer version (its schema version is
    /// higher than this binary supports); nothing is read from it.
    #[error(transparent)]
    NewerFormat(#[from] ant_core::NewerFormat),
    /// The Postgres schema needs migrations, and migrating was disabled.
    #[error(
        "Postgres schema \"{schema}\" is at version {found}, this version of ant needs {required} \
         (migrations not run: --no-migrate)"
    )]
    MigrationRequired { schema: String, found: u32, required: u32 },
}

impl StoreError {
    pub fn backend(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        StoreError::Backend(Box::new(e))
    }
}

/// Hint for a `hint:` line when an error chain says the stored data needs a
/// newer binary (`upgrade ant`) or a schema migration. Looks through
/// [`StoreError`]s, [`ant_core::NewerFormat`] and `io::Error`s wrapping one.
pub fn compat_hint<'a>(
    chain: impl IntoIterator<Item = &'a (dyn std::error::Error + 'static)>,
) -> Option<&'static str> {
    for e in chain {
        let newer = e.downcast_ref::<ant_core::NewerFormat>().is_some()
            || e.downcast_ref::<std::io::Error>()
                .and_then(|io| io.get_ref())
                .is_some_and(|inner| inner.downcast_ref::<ant_core::NewerFormat>().is_some());
        if newer {
            return Some(ant_core::NewerFormat::HINT);
        }
        match e.downcast_ref::<StoreError>() {
            Some(StoreError::NewerFormat(_)) => return Some(ant_core::NewerFormat::HINT),
            Some(StoreError::MigrationRequired { .. }) => {
                return Some(
                    "run `ant admin schema --migrate` with the same store flags (or start \
                     without --no-migrate)",
                );
            }
            _ => {}
        }
    }
    None
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// zstd level of stored object bodies (Postgres rows, S3 objects and packs).
pub(crate) const ZSTD_LEVEL: i32 = 3;
/// Bodies larger than this are (de)compressed and hashed on the blocking
/// pool instead of an async worker thread.
pub(crate) const BLOCKING_THRESHOLD: usize = 64 * 1024;
/// Uncompressed bytes of objects hashed and compressed per blocking task.
const COMPRESS_CHUNK_BYTES: usize = 512 * 1024;

/// `objects` cut into runs of about [`COMPRESS_CHUNK_BYTES`], one per
/// blocking task that hashes and compresses them.
pub(crate) fn compress_chunks(objects: &[EncodedObject]) -> Vec<Vec<EncodedObject>> {
    let mut chunks: Vec<Vec<EncodedObject>> = Vec::new();
    let (mut current, mut bytes) = (Vec::new(), 0usize);
    for o in objects {
        bytes += o.as_bytes().len();
        current.push(o.clone());
        if bytes >= COMPRESS_CHUNK_BYTES {
            chunks.push(std::mem::take(&mut current));
            bytes = 0;
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

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

    /// Stored commits are read leniently: imported history keeps its exact
    /// bytes, which may hold signature lines the strict decoder rejects.
    async fn read_commit(&self, id: &ObjectId) -> Result<Commit> {
        let o = self.get_required(id).await?;
        expect_kind(id, &o, ObjectKind::Commit)?;
        Ok(Commit::decode_lenient(o.body())?)
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

    /// Wait for exclusive write access to `name` among every user of the
    /// same underlying store (all processes sharing one database), so that
    /// batched writers of one ref take turns instead of racing their CAS.
    /// Advisory only: `cas_ref` stays the correctness guarantee, and nothing
    /// stops a writer that does not take the lock. Hold the returned
    /// [`RefLock`] across "read tip → compute → CAS", then
    /// [`RefLock::release`] it. The default (stores that are never shared
    /// between processes) returns an unheld lock immediately.
    async fn lock_ref(&self, name: &str) -> Result<RefLock> {
        validate_ref_name(name)?;
        Ok(RefLock::unheld())
    }

    /// Notices of the ref moves this store commits (for stores shared
    /// between processes: every process's moves), see [`feed`]. `None`: the
    /// store announces nothing. Wrappers must forward this.
    fn watch_refs(&self) -> Option<tokio::sync::broadcast::Receiver<feed::RefNotice>> {
        None
    }
}

/// Releases a held [`RefLock`]; implemented by stores with real locks.
#[async_trait]
pub trait RefLockRelease: Send + Sync {
    async fn release(self: Box<Self>) -> Result<()>;

    /// Whether [`RefLockRelease::publish`] is supported.
    fn can_publish(&self) -> bool {
        false
    }

    /// See [`RefLock::contended`]. The default (unknown) says yes.
    async fn contended(&mut self) -> Result<bool> {
        Ok(true)
    }

    /// See [`RefLock::publish`].
    async fn publish(
        &mut self,
        _expected: Option<ObjectId>,
        _new: ObjectId,
        _kind: &str,
        _derived: Vec<(Vec<u8>, Bytes)>,
    ) -> Result<CasOutcome> {
        Err(StoreError::InvalidArgument("this ref lock cannot publish".into()))
    }

    /// See [`RefLock::publish_pipelined`]. The default publishes durably
    /// ([`Self::publish`]) and returns a resolved [`Durability`].
    async fn publish_pipelined(
        &mut self,
        expected: Option<ObjectId>,
        new: ObjectId,
        kind: &str,
        derived: Vec<(Vec<u8>, Bytes)>,
    ) -> Result<(CasOutcome, Durability)> {
        let out = self.publish(expected, new, kind, derived).await?;
        Ok((out, Box::pin(async { Ok(()) })))
    }
}

/// Resolves once a pipelined publish is durable ([`RefLock::publish_pipelined`]).
pub type Durability = std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>;

/// Exclusive write access to one ref, from [`RefStore::lock_ref`]. Dropping
/// it without [`RefLock::release`] also releases it, possibly later (e.g.
/// when the store closes the connection that held it).
pub struct RefLock(Option<Box<dyn RefLockRelease>>);

impl RefLock {
    /// A lock that excludes nobody (stores without cross-process locking).
    pub fn unheld() -> Self {
        Self(None)
    }

    pub fn held(release: impl RefLockRelease + 'static) -> Self {
        Self(Some(Box::new(release)))
    }

    /// Whether this lock actually excludes other writers.
    pub fn is_held(&self) -> bool {
        self.0.is_some()
    }

    pub async fn release(self) -> Result<()> {
        match self.0 {
            Some(lock) => lock.release().await,
            None => Ok(()),
        }
    }

    /// Whether someone else is waiting for this lock (another server
    /// replica, an import publishing the ref). A holder that takes turns
    /// asks at the end of a turn and keeps the lock while nobody waits.
    /// Stores that cannot tell, and an unheld lock, say yes.
    pub async fn contended(&mut self) -> Result<bool> {
        match &mut self.0 {
            Some(lock) => lock.contended().await,
            None => Ok(true),
        }
    }

    /// Whether [`Self::publish`] is available: the lock holds a database
    /// session of its own, opened before the lock was granted.
    pub fn can_publish(&self) -> bool {
        self.0.as_ref().is_some_and(|lock| lock.can_publish())
    }

    /// Compare-and-swap the locked ref from `expected` to `new` and, only if
    /// it moved, insert `derived` rows (`kind`, key → value; existing keys
    /// are kept), atomically, in one synchronous (durable) commit made on
    /// the lock's own session.
    ///
    /// That session is the crash witness for [`deferred_durability`]: it
    /// was opened before any write made while holding the lock, and a
    /// database crash or restart (the only way acknowledged but unflushed
    /// writes are lost) ends every session. So when `publish` commits, no
    /// crash happened since the lock was taken, every write acknowledged
    /// before the call precedes this commit in the write-ahead log, and the
    /// synchronous commit flushes them with it. If the session died,
    /// `publish` fails and nothing is published.
    pub async fn publish(
        &mut self,
        expected: Option<ObjectId>,
        new: ObjectId,
        kind: &str,
        derived: Vec<(Vec<u8>, Bytes)>,
    ) -> Result<CasOutcome> {
        match &mut self.0 {
            Some(lock) if lock.can_publish() => lock.publish(expected, new, kind, derived).await,
            _ => Err(StoreError::InvalidArgument("this ref lock cannot publish".into())),
        }
    }

    /// [`Self::publish`] that may return as soon as the move is committed
    /// (visible to readers), before it is durable, with a [`Durability`]
    /// that resolves once it is (or fails if it cannot be made durable).
    /// Lets a land queue publish its next batch (a later commit, so durable
    /// no earlier than this one) while this one's fsync is in flight; its
    /// requests must not be answered before the [`Durability`] resolves.
    /// Stores without a cheaper path (Postgres) publish durably and return
    /// a resolved one.
    pub async fn publish_pipelined(
        &mut self,
        expected: Option<ObjectId>,
        new: ObjectId,
        kind: &str,
        derived: Vec<(Vec<u8>, Bytes)>,
    ) -> Result<(CasOutcome, Durability)> {
        match &mut self.0 {
            Some(lock) if lock.can_publish() => {
                lock.publish_pipelined(expected, new, kind, derived).await
            }
            _ => Err(StoreError::InvalidArgument("this ref lock cannot publish".into())),
        }
    }
}

impl std::fmt::Debug for RefLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RefLock").field(&self.is_held()).finish()
    }
}

tokio::task_local! {
    static DEFERRED: ();
}

/// Run `fut` with deferred durability: stores that support it (Postgres)
/// acknowledge the writes `fut` makes before they are durable (Postgres:
/// `synchronous_commit = off`, no wait for the WAL flush). Reads, and
/// writes outside `fut`, are unaffected; other stores ignore it.
///
/// A database crash can lose acknowledged deferred writes, so they must not
/// become reachable before a later synchronous commit has made them durable
/// *and* proved that no crash happened in between: use this only for writes
/// that a [`RefLock::publish`] of the lock held throughout `fut` makes
/// reachable. The scope is task-local; work spawned onto other tasks writes
/// synchronously, which is always safe.
pub async fn deferred_durability<F: std::future::Future>(fut: F) -> F::Output {
    DEFERRED.scope((), fut).await
}

/// Whether the current task runs inside [`deferred_durability`].
pub(crate) fn durability_deferred() -> bool {
    DEFERRED.try_with(|_| ()).is_ok()
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

    /// The commits reachable from `tips` (inclusive, through every parent)
    /// whose generation is above `min_generation`, each once, at most
    /// `limit`. The walk neither returns nor passes through commits at or
    /// below `min_generation` or unrecorded ones. Order: the recorded `tips`
    /// above `min_generation` first (so a `limit >= tips.len()` returns all
    /// of them), then the rest in breadth-first order (a truncated result is
    /// the part of the ancestry closest to `tips`). The default reads one
    /// `get_commits` per level; persistent stores should override this with
    /// a single server-side traversal.
    async fn ancestors_above(
        &self,
        tips: &[ObjectId],
        min_generation: u64,
        limit: usize,
    ) -> Result<Vec<CommitNode>> {
        let mut seen = std::collections::HashSet::new();
        let mut level: Vec<ObjectId> = tips.iter().copied().filter(|id| seen.insert(*id)).collect();
        let mut out = Vec::new();
        while !level.is_empty() && out.len() < limit {
            let mut next = Vec::new();
            for n in self.get_commits(&level).await?.into_iter().flatten() {
                if n.generation <= min_generation || out.len() >= limit {
                    continue;
                }
                next.extend(n.parents.iter().copied().filter(|p| seen.insert(*p)));
                out.push(n);
            }
            level = next;
        }
        Ok(out)
    }

    /// Log entries with sequence number `> after`, ascending, at most `limit`.
    /// Sequence numbers start at 1 and strictly increase (gaps allowed).
    async fn tail(&self, after: u64, limit: usize) -> Result<Vec<(u64, ObjectId)>>;

    /// Index-aligned log sequence numbers of recorded commits (`None`:
    /// unrecorded, or not known to this store). Commits are recorded only
    /// after their parents (the parents a node's generation is computed
    /// from), so the log order is a topological order: a commit recorded
    /// after `d` is not an ancestor of `d` (`graph::merge_base` relies on
    /// it). The default knows none; persistent stores should override this
    /// with a single round trip.
    async fn log_seqs(&self, ids: &[ObjectId]) -> Result<Vec<Option<u64>>> {
        Ok(vec![None; ids.len()])
    }

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

#[cfg(test)]
mod durability_tests {
    use super::{deferred_durability, durability_deferred};

    #[tokio::test]
    async fn deferred_durability_is_scoped_to_the_task() {
        assert!(!durability_deferred());
        deferred_durability(async {
            assert!(durability_deferred());
            // Work moved to another task writes synchronously (always safe).
            assert!(!tokio::spawn(async { durability_deferred() }).await.unwrap());
            tokio::task::yield_now().await;
            assert!(durability_deferred());
        })
        .await;
        assert!(!durability_deferred());
    }
}
