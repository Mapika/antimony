//! Import existing git repositories (SHA-1) into antimony.
//!
//! - Open the repo with `gix`. For each `(git_ref, ant_ref)` in
//!   `opts.refs`, collect all commits reachable from the git ref that are
//!   not yet imported, and convert them parents-first (topological order).
//! - Conversion rewrites object ids to SHA-256 git objects
//!   (`ant_core::{Tree, Commit}`) while preserving everything else byte for
//!   byte: tree entry names/modes, commit author/committer/time zones,
//!   extra headers (e.g. `encoding`, `mergetag`, `gpgsig`) and messages
//!   (raw bytes: names and messages need not be UTF-8). The result equals
//!   what `git fast-export | git fast-import` into a
//!   `--object-format=sha256` repo produces for unsigned commits (the test
//!   checks every commit id).
//! - The commit mapping is persisted in `MetaStore` derived data, kind
//!   `"git-import/commit/v1"`, key = source commit id bytes, value = 32-byte
//!   antimony id — this makes re-imports incremental (already-imported
//!   commits are skipped without reading their trees).
//! - Finally each `ant_ref` is pointed at the converted tip via `cas_ref`
//!   (expected = current value; it's an import tool, not concurrent).
//! - Stats count objects actually written (not skipped). When no previously
//!   imported commit is found the target is assumed empty and the existence
//!   check is skipped (re-puts are idempotent), so stats then count every
//!   converted object.
//!
//! # Pipeline
//!
//! ```text
//!  discover ──► N prep workers ──(in order)──► converter ──► writer ──► store
//!  (graph)      read/diff/parse/hash           memo, trees    batched    objects,
//!                                              commits        put_many   commits
//! ```
//!
//! 1. `graph`: walks the history once (commit headers only, read in parallel),
//!    asks the store which commits are already imported, and orders the rest
//!    parents-first. The graph is a handful of flat arrays (about 100 bytes per
//!    commit).
//! 2. `prep`: workers (each with its own git handle) take chunks of that order
//!    and do everything that needs only source objects: parse the commit, diff
//!    its tree against its parents' trees, read and hash the blobs it
//!    introduces and parse the changed trees. Nothing is shared between
//!    workers, so this scales with cores.
//! 3. `convert`: one thread consumes the prepared commits in order, maps source
//!    ids to antimony ids through a bounded memo (`memo`), encodes and hashes
//!    the rewritten trees and the commit, and computes generations and
//!    changesets from the workers' diffs (no tree re-reads).
//! 4. The async writer stores each batch's objects concurrently (bounded by
//!    bytes in flight) and records commits strictly in order, only after the
//!    objects of their batch and all earlier ones are stored.
//!
//! Memory is flat in the length of the history: the memo is a fixed-size cache
//! (a miss just converts the object again, with identical output), the caches
//! are byte-bounded, and in-flight data is bounded by bytes. The only per-commit
//! state is the graph and the commit-id table. Mapped pack pages are released
//! periodically (`rss`).

mod commit;
mod convert;
mod graph;
mod memo;
mod prep;
mod read;
mod rss;
mod tree;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use ant_core::ObjectId;
use ant_store::{Backend, CasOutcome, CommitNode, PathChange};
use anyhow::{Context as _, anyhow, bail};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;

use crate::convert::{Converter, Sink};
use crate::graph::{COMMIT_KIND, discover};
use crate::memo::Memo;
use crate::prep::Pipeline;
use crate::read::Reader;

/// Bytes of object batches stored concurrently (commits are still recorded in order).
const INFLIGHT_BYTES: usize = 128 << 20;
/// Batches queued between the object writers and the in-order commit recorder.
const RECORD_QUEUE: usize = 64;
/// Commits recorded per `put_commits`: batches are merged up to this size.
const RECORD_COMMITS: usize = 1500;
/// Resident file-backed memory above which mapped packs are released.
const PACK_RSS_LIMIT: usize = 96 << 20;
/// Delta-base cache per git handle (one per worker thread).
const DELTA_CACHE_BYTES: usize = 16 << 20;
/// Largest memo, in objects (56 bytes each).
const MEMO_MAX_ENTRIES: usize = 1 << 21;

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

/// Called by the recorder after each object batch is stored (argument: the
/// number of batches so far), once everything pending has been recorded.
/// Tests use it to fail or pause an import at a known point.
#[derive(Clone)]
pub(crate) struct BatchHook(pub Arc<dyn Fn(usize) -> anyhow::Result<()> + Send + Sync>);

impl std::fmt::Debug for BatchHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BatchHook")
    }
}

/// Knobs for benchmarking and tests; the defaults suit production. Environment:
/// `ANT_IMPORT_WORKERS`, `ANT_IMPORT_MEMO_ENTRIES`, `ANT_IMPORT_DEFERRED`.
#[derive(Debug, Clone, Default)]
pub(crate) struct Tuning {
    pub workers: Option<usize>,
    pub memo_entries: Option<usize>,
    /// Commits per `put_commits` (default [`RECORD_COMMITS`]).
    pub record_commits: Option<usize>,
    /// Use deferred durability where the store supports it (see [`Publisher`]).
    /// Off by default: it makes commit recording 10x cheaper but, on the
    /// machines measured, only moves the wait to the object writes.
    pub deferred: bool,
    pub on_batch: Option<BatchHook>,
}

impl Tuning {
    fn from_env() -> Self {
        let get = |k: &str| std::env::var(k).ok().and_then(|v| v.parse().ok());
        Self {
            workers: get("ANT_IMPORT_WORKERS"),
            memo_entries: get("ANT_IMPORT_MEMO_ENTRIES"),
            deferred: std::env::var_os("ANT_IMPORT_DEFERRED").is_some_and(|v| v != "0"),
            ..Self::default()
        }
    }

    fn workers(&self) -> usize {
        self.workers.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map_or(4, |n| n.get())
                .saturating_sub(2)
                .clamp(2, 8)
        })
    }
}

/// A converted commit ready to be recorded.
pub(crate) struct NewCommit {
    pub node: CommitNode,
    pub changes: Vec<PathChange>,
    /// Source commit id (key of the derived mapping).
    pub git: gix::ObjectId,
}

/// Producer → writer messages. Objects always precede the commits that
/// reference them, so the writer can store a batch and then record its
/// commits parents-first.
pub(crate) enum Msg {
    Batch {
        objects: Vec<ant_core::EncodedObject>,
        bytes: usize,
        commits: Vec<NewCommit>,
        /// Ask the store which objects exist (needed for exact stats on
        /// incremental imports); skipped on fresh imports.
        check_present: bool,
    },
    /// Point `ant_ref` at `id` once everything before is recorded, then ack.
    Tip { ant_ref: String, id: ObjectId, ack: std::sync::mpsc::SyncSender<()> },
}

pub async fn import(
    repo: &Path,
    backend: &Backend,
    opts: &ImportOptions,
) -> anyhow::Result<ImportStats> {
    import_tuned(repo, backend, opts, Tuning::from_env()).await
}

pub(crate) async fn import_tuned(
    repo: &Path,
    backend: &Backend,
    opts: &ImportOptions,
    tuning: Tuning,
) -> anyhow::Result<ImportStats> {
    let batch_objects = opts.batch_objects.max(1);
    let publisher = Publisher::acquire(backend, &opts.refs, tuning.deferred).await?;
    let recorder = Recorder {
        commits: tuning.record_commits.unwrap_or(RECORD_COMMITS).max(1),
        hook: tuning.on_batch.clone(),
    };
    let (tx, rx) = mpsc::channel::<Msg>(4);
    let producer = {
        let repo = repo.to_path_buf();
        let backend = backend.clone();
        let refs = opts.refs.clone();
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            produce(&repo, backend, handle, &refs, batch_objects, tx, &tuning)
        })
    };
    let mut stats = ImportStats::default();
    let written = write(backend, rx, &mut stats, publisher, recorder).await;
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

struct Recorder {
    commits: usize,
    hook: Option<BatchHook>,
}

/// Moves the imported refs, and decides whether the import may use deferred
/// durability.
///
/// When the store has real ref locks (Postgres) every target ref gets a
/// *witness*: a lock on the private name `import-witness/<ref>`, taken before
/// any write and held to the end. It is not the target ref's own lock, so
/// lands on the target ref are not blocked while the import runs.
///
/// With witnesses the import runs with deferred durability: objects,
/// commits, changesets and mappings are acknowledged before their WAL flush,
/// which removes a synchronous flush per statement from the serial recording
/// path. Crash safety:
/// - The database log is sequential and a crash loses only a suffix of it.
///   Every write is acknowledged before the writes that depend on it are sent
///   (objects before the commits naming them, commits before their
///   mappings), so whatever survives a crash is a consistent prefix: a
///   re-run finds the surviving mappings and continues incrementally.
/// - Before a ref moves, the witness publishes a marker ref (one synchronous
///   commit on the witness session, then removed). The session predates the
///   deferred writes and a crash ends it, so this proves that no crash
///   happened and flushes every earlier write with it. Only then is the
///   target ref's lock taken, for the length of one compare-and-swap.
///
/// The swap is against the tip the import started from (per ref, updated as
/// the import moves it): if the ref moved meanwhile (a land), the import
/// stops with an error. Its commits stay recorded and a re-run publishes
/// them without converting anything. Two imports of one ref exclude each
/// other through the witness. Stores without real locks write synchronously.
struct Publisher {
    witnesses: Vec<Witness>,
    /// Tip of each ref as this import last saw it.
    expected: HashMap<String, Option<ObjectId>>,
}

struct Witness {
    ant_ref: String,
    marker: String,
    lock: ant_store::RefLock,
}

impl Publisher {
    async fn acquire(
        backend: &Backend,
        refs: &[(String, String)],
        defer: bool,
    ) -> anyhow::Result<Self> {
        let mut names: Vec<&str> = refs.iter().map(|(_, a)| a.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        let mut witnesses = Vec::new();
        let mut expected = HashMap::new();
        for name in names {
            if defer {
                let marker = format!("import-witness/{name}");
                let lock = backend
                    .refs
                    .lock_ref(&marker)
                    .await
                    .with_context(|| format!("locking import witness for ref {name}"))?;
                if lock.can_publish() {
                    // Left behind by an import that died between marker and removal.
                    if let Some(stale) = backend.refs.get_ref(&marker).await? {
                        backend.refs.cas_ref(&marker, Some(stale), None).await?;
                    }
                    witnesses.push(Witness { ant_ref: name.to_string(), marker, lock });
                }
            }
            expected.insert(name.to_string(), backend.refs.get_ref(name).await?);
        }
        Ok(Self { witnesses, expected })
    }

    /// Whether every ref has a witness (deferred durability is safe).
    fn deferred(&self) -> bool {
        !self.witnesses.is_empty() && self.witnesses.len() == self.expected.len()
    }

    /// Point `ant_ref` at `id`, durably, if it still is where the import left it.
    async fn publish(
        &mut self,
        backend: &Backend,
        ant_ref: &str,
        id: ObjectId,
    ) -> anyhow::Result<()> {
        let expected = self.expected.get(ant_ref).copied().flatten();
        let outcome = if self.deferred() {
            let w = self
                .witnesses
                .iter_mut()
                .find(|w| w.ant_ref == ant_ref)
                .ok_or_else(|| anyhow!("no import witness for ref {ant_ref}"))?;
            match w.lock.publish(None, id, "import.witness", Vec::new()).await? {
                CasOutcome::Updated => {}
                CasOutcome::Mismatch { .. } => bail!("marker ref {} already exists", w.marker),
            }
            let marker = w.marker.clone();
            let mut real = backend
                .refs
                .lock_ref(ant_ref)
                .await
                .with_context(|| format!("locking ref {ant_ref} to publish the import"))?;
            let swapped = if real.can_publish() {
                real.publish(expected, id, "import.tip", Vec::new()).await
            } else {
                backend.refs.cas_ref(ant_ref, expected, Some(id)).await
            };
            let _ = real.release().await;
            backend.refs.cas_ref(&marker, Some(id), None).await?;
            swapped?
        } else {
            backend.refs.cas_ref(ant_ref, expected, Some(id)).await?
        };
        match outcome {
            CasOutcome::Updated => {
                self.expected.insert(ant_ref.to_string(), Some(id));
                Ok(())
            }
            CasOutcome::Mismatch { actual } => bail!(
                "ref {ant_ref} moved during the import (now {actual:?}); the imported commits are \
                 recorded, re-run the import to publish them"
            ),
        }
    }

    async fn release(self) {
        for w in self.witnesses {
            let _ = w.lock.release().await;
        }
    }
}

enum Item {
    Batch(JoinHandle<anyhow::Result<Counts>>, Vec<NewCommit>),
    Tip { ant_ref: String, id: ObjectId, ack: std::sync::mpsc::SyncSender<()> },
}

async fn write(
    backend: &Backend,
    mut rx: mpsc::Receiver<Msg>,
    stats: &mut ImportStats,
    publisher: Publisher,
    config: Recorder,
) -> anyhow::Result<()> {
    let deferred = publisher.deferred();
    let (rec_tx, rec_rx) = mpsc::channel::<Item>(RECORD_QUEUE);
    let recorder = tokio::spawn(record(backend.clone(), rec_rx, publisher, config));
    let budget = Arc::new(Semaphore::new(INFLIGHT_BYTES >> 20));
    while let Some(msg) = rx.recv().await {
        let item = match msg {
            Msg::Batch { objects, bytes, commits, check_present } => {
                let mb = ((bytes >> 20) + 1).min(INFLIGHT_BYTES >> 20) as u32;
                let permit = budget.clone().acquire_many_owned(mb).await?;
                let b = backend.clone();
                let task = tokio::spawn(async move {
                    let _permit = permit;
                    if deferred {
                        ant_store::deferred_durability(store_objects(&b, objects, check_present))
                            .await
                    } else {
                        store_objects(&b, objects, check_present).await
                    }
                });
                Item::Batch(task, commits)
            }
            Msg::Tip { ant_ref, id, ack } => Item::Tip { ant_ref, id, ack },
        };
        if rec_tx.send(item).await.is_err() {
            break; // the recorder stopped; its error is reported below
        }
    }
    drop(rec_tx);
    *stats = recorder.await.context("commit recorder")??;
    Ok(())
}

/// Record commits in order: wait for each batch's objects, merge small
/// batches, and keep the derived mapping one step behind so it overlaps the
/// next commit write. A mapping is only written after its commits.
async fn record(
    backend: Backend,
    rx: mpsc::Receiver<Item>,
    mut publisher: Publisher,
    config: Recorder,
) -> anyhow::Result<ImportStats> {
    let result = if publisher.deferred() {
        ant_store::deferred_durability(record_inner(&backend, rx, &mut publisher, &config)).await
    } else {
        record_inner(&backend, rx, &mut publisher, &config).await
    };
    publisher.release().await;
    result
}

async fn record_inner(
    backend: &Backend,
    mut rx: mpsc::Receiver<Item>,
    publisher: &mut Publisher,
    config: &Recorder,
) -> anyhow::Result<ImportStats> {
    let deferred = publisher.deferred();
    let mut batches = 0usize;
    let mut stats = ImportStats::default();
    let mut pending: Vec<NewCommit> = Vec::new();
    let mut mapping_task: Option<JoinHandle<anyhow::Result<()>>> = None;

    async fn flush(
        deferred: bool,
        backend: &Backend,
        pending: &mut Vec<NewCommit>,
        mapping_task: &mut Option<JoinHandle<anyhow::Result<()>>>,
        stats: &mut ImportStats,
    ) -> anyhow::Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let mut mapping = Vec::with_capacity(pending.len());
        let mut records = Vec::with_capacity(pending.len());
        for c in pending.drain(..) {
            mapping.push((c.git.as_slice().to_vec(), c.node.id.as_bytes().to_vec().into()));
            records.push((c.node, c.changes));
        }
        stats.commits += records.len() as u64;
        backend.meta.put_commits(records).await?;
        if let Some(prev) = mapping_task.take() {
            prev.await.context("mapping writer")??;
        }
        let meta = backend.meta.clone();
        *mapping_task = Some(tokio::spawn(async move {
            // After the commits: a mapping must never point at an unrecorded commit.
            if deferred {
                ant_store::deferred_durability(meta.put_derived_many(COMMIT_KIND, mapping)).await?;
            } else {
                meta.put_derived_many(COMMIT_KIND, mapping).await?;
            }
            Ok(())
        }));
        Ok(())
    }

    while let Some(item) = rx.recv().await {
        match item {
            Item::Batch(task, commits) => {
                let counts = task.await.context("object writer task")??;
                stats.trees += counts.trees;
                stats.blobs += counts.blobs;
                pending.extend(commits);
                batches += 1;
                if pending.len() >= config.commits {
                    flush(deferred, backend, &mut pending, &mut mapping_task, &mut stats).await?;
                }
                if let Some(hook) = &config.hook {
                    flush(deferred, backend, &mut pending, &mut mapping_task, &mut stats).await?;
                    if let Some(prev) = mapping_task.take() {
                        prev.await.context("mapping writer")??;
                    }
                    tokio::task::block_in_place(|| (hook.0)(batches))?;
                }
            }
            Item::Tip { ant_ref, id, ack } => {
                flush(deferred, backend, &mut pending, &mut mapping_task, &mut stats).await?;
                if let Some(prev) = mapping_task.take() {
                    prev.await.context("mapping writer")??;
                }
                publisher.publish(backend, &ant_ref, id).await?;
                let _ = ack.send(());
            }
        }
    }
    flush(deferred, backend, &mut pending, &mut mapping_task, &mut stats).await?;
    if let Some(prev) = mapping_task.take() {
        prev.await.context("mapping writer")??;
    }
    Ok(stats)
}

/// Store the objects the target does not already hold; only those are counted.
async fn store_objects(
    backend: &Backend,
    objects: Vec<ant_core::EncodedObject>,
    check_present: bool,
) -> anyhow::Result<Counts> {
    use ant_core::ObjectKind;
    let mut counts = Counts::default();
    if objects.is_empty() {
        return Ok(counts);
    }
    let missing: Vec<_> = if check_present {
        let ids: Vec<ObjectId> = objects.iter().map(|o| o.id()).collect();
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
    tuning: &Tuning,
) -> anyhow::Result<()> {
    // Every thread has its own handle, each with a delta-base cache that
    // defaults to ~100 MiB: bound it so memory does not scale with threads.
    let options = gix::open::Options::default()
        .config_overrides([format!("gitoxide.core.deltaBaseCacheLimit={DELTA_CACHE_BYTES}")]);
    let repo = gix::open_opts(repo_path, options)
        .with_context(|| format!("opening git repo {}", repo_path.display()))?;
    if repo.object_hash() != gix::hash::Kind::Sha1 {
        bail!("only SHA-1 source repositories are supported");
    }
    let sync = repo.into_sync();
    rss::limit_arenas();
    let workers = tuning.workers();
    let progress = std::env::var_os("ANT_IMPORT_STATS").is_some();
    let mut sink = Some(Sink::new(tx, batch_objects));
    let mut memo: Option<Memo> = None;
    for (git_ref, ant_ref) in refs {
        let tip = resolve(&sync.to_thread_local(), git_ref)?;
        let graph = discover(&sync, &backend, &handle, tip, workers)?;
        let mut s = sink.take().ok_or_else(|| anyhow!("import sink lost"))?;
        s.incremental |= !graph.known.is_empty();
        let m = memo.take().unwrap_or_else(|| {
            let want = tuning
                .memo_entries
                .unwrap_or_else(|| (graph.order.len() * 12).clamp(1 << 14, MEMO_MAX_ENTRIES));
            Memo::with_capacity(want)
        });
        let mut conv = Converter::new(&graph, Reader::new(sync.to_thread_local()), m, s);
        let pipeline = Pipeline::new(&graph, workers);
        let result = std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| pipeline.work(&sync));
            }
            let r = (|| {
                let mut chunks = 0usize;
                while let Some(chunk) = pipeline.next() {
                    for prep in chunk? {
                        conv.convert(prep)?;
                    }
                    chunks += 1;
                    if progress && chunks.is_multiple_of(2000) {
                        eprintln!(
                            "import: commits={} memo hits={} misses={} fallback trees={} blobs={} ids={}",
                            conv.stats.commits,
                            conv.memo.hits,
                            conv.memo.misses,
                            conv.stats.fallback_trees,
                            conv.stats.fallback_blobs,
                            conv.stats.fallback_ids
                        );
                    }
                    if chunks.is_multiple_of(4) {
                        rss::trim_mapped_packs(PACK_RSS_LIMIT);
                        rss::release_heap();
                    }
                }
                Ok::<(), anyhow::Error>(())
            })();
            pipeline.stop();
            r
        });
        result?;
        conv.sink().flush()?;
        let id = conv.commit_id(0).ok_or_else(|| anyhow!("{git_ref} was not converted"))?;
        tracing::info!(
            git_ref,
            commits = conv.stats.commits,
            memo_hits = conv.memo.hits,
            memo_misses = conv.memo.misses,
            fallback_trees = conv.stats.fallback_trees,
            fallback_blobs = conv.stats.fallback_blobs,
            fallback_ids = conv.stats.fallback_ids,
            "converted"
        );
        let (m, s) = conv.into_parts();
        let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
        s.send(Msg::Tip { ant_ref: ant_ref.clone(), id, ack: ack_tx })?;
        // Wait until this ref's commits are recorded: the next ref's discovery
        // asks the store which commits already exist.
        ack_rx.recv().map_err(|_| anyhow!("import writer stopped"))?;
        memo = Some(m);
        sink = Some(s);
    }
    Ok(())
}

fn resolve(repo: &gix::Repository, name: &str) -> anyhow::Result<gix::ObjectId> {
    let mut reference = repo.find_reference(name).with_context(|| format!("resolving {name}"))?;
    Ok(reference.peel_to_id()?.detach())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::process::Command;

    use super::*;
    use ant_core::{ObjectId, RepoPath};
    use ant_store::ObjectStoreExt;
    use ant_store::graph::lookup_path;

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
        std::fs::set_permissions(
            r.join("run.sh"),
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();
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
        let stats =
            import(&src, &b, &ImportOptions::single("refs/heads/main", "main")).await.unwrap();
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
        let f = lookup_path(b.objects.as_ref(), c.tree, &RepoPath::new("a/c.txt").unwrap())
            .await
            .unwrap()
            .unwrap();
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
            git_env(
                dir,
                &["-c", "i18n.commitEncoding=ISO-8859-1", "commit", "-q", "-F", msg_file],
                env,
            );
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
        std::fs::write(r.join("dir").join(std::ffi::OsStr::from_bytes(b"bad-\xe9.txt")), "x")
            .unwrap();
        git(&r, &["add", "-A"]);
        git(&r, &["commit", "-qm", "bad name"]);
        let head = git(&r, &["rev-parse", "HEAD"]);

        let b = Backend::in_memory();
        let err =
            import(&r, &b, &ImportOptions::single("refs/heads/main", "main")).await.unwrap_err();
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
            let expected = ant_store::graph::diff_trees(b.objects.as_ref(), first, Some(c.tree))
                .await
                .unwrap();
            assert_eq!(
                b.meta.get_changeset(id).await.unwrap().unwrap(),
                expected,
                "changeset of {id}"
            );
            let parent_gens = b.meta.get_commits(&c.parents).await.unwrap();
            let parent_gen =
                parent_gens.iter().map(|p| p.as_ref().unwrap().generation).max().unwrap_or(0);
            assert_eq!(node.generation, parent_gen + 1);
            assert_eq!(node.committer_time, c.committer.time);
        }
    }

    /// Deterministic pseudo-random history with branches, merges, renames, mode
    /// flips, file <-> directory flips and reverts.
    fn make_random_repo(root: &Path, commits: usize) -> PathBuf {
        let r = root.join("rand");
        std::fs::create_dir(&r).unwrap();
        git(&r, &["init", "-q", "-b", "main"]);
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let dirs = ["", "a/", "a/b/", "c/", "c/d/e/", "x-y/"];
        let mut on_side = false;
        let mut n_side = 0;
        for i in 0..commits {
            let ops = 1 + next(3);
            for _ in 0..ops {
                let dir = dirs[next(dirs.len() as u64) as usize];
                let name = format!("{dir}f{}", next(6));
                let path = r.join(&name);
                match next(8) {
                    0..=2 => {
                        if path.is_dir() {
                            continue;
                        }
                        std::fs::create_dir_all(path.parent().unwrap()).ok();
                        // few distinct contents: repeated blobs and reverts
                        std::fs::write(&path, format!("content {}\n", next(5))).unwrap();
                    }
                    3 => {
                        if path.is_file() {
                            std::fs::remove_file(&path).unwrap();
                        }
                    }
                    4 => {
                        if path.is_file() {
                            let to = r.join(format!("{dir}g{}", next(3)));
                            if !to.exists() {
                                std::fs::rename(&path, &to).unwrap();
                            }
                        }
                    }
                    5 => {
                        if path.is_file() {
                            let mode = if next(2) == 0 { 0o755 } else { 0o644 };
                            std::fs::set_permissions(
                                &path,
                                std::os::unix::fs::PermissionsExt::from_mode(mode),
                            )
                            .unwrap();
                        }
                    }
                    6 => {
                        if path.is_file() {
                            std::fs::remove_file(&path).unwrap();
                            std::fs::create_dir_all(&path).unwrap();
                            std::fs::write(path.join("inner"), "inner\n").unwrap();
                        }
                    }
                    _ => {
                        if path.is_dir() {
                            std::fs::remove_dir_all(&path).unwrap();
                            std::fs::write(&path, "was a dir\n").unwrap();
                        }
                    }
                }
            }
            git(&r, &["add", "-A"]);
            git(&r, &["commit", "-q", "--allow-empty", "-m", &format!("c{i}")]);
            if !on_side && next(6) == 0 {
                git(&r, &["checkout", "-qb", &format!("s{n_side}")]);
                on_side = true;
            } else if on_side && next(3) == 0 {
                let side = format!("s{n_side}");
                n_side += 1;
                git(&r, &["checkout", "-q", "main"]);
                let merged = Command::new("git")
                    .args(["merge", "-q", "--no-edit", "-X", "theirs", &side])
                    .current_dir(&r)
                    .env("GIT_AUTHOR_NAME", "Ada")
                    .env("GIT_AUTHOR_EMAIL", "ada@example.com")
                    .env("GIT_COMMITTER_NAME", "Bob")
                    .env("GIT_COMMITTER_EMAIL", "bob@example.com")
                    .output()
                    .unwrap();
                if !merged.status.success() {
                    git(&r, &["merge", "--abort"]);
                }
                on_side = false;
            }
        }
        if on_side {
            git(&r, &["checkout", "-q", "main"]);
        }
        r
    }

    /// Whatever the worker count and however small (so evicting) the memo is,
    /// the import produces the same tip, and every recorded changeset equals a
    /// re-read tree diff.
    #[tokio::test]
    async fn random_history_is_independent_of_tuning() {
        let tmp = tempfile::tempdir().unwrap();
        let src = make_random_repo(tmp.path(), 120);
        let mut first_tip = None;
        for (workers, memo_entries) in [(1, Some(1024)), (4, None), (3, Some(1024))] {
            let b = Backend::in_memory();
            let tuning = Tuning { workers: Some(workers), memo_entries, ..Tuning::default() };
            let mut opts = ImportOptions::single("refs/heads/main", "main");
            opts.batch_objects = 7;
            let stats = import_tuned(&src, &b, &opts, tuning).await.unwrap();
            assert!(stats.commits > 100);
            let tip = b.refs.get_ref("main").await.unwrap().unwrap();
            assert_eq!(
                *first_tip.get_or_insert(tip),
                tip,
                "workers={workers} memo={memo_entries:?}"
            );
            let log = b.meta.tail(0, 1000).await.unwrap();
            assert_eq!(log.len() as u64, stats.commits);
            for (_, id) in &log {
                let c = b.objects.read_commit(id).await.unwrap();
                let first = match c.parents.first() {
                    Some(p) => Some(b.objects.read_commit(p).await.unwrap().tree),
                    None => None,
                };
                let want = ant_store::graph::diff_trees(b.objects.as_ref(), first, Some(c.tree))
                    .await
                    .unwrap();
                assert_eq!(b.meta.get_changeset(id).await.unwrap().unwrap(), want, "{id}");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Postgres: deferred durability, interruption, concurrent lands
    // -----------------------------------------------------------------------

    use ant_store::postgres::PgStore;
    use std::sync::Mutex;
    use std::sync::mpsc as std_mpsc;

    async fn pg_backend() -> Backend {
        let url = std::env::var("ANT_TEST_PG_URL")
            .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into());
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let schema = format!("t_imp_{}_{nanos}", std::process::id());
        let pg = Arc::new(
            PgStore::connect_scratch(&url, &schema)
                .await
                .expect("connect (is `scripts/dev-services.sh start` running?)"),
        );
        Backend { objects: pg.clone(), refs: pg.clone(), meta: pg }
    }

    /// `n` commits in a line, each touching one of a few files.
    fn make_linear(root: &Path, n: usize) -> PathBuf {
        let r = root.join("lin");
        std::fs::create_dir(&r).unwrap();
        git(&r, &["init", "-q", "-b", "main"]);
        for i in 0..n {
            std::fs::write(r.join(format!("f{}.txt", i % 4)), format!("content {i}\n")).unwrap();
            git(&r, &["add", "-A"]);
            git(&r, &["commit", "-qm", &format!("commit {i}")]);
        }
        r
    }

    type Snapshot =
        (Vec<ObjectId>, Vec<Option<ant_store::CommitNode>>, Vec<Option<Vec<PathChange>>>);

    async fn snapshot(b: &Backend) -> Snapshot {
        let log: Vec<ObjectId> =
            b.meta.tail(0, usize::MAX).await.unwrap().into_iter().map(|(_, id)| id).collect();
        let nodes = b.meta.get_commits(&log).await.unwrap();
        let sets = b.meta.get_changesets(&log).await.unwrap();
        (log, nodes, sets)
    }

    fn small_batches(workers: usize) -> (ImportOptions, Tuning) {
        let mut opts = ImportOptions::single("refs/heads/main", "main");
        opts.batch_objects = 1;
        (
            opts,
            Tuning {
                workers: Some(workers),
                record_commits: Some(2),
                deferred: true,
                ..Tuning::default()
            },
        )
    }

    /// The deferred path stores exactly what the synchronous path stores.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs postgres: scripts/dev-services.sh start"]
    async fn pg_deferred_import_equals_synchronous_import() {
        let tmp = tempfile::tempdir().unwrap();
        let src = make_linear(tmp.path(), 12);
        let (opts, tuning) = small_batches(2);

        let deferred = pg_backend().await;
        let p = Publisher::acquire(&deferred, &opts.refs, true).await.unwrap();
        assert!(p.deferred(), "postgres must take the deferred path");
        p.release().await;
        let a = import_tuned(&src, &deferred, &opts, tuning.clone()).await.unwrap();

        let sync = pg_backend().await;
        let sync_tuning = Tuning { deferred: false, ..tuning };
        let b = import_tuned(&src, &sync, &opts, sync_tuning).await.unwrap();

        assert_eq!(a, b);
        assert_eq!(a.commits, 12);
        assert_eq!(snapshot(&deferred).await, snapshot(&sync).await);
        let tip = deferred.refs.get_ref("main").await.unwrap();
        assert!(tip.is_some());
        assert_eq!(tip, sync.refs.get_ref("main").await.unwrap());
        assert_eq!(deferred.refs.get_ref("import-witness/main").await.unwrap(), None);
        assert_eq!(deferred.refs.list_refs("").await.unwrap().len(), 1, "no marker refs remain");
    }

    /// An import that dies midway publishes nothing; a re-run continues
    /// from what was recorded and ends in the same state as an undisturbed one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs postgres: scripts/dev-services.sh start"]
    async fn pg_interrupted_import_resumes_incrementally() {
        let tmp = tempfile::tempdir().unwrap();
        let src = make_linear(tmp.path(), 12);
        let (opts, tuning) = small_batches(2);

        let b = pg_backend().await;
        let fail = Tuning {
            on_batch: Some(BatchHook(Arc::new(|n| {
                if n == 6 { Err(anyhow!("failpoint")) } else { Ok(()) }
            }))),
            ..tuning.clone()
        };
        let err = import_tuned(&src, &b, &opts, fail).await.unwrap_err();
        assert!(format!("{err:#}").contains("failpoint"), "{err:#}");
        assert_eq!(b.refs.get_ref("main").await.unwrap(), None, "ref must not move");
        assert_eq!(b.refs.get_ref("import-witness/main").await.unwrap(), None);
        let recorded = b.meta.tail(0, usize::MAX).await.unwrap().len();
        assert!((1..12).contains(&recorded), "partial prefix recorded, got {recorded}");

        let resumed = import_tuned(&src, &b, &opts, tuning.clone()).await.unwrap();
        assert_eq!(resumed.commits as usize, 12 - recorded, "only the rest is converted");

        let clean = pg_backend().await;
        import_tuned(&src, &clean, &opts, tuning).await.unwrap();
        // Same commits, nodes, changesets and tip (the log order of the resumed
        // run is its own parents-first order).
        let (mut log_a, ..) = snapshot(&b).await;
        let (mut log_b, ..) = snapshot(&clean).await;
        log_a.sort();
        log_b.sort();
        assert_eq!(log_a, log_b);
        let tip = b.refs.get_ref("main").await.unwrap();
        assert_eq!(tip, clean.refs.get_ref("main").await.unwrap());
        let tip = tip.unwrap();
        assert_eq!(
            b.meta.get_commits(&[tip]).await.unwrap(),
            clean.meta.get_commits(&[tip]).await.unwrap()
        );
        assert_eq!(
            b.meta.get_changesets(&log_a).await.unwrap(),
            clean.meta.get_changesets(&log_a).await.unwrap()
        );
    }

    /// Stops the import at its second batch, runs `during`, then lets it go on.
    async fn import_paused<F, Fut>(
        src: &Path,
        b: &Backend,
        during: F,
    ) -> anyhow::Result<ImportStats>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let (opts, tuning) = small_batches(2);
        let (at_tx, at_rx) = std_mpsc::channel::<()>();
        let (go_tx, go_rx) = std_mpsc::channel::<()>();
        let (at_tx, go_rx) = (Mutex::new(at_tx), Mutex::new(go_rx));
        let tuning = Tuning {
            on_batch: Some(BatchHook(Arc::new(move |n| {
                if n == 2 {
                    let _ = at_tx.lock().unwrap().send(());
                    let _ = go_rx.lock().unwrap().recv();
                }
                Ok(())
            }))),
            ..tuning
        };
        let (src, bb) = (src.to_path_buf(), b.clone());
        let import = tokio::spawn(async move { import_tuned(&src, &bb, &opts, tuning).await });
        tokio::task::spawn_blocking(move || at_rx.recv()).await.unwrap().unwrap();
        during().await;
        go_tx.send(()).unwrap();
        import.await.unwrap()
    }

    /// Lands on other refs, and lock + swap on the imported ref, proceed
    /// while the import is running.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs postgres: scripts/dev-services.sh start"]
    async fn pg_lands_proceed_during_an_import() {
        let tmp = tempfile::tempdir().unwrap();
        let src = make_linear(tmp.path(), 8);
        let b = pg_backend().await;
        let id = ObjectId::from_bytes(&[7u8; 32]).unwrap();
        let during = || async {
            let wait = std::time::Duration::from_secs(5);
            // Another ref: lock, swap.
            let mut other = tokio::time::timeout(wait, b.refs.lock_ref("other"))
                .await
                .expect("lock on another ref")
                .unwrap();
            assert!(other.publish(None, id, "t", vec![]).await.is_ok());
            other.release().await.unwrap();
            // The imported ref itself: a land takes its lock freely.
            let lock = tokio::time::timeout(wait, b.refs.lock_ref("main"))
                .await
                .expect("lock on the imported ref")
                .unwrap();
            assert_eq!(b.refs.get_ref("main").await.unwrap(), None);
            lock.release().await.unwrap();
        };
        import_paused(&src, &b, during).await.unwrap();
        assert!(b.refs.get_ref("main").await.unwrap().is_some());
        assert_eq!(b.refs.get_ref("other").await.unwrap(), Some(id));
    }

    /// A land that moves the imported ref meanwhile is never overwritten: the
    /// import fails clearly, and a re-run publishes without converting again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs postgres: scripts/dev-services.sh start"]
    async fn pg_import_does_not_overwrite_a_ref_that_moved() {
        let tmp = tempfile::tempdir().unwrap();
        let src = make_linear(tmp.path(), 8);
        let b = pg_backend().await;
        let id = ObjectId::from_bytes(&[9u8; 32]).unwrap();
        let during = || async {
            let lock = b.refs.lock_ref("main").await.unwrap();
            assert!(matches!(
                b.refs.cas_ref("main", None, Some(id)).await.unwrap(),
                CasOutcome::Updated
            ));
            lock.release().await.unwrap();
        };
        let err = import_paused(&src, &b, during).await.unwrap_err();
        assert!(format!("{err:#}").contains("moved during the import"), "{err:#}");
        assert_eq!(b.refs.get_ref("main").await.unwrap(), Some(id));
        assert_eq!(b.refs.get_ref("import-witness/main").await.unwrap(), None);

        // The operator resets the ref; nothing is converted again.
        b.refs.cas_ref("main", Some(id), None).await.unwrap();
        let (opts, tuning) = small_batches(2);
        let again = import_tuned(&src, &b, &opts, tuning).await.unwrap();
        assert_eq!(again.commits, 0);
        assert!(b.refs.get_ref("main").await.unwrap().is_some());
    }
}
