//! Path history and blame, backed by the per-ref history index
//! (`MetaStore::{index_history, query_history, ...}`).
//!
//! - `schedule_catch_up(b, ref, metrics)` runs `catch_up` in the background,
//!   coalesced per ref, after every successful `Land` and every `UpdateRef`
//!   to a new target (best effort: errors are logged; queries fall back, see
//!   `log`).
//! - `ops::query::log_as` with a path: a rev naming a ref goes to
//!   `lagging_path_log`, which reads that ref's watermark once — rev at or
//!   behind it → index only; rev ahead of it (a busy ref between catch-ups)
//!   → scan only the unindexed commits and take the rest from the index. A
//!   commit-id rev uses `path_log` when it `is_first_parent_ancestor` of an
//!   indexed ref's watermark (`indexed_ref_for`). Anything else scans
//!   recorded changesets — on a ref under constant landing that scan walks
//!   the whole history, so the index paths must cover every race between
//!   resolving the rev and a concurrent catch-up. When the index cannot
//!   serve a ref-named rev (no index yet, e.g. a ref imported straight
//!   into the store; lag past `MAX_LAG`; a non-fast-forward move), the
//!   query also schedules a catch-up of that ref, so only the first
//!   queries scan.
//! - Exposed as the `Blame` RPC and `ant blame <rev>:<path>` (output per
//!   line: `<commit-id> <line-no>\t<line>`).
//!
//! `catch_up(b, ref_name)`:
//! - Let `tip` = ref target, `wm` = watermark. If `wm` is None → index the
//!   whole first-parent chain of `tip`. If `wm` is a first-parent ancestor of
//!   `tip` → index only commits after it. Otherwise (ref moved non-FF) →
//!   `clear_history` and rebuild from scratch.
//! - Rows: for each commit on the chain, one row per path in its recorded
//!   changeset (one batched `MetaStore::get_changesets` per batch), with the commit's generation.
//! - The chain is walked through the node cache as far as it reaches
//!   (recently landed commits) and the changesets come from the changeset
//!   cache where present, so a catch-up behind a busy ref costs no chain
//!   query.
//! - Index writes (rows, watermark, a rebuild's clearing) use deferred
//!   durability (`ant_store::deferred_durability`): the index is derived
//!   data, each batch's rows commit before the watermark that covers them,
//!   and the database log is sequential, so a crash loses at most a suffix
//!   (never a watermark without its rows) and the next catch-up redoes it.
//! - Batches of ≤ 1000 commits; write rows then CAS the watermark forward
//!   (a concurrent catch-up winning the CAS is fine — rows are idempotent;
//!   loops until the watermark is `tip` or the ref moves again).
//! - Long histories are walked iteratively, first parents in batches, never
//!   loading all commits at once.
//!
//! `path_log(b, ref_name, rev_generation, path, limit)`: `query_history` →
//! `LogEntry`s (commit objects through the commit cache, misses in one
//! `get_many`).
//!
//! `blame(b, rev, path)`: per-line attribution of the file at `rev`.
//! Walks the file's history newest→oldest (the index when available, else
//! first-parent changesets); each change line-maps the previous version
//! against the newer one (`diff_texts`: common prefix/suffix found bytewise,
//! the changed middle interned to integer line ids and diffed by a
//! bit-parallel LCS with a deterministic tie-break; `line_map` in the tests
//! is the quadratic reference with identical results) and attributes lines
//! that commit introduced; lines surviving to the file's creation belong to
//! the creating commit. A producer task reads change batches (16, doubling
//! to 128) and starts their blob `get_many`s a few batches ahead of the
//! walk.
//!
//! Blame is recursively derived data (`ant_store::derive`, kind
//! [`Blame`], `"blame/v2"`): the value is keyed by `(L, path)` where `L` is
//! the last commit on `rev`'s first-parent chain that changed `path` (the
//! first entry of its history, whose new blob is the file at `rev`), so all
//! revisions sharing that last change share one value. Value of `(L, path)`
//! = the previous version's value pushed through the line map of `L`'s
//! change, lines `L` introduced owned by `L` (`Blame::derive`). Evaluated
//! iteratively: every history batch is probed for stored values (one
//! `get_derived_many`, alongside its blob reads), and the walk stops at the
//! first version that has one, taking the owners of the lines still open
//! from it; so after a change to the file only that change is diffed.
//! Deferred policy: only the queried `(L, path)` is stored. A stored value
//! is used only when its version's blob is the one being walked and its
//! line count matches. Older servers' `"blame/v1"` rows (keyed by rev
//! commit) are no longer read; GC deletes both kinds with their commits.
//! Binary (non-UTF-8) files → `InvalidArgument`. Missing path → `NotFound`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};

use ant_core::{ObjectId, RepoPath};
use ant_store::derive::{self, Derivation, Policy};
use ant_store::{Backend, ChangeKind, HistoryRow, PathChange, StoreError};
use bytes::Bytes;

use super::query::{self, LogEntry};
use super::{OpError, OpResult, cache};
use crate::acl::Access;
use crate::metrics::Metrics;

/// Commits per index batch (changeset fetch + row write + watermark CAS).
const INDEX_BATCH: usize = 1000;
/// Nodes fetched per `first_parent_chain` call.
const CHAIN_CHUNK: usize = 8192;
/// Attempts before `catch_up` gives up on a ref that keeps moving.
const MAX_ROUNDS: usize = 4;
/// Derived-data kind of [`Blame`] values (keyed by the last commit changing
/// the path); `"blame/v1"` (keyed by the queried commit) is no longer used.
pub(crate) const BLAME_KIND: &str = "blame/v2";
/// Index rows read before the rest of a file's history (most blames end at
/// a stored value within the first few versions).
const INDEX_FIRST_PAGE: usize = 64;
/// Largest batch of changes (and previous-version blobs) fetched per blame step.
const BLAME_CHUNK: usize = 128;
/// First (small) batch of a blame walk, so short histories fetch little.
const BLAME_FIRST_CHUNK: usize = 16;
/// Batches read ahead of the walk.
const BLAME_PREFETCH: usize = 4;
/// Cap on LCS table cells for one line diff; larger middles count as replaced.
const MAX_LCS_CELLS: usize = 4_000_000;
/// Unindexed commits `lagging_path_log` scans before giving up on the index.
const MAX_LAG: usize = 4096;
/// Refs examined when looking for an index that covers a commit id.
const MAX_REFS_PROBED: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlameLine {
    pub commit: ObjectId,
    pub line: String,
}

/// Walk the first-parent chain from `tip` down to (excluding) `stop`, or to
/// the root. Returns `(id, generation)` newest first and whether `stop` was
/// reached (always false when `stop` is None). Commits in the node cache
/// (recently landed history, the common case of a catch-up behind a busy
/// ref) are walked in memory; otherwise one store call per `CHAIN_CHUNK`
/// commits.
async fn walk_chain(
    b: &Backend,
    tip: ObjectId,
    stop: Option<ObjectId>,
) -> OpResult<(Vec<(ObjectId, u64)>, bool)> {
    let mut out = Vec::new();
    let mut from = tip;
    let nodes = &cache::meta(&b.meta).nodes;
    loop {
        if Some(from) == stop {
            return Ok((out, true));
        }
        if let Some(node) = nodes.get(&from) {
            out.push((node.id, node.generation));
            match node.parents.first() {
                Some(parent) => from = *parent,
                None => return Ok((out, false)),
            }
            continue;
        }
        let chunk = b.meta.first_parent_chain(from, stop, CHAIN_CHUNK).await?;
        let Some(last) = chunk.last() else {
            return Err(OpError::not_found_commit(from));
        };
        out.extend(chunk.iter().map(|n| (n.id, n.generation)));
        match last.parents.first() {
            None => return Ok((out, false)),
            Some(p) if Some(*p) == stop => return Ok((out, true)),
            Some(p) if chunk.len() == CHAIN_CHUNK => from = *p,
            Some(p) => return Err(OpError::not_found_commit(*p)),
        }
    }
}

/// True if `ancestor` lies on the first-parent chain of `descendant`
/// (inclusive; `ant_store::graph::is_first_parent_ancestor`).
pub(crate) async fn is_first_parent_ancestor(
    b: &Backend,
    ancestor: ObjectId,
    descendant: ObjectId,
) -> OpResult<bool> {
    Ok(ant_store::graph::is_first_parent_ancestor(b, ancestor, descendant).await?)
}

/// Bring `ref_name`'s history index up to date. Returns commits indexed.
pub async fn catch_up(b: &Backend, ref_name: &str) -> OpResult<usize> {
    catch_up_observed(b, ref_name, None).await
}

/// [`catch_up`] reporting the index lag and the duration of runs that indexed
/// something to `metrics`.
async fn catch_up_observed(
    b: &Backend,
    ref_name: &str,
    metrics: Option<&Metrics>,
) -> OpResult<usize> {
    let start = std::time::Instant::now();
    let indexed = catch_up_rounds(b, ref_name, metrics).await?;
    if let (Some(m), true) = (metrics, indexed > 0) {
        m.record_catch_up(start.elapsed().as_secs_f64());
    }
    Ok(indexed)
}

async fn catch_up_rounds(
    b: &Backend,
    ref_name: &str,
    metrics: Option<&Metrics>,
) -> OpResult<usize> {
    ant_store::validate_ref_name(ref_name)?;
    let lag = |commits: usize| {
        if let Some(m) = metrics {
            m.set_history_lag(ref_name, commits);
        }
    };
    let mut indexed = 0;
    for _ in 0..MAX_ROUNDS {
        let Some(tip) = b.refs.get_ref(ref_name).await? else { return Ok(indexed) };
        let mut wm = b.meta.history_watermark(ref_name).await?;
        if wm == Some(tip) {
            lag(0);
            return Ok(indexed);
        }
        // Newest first; ends before the watermark when it is an ancestor.
        let (mut chain, hit) = walk_chain(b, tip, wm).await?;
        if wm.is_some() && !hit {
            // Ref moved non-fast-forward: `chain` is the full new chain.
            ant_store::deferred_durability(b.meta.clear_history(ref_name)).await?;
            wm = None;
        }
        chain.reverse();
        lag(chain.len());
        let mut pos = 0;
        while pos < chain.len() {
            let end = (pos + INDEX_BATCH).min(chain.len());
            let batch = &chain[pos..end];
            let ids: Vec<ObjectId> = batch.iter().map(|(id, _)| *id).collect();
            let sets = cached_changesets(b, &ids).await?;
            let mut rows = Vec::new();
            for ((id, generation), set) in batch.iter().zip(sets) {
                let set = set.ok_or_else(|| OpError::NotFound(format!("changeset of {id}")))?;
                rows.extend(set.iter().map(|c| HistoryRow {
                    path: c.path.clone(),
                    generation: *generation,
                    commit: *id,
                }));
            }
            // Deferred durability (see the module docs): the rows commit
            // before the watermark that covers them.
            ant_store::deferred_durability(b.meta.index_history(ref_name, &rows)).await?;
            let last = batch[batch.len() - 1].0;
            if ant_store::deferred_durability(b.meta.set_history_watermark(ref_name, wm, last))
                .await?
            {
                indexed += batch.len();
                wm = Some(last);
                pos = end;
                lag(chain.len() - pos);
                continue;
            }
            // Someone else advanced the watermark; rows are idempotent. Skip
            // ahead if it is on our chain, otherwise re-plan from scratch.
            let now = b.meta.history_watermark(ref_name).await?;
            match now.and_then(|w| chain.iter().position(|(id, _)| *id == w)) {
                Some(i) => {
                    wm = now;
                    pos = i + 1;
                }
                None => break,
            }
        }
        if pos >= chain.len() && b.refs.get_ref(ref_name).await? == Some(tip) {
            lag(0);
            return Ok(indexed);
        }
    }
    Ok(indexed)
}

/// Index-aligned changesets, from the changeset cache where present (a
/// catch-up behind a busy ref indexes what was just landed); misses are read
/// from the store and not cached, so rebuilding a long history does not
/// churn the cache.
async fn cached_changesets(
    b: &Backend,
    ids: &[ObjectId],
) -> OpResult<Vec<Option<Arc<Vec<PathChange>>>>> {
    let mut out = cache::meta(&b.meta).changesets.get_many(ids);
    let missing: Vec<ObjectId> =
        ids.iter().zip(&out).filter(|(_, set)| set.is_none()).map(|(id, _)| *id).collect();
    if !missing.is_empty() {
        let mut fetched = b.meta.get_changesets(&missing).await?.into_iter();
        for slot in out.iter_mut().filter(|set| set.is_none()) {
            *slot = fetched.next().flatten().map(Arc::new);
        }
    }
    Ok(out)
}

type CatchUpKey = (usize, String);
/// Running background catch-ups; the flag means "run once more when done".
static CATCH_UPS: LazyLock<Mutex<HashMap<CatchUpKey, bool>>> = LazyLock::new(Default::default);

/// Number of (backend, ref) catch-ups currently running in the background.
pub(crate) fn running_catch_ups() -> usize {
    crate::lock(&CATCH_UPS).len()
}

/// Fire-and-forget [`catch_up`], coalesced per (backend, ref): if one is
/// already running it is told to run once more instead of starting a second.
/// Reports to `metrics` like [`catch_up_observed`]. Errors are logged;
/// queries fall back to changeset scans.
pub fn schedule_catch_up(b: &Backend, ref_name: &str, metrics: Option<&Arc<Metrics>>) {
    let key = (Arc::as_ptr(&b.meta).cast::<()>() as usize, ref_name.to_string());
    {
        let mut running = crate::lock(&CATCH_UPS);
        if let Some(dirty) = running.get_mut(&key) {
            *dirty = true;
            return;
        }
        running.insert(key.clone(), false);
    }
    let b = b.clone();
    let ref_name = ref_name.to_string();
    let metrics = metrics.cloned();
    tokio::spawn(async move {
        loop {
            if let Err(error) = catch_up_observed(&b, &ref_name, metrics.as_deref()).await {
                tracing::warn!(%error, ref_name, "history catch-up failed");
            }
            let mut running = crate::lock(&CATCH_UPS);
            match running.get_mut(&key) {
                Some(dirty) if *dirty => *dirty = false,
                _ => {
                    running.remove(&key);
                    return;
                }
            }
        }
    });
}

/// An indexed ref whose first-parent chain (as indexed so far) contains `id`.
/// `rev` is the user's revision string; a leading ref name is tried first.
pub async fn indexed_ref_for(b: &Backend, rev: &str, id: ObjectId) -> OpResult<Option<String>> {
    let mut candidates = Vec::new();
    if let Some(base) = rev_ref(rev) {
        candidates.push(base.to_string());
    } else {
        candidates.extend(
            b.refs.list_refs("").await?.into_iter().take(MAX_REFS_PROBED).map(|(name, _)| name),
        );
    }
    for name in candidates {
        if let Some(wm) = b.meta.history_watermark(&name).await?
            && is_first_parent_ancestor(b, id, wm).await?
        {
            return Ok(Some(name));
        }
    }
    Ok(None)
}

pub async fn path_log(
    b: &Backend,
    ref_name: &str,
    max_generation: u64,
    path: &RepoPath,
    limit: usize,
) -> OpResult<Vec<LogEntry>> {
    let limit = if limit == 0 { usize::MAX } else { limit };
    let hits = b.meta.query_history(ref_name, path, max_generation, limit).await?;
    log_entries(b, hits).await
}

/// Read the commit objects of `(generation, id)` hits through the commit
/// cache (recent commits are read by every log): at most one round trip.
pub(crate) async fn log_entries(
    b: &Backend,
    hits: Vec<(u64, ObjectId)>,
) -> OpResult<Vec<LogEntry>> {
    let ids: Vec<ObjectId> = hits.iter().map(|(_, id)| *id).collect();
    let commits = cache::read_commits(b, &ids).await?;
    Ok(hits
        .into_iter()
        .zip(commits)
        .map(|((generation, id), commit)| LogEntry { id, generation, commit: (*commit).clone() })
        .collect())
}

/// The ref a revision string names (`main`, `main~3`), or None for commit
/// ids and anything that is not a valid ref name.
fn rev_ref(rev: &str) -> Option<&str> {
    let base = rev.rsplit_once('~').map_or(rev, |(base, _)| base);
    (base.parse::<ObjectId>().is_err() && ant_store::validate_ref_name(base).is_ok())
        .then_some(base)
}

/// How a path's history at a `rev` naming a ref splits between commits the
/// ref's index does not cover yet and the index (see [`lagging_path_log`]).
struct IndexPlan {
    /// The indexed ref.
    base: String,
    /// Unindexed commits above the watermark that touch the path (or lie
    /// under it), newest first, at most the requested limit.
    lag: Vec<(u64, ObjectId)>,
    /// The rest comes from `base`'s index at generations up to this.
    max_generation: u64,
}

/// The [`IndexPlan`] for `rev` (resolved to `tip`), decided by one read of
/// the ref's watermark; `None` when the index cannot serve it (see
/// [`lagging_path_log`]), and then a catch-up of the ref is scheduled.
async fn index_plan(
    b: &Backend,
    rev: &str,
    tip: ObjectId,
    path: &RepoPath,
    limit: usize,
) -> OpResult<Option<IndexPlan>> {
    let Some(base) = rev_ref(rev) else { return Ok(None) };
    let plan = index_plan_for(b, base, tip, path, limit).await?;
    if plan.is_none() {
        // The ref's index is missing (a ref set outside this server, e.g.
        // by an import), trails too far or was moved non-fast-forward:
        // bring it up to date in the background so the next queries use
        // it instead of scanning the history again.
        schedule_catch_up(b, base, None);
    }
    Ok(plan)
}

async fn index_plan_for(
    b: &Backend,
    base: &str,
    tip: ObjectId,
    path: &RepoPath,
    limit: usize,
) -> OpResult<Option<IndexPlan>> {
    let Some(wm) = b.meta.history_watermark(base).await? else { return Ok(None) };
    let [Some(tip_node), Some(wm_node)] =
        <[_; 2]>::try_from(cache::get_nodes(b, &[tip, wm]).await?).unwrap_or([None, None])
    else {
        return Ok(None);
    };
    let base = base.to_string();
    if tip_node.generation <= wm_node.generation {
        // Without this, a watermark that caught up between resolving `tip`
        // and reading it sent the query to a full changeset scan.
        if !is_first_parent_ancestor(b, tip, wm).await? {
            return Ok(None);
        }
        return Ok(Some(IndexPlan { base, lag: Vec::new(), max_generation: tip_node.generation }));
    }
    // Generations strictly decrease along the chain: it is at most the gap long.
    let gap = usize::try_from(tip_node.generation - wm_node.generation).unwrap_or(usize::MAX);
    let chain = b.meta.first_parent_chain(tip, Some(wm), MAX_LAG.min(gap)).await?;
    if chain.last().and_then(|n| n.parents.first()) != Some(&wm) {
        return Ok(None);
    }
    let ids: Vec<ObjectId> = chain.iter().map(|n| n.id).collect();
    let sets = cache::get_changesets(b, &ids).await?;
    let mut lag = Vec::new();
    for (node, set) in chain.iter().zip(sets) {
        if lag.len() >= limit {
            break;
        }
        let set = set.ok_or_else(|| OpError::NotFound(format!("changeset of {}", node.id)))?;
        if set.iter().any(|c| c.path.starts_with(path)) {
            lag.push((node.generation, node.id));
        }
    }
    // Bounded by the watermark's generation: rows a concurrent catch-up adds
    // for the commits just scanned are not returned twice.
    Ok(Some(IndexPlan { base, lag, max_generation: wm_node.generation }))
}

/// `log --path` for a `rev` naming a ref (`tip` = the commit it resolved
/// to), decided by one read of that ref's watermark:
/// - `tip` at or behind the watermark on its first-parent chain (caught up,
///   `main~N`, or a catch-up that finished after `tip` was resolved): served
///   from the index alone.
/// - `tip` ahead of it, the usual case on a busy ref where the watermark
///   trails by the commits landed since the last catch-up: when the
///   watermark is at most [`MAX_LAG`] first parents back, the unindexed
///   commits are scanned (one chain read, changesets through the cache) and
///   the rest comes from the index.
///
/// `None` when that does not apply (commit-id rev, no index, ref moved
/// non-fast-forward, lag too long); the caller then scans changesets, and
/// for a ref a background catch-up ([`schedule_catch_up`]) is started so
/// that later queries are served from the index.
pub async fn lagging_path_log(
    b: &Backend,
    rev: &str,
    tip: ObjectId,
    path: &RepoPath,
    limit: usize,
) -> OpResult<Option<Vec<LogEntry>>> {
    let limit = if limit == 0 { usize::MAX } else { limit };
    let Some(plan) = index_plan(b, rev, tip, path, limit).await? else { return Ok(None) };
    let mut hits = plan.lag;
    if hits.len() < limit {
        let rest = limit - hits.len();
        hits.extend(b.meta.query_history(&plan.base, path, plan.max_generation, rest).await?);
    }
    log_entries(b, hits).await.map(Some)
}

// ---------------------------------------------------------------------------
// Blame
// ---------------------------------------------------------------------------

/// Where the (newest-first) list of commits touching one file comes from.
enum FileHistory {
    /// A ref's index: `queue` first (unindexed commits above the watermark),
    /// then index rows at generations `<= below`, read in two pages (a small
    /// one for the common case of a recently touched file, then the rest).
    Indexed {
        queue: VecDeque<ObjectId>,
        base: String,
        below: u64,
        page: IndexPage,
    },
    Scan {
        next: Option<ObjectId>,
        buf: VecDeque<ObjectId>,
    },
}

enum IndexPage {
    First,
    /// The rest: rows at generations `<= below`, minus the first page's.
    Rest(HashSet<ObjectId>),
    Done,
}

impl FileHistory {
    /// The history of `path` at `rev` (= commit `id`): from an index when one
    /// covers it (see [`index_plan`], [`indexed_ref_for`]), else a scan of
    /// first-parent changesets.
    async fn open(b: &Backend, rev: &str, id: ObjectId, path: &RepoPath) -> OpResult<Self> {
        if let Some(plan) = index_plan(b, rev, id, path, usize::MAX).await? {
            return Ok(FileHistory::Indexed {
                queue: plan.lag.into_iter().map(|(_, c)| c).collect(),
                base: plan.base,
                below: plan.max_generation,
                page: IndexPage::First,
            });
        }
        Ok(match indexed_ref_for(b, rev, id).await? {
            Some(base) => {
                let g = b
                    .meta
                    .get_commits(&[id])
                    .await?
                    .pop()
                    .flatten()
                    .map_or(u64::MAX, |n| n.generation);
                FileHistory::Indexed {
                    queue: VecDeque::new(),
                    base,
                    below: g,
                    page: IndexPage::First,
                }
            }
            None => FileHistory::Scan { next: Some(id), buf: VecDeque::new() },
        })
    }

    /// Up to `n` further `(commit, change)` pairs for exactly `path`; empty
    /// means the history is exhausted.
    async fn next(
        &mut self,
        b: &Backend,
        path: &RepoPath,
        n: usize,
    ) -> OpResult<Vec<(ObjectId, PathChange)>> {
        let mut out = Vec::new();
        while out.is_empty() {
            let ids: Vec<ObjectId> = match self {
                FileHistory::Indexed { queue, base, below, page } => {
                    if queue.is_empty() {
                        match std::mem::replace(page, IndexPage::Done) {
                            IndexPage::Done => break,
                            IndexPage::First => {
                                let hits = b
                                    .meta
                                    .query_history(base, path, *below, INDEX_FIRST_PAGE)
                                    .await?;
                                if hits.len() == INDEX_FIRST_PAGE {
                                    *below = hits.last().map_or(0, |(g, _)| *g);
                                    *page = IndexPage::Rest(hits.iter().map(|(_, c)| *c).collect());
                                }
                                queue.extend(hits.into_iter().map(|(_, c)| c));
                            }
                            IndexPage::Rest(seen) => {
                                let hits =
                                    b.meta.query_history(base, path, *below, usize::MAX).await?;
                                queue.extend(
                                    hits.into_iter().map(|(_, c)| c).filter(|c| !seen.contains(c)),
                                );
                            }
                        }
                        continue;
                    }
                    let take = queue.len().min(n);
                    queue.drain(..take).collect()
                }
                FileHistory::Scan { next, buf } => {
                    if buf.is_empty() {
                        let Some(from) = *next else { break };
                        let chunk = b.meta.first_parent_chain(from, None, INDEX_BATCH).await?;
                        *next = chunk.last().and_then(|c| c.parents.first().copied());
                        buf.extend(chunk.iter().map(|c| c.id));
                        if buf.is_empty() {
                            break;
                        }
                    }
                    let take = buf.len().min(INDEX_BATCH);
                    buf.drain(..take).collect()
                }
            };
            for (id, set) in ids.iter().zip(b.meta.get_changesets(&ids).await?) {
                if let Some(c) = set.into_iter().flatten().find(|c| c.path == *path) {
                    out.push((*id, c));
                }
            }
        }
        Ok(out)
    }
}

fn encode_blame(owners: &[ObjectId]) -> Bytes {
    let mut table: Vec<ObjectId> = Vec::new();
    let mut index: HashMap<ObjectId, u32> = HashMap::new();
    let per_line: Vec<u32> = owners
        .iter()
        .map(|id| {
            *index.entry(*id).or_insert_with(|| {
                table.push(*id);
                (table.len() - 1) as u32
            })
        })
        .collect();
    let mut out = Vec::with_capacity(8 + table.len() * 32 + per_line.len() * 4);
    out.extend((table.len() as u32).to_le_bytes());
    for id in &table {
        out.extend(id.as_bytes());
    }
    out.extend((per_line.len() as u32).to_le_bytes());
    for i in per_line {
        out.extend(i.to_le_bytes());
    }
    out.into()
}

/// The owners in an [`encode_blame`] value; `None` if malformed.
fn decode_blame(v: &[u8]) -> Option<Vec<ObjectId>> {
    let u32_at = |o: usize| Some(u32::from_le_bytes(v.get(o..o + 4)?.try_into().ok()?) as usize);
    let n = u32_at(0)?;
    let table: Vec<ObjectId> = (0..n)
        .map(|i| ObjectId::from_bytes(v.get(4 + i * 32..36 + i * 32)?).ok())
        .collect::<Option<_>>()?;
    let base = 4 + n * 32;
    let lines = u32_at(base)?;
    if v.len() != base + 4 + lines.checked_mul(4)? {
        return None;
    }
    (0..lines).map(|i| table.get(u32_at(base + 4 + i * 4)?).copied()).collect()
}

/// Blame as recursively derived data. The value at key `(commit, path)`,
/// `commit` changing `path` on its first-parent chain, is the owning commit
/// of each line of `path` at `commit`; the predecessor is the previous
/// commit changing `path` down that chain, and the change is the line map
/// from the previous version to this one (`diff_texts`). Every revision
/// whose last change to `path` is `commit` shares this one value.
pub(crate) struct Blame;

impl Derivation for Blame {
    const KIND: &'static str = BLAME_KIND;
    const POLICY: Policy = Policy::Deferred;
    type Key = (ObjectId, RepoPath);
    type Value = Vec<ObjectId>;
    /// For each line of the new version, its line in the previous one.
    type Change = Vec<Option<usize>>;

    fn key((commit, path): &Self::Key) -> Vec<u8> {
        let mut key = commit.as_bytes().to_vec();
        key.extend(path.as_str().as_bytes());
        key
    }

    fn encode(value: &Self::Value) -> Bytes {
        encode_blame(value)
    }

    fn decode(_: &Self::Key, bytes: &[u8]) -> Option<Self::Value> {
        decode_blame(bytes)
    }

    /// Introduced lines (and, with no predecessor, lines the history does not
    /// explain because it was pruned) belong to `commit`; the others keep
    /// their owner in the previous version.
    fn derive(
        (commit, _): &Self::Key,
        map: &Self::Change,
        parent: Option<&Self::Value>,
    ) -> Self::Value {
        map.iter()
            .map(|m| m.and_then(|i| parent.and_then(|p| p.get(i)).copied()).unwrap_or(*commit))
            .collect()
    }
}

/// Reference implementation of [`line_map_ids`] (quadratic LCS table over the
/// strings themselves); the oracle the optimised version is tested against.
#[cfg(test)]
fn line_map(old: &[&str], new: &[&str]) -> Vec<Option<usize>> {
    let mut map = vec![None; new.len()];
    let pre = old.iter().zip(new).take_while(|(a, b)| a == b).count();
    let suf =
        old[pre..].iter().rev().zip(new[pre..].iter().rev()).take_while(|(a, b)| a == b).count();
    for (i, m) in map.iter_mut().enumerate().take(pre) {
        *m = Some(i);
    }
    for k in 0..suf {
        map[new.len() - 1 - k] = Some(old.len() - 1 - k);
    }
    let (o, n) = (&old[pre..old.len() - suf], &new[pre..new.len() - suf]);
    if o.is_empty() || n.is_empty() || (o.len() + 1).saturating_mul(n.len() + 1) > MAX_LCS_CELLS {
        return map;
    }
    let w = n.len() + 1;
    let mut t = vec![0u32; (o.len() + 1) * w];
    for i in (0..o.len()).rev() {
        for j in (0..n.len()).rev() {
            t[i * w + j] = if o[i] == n[j] {
                t[(i + 1) * w + j + 1] + 1
            } else {
                t[(i + 1) * w + j].max(t[i * w + j + 1])
            };
        }
    }
    let (mut i, mut j) = (0, 0);
    while i < o.len() && j < n.len() {
        if o[i] == n[j] {
            map[pre + j] = Some(pre + i);
            i += 1;
            j += 1;
        } else if t[(i + 1) * w + j] >= t[i * w + j + 1] {
            i += 1;
        } else {
            j += 1;
        }
    }
    map
}

/// Row-wise bit-parallel LCS table (Hyyro): `rows[r]` encodes, for the first
/// `r` symbols of `x`, the increments of the LCS length along `y`'s prefixes,
/// so any `L[r][c]` is a popcount away.
struct LcsRows {
    words: usize,
    bits: Vec<u64>,
    /// Zero bits in all words before word `w` of row `r`.
    zeros_before: Vec<u32>,
}

impl LcsRows {
    fn new(x: &[u32], y: &[u32]) -> Self {
        let words = y.len().div_ceil(64);
        // Match masks, only for symbols that occur in `x` (ids are dense:
        // they come from one interner).
        let slots = x.iter().chain(y).max().map_or(0, |&m| m as usize + 1);
        let mut mask_of = vec![u32::MAX; slots];
        let mut distinct = 0usize;
        for &a in x {
            if mask_of[a as usize] == u32::MAX {
                mask_of[a as usize] = distinct as u32;
                distinct += 1;
            }
        }
        let mut masks = vec![0u64; distinct * words];
        for (k, &b) in y.iter().enumerate() {
            let m = mask_of[b as usize];
            if m != u32::MAX {
                masks[m as usize * words + k / 64] |= 1 << (k % 64);
            }
        }
        let mut bits = vec![0u64; (x.len() + 1) * words];
        let mut zeros_before = vec![0u32; (x.len() + 1) * words];
        bits[..words].fill(!0);
        for (r, a) in x.iter().enumerate() {
            let mask = &masks[mask_of[*a as usize] as usize * words..][..words];
            let (prev, next) = bits[r * words..(r + 2) * words].split_at_mut(words);
            let mut carry = false;
            for ((n, &v), &m) in next.iter_mut().zip(&*prev).zip(mask) {
                let u = v & m;
                let (s1, c1) = v.overflowing_add(u);
                let (s2, c2) = s1.overflowing_add(carry as u64);
                carry = c1 | c2;
                *n = s2 | (v & !m);
            }
        }
        for r in 0..=x.len() {
            let mut z = 0u32;
            for w in 0..words {
                zeros_before[r * words + w] = z;
                z += (!bits[r * words + w]).count_ones();
            }
        }
        LcsRows { words, bits, zeros_before }
    }

    /// LCS length of the first `r` symbols of `x` and first `c` of `y`.
    fn len(&self, r: usize, c: usize) -> u32 {
        let (w, rem) = (c / 64, c % 64);
        if rem == 0 {
            return if w == self.words {
                self.zeros_before[r * self.words + w - 1]
                    + (!self.bits[r * self.words + w - 1]).count_ones()
            } else {
                self.zeros_before[r * self.words + w]
            };
        }
        let word = !self.bits[r * self.words + w] & ((1u64 << rem) - 1);
        self.zeros_before[r * self.words + w] + word.count_ones()
    }
}

/// For each line of `new`, the index of the matching line of `old`, or None
/// if the line was introduced. Common prefix/suffix are trimmed, then an LCS
/// runs on the middle (counted as fully replaced if too large). Same result
/// as the quadratic reference `line_map`, including its tie-breaking.
pub(crate) fn line_map_ids(old: &[u32], new: &[u32]) -> Vec<Option<usize>> {
    let mut map = vec![None; new.len()];
    let pre = old.iter().zip(new).take_while(|(a, b)| a == b).count();
    let suf =
        old[pre..].iter().rev().zip(new[pre..].iter().rev()).take_while(|(a, b)| a == b).count();
    for (i, m) in map.iter_mut().enumerate().take(pre) {
        *m = Some(i);
    }
    for k in 0..suf {
        map[new.len() - 1 - k] = Some(old.len() - 1 - k);
    }
    let (o, n) = (&old[pre..old.len() - suf], &new[pre..new.len() - suf]);
    if o.is_empty() || n.is_empty() || (o.len() + 1).saturating_mul(n.len() + 1) > MAX_LCS_CELLS {
        return map;
    }
    // t[i][j] = LCS(o[i..], n[j..]) = LCS of the reversed prefixes of lengths
    // (|o|-i, |n|-j); the table is symmetric, so rows go to the shorter side.
    let ro: Vec<u32> = o.iter().rev().copied().collect();
    let rn: Vec<u32> = n.iter().rev().copied().collect();
    let swapped = rn.len() < ro.len();
    let rows = if swapped { LcsRows::new(&rn, &ro) } else { LcsRows::new(&ro, &rn) };
    let t = |i: usize, j: usize| {
        let (a, b) = (o.len() - i, n.len() - j);
        if swapped { rows.len(b, a) } else { rows.len(a, b) }
    };
    let (mut i, mut j) = (0, 0);
    while i < o.len() && j < n.len() {
        if o[i] == n[j] {
            map[pre + j] = Some(pre + i);
            i += 1;
            j += 1;
        } else if t(i + 1, j) >= t(i, j + 1) {
            i += 1;
        } else {
            j += 1;
        }
    }
    map
}

/// Fast multiplicative hasher for line interning, keyed per process so that
/// colliding lines cannot be crafted in advance.
#[derive(Clone)]
struct SeededFx(u64);

struct FxState(u64);

impl std::hash::BuildHasher for SeededFx {
    type Hasher = FxState;
    fn build_hasher(&self) -> FxState {
        FxState(self.0)
    }
}

impl std::hash::Hasher for FxState {
    fn write(&mut self, bytes: &[u8]) {
        const K: u64 = 0x517c_c1b7_2722_0a95;
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            let w = u64::from_le_bytes(c.try_into().unwrap_or([0; 8]));
            self.0 = (self.0.rotate_left(5) ^ w).wrapping_mul(K);
        }
        let rest = chunks.remainder();
        if !rest.is_empty() {
            let mut b = [0u8; 8];
            b[..rest.len()].copy_from_slice(rest);
            self.0 = (self.0.rotate_left(5) ^ u64::from_le_bytes(b)).wrapping_mul(K);
        }
    }
    fn finish(&self) -> u64 {
        self.0 ^ (self.0 >> 29)
    }
}

static LINE_HASH_SEED: LazyLock<u64> = LazyLock::new(|| {
    use std::hash::BuildHasher;
    std::collections::hash_map::RandomState::new().hash_one(0x5eed_u64)
});

/// Number of lines `str::lines` yields for `b` (valid UTF-8 or not: only
/// newlines matter).
fn line_count(b: &[u8]) -> usize {
    b.iter().filter(|&&c| c == b'\n').count() + usize::from(b.last().is_some_and(|&c| c != b'\n'))
}

/// Interns the lines of two texts into one id space.
fn intern_pair(old: &str, new: &str) -> (Vec<u32>, Vec<u32>) {
    let mut ids: HashMap<&str, u32, SeededFx> = HashMap::with_capacity_and_hasher(
        line_count(old.as_bytes()) + line_count(new.as_bytes()) / 4 + 8,
        SeededFx(*LINE_HASH_SEED),
    );
    let mut intern = |text| -> Vec<u32> {
        let text: &str = text;
        text.lines()
            .map(|l| {
                let next = ids.len() as u32;
                *ids.entry(l).or_insert(next)
            })
            .collect()
    };
    let o = intern(old);
    (o, intern(new))
}

/// [`line_map_ids`] over two texts split like `str::lines`. Only the part
/// between the common prefix and suffix (found bytewise, snapped to line
/// boundaries) is split and interned, so a small edit costs a memcmp over
/// the file plus the diff of the edited region.
fn diff_texts(old: &str, new: &str) -> Vec<Option<usize>> {
    let (ob, nb) = (old.as_bytes(), new.as_bytes());
    // Common prefix, snapped back to just after its last newline.
    let p = ob.iter().zip(nb).take_while(|(a, b)| a == b).count();
    let p = ob[..p].iter().rposition(|&c| c == b'\n').map_or(0, |i| i + 1);
    let mut pre = line_count(&ob[..p]);
    // Extend to the maximal common prefix of *lines* (CRLF and LF endings
    // compare equal; a repeated line may continue it past the first byte
    // difference), as the reference does before looking at the suffix.
    let (mut po, mut pn) = (p, p);
    for (a, b) in old[p..].split_inclusive('\n').zip(new[p..].split_inclusive('\n')) {
        if a.lines().next() != b.lines().next() {
            break;
        }
        (po, pn, pre) = (po + a.len(), pn + b.len(), pre + 1);
    }
    let (ob, nb) = (&ob[po..], &nb[pn..]);
    // Common suffix: the largest one that starts at a line start in both.
    let s = ob.iter().rev().zip(nb.iter().rev()).take_while(|(a, b)| a == b).count();
    let aligned = |k: usize| {
        (k == ob.len() || ob[ob.len() - k - 1] == b'\n')
            && (k == nb.len() || nb[nb.len() - k - 1] == b'\n')
    };
    let s = if aligned(s) {
        s
    } else {
        // The `s` common bytes: a line start inside them is aligned in both.
        ob[ob.len() - s..].iter().position(|&c| c == b'\n').map_or(0, |i| s - i - 1)
    };
    let suf = line_count(&ob[ob.len() - s..]);
    // All cut points follow a newline (or are text ends): char boundaries.
    let (o_mid, n_mid) = (&old[po..old.len() - s], &new[pn..new.len() - s]);

    let (oi, ni) = intern_pair(o_mid, n_mid);
    let (old_lines, new_lines) = (pre + oi.len() + suf, pre + ni.len() + suf);
    let mid = line_map_ids(&oi, &ni);
    let mut map = Vec::with_capacity(new_lines);
    map.extend((0..pre).map(Some));
    map.extend(mid.into_iter().map(|m| m.map(|k| pre + k)));
    map.extend((old_lines - suf..old_lines).map(Some));
    debug_assert_eq!(map.len(), new_lines);
    map
}

fn utf8(bytes: &[u8], path: &RepoPath) -> OpResult<String> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| OpError::InvalidArgument(format!("{path} is not valid UTF-8")))
}

/// Stored blame values, by the commit whose version of the file they cover.
type Stored = HashMap<ObjectId, Vec<ObjectId>>;

/// One batch of a file's history with the blobs of its previous versions and
/// the stored blame values of its versions.
struct BlameChunk {
    step: Vec<(ObjectId, PathChange)>,
    blobs: HashMap<ObjectId, ant_core::EncodedObject>,
    stored: Stored,
}

/// The stored [`Blame`] values of the versions in `step`: one round trip.
async fn probe(b: &Backend, path: &RepoPath, step: &[(ObjectId, PathChange)]) -> OpResult<Stored> {
    let keys: Vec<(ObjectId, RepoPath)> = step
        .iter()
        .filter(|(_, c)| c.kind != ChangeKind::Deleted)
        .map(|(commit, _)| (*commit, path.clone()))
        .collect();
    let found = derive::get_many::<Blame>(b.meta.as_ref(), &keys).await?;
    Ok(keys.into_iter().zip(found).filter_map(|((commit, _), v)| Some((commit, v?))).collect())
}

/// Reads the blobs of `step` and, unless already `stored`, probes its stored
/// values (concurrently).
async fn fetch_chunk(
    b: Backend,
    path: RepoPath,
    step: Vec<(ObjectId, PathChange)>,
    stored: Option<Stored>,
) -> OpResult<BlameChunk> {
    let mut ids: Vec<ObjectId> = step.iter().filter_map(|(_, c)| c.old.map(|(_, id)| id)).collect();
    ids.sort_unstable();
    ids.dedup();
    let blobs = async { Ok::<_, OpError>(b.objects.get_many(&ids).await?) };
    let (fetched, stored) = match stored {
        Some(stored) => (blobs.await?, stored),
        None => tokio::try_join!(blobs, probe(&b, &path, &step))?,
    };
    let mut blobs = HashMap::with_capacity(ids.len());
    for (id, o) in ids.into_iter().zip(fetched) {
        blobs.insert(id, o.ok_or_else(|| OpError::not_found_object(id))?);
    }
    Ok(BlameChunk { step, blobs, stored })
}

/// Reads a file's history ahead of the walk: a producer task pulls change
/// batches (growing from `BLAME_FIRST_CHUNK` to `BLAME_CHUNK`) and starts
/// their blob fetches and stored-value probes, keeping up to
/// `BLAME_PREFETCH` batches in flight so store latency overlaps the diffing.
/// Dropping it stops the producer; fetches already started finish
/// unobserved.
struct Prefetch {
    rx: tokio::sync::mpsc::Receiver<tokio::task::JoinHandle<OpResult<BlameChunk>>>,
    producer: tokio::task::JoinHandle<()>,
}

impl Prefetch {
    /// `first` is the history's first batch, already read and probed.
    fn start(
        b: Backend,
        path: RepoPath,
        mut history: FileHistory,
        first: Vec<(ObjectId, PathChange)>,
        first_stored: Stored,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(BLAME_PREFETCH);
        let producer = tokio::spawn(async move {
            if first.is_empty() {
                return;
            }
            let task =
                tokio::spawn(fetch_chunk(b.clone(), path.clone(), first, Some(first_stored)));
            if tx.send(task).await.is_err() {
                return;
            }
            let mut n = (BLAME_FIRST_CHUNK * 2).min(BLAME_CHUNK);
            loop {
                match history.next(&b, &path, n).await {
                    Ok(step) if step.is_empty() => break,
                    Ok(step) => {
                        let task = tokio::spawn(fetch_chunk(b.clone(), path.clone(), step, None));
                        if tx.send(task).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(tokio::spawn(async move { Err(e) })).await;
                        break;
                    }
                }
                n = (n * 2).min(BLAME_CHUNK);
            }
        });
        Prefetch { rx, producer }
    }

    /// The next batch, or None when the history is exhausted.
    async fn next(&mut self) -> OpResult<Option<BlameChunk>> {
        match self.rx.recv().await {
            None => Ok(None),
            Some(handle) => handle.await.map_err(StoreError::backend)?.map(Some),
        }
    }
}

impl Drop for Prefetch {
    fn drop(&mut self) {
        self.producer.abort();
    }
}

fn attribute(owners: Vec<ObjectId>, lines: Vec<&str>) -> Vec<BlameLine> {
    owners
        .into_iter()
        .zip(lines)
        .map(|(commit, l)| BlameLine { commit, line: l.to_string() })
        .collect()
}

/// [`blame`] on behalf of `access`: an unreadable path is reported exactly
/// like a missing one (`NotFound`).
pub async fn blame_as(
    b: &Backend,
    access: Option<&Access>,
    rev: &str,
    path: &RepoPath,
) -> OpResult<Vec<BlameLine>> {
    if let Some(access) = access
        && !access.can_read(path)
    {
        query::resolve_rev(b, rev).await?;
        return Err(OpError::NotFound(format!("{path} at {rev}")));
    }
    blame(b, rev, path).await
}

/// The body of an object as its own `Bytes` handle (no copy).
fn body_bytes(o: &ant_core::EncodedObject) -> Bytes {
    let all = o.as_bytes();
    all.slice(all.len() - o.body().len()..)
}

/// [`diff_texts`] for each `(older, newer)` pair, spread over blocking
/// threads; `None` where either side is not UTF-8.
async fn diff_pairs(pairs: Vec<(Bytes, Bytes)>) -> OpResult<Vec<Option<Vec<Option<usize>>>>> {
    const MIN_PER_TASK: usize = 4;
    let diff = |(old, new): &(Bytes, Bytes)| {
        let (old, new) = (std::str::from_utf8(old).ok()?, std::str::from_utf8(new).ok()?);
        Some(diff_texts(old, new))
    };
    let workers = std::thread::available_parallelism().map_or(1, |n| n.get()).min(8);
    let per_task = pairs.len().div_ceil(workers).max(MIN_PER_TASK);
    if pairs.len() <= per_task {
        return Ok(pairs.iter().map(diff).collect());
    }
    let mut pairs = pairs;
    let mut tasks = Vec::new();
    while !pairs.is_empty() {
        let rest = pairs.split_off(per_task.min(pairs.len()));
        let mine = std::mem::replace(&mut pairs, rest);
        tasks.push(tokio::task::spawn_blocking(move || mine.iter().map(diff).collect::<Vec<_>>()));
    }
    let mut out = Vec::new();
    for t in tasks {
        out.extend(t.await.map_err(StoreError::backend)?);
    }
    Ok(out)
}

/// The blame walk: the commit owning each line of `head` (blob `head_blob`),
/// the file at commit `id`, given its history (`first`: the first batch,
/// already read, and its stored values). Walks newest to oldest, diffing
/// each change; a version with a stored value ends the walk (that value is
/// the fold of the rest of the history).
async fn blame_walk(
    b: &Backend,
    id: ObjectId,
    path: &RepoPath,
    history: FileHistory,
    (first, first_stored): (Vec<(ObjectId, PathChange)>, Stored),
    head: Bytes,
    head_blob: ObjectId,
) -> OpResult<Vec<ObjectId>> {
    let lines = std::str::from_utf8(&head).map_or(0, |t| t.lines().count());
    let mut chunks = Prefetch::start(b.clone(), path.clone(), history, first, first_stored);

    // `cur[k]` = line index, in the version being walked, of final line k.
    let mut cur: Vec<Option<usize>> = (0..lines).map(Some).collect();
    let mut owners: Vec<Option<ObjectId>> = vec![None; lines];
    let mut remaining = lines;
    // The version being walked: the file at `id`, then each older blob
    // (empty before the file's creation; `walked_blob` None).
    let mut walked = head;
    let mut walked_blob = Some(head_blob);
    let mut oldest = id;
    'walk: while remaining > 0 {
        let Some(BlameChunk { step, blobs, stored }) = chunks.next().await? else { break };
        // The (older, newer) text pairs of this batch, up to a version with a
        // stored value. Their diffs do not depend on the attribution so far:
        // compute them in parallel, then apply them in order (stopping where
        // the sequential walk stops).
        let mut steps = Vec::with_capacity(step.len());
        let mut pairs = Vec::with_capacity(step.len());
        let mut below = None;
        for (commit, change) in step {
            if change.kind == ChangeKind::Deleted {
                continue;
            }
            // The stored value covers exactly the version being walked.
            if let Some(v) = stored.get(&commit)
                && change.new.map(|(_, blob)| blob) == walked_blob
                && v.len() == line_count(&walked)
            {
                below = Some(v);
                break;
            }
            let older = change.old.map_or_else(Bytes::new, |(_, bid)| body_bytes(&blobs[&bid]));
            walked_blob = change.old.map(|(_, bid)| bid);
            pairs.push((older.clone(), std::mem::replace(&mut walked, older)));
            steps.push((commit, change.old.is_none()));
        }
        let maps = diff_pairs(pairs).await?;
        for ((commit, created), map) in steps.into_iter().zip(maps) {
            let map =
                map.ok_or_else(|| OpError::InvalidArgument(format!("{path} is not valid UTF-8")))?;
            oldest = commit;
            for (slot, owner) in cur.iter_mut().zip(owners.iter_mut()) {
                let Some(j) = *slot else { continue };
                match map.get(j).copied().flatten() {
                    Some(o) => *slot = Some(o),
                    None => {
                        *owner = Some(commit);
                        *slot = None;
                        remaining -= 1;
                    }
                }
            }
            if created || remaining == 0 {
                break 'walk;
            }
        }
        if let Some(v) = below {
            for (slot, owner) in cur.iter().zip(owners.iter_mut()) {
                if let Some(j) = *slot {
                    *owner = v.get(j).copied();
                }
            }
            break;
        }
    }
    // History ran out (e.g. pruned): attribute leftovers to the oldest commit seen.
    let owners: Vec<ObjectId> = owners.into_iter().map(|o| o.unwrap_or(oldest)).collect();
    Ok(owners)
}

/// Per-line attribution of `path` at `rev`; see the module docs.
pub async fn blame(b: &Backend, rev: &str, path: &RepoPath) -> OpResult<Vec<BlameLine>> {
    let id = query::resolve_rev(b, rev).await?;
    let info = query::read_path(b, rev, path, true)
        .await?
        .ok_or_else(|| OpError::NotFound(format!("{path} at {rev}")))?;
    let Some(content) = info.content else {
        return Err(OpError::InvalidArgument(format!("{path} is not a file")));
    };
    let text = utf8(&content, path)?;
    let lines: Vec<&str> = text.lines().collect();

    let mut history = FileHistory::open(b, rev, id, path).await?;
    let first = history.next(b, path, BLAME_FIRST_CHUNK).await?;
    let stored = probe(b, path, &first).await?;
    // The last commit that changed the file (to exactly this content): the
    // key of the value, shared by every revision since.
    let last = first
        .first()
        .filter(|(_, c)| {
            c.kind != ChangeKind::Deleted && c.new.map(|(_, blob)| blob) == Some(info.id)
        })
        .map(|(commit, _)| *commit);
    if let Some(v) = last.and_then(|l| stored.get(&l))
        && v.len() == lines.len()
    {
        return Ok(attribute(v.clone(), lines));
    }
    let owners = blame_walk(b, id, path, history, (first, stored), content, info.id).await?;
    if let Some(l) = last {
        derive::put::<Blame>(b.meta.as_ref(), &(l, path.clone()), &owners).await?;
    }
    Ok(attribute(owners, lines))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::land::{LandOutcome, land};
    use crate::ops::query;
    use crate::ops::test_support::{commit, delete, read, write};
    use ant_store::HistoryRow;

    async fn landed(
        b: &Backend,
        base: ObjectId,
        ops: Vec<crate::ops::changeset::FileOp>,
    ) -> ObjectId {
        let c = commit(b, Some(base), ops).await.unwrap();
        let LandOutcome::Landed { new_tip, .. } = land(b, "main", c).await.unwrap() else {
            panic!()
        };
        new_tip
    }

    #[tokio::test]
    async fn index_matches_changeset_scan() {
        let b = Backend::in_memory();
        let c0 =
            commit(&b, None, vec![write("src/a.rs", "1"), write("README", "r")]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let mut tip = c0;
        for i in 0..30 {
            let p = if i % 3 == 0 { "src/a.rs".to_string() } else { format!("docs/{i}.md") };
            tip = landed(&b, tip, vec![write(&p, &i.to_string())]).await;
        }
        assert!(catch_up(&b, "main").await.unwrap() > 0);
        assert_eq!(catch_up(&b, "main").await.unwrap(), 0, "second catch-up is a no-op");
        for path in ["src", "src/a.rs", "docs", "README", "nope"] {
            let p = RepoPath::new(path).unwrap();
            let gen_ = b.meta.get_commits(&[tip]).await.unwrap()[0].clone().unwrap().generation;
            let fast: Vec<_> = path_log(&b, "main", gen_, &p, 0)
                .await
                .unwrap()
                .into_iter()
                .map(|e| e.id)
                .collect();
            let slow: Vec<_> = query::log(&b, "main", Some(&p), 0)
                .await
                .unwrap()
                .into_iter()
                .map(|e| e.id)
                .collect();
            assert_eq!(fast, slow, "{path}");
        }
    }

    #[tokio::test]
    async fn non_fast_forward_rebuilds() {
        let b = Backend::in_memory();
        let c0 = commit(&b, None, vec![write("f", "0")]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let c1 = landed(&b, c0, vec![write("f", "1")]).await;
        catch_up(&b, "main").await.unwrap();
        // Force main back to c0 and onto a different branch.
        let other = commit(&b, Some(c0), vec![write("g", "x")]).await.unwrap();
        b.refs.cas_ref("main", Some(c1), Some(other)).await.unwrap();
        catch_up(&b, "main").await.unwrap();
        let gen_ = b.meta.get_commits(&[other]).await.unwrap()[0].clone().unwrap().generation;
        let f = RepoPath::new("f").unwrap();
        let ids: Vec<_> =
            path_log(&b, "main", gen_, &f, 0).await.unwrap().into_iter().map(|e| e.id).collect();
        assert_eq!(ids, vec![c0], "c1 is no longer on main");
    }

    #[tokio::test]
    async fn blame_attributes_lines() {
        let b = Backend::in_memory();
        let c0 = commit(&b, None, vec![write("f", "a\nb\nc\n")]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let c1 = landed(&b, c0, vec![write("f", "a\nB\nc\nd\n")]).await;
        let c2 = landed(&b, c1, vec![write("g", "other\n")]).await;
        let c3 = landed(&b, c2, vec![write("f", "z\na\nB\nc\nd\n")]).await;
        catch_up(&b, "main").await.unwrap();
        let bl = blame(&b, "main", &RepoPath::new("f").unwrap()).await.unwrap();
        let got: Vec<_> = bl.iter().map(|l| (l.commit, l.line.as_str())).collect();
        assert_eq!(got, vec![(c3, "z"), (c0, "a"), (c1, "B"), (c0, "c"), (c1, "d")]);
        // Cached second call agrees.
        assert_eq!(blame(&b, "main", &RepoPath::new("f").unwrap()).await.unwrap(), bl);
        assert!(matches!(
            blame(&b, "main", &RepoPath::new("nope").unwrap()).await,
            Err(crate::ops::OpError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn incremental_catch_up_and_log_routing() {
        let b = Backend::in_memory();
        let c0 = commit(&b, None, vec![write("a/x", "0")]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let mut tip = c0;
        for i in 0..5 {
            tip = landed(
                &b,
                tip,
                vec![write(if i % 2 == 0 { "a/x" } else { "b/y" }, &i.to_string())],
            )
            .await;
        }
        assert_eq!(catch_up(&b, "main").await.unwrap(), 6);
        for i in 5..8 {
            tip = landed(&b, tip, vec![write("a/x", &i.to_string())]).await;
        }
        // Lagging watermark: log must still be right (falls back to the scan).
        let a = RepoPath::new("a").unwrap();
        let before = query::log(&b, "main", Some(&a), 0).await.unwrap();
        assert_eq!(catch_up(&b, "main").await.unwrap(), 3, "only commits after the watermark");
        assert_eq!(query::log(&b, "main", Some(&a), 0).await.unwrap(), before);
        // Same via a bare commit id and `~N`; limits are honored.
        let by_id = query::log(&b, &tip.to_hex(), Some(&a), 2).await.unwrap();
        assert_eq!(by_id, before[..2]);
        let back = query::log(&b, "main~2", Some(&a), 0).await.unwrap();
        assert_eq!(back, before[2..]);
        // The index really serves it: poison a row the scan would not produce.
        let g = b.meta.get_commits(&[c0]).await.unwrap()[0].clone().unwrap().generation;
        b.meta
            .index_history("main", &[HistoryRow { path: a.clone(), generation: g, commit: c0 }])
            .await
            .unwrap();
        assert_eq!(path_log(&b, "main", u64::MAX, &a, 0).await.unwrap().len(), before.len());
    }

    #[tokio::test]
    async fn first_parent_ancestry() {
        let b = Backend::in_memory();
        let c0 = commit(&b, None, vec![write("f", "0")]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let mut chain = vec![c0];
        for i in 1..6 {
            chain.push(landed(&b, chain[i - 1], vec![write("f", &i.to_string())]).await);
        }
        let side = commit(&b, Some(chain[2]), vec![write("g", "x")]).await.unwrap();
        for (i, a) in chain.iter().enumerate() {
            for (j, d) in chain.iter().enumerate() {
                assert_eq!(is_first_parent_ancestor(&b, *a, *d).await.unwrap(), i <= j, "{i} {j}");
            }
        }
        assert!(is_first_parent_ancestor(&b, chain[2], side).await.unwrap());
        assert!(!is_first_parent_ancestor(&b, chain[3], side).await.unwrap());
        assert!(!is_first_parent_ancestor(&b, side, chain[5]).await.unwrap());
        let unknown = ObjectId([9; 32]);
        assert!(!is_first_parent_ancestor(&b, unknown, chain[5]).await.unwrap());
        assert!(!is_first_parent_ancestor(&b, chain[0], unknown).await.unwrap());
    }

    /// A watermark trailing the tip (normal on a busy ref) still uses the
    /// index for everything before it and scans only the lag.
    #[tokio::test]
    async fn lagging_index_scans_only_the_lag() {
        let b = Backend::in_memory();
        let c0 = commit(&b, None, vec![write("a/x", "init")]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let mut tip = c0;
        for i in 0..6 {
            let path = if i % 2 == 0 { "a/x" } else { "b/y" };
            tip = landed(&b, tip, vec![write(path, &i.to_string())]).await;
        }
        catch_up(&b, "main").await.unwrap();
        let wm = b.meta.history_watermark("main").await.unwrap();
        for i in 6..10 {
            let path = if i % 2 == 0 { "a/x" } else { "b/y" };
            tip = landed(&b, tip, vec![write(path, &i.to_string())]).await;
        }
        let a = RepoPath::new("a").unwrap();
        // The scan's answer (bare commit ids never use the index).
        let scan = query::log(&b, &tip.to_hex(), Some(&a), 0).await.unwrap();
        assert_eq!(scan.len(), 6);
        let lagging = lagging_path_log(&b, "main", tip, &a, 0).await.unwrap().unwrap();
        assert_eq!(lagging, scan);
        assert_eq!(query::log(&b, "main", Some(&a), 0).await.unwrap(), scan);
        for limit in 1..=6 {
            let got = lagging_path_log(&b, "main", tip, &a, limit).await.unwrap().unwrap();
            assert_eq!(got, scan[..limit], "limit {limit}");
        }
        // Not applicable: unknown ref, or no watermark on the chain.
        assert_eq!(lagging_path_log(&b, "other", tip, &a, 0).await.unwrap(), None);
        assert_eq!(lagging_path_log(&b, &tip.to_hex(), tip, &a, 0).await.unwrap(), None);
        // The older part really comes from the index: a poisoned row (c0
        // claimed again under `b`) shows up; the scanned lag does not repeat.
        let wm_gen = b.meta.get_commits(&[wm.unwrap()]).await.unwrap()[0].clone().unwrap();
        let bpath = RepoPath::new("b").unwrap();
        b.meta
            .index_history(
                "main",
                &[HistoryRow { path: bpath.clone(), generation: wm_gen.generation, commit: c0 }],
            )
            .await
            .unwrap();
        let got = lagging_path_log(&b, "main", tip, &bpath, 0).await.unwrap().unwrap();
        assert!(got.iter().any(|e| e.id == c0), "index rows are used below the watermark");
    }

    /// Calls of one store operation so far (`ant_store_op_duration_seconds`).
    fn op_calls(m: &crate::metrics::Metrics, store: &str, op: &str) -> u64 {
        let key = format!("ant_store_op_duration_seconds_count{{store=\"{store}\",op=\"{op}\"}} ");
        m.render()
            .lines()
            .find_map(|l| l.strip_prefix(&key))
            .map_or(0, |n| n.trim().parse().unwrap())
    }

    /// `n` commits on `main` (set directly, as an import does: no index),
    /// every 10th touching `a/x`; returns the tip.
    async fn unindexed_history(b: &Backend, n: usize) -> ObjectId {
        let mut tip = commit(b, None, vec![write("a/x", "init")]).await.unwrap();
        for i in 1..n {
            let path = if i % 10 == 0 { "a/x".to_string() } else { format!("b/{}", i % 7) };
            tip = commit(b, Some(tip), vec![write(&path, &i.to_string())]).await.unwrap();
        }
        b.refs.cas_ref("main", None, Some(tip)).await.unwrap();
        tip
    }

    /// A ref without an index (set outside the server, e.g. imported) is
    /// scanned once; that query starts its catch-up, and later queries are
    /// served from the index.
    #[tokio::test]
    async fn unindexed_ref_is_indexed_after_a_query() {
        let b = Backend::in_memory();
        let tip = unindexed_history(&b, 60).await;
        let a = RepoPath::new("a").unwrap();
        assert_eq!(b.meta.history_watermark("main").await.unwrap(), None);
        let scan = query::log(&b, "main", Some(&a), 0).await.unwrap();
        assert_eq!(scan.len(), 6);
        let deadline = tokio::time::Instant::now() + crate::ops::test_support::DEADLINE;
        while b.meta.history_watermark("main").await.unwrap() != Some(tip) {
            assert!(tokio::time::Instant::now() < deadline, "no catch-up after the query");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(query::log(&b, "main", Some(&a), 0).await.unwrap(), scan);
        // Served by the index: a poisoned row shows up.
        let root = scan[scan.len() - 1].clone();
        let bpath = RepoPath::new("b").unwrap();
        b.meta
            .index_history(
                "main",
                &[HistoryRow { path: bpath.clone(), generation: root.generation, commit: root.id }],
            )
            .await
            .unwrap();
        let got = query::log(&b, "main", Some(&bpath), 0).await.unwrap();
        assert!(got.iter().any(|e| e.id == root.id));
    }

    /// Store calls of `log --path`: from the index, one history query and
    /// no chain or changeset reads however long the history; scanning
    /// (no index covers the rev), a few reads growing in size, not one per
    /// 256 commits.
    #[tokio::test]
    async fn path_log_store_calls_are_bounded() {
        let metrics = crate::metrics::Metrics::new();
        let b = metrics.instrument(&Backend::in_memory());
        let tip = unindexed_history(&b, 3000).await;
        let a = RepoPath::new("a").unwrap();
        let calls = |op: &str| op_calls(&metrics, "meta", op);

        let (chains, sets) = (calls("first_parent_chain"), calls("get_changesets"));
        let scan = query::log(&b, &tip.to_hex(), Some(&a), 0).await.unwrap();
        assert_eq!(scan.len(), 300);
        let (chains, sets) = (calls("first_parent_chain") - chains, calls("get_changesets") - sets);
        assert!(chains <= 5, "{chains} chain reads for 3000 commits");
        assert!(sets <= 5, "{sets} changeset reads for 3000 commits");

        catch_up(&b, "main").await.unwrap();
        for limit in [0, 20] {
            let before: Vec<u64> =
                ["query_history", "first_parent_chain", "get_changesets", "history_watermark"]
                    .iter()
                    .map(|op| calls(op))
                    .collect();
            let objects = op_calls(&metrics, "objects", "get_many");
            let got = query::log(&b, "main", Some(&a), limit).await.unwrap();
            assert_eq!(got.len(), if limit == 0 { 300 } else { 20 });
            assert_eq!(got[..], scan[..got.len()]);
            assert_eq!(calls("query_history") - before[0], 1);
            assert_eq!(calls("first_parent_chain") - before[1], 0);
            assert_eq!(calls("get_changesets") - before[2], 0);
            assert_eq!(calls("history_watermark") - before[3], 1);
            assert!(op_calls(&metrics, "objects", "get_many") - objects <= 1);
        }
    }

    #[tokio::test]
    async fn blame_scan_equals_indexed_and_rejects_binary() {
        let b = Backend::in_memory();
        let c0 =
            commit(&b, None, vec![write("f", "1\n2\n3\n4\n"), write("bin", "x")]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let c1 = landed(&b, c0, vec![write("f", "1\n2\nnew\n3\n4\n")]).await;
        let c2 = landed(&b, c1, vec![write("f", "1\nnew\n3\n")]).await;
        let c3 = landed(&b, c2, vec![delete("f")]).await;
        let c4 = landed(&b, c3, vec![write("f", "again\n")]).await;
        let f = RepoPath::new("f").unwrap();
        let scan = blame(&b, &c2.to_hex(), &f).await.unwrap();
        let got: Vec<_> = scan.iter().map(|l| (l.commit, l.line.as_str())).collect();
        assert_eq!(got, vec![(c0, "1"), (c1, "new"), (c0, "3")]);
        catch_up(&b, "main").await.unwrap();
        // Different rev strings take different cache keys but must agree.
        assert_eq!(blame(&b, "main~2", &f).await.unwrap(), scan);
        let last = blame(&b, "main", &f).await.unwrap();
        assert_eq!((last[0].commit, last[0].line.as_str()), (c4, "again"));
        assert_eq!(last.len(), 1);
        let bin = commit(
            &b,
            Some(c4),
            vec![crate::ops::changeset::FileOp::Write {
                path: RepoPath::new("blob").unwrap(),
                mode: ant_core::EntryMode::File,
                content: crate::ops::changeset::WriteContent::Inline(bytes::Bytes::from_static(&[
                    0xff, 0xfe, 0,
                ])),
            }],
        )
        .await
        .unwrap();
        land(&b, "main", bin).await.unwrap();
        assert!(matches!(
            blame(&b, "main", &RepoPath::new("blob").unwrap()).await,
            Err(OpError::InvalidArgument(_))
        ));
        assert!(matches!(
            blame(&b, "main", &RepoPath::new("src").unwrap()).await,
            Err(OpError::NotFound(_))
        ));
    }

    #[test]
    fn line_map_basics() {
        let m = |o: &[&str], n: &[&str]| line_map(o, n);
        assert_eq!(m(&[], &["a", "b"]), vec![None, None]);
        assert_eq!(m(&["a", "b"], &[]), Vec::<Option<usize>>::new());
        assert_eq!(m(&["a", "b", "c"], &["a", "x", "c"]), vec![Some(0), None, Some(2)]);
        assert_eq!(m(&["a", "b", "c", "d"], &["b", "a", "d"]), vec![Some(1), None, Some(3)]);
        assert_eq!(m(&["a", "a"], &["a", "a", "a"]), vec![Some(0), Some(1), None]);
    }

    /// Line ids for a sequence of small-alphabet symbols.
    fn ids_and_strs(v: &[u8]) -> (Vec<u32>, Vec<String>) {
        (v.iter().map(|&x| u32::from(x)).collect(), v.iter().map(|x| format!("l{x}")).collect())
    }

    fn assert_maps_agree(o: &[u8], n: &[u8]) {
        let ((oi, os), (ni, ns)) = (ids_and_strs(o), ids_and_strs(n));
        let (or, nr): (Vec<&str>, Vec<&str>) =
            (os.iter().map(String::as_str).collect(), ns.iter().map(String::as_str).collect());
        assert_eq!(line_map_ids(&oi, &ni), line_map(&or, &nr), "old={o:?} new={n:?}");
    }

    #[test]
    fn line_map_ids_matches_reference_on_edge_shapes() {
        assert_maps_agree(&[], &[]);
        assert_maps_agree(&[1], &[]);
        assert_maps_agree(&[], &[1]);
        assert_maps_agree(&[1, 2, 3], &[3, 2, 1]);
        assert_maps_agree(&[1, 1, 1, 1], &[1, 1]);
        // Rows on either side, widths around the 64-bit word boundary.
        for w in [1usize, 63, 64, 65, 127, 128, 129, 200] {
            let a: Vec<u8> = (0..w).map(|i| (i * 7 % 5) as u8).collect();
            let b: Vec<u8> = (0..w + 3).map(|i| (i * 3 % 4) as u8).collect();
            assert_maps_agree(&a, &b);
            assert_maps_agree(&b, &a);
            assert_maps_agree(&a[..w / 2], &b);
            assert_maps_agree(&b, &a[..w / 2]);
        }
    }

    /// Around the LCS size cap: just under it the table is used, just over it
    /// the middle counts as replaced; both as in the reference.
    #[test]
    fn line_map_ids_matches_reference_at_the_cell_cap() {
        for w in [1990usize, 2010] {
            let a: Vec<u8> = (0..w).map(|i| (i * 31 % 7) as u8).collect();
            let mut b: Vec<u8> = a.iter().rev().copied().collect();
            b[0] = 9;
            b.push(8);
            assert_maps_agree(&a, &b);
        }
    }

    proptest::proptest! {
        #[test]
        fn line_map_ids_matches_reference(
            o in proptest::collection::vec(0u8..6, 0..300),
            n in proptest::collection::vec(0u8..6, 0..300),
        ) {
            assert_maps_agree(&o, &n);
        }

        #[test]
        fn line_map_ids_matches_reference_on_edits(
            base in proptest::collection::vec(0u8..40, 0..250),
            edits in proptest::collection::vec((0usize..250, 0usize..6, 0u8..40), 0..8),
        ) {
            let mut new = base.clone();
            for (at, del, ins) in edits {
                let at = at.min(new.len());
                let end = (at + del).min(new.len());
                new.splice(at..end, [ins]);
            }
            assert_maps_agree(&base, &new);
        }
    }

    /// Text from `(symbol, terminator)` pairs; terminators mix LF, CRLF, a bare
    /// CR and nothing (only meaningful on the last line).
    fn render_text(v: &[(u8, u8)]) -> String {
        let mut t = String::new();
        for (sym, term) in v {
            t.push_str(&format!("line{sym}"));
            t.push_str(["\n", "\r\n", "\r", "", "\n\n"][usize::from(*term) % 5]);
        }
        t
    }

    fn assert_texts_agree(o: &str, n: &str) {
        let (ol, nl): (Vec<&str>, Vec<&str>) = (o.lines().collect(), n.lines().collect());
        assert_eq!(diff_texts(o, n), line_map(&ol, &nl), "old={o:?} new={n:?}");
    }

    #[test]
    fn diff_texts_matches_reference_on_edge_shapes() {
        for (o, n) in [
            ("", ""),
            ("a", "a"),
            ("a\n", "a"),
            ("a", "a\n"),
            ("a\r\n", "a\n"),
            ("a\n", "a\r\n"),
            ("a\nb", "a\nbb"),
            ("b\n", "xb\n"),
            ("xb\n", "b\n"),
            ("a\nb\n", "a\nx\nb\n"),
            ("a\n\n\nb\n", "a\n\nb\n"),
            ("a\r", "a\r\n"),
            ("\n", "\n\n"),
            ("same\nx\nsame\n", "same\ny\nsame\n"),
            ("ü\nb\n", "ü\nc\n"),
        ] {
            assert_texts_agree(o, n);
            assert_texts_agree(n, o);
        }
    }

    proptest::proptest! {
        #[test]
        fn diff_texts_matches_reference(
            o in proptest::collection::vec((0u8..5, 0u8..5), 0..40),
            n in proptest::collection::vec((0u8..5, 0u8..5), 0..40),
        ) {
            assert_texts_agree(&render_text(&o), &render_text(&n));
        }

        #[test]
        fn diff_texts_matches_reference_on_edits(
            base in proptest::collection::vec((0u8..30, 0u8..2), 0..120),
            edits in proptest::collection::vec((0usize..120, 0usize..5, 0u8..30, 0u8..5), 0..6),
        ) {
            let mut new = base.clone();
            for (at, del, sym, term) in edits {
                let at = at.min(new.len());
                let end = (at + del).min(new.len());
                new.splice(at..end, [(sym, term)]);
            }
            assert_texts_agree(&render_text(&base), &render_text(&new));
        }
    }

    /// The pre-optimisation blame (batched blob reads, `line_map` over
    /// strings), minus the derived-data cache. The oracle for the optimised walk.
    async fn blame_oracle(b: &Backend, rev: &str, path: &RepoPath) -> OpResult<Vec<BlameLine>> {
        let id = query::resolve_rev(b, rev).await?;
        let info = query::read_path(b, rev, path, true).await?.unwrap();
        let text = utf8(&info.content.unwrap(), path)?;
        let lines: Vec<&str> = text.lines().collect();
        let mut history = match indexed_ref_for(b, rev, id).await? {
            Some(r) => {
                let g = b.meta.get_commits(&[id]).await?.pop().flatten().unwrap().generation;
                let hits = b.meta.query_history(&r, path, g, usize::MAX).await?;
                FileHistory::Indexed {
                    queue: hits.into_iter().map(|(_, id)| id).collect(),
                    base: r,
                    below: 0,
                    page: IndexPage::Done,
                }
            }
            None => FileHistory::Scan { next: Some(id), buf: VecDeque::new() },
        };
        let mut cur: Vec<Option<usize>> = (0..lines.len()).map(Some).collect();
        let mut owners: Vec<Option<ObjectId>> = vec![None; lines.len()];
        let mut remaining = lines.len();
        let mut cur_text = text.clone();
        let mut oldest = id;
        'walk: while remaining > 0 {
            let step = history.next(b, path, 64).await?;
            if step.is_empty() {
                break;
            }
            let mut blob_ids: Vec<ObjectId> =
                step.iter().filter_map(|(_, c)| c.old.map(|(_, id)| id)).collect();
            blob_ids.sort_unstable();
            blob_ids.dedup();
            let fetched = b.objects.get_many(&blob_ids).await?;
            let old_blobs: HashMap<_, _> = blob_ids.iter().copied().zip(fetched).collect();
            for (commit, change) in step {
                if change.kind == ChangeKind::Deleted {
                    continue;
                }
                oldest = commit;
                let old_text = match change.old {
                    Some((_, bid)) => utf8(old_blobs[&bid].as_ref().unwrap().body(), path)?,
                    None => String::new(),
                };
                let map = line_map(
                    &old_text.lines().collect::<Vec<_>>(),
                    &cur_text.lines().collect::<Vec<_>>(),
                );
                for (slot, owner) in cur.iter_mut().zip(owners.iter_mut()) {
                    let Some(j) = *slot else { continue };
                    match map.get(j).copied().flatten() {
                        Some(o) => *slot = Some(o),
                        None => {
                            *owner = Some(commit);
                            *slot = None;
                            remaining -= 1;
                        }
                    }
                }
                cur_text = old_text;
                if change.old.is_none() || remaining == 0 {
                    break 'walk;
                }
            }
        }
        Ok(attribute(owners.into_iter().map(|o| o.unwrap_or(oldest)).collect(), lines))
    }

    /// Deterministic pseudo-random histories (edits, deletions and
    /// re-additions, CRLF, empty files, absent final newline, commits that
    /// leave the file alone, merges of side branches whose first-parent
    /// changeset rewrites the file): the incremental blame equals the oracle
    /// for every version and for side commits, indexed (caught up or
    /// lagging) and not, blamed oldest first (each from its predecessor's
    /// stored value), newest first (cold walks) or shuffled, cached and not.
    #[tokio::test]
    async fn blame_equals_oracle_on_random_histories() {
        for seed in 0..24u64 {
            blame_matches_oracle_on(seed, 60).await;
        }
    }

    /// [`blame_equals_oracle_on_random_histories`] with more and longer
    /// histories (`PROPTEST_CASES` seeds, default 24).
    #[tokio::test]
    #[ignore = "slow; run by the deeper property tier"]
    async fn blame_equals_oracle_on_many_random_histories() {
        let cases = std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(24);
        for seed in 1000..1000 + cases {
            blame_matches_oracle_on(seed, 120).await;
        }
    }

    async fn blame_matches_oracle_on(seed: u64, steps: usize) {
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut next = move |m: usize| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            (rng % m as u64) as usize
        };
        let b = Backend::in_memory();
        let nl = if seed.is_multiple_of(3) { "\r\n" } else { "\n" };
        let mut lines: Vec<String> = (0..next(40)).map(|i| format!("init {i}")).collect();
        let render = |l: &[String]| {
            let mut t = l.join(nl);
            if !l.is_empty() && !l.len().is_multiple_of(5) {
                t.push_str(nl);
            }
            t
        };
        let edit = |lines: &mut Vec<String>, step: usize, next: &mut dyn FnMut(usize) -> usize| {
            for _ in 0..1 + next(4) {
                let at = next(lines.len() + 1);
                match next(3) {
                    0 => lines.insert(at, format!("new {step} {}", next(5))),
                    1 if at < lines.len() => {
                        lines.remove(at);
                    }
                    _ if at < lines.len() => lines[at] = format!("mod {step} {}", next(3)),
                    _ => {}
                }
            }
            if next(8) == 0 {
                lines.reverse();
            }
        };
        let c0 =
            commit(&b, None, vec![write("f", &render(&lines)), write("o", "x")]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let mut tip = c0;
        let mut exists = true;
        let mut revs = vec![tip];
        for step in 0..steps {
            if step == steps / 2 && seed.is_multiple_of(2) {
                // Indexed up to here; later commits lag behind the watermark.
                catch_up(&b, "main").await.unwrap();
            }
            if exists && next(8) == 0 {
                // A side branch forked from an earlier version, merged with
                // the file as the side has it.
                let fork = revs[next(revs.len())];
                let mut side_lines: Vec<String> = match read(&b, fork, "f").await {
                    Some(t) => t.lines().map(str::to_owned).collect(),
                    None => Vec::new(),
                };
                edit(&mut side_lines, step + 1000, &mut next);
                let side = commit(&b, Some(fork), vec![write("f", &render(&side_lines))]).await;
                let side = side.unwrap();
                let side_tip = if next(2) == 0 {
                    edit(&mut side_lines, step + 2000, &mut next);
                    commit(&b, Some(side), vec![write("f", &render(&side_lines))]).await.unwrap()
                } else {
                    side
                };
                let mut merged = side_lines.clone();
                if next(2) == 0 {
                    edit(&mut merged, step + 3000, &mut next);
                }
                let tree = commit(&b, Some(tip), vec![write("f", &render(&merged))]).await.unwrap();
                let tree = b.meta.get_commits(&[tree]).await.unwrap()[0].clone().unwrap().tree;
                let merge = ant_core::Commit {
                    tree,
                    parents: vec![tip, side_tip],
                    author: crate::ops::test_support::sig(),
                    committer: crate::ops::test_support::sig(),
                    extra_headers: vec![],
                    message: format!("merge {step}\n").into_bytes(),
                };
                let merge = ant_store::graph::write_commit(&b, &merge).await.unwrap();
                let moved = b.refs.cas_ref("main", Some(tip), Some(merge)).await.unwrap();
                assert_eq!(moved, ant_store::CasOutcome::Updated);
                lines = merged;
                revs.extend([side, side_tip, merge]);
                tip = merge;
                continue;
            }
            let mut ops = vec![write("o", &step.to_string())];
            if !exists {
                exists = true;
                lines = (0..next(30)).map(|i| format!("again {step} {i}")).collect();
                ops.push(write("f", &render(&lines)));
            } else if next(15) == 0 {
                exists = false;
                ops.push(delete("f"));
            } else if next(3) != 0 {
                // Otherwise only `o` changes: this tip shares the last change.
                edit(&mut lines, step, &mut next);
                ops.push(write("f", &render(&lines)));
            }
            tip = landed(&b, tip, ops).await;
            revs.push(tip);
        }
        let path = RepoPath::new("f").unwrap();
        let mut order: Vec<String> = revs.iter().map(ObjectId::to_string).collect();
        match seed % 3 {
            0 => {}
            1 => order.reverse(),
            _ => {
                for i in (1..order.len()).rev() {
                    order.swap(i, next(i + 1));
                }
            }
        }
        // Ref revs too: "main" (lagging on even seeds), "main~3" (indexed).
        order.splice(order.len() / 2..order.len() / 2, ["main".to_string(), "main~3".into()]);
        for rev in order {
            if query::read_path(&b, &rev, &path, true).await.unwrap().is_none() {
                continue;
            }
            let want = blame_oracle(&b, &rev, &path).await.unwrap();
            assert_eq!(blame(&b, &rev, &path).await.unwrap(), want, "seed {seed} {rev}");
            assert_eq!(blame(&b, &rev, &path).await.unwrap(), want, "cached, seed {seed} {rev}");
        }
    }

    /// Micro-benchmark: blame of a long-lived 1700-line file with 1200
    /// versions: the reference walk, first blames (cold, at different
    /// versions), cached, after landing 1/10/100 commits that leave the file
    /// alone, and after commits that change it (incremental; with the index
    /// caught up and lagging). In memory by default; with
    /// `ANT_BLAME_BENCH_SCHEMA=<schema>` on Postgres (localhost:54329) and S3
    /// (localhost:9009, bucket `ANT_BLAME_BENCH_BUCKET`, default `ant-blame`)
    /// like a deployment. Run with
    /// `cargo test -p ant-server --release --lib blame_bench -- --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "benchmark"]
    async fn blame_bench() {
        use std::time::{Duration, Instant};
        let b = match std::env::var("ANT_BLAME_BENCH_SCHEMA") {
            Err(_) => Backend::in_memory(),
            Ok(schema) => {
                #[derive(clap::Parser)]
                struct Args {
                    #[command(flatten)]
                    store: crate::config::StoreArgs,
                }
                let bucket =
                    std::env::var("ANT_BLAME_BENCH_BUCKET").unwrap_or_else(|_| "ant-blame".into());
                let args = <Args as clap::Parser>::parse_from([
                    "bench",
                    "--pg-url=postgres://ant:ant@localhost:54329/ant",
                    &format!("--pg-schema={schema}"),
                    "--s3-endpoint=http://localhost:9009",
                    &format!("--s3-bucket={bucket}"),
                    "--s3-access-key=x",
                    "--s3-secret-key=x",
                ]);
                args.store.backend().await.unwrap()
            }
        };
        let mut rng = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move |m: usize| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            (rng % m as u64) as usize
        };
        let mut lines: Vec<String> = (0..1700).map(|i| format!("    int v{i} = f({i});")).collect();
        let c0 = commit(&b, None, vec![write("f", &(lines.join("\n") + "\n"))]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let mut tip = c0;
        let mut tips = vec![c0];
        let mut edit = |lines: &mut Vec<String>, step: usize| {
            let k = if step.is_multiple_of(10) { 2 } else { 1 + next(3) };
            for e in 0..k {
                // every tenth commit edits both ends of the file
                let at = if step.is_multiple_of(10) {
                    [3, lines.len() - 3][e]
                } else {
                    next(lines.len())
                };
                match next(3) {
                    0 => lines.insert(at, format!("    new {step} {e}")),
                    1 => {
                        lines.remove(at);
                    }
                    _ => lines[at] = format!("    mod {step} {e}"),
                }
            }
        };
        let versions = if cfg!(debug_assertions) { 150 } else { 1200 };
        for step in 0..versions {
            edit(&mut lines, step);
            tip = landed(&b, tip, vec![write("f", &(lines.join("\n") + "\n"))]).await;
            tips.push(tip);
        }
        catch_up(&b, "main").await.unwrap();
        let path = RepoPath::new("f").unwrap();
        let time = |d: &mut Vec<Duration>, t: Instant| d.push(t.elapsed());
        let report = |name: &str, v: &mut Vec<Duration>| {
            v.sort();
            println!(
                "blame {name}: median {:?} (min {:?}, max {:?}, n {})",
                v[v.len() / 2],
                v[0],
                v[v.len() - 1],
                v.len()
            );
        };
        let mut reference = vec![];
        let rev = tip.to_string();
        for _ in 0..3 {
            let t = Instant::now();
            blame_oracle(&b, &rev, &path).await.unwrap();
            time(&mut reference, t);
        }
        report("reference walk", &mut reference);
        // Cold: newest version first, then older ones (nothing stored below).
        let mut first = vec![];
        for i in 0..5 {
            let rev = tips[tips.len() - 1 - 7 * i].to_string();
            let t = Instant::now();
            let got = blame(&b, &rev, &path).await.unwrap();
            time(&mut first, t);
            if i == 0 {
                assert_eq!(got, blame_oracle(&b, &rev, &path).await.unwrap());
            }
        }
        report("first", &mut first);
        let mut cached = vec![];
        for _ in 0..5 {
            let t = Instant::now();
            blame(&b, "main", &path).await.unwrap();
            time(&mut cached, t);
        }
        report("cached", &mut cached);
        // Commits that leave the file alone: the value is shared.
        for n in [1, 10, 100] {
            for i in 0..n {
                tip = landed(&b, tip, vec![write("g", &format!("{n} {i}"))]).await;
            }
            catch_up(&b, "main").await.unwrap();
            let mut after = vec![];
            let t = Instant::now();
            blame(&b, "main", &path).await.unwrap();
            time(&mut after, t);
            report(&format!("after {n} commits to other paths"), &mut after);
        }
        // Commits that change the file: derived from the previous value.
        for lagging in [false, true] {
            let mut incremental = vec![];
            for i in 0..5 {
                edit(&mut lines, 7 + versions + i);
                tip = landed(&b, tip, vec![write("f", &(lines.join("\n") + "\n"))]).await;
                if !lagging {
                    catch_up(&b, "main").await.unwrap();
                }
                let t = Instant::now();
                let got = blame(&b, "main", &path).await.unwrap();
                time(&mut incremental, t);
                if i == 0 {
                    assert_eq!(got, blame_oracle(&b, "main", &path).await.unwrap());
                }
            }
            let index = if lagging { "lagging" } else { "caught up" };
            report(&format!("after a commit changing it (index {index})"), &mut incremental);
        }
    }

    #[tokio::test]
    async fn blame_roundtrip_encoding() {
        let ids = [ObjectId([1; 32]), ObjectId([2; 32]), ObjectId([1; 32])];
        let enc = encode_blame(&ids);
        assert_eq!(decode_blame(&enc).unwrap(), ids);
        assert!(decode_blame(&enc[..enc.len() - 1]).is_none());
        assert!(decode_blame(&[]).is_none());
        let mut long = enc.to_vec();
        long.push(0);
        assert!(decode_blame(&long).is_none());
        // An owner index outside the table.
        let mut bad = enc.to_vec();
        let at = bad.len() - 4;
        bad[at] = 7;
        assert!(decode_blame(&bad).is_none());
    }

    /// The derivation's own rule, folded bottom-up over every version
    /// (`derive::fold`), is the walk's answer at every version.
    #[tokio::test]
    async fn blame_fold_equals_walk() {
        let b = Backend::in_memory();
        let versions = [
            "a\nb\nc\n",
            "a\nB\nc\nd\n",
            "z\na\nB\nc\nd\n",
            "z\na\nc\nd\nB\n",
            "z\na\nc\nd\nB\n\nend",
        ];
        let c0 = commit(&b, None, vec![write("f", versions[0])]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let mut commits = vec![c0];
        for (i, v) in versions.iter().enumerate().skip(1) {
            let tip = landed(&b, commits[i - 1], vec![write("f", v)]).await;
            commits.push(tip);
        }
        let path = RepoPath::new("f").unwrap();
        let chain = commits.iter().enumerate().map(|(i, c)| {
            let old = if i == 0 { "" } else { versions[i - 1] };
            ((*c, path.clone()), diff_texts(old, versions[i]))
        });
        let folded = derive::fold::<Blame>(None, chain);
        for ((c, _), owners) in folded {
            let got: Vec<ObjectId> =
                blame(&b, &c.to_hex(), &path).await.unwrap().iter().map(|l| l.commit).collect();
            assert_eq!(got, owners);
        }
    }

    /// Revisions sharing the last change to a file share one stored value,
    /// keyed by that change; a later change is derived from it (a planted
    /// value shows through), not walked again; a value that does not fit
    /// the version (line count) is ignored.
    #[tokio::test]
    async fn blame_is_keyed_by_the_last_change_and_derived_from_it() {
        let b = Backend::in_memory();
        let c0 = commit(&b, None, vec![write("f", "a\nb\nc\n")]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let c1 = landed(&b, c0, vec![write("f", "a\nB\nc\n")]).await;
        let c2 = landed(&b, c1, vec![write("g", "1\n")]).await;
        let c3 = landed(&b, c2, vec![write("g", "2\n")]).await;
        catch_up(&b, "main").await.unwrap();
        let f = RepoPath::new("f").unwrap();
        let key = |c: ObjectId| (c, f.clone());
        let at_c1 = blame(&b, &c1.to_hex(), &f).await.unwrap();
        let stored = derive::get::<Blame>(b.meta.as_ref(), &key(c1)).await.unwrap();
        assert_eq!(stored, Some(vec![c0, c1, c0]));
        for rev in [c2.to_hex(), c3.to_hex(), "main".into(), "main~1".into()] {
            assert_eq!(blame(&b, &rev, &f).await.unwrap(), at_c1, "{rev}");
        }
        for c in [c2, c3] {
            assert_eq!(derive::get::<Blame>(b.meta.as_ref(), &key(c)).await.unwrap(), None);
        }
        // Plant a (wrong, but well-formed) value for c1: c4's blame takes the
        // owners of its unchanged lines from it.
        let planted = ObjectId([7; 32]);
        let c1b = landed(&b, c3, vec![write("f", "x\ny\nz\n")]).await;
        b.meta
            .put_derived(BLAME_KIND, &Blame::key(&key(c1b)), encode_blame(&[planted; 3]))
            .await
            .unwrap();
        let c4 = landed(&b, c1b, vec![write("f", "x\nY\nz\n")]).await;
        let got: Vec<_> =
            blame(&b, "main", &f).await.unwrap().into_iter().map(|l| l.commit).collect();
        assert_eq!(got, vec![planted, c4, planted]);
        // A planted value with the wrong line count is not used.
        let c5 = landed(&b, c4, vec![write("f", "x\nY\nz\nw\n")]).await;
        b.meta
            .put_derived(BLAME_KIND, &Blame::key(&key(c5)), encode_blame(&[planted; 2]))
            .await
            .unwrap();
        let got: Vec<_> =
            blame(&b, "main", &f).await.unwrap().into_iter().map(|l| l.commit).collect();
        assert_eq!(got, vec![planted, c4, planted, c5]);
        // Rows of the old per-revision kind are ignored.
        let c6 = landed(&b, c5, vec![write("g", "3\n")]).await;
        let mut v1_key = c6.as_bytes().to_vec();
        v1_key.extend(b"f");
        b.meta.put_derived("blame/v1", &v1_key, encode_blame(&[planted; 4])).await.unwrap();
        assert_eq!(blame(&b, "main", &f).await.unwrap().last().unwrap().commit, c5);
    }

    /// Micro-benchmark: 20k-commit linear history in Postgres. Prints
    /// timings; run with
    /// `cargo test -p ant-server --release history_bench -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore = "benchmark; needs postgres: docker compose up -d"]
    async fn history_bench_postgres() {
        use ant_core::{Commit, Object, Signature};
        use ant_store::postgres::PgStore;
        use ant_store::{ChangeKind, CommitNode, PathChange};
        use std::sync::Arc;
        use std::time::Instant;

        const N: usize = 20_000;
        let url = ant_store::testing::pg_url();
        let schema = ant_store::testing::scratch_schema("hist");
        let pg = Arc::new(PgStore::connect_scratch(&url, &schema).await.unwrap());
        let b = Backend { objects: pg.clone(), refs: pg.clone(), meta: pg.clone() };

        let t = Instant::now();
        let sig =
            Signature { name: "bench".into(), email: "b@x".into(), time: 0, tz_offset_minutes: 0 };
        let mut parent: Option<ObjectId> = None;
        let (mut objs, mut tip) = (Vec::new(), None);
        for i in 0..N {
            let commit = Commit {
                tree: ObjectId::empty_tree(),
                parents: parent.into_iter().collect(),
                author: sig.clone(),
                committer: Signature { time: i as i64, ..sig.clone() },
                extra_headers: vec![],
                message: format!("commit {i}\n").into_bytes(),
            };
            let obj = Object::Commit(commit).encode();
            let id = obj.id();
            objs.push(obj);
            let path = RepoPath::new(format!("dir{}/file{}", i % 50, i % 997)).unwrap();
            let kind = if i < 997 { ChangeKind::Added } else { ChangeKind::Modified };
            let change = PathChange {
                path,
                kind,
                old: None,
                new: Some((ant_core::EntryMode::File, ObjectId([(i % 251) as u8; 32]))),
            };
            let node = CommitNode {
                id,
                tree: ObjectId::empty_tree(),
                parents: parent.into_iter().collect(),
                generation: i as u64 + 1,
                committer_time: i as i64,
            };
            if objs.len() == 1000 {
                b.objects.put_many(&std::mem::take(&mut objs)).await.unwrap();
            }
            b.meta.put_commit(node, vec![change]).await.unwrap();
            parent = Some(id);
            tip = Some(id);
        }
        b.objects.put_many(&objs).await.unwrap();
        let tip = tip.unwrap();
        b.refs.cas_ref("main", None, Some(tip)).await.unwrap();
        eprintln!("setup: {N} commits in {:?}", t.elapsed());

        let p = RepoPath::new("dir7/file7").unwrap();
        let t = Instant::now();
        let slow = query::log(&b, "main", Some(&p), 0).await.unwrap();
        let scan = t.elapsed();
        // The unindexed query scheduled a background catch-up: let it
        // finish and drop what it indexed, so the timed catch-up below
        // indexes the whole history from scratch.
        while running_catch_ups() > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        b.meta.clear_history("main").await.unwrap();

        let t = Instant::now();
        let n = catch_up(&b, "main").await.unwrap();
        let idx = t.elapsed();
        assert_eq!(n, N);

        let t = Instant::now();
        let fast = query::log(&b, "main", Some(&p), 0).await.unwrap();
        let indexed = t.elapsed();
        assert_eq!(fast, slow);
        let t = Instant::now();
        let again = catch_up(&b, "main").await.unwrap();
        eprintln!(
            "catch_up({N} commits) = {idx:?}; no-op catch_up = {:?} ({again}); \
             log --path ({} hits): scan {scan:?} -> indexed {indexed:?}",
            t.elapsed(),
            fast.len()
        );
    }
}
