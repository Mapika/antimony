//! Path history and blame, backed by the per-ref history index
//! (`MetaStore::{index_history, query_history, ...}`).
//!
//! - `catch_up(b, ref)` runs after every successful `land` and `UpdateRef`
//!   (best effort: errors are logged; queries fall back, see `log`).
//! - `ops::query::log` with a path: a rev naming a ref goes to
//!   `lagging_path_log`, which reads that ref's watermark once — rev at or
//!   behind it → index only; rev ahead of it (a busy ref between catch-ups)
//!   → scan only the unindexed commits and take the rest from the index. A
//!   commit-id rev uses `path_log` when it `is_first_parent_ancestor` of an
//!   indexed ref's watermark (`indexed_ref_for`). Anything else scans
//!   recorded changesets — on a ref under constant landing that scan walks
//!   the whole history, so the index paths must cover every race between
//!   resolving the rev and a concurrent catch-up.
//! - Exposed as the `Blame` RPC and `ant blame <rev>:<path>` (output per
//!   line: `<commit-id> <line-no>\t<line>`).
//!
//! `catch_up(b, ref_name)`:
//! - Let `tip` = ref target, `wm` = watermark. If `wm` is None → index the
//!   whole first-parent chain of `tip`. If `wm` is a first-parent ancestor of
//!   `tip` → index only commits after it. Otherwise (ref moved non-FF) →
//!   `clear_history` and rebuild from scratch.
//! - Rows: for each commit on the chain, one row per path in its recorded
//!   changeset (`MetaStore::get_changeset`), with the commit's generation.
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
//! against the newer one (`line_map`, an LCS) and attributes lines that
//! commit introduced; lines surviving to the file's creation belong to the
//! creating commit. Results are cached as derived data `"blame/v1"`,
//! key = rev id ++ path bytes, value = compact encoding.
//! Binary (non-UTF-8) files → `InvalidArgument`. Missing path → `NotFound`.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};

use ant_core::{ObjectId, RepoPath};
use ant_store::{Backend, ChangeKind, HistoryRow, PathChange};
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
/// Derived-data kind for cached blame results.
const BLAME_KIND: &str = "blame/v1";
/// Commits whose changesets/blobs are fetched per blame step.
const BLAME_CHUNK: usize = 64;
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
/// reached (always false when `stop` is None). One store call per
/// `CHAIN_CHUNK` commits.
async fn walk_chain(
    b: &Backend,
    tip: ObjectId,
    stop: Option<ObjectId>,
) -> OpResult<(Vec<(ObjectId, u64)>, bool)> {
    let mut out = Vec::new();
    let mut from = tip;
    loop {
        if Some(from) == stop {
            return Ok((out, true));
        }
        let chunk = b.meta.first_parent_chain(from, stop, CHAIN_CHUNK).await?;
        let Some(last) = chunk.last() else {
            return Err(OpError::NotFound(format!("commit {from}")));
        };
        out.extend(chunk.iter().map(|n| (n.id, n.generation)));
        match last.parents.first() {
            None => return Ok((out, false)),
            Some(p) if Some(*p) == stop => return Ok((out, true)),
            Some(p) if chunk.len() == CHAIN_CHUNK => from = *p,
            Some(p) => return Err(OpError::NotFound(format!("commit {p}"))),
        }
    }
}

/// True if `ancestor` lies on the first-parent chain of `descendant`
/// (inclusive). Prunes by generation.
pub async fn is_first_parent_ancestor(
    b: &Backend,
    ancestor: ObjectId,
    descendant: ObjectId,
) -> OpResult<bool> {
    const CHUNK: usize = 1024;
    if ancestor == descendant {
        return Ok(true);
    }
    let [Some(target), Some(desc)] =
        <[_; 2]>::try_from(b.meta.get_commits(&[ancestor, descendant]).await?)
            .unwrap_or([None, None])
    else {
        return Ok(false);
    };
    // Generations strictly decrease along a chain: a newer `ancestor` (the
    // common case of a tip ahead of a lagging watermark) needs no walk, and
    // otherwise the walk is at most the generation gap long.
    if desc.generation <= target.generation {
        return Ok(false);
    }
    let gap = usize::try_from(desc.generation - target.generation).unwrap_or(usize::MAX);
    let mut from = descendant;
    loop {
        let chunk = b.meta.first_parent_chain(from, Some(ancestor), CHUNK.min(gap)).await?;
        let Some(last) = chunk.last() else { return Ok(false) };
        match last.parents.first() {
            Some(p) if *p == ancestor => return Ok(true),
            Some(p) if chunk.len() == CHUNK && last.generation > target.generation + 1 => from = *p,
            _ => return Ok(false),
        }
    }
}

/// Bring `ref_name`'s history index up to date. Returns commits indexed.
pub async fn catch_up(b: &Backend, ref_name: &str) -> OpResult<usize> {
    catch_up_observed(b, ref_name, None).await
}

/// [`catch_up`] reporting the index lag and the duration of runs that indexed
/// something to `metrics`.
pub async fn catch_up_observed(
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
            b.meta.clear_history(ref_name).await?;
            wm = None;
        }
        chain.reverse();
        lag(chain.len());
        let mut pos = 0;
        while pos < chain.len() {
            let end = (pos + INDEX_BATCH).min(chain.len());
            let batch = &chain[pos..end];
            let ids: Vec<ObjectId> = batch.iter().map(|(id, _)| *id).collect();
            let sets = b.meta.get_changesets(&ids).await?;
            let mut rows = Vec::new();
            for ((id, generation), set) in batch.iter().zip(sets) {
                let set = set.ok_or_else(|| OpError::NotFound(format!("changeset of {id}")))?;
                rows.extend(set.into_iter().map(|c| HistoryRow {
                    path: c.path,
                    generation: *generation,
                    commit: *id,
                }));
            }
            b.meta.index_history(ref_name, &rows).await?;
            let last = batch[batch.len() - 1].0;
            if b.meta.set_history_watermark(ref_name, wm, last).await? {
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

type CatchUpKey = (usize, String);
/// Running background catch-ups; the flag means "run once more when done".
static CATCH_UPS: LazyLock<Mutex<HashMap<CatchUpKey, bool>>> = LazyLock::new(Default::default);

/// Number of (backend, ref) catch-ups currently running in the background.
pub(crate) fn running_catch_ups() -> usize {
    CATCH_UPS.lock().unwrap_or_else(|e| e.into_inner()).len()
}

/// Fire-and-forget `catch_up`, coalesced per (backend, ref): if one is already
/// running it is told to run once more instead of starting a second. Errors
/// are logged; queries fall back to changeset scans.
pub fn schedule_catch_up(b: &Backend, ref_name: &str) {
    schedule_catch_up_observed(b, ref_name, None);
}

/// [`schedule_catch_up`] reporting to `metrics`.
pub fn schedule_catch_up_observed(b: &Backend, ref_name: &str, metrics: Option<Arc<Metrics>>) {
    let key = (Arc::as_ptr(&b.meta).cast::<()>() as usize, ref_name.to_string());
    {
        let mut running = CATCH_UPS.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(dirty) = running.get_mut(&key) {
            *dirty = true;
            return;
        }
        running.insert(key.clone(), false);
    }
    let b = b.clone();
    let ref_name = ref_name.to_string();
    tokio::spawn(async move {
        loop {
            if let Err(error) = catch_up_observed(&b, &ref_name, metrics.as_deref()).await {
                tracing::warn!(%error, ref_name, "history catch-up failed");
            }
            let mut running = CATCH_UPS.lock().unwrap_or_else(|e| e.into_inner());
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
async fn log_entries(b: &Backend, hits: Vec<(u64, ObjectId)>) -> OpResult<Vec<LogEntry>> {
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
/// non-fast-forward, lag too long); the caller then scans changesets.
pub async fn lagging_path_log(
    b: &Backend,
    rev: &str,
    tip: ObjectId,
    path: &RepoPath,
    limit: usize,
) -> OpResult<Option<Vec<LogEntry>>> {
    let Some(base) = rev_ref(rev) else { return Ok(None) };
    let Some(wm) = b.meta.history_watermark(base).await? else { return Ok(None) };
    let limit = if limit == 0 { usize::MAX } else { limit };
    let [Some(tip_node), Some(wm_node)] =
        <[_; 2]>::try_from(cache::get_nodes(b, &[tip, wm]).await?).unwrap_or([None, None])
    else {
        return Ok(None);
    };
    if tip_node.generation <= wm_node.generation {
        // Without this, a watermark that caught up between resolving `tip`
        // and reading it sent the query to a full changeset scan.
        if !is_first_parent_ancestor(b, tip, wm).await? {
            return Ok(None);
        }
        let hits = b.meta.query_history(base, path, tip_node.generation, limit).await?;
        return log_entries(b, hits).await.map(Some);
    }
    // Generations strictly decrease along the chain: it is at most the gap long.
    let gap = usize::try_from(tip_node.generation - wm_node.generation).unwrap_or(usize::MAX);
    let chain = b.meta.first_parent_chain(tip, Some(wm), MAX_LAG.min(gap)).await?;
    if chain.last().and_then(|n| n.parents.first()) != Some(&wm) {
        return Ok(None);
    }
    let ids: Vec<ObjectId> = chain.iter().map(|n| n.id).collect();
    let sets = cache::get_changesets(b, &ids).await?;
    let mut hits = Vec::new();
    for (node, set) in chain.iter().zip(sets) {
        if hits.len() >= limit {
            break;
        }
        let set = set.ok_or_else(|| OpError::NotFound(format!("changeset of {}", node.id)))?;
        if set.iter().any(|c| c.path.starts_with(path)) {
            hits.push((node.generation, node.id));
        }
    }
    if hits.len() < limit {
        // Bounded by the watermark's generation: rows a concurrent catch-up
        // adds for the commits just scanned are not returned twice.
        let rest = limit - hits.len();
        hits.extend(b.meta.query_history(base, path, wm_node.generation, rest).await?);
    }
    log_entries(b, hits).await.map(Some)
}

// ---------------------------------------------------------------------------
// Blame
// ---------------------------------------------------------------------------

/// Where the (newest-first) list of commits touching one file comes from.
enum FileHistory {
    Indexed(VecDeque<ObjectId>),
    Scan { next: Option<ObjectId>, buf: VecDeque<ObjectId> },
}

impl FileHistory {
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
            let (queue, next) = match self {
                FileHistory::Indexed(q) => (q, None),
                FileHistory::Scan { next, buf } => (buf, Some(next)),
            };
            let scan = next.is_some();
            if queue.is_empty() {
                let Some(from) = next.as_ref().and_then(|n| **n) else { break };
                let chunk = b.meta.first_parent_chain(from, None, INDEX_BATCH).await?;
                if let Some(slot) = next {
                    *slot = chunk.last().and_then(|c| c.parents.first().copied());
                }
                queue.extend(chunk.iter().map(|c| c.id));
                if queue.is_empty() {
                    break;
                }
            }
            let take = queue.len().min(if scan { INDEX_BATCH } else { n });
            let ids: Vec<ObjectId> = queue.drain(..take).collect();
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

fn decode_blame(v: &[u8], lines: usize) -> Option<Vec<ObjectId>> {
    let u32_at = |o: usize| Some(u32::from_le_bytes(v.get(o..o + 4)?.try_into().ok()?) as usize);
    let n = u32_at(0)?;
    let table: Vec<ObjectId> = (0..n)
        .map(|i| ObjectId::from_bytes(v.get(4 + i * 32..36 + i * 32)?).ok())
        .collect::<Option<_>>()?;
    let base = 4 + n * 32;
    if u32_at(base)? != lines || v.len() != base + 4 + lines * 4 {
        return None;
    }
    (0..lines).map(|i| table.get(u32_at(base + 4 + i * 4)?).copied()).collect()
}

/// For each line of `new`, the index of the matching line of `old`, or None
/// if the line was introduced. Common prefix/suffix are trimmed, then an LCS
/// runs on the middle (counted as fully replaced if too large).
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

fn utf8(bytes: &[u8], path: &RepoPath) -> OpResult<String> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| OpError::InvalidArgument(format!("{path} is not valid UTF-8")))
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

    let mut key = id.as_bytes().to_vec();
    key.extend(path.as_str().as_bytes());
    if let Some(v) = b.meta.get_derived(BLAME_KIND, &key).await?
        && let Some(owners) = decode_blame(&v, lines.len())
    {
        return Ok(attribute(owners, lines));
    }

    let mut history = match indexed_ref_for(b, rev, id).await? {
        Some(r) => {
            let g =
                b.meta.get_commits(&[id]).await?.pop().flatten().map_or(u64::MAX, |n| n.generation);
            let hits = b.meta.query_history(&r, path, g, usize::MAX).await?;
            FileHistory::Indexed(hits.into_iter().map(|(_, id)| id).collect())
        }
        None => FileHistory::Scan { next: Some(id), buf: VecDeque::new() },
    };

    // `cur[k]` = line index, in the version being walked, of final line k.
    let mut cur: Vec<Option<usize>> = (0..lines.len()).map(Some).collect();
    let mut owners: Vec<Option<ObjectId>> = vec![None; lines.len()];
    let mut remaining = lines.len();
    let mut cur_text = text.clone();
    let mut oldest = id;
    'walk: while remaining > 0 {
        let step = history.next(b, path, BLAME_CHUNK).await?;
        if step.is_empty() {
            break;
        }
        let mut blob_ids: Vec<ObjectId> =
            step.iter().filter_map(|(_, c)| c.old.map(|(_, id)| id)).collect();
        blob_ids.sort_unstable();
        blob_ids.dedup();
        let fetched = b.objects.get_many(&blob_ids).await?;
        let mut old_blobs: HashMap<ObjectId, ant_core::EncodedObject> = HashMap::new();
        for (bid, o) in blob_ids.iter().zip(fetched) {
            old_blobs.insert(*bid, o.ok_or_else(|| OpError::not_found_object(*bid))?);
        }
        for (commit, change) in step {
            if change.kind == ChangeKind::Deleted {
                continue;
            }
            oldest = commit;
            let old_text = match change.old {
                Some((_, bid)) => utf8(old_blobs[&bid].body(), path)?,
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
    // History ran out (e.g. pruned): attribute leftovers to the oldest commit seen.
    let owners: Vec<ObjectId> = owners.into_iter().map(|o| o.unwrap_or(oldest)).collect();
    b.meta.put_derived(BLAME_KIND, &key, encode_blame(&owners)).await?;
    Ok(attribute(owners, lines))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::land::{LandOutcome, land};
    use crate::ops::query;
    use crate::ops::test_support::{commit, delete, write};
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
        let _ = delete; // keep helper import used across variants
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

    #[tokio::test]
    async fn blame_roundtrip_encoding() {
        let ids = [ObjectId([1; 32]), ObjectId([2; 32]), ObjectId([1; 32])];
        let enc = encode_blame(&ids);
        assert_eq!(decode_blame(&enc, 3).unwrap(), ids);
        assert!(decode_blame(&enc, 2).is_none());
        assert!(decode_blame(&enc[..enc.len() - 1], 3).is_none());
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
        let url = std::env::var("ANT_TEST_PG_URL")
            .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into());
        let schema = format!(
            "hist_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.subsec_nanos())
        );
        let pg = Arc::new(PgStore::connect(&url, &schema).await.unwrap());
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

        use sqlx::Connection;
        if let Ok(mut c) = sqlx::PgConnection::connect(&url).await {
            let _ = sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                .execute(&mut c)
                .await;
        }
    }
}
