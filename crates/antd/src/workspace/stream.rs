//! Pipelined materialization: the tree walk, the blob fetches and the file
//! writes of a new workspace overlap instead of running as phases.
//!
//! - Trees are fetched as soon as they are discovered, in batches, with up
//!   to [`TREE_CALLS`] fetches in flight (no barrier between depths): a
//!   subdirectory is requested the moment its parent is decoded.
//! - Directories wholly in the sparse scope go through [`DirFetch`] (tree
//!   bundles), the rest through `get_many`, exactly as the level walk did.
//!   Whatever a bundle delivered beyond the requested trees (subtrees and
//!   blobs, already stored in the cache by the fetcher) is used directly:
//!   subtrees are expanded without another request and their files written
//!   from memory, without reading the cache back.
//! - Leaves whose blobs did not come with a bundle are fetched in batches of
//!   [`BATCH`] ids (`get_many`; local hits on a warm cache).
//! - Up to [`writers`] write batches run at once, each a blocking task that
//!   creates missing parent directories as it goes (so only directories
//!   with files in scope are created, as before).
//! - Memory is bounded: while more than [`PENDING_BYTES`] of fetched file
//!   content waits for a writer, no new tree fetch starts.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use ant_core::{EncodedObject, EntryMode, ObjectId, RepoPath};
use ant_store::ObjectStore;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};

use super::DirFetch;
use super::sparse::Matcher;
use super::write::{
    Dest, WriteItem, Written, blob_body, decode_tree, fetch_and_write, write_items,
};
use crate::walk::BATCH;

/// Tree fetches in flight.
const TREE_CALLS: usize = 32;
/// Most trees per fetch.
const TREE_BATCH: usize = 64;
/// Files per write batch of in-memory content.
const WRITE_ITEMS: usize = 512;
/// Bytes per write batch of in-memory content.
const WRITE_BYTES: usize = 16 << 20;
/// Fetched file content waiting for (or in) a writer before tree fetches
/// pause.
const PENDING_BYTES: usize = 512 << 20;

/// Write batches in flight.
fn writers() -> usize {
    std::thread::available_parallelism().map_or(8, |n| n.get()).clamp(4, 16)
}

type Leaf = (RepoPath, EntryMode, ObjectId);

/// What one fetched batch of trees contributes.
#[derive(Default)]
struct Expansion {
    /// Subtrees still to fetch.
    subtrees: Vec<(RepoPath, ObjectId)>,
    /// Leaves with their content at hand.
    ready: Vec<WriteItem>,
    /// Leaves whose blobs must be fetched.
    need: Vec<Leaf>,
}

enum Done {
    Trees(anyhow::Result<Expansion>),
    Written { bytes: usize, result: anyhow::Result<Written> },
}

/// Timings and counts of one pipelined materialization (debug log).
#[derive(Default, Debug)]
struct Stats {
    tree_fetches: usize,
    trees_fetched: usize,
    files_from_memory: usize,
    files_fetched: usize,
    write_batches: usize,
}

/// Materialize the leaves of `base_tree` that `matcher` selects into the
/// empty directory `dir` (which must exist). Returns the index entries.
pub(super) async fn materialize_leaves(
    objects: &dyn ObjectStore,
    dirs: Option<&dyn DirFetch>,
    dir: &Path,
    matcher: Arc<Matcher>,
    base_tree: ObjectId,
) -> anyhow::Result<Written> {
    let start = Instant::now();
    let max_writers = writers();
    let max_calls = TREE_CALLS;
    let mut stats = Stats::default();
    let mut queue: VecDeque<(RepoPath, ObjectId)> = VecDeque::from([(RepoPath::root(), base_tree)]);
    let mut ready: Vec<WriteItem> = Vec::new();
    let mut ready_bytes = 0usize;
    let mut need: Vec<Leaf> = Vec::new();
    let mut pending_bytes = 0usize;
    let (mut tree_calls, mut writes) = (0usize, 0usize);
    let mut out: Written = Vec::new();
    let mut tasks: FuturesUnordered<BoxFuture<'_, Done>> = FuturesUnordered::new();
    loop {
        // Start tree fetches: split what is queued over the free slots.
        while tree_calls < max_calls
            && !queue.is_empty()
            && (pending_bytes < PENDING_BYTES || tasks.is_empty())
        {
            let free = max_calls - tree_calls;
            let n = queue.len().div_ceil(free).clamp(1, TREE_BATCH);
            let batch: Vec<(RepoPath, ObjectId)> = queue.drain(..n).collect();
            stats.tree_fetches += 1;
            stats.trees_fetched += batch.len();
            tree_calls += 1;
            let m = matcher.clone();
            tasks.push(Box::pin(async move {
                Done::Trees(fetch_and_expand(objects, dirs, m, batch).await)
            }));
        }
        // Start writes: full batches, or whatever is left once no tree fetch
        // can add more.
        let draining = tree_calls == 0 && queue.is_empty();
        while writes < max_writers
            && !ready.is_empty()
            && (ready.len() >= WRITE_ITEMS || ready_bytes >= WRITE_BYTES || draining)
        {
            let (mut n, mut bytes) = (0, 0);
            while n < ready.len() && n < WRITE_ITEMS && bytes < WRITE_BYTES {
                bytes += ready[n].body.len();
                n += 1;
            }
            let items: Vec<WriteItem> = ready.drain(..n).collect();
            ready_bytes -= bytes;
            writes += 1;
            stats.write_batches += 1;
            let d = dir.to_path_buf();
            tasks.push(Box::pin(async move {
                let result =
                    tokio::task::spawn_blocking(move || write_items(&d, items, Dest::Fresh))
                        .await
                        .map_err(anyhow::Error::from)
                        .and_then(|r| r);
                Done::Written { bytes, result }
            }));
        }
        while writes < max_writers && !need.is_empty() && (need.len() >= BATCH || draining) {
            let chunk: Vec<Leaf> = need.drain(..need.len().min(BATCH)).collect();
            writes += 1;
            stats.write_batches += 1;
            stats.files_fetched += chunk.len();
            tasks.push(Box::pin(async move {
                let result = fetch_and_write(objects, dir, &chunk, Dest::Fresh).await;
                Done::Written { bytes: 0, result }
            }));
        }
        let Some(done) = tasks.next().await else {
            if queue.is_empty() && ready.is_empty() && need.is_empty() {
                break;
            }
            continue;
        };
        match done {
            Done::Trees(r) => {
                tree_calls -= 1;
                let e = r?;
                queue.extend(e.subtrees);
                for it in &e.ready {
                    ready_bytes += it.body.len();
                    pending_bytes += it.body.len();
                }
                stats.files_from_memory += e.ready.len();
                ready.extend(e.ready);
                need.extend(e.need);
            }
            Done::Written { bytes, result } => {
                writes -= 1;
                pending_bytes -= bytes;
                out.extend(result?);
            }
        }
    }
    tracing::debug!(
        elapsed_ms = start.elapsed().as_millis() as u64,
        ?stats,
        files = out.len(),
        "materialized"
    );
    Ok(out)
}

/// Fetch one batch of trees (directories wholly in scope through `dirs`,
/// the rest with `get_many`) and expand them, and every subtree that came
/// along, on a blocking thread.
async fn fetch_and_expand(
    objects: &dyn ObjectStore,
    dirs: Option<&dyn DirFetch>,
    matcher: Arc<Matcher>,
    batch: Vec<(RepoPath, ObjectId)>,
) -> anyhow::Result<Expansion> {
    let whole: Vec<bool> = match dirs {
        Some(_) => batch.iter().map(|(p, _)| matcher.covers_all_under(p)).collect(),
        None => vec![false; batch.len()],
    };
    let pick = |want: bool| -> Vec<ObjectId> {
        batch.iter().zip(&whole).filter(|(_, w)| **w == want).map(|((_, id), _)| *id).collect()
    };
    let (by_dir, by_id) = (pick(true), pick(false));
    let (dir_objs, extra) = match dirs {
        Some(d) if !by_dir.is_empty() => d.get_dirs_with_contents(&by_dir).await?,
        _ => (Vec::new(), Vec::new()),
    };
    let id_objs = if by_id.is_empty() { Vec::new() } else { objects.get_many(&by_id).await? };
    let (mut dir_objs, mut id_objs) = (dir_objs.into_iter(), id_objs.into_iter());
    let fetched: Vec<Option<EncodedObject>> =
        whole.iter().map(|w| if *w { dir_objs.next() } else { id_objs.next() }.flatten()).collect();
    tokio::task::spawn_blocking(move || expand(&matcher, batch, fetched, extra)).await?
}

/// Blocking: decode `batch` and, depth first, every subtree found in
/// `extra`; leaves found in `extra` come with their content.
fn expand(
    matcher: &Matcher,
    batch: Vec<(RepoPath, ObjectId)>,
    fetched: Vec<Option<EncodedObject>>,
    extra: Vec<(ObjectId, EncodedObject)>,
) -> anyhow::Result<Expansion> {
    let extra: HashMap<ObjectId, EncodedObject> = extra.into_iter().collect();
    let mut out = Expansion::default();
    let mut stack: Vec<(RepoPath, ObjectId, Option<EncodedObject>)> =
        batch.into_iter().zip(fetched).map(|((p, id), o)| (p, id, o)).collect();
    while let Some((parent, id, obj)) = stack.pop() {
        let tree = decode_tree(&id, obj)?;
        for e in tree.entries() {
            let child = parent.join(&e.name)?;
            if e.mode.is_tree() {
                if !matcher.may_contain_under(&child) {
                    continue;
                }
                match extra.get(&e.id) {
                    Some(o) => stack.push((child, e.id, Some(o.clone()))),
                    None => out.subtrees.push((child, e.id)),
                }
            } else if matcher.matches(&child) {
                let mode = e.mode.class();
                if mode == EntryMode::Gitlink {
                    out.ready.push(WriteItem { path: child, mode, id: e.id, body: Bytes::new() });
                    continue;
                }
                match extra.get(&e.id) {
                    Some(o) => {
                        let body = blob_body(o, &e.id)?;
                        out.ready.push(WriteItem { path: child, mode, id: e.id, body });
                    }
                    None => out.need.push((child, mode, e.id)),
                }
            }
        }
    }
    Ok(out)
}
