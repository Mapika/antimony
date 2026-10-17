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
//! - Files are created in path order (bytewise; putting a directory's own
//!   files before its subdirectories measured slower): leaves are held
//!   back and sorted before they are handed out in contiguous batches, so
//!   each writer
//!   creates a directory, its files and its subdirectories together, and
//!   concurrent writers work in different subtrees. On ext4 this matters
//!   more than anything else: in the walk's breadth-first order (every
//!   writer jumping between thousands of directories) a warm 96k-file
//!   checkout on a cloud disk took 30% longer, with threads blocked in the
//!   filesystem rather than busy. Leaves without content (a warm cache) are
//!   held until the walk is done (or [`HOLD_LEAVES`] are waiting); content
//!   delivered by bundles (a cold checkout, bound by the network) is
//!   written once [`HOLD_BYTES`] of it is waiting, so downloads and writes
//!   still overlap.
//! - Leaves whose blobs did not come with a bundle are fetched in batches of
//!   [`BATCH`] ids (`get_many`; local hits on a warm cache), at most
//!   [`fetchers`] at once, apart from the writers: a writer only creates
//!   files, it never waits for a read.
//! - Up to [`writers`] write batches run at once, each a blocking task that
//!   creates missing parent directories as it goes (so only directories
//!   with files in scope are created, as before).
//! - Memory is bounded: while more than [`PENDING_BYTES`] of fetched file
//!   content waits for a writer, no new tree or blob fetch starts.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use ant_core::{EncodedObject, EntryMode, ObjectId, RepoPath};
use ant_store::ObjectStore;
use anyhow::anyhow;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};

use super::DirFetch;
use super::sparse::Matcher;
use super::write::{Dest, WriteItem, Written, blob_body, decode_tree, write_items};
use crate::walk::BATCH;

/// Tree fetches in flight.
const TREE_CALLS: usize = 32;
/// Most trees per fetch.
const TREE_BATCH: usize = 64;
/// Files per write batch.
const WRITE_ITEMS: usize = 512;
/// Bytes per write batch.
const WRITE_BYTES: usize = 16 << 20;
/// Leaves without content held for sorting before fetching starts anyway.
const HOLD_LEAVES: usize = 1 << 20;
/// Content held for sorting before writing starts anyway.
const HOLD_BYTES: usize = 64 << 20;
/// Fetched file content waiting for (or in) a writer before tree and blob
/// fetches pause.
const PENDING_BYTES: usize = 512 << 20;

/// Write batches in flight: file creation is kernel work, one thread per
/// core keeps them busy (more measured no faster).
fn writers() -> usize {
    std::thread::available_parallelism().map_or(8, |n| n.get()).clamp(4, 16)
}

/// Blob fetches (`get_many` batches) in flight.
fn fetchers(writers: usize) -> usize {
    (writers / 2).clamp(2, 8)
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
    Fetched(anyhow::Result<Vec<WriteItem>>),
    Written { bytes: usize, result: anyhow::Result<Written> },
}

/// Timings and counts of one pipelined materialization (debug log).
#[derive(Default, Debug)]
struct Stats {
    tree_fetches: usize,
    trees_fetched: usize,
    files_from_memory: usize,
    files_fetched: usize,
    blob_fetches: usize,
    write_batches: usize,
    walk_ms: u64,
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
    let max_fetches = fetchers(max_writers);
    let mut stats = Stats::default();
    let mut queue: VecDeque<(RepoPath, ObjectId)> = VecDeque::from([(RepoPath::root(), base_tree)]);
    // Content waiting for a writer; sorted by path up to `ready_sorted`.
    let mut ready: VecDeque<WriteItem> = VecDeque::new();
    let mut ready_sorted = 0usize;
    let mut ready_bytes = 0usize;
    // Leaves to fetch; sorted by path up to `need_sorted`.
    let mut need: VecDeque<Leaf> = VecDeque::new();
    let mut need_sorted = 0usize;
    let mut pending_bytes = 0usize;
    let (mut tree_calls, mut fetches, mut writes) = (0usize, 0usize, 0usize);
    let mut out: Written = Vec::new();
    let mut tasks: FuturesUnordered<BoxFuture<'_, Done>> = FuturesUnordered::new();
    loop {
        let room = pending_bytes < PENDING_BYTES || tasks.is_empty();
        // Start tree fetches: split what is queued over the free slots.
        while tree_calls < TREE_CALLS && !queue.is_empty() && room {
            let free = TREE_CALLS - tree_calls;
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
        let walked = tree_calls == 0 && queue.is_empty();
        if walked && stats.walk_ms == 0 {
            stats.walk_ms = start.elapsed().as_millis().max(1) as u64;
        }
        // Start blob fetches, in path order: once the walk is done (or too
        // many leaves wait).
        if (walked || need.len() >= HOLD_LEAVES) && need_sorted < need.len() {
            need.make_contiguous().sort_unstable_by(|a, b| a.0.cmp(&b.0));
            need_sorted = need.len();
        }
        while fetches < max_fetches
            && need_sorted > 0
            && (pending_bytes < PENDING_BYTES || tasks.is_empty())
        {
            let n = need_sorted.min(BATCH);
            let chunk: Vec<Leaf> = need.drain(..n).collect();
            need_sorted -= n;
            fetches += 1;
            stats.blob_fetches += 1;
            stats.files_fetched += chunk.len();
            tasks.push(Box::pin(async move { Done::Fetched(fetch_blobs(objects, chunk).await) }));
        }
        // Start writes, in path order: once the walk is done or enough
        // content is held; partial batches only when nothing more can come.
        let drained = walked && fetches == 0 && need.is_empty();
        if (walked || ready_bytes >= HOLD_BYTES) && ready_sorted < ready.len() {
            ready.make_contiguous().sort_unstable_by(|a, b| a.path.cmp(&b.path));
            ready_sorted = ready.len();
        }
        while writes < max_writers
            && ready_sorted > 0
            && (ready_sorted >= WRITE_ITEMS || ready_bytes >= WRITE_BYTES || drained)
        {
            let (mut n, mut bytes) = (0, 0);
            while n < ready_sorted && n < WRITE_ITEMS && bytes < WRITE_BYTES {
                bytes += ready[n].body.len();
                n += 1;
            }
            let items: Vec<WriteItem> = ready.drain(..n).collect();
            ready_sorted -= n;
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
            Done::Fetched(r) => {
                fetches -= 1;
                let items = r?;
                for it in &items {
                    ready_bytes += it.body.len();
                    pending_bytes += it.body.len();
                }
                ready.extend(items);
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
        writers = max_writers,
        "materialized"
    );
    Ok(out)
}

/// Fetch the blobs of `chunk` with one `get_many`.
async fn fetch_blobs(
    objects: &dyn ObjectStore,
    chunk: Vec<Leaf>,
) -> anyhow::Result<Vec<WriteItem>> {
    let ids: Vec<ObjectId> = chunk.iter().map(|(_, _, id)| *id).collect();
    let fetched = objects.get_many(&ids).await?;
    chunk
        .into_iter()
        .zip(fetched)
        .map(|((path, mode, id), obj)| {
            let obj = obj.ok_or_else(|| anyhow!("missing blob {id} for {path}"))?;
            let body = blob_body(&obj, &id)?;
            Ok(WriteItem { path, mode, id, body })
        })
        .collect()
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
