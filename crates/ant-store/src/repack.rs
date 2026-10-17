//! The repacking engine shared by `ant admin repack` on Postgres + S3
//! ([`crate::postgres::repack`], whose module docs describe a run) and on a
//! local data directory ([`crate::local::repack`]): the walk over history,
//! the selection of rows, and the packing into delta frames
//! ([`crate::packed::cluster`]). Where rows live, how a pack is stored and
//! how rows are switched to it is the [`Target`]'s business.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind};
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::{self, FuturesUnordered, StreamExt};

use crate::packed::cluster::{Builder, ClusterOptions, Record};
use crate::packed::frame;
use crate::{CommitNode, ObjectStore, Result, StoreError};

/// Default `min_age` of a repack.
pub const DEFAULT_MIN_AGE: Duration = Duration::from_secs(24 * 3600);
/// Default `keep_recent` of a repack.
pub const DEFAULT_KEEP_RECENT: usize = 100;
/// Default `pack_bytes` of a repack.
pub(crate) const PACK_BYTES: usize = 64 * 1024 * 1024;
/// Objects per segment (a unit of parallel work; segments end where a path
/// does, once this long).
pub(crate) const SEGMENT_OBJECTS: usize = 2048;
/// Uncompressed bytes fetched per read of a segment, roughly.
const FETCH_BYTES: usize = 16 * 1024 * 1024;
const FETCH_MIN: usize = 8;
const FETCH_MAX: usize = 512;
/// Ids per read while walking trees.
const WALK_BATCH: usize = 1000;
/// Commits whose trees are walked together.
const WALK_COMMITS: usize = 1024;
/// Reads in flight while walking trees.
const WALK_READS: usize = 8;
/// Uploads in flight beyond which no new segment is started (each holds a
/// pack in memory).
const UPLOADS_AHEAD: usize = 1;
/// Rows per page of the index and object scans.
pub(crate) const SCAN_PAGE: usize = 20_000;

/// The settings of a run every target shares.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Settings {
    pub dry_run: bool,
    pub min_age: Duration,
    pub keep_recent: usize,
    pub all: bool,
    pub small: bool,
    pub cluster: ClusterOptions,
    pub jobs: usize,
    pub batch: usize,
    pub stop_after_packs: Option<usize>,
    pub pack_bytes: usize,
}

/// What a repack did (dry run: would do).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepackReport {
    pub dry_run: bool,
    /// Every selected object was handled (false: stopped early, run again).
    pub finished: bool,
    /// Commits walked, and objects reached by the walk (commits included).
    pub commits: u64,
    pub reachable: u64,
    /// Objects of the hot set.
    pub hot: u64,
    /// Objects selected: from the packed index (cold reached, hot, not
    /// reached) and from the small-object table.
    pub selected_cold: u64,
    pub selected_hot: u64,
    pub selected_other: u64,
    pub selected_small: u64,
    /// Bytes the selected objects take now: their share of pack records,
    /// and their small-object rows' data (Postgres, or the local SQLite).
    pub old_s3_bytes: u64,
    pub old_pg_bytes: u64,
    /// Objects moved to the new packs (of them small objects), and selected
    /// objects not moved (gone, condemned, locked, or not readable).
    pub moved: u64,
    pub moved_small: u64,
    pub skipped: u64,
    /// Packs and records written, their bytes, delta members and the deepest
    /// chain.
    pub packs: u64,
    pub records: u64,
    pub new_bytes: u64,
    pub deltas: u64,
    pub max_depth: u32,
    /// Old packs deleted once no row pointed into them (local stores; on
    /// S3 `ant admin gc` deletes them): count, bytes.
    pub packs_deleted: u64,
    pub packs_deleted_bytes: u64,
}

impl RepackReport {
    /// Bytes saved (estimated for a dry run): what the selected objects took
    /// minus what the new packs take. Old packs' bytes are freed by the
    /// collector (local stores: by the repack itself) once nothing points
    /// into them, and database space is reused (or returned by vacuuming).
    pub fn saved_bytes(&self) -> i64 {
        (self.old_s3_bytes + self.old_pg_bytes) as i64 - self.new_bytes as i64
    }

    fn fields(&mut self) -> [(&'static str, &mut u64); 18] {
        [
            ("commits", &mut self.commits),
            ("reachable", &mut self.reachable),
            ("hot", &mut self.hot),
            ("selected_cold", &mut self.selected_cold),
            ("selected_hot", &mut self.selected_hot),
            ("selected_other", &mut self.selected_other),
            ("selected_small", &mut self.selected_small),
            ("old_s3_bytes", &mut self.old_s3_bytes),
            ("old_pg_bytes", &mut self.old_pg_bytes),
            ("moved", &mut self.moved),
            ("moved_small", &mut self.moved_small),
            ("skipped", &mut self.skipped),
            ("packs", &mut self.packs),
            ("records", &mut self.records),
            ("new_bytes", &mut self.new_bytes),
            ("deltas", &mut self.deltas),
            ("packs_deleted", &mut self.packs_deleted),
            ("packs_deleted_bytes", &mut self.packs_deleted_bytes),
        ]
    }

    /// `key=value` pairs (the local maintenance socket's report line).
    pub fn encode(&self) -> String {
        let mut me = self.clone();
        let mut out: Vec<String> =
            me.fields().into_iter().map(|(k, v)| format!("{k}={v}")).collect();
        out.push(format!("max_depth={}", self.max_depth));
        out.push(format!("dry_run={}", u8::from(self.dry_run)));
        out.push(format!("finished={}", u8::from(self.finished)));
        out.join(" ")
    }

    /// The report [`Self::encode`] wrote (unknown keys are ignored).
    pub fn decode(s: &str) -> Self {
        let values: HashMap<&str, &str> =
            s.split_whitespace().filter_map(|kv| kv.split_once('=')).collect();
        let mut r = Self {
            dry_run: values.get("dry_run") == Some(&"1"),
            finished: values.get("finished") == Some(&"1"),
            max_depth: values.get("max_depth").and_then(|v| v.parse().ok()).unwrap_or(0),
            ..Self::default()
        };
        for (k, v) in r.fields() {
            if let Some(x) = values.get(k).and_then(|x| x.parse().ok()) {
                *v = x;
            }
        }
        r
    }
}

/// Where a selected object is now.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Source {
    /// A packed-index row pointing at `(pack, off)`.
    Index { pack: [u8; 16], off: u64 },
    /// A small-object row.
    Small,
}

/// A packed-object index row, as a scan reports it.
pub(crate) struct IndexRow {
    pub id: ObjectId,
    pub pack: [u8; 16],
    pub off: u64,
    /// The object's share of its record.
    pub bytes: u64,
    /// Written before the cutoff.
    pub old: bool,
    /// Condemned by a running collection.
    pub condemned: bool,
    /// 1: in a delta pack of an earlier repack; 2: in one of its hot packs;
    /// 0: neither.
    pub repacked: u8,
}

/// A small-object row, as a scan reports it.
pub(crate) struct SmallRow {
    pub id: ObjectId,
    pub bytes: u64,
    pub old: bool,
    pub condemned: bool,
}

/// One object of a new pack: id, offset, record length, share of the
/// record (`None`: the whole record).
pub(crate) type Entry = (ObjectId, u64, u32, Option<u32>);

/// Outcome of switching one pack's rows.
#[derive(Default)]
pub(crate) struct Switched {
    pub moved: u64,
    pub moved_small: u64,
    pub skipped: u64,
}

/// Where a repack reads its rows and puts its packs.
#[async_trait]
pub(crate) trait Target: Clone + Send + Sync + 'static {
    async fn list_refs(&self) -> Result<Vec<(String, ObjectId)>>;
    async fn get_commits(&self, ids: &[ObjectId]) -> Result<Vec<Option<CommitNode>>>;
    /// Rows written before this time (Unix seconds) may be moved.
    async fn cutoff(&self, min_age: Duration) -> Result<f64>;
    /// Up to `limit` packed-index rows with ids after `after` (empty: from
    /// the start), in id order.
    async fn scan_index(&self, after: &[u8], cutoff: f64, limit: usize) -> Result<Vec<IndexRow>>;
    /// The same for small-object rows.
    async fn scan_small(&self, after: &[u8], cutoff: f64, limit: usize) -> Result<Vec<SmallRow>>;
    /// Store `body` as a new pack (durably), then point the rows of
    /// `entries` at it, `batch` ids per transaction: index rows only if they
    /// still point where `sources` says, small objects only by deleting
    /// their row in the same transaction; both keep the write time.
    async fn store_pack(
        &self,
        hot: bool,
        body: Vec<u8>,
        entries: Vec<Entry>,
        sources: Arc<HashMap<ObjectId, Source>>,
        batch: usize,
    ) -> Result<Switched>;
}

/// What the walk learned about a reachable object.
#[derive(Clone, Copy)]
struct Meta {
    /// 0 tree, 1 blob, 2 commit (sort order: trees, then blobs, then commits).
    kind: u8,
    path: u32,
    /// Rank of the newest commit reaching it (0 = newest).
    rank: u32,
}

const KIND_TREE: u8 = 0;
const KIND_BLOB: u8 = 1;
const KIND_COMMIT: u8 = 2;

/// How a segment's objects are packed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Reached by the walk, ordered already: delta frames.
    Cold,
    /// Not reached: ordered by similarity once read, then delta frames.
    Other,
    /// Hot: plain frames.
    Hot,
}

struct Segment {
    mode: Mode,
    ids: Vec<ObjectId>,
}

/// Records of one segment, and its selected objects that could not be read.
struct Packed {
    records: Vec<Record>,
    missing: u64,
    /// Hot objects' plain frames.
    hot: bool,
}

/// Paths seen by the walk, interned.
#[derive(Default)]
pub(crate) struct Paths {
    ids: HashMap<Vec<u8>, u32>,
    pub(crate) names: Vec<Vec<u8>>,
}

impl Paths {
    pub(crate) fn intern(&mut self, path: Vec<u8>) -> u32 {
        if let Some(&id) = self.ids.get(&path) {
            return id;
        }
        let id = self.names.len() as u32;
        self.names.push(path.clone());
        self.ids.insert(path, id);
        id
    }

    pub(crate) fn child(&mut self, parent: u32, name: &[u8]) -> u32 {
        let parent = &self.names[parent as usize];
        let mut path = Vec::with_capacity(parent.len() + 1 + name.len());
        if !parent.is_empty() {
            path.extend_from_slice(parent);
            path.push(b'/');
        }
        path.extend_from_slice(name);
        self.intern(path)
    }

    /// Position of every path in byte order (so one directory's files sort
    /// together).
    pub(crate) fn order(&self) -> Vec<u32> {
        let mut by_name: Vec<u32> = (0..self.names.len() as u32).collect();
        by_name.sort_by(|a, b| self.names[*a as usize].cmp(&self.names[*b as usize]));
        let mut order = vec![0u32; by_name.len()];
        for (pos, id) in by_name.into_iter().enumerate() {
            order[id as usize] = pos as u32;
        }
        order
    }
}

/// Entries of an encoded tree body: `(is_tree, name, id)`, gitlinks skipped.
/// Lenient like the collector's parser: whatever is stored is walked.
pub(crate) fn tree_entries(body: &[u8]) -> Result<Vec<(bool, &[u8], ObjectId)>> {
    let malformed = || StoreError::InvalidArgument("repack: malformed tree object".into());
    let mut out = Vec::new();
    let mut rest = body;
    while !rest.is_empty() {
        let sp = rest.iter().position(|&b| b == b' ').ok_or_else(malformed)?;
        let mode = &rest[..sp];
        rest = &rest[sp + 1..];
        let nul = rest.iter().position(|&b| b == 0).ok_or_else(malformed)?;
        let name = &rest[..nul];
        rest = &rest[nul + 1..];
        if rest.len() < ObjectId::LEN {
            return Err(malformed());
        }
        let id = ObjectId::from_bytes(&rest[..ObjectId::LEN])?;
        rest = &rest[ObjectId::LEN..];
        match EntryMode::from_octal(mode).map(EntryMode::class) {
            Ok(EntryMode::Tree) => out.push((true, name, id)),
            Ok(EntryMode::Gitlink) => {}
            _ => out.push((false, name, id)),
        }
    }
    Ok(out)
}

/// State of one repack.
struct Run<'a, T: Target> {
    target: &'a T,
    objects: &'a dyn ObjectStore,
    opts: Settings,
    progress: &'a (dyn Fn(&str) + Send + Sync),
    report: RepackReport,
    meta: HashMap<ObjectId, Meta>,
    hot: HashSet<ObjectId>,
    paths: Paths,
    /// Where each selected object is now.
    sources: HashMap<ObjectId, Source>,
}

/// Repack `target` (see [`crate::postgres::repack`]): `objects` is the
/// repository's object store, used for every read. The caller makes sure
/// that one repack runs at a time.
pub(crate) async fn run<T: Target>(
    target: &T,
    objects: &dyn ObjectStore,
    opts: Settings,
    progress: &(dyn Fn(&str) + Send + Sync),
) -> Result<RepackReport> {
    let mut run = Run {
        target,
        objects,
        opts,
        progress,
        report: RepackReport { dry_run: opts.dry_run, ..RepackReport::default() },
        meta: HashMap::new(),
        hot: HashSet::new(),
        paths: Paths::default(),
        sources: HashMap::new(),
    };
    run.run().await?;
    Ok(run.report)
}

impl<T: Target> Run<'_, T> {
    fn say(&self, line: &str) {
        (self.progress)(line);
    }

    async fn run(&mut self) -> Result<()> {
        let started = Instant::now();
        let commits = self.commits().await?;
        self.say(&format!("walking {} commits", commits.len()));
        self.hot_set(&commits).await?;
        self.walk(&commits).await?;
        self.report.reachable = self.meta.len() as u64;
        self.say(&format!(
            "reached {} objects ({} hot) in {:.1}s",
            self.report.reachable,
            self.report.hot,
            started.elapsed().as_secs_f64()
        ));
        let segments = self.select().await?;
        let r = &self.report;
        self.say(&format!(
            "selected {} cold, {} hot and {} other packed objects ({:.1} MiB), {} small \
             objects ({:.1} MiB)",
            r.selected_cold,
            r.selected_hot,
            r.selected_other,
            mib(r.old_s3_bytes),
            r.selected_small,
            mib(r.old_pg_bytes)
        ));
        // The walk's maps are not needed any more.
        self.meta = HashMap::new();
        self.hot = HashSet::new();
        self.paths = Paths::default();
        self.pack(segments).await
    }

    /// Commits reachable from the refs, newest first (generation, then
    /// committer time).
    async fn commits(&mut self) -> Result<Vec<CommitNode>> {
        let refs = self.target.list_refs().await?;
        let mut seen: HashSet<ObjectId> = HashSet::new();
        let mut frontier: Vec<ObjectId> = refs.iter().map(|(_, id)| *id).collect();
        let mut all = Vec::new();
        while !frontier.is_empty() {
            frontier.retain(|id| seen.insert(*id));
            let mut next = Vec::new();
            for chunk in frontier.chunks(WALK_BATCH) {
                for node in self.target.get_commits(chunk).await?.into_iter().flatten() {
                    next.extend(node.parents.iter().copied());
                    all.push(node);
                }
            }
            frontier = next;
        }
        all.sort_by(|a, b| {
            b.generation
                .cmp(&a.generation)
                .then(b.committer_time.cmp(&a.committer_time))
                .then(a.id.cmp(&b.id))
        });
        self.report.commits = all.len() as u64;
        Ok(all)
    }

    /// Everything reachable from the newest `keep_recent` first-parent
    /// commits of every ref.
    async fn hot_set(&mut self, commits: &[CommitNode]) -> Result<()> {
        let nodes: HashMap<ObjectId, &CommitNode> = commits.iter().map(|c| (c.id, c)).collect();
        let mut trees = Vec::new();
        for (_, tip) in self.target.list_refs().await? {
            let mut cur = Some(tip);
            for _ in 0..self.opts.keep_recent {
                let Some(node) = cur.and_then(|id| nodes.get(&id)) else { break };
                if self.hot.insert(node.id) {
                    trees.push(node.tree);
                }
                cur = node.parents.first().copied();
            }
        }
        let mut frontier: Vec<ObjectId> =
            trees.into_iter().filter(|t| self.hot.insert(*t)).collect();
        while !frontier.is_empty() {
            let mut next = Vec::new();
            for chunk in frontier.chunks(WALK_BATCH) {
                for o in self.objects.get_many(chunk).await?.into_iter().flatten() {
                    for (is_tree, _, id) in tree_entries(o.body())? {
                        if self.hot.insert(id) && is_tree {
                            next.push(id);
                        }
                    }
                }
            }
            frontier = next;
        }
        self.report.hot = self.hot.len() as u64;
        Ok(())
    }

    /// Path and recency of every object reachable from `commits` (newest
    /// first) outside the hot set.
    async fn walk(&mut self, commits: &[CommitNode]) -> Result<()> {
        let root = self.paths.intern(Vec::new());
        for (rank, c) in commits.iter().enumerate() {
            if !self.hot.contains(&c.id) {
                self.meta.insert(c.id, Meta { kind: KIND_COMMIT, path: root, rank: rank as u32 });
            }
        }
        for (n, chunk) in commits.chunks(WALK_COMMITS).enumerate() {
            let mut frontier: Vec<(ObjectId, u32, u32)> = Vec::new();
            for (i, c) in chunk.iter().enumerate() {
                let rank = (n * WALK_COMMITS + i) as u32;
                if !self.hot.contains(&c.tree) && !self.meta.contains_key(&c.tree) {
                    self.meta.insert(c.tree, Meta { kind: KIND_TREE, path: root, rank });
                    frontier.push((c.tree, root, rank));
                }
            }
            while !frontier.is_empty() {
                let mut next = Vec::new();
                let objects = self.objects;
                // Owned parts: the future stays `Send` (no borrowed closure
                // arguments).
                let parts: Vec<Vec<(ObjectId, u32, u32)>> =
                    frontier.chunks(WALK_BATCH).map(<[_]>::to_vec).collect();
                let mut reads = stream::iter(parts)
                    .map(|part| async move {
                        let ids: Vec<ObjectId> = part.iter().map(|t| t.0).collect();
                        let got = objects.get_many(&ids).await;
                        got.map(|got| (part, got))
                    })
                    .buffered(WALK_READS);
                while let Some(read) = reads.next().await {
                    let (part, got) = read?;
                    for (&(_, path, rank), o) in part.iter().zip(got) {
                        let Some(o) = o else { continue };
                        for (is_tree, name, id) in tree_entries(o.body())? {
                            if self.hot.contains(&id) || self.meta.contains_key(&id) {
                                continue;
                            }
                            let child = self.paths.child(path, name);
                            let kind = if is_tree { KIND_TREE } else { KIND_BLOB };
                            self.meta.insert(id, Meta { kind, path: child, rank });
                            if is_tree {
                                next.push((id, child, rank));
                            }
                        }
                    }
                }
                drop(reads);
                frontier = next;
            }
        }
        Ok(())
    }

    /// Scan the packed index and the small objects for rows to move;
    /// returns the work, in segments.
    async fn select(&mut self) -> Result<Vec<Segment>> {
        let cutoff = self.target.cutoff(self.opts.min_age).await?;
        let mut cold: Vec<(ObjectId, Meta)> = Vec::new();
        let mut other: Vec<ObjectId> = Vec::new();
        let mut hot: Vec<ObjectId> = Vec::new();
        let mut last: Vec<u8> = Vec::new();
        loop {
            let rows = self.target.scan_index(&last, cutoff, SCAN_PAGE).await?;
            let Some(tail) = rows.last() else { break };
            last = tail.id.as_bytes().to_vec();
            for r in &rows {
                // In a delta pack of an earlier repack; in one of its hot
                // packs (left alone while the object is still hot).
                let done = match r.repacked {
                    1 => true,
                    2 => self.hot.contains(&r.id),
                    _ => false,
                };
                if !r.old || r.condemned || (done && !self.opts.all) {
                    continue;
                }
                self.sources.insert(r.id, Source::Index { pack: r.pack, off: r.off });
                self.report.old_s3_bytes += r.bytes;
                if self.hot.contains(&r.id) {
                    hot.push(r.id);
                } else if let Some(m) = self.meta.get(&r.id) {
                    cold.push((r.id, *m));
                } else {
                    other.push(r.id);
                }
            }
        }
        if self.opts.small {
            let mut last: Vec<u8> = Vec::new();
            loop {
                let rows = self.target.scan_small(&last, cutoff, SCAN_PAGE).await?;
                let Some(tail) = rows.last() else { break };
                last = tail.id.as_bytes().to_vec();
                for r in &rows {
                    // Only cold objects the walk reached; an id with an index
                    // row as well is moved through that.
                    let Some(m) = self.meta.get(&r.id) else { continue };
                    if !r.old || r.condemned || self.sources.contains_key(&r.id) {
                        continue;
                    }
                    self.sources.insert(r.id, Source::Small);
                    self.report.old_pg_bytes += r.bytes;
                    self.report.selected_small += 1;
                    cold.push((r.id, *m));
                }
            }
        }
        self.report.selected_cold = cold.len() as u64 - self.report.selected_small;
        self.report.selected_hot = hot.len() as u64;
        self.report.selected_other = other.len() as u64;

        // Trees, then blobs, then commits; by path (one directory's entries
        // together), newest first.
        let order = self.paths.order();
        cold.sort_by_key(|(id, m)| (m.kind, order[m.path as usize], m.rank, *id));
        let mut segments = Vec::new();
        let mut cur: Vec<ObjectId> = Vec::new();
        let mut prev: Option<(u8, u32)> = None;
        for (id, m) in cold {
            let group = (m.kind, m.path);
            // Segments end with a path, unless one path's history alone
            // would keep a single job busy (commits, a root directory): a
            // cut costs one more whole object.
            if (cur.len() >= SEGMENT_OBJECTS && prev != Some(group))
                || cur.len() >= SEGMENT_OBJECTS * 4
            {
                segments.push(Segment { mode: Mode::Cold, ids: std::mem::take(&mut cur) });
            }
            prev = Some(group);
            cur.push(id);
        }
        if !cur.is_empty() {
            segments.push(Segment { mode: Mode::Cold, ids: cur });
        }
        for ids in other.chunks(SEGMENT_OBJECTS / 2) {
            segments.push(Segment { mode: Mode::Other, ids: ids.to_vec() });
        }
        for ids in hot.chunks(SEGMENT_OBJECTS / 2) {
            segments.push(Segment { mode: Mode::Hot, ids: ids.to_vec() });
        }
        Ok(segments)
    }

    /// Read, compress and write the segments; switch rows pack by pack.
    ///
    /// Segments being read are polled all the time, also while uploads are
    /// awaited: a read that is not polled keeps whatever it holds, such as
    /// S3 request slots (`S3Timeouts::max_concurrency`, shared with the
    /// uploads), and uploads waiting for them would never finish. So
    /// backpressure stops *starting* segments (at most `jobs` at once, none
    /// while more than [`UPLOADS_AHEAD`] uploads are in flight); it never
    /// pauses one that runs.
    async fn pack(&mut self, segments: Vec<Segment>) -> Result<()> {
        let total = segments.len();
        let started = Instant::now();
        let sources = Arc::new(std::mem::take(&mut self.sources));
        let (objects, opts) = (self.objects, self.opts);
        let jobs = opts.jobs.max(1);
        let mut pending = segments.into_iter();
        let mut running = FuturesUnordered::new();
        // Uploads are futures of this run, not tasks: a run that is dropped
        // (cancelled) drops them with it, so nothing of it outlives it.
        let mut uploads: FuturesUnordered<BoxFuture<'static, Result<Switched>>> =
            FuturesUnordered::new();
        // Delta frames and hot objects' plain frames go to separate packs.
        let mut bufs = [PackBuf::default(), PackBuf { hot: true, ..PackBuf::default() }];
        let mut done = 0usize;
        let mut stopped = false;
        let mut failed: Option<StoreError> = None;
        loop {
            if !stopped && failed.is_none() {
                while running.len() < jobs && uploads.len() <= UPLOADS_AHEAD {
                    let Some(s) = pending.next() else { break };
                    running.push(pack_segment(objects, opts.cluster, s));
                }
            }
            tokio::select! {
                Some(out) = running.next(), if !running.is_empty() => {
                    let out = match out {
                        Ok(out) => out,
                        Err(e) => {
                            // Let the uploads in flight finish (switching
                            // their rows) before giving up: nothing outlives
                            // the run.
                            failed = failed.or(Some(e));
                            running.clear();
                            continue;
                        }
                    };
                    done += 1;
                    self.report.skipped += out.missing;
                    let buf = &mut bufs[usize::from(out.hot)];
                    for record in out.records {
                        self.report.records += 1;
                        self.report.deltas += record.deltas as u64;
                        self.report.max_depth = self.report.max_depth.max(record.max_depth);
                        buf.add(record)?;
                        if buf.body.len() >= self.opts.pack_bytes.max(1) {
                            uploads.push(self.flush(buf, sources.clone()));
                        }
                    }
                    if done.is_multiple_of(16) || done == total {
                        self.say(&format!(
                            "packed {done}/{total} segments: {} packs, {:.1} MiB, {} deltas \
                             ({:.0}s)",
                            self.report.packs,
                            mib(self.report.new_bytes),
                            self.report.deltas,
                            started.elapsed().as_secs_f64()
                        ));
                    }
                    if self.opts.stop_after_packs.is_some_and(|n| self.report.packs as usize >= n)
                    {
                        stopped = true;
                        running.clear();
                    }
                }
                Some(up) = uploads.next(), if !uploads.is_empty() => {
                    match up {
                        Ok(s) => self.tally(s),
                        Err(e) => {
                            failed = failed.or(Some(e));
                            running.clear();
                        }
                    }
                }
                else => break,
            }
        }
        if !stopped && failed.is_none() {
            for buf in &mut bufs {
                if !buf.body.is_empty() {
                    uploads.push(self.flush(buf, sources.clone()));
                }
            }
        }
        while let Some(up) = uploads.next().await {
            match up {
                Ok(s) => self.tally(s),
                Err(e) => failed = failed.or(Some(e)),
            }
        }
        if let Some(e) = failed {
            return Err(e);
        }
        self.report.finished = !stopped;
        Ok(())
    }

    fn tally(&mut self, s: Switched) {
        self.report.moved += s.moved;
        self.report.moved_small += s.moved_small;
        self.report.skipped += s.skipped;
    }

    /// Start storing and switching the current pack (dry run: count it).
    fn flush(
        &mut self,
        buf: &mut PackBuf,
        sources: Arc<HashMap<ObjectId, Source>>,
    ) -> BoxFuture<'static, Result<Switched>> {
        let hot = buf.hot;
        let body = std::mem::take(&mut buf.body);
        let entries = std::mem::take(&mut buf.entries);
        self.report.packs += 1;
        self.report.new_bytes += body.len() as u64;
        let (target, dry_run, batch) = (self.target.clone(), self.opts.dry_run, self.opts.batch);
        Box::pin(async move {
            if dry_run {
                return Ok(Switched { moved: entries.len() as u64, ..Switched::default() });
            }
            target.store_pack(hot, body, entries, sources, batch).await
        })
    }
}

fn mib(b: u64) -> f64 {
    b as f64 / (1024.0 * 1024.0)
}

/// A pack being assembled: its body and its entries; `hot`: it holds hot
/// objects' plain frames.
#[derive(Default)]
struct PackBuf {
    body: Vec<u8>,
    entries: Vec<Entry>,
    hot: bool,
}

impl PackBuf {
    fn add(&mut self, record: Record) -> Result<()> {
        let off = self.body.len() as u64;
        let len = u32::try_from(record.bytes.len()).map_err(|_| {
            StoreError::InvalidArgument(format!("pack record of {} bytes", record.bytes.len()))
        })?;
        if let [(id, _)] = record.members.as_slice() {
            self.entries.push((*id, off, len, None));
        } else {
            // Each member's share of the record, by the bytes it takes in it.
            let raw = record.members.iter().map(|(_, n)| *n as u64).sum::<u64>().max(1);
            for (id, n) in &record.members {
                let share = (u64::from(len) * *n as u64).div_ceil(raw).clamp(1, len.into());
                self.entries.push((*id, off, len, Some(share as u32)));
            }
        }
        self.body.extend_from_slice(&record.bytes);
        Ok(())
    }
}

/// Read the objects of one segment and compress them into records.
async fn pack_segment(
    objects: &dyn ObjectStore,
    cluster: ClusterOptions,
    seg: Segment,
) -> Result<Packed> {
    let mut missing = 0u64;
    let mut records = Vec::new();
    if seg.mode != Mode::Cold {
        // Small segments, read at once, ordered once read.
        let got = objects.get_many(&seg.ids).await?;
        let mut found: Vec<(ObjectId, EncodedObject)> = Vec::with_capacity(got.len());
        for (id, o) in seg.ids.iter().zip(got) {
            match o {
                Some(o) => found.push((*id, o)),
                None => missing += 1,
            }
        }
        let mode = seg.mode;
        let records = tokio::task::spawn_blocking(move || -> Result<Vec<Record>> {
            if mode == Mode::Hot {
                return plain_records(found);
            }
            let rank = |k: ObjectKind| match k {
                ObjectKind::Tree => 0u8,
                ObjectKind::Blob => 1,
                ObjectKind::Commit => 2,
            };
            let mut keyed: Vec<(u8, u64, ObjectId, EncodedObject)> = found
                .into_iter()
                .map(|(id, o)| (rank(o.kind()), frame::similarity_key(&o), id, o))
                .collect();
            keyed.sort_by_key(|k| (k.0, k.1, k.2));
            let mut b = Builder::new(cluster);
            let mut out = Vec::new();
            for (_, _, id, o) in keyed {
                out.extend(b.push(id, o)?);
            }
            out.extend(b.finish()?);
            Ok(out)
        })
        .await
        .map_err(StoreError::backend)??;
        return Ok(Packed { records, missing, hot: mode == Mode::Hot });
    }
    let mut builder = Builder::new(cluster);
    let mut at = 0usize;
    let mut fetch = FETCH_MIN;
    while at < seg.ids.len() {
        let ids: Vec<ObjectId> = seg.ids[at..(at + fetch).min(seg.ids.len())].to_vec();
        at += ids.len();
        let got = objects.get_many(&ids).await?;
        let bytes: usize = got.iter().flatten().map(|o| o.as_bytes().len()).sum();
        let avg = (bytes / ids.len().max(1)).max(1);
        fetch = (FETCH_BYTES / avg).clamp(FETCH_MIN, FETCH_MAX);
        let mut b = builder;
        let (b, out, gone) = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut out = Vec::new();
            let mut gone = 0u64;
            for (id, o) in ids.into_iter().zip(got) {
                match o {
                    Some(o) => out.extend(b.push(id, o)?),
                    None => gone += 1,
                }
            }
            Ok((b, out, gone))
        })
        .await
        .map_err(StoreError::backend)??;
        builder = b;
        records.extend(out);
        missing += gone;
    }
    let last = tokio::task::spawn_blocking(move || builder.finish())
        .await
        .map_err(StoreError::backend)??;
    records.extend(last);
    Ok(Packed { records, missing, hot: false })
}

/// Plain frames of hot objects, grouped like a write batch.
fn plain_records(found: Vec<(ObjectId, EncodedObject)>) -> Result<Vec<Record>> {
    let shape: Vec<(ObjectKind, u64, usize)> = found
        .iter()
        .map(|(_, o)| (o.kind(), frame::similarity_key(o), o.as_bytes().len()))
        .collect();
    let mut out = Vec::new();
    for record in frame::plan(&shape, frame::FRAME_RAW) {
        let members: Vec<&EncodedObject> = record.iter().map(|&i| &found[i].1).collect();
        let ids: Vec<ObjectId> = record.iter().map(|&i| found[i].0).collect();
        let bytes = frame::encode(&members, &ids)?;
        out.push(Record {
            bytes,
            members: record.iter().map(|&i| (found[i].0, found[i].1.as_bytes().len())).collect(),
            deltas: 0,
            max_depth: 0,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_sort_directories_together() {
        let mut p = Paths::default();
        let root = p.intern(Vec::new());
        let src = p.child(root, b"src");
        let lib = p.child(src, b"lib.rs");
        let readme = p.child(root, b"README");
        let main = p.child(src, b"main.rs");
        assert_eq!(p.names[main as usize], b"src/main.rs");
        let order = p.order();
        let mut by: Vec<u32> = vec![root, src, lib, readme, main];
        by.sort_by_key(|i| order[*i as usize]);
        assert_eq!(by, vec![root, readme, src, lib, main]);
        assert_eq!(p.child(src, b"lib.rs"), lib, "interned");
    }

    #[test]
    fn tree_entries_skip_gitlinks_and_reject_garbage() {
        use ant_core::{Object, Tree, TreeEntry};
        let t = Tree::new(vec![
            TreeEntry { mode: EntryMode::File, name: "a".into(), id: ObjectId([1; 32]) },
            TreeEntry { mode: EntryMode::Tree, name: "d".into(), id: ObjectId([2; 32]) },
            TreeEntry { mode: EntryMode::Gitlink, name: "m".into(), id: ObjectId([3; 32]) },
        ])
        .unwrap();
        let o = Object::Tree(t).encode();
        let e = tree_entries(o.body()).unwrap();
        assert_eq!(
            e,
            vec![
                (false, b"a".as_slice(), ObjectId([1; 32])),
                (true, b"d".as_slice(), ObjectId([2; 32]))
            ]
        );
        assert!(tree_entries(b"100644 a\0short").is_err());
        assert!(tree_entries(b"100644 a").is_err());
    }

    #[test]
    fn reports_round_trip_through_their_text_form() {
        let r = RepackReport {
            dry_run: false,
            finished: true,
            commits: 3,
            moved: 10,
            new_bytes: 12345,
            max_depth: 7,
            packs_deleted: 2,
            ..RepackReport::default()
        };
        assert_eq!(RepackReport::decode(&r.encode()), r);
    }
}
