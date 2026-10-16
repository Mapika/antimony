//! Repacking (`ant admin repack`): rewrite stored objects git-`gc`-style,
//! with deltas chosen across the whole history.
//!
//! Writers store each batch on its own: an import batch or a land puts the
//! large objects it writes into fresh S3 packs (look-alikes of that batch
//! compressed together, `crate::packed`), and small ones into Postgres rows
//! compressed one by one. Versions of one file written by different batches
//! never share anything. A repack collects them:
//!
//! 1. **Walk.** Every commit reachable from a ref, newest first, and every
//!    tree and blob reachable from them, each with the path it is first seen
//!    at (from the newest commit) and that commit's rank. The *hot* set is
//!    everything reachable from the newest [`RepackOptions::keep_recent`]
//!    first-parent commits of every ref (their trees in full).
//! 2. **Select.** Rows written before the cutoff (now minus
//!    [`RepackOptions::min_age`]) that are not condemned by a collection run
//!    and (unless [`RepackOptions::all`]) not already where a repack put
//!    them (a delta pack, or a hot pack while the object is still hot):
//!    packed-object index rows, and Postgres-resident (`objects`) rows of
//!    cold objects the walk reached.
//! 3. **Pack.** Cold objects, ordered by kind, path and recency (newest
//!    first), become delta frames (`packed::cluster`): each object a delta
//!    against the best of the previous [`ClusterOptions::window`] objects of
//!    its record, chains at most [`ClusterOptions::depth`] deep, records cut
//!    at [`ClusterOptions::cluster_raw`] bytes and compressed whole. Hot
//!    objects that live in S3 are rewritten as plain frames (one decode, no
//!    chains, as writers store them): checking out a recent revision costs
//!    what it did. Packed objects the walk did not reach (not reachable from
//!    a ref: kept by other roots, or garbage not collected yet) are grouped
//!    by similarity instead. Every record is decoded and compared with its
//!    input before it is uploaded.
//! 4. **Switch.** After a pack is uploaded, the index rows of its objects
//!    are moved to it in short transactions, and Postgres objects are moved
//!    by deleting their `objects` row and inserting an index row in the same
//!    transaction. Readers find every object at all times: before the switch
//!    commits at the old location, after it at the new one (a reader that
//!    looked up the old location keeps finding it: garbage collection keeps
//!    the old pack for its quiesce period after the move, `packs.moved_at`).
//!
//! Old packs are freed by `ant admin gc` once no index row points into them.
//! The new packs are recorded in `packs` (`repacked_at`; `hot` for the
//! packs of hot objects, kept apart from delta packs), so a later run skips
//! their objects (until a hot one goes cold) and continues where an
//! interrupted one stopped: an interrupted run leaves at most unindexed
//! packs, which the collector deletes after its grace period.
//!
//! # Safety with garbage collection and writers
//!
//! A move keeps the row's write time (`written_at`), so it neither makes
//! old data look young (which would let it outlive a collection that is
//! deleting its children) nor young data old. It skips ids condemned by a
//! running collection and rows locked by anyone (`FOR UPDATE SKIP LOCKED`:
//! a writer refreshing the row, or a sweep deleting it), and a Postgres
//! object is moved only if deleting its `objects` row succeeds in the same
//! transaction: an object the sweep deletes is never re-created in the
//! index. The packed index switch requires the row to still point where the
//! repack read the object from. A delta base lives in the same record as
//! the objects built on it, so the collector deleting the base's index row
//! leaves them readable, and the pack stays while any row points into it.
//! See `super::gc` for the collector's side.
//!
//! Memory: about 100 bytes per object in the repository (the walk), plus
//! per job a fetch batch, a delta window and a record.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind};
use futures::stream::{self, FuturesUnordered, StreamExt};
use sqlx::Row;

use super::{PgStore, be, object_id_col};
use crate::packed::cluster::{Builder, ClusterOptions, Record};
use crate::packed::frame;
use crate::s3::S3ObjectStore;
use crate::{MetaStore, ObjectStore, RefStore, Result, StoreError};

/// Default [`RepackOptions::min_age`].
pub const DEFAULT_MIN_AGE: Duration = Duration::from_secs(24 * 3600);
/// Default [`RepackOptions::keep_recent`].
pub const DEFAULT_KEEP_RECENT: usize = 100;
/// Default [`RepackOptions::pack_bytes`].
const PACK_BYTES: usize = 64 * 1024 * 1024;
/// Objects per segment (a unit of parallel work; segments end where a path
/// does, once this long).
const SEGMENT_OBJECTS: usize = 2048;
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
const SCAN_PAGE: i64 = 20_000;

/// Settings of one repack.
#[derive(Clone)]
pub struct RepackOptions {
    /// Read, compress and count only; write nothing.
    pub dry_run: bool,
    /// Rows written less than this long ago are left as they are.
    pub min_age: Duration,
    /// First-parent commits per ref whose trees stay one decode away (hot):
    /// their objects are not delta-compressed.
    pub keep_recent: usize,
    /// Also rewrite objects already in packs a repack wrote (a full
    /// repack, like `git repack -a -d -f`): merges the deltas of several
    /// incremental runs and drops their dead bytes.
    pub all: bool,
    /// Move cold Postgres-resident objects into packs too.
    pub small: bool,
    pub cluster: ClusterOptions,
    /// Segments packed at once (CPU threads used).
    pub jobs: usize,
    /// Ids per switch transaction.
    pub batch: usize,
    /// Stop (resumably) after this many packs.
    pub stop_after_packs: Option<usize>,
    /// Target size of one pack (compressed bytes).
    pub pack_bytes: usize,
    /// Where the packs go (the store's S3 side).
    pub s3: S3ObjectStore,
}

impl RepackOptions {
    pub fn new(s3: S3ObjectStore) -> Self {
        Self {
            dry_run: false,
            min_age: DEFAULT_MIN_AGE,
            keep_recent: DEFAULT_KEEP_RECENT,
            all: false,
            small: true,
            cluster: ClusterOptions::default(),
            jobs: std::thread::available_parallelism().map_or(4, |n| n.get()).min(8),
            batch: 1000,
            stop_after_packs: None,
            pack_bytes: PACK_BYTES,
            s3,
        }
    }
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
    /// reached) and from Postgres.
    pub selected_cold: u64,
    pub selected_hot: u64,
    pub selected_other: u64,
    pub selected_small: u64,
    /// Bytes the selected objects take now: their share of S3 records, and
    /// their Postgres rows' data.
    pub old_s3_bytes: u64,
    pub old_pg_bytes: u64,
    /// Objects moved to the new packs (of them from Postgres), and selected
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
}

impl RepackReport {
    /// Bytes saved (estimated for a dry run): what the selected objects took
    /// minus what the new packs take. Old packs' bytes are freed by the
    /// collector once nothing points into them, and Postgres space is reused
    /// (or returned by `VACUUM FULL`).
    pub fn saved_bytes(&self) -> i64 {
        (self.old_s3_bytes + self.old_pg_bytes) as i64 - self.new_bytes as i64
    }
}

/// Advisory-lock key held by the one repack of a schema.
fn run_key(schema: &str) -> String {
    format!("ant-repack/{schema}")
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

/// Where a selected object is now.
#[derive(Clone, Copy)]
enum Source {
    /// A packed-index row pointing at `(pack, off)`.
    Index { pack: [u8; 16], off: u64 },
    /// An `objects` row.
    Small,
}

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
struct Paths {
    ids: HashMap<Vec<u8>, u32>,
    names: Vec<Vec<u8>>,
}

impl Paths {
    fn intern(&mut self, path: Vec<u8>) -> u32 {
        if let Some(&id) = self.ids.get(&path) {
            return id;
        }
        let id = self.names.len() as u32;
        self.names.push(path.clone());
        self.ids.insert(path, id);
        id
    }

    fn child(&mut self, parent: u32, name: &[u8]) -> u32 {
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
    fn order(&self) -> Vec<u32> {
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
fn tree_entries(body: &[u8]) -> Result<Vec<(bool, &[u8], ObjectId)>> {
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
struct Run<'a> {
    store: &'a PgStore,
    objects: &'a dyn ObjectStore,
    opts: &'a RepackOptions,
    progress: &'a (dyn Fn(&str) + Send + Sync),
    report: RepackReport,
    meta: HashMap<ObjectId, Meta>,
    hot: HashSet<ObjectId>,
    paths: Paths,
    /// Where each selected object is now.
    sources: HashMap<ObjectId, Source>,
}

impl PgStore {
    /// Repack (see the module docs). `objects` is the repository's object
    /// store (tiered: Postgres and S3 packs), used for every read; packs go
    /// to `opts.s3`. `progress` receives a line per step.
    pub async fn repack(
        &self,
        objects: &dyn ObjectStore,
        opts: &RepackOptions,
        progress: &(dyn Fn(&str) + Send + Sync),
    ) -> Result<RepackReport> {
        let mut conn = self.pool.acquire().await.map_err(be)?;
        // The run lock ends with the session.
        conn.close_on_drop();
        let locked: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1, 0))")
                .bind(run_key(&self.schema))
                .fetch_one(&mut *conn)
                .await
                .map_err(be)?;
        if !locked {
            return Err(StoreError::InvalidArgument(format!(
                "another repack of schema \"{}\" is running",
                self.schema
            )));
        }
        let mut run = Run {
            store: self,
            objects,
            opts,
            progress,
            report: RepackReport { dry_run: opts.dry_run, ..RepackReport::default() },
            meta: HashMap::new(),
            hot: HashSet::new(),
            paths: Paths::default(),
            sources: HashMap::new(),
        };
        let result = run.run().await;
        sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
            .bind(run_key(&self.schema))
            .execute(&mut *conn)
            .await
            .map_err(be)?;
        result?;
        Ok(run.report)
    }
}

impl Run<'_> {
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
            "selected {} cold, {} hot and {} other packed objects ({:.1} MiB), {} from \
             Postgres ({:.1} MiB)",
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
    async fn commits(&mut self) -> Result<Vec<crate::CommitNode>> {
        let refs = self.store.list_refs("").await?;
        let mut seen: HashSet<ObjectId> = HashSet::new();
        let mut frontier: Vec<ObjectId> = refs.iter().map(|(_, id)| *id).collect();
        let mut all = Vec::new();
        while !frontier.is_empty() {
            frontier.retain(|id| seen.insert(*id));
            let mut next = Vec::new();
            for chunk in frontier.chunks(WALK_BATCH) {
                for node in self.store.get_commits(chunk).await?.into_iter().flatten() {
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
    async fn hot_set(&mut self, commits: &[crate::CommitNode]) -> Result<()> {
        let nodes: HashMap<ObjectId, &crate::CommitNode> =
            commits.iter().map(|c| (c.id, c)).collect();
        let mut trees = Vec::new();
        for (_, tip) in self.store.list_refs("").await? {
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
    async fn walk(&mut self, commits: &[crate::CommitNode]) -> Result<()> {
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

    /// Scan the packed index and the Postgres objects for rows to move;
    /// returns the work, in segments.
    async fn select(&mut self) -> Result<Vec<Segment>> {
        let cutoff: f64 = sqlx::query_scalar("SELECT extract(epoch FROM now())::float8 - $1")
            .bind(self.opts.min_age.as_secs_f64())
            .fetch_one(&self.store.pool)
            .await
            .map_err(be)?;
        let mut cold: Vec<(ObjectId, Meta)> = Vec::new();
        let mut other: Vec<ObjectId> = Vec::new();
        let mut hot: Vec<ObjectId> = Vec::new();
        let mut last: Vec<u8> = Vec::new();
        loop {
            let rows = sqlx::query(
                "SELECT x.id, x.pack, x.off, coalesce(x.share, x.len)::bigint AS bytes, \
                        x.written_at < to_timestamp($2) AS old, \
                        EXISTS (SELECT 1 FROM gc_condemned g WHERE g.id = x.id) AS condemned, \
                        coalesce((SELECT CASE WHEN p.hot THEN 2 ELSE 1 END FROM packs p \
                                  WHERE p.pack = x.pack AND p.repacked_at IS NOT NULL), 0) \
                          AS repacked \
                 FROM object_index x WHERE x.id > $1 ORDER BY x.id LIMIT $3",
            )
            .bind(&last)
            .bind(cutoff)
            .bind(SCAN_PAGE)
            .fetch_all(&self.store.pool)
            .await
            .map_err(be)?;
            let Some(tail) = rows.last() else { break };
            last = tail.try_get("id").map_err(be)?;
            for r in &rows {
                let old: bool = r.try_get("old").map_err(be)?;
                let condemned: bool = r.try_get("condemned").map_err(be)?;
                let id = object_id_col(r, "id")?;
                // 1: in a delta pack of an earlier repack; 2: in one of its
                // hot packs (left alone while the object is still hot).
                let done = match r.try_get::<i32, _>("repacked").map_err(be)? {
                    1 => true,
                    2 => self.hot.contains(&id),
                    _ => false,
                };
                if !old || condemned || (done && !self.opts.all) {
                    continue;
                }
                let pack: Vec<u8> = r.try_get("pack").map_err(be)?;
                let pack: [u8; 16] = pack
                    .as_slice()
                    .try_into()
                    .map_err(|_| StoreError::InvalidArgument("corrupt pack id".into()))?;
                let off: i64 = r.try_get("off").map_err(be)?;
                let bytes: i64 = r.try_get("bytes").map_err(be)?;
                self.sources.insert(id, Source::Index { pack, off: off as u64 });
                self.report.old_s3_bytes += bytes as u64;
                if self.hot.contains(&id) {
                    hot.push(id);
                } else if let Some(m) = self.meta.get(&id) {
                    cold.push((id, *m));
                } else {
                    other.push(id);
                }
            }
        }
        if self.opts.small {
            let mut last: Vec<u8> = Vec::new();
            loop {
                let rows = sqlx::query(
                    "SELECT o.id, octet_length(o.data)::bigint AS bytes, \
                            o.written_at < to_timestamp($2) AS old, \
                            EXISTS (SELECT 1 FROM gc_condemned g WHERE g.id = o.id) AS condemned \
                     FROM objects o WHERE o.id > $1 ORDER BY o.id LIMIT $3",
                )
                .bind(&last)
                .bind(cutoff)
                .bind(SCAN_PAGE)
                .fetch_all(&self.store.pool)
                .await
                .map_err(be)?;
                let Some(tail) = rows.last() else { break };
                last = tail.try_get("id").map_err(be)?;
                for r in &rows {
                    let old: bool = r.try_get("old").map_err(be)?;
                    let condemned: bool = r.try_get("condemned").map_err(be)?;
                    let id = object_id_col(r, "id")?;
                    // Only cold objects the walk reached; an id with an index
                    // row as well is moved through that.
                    let Some(m) = self.meta.get(&id) else { continue };
                    if !old || condemned || self.sources.contains_key(&id) {
                        continue;
                    }
                    let bytes: i64 = r.try_get("bytes").map_err(be)?;
                    self.sources.insert(id, Source::Small);
                    self.report.old_pg_bytes += bytes as u64;
                    self.report.selected_small += 1;
                    cold.push((id, *m));
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
        let mut uploads: FuturesUnordered<tokio::task::JoinHandle<Result<Switched>>> =
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
                    match up.map_err(StoreError::backend).and_then(|r| r) {
                        Ok(s) => self.tally(s)?,
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
            match up.map_err(StoreError::backend).and_then(|r| r) {
                Ok(s) => self.tally(s)?,
                Err(e) => failed = failed.or(Some(e)),
            }
        }
        if let Some(e) = failed {
            return Err(e);
        }
        self.report.finished = !stopped;
        Ok(())
    }

    fn tally(&mut self, s: Switched) -> Result<()> {
        self.report.moved += s.moved;
        self.report.moved_small += s.moved_small;
        self.report.skipped += s.skipped;
        Ok(())
    }

    /// Start uploading and switching the current pack (dry run: count it).
    fn flush(
        &mut self,
        buf: &mut PackBuf,
        sources: Arc<HashMap<ObjectId, Source>>,
    ) -> tokio::task::JoinHandle<Result<Switched>> {
        let hot = buf.hot;
        let body = std::mem::take(&mut buf.body);
        let entries = std::mem::take(&mut buf.entries);
        self.report.packs += 1;
        self.report.new_bytes += body.len() as u64;
        let (store, s3, dry_run, batch) =
            (self.store.clone(), self.opts.s3.clone(), self.opts.dry_run, self.opts.batch);
        tokio::spawn(async move {
            if dry_run {
                return Ok(Switched { moved: entries.len() as u64, ..Switched::default() });
            }
            let pack: [u8; 16] = rand::random();
            s3.put_raw(s3.pack_key(&pack), body).await?;
            store.switch(pack, hot, &entries, &sources, batch).await
        })
    }
}

fn mib(b: u64) -> f64 {
    b as f64 / (1024.0 * 1024.0)
}

/// A pack being assembled: its body and `(id, offset, length, share)` per
/// object; `hot`: it holds hot objects' plain frames.
#[derive(Default)]
struct PackBuf {
    body: Vec<u8>,
    entries: Vec<(ObjectId, u64, u32, Option<u32>)>,
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

/// Outcome of switching one pack's rows.
#[derive(Default)]
struct Switched {
    moved: u64,
    moved_small: u64,
    skipped: u64,
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

impl PgStore {
    /// Point the rows of `entries` (objects of the uploaded pack `pack`) at
    /// it, `batch` ids per transaction; see the module docs for what is
    /// skipped.
    async fn switch(
        &self,
        pack: [u8; 16],
        hot: bool,
        entries: &[(ObjectId, u64, u32, Option<u32>)],
        sources: &HashMap<ObjectId, Source>,
        batch: usize,
    ) -> Result<Switched> {
        let mut out = Switched::default();
        sqlx::query(
            "INSERT INTO packs (pack, repacked_at, hot) VALUES ($1, now(), $2) \
             ON CONFLICT (pack) DO UPDATE SET repacked_at = now(), hot = $2",
        )
        .bind(pack.as_slice())
        .bind(hot)
        .execute(&self.pool)
        .await
        .map_err(be)?;
        let mut sorted: Vec<&(ObjectId, u64, u32, Option<u32>)> = entries.iter().collect();
        sorted.sort_unstable_by_key(|e| e.0);
        sorted.dedup_by_key(|e| e.0);
        for chunk in sorted.chunks(batch.max(1)) {
            let mut moved = Vec::new();
            let mut small = Vec::new();
            for e in chunk {
                match sources.get(&e.0) {
                    Some(Source::Index { pack, off }) => moved.push((*e, *pack, *off)),
                    Some(Source::Small) => small.push(*e),
                    None => out.skipped += 1,
                }
            }
            let mut tx = self.pool.begin().await.map_err(be)?;
            if !moved.is_empty() {
                let ids: Vec<&[u8]> = moved.iter().map(|m| m.0.0.as_bytes().as_slice()).collect();
                let offs: Vec<i64> = moved.iter().map(|m| m.0.1 as i64).collect();
                let lens: Vec<i32> = moved.iter().map(|m| m.0.2 as i32).collect();
                let shares: Vec<Option<i32>> =
                    moved.iter().map(|m| m.0.3.map(|s| s as i32)).collect();
                let old_packs: Vec<&[u8]> = moved.iter().map(|m| m.1.as_slice()).collect();
                let old_offs: Vec<i64> = moved.iter().map(|m| m.2 as i64).collect();
                // Rows still where the object was read from, not condemned,
                // not locked by anyone; the packs they leave are recorded (in
                // key order, so concurrent switches never deadlock).
                let n: i64 = sqlx::query_scalar(
                    "WITH n AS (SELECT * FROM unnest($1::bytea[], $3::bigint[], $4::integer[], \
                                        $5::integer[], $6::bytea[], $7::bigint[]) \
                                AS t(id, off, len, share, old_pack, old_off)), \
                     upd AS (UPDATE object_index x \
                         SET pack = $2, off = n.off, len = n.len, share = n.share \
                         FROM n WHERE x.id = n.id AND x.pack = n.old_pack AND x.off = n.old_off \
                           AND x.id IN (SELECT o.id FROM object_index o WHERE o.id = ANY($1) \
                               AND NOT EXISTS (SELECT 1 FROM gc_condemned g WHERE g.id = o.id) \
                               ORDER BY o.id FOR UPDATE SKIP LOCKED) \
                         RETURNING x.id, n.old_pack), \
                     mv AS (INSERT INTO packs (pack, moved_at) \
                         SELECT old_pack, now() FROM (SELECT DISTINCT old_pack FROM upd) u \
                         ORDER BY old_pack \
                         ON CONFLICT (pack) DO UPDATE SET moved_at = now()) \
                     SELECT count(*) FROM upd",
                )
                .bind(&ids)
                .bind(pack.as_slice())
                .bind(&offs)
                .bind(&lens)
                .bind(&shares)
                .bind(&old_packs)
                .bind(&old_offs)
                .fetch_one(&mut *tx)
                .await
                .map_err(be)?;
                out.moved += n as u64;
                out.skipped += moved.len() as u64 - n as u64;
            }
            if !small.is_empty() {
                let ids: Vec<&[u8]> = small.iter().map(|m| m.0.as_bytes().as_slice()).collect();
                let offs: Vec<i64> = small.iter().map(|m| m.1 as i64).collect();
                let lens: Vec<i32> = small.iter().map(|m| m.2 as i32).collect();
                let shares: Vec<Option<i32>> =
                    small.iter().map(|m| m.3.map(|s| s as i32)).collect();
                // The object moves only if its row is deleted here: never
                // one a sweep deletes, condemned, or locked by a writer.
                // The index row keeps the object's write time.
                let n: i64 = sqlx::query_scalar(
                    "WITH n AS (SELECT * FROM unnest($1::bytea[], $3::bigint[], $4::integer[], \
                                        $5::integer[]) AS t(id, off, len, share)), \
                     del AS (DELETE FROM objects o WHERE o.id IN ( \
                         SELECT s.id FROM objects s WHERE s.id = ANY($1) \
                           AND NOT EXISTS (SELECT 1 FROM gc_condemned g WHERE g.id = s.id) \
                         ORDER BY s.id FOR UPDATE SKIP LOCKED) \
                         RETURNING o.id, o.written_at), \
                     ins AS (INSERT INTO object_index (id, pack, off, len, share, written_at) \
                         SELECT n.id, $2, n.off, n.len, n.share, del.written_at \
                         FROM n JOIN del ON del.id = n.id \
                         ON CONFLICT (id) DO UPDATE \
                         SET written_at = greatest(object_index.written_at, EXCLUDED.written_at) \
                         RETURNING id) \
                     SELECT count(*) FROM ins",
                )
                .bind(&ids)
                .bind(pack.as_slice())
                .bind(&offs)
                .bind(&lens)
                .bind(&shares)
                .fetch_one(&mut *tx)
                .await
                .map_err(be)?;
                out.moved += n as u64;
                out.moved_small += n as u64;
                out.skipped += small.len() as u64 - n as u64;
            }
            tx.commit().await.map_err(be)?;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packed::PackedS3Store;
    use crate::s3::S3Config;
    use crate::tiered::TieredObjectStore;
    use crate::{ObjectStoreExt, conformance};

    /// A Postgres + S3 store whose objects larger than 64 bytes go to packs.
    async fn fresh() -> (PgStore, S3ObjectStore, Arc<dyn ObjectStore>) {
        let pg = PgStore::connect_scratch(
            &crate::testing::pg_url(),
            &crate::testing::scratch_schema("rp"),
        )
        .await
        .unwrap();
        let cfg = S3Config::for_tests(&format!("rp{}", rand::random::<u64>()));
        let s3 = S3ObjectStore::connect_scratch(&cfg).await.unwrap();
        s3.ensure_bucket().await.unwrap();
        let packed = PackedS3Store::new(pg.clone(), s3.clone());
        let tiered = TieredObjectStore::new(Arc::new(pg.clone()), Arc::new(packed), 64);
        (pg, s3, Arc::new(tiered))
    }

    fn opts(s3: &S3ObjectStore) -> RepackOptions {
        let mut o = RepackOptions::new(s3.clone());
        o.min_age = Duration::ZERO;
        o.all = true;
        o.cluster.level = 3;
        o.cluster.cluster_raw = 256 * 1024;
        o
    }

    /// Repacks everything after every write.
    struct Repacking {
        pg: PgStore,
        objects: Arc<dyn ObjectStore>,
        opts: RepackOptions,
    }

    #[async_trait::async_trait]
    impl ObjectStore for Repacking {
        async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
            self.objects.get_many(ids).await
        }

        async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
            self.objects.put_many(objects).await?;
            match self.pg.repack(self.objects.as_ref(), &self.opts, &|_| {}).await {
                Ok(r) => {
                    assert!(r.finished, "{r:?}");
                    Ok(())
                }
                // Concurrent writers: one repacks at a time.
                Err(StoreError::InvalidArgument(m)) if m.contains("another repack") => Ok(()),
                Err(e) => Err(e),
            }
        }

        async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
            self.objects.has_many(ids).await
        }
    }

    async fn count(pg: &PgStore, q: &str) -> i64 {
        sqlx::query_scalar(sqlx::AssertSqlSafe(q.to_owned())).fetch_one(&pg.pool).await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs postgres and s3: docker compose up -d"]
    async fn repacked_stores_conform() {
        let (pg, s3, objects) = fresh().await;
        let r = Repacking { pg: pg.clone(), objects, opts: opts(&s3) };
        conformance::object_store(Arc::new(r)).await;
        let repacked = "SELECT count(*) FROM object_index x JOIN packs p USING (pack) \
                        WHERE p.repacked_at IS NOT NULL";
        assert!(count(&pg, repacked).await > 0);
        // The same with deltas off and with records of one object.
        for cluster in [
            ClusterOptions { depth: 0, ..opts(&s3).cluster },
            ClusterOptions { cluster_raw: 1, ..opts(&s3).cluster },
        ] {
            let (pg, s3, objects) = fresh().await;
            let r = Repacking { pg, objects, opts: RepackOptions { cluster, ..opts(&s3) } };
            conformance::object_store(Arc::new(r)).await;
        }
    }

    /// One repack per schema at a time.
    #[tokio::test]
    #[ignore = "needs postgres and s3: docker compose up -d"]
    async fn repacks_are_exclusive() {
        let (pg, s3, objects) = fresh().await;
        let mut holder = pg.pool.acquire().await.unwrap();
        sqlx::query("SELECT pg_advisory_lock(hashtextextended($1, 0))")
            .bind(run_key(&pg.schema))
            .execute(&mut *holder)
            .await
            .unwrap();
        let err = pg.repack(objects.as_ref(), &opts(&s3), &|_| {}).await.unwrap_err();
        assert!(err.to_string().contains("another repack"), "{err}");
        sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
            .bind(run_key(&pg.schema))
            .execute(&mut *holder)
            .await
            .unwrap();
        let r = pg.repack(objects.as_ref(), &opts(&s3), &|_| {}).await.unwrap();
        assert!(r.finished);
        // The lock is released after a run (and after a failed one).
        assert!(pg.repack(objects.as_ref(), &opts(&s3), &|_| {}).await.unwrap().finished);
    }

    /// Versions of a few files: each a delta of the next once repacked.
    fn versions(n: usize) -> Vec<EncodedObject> {
        (0..n)
            .map(|v| {
                let mut text = String::new();
                for line in 0..200 {
                    let w = if line == v % 200 { format!("edit {v}") } else { "same".into() };
                    text.push_str(&format!("{line}: a line of a file, {w}\n"));
                }
                EncodedObject::new(ObjectKind::Blob, text.as_bytes())
            })
            .collect()
    }

    #[tokio::test]
    #[ignore = "needs postgres and s3: docker compose up -d"]
    async fn corrupt_delta_records_are_read_errors_not_panics() {
        let (pg, s3, objects) = fresh().await;
        let objs = versions(30);
        let ids: Vec<ObjectId> = objs.iter().map(EncodedObject::id).collect();
        objects.put_many(&objs).await.unwrap();
        let r = pg.repack(objects.as_ref(), &opts(&s3), &|_| {}).await.unwrap();
        assert!(r.finished && r.moved == 30 && r.deltas >= 25, "{r:?}");
        let index = pg.lookup_index(&ids).await.unwrap();
        let pack = index[&ids[0]].pack;
        assert!(index.values().all(|e| e.pack == pack), "one pack");
        let key = s3.pack_key(&pack);
        let size = index.values().map(|e| e.off + u64::from(e.len)).max().unwrap();
        let good = s3.get_range(&key, 0, size).await.unwrap().to_vec();
        assert!(good.starts_with(b"ADF1"));
        let reader = || -> Arc<dyn ObjectStore> {
            let packed = PackedS3Store::new(pg.clone(), s3.clone());
            Arc::new(TieredObjectStore::new(Arc::new(pg.clone()), Arc::new(packed), 64))
        };
        let damaged = |f: &dyn Fn(&mut Vec<u8>)| {
            let mut b = good.clone();
            f(&mut b);
            b
        };
        let table_end = 12 + 30 * 20;
        let cases = [
            // The data: every base and delta.
            damaged(&|b| b[table_end..].iter_mut().step_by(13).for_each(|x| *x ^= 0x20)),
            // The member table: bases, lengths and sizes.
            damaged(&|b| b[12..table_end].iter_mut().skip(8).step_by(20).for_each(|x| *x ^= 1)),
            damaged(&|b| b[12..table_end].iter_mut().skip(16).step_by(20).for_each(|x| *x ^= 7)),
            // The header.
            damaged(&|b| b[8..12].copy_from_slice(&u32::MAX.to_le_bytes())),
        ];
        for body in cases {
            let len = body.len();
            s3.put_raw(key.clone(), body).await.unwrap();
            if len < good.len() {
                // A short pack: the ranged read fails (after its retries).
                continue;
            }
            let r = reader().get_many(&ids).await;
            assert!(matches!(r, Err(StoreError::Backend(_))), "{r:?}");
        }
        s3.put_raw(key, good).await.unwrap();
        let got = reader().get_many(&ids).await.unwrap();
        assert!(got.iter().zip(&objs).all(|(g, o)| g.as_ref() == Some(o)));
        assert_eq!(reader().get(&ids[29]).await.unwrap().as_ref(), Some(&objs[29]));
    }

    /// Reads and uploads share the S3 store's request slots: a repack must
    /// keep polling the segments it reads while it waits for uploads, or a
    /// paused read holding the slots starves the uploads it waits for
    /// (a hang until the S3 operation timeout). One slot, several segments
    /// read at once, a pack per record: every wait for uploads finds other
    /// segments mid-read.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs postgres and s3: docker compose up -d"]
    async fn uploads_never_wait_on_slots_held_by_paused_reads() {
        let pg = PgStore::connect_scratch(
            &crate::testing::pg_url(),
            &crate::testing::scratch_schema("rp"),
        )
        .await
        .unwrap();
        let mut cfg = S3Config::for_tests(&format!("rp{}", rand::random::<u64>()));
        cfg.timeouts.max_concurrency = 1;
        let s3 = S3ObjectStore::connect_scratch(&cfg).await.unwrap();
        s3.ensure_bucket().await.unwrap();
        let packed = PackedS3Store::new(pg.clone(), s3.clone());
        let objects: Arc<dyn ObjectStore> =
            Arc::new(TieredObjectStore::new(Arc::new(pg.clone()), Arc::new(packed), 64));
        // Unreachable objects (no refs): segments of `SEGMENT_OBJECTS / 2`,
        // spread over many packs (many ranged reads per segment).
        let n = SEGMENT_OBJECTS * 2;
        let all: Vec<EncodedObject> = (0..n)
            .map(|i| {
                let body: Vec<u8> = (0..200).map(|_| rand::random::<u8>()).collect();
                EncodedObject::new(ObjectKind::Blob, &[format!("{i}\n").as_bytes(), &body].concat())
            })
            .collect();
        for chunk in all.chunks(16) {
            objects.put_many(chunk).await.unwrap();
        }
        let cluster = ClusterOptions { cluster_raw: 4096, ..opts(&s3).cluster };
        let o = RepackOptions { jobs: 4, pack_bytes: 1, cluster, ..opts(&s3) };
        for round in 0..3 {
            let run = pg.repack(objects.as_ref(), &o, &|_| {});
            let r = tokio::time::timeout(Duration::from_secs(60), run)
                .await
                .unwrap_or_else(|_| panic!("round {round}: repack hangs: {:?}", crate::s3::stats()))
                .unwrap();
            assert!(r.finished && r.moved == n as u64 && r.packs > 100, "{r:?}");
        }
        let ids: Vec<ObjectId> = all.iter().map(EncodedObject::id).collect();
        let got = objects.get_many(&ids).await.unwrap();
        assert!(got.iter().zip(&all).all(|(g, o)| g.as_ref() == Some(o)));
    }

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
}
