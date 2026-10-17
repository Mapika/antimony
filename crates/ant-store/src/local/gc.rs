//! Garbage collection of a local data directory (`ant admin gc --local`).
//!
//! A run is safe while the server keeps serving: it runs inside the process
//! that owns the directory, either the server itself (a request on its
//! maintenance socket, `super::admin`, or `ant serve --gc-interval`) or an
//! offline tool holding the directory's lock while no server runs. It is
//! the protocol of the Postgres collector (`crate::postgres::gc`, whose
//! module docs carry the full argument) on a single process with one
//! writer thread, which makes several of its parts simpler.
//!
//! # What is kept (roots)
//!
//! As in Postgres: ref targets, history watermarks and every commit in the
//! history index, both sides of the derived rows of
//! [`GcOptions::root_kinds`] (landed-as and git-import mappings), and every
//! object, packed-object index row and commit record written within the
//! grace period (unlanded changesets, uploads, imports in progress).
//! Storing an existing object, or recording an existing commit, refreshes a
//! write time older than an hour (`LocalStore::put_many`, `put_commits`).
//!
//! # A run
//!
//! 1. **Mark.** The closure of the roots (commit → tree and parents, tree →
//!    entries, gitlinks skipped), in memory; commit records give tree and
//!    parents without reading the commit object. Rows written since the
//!    mark began are marked again right before step 2. Ids the published
//!    roots (refs, history, root kinds) reach but the store lacks are
//!    reported as missing ([`GcReport::missing`]).
//! 2. **Condemn.** Every id with a row written before the cutoff (`start -
//!    grace`) that is not marked is condemned: an in-memory set the store's
//!    readers consult ([`super::Hidden`]). From then on readers treat a
//!    condemned id's rows as missing *while the row is older than the
//!    cutoff* (objects, packed-object index rows, commit records and their
//!    changesets; `get_many`, `has_many`, `get_commits`, `get_changesets`,
//!    `first_parent_chain`, `ancestors_above`): exactly the rows the sweep
//!    would delete. No operation that starts later can read one and build
//!    on it.
//! 3. **Quiesce.** Wait [`GcOptions::quiesce`] (default 30 minutes): longer
//!    than the server's cache lifetime (10 minutes) plus its longest
//!    operation, so no operation still holds an id it read before step 2.
//!    Rows written meanwhile are marked again every few seconds.
//! 4. **Re-mark.** The roots are read again, and every row written since
//!    the mark began (minus a margin) is marked with its closure; marked
//!    ids leave the condemned set (before their bodies are read).
//! 5. **Sweep.** Batches of condemned ids are deleted, each batch one
//!    durable transaction on the writer thread, deleting a row only if it is
//!    *still* older than the cutoff: objects, index rows, commit records
//!    with their changesets and commit-keyed derived rows
//!    ([`GcOptions::commit_keyed_kinds`]). Afterwards the condemned set is
//!    dropped.
//! 6. **Packs.** A pack no index row points into is deleted; one whose dead
//!    share reaches [`GcOptions::compact_ratio`] is compacted (below). Busy
//!    packs (`super::packs`: the active pack, and packs with appends whose
//!    index rows are not written yet) are left alone.
//! 7. **Space.** Freed SQLite pages go back to the file system: online with
//!    `PRAGMA incremental_vacuum` in short steps (databases created by this
//!    version, `auto_vacuum = INCREMENTAL`); offline ([`GcOptions::exclusive`])
//!    with `VACUUM` when more than a quarter of the file is free, which also
//!    switches older databases to incremental vacuuming.
//!
//! An offline run ([`GcOptions::exclusive`]) skips the quiesce wait:
//! nothing else uses the store.
//!
//! # Why nothing reachable is deleted
//!
//! The Postgres argument carries over (`crate::postgres::gc`, *Why nothing
//! reachable is deleted*); what differs:
//!
//! - *Re-storing and read-your-writes.* Every write goes through the one
//!   writer thread, and so does every sweep batch: a refresh and a delete
//!   of the same row are serialized, and the delete re-checks the write
//!   time in its own transaction. Once `put_many` (or `index_objects`,
//!   `put_commits`) returns, the row is younger than the cutoff of any run
//!   (grace ≥ 1 day > the 1 hour refresh age): readers never hide it (they
//!   hide only rows older than the cutoff), and no sweep deletes it. So no
//!   take-back from the condemned set is needed.
//! - *Blind ref moves.* Each sweep batch, on the writer thread (so after
//!   every ref move committed before it, and before any after it), checks
//!   that no ref or history watermark points at a condemned id; if one
//!   does, the batch deletes nothing and the roots are marked again first.
//! - *Packs.* A pack gains index rows only from writers that appended to it
//!   (busy until their rows are written) or from maintenance, which runs
//!   one at a time (collection or repack, `LocalStore::maintenance`). So a
//!   pack that is not busy and has no rows stays that way. Compaction
//!   copies whole records (each self-contained: a frame or delta frame
//!   decodes from its own bytes) to a new pack, fsyncs it, moves the index
//!   rows still pointing at the old location in one durable transaction,
//!   and only then deletes the old pack; a reader that looked up the old
//!   location retries (`super::packs`, *Deletion*). A crash at any point
//!   leaves every row pointing at durable bytes: at worst an unreferenced
//!   pack, deleted by the next run.
//!
//! The condemned set lives in memory: a run interrupted (cancelled, or the
//! process stopped) leaves nothing condemned, and the next run starts
//! over. Rows already deleted were garbage; everything else is intact.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ant_core::{EntryMode, ObjectId, ObjectKind};

use super::{Hidden, LocalStore, Sq, id_of, now_secs};
use crate::{ObjectStore, Result, StoreError};

pub use crate::postgres::gc::{
    DEFAULT_COMMIT_KEYED_KINDS, DEFAULT_GRACE, DEFAULT_QUIESCE, DEFAULT_ROOT_KINDS, MIN_GRACE,
};

/// Look-back added to "written since" scans: a write time is taken on the
/// writer thread just before its group commits, and has one-second
/// resolution.
const MARGIN_SECS: i64 = 60;
/// How often rows written meanwhile are marked during the quiesce wait and
/// the sweep.
const REMARK_EVERY: Duration = Duration::from_secs(5);
/// Pages freed per incremental-vacuum step (one short writer job each).
const VACUUM_STEP_PAGES: u32 = 4096;

/// Settings of one local collection run.
#[derive(Debug, Clone)]
pub struct GcOptions {
    /// Rows written less than this long before the run are kept, with
    /// everything they reach. At least [`MIN_GRACE`].
    pub grace: Duration,
    /// Wait between condemning and deleting while serving: must exceed the
    /// server's cache lifetime plus its longest request. Ignored when
    /// [`Self::exclusive`].
    pub quiesce: Duration,
    /// Mark and count only; change nothing.
    pub dry_run: bool,
    /// Nothing else uses the store (an offline run: `ant admin gc --local`
    /// with no server): no quiesce wait, and a full `VACUUM` when worth it.
    pub exclusive: bool,
    /// Derived kinds whose 32-byte keys and values are roots.
    pub root_kinds: Vec<String>,
    /// Derived kinds keyed by `<commit id><suffix>`, deleted with their commit.
    pub commit_keyed_kinds: Vec<String>,
    /// Ids per mark read and per delete transaction.
    pub batch: usize,
    /// Compact a pack when at least this share of its bytes is dead (1.0 or
    /// more: never).
    pub compact_ratio: f64,
}

impl Default for GcOptions {
    fn default() -> Self {
        Self {
            grace: DEFAULT_GRACE,
            quiesce: DEFAULT_QUIESCE,
            dry_run: false,
            exclusive: false,
            root_kinds: DEFAULT_ROOT_KINDS.iter().map(|k| k.to_string()).collect(),
            commit_keyed_kinds: DEFAULT_COMMIT_KEYED_KINDS.iter().map(|k| k.to_string()).collect(),
            batch: 500,
            compact_ratio: 0.5,
        }
    }
}

/// What a run found and did (a dry run: would do).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GcReport {
    pub dry_run: bool,
    /// Rows written before this time (Unix seconds) were candidates.
    pub cutoff: i64,
    /// Ids marked reachable.
    pub live: u64,
    /// Ids reachable from published roots (refs, history, root kinds) but
    /// missing from the store. Ids that only rows written within the grace
    /// period reach are not counted: their writers may still be storing
    /// them, or stopped before they did, and nothing published needs them.
    pub missing: u64,
    /// Ids condemned (dry run: that would be).
    pub condemned: u64,
    /// Condemned ids kept after all (marked again, or written again).
    pub kept: u64,
    /// Small objects deleted: count, stored (compressed) bytes.
    pub objects: u64,
    pub object_bytes: u64,
    /// Packed objects (index rows) deleted: count, packed bytes (frame
    /// members: their share).
    pub large_objects: u64,
    pub large_object_bytes: u64,
    /// Commit records deleted.
    pub commits: u64,
    /// Changesets deleted: count, stored bytes.
    pub changesets: u64,
    pub changeset_bytes: u64,
    /// Derived rows deleted with their commits.
    pub derived: u64,
    /// Pack files deleted: count, bytes.
    pub packs: u64,
    pub pack_bytes: u64,
    /// Packs compacted: count, bytes before, bytes of their live records.
    pub packs_compacted: u64,
    pub compacted_from: u64,
    pub compacted_to: u64,
    /// Packs kept: count, bytes, bytes no index row points at.
    pub packs_kept: u64,
    pub packs_kept_bytes: u64,
    pub packs_dead_bytes: u64,
    /// Freed database pages were returned to the file system (`VACUUM` or
    /// incremental vacuuming).
    pub vacuumed: bool,
}

impl GcReport {
    /// `key=value` pairs (the maintenance socket's report line).
    pub fn encode(&self) -> String {
        let mut me = self.clone();
        let mut out: Vec<String> =
            me.fields().into_iter().map(|(k, v)| format!("{k}={v}")).collect();
        out.push(format!("cutoff={}", self.cutoff));
        out.push(format!("dry_run={}", u8::from(self.dry_run)));
        out.push(format!("vacuumed={}", u8::from(self.vacuumed)));
        out.join(" ")
    }

    /// The report [`Self::encode`] wrote (unknown keys are ignored).
    pub fn decode(s: &str) -> Self {
        let values: HashMap<&str, &str> =
            s.split_whitespace().filter_map(|kv| kv.split_once('=')).collect();
        let mut r = Self {
            cutoff: values.get("cutoff").and_then(|v| v.parse().ok()).unwrap_or(0),
            dry_run: values.get("dry_run") == Some(&"1"),
            vacuumed: values.get("vacuumed") == Some(&"1"),
            ..Self::default()
        };
        for (k, v) in r.fields() {
            if let Some(x) = values.get(k).and_then(|x| x.parse().ok()) {
                *v = x;
            }
        }
        r
    }

    fn fields(&mut self) -> [(&'static str, &mut u64); 20] {
        [
            ("live", &mut self.live),
            ("missing", &mut self.missing),
            ("condemned", &mut self.condemned),
            ("kept", &mut self.kept),
            ("objects", &mut self.objects),
            ("object_bytes", &mut self.object_bytes),
            ("large_objects", &mut self.large_objects),
            ("large_object_bytes", &mut self.large_object_bytes),
            ("commits", &mut self.commits),
            ("changesets", &mut self.changesets),
            ("changeset_bytes", &mut self.changeset_bytes),
            ("derived", &mut self.derived),
            ("packs", &mut self.packs),
            ("pack_bytes", &mut self.pack_bytes),
            ("packs_compacted", &mut self.packs_compacted),
            ("compacted_from", &mut self.compacted_from),
            ("compacted_to", &mut self.compacted_to),
            ("packs_kept", &mut self.packs_kept),
            ("packs_kept_bytes", &mut self.packs_kept_bytes),
            ("packs_dead_bytes", &mut self.packs_dead_bytes),
        ]
    }
}

const KIND_UNKNOWN: u8 = 0;
const KIND_COMMIT: u8 = 1;
const KIND_TREE: u8 = 2;
const KIND_BLOB: u8 = 3;

/// Child ids of a tree body (gitlinks skipped; unparsable modes count as
/// blobs, so their targets stay).
fn tree_children(body: &[u8], out: &mut Vec<(ObjectId, u8)>) -> Result<()> {
    let malformed = || StoreError::InvalidArgument("gc: malformed tree object".into());
    let mut rest = body;
    while !rest.is_empty() {
        let sp = rest.iter().position(|&b| b == b' ').ok_or_else(malformed)?;
        let mode = &rest[..sp];
        rest = &rest[sp + 1..];
        let nul = rest.iter().position(|&b| b == 0).ok_or_else(malformed)?;
        rest = &rest[nul + 1..];
        if rest.len() < ObjectId::LEN {
            return Err(malformed());
        }
        let id = ObjectId::from_bytes(&rest[..ObjectId::LEN])?;
        rest = &rest[ObjectId::LEN..];
        match EntryMode::from_octal(mode).map(EntryMode::class) {
            Ok(EntryMode::Tree) => out.push((id, KIND_TREE)),
            Ok(EntryMode::Gitlink) => {}
            _ => out.push((id, KIND_BLOB)),
        }
    }
    Ok(())
}

/// Tree and parent ids of a commit body.
fn commit_children(body: &[u8], out: &mut Vec<(ObjectId, u8)>) -> Result<()> {
    let malformed = || StoreError::InvalidArgument("gc: malformed commit object".into());
    for line in body.split(|&b| b == b'\n') {
        if line.is_empty() {
            break;
        }
        let (kind, hex) = if let Some(hex) = line.strip_prefix(b"tree ") {
            (KIND_TREE, hex)
        } else if let Some(hex) = line.strip_prefix(b"parent ") {
            (KIND_COMMIT, hex)
        } else {
            continue;
        };
        let hex = std::str::from_utf8(hex).map_err(|_| malformed())?;
        out.push((hex.parse::<ObjectId>().map_err(|_| malformed())?, kind));
    }
    Ok(())
}

/// Ids of the rows of `sql` (first column; `param` bound as `?1`).
fn ids_of(
    c: &rusqlite::Connection,
    sql: &str,
    param: Option<i64>,
    kind: u8,
    out: &mut Vec<(ObjectId, u8)>,
) -> Result<()> {
    let mut st = c.prepare(sql).sq()?;
    let mut rows = match param {
        Some(p) => st.query([p]).sq()?,
        None => st.query([]).sq()?,
    };
    while let Some(r) = rows.next().sq()? {
        let b: Vec<u8> = r.get(0).sq()?;
        if b.len() == ObjectId::LEN {
            out.push((id_of(&b)?, kind));
        }
    }
    Ok(())
}

/// Published roots of a run, `(id, kind)`: ref targets, history
/// watermarks and index, and the derived root kinds. Everything they reach
/// was stored before they were written; the other roots are the rows
/// written within the grace period ([`written_since`]).
fn published_roots(c: &rusqlite::Connection, opts: &GcOptions) -> Result<Vec<(ObjectId, u8)>> {
    let mut out = Vec::new();
    ids_of(c, "SELECT target FROM refs", None, KIND_UNKNOWN, &mut out)?;
    ids_of(c, "SELECT commit_id FROM history_watermark", None, KIND_COMMIT, &mut out)?;
    ids_of(c, "SELECT DISTINCT commit_id FROM history", None, KIND_COMMIT, &mut out)?;
    let mut st = c.prepare("SELECT key, value FROM derived WHERE kind = ?1").sq()?;
    for kind in &opts.root_kinds {
        let mut rows = st.query([kind]).sq()?;
        while let Some(r) = rows.next().sq()? {
            for col in 0..2 {
                let b: Vec<u8> = r.get(col).sq()?;
                if b.len() == ObjectId::LEN {
                    out.push((id_of(&b)?, KIND_UNKNOWN));
                }
            }
        }
    }
    Ok(out)
}

/// Ids of every row written at or after `since`.
fn written_since(c: &rusqlite::Connection, since: i64) -> Result<Vec<(ObjectId, u8)>> {
    let mut out = Vec::new();
    let p = Some(since);
    ids_of(c, "SELECT id FROM commits WHERE written_at >= ?1", p, KIND_COMMIT, &mut out)?;
    ids_of(c, "SELECT id FROM objects WHERE written_at >= ?1", p, KIND_UNKNOWN, &mut out)?;
    ids_of(c, "SELECT id FROM object_index WHERE written_at >= ?1", p, KIND_UNKNOWN, &mut out)?;
    Ok(out)
}

/// Commit records of `ids` (tree, parents).
fn recorded(
    c: &rusqlite::Connection,
    ids: &[ObjectId],
) -> Result<HashMap<ObjectId, (ObjectId, Vec<ObjectId>)>> {
    let mut st = c.prepare_cached("SELECT tree, parents FROM commits WHERE id = ?1").sq()?;
    let mut out = HashMap::new();
    for id in ids {
        let row: Option<(Vec<u8>, Vec<u8>)> = rusqlite::OptionalExtension::optional(
            st.query_row([id.as_bytes().as_slice()], |r| Ok((r.get(0)?, r.get(1)?))),
        )
        .sq()?;
        if let Some((tree, parents)) = row {
            out.insert(*id, (id_of(&tree)?, super::decode_ids(&parents)?));
        }
    }
    Ok(out)
}

/// Rows of `sql` (`id, bytes`; `?1` the cutoff) whose id is not live.
fn dead(
    c: &rusqlite::Connection,
    sql: &str,
    cutoff: i64,
    live: &HashSet<ObjectId>,
) -> Result<Vec<(ObjectId, u64)>> {
    let mut st = c.prepare(sql).sq()?;
    let mut rows = st.query([cutoff]).sq()?;
    let mut out = Vec::new();
    while let Some(r) = rows.next().sq()? {
        let id = id_of(&r.get::<_, Vec<u8>>(0).sq()?)?;
        if !live.contains(&id) {
            out.push((id, r.get::<_, i64>(1).sq()? as u64));
        }
    }
    Ok(out)
}

/// One pack's records that index rows point into: `(off, len)` with the
/// ids in it.
pub(crate) type PackRecords = HashMap<(u64, u32), Vec<ObjectId>>;

/// The records index rows point into, per pack.
pub(crate) fn pack_records(c: &rusqlite::Connection) -> Result<HashMap<[u8; 16], PackRecords>> {
    let mut st = c.prepare("SELECT id, pack, off, len FROM object_index").sq()?;
    let mut rows = st.query([]).sq()?;
    let mut out: HashMap<[u8; 16], PackRecords> = HashMap::new();
    while let Some(r) = rows.next().sq()? {
        let id = id_of(&r.get::<_, Vec<u8>>(0).sq()?)?;
        let pack = pack_id(&r.get::<_, Vec<u8>>(1).sq()?)?;
        let off = r.get::<_, i64>(2).sq()? as u64;
        let len = r.get::<_, i64>(3).sq()? as u32;
        out.entry(pack).or_default().entry((off, len)).or_default().push(id);
    }
    Ok(out)
}

pub(crate) fn pack_id(b: &[u8]) -> Result<[u8; 16]> {
    b.try_into().map_err(|_| StoreError::InvalidArgument("corrupt pack id".into()))
}

/// Ends the readers' hiding when a run ends, however it ends (finished,
/// failed, or its future dropped).
struct HiddenGuard<'a>(&'a LocalStore);

impl Drop for HiddenGuard<'_> {
    fn drop(&mut self) {
        self.0.set_hidden(None);
    }
}

/// The mark: live ids, and (from step 2 on) the condemned set they leave.
struct Marker<'a> {
    store: &'a LocalStore,
    objects: Arc<dyn ObjectStore>,
    live: HashSet<ObjectId>,
    hidden: Option<Arc<Hidden>>,
    batch: usize,
    /// Count ids found missing (marking from published roots).
    count_missing: bool,
    missing: u64,
}

impl Marker<'_> {
    /// Mark `ids` and everything they reach.
    async fn mark(&mut self, ids: Vec<(ObjectId, u8)>) -> Result<()> {
        let mut todo = self.admit(ids);
        while !todo.is_empty() {
            let take = todo.len().min(self.batch);
            let level: Vec<(ObjectId, u8)> = todo.split_off(todo.len() - take);
            let mut children: Vec<(ObjectId, u8)> = Vec::new();
            let maybe_commits: Vec<ObjectId> = level
                .iter()
                .filter(|(_, k)| matches!(*k, KIND_COMMIT | KIND_UNKNOWN))
                .map(|(id, _)| *id)
                .collect();
            let records = self.store.db().read(move |c| recorded(c, &maybe_commits)).await?;
            for (tree, parents) in records.values() {
                children.push((*tree, KIND_TREE));
                children.extend(parents.iter().map(|p| (*p, KIND_COMMIT)));
            }
            let read: Vec<ObjectId> = level
                .iter()
                .filter(|(id, k)| *k != KIND_BLOB && !records.contains_key(id))
                .map(|(id, _)| *id)
                .collect();
            if !read.is_empty() {
                for (id, object) in read.iter().zip(self.objects.get_many(&read).await?) {
                    let Some(object) = object else {
                        // Reached from a published root: the store lost it.
                        // Reached first from a recent row: that row's
                        // writer may still be storing it (commit records
                        // and their objects are written concurrently, and a
                        // land batch builds on its predecessor's writes in
                        // flight), or stopped before it did; nothing
                        // published needs it.
                        if self.count_missing {
                            self.missing += 1;
                            tracing::debug!(%id, "gc: reachable object missing");
                        }
                        continue;
                    };
                    match object.kind() {
                        ObjectKind::Tree => tree_children(object.body(), &mut children)?,
                        ObjectKind::Commit => commit_children(object.body(), &mut children)?,
                        ObjectKind::Blob => {}
                    }
                }
            }
            todo.extend(self.admit(children));
        }
        Ok(())
    }

    /// The ids not marked yet, now marked; condemned ones leave the
    /// condemned set before anything reads them.
    fn admit(&mut self, ids: Vec<(ObjectId, u8)>) -> Vec<(ObjectId, u8)> {
        let new: Vec<(ObjectId, u8)> =
            ids.into_iter().filter(|(id, _)| self.live.insert(*id)).collect();
        if let Some(h) = &self.hidden
            && !new.is_empty()
        {
            let mut condemned = h.ids.write();
            for (id, _) in &new {
                condemned.remove(id);
            }
        }
        new
    }

    /// Mark the rows written since `since`.
    async fn mark_since(&mut self, since: i64) -> Result<()> {
        let recent = self.store.db().read(move |c| written_since(c, since)).await?;
        self.count_missing = false;
        self.mark(recent).await
    }

    /// Mark the roots: the published ones first (counting what they reach
    /// that is missing), then the rows written since `cutoff`. Returns how
    /// many roots there were.
    async fn mark_roots(&mut self, opts: &GcOptions, cutoff: i64) -> Result<usize> {
        let o = opts.clone();
        let (published, recent) = self
            .store
            .db()
            .read(move |c| Ok((published_roots(c, &o)?, written_since(c, cutoff)?)))
            .await?;
        let n = published.len() + recent.len();
        self.count_missing = true;
        self.mark(published).await?;
        self.count_missing = false;
        self.mark(recent).await?;
        Ok(n)
    }

    /// Mark the roots again, and the rows written since `since`.
    async fn remark(&mut self, opts: &GcOptions, cutoff: i64, since: i64) -> Result<()> {
        self.mark_roots(opts, cutoff).await?;
        self.mark_since(since).await
    }
}

/// Collect garbage in `store` (see the module docs). Safe while the store
/// serves; [`GcOptions::exclusive`] only when nothing else uses it. Waits
/// while another maintenance run (collection or repack) holds the store.
pub async fn collect(
    store: &LocalStore,
    opts: &GcOptions,
    progress: &(dyn Fn(&str) + Send + Sync),
) -> Result<GcReport> {
    super::db::maintenance_reads(collect_run(store, opts, progress)).await
}

async fn collect_run(
    store: &LocalStore,
    opts: &GcOptions,
    progress: &(dyn Fn(&str) + Send + Sync),
) -> Result<GcReport> {
    if opts.grace < MIN_GRACE {
        return Err(StoreError::InvalidArgument(format!(
            "gc grace period must be at least {} hours",
            MIN_GRACE.as_secs() / 3600
        )));
    }
    let _slot = match store.try_maintenance() {
        Some(slot) => slot,
        None => {
            progress("waiting for the running collection or repack to finish");
            store.maintenance().await
        }
    };
    let start = now_secs();
    let cutoff = start.saturating_sub(i64::try_from(opts.grace.as_secs()).unwrap_or(i64::MAX));
    let mut report = GcReport { dry_run: opts.dry_run, cutoff, ..GcReport::default() };
    let mut m = Marker {
        store,
        objects: store.backend().objects,
        live: HashSet::new(),
        hidden: None,
        batch: opts.batch.max(1),
        count_missing: true,
        missing: 0,
    };

    // -- 1. mark
    progress("marking from the roots");
    let roots = m.mark_roots(opts, cutoff).await?;
    progress(&format!("marked from {roots} roots"));
    m.mark_since(start - MARGIN_SECS).await?;
    progress(&format!("marked {} live ids", m.live.len()));

    // -- 2. condemn
    let live = Arc::new(std::mem::take(&mut m.live));
    let l = live.clone();
    let (small, large, commits) = store
        .db()
        .read(move |c| {
            let small =
                dead(c, "SELECT id, length(data) FROM objects WHERE written_at < ?1", cutoff, &l)?;
            let large = dead(
                c,
                "SELECT id, coalesce(share, len) FROM object_index WHERE written_at < ?1",
                cutoff,
                &l,
            )?;
            let commits = dead(
                c,
                "SELECT c.id, coalesce(length(cs.data), 0) FROM commits c \
                 LEFT JOIN changesets cs ON cs.commit_id = c.id WHERE c.written_at < ?1",
                cutoff,
                &l,
            )?;
            Ok((small, large, commits))
        })
        .await?;
    m.live = Arc::try_unwrap(live).unwrap_or_else(|a| (*a).clone());
    let condemned: HashSet<ObjectId> =
        small.iter().chain(&large).chain(&commits).map(|(id, _)| *id).collect();
    report.condemned = condemned.len() as u64;
    if opts.dry_run {
        report.live = m.live.len() as u64;
        report.missing = m.missing;
        report.objects = small.len() as u64;
        report.object_bytes = small.iter().map(|(_, b)| b).sum();
        report.large_objects = large.len() as u64;
        report.large_object_bytes = large.iter().map(|(_, b)| b).sum();
        report.commits = commits.len() as u64;
        report.changesets = commits.iter().filter(|(_, b)| *b > 0).count() as u64;
        report.changeset_bytes = commits.iter().map(|(_, b)| b).sum();
        packs(store, opts, &mut report, progress).await?;
        return Ok(report);
    }
    drop((small, large, commits));
    progress(&format!("condemned {} ids", report.condemned));
    let mut guard = None;
    if !condemned.is_empty() {
        let hidden = Arc::new(Hidden { cutoff, ids: parking_lot::RwLock::new(condemned) });
        store.set_hidden(Some(hidden.clone()));
        guard = Some(HiddenGuard(store));
        m.hidden = Some(hidden);
    }

    if let Some(hidden) = m.hidden.clone() {
        // -- 3. quiesce
        let mut since = start - MARGIN_SECS;
        if !opts.exclusive && !opts.quiesce.is_zero() {
            progress(&format!("waiting {}s for operations in flight", opts.quiesce.as_secs()));
            let until = Instant::now() + opts.quiesce;
            while Instant::now() < until {
                tokio::time::sleep(REMARK_EVERY.min(until - Instant::now())).await;
                let now = now_secs();
                m.mark_since(since).await?;
                since = now - MARGIN_SECS;
            }
        }
        // -- 4. re-mark
        m.remark(opts, cutoff, start - MARGIN_SECS).await?;
        // -- 5. sweep
        let deleted = sweep(&mut m, &hidden, opts, cutoff, start, &mut report).await?;
        report.kept = report.condemned.saturating_sub(deleted);
        progress(&format!(
            "deleted {} objects, {} packed objects, {} commits; kept {}",
            report.objects, report.large_objects, report.commits, report.kept
        ));
    }
    drop(guard);
    report.live = m.live.len() as u64;
    report.missing = m.missing;
    drop(m);

    // -- 6. packs
    packs(store, opts, &mut report, progress).await?;
    // -- 7. space
    report.vacuumed = reclaim(store, opts.exclusive).await?;
    Ok(report)
}

/// What one sweep batch did.
#[derive(Default)]
struct Swept {
    /// A ref or watermark points at a condemned id: nothing was deleted.
    remark: bool,
    /// Ids with at least one row deleted.
    ids: u64,
    objects: u64,
    object_bytes: u64,
    large_objects: u64,
    large_object_bytes: u64,
    commits: u64,
    changesets: u64,
    changeset_bytes: u64,
    derived: u64,
}

/// Delete the condemned ids, batch by batch (see the module docs); returns
/// how many ids lost a row.
async fn sweep(
    m: &mut Marker<'_>,
    hidden: &Arc<Hidden>,
    opts: &GcOptions,
    cutoff: i64,
    start: i64,
    report: &mut GcReport,
) -> Result<u64> {
    let mut deleted = 0;
    let mut remarked = Instant::now();
    let mut since = start - MARGIN_SECS;
    let mut remarks = 0;
    loop {
        let todo: Vec<ObjectId> = {
            let mut v: Vec<ObjectId> = hidden.ids.read().iter().copied().collect();
            v.sort_unstable();
            v
        };
        let mut again = false;
        for part in todo.chunks(m.batch) {
            if remarked.elapsed() >= REMARK_EVERY {
                let now = now_secs();
                m.mark_since(since).await?;
                since = now - MARGIN_SECS;
                remarked = Instant::now();
            }
            // Ids marked meanwhile are no longer condemned.
            let ids: Vec<ObjectId> = {
                let condemned = hidden.ids.read();
                part.iter().copied().filter(|id| condemned.contains(id)).collect()
            };
            if ids.is_empty() {
                continue;
            }
            let (h, kinds) = (hidden.clone(), opts.commit_keyed_kinds.clone());
            let s = m
                .store
                .db()
                .write(true, move |c, _| sweep_batch(c, &h, &ids, cutoff, &kinds))
                .await?;
            if s.remark {
                remarks += 1;
                if remarks > 100 {
                    return Err(StoreError::InvalidArgument(
                        "gc: refs keep moving to condemned commits; run again later".into(),
                    ));
                }
                m.remark(opts, cutoff, start - MARGIN_SECS).await?;
                again = true;
                break;
            }
            report.objects += s.objects;
            report.object_bytes += s.object_bytes;
            report.large_objects += s.large_objects;
            report.large_object_bytes += s.large_object_bytes;
            report.commits += s.commits;
            report.changesets += s.changesets;
            report.changeset_bytes += s.changeset_bytes;
            report.derived += s.derived;
            deleted += s.ids;
            // Deleted (or young, and so never hidden): no longer condemned.
            let mut condemned = hidden.ids.write();
            for id in part {
                condemned.remove(id);
            }
        }
        if !again {
            return Ok(deleted);
        }
    }
}

/// One sweep batch, on the writer thread.
fn sweep_batch(
    c: &rusqlite::Connection,
    hidden: &Hidden,
    ids: &[ObjectId],
    cutoff: i64,
    commit_keyed_kinds: &[String],
) -> Result<Swept> {
    let mut out = Swept::default();
    let ids: Vec<ObjectId> = {
        let condemned = hidden.ids.read();
        let mut targets = Vec::new();
        ids_of(c, "SELECT target FROM refs", None, KIND_UNKNOWN, &mut targets)?;
        ids_of(c, "SELECT commit_id FROM history_watermark", None, KIND_COMMIT, &mut targets)?;
        if targets.iter().any(|(id, _)| condemned.contains(id)) {
            out.remark = true;
            return Ok(out);
        }
        // Only what is still condemned now.
        ids.iter().copied().filter(|id| condemned.contains(id)).collect()
    };
    let mut del_obj = c
        .prepare_cached(
            "DELETE FROM objects WHERE id = ?1 AND written_at < ?2 RETURNING length(data)",
        )
        .sq()?;
    let mut del_idx = c
        .prepare_cached(
            "DELETE FROM object_index WHERE id = ?1 AND written_at < ?2 \
             RETURNING coalesce(share, len)",
        )
        .sq()?;
    let mut del_commit = c
        .prepare_cached("DELETE FROM commits WHERE id = ?1 AND written_at < ?2 RETURNING 1")
        .sq()?;
    let mut del_cs = c
        .prepare_cached("DELETE FROM changesets WHERE commit_id = ?1 RETURNING length(data)")
        .sq()?;
    let mut del_derived =
        c.prepare_cached("DELETE FROM derived WHERE kind = ?1 AND key >= ?2 AND key < ?3").sq()?;
    let one = |st: &mut rusqlite::CachedStatement<'_>, id: &ObjectId| -> Result<Option<i64>> {
        rusqlite::OptionalExtension::optional(
            st.query_row(rusqlite::params![id.as_bytes().as_slice(), cutoff], |r| r.get(0)),
        )
        .sq()
    };
    for id in &ids {
        let mut deleted = false;
        if let Some(n) = one(&mut del_obj, id)? {
            out.objects += 1;
            out.object_bytes += n as u64;
            deleted = true;
        }
        if let Some(n) = one(&mut del_idx, id)? {
            out.large_objects += 1;
            out.large_object_bytes += n as u64;
            deleted = true;
        }
        if one(&mut del_commit, id)?.is_some() {
            deleted = true;
            out.commits += 1;
            let cs: Option<i64> = rusqlite::OptionalExtension::optional(
                del_cs.query_row([id.as_bytes().as_slice()], |r| r.get(0)),
            )
            .sq()?;
            if let Some(n) = cs {
                out.changesets += 1;
                out.changeset_bytes += n as u64;
            }
            let lo = id.as_bytes().to_vec();
            let mut hi = lo.clone();
            hi.push(0xff);
            for kind in commit_keyed_kinds {
                out.derived += del_derived.execute(rusqlite::params![kind, lo, hi]).sq()? as u64;
            }
        }
        out.ids += u64::from(deleted);
    }
    Ok(out)
}

/// Pack files on disk with their sizes, and the busy ones. Index rows read
/// after this returns show every row a pack that is not busy will ever have
/// (module docs): writes queued before (a switch whose caller went away
/// included) have committed, and busy packs are taken before the rows.
/// `seal`: close the active pack first (the next write starts a new one),
/// so that its dead bytes can go too; it stays busy while appends to it
/// wait for their index rows.
async fn pack_files(
    store: &LocalStore,
    seal: bool,
) -> Result<(HashMap<[u8; 16], u64>, HashSet<[u8; 16]>)> {
    store.db().write(false, |_, _| Ok(())).await?;
    let f = store.packs().clone();
    tokio::task::spawn_blocking(move || {
        if seal {
            f.seal()?;
        }
        // Busy first: a pack not busy now gains no rows later (module docs).
        let busy = f.busy();
        Ok::<_, StoreError>((f.list()?, busy))
    })
    .await
    .map_err(StoreError::backend)?
}

/// Delete packs without live records, compact mostly-dead ones.
async fn packs(
    store: &LocalStore,
    opts: &GcOptions,
    report: &mut GcReport,
    progress: &(dyn Fn(&str) + Send + Sync),
) -> Result<()> {
    let (on_disk, busy) = pack_files(store, !opts.dry_run).await?;
    let records = store.db().read(pack_records).await?;
    let mut doomed: Vec<[u8; 16]> = Vec::new();
    let mut compact: Vec<([u8; 16], PackRecords)> = Vec::new();
    for (pack, size) in &on_disk {
        let recs = records.get(pack);
        let live_bytes: u64 = recs
            .map_or(0, |r| r.keys().map(|(_, len)| u64::from(*len)).sum::<u64>())
            + super::packs::PACK_MAGIC.len() as u64;
        let dead = size.saturating_sub(live_bytes);
        if busy.contains(pack) {
            report.packs_kept += 1;
            report.packs_kept_bytes += size;
            report.packs_dead_bytes += dead;
            continue;
        }
        let Some(recs) = recs else {
            doomed.push(*pack);
            report.packs += 1;
            report.pack_bytes += size;
            continue;
        };
        if *size > 0 && dead as f64 / *size as f64 >= opts.compact_ratio {
            report.packs_compacted += 1;
            report.compacted_from += size;
            report.compacted_to += live_bytes;
            compact.push((*pack, recs.clone()));
        } else {
            report.packs_kept += 1;
            report.packs_kept_bytes += size;
            report.packs_dead_bytes += dead;
        }
    }
    drop(records);
    if opts.dry_run {
        return Ok(());
    }
    delete_packs(store, doomed).await?;
    for (pack, recs) in compact {
        compact_pack(store, pack, recs).await?;
    }
    progress(&format!(
        "packs: {} deleted, {} compacted, {} kept",
        report.packs, report.packs_compacted, report.packs_kept
    ));
    Ok(())
}

/// Delete packs that have no index rows and are not busy, with their
/// repack records.
pub(crate) async fn delete_packs(store: &LocalStore, doomed: Vec<[u8; 16]>) -> Result<()> {
    if doomed.is_empty() {
        return Ok(());
    }
    let d = doomed.clone();
    store
        .db()
        .write(true, move |c, _| {
            let mut st = c.prepare_cached("DELETE FROM packs WHERE pack = ?1").sq()?;
            for p in &d {
                st.execute([p.as_slice()]).sq()?;
            }
            Ok(())
        })
        .await?;
    let f = store.packs().clone();
    tokio::task::spawn_blocking(move || f.delete(&doomed)).await.map_err(StoreError::backend)?
}

/// Delete the packs no index row points into that are not busy (after a
/// repack moved rows out of them). Returns their count and bytes.
pub(crate) async fn delete_unreferenced(store: &LocalStore) -> Result<(u64, u64)> {
    let (on_disk, busy) = pack_files(store, true).await?;
    let referenced: HashSet<[u8; 16]> = store
        .db()
        .read(|c| {
            let mut st = c.prepare("SELECT DISTINCT pack FROM object_index").sq()?;
            let mut rows = st.query([]).sq()?;
            let mut out = HashSet::new();
            while let Some(r) = rows.next().sq()? {
                out.insert(pack_id(&r.get::<_, Vec<u8>>(0).sq()?)?);
            }
            Ok(out)
        })
        .await?;
    let doomed: Vec<([u8; 16], u64)> =
        on_disk.into_iter().filter(|(p, _)| !busy.contains(p) && !referenced.contains(p)).collect();
    let (n, bytes) = (doomed.len() as u64, doomed.iter().map(|(_, s)| s).sum());
    delete_packs(store, doomed.into_iter().map(|(p, _)| p).collect()).await?;
    Ok((n, bytes))
}

/// Copy the live records of `pack` to a new pack, move their index rows,
/// delete the old pack (see the module docs).
pub(crate) async fn compact_pack(
    store: &LocalStore,
    pack: [u8; 16],
    recs: PackRecords,
) -> Result<()> {
    let mut order: Vec<((u64, u32), Vec<ObjectId>)> = recs.into_iter().collect();
    order.sort_unstable_by_key(|(at, _)| *at);
    let f = store.packs().clone();
    // (id, old offset, new offset)
    let ((new_pack, pin), moved) = tokio::task::spawn_blocking(move || {
        let mut w = f.writer()?;
        let mut moved: Vec<(ObjectId, u64, u64)> = Vec::new();
        for ((off, len), ids) in &order {
            let body = f.read_blocking(&pack, *off, u64::from(*len))?;
            let at = w.append(&body)?;
            moved.extend(ids.iter().map(|id| (*id, *off, at)));
        }
        Ok::<_, StoreError>((w.finish()?, moved))
    })
    .await
    .map_err(StoreError::backend)??;
    // Rows still where they were read from move, in one durable
    // transaction; the old file goes only after it committed.
    let (n, left) = store
        .db()
        .write(true, move |c, _| {
            let mut st = c
                .prepare_cached(
                    "UPDATE object_index SET pack = ?2, off = ?3 \
                     WHERE id = ?1 AND pack = ?4 AND off = ?5",
                )
                .sq()?;
            let mut n = 0usize;
            for (id, old, new) in &moved {
                n += st
                    .execute(rusqlite::params![
                        id.as_bytes().as_slice(),
                        new_pack.as_slice(),
                        *new as i64,
                        pack.as_slice(),
                        *old as i64
                    ])
                    .sq()?;
            }
            let left: bool = c
                .prepare_cached("SELECT EXISTS (SELECT 1 FROM object_index WHERE pack = ?1)")
                .sq()?
                .query_row([pack.as_slice()], |r| r.get(0))
                .sq()?;
            Ok((n, left))
        })
        .await?;
    drop(pin);
    let mut doomed = Vec::new();
    if !left {
        doomed.push(pack);
    }
    if n == 0 {
        doomed.push(new_pack);
    }
    delete_packs(store, doomed).await
}

/// Return freed database pages to the file system (see the module docs).
pub(crate) async fn reclaim(store: &LocalStore, exclusive: bool) -> Result<bool> {
    let (mode, free, pages) = store
        .db()
        .read(|c| {
            let mode: i64 = c.query_row("PRAGMA auto_vacuum", [], |r| r.get(0)).sq()?;
            let free: i64 = c.query_row("PRAGMA freelist_count", [], |r| r.get(0)).sq()?;
            let pages: i64 = c.query_row("PRAGMA page_count", [], |r| r.get(0)).sq()?;
            Ok((mode, free, pages))
        })
        .await?;
    const INCREMENTAL: i64 = 2;
    let mut vacuumed = false;
    if free > 0 && mode == INCREMENTAL {
        // Short steps between other writers' groups, as many as the free
        // pages of the start need (pages freed meanwhile wait for the next
        // run).
        let mut left = free;
        for _ in 0..=free / i64::from(VACUUM_STEP_PAGES) {
            if left <= 0 {
                break;
            }
            left = store
                .db()
                .write_raw(|c| {
                    // Stepped to the end: each step frees a page.
                    let mut st = c
                        .prepare(&format!("PRAGMA incremental_vacuum({VACUUM_STEP_PAGES})"))
                        .sq()?;
                    let mut rows = st.query([]).sq()?;
                    while rows.next().sq()?.is_some() {}
                    drop(rows);
                    drop(st);
                    c.query_row("PRAGMA freelist_count", [], |r| r.get::<_, i64>(0)).sq()
                })
                .await?;
            tokio::task::yield_now().await;
        }
        vacuumed = true;
    } else if free > 0 && exclusive && pages > 0 && free * 4 > pages {
        store
            .db()
            .write_raw(|c| c.execute_batch("PRAGMA auto_vacuum = INCREMENTAL; VACUUM").sq())
            .await?;
        vacuumed = true;
    }
    if exclusive {
        // Offline: leave the WAL empty, so the files' sizes are what the
        // data takes.
        store.db().write_raw(|c| c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").sq()).await?;
    }
    Ok(vacuumed)
}

#[cfg(test)]
mod tests {
    use ant_core::{Commit, Signature};

    use super::*;
    use crate::Backend;
    use crate::graph::{build_tree, write_commit};
    use crate::local::LocalOptions;

    fn noise(seed: u64, len: usize) -> Vec<u8> {
        let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    async fn commit(b: &Backend, files: &[(&str, &[u8])], parents: Vec<ObjectId>) -> ObjectId {
        let tree = build_tree(b.objects.as_ref(), files).await.unwrap();
        let sig = Signature {
            name: b"t".to_vec(),
            email: b"t@x".to_vec(),
            time: 1,
            tz_offset_minutes: 0,
        };
        let c = Commit {
            tree,
            parents,
            author: sig.clone(),
            committer: sig,
            extra_headers: vec![],
            message: b"m\n".to_vec(),
        };
        write_commit(b, &c).await.unwrap()
    }

    /// Everything written so far looks two days old.
    async fn age(s: &LocalStore) {
        s.db()
            .write(true, |c, _| {
                for t in ["objects", "object_index", "commits"] {
                    c.execute(&format!("UPDATE {t} SET written_at = written_at - 172800"), [])
                        .sq()?;
                }
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn present(b: &Backend, ids: &[ObjectId]) -> Vec<bool> {
        b.objects.get_many(ids).await.unwrap().iter().map(Option::is_some).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreachable_old_data_goes_and_everything_else_stays() {
        let dir = tempfile::tempdir().unwrap();
        let opts = LocalOptions::default();
        let (big, big2) = (noise(1, 200_000), noise(2, 300_000));
        let (kept, gone, mapped);
        let fresh_obj = ant_core::EncodedObject::new(ObjectKind::Blob, &noise(4, 50_000));
        {
            let s = LocalStore::open(dir.path(), &opts).unwrap();
            let b = s.backend();
            let c1 = commit(&b, &[("big", &big), ("a/small", b"small")], vec![]).await;
            kept = commit(&b, &[("big", &big), ("a/small", b"small2")], vec![c1]).await;
            b.refs.cas_ref("main", None, Some(kept)).await.unwrap();
            gone = commit(&b, &[("big2", &big2), ("a/small", b"orphan")], vec![c1]).await;
            mapped = commit(&b, &[("m", b"landed elsewhere")], vec![]).await;
            let value = bytes::Bytes::copy_from_slice(mapped.as_bytes());
            b.meta.put_derived("land/landed-as/v1", c1.as_bytes(), value).await.unwrap();
            let key = [gone.as_bytes().as_slice(), b"a/small"].concat();
            b.meta.put_derived("blame/v2", &key, "x".into()).await.unwrap();
            age(&s).await;
            // Written within the grace period: kept although unreachable.
            b.objects.put_many(std::slice::from_ref(&fresh_obj)).await.unwrap();
        }
        let gone_tree = {
            let s = LocalStore::open(dir.path(), &opts).unwrap();
            s.backend().meta.get_commits(&[gone]).await.unwrap()[0].clone().unwrap().tree
        };
        let big2_id = ant_core::hash_object(ObjectKind::Blob, &big2);
        let orphan_id = ant_core::hash_object(ObjectKind::Blob, b"orphan");
        let big_id = ant_core::hash_object(ObjectKind::Blob, &big);
        let grace = Duration::from_secs(86_400);
        let gc = GcOptions { grace, exclusive: true, ..GcOptions::default() };

        // A dry run counts and changes nothing.
        let s = LocalStore::open(dir.path(), &opts).unwrap();
        let dry = GcOptions { dry_run: true, ..gc.clone() };
        let r = collect(&s, &dry, &|_| {}).await.unwrap();
        assert_eq!((r.commits, r.large_objects), (1, 1), "{r:?}");
        assert!(r.objects >= 3, "orphan blob, trees, commit object: {r:?}");
        // The commit object and its record share an id.
        assert_eq!(r.condemned, r.objects + r.large_objects, "{r:?}");
        assert_eq!(present(&s.backend(), &[gone, big2_id]).await, [true, true]);

        let r = collect(&s, &gc, &|_| {}).await.unwrap();
        assert_eq!(r.missing, 0);
        assert_eq!((r.commits, r.changesets, r.large_objects, r.derived), (1, 1, 1, 1), "{r:?}");
        assert_eq!(r.kept, 0, "{r:?}");
        assert_eq!(r.packs_compacted, 1, "{r:#?}");
        drop(s);

        let s = LocalStore::open(dir.path(), &opts).unwrap();
        let b = s.backend();
        assert_eq!(present(&b, &[gone, gone_tree, big2_id, orphan_id]).await, [false; 4]);
        assert_eq!(b.meta.get_commits(&[gone]).await.unwrap(), vec![None]);
        assert_eq!(b.meta.get_changeset(&gone).await.unwrap(), None);
        assert_eq!(present(&b, &[kept, mapped, big_id, fresh_obj.id()]).await, [true; 4]);
        let blob = b.objects.get_many(&[big_id]).await.unwrap().pop().flatten().unwrap();
        assert_eq!(blob.body(), &big[..]);
        assert!(b.meta.get_commits(&[kept]).await.unwrap()[0].is_some());
        // The compacted pack is gone; its live records moved.
        let (_, pack_bytes) = s.disk_usage().unwrap();
        assert!(pack_bytes < 300_000, "{pack_bytes}");
        // A second run finds nothing more.
        let r = collect(&s, &gc, &|_| {}).await.unwrap();
        assert_eq!((r.objects, r.large_objects, r.commits, r.packs), (0, 0, 0, 0), "{r:?}");
    }

    /// `missing` counts what published roots reach: a recent commit record
    /// whose tree is not stored (a land batch whose predecessor's writes
    /// are in flight, a writer that stopped) is not damage; a ref to it is.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_counts_only_what_published_roots_reach() {
        let dir = tempfile::tempdir().unwrap();
        let s = LocalStore::open(dir.path(), &LocalOptions::default()).unwrap();
        let b = s.backend();
        let base = commit(&b, &[("f", b"x")], vec![]).await;
        b.refs.cas_ref("main", None, Some(base)).await.unwrap();
        let node = crate::CommitNode {
            id: ObjectId([8; 32]),
            tree: ObjectId([7; 32]),
            parents: vec![base],
            generation: 2,
            committer_time: 1,
        };
        b.meta.put_commit(node.clone(), vec![]).await.unwrap();
        let dry = GcOptions { dry_run: true, exclusive: true, ..GcOptions::default() };
        assert_eq!(collect(&s, &dry, &|_| {}).await.unwrap().missing, 0);
        b.refs.cas_ref("main", Some(base), Some(node.id)).await.unwrap();
        assert_eq!(collect(&s, &dry, &|_| {}).await.unwrap().missing, 1, "the tree a ref reaches");
    }

    #[test]
    fn reports_round_trip_through_their_text_form() {
        let r = GcReport {
            dry_run: true,
            cutoff: 1_700_000_000,
            live: 5,
            condemned: 7,
            kept: 1,
            objects: 2,
            object_bytes: 300,
            packs_dead_bytes: 9,
            vacuumed: true,
            ..GcReport::default()
        };
        assert_eq!(GcReport::decode(&r.encode()), r);
        assert_eq!(
            GcReport::decode("live=3 bogus=1 x"),
            GcReport { live: 3, ..GcReport::default() }
        );
    }
}
