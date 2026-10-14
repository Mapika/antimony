//! Garbage collection of unreachable data (`ant admin gc`).
//!
//! Aborted land batches, abandoned changesets, failed imports and retried
//! uploads leave objects, commit records, changesets and S3 packs that
//! nothing references. A collection run deletes them; it is safe to run
//! while any number of server replicas keep writing, and it prefers leaving
//! garbage over any risk to reachable data.
//!
//! # What is kept (roots)
//!
//! Everything reachable (commit → parents and tree, tree → entries;
//! gitlinks are not followed) from:
//! - every ref target;
//! - every history-index watermark, and (at the start of a run) every commit
//!   in the history index;
//! - both sides of the derived rows of [`GcOptions::root_kinds`]: landed-as
//!   mappings (a land retry answers from them) and the git-import commit
//!   mapping (a re-import resumes from it);
//! - everything written within the grace period ([`GcOptions::grace`],
//!   default 7 days, at least [`MIN_GRACE`]): objects, packed-object index
//!   rows, commit records and changesets carry their write time
//!   (`written_at`), and every one written after the run's cutoff
//!   (`started_at - grace`) is a root. That covers changesets a client has
//!   created but not landed yet, blobs uploaded for a changeset to come,
//!   speculative land batches and imports in progress. A changeset commit
//!   that was never landed is collected once it is older than the grace
//!   period; landing it afterwards fails with "not found".
//!
//! # A run
//!
//! 1. **Mark.** The closure of the roots is collected in `gc_live` (an
//!    unlogged working table, so client memory stays bounded however large
//!    the store): batches of unexpanded ids are read (commit records from
//!    `commits`, everything else through the object store) and their
//!    children inserted. Ids written since the mark began are marked again
//!    right before step 2.
//! 2. **Condemn.** Every id not marked whose rows were all written before
//!    the cutoff is inserted into `gc_condemned`, one id-range chunk per
//!    statement; its object and index rows are locked and re-checked while
//!    doing so (see *Re-storing*). From that moment readers treat condemned
//!    ids as missing (`get_many`, `has_many`, the packed-object index,
//!    `get_commits`, `get_changesets`, `first_parent_chain`), object and
//!    index rows only while they are older than the cutoff, i.e. exactly
//!    those the sweep would delete: no operation that starts later can read
//!    a condemned object and build on it.
//! 3. **Quiesce.** Wait [`GcOptions::quiesce`] (default 30 minutes): longer
//!    than the servers' cache lifetime (`ant-server` caches immutable data
//!    for at most 10 minutes per entry) plus the longest operation. After
//!    it, no process holds knowledge of a condemned object obtained before
//!    step 2.
//! 4. **Re-mark.** The roots are read again, and everything written since
//!    the mark began (minus a margin for open transactions) is marked; ids
//!    reached this way leave `gc_condemned`.
//! 5. **Sweep.** Batches of condemned ids are deleted, each in one
//!    transaction: objects, index rows, commit records with their log,
//!    changesets and legacy `changes` rows, and blame cache rows of deleted
//!    commits. Zone tags and uploader records (`acl/...`) are kept: a writer
//!    tags an object before storing it, so deleting tags could leave a
//!    re-stored restricted object untagged; they are a few bytes per object.
//! 6. **Packs.** S3 packs none of whose objects are still indexed, and that
//!    are older than the grace period, are deleted. Packs with only some
//!    dead objects are kept (no repacking); their dead bytes are reported.
//!
//! Interrupting a run at any point is safe; the next run resumes it from
//! the phase recorded in `gc_state` (the mark working set is rebuilt if a
//! database crash emptied it). A dry run marks into a temporary table and
//! changes nothing else.
//!
//! # Why nothing reachable is deleted
//!
//! Let K be the condemned set. Deleting an id is safe when no reachable
//! object refers to it now or can come to refer to it. The argument:
//!
//! - *Invariant I.* After step 4, for every stored, non-condemned object X,
//!   no id in closure(X) is condemned. At condemnation, K = (old rows) \
//!   closure(roots ∪ rows written since the cutoff), so objects that existed
//!   then and are not condemned have their closure marked. Objects written
//!   after the mark began are marked in step 4, which removes their closure
//!   from K. Objects written after step 4 come from operations that started
//!   after step 2 (step 3 outlasts every operation), which reference only
//!   objects they read (never condemned ones the sweep could delete: those
//!   read as missing; a condemned id whose row was written since the
//!   cutoff is never deleted by this run and, written after the mark
//!   began, is marked with its closure in step 4) or their children, or
//!   objects they write themselves; by induction their closure avoids K.
//! - *Re-storing.* Writers deduplicate (`ON CONFLICT DO NOTHING`), so an
//!   object about to be deleted may be "stored" again by a writer that then
//!   refers to it. Storing an existing object therefore refreshes a write
//!   time older than [`FRESHEN_AFTER`] and takes the id back from
//!   `gc_condemned`, both in one transaction; objects not refreshed were
//!   written less than [`FRESHEN_AFTER`] ago (the insert statement itself
//!   reads which existing rows are that young, so the common case costs no
//!   extra statement). Either way the row is younger than any run's cutoff
//!   (grace > [`FRESHEN_AFTER`]). The sweep deletes a row only if its write
//!   time is older than the cutoff *at delete time* (the predicate is
//!   re-evaluated under the row lock, so refresh and delete serialize), and
//!   a writer whose refresh finds the row gone inserts it again. So
//!   `put_many` returning means the objects exist and outlive any run that
//!   could delete them.
//! - *Read your writes.* Once `put_many` (or `index_objects`, which backs
//!   the packed store) returns, every read finds the objects until a later
//!   run collects them legitimately. Two independent reasons:
//!   1. Readers hide a condemned object or index row only while its write
//!      time is older than the run's cutoff (`gc_state.cutoff`), and the
//!      row a returned `put_many` leaves is younger than that.
//!   2. No id with a row younger than the cutoff stays condemned once the
//!      write that made the row young has committed. A condemn chunk locks
//!      the object and index rows it condemns (`FOR SHARE SKIP LOCKED`, in
//!      the condemning statement), which re-evaluates "written before the
//!      cutoff" on the row's current version, and condemns an id only if
//!      every such row of it passed. A refresh (`FOR UPDATE`) either locks
//!      the row first, and the chunk skips it (refresh in flight, or
//!      committed: the re-check fails), or waits for the chunk to commit;
//!      the take-back that follows in the writer's transaction, a separate
//!      statement and so a later snapshot, then removes the id. Refresh and
//!      take-back commit together, so another writer whose insert finds the
//!      row young (and skips both) cannot see it still condemned. The one
//!      exception is an id condemned for an old commit record or changeset
//!      while its object row did not exist yet (a changeset retried long
//!      after its objects write failed): reason 1 covers its reads, and the
//!      clean-up at the end of the condemn step takes it back.
//! - *Blind ref moves.* `cas_ref` does not check that its target exists
//!   (open mode, admins). Each sweep batch takes the collector's fence
//!   exclusively, which waits for ref moves in flight (they hold it shared
//!   until they commit), then checks that no ref points at a condemned id;
//!   if one does, the batch is abandoned and the roots are marked again
//!   first.
//! - *Packs.* New packs get random ids, so a pack is only ever indexed by
//!   the upload that wrote it, within one operation. A pack with no index
//!   rows that is older than the grace period can therefore gain none.
//!
//! What the argument assumes: clients refer to stored objects only through
//! validated server operations (changesets, lands, ref moves under a policy)
//! or within the grace period of learning them; a client that keeps an id
//! for longer and pushes raw trees or commits naming it (admin
//! `PutObjects`), or moves a ref to a commit unreachable for longer than the
//! grace period at the instant a sweep batch deletes it, can end up
//! referring to deleted data. Periodic re-marks during the sweep narrow that
//! window further. Collect only with every replica running a version that
//! writes `written_at` (schema version 5 or later, which older binaries
//! refuse to open).

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::time::{Duration, Instant};

use ant_core::{EntryMode, ObjectId, ObjectKind};
use sqlx::Row;
use sqlx::postgres::PgRow;

use super::{PgStore, be, ids_to_vecs, object_id_col};
use crate::s3::S3ObjectStore;
use crate::{ObjectStore, Result, StoreError};

/// Shortest grace period a run accepts.
pub const MIN_GRACE: Duration = Duration::from_secs(24 * 3600);
/// Default grace period.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(7 * 24 * 3600);
/// Default wait between condemning and deleting (see the module docs).
pub const DEFAULT_QUIESCE: Duration = Duration::from_secs(30 * 60);
/// Storing an object that already exists refreshes its write time when it is
/// older than this.
pub const FRESHEN_AFTER: Duration = Duration::from_secs(3600);
/// Derived-data kinds whose keys and 32-byte values are roots: landed-as
/// mappings (`ant-server` `ops::land`) and the git-import commit mapping
/// (`ant-gitbridge`).
pub const DEFAULT_ROOT_KINDS: [&str; 2] = ["land/landed-as/v1", "git-import/commit/v1"];
/// Derived-data kinds keyed by `<commit id><suffix>` whose rows go with their
/// commit: the blame cache (`ant-server` `ops::history`).
pub const DEFAULT_COMMIT_KEYED_KINDS: [&str; 1] = ["blame/v1"];

/// Attempts at storing objects that a concurrent collection keeps deleting.
pub(super) const STORE_ATTEMPTS: usize = 8;
/// Extra look-back of "written since" scans, for transactions that were
/// still open when the previous scan ran (`written_at` is a transaction's
/// start time).
const FINAL_MARGIN: Duration = Duration::from_secs(3600);
const SWEEP_MARGIN: Duration = Duration::from_secs(120);
/// How often the sweep marks recent writes again.
const SWEEP_REMARK_EVERY: Duration = Duration::from_secs(10);
/// Id-range chunks of the condemn step (by the first id byte).
const CONDEMN_CHUNKS: u16 = 64;

const KIND_UNKNOWN: i16 = 0;
const KIND_COMMIT: i16 = 1;
const KIND_TREE: i16 = 2;
const KIND_BLOB: i16 = 3;

tokio::task_local! {
    static UNHIDDEN: ();
}

/// Whether readers on this task hide condemned ids (everywhere but inside a
/// collection run's own reads).
pub(super) fn hiding() -> bool {
    UNHIDDEN.try_with(|_| ()).is_err()
}

/// Run `fut` with condemned ids visible to this store's readers (the
/// collector must read what it may un-condemn).
async fn unhidden<F: Future>(fut: F) -> F::Output {
    UNHIDDEN.scope((), fut).await
}

/// Advisory-lock key of the fence between ref moves and sweep batches.
pub(super) fn fence_key(schema: &str) -> String {
    format!("ant-gc-fence/{schema}")
}

/// Advisory-lock key held by the one collection run of a schema.
fn run_key(schema: &str) -> String {
    format!("ant-gc/{schema}")
}

pub(super) fn lost_race() -> StoreError {
    StoreError::backend(std::io::Error::other(
        "objects kept being deleted by garbage collection while being stored",
    ))
}

/// Tables `PgStore::refresh_existing` refreshes.
#[derive(Debug, Clone, Copy)]
pub(super) enum Table {
    Objects,
    ObjectIndex,
}

/// Settings of one collection run.
#[derive(Clone)]
pub struct GcOptions {
    /// Rows written less than this long before the run started are kept,
    /// with everything they reach. At least [`MIN_GRACE`].
    pub grace: Duration,
    /// Wait between condemning and deleting: must exceed every server's
    /// cache lifetime plus its longest operation.
    pub quiesce: Duration,
    /// Mark and count only; no repository data or GC state is changed.
    pub dry_run: bool,
    /// See [`DEFAULT_ROOT_KINDS`].
    pub root_kinds: Vec<String>,
    /// See [`DEFAULT_COMMIT_KEYED_KINDS`].
    pub commit_keyed_kinds: Vec<String>,
    /// Ids per mark read and per sweep transaction.
    pub batch: usize,
    /// S3 store holding the packs (`None`: Postgres-only store).
    pub s3: Option<S3ObjectStore>,
    /// Packs uploaded less than this long ago are never deleted (their index
    /// rows follow the upload within one operation). Default: `grace`.
    pub pack_min_age: Option<Duration>,
    /// Stop (resumably) after this many sweep batches.
    pub stop_after_batches: Option<usize>,
}

impl Default for GcOptions {
    fn default() -> Self {
        Self {
            grace: DEFAULT_GRACE,
            quiesce: DEFAULT_QUIESCE,
            dry_run: false,
            root_kinds: DEFAULT_ROOT_KINDS.iter().map(|k| k.to_string()).collect(),
            commit_keyed_kinds: DEFAULT_COMMIT_KEYED_KINDS.iter().map(|k| k.to_string()).collect(),
            batch: 2000,
            s3: None,
            pack_min_age: None,
            stop_after_batches: None,
        }
    }
}

/// Counts and bytes of a run (cumulative over resumed parts). In a dry run
/// the deletion counters say what a run would delete.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcReport {
    pub dry_run: bool,
    /// The run finished (false: stopped early, resume with another run).
    pub finished: bool,
    /// This invocation resumed an interrupted run.
    pub resumed: bool,
    /// Rows written before this time (Postgres `timestamptz` text) were
    /// candidates.
    pub cutoff: String,
    /// Ids marked reachable.
    pub live: u64,
    /// Ids reachable but missing from the store (a store damaged earlier).
    pub missing: u64,
    /// Ids condemned (dry run: that would be).
    pub condemned: u64,
    /// Small objects (Postgres) deleted: count, stored (compressed) bytes.
    pub objects: u64,
    pub object_bytes: u64,
    /// Packed large objects (index rows) deleted: count, packed bytes.
    pub large_objects: u64,
    pub large_object_bytes: u64,
    /// Commit records deleted (with their log rows).
    pub commits: u64,
    /// Changesets deleted: count, stored bytes.
    pub changesets: u64,
    pub changeset_bytes: u64,
    /// Legacy per-path `changes` rows deleted.
    pub legacy_changes: u64,
    /// Derived rows deleted (blame cache of deleted commits).
    pub derived: u64,
    /// Condemned ids kept after all (written again, or locked by a writer).
    pub kept: u64,
    /// S3 packs deleted: count, bytes.
    pub packs: u64,
    pub pack_bytes: u64,
    /// Packs kept, their total size and the bytes in them no index row
    /// points at (dead objects, or copies of objects stored twice).
    pub packs_kept: u64,
    pub packs_kept_bytes: u64,
    pub packs_dead_bytes: u64,
}

impl GcReport {
    /// Share of the kept packs' bytes that is dead.
    pub fn pack_dead_ratio(&self) -> f64 {
        if self.packs_kept_bytes == 0 {
            0.0
        } else {
            self.packs_dead_bytes as f64 / self.packs_kept_bytes as f64
        }
    }

    fn fields(&mut self) -> [(&'static str, &mut u64); 18] {
        [
            ("live", &mut self.live),
            ("missing", &mut self.missing),
            ("condemned", &mut self.condemned),
            ("objects", &mut self.objects),
            ("object_bytes", &mut self.object_bytes),
            ("large_objects", &mut self.large_objects),
            ("large_object_bytes", &mut self.large_object_bytes),
            ("commits", &mut self.commits),
            ("changesets", &mut self.changesets),
            ("changeset_bytes", &mut self.changeset_bytes),
            ("legacy_changes", &mut self.legacy_changes),
            ("derived", &mut self.derived),
            ("kept", &mut self.kept),
            ("packs", &mut self.packs),
            ("pack_bytes", &mut self.pack_bytes),
            ("packs_kept", &mut self.packs_kept),
            ("packs_kept_bytes", &mut self.packs_kept_bytes),
            ("packs_dead_bytes", &mut self.packs_dead_bytes),
        ]
    }

    /// `key=value` pairs, as persisted in `gc_state.stats`.
    fn encode(&mut self) -> String {
        self.fields().into_iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(" ")
    }

    fn decode_into(&mut self, s: &str) {
        let values: HashMap<&str, u64> = s
            .split_whitespace()
            .filter_map(|kv| kv.split_once('='))
            .filter_map(|(k, v)| Some((k, v.parse().ok()?)))
            .collect();
        for (k, v) in self.fields() {
            if let Some(x) = values.get(k) {
                *v = *x;
            }
        }
    }
}

type Conn = sqlx::pool::PoolConnection<sqlx::Postgres>;

fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

/// Child ids of an encoded tree body, with their mark kinds. Parsed without
/// the canonical-order checks of `Tree::decode`: the collector must see the
/// children of whatever is stored. Gitlinks (other repositories) are skipped.
fn tree_children(body: &[u8], out: &mut Vec<(ObjectId, i16)>) -> Result<()> {
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
        // Legacy spellings ("040000", "40755", ...) behave like their class;
        // an unparsable mode is treated as a blob so its target stays alive.
        match EntryMode::from_octal(mode).map(EntryMode::class) {
            Ok(EntryMode::Tree) => out.push((id, KIND_TREE)),
            Ok(EntryMode::Gitlink) => {}
            _ => out.push((id, KIND_BLOB)),
        }
    }
    Ok(())
}

/// Tree and parent ids of an encoded commit body (header lines only).
fn commit_children(body: &[u8], out: &mut Vec<(ObjectId, i16)>) -> Result<()> {
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

impl PgStore {
    /// After the insert of `ids` into `table` (`settled`: the ids it
    /// inserted, plus those its snapshot already held with a write time
    /// younger than [`FRESHEN_AFTER`]): for the other ids (they existed with
    /// an old write time, or were inserted concurrently after the snapshot),
    /// refresh a write time older than [`FRESHEN_AFTER`] and take them back
    /// from `gc_condemned`, in one transaction. Returns the ids that no
    /// longer exist (a collection deleted them after the insert saw them):
    /// store those again.
    ///
    /// Young existing rows need neither step: no run deletes them (their
    /// write time is after any run's cutoff, and only grows), readers never
    /// hide them, and none is condemned once the write that made it young
    /// has committed (module docs, *Read your writes*). The common case of
    /// storing objects that were just written (a land rebuilding trees a
    /// changeset wrote) therefore costs no statement beyond the insert.
    ///
    /// Rows are locked in id order and a collection never waits for a
    /// writer's row lock (`SKIP LOCKED`), so this cannot deadlock with it.
    pub(super) async fn refresh_existing(
        &self,
        table: Table,
        ids: &[&[u8]],
        settled: &[PgRow],
    ) -> Result<HashSet<Vec<u8>>> {
        if settled.len() >= ids.len() {
            return Ok(HashSet::new());
        }
        let settled: HashSet<Vec<u8>> = settled
            .iter()
            .map(|r| r.try_get::<Vec<u8>, _>("id"))
            .collect::<std::result::Result<_, _>>()
            .map_err(be)?;
        let existing: Vec<&[u8]> =
            ids.iter().copied().filter(|id| !settled.contains(*id)).collect();
        if existing.is_empty() {
            return Ok(HashSet::new());
        }
        let sql = match table {
            Table::Objects => {
                "WITH old AS (SELECT id FROM objects WHERE id = ANY($1) \
                         AND written_at < now() - make_interval(secs => $2) \
                         ORDER BY id FOR UPDATE), \
                 upd AS (UPDATE objects o SET written_at = now() FROM old \
                         WHERE o.id = old.id RETURNING o.id) \
                 SELECT id FROM upd \
                 UNION ALL \
                 SELECT id FROM objects WHERE id = ANY($1) \
                   AND written_at >= now() - make_interval(secs => $2)"
            }
            Table::ObjectIndex => {
                "WITH old AS (SELECT id FROM object_index WHERE id = ANY($1) \
                         AND written_at < now() - make_interval(secs => $2) \
                         ORDER BY id FOR UPDATE), \
                 upd AS (UPDATE object_index o SET written_at = now() FROM old \
                         WHERE o.id = old.id RETURNING o.id) \
                 SELECT id FROM upd \
                 UNION ALL \
                 SELECT id FROM object_index WHERE id = ANY($1) \
                   AND written_at >= now() - make_interval(secs => $2)"
            }
        };
        // Refresh and take-back commit together, so no reader ever sees the
        // refreshed row still condemned. The take-back is a statement of its
        // own (READ COMMITTED: a snapshot taken after the refresh's lock
        // waits): a condemn chunk that locked a row first made the refresh
        // wait for it to commit, and the take-back sees its ids; one that
        // locks later sees the refresh and skips the row (`condemn_chunk!`).
        let mut tx = self.begin_write(self.objects_deferred()).await?;
        self.count_write();
        let present = sqlx::query(sql)
            .bind(&existing)
            .bind(secs(FRESHEN_AFTER))
            .fetch_all(&mut *tx)
            .await
            .map_err(be)?;
        let present: HashSet<Vec<u8>> = present
            .iter()
            .map(|r| r.try_get::<Vec<u8>, _>("id"))
            .collect::<std::result::Result<_, _>>()
            .map_err(be)?;
        self.count_write();
        sqlx::query("DELETE FROM gc_condemned WHERE id = ANY($1)")
            .bind(&existing)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        tx.commit().await.map_err(be)?;
        Ok(existing.into_iter().filter(|id| !present.contains(*id)).map(<[u8]>::to_vec).collect())
    }

    /// The ids among `ids` that a collection run has condemned (none while
    /// readers do not hide them).
    pub(crate) async fn condemned_among(&self, ids: &[ObjectId]) -> Result<HashSet<ObjectId>> {
        if ids.is_empty() || !hiding() {
            return Ok(HashSet::new());
        }
        let rows = sqlx::query("SELECT id FROM gc_condemned WHERE id = ANY($1)")
            .bind(ids_to_vecs(ids))
            .fetch_all(&self.pool)
            .await
            .map_err(be)?;
        rows.iter().map(|r| object_id_col(r, "id")).collect()
    }

    /// Forget an interrupted run: its condemned ids become visible again and
    /// nothing more is deleted. Returns whether a run was pending.
    pub async fn gc_abort(&self) -> Result<bool> {
        let mut conn = self.pool.acquire().await.map_err(be)?;
        conn.close_on_drop();
        lock_run(&mut conn, &self.schema).await?;
        let pending: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM gc_state WHERE phase <> 'done') \
             OR EXISTS (SELECT 1 FROM gc_condemned)",
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(be)?;
        sqlx::raw_sql(
            "DELETE FROM gc_condemned; \
             UPDATE gc_state SET phase = 'done', finished_at = now(); \
             TRUNCATE gc_live",
        )
        .execute(&mut *conn)
        .await
        .map_err(be)?;
        Ok(pending)
    }

    /// Run (or resume) a garbage collection; see the module docs. `objects`
    /// is the repository's object store (Postgres, or tiered with S3 packs),
    /// used to read trees and commits while marking. `progress` receives a
    /// line per phase.
    pub async fn gc(
        &self,
        objects: &dyn ObjectStore,
        opts: &GcOptions,
        progress: &(dyn Fn(&str) + Send + Sync),
    ) -> Result<GcReport> {
        if opts.grace < MIN_GRACE {
            return Err(StoreError::InvalidArgument(format!(
                "gc grace period must be at least {} hours",
                MIN_GRACE.as_secs() / 3600
            )));
        }
        let mut conn = self.pool.acquire().await.map_err(be)?;
        // The run lock and a dry run's temporary table end with the session.
        conn.close_on_drop();
        lock_run(&mut conn, &self.schema).await?;
        let mut run = Run {
            store: self,
            conn,
            objects,
            opts,
            progress,
            report: GcReport { dry_run: opts.dry_run, ..GcReport::default() },
            cutoff: 0.0,
        };
        if opts.dry_run { run.dry().await } else { run.real().await }?;
        // Release now rather than when the closing session ends.
        sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
            .bind(run_key(&self.schema))
            .execute(&mut *run.conn)
            .await
            .map_err(be)?;
        Ok(run.report)
    }
}

/// Take the run lock (a session-level advisory lock, released when the
/// session closes). A previous run's session may still be closing: wait a
/// moment before reporting another run.
async fn lock_run(conn: &mut Conn, schema: &str) -> Result<()> {
    let mut locked = false;
    for attempt in 0..20 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        locked = sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1, 0))")
            .bind(run_key(schema))
            .fetch_one(&mut **conn)
            .await
            .map_err(be)?;
        if locked {
            break;
        }
    }
    if !locked {
        return Err(StoreError::InvalidArgument(format!(
            "another garbage collection of schema \"{schema}\" is running"
        )));
    }
    Ok(())
}

/// State of one invocation.
struct Run<'a> {
    store: &'a PgStore,
    conn: Conn,
    objects: &'a dyn ObjectStore,
    opts: &'a GcOptions,
    progress: &'a (dyn Fn(&str) + Send + Sync),
    report: GcReport,
    /// The run's cutoff, as Unix seconds (Postgres clock).
    cutoff: f64,
}

/// Root sources (all `SELECT id, kind`). `$1`: root kinds, `$2`: since (Unix
/// seconds) where used.
const ROOTS_REFS: &str = "SELECT target, 0::smallint FROM refs";
const ROOTS_WATERMARKS: &str = "SELECT commit_id, 1::smallint FROM history_watermark";
const ROOTS_HISTORY: &str = "SELECT DISTINCT commit_id, 1::smallint FROM history";
const ROOTS_DERIVED: &str = "SELECT key, 0::smallint FROM derived \
     WHERE kind = ANY($1) AND length(key) = 32 \
     UNION SELECT value, 0::smallint FROM derived \
     WHERE kind = ANY($1) AND length(value) = 32";
const ROOTS_FRESH_COMMITS: &str = "SELECT id, 1::smallint FROM commits \
     WHERE written_at >= to_timestamp($2) \
     UNION ALL SELECT commit_id, 1::smallint FROM changesets \
     WHERE written_at >= to_timestamp($2)";
const ROOTS_FRESH_OBJECTS: &str = "SELECT id, 0::smallint FROM objects o \
     WHERE written_at >= to_timestamp($2) \
       AND NOT EXISTS (SELECT 1 FROM gc_live l WHERE l.id = o.id) \
     UNION ALL SELECT id, 0::smallint FROM object_index x \
     WHERE written_at >= to_timestamp($2) \
       AND NOT EXISTS (SELECT 1 FROM gc_live l WHERE l.id = x.id)";

impl Run<'_> {
    fn say(&self, line: &str) {
        (self.progress)(line);
    }

    async fn now(&mut self) -> Result<f64> {
        sqlx::query_scalar("SELECT extract(epoch FROM now())::float8")
            .fetch_one(&mut *self.conn)
            .await
            .map_err(be)
    }

    async fn timestamp_text(&mut self, unix: f64) -> Result<String> {
        sqlx::query_scalar("SELECT to_timestamp($1)::text")
            .bind(unix)
            .fetch_one(&mut *self.conn)
            .await
            .map_err(be)
    }

    // -- marking ----------------------------------------------------------

    /// Insert the ids `source` selects into the mark set (taking newly
    /// marked ids back from `gc_condemned` in a real run). Returns how many
    /// were new.
    async fn add_roots(&mut self, source: &str, since: f64) -> Result<u64> {
        let uncondemn = if self.opts.dry_run {
            ""
        } else {
            ", unc AS (DELETE FROM gc_condemned g USING ins WHERE g.id = ins.id)"
        };
        let stmt = format!(
            "WITH src(id, kind) AS ({source}), \
             ins AS (INSERT INTO gc_live (id, kind, expanded) \
                     SELECT id, kind, false FROM src WHERE id IS NOT NULL \
                     ON CONFLICT (id) DO NOTHING RETURNING id){uncondemn} \
             SELECT count(*) FROM ins"
        );
        let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(stmt))
            .bind(&self.opts.root_kinds)
            .bind(since)
            .fetch_one(&mut *self.conn)
            .await
            .map_err(be)?;
        Ok(n as u64)
    }

    /// Mark the closure of everything marked but not yet expanded.
    async fn expand(&mut self) -> Result<()> {
        let batch = self.opts.batch.max(1) as i64;
        loop {
            let rows = sqlx::query("SELECT id, kind FROM gc_live WHERE NOT expanded LIMIT $1")
                .bind(batch)
                .fetch_all(&mut *self.conn)
                .await
                .map_err(be)?;
            if rows.is_empty() {
                return Ok(());
            }
            let mut todo: Vec<(ObjectId, i16)> = Vec::with_capacity(rows.len());
            for r in &rows {
                todo.push((object_id_col(r, "id")?, r.try_get("kind").map_err(be)?));
            }
            let mut children: Vec<(ObjectId, i16)> = Vec::new();
            // Commit records give tree and parents without reading objects.
            let maybe_commits: Vec<ObjectId> = todo
                .iter()
                .filter(|(_, k)| matches!(*k, KIND_COMMIT | KIND_UNKNOWN))
                .map(|(id, _)| *id)
                .collect();
            let mut recorded: HashSet<ObjectId> = HashSet::new();
            if !maybe_commits.is_empty() {
                let rows = sqlx::query("SELECT id, tree, parents FROM commits WHERE id = ANY($1)")
                    .bind(ids_to_vecs(&maybe_commits))
                    .fetch_all(&mut *self.conn)
                    .await
                    .map_err(be)?;
                for r in &rows {
                    recorded.insert(object_id_col(r, "id")?);
                    children.push((object_id_col(r, "tree")?, KIND_TREE));
                    let parents: Vec<u8> = r.try_get("parents").map_err(be)?;
                    for p in super::decode_ids(&parents)? {
                        children.push((p, KIND_COMMIT));
                    }
                }
            }
            let read: Vec<ObjectId> = todo
                .iter()
                .filter(|(id, k)| *k != KIND_BLOB && !recorded.contains(id))
                .map(|(id, _)| *id)
                .collect();
            if !read.is_empty() {
                let found = unhidden(self.objects.get_many(&read)).await?;
                for (id, object) in read.iter().zip(found) {
                    let Some(object) = object else {
                        self.report.missing += 1;
                        tracing::debug!(%id, "gc: reachable object missing");
                        continue;
                    };
                    match object.kind() {
                        ObjectKind::Tree => tree_children(object.body(), &mut children),
                        ObjectKind::Commit => commit_children(object.body(), &mut children),
                        ObjectKind::Blob => Ok(()),
                    }
                    .map_err(|e| {
                        StoreError::InvalidArgument(format!("{e} ({id}); nothing was deleted"))
                    })?;
                }
            }
            self.insert_marks(&children).await?;
            sqlx::query("UPDATE gc_live SET expanded = true WHERE id = ANY($1)")
                .bind(todo.iter().map(|(id, _)| id.as_bytes().to_vec()).collect::<Vec<_>>())
                .execute(&mut *self.conn)
                .await
                .map_err(be)?;
        }
    }

    async fn insert_marks(&mut self, children: &[(ObjectId, i16)]) -> Result<()> {
        if children.is_empty() {
            return Ok(());
        }
        let uncondemn = if self.opts.dry_run {
            ""
        } else {
            ", unc AS (DELETE FROM gc_condemned g USING ins WHERE g.id = ins.id)"
        };
        let stmt = format!(
            "WITH ins AS (INSERT INTO gc_live (id, kind, expanded) \
                 SELECT * FROM unnest($1::bytea[], $2::smallint[], $3::bool[]) \
                 ON CONFLICT (id) DO NOTHING RETURNING id){uncondemn} \
             SELECT count(*) FROM ins"
        );
        let ids: Vec<&[u8]> = children.iter().map(|(id, _)| id.as_bytes().as_slice()).collect();
        let kinds: Vec<i16> = children.iter().map(|(_, k)| *k).collect();
        // Blobs have no children: never expanded.
        let done: Vec<bool> = kinds.iter().map(|k| *k == KIND_BLOB).collect();
        sqlx::query(sqlx::AssertSqlSafe(stmt))
            .bind(&ids)
            .bind(&kinds)
            .bind(&done)
            .execute(&mut *self.conn)
            .await
            .map_err(be)?;
        Ok(())
    }

    /// Mark from the roots. `all_history`: also every commit of the history
    /// index (the first mark of a run). `since`: rows written at or after it
    /// are roots.
    async fn mark(&mut self, all_history: bool, since: f64) -> Result<()> {
        let mut added = self.add_roots(ROOTS_REFS, since).await?;
        added += self.add_roots(ROOTS_WATERMARKS, since).await?;
        if all_history {
            added += self.add_roots(ROOTS_HISTORY, since).await?;
        }
        added += self.add_roots(ROOTS_DERIVED, since).await?;
        added += self.add_roots(ROOTS_FRESH_COMMITS, since).await?;
        self.expand().await?;
        // Objects written recently that no commit reaches yet (uploads, a
        // batch in flight): their kind is unknown until read.
        added += self.add_roots(ROOTS_FRESH_OBJECTS, since).await?;
        self.expand().await?;
        tracing::debug!(added, "gc: marked roots");
        Ok(())
    }

    async fn count_live(&mut self) -> Result<u64> {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM gc_live")
            .fetch_one(&mut *self.conn)
            .await
            .map_err(be)?;
        Ok(n as u64)
    }

    /// Condemn (dry run: count) every row written before the cutoff that is
    /// not marked, one id range at a time.
    async fn condemn(&mut self) -> Result<()> {
        let stmt = if self.opts.dry_run { CONDEMN_COUNT } else { CONDEMN_CHUNK };
        let step = 256 / CONDEMN_CHUNKS;
        let mut totals = [0u64; 8];
        for chunk in 0..CONDEMN_CHUNKS {
            let lo = vec![(chunk * step) as u8];
            let hi = (chunk + 1 < CONDEMN_CHUNKS).then(|| vec![((chunk + 1) * step) as u8]);
            let row = sqlx::query(stmt)
                .bind(&lo)
                .bind(&hi)
                .bind(self.cutoff)
                .bind(!self.opts.dry_run)
                .fetch_one(&mut *self.conn)
                .await
                .map_err(be)?;
            for (i, total) in totals.iter_mut().enumerate() {
                *total += row.try_get::<i64, _>(i).map_err(be)? as u64;
            }
        }
        let [ids, objects, object_bytes, index, index_bytes, commits, changesets, changeset_bytes] =
            totals;
        if !self.opts.dry_run {
            // Defence in depth: ids with an object or index row written since
            // the cutoff are not condemned (`condemn_chunk!` re-checks
            // under row locks, and writers take refreshed ids back in the
            // refresh's transaction), except when the row did not exist yet
            // for a chunk that condemned the id for an old commit record or
            // changeset (a retried changeset whose first objects write
            // failed). Readers do not hide such rows meanwhile (they hide
            // only rows older than the cutoff).
            let back = sqlx::query(
                "DELETE FROM gc_condemned g WHERE \
                 EXISTS (SELECT 1 FROM objects o WHERE o.id = g.id \
                         AND o.written_at >= to_timestamp($1)) \
                 OR EXISTS (SELECT 1 FROM object_index x WHERE x.id = g.id \
                            AND x.written_at >= to_timestamp($1))",
            )
            .bind(self.cutoff)
            .execute(&mut *self.conn)
            .await
            .map_err(be)?
            .rows_affected();
            self.report.condemned += ids.saturating_sub(back);
        } else {
            self.report.condemned += ids;
        }
        if self.opts.dry_run {
            let r = &mut self.report;
            (r.objects, r.object_bytes) = (objects, object_bytes);
            (r.large_objects, r.large_object_bytes) = (index, index_bytes);
            r.commits = commits;
            (r.changesets, r.changeset_bytes) = (changesets, changeset_bytes);
        }
        Ok(())
    }
}

/// One chunk of the condemn step. `$1`..`$2` (null: open end): id range;
/// `$3`: cutoff (Unix seconds); `$4`: insert into `gc_condemned`. Returns
/// the number of ids condemned, then counts and bytes per table of their
/// rows.
///
/// In a real run the old object and index rows are locked (`FOR SHARE`)
/// and an id is condemned only if every object and index row it has is
/// among them. Locking re-evaluates the predicate against the row's current
/// version (READ COMMITTED re-check), so a row refreshed after this
/// statement's snapshot is skipped, and a row whose refresh has not
/// committed yet is skipped too (`SKIP LOCKED`: the collector never waits
/// for a writer). A writer that refreshes a row after this statement locked
/// it waits until the chunk commits, and its take-back, which runs after the
/// refresh in the writer's transaction, then sees the condemned id
/// (`PgStore::refresh_existing`). Commit records and changesets are not
/// refreshed by writers and are not locked.
macro_rules! condemn_chunk {
    ($lock_o:literal, $lock_x:literal) => {
        concat!(
            "WITH co AS (SELECT id, octet_length(data)::bigint AS bytes FROM objects o \
                  WHERE id >= $1 AND ($2::bytea IS NULL OR id < $2) \
                    AND written_at < to_timestamp($3) \
                    AND NOT EXISTS (SELECT 1 FROM gc_live l WHERE l.id = o.id) ",
            $lock_o,
            "), \
             cx AS (SELECT id, len::bigint AS bytes FROM object_index x \
                  WHERE id >= $1 AND ($2::bytea IS NULL OR id < $2) \
                    AND written_at < to_timestamp($3) \
                    AND NOT EXISTS (SELECT 1 FROM gc_live l WHERE l.id = x.id) ",
            $lock_x,
            "), \
             cand AS ( \
                 SELECT id, 'o'::text AS src, bytes FROM co \
                 UNION ALL \
                 SELECT id, 'i', bytes FROM cx \
                 UNION ALL \
                 SELECT id, 'c', 0::bigint FROM commits c \
                  WHERE id >= $1 AND ($2::bytea IS NULL OR id < $2) \
                    AND written_at < to_timestamp($3) \
                    AND NOT EXISTS (SELECT 1 FROM gc_live l WHERE l.id = c.id) \
                 UNION ALL \
                 SELECT commit_id, 's', octet_length(data)::bigint FROM changesets s \
                  WHERE commit_id >= $1 AND ($2::bytea IS NULL OR commit_id < $2) \
                    AND written_at < to_timestamp($3) \
                    AND NOT EXISTS (SELECT 1 FROM gc_live l WHERE l.id = s.commit_id)), \
             ids AS (SELECT DISTINCT id FROM cand d \
                  WHERE NOT EXISTS (SELECT 1 FROM objects o WHERE o.id = d.id \
                                    AND NOT EXISTS (SELECT 1 FROM co WHERE co.id = o.id)) \
                    AND NOT EXISTS (SELECT 1 FROM object_index x WHERE x.id = d.id \
                                    AND NOT EXISTS (SELECT 1 FROM cx WHERE cx.id = x.id))), \
             ins AS (INSERT INTO gc_condemned (id) SELECT id FROM ids WHERE $4 \
                     ON CONFLICT (id) DO NOTHING) \
             SELECT (SELECT count(*) FROM ids), \
                    count(*) FILTER (WHERE src = 'o'), \
                    coalesce(sum(bytes) FILTER (WHERE src = 'o'), 0)::bigint, \
                    count(*) FILTER (WHERE src = 'i'), \
                    coalesce(sum(bytes) FILTER (WHERE src = 'i'), 0)::bigint, \
                    count(*) FILTER (WHERE src = 'c'), \
                    count(*) FILTER (WHERE src = 's'), \
                    coalesce(sum(bytes) FILTER (WHERE src = 's'), 0)::bigint \
             FROM cand WHERE id IN (SELECT id FROM ids)"
        )
    };
}

/// [`condemn_chunk!`] of a real run.
const CONDEMN_CHUNK: &str =
    condemn_chunk!("FOR SHARE OF o SKIP LOCKED", "FOR SHARE OF x SKIP LOCKED");
/// [`condemn_chunk!`] of a dry run: counts only, takes no locks.
const CONDEMN_COUNT: &str = condemn_chunk!("", "");

impl Run<'_> {
    // -- the phases -------------------------------------------------------

    async fn dry(&mut self) -> Result<()> {
        sqlx::raw_sql(
            "CREATE TEMPORARY TABLE gc_live (\
                 id bytea PRIMARY KEY, kind smallint NOT NULL, expanded boolean NOT NULL); \
             CREATE INDEX ON pg_temp.gc_live (id) WHERE NOT expanded",
        )
        .execute(&mut *self.conn)
        .await
        .map_err(be)?;
        let pending: Option<String> =
            sqlx::query_scalar("SELECT phase FROM gc_state WHERE phase <> 'done'")
                .fetch_optional(&mut *self.conn)
                .await
                .map_err(be)?;
        if let Some(phase) = pending {
            self.say(&format!("note: an interrupted run is pending in phase {phase}"));
        }
        let started = self.now().await?;
        self.cutoff = started - secs(self.opts.grace);
        self.report.cutoff = self.timestamp_text(self.cutoff).await?;
        self.say("marking reachable data (dry run)");
        self.mark(true, self.cutoff).await?;
        self.report.live = self.count_live().await?;
        self.say("counting unreachable data");
        self.condemn().await?;
        if self.opts.s3.is_some() {
            self.say("scanning packs");
            self.packs().await?;
        }
        self.report.finished = true;
        Ok(())
    }

    async fn real(&mut self) -> Result<()> {
        let state = sqlx::query(
            "SELECT phase, extract(epoch FROM started_at)::float8 AS started, \
                    extract(epoch FROM cutoff)::float8 AS cutoff, quiesce_secs, stats \
             FROM gc_state WHERE phase <> 'done'",
        )
        .fetch_optional(&mut *self.conn)
        .await
        .map_err(be)?;
        let (mut phase, started, quiesce) = match state {
            Some(r) => {
                let phase: String = r.try_get("phase").map_err(be)?;
                self.cutoff = r.try_get("cutoff").map_err(be)?;
                let stats: String = r.try_get("stats").map_err(be)?;
                self.report.decode_into(&stats);
                self.report.resumed = true;
                let started: f64 = r.try_get("started").map_err(be)?;
                let quiesce: f64 = r.try_get("quiesce_secs").map_err(be)?;
                self.say(&format!("resuming the interrupted run (phase {phase})"));
                (phase, started, quiesce)
            }
            None => {
                // Leftovers of an aborted run must not be swept unmarked.
                sqlx::raw_sql("DELETE FROM gc_condemned; TRUNCATE gc_live")
                    .execute(&mut *self.conn)
                    .await
                    .map_err(be)?;
                let started = self.now().await?;
                self.cutoff = started - secs(self.opts.grace);
                sqlx::query(
                    "INSERT INTO gc_state (phase, started_at, cutoff, grace_secs, quiesce_secs) \
                     VALUES ('mark', to_timestamp($1), to_timestamp($2), $3, $4) \
                     ON CONFLICT (singleton) DO UPDATE SET phase = 'mark', \
                         started_at = EXCLUDED.started_at, cutoff = EXCLUDED.cutoff, \
                         grace_secs = EXCLUDED.grace_secs, \
                         quiesce_secs = EXCLUDED.quiesce_secs, condemned_at = NULL, \
                         remarked_at = NULL, finished_at = NULL, stats = ''",
                )
                .bind(started)
                .bind(self.cutoff)
                .bind(secs(self.opts.grace))
                .bind(secs(self.opts.quiesce))
                .execute(&mut *self.conn)
                .await
                .map_err(be)?;
                ("mark".to_owned(), started, secs(self.opts.quiesce))
            }
        };
        self.report.cutoff = self.timestamp_text(self.cutoff).await?;
        // Everything written since the mark began, with a margin for
        // transactions that were open then.
        let since_start = started - secs(FINAL_MARGIN);
        loop {
            match phase.as_str() {
                "mark" => {
                    self.say("marking reachable data");
                    self.mark(true, self.cutoff).await?;
                    // Catch up with writes made while marking.
                    self.mark(false, since_start).await?;
                    self.report.live = self.count_live().await?;
                    self.say("condemning unreachable data");
                    self.condemn().await?;
                    phase = self.set_phase("quiesce", Some("condemned_at")).await?;
                }
                "quiesce" => {
                    let wait: f64 = sqlx::query_scalar(
                        "SELECT greatest(0, extract(epoch FROM condemned_at \
                                + make_interval(secs => $1) - now()))::float8 FROM gc_state",
                    )
                    .bind(quiesce)
                    .fetch_one(&mut *self.conn)
                    .await
                    .map_err(be)?;
                    if wait > 0.0 {
                        self.say(&format!(
                            "waiting {wait:.0}s for servers to forget condemned data (quiesce)"
                        ));
                        tokio::time::sleep(Duration::from_secs_f64(wait)).await;
                    }
                    self.say("marking again");
                    self.mark(false, since_start).await?;
                    phase = self.set_phase("sweep", Some("remarked_at")).await?;
                }
                "sweep" => {
                    self.say("deleting condemned data");
                    if !self.sweep().await? {
                        self.save_stats().await?;
                        return Ok(());
                    }
                    phase = self.set_phase("packs", None).await?;
                }
                "packs" => {
                    if self.opts.s3.is_some() {
                        self.say("deleting dead packs");
                        self.packs().await?;
                    }
                    phase = self.set_phase("done", Some("finished_at")).await?;
                }
                _ => {
                    sqlx::query("TRUNCATE gc_live").execute(&mut *self.conn).await.map_err(be)?;
                    self.report.finished = true;
                    return Ok(());
                }
            }
        }
    }

    async fn save_stats(&mut self) -> Result<()> {
        let stats = self.report.encode();
        sqlx::query("UPDATE gc_state SET stats = $1")
            .bind(stats)
            .execute(&mut *self.conn)
            .await
            .map_err(be)?;
        Ok(())
    }

    /// Move to `phase` (stamping `column` with the time), saving the stats.
    async fn set_phase(&mut self, phase: &str, column: Option<&str>) -> Result<String> {
        let stamp = column.map(|c| format!(", {c} = now()")).unwrap_or_default();
        let stats = self.report.encode();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE gc_state SET phase = $1, stats = $2{stamp}"
        )))
        .bind(phase)
        .bind(stats)
        .execute(&mut *self.conn)
        .await
        .map_err(be)?;
        Ok(phase.to_owned())
    }

    /// Delete condemned ids batch by batch. Returns false when stopped early
    /// ([`GcOptions::stop_after_batches`]).
    async fn sweep(&mut self) -> Result<bool> {
        let mut batches = 0usize;
        let mut last_mark = Instant::now();
        let mut marked_at = self.now().await?;
        loop {
            if last_mark.elapsed() >= SWEEP_REMARK_EVERY {
                // Defence in depth: recent writes and ref moves.
                let now = self.now().await?;
                self.mark(false, marked_at - secs(SWEEP_MARGIN)).await?;
                (marked_at, last_mark) = (now, Instant::now());
            }
            if self.opts.stop_after_batches.is_some_and(|n| batches >= n) {
                return Ok(false);
            }
            match self.sweep_batch().await? {
                Batch::Done => return Ok(true),
                Batch::Deleted => batches += 1,
                Batch::RefMoved => {
                    self.say("a ref moved onto condemned data: marking again");
                    let now = self.now().await?;
                    self.mark(false, marked_at - secs(SWEEP_MARGIN)).await?;
                    // Marked already (closure included) yet condemned can
                    // only follow a lost working set: take targets back.
                    sqlx::query("DELETE FROM gc_condemned WHERE id IN (SELECT target FROM refs)")
                        .execute(&mut *self.conn)
                        .await
                        .map_err(be)?;
                    (marked_at, last_mark) = (now, Instant::now());
                }
            }
            if batches.is_multiple_of(16) {
                self.save_stats().await?;
            }
        }
    }

    async fn sweep_batch(&mut self) -> Result<Batch> {
        use sqlx::Connection;
        let fence = fence_key(&self.store.schema);
        let commit_keyed = self.opts.commit_keyed_kinds.clone();
        let cutoff = self.cutoff;
        let limit = self.opts.batch.max(1) as i64;
        let mut tx = self.conn.begin().await.map_err(be)?;
        // Wait for ref moves in flight; hold off new ones until commit.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(&fence)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        let moved: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM refs r JOIN gc_condemned g ON g.id = r.target)",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(be)?;
        if moved {
            tx.rollback().await.map_err(be)?;
            return Ok(Batch::RefMoved);
        }
        let taken: Vec<Vec<u8>> = sqlx::query_scalar(
            "DELETE FROM gc_condemned WHERE id IN (SELECT id FROM gc_condemned \
             ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED) RETURNING id",
        )
        .bind(limit)
        .fetch_all(&mut *tx)
        .await
        .map_err(be)?;
        if taken.is_empty() {
            tx.commit().await.map_err(be)?;
            return Ok(Batch::Done);
        }
        // A writer holding a row lock is refreshing it: skip, never wait.
        let objects = sqlx::query(
            "DELETE FROM objects WHERE id IN (SELECT id FROM objects WHERE id = ANY($1) \
                 AND written_at < to_timestamp($2) ORDER BY id FOR UPDATE SKIP LOCKED) \
             RETURNING id, octet_length(data)::bigint AS bytes",
        )
        .bind(&taken)
        .bind(cutoff)
        .fetch_all(&mut *tx)
        .await
        .map_err(be)?;
        let index = sqlx::query(
            "DELETE FROM object_index WHERE id IN (SELECT id FROM object_index \
                 WHERE id = ANY($1) AND written_at < to_timestamp($2) \
                 ORDER BY id FOR UPDATE SKIP LOCKED) \
             RETURNING id, len::bigint AS bytes",
        )
        .bind(&taken)
        .bind(cutoff)
        .fetch_all(&mut *tx)
        .await
        .map_err(be)?;
        // A commit record goes only with its object (or when the object is
        // gone already): one that was stored again keeps its record.
        let commits: Vec<Vec<u8>> = sqlx::query_scalar(
            "DELETE FROM commits WHERE id IN (SELECT id FROM commits c \
                 WHERE id = ANY($1) AND written_at < to_timestamp($2) \
                   AND NOT EXISTS (SELECT 1 FROM objects o WHERE o.id = c.id) \
                   AND NOT EXISTS (SELECT 1 FROM object_index x WHERE x.id = c.id) \
                 ORDER BY id FOR UPDATE SKIP LOCKED) \
             RETURNING id",
        )
        .bind(&taken)
        .bind(cutoff)
        .fetch_all(&mut *tx)
        .await
        .map_err(be)?;
        sqlx::query("DELETE FROM log WHERE commit_id = ANY($1)")
            .bind(&commits)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        let legacy = sqlx::query("DELETE FROM changes WHERE commit_id = ANY($1)")
            .bind(&commits)
            .execute(&mut *tx)
            .await
            .map_err(be)?
            .rows_affected();
        let changesets = sqlx::query(
            "DELETE FROM changesets WHERE commit_id IN (SELECT commit_id FROM changesets s \
                 WHERE commit_id = ANY($1) AND written_at < to_timestamp($2) \
                   AND NOT EXISTS (SELECT 1 FROM commits c WHERE c.id = s.commit_id) \
                 ORDER BY commit_id FOR UPDATE SKIP LOCKED) \
             RETURNING commit_id AS id, octet_length(data)::bigint AS bytes",
        )
        .bind(&taken)
        .bind(cutoff)
        .fetch_all(&mut *tx)
        .await
        .map_err(be)?;
        // Keys are `<commit id><path>`; paths are UTF-8, never 0xff.
        let derived = sqlx::query(
            "DELETE FROM derived d USING unnest($1::bytea[]) AS t(id) \
             WHERE d.kind = ANY($2) AND d.key >= t.id AND d.key < t.id || '\\xff'::bytea",
        )
        .bind(&commits)
        .bind(&commit_keyed)
        .execute(&mut *tx)
        .await
        .map_err(be)?
        .rows_affected();
        tx.commit().await.map_err(be)?;

        let sum = |rows: &[PgRow]| -> Result<u64> {
            rows.iter().map(|r| Ok(r.try_get::<i64, _>("bytes").map_err(be)? as u64)).sum()
        };
        let mut gone: HashSet<Vec<u8>> = HashSet::new();
        for r in objects.iter().chain(&index).chain(&changesets) {
            gone.insert(r.try_get::<Vec<u8>, _>("id").map_err(be)?);
        }
        gone.extend(commits.iter().cloned());
        let r = &mut self.report;
        r.objects += objects.len() as u64;
        r.object_bytes += sum(&objects)?;
        r.large_objects += index.len() as u64;
        r.large_object_bytes += sum(&index)?;
        r.commits += commits.len() as u64;
        r.legacy_changes += legacy;
        r.changesets += changesets.len() as u64;
        r.changeset_bytes += sum(&changesets)?;
        r.derived += derived;
        r.kept += (taken.len() as u64).saturating_sub(gone.len() as u64);
        Ok(Batch::Deleted)
    }

    /// Delete packs without index rows that are older than the pack minimum
    /// age (dry run: count them, judging index rows by the mark), and add up
    /// the dead bytes of the packs that stay.
    async fn packs(&mut self) -> Result<()> {
        let Some(s3) = self.opts.s3.clone() else { return Ok(()) };
        let min_age = self.opts.pack_min_age.unwrap_or(self.opts.grace);
        let pack_cutoff = self.now().await? - secs(min_age);
        let live_rows = if self.opts.dry_run {
            "x.written_at >= to_timestamp($2) \
             OR EXISTS (SELECT 1 FROM gc_live l WHERE l.id = x.id)"
        } else {
            "true"
        };
        let stmt = format!(
            "SELECT pack, coalesce(sum(len) FILTER (WHERE {live_rows}), 0)::bigint AS live_bytes, \
                    count(*) FILTER (WHERE {live_rows}) AS live \
             FROM object_index x WHERE pack = ANY($1) GROUP BY pack"
        );
        let mut token = None;
        loop {
            let (page, next) = s3.list_packs(token).await?;
            let ids: Vec<Vec<u8>> = page.iter().map(|p| p.id.to_vec()).collect();
            let rows = sqlx::query(sqlx::AssertSqlSafe(stmt.clone()))
                .bind(&ids)
                .bind(self.cutoff)
                .fetch_all(&mut *self.conn)
                .await
                .map_err(be)?;
            let mut live: HashMap<Vec<u8>, (u64, i64)> = HashMap::new();
            for r in &rows {
                let pack: Vec<u8> = r.try_get("pack").map_err(be)?;
                let bytes: i64 = r.try_get("live_bytes").map_err(be)?;
                let n: i64 = r.try_get("live").map_err(be)?;
                live.insert(pack, (bytes as u64, n));
            }
            let mut dead = Vec::new();
            for p in &page {
                let (bytes, n) = live.get(p.id.as_slice()).copied().unwrap_or((0, 0));
                if n == 0 && p.modified < pack_cutoff {
                    dead.push(p.id);
                    self.report.packs += 1;
                    self.report.pack_bytes += p.size;
                } else {
                    self.report.packs_kept += 1;
                    self.report.packs_kept_bytes += p.size;
                    self.report.packs_dead_bytes += p.size.saturating_sub(bytes);
                }
            }
            if !self.opts.dry_run && !dead.is_empty() {
                s3.delete_packs(&dead).await?;
            }
            match next {
                Some(t) => token = Some(t),
                None => return Ok(()),
            }
        }
    }
}

enum Batch {
    Done,
    Deleted,
    RefMoved,
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::postgres::IndexEntry;
    use crate::{CasOutcome, CommitNode, MetaStore, ObjectStoreExt, RefStore};
    use ant_core::{Commit, EncodedObject, EntryMode, Object, Signature, Tree, TreeEntry};
    use std::cell::Cell;
    use std::sync::Arc;

    tokio::task_local! {
        /// Write statements (`PgStore::write`) issued by the current task.
        pub(in crate::postgres) static WRITES: Cell<u64>;
    }

    /// `fut`'s result and the number of write statements it issued.
    async fn writes<F: Future>(fut: F) -> (F::Output, u64) {
        WRITES
            .scope(Cell::new(0), async {
                let out = fut.await;
                (out, WRITES.with(Cell::get))
            })
            .await
    }

    fn url() -> String {
        std::env::var("ANT_TEST_PG_URL")
            .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into())
    }

    async fn scratch() -> PgStore {
        PgStore::connect_scratch(&url(), &format!("t_gc_{}", rand::random::<u64>())).await.unwrap()
    }

    async fn sql(s: &PgStore, stmt: &str) {
        sqlx::raw_sql(sqlx::AssertSqlSafe(stmt.to_owned())).execute(&s.pool).await.unwrap();
    }

    async fn count(s: &PgStore, stmt: &str) -> i64 {
        sqlx::query_scalar(sqlx::AssertSqlSafe(stmt.to_owned())).fetch_one(&s.pool).await.unwrap()
    }

    fn lit(id: &ObjectId) -> String {
        format!("'\\x{}'::bytea", id.to_hex())
    }

    /// Pretend everything stored so far was written `days` ago.
    async fn age(s: &PgStore, days: u32) {
        for t in ["objects", "object_index", "commits", "changesets"] {
            sql(s, &format!("UPDATE {t} SET written_at = written_at - interval '{days} days'"))
                .await;
        }
    }

    /// A stored and recorded commit of `d/<name>` = `content` on `parent`.
    /// Returns (commit, root tree, blob).
    async fn commit(
        s: &PgStore,
        parent: Option<ObjectId>,
        name: &str,
        content: &[u8],
    ) -> (ObjectId, ObjectId, ObjectId) {
        let blob = EncodedObject::new(ObjectKind::Blob, content);
        let entry = |mode, name: &str, id| TreeEntry { mode, name: name.into(), id };
        let sub = Object::Tree(Tree::new(vec![entry(EntryMode::File, name, blob.id())]).unwrap())
            .encode();
        let root =
            Object::Tree(Tree::new(vec![entry(EntryMode::Tree, "d", sub.id())]).unwrap()).encode();
        let sig =
            Signature { name: "t".into(), email: "t@x".into(), time: 1, tz_offset_minutes: 0 };
        let c = Commit {
            tree: root.id(),
            parents: parent.into_iter().collect(),
            author: sig.clone(),
            committer: sig,
            extra_headers: vec![],
            message: format!("{name}\n").into_bytes(),
        };
        let co = EncodedObject::new(ObjectKind::Commit, &c.encode());
        s.put_many(&[blob.clone(), sub, root.clone(), co.clone()]).await.unwrap();
        let generation = match parent {
            Some(p) => s.get_commits(&[p]).await.unwrap()[0].as_ref().unwrap().generation + 1,
            None => 1,
        };
        let node = CommitNode {
            id: co.id(),
            tree: root.id(),
            parents: c.parents.clone(),
            generation,
            committer_time: 1,
        };
        s.put_commit(node, vec![]).await.unwrap();
        (co.id(), root.id(), blob.id())
    }

    fn opts() -> GcOptions {
        GcOptions { quiesce: Duration::ZERO, batch: 2, ..GcOptions::default() }
    }

    async fn run(s: &PgStore, o: &GcOptions) -> GcReport {
        s.gc(s, o, &|_| {}).await.unwrap()
    }

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_gc_deletes_unreachable_and_keeps_reachable() {
        let s = scratch().await;
        let (c1, t1, b1) = commit(&s, None, "a", b"one").await;
        let (c2, _, b2) = commit(&s, Some(c1), "a", b"two").await;
        s.cas_ref("main", None, Some(c2)).await.unwrap();
        // Garbage: an unlanded commit on c1, and a stray blob.
        let (g, gt, gb) = commit(&s, Some(c1), "a", b"garbage").await;
        let stray = s.put(EncodedObject::new(ObjectKind::Blob, b"stray")).await.unwrap();
        age(&s, 30).await;
        // Recent garbage stays (grace period).
        let (fresh, _, _) = commit(&s, Some(c1), "a", b"fresh").await;

        let dry = run(&s, &GcOptions { dry_run: true, ..opts() }).await;
        assert!(dry.dry_run && dry.finished);
        // The garbage commit, its two trees, its blob and the stray blob.
        assert_eq!((dry.objects, dry.commits, dry.condemned), (5, 1, 5), "{dry:?}");
        assert!(s.get(&g).await.unwrap().is_some());

        let r = run(&s, &opts()).await;
        assert!(r.finished && !r.dry_run, "{r:?}");
        assert_eq!((r.objects, r.commits, r.condemned, r.kept), (5, 1, 5, 0), "{r:?}");
        for id in [g, gt, gb, stray] {
            let q = format!("SELECT count(*) FROM objects WHERE id = {}", lit(&id));
            assert_eq!(count(&s, &q).await, 0, "{id}");
        }
        assert_eq!(s.get_commits(&[g]).await.unwrap(), vec![None]);
        assert_eq!(count(&s, "SELECT count(*) FROM log").await, 3);
        for id in [c1, c2, t1, b1, b2, fresh] {
            assert!(s.get(&id).await.unwrap().is_some(), "{id}");
        }
        assert_eq!(count(&s, "SELECT count(*) FROM gc_condemned").await, 0);
        assert_eq!(count(&s, "SELECT count(*) FROM gc_live").await, 0);
        // Nothing left: a second run deletes nothing.
        let again = run(&s, &opts()).await;
        assert_eq!((again.objects, again.commits, again.condemned), (0, 0, 0));
    }

    /// Regression guard (landing throughput): storing objects that exist
    /// with a young write time, as a land does with the trees its
    /// changesets wrote, is one statement, like storing new ones; only old
    /// rows cost the refresh and the take-back from `gc_condemned`.
    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_storing_young_objects_again_is_one_statement() {
        let s = scratch().await;
        let blob = |b: &[u8]| EncodedObject::new(ObjectKind::Blob, b);
        let (a, b, c) = (blob(b"a"), blob(b"b"), blob(b"c"));
        let index = |ids: &[ObjectId]| -> Vec<IndexEntry> {
            ids.iter()
                .enumerate()
                .map(|(i, id)| IndexEntry { id: *id, pack: [7; 16], off: i as u64, len: 1 })
                .collect()
        };

        let (r, n) = writes(s.put_many(&[a.clone(), b.clone()])).await;
        r.unwrap();
        assert_eq!(n, 1, "new objects");
        let (r, n) = writes(s.put_many(&[a.clone(), b.clone()])).await;
        r.unwrap();
        assert_eq!(n, 1, "young existing objects");
        let (r, n) = writes(s.put_many(&[a.clone(), c.clone()])).await;
        r.unwrap();
        assert_eq!(n, 1, "young existing and new objects");
        let (r, n) = writes(s.index_objects(&index(&[a.id(), b.id()]))).await;
        r.unwrap();
        assert_eq!(n, 1, "new index rows");
        let (r, n) = writes(s.index_objects(&index(&[a.id(), b.id(), c.id()]))).await;
        r.unwrap();
        assert_eq!(n, 1, "young existing and new index rows");

        // Old rows (one of them condemned) are refreshed and taken back.
        sql(&s, "UPDATE objects SET written_at = now() - interval '2 hours'").await;
        sql(&s, "UPDATE object_index SET written_at = now() - interval '2 hours'").await;
        sql(&s, &format!("INSERT INTO gc_condemned VALUES ({})", lit(&a.id()))).await;
        let (r, n) = writes(s.put_many(&[a.clone(), b.clone()])).await;
        r.unwrap();
        assert_eq!(n, 3, "insert, refresh, take back");
        let young = "SELECT count(*) FROM objects WHERE written_at > now() - interval '1 minute'";
        assert_eq!(count(&s, young).await, 2);
        assert_eq!(count(&s, "SELECT count(*) FROM gc_condemned").await, 0);
        let (r, n) = writes(s.index_objects(&index(&[a.id(), c.id()]))).await;
        r.unwrap();
        assert_eq!(n, 3, "index: insert, refresh, take back");
        let young =
            "SELECT count(*) FROM object_index WHERE written_at > now() - interval '1 minute'";
        assert_eq!(count(&s, young).await, 2);
        // Index rows keep pointing where they were first written.
        let off = format!("SELECT off FROM object_index WHERE id = {}", lit(&c.id()));
        assert_eq!(count(&s, &off).await, 2);
    }

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_condemned_ids_read_as_missing_until_stored_again() {
        let s = scratch().await;
        let (c, t, b) = commit(&s, None, "f", b"x").await;
        age(&s, 30).await;
        for id in [c, t, b] {
            sql(&s, &format!("INSERT INTO gc_condemned VALUES ({})", lit(&id))).await;
        }
        assert_eq!(s.get_many(&[b, t]).await.unwrap(), vec![None, None]);
        assert_eq!(s.has_many(&[b]).await.unwrap(), vec![false]);
        assert_eq!(s.get_commits(&[c]).await.unwrap(), vec![None]);
        assert_eq!(s.get_changesets(&[c]).await.unwrap(), vec![None]);
        assert!(s.first_parent_chain(c, None, 10).await.unwrap().is_empty());
        // The collector itself sees them.
        assert!(unhidden(s.get_many(&[b])).await.unwrap()[0].is_some());
        // Storing the blob again takes it back and refreshes its write time.
        s.put(EncodedObject::new(ObjectKind::Blob, b"x")).await.unwrap();
        assert!(s.get(&b).await.unwrap().is_some());
        let recent = "SELECT count(*) FROM objects WHERE written_at > now() - interval '1 minute'";
        assert_eq!(count(&s, recent).await, 1);
        assert_eq!(count(&s, "SELECT count(*) FROM gc_condemned").await, 2);
        // A recent object is not rewritten when stored again.
        let versions = format!("SELECT xmin::text::bigint FROM objects WHERE id = {}", lit(&b));
        let before = count(&s, &versions).await;
        s.put(EncodedObject::new(ObjectKind::Blob, b"x")).await.unwrap();
        assert_eq!(count(&s, &versions).await, before);
    }

    /// The cutoff of a run with the default grace period starting now.
    async fn default_cutoff(s: &PgStore) -> f64 {
        sqlx::query_scalar("SELECT extract(epoch FROM now())::float8 - $1")
            .bind(secs(DEFAULT_GRACE))
            .fetch_one(&s.pool)
            .await
            .unwrap()
    }

    /// Run the condemn statement over the whole id range in `tx`.
    async fn condemn_all(tx: &mut sqlx::PgConnection, cutoff: f64) {
        sqlx::query(CONDEMN_CHUNK)
            .bind(vec![0u8])
            .bind(None::<Vec<u8>>)
            .bind(cutoff)
            .bind(true)
            .fetch_one(tx)
            .await
            .unwrap();
    }

    async fn condemned(s: &PgStore, id: &ObjectId) -> bool {
        count(s, &format!("SELECT count(*) FROM gc_condemned WHERE id = {}", lit(id))).await > 0
    }

    /// Read-your-writes against a condemn step in flight: an old object is
    /// stored again while a condemn chunk that selected it has not committed
    /// yet. Whichever way the two interleave, once `put` returns the object
    /// reads back (formerly the chunk committed after the writer's refresh
    /// and take-back, and the object read as missing until the end of the
    /// condemn step).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_storing_again_during_a_condemn_chunk_reads_back() {
        let s = scratch().await;
        let blob = EncodedObject::new(ObjectKind::Blob, b"aged");
        let id = blob.id();
        s.put(blob.clone()).await.unwrap();
        age(&s, 30).await;
        let cutoff = default_cutoff(&s).await;
        // The chunk has selected (and, now, locked) the row; it is held open.
        let mut chunk = s.pool.begin().await.unwrap();
        condemn_all(&mut chunk, cutoff).await;
        let mut writer = tokio::spawn({
            let s = s.clone();
            async move { s.put(blob).await }
        });
        // Let the writer get as far as it can before the chunk commits: its
        // refresh waits for the chunk's row lock.
        let early = tokio::time::timeout(Duration::from_millis(500), &mut writer).await;
        chunk.commit().await.unwrap();
        assert!(early.is_err(), "the writer finished while the chunk condemning it was open");
        writer.await.unwrap().unwrap();
        assert!(s.get(&id).await.unwrap().is_some(), "stored, then read as missing");
        assert!(!condemned(&s, &id).await);
        let young = "SELECT count(*) FROM objects WHERE written_at > now() - interval '1 minute'";
        assert_eq!(count(&s, young).await, 1);
    }

    /// A condemn chunk running while a writer's refresh of a row is not
    /// committed yet leaves that row alone: it never condemns a row a writer
    /// is about to report stored.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_condemn_skips_rows_being_refreshed() {
        let s = scratch().await;
        let (busy, idle) = (b"busy".as_slice(), b"idle".as_slice());
        let (busy, idle) = (
            EncodedObject::new(ObjectKind::Blob, busy),
            EncodedObject::new(ObjectKind::Blob, idle),
        );
        s.put_many(&[busy.clone(), idle.clone()]).await.unwrap();
        age(&s, 30).await;
        let cutoff = default_cutoff(&s).await;
        // A writer's refresh, not committed yet (as `refresh_existing`).
        let mut w = s.pool.begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE objects SET written_at = now() WHERE id = {}",
            lit(&busy.id())
        )))
        .execute(&mut *w)
        .await
        .unwrap();
        let mut c = s.pool.acquire().await.unwrap();
        // The collector never waits for a writer.
        tokio::time::timeout(Duration::from_secs(10), condemn_all(&mut c, cutoff)).await.unwrap();
        assert!(!condemned(&s, &busy.id()).await, "a row being refreshed was condemned");
        assert!(condemned(&s, &idle.id()).await);
        w.commit().await.unwrap();
        assert!(s.get(&busy.id()).await.unwrap().is_some());
        assert!(s.get(&idle.id()).await.unwrap().is_none());
    }

    /// An id is condemned only when every row it has is old: a commit
    /// object stored again keeps its (old) commit record readable.
    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_condemn_needs_every_row_of_an_id_old() {
        let s = scratch().await;
        let (c, _, _) = commit(&s, None, "f", b"x").await;
        let (g, _, _) = commit(&s, None, "g", b"y").await;
        age(&s, 30).await;
        let object = s.get(&c).await.unwrap().unwrap();
        s.put(object).await.unwrap();
        let cutoff = default_cutoff(&s).await;
        let mut conn = s.pool.acquire().await.unwrap();
        condemn_all(&mut conn, cutoff).await;
        assert!(!condemned(&s, &c).await);
        assert!(s.get_commits(&[c]).await.unwrap()[0].is_some());
        assert!(condemned(&s, &g).await);
        assert!(s.get_commits(&[g]).await.unwrap()[0].is_none());
    }

    /// Readers hide a condemned id only where the sweep would delete it: rows
    /// written before the run's cutoff. A row written since (stored again,
    /// or new) reads back even while its id is condemned.
    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_condemned_ids_hide_only_rows_older_than_the_cutoff() {
        let s = scratch().await;
        let index = |id: ObjectId| vec![IndexEntry { id, pack: [7; 16], off: 0, len: 1 }];
        let (old, new) = (
            EncodedObject::new(ObjectKind::Blob, b"old"),
            EncodedObject::new(ObjectKind::Blob, b"new"),
        );
        s.put(old.clone()).await.unwrap();
        s.index_objects(&index(old.id())).await.unwrap();
        age(&s, 30).await;
        s.put(new.clone()).await.unwrap();
        s.index_objects(&index(new.id())).await.unwrap();
        sql(
            &s,
            "INSERT INTO gc_state (phase, started_at, cutoff, grace_secs, quiesce_secs) \
             VALUES ('quiesce', now(), now() - interval '7 days', 0, 0)",
        )
        .await;
        for id in [old.id(), new.id()] {
            sql(&s, &format!("INSERT INTO gc_condemned VALUES ({})", lit(&id))).await;
        }
        assert_eq!(s.has_many(&[old.id(), new.id()]).await.unwrap(), vec![false, true]);
        assert!(s.get(&new.id()).await.unwrap().is_some());
        let found = s.lookup_index(&[old.id(), new.id()]).await.unwrap();
        assert!(!found.contains_key(&old.id()) && found.contains_key(&new.id()));
    }

    /// An object deleted between the insert that saw it and the refresh is
    /// inserted again.
    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_store_again_after_concurrent_delete() {
        let s = scratch().await;
        let blob = EncodedObject::new(ObjectKind::Blob, b"racy");
        s.put(blob.clone()).await.unwrap();
        age(&s, 30).await;
        let id = blob.id();
        let ids: Vec<&[u8]> = vec![id.as_bytes().as_slice()];
        // The insert found it; then a collector deleted it.
        sql(&s, &format!("DELETE FROM objects WHERE id = {}", lit(&id))).await;
        let missing = s.refresh_existing(Table::Objects, &ids, &[]).await.unwrap();
        assert_eq!(missing.len(), 1);
        s.put(blob).await.unwrap();
        assert!(s.get(&id).await.unwrap().is_some());
    }

    /// Ref moves wait for a sweep batch holding the fence; deletes do not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_ref_moves_wait_for_the_sweep_fence() {
        use sqlx::Connection;
        let s = Arc::new(scratch().await);
        let fence = |s: &PgStore| fence_key(&s.schema);
        let mut conn = s.pool.acquire().await.unwrap();
        let mut tx = conn.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(fence(&s))
            .execute(&mut *tx)
            .await
            .unwrap();
        let s2 = s.clone();
        let mv =
            tokio::spawn(async move { s2.cas_ref("main", None, Some(ObjectId([1; 32]))).await });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!mv.is_finished());
        assert_eq!(s.get_ref("main").await.unwrap(), None);
        tx.commit().await.unwrap();
        assert_eq!(mv.await.unwrap().unwrap(), CasOutcome::Updated);
        let mut tx = conn.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(fence(&s))
            .execute(&mut *tx)
            .await
            .unwrap();
        s.cas_ref("main", Some(ObjectId([1; 32])), None).await.unwrap();
        tx.commit().await.unwrap();
    }

    /// One collector per schema; the grace period has a floor.
    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_gc_runs_are_exclusive_and_grace_is_bounded() {
        let s = scratch().await;
        let short = GcOptions { grace: Duration::from_secs(3600), ..opts() };
        assert!(matches!(s.gc(&s, &short, &|_| {}).await, Err(StoreError::InvalidArgument(_))));
        let mut conn = s.pool.acquire().await.unwrap();
        lock_run(&mut conn, &s.schema).await.unwrap();
        let err = s.gc(&s, &opts(), &|_| {}).await.unwrap_err();
        assert!(err.to_string().contains("another garbage collection"), "{err}");
        conn.close_on_drop();
        drop(conn);
        run(&s, &opts()).await;
    }

    /// Legacy mode spellings must not hide subtrees from the collector.
    #[test]
    fn tree_children_classify_legacy_modes() {
        let m = |s: &str| EntryMode::from_octal(s.as_bytes()).unwrap();
        let t = Tree::new(vec![
            TreeEntry { mode: m("100664"), name: "a".into(), id: ObjectId([1; 32]) },
            TreeEntry { mode: m("040000"), name: "d".into(), id: ObjectId([2; 32]) },
            TreeEntry { mode: m("40755"), name: "e".into(), id: ObjectId([3; 32]) },
            TreeEntry { mode: m("160755"), name: "g".into(), id: ObjectId([4; 32]) },
        ])
        .unwrap();
        let o = Object::Tree(t).encode();
        let mut out = Vec::new();
        tree_children(o.body(), &mut out).unwrap();
        out.sort();
        assert_eq!(
            out,
            [
                (ObjectId([1; 32]), KIND_BLOB),
                (ObjectId([2; 32]), KIND_TREE),
                (ObjectId([3; 32]), KIND_TREE)
            ]
        );
    }

    #[test]
    fn tree_children_follow_subtrees_and_blobs_but_not_gitlinks() {
        let t = Tree::new(vec![
            TreeEntry { mode: EntryMode::File, name: "a".into(), id: ObjectId([1; 32]) },
            TreeEntry { mode: EntryMode::Tree, name: "d".into(), id: ObjectId([2; 32]) },
            TreeEntry { mode: EntryMode::Gitlink, name: "m".into(), id: ObjectId([3; 32]) },
            TreeEntry { mode: EntryMode::Symlink, name: "s".into(), id: ObjectId([4; 32]) },
        ])
        .unwrap();
        let o = Object::Tree(t).encode();
        let mut out = Vec::new();
        tree_children(o.body(), &mut out).unwrap();
        out.sort();
        assert_eq!(
            out,
            [
                (ObjectId([1; 32]), KIND_BLOB),
                (ObjectId([2; 32]), KIND_TREE),
                (ObjectId([4; 32]), KIND_BLOB)
            ]
        );
        assert!(tree_children(b"100644 a\0short", &mut out).is_err());
        assert!(tree_children(b"100644 a", &mut out).is_err());
    }

    #[test]
    fn commit_children_are_tree_and_parents() {
        let body = format!(
            "tree {}\nparent {}\nparent {}\nauthor a <a> 1 +0000\n\ntree {}\n",
            ObjectId([1; 32]).to_hex(),
            ObjectId([2; 32]).to_hex(),
            ObjectId([3; 32]).to_hex(),
            ObjectId([9; 32]).to_hex(),
        );
        let mut out = Vec::new();
        commit_children(body.as_bytes(), &mut out).unwrap();
        assert_eq!(
            out,
            [
                (ObjectId([1; 32]), KIND_TREE),
                (ObjectId([2; 32]), KIND_COMMIT),
                (ObjectId([3; 32]), KIND_COMMIT)
            ]
        );
        assert!(commit_children(b"tree xyz\n\n", &mut out).is_err());
    }

    #[test]
    fn report_stats_round_trip() {
        let mut r = GcReport { objects: 3, pack_bytes: 7, kept: 1, ..GcReport::default() };
        let s = r.encode();
        let mut back = GcReport::default();
        back.decode_into(&s);
        assert_eq!((back.objects, back.pack_bytes, back.kept), (3, 7, 1));
        assert_eq!(back.commits, 0);
        assert!(
            (GcReport { packs_kept_bytes: 4, packs_dead_bytes: 1, ..r }.pack_dead_ratio() - 0.25)
                .abs()
                < 1e-9
        );
    }
}
