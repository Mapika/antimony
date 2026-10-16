//! Local mode: every store in one data directory, no Postgres or S3.
//!
//! A single-node backend for one machine (a workstation running tens of
//! agents): refs, metadata and small objects in SQLite, large objects in
//! local pack files with the same record formats as S3 packs.
//!
//! # Data directory (format version [`DATA_FORMAT`])
//!
//! ```text
//! <dir>/FORMAT              "1\n": layout version of the directory
//! <dir>/LOCK                held (flock) by the one process using it
//! <dir>/meta.db             SQLite (WAL mode; -wal and -shm beside it):
//!                           schema `local::migrate`, versioned
//! <dir>/packs/<hex>.pack    pack files (`local::packs`)
//! ```
//!
//! One process owns a directory at a time (an exclusive lock on `LOCK`):
//! the server, or an offline tool (`ant import`, `ant admin gc`) while no
//! server runs. Opening a directory that is in use fails at once.
//!
//! # Stores
//!
//! - Objects up to the inline threshold (default 4 KiB encoded) are zstd
//!   rows of `objects`; larger ones go through
//!   [`crate::packed::PackedStore`] into pack files indexed by
//!   `object_index` (frames of look-alike objects, like S3 packs).
//! - Refs: `cas_ref` reads and writes inside one write transaction (SQLite
//!   has one writer at a time, so the check and the move are atomic).
//!   `lock_ref` gives an in-process per-ref lock whose `publish` moves the
//!   ref and inserts the landed-as rows in one durable transaction.
//! - Commit graph: `commits` holds the node and its log sequence number
//!   (`seq`, allocated in the write transaction, so log order is commit
//!   order); changesets are `changesets` rows written in the same
//!   transaction. Graph walks (`first_parent_chain`, `ancestors_above`) are
//!   point lookups inside one reader call: no round trips, unlike a server
//!   database, so no recursive SQL is needed.
//! - Derived data, history index and watermarks: tables of the same shape
//!   as Postgres's.
//! - Ref moves are announced on an in-process feed after their transaction
//!   commits (`watch_refs`); there are no other processes to tell.
//!
//! # Durability
//!
//! Writes go through one writer thread that commits whatever is queued as
//! one transaction (`local::db`): group commit across all callers. A group
//! is fsynced (`synchronous = FULL`) unless all of its writes were made
//! inside [`crate::deferred_durability`], which commit with `NORMAL`
//! (surviving a crash of the process, not of the machine). That is sound for
//! the same reason as Postgres's deferred writes: the WAL is appended in
//! commit order and an fsync covers everything before it, so the land's
//! publish (a FULL commit) makes its deferred objects, commits and
//! changesets durable first; and losing NORMAL commits takes an OS crash,
//! which also ends the process holding the ref lock, so a publish that
//! succeeds proves none were lost since the lock was taken.
//!
//! The same argument covers every write of this process, deferred or not:
//! one process owns the directory, so an OS crash that loses a NORMAL
//! commit also ends every operation that came after it. Only writes that
//! make data reachable or acknowledge a unit of work are therefore fsynced:
//! commit records (`put_commits`: a created changeset), ref moves and land
//! publishes, history watermarks. Objects, packed-object index rows,
//! derived data and history-index rows commit with `NORMAL` and become
//! durable with the next fsynced commit, which always precedes anything
//! that references them (a commit record, a ref move). After an OS crash
//! the store is consistent: no ref or commit record points at a lost
//! object; at worst an upload acknowledged just before the crash has to be
//! repeated (the changeset that would use it fails with "not found").
//! Pack bytes are always fsynced before their index rows are written
//! ([`packs`]): pack files are outside the WAL's ordering.

pub(crate) mod db;
pub mod gc;

pub mod migrate;
pub mod packs;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ant_core::{EncodedObject, ObjectId, RepoPath};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension};

use crate::changeset_codec;
use crate::feed::{MoveCause, RefFeed, RefMove};
use crate::packed::{IndexEntry, PackIndex, PackedStore};
use crate::tiered::{DEFAULT_INLINE_THRESHOLD, TieredObjectStore};
use crate::{
    Backend, CasOutcome, CommitNode, HistoryRow, MetaStore, ObjectStore, PathChange, RefLock,
    RefLockRelease, RefStore, Result, StoreError, validate_ref_name,
};
use db::{Db, DbOptions, Effects};
pub use migrate::{MIGRATIONS, SCHEMA_VERSION, SchemaStatus};
pub use packs::LocalPacks;

/// Layout version of a data directory (the `FORMAT` file).
pub const DATA_FORMAT: u64 = 1;
/// Name of the SQLite database inside a data directory.
pub const DB_FILE: &str = "meta.db";
/// Name of the pack directory inside a data directory.
pub const PACKS_DIR: &str = "packs";
/// Name of the format marker file.
pub const FORMAT_FILE: &str = "FORMAT";
/// Name of the lock file.
pub const LOCK_FILE: &str = "LOCK";

/// Storing an object that exists refreshes its write time when it is older
/// than this many seconds (garbage collection keeps recent writes).
const FRESHEN_SECS: i64 = 3600;

pub(crate) fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// Tuning of a local store. The defaults suit one machine running tens of
/// agents in a few hundred MB.
#[derive(Debug, Clone)]
pub struct LocalOptions {
    /// SQLite reader connections at most (reads beyond wait their turn).
    pub readers: usize,
    /// SQLite page cache per connection, KiB.
    pub cache_kib: u32,
    /// Objects whose encoded size is at most this many bytes live in
    /// SQLite; larger ones in pack files.
    pub inline_threshold: usize,
    /// Decoded pack frames kept in memory, bytes.
    pub frame_cache_bytes: usize,
    /// Compress look-alike large objects together (multi-object frames).
    pub pack_frames: bool,
    /// Create or migrate the schema when opening (off: fail unless current).
    pub migrate: bool,
    /// Commit durability (see [`LocalSync`]).
    pub sync: LocalSync,
}

/// How local-store commits reach the disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LocalSync {
    /// Every acknowledged write that is not deferred, and every land
    /// publish, is fsynced before it returns (`synchronous = FULL`; pack
    /// bytes are always fsynced). The default.
    #[default]
    Full,
    /// No fsync at commit (`synchronous = NORMAL`): a crash of the process
    /// loses nothing, but an OS crash or power loss can lose the last
    /// acknowledged writes, lands included. The database stays consistent.
    Normal,
}

impl Default for LocalOptions {
    fn default() -> Self {
        Self {
            readers: 4,
            cache_kib: 8 * 1024,
            inline_threshold: DEFAULT_INLINE_THRESHOLD,
            frame_cache_bytes: 16 * 1024 * 1024,
            pack_frames: true,
            migrate: true,
            sync: LocalSync::Full,
        }
    }
}

/// The default data directory: `$ANT_LOCAL_DIR`, else
/// `$XDG_DATA_HOME/antimony/local`, else `~/.local/share/antimony/local`.
pub fn default_dir() -> Option<PathBuf> {
    if let Some(d) = std::env::var_os("ANT_LOCAL_DIR").filter(|d| !d.is_empty()) {
        return Some(PathBuf::from(d));
    }
    let data = std::env::var_os("XDG_DATA_HOME")
        .filter(|d| !d.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))?;
    Some(data.join("antimony/local"))
}

/// Per-ref writer locks (in-process: one process owns a data directory).
/// Entries exist only while a lock is held or awaited.
type RefLocks = Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>;

struct Inner {
    dir: PathBuf,
    // Field order is drop order: the database closes (its writer thread
    // finishes and checkpoints) before the directory lock is released.
    db: Db,
    packs: LocalPacks,
    feed: RefFeed,
    locks: RefLocks,
    options: LocalOptions,
    _lock: std::fs::File,
    /// Scratch stores: the directory, deleted after everything above.
    _scratch: Option<tempfile::TempDir>,
}

/// Refs, metadata and small objects of a local data directory (see the
/// module docs); [`LocalStore::backend`] adds the pack files. Clones share
/// one database.
#[derive(Clone)]
pub struct LocalStore {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for LocalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalStore").field("dir", &self.inner.dir).finish()
    }
}

fn io_at(path: &Path, what: &str, e: std::io::Error) -> StoreError {
    StoreError::backend(std::io::Error::new(e.kind(), format!("{what} {}: {e}", path.display())))
}

/// Maps rusqlite errors into [`StoreError`]s.
pub(crate) trait Sq<T> {
    fn sq(self) -> Result<T>;
}

impl<T> Sq<T> for rusqlite::Result<T> {
    fn sq(self) -> Result<T> {
        self.map_err(StoreError::backend)
    }
}

fn to_i64(v: u64, what: &str) -> Result<i64> {
    i64::try_from(v).map_err(|_| StoreError::InvalidArgument(format!("{what} {v} out of range")))
}

fn id_of(bytes: &[u8]) -> Result<ObjectId> {
    Ok(ObjectId::from_bytes(bytes)?)
}

fn decode_ids(bytes: &[u8]) -> Result<Vec<ObjectId>> {
    bytes.chunks_exact(ObjectId::LEN).map(id_of).collect()
}

/// Read (or, for a new directory, write) the `FORMAT` marker.
fn check_format_file(dir: &Path) -> Result<()> {
    let path = dir.join(FORMAT_FILE);
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            let found = ant_core::version::parse_format_marker(&text).ok_or_else(|| {
                StoreError::InvalidArgument(format!("{}: unreadable format marker", path.display()))
            })?;
            ant_core::version::check_format(
                || format!("local data directory {} format", dir.display()),
                found,
                DATA_FORMAT,
            )?;
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let tmp = dir.join(format!("{FORMAT_FILE}.tmp"));
            std::fs::write(&tmp, format!("{DATA_FORMAT}\n"))
                .map_err(|e| io_at(&tmp, "writing", e))?;
            std::fs::File::open(&tmp)
                .and_then(|f| f.sync_all())
                .map_err(|e| io_at(&tmp, "syncing", e))?;
            std::fs::rename(&tmp, &path).map_err(|e| io_at(&path, "creating", e))?;
            packs::sync_dir(dir).map_err(|e| io_at(dir, "syncing", e))?;
            Ok(())
        }
        Err(e) => Err(io_at(&path, "reading", e)),
    }
}

/// Take the directory's lock file, failing at once if another process (or
/// another store in this one) holds it.
fn lock_dir(dir: &Path) -> Result<std::fs::File> {
    let path = dir.join(LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|e| io_at(&path, "opening", e))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(StoreError::InvalidArgument(format!(
            "local data directory {} is in use by another process (stop the server using it \
             first)",
            dir.display()
        ))),
        Err(std::fs::TryLockError::Error(e)) => Err(io_at(&path, "locking", e)),
    }
}

impl LocalStore {
    /// Open the data directory `dir`, creating it (and its schema) if
    /// needed, and lock it for this process.
    pub fn open(dir: &Path, options: &LocalOptions) -> Result<Self> {
        Self::open_inner(dir, options, None)
    }

    /// A store in a fresh temporary directory under `$TMPDIR`
    /// (`ant-local-<label>-*`), deleted with the last clone of the store
    /// (and of backends made from it), even on panic. For tests.
    pub fn open_scratch(label: &str) -> Result<Self> {
        Self::open_scratch_with(label, &LocalOptions::default())
    }

    /// [`Self::open_scratch`] with explicit options.
    pub fn open_scratch_with(label: &str, options: &LocalOptions) -> Result<Self> {
        let dir = tempfile::Builder::new()
            .prefix(&format!("ant-local-{label}-"))
            .tempdir()
            .map_err(StoreError::backend)?;
        let path = dir.path().to_owned();
        Self::open_inner(&path, options, Some(dir))
    }

    fn open_inner(
        dir: &Path,
        options: &LocalOptions,
        scratch: Option<tempfile::TempDir>,
    ) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| io_at(dir, "creating", e))?;
        let lock = lock_dir(dir)?;
        check_format_file(dir)?;
        let feed = RefFeed::default();
        let label = dir.display().to_string();
        let migrate = options.migrate;
        let db = Db::open(
            &dir.join(DB_FILE),
            DbOptions {
                readers: options.readers,
                cache_kib: options.cache_kib,
                no_fsync: options.sync == LocalSync::Normal,
            },
            feed.clone(),
            |conn| migrate::migrate(conn, migrate, &label).map(|_| ()),
        )?;
        let packs = LocalPacks::open(&dir.join(PACKS_DIR))?;
        Ok(Self {
            inner: Arc::new(Inner {
                dir: dir.to_owned(),
                db,
                packs,
                feed,
                locks: Arc::default(),
                options: options.clone(),
                _lock: lock,
                _scratch: scratch,
            }),
        })
    }

    /// Schema version state of the data directory `dir` (which need not
    /// exist), without changing or locking it.
    pub fn schema_status(dir: &Path) -> Result<SchemaStatus> {
        let path = dir.join(DB_FILE);
        if !path.exists() {
            return Ok(SchemaStatus { version: 0, supported: SCHEMA_VERSION });
        }
        let conn =
            Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).sq()?;
        migrate::status(&conn)
    }

    pub fn dir(&self) -> &Path {
        &self.inner.dir
    }

    pub fn packs(&self) -> &LocalPacks {
        &self.inner.packs
    }

    pub fn options(&self) -> &LocalOptions {
        &self.inner.options
    }

    /// Every store of a backend on this directory: small objects here,
    /// large ones in its pack files.
    pub fn backend(&self) -> Backend {
        let mut packed = PackedStore::new(self.clone(), self.inner.packs.clone())
            .with_frame_cache_bytes(self.inner.options.frame_cache_bytes);
        if !self.inner.options.pack_frames {
            packed = packed.with_frame_bytes(0);
        }
        let objects = Arc::new(TieredObjectStore::new(
            Arc::new(self.clone()),
            Arc::new(packed),
            self.inner.options.inline_threshold,
        ));
        let me = Arc::new(self.clone());
        Backend { objects, refs: me.clone(), meta: me }
    }

    pub(crate) fn db(&self) -> &Db {
        &self.inner.db
    }

    /// Readiness probe: one trivial read.
    pub async fn ping(&self) -> Result<()> {
        self.db().read(|c| c.query_row("SELECT 1", [], |_| Ok(())).sq()).await
    }

    /// Run SQLite's own maintenance (`PRAGMA optimize`: refreshes planner
    /// statistics where they are stale). Cheap; for idle moments.
    pub async fn optimize(&self) -> Result<()> {
        self.db().write(true, |c, _| c.execute_batch("PRAGMA optimize").sq()).await
    }

    /// Bytes used: (SQLite database plus WAL, pack files).
    pub fn disk_usage(&self) -> Result<(u64, u64)> {
        let size = |p: PathBuf| std::fs::metadata(p).map_or(0, |m| m.len());
        let db = size(self.inner.dir.join(DB_FILE))
            + size(self.inner.dir.join(format!("{DB_FILE}-wal")));
        let packs = self.inner.packs.list()?.values().sum();
        Ok((db, packs))
    }
}

/// CAS of `name` from `expected` to `new` inside a write transaction.
fn cas_in(
    c: &Connection,
    name: &str,
    expected: Option<ObjectId>,
    new: Option<ObjectId>,
) -> Result<Option<ObjectId>> {
    let actual: Option<Vec<u8>> = c
        .prepare_cached("SELECT target FROM refs WHERE name = ?1")
        .sq()?
        .query_row([name], |r| r.get(0))
        .optional()
        .sq()?;
    let actual = actual.map(|b| id_of(&b)).transpose()?;
    if actual != expected {
        return Ok(actual);
    }
    match new {
        Some(n) => {
            c.prepare_cached(
                "INSERT INTO refs (name, target) VALUES (?1, ?2) \
                 ON CONFLICT (name) DO UPDATE SET target = excluded.target",
            )
            .sq()?
            .execute(rusqlite::params![name, n.as_bytes().as_slice()])
            .sq()?;
        }
        None => {
            c.prepare_cached("DELETE FROM refs WHERE name = ?1").sq()?.execute([name]).sq()?;
        }
    }
    Ok(expected)
}

fn insert_derived(c: &Connection, kind: &str, entries: &[(Vec<u8>, Bytes)]) -> Result<()> {
    let mut st = c
        .prepare_cached(
            "INSERT INTO derived (kind, key, value) VALUES (?1, ?2, ?3) \
             ON CONFLICT (kind, key) DO NOTHING",
        )
        .sq()?;
    for (k, v) in entries {
        st.execute(rusqlite::params![kind, k, v.as_ref()]).sq()?;
    }
    Ok(())
}

#[async_trait]
impl ObjectStore for LocalStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = ids.to_vec();
        self.db()
            .read(move |c| {
                let mut st = c.prepare_cached("SELECT data FROM objects WHERE id = ?1").sq()?;
                let mut found: HashMap<ObjectId, Option<EncodedObject>> = HashMap::new();
                let mut out = Vec::with_capacity(ids.len());
                for id in &ids {
                    if let Some(o) = found.get(id) {
                        out.push(o.clone());
                        continue;
                    }
                    let data: Option<Vec<u8>> =
                        st.query_row([id.as_bytes().as_slice()], |r| r.get(0)).optional().sq()?;
                    let obj = data
                        .map(|d| crate::s3::S3ObjectStore::decode_object(*id, &d))
                        .transpose()?;
                    found.insert(*id, obj.clone());
                    out.push(obj);
                }
                Ok(out)
            })
            .await
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }
        let tasks = crate::compress_chunks(objects).into_iter().map(|chunk| {
            tokio::task::spawn_blocking(move || -> Result<Vec<(ObjectId, Vec<u8>)>> {
                let mut zstd =
                    zstd::bulk::Compressor::new(crate::ZSTD_LEVEL).map_err(StoreError::backend)?;
                chunk
                    .into_iter()
                    .map(|o| {
                        Ok((o.id(), zstd.compress(o.as_bytes()).map_err(StoreError::backend)?))
                    })
                    .collect()
            })
        });
        let mut rows: Vec<(ObjectId, Vec<u8>)> = Vec::with_capacity(objects.len());
        for part in futures::future::try_join_all(tasks).await.map_err(StoreError::backend)? {
            rows.extend(part?);
        }
        rows.sort_unstable_by_key(|a| a.0);
        rows.dedup_by(|a, b| a.0 == b.0);
        // Made durable by the commit or ref move that references it (see
        // "Durability" in the module docs).
        let durable = false;
        self.db()
            .write(durable, move |c, _| {
                let now = now_secs();
                let mut st = c
                    .prepare_cached(
                        "INSERT INTO objects (id, data, written_at) VALUES (?1, ?2, ?3) \
                         ON CONFLICT (id) DO UPDATE SET written_at = excluded.written_at \
                         WHERE objects.written_at < excluded.written_at - ?4",
                    )
                    .sq()?;
                for (id, data) in &rows {
                    st.execute(rusqlite::params![
                        id.as_bytes().as_slice(),
                        data,
                        now,
                        FRESHEN_SECS
                    ])
                    .sq()?;
                }
                Ok(())
            })
            .await
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = ids.to_vec();
        self.db()
            .read(move |c| {
                let mut st = c.prepare_cached("SELECT 1 FROM objects WHERE id = ?1").sq()?;
                ids.iter()
                    .map(|id| st.exists([id.as_bytes().as_slice()]).sq())
                    .collect::<Result<Vec<bool>>>()
            })
            .await
    }
}

#[async_trait]
impl PackIndex for LocalStore {
    async fn lookup_index(&self, ids: &[ObjectId]) -> Result<HashMap<ObjectId, IndexEntry>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids = ids.to_vec();
        self.db()
            .read(move |c| {
                let mut st = c
                    .prepare_cached("SELECT pack, off, len FROM object_index WHERE id = ?1")
                    .sq()?;
                let mut out = HashMap::with_capacity(ids.len());
                for id in &ids {
                    if out.contains_key(id) {
                        continue;
                    }
                    let row: Option<(Vec<u8>, i64, i64)> = st
                        .query_row([id.as_bytes().as_slice()], |r| {
                            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                        })
                        .optional()
                        .sq()?;
                    if let Some((pack, off, len)) = row {
                        let pack: [u8; 16] = pack
                            .as_slice()
                            .try_into()
                            .map_err(|_| StoreError::InvalidArgument("corrupt pack id".into()))?;
                        out.insert(
                            *id,
                            IndexEntry {
                                id: *id,
                                pack,
                                off: off as u64,
                                len: len as u32,
                                share: None,
                            },
                        );
                    }
                }
                Ok(out)
            })
            .await
    }

    async fn index_objects(&self, entries: &[IndexEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let entries = entries.to_vec();
        // Made durable by the commit or ref move that references it (see
        // "Durability" in the module docs).
        let durable = false;
        self.db()
            .write(durable, move |c, _| {
                let now = now_secs();
                let mut st = c
                    .prepare_cached(
                        "INSERT INTO object_index (id, pack, off, len, share, written_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                         ON CONFLICT (id) DO UPDATE SET written_at = excluded.written_at \
                         WHERE object_index.written_at < excluded.written_at - ?7",
                    )
                    .sq()?;
                for e in &entries {
                    st.execute(rusqlite::params![
                        e.id.as_bytes().as_slice(),
                        e.pack.as_slice(),
                        to_i64(e.off, "pack offset")?,
                        i64::from(e.len),
                        e.share.map(i64::from),
                        now,
                        FRESHEN_SECS
                    ])
                    .sq()?;
                }
                Ok(())
            })
            .await
    }

    async fn condemned_among(&self, _ids: &[ObjectId]) -> Result<HashSet<ObjectId>> {
        // Collection runs offline (`gc`): nothing is ever condemned while
        // the store is open.
        Ok(HashSet::new())
    }
}

struct LocalRefLock {
    store: LocalStore,
    name: String,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for LocalRefLock {
    fn drop(&mut self) {
        let mut locks = self.store.inner.locks.lock();
        drop(self.guard.take());
        // Waiters hold clones of the entry; remove it only when none remain.
        if locks.get(&self.name).is_some_and(|m| Arc::strong_count(m) == 1) {
            locks.remove(&self.name);
        }
    }
}

#[async_trait]
impl RefLockRelease for LocalRefLock {
    async fn release(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn can_publish(&self) -> bool {
        true
    }

    /// Move the ref and insert the derived rows in one durable transaction.
    /// Every write acknowledged before (deferred ones included) precedes it
    /// in the WAL, so its fsync makes them durable too.
    async fn publish(
        &mut self,
        expected: Option<ObjectId>,
        new: ObjectId,
        kind: &str,
        derived: Vec<(Vec<u8>, Bytes)>,
    ) -> Result<CasOutcome> {
        let (name, kind) = (self.name.clone(), kind.to_owned());
        self.store
            .db()
            .write(true, move |c, fx| {
                let actual = cas_in(c, &name, expected, Some(new))?;
                if actual != expected {
                    return Ok(CasOutcome::Mismatch { actual });
                }
                insert_derived(c, &kind, &derived)?;
                if expected != Some(new) {
                    fx.moves.push(RefMove {
                        name,
                        old: expected,
                        new: Some(new),
                        cause: MoveCause::Land,
                        nonce: crate::feed::nonce(),
                    });
                }
                Ok(CasOutcome::Updated)
            })
            .await
    }
}

#[async_trait]
impl RefStore for LocalStore {
    async fn get_ref(&self, name: &str) -> Result<Option<ObjectId>> {
        validate_ref_name(name)?;
        let name = name.to_owned();
        self.db()
            .read(move |c| {
                let t: Option<Vec<u8>> = c
                    .prepare_cached("SELECT target FROM refs WHERE name = ?1")
                    .sq()?
                    .query_row([&name], |r| r.get(0))
                    .optional()
                    .sq()?;
                t.map(|b| id_of(&b)).transpose()
            })
            .await
    }

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ObjectId>,
        new: Option<ObjectId>,
    ) -> Result<CasOutcome> {
        validate_ref_name(name)?;
        let name = name.to_owned();
        let cause = crate::feed::move_cause();
        self.db()
            .write(true, move |c, fx: &mut Effects| {
                let actual = cas_in(c, &name, expected, new)?;
                if actual != expected {
                    return Ok(CasOutcome::Mismatch { actual });
                }
                if expected != new {
                    fx.moves.push(RefMove {
                        name,
                        old: expected,
                        new,
                        cause,
                        nonce: crate::feed::nonce(),
                    });
                }
                Ok(CasOutcome::Updated)
            })
            .await
    }

    async fn list_refs(&self, prefix: &str) -> Result<Vec<(String, ObjectId)>> {
        let prefix = prefix.to_owned();
        self.db()
            .read(move |c| {
                let mut st = c
                    .prepare_cached("SELECT name, target FROM refs WHERE name >= ?1 ORDER BY name")
                    .sq()?;
                let mut rows = st.query([&prefix]).sq()?;
                let mut out = Vec::new();
                while let Some(r) = rows.next().sq()? {
                    let name: String = r.get(0).sq()?;
                    if !name.starts_with(&prefix) {
                        break;
                    }
                    let target: Vec<u8> = r.get(1).sq()?;
                    out.push((name, id_of(&target)?));
                }
                Ok(out)
            })
            .await
    }

    async fn lock_ref(&self, name: &str) -> Result<RefLock> {
        validate_ref_name(name)?;
        let mutex = self.inner.locks.lock().entry(name.to_string()).or_default().clone();
        let mut lock = LocalRefLock { store: self.clone(), name: name.to_string(), guard: None };
        // `lock` owns the entry from here, so a cancelled wait still cleans up.
        lock.guard = Some(mutex.lock_owned().await);
        Ok(RefLock::held(lock))
    }

    fn watch_refs(&self) -> Option<tokio::sync::broadcast::Receiver<crate::feed::RefNotice>> {
        Some(self.inner.feed.subscribe())
    }
}

/// A `commits` row: id, tree, parents, generation, committer time.
type CommitRow = (Vec<u8>, Vec<u8>, Vec<u8>, i64, i64);

/// A `commits` row read by [`GET_COMMIT`].
fn commit_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<CommitRow> {
    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
}

const GET_COMMIT: &str =
    "SELECT id, tree, parents, generation, committer_time FROM commits WHERE id = ?1";

fn node_of(row: CommitRow) -> Result<CommitNode> {
    let (id, tree, parents, generation, committer_time) = row;
    Ok(CommitNode {
        id: id_of(&id)?,
        tree: id_of(&tree)?,
        parents: decode_ids(&parents)?,
        generation: generation as u64,
        committer_time,
    })
}

/// Look up one commit node with a cached statement.
fn get_node(st: &mut rusqlite::CachedStatement<'_>, id: &ObjectId) -> Result<Option<CommitNode>> {
    st.query_row([id.as_bytes().as_slice()], commit_row).optional().sq()?.map(node_of).transpose()
}

#[async_trait]
impl MetaStore for LocalStore {
    async fn put_commit(&self, node: CommitNode, changes: Vec<PathChange>) -> Result<u64> {
        Ok(self.put_commits(vec![(node, changes)]).await?[0])
    }

    async fn put_commits(&self, commits: Vec<(CommitNode, Vec<PathChange>)>) -> Result<Vec<u64>> {
        if commits.is_empty() {
            return Ok(Vec::new());
        }
        let order: Vec<ObjectId> = commits.iter().map(|(n, _)| n.id).collect();
        // Equal ids are equal commits: only the first occurrence is recorded.
        let mut first = HashSet::with_capacity(commits.len());
        let unique: Vec<(CommitNode, Vec<PathChange>)> =
            commits.into_iter().filter(|(n, _)| first.insert(n.id)).collect();
        for (n, _) in &unique {
            to_i64(n.generation, "generation")?;
        }
        // Encoding (zstd) is CPU work: off the async threads, and never on
        // the single writer thread.
        let encode = move || -> Result<Vec<(CommitNode, Vec<u8>)>> {
            unique.into_iter().map(|(n, c)| Ok((n, changeset_codec::encode(&c)?))).collect()
        };
        let rows = if first.len() > 16 {
            tokio::task::spawn_blocking(encode).await.map_err(StoreError::backend)??
        } else {
            encode()?
        };
        let durable = !crate::durability_deferred();
        let seqs = self
            .db()
            .write(durable, move |c, _| {
                let now = now_secs();
                let mut next: i64 = c
                    .query_row("SELECT coalesce(max(seq), 0) + 1 FROM commits", [], |r| r.get(0))
                    .sq()?;
                let mut seq_of = c.prepare_cached("SELECT seq FROM commits WHERE id = ?1").sq()?;
                let mut insert = c
                    .prepare_cached(
                        "INSERT INTO commits \
                         (id, tree, parents, generation, committer_time, seq, written_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    )
                    .sq()?;
                let mut insert_cs = c
                    .prepare_cached(
                        "INSERT INTO changesets (commit_id, data) VALUES (?1, ?2) \
                         ON CONFLICT (commit_id) DO NOTHING",
                    )
                    .sq()?;
                let mut seqs: HashMap<ObjectId, u64> = HashMap::with_capacity(rows.len());
                for (n, cs) in &rows {
                    let id = n.id.as_bytes().as_slice();
                    let existing: Option<i64> =
                        seq_of.query_row([id], |r| r.get(0)).optional().sq()?;
                    let seq = match existing {
                        Some(s) => s,
                        None => {
                            let parents: Vec<u8> = n
                                .parents
                                .iter()
                                .flat_map(|p| p.as_bytes().iter().copied())
                                .collect();
                            insert_cs.execute(rusqlite::params![id, cs]).sq()?;
                            insert
                                .execute(rusqlite::params![
                                    id,
                                    n.tree.as_bytes().as_slice(),
                                    parents,
                                    n.generation as i64,
                                    n.committer_time,
                                    next,
                                    now
                                ])
                                .sq()?;
                            next += 1;
                            next - 1
                        }
                    };
                    seqs.insert(n.id, seq as u64);
                }
                Ok(seqs)
            })
            .await?;
        order
            .iter()
            .map(|id| {
                seqs.get(id)
                    .copied()
                    .ok_or_else(|| StoreError::InvalidArgument("commit missing from log".into()))
            })
            .collect()
    }

    async fn get_commits(&self, ids: &[ObjectId]) -> Result<Vec<Option<CommitNode>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = ids.to_vec();
        self.db()
            .read(move |c| {
                let mut st = c.prepare_cached(GET_COMMIT).sq()?;
                ids.iter().map(|id| get_node(&mut st, id)).collect()
            })
            .await
    }

    async fn get_changeset(&self, id: &ObjectId) -> Result<Option<Vec<PathChange>>> {
        Ok(self.get_changesets(std::slice::from_ref(id)).await?.pop().flatten())
    }

    async fn get_changesets(&self, ids: &[ObjectId]) -> Result<Vec<Option<Vec<PathChange>>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = ids.to_vec();
        self.db()
            .read(move |c| {
                let mut st = c
                    .prepare_cached(
                        "SELECT cs.data FROM commits c JOIN changesets cs ON cs.commit_id = c.id \
                         WHERE c.id = ?1",
                    )
                    .sq()?;
                ids.iter()
                    .map(|id| {
                        let data: Option<Vec<u8>> = st
                            .query_row([id.as_bytes().as_slice()], |r| r.get(0))
                            .optional()
                            .sq()?;
                        data.map(|d| changeset_codec::decode(&d)).transpose()
                    })
                    .collect()
            })
            .await
    }

    async fn first_parent_chain(
        &self,
        tip: ObjectId,
        stop: Option<ObjectId>,
        limit: usize,
    ) -> Result<Vec<CommitNode>> {
        self.db()
            .read(move |c| {
                let mut st = c.prepare_cached(GET_COMMIT).sq()?;
                let mut out = Vec::new();
                let mut next = Some(tip);
                while let Some(id) = next.filter(|id| Some(*id) != stop) {
                    if out.len() >= limit {
                        break;
                    }
                    let Some(node) = get_node(&mut st, &id)? else { break };
                    next = node.parents.first().copied();
                    out.push(node);
                }
                Ok(out)
            })
            .await
    }

    async fn ancestors_above(
        &self,
        tips: &[ObjectId],
        min_generation: u64,
        limit: usize,
    ) -> Result<Vec<CommitNode>> {
        if tips.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let tips = tips.to_vec();
        self.db()
            .read(move |c| {
                let mut st = c.prepare_cached(GET_COMMIT).sq()?;
                let mut seen = HashSet::new();
                let mut queue: VecDeque<ObjectId> =
                    tips.iter().copied().filter(|id| seen.insert(*id)).collect();
                let mut out = Vec::new();
                while let Some(id) = queue.pop_front() {
                    if out.len() >= limit {
                        break;
                    }
                    let Some(n) = get_node(&mut st, &id)? else { continue };
                    if n.generation <= min_generation {
                        continue;
                    }
                    queue.extend(n.parents.iter().copied().filter(|p| seen.insert(*p)));
                    out.push(n);
                }
                Ok(out)
            })
            .await
    }

    async fn tail(&self, after: u64, limit: usize) -> Result<Vec<(u64, ObjectId)>> {
        let Ok(after) = i64::try_from(after) else { return Ok(Vec::new()) };
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.db()
            .read(move |c| {
                let mut st = c
                    .prepare_cached(
                        "SELECT seq, id FROM commits WHERE seq > ?1 ORDER BY seq LIMIT ?2",
                    )
                    .sq()?;
                let rows = st
                    .query_map([after, limit], |r| {
                        Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
                    })
                    .sq()?;
                rows.map(|r| {
                    let (seq, id) = r.sq()?;
                    Ok((seq as u64, id_of(&id)?))
                })
                .collect()
            })
            .await
    }

    async fn log_seqs(&self, ids: &[ObjectId]) -> Result<Vec<Option<u64>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = ids.to_vec();
        self.db()
            .read(move |c| {
                let mut st = c.prepare_cached("SELECT seq FROM commits WHERE id = ?1").sq()?;
                ids.iter()
                    .map(|id| {
                        let s: Option<i64> = st
                            .query_row([id.as_bytes().as_slice()], |r| r.get(0))
                            .optional()
                            .sq()?;
                        Ok(s.map(|s| s as u64))
                    })
                    .collect()
            })
            .await
    }

    async fn get_derived(&self, kind: &str, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.get_derived_many(kind, &[key]).await?.pop().flatten())
    }

    async fn get_derived_many(&self, kind: &str, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let kind = kind.to_owned();
        let keys: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        self.db()
            .read(move |c| {
                let mut st = c
                    .prepare_cached("SELECT value FROM derived WHERE kind = ?1 AND key = ?2")
                    .sq()?;
                keys.iter()
                    .map(|k| {
                        let v: Option<Vec<u8>> = st
                            .query_row(rusqlite::params![kind, k], |r| r.get(0))
                            .optional()
                            .sq()?;
                        Ok(v.map(Bytes::from))
                    })
                    .collect()
            })
            .await
    }

    async fn put_derived(&self, kind: &str, key: &[u8], value: Bytes) -> Result<()> {
        self.put_derived_many(kind, vec![(key.to_vec(), value)]).await
    }

    async fn put_derived_many(&self, kind: &str, entries: Vec<(Vec<u8>, Bytes)>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let kind = kind.to_owned();
        // Made durable by the commit or ref move that references it (see
        // "Durability" in the module docs).
        let durable = false;
        self.db().write(durable, move |c, _| insert_derived(c, &kind, &entries)).await
    }

    async fn index_history(&self, ref_name: &str, rows: &[HistoryRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        for r in rows {
            to_i64(r.generation, "generation")?;
        }
        let ref_name = ref_name.to_owned();
        let rows = rows.to_vec();
        // Not fsynced: the watermark move that follows (durable) covers them.
        self.db()
            .write(false, move |c, _| {
                let mut st = c
                    .prepare_cached(
                        "INSERT INTO history (ref, path, generation, commit_id) \
                         VALUES (?1, ?2, ?3, ?4) ON CONFLICT DO NOTHING",
                    )
                    .sq()?;
                for r in &rows {
                    st.execute(rusqlite::params![
                        ref_name,
                        r.path.as_str(),
                        r.generation as i64,
                        r.commit.as_bytes().as_slice()
                    ])
                    .sq()?;
                }
                Ok(())
            })
            .await
    }

    async fn query_history(
        &self,
        ref_name: &str,
        prefix: &RepoPath,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<(u64, ObjectId)>> {
        let max = i64::try_from(max_generation).unwrap_or(i64::MAX);
        let limit_i = i64::try_from(limit).unwrap_or(i64::MAX);
        let ref_name = ref_name.to_owned();
        let p = prefix.as_str().to_owned();
        self.db()
            .read(move |c| {
                let parse =
                    |r: &rusqlite::Row<'_>| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?));
                let mut hits: Vec<(u64, ObjectId)> = Vec::new();
                if p.is_empty() {
                    let mut st = c
                        .prepare_cached(
                            "SELECT DISTINCT generation, commit_id FROM history \
                             INDEXED BY history_by_gen WHERE ref = ?1 AND generation <= ?2 \
                             ORDER BY generation DESC, commit_id DESC LIMIT ?3",
                        )
                        .sq()?;
                    for r in st.query_map(rusqlite::params![ref_name, max, limit_i], parse).sq()? {
                        let (g, id) = r.sq()?;
                        hits.push((g as u64, id_of(&id)?));
                    }
                    return Ok(hits);
                }
                // The path itself, then everything under `p/` (`/` + 1 = `0`):
                // ranges of the primary key; sorted by generation here.
                let mut exact = c
                    .prepare_cached(
                        "SELECT generation, commit_id FROM history \
                         WHERE ref = ?1 AND path = ?2 AND generation <= ?3",
                    )
                    .sq()?;
                for r in exact.query_map(rusqlite::params![ref_name, p, max], parse).sq()? {
                    let (g, id) = r.sq()?;
                    hits.push((g as u64, id_of(&id)?));
                }
                let mut under = c
                    .prepare_cached(
                        "SELECT generation, commit_id FROM history \
                         WHERE ref = ?1 AND path >= ?2 AND path < ?3 AND generation <= ?4",
                    )
                    .sq()?;
                let (lo, hi) = (format!("{p}/"), format!("{p}0"));
                for r in under.query_map(rusqlite::params![ref_name, lo, hi, max], parse).sq()? {
                    let (g, id) = r.sq()?;
                    hits.push((g as u64, id_of(&id)?));
                }
                hits.sort_unstable_by(|a, b| b.cmp(a));
                hits.dedup();
                hits.truncate(limit);
                Ok(hits)
            })
            .await
    }

    async fn history_watermark(&self, ref_name: &str) -> Result<Option<ObjectId>> {
        let ref_name = ref_name.to_owned();
        self.db()
            .read(move |c| {
                let v: Option<Vec<u8>> = c
                    .prepare_cached("SELECT commit_id FROM history_watermark WHERE ref = ?1")
                    .sq()?
                    .query_row([&ref_name], |r| r.get(0))
                    .optional()
                    .sq()?;
                v.map(|b| id_of(&b)).transpose()
            })
            .await
    }

    async fn set_history_watermark(
        &self,
        ref_name: &str,
        expected: Option<ObjectId>,
        new: ObjectId,
    ) -> Result<bool> {
        let ref_name = ref_name.to_owned();
        self.db()
            .write(true, move |c, _| {
                let n = match expected {
                    None => c
                        .prepare_cached(
                            "INSERT INTO history_watermark (ref, commit_id) VALUES (?1, ?2) \
                             ON CONFLICT (ref) DO NOTHING",
                        )
                        .sq()?
                        .execute(rusqlite::params![ref_name, new.as_bytes().as_slice()])
                        .sq()?,
                    Some(e) => c
                        .prepare_cached(
                            "UPDATE history_watermark SET commit_id = ?3 \
                             WHERE ref = ?1 AND commit_id = ?2",
                        )
                        .sq()?
                        .execute(rusqlite::params![
                            ref_name,
                            e.as_bytes().as_slice(),
                            new.as_bytes().as_slice()
                        ])
                        .sq()?,
                };
                Ok(n == 1)
            })
            .await
    }

    async fn clear_history(&self, ref_name: &str) -> Result<()> {
        let ref_name = ref_name.to_owned();
        self.db()
            .write(true, move |c, _| {
                c.prepare_cached("DELETE FROM history WHERE ref = ?1")
                    .sq()?
                    .execute([&ref_name])
                    .sq()?;
                c.prepare_cached("DELETE FROM history_watermark WHERE ref = ?1")
                    .sq()?
                    .execute([&ref_name])
                    .sq()?;
                Ok(())
            })
            .await
    }
}

#[cfg(test)]
mod tests;
