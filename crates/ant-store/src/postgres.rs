//! Postgres-backed `RefStore` + `MetaStore` + small-object `ObjectStore`.
//!
//! - All tables live in the schema passed to [`PgStore::connect`], which
//!   creates the schema and tables idempotently (`CREATE ... IF NOT EXISTS`).
//!   Queries are built at runtime (`sqlx::query`), not with the `query!`
//!   macros: there is no DATABASE_URL at build time.
//! - Refs: one row per ref; `cas_ref` is a single atomic statement
//!   (`UPDATE ... WHERE name=$1 AND target=$2`, `INSERT ... ON CONFLICT DO
//!   NOTHING`, `DELETE ... WHERE target=$2`) and reports
//!   `Mismatch { actual }` by re-reading when 0 rows were affected.
//! - Log sequence numbers come from an identity column (gaps are fine).
//!   `put_commit` inserts the node row and changeset in one atomic statement
//!   (a single round trip); for an existing commit it returns the existing
//!   seq.
//! - Changesets: one `changes` row per path, read back in written order.
//! - Objects: `bytea` keyed by the 32-byte id, zstd-compressed.
//! - `get_many`/`has_many` are one round trip each (`WHERE id = ANY($1)`).
//! - Pool: `sqlx::postgres::PgPoolOptions`, max 64 connections (raised from
//!   16: with 64 concurrent agents the write path queued on the pool; fleet
//!   bench 64 agents ~790 → ~1270 lands/s), with `search_path` set to the
//!   schema on every connection (`after_connect`). [`PgStore::connect_with`]
//!   sets the limits ([`PgOptions`]) so several server processes fit under
//!   the database's `max_connections`.
//! - `lock_ref`: a session-level `pg_advisory_lock` on a hash of
//!   (schema, ref) held by a connection of a separate, small, lazily opened
//!   lock pool (holders never compete with the queries they lock for).
//!   Waits are bounded by `lock_timeout`; an unreleased lock closes its
//!   connection, which releases it server-side. `RefLock::publish` runs the
//!   ref CAS and the derived-row inserts as one statement on that same
//!   connection (synchronous commit).
//! - Inside [`crate::deferred_durability`], object, changeset, commit and
//!   derived writes commit with `SET LOCAL synchronous_commit = off`.

use std::collections::{HashMap, HashSet};

use ant_core::{EncodedObject, EntryMode, ObjectId, RepoPath};
use async_trait::async_trait;
use bytes::Bytes;
use sqlx::Row;

use crate::changeset_codec;

use crate::{
    CasOutcome, ChangeKind, CommitNode, MetaStore, ObjectStore, PathChange, RefStore, Result,
    StoreError, validate_ref_name,
};

type PgQuery<'q> = sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>;

/// Compression level for stored object bodies (matches the S3 store).
const ZSTD_LEVEL: i32 = 3;

/// Environment switch for bulk loads: object inserts commit without waiting for
/// the WAL flush (see [`PgStore::with_async_object_commit`]).
pub const BULK_ENV: &str = "ANT_PG_BULK";

/// Location of a packed object: `len` bytes at `off` of pack `pack`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexEntry {
    pub id: ObjectId,
    pub pack: [u8; 16],
    pub off: u64,
    pub len: u32,
}

/// Uncompressed bytes of objects hashed and compressed per blocking task.
const COMPRESS_CHUNK_BYTES: usize = 512 * 1024;

#[derive(Clone)]
pub struct PgStore {
    pool: sqlx::PgPool,
    /// Connections that hold (or wait for) ref locks, separate from `pool`
    /// so lock holders can never starve the work they lock for. `None`
    /// (`lock_connections: 0`): no ref locks, for a single server process.
    locks: Option<sqlx::PgPool>,
    schema: String,
    async_object_commit: bool,
}

/// Connection limits of a [`PgStore`]. Every process connected to one
/// database adds up to `max_connections + lock_connections` connections.
#[derive(Debug, Clone)]
pub struct PgOptions {
    /// Query pool size.
    pub max_connections: u32,
    /// Ref-lock pool size: refs that can be locked (or awaited) at once by
    /// this process. Connections close after a minute idle. 0 disables ref
    /// locks (fine for a single server process: its land queue already
    /// serializes each ref).
    pub lock_connections: u32,
    /// Longest wait for a ref lock before `lock_ref` fails (callers then
    /// fall back to plain CAS).
    pub lock_timeout: std::time::Duration,
}

impl Default for PgOptions {
    fn default() -> Self {
        Self {
            max_connections: 64,
            lock_connections: 8,
            lock_timeout: std::time::Duration::from_secs(10),
        }
    }
}

/// Quote a SQL identifier, doubling any embedded double quotes.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn be(e: impl std::error::Error + Send + Sync + 'static) -> StoreError {
    StoreError::backend(e)
}

fn change_kind_from_code(c: i16) -> Result<ChangeKind> {
    Ok(match c {
        0 => ChangeKind::Added,
        1 => ChangeKind::Modified,
        2 => ChangeKind::Deleted,
        other => {
            return Err(StoreError::InvalidArgument(format!("bad change kind {other}")));
        }
    })
}

fn object_id_col(row: &sqlx::postgres::PgRow, col: &str) -> Result<ObjectId> {
    let bytes: Vec<u8> = row.try_get(col).map_err(be)?;
    Ok(ObjectId::from_bytes(&bytes)?)
}

fn change_side(
    mode: Option<String>,
    oid: Option<Vec<u8>>,
) -> Result<Option<(EntryMode, ObjectId)>> {
    match (mode, oid) {
        (Some(m), Some(o)) => {
            Ok(Some((EntryMode::from_octal(m.as_bytes())?, ObjectId::from_bytes(&o)?)))
        }
        (None, None) => Ok(None),
        _ => Err(StoreError::InvalidArgument("corrupt changeset row".into())),
    }
}

fn decode_ids(bytes: &[u8]) -> Result<Vec<ObjectId>> {
    bytes.chunks_exact(ObjectId::LEN).map(|c| Ok(ObjectId::from_bytes(c)?)).collect()
}

fn ids_to_vecs(ids: &[ObjectId]) -> Vec<Vec<u8>> {
    ids.iter().map(|id| id.as_bytes().to_vec()).collect()
}

/// Connection counts of a [`PgStore`]'s query pool ([`PgStore::pool_stats`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    /// Open connections (idle plus in use).
    pub size: u32,
    /// Open connections waiting in the pool.
    pub idle: u32,
    /// Configured maximum.
    pub max: u32,
}

impl PoolStats {
    pub fn in_use(&self) -> u32 {
        self.size.saturating_sub(self.idle)
    }
}

impl PgStore {
    /// Commit object inserts with `synchronous_commit = off` (also enabled by the
    /// `ANT_PG_BULK` environment variable at connect time).
    ///
    /// Objects are immutable and content-addressed, so a crash can at worst
    /// lose objects whose insert returned a moment ago; Postgres flushes WAL in
    /// order, so any later synchronous commit (the commit records and ref
    /// updates that reference those objects) makes them durable first. A
    /// writer that records commits only after storing their objects (as the
    /// importer and the server do) therefore keeps its guarantees while bulk
    /// loads skip one fsync per batch.
    pub fn with_async_object_commit(mut self, on: bool) -> Self {
        self.async_object_commit = on;
        self
    }

    /// Store changeset blobs ahead of the commit rows that make them reachable.
    /// They are the bulkiest part of recording commits, so large batches are
    /// inserted by several statements at once. A crash in between leaves
    /// unreferenced rows only; a commit never exists without its changeset.
    async fn insert_changesets(&self, ids: &[&[u8]], data: &[Vec<u8>]) -> Result<()> {
        const PART: usize = 256;
        // Key-ordered, like every batch write here, so concurrent writers of
        // overlapping commits cannot deadlock.
        let mut order: Vec<usize> = (0..ids.len()).collect();
        order.sort_unstable_by_key(|&i| ids[i]);
        let ids: Vec<&[u8]> = order.iter().map(|&i| ids[i]).collect();
        let data: Vec<&[u8]> = order.iter().map(|&i| data[i].as_slice()).collect();
        let parts = ids.chunks(PART).zip(data.chunks(PART)).map(|(ids, data)| async move {
            let query = sqlx::query(
                "INSERT INTO changesets (commit_id, data) \
                 SELECT * FROM unnest($1::bytea[], $2::bytea[]) ON CONFLICT (commit_id) DO NOTHING",
            )
            .bind(ids)
            .bind(data);
            self.write(query, crate::durability_deferred()).await
        });
        futures::future::try_join_all(parts).await?;
        Ok(())
    }

    /// Run one write statement and return its rows. `deferred`: commit it
    /// with `synchronous_commit = off` (acknowledged before the WAL flush;
    /// see [`crate::deferred_durability`] and [`Self::with_async_object_commit`]).
    async fn write(
        &self,
        query: PgQuery<'_>,
        deferred: bool,
    ) -> Result<Vec<sqlx::postgres::PgRow>> {
        if !deferred {
            return query.fetch_all(&self.pool).await.map_err(be);
        }
        let mut tx =
            self.pool.begin_with("BEGIN; SET LOCAL synchronous_commit = off").await.map_err(be)?;
        let rows = query.fetch_all(&mut *tx).await.map_err(be)?;
        tx.commit().await.map_err(be)?;
        Ok(rows)
    }

    /// Whether object writes are deferred (bulk mode or a deferred scope).
    fn objects_deferred(&self) -> bool {
        self.async_object_commit || crate::durability_deferred()
    }

    /// Record where objects live inside packs; first writer wins (identical content).
    pub(crate) async fn index_objects(&self, entries: &[IndexEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut sorted: Vec<&IndexEntry> = entries.iter().collect();
        sorted.sort_unstable_by_key(|e| e.id);
        sorted.dedup_by_key(|e| e.id);
        let ids: Vec<&[u8]> = sorted.iter().map(|e| e.id.as_bytes().as_slice()).collect();
        let packs: Vec<&[u8]> = sorted.iter().map(|e| e.pack.as_slice()).collect();
        let offs: Vec<i64> = sorted.iter().map(|e| e.off as i64).collect();
        let lens: Vec<i32> = sorted.iter().map(|e| e.len as i32).collect();
        let insert = sqlx::query(
            "INSERT INTO object_index (id, pack, off, len) \
             SELECT * FROM unnest($1::bytea[], $2::bytea[], $3::bigint[], $4::integer[]) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(&ids)
        .bind(&packs)
        .bind(&offs)
        .bind(&lens);
        self.write(insert, self.objects_deferred()).await?;
        Ok(())
    }

    pub(crate) async fn lookup_index(
        &self,
        ids: &[ObjectId],
    ) -> Result<HashMap<ObjectId, IndexEntry>> {
        let rows = sqlx::query("SELECT id, pack, off, len FROM object_index WHERE id = ANY($1)")
            .bind(ids_to_vecs(ids))
            .fetch_all(&self.pool)
            .await
            .map_err(be)?;
        let mut out = HashMap::with_capacity(rows.len());
        for r in &rows {
            let id = object_id_col(r, "id")?;
            let pack: Vec<u8> = r.try_get("pack").map_err(be)?;
            let pack: [u8; 16] = pack
                .as_slice()
                .try_into()
                .map_err(|_| StoreError::InvalidArgument("corrupt pack id".into()))?;
            let off: i64 = r.try_get("off").map_err(be)?;
            let len: i32 = r.try_get("len").map_err(be)?;
            out.insert(id, IndexEntry { id, pack, off: off as u64, len: len as u32 });
        }
        Ok(out)
    }

    /// Changesets stored as one row per path (databases written by earlier versions).
    async fn legacy_changesets(
        &self,
        ids: &[ObjectId],
    ) -> Result<HashMap<ObjectId, Vec<PathChange>>> {
        let rows = sqlx::query(
            "SELECT commit_id, path, kind, old_mode, old_id, new_mode, new_id FROM changes \
             WHERE commit_id = ANY($1) ORDER BY commit_id, idx",
        )
        .bind(ids_to_vecs(ids))
        .fetch_all(&self.pool)
        .await
        .map_err(be)?;
        let mut map: HashMap<ObjectId, Vec<PathChange>> =
            ids.iter().map(|id| (*id, Vec::new())).collect();
        for r in &rows {
            let id = object_id_col(r, "commit_id")?;
            let path: String = r.try_get("path").map_err(be)?;
            map.entry(id).or_default().push(PathChange {
                path: RepoPath::new(path)?,
                kind: change_kind_from_code(r.try_get("kind").map_err(be)?)?,
                old: change_side(
                    r.try_get("old_mode").map_err(be)?,
                    r.try_get("old_id").map_err(be)?,
                )?,
                new: change_side(
                    r.try_get("new_mode").map_err(be)?,
                    r.try_get("new_id").map_err(be)?,
                )?,
            });
        }
        Ok(map)
    }

    /// Current state of the query pool (for monitoring).
    pub fn pool_stats(&self) -> PoolStats {
        PoolStats {
            size: self.pool.size(),
            idle: u32::try_from(self.pool.num_idle()).unwrap_or(u32::MAX),
            max: self.pool.options().get_max_connections(),
        }
    }

    /// Connect and ensure `schema` and all tables exist.
    pub async fn connect(url: &str, schema: &str) -> Result<Self> {
        Self::connect_with(url, schema, &PgOptions::default()).await
    }

    /// [`Self::connect`] with explicit connection limits.
    pub async fn connect_with(url: &str, schema: &str, options: &PgOptions) -> Result<Self> {
        use sqlx::Connection;

        let qs = quote_ident(schema);
        let ql = qs.replace('\'', "''");

        // Create the schema and tables under a transaction-scoped advisory lock
        // so concurrent connects to a fresh schema do not race on the catalog.
        let mut conn = sqlx::PgConnection::connect(url).await.map_err(be)?;
        let mut tx = conn.begin().await.map_err(be)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(schema)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        let ddl = [
            format!("CREATE SCHEMA IF NOT EXISTS {qs}"),
            format!(
                "CREATE TABLE IF NOT EXISTS {qs}.objects (\
                    id bytea PRIMARY KEY, \
                    data bytea NOT NULL)"
            ),
            // Where large objects live inside S3 packs (see `crate::packed`).
            format!(
                "CREATE TABLE IF NOT EXISTS {qs}.object_index (\
                    id bytea PRIMARY KEY, \
                    pack bytea NOT NULL, \
                    off bigint NOT NULL, \
                    len integer NOT NULL)"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {qs}.refs (\
                    name text PRIMARY KEY, \
                    target bytea NOT NULL)"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {qs}.commits (\
                    id bytea PRIMARY KEY, \
                    tree bytea NOT NULL, \
                    parents bytea NOT NULL, \
                    generation bigint NOT NULL, \
                    committer_time bigint NOT NULL)"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {qs}.log (\
                    seq bigserial PRIMARY KEY, \
                    commit_id bytea NOT NULL UNIQUE)"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {qs}.changes (\
                    commit_id bytea NOT NULL, \
                    idx integer NOT NULL, \
                    path text NOT NULL, \
                    kind smallint NOT NULL, \
                    old_mode text, \
                    old_id bytea, \
                    new_mode text, \
                    new_id bytea, \
                    PRIMARY KEY (commit_id, idx))"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {qs}.changesets (\
                    commit_id bytea PRIMARY KEY, \
                    data bytea NOT NULL)"
            ),
            // Object bodies and changesets are already compressed: store them
            // out of line without Postgres trying (and failing) to compress them.
            // Guarded: these ALTERs take heavy locks, so run them only once.
            format!(
                "DO $$ BEGIN \
                 IF (SELECT attstorage FROM pg_attribute \
                     WHERE attrelid = '{ql}.objects'::regclass AND attname = 'data') <> 'e' THEN \
                   ALTER TABLE {qs}.objects ALTER COLUMN data SET STORAGE EXTERNAL; \
                 END IF; \
                 IF (SELECT attstorage FROM pg_attribute \
                     WHERE attrelid = '{ql}.changesets'::regclass AND attname = 'data') <> 'e' THEN \
                   ALTER TABLE {qs}.changesets ALTER COLUMN data SET STORAGE EXTERNAL; \
                 END IF; \
                 END $$"
            ),
            // Insert-only tables: vacuum when they have doubled, not every 20%.
            format!(
                "DO $$ BEGIN \
                 IF (SELECT reloptions IS NULL FROM pg_class \
                     WHERE oid = '{ql}.objects'::regclass) THEN \
                   ALTER TABLE {qs}.objects SET (autovacuum_vacuum_insert_scale_factor = 1.0, \
                     autovacuum_vacuum_insert_threshold = 200000, \
                     toast.autovacuum_vacuum_insert_scale_factor = 1.0, \
                     toast.autovacuum_vacuum_insert_threshold = 200000); \
                 END IF; \
                 IF (SELECT reloptions IS NULL FROM pg_class \
                     WHERE oid = '{ql}.changesets'::regclass) THEN \
                   ALTER TABLE {qs}.changesets SET (autovacuum_vacuum_insert_scale_factor = 1.0, \
                     autovacuum_vacuum_insert_threshold = 200000); \
                 END IF; \
                 END $$"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {qs}.derived (\
                    kind text NOT NULL, \
                    key bytea NOT NULL, \
                    value bytea NOT NULL, \
                    PRIMARY KEY (kind, key))"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {qs}.history (\
                    ref text NOT NULL, \
                    path text NOT NULL, \
                    generation bigint NOT NULL, \
                    commit_id bytea NOT NULL, \
                    PRIMARY KEY (ref, path, generation, commit_id))"
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS history_by_gen ON {qs}.history \
                    (ref, generation DESC)"
            ),
            format!(
                "CREATE INDEX IF NOT EXISTS history_by_path ON {qs}.history \
                    (ref, path text_pattern_ops, generation DESC)"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {qs}.history_watermark (\
                    ref text PRIMARY KEY, \
                    commit_id bytea NOT NULL)"
            ),
        ];
        for stmt in &ddl {
            // Schema name is quoted by `quote_ident`; no user data is interpolated.
            sqlx::query(sqlx::AssertSqlSafe(stmt.as_str())).execute(&mut *tx).await.map_err(be)?;
        }
        tx.commit().await.map_err(be)?;
        conn.close().await.map_err(be)?;

        let search_path = format!("SET search_path TO {qs}");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(options.max_connections)
            .after_connect(move |conn, _meta| {
                let stmt = search_path.clone();
                Box::pin(async move {
                    sqlx::query(sqlx::AssertSqlSafe(stmt.as_str())).execute(&mut *conn).await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .map_err(be)?;

        // Lock waits are bounded server-side; a failed wait leaves the
        // connection clean.
        let lock_timeout = format!("SET lock_timeout = {}", options.lock_timeout.as_millis());
        let locks = (options.lock_connections > 0)
            .then(|| {
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(options.lock_connections)
                    .min_connections(0)
                    .idle_timeout(Some(std::time::Duration::from_secs(60)))
                    .acquire_timeout(options.lock_timeout)
                    .after_connect(move |conn, _meta| {
                        let stmt = lock_timeout.clone();
                        Box::pin(async move {
                            sqlx::query(sqlx::AssertSqlSafe(stmt.as_str()))
                                .execute(&mut *conn)
                                .await?;
                            Ok(())
                        })
                    })
                    .connect_lazy(url)
                    .map_err(be)
            })
            .transpose()?;

        let async_object_commit = std::env::var(BULK_ENV).is_ok_and(|v| !v.is_empty() && v != "0");
        Ok(Self { pool, locks, schema: schema.to_owned(), async_object_commit })
    }
}

/// A held session-level advisory lock (`pg_advisory_lock`) and the
/// connection holding it. Released with `pg_advisory_unlock`, after which the
/// connection goes back to the lock pool; a lock dropped unreleased (or a
/// failed release) closes its connection instead, which makes the server drop
/// the lock, so a locked connection is never reused.
///
/// The same connection publishes (`RefLock::publish`): the lock pool sets no
/// `search_path`, so its statements name the schema explicitly.
struct PgRefLock {
    conn: Option<sqlx::pool::PoolConnection<sqlx::Postgres>>,
    key: String,
    /// The locked ref.
    name: String,
    /// Quoted schema identifier.
    schema: String,
}

impl Drop for PgRefLock {
    fn drop(&mut self) {
        if let Some(conn) = &mut self.conn {
            conn.close_on_drop();
        }
    }
}

#[async_trait]
impl crate::RefLockRelease for PgRefLock {
    async fn release(mut self: Box<Self>) -> Result<()> {
        let Some(conn) = self.conn.as_mut() else { return Ok(()) };
        let released: bool =
            sqlx::query_scalar("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
                .bind(&self.key)
                .fetch_one(&mut **conn)
                .await
                .map_err(be)?;
        if !released {
            return Err(StoreError::InvalidArgument(format!("ref lock {} was not held", self.key)));
        }
        // Unlocked: return the connection to the pool (Drop sees None).
        drop(self.conn.take());
        Ok(())
    }

    fn can_publish(&self) -> bool {
        self.conn.is_some()
    }

    /// One statement (one synchronous commit) on the lock's session: move the
    /// ref and, only if it moved, insert the derived rows.
    async fn publish(
        &mut self,
        expected: Option<ObjectId>,
        new: ObjectId,
        kind: &str,
        derived: Vec<(Vec<u8>, Bytes)>,
    ) -> Result<CasOutcome> {
        let Some(conn) = self.conn.as_mut() else {
            return Err(StoreError::InvalidArgument(format!("ref lock {} not held", self.key)));
        };
        // Key order for a consistent lock order across concurrent writers.
        let mut derived = derived;
        derived.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        derived.dedup_by(|a, b| a.0 == b.0);
        let (keys, values): (Vec<Vec<u8>>, Vec<Vec<u8>>) =
            derived.into_iter().map(|(k, v)| (k, v.to_vec())).unzip();
        let qs = &self.schema;
        let moved = match expected {
            Some(_) => format!(
                "UPDATE {qs}.refs SET target = $3 WHERE name = $1 AND target = $2 RETURNING 1"
            ),
            None => format!(
                "INSERT INTO {qs}.refs (name, target) SELECT $1, $3 WHERE $2::bytea IS NULL \
                 ON CONFLICT (name) DO NOTHING RETURNING 1"
            ),
        };
        let stmt = format!(
            "WITH moved AS ({moved}), \
             rows AS (INSERT INTO {qs}.derived (kind, key, value) \
                 SELECT $4, k, v FROM unnest($5::bytea[], $6::bytea[]) AS t(k, v) \
                 WHERE EXISTS (SELECT 1 FROM moved) \
                 ON CONFLICT (kind, key) DO NOTHING) \
             SELECT count(*) FROM moved"
        );
        // The schema is quoted by `quote_ident`; everything else is bound.
        let moved: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(stmt))
            .bind(&self.name)
            .bind(expected.map(|e| e.as_bytes().to_vec()))
            .bind(new.as_bytes().as_slice())
            .bind(kind)
            .bind(&keys)
            .bind(&values)
            .fetch_one(&mut **conn)
            .await
            .map_err(be)?;
        if moved == 1 {
            return Ok(CasOutcome::Updated);
        }
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT target FROM {qs}.refs WHERE name = $1"
        )))
        .bind(&self.name)
        .fetch_optional(&mut **conn)
        .await
        .map_err(be)?;
        let actual = row.map(|r| object_id_col(&r, "target")).transpose()?;
        Ok(CasOutcome::Mismatch { actual })
    }
}

#[async_trait]
impl ObjectStore for PgStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query("SELECT id, data FROM objects WHERE id = ANY($1)")
            .bind(ids_to_vecs(ids))
            .fetch_all(&self.pool)
            .await
            .map_err(be)?;
        let raw: Vec<(ObjectId, Vec<u8>)> = rows
            .iter()
            .map(|r| Ok((object_id_col(r, "id")?, r.try_get("data").map_err(be)?)))
            .collect::<Result<_>>()?;
        // Decompress and verify off the async threads.
        let decoded: HashMap<ObjectId, EncodedObject> = tokio::task::spawn_blocking(move || {
            raw.into_iter()
                .map(|(id, data)| {
                    let bytes = zstd::decode_all(&data[..]).map_err(be)?;
                    let obj = EncodedObject::from_bytes(Bytes::from(bytes))?;
                    obj.verify(&id)?;
                    Ok((id, obj))
                })
                .collect::<Result<HashMap<_, _>>>()
        })
        .await
        .map_err(be)??;
        Ok(ids.iter().map(|id| decoded.get(id).cloned()).collect())
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }
        // Hash and compress on the blocking pool, in parallel chunks.
        let mut chunks: Vec<Vec<EncodedObject>> = Vec::new();
        let (mut current, mut bytes) = (Vec::new(), 0usize);
        for o in objects {
            bytes += o.as_bytes().len();
            current.push(o.clone());
            if bytes >= COMPRESS_CHUNK_BYTES {
                chunks.push(std::mem::take(&mut current));
                bytes = 0;
            }
        }
        if !current.is_empty() {
            chunks.push(current);
        }
        let tasks = chunks.into_iter().map(|chunk| {
            tokio::task::spawn_blocking(move || -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
                // One compression context per chunk: creating one per object
                // costs more than compressing a small object.
                let mut zstd = zstd::bulk::Compressor::new(ZSTD_LEVEL).map_err(be)?;
                chunk
                    .into_iter()
                    .map(|raw| {
                        let id = raw.id();
                        Ok((id.as_bytes().to_vec(), zstd.compress(raw.as_bytes()).map_err(be)?))
                    })
                    .collect()
            })
        });
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(objects.len());
        for part in futures::future::try_join_all(tasks).await.map_err(be)? {
            rows.extend(part?);
        }
        // Insert in key order so concurrent batches sharing objects take row
        // locks in the same order; arrival order deadlocks (found importing
        // the Linux kernel with concurrent writers).
        rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        rows.dedup_by(|a, b| a.0 == b.0);
        let (ids, data): (Vec<Vec<u8>>, Vec<Vec<u8>>) = rows.into_iter().unzip();
        let insert = sqlx::query(
            "INSERT INTO objects (id, data) SELECT * FROM unnest($1::bytea[], $2::bytea[]) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(&ids)
        .bind(&data);
        self.write(insert, self.objects_deferred()).await?;
        Ok(())
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query("SELECT id FROM objects WHERE id = ANY($1)")
            .bind(ids_to_vecs(ids))
            .fetch_all(&self.pool)
            .await
            .map_err(be)?;
        let present: HashSet<ObjectId> =
            rows.iter().map(|r| object_id_col(r, "id")).collect::<Result<_>>()?;
        Ok(ids.iter().map(|id| present.contains(id)).collect())
    }
}

/// Read a ref's target through any executor (pool or transaction).
async fn read_ref<'e, E>(exec: E, name: &str) -> Result<Option<ObjectId>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let row = sqlx::query("SELECT target FROM refs WHERE name = $1")
        .bind(name)
        .fetch_optional(exec)
        .await
        .map_err(be)?;
    match row {
        Some(r) => Ok(Some(object_id_col(&r, "target")?)),
        None => Ok(None),
    }
}

#[async_trait]
impl RefStore for PgStore {
    async fn get_ref(&self, name: &str) -> Result<Option<ObjectId>> {
        validate_ref_name(name)?;
        read_ref(&self.pool, name).await
    }

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ObjectId>,
        new: Option<ObjectId>,
    ) -> Result<CasOutcome> {
        validate_ref_name(name)?;
        let affected = match (expected, new) {
            (None, Some(n)) => sqlx::query(
                "INSERT INTO refs (name, target) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING",
            )
            .bind(name)
            .bind(n.as_bytes().as_slice())
            .execute(&self.pool)
            .await
            .map_err(be)?
            .rows_affected(),
            (Some(e), Some(n)) => {
                sqlx::query("UPDATE refs SET target = $3 WHERE name = $1 AND target = $2")
                    .bind(name)
                    .bind(e.as_bytes().as_slice())
                    .bind(n.as_bytes().as_slice())
                    .execute(&self.pool)
                    .await
                    .map_err(be)?
                    .rows_affected()
            }
            (Some(e), None) => sqlx::query("DELETE FROM refs WHERE name = $1 AND target = $2")
                .bind(name)
                .bind(e.as_bytes().as_slice())
                .execute(&self.pool)
                .await
                .map_err(be)?
                .rows_affected(),
            // Deleting "nothing" succeeds iff the ref is absent.
            (None, None) => {
                let actual = read_ref(&self.pool, name).await?;
                return Ok(match actual {
                    None => CasOutcome::Updated,
                    Some(_) => CasOutcome::Mismatch { actual },
                });
            }
        };
        if affected == 1 {
            return Ok(CasOutcome::Updated);
        }
        let actual = read_ref(&self.pool, name).await?;
        Ok(CasOutcome::Mismatch { actual })
    }

    /// A session-level advisory lock keyed by a 64-bit hash of (schema, ref):
    /// advisory locks are database-wide, so the schema keeps independent
    /// repositories apart (a hash collision only serializes two refs).
    async fn lock_ref(&self, name: &str) -> Result<crate::RefLock> {
        validate_ref_name(name)?;
        let Some(locks) = &self.locks else { return Ok(crate::RefLock::unheld()) };
        let conn = locks.acquire().await.map_err(be)?;
        // Owned by the guard before the lock query: an error or cancellation
        // mid-wait closes the connection rather than pooling it.
        let mut lock = PgRefLock {
            conn: Some(conn),
            key: format!("ant/{}/{name}", self.schema),
            name: name.to_owned(),
            schema: quote_ident(&self.schema),
        };
        if let Some(conn) = lock.conn.as_mut() {
            sqlx::query("SELECT pg_advisory_lock(hashtextextended($1, 0))")
                .bind(&lock.key)
                .execute(&mut **conn)
                .await
                .map_err(be)?;
        }
        Ok(crate::RefLock::held(lock))
    }

    async fn list_refs(&self, prefix: &str) -> Result<Vec<(String, ObjectId)>> {
        let rows = sqlx::query(
            "SELECT name, target FROM refs WHERE starts_with(name, $1) ORDER BY name COLLATE \"C\"",
        )
        .bind(prefix)
        .fetch_all(&self.pool)
        .await
        .map_err(be)?;
        rows.iter()
            .map(|r| {
                let name: String = r.try_get("name").map_err(be)?;
                Ok((name, object_id_col(r, "target")?))
            })
            .collect()
    }
}

#[async_trait]
impl MetaStore for PgStore {
    async fn index_history(&self, ref_name: &str, rows: &[crate::HistoryRow]) -> Result<()> {
        // Large batches: one statement per chunk keeps parameter payloads sane.
        // Primary-key order for a consistent lock order across writers.
        let mut rows = rows.to_vec();
        rows.sort_unstable_by(|a, b| {
            (a.path.as_str(), a.generation, a.commit).cmp(&(
                b.path.as_str(),
                b.generation,
                b.commit,
            ))
        });
        for chunk in rows.chunks(50_000) {
            let paths: Vec<&str> = chunk.iter().map(|r| r.path.as_str()).collect();
            let gens: Vec<i64> = chunk.iter().map(|r| r.generation as i64).collect();
            let commits: Vec<&[u8]> =
                chunk.iter().map(|r| r.commit.as_bytes().as_slice()).collect();
            sqlx::query(
                "INSERT INTO history (ref, path, generation, commit_id) \
                 SELECT $1, t.p, t.g, t.c FROM unnest($2::text[], $3::bigint[], $4::bytea[]) \
                      AS t(p, g, c) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(ref_name)
            .bind(&paths)
            .bind(&gens)
            .bind(&commits)
            .execute(&self.pool)
            .await
            .map_err(be)?;
        }
        Ok(())
    }

    async fn query_history(
        &self,
        ref_name: &str,
        prefix: &ant_core::RepoPath,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<(u64, ObjectId)>> {
        let max = i64::try_from(max_generation).unwrap_or(i64::MAX);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let p = prefix.as_str();
        let rows = if p.is_empty() {
            sqlx::query(
                "SELECT DISTINCT generation, commit_id FROM history \
                 WHERE ref = $1 AND generation <= $2 \
                 ORDER BY generation DESC, commit_id DESC LIMIT $3",
            )
            .bind(ref_name)
            .bind(max)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
        } else {
            let like =
                format!("{}/%", p.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"));
            // Not a persistent prepared statement: Postgres switches those to
            // a generic plan after a few runs, and a generic plan cannot turn
            // `LIKE $5` into a `history_by_path` range scan. It walks
            // `history_by_gen` filtering every row instead, so a narrow path
            // costs O(history rows) (seen as ~2 s path logs in the soak). An
            // unnamed statement is planned with the actual prefix each time.
            sqlx::query(
                "SELECT DISTINCT generation, commit_id FROM history \
                 WHERE ref = $1 AND (path = $4 OR path LIKE $5) AND generation <= $2 \
                 ORDER BY generation DESC, commit_id DESC LIMIT $3",
            )
            .bind(ref_name)
            .bind(max)
            .bind(limit)
            .bind(p)
            .bind(like)
            .persistent(false)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(be)?;
        rows.iter()
            .map(|r| {
                let g: i64 = r.try_get("generation").map_err(be)?;
                Ok((g as u64, object_id_col(r, "commit_id")?))
            })
            .collect()
    }

    async fn history_watermark(&self, ref_name: &str) -> Result<Option<ObjectId>> {
        let row = sqlx::query("SELECT commit_id FROM history_watermark WHERE ref = $1")
            .bind(ref_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(be)?;
        row.map(|r| object_id_col(&r, "commit_id")).transpose()
    }

    async fn set_history_watermark(
        &self,
        ref_name: &str,
        expected: Option<ObjectId>,
        new: ObjectId,
    ) -> Result<bool> {
        let affected =
            match expected {
                None => {
                    sqlx::query(
                        "INSERT INTO history_watermark (ref, commit_id) VALUES ($1, $2) \
                 ON CONFLICT (ref) DO NOTHING",
                    )
                    .bind(ref_name)
                    .bind(new.as_bytes().as_slice())
                    .execute(&self.pool)
                    .await
                }
                Some(e) => sqlx::query(
                    "UPDATE history_watermark SET commit_id = $3 WHERE ref = $1 AND commit_id = $2",
                )
                .bind(ref_name)
                .bind(e.as_bytes().as_slice())
                .bind(new.as_bytes().as_slice())
                .execute(&self.pool)
                .await,
            }
            .map_err(be)?
            .rows_affected();
        Ok(affected == 1)
    }

    async fn clear_history(&self, ref_name: &str) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(be)?;
        sqlx::query("DELETE FROM history WHERE ref = $1")
            .bind(ref_name)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        sqlx::query("DELETE FROM history_watermark WHERE ref = $1")
            .bind(ref_name)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        tx.commit().await.map_err(be)?;
        Ok(())
    }

    async fn get_derived_many(&self, kind: &str, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query("SELECT key, value FROM derived WHERE kind = $1 AND key = ANY($2)")
            .bind(kind)
            .bind(keys)
            .fetch_all(&self.pool)
            .await
            .map_err(be)?;
        let mut map: HashMap<Vec<u8>, Bytes> = HashMap::with_capacity(rows.len());
        for r in &rows {
            let k: Vec<u8> = r.try_get("key").map_err(be)?;
            let v: Vec<u8> = r.try_get("value").map_err(be)?;
            map.insert(k, Bytes::from(v));
        }
        Ok(keys.iter().map(|k| map.get(*k).cloned()).collect())
    }

    async fn first_parent_chain(
        &self,
        tip: ObjectId,
        stop: Option<ObjectId>,
        limit: usize,
    ) -> Result<Vec<CommitNode>> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let stop = stop.map(|s| s.as_bytes().to_vec());
        let rows = sqlx::query(
            "WITH RECURSIVE chain(id, tree, parents, generation, committer_time, depth) AS ( \
                 SELECT id, tree, parents, generation, committer_time, 1::bigint \
                   FROM commits WHERE id = $1 AND id IS DISTINCT FROM $2::bytea AND $3 > 0 \
                 UNION ALL \
                 SELECT c.id, c.tree, c.parents, c.generation, c.committer_time, ch.depth + 1 \
                   FROM chain ch JOIN commits c ON c.id = substring(ch.parents from 1 for 32) \
                  WHERE ch.depth < $3 \
                    AND c.id IS DISTINCT FROM $2::bytea) \
             SELECT id, tree, parents, generation, committer_time FROM chain ORDER BY depth",
        )
        .bind(tip.as_bytes().as_slice())
        .bind(stop)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(be)?;
        rows.iter()
            .map(|r| {
                let parents: Vec<u8> = r.try_get("parents").map_err(be)?;
                let generation: i64 = r.try_get("generation").map_err(be)?;
                Ok(CommitNode {
                    id: object_id_col(r, "id")?,
                    tree: object_id_col(r, "tree")?,
                    parents: decode_ids(&parents)?,
                    generation: generation as u64,
                    committer_time: r.try_get("committer_time").map_err(be)?,
                })
            })
            .collect()
    }

    async fn get_changesets(&self, ids: &[ObjectId]) -> Result<Vec<Option<Vec<PathChange>>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT c.id AS id, cs.data AS data \
             FROM commits c LEFT JOIN changesets cs ON cs.commit_id = c.id \
             WHERE c.id = ANY($1)",
        )
        .bind(ids_to_vecs(ids))
        .fetch_all(&self.pool)
        .await
        .map_err(be)?;
        let mut map: HashMap<ObjectId, Vec<PathChange>> = HashMap::with_capacity(rows.len());
        // Commits recorded before changesets were stored as one blob keep
        // their per-path rows in `changes`.
        let mut legacy: Vec<ObjectId> = Vec::new();
        for r in &rows {
            let id = object_id_col(r, "id")?;
            match r.try_get::<Option<Vec<u8>>, _>("data").map_err(be)? {
                Some(data) => {
                    map.insert(id, changeset_codec::decode(&data)?);
                }
                None => legacy.push(id),
            }
        }
        if !legacy.is_empty() {
            map.extend(self.legacy_changesets(&legacy).await?);
        }
        Ok(ids.iter().map(|id| map.get(id).cloned()).collect())
    }

    async fn put_commit(&self, node: CommitNode, changes: Vec<PathChange>) -> Result<u64> {
        Ok(self.put_commits(vec![(node, changes)]).await?[0])
    }

    async fn put_commits(&self, commits: Vec<(CommitNode, Vec<PathChange>)>) -> Result<Vec<u64>> {
        if commits.is_empty() {
            return Ok(Vec::new());
        }
        // Equal ids are equal commits: only the first occurrence is recorded.
        let mut first = HashSet::with_capacity(commits.len());
        let unique: Vec<&(CommitNode, Vec<PathChange>)> =
            commits.iter().filter(|(n, _)| first.insert(n.id)).collect();
        let ids: Vec<&[u8]> = unique.iter().map(|(n, _)| n.id.as_bytes().as_slice()).collect();
        let trees: Vec<&[u8]> = unique.iter().map(|(n, _)| n.tree.as_bytes().as_slice()).collect();
        let parents: Vec<Vec<u8>> = unique
            .iter()
            .map(|(n, _)| n.parents.iter().flat_map(|p| p.as_bytes().iter().copied()).collect())
            .collect();
        let gens: Vec<i64> = unique.iter().map(|(n, _)| n.generation as i64).collect();
        let times: Vec<i64> = unique.iter().map(|(n, _)| n.committer_time).collect();
        let changesets: Vec<Vec<u8>> =
            unique.iter().map(|(_, c)| changeset_codec::encode(c)).collect::<Result<_>>()?;
        self.insert_changesets(&ids, &changesets).await?;

        // One atomic statement (no explicit transaction round trips): insert
        // the new nodes, append them to the log in input order (parents
        // first), and return every input's seq. Already-recorded commits keep
        // their seq and changeset (the changesets were stored just before).
        let query = sqlx::query(
            "WITH input AS ( \
                 SELECT * FROM unnest($1::bytea[], $2::bytea[], $3::bytea[], $4::bigint[], \
                                      $5::bigint[]) \
                 WITH ORDINALITY AS t(id, tree, parents, generation, committer_time, ord)), \
             ins AS ( \
                 INSERT INTO commits (id, tree, parents, generation, committer_time) \
                 SELECT id, tree, parents, generation, committer_time FROM input ORDER BY ord \
                 ON CONFLICT (id) DO NOTHING RETURNING id), \
             lg AS ( \
                 INSERT INTO log (commit_id) \
                 SELECT input.id FROM input JOIN ins ON ins.id = input.id ORDER BY input.ord \
                 RETURNING commit_id, seq) \
             SELECT commit_id, seq FROM lg \
             UNION ALL \
             SELECT commit_id, seq FROM log \
              WHERE commit_id = ANY($1) AND commit_id NOT IN (SELECT id FROM ins)",
        )
        .bind(&ids)
        .bind(&trees)
        .bind(&parents)
        .bind(&gens)
        .bind(&times);
        let result = self.write(query, crate::durability_deferred()).await?;
        let mut seqs: HashMap<ObjectId, u64> = HashMap::with_capacity(ids.len());
        for r in &result {
            seqs.insert(
                object_id_col(r, "commit_id")?,
                r.try_get::<i64, _>("seq").map_err(be)? as u64,
            );
        }
        // A commit recorded concurrently by another transaction is invisible
        // to this statement's snapshot; read its seq with a fresh one.
        let late: Vec<&[u8]> = ids
            .iter()
            .copied()
            .filter(|id| ObjectId::from_bytes(id).is_ok_and(|id| !seqs.contains_key(&id)))
            .collect();
        if !late.is_empty() {
            let rows = sqlx::query("SELECT commit_id, seq FROM log WHERE commit_id = ANY($1)")
                .bind(&late)
                .fetch_all(&self.pool)
                .await
                .map_err(be)?;
            for r in &rows {
                seqs.insert(
                    object_id_col(r, "commit_id")?,
                    r.try_get::<i64, _>("seq").map_err(be)? as u64,
                );
            }
        }
        commits
            .iter()
            .map(|(n, _)| {
                seqs.get(&n.id)
                    .copied()
                    .ok_or_else(|| StoreError::InvalidArgument("commit missing from log".into()))
            })
            .collect()
    }

    async fn get_commits(&self, ids: &[ObjectId]) -> Result<Vec<Option<CommitNode>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT id, tree, parents, generation, committer_time FROM commits WHERE id = ANY($1)",
        )
        .bind(ids_to_vecs(ids))
        .fetch_all(&self.pool)
        .await
        .map_err(be)?;
        let mut map: HashMap<ObjectId, CommitNode> = HashMap::with_capacity(rows.len());
        for r in &rows {
            let id = object_id_col(r, "id")?;
            let parents_bytes: Vec<u8> = r.try_get("parents").map_err(be)?;
            let generation: i64 = r.try_get("generation").map_err(be)?;
            let committer_time: i64 = r.try_get("committer_time").map_err(be)?;
            map.insert(
                id,
                CommitNode {
                    id,
                    tree: object_id_col(r, "tree")?,
                    parents: decode_ids(&parents_bytes)?,
                    generation: generation as u64,
                    committer_time,
                },
            );
        }
        Ok(ids.iter().map(|id| map.get(id).cloned()).collect())
    }

    async fn get_changeset(&self, id: &ObjectId) -> Result<Option<Vec<PathChange>>> {
        Ok(self.get_changesets(std::slice::from_ref(id)).await?.pop().flatten())
    }

    async fn tail(&self, after: u64, limit: usize) -> Result<Vec<(u64, ObjectId)>> {
        // `seq` is a BIGINT: a cursor past i64::MAX cannot match any row, and
        // an out-of-range limit means "no limit" (clamp to i64::MAX).
        let Ok(after) = i64::try_from(after) else {
            return Ok(vec![]);
        };
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows =
            sqlx::query("SELECT seq, commit_id FROM log WHERE seq > $1 ORDER BY seq LIMIT $2")
                .bind(after)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
                .map_err(be)?;
        rows.iter()
            .map(|r| {
                let seq: i64 = r.try_get("seq").map_err(be)?;
                Ok((seq as u64, object_id_col(r, "commit_id")?))
            })
            .collect()
    }

    async fn get_derived(&self, kind: &str, key: &[u8]) -> Result<Option<Bytes>> {
        let row = sqlx::query("SELECT value FROM derived WHERE kind = $1 AND key = $2")
            .bind(kind)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(be)?;
        match row {
            Some(r) => Ok(Some(Bytes::from(r.try_get::<Vec<u8>, _>("value").map_err(be)?))),
            None => Ok(None),
        }
    }

    async fn put_derived(&self, kind: &str, key: &[u8], value: Bytes) -> Result<()> {
        sqlx::query(
            "INSERT INTO derived (kind, key, value) VALUES ($1, $2, $3) \
             ON CONFLICT (kind, key) DO NOTHING",
        )
        .bind(kind)
        .bind(key)
        .bind(value.as_ref())
        .execute(&self.pool)
        .await
        .map_err(be)?;
        Ok(())
    }

    async fn put_derived_many(&self, kind: &str, entries: Vec<(Vec<u8>, Bytes)>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        // Key order for a consistent lock order across concurrent writers.
        let mut entries = entries;
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        entries.dedup_by(|a, b| a.0 == b.0);
        let (keys, values): (Vec<Vec<u8>>, Vec<Vec<u8>>) =
            entries.into_iter().map(|(k, v)| (k, v.to_vec())).unzip();
        let query = sqlx::query(
            "INSERT INTO derived (kind, key, value) \
             SELECT $1, k, v FROM unnest($2::bytea[], $3::bytea[]) AS t(k, v) \
             ON CONFLICT (kind, key) DO NOTHING",
        )
        .bind(kind)
        .bind(&keys)
        .bind(&values);
        self.write(query, crate::durability_deferred()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::conformance;

    /// Fresh store in a unique schema. Requires `docker compose up -d`.
    async fn fresh() -> PgStore {
        let url = std::env::var("ANT_TEST_PG_URL")
            .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into());
        let schema = format!("t_{}", rand::random::<u64>());
        PgStore::connect(&url, &schema).await.expect("connect (is `docker compose up -d` running?)")
    }

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_object_store_conforms() {
        conformance::object_store(Arc::new(fresh().await)).await;
    }

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_ref_store_conforms() {
        conformance::ref_store(Arc::new(fresh().await)).await;
    }

    /// Two stores on one schema stand for two server processes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_ref_lock_conforms() {
        let url = std::env::var("ANT_TEST_PG_URL")
            .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into());
        let schema = format!("t_{}", rand::random::<u64>());
        let opts =
            PgOptions { lock_timeout: std::time::Duration::from_secs(120), ..PgOptions::default() };
        let a = PgStore::connect_with(&url, &schema, &opts).await.unwrap();
        let b = PgStore::connect_with(&url, &schema, &opts).await.unwrap();
        conformance::ref_lock(Arc::new(a), Arc::new(b)).await;
    }

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_ref_locks_are_per_schema_and_time_out() {
        let url = std::env::var("ANT_TEST_PG_URL")
            .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into());
        let opts = PgOptions {
            lock_timeout: std::time::Duration::from_millis(300),
            ..PgOptions::default()
        };
        let schema = format!("t_{}", rand::random::<u64>());
        let a = PgStore::connect_with(&url, &schema, &opts).await.unwrap();
        let b = PgStore::connect_with(&url, &schema, &opts).await.unwrap();
        let other = fresh().await;
        let held = a.lock_ref("main").await.unwrap();
        // Same ref name in another schema (repository) is independent.
        other.lock_ref("main").await.unwrap().release().await.unwrap();
        // A wait longer than lock_timeout fails instead of hanging.
        assert!(matches!(b.lock_ref("main").await, Err(StoreError::Backend(_))));
        held.release().await.unwrap();
        b.lock_ref("main").await.unwrap().release().await.unwrap();
        // lock_connections 0: no locks at all.
        let opts = PgOptions { lock_connections: 0, ..PgOptions::default() };
        let c = PgStore::connect_with(&url, &schema, &opts).await.unwrap();
        let held = a.lock_ref("main").await.unwrap();
        assert!(!c.lock_ref("main").await.unwrap().is_held());
        held.release().await.unwrap();
    }

    fn id(n: u8) -> ObjectId {
        ObjectId([n; 32])
    }

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_ref_lock_publishes_ref_and_derived_rows_atomically() {
        let s = fresh().await;
        let row = |k: &[u8], v: &[u8]| (k.to_vec(), Bytes::copy_from_slice(v));
        let mut lock = s.lock_ref("main").await.unwrap();
        assert!(lock.can_publish());
        // Creating the ref.
        assert_eq!(
            lock.publish(None, id(1), "k", vec![row(b"a", b"1")]).await.unwrap(),
            CasOutcome::Updated
        );
        assert_eq!(s.get_ref("main").await.unwrap(), Some(id(1)));
        assert_eq!(s.get_derived("k", b"a").await.unwrap().as_deref(), Some(&b"1"[..]));
        // A mismatch writes no rows.
        assert_eq!(
            lock.publish(Some(id(9)), id(2), "k", vec![row(b"b", b"2")]).await.unwrap(),
            CasOutcome::Mismatch { actual: Some(id(1)) }
        );
        assert_eq!(
            lock.publish(None, id(2), "k", vec![row(b"b", b"2")]).await.unwrap(),
            CasOutcome::Mismatch { actual: Some(id(1)) }
        );
        assert_eq!(s.get_derived("k", b"b").await.unwrap(), None);
        assert_eq!(s.get_ref("main").await.unwrap(), Some(id(1)));
        // Moving it; existing keys keep their first value.
        assert_eq!(
            lock.publish(Some(id(1)), id(2), "k", vec![row(b"b", b"2"), row(b"a", b"x")])
                .await
                .unwrap(),
            CasOutcome::Updated
        );
        assert_eq!(s.get_ref("main").await.unwrap(), Some(id(2)));
        assert_eq!(s.get_derived("k", b"a").await.unwrap().as_deref(), Some(&b"1"[..]));
        assert_eq!(s.get_derived("k", b"b").await.unwrap().as_deref(), Some(&b"2"[..]));
        lock.release().await.unwrap();
        // Without a lock session there is nothing to publish with.
        let url = std::env::var("ANT_TEST_PG_URL")
            .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into());
        let opts = PgOptions { lock_connections: 0, ..PgOptions::default() };
        let unlocked = PgStore::connect_with(&url, &s.schema, &opts).await.unwrap();
        let mut lock = unlocked.lock_ref("main").await.unwrap();
        assert!(!lock.can_publish());
        assert!(lock.publish(Some(id(2)), id(3), "k", vec![]).await.is_err());
        assert_eq!(s.get_ref("main").await.unwrap(), Some(id(2)));
    }

    /// The crash witness: deferred writes followed by the loss of the lock's
    /// session (what any database crash or restart does) never publish.
    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_publish_fails_once_the_lock_session_is_gone() {
        let s = fresh().await;
        let mut lock = s.lock_ref("main").await.unwrap();
        let node = CommitNode {
            id: id(7),
            tree: id(8),
            parents: vec![],
            generation: 1,
            committer_time: 0,
        };
        let object = EncodedObject::new(ant_core::ObjectKind::Blob, b"deferred");
        crate::deferred_durability(async {
            assert!(crate::durability_deferred());
            s.put_many(std::slice::from_ref(&object)).await.unwrap();
            s.put_commits(vec![(node, vec![])]).await.unwrap();
        })
        .await;
        assert!(!crate::durability_deferred());
        // Deferred writes are visible like any other.
        assert!(s.has_many(&[object.id()]).await.unwrap()[0]);
        assert!(s.get_commits(&[id(7)]).await.unwrap()[0].is_some());
        // Kill the session holding the lock, as a server crash would.
        let key = format!("ant/{}/main", s.schema);
        let killed: bool = sqlx::query_scalar(
            "SELECT pg_terminate_backend(pid) FROM pg_locks \
             WHERE locktype = 'advisory' AND granted AND objsubid = 1 \
               AND ((classid::bigint << 32) | objid::bigint) = hashtextextended($1, 0)",
        )
        .bind(&key)
        .fetch_one(&s.pool)
        .await
        .unwrap();
        assert!(killed);
        assert!(lock.publish(None, id(7), "k", vec![(b"k".to_vec(), Bytes::new())]).await.is_err());
        assert_eq!(s.get_ref("main").await.unwrap(), None);
        assert_eq!(s.get_derived("k", b"k").await.unwrap(), None);
        // A new lock (new session) publishes normally.
        drop(lock);
        let mut lock = s.lock_ref("main").await.unwrap();
        assert_eq!(lock.publish(None, id(7), "k", vec![]).await.unwrap(), CasOutcome::Updated);
        lock.release().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_history_index_conforms() {
        conformance::history_index(Arc::new(fresh().await)).await;
    }

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_meta_store_conforms() {
        conformance::meta_store(Arc::new(fresh().await)).await;
    }

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_meta_batch_conforms() {
        conformance::meta_store_batch(Arc::new(fresh().await)).await;
    }

    /// Concurrent batches sharing objects in opposite orders used to deadlock
    /// (seen importing the Linux kernel). Inserts are now key-ordered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_concurrent_overlapping_batches_do_not_deadlock() {
        use ant_core::ObjectKind;
        let s = Arc::new(fresh().await);
        let objs: Vec<EncodedObject> =
            (0..400u32).map(|i| EncodedObject::new(ObjectKind::Blob, &i.to_be_bytes())).collect();
        for round in 0..5 {
            let mut tasks = Vec::new();
            for t in 0..16 {
                let s = s.clone();
                let mut batch: Vec<_> = objs.iter().skip(round * 50).take(300).cloned().collect();
                if t % 2 == 1 {
                    batch.reverse();
                }
                let entries: Vec<(Vec<u8>, Bytes)> = batch
                    .iter()
                    .map(|o| (o.id().as_bytes().to_vec(), Bytes::from_static(b"x")))
                    .collect();
                tasks.push(tokio::spawn(async move {
                    s.put_many(&batch).await.unwrap();
                    s.put_derived_many("k/v1", entries).await.unwrap();
                }));
            }
            for t in tasks {
                t.await.unwrap();
            }
        }
        let ids: Vec<_> = objs.iter().map(|o| o.id()).collect();
        assert!(s.has_many(&ids).await.unwrap().into_iter().take(350).all(|b| b));
    }

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_connect_is_idempotent() {
        let url = std::env::var("ANT_TEST_PG_URL")
            .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into());
        let schema = format!("t_{}", rand::random::<u64>());
        let a = PgStore::connect(&url, &schema).await.unwrap();
        a.cas_ref("main", None, Some(ObjectId([1; 32]))).await.unwrap();
        let b = PgStore::connect(&url, &schema).await.unwrap();
        assert_eq!(b.get_ref("main").await.unwrap(), Some(ObjectId([1; 32])));
    }

    /// Commits recorded before changesets became one blob per commit still
    /// read back from their per-path `changes` rows.
    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_reads_legacy_per_path_changesets() {
        let s = fresh().await;
        let legacy = ObjectId([7; 32]);
        sqlx::query(
            "INSERT INTO commits (id, tree, parents, generation, committer_time) \
             VALUES ($1, $2, ''::bytea, 1, 5)",
        )
        .bind(legacy.as_bytes().as_slice())
        .bind(ObjectId::empty_tree().as_bytes().as_slice())
        .execute(&s.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO log (commit_id) VALUES ($1)")
            .bind(legacy.as_bytes().as_slice())
            .execute(&s.pool)
            .await
            .unwrap();
        let blob = ObjectId([8; 32]);
        for (i, path) in ["a.txt", "b/c.txt"].into_iter().enumerate() {
            sqlx::query(
                "INSERT INTO changes (commit_id, idx, path, kind, old_mode, old_id, new_mode, new_id) \
                 VALUES ($1, $2, $3, 0, NULL, NULL, '100644', $4)",
            )
            .bind(legacy.as_bytes().as_slice())
            .bind(i as i32)
            .bind(path)
            .bind(blob.as_bytes().as_slice())
            .execute(&s.pool)
            .await
            .unwrap();
        }
        // A commit with no changes at all, also legacy.
        let bare = ObjectId([9; 32]);
        sqlx::query(
            "INSERT INTO commits (id, tree, parents, generation, committer_time) \
             VALUES ($1, $2, $3, 2, 6)",
        )
        .bind(bare.as_bytes().as_slice())
        .bind(ObjectId::empty_tree().as_bytes().as_slice())
        .bind(legacy.as_bytes().as_slice())
        .execute(&s.pool)
        .await
        .unwrap();

        let want = vec![
            PathChange {
                path: RepoPath::new("a.txt").unwrap(),
                kind: ChangeKind::Added,
                old: None,
                new: Some((EntryMode::File, blob)),
            },
            PathChange {
                path: RepoPath::new("b/c.txt").unwrap(),
                kind: ChangeKind::Added,
                old: None,
                new: Some((EntryMode::File, blob)),
            },
        ];
        assert_eq!(s.get_changeset(&legacy).await.unwrap(), Some(want.clone()));
        assert_eq!(s.get_changeset(&bare).await.unwrap(), Some(vec![]));
        assert_eq!(s.get_changeset(&ObjectId([1; 32])).await.unwrap(), None);
        // Mixed with a new-style commit in one call.
        let fresh_id = ObjectId([3; 32]);
        let node = CommitNode {
            id: fresh_id,
            tree: ObjectId::empty_tree(),
            parents: vec![],
            generation: 1,
            committer_time: 1,
        };
        s.put_commit(node, want.clone()).await.unwrap();
        let got = s.get_changesets(&[fresh_id, legacy, ObjectId([1; 32]), bare]).await.unwrap();
        assert_eq!(got, vec![Some(want.clone()), Some(want), None, Some(vec![])]);
    }

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_tail_handles_extreme_bounds() {
        let s = fresh().await;
        let c1 = ObjectId([1; 32]);
        let n1 = CommitNode {
            id: c1,
            tree: ObjectId::empty_tree(),
            parents: vec![],
            generation: 1,
            committer_time: 100,
        };
        let s1 = s.put_commit(n1, vec![]).await.unwrap();
        // A cursor past i64::MAX cannot match any row (and must not wrap negative).
        assert_eq!(s.tail(u64::MAX, 10).await.unwrap(), vec![]);
        assert_eq!(s.tail(i64::MAX as u64 + 1, 10).await.unwrap(), vec![]);
        // "No limit" must not be sent to Postgres as a negative LIMIT.
        assert_eq!(s.tail(0, usize::MAX).await.unwrap(), vec![(s1, c1)]);
        assert_eq!(s.tail(s1, usize::MAX).await.unwrap(), vec![]);
    }
}
