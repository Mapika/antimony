//! Versioned schema: numbered, ordered, idempotent migrations.
//!
//! - Every schema records the migrations applied to it in `schema_version`
//!   (`version` primary key, `name`, `applied_at`, `applied_by`). Its version
//!   is the highest recorded one; 0 means none recorded: a new schema, or one
//!   written before versioning existed (the [`MIGRATIONS`] baseline).
//! - [`migrate`] applies the missing migrations in order, in one transaction
//!   holding a transaction-scoped advisory lock on the schema. Replicas
//!   starting at the same time therefore migrate exactly once: the first
//!   one migrates, the others wait for the lock, re-read the version and find
//!   nothing left to do. A schema already at [`SCHEMA_VERSION`] is detected
//!   with two reads and no lock (the common startup).
//! - Every migration is idempotent (`IF NOT EXISTS`, guarded `ALTER`s), so
//!   an unversioned schema written by any earlier release runs all of them
//!   and ends at the current layout without touching existing data.
//! - A schema whose version is newer than [`SCHEMA_VERSION`] is never read or
//!   written: [`StoreError::NewerFormat`] (upgrade ant). Without migrating
//!   ([`super::PgOptions::migrate`] off), an older schema is
//!   [`StoreError::MigrationRequired`].
//!
//! New layouts are added as a new last entry of [`MIGRATIONS`] (never by
//! editing an applied one) together with a bump of [`SCHEMA_VERSION`];
//! readers may keep fallbacks for data written before it (legacy
//! `changes` rows, one-object-per-key S3 objects).

use ant_core::NewerFormat;
use sqlx::{Connection, PgConnection};

use super::{be, quote_ident};
use crate::{Result, StoreError};

/// The schema version this binary reads and writes.
pub const SCHEMA_VERSION: u32 = 6;

/// One schema migration.
pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    pub description: &'static str,
    /// Statements, given the quoted schema identifier and the same as an SQL
    /// string literal body (single quotes doubled).
    ddl: fn(qs: &str, ql: &str) -> Vec<String>,
}

/// All migrations, in order; `MIGRATIONS[i].version == i + 1`.
pub static MIGRATIONS: [Migration; SCHEMA_VERSION as usize] = [
    Migration {
        version: 1,
        name: "baseline",
        description: "objects, refs, commit graph, log, per-path changes, derived data, \
                      history index (the oldest layout still read)",
        ddl: baseline,
    },
    Migration {
        version: 2,
        name: "object_index",
        description: "index of large objects bundled into S3 packs",
        ddl: object_index,
    },
    Migration {
        version: 3,
        name: "changesets",
        description: "one compressed changeset row per commit",
        ddl: changesets,
    },
    Migration {
        version: 4,
        name: "storage_tuning",
        description: "out-of-line uncompressed storage for compressed columns; \
                      insert-only autovacuum thresholds",
        ddl: storage_tuning,
    },
    Migration {
        version: 5,
        name: "gc",
        description: "write times of objects, packed-object index rows, commits and changesets; \
                      garbage-collection state (condemned ids, mark working set, run state)",
        ddl: gc,
    },
    Migration {
        version: 6,
        name: "pack_frames",
        description: "S3 packs may hold multi-object frames (unreadable by versions before \
                      0.1.5); objects in a frame record their share of its bytes",
        ddl: pack_frames,
    },
];

fn baseline(qs: &str, _ql: &str) -> Vec<String> {
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {qs}.objects (\
                id bytea PRIMARY KEY, \
                data bytea NOT NULL)"
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
        // Changesets of commits recorded before migration 3, one row per path.
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
    ]
}

/// Where large objects live inside S3 packs (see `crate::packed`). Objects
/// stored one per S3 key before it stay readable (looked up by key).
fn object_index(qs: &str, _ql: &str) -> Vec<String> {
    vec![format!(
        "CREATE TABLE IF NOT EXISTS {qs}.object_index (\
            id bytea PRIMARY KEY, \
            pack bytea NOT NULL, \
            off bigint NOT NULL, \
            len integer NOT NULL)"
    )]
}

/// Commits recorded before it keep their per-path `changes` rows.
fn changesets(qs: &str, _ql: &str) -> Vec<String> {
    vec![format!(
        "CREATE TABLE IF NOT EXISTS {qs}.changesets (\
            commit_id bytea PRIMARY KEY, \
            data bytea NOT NULL)"
    )]
}

fn storage_tuning(qs: &str, ql: &str) -> Vec<String> {
    vec![
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
    ]
}

/// Garbage collection (`super::gc`). Existing rows get the migration time
/// as their write time (a column default added without rewriting the
/// table), so nothing written before the upgrade is collectable until a
/// full grace period after it. BRIN indexes make "written since" scans cheap
/// on these insert-mostly tables and cost almost nothing on insert.
fn gc(qs: &str, _ql: &str) -> Vec<String> {
    let mut stmts = Vec::new();
    for table in ["objects", "object_index", "commits", "changesets"] {
        stmts.push(format!(
            "ALTER TABLE {qs}.{table} \
             ADD COLUMN IF NOT EXISTS written_at timestamptz NOT NULL DEFAULT now()"
        ));
        stmts.push(format!(
            "CREATE INDEX IF NOT EXISTS {table}_written_at ON {qs}.{table} \
             USING brin (written_at)"
        ));
    }
    stmts.extend([
        // Ids a garbage-collection run is about to delete: readers treat
        // them as missing, writers that store them again take them back.
        format!("CREATE TABLE IF NOT EXISTS {qs}.gc_condemned (id bytea PRIMARY KEY)"),
        // Mark working set of the current run (rebuilt after a crash).
        format!(
            "CREATE UNLOGGED TABLE IF NOT EXISTS {qs}.gc_live (\
                id bytea PRIMARY KEY, \
                kind smallint NOT NULL, \
                expanded boolean NOT NULL)"
        ),
        format!("CREATE INDEX IF NOT EXISTS gc_live_todo ON {qs}.gc_live (id) WHERE NOT expanded"),
        // The current (or last) run: one row.
        format!(
            "CREATE TABLE IF NOT EXISTS {qs}.gc_state (\
                singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), \
                phase text NOT NULL, \
                started_at timestamptz NOT NULL, \
                cutoff timestamptz NOT NULL, \
                grace_secs double precision NOT NULL, \
                quiesce_secs double precision NOT NULL, \
                condemned_at timestamptz, \
                remarked_at timestamptz, \
                finished_at timestamptz, \
                stats text NOT NULL DEFAULT '')"
        ),
    ]);
    stmts
}

/// Multi-object frames in S3 packs (`crate::packed`). Packs written from
/// this version on may hold records older binaries cannot decode, which is
/// why it is a migration at all: binaries that support only version 5 refuse
/// the schema instead of failing reads. `share` is the part of a frame's
/// compressed bytes attributed to one member (its uncompressed share of the
/// frame), for the garbage collector's byte counts; NULL (every row written
/// before, and objects stored alone) means the whole record (`len`). Adding
/// a nullable column does not rewrite the table.
fn pack_frames(qs: &str, _ql: &str) -> Vec<String> {
    vec![format!("ALTER TABLE {qs}.object_index ADD COLUMN IF NOT EXISTS share integer")]
}

/// Version state of one schema ([`super::PgStore::schema_status`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaStatus {
    pub schema: String,
    /// Highest applied migration; 0: none recorded (a schema that does not
    /// exist yet, or one written before versioning).
    pub version: u32,
    /// [`SCHEMA_VERSION`] of this binary.
    pub supported: u32,
    /// Migrations this binary would apply, in order (empty when the schema is
    /// current or newer).
    pub pending: Vec<&'static Migration>,
}

impl std::fmt::Debug for Migration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.version, self.name)
    }
}

impl PartialEq for Migration {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
    }
}

impl Eq for Migration {}

impl SchemaStatus {
    /// The schema was written by a newer version than this binary.
    pub fn is_newer(&self) -> bool {
        self.version > self.supported
    }

    fn newer_error(&self) -> StoreError {
        newer(&self.schema, self.version)
    }
}

fn newer(schema: &str, version: u32) -> StoreError {
    StoreError::NewerFormat(NewerFormat {
        what: format!("Postgres schema \"{schema}\" version"),
        found: u64::from(version),
        supported: u64::from(SCHEMA_VERSION),
    })
}

/// Recorded version of `schema` (0 when there is none).
async fn read_version(conn: &mut PgConnection, schema: &str) -> Result<u32> {
    let qs = quote_ident(schema);
    // A plain MVCC query, not `to_regclass`: name lookups use a catalog
    // snapshot that taking an advisory lock does not refresh, so right after
    // waiting for another migrator they could miss the table it created.
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_tables \
         WHERE schemaname = $1 AND tablename = 'schema_version')",
    )
    .bind(schema)
    .fetch_one(&mut *conn)
    .await
    .map_err(be)?;
    if !exists {
        return Ok(0);
    }
    // The schema name is quoted by `quote_ident`; nothing else is interpolated.
    let stmt = format!("SELECT coalesce(max(version), 0) FROM {qs}.schema_version");
    let version: i32 =
        sqlx::query_scalar(sqlx::AssertSqlSafe(stmt)).fetch_one(&mut *conn).await.map_err(be)?;
    u32::try_from(version)
        .map_err(|_| StoreError::InvalidArgument(format!("bad schema version {version}")))
}

pub(super) async fn status(conn: &mut PgConnection, schema: &str) -> Result<SchemaStatus> {
    let version = read_version(conn, schema).await?;
    Ok(SchemaStatus {
        schema: schema.to_owned(),
        version,
        supported: SCHEMA_VERSION,
        pending: MIGRATIONS.iter().filter(|m| m.version > version).collect(),
    })
}

/// Fail unless `schema` is exactly at [`SCHEMA_VERSION`].
pub(super) async fn require_current(conn: &mut PgConnection, schema: &str) -> Result<()> {
    let st = status(conn, schema).await?;
    if st.is_newer() {
        return Err(st.newer_error());
    }
    if st.version < SCHEMA_VERSION {
        return Err(StoreError::MigrationRequired {
            schema: schema.to_owned(),
            found: st.version,
            required: SCHEMA_VERSION,
        });
    }
    Ok(())
}

/// Bring `schema` (created if missing) to [`SCHEMA_VERSION`]. Returns the
/// versions this call applied: empty when it was current already (or another
/// process migrated it while this one waited for the lock).
pub(super) async fn migrate(conn: &mut PgConnection, schema: &str) -> Result<Vec<u32>> {
    let current = read_version(conn, schema).await?;
    if current > SCHEMA_VERSION {
        return Err(newer(schema, current));
    }
    if current == SCHEMA_VERSION {
        return Ok(Vec::new());
    }
    let qs = quote_ident(schema);
    let ql = qs.replace('\'', "''");
    let mut tx = conn.begin().await.map_err(be)?;
    // Serializes migrations (and the creation of the schema) per schema.
    // Advisory locks are database-wide; the prefix keeps the key apart from
    // the ref locks (`ant/<schema>/<ref>`).
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("ant-migrate/{schema}"))
        .execute(&mut *tx)
        .await
        .map_err(be)?;
    let current = read_version(&mut tx, schema).await?;
    if current > SCHEMA_VERSION {
        return Err(newer(schema, current));
    }
    if current == SCHEMA_VERSION {
        // Another process migrated while this one waited.
        tx.rollback().await.map_err(be)?;
        return Ok(Vec::new());
    }
    let mut stmts = vec![
        format!("CREATE SCHEMA IF NOT EXISTS {qs}"),
        format!(
            "CREATE TABLE IF NOT EXISTS {qs}.schema_version (\
                version integer PRIMARY KEY, \
                name text NOT NULL, \
                applied_at timestamptz NOT NULL DEFAULT now(), \
                applied_by text NOT NULL)"
        ),
    ];
    let mut applied = Vec::new();
    for m in MIGRATIONS.iter().filter(|m| m.version > current) {
        stmts.extend((m.ddl)(&qs, &ql));
        stmts.push(format!(
            "INSERT INTO {qs}.schema_version (version, name, applied_by) \
             VALUES ({}, '{}', 'ant {}')",
            m.version,
            m.name,
            env!("CARGO_PKG_VERSION"),
        ));
        applied.push(m.version);
    }
    for stmt in &stmts {
        // Only the quoted schema name and constants are interpolated.
        sqlx::query(sqlx::AssertSqlSafe(stmt.as_str())).execute(&mut *tx).await.map_err(be)?;
    }
    tx.commit().await.map_err(be)?;
    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrations_are_numbered_in_order() {
        for (i, m) in MIGRATIONS.iter().enumerate() {
            assert_eq!(m.version as usize, i + 1, "{}", m.name);
            assert!(!m.name.contains('\''));
        }
        assert_eq!(MIGRATIONS.last().map(|m| m.version), Some(SCHEMA_VERSION));
    }

    use std::sync::Arc;

    use super::super::{PgOptions, PgStore, SchemaGuard};
    use crate::RefStore;
    use ant_core::ObjectId;

    use crate::testing::pg_url as url;

    async fn sql(stmt: &str) {
        let mut conn = PgConnection::connect(&url()).await.unwrap();
        sqlx::raw_sql(sqlx::AssertSqlSafe(stmt.to_owned())).execute(&mut conn).await.unwrap();
        conn.close().await.unwrap();
    }

    async fn scalar(stmt: &str) -> i64 {
        let mut conn = PgConnection::connect(&url()).await.unwrap();
        let v: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(stmt.to_owned()))
            .fetch_one(&mut conn)
            .await
            .unwrap();
        conn.close().await.unwrap();
        v
    }

    fn no_migrate() -> PgOptions {
        PgOptions { migrate: false, ..PgOptions::default() }
    }

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_new_schema_is_created_at_the_current_version() {
        let schema = format!("t_mig_{}", rand::random::<u64>());
        let _guard = SchemaGuard::new(&url(), &schema);
        let st = PgStore::schema_status(&url(), &schema).await.unwrap();
        assert_eq!((st.version, st.supported), (0, SCHEMA_VERSION));
        assert_eq!(st.pending.len(), MIGRATIONS.len());
        // Without migrating, a missing schema is an error and stays missing.
        let err = PgStore::connect_with(&url(), &schema, &no_migrate()).await.err().unwrap();
        assert!(
            matches!(err, StoreError::MigrationRequired { found: 0, required: SCHEMA_VERSION, .. }),
            "{err}"
        );
        assert_eq!(
            scalar(&format!(
                "SELECT count(*) FROM information_schema.schemata WHERE schema_name = '{schema}'"
            ))
            .await,
            0
        );
        let s = PgStore::connect(&url(), &schema).await.unwrap();
        s.cas_ref("main", None, Some(ObjectId([1; 32]))).await.unwrap();
        let st = PgStore::schema_status(&url(), &schema).await.unwrap();
        assert_eq!(st.version, SCHEMA_VERSION);
        assert!(st.pending.is_empty());
        // Current: connecting without migrating works, migrating is a no-op.
        PgStore::connect_with(&url(), &schema, &no_migrate()).await.unwrap();
        assert!(PgStore::migrate(&url(), &schema).await.unwrap().is_empty());
        let names = scalar(&format!(
            "SELECT count(*) FROM \"{schema}\".schema_version WHERE applied_by LIKE 'ant %'"
        ))
        .await;
        assert_eq!(names, i64::from(SCHEMA_VERSION));
    }

    /// A schema recorded by a newer version is never read, written or
    /// "migrated", with or without migrations enabled.
    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_newer_schema_is_refused() {
        let schema = format!("t_mig_{}", rand::random::<u64>());
        let s = PgStore::connect_scratch(&url(), &schema).await.unwrap();
        s.cas_ref("main", None, Some(ObjectId([1; 32]))).await.unwrap();
        let newer = SCHEMA_VERSION + 1;
        sql(&format!(
            "INSERT INTO \"{schema}\".schema_version (version, name, applied_by) \
             VALUES ({newer}, 'from_the_future', 'ant 99')"
        ))
        .await;
        let check = |err: StoreError| match err {
            StoreError::NewerFormat(n) => {
                assert_eq!((n.found, n.supported), (u64::from(newer), u64::from(SCHEMA_VERSION)));
                assert!(n.what.contains(&schema), "{n}");
            }
            other => panic!("expected NewerFormat, got {other}"),
        };
        check(PgStore::connect(&url(), &schema).await.err().unwrap());
        check(PgStore::connect_with(&url(), &schema, &no_migrate()).await.err().unwrap());
        check(PgStore::migrate(&url(), &schema).await.err().unwrap());
        let st = PgStore::schema_status(&url(), &schema).await.unwrap();
        assert!(st.is_newer());
        assert!(st.pending.is_empty());
        // Nothing was added by the refused attempts.
        assert_eq!(
            scalar(&format!("SELECT count(*) FROM \"{schema}\".schema_version")).await,
            i64::from(newer)
        );
        assert_eq!(s.get_ref("main").await.unwrap(), Some(ObjectId([1; 32])));
    }

    /// Replicas starting together: exactly one migrates, the others wait and
    /// then find the schema current.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_concurrent_startup_migrates_exactly_once() {
        for round in 0..3 {
            let schema = format!("t_mig_{}", rand::random::<u64>());
            let _guard = SchemaGuard::new(&url(), &schema);
            if round == 2 {
                // An unversioned schema of the oldest layout.
                let qs = quote_ident(&schema);
                sql(&format!("CREATE SCHEMA {qs}")).await;
                for stmt in baseline(&qs, "") {
                    sql(&stmt).await;
                }
            }
            let barrier = Arc::new(tokio::sync::Barrier::new(4));
            let tasks: Vec<_> = (0..4)
                .map(|_| {
                    let (schema, barrier) = (schema.clone(), barrier.clone());
                    tokio::spawn(async move {
                        barrier.wait().await;
                        if round == 0 {
                            // Through the store's startup, as replicas do.
                            let opts = PgOptions { max_connections: 2, ..PgOptions::default() };
                            let s = PgStore::connect_with(&url(), &schema, &opts).await.unwrap();
                            s.get_ref("main").await.unwrap();
                            None
                        } else {
                            Some(PgStore::migrate(&url(), &schema).await.unwrap())
                        }
                    })
                })
                .collect();
            let mut applied = Vec::new();
            for t in tasks {
                applied.push(t.await.unwrap());
            }
            if round > 0 {
                let all: Vec<u32> = (1..=SCHEMA_VERSION).collect();
                let migrated: Vec<_> = applied.iter().flatten().filter(|a| !a.is_empty()).collect();
                assert_eq!(migrated, vec![&all], "round {round}: {applied:?}");
            }
            // One row per migration, all from one transaction.
            let q = format!(
                "SELECT count(*), count(DISTINCT applied_at) FROM \"{schema}\".schema_version"
            );
            let mut conn = PgConnection::connect(&url()).await.unwrap();
            let (rows, txs): (i64, i64) =
                sqlx::query_as(sqlx::AssertSqlSafe(q)).fetch_one(&mut conn).await.unwrap();
            conn.close().await.unwrap();
            assert_eq!((rows, txs), (i64::from(SCHEMA_VERSION), 1), "round {round}");
        }
    }
}
