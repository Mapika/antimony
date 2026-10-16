//! Versioned SQLite schema of a local data directory (`meta.db`).
//!
//! Like the Postgres schema (`crate::postgres::migrate`): numbered, ordered
//! migrations recorded in `schema_version` (`version` primary key, `name`,
//! `applied_at` Unix seconds, `applied_by`). Opening a store applies the
//! missing ones in one transaction; a newer schema is never read or written
//! ([`StoreError::NewerFormat`]); without migrating it is
//! [`StoreError::MigrationRequired`]. New layouts are added as a new last
//! entry of [`MIGRATIONS`] (never by editing an applied one) with a bump of
//! [`SCHEMA_VERSION`].

use ant_core::NewerFormat;
use rusqlite::Connection;

use crate::{Result, StoreError};

/// The schema version this binary reads and writes.
pub const SCHEMA_VERSION: u32 = 1;

/// One schema migration.
pub struct Migration {
    pub version: u32,
    pub name: &'static str,
    pub description: &'static str,
    sql: &'static str,
}

impl std::fmt::Debug for Migration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.version, self.name)
    }
}

/// All migrations, in order; `MIGRATIONS[i].version == i + 1`.
pub static MIGRATIONS: [Migration; SCHEMA_VERSION as usize] = [Migration {
    version: 1,
    name: "baseline",
    description: "small objects, packed-object index, refs, commit graph with log sequence, \
                  changesets, derived data, history index (with write times for gc)",
    sql: BASELINE,
}];

/// Ids are 32-byte blobs; times are Unix seconds; `generation` and `seq`
/// are positive integers below 2^63.
const BASELINE: &str = "
CREATE TABLE objects (
    id BLOB PRIMARY KEY,
    data BLOB NOT NULL,
    written_at INTEGER NOT NULL
);
CREATE TABLE object_index (
    id BLOB PRIMARY KEY,
    pack BLOB NOT NULL,
    off INTEGER NOT NULL,
    len INTEGER NOT NULL,
    share INTEGER,
    written_at INTEGER NOT NULL
);
CREATE INDEX object_index_by_pack ON object_index (pack);
CREATE TABLE refs (
    name TEXT PRIMARY KEY,
    target BLOB NOT NULL
) WITHOUT ROWID;
CREATE TABLE commits (
    id BLOB PRIMARY KEY,
    tree BLOB NOT NULL,
    parents BLOB NOT NULL,
    generation INTEGER NOT NULL,
    committer_time INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    written_at INTEGER NOT NULL
) WITHOUT ROWID;
CREATE UNIQUE INDEX commits_by_seq ON commits (seq);
CREATE TABLE changesets (
    commit_id BLOB PRIMARY KEY,
    data BLOB NOT NULL
);
CREATE TABLE derived (
    kind TEXT NOT NULL,
    key BLOB NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (kind, key)
);
CREATE TABLE history (
    ref TEXT NOT NULL,
    path TEXT NOT NULL,
    generation INTEGER NOT NULL,
    commit_id BLOB NOT NULL,
    PRIMARY KEY (ref, path, generation, commit_id)
) WITHOUT ROWID;
CREATE INDEX history_by_gen ON history (ref, generation, commit_id);
CREATE TABLE history_watermark (
    ref TEXT PRIMARY KEY,
    commit_id BLOB NOT NULL
) WITHOUT ROWID;
";

/// Version state of a local store's schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaStatus {
    /// Highest applied migration; 0: a new database.
    pub version: u32,
    /// [`SCHEMA_VERSION`] of this binary.
    pub supported: u32,
}

fn sql(e: rusqlite::Error) -> StoreError {
    StoreError::backend(e)
}

fn newer(version: u32) -> StoreError {
    StoreError::NewerFormat(NewerFormat {
        what: "local store schema version".into(),
        found: u64::from(version),
        supported: u64::from(SCHEMA_VERSION),
    })
}

pub(crate) fn read_version(conn: &Connection) -> Result<u32> {
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' \
             AND name = 'schema_version')",
            [],
            |r| r.get(0),
        )
        .map_err(sql)?;
    if !exists {
        return Ok(0);
    }
    let v: i64 = conn
        .query_row("SELECT coalesce(max(version), 0) FROM schema_version", [], |r| r.get(0))
        .map_err(sql)?;
    u32::try_from(v).map_err(|_| StoreError::InvalidArgument(format!("bad schema version {v}")))
}

pub(crate) fn status(conn: &Connection) -> Result<SchemaStatus> {
    Ok(SchemaStatus { version: read_version(conn)?, supported: SCHEMA_VERSION })
}

/// Bring the schema to [`SCHEMA_VERSION`] (`migrate`), or check that it is
/// there. Returns the versions applied.
pub(crate) fn migrate(conn: &Connection, migrate: bool, dir: &str) -> Result<Vec<u32>> {
    let current = read_version(conn)?;
    if current > SCHEMA_VERSION {
        return Err(newer(current));
    }
    if current == SCHEMA_VERSION {
        return Ok(Vec::new());
    }
    if !migrate {
        return Err(StoreError::MigrationRequired {
            schema: dir.to_owned(),
            found: current,
            required: SCHEMA_VERSION,
        });
    }
    conn.execute_batch("BEGIN IMMEDIATE").map_err(sql)?;
    let applied = (|| {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at INTEGER NOT NULL,
                applied_by TEXT NOT NULL)",
        )
        .map_err(sql)?;
        let mut applied = Vec::new();
        let by = format!("ant {}", env!("CARGO_PKG_VERSION"));
        for m in MIGRATIONS.iter().filter(|m| m.version > current) {
            conn.execute_batch(m.sql).map_err(sql)?;
            conn.execute(
                "INSERT INTO schema_version (version, name, applied_at, applied_by) \
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![m.version, m.name, super::now_secs(), by],
            )
            .map_err(sql)?;
            applied.push(m.version);
        }
        Ok(applied)
    })();
    match applied {
        Ok(v) => {
            conn.execute_batch("COMMIT").map_err(sql)?;
            Ok(v)
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}
