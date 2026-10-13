//! Postgres-backed `RefStore` + `MetaStore` + small-object `ObjectStore`.
//!
//! TASK (store-pg): implement everything marked `todo!()`.
//!
//! Requirements:
//! - All tables live in the schema passed to [`PgStore::connect`]; create the
//!   schema and tables idempotently on connect (`CREATE ... IF NOT EXISTS`).
//!   Use runtime queries (`sqlx::query`), NOT the `query!` macros — there is
//!   no DATABASE_URL at build time.
//! - Refs: one row per ref; `cas_ref` must be a single atomic statement or a
//!   transaction (e.g. `UPDATE ... WHERE name=$1 AND target=$2`, `INSERT ...
//!   ON CONFLICT DO NOTHING`, `DELETE ... WHERE target=$2`), then report
//!   `Mismatch { actual }` by re-reading when 0 rows were affected.
//! - Log sequence numbers: a `BIGSERIAL`/identity column is fine (gaps OK).
//!   `put_commit` inserts the node row and changeset in one transaction;
//!   on conflict (already exists) return the existing seq.
//! - Changesets may be stored as one row per path or as an encoded blob per
//!   commit — your choice; ordering on read must match what was written.
//! - Objects: `bytea` keyed by the 32-byte id, zstd-compressed.
//! - `get_many`/`has_many` must be ONE round trip (`WHERE id = ANY($1)`), not
//!   one query per id.
//! - Pool: `sqlx::postgres::PgPoolOptions`, max 16 connections, and set
//!   `search_path` to the schema on every connection (`after_connect`).

use std::collections::{HashMap, HashSet};

use ant_core::{EncodedObject, EntryMode, ObjectId, RepoPath};
use async_trait::async_trait;
use bytes::Bytes;
use sqlx::Row;

use crate::{
    CasOutcome, ChangeKind, CommitNode, MetaStore, ObjectStore, PathChange, RefStore, Result,
    StoreError, validate_ref_name,
};

/// Compression level for stored object bodies (matches the S3 store).
const ZSTD_LEVEL: i32 = 3;

#[derive(Clone)]
pub struct PgStore {
    pool: sqlx::PgPool,
}

/// Quote a SQL identifier, doubling any embedded double quotes.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn be(e: impl std::error::Error + Send + Sync + 'static) -> StoreError {
    StoreError::backend(e)
}

fn change_kind_code(k: ChangeKind) -> i16 {
    match k {
        ChangeKind::Added => 0,
        ChangeKind::Modified => 1,
        ChangeKind::Deleted => 2,
    }
}

fn change_kind_from_code(c: i16) -> Result<ChangeKind> {
    Ok(match c {
        0 => ChangeKind::Added,
        1 => ChangeKind::Modified,
        2 => ChangeKind::Deleted,
        other => {
            return Err(StoreError::InvalidArgument(format!(
                "bad change kind {other}"
            )));
        }
    })
}

fn object_id_col(row: &sqlx::postgres::PgRow, col: &str) -> Result<ObjectId> {
    let bytes: Vec<u8> = row.try_get(col).map_err(be)?;
    Ok(ObjectId::from_bytes(&bytes)?)
}

fn decode_ids(bytes: &[u8]) -> Result<Vec<ObjectId>> {
    bytes
        .chunks_exact(ObjectId::LEN)
        .map(|c| Ok(ObjectId::from_bytes(c)?))
        .collect()
}

fn ids_to_vecs(ids: &[ObjectId]) -> Vec<Vec<u8>> {
    ids.iter().map(|id| id.as_bytes().to_vec()).collect()
}

impl PgStore {
    /// Connect and ensure `schema` and all tables exist.
    pub async fn connect(url: &str, schema: &str) -> Result<Self> {
        use sqlx::Connection;

        let qs = quote_ident(schema);

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
                "CREATE TABLE IF NOT EXISTS {qs}.derived (\
                    kind text NOT NULL, \
                    key bytea NOT NULL, \
                    value bytea NOT NULL, \
                    PRIMARY KEY (kind, key))"
            ),
        ];
        for stmt in &ddl {
            // Schema name is quoted by `quote_ident`; no user data is interpolated.
            sqlx::query(sqlx::AssertSqlSafe(stmt.as_str()))
                .execute(&mut *tx)
                .await
                .map_err(be)?;
        }
        tx.commit().await.map_err(be)?;
        conn.close().await.map_err(be)?;

        let search_path = format!("SET search_path TO {qs}");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(16)
            .after_connect(move |conn, _meta| {
                let stmt = search_path.clone();
                Box::pin(async move {
                    sqlx::query(sqlx::AssertSqlSafe(stmt.as_str()))
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .map_err(be)?;

        Ok(Self { pool })
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
        let owned: Vec<(Vec<u8>, Bytes)> = objects
            .iter()
            .map(|o| (o.id().as_bytes().to_vec(), o.as_bytes().clone()))
            .collect();
        let (ids, data): (Vec<Vec<u8>>, Vec<Vec<u8>>) = tokio::task::spawn_blocking(move || {
            owned
                .into_iter()
                .map(|(id, bytes)| Ok((id, zstd::encode_all(&bytes[..], ZSTD_LEVEL).map_err(be)?)))
                .collect::<Result<Vec<_>>>()
                .map(|v| v.into_iter().unzip())
        })
        .await
        .map_err(be)??;
        sqlx::query(
            "INSERT INTO objects (id, data) SELECT * FROM unnest($1::bytea[], $2::bytea[]) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(&ids)
        .bind(&data)
        .execute(&self.pool)
        .await
        .map_err(be)?;
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
        let present: HashSet<ObjectId> = rows
            .iter()
            .map(|r| object_id_col(r, "id"))
            .collect::<Result<_>>()?;
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
    // TASK (history): implement with a `history(ref, path, generation, commit)`
    // table (PK on all four; index on (ref, path text_pattern_ops, generation
    // DESC)) and a `history_watermark(ref PK, commit)` table. query_history
    // must be ONE query: `path = $p OR path LIKE $p || '/%'` (escape LIKE
    // metacharacters in $p; root prefix = no path filter), `generation <= $max`,
    // `SELECT DISTINCT generation, commit ... ORDER BY generation DESC LIMIT`.
    // Also override get_derived_many with a single `key = ANY($2)` query.
    async fn index_history(&self, ref_name: &str, rows: &[crate::HistoryRow]) -> Result<()> {
        let _ = (ref_name, rows);
        todo!("history")
    }
    async fn query_history(
        &self,
        ref_name: &str,
        prefix: &ant_core::RepoPath,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<(u64, ObjectId)>> {
        let _ = (ref_name, prefix, max_generation, limit);
        todo!("history")
    }
    async fn history_watermark(&self, ref_name: &str) -> Result<Option<ObjectId>> {
        let _ = ref_name;
        todo!("history")
    }
    async fn set_history_watermark(
        &self,
        ref_name: &str,
        expected: Option<ObjectId>,
        new: ObjectId,
    ) -> Result<bool> {
        let _ = (ref_name, expected, new);
        todo!("history")
    }
    async fn clear_history(&self, ref_name: &str) -> Result<()> {
        let _ = ref_name;
        todo!("history")
    }

    async fn put_commit(&self, node: CommitNode, changes: Vec<PathChange>) -> Result<u64> {
        let mut tx = self.pool.begin().await.map_err(be)?;
        let parents: Vec<u8> = node
            .parents
            .iter()
            .flat_map(|p| p.as_bytes().iter().copied())
            .collect();
        let inserted = sqlx::query(
            "INSERT INTO commits (id, tree, parents, generation, committer_time) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING",
        )
        .bind(node.id.as_bytes().as_slice())
        .bind(node.tree.as_bytes().as_slice())
        .bind(&parents)
        .bind(node.generation as i64)
        .bind(node.committer_time)
        .execute(&mut *tx)
        .await
        .map_err(be)?
        .rows_affected();

        if inserted == 0 {
            // Already recorded: return the original sequence number, change nothing.
            let seq: i64 = sqlx::query("SELECT seq FROM log WHERE commit_id = $1")
                .bind(node.id.as_bytes().as_slice())
                .fetch_one(&mut *tx)
                .await
                .map_err(be)?
                .try_get("seq")
                .map_err(be)?;
            tx.commit().await.map_err(be)?;
            return Ok(seq as u64);
        }

        let seq: i64 = sqlx::query("INSERT INTO log (commit_id) VALUES ($1) RETURNING seq")
            .bind(node.id.as_bytes().as_slice())
            .fetch_one(&mut *tx)
            .await
            .map_err(be)?
            .try_get("seq")
            .map_err(be)?;

        if !changes.is_empty() {
            let n = changes.len();
            let mut idx = Vec::with_capacity(n);
            let mut paths = Vec::with_capacity(n);
            let mut kinds = Vec::with_capacity(n);
            let mut old_modes: Vec<Option<String>> = Vec::with_capacity(n);
            let mut old_ids: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
            let mut new_modes: Vec<Option<String>> = Vec::with_capacity(n);
            let mut new_ids: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
            for (i, c) in changes.iter().enumerate() {
                idx.push(i as i32);
                paths.push(c.path.as_str().to_string());
                kinds.push(change_kind_code(c.kind));
                old_modes.push(c.old.map(|(m, _)| m.as_octal().to_string()));
                old_ids.push(c.old.map(|(_, id)| id.as_bytes().to_vec()));
                new_modes.push(c.new.map(|(m, _)| m.as_octal().to_string()));
                new_ids.push(c.new.map(|(_, id)| id.as_bytes().to_vec()));
            }
            sqlx::query(
                "INSERT INTO changes (commit_id, idx, path, kind, old_mode, old_id, new_mode, new_id) \
                 SELECT $1, t.idx, t.path, t.kind, t.old_mode, t.old_id, t.new_mode, t.new_id \
                 FROM unnest($2::integer[], $3::text[], $4::smallint[], $5::text[], $6::bytea[], \
                              $7::text[], $8::bytea[]) \
                      AS t(idx, path, kind, old_mode, old_id, new_mode, new_id)",
            )
            .bind(node.id.as_bytes().as_slice())
            .bind(&idx)
            .bind(&paths)
            .bind(&kinds)
            .bind(&old_modes)
            .bind(&old_ids)
            .bind(&new_modes)
            .bind(&new_ids)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        }
        tx.commit().await.map_err(be)?;
        Ok(seq as u64)
    }

    async fn put_commits(&self, commits: Vec<(CommitNode, Vec<PathChange>)>) -> Result<Vec<u64>> {
        if commits.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<Vec<u8>> = commits.iter().map(|(n, _)| n.id.as_bytes().to_vec()).collect();
        let trees: Vec<Vec<u8>> = commits.iter().map(|(n, _)| n.tree.as_bytes().to_vec()).collect();
        let parents: Vec<Vec<u8>> = commits
            .iter()
            .map(|(n, _)| n.parents.iter().flat_map(|p| p.as_bytes().iter().copied()).collect())
            .collect();
        let gens: Vec<i64> = commits.iter().map(|(n, _)| n.generation as i64).collect();
        let times: Vec<i64> = commits.iter().map(|(n, _)| n.committer_time).collect();

        let mut tx = self.pool.begin().await.map_err(be)?;
        // Rows that were actually inserted (not already recorded).
        let inserted: Vec<Vec<u8>> = sqlx::query_scalar(
            "INSERT INTO commits (id, tree, parents, generation, committer_time) \
             SELECT id, tree, parents, generation, committer_time \
             FROM unnest($1::bytea[], $2::bytea[], $3::bytea[], $4::bigint[], $5::bigint[]) \
                  WITH ORDINALITY AS t(id, tree, parents, generation, committer_time, ord) \
             ORDER BY ord \
             ON CONFLICT (id) DO NOTHING RETURNING id",
        )
        .bind(&ids)
        .bind(&trees)
        .bind(&parents)
        .bind(&gens)
        .bind(&times)
        .fetch_all(&mut *tx)
        .await
        .map_err(be)?;
        let inserted: HashSet<Vec<u8>> = inserted.into_iter().collect();

        if !inserted.is_empty() {
            // Input order = sequence order (parents first).
            let mut seen = HashSet::with_capacity(inserted.len());
            let new_ids: Vec<&Vec<u8>> =
                ids.iter().filter(|i| inserted.contains(*i) && seen.insert(*i)).collect();
            sqlx::query(
                "INSERT INTO log (commit_id) \
                 SELECT id FROM unnest($1::bytea[]) WITH ORDINALITY AS t(id, ord) ORDER BY ord",
            )
            .bind(&new_ids)
            .execute(&mut *tx)
            .await
            .map_err(be)?;

            let mut c_ids: Vec<&[u8]> = Vec::new();
            let mut idx = Vec::new();
            let mut paths = Vec::new();
            let mut kinds = Vec::new();
            let mut old_modes: Vec<Option<String>> = Vec::new();
            let mut old_ids: Vec<Option<Vec<u8>>> = Vec::new();
            let mut new_modes: Vec<Option<String>> = Vec::new();
            let mut new_ids: Vec<Option<Vec<u8>>> = Vec::new();
            let mut done = HashSet::new();
            for (raw, (_, changes)) in ids.iter().zip(&commits) {
                if !inserted.contains(raw) || !done.insert(raw) {
                    continue;
                }
                for (i, c) in changes.iter().enumerate() {
                    c_ids.push(raw);
                    idx.push(i as i32);
                    paths.push(c.path.as_str().to_string());
                    kinds.push(change_kind_code(c.kind));
                    old_modes.push(c.old.map(|(m, _)| m.as_octal().to_string()));
                    old_ids.push(c.old.map(|(_, id)| id.as_bytes().to_vec()));
                    new_modes.push(c.new.map(|(m, _)| m.as_octal().to_string()));
                    new_ids.push(c.new.map(|(_, id)| id.as_bytes().to_vec()));
                }
            }
            if !c_ids.is_empty() {
                sqlx::query(
                    "INSERT INTO changes (commit_id, idx, path, kind, old_mode, old_id, new_mode, new_id) \
                     SELECT * FROM unnest($1::bytea[], $2::integer[], $3::text[], $4::smallint[], \
                                          $5::text[], $6::bytea[], $7::text[], $8::bytea[])",
                )
                .bind(&c_ids)
                .bind(&idx)
                .bind(&paths)
                .bind(&kinds)
                .bind(&old_modes)
                .bind(&old_ids)
                .bind(&new_modes)
                .bind(&new_ids)
                .execute(&mut *tx)
                .await
                .map_err(be)?;
            }
        }

        let rows = sqlx::query("SELECT commit_id, seq FROM log WHERE commit_id = ANY($1)")
            .bind(&ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(be)?;
        tx.commit().await.map_err(be)?;
        let mut seqs: HashMap<Vec<u8>, i64> = HashMap::with_capacity(rows.len());
        for r in &rows {
            seqs.insert(r.try_get("commit_id").map_err(be)?, r.try_get("seq").map_err(be)?);
        }
        ids.iter()
            .map(|i| {
                seqs.get(i)
                    .map(|s| *s as u64)
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
        let exists = sqlx::query("SELECT 1 FROM commits WHERE id = $1")
            .bind(id.as_bytes().as_slice())
            .fetch_optional(&self.pool)
            .await
            .map_err(be)?;
        if exists.is_none() {
            return Ok(None);
        }
        let rows = sqlx::query(
            "SELECT path, kind, old_mode, old_id, new_mode, new_id FROM changes \
             WHERE commit_id = $1 ORDER BY idx",
        )
        .bind(id.as_bytes().as_slice())
        .fetch_all(&self.pool)
        .await
        .map_err(be)?;
        let side =
            |mode: Option<String>, oid: Option<Vec<u8>>| -> Result<Option<(EntryMode, ObjectId)>> {
                match (mode, oid) {
                    (Some(m), Some(o)) => Ok(Some((
                        EntryMode::from_octal(m.as_bytes())?,
                        ObjectId::from_bytes(&o)?,
                    ))),
                    (None, None) => Ok(None),
                    _ => Err(StoreError::InvalidArgument("corrupt changeset row".into())),
                }
            };
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let path: String = r.try_get("path").map_err(be)?;
            out.push(PathChange {
                path: RepoPath::new(path)?,
                kind: change_kind_from_code(r.try_get("kind").map_err(be)?)?,
                old: side(
                    r.try_get("old_mode").map_err(be)?,
                    r.try_get("old_id").map_err(be)?,
                )?,
                new: side(
                    r.try_get("new_mode").map_err(be)?,
                    r.try_get("new_id").map_err(be)?,
                )?,
            });
        }
        Ok(Some(out))
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
            Some(r) => Ok(Some(Bytes::from(
                r.try_get::<Vec<u8>, _>("value").map_err(be)?,
            ))),
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
        let (keys, values): (Vec<Vec<u8>>, Vec<Vec<u8>>) =
            entries.into_iter().map(|(k, v)| (k, v.to_vec())).unzip();
        sqlx::query(
            "INSERT INTO derived (kind, key, value) \
             SELECT $1, k, v FROM unnest($2::bytea[], $3::bytea[]) AS t(k, v) \
             ON CONFLICT (kind, key) DO NOTHING",
        )
        .bind(kind)
        .bind(&keys)
        .bind(&values)
        .execute(&self.pool)
        .await
        .map_err(be)?;
        Ok(())
    }

    async fn get_derived_many(&self, kind: &str, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query("SELECT key, value FROM derived WHERE kind = $1 AND key = ANY($2)")
            .bind(kind)
            .bind(keys.iter().map(|k| k.to_vec()).collect::<Vec<_>>())
            .fetch_all(&self.pool)
            .await
            .map_err(be)?;
        let mut found: HashMap<Vec<u8>, Bytes> = HashMap::with_capacity(rows.len());
        for r in &rows {
            let value: Vec<u8> = r.try_get("value").map_err(be)?;
            found.insert(r.try_get("key").map_err(be)?, Bytes::from(value));
        }
        Ok(keys.iter().map(|k| found.get(*k).cloned()).collect())
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
        PgStore::connect(&url, &schema)
            .await
            .expect("connect (is `docker compose up -d` running?)")
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

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_connect_is_idempotent() {
        let url = std::env::var("ANT_TEST_PG_URL")
            .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into());
        let schema = format!("t_{}", rand::random::<u64>());
        let a = PgStore::connect(&url, &schema).await.unwrap();
        a.cas_ref("main", None, Some(ObjectId([1; 32])))
            .await
            .unwrap();
        let b = PgStore::connect(&url, &schema).await.unwrap();
        assert_eq!(b.get_ref("main").await.unwrap(), Some(ObjectId([1; 32])));
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
