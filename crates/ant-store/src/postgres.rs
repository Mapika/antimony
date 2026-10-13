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

use ant_core::{EncodedObject, ObjectId};
use async_trait::async_trait;
use bytes::Bytes;

use crate::{CasOutcome, CommitNode, MetaStore, ObjectStore, PathChange, RefStore, Result};

#[derive(Clone)]
pub struct PgStore {
    #[allow(dead_code)]
    pool: sqlx::PgPool,
}

impl PgStore {
    /// Connect and ensure `schema` and all tables exist.
    pub async fn connect(url: &str, schema: &str) -> Result<Self> {
        let _ = (url, schema);
        todo!("store-pg")
    }
}

#[async_trait]
impl ObjectStore for PgStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        let _ = ids;
        todo!("store-pg")
    }
    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        let _ = objects;
        todo!("store-pg")
    }
    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        let _ = ids;
        todo!("store-pg")
    }
}

#[async_trait]
impl RefStore for PgStore {
    async fn get_ref(&self, name: &str) -> Result<Option<ObjectId>> {
        let _ = name;
        todo!("store-pg")
    }
    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ObjectId>,
        new: Option<ObjectId>,
    ) -> Result<CasOutcome> {
        let _ = (name, expected, new);
        todo!("store-pg")
    }
    async fn list_refs(&self, prefix: &str) -> Result<Vec<(String, ObjectId)>> {
        let _ = prefix;
        todo!("store-pg")
    }
}

#[async_trait]
impl MetaStore for PgStore {
    async fn put_commit(&self, node: CommitNode, changes: Vec<PathChange>) -> Result<u64> {
        let _ = (node, changes);
        todo!("store-pg")
    }
    async fn get_commits(&self, ids: &[ObjectId]) -> Result<Vec<Option<CommitNode>>> {
        let _ = ids;
        todo!("store-pg")
    }
    async fn get_changeset(&self, id: &ObjectId) -> Result<Option<Vec<PathChange>>> {
        let _ = id;
        todo!("store-pg")
    }
    async fn tail(&self, after: u64, limit: usize) -> Result<Vec<(u64, ObjectId)>> {
        let _ = (after, limit);
        todo!("store-pg")
    }
    async fn get_derived(&self, kind: &str, key: &[u8]) -> Result<Option<Bytes>> {
        let _ = (kind, key);
        todo!("store-pg")
    }
    async fn put_derived(&self, kind: &str, key: &[u8], value: Bytes) -> Result<()> {
        let _ = (kind, key, value);
        todo!("store-pg")
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

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn pg_meta_store_conforms() {
        conformance::meta_store(Arc::new(fresh().await)).await;
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
}
