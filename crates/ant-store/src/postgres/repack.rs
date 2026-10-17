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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ant_core::ObjectId;
use async_trait::async_trait;
use sqlx::Row;

use super::{PgStore, be, object_id_col};
use crate::packed::cluster::ClusterOptions;
use crate::repack::{Entry, IndexRow, Settings, SmallRow, Source, Switched, Target};
use crate::s3::S3ObjectStore;
use crate::{CommitNode, MetaStore, ObjectStore, RefStore, Result, StoreError};

pub use crate::repack::{DEFAULT_KEEP_RECENT, DEFAULT_MIN_AGE, RepackReport};

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
            pack_bytes: crate::repack::PACK_BYTES,
            s3,
        }
    }

    fn settings(&self) -> Settings {
        Settings {
            dry_run: self.dry_run,
            min_age: self.min_age,
            keep_recent: self.keep_recent,
            all: self.all,
            small: self.small,
            cluster: self.cluster,
            jobs: self.jobs,
            batch: self.batch,
            stop_after_packs: self.stop_after_packs,
            pack_bytes: self.pack_bytes,
        }
    }
}

/// Advisory-lock key held by the one repack of a schema.
fn run_key(schema: &str) -> String {
    format!("ant-repack/{schema}")
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
        let target = PgTarget { store: self.clone(), s3: opts.s3.clone() };
        let result = crate::repack::run(&target, objects, opts.settings(), progress).await;
        sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
            .bind(run_key(&self.schema))
            .execute(&mut *conn)
            .await
            .map_err(be)?;
        result
    }
}

/// Postgres rows, S3 packs.
#[derive(Clone)]
struct PgTarget {
    store: PgStore,
    s3: S3ObjectStore,
}

#[async_trait]
impl Target for PgTarget {
    async fn list_refs(&self) -> Result<Vec<(String, ObjectId)>> {
        self.store.list_refs("").await
    }

    async fn get_commits(&self, ids: &[ObjectId]) -> Result<Vec<Option<CommitNode>>> {
        self.store.get_commits(ids).await
    }

    async fn cutoff(&self, min_age: Duration) -> Result<f64> {
        sqlx::query_scalar("SELECT extract(epoch FROM now())::float8 - $1")
            .bind(min_age.as_secs_f64())
            .fetch_one(&self.store.pool)
            .await
            .map_err(be)
    }

    async fn scan_index(&self, after: &[u8], cutoff: f64, limit: usize) -> Result<Vec<IndexRow>> {
        let rows = sqlx::query(
            "SELECT x.id, x.pack, x.off, coalesce(x.share, x.len)::bigint AS bytes, \
                    x.written_at < to_timestamp($2) AS old, \
                    EXISTS (SELECT 1 FROM gc_condemned g WHERE g.id = x.id) AS condemned, \
                    coalesce((SELECT CASE WHEN p.hot THEN 2 ELSE 1 END FROM packs p \
                              WHERE p.pack = x.pack AND p.repacked_at IS NOT NULL), 0) \
                      AS repacked \
             FROM object_index x WHERE x.id > $1 ORDER BY x.id LIMIT $3",
        )
        .bind(after)
        .bind(cutoff)
        .bind(limit as i64)
        .fetch_all(&self.store.pool)
        .await
        .map_err(be)?;
        rows.iter()
            .map(|r| {
                let pack: Vec<u8> = r.try_get("pack").map_err(be)?;
                Ok(IndexRow {
                    id: object_id_col(r, "id")?,
                    pack: pack
                        .as_slice()
                        .try_into()
                        .map_err(|_| StoreError::InvalidArgument("corrupt pack id".into()))?,
                    off: r.try_get::<i64, _>("off").map_err(be)? as u64,
                    bytes: r.try_get::<i64, _>("bytes").map_err(be)? as u64,
                    old: r.try_get("old").map_err(be)?,
                    condemned: r.try_get("condemned").map_err(be)?,
                    repacked: r.try_get::<i32, _>("repacked").map_err(be)? as u8,
                })
            })
            .collect()
    }

    async fn scan_small(&self, after: &[u8], cutoff: f64, limit: usize) -> Result<Vec<SmallRow>> {
        let rows = sqlx::query(
            "SELECT o.id, octet_length(o.data)::bigint AS bytes, \
                    o.written_at < to_timestamp($2) AS old, \
                    EXISTS (SELECT 1 FROM gc_condemned g WHERE g.id = o.id) AS condemned \
             FROM objects o WHERE o.id > $1 ORDER BY o.id LIMIT $3",
        )
        .bind(after)
        .bind(cutoff)
        .bind(limit as i64)
        .fetch_all(&self.store.pool)
        .await
        .map_err(be)?;
        rows.iter()
            .map(|r| {
                Ok(SmallRow {
                    id: object_id_col(r, "id")?,
                    bytes: r.try_get::<i64, _>("bytes").map_err(be)? as u64,
                    old: r.try_get("old").map_err(be)?,
                    condemned: r.try_get("condemned").map_err(be)?,
                })
            })
            .collect()
    }

    async fn store_pack(
        &self,
        hot: bool,
        body: Vec<u8>,
        entries: Vec<Entry>,
        sources: Arc<HashMap<ObjectId, Source>>,
        batch: usize,
    ) -> Result<Switched> {
        let pack: [u8; 16] = rand::random();
        self.s3.put_raw(self.s3.pack_key(&pack), body).await?;
        self.store.switch(pack, hot, &entries, &sources, batch).await
    }
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
    use ant_core::{EncodedObject, ObjectKind};

    use super::*;
    use crate::packed::PackedS3Store;
    use crate::repack::SEGMENT_OBJECTS;
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
}
