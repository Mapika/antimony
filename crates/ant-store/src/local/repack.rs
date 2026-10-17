//! Repacking a local data directory (`ant admin repack --local`): the
//! engine of the Postgres + S3 repack (`crate::repack`; the run is described
//! in `crate::postgres::repack`) on SQLite rows and local pack files.
//!
//! Large objects (packed-object index rows) and cold small objects (SQLite
//! `objects` rows that the walk reaches outside the hot set) written before
//! the cutoff are rewritten into new pack files: delta frames across
//! history for cold objects, plain frames for hot ones. Each new pack is
//! written and fsynced in full, then its rows are switched to it in
//! durable transactions on the writer thread (`batch` ids each): an index
//! row only while it still points where the object was read from, a small
//! object only by deleting its `objects` row in the same transaction; the
//! write time is kept. Packs are recorded in the `packs` table (with a
//! `hot` flag), so a later run skips their objects (unless `all`) and an
//! interrupted run resumes where it stopped.
//!
//! Once every pack is switched, the old packs no index row points into any
//! more are deleted (readers that looked up an old location retry, see
//! `super::packs`), and freed SQLite pages are returned to the file system
//! (`super::gc`'s vacuuming).
//!
//! # Safety
//!
//! A repack holds the store's maintenance slot, so it never runs alongside
//! a collection (`super::gc`): nothing is condemned while it runs, and the
//! only other writers are the store's ordinary ones. Those never move a
//! row (storing an existing object refreshes its write time, keeping the
//! location), so a switch that finds the row where it was read from moves
//! the bytes of exactly that object. A new pack is pinned busy until its
//! rows are switched, and pack deletions first wait for every write queued
//! before them, so no pack a switch is about to point rows at is ever
//! deleted. Readers see every object at all times: before a switch commits
//! at the old location, after it at the new one (the small-object tier is
//! read before the packed one, and a small object moves in one
//! transaction). A crash leaves at worst a new pack no row points into,
//! deleted by the next run or collection.
//!
//! Memory: about 100 bytes per object in the repository (the walk), plus
//! per job a fetch batch, a delta window and a record, plus up to two packs
//! of [`RepackOptions::pack_bytes`] being written.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ant_core::ObjectId;
use async_trait::async_trait;

use super::{LocalStore, Sq, id_of, now_secs, to_i64};
use crate::packed::cluster::ClusterOptions;
use crate::repack::{Entry, IndexRow, Settings, SmallRow, Source, Switched, Target};
use crate::{CommitNode, MetaStore, RefStore, Result, StoreError};

pub use crate::repack::{DEFAULT_KEEP_RECENT, DEFAULT_MIN_AGE, RepackReport};

/// Settings of one local repack.
#[derive(Debug, Clone)]
pub struct RepackOptions {
    /// Read, compress and count only; write nothing.
    pub dry_run: bool,
    /// Rows written less than this long ago are left as they are.
    pub min_age: Duration,
    /// First-parent commits per ref whose trees stay one decode away (hot).
    pub keep_recent: usize,
    /// Also rewrite objects already in packs a repack wrote (a full repack).
    pub all: bool,
    /// Move cold small objects (SQLite rows) into packs too.
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
    /// Nothing else uses the store (offline): end with a full `VACUUM`
    /// when worth it (see `super::gc`).
    pub exclusive: bool,
}

impl Default for RepackOptions {
    fn default() -> Self {
        Self {
            dry_run: false,
            min_age: DEFAULT_MIN_AGE,
            keep_recent: DEFAULT_KEEP_RECENT,
            all: false,
            small: true,
            cluster: ClusterOptions::default(),
            jobs: std::thread::available_parallelism().map_or(4, |n| n.get()).min(8),
            batch: 200,
            stop_after_packs: None,
            pack_bytes: crate::repack::PACK_BYTES,
            exclusive: false,
        }
    }
}

impl RepackOptions {
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

/// Repack `store` (see the module docs). Safe while it serves; waits while
/// another maintenance run (collection or repack) holds the store.
pub async fn repack(
    store: &LocalStore,
    opts: &RepackOptions,
    progress: &(dyn Fn(&str) + Send + Sync),
) -> Result<RepackReport> {
    super::db::maintenance_reads(repack_run(store, opts, progress)).await
}

async fn repack_run(
    store: &LocalStore,
    opts: &RepackOptions,
    progress: &(dyn Fn(&str) + Send + Sync),
) -> Result<RepackReport> {
    let _slot = match store.try_maintenance() {
        Some(slot) => slot,
        None => {
            progress("waiting for the running collection or repack to finish");
            store.maintenance().await
        }
    };
    let objects = store.backend().objects;
    let target = LocalTarget(store.clone());
    let mut report =
        crate::repack::run(&target, objects.as_ref(), opts.settings(), progress).await?;
    if !opts.dry_run {
        let (n, bytes) = super::gc::delete_unreferenced(store).await?;
        report.packs_deleted = n;
        report.packs_deleted_bytes = bytes;
        progress(&format!("deleted {n} old packs ({bytes} bytes)"));
        super::gc::reclaim(store, opts.exclusive).await?;
    }
    Ok(report)
}

/// SQLite rows, local pack files.
#[derive(Clone)]
struct LocalTarget(LocalStore);

#[async_trait]
impl Target for LocalTarget {
    async fn list_refs(&self) -> Result<Vec<(String, ObjectId)>> {
        RefStore::list_refs(&self.0, "").await
    }

    async fn get_commits(&self, ids: &[ObjectId]) -> Result<Vec<Option<CommitNode>>> {
        MetaStore::get_commits(&self.0, ids).await
    }

    /// Write times have whole seconds: a row is old when written at or
    /// before the cutoff (`min_age` 0: everything written so far).
    async fn cutoff(&self, min_age: Duration) -> Result<f64> {
        Ok(now_secs() as f64 - min_age.as_secs_f64())
    }

    async fn scan_index(&self, after: &[u8], cutoff: f64, limit: usize) -> Result<Vec<IndexRow>> {
        let after = after.to_vec();
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.0
            .db()
            .read(move |c| {
                let mut st = c
                    .prepare_cached(
                        "SELECT x.id, x.pack, x.off, coalesce(x.share, x.len), x.written_at, \
                                p.hot \
                         FROM object_index x LEFT JOIN packs p ON p.pack = x.pack \
                         WHERE x.id > ?1 ORDER BY x.id LIMIT ?2",
                    )
                    .sq()?;
                let mut rows = st.query(rusqlite::params![after, limit]).sq()?;
                let mut out = Vec::new();
                while let Some(r) = rows.next().sq()? {
                    let hot: Option<i64> = r.get(5).sq()?;
                    out.push(IndexRow {
                        id: id_of(&r.get::<_, Vec<u8>>(0).sq()?)?,
                        pack: super::gc::pack_id(&r.get::<_, Vec<u8>>(1).sq()?)?,
                        off: r.get::<_, i64>(2).sq()? as u64,
                        bytes: r.get::<_, i64>(3).sq()? as u64,
                        old: (r.get::<_, i64>(4).sq()? as f64) <= cutoff,
                        condemned: false,
                        repacked: match hot {
                            None => 0,
                            Some(0) => 1,
                            Some(_) => 2,
                        },
                    });
                }
                Ok(out)
            })
            .await
    }

    async fn scan_small(&self, after: &[u8], cutoff: f64, limit: usize) -> Result<Vec<SmallRow>> {
        let after = after.to_vec();
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.0
            .db()
            .read(move |c| {
                let mut st = c
                    .prepare_cached(
                        "SELECT id, length(data), written_at FROM objects \
                         WHERE id > ?1 ORDER BY id LIMIT ?2",
                    )
                    .sq()?;
                let mut rows = st.query(rusqlite::params![after, limit]).sq()?;
                let mut out = Vec::new();
                while let Some(r) = rows.next().sq()? {
                    out.push(SmallRow {
                        id: id_of(&r.get::<_, Vec<u8>>(0).sq()?)?,
                        bytes: r.get::<_, i64>(1).sq()? as u64,
                        old: (r.get::<_, i64>(2).sq()? as f64) <= cutoff,
                        condemned: false,
                    });
                }
                Ok(out)
            })
            .await
    }

    async fn store_pack(
        &self,
        hot: bool,
        body: Vec<u8>,
        entries: Vec<Entry>,
        sources: Arc<HashMap<ObjectId, Source>>,
        batch: usize,
    ) -> Result<Switched> {
        let f = self.0.packs().clone();
        let ((pack, pin), base) = tokio::task::spawn_blocking(move || {
            let mut w = f.writer()?;
            let base = w.append(&body)?;
            Ok::<_, StoreError>((w.finish()?, base))
        })
        .await
        .map_err(StoreError::backend)??;
        let mut sorted: Vec<Entry> = entries;
        sorted.sort_unstable_by_key(|e| e.0);
        sorted.dedup_by_key(|e| e.0);
        let mut out = Switched::default();
        let mut first = true;
        for chunk in sorted.chunks(batch.max(1)) {
            let mut moved: Vec<(Entry, [u8; 16], u64)> = Vec::new();
            let mut small: Vec<Entry> = Vec::new();
            for e in chunk {
                match sources.get(&e.0) {
                    Some(Source::Index { pack, off }) => moved.push((*e, *pack, *off)),
                    Some(Source::Small) => small.push(*e),
                    None => out.skipped += 1,
                }
            }
            let record = std::mem::take(&mut first);
            let (want_moved, want_small) = (moved.len() as u64, small.len() as u64);
            let (n_moved, n_small) = self
                .0
                .db()
                .write(true, move |c, _| {
                    if record {
                        c.prepare_cached(
                            "INSERT INTO packs (pack, repacked_at, hot) VALUES (?1, ?2, ?3) \
                             ON CONFLICT (pack) DO UPDATE \
                             SET repacked_at = excluded.repacked_at, hot = excluded.hot",
                        )
                        .sq()?
                        .execute(rusqlite::params![pack.as_slice(), now_secs(), i64::from(hot)])
                        .sq()?;
                    }
                    let mut up = c
                        .prepare_cached(
                            "UPDATE object_index SET pack = ?2, off = ?3, len = ?4, share = ?5 \
                             WHERE id = ?1 AND pack = ?6 AND off = ?7",
                        )
                        .sq()?;
                    let mut n_moved = 0u64;
                    for ((id, off, len, share), old_pack, old_off) in &moved {
                        n_moved += up
                            .execute(rusqlite::params![
                                id.as_bytes().as_slice(),
                                pack.as_slice(),
                                to_i64(base + off, "pack offset")?,
                                i64::from(*len),
                                share.map(i64::from),
                                old_pack.as_slice(),
                                to_i64(*old_off, "pack offset")?
                            ])
                            .sq()? as u64;
                    }
                    let mut del = c
                        .prepare_cached("DELETE FROM objects WHERE id = ?1 RETURNING written_at")
                        .sq()?;
                    let mut ins = c
                        .prepare_cached(
                            "INSERT INTO object_index (id, pack, off, len, share, written_at) \
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                             ON CONFLICT (id) DO UPDATE \
                             SET written_at = max(object_index.written_at, excluded.written_at)",
                        )
                        .sq()?;
                    let mut n_small = 0u64;
                    for (id, off, len, share) in &small {
                        let at: Option<i64> = rusqlite::OptionalExtension::optional(
                            del.query_row([id.as_bytes().as_slice()], |r| r.get(0)),
                        )
                        .sq()?;
                        let Some(at) = at else { continue };
                        ins.execute(rusqlite::params![
                            id.as_bytes().as_slice(),
                            pack.as_slice(),
                            to_i64(base + off, "pack offset")?,
                            i64::from(*len),
                            share.map(i64::from),
                            at
                        ])
                        .sq()?;
                        n_small += 1;
                    }
                    Ok((n_moved, n_small))
                })
                .await?;
            out.moved += n_moved + n_small;
            out.moved_small += n_small;
            out.skipped += (want_moved - n_moved) + (want_small - n_small);
        }
        // The rows are written: the pack may be deleted like any other once
        // none points into it.
        drop(pin);
        Ok(out)
    }
}
