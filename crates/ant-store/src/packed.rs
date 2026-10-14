//! Large objects bundled into few S3 objects.
//!
//! One S3 `PUT` per object is what limits bulk loads: an S3-compatible server
//! spends far more on a request than on a few kilobytes of body (measured on a
//! local SeaweedFS: 12 KB objects at under 1k PUT/s, 8 MB objects at 1 GB/s).
//! [`PackedS3Store`] therefore writes each `put_many` batch as a handful of
//! *packs* (concatenated, individually zstd-compressed objects) and records
//! `id -> (pack, offset, length)` in the Postgres `object_index` table. A read
//! is one index lookup plus one ranged `GET` per object.
//!
//! - Order of writes: packs first, index rows after. An index row never points
//!   at a missing pack; a crash in between leaves an unreferenced pack.
//! - Pack ids are random (earlier versions hashed the content), so a pack is
//!   only ever indexed by the upload that wrote it: garbage collection can
//!   delete an unindexed old pack without racing a writer re-uploading the
//!   same bytes under the same key (`postgres::gc`). A retried upload writes
//!   a new pack; the one it replaces becomes garbage.
//! - Objects that are not in the index are looked up under their own S3 key
//!   (`S3ObjectStore`'s layout), so stores written before packs existed stay
//!   readable; ids condemned by a collection run are not.
//! - An object stored twice (concurrent writers) occupies space in two packs;
//!   the index keeps the first. Packs are never rewritten here.

use std::{collections::HashSet, sync::Arc};

use ant_core::{EncodedObject, ObjectId};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};
use tokio::sync::Semaphore;

use crate::postgres::{IndexEntry, PgStore};
use crate::s3::S3ObjectStore;
use crate::{ObjectStore, Result, StoreError};

/// Target size of one pack, before compression.
const PACK_BYTES: usize = 24 * 1024 * 1024;
/// Uncompressed bytes hashed and compressed per blocking task.
const COMPRESS_CHUNK_BYTES: usize = 512 * 1024;
/// Concurrent S3 requests per call.
const FANOUT: usize = 32;
const ZSTD_LEVEL: i32 = 3;
/// Bodies larger than this are decompressed on the blocking pool.
const BLOCKING_THRESHOLD: usize = 64 * 1024;
/// Objects of one pack at most this far apart are read with one ranged GET
/// (the gap is downloaded and dropped)...
const COALESCE_GAP: u64 = 256 * 1024;
/// ...as long as the whole range stays below this many bytes.
const COALESCE_SPAN: u64 = 2 * 1024 * 1024;

/// Bytes of ranged reads in flight per store, across all callers. Bounds
/// memory, and S3 servers throttle beyond some in-flight volume (SeaweedFS
/// answers 429 above 64 MiB per volume server, counting whole storage chunks
/// rather than requested ranges, and the SDK then backs off for seconds).
/// Every read counts at least `READ_MIN_COST`, which caps concurrent requests.
const READ_BUDGET: u64 = 48 * 1024 * 1024;
const READ_MIN_COST: u64 = 1024 * 1024;
const READ_UNIT: u64 = 4096;
const READ_PERMITS: usize = (READ_BUDGET / READ_UNIT) as usize;

/// One ranged GET: `entries` (sorted by offset, all in `pack`) lie within
/// `start..end`.
#[derive(Debug)]
struct Run {
    pack: [u8; 16],
    start: u64,
    end: u64,
    entries: Vec<IndexEntry>,
}

/// Group packed objects into few ranged reads. Request overhead, not bytes,
/// bounds S3 reads of small objects, and objects written together (one
/// import or land batch) are read together (a tree walk, a pack export).
fn coalesce(mut entries: Vec<IndexEntry>) -> Vec<Run> {
    entries.sort_by_key(|e| (e.pack, e.off));
    let mut runs: Vec<Run> = Vec::new();
    for e in entries {
        let end = e.off + u64::from(e.len);
        if let Some(run) = runs.last_mut().filter(|run| {
            run.pack == e.pack
                && e.off <= run.end.saturating_add(COALESCE_GAP)
                && end.max(run.end) - run.start <= COALESCE_SPAN
        }) {
            run.end = run.end.max(end);
            run.entries.push(e);
        } else {
            runs.push(Run { pack: e.pack, start: e.off, end, entries: vec![e] });
        }
    }
    runs
}

#[derive(Clone)]
pub struct PackedS3Store {
    index: PgStore,
    s3: S3ObjectStore,
    /// Range bytes being downloaded, in `READ_UNIT`s (see `READ_BUDGET`).
    reading: Arc<Semaphore>,
}

impl PackedS3Store {
    /// `index` holds the `object_index` table; `s3` receives the packs and is
    /// also consulted for objects stored one per key.
    pub fn new(index: PgStore, s3: S3ObjectStore) -> Self {
        Self { index, s3, reading: Arc::new(Semaphore::new(READ_PERMITS)) }
    }

    /// Read one run with a single ranged GET and decode its objects.
    async fn get_run(&self, run: Run) -> Result<Vec<(ObjectId, EncodedObject)>> {
        let len = run.end - run.start;
        let units = len.max(READ_MIN_COST).div_ceil(READ_UNIT).clamp(1, READ_PERMITS as u64) as u32;
        let permit = self.reading.acquire_many(units).await.map_err(StoreError::backend)?;
        let body = self.s3.get_range(&self.s3.pack_key(&run.pack), run.start, len).await?;
        drop(permit);
        let decode = move || -> Result<Vec<(ObjectId, EncodedObject)>> {
            run.entries
                .iter()
                .map(|e| {
                    let at = (e.off - run.start) as usize;
                    let object =
                        S3ObjectStore::decode_object(e.id, &body[at..at + e.len as usize])?;
                    Ok((e.id, object))
                })
                .collect()
        };
        if (run.end - run.start) as usize > BLOCKING_THRESHOLD {
            tokio::task::spawn_blocking(decode).await.map_err(StoreError::backend)?
        } else {
            decode()
        }
    }
}

/// One pack under construction.
struct Pack {
    body: Vec<u8>,
    entries: Vec<(ObjectId, u64, u32)>,
}

#[async_trait]
impl ObjectStore for PackedS3Store {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let index = self.index.lookup_index(ids).await?;
        let mut wanted: HashSet<ObjectId> = HashSet::new();
        let found: Vec<IndexEntry> = ids
            .iter()
            .filter(|id| wanted.insert(**id))
            .filter_map(|id| index.get(id).copied())
            .collect();
        let fetched: Vec<Vec<(ObjectId, EncodedObject)>> = stream::iter(coalesce(found))
            .map(|run| self.get_run(run))
            .buffer_unordered(FANOUT)
            .try_collect()
            .await?;
        let mut got: std::collections::HashMap<ObjectId, EncodedObject> =
            fetched.into_iter().flatten().collect();
        // Objects stored one per key (before packs existed).
        let mut legacy_ids: Vec<ObjectId> =
            wanted.iter().filter(|id| !got.contains_key(id)).copied().collect();
        if !legacy_ids.is_empty() {
            let condemned = self.index.condemned_among(&legacy_ids).await?;
            legacy_ids.retain(|id| !condemned.contains(id));
        }
        if !legacy_ids.is_empty() {
            for (id, obj) in legacy_ids.iter().zip(self.s3.get_many(&legacy_ids).await?) {
                if let Some(obj) = obj {
                    got.insert(*id, obj);
                }
            }
        }
        Ok(ids.iter().map(|id| got.get(id).cloned()).collect())
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }
        // Hash and compress in parallel on the blocking pool.
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
            tokio::task::spawn_blocking(move || -> Result<Vec<(ObjectId, Vec<u8>)>> {
                chunk
                    .into_iter()
                    .map(|o| {
                        let z = zstd::stream::encode_all(&o.as_bytes()[..], ZSTD_LEVEL)
                            .map_err(StoreError::backend)?;
                        Ok((o.id(), z))
                    })
                    .collect()
            })
        });
        let mut seen: HashSet<ObjectId> = HashSet::with_capacity(objects.len());
        let mut packs: Vec<Pack> = vec![Pack { body: Vec::new(), entries: Vec::new() }];
        for part in futures::future::try_join_all(tasks).await.map_err(StoreError::backend)? {
            for (id, z) in part? {
                if !seen.insert(id) {
                    continue;
                }
                let last = packs.last_mut().expect("at least one pack");
                if last.body.len() + z.len() > PACK_BYTES && !last.body.is_empty() {
                    packs.push(Pack { body: Vec::new(), entries: Vec::new() });
                }
                let cur = packs.last_mut().expect("at least one pack");
                cur.entries.push((id, cur.body.len() as u64, z.len() as u32));
                cur.body.extend_from_slice(&z);
            }
        }
        packs.retain(|p| !p.body.is_empty());

        // Packs first, then the index rows that point into them.
        let mut entries: Vec<IndexEntry> = Vec::new();
        let uploads: Vec<(String, Vec<u8>)> = packs
            .into_iter()
            .map(|p| {
                let pack: [u8; 16] = rand::random();
                entries.extend(p.entries.into_iter().map(|(id, off, len)| IndexEntry {
                    id,
                    pack,
                    off,
                    len,
                }));
                (self.s3.pack_key(&pack), p.body)
            })
            .collect();
        let puts =
            uploads.into_iter().map(|(key, body)| async move { self.s3.put_raw(key, body).await });
        stream::iter(puts).buffer_unordered(FANOUT).try_collect::<Vec<()>>().await?;
        self.index.index_objects(&entries).await
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let index = self.index.lookup_index(ids).await?;
        let mut out: Vec<bool> = ids.iter().map(|id| index.contains_key(id)).collect();
        let misses: Vec<usize> =
            out.iter().enumerate().filter(|(_, p)| !**p).map(|(i, _)| i).collect();
        if !misses.is_empty() {
            let miss_ids: Vec<ObjectId> = misses.iter().map(|&i| ids[i]).collect();
            let condemned = self.index.condemned_among(&miss_ids).await?;
            for (i, present) in misses.into_iter().zip(self.s3.has_many(&miss_ids).await?) {
                out[i] = present && !condemned.contains(&ids[i]);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::s3::S3Config;
    use crate::{ObjectStoreExt, conformance};
    use ant_core::ObjectKind;

    async fn fresh() -> (PackedS3Store, S3ObjectStore) {
        let url = std::env::var("ANT_TEST_PG_URL")
            .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into());
        let pg =
            PgStore::connect_scratch(&url, &format!("t_{}", rand::random::<u64>())).await.unwrap();
        let cfg = S3Config::for_tests(&format!("p{}", rand::random::<u64>()));
        let s3 = S3ObjectStore::connect(&cfg).await.unwrap();
        s3.ensure_bucket().await.unwrap();
        (PackedS3Store::new(pg, s3.clone()), s3)
    }

    #[test]
    fn nearby_objects_of_one_pack_share_a_range_read() {
        let entry = |pack: u8, off: u64, len: u32| IndexEntry {
            id: ObjectId::from_bytes(&[pack.wrapping_add(off as u8); 32]).unwrap(),
            pack: [pack; 16],
            off,
            len,
        };
        let far = COALESCE_GAP + 1;
        let mut entries = vec![
            entry(1, 5000, 10),
            entry(2, 0, 100),
            entry(1, 0, 100),
            entry(1, 100 + COALESCE_GAP, 50),
            entry(1, 150 + COALESCE_GAP + far, 7),
        ];
        // Pack 3: a chain of near neighbours, longer than one span.
        let steps = COALESCE_SPAN / COALESCE_GAP;
        entries.extend((0..=steps).map(|k| entry(3, k * COALESCE_GAP, 10)));
        let runs = coalesce(entries);
        let shape: Vec<_> =
            runs.iter().map(|r| (r.pack[0], r.start, r.end, r.entries.len())).collect();
        assert_eq!(
            shape,
            [
                (1, 0, 150 + COALESCE_GAP, 3),
                (1, 150 + COALESCE_GAP + far, 157 + COALESCE_GAP + far, 1),
                (2, 0, 100, 1),
                // Each step is close enough, but the range stops at the span.
                (3, 0, (steps - 1) * COALESCE_GAP + 10, steps as usize),
                (3, steps * COALESCE_GAP, steps * COALESCE_GAP + 10, 1),
            ]
        );
        for run in &runs {
            assert!(run.entries.windows(2).all(|w| w[0].off <= w[1].off));
            assert!(run.entries.iter().all(|e| e.off >= run.start
                && e.off + u64::from(e.len) <= run.end
                && e.pack == run.pack));
        }
        assert!(coalesce(Vec::new()).is_empty());
    }

    #[tokio::test]
    #[ignore = "needs postgres and s3: docker compose up -d"]
    async fn packed_store_conforms() {
        conformance::object_store(Arc::new(fresh().await.0)).await;
    }

    #[tokio::test]
    #[ignore = "needs postgres and s3: docker compose up -d"]
    async fn many_objects_span_packs_and_legacy_objects_stay_readable() {
        let (store, s3) = fresh().await;
        // Incompressible bodies so the batch exceeds one pack.
        let big: Vec<EncodedObject> = (0..8u8)
            .map(|i| {
                let mut v = vec![0u8; 5 * 1024 * 1024];
                let mut x = 0x9E37_79B9u32.wrapping_mul(u32::from(i) + 1);
                for b in v.iter_mut() {
                    x ^= x << 13;
                    x ^= x >> 17;
                    x ^= x << 5;
                    *b = x as u8;
                }
                EncodedObject::new(ObjectKind::Blob, &v)
            })
            .collect();
        store.put_many(&big).await.unwrap();
        let ids: Vec<ObjectId> = big.iter().map(|o| o.id()).collect();
        let got = store.get_many(&ids).await.unwrap();
        for (o, g) in big.iter().zip(got) {
            assert_eq!(g.as_ref(), Some(o));
        }
        // An object stored the old way (one key) is still found.
        let old = EncodedObject::new(ObjectKind::Blob, b"stored before packs");
        s3.put_many(std::slice::from_ref(&old)).await.unwrap();
        assert_eq!(store.get_many(&[old.id()]).await.unwrap(), vec![Some(old.clone())]);
        assert_eq!(store.has_many(&[old.id(), ids[0]]).await.unwrap(), vec![true, true]);
        assert_eq!(store.get(&old.id()).await.unwrap(), Some(old));
        // Re-putting is harmless.
        store.put_many(&big[..2]).await.unwrap();
        assert_eq!(store.get_many(&ids[..2]).await.unwrap().len(), 2);
    }

    /// A store whose S3 side is a scripted fake (index in scratch Postgres).
    async fn fake_backed(
        script: Vec<crate::s3_fake::Act>,
        attempts: u32,
    ) -> (PackedS3Store, crate::s3_fake::Fake) {
        use crate::s3_fake::{config, fast_timeouts, spawn};
        let url = std::env::var("ANT_TEST_PG_URL")
            .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into());
        let pg =
            PgStore::connect_scratch(&url, &format!("t_{}", rand::random::<u64>())).await.unwrap();
        let fake = spawn(script).await;
        let s3 = S3ObjectStore::connect(&config(&fake, fast_timeouts(attempts))).await.unwrap();
        (PackedS3Store::new(pg, s3), fake)
    }

    fn run_of(len: u64) -> Run {
        // The fake serves byte(i) = i % 251; these entries are never decoded
        // because the reads below fail.
        let id = ObjectId::from_bytes(&[9; 32]).unwrap();
        Run {
            pack: [1; 16],
            start: 0,
            end: len,
            entries: vec![IndexEntry { id, pack: [1; 16], off: 0, len: len as u32 }],
        }
    }

    #[tokio::test]
    #[ignore = "needs postgres: docker compose up -d"]
    async fn stalled_reads_release_their_read_budget() {
        use std::sync::atomic::Ordering;
        let (store, fake) = fake_backed(vec![crate::s3_fake::Act::Stall], 2).await;
        assert_eq!(store.reading.available_permits(), READ_PERMITS);
        // More concurrent stalled reads than the budget admits at once (each
        // costs READ_MIN_COST): they queue on permits, time out, and give the
        // permits back.
        let n = READ_BUDGET / READ_MIN_COST + 8;
        let t = std::time::Instant::now();
        let results = futures::future::join_all((0..n).map(|_| {
            let store = store.clone();
            async move { store.get_run(run_of(4096)).await }
        }))
        .await;
        assert!(results.iter().all(|r| matches!(r, Err(StoreError::Backend(_)))));
        assert_eq!(store.reading.available_permits(), READ_PERMITS, "permits leaked");
        assert!(fake.hits.load(Ordering::SeqCst) >= n as usize);
        // Bounded: queued waves of (2 attempts x 300 ms), not a hang.
        assert!(t.elapsed() < std::time::Duration::from_secs(30), "{:?}", t.elapsed());

        // A caller that gives up mid-read (future dropped) releases too.
        let s2 = store.clone();
        let h = tokio::spawn(async move { s2.get_run(run_of(4096)).await });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        h.abort();
        let _ = h.await;
        assert_eq!(store.reading.available_permits(), READ_PERMITS, "permits leaked on cancel");

        // The store is still usable: a good response goes through.
        let (store, _) = fake_backed(vec![crate::s3_fake::Act::Ok], 2).await;
        let body = store.s3.get_range("x", 0, 100).await.unwrap();
        assert_eq!(body.len(), 100);
    }
}
