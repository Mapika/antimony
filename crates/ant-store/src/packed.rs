//! Large objects bundled into few S3 objects (or local pack files).
//!
//! [`PackedStore`] is generic over where the index lives ([`PackIndex`]:
//! Postgres's `object_index` table, or the local store's SQLite) and where
//! the packs live ([`PackFiles`]: S3, or append-only files of a local data
//! directory, `crate::local`). [`PackedS3Store`] is the Postgres + S3 pair
//! the docs below describe; the local pair differs only where noted in
//! `crate::local`.
//!
//! One S3 `PUT` per object is what limits bulk loads: an S3-compatible server
//! spends far more on a request than on a few kilobytes of body (measured on a
//! local SeaweedFS: 12 KB objects at under 1k PUT/s, 8 MB objects at 1 GB/s).
//! [`PackedS3Store`] therefore writes each `put_many` batch as a handful of
//! *packs* (concatenated compressed records, see [`frame`]) and records
//! `id -> (pack, offset, length)` of each object's record in the Postgres
//! `object_index` table. A read is one index lookup plus one ranged `GET`
//! per record (nearby records share one).
//!
//! - Compression: objects of one batch that look alike (versions of one file
//!   or directory: [`frame::similarity_key`]) are compressed together as one
//!   multi-object frame of at most [`FRAME_RAW`] uncompressed bytes, so a
//!   history import stores each version as little more than its difference
//!   to its neighbours. Objects with no look-alike in the batch are stored
//!   alone, in the record format of earlier versions.
//! - Reads decode a whole frame (bounded work, no delta chains) and keep
//!   recently decoded frames ([`FRAME_CACHE_BYTES`]): walks that read the
//!   versions of a directory one commit at a time decode each frame once.
//!   The cache holds immutable record contents only; which ids exist is
//!   always asked of the index first, so it never shows a deleted or
//!   condemned object.
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

pub(crate) mod cluster;
pub(crate) mod frame;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ant_core::{EncodedObject, ObjectId, ObjectKind};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};
use parking_lot::Mutex;
use tokio::sync::Semaphore;

use bytes::Bytes;

use crate::postgres::PgStore;
use crate::s3::S3ObjectStore;
use crate::{BLOCKING_THRESHOLD, ObjectStore, Result, StoreError};

pub use cluster::ClusterOptions;
pub use frame::FRAME_RAW;

/// Target size of one pack, after compression.
const PACK_BYTES: usize = 24 * 1024 * 1024;
/// Concurrent S3 requests per call.
const FANOUT: usize = 32;
/// Objects of one pack at most this far apart are read with one ranged GET
/// (the gap is downloaded and dropped)...
const COALESCE_GAP: u64 = 256 * 1024;
/// ...as long as the whole range stays below this many bytes.
const COALESCE_SPAN: u64 = 2 * 1024 * 1024;
/// Uncompressed bytes of decoded frames kept per store (shared by clones).
pub const FRAME_CACHE_BYTES: usize = 64 * 1024 * 1024;
/// Uncompressed bytes of frames being decoded at once, per store: a ranged
/// read of a few MiB can hold dozens of frames of up to `FRAME_RAW` each, so
/// `READ_BUDGET` alone does not bound decoding memory.
const DECODE_BUDGET: usize = 64 * 1024 * 1024;
const DECODE_UNIT: usize = 64 * 1024;
const DECODE_PERMITS: usize = DECODE_BUDGET / DECODE_UNIT;
/// Uncompressed bytes of records compressed per blocking task.
const ENCODE_TASK_BYTES: usize = 1024 * 1024;

/// Bytes of ranged reads in flight per store, across all callers. Bounds
/// memory, and S3 servers throttle beyond some in-flight volume (SeaweedFS
/// answers 429 above 64 MiB per volume server, counting whole storage chunks
/// rather than requested ranges, and the SDK then backs off for seconds).
/// Every read counts at least `READ_MIN_COST`, which caps concurrent requests.
const READ_BUDGET: u64 = 48 * 1024 * 1024;
const READ_MIN_COST: u64 = 1024 * 1024;
const READ_UNIT: u64 = 4096;
const READ_PERMITS: usize = (READ_BUDGET / READ_UNIT) as usize;

/// Location of a packed object: the record of `len` bytes at `off` of pack
/// `pack` (a frame shared with other objects, or the object alone).
#[derive(Debug, Clone, Copy)]
pub struct IndexEntry {
    pub id: ObjectId,
    pub pack: [u8; 16],
    pub off: u64,
    pub len: u32,
    /// The part of a shared frame's bytes attributed to this object (`None`:
    /// the whole record). Written, never read back by readers.
    pub share: Option<u32>,
}

/// The `id -> (pack, offset, length)` index of a [`PackedStore`].
#[async_trait]
pub trait PackIndex: Clone + Send + Sync + 'static {
    /// Index rows of those `ids` that are stored (and readable).
    async fn lookup_index(&self, ids: &[ObjectId]) -> Result<HashMap<ObjectId, IndexEntry>>;
    /// Record where objects live; first writer wins (identical content).
    /// Called only after the packs they point into are durable.
    async fn index_objects(&self, entries: &[IndexEntry]) -> Result<()>;
    /// Those `ids` that a collection run condemned (readers treat them as
    /// missing); only asked about objects stored outside packs.
    async fn condemned_among(&self, ids: &[ObjectId]) -> Result<HashSet<ObjectId>>;
}

/// Storage of the packs of a [`PackedStore`].
#[async_trait]
pub trait PackFiles: Clone + Send + Sync + 'static {
    /// Store each body as (part of) a pack and make it durable; returns,
    /// index-aligned, the pack holding it and the offset of its first byte.
    async fn write_packs(&self, bodies: Vec<Vec<u8>>) -> Result<Vec<([u8; 16], u64)>>;
    /// `len` bytes at `offset` of pack `pack`.
    async fn read_range(&self, pack: &[u8; 16], offset: u64, len: u64) -> Result<Bytes>;
    /// Whether objects may also be stored one per key, outside packs (S3
    /// stores written before packs existed).
    fn may_hold_unpacked(&self) -> bool {
        false
    }
    /// Objects stored one per key (see [`Self::may_hold_unpacked`]).
    async fn get_unpacked(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        Ok(vec![None; ids.len()])
    }
    /// Index-aligned existence of objects stored one per key.
    async fn has_unpacked(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        Ok(vec![false; ids.len()])
    }
}

#[async_trait]
impl PackIndex for PgStore {
    async fn lookup_index(&self, ids: &[ObjectId]) -> Result<HashMap<ObjectId, IndexEntry>> {
        PgStore::lookup_index(self, ids).await
    }
    async fn index_objects(&self, entries: &[IndexEntry]) -> Result<()> {
        PgStore::index_objects(self, entries).await
    }
    async fn condemned_among(&self, ids: &[ObjectId]) -> Result<HashSet<ObjectId>> {
        PgStore::condemned_among(self, ids).await
    }
}

#[async_trait]
impl PackFiles for S3ObjectStore {
    /// One S3 object per pack, under a random id: a pack is only ever
    /// indexed by the upload that wrote it (see the module docs).
    async fn write_packs(&self, bodies: Vec<Vec<u8>>) -> Result<Vec<([u8; 16], u64)>> {
        let placed: Vec<([u8; 16], u64)> = bodies.iter().map(|_| (rand::random(), 0)).collect();
        let keys: Vec<String> = placed.iter().map(|(pack, _)| self.pack_key(pack)).collect();
        let puts = bodies.into_iter().zip(keys).map(|(body, key)| self.put_raw(key, body));
        stream::iter(puts).buffer_unordered(FANOUT).try_collect::<Vec<()>>().await?;
        Ok(placed)
    }
    async fn read_range(&self, pack: &[u8; 16], offset: u64, len: u64) -> Result<Bytes> {
        self.get_range(&self.pack_key(pack), offset, len).await
    }
    fn may_hold_unpacked(&self) -> bool {
        true
    }
    async fn get_unpacked(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        ObjectStore::get_many(self, ids).await
    }
    async fn has_unpacked(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        ObjectStore::has_many(self, ids).await
    }
}

/// One ranged GET: `entries` (sorted by offset, all in `pack`) lie within
/// `start..end`. Members of one frame share an offset.
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

/// A decoded multi-object record: a frame or a delta frame.
enum Decoded {
    Frame(frame::Frame),
    Delta(frame::DeltaFrame),
}

impl Decoded {
    /// Decode a frame or delta frame record (`None`: a single-object record).
    fn decode(record: &[u8]) -> Option<Result<Self>> {
        if frame::is_frame(record) {
            Some(frame::decode_frame(record).map(Self::Frame))
        } else if frame::is_delta_frame(record) {
            Some(frame::decode_delta_frame(record).map(Self::Delta))
        } else {
            None
        }
    }

    fn weight(&self) -> usize {
        match self {
            Self::Frame(f) => f.weight(),
            Self::Delta(f) => f.weight(),
        }
    }

    fn member_len(&self, id: &ObjectId) -> usize {
        match self {
            Self::Frame(f) => f.member_len(id),
            Self::Delta(f) => f.member_len(id),
        }
    }

    fn get(&self, id: &ObjectId) -> Option<Result<EncodedObject>> {
        match self {
            Self::Frame(f) => f.get(id),
            Self::Delta(f) => f.get(id),
        }
    }
}

/// Recently decoded frames by `(pack, offset)`, bounded by bytes.
struct FrameCache {
    frames: lru::LruCache<([u8; 16], u64), Arc<Decoded>>,
    bytes: usize,
    limit: usize,
}

impl FrameCache {
    fn new(limit: usize) -> Self {
        Self { frames: lru::LruCache::unbounded(), bytes: 0, limit }
    }

    fn get(&mut self, key: &([u8; 16], u64)) -> Option<Arc<Decoded>> {
        self.frames.get(key).cloned()
    }

    fn insert(&mut self, key: ([u8; 16], u64), f: Arc<Decoded>) {
        if f.weight() > self.limit {
            return;
        }
        self.bytes += f.weight();
        if let Some(old) = self.frames.put(key, f) {
            self.bytes -= old.weight();
        }
        while self.bytes > self.limit {
            match self.frames.pop_lru() {
                Some((_, old)) => self.bytes -= old.weight(),
                None => break,
            }
        }
    }
}

/// The object `e.id` out of a decoded frame; the index naming a frame that
/// lacks it is corruption.
fn member(f: &Decoded, e: &IndexEntry) -> Result<EncodedObject> {
    f.get(&e.id).unwrap_or_else(|| {
        Err(frame::corrupt(format!(
            "object {} is not in the frame at {} of pack {}",
            e.id,
            e.off,
            hex::encode(e.pack)
        )))
    })
}

/// Large objects in packs of Postgres-indexed S3 objects.
pub type PackedS3Store = PackedStore<PgStore, S3ObjectStore>;

#[derive(Clone)]
pub struct PackedStore<I, F> {
    index: I,
    files: F,
    /// Range bytes being downloaded, in `READ_UNIT`s (see `READ_BUDGET`).
    reading: Arc<Semaphore>,
    /// Uncompressed frame bytes being decoded, in `DECODE_UNIT`s.
    decoding: Arc<Semaphore>,
    frames: Arc<Mutex<FrameCache>>,
    /// Uncompressed bytes per frame written ([`FRAME_RAW`]; 0: every object
    /// alone, the format older versions read).
    frame_raw: usize,
}

impl<I: PackIndex, F: PackFiles> PackedStore<I, F> {
    /// `index` holds the object index; `files` receives the packs and (S3) is
    /// also consulted for objects stored one per key.
    pub fn new(index: I, files: F) -> Self {
        Self {
            index,
            files,
            reading: Arc::new(Semaphore::new(READ_PERMITS)),
            decoding: Arc::new(Semaphore::new(DECODE_PERMITS)),
            frames: Arc::new(Mutex::new(FrameCache::new(FRAME_CACHE_BYTES))),
            frame_raw: FRAME_RAW,
        }
    }

    /// Keep at most `bytes` of decoded frames (default [`FRAME_CACHE_BYTES`]).
    pub fn with_frame_cache_bytes(mut self, bytes: usize) -> Self {
        self.frames = Arc::new(Mutex::new(FrameCache::new(bytes)));
        self
    }

    /// Write frames of at most `bytes` uncompressed bytes; 0 stores every
    /// object alone, in the record format of versions before 0.2.0.
    pub fn with_frame_bytes(mut self, bytes: usize) -> Self {
        self.frame_raw = bytes.min(frame::MAX_FRAME_RAW);
        self
    }

    /// Read one run with a single ranged GET and decode its records.
    async fn get_run(&self, run: Run) -> Result<Vec<(ObjectId, EncodedObject)>> {
        let len = run.end - run.start;
        let units = len.max(READ_MIN_COST).div_ceil(READ_UNIT).clamp(1, READ_PERMITS as u64) as u32;
        let permit = self.reading.acquire_many(units).await.map_err(StoreError::backend)?;
        let body = self.files.read_range(&run.pack, run.start, len).await?;
        drop(permit);
        // Every frame's uncompressed size: decoding memory, and CPU work.
        let raw = run
            .entries
            .chunk_by(|a, b| a.off == b.off)
            .map(|group| {
                let at = (group[0].off - run.start) as usize;
                frame::frame_raw_len(&body[at..at + group[0].len as usize]).unwrap_or(0)
            })
            .sum::<usize>();
        let work = len as usize + raw;
        let decoding = if raw == 0 {
            None
        } else {
            let units = raw.div_ceil(DECODE_UNIT).clamp(1, DECODE_PERMITS) as u32;
            Some(
                self.decoding
                    .clone()
                    .acquire_many_owned(units)
                    .await
                    .map_err(StoreError::backend)?,
            )
        };
        let cache = self.frames.clone();
        let decode = move || -> Result<Vec<(ObjectId, EncodedObject)>> {
            let _decoding = decoding;
            let mut out = Vec::with_capacity(run.entries.len());
            for group in run.entries.chunk_by(|a, b| a.off == b.off) {
                let first = &group[0];
                let at = (first.off - run.start) as usize;
                let record = &body[at..at + first.len as usize];
                if let Some(f) = Decoded::decode(record) {
                    let f = Arc::new(f?);
                    for e in group {
                        out.push((e.id, member(&f, e)?));
                    }
                    cache.lock().insert((run.pack, first.off), f);
                } else {
                    for e in group {
                        out.push((e.id, frame::decode_single(e.id, record)?));
                    }
                }
            }
            Ok(out)
        };
        if work > BLOCKING_THRESHOLD {
            tokio::task::spawn_blocking(decode).await.map_err(StoreError::backend)?
        } else {
            decode()
        }
    }
}

/// One pack under construction: its body and `(id, offset, length, share)`
/// per object.
#[derive(Default)]
struct Pack {
    body: Vec<u8>,
    entries: Vec<(ObjectId, u64, u32, Option<u32>)>,
}

/// One encoded record: its members' ids with their uncompressed lengths,
/// and the record bytes.
type Encoded = (Vec<(ObjectId, usize)>, Vec<u8>);

/// Compress `records` (indices into `objects`) on the blocking pool.
fn encode_task(
    objects: Arc<Vec<(ObjectId, u64, EncodedObject)>>,
    records: Vec<Vec<usize>>,
) -> tokio::task::JoinHandle<Result<Vec<Encoded>>> {
    tokio::task::spawn_blocking(move || {
        records
            .into_iter()
            .map(|record| {
                let members: Vec<&EncodedObject> = record.iter().map(|&i| &objects[i].2).collect();
                let ids: Vec<ObjectId> = record.iter().map(|&i| objects[i].0).collect();
                let bytes = frame::encode(&members, &ids)?;
                let sizes =
                    record.iter().map(|&i| (objects[i].0, objects[i].2.as_bytes().len())).collect();
                Ok((sizes, bytes))
            })
            .collect()
    })
}

#[async_trait]
impl<I: PackIndex, F: PackFiles> ObjectStore for PackedStore<I, F> {
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
        // Members of frames decoded recently need no S3 read.
        let mut cached: Vec<(IndexEntry, Arc<Decoded>)> = Vec::new();
        let mut fetch: Vec<IndexEntry> = Vec::with_capacity(found.len());
        {
            let mut cache = self.frames.lock();
            for e in found {
                match cache.get(&(e.pack, e.off)) {
                    Some(f) => cached.push((e, f)),
                    None => fetch.push(e),
                }
            }
        }
        let mut got: HashMap<ObjectId, EncodedObject> = HashMap::with_capacity(wanted.len());
        if !cached.is_empty() {
            // Copying out and hashing the members.
            let work: usize = cached.iter().map(|(e, f)| f.member_len(&e.id)).sum();
            let extract = move || -> Result<Vec<(ObjectId, EncodedObject)>> {
                cached.iter().map(|(e, f)| Ok((e.id, member(f, e)?))).collect()
            };
            let objects = if work > BLOCKING_THRESHOLD {
                tokio::task::spawn_blocking(extract).await.map_err(StoreError::backend)??
            } else {
                extract()?
            };
            got.extend(objects);
        }
        let fetched: Vec<Vec<(ObjectId, EncodedObject)>> = stream::iter(coalesce(fetch))
            .map(|run| self.get_run(run))
            .buffer_unordered(FANOUT)
            .try_collect()
            .await?;
        got.extend(fetched.into_iter().flatten());
        // Objects stored one per key (before packs existed).
        let mut legacy_ids: Vec<ObjectId> = if self.files.may_hold_unpacked() {
            wanted.iter().filter(|id| !got.contains_key(id)).copied().collect()
        } else {
            Vec::new()
        };
        if !legacy_ids.is_empty() {
            let condemned = self.index.condemned_among(&legacy_ids).await?;
            legacy_ids.retain(|id| !condemned.contains(id));
        }
        if !legacy_ids.is_empty() {
            for (id, obj) in legacy_ids.iter().zip(self.files.get_unpacked(&legacy_ids).await?) {
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
        // Hash and key in parallel on the blocking pool.
        let tasks = crate::compress_chunks(objects).into_iter().map(|chunk| {
            tokio::task::spawn_blocking(move || {
                chunk
                    .into_iter()
                    .map(|o| (o.id(), frame::similarity_key(&o), o))
                    .collect::<Vec<_>>()
            })
        });
        let mut seen: HashSet<ObjectId> = HashSet::with_capacity(objects.len());
        let mut unique: Vec<(ObjectId, u64, EncodedObject)> = Vec::with_capacity(objects.len());
        for part in futures::future::try_join_all(tasks).await.map_err(StoreError::backend)? {
            unique.extend(part.into_iter().filter(|(id, _, _)| seen.insert(*id)));
        }
        let records: Vec<Vec<usize>> = if self.frame_raw == 0 {
            (0..unique.len()).map(|i| vec![i]).collect()
        } else {
            let shape: Vec<(ObjectKind, u64, usize)> =
                unique.iter().map(|(_, key, o)| (o.kind(), *key, o.as_bytes().len())).collect();
            frame::plan(&shape, self.frame_raw)
        };

        // Compress records in parallel, about ENCODE_TASK_BYTES per task.
        let unique = Arc::new(unique);
        let mut tasks = Vec::new();
        let mut batch: Vec<Vec<usize>> = Vec::new();
        let mut batch_bytes = 0usize;
        for record in records {
            batch_bytes += record.iter().map(|&i| unique[i].2.as_bytes().len()).sum::<usize>();
            batch.push(record);
            if batch_bytes >= ENCODE_TASK_BYTES {
                tasks.push(encode_task(unique.clone(), std::mem::take(&mut batch)));
                batch_bytes = 0;
            }
        }
        if !batch.is_empty() {
            tasks.push(encode_task(unique.clone(), batch));
        }
        let mut packs: Vec<Pack> = Vec::new();
        let mut cur = Pack::default();
        for part in futures::future::try_join_all(tasks).await.map_err(StoreError::backend)? {
            for (members, bytes) in part? {
                if cur.body.len() + bytes.len() > PACK_BYTES && !cur.body.is_empty() {
                    packs.push(std::mem::take(&mut cur));
                }
                let off = cur.body.len() as u64;
                let len = u32::try_from(bytes.len()).map_err(|_| {
                    StoreError::InvalidArgument(format!("pack record of {} bytes", bytes.len()))
                })?;
                if let [(id, _)] = members.as_slice() {
                    cur.entries.push((*id, off, len, None));
                } else {
                    // Each member's share of the frame, by uncompressed size.
                    let raw = members.iter().map(|(_, n)| *n as u64).sum::<u64>().max(1);
                    for (id, n) in &members {
                        let share = (u64::from(len) * *n as u64).div_ceil(raw).clamp(1, len.into());
                        cur.entries.push((*id, off, len, Some(share as u32)));
                    }
                }
                cur.body.extend_from_slice(&bytes);
            }
        }
        if !cur.body.is_empty() {
            packs.push(cur);
        }

        // Packs first (durable), then the index rows that point into them.
        let (bodies, layouts): (Vec<Vec<u8>>, Vec<_>) =
            packs.into_iter().map(|p| (p.body, p.entries)).unzip();
        let placed = self.files.write_packs(bodies).await?;
        let mut entries: Vec<IndexEntry> = Vec::new();
        for ((pack, base), layout) in placed.into_iter().zip(layouts) {
            entries.extend(layout.into_iter().map(|(id, off, len, share)| IndexEntry {
                id,
                pack,
                off: base + off,
                len,
                share,
            }));
        }
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
        if !misses.is_empty() && self.files.may_hold_unpacked() {
            let miss_ids: Vec<ObjectId> = misses.iter().map(|&i| ids[i]).collect();
            let condemned = self.index.condemned_among(&miss_ids).await?;
            for (i, present) in misses.into_iter().zip(self.files.has_unpacked(&miss_ids).await?) {
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
        let url = crate::testing::pg_url();
        let pg =
            PgStore::connect_scratch(&url, &crate::testing::scratch_schema("s")).await.unwrap();
        let cfg = S3Config::for_tests(&format!("p{}", rand::random::<u64>()));
        let s3 = S3ObjectStore::connect_scratch(&cfg).await.unwrap();
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
            share: None,
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
        let url = crate::testing::pg_url();
        let pg =
            PgStore::connect_scratch(&url, &crate::testing::scratch_schema("s")).await.unwrap();
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
            entries: vec![IndexEntry { id, pack: [1; 16], off: 0, len: len as u32, share: None }],
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
        let body = store.files.get_range("x", 0, 100).await.unwrap();
        assert_eq!(body.len(), 100);
    }

    /// Versions of a file (one line edited per version) and of a directory
    /// (one entry added per version), as an import writes them.
    fn history(versions: usize) -> Vec<EncodedObject> {
        let mut out = Vec::new();
        for v in 0..versions {
            let mut text = String::new();
            for line in 0..400 {
                let word = if line == v % 400 { format!("edit {v}") } else { "same".into() };
                text.push_str(&format!("{line:4}: a line of source code, {word}\n"));
            }
            out.push(EncodedObject::new(ObjectKind::Blob, text.as_bytes()));
            let mut tree = Vec::new();
            for e in 0..(100 + v) {
                tree.extend_from_slice(format!("100644 file{e:05}.c\0").as_bytes());
                let entry = format!("{e}/{}", e % 7 == v % 7);
                tree.extend_from_slice(
                    EncodedObject::new(ObjectKind::Blob, entry.as_bytes()).id().as_bytes(),
                );
            }
            out.push(EncodedObject::new(ObjectKind::Tree, &tree));
        }
        out
    }

    /// Bytes of the records the index points at.
    async fn stored_bytes(store: &PackedS3Store, ids: &[ObjectId]) -> u64 {
        let index = store.index.lookup_index(ids).await.unwrap();
        let records: HashSet<([u8; 16], u64, u32)> =
            index.values().map(|e| (e.pack, e.off, e.len)).collect();
        records.iter().map(|r| u64::from(r.2)).sum()
    }

    #[tokio::test]
    #[ignore = "needs postgres and s3: docker compose up -d"]
    async fn frames_conform_and_so_does_the_old_record_format() {
        conformance::object_store(Arc::new(fresh().await.0.with_frame_bytes(0))).await;
        conformance::object_store(Arc::new(fresh().await.0.with_frame_bytes(4096))).await;
    }

    #[tokio::test]
    #[ignore = "needs postgres and s3: docker compose up -d"]
    async fn versions_written_together_are_stored_compactly_and_read_back_any_way() {
        let (store, s3) = fresh().await;
        let objects = history(60);
        let ids: Vec<ObjectId> = objects.iter().map(EncodedObject::id).collect();
        store.put_many(&objects).await.unwrap();
        let (single, _) = fresh().await;
        let single = single.with_frame_bytes(0);
        single.put_many(&objects).await.unwrap();
        let (framed, alone) = (stored_bytes(&store, &ids).await, stored_bytes(&single, &ids).await);
        assert!(framed * 8 < alone, "frames {framed} bytes, objects alone {alone}");

        // All at once, from a store with nothing cached.
        let reader = PackedS3Store::new(store.index.clone(), s3.clone());
        let got = reader.get_many(&ids).await.unwrap();
        assert!(got.iter().zip(&objects).all(|(g, o)| g.as_ref() == Some(o)));
        // One at a time and in odd subsets, each from a cold store too.
        for (i, id) in ids.iter().enumerate().step_by(7) {
            let cold = PackedS3Store::new(store.index.clone(), s3.clone());
            assert_eq!(cold.get(id).await.unwrap().as_ref(), Some(&objects[i]));
            assert_eq!(reader.get(id).await.unwrap().as_ref(), Some(&objects[i]));
        }
        let subset: Vec<ObjectId> = ids.iter().rev().step_by(3).copied().collect();
        let got = reader.get_many(&subset).await.unwrap();
        for (id, g) in subset.iter().zip(got) {
            assert_eq!(g.map(|o| o.id()), Some(*id));
        }
        // Duplicates and absent ids among frame members.
        let absent = EncodedObject::new(ObjectKind::Blob, b"absent").id();
        let got = reader.get_many(&[ids[3], absent, ids[3]]).await.unwrap();
        assert_eq!(got, vec![Some(objects[3].clone()), None, Some(objects[3].clone())]);
        assert_eq!(reader.has_many(&[ids[5], absent]).await.unwrap(), vec![true, false]);
    }

    #[tokio::test]
    #[ignore = "needs postgres and s3: docker compose up -d"]
    async fn decoded_frames_are_reused_without_reading_s3() {
        let (store, s3) = fresh().await;
        let objects = history(20);
        let ids: Vec<ObjectId> = objects.iter().map(EncodedObject::id).collect();
        store.put_many(&objects).await.unwrap();
        let reader = PackedS3Store::new(store.index.clone(), s3.clone());
        assert_eq!(reader.get(&ids[0]).await.unwrap().as_ref(), Some(&objects[0]));
        // Remove the packs: members of the frame decoded above are still
        // served (the cache holds immutable record contents; the index still
        // names them), and a store without the cache fails.
        let index = store.index.lookup_index(&ids).await.unwrap();
        let packs: HashSet<[u8; 16]> = index.values().map(|e| e.pack).collect();
        s3.delete_packs(&packs.into_iter().collect::<Vec<_>>()).await.unwrap();
        let first = index[&ids[0]];
        let mates: Vec<usize> = (0..ids.len())
            .filter(|&i| index[&ids[i]].pack == first.pack && index[&ids[i]].off == first.off)
            .collect();
        assert!(mates.len() > 1, "the first object shares a frame");
        for &i in &mates {
            assert_eq!(reader.get(&ids[i]).await.unwrap().as_ref(), Some(&objects[i]));
        }
        let cold = PackedS3Store::new(store.index.clone(), s3.clone());
        assert!(cold.get(&ids[0]).await.is_err());
    }

    #[test]
    fn the_frame_cache_stays_within_its_budget() {
        let objects = history(8);
        let ids: Vec<ObjectId> = objects.iter().map(EncodedObject::id).collect();
        let refs: Vec<&EncodedObject> = objects.iter().collect();
        let f = Arc::new(Decoded::Frame(
            frame::decode_frame(&frame::encode(&refs, &ids).unwrap()).unwrap(),
        ));
        let w = f.weight();
        let mut cache = FrameCache::new(3 * w);
        for off in 0..10u64 {
            cache.insert(([1; 16], off), f.clone());
            assert!(cache.bytes <= 3 * w);
        }
        assert_eq!(cache.frames.len(), 3);
        assert!(cache.get(&([1; 16], 9)).is_some());
        assert!(cache.get(&([1; 16], 0)).is_none());
        // Re-inserting a key does not count it twice; a frame larger than
        // the whole budget is not kept.
        cache.insert(([1; 16], 9), f.clone());
        assert_eq!(cache.bytes, 3 * w);
        let mut tiny = FrameCache::new(w - 1);
        tiny.insert(([1; 16], 0), f);
        assert_eq!((tiny.bytes, tiny.frames.len()), (0, 0));
    }

    #[tokio::test]
    #[ignore = "needs postgres and s3: docker compose up -d"]
    async fn old_single_records_and_frames_mix_in_one_read() {
        let (store, s3) = fresh().await;
        let old = store.clone().with_frame_bytes(0);
        let a = history(10);
        let b = history(30).split_off(20);
        old.put_many(&a).await.unwrap();
        store.put_many(&b).await.unwrap();
        // One object stored one per key (before packs existed).
        let legacy = EncodedObject::new(ObjectKind::Blob, b"stored before packs");
        s3.put_many(std::slice::from_ref(&legacy)).await.unwrap();
        let all: Vec<EncodedObject> = a.iter().chain(&b).chain([&legacy]).cloned().collect();
        let ids: Vec<ObjectId> = all.iter().map(EncodedObject::id).collect();
        for reader in [old, store.clone(), PackedS3Store::new(store.index.clone(), s3.clone())] {
            let got = reader.get_many(&ids).await.unwrap();
            assert!(got.iter().zip(&all).all(|(g, o)| g.as_ref() == Some(o)));
        }
        let index = store.index.lookup_index(&ids).await.unwrap();
        assert_eq!(index.len(), ids.len() - 1);
        let shared = |o: &EncodedObject| {
            let e = index[&o.id()];
            index.values().filter(|x| (x.pack, x.off) == (e.pack, e.off)).count()
        };
        assert!(a.iter().all(|o| shared(o) == 1), "written alone");
        assert!(b.iter().any(|o| shared(o) > 1), "written in frames");
    }

    #[tokio::test]
    #[ignore = "needs postgres and s3: docker compose up -d"]
    async fn corrupt_packs_are_read_errors_not_panics() {
        let (store, s3) = fresh().await;
        let objects = history(8);
        let ids: Vec<ObjectId> = objects.iter().map(EncodedObject::id).collect();
        store.put_many(&objects).await.unwrap();
        let index = store.index.lookup_index(&ids).await.unwrap();
        let pack = index[&ids[0]].pack;
        assert!(index.values().all(|e| e.pack == pack), "one pack");
        let key = s3.pack_key(&pack);
        let size = index.values().map(|e| e.off + u64::from(e.len)).max().unwrap();
        let good = s3.get_range(&key, 0, size).await.unwrap().to_vec();
        let damaged = |f: &dyn Fn(&mut Vec<u8>)| {
            let mut b = good.clone();
            f(&mut b);
            b
        };
        let cases = [
            damaged(&|b| b.iter_mut().for_each(|x| *x = 0xAA)),
            damaged(&|b| b.iter_mut().skip(40).step_by(97).for_each(|x| *x ^= 0x10)),
            damaged(&|b| {
                // Frame headers claiming absurd sizes.
                for e in index.values() {
                    let at = e.off as usize;
                    if b[at..].starts_with(b"AFR1") {
                        b[at + 8..at + 12].copy_from_slice(&u32::MAX.to_le_bytes());
                    }
                }
            }),
        ];
        for body in cases {
            s3.put_raw(key.clone(), body).await.unwrap();
            let cold = PackedS3Store::new(store.index.clone(), s3.clone());
            let r = cold.get_many(&ids).await;
            assert!(matches!(r, Err(StoreError::Backend(_))), "{r:?}");
        }
        s3.put_raw(key, good).await.unwrap();
        let cold = PackedS3Store::new(store.index.clone(), s3.clone());
        let got = cold.get_many(&ids).await.unwrap();
        assert!(got.iter().zip(&objects).all(|(g, o)| g.as_ref() == Some(o)));
    }
}
