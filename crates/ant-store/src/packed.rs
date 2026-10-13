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
//! - Pack keys are content-addressed, so retries are idempotent.
//! - Objects that are not in the index are looked up under their own S3 key
//!   (`S3ObjectStore`'s layout), so stores written before packs existed stay
//!   readable.
//! - An object stored twice (concurrent writers) occupies space in two packs;
//!   the index keeps the first. Packs are never rewritten here.

use std::collections::HashSet;

use ant_core::{EncodedObject, ObjectId, ObjectKind, hash_object};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};

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

#[derive(Clone)]
pub struct PackedS3Store {
    index: PgStore,
    s3: S3ObjectStore,
}

impl PackedS3Store {
    /// `index` holds the `object_index` table; `s3` receives the packs and is
    /// also consulted for objects stored one per key.
    pub fn new(index: PgStore, s3: S3ObjectStore) -> Self {
        Self { index, s3 }
    }

    async fn get_packed(&self, e: IndexEntry) -> Result<EncodedObject> {
        let body = self.s3.get_range(&self.s3.pack_key(&e.pack), e.off, u64::from(e.len)).await?;
        let id = e.id;
        if body.len() > BLOCKING_THRESHOLD {
            tokio::task::spawn_blocking(move || S3ObjectStore::decode_object(id, &body))
                .await
                .map_err(StoreError::backend)?
        } else {
            S3ObjectStore::decode_object(id, &body)
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
        let fetched: Vec<(ObjectId, EncodedObject)> = stream::iter(found)
            .map(|e| async move { Ok::<_, StoreError>((e.id, self.get_packed(e).await?)) })
            .buffer_unordered(FANOUT)
            .try_collect()
            .await?;
        let mut got: std::collections::HashMap<ObjectId, EncodedObject> =
            fetched.into_iter().collect();
        // Objects stored one per key (before packs existed).
        let legacy_ids: Vec<ObjectId> =
            wanted.iter().filter(|id| !got.contains_key(id)).copied().collect();
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
                let digest = hash_object(ObjectKind::Blob, &p.body);
                let mut pack = [0u8; 16];
                pack.copy_from_slice(&digest.as_bytes()[..16]);
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
            for (i, present) in misses.into_iter().zip(self.s3.has_many(&miss_ids).await?) {
                out[i] = present;
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

    async fn fresh() -> (PackedS3Store, S3ObjectStore) {
        let url = std::env::var("ANT_TEST_PG_URL")
            .unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into());
        let pg = PgStore::connect(&url, &format!("t_{}", rand::random::<u64>())).await.unwrap();
        let cfg = S3Config::for_tests(&format!("p{}", rand::random::<u64>()));
        let s3 = S3ObjectStore::connect(&cfg).await.unwrap();
        s3.ensure_bucket().await.unwrap();
        (PackedS3Store::new(pg, s3.clone()), s3)
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
        s3.put_many(&[old.clone()]).await.unwrap();
        assert_eq!(store.get_many(&[old.id()]).await.unwrap(), vec![Some(old.clone())]);
        assert_eq!(store.has_many(&[old.id(), ids[0]]).await.unwrap(), vec![true, true]);
        assert_eq!(store.get(&old.id()).await.unwrap(), Some(old));
        // Re-putting is harmless.
        store.put_many(&big[..2]).await.unwrap();
        assert_eq!(store.get_many(&ids[..2]).await.unwrap().len(), 2);
    }
}
