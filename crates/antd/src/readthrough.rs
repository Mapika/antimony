//! Read-through cache: local store first, misses fetched from upstream in
//! one batch, verified, and written back locally.
//!
//! - `get_many`: one `local.get_many(ids)`; collect misses (deduplicated);
//!   one `upstream.get_many(misses)`; `verify` each fetched object against
//!   its requested id (mismatch → `StoreError::Backend`); one
//!   `local.put_many(fetched)`; return merged results in input order.
//! - `verified_upstream()` skips the second hash when the upstream already
//!   verifies (`RemoteObjectStore` does); `over_disk` stores fetched objects
//!   into the disk cache without re-hashing them. Otherwise verification runs
//!   on several threads.
//! - `put_many`: write to upstream first, then local (write-through).
//! - `has_many`: local first, upstream for misses.
//! - `with_tree_bundles(remote)`: [`ReadThrough::get_trees_bundled`] (the
//!   workspace's [`DirFetch`]) fetches locally missing trees as HTTP tree
//!   bundles, storing every bundled object (already verified by `remote`)
//!   locally, so the walk's later reads of their blobs and subtrees are
//!   local hits. Trees that no bundle delivered go through `get_many`.
//! - In-flight dedup: concurrent `get_many` calls that miss on the same id
//!   trigger only one upstream fetch for it (a map of shared futures).
//! - Cross-process dedup (`over_disk`, on by default;
//!   [`ReadThrough::with_cross_process_dedup`]): before fetching, a process
//!   claims each missing object (or tree, for bundles) with a lock file in
//!   the shared cache (`crate::claim`). Ids another process has claimed are
//!   fetched by that process: this one fetches and stores its own claims
//!   first, then waits (up to [`WAIT_FOR_PEER`]) for the others' and reads
//!   them from the cache, fetching itself whatever is still missing.
//! - Tier counters (`crate::tiers`) record how each object was served and are
//!   added to the cache directory's totals when the `ReadThrough` is dropped.

use std::collections::HashMap;
use std::sync::Arc;

use ant_core::{EncodedObject, ObjectId};
use ant_store::{ObjectStore, Result, StoreError};
use async_trait::async_trait;
use futures::future::{BoxFuture, FutureExt, Shared};

use crate::claim::{Claim, Claims};
use crate::disk::DiskObjectStore;
use crate::par::par_map;
use crate::remote::RemoteObjectStore;
use crate::tiers::TierCounters;
use crate::workspace::DirFetch;
use parking_lot::Mutex;

type Fetched = std::result::Result<Arc<HashMap<ObjectId, EncodedObject>>, Arc<String>>;
/// How long to wait for another process that claimed an object before
/// fetching it anyway.
pub const WAIT_FOR_PEER: std::time::Duration = std::time::Duration::from_secs(30);

type InFlight = Shared<BoxFuture<'static, Fetched>>;

pub struct ReadThrough {
    local: Arc<dyn ObjectStore>,
    /// `local` again, when it is a [`DiskObjectStore`]: fetched objects are
    /// already verified, so they are stored without being re-hashed.
    disk: Option<Arc<DiskObjectStore>>,
    upstream: Arc<dyn ObjectStore>,
    /// Upstream verifies what it returns (e.g. `RemoteObjectStore`).
    trusted_upstream: bool,
    inflight: Arc<Mutex<HashMap<ObjectId, InFlight>>>,
    /// Source of tree bundles (HTTP), if enabled.
    bundles: Option<RemoteObjectStore>,
    /// Cross-process fetch claims in the disk cache's directory.
    claims: Option<Claims>,
    stats: Arc<TierCounters>,
}

impl ReadThrough {
    pub fn new(local: Arc<dyn ObjectStore>, upstream: Arc<dyn ObjectStore>) -> Self {
        Self {
            local,
            disk: None,
            upstream,
            trusted_upstream: false,
            inflight: Default::default(),
            bundles: None,
            claims: None,
            stats: Arc::default(),
        }
    }

    /// Read-through over a disk cache. Fetched objects are written to it
    /// without being hashed again (they were just verified).
    pub fn over_disk(disk: Arc<DiskObjectStore>, upstream: Arc<dyn ObjectStore>) -> Self {
        Self {
            claims: Some(Claims::new(disk.root())),
            local: disk.clone(),
            disk: Some(disk),
            upstream,
            trusted_upstream: false,
            inflight: Default::default(),
            bundles: None,
            stats: Arc::default(),
        }
    }

    /// Turn cross-process fetch claims on or off (on by default over a disk
    /// cache; off: every process fetches its own misses, as before).
    pub fn with_cross_process_dedup(mut self, on: bool) -> Self {
        self.claims = match (&self.disk, on) {
            (Some(disk), true) => Some(Claims::new(disk.root())),
            _ => None,
        };
        self
    }

    /// Fetch whichever of `ids` are missing locally (without reading the ones
    /// that are present); returns how many were missing. Objects upstream
    /// lacks are not an error.
    pub async fn ensure_local(&self, ids: &[ObjectId]) -> Result<usize> {
        let present = self.local.has_many(ids).await?;
        let misses: Vec<ObjectId> =
            ids.iter().zip(&present).filter(|(_, p)| !**p).map(|(id, _)| *id).collect();
        if !misses.is_empty() {
            self.get_many(&misses).await?;
        }
        Ok(misses.len())
    }

    /// This handle's tier counters so far (`crate::tiers::NAMES` order).
    pub fn tier_counts(&self) -> [u64; 7] {
        self.stats.snapshot()
    }

    /// Fetch missing trees for walks as tree bundles from `remote` (used
    /// only while it has an HTTP base and the server serves bundles).
    pub fn with_tree_bundles(mut self, remote: RemoteObjectStore) -> Self {
        self.bundles = Some(remote);
        self
    }

    /// `get_many(trees)` for a walk that will also read the trees' blobs
    /// and subtrees: trees missing locally are fetched as bundles when
    /// enabled, and every bundled object is stored locally.
    pub async fn get_trees_bundled(
        &self,
        trees: &[ObjectId],
    ) -> Result<Vec<Option<EncodedObject>>> {
        let Some(remote) = self.bundles.as_ref().filter(|r| r.tree_bundles_enabled()) else {
            return self.get_many(trees).await;
        };
        let mut out = self.local.get_many(trees).await?;
        self.stats.add(&self.stats.local_hits, out.iter().flatten().count());
        let mut misses: Vec<ObjectId> =
            trees.iter().zip(&out).filter(|(_, o)| o.is_none()).map(|(id, _)| *id).collect();
        if misses.is_empty() {
            return Ok(out);
        }
        misses.sort_unstable();
        misses.dedup();
        // Trees another process is already fetching as bundles are waited
        // for after our own bundles are stored (never while holding claims).
        let (owned, peers) = self.claim(&misses, "b-").await;
        // A peer may have finished between our local miss and our claim.
        let owned = drop_present(self.local.as_ref(), owned).await?;
        let (ids, guards): (Vec<ObjectId>, Vec<Option<Claim>>) = owned.into_iter().unzip();
        let mut fetched: HashMap<ObjectId, EncodedObject> = HashMap::new();
        if !ids.is_empty() {
            self.stats.add(&self.stats.bundle_requests, ids.len());
            for bundle in remote.get_tree_bundles(&ids).await.into_iter().flatten() {
                fetched.extend(bundle);
            }
        }
        if !fetched.is_empty() {
            self.stats.add(&self.stats.bundled_objects, fetched.len());
            let found: Vec<(ObjectId, EncodedObject)> =
                fetched.iter().map(|(id, o)| (*id, o.clone())).collect();
            match &self.disk {
                Some(disk) => disk.put_many_trusted(found).await?,
                None => {
                    let objs: Vec<EncodedObject> = found.into_iter().map(|(_, o)| o).collect();
                    self.local.put_many(&objs).await?
                }
            }
        }
        drop(guards);
        if !peers.is_empty() {
            self.wait_for_peers(&peers, "b-").await;
            let got = self.local.get_many(&peers).await?;
            self.stats.add(&self.stats.waited_hits, got.iter().flatten().count());
            for (id, o) in peers.iter().zip(got) {
                if let Some(o) = o {
                    fetched.insert(*id, o);
                }
            }
        }
        let mut rest = Vec::new();
        for (slot, id) in out.iter_mut().zip(trees) {
            if slot.is_none() {
                *slot = fetched.get(id).cloned();
                if slot.is_none() {
                    rest.push(*id);
                }
            }
        }
        if !rest.is_empty() {
            let got: HashMap<ObjectId, EncodedObject> = rest
                .iter()
                .copied()
                .zip(self.get_many(&rest).await?)
                .filter_map(|(id, o)| Some((id, o?)))
                .collect();
            for (slot, id) in out.iter_mut().zip(trees) {
                if slot.is_none() {
                    *slot = got.get(id).cloned();
                }
            }
        }
        Ok(out)
    }

    /// Declare that `upstream` already hash-verifies every object it returns
    /// (true of [`RemoteObjectStore`], also behind wrappers that merely
    /// forward), so fetched objects are not hashed a second time.
    pub fn verified_upstream(mut self) -> Self {
        self.trusted_upstream = true;
        self
    }

    /// Claim each of `ids` (key prefix `prefix`); returns the ones this
    /// process now owns (with their guard, `None` if claiming failed: fetch
    /// anyway) and the ones a peer holds. Without claims everything is owned.
    async fn claim(
        &self,
        ids: &[ObjectId],
        prefix: &'static str,
    ) -> (Vec<(ObjectId, Option<Claim>)>, Vec<ObjectId>) {
        claim_ids(self.claims.clone(), ids.to_vec(), prefix).await
    }

    /// Wait (bounded by [`WAIT_FOR_PEER`] overall) until the peers holding
    /// `ids` are done.
    async fn wait_for_peers(&self, ids: &[ObjectId], prefix: &'static str) {
        if let Some(claims) = self.claims.clone() {
            wait_ids(claims, ids.to_vec(), prefix).await;
        }
    }

    /// Fetch `ids` from upstream, verify, and fill the local store. Re-checks
    /// local first: a previous fetch may have landed after our local miss.
    /// With claims, ids a peer process is fetching are waited for (after this
    /// process's own are stored) and read from the cache.
    async fn fetch(
        local: Arc<dyn ObjectStore>,
        disk: Option<Arc<DiskObjectStore>>,
        trusted: bool,
        upstream: Arc<dyn ObjectStore>,
        claims: Option<Claims>,
        stats: Arc<TierCounters>,
        ids: Vec<ObjectId>,
    ) -> Result<HashMap<ObjectId, EncodedObject>> {
        let mut out = HashMap::with_capacity(ids.len());
        let mut missing = Vec::new();
        for (id, o) in ids.iter().zip(local.get_many(&ids).await?) {
            match o {
                Some(o) => {
                    stats.add(&stats.local_hits, 1);
                    out.insert(*id, o);
                }
                None => missing.push(*id),
            }
        }
        if missing.is_empty() {
            return Ok(out);
        }
        if let Some(claims) = claims {
            let (owned, others) = claim_ids(Some(claims.clone()), missing, "").await;
            // A peer may have finished between our local miss and our claim.
            let claimed = owned.len();
            let owned = drop_present(local.as_ref(), owned).await?;
            let landed_meanwhile = owned.len() != claimed;
            let (mine, guards): (Vec<ObjectId>, Vec<Option<Claim>>) = owned.into_iter().unzip();
            if !mine.is_empty() {
                let found = Self::fetch_store(&local, &disk, trusted, &upstream, &stats, mine);
                out.extend(found.await?);
            }
            drop(guards);
            missing = Vec::new();
            if !others.is_empty() {
                wait_ids(claims, others.clone(), "").await;
                let got = local.get_many(&others).await?;
                for (id, o) in others.into_iter().zip(got) {
                    match o {
                        Some(o) => {
                            stats.add(&stats.waited_hits, 1);
                            out.insert(id, o);
                        }
                        None => missing.push(id),
                    }
                }
            }
            if landed_meanwhile {
                let lacking: Vec<ObjectId> =
                    ids.iter().filter(|id| !out.contains_key(id)).copied().collect();
                for (id, o) in lacking.iter().zip(local.get_many(&lacking).await?) {
                    if let Some(o) = o {
                        out.insert(*id, o);
                    }
                }
            }
        }
        if !missing.is_empty() {
            out.extend(
                Self::fetch_store(&local, &disk, trusted, &upstream, &stats, missing).await?,
            );
        }
        Ok(out)
    }

    /// Upstream `get_many` of `missing`, verified, stored locally.
    async fn fetch_store(
        local: &Arc<dyn ObjectStore>,
        disk: &Option<Arc<DiskObjectStore>>,
        trusted: bool,
        upstream: &Arc<dyn ObjectStore>,
        stats: &TierCounters,
        missing: Vec<ObjectId>,
    ) -> Result<Vec<(ObjectId, EncodedObject)>> {
        stats.add(&stats.fetched_requests, 1);
        let got = upstream.get_many(&missing).await?;
        let found: Vec<(ObjectId, EncodedObject)> =
            missing.iter().copied().zip(got).filter_map(|(id, o)| Some((id, o?))).collect();
        stats.add(&stats.fetched_objects, found.len());
        let found = if trusted {
            found
        } else {
            // Hash in parallel; the first mismatch (in input order) is the error.
            let verdicts = tokio::task::spawn_blocking(move || {
                let r = par_map(&found, 16, |(id, o)| o.verify(id));
                (found, r)
            })
            .await
            .map_err(StoreError::backend)?;
            let (found, results) = verdicts;
            results
                .into_iter()
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(StoreError::backend)?;
            found
        };
        match disk {
            Some(disk) => disk.put_many_trusted(found.clone()).await?,
            None => {
                let objs: Vec<EncodedObject> = found.iter().map(|(_, o)| o.clone()).collect();
                local.put_many(&objs).await?
            }
        }
        Ok(found)
    }
}

/// Claim `ids` under `{prefix}{hex}` keys (see [`crate::claim`]): the ids to
/// fetch (with their guards) and the ids a peer is fetching. Without claims,
/// or when claiming fails, an id is ours to fetch.
async fn claim_ids(
    claims: Option<Claims>,
    ids: Vec<ObjectId>,
    prefix: &'static str,
) -> (Vec<(ObjectId, Option<Claim>)>, Vec<ObjectId>) {
    let Some(claims) = claims else {
        return (ids.into_iter().map(|id| (id, None)).collect(), Vec::new());
    };
    let work = ids.clone();
    let done = tokio::task::spawn_blocking(move || {
        let mut owned = Vec::new();
        let mut peers = Vec::new();
        for id in work {
            match claims.try_claim(&format!("{prefix}{}", id.to_hex())) {
                Ok(Some(c)) => owned.push((id, Some(c))),
                Ok(None) => peers.push(id),
                Err(_) => owned.push((id, None)),
            }
        }
        (owned, peers)
    })
    .await;
    // A panic in the blocking pool is not worth failing a read: fetch all.
    done.unwrap_or_else(|_| (ids.into_iter().map(|id| (id, None)).collect(), Vec::new()))
}

/// Drop the claims on ids that are in the local store after all (the
/// previous holder finished between our miss and our claim).
async fn drop_present(
    local: &dyn ObjectStore,
    owned: Vec<(ObjectId, Option<Claim>)>,
) -> Result<Vec<(ObjectId, Option<Claim>)>> {
    if owned.is_empty() {
        return Ok(owned);
    }
    let ids: Vec<ObjectId> = owned.iter().map(|(id, _)| *id).collect();
    let present = local.has_many(&ids).await?;
    Ok(owned.into_iter().zip(present).filter(|(_, p)| !*p).map(|(o, _)| o).collect())
}

/// Wait until every peer holding one of `ids` is done, for at most
/// [`WAIT_FOR_PEER`] in total.
async fn wait_ids(claims: Claims, ids: Vec<ObjectId>, prefix: &'static str) {
    let _ = tokio::task::spawn_blocking(move || {
        let deadline = std::time::Instant::now() + WAIT_FOR_PEER;
        for id in ids {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            claims.wait_released(&format!("{prefix}{}", id.to_hex()), left);
        }
    })
    .await;
}

impl Drop for ReadThrough {
    fn drop(&mut self) {
        if let Some(disk) = &self.disk {
            self.stats.persist(disk.root());
        }
    }
}

#[async_trait]
impl DirFetch for ReadThrough {
    async fn get_dirs(&self, trees: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        self.get_trees_bundled(trees).await
    }
}

#[async_trait]
impl ObjectStore for ReadThrough {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        let mut out = self.local.get_many(ids).await?;
        let mut misses: Vec<ObjectId> =
            ids.iter().zip(&out).filter(|(_, o)| o.is_none()).map(|(id, _)| *id).collect();
        self.stats.add(&self.stats.local_hits, ids.len() - misses.len());
        if misses.is_empty() {
            return Ok(out);
        }
        misses.sort_unstable();
        misses.dedup();

        // Join fetches already in flight; start one batch for the rest.
        let mut waits: Vec<InFlight> = Vec::new();
        let mut led = Vec::new();
        {
            let mut map = self.inflight.lock();
            let mut mine = Vec::new();
            for id in &misses {
                match map.get(id) {
                    Some(f) => {
                        if !waits.iter().any(|w| w.ptr_eq(f)) {
                            waits.push(f.clone());
                        }
                    }
                    None => mine.push(*id),
                }
            }
            self.stats.add(&self.stats.joined, misses.len() - mine.len());
            if !mine.is_empty() {
                let (local, disk, trusted, upstream, claims, stats, batch) = (
                    self.local.clone(),
                    self.disk.clone(),
                    self.trusted_upstream,
                    self.upstream.clone(),
                    self.claims.clone(),
                    self.stats.clone(),
                    mine.clone(),
                );
                let fut: InFlight = async move {
                    Self::fetch(local, disk, trusted, upstream, claims, stats, batch)
                        .await
                        .map(Arc::new)
                        .map_err(|e| Arc::new(e.to_string()))
                }
                .boxed()
                .shared();
                for id in &mine {
                    map.insert(*id, fut.clone());
                }
                waits.push(fut);
                led = mine;
            }
        }

        let results = futures::future::join_all(waits).await;
        if !led.is_empty() {
            let mut map = self.inflight.lock();
            for id in &led {
                map.remove(id);
            }
        }
        let mut found: HashMap<ObjectId, EncodedObject> = HashMap::new();
        for r in results {
            let r = r.map_err(|e| StoreError::backend(std::io::Error::other(e.to_string())))?;
            found.extend(r.iter().map(|(k, v)| (*k, v.clone())));
        }
        for (slot, id) in out.iter_mut().zip(ids) {
            if slot.is_none() {
                *slot = found.get(id).cloned();
            }
        }
        Ok(out)
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        self.upstream.put_many(objects).await?;
        self.local.put_many(objects).await
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        let mut out = self.local.has_many(ids).await?;
        let misses: Vec<usize> = (0..ids.len()).filter(|&i| !out[i]).collect();
        if !misses.is_empty() {
            let miss_ids: Vec<ObjectId> = misses.iter().map(|&i| ids[i]).collect();
            for (i, present) in misses.into_iter().zip(self.upstream.has_many(&miss_ids).await?) {
                out[i] = present;
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use ant_core::ObjectKind;
    use ant_store::memory::MemoryObjectStore;
    use ant_store::{ObjectStoreExt, conformance};

    use crate::disk::DiskObjectStore;
    use crate::test_support::CountingStore;

    /// Upstream that counts how many ids it was asked for and is slow.
    fn slow_upstream() -> CountingStore {
        CountingStore::default().with_delay(Duration::from_millis(50))
    }

    /// Upstream that returns a different object than requested.
    struct Lying(EncodedObject);

    #[async_trait]
    impl ObjectStore for Lying {
        async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
            Ok(ids.iter().map(|_| Some(self.0.clone())).collect())
        }
        async fn put_many(&self, _: &[EncodedObject]) -> Result<()> {
            Ok(())
        }
        async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
            Ok(vec![true; ids.len()])
        }
    }

    #[tokio::test]
    async fn readthrough_conforms() {
        let rt = ReadThrough::new(
            Arc::new(MemoryObjectStore::default()),
            Arc::new(MemoryObjectStore::default()),
        );
        conformance::object_store(Arc::new(rt)).await;
    }

    #[tokio::test]
    async fn misses_fill_local() {
        let local = Arc::new(MemoryObjectStore::default());
        let up = Arc::new(slow_upstream());
        let o = EncodedObject::new(ObjectKind::Blob, b"remote");
        up.inner().put(o.clone()).await.unwrap();
        let rt = ReadThrough::new(local.clone(), up.clone());
        assert_eq!(rt.get(&o.id()).await.unwrap(), Some(o.clone()));
        assert_eq!(local.get(&o.id()).await.unwrap(), Some(o.clone()));
        assert_eq!(rt.get(&o.id()).await.unwrap(), Some(o));
        assert_eq!(up.ids(), 1);
    }

    #[tokio::test]
    async fn concurrent_misses_dedup() {
        let up = Arc::new(slow_upstream());
        let o = EncodedObject::new(ObjectKind::Blob, b"hot");
        up.inner().put(o.clone()).await.unwrap();
        let rt = Arc::new(ReadThrough::new(Arc::new(MemoryObjectStore::default()), up.clone()));
        let mut tasks = Vec::new();
        for _ in 0..20 {
            let rt = rt.clone();
            let id = o.id();
            tasks.push(tokio::spawn(async move { rt.get(&id).await }));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap().unwrap(), Some(o.clone()));
        }
        assert_eq!(up.ids(), 1);
    }

    #[tokio::test]
    async fn rejects_hash_mismatch() {
        let liar = EncodedObject::new(ObjectKind::Blob, b"not what you asked for");
        let rt = ReadThrough::new(Arc::new(MemoryObjectStore::default()), Arc::new(Lying(liar)));
        assert!(rt.get(&ObjectId([3; 32])).await.is_err());
    }

    /// A reader racing the evictor (files vanish between lookup and open,
    /// or mid-read) always gets correct bytes: a lost file is a miss and is
    /// refetched from upstream.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reader_racing_eviction_refetches() {
        let dir = tempfile::tempdir().unwrap();
        let disk = Arc::new(DiskObjectStore::open(dir.path(), u64::MAX).unwrap());
        let up = Arc::new(CountingStore::default());
        let objs: Vec<_> = (0..64)
            .map(|i| {
                let body: Vec<u8> =
                    (0..5000u32).map(|j| (j.wrapping_mul(i + 7) >> 3) as u8).collect();
                EncodedObject::new(ObjectKind::Blob, &body)
            })
            .collect();
        up.inner().put_many(&objs).await.unwrap();
        let rt = Arc::new(ReadThrough::over_disk(disk.clone(), up.clone()).verified_upstream());
        let ids: Vec<ObjectId> = objs.iter().map(|o| o.id()).collect();

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let evictor = {
            let (disk, stop) = (disk.clone(), stop.clone());
            tokio::spawn(async move {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    disk.evict_to(0).await.unwrap();
                    tokio::task::yield_now().await;
                }
            })
        };
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (rt, ids, objs) = (rt.clone(), ids.clone(), objs.clone());
                tokio::spawn(async move {
                    for _ in 0..40 {
                        let got = rt.get_many(&ids).await.unwrap();
                        assert_eq!(got.len(), objs.len());
                        for (g, o) in got.iter().zip(&objs) {
                            assert_eq!(g.as_ref(), Some(o));
                        }
                    }
                })
            })
            .collect();
        for r in readers {
            r.await.unwrap();
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        evictor.await.unwrap();
        assert!(up.ids() > objs.len(), "evicted objects were fetched again");
    }

    fn blobs(n: usize) -> Vec<EncodedObject> {
        (0..n)
            .map(|i| EncodedObject::new(ObjectKind::Blob, format!("blob number {i}").as_bytes()))
            .collect()
    }

    /// Processes sharing a cache directory (here: separate handles, as each
    /// process has its own) fetch each missing object from upstream once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn processes_sharing_a_cache_fetch_once() {
        let dir = tempfile::tempdir().unwrap();
        let up = Arc::new(CountingStore::default().with_delay(Duration::from_millis(100)));
        let objs = blobs(40);
        up.inner().put_many(&objs).await.unwrap();
        let ids: Vec<ObjectId> = objs.iter().map(|o| o.id()).collect();
        let handles: Vec<_> = (0..6)
            .map(|_| {
                let disk = Arc::new(DiskObjectStore::open(dir.path(), u64::MAX).unwrap());
                let rt = ReadThrough::over_disk(disk, up.clone()).verified_upstream();
                let ids = ids.clone();
                tokio::spawn(async move {
                    let got = rt.get_many(&ids).await.unwrap();
                    (got, rt.tier_counts())
                })
            })
            .collect();
        let mut waited = 0;
        for h in handles {
            let (got, counts) = h.await.unwrap();
            assert!(got.iter().zip(&objs).all(|(g, o)| g.as_ref() == Some(o)));
            waited += counts[1];
        }
        assert_eq!(up.ids(), objs.len(), "each object fetched once");
        assert!(waited > 0, "some handle waited for a peer");
        assert!(!dir.path().join("claims").read_dir().unwrap().any(|e| e.is_ok()), "claims gone");
    }

    /// Without the claims every process fetches for itself.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dedup_can_be_turned_off() {
        let dir = tempfile::tempdir().unwrap();
        let up = Arc::new(CountingStore::default().with_delay(Duration::from_millis(100)));
        let objs = blobs(10);
        up.inner().put_many(&objs).await.unwrap();
        let ids: Vec<ObjectId> = objs.iter().map(|o| o.id()).collect();
        let handles: Vec<_> = (0..3)
            .map(|_| {
                let disk = Arc::new(DiskObjectStore::open(dir.path(), u64::MAX).unwrap());
                let rt = ReadThrough::over_disk(disk, up.clone())
                    .verified_upstream()
                    .with_cross_process_dedup(false);
                let ids = ids.clone();
                tokio::spawn(async move { rt.get_many(&ids).await.unwrap() })
            })
            .collect();
        for h in handles {
            h.await.unwrap();
        }
        assert!(up.ids() > objs.len());
    }

    /// A claim left behind by a crashed process (a file nobody locks) and a
    /// claim whose holder never delivers (upstream lacks the object) do not
    /// block a reader: it fetches by itself.
    #[tokio::test]
    async fn abandoned_claims_do_not_block() {
        let dir = tempfile::tempdir().unwrap();
        let up = Arc::new(CountingStore::default());
        let o = EncodedObject::new(ObjectKind::Blob, b"after a crash");
        up.inner().put(o.clone()).await.unwrap();
        let disk = Arc::new(DiskObjectStore::open(dir.path(), u64::MAX).unwrap());
        std::fs::create_dir_all(dir.path().join("claims")).unwrap();
        std::fs::write(dir.path().join("claims").join(o.id().to_hex()), b"").unwrap();
        let rt = ReadThrough::over_disk(disk, up.clone()).verified_upstream();
        let t = std::time::Instant::now();
        assert_eq!(rt.get(&o.id()).await.unwrap(), Some(o));
        assert!(t.elapsed() < Duration::from_secs(5));
    }

    /// Counters record where objects came from and survive in the directory.
    #[tokio::test]
    async fn tier_counts_are_persisted() {
        let dir = tempfile::tempdir().unwrap();
        let up = Arc::new(CountingStore::default());
        let objs = blobs(5);
        up.inner().put_many(&objs).await.unwrap();
        let ids: Vec<ObjectId> = objs.iter().map(|o| o.id()).collect();
        {
            let disk = Arc::new(DiskObjectStore::open(dir.path(), u64::MAX).unwrap());
            let rt = ReadThrough::over_disk(disk, up.clone()).verified_upstream();
            rt.get_many(&ids).await.unwrap();
            rt.get_many(&ids).await.unwrap();
            let c = rt.tier_counts();
            assert_eq!((c[0], c[5], c[6]), (5, 1, 5), "{c:?}");
        }
        let totals = crate::tiers::read(dir.path());
        assert_eq!((totals[0], totals[6]), (5, 5), "{totals:?}");
    }
}
