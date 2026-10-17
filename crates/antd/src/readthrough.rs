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
//!   [`ReadThrough::get_trees_bundled_with_contents`] also hands the bundled
//!   objects to the caller (a checkout writes them out without reading them
//!   back).
//! - In-flight dedup: concurrent `get_many` calls that miss on the same id
//!   trigger only one upstream fetch for it (a map of shared futures).
//! - Cross-process dedup (`over_disk`, on by default;
//!   [`ReadThrough::with_cross_process_dedup`]): before fetching, a process
//!   claims each missing object (or tree, for bundles) with a lock file in
//!   the shared cache (`crate::claim`). Ids another process has claimed are
//!   fetched by that process: this one fetches and stores its own claims
//!   first, then waits (up to [`WAIT_FOR_PEER`]) for the others' and reads
//!   them from the cache, fetching itself whatever is still missing. Claims
//!   are only taken while another process is using the cache
//!   ([`Claims::peers_active`], checked once per batch): alone, a process
//!   fetches without per-object claim files, under one announcement of the
//!   whole batch ([`Claims::announce`]), which a process that starts
//!   meanwhile waits for instead of fetching the same ids again.
//! - Tier counters (`crate::tiers`) record how each object was served and are
//!   added to the cache directory's totals when the `ReadThrough` is dropped.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use ant_core::{EncodedObject, ObjectId};
use ant_store::{ObjectStore, Result, StoreError};
use async_trait::async_trait;
use futures::future::{BoxFuture, FutureExt, Shared};

use crate::claim::{Batch, Claim, Claims, Overlap};
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
            (Some(_), true) if self.claims.is_some() => self.claims.take(),
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

    /// Cross-process claims taken so far (0 without dedup, and while no
    /// other process uses the cache).
    pub fn claims_taken(&self) -> u64 {
        self.claims.as_ref().map_or(0, Claims::taken)
    }

    /// Batches announced so far (one per batch fetched without claims).
    pub fn batches_announced(&self) -> u64 {
        self.claims.as_ref().map_or(0, Claims::announced)
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
        Ok(self.get_trees_bundled_with_contents(trees).await?.0)
    }

    /// [`Self::get_trees_bundled`], also returning the other objects the
    /// bundles this call fetched delivered (blobs and subtrees, verified and
    /// already stored locally), so a walk can use them without reading them
    /// back from the cache.
    pub async fn get_trees_bundled_with_contents(
        &self,
        trees: &[ObjectId],
    ) -> Result<(Vec<Option<EncodedObject>>, Vec<(ObjectId, EncodedObject)>)> {
        let Some(remote) = self.bundles.as_ref().filter(|r| r.tree_bundles_enabled()) else {
            return Ok((self.get_many(trees).await?, Vec::new()));
        };
        let mut out = self.local.get_many(trees).await?;
        self.stats.add(&self.stats.local_hits, out.iter().flatten().count());
        let mut misses: Vec<ObjectId> =
            trees.iter().zip(&out).filter(|(_, o)| o.is_none()).map(|(id, _)| *id).collect();
        if misses.is_empty() {
            return Ok((out, Vec::new()));
        }
        misses.sort_unstable();
        misses.dedup();
        let (local, disk, stats) = (&self.local, &self.disk, &self.stats);
        let fetch_bundles = |ids: Vec<ObjectId>| async move {
            stats.add(&stats.bundle_requests, ids.len());
            let mut fetched: HashMap<ObjectId, EncodedObject> = HashMap::new();
            for bundle in remote.get_tree_bundles(&ids).await.into_iter().flatten() {
                fetched.extend(bundle);
            }
            stats.add(&stats.bundled_objects, fetched.len());
            // The bundles' own trees are stored last: a process that finds
            // one of them in the cache walks on below it without waiting,
            // so everything its bundle delivered must be there already.
            let wanted: HashSet<ObjectId> = ids.into_iter().collect();
            let (tops, below): (Vec<_>, Vec<_>) =
                fetched.into_iter().partition(|(id, _)| wanted.contains(id));
            for part in [&below, &tops] {
                if part.is_empty() {
                    continue;
                }
                match disk {
                    Some(disk) => disk.put_many_trusted(part.clone()).await?,
                    None => {
                        let objs: Vec<EncodedObject> =
                            part.iter().map(|(_, o)| o.clone()).collect();
                        local.put_many(&objs).await?
                    }
                }
            }
            let mut found = below;
            found.extend(tops);
            Ok(found)
        };
        // Trees another process is already fetching as bundles are waited
        // for after our own bundles are stored (never while holding claims).
        let fetched: HashMap<ObjectId, EncodedObject> = match &self.claims {
            Some(claims) => {
                let done =
                    coordinate(claims, local.as_ref(), misses.clone(), "b-", fetch_bundles).await?;
                let mut fetched: HashMap<ObjectId, EncodedObject> = done.got.into_iter().collect();
                // What peers fetched (or landed meanwhile) is in the cache now.
                let lacking: Vec<ObjectId> =
                    misses.iter().filter(|id| !fetched.contains_key(id)).copied().collect();
                if !lacking.is_empty() {
                    let got = local.get_many(&lacking).await?;
                    stats.add(&stats.waited_hits, got.iter().flatten().count());
                    fetched
                        .extend(lacking.into_iter().zip(got).filter_map(|(id, o)| Some((id, o?))));
                }
                fetched
            }
            None => fetch_bundles(misses).await?.into_iter().collect(),
        };
        let mut rest = Vec::new();
        for (slot, id) in out.iter_mut().zip(trees) {
            if slot.is_none() {
                *slot = fetched.get(id).cloned();
                if slot.is_none() {
                    rest.push(*id);
                }
            }
        }
        let requested: HashSet<&ObjectId> = trees.iter().collect();
        let extra: Vec<(ObjectId, EncodedObject)> =
            fetched.into_iter().filter(|(id, _)| !requested.contains(id)).collect();
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
        Ok((out, extra))
    }

    /// Declare that `upstream` already hash-verifies every object it returns
    /// (true of [`RemoteObjectStore`], also behind wrappers that merely
    /// forward), so fetched objects are not hashed a second time.
    pub fn verified_upstream(mut self) -> Self {
        self.trusted_upstream = true;
        self
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
        let fetch_own = |mine| Self::fetch_store(&local, &disk, trusted, &upstream, &stats, mine);
        let Some(claims) = claims else {
            out.extend(fetch_own(missing).await?);
            return Ok(out);
        };
        let done = coordinate(&claims, local.as_ref(), missing.clone(), "", fetch_own).await?;
        out.extend(done.got);
        // The rest was fetched by peers (or landed meanwhile): read it from
        // the cache, and fetch what is still missing (a peer gave up or
        // failed) ourselves. Ids we asked upstream for and did not get are
        // not asked for again.
        let lacking: Vec<ObjectId> = missing
            .into_iter()
            .filter(|id| !out.contains_key(id) && !done.asked.contains(id))
            .collect();
        if lacking.is_empty() {
            return Ok(out);
        }
        let mut still = Vec::new();
        let got = local.get_many(&lacking).await?;
        for (id, o) in lacking.into_iter().zip(got) {
            match o {
                Some(o) => {
                    stats.add(&stats.waited_hits, 1);
                    out.insert(id, o);
                }
                None => still.push(id),
            }
        }
        if !still.is_empty() {
            out.extend(Self::fetch_store(&local, &disk, trusted, &upstream, &stats, still).await?);
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

/// How one fetch of missing ids is split up (see [`plan`]).
#[derive(Default)]
struct Plan {
    /// Ids to fetch, with their claim (`None`: fetched without one).
    owned: Vec<(ObjectId, Option<Claim>)>,
    /// Ids another process has claimed.
    peers: Vec<ObjectId>,
    /// Ids in another handle's in-flight batch.
    deferred: Overlap,
    /// This handle's announcement of `owned` (fetching alone).
    batch: Option<Batch>,
}

impl Plan {
    fn unclaimed(ids: Vec<ObjectId>) -> Self {
        Self { owned: ids.into_iter().map(|id| (id, None)).collect(), ..Self::default() }
    }
}

/// Decide who fetches `ids` (see [`crate::claim`]). First round
/// (`announce`): announce the batch, then check for peers; alone, fetch it
/// all under that one announcement. With peers (and in later rounds), ids in
/// another handle's in-flight batch are deferred to it, the others claimed
/// one by one under `{prefix}{hex}` keys. Claiming that fails makes an id
/// ours to fetch.
async fn plan(claims: &Claims, ids: Vec<ObjectId>, prefix: &'static str, announce: bool) -> Plan {
    let claims = claims.clone();
    let work = ids.clone();
    let done = tokio::task::spawn_blocking(move || {
        let mut ids = work;
        let mut deferred = Overlap::default();
        if announce {
            match claims.announce(&ids) {
                Ok(batch) => {
                    if !claims.peers_active() {
                        return Plan { batch: Some(batch), ..Plan::unclaimed(ids) };
                    }
                    drop(batch);
                    deferred = claims.in_flight(&ids);
                    if !deferred.ids.is_empty() {
                        let skip: HashSet<ObjectId> = deferred.ids.iter().copied().collect();
                        ids.retain(|id| !skip.contains(id));
                    }
                }
                // Cannot announce (e.g. a read-only cache): claim per
                // object whenever someone else may be fetching.
                Err(_) if !claims.peers_active() => return Plan::unclaimed(ids),
                Err(_) => {}
            }
        }
        let mut owned = Vec::new();
        let mut peers = Vec::new();
        for id in ids {
            match claims.try_claim(&format!("{prefix}{}", id.to_hex())) {
                Ok(Some(c)) => owned.push((id, Some(c))),
                Ok(None) => peers.push(id),
                Err(_) => owned.push((id, None)),
            }
        }
        Plan { owned, peers, deferred, batch: None }
    })
    .await;
    // A panic in the blocking pool is not worth failing a read: fetch all.
    done.unwrap_or_else(|_| Plan::unclaimed(ids))
}

/// What [`coordinate`] fetched itself.
struct Coordinated {
    /// Objects this handle fetched (and stored).
    got: Vec<(ObjectId, EncodedObject)>,
    /// Every id this handle asked upstream for.
    asked: HashSet<ObjectId>,
}

/// Fetch `ids` (missing locally) with `fetch_own`, coordinated with the
/// other processes on the cache: [`plan`], fetch and store what is ours
/// (holding its claims or announcement, never waiting meanwhile), release,
/// then wait (bounded by [`WAIT_FOR_PEER`] overall) for the batches and
/// claims of others; ids whose batch ended without them go through a claim
/// round of their own. The caller reads what others fetched from the cache.
async fn coordinate<F, Fut>(
    claims: &Claims,
    local: &dyn ObjectStore,
    ids: Vec<ObjectId>,
    prefix: &'static str,
    mut fetch_own: F,
) -> Result<Coordinated>
where
    F: FnMut(Vec<ObjectId>) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<(ObjectId, EncodedObject)>>>,
{
    let deadline = Instant::now() + WAIT_FOR_PEER;
    let mut done = Coordinated { got: Vec::new(), asked: HashSet::new() };
    let Plan { owned, mut peers, deferred, batch } = plan(claims, ids, prefix, true).await;
    fetch_owned(local, owned, &mut fetch_own, &mut done).await?;
    drop(batch);
    if !deferred.ids.is_empty() {
        let ids = deferred.ids.clone();
        let c = claims.clone();
        let left = deadline.saturating_duration_since(Instant::now());
        let _ = tokio::task::spawn_blocking(move || c.wait_batches(deferred, left)).await;
        let present = local.has_many(&ids).await?;
        let still: Vec<ObjectId> =
            ids.into_iter().zip(present).filter(|(_, p)| !*p).map(|(id, _)| id).collect();
        if !still.is_empty() {
            let again = plan(claims, still, prefix, false).await;
            fetch_owned(local, again.owned, &mut fetch_own, &mut done).await?;
            peers.extend(again.peers);
        }
    }
    if !peers.is_empty() {
        wait_ids(claims.clone(), peers, prefix, deadline).await;
    }
    Ok(done)
}

/// Fetch the owned ids not in the local store after all (a previous holder
/// finished between our miss and our claim), then release their claims.
async fn fetch_owned<F, Fut>(
    local: &dyn ObjectStore,
    owned: Vec<(ObjectId, Option<Claim>)>,
    fetch_own: &mut F,
    done: &mut Coordinated,
) -> Result<()>
where
    F: FnMut(Vec<ObjectId>) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<(ObjectId, EncodedObject)>>>,
{
    if owned.is_empty() {
        return Ok(());
    }
    let ids: Vec<ObjectId> = owned.iter().map(|(id, _)| *id).collect();
    let present = local.has_many(&ids).await?;
    let (mine, guards): (Vec<ObjectId>, Vec<Option<Claim>>) =
        owned.into_iter().zip(present).filter(|(_, p)| !*p).map(|(o, _)| o).unzip();
    if !mine.is_empty() {
        done.asked.extend(mine.iter().copied());
        done.got.extend(fetch_own(mine).await?);
    }
    drop(guards);
    Ok(())
}

/// Wait until every peer holding one of `ids` is done, or `deadline`.
async fn wait_ids(claims: Claims, ids: Vec<ObjectId>, prefix: &'static str, deadline: Instant) {
    let _ = tokio::task::spawn_blocking(move || {
        for id in ids {
            let left = deadline.saturating_duration_since(Instant::now());
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

    async fn get_dirs_with_contents(
        &self,
        trees: &[ObjectId],
    ) -> Result<(Vec<Option<EncodedObject>>, Vec<(ObjectId, EncodedObject)>)> {
        self.get_trees_bundled_with_contents(trees).await
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

        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let stop = Arc::new(AtomicBool::new(false));
        // Completed evictions: every reader keeps reading until at least two
        // full evictions have finished after its first read, so the race
        // this test is about always happens (fast readers on a slow, small
        // runner could otherwise finish before the evictor ran at all).
        let evictions = Arc::new(AtomicUsize::new(0));
        let evictor = {
            let (disk, stop, evictions) = (disk.clone(), stop.clone(), evictions.clone());
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    disk.evict_to(0).await.unwrap();
                    evictions.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
            })
        };
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (rt, ids, objs) = (rt.clone(), ids.clone(), objs.clone());
                let evictions = evictions.clone();
                tokio::spawn(async move {
                    let mut after_first = None;
                    let mut i = 0;
                    loop {
                        let before = evictions.load(Ordering::Relaxed);
                        let got = rt.get_many(&ids).await.unwrap();
                        assert_eq!(got.len(), objs.len());
                        for (g, o) in got.iter().zip(&objs) {
                            assert_eq!(g.as_ref(), Some(o));
                        }
                        let first =
                            *after_first.get_or_insert_with(|| evictions.load(Ordering::Relaxed));
                        i += 1;
                        // Eviction `first + 2` started after `first + 1` ended, so
                        // wholly after the first read; this read started after it
                        // finished, so it had to fetch again.
                        if i >= 40 && before >= first + 2 {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
            })
            .collect();
        for r in readers {
            r.await.unwrap();
        }
        stop.store(true, Ordering::Relaxed);
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

    /// A process alone on its cache takes no claims (no per-object claim
    /// files: they made a 96k-file cold checkout ~2 s slower); once a second
    /// handle is alive, both claim again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_process_alone_takes_no_claims() {
        let dir = tempfile::tempdir().unwrap();
        let up = Arc::new(CountingStore::default());
        let objs = blobs(300);
        up.inner().put_many(&objs).await.unwrap();
        let ids: Vec<ObjectId> = objs.iter().map(|o| o.id()).collect();
        let open = || {
            let disk = Arc::new(DiskObjectStore::open(dir.path(), u64::MAX).unwrap());
            ReadThrough::over_disk(disk, up.clone())
                .with_cross_process_dedup(true)
                .verified_upstream()
        };
        let rt = open();
        for chunk in ids[..200].chunks(50) {
            let got = rt.get_many(chunk).await.unwrap();
            assert!(got.iter().all(Option::is_some));
        }
        assert_eq!(rt.claims_taken(), 0, "alone: no claims");
        assert!(!dir.path().join("claims").exists(), "no claim files were created");
        assert_eq!(rt.batches_announced(), 4, "one announcement per batch, not per object");
        assert_eq!(up.ids(), 200);

        let peer = open();
        rt.get_many(&ids[200..250]).await.unwrap();
        assert_eq!(rt.claims_taken(), 50, "with a live peer every miss is claimed");
        drop(peer);
        rt.get_many(&ids[250..]).await.unwrap();
        assert_eq!(rt.claims_taken(), 50, "alone again");
        assert_eq!(rt.batches_announced(), 6);
        assert_eq!(up.ids(), 300);
        let left = dir.path().join("batches").read_dir().unwrap().flatten().count();
        assert_eq!(left, 0, "announcements are removed after their batch");
    }

    /// Upstream that answers only once the gate is opened.
    struct Gated {
        up: Arc<CountingStore>,
        open: tokio::sync::watch::Receiver<bool>,
    }

    #[async_trait]
    impl ObjectStore for Gated {
        async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
            let _ = self.open.clone().wait_for(|o| *o).await;
            self.up.get_many(ids).await
        }
        async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
            self.up.put_many(objects).await
        }
        async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
            self.up.has_many(ids).await
        }
    }

    /// A process that starts while another one fetches a batch alone (no
    /// per-object claims) waits for that batch instead of fetching the
    /// overlapping ids again; ids outside it it fetches itself, once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_peer_arriving_mid_batch_waits_for_it() {
        let dir = tempfile::tempdir().unwrap();
        let up = Arc::new(CountingStore::default());
        let objs = blobs(90);
        up.inner().put_many(&objs).await.unwrap();
        let ids: Vec<ObjectId> = objs.iter().map(|o| o.id()).collect();
        let (gate, open) = tokio::sync::watch::channel(false);
        let gated: Arc<dyn ObjectStore> = Arc::new(Gated { up: up.clone(), open });
        let rt = |dir: &std::path::Path| {
            let disk = Arc::new(DiskObjectStore::open(dir, u64::MAX).unwrap());
            Arc::new(ReadThrough::over_disk(disk, gated.clone()).verified_upstream())
        };
        let first = rt(dir.path());
        let a = {
            let (first, ids) = (first.clone(), ids[..60].to_vec());
            tokio::spawn(async move { first.get_many(&ids).await.unwrap() })
        };
        // The first one is fetching (held at the gate) under its announcement.
        let batches = dir.path().join("batches");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !batches.read_dir().is_ok_and(|mut d| d.next().is_some()) {
            assert!(std::time::Instant::now() < deadline, "no announcement");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(first.claims_taken(), 0, "it was alone when it started");
        let second = rt(dir.path());
        let b = {
            let (second, ids) = (second.clone(), ids[30..].to_vec());
            tokio::spawn(async move { second.get_many(&ids).await.unwrap() })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        gate.send(true).unwrap();
        let (got_a, got_b) = (a.await.unwrap(), b.await.unwrap());
        assert!(got_a.iter().zip(&objs[..60]).all(|(g, o)| g.as_ref() == Some(o)));
        assert!(got_b.iter().zip(&objs[30..]).all(|(g, o)| g.as_ref() == Some(o)));
        assert_eq!(up.ids(), 90, "every object fetched once: {:?}", up.reads());
        let c = second.tier_counts();
        assert_eq!(c[6], 30, "the second fetched only its own ids: {c:?}");
    }

    /// Handles that start at the same moment see each other: every object
    /// is fetched once, whether they were registered before either fetched
    /// (even rounds) or each registers and fetches at once (odd rounds, a
    /// few ms apart: the first may start alone and the others join its
    /// batch).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn simultaneous_starters_fetch_once() {
        for round in 0..40 {
            let dir = tempfile::tempdir().unwrap();
            let up = Arc::new(CountingStore::default().with_delay(Duration::from_millis(20)));
            let objs = blobs(50 + round);
            up.inner().put_many(&objs).await.unwrap();
            let ids: Vec<ObjectId> = objs.iter().map(|o| o.id()).collect();
            let open = {
                let (root, up) = (dir.path().to_path_buf(), up.clone());
                move || {
                    let disk = Arc::new(DiskObjectStore::open(&root, u64::MAX).unwrap());
                    ReadThrough::over_disk(disk, up.clone()).verified_upstream()
                }
            };
            let early: Vec<_> = (0..3).map(|_| (round % 2 == 0).then(&open)).collect();
            let start = Arc::new(tokio::sync::Barrier::new(early.len()));
            let tasks: Vec<_> = early
                .into_iter()
                .enumerate()
                .map(|(i, rt)| {
                    let (ids, start, open) = (ids.clone(), start.clone(), open.clone());
                    // Late starters arrive 0-12 ms apart: often while an
                    // earlier one fetches (20 ms) under its announcement.
                    let stagger = Duration::from_millis((i * (round % 7)) as u64);
                    tokio::spawn(async move {
                        start.wait().await;
                        if rt.is_none() {
                            tokio::time::sleep(stagger).await;
                        }
                        let rt = rt.unwrap_or_else(open);
                        rt.get_many(&ids).await.unwrap()
                    })
                })
                .collect();
            for t in tasks {
                let got = t.await.unwrap();
                assert!(got.iter().zip(&objs).all(|(g, o)| g.as_ref() == Some(o)));
            }
            assert_eq!(up.ids(), objs.len(), "round {round}: {:?}", up.reads());
        }
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
