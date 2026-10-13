//! Shared helpers: latency stats, process metrics, and store-timing wrappers.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ant_core::{EncodedObject, ObjectId};
use ant_store::{Backend, CasOutcome, CommitNode, MetaStore, ObjectStore, PathChange, RefStore, Result};
use async_trait::async_trait;
use bytes::Bytes;

use crate::json::J;

// ---------------------------------------------------------------------------
// Latency statistics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct Lat {
    pub n: usize,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

impl Lat {
    /// `micros` is consumed (sorted in place).
    pub fn from_micros(mut micros: Vec<u64>) -> Self {
        if micros.is_empty() {
            return Self::default();
        }
        micros.sort_unstable();
        let pct = |p: f64| {
            let rank = ((p / 100.0) * micros.len() as f64).ceil() as usize;
            micros[rank.clamp(1, micros.len()) - 1] as f64 / 1000.0
        };
        let sum: u64 = micros.iter().sum();
        Self {
            n: micros.len(),
            mean_ms: sum as f64 / micros.len() as f64 / 1000.0,
            p50_ms: pct(50.0),
            p95_ms: pct(95.0),
            p99_ms: pct(99.0),
            max_ms: micros[micros.len() - 1] as f64 / 1000.0,
        }
    }

    pub fn json(&self) -> J {
        J::obj()
            .with("n", self.n)
            .with("mean_ms", self.mean_ms)
            .with("p50_ms", self.p50_ms)
            .with("p95_ms", self.p95_ms)
            .with("p99_ms", self.p99_ms)
            .with("max_ms", self.max_ms)
    }

    pub fn line(&self, name: &str) -> String {
        format!(
            "  {name:<10} n={:<7} mean={:>8.2}ms p50={:>8.2}ms p95={:>8.2}ms p99={:>8.2}ms max={:>9.2}ms",
            self.n, self.mean_ms, self.p50_ms, self.p95_ms, self.p99_ms, self.max_ms
        )
    }
}

// ---------------------------------------------------------------------------
// Process metrics
// ---------------------------------------------------------------------------

/// Peak resident set size in bytes (`VmHWM`), if available.
pub fn peak_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// (user, system) CPU seconds consumed by this process so far.
pub fn cpu_seconds() -> Option<(f64, f64)> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Fields after the parenthesised comm; utime/stime are fields 14/15 overall.
    let rest = &stat[stat.rfind(')')? + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    let utime: f64 = f.get(11)?.parse().ok()?;
    let stime: f64 = f.get(12)?.parse().ok()?;
    Some((utime / 100.0, stime / 100.0))
}

pub fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

// ---------------------------------------------------------------------------
// Timing wrappers
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct Counter {
    pub calls: AtomicU64,
    pub nanos: AtomicU64,
    pub items: AtomicU64,
    pub bytes: AtomicU64,
    pub max_nanos: AtomicU64,
}

impl Counter {
    pub fn record(&self, start: Instant, items: u64, bytes: u64) {
        let n = start.elapsed().as_nanos() as u64;
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.nanos.fetch_add(n, Ordering::Relaxed);
        self.items.fetch_add(items, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.max_nanos.fetch_max(n, Ordering::Relaxed);
    }

    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }
    pub fn items(&self) -> u64 {
        self.items.load(Ordering::Relaxed)
    }
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
    pub fn secs(&self) -> f64 {
        self.nanos.load(Ordering::Relaxed) as f64 / 1e9
    }

    pub fn json(&self) -> J {
        let calls = self.calls();
        J::obj()
            .with("calls", calls)
            .with("total_s", self.secs())
            .with("mean_ms", if calls == 0 { 0.0 } else { self.secs() * 1000.0 / calls as f64 })
            .with("max_ms", self.max_nanos.load(Ordering::Relaxed) as f64 / 1e6)
            .with("items", self.items())
            .with("bytes", self.bytes())
    }
}

/// Registry of named counters (shared between the wrappers and the reporter).
#[derive(Default, Clone)]
pub struct Metrics {
    map: Arc<Mutex<BTreeMap<String, Arc<Counter>>>>,
}

impl Metrics {
    pub fn counter(&self, name: &str) -> Arc<Counter> {
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(name.to_string()).or_default().clone()
    }

    fn snapshot(&self) -> Vec<(String, Arc<Counter>)> {
        let map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// (name, calls, seconds, items) for every counter that has been used.
    pub fn totals(&self) -> Vec<(String, u64, f64, u64)> {
        self.snapshot()
            .into_iter()
            .filter(|(_, c)| c.calls() > 0)
            .map(|(k, c)| (k, c.calls(), c.secs(), c.items()))
            .collect()
    }

    pub fn json(&self) -> J {
        self.snapshot().into_iter().fold(J::obj(), |o, (k, c)| o.with(&k, c.json()))
    }

    pub fn print(&self, title: &str) {
        println!("{title}");
        println!("  {:<24} {:>9} {:>10} {:>9} {:>10} {:>10}", "op", "calls", "total_s", "mean_ms", "max_ms", "items");
        for (name, c) in self.snapshot() {
            let calls = c.calls();
            let mean = if calls == 0 { 0.0 } else { c.secs() * 1000.0 / calls as f64 };
            println!(
                "  {name:<24} {calls:>9} {:>10.2} {mean:>9.2} {:>10.2} {:>10}",
                c.secs(),
                c.max_nanos.load(Ordering::Relaxed) as f64 / 1e6,
                c.items()
            );
        }
    }
}

pub async fn timed<T>(c: &Counter, items: u64, fut: impl Future<Output = T>) -> T {
    let start = Instant::now();
    let out = fut.await;
    c.record(start, items, 0);
    out
}

pub struct TimedObjects {
    inner: Arc<dyn ObjectStore>,
    get: Arc<Counter>,
    put: Arc<Counter>,
    has: Arc<Counter>,
}

impl TimedObjects {
    pub fn new(inner: Arc<dyn ObjectStore>, m: &Metrics, prefix: &str) -> Self {
        Self {
            inner,
            get: m.counter(&format!("{prefix}.get_many")),
            put: m.counter(&format!("{prefix}.put_many")),
            has: m.counter(&format!("{prefix}.has_many")),
        }
    }
}

#[async_trait]
impl ObjectStore for TimedObjects {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        let start = Instant::now();
        let out = self.inner.get_many(ids).await;
        let bytes = out
            .as_ref()
            .map(|v| v.iter().flatten().map(|o| o.as_bytes().len() as u64).sum())
            .unwrap_or(0);
        self.get.record(start, ids.len() as u64, bytes);
        out
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        let start = Instant::now();
        let out = self.inner.put_many(objects).await;
        let bytes = objects.iter().map(|o| o.as_bytes().len() as u64).sum();
        self.put.record(start, objects.len() as u64, bytes);
        out
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        timed(&self.has, ids.len() as u64, self.inner.has_many(ids)).await
    }
}

pub struct TimedRefs {
    inner: Arc<dyn RefStore>,
    get: Arc<Counter>,
    cas: Arc<Counter>,
    cas_mismatch: Arc<Counter>,
    list: Arc<Counter>,
}

#[async_trait]
impl RefStore for TimedRefs {
    async fn get_ref(&self, name: &str) -> Result<Option<ObjectId>> {
        timed(&self.get, 1, self.inner.get_ref(name)).await
    }

    async fn cas_ref(&self, name: &str, expected: Option<ObjectId>, new: Option<ObjectId>) -> Result<CasOutcome> {
        let start = Instant::now();
        let out = self.inner.cas_ref(name, expected, new).await;
        self.cas.record(start, 1, 0);
        if matches!(out, Ok(CasOutcome::Mismatch { .. })) {
            self.cas_mismatch.record(start, 1, 0);
        }
        out
    }

    async fn list_refs(&self, prefix: &str) -> Result<Vec<(String, ObjectId)>> {
        timed(&self.list, 1, self.inner.list_refs(prefix)).await
    }
}

pub struct TimedMeta {
    inner: Arc<dyn MetaStore>,
    put_commit: Arc<Counter>,
    put_commits: Arc<Counter>,
    put_derived_many: Arc<Counter>,
    get_commits: Arc<Counter>,
    get_changeset: Arc<Counter>,
    get_changesets: Arc<Counter>,
    first_parent_chain: Arc<Counter>,
    tail: Arc<Counter>,
    get_derived: Arc<Counter>,
    put_derived: Arc<Counter>,
}

#[async_trait]
impl MetaStore for TimedMeta {
    async fn put_commit(&self, node: CommitNode, changes: Vec<PathChange>) -> Result<u64> {
        let n = changes.len() as u64;
        timed(&self.put_commit, n, self.inner.put_commit(node, changes)).await
    }

    async fn put_commits(&self, commits: Vec<(CommitNode, Vec<PathChange>)>) -> Result<Vec<u64>> {
        let n = commits.len() as u64;
        timed(&self.put_commits, n, self.inner.put_commits(commits)).await
    }

    async fn put_derived_many(&self, kind: &str, entries: Vec<(Vec<u8>, Bytes)>) -> Result<()> {
        let n = entries.len() as u64;
        timed(&self.put_derived_many, n, self.inner.put_derived_many(kind, entries)).await
    }

    async fn get_commits(&self, ids: &[ObjectId]) -> Result<Vec<Option<CommitNode>>> {
        timed(&self.get_commits, ids.len() as u64, self.inner.get_commits(ids)).await
    }

    async fn get_changeset(&self, id: &ObjectId) -> Result<Option<Vec<PathChange>>> {
        timed(&self.get_changeset, 1, self.inner.get_changeset(id)).await
    }

    // Forward the batched variants: the trait defaults would turn one store
    // round trip into one per id and distort the measured server load.
    async fn get_changesets(&self, ids: &[ObjectId]) -> Result<Vec<Option<Vec<PathChange>>>> {
        timed(&self.get_changesets, ids.len() as u64, self.inner.get_changesets(ids)).await
    }

    async fn first_parent_chain(
        &self,
        tip: ObjectId,
        stop: Option<ObjectId>,
        limit: usize,
    ) -> Result<Vec<CommitNode>> {
        timed(&self.first_parent_chain, 1, self.inner.first_parent_chain(tip, stop, limit)).await
    }

    async fn tail(&self, after: u64, limit: usize) -> Result<Vec<(u64, ObjectId)>> {
        timed(&self.tail, 1, self.inner.tail(after, limit)).await
    }

    async fn get_derived(&self, kind: &str, key: &[u8]) -> Result<Option<Bytes>> {
        timed(&self.get_derived, 1, self.inner.get_derived(kind, key)).await
    }

    async fn put_derived(&self, kind: &str, key: &[u8], value: Bytes) -> Result<()> {
        timed(&self.put_derived, 1, self.inner.put_derived(kind, key, value)).await
    }

    async fn get_derived_many(&self, kind: &str, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>> {
        timed(&self.get_derived, keys.len() as u64, self.inner.get_derived_many(kind, keys)).await
    }

    // History-index calls are passed through untimed.
    async fn index_history(&self, ref_name: &str, rows: &[ant_store::HistoryRow]) -> Result<()> {
        self.inner.index_history(ref_name, rows).await
    }

    async fn query_history(
        &self,
        ref_name: &str,
        prefix: &ant_core::RepoPath,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<(u64, ObjectId)>> {
        self.inner.query_history(ref_name, prefix, max_generation, limit).await
    }

    async fn history_watermark(&self, ref_name: &str) -> Result<Option<ObjectId>> {
        self.inner.history_watermark(ref_name).await
    }

    async fn set_history_watermark(&self, ref_name: &str, expected: Option<ObjectId>, new: ObjectId) -> Result<bool> {
        self.inner.set_history_watermark(ref_name, expected, new).await
    }

    async fn clear_history(&self, ref_name: &str) -> Result<()> {
        self.inner.clear_history(ref_name).await
    }
}

/// Wrap every store of `backend` so each call is timed under `prefix.*`.
pub fn instrument(backend: &Backend, m: &Metrics, prefix: &str) -> Backend {
    let c = |n: &str| m.counter(&format!("{prefix}.{n}"));
    Backend {
        objects: Arc::new(TimedObjects::new(backend.objects.clone(), m, prefix)),
        refs: Arc::new(TimedRefs {
            inner: backend.refs.clone(),
            get: c("get_ref"),
            cas: c("cas_ref"),
            cas_mismatch: c("cas_ref.mismatch"),
            list: c("list_refs"),
        }),
        meta: Arc::new(TimedMeta {
            inner: backend.meta.clone(),
            put_commit: c("put_commit"),
            put_commits: c("put_commits"),
            put_derived_many: c("put_derived_many"),
            get_commits: c("get_commits"),
            get_changeset: c("get_changeset"),
            get_changesets: c("get_changesets"),
            first_parent_chain: c("first_parent_chain"),
            tail: c("tail"),
            get_derived: c("get_derived"),
            put_derived: c("put_derived"),
        }),
    }
}

/// Counting/timing wrapper for any `ObjectStore` (used on the checkout path).
pub fn timed_store(inner: Arc<dyn ObjectStore>, m: &Metrics, prefix: &str) -> Arc<dyn ObjectStore> {
    Arc::new(TimedObjects::new(inner, m, prefix))
}
