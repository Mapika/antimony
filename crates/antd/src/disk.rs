//! On-disk content-addressed object cache.
//!
//! - Layout: `{root}/objects/{hex[0..2]}/{hex}`; file content is the
//!   zstd-compressed (level 1) encoded object. Writes go to a `.tmp.*` file
//!   in the same shard directory (a same-directory `rename` is cheap and
//!   parallel; one across directories serializes on a filesystem-wide lock)
//!   and are `rename`d into place, never written at the final path. Readers
//!   and `scan` ignore `.tmp.*`; `{root}/tmp` is kept for older clients. If
//!   the final path already exists, the write is skipped.
//! - `get_many`: missing file -> `None`. A corrupt file (bad zstd / framing)
//!   is deleted and reported as `None` (it's a cache).
//! - Filesystem work runs in `spawn_blocking`, one blocking task per batch.
//!   Inside a batch, objects are read/written by several threads
//!   ([`crate::par::par_map`]); zstd contexts are thread-local and reused. Objects are
//!   compressed with the content size in the frame header so reads allocate
//!   once (older entries without it still decode).
//! - Format version: `{root}/format` holds [`CACHE_FORMAT`]. `open` writes
//!   it when missing (a cache from before the marker is version 1) and
//!   refuses a cache whose version is newer (`ant_core::NewerFormat`).
//!
//! # Size budget and eviction
//!
//! The budget (`max_bytes`) is enforced automatically.
//!
//! **Policy**: least recently used first, by file mtime. A hit bumps the
//! mtime, rate-limited to once per `touch_interval` per object (default 60 s)
//! so a hot object costs one metadata write a minute, not one per read;
//! storing an object that already exists counts as a use too. Eviction
//! removes the oldest files until the cache is at most 90% of the budget.
//!
//! **Size tracking** never stats the cache per write. Each handle counts the
//! bytes it newly writes; once it holds `max_bytes/64` (at most 8 MiB) it adds
//! them to a shared counter, `{root}/gc/usage` (`"<bytes> <unix-secs>\n"`,
//! updated under the short flock `{root}/gc/usage.lock`); a handle's last
//! bytes are flushed when it is dropped. A full scan runs only when the
//! counter says the cache is over budget, is missing (first use, or a cache
//! from an older binary), or was last reconciled over six hours ago (to pick
//! up writers that do not count and manual deletions). Older binaries only
//! look at `objects/`, `tmp/` and `format`, so `gc/` is invisible to them.
//!
//! **Locking**: many processes share a cache, so eviction holds an flock on
//! `{root}/gc/evict.lock`. Automatic eviction uses `try_lock` and skips when
//! another process holds it (it is already evicting); only an explicit
//! [`DiskObjectStore::gc`] / [`DiskObjectStore::evict_to`] waits.
//!
//! **Safety**: the evictor re-checks each victim's mtime just before removing
//! it (an object read since the scan survives) and also removes `.tmp.*`
//! files abandoned for over an hour. In-progress writes are never candidates:
//! they carry `.tmp.*` names until the final `rename`. A reader that loses an
//! object to eviction sees an ordinary miss (a file already opened stays
//! readable after unlink) and [`crate::readthrough::ReadThrough`] refetches it.
//!
//! Enforcement is approximate: the cache can exceed the budget by about
//! `max_bytes/64` per concurrent writer plus the batch in flight (accounting
//! happens after each batch). Budgets of `u64::MAX/4` or more disable it.
//! Long-running daemons call [`DiskObjectStore::spawn_maintenance`] so that
//! counters are flushed and the budget enforced periodically even when idle.

use std::cell::RefCell;
use std::fs::{File, TryLockError};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ant_core::{EncodedObject, ObjectId};
use ant_store::{ObjectStore, Result, StoreError};
use async_trait::async_trait;
use bytes::Bytes;

use crate::par::par_map;

/// Prefix of in-progress writes inside a shard directory.
const TMP_PREFIX: &str = ".tmp.";

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Largest decompressed size trusted from a frame header (guards allocation
/// against a corrupt header; larger objects take the streaming path).
const MAX_PREALLOC: u64 = 1 << 30;

/// Default for [`DiskObjectStore::with_touch_interval`].
pub const DEFAULT_TOUCH_INTERVAL: Duration = Duration::from_secs(60);
/// Directory (under the cache root) with the size counter and the locks.
const GC_DIR: &str = "gc";
/// An in-progress write untouched for this long was abandoned by a crash.
const STALE_TMP: Duration = Duration::from_secs(3600);
/// The size counter is reconciled with a real scan at least this often.
const RESCAN_AFTER: Duration = Duration::from_secs(6 * 3600);
/// Upper bound of the per-handle write count before it is published.
const MAX_FLUSH_BYTES: u64 = 8 << 20;
/// Budgets at or above this mean "unlimited": no accounting at all.
const UNLIMITED: u64 = u64::MAX / 4;

thread_local! {
    static DECOMPRESSOR: RefCell<Option<zstd::bulk::Decompressor<'static>>> =
        const { RefCell::new(None) };
    static COMPRESSOR: RefCell<Option<zstd::bulk::Compressor<'static>>> =
        const { RefCell::new(None) };
}

/// Version of the cache layout, recorded in `{root}/format` (`"1\n"`). A
/// cache without the file was written before it existed: version 1.
pub const CACHE_FORMAT: u64 = 1;
const FORMAT_FILE: &str = "format";

/// Refuse a cache written by a newer version; record the version in a cache
/// that has no marker yet.
fn check_cache_format(root: &Path) -> std::io::Result<()> {
    let path = root.join(FORMAT_FILE);
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            let found = ant_core::version::parse_format_marker(&text).ok_or_else(|| {
                std::io::Error::new(
                    ErrorKind::InvalidData,
                    format!("corrupt cache format marker {}: {text:?}", path.display()),
                )
            })?;
            ant_core::version::check_format(
                || format!("disk cache {} format", root.display()),
                found,
                CACHE_FORMAT,
            )
            .map_err(std::io::Error::other)
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            // Unique temp name: concurrent opens each rename a whole file.
            let tmp = root.join(format!(
                ".{FORMAT_FILE}{TMP_PREFIX}{}.{}",
                std::process::id(),
                TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::write(&tmp, format!("{CACHE_FORMAT}\n"))?;
            std::fs::rename(&tmp, &path).inspect_err(|_| {
                let _ = std::fs::remove_file(&tmp);
            })
        }
        Err(e) => Err(e),
    }
}

/// Outcome of an eviction pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EvictReport {
    /// Objects deleted.
    pub removed: u64,
    /// Bytes deleted (objects only).
    pub freed: u64,
    /// Objects left.
    pub objects: u64,
    /// Bytes left.
    pub bytes: u64,
}

/// Current contents of the cache (a full scan) and its budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub objects: u64,
    pub bytes: u64,
    pub budget: u64,
}

/// State shared with the handle's blocking tasks.
struct Shared {
    root: PathBuf,
    max_bytes: u64,
    /// Newly written bytes not yet added to the shared counter.
    pending: AtomicU64,
    /// Publish `pending` once it reaches this.
    flush_at: u64,
}

pub struct DiskObjectStore {
    shared: Arc<Shared>,
    touch_interval: Duration,
}

impl DiskObjectStore {
    /// Open (creating directories and the format marker as needed). A cache
    /// written by a newer version is refused: an `io::Error` wrapping
    /// [`ant_core::NewerFormat`].
    pub fn open(root: impl AsRef<Path>, max_bytes: u64) -> std::io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(root.join("objects"))?;
        std::fs::create_dir_all(root.join("tmp"))?;
        std::fs::create_dir_all(root.join(GC_DIR))?;
        check_cache_format(&root)?;
        crate::claim::sweep(&root);
        let flush_at = (max_bytes / 64).clamp(1, MAX_FLUSH_BYTES);
        Ok(Self {
            shared: Arc::new(Shared { root, max_bytes, pending: AtomicU64::new(0), flush_at }),
            touch_interval: DEFAULT_TOUCH_INTERVAL,
        })
    }

    /// The size budget in bytes (`u64::MAX / 4` and above: unlimited).
    pub fn budget(&self) -> u64 {
        self.shared.max_bytes
    }

    /// The cache directory.
    pub fn root(&self) -> &Path {
        &self.shared.root
    }

    /// Bytes in the cache according to the shared counter (no scan), if a
    /// counter exists and the budget is enforced.
    pub fn tracked_usage(&self) -> Option<u64> {
        let pending = self.shared.pending.load(Ordering::Relaxed);
        self.shared.read_usage().map(|u| u.bytes.saturating_add(pending))
    }

    /// Bump an object's mtime on a hit only if it is older than this (zero:
    /// always). Default [`DEFAULT_TOUCH_INTERVAL`].
    pub fn with_touch_interval(mut self, interval: Duration) -> Self {
        self.touch_interval = interval;
        self
    }

    /// Store objects whose ids the caller has already established (e.g.
    /// verified on fetch), skipping the re-hash `put_many` would do. Ids that
    /// do not match their object poison the cache; callers must have checked.
    pub async fn put_many_trusted(&self, objects: Vec<(ObjectId, EncodedObject)>) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }
        self.write_batch(objects, |w, (id, o)| write_one(w, id, o)).await
    }

    /// Write `items` with `write` on the blocking pool, several at a time,
    /// then account for the newly written bytes (which may trigger eviction).
    async fn write_batch<T: Send + Sync + 'static>(
        &self,
        items: Vec<T>,
        write: fn(&Writer<'_>, &T) -> std::io::Result<u64>,
    ) -> Result<()> {
        let shared = self.shared.clone();
        let touch = self.touch_interval;
        blocking(move || {
            let w = Writer { root: &shared.root, touch };
            let written = par_map(&items, 16, |item| write(&w, item))
                .into_iter()
                .sum::<std::io::Result<u64>>()?;
            shared.account(written);
            Ok(())
        })
        .await
        .map_err(StoreError::backend)
    }

    /// Total bytes on disk under `objects/`.
    pub async fn disk_usage(&self) -> std::io::Result<u64> {
        Ok(self.stats().await?.bytes)
    }

    /// Objects and bytes in the cache (a full scan) and the budget.
    pub async fn stats(&self) -> std::io::Result<CacheStats> {
        let (root, budget) = (self.shared.root.clone(), self.shared.max_bytes);
        blocking(move || {
            let files = scan(&root)?;
            Ok(CacheStats {
                objects: files.len() as u64,
                bytes: files.iter().map(|f| f.size).sum(),
                budget,
            })
        })
        .await
    }

    /// Evict least-recently-used objects if over budget (down to 90% of it),
    /// waiting for any other evictor to finish first. Returns bytes freed.
    pub async fn gc(&self) -> std::io::Result<u64> {
        Ok(self.evict_over_budget().await?.freed)
    }

    /// [`gc`](Self::gc) with the full report.
    pub async fn evict_over_budget(&self) -> std::io::Result<EvictReport> {
        let shared = self.shared.clone();
        blocking(move || shared.evict(Goal::Budget, true)).await.map(|r| r.unwrap_or_default())
    }

    /// Like [`gc`](Self::gc), but without waiting: `None` when another
    /// process or thread is evicting right now.
    pub async fn try_gc(&self) -> std::io::Result<Option<EvictReport>> {
        let shared = self.shared.clone();
        blocking(move || shared.evict(Goal::Budget, false)).await
    }

    /// Evict least-recently-used objects until at most `target` bytes remain,
    /// whatever the budget, waiting for other evictors.
    pub async fn evict_to(&self, target: u64) -> std::io::Result<EvictReport> {
        let shared = self.shared.clone();
        blocking(move || shared.evict(Goal::Bytes(target), true))
            .await
            .map(|r| r.unwrap_or_default())
    }

    /// Publish this handle's pending write count and enforce the budget if
    /// the shared counter says it is exceeded. For idle long-running
    /// processes; writes do this themselves.
    pub async fn maintain(&self) -> std::io::Result<()> {
        let shared = self.shared.clone();
        blocking(move || shared.flush_and_check()).await
    }

    /// Run [`maintain`](Self::maintain) every `every` until the store is
    /// dropped (the task holds only a weak reference). For FUSE daemons and
    /// other processes that live for days.
    pub fn spawn_maintenance(self: &Arc<Self>, every: Duration) -> tokio::task::JoinHandle<()> {
        let weak: Weak<Self> = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await; // the first tick is immediate
            loop {
                tick.tick().await;
                let Some(store) = weak.upgrade() else { return };
                if let Err(e) = store.maintain().await {
                    tracing::warn!("cache maintenance failed: {e}");
                }
            }
        })
    }
}

/// How far an eviction pass should go.
#[derive(Clone, Copy)]
enum Goal {
    /// If over budget, down to 90% of it; otherwise nothing.
    Budget,
    /// Down to this many bytes.
    Bytes(u64),
}

/// The shared counter: bytes in the cache as of `scanned`, plus what
/// writers added since.
struct Usage {
    bytes: u64,
    scanned: u64,
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

impl Shared {
    fn gc_path(&self, name: &str) -> PathBuf {
        self.root.join(GC_DIR).join(name)
    }

    fn open_lock(&self, name: &str) -> std::io::Result<File> {
        let path = self.gc_path(name);
        let open = || File::options().create(true).append(true).open(&path);
        match open() {
            Err(e) if e.kind() == ErrorKind::NotFound => {
                std::fs::create_dir_all(self.root.join(GC_DIR))?;
                open()
            }
            r => r,
        }
    }

    /// Count `written` new bytes; publish them (and enforce the budget) once
    /// enough have accumulated. Failures are logged: the cache is best-effort.
    fn account(&self, written: u64) {
        if self.max_bytes >= UNLIMITED || written == 0 {
            return;
        }
        let pending = self.pending.fetch_add(written, Ordering::Relaxed) + written;
        if pending >= self.flush_at
            && let Err(e) = self.flush_and_check()
        {
            tracing::warn!("cache size accounting failed: {e}");
        }
    }

    /// Add `pending` to the shared counter; evict if it exceeds the budget,
    /// is missing or is stale.
    fn flush_and_check(&self) -> std::io::Result<()> {
        if self.max_bytes >= UNLIMITED {
            return Ok(());
        }
        let add = self.pending.swap(0, Ordering::Relaxed);
        let usage = self.add_usage(add)?;
        let due = match usage {
            None => true,
            Some(u) => {
                u.bytes > self.max_bytes
                    || unix_now().saturating_sub(u.scanned) > RESCAN_AFTER.as_secs()
            }
        };
        if due {
            self.evict(Goal::Budget, false)?;
        }
        Ok(())
    }

    /// Add `add` to the counter under its lock; `None` if there is none yet.
    fn add_usage(&self, add: u64) -> std::io::Result<Option<Usage>> {
        let lock = self.open_lock("usage.lock")?;
        lock.lock()?;
        let Some(mut usage) = self.read_usage() else { return Ok(None) };
        usage.bytes = usage.bytes.saturating_add(add);
        self.write_usage(&usage)?;
        Ok(Some(usage))
    }

    fn read_usage(&self) -> Option<Usage> {
        let text = std::fs::read_to_string(self.gc_path("usage")).ok()?;
        let mut it = text.split_whitespace();
        Some(Usage { bytes: it.next()?.parse().ok()?, scanned: it.next()?.parse().ok()? })
    }

    /// Atomic replace; callers hold `usage.lock`.
    fn write_usage(&self, usage: &Usage) -> std::io::Result<()> {
        let tmp = self.gc_path(&format!(
            "usage{TMP_PREFIX}{}.{}",
            std::process::id(),
            TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&tmp, format!("{} {}\n", usage.bytes, usage.scanned))?;
        std::fs::rename(&tmp, self.gc_path("usage")).inspect_err(|_| {
            let _ = std::fs::remove_file(&tmp);
        })
    }

    /// Scan, evict as `goal` says and reconcile the counter, holding the
    /// eviction lock. `None` if `wait` is false and someone else holds it.
    fn evict(&self, goal: Goal, wait: bool) -> std::io::Result<Option<EvictReport>> {
        let lock = self.open_lock("evict.lock")?;
        if wait {
            lock.lock()?;
        } else {
            match lock.try_lock() {
                Ok(()) => {}
                Err(TryLockError::WouldBlock) => return Ok(None),
                Err(TryLockError::Error(e)) => return Err(e),
            }
        }
        // The scan sees everything written so far.
        self.pending.store(0, Ordering::Relaxed);
        let mut tmps = Vec::new();
        let mut files = scan_with(&self.root, |p, m| tmps.push((p, m)))?;
        let cutoff = SystemTime::now() - STALE_TMP;
        for (path, modified) in tmps {
            if modified < cutoff {
                let _ = std::fs::remove_file(path);
            }
        }
        let mut total: u64 = files.iter().map(|f| f.size).sum();
        let (trigger, target) = match goal {
            Goal::Budget => {
                let max = self.max_bytes;
                (max, max / 10 * 9 + (max % 10) * 9 / 10)
            }
            Goal::Bytes(t) => (t, t),
        };
        let mut report = EvictReport::default();
        let mut remaining = files.len() as u64;
        if total > trigger {
            files.sort_by_key(|f| f.modified);
            for f in &files {
                if total <= target {
                    break;
                }
                // Read since the scan: it is hot again, keep it.
                if std::fs::metadata(&f.path)
                    .and_then(|m| m.modified())
                    .is_ok_and(|m| m > f.modified)
                {
                    continue;
                }
                match std::fs::remove_file(&f.path) {
                    Ok(()) => report.removed += 1,
                    Err(e) if e.kind() == ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
                total -= f.size;
                remaining -= 1;
                report.freed += f.size;
            }
        }
        report.objects = remaining;
        report.bytes = total;
        let usage = self.open_lock("usage.lock")?;
        usage.lock()?;
        self.write_usage(&Usage { bytes: total, scanned: unix_now() })?;
        Ok(Some(report))
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        let add = *self.pending.get_mut();
        if add > 0 && self.max_bytes < UNLIMITED {
            let _ = self.add_usage(add);
        }
    }
}

/// What a blocking write task needs besides the object.
struct Writer<'a> {
    root: &'a Path,
    touch: Duration,
}

fn object_path(root: &Path, id: &ObjectId) -> PathBuf {
    let hex = id.to_hex();
    root.join("objects").join(&hex[..2]).join(hex)
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> std::io::Result<T> {
    tokio::task::spawn_blocking(f).await.map_err(std::io::Error::other)?
}

struct Cached {
    path: PathBuf,
    size: u64,
    modified: SystemTime,
}

/// Every cached object.
fn scan(root: &Path) -> std::io::Result<Vec<Cached>> {
    scan_with(root, |_, _| {})
}

/// Like [`scan`]; `on_tmp` sees every in-progress write (path, mtime).
fn scan_with(
    root: &Path,
    mut on_tmp: impl FnMut(PathBuf, SystemTime),
) -> std::io::Result<Vec<Cached>> {
    let mut out = Vec::new();
    for shard in std::fs::read_dir(root.join("objects"))? {
        let shard = shard?;
        if !shard.file_type()?.is_dir() {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(shard.path()) else { continue };
        for f in entries {
            let Ok(f) = f else { continue };
            // Racing deletions just shrink the result.
            let Ok(meta) = f.metadata() else { continue };
            if !meta.is_file() {
                continue;
            }
            let Ok(modified) = meta.modified() else { continue };
            if f.file_name().as_encoded_bytes().starts_with(TMP_PREFIX.as_bytes()) {
                on_tmp(f.path(), modified); // in-progress write
                continue;
            }
            out.push(Cached { path: f.path(), size: meta.len(), modified });
        }
    }
    Ok(out)
}

fn decompress(compressed: &[u8]) -> Option<Vec<u8>> {
    match zstd::zstd_safe::get_frame_content_size(compressed) {
        Ok(Some(n)) if n <= MAX_PREALLOC => DECOMPRESSOR.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                *slot = zstd::bulk::Decompressor::new().ok();
            }
            slot.as_mut()?.decompress(compressed, n as usize).ok()
        }),
        _ => zstd::decode_all(compressed).ok(),
    }
}

fn compress(raw: &[u8]) -> std::io::Result<Vec<u8>> {
    COMPRESSOR.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = Some(zstd::bulk::Compressor::new(1)?);
        }
        match slot.as_mut() {
            Some(c) => c.compress(raw),
            None => Err(std::io::Error::other("zstd compressor unavailable")),
        }
    })
}

/// Mark the object as used now, unless it already was within `interval`.
/// Failures (read-only cache, file evicted meanwhile) are ignored.
fn touch(file: &File, modified: Option<SystemTime>, interval: Duration) {
    let now = SystemTime::now();
    let fresh = modified.is_some_and(|m| now.duration_since(m).is_ok_and(|age| age < interval));
    if !fresh {
        let _ = file.set_modified(now);
    }
}

/// A missing, unreadable or corrupt file is a miss (corrupt ones are removed).
fn read_one(path: &Path, touch_interval: Duration) -> Option<EncodedObject> {
    let mut file = File::open(path).ok()?;
    let meta = file.metadata().ok();
    let mut compressed = Vec::new();
    if let Some(meta) = &meta {
        compressed.reserve_exact(meta.len() as usize);
    }
    file.read_to_end(&mut compressed).ok()?;
    let decoded =
        decompress(&compressed).and_then(|raw| EncodedObject::from_bytes(Bytes::from(raw)).ok());
    match decoded {
        Some(o) => {
            touch(&file, meta.and_then(|m| m.modified().ok()), touch_interval);
            Some(o)
        }
        None => {
            // Corrupt cache entry: drop it and report a miss.
            let _ = std::fs::remove_file(path);
            None
        }
    }
}

/// Write the object unless present; returns the bytes newly stored.
fn write_one(w: &Writer<'_>, id: &ObjectId, obj: &EncodedObject) -> std::io::Result<u64> {
    let (root, dest) = (w.root, object_path(w.root, id));
    if let Ok(meta) = std::fs::metadata(&dest) {
        // Storing an object again is a use: keep it from being evicted.
        if let Ok(f) = File::open(&dest) {
            touch(&f, meta.modified().ok(), w.touch);
        }
        return Ok(0);
    }
    let compressed = compress(&obj.as_bytes()[..])?;
    // Temp file beside its destination: a rename within one directory only
    // locks that directory, whereas a cross-directory rename takes a
    // filesystem-wide lock that serializes every concurrent writer.
    let tmp = dest.with_file_name(format!(
        "{TMP_PREFIX}{}.{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let write = |tmp: &Path| -> std::io::Result<()> {
        let mut f = File::create(tmp)?;
        f.write_all(&compressed)
    };
    let written = match write(&tmp) {
        Err(e) if e.kind() == ErrorKind::NotFound => {
            std::fs::create_dir_all(dest.parent().unwrap_or(root))?;
            write(&tmp)
        }
        r => r,
    };
    let renamed = written.and_then(|()| std::fs::rename(&tmp, &dest));
    if renamed.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    renamed.map(|()| compressed.len() as u64)
}

#[async_trait]
impl ObjectStore for DiskObjectStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let root = self.shared.root.clone();
        let touch = self.touch_interval;
        let ids = ids.to_vec();
        blocking(move || Ok(par_map(&ids, 32, |id| read_one(&object_path(&root, id), touch))))
            .await
            .map_err(StoreError::backend)
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }
        self.write_batch(objects.to_vec(), |w, o| write_one(w, &o.id(), o)).await
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        let root = self.shared.root.clone();
        let ids = ids.to_vec();
        blocking(move || Ok(par_map(&ids, 256, |id| object_path(&root, id).exists())))
            .await
            .map_err(StoreError::backend)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use ant_core::ObjectKind;
    use ant_store::{ObjectStoreExt, conformance};

    #[tokio::test]
    async fn disk_store_conforms() {
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        conformance::object_store(Arc::new(s)).await;
    }

    #[tokio::test]
    async fn layout_and_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let o = EncodedObject::new(ObjectKind::Blob, b"persist me");
        {
            let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
            s.put(o.clone()).await.unwrap();
            let p = object_path(dir.path(), &o.id());
            assert!(p.starts_with(dir.path().join("objects")));
            assert!(p.ends_with(format!("{}/{}", &o.id().to_hex()[..2], o.id().to_hex())));
            assert!(p.exists());
        }
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        assert_eq!(s.get(&o.id()).await.unwrap(), Some(o));
    }

    #[tokio::test]
    async fn format_marker_is_written_and_newer_caches_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        // A cache written before the marker existed: objects, no `format`.
        let o = EncodedObject::new(ObjectKind::Blob, b"old cache");
        DiskObjectStore::open(dir.path(), u64::MAX).unwrap().put(o.clone()).await.unwrap();
        std::fs::remove_file(dir.path().join("format")).unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        assert_eq!(s.get(&o.id()).await.unwrap(), Some(o.clone()));
        assert_eq!(std::fs::read_to_string(dir.path().join("format")).unwrap(), "1\n");
        // No temp files next to it.
        let names: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().all(|n| !n.contains(TMP_PREFIX)), "{names:?}");
        // A newer version is refused with a recognizable error.
        std::fs::write(dir.path().join("format"), "2\n").unwrap();
        let err = DiskObjectStore::open(dir.path(), u64::MAX).err().unwrap();
        let newer = err.get_ref().and_then(|e| e.downcast_ref::<ant_core::NewerFormat>());
        assert_eq!(newer.map(|n| (n.found, n.supported)), Some((2, CACHE_FORMAT)), "{err}");
        // Garbage is corruption, not a newer version.
        std::fs::write(dir.path().join("format"), "x").unwrap();
        let err = DiskObjectStore::open(dir.path(), u64::MAX).err().unwrap();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn corrupt_file_is_a_miss() {
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        let o = EncodedObject::new(ObjectKind::Blob, b"x");
        s.put(o.clone()).await.unwrap();
        std::fs::write(object_path(dir.path(), &o.id()), b"garbage").unwrap();
        assert_eq!(s.get(&o.id()).await.unwrap(), None);
        assert!(!object_path(dir.path(), &o.id()).exists());
    }

    #[tokio::test]
    async fn reads_frames_without_content_size() {
        // Entries written by older versions used the streaming encoder.
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        let o = EncodedObject::new(ObjectKind::Blob, &vec![7u8; 10_000]);
        let path = object_path(dir.path(), &o.id());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, zstd::encode_all(&o.as_bytes()[..], 1).unwrap()).unwrap();
        assert_eq!(s.get(&o.id()).await.unwrap(), Some(o));
    }

    #[tokio::test]
    async fn large_batches_roundtrip_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        let objs: Vec<_> = (0..1000)
            .map(|i| EncodedObject::new(ObjectKind::Blob, format!("object {i}").as_bytes()))
            .collect();
        s.put_many(&objs).await.unwrap();
        let mut ids: Vec<_> = objs.iter().map(|o| o.id()).collect();
        ids.insert(500, ObjectId([9; 32]));
        let got = s.get_many(&ids).await.unwrap();
        assert!(got[500].is_none());
        for (i, o) in objs.iter().enumerate() {
            assert_eq!(got[if i < 500 { i } else { i + 1 }].as_ref(), Some(o));
        }
        // No in-progress files are left in shard directories.
        assert!(s.disk_usage().await.unwrap() > 0);
        for shard in std::fs::read_dir(dir.path().join("objects")).unwrap() {
            for f in std::fs::read_dir(shard.unwrap().path()).unwrap() {
                assert!(!f.unwrap().file_name().to_string_lossy().starts_with(".tmp."));
            }
        }
    }

    #[tokio::test]
    async fn gc_evicts_least_recently_used() {
        let dir = tempfile::tempdir().unwrap();
        // Incompressible-ish payloads so sizes are predictable.
        let mk = |seed: u8| {
            let body: Vec<u8> =
                (0..20_000u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8 ^ seed).collect();
            EncodedObject::new(ObjectKind::Blob, &body)
        };
        let objs: Vec<_> = (0..10).map(mk).collect();
        let s = DiskObjectStore::open(dir.path(), u64::MAX)
            .unwrap()
            .with_touch_interval(Duration::ZERO);
        for o in &objs {
            s.put(o.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        // Touch the oldest so it becomes most-recently-used.
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(s.get(&objs[0].id()).await.unwrap().is_some());

        let total = s.disk_usage().await.unwrap();
        let s = DiskObjectStore::open(dir.path(), total / 2).unwrap();
        let freed = s.gc().await.unwrap();
        assert!(freed > 0);
        assert!(s.disk_usage().await.unwrap() <= (total / 2) * 9 / 10 + 1);
        assert!(s.get(&objs[0].id()).await.unwrap().is_some(), "recently used survived");
        assert!(s.get(&objs[1].id()).await.unwrap().is_none(), "oldest evicted");
    }

    #[tokio::test]
    async fn concurrent_writers_two_handles() {
        let dir = tempfile::tempdir().unwrap();
        let a = Arc::new(DiskObjectStore::open(dir.path(), u64::MAX).unwrap());
        let b = Arc::new(DiskObjectStore::open(dir.path(), u64::MAX).unwrap());
        let objs: Vec<_> = (0..200)
            .map(|i| EncodedObject::new(ObjectKind::Blob, format!("{i}").as_bytes()))
            .collect();
        let (oa, ob) = (objs.clone(), objs.clone());
        let ta = tokio::spawn(async move { a.put_many(&oa).await });
        let tb = tokio::spawn(async move { b.put_many(&ob).await });
        ta.await.unwrap().unwrap();
        tb.await.unwrap().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        let ids: Vec<_> = objs.iter().map(|o| o.id()).collect();
        assert!(s.get_many(&ids).await.unwrap().iter().all(Option::is_some));
        // No temp files left behind.
        assert_eq!(std::fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
    }

    /// Incompressible payload of about `len` bytes.
    fn blob(seed: u64, len: usize) -> EncodedObject {
        let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let body: Vec<u8> = (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect();
        EncodedObject::new(ObjectKind::Blob, &body)
    }

    const BLOB: usize = 20_000;

    async fn bytes(s: &DiskObjectStore) -> u64 {
        s.stats().await.unwrap().bytes
    }

    #[tokio::test]
    async fn writes_stay_near_budget_and_hot_objects_survive() {
        let dir = tempfile::tempdir().unwrap();
        let budget = 40 * BLOB as u64;
        let s =
            DiskObjectStore::open(dir.path(), budget).unwrap().with_touch_interval(Duration::ZERO);
        let hot: Vec<_> = (0..8).map(|i| blob(1000 + i, BLOB)).collect();
        s.put_many(&hot).await.unwrap();
        let hot_ids: Vec<_> = hot.iter().map(|o| o.id()).collect();
        let mut peak = 0;
        for i in 0..250u64 {
            // Reading refreshes recency, so the hot set outlives the cold stream.
            assert!(s.get_many(&hot_ids).await.unwrap().iter().all(Option::is_some), "iter {i}");
            s.put(blob(i, BLOB)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(6)).await;
            peak = peak.max(bytes(&s).await);
        }
        // Bounded by the budget plus the unpublished write count and a batch.
        assert!(peak <= budget + budget / 8, "peak {peak} vs budget {budget}");
        assert!(bytes(&s).await < 250 * BLOB as u64 / 2, "eviction ran");
        assert!(s.get_many(&hot_ids).await.unwrap().iter().all(Option::is_some));
    }

    /// Writer used by the multi-process test below: does nothing unless the
    /// parent re-executes the test binary with the environment set.
    #[tokio::test]
    async fn child_writer() {
        let Some(root) = std::env::var_os("ANT_TEST_CACHE_CHILD") else { return };
        let seed: u64 = std::env::var("ANT_TEST_CACHE_SEED").unwrap().parse().unwrap();
        let budget: u64 = std::env::var("ANT_TEST_CACHE_BUDGET").unwrap().parse().unwrap();
        let s = DiskObjectStore::open(&root, budget).unwrap();
        for batch in 0..60u64 {
            let objs: Vec<_> =
                (0..5).map(|i| blob(seed * 1_000_000 + batch * 5 + i, BLOB)).collect();
            s.put_many(&objs).await.unwrap();
        }
        std::fs::write(std::path::Path::new(&root).join(format!("done.{seed}")), b"").unwrap();
    }

    #[tokio::test]
    async fn concurrent_processes_and_threads_respect_the_budget() {
        let dir = tempfile::tempdir().unwrap();
        let budget = 100 * BLOB as u64;
        let exe = std::env::current_exe().unwrap();
        let children: Vec<_> = (1..=4)
            .map(|seed| {
                std::process::Command::new(&exe)
                    .args(["--exact", "disk::tests::child_writer", "--nocapture"])
                    .env("ANT_TEST_CACHE_CHILD", dir.path())
                    .env("ANT_TEST_CACHE_SEED", seed.to_string())
                    .env("ANT_TEST_CACHE_BUDGET", budget.to_string())
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .spawn()
                    .unwrap()
            })
            .collect();
        let root = dir.path().to_path_buf();
        let threads: Vec<_> = (10..12u64)
            .map(|seed| {
                let root = root.clone();
                tokio::spawn(async move {
                    let s = DiskObjectStore::open(root, budget).unwrap();
                    for batch in 0..60u64 {
                        let objs: Vec<_> =
                            (0..5).map(|i| blob(seed * 1_000_000 + batch * 5 + i, BLOB)).collect();
                        s.put_many(&objs).await.unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.await.unwrap();
        }
        for mut c in children {
            assert!(c.wait().unwrap().success());
        }
        for seed in 1..=4 {
            assert!(dir.path().join(format!("done.{seed}")).exists(), "child {seed} ran");
        }
        let s = DiskObjectStore::open(dir.path(), budget).unwrap();
        let st = s.stats().await.unwrap();
        let written = 6 * 300 * BLOB as u64;
        assert!(st.bytes < written / 3, "evicted: {} of {written}", st.bytes);
        // Each of the six writers may hold back budget/64 plus a batch.
        assert!(st.bytes <= budget + budget / 2, "{} vs budget {budget}", st.bytes);
        // Whatever slack remains, one explicit pass lands under the budget.
        s.gc().await.unwrap();
        assert!(bytes(&s).await <= budget);
        // No in-progress files linger.
        for shard in std::fs::read_dir(dir.path().join("objects")).unwrap() {
            for f in std::fs::read_dir(shard.unwrap().path()).unwrap() {
                assert!(!f.unwrap().file_name().to_string_lossy().starts_with(TMP_PREFIX));
            }
        }
    }

    #[tokio::test]
    async fn eviction_lock_excludes_concurrent_evictors() {
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), 5 * BLOB as u64).unwrap();
        let other = DiskObjectStore::open(dir.path(), 5 * BLOB as u64).unwrap();
        let objs: Vec<_> = (0..20).map(|i| blob(i, BLOB)).collect();
        // Unbounded handle so writes do not evict by themselves.
        let w = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        w.put_many(&objs).await.unwrap();

        let lock = File::options()
            .create(true)
            .append(true)
            .open(dir.path().join(GC_DIR).join("evict.lock"))
            .unwrap();
        lock.lock().unwrap();
        // Someone is evicting: automatic passes skip, nothing is deleted.
        assert_eq!(s.try_gc().await.unwrap(), None);
        assert_eq!(other.try_gc().await.unwrap(), None);
        s.maintain().await.unwrap();
        assert_eq!(s.stats().await.unwrap().objects, 20);
        // An explicit gc waits for the holder.
        let t = std::time::Instant::now();
        let waiter = tokio::spawn(async move { s.gc().await.unwrap() });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!waiter.is_finished());
        lock.unlock().unwrap();
        assert!(waiter.await.unwrap() > 0);
        assert!(t.elapsed() >= Duration::from_millis(200));
        let again = other.try_gc().await.unwrap().expect("lock is free");
        assert_eq!(again.removed, 0);
        assert!(bytes(&other).await <= 5 * BLOB as u64);
    }

    #[tokio::test]
    async fn eviction_skips_in_progress_writes_and_clears_abandoned_ones() {
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        let o = blob(1, BLOB);
        s.put(o.clone()).await.unwrap();
        let shard = object_path(dir.path(), &o.id()).parent().unwrap().to_path_buf();
        let fresh = shard.join(".tmp.1.1");
        let stale = shard.join(".tmp.2.2");
        std::fs::write(&fresh, b"being written").unwrap();
        std::fs::write(&stale, b"crashed").unwrap();
        let long_ago = SystemTime::now() - Duration::from_secs(2 * 3600);
        File::options().write(true).open(&stale).unwrap().set_modified(long_ago).unwrap();
        let r = s.evict_to(0).await.unwrap();
        assert_eq!((r.removed, r.objects, r.bytes), (1, 0, 0));
        assert!(fresh.exists(), "in-progress write untouched");
        assert!(!stale.exists(), "abandoned write removed");
        assert_eq!(s.get(&o.id()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn recently_read_objects_are_evicted_last() {
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        let (a, b) = (blob(1, BLOB), blob(2, BLOB));
        s.put_many(&[a.clone(), b.clone()]).await.unwrap();
        let old = SystemTime::now() - Duration::from_secs(600);
        for o in [&a, &b] {
            let f = File::options().write(true).open(object_path(dir.path(), &o.id())).unwrap();
            f.set_modified(old).unwrap();
        }
        // `a` is read after both went cold; room for one object remains.
        assert!(s.get(&a.id()).await.unwrap().is_some());
        let one = bytes(&s).await / 2 + 1;
        s.evict_to(one).await.unwrap();
        assert!(s.get(&a.id()).await.unwrap().is_some(), "recently read survives");
        assert!(s.get(&b.id()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn reads_refresh_recency_at_most_once_per_interval() {
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        let (old, recent) = (blob(1, 100), blob(2, 100));
        s.put_many(&[old.clone(), recent.clone()]).await.unwrap();
        let set = |o: &EncodedObject, ago: u64| {
            let f = File::options().write(true).open(object_path(dir.path(), &o.id())).unwrap();
            f.set_modified(SystemTime::now() - Duration::from_secs(ago)).unwrap();
        };
        let mtime = |o: &EncodedObject| {
            std::fs::metadata(object_path(dir.path(), &o.id())).unwrap().modified().unwrap()
        };
        set(&old, 3600);
        set(&recent, 10);
        let before = mtime(&recent);
        s.get_many(&[old.id(), recent.id()]).await.unwrap();
        assert!(mtime(&old) > SystemTime::now() - Duration::from_secs(30), "stale mtime bumped");
        assert_eq!(mtime(&recent), before, "recent mtime left alone (default 60 s interval)");
        // Storing an existing object is a use as well.
        set(&old, 3600);
        s.put(old.clone()).await.unwrap();
        assert!(mtime(&old) > SystemTime::now() - Duration::from_secs(30));
    }

    #[tokio::test]
    async fn usage_counter_matches_the_disk() {
        let dir = tempfile::tempdir().unwrap();
        let budget = 1000 * BLOB as u64;
        {
            let s = DiskObjectStore::open(dir.path(), budget).unwrap();
            for i in 0..30 {
                s.put(blob(i, BLOB)).await.unwrap();
            }
            // A second handle's writes are counted too (and flushed on drop).
            let t = DiskObjectStore::open(dir.path(), budget).unwrap();
            t.put(blob(100, 1000)).await.unwrap();
        }
        let s = DiskObjectStore::open(dir.path(), budget).unwrap();
        let text = std::fs::read_to_string(dir.path().join(GC_DIR).join("usage")).unwrap();
        let counted: u64 = text.split_whitespace().next().unwrap().parse().unwrap();
        assert_eq!(counted, bytes(&s).await);
        // Unbounded caches do no accounting at all.
        let dir = tempfile::tempdir().unwrap();
        let s = DiskObjectStore::open(dir.path(), u64::MAX).unwrap();
        s.put(blob(1, 100)).await.unwrap();
        assert!(!dir.path().join(GC_DIR).join("usage").exists());
    }
}
