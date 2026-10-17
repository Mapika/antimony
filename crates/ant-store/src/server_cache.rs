//! Persistent cache of data a server derives from the stores and would
//! otherwise rebuild after every restart: encoded tree bundles (`GET /t/`)
//! and the base packs of the git export.
//!
//! Two tiers, either or both:
//! - [`DiskCache`]: a directory of one server process (`--cache-dir`),
//!   bounded by a byte budget, least recently used entries evicted first.
//! - [`SharedCache`]: keys under `{s3 prefix}/cache/` in the store's own
//!   bucket, shared by every replica of the repository, bounded by a
//!   budget that [`SharedCache::prune`] enforces (oldest first).
//!
//! [`ServerCache`] reads the disk tier first, then the shared one (copying
//! a shared hit to disk), and writes both. It is best effort throughout:
//! I/O errors are logged and counted, never returned, and a corrupt entry
//! is a miss (and is deleted).
//!
//! # What this layer guarantees, and what it does not
//!
//! Entries are immutable and named by what they were derived from (a tree
//! id, the tips of a pack), so a key never needs invalidating; an entry is
//! either exactly the bytes once written under its key or it is not
//! returned. Every entry carries a SHA-256 of all its bytes, checked on
//! every read; writes go to a temporary file renamed into place (disk) or
//! to a single PUT (S3), and pointers (small named entries naming another
//! entry's key) are written after the entry they name. A crash at any
//! point leaves whole entries, stray temporary files (deleted at the next
//! open) or entries that fail their check.
//!
//! Whether an entry may still be *served* is the caller's business: access
//! control and garbage collection are checked against the stores at use
//! (`ant-server`'s bundle endpoint, `ant-gitserve`'s base packs).
//!
//! # Entry format (version [`FORMAT_VERSION`])
//!
//! ```text
//! magic "ANTCACHE" | version u32 LE | kind u32 LE | meta length u64 LE |
//! payload length u64 LE | meta | payload | SHA-256 of everything before
//! ```
//!
//! Disk layout: `<dir>/v1/<namespace>/{bundles,git,pointers}/<key[..2]>/<key>`,
//! plus `LOCK` (one process per namespace directory) and `tmp/`. S3 layout:
//! `{prefix}/cache/v1/{bundles,git,pointers}/<key>`. A format change takes a
//! new version directory; older ones are ignored (and can be deleted).

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::s3::S3ObjectStore;

/// Version of the entry format and layout.
pub const FORMAT_VERSION: u32 = 1;
const VERSION_DIR: &str = "v1";
const MAGIC: &[u8; 8] = b"ANTCACHE";
const HEADER: u64 = 32;
const TRAILER: u64 = 32;
/// Largest meta section accepted when reading.
const MAX_META: u64 = 1 << 30;
/// Entries larger than this are never put into the shared tier (they are
/// assembled in memory for the PUT).
const MAX_SHARED_ENTRY: u64 = 2 << 30;
/// Bytes of background writes waiting or in flight at most, in KiB
/// ([`ServerCache::put_background`]); more are skipped. A checkout of a
/// fresh tree builds its bundles in a burst: they queue here, bounded.
const BACKGROUND_KIB: usize = 256 << 10;
/// Copy buffer.
const CHUNK: usize = 1 << 20;
/// Disk entries up to this size are read into memory whole.
const IN_MEMORY: u64 = 64 << 20;

/// What an entry holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    /// An encoded tree bundle; meta: the ids its visibility depends on.
    Bundle,
    /// The entries of a git export base pack; meta: what it contains.
    GitBase,
    /// A named pointer to another entry (meta: that entry's key; no
    /// payload). Not evicted, not counted in budgets.
    Pointer,
}

impl Kind {
    fn dir(self) -> &'static str {
        match self {
            Kind::Bundle => "bundles",
            Kind::GitBase => "git",
            Kind::Pointer => "pointers",
        }
    }

    fn tag(self) -> u32 {
        match self {
            Kind::Bundle => 1,
            Kind::GitBase => 2,
            Kind::Pointer => 3,
        }
    }
}

/// Keys are 2..=160 characters of `[0-9a-z.-]`, not starting with a dot.
pub fn valid_key(key: &str) -> bool {
    (2..=160).contains(&key.len())
        && !key.starts_with('.')
        && key
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
}

/// A short, stable directory name for a store identity (e.g. its Postgres
/// schema and S3 prefix): servers of different repositories never share
/// entries in one cache directory.
pub fn namespace(identity: &str) -> String {
    hex::encode(&Sha256::digest(identity.as_bytes())[..8])
}

fn header(kind: Kind, meta_len: u64, payload_len: u64) -> [u8; HEADER as usize] {
    let mut h = [0u8; HEADER as usize];
    h[..8].copy_from_slice(MAGIC);
    h[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    h[12..16].copy_from_slice(&kind.tag().to_le_bytes());
    h[16..24].copy_from_slice(&meta_len.to_le_bytes());
    h[24..32].copy_from_slice(&payload_len.to_le_bytes());
    h
}

/// (meta length, payload length) of a header that fits `kind` and an entry
/// of `total` bytes.
fn parse_header(h: &[u8], kind: Kind, total: u64) -> Option<(u64, u64)> {
    if h.len() < HEADER as usize || &h[..8] != MAGIC {
        return None;
    }
    let u32_at = |i: usize| u32::from_le_bytes(h[i..i + 4].try_into().unwrap_or_default());
    let u64_at = |i: usize| u64::from_le_bytes(h[i..i + 8].try_into().unwrap_or_default());
    if u32_at(8) != FORMAT_VERSION || u32_at(12) != kind.tag() {
        return None;
    }
    let (meta, payload) = (u64_at(16), u64_at(24));
    let expected = HEADER.checked_add(meta)?.checked_add(payload)?.checked_add(TRAILER)?;
    (meta <= MAX_META && expected == total).then_some((meta, payload))
}

/// Total bytes of an entry.
pub fn entry_len(meta_len: usize, payload_len: u64) -> u64 {
    HEADER + meta_len as u64 + payload_len + TRAILER
}

/// Where an entry's payload comes from when it is written.
pub enum Source {
    Bytes(Bytes),
    /// `len` bytes of `file` from `offset` (positional reads only).
    File {
        file: File,
        offset: u64,
        len: u64,
    },
}

impl Source {
    pub fn len(&self) -> u64 {
        match self {
            Source::Bytes(b) => b.len() as u64,
            Source::File { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Feed the payload to `f` in chunks.
    fn each_chunk(&self, mut f: impl FnMut(&[u8]) -> io::Result<()>) -> io::Result<()> {
        match self {
            Source::Bytes(b) => f(b),
            Source::File { file, offset, len } => {
                let mut buf = vec![0u8; CHUNK.min(*len as usize)];
                let mut done = 0u64;
                while done < *len {
                    let take = (*len - done).min(CHUNK as u64) as usize;
                    file.read_exact_at(&mut buf[..take], offset + done)?;
                    f(&buf[..take])?;
                    done += take as u64;
                }
                Ok(())
            }
        }
    }
}

/// Write one entry to `out`.
fn write_entry(out: &mut impl Write, kind: Kind, meta: &[u8], payload: &Source) -> io::Result<()> {
    let mut hash = Sha256::new();
    let h = header(kind, meta.len() as u64, payload.len());
    for part in [&h[..], meta] {
        hash.update(part);
        out.write_all(part)?;
    }
    payload.each_chunk(|chunk| {
        hash.update(chunk);
        out.write_all(chunk)
    })?;
    out.write_all(&hash.finalize())
}

/// The bytes of one entry (for the shared tier).
pub fn encode(kind: Kind, meta: &[u8], payload: &Source) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(entry_len(meta.len(), payload.len()) as usize);
    write_entry(&mut out, kind, meta, payload)?;
    Ok(out)
}

/// Meta and payload range of a whole entry held in memory, if it is an
/// intact entry of `kind`.
pub fn decode(kind: Kind, bytes: &[u8]) -> Option<(Vec<u8>, std::ops::Range<usize>)> {
    let (meta, payload) = parse_header(bytes, kind, bytes.len() as u64)?;
    let body = bytes.len() - TRAILER as usize;
    if Sha256::digest(&bytes[..body])[..] != bytes[body..] {
        return None;
    }
    let meta_end = HEADER as usize + meta as usize;
    Some((bytes[HEADER as usize..meta_end].to_vec(), meta_end..meta_end + payload as usize))
}

/// Meta, payload offset and length of the entry in `file`, if it is an
/// intact entry of `kind` (reads and hashes the whole file).
fn verify_file(file: &File, kind: Kind) -> io::Result<Option<(Vec<u8>, u64, u64)>> {
    let total = file.metadata()?.len();
    if total < HEADER + TRAILER {
        return Ok(None);
    }
    let mut h = [0u8; HEADER as usize];
    file.read_exact_at(&mut h, 0)?;
    let Some((meta_len, payload_len)) = parse_header(&h, kind, total) else {
        return Ok(None);
    };
    let mut hash = Sha256::new();
    let body = total - TRAILER;
    let mut buf = vec![0u8; CHUNK.min(body as usize)];
    let mut meta = Vec::with_capacity(meta_len as usize);
    let mut done = 0u64;
    while done < body {
        let take = (body - done).min(CHUNK as u64) as usize;
        file.read_exact_at(&mut buf[..take], done)?;
        hash.update(&buf[..take]);
        // Collect the meta section on the way.
        let (start, end) = (done, done + take as u64);
        let (m0, m1) = (HEADER, HEADER + meta_len);
        if start < m1 && end > m0 {
            let from = m0.max(start) - start;
            let to = m1.min(end) - start;
            meta.extend_from_slice(&buf[from as usize..to as usize]);
        }
        done = end;
    }
    let mut trailer = [0u8; TRAILER as usize];
    file.read_exact_at(&mut trailer, body)?;
    if hash.finalize()[..] != trailer {
        return Ok(None);
    }
    Ok(Some((meta, HEADER + meta_len, payload_len)))
}

/// A verified entry.
pub struct Entry {
    pub meta: Vec<u8>,
    payload: Payload,
}

enum Payload {
    File { file: File, offset: u64, len: u64 },
    Memory(Bytes),
}

impl Entry {
    /// Payload bytes.
    pub fn len(&self) -> u64 {
        match &self.payload {
            Payload::File { len, .. } => *len,
            Payload::Memory(b) => b.len() as u64,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The payload in memory (blocking for an entry on disk).
    pub fn read_payload(&self) -> io::Result<Bytes> {
        match &self.payload {
            Payload::Memory(b) => Ok(b.clone()),
            Payload::File { file, offset, len } => {
                let mut buf = vec![0u8; *len as usize];
                file.read_exact_at(&mut buf, *offset)?;
                Ok(Bytes::from(buf))
            }
        }
    }

    /// A file holding the payload at (offset, length): the entry's own file
    /// (it stays readable after an eviction unlinks it), or an unlinked
    /// temporary file for an entry read into memory (blocking).
    pub fn into_file(self) -> io::Result<(File, u64, u64)> {
        match self.payload {
            Payload::File { file, offset, len } => Ok((file, offset, len)),
            Payload::Memory(b) => {
                let mut file = tempfile::tempfile()?;
                file.write_all(&b)?;
                Ok((file, 0, b.len() as u64))
            }
        }
    }
}

#[derive(Default)]
struct Index {
    entries: HashMap<(Kind, String), (u64, u64)>,
    /// Use sequence -> entry, least recently used first.
    order: BTreeMap<u64, (Kind, String)>,
    bytes: u64,
    next: u64,
}

impl Index {
    fn upsert(&mut self, kind: Kind, key: &str, size: u64) {
        self.remove(kind, key);
        let seq = self.next;
        self.next += 1;
        self.entries.insert((kind, key.to_owned()), (size, seq));
        self.order.insert(seq, (kind, key.to_owned()));
        self.bytes += size;
    }

    fn touch(&mut self, kind: Kind, key: &str) {
        let k = (kind, key.to_owned());
        if let Some((size, seq)) = self.entries.get(&k).copied() {
            self.order.remove(&seq);
            let next = self.next;
            self.next += 1;
            self.entries.insert(k.clone(), (size, next));
            self.order.insert(next, k);
        }
    }

    fn remove(&mut self, kind: Kind, key: &str) {
        if let Some((size, seq)) = self.entries.remove(&(kind, key.to_owned())) {
            self.order.remove(&seq);
            self.bytes -= size;
        }
    }

    /// Remove least recently used entries until `bytes <= budget`; returns
    /// them.
    fn evict(&mut self, budget: u64) -> Vec<(Kind, String)> {
        let mut out = Vec::new();
        while self.bytes > budget {
            let Some((_, (kind, key))) = self.order.pop_first() else { break };
            if let Some((size, _)) = self.entries.remove(&(kind, key.clone())) {
                self.bytes -= size;
            }
            out.push((kind, key));
        }
        out
    }
}

/// The disk tier: one namespace directory, held by one process.
pub struct DiskCache {
    dir: PathBuf,
    budget: u64,
    _lock: File,
    index: Mutex<Index>,
    evicted: AtomicU64,
}

impl std::fmt::Debug for DiskCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCache").field("dir", &self.dir).field("budget", &self.budget).finish()
    }
}

fn lock_index(m: &Mutex<Index>) -> std::sync::MutexGuard<'_, Index> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl DiskCache {
    /// Open (creating) `<root>/v1/<namespace>` with a budget of `budget`
    /// bytes (blocking: scans the entries, deletes temporary files left by
    /// a crash, evicts beyond the budget). Fails if another process holds
    /// the directory.
    pub fn open(root: &Path, namespace: &str, budget: u64) -> io::Result<Self> {
        if !valid_key(namespace) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid cache namespace"));
        }
        let dir = root.join(VERSION_DIR).join(namespace);
        std::fs::create_dir_all(dir.join("tmp"))?;
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("LOCK"))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("cache directory {} is in use by another process", dir.display()),
                ));
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(e),
        }
        for e in std::fs::read_dir(dir.join("tmp"))? {
            let _ = std::fs::remove_file(e?.path());
        }
        let mut found = Vec::new();
        for kind in [Kind::Bundle, Kind::GitBase] {
            let kdir = dir.join(kind.dir());
            let Ok(fans) = std::fs::read_dir(&kdir) else { continue };
            for fan in fans {
                let Ok(files) = std::fs::read_dir(fan?.path()) else { continue };
                for file in files {
                    let file = file?;
                    let name = file.file_name().to_string_lossy().into_owned();
                    let meta = file.metadata()?;
                    if !valid_key(&name) || !meta.is_file() {
                        continue;
                    }
                    let at = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                    found.push((at, kind, name, meta.len()));
                }
            }
        }
        found.sort();
        let mut index = Index::default();
        for (_, kind, key, size) in found {
            index.upsert(kind, &key, size);
        }
        let cache =
            Self { dir, budget, _lock: lock, index: Mutex::new(index), evicted: AtomicU64::new(0) };
        cache.evict();
        Ok(cache)
    }

    /// The namespace directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn budget(&self) -> u64 {
        self.budget
    }

    /// (entries, bytes) held.
    pub fn usage(&self) -> (usize, u64) {
        let index = lock_index(&self.index);
        (index.entries.len(), index.bytes)
    }

    fn path(&self, kind: Kind, key: &str) -> PathBuf {
        self.dir.join(kind.dir()).join(&key[..2]).join(key)
    }

    fn evict(&self) {
        let victims = lock_index(&self.index).evict(self.budget);
        for (kind, key) in victims {
            self.evicted.fetch_add(1, Ordering::Relaxed);
            let _ = std::fs::remove_file(self.path(kind, &key));
        }
    }

    /// The verified entry `key` of `kind` (blocking). A corrupt entry is
    /// deleted and reported as absent.
    pub fn get(&self, kind: Kind, key: &str) -> io::Result<Option<Entry>> {
        if !valid_key(key) {
            return Ok(None);
        }
        let path = self.path(kind, key);
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                lock_index(&self.index).remove(kind, key);
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        // Small entries (bundles, pointers) are read and checked in one go
        // and served from memory; base packs are checked in a streaming
        // pass and stay on disk.
        let size = file.metadata()?.len();
        let verified = if kind != Kind::GitBase && size <= IN_MEMORY {
            let mut bytes = Vec::with_capacity(size as usize);
            (&file).read_to_end(&mut bytes)?;
            let bytes = Bytes::from(bytes);
            decode(kind, &bytes).map(|(meta, range)| (meta, Payload::Memory(bytes.slice(range))))
        } else {
            match verify_file(&file, kind)? {
                Some((meta, offset, len)) => {
                    Some((meta, Payload::File { file: file.try_clone()?, offset, len }))
                }
                None => None,
            }
        };
        match verified {
            Some((meta, payload)) => {
                if kind != Kind::Pointer {
                    lock_index(&self.index).touch(kind, key);
                    // Keeps the use order across restarts (best effort).
                    let _ = file.set_modified(SystemTime::now());
                }
                Ok(Some(Entry { meta, payload }))
            }
            None => {
                tracing::warn!(path = %path.display(), "server cache: corrupt entry deleted");
                CORRUPT.fetch_add(1, Ordering::Relaxed);
                let _ = std::fs::remove_file(&path);
                lock_index(&self.index).remove(kind, key);
                Ok(None)
            }
        }
    }

    /// Whether an entry of `size` bytes is kept at all (at most half the
    /// budget; pointers always).
    fn fits(&self, kind: Kind, size: u64) -> bool {
        kind == Kind::Pointer || size <= self.budget / 2
    }

    /// Store an entry (blocking): written to a temporary file, renamed into
    /// place. `Ok(false)`: too large for the budget, not stored.
    pub fn put(&self, kind: Kind, key: &str, meta: &[u8], payload: &Source) -> io::Result<bool> {
        let size = entry_len(meta.len(), payload.len());
        self.install(kind, key, size, |out| write_entry(out, kind, meta, payload))
    }

    /// Store an already encoded entry (blocking), e.g. one read from the
    /// shared tier.
    pub fn put_encoded(&self, kind: Kind, key: &str, bytes: &[u8]) -> io::Result<bool> {
        self.install(kind, key, bytes.len() as u64, |out| out.write_all(bytes))
    }

    fn install(
        &self,
        kind: Kind,
        key: &str,
        size: u64,
        write: impl FnOnce(&mut BufWriter<File>) -> io::Result<()>,
    ) -> io::Result<bool> {
        if !valid_key(key) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid cache key"));
        }
        if !self.fits(kind, size) {
            return Ok(false);
        }
        let tmp = self.dir.join("tmp").join(format!("{key}.{:016x}", rand::random::<u64>()));
        let result = (|| {
            let mut out = BufWriter::with_capacity(CHUNK, File::create(&tmp)?);
            write(&mut out)?;
            out.into_inner().map_err(|e| e.into_error())?;
            let path = self.path(kind, key);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            // No fsync: a torn entry fails its check and is dropped.
            std::fs::rename(&tmp, &path)
        })();
        if let Err(e) = result {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        if kind != Kind::Pointer {
            lock_index(&self.index).upsert(kind, key, size);
            self.evict();
        }
        Ok(true)
    }

    /// Delete an entry (blocking; absent is fine).
    pub fn remove(&self, kind: Kind, key: &str) -> io::Result<()> {
        if !valid_key(key) {
            return Ok(());
        }
        lock_index(&self.index).remove(kind, key);
        match std::fs::remove_file(self.path(kind, key)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }
}

/// The shared tier: entries in the store's S3 bucket, under
/// `{prefix}/cache/v1/`.
pub struct SharedCache {
    s3: S3ObjectStore,
    budget: u64,
}

impl SharedCache {
    pub fn new(s3: S3ObjectStore, budget: u64) -> Self {
        Self { s3, budget }
    }

    pub fn budget(&self) -> u64 {
        self.budget
    }

    fn rel(kind: Kind, key: &str) -> String {
        format!("{VERSION_DIR}/{}/{key}", kind.dir())
    }

    pub async fn get(&self, kind: Kind, key: &str) -> crate::Result<Option<Bytes>> {
        if !valid_key(key) {
            return Ok(None);
        }
        self.s3.cache_get(&Self::rel(kind, key)).await
    }

    pub async fn put(&self, kind: Kind, key: &str, bytes: Bytes) -> crate::Result<()> {
        self.s3.cache_put(&Self::rel(kind, key), bytes).await
    }

    pub async fn remove(&self, kind: Kind, key: &str) -> crate::Result<()> {
        self.s3.cache_delete(&[Self::rel(kind, key)]).await
    }

    /// Delete the oldest entries (by S3 modification time; pointers are
    /// kept) until the cache holds at most 90 % of its budget, if it holds
    /// more than the budget. Returns (entries deleted, bytes held after).
    pub async fn prune(&self) -> crate::Result<(usize, u64)> {
        let pointers = format!("{VERSION_DIR}/{}/", Kind::Pointer.dir());
        let mut all: Vec<(String, u64, f64)> = self
            .s3
            .cache_list(&format!("{VERSION_DIR}/"))
            .await?
            .into_iter()
            .filter(|(k, _, _)| !k.starts_with(&pointers))
            .collect();
        let mut bytes: u64 = all.iter().map(|(_, s, _)| s).sum();
        if bytes <= self.budget {
            return Ok((0, bytes));
        }
        all.sort_by(|a, b| a.2.total_cmp(&b.2));
        let target = self.budget / 10 * 9;
        let mut doomed = Vec::new();
        for (key, size, _) in all {
            if bytes <= target {
                break;
            }
            bytes -= size;
            doomed.push(key);
        }
        self.s3.cache_delete(&doomed).await?;
        Ok((doomed.len(), bytes))
    }
}

static CORRUPT: AtomicU64 = AtomicU64::new(0);

/// Counters of a [`ServerCache`] (and the process-wide corrupt count).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub disk_hits: u64,
    pub shared_hits: u64,
    pub misses: u64,
    pub writes: u64,
    pub write_errors: u64,
    /// Background writes skipped because too many were in flight.
    pub skipped: u64,
    pub corrupt: u64,
    pub evicted: u64,
    pub disk_entries: u64,
    pub disk_bytes: u64,
    pub disk_budget: u64,
    pub shared: bool,
}

#[derive(Default)]
struct Counters {
    disk_hits: AtomicU64,
    shared_hits: AtomicU64,
    misses: AtomicU64,
    writes: AtomicU64,
    write_errors: AtomicU64,
    skipped: AtomicU64,
}

/// The tiers together (see the module docs).
pub struct ServerCache {
    disk: Option<Arc<DiskCache>>,
    shared: Option<Arc<SharedCache>>,
    counters: Counters,
    writes: Arc<tokio::sync::Semaphore>,
}

impl std::fmt::Debug for ServerCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerCache")
            .field("disk", &self.disk)
            .field("shared", &self.shared.is_some())
            .finish()
    }
}

fn add(c: &AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
}

impl ServerCache {
    /// `None` when neither tier is given.
    pub fn new(disk: Option<DiskCache>, shared: Option<SharedCache>) -> Option<Self> {
        if disk.is_none() && shared.is_none() {
            return None;
        }
        Some(Self {
            disk: disk.map(Arc::new),
            shared: shared.map(Arc::new),
            counters: Counters::default(),
            writes: Arc::new(tokio::sync::Semaphore::new(BACKGROUND_KIB)),
        })
    }

    pub fn disk(&self) -> Option<&DiskCache> {
        self.disk.as_deref()
    }

    pub fn stats(&self) -> CacheStats {
        let c = &self.counters;
        let (entries, bytes) = self.disk.as_ref().map_or((0, 0), |d| d.usage());
        CacheStats {
            disk_hits: c.disk_hits.load(Ordering::Relaxed),
            shared_hits: c.shared_hits.load(Ordering::Relaxed),
            misses: c.misses.load(Ordering::Relaxed),
            writes: c.writes.load(Ordering::Relaxed),
            write_errors: c.write_errors.load(Ordering::Relaxed),
            skipped: c.skipped.load(Ordering::Relaxed),
            corrupt: CORRUPT.load(Ordering::Relaxed),
            evicted: self.disk.as_ref().map_or(0, |d| d.evicted.load(Ordering::Relaxed)),
            disk_entries: entries as u64,
            disk_bytes: bytes,
            disk_budget: self.disk.as_ref().map_or(0, |d| d.budget),
            shared: self.shared.is_some(),
        }
    }

    /// The entry `key` of `kind`: from disk, else from the shared tier
    /// (then also stored on disk); `None` when absent, corrupt or
    /// unreadable.
    pub async fn get(&self, kind: Kind, key: &str) -> Option<Entry> {
        if let Some(disk) = &self.disk {
            let (disk, k) = (disk.clone(), key.to_owned());
            match tokio::task::spawn_blocking(move || disk.get(kind, &k)).await {
                Ok(Ok(Some(entry))) => {
                    if kind != Kind::Pointer {
                        add(&self.counters.disk_hits);
                    }
                    return Some(entry);
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => tracing::warn!(error = %e, key, "server cache: disk read failed"),
                Err(e) => tracing::warn!(error = %e, key, "server cache: disk read failed"),
            }
        }
        if let Some(entry) = self.get_shared(kind, key).await {
            return Some(entry);
        }
        if kind != Kind::Pointer {
            add(&self.counters.misses);
        }
        None
    }

    async fn get_shared(&self, kind: Kind, key: &str) -> Option<Entry> {
        let shared = self.shared.as_ref()?;
        let bytes = match shared.get(kind, key).await {
            Ok(Some(b)) => b,
            Ok(None) => return None,
            Err(e) => {
                tracing::warn!(error = %e, key, "server cache: shared read failed");
                return None;
            }
        };
        let disk = self.disk.clone();
        let k = key.to_owned();
        let decoded = tokio::task::spawn_blocking(move || {
            let decoded = decode(kind, &bytes);
            if decoded.is_some()
                && let Some(disk) = disk
                && let Err(e) = disk.put_encoded(kind, &k, &bytes)
            {
                tracing::warn!(error = %e, key = k, "server cache: disk write failed");
            }
            decoded.map(|(meta, range)| (meta, bytes.slice(range)))
        })
        .await
        .ok()?;
        match decoded {
            Some((meta, payload)) => {
                if kind != Kind::Pointer {
                    add(&self.counters.shared_hits);
                }
                Some(Entry { meta, payload: Payload::Memory(payload) })
            }
            None => {
                tracing::warn!(key, "server cache: corrupt shared entry deleted");
                CORRUPT.fetch_add(1, Ordering::Relaxed);
                let _ = shared.remove(kind, key).await;
                None
            }
        }
    }

    /// Store an entry in every tier (waits for the writes); true when at
    /// least one tier stored it.
    pub async fn put(&self, kind: Kind, key: &str, meta: Vec<u8>, payload: Source) -> bool {
        if !valid_key(key) {
            tracing::warn!(key, "server cache: invalid key not stored");
            return false;
        }
        let mut stored = false;
        let payload = Arc::new(payload);
        let meta = Arc::new(meta);
        if let Some(disk) = &self.disk {
            let (disk, k, m, p) = (disk.clone(), key.to_owned(), meta.clone(), payload.clone());
            match tokio::task::spawn_blocking(move || disk.put(kind, &k, &m, &p)).await {
                Ok(Ok(true)) => {
                    stored = true;
                    add(&self.counters.writes);
                }
                Ok(Ok(false)) => {}
                Ok(Err(e)) => {
                    add(&self.counters.write_errors);
                    tracing::warn!(error = %e, key, "server cache: disk write failed");
                }
                Err(e) => tracing::warn!(error = %e, key, "server cache: disk write failed"),
            }
        }
        if let Some(shared) = &self.shared {
            let size = entry_len(meta.len(), payload.len());
            if kind != Kind::Pointer && (size > MAX_SHARED_ENTRY || size > shared.budget / 2) {
                return stored;
            }
            let (m, p) = (meta.clone(), payload.clone());
            let bytes = match tokio::task::spawn_blocking(move || encode(kind, &m, &p)).await {
                Ok(Ok(b)) => Bytes::from(b),
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, key, "server cache: encoding failed");
                    return stored;
                }
                Err(_) => return stored,
            };
            match shared.put(kind, key, bytes).await {
                Ok(()) => {
                    stored = true;
                    add(&self.counters.writes);
                }
                Err(e) => {
                    add(&self.counters.write_errors);
                    tracing::warn!(error = %e, key, "server cache: shared write failed");
                }
            }
        }
        stored
    }

    /// [`Self::put`] in a background task, unless too many bytes are
    /// queued already (then the entry is skipped; the cache is best
    /// effort).
    pub fn put_background(
        self: &Arc<Self>,
        kind: Kind,
        key: String,
        meta: Vec<u8>,
        payload: Source,
    ) {
        let kib = (entry_len(meta.len(), payload.len()) >> 10).clamp(1, BACKGROUND_KIB as u64);
        let Ok(permit) = self.writes.clone().try_acquire_many_owned(kib as u32) else {
            add(&self.counters.skipped);
            return;
        };
        let this = self.clone();
        tokio::spawn(async move {
            let _ = this.put(kind, &key, meta, payload).await;
            drop(permit);
        });
    }

    /// Delete an entry from every tier.
    pub async fn remove(&self, kind: Kind, key: &str) {
        if let Some(disk) = &self.disk {
            let (disk, k) = (disk.clone(), key.to_owned());
            let _ = tokio::task::spawn_blocking(move || disk.remove(kind, &k)).await;
        }
        if let Some(shared) = &self.shared
            && let Err(e) = shared.remove(kind, key).await
        {
            tracing::warn!(error = %e, key, "server cache: shared delete failed");
        }
    }

    /// The key pointer `name` names: the shared tier first (another replica
    /// may have moved it), then disk.
    pub async fn pointer(&self, name: &str) -> Option<String> {
        let entry = match self.get_shared(Kind::Pointer, name).await {
            Some(e) => Some(e),
            None => match &self.disk {
                Some(disk) => {
                    let (disk, n) = (disk.clone(), name.to_owned());
                    tokio::task::spawn_blocking(move || disk.get(Kind::Pointer, &n))
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .flatten()
                }
                None => None,
            },
        }?;
        String::from_utf8(entry.meta).ok().filter(|k| valid_key(k))
    }

    /// Point `name` at `key` (write the entry first).
    pub async fn set_pointer(&self, name: &str, key: &str) {
        let _ = self
            .put(Kind::Pointer, name, key.as_bytes().to_vec(), Source::Bytes(Bytes::new()))
            .await;
    }

    /// Prune the shared tier every `every` in a background task (no-op
    /// without one).
    pub fn spawn_pruner(self: &Arc<Self>, every: Duration) {
        let Some(shared) = self.shared.clone() else { return };
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            loop {
                tick.tick().await;
                match shared.prune().await {
                    Ok((0, _)) => {}
                    Ok((n, bytes)) => {
                        tracing::info!(deleted = n, bytes, "server cache: shared tier pruned")
                    }
                    Err(e) => tracing::warn!(error = %e, "server cache: shared prune failed"),
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(b: &[u8]) -> Source {
        Source::Bytes(Bytes::copy_from_slice(b))
    }

    #[test]
    fn encode_decode_round_trip_and_reject_damage() {
        let bytes = encode(Kind::Bundle, b"meta", &src(b"payload")).unwrap();
        let (meta, range) = decode(Kind::Bundle, &bytes).unwrap();
        assert_eq!(meta, b"meta");
        assert_eq!(&bytes[range], b"payload");
        assert!(decode(Kind::GitBase, &bytes).is_none(), "wrong kind");
        for i in [0, 9, 13, 20, 33, 40, bytes.len() - 1] {
            let mut bad = bytes.clone();
            bad[i] ^= 1;
            assert!(decode(Kind::Bundle, &bad).is_none(), "flipped byte {i}");
        }
        assert!(decode(Kind::Bundle, &bytes[..bytes.len() - 1]).is_none(), "truncated");
        let mut long = bytes.clone();
        long.push(0);
        assert!(decode(Kind::Bundle, &long).is_none(), "trailing garbage");
    }

    #[test]
    fn keys_are_restricted() {
        assert!(valid_key("ab12-x.y"));
        for bad in ["", "a", ".ab", "AB", "a/b", "a b", &"a".repeat(161)] {
            assert!(!valid_key(bad), "{bad:?}");
        }
    }

    #[test]
    fn disk_entries_survive_reopen_and_corruption_is_a_miss() {
        let root = tempfile::tempdir().unwrap();
        let payload = vec![7u8; 3 * CHUNK + 5];
        {
            let disk = DiskCache::open(root.path(), "ns", 1 << 30).unwrap();
            assert!(disk.put(Kind::GitBase, "base", b"m", &src(&payload)).unwrap());
            assert!(disk.put(Kind::Bundle, "b1", b"ids", &src(b"body")).unwrap());
            assert!(disk.put(Kind::Pointer, "p1", b"base", &src(b"")).unwrap());
            // A second process cannot open the directory meanwhile.
            assert!(DiskCache::open(root.path(), "ns", 1 << 30).is_err());
            // A torn temporary file from a crash.
            std::fs::write(disk.dir().join("tmp").join("x.1"), b"partial").unwrap();
        }
        let disk = DiskCache::open(root.path(), "ns", 1 << 30).unwrap();
        assert_eq!(std::fs::read_dir(disk.dir().join("tmp")).unwrap().count(), 0);
        assert_eq!(disk.usage().0, 2, "pointers are not counted");
        let e = disk.get(Kind::GitBase, "base").unwrap().unwrap();
        assert_eq!(e.meta, b"m");
        assert_eq!(e.read_payload().unwrap(), payload);
        let (file, offset, len) = e.into_file().unwrap();
        let mut buf = vec![0u8; len as usize];
        file.read_exact_at(&mut buf, offset).unwrap();
        assert_eq!(buf, payload);
        assert_eq!(disk.get(Kind::Pointer, "p1").unwrap().unwrap().meta, b"base");
        assert!(disk.get(Kind::Bundle, "b2").unwrap().is_none());
        // Another kind under the same key is another entry.
        assert!(disk.get(Kind::Bundle, "base").unwrap().is_none());

        // Flip a payload byte: the entry is dropped, file and all.
        let path = disk.path(Kind::Bundle, "b1");
        let mut bytes = std::fs::read(&path).unwrap();
        let n = bytes.len();
        bytes[n - 40] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        assert!(disk.get(Kind::Bundle, "b1").unwrap().is_none());
        assert!(!path.exists());
        assert_eq!(disk.usage().0, 1);
        // The streaming check of large entries agrees.
        let path = disk.path(Kind::GitBase, "base");
        let (meta, offset, len) =
            verify_file(&File::open(&path).unwrap(), Kind::GitBase).unwrap().unwrap();
        assert_eq!((meta.as_slice(), offset, len), (&b"m"[..], HEADER + 1, payload.len() as u64));
        let mut damaged = std::fs::read(&path).unwrap();
        damaged[CHUNK + 7] ^= 1;
        let copy = root.path().join("copy");
        std::fs::write(&copy, &damaged).unwrap();
        assert!(verify_file(&File::open(&copy).unwrap(), Kind::GitBase).unwrap().is_none());
        assert!(verify_file(&File::open(&path).unwrap(), Kind::Bundle).unwrap().is_none());
        // Truncated.
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(100).unwrap();
        assert!(disk.get(Kind::GitBase, "base").unwrap().is_none());
    }

    #[test]
    fn disk_budget_evicts_least_recently_used() {
        let root = tempfile::tempdir().unwrap();
        let body = vec![1u8; 1000];
        let size = entry_len(0, 1000);
        let disk = DiskCache::open(root.path(), "ns", 4 * size).unwrap();
        for k in ["k1", "k2", "k3", "k4"] {
            assert!(disk.put(Kind::Bundle, k, b"", &src(&body)).unwrap());
        }
        assert!(disk.get(Kind::Bundle, "k1").unwrap().is_some()); // now most recent
        assert!(disk.put(Kind::Bundle, "k5", b"", &src(&body)).unwrap());
        assert_eq!(disk.usage(), (4, 4 * size));
        assert!(disk.get(Kind::Bundle, "k2").unwrap().is_none(), "least recently used went");
        assert!(disk.get(Kind::Bundle, "k1").unwrap().is_some());
        // Over half the budget: not stored.
        let big = vec![0u8; 3 * 1000];
        assert!(!disk.put(Kind::Bundle, "k6", b"", &src(&big)).unwrap());
        drop(disk);
        // Reopening with a smaller budget evicts down to it.
        let disk = DiskCache::open(root.path(), "ns", 2 * size).unwrap();
        assert_eq!(disk.usage(), (2, 2 * size));
    }

    #[tokio::test]
    async fn server_cache_reads_and_writes_the_disk_tier() {
        let root = tempfile::tempdir().unwrap();
        let disk = DiskCache::open(root.path(), "ns", 1 << 20).unwrap();
        let cache = Arc::new(ServerCache::new(Some(disk), None).unwrap());
        assert!(cache.get(Kind::Bundle, "aa").await.is_none());
        cache.put(Kind::Bundle, "aa", b"m".to_vec(), src(b"x")).await;
        let e = cache.get(Kind::Bundle, "aa").await.unwrap();
        assert_eq!((e.meta.as_slice(), e.read_payload().unwrap().as_ref()), (&b"m"[..], &b"x"[..]));
        assert_eq!(cache.pointer("ptr").await, None);
        cache.set_pointer("ptr", "aa").await;
        assert_eq!(cache.pointer("ptr").await.as_deref(), Some("aa"));
        cache.remove(Kind::Bundle, "aa").await;
        assert!(cache.get(Kind::Bundle, "aa").await.is_none());
        let s = cache.stats();
        assert_eq!((s.disk_hits, s.misses, s.writes), (1, 2, 2));
        assert!(ServerCache::new(None, None).is_none());
    }

    /// The shared tier against the test S3 (SeaweedFS), in a scratch prefix.
    #[tokio::test]
    #[ignore = "needs S3 (SeaweedFS on localhost:9009)"]
    async fn shared_tier_round_trip_copy_to_disk_and_prune() {
        let cfg = crate::s3::S3Config::for_tests("server-cache");
        let s3 = S3ObjectStore::connect_scratch(&cfg).await.unwrap();
        s3.ensure_bucket().await.unwrap();
        let entry = entry_len(1, 1000);
        let shared = SharedCache::new(s3.clone(), 3 * entry);
        let root = tempfile::tempdir().unwrap();
        // Replica A writes; replica B (its own disk) reads.
        let a = ServerCache::new(None, Some(SharedCache::new(s3.clone(), 3 * entry))).unwrap();
        a.put(Kind::Bundle, "t1", b"m".to_vec(), src(&[5u8; 1000])).await;
        a.set_pointer("git-x", "t1").await;
        let disk = DiskCache::open(root.path(), "ns", 1 << 20).unwrap();
        let b =
            ServerCache::new(Some(disk), Some(SharedCache::new(s3.clone(), 3 * entry))).unwrap();
        assert_eq!(b.pointer("git-x").await.as_deref(), Some("t1"));
        let e = b.get(Kind::Bundle, "t1").await.unwrap();
        assert_eq!(e.read_payload().unwrap().as_ref(), &[5u8; 1000]);
        assert_eq!(b.stats().shared_hits, 1);
        // Now on B's disk too.
        assert!(b.disk().unwrap().get(Kind::Bundle, "t1").unwrap().is_some());
        // A corrupt shared entry is a miss and is deleted.
        s3.cache_put("v1/bundles/t2", Bytes::from_static(b"garbage")).await.unwrap();
        assert!(a.get(Kind::Bundle, "t2").await.is_none());
        assert!(s3.cache_get("v1/bundles/t2").await.unwrap().is_none());
        // Pruning keeps the newest entries within the budget.
        for k in ["t2", "t3", "t4", "t5"] {
            a.put(Kind::Bundle, k, b"m".to_vec(), src(&[1u8; 1000])).await;
        }
        let (deleted, bytes) = shared.prune().await.unwrap();
        assert!(deleted >= 2 && bytes <= 3 * entry, "{deleted} {bytes}");
        assert_eq!(shared.prune().await.unwrap().0, 0);
        assert!(s3.cache_get("v1/pointers/git-x").await.unwrap().is_some(), "pointers stay");
    }
}
