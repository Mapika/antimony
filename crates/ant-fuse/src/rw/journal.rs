//! The metadata journal (`ops.log`) and batched durability.
//!
//! Whiteout changes are appended as length-prefixed, checksummed records
//! instead of rewriting `whiteouts` (temp + fsync + rename + dir fsync) per
//! operation. Readers replay the journal over the `base`/`whiteouts`
//! snapshots and stop at the first torn or corrupt frame; the mount truncates
//! such a tail before appending. The journal is folded into the snapshots
//! (compaction) at mount, at unmount and when it exceeds [`COMPACT_BYTES`].
//!
//! Record kinds: whiteout (1), rename (2), done (3), reset (4) and update
//! (5, `ant ws update`; binaries from before it refuse a journal holding one,
//! which only exists while an update is unfinished or not yet compacted).
//!
//! Frame: `len: u32 LE | crc32(len ++ payload): u32 LE | payload`, where the
//! payload is a tag byte and the record's fields (`u32 LE` length-prefixed
//! strings, raw 32-byte ids). An unknown tag behind a valid checksum is an
//! error, never silently dropped.
//!
//! Durability is batched ([`Journal`]): appends and upper-directory changes
//! are made durable by explicit `fsync`/`fsyncdir`, at unmount and by a
//! background flusher at most [`FLUSH_INTERVAL`] later. To keep the persisted
//! state a prefix of the operations, the two streams are ordered: pending
//! journal records are synced before the next upper-directory change, and
//! dirtied directories before the next journal record. Workloads that only
//! create/write/rename/remove their own files (builds) or only delete tracked
//! files never pay a synchronous fsync.

use std::collections::{BTreeSet, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ant_core::{ObjectId, RepoPath};
use parking_lot::{Condvar, Mutex};

use super::other;
use super::state::{base, save_base, save_whiteouts, snapshot_whiteouts};

pub(super) const JOURNAL: &str = "ops.log";
/// Compact when the journal grows past this size.
pub(super) const COMPACT_BYTES: u64 = 4 << 20;
/// Longest time an acknowledged metadata change stays volatile.
pub(super) const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

const TAG_WHITEOUT: u8 = 1;
const TAG_RENAME: u8 = 2;
const TAG_DONE: u8 = 3;
const TAG_RESET: u8 = 4;
const TAG_UPDATE: u8 = 5;
const MAX_RECORD: usize = 1 << 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Record {
    /// Hide a lower-layer path.
    Whiteout(String),
    /// Whiteout both paths, then move `upper/old` to `upper/new`. Completed
    /// by a following [`Record::Done`]; a trailing `Rename` is rolled forward
    /// by recovery.
    Rename {
        old: String,
        new: String,
    },
    Done,
    /// A new base with its complete whiteout set (written by `reset`).
    Reset {
        commit: ObjectId,
        tree: ObjectId,
        whiteouts: Vec<String>,
    },
    /// `ant ws update` (see `update`): a new base with its complete whiteout
    /// set, staged upper files to move into place (`writes[i]` comes from
    /// `staging/<i>`), upper files to remove and the resulting conflicts.
    /// Completed by a following [`Record::Done`]; a trailing `Update` is
    /// rolled forward by recovery.
    Update(Box<UpdateRecord>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct UpdateRecord {
    pub(super) commit: ObjectId,
    pub(super) tree: ObjectId,
    pub(super) whiteouts: Vec<String>,
    pub(super) writes: Vec<String>,
    pub(super) removes: Vec<String>,
    pub(super) conflicts: Vec<String>,
}

const fn crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}
static CRC_TABLE: [u32; 256] = crc_table();
fn crc32(parts: &[&[u8]]) -> u32 {
    let mut c = !0u32;
    for part in parts {
        for &b in *part {
            c = CRC_TABLE[((c ^ u32::from(b)) & 0xff) as usize] ^ (c >> 8);
        }
    }
    !c
}

fn put_paths(out: &mut Vec<u8>, paths: &[String]) {
    out.extend_from_slice(&(paths.len() as u32).to_le_bytes());
    for path in paths {
        put_str(out, path);
    }
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(other("truncated journal record"));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }
    fn u32(&mut self) -> io::Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn path(&mut self) -> io::Result<String> {
        let n = self.u32()? as usize;
        let s = std::str::from_utf8(self.take(n)?).map_err(other)?;
        if RepoPath::new(s).map_err(other)?.is_root() {
            return Err(other("journal record names the root"));
        }
        Ok(s.to_owned())
    }
    fn paths(&mut self) -> io::Result<Vec<String>> {
        let n = self.u32()? as usize;
        let mut out = Vec::with_capacity(n.min(1 << 16));
        for _ in 0..n {
            out.push(self.path()?);
        }
        Ok(out)
    }
    fn id(&mut self) -> io::Result<ObjectId> {
        let b = self.take(32)?;
        let mut id = [0u8; 32];
        id.copy_from_slice(b);
        Ok(ObjectId(id))
    }
}

impl Record {
    pub(super) fn frame(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        match self {
            Record::Whiteout(path) => {
                payload.push(TAG_WHITEOUT);
                put_str(&mut payload, path);
            }
            Record::Rename { old, new } => {
                payload.push(TAG_RENAME);
                put_str(&mut payload, old);
                put_str(&mut payload, new);
            }
            Record::Done => payload.push(TAG_DONE),
            Record::Reset { commit, tree, whiteouts } => {
                payload.push(TAG_RESET);
                payload.extend_from_slice(commit.as_bytes());
                payload.extend_from_slice(tree.as_bytes());
                put_paths(&mut payload, whiteouts);
            }
            Record::Update(u) => {
                payload.push(TAG_UPDATE);
                payload.extend_from_slice(u.commit.as_bytes());
                payload.extend_from_slice(u.tree.as_bytes());
                for paths in [&u.whiteouts, &u.writes, &u.removes, &u.conflicts] {
                    put_paths(&mut payload, paths);
                }
            }
        }
        let len = (payload.len() as u32).to_le_bytes();
        let mut frame = Vec::with_capacity(payload.len() + 8);
        frame.extend_from_slice(&len);
        frame.extend_from_slice(&crc32(&[&len, &payload]).to_le_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    /// The base this record switches to, if any.
    pub(super) fn base(&self) -> Option<(ObjectId, ObjectId)> {
        match self {
            Record::Reset { commit, tree, .. } => Some((*commit, *tree)),
            Record::Update(u) => Some((u.commit, u.tree)),
            _ => None,
        }
    }

    /// Apply the record's effect on the whiteout set.
    pub(super) fn apply(self, deleted: &mut BTreeSet<String>) {
        match self {
            Record::Whiteout(path) => {
                deleted.insert(path);
            }
            Record::Rename { old, new } => {
                deleted.insert(old);
                deleted.insert(new);
            }
            Record::Done => {}
            Record::Reset { whiteouts, .. } => *deleted = whiteouts.into_iter().collect(),
            Record::Update(u) => *deleted = u.whiteouts.into_iter().collect(),
        }
    }

    fn decode(payload: &[u8]) -> io::Result<Record> {
        let mut r = Reader(payload);
        let record = match r.take(1)?[0] {
            TAG_WHITEOUT => Record::Whiteout(r.path()?),
            TAG_RENAME => Record::Rename { old: r.path()?, new: r.path()? },
            TAG_DONE => Record::Done,
            TAG_RESET => Record::Reset { commit: r.id()?, tree: r.id()?, whiteouts: r.paths()? },
            TAG_UPDATE => Record::Update(Box::new(UpdateRecord {
                commit: r.id()?,
                tree: r.id()?,
                whiteouts: r.paths()?,
                writes: r.paths()?,
                removes: r.paths()?,
                conflicts: r.paths()?,
            })),
            tag => return Err(other(format!("unsupported journal record kind {tag}"))),
        };
        if !r.0.is_empty() {
            return Err(other("trailing bytes in journal record"));
        }
        Ok(record)
    }
}

/// Decode the valid prefix of a journal image: returns its records and its
/// length in bytes. A short, zero-length or checksum-failing frame is a torn
/// tail (an append cut short by a crash) and ends the journal.
pub(super) fn parse(data: &[u8]) -> io::Result<(Vec<Record>, usize)> {
    let mut records = Vec::new();
    let mut pos = 0;
    while data.len() - pos >= 8 {
        let len_bytes = &data[pos..pos + 4];
        let len =
            u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        let crc = u32::from_le_bytes([data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7]]);
        if len == 0 || len > MAX_RECORD || data.len() - pos - 8 < len {
            break;
        }
        let payload = &data[pos + 8..pos + 8 + len];
        if crc32(&[len_bytes, payload]) != crc {
            break;
        }
        records.push(Record::decode(payload)?);
        pos += 8 + len;
    }
    Ok((records, pos))
}

pub(super) fn read_journal(state: &Path) -> io::Result<Vec<u8>> {
    match fs::read(state.join(JOURNAL)) {
        Ok(data) => Ok(data),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

/// The persisted workspace state: snapshots plus the journal's valid prefix.
pub(super) struct Persisted {
    pub(super) commit: ObjectId,
    pub(super) tree: ObjectId,
    pub(super) deleted: BTreeSet<String>,
    /// Length of the journal's valid prefix.
    pub(super) len: u64,
}

pub(super) fn load(state: &Path) -> io::Result<Persisted> {
    let (mut commit, mut tree) = base(state)?;
    let mut deleted = snapshot_whiteouts(state)?;
    let (records, len) = parse(&read_journal(state)?)?;
    for record in records {
        if let Some(base) = record.base() {
            (commit, tree) = base;
        }
        record.apply(&mut deleted);
    }
    Ok(Persisted { commit, tree, deleted, len: len as u64 })
}

/// Append one record durably (for `reset` and recovery, outside a mount's
/// [`Journal`]). The caller holds the state lock.
pub(super) fn append_durably(state: &Path, record: &Record) -> io::Result<()> {
    let mut f = OpenOptions::new().create(true).append(true).open(state.join(JOURNAL))?;
    f.write_all(&record.frame())?;
    f.sync_data()
}

/// Fold the journal into the snapshots and empty it, under the state lock.
/// Replaying a journal over a snapshot that already contains it yields the
/// same state, so a crash between the two steps is harmless. The journal is
/// truncated in place: a mounted daemon keeps appending to the same file.
pub(super) fn compact(state: &Path, journal: &File) -> io::Result<Persisted> {
    let mut p = load(state)?;
    if p.len == 0 && journal.metadata()?.len() == 0 {
        return Ok(p);
    }
    if snapshot_whiteouts(state)? != p.deleted {
        save_whiteouts(state, &p.deleted)?;
    }
    if base(state)? != (p.commit, p.tree) {
        save_base(state, p.commit, p.tree)?;
    }
    journal.set_len(0)?;
    journal.sync_data()?;
    p.len = 0;
    Ok(p)
}

#[derive(Default)]
struct Durable {
    journal_written: u64,
    journal_synced: u64,
    dirs: HashSet<PathBuf>,
    dirs_written: u64,
    dirs_synced: u64,
}

/// A mounted workspace's open journal, its state-lock descriptor and the
/// pending-durability bookkeeping shared with the background flusher.
pub(super) struct Journal {
    file: File,
    lock: File,
    durable: Mutex<Durable>,
    sync_lock: Mutex<()>,
    stopped: Mutex<bool>,
    wake: Condvar,
}

/// Exclusive `flock` on the state lock, released on drop.
pub(super) struct Flock<'a>(&'a File);
impl Drop for Flock<'_> {
    fn drop(&mut self) {
        // SAFETY: the descriptor is valid for the guard's lifetime.
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

impl Journal {
    pub(super) fn open(state: &Path) -> io::Result<Self> {
        let file =
            OpenOptions::new().create(true).read(true).append(true).open(state.join(JOURNAL))?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(state.join("lock"))?;
        Ok(Self {
            file,
            lock,
            durable: Mutex::new(Durable::default()),
            sync_lock: Mutex::new(()),
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        })
    }

    pub(super) fn file(&self) -> &File {
        &self.file
    }

    /// Take the state lock through this mount's own descriptor (a separate
    /// open file description from `StateLock`, so the two exclude each other).
    pub(super) fn lock(&self) -> io::Result<Flock<'_>> {
        // SAFETY: flock only consumes the valid descriptor.
        if unsafe { libc::flock(self.lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Flock(&self.lock))
    }

    /// Current size of the journal file (one `fstat`).
    pub(super) fn size(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// Append a record; `len` tracks the journal size this mount has seen.
    pub(super) fn append(&self, len: &mut u64, record: &Record) -> io::Result<()> {
        self.before_journal()?;
        let frame = record.frame();
        if let Err(e) = (&self.file).write_all(&frame) {
            // Never leave a partial frame in front of later records.
            let _ = self.file.set_len(*len);
            return Err(e);
        }
        *len += frame.len() as u64;
        self.durable.lock().journal_written += 1;
        Ok(())
    }

    /// The journal was emptied by compaction; its old contents live on in
    /// durable snapshots.
    pub(super) fn compacted(&self) {
        let mut d = self.durable.lock();
        d.journal_synced = d.journal_written;
    }

    /// Record that `path` was created, removed or renamed in the upper layer.
    pub(super) fn dirty(&self, path: &Path) {
        if let Some(parent) = path.parent() {
            let mut d = self.durable.lock();
            d.dirs.insert(parent.to_owned());
            d.dirs_written += 1;
        }
    }

    /// Call before changing the upper directory: earlier journal records
    /// must not be outlived by later directory changes.
    pub(super) fn before_physical(&self) -> io::Result<()> {
        let pending = {
            let d = self.durable.lock();
            d.journal_written > d.journal_synced
        };
        if pending { self.sync() } else { Ok(()) }
    }

    fn before_journal(&self) -> io::Result<()> {
        let pending = {
            let d = self.durable.lock();
            d.dirs_written > d.dirs_synced
        };
        if pending { self.sync() } else { Ok(()) }
    }

    /// Make every acknowledged journal record and upper-directory change
    /// durable. Concurrent callers wait for an in-flight sync, then sync
    /// whatever it did not cover.
    pub(super) fn sync(&self) -> io::Result<()> {
        let _sync = self.sync_lock.lock();
        let (journal, synced, dirs, dirs_written) = {
            let mut d = self.durable.lock();
            (d.journal_written, d.journal_synced, std::mem::take(&mut d.dirs), d.dirs_written)
        };
        let result = (|| {
            if journal > synced {
                self.file.sync_data()?;
            }
            for dir in &dirs {
                match File::open(dir) {
                    Ok(f) => f.sync_all()?,
                    // Removed since; its parent was dirtied by the removal.
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        })();
        let mut d = self.durable.lock();
        match result {
            Ok(()) => {
                d.journal_synced = d.journal_synced.max(journal);
                d.dirs_synced = d.dirs_synced.max(dirs_written);
            }
            Err(_) => d.dirs.extend(dirs),
        }
        result
    }

    fn pending(&self) -> bool {
        let d = self.durable.lock();
        d.journal_written > d.journal_synced || d.dirs_written > d.dirs_synced
    }

    /// Background flusher: syncs pending changes at most `FLUSH_INTERVAL`
    /// after they were made, until [`Journal::stop`].
    pub(super) fn spawn_flusher(self: &Arc<Self>) -> io::Result<std::thread::JoinHandle<()>> {
        let journal = self.clone();
        std::thread::Builder::new().name("ant-rw-flush".into()).spawn(move || {
            let mut stopped = journal.stopped.lock();
            while !*stopped {
                journal.wake.wait_for(&mut stopped, FLUSH_INTERVAL);
                if *stopped {
                    break;
                }
                drop(stopped);
                if journal.pending()
                    && let Err(error) = journal.sync()
                {
                    tracing::error!(%error, "lazy workspace flush failed");
                }
                stopped = journal.stopped.lock();
            }
        })
    }

    pub(super) fn stop(&self) {
        *self.stopped.lock() = true;
        self.wake.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records() -> Vec<Record> {
        vec![
            Record::Whiteout("a/b".into()),
            Record::Rename { old: "dir".into(), new: "moved".into() },
            Record::Done,
            Record::Reset {
                commit: ObjectId([3; 32]),
                tree: ObjectId([4; 32]),
                whiteouts: vec!["x".into(), "y/z".into()],
            },
            Record::Whiteout("über".into()),
            Record::Update(Box::new(UpdateRecord {
                commit: ObjectId([7; 32]),
                tree: ObjectId([8; 32]),
                whiteouts: vec!["w".into()],
                writes: vec!["a/merged".into(), "b".into()],
                removes: vec![],
                conflicts: vec!["b".into()],
            })),
            Record::Done,
        ]
    }

    #[test]
    fn round_trip_and_every_torn_tail_is_a_prefix() {
        let records = records();
        let mut image = Vec::new();
        let mut ends = vec![0];
        for r in &records {
            image.extend_from_slice(&r.frame());
            ends.push(image.len());
        }
        assert_eq!(parse(&image).unwrap(), (records.clone(), image.len()));
        // Cut anywhere: the result is the longest complete prefix.
        for cut in 0..image.len() {
            let (got, len) = parse(&image[..cut]).unwrap();
            let complete = ends.iter().rposition(|&e| e <= cut).unwrap();
            assert_eq!(len, ends[complete], "cut {cut}");
            assert_eq!(got, records[..complete], "cut {cut}");
        }
        // Zero-filled tail (size extended before data reached the disk).
        let mut zeros = image.clone();
        zeros.extend_from_slice(&[0; 64]);
        assert_eq!(parse(&zeros).unwrap(), (records.clone(), image.len()));
        // A flipped bit ends the journal at that record.
        for at in 0..image.len() {
            let mut bad = image.clone();
            bad[at] ^= 0x10;
            let (got, len) = parse(&bad).unwrap();
            let first_bad = ends.iter().rposition(|&e| e <= at).unwrap();
            assert_eq!(len, ends[first_bad], "flip at {at}");
            assert_eq!(got, records[..first_bad]);
        }
    }

    #[test]
    fn unknown_kind_behind_valid_checksum_is_an_error() {
        let payload = [99u8, 1, 2];
        let len = (payload.len() as u32).to_le_bytes();
        let mut image = Vec::new();
        image.extend_from_slice(&len);
        image.extend_from_slice(&crc32(&[&len, &payload]).to_le_bytes());
        image.extend_from_slice(&payload);
        assert!(parse(&image).is_err());
    }

    #[test]
    fn load_replays_over_snapshots_and_compaction_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path();
        save_base(state, ObjectId([1; 32]), ObjectId([2; 32])).unwrap();
        save_whiteouts(state, &BTreeSet::from(["old".to_string()])).unwrap();
        for r in [
            Record::Whiteout("a".into()),
            Record::Reset {
                commit: ObjectId([5; 32]),
                tree: ObjectId([6; 32]),
                whiteouts: vec!["kept".into()],
            },
            Record::Whiteout("b".into()),
        ] {
            append_durably(state, &r).unwrap();
        }
        let p = load(state).unwrap();
        assert_eq!((p.commit, p.tree), (ObjectId([5; 32]), ObjectId([6; 32])));
        assert_eq!(p.deleted, BTreeSet::from(["kept".into(), "b".into()]));
        // A crash after writing the snapshots but before truncating replays
        // the journal over its own result: same state.
        let journal = fs::read(state.join(JOURNAL)).unwrap();
        let file = OpenOptions::new().read(true).append(true).open(state.join(JOURNAL)).unwrap();
        compact(state, &file).unwrap();
        assert_eq!(fs::metadata(state.join(JOURNAL)).unwrap().len(), 0);
        fs::write(state.join(JOURNAL), &journal).unwrap();
        let again = load(state).unwrap();
        assert_eq!((again.commit, again.tree, again.deleted), (p.commit, p.tree, p.deleted));
        assert_eq!(base(state).unwrap(), (ObjectId([5; 32]), ObjectId([6; 32])));
    }
}
