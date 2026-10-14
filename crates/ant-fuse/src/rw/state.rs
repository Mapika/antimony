//! Persistent workspace state: locks, the `base`/`whiteouts` snapshots and
//! crash recovery (the journal itself is in `journal`).

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use ant_core::{ObjectId, RepoPath};
use parking_lot::Mutex;

use super::journal::{JOURNAL, Record, append_durably, parse, read_journal};
use super::{metadata, other};

// Separate open file descriptions make flock serialize the daemon, status and
// reset, including callers in the same process. Never held on a Tokio worker.
pub(super) struct StateLock(File);
impl StateLock {
    pub(super) fn take(dir: &Path) -> io::Result<Self> {
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("lock"))?;
        // SAFETY: flock only consumes the valid descriptor, with no pointers.
        if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(f))
    }
}
impl Drop for StateLock {
    fn drop(&mut self) {
        // SAFETY: the file is still alive here.
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

// Exclusive "this state dir is mounted" marker. Within a process an exact
// registry decides, so the answer is correct the instant a previous mount's
// filesystem is dropped (i.e. once `umount_and_join` returns). Across processes
// a POSIX record lock decides. Deliberately not flock/OFD: those belong to the
// open file description, and another thread that forks (to spawn fusermount, or
// anything else) holds a copy of that description until it execs, so the lock
// would spuriously still look held right after unmount. Record locks are never
// inherited by a forked child. They are dropped when any descriptor of the file
// closes in this process, which is why only this type ever opens `mount.lock`.
static MOUNTED: std::sync::LazyLock<Mutex<std::collections::HashSet<PathBuf>>> =
    std::sync::LazyLock::new(Default::default);
pub(super) struct MountGuard {
    file: Option<File>,
    dir: PathBuf,
}
impl MountGuard {
    pub(super) fn take(dir: &Path) -> io::Result<Self> {
        let busy = || other("lazy workspace is already mounted");
        if !MOUNTED.lock().insert(dir.to_owned()) {
            return Err(busy());
        }
        // From here on the guard owns the registry entry, released on drop.
        let mut guard = Self { file: None, dir: dir.to_owned() };
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("mount.lock"))?;
        // SAFETY: an all-zero flock is valid; the fields we need are set below.
        let mut lock: libc::flock = unsafe { std::mem::zeroed() };
        lock.l_type = libc::F_WRLCK as _;
        lock.l_whence = libc::SEEK_SET as _;
        // SAFETY: the descriptor is valid and `lock` outlives the call.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &lock) } != 0 {
            return Err(match io::Error::last_os_error().raw_os_error() {
                Some(libc::EACCES | libc::EAGAIN) => busy(),
                _ => io::Error::last_os_error(),
            });
        }
        guard.file = Some(file);
        Ok(guard)
    }
}
impl Drop for MountGuard {
    fn drop(&mut self) {
        // Release the record lock before the registry entry, so the next
        // mount never sees a stale lock.
        self.file = None;
        MOUNTED.lock().remove(&self.dir);
    }
}

pub(super) fn atomic(dir: &Path, name: &str, data: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!(".{name}.tmp"));
    let mut f = File::create(&tmp)?;
    f.write_all(data)?;
    f.sync_all()?;
    fs::rename(tmp, dir.join(name))?;
    File::open(dir)?.sync_all()
}

/// Version of the state directory layout, recorded in `format` (`"1\n"`).
/// State directories from before the marker are version 1.
pub const STATE_FORMAT: u64 = 1;
const FORMAT: &str = "format";

/// Refuse a state directory written by a newer version (an `io::Error`
/// wrapping [`ant_core::NewerFormat`]).
pub(super) fn check_format(state: &Path) -> io::Result<()> {
    let text = match fs::read_to_string(state.join(FORMAT)) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let found = ant_core::version::parse_format_marker(&text)
        .ok_or_else(|| other(format!("corrupt lazy workspace format {text:?}")))?;
    ant_core::version::check_format(
        || format!("lazy workspace state {} format", state.display()),
        found,
        STATE_FORMAT,
    )
    .map_err(io::Error::other)
}

/// [`check_format`], then record the version if the marker is missing. The
/// caller holds the state lock.
pub(super) fn ensure_format(state: &Path) -> io::Result<()> {
    check_format(state)?;
    if !state.join(FORMAT).exists() {
        atomic(state, FORMAT, format!("{STATE_FORMAT}\n").as_bytes())?;
    }
    Ok(())
}

/// Read the persisted base (commit, tree).
pub fn base(state: &Path) -> io::Result<(ObjectId, ObjectId)> {
    check_format(state)?;
    let text = fs::read_to_string(state.join("base"))?;
    let get = |key: &str| -> io::Result<ObjectId> {
        text.lines()
            .find_map(|l| l.strip_prefix(key))
            .ok_or_else(|| other("invalid lazy workspace base"))?
            .parse()
            .map_err(other)
    };
    Ok((get("base_commit=")?, get("base_tree=")?))
}

pub(super) fn save_base(state: &Path, commit: ObjectId, tree: ObjectId) -> io::Result<()> {
    atomic(state, "base", format!("base_commit={commit}\nbase_tree={tree}\n").as_bytes())
}

/// The effective whiteouts: the snapshot with the journal replayed.
#[cfg(test)]
pub(super) fn whiteouts(state: &Path) -> io::Result<BTreeSet<String>> {
    let mut deleted = snapshot_whiteouts(state)?;
    let (records, _) = parse(&read_journal(state)?)?;
    for record in records {
        record.apply(&mut deleted);
    }
    Ok(deleted)
}
/// The `whiteouts` snapshot file alone.
pub(super) fn snapshot_whiteouts(state: &Path) -> io::Result<BTreeSet<String>> {
    let text = fs::read_to_string(state.join("whiteouts"))?;
    text.lines()
        .map(|p| {
            RepoPath::new(p).map_err(other)?;
            Ok(p.to_owned())
        })
        .collect()
}
pub(super) fn save_whiteouts(state: &Path, paths: &BTreeSet<String>) -> io::Result<()> {
    let mut text = paths.iter().cloned().collect::<Vec<_>>().join("\n");
    if !text.is_empty() {
        text.push('\n');
    }
    atomic(state, "whiteouts", text.as_bytes())
}
/// Bring the state directory to a consistent state after a crash, under the
/// state lock: roll forward a legacy `intent`, drop a torn journal tail and
/// roll forward a trailing journal rename. Idempotent; a no-op when clean.
pub(super) fn recover(state: &Path) -> io::Result<()> {
    // Every operation on the state directory starts here: never interpret
    // (or "repair") a layout from a newer version.
    check_format(state)?;
    recover_intent(state)?;
    let data = read_journal(state)?;
    if data.is_empty() {
        // An update that never journaled its record left at most staged
        // files: the workspace is still at the old base.
        return super::update::discard_staging(state);
    }
    let (records, len) = parse(&data)?;
    if len < data.len() {
        let f = OpenOptions::new().write(true).open(state.join(JOURNAL))?;
        f.set_len(len as u64)?;
        f.sync_data()?;
    }
    // `reset` journals the new base before rewriting the `base` snapshot
    // (which `ant` reads directly); finish an interrupted rewrite.
    let last_base = records.iter().rev().find_map(Record::base);
    if let Some((commit, tree)) = last_base
        && base(state)? != (commit, tree)
    {
        save_base(state, commit, tree)?;
    }
    // Records are appended in operation order and a rename or update is
    // followed by `Done` before anything else, so only the last record can
    // be unfinished.
    if let Some(Record::Update(update)) = records.last() {
        super::update::roll_forward(state, update)?;
        append_durably(state, &Record::Done)?;
        return Ok(());
    }
    super::update::discard_staging(state)?;
    if let Some(Record::Rename { old, new }) = records.last() {
        let old = state.join("upper").join(old);
        if metadata(&old)?.is_some() {
            let new = state.join("upper").join(new);
            fs::rename(&old, &new)?;
            sync_parent(&new)?;
            sync_parent(&old)?;
        }
        append_durably(state, &Record::Done)?;
    }
    Ok(())
}

// Written by earlier versions: one pending delete/rename, applied to the
// whiteouts snapshot and the upper directory, then removed.
fn recover_intent(state: &Path) -> io::Result<()> {
    let record = match fs::read_to_string(state.join("intent")) {
        Ok(s) => s,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let fields: Vec<_> = record.lines().collect();
    let (operation, paths) = fields.split_first().ok_or_else(|| other("empty mutation intent"))?;
    if !matches!((*operation, paths.len()), ("delete", 1) | ("rename", 2)) {
        return Err(other("invalid mutation intent"));
    }
    for path in paths {
        if RepoPath::new(*path).map_err(other)?.is_root() {
            return Err(other("root mutation intent"));
        }
    }
    let mut deleted = snapshot_whiteouts(state)?;
    deleted.extend(paths.iter().map(|p| p.to_string()));
    save_whiteouts(state, &deleted)?;
    let old = state.join("upper").join(paths[0]);
    if let Some(m) = metadata(&old)? {
        if *operation == "rename" {
            let new = state.join("upper").join(paths[1]);
            fs::rename(&old, &new)?;
            sync_parent(&new)?;
        } else if m.is_dir() {
            fs::remove_dir(&old)?;
        } else {
            fs::remove_file(&old)?;
        }
        sync_parent(&old)?;
    }
    fs::remove_file(state.join("intent"))?;
    File::open(state)?.sync_all()
}
pub(super) fn sync_parent(path: &Path) -> io::Result<()> {
    File::open(path.parent().ok_or_else(|| other("path has no parent"))?)?.sync_all()
}
