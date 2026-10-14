//! Writable lazy workspaces: a copy-on-write layer implemented inside our
//! FUSE filesystem (no kernel overlayfs, no privileges needed).
//!
//! The state directory lives outside the mountpoint (the CLI uses
//! `~/.cache/antimony/ws/<hash of mountpoint>/`) and holds (`state`):
//! - `upper/`: the repo tree of every file or directory created or modified
//!   (copy-up on first write copies the whole file from the lower layer);
//! - `whiteouts`: snapshot of the repo paths deleted from the lower layer,
//!   newline-separated (a deleted directory hides everything below it);
//! - `base`: snapshot `base_commit=<hex>\nbase_tree=<hex>\n`;
//! - `ops.log`: append-only journal of whiteout and base changes since the
//!   snapshots (`journal`: checksummed records, compacted into the snapshots
//!   at mount, at unmount and past 4 MiB);
//! - `intent`: a pending delete/rename written by earlier versions, rolled
//!   forward by recovery;
//! - `lock` / `mount.lock`: the state lock and the exclusive mount marker.
//!
//! Snapshots are replaced atomically (write temp + fsync + rename + fsync).
//!
//! # Performance model
//!
//! The mount is exclusive (`MountGuard`): recovery runs and base/whiteouts
//! are loaded once at mount and kept in memory. `read`/`write`/`fsync` on an
//! open upper file use positional I/O on the handle's own descriptor, without
//! the overlay mutex or the state lock, on several FUSE threads. A write is
//! acknowledged before its copy into the upper file (`overlay::Pending`), so
//! a sequential writer's next request overlaps it; reads, attributes, flush,
//! fsync and truncation of that inode wait for writes in flight, and
//! overlapping writes apply in order. `status` reads upper files directly, so
//! it sees a write once that copy completes (microseconds after the reply),
//! and always once the file is closed or synced. Namespace operations
//! serialize on the overlay mutex; mutations also hold the state lock so
//! `status`/`reset` (other processes) see whole operations. `reset` on a
//! live mount appends a journal record; the daemon notices the journal size
//! change with one `fstat` per namespace operation and reloads. If `reset`
//! prunes an upper file that is still open for writing, the next write,
//! flush or fsync on it sees `nlink == 0` and publishes the file again, with
//! its writes.
//!
//! # Crash semantics
//!
//! - **Daemon crash** (killed, panics; the kernel survives): the workspace
//!   reflects every completed metadata operation, i.e. a prefix of them, and
//!   the interrupted one is either applied or not (a trailing journal rename
//!   is rolled forward; a torn journal record is dropped). File data is kept
//!   by the kernel page cache once its file was closed (FUSE flush) or
//!   synced; the last acknowledged writes to a file still open may be lost
//!   or partially applied, like any unsynced data.
//! - **System crash**: metadata changes are made durable in batches, by an
//!   explicit `fsync`/`fsyncdir`, at unmount, and at most 100 ms later by a
//!   background flusher; only changes acknowledged after the last such point
//!   may be lost. Journal records and upper-directory changes are synced in
//!   order (pending records before the next directory change and vice
//!   versa), so what survives is a prefix of the operations. File data
//!   written without `fsync` may be lost, as on any POSIX filesystem.
//! - **Never torn**: no operation leaves a file whose content is neither the
//!   old nor the new version, unless the application itself wrote it
//!   partially. Copy-up writes a temp file, fsyncs it and renames it into
//!   place, so a published upper file never shadows the lower file with a
//!   partial copy. Compound operations (deleting a modified tracked file,
//!   renaming a tracked path) journal their whiteouts first, so an
//!   interruption leaves the upper copy, i.e. the old state, or is rolled
//!   forward.
//!
//! Reads: upper wins, whiteouts hide the lower layer, everything else is the
//! lazy read-only view (`crate::filesystem`). Supported mutations
//! (`overlay`, `callbacks`): create, write, truncate, chmod (only the exec bit
//! is meaningful), mkdir, rmdir (ENOTEMPTY across layers), unlink, rename of
//! files and directories (a lower directory is copied up recursively),
//! symlink, readlink, fsync and utimens; hard links are EPERM. Inode numbers
//! stay stable across copy-up for the life of the mount.
//!
//! [`changes`] lists the workspace's changes (sorted, files byte-identical to
//! the base are not changes) from the upper directory and whiteouts only,
//! reading just the base trees on affected paths; [`reset`] moves the
//! workspace to a new base, dropping upper entries that now match it.

mod callbacks;
mod changes;
mod journal;
mod overlay;
mod state;

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;

use ant_core::{EntryMode, ObjectId};
use ant_store::ObjectStore;
use fuser::{BackgroundSession, Config, Errno, FileType, MountOption};

use crate::MountOptions;
use crate::filesystem::View;
pub use changes::{changes, reset};
use overlay::Writable;
pub use state::base;
use state::{MountGuard, StateLock, recover, save_base, save_whiteouts};

/// FUSE request threads per writable mount. Namespace operations serialize
/// on the overlay; reads and writes of open upper files run in parallel.
const THREADS: usize = 8;

fn error(code: i32) -> io::Error {
    io::Error::from_raw_os_error(code)
}
fn fuse_error(e: Errno) -> io::Error {
    error(e.code())
}
fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

fn under(path: &str, prefix: &str) -> bool {
    path == prefix || path.strip_prefix(prefix).is_some_and(|s| s.starts_with('/'))
}
fn hidden(paths: &BTreeSet<String>, path: &str) -> bool {
    paths.contains(path) || path.match_indices('/').any(|(i, _)| paths.contains(&path[..i]))
}
fn metadata(path: &Path) -> io::Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(m) => Ok(Some(m)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
fn mode(m: &fs::Metadata) -> EntryMode {
    if m.is_dir() {
        EntryMode::Tree
    } else if m.file_type().is_symlink() {
        EntryMode::Symlink
    } else if m.mode() & 0o111 != 0 {
        EntryMode::Executable
    } else {
        EntryMode::File
    }
}
fn file_kind(m: EntryMode) -> FileType {
    match m {
        EntryMode::Tree | EntryMode::Gitlink => FileType::Directory,
        EntryMode::Symlink => FileType::Symlink,
        _ => FileType::RegularFile,
    }
}

/// Mount a persistent copy-on-write workspace. `state_dir` must be outside
/// `mountpoint`. Existing state wins over the supplied initial base. Like
/// `mount`, this blocks and requires a live multi-thread Tokio runtime.
///
/// Do not `fork` (spawn helper processes) from the same process while its own
/// threads do I/O on this mount: the kernel can hand a concurrent direct-I/O
/// read stale buffer contents (see `KERNEL_GATE`). Daemon and clients in
/// separate processes, as the CLI does it, are unaffected.
pub fn mount_rw(
    objects: Arc<dyn ObjectStore>,
    base_commit: ObjectId,
    base_tree: ObjectId,
    mountpoint: &Path,
    state_dir: &Path,
    rt: tokio::runtime::Handle,
    opts: MountOptions,
) -> io::Result<BackgroundSession> {
    if rt.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
        return Err(other("ant-fuse requires a multi-threaded Tokio runtime"));
    }
    fs::create_dir_all(state_dir)?;
    let state_dir = fs::canonicalize(state_dir)?;
    if state_dir.starts_with(fs::canonicalize(mountpoint)?) {
        return Err(other("state directory must be outside mountpoint"));
    }
    let mount_lock = MountGuard::take(&state_dir)?;
    let lock = StateLock::take(&state_dir)?;
    fs::create_dir_all(state_dir.join("upper"))?;
    if !state_dir.join("base").exists() {
        save_base(&state_dir, base_commit, base_tree)?;
    }
    if !state_dir.join("whiteouts").exists() {
        save_whiteouts(&state_dir, &BTreeSet::new())?;
    }
    recover(&state_dir)?;
    let root = journal::load(&state_dir)?.tree;
    drop(lock);
    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::RW,
        MountOption::DefaultPermissions,
        MountOption::FSName(opts.fs_name.clone()),
    ];
    config.n_threads = Some(THREADS);
    config.clone_fd = true;
    let lower = View::new(objects, root, rt, opts);
    let view = Writable::open(mount_lock, lower, state_dir)?;
    fuser::spawn_mount(view, mountpoint, &config)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod operation_tests;
