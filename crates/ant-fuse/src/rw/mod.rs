//! Writable lazy workspaces: a copy-on-write layer implemented inside our
//! FUSE filesystem (no kernel overlayfs, no privileges needed).
//!
//! The state directory lives outside the mountpoint (the CLI uses
//! `~/.cache/antimony/ws/<hash of mountpoint>/`) and holds (`state`):
//! - `upper/`: the repo tree of every file or directory created or modified
//!   (copy-up on first write copies the whole file from the lower layer);
//! - `whiteouts`: newline-separated repo paths deleted from the lower layer
//!   (a deleted directory hides everything below it);
//! - `base`: `base_commit=<hex>\nbase_tree=<hex>\n`;
//! - `intent`: a pending delete/rename, rolled forward after a crash;
//! - `lock` / `mount.lock`: the state lock and the exclusive mount marker.
//!
//! Every metadata file is replaced atomically (write temp + rename + fsync).
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
mod overlay;
mod state;

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;

use ant_core::{EntryMode, ObjectId};
use ant_store::ObjectStore;
use fuser::{BackgroundSession, Config, Errno, FileType, INodeNo, MountOption};
use parking_lot::Mutex;

use crate::MountOptions;
use crate::filesystem::View;
pub use changes::{changes, reset};
use overlay::{Overlay, Writable};
pub use state::base;
use state::{MountGuard, StateLock, recover, save_base, save_whiteouts};

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
    let (_, root) = base(&state_dir)?;
    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::RW,
        MountOption::DefaultPermissions,
        MountOption::FSName(opts.fs_name.clone()),
    ];
    let view = Writable {
        _mount_lock: mount_lock,
        lower: View::new(objects, root, rt, opts),
        dir: state_dir,
        overlay: Mutex::new(Overlay {
            paths: HashMap::from([(INodeNo(1), String::new())]),
            handles: HashMap::new(),
            directories: HashMap::new(),
            next_handle: 1,
        }),
    };
    drop(lock);
    fuser::spawn_mount(view, mountpoint, &config)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod operation_tests;
