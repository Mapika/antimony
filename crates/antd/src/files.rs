//! Helpers for workspace files on disk, shared by sparse workspaces
//! ([`crate::workspace`]) and lazy ones (`ant_fuse::rw`).

use std::fs;
use std::io;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use ant_core::EntryMode;

/// `path` equals `prefix` or lies beneath it (repo paths, `/`-separated).
pub fn is_under(path: &str, prefix: &str) -> bool {
    path == prefix || path.strip_prefix(prefix).is_some_and(|rest| rest.starts_with('/'))
}

/// The tree entry mode of a file on disk (`meta` from `symlink_metadata`):
/// directories are trees, any exec bit makes a file executable.
pub fn entry_mode(meta: &fs::Metadata) -> EntryMode {
    if meta.is_dir() {
        EntryMode::Tree
    } else if meta.file_type().is_symlink() {
        EntryMode::Symlink
    } else if meta.mode() & 0o111 != 0 {
        EntryMode::Executable
    } else {
        EntryMode::File
    }
}

/// The blob body of a file or symlink on disk: its content, or the link
/// target for a symlink.
pub fn read_body(path: &Path, meta: &fs::Metadata) -> io::Result<Vec<u8>> {
    if meta.file_type().is_symlink() {
        Ok(fs::read_link(path)?.into_os_string().into_vec())
    } else {
        fs::read(path)
    }
}

/// Permission bits a workspace gives a file of `mode`.
pub fn file_perm(mode: EntryMode) -> u32 {
    if mode == EntryMode::Executable { 0o755 } else { 0o644 }
}
