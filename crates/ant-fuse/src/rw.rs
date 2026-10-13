//! Writable lazy workspaces: a copy-on-write layer implemented inside our
//! FUSE filesystem (no kernel overlayfs, no privileges needed).
//!
//! TASK (rw-fuse): implement `mount_rw` and `changes`, then add
//! `ant ws new --lazy <dir>` (mounts in a detached background process that
//! survives the CLI; `ant ws unmount <dir>`), and make `status`/`submit`/`land`
//! work on lazy workspaces via `changes` (land's reset = replace `base_tree`
//! and clear the upper layer entries that now match the new base).
//!
//! Layout of the upper (state) directory, which must live OUTSIDE the
//! mountpoint (e.g. `~/.cache/antimony/ws/<hash of mountpoint>/`):
//! - `upper/` mirrors the repo tree for every file/dir created or modified
//!   (copy-up on first write: whole file copied from the lower layer).
//! - `whiteouts`: newline-separated repo paths deleted from the lower layer
//!   (a deleted dir hides everything below it). Persist durably (write temp +
//!   rename) after each mutation; tolerate crashes between ops.
//! - `base`: `base_commit=<hex>\nbase_tree=<hex>\n`.
//!
//! Semantics (POSIX enough for compilers, editors, git-like tools):
//! - Reads: upper wins, whiteouts hide lower, else lower (lazy, as in the
//!   read-only view; reuse its inode/caching code).
//! - Supported mutations: create, write, truncate/setattr size, chmod (only
//!   the exec bit is meaningful; store others), mkdir, rmdir (ENOTEMPTY
//!   honored across layers), unlink, rename (files and directories; a lower
//!   directory rename may copy up recursively in v1), symlink, readlink,
//!   fsync, utimens (mtime stored in upper). Hard links → EPERM.
//! - Editor pattern "write temp + rename over original" must work.
//! - Inode numbers stay stable across copy-up for the life of the mount.
//! - Concurrency: many processes reading/writing different files at once.
//!
//! `changes(objects, state_dir)` → `Vec<antd::workspace::LocalChange>`-shaped
//! results (path, kind, new (mode, blob id)), sorted by path, computed from the
//! upper dir + whiteouts only — cost proportional to the number of changed
//! paths, never to repo size. Files copied up but byte-identical to base (and
//! same mode) are NOT changes. New blob contents are put into `objects`.
//!
//! Tests (real FUSE mounts, multi-thread runtime, like `lib.rs` tests):
//! read-through of untouched files; edit-in-place; temp+rename; delete file;
//! delete dir then recreate a file inside; rename lower dir; chmod +x;
//! symlink create; `changes` matches the expected list in each case; a
//! 20k-file base tree where `changes` after editing 2 files reads ≤ a few
//! dozen objects (count via a wrapping ObjectStore); remount from the same
//! state dir preserves edits; concurrent writers (16 threads × 50 files).
