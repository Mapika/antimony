//! Sparse working copies.
//!
//! On-disk state lives in `{dir}/.ant/` (never reported as a change):
//! - `.ant/workspace`: `format=<n>` ([`WORKSPACE_FORMAT`], the version of
//!   the whole `.ant/` layout; absent = 1, workspaces from before it),
//!   `base_commit=<hex>`, `base_tree=<hex>`, and one `sparse=<pattern>` line
//!   per pattern. Plain text, one `key=value` per line. `open` refuses a
//!   newer format (`ant_core::NewerFormat`).
//! - `.ant/index`: one tab-separated line per materialized non-tree entry
//!   (path, mode, object id, size, mtime in ns, inode). After `update`, an
//!   entry may carry an all-zero stat: the path differs locally from the new
//!   base (it is re-hashed, or reported deleted when missing).
//! - `.ant/conflicts`: unresolved conflicts left by `update`, one escaped
//!   path per line (absent when there are none).
//! - `.ant/track` (written by the CLI): the ref `ant ws update` follows.
//!
//! Files: `index` (state files), `sparse` (patterns), `write`
//! (materializing entries), `scan` (`status`).
//!
//! Sparse patterns (`Sparse::matches`):
//! - Empty pattern list = everything.
//! - A pattern with no glob metacharacters (`*?[{`) is a path prefix:
//!   `src/foo` matches `src/foo` and everything under `src/foo/`.
//! - Otherwise it's a `globset` glob matched against the full path, with
//!   `literal_separator(true)` (so `*` doesn't cross `/`; use `**`).
//!
//! `materialize`:
//! - `dir` must not exist or be empty (else `AlreadyExists` io error).
//! - Walks the tree breadth-first, fetching each level's subtrees with one
//!   `get_many` call. With a [`DirFetch`] (`materialize_with`; the CLI
//!   passes its read-through store, which uses HTTP tree bundles when
//!   `--http` is set), trees of directories wholly inside the sparse scope
//!   are fetched with `DirFetch::get_dirs` instead, which may bring their
//!   blobs and subdirectories into the cache in the same round trip. Skips subtrees that cannot contain a match
//!   (`Sparse::may_contain_under(dir_path)`: true if any prefix pattern
//!   starts with or is under that dir, or any glob pattern exists — globs are
//!   not pruned).
//! - Fetches blobs in batches of 512 ids per `get_many`, several batches in
//!   flight at once (fetch, decode and file writes overlap). Tree levels are
//!   decoded in parallel and all needed directories are created up front,
//!   level by level in parallel. Each file is one `open` (mode set at
//!   creation), one `write` and one `fstat`. Files get the right mode
//!   (0o755 for Executable, 0o644 otherwise), symlinks via
//!   `std::os::unix::fs::symlink`, gitlinks as empty directories (not
//!   indexed).
//! - Every written entry is indexed with its post-write stat.
//!
//! `status` (no network; uses only the index + local hashing):
//! - Walks the workspace (skipping `.ant`), listing and stat'ing directories
//!   on several threads, one tree level at a time. For each file/symlink:
//!   indexed and (size, mtime_ns, ino, mode) unchanged → unchanged, without
//!   reading the file; otherwise read + hash as a blob and compare ids/mode.
//!   An entry whose recorded mtime is not older than the index file itself
//!   is "racy" (the file may have changed within the same timestamp tick as
//!   indexing) and is always re-hashed.
//! - Indexed paths missing on disk → `Deleted`.
//! - Unindexed files anywhere (even outside the sparse patterns) → `Added`
//!   (it's the agent's new file), unless an ignore rule hides them
//!   ([`crate::ignore`]: `.gitignore` files at any depth plus `.ant/ignore`).
//!   Indexed (tracked) files are always compared, ignored or not. An ignored
//!   directory is not entered unless it contains indexed files (so a huge
//!   `target/` costs one `readdir` of its parent), and its untracked
//!   contents are reported only by `status_with_ignored`.
//! - Empty directories are ignored. Output sorted by path.
//! - New/modified blob contents are stored into `objects` (so `submit` can
//!   upload them by id) — `LocalChange.new` carries their ids.
//!
//! `reset_to(objects, new_commit, new_tree)`: move the workspace to a new base
//! (e.g. after landing). Requires `status()` to be empty except for changes
//! that are already identical in `new_tree` (the just-landed changes);
//! otherwise returns an error naming the dirty paths. Applies
//! `diff_trees(old_base_tree, new_tree)` to the in-scope files on disk
//! (writes, deletes, removes now-empty dirs), then rewrites the index and
//! config.
//!
//! `update(objects, new_commit, new_tree)`: like `reset_to`, but carries
//! local changes over (`crate::update::plan`) and records conflicts.

mod index;
mod scan;
mod sparse;
mod write;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ant_core::EncodedObject;
use ant_core::{EntryMode, ObjectId, ObjectKind, RepoPath, hash_object};
use ant_store::ObjectStore;
use ant_store::graph::diff_trees;
use anyhow::{Context, anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;

use crate::par::par_map;
use index::{
    Entry, Index, STALE, atomic_write, index_mtime, read_index, save_state, stat_of, state_dir,
};
pub(crate) use index::{escape_path, unescape_path};
use scan::{put_blobs, scan};
pub use sparse::Sparse;
use write::{
    WriteItem, create_dirs, expand_tree, leaf_dirs, prepare_empty_dir, remove_leaf, write_items,
    write_leaves,
};

const STATE_DIR: &str = ".ant";
const CONFIG_FILE: &str = "workspace";
const INDEX_FILE: &str = "index";
const CONFLICTS_FILE: &str = "conflicts";
/// `.ant/ignore`: optional workspace-local ignore patterns (user-written).
const IGNORE_FILE: &str = "ignore";
/// Version of the `.ant/` state layout, the `format=` line of
/// `.ant/workspace` (absent in workspaces created before it: version 1).
pub const WORKSPACE_FORMAT: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalChangeKind {
    Added,
    Modified,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalChange {
    pub path: RepoPath,
    pub kind: LocalChangeKind,
    /// New entry for Added/Modified; blob is present in the local store.
    pub new: Option<(EntryMode, ObjectId)>,
}

/// Tree reads for a walk that goes on to read the trees' blobs and subtrees
/// (a full or prefix-sparse checkout of those directories).
#[async_trait]
pub trait DirFetch: Send + Sync {
    /// Like `ObjectStore::get_many(trees)` (same results, input order), but
    /// may also make the directories' contents local in the same round trip.
    async fn get_dirs(&self, trees: &[ObjectId]) -> ant_store::Result<Vec<Option<EncodedObject>>>;
}

#[derive(Debug)]
pub struct Workspace {
    dir: PathBuf,
    pub base_commit: ObjectId,
    pub base_tree: ObjectId,
    pub sparse: Sparse,
}

impl Workspace {
    pub async fn materialize(
        objects: &dyn ObjectStore,
        dir: &Path,
        base_commit: ObjectId,
        base_tree: ObjectId,
        sparse: Sparse,
    ) -> anyhow::Result<Self> {
        Self::materialize_with(objects, None, dir, base_commit, base_tree, sparse).await
    }

    /// [`materialize`](Self::materialize), reading the trees of directories
    /// wholly inside the sparse scope through `dirs` (e.g. HTTP tree
    /// bundles) when given.
    pub async fn materialize_with(
        objects: &dyn ObjectStore,
        dirs: Option<&dyn DirFetch>,
        dir: &Path,
        base_commit: ObjectId,
        base_tree: ObjectId,
        sparse: Sparse,
    ) -> anyhow::Result<Self> {
        let matcher = sparse.compile()?;

        let leaves = walk_leaves(objects, dirs, matcher, base_tree).await?;

        let d = dir.to_path_buf();
        let levels = leaf_dirs(&leaves);
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            prepare_empty_dir(&d)?;
            create_dirs(&d, &levels)
        })
        .await??;

        let mut index = Index::new();
        for (key, entry) in write_leaves(objects, dir, &leaves, true).await? {
            if let Some(e) = entry {
                index.insert(key, e);
            }
        }
        let mut ws = Self { dir: dir.to_path_buf(), base_commit, base_tree, sparse };
        ws.persist(base_commit, base_tree, index, None).await?;
        Ok(ws)
    }

    pub fn open(dir: &Path) -> anyhow::Result<Self> {
        let path = state_dir(dir).join(CONFIG_FILE);
        let text = fs::read_to_string(&path)
            .with_context(|| format!("reading workspace config {}", path.display()))?;
        // First, so a newer layout is refused before anything in it is
        // interpreted. No `format` line: written before it existed (1).
        if let Some(val) = text.lines().find_map(|l| l.strip_prefix("format=")) {
            let found = ant_core::version::parse_format_marker(val)
                .ok_or_else(|| anyhow!("corrupt workspace format {val:?}"))?;
            ant_core::version::check_format(
                || format!("workspace {} format", dir.display()),
                found,
                WORKSPACE_FORMAT,
            )?;
        }
        let mut base_commit = None;
        let mut base_tree = None;
        let mut patterns = Vec::new();
        for line in text.lines() {
            let Some((key, val)) = line.split_once('=') else {
                continue;
            };
            match key {
                "base_commit" => base_commit = Some(val.parse::<ObjectId>()?),
                "base_tree" => base_tree = Some(val.parse::<ObjectId>()?),
                "sparse" => patterns.push(val.to_string()),
                _ => {}
            }
        }
        let base_commit =
            base_commit.ok_or_else(|| anyhow!("workspace config has no base_commit"))?;
        let base_tree = base_tree.ok_or_else(|| anyhow!("workspace config has no base_tree"))?;
        Ok(Self { dir: dir.to_path_buf(), base_commit, base_tree, sparse: Sparse::new(patterns)? })
    }

    pub async fn status(&self, objects: &dyn ObjectStore) -> anyhow::Result<Vec<LocalChange>> {
        Ok(self.status_with_ignored(objects).await?.0)
    }

    /// [`status`](Self::status) plus the untracked paths the ignore rules
    /// hide (sorted; ignored directories end in `/`).
    pub async fn status_with_ignored(
        &self,
        objects: &dyn ObjectStore,
    ) -> anyhow::Result<(Vec<LocalChange>, Vec<String>)> {
        let dir = self.dir.clone();
        let found = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let index = read_index(&dir)?;
            let mtime = index_mtime(&dir)?;
            scan(&dir, &index, mtime)
        })
        .await??;
        put_blobs(objects, &found.blobs).await?;
        Ok((found.changes, found.ignored))
    }

    pub async fn reset_to(
        &mut self,
        objects: &dyn ObjectStore,
        new_commit: ObjectId,
        new_tree: ObjectId,
    ) -> anyhow::Result<()> {
        // One diff serves both the landed check and the apply step. Map each
        // changed path to its new entry (`None` = removed in the new tree).
        let changes = diff_trees(objects, Some(self.base_tree), Some(new_tree)).await?;
        let at_new: HashMap<String, Option<(EntryMode, ObjectId)>> =
            changes.iter().map(|ch| (ch.path.as_str().to_string(), ch.new)).collect();

        // Every local change must already be present, identically, in the new tree.
        let local = self.status(objects).await?;
        let mut dirty = Vec::new();
        for c in &local {
            let landed = match (c.kind, c.new) {
                // Deleted locally: landed only if the new tree removed the file.
                (LocalChangeKind::Deleted, _) => at_new.get(c.path.as_str()) == Some(&None),
                (_, Some(new)) => at_new.get(c.path.as_str()) == Some(&Some(new)),
                (_, None) => false,
            };
            if !landed {
                dirty.push(c.path.to_string());
            }
        }
        if !dirty.is_empty() {
            bail!("workspace has local changes not in the new tree: {}", dirty.join(", "));
        }

        let matcher = self.sparse.compile()?;
        let mut removals = Vec::new();
        let mut writes = Vec::new();
        for ch in changes {
            if !matcher.matches(&ch.path) {
                continue;
            }
            match ch.new {
                None => removals.push(ch.path),
                Some((mode, id)) => writes.push((ch.path, mode, id)),
            }
        }

        // Landed local changes outside the sparse scope are not touched on disk,
        // so fold them into the index: a landed deletion drops its entry, and a
        // landed add/modify is indexed from its current stat. Otherwise status()
        // would keep reporting them and submit would resend them.
        let mut out_removed = Vec::new();
        let mut out_added = Vec::new();
        for c in &local {
            if matcher.matches(&c.path) {
                continue;
            }
            match (c.kind, c.new) {
                (LocalChangeKind::Deleted, _) => out_removed.push(c.path.as_str().to_string()),
                (_, Some((mode, id))) => out_added.push((c.path.as_str().to_string(), mode, id)),
                (_, None) => {}
            }
        }

        let removed_keys: Vec<String> = removals.iter().map(|p| p.as_str().to_string()).collect();
        let d = self.dir.clone();
        let mut index = tokio::task::spawn_blocking(move || -> anyhow::Result<Index> {
            for p in &removals {
                remove_leaf(&d, p)?;
            }
            let mut index = read_index(&d)?;
            for key in removed_keys.iter().chain(out_removed.iter()) {
                index.remove(key);
            }
            for (key, mode, id) in out_added {
                let meta = fs::symlink_metadata(d.join(&key))
                    .with_context(|| format!("statting {key}"))?;
                index.insert(key, Entry { mode, id, stat: stat_of(&meta) });
            }
            Ok(index)
        })
        .await??;

        for (key, entry) in write_leaves(objects, &self.dir, &writes, false).await? {
            match entry {
                Some(e) => {
                    index.insert(key, e);
                }
                None => {
                    index.remove(&key);
                }
            }
        }

        self.persist(new_commit, new_tree, index, None).await
    }

    /// Move the workspace to `new_tree`, carrying local edits over
    /// ([`crate::update::plan`]). Only paths in the sparse scope, or already
    /// on disk, are written. Afterwards `status` reports exactly the paths
    /// that still differ from the new base (merged files as modified, kept
    /// local deletions as deleted). Conflicted paths are recorded in
    /// `.ant/conflicts`. Returns the plan.
    ///
    /// Not atomic: files are written before the new base is recorded, so an
    /// interruption can leave upstream changes reported as local edits.
    pub async fn update(
        &mut self,
        objects: &dyn ObjectStore,
        new_commit: ObjectId,
        new_tree: ObjectId,
    ) -> anyhow::Result<Vec<crate::update::PathUpdate>> {
        use crate::update::Target;
        let local = self.status(objects).await?;
        let changed: HashSet<&RepoPath> = local.iter().map(|c| &c.path).collect();
        let plan = crate::update::plan(objects, self.base_tree, new_tree, &local).await?;
        let matcher = self.sparse.compile()?;
        let mut removals = Vec::new();
        let mut fetch = Vec::new();
        let mut items = Vec::new();
        // Index decision per path: Some(None) = drop, Some(Some(id..)) = index.
        for u in &plan {
            let in_scope = matcher.matches(&u.path);
            let on_disk = u.local.is_some() && (in_scope || changed.contains(&u.path));
            match &u.target {
                Target::Upstream if u.local != u.upstream => match u.upstream {
                    Some((mode, id)) if in_scope || on_disk => {
                        fetch.push((u.path.clone(), mode, id));
                    }
                    None if on_disk => removals.push(u.path.clone()),
                    _ => {}
                },
                Target::Write { mode, data } => items.push(WriteItem {
                    path: u.path.clone(),
                    mode: *mode,
                    id: hash_object(ObjectKind::Blob, data),
                    body: Bytes::copy_from_slice(data),
                }),
                _ => {}
            }
        }
        let d = self.dir.clone();
        let removed = removals.clone();
        let mut written: HashMap<String, Option<Entry>> = tokio::task::spawn_blocking(move || {
            for p in &removed {
                remove_leaf(&d, p)?;
            }
            write_items(&d, items, false).map(|w| w.into_iter().collect())
        })
        .await??;
        written.extend(write_leaves(objects, &self.dir, &fetch, false).await?);
        let d = self.dir.clone();
        let plan_paths: Vec<_> = plan
            .iter()
            .map(|u| {
                let in_scope = matcher.matches(&u.path);
                let result = match &u.target {
                    Target::Write { mode, data } => {
                        Some((*mode, hash_object(ObjectKind::Blob, data)))
                    }
                    _ => u.result().flatten(),
                };
                (u.path.as_str().to_string(), in_scope, result, u.upstream)
            })
            .collect();
        let index = tokio::task::spawn_blocking(move || -> anyhow::Result<Index> {
            let mut index = read_index(&d)?;
            for (key, in_scope, result, upstream) in plan_paths {
                index.remove(&key);
                let Some((mode, id)) = upstream else { continue };
                if mode == EntryMode::Gitlink {
                    continue;
                }
                let present = result.is_some();
                if !in_scope && !present {
                    continue;
                }
                let stat = if result == upstream {
                    match written.remove(&key) {
                        Some(Some(e)) => e.stat,
                        Some(None) => continue,
                        None => match fs::symlink_metadata(d.join(&key)) {
                            Ok(m) => stat_of(&m),
                            // Out of scope and not materialized: not indexed.
                            Err(e) if e.kind() == ErrorKind::NotFound && !in_scope => continue,
                            Err(e) if e.kind() == ErrorKind::NotFound => STALE,
                            Err(e) => return Err(e.into()),
                        },
                    }
                } else {
                    STALE
                };
                index.insert(key, Entry { mode, id, stat });
            }
            Ok(index)
        })
        .await??;
        let conflicts: Vec<RepoPath> =
            plan.iter().filter(|u| u.conflict).map(|u| u.path.clone()).collect();
        self.persist(new_commit, new_tree, index, Some(conflicts)).await?;
        Ok(plan)
    }

    /// Record `index` and the base (`commit`, `tree`) in `.ant/`, then the
    /// conflicts when given, and adopt the new base.
    async fn persist(
        &mut self,
        commit: ObjectId,
        tree: ObjectId,
        index: Index,
        conflicts: Option<Vec<RepoPath>>,
    ) -> anyhow::Result<()> {
        let d = self.dir.clone();
        let patterns = self.sparse.patterns.clone();
        tokio::task::spawn_blocking(move || {
            save_state(&d, commit, tree, &patterns, &index)?;
            match conflicts {
                Some(paths) => write_conflicts(&conflicts_file(&d), &paths),
                None => Ok(()),
            }
        })
        .await??;
        self.base_commit = commit;
        self.base_tree = tree;
        Ok(())
    }
}

/// The trees of one walk level, in `frontier` order: directories wholly in
/// scope through `dirs` (when given), the rest with one `get_many`.
/// The leaves (files, symlinks, gitlinks) of `base_tree` the matcher selects,
/// found by a breadth-first walk with one fetch per level (whole directories
/// through `dirs` where the matcher covers them and it is set).
async fn walk_leaves(
    objects: &dyn ObjectStore,
    dirs: Option<&dyn DirFetch>,
    matcher: sparse::Matcher,
    base_tree: ObjectId,
) -> anyhow::Result<Vec<(RepoPath, EntryMode, ObjectId)>> {
    // One get_many per level for the subtrees we need; the trees of a level
    // are decoded in parallel.
    let matcher = Arc::new(matcher);
    let mut leaves: Vec<(RepoPath, EntryMode, ObjectId)> = Vec::new();
    let mut frontier = vec![(RepoPath::root(), base_tree)];
    while !frontier.is_empty() {
        let level = fetch_level(objects, dirs, &matcher, &frontier).await?;
        let m = matcher.clone();
        let expanded = tokio::task::spawn_blocking(move || {
            let work: Vec<_> = frontier.into_iter().zip(level).collect();
            par_map(&work, 64, |((parent, id), obj)| expand_tree(&m, parent, id, obj.clone()))
        })
        .await?;
        let mut next = Vec::new();
        for r in expanded {
            let (subtrees, found) = r?;
            next.extend(subtrees);
            leaves.extend(found);
        }
        frontier = next;
    }
    Ok(leaves)
}

/// What [`seed`] covered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seeded {
    /// Leaves (files, symlinks) in scope.
    pub leaves: usize,
    /// Distinct blobs among them.
    pub blobs: usize,
}

/// Blob ids handed to [`seed`]'s `ensure` per call.
const SEED_BATCH: usize = 2048;

/// Make the tree `base_tree` (restricted to `sparse`) local through
/// `objects`/`dirs`: walk it exactly as a checkout would, so the same bundles
/// are fetched, then pass every blob in scope, in batches, to `ensure` (which
/// fetches the ones that are missing locally). No files are written.
pub async fn seed(
    objects: &dyn ObjectStore,
    dirs: Option<&dyn DirFetch>,
    base_tree: ObjectId,
    sparse: &Sparse,
    ensure: &(
         dyn Fn(Vec<ObjectId>) -> futures::future::BoxFuture<'static, ant_store::Result<()>> + Sync
     ),
) -> anyhow::Result<Seeded> {
    let leaves = walk_leaves(objects, dirs, sparse.compile()?, base_tree).await?;
    let mut blobs: Vec<ObjectId> =
        leaves.iter().filter(|(_, m, _)| *m != EntryMode::Gitlink).map(|(_, _, id)| *id).collect();
    blobs.sort_unstable();
    blobs.dedup();
    for batch in blobs.chunks(SEED_BATCH) {
        ensure(batch.to_vec()).await?;
    }
    Ok(Seeded { leaves: leaves.len(), blobs: blobs.len() })
}

async fn fetch_level(
    objects: &dyn ObjectStore,
    dirs: Option<&dyn DirFetch>,
    matcher: &sparse::Matcher,
    frontier: &[(RepoPath, ObjectId)],
) -> anyhow::Result<Vec<Option<EncodedObject>>> {
    let whole: Vec<bool> = match dirs {
        Some(_) => frontier.iter().map(|(p, _)| matcher.covers_all_under(p)).collect(),
        None => vec![false; frontier.len()],
    };
    let pick = |want: bool| -> Vec<ObjectId> {
        frontier.iter().zip(&whole).filter(|(_, w)| **w == want).map(|((_, id), _)| *id).collect()
    };
    let (by_dir, by_id) = (pick(true), pick(false));
    let mut by_dir = match dirs {
        Some(d) if !by_dir.is_empty() => d.get_dirs(&by_dir).await?,
        _ => Vec::new(),
    }
    .into_iter();
    let mut by_id =
        if by_id.is_empty() { Vec::new() } else { objects.get_many(&by_id).await? }.into_iter();
    Ok(whole.iter().map(|w| if *w { by_dir.next() } else { by_id.next() }.flatten()).collect())
}

/// The workspace's unresolved conflicts file (`.ant/conflicts`).
pub fn conflicts_file(dir: &Path) -> PathBuf {
    state_dir(dir).join(CONFLICTS_FILE)
}

/// Unresolved conflicts recorded in `file` (missing file: none).
pub fn read_conflicts(file: &Path) -> anyhow::Result<Vec<RepoPath>> {
    match fs::read_to_string(file) {
        Ok(text) => crate::update::parse_conflicts(&text),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e).with_context(|| format!("reading {}", file.display())),
    }
}

/// Replace the conflicts in `file` atomically (an empty list removes it).
pub fn write_conflicts(file: &Path, paths: &[RepoPath]) -> anyhow::Result<()> {
    if paths.is_empty() {
        return match fs::remove_file(file) {
            Err(e) if e.kind() != ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        };
    }
    let text = crate::update::render_conflicts(paths.iter().map(|p| p.as_str()));
    atomic_write(file, text.as_bytes())
}

#[cfg(test)]
mod tests;
