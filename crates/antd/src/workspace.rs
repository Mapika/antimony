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
//!   `get_many` call. Skips subtrees that cannot contain a match (`Sparse::may_contain_under(dir_path)`: true if any prefix pattern
//!   starts with or is under that dir, or any glob pattern exists — globs are
//!   not pruned).
//! - Fetches blobs in batches of 512 ids per `get_many`, several batches in
//!   flight at once (fetch, decode and file writes overlap). Tree levels are
//!   decoded in parallel and all needed directories are created up front,
//!   level by level in parallel. Each file is one `open` (mode set at
//!   creation), one `write` and one `fstat`. Files get the right mode (0o755 for Executable, 0o644 otherwise), symlinks via
//!   `std::os::unix::fs::symlink`, gitlinks as empty directories (not
//!   indexed).
//! - Every written entry is indexed with its post-write stat.
//!
//! `status` (no network; uses only the index + local hashing):
//! - Walks the workspace (skipping `.ant`), listing and stat'ing directories
//!   on several threads, one tree level at a time. For each file/symlink:
//!   indexed and (size, mtime_ns, ino, mode) unchanged → unchanged, without
//!   reading the file; otherwise read + hash as a blob and compare ids/mode.
//! - Indexed paths missing on disk → `Deleted`.
//! - Unindexed files anywhere (even outside the sparse patterns) → `Added`
//!   (it's the agent's new file — never ignore it).
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::io::{self, ErrorKind};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, RepoPath, Tree, hash_object};
use ant_store::ObjectStore;
use ant_store::graph::diff_trees;

use crate::disk::par_map;
use anyhow::{Context, anyhow, bail};
use bytes::Bytes;
use futures::StreamExt;
use globset::GlobBuilder;

/// Objects fetched or stored per `get_many` / `put_many` call.
const BATCH: usize = 512;
/// Batches fetched and written concurrently.
const WRITE_CONCURRENCY: usize = 8;
const STATE_DIR: &str = ".ant";
const CONFIG_FILE: &str = "workspace";
const INDEX_FILE: &str = "index";
const CONFLICTS_FILE: &str = "conflicts";
/// Version of the `.ant/` state layout, the `format=` line of
/// `.ant/workspace` (absent in workspaces created before it: version 1).
pub const WORKSPACE_FORMAT: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sparse {
    pub patterns: Vec<String>,
}

impl Sparse {
    pub fn all() -> Self {
        Self { patterns: vec![] }
    }

    pub fn new(patterns: Vec<String>) -> anyhow::Result<Self> {
        let patterns =
            patterns.into_iter().map(normalize_pattern).collect::<anyhow::Result<Vec<_>>>()?;
        let sparse = Self { patterns };
        // Validates glob syntax up front.
        sparse.compile()?;
        Ok(sparse)
    }

    pub fn matches(&self, path: &RepoPath) -> bool {
        // Patterns from `new` are always valid, so compilation cannot fail here.
        // A hand-built invalid glob matches nothing.
        self.compile().map(|m| m.matches(path)).unwrap_or(false)
    }

    /// Could any path under directory `dir` match?
    pub fn may_contain_under(&self, dir: &RepoPath) -> bool {
        self.compile().map(|m| m.may_contain_under(dir)).unwrap_or(false)
    }

    /// Compile once for hot loops (walks over many entries).
    fn compile(&self) -> anyhow::Result<Matcher> {
        let mut prefixes = Vec::new();
        let mut globs = Vec::new();
        for p in &self.patterns {
            if is_glob(p) {
                let glob = GlobBuilder::new(p)
                    .literal_separator(true)
                    .build()
                    .with_context(|| format!("invalid sparse glob {p:?}"))?;
                globs.push(glob.compile_matcher());
            } else {
                prefixes.push(p.clone());
            }
        }
        Ok(Matcher { all: self.patterns.is_empty(), prefixes, globs })
    }
}

fn is_glob(pattern: &str) -> bool {
    pattern.contains(['*', '?', '[', '{'])
}

/// Validates a pattern and canonicalizes prefix patterns (no trailing `/`).
fn normalize_pattern(pattern: String) -> anyhow::Result<String> {
    if pattern.is_empty() || pattern.contains(['\n', '\0']) {
        bail!("invalid sparse pattern {pattern:?}");
    }
    if is_glob(&pattern) {
        return Ok(pattern);
    }
    let trimmed = pattern.trim_end_matches('/').to_string();
    if trimmed.is_empty() {
        bail!("invalid sparse pattern {pattern:?}");
    }
    RepoPath::new(trimmed.as_str())
        .with_context(|| format!("invalid sparse pattern {pattern:?}"))?;
    Ok(trimmed)
}

/// `path` equals `prefix` or lies beneath it.
fn under(path: &str, prefix: &str) -> bool {
    path == prefix
        || (path.len() > prefix.len()
            && path.starts_with(prefix)
            && path.as_bytes()[prefix.len()] == b'/')
}

struct Matcher {
    all: bool,
    prefixes: Vec<String>,
    globs: Vec<globset::GlobMatcher>,
}

impl Matcher {
    fn matches(&self, path: &RepoPath) -> bool {
        if self.all {
            return true;
        }
        let s = path.as_str();
        self.prefixes.iter().any(|p| under(s, p)) || self.globs.iter().any(|g| g.is_match(s))
    }

    fn may_contain_under(&self, dir: &RepoPath) -> bool {
        if self.all || dir.is_root() || !self.globs.is_empty() {
            return true;
        }
        let d = dir.as_str();
        self.prefixes.iter().any(|p| under(p, d) || under(d, p))
    }
}

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

#[derive(Debug)]
pub struct Workspace {
    #[allow(dead_code)]
    dir: PathBuf,
    pub base_commit: ObjectId,
    pub base_tree: ObjectId,
    pub sparse: Sparse,
}

/// The filesystem stat fields recorded per indexed entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Stat {
    size: u64,
    mtime_ns: i64,
    ino: u64,
    st_mode: u32,
}

fn stat_of(m: &fs::Metadata) -> Stat {
    Stat {
        size: m.len(),
        mtime_ns: m.mtime().saturating_mul(1_000_000_000).saturating_add(m.mtime_nsec()),
        ino: m.ino(),
        st_mode: m.mode(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    mode: EntryMode,
    id: ObjectId,
    stat: Stat,
}

/// Indexed (materialized, non-tree) entries keyed by path, sorted.
type Index = BTreeMap<String, Entry>;

/// Shape of a file on disk: git mode plus its body (target for symlinks).
fn hash_disk(full: &Path, meta: &fs::Metadata) -> anyhow::Result<(EntryMode, Vec<u8>)> {
    if meta.file_type().is_symlink() {
        let target =
            fs::read_link(full).with_context(|| format!("reading link {}", full.display()))?;
        return Ok((EntryMode::Symlink, target.into_os_string().into_vec()));
    }
    let mode = if meta.mode() & 0o111 != 0 { EntryMode::Executable } else { EntryMode::File };
    let body = fs::read(full).with_context(|| format!("reading {}", full.display()))?;
    Ok((mode, body))
}

pub(crate) fn escape_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for c in path.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

pub(crate) fn unescape_path(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

fn state_dir(dir: &Path) -> PathBuf {
    dir.join(STATE_DIR)
}

fn read_index(dir: &Path) -> anyhow::Result<Index> {
    let path = state_dir(dir).join(INDEX_FILE);
    let text = match fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Index::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let mut index = Index::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let f: Vec<&str> = line.splitn(7, '\t').collect();
        if f.len() != 7 {
            bail!("corrupt workspace index line {line:?}");
        }
        let entry = Entry {
            mode: EntryMode::from_octal(f[0].as_bytes())?,
            id: f[1].parse::<ObjectId>()?,
            stat: Stat {
                size: f[2].parse()?,
                mtime_ns: f[3].parse()?,
                ino: f[4].parse()?,
                st_mode: f[5].parse()?,
            },
        };
        index.insert(unescape_path(f[6]), entry);
    }
    Ok(index)
}

/// Mtime of the index file in ns, used to detect "racy" entries whose
/// content may have changed within the same timestamp tick as indexing.
fn index_mtime(dir: &Path) -> anyhow::Result<Option<i64>> {
    match fs::symlink_metadata(state_dir(dir).join(INDEX_FILE)) {
        Ok(m) => Ok(Some(stat_of(&m).mtime_ns)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn render_index(index: &Index) -> String {
    let mut out = String::new();
    for (path, e) in index {
        out.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            e.mode.as_octal(),
            e.id.to_hex(),
            e.stat.size,
            e.stat.mtime_ns,
            e.stat.ino,
            e.stat.st_mode,
            escape_path(path),
        ));
    }
    out
}

fn atomic_write(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, data).with_context(|| format!("writing {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("renaming into {}", path.display()))?;
    Ok(())
}

/// Persist config and index. Both are replaced atomically per file.
fn save_state(
    dir: &Path,
    commit: ObjectId,
    tree: ObjectId,
    patterns: &[String],
    index: &Index,
) -> anyhow::Result<()> {
    let state = state_dir(dir);
    fs::create_dir_all(&state)?;
    let mut cfg = format!(
        "format={WORKSPACE_FORMAT}\nbase_commit={}\nbase_tree={}\n",
        commit.to_hex(),
        tree.to_hex()
    );
    for p in patterns {
        cfg.push_str(&format!("sparse={p}\n"));
    }
    atomic_write(&state.join(CONFIG_FILE), cfg.as_bytes())?;
    atomic_write(&state.join(INDEX_FILE), render_index(index).as_bytes())?;
    Ok(())
}

/// Create `dir` if needed; fail if it already has content.
fn prepare_empty_dir(dir: &Path) -> anyhow::Result<()> {
    match fs::read_dir(dir) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(io::Error::new(
                    ErrorKind::AlreadyExists,
                    format!("{} is not empty", dir.display()),
                )
                .into());
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    fs::create_dir_all(dir)?;
    Ok(())
}

/// Directories that must exist for `leaves`: every parent plus its ancestors,
/// as repo-relative paths grouped by depth (parents before children), each
/// level sorted so siblings are adjacent.
fn leaf_dirs(leaves: &[(RepoPath, EntryMode, ObjectId)]) -> Vec<Vec<String>> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut levels: Vec<Vec<String>> = Vec::new();
    for (path, _, _) in leaves {
        let mut cur = path.as_str().rsplit_once('/').map(|(parent, _)| parent);
        while let Some(d) = cur {
            if !seen.insert(d) {
                break; // it and all its ancestors are already listed
            }
            let depth = d.matches('/').count();
            if levels.len() <= depth {
                levels.resize_with(depth + 1, Vec::new);
            }
            levels[depth].push(d.to_string());
            cur = d.rsplit_once('/').map(|(parent, _)| parent);
        }
    }
    for level in &mut levels {
        level.sort_unstable();
    }
    levels
}

/// Blocking: create `levels` of directories under `dir`, one level at a time,
/// each level in parallel (siblings stay on one thread, so their shared
/// parent directory is not contended).
fn create_dirs(dir: &Path, levels: &[Vec<String>]) -> anyhow::Result<()> {
    for level in levels {
        let results = par_map(level, 64, |d| {
            let full = dir.join(d);
            match fs::create_dir(&full) {
                Err(e) if e.kind() != ErrorKind::AlreadyExists => {
                    Err(e).with_context(|| format!("creating {}", full.display()))
                }
                _ => Ok(()),
            }
        });
        results.into_iter().collect::<anyhow::Result<()>>()?;
    }
    Ok(())
}

/// Remove whatever occupies `full` so a new entry can be created there.
/// Never follows symlinks.
fn remove_existing(full: &Path) -> io::Result<()> {
    match fs::symlink_metadata(full) {
        Ok(m) if m.is_dir() => fs::remove_dir(full),
        Ok(_) => fs::remove_file(full),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Delete a tracked leaf and prune ancestor directories left empty.
fn remove_leaf(dir: &Path, rel: &RepoPath) -> anyhow::Result<()> {
    let full = dir.join(rel.as_str());
    match fs::symlink_metadata(&full) {
        // A non-empty directory here is untracked content; leave it alone.
        Ok(m) if m.is_dir() => {
            let _ = fs::remove_dir(&full);
        }
        Ok(_) => fs::remove_file(&full).with_context(|| format!("removing {}", full.display()))?,
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let mut cur = full.parent();
    while let Some(p) = cur {
        if p == dir || !p.starts_with(dir) || fs::remove_dir(p).is_err() {
            break;
        }
        cur = p.parent();
    }
    Ok(())
}

struct WriteItem {
    path: RepoPath,
    mode: EntryMode,
    id: ObjectId,
    body: Bytes,
}

/// Index entry per written path (`None`: not indexed, e.g. a gitlink).
type Written = Vec<(String, Option<Entry>)>;

/// The process umask, if it can be read (Linux: `/proc/self/status`).
fn umask() -> Option<u32> {
    static UMASK: std::sync::OnceLock<Option<u32>> = std::sync::OnceLock::new();
    *UMASK.get_or_init(|| {
        let status = fs::read_to_string("/proc/self/status").ok()?;
        let v = status.lines().find_map(|l| l.strip_prefix("Umask:"))?;
        u32::from_str_radix(v.trim(), 8).ok()
    })
}

/// Create `parent` (and missing ancestors) unless `*last` says it exists.
/// Items arrive grouped by directory, so this is one `mkdir` per directory
/// rather than a stat-per-component walk per file.
fn ensure_dir(parent: &Path, last: &mut Option<PathBuf>) -> io::Result<()> {
    if last.as_deref() == Some(parent) {
        return Ok(());
    }
    match fs::create_dir(parent) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::AlreadyExists && parent.is_dir() => {}
        Err(e) if e.kind() == ErrorKind::NotFound => fs::create_dir_all(parent)?,
        Err(e) => return Err(e),
    }
    *last = Some(parent.to_path_buf());
    Ok(())
}

/// Create the file at `full` with `body`: one `open` (permissions set at
/// creation), one `write`, one `fstat`.
fn write_file(full: &Path, mode: EntryMode, body: &[u8]) -> anyhow::Result<fs::Metadata> {
    use std::io::Write;
    let perm = if mode == EntryMode::Executable { 0o755 } else { 0o644 };
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(perm)
        .open(full)
        .with_context(|| format!("writing {}", full.display()))?;
    f.write_all(body).with_context(|| format!("writing {}", full.display()))?;
    // `mode` at creation is filtered by the umask; fix up only if it matters.
    if umask().is_none_or(|u| u & perm != 0) {
        f.set_permissions(fs::Permissions::from_mode(perm))?;
    }
    Ok(f.metadata()?)
}

/// Blocking: write items under `dir`. Returns the index entry per item;
/// gitlinks produce `None` (empty directory, not indexed). `fresh`: nothing
/// can exist at the destinations (new workspace), so skip clearing them.
fn write_items(dir: &Path, items: Vec<WriteItem>, fresh: bool) -> anyhow::Result<Written> {
    let mut out = Vec::with_capacity(items.len());
    let mut last_dir: Option<PathBuf> = None;
    for it in items {
        let key = it.path.as_str().to_string();
        let full = dir.join(&key);
        if let (false, Some(parent)) = (fresh, full.parent()) {
            ensure_dir(parent, &mut last_dir)?;
        }
        if !fresh {
            remove_existing(&full).with_context(|| format!("clearing {}", full.display()))?;
        }
        let meta = match it.mode {
            EntryMode::Gitlink => {
                fs::create_dir_all(&full)?;
                out.push((key, None));
                continue;
            }
            EntryMode::Symlink => {
                symlink(OsStr::from_bytes(&it.body), &full)
                    .with_context(|| format!("creating symlink {}", full.display()))?;
                fs::symlink_metadata(&full)?
            }
            EntryMode::File | EntryMode::Executable => write_file(&full, it.mode, &it.body)?,
            EntryMode::Tree => bail!("tree entry {key} passed as a leaf"),
        };
        out.push((key, Some(Entry { mode: it.mode, id: it.id, stat: stat_of(&meta) })));
    }
    Ok(out)
}

fn blob_body(obj: &EncodedObject, id: &ObjectId) -> anyhow::Result<Bytes> {
    if obj.kind() != ObjectKind::Blob {
        bail!("object {id} is not a blob");
    }
    let all = obj.as_bytes();
    Ok(all.slice(all.len() - obj.body().len()..))
}

/// Fetch the blobs for `leaves` in batches of `BATCH` ids and write them,
/// several batches in flight at once (fetch, decompress and file writes of
/// different batches overlap). Gitlinks are written without fetching anything.
async fn write_leaves(
    objects: &dyn ObjectStore,
    dir: &Path,
    leaves: &[(RepoPath, EntryMode, ObjectId)],
    fresh: bool,
) -> anyhow::Result<Written> {
    let batches: Vec<anyhow::Result<Written>> = futures::stream::iter(leaves.chunks(BATCH))
        .map(|chunk| async move {
            let ids: Vec<ObjectId> = chunk
                .iter()
                .filter(|(_, mode, _)| *mode != EntryMode::Gitlink)
                .map(|(_, _, id)| *id)
                .collect();
            let fetched: Vec<Option<EncodedObject>> =
                if ids.is_empty() { Vec::new() } else { objects.get_many(&ids).await? };
            let mut fetched = fetched.into_iter();
            let mut items = Vec::with_capacity(chunk.len());
            for (path, mode, id) in chunk {
                let body = if *mode == EntryMode::Gitlink {
                    Bytes::new()
                } else {
                    let obj = fetched
                        .next()
                        .flatten()
                        .ok_or_else(|| anyhow!("missing blob {id} for {path}"))?;
                    blob_body(&obj, id)?
                };
                items.push(WriteItem { path: path.clone(), mode: *mode, id: *id, body });
            }
            let d = dir.to_path_buf();
            tokio::task::spawn_blocking(move || write_items(&d, items, fresh)).await?
        })
        .buffer_unordered(WRITE_CONCURRENCY)
        .collect()
        .await;
    let mut out = Vec::with_capacity(leaves.len());
    for b in batches {
        out.extend(b?);
    }
    Ok(out)
}

type Expanded = (Vec<(RepoPath, ObjectId)>, Vec<(RepoPath, EntryMode, ObjectId)>);

/// Decode the tree `id` at `parent`: the subtrees worth descending into and
/// the leaves inside the sparse scope.
fn expand_tree(
    matcher: &Matcher,
    parent: &RepoPath,
    id: &ObjectId,
    obj: Option<EncodedObject>,
) -> anyhow::Result<Expanded> {
    let tree = decode_tree(id, obj)?;
    let mut subtrees = Vec::new();
    let mut leaves = Vec::new();
    for e in tree.entries() {
        let child = parent.join(&e.name)?;
        if e.mode.is_tree() {
            if matcher.may_contain_under(&child) {
                subtrees.push((child, e.id));
            }
        } else if matcher.matches(&child) {
            leaves.push((child, e.mode, e.id));
        }
    }
    Ok((subtrees, leaves))
}

fn decode_tree(id: &ObjectId, obj: Option<EncodedObject>) -> anyhow::Result<Tree> {
    let obj = obj.ok_or_else(|| anyhow!("missing tree {id}"))?;
    if obj.kind() != ObjectKind::Tree {
        bail!("object {id} is not a tree");
    }
    Ok(Tree::decode(obj.body())?)
}

async fn put_blobs(objects: &dyn ObjectStore, blobs: &[EncodedObject]) -> anyhow::Result<()> {
    for chunk in blobs.chunks(BATCH) {
        objects.put_many(chunk).await?;
    }
    Ok(())
}

/// What one directory contributed to a [`scan`].
#[derive(Default)]
struct DirScan<'a> {
    subdirs: Vec<String>,
    changes: Vec<LocalChange>,
    blobs: Vec<EncodedObject>,
    /// Index keys found on disk.
    seen: Vec<&'a str>,
}

/// Blocking: list the directory `rel` (relative to `dir`, "" for the root)
/// and compare its files against `index`.
fn scan_dir<'a>(
    dir: &Path,
    rel: &str,
    index: &'a Index,
    index_mtime: Option<i64>,
) -> anyhow::Result<DirScan<'a>> {
    let mut out = DirScan::default();
    let full_dir = if rel.is_empty() { dir.to_path_buf() } else { dir.join(rel) };
    let listing =
        fs::read_dir(&full_dir).with_context(|| format!("walking {}", full_dir.display()))?;
    for entry in listing {
        let entry = entry.with_context(|| format!("walking {}", full_dir.display()))?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| anyhow!("non-UTF-8 path {}", full_dir.join(&name).display()))?;
        if rel.is_empty() && name == STATE_DIR {
            continue;
        }
        let ft = entry.file_type()?;
        let rel_str = if rel.is_empty() { name.to_string() } else { format!("{rel}/{name}") };
        if ft.is_dir() {
            // Directories (including empty ones) are not tracked themselves.
            out.subdirs.push(rel_str);
            continue;
        }
        if !(ft.is_file() || ft.is_symlink()) {
            continue;
        }
        let meta = entry.metadata()?;
        let (kind, new) = match index.get_key_value(rel_str.as_str()) {
            Some((key, rec)) => {
                out.seen.push(key.as_str());
                let racy = index_mtime.is_some_and(|im| rec.stat.mtime_ns >= im);
                if !racy && rec.stat == stat_of(&meta) {
                    continue;
                }
                let (mode, body) = hash_disk(&entry.path(), &meta)?;
                let id = hash_object(ObjectKind::Blob, &body);
                if id == rec.id && mode == rec.mode {
                    continue;
                }
                out.blobs.push(EncodedObject::new(ObjectKind::Blob, &body));
                (LocalChangeKind::Modified, Some((mode, id)))
            }
            None => {
                let (mode, body) = hash_disk(&entry.path(), &meta)?;
                let id = hash_object(ObjectKind::Blob, &body);
                out.blobs.push(EncodedObject::new(ObjectKind::Blob, &body));
                (LocalChangeKind::Added, Some((mode, id)))
            }
        };
        out.changes.push(LocalChange { path: RepoPath::new(rel_str.as_str())?, kind, new });
    }
    Ok(out)
}

/// Blocking: walk `dir` and compare it against `index`. Returns the changes
/// plus the encoded blobs for every Added/Modified entry. Directories are
/// listed and their files stat'ed in parallel, one tree level at a time.
fn scan(
    dir: &Path,
    index: &Index,
    index_mtime: Option<i64>,
) -> anyhow::Result<(Vec<LocalChange>, Vec<EncodedObject>)> {
    let mut changes = Vec::new();
    let mut blobs = Vec::new();
    let mut seen: Vec<&str> = Vec::with_capacity(index.len());
    let mut level: Vec<String> = vec![String::new()];
    while !level.is_empty() {
        let results = par_map(&level, 8, |rel| scan_dir(dir, rel, index, index_mtime));
        let mut next = Vec::new();
        for r in results {
            let r = r?;
            next.extend(r.subdirs);
            changes.extend(r.changes);
            blobs.extend(r.blobs);
            seen.extend(r.seen);
        }
        level = next;
    }
    // Every indexed path is on disk exactly once iff the counts agree.
    if seen.len() < index.len() {
        let seen: HashSet<&str> = seen.into_iter().collect();
        for key in index.keys() {
            if !seen.contains(key.as_str()) {
                changes.push(LocalChange {
                    path: RepoPath::new(key.as_str())?,
                    kind: LocalChangeKind::Deleted,
                    new: None,
                });
            }
        }
    }
    changes.sort_by(|a, b| a.path.cmp(&b.path));
    Ok((changes, blobs))
}

impl Workspace {
    pub async fn materialize(
        objects: &dyn ObjectStore,
        dir: &Path,
        base_commit: ObjectId,
        base_tree: ObjectId,
        sparse: Sparse,
    ) -> anyhow::Result<Self> {
        let matcher = sparse.compile()?;

        // Breadth-first walk: one get_many per level for the subtrees we need;
        // the trees of a level are decoded in parallel.
        let matcher = Arc::new(matcher);
        let mut leaves: Vec<(RepoPath, EntryMode, ObjectId)> = Vec::new();
        let mut frontier = vec![(RepoPath::root(), base_tree)];
        while !frontier.is_empty() {
            let ids: Vec<ObjectId> = frontier.iter().map(|(_, id)| *id).collect();
            let level = objects.get_many(&ids).await?;
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
        let d = dir.to_path_buf();
        let patterns = sparse.patterns.clone();
        tokio::task::spawn_blocking(move || {
            save_state(&d, base_commit, base_tree, &patterns, &index)
        })
        .await??;

        Ok(Self { dir: dir.to_path_buf(), base_commit, base_tree, sparse })
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

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub async fn status(&self, objects: &dyn ObjectStore) -> anyhow::Result<Vec<LocalChange>> {
        let dir = self.dir.clone();
        let (changes, blobs) = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let index = read_index(&dir)?;
            let mtime = index_mtime(&dir)?;
            scan(&dir, &index, mtime)
        })
        .await??;
        put_blobs(objects, &blobs).await?;
        Ok(changes)
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

        let d = self.dir.clone();
        let patterns = self.sparse.patterns.clone();
        tokio::task::spawn_blocking(move || {
            save_state(&d, new_commit, new_tree, &patterns, &index)
        })
        .await??;
        self.base_commit = new_commit;
        self.base_tree = new_tree;
        Ok(())
    }
}

/// Index stat that never matches a file on disk: the entry is re-hashed (and
/// reported as modified) or, when the path is missing, reported as deleted.
const STALE: Stat = Stat { size: 0, mtime_ns: 0, ino: 0, st_mode: 0 };

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

impl Workspace {
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
        let d = self.dir.clone();
        let patterns = self.sparse.patterns.clone();
        let conflicts: Vec<RepoPath> =
            plan.iter().filter(|u| u.conflict).map(|u| u.path.clone()).collect();
        tokio::task::spawn_blocking(move || {
            save_state(&d, new_commit, new_tree, &patterns, &index)?;
            write_conflicts(&conflicts_file(&d), &conflicts)
        })
        .await??;
        self.base_commit = new_commit;
        self.base_tree = new_tree;
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use ant_core::{EncodedObject, Object, ObjectKind, Tree, TreeEntry};
    use ant_store::graph::build_tree;
    use ant_store::memory::MemoryObjectStore;
    use ant_store::{ObjectStoreExt, Result as SResult};
    use async_trait::async_trait;

    fn p(s: &str) -> RepoPath {
        RepoPath::new(s).unwrap()
    }

    const C: ObjectId = ObjectId([0xc0; 32]);
    const C2: ObjectId = ObjectId([0xc2; 32]);

    #[derive(Default)]
    struct CountingStore {
        inner: MemoryObjectStore,
        calls: AtomicUsize,
    }
    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn get_many(&self, ids: &[ObjectId]) -> SResult<Vec<Option<EncodedObject>>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get_many(ids).await
        }
        async fn put_many(&self, o: &[EncodedObject]) -> SResult<()> {
            self.inner.put_many(o).await
        }
        async fn has_many(&self, ids: &[ObjectId]) -> SResult<Vec<bool>> {
            self.inner.has_many(ids).await
        }
    }

    #[test]
    fn sparse_matching() {
        let s = Sparse::new(vec!["src/foo".into(), "docs/*.md".into(), "**/BUILD".into()]).unwrap();
        assert!(s.matches(&p("src/foo")));
        assert!(s.matches(&p("src/foo/a/b.rs")));
        assert!(!s.matches(&p("src/foobar/x")));
        assert!(s.matches(&p("docs/a.md")));
        assert!(!s.matches(&p("docs/sub/a.md")));
        assert!(s.matches(&p("x/y/BUILD")));
        assert!(Sparse::all().matches(&p("anything")));

        let prefix_only = Sparse::new(vec!["src/foo".into()]).unwrap();
        assert!(prefix_only.may_contain_under(&p("src")));
        assert!(prefix_only.may_contain_under(&p("src/foo/bar")));
        assert!(!prefix_only.may_contain_under(&p("lib")));
        assert!(!prefix_only.may_contain_under(&p("src/foobar")));
    }

    #[tokio::test]
    async fn materialize_full_and_status_clean() {
        let objs = MemoryObjectStore::default();
        let t =
            build_tree(&objs, &[("a.txt", b"A"), ("d/e/f.rs", b"F"), ("d/g", b"G")]).await.unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        let ws = Workspace::materialize(&objs, &dir, C, t, Sparse::all()).await.unwrap();
        assert_eq!(std::fs::read(dir.join("d/e/f.rs")).unwrap(), b"F");
        assert!(ws.status(&objs).await.unwrap().is_empty());

        let reopened = Workspace::open(&dir).unwrap();
        assert_eq!(reopened.base_commit, C);
        assert_eq!(reopened.base_tree, t);
        assert!(reopened.status(&objs).await.unwrap().is_empty());
    }

    /// Workspaces from before the format line keep working; a newer format
    /// is refused with a recognizable error.
    #[tokio::test]
    async fn workspace_format_versions() {
        let objs = MemoryObjectStore::default();
        let t = build_tree(&objs, &[("a.txt", b"A")]).await.unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        let mut ws =
            Workspace::materialize(&objs, &dir, C, t, Sparse::new(vec!["a.txt".into()]).unwrap())
                .await
                .unwrap();
        let config = dir.join(".ant/workspace");
        let written = fs::read_to_string(&config).unwrap();
        assert!(written.starts_with(&format!("format={WORKSPACE_FORMAT}\n")), "{written}");

        // The layout written before versioning: no format line.
        fs::write(&config, format!("base_commit={C}\nbase_tree={t}\nsparse=a.txt\n")).unwrap();
        let old = Workspace::open(&dir).unwrap();
        assert_eq!((old.base_commit, old.base_tree), (C, t));
        assert_eq!(old.sparse.patterns, vec!["a.txt".to_string()]);
        assert!(old.status(&objs).await.unwrap().is_empty());
        // Rewritten state records the version.
        ws.reset_to(&objs, C2, t).await.unwrap();
        assert!(fs::read_to_string(&config).unwrap().starts_with("format=1\n"));

        // Newer: refused before anything else in the file is interpreted.
        fs::write(&config, "format=2\nbase_commit=not-an-id-in-v1\n").unwrap();
        let err = Workspace::open(&dir).err().unwrap();
        let newer = err.downcast_ref::<ant_core::NewerFormat>().unwrap();
        assert_eq!((newer.found, newer.supported), (2, WORKSPACE_FORMAT));
        fs::write(&config, format!("format=x\nbase_commit={C}\nbase_tree={t}\n")).unwrap();
        let err = Workspace::open(&dir).err().unwrap();
        assert!(err.downcast_ref::<ant_core::NewerFormat>().is_none());
        assert!(err.to_string().contains("corrupt workspace format"), "{err}");
    }

    #[tokio::test]
    async fn sparse_skips_unneeded_subtrees() {
        let objs = CountingStore::default();
        let mut files: Vec<(String, Vec<u8>)> = Vec::new();
        for i in 0..50 {
            files.push((format!("other{i}/x/y.rs"), b"o".to_vec()));
        }
        files.push(("want/a.rs".into(), b"w".to_vec()));
        let refs: Vec<(&str, &[u8])> =
            files.iter().map(|(a, b)| (a.as_str(), b.as_slice())).collect();
        let t = build_tree(&objs.inner, &refs).await.unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        Workspace::materialize(&objs, &dir, C, t, Sparse::new(vec!["want".into()]).unwrap())
            .await
            .unwrap();
        assert!(dir.join("want/a.rs").exists());
        assert!(!dir.join("other0").exists());
        // root tree level + "want" level + one blob batch; definitely not ~150.
        assert!(
            objs.calls.load(Ordering::SeqCst) <= 4,
            "calls = {}",
            objs.calls.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn modes_symlinks_and_gitlinks() {
        let objs = MemoryObjectStore::default();
        let exe = objs.put(EncodedObject::new(ObjectKind::Blob, b"#!/bin/sh\n")).await.unwrap();
        let target = objs.put(EncodedObject::new(ObjectKind::Blob, b"run.sh")).await.unwrap();
        let tree = Object::Tree(
            Tree::new(vec![
                TreeEntry { mode: EntryMode::Executable, name: "run.sh".into(), id: exe },
                TreeEntry { mode: EntryMode::Symlink, name: "link".into(), id: target },
                TreeEntry { mode: EntryMode::Gitlink, name: "sub".into(), id: ObjectId([5; 32]) },
            ])
            .unwrap(),
        )
        .encode();
        let t = objs.put(tree).await.unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        let ws = Workspace::materialize(&objs, &dir, C, t, Sparse::all()).await.unwrap();
        let m = std::fs::metadata(dir.join("run.sh")).unwrap().permissions().mode();
        assert_eq!(m & 0o111, 0o111);
        assert_eq!(std::fs::read_link(dir.join("link")).unwrap(), PathBuf::from("run.sh"));
        assert!(dir.join("sub").is_dir());
        assert!(ws.status(&objs).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn status_detects_changes() {
        let objs = MemoryObjectStore::default();
        let t = build_tree(
            &objs,
            &[("keep", b"k"), ("mod", b"m"), ("del", b"d"), ("in/scope", b"s"), ("out/side", b"o")],
        )
        .await
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        let sparse =
            Sparse::new(vec!["keep".into(), "mod".into(), "del".into(), "in".into()]).unwrap();
        let ws = Workspace::materialize(&objs, &dir, C, t, sparse).await.unwrap();
        assert!(!dir.join("out").exists());

        std::fs::write(dir.join("mod"), b"m2").unwrap();
        std::fs::remove_file(dir.join("del")).unwrap();
        std::fs::create_dir_all(dir.join("new/dir")).unwrap();
        std::fs::write(dir.join("new/dir/f"), b"n").unwrap();
        std::fs::create_dir_all(dir.join("empty")).unwrap();
        // Same size, different content, and force same mtime: must still be
        // caught because we compare inode/mtime/size and rehash on mismatch —
        // here the content changes size-preservingly, so rewrite via a new inode.
        std::fs::write(dir.join("in/scope.tmp"), b"S").unwrap();
        std::fs::rename(dir.join("in/scope.tmp"), dir.join("in/scope")).unwrap();
        std::fs::set_permissions(dir.join("keep"), std::fs::Permissions::from_mode(0o755)).unwrap();

        let st = ws.status(&objs).await.unwrap();
        let summary: Vec<_> = st.iter().map(|c| (c.path.as_str(), c.kind)).collect();
        assert_eq!(
            summary,
            [
                ("del", LocalChangeKind::Deleted),
                ("in/scope", LocalChangeKind::Modified),
                ("keep", LocalChangeKind::Modified),
                ("mod", LocalChangeKind::Modified),
                ("new/dir/f", LocalChangeKind::Added),
            ]
        );
        let keep = st.iter().find(|c| c.path.as_str() == "keep").unwrap();
        assert_eq!(keep.new.unwrap().0, EntryMode::Executable);
        let m = st.iter().find(|c| c.path.as_str() == "mod").unwrap();
        assert_eq!(&objs.read_blob(&m.new.unwrap().1).await.unwrap()[..], b"m2");
    }

    #[tokio::test]
    async fn parallel_scan_many_dirs_and_deletions() {
        let objs = MemoryObjectStore::default();
        let files: Vec<(String, Vec<u8>)> = (0..400)
            .map(|i| (format!("d{}/s{}/f{i}", i % 20, i % 7), vec![b'x'; i % 50]))
            .collect();
        let refs: Vec<(&str, &[u8])> =
            files.iter().map(|(a, b)| (a.as_str(), b.as_slice())).collect();
        let t = build_tree(&objs, &refs).await.unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        let ws = Workspace::materialize(&objs, &dir, C, t, Sparse::all()).await.unwrap();
        assert!(ws.status(&objs).await.unwrap().is_empty());
        std::fs::remove_file(dir.join(&files[3].0)).unwrap();
        std::fs::remove_file(dir.join(&files[399].0)).unwrap();
        std::fs::write(dir.join("d1/new"), b"n").unwrap();
        let st = ws.status(&objs).await.unwrap();
        let summary: Vec<_> = st.iter().map(|c| (c.path.as_str().to_string(), c.kind)).collect();
        let mut expect = vec![
            (files[3].0.clone(), LocalChangeKind::Deleted),
            (files[399].0.clone(), LocalChangeKind::Deleted),
            ("d1/new".to_string(), LocalChangeKind::Added),
        ];
        expect.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(summary, expect);
    }

    #[tokio::test]
    async fn reset_to_new_base() {
        let objs = Arc::new(MemoryObjectStore::default());
        let t1 = build_tree(objs.as_ref(), &[("a", b"1"), ("b/c", b"2")]).await.unwrap();
        let t2 = build_tree(objs.as_ref(), &[("a", b"1-mine"), ("d", b"upstream")]).await.unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        let mut ws =
            Workspace::materialize(objs.as_ref(), &dir, C, t1, Sparse::all()).await.unwrap();
        // Local change that is identical in t2 (we "landed" it).
        std::fs::write(dir.join("a"), b"1-mine").unwrap();
        ws.reset_to(objs.as_ref(), C2, t2).await.unwrap();
        assert_eq!(std::fs::read(dir.join("d")).unwrap(), b"upstream");
        assert!(!dir.join("b").exists());
        assert!(ws.status(objs.as_ref()).await.unwrap().is_empty());
        assert_eq!(Workspace::open(&dir).unwrap().base_commit, C2);

        // Dirty change not in the new tree → refuse.
        std::fs::write(dir.join("d"), b"local edit").unwrap();
        assert!(ws.reset_to(objs.as_ref(), C, t1).await.is_err());
    }

    #[tokio::test]
    async fn reset_to_indexes_landed_out_of_scope_add() {
        let objs = Arc::new(MemoryObjectStore::default());
        let t1 = build_tree(objs.as_ref(), &[("a", b"1"), ("b/c", b"2")]).await.unwrap();
        let t2 = build_tree(objs.as_ref(), &[("a", b"1"), ("b/c", b"2"), ("x", b"landed")])
            .await
            .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        let sparse = Sparse::new(vec!["a".into()]).unwrap();
        let mut ws = Workspace::materialize(objs.as_ref(), &dir, C, t1, sparse).await.unwrap();
        // Added outside the sparse scope, identical to the landed tree.
        std::fs::write(dir.join("x"), b"landed").unwrap();
        ws.reset_to(objs.as_ref(), C2, t2).await.unwrap();
        // It must now be indexed, so the workspace is clean and not resent.
        assert!(ws.status(objs.as_ref()).await.unwrap().is_empty());
        assert!(!dir.join("b").exists());
    }
    fn summary(changes: &[LocalChange]) -> Vec<(String, LocalChangeKind)> {
        changes.iter().map(|c| (c.path.as_str().to_string(), c.kind)).collect()
    }

    #[tokio::test]
    async fn update_carries_local_edits_onto_new_base() {
        use LocalChangeKind::*;
        let objs = Arc::new(MemoryObjectStore::default());
        let files1: &[(&str, &[u8])] = &[
            ("both", b"1\n2\n3\n4\n5\n"),
            ("clash", b"1\n2\n3\n"),
            ("gone", b"g"),
            ("keep", b"k"),
            ("local", b"l"),
            ("up", b"u"),
            ("moddel", b"m"),
        ];
        let files2: &[(&str, &[u8])] = &[
            ("both", b"one\n2\n3\n4\n5\n"),
            ("clash", b"1\nup\n3\n"),
            ("keep", b"k"),
            ("local", b"l"),
            ("up", b"u2"),
            ("new/file", b"n"),
        ];
        let t1 = build_tree(objs.as_ref(), files1).await.unwrap();
        let t2 = build_tree(objs.as_ref(), files2).await.unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        let mut ws =
            Workspace::materialize(objs.as_ref(), &dir, C, t1, Sparse::all()).await.unwrap();
        std::fs::write(dir.join("both"), b"1\n2\n3\n4\nfive\n").unwrap();
        std::fs::write(dir.join("clash"), b"1\nmine\n3\n").unwrap();
        std::fs::write(dir.join("local"), b"l2").unwrap();
        std::fs::write(dir.join("moddel"), b"m2").unwrap();
        std::fs::write(dir.join("added"), b"a").unwrap();
        std::fs::remove_file(dir.join("keep")).unwrap();
        let plan = ws.update(objs.as_ref(), C2, t2).await.unwrap();
        assert_eq!(ws.base_commit, C2);
        let conflicts: Vec<&str> =
            plan.iter().filter(|u| u.conflict).map(|u| u.path.as_str()).collect();
        assert_eq!(conflicts, ["clash", "moddel"]);
        assert_eq!(read_conflicts(&conflicts_file(&dir)).unwrap(), [p("clash"), p("moddel")]);
        let read = |f: &str| std::fs::read(dir.join(f)).unwrap();
        assert_eq!(read("both"), b"one\n2\n3\n4\nfive\n");
        assert_eq!(
            read("clash"),
            b"1\n<<<<<<< upstream\nup\n||||||| base\n2\n=======\nmine\n>>>>>>> local\n3\n"
        );
        assert_eq!(read("up"), b"u2");
        assert_eq!(read("new/file"), b"n");
        assert_eq!(read("local"), b"l2");
        assert_eq!(read("moddel"), b"m2");
        assert!(!dir.join("gone").exists());
        assert!(!dir.join("keep").exists());
        let reopened = Workspace::open(&dir).unwrap();
        assert_eq!((reopened.base_commit, reopened.base_tree), (C2, t2));
        assert_eq!(
            summary(&ws.status(objs.as_ref()).await.unwrap()),
            [
                ("added".into(), Added),
                ("both".into(), Modified),
                ("clash".into(), Modified),
                ("keep".into(), Deleted),
                ("local".into(), Modified),
                ("moddel".into(), Added),
            ]
        );
        // Nothing left to carry: a second update is a no-op.
        let again = ws.update(objs.as_ref(), C2, t2).await.unwrap();
        assert!(again.iter().all(|u| u.target == crate::update::Target::Local));
        write_conflicts(&conflicts_file(&dir), &[]).unwrap();
        assert!(read_conflicts(&conflicts_file(&dir)).unwrap().is_empty());
    }

    #[tokio::test]
    async fn update_respects_sparse_scope() {
        let objs = Arc::new(MemoryObjectStore::default());
        let t1 = build_tree(objs.as_ref(), &[("in/a", b"1"), ("out/b", b"2")]).await.unwrap();
        let t2 = build_tree(
            objs.as_ref(),
            &[("in/a", b"3"), ("in/n", b"4"), ("out/b", b"5"), ("out/c", b"6")],
        )
        .await
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        let sparse = Sparse::new(vec!["in".into()]).unwrap();
        let mut ws = Workspace::materialize(objs.as_ref(), &dir, C, t1, sparse).await.unwrap();
        // A local file outside the scope, identical to upstream's new file.
        std::fs::create_dir_all(dir.join("out")).unwrap();
        std::fs::write(dir.join("out/c"), b"6").unwrap();
        let plan = ws.update(objs.as_ref(), C2, t2).await.unwrap();
        assert!(plan.iter().all(|u| !u.conflict));
        assert_eq!(std::fs::read(dir.join("in/a")).unwrap(), b"3");
        assert_eq!(std::fs::read(dir.join("in/n")).unwrap(), b"4");
        assert!(!dir.join("out/b").exists());
        let st = ws.status(objs.as_ref()).await.unwrap();
        assert!(st.is_empty(), "{st:?}");
    }
}
