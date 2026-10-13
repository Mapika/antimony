//! Sparse working copies.
//!
//! TASK (antd-workspace): implement everything marked `todo!()`.
//!
//! On-disk state lives in `{dir}/.ant/` (never reported as a change):
//! - `.ant/workspace`: `base_commit=<hex>`, `base_tree=<hex>`, and one
//!   `sparse=<pattern>` line per pattern. Plain text, one `key=value` per line.
//! - `.ant/index`: one record per materialized non-tree entry: path, mode,
//!   object id, size, mtime (ns), inode. Format is yours (keep it simple and
//!   fast to parse; tab-separated text is fine).
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
//! - Walk the tree breadth-first, fetching each level's subtrees with ONE
//!   `get_many` call (not one per tree). Skip subtrees that cannot contain a
//!   match (`Sparse::may_contain_under(dir_path)`: true if any prefix pattern
//!   starts with or is under that dir, or any glob pattern exists — globs are
//!   not pruned).
//! - Fetch blobs in batches of 512 ids per `get_many`. Write files with the
//!   right mode (0o755 for Executable, 0o644 otherwise), symlinks via
//!   `std::os::unix::fs::symlink`, gitlinks as empty directories (not
//!   indexed).
//! - Record every written entry in the index with its post-write stat.
//!
//! `status` (no network; uses only the index + local hashing):
//! - Walk the workspace (skip `.ant`). For each file/symlink:
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
//! that are already identical in `new_tree` (the just-landed changes).
//! Otherwise returns an error naming the dirty paths. Applies
//! `diff_trees(old_base_tree, new_tree)` to the in-scope files on disk
//! (writes, deletes, removes now-empty dirs), then rewrites the index and
//! config.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::io::{self, ErrorKind};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, RepoPath, Tree, hash_object};
use ant_store::ObjectStore;
use ant_store::graph::diff_trees;
use anyhow::{Context, anyhow, bail};
use bytes::Bytes;
use globset::GlobBuilder;
use walkdir::WalkDir;

/// Objects fetched or stored per `get_many` / `put_many` call.
const BATCH: usize = 512;
const STATE_DIR: &str = ".ant";
const CONFIG_FILE: &str = "workspace";
const INDEX_FILE: &str = "index";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sparse {
    pub patterns: Vec<String>,
}

impl Sparse {
    pub fn all() -> Self {
        Self { patterns: vec![] }
    }

    pub fn new(patterns: Vec<String>) -> anyhow::Result<Self> {
        let patterns = patterns.into_iter().map(normalize_pattern).collect::<anyhow::Result<Vec<_>>>()?;
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
    RepoPath::new(trimmed.as_str()).with_context(|| format!("invalid sparse pattern {pattern:?}"))?;
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
        let target = fs::read_link(full).with_context(|| format!("reading link {}", full.display()))?;
        return Ok((EntryMode::Symlink, target.into_os_string().into_vec()));
    }
    let mode = if meta.mode() & 0o111 != 0 { EntryMode::Executable } else { EntryMode::File };
    let body = fs::read(full).with_context(|| format!("reading {}", full.display()))?;
    Ok((mode, body))
}

fn escape_path(path: &str) -> String {
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

fn unescape_path(s: &str) -> String {
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
fn save_state(dir: &Path, commit: ObjectId, tree: ObjectId, patterns: &[String], index: &Index) -> anyhow::Result<()> {
    let state = state_dir(dir);
    fs::create_dir_all(&state)?;
    let mut cfg = format!("base_commit={}\nbase_tree={}\n", commit.to_hex(), tree.to_hex());
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

/// Blocking: write items under `dir`. Returns the index entry per item;
/// gitlinks produce `None` (empty directory, not indexed).
fn write_items(dir: &Path, items: Vec<WriteItem>) -> anyhow::Result<Vec<(String, Option<Entry>)>> {
    let mut out = Vec::with_capacity(items.len());
    for it in items {
        let key = it.path.as_str().to_string();
        let full = dir.join(&key);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent)?;
        }
        remove_existing(&full).with_context(|| format!("clearing {}", full.display()))?;
        match it.mode {
            EntryMode::Gitlink => {
                fs::create_dir_all(&full)?;
                out.push((key, None));
                continue;
            }
            EntryMode::Symlink => {
                symlink(OsStr::from_bytes(&it.body), &full)
                    .with_context(|| format!("creating symlink {}", full.display()))?;
            }
            EntryMode::File | EntryMode::Executable => {
                fs::write(&full, &it.body).with_context(|| format!("writing {}", full.display()))?;
                let perm = if it.mode == EntryMode::Executable { 0o755 } else { 0o644 };
                fs::set_permissions(&full, fs::Permissions::from_mode(perm))?;
            }
            EntryMode::Tree => bail!("tree entry {key} passed as a leaf"),
        }
        let meta = fs::symlink_metadata(&full)?;
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

/// Fetch the blobs for `leaves` in batches of `BATCH` ids and write them.
/// Gitlinks are written without fetching anything.
async fn write_leaves(
    objects: &dyn ObjectStore,
    dir: &Path,
    leaves: &[(RepoPath, EntryMode, ObjectId)],
) -> anyhow::Result<Vec<(String, Option<Entry>)>> {
    let mut out = Vec::with_capacity(leaves.len());
    for chunk in leaves.chunks(BATCH) {
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
        out.extend(tokio::task::spawn_blocking(move || write_items(&d, items)).await??);
    }
    Ok(out)
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

/// Blocking: walk `dir` and compare it against `index`. Returns the changes
/// plus the encoded blobs for every Added/Modified entry.
fn scan(dir: &Path, index: &Index, index_mtime: Option<i64>) -> anyhow::Result<(Vec<LocalChange>, Vec<EncodedObject>)> {
    let mut changes = Vec::new();
    let mut blobs = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    let walker = WalkDir::new(dir)
        .follow_links(false)
        .min_depth(1)
        .into_iter()
        .filter_entry(|e| !(e.depth() == 1 && e.file_name() == STATE_DIR));
    for entry in walker {
        let entry = entry.with_context(|| format!("walking {}", dir.display()))?;
        let ft = entry.file_type();
        if !(ft.is_file() || ft.is_symlink()) {
            continue; // directories (including empty ones) are not tracked
        }
        let rel = entry.path().strip_prefix(dir)?;
        let rel_str = rel
            .to_str()
            .ok_or_else(|| anyhow!("non-UTF-8 path {}", rel.display()))?;
        let meta = entry.metadata()?;
        let (kind, new) = match index.get_key_value(rel_str) {
            Some((key, rec)) => {
                seen.insert(key.as_str());
                let racy = index_mtime.is_some_and(|im| rec.stat.mtime_ns >= im);
                if !racy && rec.stat == stat_of(&meta) {
                    continue;
                }
                let (mode, body) = hash_disk(entry.path(), &meta)?;
                let id = hash_object(ObjectKind::Blob, &body);
                if id == rec.id && mode == rec.mode {
                    continue;
                }
                blobs.push(EncodedObject::new(ObjectKind::Blob, &body));
                (LocalChangeKind::Modified, Some((mode, id)))
            }
            None => {
                let (mode, body) = hash_disk(entry.path(), &meta)?;
                let id = hash_object(ObjectKind::Blob, &body);
                blobs.push(EncodedObject::new(ObjectKind::Blob, &body));
                (LocalChangeKind::Added, Some((mode, id)))
            }
        };
        changes.push(LocalChange { path: RepoPath::new(rel_str)?, kind, new });
    }
    for key in index.keys() {
        if !seen.contains(key.as_str()) {
            changes.push(LocalChange {
                path: RepoPath::new(key.as_str())?,
                kind: LocalChangeKind::Deleted,
                new: None,
            });
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

        // Breadth-first walk: one get_many per level for the subtrees we need.
        let mut leaves: Vec<(RepoPath, EntryMode, ObjectId)> = Vec::new();
        let mut frontier = vec![(RepoPath::root(), base_tree)];
        while !frontier.is_empty() {
            let ids: Vec<ObjectId> = frontier.iter().map(|(_, id)| *id).collect();
            let level = objects.get_many(&ids).await?;
            let mut next = Vec::new();
            for ((parent, id), obj) in frontier.iter().zip(level) {
                let tree = decode_tree(id, obj)?;
                for e in tree.entries() {
                    let child = parent.join(&e.name)?;
                    if e.mode.is_tree() {
                        if matcher.may_contain_under(&child) {
                            next.push((child, e.id));
                        }
                    } else if matcher.matches(&child) {
                        leaves.push((child, e.mode, e.id));
                    }
                }
            }
            frontier = next;
        }

        let d = dir.to_path_buf();
        tokio::task::spawn_blocking(move || prepare_empty_dir(&d)).await??;

        let mut index = Index::new();
        for (key, entry) in write_leaves(objects, dir, &leaves).await? {
            if let Some(e) = entry {
                index.insert(key, e);
            }
        }
        let d = dir.to_path_buf();
        let patterns = sparse.patterns.clone();
        tokio::task::spawn_blocking(move || save_state(&d, base_commit, base_tree, &patterns, &index)).await??;

        Ok(Self { dir: dir.to_path_buf(), base_commit, base_tree, sparse })
    }

    pub fn open(dir: &Path) -> anyhow::Result<Self> {
        let path = state_dir(dir).join(CONFIG_FILE);
        let text = fs::read_to_string(&path)
            .with_context(|| format!("reading workspace config {}", path.display()))?;
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
        let base_commit = base_commit.ok_or_else(|| anyhow!("workspace config has no base_commit"))?;
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
                let meta = fs::symlink_metadata(d.join(&key)).with_context(|| format!("statting {key}"))?;
                index.insert(key, Entry { mode, id, stat: stat_of(&meta) });
            }
            Ok(index)
        })
        .await??;

        for (key, entry) in write_leaves(objects, &self.dir, &writes).await? {
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
        tokio::task::spawn_blocking(move || save_state(&d, new_commit, new_tree, &patterns, &index)).await??;
        self.base_commit = new_commit;
        self.base_tree = new_tree;
        Ok(())
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
        let t = build_tree(&objs, &[("a.txt", b"A"), ("d/e/f.rs", b"F"), ("d/g", b"G")]).await.unwrap();
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

    #[tokio::test]
    async fn sparse_skips_unneeded_subtrees() {
        let objs = CountingStore::default();
        let mut files: Vec<(String, Vec<u8>)> = Vec::new();
        for i in 0..50 {
            files.push((format!("other{i}/x/y.rs"), b"o".to_vec()));
        }
        files.push(("want/a.rs".into(), b"w".to_vec()));
        let refs: Vec<(&str, &[u8])> = files.iter().map(|(a, b)| (a.as_str(), b.as_slice())).collect();
        let t = build_tree(&objs.inner, &refs).await.unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        Workspace::materialize(&objs, &dir, C, t, Sparse::new(vec!["want".into()]).unwrap()).await.unwrap();
        assert!(dir.join("want/a.rs").exists());
        assert!(!dir.join("other0").exists());
        // root tree level + "want" level + one blob batch; definitely not ~150.
        assert!(objs.calls.load(Ordering::SeqCst) <= 4, "calls = {}", objs.calls.load(Ordering::SeqCst));
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
        let t = build_tree(&objs, &[("keep", b"k"), ("mod", b"m"), ("del", b"d"), ("in/scope", b"s"), ("out/side", b"o")])
            .await
            .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        let sparse = Sparse::new(vec!["keep".into(), "mod".into(), "del".into(), "in".into()]).unwrap();
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
    async fn reset_to_new_base() {
        let objs = Arc::new(MemoryObjectStore::default());
        let t1 = build_tree(objs.as_ref(), &[("a", b"1"), ("b/c", b"2")]).await.unwrap();
        let t2 = build_tree(objs.as_ref(), &[("a", b"1-mine"), ("d", b"upstream")]).await.unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ws");
        let mut ws = Workspace::materialize(objs.as_ref(), &dir, C, t1, Sparse::all()).await.unwrap();
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
        let t2 = build_tree(objs.as_ref(), &[("a", b"1"), ("b/c", b"2"), ("x", b"landed")]).await.unwrap();
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
}
