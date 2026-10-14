//! Writing tree entries to disk: materializing, resetting and updating.

use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::io::{self, ErrorKind};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, RepoPath, Tree};
use ant_store::ObjectStore;
use anyhow::{Context, anyhow, bail};
use bytes::Bytes;
use futures::StreamExt;

use super::index::{Entry, stat_of};
use super::sparse::Matcher;
use crate::files::file_perm;
use crate::par::par_map;
use crate::walk::BATCH;

/// Batches fetched and written concurrently.
const WRITE_CONCURRENCY: usize = 8;

/// Create `dir` if needed; fail if it already has content.
pub(super) fn prepare_empty_dir(dir: &Path) -> anyhow::Result<()> {
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
pub(super) fn leaf_dirs(leaves: &[(RepoPath, EntryMode, ObjectId)]) -> Vec<Vec<String>> {
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
pub(super) fn create_dirs(dir: &Path, levels: &[Vec<String>]) -> anyhow::Result<()> {
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
pub(super) fn remove_existing(full: &Path) -> io::Result<()> {
    match fs::symlink_metadata(full) {
        Ok(m) if m.is_dir() => fs::remove_dir(full),
        Ok(_) => fs::remove_file(full),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Delete a tracked leaf and prune ancestor directories left empty.
pub(super) fn remove_leaf(dir: &Path, rel: &RepoPath) -> anyhow::Result<()> {
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

pub(super) struct WriteItem {
    pub(super) path: RepoPath,
    pub(super) mode: EntryMode,
    pub(super) id: ObjectId,
    pub(super) body: Bytes,
}

/// Index entry per written path (`None`: not indexed, e.g. a gitlink).
pub(super) type Written = Vec<(String, Option<Entry>)>;

/// The process umask, if it can be read (Linux: `/proc/self/status`).
pub(super) fn umask() -> Option<u32> {
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
pub(super) fn ensure_dir(parent: &Path, last: &mut Option<PathBuf>) -> io::Result<()> {
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
pub(super) fn write_file(
    full: &Path,
    mode: EntryMode,
    body: &[u8],
) -> anyhow::Result<fs::Metadata> {
    use std::io::Write;
    let perm = file_perm(mode);
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
pub(super) fn write_items(
    dir: &Path,
    items: Vec<WriteItem>,
    fresh: bool,
) -> anyhow::Result<Written> {
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

pub(super) fn blob_body(obj: &EncodedObject, id: &ObjectId) -> anyhow::Result<Bytes> {
    if obj.kind() != ObjectKind::Blob {
        bail!("object {id} is not a blob");
    }
    let all = obj.as_bytes();
    Ok(all.slice(all.len() - obj.body().len()..))
}

/// Fetch the blobs for `leaves` in batches of `BATCH` ids and write them,
/// several batches in flight at once (fetch, decompress and file writes of
/// different batches overlap). Gitlinks are written without fetching anything.
pub(super) async fn write_leaves(
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

pub(super) type Expanded = (Vec<(RepoPath, ObjectId)>, Vec<(RepoPath, EntryMode, ObjectId)>);

/// Decode the tree `id` at `parent`: the subtrees worth descending into and
/// the leaves inside the sparse scope.
pub(super) fn expand_tree(
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

pub(super) fn decode_tree(id: &ObjectId, obj: Option<EncodedObject>) -> anyhow::Result<Tree> {
    let obj = obj.ok_or_else(|| anyhow!("missing tree {id}"))?;
    if obj.kind() != ObjectKind::Tree {
        bail!("object {id} is not a tree");
    }
    Ok(Tree::decode(obj.body())?)
}
