//! Status ([`changes`]) and rebasing ([`reset`]) of a workspace from its
//! state directory, without walking untouched base subtrees.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self};
use std::io::{self};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, RepoPath, Tree};
use ant_store::ObjectStore;
use antd::workspace::{LocalChange, LocalChangeKind};

use super::state::*;
use super::{hidden, mode, other};

pub(super) struct Upper {
    files: BTreeMap<String, (EntryMode, ObjectId)>,
    dirs: BTreeSet<String>,
    blobs: Vec<EncodedObject>,
}
pub(super) fn scan_upper(state: &Path) -> io::Result<Upper> {
    let root = state.join("upper");
    let mut out = Upper { files: BTreeMap::new(), dirs: BTreeSet::new(), blobs: Vec::new() };
    let mut todo = vec![String::new()];
    while let Some(dir) = todo.pop() {
        for entry in fs::read_dir(root.join(&dir))? {
            let entry = entry?;
            let name = entry.file_name().into_string().map_err(|_| other("non-UTF8 repo path"))?;
            if dir.is_empty() && name == ".ant" {
                continue;
            }
            let path = if dir.is_empty() { name } else { format!("{dir}/{name}") };
            RepoPath::new(&path).map_err(other)?;
            let m = fs::symlink_metadata(entry.path())?;
            if m.is_dir() {
                out.dirs.insert(path.clone());
                todo.push(path);
            } else {
                let data = if m.file_type().is_symlink() {
                    fs::read_link(entry.path())?.as_os_str().as_bytes().to_vec()
                } else {
                    fs::read(entry.path())?
                };
                let blob = EncodedObject::new(ObjectKind::Blob, &data);
                out.files.insert(path, (mode(&m), blob.id()));
                out.blobs.push(blob);
            }
        }
    }
    Ok(out)
}

// Walk only branches intersecting upper entries or whiteouts, breadth-first.
// All trees at a depth are fetched together. Never fetch base blobs.
pub(super) async fn affected_base(
    objects: &dyn ObjectStore,
    root: ObjectId,
    upper: &Upper,
    deleted: &BTreeSet<String>,
) -> io::Result<BTreeMap<String, (EntryMode, ObjectId)>> {
    let mut exact = upper.dirs.clone();
    exact.extend(upper.files.keys().cloned());
    exact.extend(deleted.iter().cloned());
    if exact.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut branches = BTreeSet::new();
    for path in &exact {
        for (i, _) in path.match_indices('/') {
            branches.insert(path[..i].to_string());
        }
    }
    let mut out = BTreeMap::new();
    let mut todo = vec![(String::new(), root, false)];
    while !todo.is_empty() {
        let mut next = Vec::new();
        for batch in todo.chunks(512) {
            let ids = batch.iter().map(|(_, id, _)| *id).collect::<Vec<_>>();
            let found = objects.get_many(&ids).await.map_err(other)?;
            if found.len() != ids.len() {
                return Err(other("misaligned tree batch"));
            }
            for ((prefix, _, inherited), object) in batch.iter().zip(found) {
                let object = object.ok_or_else(|| other("missing base tree"))?;
                if object.kind() != ObjectKind::Tree {
                    return Err(other("base object is not a tree"));
                }
                let tree = Tree::decode(object.body()).map_err(other)?;
                for e in tree.entries() {
                    let path = if prefix.is_empty() {
                        e.name.clone()
                    } else {
                        format!("{prefix}/{}", e.name)
                    };
                    let all =
                        *inherited || deleted.contains(&path) || upper.files.contains_key(&path);
                    if !all && !exact.contains(&path) && !branches.contains(&path) {
                        continue;
                    }
                    if e.mode == EntryMode::Tree {
                        next.push((path, e.id, all));
                    } else {
                        out.insert(path, (e.mode, e.id));
                    }
                }
            }
        }
        todo = next;
    }
    Ok(out)
}

pub(super) fn compare(
    upper: &Upper,
    deleted: &BTreeSet<String>,
    lower: &BTreeMap<String, (EntryMode, ObjectId)>,
) -> io::Result<Vec<LocalChange>> {
    let mut paths: BTreeSet<_> = upper.files.keys().cloned().collect();
    paths.extend(lower.keys().cloned());
    let mut out = Vec::new();
    for path in paths {
        let old = lower.get(&path).copied();
        let new = upper.files.get(&path).copied().or_else(|| {
            if hidden(deleted, &path)
                || upper.dirs.contains(&path)
                || path.match_indices('/').any(|(i, _)| upper.files.contains_key(&path[..i]))
            {
                None
            } else {
                old
            }
        });
        if old == new {
            continue;
        }
        let kind = match (old, new) {
            (None, Some(_)) => LocalChangeKind::Added,
            (Some(_), None) => LocalChangeKind::Deleted,
            _ => LocalChangeKind::Modified,
        };
        out.push(LocalChange { path: RepoPath::new(path).map_err(other)?, kind, new });
    }
    Ok(out)
}

/// Compute sorted changes without walking untouched base subtrees. Blocking
/// filesystem work and state locking run on a blocking thread. New blobs are
/// inserted in batches into `objects`.
pub async fn changes(
    objects: Arc<dyn ObjectStore>,
    state_dir: &Path,
) -> io::Result<Vec<LocalChange>> {
    let state = state_dir.to_path_buf();
    let rt = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let _lock = StateLock::take(&state)?;
        recover(&state)?;
        let (_, root) = base(&state)?;
        let deleted = whiteouts(&state)?;
        let upper = scan_upper(&state)?;
        let lower = rt.block_on(affected_base(objects.as_ref(), root, &upper, &deleted))?;
        let result = compare(&upper, &deleted, &lower)?;
        let changed: BTreeSet<_> = result.iter().filter_map(|c| c.new.map(|(_, id)| id)).collect();
        let blobs =
            upper.blobs.into_iter().filter(|b| changed.contains(&b.id())).collect::<Vec<_>>();
        for batch in blobs.chunks(512) {
            rt.block_on(objects.put_many(batch)).map_err(other)?;
        }
        Ok(result)
    })
    .await
    .map_err(other)?
}

/// Replace the base while preserving edits that differ from the new tree.
/// Matching upper files and obsolete whiteouts are removed. A live mount
/// observes the replacement on its next operation, preserving inode numbers.
pub async fn reset(
    objects: Arc<dyn ObjectStore>,
    state_dir: &Path,
    commit: ObjectId,
    tree: ObjectId,
) -> io::Result<()> {
    let state = state_dir.to_path_buf();
    let rt = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let _lock = StateLock::take(&state)?;
        recover(&state)?;
        let upper = scan_upper(&state)?;
        let mut deleted = whiteouts(&state)?;
        let lower = rt.block_on(affected_base(objects.as_ref(), tree, &upper, &deleted))?;
        let (_, old_tree) = base(&state)?;
        let old_lower = rt.block_on(affected_base(objects.as_ref(), old_tree, &upper, &deleted))?;
        // Publish first: interruption leaves redundant copies, never loses edits.
        save_base(&state, commit, tree)?;
        deleted = lower
            .keys()
            .filter(|path| hidden(&deleted, path) && !upper.files.contains_key(*path))
            .cloned()
            .collect();
        save_whiteouts(&state, &deleted)?;
        for (path, value) in &upper.files {
            if lower.get(path) == Some(value) || old_lower.get(path) == Some(value) {
                fs::remove_file(state.join("upper").join(path))?;
            }
        }
        // Remove empty copy-up directories so upstream removals become visible.
        let mut dirs: Vec<_> = upper.dirs.into_iter().collect();
        dirs.sort_by_key(|p| std::cmp::Reverse(p.len()));
        for path in dirs {
            match fs::remove_dir(state.join("upper").join(path)) {
                Ok(()) => {}
                Err(e) if e.raw_os_error() == Some(libc::ENOTEMPTY) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    })
    .await
    .map_err(other)?
}
