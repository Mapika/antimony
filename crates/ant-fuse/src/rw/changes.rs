//! Status ([`changes`]) and rebasing ([`reset`]) of a workspace from its
//! state directory, without walking untouched base subtrees.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self};
use std::io::{self};
use std::path::Path;
use std::sync::Arc;

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, RepoPath};
use ant_store::ObjectStore;
use antd::files::read_body;
use antd::walk::{BATCH, Visit, WalkError, walk};
use antd::workspace::{LocalChange, LocalChangeKind};

use super::journal::{Persisted, Record, append_durably, load};
use super::state::*;
use super::{hidden, mode, other};

pub(super) struct Upper {
    pub(super) files: BTreeMap<String, (EntryMode, ObjectId)>,
    pub(super) dirs: BTreeSet<String>,
    pub(super) blobs: Vec<EncodedObject>,
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
                let data = read_body(&entry.path(), &m)?;
                let blob = EncodedObject::new(ObjectKind::Blob, &data);
                out.files.insert(path, (mode(&m), blob.id()));
                out.blobs.push(blob);
            }
        }
    }
    Ok(out)
}

/// The base entries on paths intersecting upper entries or whiteouts (all
/// of them below upper files and whiteouts). Walks only those branches,
/// breadth-first, one batched read per depth; never fetches base blobs.
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
    walk(objects, root, |path, e, inherited| {
        let all = inherited || deleted.contains(path) || upper.files.contains_key(path);
        if !all && !exact.contains(path) && !branches.contains(path) {
            Visit::Skip
        } else if e.mode == EntryMode::Tree {
            Visit::Descend(all)
        } else {
            Visit::Leaf
        }
    })
    .await
    .map_err(|e| match e {
        WalkError::Store(e) => other(e),
        WalkError::Misaligned => other("misaligned tree batch"),
        WalkError::Missing(_) => other("missing base tree"),
        WalkError::NotTree(_) => other("base object is not a tree"),
        WalkError::Decode(e) => other(e),
    })
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

/// A workspace's local changes, from a state directory the caller has
/// locked and recovered.
pub(super) struct Local {
    /// The old base tree (as persisted).
    pub(super) tree: ObjectId,
    pub(super) deleted: BTreeSet<String>,
    pub(super) upper: Upper,
    /// Sorted changes against the base.
    pub(super) changes: Vec<LocalChange>,
}

/// Compute [`Local`] and store the new blobs of changed files into `objects`
/// in batches, so later readers (submit, `update`'s plan) find them by id.
pub(super) fn local_changes(
    state: &Path,
    objects: &dyn ObjectStore,
    rt: &tokio::runtime::Handle,
) -> io::Result<Local> {
    let Persisted { tree, deleted, .. } = load(state)?;
    let upper = scan_upper(state)?;
    let lower = rt.block_on(affected_base(objects, tree, &upper, &deleted))?;
    let changes = compare(&upper, &deleted, &lower)?;
    let changed: BTreeSet<_> = changes.iter().filter_map(|c| c.new.map(|(_, id)| id)).collect();
    let blobs: Vec<_> = upper.blobs.iter().filter(|b| changed.contains(&b.id())).cloned().collect();
    for batch in blobs.chunks(BATCH) {
        rt.block_on(objects.put_many(batch)).map_err(other)?;
    }
    Ok(Local { tree, deleted, upper, changes })
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
        Ok(local_changes(&state, objects.as_ref(), &rt)?.changes)
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
        let Persisted { tree: old_tree, deleted, .. } = load(&state)?;
        let lower = rt.block_on(affected_base(objects.as_ref(), tree, &upper, &deleted))?;
        let old_lower = rt.block_on(affected_base(objects.as_ref(), old_tree, &upper, &deleted))?;
        // Publish first: interruption leaves redundant copies, never loses edits.
        // A mounted daemon notices the journal append and reloads. The journal
        // record is authoritative; recovery finishes the snapshot rewrite.
        let whiteouts = lower
            .keys()
            .filter(|path| hidden(&deleted, path) && !upper.files.contains_key(*path))
            .cloned()
            .collect();
        append_durably(&state, &Record::Reset { commit, tree, whiteouts })?;
        save_base(&state, commit, tree)?;
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
