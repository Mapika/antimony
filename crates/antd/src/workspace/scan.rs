//! `status`: comparing the files on disk against the index.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

use ant_core::{EncodedObject, EntryMode, ObjectKind, RepoPath, hash_object};
use ant_store::ObjectStore;
use anyhow::{Context, anyhow};

use super::index::{Index, stat_of};
use super::{LocalChange, LocalChangeKind, STATE_DIR};
use crate::files::{entry_mode, read_body};
use crate::par::par_map;
use crate::walk::BATCH;

/// Shape of a file on disk: git mode plus its body (target for symlinks).
pub(super) fn hash_disk(full: &Path, meta: &fs::Metadata) -> anyhow::Result<(EntryMode, Vec<u8>)> {
    let what = if meta.file_type().is_symlink() { "reading link" } else { "reading" };
    let body = read_body(full, meta).with_context(|| format!("{what} {}", full.display()))?;
    Ok((entry_mode(meta), body))
}

pub(super) async fn put_blobs(
    objects: &dyn ObjectStore,
    blobs: &[EncodedObject],
) -> anyhow::Result<()> {
    for chunk in blobs.chunks(BATCH) {
        objects.put_many(chunk).await?;
    }
    Ok(())
}

/// What one directory contributed to a [`scan`].
#[derive(Default)]
pub(super) struct DirScan<'a> {
    subdirs: Vec<String>,
    changes: Vec<LocalChange>,
    blobs: Vec<EncodedObject>,
    /// Index keys found on disk.
    seen: Vec<&'a str>,
}

/// Blocking: list the directory `rel` (relative to `dir`, "" for the root)
/// and compare its files against `index`.
pub(super) fn scan_dir<'a>(
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
        let indexed = index.get_key_value(rel_str.as_str());
        if let Some((key, rec)) = indexed {
            out.seen.push(key.as_str());
            let racy = index_mtime.is_some_and(|im| rec.stat.mtime_ns >= im);
            if !racy && rec.stat == stat_of(&meta) {
                continue;
            }
        }
        let (mode, body) = hash_disk(&entry.path(), &meta)?;
        let id = hash_object(ObjectKind::Blob, &body);
        let kind = match indexed {
            Some((_, rec)) if id == rec.id && mode == rec.mode => continue,
            Some(_) => LocalChangeKind::Modified,
            None => LocalChangeKind::Added,
        };
        out.blobs.push(EncodedObject::new(ObjectKind::Blob, &body));
        let path = RepoPath::new(rel_str.as_str())?;
        out.changes.push(LocalChange { path, kind, new: Some((mode, id)) });
    }
    Ok(out)
}

/// Blocking: walk `dir` and compare it against `index`. Returns the changes
/// plus the encoded blobs for every Added/Modified entry. Directories are
/// listed and their files stat'ed in parallel, one tree level at a time.
pub(super) fn scan(
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
