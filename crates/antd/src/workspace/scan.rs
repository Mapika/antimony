//! `status`: comparing the files on disk against the index.

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::OnceLock;

use ant_core::{EncodedObject, EntryMode, ObjectKind, RepoPath, hash_object};
use ant_store::ObjectStore;
use anyhow::{Context, anyhow};

use super::index::{Index, stat_of, state_dir};
use super::{IGNORE_FILE, LocalChange, LocalChangeKind, STATE_DIR};
use crate::files::{entry_mode, read_body};
use crate::ignore::{GITIGNORE, Rules};
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

/// A directory to scan: its path, the ignore rules in force in it, and whether
/// it lies inside an ignored directory that was entered only because it
/// contains tracked files (everything untracked there is ignored).
pub(super) struct Dir {
    rel: String,
    rules: Rules,
    in_ignored: bool,
}

/// Directories that contain at least one indexed path (ancestors of index
/// keys), computed on first use: only an ignored directory needs the answer.
pub(super) struct TrackedDirs<'a> {
    index: &'a Index,
    dirs: OnceLock<HashSet<&'a str>>,
}

impl<'a> TrackedDirs<'a> {
    fn new(index: &'a Index) -> Self {
        Self { index, dirs: OnceLock::new() }
    }

    fn contains(&self, dir: &str) -> bool {
        self.dirs
            .get_or_init(|| {
                let mut dirs = HashSet::new();
                for key in self.index.keys() {
                    for (i, _) in key.match_indices('/') {
                        dirs.insert(&key[..i]);
                    }
                }
                dirs
            })
            .contains(dir)
    }
}

/// What one directory contributed to a [`scan`].
#[derive(Default)]
pub(super) struct DirScan<'a> {
    subdirs: Vec<Dir>,
    changes: Vec<LocalChange>,
    blobs: Vec<EncodedObject>,
    /// Index keys found on disk.
    seen: Vec<&'a str>,
    /// Untracked ignored entries (directories with a trailing `/`), not
    /// entered and not hashed.
    ignored: Vec<String>,
}

/// Blocking: list the directory `item` (relative to `dir`) and compare its
/// files against `index`.
pub(super) fn scan_dir<'a>(
    dir: &Path,
    item: &Dir,
    index: &'a Index,
    tracked: &TrackedDirs<'_>,
    index_mtime: Option<i64>,
) -> anyhow::Result<DirScan<'a>> {
    let mut out = DirScan::default();
    let rel = item.rel.as_str();
    let full_dir = if rel.is_empty() { dir.to_path_buf() } else { dir.join(rel) };
    let listing =
        fs::read_dir(&full_dir).with_context(|| format!("walking {}", full_dir.display()))?;
    let mut entries = Vec::new();
    for entry in listing {
        let entry = entry.with_context(|| format!("walking {}", full_dir.display()))?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| anyhow!("non-UTF-8 path {}", full_dir.join(&name).display()))?
            .to_string();
        if rel.is_empty() && name == STATE_DIR {
            continue;
        }
        entries.push((name, entry));
    }
    // This directory's own rule file (unreadable: no rules; bytes that are not UTF-8 read lossily, so they
    // match nothing but leave the other rules in force, as in git).
    let rules = if item.in_ignored {
        Rules::none()
    } else if entries.iter().any(|(n, _)| n == GITIGNORE) {
        let text = fs::read(full_dir.join(GITIGNORE)).ok().map(crate::ignore::lossy);
        item.rules.child(rel, text.as_deref())
    } else {
        item.rules.clone()
    };
    for (name, entry) in entries {
        let ft = entry.file_type()?;
        let rel_str = if rel.is_empty() { name } else { format!("{rel}/{name}") };
        if ft.is_dir() {
            // Directories (including empty ones) are not tracked themselves.
            // An ignored one is entered only to reach tracked files.
            let ignored = item.in_ignored || rules.is_ignored(&rel_str, true);
            if !ignored {
                out.subdirs.push(Dir { rel: rel_str, rules: rules.clone(), in_ignored: false });
            } else if tracked.contains(&rel_str) {
                out.subdirs.push(Dir { rel: rel_str, rules: Rules::none(), in_ignored: true });
            } else {
                out.ignored.push(format!("{rel_str}/"));
            }
            continue;
        }
        if !(ft.is_file() || ft.is_symlink()) {
            continue;
        }
        let indexed = index.get_key_value(rel_str.as_str());
        if indexed.is_none() && (item.in_ignored || rules.is_ignored(&rel_str, false)) {
            out.ignored.push(rel_str);
            continue;
        }
        let meta = entry.metadata()?;
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

/// Result of a [`scan`].
pub(super) struct Scan {
    pub changes: Vec<LocalChange>,
    pub blobs: Vec<EncodedObject>,
    /// Untracked ignored paths, sorted; directories end in `/`.
    pub ignored: Vec<String>,
}

/// Blocking: walk `dir` and compare it against `index`. Returns the changes
/// plus the encoded blobs for every Added/Modified entry. Directories are
/// listed and their files stat'ed in parallel, one tree level at a time.
/// Untracked files matching the ignore rules (`.ant/ignore`, `.gitignore`
/// files; see [`crate::ignore`]) are left out, and ignored directories are not
/// entered unless they contain tracked files.
pub(super) fn scan(dir: &Path, index: &Index, index_mtime: Option<i64>) -> anyhow::Result<Scan> {
    let mut changes = Vec::new();
    let mut blobs = Vec::new();
    let mut ignored = Vec::new();
    let mut seen: Vec<&str> = Vec::with_capacity(index.len());
    let local = fs::read(state_dir(dir).join(IGNORE_FILE)).ok();
    let local = local.map(crate::ignore::lossy);
    let base = Rules::none().child("", local.as_deref());
    let tracked = TrackedDirs::new(index);
    let mut level = vec![Dir { rel: String::new(), rules: base, in_ignored: false }];
    while !level.is_empty() {
        let results = par_map(&level, 8, |d| scan_dir(dir, d, index, &tracked, index_mtime));
        let mut next = Vec::new();
        for r in results {
            let r = r?;
            next.extend(r.subdirs);
            changes.extend(r.changes);
            blobs.extend(r.blobs);
            seen.extend(r.seen);
            ignored.extend(r.ignored);
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
    ignored.sort();
    Ok(Scan { changes, blobs, ignored })
}
