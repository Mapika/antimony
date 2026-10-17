//! Status ([`changes`]) and rebasing ([`reset`]) of a workspace from its
//! state directory, without walking untouched base subtrees.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self};
use std::io::{self};
use std::path::Path;
use std::sync::Arc;

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, RepoPath, Tree};
use ant_store::ObjectStore;
use antd::files::read_body;
use antd::ignore::{GITIGNORE, Rules};
use antd::walk::{BATCH, Visit, WalkError, walk_cached};
use antd::workspace::{LocalChange, LocalChangeKind};

use super::journal::{Persisted, Record, append_durably, load};
use super::state::*;
use super::{hidden, metadata, mode, other};

pub(super) struct Upper {
    pub(super) files: BTreeMap<String, (EntryMode, ObjectId)>,
    pub(super) dirs: BTreeSet<String>,
    pub(super) blobs: Vec<EncodedObject>,
    /// Entries the ignore rules hide (path → is a directory): neither read
    /// nor entered. Tracked files below them are recovered by
    /// [`restore_tracked`] once the base is known.
    pub(super) ignored: BTreeMap<String, bool>,
    /// Base trees the rule scan read, so [`affected_base`] does not read
    /// them again.
    trees: HashMap<ObjectId, EncodedObject>,
}

/// What [`scan_upper`] needs to apply ignore rules: the base tree, whose
/// `.gitignore` files (when not shadowed by the upper layer) are rules too.
pub(super) struct BaseView<'a> {
    pub(super) objects: &'a dyn ObjectStore,
    pub(super) rt: &'a tokio::runtime::Handle,
    pub(super) tree: ObjectId,
    pub(super) deleted: &'a BTreeSet<String>,
}

/// A directory of the upper layer to scan, with its base tree (when the base
/// has a directory there) and the rules inherited from its parent.
struct Pending {
    dir: String,
    base: Option<ObjectId>,
    rules: Rules,
}

/// What a base directory contributes to the rules: the id of its
/// `.gitignore` blob and the trees of its subdirectories.
#[derive(Default)]
struct BaseDir {
    gitignore: Option<ObjectId>,
    children: HashMap<String, ObjectId>,
}

fn base_dirs(
    view: &BaseView<'_>,
    level: &[Pending],
    trees: &mut HashMap<ObjectId, EncodedObject>,
) -> io::Result<Vec<BaseDir>> {
    let mut out: Vec<BaseDir> = level.iter().map(|_| BaseDir::default()).collect();
    let wanted: Vec<(usize, ObjectId)> =
        level.iter().enumerate().filter_map(|(i, p)| p.base.map(|id| (i, id))).collect();
    for chunk in wanted.chunks(BATCH) {
        let ids: Vec<ObjectId> = chunk.iter().map(|(_, id)| *id).collect();
        let found = view.rt.block_on(view.objects.get_many(&ids)).map_err(other)?;
        if found.len() != ids.len() {
            return Err(other("misaligned tree batch"));
        }
        for ((i, id), obj) in chunk.iter().zip(found) {
            let obj = obj.ok_or_else(|| other(format!("missing base tree {id}")))?;
            if obj.kind() != ObjectKind::Tree {
                return Err(other("base object is not a tree"));
            }
            let dir = &level[*i].dir;
            let decoded = Tree::decode(obj.body()).map_err(other)?;
            trees.insert(*id, obj);
            for e in decoded.entries() {
                let path =
                    if dir.is_empty() { e.name.clone() } else { format!("{dir}/{}", e.name) };
                if hidden(view.deleted, &path) {
                    continue;
                }
                if e.mode.is_tree() {
                    out[*i].children.insert(e.name.clone(), e.id);
                } else if e.name == GITIGNORE && e.mode != EntryMode::Gitlink {
                    out[*i].gitignore = Some(e.id);
                }
            }
        }
    }
    Ok(out)
}

/// Read the upper layer. With a `view`, entries matching the ignore rules
/// (`<state>/meta/ignore`, then the `.gitignore` files of the visible
/// workspace: upper version, else base version) are recorded in
/// [`Upper::ignored`] instead of being read; without one nothing is ignored.
pub(super) fn scan_upper(state: &Path, view: Option<&BaseView<'_>>) -> io::Result<Upper> {
    let root = state.join("upper");
    let mut out = Upper {
        files: BTreeMap::new(),
        dirs: BTreeSet::new(),
        blobs: Vec::new(),
        ignored: BTreeMap::new(),
        trees: HashMap::new(),
    };
    let local = view
        .and_then(|_| fs::read(state.join("meta").join("ignore")).ok())
        .map(antd::ignore::lossy);
    let mut level = vec![Pending {
        dir: String::new(),
        base: view.map(|v| v.tree),
        rules: Rules::none().child("", local.as_deref()),
    }];
    while !level.is_empty() {
        let mut listings = Vec::with_capacity(level.len());
        for p in &level {
            let mut entries = Vec::new();
            for entry in fs::read_dir(root.join(&p.dir))? {
                let entry = entry?;
                let name =
                    entry.file_name().into_string().map_err(|_| other("non-UTF8 repo path"))?;
                if p.dir.is_empty() && name == ".ant" {
                    continue;
                }
                let path =
                    if p.dir.is_empty() { name.clone() } else { format!("{}/{name}", p.dir) };
                RepoPath::new(&path).map_err(other)?;
                let m = fs::symlink_metadata(entry.path())?;
                entries.push((name, path, m, entry.path()));
            }
            listings.push(entries);
        }
        // The rules of each directory: the upper `.gitignore` when there is
        // one, else the base's, read in one batch for the level.
        let mut texts: Vec<Option<String>> = vec![None; level.len()];
        let mut bases: Vec<BaseDir> = Vec::new();
        if let Some(v) = view {
            bases = base_dirs(v, &level, &mut out.trees)?;
            let mut fetch: Vec<(usize, ObjectId)> = Vec::new();
            for (i, entries) in listings.iter().enumerate() {
                let upper_file = entries.iter().find(|(n, _, m, _)| n == GITIGNORE && !m.is_dir());
                if let Some((_, _, _, full)) = upper_file {
                    texts[i] = fs::read(full).ok().map(antd::ignore::lossy);
                } else if let Some(id) = bases[i].gitignore {
                    fetch.push((i, id));
                }
            }
            for chunk in fetch.chunks(BATCH) {
                let ids: Vec<ObjectId> = chunk.iter().map(|(_, id)| *id).collect();
                let found = v.rt.block_on(v.objects.get_many(&ids)).map_err(other)?;
                if found.len() != ids.len() {
                    return Err(other("misaligned blob batch"));
                }
                for ((i, _), obj) in chunk.iter().zip(found) {
                    texts[*i] = obj.map(|o| antd::ignore::lossy(o.body().to_vec()));
                }
            }
        }
        let mut next = Vec::new();
        for (i, entries) in listings.into_iter().enumerate() {
            let p = &level[i];
            let rules = if view.is_some() {
                p.rules.child(&p.dir, texts[i].as_deref())
            } else {
                Rules::none()
            };
            for (name, path, m, full) in entries {
                if view.is_some() && rules.is_ignored(&path, m.is_dir()) {
                    if m.is_dir() {
                        out.dirs.insert(path.clone());
                    }
                    out.ignored.insert(path, m.is_dir());
                } else if m.is_dir() {
                    out.dirs.insert(path.clone());
                    let base = bases.get(i).and_then(|b| b.children.get(&name)).copied();
                    next.push(Pending { dir: path, base, rules: rules.clone() });
                } else {
                    let data = read_body(&full, &m)?;
                    let blob = EncodedObject::new(ObjectKind::Blob, &data);
                    out.files.insert(path, (mode(&m), blob.id()));
                    out.blobs.push(blob);
                }
            }
        }
        level = next;
    }
    Ok(out)
}

/// Tracked files below (or at) ignored entries keep being compared: read
/// their upper copies (`lower` holds every base entry below ignored paths,
/// see [`affected_base`]). Returns the ignored entries that contain no
/// tracked path: the untracked ignored paths, directories with a trailing `/`.
pub(super) fn restore_tracked(
    state: &Path,
    upper: &mut Upper,
    lower: &BTreeMap<String, (EntryMode, ObjectId)>,
) -> io::Result<Vec<String>> {
    if upper.ignored.is_empty() {
        return Ok(Vec::new());
    }
    let root = state.join("upper");
    for path in lower.keys() {
        let covered = upper.ignored.contains_key(path.as_str())
            || path.match_indices('/').any(|(i, _)| upper.ignored.contains_key(&path[..i]));
        if !covered || upper.files.contains_key(path) {
            continue;
        }
        let full = root.join(path);
        let Some(m) = metadata(&full)? else { continue };
        if m.is_dir() {
            continue;
        }
        let data = read_body(&full, &m)?;
        let blob = EncodedObject::new(ObjectKind::Blob, &data);
        upper.files.insert(path.clone(), (mode(&m), blob.id()));
        upper.blobs.push(blob);
    }
    let mut untracked = Vec::new();
    for (path, is_dir) in &upper.ignored {
        let tracked = if *is_dir {
            let prefix = format!("{path}/");
            lower.range(prefix.clone()..).next().is_some_and(|(k, _)| k.starts_with(&prefix))
        } else {
            lower.contains_key(path)
        };
        if !tracked {
            untracked.push(if *is_dir { format!("{path}/") } else { path.clone() });
        }
    }
    untracked.sort();
    Ok(untracked)
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
    exact.extend(upper.ignored.keys().cloned());
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
    walk_cached(objects, root, &upper.trees, |path, e, inherited| {
        let all = inherited
            || deleted.contains(path)
            || upper.files.contains_key(path)
            || upper.ignored.contains_key(path);
        if !all && !exact.contains(path) && !branches.contains(path) {
            Visit::Skip
        } else if e.mode.is_tree() {
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
    /// Untracked ignored paths, sorted (directories end in `/`).
    pub(super) ignored: Vec<String>,
}

/// Compute [`Local`] and store the new blobs of changed files into `objects`
/// in batches, so later readers (submit, `update`'s plan) find them by id.
pub(super) fn local_changes(
    state: &Path,
    objects: &dyn ObjectStore,
    rt: &tokio::runtime::Handle,
) -> io::Result<Local> {
    let Persisted { tree, deleted, .. } = load(state)?;
    let view = BaseView { objects, rt, tree, deleted: &deleted };
    let mut upper = scan_upper(state, Some(&view))?;
    let lower = rt.block_on(affected_base(objects, tree, &upper, &deleted))?;
    let ignored = restore_tracked(state, &mut upper, &lower)?;
    let changes = compare(&upper, &deleted, &lower)?;
    let changed: BTreeSet<_> = changes.iter().filter_map(|c| c.new.map(|(_, id)| id)).collect();
    let blobs: Vec<_> = upper.blobs.iter().filter(|b| changed.contains(&b.id())).cloned().collect();
    for batch in blobs.chunks(BATCH) {
        rt.block_on(objects.put_many(batch)).map_err(other)?;
    }
    Ok(Local { tree, deleted, upper, changes, ignored })
}

/// Compute sorted changes without walking untouched base subtrees. Blocking
/// filesystem work and state locking run on a blocking thread. New blobs are
/// inserted in batches into `objects`.
pub async fn changes(
    objects: Arc<dyn ObjectStore>,
    state_dir: &Path,
) -> io::Result<Vec<LocalChange>> {
    Ok(changes_with_ignored(objects, state_dir).await?.0)
}

/// [`changes`] plus the untracked paths the ignore rules hide (sorted;
/// directories end in `/`). Ignored entries are neither read nor reported;
/// a tracked file below an ignored path is still compared. An ignored
/// directory that also holds tracked files is not listed.
pub async fn changes_with_ignored(
    objects: Arc<dyn ObjectStore>,
    state_dir: &Path,
) -> io::Result<(Vec<LocalChange>, Vec<String>)> {
    let state = state_dir.to_path_buf();
    let rt = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let _lock = StateLock::take(&state)?;
        recover(&state)?;
        let local = local_changes(&state, objects.as_ref(), &rt)?;
        Ok((local.changes, local.ignored))
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
        let Persisted { tree: old_tree, deleted, .. } = load(&state)?;
        let view =
            BaseView { objects: objects.as_ref(), rt: &rt, tree: old_tree, deleted: &deleted };
        let mut upper = scan_upper(&state, Some(&view))?;
        let lower = rt.block_on(affected_base(objects.as_ref(), tree, &upper, &deleted))?;
        restore_tracked(&state, &mut upper, &lower)?;
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
