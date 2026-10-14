//! `ant ws update` for lazy workspaces: move the base to a new tree and
//! carry the upper layer's edits over (`antd::update::plan`), atomically
//! with respect to crashes.
//!
//! 1. Under the state lock: recover, compute the local changes and the plan,
//!    and the new whiteout set (an old whiteout stays as it is when nothing
//!    below it must become visible, otherwise it is expanded to the new
//!    base's hidden leaves below it).
//! 2. *Prepare*: merged and conflict-marked files are written to
//!    `staging/<i>` and fsynced. Nothing visible changes; recovery deletes a
//!    staging directory that no journal record refers to.
//! 3. *Commit point*: one durable [`Record::Update`] with the new base,
//!    whiteouts, staged writes, upper files to remove and the conflicts.
//!    Before it the workspace is the old base with its edits, after it the
//!    new base with the merged edits: every reader (`load`, the daemon's
//!    reload, `changes`) runs `recover` first, under the state lock, and
//!    recovery rolls a trailing `Update` forward.
//! 4. *Roll forward* ([`roll_forward`], idempotent): rewrite the `base`
//!    snapshot, move staged files into `upper/`, remove upper files whose
//!    upstream version wins (pruning directories they leave empty), write
//!    the `conflicts` file, delete `staging/`, sync, then append
//!    [`Record::Done`].
//!
//! A live mount notices the journal change on its next namespace operation
//! and reloads, as for `reset`. Files held open for writing across an update
//! keep writing to their old upper inode (see the module docs on `reset`).

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ant_core::{ObjectId, RepoPath};
use ant_store::ObjectStore;
use antd::files::file_perm;
use antd::update::{PathUpdate, Target};

use super::changes::{Local, affected_base, local_changes};
use super::journal::{Record, UpdateRecord, append_durably};
use super::state::{StateLock, atomic, base, recover, save_base};
use super::{metadata, other, under};

const STAGING: &str = "staging";
/// Unresolved conflicts of the workspace (`antd::update::render_conflicts`).
pub const CONFLICTS: &str = "conflicts";

/// Unresolved conflicts recorded in the state directory.
pub fn conflicts(state_dir: &Path) -> io::Result<Vec<RepoPath>> {
    antd::workspace::read_conflicts(&state_dir.join(CONFLICTS)).map_err(other)
}

/// Drop `resolved` from the recorded conflicts (under the state lock).
pub fn resolve(state_dir: &Path, resolved: &[RepoPath]) -> io::Result<()> {
    let _lock = StateLock::take(state_dir)?;
    let left: Vec<String> = conflicts(state_dir)?
        .into_iter()
        .filter(|p| !resolved.contains(p))
        .map(|p| p.to_string())
        .collect();
    write_conflicts(state_dir, &left)
}

fn write_conflicts(state: &Path, paths: &[String]) -> io::Result<()> {
    if paths.is_empty() {
        return match fs::remove_file(state.join(CONFLICTS)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        };
    }
    let text = antd::update::render_conflicts(paths.iter().map(String::as_str));
    atomic(state, CONFLICTS, text.as_bytes())
}

/// Move the workspace to (`commit`, `tree`), carrying local edits over.
/// Refused while conflicts are unresolved. Returns the plan (sorted).
pub async fn update(
    objects: Arc<dyn ObjectStore>,
    state_dir: &Path,
    commit: ObjectId,
    tree: ObjectId,
) -> io::Result<Vec<PathUpdate>> {
    update_with(objects, state_dir, commit, tree, |_| Ok(())).await
}

/// [`update`] with a hook called before each step that changes the state
/// directory (tests return an error to simulate a crash there).
pub(super) async fn update_with(
    objects: Arc<dyn ObjectStore>,
    state_dir: &Path,
    commit: ObjectId,
    tree: ObjectId,
    mut step: impl FnMut(&str) -> io::Result<()> + Send + 'static,
) -> io::Result<Vec<PathUpdate>> {
    let state = state_dir.to_path_buf();
    let rt = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let _lock = StateLock::take(&state)?;
        recover(&state)?;
        let pending = conflicts(&state)?;
        if !pending.is_empty() {
            return Err(other(format!(
                "workspace has {} unresolved conflict(s), e.g. {}",
                pending.len(),
                pending[0]
            )));
        }
        // The plan reads the local versions of changed files by id, which
        // `local_changes` stores.
        let Local { tree: old_tree, deleted, upper, changes: local } =
            local_changes(&state, objects.as_ref(), &rt)?;
        let plan = rt
            .block_on(antd::update::plan(objects.as_ref(), old_tree, tree, &local))
            .map_err(other)?;
        let new_lower = rt.block_on(affected_base(objects.as_ref(), tree, &upper, &deleted))?;

        let mut writes = Vec::new();
        let mut removes = Vec::new();
        for u in &plan {
            match &u.target {
                Target::Write { mode, data } => writes.push((u.path.to_string(), *mode, data)),
                Target::Upstream if upper.files.contains_key(u.path.as_str()) => {
                    removes.push(u.path.to_string());
                }
                _ => {}
            }
        }
        let removed: HashSet<&str> = removes.iter().map(String::as_str).collect();
        let mut in_upper: HashSet<&str> =
            upper.files.keys().map(String::as_str).filter(|p| !removed.contains(p)).collect();
        in_upper.extend(writes.iter().map(|(p, _, _)| p.as_str()));
        let by_path: BTreeMap<&str, &PathUpdate> =
            plan.iter().map(|u| (u.path.as_str(), u)).collect();
        // Must the new base's entry at `path` show through?
        let visible = |path: &str| {
            !in_upper.contains(path)
                && by_path
                    .get(path)
                    .is_some_and(|u| u.target == Target::Upstream && u.upstream.is_some())
        };
        let mut whiteouts = BTreeSet::new();
        for w in &deleted {
            let below: Vec<&String> = new_lower.keys().filter(|p| under(p, w)).collect();
            if below.is_empty() {
                continue;
            }
            if below.iter().any(|p| visible(p)) {
                whiteouts.extend(
                    below
                        .into_iter()
                        .filter(|p| !visible(p) && !in_upper.contains(p.as_str()))
                        .cloned(),
                );
            } else {
                whiteouts.insert(w.clone());
            }
        }
        let conflicts: Vec<String> =
            plan.iter().filter(|u| u.conflict).map(|u| u.path.to_string()).collect();

        // Prepare: staged files are invisible until the record commits them.
        step("prepare")?;
        discard_staging(&state)?;
        let staging = state.join(STAGING);
        fs::create_dir(&staging)?;
        for (i, (_, mode, data)) in writes.iter().enumerate() {
            step("stage")?;
            let perm = file_perm(*mode);
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(perm)
                .open(staging.join(i.to_string()))?;
            f.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(perm))?;
            f.write_all(data)?;
            f.sync_all()?;
        }
        File::open(&staging)?.sync_all()?;
        File::open(&state)?.sync_all()?;

        step("commit")?;
        let record = UpdateRecord {
            commit,
            tree,
            whiteouts: whiteouts.into_iter().collect(),
            writes: writes.iter().map(|(p, _, _)| p.clone()).collect(),
            removes,
            conflicts,
        };
        append_durably(&state, &Record::Update(Box::new(record.clone())))?;
        roll_forward_with(&state, &record, &mut step)?;
        step("done")?;
        append_durably(&state, &Record::Done)?;
        Ok(plan)
    })
    .await
    .map_err(other)?
}

/// Delete a staging directory left by an update that never committed.
pub(super) fn discard_staging(state: &Path) -> io::Result<()> {
    match fs::remove_dir_all(state.join(STAGING)) {
        Ok(()) => File::open(state)?.sync_all(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Finish a committed update (idempotent; the caller holds the state lock
/// and appends [`Record::Done`] afterwards).
pub(super) fn roll_forward(state: &Path, record: &UpdateRecord) -> io::Result<()> {
    roll_forward_with(state, record, &mut |_| Ok(()))
}

fn roll_forward_with(
    state: &Path,
    record: &UpdateRecord,
    step: &mut dyn FnMut(&str) -> io::Result<()>,
) -> io::Result<()> {
    if base(state)? != (record.commit, record.tree) {
        step("base")?;
        save_base(state, record.commit, record.tree)?;
    }
    let upper = state.join("upper");
    let staging = state.join(STAGING);
    let mut dirty: BTreeSet<PathBuf> = BTreeSet::new();
    for (i, path) in record.writes.iter().enumerate() {
        let from = staging.join(i.to_string());
        if metadata(&from)?.is_none() {
            continue; // moved before an interruption
        }
        step("write")?;
        let to = upper.join(path);
        let parent = to.parent().ok_or_else(|| other("upper path has no parent"))?;
        create_dirs(&upper, parent, &mut dirty)?;
        fs::rename(&from, &to)?;
        dirty.insert(parent.to_path_buf());
    }
    for path in &record.removes {
        let full = upper.join(path);
        match fs::remove_file(&full) {
            Ok(()) => step("remove")?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        // Prune directories the removal left empty, so upstream removals
        // show through.
        let mut dir = full.parent().map(Path::to_path_buf);
        while let Some(d) = dir {
            if d == upper || !d.starts_with(&upper) {
                break;
            }
            match fs::remove_dir(&d) {
                Ok(()) => {}
                Err(e)
                    if e.kind() == io::ErrorKind::NotFound
                        || e.raw_os_error() == Some(libc::ENOTEMPTY) =>
                {
                    dirty.insert(d.clone());
                    break;
                }
                Err(e) => return Err(e),
            }
            dir = d.parent().map(Path::to_path_buf);
            if let Some(p) = &dir {
                dirty.insert(p.clone());
            }
        }
        if let Some(p) = full.parent() {
            dirty.insert(p.to_path_buf());
        }
    }
    for d in &dirty {
        match File::open(d) {
            Ok(f) => f.sync_all()?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    step("conflicts")?;
    write_conflicts(state, &record.conflicts)?;
    step("cleanup")?;
    discard_staging(state)
}

/// `create_dir_all` for an upper directory, remembering what to sync.
fn create_dirs(upper: &Path, dir: &Path, dirty: &mut BTreeSet<PathBuf>) -> io::Result<()> {
    if dir == upper || metadata(dir)?.is_some_and(|m| m.is_dir()) {
        return Ok(());
    }
    let parent = dir.parent().ok_or_else(|| other("upper path has no parent"))?;
    create_dirs(upper, parent, dirty)?;
    match fs::create_dir(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    dirty.insert(parent.to_path_buf());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use ant_core::EntryMode;
    use ant_store::graph::build_tree;
    use ant_store::memory::MemoryObjectStore;
    use antd::walk::{Visit, walk};

    use super::*;
    use crate::rw::changes::{compare, scan_upper};
    use crate::rw::journal::{Persisted, load};
    use crate::rw::state::save_whiteouts;

    /// The visible workspace: path → (exec, content).
    type View = BTreeMap<String, (bool, Vec<u8>)>;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap()
    }

    async fn leaves(
        objects: &MemoryObjectStore,
        tree: ObjectId,
    ) -> BTreeMap<String, (EntryMode, ObjectId)> {
        let every = |_: &str, e: &ant_core::TreeEntry, _| {
            if e.mode.is_tree() { Visit::Descend(false) } else { Visit::Leaf }
        };
        walk(objects, tree, every).await.unwrap()
    }

    /// Read the workspace like the overlay does, from the state directory.
    fn view(rt: &tokio::runtime::Runtime, objects: &MemoryObjectStore, state: &Path) -> View {
        let Persisted { tree, deleted, .. } = load(state).unwrap();
        let upper = scan_upper(state).unwrap();
        let lower = rt.block_on(leaves(objects, tree));
        let mut out = View::new();
        for (path, (mode, id)) in &lower {
            let obj = rt.block_on(objects.get_many(&[*id])).unwrap().pop().flatten().unwrap();
            out.insert(path.clone(), (*mode == EntryMode::Executable, obj.body().to_vec()));
        }
        for c in compare(&upper, &deleted, &lower).unwrap() {
            match c.new {
                None => {
                    out.remove(c.path.as_str());
                }
                Some((mode, _)) => {
                    let data = fs::read(state.join("upper").join(c.path.as_str())).unwrap();
                    out.insert(c.path.to_string(), (mode == EntryMode::Executable, data));
                }
            }
        }
        out
    }

    struct Case {
        objects: Arc<MemoryObjectStore>,
        old: ObjectId,
        new: ObjectId,
        _tmp: tempfile::TempDir,
        state: PathBuf,
    }

    /// Old base, new base and a state directory with local edits of every
    /// kind: merged, conflicting, local-only, deleted directory with an
    /// upstream addition, a redundant copy-up and a new file.
    fn case(rt: &tokio::runtime::Runtime) -> Case {
        let objects = Arc::new(MemoryObjectStore::default());
        let old = rt
            .block_on(build_tree(
                objects.as_ref(),
                &[
                    ("merge", &b"1\n2\n3\n4\n5\n"[..]),
                    ("clash", b"1\n2\n3\n"),
                    ("local", b"l"),
                    ("up", b"u"),
                    ("copy", b"c"),
                    ("gone/a", b"a"),
                    ("gone/b", b"b"),
                    ("deep/x/y", b"y"),
                ],
            ))
            .unwrap();
        let new = rt
            .block_on(build_tree(
                objects.as_ref(),
                &[
                    ("merge", &b"one\n2\n3\n4\n5\n"[..]),
                    ("clash", b"1\nup\n3\n"),
                    ("local", b"l"),
                    ("up", b"u2"),
                    ("copy", b"c2"),
                    ("gone/a", b"a"),
                    ("gone/b", b"b"),
                    ("gone/new", b"n"),
                    ("deep/x/z", b"z"),
                ],
            ))
            .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        let upper = state.join("upper");
        fs::create_dir_all(upper.join("deep/x")).unwrap();
        save_base(&state, ObjectId([1; 32]), old).unwrap();
        save_whiteouts(&state, &BTreeSet::from(["gone".to_string()])).unwrap();
        fs::write(upper.join("merge"), b"1\n2\n3\n4\nfive\n").unwrap();
        fs::write(upper.join("clash"), b"1\nmine\n3\n").unwrap();
        fs::write(upper.join("local"), b"l2").unwrap();
        fs::set_permissions(upper.join("local"), fs::Permissions::from_mode(0o755)).unwrap();
        fs::write(upper.join("copy"), b"c").unwrap();
        fs::write(upper.join("added"), b"new").unwrap();
        fs::write(upper.join("deep/x/y"), b"y").unwrap();
        Case { objects, old, new, _tmp: tmp, state }
    }

    fn expected_new() -> View {
        let f = |s: &[u8]| (false, s.to_vec());
        BTreeMap::from([
            ("added".into(), f(b"new")),
            (
                "clash".into(),
                f(b"1\n<<<<<<< upstream\nup\n||||||| base\n2\n=======\nmine\n>>>>>>> local\n3\n"),
            ),
            ("copy".into(), f(b"c2")),
            ("deep/x/z".into(), f(b"z")),
            ("gone/new".into(), f(b"n")),
            ("local".into(), (true, b"l2".to_vec())),
            ("merge".into(), f(b"one\n2\n3\n4\nfive\n")),
            ("up".into(), f(b"u2")),
        ])
    }

    #[test]
    fn update_merges_and_records_conflicts() {
        let rt = runtime();
        let c = case(&rt);
        let before = view(&rt, &c.objects, &c.state);
        assert!(!before.contains_key("gone/a"));
        assert_eq!(before["clash"].1, b"1\nmine\n3\n");
        let plan =
            rt.block_on(update(c.objects.clone(), &c.state, ObjectId([2; 32]), c.new)).unwrap();
        assert_eq!(plan.iter().filter(|u| u.conflict).count(), 1);
        assert_eq!(view(&rt, &c.objects, &c.state), expected_new());
        assert_eq!(base(&c.state).unwrap(), (ObjectId([2; 32]), c.new));
        assert_eq!(conflicts(&c.state).unwrap(), [RepoPath::new("clash").unwrap()]);
        // The redundant copy was dropped; the directory whiteout was expanded.
        assert!(!c.state.join("upper/copy").exists());
        let p = load(&c.state).unwrap();
        assert_eq!(p.deleted, BTreeSet::from(["gone/a".to_string(), "gone/b".to_string()]));
        assert!(!c.state.join(STAGING).exists());
        // Status sees exactly the remaining local edits.
        let changes = rt.block_on(super::super::changes(c.objects.clone(), &c.state)).unwrap();
        let paths: Vec<_> = changes.iter().map(|c| c.path.to_string()).collect();
        assert_eq!(paths, ["added", "clash", "gone/a", "gone/b", "local", "merge"]);
        // Further updates are refused until the conflict is resolved.
        let again = rt.block_on(update(c.objects.clone(), &c.state, ObjectId([2; 32]), c.new));
        assert!(again.unwrap_err().to_string().contains("unresolved conflict"));
        resolve(&c.state, &[RepoPath::new("clash").unwrap()]).unwrap();
        assert!(conflicts(&c.state).unwrap().is_empty());
        let plan =
            rt.block_on(update(c.objects.clone(), &c.state, ObjectId([2; 32]), c.new)).unwrap();
        assert!(plan.iter().all(|u| u.target == Target::Local && !u.conflict));
        assert!(c.old != c.new);
    }

    /// Interrupt the update before every step that changes the state
    /// directory, then recover: the workspace is either entirely at the old
    /// base with its edits or entirely at the new base with merged edits.
    #[test]
    fn update_is_atomic_across_crashes() {
        let rt = runtime();
        let steps = {
            let c = case(&rt);
            let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
            let s = seen.clone();
            rt.block_on(update_with(
                c.objects.clone(),
                &c.state,
                ObjectId([2; 32]),
                c.new,
                move |n| {
                    s.lock().push(n.to_string());
                    Ok(())
                },
            ))
            .unwrap();
            seen.lock().clone()
        };
        assert!(steps.len() >= 8, "{steps:?}");
        let (mut old_states, mut new_states) = (0, 0);
        for (crash_at, step) in steps.iter().enumerate() {
            let c = case(&rt);
            let before = view(&rt, &c.objects, &c.state);
            let mut n = 0;
            let result = rt.block_on(update_with(
                c.objects.clone(),
                &c.state,
                ObjectId([2; 32]),
                c.new,
                move |_| {
                    n += 1;
                    if n > crash_at { Err(other("simulated crash")) } else { Ok(()) }
                },
            ));
            assert!(result.is_err());
            // What any reader does first.
            {
                let _lock = StateLock::take(&c.state).unwrap();
                recover(&c.state).unwrap();
            }
            let got = view(&rt, &c.objects, &c.state);
            let at = load(&c.state).unwrap();
            if at.tree == c.old {
                assert_eq!(got, before, "crash before step {crash_at} ({})", step);
                assert!(conflicts(&c.state).unwrap().is_empty());
                old_states += 1;
            } else {
                assert_eq!(at.tree, c.new);
                assert_eq!(got, expected_new(), "crash before step {crash_at} ({})", step);
                assert_eq!(conflicts(&c.state).unwrap(), [RepoPath::new("clash").unwrap()]);
                new_states += 1;
            }
            assert!(!c.state.join(STAGING).exists());
            assert_eq!(base(&c.state).unwrap().1, at.tree);
            // Recovery is idempotent.
            {
                let _lock = StateLock::take(&c.state).unwrap();
                recover(&c.state).unwrap();
            }
            assert_eq!(view(&rt, &c.objects, &c.state), got);
        }
        assert!(old_states > 0 && new_states > 0, "{old_states} old, {new_states} new");
    }
}
