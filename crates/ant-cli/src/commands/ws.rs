//! Workspace commands: `ws new [--lazy] / update / resolve / remount /
//! unmount`, `status`, `submit`, `land`, and the hidden `ws serve` (the lazy
//! workspace's FUSE daemon, started by `ws new --lazy` and `ws remount`).

use std::fmt::Write as _;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ant_core::{EncodedObject, ObjectId};
use ant_proto::v1 as pb;
use ant_store::{ObjectStore, ObjectStoreExt};
use antd::disk::DiskObjectStore;
use antd::readthrough::ReadThrough;
use antd::workspace::{LocalChange, LocalChangeKind, Sparse, Workspace};
use anyhow::{Context as _, Result, anyhow, bail};

use crate::author::author_signature;
use crate::ctx::{Ctx, Outcome, RpcResult, id_from, print_hint, resolve_commit};
use crate::discover::{require_workspace, start_dir};

const LAST_SUBMIT: &str = ".ant/last_submit";
/// The ref `ws update` follows by default (written by `ws new --at <ref>`).
const TRACK: &str = "track";
const DEFAULT_REF: &str = "main";

/// `at` if it names an existing ref (so `ws update` can follow it).
async fn tracked_ref(remote: &antd::remote::RemoteObjectStore, at: &str) -> Result<Option<String>> {
    let r = remote.client().get_ref(pb::GetRefRequest { name: at.into() }).await.rpc()?;
    Ok(r.into_inner().target.map(|_| at.to_string()))
}

async fn read_track(root: &Path) -> Result<String> {
    match tokio::fs::read_to_string(root.join(".ant").join(TRACK)).await {
        Ok(name) if !name.trim().is_empty() => Ok(name.trim().to_string()),
        Ok(_) => Ok(DEFAULT_REF.into()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(DEFAULT_REF.into()),
        Err(e) => Err(e.into()),
    }
}

/// Poll `done` every 25 ms until it holds or `timeout` passes; returns
/// whether it held.
async fn poll_until<F, Fut>(timeout: Duration, mut done: F) -> Result<bool>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if done().await? {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Run `fusermount3 -u [-z] <dir>` off the async threads; fails with
/// `<what> failed: <stderr>` when it exits unsuccessfully.
async fn fusermount_unmount(dir: PathBuf, lazy: bool, what: &str) -> Result<()> {
    let output = tokio::task::spawn_blocking(move || {
        let mut cmd = std::process::Command::new("fusermount3");
        cmd.arg("-u");
        if lazy {
            cmd.arg("-z");
        }
        cmd.arg(&dir).output()
    })
    .await?
    .context("running fusermount3")?;
    if !output.status.success() {
        bail!("{what} failed: {}", String::from_utf8_lossy(&output.stderr).trim());
    }
    Ok(())
}

/// The workspace's unresolved conflicts file.
async fn conflicts_file(root: &Path) -> Result<PathBuf> {
    Ok(match lazy_state(root).await? {
        Some(state) => state.join(ant_fuse::rw::CONFLICTS),
        None => antd::workspace::conflicts_file(root),
    })
}

async fn pending_conflicts(root: &Path) -> Result<Vec<ant_core::RepoPath>> {
    let file = conflicts_file(root).await?;
    tokio::task::spawn_blocking(move || antd::workspace::read_conflicts(&file)).await?
}

/// Error out while conflicts are unresolved (see `hint_for`).
async fn require_no_conflicts(root: &Path) -> Result<()> {
    let pending = pending_conflicts(root).await?;
    if !pending.is_empty() {
        let list: Vec<String> = pending.iter().map(|p| p.to_string()).collect();
        bail!("workspace has unresolved conflicts: {}", list.join(", "));
    }
    Ok(())
}

pub async fn new(ctx: &Ctx, dir: &Path, at: &str, patterns: Vec<String>) -> Result<Outcome> {
    let sparse = if patterns.is_empty() { Sparse::all() } else { Sparse::new(patterns)? };
    let (rt, remote) = ctx.read_through().await?;
    let commit = resolve_commit(&mut remote.client(), at).await?;
    let tree = rt.read_commit(&commit).await?.tree;
    let track = tracked_ref(&remote, at).await?;
    Workspace::materialize_with(rt.as_ref(), Some(rt.as_ref()), dir, commit, tree, sparse).await?;
    if let Some(name) = track {
        tokio::fs::write(dir.join(".ant").join(TRACK), format!("{name}\n")).await?;
    }
    Ok(Outcome::ok(format!("{commit}\n")))
}

fn letter(k: LocalChangeKind) -> char {
    match k {
        LocalChangeKind::Added => 'A',
        LocalChangeKind::Modified => 'M',
        LocalChangeKind::Deleted => 'D',
    }
}

/// The workspace root, its config, local changes and the untracked paths the
/// ignore rules hide (sorted; directories end in `/`).
async fn local_changes(ctx: &Ctx) -> Result<(PathBuf, Workspace, Vec<LocalChange>, Vec<String>)> {
    let root = require_workspace(ctx.directory.as_deref())?;
    let ws = Workspace::open(&root)?;
    let disk = ctx.disk()?;
    let (changes, ignored) = if let Some(state) = lazy_state(&root).await? {
        let (read, _) = ctx.read_through().await?;
        let objects = Arc::new(LazyObjects { read, local: disk });
        ant_fuse::rw::changes_with_ignored(objects, &state).await?
    } else {
        ws.status_with_ignored(disk.as_ref()).await?
    };
    Ok((root, ws, changes, ignored))
}

pub async fn status(ctx: &Ctx, show_ignored: bool) -> Result<Outcome> {
    let (root, _, changes, ignored) = local_changes(ctx).await?;
    let mut lines: std::collections::BTreeMap<String, char> =
        changes.into_iter().map(|c| (c.path.to_string(), letter(c.kind))).collect();
    for path in pending_conflicts(&root).await? {
        lines.insert(path.to_string(), 'C');
    }
    if show_ignored {
        for path in ignored {
            lines.entry(path).or_insert('!');
        }
    }
    let mut out = String::new();
    for (path, letter) in lines {
        writeln!(out, "{letter}\t{path}")?;
    }
    Ok(Outcome::ok(out))
}

pub async fn submit(ctx: &Ctx, message: &str) -> Result<Outcome> {
    require_no_conflicts(&require_workspace(ctx.directory.as_deref())?).await?;
    let (root, ws, changes, _) = local_changes(ctx).await?;
    if changes.is_empty() {
        bail!("nothing to submit");
    }
    let disk = ctx.disk()?;

    // Upload the changed blobs. The status scan stored them in the local
    // cache, but the cache may have evicted some since (tiny budget, `ant
    // cache gc`); those are re-read from the workspace and hash-checked.
    let ids: Vec<ObjectId> = changes.iter().filter_map(|c| c.new.map(|(_, id)| id)).collect();
    let mut blobs: Vec<EncodedObject> = Vec::with_capacity(ids.len());
    let source = match lazy_state(&root).await? {
        Some(state) => state.join("upper"),
        None => root.clone(),
    };
    let cached = disk.get_many(&ids).await?;
    for (change, obj) in changes.iter().filter(|c| c.new.is_some()).zip(cached) {
        blobs.push(match obj {
            Some(obj) => obj,
            None => restage_blob(&source, change).await?,
        });
    }
    let remote = ctx.remote().await?;
    remote.put_many(&blobs).await.context("uploading blobs")?;

    let ops = changes
        .iter()
        .map(|c| pb::FileOp {
            path: c.path.to_string(),
            op: Some(match c.new {
                Some((mode, id)) => pb::file_op::Op::Write(pb::WriteOp {
                    mode: pb::EntryMode::from(mode) as i32,
                    content: Some(pb::write_op::Content::BlobId(id.as_bytes().to_vec())),
                }),
                None => pb::file_op::Op::Delete(pb::DeleteOp {}),
            }),
        })
        .collect();
    let author = author_signature().await?;
    let r = remote
        .client()
        .create_changeset(pb::CreateChangesetRequest {
            base: Some(ws.base_commit.as_bytes().to_vec()),
            ops,
            author: Some(author),
            committer: None,
            message: message.to_string(),
        })
        .await
        .rpc()?
        .into_inner();
    let commit = id_from(&r.commit)?;
    tokio::fs::write(root.join(LAST_SUBMIT), format!("{commit}\n")).await?;
    Ok(Outcome::ok(format!("{commit}\n")))
}

/// Re-read the content of `change` from the workspace file under `source`
/// (the working tree, or a lazy workspace's upper layer) after the cache lost
/// its staged blob. The content is used only if it still hashes to the id the
/// scan computed; otherwise the file changed in between and submit fails
/// rather than upload something other than what was reported.
async fn restage_blob(source: &Path, change: &LocalChange) -> Result<EncodedObject> {
    let Some((_, id)) = change.new else {
        bail!("{} has no new content", change.path);
    };
    let file = source.join(change.path.as_str());
    let body = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
        let meta = std::fs::symlink_metadata(&file)?;
        antd::files::read_body(&file, &meta)
    })
    .await?
    .with_context(|| format!("reading {} to restore its evicted blob {id}", change.path))?;
    let blob = EncodedObject::new(ant_core::ObjectKind::Blob, &body);
    if blob.id() != id {
        print_hint("the file changed while submitting; run `ant status`, then `ant submit` again");
        bail!("{} changed after it was scanned (blob {id} is gone from the cache)", change.path);
    }
    Ok(blob)
}

pub async fn land(ctx: &Ctx, ref_name: &str, commit: Option<String>) -> Result<Outcome> {
    // The workspace is optional only when an explicit commit is given.
    let root = match (require_workspace(ctx.directory.as_deref()), &commit) {
        (Ok(r), _) => Some(r),
        (Err(_), Some(_)) => None,
        (Err(e), None) => return Err(e),
    };
    let commit_hex = match commit {
        Some(c) => c,
        None => {
            // Never fails: without a commit argument the workspace is required.
            let root = root.as_ref().ok_or_else(|| anyhow!("no workspace"))?;
            let p = root.join(LAST_SUBMIT);
            tokio::fs::read_to_string(&p)
                .await
                .map_err(|_| {
                    anyhow!("no commit given and {} not found; run `ant submit` first", p.display())
                })?
                .trim()
                .to_string()
        }
    };
    let (rt, remote) = ctx.read_through().await?;
    let mut client = remote.client();
    let commit = resolve_commit(&mut client, &commit_hex).await?.as_bytes().to_vec();
    let r = client
        .land(pb::LandRequest { ref_name: ref_name.into(), commit })
        .await
        .rpc()?
        .into_inner();
    match r.outcome {
        Some(pb::land_response::Outcome::Landed(l)) => {
            let tip = id_from(&l.new_tip)?;
            if let Some(root) = root {
                let tree = rt.read_commit(&tip).await?.tree;
                let mut ws = Workspace::open(&root)?;
                // The new tip's trees and blobs are not cached locally yet, so
                // the reset must read through to the server.
                if let Some(state) = lazy_state(&root).await? {
                    ant_fuse::rw::reset(rt.clone(), &state, tip, tree)
                        .await
                        .context("resetting lazy workspace")?;
                } else {
                    ws.reset_to(rt.as_ref(), tip, tree)
                        .await
                        .context("resetting workspace to new tip")?;
                }
            }
            Ok(Outcome::ok(format!("landed {tip}{}\n", if l.rebased { " rebased" } else { "" })))
        }
        Some(pb::land_response::Outcome::Conflict(c)) => {
            let mut out = String::from("conflict\n");
            for p in c.paths {
                writeln!(out, "{p}")?;
            }
            print_hint(&format!(
                "{ref_name} moved under you; run `ant ws update --to {ref_name}` to merge \
                 your edits onto the new tip, resolve any conflicts, then submit and land again"
            ));
            Ok(Outcome::with_code(out, 2))
        }
        None => bail!("server returned an empty land response"),
    }
}

pub async fn update(ctx: &Ctx, to: Option<String>) -> Result<Outcome> {
    let root = require_workspace(ctx.directory.as_deref())?;
    require_no_conflicts(&root).await?;
    let rev = match to {
        Some(rev) => rev,
        None => read_track(&root).await?,
    };
    let old = Workspace::open(&root)?.base_commit;
    let (read, remote) = ctx.read_through().await?;
    let commit = resolve_commit(&mut remote.client(), &rev).await?;
    let tree = read.read_commit(&commit).await?.tree;
    // Reads go through to the server; local and merged blobs stay local.
    let objects = Arc::new(LazyObjects { read, local: ctx.disk()? });
    let plan = match lazy_state(&root).await? {
        Some(state) => ant_fuse::rw::update(objects, &state, commit, tree)
            .await
            .context("updating lazy workspace")?,
        None => Workspace::open(&root)?.update(objects.as_ref(), commit, tree).await?,
    };
    if commit != old {
        // A commit submitted from the old base is superseded by the update.
        match tokio::fs::remove_file(root.join(LAST_SUBMIT)).await {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
    }
    let mut out = format!("updated {commit}\n");
    let conflicts: Vec<_> = plan.iter().filter(|u| u.conflict).collect();
    for u in &conflicts {
        writeln!(out, "C\t{}", u.path)?;
    }
    if conflicts.is_empty() {
        return Ok(Outcome::ok(out));
    }
    print_hint(&format!(
        "fix the listed files (text conflicts carry <<<<<<< markers; other conflicts keep \
         your version, `ant cat {commit}:<path>` shows upstream's), then run \
         `ant ws resolve <path>...` before submitting"
    ));
    Ok(Outcome::with_code(out, 2))
}

pub async fn resolve(ctx: &Ctx, paths: Vec<String>) -> Result<Outcome> {
    let root = require_workspace(ctx.directory.as_deref())?;
    let pending = pending_conflicts(&root).await?;
    let mut resolved = Vec::with_capacity(paths.len());
    for p in paths {
        let path = ant_core::RepoPath::new(p.trim_matches('/'))
            .with_context(|| format!("invalid path {p:?}"))?;
        if !pending.contains(&path) {
            bail!("{path} is not conflicted");
        }
        match tokio::fs::read(root.join(path.as_str())).await {
            Ok(data) if ant_core::merge::has_conflict_markers(&data) => {
                bail!("{path} still contains conflict markers");
            }
            Ok(_) => {}
            // Resolved by deleting it (or turning it into a directory).
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    || e.kind() == std::io::ErrorKind::IsADirectory => {}
            Err(e) => return Err(e).with_context(|| format!("reading {path}")),
        }
        resolved.push(path);
    }
    match lazy_state(&root).await? {
        Some(state) => ant_fuse::rw::resolve(&state, &resolved)?,
        None => {
            let left: Vec<_> = pending.into_iter().filter(|p| !resolved.contains(p)).collect();
            let file = conflicts_file(&root).await?;
            tokio::task::spawn_blocking(move || antd::workspace::write_conflicts(&file, &left))
                .await??;
        }
    }
    Ok(Outcome::empty())
}

async fn lazy_state(root: &Path) -> Result<Option<PathBuf>> {
    match tokio::fs::read_to_string(root.join(".ant/lazy")).await {
        Ok(path) => Ok(Some(path.into())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::NotConnected => {
            Err(crate::discover::DeadMount(root.to_path_buf()).into())
        }
        Err(e) => Err(e.into()),
    }
}

pub async fn new_lazy(ctx: &Ctx, dir: &Path, at: &str) -> Result<Outcome> {
    let dir = std::path::absolute(dir)?;
    if let Some(state) = lazy_state(&dir).await? {
        spawn_lazy(ctx, &dir, &state).await?;
        let config = tokio::fs::read_to_string(state.join("base")).await?;
        let commit = config
            .lines()
            .find_map(|l| l.strip_prefix("base_commit="))
            .ok_or_else(|| anyhow!("invalid lazy base"))?;
        return Ok(Outcome::ok(format!("{commit}\n")));
    }
    let (objects, remote) = ctx.read_through().await?;
    let commit = resolve_commit(&mut remote.client(), at).await?;
    let tree = objects.read_commit(&commit).await?.tree;
    let track = tracked_ref(&remote, at).await?;
    let cache = std::path::absolute(&ctx.cache)?;
    let d = dir.clone();
    let state = tokio::task::spawn_blocking(move || -> Result<PathBuf> {
        use std::os::unix::{ffi::OsStrExt, fs::symlink};
        std::fs::create_dir_all(&d)?;
        let d = std::fs::canonicalize(d)?;
        if std::fs::read_dir(&d)?.next().is_some() {
            bail!("workspace directory is not empty");
        }
        let id = ant_core::hash_object(ant_core::ObjectKind::Blob, d.as_os_str().as_bytes());
        let state = cache.join("ws").join(id.to_hex());
        if state.starts_with(&d) {
            bail!("lazy workspace state must be outside the mountpoint; choose another --cache");
        }
        if state.exists() {
            bail!("lazy workspace state already exists at {}", state.display());
        }
        std::fs::create_dir_all(state.join("upper"))?;
        std::fs::create_dir(state.join("meta"))?;
        std::fs::write(state.join("base"), format!("base_commit={commit}\nbase_tree={tree}\n"))?;
        std::fs::write(state.join("whiteouts"), "")?;
        std::fs::write(state.join("meta/lazy"), state.as_os_str().as_bytes())?;
        if let Some(name) = track {
            std::fs::write(state.join("meta").join(TRACK), format!("{name}\n"))?;
        }
        symlink("../base", state.join("meta/workspace"))?;
        symlink(state.join("meta"), state.join("upper/.ant"))?;
        symlink(state.join("meta"), d.join(".ant"))?;
        Ok(state)
    })
    .await??;
    spawn_lazy(ctx, &dir, &state).await?;
    Ok(Outcome::ok(format!("{commit}\n")))
}

async fn spawn_lazy(ctx: &Ctx, dir: &Path, state: &Path) -> Result<()> {
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.arg("--server")
        .arg(&ctx.server)
        .arg("--cache")
        .arg(std::path::absolute(&ctx.cache)?)
        .arg("--cache-bytes")
        .arg(ctx.cache_bytes.to_string());
    if let Some(http) = &ctx.http {
        cmd.arg("--http").arg(http);
    }
    cmd.arg("ws")
        .arg("serve")
        .arg(dir)
        .arg(state)
        .current_dir("/")
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::null());
    let log = tokio::fs::File::create(state.join("daemon.log")).await?.into_std().await;
    cmd.stderr(log);
    let mut child = tokio::task::spawn_blocking(move || cmd.spawn())
        .await?
        .context("starting lazy workspace daemon")?;
    let ready = state.join("ready");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::fs::read_to_string(&ready).await.is_ok_and(|pid| pid == child.id().to_string()) {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            let log = tokio::fs::read_to_string(state.join("daemon.log")).await?;
            bail!("lazy workspace daemon exited ({status}): {log}");
        }
        if tokio::time::Instant::now() >= deadline {
            child.kill()?;
            tokio::task::spawn_blocking(move || child.wait()).await??;
            bail!("timed out mounting lazy workspace");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub async fn serve(ctx: &Ctx, dir: &Path, state: &Path) -> Result<Outcome> {
    let (objects, _) = ctx.read_through_daemon().await?;
    let d = dir.to_path_buf();
    let s = state.to_path_buf();
    let handle = tokio::runtime::Handle::current();
    let session = tokio::task::spawn_blocking(move || {
        let (commit, tree) = ant_fuse::rw::base(&s)?;
        ant_fuse::mount_rw(objects, commit, tree, &d, &s, handle, ant_fuse::MountOptions::default())
    })
    .await??;
    tokio::fs::write(state.join("ready"), std::process::id().to_string()).await?;
    tokio::task::spawn_blocking(move || session.join()).await??;
    if tokio::fs::read_to_string(state.join("ready"))
        .await
        .is_ok_and(|pid| pid == std::process::id().to_string())
    {
        tokio::fs::remove_file(state.join("ready")).await?;
    }
    Ok(Outcome::empty())
}

/// Remount a lazy workspace: detach a dead mount, then start a daemon on the
/// existing state (recovery runs at mount).
pub async fn remount(ctx: &Ctx, dir: Option<PathBuf>) -> Result<Outcome> {
    use crate::discover::{find_dead_mount, find_workspace, is_dead};
    let dir = match dir {
        Some(d) => std::path::absolute(d)?,
        None => {
            let start = start_dir(ctx.directory.as_deref())?;
            find_dead_mount(&start)
                .or_else(|| find_workspace(&start))
                .ok_or_else(|| anyhow!("not inside an ant workspace (from {})", start.display()))?
        }
    };
    if is_dead(&dir) {
        fusermount_unmount(dir.clone(), true, "detaching dead mount").await?;
        if !poll_until(Duration::from_secs(5), || async { Ok(!is_dead(&dir)) }).await? {
            bail!("dead mount {} did not detach", dir.display());
        }
    }
    let state = lazy_state(&dir)
        .await?
        .ok_or_else(|| anyhow!("not a lazy workspace: {}", dir.display()))?;
    if let Ok(pid) = tokio::fs::read_to_string(state.join("ready")).await
        && tokio::fs::try_exists(format!("/proc/{}", pid.trim())).await?
    {
        bail!("lazy workspace {} is already mounted (daemon pid {})", dir.display(), pid.trim());
    }
    new_lazy(ctx, &dir, DEFAULT_REF).await
}

pub async fn unmount(dir: &Path) -> Result<Outcome> {
    let state =
        lazy_state(dir).await?.ok_or_else(|| anyhow!("not a lazy workspace: {}", dir.display()))?;
    fusermount_unmount(std::path::absolute(dir)?, false, "unmount").await?;
    // Wait (bounded) for the daemon to exit and drop its `ready` marker.
    let ready = state.join("ready");
    poll_until(Duration::from_secs(5), || async { Ok(!tokio::fs::try_exists(&ready).await?) })
        .await?;
    Ok(Outcome::empty())
}

/// Object store for lazy `status` and for `ws update`: reads go through to the
/// server (base trees, upstream blobs), writes (new and merged blobs) are only
/// staged in the local cache. Submit remains the operation that uploads
/// changes to the server.
struct LazyObjects {
    read: Arc<ReadThrough>,
    local: Arc<DiskObjectStore>,
}
#[async_trait::async_trait]
impl ObjectStore for LazyObjects {
    async fn get_many(&self, ids: &[ObjectId]) -> ant_store::Result<Vec<Option<EncodedObject>>> {
        self.read.get_many(ids).await
    }
    async fn put_many(&self, objects: &[EncodedObject]) -> ant_store::Result<()> {
        self.local.put_many(objects).await
    }
    async fn has_many(&self, ids: &[ObjectId]) -> ant_store::Result<Vec<bool>> {
        self.read.has_many(ids).await
    }
}

#[cfg(test)]
mod restage_tests {
    use super::*;
    use ant_core::{ObjectKind, RepoPath};

    fn change(path: &str, content: &[u8]) -> LocalChange {
        let id = EncodedObject::new(ObjectKind::Blob, content).id();
        LocalChange {
            path: RepoPath::new(path).expect("path"),
            kind: LocalChangeKind::Modified,
            new: Some((ant_core::EntryMode::File, id)),
        }
    }

    #[tokio::test]
    async fn restage_reads_matching_file() {
        let dir = tempfile::tempdir().expect("tmp");
        std::fs::create_dir_all(dir.path().join("d")).expect("mkdir");
        std::fs::write(dir.path().join("d/f"), b"hello").expect("write");
        let c = change("d/f", b"hello");
        let blob = restage_blob(dir.path(), &c).await.expect("restage");
        assert_eq!(Some(blob.id()), c.new.map(|(_, id)| id));
    }

    #[tokio::test]
    async fn restage_refuses_changed_or_missing_file() {
        let dir = tempfile::tempdir().expect("tmp");
        let c = change("f", b"hello");
        std::fs::write(dir.path().join("f"), b"changed").expect("write");
        let err = restage_blob(dir.path(), &c).await.expect_err("changed");
        assert!(err.to_string().contains("changed after it was scanned"), "{err}");
        std::fs::remove_file(dir.path().join("f")).expect("rm");
        assert!(restage_blob(dir.path(), &c).await.is_err());
    }
}
