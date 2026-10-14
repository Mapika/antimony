//! Workspace commands: ws new / status / submit / land.

use std::fmt::Write as _;
use std::path::Path;

use ant_core::{EncodedObject, ObjectId};
use ant_proto::v1 as pb;
use ant_store::{ObjectStore, ObjectStoreExt};
use antd::workspace::{LocalChange, LocalChangeKind, Sparse, Workspace};
use anyhow::{Context as _, Result, anyhow, bail};

use crate::author::current_author;
use crate::ctx::{Ctx, Outcome, RpcResult, id_from};
use crate::discover::require_workspace;

const LAST_SUBMIT: &str = ".ant/last_submit";

pub async fn new(ctx: &Ctx, dir: &Path, at: &str, patterns: Vec<String>) -> Result<Outcome> {
    let sparse = if patterns.is_empty() { Sparse::all() } else { Sparse::new(patterns)? };
    let (rt, remote) = ctx.read_through().await?;
    let commit = remote
        .client()
        .resolve_rev(pb::ResolveRevRequest { rev: at.into() })
        .await
        .rpc()?
        .into_inner()
        .commit;
    let commit = id_from(&commit)?;
    let tree = rt.read_commit(&commit).await?.tree;
    Workspace::materialize(rt.as_ref(), dir, commit, tree, sparse).await?;
    Ok(Outcome::ok(format!("{commit}\n")))
}

fn letter(k: LocalChangeKind) -> char {
    match k {
        LocalChangeKind::Added => 'A',
        LocalChangeKind::Modified => 'M',
        LocalChangeKind::Deleted => 'D',
    }
}

async fn local_changes(ctx: &Ctx) -> Result<(std::path::PathBuf, Workspace, Vec<LocalChange>)> {
    let root = require_workspace(ctx.directory.as_deref())?;
    let ws = Workspace::open(&root)?;
    let disk = ctx.disk()?;
    let changes = if let Some(state) = lazy_state(&root).await? {
        let (read, _) = ctx.read_through().await?;
        let objects = std::sync::Arc::new(LazyObjects { read, local: disk });
        ant_fuse::rw::changes(objects, &state).await?
    } else {
        ws.status(disk.as_ref()).await?
    };
    Ok((root, ws, changes))
}

pub async fn status(ctx: &Ctx) -> Result<Outcome> {
    let (_, _, changes) = local_changes(ctx).await?;
    let mut out = String::new();
    for c in changes {
        writeln!(out, "{}\t{}", letter(c.kind), c.path)?;
    }
    Ok(Outcome::ok(out))
}

pub async fn submit(ctx: &Ctx, message: &str) -> Result<Outcome> {
    let (root, ws, changes) = local_changes(ctx).await?;
    if changes.is_empty() {
        bail!("nothing to submit");
    }
    let disk = ctx.disk()?;

    // Upload the changed blobs (they were stored locally by status()).
    let ids: Vec<ObjectId> = changes.iter().filter_map(|c| c.new.map(|(_, id)| id)).collect();
    let mut blobs: Vec<EncodedObject> = Vec::with_capacity(ids.len());
    for (id, obj) in ids.iter().zip(disk.get_many(&ids).await?) {
        blobs.push(obj.ok_or_else(|| anyhow!("local blob {id} missing from cache"))?);
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
    let a = tokio::task::spawn_blocking(current_author).await?;
    let author = pb::Signature::from(&a);
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
    let commit = client
        .resolve_rev(pb::ResolveRevRequest { rev: commit_hex })
        .await
        .rpc()?
        .into_inner()
        .commit;
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
            eprintln!(
                "hint: {ref_name} moved under you; start a fresh workspace at the new tip \
                 (`ant ws new <dir> --at {ref_name}`), redo the edit there, then submit and land again"
            );
            Ok(Outcome { stdout: out.into_bytes(), code: 2 })
        }
        None => bail!("server returned an empty land response"),
    }
}

async fn lazy_state(root: &Path) -> Result<Option<std::path::PathBuf>> {
    match tokio::fs::read_to_string(root.join(".ant/lazy")).await {
        Ok(path) => Ok(Some(path.into())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
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
    let commit = id_from(
        &remote
            .client()
            .resolve_rev(pb::ResolveRevRequest { rev: at.into() })
            .await
            .rpc()?
            .into_inner()
            .commit,
    )?;
    let tree = objects.read_commit(&commit).await?.tree;
    let cache = std::path::absolute(&ctx.cache)?;
    let d = dir.clone();
    let state = tokio::task::spawn_blocking(move || -> Result<std::path::PathBuf> {
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
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
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
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

pub async fn serve(ctx: &Ctx, dir: &Path, state: &Path) -> Result<Outcome> {
    let (objects, _) = ctx.read_through().await?;
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

pub async fn unmount(dir: &Path) -> Result<Outcome> {
    let state =
        lazy_state(dir).await?.ok_or_else(|| anyhow!("not a lazy workspace: {}", dir.display()))?;
    let dir = std::path::absolute(dir)?;
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("fusermount3").arg("-u").arg(&dir).output()
    })
    .await??;
    if !output.status.success() {
        bail!("unmount failed: {}", String::from_utf8_lossy(&output.stderr).trim());
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while tokio::fs::try_exists(state.join("ready")).await?
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    Ok(Outcome::empty())
}

// Lazy status may fetch base trees but only stages new blobs locally. Submit
// remains the operation that uploads changes to the server.
struct LazyObjects {
    read: std::sync::Arc<antd::readthrough::ReadThrough>,
    local: std::sync::Arc<antd::disk::DiskObjectStore>,
}
type StoreFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = ant_store::Result<T>> + Send + 'a>>;
impl ObjectStore for LazyObjects {
    fn get_many<'a, 'b, 'f>(
        &'a self,
        ids: &'b [ObjectId],
    ) -> StoreFuture<'f, Vec<Option<EncodedObject>>>
    where
        'a: 'f,
        'b: 'f,
        Self: 'f,
    {
        Box::pin(async move { self.read.get_many(ids).await })
    }
    fn put_many<'a, 'b, 'f>(&'a self, objects: &'b [EncodedObject]) -> StoreFuture<'f, ()>
    where
        'a: 'f,
        'b: 'f,
        Self: 'f,
    {
        Box::pin(async move { self.local.put_many(objects).await })
    }
    fn has_many<'a, 'b, 'f>(&'a self, ids: &'b [ObjectId]) -> StoreFuture<'f, Vec<bool>>
    where
        'a: 'f,
        'b: 'f,
        Self: 'f,
    {
        Box::pin(async move { self.read.has_many(ids).await })
    }
}
