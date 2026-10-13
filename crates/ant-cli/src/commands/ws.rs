//! Workspace commands: ws new / status / submit / land.

use std::fmt::Write as _;
use std::path::Path;

use ant_core::{EncodedObject, ObjectId};
use ant_proto::v1 as pb;
use ant_store::{ObjectStore, ObjectStoreExt};
use antd::workspace::{LocalChange, LocalChangeKind, Sparse, Workspace};
use anyhow::{Context as _, Result, anyhow, bail};

use crate::author::current_author;
use crate::ctx::{Ctx, Outcome, RpcResult, id_from, mode_to_pb};
use crate::discover::require_workspace;

const LAST_SUBMIT: &str = ".ant/last_submit";

pub async fn new(ctx: &Ctx, dir: &Path, at: &str, patterns: Vec<String>) -> Result<Outcome> {
    let sparse = if patterns.is_empty() { Sparse::all() } else { Sparse::new(patterns)? };
    let (rt, remote) = ctx.read_through().await?;
    let commit = remote.client().resolve_rev(pb::ResolveRevRequest { rev: at.into() }).await.rpc()?.into_inner().commit;
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
    let changes = ws.status(disk.as_ref()).await?;
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
                    mode: mode_to_pb(mode) as i32,
                    content: Some(pb::write_op::Content::BlobId(id.as_bytes().to_vec())),
                }),
                None => pb::file_op::Op::Delete(pb::DeleteOp {}),
            }),
        })
        .collect();
    let a = tokio::task::spawn_blocking(current_author).await?;
    let author = pb::Signature { name: a.name, email: a.email, time: a.time, tz_offset_minutes: a.tz_offset_minutes };
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
                .map_err(|_| anyhow!("no commit given and {} not found; run `ant submit` first", p.display()))?
                .trim()
                .to_string()
        }
    };
    let (rt, remote) = ctx.read_through().await?;
    let mut client = remote.client();
    let commit = client.resolve_rev(pb::ResolveRevRequest { rev: commit_hex }).await.rpc()?.into_inner().commit;
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
                ws.reset_to(rt.as_ref(), tip, tree).await.context("resetting workspace to new tip")?;
            }
            Ok(Outcome::ok(format!("landed {tip}{}\n", if l.rebased { " rebased" } else { "" })))
        }
        Some(pb::land_response::Outcome::Conflict(c)) => {
            let mut out = String::from("conflict\n");
            for p in c.paths {
                writeln!(out, "{p}")?;
            }
            Ok(Outcome { stdout: out.into_bytes(), code: 2 })
        }
        None => bail!("server returned an empty land response"),
    }
}
