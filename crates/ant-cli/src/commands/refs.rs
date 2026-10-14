//! `ant ref get / list / init / set`.

use std::collections::HashSet;
use std::fmt::Write as _;

use ant_core::ObjectId;
use ant_proto::v1 as pb;
use antd::remote::AntRpcClient as Client;
use anyhow::{Result, anyhow, bail};

use crate::ctx::{Ctx, Outcome, RpcResult, id_from, resolve_commit};

/// First-parent commits fetched per `Log` call while checking ancestry.
const LOG_PAGE: u32 = 1000;

/// Is `ancestor` reachable from `descendant`? Walks first-parent chains with
/// `Log` pages (each commit carries its generation), following other parents
/// as new chains, and stops every chain at the ancestor's generation, so the
/// cost follows the distance between the two commits.
async fn is_ancestor(
    client: &mut Client,
    ancestor: ObjectId,
    descendant: ObjectId,
) -> Result<bool> {
    if ancestor == descendant {
        return Ok(true);
    }
    let Some(generation) = log_page(client, ancestor, 1).await?.first().map(|c| c.generation)
    else {
        return Ok(false);
    };
    let mut seen = HashSet::from([descendant]);
    let mut chains = vec![descendant];
    while let Some(start) = chains.pop() {
        let commits = log_page(client, start, LOG_PAGE).await?;
        let mut resume = None;
        for c in &commits {
            if id_from(&c.id)? == ancestor {
                return Ok(true);
            }
            resume = None;
            if c.generation <= generation {
                break;
            }
            for parent in c.parents.iter().skip(1) {
                let parent = id_from(parent)?;
                if seen.insert(parent) {
                    chains.push(parent);
                }
            }
            match c.parents.first().map(|p| id_from(p)).transpose()? {
                // The next commit of this page, unless another chain has it.
                Some(parent) if seen.insert(parent) => resume = Some(parent),
                _ => break,
            }
        }
        if commits.len() == LOG_PAGE as usize
            && let Some(parent) = resume
        {
            chains.push(parent);
        }
    }
    Ok(false)
}

async fn log_page(client: &mut Client, rev: ObjectId, limit: u32) -> Result<Vec<pb::CommitInfo>> {
    let r = client.log(pb::LogRequest { rev: rev.to_hex(), path: None, limit }).await.rpc()?;
    Ok(r.into_inner().commits)
}

pub async fn get(ctx: &Ctx, name: &str) -> Result<Outcome> {
    let mut client = ctx.remote().await?.client();
    let r = client.get_ref(pb::GetRefRequest { name: name.into() }).await.rpc()?.into_inner();
    let target = r.target.ok_or_else(|| anyhow!("ref {name} not found"))?;
    Ok(Outcome::ok(format!("{}\n", id_from(&target)?)))
}

pub async fn list(ctx: &Ctx, prefix: &str) -> Result<Outcome> {
    let mut client = ctx.remote().await?.client();
    let r =
        client.list_refs(pb::ListRefsRequest { prefix: prefix.into() }).await.rpc()?.into_inner();
    let mut out = String::new();
    for e in r.refs {
        writeln!(out, "{} {}", id_from(&e.target)?, e.name)?;
    }
    Ok(Outcome::ok(out))
}

/// Create `name` at a root commit with the empty tree (what an empty server
/// needs before the first workspace can be created).
pub async fn init(ctx: &Ctx, name: &str, message: &str) -> Result<Outcome> {
    let mut client = ctx.remote().await?.client();
    let current = client.get_ref(pb::GetRefRequest { name: name.into() }).await.rpc()?.into_inner();
    if let Some(target) = current.target {
        bail!("ref {name} already exists (at {})", id_from(&target)?);
    }
    let author = crate::author::author_signature().await?;
    let commit = client
        .create_changeset(pb::CreateChangesetRequest {
            base: None,
            ops: Vec::new(),
            author: Some(author),
            committer: None,
            message: message.to_string(),
        })
        .await
        .rpc()?
        .into_inner()
        .commit;
    let r =
        client.land(pb::LandRequest { ref_name: name.into(), commit }).await.rpc()?.into_inner();
    match r.outcome {
        Some(pb::land_response::Outcome::Landed(l)) => {
            Ok(Outcome::ok(format!("{}\n", id_from(&l.new_tip)?)))
        }
        // Someone created the ref between the check and the land.
        Some(pb::land_response::Outcome::Conflict(_)) => bail!("ref {name} already exists"),
        None => bail!("server returned an empty land response"),
    }
}

pub async fn set(
    ctx: &Ctx,
    name: &str,
    rev: &str,
    expect: Option<String>,
    create: bool,
    force: bool,
) -> Result<Outcome> {
    let mut client = ctx.remote().await?.client();
    let new = resolve_commit(&mut client, rev).await?;
    // Without --expect/--create, compare-and-swap against the current value.
    let expected = if create {
        None
    } else if let Some(hex) = expect {
        Some(hex.parse::<ObjectId>()?.as_bytes().to_vec())
    } else {
        client.get_ref(pb::GetRefRequest { name: name.into() }).await.rpc()?.into_inner().target
    };
    if let (Some(old), false) = (&expected, force) {
        let old = id_from(old)?;
        if !is_ancestor(&mut client, old, new).await? {
            bail!("non-fast-forward update of ref {name}: {new} does not descend from {old}");
        }
    }
    let r = client
        .update_ref(pb::UpdateRefRequest {
            name: name.into(),
            expected,
            new_target: Some(new.as_bytes().to_vec()),
        })
        .await
        .rpc()?
        .into_inner();
    if !r.updated {
        let exists = r.actual.is_some();
        let actual = match r.actual {
            Some(a) => id_from(&a)?.to_string(),
            None => "(missing)".into(),
        };
        if create && exists {
            bail!("ref {name} already exists (at {actual})");
        }
        bail!("ref {name} changed concurrently; actual value is {actual}");
    }
    Ok(Outcome::ok(format!("{new}\n")))
}
