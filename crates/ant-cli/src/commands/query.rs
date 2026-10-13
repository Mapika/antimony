//! ls / cat / log / diff via RPCs (no checkout).

use std::fmt::Write as _;

use ant_core::EntryMode;
use ant_proto::v1 as pb;
use anyhow::{Result, anyhow, bail};

use crate::ctx::{Ctx, Outcome, RpcResult, id_from, kind_name, mode_from_pb};
use crate::rev::parse_rev_path;

pub async fn ls(ctx: &Ctx, target: &str) -> Result<Outcome> {
    let (rev, path) = parse_rev_path(target)?;
    let path = path.unwrap_or("");
    let mut client = ctx.remote().await?.client();
    let r = client
        .read_path(pb::ReadPathRequest { rev: rev.into(), path: path.into(), include_content: false })
        .await
        .rpc()?
        .into_inner();
    if !r.found {
        bail!("{target}: no such path");
    }
    let mut out = String::new();
    if mode_from_pb(r.mode)? == EntryMode::Tree {
        for e in r.entries {
            let mode = mode_from_pb(e.mode)?;
            writeln!(out, "{} {} {}\t{}", mode.as_octal(), kind_name(mode), id_from(&e.id)?, e.name)?;
        }
    } else {
        let mode = mode_from_pb(r.mode)?;
        let name = path.rsplit('/').next().unwrap_or(path);
        writeln!(out, "{} {} {}\t{}", mode.as_octal(), kind_name(mode), id_from(&r.id)?, name)?;
    }
    Ok(Outcome::ok(out))
}

pub async fn cat(ctx: &Ctx, target: &str) -> Result<Outcome> {
    let (rev, path) = parse_rev_path(target)?;
    let path = path.filter(|p| !p.is_empty()).ok_or_else(|| anyhow!("cat needs <rev>:<path>"))?;
    let mut client = ctx.remote().await?.client();
    let r = client
        .read_path(pb::ReadPathRequest { rev: rev.into(), path: path.into(), include_content: true })
        .await
        .rpc()?
        .into_inner();
    if !r.found {
        bail!("{target}: no such path");
    }
    if !mode_from_pb(r.mode)?.is_blob() {
        bail!("{target}: not a blob");
    }
    Ok(Outcome::ok(r.content))
}

pub async fn log(ctx: &Ctx, rev: &str, limit: Option<u32>, path: Option<String>) -> Result<Outcome> {
    let mut client = ctx.remote().await?.client();
    let path = path.map(|p| p.trim_matches('/').to_string());
    let r = client
        .log(pb::LogRequest { rev: rev.into(), path, limit: limit.unwrap_or(0) })
        .await
        .rpc()?
        .into_inner();
    let mut out = String::new();
    for c in r.commits {
        let subject = c.message.lines().next().unwrap_or("");
        writeln!(out, "{} {}", id_from(&c.id)?, subject)?;
    }
    Ok(Outcome::ok(out))
}

pub async fn diff(ctx: &Ctx, from: &str, to: &str) -> Result<Outcome> {
    let mut client = ctx.remote().await?.client();
    let r = client
        .diff(pb::DiffRequest { from_rev: from.into(), to_rev: to.into() })
        .await
        .rpc()?
        .into_inner();
    let mut out = String::new();
    for c in r.changes {
        let letter = match pb::ChangeKind::try_from(c.kind) {
            Ok(pb::ChangeKind::Added) => 'A',
            Ok(pb::ChangeKind::Modified) => 'M',
            Ok(pb::ChangeKind::Deleted) => 'D',
            _ => bail!("server sent unknown change kind {}", c.kind),
        };
        writeln!(out, "{letter}\t{}", c.path)?;
    }
    Ok(Outcome::ok(out))
}
