use std::fmt::Write as _;

use ant_proto::v1 as pb;
use anyhow::{Result, anyhow, bail};

use crate::ctx::{Ctx, Outcome, RpcResult, id_from};

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

pub async fn set(
    ctx: &Ctx,
    name: &str,
    rev: &str,
    expect: Option<String>,
    create: bool,
) -> Result<Outcome> {
    let mut client = ctx.remote().await?.client();
    let new = client
        .resolve_rev(pb::ResolveRevRequest { rev: rev.into() })
        .await
        .rpc()?
        .into_inner()
        .commit;
    // Without --expect/--create, compare-and-swap against the current value.
    let expected = if create {
        None
    } else if let Some(hex) = expect {
        Some(hex.parse::<ant_core::ObjectId>()?.as_bytes().to_vec())
    } else {
        client.get_ref(pb::GetRefRequest { name: name.into() }).await.rpc()?.into_inner().target
    };
    let r = client
        .update_ref(pb::UpdateRefRequest {
            name: name.into(),
            expected,
            new_target: Some(new.clone()),
        })
        .await
        .rpc()?
        .into_inner();
    if !r.updated {
        let actual = match r.actual {
            Some(a) => id_from(&a)?.to_string(),
            None => "(missing)".into(),
        };
        bail!("ref {name} changed concurrently; actual value is {actual}");
    }
    Ok(Outcome::ok(format!("{}\n", id_from(&new)?)))
}
