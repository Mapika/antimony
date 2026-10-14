//! `ant cache stats` and `ant cache gc`: the local object cache, no server.

use std::fmt::Write as _;

use anyhow::Result;

use crate::ctx::{Ctx, Outcome};

pub async fn stats(ctx: &Ctx) -> Result<Outcome> {
    let s = ctx.disk()?.stats().await?;
    let mut out = String::new();
    writeln!(out, "objects {}\nbytes {}\nbudget {}", s.objects, s.bytes, s.budget)?;
    Ok(Outcome::ok(out))
}

pub async fn gc(ctx: &Ctx, to: Option<u64>) -> Result<Outcome> {
    let disk = ctx.disk()?;
    let r = match to {
        Some(target) => disk.evict_to(target).await?,
        None => disk.evict_over_budget().await?,
    };
    let mut out = String::new();
    writeln!(
        out,
        "removed {}\nfreed {}\nobjects {}\nbytes {}",
        r.removed, r.freed, r.objects, r.bytes
    )?;
    Ok(Outcome::ok(out))
}
