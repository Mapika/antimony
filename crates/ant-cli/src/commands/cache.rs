//! `ant cache stats`, `ant cache gc` and `ant cache seed`: the local object
//! cache (`seed` talks to the server to fill it).

use std::fmt::Write as _;
use std::sync::Arc;

use ant_core::{ObjectId, Tree};
use ant_store::ObjectStoreExt;
use antd::tiers::NAMES;
use antd::workspace::Sparse;
use anyhow::{Result, anyhow};
use futures::FutureExt;

use crate::ctx::{Ctx, Outcome, resolve_commit};
use crate::rev::parse_rev_path;

pub async fn stats(ctx: &Ctx, tiers: bool) -> Result<Outcome> {
    let s = ctx.disk()?.stats().await?;
    let mut out = String::new();
    writeln!(out, "objects {}\nbytes {}\nbudget {}", s.objects, s.bytes, s.budget)?;
    if tiers {
        for (name, value) in NAMES.iter().zip(antd::tiers::read(&ctx.cache)) {
            writeln!(out, "{name} {value}")?;
        }
    }
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

/// Walk `<rev>[:<path>]`'s tree as a checkout would (so the same tree
/// bundles are fetched), then fetch every blob in scope that is missing.
pub async fn seed(ctx: &Ctx, target: &str, patterns: Vec<String>) -> Result<Outcome> {
    let (rev, path) = parse_rev_path(target)?;
    let sparse = if patterns.is_empty() { Sparse::all() } else { Sparse::new(patterns)? };
    let (rt, remote) = ctx.read_through().await?;
    let commit = resolve_commit(&mut remote.client(), rev).await?;
    let mut tree = rt.read_commit(&commit).await?.tree;
    for name in path.unwrap_or("").split('/').filter(|n| !n.is_empty()) {
        let obj = rt.get_required(&tree).await?;
        let entries = Tree::decode(obj.body())?;
        let entry = entries
            .entries()
            .iter()
            .find(|e| e.name == name && e.mode.is_tree())
            .ok_or_else(|| anyhow!("{target}: {name} is not a directory"))?;
        tree = entry.id;
    }
    let before = rt.tier_counts();
    let ensure = {
        let rt = Arc::clone(&rt);
        move |ids: Vec<ObjectId>| {
            let rt = Arc::clone(&rt);
            async move { rt.ensure_local(&ids).await.map(|_| ()) }.boxed()
        }
    };
    let seeded = antd::workspace::seed(rt.as_ref(), Some(rt.as_ref()), tree, &sparse, &ensure)
        .await
        .map_err(|e| anyhow!("seeding {target}: {e:#}"))?;
    let after = rt.tier_counts();
    let delta = |name: &str| {
        let i = NAMES.iter().position(|n| *n == name).unwrap_or(0);
        after[i] - before[i]
    };
    let mut out = String::new();
    writeln!(
        out,
        "tree {tree}\nfiles {}\nblobs {}\nbundled {}\nfetched {}",
        seeded.leaves,
        seeded.blobs,
        delta("bundled_objects"),
        delta("fetched_objects")
    )?;
    Ok(Outcome::ok(out))
}
