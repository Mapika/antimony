use std::path::Path;
use std::sync::Arc;

use ant_core::ObjectId;
use ant_proto::v1 as pb;
use ant_store::{ObjectStore, ObjectStoreExt};
use anyhow::{Context as _, Result};

use crate::ctx::{Ctx, Outcome, RpcResult, id_from};

pub async fn run(ctx: &Ctx, rev: &str, mountpoint: &Path) -> Result<Outcome> {
    let (rt, remote) = ctx.read_through().await?;
    let commit = remote
        .client()
        .resolve_rev(pb::ResolveRevRequest { rev: rev.into() })
        .await
        .rpc()?
        .into_inner()
        .commit;
    let commit: ObjectId = id_from(&commit)?;
    let tree = rt.read_commit(&commit).await?.tree;

    let mp = mountpoint.to_path_buf();
    let handle = tokio::runtime::Handle::current();
    let store: Arc<dyn ObjectStore> = rt;
    let session = tokio::task::spawn_blocking(move || {
        ant_fuse::mount(store, tree, &mp, handle, ant_fuse::MountOptions::default())
    })
    .await?
    .with_context(|| format!("mounting at {}", mountpoint.display()))?;
    eprintln!("mounted {commit} at {}", mountpoint.display());

    wait_for_shutdown().await?;
    tokio::task::spawn_blocking(move || drop(session)).await?;
    Ok(Outcome::empty())
}

async fn wait_for_shutdown() -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate())?;
    tokio::select! {
        r = tokio::signal::ctrl_c() => r?,
        _ = term.recv() => {}
    }
    Ok(())
}
