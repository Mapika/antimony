//! `ant mount`: read-only FUSE view of a commit, in the foreground.

use std::path::Path;
use std::sync::Arc;

use ant_store::{ObjectStore, ObjectStoreExt};
use anyhow::{Context as _, Result};

use crate::ctx::{Ctx, Outcome, resolve_commit};

pub async fn run(ctx: &Ctx, rev: &str, mountpoint: &Path) -> Result<Outcome> {
    let (rt, remote) = ctx.read_through().await?;
    let commit = resolve_commit(&mut remote.client(), rev).await?;
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
