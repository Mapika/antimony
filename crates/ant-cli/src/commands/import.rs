//! `ant import`: git history straight into the stores (no server).

use std::fmt::Write as _;

use ant_gitbridge::ImportOptions;
use ant_server::acl::{self, Acl};
use anyhow::{Result, anyhow};

use crate::args::ImportArgs;
use crate::ctx::Outcome;

pub async fn run(a: ImportArgs) -> Result<Outcome> {
    let backend = a.store.backend().await?;
    let opts = ImportOptions::single(&a.git_ref, &a.ref_name);
    let stats = ant_gitbridge::import(&a.git_dir, &backend, &opts).await?;
    eprintln!("imported {} commits, {} trees, {} blobs", stats.commits, stats.trees, stats.blobs);
    let tip = backend
        .refs
        .get_ref(&a.ref_name)
        .await?
        .ok_or_else(|| anyhow!("import did not create ref {}", a.ref_name))?;
    // The ref's path-history index (`log --path`, blame), as a server
    // builds it after a land: otherwise the first queries scan the history.
    let indexed = ant_server::ops::history::catch_up(&backend, &a.ref_name).await?;
    eprintln!("history index: {indexed} commits");
    if let Some(policy) = &a.policy {
        let acl = Acl::load(policy)?;
        if let Some(policy) = acl.policy() {
            let stats = acl::reindex_history(&backend, policy, tip, false).await?;
            eprintln!(
                "acl: walked {} trees ({} skipped), wrote {} zone tags",
                stats.trees_walked, stats.trees_skipped, stats.tags_written
            );
        }
    }
    let mut out = String::new();
    writeln!(out, "{} {}", a.ref_name, tip)?;
    Ok(Outcome::ok(out))
}
