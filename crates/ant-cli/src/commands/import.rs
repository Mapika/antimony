use std::fmt::Write as _;

use ant_gitbridge::ImportOptions;
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
    let mut out = String::new();
    writeln!(out, "{} {}", a.ref_name, tip)?;
    Ok(Outcome::ok(out))
}
