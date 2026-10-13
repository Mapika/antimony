//! `ant admin ...`: maintenance straight against the stores.

use std::fmt::Write as _;

use ant_server::acl::{self, Acl};
use ant_server::ops::query;
use anyhow::{Result, anyhow};

use crate::args::ReindexArgs;
use crate::ctx::Outcome;

/// `ant admin reindex-acl <rev> --policy <file> [--full] [--history] <store flags>`.
/// stdout: `<commit-id> <trees walked> <trees skipped> <tags written>`.
pub async fn reindex_acl(a: ReindexArgs) -> Result<Outcome> {
    let backend = a.store.backend().await?;
    let acl = Acl::load(&a.policy)?;
    let policy = acl.policy().ok_or_else(|| anyhow!("no policy loaded"))?;
    let commit = query::resolve_rev(&backend, &a.rev).await?;
    let stats = if a.history {
        acl::reindex_history(&backend, policy, commit, a.full).await?
    } else {
        acl::reindex_with(&backend, policy, commit, a.full).await?
    };
    let mut out = String::new();
    writeln!(out, "{commit} {} {} {}", stats.trees_walked, stats.trees_skipped, stats.tags_written)?;
    Ok(Outcome::ok(out))
}
