//! `ant admin ...`: maintenance straight against the stores.

use std::fmt::Write as _;

use ant_server::acl::{self, Acl};
use ant_server::ops::query;
use ant_store::StoreError;
use ant_store::postgres::{MIGRATIONS, PgStore};
use anyhow::{Result, anyhow};

use crate::args::{ReindexArgs, SchemaArgs};
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
    writeln!(
        out,
        "{commit} {} {} {}",
        stats.trees_walked, stats.trees_skipped, stats.tags_written
    )?;
    Ok(Outcome::ok(out))
}

/// `ant admin schema [--migrate] <store flags>`. stdout: `migrated <version>
/// <name>` per migration applied (with `--migrate`), then `version <n>`,
/// `supported <n>` and `pending <version> <name>` per migration still to run.
pub async fn schema(a: SchemaArgs) -> Result<Outcome> {
    let url = a.store.pg_url().ok_or_else(|| anyhow!("admin schema needs --pg-url"))?;
    let schema = a.store.pg_schema();
    let mut out = String::new();
    if a.migrate {
        for v in PgStore::migrate(url, schema).await? {
            let name = MIGRATIONS.iter().find(|m| m.version == v).map_or("?", |m| m.name);
            writeln!(out, "migrated {v} {name}")?;
        }
    }
    let status = PgStore::schema_status(url, schema).await?;
    if status.is_newer() {
        return Err(StoreError::NewerFormat(ant_core::NewerFormat {
            what: format!("Postgres schema \"{schema}\" version"),
            found: u64::from(status.version),
            supported: u64::from(status.supported),
        })
        .into());
    }
    writeln!(out, "version {}", status.version)?;
    writeln!(out, "supported {}", status.supported)?;
    for m in &status.pending {
        writeln!(out, "pending {} {}", m.version, m.name)?;
    }
    Ok(Outcome::ok(out))
}
