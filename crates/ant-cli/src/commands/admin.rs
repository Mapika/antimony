//! `ant admin ...`: maintenance straight against the stores.

use std::fmt::Write as _;

use ant_server::acl::{self, Acl};
use ant_server::ops::query;
use ant_store::StoreError;
use ant_store::packed::ClusterOptions;
use ant_store::postgres::gc::GcOptions;
use ant_store::postgres::repack::RepackOptions;
use ant_store::postgres::{MIGRATIONS, PgStore};
use ant_store::s3::S3ObjectStore;
use anyhow::{Context, Result, anyhow};

use crate::args::{GcArgs, ReindexArgs, RepackArgs, SchemaArgs};
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

/// `ant admin gc [--dry-run] [--grace <dur>] [--quiesce <dur>] [--max-batches <n>]
/// [--abort] <store flags>`. Progress goes to stderr (`gc: ...`); stdout is
/// the report (see the output contract in `main.rs`).
pub async fn gc(a: GcArgs) -> Result<Outcome> {
    let (backend, probes) = a.store.backend_with_probes().await?;
    let pg = probes.pg.ok_or_else(|| anyhow!("admin gc needs a Postgres store (--pg-url)"))?;
    if a.abort {
        let pending = pg.gc_abort().await?;
        let out = if pending { "aborted\n" } else { "nothing to abort\n" };
        return Ok(Outcome::ok(out.to_string()));
    }
    let s3 = match a.store.s3_config()? {
        Some(cfg) => Some(S3ObjectStore::connect(&cfg).await.context("configuring S3")?),
        None => None,
    };
    let opts = GcOptions {
        grace: a.grace,
        quiesce: a.quiesce,
        dry_run: a.dry_run,
        s3,
        stop_after_batches: a.max_batches,
        ..GcOptions::default()
    };
    let r = pg.gc(backend.objects.as_ref(), &opts, &|line| eprintln!("gc: {line}")).await?;
    let state = match (r.dry_run, r.finished) {
        (true, _) => "dry-run",
        (false, true) => "done",
        (false, false) => "stopped",
    };
    let mut out = String::new();
    writeln!(out, "{state}{}", if r.resumed { " resumed" } else { "" })?;
    writeln!(out, "cutoff {}", r.cutoff)?;
    writeln!(out, "live {}", r.live)?;
    writeln!(out, "condemned {}", r.condemned)?;
    writeln!(out, "objects {} {}", r.objects, r.object_bytes)?;
    writeln!(out, "large-objects {} {}", r.large_objects, r.large_object_bytes)?;
    writeln!(out, "commits {}", r.commits)?;
    writeln!(out, "changesets {} {}", r.changesets, r.changeset_bytes)?;
    if !r.dry_run {
        writeln!(out, "legacy-changes {}", r.legacy_changes)?;
        writeln!(out, "derived {}", r.derived)?;
        writeln!(out, "kept {}", r.kept)?;
    }
    if opts.s3.is_some() {
        writeln!(out, "packs {} {}", r.packs, r.pack_bytes)?;
        writeln!(
            out,
            "packs-kept {} {} {} {:.1}%",
            r.packs_kept,
            r.packs_kept_bytes,
            r.packs_dead_bytes,
            100.0 * r.pack_dead_ratio()
        )?;
    }
    if r.missing > 0 {
        eprintln!("gc: warning: {} reachable objects are missing from the store", r.missing);
    }
    Ok(Outcome::ok(out))
}

/// `ant admin repack [--dry-run] [--min-age <dur>] [--keep-recent <n>] [--all]
/// [--no-small] [--window <n>] [--depth <n>] [--record-bytes <n>] [--level <n>]
/// [--jobs <n>] [--max-packs <n>] <store flags>`. Progress goes to stderr
/// (`repack: ...`); stdout is the report (see the output contract in
/// `main.rs`).
pub async fn repack(a: RepackArgs) -> Result<Outcome> {
    let (backend, probes) = a.store.backend_with_probes().await?;
    let pg = probes.pg.ok_or_else(|| anyhow!("admin repack needs a Postgres store (--pg-url)"))?;
    let cfg = a
        .store
        .s3_config()?
        .ok_or_else(|| anyhow!("admin repack needs an S3 store (--s3-endpoint ...)"))?;
    let s3 = S3ObjectStore::connect(&cfg).await.context("configuring S3")?;
    let mut opts = RepackOptions::new(s3);
    opts.dry_run = a.dry_run;
    opts.min_age = a.min_age;
    opts.keep_recent = a.keep_recent;
    opts.all = a.all;
    opts.small = !a.no_small;
    opts.cluster = ClusterOptions {
        window: a.window,
        depth: a.depth,
        cluster_raw: a.record_bytes,
        level: a.level,
    };
    if let Some(j) = a.jobs {
        opts.jobs = j.max(1);
    }
    opts.stop_after_packs = a.max_packs;
    let r = pg.repack(backend.objects.as_ref(), &opts, &|line| eprintln!("repack: {line}")).await?;
    let state = match (r.dry_run, r.finished) {
        (true, _) => "dry-run",
        (false, true) => "done",
        (false, false) => "stopped",
    };
    let mut out = String::new();
    writeln!(out, "{state}")?;
    writeln!(out, "commits {}", r.commits)?;
    writeln!(out, "reachable {}", r.reachable)?;
    writeln!(out, "hot {}", r.hot)?;
    writeln!(
        out,
        "selected {} {} {} {}",
        r.selected_cold, r.selected_hot, r.selected_other, r.selected_small
    )?;
    writeln!(out, "old-bytes {} {}", r.old_s3_bytes, r.old_pg_bytes)?;
    writeln!(out, "new-bytes {}", r.new_bytes)?;
    writeln!(out, "saved {}", r.saved_bytes())?;
    writeln!(out, "packs {} {}", r.packs, r.records)?;
    writeln!(out, "deltas {} {}", r.deltas, r.max_depth)?;
    writeln!(out, "moved {} {}", r.moved, r.moved_small)?;
    writeln!(out, "skipped {}", r.skipped)?;
    Ok(Outcome::ok(out))
}
