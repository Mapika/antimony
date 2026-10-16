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

use crate::args::{CopyLocalArgs, GcArgs, ReindexArgs, RepackArgs, SchemaArgs};
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
    if let Some(dir) = a.store.local_dir()? {
        return local_schema(&a, &dir).await;
    }
    let url = a.store.pg_url().ok_or_else(|| anyhow!("admin schema needs --pg-url or --local"))?;
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

/// `ant admin schema --local [<dir>]`: the local store's SQLite schema.
async fn local_schema(a: &SchemaArgs, dir: &std::path::Path) -> Result<Outcome> {
    use ant_store::local::{LocalStore, MIGRATIONS as LOCAL_MIGRATIONS};
    let mut out = String::new();
    if a.migrate {
        let before = LocalStore::schema_status(dir)?.version;
        let options = ant_store::local::LocalOptions { migrate: true, ..a.store.local_options() };
        let d = dir.to_owned();
        tokio::task::spawn_blocking(move || LocalStore::open(&d, &options))
            .await
            .context("opening the local data directory")??;
        for m in LOCAL_MIGRATIONS.iter().filter(|m| m.version > before) {
            writeln!(out, "migrated {} {}", m.version, m.name)?;
        }
    }
    let status = LocalStore::schema_status(dir)?;
    if status.version > status.supported {
        return Err(StoreError::NewerFormat(ant_core::NewerFormat {
            what: "local store schema version".into(),
            found: u64::from(status.version),
            supported: u64::from(status.supported),
        })
        .into());
    }
    writeln!(out, "version {}", status.version)?;
    writeln!(out, "supported {}", status.supported)?;
    for m in LOCAL_MIGRATIONS.iter().filter(|m| m.version > status.version) {
        writeln!(out, "pending {} {}", m.version, m.name)?;
    }
    Ok(Outcome::ok(out))
}

/// `YYYY-MM-DD HH:MM:SS+00` of Unix seconds (UTC).
fn utc_text(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}+00",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// `ant admin gc --local`: an offline run (the directory's lock keeps a
/// server out while it runs).
async fn local_gc(a: &GcArgs, store: ant_store::local::LocalStore) -> Result<Outcome> {
    use ant_store::local::gc::{GcOptions as LocalGcOptions, collect};
    if a.abort {
        return Err(anyhow!("--abort: local collection runs have nothing to abort"));
    }
    let opts = LocalGcOptions { grace: a.grace, dry_run: a.dry_run, ..LocalGcOptions::default() };
    let r = collect(&store, &opts, &|line| eprintln!("gc: {line}")).await?;
    let mut out = String::new();
    writeln!(out, "{}", if r.dry_run { "dry-run" } else { "done" })?;
    writeln!(out, "cutoff {}", utc_text(r.cutoff))?;
    writeln!(out, "live {}", r.live)?;
    writeln!(out, "condemned {}", r.objects + r.large_objects + r.commits)?;
    writeln!(out, "objects {} {}", r.objects, r.object_bytes)?;
    writeln!(out, "large-objects {} {}", r.large_objects, r.large_object_bytes)?;
    writeln!(out, "commits {}", r.commits)?;
    writeln!(out, "changesets {} {}", r.changesets, r.changeset_bytes)?;
    if !r.dry_run {
        writeln!(out, "legacy-changes 0")?;
        writeln!(out, "derived {}", r.derived)?;
        writeln!(out, "kept 0")?;
    }
    writeln!(out, "packs {} {}", r.packs, r.pack_bytes)?;
    let ratio = if r.packs_kept_bytes == 0 {
        0.0
    } else {
        r.packs_dead_bytes as f64 / r.packs_kept_bytes as f64
    };
    writeln!(
        out,
        "packs-kept {} {} {} {:.1}%",
        r.packs_kept,
        r.packs_kept_bytes,
        r.packs_dead_bytes,
        100.0 * ratio
    )?;
    writeln!(out, "packs-compacted {} {} {}", r.packs_compacted, r.compacted_from, r.compacted_to)?;
    if !r.dry_run {
        writeln!(out, "vacuumed {}", if r.vacuumed { "yes" } else { "no" })?;
    }
    if r.missing > 0 {
        eprintln!("gc: warning: {} reachable objects are missing from the store", r.missing);
    }
    Ok(Outcome::ok(out))
}

/// `ant admin copy-local [--from <dir>] <store flags>`: stdout `objects <n>`,
/// `commits <n>`, `derived <n>`, `refs <n>`, then `skipped <ref> <id>` per
/// ref the destination already had. Progress on stderr (`copy: ...`).
pub async fn copy_local(a: CopyLocalArgs) -> Result<Outcome> {
    use ant_store::local::{LocalOptions, LocalStore, copy::copy_to};
    let from = match a.from {
        Some(d) => d,
        None => ant_store::local::default_dir()
            .ok_or_else(|| anyhow!("no --from given and $HOME is not set"))?,
    };
    if a.store.local_dir()?.as_deref() == Some(from.as_path()) {
        return Err(anyhow!("--from and the destination are the same directory"));
    }
    let dst = a.store.backend().await?;
    let options = LocalOptions { migrate: false, ..LocalOptions::default() };
    let src = tokio::task::spawn_blocking(move || LocalStore::open(&from, &options))
        .await
        .context("opening the local data directory")??;
    let r = copy_to(&src, &dst, &|line| eprintln!("copy: {line}")).await?;
    let mut out = String::new();
    writeln!(out, "objects {}", r.objects)?;
    writeln!(out, "commits {}", r.commits)?;
    writeln!(out, "derived {}", r.derived)?;
    writeln!(out, "refs {}", r.refs)?;
    for (name, id) in &r.refs_skipped {
        writeln!(out, "skipped {name} {id}")?;
    }
    Ok(Outcome::ok(out))
}

/// `ant admin gc [--dry-run] [--grace <dur>] [--quiesce <dur>] [--max-batches <n>]
/// [--abort] <store flags>`. Progress goes to stderr (`gc: ...`); stdout is
/// the report (see the output contract in `main.rs`).
pub async fn gc(a: GcArgs) -> Result<Outcome> {
    let (backend, probes) = a.store.backend_with_probes().await?;
    if let Some(local) = probes.local {
        drop(backend);
        return local_gc(&a, local).await;
    }
    let pg = probes
        .pg
        .ok_or_else(|| anyhow!("admin gc needs a Postgres store (--pg-url) or --local"))?;
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
    if probes.local.is_some() {
        return Err(anyhow!(
            "admin repack needs Postgres + S3; local stores are compacted by `ant admin gc --local`"
        ));
    }
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

#[cfg(test)]
mod tests {
    #[test]
    fn utc_text_formats_unix_seconds() {
        assert_eq!(super::utc_text(0), "1970-01-01 00:00:00+00");
        assert_eq!(super::utc_text(951_782_400), "2000-02-29 00:00:00+00");
        assert_eq!(super::utc_text(1_792_108_799), "2026-10-15 23:59:59+00");
    }
}
