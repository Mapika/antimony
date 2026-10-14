//! `import` subcommand: run `ant_gitbridge::import` with every store call timed.
//!
//! The import pipeline (see `ant_gitbridge`) reads and converts history on
//! blocking threads and feeds an async writer that stores objects
//! (`has_many`, `put_many`) and records commits (`put_commits`,
//! `put_derived_many`). The writer-side store time versus wall time says
//! whether the store or the conversion is the bottleneck; process CPU time
//! tells CPU-bound conversion from waiting on store I/O.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ant_gitbridge::{ImportOptions, ImportStats};
use ant_server::config::StoreArgs;
use anyhow::Context as _;

use crate::json::J;
use crate::util::{
    Counter, Metrics, cpu_seconds, cpu_total, instrument, mib, peak_rss_bytes, write_json,
};

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Path to the git directory (bare repo or work tree).
    pub git_dir: PathBuf,
    #[arg(long, default_value = "refs/heads/master")]
    pub git_ref: String,
    /// Antimony ref to point at the imported tip.
    #[arg(long, default_value = "main")]
    pub ant_ref: String,
    /// Objects per store batch (ImportOptions::batch_objects).
    #[arg(long, default_value_t = 1024)]
    pub batch_objects: usize,
    /// Stop (and report partial numbers) after this many seconds; 0 = no limit.
    #[arg(long, default_value_t = 0)]
    pub max_seconds: u64,
    #[arg(long)]
    pub json: Option<PathBuf>,
    #[command(flatten)]
    pub store: StoreArgs,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let backend = args.store.backend().await?;
    let metrics = Metrics::default();
    let timed = instrument(&backend, &metrics, "store");
    let put_objects = metrics.counter("store.put_many");
    let put_commit = metrics.counter("store.put_commit");
    let put_commits = metrics.counter("store.put_commits");
    // Commits recorded so far: single `put_commit` calls plus batched ones.
    let recorded = {
        let (a, b) = (put_commit.clone(), put_commits.clone());
        move || a.calls() + b.items()
    };

    let mut opts = ImportOptions::single(&args.git_ref, &args.ant_ref);
    opts.batch_objects = args.batch_objects;

    println!(
        "importing {} ({} -> {}) batch_objects={}",
        args.git_dir.display(),
        args.git_ref,
        args.ant_ref,
        args.batch_objects
    );
    let start = Instant::now();
    let cpu0 = cpu_seconds().unwrap_or((0.0, 0.0));

    let ticker = tokio::spawn(progress(start, put_objects.clone(), recorded.clone(), metrics.clone()));

    let import = ant_gitbridge::import(&args.git_dir, &timed, &opts);
    let (result, timed_out) = if args.max_seconds > 0 {
        match tokio::time::timeout(Duration::from_secs(args.max_seconds), import).await {
            Ok(r) => (Some(r), false),
            Err(_) => (None, true),
        }
    } else {
        (Some(import.await), false)
    };
    ticker.abort();
    let wall = start.elapsed().as_secs_f64();
    let cpu1 = cpu_seconds().unwrap_or((0.0, 0.0));

    let stats: ImportStats = match result {
        Some(Ok(s)) => s,
        Some(Err(e)) => return Err(e).context("import failed"),
        // Partial: derive from what the writer persisted.
        None => ImportStats { commits: recorded(), trees: 0, blobs: put_objects.items() },
    };
    let objects =
        if timed_out { put_objects.items() } else { stats.commits + stats.trees + stats.blobs };

    let pg = match args.store.pg_url() {
        Some(url) => Some(pg_size(url, args.store.pg_schema()).await?),
        None => None,
    };

    let rss = peak_rss_bytes();
    println!();
    println!(
        "== import summary{} ==",
        if timed_out { " (STOPPED at --max-seconds; partial)" } else { "" }
    );
    println!("wall            {wall:.2}s");
    println!(
        "cpu             user {:.1}s sys {:.1}s  (of {wall:.1}s wall x cores)",
        cpu1.0 - cpu0.0,
        cpu1.1 - cpu0.1
    );
    if timed_out {
        println!("objects written {objects} (commits {} recorded)", recorded());
    } else {
        println!("commits         {}", stats.commits);
        println!("trees           {}", stats.trees);
        println!("blobs           {}", stats.blobs);
    }
    println!(
        "commits/s       {:.1}",
        if timed_out { recorded() } else { stats.commits } as f64 / wall
    );
    println!("objects/s       {:.1}", objects as f64 / wall);
    println!(
        "object bytes    {:.1} MiB written ({:.2} MiB/s)",
        mib(put_objects.bytes()),
        mib(put_objects.bytes()) / wall
    );
    if let Some(r) = rss {
        println!("peak RSS        {:.1} MiB", mib(r));
    }
    if let Some((total, tables)) = &pg {
        println!("pg schema size  {:.1} MiB", mib(*total));
        for (name, bytes, rows) in tables {
            println!("    {name:<20} {:>10.1} MiB  ~{rows} rows", mib(*bytes as u64));
        }
    }
    metrics.print("store-call timing (sum over all callers; writer is sequential)");
    let writer_secs: f64 = [
        "has_many",
        "put_many",
        "get_many",
        "put_commit",
        "put_commits",
        "get_commits",
        "put_derived",
        "put_derived_many",
        "cas_ref",
        "get_ref",
    ]
    .iter()
    .map(|n| metrics.counter(&format!("store.{n}")).secs())
    .sum();
    println!(
        "  writer-side store time ~{writer_secs:.1}s = {:.0}% of wall (rest = writer waiting on the producer)",
        100.0 * writer_secs / wall
    );

    let json = J::obj()
        .with("command", "import")
        .with("git_dir", args.git_dir.display().to_string())
        .with("git_ref", args.git_ref.as_str())
        .with("batch_objects", args.batch_objects)
        .with("timed_out", timed_out)
        .with("wall_s", wall)
        .with("cpu_user_s", cpu1.0 - cpu0.0)
        .with("cpu_sys_s", cpu1.1 - cpu0.1)
        .with("commits", if timed_out { recorded() } else { stats.commits })
        .with("trees", stats.trees)
        .with("blobs", stats.blobs)
        .with("objects", objects)
        .with("commits_per_s", if timed_out { recorded() } else { stats.commits } as f64 / wall)
        .with("objects_per_s", objects as f64 / wall)
        .with("object_bytes_written", put_objects.bytes())
        .with("peak_rss_bytes", rss)
        .with("writer_store_s", writer_secs)
        .with("pg_schema_bytes", pg.as_ref().map(|p| p.0))
        .with(
            "pg_tables",
            pg.map(|(_, t)| {
                J::Arr(
                    t.into_iter()
                        .map(|(n, b, r)| {
                            J::obj()
                                .with("table", n)
                                .with("bytes", b as u64)
                                .with("rows_est", r as u64)
                        })
                        .collect(),
                )
            }),
        )
        .with("store_calls", metrics.json());
    write_json(&args.json, &json)
}

/// Progress ticker (until aborted): every 10 s, the interval's throughput and
/// per-op store time.
async fn progress(
    start: Instant,
    put_objects: Arc<Counter>,
    recorded: impl Fn() -> u64,
    metrics: Metrics,
) {
    let mut last = (0u64, 0u64, Instant::now());
    let mut last_ops: HashMap<String, (u64, f64)> = HashMap::new();
    let mut last_cpu = cpu_total();
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let (o, c) = (put_objects.items(), recorded());
        let dt = last.2.elapsed().as_secs_f64();
        let cpu = cpu_total();
        println!(
            "  [{:>6.0}s] objects={o} commits={c}  (+{:.0} obj/s, +{:.0} commits/s; process cpu {:.0}%; rss {:.0} MiB)",
            start.elapsed().as_secs_f64(),
            (o - last.0) as f64 / dt,
            (c - last.1) as f64 / dt,
            100.0 * (cpu - last_cpu) / dt,
            mib(peak_rss_bytes().unwrap_or(0))
        );
        let mut parts = Vec::new();
        for (name, calls, secs) in metrics.totals() {
            let (pcalls, psecs) = last_ops.insert(name.clone(), (calls, secs)).unwrap_or((0, 0.0));
            if calls > pcalls {
                parts.push(format!(
                    "{}={}x/{:.2}s",
                    name.trim_start_matches("store."),
                    calls - pcalls,
                    secs - psecs
                ));
            }
        }
        println!("           ops in interval: {}", parts.join(" "));
        last = (o, c, Instant::now());
        last_cpu = cpu;
    }
}

/// Total size of all tables (incl. indexes/TOAST) in `schema`, plus per-table breakdown.
async fn pg_size(url: &str, schema: &str) -> anyhow::Result<(u64, Vec<(String, i64, i64)>)> {
    let pool = sqlx::PgPool::connect(url).await?;
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT c.relname::text, pg_total_relation_size(c.oid)::bigint, GREATEST(c.reltuples, 0)::bigint \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relkind = 'r' ORDER BY 2 DESC",
    )
    .bind(schema)
    .fetch_all(&pool)
    .await?;
    let total = rows.iter().map(|r| r.1.max(0) as u64).sum();
    Ok((total, rows))
}
