//! `ant-bench`: performance / scale harness for antimony.
//!
//! Subcommands:
//!   import   <git-dir> ...   git -> antimony import throughput + per-phase store timings
//!   fleet    --agents N ...  concurrent agent simulation with correctness invariants
//!   checkout --server URL    cold/warm sparse workspace materialization
//!
//! Every subcommand prints a human summary and, with `--json <file>`, writes one
//! JSON object with all metrics.

mod checkout;
mod fleet;
mod import;
mod json;
mod util;

use std::path::{Path, PathBuf};

use ant_server::config::StoreArgs;
use anyhow::Context as _;
use clap::{ArgMatches, CommandFactory, FromArgMatches, Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "ant-bench", about = "antimony performance and scale harness")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Import a git repository and report throughput.
    Import {
        /// Path to the git directory (bare repo or work tree).
        git_dir: PathBuf,
        #[arg(long, default_value = "refs/heads/master")]
        git_ref: String,
        /// Antimony ref to point at the imported tip.
        #[arg(long, default_value = "main")]
        ant_ref: String,
        /// Objects per store batch (ImportOptions::batch_objects).
        #[arg(long, default_value_t = 1024)]
        batch_objects: usize,
        /// Stop (and report partial numbers) after this many seconds; 0 = no limit.
        #[arg(long, default_value_t = 0)]
        max_seconds: u64,
        #[arg(long)]
        json: Option<PathBuf>,
        #[command(flatten)]
        store: StoreArgs,
    },
    /// Simulate N concurrent agents landing changes on one ref.
    Fleet {
        #[arg(long, default_value_t = 16)]
        agents: usize,
        /// Run duration in seconds.
        #[arg(long, default_value_t = 20)]
        duration: u64,
        /// gRPC URL of a running server (e.g. http://127.0.0.1:7420).
        #[arg(long)]
        server: Option<String>,
        /// Serve in-process using the store flags below.
        #[arg(long)]
        embedded: bool,
        /// Files written per changeset.
        #[arg(long, default_value_t = 4)]
        files_per_change: usize,
        /// Probability that a change also rewrites one line of the shared hot file.
        #[arg(long, default_value_t = 0.0)]
        hot_rate: f64,
        #[arg(long = "ref", default_value = "main")]
        ref_name: String,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// Experiment: serve embedded gRPC with TCP_NODELAY on accepted sockets
        /// (ant_server::rpc::serve_on does not set it).
        #[arg(long)]
        nodelay: bool,
        #[arg(long)]
        json: Option<PathBuf>,
        #[command(flatten)]
        store: StoreArgs,
    },
    /// Time sparse workspace materialization cold and warm.
    Checkout {
        #[arg(long)]
        server: String,
        #[arg(long)]
        http: Option<String>,
        #[arg(long, default_value = "main")]
        rev: String,
        /// Sparse pattern (repeatable); none = full checkout.
        #[arg(long)]
        sparse: Vec<String>,
        #[arg(long, default_value_t = 1)]
        iterations: usize,
        /// Disk cache directory (wiped for cold runs).
        #[arg(long)]
        cache: PathBuf,
        /// Workspace directory (default: `<cache>.ws`).
        #[arg(long)]
        workdir: Option<PathBuf>,
        #[arg(long)]
        json: Option<PathBuf>,
    },
}

/// Postgres coordinates re-read from the parsed flags (`StoreArgs` keeps them private).
#[derive(Debug, Clone, Default)]
pub struct StoreInfo {
    pub pg_url: Option<String>,
    pub pg_schema: Option<String>,
}

impl StoreInfo {
    fn from_matches(m: &ArgMatches) -> Self {
        Self {
            pg_url: m.get_one::<String>("pg_url").cloned(),
            pg_schema: m.get_one::<String>("pg_schema").cloned(),
        }
    }
}

pub fn write_json(path: &Option<PathBuf>, value: &json::J) -> anyhow::Result<()> {
    if let Some(p) = path {
        std::fs::write(p, value.render()).with_context(|| format!("writing {}", p.display()))?;
        println!("wrote {}", p.display());
    }
    Ok(())
}

pub fn dir_size(path: &Path) -> (u64, u64) {
    // (files, bytes); skips the `.ant` state dir at the top level only when asked
    // by callers via `dir_size_skip`.
    dir_size_skip(path, None)
}

pub fn dir_size_skip(path: &Path, skip: Option<&str>) -> (u64, u64) {
    let mut files = 0;
    let mut bytes = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else { continue };
        for entry in rd.flatten() {
            if dir == path && skip.is_some_and(|s| entry.file_name() == s) {
                continue;
            }
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_dir() {
                stack.push(entry.path());
            } else {
                files += 1;
                bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    (files, bytes)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .init();
    let code = match run().await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            2
        }
    };
    std::process::exit(code);
}

async fn run() -> anyhow::Result<i32> {
    let matches = Cli::command().get_matches();
    let cli = Cli::from_arg_matches(&matches)?;
    let sub = matches.subcommand().map(|(_, m)| m);
    let info = sub.map(StoreInfo::from_matches).unwrap_or_default();
    match cli.cmd {
        Cmd::Import { git_dir, git_ref, ant_ref, batch_objects, max_seconds, json, store } => {
            let args = import::Args { git_dir, git_ref, ant_ref, batch_objects, max_seconds, json, store, info };
            import::run(args).await?;
            Ok(0)
        }
        Cmd::Fleet {
            agents,
            duration,
            server,
            embedded,
            files_per_change,
            hot_rate,
            ref_name,
            seed,
            nodelay,
            json,
            store,
        } => {
            let args = fleet::Args {
                agents,
                duration,
                server,
                embedded,
                files_per_change,
                hot_rate,
                ref_name,
                seed,
                nodelay,
                json,
                store,
                info,
            };
            fleet::run(args).await
        }
        Cmd::Checkout { server, http, rev, sparse, iterations, cache, workdir, json } => {
            let args = checkout::Args { server, http, rev, sparse, iterations, cache, workdir, json };
            checkout::run(args).await?;
            Ok(0)
        }
    }
}
