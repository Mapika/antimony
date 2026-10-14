//! `ant-bench`: performance / scale harness for antimony.
//!
//! Subcommands:
//!   import   <git-dir> ...   git -> antimony import throughput + per-phase store timings
//!   fleet    --agents N ...  concurrent agent simulation with correctness invariants
//!   checkout --server URL    cold/warm sparse workspace materialization
//!   soak     --replicas N ... multi-replica long run: leaks, drift, errors
//!
//! Every subcommand prints a human summary and, with `--json <file>`, writes one
//! JSON object with all metrics.

mod checkout;
mod fleet;
mod import;
mod json;
mod replicas;
mod soak;
mod util;

use clap::{Parser, Subcommand};
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
    Import(import::Args),
    /// Simulate N concurrent agents landing changes on one ref.
    Fleet(fleet::Args),
    /// Time sparse workspace materialization cold and warm.
    Checkout(checkout::Args),
    /// Drive N server replicas with many agents for a long time; report per window.
    Soak(soak::Args),
    /// Run a server (same as `ant serve`); used by `soak` to spawn replicas.
    #[command(hide = true)]
    Serve(ant_server::serve::ServeArgs),
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
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
    match Cli::parse().cmd {
        Cmd::Import(args) => import::run(args).await.map(|()| 0),
        Cmd::Fleet(args) => fleet::run(args).await,
        Cmd::Checkout(args) => checkout::run(args).await.map(|()| 0),
        Cmd::Soak(args) => soak::run(args).await,
        Cmd::Serve(args) => ant_server::serve::run(args).await.map(|()| 0),
    }
}
