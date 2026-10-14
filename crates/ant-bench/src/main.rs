//! `ant-bench`: performance / scale harness for antimony.
//!
//! Subcommands:
//!   import   <git-dir> ...   git -> antimony import throughput + per-phase store timings
//!   fleet    --agents N ...  concurrent agent simulation with correctness invariants
//!   checkout --server URL    cold/warm sparse workspace materialization
//!   soak     --replicas N ... multi-replica long run: leaks, drift, errors
//!
//! Every subcommand prints a human summary and, with `--json <file>`, writes one
//! JSON object with all metrics. Exit code: 0 on success, 1 when `fleet` or
//! `soak` invariants fail, 2 on an error (`error: ...` on stderr).

mod cdn;
mod checkout;
mod fleet;
mod import;
mod json;
mod replicas;
mod rpc;
mod s3clean;
mod soak;
mod synth;
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

fn main() {
    let cli = Cli::parse();
    if let Cmd::Serve(args) = &cli.cmd {
        // Before the runtime exists: threads keep the arena they start with.
        ant_server::serve::configure_malloc(args.malloc_arenas);
    }
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: starting runtime: {e}");
            std::process::exit(2);
        }
    };
    let code = rt.block_on(main_async(cli));
    std::process::exit(code);
}

async fn main_async(cli: Cli) -> i32 {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();
    match run(cli).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            2
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<i32> {
    match cli.cmd {
        Cmd::Import(args) => import::run(args).await.map(|()| 0),
        Cmd::Fleet(args) => fleet::run(args).await,
        Cmd::Checkout(args) => checkout::run(args).await.map(|()| 0),
        Cmd::Soak(args) => soak::run(args).await,
        Cmd::Serve(args) => ant_server::serve::run(args).await.map(|()| 0),
    }
}
