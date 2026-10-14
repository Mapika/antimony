//! `ant-server` binary: thin wrapper over [`ant_server::serve`] (the same
//! server is available as `ant serve`).

use std::process::ExitCode;

use ant_server::serve::{ServeArgs, run};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "ant-server", about = "antimony server: gRPC + HTTP object endpoint", version)]
struct Args {
    #[command(flatten)]
    serve: ServeArgs,
}

fn main() -> ExitCode {
    let args = Args::parse().serve;
    // Before the runtime exists: threads keep the arena they start with.
    ant_server::serve::configure_malloc(args.malloc_arenas);
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: starting runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            if let Some(hint) = ant_store::compat_hint(e.chain()) {
                eprintln!("hint: {hint}");
            }
            ExitCode::FAILURE
        }
    }
}
