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

#[tokio::main]
async fn main() -> ExitCode {
    match run(Args::parse().serve).await {
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
