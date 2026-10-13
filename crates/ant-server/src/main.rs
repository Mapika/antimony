//! `ant-server` binary: thin wrapper over [`ant_server::serve`] (the same
//! server is available as `ant serve`).

use ant_server::serve::{ServeArgs, run};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "ant-server", about = "antimony server: gRPC + HTTP object endpoint", version)]
struct Args {
    #[command(flatten)]
    serve: ServeArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run(Args::parse().serve).await
}
