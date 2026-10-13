//! TASK (transport): `ant-server` binary.
//!
//! Flags (clap, each also readable from env `ANT_*`):
//!   --grpc-listen <addr>   default 127.0.0.1:7420
//!   --http-listen <addr>   default 127.0.0.1:7421
//!   --memory               in-memory backend (dev/testing)
//!   --pg-url <url>         Postgres for refs/meta/small objects
//!   --pg-schema <name>     default "ant"
//!   --s3-endpoint/--s3-bucket/--s3-prefix/--s3-access-key/--s3-secret-key/--s3-region
//!                          large objects; objects > 4 KiB go to S3 via
//!                          `TieredObjectStore`
//! Exactly one of --memory or --pg-url. Runs gRPC and HTTP concurrently;
//! logs with tracing-subscriber (RUST_LOG, default info).

use std::net::SocketAddr;

use anyhow::Context;
use ant_server::config::StoreArgs;
use ant_server::{http, rpc};
use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "ant-server", about = "antimony server: gRPC + HTTP object endpoint")]
struct Args {
    /// gRPC listen address.
    #[arg(long, env = "ANT_GRPC_LISTEN", default_value = "127.0.0.1:7420")]
    grpc_listen: SocketAddr,

    /// Plain-HTTP object endpoint listen address.
    #[arg(long, env = "ANT_HTTP_LISTEN", default_value = "127.0.0.1:7421")]
    http_listen: SocketAddr,

    #[command(flatten)]
    store: StoreArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let args = Args::parse();
    let backend = args.store.backend().await?;

    let grpc = tokio::net::TcpListener::bind(args.grpc_listen)
        .await
        .with_context(|| format!("binding gRPC on {}", args.grpc_listen))?;
    let http_listener = tokio::net::TcpListener::bind(args.http_listen)
        .await
        .with_context(|| format!("binding HTTP on {}", args.http_listen))?;
    tracing::info!(grpc = %args.grpc_listen, http = %args.http_listen, "ant-server listening");

    let grpc_fut = rpc::serve_on(backend.clone(), grpc);
    let http_fut = async move { http::serve_on(backend, http_listener).await.context("HTTP server failed") };
    tokio::try_join!(grpc_fut, http_fut)?;
    Ok(())
}
