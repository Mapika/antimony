//! Server startup shared by the `ant-server` binary and `ant serve`.
//!
//! Flags (clap, each also readable from env `ANT_*`):
//!   --grpc-listen <addr>   default 127.0.0.1:7420
//!   --http-listen <addr>   default 127.0.0.1:7421
//!   --policy <file>        (ANT_POLICY) access-control policy (`ant_auth`);
//!                          without it the server runs in OPEN mode
//!   plus the store flags of [`StoreArgs`] (`--memory` | `--pg-url` ...).

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use clap::Args;
use tracing_subscriber::EnvFilter;

use crate::acl::Acl;
use crate::config::StoreArgs;
use crate::{http, rpc};

#[derive(Args, Debug)]
pub struct ServeArgs {
    /// gRPC listen address.
    #[arg(long, env = "ANT_GRPC_LISTEN", default_value = "127.0.0.1:7420")]
    pub grpc_listen: SocketAddr,

    /// Plain-HTTP object endpoint listen address.
    #[arg(long, env = "ANT_HTTP_LISTEN", default_value = "127.0.0.1:7421")]
    pub http_listen: SocketAddr,

    /// Access-control policy file (TOML, see `ant_auth`). Without it every
    /// request is allowed.
    #[arg(long, env = "ANT_POLICY")]
    pub policy: Option<PathBuf>,

    #[command(flatten)]
    pub store: StoreArgs,
}

/// Run the server (gRPC and HTTP concurrently) until one of them fails.
/// Logs with tracing-subscriber (`RUST_LOG`, default info).
pub async fn run(args: ServeArgs) -> anyhow::Result<()> {
    // Ignore the error: a subscriber may already be installed.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();

    let backend = args.store.backend().await?;
    let acl = match &args.policy {
        Some(path) => {
            let acl = Acl::load(path)?;
            tracing::info!(policy = %path.display(), "access control enabled");
            acl
        }
        None => {
            tracing::warn!("no --policy given: OPEN mode, every request is allowed");
            Acl::open()
        }
    };

    let grpc = tokio::net::TcpListener::bind(args.grpc_listen)
        .await
        .with_context(|| format!("binding gRPC on {}", args.grpc_listen))?;
    let http_listener = tokio::net::TcpListener::bind(args.http_listen)
        .await
        .with_context(|| format!("binding HTTP on {}", args.http_listen))?;
    tracing::info!(grpc = %args.grpc_listen, http = %args.http_listen, "ant-server listening");

    let grpc_fut = rpc::serve_with_acl(backend.clone(), acl.clone(), grpc);
    let http_fut = async move {
        http::serve_with_acl(backend, acl, http_listener).await.context("HTTP server failed")
    };
    tokio::try_join!(grpc_fut, http_fut)?;
    Ok(())
}
