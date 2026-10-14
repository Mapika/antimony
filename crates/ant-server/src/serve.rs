//! Server startup shared by the `ant-server` binary and `ant serve`, plus
//! [`spawn_local`] for in-process servers on ephemeral ports.
//!
//! Flags (clap, each also readable from env `ANT_*`):
//!   --grpc-listen <addr>   default 127.0.0.1:7420
//!   --http-listen <addr>   default 127.0.0.1:7421
//!   --metrics-listen <addr>  (ANT_METRICS_LISTEN) serve `/metrics` on its own
//!                          listener instead of the HTTP port
//!   --pg-lock-timeout <dur> (ANT_PG_LOCK_TIMEOUT) in the store flags, default 10s
//!   --log-format json|text (ANT_LOG_FORMAT) default: text on a terminal,
//!                          json otherwise; level via `RUST_LOG`
//!   --policy <file>        (ANT_POLICY) access-control policy (`ant_auth`);
//!                          without it the server runs in OPEN mode
//!   plus the store flags of [`StoreArgs`] (`--memory` | `--pg-url` ...).

use std::io::IsTerminal;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use ant_store::Backend;
use anyhow::Context;
use clap::{Args, ValueEnum};
use tracing_subscriber::EnvFilter;

use crate::acl::Acl;
use crate::config::StoreArgs;
use crate::metrics::Metrics;
use crate::{http, rpc};

/// Log line format (`--log-format`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    /// Human-readable lines.
    Text,
    /// One JSON object per line, including the enclosing spans (and so the
    /// `request_id`) of each event.
    Json,
}

#[derive(Args, Debug)]
pub struct ServeArgs {
    /// gRPC listen address.
    #[arg(long, env = "ANT_GRPC_LISTEN", default_value = "127.0.0.1:7420")]
    pub grpc_listen: SocketAddr,

    /// Plain-HTTP object endpoint listen address.
    #[arg(long, env = "ANT_HTTP_LISTEN", default_value = "127.0.0.1:7421")]
    pub http_listen: SocketAddr,

    /// Serve Prometheus metrics (`GET /metrics`) on this address only,
    /// instead of on the HTTP port. The endpoint is unauthenticated.
    #[arg(long, env = "ANT_METRICS_LISTEN")]
    pub metrics_listen: Option<SocketAddr>,

    /// Log format: `text` or `json` (default: text on a terminal, json
    /// otherwise).
    #[arg(long, env = "ANT_LOG_FORMAT", value_enum)]
    pub log_format: Option<LogFormat>,

    /// Access-control policy file (TOML, see `ant_auth`). Without it every
    /// request is allowed.
    #[arg(long, env = "ANT_POLICY")]
    pub policy: Option<PathBuf>,

    #[command(flatten)]
    pub store: StoreArgs,
}

/// Install the global tracing subscriber (`RUST_LOG`, default info) writing to
/// stderr. An already installed subscriber is left alone.
pub fn init_logging(format: Option<LogFormat>) {
    let format = format.unwrap_or(if std::io::stderr().is_terminal() {
        LogFormat::Text
    } else {
        LogFormat::Json
    });
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr);
    let _ = match format {
        LogFormat::Text => builder.try_init(),
        LogFormat::Json => builder.json().try_init(),
    };
}

/// Run the server (gRPC and HTTP concurrently) until one of them fails.
/// Logs with tracing-subscriber (`RUST_LOG`, default info).
pub async fn run(args: ServeArgs) -> anyhow::Result<()> {
    init_logging(args.log_format);

    let (backend, probes) = args.store.backend_with_probes().await?;
    let metrics = Metrics::new();
    metrics.set_probes(probes);
    let backend = metrics.instrument(&backend);
    spawn_malloc_trim();
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
    let metrics_listener = match args.metrics_listen {
        Some(addr) => Some(
            tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("binding metrics on {addr}"))?,
        ),
        None => None,
    };
    tracing::info!(
        grpc = %args.grpc_listen,
        http = %args.http_listen,
        metrics = %args.metrics_listen.unwrap_or(args.http_listen),
        "ant-server listening"
    );

    let grpc_fut = rpc::serve_with_metrics(backend.clone(), acl.clone(), grpc, metrics.clone());
    let expose = metrics_listener.is_none();
    let http_metrics = metrics.clone();
    let http_fut = async move {
        http::serve_with_metrics(backend, acl, http_listener, http_metrics, expose)
            .await
            .context("HTTP server failed")
    };
    let metrics_fut = async move {
        match metrics_listener {
            Some(listener) => {
                http::serve_metrics(metrics, listener).await.context("metrics server failed")
            }
            None => std::future::pending().await,
        }
    };
    tokio::try_join!(grpc_fut, http_fut, metrics_fut)?;
    Ok(())
}

/// Addresses of a server started by [`spawn_local`].
#[derive(Debug, Clone, Copy)]
pub struct LocalServer {
    pub grpc: SocketAddr,
    pub http: SocketAddr,
}

impl LocalServer {
    pub fn grpc_url(&self) -> String {
        format!("http://{}", self.grpc)
    }

    pub fn http_url(&self) -> String {
        format!("http://{}", self.http)
    }
}

/// Serve gRPC and HTTP (object endpoint plus git export) for `backend` on
/// ephemeral 127.0.0.1 ports, in background tasks of the current runtime.
/// For tests, benchmarks and embedded use; logging is left to the caller.
pub async fn spawn_local(backend: Backend, acl: Acl) -> std::io::Result<LocalServer> {
    spawn_local_inner(backend, acl, Metrics::new()).await
}

/// [`spawn_local`] with the full production instrumentation: the stores are
/// wrapped with [`Metrics::instrument`] and every layer reports into
/// `metrics`, which the caller can render or scrape at `/metrics`. Callers
/// that also use `backend` directly bypass the store timing (and, since land
/// queues and caches are keyed by store identity, do not share them with the
/// server); read results through the server instead.
pub async fn spawn_local_with_metrics(
    backend: Backend,
    acl: Acl,
    metrics: Arc<Metrics>,
) -> std::io::Result<LocalServer> {
    let backend = metrics.instrument(&backend);
    spawn_local_inner(backend, acl, metrics).await
}

async fn spawn_local_inner(
    backend: Backend,
    acl: Acl,
    metrics: Arc<Metrics>,
) -> std::io::Result<LocalServer> {
    let grpc = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addrs = LocalServer { grpc: grpc.local_addr()?, http: http_listener.local_addr()? };
    tokio::spawn(rpc::serve_with_metrics(backend.clone(), acl.clone(), grpc, metrics.clone()));
    tokio::spawn(http::serve_with_metrics(backend, acl, http_listener, metrics, true));
    Ok(addrs)
}

/// Seconds between `malloc_trim` calls (`ANT_MALLOC_TRIM_SECS`, 0 = never).
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn spawn_malloc_trim() {
    let secs =
        std::env::var("ANT_MALLOC_TRIM_SECS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
    if secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs));
        loop {
            tick.tick().await;
            // SAFETY: malloc_trim only releases free heap memory to the OS.
            let _ = tokio::task::spawn_blocking(|| unsafe { libc::malloc_trim(0) }).await;
        }
    });
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn spawn_malloc_trim() {}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        serve: ServeArgs,
    }

    #[test]
    fn observability_flags_parse() {
        let cli = Cli::try_parse_from([
            "ant-server",
            "--memory",
            "--log-format",
            "json",
            "--metrics-listen",
            "127.0.0.1:9100",
        ])
        .unwrap();
        assert_eq!(cli.serve.log_format, Some(LogFormat::Json));
        assert_eq!(cli.serve.metrics_listen, Some("127.0.0.1:9100".parse().unwrap()));

        let cli = Cli::try_parse_from(["ant-server", "--memory"]).unwrap();
        assert_eq!(cli.serve.log_format, None);
        assert_eq!(cli.serve.metrics_listen, None);
        assert!(Cli::try_parse_from(["ant-server", "--memory", "--log-format", "xml"]).is_err());
    }

    #[test]
    fn pg_lock_timeout_flag_parses() {
        use std::time::Duration;
        // Defaults and overrides are checked through the clap command: the
        // field is private to `StoreArgs`, so assert parse success/failure.
        assert!(Cli::try_parse_from(["ant-server", "--memory"]).is_ok());
        assert!(
            Cli::try_parse_from(["ant-server", "--memory", "--pg-lock-timeout", "250ms"]).is_ok()
        );
        assert!(Cli::try_parse_from(["ant-server", "--memory", "--pg-lock-timeout", "0"]).is_err());
        assert!(Cli::try_parse_from(["ant-server", "--memory", "--pg-lock-timeout", "x"]).is_err());
        assert_eq!(crate::config::parse_duration("250ms"), Ok(Duration::from_millis(250)));
    }
}
