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
use std::sync::Arc;

use anyhow::{Context, bail};
use ant_store::postgres::PgStore;
use ant_store::s3::{S3Config, S3ObjectStore};
use ant_store::tiered::{DEFAULT_INLINE_THRESHOLD, TieredObjectStore};
use ant_store::{Backend, ObjectStore};
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

    /// Use the in-memory backend (dev/testing). Exclusive with --pg-url.
    #[arg(long, env = "ANT_MEMORY")]
    memory: bool,

    /// Postgres URL for refs, metadata and small objects.
    #[arg(long, env = "ANT_PG_URL")]
    pg_url: Option<String>,

    /// Postgres schema holding the tables.
    #[arg(long, env = "ANT_PG_SCHEMA", default_value = "ant")]
    pg_schema: String,

    /// S3 endpoint for large objects.
    #[arg(long, env = "ANT_S3_ENDPOINT")]
    s3_endpoint: Option<String>,

    #[arg(long, env = "ANT_S3_BUCKET")]
    s3_bucket: Option<String>,

    #[arg(long, env = "ANT_S3_PREFIX", default_value = "")]
    s3_prefix: String,

    #[arg(long, env = "ANT_S3_ACCESS_KEY")]
    s3_access_key: Option<String>,

    #[arg(long, env = "ANT_S3_SECRET_KEY")]
    s3_secret_key: Option<String>,

    #[arg(long, env = "ANT_S3_REGION", default_value = "us-east-1")]
    s3_region: String,
}

impl Args {
    fn s3_config(&self) -> anyhow::Result<Option<S3Config>> {
        let any = self.s3_endpoint.is_some()
            || self.s3_bucket.is_some()
            || self.s3_access_key.is_some()
            || self.s3_secret_key.is_some();
        if !any {
            return Ok(None);
        }
        let (Some(endpoint), Some(bucket), Some(access_key), Some(secret_key)) = (
            self.s3_endpoint.clone(),
            self.s3_bucket.clone(),
            self.s3_access_key.clone(),
            self.s3_secret_key.clone(),
        ) else {
            bail!("--s3-endpoint, --s3-bucket, --s3-access-key and --s3-secret-key must be given together");
        };
        Ok(Some(S3Config {
            endpoint,
            region: self.s3_region.clone(),
            bucket,
            prefix: self.s3_prefix.clone(),
            access_key,
            secret_key,
        }))
    }

    async fn backend(&self) -> anyhow::Result<Backend> {
        match (self.memory, self.pg_url.as_deref()) {
            (true, None) => {
                if self.s3_config()?.is_some() {
                    bail!("--s3-* flags require --pg-url, not --memory");
                }
                Ok(Backend::in_memory())
            }
            (false, Some(url)) => {
                let pg = Arc::new(
                    PgStore::connect(url, &self.pg_schema)
                        .await
                        .context("connecting to Postgres")?,
                );
                let objects: Arc<dyn ObjectStore> = match self.s3_config()? {
                    None => pg.clone(),
                    Some(cfg) => {
                        let s3 = S3ObjectStore::connect(&cfg).await.context("configuring S3")?;
                        s3.ensure_bucket().await.context("ensuring S3 bucket")?;
                        Arc::new(TieredObjectStore::new(
                            pg.clone(),
                            Arc::new(s3),
                            DEFAULT_INLINE_THRESHOLD,
                        ))
                    }
                };
                Ok(Backend { objects, refs: pg.clone(), meta: pg })
            }
            (true, Some(_)) => bail!("--memory and --pg-url are mutually exclusive"),
            (false, None) => bail!("exactly one of --memory or --pg-url is required"),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let args = Args::parse();
    let backend = args.backend().await?;

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
