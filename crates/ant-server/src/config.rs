//! Store flags shared by `ant-server` and the `ant` CLI.

use std::sync::Arc;

use ant_store::postgres::PgStore;
use ant_store::s3::{S3Config, S3ObjectStore};
use ant_store::tiered::{DEFAULT_INLINE_THRESHOLD, TieredObjectStore};
use ant_store::{Backend, ObjectStore};
use anyhow::{Context, bail};

/// Backend selection flags (`--memory` | `--pg-url` + optional `--s3-*`).
#[derive(clap::Args, Debug, Clone)]
pub struct StoreArgs {
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

    /// Objects whose encoded size is at most this many bytes live in Postgres;
    /// larger ones go to S3 (only with --s3-*).
    #[arg(long, env = "ANT_INLINE_THRESHOLD", default_value_t = DEFAULT_INLINE_THRESHOLD)]
    inline_threshold: usize,
}

impl StoreArgs {
    /// The Postgres URL, when a Postgres backend is selected.
    pub fn pg_url(&self) -> Option<&str> {
        self.pg_url.as_deref()
    }

    /// The Postgres schema holding the tables.
    pub fn pg_schema(&self) -> &str {
        &self.pg_schema
    }

    pub fn s3_config(&self) -> anyhow::Result<Option<S3Config>> {
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
            bail!(
                "--s3-endpoint, --s3-bucket, --s3-access-key and --s3-secret-key must be given together"
            );
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

    pub async fn backend(&self) -> anyhow::Result<Backend> {
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
                            self.inline_threshold,
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
