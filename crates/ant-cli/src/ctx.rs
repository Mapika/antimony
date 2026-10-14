//! Shared connection/cache context.

use std::path::PathBuf;
use std::sync::Arc;

use ant_core::{EntryMode, ObjectId};
use ant_proto::v1 as pb;
use antd::disk::DiskObjectStore;
use antd::readthrough::ReadThrough;
use antd::remote::RemoteObjectStore;
use anyhow::{Context as _, Result, anyhow};

use crate::args::Cli;

/// Command output: bytes for stdout plus the process exit code.
pub struct Outcome {
    pub stdout: Vec<u8>,
    pub code: i32,
}

impl Outcome {
    pub fn ok(stdout: impl Into<Vec<u8>>) -> Self {
        Self { stdout: stdout.into(), code: 0 }
    }
    pub fn empty() -> Self {
        Self::ok(Vec::new())
    }
}

pub struct Ctx {
    pub server: String,
    pub http: Option<String>,
    pub cache: PathBuf,
    pub cache_bytes: u64,
    pub directory: Option<PathBuf>,
    pub token: Option<String>,
}

impl Ctx {
    pub fn from_cli(cli: &Cli) -> Result<Self> {
        let cache = match &cli.cache {
            Some(c) => c.clone(),
            None => {
                let home = std::env::var_os("HOME")
                    .ok_or_else(|| anyhow!("HOME is not set; pass --cache"))?;
                PathBuf::from(home).join(".cache/antimony")
            }
        };
        Ok(Self {
            server: cli.server.clone(),
            http: cli.http.clone(),
            cache,
            cache_bytes: cli.cache_bytes,
            directory: cli.directory.clone(),
            token: cli.token.clone().filter(|t| !t.is_empty()),
        })
    }

    pub async fn remote(&self) -> Result<RemoteObjectStore> {
        RemoteObjectStore::connect_with_token(
            &self.server,
            self.http.as_deref(),
            self.token.as_deref(),
        )
        .await
        .with_context(|| format!("connecting to {}", self.server))
    }

    pub fn disk(&self) -> Result<Arc<DiskObjectStore>> {
        let d = DiskObjectStore::open(&self.cache, self.cache_bytes)
            .with_context(|| format!("opening cache {}", self.cache.display()))?;
        Ok(Arc::new(d))
    }

    /// `ReadThrough(DiskObjectStore, RemoteObjectStore)` plus the remote itself.
    pub async fn read_through(&self) -> Result<(Arc<ReadThrough>, RemoteObjectStore)> {
        let remote = self.remote().await?;
        let rt = ReadThrough::over_disk(self.disk()?, Arc::new(remote.clone())).verified_upstream();
        Ok((Arc::new(rt), remote))
    }
}

/// Turn a gRPC status into a plain error carrying just the message.
pub trait RpcResult<T> {
    fn rpc(self) -> Result<T>;
}

impl<T> RpcResult<T> for std::result::Result<T, tonic::Status> {
    fn rpc(self) -> Result<T> {
        self.map_err(|s| anyhow!("{}", s.message()))
    }
}

pub fn id_from(bytes: &[u8]) -> Result<ObjectId> {
    Ok(ObjectId::from_bytes(bytes)?)
}

pub fn mode_from_pb(v: i32) -> Result<EntryMode> {
    pb::EntryMode::try_from(v)
        .ok()
        .and_then(ant_proto::entry_mode)
        .ok_or_else(|| anyhow!("server sent unknown entry mode {v}"))
}

pub fn kind_name(m: EntryMode) -> &'static str {
    match m {
        EntryMode::Tree => "tree",
        EntryMode::Gitlink => "commit",
        _ => "blob",
    }
}
