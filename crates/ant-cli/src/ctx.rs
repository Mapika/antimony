//! Shared connection/cache context, command outcomes and RPC helpers.

use std::path::PathBuf;
use std::sync::Arc;

use ant_core::{EntryMode, ObjectId};
use ant_proto::v1 as pb;
use antd::disk::DiskObjectStore;
use antd::readthrough::ReadThrough;
use antd::remote::{AntRpcClient, RemoteObjectStore};
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
    /// Output with a non-zero "partial" exit code (e.g. 2 for conflicts).
    pub fn with_code(stdout: impl Into<Vec<u8>>, code: i32) -> Self {
        Self { stdout: stdout.into(), code }
    }
}

/// Print a `hint: ...` line on stderr for a successful command whose result
/// needs follow-up (errors get theirs from `main::hint_for`).
pub fn print_hint(hint: &str) {
    eprintln!("hint: {hint}");
}

/// How often a daemon flushes its cache accounting and enforces the budget.
const MAINTENANCE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);

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

    /// `ReadThrough(DiskObjectStore, RemoteObjectStore)` plus the remote
    /// itself; walks may use HTTP tree bundles when `--http` is set.
    pub async fn read_through(&self) -> Result<(Arc<ReadThrough>, RemoteObjectStore)> {
        self.read_through_over(self.disk()?).await
    }

    /// [`read_through`](Self::read_through) for long-running daemons (`ant
    /// mount`, lazy workspaces): the cache budget is also enforced every
    /// [`MAINTENANCE_INTERVAL`], not only when this process writes.
    pub async fn read_through_daemon(&self) -> Result<(Arc<ReadThrough>, RemoteObjectStore)> {
        let disk = self.disk()?;
        disk.spawn_maintenance(MAINTENANCE_INTERVAL);
        self.read_through_over(disk).await
    }

    async fn read_through_over(
        &self,
        disk: Arc<DiskObjectStore>,
    ) -> Result<(Arc<ReadThrough>, RemoteObjectStore)> {
        let remote = self.remote().await?;
        let rt = ReadThrough::over_disk(disk, Arc::new(remote.clone()))
            .verified_upstream()
            .with_tree_bundles(remote.clone());
        Ok((Arc::new(rt), remote))
    }
}

/// Turn a gRPC status into a plain error carrying just the message.
pub trait RpcResult<T> {
    fn rpc(self) -> Result<T>;
}

impl<T> RpcResult<T> for std::result::Result<T, tonic::Status> {
    fn rpc(self) -> Result<T> {
        self.map_err(|s| {
            anyhow::Error::new(RpcError { code: s.code(), message: s.message().into() })
        })
    }
}

/// A server error: displays as the bare message (the CLI output contract),
/// while keeping the gRPC code so `main` can attach a `hint:`.
#[derive(Debug)]
pub struct RpcError {
    pub code: tonic::Code,
    pub message: String,
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RpcError {}

pub fn id_from(bytes: &[u8]) -> Result<ObjectId> {
    Ok(ObjectId::from_bytes(bytes)?)
}

/// Resolve `rev` on the server to a commit id.
pub async fn resolve_commit(client: &mut AntRpcClient, rev: &str) -> Result<ObjectId> {
    let r = client.resolve_rev(pb::ResolveRevRequest { rev: rev.into() }).await.rpc()?;
    id_from(&r.into_inner().commit)
}

pub fn mode_from_pb(v: i32) -> Result<EntryMode> {
    ant_proto::entry_mode_from_i32(v).ok_or_else(|| anyhow!("server sent unknown entry mode {v}"))
}

pub fn kind_name(m: EntryMode) -> &'static str {
    match m.class() {
        EntryMode::Tree => "tree",
        EntryMode::Gitlink => "commit",
        _ => "blob",
    }
}
