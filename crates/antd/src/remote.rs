//! `ObjectStore` (and thin helpers for the other RPCs) over the server's
//! gRPC API, with optional plain-HTTP object reads through a cache/CDN.
//!
//! TASK (transport): implement everything marked `todo!()`.
//!
//! - `connect(grpc_url, http_base)`: tonic channel with
//!   `ant_proto::MAX_MESSAGE_BYTES` limits on the generated client.
//! - `get_many`: if `http_base` is set, GET `{http_base}/o/{hex}` for each id
//!   with bounded concurrency (64) via reqwest; else (or on HTTP transport
//!   error) use the `GetObjects` stream, chunking requests to ≤ 4096 ids.
//!   Results in input order. Verify hashes.
//! - `put_many`: `PutObjects` in chunks of ≤ 64 MiB total payload.
//! - `has_many`: `HasObjects`.
//! - `client()` exposes the raw generated client for other RPCs (refs,
//!   changesets, land, queries) — the CLI uses it directly.

use ant_core::{EncodedObject, ObjectId};
use ant_proto::v1::ant_client::AntClient;
use ant_store::{ObjectStore, Result};
use async_trait::async_trait;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct RemoteObjectStore {
    #[allow(dead_code)]
    client: AntClient<Channel>,
    #[allow(dead_code)]
    http_base: Option<String>,
}

impl RemoteObjectStore {
    pub async fn connect(grpc_url: &str, http_base: Option<&str>) -> anyhow::Result<Self> {
        let _ = (grpc_url, http_base);
        todo!("transport")
    }

    pub fn client(&self) -> AntClient<Channel> {
        self.client.clone()
    }
}

#[async_trait]
impl ObjectStore for RemoteObjectStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        let _ = ids;
        todo!("transport")
    }
    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        let _ = objects;
        todo!("transport")
    }
    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        let _ = ids;
        todo!("transport")
    }
}
