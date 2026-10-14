//! `ObjectStore` (and thin helpers for the other RPCs) over the server's
//! gRPC API, with optional plain-HTTP object reads through a cache/CDN.
//!
//! - `connect(grpc_url, http_base)`: tonic channel with
//!   `ant_proto::MAX_MESSAGE_BYTES` limits on the generated client.
//! - `get_many`: if `http_base` is set, GET `{http_base}/o/{hex}` for each id
//!   with bounded concurrency (64) via reqwest; else (or on HTTP transport
//!   error) the `GetObjects` stream, in requests of ≤ 4096 ids. Results come
//!   in input order, hashes verified.
//! - `put_many`: `PutObjects` in chunks of ≤ 64 MiB total payload.
//! - `has_many`: `HasObjects`.
//! - `client()` exposes the raw generated client for other RPCs (refs,
//!   changesets, land, queries) — the CLI uses it directly.
//! - `connect_with_token` sends `authorization: Bearer <token>` on every
//!   RPC and HTTP object request (servers running with an access policy).

use ant_core::{EncodedObject, ObjectId};
use ant_proto::MAX_MESSAGE_BYTES;
use ant_proto::v1 as pb;
use ant_proto::v1::ant_client::AntClient;
use ant_store::{ObjectStore, Result, StoreError};
use async_trait::async_trait;

use crate::par::par_map;
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

/// Concurrent HTTP object fetches.
const HTTP_CONCURRENCY: usize = 64;
/// Max ids per `GetObjects` / `HasObjects` request.
const RPC_IDS_PER_REQUEST: usize = 4096;
/// `GetObjects` requests in flight per `get_many`.
const GRPC_CONCURRENCY: usize = 4;
/// Max total payload bytes per `PutObjects` request.
const PUT_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Adds `authorization: Bearer <token>` to every request when set.
#[derive(Clone, Default)]
pub struct BearerAuth(Option<MetadataValue<Ascii>>);

impl Interceptor for BearerAuth {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        if let Some(v) = &self.0 {
            req.metadata_mut().insert("authorization", v.clone());
        }
        Ok(req)
    }
}

/// The generated client with [`BearerAuth`].
pub type AntRpcClient = AntClient<InterceptedService<Channel, BearerAuth>>;

#[derive(Clone)]
pub struct RemoteObjectStore {
    client: AntRpcClient,
    http_base: Option<String>,
    http: reqwest::Client,
    /// `Bearer <token>` for HTTP object reads.
    authorization: Option<String>,
}

impl RemoteObjectStore {
    pub async fn connect(grpc_url: &str, http_base: Option<&str>) -> anyhow::Result<Self> {
        Self::connect_with_token(grpc_url, http_base, None).await
    }

    /// [`Self::connect`], authenticating every request with `token`.
    pub async fn connect_with_token(
        grpc_url: &str,
        http_base: Option<&str>,
        token: Option<&str>,
    ) -> anyhow::Result<Self> {
        let authorization = token.map(|t| format!("Bearer {t}"));
        let auth = match &authorization {
            Some(a) => BearerAuth(Some(
                a.parse().map_err(|_| anyhow::anyhow!("token is not a valid header value"))?,
            )),
            None => BearerAuth(None),
        };
        let channel = Channel::from_shared(grpc_url.to_owned())?.connect().await?;
        let client = AntClient::with_interceptor(channel, auth)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        Ok(Self {
            client,
            http_base: http_base.map(|b| b.trim_end_matches('/').to_owned()),
            http: reqwest::Client::new(),
            authorization,
        })
    }

    pub fn client(&self) -> AntRpcClient {
        self.client.clone()
    }

    /// Fetch every id over HTTP. Any failure (transport, unexpected status,
    /// hash mismatch) is an error so the caller can fall back to gRPC.
    async fn get_many_http(
        &self,
        base: &str,
        ids: &[ObjectId],
    ) -> Result<Vec<Option<EncodedObject>>> {
        let http = self.http.clone();
        let base = base.to_owned();
        let auth = self.authorization.clone();
        let raw: Vec<Option<Bytes>> = stream::iter(ids.iter().copied())
            .map(move |id| {
                let http = http.clone();
                let url = format!("{base}/o/{}", id.to_hex());
                let auth = auth.clone();
                async move { fetch_http_object(&http, &url, auth.as_deref()).await }
            })
            .buffered(HTTP_CONCURRENCY)
            .try_collect()
            .await?;
        decode_all_verified(ids, raw).await
    }

    /// Fetch ids via the `GetObjects` stream, in chunks of at most
    /// `RPC_IDS_PER_REQUEST`. Results are returned in input order.
    async fn get_many_grpc(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        // A few requests in flight at once; `buffered` keeps them in order.
        let chunks: Vec<Vec<ObjectId>> =
            ids.chunks(RPC_IDS_PER_REQUEST).map(<[ObjectId]>::to_vec).collect();
        let parts: Vec<Vec<Option<Bytes>>> = stream::iter(chunks)
            .map(|chunk| {
                let this = self.clone();
                async move { this.get_chunk_grpc(&chunk).await }
            })
            .buffered(GRPC_CONCURRENCY)
            .try_collect()
            .await?;
        decode_all_verified(ids, parts.into_iter().flatten().collect()).await
    }

    /// One `GetObjects` request; raw (unverified) objects in `chunk` order.
    async fn get_chunk_grpc(&self, chunk: &[ObjectId]) -> Result<Vec<Option<Bytes>>> {
        let req = pb::GetObjectsRequest { ids: chunk.iter().map(|i| i.0.to_vec()).collect() };
        let mut stream =
            self.client.clone().get_objects(req).await.map_err(StoreError::backend)?.into_inner();
        let mut raw: Vec<Option<Bytes>> = Vec::with_capacity(chunk.len());
        while let Some(item) = stream.message().await.map_err(StoreError::backend)? {
            let expected =
                chunk.get(raw.len()).ok_or_else(|| protocol_error("extra object in stream"))?;
            if item.id != expected.0 {
                return Err(protocol_error("object stream out of order"));
            }
            raw.push(item.found.then(|| Bytes::from(item.data)));
        }
        if raw.len() != chunk.len() {
            return Err(protocol_error("object stream ended early"));
        }
        Ok(raw)
    }
}

fn protocol_error(msg: &str) -> StoreError {
    StoreError::backend(std::io::Error::other(format!("protocol error: {msg}")))
}

fn decode_verified(data: Bytes, id: &ObjectId) -> Result<EncodedObject> {
    let obj = EncodedObject::from_bytes(data)?;
    obj.verify(id)?;
    Ok(obj)
}

/// Frame and hash-check every fetched object (the sole verification of
/// remote data), spread over several threads: hashing a large checkout is
/// CPU-bound and must not run on the async executor.
async fn decode_all_verified(
    ids: &[ObjectId],
    raw: Vec<Option<Bytes>>,
) -> Result<Vec<Option<EncodedObject>>> {
    let work: Vec<(ObjectId, Option<Bytes>)> = ids.iter().copied().zip(raw).collect();
    tokio::task::spawn_blocking(move || {
        par_map(&work, 16, |(id, data)| {
            data.as_ref().map(|d| decode_verified(d.clone(), id)).transpose()
        })
        .into_iter()
        .collect()
    })
    .await
    .map_err(StoreError::backend)?
}

async fn fetch_http_object(
    http: &reqwest::Client,
    url: &str,
    authorization: Option<&str>,
) -> Result<Option<Bytes>> {
    let mut req = http.get(url);
    if let Some(a) = authorization {
        req = req.header(reqwest::header::AUTHORIZATION, a);
    }
    let resp = req.send().await.map_err(StoreError::backend)?;
    match resp.status() {
        s if s == reqwest::StatusCode::NOT_FOUND => Ok(None),
        s if s.is_success() => Ok(Some(resp.bytes().await.map_err(StoreError::backend)?)),
        s => Err(StoreError::backend(std::io::Error::other(format!("HTTP {s} for {url}")))),
    }
}

#[async_trait]
impl ObjectStore for RemoteObjectStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(base) = &self.http_base {
            match self.get_many_http(base, ids).await {
                Ok(objects) => return Ok(objects),
                Err(e) => {
                    tracing::debug!(error = %e, "HTTP object read failed; falling back to gRPC")
                }
            }
        }
        self.get_many_grpc(ids).await
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        let mut batch: Vec<Vec<u8>> = Vec::new();
        let mut batch_bytes = 0usize;
        for obj in objects {
            let len = obj.as_bytes().len();
            if !batch.is_empty() && batch_bytes + len > PUT_BATCH_BYTES {
                self.put_batch(std::mem::take(&mut batch)).await?;
                batch_bytes = 0;
            }
            batch.push(obj.as_bytes().to_vec());
            batch_bytes += len;
        }
        if !batch.is_empty() {
            self.put_batch(batch).await?;
        }
        Ok(())
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(RPC_IDS_PER_REQUEST) {
            let req = pb::HasObjectsRequest { ids: chunk.iter().map(|i| i.0.to_vec()).collect() };
            let resp = self
                .client
                .clone()
                .has_objects(req)
                .await
                .map_err(StoreError::backend)?
                .into_inner();
            if resp.present.len() != chunk.len() {
                return Err(protocol_error("HasObjects response length mismatch"));
            }
            out.extend(resp.present);
        }
        Ok(out)
    }
}

impl RemoteObjectStore {
    async fn put_batch(&self, data: Vec<Vec<u8>>) -> Result<()> {
        let expected = data.len();
        let resp = self
            .client
            .clone()
            .put_objects(pb::PutObjectsRequest { data })
            .await
            .map_err(StoreError::backend)?
            .into_inner();
        if resp.ids.len() != expected {
            return Err(protocol_error("PutObjects response length mismatch"));
        }
        Ok(())
    }
}
