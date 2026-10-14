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
//! - `get_tree_bundles(trees)` (HTTP only): `GET {http_base}/t/{hex}` per
//!   tree (concurrency 64), each a bundle of the tree plus its blobs and
//!   whole subdirectories (`ant_proto::bundle`); every object is verified
//!   against its id, and the first must be the requested tree. Per tree the
//!   result is `None` when no bundle is available: no HTTP base, transport
//!   or decoding error, or a 404. A 404/405/501 without the `x-ant-bundle`
//!   header means an older server without the route: bundles are then off
//!   for this store (and its clones) and callers fetch per object.
//! - `http_requests()`: HTTP object and bundle requests issued so far.
//! - `connect_with_token` sends `authorization: Bearer <token>` on every
//!   RPC and HTTP object request (servers running with an access policy).

use ant_core::{EncodedObject, ObjectId};
use ant_proto::MAX_MESSAGE_BYTES;
use ant_proto::v1 as pb;
use ant_proto::v1::ant_client::AntClient;
use ant_store::{ObjectStore, Result, StoreError};
use async_trait::async_trait;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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
/// Largest decompressed tree bundle accepted (servers send up to ~8 MiB).
const MAX_BUNDLE_BYTES: usize = 256 * 1024 * 1024;

/// HTTP requests issued by a [`RemoteObjectStore`] (and its clones).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HttpRequests {
    /// `GET /o/{id}`.
    pub objects: u64,
    /// `GET /t/{id}`.
    pub bundles: u64,
}

#[derive(Default)]
struct HttpState {
    objects: AtomicU64,
    bundles: AtomicU64,
    /// The server answered `/t/` like a server without the route.
    no_bundles: AtomicBool,
}

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
    state: Arc<HttpState>,
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
            state: Arc::default(),
        })
    }

    pub fn client(&self) -> AntRpcClient {
        self.client.clone()
    }

    /// HTTP requests issued so far by this store and its clones.
    pub fn http_requests(&self) -> HttpRequests {
        HttpRequests {
            objects: self.state.objects.load(Ordering::Relaxed),
            bundles: self.state.bundles.load(Ordering::Relaxed),
        }
    }

    /// True if [`Self::get_tree_bundles`] may fetch anything: an HTTP base
    /// is set and the server has not shown it lacks the route.
    pub fn tree_bundles_enabled(&self) -> bool {
        self.http_base.is_some() && !self.state.no_bundles.load(Ordering::Relaxed)
    }

    /// The tree bundle of each of `trees` (see the module docs): verified
    /// `(id, object)` pairs, the tree first, or `None` where no bundle is
    /// available (the caller then fetches per object).
    pub async fn get_tree_bundles(
        &self,
        trees: &[ObjectId],
    ) -> Vec<Option<Vec<(ObjectId, EncodedObject)>>> {
        let Some(base) = self.http_base.as_deref().filter(|_| self.tree_bundles_enabled()) else {
            return vec![None; trees.len()];
        };
        stream::iter(trees.iter().copied())
            .map(|tree| async move {
                if self.state.no_bundles.load(Ordering::Relaxed) {
                    return None;
                }
                match self.fetch_bundle(base, tree).await {
                    Ok(found) => found,
                    Err(e) => {
                        tracing::debug!(error = %e, %tree, "tree bundle unavailable; fetching per object");
                        None
                    }
                }
            })
            .buffered(HTTP_CONCURRENCY)
            .collect()
            .await
    }

    /// One `GET /t/{tree}`: `Ok(None)` for "no bundle" answers (404s; an
    /// older server also turns bundles off), errors for anything unusable.
    async fn fetch_bundle(
        &self,
        base: &str,
        tree: ObjectId,
    ) -> Result<Option<Vec<(ObjectId, EncodedObject)>>> {
        let url = format!("{base}/t/{}", tree.to_hex());
        let mut req = self.http.get(&url);
        if let Some(a) = &self.authorization {
            req = req.header(reqwest::header::AUTHORIZATION, a);
        }
        self.state.bundles.fetch_add(1, Ordering::Relaxed);
        let resp = req.send().await.map_err(StoreError::backend)?;
        let status = resp.status();
        if !status.is_success() {
            let has_route = resp.headers().contains_key(ant_proto::bundle::HEADER);
            let missing_route = matches!(status.as_u16(), 404 | 405 | 501) && !has_route;
            if missing_route {
                self.state.no_bundles.store(true, Ordering::Relaxed);
            }
            if missing_route || status == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            return Err(StoreError::backend(std::io::Error::other(format!(
                "HTTP {status} for {url}"
            ))));
        }
        let body = resp.bytes().await.map_err(StoreError::backend)?;
        let objects = tokio::task::spawn_blocking(move || decode_bundle(&body, tree))
            .await
            .map_err(StoreError::backend)??;
        Ok(Some(objects))
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
                let state = self.state.clone();
                async move {
                    state.objects.fetch_add(1, Ordering::Relaxed);
                    fetch_http_object(&http, &url, auth.as_deref()).await
                }
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

/// Blocking: unframe a bundle body and hash-check every object; the first
/// must be `tree`.
fn decode_bundle(body: &[u8], tree: ObjectId) -> Result<Vec<(ObjectId, EncodedObject)>> {
    let frames = ant_proto::bundle::decode(body, MAX_BUNDLE_BYTES).map_err(StoreError::backend)?;
    if frames.first().map(|(id, _)| *id) != Some(tree) {
        return Err(protocol_error("bundle does not start with the requested tree"));
    }
    par_map(&frames, 16, |(id, data)| decode_verified(data.clone(), id).map(|o| (*id, o)))
        .into_iter()
        .collect()
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
