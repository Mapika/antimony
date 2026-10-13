//! gRPC adapter over `ops`.
//!
//! [`AntService`] implements the generated `ant_proto::v1::ant_server::Ant`
//! trait.
//!
//! - Every RPC is a thin translation layer: decode/validate request →
//!   call `ops::*` / `Backend` → encode response. No business logic here.
//! - Error mapping: `OpError::NotFound` → `Status::not_found`,
//!   `InvalidArgument` (and malformed ids/paths/modes in the request) →
//!   `invalid_argument`, `Aborted` → `aborted`, store/backend errors →
//!   `internal`. Never panic on bad input.
//! - `GetObjects`: stream results in request order; fetch from the store in
//!   chunks of 256 ids (one `get_many` per chunk). Reject > 100_000 ids.
//! - `PutObjects`: parse each with `EncodedObject::from_bytes`, store with one
//!   `put_many`, return ids in order.
//! - `Log`/`Diff`/`ReadPath`/`ResolveRev`/`Land`/`CreateChangeset`: map 1:1
//!   onto `ops::query`, `ops::land`, `ops::changeset`.
//! - Max decoding/encoding message size is `ant_proto::MAX_MESSAGE_BYTES`.
//!
//! Access control (`crate::acl`): with a policy, every RPC authenticates the
//! `authorization: Bearer <token>` metadata (missing/unknown →
//! `unauthenticated`) and delegates to the `ops::*_as` variants; object
//! RPCs report unreadable objects exactly like missing ones.

use std::net::SocketAddr;

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, RepoPath, Signature};
use ant_proto::MAX_MESSAGE_BYTES;
use ant_proto::v1 as pb;
use ant_proto::v1::ant_server::{Ant, AntServer};
use ant_store::{Backend, CasOutcome, ChangeKind, StoreError};
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use tonic::{Request, Response, Status};

use crate::acl::{self, Access, Acl};
use crate::ops::OpError;
use crate::ops::changeset::{self, ChangesetRequest, FileOp, WriteContent};
use crate::ops::history;
use crate::ops::land::{self, LandOutcome};
use crate::ops::query;

/// Ids per `get_many` call when serving `GetObjects`.
const OBJECT_CHUNK: usize = 256;
/// Maximum number of ids accepted by one `GetObjects` / `HasObjects` request.
const MAX_IDS: usize = 100_000;

#[derive(Clone)]
pub struct AntService {
    pub backend: Backend,
    acl: Acl,
}

impl AntService {
    /// Open mode (no access control).
    pub fn new(backend: Backend) -> Self {
        Self::with_acl(backend, Acl::open())
    }

    pub fn with_acl(backend: Backend, acl: Acl) -> Self {
        Self { backend, acl }
    }

    /// The caller of `request`: `None` in open mode.
    fn access<T>(&self, request: &Request<T>) -> Result<Option<Access>, Status> {
        if self.acl.is_open() {
            return Ok(None);
        }
        let header = request.metadata().get("authorization").and_then(|v| v.to_str().ok());
        self.acl.authenticate_header(header).map_err(|e| Status::unauthenticated(e.to_string()))
    }
}

/// Serve gRPC on `addr` until the future is dropped.
pub async fn serve(backend: Backend, addr: SocketAddr) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve_on(backend, listener).await
}

/// Serve gRPC on an already-bound listener (tests bind port 0).
pub async fn serve_on(backend: Backend, listener: tokio::net::TcpListener) -> anyhow::Result<()> {
    serve_with_acl(backend, Acl::open(), listener).await
}

/// [`serve_on`] enforcing `acl`.
pub async fn serve_with_acl(
    backend: Backend,
    acl: Acl,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    let svc = AntServer::new(AntService::with_acl(backend, acl))
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    // Without TCP_NODELAY, Nagle + delayed ACKs add ~40 ms to small RPCs.
    let incoming = tonic::transport::server::TcpIncoming::from(listener).with_nodelay(Some(true));
    tonic::transport::Server::builder().add_service(svc).serve_with_incoming(incoming).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// error and value mapping
// ---------------------------------------------------------------------------

fn store_status(e: StoreError) -> Status {
    match e {
        StoreError::NotFound(id) => Status::not_found(format!("object {id}")),
        StoreError::InvalidArgument(m) => Status::invalid_argument(m),
        StoreError::Core(e) => Status::invalid_argument(e.to_string()),
        StoreError::WrongKind { .. } => Status::invalid_argument(e.to_string()),
        StoreError::Backend(_) => {
            tracing::error!(error = %e, "store error");
            Status::internal("storage backend error")
        }
    }
}

fn op_status(e: OpError) -> Status {
    match e {
        OpError::NotFound(m) => Status::not_found(m),
        OpError::InvalidArgument(m) => Status::invalid_argument(m),
        OpError::Aborted(m) => Status::aborted(m),
        OpError::PermissionDenied(m) => Status::permission_denied(m),
        OpError::Store(e) => store_status(e),
        OpError::Core(e) => Status::invalid_argument(e.to_string()),
    }
}

fn parse_id(bytes: &[u8]) -> Result<ObjectId, Status> {
    ObjectId::from_bytes(bytes).map_err(|e| Status::invalid_argument(e.to_string()))
}

fn parse_opt_id(bytes: Option<&Vec<u8>>) -> Result<Option<ObjectId>, Status> {
    bytes.map(|b| parse_id(b)).transpose()
}

fn parse_ids(raw: &[Vec<u8>]) -> Result<Vec<ObjectId>, Status> {
    if raw.len() > MAX_IDS {
        return Err(Status::invalid_argument(format!(
            "too many ids: {} (max {MAX_IDS})",
            raw.len()
        )));
    }
    raw.iter().map(|b| parse_id(b)).collect()
}

fn parse_rev_path(path: &str) -> Result<RepoPath, Status> {
    RepoPath::new(path).map_err(|e| Status::invalid_argument(e.to_string()))
}

fn mode_from_pb(v: i32) -> Result<EntryMode, Status> {
    let mode = pb::EntryMode::try_from(v)
        .map_err(|_| Status::invalid_argument(format!("unknown entry mode {v}")))?;
    ant_proto::entry_mode(mode)
        .ok_or_else(|| Status::invalid_argument("entry mode must be specified"))
}

fn mode_to_pb(m: EntryMode) -> i32 {
    pb::EntryMode::from(m) as i32
}

fn kind_to_pb(k: ChangeKind) -> i32 {
    (match k {
        ChangeKind::Added => pb::ChangeKind::Added,
        ChangeKind::Modified => pb::ChangeKind::Modified,
        ChangeKind::Deleted => pb::ChangeKind::Deleted,
    }) as i32
}

fn file_op_from_pb(op: pb::FileOp) -> Result<FileOp, Status> {
    let path = parse_rev_path(&op.path)?;
    match op.op {
        Some(pb::file_op::Op::Write(w)) => {
            let mode = mode_from_pb(w.mode)?;
            let content = match w.content {
                Some(pb::write_op::Content::Inline(data)) => {
                    WriteContent::Inline(Bytes::from(data))
                }
                Some(pb::write_op::Content::BlobId(id)) => WriteContent::Blob(parse_id(&id)?),
                None => {
                    return Err(Status::invalid_argument(format!(
                        "write to {path} has no content"
                    )));
                }
            };
            Ok(FileOp::Write { path, mode, content })
        }
        Some(pb::file_op::Op::Delete(_)) => Ok(FileOp::Delete { path }),
        None => Err(Status::invalid_argument(format!("op for {path} has no operation"))),
    }
}

fn commit_info_to_pb(e: query::LogEntry) -> pb::CommitInfo {
    pb::CommitInfo {
        id: e.id.0.to_vec(),
        tree: e.commit.tree.0.to_vec(),
        parents: e.commit.parents.iter().map(|p| p.0.to_vec()).collect(),
        generation: e.generation,
        author: Some((&e.commit.author).into()),
        committer: Some((&e.commit.committer).into()),
        message: e.commit.message,
    }
}

fn to_ids(ids: impl IntoIterator<Item = ObjectId>) -> Vec<Vec<u8>> {
    ids.into_iter().map(|i| i.0.to_vec()).collect()
}

/// Index-aligned read access to objects by id (all true in open mode).
async fn readable_ids(
    b: &Backend,
    access: Option<&Access>,
    ids: &[ObjectId],
) -> Result<Vec<bool>, StoreError> {
    match access {
        None => Ok(vec![true; ids.len()]),
        Some(a) => acl::readable(b, &a.policy, &a.principal, ids).await,
    }
}

// ---------------------------------------------------------------------------
// service
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl Ant for AntService {
    type GetObjectsStream = BoxStream<'static, Result<pb::ObjectResult, Status>>;

    async fn get_objects(
        &self,
        request: Request<pb::GetObjectsRequest>,
    ) -> Result<Response<Self::GetObjectsStream>, Status> {
        let access = self.access(&request)?;
        let ids = parse_ids(&request.into_inner().ids)?;
        let backend = self.backend.clone();
        let chunks: Vec<Vec<ObjectId>> =
            ids.chunks(OBJECT_CHUNK).map(<[ObjectId]>::to_vec).collect();
        let stream = futures::stream::iter(chunks)
            .then(move |chunk| {
                let backend = backend.clone();
                let access = access.clone();
                async move {
                    let allowed = match readable_ids(&backend, access.as_ref(), &chunk).await {
                        Ok(a) => a,
                        Err(e) => return futures::stream::iter(vec![Err(store_status(e))]),
                    };
                    let results: Vec<Result<pb::ObjectResult, Status>> =
                        match backend.objects.get_many(&chunk).await {
                            Ok(found) => chunk
                                .iter()
                                .zip(found)
                                .zip(allowed)
                                .map(|((id, obj), allowed)| {
                                    // Unreadable is indistinguishable from missing.
                                    Ok(match obj.filter(|_| allowed) {
                                        Some(o) => pb::ObjectResult {
                                            id: id.0.to_vec(),
                                            found: true,
                                            data: o.as_bytes().to_vec(),
                                        },
                                        None => pb::ObjectResult {
                                            id: id.0.to_vec(),
                                            found: false,
                                            data: Vec::new(),
                                        },
                                    })
                                })
                                .collect(),
                            Err(e) => vec![Err(store_status(e))],
                        };
                    futures::stream::iter(results)
                }
            })
            .flatten()
            .boxed();
        Ok(Response::new(stream))
    }

    async fn has_objects(
        &self,
        request: Request<pb::HasObjectsRequest>,
    ) -> Result<Response<pb::HasObjectsResponse>, Status> {
        let access = self.access(&request)?;
        let ids = parse_ids(&request.into_inner().ids)?;
        let mut present = self.backend.objects.has_many(&ids).await.map_err(store_status)?;
        let allowed =
            readable_ids(&self.backend, access.as_ref(), &ids).await.map_err(store_status)?;
        for (p, a) in present.iter_mut().zip(allowed) {
            *p &= a;
        }
        Ok(Response::new(pb::HasObjectsResponse { present }))
    }

    async fn put_objects(
        &self,
        request: Request<pb::PutObjectsRequest>,
    ) -> Result<Response<pb::PutObjectsResponse>, Status> {
        let access = self.access(&request)?;
        let data = request.into_inner().data;
        let mut objects = Vec::with_capacity(data.len());
        for d in data {
            let obj = EncodedObject::from_bytes(Bytes::from(d))
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            objects.push(obj);
        }
        if let Some(access) = &access {
            // Trees/commits could smuggle history past the path checks.
            if !access.is_admin() && objects.iter().any(|o| o.kind() != ObjectKind::Blob) {
                return Err(Status::permission_denied("only blobs may be uploaded"));
            }
            let kinds: Vec<_> = objects.iter().map(|o| (o.id(), o.kind())).collect();
            acl::record_uploads(&self.backend, access, &kinds).await.map_err(store_status)?;
        }
        let ids = to_ids(objects.iter().map(EncodedObject::id));
        self.backend.objects.put_many(&objects).await.map_err(store_status)?;
        Ok(Response::new(pb::PutObjectsResponse { ids }))
    }

    async fn get_ref(
        &self,
        request: Request<pb::GetRefRequest>,
    ) -> Result<Response<pb::GetRefResponse>, Status> {
        self.access(&request)?;
        let name = request.into_inner().name;
        ant_store::validate_ref_name(&name).map_err(store_status)?;
        let target = self.backend.refs.get_ref(&name).await.map_err(store_status)?;
        Ok(Response::new(pb::GetRefResponse { target: target.map(|t| t.0.to_vec()) }))
    }

    async fn list_refs(
        &self,
        request: Request<pb::ListRefsRequest>,
    ) -> Result<Response<pb::ListRefsResponse>, Status> {
        self.access(&request)?;
        let prefix = request.into_inner().prefix;
        let refs = self
            .backend
            .refs
            .list_refs(&prefix)
            .await
            .map_err(store_status)?
            .into_iter()
            .map(|(name, target)| pb::RefEntry { name, target: target.0.to_vec() })
            .collect();
        Ok(Response::new(pb::ListRefsResponse { refs }))
    }

    async fn update_ref(
        &self,
        request: Request<pb::UpdateRefRequest>,
    ) -> Result<Response<pb::UpdateRefResponse>, Status> {
        let access = self.access(&request)?;
        let req = request.into_inner();
        ant_store::validate_ref_name(&req.name).map_err(store_status)?;
        let expected = parse_opt_id(req.expected.as_ref())?;
        let new = parse_opt_id(req.new_target.as_ref())?;
        if let Some(access) = &access {
            acl::check_update_ref(&self.backend, access, &req.name, expected, new)
                .await
                .map_err(op_status)?;
        }
        let outcome =
            self.backend.refs.cas_ref(&req.name, expected, new).await.map_err(store_status)?;
        Ok(Response::new(match outcome {
            CasOutcome::Updated => {
                if new.is_some() {
                    history::schedule_catch_up(&self.backend, &req.name);
                }
                pb::UpdateRefResponse { updated: true, actual: None }
            }
            CasOutcome::Mismatch { actual } => {
                pb::UpdateRefResponse { updated: false, actual: actual.map(|a| a.0.to_vec()) }
            }
        }))
    }

    async fn create_changeset(
        &self,
        request: Request<pb::CreateChangesetRequest>,
    ) -> Result<Response<pb::CreateChangesetResponse>, Status> {
        let access = self.access(&request)?;
        let req = request.into_inner();
        let base = parse_opt_id(req.base.as_ref())?;
        let ops = req.ops.into_iter().map(file_op_from_pb).collect::<Result<Vec<_>, _>>()?;
        let author = req
            .author
            .map(Signature::from)
            .ok_or_else(|| Status::invalid_argument("author is required"))?;
        let committer = req.committer.map(Signature::from);
        let commit = changeset::create_changeset_as(
            &self.backend,
            access.as_ref(),
            ChangesetRequest { base, ops, author, committer, message: req.message },
        )
        .await
        .map_err(op_status)?;
        Ok(Response::new(pb::CreateChangesetResponse { commit: commit.0.to_vec() }))
    }

    async fn land(
        &self,
        request: Request<pb::LandRequest>,
    ) -> Result<Response<pb::LandResponse>, Status> {
        let access = self.access(&request)?;
        let req = request.into_inner();
        ant_store::validate_ref_name(&req.ref_name).map_err(store_status)?;
        let commit = parse_id(&req.commit)?;
        let outcome = land::land_as(&self.backend, access.as_ref(), &req.ref_name, commit)
            .await
            .map_err(op_status)?;
        if matches!(outcome, LandOutcome::Landed { .. }) {
            // Coalesced per ref, so a burst of lands in one leader batch (or
            // across batches) costs one background catch-up, not one each.
            history::schedule_catch_up(&self.backend, &req.ref_name);
        }
        let outcome = match outcome {
            LandOutcome::Landed { new_tip, rebased } => {
                pb::land_response::Outcome::Landed(pb::Landed {
                    new_tip: new_tip.0.to_vec(),
                    rebased,
                })
            }
            LandOutcome::Conflict { tip, paths } => {
                pb::land_response::Outcome::Conflict(pb::Conflict {
                    tip: tip.0.to_vec(),
                    paths: paths.iter().map(|p| p.as_str().to_owned()).collect(),
                })
            }
        };
        Ok(Response::new(pb::LandResponse { outcome: Some(outcome) }))
    }

    async fn resolve_rev(
        &self,
        request: Request<pb::ResolveRevRequest>,
    ) -> Result<Response<pb::ResolveRevResponse>, Status> {
        self.access(&request)?;
        let rev = request.into_inner().rev;
        let commit = query::resolve_rev(&self.backend, &rev).await.map_err(op_status)?;
        Ok(Response::new(pb::ResolveRevResponse { commit: commit.0.to_vec() }))
    }

    async fn read_path(
        &self,
        request: Request<pb::ReadPathRequest>,
    ) -> Result<Response<pb::ReadPathResponse>, Status> {
        let access = self.access(&request)?;
        let req = request.into_inner();
        let path = parse_rev_path(&req.path)?;
        let info = query::read_path_as(
            &self.backend,
            access.as_ref(),
            &req.rev,
            &path,
            req.include_content,
        )
        .await
        .map_err(op_status)?;
        Ok(Response::new(match info {
            None => pb::ReadPathResponse::default(),
            Some(info) => pb::ReadPathResponse {
                found: true,
                mode: mode_to_pb(info.mode),
                id: info.id.0.to_vec(),
                entries: info
                    .entries
                    .unwrap_or_default()
                    .into_iter()
                    .map(|e| pb::TreeEntry {
                        mode: mode_to_pb(e.mode),
                        name: e.name,
                        id: e.id.0.to_vec(),
                    })
                    .collect(),
                content: info.content.map(|c| c.to_vec()).unwrap_or_default(),
            },
        }))
    }

    async fn log(
        &self,
        request: Request<pb::LogRequest>,
    ) -> Result<Response<pb::LogResponse>, Status> {
        let access = self.access(&request)?;
        let req = request.into_inner();
        let path = req.path.as_deref().map(parse_rev_path).transpose()?;
        let entries = query::log_as(
            &self.backend,
            access.as_ref(),
            &req.rev,
            path.as_ref(),
            req.limit as usize,
        )
        .await
        .map_err(op_status)?;
        Ok(Response::new(pb::LogResponse {
            commits: entries.into_iter().map(commit_info_to_pb).collect(),
        }))
    }

    async fn blame(
        &self,
        request: Request<pb::BlameRequest>,
    ) -> Result<Response<pb::BlameResponse>, Status> {
        let access = self.access(&request)?;
        let req = request.into_inner();
        let path = parse_rev_path(&req.path)?;
        let lines = history::blame_as(&self.backend, access.as_ref(), &req.rev, &path)
            .await
            .map_err(op_status)?;
        Ok(Response::new(pb::BlameResponse {
            lines: lines
                .into_iter()
                .map(|l| pb::BlameLine { commit: l.commit.0.to_vec(), line: l.line })
                .collect(),
        }))
    }

    async fn diff(
        &self,
        request: Request<pb::DiffRequest>,
    ) -> Result<Response<pb::DiffResponse>, Status> {
        let access = self.access(&request)?;
        let req = request.into_inner();
        let from = if req.from_rev.is_empty() { None } else { Some(req.from_rev.as_str()) };
        let changes = query::diff_as(&self.backend, access.as_ref(), from, &req.to_rev)
            .await
            .map_err(op_status)?;
        let changes = changes
            .into_iter()
            .map(|c| {
                let (old_mode, old_id) = match c.old {
                    Some((m, id)) => (mode_to_pb(m), id.0.to_vec()),
                    None => (pb::EntryMode::Unspecified as i32, Vec::new()),
                };
                let (new_mode, new_id) = match c.new {
                    Some((m, id)) => (mode_to_pb(m), id.0.to_vec()),
                    None => (pb::EntryMode::Unspecified as i32, Vec::new()),
                };
                pb::PathChange {
                    path: c.path.as_str().to_owned(),
                    kind: kind_to_pb(c.kind),
                    old_mode,
                    old_id,
                    new_mode,
                    new_id,
                }
            })
            .collect();
        Ok(Response::new(pb::DiffResponse { changes }))
    }
}
