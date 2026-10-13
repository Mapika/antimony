//! gRPC adapter over `ops`.
//!
//! TASK (transport): implement `AntService` (the generated
//! `ant_proto::v1::ant_server::Ant` trait) and `serve`.
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
//! - Set max decoding/encoding message size to `ant_proto::MAX_MESSAGE_BYTES`
//!   on the generated server.

use std::net::SocketAddr;

use ant_store::Backend;

#[derive(Clone)]
pub struct AntService {
    pub backend: Backend,
}

impl AntService {
    pub fn new(backend: Backend) -> Self {
        Self { backend }
    }
}

/// Serve gRPC on `addr` until the future is dropped.
pub async fn serve(backend: Backend, addr: SocketAddr) -> anyhow::Result<()> {
    let _ = (backend, addr);
    todo!("transport")
}

/// Serve gRPC on an already-bound listener (tests bind port 0).
pub async fn serve_on(backend: Backend, listener: tokio::net::TcpListener) -> anyhow::Result<()> {
    let _ = (backend, listener);
    todo!("transport")
}
