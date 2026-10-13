//! Transport-independent server operations. The gRPC/HTTP layers are thin
//! adapters over these functions; tests exercise them directly against
//! `Backend::in_memory()`.

pub mod changeset;
pub mod history;
pub mod land;
pub mod query;

use ant_core::ObjectId;
use ant_store::StoreError;

#[derive(Debug, thiserror::Error)]
pub enum OpError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// Retryable contention (e.g. CAS kept losing).
    #[error("aborted: {0}")]
    Aborted(String),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Core(#[from] ant_core::Error),
}

impl OpError {
    pub fn not_found_object(id: ObjectId) -> Self {
        OpError::NotFound(format!("object {id}"))
    }
}

pub type OpResult<T> = std::result::Result<T, OpError>;
