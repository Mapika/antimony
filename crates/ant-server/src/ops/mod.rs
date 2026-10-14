//! Transport-independent server operations. The gRPC layer (`rpc`) is a thin
//! adapter over these functions (the HTTP object endpoint reads the store
//! directly); tests exercise them directly against `Backend::in_memory()`.

pub mod ancestry;
pub(crate) mod cache;
pub mod changeset;
pub mod history;
pub mod land;
pub mod patch;
pub mod query;
mod tree_edit;
pub mod watch;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod legacy_mode_tests;

pub use cache::{DEFAULT_MAX_AGE as CACHE_MAX_AGE, cache_max_age, set_cache_max_age};

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
    /// The authenticated caller lacks a required permission (`acl`).
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Core(#[from] ant_core::Error),
}

impl OpError {
    pub fn not_found_object(id: ObjectId) -> Self {
        OpError::NotFound(format!("object {id}"))
    }

    pub fn not_found_commit(id: ObjectId) -> Self {
        OpError::NotFound(format!("commit {id}"))
    }
}

pub type OpResult<T> = std::result::Result<T, OpError>;

#[cfg(test)]
mod gc_contract_tests {
    use ant_store::postgres::gc;

    /// Garbage collection keeps these derived kinds' ids alive and deletes
    /// these commit-keyed rows with their commits; it must know their names.
    #[test]
    fn gc_knows_the_server_derived_kinds() {
        assert!(gc::DEFAULT_ROOT_KINDS.contains(&super::land::LANDED_AS));
        assert!(gc::DEFAULT_COMMIT_KEYED_KINDS.contains(&super::history::BLAME_KIND));
        // Condemned data must outlive every cache entry before deletion.
        assert!(gc::DEFAULT_QUIESCE >= super::CACHE_MAX_AGE * 2);
    }
}
