//! antimony server: transport-independent operations (`ops`) plus gRPC
//! (`rpc`) and plain-HTTP immutable object (`http`) front ends.

pub mod acl;
pub mod bundle;
pub mod config;
pub mod health;
pub mod http;
pub mod limits;
pub mod metrics;
pub mod observe;
pub mod ops;
pub mod rpc;
pub mod serve;

/// Lock `mutex`, ignoring poisoning: every guarded value here stays
/// consistent across a panicking holder (caches, queues, registries).
pub(crate) fn lock<T: ?Sized>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}
