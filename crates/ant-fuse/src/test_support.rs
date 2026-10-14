//! Helpers shared by the crate's unit tests.

pub(crate) use ant_store::testing::CountingStore;

/// The multi-thread runtime the mounts and blocking state operations need.
pub(crate) fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
}
