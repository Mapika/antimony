//! Client-side engine:
//!
//! - [`disk`]: a local content-addressed object cache shared by all agents on
//!   a machine. It is safe for concurrent use by many processes without a
//!   coordinating daemon: objects are immutable and written via
//!   write-temp-then-rename. Its size budget is enforced automatically by
//!   LRU eviction (counter file + eviction lock; see the module docs).
//! - [`readthrough`]: the cache in front of the server, with batched,
//!   de-duplicated and hash-verified fetches.
//! - [`remote`]: the server as an `ObjectStore` (gRPC, optionally HTTP reads).
//! - [`workspace`]: sparse working copies (real files on disk); [`files`]:
//!   file helpers shared with lazy workspaces.
//! - [`update`]: planning a workspace's move to a new base (3-way rules).
//! - [`walk`]: batched tree walks restricted to a few branches.

pub mod claim;
pub mod disk;
pub mod files;
pub mod ignore;
mod pack;
mod par;
pub mod readthrough;
pub mod remote;
pub mod retry;
pub mod tiers;
pub mod update;
pub mod walk;
pub mod workspace;

#[cfg(test)]
mod test_support;
