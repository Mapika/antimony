//! antimony server: transport-independent operations (`ops`) plus gRPC
//! (`rpc`) and plain-HTTP immutable object (`http`) front ends.

pub mod acl;
pub mod config;
pub mod http;
pub mod metrics;
pub mod observe;
pub mod ops;
pub mod rpc;
pub mod serve;
