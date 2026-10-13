//! antimony server: transport-independent operations (`ops`) plus gRPC
//! (`rpc`) and plain-HTTP immutable object (`http`) front ends.

pub mod http;
pub mod ops;
pub mod rpc;
