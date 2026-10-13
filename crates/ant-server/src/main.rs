//! TASK (transport): `ant-server` binary.
//!
//! Flags (clap, each also readable from env `ANT_*`):
//!   --grpc-listen <addr>   default 127.0.0.1:7420
//!   --http-listen <addr>   default 127.0.0.1:7421
//!   --memory               in-memory backend (dev/testing)
//!   --pg-url <url>         Postgres for refs/meta/small objects
//!   --pg-schema <name>     default "ant"
//!   --s3-endpoint/--s3-bucket/--s3-prefix/--s3-access-key/--s3-secret-key/--s3-region
//!                          large objects; objects > 4 KiB go to S3 via
//!                          `TieredObjectStore`
//! Exactly one of --memory or --pg-url. Runs gRPC and HTTP concurrently;
//! logs with tracing-subscriber (RUST_LOG, default info).

fn main() {
    todo!("transport")
}
