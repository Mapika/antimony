//! Helpers shared by the CLI end-to-end tests.

use std::net::TcpListener;
use std::process::Child;

/// Postgres from `docker compose up -d`.
pub const PG_URL: &str = "postgres://ant:ant@localhost:54329/ant";

/// A spawned process (e.g. `ant serve`) killed and reaped on drop.
pub struct KillOnDrop(pub Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Two currently free local ports (for a server's gRPC and HTTP listeners).
pub fn free_ports() -> (u16, u16) {
    let a = TcpListener::bind("127.0.0.1:0").expect("bind");
    let b = TcpListener::bind("127.0.0.1:0").expect("bind");
    let ports = (a.local_addr().expect("addr").port(), b.local_addr().expect("addr").port());
    drop(a);
    drop(b);
    ports
}
