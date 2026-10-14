//! Helpers shared by the CLI end-to-end tests. Each test binary uses a subset.
#![allow(dead_code)]

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

/// `<pid>_<nanos>`: a suffix for schema names and S3 prefixes unique to
/// this run.
pub fn unique_suffix() -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos();
    format!("{}_{nanos}", std::process::id())
}

/// An `ant` command for the server on `grpc`, with the object cache at
/// `cache`, a fixed author, no token, no HTTP endpoint and no stdin.
pub fn ant(grpc: u16, cache: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ant"));
    cmd.env("ANT_SERVER", format!("http://127.0.0.1:{grpc}"))
        .env("ANT_CACHE", cache)
        .env("ANT_AUTHOR_NAME", "Agent")
        .env("ANT_AUTHOR_EMAIL", "agent@example.com")
        .env_remove("ANT_TOKEN")
        .env_remove("ANT_HTTP")
        .stdin(Stdio::null());
    cmd
}

/// Start an empty in-memory, open-mode `ant serve`; returns its gRPC port.
/// Wait for it with [`wait_until`].
pub fn spawn_memory_server() -> (u16, KillOnDrop) {
    let (grpc, http) = free_ports();
    let server = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_ant"))
            .args(["serve", "--memory", "--grpc-listen"])
            .arg(format!("127.0.0.1:{grpc}"))
            .arg("--http-listen")
            .arg(format!("127.0.0.1:{http}"))
            .env_remove("ANT_POLICY")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawning ant serve"),
    );
    (grpc, server)
}

/// Poll `ready` every 100 ms; panics with "server not ready" after 20 s.
pub fn wait_until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready() {
        assert!(Instant::now() < deadline, "server not ready");
        std::thread::sleep(Duration::from_millis(100));
    }
}
