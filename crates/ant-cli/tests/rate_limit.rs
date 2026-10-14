//! `ant` against a server that rate-limits (`ant serve --rate-*`): commands
//! are retried after the server's hint and succeed instead of failing.

mod common;

use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::{KillOnDrop, ant, free_ports, wait_until};

fn spawn_limited_server(extra: &[&str]) -> (u16, KillOnDrop) {
    let (grpc, http) = free_ports();
    let server = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_ant"))
            .args(["serve", "--memory", "--grpc-listen"])
            .arg(format!("127.0.0.1:{grpc}"))
            .arg("--http-listen")
            .arg(format!("127.0.0.1:{http}"))
            .args(extra)
            .env_remove("ANT_POLICY")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawning ant serve"),
    );
    // Connecting does not spend a token (only RPCs do).
    wait_until(|| TcpStream::connect(("127.0.0.1", grpc)).is_ok());
    (grpc, server)
}

#[test]
fn commands_retry_rate_limited_requests_and_succeed() {
    // One read and one write token every 250 ms.
    let (grpc, _server) = spawn_limited_server(&["--rate-read", "4:1", "--rate-write", "4:1"]);
    let cache = tempfile::tempdir().unwrap();

    // `ref init` writes (CreateChangeset, Land); back-to-back writes from
    // separate processes share the per-IP bucket.
    let started = Instant::now();
    let r = ant(grpc, cache.path()).args(["ref", "init", "main"]).output().unwrap();
    assert!(r.status.success(), "{}", String::from_utf8_lossy(&r.stderr));

    // Reads: the process that finds the bucket empty waits and succeeds.
    for _ in 0..4 {
        let r = ant(grpc, cache.path()).args(["ref", "list"]).output().unwrap();
        assert!(r.status.success(), "{}", String::from_utf8_lossy(&r.stderr));
        assert!(String::from_utf8_lossy(&r.stdout).contains("main"));
    }
    let took = started.elapsed();
    // Four reads and two writes through 4/s buckets need at least ~0.5 s.
    assert!(took >= Duration::from_millis(500), "{took:?}");
    assert!(took < Duration::from_secs(30), "{took:?}");
}
