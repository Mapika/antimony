//! End-to-end tests of the cache tier commands against an in-memory `ant
//! serve`: `ant cache-serve`, `ant cache seed`, `ant cache stats --tiers` and
//! checkouts by several processes sharing one cache directory.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use common::{KillOnDrop, ant, free_ports, wait_until};

const FILES: usize = 300;

struct Node {
    grpc: u16,
    http: u16,
    _server: KillOnDrop,
    tmp: tempfile::TempDir,
}

fn start() -> Node {
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
    let tmp = tempfile::tempdir().expect("tempdir");
    let probe = tmp.path().join("probe-cache");
    wait_until(|| {
        ant(grpc, &probe).args(["ref", "list"]).output().is_ok_and(|o| o.status.success())
    });
    seed(grpc);
    Node { grpc, http, _server: server, tmp }
}

fn seed(grpc: u16) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        use ant_proto::v1 as pb;
        let mut c = pb::ant_client::AntClient::connect(format!("http://127.0.0.1:{grpc}"))
            .await
            .expect("connect");
        let ops = (0..FILES)
            .map(|i| pb::FileOp {
                path: format!("d{}/s{}/f{i}.txt", i % 11, i % 4),
                op: Some(pb::file_op::Op::Write(pb::WriteOp {
                    mode: pb::EntryMode::File as i32,
                    content: Some(pb::write_op::Content::Inline(
                        format!("file {i}\n").repeat(i % 25 + 1).into_bytes(),
                    )),
                })),
            })
            .collect();
        let sig = pb::Signature {
            name: "s".into(),
            email: "s@x".into(),
            time: 1_700_000_000,
            tz_offset_minutes: 0,
        };
        let commit = c
            .create_changeset(pb::CreateChangesetRequest {
                base: None,
                ops,
                author: Some(sig),
                committer: None,
                message: "seed\n".into(),
            })
            .await
            .expect("changeset")
            .into_inner()
            .commit;
        c.land(pb::LandRequest { ref_name: "main".into(), commit }).await.expect("land");
    });
}

impl Node {
    fn cache(&self, name: &str) -> PathBuf {
        self.tmp.path().join(name)
    }

    /// `ant` with its own cache, reading objects over `http`.
    fn ant(&self, cache: &Path, http: &str) -> Command {
        let mut cmd = ant(self.grpc, cache);
        cmd.env("ANT_HTTP", http);
        cmd
    }

    fn origin_http(&self) -> String {
        format!("http://127.0.0.1:{}", self.http)
    }
}

fn ok(out: Output) -> String {
    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf8")
}

/// `key value` lines to a lookup.
fn kv(text: &str) -> std::collections::HashMap<String, u64> {
    text.lines()
        .map(|l| {
            let (k, v) = l.split_once(' ').expect("`key value`");
            (k.to_string(), v.parse().expect("number"))
        })
        .collect()
}

fn files_under(dir: &Path) -> usize {
    fn walk(dir: &Path, n: &mut usize) {
        for e in std::fs::read_dir(dir).expect("read_dir") {
            let e = e.expect("entry");
            if e.file_name() == ".ant" {
                continue;
            }
            if e.file_type().expect("type").is_dir() {
                walk(&e.path(), n);
            } else {
                *n += 1;
            }
        }
    }
    let mut n = 0;
    walk(dir, &mut n);
    n
}

#[test]
fn seed_fills_the_cache_so_a_checkout_fetches_nothing() {
    let node = start();
    let cache = node.cache("seeded");
    let out =
        ok(node.ant(&cache, &node.origin_http()).args(["cache", "seed", "main"]).output().unwrap());
    let seeded =
        kv(out.lines().filter(|l| !l.starts_with("tree ")).collect::<Vec<_>>().join("\n").as_str());
    assert!(out.starts_with("tree "), "{out}");
    assert_eq!(seeded["files"], FILES as u64);
    assert_eq!(seeded["blobs"] as usize, FILES - duplicates());
    assert!(seeded["bundled"] > 0, "seeding used tree bundles: {seeded:?}");

    let stats = kv(&ok(node
        .ant(&cache, &node.origin_http())
        .args(["cache", "stats", "--tiers"])
        .output()
        .unwrap()));
    let fetched_before = stats["bundled_objects"] + stats["fetched_objects"];
    assert!(stats["objects"] >= FILES as u64 && fetched_before > 0);

    // The checkout is served by the cache alone: even with the server's HTTP
    // port closed, no bundle or object request is made.
    let dir = node.tmp.path().join("ws");
    ok(node
        .ant(&cache, "http://127.0.0.1:1")
        .args(["ws", "new", dir.to_str().unwrap()])
        .output()
        .unwrap());
    assert_eq!(files_under(&dir), FILES);
    let after = kv(&ok(node
        .ant(&cache, &node.origin_http())
        .args(["cache", "stats", "--tiers"])
        .output()
        .unwrap()));
    assert_eq!(after["bundled_objects"], stats["bundled_objects"], "{after:?}");
    assert_eq!(after["fetched_objects"], stats["fetched_objects"], "{after:?}");
    assert!(after["local_hits"] > stats["local_hits"]);

    // A sparse seed covers only its patterns; a bad path is refused.
    let cache2 = node.cache("sparse");
    let out = ok(node
        .ant(&cache2, &node.origin_http())
        .args(["cache", "seed", "main", "--sparse", "d3"])
        .output()
        .unwrap());
    assert!(
        kv(out.lines().skip(1).collect::<Vec<_>>().join("\n").as_str())["files"] < FILES as u64 / 5
    );
    let bad = node
        .ant(&cache2, &node.origin_http())
        .args(["cache", "seed", "main:nope"])
        .output()
        .unwrap();
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).starts_with("error: "));
}

/// Distinct contents are fewer than files only when contents repeat.
fn duplicates() -> usize {
    let mut seen = std::collections::HashSet::new();
    (0..FILES).filter(|i| !seen.insert(format!("file {i}\n").repeat(i % 25 + 1))).count()
}

#[test]
fn cache_serve_command_serves_checkouts_and_warm_ones_skip_the_origin() {
    let node = start();
    // Needs a cache directory of its own.
    let no_cache = Command::new(env!("CARGO_BIN_EXE_ant"))
        .args(["cache-serve", "--upstream", &node.origin_http()])
        .env_remove("ANT_CACHE")
        .output()
        .unwrap();
    assert!(!no_cache.status.success());
    assert!(String::from_utf8_lossy(&no_cache.stderr).contains("--cache"));

    let (port, _) = free_ports();
    let tier = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_ant"))
            .args(["cache-serve", "--auth", "open", "--listen"])
            .arg(format!("127.0.0.1:{port}"))
            .arg("--upstream")
            .arg(node.origin_http())
            .env("ANT_CACHE", node.cache("tier"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let url = format!("http://127.0.0.1:{port}");
    wait_until(|| reqwest_get_ok(port, "/healthz"));

    let upstream_requests = || -> u64 {
        let text = http_text(port, "/metrics");
        text.lines()
            .filter(|l| l.starts_with("ant_cacheserve_upstream_requests_total{"))
            .map(|l| l.rsplit(' ').next().unwrap().parse::<u64>().unwrap())
            .sum()
    };
    let one = node.cache("client1");
    let d1 = node.tmp.path().join("ws1");
    ok(node.ant(&one, &url).args(["ws", "new", d1.to_str().unwrap()]).output().unwrap());
    assert_eq!(files_under(&d1), FILES);
    let cold = upstream_requests();
    assert!(cold > 0);

    let two = node.cache("client2");
    let d2 = node.tmp.path().join("ws2");
    ok(node.ant(&two, &url).args(["ws", "new", d2.to_str().unwrap()]).output().unwrap());
    assert_eq!(files_under(&d2), FILES);
    assert_eq!(upstream_requests(), cold, "a second cold client costs the origin nothing");
    drop(tier);
}

fn http_text(port: u16, path: &str) -> String {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    write!(s, "GET {path} HTTP/1.0\r\nhost: x\r\n\r\n").expect("send");
    let mut out = String::new();
    s.read_to_string(&mut out).expect("read");
    out
}

fn reqwest_get_ok(port: u16, path: &str) -> bool {
    use std::io::{Read, Write};
    let Ok(mut s) = std::net::TcpStream::connect(("127.0.0.1", port)) else { return false };
    if write!(s, "GET {path} HTTP/1.0\r\nhost: x\r\n\r\n").is_err() {
        return false;
    }
    let mut out = String::new();
    s.read_to_string(&mut out).is_ok() && out.starts_with("HTTP/1.0 200")
}

/// Several cold processes on one node, one shared cache: every object is
/// fetched once (bundles and objects together never exceed the distinct
/// objects by more than a few racy duplicates).
#[test]
fn concurrent_processes_share_fetches() {
    let node = start();
    let cache = node.cache("shared");
    let procs: Vec<_> = (0..4)
        .map(|i| {
            let dir = node.tmp.path().join(format!("p{i}"));
            node.ant(&cache, &node.origin_http())
                .args(["ws", "new", dir.to_str().unwrap()])
                .stdout(Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();
    for mut p in procs {
        assert!(p.wait().unwrap().success());
    }
    for i in 0..4 {
        assert_eq!(files_under(&node.tmp.path().join(format!("p{i}"))), FILES);
    }
    let stats = kv(&ok(node
        .ant(&cache, &node.origin_http())
        .args(["cache", "stats", "--tiers"])
        .output()
        .unwrap()));
    let received = stats["bundled_objects"] + stats["fetched_objects"];
    assert!(
        received <= stats["objects"] * 3 / 2,
        "4 processes received {received} objects for {} distinct: {stats:?}",
        stats["objects"]
    );
    assert!(stats["waited_hits"] > 0 || stats["local_hits"] > 0);
}
