//! End-to-end test of `ant ref watch` against an in-memory `ant serve` (the
//! contract is the doc comment in `src/main.rs`).

mod common;

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use ant_proto::v1 as pb;
use common::{KillOnDrop, ant, spawn_memory_server, wait_until};

fn write(path: &str, content: &[u8]) -> pb::FileOp {
    pb::FileOp {
        path: path.into(),
        op: Some(pb::file_op::Op::Write(pb::WriteOp {
            mode: pb::EntryMode::File as i32,
            content: Some(pb::write_op::Content::Inline(content.to_vec())),
        })),
    }
}

fn hex(id: &[u8]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect()
}

/// Commits made and landed over gRPC.
struct Grpc {
    rt: tokio::runtime::Runtime,
    client: pb::ant_client::AntClient<tonic::transport::Channel>,
}

impl Grpc {
    fn connect(grpc: u16) -> Self {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let client = rt
            .block_on(pb::ant_client::AntClient::connect(format!("http://127.0.0.1:{grpc}")))
            .expect("connect");
        Self { rt, client }
    }

    fn commit(&mut self, base: Option<&str>, path: &str) -> String {
        let sig = pb::Signature {
            name: "s".into(),
            email: "s@x".into(),
            time: 1_700_000_000,
            tz_offset_minutes: 0,
        };
        let req = pb::CreateChangesetRequest {
            base: base.map(unhex),
            ops: vec![write(path, b"x\n")],
            author: Some(sig),
            committer: None,
            message: format!("{path}\n"),
        };
        let r = self.rt.block_on(self.client.create_changeset(req)).expect("changeset");
        hex(&r.into_inner().commit)
    }

    /// Land `commit` on `main`; returns the new tip.
    fn land(&mut self, commit: &str) -> String {
        let req = pb::LandRequest { ref_name: "main".into(), commit: unhex(commit) };
        let r = self.rt.block_on(self.client.land(req)).expect("land").into_inner();
        match r.outcome {
            Some(pb::land_response::Outcome::Landed(l)) => hex(&l.new_tip),
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn ref_watch_prints_one_line_per_event_until_sigint() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = tmp.path().join("cache");
    let (grpc, _server) = spawn_memory_server();
    wait_until(|| ant(grpc, &cache).args(["ref", "list"]).output().unwrap().status.success());
    let mut g = Grpc::connect(grpc);
    let c0 = g.commit(None, "a");
    let c0 = g.land(&c0);
    let c1 = g.commit(Some(&c0), "b");
    let t1 = g.land(&c1);
    let c2 = g.commit(Some(&t1), "c");
    let c3 = g.commit(Some(&c2), "d");

    let mut child = KillOnDrop(
        ant(grpc, &cache)
            .args(["ref", "watch", "main", "--from", &c0])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawning ant ref watch"),
    );
    let stdout = child.0.stdout.take().expect("stdout");
    let (tx, lines) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if tx.send(line.expect("read line")).is_err() {
                return;
            }
        }
    });
    let next = || lines.recv_timeout(Duration::from_secs(20)).expect("no event line in time");

    // The catch-up, then live events as they happen.
    assert_eq!(next(), format!("update {c0}..{t1} 1 landed"));
    let t3 = g.land(&c3); // a stack of two commits, fast-forwarded
    assert_eq!(t3, c3);
    assert_eq!(next(), format!("land {t1}..{t3} 2 landed"));
    let out = ant(grpc, &cache).args(["ref", "set", "main", &c0, "--force"]).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(next(), format!("force {t3}..{c0} 0 landed"));

    // SIGINT: a clean exit, nothing more printed.
    let pid = child.0.id().to_string();
    assert!(Command::new("kill").args(["-INT", &pid]).status().unwrap().success());
    let status = child.0.wait().expect("wait");
    assert_eq!(status.code(), Some(0));
    assert!(lines.recv_timeout(Duration::from_secs(5)).is_err(), "no output after SIGINT");

    // Errors: an unknown --from rev, an invalid ref name.
    let out = ant(grpc, &cache).args(["ref", "watch", "main", "--from", "nope"]).output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).starts_with("error: "));
    let out = ant(grpc, &cache).args(["ref", "watch", "bad name"]).output().unwrap();
    assert_eq!(out.status.code(), Some(1));
}
