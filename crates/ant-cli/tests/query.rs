//! End-to-end tests of the history queries that need no checkout:
//! `ant merge-base`, `ant log <a>..<b>`, `ant diff --patch / --merge-base /
//! -- <paths>`, `ant landed-as` and the ancestry check of `ant ref set`,
//! against an in-memory `ant serve` (the contract is the doc comment in
//! `src/main.rs`).

mod common;

use std::path::PathBuf;
use std::process::Output;

use ant_proto::v1 as pb;
use common::{KillOnDrop, ant, spawn_memory_server, wait_until};

struct Env {
    grpc: u16,
    cache: PathBuf,
}

struct Run {
    args: String,
    out: Output,
}

impl Run {
    fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.out.stdout).into_owned()
    }
    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.out.stderr).into_owned()
    }
    fn expect(self, code: i32) -> Self {
        assert_eq!(
            self.out.status.code(),
            Some(code),
            "ant {}\n--- stdout ---\n{}\n--- stderr ---\n{}",
            self.args,
            self.stdout(),
            self.stderr()
        );
        self
    }
    fn ok(self) -> String {
        self.expect(0).stdout()
    }
}

impl Env {
    fn run(&self, args: &[&str]) -> Run {
        let mut cmd = ant(self.grpc, &self.cache);
        cmd.args(args);
        Run { args: args.join(" "), out: cmd.output().expect("spawning ant") }
    }
}

fn start() -> (tempfile::TempDir, Env, KillOnDrop) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (grpc, server) = spawn_memory_server();
    let env = Env { grpc, cache: tmp.path().join("cache") };
    wait_until(|| env.run(&["ref", "list"]).out.status.code() == Some(0));
    (tmp, env, server)
}

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

/// Commits made and landed over gRPC (the CLI under test only reads).
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

    fn commit(&mut self, base: Option<&str>, ops: Vec<pb::FileOp>, msg: &str) -> String {
        let sig = pb::Signature {
            name: "s".into(),
            email: "s@x".into(),
            time: 1_700_000_000,
            tz_offset_minutes: 0,
        };
        let req = pb::CreateChangesetRequest {
            base: base.map(unhex),
            ops,
            author: Some(sig),
            committer: None,
            message: format!("{msg}\n"),
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

fn line(id: &str, subject: &str) -> String {
    format!("{id} {subject}\n")
}

#[test]
fn history_queries_without_a_checkout() {
    let (tmp, env, _server) = start();
    let mut g = Grpc::connect(env.grpc);
    let c1 =
        g.commit(None, vec![write("a.txt", b"one\ntwo\nthree\n"), write("src/x", b"x\n")], "c1");
    g.land(&c1);
    let f1 = g.commit(Some(&c1), vec![write("src/f", b"f\n")], "f1");
    let f2 = g.commit(Some(&f1), vec![write("docs/d", b"d")], "f2");
    let c2 = g.commit(Some(&c1), vec![write("a.txt", b"one\n2\nthree\n")], "c2");
    g.land(&c2);
    let bin = g.commit(Some(&c2), vec![write("b.bin", b"x\0y")], "bin");
    let other = g.commit(None, vec![write("o", b"o\n")], "other");

    // merge-base
    assert_eq!(env.run(&["merge-base", "main", &f2]).ok(), format!("{c1}\n"));
    assert_eq!(env.run(&["merge-base", &c1, "main"]).ok(), format!("{c1}\n"));
    let r = env.run(&["merge-base", "main", &other]).expect(1);
    assert!(r.stderr().starts_with("error: ") && r.stderr().contains("no common ancestor"));
    let r = env.run(&["merge-base", "main", "nope"]).expect(1);
    assert!(r.stderr().contains("hint: "), "{}", r.stderr());

    // log ranges
    assert_eq!(env.run(&["log", &format!("{c1}..main")]).ok(), line(&c2, "c2"));
    let range = format!("main..{f2}");
    assert_eq!(env.run(&["log", &range]).ok(), line(&f2, "f2") + &line(&f1, "f1"));
    assert_eq!(env.run(&["log", &range, "-n", "1"]).ok(), line(&f2, "f2"));
    assert_eq!(env.run(&["log", &range, "--", "src"]).ok(), line(&f1, "f1"));
    assert_eq!(env.run(&["log", &range, "--path", "docs/"]).ok(), line(&f2, "f2"));
    assert_eq!(env.run(&["log", &format!("{f2}..main"), "--", "src"]).ok(), "");
    env.run(&["log", "main.."]).expect(1);
    env.run(&["log", &range, "--path", "a", "--", "b"]).expect(1);
    // Plain log is unchanged, with the trailing path form too.
    assert_eq!(env.run(&["log", "main", "--", "a.txt"]).ok(), line(&c2, "c2") + &line(&c1, "c1"));

    // diff --patch
    let patch = env.run(&["diff", "--patch", &c1, "main"]).ok();
    assert_eq!(
        patch,
        "diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -1,3 +1,3 @@\n one\n-two\n+2\n three\n"
    );
    assert_eq!(
        env.run(&["diff", "--patch", "-U", "0", &c1, "main"]).ok(),
        "diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -2 +2 @@\n-two\n+2\n"
    );
    assert_eq!(
        env.run(&["diff", "--patch", "--merge-base", "main", &f2]).ok(),
        "diff --git a/docs/d b/docs/d\nnew file mode 100644\n--- /dev/null\n+++ b/docs/d\n\
         @@ -0,0 +1 @@\n+d\n\\ No newline at end of file\n\
         diff --git a/src/f b/src/f\nnew file mode 100644\n--- /dev/null\n+++ b/src/f\n\
         @@ -0,0 +1 @@\n+f\n"
    );
    assert_eq!(env.run(&["diff", "--merge-base", "main", &f2, "--", "src"]).ok(), "A\tsrc/f\n");
    assert_eq!(env.run(&["diff", "main", &f2, "--", "src", "a.txt"]).ok(), "M\ta.txt\nA\tsrc/f\n");
    assert_eq!(
        env.run(&["diff", "--patch", "main", &bin]).ok(),
        "diff --git a/b.bin b/b.bin\nnew file mode 100644\nBinary files /dev/null and b/b.bin differ\n"
    );
    env.run(&["diff", "--merge-base", "main", &other]).expect(1);
    env.run(&["diff", "-U", "1", "main", &f2]).expect(2); // usage: -U needs --patch

    // The patch applies with git.
    if std::process::Command::new("git").arg("--version").output().is_ok() {
        let dir = tmp.path().join("apply");
        std::fs::create_dir_all(&dir).expect("mkdir");
        std::fs::write(dir.join("a.txt"), "one\ntwo\nthree\n").expect("write");
        std::fs::write(dir.join("patch"), &patch).expect("write");
        let out = std::process::Command::new("git")
            .args(["apply", "patch"])
            .current_dir(&dir)
            .output()
            .expect("git apply");
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        assert_eq!(std::fs::read_to_string(dir.join("a.txt")).expect("read"), "one\n2\nthree\n");
    }

    // landed-as
    env.run(&["landed-as", &f2]).expect(1);
    let landed = g.land(&f2);
    assert_eq!(env.run(&["landed-as", &f2]).ok(), format!("{landed} main rebased\n"));
    assert_eq!(env.run(&["landed-as", &c2, "--ref", "main"]).ok(), format!("{c2} main\n"));
    let r = env.run(&["landed-as", &bin, "--ref", "main"]).expect(1);
    assert!(r.stderr().contains("has not landed on main"), "{}", r.stderr());
    env.run(&["landed-as", &c2, "--ref", "nope"]).expect(1);

    // ref set checks ancestry on the server.
    env.run(&["ref", "set", "feature", &f2, "--create"]).ok();
    let r = env.run(&["ref", "set", "main", &c1]).expect(1);
    assert!(r.stderr().contains("hint: pass --force"), "{}", r.stderr());
    assert_eq!(env.run(&["ref", "set", "feature", &landed]).expect(1).stdout(), "");
    let ff = g.commit(Some(&landed), vec![write("z", b"z\n")], "ff");
    assert_eq!(env.run(&["ref", "set", "main", &ff]).ok(), format!("{ff}\n"));
}
