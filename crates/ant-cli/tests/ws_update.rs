//! End-to-end tests of `ant ws update` / `ant ws resolve` and of `ant ref
//! set --force`, against an in-memory `ant serve` (the contract is the doc
//! comment in `src/main.rs`). The sparse variant runs in the fast tier; the
//! lazy one needs FUSE.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;

use common::{KillOnDrop, ant, spawn_memory_server, wait_until};

const A: &str = "a1\na2\na3\na4\na5\n";
const B: &str = "b1\nb2\nb3\n";

struct Env {
    grpc: u16,
    cache: PathBuf,
}

struct Run {
    args: String,
    out: Output,
}

impl Run {
    fn code(&self) -> Option<i32> {
        self.out.status.code()
    }
    fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.out.stdout).into_owned()
    }
    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.out.stderr).into_owned()
    }
    fn expect(self, code: i32) -> Self {
        assert_eq!(
            self.code(),
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
    fn run(&self, dir: Option<&Path>, args: &[&str]) -> Run {
        let mut cmd = ant(self.grpc, &self.cache);
        cmd.args(args);
        if let Some(d) = dir {
            cmd.current_dir(d);
        }
        Run { args: args.join(" "), out: cmd.output().expect("spawning ant") }
    }

    fn tip(&self, name: &str) -> String {
        self.run(None, &["ref", "get", name]).ok().trim_end().to_string()
    }

    fn cat(&self, target: &str) -> String {
        self.run(None, &["cat", target]).ok()
    }

    /// Edit, submit and land from `dir`; returns the new tip.
    fn land(&self, dir: &Path, edits: &[(&str, Option<&str>)], msg: &str) -> String {
        for (path, content) in edits {
            let p = dir.join(path);
            match content {
                Some(c) => {
                    fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
                    fs::write(p, c).expect("write");
                }
                None => fs::remove_file(p).expect("remove"),
            }
        }
        self.run(Some(dir), &["submit", "-m", msg]).ok();
        let out = self.run(Some(dir), &["land"]).ok();
        assert!(out.starts_with("landed "), "{out}");
        self.tip("main")
    }
}

fn start() -> (tempfile::TempDir, Env, KillOnDrop) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (grpc, server) = spawn_memory_server();
    let env = Env { grpc, cache: tmp.path().join("cache") };
    wait_until(|| env.run(None, &["ref", "list"]).code() == Some(0));
    seed(grpc);
    (tmp, env, server)
}

/// The first commit of `main`.
fn seed(grpc: u16) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        use ant_proto::v1 as pb;
        let mut c = pb::ant_client::AntClient::connect(format!("http://127.0.0.1:{grpc}"))
            .await
            .expect("connect");
        let write = |path: &str, content: &str| pb::FileOp {
            path: path.into(),
            op: Some(pb::file_op::Op::Write(pb::WriteOp {
                mode: pb::EntryMode::File as i32,
                content: Some(pb::write_op::Content::Inline(content.as_bytes().to_vec())),
            })),
        };
        let sig = pb::Signature {
            name: "s".into(),
            email: "s@x".into(),
            time: 1_700_000_000,
            tz_offset_minutes: 0,
        };
        let commit = c
            .create_changeset(pb::CreateChangesetRequest {
                base: None,
                ops: vec![
                    write("a.txt", A),
                    write("b.txt", B),
                    write("up.txt", "u\n"),
                    write("mine.txt", "m\n"),
                    write("other/keep.txt", "k\n"),
                ],
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

struct Mounts<'a> {
    env: &'a Env,
    dirs: Vec<PathBuf>,
}
impl Drop for Mounts<'_> {
    fn drop(&mut self) {
        for dir in &self.dirs {
            let _ = self.env.run(None, &["ws", "unmount", dir.to_str().expect("utf8")]);
        }
    }
}

fn lines(s: &str) -> Vec<&str> {
    s.lines().collect()
}

#[test]
fn ws_update_sparse() {
    run_update(false);
}

#[test]
#[ignore = "needs FUSE (/dev/fuse, fusermount3)"]
fn ws_update_lazy() {
    run_update(true);
}

fn run_update(lazy: bool) {
    let (tmp, env, _server) = start();
    let root = tmp.path();
    let mut mounts = Mounts { env: &env, dirs: Vec::new() };
    let mut new_ws = |name: &str, at: &str| -> PathBuf {
        let dir = root.join(name);
        let d = dir.to_str().expect("utf8");
        let mut args = vec!["ws", "new", d, "--at", at];
        if lazy {
            args.push("--lazy");
            mounts.dirs.push(dir.clone());
        } else {
            // Everything but `other/`.
            args.extend(["--sparse", "*.txt"]);
        }
        env.run(None, &args).ok();
        dir
    };
    let tip0 = env.tip("main");
    let upstream = new_ws("upstream", "main");
    let ws = new_ws("ws", "main");
    let w = Some(ws.as_path());

    // Nothing to do.
    assert_eq!(env.run(w, &["ws", "update"]).ok(), format!("updated {tip0}\n"));
    assert_eq!(env.run(w, &["status"]).ok(), "");

    // Upstream-only, local-only and both-sides-mergeable edits.
    let tip1 = env.land(
        &upstream,
        &[
            ("up.txt", Some("u2\n")),
            ("a.txt", Some("A1\na2\na3\na4\na5\n")),
            ("other/keep.txt", Some("k2\n")),
            ("other/new.txt", Some("n\n")),
        ],
        "upstream edits",
    );
    fs::write(ws.join("mine.txt"), "m2\n").expect("edit");
    fs::write(ws.join("a.txt"), "a1\na2\na3\na4\nA5\n").expect("edit");
    fs::write(ws.join("new.txt"), "local new\n").expect("add");
    assert_eq!(env.run(w, &["ws", "update"]).ok(), format!("updated {tip1}\n"));
    assert_eq!(fs::read_to_string(ws.join("up.txt")).expect("up"), "u2\n");
    assert_eq!(fs::read_to_string(ws.join("a.txt")).expect("a"), "A1\na2\na3\na4\nA5\n");
    assert_eq!(fs::read_to_string(ws.join("mine.txt")).expect("mine"), "m2\n");
    assert_eq!(fs::read_to_string(ws.join("new.txt")).expect("new"), "local new\n");
    if lazy {
        assert_eq!(fs::read_to_string(ws.join("other/new.txt")).expect("other"), "n\n");
    } else {
        // Outside the sparse patterns: not materialized.
        assert!(!ws.join("other").exists());
    }
    assert_eq!(lines(&env.run(w, &["status"]).ok()), ["M\ta.txt", "M\tmine.txt", "A\tnew.txt"]);

    // A genuine conflict: the land fails, the update marks the file.
    let tip2 = env.land(&upstream, &[("b.txt", Some("b1\nupstream\nb3\n"))], "upstream b");
    fs::write(ws.join("b.txt"), "b1\nlocal\nb3\n").expect("edit");
    env.run(w, &["submit", "-m", "local edits"]).ok();
    let r = env.run(w, &["land"]).expect(2);
    assert_eq!(lines(&r.stdout()), ["conflict", "b.txt"]);
    assert!(
        r.stderr().contains("hint: main moved under you; run `ant ws update"),
        "{}",
        r.stderr()
    );
    let r = env.run(w, &["ws", "update"]).expect(2);
    assert_eq!(r.stdout(), format!("updated {tip2}\nC\tb.txt\n"));
    assert!(r.stderr().contains("hint: ") && r.stderr().contains("ant ws resolve"));
    assert!(!ws.join(".ant/last_submit").exists(), "the stale submit is dropped");
    assert_eq!(
        fs::read_to_string(ws.join("b.txt")).expect("b"),
        "b1\n<<<<<<< upstream\nupstream\n||||||| base\nb2\n=======\nlocal\n>>>>>>> local\nb3\n"
    );
    assert_eq!(
        lines(&env.run(w, &["status"]).ok()),
        ["M\ta.txt", "C\tb.txt", "M\tmine.txt", "A\tnew.txt"]
    );
    // Blocked until resolved.
    let r = env.run(w, &["submit", "-m", "too early"]).expect(1);
    assert!(r.stderr().contains("error: workspace has unresolved conflicts: b.txt"));
    assert!(r.stderr().contains("hint: "), "{}", r.stderr());
    env.run(w, &["ws", "update"]).expect(1);
    let r = env.run(w, &["ws", "resolve", "b.txt"]).expect(1);
    assert!(r.stderr().contains("still contains conflict markers"), "{}", r.stderr());
    assert!(r.stderr().contains("hint: "));
    let r = env.run(w, &["ws", "resolve", "a.txt"]).expect(1);
    assert!(r.stderr().contains("a.txt is not conflicted"), "{}", r.stderr());
    fs::write(ws.join("b.txt"), "b1\nupstream and local\nb3\n").expect("fix");
    assert_eq!(env.run(w, &["ws", "resolve", "b.txt"]).ok(), "");
    assert_eq!(
        lines(&env.run(w, &["status"]).ok()),
        ["M\ta.txt", "M\tb.txt", "M\tmine.txt", "A\tnew.txt"]
    );
    let tip3 = env.land(&ws, &[], "local edits, resolved");
    assert_eq!(env.cat("main:b.txt"), "b1\nupstream and local\nb3\n");
    assert_eq!(env.cat("main:a.txt"), "A1\na2\na3\na4\nA5\n");
    assert_eq!(env.cat("main:up.txt"), "u2\n");
    assert_eq!(env.cat("main:new.txt"), "local new\n");
    assert_eq!(env.cat("main:other/new.txt"), "n\n");
    assert_eq!(env.run(w, &["status"]).ok(), "");

    // `--to` an older rev, and back.
    let old = new_ws("old", "main");
    let o = Some(old.as_path());
    let tip1_again = env.run(o, &["ws", "update", "--to", &tip1]).ok();
    assert_eq!(tip1_again, format!("updated {tip1}\n"));
    assert_eq!(fs::read_to_string(old.join("b.txt")).expect("b"), B);
    assert!(!old.join("new.txt").exists());
    assert_eq!(env.run(o, &["status"]).ok(), "");
    assert_eq!(env.run(o, &["ws", "update"]).ok(), format!("updated {tip3}\n"));
    assert_eq!(fs::read_to_string(old.join("new.txt")).expect("new"), "local new\n");

    // A workspace created at another ref follows that ref.
    env.run(None, &["ref", "set", "feature", &tip1, "--create"]).ok();
    let feature = new_ws("feature", "feature");
    let f = Some(feature.as_path());
    assert_eq!(env.run(f, &["ws", "update"]).ok(), format!("updated {tip1}\n"));
    // Fast-forward needs no --force.
    env.run(None, &["ref", "set", "feature", "main"]).ok();
    assert_eq!(env.run(f, &["ws", "update"]).ok(), format!("updated {tip3}\n"));

    // Rewinding a ref needs --force.
    let r = env.run(None, &["ref", "set", "feature", &tip0]).expect(1);
    assert!(r.stderr().contains("error: non-fast-forward update of ref feature"), "{}", r.stderr());
    assert!(r.stderr().contains("hint: pass --force"), "{}", r.stderr());
    assert_eq!(env.tip("feature"), tip3);
    let r = env.run(None, &["ref", "set", "feature", &tip1, "--expect", &tip3]).expect(1);
    assert!(r.stderr().contains("non-fast-forward"), "{}", r.stderr());
    assert_eq!(
        env.run(None, &["ref", "set", "feature", &tip0, "--force"]).ok(),
        format!("{tip0}\n")
    );
    assert_eq!(env.tip("feature"), tip0);
    drop(mounts);
}

#[test]
fn submit_survives_evicted_blobs_sparse() {
    submit_evicted(false);
}

#[test]
#[ignore = "needs FUSE (/dev/fuse, fusermount3)"]
fn submit_survives_evicted_blobs_lazy() {
    submit_evicted(true);
}

/// A one-byte cache budget evicts every blob `submit` has just staged, so the
/// upload must re-read the files from the workspace (working tree or upper
/// layer) instead of failing with "missing from cache".
fn submit_evicted(lazy: bool) {
    let (tmp, env, _server) = start();
    let mut mounts = Mounts { env: &env, dirs: Vec::new() };
    let ws = tmp.path().join("ws");
    let d = ws.to_str().expect("utf8");
    let mut args = vec!["ws", "new", d];
    if lazy {
        args.push("--lazy");
        mounts.dirs.push(ws.clone());
    }
    env.run(None, &args).ok();
    let w = Some(ws.as_path());
    fs::write(ws.join("a.txt"), "edited a\n").expect("edit");
    fs::write(ws.join("fresh.txt"), "fresh\n").expect("add");
    assert_eq!(lines(&env.run(w, &["status"]).ok()), ["M\ta.txt", "A\tfresh.txt"]);
    env.run(None, &["cache", "gc", "--to", "0"]).ok();
    let r = env.run(w, &["--cache-bytes", "1", "submit", "-m", "evicted"]).ok();
    let commit = r.trim_end();
    assert_eq!(env.cat(&format!("{commit}:a.txt")), "edited a\n");
    assert_eq!(env.cat(&format!("{commit}:fresh.txt")), "fresh\n");
    drop(mounts);
}
