//! End-to-end tests of ignore rules (`.gitignore` files, `.ant/ignore`) in
//! `ant status [--ignored]` and `ant submit`, against an in-memory `ant
//! serve` (the contract is the doc comment in `src/main.rs`). The sparse
//! variant runs in the fast tier; the lazy one needs FUSE.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;

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
    fn ok(self) -> String {
        assert_eq!(
            self.out.status.code(),
            Some(0),
            "ant {}\n--- stdout ---\n{}\n--- stderr ---\n{}",
            self.args,
            self.stdout(),
            self.stderr()
        );
        self.stdout()
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
}

fn start() -> (tempfile::TempDir, Env, KillOnDrop) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (grpc, server) = spawn_memory_server();
    let env = Env { grpc, cache: tmp.path().join("cache") };
    wait_until(|| env.run(None, &["ref", "list"]).out.status.success());
    seed(grpc);
    (tmp, env, server)
}

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
                    write(".gitignore", "target/\n*.log\n!keep.log\n"),
                    write("src/lib.rs", "lib\n"),
                    write("src/.gitignore", "gen_*\n"),
                    // Tracked although it matches `*.log`.
                    write("build.log", "tracked log\n"),
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

struct Mount<'a> {
    env: &'a Env,
    dir: Option<PathBuf>,
}
impl Drop for Mount<'_> {
    fn drop(&mut self) {
        if let Some(dir) = &self.dir {
            let _ = self.env.run(None, &["ws", "unmount", dir.to_str().expect("utf8")]);
        }
    }
}

fn put(dir: &Path, path: &str, body: &str) {
    let p = dir.join(path);
    fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
    fs::write(p, body).expect("write");
}

#[test]
fn ignore_rules_sparse() {
    run(false);
}

#[test]
#[ignore = "needs FUSE (/dev/fuse, fusermount3)"]
fn ignore_rules_lazy() {
    run(true);
}

fn run(lazy: bool) {
    let (tmp, env, _server) = start();
    let ws = tmp.path().join("ws");
    let d = ws.to_str().expect("utf8");
    let mut mount = Mount { env: &env, dir: None };
    if lazy {
        env.run(None, &["ws", "new", d, "--lazy"]).ok();
        mount.dir = Some(ws.clone());
    } else {
        env.run(None, &["ws", "new", d]).ok();
    }
    let w = Some(ws.as_path());
    assert_eq!(env.run(w, &["status"]).ok(), "");

    // Build-like output, an untracked file the rules do not cover, a
    // modified tracked file that matches a rule, and `.ant/ignore`.
    put(&ws, "target/debug/app", "binary\n");
    put(&ws, "target/debug/deps/x.o", "obj\n");
    put(&ws, "a.log", "log\n");
    put(&ws, "keep.log", "kept by negation\n");
    put(&ws, "src/gen_a.rs", "generated\n");
    put(&ws, "gen_b.rs", "not under src/\n");
    put(&ws, "new.txt", "new\n");
    put(&ws, "build.log", "tracked log, edited\n");
    put(&ws, ".ant/ignore", "scratch/\n");
    put(&ws, "scratch/notes", "n\n");

    assert_eq!(
        env.run(w, &["status"]).ok(),
        "M\tbuild.log\nA\tgen_b.rs\nA\tkeep.log\nA\tnew.txt\n"
    );
    assert_eq!(
        env.run(w, &["status", "--ignored"]).ok(),
        "!\ta.log\nM\tbuild.log\nA\tgen_b.rs\nA\tkeep.log\nA\tnew.txt\n!\tscratch/\n\
         !\tsrc/gen_a.rs\n!\ttarget/\n"
    );
    // Ignored files exist and are readable.
    assert_eq!(fs::read_to_string(ws.join("target/debug/app")).expect("read"), "binary\n");

    // Submit and land: only the reported paths.
    env.run(w, &["submit", "-m", "work"]).ok();
    assert!(env.run(w, &["land"]).ok().starts_with("landed "));
    let root = env.run(None, &["ls", "main"]).ok();
    let src = env.run(None, &["ls", "main:src"]).ok();
    for want in ["build.log", "gen_b.rs", "keep.log", "new.txt"] {
        assert!(root.contains(want), "{want} missing in\n{root}");
    }
    for unwanted in ["target", "a.log", "scratch"] {
        assert!(!root.contains(unwanted), "{unwanted} was submitted:\n{root}");
    }
    assert!(src.contains("lib.rs") && !src.contains("gen_a.rs"), "{src}");
    assert_eq!(env.run(None, &["cat", "main:build.log"]).ok(), "tracked log, edited\n");

    // After landing the workspace is clean and the ignored files remain.
    assert_eq!(env.run(w, &["status"]).ok(), "");
    assert_eq!(
        env.run(w, &["status", "--ignored"]).ok(),
        "!\ta.log\n!\tscratch/\n!\tsrc/gen_a.rs\n!\ttarget/\n"
    );
    assert!(ws.join("target/debug/deps/x.o").exists());

    // A file that is tracked-but-ignored is reported when deleted, too.
    fs::remove_file(ws.join("build.log")).expect("rm");
    assert_eq!(env.run(w, &["status"]).ok(), "D\tbuild.log\n");
}
