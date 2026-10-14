//! End-to-end tests of client robustness: seeding an empty server with
//! `ant ref init`, and recovering a lazy workspace whose FUSE daemon was
//! killed (`ant ws remount`; needs FUSE, so ignored in the fast tier).

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use common::{KillOnDrop, ant, spawn_memory_server, wait_until};

struct Env {
    grpc: u16,
    cache: PathBuf,
    _server: KillOnDrop,
    _tmp: tempfile::TempDir,
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}
fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

impl Env {
    fn run(&self, args: &[&str]) -> Output {
        ant(self.grpc, &self.cache).args(args).output().expect("spawning ant")
    }

    fn ok(&self, args: &[&str]) -> String {
        let o = self.run(args);
        assert!(o.status.success(), "ant {args:?}\n{}\n{}", stdout(&o), stderr(&o));
        stdout(&o)
    }
}

/// An empty in-memory server.
fn start() -> Env {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (grpc, server) = spawn_memory_server();
    let env = Env { grpc, cache: tmp.path().join("cache"), _server: server, _tmp: tmp };
    wait_until(|| env.run(&["ref", "list"]).status.success());
    env
}

fn lands(env: &Env, ws: &Path, file: &str, content: &str, msg: &str) {
    fs::write(ws.join(file), content).expect("write");
    let w = ws.to_str().expect("utf8");
    env.ok(&["-C", w, "submit", "-m", msg]);
    assert!(env.ok(&["-C", w, "land"]).starts_with("landed "));
}

#[test]
fn ref_init_seeds_an_empty_server() {
    let env = start();
    assert_eq!(env.ok(&["ref", "list"]), "");
    // Without a first commit there is nothing to base a workspace on.
    let o = env.run(&["ws", "new", env._tmp.path().join("nope").to_str().expect("utf8")]);
    assert_eq!(o.status.code(), Some(1));

    let id = env.ok(&["ref", "init"]);
    let id = id.trim();
    assert_eq!(id.len(), 64, "{id}");
    assert_eq!(env.ok(&["ref", "get", "main"]).trim(), id);
    assert_eq!(env.ok(&["ls", "main"]), "", "empty tree");
    assert_eq!(env.ok(&["log", "main"]), format!("{id} initial commit\n"));

    // The ref now exists: init refuses, and does not move it.
    let o = env.run(&["ref", "init"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(stderr(&o).contains("error: ref main already exists"), "{}", stderr(&o));
    assert_eq!(env.ok(&["ref", "get", "main"]).trim(), id);

    // Other refs and messages.
    let other = env.ok(&["ref", "init", "refs/heads/dev", "-m", "start dev"]);
    assert_eq!(env.ok(&["log", "refs/heads/dev"]), format!("{} start dev\n", other.trim()));

    // A workspace on the seeded ref works end to end.
    let ws = env._tmp.path().join("ws");
    assert_eq!(env.ok(&["ws", "new", ws.to_str().expect("utf8")]).trim(), id);
    lands(&env, &ws, "hello.txt", "hi\n", "first file");
    assert_eq!(env.ok(&["cat", "main:hello.txt"]), "hi\n");
}

#[test]
fn serve_help_has_no_client_flags() {
    let o = Command::new(env!("CARGO_BIN_EXE_ant"))
        .args(["serve", "--help"])
        .output()
        .expect("spawning ant");
    assert!(o.status.success());
    let help = stdout(&o);
    assert!(!help.contains("--token"), "{help}");
    assert!(!help.contains("--server"), "{help}");
    assert!(help.contains("--grpc-listen"));
}

fn pid_alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

/// Detach a mount even if its daemon is dead.
fn detach(dir: &Path) {
    let _ = Command::new("fusermount3").arg("-u").arg("-z").arg(dir).output();
}

#[test]
#[ignore = "needs FUSE (/dev/fuse, fusermount3)"]
fn lazy_workspace_survives_daemon_kill() {
    let env = start();
    env.ok(&["ref", "init"]);
    let seed = env._tmp.path().join("seed");
    env.ok(&["ws", "new", seed.to_str().expect("utf8")]);
    lands(&env, &seed, "kept.txt", "base\n", "seed file");

    let dir = env._tmp.path().join("lazy");
    let d = dir.to_str().expect("utf8");
    env.ok(&["ws", "new", d, "--lazy"]);
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            detach(&self.0);
        }
    }
    let _cleanup = Cleanup(dir.clone());

    // Uncommitted edits: modify, add, delete.
    fs::write(dir.join("kept.txt"), "edited\n").expect("edit");
    fs::write(dir.join("new.txt"), "brand new\n").expect("add");
    let state = fs::read_to_string(dir.join(".ant/lazy")).expect("state dir");
    let pid: u32 = fs::read_to_string(Path::new(state.trim()).join("ready"))
        .expect("ready")
        .trim()
        .parse()
        .expect("pid");
    assert_eq!(env.ok(&["-C", d, "status"]), "M\tkept.txt\nA\tnew.txt\n");

    // SIGKILL the daemon.
    let killed = Command::new("kill").args(["-9", &pid.to_string()]).status().expect("kill");
    assert!(killed.success());
    let deadline = Instant::now() + Duration::from_secs(10);
    while pid_alive(pid) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        fs::metadata(&dir).expect_err("dead mount").kind(),
        std::io::ErrorKind::NotConnected
    );

    // The error says what happened and how to fix it.
    for args in [vec!["-C", d, "status"], vec!["-C", d, "submit", "-m", "x"]] {
        let o = env.run(&args);
        assert_eq!(o.status.code(), Some(1));
        let err = stderr(&o);
        assert!(err.contains(&format!("error: lazy workspace mount {d} is dead")), "{err}");
        assert!(err.contains("hint: ") && err.contains(&format!("ant ws remount {d}")));
        assert!(!err.contains("not inside an ant workspace"), "{err}");
    }

    // Remount recovers the same workspace with its edits.
    let base = env.ok(&["ws", "remount", d]);
    assert_eq!(base.trim().len(), 64);
    assert_eq!(env.ok(&["-C", d, "status"]), "M\tkept.txt\nA\tnew.txt\n");
    assert_eq!(fs::read_to_string(dir.join("kept.txt")).expect("kept"), "edited\n");
    assert_eq!(fs::read_to_string(dir.join("new.txt")).expect("new"), "brand new\n");

    // A live mount is not remounted again.
    let o = env.run(&["ws", "remount", d]);
    assert_eq!(o.status.code(), Some(1));
    assert!(stderr(&o).contains("already mounted"), "{}", stderr(&o));

    // And it is a working workspace.
    env.ok(&["-C", d, "submit", "-m", "after recovery"]);
    assert!(env.ok(&["-C", d, "land"]).starts_with("landed "));
    assert_eq!(env.ok(&["cat", "main:kept.txt"]), "edited\n");
    assert_eq!(env.ok(&["cat", "main:new.txt"]), "brand new\n");
    env.ok(&["ws", "unmount", d]);
}
