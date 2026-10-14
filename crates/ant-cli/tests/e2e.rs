//! End-to-end test for the `ant` CLI against a real `ant-server`, Postgres and
//! S3. The contract is the doc comment in `src/main.rs`.
//!
//! Run with `docker compose up -d` and then
//! `cargo test -p ant-cli -- --include-ignored`.

mod common;

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::{KillOnDrop, PG_URL, free_ports};

const S3_ENDPOINT: &str = "http://localhost:9009";
const S3_BUCKET: &str = "ant-e2e";

/// Result of running one command, with helpers that dump everything on failure.
struct Run {
    desc: String,
    out: Output,
}

impl Run {
    fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.out.stdout).into_owned()
    }

    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.out.stderr).into_owned()
    }

    fn code(&self) -> Option<i32> {
        self.out.status.code()
    }

    fn dump(&self) -> String {
        format!(
            "command: {}\nstatus: {:?}\n--- stdout ---\n{}\n--- stderr ---\n{}\n",
            self.desc,
            self.out.status,
            String::from_utf8_lossy(&self.out.stdout),
            String::from_utf8_lossy(&self.out.stderr)
        )
    }

    fn fail(&self, what: &str) -> ! {
        panic!("{what}\n{}", self.dump());
    }

    /// Asserts exit code 0 and returns self.
    fn ok(self) -> Self {
        if self.code() != Some(0) {
            self.fail("expected exit code 0");
        }
        self
    }

    /// Asserts exit code 0 and returns trimmed stdout.
    fn ok_text(self) -> String {
        let r = self.ok();
        r.stdout().trim_end_matches('\n').to_string()
    }

    fn expect_code(self, code: i32) -> Self {
        if self.code() != Some(code) {
            self.fail(&format!("expected exit code {code}"));
        }
        self
    }
}

fn is_id(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn assert_id(r: &Run, s: &str) {
    if !is_id(s) {
        r.fail(&format!("expected a 64-char lowercase hex id, got {s:?}"));
    }
}

fn check_eq<T: PartialEq + std::fmt::Debug>(r: &Run, what: &str, actual: T, expected: T) {
    if actual != expected {
        r.fail(&format!("{what}: expected {expected:?}, got {actual:?}"));
    }
}

struct Ctx {
    ant: PathBuf,
    env: Vec<(String, String)>,
}

impl Ctx {
    fn run(&self, dir: Option<&Path>, args: &[&str]) -> Run {
        let mut cmd = Command::new(&self.ant);
        cmd.args(args).stdin(Stdio::null());
        for (k, v) in &self.env {
            cmd.env(k, v);
        }
        if let Some(d) = dir {
            cmd.current_dir(d);
        }
        let out = cmd.output().expect("spawning ant");
        Run { desc: format!("ant {} (cwd {:?})", args.join(" "), dir), out }
    }
}

struct LazyMounts<'a> {
    ctx: &'a Ctx,
    dirs: Vec<&'a Path>,
}
impl Drop for LazyMounts<'_> {
    fn drop(&mut self) {
        for dir in &self.dirs {
            let _ = self.ctx.run(None, &["ws", "unmount", dir.to_str().unwrap()]);
        }
    }
}

fn git(repo: &Path, date: &str, args: &[&str]) {
    let out = Command::new("git")
        .current_dir(repo)
        .args(["-c", "core.autocrlf=false", "-c", "commit.gpgsign=false"])
        .args(args)
        .env("GIT_AUTHOR_NAME", "Test Author")
        .env("GIT_AUTHOR_EMAIL", "author@example.com")
        .env("GIT_COMMITTER_NAME", "Test Committer")
        .env("GIT_COMMITTER_EMAIL", "committer@example.com")
        .env("GIT_AUTHOR_DATE", date)
        .env("GIT_COMMITTER_DATE", date)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("running git");
    assert!(
        out.status.success(),
        "git {args:?} failed\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn write(root: &Path, rel: &str, contents: &[u8]) {
    let p = root.join(rel);
    fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
    fs::write(p, contents).expect("write");
}

fn big_blob() -> Vec<u8> {
    // > 4 KiB so it goes to S3 through the tiered store; includes non-UTF8 bytes.
    (0..20_000u32).map(|i| (i.wrapping_mul(31) % 251) as u8).collect()
}

const SHARED: &str = "line one\nline two\nline three\nline four\nline five\n";
const LIB_V1: &str = "pub fn answer() -> u32 {\n    41\n}\n";
const LIB_V2: &str = "pub fn answer() -> u32 {\n    42\n}\n";

fn build_repo(repo: &Path) {
    fs::create_dir_all(repo).expect("mkdir repo");
    git(repo, "2024-01-01T00:00:00+0000", &["init", "-q", "-b", "main"]);

    write(repo, "README.md", b"# demo\n");
    write(repo, "src/lib.rs", LIB_V1.as_bytes());
    write(repo, "src/util/mod.rs", b"pub fn helper() {}\n");
    write(repo, "docs/guide.md", b"# guide\nhello\n");
    write(repo, "scripts/run.sh", b"#!/bin/sh\necho hi\n");
    fs::set_permissions(repo.join("scripts/run.sh"), fs::Permissions::from_mode(0o755))
        .expect("chmod");
    write(repo, "shared.txt", SHARED.as_bytes());
    write(repo, "old.txt", b"to be removed\n");
    write(repo, "big.bin", &big_blob());
    symlink("README.md", repo.join("link")).expect("symlink");
    git(repo, "2024-01-01T00:00:00+0000", &["add", "-A"]);
    git(repo, "2024-01-01T00:00:00+0000", &["commit", "-q", "-m", "initial import"]);

    write(repo, "docs/extra.md", b"extra docs\n");
    git(repo, "2024-01-02T00:00:00+0000", &["add", "-A"]);
    git(repo, "2024-01-02T00:00:00+0000", &["commit", "-q", "-m", "add extra docs"]);

    write(repo, "src/lib.rs", LIB_V2.as_bytes());
    git(repo, "2024-01-03T00:00:00+0000", &["rm", "-q", "old.txt"]);
    git(repo, "2024-01-03T00:00:00+0000", &["add", "-A"]);
    git(repo, "2024-01-03T00:00:00+0000", &["commit", "-q", "-m", "update lib and drop old"]);
}

/// `(mode, kind, name)` for each `ant ls` line, checking the id column.
fn parse_ls(r: &Run, lines: &[String]) -> Vec<(String, String, String)> {
    lines
        .iter()
        .map(|l| {
            let Some((meta, name)) = l.split_once('\t') else {
                r.fail(&format!("ls line without tab: {l:?}"));
            };
            let parts: Vec<&str> = meta.split(' ').collect();
            if parts.len() != 3 || !is_id(parts[2]) {
                r.fail(&format!("bad ls line: {l:?}"));
            }
            (parts[0].to_string(), parts[1].to_string(), name.to_string())
        })
        .collect()
}

fn entry(mode: &str, kind: &str, name: &str) -> (String, String, String) {
    (mode.to_string(), kind.to_string(), name.to_string())
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
#[ignore = "needs docker compose up -d"]
fn cli_end_to_end() {
    run_end_to_end(false);
}

#[test]
#[ignore = "needs docker compose up -d and FUSE"]
fn cli_lazy_end_to_end() {
    run_end_to_end(true);
}

fn run_end_to_end(lazy: bool) {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos();
    let uniq = format!("{}_{}", std::process::id(), nanos);
    let schema = format!("e2e_{uniq}");
    // Declared first, so it drops last (after the server) and also on panic.
    let _schema = ant_store::postgres::SchemaGuard::new(PG_URL, &schema);
    let prefix = format!("e2e/{uniq}/");

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();

    let repo = root.join("repo");
    build_repo(&repo);

    let store_flags: Vec<String> = [
        "--pg-url",
        PG_URL,
        "--pg-schema",
        &schema,
        "--s3-endpoint",
        S3_ENDPOINT,
        "--s3-bucket",
        S3_BUCKET,
        "--s3-prefix",
        &prefix,
        "--s3-access-key",
        "test",
        "--s3-secret-key",
        "test",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    let (grpc_port, http_port) = free_ports();
    let cache = root.join("cache");
    let ctx = Ctx {
        ant: PathBuf::from(env!("CARGO_BIN_EXE_ant")),
        env: vec![
            ("ANT_SERVER".into(), format!("http://127.0.0.1:{grpc_port}")),
            ("ANT_HTTP".into(), format!("http://127.0.0.1:{http_port}")),
            ("ANT_CACHE".into(), cache.display().to_string()),
            ("ANT_AUTHOR_NAME".into(), "Agent Smith".into()),
            ("ANT_AUTHOR_EMAIL".into(), "agent@example.com".into()),
        ],
    };

    // ---- import ----
    let git_dir = repo.join(".git");
    let mut args = vec![
        "import".to_string(),
        git_dir.display().to_string(),
        "--git-ref".into(),
        "refs/heads/main".into(),
        "--ref".into(),
        "main".into(),
    ];
    args.extend(store_flags.iter().cloned());
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let r = ctx.run(None, &arg_refs);
    let text = r.stdout();
    let Some((name, tip)) = text.trim_end().split_once(' ') else {
        r.fail("import output should be `main <id>`");
    };
    check_eq(&r, "import ref name", name, "main");
    assert_id(&r, tip);
    let tip = tip.to_string();

    // ---- server ----
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ant"));
    cmd.arg("serve");
    cmd.args([
        "--grpc-listen",
        &format!("127.0.0.1:{grpc_port}"),
        "--http-listen",
        &format!("127.0.0.1:{http_port}"),
    ])
    .args(&store_flags)
    .stdin(Stdio::null());
    let mut server = KillOnDrop(cmd.spawn().expect("spawning ant-server"));

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let r = ctx.run(None, &["ref", "get", "main"]);
        if r.code() == Some(0) {
            break;
        }
        if let Ok(Some(st)) = server.0.try_wait() {
            panic!("ant-server exited early: {st}\n{}", r.dump());
        }
        if Instant::now() > deadline {
            r.fail("server not ready within 15s");
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    // ---- read side ----
    let r = ctx.run(None, &["ref", "get", "main"]);
    let got = r.stdout();
    check_eq(&r, "ref get main", got.trim_end(), tip.as_str());

    let r = ctx.run(None, &["ref", "list"]);
    let lines = r.stdout();
    if !lines.lines().any(|l| l == format!("{tip} main")) {
        r.fail("ref list should contain `<tip> main`");
    }

    let r = ctx.run(None, &["ls", "main"]);
    let lines = r.stdout().lines().map(str::to_string).collect::<Vec<_>>();
    let got = parse_ls(&r, &lines);
    let expected = vec![
        entry("100644", "blob", "README.md"),
        entry("100644", "blob", "big.bin"),
        entry("40000", "tree", "docs"),
        entry("120000", "blob", "link"),
        entry("40000", "tree", "scripts"),
        entry("100644", "blob", "shared.txt"),
        entry("40000", "tree", "src"),
    ];
    check_eq(&r, "ls main", got, expected);

    let r = ctx.run(None, &["ls", "main:scripts"]);
    let lines = r.stdout().lines().map(str::to_string).collect::<Vec<_>>();
    check_eq(&r, "ls main:scripts", parse_ls(&r, &lines), vec![entry("100755", "blob", "run.sh")]);

    let r = ctx.run(None, &["ls", "main:README.md"]);
    let lines = r.stdout().lines().map(str::to_string).collect::<Vec<_>>();
    check_eq(&r, "ls blob path", parse_ls(&r, &lines), vec![entry("100644", "blob", "README.md")]);

    ctx.run(None, &["ls", "main:does/not/exist"]).expect_code(1);
    ctx.run(None, &["cat", "main:src"]).expect_code(1);
    let r = ctx.run(None, &["ref", "get", "no-such-ref"]).expect_code(1);
    if !r.stderr().contains("hint: list the existing refs") {
        r.fail("missing ref should come with a hint");
    }

    let cat = |path: &str| -> Vec<u8> {
        let r = ctx.run(None, &["cat", &format!("main:{path}")]).ok();
        r.out.stdout.clone()
    };
    check_bytes(&ctx, "src/lib.rs", &cat("src/lib.rs"), LIB_V2.as_bytes());
    check_bytes(&ctx, "big.bin", &cat("big.bin"), &big_blob());
    check_bytes(&ctx, "shared.txt", &cat("shared.txt"), SHARED.as_bytes());
    check_bytes(&ctx, "scripts/run.sh", &cat("scripts/run.sh"), b"#!/bin/sh\necho hi\n");

    let r = ctx.run(None, &["log", "main", "-n", "2"]);
    let lines = r.stdout().lines().map(str::to_string).collect::<Vec<_>>();
    check_eq(&r, "log line count", lines.len(), 2);
    let subjects: Vec<String> = lines
        .iter()
        .map(|l| match l.split_once(' ') {
            Some((id, subj)) if is_id(id) => subj.to_string(),
            _ => r.fail(&format!("bad log line {l:?}")),
        })
        .collect();
    check_eq(&r, "log subjects", subjects, strs(&["update lib and drop old", "add extra docs"]));
    if !lines[0].starts_with(&tip) {
        r.fail("first log line should be the tip commit");
    }

    let r = ctx.run(None, &["diff", "main~1", "main"]);
    let lines = r.stdout().lines().map(str::to_string).collect::<Vec<_>>();
    check_eq(&r, "diff", sorted(lines), strs(&["D\told.txt", "M\tsrc/lib.rs"]));
    let r = ctx.run(None, &["diff", "main~2", "main~1"]);
    let lines = r.stdout().lines().map(str::to_string).collect::<Vec<_>>();
    check_eq(&r, "diff main~2 main~1", lines, strs(&["A\tdocs/extra.md"]));

    // ---- workspaces ----
    // B, C and D are all created at the same (soon to be old) base.
    let new_ws = |dir: &Path, extra: &[&str]| -> String {
        let mut a = vec!["ws", "new", dir.to_str().expect("utf8"), "--at", "main"];
        if lazy {
            a.push("--lazy");
        } else {
            a.extend_from_slice(extra);
        }
        let r = ctx.run(None, &a);
        let id = r.stdout().trim_end().to_string();
        assert_id(&r, &id);
        id
    };
    let dir_a = root.join("ws-a");
    let dir_b = root.join("ws-b");
    let dir_c = root.join("ws-c");
    let dir_d = root.join("ws-d");
    let _mounts = LazyMounts {
        ctx: &ctx,
        dirs: if lazy { vec![&dir_a, &dir_b, &dir_c, &dir_d] } else { vec![] },
    };
    check_str(&new_ws(&dir_b, &[]), &tip);
    check_str(&new_ws(&dir_c, &[]), &tip);
    check_str(&new_ws(&dir_d, &[]), &tip);
    check_str(&new_ws(&dir_a, &["--sparse", "src"]), &tip);

    // sparse: only src/ is materialised
    assert!(dir_a.join("src/lib.rs").is_file(), "sparse src/lib.rs missing");
    assert!(dir_a.join("src/util/mod.rs").is_file(), "sparse src/util/mod.rs missing");
    let mut top: Vec<String> = fs::read_dir(&dir_a)
        .expect("read ws dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .filter(|n| n != ".ant")
        .collect();
    top.sort();
    if !lazy {
        assert_eq!(top, strs(&["src"]), "sparse workspace should only contain src/");
    }
    // full workspace has everything, including exec bit
    assert_eq!(fs::read(dir_b.join("big.bin")).expect("big.bin"), big_blob());
    let mode = fs::metadata(dir_b.join("scripts/run.sh")).expect("run.sh").permissions().mode();
    assert!(mode & 0o111 != 0, "run.sh should be executable, mode {mode:o}");

    // clean workspace: empty status
    let r = ctx.run(Some(&dir_a), &["status"]);
    check_eq(&r, "clean status", r.stdout(), String::new());

    // ---- workspace A: edit / add / delete, submit, land ----
    fs::write(dir_a.join("src/lib.rs"), "pub fn answer() -> u32 {\n    43\n}\n").expect("edit");
    fs::write(dir_a.join("src/new.rs"), b"pub const NEW: u8 = 1;\n").expect("add");
    fs::remove_file(dir_a.join("src/util/mod.rs")).expect("delete");
    let r = ctx.run(Some(&dir_a), &["status"]);
    let lines = r.stdout().lines().map(str::to_string).collect::<Vec<_>>();
    check_eq(
        &r,
        "status A",
        lines,
        strs(&["M\tsrc/lib.rs", "A\tsrc/new.rs", "D\tsrc/util/mod.rs"]),
    );

    ctx.run(Some(&dir_b), &["submit", "-m", "nothing"]).expect_code(1);

    let r = ctx.run(Some(&dir_a), &["submit", "-m", "agent change"]);
    let sub_a = r.stdout().trim_end().to_string();
    assert_id(&r, &sub_a);
    let r = ctx.run(Some(&dir_a), &["land"]);
    let text = r.stdout();
    let Some(landed_a) = text.trim_end().strip_prefix("landed ") else {
        r.fail("expected `landed <id>`");
    };
    assert_id(&r, landed_a);
    let landed_a = landed_a.to_string();

    let r = ctx.run(Some(&dir_a), &["status"]);
    check_eq(&r, "status after land", r.stdout(), String::new());
    let r = ctx.run(None, &["ref", "get", "main"]);
    check_eq(&r, "main tip after A", r.stdout().trim_end().to_string(), landed_a.clone());
    let r = ctx.run(None, &["log", "main", "-n", "1"]);
    let text = r.stdout();
    check_eq(&r, "log -n 1", text.trim_end().split_once(' ').map(|(_, s)| s), Some("agent change"));
    check_bytes(&ctx, "src/new.rs", &cat("src/new.rs"), b"pub const NEW: u8 = 1;\n");
    check_bytes(&ctx, "src/lib.rs", &cat("src/lib.rs"), b"pub fn answer() -> u32 {\n    43\n}\n");
    ctx.run(None, &["cat", "main:src/util/mod.rs"]).expect_code(1);
    let r = ctx.run(None, &["diff", &tip, "main"]);
    let lines = r.stdout().lines().map(str::to_string).collect::<Vec<_>>();
    check_eq(
        &r,
        "diff base..main",
        sorted(lines),
        strs(&["A\tsrc/new.rs", "D\tsrc/util/mod.rs", "M\tsrc/lib.rs"]),
    );

    // ---- B and C both change the same line of shared.txt; main already moved ----
    let shared_b = "line one\nline two\nB was here\nline four\nline five\n";
    let shared_c = "line one\nline two\nC was here\nline four\nline five\n";
    fs::write(dir_b.join("shared.txt"), shared_b).expect("edit B");
    fs::write(dir_c.join("shared.txt"), shared_c).expect("edit C");

    let r = ctx.run(Some(&dir_b), &["submit", "-m", "B edits shared"]);
    assert_id(&r, r.stdout().trim_end());
    let r = ctx.run(Some(&dir_b), &["land"]);
    let text = r.stdout();
    let t = text.trim_end();
    let Some(rest) = t.strip_prefix("landed ") else {
        r.fail("B: expected `landed <id>[ rebased]`");
    };
    let id_b = rest.strip_suffix(" rebased").unwrap_or(rest);
    assert_id(&r, id_b);
    let tip_after_b = id_b.to_string();
    check_bytes(&ctx, "shared.txt after B", &cat("shared.txt"), shared_b.as_bytes());
    // untouched files from A are still there after rebase
    check_bytes(&ctx, "src/new.rs after B", &cat("src/new.rs"), b"pub const NEW: u8 = 1;\n");

    let r = ctx.run(Some(&dir_c), &["submit", "-m", "C edits shared"]);
    assert_id(&r, r.stdout().trim_end());
    let r = ctx.run(Some(&dir_c), &["land"]).expect_code(2);
    let lines = r.stdout().lines().map(str::to_string).collect::<Vec<_>>();
    if lines.first().map(String::as_str) != Some("conflict") {
        r.fail("C: stdout should start with `conflict`");
    }
    check_eq(&r, "conflict paths", lines[1..].to_vec(), strs(&["shared.txt"]));
    if !r.stderr().contains("hint: main moved under you") {
        r.fail("conflict should explain how to proceed on stderr");
    }
    check_eq(
        &r,
        "main unchanged by conflict",
        ctx.run(None, &["ref", "get", "main"]).ok_text(),
        tip_after_b.clone(),
    );

    // ---- D: old base, disjoint file -> rebased ----
    fs::write(dir_d.join("docs/guide.md"), "# guide\nhello from D\n").expect("edit D");
    let r = ctx.run(Some(&dir_d), &["status"]);
    let lines = r.stdout().lines().map(str::to_string).collect::<Vec<_>>();
    check_eq(&r, "status D", lines, strs(&["M\tdocs/guide.md"]));
    let r = ctx.run(Some(&dir_d), &["submit", "-m", "D edits guide"]);
    assert_id(&r, r.stdout().trim_end());
    let r = ctx.run(Some(&dir_d), &["land"]);
    let text = r.stdout();
    let Some(id_d) =
        text.trim_end().strip_prefix("landed ").and_then(|s| s.strip_suffix(" rebased"))
    else {
        r.fail("D: expected `landed <id> rebased`");
    };
    assert_id(&r, id_d);
    check_eq(
        &r,
        "main tip after D",
        ctx.run(None, &["ref", "get", "main"]).ok_text(),
        id_d.to_string(),
    );
    check_bytes(&ctx, "docs/guide.md", &cat("docs/guide.md"), b"# guide\nhello from D\n");
    check_bytes(&ctx, "shared.txt still B", &cat("shared.txt"), shared_b.as_bytes());
    check_bytes(
        &ctx,
        "src/lib.rs still A",
        &cat("src/lib.rs"),
        b"pub fn answer() -> u32 {\n    43\n}\n",
    );
    let r = ctx.run(Some(&dir_d), &["status"]);
    check_eq(&r, "status D after land", r.stdout(), String::new());

    let r = ctx.run(None, &["log", "main", "-n", "3"]);
    let subjects: Vec<String> = r
        .stdout()
        .lines()
        .map(|l| l.split_once(' ').map(|x| x.1.to_string()).unwrap_or_default())
        .collect();
    check_eq(&r, "final log", subjects, strs(&["D edits guide", "B edits shared", "agent change"]));
    if lazy {
        for dir in [&dir_a, &dir_b, &dir_c, &dir_d] {
            let before = ctx.run(Some(dir), &["status"]).ok_text();
            ctx.run(None, &["ws", "unmount", dir.to_str().unwrap()]).ok();
            assert!(dir.join(".ant/workspace").is_file());
            assert!(!dir.join("src").exists());
            ctx.run(None, &["ws", "new", "--lazy", dir.to_str().unwrap()]).ok();
            assert_eq!(ctx.run(Some(dir), &["status"]).ok_text(), before);
            ctx.run(None, &["ws", "unmount", dir.to_str().unwrap()]).ok();
        }
    }
}

fn check_str(actual: &str, expected: &str) {
    assert_eq!(actual, expected, "ws new should print the base commit id");
}

fn check_bytes(ctx: &Ctx, what: &str, actual: &[u8], expected: &[u8]) {
    if actual != expected {
        let r = ctx.run(None, &["ref", "list"]);
        panic!(
            "cat {what}: bytes differ (got {} bytes, expected {})\nrefs:\n{}",
            actual.len(),
            expected.len(),
            r.stdout()
        );
    }
}
