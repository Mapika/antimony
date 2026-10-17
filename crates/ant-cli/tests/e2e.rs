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
use std::time::{Duration, Instant};

use common::{KillOnDrop, PG_URL, free_ports};

const S3_ENDPOINT: &str = "http://localhost:9009";

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
    run_end_to_end(false, Stores::Postgres);
}

/// The same flow in local mode (`--local <dir>`: SQLite + pack files; no
/// services needed): import, then serve the directory.
#[test]
fn cli_end_to_end_local() {
    run_end_to_end(false, Stores::Local);
}

#[test]
#[ignore = "needs docker compose up -d and FUSE"]
fn cli_lazy_end_to_end() {
    run_end_to_end(true, Stores::Postgres);
}

/// Where an end-to-end run keeps its data.
enum Stores {
    /// Postgres + S3 (the dev services).
    Postgres,
    /// A local data directory.
    Local,
}

fn run_end_to_end(lazy: bool, stores: Stores) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    // Declared before the server, so they drop last (after it) and also on panic.
    let mut _schema = None;
    let mut _s3 = None;
    let store_flags: Vec<String> = match stores {
        Stores::Postgres => {
            let schema = ant_store::testing::scratch_schema("e2e");
            _schema = Some(ant_store::postgres::SchemaGuard::new(PG_URL, &schema));
            // The shared test bucket under a per-run prefix.
            let s3_cfg = ant_store::s3::S3Config::for_tests("e2e");
            let prefix = s3_cfg.prefix.clone();
            _s3 = Some(ant_store::testing::S3PrefixGuard(s3_cfg.clone()));
            [
                "--pg-url",
                PG_URL,
                "--pg-schema",
                &schema,
                "--s3-endpoint",
                S3_ENDPOINT,
                "--s3-bucket",
                &s3_cfg.bucket,
                "--s3-prefix",
                &prefix,
                "--s3-access-key",
                "test",
                "--s3-secret-key",
                "test",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect()
        }
        Stores::Local => vec!["--local".into(), root.join("data").display().to_string()],
    };

    let repo = root.join("repo");
    build_repo(&repo);

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

/// `ant admin schema`, `--no-migrate` and the refusal of a newer schema
/// (Postgres only).
#[test]
#[ignore = "needs docker compose up -d"]
fn cli_schema_versions() {
    let schema = ant_store::testing::scratch_schema("e2e_schema");
    let _schema = ant_store::postgres::SchemaGuard::new(PG_URL, &schema);
    let tmp = tempfile::tempdir().expect("tempdir");
    let ctx = Ctx {
        ant: PathBuf::from(env!("CARGO_BIN_EXE_ant")),
        env: vec![("ANT_CACHE".into(), tmp.path().join("cache").display().to_string())],
    };
    let store = ["--pg-url", PG_URL, "--pg-schema", &schema];
    let with = |args: &[&'static str]| -> Vec<String> {
        args.iter().chain(store.iter()).map(|s| s.to_string()).collect()
    };
    let run = |args: &[String]| {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        ctx.run(None, &args)
    };
    let latest = ant_store::postgres::SCHEMA_VERSION;
    let names: Vec<String> = ant_store::postgres::MIGRATIONS
        .iter()
        .map(|m| format!("{} {}", m.version, m.name))
        .collect();

    // A schema that does not exist yet: everything pending, nothing created.
    let r = run(&with(&["admin", "schema"])).ok();
    let mut want = format!("version 0\nsupported {latest}\n");
    for n in &names {
        want.push_str(&format!("pending {n}\n"));
    }
    check_eq(&r, "schema status", r.stdout(), want);
    let r = run(&with(&["import", "/nonexistent.git", "--no-migrate"])).expect_code(1);
    let err = r.stderr();
    if !err.contains("error: ") || !err.contains("hint: run `ant admin schema --migrate`") {
        r.fail("--no-migrate on an unmigrated schema: error + hint");
    }

    let r = run(&with(&["admin", "schema", "--migrate"])).ok();
    let mut want = String::new();
    for n in &names {
        want.push_str(&format!("migrated {n}\n"));
    }
    want.push_str(&format!("version {latest}\nsupported {latest}\n"));
    check_eq(&r, "migrate", r.stdout(), want);
    let r = run(&with(&["admin", "schema", "--migrate"])).ok();
    check_eq(&r, "migrate again", r.stdout(), format!("version {latest}\nsupported {latest}\n"));

    // A newer version's schema: refused by every entry point.
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        use sqlx::Connection;
        let mut conn = sqlx::PgConnection::connect(PG_URL).await.expect("connect");
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO \"{schema}\".schema_version (version, name, applied_by) \
             VALUES ({}, 'future', 'ant 99')",
            latest + 1
        )))
        .execute(&mut conn)
        .await
        .expect("insert");
        conn.close().await.expect("close");
    });
    let (grpc, http) = free_ports();
    let mut serve: Vec<String> = vec![
        "serve".into(),
        "--grpc-listen".into(),
        format!("127.0.0.1:{grpc}"),
        "--http-listen".into(),
        format!("127.0.0.1:{http}"),
    ];
    serve.extend(with(&[]));
    for args in [
        with(&["admin", "schema"]),
        with(&["admin", "schema", "--migrate"]),
        with(&["import", "/nonexistent.git"]),
        serve,
    ] {
        let r = run(&args).expect_code(1);
        let err = r.stderr();
        if !err.contains(&format!("version {} is newer than this version of ant", latest + 1))
            || !err.contains("hint: upgrade ant")
        {
            r.fail("newer schema: error + upgrade hint");
        }
    }
}

#[test]
#[ignore = "needs docker compose up -d"]
fn cli_gc() {
    let schema = ant_store::testing::scratch_schema("e2e_gc");
    let _schema = ant_store::postgres::SchemaGuard::new(PG_URL, &schema);
    let tmp = tempfile::tempdir().expect("tempdir");
    let ctx = Ctx {
        ant: PathBuf::from(env!("CARGO_BIN_EXE_ant")),
        env: vec![("ANT_CACHE".into(), tmp.path().join("cache").display().to_string())],
    };
    let run = |args: &[&str]| {
        let mut all: Vec<&str> = args.to_vec();
        all.extend(["--pg-url", PG_URL, "--pg-schema", &schema]);
        ctx.run(None, &all)
    };
    let counts = "live 0\ncondemned 0\nobjects 0 0\nlarge-objects 0 0\ncommits 0\nchangesets 0 0\n";
    let r = run(&["admin", "gc", "--dry-run"]).ok();
    let out = r.stdout();
    if !out.starts_with("dry-run\ncutoff ") || !out.ends_with(counts) {
        r.fail("dry run report");
    }
    let r = run(&["admin", "gc", "--quiesce", "0s"]).ok();
    let out = r.stdout();
    let tail = format!("{counts}legacy-changes 0\nderived 0\nkept 0\n");
    if !out.starts_with("done\ncutoff ") || !out.ends_with(&tail) {
        r.fail("run report");
    }
    let r = run(&["admin", "gc", "--abort"]).ok();
    check_eq(&r, "abort", r.stdout(), "nothing to abort\n".to_string());
    let r = run(&["admin", "gc", "--grace", "1h"]).expect_code(1);
    if !r.stderr().contains("grace period must be at least 24 hours") {
        r.fail("short grace refused");
    }
}

/// `ant admin gc|repack` / `ant admin schema` on a local data directory,
/// which one process uses at a time: offline, then through the server using
/// it (its maintenance socket).
#[test]
fn cli_gc_and_schema_local() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("data");
    let data_s = data.display().to_string();
    let ctx = Ctx {
        ant: PathBuf::from(env!("CARGO_BIN_EXE_ant")),
        env: vec![("ANT_CACHE".into(), tmp.path().join("cache").display().to_string())],
    };
    let run = |args: &[&str]| {
        let mut all: Vec<&str> = args.to_vec();
        all.extend(["--local", &data_s]);
        ctx.run(None, &all)
    };
    let r = run(&["admin", "schema"]).ok();
    check_eq(
        &r,
        "new directory",
        r.stdout(),
        "version 0\nsupported 2\npending 1 baseline\npending 2 repacked_packs\n".into(),
    );
    let r = run(&["admin", "schema", "--migrate"]).ok();
    check_eq(
        &r,
        "migrated",
        r.stdout(),
        "migrated 1 baseline\nmigrated 2 repacked_packs\nversion 2\nsupported 2\n".into(),
    );
    let counts = "live 0\ncondemned 0\nobjects 0 0\nlarge-objects 0 0\ncommits 0\nchangesets 0 0\n";
    let r = run(&["admin", "gc", "--dry-run"]).ok();
    let out = r.stdout();
    if !out.starts_with("dry-run\ncutoff ") || !out.contains(counts) {
        r.fail("dry run report");
    }
    let r = run(&["admin", "gc"]).ok();
    let out = r.stdout();
    let tail = format!("{counts}legacy-changes 0\nderived 0\nkept 0\npacks 0 0\n");
    if !out.starts_with("done\ncutoff ") || !out.contains(&tail) || !out.ends_with("vacuumed no\n")
    {
        r.fail("run report");
    }
    run(&["admin", "gc", "--abort"]).expect_code(1);
    let r = run(&["admin", "gc", "--grace", "1h"]).expect_code(1);
    if !r.stderr().contains("grace period must be at least 24 hours") {
        r.fail("short grace refused");
    }
    let repacked = "done\ncommits 0\nreachable 0\nhot 0\nselected 0 0 0 0\nold-bytes 0 0\n\
                    new-bytes 0\nsaved 0\npacks 0 0\ndeltas 0 0\nmoved 0 0\nskipped 0\n\
                    packs-deleted 0 0\n";
    let r = run(&["admin", "repack"]).ok();
    check_eq(&r, "offline repack", r.stdout(), repacked.into());
    // While a server uses the directory, gc and repack run in it; offline
    // tools are refused.
    let (grpc, http) = free_ports();
    let _server = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_ant"))
            .args(["serve", "--local", &data_s, "--grpc-listen"])
            .arg(format!("127.0.0.1:{grpc}"))
            .arg("--http-listen")
            .arg(format!("127.0.0.1:{http}"))
            // Scheduled collections as well (none due during the test).
            .args(["--gc-interval", "1h", "--gc-grace", "2d"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawning ant serve"),
    );
    common::wait_until(|| std::net::TcpStream::connect(("127.0.0.1", grpc)).is_ok());
    let r = run(&["admin", "gc", "--quiesce", "1s"]).ok();
    let out = r.stdout();
    if !out.starts_with("done\ncutoff ") || !out.contains(&tail) {
        r.fail("gc through the server");
    }
    let r = run(&["admin", "gc", "--grace", "1h"]).expect_code(1);
    if !r.stderr().contains("grace period must be at least 24 hours") {
        r.fail("short grace refused by the server");
    }
    let r = run(&["admin", "repack", "--min-age", "0"]).ok();
    check_eq(&r, "repack through the server", r.stdout(), repacked.into());
    let r = run(&["admin", "schema", "--migrate"]).expect_code(1);
    if !r.stderr().contains("is in use by another process") {
        r.fail("directory in use");
    }
}

/// `ant local start|status|stop` on a fresh data directory.
#[test]
fn cli_local_start_status_stop() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("data");
    let data_s = data.display().to_string();
    let (grpc, http) = free_ports();
    let ctx = Ctx {
        ant: PathBuf::from(env!("CARGO_BIN_EXE_ant")),
        env: vec![
            ("ANT_CACHE".into(), tmp.path().join("cache").display().to_string()),
            ("ANT_SERVER".into(), format!("http://127.0.0.1:{grpc}")),
        ],
    };
    /// Stops the server on drop (also on panic).
    struct Stop<'a>(&'a Ctx, &'a str);
    impl Drop for Stop<'_> {
        fn drop(&mut self) {
            let _ = self.0.run(None, &["local", "stop", "--dir", self.1]);
        }
    }
    let r = ctx.run(None, &["local", "status", "--dir", &data_s]).expect_code(1);
    check_eq(&r, "status before", r.stdout(), "stopped\n".into());
    let (g, h) = (format!("127.0.0.1:{grpc}"), format!("127.0.0.1:{http}"));
    let start = ["local", "start", "--dir", &data_s, "--grpc-listen", &g, "--http-listen", &h];
    let r = ctx.run(None, &start).ok();
    let _stop = Stop(&ctx, &data_s);
    let out = r.stdout();
    let urls = format!("http://{g} http://{h}\n");
    let Some(pid) = out.strip_prefix("started ").and_then(|o| o.strip_suffix(&format!(" {urls}")))
    else {
        r.fail("start output");
    };
    let r = ctx.run(None, &["local", "status", "--dir", &data_s]).ok();
    check_eq(&r, "status", r.stdout(), format!("running {pid} {urls}"));
    let r = ctx.run(None, &start).ok();
    check_eq(&r, "second start", r.stdout(), format!("running {pid} {urls}"));
    // The server answers and collects garbage on request; the directory is
    // busy for offline tools.
    let r = ctx.run(None, &["ref", "list"]).ok();
    check_eq(&r, "no refs yet", r.stdout(), String::new());
    let r = ctx.run(None, &["admin", "gc", "--local", &data_s]).ok();
    if !r.stdout().starts_with("done\n") {
        r.fail("gc in the server");
    }
    let r = ctx.run(None, &["admin", "schema", "--migrate", "--local", &data_s]).expect_code(1);
    if !r.stderr().contains("in use") {
        r.fail("busy directory");
    }
    let r = ctx.run(None, &["local", "stop", "--dir", &data_s]).ok();
    check_eq(&r, "stop", r.stdout(), format!("stopped {pid}\n"));
    let r = ctx.run(None, &["local", "stop", "--dir", &data_s]).ok();
    check_eq(&r, "stop again", r.stdout(), "stopped\n".into());
    ctx.run(None, &["local", "status", "--dir", &data_s]).expect_code(1);
    assert!(data.join("server.log").exists());
}

/// `ant admin copy-local`: an imported local repository copied into another
/// store (a second local directory; with the dev services also Postgres),
/// then served from there.
fn copy_local_into(dest: &[&str]) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let repo = tmp.path().join("repo");
    build_repo(&repo);
    let src = tmp.path().join("src").display().to_string();
    let ctx = Ctx {
        ant: PathBuf::from(env!("CARGO_BIN_EXE_ant")),
        env: vec![("ANT_CACHE".into(), tmp.path().join("cache").display().to_string())],
    };
    let git_dir = repo.join(".git").display().to_string();
    let r =
        ctx.run(None, &["import", &git_dir, "--git-ref", "refs/heads/main", "--local", &src]).ok();
    let tip = r.stdout().trim_end().strip_prefix("main ").expect("main <id>").to_string();
    let mut args = vec!["admin", "copy-local", "--from", &src];
    args.extend_from_slice(dest);
    let r = ctx.run(None, &args).ok();
    let out = r.stdout();
    if !out.contains("commits 3\n") || !out.contains("refs 1\n") || !out.starts_with("objects ") {
        r.fail("copy report");
    }
    let r = ctx.run(None, &args).ok();
    if !r.stdout().ends_with(&format!("refs 0\nskipped main {tip}\n")) {
        r.fail("second copy skips the existing ref");
    }
    // Serve the copy and read through it.
    let (grpc, http) = free_ports();
    let _server = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_ant"))
            .arg("serve")
            .args(dest)
            .arg("--grpc-listen")
            .arg(format!("127.0.0.1:{grpc}"))
            .arg("--http-listen")
            .arg(format!("127.0.0.1:{http}"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawning ant serve"),
    );
    let client = Ctx {
        ant: ctx.ant.clone(),
        env: vec![
            ("ANT_CACHE".into(), tmp.path().join("cache2").display().to_string()),
            ("ANT_SERVER".into(), format!("http://127.0.0.1:{grpc}")),
        ],
    };
    common::wait_until(|| client.run(None, &["ref", "get", "main"]).code() == Some(0));
    let r = client.run(None, &["cat", "main:src/lib.rs"]).ok();
    check_eq(&r, "content", r.stdout(), LIB_V2.to_string());
    let r = client.run(None, &["log", "main"]).ok();
    check_eq(&r, "history", r.stdout().lines().count(), 3);
}

#[test]
fn cli_copy_local_into_local() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dest = tmp.path().join("dest").display().to_string();
    copy_local_into(&["--local", &dest]);
}

#[test]
#[ignore = "needs docker compose up -d"]
fn cli_copy_local_into_postgres() {
    let schema = ant_store::testing::scratch_schema("e2e_copy");
    let _schema = ant_store::postgres::SchemaGuard::new(PG_URL, &schema);
    let s3_cfg = ant_store::s3::S3Config::for_tests("e2e-copy");
    let _s3 = ant_store::testing::S3PrefixGuard(s3_cfg.clone());
    copy_local_into(&[
        "--pg-url",
        PG_URL,
        "--pg-schema",
        &schema,
        "--s3-endpoint",
        S3_ENDPOINT,
        "--s3-bucket",
        &s3_cfg.bucket,
        "--s3-prefix",
        &s3_cfg.prefix,
        "--s3-access-key",
        "test",
        "--s3-secret-key",
        "test",
    ]);
}

/// stdout of a git command in `repo` (which must succeed).
fn git_stdout(repo: &Path, args: &[&str]) -> Vec<u8> {
    let out = Command::new("git")
        .current_dir(repo)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("running git");
    assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    out.stdout
}

/// A history whose file content and commit message are not UTF-8 (Latin-1
/// lines among UTF-8 ones, a stray 0xff, CRLF, no final newline), imported
/// with `ant import` and served locally: `ant blame` attributes every line
/// as `git blame` does and prints the bytes; `ant diff --patch` prints a
/// patch `git apply` takes; `ant cat` and `ant log` print the bytes.
#[test]
fn cli_content_that_is_not_utf8_local() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let repo = tmp.path().join("repo");
    fs::create_dir_all(&repo).expect("mkdir");
    git(&repo, "2024-01-01T00:00:00+0000", &["init", "-q", "-b", "main"]);
    let v1: &[u8] = b"line one\nMaintainer: Ana\n";
    let v2: &[u8] = b"line one\nMaintainer: J\xf6rg M\xfcller\nMaintainer: Ana\n";
    let v3: &[u8] =
        b"line one\nMaintainer: J\xf6rg M\xfcller\nMaintainer: Ana\nZo\xc3\xab\r\nlast \xff";
    write(&repo, "MAINTAINERS", v1);
    git(&repo, "2024-01-01T00:00:00+0000", &["add", "-A"]);
    git(&repo, "2024-01-01T00:00:00+0000", &["commit", "-q", "-m", "utf8 start"]);
    write(&repo, "MAINTAINERS", v2);
    git(&repo, "2024-01-02T00:00:00+0000", &["commit", "-q", "-a", "-m", "latin1 line"]);
    write(&repo, "MAINTAINERS", v3);
    let msg = tmp.path().join("msg");
    fs::write(&msg, b"caf\xe9 commit\n\nbody \xff\n").expect("write msg");
    let msg = msg.display().to_string();
    let latin1 = ["-c", "i18n.commitEncoding=ISO-8859-1", "commit", "-q", "-a", "-F", &msg];
    git(&repo, "2024-01-03T00:00:00+0000", &latin1);

    let data = tmp.path().join("data").display().to_string();
    let (grpc, http) = free_ports();
    let ctx = Ctx {
        ant: PathBuf::from(env!("CARGO_BIN_EXE_ant")),
        env: vec![
            ("ANT_SERVER".into(), format!("http://127.0.0.1:{grpc}")),
            ("ANT_CACHE".into(), tmp.path().join("cache").display().to_string()),
        ],
    };
    let git_dir = repo.join(".git").display().to_string();
    ctx.run(None, &["import", &git_dir, "--git-ref", "refs/heads/main", "--local", &data]).ok();
    let _server = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_ant"))
            .args(["serve", "--local", &data])
            .arg("--grpc-listen")
            .arg(format!("127.0.0.1:{grpc}"))
            .arg("--http-listen")
            .arg(format!("127.0.0.1:{http}"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawning ant serve"),
    );
    common::wait_until(|| ctx.run(None, &["ref", "get", "main"]).code() == Some(0));

    // git commit (sha1) -> antimony commit, by position on the linear history.
    let git_ids = String::from_utf8(git_stdout(&repo, &["rev-list", "main"])).unwrap();
    let r = ctx.run(None, &["log", "main"]).ok();
    let log: Vec<&[u8]> = r.out.stdout.split(|&b| b == b'\n').filter(|l| !l.is_empty()).collect();
    check_eq(&r, "commits", log.len(), 3);
    check_eq(&r, "latin1 subject", &log[0][65..], &b"caf\xe9 commit"[..]);
    let ant_ids: Vec<String> =
        log.iter().map(|l| String::from_utf8(l[..64].to_vec()).unwrap()).collect();
    let to_ant: std::collections::HashMap<&str, &str> =
        git_ids.lines().zip(ant_ids.iter().map(String::as_str)).collect();

    // Blame: each line's owner as `git blame` has it, the line's bytes as
    // stored (`ant blame` drops the terminator, the `\r` of a CRLF too).
    let porcelain = git_stdout(&repo, &["blame", "--line-porcelain", "main", "--", "MAINTAINERS"]);
    let mut want = Vec::new();
    let mut owner = String::new();
    let mut n = 0;
    for line in porcelain.split(|&b| b == b'\n') {
        if let Some(text) = line.strip_prefix(b"\t") {
            n += 1;
            let text = text.strip_suffix(b"\r").unwrap_or(text);
            want.extend_from_slice(format!("{} {n}\t", to_ant[owner.as_str()]).as_bytes());
            want.extend_from_slice(text);
            want.push(b'\n');
        } else if line.len() > 40 && line[..40].iter().all(u8::is_ascii_hexdigit) {
            owner = String::from_utf8(line[..40].to_vec()).unwrap();
        }
    }
    assert_eq!(n, 5);
    let r = ctx.run(None, &["blame", "main:MAINTAINERS"]).ok();
    if r.out.stdout != want {
        r.fail(&format!("blame: expected {:?}", String::from_utf8_lossy(&want)));
    }

    let r = ctx.run(None, &["cat", "main:MAINTAINERS"]).ok();
    check_eq(&r, "cat", r.out.stdout.as_slice(), v3);

    // The patch from v1 to v3 applies in reverse to the checkout at v3.
    let r = ctx.run(None, &["diff", "--patch", "main~2", "main"]).ok();
    if r.out.stdout.windows(7).any(|w| w == b"Binary ") {
        r.fail("content that is not UTF-8 is text");
    }
    let patch = tmp.path().join("p.diff");
    fs::write(&patch, &r.out.stdout).expect("write patch");
    let patch = patch.display().to_string();
    git_stdout(&repo, &["apply", "-R", "--check", &patch]);
    git_stdout(&repo, &["apply", "-R", &patch]);
    assert_eq!(fs::read(repo.join("MAINTAINERS")).unwrap(), v1);
}
