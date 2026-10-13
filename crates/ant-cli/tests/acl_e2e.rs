//! `ant` against an `ant-server` running with `--policy`: tokens come from
//! `ANT_TOKEN`. The in-memory test runs by default; the Postgres one
//! (`ant import --policy` + `ant admin reindex-acl`) needs
//! `docker compose up -d` and `--include-ignored`.

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PG_URL: &str = "postgres://ant:ant@localhost:54329/ant";
const READER: &str = "reader-token";
const WRITER: &str = "writer-token";
const ADMIN: &str = "admin-token";

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn sha256_hex(s: &str) -> String {
    ant_core::ObjectId(ant_auth::hash_token(s)).to_hex()
}

fn policy() -> String {
    format!(
        r#"
[[principal]]
name = "reader"
token_sha256 = "{}"
groups = ["agents"]

[[principal]]
name = "writer"
token_sha256 = "{}"
groups = ["agents", "security"]

[[principal]]
name = "admin"
token_sha256 = "{}"
admin = true

[[path]]
prefix = ""
read = ["*"]
write = ["group:agents"]

[[path]]
prefix = "secrets"
read = ["group:security"]
write = ["group:security"]

[[ref]]
pattern = "main"
update = []
land = ["group:agents"]
"#,
        sha256_hex(READER),
        sha256_hex(WRITER),
        sha256_hex(ADMIN)
    )
}

fn free_ports() -> (u16, u16) {
    let a = TcpListener::bind("127.0.0.1:0").expect("bind");
    let b = TcpListener::bind("127.0.0.1:0").expect("bind");
    let ports = (a.local_addr().expect("addr").port(), b.local_addr().expect("addr").port());
    drop(a);
    drop(b);
    ports
}

fn cargo_build_server() -> PathBuf {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let st = Command::new(cargo).args(["build", "-p", "ant-server"]).status().expect("cargo build");
    assert!(st.success(), "cargo build -p ant-server failed");
    let ant = PathBuf::from(env!("CARGO_BIN_EXE_ant"));
    let server = ant.parent().expect("bin dir").join("ant-server");
    assert!(server.exists(), "ant-server not found at {server:?}");
    server
}

struct Env {
    grpc: u16,
    http: u16,
    root: PathBuf,
}

impl Env {
    /// Run `ant` as the principal owning `token` (None = no token), with a
    /// per-principal object cache.
    fn ant(&self, token: Option<&str>, dir: Option<&Path>, args: &[&str]) -> Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_ant"));
        cmd.args(args)
            .stdin(Stdio::null())
            .env("ANT_SERVER", format!("http://127.0.0.1:{}", self.grpc))
            .env("ANT_HTTP", format!("http://127.0.0.1:{}", self.http))
            .env("ANT_CACHE", self.root.join(format!("cache-{}", token.unwrap_or("anon"))))
            .env("ANT_AUTHOR_NAME", "Agent")
            .env("ANT_AUTHOR_EMAIL", "agent@example.com")
            .env_remove("ANT_TOKEN")
            .env_remove("ANT_POLICY");
        if let Some(t) = token {
            cmd.env("ANT_TOKEN", t);
        }
        if let Some(d) = dir {
            cmd.current_dir(d);
        }
        cmd.output().expect("spawning ant")
    }

    fn ok(&self, token: Option<&str>, dir: Option<&Path>, args: &[&str]) -> String {
        let out = self.ant(token, dir, args);
        assert!(
            out.status.success(),
            "ant {args:?} as {token:?} failed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).expect("utf8")
    }

    fn fails(&self, token: Option<&str>, dir: Option<&Path>, args: &[&str]) -> String {
        let out = self.ant(token, dir, args);
        assert_eq!(
            out.status.code(),
            Some(1),
            "ant {args:?} as {token:?} should fail\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stderr).into_owned()
    }

    fn wait_ready(&self, server: &mut KillOnDrop) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let out = self.ant(Some(ADMIN), None, &["ref", "list"]);
            if out.status.success() {
                return;
            }
            if let Ok(Some(st)) = server.0.try_wait() {
                panic!("ant-server exited early: {st}");
            }
            assert!(Instant::now() < deadline, "server not ready: {}", String::from_utf8_lossy(&out.stderr));
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

fn start_server(server_bin: &Path, root: &Path, store: &[&str]) -> (Env, KillOnDrop) {
    let (grpc, http) = free_ports();
    let policy_file = root.join("policy.toml");
    fs::write(&policy_file, policy()).expect("policy");
    let mut cmd = Command::new(server_bin);
    cmd.args(["--grpc-listen", &format!("127.0.0.1:{grpc}"), "--http-listen", &format!("127.0.0.1:{http}")])
        .args(store)
        .env("ANT_POLICY", &policy_file)
        .stdin(Stdio::null());
    let mut server = KillOnDrop(cmd.spawn().expect("spawning ant-server"));
    let env = Env { grpc, http, root: root.to_path_buf() };
    env.wait_ready(&mut server);
    (env, server)
}

#[test]
fn cli_with_tokens() {
    let server_bin = cargo_build_server();
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    let (env, _server) = start_server(&server_bin, root, &["--memory"]);

    // No token / wrong token: rejected.
    let err = env.fails(None, None, &["ref", "list"]);
    assert!(err.contains("missing bearer token"), "{err}");
    let err = env.fails(Some("nope"), None, &["ref", "list"]);
    assert!(err.contains("unknown token"), "{err}");

    let w_dir = root.join("ws-writer");
    seed_main(&env);
    env.ok(Some(WRITER), None, &["ws", "new", w_dir.to_str().expect("utf8"), "--at", "main"]);
    assert_eq!(fs::read(w_dir.join("secrets/key")).expect("key"), b"seed secret\n");

    // Writer edits a secret through the normal upload flow (PutObjects +
    // blob ids) and lands it.
    fs::write(w_dir.join("secrets/key"), b"rotated secret\n").expect("edit");
    fs::write(w_dir.join("src/lib.rs"), b"pub fn v2() {}\n").expect("edit");
    env.ok(Some(WRITER), Some(&w_dir), &["submit", "-m", "rotate"]);
    let out = env.ok(Some(WRITER), Some(&w_dir), &["land"]);
    assert!(out.starts_with("landed "), "{out}");

    // Reader: public content works, secrets look missing.
    assert_eq!(env.ok(Some(READER), None, &["cat", "main:src/lib.rs"]), "pub fn v2() {}\n");
    env.fails(Some(READER), None, &["cat", "main:secrets/key"]);
    env.fails(Some(READER), None, &["ls", "main:secrets"]);
    env.fails(Some(READER), None, &["blame", "main:secrets/key"]);
    let diff = env.ok(Some(READER), None, &["diff", "main~1", "main"]);
    assert_eq!(diff, "M\tsrc/lib.rs\n");
    let log = env.ok(Some(READER), None, &["log", "main", "--path", "secrets"]);
    assert_eq!(log, "");
    assert_eq!(env.ok(Some(WRITER), None, &["cat", "main:secrets/key"]), "rotated secret\n");
    // v1: the root listing still names the restricted directory.
    assert!(env.ok(Some(READER), None, &["ls", "main"]).contains("\tsecrets\n"));

    // The secret blob cannot be fetched by id over plain HTTP either.
    let secret_id = env.ok(Some(WRITER), None, &["ls", "main:secrets/key"]);
    let secret_id = secret_id.split_whitespace().nth(2).expect("id").to_string();
    let url = format!("http://127.0.0.1:{}/o/{secret_id}", env.http);
    let status = |token: &str| -> String {
        let out = Command::new("curl")
            .args(["-s", "-o", "/dev/null", "-w", "%{http_code}", "-H", &format!("Authorization: Bearer {token}"), &url])
            .output()
            .expect("curl");
        String::from_utf8_lossy(&out.stdout).into_owned()
    };
    assert_eq!(status(READER), "404");
    assert_eq!(status(WRITER), "200");

    // Reader works in a sparse workspace outside secrets/ and lands.
    let r_dir = root.join("ws-reader");
    env.ok(Some(READER), None, &["ws", "new", r_dir.to_str().expect("utf8"), "--at", "main", "--sparse", "src"]);
    fs::write(r_dir.join("src/reader.rs"), b"// reader\n").expect("write");
    env.ok(Some(READER), Some(&r_dir), &["submit", "-m", "reader change"]);
    let out = env.ok(Some(READER), Some(&r_dir), &["land"]);
    assert!(out.starts_with("landed "), "{out}");
    // A full workspace needs the secrets: it fails for the reader.
    let full = root.join("ws-reader-full");
    env.fails(Some(READER), None, &["ws", "new", full.to_str().expect("utf8"), "--at", "main"]);
    // Refs: main is admin-only for direct updates.
    let tip = env.ok(Some(READER), None, &["ref", "get", "main"]);
    let err = env.fails(Some(READER), None, &["ref", "set", "main", "main~1", "--expect", tip.trim()]);
    assert!(err.contains("cannot update ref main"), "{err}");
    env.ok(Some(ADMIN), None, &["ref", "set", "main", "main~1", "--expect", tip.trim()]);
}

/// Create `main` (src/lib.rs, secrets/key) as the writer over raw gRPC:
/// `ant` has no command that creates a root commit on a server.
fn seed_main(env: &Env) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        use ant_proto::v1 as pb;
        let channel = tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{}", env.grpc))
            .expect("url")
            .connect()
            .await
            .expect("connect");
        let token: tonic::metadata::MetadataValue<_> = format!("Bearer {WRITER}").parse().expect("header");
        let mut c = pb::ant_client::AntClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
            req.metadata_mut().insert("authorization", token.clone());
            Ok(req)
        });
        let write = |path: &str, content: &str| pb::FileOp {
            path: path.into(),
            op: Some(pb::file_op::Op::Write(pb::WriteOp {
                mode: pb::EntryMode::File as i32,
                content: Some(pb::write_op::Content::Inline(content.as_bytes().to_vec())),
            })),
        };
        let sig = pb::Signature { name: "s".into(), email: "s@x".into(), time: 1_700_000_000, tz_offset_minutes: 0 };
        let commit = c
            .create_changeset(pb::CreateChangesetRequest {
                base: None,
                ops: vec![write("src/lib.rs", "pub fn v1() {}\n"), write("secrets/key", "seed secret\n")],
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

fn git(repo: &Path, args: &[&str]) {
    let out = Command::new("git")
        .current_dir(repo)
        .args(["-c", "commit.gpgsign=false"])
        .args(args)
        .env("GIT_AUTHOR_NAME", "T")
        .env("GIT_AUTHOR_EMAIL", "t@example.com")
        .env("GIT_COMMITTER_NAME", "T")
        .env("GIT_COMMITTER_EMAIL", "t@example.com")
        .env("GIT_AUTHOR_DATE", "2024-01-01T00:00:00+0000")
        .env("GIT_COMMITTER_DATE", "2024-01-01T00:00:00+0000")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("git");
    assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
}

#[test]
#[ignore = "needs docker compose up -d"]
fn import_and_reindex_with_policy_postgres() {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos();
    let schema = format!("acl_e2e_{}_{nanos}", std::process::id());
    let server_bin = cargo_build_server();
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    let repo = root.join("repo");
    fs::create_dir_all(repo.join("secrets")).expect("mkdir");
    fs::create_dir_all(repo.join("src")).expect("mkdir");
    git(&repo, &["init", "-q", "-b", "main"]);
    fs::write(repo.join("src/lib.rs"), b"pub fn a() {}\n").expect("write");
    fs::write(repo.join("secrets/key"), b"imported secret\n").expect("write");
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "init"]);
    fs::write(root.join("policy.toml"), policy()).expect("policy");
    let store = ["--pg-url", PG_URL, "--pg-schema", &schema];

    let run = |args: &[&str]| -> String {
        let out = Command::new(env!("CARGO_BIN_EXE_ant"))
            .args(args)
            .args(store)
            .env("ANT_POLICY", root.join("policy.toml"))
            .output()
            .expect("ant");
        assert!(out.status.success(), "ant {args:?}: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8(out.stdout).expect("utf8")
    };
    let result = std::panic::catch_unwind(|| {
        let git_dir = repo.join(".git");
        run(&["import", git_dir.to_str().expect("utf8")]);
        // A second reindex finds everything already indexed.
        let out = run(&["admin", "reindex-acl", "main"]);
        let fields: Vec<&str> = out.split_whitespace().collect();
        assert_eq!(fields[1..], ["0", "1", "0"], "{out}");
        let out = run(&["admin", "reindex-acl", "main", "--full"]);
        let fields: Vec<&str> = out.split_whitespace().collect();
        assert_eq!(fields[1], "3", "{out}");

        let (env, _server) = start_server(&server_bin, root, &store);
        assert_eq!(env.ok(Some(READER), None, &["cat", "main:src/lib.rs"]), "pub fn a() {}\n");
        env.fails(Some(READER), None, &["cat", "main:secrets/key"]);
        assert_eq!(env.ok(Some(WRITER), None, &["cat", "main:secrets/key"]), "imported secret\n");
        let id = env.ok(Some(WRITER), None, &["ls", "main:secrets/key"]);
        let id = id.split_whitespace().nth(2).expect("id").to_string();
        let out = Command::new("curl")
            .args(["-s", "-o", "/dev/null", "-w", "%{http_code}", "-H", &format!("Authorization: Bearer {READER}")])
            .arg(format!("http://127.0.0.1:{}/o/{id}", env.http))
            .output()
            .expect("curl");
        assert_eq!(String::from_utf8_lossy(&out.stdout), "404");
    });
    drop_schema(&schema);
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

fn drop_schema(schema: &str) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        use sqlx::Connection;
        let mut conn = sqlx::PgConnection::connect(PG_URL).await.expect("connect");
        let stmt = format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE");
        sqlx::query(sqlx::AssertSqlSafe(stmt.as_str())).execute(&mut conn).await.expect("drop");
    });
}
