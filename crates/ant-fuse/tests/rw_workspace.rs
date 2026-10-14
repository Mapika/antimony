//! Process-level tests of writable lazy workspaces: the daemon runs in a
//! child process (this test binary re-executed as `daemon_child`), exactly
//! like `ant ws serve`, so it can be killed with SIGKILL and so client I/O
//! never shares a process with the daemon (see `KERNEL_GATE` in `lib.rs`).
//!
//! - `rw_write_bench`: the write-performance contract
//!   (`docs/contracts/rw-write-performance.md`). Without `ANT_RW_BENCH=1` it
//!   runs a reduced smoke version and asserts nothing about speed.
//! - `crash_*`: kill the daemon at random points of a scripted workload,
//!   remount, and check that the workspace is a prefix of the completed
//!   operations and that no file is torn.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ant_core::ObjectId;
use ant_store::graph::build_tree;
use ant_store::memory::MemoryObjectStore;

mod support {
    use super::*;

    /// Deterministic lower-layer contents, rebuilt identically by the daemon
    /// child and the test (both need the same store contents).
    pub fn fixture(name: &str) -> Vec<(String, Vec<u8>)> {
        match name {
            "bench" => {
                let mut files: Vec<(String, Vec<u8>)> = (0..20_000)
                    .map(|i| (format!("tree/d{:03}/f{:03}", i / 100, i % 100), b"base\n".to_vec()))
                    .collect();
                files.extend(small_crate());
                files.push(("README".into(), b"bench fixture\n".to_vec()));
                files
            }
            "crash" => {
                let mut files = vec![
                    ("file".to_string(), b"original".to_vec()),
                    ("dir/a".into(), b"a".to_vec()),
                    ("dir/sub/b".into(), b"b".to_vec()),
                    ("dir/sub/c".into(), b"c".to_vec()),
                ];
                for i in 0..6 {
                    files.push((format!("big{i}"), big(i, 3 << 20)));
                }
                for i in 0..20 {
                    files.push((format!("src/m{i:02}.rs"), format!("// module {i}\n").into()));
                }
                files
            }
            other => panic!("unknown fixture {other}"),
        }
    }

    pub fn big(seed: usize, len: usize) -> Vec<u8> {
        (0..len).map(|i| ((i * 31 + seed * 7 + i / 4093) % 251) as u8).collect()
    }

    /// A dependency-free crate with enough code for rustc to write a few
    /// dozen artifacts.
    pub fn small_crate() -> Vec<(String, Vec<u8>)> {
        let mut files = vec![(
            "crate/Cargo.toml".to_string(),
            b"[package]\nname = \"benchcrate\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\n\n[[bin]]\nname = \"benchcrate\"\npath = \"src/main.rs\"\n".to_vec(),
        )];
        let mut lib = String::new();
        for m in 0..24 {
            lib.push_str(&format!("pub mod m{m};\n"));
            let mut body = String::new();
            for f in 0..40 {
                body.push_str(&format!(
                    "#[derive(Debug, Clone, PartialEq)]\npub struct S{f} {{ pub a: Vec<u64>, pub b: String }}\n\
                     impl S{f} {{\n    pub fn new(n: u64) -> Self {{ Self {{ a: (0..n).map(|x| x * {f}).collect(), b: format!(\"{m}-{{n}}\") }} }}\n\
                     \x20   pub fn sum(&self) -> u64 {{ self.a.iter().copied().fold(0u64, |s, x| s.wrapping_add(x)) }}\n}}\n\
                     pub fn f{f}<T: std::fmt::Debug + Clone>(x: T) -> String {{ format!(\"{{:?}}{{:?}}\", x.clone(), S{f}::new({f}).sum()) }}\n"
                ));
            }
            files.push((format!("crate/src/m{m}.rs"), body.into_bytes()));
        }
        files.push(("crate/src/lib.rs".into(), lib.into_bytes()));
        let mut main = String::from("fn main() {\n");
        for m in 0..24 {
            main.push_str(&format!("    println!(\"{{}}\", benchcrate::m{m}::f3({m}u8));\n"));
        }
        main.push_str("}\n");
        files.push(("crate/src/main.rs".into(), main.into_bytes()));
        files
    }

    pub fn build(rt: &tokio::runtime::Runtime, name: &str) -> (Arc<MemoryObjectStore>, ObjectId) {
        let objects = Arc::new(MemoryObjectStore::default());
        let files = fixture(name);
        let entries = files.iter().map(|(p, c)| (p.as_str(), c.as_slice())).collect::<Vec<_>>();
        let tree = rt.block_on(build_tree(objects.as_ref(), &entries)).unwrap();
        (objects, tree)
    }

    pub fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
    }

    /// A daemon child process serving one mount.
    pub struct Daemon {
        pub child: Child,
        pub mount: PathBuf,
    }

    impl Daemon {
        pub fn start(fixture: &str, mount: &Path, state: &Path) -> Self {
            let ready = state.with_extension("ready");
            let _ = fs::remove_file(&ready);
            let child = Command::new(std::env::current_exe().unwrap())
                .args(["daemon_child", "--exact", "--nocapture", "--test-threads=1"])
                .env("ANT_RW_DAEMON", fixture)
                .env("ANT_RW_MOUNT", mount)
                .env("ANT_RW_STATE", state)
                .env("ANT_RW_READY", &ready)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .spawn()
                .unwrap();
            let mut daemon = Self { child, mount: mount.to_owned() };
            let deadline = Instant::now() + Duration::from_secs(60);
            while !ready.exists() {
                if let Some(status) = daemon.child.try_wait().unwrap() {
                    panic!("daemon exited early: {status}");
                }
                assert!(Instant::now() < deadline, "daemon did not become ready");
                std::thread::sleep(Duration::from_millis(5));
            }
            daemon
        }

        /// Clean unmount: the daemon compacts its state and exits.
        pub fn stop(mut self) {
            let out = Command::new("fusermount3").arg("-u").arg(&self.mount).output().unwrap();
            assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
            let status = self.child.wait().unwrap();
            assert!(status.success(), "daemon failed: {status}");
        }

        /// SIGKILL the daemon and detach the dead mount.
        pub fn kill(mut self) {
            self.child.kill().unwrap();
            self.child.wait().unwrap();
            detach(&self.mount);
        }
    }

    impl Drop for Daemon {
        // A failing test must not leave a daemon (holding the test's stderr)
        // or a mount behind.
        fn drop(&mut self) {
            if self.child.try_wait().is_ok_and(|s| s.is_none()) {
                let _ = self.child.kill();
                let _ = self.child.wait();
                detach(&self.mount);
            }
        }
    }

    pub fn detach(mount: &Path) {
        let _ = Command::new("fusermount3").arg("-u").arg("-z").arg(mount).output();
    }

    /// Body of the `daemon_child` test when re-executed by [`Daemon::start`].
    pub fn daemon_main(fixture: &str) {
        let mount = PathBuf::from(std::env::var_os("ANT_RW_MOUNT").unwrap());
        let state = PathBuf::from(std::env::var_os("ANT_RW_STATE").unwrap());
        let ready = PathBuf::from(std::env::var_os("ANT_RW_READY").unwrap());
        let rt = runtime();
        let (objects, tree) = build(&rt, fixture);
        let session = ant_fuse::mount_rw(
            objects,
            ObjectId([1; 32]),
            tree,
            &mount,
            &state,
            rt.handle().clone(),
            ant_fuse::MountOptions::default(),
        )
        .unwrap();
        fs::write(&ready, std::process::id().to_string()).unwrap();
        session.join().unwrap();
    }
}
use support::*;

#[test]
fn daemon_child() {
    if let Ok(fixture) = std::env::var("ANT_RW_DAEMON") {
        daemon_main(&fixture);
    }
}

fn scratch(name: &str) -> tempfile::TempDir {
    tempfile::Builder::new().prefix(name).tempdir().unwrap()
}

struct Report(Vec<String>);
impl Report {
    fn push(&mut self, line: String) {
        eprintln!("{line}");
        self.0.push(line);
    }
}

fn rate(bytes: u64, d: Duration) -> f64 {
    bytes as f64 / d.as_secs_f64() / (1 << 20) as f64
}

fn sequential_write(path: &Path, total: u64) -> Duration {
    let chunk = vec![0x5au8; 128 << 10];
    let start = Instant::now();
    let mut f = fs::File::create(path).unwrap();
    let mut left = total;
    while left > 0 {
        let n = left.min(chunk.len() as u64) as usize;
        f.write_all(&chunk[..n]).unwrap();
        left -= n as u64;
    }
    drop(f);
    let took = start.elapsed();
    assert_eq!(fs::metadata(path).unwrap().len(), total);
    fs::remove_file(path).unwrap();
    took
}

fn small_files(dir: &Path, count: usize, threads: usize) -> Duration {
    fs::create_dir_all(dir).unwrap();
    let body = vec![7u8; 4096];
    let start = Instant::now();
    std::thread::scope(|s| {
        for t in 0..threads {
            let body = &body;
            s.spawn(move || {
                for i in (t..count).step_by(threads) {
                    let mut f = fs::File::create(dir.join(format!("f{i:05}"))).unwrap();
                    f.write_all(body).unwrap();
                }
            });
        }
    });
    start.elapsed()
}

fn cargo_build(dir: &Path) -> Duration {
    let mut cmd = Command::new("cargo");
    cmd.args(["build", "--offline", "--quiet"]).current_dir(dir);
    for (k, _) in std::env::vars_os() {
        let k = k.to_string_lossy().into_owned();
        if (k.starts_with("CARGO") && k != "CARGO_HOME")
            || k.starts_with("RUSTC")
            || k == "RUSTFLAGS"
        {
            cmd.env_remove(&k);
        }
    }
    let start = Instant::now();
    let out = cmd.output().unwrap();
    let took = start.elapsed();
    assert!(out.status.success(), "cargo build failed: {}", String::from_utf8_lossy(&out.stderr));
    took
}

/// The write-performance contract. Set `ANT_RW_BENCH=1` for full sizes and
/// target assertions; otherwise a reduced smoke run reports numbers only.
#[test]
#[ignore = "benchmark; real FUSE; ANT_RW_BENCH=1 for full size and targets"]
fn rw_write_bench() {
    let full = std::env::var("ANT_RW_BENCH").is_ok_and(|v| v == "1");
    let (seq_bytes, files) = if full { (1u64 << 30, 10_000) } else { (64u64 << 20, 1_000) };
    let tmp = scratch("rwperf");
    let mount = tmp.path().join("mnt");
    let state = tmp.path().join("state");
    fs::create_dir(&mount).unwrap();
    let daemon = Daemon::start("bench", &mount, &state);
    let mut report = Report(Vec::new());

    // 1. Sequential write, 128 KiB chunks, new file: mount vs upper dir.
    let mut direct = Duration::MAX;
    let mut fuse = Duration::MAX;
    for _ in 0..if full { 3 } else { 1 } {
        direct = direct.min(sequential_write(&state.join("upper/direct.bin"), seq_bytes));
        fuse = fuse.min(sequential_write(&mount.join("seq.bin"), seq_bytes));
    }
    let seq_ratio = direct.as_secs_f64() / fuse.as_secs_f64();
    report.push(format!(
        "sequential write {} MiB: direct {:.0} MiB/s, mount {:.0} MiB/s, ratio {:.0}% (target >= 60%)",
        seq_bytes >> 20,
        rate(seq_bytes, direct),
        rate(seq_bytes, fuse),
        seq_ratio * 100.0
    ));

    // 2. Small files, 8 threads.
    let took = small_files(&mount.join("small"), files, 8);
    let files_per_sec = files as f64 / took.as_secs_f64();
    report.push(format!(
        "{files} x 4 KiB create+write+close, 8 threads: {files_per_sec:.0} files/s (target >= 5000)"
    ));

    // 3. cargo build of a small crate: lazy workspace vs regular directory.
    let regular = tmp.path().join("regular");
    for (path, data) in small_crate() {
        let p = regular.join(path);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, data).unwrap();
    }
    let in_regular = cargo_build(&regular.join("crate"));
    let in_mount = cargo_build(&mount.join("crate"));
    let build_ratio = in_mount.as_secs_f64() / in_regular.as_secs_f64();
    report.push(format!(
        "cargo build: regular {:.2}s, lazy {:.2}s, ratio {build_ratio:.2}x (target <= 1.5x)",
        in_regular.as_secs_f64(),
        in_mount.as_secs_f64()
    ));
    fs::remove_dir_all(&regular).unwrap();
    fs::remove_dir_all(mount.join("crate/target")).unwrap();
    fs::remove_dir_all(mount.join("small")).unwrap();

    // 4. Status after editing 2 files of the 20k-file tree.
    fs::write(mount.join("tree/d001/f001"), b"one").unwrap();
    fs::write(mount.join("tree/d199/f099"), b"two").unwrap();
    let rt = runtime();
    let (objects, _) = build(&rt, "bench");
    let mut status = Duration::MAX;
    for _ in 0..5 {
        let start = Instant::now();
        let changes = rt.block_on(ant_fuse::rw::changes(objects.clone(), &state)).unwrap();
        status = status.min(start.elapsed());
        let paths = changes.iter().map(|c| c.path.to_string()).collect::<Vec<_>>();
        assert!(paths.iter().any(|p| p == "tree/d001/f001"), "{paths:?}");
        assert!(paths.iter().any(|p| p == "tree/d199/f099"), "{paths:?}");
    }
    report.push(format!("status after 2 edits in 20k files: {:.2} ms", status.as_secs_f64() * 1e3));
    daemon.stop();
    eprintln!(
        "rw_write_bench ({}):\n  {}",
        if full { "full" } else { "smoke" },
        report.0.join("\n  ")
    );
    if full {
        assert!(seq_ratio >= 0.6, "sequential write below target");
        assert!(files_per_sec >= 5000.0, "small files below target");
        assert!(build_ratio <= 1.5, "cargo build below target");
    }
}

mod rw_crash;

/// Kill the daemon at random points of a random workload (renames of lower
/// directories, copy-ups of 3 MiB files, deletes of edited tracked files,
/// truncates, chmods, new files), remount, and require: every operation that
/// completed survived, the interrupted one is all-or-nothing (or, for a data
/// write, a partial write by the application), no file is torn (contents
/// match the model exactly) and `status` agrees with the view. Reproduce a
/// failure with `ANT_RW_CRASH_SEED`; `ANT_RW_CRASH_ROUNDS` sets the kills.
#[test]
#[ignore = "needs FUSE (/dev/fuse, fusermount3)"]
fn crash_daemon_kill_preserves_completed_operations() {
    let seed = std::env::var("ANT_RW_CRASH_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos() as u64);
    let rounds =
        std::env::var("ANT_RW_CRASH_ROUNDS").ok().and_then(|s| s.parse().ok()).unwrap_or(25);
    rw_crash::run(seed, rounds);
}
