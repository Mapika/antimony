//! `ant local start|stop|status`: a local-mode server (`ant serve --local
//! <dir>`) in the background, one per data directory.
//!
//! Files in the data directory: `server.pid` (`<pid>\n<grpc url>\n<http
//! url>\n`, written by `start`) and `server.log` (the server's log,
//! appended). Whether a server runs is decided by the directory's `LOCK`
//! file (held by whichever process uses the directory), so a stale pid file
//! never fools `status`.

use std::fmt::Write as _;
use std::io::Write as _;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};

use crate::args::{LocalCmd, LocalDirArgs, LocalStartArgs};
use crate::ctx::Outcome;

const PID_FILE: &str = "server.pid";
const LOG_FILE: &str = "server.log";
/// Longest wait for a started server to listen, or a stopped one to exit.
const WAIT: Duration = Duration::from_secs(20);

pub async fn run(cmd: LocalCmd) -> Result<Outcome> {
    match cmd {
        LocalCmd::Start(a) => start(a).await,
        LocalCmd::Stop(a) => stop(&dir_of(&a)?).await,
        LocalCmd::Status(a) => status(&dir_of(&a)?),
    }
}

fn dir_of(a: &LocalDirArgs) -> Result<PathBuf> {
    match &a.dir {
        Some(d) => Ok(d.clone()),
        None => ant_store::local::default_dir()
            .ok_or_else(|| anyhow!("no --dir given and $HOME is not set")),
    }
}

/// What uses a data directory.
enum State {
    /// Nobody (its lock is free).
    Stopped,
    /// A server started by `ant local start`: pid, gRPC and HTTP urls.
    Running(u32, String, String),
    /// Something else holds the lock (a server started by hand, an offline
    /// tool such as `ant import` or `ant admin gc`).
    Busy,
}

fn state(dir: &Path) -> Result<State> {
    let lock = dir.join(ant_store::local::LOCK_FILE);
    let file = match std::fs::OpenOptions::new().read(true).write(true).open(&lock) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(State::Stopped),
        Err(e) => return Err(e).with_context(|| format!("opening {}", lock.display())),
    };
    match file.try_lock() {
        // Free: released when `file` drops.
        Ok(()) => return Ok(State::Stopped),
        Err(std::fs::TryLockError::WouldBlock) => {}
        Err(std::fs::TryLockError::Error(e)) => {
            return Err(e).with_context(|| format!("locking {}", lock.display()));
        }
    }
    let Ok(text) = std::fs::read_to_string(dir.join(PID_FILE)) else { return Ok(State::Busy) };
    let mut lines = text.lines();
    match (lines.next().and_then(|p| p.parse().ok()), lines.next(), lines.next()) {
        (Some(pid), Some(grpc), Some(http)) if Path::new(&format!("/proc/{pid}")).exists() => {
            Ok(State::Running(pid, grpc.to_owned(), http.to_owned()))
        }
        _ => Ok(State::Busy),
    }
}

/// `running <pid> <grpc> <http>` (exit 0), `busy` (exit 0) or `stopped`
/// (exit 1).
fn status(dir: &Path) -> Result<Outcome> {
    Ok(match state(dir)? {
        State::Running(pid, grpc, http) => Outcome::ok(format!("running {pid} {grpc} {http}\n")),
        State::Busy => Outcome::ok("busy\n"),
        State::Stopped => Outcome::with_code("stopped\n", 1),
    })
}

fn url(addr: &SocketAddr) -> String {
    format!("http://{addr}")
}

async fn start(a: LocalStartArgs) -> Result<Outcome> {
    let dir = dir_of(&a.dir)?;
    match state(&dir)? {
        State::Running(pid, grpc, http) => {
            return Ok(Outcome::ok(format!("running {pid} {grpc} {http}\n")));
        }
        State::Busy => bail!(
            "local data directory {} is in use by another process (not started by `ant local \
             start`)",
            dir.display()
        ),
        State::Stopped => {}
    }
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let log_path = dir.join(LOG_FILE);
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("opening {}", log_path.display()))?;
    let exe = std::env::current_exe().context("locating the ant binary")?;
    let mut cmd = Command::new(exe);
    cmd.arg("serve")
        .arg("--local")
        .arg(&dir)
        .arg("--grpc-listen")
        .arg(a.grpc_listen.to_string())
        .arg("--http-listen")
        .arg(a.http_listen.to_string())
        .args(&a.serve_args)
        .stdin(Stdio::null())
        .stdout(log.try_clone().context("log file")?)
        .stderr(log)
        // Its own process group: a Ctrl-C in this terminal does not stop it.
        .process_group(0);
    let mut child = cmd.spawn().context("starting ant serve")?;
    let pid = child.id();
    let (grpc, http) = (url(&a.grpc_listen), url(&a.http_listen));
    let pid_path = dir.join(PID_FILE);
    let mut f = std::fs::File::create(&pid_path)
        .with_context(|| format!("writing {}", pid_path.display()))?;
    write!(f, "{pid}\n{grpc}\n{http}\n").context("writing the pid file")?;
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(st) = child.try_wait().context("waiting for ant serve")? {
            let _ = std::fs::remove_file(&pid_path);
            bail!("the server exited at once ({st}); see {}", log_path.display());
        }
        if tokio::net::TcpStream::connect(a.grpc_listen).await.is_ok() {
            break;
        }
        if Instant::now() > deadline {
            bail!("the server did not listen within {WAIT:?}; see {}", log_path.display());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut out = String::new();
    writeln!(out, "started {pid} {grpc} {http}")?;
    Ok(Outcome::ok(out))
}

/// SIGTERM the server and wait for it to release the directory.
async fn stop(dir: &Path) -> Result<Outcome> {
    let (pid, _, _) = match state(dir)? {
        State::Stopped => return Ok(Outcome::ok("stopped\n")),
        State::Busy => bail!(
            "local data directory {} is used by a process `ant local start` did not start",
            dir.display()
        ),
        State::Running(pid, grpc, http) => (pid, grpc, http),
    };
    let pid_t = libc::pid_t::try_from(pid).context("pid out of range")?;
    // SAFETY: kill only sends a signal.
    if unsafe { libc::kill(pid_t, libc::SIGTERM) } != 0 {
        return Err(std::io::Error::last_os_error()).context(format!("signalling {pid}"));
    }
    let deadline = Instant::now() + WAIT;
    while !matches!(state(dir)?, State::Stopped) {
        if Instant::now() > deadline {
            bail!("server {pid} did not stop within {WAIT:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = std::fs::remove_file(dir.join(PID_FILE));
    Ok(Outcome::ok(format!("stopped {pid}\n")))
}
