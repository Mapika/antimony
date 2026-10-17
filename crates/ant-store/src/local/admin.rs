//! The maintenance socket of a local-mode server, and scheduled collection.
//!
//! One process owns a data directory (its `LOCK`), so maintenance of a
//! directory a server uses runs inside that server. The server listens on
//! a Unix socket in the directory, `<dir>/admin.sock` (mode 0600: whoever
//! may use the socket may already read and write the directory), and
//! `ant admin gc --local` / `ant admin repack --local` connect to it when a
//! server holds the directory, or open the directory themselves when none
//! does.
//!
//! # Protocol (text lines)
//!
//! The client sends one request line, `gc <key=value>...` or `repack
//! <key=value>...` (unknown keys are refused, so an older server never
//! silently ignores a setting). The server answers with any number of
//! `progress <text>` lines, then one `report <key=value>...` line
//! ([`GcReport::encode`], [`RepackReport::encode`]) or `error <message>`,
//! and closes the connection. A client that disconnects cancels its run
//! (a cancelled collection leaves nothing condemned; a cancelled repack
//! leaves at worst an unreferenced pack). Runs wait for each other
//! (`LocalStore::maintenance`).

use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use super::LocalStore;
use super::gc::{GcOptions, GcReport, collect};
use super::repack::{RepackOptions, RepackReport, repack};
use crate::{Result, StoreError};

/// Name of the maintenance socket inside a data directory.
pub const SOCKET_FILE: &str = "admin.sock";
/// Longest request line accepted.
const MAX_REQUEST: usize = 4096;

fn io(e: std::io::Error) -> StoreError {
    StoreError::backend(e)
}

/// Run `f` with a path naming `<dir>/admin.sock` that fits a Unix socket
/// address (at most 107 bytes): the path itself, or one through
/// `/proc/self/fd` of an open handle on `dir`.
fn with_socket_path<T>(
    dir: &Path,
    f: impl FnOnce(&Path) -> std::io::Result<T>,
) -> std::io::Result<T> {
    let path = dir.join(SOCKET_FILE);
    if path.as_os_str().len() < 100 {
        return f(&path);
    }
    let handle = std::fs::File::open(dir)?;
    let short = PathBuf::from(format!("/proc/self/fd/{}/{SOCKET_FILE}", handle.as_raw_fd()));
    let out = f(&short);
    drop(handle);
    out
}

/// A listening maintenance socket; stops listening and removes the socket
/// file when dropped.
pub struct AdminServer {
    task: tokio::task::JoinHandle<()>,
    path: PathBuf,
}

impl Drop for AdminServer {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.path);
    }
}

impl AdminServer {
    /// Path of the socket.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Listen on `<dir>/admin.sock` of `store` (replacing a stale socket file:
/// the store holds the directory's lock, so no other server listens there).
pub fn serve(store: LocalStore) -> Result<AdminServer> {
    let dir = store.dir().to_owned();
    let path = dir.join(SOCKET_FILE);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(io(e)),
    }
    let listener = with_socket_path(&dir, |p| {
        let l = std::os::unix::net::UnixListener::bind(p)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600))?;
        Ok(l)
    })
    .map_err(|e| {
        StoreError::backend(std::io::Error::new(
            e.kind(),
            format!("listening on {}: {e}", path.display()),
        ))
    })?;
    listener.set_nonblocking(true).map_err(io)?;
    let listener = UnixListener::from_std(listener).map_err(io)?;
    let task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((conn, _)) => {
                    let store = store.clone();
                    tokio::spawn(async move {
                        if let Err(error) = handle(store, conn).await {
                            tracing::debug!(%error, "maintenance socket connection ended");
                        }
                    });
                }
                Err(error) => {
                    tracing::warn!(%error, "maintenance socket accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    });
    Ok(AdminServer { task, path })
}

/// A parsed request.
enum Request {
    Gc(GcOptions),
    Repack(RepackOptions),
}

fn parse_kv(rest: &str) -> std::result::Result<HashMap<String, String>, String> {
    rest.split_whitespace()
        .map(|kv| {
            kv.split_once('=')
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .ok_or_else(|| format!("malformed setting {kv:?}"))
        })
        .collect()
}

fn take<T: std::str::FromStr>(
    kv: &mut HashMap<String, String>,
    key: &str,
) -> std::result::Result<Option<T>, String> {
    match kv.remove(key) {
        None => Ok(None),
        Some(v) => v.parse().map(Some).map_err(|_| format!("bad value of {key}: {v:?}")),
    }
}

fn flag(kv: &mut HashMap<String, String>, key: &str) -> std::result::Result<Option<bool>, String> {
    Ok(take::<u8>(kv, key)?.map(|v| v != 0))
}

fn parse(line: &str) -> std::result::Result<Request, String> {
    let (verb, rest) = line.trim().split_once(' ').unwrap_or((line.trim(), ""));
    let mut kv = parse_kv(rest)?;
    let req = match verb {
        "gc" => {
            let mut o = GcOptions::default();
            if let Some(v) = flag(&mut kv, "dry_run")? {
                o.dry_run = v;
            }
            if let Some(v) = take::<u64>(&mut kv, "grace")? {
                o.grace = Duration::from_secs(v);
            }
            if let Some(v) = take::<u64>(&mut kv, "quiesce")? {
                o.quiesce = Duration::from_secs(v);
            }
            if let Some(v) = take::<f64>(&mut kv, "compact_ratio")? {
                o.compact_ratio = v;
            }
            Request::Gc(o)
        }
        "repack" => {
            let mut o = RepackOptions::default();
            if let Some(v) = flag(&mut kv, "dry_run")? {
                o.dry_run = v;
            }
            if let Some(v) = take::<u64>(&mut kv, "min_age")? {
                o.min_age = Duration::from_secs(v);
            }
            if let Some(v) = take(&mut kv, "keep_recent")? {
                o.keep_recent = v;
            }
            if let Some(v) = flag(&mut kv, "all")? {
                o.all = v;
            }
            if let Some(v) = flag(&mut kv, "small")? {
                o.small = v;
            }
            if let Some(v) = take(&mut kv, "window")? {
                o.cluster.window = v;
            }
            if let Some(v) = take(&mut kv, "depth")? {
                o.cluster.depth = v;
            }
            if let Some(v) = take(&mut kv, "record_bytes")? {
                o.cluster.cluster_raw = v;
            }
            if let Some(v) = take(&mut kv, "level")? {
                o.cluster.level = v;
            }
            if let Some(v) = take::<usize>(&mut kv, "jobs")? {
                o.jobs = v.max(1);
            }
            if let Some(v) = take(&mut kv, "max_packs")? {
                o.stop_after_packs = Some(v);
            }
            if let Some(v) = take(&mut kv, "pack_bytes")? {
                o.pack_bytes = v;
            }
            Request::Repack(o)
        }
        other => return Err(format!("unknown request {other:?}")),
    };
    if let Some(k) = kv.keys().next() {
        return Err(format!("unknown setting {k:?}"));
    }
    Ok(req)
}

/// The request line of a collection with `opts`.
pub fn gc_request(opts: &GcOptions) -> String {
    format!(
        "gc dry_run={} grace={} quiesce={} compact_ratio={}\n",
        u8::from(opts.dry_run),
        opts.grace.as_secs(),
        opts.quiesce.as_secs(),
        opts.compact_ratio
    )
}

/// The request line of a repack with `opts`.
pub fn repack_request(opts: &RepackOptions) -> String {
    let mut line = format!(
        "repack dry_run={} min_age={} keep_recent={} all={} small={} window={} depth={} \
         record_bytes={} level={} jobs={} pack_bytes={}",
        u8::from(opts.dry_run),
        opts.min_age.as_secs(),
        opts.keep_recent,
        u8::from(opts.all),
        u8::from(opts.small),
        opts.cluster.window,
        opts.cluster.depth,
        opts.cluster.cluster_raw,
        opts.cluster.level,
        opts.jobs,
        opts.pack_bytes
    );
    if let Some(n) = opts.stop_after_packs {
        line.push_str(&format!(" max_packs={n}"));
    }
    line.push('\n');
    line
}

/// Serve one connection: one request, its progress and its report.
async fn handle(store: LocalStore, conn: UnixStream) -> std::io::Result<()> {
    let (read, mut write) = conn.into_split();
    let mut read = BufReader::new(read);
    let mut line = String::new();
    let n = (&mut read).take(MAX_REQUEST as u64).read_line(&mut line).await?;
    if n == 0 || !line.ends_with('\n') {
        return write.write_all(b"error incomplete request\n").await;
    }
    let req = match parse(&line) {
        Ok(r) => r,
        Err(e) => return write.write_all(format!("error {e}\n").as_bytes()).await,
    };
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let progress = move |s: &str| {
        let _ = tx.send(s.replace('\n', " "));
    };
    let job = spawn_maintenance(async move {
        match req {
            Request::Gc(o) => {
                tracing::info!(dry_run = o.dry_run, "local collection requested");
                collect(&store, &o, &progress).await.map(|r| r.encode())
            }
            Request::Repack(o) => {
                tracing::info!(dry_run = o.dry_run, "local repack requested");
                repack(&store, &o, &progress).await.map(|r| r.encode())
            }
        }
    });
    // Dropped (the client went away, or this connection's task ended): the
    // job is cancelled with it.
    let mut job = AbortOnDrop(job);
    let run = async {
        match (&mut job.0).await {
            Ok(r) => r,
            Err(e) => Err(StoreError::backend(e)),
        }
    };
    tokio::pin!(run);
    // The client sends nothing more: a read returning means it went away.
    let mut rest = Vec::new();
    let mut tail = read.take(MAX_REQUEST as u64);
    let gone = tail.read_to_end(&mut rest);
    tokio::pin!(gone);
    let result = loop {
        tokio::select! {
            r = &mut run => break r,
            Some(p) = rx.recv() => {
                write.write_all(format!("progress {p}\n").as_bytes()).await?;
            }
            _ = &mut gone => {
                tracing::info!("maintenance client went away: run cancelled");
                return Ok(());
            }
        }
    };
    while let Ok(p) = rx.try_recv() {
        write.write_all(format!("progress {p}\n").as_bytes()).await?;
    }
    let out = match result {
        Ok(report) => {
            tracing::info!(%report, "local maintenance done");
            format!("report {report}\n")
        }
        Err(e) => {
            tracing::warn!(error = %e, "local maintenance failed");
            format!("error {}\n", e.to_string().replace('\n', " "))
        }
    };
    write.write_all(out.as_bytes()).await?;
    write.shutdown().await
}

/// Aborts a task when dropped.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Worker threads of the maintenance runtime.
const MAINTENANCE_WORKERS: usize = 2;

/// Run `fut` on the maintenance runtime of this process: worker threads (and
/// a blocking pool) of its own, so that the CPU work of a collection or
/// repack (walking trees, sorting, assembling packs) never occupies the
/// workers serving requests. If that runtime cannot start, on the current
/// one.
fn spawn_maintenance<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    static RT: std::sync::OnceLock<Option<tokio::runtime::Runtime>> = std::sync::OnceLock::new();
    let rt = RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(MAINTENANCE_WORKERS)
            .thread_name("ant-maintenance")
            .enable_all()
            .build()
            .map_err(|error| tracing::warn!(%error, "no maintenance runtime"))
            .ok()
    });
    match rt {
        Some(rt) => rt.spawn(fut),
        None => tokio::spawn(fut),
    }
}

/// Connect to the maintenance socket of the server using `dir`; `None` when
/// no server listens there.
pub async fn connect(dir: &Path) -> Result<Option<UnixStream>> {
    let stream = with_socket_path(dir, |p| std::os::unix::net::UnixStream::connect(p));
    match stream {
        Ok(s) => {
            s.set_nonblocking(true).map_err(io)?;
            Ok(Some(UnixStream::from_std(s).map_err(io)?))
        }
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            Ok(None)
        }
        Err(e) => Err(StoreError::backend(std::io::Error::new(
            e.kind(),
            format!("connecting to {}: {e}", dir.join(SOCKET_FILE).display()),
        ))),
    }
}

/// Send `request` over `conn`, pass the progress lines to `progress`, and
/// return the report line (without `report `).
pub async fn exchange(
    conn: UnixStream,
    request: &str,
    progress: &(dyn Fn(&str) + Send + Sync),
) -> Result<String> {
    let (read, mut write) = conn.into_split();
    write.write_all(request.as_bytes()).await.map_err(io)?;
    let mut lines = BufReader::new(read).lines();
    while let Some(line) = lines.next_line().await.map_err(io)? {
        if let Some(p) = line.strip_prefix("progress ") {
            progress(p);
        } else if let Some(r) = line.strip_prefix("report ") {
            return Ok(r.to_owned());
        } else if let Some(e) = line.strip_prefix("error ") {
            return Err(StoreError::InvalidArgument(format!("the server: {e}")));
        }
    }
    Err(StoreError::InvalidArgument("the server closed the maintenance connection".into()))
}

/// Run a collection in the server using `dir` (`None`: no server listens).
pub async fn remote_gc(
    dir: &Path,
    opts: &GcOptions,
    progress: &(dyn Fn(&str) + Send + Sync),
) -> Result<Option<GcReport>> {
    let Some(conn) = connect(dir).await? else { return Ok(None) };
    let line = exchange(conn, &gc_request(opts), progress).await?;
    Ok(Some(GcReport::decode(&line)))
}

/// Run a repack in the server using `dir` (`None`: no server listens).
pub async fn remote_repack(
    dir: &Path,
    opts: &RepackOptions,
    progress: &(dyn Fn(&str) + Send + Sync),
) -> Result<Option<RepackReport>> {
    let Some(conn) = connect(dir).await? else { return Ok(None) };
    let line = exchange(conn, &repack_request(opts), progress).await?;
    Ok(Some(RepackReport::decode(&line)))
}

/// Collect garbage in `store` every `every` (the first run one interval
/// after start), logging each run; the task runs until aborted.
pub fn schedule_gc(
    store: LocalStore,
    every: Duration,
    opts: GcOptions,
) -> tokio::task::JoinHandle<()> {
    spawn_maintenance(async move {
        loop {
            tokio::time::sleep(every).await;
            let started = std::time::Instant::now();
            let progress = |line: &str| tracing::debug!(line, "scheduled collection");
            match collect(&store, &opts, &progress).await {
                Ok(r) => tracing::info!(
                    condemned = r.condemned,
                    objects = r.objects,
                    large_objects = r.large_objects,
                    commits = r.commits,
                    packs = r.packs,
                    packs_compacted = r.packs_compacted,
                    secs = started.elapsed().as_secs(),
                    "scheduled collection done"
                ),
                Err(error) => tracing::warn!(%error, "scheduled collection failed"),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_round_trip_and_unknown_settings_are_refused() {
        let g = GcOptions {
            dry_run: true,
            grace: Duration::from_secs(86_400 * 2),
            quiesce: Duration::from_secs(5),
            ..GcOptions::default()
        };
        let Ok(Request::Gc(p)) = parse(&gc_request(&g)) else { panic!() };
        assert_eq!((p.dry_run, p.grace, p.quiesce), (g.dry_run, g.grace, g.quiesce));
        let r = RepackOptions {
            all: true,
            min_age: Duration::ZERO,
            stop_after_packs: Some(3),
            jobs: 2,
            ..RepackOptions::default()
        };
        let Ok(Request::Repack(p)) = parse(&repack_request(&r)) else { panic!() };
        assert_eq!(
            (p.all, p.min_age, p.stop_after_packs, p.jobs, p.cluster.level),
            (true, Duration::ZERO, Some(3), 2, r.cluster.level)
        );
        assert!(parse("gc bogus=1\n").is_err());
        assert!(parse("gc grace=x\n").is_err());
        assert!(parse("vacuum\n").is_err());
        assert!(parse("gc dry_run\n").is_err());
    }
}
