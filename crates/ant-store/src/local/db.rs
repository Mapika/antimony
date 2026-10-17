//! SQLite access for the local store: one writer thread with group commit,
//! a small pool of reader connections.
//!
//! - **Writer.** Every write is a job sent to one dedicated thread that owns
//!   the only write connection (SQLite allows one writer at a time anyway).
//!   The thread takes the first queued job plus whatever else is queued
//!   (up to [`MAX_GROUP`]) and runs them in one `BEGIN IMMEDIATE`
//!   transaction, each inside its own savepoint: a failing job rolls back
//!   alone, the others commit. Callers are answered only after the commit,
//!   so a returned write is visible to every later read.
//! - **Durability, pipelined.** The database runs in WAL mode with
//!   `synchronous = NORMAL`: a commit appends to the WAL without an fsync
//!   (it survives a crash of this process, not of the OS). Durable jobs are
//!   answered by a second thread, the *syncer*, only after an `fdatasync`
//!   of the WAL file that started after their commit; meanwhile the writer
//!   already runs the next group. One fsync covers every commit before it
//!   (the WAL is appended in commit order), so durable jobs of many groups
//!   share it. A checkpoint (SQLite's automatic one, every 1000 pages)
//!   syncs the WAL before it copies frames into the database and syncs the
//!   database afterwards, so committed data is always in a synced WAL or a
//!   synced database. Non-durable jobs are answered right after the
//!   commit; see `super` for which writes those are and why that is sound.
//! - **Readers.** Up to `readers` connections, opened on first use and
//!   reused; each read runs on the blocking pool (`spawn_blocking`), never
//!   on an async worker. WAL readers never block the writer or each other.
//!   Reads of maintenance runs (inside [`maintenance_reads`]) take up to
//!   [`MAINTENANCE_READERS`] connections of their own, so a collection or
//!   repack never queues the server's reads behind its own.
//! - **Ref moves** a job makes are announced on the feed once they are
//!   durable, in commit order; a job that rolled back announces nothing.

use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use parking_lot::Mutex;
use rusqlite::{Connection, OpenFlags};
use tokio::sync::{Semaphore, oneshot};

use crate::feed::{RefFeed, RefMove};
use crate::{Result, StoreError};

/// Jobs committed together at most.
pub(crate) const MAX_GROUP: usize = 256;
/// Reader connections of maintenance runs at most (beyond `readers`).
pub(crate) const MAINTENANCE_READERS: usize = 2;

tokio::task_local! {
    static MAINTENANCE: ();
}

/// Run `fut` with its reads counted as maintenance reads (see the module
/// docs).
pub(crate) async fn maintenance_reads<F: std::future::Future>(fut: F) -> F::Output {
    MAINTENANCE.scope((), fut).await
}

pub(crate) fn be(e: impl std::error::Error + Send + Sync + 'static) -> StoreError {
    StoreError::backend(e)
}

fn sql_err(e: rusqlite::Error) -> StoreError {
    StoreError::backend(e)
}

/// What a job wants done after its group commits.
#[derive(Default)]
pub(crate) struct Effects {
    pub moves: Vec<RefMove>,
}

/// Answer of one job, called with the group's commit result.
type Completion = Box<dyn FnOnce(Result<()>) + Send>;
/// A job: runs inside the group transaction (`None`: the transaction could
/// not be opened, the job fails without running); returns whether it
/// succeeded (its savepoint is released) or failed (rolled back), and its
/// completion.
type JobFn = Box<dyn FnOnce(Option<&Connection>, &mut Effects) -> (bool, Completion) + Send>;

struct Job {
    durable: bool,
    /// Runs alone, outside any transaction (`VACUUM`, maintenance).
    raw: bool,
    run: JobFn,
}

/// Tuning of a [`Db`].
#[derive(Debug, Clone)]
pub struct DbOptions {
    /// Reader connections at most.
    pub readers: usize,
    /// SQLite page cache per connection, in KiB.
    pub cache_kib: u32,
    /// Never fsync at commit (no syncer): see `LocalOptions::sync`.
    pub no_fsync: bool,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self { readers: 4, cache_kib: 8 * 1024, no_fsync: false }
    }
}

struct Writer {
    tx: Option<mpsc::Sender<Job>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for Writer {
    fn drop(&mut self) {
        // Closing the queue ends the thread after the jobs already queued;
        // joining makes the connection close (and checkpoint) before the
        // data directory's lock is released.
        drop(self.tx.take());
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

pub(crate) struct Db {
    path: PathBuf,
    writer: Mutex<Writer>,
    readers: Mutex<Vec<Connection>>,
    permits: Semaphore,
    maintenance_permits: Semaphore,
    options: DbOptions,
}

/// Settings of every connection.
fn configure(conn: &Connection, cache_kib: u32) -> Result<()> {
    conn.busy_timeout(std::time::Duration::from_secs(30)).map_err(sql_err)?;
    conn.execute_batch(&format!(
        "PRAGMA cache_size = -{cache_kib}; PRAGMA temp_store = MEMORY; PRAGMA foreign_keys = OFF;"
    ))
    .map_err(sql_err)?;
    Ok(())
}

fn open_conn(path: &Path, flags: OpenFlags, cache_kib: u32) -> Result<Connection> {
    let conn = Connection::open_with_flags(path, flags).map_err(sql_err)?;
    configure(&conn, cache_kib)?;
    Ok(conn)
}

impl Db {
    /// Open (creating) the database at `path`; `init` runs on the write
    /// connection before anything else (schema creation and migrations).
    pub(crate) fn open(
        path: &Path,
        options: DbOptions,
        feed: RefFeed,
        init: impl FnOnce(&Connection) -> Result<()>,
    ) -> Result<Self> {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let conn = open_conn(path, flags, options.cache_kib)?;
        let mode: String =
            conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0)).map_err(sql_err)?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(StoreError::InvalidArgument(format!(
                "{}: SQLite refused WAL mode (got {mode})",
                path.display()
            )));
        }
        // Commits never fsync themselves (the syncer does); the WAL file is
        // truncated back after checkpoints that let it grow beyond 64 MiB.
        conn.execute_batch(
            "PRAGMA synchronous = NORMAL; PRAGMA journal_size_limit = 67108864; \
             PRAGMA wal_autocheckpoint = 0;",
        )
        .map_err(sql_err)?;
        init(&conn)?;
        let wal = {
            let mut w = path.as_os_str().to_owned();
            w.push("-wal");
            PathBuf::from(w)
        };
        let syncer = if options.no_fsync {
            None
        } else {
            let (stx, srx) = mpsc::channel::<Synced>();
            let feed = feed.clone();
            let wal = wal.clone();
            let thread = std::thread::Builder::new()
                .name("ant-sqlite-sync".into())
                .spawn(move || sync_loop(wal, srx, feed))
                .map_err(be)?;
            Some((stx, thread))
        };
        let restart = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let checkpointer = {
            let (ctx, crx) = mpsc::sync_channel::<()>(1);
            let flag = restart.clone();
            let ckpt = open_conn(
                path,
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                256,
            )?;
            let wal = wal.clone();
            let thread = std::thread::Builder::new()
                .name("ant-sqlite-checkpoint".into())
                .spawn(move || checkpoint_loop(ckpt, &wal, crx, &flag))
                .map_err(be)?;
            (ctx, thread, restart)
        };
        let (tx, rx) = mpsc::channel::<Job>();
        let thread = std::thread::Builder::new()
            .name("ant-sqlite-writer".into())
            .spawn(move || writer_loop(conn, rx, feed, syncer, checkpointer))
            .map_err(be)?;
        Ok(Self {
            path: path.to_owned(),
            writer: Mutex::new(Writer { tx: Some(tx), thread: Some(thread) }),
            readers: Mutex::new(Vec::new()),
            permits: Semaphore::new(options.readers.max(1)),
            maintenance_permits: Semaphore::new(MAINTENANCE_READERS),
            options,
        })
    }

    /// Run `f` in the writer's next group transaction. `durable`: the group
    /// commits with an fsync (see the module docs).
    pub(crate) async fn write<R, F>(&self, durable: bool, f: F) -> Result<R>
    where
        R: Send + 'static,
        F: FnOnce(&Connection, &mut Effects) -> Result<R> + Send + 'static,
    {
        self.submit(durable, false, f).await
    }

    /// Run `f` on the write connection alone, outside any transaction, after
    /// the writes queued before it (maintenance such as `VACUUM`; `f` opens
    /// its own transactions if it needs them).
    pub(crate) async fn write_raw<R, F>(&self, f: F) -> Result<R>
    where
        R: Send + 'static,
        F: FnOnce(&Connection) -> Result<R> + Send + 'static,
    {
        self.submit(true, true, move |c, _| f(c)).await
    }

    async fn submit<R, F>(&self, durable: bool, raw: bool, f: F) -> Result<R>
    where
        R: Send + 'static,
        F: FnOnce(&Connection, &mut Effects) -> Result<R> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<Result<R>>();
        let run: JobFn = Box::new(move |conn, fx| {
            let r = match conn {
                None => Err(StoreError::InvalidArgument("local store: no transaction".into())),
                Some(conn) => match std::panic::catch_unwind(AssertUnwindSafe(|| f(conn, fx))) {
                    Ok(r) => r,
                    Err(_) => Err(StoreError::InvalidArgument("local store write panicked".into())),
                },
            };
            let ok = r.is_ok();
            let done: Completion = Box::new(move |commit: Result<()>| {
                let _ = tx.send(commit.and(r));
            });
            (ok, done)
        });
        let sent = match &self.writer.lock().tx {
            Some(tx) => tx.send(Job { durable, raw, run }).is_ok(),
            None => false,
        };
        if !sent {
            return Err(StoreError::InvalidArgument("local store is closed".into()));
        }
        rx.await.map_err(|_| StoreError::InvalidArgument("local store writer stopped".into()))?
    }

    /// Run `f` on a reader connection, on the blocking pool.
    pub(crate) async fn read<R, F>(&self, f: F) -> Result<R>
    where
        R: Send + 'static,
        F: FnOnce(&Connection) -> Result<R> + Send + 'static,
    {
        let permits = match MAINTENANCE.try_with(|_| ()) {
            Ok(()) => &self.maintenance_permits,
            Err(_) => &self.permits,
        };
        let _permit = permits.acquire().await.map_err(be)?;
        let pooled = self.readers.lock().pop();
        let path = self.path.clone();
        let cache = self.options.cache_kib;
        let (conn, r) = tokio::task::spawn_blocking(move || {
            let conn = match pooled {
                Some(c) => c,
                None => {
                    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
                    match open_conn(&path, flags, cache) {
                        Ok(c) => c,
                        Err(e) => return (None, Err(e)),
                    }
                }
            };
            let r = f(&conn);
            (Some(conn), r)
        })
        .await
        .map_err(be)?;
        if let Some(conn) = conn {
            self.readers.lock().push(conn);
        }
        r
    }
}

/// Durable completions (and the ref moves of their group) waiting for the
/// fsync of their commit.
struct Synced {
    done: Vec<Completion>,
    moves: Vec<RefMove>,
}

fn writer_loop(
    conn: Connection,
    rx: mpsc::Receiver<Job>,
    feed: RefFeed,
    syncer: Option<(mpsc::Sender<Synced>, std::thread::JoinHandle<()>)>,
    checkpointer: (
        mpsc::SyncSender<()>,
        std::thread::JoinHandle<()>,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
    ),
) {
    // A raw job met while collecting a group waits for the next round.
    let mut held: Option<Job> = None;
    let sync_tx = syncer.as_ref().map(|(tx, _)| tx.clone());
    loop {
        let first = match held.take() {
            Some(j) => j,
            None => match rx.recv() {
                Ok(j) => j,
                Err(_) => break,
            },
        };
        if first.raw {
            let (_, done) = (first.run)(Some(&conn), &mut Effects::default());
            match &sync_tx {
                Some(tx) => {
                    if let Err(mpsc::SendError(s)) =
                        tx.send(Synced { done: vec![done], moves: vec![] })
                    {
                        s.done.into_iter().for_each(|d| d(Ok(())));
                    }
                }
                None => done(Ok(())),
            }
            continue;
        }
        let mut jobs = vec![first];
        while jobs.len() < MAX_GROUP {
            match rx.try_recv() {
                Ok(j) if j.raw => {
                    held = Some(j);
                    break;
                }
                Ok(j) => jobs.push(j),
                Err(_) => break,
            }
        }
        run_group(&conn, jobs, &feed, sync_tx.as_ref());
        // Coalescing nudge: the checkpointer looks at the WAL's size.
        let _ = checkpointer.0.try_send(());
        if checkpointer.2.swap(false, std::sync::atomic::Ordering::Relaxed) {
            restart_wal(&conn);
        }
    }
    drop(sync_tx);
    if let Some((tx, thread)) = syncer {
        drop(tx);
        let _ = thread.join();
    }
    drop(checkpointer.0);
    let _ = checkpointer.1.join();
    // Last connection to close checkpoints the WAL into the database.
    let _ = conn.close();
}

/// A commit group slower than this is logged (with where the time went).
const SLOW_GROUP: std::time::Duration = std::time::Duration::from_millis(200);

fn run_group(
    conn: &Connection,
    jobs: Vec<Job>,
    feed: &RefFeed,
    sync: Option<&mpsc::Sender<Synced>>,
) {
    let started = std::time::Instant::now();
    let n = jobs.len();
    if let Err(e) = conn.execute_batch("BEGIN IMMEDIATE") {
        let msg = e.to_string();
        for job in jobs {
            // Run nothing; answer with the error.
            let (_, done) = (job.run)(None, &mut Effects::default());
            done(Err(StoreError::InvalidArgument(format!("local store: begin: {msg}"))));
        }
        return;
    }
    let mut completions = Vec::with_capacity(jobs.len());
    let mut moves = Vec::new();
    for job in jobs {
        let savepoint = conn.execute_batch("SAVEPOINT job");
        let mut fx = Effects::default();
        let (ok, done) = (job.run)(Some(conn), &mut fx);
        if savepoint.is_ok() {
            let end = if ok { "RELEASE job" } else { "ROLLBACK TO job; RELEASE job" };
            // A failed release leaves the transaction to the commit below.
            let _ = conn.execute_batch(end);
        }
        if ok {
            moves.extend(fx.moves);
        }
        completions.push((job.durable, done));
    }
    let ran = started.elapsed();
    let commit = conn.execute_batch("COMMIT");
    let total = started.elapsed();
    tracing::trace!(
        jobs = n,
        run_us = ran.as_micros() as u64,
        commit_us = (total - ran).as_micros() as u64,
        "local store commit group"
    );
    if total >= SLOW_GROUP {
        tracing::warn!(
            jobs = n,
            run_ms = ran.as_millis() as u64,
            commit_ms = (total - ran).as_millis() as u64,
            "slow local store commit group"
        );
    }
    if let Err(e) = commit {
        let _ = conn.execute_batch("ROLLBACK");
        let msg = e.to_string();
        for (_, done) in completions {
            done(Err(StoreError::InvalidArgument(format!("local store: commit: {msg}"))));
        }
        return;
    }
    let Some(sync) = sync else {
        // No fsyncs at all (`LocalSync::Normal`).
        for m in moves {
            feed.moved(m);
        }
        for (_, done) in completions {
            done(Ok(()));
        }
        return;
    };
    let mut durable = Vec::new();
    for (d, done) in completions {
        if d { durable.push(done) } else { done(Ok(())) }
    }
    if durable.is_empty() && moves.is_empty() {
        return;
    }
    if let Err(mpsc::SendError(s)) = sync.send(Synced { done: durable, moves }) {
        // The syncer is gone (it only stops after the writer): answer here.
        for done in s.done {
            done(Err(StoreError::InvalidArgument("local store: syncer stopped".into())));
        }
    }
}

/// The syncer: fsync the WAL for every batch of durable completions queued
/// since the last fsync, then answer them and announce their ref moves.
fn sync_loop(wal: PathBuf, rx: mpsc::Receiver<Synced>, feed: RefFeed) {
    let mut file: Option<(std::fs::File, u64)> = None;
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        while let Ok(more) = rx.try_recv() {
            batch.push(more);
        }
        let result = fsync_wal(&wal, &mut file);
        for s in batch {
            for m in s.moves {
                feed.moved(m);
            }
            for done in s.done {
                done(result.as_ref().map(|_| ()).map_err(|e| {
                    StoreError::InvalidArgument(format!("local store: fsync of the WAL: {e}"))
                }));
            }
        }
    }
}

/// WAL size above which the checkpointer copies frames into the database.
const CHECKPOINT_BYTES: u64 = 16 * 1024 * 1024;
/// WAL size above which the writer restarts the WAL from its beginning.
const RESTART_BYTES: u64 = 64 * 1024 * 1024;

/// Restart the WAL (between groups, on the write connection): SQLite only
/// starts writing the WAL from its beginning again once every frame is in
/// the database *and* no reader still uses the WAL, which a steady stream
/// of short reads never allows by itself; the WAL would grow without bound
/// (seen: 1.3 GB in 30 s of landing). `RESTART` waits for the readers of
/// the moment (milliseconds) and makes the next write start over; the file
/// is then cut back to `journal_size_limit`.
fn restart_wal(conn: &Connection) {
    let started = std::time::Instant::now();
    let r: rusqlite::Result<(i64, i64, i64)> =
        conn.query_row("PRAGMA wal_checkpoint(RESTART)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        });
    match r {
        Ok((busy, log, done)) => tracing::debug!(
            busy,
            frames = log,
            checkpointed = done,
            ms = started.elapsed().as_millis() as u64,
            "local store WAL restart"
        ),
        Err(error) => tracing::warn!(%error, "local store WAL restart failed"),
    }
}

/// The checkpointer: on each nudge from the writer, a passive checkpoint
/// when the WAL has grown past [`CHECKPOINT_BYTES`], on its own connection,
/// so the writer never stops for one (SQLite's automatic checkpoint is off).
/// A checkpoint syncs the WAL before copying and the database after.
fn checkpoint_loop(
    conn: Connection,
    wal: &Path,
    rx: mpsc::Receiver<()>,
    restart: &std::sync::atomic::AtomicBool,
) {
    while rx.recv().is_ok() {
        let size = std::fs::metadata(wal).map_or(0, |m| m.len());
        if size < CHECKPOINT_BYTES {
            continue;
        }
        let started = std::time::Instant::now();
        let r: rusqlite::Result<(i64, i64, i64)> =
            conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            });
        match r {
            Ok((_, log, done)) => {
                tracing::debug!(
                    frames = log,
                    checkpointed = done,
                    ms = started.elapsed().as_millis() as u64,
                    "local store checkpoint"
                );
                if size >= RESTART_BYTES {
                    restart.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
            Err(error) => tracing::warn!(%error, "local store checkpoint failed"),
        }
    }
    let _ = conn.close();
}

/// `fdatasync` the WAL file, (re)opening it when SQLite replaced it.
fn fsync_wal(path: &Path, file: &mut Option<(std::fs::File, u64)>) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let ino = match std::fs::metadata(path) {
        Ok(m) => m.ino(),
        // No WAL file: every commit has been checkpointed into the
        // database, which the checkpoint synced.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    if file.as_ref().is_none_or(|(_, i)| *i != ino) {
        *file = Some((std::fs::File::open(path)?, ino));
    }
    match file {
        Some((f, _)) => f.sync_data(),
        None => Ok(()),
    }
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").field("path", &self.path).finish()
    }
}
