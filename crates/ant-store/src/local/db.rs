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
//!   share it. A checkpoint syncs the WAL before it copies frames into the
//!   database, and the WAL only starts over after a checkpoint that synced
//!   the database, so committed data is always in a synced WAL or a synced
//!   database. Non-durable jobs are answered right after the commit; see
//!   `super` for which writes those are and why that is sound. A job may
//!   also be answered at commit *and* once an fsync covers it
//!   ([`Db::write_pipelined`]: a land's publish).
//! - **Checkpoints.** SQLite's automatic checkpoint is off. The writer
//!   learns the WAL's length after each commit (`sqlite3_wal_hook`) and
//!   nudges the *checkpointer* thread every [`CHECKPOINT_BYTES`] of growth:
//!   a passive checkpoint on its own connection, then an `fdatasync` of the
//!   database (SQLite syncs the database only when a checkpoint copied the
//!   whole WAL, never the case beside a busy writer). At [`RESTART_BYTES`]
//!   the writer restarts the WAL ([`restart_wal`]) right after such a
//!   checkpoint: it copies and syncs only the few frames committed since,
//!   and waits for the reads in flight, not for a gap in the read stream.
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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Called once an fsync covers the job's commit (a job answered at
    /// commit that also wants to know when it is durable).
    synced: Option<Completion>,
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
    /// WAL growth between passive checkpoints, bytes.
    pub checkpoint_bytes: u64,
    /// WAL length at which it is restarted, bytes.
    pub restart_bytes: u64,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            readers: 4,
            cache_kib: 8 * 1024,
            no_fsync: false,
            checkpoint_bytes: CHECKPOINT_BYTES,
            restart_bytes: RESTART_BYTES,
        }
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
    permits: Arc<Semaphore>,
    maintenance_permits: Arc<Semaphore>,
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

/// Busy handler of the write connection: 100 µs steps for the first 10 ms,
/// then 1 ms steps, giving up after about 30 s (the readers' timeout).
fn writer_busy(attempt: i32) -> bool {
    const FINE: i32 = 100;
    let step = if attempt < FINE { 100 } else { 1000 };
    std::thread::sleep(std::time::Duration::from_micros(step));
    attempt < FINE + 30_000
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
        let checkpointer = {
            let (tx, rx) = mpsc::sync_channel::<()>(1);
            let ckpt = open_conn(
                path,
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                256,
            )?;
            let flags = std::sync::Arc::new(RestartFlags::default());
            let f = flags.clone();
            let db_file = path.to_owned();
            let thread = std::thread::Builder::new()
                .name("ant-sqlite-checkpoint".into())
                .spawn(move || checkpoint_loop(ckpt, &db_file, rx, &f))
                .map_err(be)?;
            Checkpointer { tx, thread, flags }
        };
        conn.busy_handler(Some(writer_busy)).map_err(sql_err)?;
        let page_size: i64 =
            conn.query_row("PRAGMA page_size", [], |r| r.get(0)).map_err(sql_err)?;
        // The WAL's length after each commit, for checkpoint scheduling
        // (the file's size says nothing: it is reused from its start).
        conn.wal_hook(Some(wal_pages_hook));
        let readers_max = u32::try_from(options.readers.clamp(1, 1 << 16)).unwrap_or(1);
        let permits = Arc::new(Semaphore::new(readers_max as usize));
        let maintenance_permits = Arc::new(Semaphore::new(MAINTENANCE_READERS));
        let readers = Readers {
            permits: permits.clone(),
            max: readers_max,
            maintenance: maintenance_permits.clone(),
        };
        let page_size = u64::try_from(page_size).unwrap_or(4096).max(512);
        let schedule = WalSchedule::new(page_size, options.checkpoint_bytes, options.restart_bytes);
        let (tx, rx) = mpsc::channel::<Job>();
        let thread = std::thread::Builder::new()
            .name("ant-sqlite-writer".into())
            .spawn(move || writer_loop(conn, rx, feed, syncer, checkpointer, readers, schedule))
            .map_err(be)?;
        Ok(Self {
            path: path.to_owned(),
            writer: Mutex::new(Writer { tx: Some(tx), thread: Some(thread) }),
            readers: Mutex::new(Vec::new()),
            permits,
            maintenance_permits,
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
        self.submit(durable, false, None, f).await
    }

    /// [`Self::write`] answered at commit, plus a receiver answered once an
    /// fsync covers that commit (at once without fsyncs, `no_fsync`): one
    /// job instead of a write and an empty durable write behind it.
    pub(crate) async fn write_pipelined<R, F>(
        &self,
        f: F,
    ) -> Result<(R, oneshot::Receiver<Result<()>>)>
    where
        R: Send + 'static,
        F: FnOnce(&Connection, &mut Effects) -> Result<R> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let synced: Completion = Box::new(move |r| {
            let _ = tx.send(r);
        });
        Ok((self.submit(false, false, Some(synced), f).await?, rx))
    }

    /// Run `f` on the write connection alone, outside any transaction, after
    /// the writes queued before it (maintenance such as `VACUUM`; `f` opens
    /// its own transactions if it needs them).
    pub(crate) async fn write_raw<R, F>(&self, f: F) -> Result<R>
    where
        R: Send + 'static,
        F: FnOnce(&Connection) -> Result<R> + Send + 'static,
    {
        self.submit(true, true, None, move |c, _| f(c)).await
    }

    async fn submit<R, F>(
        &self,
        durable: bool,
        raw: bool,
        synced: Option<Completion>,
        f: F,
    ) -> Result<R>
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
            Some(tx) => tx.send(Job { durable, raw, run, synced }).is_ok(),
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
    checkpointer: Checkpointer,
    readers: Readers,
    mut wal: WalSchedule,
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
        match wal.after_commit(WAL_PAGES.get() * wal.page_size) {
            WalAction::None => {}
            // Coalescing: a nudge while one is pending is dropped (the
            // pending one runs after this commit and sees the request).
            WalAction::Checkpoint => {
                let _ = checkpointer.tx.try_send(());
            }
            WalAction::Restart => {
                checkpointer.flags.wanted.store(true, Ordering::Relaxed);
                let _ = checkpointer.tx.try_send(());
            }
        }
        if checkpointer.flags.ready.swap(false, Ordering::Relaxed) && !restart_wal(&conn, &readers)
        {
            checkpointer.flags.ready.store(true, Ordering::Relaxed);
        }
    }
    drop(sync_tx);
    if let Some((tx, thread)) = syncer {
        drop(tx);
        let _ = thread.join();
    }
    drop(checkpointer.tx);
    let _ = checkpointer.thread.join();
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
    let mut synced = Vec::new();
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
        synced.extend(job.synced);
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
        for done in completions.into_iter().map(|(_, d)| d).chain(synced) {
            done(Err(StoreError::InvalidArgument(format!("local store: commit: {msg}"))));
        }
        return;
    }
    let Some(sync) = sync else {
        // No fsyncs at all (`LocalSync::Normal`).
        for m in moves {
            feed.moved(m);
        }
        for done in completions.into_iter().map(|(_, d)| d).chain(synced) {
            done(Ok(()));
        }
        return;
    };
    let mut durable = synced;
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

/// WAL growth after which the checkpointer copies frames into the database.
const CHECKPOINT_BYTES: u64 = 16 * 1024 * 1024;
/// WAL length above which the writer restarts the WAL from its beginning.
const RESTART_BYTES: u64 = 64 * 1024 * 1024;

thread_local! {
    /// Frames in the WAL after the last commit on this thread (the writer's).
    static WAL_PAGES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// `sqlite3_wal_hook`: runs on the committing thread after each commit.
fn wal_pages_hook(_: &rusqlite::hooks::Wal, pages: std::ffi::c_int) -> rusqlite::Result<()> {
    WAL_PAGES.set(u64::try_from(pages).unwrap_or(0));
    Ok(())
}

/// What the writer does about the WAL after a commit.
#[derive(Debug, PartialEq, Eq)]
enum WalAction {
    None,
    /// Nudge the checkpointer (a passive checkpoint on its own thread).
    Checkpoint,
    /// Restart the WAL on the writer (see [`restart_wal`]), after a
    /// passive checkpoint (see [`RestartFlags`]).
    Restart,
}

/// The reader pool's permits, as the writer sees them (see [`restart_wal`]).
struct Readers {
    /// Permits of maintenance reads (all [`MAINTENANCE_READERS`] of them).
    maintenance: Arc<Semaphore>,
    permits: Arc<Semaphore>,
    max: u32,
}

/// The checkpointer thread and how the writer talks to it.
struct Checkpointer {
    /// Nudges: run a passive checkpoint.
    tx: mpsc::SyncSender<()>,
    thread: std::thread::JoinHandle<()>,
    flags: std::sync::Arc<RestartFlags>,
}

/// A WAL restart in two steps: the writer asks for it (`wanted`), the
/// checkpointer copies the WAL into the database first (passive, beside
/// the writer) and then tells the writer (`ready`), whose `RESTART` has
/// only the frames committed meanwhile left to copy.
#[derive(Default)]
struct RestartFlags {
    wanted: AtomicBool,
    ready: AtomicBool,
}

/// Checkpoint scheduling from the WAL's length after each commit: one
/// passive checkpoint per `checkpoint` bytes of growth ([`CHECKPOINT_BYTES`]),
/// one restart once it is `restart` bytes long ([`RESTART_BYTES`]; again
/// only after more growth if a restart could not shorten it).
#[derive(Debug)]
struct WalSchedule {
    /// Bytes per WAL frame's page.
    page_size: u64,
    checkpoint: u64,
    restart: u64,
    /// WAL length at the last checkpoint nudge.
    nudged: u64,
    /// WAL length at the last restart request.
    restarted: u64,
}

impl WalSchedule {
    fn new(page_size: u64, checkpoint: u64, restart: u64) -> Self {
        Self { page_size, checkpoint: checkpoint.max(1), restart, nudged: 0, restarted: 0 }
    }

    fn after_commit(&mut self, len: u64) -> WalAction {
        if len < self.nudged {
            // The WAL started over (a restart, or SQLite's own reset).
            (self.nudged, self.restarted) = (0, 0);
        }
        if len >= self.restart && len >= self.restarted + self.checkpoint {
            self.restarted = len;
            self.nudged = len;
            return WalAction::Restart;
        }
        if len >= self.nudged + self.checkpoint {
            self.nudged = len;
            return WalAction::Checkpoint;
        }
        WalAction::None
    }
}

/// Restart the WAL (between groups, on the write connection): SQLite only
/// starts writing the WAL from its beginning again once every frame is in
/// the database *and* no reader still uses the WAL, which a steady stream
/// of short reads never allows by itself; the WAL would grow without bound
/// (seen: 1.3 GB in 30 s of landing). `RESTART` waits for the readers of
/// the moment (milliseconds) and makes the next write start over; the file
/// is then cut back to `journal_size_limit`. Returns false when it should be
/// tried again after the next commit (a checkpoint or maintenance read ran).
fn restart_wal(conn: &Connection, readers: &Readers) -> bool {
    let started = std::time::Instant::now();
    // Maintenance reads (collection, repack) can be long: the restart waits
    // for none of them, it is tried again after the next commit.
    let Ok(_maintenance) = readers.maintenance.try_acquire_many(MAINTENANCE_READERS as u32) else {
        return false;
    };
    let checkpoint = |mode: &str| -> rusqlite::Result<(i64, i64, i64)> {
        conn.query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
    };
    // First the frames committed since the checkpointer's last copy (with
    // the writer here, SQLite also syncs the database), with reads going on.
    let r = checkpoint("PASSIVE").and_then(|tail| {
        if tail.1 < 0 {
            // The checkpointer is running one (its lock is not waited for).
            return Ok(tail);
        }
        let copied = started.elapsed();
        // Then every reader permit: reads in flight finish, new ones wait
        // (the semaphore is fair), so no reader holds the WAL and the
        // restart does not poll for a gap in a steady stream of reads.
        let _quiet = futures::executor::block_on(readers.permits.acquire_many(readers.max));
        let r = checkpoint("RESTART");
        tracing::debug!(
            frames = tail.1,
            copy_ms = copied.as_millis() as u64,
            "local store WAL restart, tail copied"
        );
        r
    });
    match r {
        Ok((busy, log, done)) => {
            tracing::debug!(
                busy,
                frames = log,
                checkpointed = done,
                ms = started.elapsed().as_millis() as u64,
                "local store WAL restart"
            );
            // `-1`: the checkpoint could not start; try again shortly.
            log >= 0
        }
        Err(error) => {
            tracing::warn!(%error, "local store WAL restart failed");
            true
        }
    }
}

/// The checkpointer: on each nudge from the writer (every
/// [`CHECKPOINT_BYTES`] of WAL growth), a passive checkpoint on its own
/// connection, so the writer never stops for one (SQLite's automatic
/// checkpoint is off), and a sync of the database; asked for a restart, it
/// then tells the writer (see [`RestartFlags`]).
fn checkpoint_loop(
    conn: Connection,
    db_path: &Path,
    rx: mpsc::Receiver<()>,
    restart: &RestartFlags,
) {
    let mut db_file: Option<std::fs::File> = None;
    while rx.recv().is_ok() {
        let wanted = restart.wanted.swap(false, Ordering::Relaxed);
        checkpoint(&conn, db_path, &mut db_file);
        if wanted {
            restart.ready.store(true, Ordering::Relaxed);
        }
    }
    let _ = conn.close();
}

/// One passive checkpoint, then a sync of the database file.
///
/// SQLite syncs the database only when a checkpoint copied the whole WAL,
/// which a checkpoint beside a busy writer never does: every page copied
/// here would wait for the restart's sync, with the writer stopped (tens of
/// ms per 64 MiB). Syncing here leaves the restart only the last few.
fn checkpoint(conn: &Connection, db_path: &Path, db_file: &mut Option<std::fs::File>) {
    let started = std::time::Instant::now();
    let r: rusqlite::Result<(i64, i64, i64)> =
        conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        });
    match r {
        Ok((_, log, done)) => {
            let copied = started.elapsed();
            let synced = sync_db(db_path, db_file);
            tracing::debug!(
                frames = log,
                checkpointed = done,
                copy_ms = copied.as_millis() as u64,
                ms = started.elapsed().as_millis() as u64,
                "local store checkpoint"
            );
            if let Err(error) = synced {
                tracing::warn!(%error, "local store: syncing the database failed");
            }
        }
        Err(error) => tracing::warn!(%error, "local store checkpoint failed"),
    }
}

/// `fdatasync` the database file (opened once; SQLite never replaces it).
/// Only a speed-up: correctness never depends on it (see [`checkpoint_loop`]).
fn sync_db(path: &Path, file: &mut Option<std::fs::File>) -> std::io::Result<()> {
    if file.is_none() {
        *file = Some(std::fs::File::open(path)?);
    }
    match file {
        Some(f) => f.sync_data(),
        None => Ok(()),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoints_follow_wal_growth_and_restarts_its_length() {
        let mut s = WalSchedule::new(4096, 10, 40);
        let actions: Vec<WalAction> = [0, 5, 10, 12, 21, 30, 39, 40, 45, 50, 55, 3, 9, 13]
            .into_iter()
            .map(|l| s.after_commit(l))
            .collect();
        use WalAction::{Checkpoint as C, None as N, Restart as R};
        assert_eq!(actions, [N, N, C, N, C, N, C, R, N, R, N, N, N, C]);
    }

    fn open(dir: &Path, options: DbOptions) -> Db {
        Db::open(&dir.join("t.db"), options, RefFeed::default(), |c| {
            c.execute_batch("CREATE TABLE IF NOT EXISTS t (k INTEGER PRIMARY KEY, v BLOB NOT NULL)")
                .map_err(sql_err)
        })
        .unwrap()
    }

    /// Under a steady stream of writes and reads the WAL is checkpointed
    /// and restarted by length (its file is reused, so its size says
    /// nothing): it stays near the restart length, and nothing is lost.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_wal_stays_bounded_under_writes_and_reads() {
        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions {
            checkpoint_bytes: 64 << 10,
            restart_bytes: 256 << 10,
            ..DbOptions::default()
        };
        let db = Arc::new(open(dir.path(), options));
        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (db, stop) = (db.clone(), stop.clone());
                tokio::spawn(async move {
                    while !stop.load(Ordering::Relaxed) {
                        db.read(|c| {
                            c.query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0))
                                .map_err(sql_err)
                        })
                        .await
                        .unwrap();
                    }
                })
            })
            .collect();
        let wal = dir.path().join("t.db-wal");
        let mut largest = 0;
        for round in 0..40i64 {
            let writes = (0..16i64).map(|j| {
                db.write(j % 2 == 0, move |c, _| {
                    c.execute("INSERT INTO t (k, v) VALUES (?1, zeroblob(4000))", [round * 16 + j])
                        .map_err(sql_err)
                })
            });
            futures::future::try_join_all(writes).await.unwrap();
            largest = largest.max(std::fs::metadata(&wal).map_or(0, |m| m.len()));
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.await.unwrap();
        }
        // 640 rows of 4 KB (2.6 MB of pages) went through a 256 KiB WAL.
        assert!(largest < 2 << 20, "WAL file grew to {largest} bytes");
        let count = |c: &Connection| {
            c.query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0)).map_err(sql_err)
        };
        assert_eq!(db.read(count).await.unwrap(), 640);
    }

    /// A pipelined write is answered at commit and its receiver once an
    /// fsync covers it (at once without fsyncs); a failing one is answered
    /// with its error.
    #[tokio::test]
    async fn pipelined_writes_are_answered_then_synced() {
        for no_fsync in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = open(dir.path(), DbOptions { no_fsync, ..DbOptions::default() });
            let insert = |c: &Connection, _: &mut Effects| {
                c.execute("INSERT INTO t (k, v) VALUES (1, x'00')", []).map_err(sql_err)
            };
            let (n, synced) = db.write_pipelined(insert).await.unwrap();
            assert_eq!(n, 1);
            synced.await.unwrap().unwrap();
            assert!(db.write_pipelined(insert).await.is_err(), "duplicate key");
        }
    }
}
