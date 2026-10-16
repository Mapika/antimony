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
//! - **Durability.** The database runs in WAL mode. A group commits with
//!   `synchronous = FULL` (the WAL is fsynced before the commit returns) if
//!   any of its jobs is durable, and with `NORMAL` (no fsync; the commit
//!   survives a crash of this process, not of the OS) if all of them were
//!   made inside [`crate::deferred_durability`]. The WAL is one append-only
//!   file, so a FULL commit's fsync also covers every NORMAL commit before
//!   it: a land's deferred writes become durable with its publish (see
//!   `super` for why that is the same argument as Postgres's).
//! - **Readers.** Up to `readers` connections, opened on first use and
//!   reused; each read runs on the blocking pool (`spawn_blocking`), never
//!   on an async worker. WAL readers never block the writer or each other.
//! - **Ref moves** a job makes are announced on the feed after its group
//!   commits, in commit order; a job that rolled back announces nothing.

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
}

impl Default for DbOptions {
    fn default() -> Self {
        Self { readers: 4, cache_kib: 8 * 1024 }
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
        // FULL until a group says otherwise; the WAL file is truncated back
        // after checkpoints that let it grow beyond 64 MiB.
        conn.execute_batch("PRAGMA synchronous = FULL; PRAGMA journal_size_limit = 67108864;")
            .map_err(sql_err)?;
        init(&conn)?;
        let (tx, rx) = mpsc::channel::<Job>();
        let thread = std::thread::Builder::new()
            .name("ant-sqlite-writer".into())
            .spawn(move || writer_loop(conn, rx, feed))
            .map_err(be)?;
        Ok(Self {
            path: path.to_owned(),
            writer: Mutex::new(Writer { tx: Some(tx), thread: Some(thread) }),
            readers: Mutex::new(Vec::new()),
            permits: Semaphore::new(options.readers.max(1)),
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
        let _permit = self.permits.acquire().await.map_err(be)?;
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

fn writer_loop(conn: Connection, rx: mpsc::Receiver<Job>, feed: RefFeed) {
    let mut synchronous_full = true;
    // A raw job met while collecting a group waits for the next round.
    let mut held: Option<Job> = None;
    loop {
        let first = match held.take() {
            Some(j) => j,
            None => match rx.recv() {
                Ok(j) => j,
                Err(_) => break,
            },
        };
        if first.raw {
            if !synchronous_full && conn.execute_batch("PRAGMA synchronous = FULL").is_ok() {
                synchronous_full = true;
            }
            let (_, done) = (first.run)(Some(&conn), &mut Effects::default());
            done(Ok(()));
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
        let durable = jobs.iter().any(|j| j.durable);
        if durable != synchronous_full {
            let pragma = if durable { "FULL" } else { "NORMAL" };
            if conn.execute_batch(&format!("PRAGMA synchronous = {pragma}")).is_ok() {
                synchronous_full = durable;
            }
        }
        run_group(&conn, jobs, &feed);
    }
    // Last connection to close checkpoints the WAL into the database.
    let _ = conn.close();
}

fn run_group(conn: &Connection, jobs: Vec<Job>, feed: &RefFeed) {
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
        completions.push(done);
    }
    match conn.execute_batch("COMMIT") {
        Ok(()) => {
            for m in moves {
                feed.moved(m);
            }
            for done in completions {
                done(Ok(()));
            }
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            let msg = e.to_string();
            for done in completions {
                done(Err(StoreError::InvalidArgument(format!("local store: commit: {msg}"))));
            }
        }
    }
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").field("path", &self.path).finish()
    }
}
