//! Ref-move notifications for [`PgStore`](super::PgStore) (see
//! [`crate::feed`]).
//!
//! After a ref move commits (`cas_ref`, `RefLock::publish`), the store sends
//! it to its in-process feed and queues a `pg_notify('ant_ref_moves',
//! <payload>)` for the notifier task, which sends the queued payloads in
//! order, a batch per statement, from the query pool with
//! `synchronous_commit = off`. Only committed moves are therefore announced,
//! and each replica's own announcements stay in move order.
//!
//! **Why not notify inside the moving transaction?** Postgres serializes the
//! commits of all notifying transactions of a database, WAL flush included
//! (`NOTIFY` holds a database-wide lock from pre-commit to the end of the
//! commit), which defeats group commit: measured with 8 refs moving at once,
//! ~1,200 durable moves/s without a notify, ~300/s with one in the move's
//! own statement, ~1,300/s with a separate non-durable notify transaction.
//! The cost of the separate transaction is that a crash between a move's
//! commit and its notify loses the announcement (as does a full notifier
//! queue, or a failed notify statement): watchers then learn of the move
//! from the next announced move of that ref (its `old` is not their tip, so
//! they re-read the ref) or from their periodic re-read. Announcements of
//! different replicas may also arrive slightly out of move order; watchers
//! re-read the ref then too.
//!
//! The first [`PgFeed::subscribe`] starts a `LISTEN` task on a dedicated
//! connection (application name `ant-refs/<schema>`, outside both pools)
//! that relays notices of this schema into the feed and sends `Resync`
//! after every (re)connect. Both tasks end with the last clone of the store.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::Mutex;
use sqlx::postgres::{PgConnectOptions, PgListener, PgPoolOptions};
use tokio::sync::{broadcast, mpsc};

use crate::feed::{self, PG_CHANNEL, RefFeed, RefMove, RefNotice};

/// Longest wait between listener reconnect attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(5);
/// Announcements waiting for the notifier; more are dropped (watchers
/// re-read the ref instead).
const NOTIFY_QUEUE: usize = 4096;
/// Announcements sent per notify statement.
const NOTIFY_BATCH: usize = 256;

/// Application name of a schema's listener connection (at most 63 bytes,
/// Postgres truncates longer names).
pub fn listener_application_name(schema: &str) -> String {
    let mut name = format!("ant-refs/{schema}");
    while name.len() > 63 {
        name.pop();
    }
    name
}

pub(crate) struct PgFeed {
    feed: RefFeed,
    url: String,
    schema: String,
    /// The query pool, for the notifier.
    pool: sqlx::PgPool,
    listener: Mutex<Option<tokio::task::AbortHandle>>,
    /// Started with the first announced move.
    notifier: OnceLock<mpsc::Sender<String>>,
}

impl Drop for PgFeed {
    fn drop(&mut self) {
        // The notifier ends by itself once its queue is closed and drained.
        if let Some(task) = self.listener.lock().take() {
            task.abort();
        }
    }
}

impl PgFeed {
    pub(crate) fn new(url: &str, schema: &str, pool: sqlx::PgPool) -> Arc<Self> {
        Arc::new(Self {
            feed: RefFeed::default(),
            url: url.to_owned(),
            schema: schema.to_owned(),
            pool,
            listener: Mutex::new(None),
            notifier: OnceLock::new(),
        })
    }

    /// Announce a committed move: in-process now, to every replica through
    /// the notifier.
    pub(crate) fn committed(&self, m: RefMove) {
        let payload = feed::encode_payload(&self.schema, &m);
        self.feed.moved(m);
        let Ok(handle) = tokio::runtime::Handle::try_current() else { return };
        let queue = self.notifier.get_or_init(|| {
            let (tx, rx) = mpsc::channel(NOTIFY_QUEUE);
            handle.spawn(notify_loop(self.pool.clone(), self.schema.clone(), rx));
            tx
        });
        if queue.try_send(payload).is_err() {
            tracing::warn!(schema = self.schema, "ref notifier queue full; announcement dropped");
        }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<RefNotice> {
        let rx = self.feed.subscribe();
        let mut listener = self.listener.lock();
        if listener.is_none()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            let task =
                handle.spawn(listen(self.url.clone(), self.schema.clone(), self.feed.clone()));
            *listener = Some(task.abort_handle());
        }
        rx
    }
}

/// Send queued announcements, in order, a batch per statement, until the
/// queue closes.
async fn notify_loop(pool: sqlx::PgPool, schema: String, mut rx: mpsc::Receiver<String>) {
    let mut batch = Vec::with_capacity(NOTIFY_BATCH);
    while rx.recv_many(&mut batch, NOTIFY_BATCH).await > 0 {
        // Non-durable: an announcement is a hint (see the module docs), and
        // a durable commit would wait for a WAL flush per batch.
        let sent = sqlx::query(
            "WITH durability AS (SELECT set_config('synchronous_commit', 'off', true)) \
             SELECT pg_notify($1, p) FROM durability, unnest($2::text[]) AS t(p)",
        )
        .bind(PG_CHANNEL)
        .bind(&batch)
        .execute(&pool)
        .await;
        if let Err(error) = sent {
            tracing::warn!(%error, schema, moves = batch.len(), "announcing ref moves failed");
        }
        batch.clear();
    }
}

/// Relay notifications until aborted, reconnecting with backoff.
async fn listen(url: String, schema: String, feed: RefFeed) {
    let mut backoff = Duration::from_millis(100);
    loop {
        match connect(&url, &schema).await {
            Ok(mut listener) => {
                backoff = Duration::from_millis(100);
                // Moves made before LISTEN took effect were not relayed.
                feed.send(RefNotice::Resync);
                loop {
                    match listener.try_recv().await {
                        Ok(Some(n)) => {
                            if let Some(m) = feed::decode_payload(&schema, n.payload()) {
                                feed.send(RefNotice::Moved(Arc::new(m)));
                            }
                        }
                        // Reconnected (eagerly) after losing the connection.
                        Ok(None) => feed.send(RefNotice::Resync),
                        Err(error) => {
                            tracing::warn!(%error, schema, "ref listener failed; reconnecting");
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, schema, "ref listener cannot connect; retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

async fn connect(url: &str, schema: &str) -> Result<PgListener, sqlx::Error> {
    let options: PgConnectOptions =
        url.parse::<PgConnectOptions>()?.application_name(&listener_application_name(schema));
    // One connection, reused across reconnects by the listener.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .max_lifetime(None)
        .idle_timeout(None)
        .connect_lazy_with(options);
    let mut listener = PgListener::connect_with(&pool).await?;
    listener.ignore_pool_close_event(true);
    listener.listen(PG_CHANNEL).await?;
    Ok(listener)
}
