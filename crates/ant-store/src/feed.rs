//! Ref-move notifications ([`RefStore::watch_refs`](crate::RefStore::watch_refs)).
//!
//! Stores that support them announce every ref move they commit on an
//! in-process [`RefFeed`] (a bounded `tokio::sync::broadcast`):
//! - the in-memory store sends from inside `cas_ref`, under its map lock,
//!   so notices arrive exactly in commit order;
//! - Postgres sends the notice in-process right after the move commits and
//!   queues a `pg_notify` on [`PG_CHANNEL`] for a notifier task (a separate,
//!   non-durable transaction: notifying inside the move's own transaction
//!   would serialize every ref move's commit, see `postgres::notify`). A
//!   `LISTEN` connection relays every replica's notices (this one's
//!   included, as duplicates) into the feed; after each (re)connect it sends
//!   [`RefNotice::Resync`], because notices sent while it was not listening
//!   are lost.
//!
//! Notices are therefore hints, not a log: they can arrive twice, out of
//! order (in-process vs relayed, or two replicas' notifiers) or not at all
//! (a lagging receiver, a lost listener connection, a crash between a move
//! and its notify). Consumers keep their own view of a ref, apply a
//! notice only when its `old` is the tip they know, skip notices whose
//! `nonce` they have seen, and otherwise (and on `Resync` or a lagged
//! receiver) re-read the ref (`ant-server` `ops::watch`).

use std::sync::Arc;

use ant_core::ObjectId;
use tokio::sync::broadcast;

/// Notices buffered per receiver before it lags (and must re-read refs).
pub const REF_FEED_CAPACITY: usize = 4096;

/// The Postgres `NOTIFY` channel of ref moves (one per database; the payload
/// names the schema).
pub const PG_CHANNEL: &str = "ant_ref_moves";

/// Who moved a ref.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveCause {
    /// A land publish (inside [`landing`]).
    Land,
    /// Anything else: `UpdateRef`, imports, tools.
    Update,
}

impl MoveCause {
    fn as_str(self) -> &'static str {
        match self {
            MoveCause::Land => "land",
            MoveCause::Update => "update",
        }
    }
}

/// One committed ref move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefMove {
    pub name: String,
    /// `None`: the move created the ref.
    pub old: Option<ObjectId>,
    /// `None`: the move deleted the ref.
    pub new: Option<ObjectId>,
    pub cause: MoveCause,
    /// Random per move: tells a relayed duplicate from a new move.
    pub nonce: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefNotice {
    Moved(Arc<RefMove>),
    /// Moves may have gone unannounced: re-read the refs you follow.
    Resync,
}

tokio::task_local! {
    static LANDING: ();
}

/// Run `fut` as a land: ref moves it makes are announced as
/// [`MoveCause::Land`] (task-local, like `deferred_durability`).
pub async fn landing<F: std::future::Future>(fut: F) -> F::Output {
    LANDING.scope((), fut).await
}

/// The cause of a ref move made by the current task.
pub fn move_cause() -> MoveCause {
    if LANDING.try_with(|_| ()).is_ok() { MoveCause::Land } else { MoveCause::Update }
}

/// In-process fan-out of [`RefNotice`]s (clones share the channel).
#[derive(Clone)]
pub struct RefFeed {
    tx: broadcast::Sender<RefNotice>,
}

impl Default for RefFeed {
    fn default() -> Self {
        Self { tx: broadcast::channel(REF_FEED_CAPACITY).0 }
    }
}

impl std::fmt::Debug for RefFeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefFeed").field("receivers", &self.tx.receiver_count()).finish()
    }
}

impl RefFeed {
    pub fn subscribe(&self) -> broadcast::Receiver<RefNotice> {
        self.tx.subscribe()
    }

    /// Whether anyone listens (sending is pointless otherwise).
    pub fn has_receivers(&self) -> bool {
        self.tx.receiver_count() > 0
    }

    pub fn send(&self, notice: RefNotice) {
        // No receivers is fine: nobody watches.
        let _ = self.tx.send(notice);
    }

    pub fn moved(&self, m: RefMove) {
        if self.has_receivers() {
            self.send(RefNotice::Moved(Arc::new(m)));
        }
    }
}

/// A fresh move nonce.
pub fn nonce() -> u64 {
    rand::random()
}

fn hex_or_dash(id: Option<ObjectId>) -> String {
    id.map_or_else(|| "-".to_owned(), |id| id.to_hex())
}

fn parse_side(s: &str) -> Option<Option<ObjectId>> {
    if s == "-" { Some(None) } else { s.parse().ok().map(Some) }
}

/// The `pg_notify` payload of a move in `schema`:
/// `v1 <nonce> <cause> <old|-> <new|-> <ref> <schema>` (ref names contain
/// no spaces; the schema is the rest of the line).
pub fn encode_payload(schema: &str, m: &RefMove) -> String {
    format!(
        "v1 {:016x} {} {} {} {} {schema}",
        m.nonce,
        m.cause.as_str(),
        hex_or_dash(m.old),
        hex_or_dash(m.new),
        m.name
    )
}

/// Parse a payload; `None` for another schema, or a payload this version
/// does not understand (a newer format).
pub fn decode_payload(schema: &str, payload: &str) -> Option<RefMove> {
    let mut parts = payload.splitn(7, ' ');
    if parts.next()? != "v1" {
        return None;
    }
    let nonce = u64::from_str_radix(parts.next()?, 16).ok()?;
    let cause = match parts.next()? {
        "land" => MoveCause::Land,
        "update" => MoveCause::Update,
        _ => return None,
    };
    let old = parse_side(parts.next()?)?;
    let new = parse_side(parts.next()?)?;
    let name = parts.next()?.to_owned();
    (parts.next()? == schema).then_some(RefMove { name, old, new, cause, nonce })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> ObjectId {
        ObjectId([n; 32])
    }

    #[test]
    fn payloads_round_trip_and_filter_by_schema() {
        let m = RefMove {
            name: "ws/agent-1/x".into(),
            old: None,
            new: Some(id(7)),
            cause: MoveCause::Land,
            nonce: 0xfeed,
        };
        let p = encode_payload("odd schema", &m);
        assert_eq!(decode_payload("odd schema", &p), Some(m.clone()));
        assert_eq!(decode_payload("other", &p), None);
        let d = RefMove { old: Some(id(1)), new: None, cause: MoveCause::Update, ..m };
        assert_eq!(decode_payload("s", &encode_payload("s", &d)), Some(d));
        assert_eq!(decode_payload("s", "v2 whatever"), None);
        assert_eq!(decode_payload("s", "v1 zz land - - main s"), None);
    }

    #[tokio::test]
    async fn cause_is_task_local() {
        assert_eq!(move_cause(), MoveCause::Update);
        landing(async { assert_eq!(move_cause(), MoveCause::Land) }).await;
        assert_eq!(move_cause(), MoveCause::Update);
    }
}
