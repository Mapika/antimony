//! Ref event streams (`WatchRef`): CI and other post-merge tooling learn
//! about new lands without polling.
//!
//! **Events.** A [`RefEvent`] moves the ref from `old` to `new` (`None`: the
//! ref did not exist / was deleted). `landed` lists the commits the move
//! added, oldest first: `new`'s first-parent chain down to, excluding, the
//! newest commit reachable from `old` (as `LogRange old..new`); for a
//! creation, `new`'s whole first-parent history. Kinds:
//! - [`EventKind::Land`]: one move made by `Land` (a group-commit batch: every
//!   land of the batch is in `landed`, in landing order);
//! - [`EventKind::Update`]: one fast-forward move by anything else
//!   (`UpdateRef`, imports), or a `coalesced` fast-forward;
//! - [`EventKind::Force`]: `old` is not an ancestor of `new` (a rewind or a
//!   move to unrelated history), or a deletion: `landed` is empty.
//!
//! `coalesced` events cover an unknown number of moves: the catch-up from
//! `from`, a gap the server filled from the ref itself, or a slow watcher's
//! backlog. Their kind is `Update` or `Force` only; `landed` is still exact.
//! One move adding more than [`MAX_EVENT_COMMITS`] commits is split into
//! consecutive events at first-parent commits.
//!
//! **Ordering and gaps.** Every stream is gapless: each event's `old` is the
//! previous event's `new` (the first one's `old` is `from`, or the tip when
//! the watch started). Per backend and ref, one hub task follows the ref:
//! it applies a store notice (`ant_store::feed`) only when its `old` is the
//! tip the hub knows, skips notices whose nonce it has seen (Postgres relays
//! this process's own moves again), and otherwise — an out-of-order or
//! missing notice, a lagging feed, a listener reconnect (`Resync`), or every
//! [`RESYNC_INTERVAL`] — re-reads the ref and emits one coalesced event from
//! its tip to the ref's value (nothing if unchanged). Each event is built
//! once per hub (one first-parent chain read for a land), then shared by
//! all of its watchers.
//!
//! **Backpressure.** The hub publishes into a ring of [`WATCH_BUFFER`]
//! events (per ref and process, shared by its watchers, each event at most
//! [`MAX_EVENT_COMMITS`] ids). A watcher's stream is pulled by its gRPC
//! response: a client that stops reading stops pulling, and once the ring
//! has moved on by more than its size the watcher has lagged; its next event
//! is one coalesced event from its last tip to the newest event's `new`,
//! built by its own range walk. Memory per watcher is therefore bounded no
//! matter how slow it is; a slow watcher gets fewer, larger events.
//!
//! **Access** (`watch_ref`): reading a ref's events needs
//! `Policy::can_read_ref` (a ref rule naming the caller under `read`,
//! `update` or `land`; admins always). Events carry commit ids only, which
//! (as with `Log`) reveal no paths or contents.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::Duration;

use ant_core::ObjectId;
use ant_store::feed::{MoveCause, RefNotice};
use ant_store::{Backend, StoreError, graph};
use futures::Stream;
use tokio::sync::{broadcast, mpsc, oneshot};

use super::{OpError, OpResult};
use crate::acl::Access;
use crate::metrics::Metrics;

/// Events buffered per ref hub before a watcher that has not pulled them
/// lags (and gets a coalesced event instead).
pub const WATCH_BUFFER: usize = 256;
/// Most commit ids in one event's `landed`.
pub const MAX_EVENT_COMMITS: usize = 4096;
/// How often a hub re-reads its ref even without notices (a safety net for
/// moves nobody announced).
pub const RESYNC_INTERVAL: Duration = Duration::from_secs(30);
/// Re-read interval of a ref whose store announces nothing.
const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Wait before retrying after a store error.
const RETRY: Duration = Duration::from_millis(200);
/// Longest wait for a hub to re-read its ref for a new watcher.
const SYNC_TIMEOUT: Duration = Duration::from_secs(30);
/// Notice nonces remembered per hub (duplicates arrive within this window).
const SEEN_NONCES: usize = 4096;
/// First-parent nodes read per round trip by a range walk (doubling).
const FIRST_CHUNK: usize = 256;
const MAX_CHUNK: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Land,
    Update,
    Force,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefEvent {
    pub old: Option<ObjectId>,
    pub new: Option<ObjectId>,
    /// Oldest first.
    pub landed: Vec<ObjectId>,
    pub kind: EventKind,
    pub coalesced: bool,
}

/// Knobs of a watch (tests use small buffers).
#[derive(Debug, Clone)]
pub struct WatchOptions {
    /// Ring size of the ref's hub; only the watcher that creates the hub
    /// sets it.
    pub buffer: usize,
}

impl Default for WatchOptions {
    fn default() -> Self {
        Self { buffer: WATCH_BUFFER }
    }
}

// ---------------------------------------------------------------------------
// range walks
// ---------------------------------------------------------------------------

/// Unrecorded commits (an admin's raw object upload) have no history to
/// walk: such moves count as force moves.
fn unknown_history(e: &StoreError) -> bool {
    matches!(e, StoreError::InvalidArgument(_) | StoreError::NotFound(_))
}

/// `to`'s first-parent chain, newest first, down to (excluding) `stop`;
/// gives up (returning `None`) at a commit of generation `<= floor` that is
/// not `stop`, a root or an unrecorded commit.
async fn chain_down_to(
    b: &Backend,
    to: ObjectId,
    stop: Option<ObjectId>,
    floor: u64,
) -> OpResult<Option<Vec<ObjectId>>> {
    let mut out = Vec::new();
    let mut next = to;
    let mut want = FIRST_CHUNK;
    loop {
        let chain = b.meta.first_parent_chain(next, stop, want).await?;
        let Some(last) = chain.last() else {
            return Ok(None);
        };
        for node in &chain {
            if stop.is_some() && node.generation <= floor {
                return Ok(None);
            }
            out.push(node.id);
        }
        match last.parents.first().copied() {
            Some(p) if Some(p) == stop => return Ok(Some(out)),
            // A root: the whole history (or `stop` is not on the chain).
            None => return Ok(stop.is_none().then_some(out)),
            Some(p) if chain.len() == want => next = p,
            // Stopped early: the parent is unrecorded.
            Some(_) => return Ok(None),
        }
        want = (want * 2).min(MAX_CHUNK);
    }
}

/// Commits `old..new` adds (oldest first), or `None` when `old` is not an
/// ancestor of `new`.
async fn landed_between(
    b: &Backend,
    old: Option<ObjectId>,
    new: ObjectId,
) -> OpResult<Option<Vec<ObjectId>>> {
    let Some(old) = old else {
        return Ok(chain_down_to(b, new, None, 0).await?.map(reversed));
    };
    if old == new {
        return Ok(Some(Vec::new()));
    }
    // Usually the previous event's tip: cached.
    let Some(old_node) = super::cache::get_nodes(b, &[old]).await?.pop().flatten() else {
        return Ok(None);
    };
    // The common case (lands, fast-forwards): `old` is on the chain.
    if let Some(chain) = chain_down_to(b, new, Some(old), old_node.generation).await? {
        return Ok(Some(reversed(chain)));
    }
    // Merged in through another parent?
    match graph::is_ancestor(b, old, new).await {
        Ok(true) => {}
        Ok(false) => return Ok(None),
        Err(e) if unknown_history(&e) => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let end = match graph::first_parent_range_end(b, old, new).await {
        Ok(end) => end,
        Err(e) if unknown_history(&e) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let floor = match end {
        Some(end) => b.meta.get_commits(&[end]).await?.pop().flatten().map_or(0, |n| n.generation),
        None => 0,
    };
    Ok(chain_down_to(b, new, end, floor).await?.map(reversed))
}

fn reversed(mut v: Vec<ObjectId>) -> Vec<ObjectId> {
    v.reverse();
    v
}

/// The events of moving `old` → `new` (empty if equal): one, or several
/// consecutive ones when `landed` is longer than [`MAX_EVENT_COMMITS`].
/// `cause`: the move's cause when it is one known move.
async fn events_between(
    b: &Backend,
    old: Option<ObjectId>,
    new: Option<ObjectId>,
    cause: Option<MoveCause>,
) -> OpResult<Vec<RefEvent>> {
    if old == new {
        return Ok(Vec::new());
    }
    let coalesced = cause.is_none();
    let force = |landed| RefEvent { old, new, landed, kind: EventKind::Force, coalesced };
    let Some(new_id) = new else { return Ok(vec![force(Vec::new())]) };
    let landed = match landed_between(b, old, new_id).await {
        Ok(Some(landed)) => landed,
        Ok(None) => return Ok(vec![force(Vec::new())]),
        Err(OpError::Store(e)) if unknown_history(&e) => return Ok(vec![force(Vec::new())]),
        Err(e) => return Err(e),
    };
    let kind = match cause {
        Some(MoveCause::Land) => EventKind::Land,
        _ => EventKind::Update,
    };
    let mut events = Vec::new();
    let mut from = old;
    let mut chunks = landed.chunks(MAX_EVENT_COMMITS).peekable();
    while let Some(chunk) = chunks.next() {
        let to = if chunks.peek().is_some() { chunk.last().copied() } else { new };
        events.push(RefEvent { old: from, new: to, landed: chunk.to_vec(), kind, coalesced });
        from = to;
    }
    if events.is_empty() {
        // A move to an ancestor reached via another parent: nothing added.
        events.push(RefEvent { old, new, landed: Vec::new(), kind, coalesced });
    }
    Ok(events)
}

// ---------------------------------------------------------------------------
// hubs
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Eq, Hash)]
struct HubKey {
    /// Identity (`Arc` address) of the backend's ref store.
    refs: usize,
    name: String,
}

struct Shared {
    /// The tip the last event moved to (the hub's view of the ref).
    tip: Mutex<Option<ObjectId>>,
    events: broadcast::Sender<Arc<RefEvent>>,
}

/// A ref's hub, shared by its watchers; the task stops with the last one.
struct Hub {
    key: HubKey,
    shared: Arc<Shared>,
    /// Sync requests: the hub re-reads the ref, publishes up to it, replies.
    sync: mpsc::UnboundedSender<oneshot::Sender<()>>,
    task: tokio::task::AbortHandle,
}

static HUBS: LazyLock<Mutex<HashMap<HubKey, Weak<Hub>>>> = LazyLock::new(Default::default);

impl Drop for Hub {
    fn drop(&mut self) {
        self.task.abort();
        let mut hubs = crate::lock(&HUBS);
        if hubs.get(&self.key).is_some_and(|w| w.strong_count() == 0) {
            hubs.remove(&self.key);
        }
    }
}

impl Hub {
    /// Wait until the hub has published up to (at least) the ref's value
    /// as of now.
    async fn sync(&self, name: &str) -> OpResult<()> {
        let (reply, done) = oneshot::channel();
        let synced = self.sync.send(reply).is_ok()
            && matches!(tokio::time::timeout(SYNC_TIMEOUT, done).await, Ok(Ok(())));
        if !synced {
            return Err(OpError::Aborted(format!("ref {name} cannot be read now")));
        }
        Ok(())
    }
}

fn hub_for(b: &Backend, name: &str, buffer: usize) -> Arc<Hub> {
    let key = HubKey { refs: Arc::as_ptr(&b.refs).cast::<()>() as usize, name: name.to_owned() };
    let mut hubs = crate::lock(&HUBS);
    if let Some(hub) = hubs.get(&key).and_then(Weak::upgrade) {
        return hub;
    }
    let shared =
        Arc::new(Shared { tip: Mutex::new(None), events: broadcast::channel(buffer.max(1)).0 });
    // Subscribed before the first read of the ref: no move is missed.
    let feed = b.refs.watch_refs();
    let (sync, requests) = mpsc::unbounded_channel();
    let task = tokio::spawn(follow(b.clone(), name.to_owned(), shared.clone(), feed, requests));
    let hub = Arc::new(Hub { key: key.clone(), shared, sync, task: task.abort_handle() });
    hubs.insert(key, Arc::downgrade(&hub));
    hub
}

/// Recently seen notice nonces.
#[derive(Default)]
struct Seen {
    order: VecDeque<u64>,
    set: HashSet<u64>,
}

impl Seen {
    /// False if `nonce` was seen already.
    fn insert(&mut self, nonce: u64) -> bool {
        if !self.set.insert(nonce) {
            return false;
        }
        self.order.push_back(nonce);
        if self.order.len() > SEEN_NONCES
            && let Some(old) = self.order.pop_front()
        {
            self.set.remove(&old);
        }
        true
    }
}

/// What woke the hub.
enum Wake {
    Notice(Result<RefNotice, broadcast::error::RecvError>),
    Sync(oneshot::Sender<()>),
    Timer,
}

/// What the hub does next.
enum Step {
    /// Apply one announced move from the current tip.
    Move(Option<ObjectId>, MoveCause),
    /// Re-read the ref.
    Resync,
    Nothing,
}

async fn recv(feed: &mut Option<broadcast::Receiver<RefNotice>>) -> Wake {
    match feed {
        Some(rx) => Wake::Notice(rx.recv().await),
        None => std::future::pending().await,
    }
}

/// The hub task: follow `name` and publish its events.
async fn follow(
    b: Backend,
    name: String,
    shared: Arc<Shared>,
    mut feed: Option<broadcast::Receiver<RefNotice>>,
    mut requests: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
) {
    let mut tip = loop {
        match b.refs.get_ref(&name).await {
            Ok(tip) => break tip,
            Err(error) => {
                tracing::warn!(%error, ref_name = name, "watch: reading the ref failed; retrying");
                tokio::time::sleep(RETRY).await;
            }
        }
    };
    *crate::lock(&shared.tip) = tip;
    let mut seen = Seen::default();
    // A failed step is retried as a resync after `RETRY`.
    let mut retry = false;
    loop {
        let interval = match (&feed, retry) {
            (_, true) => RETRY,
            (Some(_), false) => RESYNC_INTERVAL,
            (None, false) => POLL_INTERVAL,
        };
        let wake = tokio::select! {
            wake = recv(&mut feed) => wake,
            Some(reply) = requests.recv() => Wake::Sync(reply),
            () = tokio::time::sleep(interval) => Wake::Timer,
        };
        let mut replies = Vec::new();
        let step = match wake {
            Wake::Notice(Ok(RefNotice::Moved(m))) if m.name == name => {
                if !seen.insert(m.nonce) {
                    Step::Nothing
                } else if retry || m.old != tip {
                    Step::Resync
                } else if m.new == tip {
                    Step::Nothing
                } else {
                    Step::Move(m.new, m.cause)
                }
            }
            Wake::Notice(Ok(RefNotice::Moved(_))) => Step::Nothing,
            Wake::Notice(Err(broadcast::error::RecvError::Closed)) => {
                feed = None;
                Step::Resync
            }
            Wake::Sync(reply) => {
                replies.push(reply);
                while let Ok(reply) = requests.try_recv() {
                    replies.push(reply);
                }
                Step::Resync
            }
            // `Resync`, a lagging feed, or the timer.
            Wake::Notice(_) | Wake::Timer => Step::Resync,
        };
        let result = match step {
            Step::Nothing => continue,
            Step::Move(new, cause) => publish(&b, &shared, &mut tip, new, Some(cause)).await,
            Step::Resync => {
                // Apply the moves already queued first, so the re-read
                // rarely coalesces what notices are about to announce.
                let mut drained = Ok(());
                if let Some(rx) = feed.as_mut().filter(|_| !retry) {
                    loop {
                        match rx.try_recv() {
                            Ok(RefNotice::Moved(m)) if m.name == name && seen.insert(m.nonce) => {
                                if m.old == tip && m.new != tip {
                                    drained =
                                        publish(&b, &shared, &mut tip, m.new, Some(m.cause)).await;
                                    if drained.is_err() {
                                        break;
                                    }
                                }
                            }
                            Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                            Err(_) => break,
                        }
                    }
                }
                match drained {
                    Ok(()) => match b.refs.get_ref(&name).await {
                        Ok(actual) => publish(&b, &shared, &mut tip, actual, None).await,
                        Err(e) => Err(e.into()),
                    },
                    Err(e) => Err(e),
                }
            }
        };
        retry = match result {
            Ok(()) => false,
            Err(error) => {
                tracing::warn!(%error, ref_name = name, "watch: building an event failed; retrying");
                true
            }
        };
        // A failed sync drops its replies: those watchers fail to start.
        if !retry {
            for reply in replies {
                let _ = reply.send(());
            }
        }
    }
}

/// Build and publish the events of `*tip` → `new`, then advance `*tip`.
async fn publish(
    b: &Backend,
    shared: &Shared,
    tip: &mut Option<ObjectId>,
    new: Option<ObjectId>,
    cause: Option<MoveCause>,
) -> OpResult<()> {
    let events = events_between(b, *tip, new, cause).await?;
    let mut current = crate::lock(&shared.tip);
    for event in events {
        // No watcher right now is fine.
        let _ = shared.events.send(Arc::new(event));
    }
    *current = new;
    *tip = new;
    Ok(())
}

// ---------------------------------------------------------------------------
// watchers
// ---------------------------------------------------------------------------

/// Counts a live watcher in the metrics while it exists.
struct Watching(Option<Arc<Metrics>>);

impl Drop for Watching {
    fn drop(&mut self) {
        if let Some(m) = &self.0 {
            m.record_watcher(-1);
        }
    }
}

struct WatchState {
    b: Backend,
    _hub: Arc<Hub>,
    rx: broadcast::Receiver<Arc<RefEvent>>,
    /// The `new` of the last event handed out.
    last: Option<ObjectId>,
    pending: VecDeque<RefEvent>,
    watching: Watching,
}

impl WatchState {
    fn metrics(&self) -> Option<&Arc<Metrics>> {
        self.watching.0.as_ref()
    }

    async fn next(&mut self) -> Option<OpResult<RefEvent>> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                self.last = event.new;
                if let Some(m) = self.metrics() {
                    m.record_watch_event(event.coalesced);
                }
                return Some(Ok(event));
            }
            let mut lagged = false;
            let mut event = loop {
                match self.rx.recv().await {
                    Ok(event) => break event,
                    Err(broadcast::error::RecvError::Lagged(_)) => lagged = true,
                    Err(broadcast::error::RecvError::Closed) => {
                        return Some(Err(OpError::Aborted("ref watch ended".into())));
                    }
                }
            };
            if !lagged && event.old == self.last {
                self.pending.push_back((*event).clone());
                continue;
            }
            // Fell behind: one coalesced event to the newest buffered tip.
            loop {
                match self.rx.try_recv() {
                    Ok(newer) => event = newer,
                    Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                    Err(_) => break,
                }
            }
            match events_between(&self.b, self.last, event.new, None).await {
                Ok(events) => self.pending.extend(events),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// The events of `name` (see the module docs), starting with a catch-up
/// from `from` when given. `access`: the caller (`None` in open mode).
pub async fn watch_ref(
    b: &Backend,
    access: Option<&Access>,
    name: &str,
    from: Option<ObjectId>,
    options: WatchOptions,
    metrics: Option<Arc<Metrics>>,
) -> OpResult<impl Stream<Item = OpResult<RefEvent>> + Send + 'static> {
    ant_store::validate_ref_name(name)?;
    if let Some(a) = access
        && !a.policy.can_read_ref(&a.principal, name)
    {
        return Err(OpError::PermissionDenied(format!("cannot read ref {name}")));
    }
    if let Some(from) = from
        && b.meta.get_commits(&[from]).await?.pop().flatten().is_none()
    {
        return Err(OpError::not_found_commit(from));
    }
    let hub = hub_for(b, name, options.buffer);
    // The hub may lag the ref by a few notices: the stream starts at the
    // ref's value as of now.
    hub.sync(name).await?;
    let (rx, tip) = {
        let tip = crate::lock(&hub.shared.tip);
        (hub.shared.events.subscribe(), *tip)
    };
    let mut pending = VecDeque::new();
    let mut last = tip;
    if let Some(from) = from {
        pending.extend(events_between(b, Some(from), tip, None).await?);
        last = Some(from);
    }
    if let Some(m) = &metrics {
        m.record_watcher(1);
    }
    let state =
        WatchState { b: b.clone(), _hub: hub, rx, last, pending, watching: Watching(metrics) };
    Ok(futures::stream::unfold(state, |mut state| async move {
        state.next().await.map(|item| (item, state))
    }))
}

#[cfg(test)]
mod tests;
