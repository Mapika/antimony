//! Group commit: per-(backend, ref) request queues drained by a detached
//! worker that replays each batch in memory and publishes it with one CAS.
//!
//! **Turns.** The worker takes the store's ref lock, lands batches while its
//! queue is non-empty for up to [`LOCK_TURN`], then releases the lock (other
//! replicas waiting for it get their turn) and takes it again.
//!
//! **Pipelining.** While a batch's publish is in flight, the worker drains
//! the next batch and replays and writes it against the first batch's
//! tentative tip, so the next batch is ready when the publish completes. The
//! speculative batch is published only after its predecessor succeeded (its
//! CAS expects the predecessor's tip), and it sees the predecessor's
//! landed-as mappings before they are published (`carry`). If the
//! predecessor's publish fails or mismatches, the speculative batch is
//! discarded unreplied and its requests go back to the front of the queue.
//! Its writes are unreachable and harmless: a replay rebuilds and rewrites
//! everything it publishes. Replies are sent only after the batch's own
//! publish (or, for a batch that moves nothing, its predecessor's).
//!
//! **Durability.** When the lock can publish (`RefLock::publish`: Postgres),
//! a batch's objects and commit records are written with deferred
//! durability (no WAL flush wait), and the ref move plus the landed-as
//! mappings are one synchronous commit on the lock's own session. That commit
//! flushes every earlier write, and it fails if the database crashed since
//! the lock was taken, so a ref never points at lost objects. Each
//! speculative batch is published within the turn (lock session) that wrote
//! it. Otherwise (in-memory stores, ref lock unavailable) objects and records
//! are written durably, then the CAS, then the mappings.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use ant_auth::Policy;
use ant_core::ObjectId;
use ant_store::{Backend, CasOutcome, RefLock};
use tokio::sync::oneshot;

use super::replay::BatchState;
use super::verify::Verified;
use super::{LANDED_AS, LandOutcome, MAX_LAND_ATTEMPTS};
use crate::metrics::Metrics;
use crate::ops::{OpError, OpResult, cache};

pub(super) const MAX_BATCH: usize = 64;

/// How long a worker keeps landing under one ref lock while requests keep
/// arriving. Short enough that replicas sharing the ref take turns often;
/// long enough to keep the pipeline busy.
const LOCK_TURN: Duration = Duration::from_millis(20);

type Reply = oneshot::Sender<OpResult<LandOutcome>>;
struct Request {
    commit: ObjectId,
    verified: Verified,
    policy: Option<Arc<Policy>>,
    metrics: Option<Arc<Metrics>>,
    reply: Reply,
}

// Include all backend identities: independent repositories named "main" must
// never share requests. The worker owns the Backend while the key is live.
#[derive(Clone, Hash, PartialEq, Eq)]
struct QueueKey {
    /// Store identities (`Arc` addresses) of the backend.
    stores: [usize; 3],
    ref_name: String,
}
type Queues = HashMap<QueueKey, VecDeque<Request>>;
static QUEUES: LazyLock<Mutex<Queues>> = LazyLock::new(Default::default);

struct Worker {
    key: QueueKey,
    active: bool,
}

impl Drop for Worker {
    fn drop(&mut self) {
        if self.active {
            // Panic/runtime shutdown closes every queued reply. In-flight
            // replies are owned by the task and close when its future drops.
            crate::lock(&QUEUES).remove(&self.key);
        }
    }
}

impl Worker {
    async fn run(mut self, b: Backend) {
        // Coalesce callers that became runnable together, without a timer.
        tokio::task::yield_now().await;
        loop {
            {
                let mut queues = crate::lock(&QUEUES);
                match queues.get(&self.key) {
                    None => return,
                    Some(queue) if queue.is_empty() => {
                        queues.remove(&self.key);
                        self.active = false;
                        return;
                    }
                    Some(_) => {}
                }
            }
            // Take turns with other processes landing on this ref. Draining
            // after the lock is granted lets requests that arrived during the
            // wait join the first batch. Without the lock (store error,
            // timeout) the CAS still keeps the ref correct.
            let mut lock = match b.refs.lock_ref(&self.key.ref_name).await {
                Ok(lock) => lock,
                Err(error) => {
                    tracing::warn!(%error, ref_name = self.key.ref_name, "ref lock unavailable; landing with CAS only");
                    RefLock::unheld()
                }
            };
            self.turn(&b, &mut lock).await;
            // Every batch of the turn is published (mappings included) or
            // failed, so the next holder sees all of it.
            if let Err(error) = lock.release().await {
                tracing::warn!(%error, ref_name = self.key.ref_name, "releasing ref lock failed");
            }
        }
    }

    /// Land batches under one hold of `lock` (see the module docs). Returns
    /// with no batch prepared and unpublished.
    async fn turn(&self, b: &Backend, lock: &mut RefLock) {
        let ref_name = self.key.ref_name.as_str();
        let started = Instant::now();
        let deferred = lock.can_publish();
        // The ref's value, when this turn knows it (it published it).
        let mut known: Option<Option<ObjectId>> = None;
        // A replayed and written batch whose predecessors are all published.
        let mut ready: Option<Batch> = None;
        let mut batches = 0usize;
        loop {
            let mut current = match ready.take() {
                Some(batch) => batch,
                None => {
                    if batches > 0 && started.elapsed() >= LOCK_TURN {
                        return;
                    }
                    let Some(requests) = self.drain() else { return };
                    batches += 1;
                    let mut batch = Batch::new(requests);
                    if let Err(error) = batch.replay(b, ref_name, known, None, deferred).await {
                        batch.fail(error);
                        known = None;
                        continue;
                    }
                    batch
                }
            };
            // Read-only batches linearize at their start (already published).
            // Otherwise even conflict replies wait for the publish: their
            // tentative tips must exist in the published history.
            let Some(tip) = current.tip.filter(|_| current.moves()) else {
                known = Some(current.start);
                current.succeed();
                continue;
            };
            let mut next = None;
            if started.elapsed() < LOCK_TURN
                && let Some(requests) = self.drain()
            {
                batches += 1;
                next = Some(Batch::new(requests));
            }
            let carry = current.state.pending.clone();
            let mappings = current.mappings();
            let start = current.start;
            let (published, speculated) =
                futures::join!(publish(b, ref_name, lock, deferred, start, tip, mappings), async {
                    match next.as_mut() {
                        Some(next) => Some(
                            next.replay(b, ref_name, Some(Some(tip)), Some(&carry), deferred).await,
                        ),
                        None => None,
                    }
                },);
            match published {
                Ok(CasOutcome::Updated) => {
                    known = Some(Some(tip));
                    current.succeed();
                    match (next, speculated) {
                        (Some(next), Some(Err(error))) => next.fail(error),
                        (next, _) => ready = next,
                    }
                }
                Ok(CasOutcome::Mismatch { actual }) => {
                    if let Some(m) = current.requests.iter().find_map(|r| r.metrics.as_deref()) {
                        m.record_land_cas_retry();
                    }
                    if let Some(next) = next {
                        self.requeue(next.requests);
                    }
                    match current.retry(b, ref_name, actual, deferred).await {
                        Ok(()) => ready = Some(current),
                        Err(error) => {
                            current.fail(error);
                            known = None;
                        }
                    }
                }
                Err(error) => {
                    if let Some(next) = next {
                        self.requeue(next.requests);
                    }
                    current.fail(error);
                    // The lock's session may be gone: take the lock again.
                    return;
                }
            }
        }
    }

    /// Up to [`MAX_BATCH`] queued requests, in arrival order.
    fn drain(&self) -> Option<Vec<Request>> {
        let mut queues = crate::lock(&QUEUES);
        let queue = queues.get_mut(&self.key)?;
        if queue.is_empty() {
            return None;
        }
        let count = queue.len().min(MAX_BATCH);
        Some(queue.drain(..count).collect())
    }

    /// Put drained but unanswered requests back at the front of the queue.
    fn requeue(&self, requests: Vec<Request>) {
        let mut queues = crate::lock(&QUEUES);
        // The queue lives as long as this worker is active.
        if let Some(queue) = queues.get_mut(&self.key) {
            for request in requests.into_iter().rev() {
                queue.push_front(request);
            }
        }
    }
}

/// Move the ref from `start` to `tip` and record the batch's landed-as
/// mappings, durably, before anyone is replied to.
async fn publish(
    b: &Backend,
    ref_name: &str,
    lock: &mut RefLock,
    deferred: bool,
    start: Option<ObjectId>,
    tip: ObjectId,
    mappings: Vec<(Vec<u8>, bytes::Bytes)>,
) -> OpResult<CasOutcome> {
    if deferred {
        // One commit for both, which also makes the batch's deferred writes
        // durable (or fails if the database lost them).
        return Ok(lock.publish(start, tip, LANDED_AS, mappings).await?);
    }
    let cas = b.refs.cas_ref(ref_name, start, Some(tip)).await?;
    if cas == CasOutcome::Updated {
        b.meta.put_derived_many(LANDED_AS, mappings).await?;
    }
    Ok(cas)
}

/// One group-commit batch: its requests, the replay state (kept across CAS
/// retries: nodes, trees and blobs are immutable) and the latest replay's
/// result.
struct Batch {
    requests: Vec<Request>,
    ids: Vec<ObjectId>,
    state: BatchState,
    replays: usize,
    start: Option<ObjectId>,
    tip: Option<ObjectId>,
    outcomes: Vec<OpResult<LandOutcome>>,
}

impl Batch {
    fn new(requests: Vec<Request>) -> Self {
        if let Some(m) = requests.iter().find_map(|r| r.metrics.as_deref()) {
            m.record_land_batch(requests.len());
        }
        let ids = requests.iter().map(|r| r.commit).collect();
        Self {
            requests,
            ids,
            state: BatchState::default(),
            replays: 0,
            start: None,
            tip: None,
            outcomes: Vec::new(),
        }
    }

    fn moves(&self) -> bool {
        self.tip != self.start
    }

    /// Replay every request in arrival order against `start` (`None`: read
    /// the ref), then write what the replay built. `carry`: landed-as
    /// mappings of an unpublished predecessor this replay builds on.
    async fn replay(
        &mut self,
        b: &Backend,
        ref_name: &str,
        start: Option<Option<ObjectId>>,
        carry: Option<&BTreeMap<ObjectId, ObjectId>>,
        deferred: bool,
    ) -> OpResult<()> {
        let keys: Vec<_> = self.ids.iter().map(|id| id.as_bytes().as_slice()).collect();
        // Independent reads, one round trip.
        let (start, mapped) = match start {
            Some(start) => (start, b.meta.get_derived_many(LANDED_AS, &keys).await?),
            None => futures::try_join!(async { b.refs.get_ref(ref_name).await }, async {
                b.meta.get_derived_many(LANDED_AS, &keys).await
            })?,
        };
        if self.replays == 0 {
            // Batch-wide prefetch: nodes, then the changesets and messages a
            // rebase needs (unrecorded ids are reported per request by `apply`).
            self.state.prefetch(b, &self.ids).await?;
            let recorded: Vec<_> =
                self.ids.iter().copied().filter(|id| self.state.nodes.contains_key(id)).collect();
            if !recorded.is_empty() {
                cache::get_changesets(b, &recorded).await?;
                cache::read_commits(b, &recorded).await?;
            }
        }
        self.replays += 1;
        for (id, mapped) in self.ids.iter().zip(mapped) {
            self.state.mappings.insert(*id, mapped);
        }
        for (original, landed) in carry.into_iter().flatten() {
            self.state.mappings.insert(*original, Some(landed.as_bytes().to_vec().into()));
        }
        let mut tip = start;
        let mut outcomes = Vec::with_capacity(self.requests.len());
        for request in &self.requests {
            let outcome = self
                .state
                .apply(b, request.commit, tip, &request.verified, request.policy.as_ref())
                .await;
            if let Ok(LandOutcome::Landed { new_tip, .. }) = &outcome {
                tip = Some(*new_tip);
            }
            outcomes.push(outcome);
        }
        (self.start, self.tip, self.outcomes) = (start, tip, outcomes);
        // Every rebased commit is written and recorded before it can become
        // reachable from the ref.
        if self.moves() {
            if deferred {
                ant_store::deferred_durability(self.state.flush(b)).await?;
            } else {
                self.state.flush(b).await?;
            }
        }
        Ok(())
    }

    /// Replay again from `actual` after a CAS mismatch, re-reading landed-as
    /// mappings (another process may have published some).
    async fn retry(
        &mut self,
        b: &Backend,
        ref_name: &str,
        actual: Option<ObjectId>,
        deferred: bool,
    ) -> OpResult<()> {
        if self.replays >= MAX_LAND_ATTEMPTS {
            return Err(OpError::Aborted(format!(
                "ref {ref_name} changed during {MAX_LAND_ATTEMPTS} land attempts"
            )));
        }
        self.state.pending.clear();
        self.state.mappings.clear();
        self.replay(b, ref_name, Some(actual), None, deferred).await
    }

    /// The landed-as mappings this batch publishes.
    fn mappings(&self) -> Vec<(Vec<u8>, bytes::Bytes)> {
        self.state
            .pending
            .iter()
            .map(|(original, landed)| {
                (original.as_bytes().to_vec(), landed.as_bytes().to_vec().into())
            })
            .collect()
    }

    fn succeed(self) {
        for (request, outcome) in self.requests.into_iter().zip(self.outcomes) {
            let _ = request.reply.send(outcome);
        }
    }

    fn fail(self, error: OpError) {
        let message = error.to_string();
        for request in self.requests {
            let _ = request.reply.send(Err(OpError::Aborted(message.clone())));
        }
    }
}

/// Live land queues and the requests waiting in them (process-wide).
pub(crate) fn queue_stats() -> (usize, usize) {
    let queues = crate::lock(&QUEUES);
    (queues.len(), queues.values().map(VecDeque::len).sum())
}

/// Waiting requests per ref name, summed over backends (for metrics).
pub(crate) fn queue_depths() -> Vec<(String, usize)> {
    let queues = crate::lock(&QUEUES);
    let mut by_ref: HashMap<&str, usize> = HashMap::new();
    for (key, queue) in queues.iter() {
        *by_ref.entry(key.ref_name.as_str()).or_default() += queue.len();
    }
    by_ref.into_iter().map(|(r, n)| (r.to_owned(), n)).collect()
}

#[cfg(test)]
pub(super) fn enqueue(
    b: &Backend,
    ref_name: &str,
    commit: ObjectId,
) -> oneshot::Receiver<OpResult<LandOutcome>> {
    enqueue_checked(b, ref_name, commit, Verified::All, None, None)
}

/// Queue a request already checked by `verify_land` (or `All` in open mode).
pub(super) fn enqueue_checked(
    b: &Backend,
    ref_name: &str,
    commit: ObjectId,
    verified: Verified,
    policy: Option<Arc<Policy>>,
    metrics: Option<Arc<Metrics>>,
) -> oneshot::Receiver<OpResult<LandOutcome>> {
    let key = QueueKey {
        stores: [
            Arc::as_ptr(&b.objects).cast::<()>() as usize,
            Arc::as_ptr(&b.refs).cast::<()>() as usize,
            Arc::as_ptr(&b.meta).cast::<()>() as usize,
        ],
        ref_name: ref_name.to_owned(),
    };
    let (reply, receiver) = oneshot::channel();
    let mut queues = crate::lock(&QUEUES);
    let lead = !queues.contains_key(&key);
    queues.entry(key.clone()).or_default().push_back(Request {
        commit,
        verified,
        policy,
        metrics,
        reply,
    });
    if lead {
        tokio::spawn(Worker { key, active: true }.run(b.clone()));
    }
    receiver
}
