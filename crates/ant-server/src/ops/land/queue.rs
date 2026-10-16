//! Group commit: per-(backend, ref) request queues drained by a detached
//! worker that replays each batch in memory and publishes it with one CAS.
//!
//! **Turns.** The worker takes the store's ref lock and lands batches while
//! its queue is non-empty. Every [`LOCK_TURN`] it asks the lock whether
//! anyone else waits for it (`RefLock::contended`: another replica, an
//! import publishing the ref): if so, it finishes the turn, releases the
//! lock (the waiter gets its turn) and takes it again; otherwise the turn
//! goes on, so an uncontended ref keeps its pipeline full instead of
//! draining it and replaying from scratch every turn.
//!
//! **Pipelining.** A batch goes through three stages: *prepare* (replay
//! every request in memory on the tentative tip of the batch before it),
//! *write* (its new objects and commit records, on a task of its own) and
//! *publish* (one CAS, after its own writes and its predecessor's publish).
//! The stages of consecutive batches overlap: while a batch's writes finish
//! and it publishes, the worker prepares (and starts writing) the next one,
//! up to [`MAX_AHEAD`] prepared batches behind the one publishing. A
//! publish starts as soon as its own batch's writes are done: it never
//! waits for the replay running beside it (with slow, fsync-bound commits
//! that wait would add a replay to every publish, and each request waits
//! for several). A speculative batch is seeded with what its
//! predecessor read and built (nodes, trees, blobs: not yet written or
//! cached) and sees the landed-as mappings of every unpublished batch
//! before it (`carry`). It is published only after all of them succeeded
//! (its CAS expects its predecessor's tip). If a publish fails or
//! mismatches, or a batch's writes fail, every batch behind it is discarded
//! unreplied, once its writes are done, and its requests go back to the
//! front of the queue in arrival order. Their writes are unreachable and
//! harmless: a replay rebuilds and rewrites everything it publishes.
//! Replies are sent only after the batch's own publish (or, for a batch that
//! moves nothing, its predecessor's).
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
//!
//! **Pipelined publishes** (`RefLock::publish_pipelined`; the local store):
//! the publish returns once committed, and the batch is answered only when
//! its durability resolves (an fsync covering it), while the next batch
//! already publishes. Readers may see a move a moment before it is durable;
//! nobody who landed is told so before it is, and a later commit is never
//! durable before an earlier one.

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

/// Prepared batches a worker keeps behind the one it publishes: one whose
/// writes are in flight and one being replayed (see the module docs).
const MAX_AHEAD: usize = 2;

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
        let mut started = Instant::now();
        let deferred = lock.can_publish();
        // The ref's value, when this turn knows it (it published it).
        let mut known: Option<Option<ObjectId>> = None;
        // Prepared, unpublished batches in order, each built on its
        // predecessor's tentative tip, their writes possibly in flight.
        let mut pipeline: VecDeque<Batch> = VecDeque::new();
        let mut batches = 0usize;
        loop {
            let mut current = match pipeline.pop_front() {
                Some(batch) => batch,
                None => {
                    if batches > 0 && !extend_turn(lock, &mut started).await {
                        return;
                    }
                    let Some(requests) = self.drain() else { return };
                    batches += 1;
                    let mut batch = Batch::new(requests);
                    if let Err(error) = batch.prepare(b, ref_name, known, None).await {
                        batch.fail(error);
                        known = None;
                        continue;
                    }
                    batch.start_writes(b, deferred);
                    batch
                }
            };
            // The batch prepared (and started writing) while this one's
            // writes finish and it publishes.
            let mut ahead = None;
            if pipeline.len() < MAX_AHEAD
                && extend_turn(lock, &mut started).await
                && let Some(requests) = self.drain()
            {
                batches += 1;
                ahead = Some(Batch::new(requests));
            }
            let writes = current.writes.take();
            let carry = unpublished_mappings(&current, &pipeline);
            // Read-only batches linearize at their start (already published).
            // Otherwise even conflict replies wait for the publish: their
            // tentative tips must exist in the published history.
            let tip = current.tip.filter(|_| current.moves());
            let mappings = if tip.is_some() { current.mappings() } else { Vec::new() };
            let start = current.start;
            // With nothing to prepare yet, requests that arrive meanwhile
            // form the next batch, prepared while this one publishes.
            let mut next = None;
            let room = ahead.is_none() && pipeline.len() < MAX_AHEAD;
            let (written, published, prepared) = {
                let newest = pipeline.back().unwrap_or(&current);
                let speculation = speculate(b, ref_name, deferred, ahead.as_mut(), newest, &carry);
                futures::pin_mut!(speculation);
                let mut prepared = None;
                // This batch's writes finish while the next one is
                // prepared...
                let written = {
                    let written = async {
                        match writes {
                            Some(writes) => writes.await.map_err(|error| {
                                OpError::Aborted(format!("land writes failed: {error}"))
                            })?,
                            None => Ok(()),
                        }
                    };
                    futures::pin_mut!(written);
                    loop {
                        tokio::select! {
                            biased;
                            result = &mut written => break result,
                            result = &mut speculation, if prepared.is_none() => {
                                prepared = Some(result);
                            }
                        }
                    }
                };
                // ...and it publishes as soon as they did, still alongside
                // that preparation: a publish never waits for a replay.
                let rest = async {
                    match prepared {
                        Some(prepared) => prepared,
                        None => speculation.await,
                    }
                };
                match (&written, tip) {
                    (Ok(()), Some(tip)) => {
                        if room
                            && extend_turn(lock, &mut started).await
                            && let Some(requests) = self.drain()
                        {
                            batches += 1;
                            next = Some(Batch::new(requests));
                        }
                        let (published, prepared, speculated) = futures::join!(
                            publish(b, ref_name, lock, deferred, start, tip, mappings),
                            rest,
                            speculate(b, ref_name, deferred, next.as_mut(), newest, &carry)
                        );
                        (written, Some(published), prepared.or(speculated))
                    }
                    _ => (written, None, rest.await),
                }
            };
            // At most one of them was drained.
            let mut ahead = ahead.or(next);
            if let Some(Err(error)) = prepared
                && let Some(batch) = ahead.take()
            {
                batch.fail(error);
            }
            if let Err(error) = written {
                // Everything built on it is void.
                if let Some(batch) = ahead {
                    self.requeue(batch.abandon().await);
                }
                self.requeue_all(&mut pipeline).await;
                current.fail(error);
                known = None;
                continue;
            }
            let Some(published) = published else {
                pipeline.extend(ahead);
                known = Some(current.start);
                current.succeed();
                continue;
            };
            match published {
                Ok((CasOutcome::Updated, durable)) => {
                    known = Some(tip);
                    match durable {
                        // Answered once the publish is durable; the next
                        // batch publishes meanwhile (`RefLock::publish_pipelined`).
                        Some(durable) => {
                            tokio::spawn(async move {
                                match durable.await {
                                    Ok(()) => current.succeed(),
                                    Err(error) => current.fail(error.into()),
                                }
                            });
                        }
                        None => current.succeed(),
                    }
                    pipeline.extend(ahead);
                }
                Ok((CasOutcome::Mismatch { actual }, _)) => {
                    if let Some(m) = current.requests.iter().find_map(|r| r.metrics.as_deref()) {
                        m.record_land_cas_retry();
                    }
                    // Newest first, so the queue keeps arrival order.
                    if let Some(batch) = ahead {
                        self.requeue(batch.abandon().await);
                    }
                    self.requeue_all(&mut pipeline).await;
                    match current.retry(b, ref_name, actual).await {
                        Ok(()) => {
                            current.start_writes(b, deferred);
                            pipeline.push_front(current);
                        }
                        Err(error) => {
                            current.fail(error);
                            known = None;
                        }
                    }
                }
                Err(error) => {
                    if let Some(batch) = ahead {
                        self.requeue(batch.abandon().await);
                    }
                    self.requeue_all(&mut pipeline).await;
                    current.fail(error);
                    // The lock's session may be gone: take the lock again.
                    return;
                }
            }
        }
    }

    /// Requeue every batch of `pipeline` (newest first, keeping arrival
    /// order), abandoning its writes.
    async fn requeue_all(&self, pipeline: &mut VecDeque<Batch>) {
        while let Some(batch) = pipeline.pop_back() {
            self.requeue(batch.abandon().await);
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

/// Landed-as mappings of `current` and of every batch queued behind it, none
/// of them published yet: a batch replayed after them must see them.
fn unpublished_mappings(
    current: &Batch,
    pipeline: &VecDeque<Batch>,
) -> BTreeMap<ObjectId, ObjectId> {
    let mut carry = current.state.pending.clone();
    for batch in pipeline {
        carry.extend(batch.state.pending.iter().map(|(k, v)| (*k, *v)));
    }
    carry
}

/// Replay `batch` (if any) on the tentative tip of `newest`, the last
/// unpublished batch, seeing `carry`, and start its writes. `None` when
/// there is no batch.
async fn speculate(
    b: &Backend,
    ref_name: &str,
    deferred: bool,
    batch: Option<&mut Batch>,
    newest: &Batch,
    carry: &BTreeMap<ObjectId, ObjectId>,
) -> Option<OpResult<()>> {
    let batch = batch?;
    batch.state.seed(&newest.state);
    let prepared = batch.prepare(b, ref_name, Some(newest.tip), Some(carry)).await;
    if prepared.is_ok() {
        batch.start_writes(b, deferred);
    }
    Some(prepared)
}

/// Whether the turn that began at `started` may go on: within
/// [`LOCK_TURN`], or past it while nobody else waits for the lock (a new
/// turn period then starts). A failed probe ends the turn.
async fn extend_turn(lock: &mut RefLock, started: &mut Instant) -> bool {
    if started.elapsed() < LOCK_TURN {
        return true;
    }
    match lock.contended().await {
        Ok(false) => {
            *started = Instant::now();
            true
        }
        Ok(true) => false,
        Err(error) => {
            tracing::warn!(%error, "probing the ref lock for waiters failed");
            false
        }
    }
}

/// Move the ref from `start` to `tip` and record the batch's landed-as
/// mappings, durably before anyone is replied to: either now, or (a
/// pipelined publish) once the returned durability resolves.
async fn publish(
    b: &Backend,
    ref_name: &str,
    lock: &mut RefLock,
    deferred: bool,
    start: Option<ObjectId>,
    tip: ObjectId,
    mappings: Vec<(Vec<u8>, bytes::Bytes)>,
) -> OpResult<(CasOutcome, Option<ant_store::Durability>)> {
    if deferred {
        // One commit for both, which also makes the batch's deferred writes
        // durable (or fails if the database lost them).
        let (out, durable) = lock.publish_pipelined(start, tip, LANDED_AS, mappings).await?;
        return Ok((out, Some(durable)));
    }
    let cas = ant_store::feed::landing(b.refs.cas_ref(ref_name, start, Some(tip))).await?;
    if cas == CasOutcome::Updated {
        b.meta.put_derived_many(LANDED_AS, mappings).await?;
    }
    Ok((cas, None))
}

/// One group-commit batch: its requests, the replay state (kept across CAS
/// retries: nodes, trees and blobs are immutable), the latest replay's
/// result and its writes in flight.
struct Batch {
    requests: Vec<Request>,
    ids: Vec<ObjectId>,
    state: BatchState,
    replays: usize,
    start: Option<ObjectId>,
    tip: Option<ObjectId>,
    outcomes: Vec<OpResult<LandOutcome>>,
    /// The latest replay's writes, running on their own task.
    writes: Option<tokio::task::JoinHandle<OpResult<()>>>,
}

impl Drop for Batch {
    fn drop(&mut self) {
        // Writes of a batch nobody publishes are unreachable: stop them.
        if let Some(writes) = self.writes.take() {
            writes.abort();
        }
    }
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
            writes: None,
        }
    }

    fn moves(&self) -> bool {
        self.tip != self.start
    }

    /// Replay every request in arrival order against `start` (`None`: read
    /// the ref), in memory; [`Self::start_writes`] writes what it built.
    /// `carry`: landed-as mappings of unpublished predecessors this replay
    /// builds on.
    async fn prepare(
        &mut self,
        b: &Backend,
        ref_name: &str,
        start: Option<Option<ObjectId>>,
        carry: Option<&BTreeMap<ObjectId, ObjectId>>,
    ) -> OpResult<()> {
        let (ids, state, first) = (&self.ids, &mut self.state, self.replays == 0);
        let keys: Vec<_> = ids.iter().map(|id| id.as_bytes().as_slice()).collect();
        // Independent reads, concurrently: the tip and the mappings (one
        // round trip), and on the first replay the batch-wide prefetch of
        // nodes, then the changesets and messages a rebase needs
        // (unrecorded ids are reported per request by `apply`).
        let reads = async {
            Ok::<_, OpError>(match start {
                Some(start) => (start, b.meta.get_derived_many(LANDED_AS, &keys).await?),
                None => futures::try_join!(async { b.refs.get_ref(ref_name).await }, async {
                    b.meta.get_derived_many(LANDED_AS, &keys).await
                })?,
            })
        };
        let prefetch = async {
            if first {
                state.prefetch(b, ids).await?;
                let recorded: Vec<_> =
                    ids.iter().copied().filter(|id| state.nodes.contains_key(id)).collect();
                if !recorded.is_empty() {
                    futures::try_join!(
                        cache::get_changesets(b, &recorded),
                        cache::read_commits(b, &recorded)
                    )?;
                }
            }
            Ok(())
        };
        let ((start, mapped), ()) = futures::try_join!(reads, prefetch)?;
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
        Ok(())
    }

    /// Write and record what the latest replay built, on a task of its own
    /// (deferred durability when the lock publishes; see the module docs).
    /// Every rebased commit is written and recorded before it can become
    /// reachable from the ref: the worker waits for these writes before
    /// publishing the batch.
    fn start_writes(&mut self, b: &Backend, deferred: bool) {
        let writes = self.state.take_writes();
        if !self.moves() || writes.is_empty() {
            return;
        }
        let b = b.clone();
        self.writes = Some(tokio::spawn(async move {
            if deferred {
                ant_store::deferred_durability(writes.write(&b)).await
            } else {
                writes.write(&b).await
            }
        }));
    }

    /// Replay again from `actual` after a CAS mismatch, re-reading landed-as
    /// mappings (another process may have published some).
    async fn retry(
        &mut self,
        b: &Backend,
        ref_name: &str,
        actual: Option<ObjectId>,
    ) -> OpResult<()> {
        if self.replays >= MAX_LAND_ATTEMPTS {
            return Err(OpError::Aborted(format!(
                "ref {ref_name} changed during {MAX_LAND_ATTEMPTS} land attempts"
            )));
        }
        self.state.pending.clear();
        self.state.mappings.clear();
        self.prepare(b, ref_name, Some(actual), None).await
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

    /// The unanswered requests, for requeueing, once the batch's writes are
    /// done (they are unreachable and harmless, and they finish within the
    /// turn that started them, like everything written under the lock).
    async fn abandon(mut self) -> Vec<Request> {
        if let Some(writes) = self.writes.take() {
            let _ = writes.await;
        }
        std::mem::take(&mut self.requests)
    }

    fn succeed(mut self) {
        let outcomes = std::mem::take(&mut self.outcomes);
        for (request, outcome) in std::mem::take(&mut self.requests).into_iter().zip(outcomes) {
            let _ = request.reply.send(outcome);
        }
    }

    fn fail(mut self, error: OpError) {
        let message = error.to_string();
        for request in std::mem::take(&mut self.requests) {
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
