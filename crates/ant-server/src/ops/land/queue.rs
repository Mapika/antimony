//! Group commit: per-(backend, ref) request queues drained by a detached
//! worker that replays each batch in memory and publishes it with one CAS.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};

use ant_auth::Policy;
use ant_core::ObjectId;
use ant_store::{Backend, CasOutcome, RefLock};
use tokio::sync::oneshot;

use super::replay::BatchState;
use super::verify::Verified;
use super::{LANDED_AS, LandOutcome, MAX_LAND_ATTEMPTS};
use crate::ops::{OpError, OpResult, cache};

pub(super) const MAX_BATCH: usize = 64;
type Reply = oneshot::Sender<OpResult<LandOutcome>>;
struct Request {
    commit: ObjectId,
    verified: Verified,
    policy: Option<Arc<Policy>>,
    reply: Reply,
}

// Include all backend identities: independent repositories named "main" must
// never share requests. The worker owns the Backend while the key is live.
#[derive(Clone, Hash, PartialEq, Eq)]
struct QueueKey(usize, usize, usize, String);
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
            QUEUES.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.key);
        }
    }
}

impl Worker {
    async fn run(mut self, b: Backend) {
        // Coalesce callers that became runnable together, without a timer.
        tokio::task::yield_now().await;
        loop {
            {
                let mut queues = QUEUES.lock().unwrap_or_else(|e| e.into_inner());
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
            // wait join this batch. Without the lock (store error, timeout)
            // the CAS below still keeps the ref correct.
            let lock = match b.refs.lock_ref(&self.key.3).await {
                Ok(lock) => lock,
                Err(error) => {
                    tracing::warn!(%error, ref_name = self.key.3, "ref lock unavailable; landing with CAS only");
                    RefLock::unheld()
                }
            };
            let batch: Vec<_> = {
                let mut queues = QUEUES.lock().unwrap_or_else(|e| e.into_inner());
                let Some(queue) = queues.get_mut(&self.key) else {
                    return;
                };
                let count = queue.len().min(MAX_BATCH);
                queue.drain(..count).collect()
            };
            let result = land_batch(&b, &self.key.3, &batch).await;
            // Mappings are durable before land_batch returns, so the next
            // holder sees everything this batch published.
            if let Err(error) = lock.release().await {
                tracing::warn!(%error, ref_name = self.key.3, "releasing ref lock failed");
            }
            match result {
                Ok(outcomes) => {
                    for (request, outcome) in batch.into_iter().zip(outcomes) {
                        let _ = request.reply.send(outcome);
                    }
                }
                Err(error) => {
                    let message = error.to_string();
                    for request in batch {
                        let _ = request.reply.send(Err(OpError::Aborted(message.clone())));
                    }
                }
            }
        }
    }
}

/// Live land queues and the requests waiting in them (process-wide).
pub(crate) fn queue_stats() -> (usize, usize) {
    let queues = QUEUES.lock().unwrap_or_else(|e| e.into_inner());
    (queues.len(), queues.values().map(VecDeque::len).sum())
}

#[cfg(test)]
pub(super) fn enqueue(
    b: &Backend,
    ref_name: &str,
    commit: ObjectId,
) -> oneshot::Receiver<OpResult<LandOutcome>> {
    enqueue_checked(b, ref_name, commit, Verified::All, None)
}

/// Queue a request already checked by `verify_land` (or `All` in open mode).
pub(super) fn enqueue_checked(
    b: &Backend,
    ref_name: &str,
    commit: ObjectId,
    verified: Verified,
    policy: Option<Arc<Policy>>,
) -> oneshot::Receiver<OpResult<LandOutcome>> {
    let key = QueueKey(
        Arc::as_ptr(&b.objects) as *const () as usize,
        Arc::as_ptr(&b.refs) as *const () as usize,
        Arc::as_ptr(&b.meta) as *const () as usize,
        ref_name.to_owned(),
    );
    let (reply, receiver) = oneshot::channel();
    let mut queues = QUEUES.lock().unwrap_or_else(|e| e.into_inner());
    let lead = !queues.contains_key(&key);
    queues.entry(key.clone()).or_default().push_back(Request { commit, verified, policy, reply });
    if lead {
        tokio::spawn(Worker { key, active: true }.run(b.clone()));
    }
    receiver
}

async fn land_batch(
    b: &Backend,
    ref_name: &str,
    batch: &[Request],
) -> OpResult<Vec<OpResult<LandOutcome>>> {
    let mut state = BatchState::default();
    let ids: Vec<_> = batch.iter().map(|r| r.commit).collect();
    let keys: Vec<_> = ids.iter().map(|id| id.as_bytes().as_slice()).collect();
    // Independent reads, one round trip.
    let (mut start, mut mapped) =
        futures::try_join!(async { b.refs.get_ref(ref_name).await }, async {
            b.meta.get_derived_many(LANDED_AS, &keys).await
        },)?;
    // Batch-wide prefetch: nodes, then the changesets and messages a rebase
    // needs (unrecorded ids are reported per request by `apply`).
    state.prefetch(b, &ids).await?;
    let recorded: Vec<_> = ids.iter().copied().filter(|id| state.nodes.contains_key(id)).collect();
    if !recorded.is_empty() {
        cache::get_changesets(b, &recorded).await?;
        cache::read_commits(b, &recorded).await?;
    }
    for attempt in 0..MAX_LAND_ATTEMPTS {
        if attempt > 0 {
            mapped = b.meta.get_derived_many(LANDED_AS, &keys).await?;
        }
        for (id, mapped) in ids.iter().zip(std::mem::take(&mut mapped)) {
            state.mappings.insert(*id, mapped);
        }
        let mut tip = start;
        let mut outcomes = Vec::with_capacity(batch.len());
        for request in batch {
            let outcome = state
                .apply(b, request.commit, tip, &request.verified, request.policy.as_ref())
                .await;
            if let Ok(LandOutcome::Landed { new_tip, .. }) = &outcome {
                tip = Some(*new_tip);
            }
            outcomes.push(outcome);
        }
        // Read-only batches may linearize at the initial read. Otherwise even
        // conflict replies wait for the CAS: their tentative tips must exist
        // in the published history.
        if tip == start {
            return Ok(outcomes);
        }
        // Every rebased commit is written and recorded before it can become
        // reachable from the ref.
        state.flush(b).await?;
        match b.refs.cas_ref(ref_name, start, tip).await? {
            CasOutcome::Updated => {
                let mappings: Vec<_> = std::mem::take(&mut state.pending)
                    .into_iter()
                    .map(|(original, landed)| {
                        (original.as_bytes().to_vec(), landed.as_bytes().to_vec().into())
                    })
                    .collect();
                b.meta.put_derived_many(LANDED_AS, mappings).await?;
                return Ok(outcomes);
            }
            CasOutcome::Mismatch { actual } => {
                start = actual;
                state.pending.clear();
                // Another process may have published new landed-as mappings.
                state.mappings.clear();
            }
        }
    }
    Err(OpError::Aborted(format!(
        "ref {ref_name} changed during {MAX_LAND_ATTEMPTS} land attempts"
    )))
}
