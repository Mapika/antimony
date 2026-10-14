//! The async side of an import: stores object batches concurrently, records
//! commits strictly in order behind them, and moves the imported refs
//! ([`Publisher`]).

use std::collections::HashMap;
use std::sync::Arc;

use ant_core::{ObjectId, ObjectKind};
use ant_store::{Backend, CasOutcome};
use anyhow::{Context as _, anyhow, bail};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;

use crate::graph::COMMIT_KIND;
use crate::{BatchHook, ImportStats, Msg, NewCommit};

/// Bytes of object batches stored concurrently (commits are still recorded in order).
const INFLIGHT_BYTES: usize = 128 << 20;
/// Batches queued between the object writers and the in-order commit recorder.
const RECORD_QUEUE: usize = 64;

#[derive(Default)]
struct Counts {
    trees: u64,
    blobs: u64,
}

pub(crate) struct Recorder {
    pub commits: usize,
    pub hook: Option<BatchHook>,
}

/// Moves the imported refs, and decides whether the import may use deferred
/// durability.
///
/// When the store has real ref locks (Postgres) every target ref gets a
/// *witness*: a lock on the private name `import-witness/<ref>`, taken before
/// any write and held to the end. It is not the target ref's own lock, so
/// lands on the target ref are not blocked while the import runs.
///
/// With witnesses the import runs with deferred durability: objects,
/// commits, changesets and mappings are acknowledged before their WAL flush,
/// which removes a synchronous flush per statement from the serial recording
/// path. Crash safety:
/// - The database log is sequential and a crash loses only a suffix of it.
///   Every write is acknowledged before the writes that depend on it are sent
///   (objects before the commits naming them, commits before their
///   mappings), so whatever survives a crash is a consistent prefix: a
///   re-run finds the surviving mappings and continues incrementally.
/// - Before a ref moves, the witness publishes a marker ref (one synchronous
///   commit on the witness session, then removed). The session predates the
///   deferred writes and a crash ends it, so this proves that no crash
///   happened and flushes every earlier write with it. Only then is the
///   target ref's lock taken, for the length of one compare-and-swap.
///
/// The swap is against the tip the import started from (per ref, updated as
/// the import moves it): if the ref moved meanwhile (a land), the import
/// stops with an error. Its commits stay recorded and a re-run publishes
/// them without converting anything. Two imports of one ref exclude each
/// other through the witness. Stores without real locks write synchronously.
pub(crate) struct Publisher {
    witnesses: Vec<Witness>,
    /// Tip of each ref as this import last saw it.
    expected: HashMap<String, Option<ObjectId>>,
}

struct Witness {
    ant_ref: String,
    marker: String,
    lock: ant_store::RefLock,
}

impl Publisher {
    pub async fn acquire(
        backend: &Backend,
        refs: &[(String, String)],
        defer: bool,
    ) -> anyhow::Result<Self> {
        let mut names: Vec<&str> = refs.iter().map(|(_, a)| a.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        let mut witnesses = Vec::new();
        let mut expected = HashMap::new();
        for name in names {
            if defer {
                let marker = format!("import-witness/{name}");
                let lock = backend
                    .refs
                    .lock_ref(&marker)
                    .await
                    .with_context(|| format!("locking import witness for ref {name}"))?;
                if lock.can_publish() {
                    // Left behind by an import that died between marker and removal.
                    if let Some(stale) = backend.refs.get_ref(&marker).await? {
                        backend.refs.cas_ref(&marker, Some(stale), None).await?;
                    }
                    witnesses.push(Witness { ant_ref: name.to_string(), marker, lock });
                }
            }
            expected.insert(name.to_string(), backend.refs.get_ref(name).await?);
        }
        Ok(Self { witnesses, expected })
    }

    /// Whether every ref has a witness (deferred durability is safe).
    pub fn deferred(&self) -> bool {
        !self.witnesses.is_empty() && self.witnesses.len() == self.expected.len()
    }

    /// Point `ant_ref` at `id`, durably, if it still is where the import left it.
    async fn publish(
        &mut self,
        backend: &Backend,
        ant_ref: &str,
        id: ObjectId,
    ) -> anyhow::Result<()> {
        let expected = self.expected.get(ant_ref).copied().flatten();
        let outcome = if self.deferred() {
            let w = self
                .witnesses
                .iter_mut()
                .find(|w| w.ant_ref == ant_ref)
                .ok_or_else(|| anyhow!("no import witness for ref {ant_ref}"))?;
            match w.lock.publish(None, id, "import.witness", Vec::new()).await? {
                CasOutcome::Updated => {}
                CasOutcome::Mismatch { .. } => bail!("marker ref {} already exists", w.marker),
            }
            let marker = w.marker.clone();
            let mut real = backend
                .refs
                .lock_ref(ant_ref)
                .await
                .with_context(|| format!("locking ref {ant_ref} to publish the import"))?;
            let swapped = if real.can_publish() {
                real.publish(expected, id, "import.tip", Vec::new()).await
            } else {
                backend.refs.cas_ref(ant_ref, expected, Some(id)).await
            };
            let _ = real.release().await;
            backend.refs.cas_ref(&marker, Some(id), None).await?;
            swapped?
        } else {
            backend.refs.cas_ref(ant_ref, expected, Some(id)).await?
        };
        match outcome {
            CasOutcome::Updated => {
                self.expected.insert(ant_ref.to_string(), Some(id));
                Ok(())
            }
            CasOutcome::Mismatch { actual } => bail!(
                "ref {ant_ref} moved during the import (now {actual:?}); the imported commits are \
                 recorded, re-run the import to publish them"
            ),
        }
    }

    pub async fn release(self) {
        for w in self.witnesses {
            let _ = w.lock.release().await;
        }
    }
}

enum Item {
    Batch(JoinHandle<anyhow::Result<Counts>>, Vec<NewCommit>),
    Tip { ant_ref: String, id: ObjectId, ack: std::sync::mpsc::SyncSender<()> },
}

pub(crate) async fn write(
    backend: &Backend,
    mut rx: mpsc::Receiver<Msg>,
    stats: &mut ImportStats,
    publisher: Publisher,
    config: Recorder,
) -> anyhow::Result<()> {
    let deferred = publisher.deferred();
    let (rec_tx, rec_rx) = mpsc::channel::<Item>(RECORD_QUEUE);
    let recorder = tokio::spawn(record(backend.clone(), rec_rx, publisher, config));
    let budget = Arc::new(Semaphore::new(INFLIGHT_BYTES >> 20));
    while let Some(msg) = rx.recv().await {
        let item = match msg {
            Msg::Batch { objects, bytes, commits, check_present } => {
                let mb = ((bytes >> 20) + 1).min(INFLIGHT_BYTES >> 20) as u32;
                let permit = budget.clone().acquire_many_owned(mb).await?;
                let b = backend.clone();
                let task = tokio::spawn(async move {
                    let _permit = permit;
                    durability(deferred, store_objects(&b, objects, check_present)).await
                });
                Item::Batch(task, commits)
            }
            Msg::Tip { ant_ref, id, ack } => Item::Tip { ant_ref, id, ack },
        };
        if rec_tx.send(item).await.is_err() {
            break; // the recorder stopped; its error is reported below
        }
    }
    drop(rec_tx);
    *stats = recorder.await.context("commit recorder")??;
    Ok(())
}

/// Record commits in order: wait for each batch's objects, merge small
/// batches, and keep the derived mapping one step behind so it overlaps the
/// next commit write. A mapping is only written after its commits.
async fn record(
    backend: Backend,
    rx: mpsc::Receiver<Item>,
    mut publisher: Publisher,
    config: Recorder,
) -> anyhow::Result<ImportStats> {
    let deferred = publisher.deferred();
    let result = durability(deferred, record_inner(&backend, rx, &mut publisher, &config)).await;
    publisher.release().await;
    result
}

async fn record_inner(
    backend: &Backend,
    mut rx: mpsc::Receiver<Item>,
    publisher: &mut Publisher,
    config: &Recorder,
) -> anyhow::Result<ImportStats> {
    let deferred = publisher.deferred();
    let mut batches = 0usize;
    let mut stats = ImportStats::default();
    let mut pending: Vec<NewCommit> = Vec::new();
    let mut mapping_task: Option<JoinHandle<anyhow::Result<()>>> = None;

    async fn flush(
        deferred: bool,
        backend: &Backend,
        pending: &mut Vec<NewCommit>,
        mapping_task: &mut Option<JoinHandle<anyhow::Result<()>>>,
        stats: &mut ImportStats,
    ) -> anyhow::Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let mut mapping = Vec::with_capacity(pending.len());
        let mut records = Vec::with_capacity(pending.len());
        for c in pending.drain(..) {
            mapping.push((c.git.as_slice().to_vec(), c.node.id.as_bytes().to_vec().into()));
            records.push((c.node, c.changes));
        }
        stats.commits += records.len() as u64;
        backend.meta.put_commits(records).await?;
        wait_mapping(mapping_task).await?;
        let meta = backend.meta.clone();
        *mapping_task = Some(tokio::spawn(async move {
            // After the commits: a mapping must never point at an unrecorded commit.
            durability(deferred, meta.put_derived_many(COMMIT_KIND, mapping)).await?;
            Ok(())
        }));
        Ok(())
    }

    while let Some(item) = rx.recv().await {
        match item {
            Item::Batch(task, commits) => {
                let counts = task.await.context("object writer task")??;
                stats.trees += counts.trees;
                stats.blobs += counts.blobs;
                pending.extend(commits);
                batches += 1;
                if pending.len() >= config.commits {
                    flush(deferred, backend, &mut pending, &mut mapping_task, &mut stats).await?;
                }
                if let Some(hook) = &config.hook {
                    flush(deferred, backend, &mut pending, &mut mapping_task, &mut stats).await?;
                    wait_mapping(&mut mapping_task).await?;
                    tokio::task::block_in_place(|| (hook.0)(batches))?;
                }
            }
            Item::Tip { ant_ref, id, ack } => {
                flush(deferred, backend, &mut pending, &mut mapping_task, &mut stats).await?;
                wait_mapping(&mut mapping_task).await?;
                publisher.publish(backend, &ant_ref, id).await?;
                let _ = ack.send(());
            }
        }
    }
    flush(deferred, backend, &mut pending, &mut mapping_task, &mut stats).await?;
    wait_mapping(&mut mapping_task).await?;
    Ok(stats)
}

/// Wait for the mapping write in flight, if any.
async fn wait_mapping(task: &mut Option<JoinHandle<anyhow::Result<()>>>) -> anyhow::Result<()> {
    if let Some(prev) = task.take() {
        prev.await.context("mapping writer")??;
    }
    Ok(())
}

/// Run `fut` with [`ant_store::deferred_durability`] when `deferred` (see
/// [`Publisher`]), else as is.
async fn durability<F: std::future::Future>(deferred: bool, fut: F) -> F::Output {
    if deferred { ant_store::deferred_durability(fut).await } else { fut.await }
}

/// Store the objects the target does not already hold; only those are counted.
async fn store_objects(
    backend: &Backend,
    objects: Vec<ant_core::EncodedObject>,
    check_present: bool,
) -> anyhow::Result<Counts> {
    let mut counts = Counts::default();
    if objects.is_empty() {
        return Ok(counts);
    }
    let missing: Vec<_> = if check_present {
        let ids: Vec<ObjectId> = objects.iter().map(|o| o.id()).collect();
        let present = backend.objects.has_many(&ids).await?;
        objects.into_iter().zip(present).filter_map(|(o, p)| (!p).then_some(o)).collect()
    } else {
        objects
    };
    for obj in &missing {
        match obj.kind() {
            ObjectKind::Blob => counts.blobs += 1,
            ObjectKind::Tree => counts.trees += 1,
            ObjectKind::Commit => {}
        }
    }
    if !missing.is_empty() {
        backend.objects.put_many(&missing).await?;
    }
    Ok(counts)
}
