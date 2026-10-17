//! Helpers shared by the `ops` unit tests.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ant_core::{EntryMode, ObjectId, RepoPath, Signature};
use ant_store::graph::lookup_path;
use ant_store::{Backend, CasOutcome, ObjectStoreExt, RefStore, StoreError};
use bytes::Bytes;
use tokio::sync::{Notify, Semaphore};
use tokio::time::timeout;

use super::OpResult;
use super::changeset::{ChangesetRequest, FileOp, WriteContent, create_changeset};
use super::land::LandOutcome;

/// Upper bound for waits on land replies and gated store calls.
pub const DEADLINE: Duration = Duration::from_secs(20);

pub fn sig() -> Signature {
    Signature {
        name: "agent".into(),
        email: "a@x".into(),
        time: 1_700_000_000,
        tz_offset_minutes: 0,
    }
}

pub fn write(path: &str, content: &str) -> FileOp {
    write_bytes(path, content.as_bytes())
}

pub fn write_bytes(path: &str, content: &[u8]) -> FileOp {
    FileOp::Write {
        path: RepoPath::new(path).unwrap(),
        mode: EntryMode::File,
        content: WriteContent::Inline(Bytes::copy_from_slice(content)),
    }
}

pub fn delete(path: &str) -> FileOp {
    FileOp::Delete { path: RepoPath::new(path).unwrap() }
}

pub async fn commit(b: &Backend, base: Option<ObjectId>, ops: Vec<FileOp>) -> OpResult<ObjectId> {
    create_changeset(
        b,
        ChangesetRequest { base, ops, author: sig(), committer: None, message: "msg\n".into() },
    )
    .await
}

pub async fn read(b: &Backend, commit_id: ObjectId, path: &str) -> Option<String> {
    Some(String::from_utf8(read_bytes(b, commit_id, path).await?).unwrap())
}

pub async fn read_bytes(b: &Backend, commit_id: ObjectId, path: &str) -> Option<Vec<u8>> {
    let c = b.objects.read_commit(&commit_id).await.unwrap();
    let (mode, id) =
        lookup_path(b.objects.as_ref(), c.tree, &RepoPath::new(path).unwrap()).await.unwrap()?;
    assert!(mode.is_blob(), "{path} is not a blob");
    Some(b.objects.read_blob(&id).await.unwrap().to_vec())
}

/// The target of `main`.
pub async fn tip(b: &Backend) -> ObjectId {
    b.refs.get_ref("main").await.unwrap().unwrap()
}

/// The commits `main` gained since `root`, oldest first; the history must be
/// linear.
pub async fn history(b: &Backend, root: ObjectId) -> Vec<ObjectId> {
    let mut current = tip(b).await;
    let mut chain = Vec::new();
    while current != root {
        assert!(!chain.contains(&current));
        chain.push(current);
        let node = b.objects.read_commit(&current).await.unwrap();
        assert_eq!(node.parents.len(), 1);
        current = node.parents[0];
    }
    chain.reverse();
    chain
}

/// Await queued land replies (within [`DEADLINE`]), in order.
pub async fn replies(
    receivers: Vec<tokio::sync::oneshot::Receiver<OpResult<LandOutcome>>>,
) -> Vec<OpResult<LandOutcome>> {
    timeout(DEADLINE, futures::future::join_all(receivers))
        .await
        .unwrap()
        .into_iter()
        .map(Result::unwrap)
        .collect()
}

/// Ref store wrapper for land tests: counts CASes; its next `get_ref` and/or
/// `cas_ref` can be held until released, and its CASes can be made to
/// mismatch (once, by another writer moving the ref first, or always) or to
/// fail once.
pub struct GatedRefs {
    inner: Arc<dyn RefStore>,
    pub cas_calls: AtomicUsize,
    pub hold_get: AtomicBool,
    pub get_entered: Notify,
    pub get_release: Semaphore,
    pub hold_cas: AtomicBool,
    pub cas_entered: Notify,
    pub cas_release: Semaphore,
    pub move_to: Mutex<Option<ObjectId>>,
    pub always_mismatch: AtomicBool,
    pub fail_cas: AtomicBool,
}

impl GatedRefs {
    /// Wrap `b`'s ref store (which also gives `b` its own land queue).
    pub fn install(b: &mut Backend) -> Arc<Self> {
        let refs = Arc::new(Self {
            inner: b.refs.clone(),
            cas_calls: AtomicUsize::new(0),
            hold_get: AtomicBool::new(false),
            get_entered: Notify::new(),
            get_release: Semaphore::new(0),
            hold_cas: AtomicBool::new(false),
            cas_entered: Notify::new(),
            cas_release: Semaphore::new(0),
            move_to: Mutex::new(None),
            always_mismatch: AtomicBool::new(false),
            fail_cas: AtomicBool::new(false),
        });
        b.refs = refs.clone();
        refs
    }
}

#[async_trait::async_trait]
impl RefStore for GatedRefs {
    async fn get_ref(&self, name: &str) -> ant_store::Result<Option<ObjectId>> {
        if self.hold_get.swap(false, Ordering::SeqCst) {
            self.get_entered.notify_one();
            self.get_release.acquire().await.unwrap().forget();
        }
        self.inner.get_ref(name).await
    }

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ObjectId>,
        new: Option<ObjectId>,
    ) -> ant_store::Result<CasOutcome> {
        self.cas_calls.fetch_add(1, Ordering::SeqCst);
        if self.always_mismatch.load(Ordering::SeqCst) {
            return Ok(CasOutcome::Mismatch { actual: expected });
        }
        if self.hold_cas.swap(false, Ordering::SeqCst) {
            self.cas_entered.notify_one();
            self.cas_release.acquire().await.unwrap().forget();
        }
        if self.fail_cas.swap(false, Ordering::SeqCst) {
            return Err(StoreError::InvalidArgument("injected CAS failure".into()));
        }
        let move_to = self.move_to.lock().unwrap().take();
        if let Some(id) = move_to {
            assert_eq!(self.inner.cas_ref(name, expected, Some(id)).await?, CasOutcome::Updated);
        }
        self.inner.cas_ref(name, expected, new).await
    }

    async fn list_refs(&self, prefix: &str) -> ant_store::Result<Vec<(String, ObjectId)>> {
        self.inner.list_refs(prefix).await
    }

    fn watch_refs(&self) -> Option<tokio::sync::broadcast::Receiver<ant_store::feed::RefNotice>> {
        self.inner.watch_refs()
    }
}
