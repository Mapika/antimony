//! Landing: advance a ref to include a commit, rebasing server-side when the
//! ref moved. This replaces a merge queue: conflicts are rare at file
//! granularity and are reported back to the agent to fix and retry.
//!
//! [`rebase`] replays a recorded single-parent commit's changeset onto
//! another commit. For each changed path, with base = the entry in the
//! parent's tree, ours = the entry in `onto`'s tree and theirs = the new
//! entry:
//! - ours == theirs → already applied, skipped;
//! - ours == base → theirs is applied (write or delete);
//! - base, ours and theirs are all regular files whose mode changed on at
//!   most one side and whose blobs are UTF-8 → 3-way text merge
//!   (`diffy::merge`); a clean merge writes the merged blob;
//! - anything else (modify/delete, add/add, symlinks, binary, text merge
//!   conflicts, structural conflicts such as a directory turned into a file)
//!   → the path conflicts.
//!
//! Conflicts are returned sorted and nothing reachable is written. Otherwise
//! the rebased commit keeps the original author, committer and message, with
//! parent `[onto]`. Merge commits are `InvalidArgument`.
//!
//! [`land`] / [`land_as`]: an absent ref is created; a commit already in the
//! ref's history (directly or via a recorded landed-as mapping) is a no-op; a
//! descendant of the tip fast-forwards; anything else is rebased (with its
//! whole unlanded first-parent stack, up to [`MAX_STACK`] commits).
//!
//! Concurrent requests are queued per backend and ref (`queue`). A detached
//! worker replays up to 64 of them in arrival order in memory (`replay`),
//! writes the new objects and commit records, then publishes the final tip
//! with one CAS. Conflicts do not advance the tentative tip; each reply
//! carries the request's own resulting commit (or the tip it conflicted
//! with). On CAS mismatch the batch is replayed from the new tip, rebasing
//! the original commits again; after [`MAX_LAND_ATTEMPTS`] attempts every
//! request is `Aborted`. Landed-as mappings become durable after the CAS and
//! before replying. Caller cancellation never cancels the worker. Under a
//! policy, permissions are checked per request before it joins a batch
//! (`verify`).

mod queue;
mod replay;
mod verify;

use ant_core::{ObjectId, RepoPath};
use ant_store::Backend;

use super::{OpError, OpResult};
use crate::acl::Access;
pub(crate) use queue::queue_stats;
use queue::enqueue_checked;
use replay::BatchState;
use verify::{Verified, verify_land};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LandOutcome {
    Landed { new_tip: ObjectId, rebased: bool },
    Conflict { tip: ObjectId, paths: Vec<RepoPath> },
}

pub const MAX_LAND_ATTEMPTS: usize = 32;

/// Longest unlanded stack `land` will replay.
pub const MAX_STACK: usize = 256;

/// Derived-data kind mapping a commit id to the id it was landed as after a
/// server-side rebase. Makes retries of a land idempotent.
const LANDED_AS: &str = "land/landed-as/v1";

pub async fn rebase(
    b: &Backend,
    commit: ObjectId,
    onto: ObjectId,
) -> OpResult<Result<ObjectId, Vec<RepoPath>>> {
    let mut state = BatchState::default();
    let result = state.rebase(b, None, commit, onto).await?;
    if result.is_ok() {
        state.flush(b).await?;
    }
    Ok(result)
}

pub async fn land(b: &Backend, ref_name: &str, commit: ObjectId) -> OpResult<LandOutcome> {
    land_as(b, None, ref_name, commit).await
}

/// [`land`] on behalf of `access` (`None` = open mode). The caller must be
/// allowed to land onto `ref_name` and to write every path changed by every
/// commit that gets replayed (or, for a fast-forward, by the net diff).
/// Checked per request before it joins a group-commit batch.
pub async fn land_as(
    b: &Backend,
    access: Option<&Access>,
    ref_name: &str,
    commit: ObjectId,
) -> OpResult<LandOutcome> {
    ant_store::validate_ref_name(ref_name)?;
    let (verified, policy) = match access {
        Some(a) => (verify_land(b, a, ref_name, commit).await?, Some(a.policy.clone())),
        None => (Verified::All, None),
    };
    enqueue_checked(b, ref_name, commit, verified, policy)
        .await
        .map_err(|_| OpError::Aborted("land worker stopped before replying".into()))?
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod independent_tests;

#[cfg(test)]
mod group_tests;

#[cfg(test)]
mod batch_equivalence;
