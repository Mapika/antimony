//! Per-request permission pre-checks for [`super::land_as`].

use std::collections::HashSet;

use ant_core::ObjectId;
use ant_store::{Backend, graph};

use super::MAX_STACK;
use super::replay::BatchState;
use crate::acl::Access;
use crate::ops::{OpError, OpResult, cache};

/// What a land request's permission pre-check (run against the ref tip seen
/// before joining a batch) proved the caller may do.
#[derive(Debug, Clone)]
pub(super) enum Verified {
    /// Open mode or admin.
    All,
    /// The ref did not exist: only creating it was checked.
    Creation,
    /// Every changeset on `commit`'s first-parent chain down to (excluding)
    /// `stop`, the first commit already landed (in the ref's history or mapped
    /// to a landed rebase), is writable by the caller. Covers replaying any
    /// of them, and fast-forwarding from any tip on that chain.
    Chain { set: HashSet<ObjectId>, stop: Option<ObjectId> },
    /// The tree diff from `from` to `commit` is writable (fast-forward only;
    /// e.g. merges whose first-parent changesets carry other people's work).
    Exact { from: ObjectId },
}

const RETRY: &str = "ref moved while checking permissions; retry";

impl Verified {
    pub(super) fn allows_fast_forward(&self, tip: ObjectId) -> OpResult<()> {
        match self {
            Verified::All => Ok(()),
            Verified::Chain { set, stop } if *stop == Some(tip) || set.contains(&tip) => Ok(()),
            Verified::Exact { from } if *from == tip => Ok(()),
            _ => Err(OpError::Aborted(RETRY.into())),
        }
    }

    pub(super) fn allows_replay(&self, stack: &[ObjectId]) -> OpResult<()> {
        match self {
            Verified::All => Ok(()),
            Verified::Chain { set, .. } if stack.iter().all(|c| set.contains(c)) => Ok(()),
            // Includes `Exact` (only a fast-forward was verified): a retry
            // re-verifies against the new tip.
            _ => Err(OpError::Aborted(RETRY.into())),
        }
    }
}

/// Per-request permission pre-check for `land_as`, run before the request
/// joins a batch (so a denied request never touches anyone else's).
pub(super) async fn verify_land(
    b: &Backend,
    access: &Access,
    ref_name: &str,
    commit: ObjectId,
) -> OpResult<Verified> {
    if !access.policy.can_land(&access.principal, ref_name) {
        return Err(OpError::PermissionDenied(format!("cannot land onto {ref_name}")));
    }
    if access.is_admin() {
        return Ok(Verified::All);
    }
    let mut state = BatchState::default();
    state.nodes(b, &[commit]).await?;
    let Some(tip) = b.refs.get_ref(ref_name).await? else {
        return Ok(Verified::Creation);
    };
    let mut chain = Vec::new();
    let mut stop = None;
    let mut complete = true;
    let mut cur = Some(commit);
    while let Some(c) = cur {
        if state.already_landed(b, c, tip).await? {
            stop = Some(c);
            break;
        }
        if chain.len() >= MAX_STACK {
            complete = false;
            break;
        }
        state.nodes(b, &[c]).await?;
        cur = state.nodes[&c].parents.first().copied();
        chain.push(c);
    }
    let mut verdict =
        Err(OpError::PermissionDenied(format!("more than {MAX_STACK} commits to verify")));
    if complete {
        verdict = Ok(());
        let sets = b.meta.get_changesets(&chain).await?;
        for (id, set) in chain.iter().zip(sets) {
            let set = set.ok_or_else(|| OpError::not_found_object(*id))?;
            verdict = access.check_writable(set.iter().map(|c| &c.path));
            if verdict.is_err() {
                break;
            }
        }
    }
    if !state.ancestor(b, tip, commit).await? {
        // Will be replayed: every replayed changeset was checked above.
        verdict?;
        return Ok(Verified::Chain { set: chain.into_iter().collect(), stop });
    }
    if verdict.is_ok() && (stop == Some(tip) || chain.contains(&tip)) {
        // Fast-forward along the first-parent chain: its changesets compose
        // to exactly the tree diff.
        return Ok(Verified::Chain { set: chain.into_iter().collect(), stop });
    }
    // Fast-forward through history the changesets do not describe relative
    // to `tip` (tip reached via a merge's second parent, or merges carrying
    // other people's changes): what changes is the trees' net diff.
    state.nodes(b, &[tip]).await?;
    let (old, new) = (state.nodes[&tip].tree, state.nodes[&commit].tree);
    let trees = cache::CachedTrees(b);
    let changes = graph::diff_trees_with(&trees, Some(old), Some(new)).await?;
    access.check_writable(changes.iter().map(|c| &c.path))?;
    Ok(Verified::Exact { from: tip })
}
