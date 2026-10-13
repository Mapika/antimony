//! Landing: advance a ref to include a commit, rebasing server-side when the
//! ref moved. This replaces a merge queue — conflicts are rare at file
//! granularity and are reported back to the agent to fix and retry.
//!
//! TASK (server-land): implement `land` and `rebase`.
//!
//! `rebase(b, commit, onto)`:
//! - `commit` must be recorded and have exactly one parent `P`; merge
//!   commits → `InvalidArgument`.
//! - Take `commit`'s recorded changeset (vs `P`). For each change at path:
//!     base = entry at path in P's tree, ours = entry in `onto`'s tree,
//!     theirs = change.new.
//!   * ours == base            → apply theirs (write or delete)
//!   * ours == theirs          → already applied, skip
//!   * base, ours, theirs all regular files (File/Executable) with
//!     `ours.mode == base.mode` or `theirs.mode == base.mode` (pick the
//!     changed mode), and all three blobs valid UTF-8 → 3-way text merge
//!     with `diffy::merge(base, ours, theirs)`; clean → write merged blob;
//!     conflict → path conflicts.
//!   * anything else (modify/delete, add/add with different content,
//!     symlinks, binary) → path conflicts.
//! - Structural conflicts when applying onto `onto` (e.g. `onto` turned a
//!   parent dir into a file) → path conflicts too.
//! - If any conflicts: return `Err(paths)` sorted, write nothing that
//!   matters (unreferenced objects are fine).
//! - Otherwise create the rebased commit with parent `[onto]`, the original
//!   author, committer and message preserved verbatim, via
//!   `changeset::create_changeset` (or equivalent tree building + write_commit).
//!   Return `Ok(new_commit_id)`.
//!
//! `land(b, ref_name, commit)`:
//! - `commit` must be recorded (else `NotFound`).
//! - ref absent → create it at `commit` (CAS expected=None).
//! - `commit` already an ancestor of tip → `Landed { new_tip: tip, rebased: false }`.
//! - tip is an ancestor of `commit` → fast-forward CAS, `rebased: false`.
//! - else `rebase(commit, tip)`; conflict → `Conflict { tip, paths }`;
//!   success → CAS tip→rebased, `rebased: true`.
//! - On CAS mismatch, retry from the new tip (rebasing the ORIGINAL commit
//!   again); after 32 failed attempts → `Aborted`.
//! - Must be safe under heavy concurrency: N concurrent lands of
//!   non-conflicting commits must all land, each exactly once.

use ant_core::{ObjectId, RepoPath};
use ant_store::Backend;

use super::OpResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LandOutcome {
    Landed { new_tip: ObjectId, rebased: bool },
    Conflict { tip: ObjectId, paths: Vec<RepoPath> },
}

pub const MAX_LAND_ATTEMPTS: usize = 32;

pub async fn rebase(
    b: &Backend,
    commit: ObjectId,
    onto: ObjectId,
) -> OpResult<Result<ObjectId, Vec<RepoPath>>> {
    let _ = (b, commit, onto);
    todo!("server-land")
}

pub async fn land(b: &Backend, ref_name: &str, commit: ObjectId) -> OpResult<LandOutcome> {
    let _ = (b, ref_name, commit);
    todo!("server-land")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::ops::OpError;
    use crate::ops::changeset::tests::{commit, delete, read, write};
    use ant_store::graph::is_ancestor;
    use ant_store::ObjectStoreExt;

    async fn tip(b: &Backend) -> ObjectId {
        b.refs.get_ref("main").await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn create_and_fast_forward() {
        let b = Backend::in_memory();
        let c1 = commit(&b, None, vec![write("f", "1")]).await.unwrap();
        assert_eq!(land(&b, "main", c1).await.unwrap(), LandOutcome::Landed { new_tip: c1, rebased: false });
        let c2 = commit(&b, Some(c1), vec![write("f", "2")]).await.unwrap();
        assert_eq!(land(&b, "main", c2).await.unwrap(), LandOutcome::Landed { new_tip: c2, rebased: false });
        // Re-landing an ancestor is a no-op.
        assert_eq!(land(&b, "main", c1).await.unwrap(), LandOutcome::Landed { new_tip: c2, rebased: false });
        assert_eq!(tip(&b).await, c2);
    }

    #[tokio::test]
    async fn rebase_disjoint_paths() {
        let b = Backend::in_memory();
        let base = commit(&b, None, vec![write("a", "a"), write("b", "b")]).await.unwrap();
        land(&b, "main", base).await.unwrap();
        let x = commit(&b, Some(base), vec![write("a", "A")]).await.unwrap();
        let y = commit(&b, Some(base), vec![write("b", "B"), write("new/file", "n")]).await.unwrap();
        land(&b, "main", x).await.unwrap();
        let out = land(&b, "main", y).await.unwrap();
        let LandOutcome::Landed { new_tip, rebased: true } = out else { panic!("{out:?}") };
        assert_eq!(read(&b, new_tip, "a").await.as_deref(), Some("A"));
        assert_eq!(read(&b, new_tip, "b").await.as_deref(), Some("B"));
        assert_eq!(read(&b, new_tip, "new/file").await.as_deref(), Some("n"));
        let rebased = b.objects.read_commit(&new_tip).await.unwrap();
        let orig = b.objects.read_commit(&y).await.unwrap();
        assert_eq!(rebased.parents, vec![x]);
        assert_eq!(rebased.author, orig.author);
        assert_eq!(rebased.message, orig.message);
        assert!(is_ancestor(&b, x, new_tip).await.unwrap());
    }

    #[tokio::test]
    async fn three_way_text_merge_same_file() {
        let b = Backend::in_memory();
        let text = "l1\nl2\nl3\nl4\nl5\nl6\n";
        let base = commit(&b, None, vec![write("f", text)]).await.unwrap();
        land(&b, "main", base).await.unwrap();
        let x = commit(&b, Some(base), vec![write("f", "L1\nl2\nl3\nl4\nl5\nl6\n")]).await.unwrap();
        let y = commit(&b, Some(base), vec![write("f", "l1\nl2\nl3\nl4\nl5\nL6\n")]).await.unwrap();
        land(&b, "main", x).await.unwrap();
        let LandOutcome::Landed { new_tip, .. } = land(&b, "main", y).await.unwrap() else { panic!() };
        assert_eq!(read(&b, new_tip, "f").await.as_deref(), Some("L1\nl2\nl3\nl4\nl5\nL6\n"));
    }

    #[tokio::test]
    async fn conflicts_are_reported_not_landed() {
        let b = Backend::in_memory();
        let base = commit(&b, None, vec![write("f", "x\n"), write("g", "g\n"), write("h", "h\n")]).await.unwrap();
        land(&b, "main", base).await.unwrap();
        let x = commit(&b, Some(base), vec![write("f", "ours\n"), delete("g"), write("h", "same\n")]).await.unwrap();
        let y = commit(&b, Some(base), vec![write("f", "theirs\n"), write("g", "mod\n"), write("h", "same\n")])
            .await
            .unwrap();
        land(&b, "main", x).await.unwrap();
        let out = land(&b, "main", y).await.unwrap();
        let paths: Vec<_> = match out {
            LandOutcome::Conflict { tip: t, paths } => {
                assert_eq!(t, x);
                paths.iter().map(|p| p.as_str().to_string()).collect()
            }
            other => panic!("{other:?}"),
        };
        // h: both sides made the identical change → not a conflict.
        assert_eq!(paths, ["f", "g"]);
        assert_eq!(tip(&b).await, x);
    }

    #[tokio::test]
    async fn structural_conflict() {
        let b = Backend::in_memory();
        let base = commit(&b, None, vec![write("p", "file")]).await.unwrap();
        land(&b, "main", base).await.unwrap();
        let x = commit(&b, Some(base), vec![delete("p"), write("p/q", "dir now")]).await.unwrap();
        let y = commit(&b, Some(base), vec![write("p2", "unrelated"), write("p", "edited")]).await.unwrap();
        land(&b, "main", x).await.unwrap();
        assert!(matches!(land(&b, "main", y).await.unwrap(), LandOutcome::Conflict { .. }));
    }

    #[tokio::test]
    async fn rejects_unrecorded_and_merge_commits() {
        let b = Backend::in_memory();
        assert!(matches!(land(&b, "main", ObjectId([1; 32])).await, Err(OpError::NotFound(_))));
        let r = commit(&b, None, vec![write("f", "1")]).await.unwrap();
        let a = commit(&b, Some(r), vec![write("a", "1")]).await.unwrap();
        let c = commit(&b, Some(r), vec![write("c", "1")]).await.unwrap();
        let m = ant_store::graph::write_commit(
            &b,
            &ant_core::Commit {
                tree: b.objects.read_commit(&a).await.unwrap().tree,
                parents: vec![a, c],
                author: crate::ops::changeset::tests::sig(),
                committer: crate::ops::changeset::tests::sig(),
                extra_headers: vec![],
                message: "merge".into(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(rebase(&b, m, r).await, Err(OpError::InvalidArgument(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_lands_all_succeed() {
        let b = Backend::in_memory();
        let base = commit(&b, None, vec![write("seed", "s")]).await.unwrap();
        land(&b, "main", base).await.unwrap();
        const N: usize = 24;
        let mut commits = Vec::new();
        for i in 0..N {
            commits.push(commit(&b, Some(base), vec![write(&format!("agent{i}/out"), "done")]).await.unwrap());
        }
        let b = Arc::new(b);
        let mut tasks = Vec::new();
        for c in commits {
            let b = b.clone();
            tasks.push(tokio::spawn(async move { land(&b, "main", c).await }));
        }
        for t in tasks {
            assert!(matches!(t.await.unwrap().unwrap(), LandOutcome::Landed { .. }));
        }
        let t = tip(&b).await;
        for i in 0..N {
            assert_eq!(read(&b, t, &format!("agent{i}/out")).await.as_deref(), Some("done"), "agent{i}");
        }
        // Linear first-parent history of N+1 commits.
        let node = b.meta.get_commits(&[t]).await.unwrap().pop().unwrap().unwrap();
        assert_eq!(node.generation as usize, N + 1);
    }
}
