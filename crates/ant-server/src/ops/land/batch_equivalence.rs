//! The batched in-memory replay must be indistinguishable from landing the
//! same requests one at a time: same outcomes (hence same rebased commit
//! ids, i.e. same trees and metadata), same conflict sets, same recorded
//! changesets, and changesets equal to the tree diff.

use std::collections::BTreeSet;

use ant_store::graph::diff_trees;
use proptest::prelude::*;

use super::queue::enqueue;
use super::*;
use crate::ops::changeset::FileOp;
use crate::ops::test_support::{commit, delete, write};

const PATHS: [&str; 6] = ["a", "b", "d/x", "d/y", "e/f/g", "hot"];
const HOT: &str = "l0\nl1\nl2\nl3\nl4\nl5\nl6\nl7\n";

#[derive(Debug, Clone)]
enum Op {
    Write(usize, u8),
    Delete(usize),
    HotLine(usize, u8),
}

#[derive(Debug, Clone)]
struct Change {
    /// `None`: based on the seed; `Some(i)`: stacked on an earlier change.
    parent: Option<usize>,
    ops: Vec<Op>,
}

fn change() -> impl Strategy<Value = Change> {
    let op = prop_oneof![
        (0..PATHS.len() - 1, 0u8..3).prop_map(|(p, v)| Op::Write(p, v)),
        (0..PATHS.len() - 1).prop_map(Op::Delete),
        (0usize..8, 0u8..3).prop_map(|(l, v)| Op::HotLine(l, v)),
    ];
    (prop::option::weighted(0.25, 0usize..64), prop::collection::vec(op, 1..4))
        .prop_map(|(parent, ops)| Change { parent, ops })
}

fn file_ops(ops: &[Op]) -> Vec<FileOp> {
    let mut seen = BTreeSet::new();
    let mut hot: Vec<String> = HOT.lines().map(str::to_owned).collect();
    let mut hot_touched = false;
    let mut out = Vec::new();
    for op in ops {
        match *op {
            Op::Write(p, v) if seen.insert(p) => out.push(write(PATHS[p], &format!("v{v}"))),
            Op::Delete(p) if seen.insert(p) => out.push(delete(PATHS[p])),
            Op::HotLine(l, v) => {
                hot[l] = format!("L{l}v{v}");
                hot_touched = true;
            }
            _ => {}
        }
    }
    if hot_touched {
        out.push(write("hot", &(hot.join("\n") + "\n")));
    }
    out
}

/// Create the same commits in `b` (deterministic ids); `None` where
/// creation fails (e.g. deleting a path the parent already deleted).
async fn create(b: &Backend, changes: &[Change]) -> (ObjectId, Vec<Option<ObjectId>>) {
    let seed = commit(
        b,
        None,
        PATHS.iter().map(|p| write(p, if *p == "hot" { HOT } else { "seed" })).collect(),
    )
    .await
    .unwrap();
    let mut ids: Vec<Option<ObjectId>> = Vec::new();
    for change in changes {
        let parent = change
            .parent
            .filter(|_| !ids.is_empty())
            .and_then(|i| ids[i % ids.len()])
            .unwrap_or(seed);
        ids.push(commit(b, Some(parent), file_ops(&change.ops)).await.ok());
    }
    (seed, ids)
}

fn summary(outcomes: Vec<OpResult<LandOutcome>>) -> Vec<Result<LandOutcome, String>> {
    outcomes.into_iter().map(|o| o.map_err(|e| e.to_string())).collect()
}

async fn check(changes: Vec<Change>) -> Result<(), TestCaseError> {
    let batched = Backend::in_memory();
    let sequential = Backend::in_memory();
    let (seed, ids) = create(&batched, &changes).await;
    let (seed2, ids2) = create(&sequential, &changes).await;
    prop_assert_eq!(seed, seed2);
    prop_assert_eq!(&ids, &ids2);
    let ids: Vec<ObjectId> = ids.into_iter().flatten().collect();
    land(&batched, "main", seed).await.unwrap();
    land(&sequential, "main", seed).await.unwrap();

    // One batch: enqueue synchronously, then await.
    let replies: Vec<_> = ids.iter().map(|id| enqueue(&batched, "main", *id)).collect();
    let mut one_batch = Vec::new();
    for reply in replies {
        one_batch.push(reply.await.unwrap());
    }
    let mut one_by_one = Vec::new();
    for id in &ids {
        one_by_one.push(land(&sequential, "main", *id).await);
    }
    prop_assert_eq!(summary(one_batch), summary(one_by_one));
    let tip = batched.refs.get_ref("main").await.unwrap();
    prop_assert_eq!(tip, sequential.refs.get_ref("main").await.unwrap());

    // Same recorded history and changesets, each equal to its tree diff.
    let chain = batched.meta.first_parent_chain(tip.unwrap(), Some(seed), 1000).await.unwrap();
    for node in chain {
        let parent = batched.meta.get_commits(&node.parents).await.unwrap()[0].clone().unwrap();
        let recorded = batched.meta.get_changeset(&node.id).await.unwrap().unwrap();
        prop_assert_eq!(
            &recorded,
            &sequential.meta.get_changeset(&node.id).await.unwrap().unwrap()
        );
        let diff =
            diff_trees(batched.objects.as_ref(), Some(parent.tree), Some(node.tree)).await.unwrap();
        prop_assert_eq!(recorded, diff);
        prop_assert_eq!(
            batched.objects.get_many(&[node.id]).await.unwrap(),
            sequential.objects.get_many(&[node.id]).await.unwrap()
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn batch_replay_matches_sequential_landing(
        changes in prop::collection::vec(change(), 1..24)
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(check(changes))?;
    }
}
