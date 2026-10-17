use std::sync::Arc;

use ant_store::ObjectStoreExt;
use ant_store::graph::is_ancestor;

use super::*;
use crate::ops::test_support::{commit, delete, read, tip, write};

#[tokio::test]
async fn create_and_fast_forward() {
    let b = Backend::in_memory();
    let c1 = commit(&b, None, vec![write("f", "1")]).await.unwrap();
    assert_eq!(
        land(&b, "main", c1).await.unwrap(),
        LandOutcome::Landed { new_tip: c1, rebased: false }
    );
    let c2 = commit(&b, Some(c1), vec![write("f", "2")]).await.unwrap();
    assert_eq!(
        land(&b, "main", c2).await.unwrap(),
        LandOutcome::Landed { new_tip: c2, rebased: false }
    );
    // Re-landing an ancestor is a no-op.
    assert_eq!(
        land(&b, "main", c1).await.unwrap(),
        LandOutcome::Landed { new_tip: c2, rebased: false }
    );
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
    let base = commit(&b, None, vec![write("f", "x\n"), write("g", "g\n"), write("h", "h\n")])
        .await
        .unwrap();
    land(&b, "main", base).await.unwrap();
    let x = commit(&b, Some(base), vec![write("f", "ours\n"), delete("g"), write("h", "same\n")])
        .await
        .unwrap();
    let y = commit(
        &b,
        Some(base),
        vec![write("f", "theirs\n"), write("g", "mod\n"), write("h", "same\n")],
    )
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
    let y =
        commit(&b, Some(base), vec![write("p2", "unrelated"), write("p", "edited")]).await.unwrap();
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
            author: crate::ops::test_support::sig(),
            committer: crate::ops::test_support::sig(),
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
        commits.push(
            commit(&b, Some(base), vec![write(&format!("agent{i}/out"), "done")]).await.unwrap(),
        );
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
        assert_eq!(
            read(&b, t, &format!("agent{i}/out")).await.as_deref(),
            Some("done"),
            "agent{i}"
        );
    }
    // Linear first-parent history of N+1 commits.
    let node = b.meta.get_commits(&[t]).await.unwrap().pop().unwrap().unwrap();
    assert_eq!(node.generation as usize, N + 1);
}

/// A recorded commit whose object is gone (a collection condemned the
/// unreachable changeset while it was being landed) lands as `NotFound`.
#[tokio::test]
async fn landing_a_commit_whose_object_is_gone_is_not_found() {
    let b = Backend::in_memory();
    let base = commit(&b, None, vec![write("a", "a")]).await.unwrap();
    land(&b, "main", base).await.unwrap();
    let x = commit(&b, Some(base), vec![write("a", "A")]).await.unwrap();
    land(&b, "main", x).await.unwrap();
    let tree = b.objects.read_commit(&base).await.unwrap().tree;
    let gone = ObjectId([9; 32]);
    let node = ant_store::CommitNode {
        id: gone,
        tree,
        parents: vec![base],
        generation: 2,
        committer_time: 1,
    };
    b.meta.put_commit(node, vec![]).await.unwrap();
    let err = land(&b, "main", gone).await.unwrap_err();
    assert!(matches!(err, OpError::NotFound(_)), "{err:?}");
}
