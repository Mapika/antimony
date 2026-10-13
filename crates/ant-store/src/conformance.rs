//! Behavioral test suites every store implementation must pass. Call these
//! from each implementation's tests with a *fresh, empty* store.

use std::sync::Arc;

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, RepoPath};
use bytes::Bytes;

use crate::{
    CasOutcome, ChangeKind, CommitNode, HistoryRow, MetaStore, ObjectStore, ObjectStoreExt,
    PathChange, RefStore, StoreError,
};

fn blob(s: &[u8]) -> EncodedObject {
    EncodedObject::new(ObjectKind::Blob, s)
}

/// Object-store suite. `big` sizes cover inline vs. out-of-line paths of
/// tiered implementations.
pub async fn object_store(s: Arc<dyn ObjectStore>) {
    let a = blob(b"alpha");
    let b = blob(b"beta");
    let big = blob(&vec![7u8; 3 * 1024 * 1024]);
    let empty = blob(b"");
    let missing = blob(b"never stored").id();

    assert_eq!(s.get_many(&[a.id()]).await.unwrap(), vec![None]);
    s.put_many(&[a.clone(), b.clone(), big.clone(), empty.clone()]).await.unwrap();
    // Idempotent re-put.
    s.put_many(&[a.clone(), a.clone()]).await.unwrap();

    let got = s
        .get_many(&[a.id(), missing, b.id(), a.id(), big.id(), empty.id()])
        .await
        .unwrap();
    assert_eq!(
        got,
        vec![Some(a.clone()), None, Some(b.clone()), Some(a.clone()), Some(big.clone()), Some(empty.clone())]
    );
    assert_eq!(
        s.has_many(&[missing, a.id(), big.id()]).await.unwrap(),
        vec![false, true, true]
    );
    assert_eq!(s.get_many(&[]).await.unwrap(), vec![]);

    // Ext helpers + kind checking.
    assert_eq!(&s.read_blob(&a.id()).await.unwrap()[..], b"alpha");
    assert!(matches!(s.read_tree(&a.id()).await, Err(StoreError::WrongKind { .. })));
    assert!(matches!(s.get_required(&missing).await, Err(StoreError::NotFound(_))));

    // Many small objects in one batch.
    let many: Vec<_> = (0..500).map(|i| blob(format!("obj-{i}").as_bytes())).collect();
    s.put_many(&many).await.unwrap();
    let ids: Vec<_> = many.iter().map(|o| o.id()).collect();
    let got = s.get_many(&ids).await.unwrap();
    assert!(got.iter().zip(&many).all(|(g, o)| g.as_ref() == Some(o)));

    // Concurrent puts of the same object.
    let shared = blob(b"contended");
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let s = s.clone();
        let o = shared.clone();
        tasks.push(tokio::spawn(async move { s.put_many(&[o]).await }));
    }
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    assert_eq!(s.get(&shared.id()).await.unwrap(), Some(shared));
}

pub async fn ref_store(s: Arc<dyn RefStore>) {
    let x = ObjectId([1; 32]);
    let y = ObjectId([2; 32]);

    assert_eq!(s.get_ref("main").await.unwrap(), None);
    // Create requires expected = None.
    assert_eq!(
        s.cas_ref("main", Some(x), Some(y)).await.unwrap(),
        CasOutcome::Mismatch { actual: None }
    );
    assert_eq!(s.cas_ref("main", None, Some(x)).await.unwrap(), CasOutcome::Updated);
    assert_eq!(s.get_ref("main").await.unwrap(), Some(x));
    // Create-if-absent fails when present.
    assert_eq!(
        s.cas_ref("main", None, Some(y)).await.unwrap(),
        CasOutcome::Mismatch { actual: Some(x) }
    );
    assert_eq!(s.cas_ref("main", Some(x), Some(y)).await.unwrap(), CasOutcome::Updated);
    assert_eq!(s.get_ref("main").await.unwrap(), Some(y));

    // Delete.
    s.cas_ref("tmp/a", None, Some(x)).await.unwrap();
    assert_eq!(s.cas_ref("tmp/a", Some(x), None).await.unwrap(), CasOutcome::Updated);
    assert_eq!(s.get_ref("tmp/a").await.unwrap(), None);

    // Listing is prefix-filtered and sorted.
    for n in ["ws/b/1", "ws/a/2", "ws/a/1", "wsx", "release/1"] {
        s.cas_ref(n, None, Some(x)).await.unwrap();
    }
    let names: Vec<_> = s.list_refs("ws/").await.unwrap().into_iter().map(|(n, _)| n).collect();
    assert_eq!(names, ["ws/a/1", "ws/a/2", "ws/b/1"]);
    assert_eq!(s.list_refs("").await.unwrap().len(), 6);

    // Invalid names are rejected.
    for bad in ["", "a//b", "a/../b", "has space", "a:b", "/lead"] {
        assert!(
            matches!(s.cas_ref(bad, None, Some(x)).await, Err(StoreError::InvalidArgument(_))),
            "{bad:?}"
        );
    }

    // Linearizability under contention: N tasks each advance a counter ref
    // once via CAS-retry. Every increment must land exactly once.
    const N: u8 = 24;
    s.cas_ref("counter", None, Some(ObjectId([0; 32]))).await.unwrap();
    let mut tasks = Vec::new();
    for _ in 0..N {
        let s = s.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let cur = s.get_ref("counter").await.unwrap().unwrap();
                let next = ObjectId([cur.0[0] + 1; 32]);
                if s.cas_ref("counter", Some(cur), Some(next)).await.unwrap() == CasOutcome::Updated {
                    break;
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(s.get_ref("counter").await.unwrap(), Some(ObjectId([N; 32])));
}

pub async fn meta_store(s: Arc<dyn MetaStore>) {
    let c1 = ObjectId([1; 32]);
    let c2 = ObjectId([2; 32]);
    let n1 = CommitNode { id: c1, tree: ObjectId::empty_tree(), parents: vec![], generation: 1, committer_time: 100 };
    let n2 = CommitNode { id: c2, tree: ObjectId([9; 32]), parents: vec![c1], generation: 2, committer_time: 200 };
    let ch2 = vec![
        PathChange {
            path: RepoPath::new("a/x.rs").unwrap(),
            kind: ChangeKind::Added,
            old: None,
            new: Some((EntryMode::File, ObjectId([5; 32]))),
        },
        PathChange {
            path: RepoPath::new("b").unwrap(),
            kind: ChangeKind::Modified,
            old: Some((EntryMode::File, ObjectId([6; 32]))),
            new: Some((EntryMode::Executable, ObjectId([6; 32]))),
        },
        PathChange {
            path: RepoPath::new("c/link").unwrap(),
            kind: ChangeKind::Deleted,
            old: Some((EntryMode::Symlink, ObjectId([7; 32]))),
            new: None,
        },
    ];

    assert_eq!(s.tail(0, 10).await.unwrap(), vec![]);
    let s1 = s.put_commit(n1.clone(), vec![]).await.unwrap();
    let s2 = s.put_commit(n2.clone(), ch2.clone()).await.unwrap();
    assert!(s1 >= 1 && s2 > s1);
    // Idempotent.
    assert_eq!(s.put_commit(n2.clone(), ch2.clone()).await.unwrap(), s2);

    assert_eq!(
        s.get_commits(&[c2, ObjectId([3; 32]), c1]).await.unwrap(),
        vec![Some(n2.clone()), None, Some(n1.clone())]
    );
    assert_eq!(s.get_changeset(&c2).await.unwrap(), Some(ch2));
    assert_eq!(s.get_changeset(&c1).await.unwrap(), Some(vec![]));
    assert_eq!(s.get_changeset(&ObjectId([3; 32])).await.unwrap(), None);

    assert_eq!(s.tail(0, 10).await.unwrap(), vec![(s1, c1), (s2, c2)]);
    assert_eq!(s.tail(s1, 10).await.unwrap(), vec![(s2, c2)]);
    assert_eq!(s.tail(0, 1).await.unwrap(), vec![(s1, c1)]);
    assert_eq!(s.tail(s2, 10).await.unwrap(), vec![]);

    // Concurrent appends get distinct, increasing sequence numbers.
    let mut tasks = Vec::new();
    for i in 0..32u8 {
        let s = s.clone();
        tasks.push(tokio::spawn(async move {
            let id = ObjectId([100 + i; 32]);
            let n = CommitNode { id, tree: ObjectId::empty_tree(), parents: vec![c2], generation: 3, committer_time: 0 };
            s.put_commit(n, vec![]).await.unwrap()
        }));
    }
    let mut seqs = Vec::new();
    for t in tasks {
        seqs.push(t.await.unwrap());
    }
    seqs.sort_unstable();
    seqs.dedup();
    assert_eq!(seqs.len(), 32);
    let log = s.tail(0, 1000).await.unwrap();
    assert_eq!(log.len(), 34);
    assert!(log.windows(2).all(|w| w[0].0 < w[1].0));

    // Derived data.
    assert_eq!(s.get_derived("k/v1", b"key").await.unwrap(), None);
    s.put_derived("k/v1", b"key", Bytes::from_static(b"val")).await.unwrap();
    s.put_derived("k/v1", b"key", Bytes::from_static(b"val")).await.unwrap();
    assert_eq!(s.get_derived("k/v1", b"key").await.unwrap().as_deref(), Some(&b"val"[..]));
    assert_eq!(s.get_derived("k/v2", b"key").await.unwrap(), None);
    s.put_derived("k/v1", &[0, 255, 1], Bytes::from_static(&[9; 100])).await.unwrap();
    assert_eq!(s.get_derived("k/v1", &[0, 255, 1]).await.unwrap().unwrap().len(), 100);
}

/// History-index suite (part of the meta-store contract).
pub async fn history_index(s: Arc<dyn MetaStore>) {
    let c = |n: u8| ObjectId([n; 32]);
    let row = |p: &str, g: u64, n: u8| HistoryRow { path: RepoPath::new(p).unwrap(), generation: g, commit: c(n) };
    let p = |x: &str| RepoPath::new(x).unwrap();

    assert_eq!(s.query_history("main", &RepoPath::root(), u64::MAX, 10).await.unwrap(), vec![]);
    s.index_history(
        "main",
        &[row("src/a.rs", 1, 1), row("README", 1, 1), row("src/b/c.rs", 2, 2), row("docs/x", 3, 3), row("src/a.rs", 4, 4)],
    )
    .await
    .unwrap();
    // Idempotent.
    s.index_history("main", &[row("src/a.rs", 4, 4)]).await.unwrap();
    s.index_history("other", &[row("src/a.rs", 9, 9)]).await.unwrap();

    let q = |prefix: &str, max: u64, limit: usize| {
        let s = s.clone();
        let prefix = if prefix.is_empty() { RepoPath::root() } else { p(prefix) };
        async move { s.query_history("main", &prefix, max, limit).await.unwrap() }
    };
    assert_eq!(q("", u64::MAX, 10).await, vec![(4, c(4)), (3, c(3)), (2, c(2)), (1, c(1))]);
    assert_eq!(q("src", u64::MAX, 10).await, vec![(4, c(4)), (2, c(2)), (1, c(1))]);
    assert_eq!(q("src/a.rs", u64::MAX, 10).await, vec![(4, c(4)), (1, c(1))]);
    assert_eq!(q("src/a.rs", 3, 10).await, vec![(1, c(1))]);
    assert_eq!(q("src", u64::MAX, 2).await, vec![(4, c(4)), (2, c(2))]);
    // Prefix is component-wise: "sr" matches nothing, "src/b" only c.rs.
    assert_eq!(q("sr", u64::MAX, 10).await, vec![]);
    assert_eq!(q("src/b", u64::MAX, 10).await, vec![(2, c(2))]);

    // Watermark CAS.
    assert_eq!(s.history_watermark("main").await.unwrap(), None);
    assert!(s.set_history_watermark("main", None, c(4)).await.unwrap());
    assert!(!s.set_history_watermark("main", None, c(5)).await.unwrap());
    assert!(s.set_history_watermark("main", Some(c(4)), c(5)).await.unwrap());
    assert_eq!(s.history_watermark("main").await.unwrap(), Some(c(5)));

    s.clear_history("main").await.unwrap();
    assert_eq!(s.history_watermark("main").await.unwrap(), None);
    assert_eq!(q("", u64::MAX, 10).await, vec![]);
    assert_eq!(s.query_history("other", &RepoPath::root(), u64::MAX, 10).await.unwrap(), vec![(9, c(9))]);
}
