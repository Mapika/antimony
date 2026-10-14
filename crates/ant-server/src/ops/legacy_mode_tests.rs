//! Trees imported from old histories carry legacy mode spellings (`100664`,
//! zero-padded `040000`, ...). Every operation must treat them as their
//! semantic class, keep untouched entries byte-identical, and write canonical
//! modes only for the entries it rewrites (as a rewriting `git` does).

use ant_core::{Commit, EncodedObject, EntryMode, ObjectId, ObjectKind, RepoPath, TreeEntry};
use ant_store::graph::{diff_trees, lookup_path, write_commit, write_tree};
use ant_store::{Backend, ObjectStoreExt};

use super::land::{LandOutcome, land};
use super::query::{diff, read_path};
use super::test_support::{commit, delete, read, sig, write};

fn legacy(mode: &str) -> EntryMode {
    let m = EntryMode::from_octal(mode.as_bytes()).unwrap();
    assert!(m.is_legacy());
    m
}

async fn blob(b: &Backend, content: &str) -> ObjectId {
    b.objects.put(EncodedObject::new(ObjectKind::Blob, content.as_bytes())).await.unwrap()
}

/// `a` (100664), `x` (100775), `keep` (100640) and a zero-padded directory
/// `d` (040000) holding `f` (100664). Returns (commit, root tree).
async fn legacy_base(b: &Backend) -> (ObjectId, ObjectId) {
    let f = blob(b, "in dir\n").await;
    let d = write_tree(
        b.objects.as_ref(),
        vec![TreeEntry { mode: legacy("100664"), name: "f".into(), id: f }],
    )
    .await
    .unwrap();
    let entries = vec![
        TreeEntry { mode: legacy("100664"), name: "a".into(), id: blob(b, "1\n2\n3\n").await },
        TreeEntry { mode: legacy("100775"), name: "x".into(), id: blob(b, "run\n").await },
        TreeEntry { mode: legacy("100640"), name: "keep".into(), id: blob(b, "keep\n").await },
        TreeEntry { mode: legacy("040000"), name: "d".into(), id: d },
    ];
    let tree = write_tree(b.objects.as_ref(), entries).await.unwrap();
    let c = write_commit(
        b,
        &Commit {
            tree,
            parents: vec![],
            author: sig(),
            committer: sig(),
            extra_headers: vec![],
            message: b"legacy\n".to_vec(),
        },
    )
    .await
    .unwrap();
    b.refs.cas_ref("main", None, Some(c)).await.unwrap();
    (c, tree)
}

fn spellings(tree: &ant_core::Tree) -> Vec<(String, String)> {
    tree.entries().iter().map(|e| (e.name.clone(), e.mode.as_octal().into_owned())).collect()
}

fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
    v.iter().map(|(a, b)| (a.to_string(), b.to_string())).collect()
}

#[tokio::test]
async fn queries_report_semantic_classes() {
    let b = Backend::in_memory();
    let (c, _) = legacy_base(&b).await;
    let root = read_path(&b, "main", &RepoPath::new("").unwrap(), false).await.unwrap().unwrap();
    assert_eq!(root.mode, EntryMode::Tree);
    let a = read_path(&b, "main", &RepoPath::new("a").unwrap(), true).await.unwrap().unwrap();
    assert_eq!(a.mode, EntryMode::File);
    assert_eq!(a.content.as_deref(), Some(&b"1\n2\n3\n"[..]));
    let x = read_path(&b, "main", &RepoPath::new("x").unwrap(), false).await.unwrap().unwrap();
    assert_eq!(x.mode, EntryMode::Executable);
    // A zero-padded directory is a directory: it lists and resolves children.
    let d = read_path(&b, "main", &RepoPath::new("d").unwrap(), false).await.unwrap().unwrap();
    assert_eq!(d.mode, EntryMode::Tree);
    assert_eq!(d.entries.unwrap().len(), 1);
    assert_eq!(read(&b, c, "d/f").await.as_deref(), Some("in dir\n"));
    // Diffing against the empty tree sees plain additions with canonical classes.
    let changes = diff(&b, None, "main").await.unwrap();
    let got: Vec<_> = changes.iter().map(|c| (c.path.as_str(), c.new.unwrap().0)).collect();
    assert_eq!(
        got,
        [
            ("a", EntryMode::File),
            ("d/f", EntryMode::File),
            ("keep", EntryMode::File),
            ("x", EntryMode::Executable)
        ]
    );
}

#[tokio::test]
async fn rewriting_normalizes_only_what_changed() {
    let b = Backend::in_memory();
    let (c1, t1) = legacy_base(&b).await;
    let c2 =
        commit(&b, Some(c1), vec![write("a", "changed\n"), write("d/f", "new\n")]).await.unwrap();
    let t2 = b.objects.read_commit(&c2).await.unwrap().tree;
    let tree = b.objects.read_tree(&t2).await.unwrap();
    assert_eq!(
        spellings(&tree),
        pairs(&[
            ("a", "100644"),    // rewritten: canonical
            ("d", "40000"),     // rewritten directory
            ("keep", "100640"), // untouched: byte-identical
            ("x", "100775"),
        ])
    );
    let (_, d) =
        lookup_path(b.objects.as_ref(), t2, &RepoPath::new("d").unwrap()).await.unwrap().unwrap();
    assert_eq!(spellings(&b.objects.read_tree(&d).await.unwrap()), pairs(&[("f", "100644")]));

    // Only the content changed, not the semantic mode.
    let changes = diff_trees(b.objects.as_ref(), Some(t1), Some(t2)).await.unwrap();
    let got: Vec<_> = changes.iter().map(|c| c.path.as_str()).collect();
    assert_eq!(got, ["a", "d/f"]);
    assert!(changes.iter().all(|c| c.old.unwrap().0 == EntryMode::File));
    assert!(changes.iter().all(|c| c.new.unwrap().0 == EntryMode::File));

    // Deleting an unrelated sibling leaves the other legacy entries as they were.
    let c3 = commit(&b, Some(c1), vec![delete("x")]).await.unwrap();
    let t3 = b.objects.read_commit(&c3).await.unwrap().tree;
    assert_eq!(
        spellings(&b.objects.read_tree(&t3).await.unwrap()),
        pairs(&[("a", "100664"), ("d", "040000"), ("keep", "100640")])
    );
}

#[tokio::test]
async fn landing_merges_text_on_legacy_mode_files() {
    let b = Backend::in_memory();
    let (c1, _) = legacy_base(&b).await;
    let edit_first = commit(&b, Some(c1), vec![write("a", "one\n2\n3\n")]).await.unwrap();
    let edit_last = commit(&b, Some(c1), vec![write("a", "1\n2\nthree\n")]).await.unwrap();
    let LandOutcome::Landed { new_tip, .. } = land(&b, "main", edit_first).await.unwrap() else {
        panic!("first land must succeed");
    };
    assert_eq!(new_tip, edit_first);
    let LandOutcome::Landed { new_tip, rebased } = land(&b, "main", edit_last).await.unwrap()
    else {
        panic!("second land must merge");
    };
    assert!(rebased);
    assert_eq!(read(&b, new_tip, "a").await.as_deref(), Some("one\n2\nthree\n"));
    let tree = b.objects.read_commit(&new_tip).await.unwrap().tree;
    let entries = b.objects.read_tree(&tree).await.unwrap();
    assert_eq!(entries.get("a").unwrap().mode, EntryMode::File);
    assert_eq!(entries.get("keep").unwrap().mode.as_octal(), "100640");
}

#[tokio::test]
async fn landing_unrelated_changes_keeps_untouched_legacy_entries() {
    let b = Backend::in_memory();
    let (c1, t1) = legacy_base(&b).await;
    let theirs = commit(&b, Some(c1), vec![write("new", "n\n")]).await.unwrap();
    let ours = commit(&b, Some(c1), vec![write("other", "o\n")]).await.unwrap();
    land(&b, "main", ours).await.unwrap();
    let LandOutcome::Landed { new_tip, .. } = land(&b, "main", theirs).await.unwrap() else {
        panic!("must land");
    };
    let tree = b.objects.read_commit(&new_tip).await.unwrap().tree;
    let t = b.objects.read_tree(&tree).await.unwrap();
    let base = b.objects.read_tree(&t1).await.unwrap();
    for name in ["a", "x", "keep", "d"] {
        assert_eq!(t.get(name), base.get(name), "{name} untouched");
    }
}
