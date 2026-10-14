//! Commits with unusual-but-valid raw bytes must import to exactly the ids
//! git's SHA-256 conversion gives (commit text is copied verbatim except
//! object ids), with the stored text byte-identical to the oracle's.

#[path = "support/commit_corpus.rs"]
mod corpus;

use ant_core::ObjectId;
use ant_gitbridge::{ImportOptions, import};
use ant_store::{Backend, ObjectStoreExt};

#[tokio::test]
async fn unusual_commit_bytes_import_as_git_sha256_does() {
    let tmp = tempfile::tempdir().unwrap();
    let (src, oracle) = (tmp.path().join("src"), tmp.path().join("oracle"));
    for d in [&src, &oracle] {
        std::fs::create_dir(d).unwrap();
    }
    corpus::build(&src, "sha1");
    let oracle_ids = corpus::build(&oracle, "sha256");
    let oracle_tree = corpus::text(&oracle, &["rev-parse", "main^{tree}"]);
    let names: Vec<_> = corpus::all_cases().iter().map(|c| c.name).collect();

    let b = Backend::in_memory();
    import(&src, &b, &ImportOptions::single("refs/heads/main", "main")).await.unwrap();

    let mut mismatches = Vec::new();
    for (name, oid) in names.iter().zip(&oracle_ids) {
        let id: ObjectId = oid.parse().unwrap();
        let want = corpus::run(&oracle, &["cat-file", "commit", oid], b"");
        match b.objects.get(&id).await.unwrap() {
            Some(o) => {
                assert_eq!(o.body(), want.as_slice(), "{name}: bytes");
                // Stored commits stay readable whatever their signature lines hold.
                let c = b.objects.read_commit(&id).await.unwrap_or_else(|e| panic!("{name}: {e}"));
                assert_eq!(c.tree.to_hex(), oracle_tree, "{name}: tree");
            }
            None => mismatches.push(*name),
        }
    }
    assert!(mismatches.is_empty(), "not imported under git's SHA-256 id: {mismatches:?}");
    let tip = b.refs.get_ref("main").await.unwrap().unwrap();
    assert_eq!(&tip.to_hex(), oracle_ids.last().unwrap(), "tip id");
}
