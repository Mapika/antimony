//! A clone of a repository imported from commits with unusual-but-valid raw
//! bytes (odd names, zones, headers, messages) has exactly the objects of
//! git's SHA-256 conversion, byte for byte, and fsck-level reads work.

#[path = "../../ant-gitbridge/tests/support/commit_corpus.rs"]
mod corpus;

use ant_gitbridge::{ImportOptions, import};
use ant_store::Backend;
use corpus::{ENV, all_cases, build, run, text};

#[tokio::test]
async fn clone_of_unusual_commits_has_git_sha256_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let (src, oracle, clone) =
        (tmp.path().join("src"), tmp.path().join("oracle"), tmp.path().join("clone"));
    for d in [&src, &oracle] {
        std::fs::create_dir(d).unwrap();
    }
    build(&src, "sha1");
    let oracle_ids = build(&oracle, "sha256");

    let b = Backend::in_memory();
    import(&src, &b, &ImportOptions::single("refs/heads/main", "main")).await.unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/repo.git", listener.local_addr().unwrap());
    let app = ant_gitserve::router(b.clone());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let out = tokio::process::Command::new("git")
        .args(["clone", "-q", &url])
        .arg(&clone)
        .envs(ENV.iter().copied())
        .output()
        .await
        .unwrap();
    task.abort();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));

    assert_eq!(text(&clone, &["rev-parse", "HEAD"]), *oracle_ids.last().unwrap());
    for (case, id) in all_cases().iter().zip(&oracle_ids) {
        assert_eq!(
            run(&clone, &["cat-file", "commit", id], b""),
            run(&oracle, &["cat-file", "commit", id], b""),
            "{}: exported commit bytes",
            case.name
        );
    }
}
