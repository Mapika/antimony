//! Import existing git repositories (SHA-1 or SHA-256) into antimony.
//!
//! TASK (gitbridge): implement `import`.
//!
//! - Open the repo with `gix`. For each `(git_ref, ant_ref)` in
//!   `opts.refs`, collect all commits reachable from the git ref that are
//!   not yet imported, and convert them parents-first (topological order).
//! - Conversion rewrites object ids to SHA-256 git objects
//!   (`ant_core::{Tree, Commit}`) while preserving everything else byte for
//!   byte: tree entry names/modes, commit author/committer/time zones,
//!   extra headers (e.g. `encoding`, `mergetag`, `gpgsig`) and messages. The
//!   result MUST equal what `git fast-export | git fast-import` into a
//!   `--object-format=sha256` repo produces for unsigned commits (the test
//!   checks every commit id).
//! - Memoize SHA-1→SHA-256 for trees and blobs within a run (identical
//!   subtrees are converted once). Persist the commit mapping in
//!   `MetaStore` derived data, kind `"git-import/commit/v1"`, key = source
//!   commit id bytes, value = 32-byte antimony id — this makes re-imports
//!   incremental (already-imported commits are skipped without reading
//!   their trees).
//! - Write objects in batches (`opts.batch_objects` per `put_many`); record
//!   each commit with `ant_store::graph::record_commit` after its objects
//!   are stored (parents first).
//! - Finally point each `ant_ref` at the converted tip via `cas_ref`
//!   (expected = current value; it's an import tool, not concurrent).
//! - gix is synchronous: do git reading inside `spawn_blocking`, streaming
//!   converted objects to the async writer through a bounded channel, so
//!   memory stays bounded on big repos.
//! - Non-UTF-8 tree entry names or commit text → error naming the object.
//! - Return stats counting objects actually written (not skipped).

use std::path::Path;

use ant_store::Backend;

#[derive(Debug, Clone)]
pub struct ImportOptions {
    /// (git ref, antimony ref), e.g. ("refs/heads/main", "main").
    pub refs: Vec<(String, String)>,
    pub batch_objects: usize,
}

impl ImportOptions {
    pub fn single(git_ref: &str, ant_ref: &str) -> Self {
        Self { refs: vec![(git_ref.into(), ant_ref.into())], batch_objects: 1024 }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportStats {
    pub commits: u64,
    pub trees: u64,
    pub blobs: u64,
}

pub async fn import(repo: &Path, backend: &Backend, opts: &ImportOptions) -> anyhow::Result<ImportStats> {
    let _ = (repo, backend, opts);
    todo!("gitbridge")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::process::Command;

    use super::*;
    use ant_core::{ObjectId, RepoPath};
    use ant_store::graph::lookup_path;
    use ant_store::ObjectStoreExt;

    fn git(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_AUTHOR_NAME", "Ada")
            .env("GIT_AUTHOR_EMAIL", "ada@example.com")
            .env("GIT_COMMITTER_NAME", "Bob")
            .env("GIT_COMMITTER_EMAIL", "bob@example.com")
            .env("GIT_AUTHOR_DATE", "1700000000 +0530")
            .env("GIT_COMMITTER_DATE", "1700000100 -0700")
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8(out.stdout).unwrap()
    }

    /// History with nested dirs, exec bit, symlink, rename, delete, a merge.
    fn make_repo(root: &Path) -> PathBuf {
        let r = root.join("src");
        std::fs::create_dir(&r).unwrap();
        git(&r, &["init", "-q", "-b", "main"]);
        std::fs::create_dir_all(r.join("a/b")).unwrap();
        std::fs::write(r.join("a/b/c.txt"), "c\n").unwrap();
        std::fs::write(r.join("run.sh"), "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(r.join("run.sh"), std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
        std::os::unix::fs::symlink("run.sh", r.join("link")).unwrap();
        git(&r, &["add", "-A"]);
        git(&r, &["commit", "-qm", "first\n\nbody line"]);
        git(&r, &["checkout", "-qb", "side"]);
        std::fs::write(r.join("side.txt"), "s\n").unwrap();
        git(&r, &["add", "-A"]);
        git(&r, &["commit", "-qm", "side work"]);
        git(&r, &["checkout", "-q", "main"]);
        git(&r, &["mv", "a/b/c.txt", "a/c.txt"]);
        git(&r, &["rm", "-q", "link"]);
        git(&r, &["commit", "-qm", "move and delete"]);
        git(&r, &["merge", "-q", "--no-edit", "side"]);
        r
    }

    /// git's own SHA-256 conversion as an oracle: commit subject → id.
    fn oracle(root: &Path, src: &Path) -> Vec<(String, String)> {
        let dst = root.join("oracle");
        std::fs::create_dir(&dst).unwrap();
        git(&dst, &["init", "-q", "-b", "main", "--object-format=sha256"]);
        let export = Command::new("git").args(["fast-export", "--all"]).current_dir(src).output().unwrap();
        let mut child = Command::new("git")
            .args(["fast-import", "--quiet"])
            .current_dir(&dst)
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write;
        child.stdin.take().unwrap().write_all(&export.stdout).unwrap();
        assert!(child.wait().unwrap().success());
        git(&dst, &["log", "--format=%H %s", "main"])
            .lines()
            .map(|l| {
                let (h, s) = l.split_once(' ').unwrap();
                (h.to_string(), s.to_string())
            })
            .collect()
    }

    #[tokio::test]
    async fn import_matches_git_sha256_conversion() {
        let tmp = tempfile::tempdir().unwrap();
        let src = make_repo(tmp.path());
        let expected = oracle(tmp.path(), &src);
        assert_eq!(expected.len(), 4);

        let b = Backend::in_memory();
        let stats = import(&src, &b, &ImportOptions::single("refs/heads/main", "main")).await.unwrap();
        assert_eq!(stats.commits, 4);

        let tip = b.refs.get_ref("main").await.unwrap().unwrap();
        assert_eq!(tip.to_hex(), expected[0].0, "tip id must equal git's sha256 conversion");
        for (hex, subject) in &expected {
            let id: ObjectId = hex.parse().unwrap();
            let c = b.objects.read_commit(&id).await.unwrap_or_else(|e| panic!("{subject}: {e}"));
            assert!(c.message.starts_with(subject.as_str()));
            assert!(b.meta.get_commits(&[id]).await.unwrap()[0].is_some(), "{subject} recorded");
        }

        let c = b.objects.read_commit(&tip).await.unwrap();
        assert_eq!(c.author.tz_offset_minutes, 330);
        assert_eq!(c.committer.tz_offset_minutes, -420);
        let f = lookup_path(b.objects.as_ref(), c.tree, &RepoPath::new("a/c.txt").unwrap()).await.unwrap().unwrap();
        assert_eq!(&b.objects.read_blob(&f.1).await.unwrap()[..], b"c\n");
        let node = b.meta.get_commits(&[tip]).await.unwrap().pop().unwrap().unwrap();
        assert_eq!(node.parents.len(), 2);
        assert_eq!(node.generation, 3);
    }

    #[tokio::test]
    async fn reimport_is_incremental() {
        let tmp = tempfile::tempdir().unwrap();
        let src = make_repo(tmp.path());
        let b = Backend::in_memory();
        let opts = ImportOptions::single("refs/heads/main", "main");
        import(&src, &b, &opts).await.unwrap();
        let again = import(&src, &b, &opts).await.unwrap();
        assert_eq!(again, ImportStats::default());

        std::fs::write(src.join("new.txt"), "n\n").unwrap();
        git(&src, &["add", "-A"]);
        git(&src, &["commit", "-qm", "later"]);
        let inc = import(&src, &b, &opts).await.unwrap();
        assert_eq!(inc.commits, 1);
        assert_eq!(inc.blobs, 1);
        let tip = b.refs.get_ref("main").await.unwrap().unwrap();
        assert!(b.objects.read_commit(&tip).await.unwrap().message.starts_with("later"));
    }
}
