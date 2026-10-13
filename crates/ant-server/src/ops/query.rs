//! Read-only queries that let agents and CI work without a checkout.
//!
//! TASK (server-changeset): implement everything marked `todo!()`.
//!
//! - `resolve_rev`: `<ref>` | `<64-hex id>`, optionally suffixed `~N`
//!   (walk N first parents). A bare hex id must be a recorded commit. A
//!   string that is both a valid ref name and 64-hex is treated as an id.
//!   Unknown ref / id → `NotFound`; walking past a root → `NotFound`;
//!   malformed (`main~x`) → `InvalidArgument`.
//! - `read_path`: entry at `path` in the rev's tree. Trees list their
//!   entries; blobs return content only if `include_content`.
//! - `log`: first-parent history from the rev, newest first, at most
//!   `limit` commits (0 = no limit). With `path`, keep only commits whose
//!   recorded changeset has a change at or under `path`
//!   (`RepoPath::starts_with`). Use `MetaStore` changesets — do not diff
//!   trees here.
//! - `diff`: changes between two revs' trees (`from = None` = empty tree),
//!   via `ant_store::graph::diff_trees`.

use ant_core::{Commit, EntryMode, ObjectId, RepoPath, TreeEntry};
use ant_store::{Backend, PathChange};
use bytes::Bytes;

use super::OpResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathInfo {
    pub mode: EntryMode,
    pub id: ObjectId,
    /// Present for trees.
    pub entries: Option<Vec<TreeEntry>>,
    /// Present for blobs when requested.
    pub content: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub id: ObjectId,
    pub generation: u64,
    pub commit: Commit,
}

pub async fn resolve_rev(b: &Backend, rev: &str) -> OpResult<ObjectId> {
    let _ = (b, rev);
    todo!("server-changeset")
}

pub async fn read_path(
    b: &Backend,
    rev: &str,
    path: &RepoPath,
    include_content: bool,
) -> OpResult<Option<PathInfo>> {
    let _ = (b, rev, path, include_content);
    todo!("server-changeset")
}

pub async fn log(
    b: &Backend,
    rev: &str,
    path: Option<&RepoPath>,
    limit: usize,
) -> OpResult<Vec<LogEntry>> {
    let _ = (b, rev, path, limit);
    todo!("server-changeset")
}

pub async fn diff(b: &Backend, from: Option<&str>, to: &str) -> OpResult<Vec<PathChange>> {
    let _ = (b, from, to);
    todo!("server-changeset")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::OpError;
    use crate::ops::changeset::tests::{commit, delete, write};
    use ant_store::{CasOutcome, ChangeKind};

    async fn setup() -> (Backend, Vec<ObjectId>) {
        let b = Backend::in_memory();
        let c1 = commit(&b, None, vec![write("src/a.rs", "a1"), write("README", "r")]).await.unwrap();
        let c2 = commit(&b, Some(c1), vec![write("src/a.rs", "a2")]).await.unwrap();
        let c3 = commit(&b, Some(c2), vec![write("docs/x.md", "x")]).await.unwrap();
        let c4 = commit(&b, Some(c3), vec![delete("README"), write("src/b/c.rs", "c")]).await.unwrap();
        assert_eq!(b.refs.cas_ref("main", None, Some(c4)).await.unwrap(), CasOutcome::Updated);
        (b, vec![c1, c2, c3, c4])
    }

    #[tokio::test]
    async fn resolve() {
        let (b, c) = setup().await;
        assert_eq!(resolve_rev(&b, "main").await.unwrap(), c[3]);
        assert_eq!(resolve_rev(&b, "main~1").await.unwrap(), c[2]);
        assert_eq!(resolve_rev(&b, "main~3").await.unwrap(), c[0]);
        assert_eq!(resolve_rev(&b, &c[1].to_hex()).await.unwrap(), c[1]);
        assert_eq!(resolve_rev(&b, &format!("{}~1", c[1])).await.unwrap(), c[0]);
        assert!(matches!(resolve_rev(&b, "main~4").await, Err(OpError::NotFound(_))));
        assert!(matches!(resolve_rev(&b, "nope").await, Err(OpError::NotFound(_))));
        assert!(matches!(resolve_rev(&b, &"ab".repeat(32)).await, Err(OpError::NotFound(_))));
        assert!(matches!(resolve_rev(&b, "main~x").await, Err(OpError::InvalidArgument(_))));
    }

    #[tokio::test]
    async fn read_paths() {
        let (b, _) = setup().await;
        let root = read_path(&b, "main", &RepoPath::root(), false).await.unwrap().unwrap();
        assert_eq!(root.mode, EntryMode::Tree);
        let names: Vec<_> = root.entries.unwrap().into_iter().map(|e| e.name).collect();
        assert_eq!(names, ["docs", "src"]);

        let f = read_path(&b, "main~3", &RepoPath::new("src/a.rs").unwrap(), true).await.unwrap().unwrap();
        assert_eq!(f.mode, EntryMode::File);
        assert_eq!(f.content.as_deref(), Some(&b"a1"[..]));
        assert!(f.entries.is_none());

        let f = read_path(&b, "main", &RepoPath::new("src/a.rs").unwrap(), false).await.unwrap().unwrap();
        assert!(f.content.is_none());

        assert!(read_path(&b, "main", &RepoPath::new("README").unwrap(), true).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn log_and_path_filter() {
        let (b, c) = setup().await;
        let ids = |v: Vec<LogEntry>| v.into_iter().map(|e| e.id).collect::<Vec<_>>();
        assert_eq!(ids(log(&b, "main", None, 0).await.unwrap()), [c[3], c[2], c[1], c[0]]);
        assert_eq!(ids(log(&b, "main", None, 2).await.unwrap()), [c[3], c[2]]);
        let src = RepoPath::new("src").unwrap();
        assert_eq!(ids(log(&b, "main", Some(&src), 0).await.unwrap()), [c[3], c[1], c[0]]);
        let a = RepoPath::new("src/a.rs").unwrap();
        assert_eq!(ids(log(&b, "main", Some(&a), 0).await.unwrap()), [c[1], c[0]]);
        let e = &log(&b, "main", None, 1).await.unwrap()[0];
        assert_eq!(e.generation, 4);
        assert_eq!(e.commit.parents, vec![c[2]]);
    }

    #[tokio::test]
    async fn diffs() {
        let (b, _) = setup().await;
        let d = diff(&b, Some("main~2"), "main").await.unwrap();
        let s: Vec<_> = d.iter().map(|c| (c.path.as_str(), c.kind)).collect();
        assert_eq!(
            s,
            [("README", ChangeKind::Deleted), ("docs/x.md", ChangeKind::Added), ("src/b/c.rs", ChangeKind::Added)]
        );
        assert_eq!(diff(&b, None, "main~3").await.unwrap().len(), 2);
    }
}
