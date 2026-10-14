//! Read-only queries that let agents and CI work without a checkout.
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
//!   (`RepoPath::starts_with`), from recorded `MetaStore` changesets (or the
//!   history index, see `history`) — no tree diffs.
//! - `diff`: changes between two revs' trees (`from = None` = empty tree),
//!   via `ant_store::graph::diff_trees_with` over the tree cache (one
//!   batched read of the cache misses per depth).

use ant_core::{Commit, EntryMode, ObjectId, RepoPath, TreeEntry};
use ant_store::{Backend, CommitNode, ObjectStoreExt, PathChange, StoreError, graph};
use bytes::Bytes;

use super::{OpError, OpResult, cache, history};
use crate::acl::Access;

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
    let (base, steps) = match rev.rsplit_once('~') {
        Some((base, n)) => {
            let n: usize =
                n.parse().map_err(|_| OpError::InvalidArgument(format!("bad rev {rev:?}")))?;
            (base, n)
        }
        None => (rev, 0),
    };
    let mut id = match base.parse::<ObjectId>() {
        Ok(id) => {
            cache::get_node(b, id).await?;
            id
        }
        Err(_) => b
            .refs
            .get_ref(base)
            .await
            .map_err(|e| match e {
                StoreError::InvalidArgument(m) => OpError::InvalidArgument(m),
                e => e.into(),
            })?
            .ok_or_else(|| OpError::NotFound(format!("ref {base}")))?,
    };
    for _ in 0..steps {
        id = *cache::get_node(b, id)
            .await?
            .parents
            .first()
            .ok_or_else(|| OpError::NotFound(format!("{rev}: history is shorter")))?;
    }
    Ok(id)
}

async fn rev_tree(b: &Backend, rev: &str) -> OpResult<ObjectId> {
    let id = resolve_rev(b, rev).await?;
    Ok(cache::get_node(b, id).await?.tree)
}

pub async fn read_path(
    b: &Backend,
    rev: &str,
    path: &RepoPath,
    include_content: bool,
) -> OpResult<Option<PathInfo>> {
    let tree = rev_tree(b, rev).await?;
    let Some((mode, id)) = graph::lookup_path(b.objects.as_ref(), tree, path).await? else {
        return Ok(None);
    };
    let entries = if mode.is_tree() {
        Some(b.objects.read_tree(&id).await?.entries().to_vec())
    } else {
        None
    };
    let content = if include_content && mode.is_blob() {
        Some(b.objects.read_blob(&id).await?)
    } else {
        None
    };
    Ok(Some(PathInfo { mode, id, entries, content }))
}

/// [`read_path`] on behalf of `access`: a path the caller cannot read is
/// reported exactly like a missing one. Tree listings still show restricted
/// entry names and ids (v1: content, not structure, is protected).
pub async fn read_path_as(
    b: &Backend,
    access: Option<&Access>,
    rev: &str,
    path: &RepoPath,
    include_content: bool,
) -> OpResult<Option<PathInfo>> {
    if let Some(access) = access
        && !access.can_read(path)
    {
        // Resolve anyway so a bad rev still errors the same way.
        rev_tree(b, rev).await?;
        return Ok(None);
    }
    read_path(b, rev, path, include_content).await
}

pub async fn log(
    b: &Backend,
    rev: &str,
    path: Option<&RepoPath>,
    limit: usize,
) -> OpResult<Vec<LogEntry>> {
    log_as(b, None, rev, path, limit).await
}

/// [`log`] on behalf of `access`: with `path`, only changes at paths the
/// caller can read select a commit (so `--path secrets` reveals nothing).
pub async fn log_as(
    b: &Backend,
    access: Option<&Access>,
    rev: &str,
    path: Option<&RepoPath>,
    limit: usize,
) -> OpResult<Vec<LogEntry>> {
    let tip = resolve_rev(b, rev).await?;
    // The index cannot filter by readability; use it only when everything
    // under `path` is readable.
    let restricted = match (access, path) {
        (Some(a), Some(p)) => !a.can_read_all_under(p),
        _ => false,
    };
    if let Some(path) = path
        && !restricted
        && let Some(indexed) = indexed_path_log(b, rev, tip, path, limit).await
    {
        return Ok(indexed);
    }
    let limit = if limit == 0 { usize::MAX } else { limit };
    let mut out = Vec::new();
    let mut next = Some(tip);
    // Chunked walk: one chain read, one changeset read (with a path) and one
    // commit read per chunk instead of round trips per commit.
    while let Some(from) = next {
        if out.len() >= limit {
            break;
        }
        let want = if path.is_some() { LOG_SCAN_CHUNK } else { limit - out.len() };
        let chain = b.meta.first_parent_chain(from, None, want.min(LOG_SCAN_CHUNK)).await?;
        let Some(last) = chain.last() else {
            return Err(OpError::not_found_commit(from));
        };
        next = last.parents.first().copied();
        let mut picked: Vec<&CommitNode> = match path {
            None => chain.iter().collect(),
            Some(path) => {
                let ids: Vec<ObjectId> = chain.iter().map(|n| n.id).collect();
                let sets = b.meta.get_changesets(&ids).await?;
                chain
                    .iter()
                    .zip(sets)
                    .filter(|(_, changes)| {
                        changes.as_deref().unwrap_or_default().iter().any(|c| {
                            c.path.starts_with(path)
                                && (!restricted || access.is_none_or(|a| a.can_read(&c.path)))
                        })
                    })
                    .map(|(n, _)| n)
                    .collect()
            }
        };
        picked.truncate(limit - out.len());
        let ids: Vec<ObjectId> = picked.iter().map(|n| n.id).collect();
        // Through the commit cache: recent commits are read by every log.
        for (n, commit) in picked.into_iter().zip(cache::read_commits(b, &ids).await?) {
            out.push(LogEntry { id: n.id, generation: n.generation, commit: (*commit).clone() });
        }
    }
    Ok(out)
}

/// First-parent commits read per round trip by the log scan.
const LOG_SCAN_CHUNK: usize = 256;

/// Serve `log --path` from the history index when `tip` is on an indexed
/// ref's first-parent chain (for a ref name, also when the index trails it
/// by a short lag). Any failure falls back to the changeset scan.
async fn indexed_path_log(
    b: &Backend,
    rev: &str,
    tip: ObjectId,
    path: &RepoPath,
    limit: usize,
) -> Option<Vec<LogEntry>> {
    if let Some(log) = history::lagging_path_log(b, rev, tip, path, limit).await.ok()? {
        return Some(log);
    }
    let ref_name = history::indexed_ref_for(b, rev, tip).await.ok()??;
    let generation = cache::get_node(b, tip).await.ok()?.generation;
    history::path_log(b, &ref_name, generation, path, limit).await.ok()
}

pub async fn diff(b: &Backend, from: Option<&str>, to: &str) -> OpResult<Vec<PathChange>> {
    diff_as(b, None, from, to).await
}

/// [`diff`] on behalf of `access`: changes at unreadable paths are dropped.
pub async fn diff_as(
    b: &Backend,
    access: Option<&Access>,
    from: Option<&str>,
    to: &str,
) -> OpResult<Vec<PathChange>> {
    let mut changes = diff_unfiltered(b, from, to).await?;
    if let Some(access) = access
        && !access.can_read_everything()
    {
        changes.retain(|c| access.can_read(&c.path));
    }
    Ok(changes)
}

async fn diff_unfiltered(b: &Backend, from: Option<&str>, to: &str) -> OpResult<Vec<PathChange>> {
    let old = match from {
        Some(rev) => Some(rev_tree(b, rev).await?),
        None => None,
    };
    let new = rev_tree(b, to).await?;
    // Batched per depth and through the tree cache: a status diff spanning
    // hundreds of lands touches hundreds of directories.
    Ok(graph::diff_trees_with(&cache::CachedTrees(b), old, Some(new)).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::OpError;
    use crate::ops::test_support::{commit, delete, write};
    use ant_store::{CasOutcome, ChangeKind};

    async fn setup() -> (Backend, Vec<ObjectId>) {
        let b = Backend::in_memory();
        let c1 =
            commit(&b, None, vec![write("src/a.rs", "a1"), write("README", "r")]).await.unwrap();
        let c2 = commit(&b, Some(c1), vec![write("src/a.rs", "a2")]).await.unwrap();
        let c3 = commit(&b, Some(c2), vec![write("docs/x.md", "x")]).await.unwrap();
        let c4 =
            commit(&b, Some(c3), vec![delete("README"), write("src/b/c.rs", "c")]).await.unwrap();
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

        let f = read_path(&b, "main~3", &RepoPath::new("src/a.rs").unwrap(), true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(f.mode, EntryMode::File);
        assert_eq!(f.content.as_deref(), Some(&b"a1"[..]));
        assert!(f.entries.is_none());

        let f = read_path(&b, "main", &RepoPath::new("src/a.rs").unwrap(), false)
            .await
            .unwrap()
            .unwrap();
        assert!(f.content.is_none());

        assert!(
            read_path(&b, "main", &RepoPath::new("README").unwrap(), true).await.unwrap().is_none()
        );
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
            [
                ("README", ChangeKind::Deleted),
                ("docs/x.md", ChangeKind::Added),
                ("src/b/c.rs", ChangeKind::Added)
            ]
        );
        assert_eq!(diff(&b, None, "main~3").await.unwrap().len(), 2);
    }
}
