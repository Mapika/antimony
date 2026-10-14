//! Commit-graph queries for CI, bisect, revert and "did it merge" tooling,
//! so none of them needs a checkout or a log scan on the client.
//!
//! - `merge_base(a, b)`: `ant_store::graph::merge_base` of two revs (the
//!   best common ancestor of the highest generation, ties by larger id).
//! - `is_ancestor(ancestor, descendant, first_parent_only)`: reachability
//!   (`graph::is_ancestor`), or presence on `descendant`'s first-parent
//!   chain (`graph::is_first_parent_ancestor`).
//! - `log_range(from, to, path, limit, page)`: `to`'s first-parent chain,
//!   newest first, down to (excluding) the first commit reachable from
//!   `from` (`graph::first_parent_range_end`; no `from` = to the root).
//!   With a path, only commits whose changeset touches it, as `log` does:
//!   from the history index when the page start lies on an indexed ref's
//!   first-parent chain (the rev's own ref first, see
//!   `history::indexed_ref_for`) and every path under `path` is readable,
//!   else by scanning recorded changesets of the range (chunks of
//!   [`RANGE_CHUNK`]: one chain read and one changeset read each).
//!   Pages: at most `limit` commits (0 = all); `next` = the first-parent
//!   commit to continue from (`None`: done). With a path filter served by
//!   the scan, the last page may be empty.
//! - `landed_as(commit, ref)`: whether `commit` is in a ref's history, as
//!   itself (fast-forward, or already contained) or as the commit a
//!   server-side rebase landed it as (the `land/landed-as/v1` mapping
//!   written by `ops::land`, which maps every commit of a landed stack).
//!   Without a ref, the first [`MAX_LANDED_PROBE`] refs in name order are
//!   tried.
//!
//! Walks read commit nodes in batches (`graph`: the ancestry above a
//! generation floor, or a first-parent segment, per store call; the
//! first-parent skip index for "on the first-parent chain"), never one commit
//! or one merge per round trip. Access control: commit
//! history is not path-restricted (as with `Log`), so `merge_base`,
//! `is_ancestor` and `landed_as` need only an authenticated caller;
//! `log_range` with a path counts only readable changes (`log_range_as`).

use ant_core::{ObjectId, RepoPath};
use ant_store::{Backend, CommitNode, StoreError, graph};

use super::land::LANDED_AS;
use super::query::{LogEntry, resolve_rev};
use super::{OpError, OpResult, cache, history};
use crate::acl::Access;

/// Refs tried by `landed_as` without a ref name.
pub const MAX_LANDED_PROBE: usize = 64;
/// First-parent commits read per round trip by the range scan.
const RANGE_CHUNK: usize = 256;

pub async fn merge_base(b: &Backend, x: &str, y: &str) -> OpResult<Option<ObjectId>> {
    let (x, y) = (resolve_rev(b, x).await?, resolve_rev(b, y).await?);
    Ok(graph::merge_base(b, x, y).await?)
}

pub async fn is_ancestor(
    b: &Backend,
    ancestor: &str,
    descendant: &str,
    first_parent_only: bool,
) -> OpResult<bool> {
    let (a, d) = (resolve_rev(b, ancestor).await?, resolve_rev(b, descendant).await?);
    Ok(if first_parent_only {
        graph::is_first_parent_ancestor(b, a, d).await?
    } else {
        graph::is_ancestor(b, a, d).await?
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogPage {
    pub entries: Vec<LogEntry>,
    /// Where the next page starts (`None`: this was the last page).
    pub next: Option<ObjectId>,
}

pub async fn log_range(
    b: &Backend,
    from: Option<&str>,
    to: &str,
    path: Option<&RepoPath>,
    limit: usize,
    page: Option<ObjectId>,
) -> OpResult<LogPage> {
    log_range_as(b, None, from, to, path, limit, page).await
}

/// [`log_range`] on behalf of `access`: with `path`, only changes at paths
/// the caller can read select a commit (as `query::log_as`).
pub async fn log_range_as(
    b: &Backend,
    access: Option<&Access>,
    from: Option<&str>,
    to: &str,
    path: Option<&RepoPath>,
    limit: usize,
    page: Option<ObjectId>,
) -> OpResult<LogPage> {
    let to_id = resolve_rev(b, to).await?;
    let from_id = match from {
        Some(rev) => Some(resolve_rev(b, rev).await?),
        None => None,
    };
    let start = page.unwrap_or(to_id);
    let start_node = cache::get_node(b, start).await?;
    let end = match from_id {
        Some(f) => graph::first_parent_range_end(b, f, start).await?,
        None => None,
    };
    let limit = if limit == 0 { usize::MAX } else { limit };
    let restricted = match (access, path) {
        (Some(a), Some(p)) => !a.can_read_all_under(p),
        _ => false,
    };
    if let Some(path) = path
        && !restricted
        && let Some(page) = indexed_range(b, to, &start_node, end, path, limit).await?
    {
        return Ok(page);
    }
    scan_range(b, access.filter(|_| restricted), start, end, path, limit).await
}

/// A path-filtered page from the history index, when `start` lies on an
/// indexed ref's first-parent chain (`None` otherwise).
async fn indexed_range(
    b: &Backend,
    rev: &str,
    start: &CommitNode,
    end: Option<ObjectId>,
    path: &RepoPath,
    limit: usize,
) -> OpResult<Option<LogPage>> {
    let Some(ref_name) = history::indexed_ref_for(b, rev, start.id).await? else {
        return Ok(None);
    };
    let floor = match end {
        Some(end) => cache::get_node(b, end).await?.generation,
        None => 0,
    };
    // One extra row tells whether another page exists.
    let mut hits =
        b.meta.query_history(&ref_name, path, start.generation, limit.saturating_add(1)).await?;
    hits.retain(|(generation, _)| *generation > floor);
    let more = hits.len() > limit;
    hits.truncate(limit);
    let entries = history::log_entries(b, hits).await?;
    let next = match entries.last() {
        Some(last) if more => last.commit.parents.first().copied(),
        _ => None,
    };
    Ok(Some(LogPage { entries, next }))
}

/// A page by walking the chain from `start` down to `end` (exclusive),
/// filtering by recorded changesets when there is a `path`; `access` is
/// set when changes at unreadable paths must not count.
async fn scan_range(
    b: &Backend,
    access: Option<&Access>,
    start: ObjectId,
    end: Option<ObjectId>,
    path: Option<&RepoPath>,
    limit: usize,
) -> OpResult<LogPage> {
    let mut picked: Vec<(u64, ObjectId)> = Vec::new();
    let mut next = (Some(start) != end).then_some(start);
    while let Some(from) = next {
        if picked.len() >= limit {
            break;
        }
        let want =
            if path.is_some() { RANGE_CHUNK } else { (limit - picked.len()).min(RANGE_CHUNK) };
        let chain = b.meta.first_parent_chain(from, end, want).await?;
        let Some(last) = chain.last() else {
            return Err(OpError::not_found_commit(from));
        };
        next = last.parents.first().copied().filter(|p| Some(*p) != end);
        let mut nodes: Vec<&CommitNode> = chain.iter().collect();
        if let Some(path) = path {
            let ids: Vec<ObjectId> = chain.iter().map(|n| n.id).collect();
            let sets = cache::get_changesets(b, &ids).await?;
            let mut keep = sets.iter().map(|set| {
                set.as_deref().is_some_and(|changes| {
                    changes.iter().any(|c| {
                        c.path.starts_with(path) && access.is_none_or(|a| a.can_read(&c.path))
                    })
                })
            });
            nodes.retain(|_| keep.next().unwrap_or(false));
        }
        let room = limit - picked.len();
        if nodes.len() > room {
            // The page ends inside this chunk: continue below its last commit.
            next = nodes[room - 1].parents.first().copied().filter(|p| Some(*p) != end);
            nodes.truncate(room);
        }
        picked.extend(nodes.iter().map(|n| (n.generation, n.id)));
    }
    let entries = history::log_entries(b, picked).await?;
    Ok(LogPage { entries, next })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Landed {
    pub ref_name: String,
    pub landed_commit: ObjectId,
    /// Landed as a different (rebased) commit.
    pub rebased: bool,
}

/// Where `commit` landed (see the module docs); `None` if it is in none of
/// the refs looked at. A named ref that does not exist is `NotFound`.
pub async fn landed_as(
    b: &Backend,
    commit: &str,
    ref_name: Option<&str>,
) -> OpResult<Option<Landed>> {
    let id = resolve_rev(b, commit).await?;
    let mapped = b
        .meta
        .get_derived(LANDED_AS, id.as_bytes())
        .await?
        .map(|v| ObjectId::from_bytes(&v))
        .transpose()?;
    let refs = match ref_name {
        Some(name) => {
            ant_store::validate_ref_name(name)?;
            let tip = b
                .refs
                .get_ref(name)
                .await?
                .ok_or_else(|| OpError::NotFound(format!("ref {name}")))?;
            vec![(name.to_string(), tip)]
        }
        None => b.refs.list_refs("").await?.into_iter().take(MAX_LANDED_PROBE).collect(),
    };
    // The rebased commit first: it is what a land of `commit` produced.
    let candidates: Vec<ObjectId> = mapped.into_iter().chain([id]).collect();
    for (name, tip) in refs {
        for &c in &candidates {
            match graph::is_ancestor(b, c, tip).await {
                Ok(true) => {
                    return Ok(Some(Landed { ref_name: name, landed_commit: c, rebased: c != id }));
                }
                Ok(false) => {}
                // A probed ref at an unrecorded commit (or a mapping to one)
                // says nothing about this commit.
                Err(StoreError::InvalidArgument(_)) if ref_name.is_none() || c != id => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(None)
}

/// Page tokens of `LogRange`: `v1.<hex commit id>`.
pub fn encode_page_token(next: ObjectId) -> String {
    format!("v1.{next}")
}

pub fn decode_page_token(token: &str) -> OpResult<Option<ObjectId>> {
    if token.is_empty() {
        return Ok(None);
    }
    token
        .strip_prefix("v1.")
        .and_then(|hex| hex.parse::<ObjectId>().ok())
        .map(Some)
        .ok_or_else(|| OpError::InvalidArgument(format!("bad page token {token:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::land::{self, LandOutcome};
    use crate::ops::query::{self, DiffOptions};
    use crate::ops::test_support::{commit, delete, write};
    use ant_store::CasOutcome;

    /// main: c[0]..c[5] (c[1], c[4] touch src/, c[2], c[3] docs/);
    /// feature: f1, f2 off c[2].
    async fn setup() -> (Backend, Vec<ObjectId>, Vec<ObjectId>) {
        let b = Backend::in_memory();
        let mut c = Vec::new();
        let files = ["README", "src/a.rs", "docs/x", "docs/y", "src/b.rs", "z"];
        for (i, f) in files.iter().enumerate() {
            let id =
                commit(&b, c.last().copied(), vec![write(f, &format!("v{i}\n"))]).await.unwrap();
            c.push(id);
        }
        assert_eq!(b.refs.cas_ref("main", None, Some(c[5])).await.unwrap(), CasOutcome::Updated);
        let f1 = commit(&b, Some(c[2]), vec![write("src/f", "f1\n")]).await.unwrap();
        let f2 = commit(&b, Some(f1), vec![write("docs/f", "f2\n")]).await.unwrap();
        (b, c, vec![f1, f2])
    }

    fn ids(p: &LogPage) -> Vec<ObjectId> {
        p.entries.iter().map(|e| e.id).collect()
    }

    /// Every page of a range; returns the commits and the number of pages.
    async fn all_pages(
        b: &Backend,
        from: Option<&str>,
        to: &str,
        path: Option<&str>,
        limit: usize,
    ) -> (Vec<ObjectId>, usize) {
        let path = path.map(|p| RepoPath::new(p).unwrap());
        let (mut out, mut pages, mut next) = (Vec::new(), 0, None);
        loop {
            let page = log_range(b, from, to, path.as_ref(), limit, next).await.unwrap();
            pages += 1;
            assert!(limit == 0 || page.entries.len() <= limit);
            out.extend(ids(&page));
            match page.next {
                Some(n) => next = Some(n),
                None => return (out, pages),
            }
        }
    }

    #[tokio::test]
    async fn merge_base_and_ancestry() {
        let (b, c, f) = setup().await;
        let fh = f[1].to_hex();
        assert_eq!(merge_base(&b, "main", &fh).await.unwrap(), Some(c[2]));
        assert_eq!(merge_base(&b, "main~1", "main").await.unwrap(), Some(c[4]));
        assert!(is_ancestor(&b, "main~3", "main", false).await.unwrap());
        assert!(is_ancestor(&b, "main~3", "main", true).await.unwrap());
        assert!(!is_ancestor(&b, &fh, "main", false).await.unwrap());
        assert!(is_ancestor(&b, &c[1].to_hex(), &fh, true).await.unwrap());
        assert!(matches!(merge_base(&b, "nope", "main").await, Err(OpError::NotFound(_))));
        assert!(matches!(is_ancestor(&b, "main", "nope", false).await, Err(OpError::NotFound(_))));
        let other = commit(&b, None, vec![write("q", "q\n")]).await.unwrap();
        assert_eq!(merge_base(&b, &other.to_hex(), "main").await.unwrap(), None);
    }

    #[tokio::test]
    async fn ranges_pages_and_paths() {
        let (b, c, f) = setup().await;
        let fh = f[1].to_hex();
        let whole = |v: &[usize]| v.iter().map(|&i| c[i]).collect::<Vec<_>>();
        assert_eq!(all_pages(&b, Some("main~3"), "main", None, 0).await.0, whole(&[5, 4, 3]));
        assert_eq!(all_pages(&b, Some(&fh), "main", None, 0).await.0, whole(&[5, 4, 3]));
        assert_eq!(all_pages(&b, Some("main"), &fh, None, 0).await.0, vec![f[1], f[0]]);
        assert_eq!(all_pages(&b, None, "main~4", None, 0).await.0, whole(&[1, 0]));
        assert!(all_pages(&b, Some("main"), "main~2", None, 0).await.0.is_empty());
        // Pages of 2: [5, 4], [3, 2], [1, 0].
        let (got, pages) = all_pages(&b, None, "main", None, 2).await;
        assert_eq!((got, pages), (whole(&[5, 4, 3, 2, 1, 0]), 3));
        let first = log_range(&b, Some("main~3"), "main", None, 2, None).await.unwrap();
        assert_eq!((ids(&first), first.next), (whole(&[5, 4]), Some(c[3])));
        let second = log_range(&b, Some("main~3"), "main", None, 2, first.next).await.unwrap();
        assert_eq!((ids(&second), second.next), (whole(&[3]), None));

        // Paths: the scan and the history index agree.
        for indexed in [false, true] {
            if indexed {
                history::catch_up(&b, "main").await.unwrap();
            }
            let src = all_pages(&b, Some("main~5"), "main", Some("src"), 0).await.0;
            assert_eq!(src, whole(&[4, 1]), "indexed: {indexed}");
            let src1 = all_pages(&b, Some("main~5"), "main", Some("src"), 1).await.0;
            assert_eq!(src1, whole(&[4, 1]), "indexed: {indexed}");
            let docs = all_pages(&b, Some(&fh), "main", Some("docs"), 1).await.0;
            assert_eq!(docs, whole(&[3]), "indexed: {indexed}");
            let none = all_pages(&b, Some("main~1"), "main", Some("src"), 0).await.0;
            assert!(none.is_empty(), "indexed: {indexed}");
        }
        assert!(matches!(
            log_range(&b, Some("nope"), "main", None, 0, None).await,
            Err(OpError::NotFound(_))
        ));
        assert!(matches!(decode_page_token("v1.zz"), Err(OpError::InvalidArgument(_))));
        assert_eq!(decode_page_token(&encode_page_token(c[2])).unwrap(), Some(c[2]));
        assert_eq!(decode_page_token("").unwrap(), None);
    }

    #[tokio::test]
    async fn landed_as_follows_rebases_and_fast_forwards() {
        let (b, c, f) = setup().await;
        // f2 sits on f1: landing f2 replays the stack onto main.
        let LandOutcome::Landed { new_tip, rebased: true } =
            land::land(&b, "main", f[1]).await.unwrap()
        else {
            panic!("expected a rebase");
        };
        let l = landed_as(&b, &f[1].to_hex(), Some("main")).await.unwrap().unwrap();
        assert_eq!(l, Landed { ref_name: "main".into(), landed_commit: new_tip, rebased: true });
        // Every commit of the stack is mapped.
        let l1 = landed_as(&b, &f[0].to_hex(), None).await.unwrap().unwrap();
        assert!(l1.rebased && l1.landed_commit != f[0]);
        assert!(is_ancestor(&b, &l1.landed_commit.to_hex(), "main", true).await.unwrap());
        // Fast-forward and plain ancestry.
        let ff = commit(&b, Some(new_tip), vec![delete("z")]).await.unwrap();
        assert!(landed_as(&b, &ff.to_hex(), None).await.unwrap().is_none());
        land::land(&b, "main", ff).await.unwrap();
        let l = landed_as(&b, &ff.to_hex(), None).await.unwrap().unwrap();
        assert_eq!(l, Landed { ref_name: "main".into(), landed_commit: ff, rebased: false });
        assert!(!landed_as(&b, &c[0].to_hex(), None).await.unwrap().unwrap().rebased);
        // Another ref, and refs that do not have it.
        b.refs.cas_ref("release", None, Some(c[1])).await.unwrap();
        assert!(landed_as(&b, &ff.to_hex(), Some("release")).await.unwrap().is_none());
        assert_eq!(
            landed_as(&b, &c[1].to_hex(), Some("release")).await.unwrap().unwrap().ref_name,
            "release"
        );
        assert!(matches!(
            landed_as(&b, &ff.to_hex(), Some("missing")).await,
            Err(OpError::NotFound(_))
        ));
        assert!(matches!(landed_as(&b, "nope", None).await, Err(OpError::NotFound(_))));
    }

    #[tokio::test]
    async fn diffs_with_prefixes_merge_base_and_patches() {
        let (b, _c, f) = setup().await;
        let fh = f[1].to_hex();
        let paths = |r: &query::DiffResult| {
            r.changes.iter().map(|c| c.path.as_str().to_string()).collect::<Vec<_>>()
        };
        let opts = |prefixes: &[&str], merge_base: bool, patch: bool| DiffOptions {
            prefixes: prefixes.iter().map(|p| RepoPath::new(*p).unwrap()).collect(),
            merge_base,
            patch: patch.then_some(3),
        };
        // main vs feature: the plain diff also shows main's later changes.
        let r =
            query::diff_with(&b, None, Some("main"), &fh, &opts(&[], false, false)).await.unwrap();
        assert_eq!(paths(&r), ["docs/f", "docs/y", "src/b.rs", "src/f", "z"]);
        let r =
            query::diff_with(&b, None, Some("main"), &fh, &opts(&[], true, false)).await.unwrap();
        assert_eq!(paths(&r), ["docs/f", "src/f"]);
        assert_eq!(r.from_commit, Some(merge_base(&b, "main", &fh).await.unwrap().unwrap()));
        // Prefixes: subtrees only; a nested prefix adds nothing; files work.
        for p in [&["src"][..], &["src", "src/f"], &["src/f", "src/b.rs"]] {
            let r = query::diff_with(&b, None, Some("main"), &fh, &opts(p, false, false))
                .await
                .unwrap();
            assert_eq!(paths(&r), ["src/b.rs", "src/f"], "{p:?}");
        }
        let r = query::diff_with(&b, None, Some("main"), &fh, &opts(&["nope", "z"], false, false))
            .await
            .unwrap();
        assert_eq!(paths(&r), ["z"]);
        let r =
            query::diff_with(&b, None, None, "main", &opts(&["docs"], false, false)).await.unwrap();
        assert_eq!(paths(&r), ["docs/x", "docs/y"]);
        // Patches.
        let r = query::diff_with(&b, None, Some("main~1"), "main", &opts(&[], false, true))
            .await
            .unwrap();
        let p = r.patches.unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].as_ref().unwrap().hunks[0].lines, ["+v5"]);
        // A merge-base diff needs a from rev; unrelated histories have none.
        assert!(matches!(
            query::diff_with(&b, None, None, "main", &opts(&[], true, false)).await,
            Err(OpError::InvalidArgument(_))
        ));
        let other = commit(&b, None, vec![write("q", "q\n")]).await.unwrap();
        assert!(matches!(
            query::diff_with(&b, None, Some(&other.to_hex()), "main", &opts(&[], true, false))
                .await,
            Err(OpError::NotFound(_))
        ));
    }
}
