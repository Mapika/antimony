//! Path history and blame, backed by the per-ref history index
//! (`MetaStore::{index_history, query_history, ...}`).
//!
//! TASK (history): implement everything marked `todo!()`, then:
//! - call `catch_up(b, ref)` after every successful `land` and `UpdateRef`
//!   (best effort: log and continue on error; queries fall back, see `log`);
//! - make `ops::query::log` with a path use `path_log` when the rev is on an
//!   indexed ref's first-parent chain (rev resolved from a ref name, or a
//!   commit id that `is_first_parent_ancestor` of that ref's watermark), else
//!   keep the existing changeset scan;
//! - add a `Blame` RPC + `ant blame <rev>:<path>` (output per line:
//!   `<commit-id> <line-no>\t<line>`), additive proto change only.
//!
//! `catch_up(b, ref_name)`:
//! - Let `tip` = ref target, `wm` = watermark. If `wm` is None → index the
//!   whole first-parent chain of `tip`. If `wm` is a first-parent ancestor of
//!   `tip` → index only commits after it. Otherwise (ref moved non-FF) →
//!   `clear_history` and rebuild from scratch.
//! - Rows: for each commit on the chain, one row per path in its recorded
//!   changeset (`MetaStore::get_changeset`), with the commit's generation.
//! - Batches of ≤ 1000 commits; write rows then CAS the watermark forward
//!   (a concurrent catch-up winning the CAS is fine — rows are idempotent;
//!   loop until the watermark is `tip` or the ref moves again).
//! - Must handle long histories without recursion and without loading all
//!   commits at once (walk first parents in batches via `get_commits`).
//!
//! `path_log(b, ref_name, rev_generation, path, limit)`: `query_history` →
//! `LogEntry`s (read the commit objects in one `get_many`).
//!
//! `blame(b, rev, path)`: per-line attribution of the file at `rev`.
//! Walk the file's history newest→oldest (use the index when available, else
//! first-parent changesets); for each change, line-diff the previous version
//! against the newer one (`diffy` or an LCS of your own) and attribute lines
//! introduced by that commit; lines surviving to the file's creation belong
//! to the creating commit. Cache results as derived data
//! `"blame/v1"`, key = rev id ++ path bytes, value = compact encoding.
//! Binary (non-UTF-8) files → `InvalidArgument`. Missing path → `NotFound`.

use ant_core::{ObjectId, RepoPath};
use ant_store::Backend;

use super::OpResult;
use super::query::LogEntry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlameLine {
    pub commit: ObjectId,
    pub line: String,
}

/// Bring `ref_name`'s history index up to date. Returns commits indexed.
pub async fn catch_up(b: &Backend, ref_name: &str) -> OpResult<usize> {
    let _ = (b, ref_name);
    todo!("history")
}

pub async fn path_log(
    b: &Backend,
    ref_name: &str,
    max_generation: u64,
    path: &RepoPath,
    limit: usize,
) -> OpResult<Vec<LogEntry>> {
    let _ = (b, ref_name, max_generation, path, limit);
    todo!("history")
}

pub async fn blame(b: &Backend, rev: &str, path: &RepoPath) -> OpResult<Vec<BlameLine>> {
    let _ = (b, rev, path);
    todo!("history")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::changeset::tests::{commit, delete, write};
    use crate::ops::land::{LandOutcome, land};
    use crate::ops::query;

    async fn landed(b: &Backend, base: ObjectId, ops: Vec<crate::ops::changeset::FileOp>) -> ObjectId {
        let c = commit(b, Some(base), ops).await.unwrap();
        let LandOutcome::Landed { new_tip, .. } = land(b, "main", c).await.unwrap() else { panic!() };
        new_tip
    }

    #[tokio::test]
    #[ignore = "TASK(history): remove this ignore when implementing"]
    async fn index_matches_changeset_scan() {
        let b = Backend::in_memory();
        let c0 = commit(&b, None, vec![write("src/a.rs", "1"), write("README", "r")]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let mut tip = c0;
        for i in 0..30 {
            let p = if i % 3 == 0 { "src/a.rs".to_string() } else { format!("docs/{i}.md") };
            tip = landed(&b, tip, vec![write(&p, &i.to_string())]).await;
        }
        assert!(catch_up(&b, "main").await.unwrap() > 0);
        assert_eq!(catch_up(&b, "main").await.unwrap(), 0, "second catch-up is a no-op");
        for path in ["src", "src/a.rs", "docs", "README", "nope"] {
            let p = RepoPath::new(path).unwrap();
            let gen_ = b.meta.get_commits(&[tip]).await.unwrap()[0].clone().unwrap().generation;
            let fast: Vec<_> = path_log(&b, "main", gen_, &p, 0).await.unwrap().into_iter().map(|e| e.id).collect();
            let slow: Vec<_> = query::log(&b, "main", Some(&p), 0).await.unwrap().into_iter().map(|e| e.id).collect();
            assert_eq!(fast, slow, "{path}");
        }
    }

    #[tokio::test]
    #[ignore = "TASK(history): remove this ignore when implementing"]
    async fn non_fast_forward_rebuilds() {
        let b = Backend::in_memory();
        let c0 = commit(&b, None, vec![write("f", "0")]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let c1 = landed(&b, c0, vec![write("f", "1")]).await;
        catch_up(&b, "main").await.unwrap();
        // Force main back to c0 and onto a different branch.
        let other = commit(&b, Some(c0), vec![write("g", "x")]).await.unwrap();
        b.refs.cas_ref("main", Some(c1), Some(other)).await.unwrap();
        catch_up(&b, "main").await.unwrap();
        let gen_ = b.meta.get_commits(&[other]).await.unwrap()[0].clone().unwrap().generation;
        let f = RepoPath::new("f").unwrap();
        let ids: Vec<_> = path_log(&b, "main", gen_, &f, 0).await.unwrap().into_iter().map(|e| e.id).collect();
        assert_eq!(ids, vec![c0], "c1 is no longer on main");
    }

    #[tokio::test]
    #[ignore = "TASK(history): remove this ignore when implementing"]
    async fn blame_attributes_lines() {
        let b = Backend::in_memory();
        let c0 = commit(&b, None, vec![write("f", "a\nb\nc\n")]).await.unwrap();
        land(&b, "main", c0).await.unwrap();
        let c1 = landed(&b, c0, vec![write("f", "a\nB\nc\nd\n")]).await;
        let c2 = landed(&b, c1, vec![write("g", "other\n")]).await;
        let c3 = landed(&b, c2, vec![write("f", "z\na\nB\nc\nd\n")]).await;
        catch_up(&b, "main").await.unwrap();
        let bl = blame(&b, "main", &RepoPath::new("f").unwrap()).await.unwrap();
        let got: Vec<_> = bl.iter().map(|l| (l.commit, l.line.as_str())).collect();
        assert_eq!(got, vec![(c3, "z"), (c0, "a"), (c1, "B"), (c0, "c"), (c1, "d")]);
        // Cached second call agrees.
        assert_eq!(blame(&b, "main", &RepoPath::new("f").unwrap()).await.unwrap(), bl);
        assert!(matches!(
            blame(&b, "main", &RepoPath::new("nope").unwrap()).await,
            Err(crate::ops::OpError::NotFound(_))
        ));
        let _ = delete; // keep helper import used across variants
    }
}
