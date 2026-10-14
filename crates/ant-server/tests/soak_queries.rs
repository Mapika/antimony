//! The soak's read paths on a soak-shaped repository: agent directories
//! sharded as `agents/<shard>/<agent>/` and a long first-parent history in
//! which every commit touches one agent directory.
//!
//! - `status` = `Diff(base, tip)` with the base many lands back: one batched
//!   object read per tree depth on a cold cache (not one per directory),
//!   none on a warm one, and the same changes the landed changesets imply.
//! - `log --path` on a ref whose history index trails the tip (the normal
//!   state under constant landing), and on a rev resolved before a catch-up
//!   overtook it: answered from the index (plus the short unindexed lag),
//!   equal to the full changeset scan.
//!
//! The Postgres variant runs with `--run-ignored all` and prints timings.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ant_core::{EncodedObject, EntryMode, ObjectId, RepoPath};
use ant_server::ops::changeset::{ChangesetRequest, create_changeset};
use ant_server::ops::history;
use ant_server::ops::land::{LandOutcome, land};
use ant_server::ops::query;
use ant_store::{Backend, ChangeKind, ObjectStore};
use async_trait::async_trait;
use common::*;

const SHARDS: usize = 32;
const AGENTS: usize = 512;
/// Depth of `agents/<shard>/<agent>/<file>`: root, `agents`, shard, agent.
const TREE_LEVELS: usize = 4;

/// Forwards to another object store, counting `get_many` calls.
struct CountingObjects {
    inner: Arc<dyn ObjectStore>,
    reads: AtomicUsize,
}

#[async_trait]
impl ObjectStore for CountingObjects {
    async fn get_many(&self, ids: &[ObjectId]) -> ant_store::Result<Vec<Option<EncodedObject>>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get_many(ids).await
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> ant_store::Result<()> {
        self.inner.put_many(objects).await
    }

    async fn has_many(&self, ids: &[ObjectId]) -> ant_store::Result<Vec<bool>> {
        self.inner.has_many(ids).await
    }
}

fn agent_dir(i: usize) -> String {
    format!("agents/{:02}/{i}", i % SHARDS)
}

/// Seed every agent directory, then land `lands` commits, each rewriting
/// one file of agent `(n * 7) % AGENTS`; the history index is caught up
/// `lag` lands before the end. Returns the tips, oldest first (`tips[0]` =
/// the seed) and the path each land wrote.
async fn soak_history(b: &Backend, lands: usize, lag: usize) -> (Vec<ObjectId>, Vec<String>) {
    let mut seed = Model::new();
    for i in 0..AGENTS {
        seed.insert(format!("{}/f0.txt", agent_dir(i)), (EntryMode::File, b"seed\n".to_vec()));
    }
    let c0 = base_commit(b, store_model(b, &seed).await).await;
    assert!(matches!(land(b, "main", c0).await.unwrap(), LandOutcome::Landed { .. }));
    let mut tips = vec![c0];
    let mut paths = Vec::new();
    for n in 0..lands {
        let path = format!("{}/f0.txt", agent_dir((n * 7) % AGENTS));
        let req = ChangesetRequest {
            base: tips.last().copied(),
            ops: vec![write(&path, format!("land {n}\n"))],
            author: sig(),
            committer: None,
            message: format!("land {n}\n"),
        };
        let commit = create_changeset(b, req).await.unwrap();
        let LandOutcome::Landed { new_tip, .. } = land(b, "main", commit).await.unwrap() else {
            panic!("land {n} did not land");
        };
        tips.push(new_tip);
        paths.push(path);
        if n + 1 + lag == lands {
            history::catch_up(b, "main").await.unwrap();
        }
    }
    (tips, paths)
}

/// A backend whose object reads are counted, with caches of its own (they
/// are scoped per store instance): every tree read starts cold.
fn counted(b: &Backend) -> (Backend, Arc<CountingObjects>) {
    let objects =
        Arc::new(CountingObjects { inner: b.objects.clone(), reads: AtomicUsize::new(0) });
    (Backend { objects: objects.clone(), ..b.clone() }, objects)
}

async fn check_status_diffs(b: &Backend, tips: &[ObjectId], paths: &[String]) {
    let tip = *tips.last().unwrap();
    for back in [1, 10, 100, paths.len()] {
        let base = tips[tips.len() - 1 - back];
        let (cold, counter) = counted(b);
        let t = Instant::now();
        let changes = query::diff(&cold, Some(&base.to_hex()), &tip.to_hex()).await.unwrap();
        let elapsed = t.elapsed();
        let reads = counter.reads.load(Ordering::Relaxed);
        // What the landed changesets imply: every touched file, modified.
        let want: BTreeSet<&str> = paths[paths.len() - back..].iter().map(String::as_str).collect();
        let got: BTreeSet<&str> = changes.iter().map(|c| c.path.as_str()).collect();
        assert_eq!(got, want, "diff {back} lands back");
        assert!(changes.iter().all(|c| c.kind == ChangeKind::Modified));
        assert!(
            reads <= TREE_LEVELS,
            "diff {back} lands back ({} directories): {reads} object reads, want one per depth",
            want.len()
        );
        let t = Instant::now();
        let again = query::diff(&cold, Some(&base.to_hex()), &tip.to_hex()).await.unwrap();
        let warm = t.elapsed();
        assert_eq!(again, changes);
        assert_eq!(counter.reads.load(Ordering::Relaxed), reads, "warm diff reads no trees");
        eprintln!(
            "status diff {back:>4} lands back: {:>4} changes, {reads} reads, cold {elapsed:?}, \
             warm {warm:?}",
            changes.len()
        );
    }
}

async fn check_path_logs(b: &Backend, tips: &[ObjectId], lag: usize) {
    let tip = *tips.last().unwrap();
    let wm = tips[tips.len() - 1 - lag];
    assert_eq!(b.meta.history_watermark("main").await.unwrap(), Some(wm));
    let ids = |v: Vec<query::LogEntry>| v.into_iter().map(|e| e.id).collect::<Vec<_>>();
    let dirs: Vec<RepoPath> = [0, 3, 7, 100, 333, AGENTS - 1]
        .iter()
        .map(|i| RepoPath::new(agent_dir(*i)).unwrap())
        .collect();
    // A rev resolved before a catch-up overtakes it (half the lag back).
    let old = tips[tips.len() - 1 - lag / 2];
    let mut old_scans = Vec::new();
    let mut times = Vec::new();
    for dir in &dirs {
        // A commit-id rev past the watermark is answered by the full scan.
        let scan = ids(query::log(b, &tip.to_hex(), Some(dir), 20).await.unwrap());
        let t = Instant::now();
        let got = history::lagging_path_log(b, "main", tip, dir, 20).await.unwrap();
        times.push(t.elapsed());
        assert_eq!(got.map(ids), Some(scan.clone()), "{dir}: lagging index");
        assert_eq!(ids(query::log(b, "main", Some(dir), 20).await.unwrap()), scan, "{dir}");
        old_scans.push(ids(query::log(b, &old.to_hex(), Some(dir), 20).await.unwrap()));
    }
    times.sort();
    eprintln!(
        "path log, index {lag} commits behind: p50 {:?}, max {:?}",
        times[times.len() / 2],
        times.last().unwrap_or(&Duration::ZERO)
    );

    // The catch-up overtakes `old`: still served by the index, not a scan
    // (that fallback walked the whole history on a busy ref).
    history::catch_up(b, "main").await.unwrap();
    assert_eq!(b.meta.history_watermark("main").await.unwrap(), Some(tip));
    for (dir, scan) in dirs.iter().zip(old_scans) {
        let got = history::lagging_path_log(b, "main", old, dir, 20).await.unwrap();
        assert_eq!(got.map(ids), Some(scan), "{dir}: rev behind the watermark");
    }

    // Repeated narrow queries (a persistent prepared statement would switch
    // to a generic plan after a few runs) stay index range scans.
    let mut times = Vec::new();
    for n in 0..40 {
        let dir = RepoPath::new(agent_dir(n * 13 % AGENTS)).unwrap();
        let t = Instant::now();
        let got = history::lagging_path_log(b, "main", tip, &dir, 20).await.unwrap();
        times.push(t.elapsed());
        assert!(got.is_some_and(|log| !log.is_empty()), "{dir}");
    }
    times.sort();
    eprintln!(
        "path log, index caught up, 40 runs: p50 {:?}, max {:?}",
        times[times.len() / 2],
        times.last().unwrap_or(&Duration::ZERO)
    );
}

#[tokio::test]
async fn memory_status_diff_and_lagging_path_log() {
    let b = Backend::in_memory();
    let (tips, paths) = soak_history(&b, 600, 100).await;
    check_status_diffs(&b, &tips, &paths).await;
    check_path_logs(&b, &tips, 100).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs postgres: docker compose up -d"]
async fn pg_status_diff_and_lagging_path_log() {
    let b = common::pg::scratch_backend("t_soak_queries").await;
    let t = Instant::now();
    let (tips, paths) = soak_history(&b, 1500, 200).await;
    eprintln!("setup: {} lands in {:?}", paths.len(), t.elapsed());
    check_status_diffs(&b, &tips, &paths).await;
    check_path_logs(&b, &tips, 200).await;
}
