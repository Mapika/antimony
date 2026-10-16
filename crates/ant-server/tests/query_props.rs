//! Property tests for the commit-graph queries (`ops::ancestry`: merge
//! base, ancestry, first-parent ancestry, log ranges with pagination)
//! against a brute-force oracle, on random DAGs with merge commits (as
//! imported git history has, including octopus merges and several roots),
//! in memory; plus one larger DAG on Postgres (`--run-ignored all`).

mod common;

use ant_core::{Commit, Object, ObjectId, Signature, Tree};
use ant_server::ops::ancestry;
use ant_store::graph::write_commit;
use ant_store::{Backend, ObjectStoreExt};
use proptest::prelude::*;

/// Node `i`'s parents (indices `< i`), first parent first.
#[derive(Debug, Clone)]
struct Dag(Vec<Vec<usize>>);

fn dag(max: usize) -> impl Strategy<Value = Dag> {
    (1..max).prop_flat_map(|n| {
        let nodes: Vec<_> = (0..n)
            .map(|i| {
                // (kind, first parent back-offset, extra parents)
                (0u8..10, 0usize..4, proptest::collection::vec(0usize..64, 0..3)).prop_map(
                    move |(kind, back, extra)| {
                        if i == 0 || kind == 0 {
                            return vec![]; // a root
                        }
                        let first = i - 1 - back.min(i - 1);
                        let mut ps = vec![first];
                        if kind >= 7 {
                            // A merge (octopus when there are two extras).
                            for e in extra {
                                let p = e % i;
                                if !ps.contains(&p) {
                                    ps.push(p);
                                }
                            }
                        }
                        ps
                    },
                )
            })
            .collect();
        nodes.prop_map(Dag)
    })
}

/// Deterministic DAG for the Postgres run (a small LCG, no extra deps).
fn fixed_dag(n: usize, mut seed: u64) -> Dag {
    let mut next = move |m: usize| {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((seed >> 33) as usize) % m
    };
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if i == 0 || next(40) == 0 {
            out.push(vec![]);
            continue;
        }
        let mut ps = vec![i - 1 - next(3).min(i - 1)];
        if next(4) == 0 {
            let p = next(i);
            if !ps.contains(&p) {
                ps.push(p);
            }
        }
        out.push(ps);
    }
    Dag(out)
}

/// A merge-heavy DAG shaped like rust-lang/rust's history: a first-parent
/// chain where every commit merges a topic branch (bors), based a few
/// commits back or, now and then, far back (long-lived branches); some
/// topics are merges themselves (rollups), some merges are octopus merges,
/// and a few bring in a history with its own root (subtree merges). Long
/// enough that first-parent gaps exceed the walks' first window, so the
/// skip index and the windowed reads are exercised. Returns the DAG and
/// its first-parent chain (indices, oldest first).
fn merge_heavy_dag(merges: usize, mut seed: u64) -> (Dag, Vec<usize>) {
    let mut next = move |m: usize| {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((seed >> 33) as usize) % m.max(1)
    };
    fn add(nodes: &mut Vec<Vec<usize>>, ps: Vec<usize>) -> usize {
        nodes.push(ps);
        nodes.len() - 1
    }
    /// A chain of `len >= 1` commits on `base` (`None`: a new root).
    fn topic(nodes: &mut Vec<Vec<usize>>, base: Option<usize>, len: usize) -> usize {
        let mut tip = base;
        for _ in 0..len {
            tip = Some(add(nodes, tip.into_iter().collect()));
        }
        tip.unwrap_or(0)
    }
    let mut nodes: Vec<Vec<usize>> = vec![vec![]];
    let mut main = vec![0usize];
    for _ in 0..merges {
        let back = if next(16) == 0 { next(main.len()) } else { next(4) };
        let base = main[main.len() - 1 - back.min(main.len() - 1)];
        let len = 1 + next(3);
        let mut tip = topic(&mut nodes, Some(base), len);
        if next(8) == 0 {
            // A rollup: the topic merges another one.
            let len = 1 + next(3);
            let other = topic(&mut nodes, Some(base), len);
            tip = add(&mut nodes, vec![tip, other]);
        }
        let mut ps = vec![main[main.len() - 1], tip];
        if next(12) == 0 {
            let len = 1 + next(2);
            ps.push(topic(&mut nodes, Some(base), len)); // octopus
        }
        if next(40) == 0 {
            let len = 1 + next(3);
            ps.push(topic(&mut nodes, None, len)); // another root
        }
        main.push(add(&mut nodes, ps));
    }
    (Dag(nodes), main)
}

/// Branches off a long first-parent chain, recorded interleaved with it
/// (as agents' work is): branches start far back or recently, off the
/// chain or off other commits, grow, merge the chain in (sync) and get
/// merged into it; a few more start after its tip (new work on an old
/// base). Long enough that the two sides of a query are more than the
/// walks' first window apart, so `merge_base` decides from the first
/// parents where it can. Returns the DAG, its first-parent chain and the
/// branch tips (indices).
fn diverged_dag(steps: usize, late: usize, mut seed: u64) -> (Dag, Vec<usize>, Vec<usize>) {
    let mut next = move |m: usize| {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((seed >> 33) as usize) % m.max(1)
    };
    let mut nodes: Vec<Vec<usize>> = vec![vec![]];
    let mut main = vec![0usize];
    let mut tips: Vec<usize> = Vec::new();
    let add = |nodes: &mut Vec<Vec<usize>>, ps: Vec<usize>| {
        nodes.push(ps);
        nodes.len() - 1
    };
    for _ in 0..steps {
        match next(12) {
            0..=6 => {
                let mut ps = vec![main[main.len() - 1]];
                if !tips.is_empty() && next(4) == 0 {
                    // Merge a branch into the chain.
                    ps.push(tips.swap_remove(next(tips.len())));
                }
                main.push(add(&mut nodes, ps));
            }
            7 => {
                let base = match next(4) {
                    0 => next(nodes.len()),
                    1 => main[main.len() - 1 - next(8).min(main.len() - 1)],
                    _ => main[next(main.len())],
                };
                tips.push(add(&mut nodes, vec![base]));
            }
            8..=10 if !tips.is_empty() => {
                let i = next(tips.len());
                tips[i] = add(&mut nodes, vec![tips[i]]);
            }
            _ if !tips.is_empty() => {
                // Sync: the branch merges a chain commit.
                let i = next(tips.len());
                let m = main[next(main.len())];
                if m != tips[i] {
                    tips[i] = add(&mut nodes, vec![tips[i], m]);
                }
            }
            _ => {}
        }
    }
    for _ in 0..late {
        let base = main[next(main.len())];
        let mut tip = add(&mut nodes, vec![base]);
        for _ in 0..next(3) {
            tip = add(&mut nodes, vec![tip]);
        }
        tips.push(tip);
    }
    (Dag(nodes), main, tips)
}

struct Oracle {
    ids: Vec<ObjectId>,
    generation: Vec<u64>,
    /// `anc[i][j]`: j is an ancestor of i (inclusive).
    anc: Vec<Vec<bool>>,
    parents: Vec<Vec<usize>>,
}

impl Oracle {
    fn new(dag: &Dag, ids: Vec<ObjectId>) -> Self {
        let n = dag.0.len();
        let mut generation = vec![0u64; n];
        let mut anc = vec![vec![false; n]; n];
        for (i, ps) in dag.0.iter().enumerate() {
            generation[i] = 1 + ps.iter().map(|&p| generation[p]).max().unwrap_or(0);
            anc[i][i] = true;
            for &p in ps {
                let from_parent = anc[p].clone();
                for (mine, theirs) in anc[i].iter_mut().zip(from_parent) {
                    *mine |= theirs;
                }
            }
        }
        Self { ids, generation, anc, parents: dag.0.clone() }
    }

    fn merge_base(&self, x: usize, y: usize) -> Option<ObjectId> {
        (0..self.ids.len())
            .filter(|&j| self.anc[x][j] && self.anc[y][j])
            .max_by_key(|&j| (self.generation[j], self.ids[j]))
            .map(|j| self.ids[j])
    }

    fn first_parent_chain(&self, mut i: usize) -> Vec<usize> {
        let mut out = vec![i];
        while let Some(&p) = self.parents[i].first() {
            out.push(p);
            i = p;
        }
        out
    }

    fn range(&self, from: Option<usize>, to: usize) -> Vec<ObjectId> {
        self.first_parent_chain(to)
            .into_iter()
            .take_while(|&c| from.is_none_or(|f| !self.anc[f][c]))
            .map(|c| self.ids[c])
            .collect()
    }
}

/// Store and record the DAG's commits (the empty tree; distinct messages).
async fn build(b: &Backend, dag: &Dag) -> Vec<ObjectId> {
    let tree = b.objects.put(Object::Tree(Tree::empty()).encode()).await.unwrap();
    let mut ids: Vec<ObjectId> = Vec::with_capacity(dag.0.len());
    for (i, ps) in dag.0.iter().enumerate() {
        let sig = Signature {
            name: "p".into(),
            email: "p@x".into(),
            time: 1_700_000_000 + i as i64,
            tz_offset_minutes: 0,
        };
        let c = Commit {
            tree,
            parents: ps.iter().map(|&p| ids[p]).collect(),
            author: sig.clone(),
            committer: sig,
            extra_headers: vec![],
            message: format!("c{i}\n").into_bytes(),
        };
        ids.push(write_commit(b, &c).await.unwrap());
    }
    ids
}

async fn all_pages(
    b: &Backend,
    from: Option<&str>,
    to: &str,
    limit: usize,
) -> Result<Vec<ObjectId>, String> {
    let (mut out, mut next) = (Vec::new(), None);
    loop {
        let page =
            ancestry::log_range(b, from, to, None, limit, next).await.map_err(|e| e.to_string())?;
        if limit > 0 && page.entries.len() > limit {
            return Err(format!("page of {} > limit {limit}", page.entries.len()));
        }
        out.extend(page.entries.iter().map(|e| e.id));
        match page.next {
            Some(n) => next = Some(n),
            None => return Ok(out),
        }
    }
}

/// Check every query on the pairs `pairs` against the oracle.
async fn check(
    b: &Backend,
    o: &Oracle,
    pairs: impl IntoIterator<Item = (usize, usize)>,
    limit: usize,
) -> Result<(), String> {
    for (x, y) in pairs {
        let (hx, hy) = (o.ids[x].to_hex(), o.ids[y].to_hex());
        let mb = ancestry::merge_base(b, &hx, &hy).await.map_err(|e| e.to_string())?;
        if mb != o.merge_base(x, y) {
            return Err(format!("merge_base({x}, {y}) = {mb:?}, want {:?}", o.merge_base(x, y)));
        }
        let anc = ancestry::is_ancestor(b, &hx, &hy, false).await.map_err(|e| e.to_string())?;
        if anc != o.anc[y][x] {
            return Err(format!("is_ancestor({x}, {y}) = {anc}"));
        }
        let fp = ancestry::is_ancestor(b, &hx, &hy, true).await.map_err(|e| e.to_string())?;
        if fp != o.first_parent_chain(y).contains(&x) {
            return Err(format!("is_first_parent_ancestor({x}, {y}) = {fp}"));
        }
        let range = all_pages(b, Some(&hx), &hy, limit).await?;
        if range != o.range(Some(x), y) {
            return Err(format!("log_range({x}..{y}, limit {limit}) = {range:?}"));
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(common::config(48))]

    #[test]
    fn ancestry_queries_match_the_oracle(d in dag(24), limit in 0usize..4) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let result: Result<(), String> = rt.block_on(async {
            let b = Backend::in_memory();
            let ids = build(&b, &d).await;
            let o = Oracle::new(&d, ids);
            let n = d.0.len();
            // Whole histories too.
            for y in 0..n {
                let got = all_pages(&b, None, &o.ids[y].to_hex(), limit).await?;
                if got != o.range(None, y) {
                    return Err(format!("log_range(..{y}) = {got:?}"));
                }
            }
            check(&b, &o, (0..n).flat_map(|x| (0..n).map(move |y| (x, y))), limit).await
        });
        prop_assert!(result.is_ok(), "{:?} on {:?}", result, d);
    }
}

/// Query pairs for a merge-heavy DAG: first-parent commits far apart (both
/// ways), topic commits against first-parent commits (both ways), and
/// random pairs.
fn merge_heavy_pairs(n: usize, main: &[usize], mut seed: u64, count: usize) -> Vec<(usize, usize)> {
    let mut next = move |m: usize| {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((seed >> 33) as usize) % m.max(1)
    };
    let tip = main[main.len() - 1];
    (0..count)
        .map(|k| {
            let on_main = main[next(main.len())];
            let any = next(n);
            match k % 6 {
                0 => (on_main, tip),
                1 => (tip, on_main),
                2 => (any, tip),
                3 => (any, on_main),
                4 => (on_main, any),
                _ => (any, next(n)),
            }
        })
        .collect()
}

proptest! {
    #![proptest_config(common::config(12))]

    /// Merge-heavy histories (every first-parent commit a merge), long
    /// enough for the skip index and windowed walks to come into play.
    #[test]
    fn ancestry_queries_match_the_oracle_on_merge_heavy_histories(
        merges in 100usize..220,
        seed in any::<u64>(),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let result: Result<(), String> = rt.block_on(async {
            let b = Backend::in_memory();
            let (d, main) = merge_heavy_dag(merges, seed);
            let ids = build(&b, &d).await;
            let o = Oracle::new(&d, ids);
            check(&b, &o, merge_heavy_pairs(d.0.len(), &main, seed, 30), 0).await
        });
        prop_assert!(result.is_ok(), "{:?} with {} merges, seed {}", result, merges, seed);
    }
}

/// Query pairs for a diverged DAG: branch tips against the chain's tip
/// (both ways), then branch tips against chain commits (both ways) and
/// against each other, and random pairs.
fn diverged_pairs(
    n: usize,
    main: &[usize],
    tips: &[usize],
    mut seed: u64,
    count: usize,
) -> Vec<(usize, usize)> {
    let mut next = move |m: usize| {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((seed >> 33) as usize) % m.max(1)
    };
    let tip = main[main.len() - 1];
    let mut pairs: Vec<(usize, usize)> = tips.iter().flat_map(|&t| [(t, tip), (tip, t)]).collect();
    for k in 0..count {
        let branch = if tips.is_empty() { next(n) } else { tips[next(tips.len())] };
        let on_main = main[next(main.len())];
        pairs.push(match k % 4 {
            0 => (branch, on_main),
            1 => (on_main, branch),
            2 => (branch, if tips.is_empty() { next(n) } else { tips[next(tips.len())] }),
            _ => (next(n), next(n)),
        });
    }
    pairs
}

proptest! {
    #![proptest_config(common::config(12))]

    /// Branches based far back on a long first-parent chain, recorded
    /// before or after the chain moved on: `merge_base`'s first-parent
    /// shortcut and its fallbacks against the oracle.
    #[test]
    fn ancestry_queries_match_the_oracle_on_diverged_branches(
        steps in 500usize..900,
        late in 0usize..6,
        seed in any::<u64>(),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let result: Result<(), String> = rt.block_on(async {
            let b = Backend::in_memory();
            let (d, main, tips) = diverged_dag(steps, late, seed);
            let ids = build(&b, &d).await;
            let o = Oracle::new(&d, ids);
            check(&b, &o, diverged_pairs(d.0.len(), &main, &tips, seed, 30), 0).await
        });
        prop_assert!(result.is_ok(), "{:?} with {} steps, {} late, seed {}", result, steps, late, seed);
    }
}

#[tokio::test]
#[ignore = "needs postgres: docker compose up -d"]
async fn ancestry_queries_match_the_oracle_on_diverged_branches_postgres() {
    let b = common::pg::scratch_backend("query_props_dv").await;
    let result = tokio::spawn({
        let b = b.clone();
        async move {
            let (d, main, tips) = diverged_dag(1500, 8, 5);
            let ids = build(&b, &d).await;
            let o = Oracle::new(&d, ids);
            let pairs = diverged_pairs(d.0.len(), &main, &tips, 5, 120);
            check(&b, &o, pairs, 0).await
        }
    })
    .await;
    // The scratch store drops the schema with the last handle to it.
    drop(b);
    match result {
        Ok(r) => r.unwrap(),
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    }
}

#[tokio::test]
#[ignore = "needs postgres: docker compose up -d"]
async fn ancestry_queries_match_the_oracle_on_merge_heavy_histories_postgres() {
    let b = common::pg::scratch_backend("query_props_mh").await;
    let result = tokio::spawn({
        let b = b.clone();
        async move {
            let (d, main) = merge_heavy_dag(600, 11);
            let ids = build(&b, &d).await;
            let o = Oracle::new(&d, ids);
            let pairs = merge_heavy_pairs(d.0.len(), &main, 11, 120);
            check(&b, &o, pairs, 0).await
        }
    })
    .await;
    // The scratch store drops the schema with the last handle to it.
    drop(b);
    match result {
        Ok(r) => r.unwrap(),
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    }
}

#[tokio::test]
#[ignore = "needs postgres: docker compose up -d"]
async fn ancestry_queries_match_the_oracle_postgres() {
    let b = common::pg::scratch_backend("query_props").await;
    let result = tokio::spawn({
        let b = b.clone();
        async move {
            let d = fixed_dag(300, 7);
            let ids = build(&b, &d).await;
            let o = Oracle::new(&d, ids);
            let n = d.0.len();
            // A sample of pairs, biased towards distant ones.
            let pairs: Vec<(usize, usize)> =
                (0..n).step_by(7).flat_map(|x| (0..n).step_by(11).map(move |y| (x, y))).collect();
            check(&b, &o, pairs, 5).await
        }
    })
    .await;
    // The scratch store drops the schema with the last handle to it.
    drop(b);
    match result {
        Ok(r) => r.unwrap(),
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    }
}
