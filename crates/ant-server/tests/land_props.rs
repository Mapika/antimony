//! Property tests for `ops::land::{land, rebase}`.
//!
//! * disjoint changes from one base always both land, tree = base + both
//! * overlapping changes never lose work (conflict, exact, or clean merge),
//!   with a line-level oracle for non-overlapping line edits
//! * landing an already-landed commit / an ancestor is a no-op
//! * concurrent lands: linear history, one commit per `Landed`, no lost change
//!
//! The concurrency property also runs against Postgres (`--include-ignored`).

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use ant_core::{EntryMode, ObjectId, RepoPath, Signature};
use ant_server::ops::OpError;
use ant_server::ops::changeset::{ChangesetRequest, FileOp, WriteContent, create_changeset};
use ant_server::ops::land::{LandOutcome, land};
use ant_store::graph::is_ancestor;
use ant_store::postgres::PgStore;
use ant_store::{Backend, ObjectStoreExt};
use bytes::Bytes;
use common::*;
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn author() -> Signature {
    Signature { name: "dev".into(), email: "d@x".into(), time: 1_700_000_123, tz_offset_minutes: -270 }
}

/// Commit that turns model `from` into model `to` on top of `parent`.
/// `None` when the change is not expressible in one changeset (dir -> file).
async fn mk_commit(
    b: &Backend,
    parent: ObjectId,
    from: &Model,
    to: &Model,
    msg: &str,
) -> Option<ObjectId> {
    let mut ops = Vec::new();
    let keys: BTreeSet<&String> = from.keys().chain(to.keys()).collect();
    for k in keys {
        let path = RepoPath::new(k.clone()).unwrap();
        match (from.get(k), to.get(k)) {
            (a, c) if a == c => {}
            (_, Some((mode, c))) => ops.push(FileOp::Write {
                path,
                mode: *mode,
                content: WriteContent::Inline(Bytes::from(c.clone())),
            }),
            (Some(_), None) => ops.push(FileOp::Delete { path }),
            (None, None) => unreachable!(),
        }
    }
    let req = ChangesetRequest {
        base: Some(parent),
        ops,
        author: author(),
        committer: None,
        message: msg.into(),
    };
    match create_changeset(b, req).await {
        Ok(id) => Some(id),
        Err(OpError::InvalidArgument(_)) => None,
        Err(e) => panic!("create_changeset failed: {e}"),
    }
}

/// Create base commit for `model` and land it as `ref_name`.
async fn init_ref(b: &Backend, ref_name: &str, model: &Model) -> ObjectId {
    let tree = store_model(b, model).await;
    let c0 = base_commit(b, tree).await;
    let out = land(b, ref_name, c0).await.unwrap();
    assert_eq!(out, LandOutcome::Landed { new_tip: c0, rebased: false });
    c0
}

async fn tip(b: &Backend, ref_name: &str) -> ObjectId {
    b.refs.get_ref(ref_name).await.unwrap().unwrap()
}

async fn tree_of(b: &Backend, commit: ObjectId) -> ObjectId {
    b.objects.read_commit(&commit).await.unwrap().tree
}

async fn final_contents(b: &Backend, commit: ObjectId) -> Model {
    contents(b, tree_of(b, commit).await).await
}

/// Paths whose entry differs between two models.
fn changed(from: &Model, to: &Model) -> BTreeSet<String> {
    from.keys().chain(to.keys()).filter(|k| from.get(*k) != to.get(*k)).cloned().collect()
}

fn related(a: &BTreeSet<String>, b: &BTreeSet<String>) -> bool {
    a.iter().any(|x| b.iter().any(|y| x == y || is_prefix(x, y) || is_prefix(y, x)))
}

/// Replace/delete entries of `base` at the paths in `paths` with those from `to`.
fn overlay(mut base: Model, paths: &BTreeSet<String>, to: &Model) -> Model {
    for p in paths {
        match to.get(p) {
            Some(v) => {
                base.insert(p.clone(), v.clone());
            }
            None => {
                base.remove(p);
            }
        }
    }
    base
}

fn lines(data: &[u8]) -> Vec<&str> {
    std::str::from_utf8(data).map(|s| s.lines().collect()).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

fn component() -> impl Strategy<Value = &'static str> {
    prop_oneof![4 => Just("a"), 4 => Just("b"), 2 => Just("c")]
}

fn path() -> impl Strategy<Value = String> {
    prop::collection::vec(component(), 1..=3).prop_map(|v| v.join("/"))
}

fn blob_mode() -> impl Strategy<Value = EntryMode> {
    prop_oneof![
        8 => Just(EntryMode::File),
        1 => Just(EntryMode::Executable),
        1 => Just(EntryMode::Symlink)
    ]
}

/// Text made of base lines plus lines unique to `side`.
fn text(side: char) -> impl Strategy<Value = Vec<u8>> {
    let pool: Vec<String> =
        ["b0", "b1", "b2", "b3"].iter().map(|s| s.to_string()).chain((0..2).map(|i| format!("{side}{i}"))).collect();
    prop::collection::vec(prop::sample::select(pool), 0..6).prop_map(|ls| {
        let mut s = ls.join("\n");
        if !s.is_empty() {
            s.push('\n');
        }
        s.into_bytes()
    })
}

fn base_model() -> impl Strategy<Value = Model> {
    prop::collection::vec((path(), blob_mode(), text('b')), 0..7).prop_map(|items| {
        let mut m = Model::new();
        for (p, mode, c) in items {
            if !conflicts(&m, &p) {
                m.insert(p, (mode, c));
            }
        }
        m
    })
}

type Mutation = (String, Option<(EntryMode, Vec<u8>)>);

fn mutations(side: char) -> impl Strategy<Value = Vec<Mutation>> {
    let one = (path(), prop::option::weighted(0.7, (blob_mode(), text(side))));
    prop::collection::vec(one, 0..4)
}

fn apply_mutations(base: &Model, muts: &[Mutation]) -> Model {
    let mut m = base.clone();
    for (p, v) in muts {
        match v {
            None => {
                m.remove(p);
            }
            Some(v) => {
                m.retain(|k, _| !(is_prefix(k, p) || is_prefix(p, k)));
                m.insert(p.clone(), v.clone());
            }
        }
    }
    m
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
}

// ---------------------------------------------------------------------------
// Disjoint changes always land; overlapping ones never lose work
// ---------------------------------------------------------------------------

async fn check_pair(base: Model, mx: Vec<Mutation>, my: Vec<Mutation>) -> Result<(), TestCaseError> {
    let b = Backend::in_memory();
    let c0 = init_ref(&b, "main", &base).await;
    let xm = apply_mutations(&base, &mx);
    let ym = apply_mutations(&base, &my);
    let (Some(x), Some(y)) = (
        mk_commit(&b, c0, &base, &xm, "x\n").await,
        mk_commit(&b, c0, &base, &ym, "y\n").await,
    ) else {
        return Err(TestCaseError::reject("not expressible as one changeset"));
    };
    let cx = changed(&base, &xm);
    let cy = changed(&base, &ym);

    let out = land(&b, "main", x).await.unwrap();
    prop_assert_eq!(out, LandOutcome::Landed { new_tip: x, rebased: false });
    let out = land(&b, "main", y).await.unwrap();

    match out {
        LandOutcome::Conflict { tip: t, paths } => {
            prop_assert!(
                related(&cx, &cy),
                "disjoint changes must not conflict: x={:?} y={:?} paths={:?}",
                cx,
                cy,
                paths
            );
            prop_assert_eq!(t, x);
            prop_assert_eq!(tip(&b, "main").await, x, "ref moved on conflict");
            prop_assert!(!paths.is_empty());
            let mut sorted = paths.clone();
            sorted.sort();
            prop_assert_eq!(&paths, &sorted);
            for p in &paths {
                prop_assert!(cy.contains(p.as_str()), "conflict path {} not changed by y", p);
            }
        }
        LandOutcome::Landed { new_tip, rebased } => {
            prop_assert_eq!(tip(&b, "main").await, new_tip);
            prop_assert!(rebased, "x already landed, y must be rebased");
            let rebased_commit = b.objects.read_commit(&new_tip).await.unwrap();
            prop_assert_eq!(&rebased_commit.parents, &vec![x]);
            let orig = b.objects.read_commit(&y).await.unwrap();
            prop_assert_eq!(&rebased_commit.author, &orig.author);
            prop_assert_eq!(&rebased_commit.committer, &orig.committer);
            prop_assert_eq!(&rebased_commit.message, &orig.message);

            let flat = flatten(&b, rebased_commit.tree).await;
            prop_assert!(flat.empty_dirs.is_empty(), "empty dirs {:?}", flat.empty_dirs);
            let fin = final_contents(&b, new_tip).await;

            if !related(&cx, &cy) {
                // Exactly base + both changes; and it is the canonical tree.
                let want = overlay(overlay(base.clone(), &cx, &xm), &cy, &ym);
                prop_assert_eq!(&fin, &want);
                prop_assert_eq!(rebased_commit.tree, store_model(&b, &want).await);
            }
            for p in &cy {
                let (yv, fv) = (ym.get(p), fin.get(p));
                if fv == yv {
                    continue;
                }
                // Otherwise: a clean 3-way merge containing both sides' edits.
                let (Some(bv), Some(xv), Some(yv), Some(fv)) = (base.get(p), xm.get(p), yv, fv)
                else {
                    return Err(TestCaseError::fail(format!(
                        "{p}: y's change lost (base={:?} x={:?} y={:?} final={:?})",
                        base.get(p),
                        xm.get(p),
                        ym.get(p),
                        fin.get(p)
                    )));
                };
                prop_assert!(fv.0 == xv.0 || fv.0 == yv.0, "{}: mode {:?} from nowhere", p, fv.0);
                let fl = lines(&fv.1);
                for l in lines(&yv.1).into_iter().filter(|l| l.starts_with('y')) {
                    prop_assert!(fl.contains(&l), "{}: y line {} lost: {:?}", p, l, fv);
                }
                for l in lines(&xv.1).into_iter().filter(|l| l.starts_with('x')) {
                    prop_assert!(fl.contains(&l), "{}: x line {} lost: {:?}", p, l, fv);
                }
                let allowed: BTreeSet<&str> = [bv, xv, yv].iter().flat_map(|v| lines(&v.1)).collect();
                for l in fl {
                    prop_assert!(allowed.contains(l), "{}: line {} invented", p, l);
                }
            }
            // x's own work survives where y did not touch it; the rest is base.
            for p in &cx {
                if !cy.contains(p) {
                    prop_assert_eq!(fin.get(p), xm.get(p), "x's change at {} lost", p);
                }
            }
            for p in fin.keys().chain(base.keys()) {
                if !cx.contains(p) && !cy.contains(p) {
                    prop_assert_eq!(fin.get(p), base.get(p), "{} changed by nobody", p);
                }
            }
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(config(96))]

    #[test]
    fn overlapping_changes_never_lose_work(
        base in base_model(), mx in mutations('x'), my in mutations('y')
    ) {
        let rt = runtime();
        rt.block_on(check_pair(base, mx, my))?;
    }
}

/// Disjoint-by-construction: x only touches top-level names `x*`, y only `y*`
/// (possibly in the same shared directory), with the shared file set below.
fn disjoint_pools() -> impl Strategy<Value = (Model, Vec<Mutation>, Vec<Mutation>)> {
    let pool_x = ["d/a", "d/e/a", "f/a", "f/g/a", "xs"];
    let pool_y = ["d/b", "d/e/b", "f/b", "f/g/b", "ys"];
    let item = |pool: &'static [&'static str], side: char| {
        (prop::sample::select(pool.to_vec()), prop::option::weighted(0.6, (blob_mode(), text(side))))
            .prop_map(|(p, v)| (p.to_string(), v))
    };
    let all: Vec<&'static str> = pool_x.iter().chain(pool_y.iter()).copied().collect();
    (
        prop::collection::vec((prop::sample::select(all), text('b')), 0..9),
        prop::collection::vec(item(&["d/a", "d/e/a", "f/a", "f/g/a", "xs"], 'x'), 0..5),
        prop::collection::vec(item(&["d/b", "d/e/b", "f/b", "f/g/b", "ys"], 'y'), 0..5),
    )
        .prop_map(|(files, mx, my)| {
            let base: Model = files.into_iter().map(|(p, c)| (p.to_string(), (EntryMode::File, c))).collect();
            (base, mx, my)
        })
}

proptest! {
    #![proptest_config(config(96))]

    #[test]
    fn disjoint_changes_always_land((base, mx, my) in disjoint_pools()) {
        let rt = runtime();
        let rt_res = rt.block_on(async {
            let b = Backend::in_memory();
            let c0 = init_ref(&b, "main", &base).await;
            let xm = apply_mutations(&base, &mx);
            let ym = apply_mutations(&base, &my);
            let x = mk_commit(&b, c0, &base, &xm, "x\n").await.unwrap();
            let y = mk_commit(&b, c0, &base, &ym, "y\n").await.unwrap();
            let want = overlay(overlay(base.clone(), &changed(&base, &xm), &xm), &changed(&base, &ym), &ym);

            // Either landing order gives the same tree.
            let mut trees = Vec::new();
            for (first, second) in [(x, y), (y, x)] {
                let name = format!("r{first}");
                let name = &name[..12];
                // fresh ref at c0
                prop_assert_eq!(b.refs.cas_ref(name, None, Some(c0)).await.unwrap(), ant_store::CasOutcome::Updated);
                for c in [first, second] {
                    let out = land(&b, name, c).await.unwrap();
                    prop_assert!(matches!(out, LandOutcome::Landed { .. }), "{:?}", out);
                }
                let t = tip(&b, name).await;
                let fin = final_contents(&b, t).await;
                prop_assert_eq!(&fin, &want);
                trees.push(tree_of(&b, t).await);
            }
            prop_assert_eq!(trees[0], trees[1]);
            Ok(())
        });
        rt_res?;
    }
}

// ---------------------------------------------------------------------------
// Line-level oracle
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(config(128))]

    /// Edits to lines at least two apart (one untouched line between) merge
    /// cleanly and contain both sides' edits.
    #[test]
    fn non_overlapping_line_edits_merge(
        n in 5usize..14,
        assign in prop::collection::vec(0u8..3, 14),
    ) {
        let assign = &assign[..n];
        // 1 = x edits, 2 = y edits; drop y edits adjacent to an x edit.
        let mut a: Vec<u8> = assign.to_vec();
        for i in 0..n {
            if a[i] == 2 && ((i > 0 && a[i - 1] == 1) || (i + 1 < n && a[i + 1] == 1)) {
                a[i] = 0;
            }
        }
        let base_lines: Vec<String> = (0..n).map(|i| format!("L{i}")).collect();
        let edit = |who: u8, tag: &str| -> String {
            (0..n)
                .map(|i| if a[i] == who { format!("{tag}{i}") } else { base_lines[i].clone() })
                .map(|l| l + "\n")
                .collect()
        };
        let base_text: String = base_lines.iter().map(|l| format!("{l}\n")).collect();
        let (xt, yt) = (edit(1, "X"), edit(2, "Y"));
        let want: String = (0..n)
            .map(|i| match a[i] { 1 => format!("X{i}\n"), 2 => format!("Y{i}\n"), _ => format!("L{i}\n") })
            .collect();
        let base: Model = [("f".to_string(), (EntryMode::File, base_text.into_bytes()))].into();
        let rt = runtime();
        rt.block_on(async {
            let b = Backend::in_memory();
            let c0 = init_ref(&b, "main", &base).await;
            let xm: Model = [("f".to_string(), (EntryMode::File, xt.into_bytes()))].into();
            let ym: Model = [("f".to_string(), (EntryMode::File, yt.into_bytes()))].into();
            let x = mk_commit(&b, c0, &base, &xm, "x\n").await.unwrap();
            let y = mk_commit(&b, c0, &base, &ym, "y\n").await.unwrap();
            land(&b, "main", x).await.unwrap();
            let out = land(&b, "main", y).await.unwrap();
            let LandOutcome::Landed { new_tip, .. } = out else {
                return Err(TestCaseError::fail(format!("clean merge reported as {out:?}")));
            };
            let fin = final_contents(&b, new_tip).await;
            prop_assert_eq!(fin.get("f").map(|v| String::from_utf8_lossy(&v.1).to_string()), Some(want));
            Ok(())
        })?;
    }
}

// ---------------------------------------------------------------------------
// Idempotence
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(config(64))]

    #[test]
    fn landing_tip_or_ancestors_is_a_noop(
        base in base_model(), m1 in mutations('x'), m2 in mutations('y'), m3 in mutations('x')
    ) {
        let rt = runtime();
        rt.block_on(async {
            let b = Backend::in_memory();
            let c0 = init_ref(&b, "main", &base).await;
            let t1 = apply_mutations(&base, &m1);
            let t2 = apply_mutations(&t1, &m2);
            let t3 = apply_mutations(&t2, &m3);
            let (Some(c1), ) = (mk_commit(&b, c0, &base, &t1, "1\n").await, ) else { return Err(TestCaseError::reject("x")) };
            let Some(c2) = mk_commit(&b, c1, &t1, &t2, "2\n").await else { return Err(TestCaseError::reject("x")) };
            let Some(c3) = mk_commit(&b, c2, &t2, &t3, "3\n").await else { return Err(TestCaseError::reject("x")) };
            // Fast-forward over the whole stack.
            let out = land(&b, "main", c3).await.unwrap();
            prop_assert_eq!(out, LandOutcome::Landed { new_tip: c3, rebased: false });
            for c in [c3, c2, c1, c0, c3, c3] {
                let out = land(&b, "main", c).await.unwrap();
                prop_assert_eq!(out, LandOutcome::Landed { new_tip: c3, rebased: false });
                prop_assert_eq!(tip(&b, "main").await, c3);
            }
            Ok(())
        })?;
    }

    /// Re-landing the *original* of a commit that was landed via rebase
    /// (a client retrying after a lost response) must not change the tree.
    #[test]
    fn relanding_rebased_original_keeps_tree(
        base in base_model(), mx in mutations('x'), my in mutations('y')
    ) {
        let rt = runtime();
        rt.block_on(async {
            let b = Backend::in_memory();
            let c0 = init_ref(&b, "main", &base).await;
            let xm = apply_mutations(&base, &mx);
            let ym = apply_mutations(&base, &my);
            let (Some(x), Some(y)) = (
                mk_commit(&b, c0, &base, &xm, "x\n").await,
                mk_commit(&b, c0, &base, &ym, "y\n").await,
            ) else { return Err(TestCaseError::reject("x")) };
            land(&b, "main", x).await.unwrap();
            let LandOutcome::Landed { new_tip, rebased: true } = land(&b, "main", y).await.unwrap()
            else { return Err(TestCaseError::reject("conflict")) };
            let tree_before = tree_of(&b, new_tip).await;
            let out = land(&b, "main", y).await.unwrap();
            // A spurious Conflict is covered by `relanding_original_never_conflicts`.
            let LandOutcome::Landed { new_tip: after, .. } = out else {
                return Err(TestCaseError::reject("re-land conflicted"));
            };
            prop_assert_eq!(tree_of(&b, after).await, tree_before, "tree changed by re-landing");
            Ok(())
        })?;
    }

    /// Same scenario, strict reading of "landing an already-landed commit is
    /// a no-op": the ref must not move at all.
    #[test]
    fn relanding_rebased_original_is_noop(
        base in base_model(), mx in mutations('x'), my in mutations('y')
    ) {
        let rt = runtime();
        rt.block_on(async {
            let b = Backend::in_memory();
            let c0 = init_ref(&b, "main", &base).await;
            let xm = apply_mutations(&base, &mx);
            let ym = apply_mutations(&base, &my);
            let (Some(x), Some(y)) = (
                mk_commit(&b, c0, &base, &xm, "x\n").await,
                mk_commit(&b, c0, &base, &ym, "y\n").await,
            ) else { return Err(TestCaseError::reject("x")) };
            land(&b, "main", x).await.unwrap();
            let LandOutcome::Landed { new_tip, rebased: true } = land(&b, "main", y).await.unwrap()
            else { return Err(TestCaseError::reject("conflict")) };
            land(&b, "main", y).await.unwrap();
            prop_assert_eq!(tip(&b, "main").await, new_tip);
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(config(64))]

    /// A retry of an already-landed commit must not be reported as a conflict.
    #[test]
    fn relanding_original_never_conflicts(
        base in base_model(), mx in mutations('x'), my in mutations('y')
    ) {
        let rt = runtime();
        rt.block_on(async {
            let b = Backend::in_memory();
            let c0 = init_ref(&b, "main", &base).await;
            let xm = apply_mutations(&base, &mx);
            let ym = apply_mutations(&base, &my);
            let (Some(x), Some(y)) = (
                mk_commit(&b, c0, &base, &xm, "x\n").await,
                mk_commit(&b, c0, &base, &ym, "y\n").await,
            ) else { return Err(TestCaseError::reject("x")) };
            land(&b, "main", x).await.unwrap();
            let LandOutcome::Landed { rebased: true, .. } = land(&b, "main", y).await.unwrap()
            else { return Err(TestCaseError::reject("conflict")) };
            let out = land(&b, "main", y).await.unwrap();
            prop_assert!(matches!(out, LandOutcome::Landed { .. }), "{:?}", out);
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn regression_reland_file_to_dir_conflicts() {
    let b = Backend::in_memory();
    let base: Model = [("a".to_string(), (EntryMode::File, vec![]))].into();
    let c0 = init_ref(&b, "main", &base).await;
    let ym: Model = [("a/a".to_string(), (EntryMode::File, vec![]))].into();
    let x = mk_commit(&b, c0, &base, &base, "x\n").await.unwrap();
    let y = mk_commit(&b, c0, &base, &ym, "y\n").await.unwrap();
    land(&b, "main", x).await.unwrap();
    let out = land(&b, "main", y).await.unwrap();
    assert!(matches!(out, LandOutcome::Landed { rebased: true, .. }), "{out:?}");
    let out = land(&b, "main", y).await.unwrap();
    assert!(matches!(out, LandOutcome::Landed { .. }), "{out:?}");
}

#[tokio::test]
async fn regression_reland_original_moves_ref() {
    let b = Backend::in_memory();
    let base: Model = [("a".to_string(), (EntryMode::File, b"1\n".to_vec()))].into();
    let c0 = init_ref(&b, "main", &base).await;
    let xm: Model = [("a".to_string(), (EntryMode::File, b"2\n".to_vec()))].into();
    let ym: Model = [
        ("a".to_string(), (EntryMode::File, b"1\n".to_vec())),
        ("b".to_string(), (EntryMode::File, b"1\n".to_vec())),
    ]
    .into();
    let x = mk_commit(&b, c0, &base, &xm, "x\n").await.unwrap();
    let y = mk_commit(&b, c0, &base, &ym, "y\n").await.unwrap();
    land(&b, "main", x).await.unwrap();
    let LandOutcome::Landed { new_tip, .. } = land(&b, "main", y).await.unwrap() else { panic!() };
    land(&b, "main", y).await.unwrap();
    assert_eq!(tip(&b, "main").await, new_tip);
}

/// Landing the top of an unlanded two-commit stack onto a moved tip rebases
/// only the top commit, silently dropping the lower commit's changes.
#[tokio::test]
async fn regression_stack_landing_drops_parent_changes() {
    let b = Backend::in_memory();
    let base: Model = [("a".to_string(), (EntryMode::File, b"1\n".to_vec()))].into();
    let c0 = init_ref(&b, "main", &base).await;
    let file = |c: &str| (EntryMode::File, c.as_bytes().to_vec());
    let mut t1 = base.clone();
    t1.insert("p".into(), file("from c1\n"));
    let mut t2 = t1.clone();
    t2.insert("q".into(), file("from c2\n"));
    let mut tx = base.clone();
    tx.insert("z".into(), file("moved tip\n"));
    let c1 = mk_commit(&b, c0, &base, &t1, "c1\n").await.unwrap();
    let c2 = mk_commit(&b, c1, &t1, &t2, "c2\n").await.unwrap();
    let x = mk_commit(&b, c0, &base, &tx, "x\n").await.unwrap();
    land(&b, "main", x).await.unwrap();
    let LandOutcome::Landed { new_tip, .. } = land(&b, "main", c2).await.unwrap() else { panic!() };
    let fin = final_contents(&b, new_tip).await;
    assert!(fin.contains_key("q"));
    assert!(fin.contains_key("p"), "c1's file p was dropped: {:?}", fin.keys().collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum TaskKind {
    /// Replace the whole file with unique content.
    Whole,
    /// Replace one line (0..8) with a unique marker.
    Line(usize),
    Delete,
}

#[derive(Debug, Clone)]
struct TaskPlan {
    edits: Vec<(usize, TaskKind)>, // (pool index, kind)
}

const POOL: [&str; 6] = ["p0", "p1", "d/q0", "d/q1", "e/f/q2", "e/f/q3"];

fn pool_base() -> Model {
    POOL.iter()
        .map(|p| {
            let text: String = (0..8).map(|i| format!("{p}-l{i}\n")).collect();
            (p.to_string(), (EntryMode::File, text.into_bytes()))
        })
        .collect()
}

fn task_plan() -> impl Strategy<Value = TaskPlan> {
    let kind = prop_oneof![
        3 => Just(TaskKind::Whole),
        5 => (0usize..8).prop_map(TaskKind::Line),
        2 => Just(TaskKind::Delete),
    ];
    prop::collection::vec((0..POOL.len(), kind), 1..3)
        .prop_map(|mut edits| {
            let mut seen = BTreeSet::new();
            edits.retain(|(p, _)| seen.insert(*p));
            TaskPlan { edits }
        })
}

fn target_for(base: &Model, i: usize, plan: &TaskPlan) -> Model {
    let mut t = base.clone();
    for (p, kind) in &plan.edits {
        let path = POOL[*p].to_string();
        match kind {
            TaskKind::Delete => {
                t.remove(&path);
            }
            TaskKind::Whole => {
                let text: String = (0..8).map(|l| format!("whole-{i}-{l}\n")).collect();
                t.insert(path, (EntryMode::File, text.into_bytes()));
            }
            TaskKind::Line(k) => {
                let old = String::from_utf8(base[&path].1.clone()).unwrap();
                let new: String = old
                    .lines()
                    .enumerate()
                    .map(|(n, l)| if n == *k { format!("edit-{i}-{n}\n") } else { format!("{l}\n") })
                    .collect();
                t.insert(path, (EntryMode::File, new.into_bytes()));
            }
        }
    }
    t
}

async fn concurrency_case(b: Backend, ref_name: String, plans: Vec<TaskPlan>) -> Result<(), TestCaseError> {
    let base = pool_base();
    let c0 = init_ref(&b, &ref_name, &base).await;
    let mut commits = Vec::new();
    let mut targets = Vec::new();
    for (i, plan) in plans.iter().enumerate() {
        let t = target_for(&base, i, plan);
        commits.push(mk_commit(&b, c0, &base, &t, &format!("task-{i}\n")).await.unwrap());
        targets.push(t);
    }

    let barrier = Arc::new(tokio::sync::Barrier::new(commits.len()));
    let handles: Vec<_> = commits
        .iter()
        .map(|&c| {
            let (b, barrier, name) = (b.clone(), barrier.clone(), ref_name.clone());
            tokio::spawn(async move {
                barrier.wait().await;
                land(&b, &name, c).await
            })
        })
        .collect();
    let mut outcomes = Vec::new();
    for h in handles {
        outcomes.push(h.await.unwrap());
    }
    let mut landed = BTreeSet::new();
    for (i, o) in outcomes.iter().enumerate() {
        match o {
            Ok(LandOutcome::Landed { .. }) => {
                landed.insert(i);
            }
            Ok(LandOutcome::Conflict { paths, .. }) => prop_assert!(!paths.is_empty()),
            Err(e) => return Err(TestCaseError::fail(format!("task {i}: {e}"))),
        }
    }
    prop_assert!(!landed.is_empty(), "at least the first CAS winner must land");

    // First-parent history from tip to c0 is linear, one commit per Landed.
    let final_tip = tip(&b, &ref_name).await;
    let mut chain = Vec::new(); // newest first
    let mut cur = final_tip;
    while cur != c0 {
        let c = b.objects.read_commit(&cur).await.unwrap();
        prop_assert_eq!(c.parents.len(), 1, "non-linear history at {}", cur);
        chain.push((cur, c.message.clone()));
        cur = c.parents[0];
        prop_assert!(chain.len() <= commits.len(), "history longer than number of tasks");
    }
    chain.reverse(); // oldest first
    prop_assert_eq!(chain.len(), landed.len(), "commits != Landed outcomes");
    let landed_tips: BTreeSet<ObjectId> = outcomes
        .iter()
        .filter_map(|o| match o {
            Ok(LandOutcome::Landed { new_tip, .. }) => Some(*new_tip),
            _ => None,
        })
        .collect();
    let chain_ids: BTreeSet<ObjectId> = chain.iter().map(|(id, _)| *id).collect();
    prop_assert_eq!(&landed_tips, &chain_ids, "reported new_tips differ from history");
    // Chain order of tasks, by message.
    let order: Vec<usize> = chain
        .iter()
        .map(|(_, m)| m.trim().strip_prefix("task-").unwrap().parse::<usize>().unwrap())
        .collect();
    prop_assert_eq!(order.iter().copied().collect::<BTreeSet<_>>(), landed.clone());

    let fin = final_contents(&b, final_tip).await;
    let flat = flatten(&b, tree_of(&b, final_tip).await).await;
    prop_assert!(flat.empty_dirs.is_empty());
    let all_text: String = fin.values().map(|v| String::from_utf8_lossy(&v.1).to_string()).collect();

    for (pos, &i) in order.iter().enumerate() {
        let later: Vec<usize> = order[pos + 1..].to_vec();
        for (p, kind) in &plans[i].edits {
            let path = POOL[*p];
            let touched_later = later
                .iter()
                .any(|&j| plans[j].edits.iter().any(|(q, k)| q == p && !same_line_free(kind, k)));
            if touched_later {
                continue;
            }
            match kind {
                TaskKind::Delete => prop_assert!(!fin.contains_key(path), "{} resurrected", path),
                TaskKind::Whole => {
                    prop_assert_eq!(fin.get(path), targets[i].get(path), "task {} lost whole write to {}", i, path)
                }
                TaskKind::Line(k) => {
                    let got = fin.get(path).map(|v| lines(&v.1).get(*k).copied().unwrap_or("").to_string());
                    prop_assert_eq!(got, Some(format!("edit-{i}-{k}")), "task {} lost line edit in {}", i, path);
                }
            }
        }
    }
    // Conflicted tasks leave no trace.
    for (i, o) in outcomes.iter().enumerate() {
        if matches!(o, Ok(LandOutcome::Conflict { .. })) {
            prop_assert!(!all_text.contains(&format!("edit-{i}-")), "conflicted task {} leaked", i);
            prop_assert!(!all_text.contains(&format!("whole-{i}-")), "conflicted task {} leaked", i);
        }
    }
    Ok(())
}

/// A later landed edit only excuses an earlier one if it could have
/// overwritten it: whole writes and deletes always, line edits only for the
/// same line.
fn same_line_free(earlier: &TaskKind, later: &TaskKind) -> bool {
    match (earlier, later) {
        (TaskKind::Line(a), TaskKind::Line(b)) => a != b,
        _ => false,
    }
}

proptest! {
    #![proptest_config(config(48))]

    #[test]
    fn concurrent_lands_in_memory(plans in prop::collection::vec(task_plan(), 2..=32)) {
        let rt = runtime();
        rt.block_on(concurrency_case(Backend::in_memory(), "main".into(), plans))?;
    }
}

fn pg_url() -> String {
    std::env::var("ANT_TEST_PG_URL").unwrap_or_else(|_| "postgres://ant:ant@localhost:54329/ant".into())
}

#[test]
#[ignore = "needs docker compose"]
fn concurrent_lands_postgres() {
    use sqlx::Connection;
    let rt = runtime();
    let schema = format!(
        "props_{}_{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    let pg = Arc::new(
        rt.block_on(PgStore::connect(&pg_url(), &schema))
            .expect("connect (is `docker compose up -d` running?)"),
    );
    let backend = Backend { objects: pg.clone(), refs: pg.clone(), meta: pg };
    let counter = std::sync::atomic::AtomicUsize::new(0);

    let mut runner = proptest::test_runner::TestRunner::new(config(12));
    let result = runner.run(&prop::collection::vec(task_plan(), 2..=32), |plans| {
        let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        rt.block_on(concurrency_case(backend.clone(), format!("main-{n}"), plans))
    });

    // Always drop the schema, even when the property failed.
    rt.block_on(async {
        let mut conn = sqlx::PgConnection::connect(&pg_url()).await.unwrap();
        let stmt = format!("DROP SCHEMA \"{schema}\" CASCADE");
        sqlx::query(sqlx::AssertSqlSafe(stmt.as_str())).execute(&mut conn).await.unwrap();
    });
    if let Err(e) = result {
        panic!("{e}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ancestor_check_sanity() {
    // Guards the helper stack used above: a rebased commit descends from the
    // tip it was rebased onto and not from its own pre-rebase original.
    let b = Backend::in_memory();
    let base: Model = [("a".to_string(), (EntryMode::File, b"1\n".to_vec()))].into();
    let c0 = init_ref(&b, "main", &base).await;
    let bm: Model = [
        ("a".to_string(), (EntryMode::File, b"1\n".to_vec())),
        ("b".to_string(), (EntryMode::File, b"b\n".to_vec())),
    ]
    .into();
    let cm: Model = [
        ("a".to_string(), (EntryMode::File, b"1\n".to_vec())),
        ("c".to_string(), (EntryMode::File, b"c\n".to_vec())),
    ]
    .into();
    let x = mk_commit(&b, c0, &base, &bm, "x\n").await.unwrap();
    let y = mk_commit(&b, c0, &base, &cm, "y\n").await.unwrap();
    land(&b, "main", x).await.unwrap();
    let LandOutcome::Landed { new_tip, .. } = land(&b, "main", y).await.unwrap() else { panic!() };
    assert!(is_ancestor(&b, x, new_tip).await.unwrap());
    assert!(!is_ancestor(&b, y, new_tip).await.unwrap());
}
