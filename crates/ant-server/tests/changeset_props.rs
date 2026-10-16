//! Model-based property test for `ops::changeset::create_changeset`.
//!
//! The model is a flat `BTreeMap<path, (mode, content)>`. An oracle derived
//! from the contract in the `changeset.rs` module docs decides whether a list
//! of ops is valid (and which error variants are acceptable if it is not);
//! valid op lists must produce exactly the model's tree, keep untouched
//! subtree ids, never leave empty directories, and record a changeset equal
//! to `diff_trees(base, new)`.

use std::collections::{BTreeMap, BTreeSet};

use ant_core::{EncodedObject, EntryMode, Object, ObjectId, ObjectKind, RepoPath, Tree};
use ant_server::ops::OpError;
use ant_server::ops::changeset::{ChangesetRequest, FileOp, WriteContent, create_changeset};
use ant_store::graph::diff_trees;
use ant_store::{Backend, ChangeKind, ObjectStoreExt, PathChange};
use bytes::Bytes;
use proptest::prelude::*;

mod common;
use common::*;

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

fn component() -> impl Strategy<Value = &'static str> {
    prop_oneof![4 => Just("a"), 4 => Just("b"), 2 => Just("c"), 1 => Just("a.x")]
}

fn path() -> impl Strategy<Value = String> {
    prop::collection::vec(component(), 1..=4).prop_map(|v| v.join("/"))
}

fn content() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        Just(Vec::new()),
        Just(b"a".to_vec()),
        Just(b"b\n".to_vec()),
        Just(b"\0\xff".to_vec())
    ]
}

fn blob_mode() -> impl Strategy<Value = EntryMode> {
    prop_oneof![
        3 => Just(EntryMode::File),
        1 => Just(EntryMode::Executable),
        1 => Just(EntryMode::Symlink)
    ]
}

fn base_mode() -> impl Strategy<Value = EntryMode> {
    prop_oneof![9 => blob_mode(), 1 => Just(EntryMode::Gitlink)]
}

/// A consistent existing tree: no path is both a file and a directory.
fn model() -> impl Strategy<Value = Model> {
    prop::collection::vec((path(), base_mode(), content()), 0..9).prop_map(|items| {
        let mut m = Model::new();
        for (p, mode, c) in items {
            if conflicts(&m, &p) {
                continue;
            }
            m.insert(p, (mode, c));
        }
        m
    })
}

#[derive(Debug, Clone)]
enum Kind {
    Inline,
    Stored,
    Missing,
    NotBlob,
    BadMode(EntryMode),
    Delete,
}

#[derive(Debug, Clone)]
struct OpSpec {
    path: String,
    mode: EntryMode,
    content: Vec<u8>,
    kind: Kind,
}

fn op_spec() -> impl Strategy<Value = OpSpec> {
    let kind = prop_oneof![
        8 => Just(Kind::Inline),
        3 => Just(Kind::Stored),
        1 => Just(Kind::Missing),
        1 => Just(Kind::NotBlob),
        1 => prop_oneof![Just(EntryMode::Tree), Just(EntryMode::Gitlink)].prop_map(Kind::BadMode),
        8 => Just(Kind::Delete),
    ];
    let p = prop_oneof![40 => path(), 1 => Just(String::new())];
    (p, blob_mode(), content(), kind).prop_map(|(path, mode, content, kind)| OpSpec {
        path,
        mode,
        content,
        kind,
    })
}

// ---------------------------------------------------------------------------
// Oracle (from the contract in changeset.rs)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Variant {
    Invalid,
    NotFound,
}

fn oracle(base: &Model, ops: &[OpSpec], strict: bool) -> Result<Model, BTreeSet<Variant>> {
    let mut errs = BTreeSet::new();
    let mut seen = BTreeSet::new();
    let deleted: BTreeSet<&str> =
        ops.iter().filter(|o| matches!(o.kind, Kind::Delete)).map(|o| o.path.as_str()).collect();
    for o in ops {
        if o.path.is_empty() || !seen.insert(o.path.as_str()) {
            errs.insert(Variant::Invalid);
        }
        match o.kind {
            Kind::Delete => {
                if !base.contains_key(&o.path) {
                    errs.insert(Variant::NotFound);
                    // Lenient mode tolerates the known deviation: deleting
                    // below an existing file reports InvalidArgument.
                    if !strict && base.keys().any(|k| is_prefix(k, &o.path)) {
                        errs.insert(Variant::Invalid);
                    }
                }
            }
            Kind::Missing => {
                errs.insert(Variant::NotFound);
            }
            Kind::NotBlob | Kind::BadMode(_) => {
                errs.insert(Variant::Invalid);
            }
            Kind::Inline | Kind::Stored => {
                // Path is currently a directory.
                if base.keys().any(|k| is_prefix(&o.path, k)) {
                    errs.insert(Variant::Invalid);
                }
                // Another op addresses something beneath a path written as a file.
                if ops.iter().any(|other| is_prefix(&o.path, &other.path)) {
                    errs.insert(Variant::Invalid);
                }
                // A proper ancestor is a non-tree in the base and not deleted here.
                let mut comps: Vec<&str> = o.path.split('/').collect();
                comps.pop();
                for i in 1..=comps.len() {
                    let anc = comps[..i].join("/");
                    if base.contains_key(&anc) && !deleted.contains(anc.as_str()) {
                        errs.insert(Variant::Invalid);
                    }
                }
            }
        }
    }
    if !errs.is_empty() {
        return Err(errs);
    }
    let mut m = base.clone();
    for o in ops {
        if matches!(o.kind, Kind::Delete) {
            m.remove(&o.path);
        }
    }
    for o in ops {
        if matches!(o.kind, Kind::Inline | Kind::Stored) {
            m.insert(o.path.clone(), (o.mode, o.content.clone()));
        }
    }
    // A file written at `p` cannot also have written children.
    for k in m.keys() {
        if m.keys().any(|other| is_prefix(k, other)) {
            return Err(BTreeSet::from([Variant::Invalid]));
        }
    }
    Ok(m)
}

fn model_diff(old: &Model, new: &Model) -> Vec<PathChange> {
    let keys: BTreeSet<&String> = old.keys().chain(new.keys()).collect();
    let mut out = Vec::new();
    for k in keys {
        let o = old.get(k).map(|(m, c)| (*m, entry_id(c)));
        let n = new.get(k).map(|(m, c)| (*m, entry_id(c)));
        if o == n {
            continue;
        }
        let kind = match (o, n) {
            (None, _) => ChangeKind::Added,
            (_, None) => ChangeKind::Deleted,
            _ => ChangeKind::Modified,
        };
        out.push(PathChange { path: RepoPath::new(k.clone()).unwrap(), kind, old: o, new: n });
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out
}

// ---------------------------------------------------------------------------
// The property
// ---------------------------------------------------------------------------

fn to_file_ops(
    b: &Backend,
    specs: &[OpSpec],
    missing_seed: &mut u8,
) -> (Vec<FileOp>, Vec<EncodedObject>) {
    let _ = b;
    let mut stored = Vec::new();
    let ops = specs
        .iter()
        .map(|s| {
            let path = RepoPath::new(s.path.clone()).unwrap();
            match &s.kind {
                Kind::Delete => FileOp::Delete { path },
                Kind::Inline => FileOp::Write {
                    path,
                    mode: s.mode,
                    content: WriteContent::Inline(Bytes::from(s.content.clone())),
                },
                Kind::Stored => {
                    let o = EncodedObject::new(ObjectKind::Blob, &s.content);
                    let id = o.id();
                    stored.push(o);
                    FileOp::Write { path, mode: s.mode, content: WriteContent::Blob(id) }
                }
                Kind::Missing => {
                    *missing_seed = missing_seed.wrapping_add(1);
                    FileOp::Write {
                        path,
                        mode: s.mode,
                        content: WriteContent::Blob(ObjectId([*missing_seed; 32])),
                    }
                }
                Kind::NotBlob => FileOp::Write {
                    path,
                    mode: s.mode,
                    content: WriteContent::Blob(ObjectId::empty_tree()),
                },
                Kind::BadMode(m) => {
                    FileOp::Write { path, mode: *m, content: WriteContent::Inline(Bytes::new()) }
                }
            }
        })
        .collect();
    (ops, stored)
}

async fn run_case(
    base: Model,
    use_base: bool,
    specs: Vec<OpSpec>,
    strict: bool,
) -> Result<(), TestCaseError> {
    let b = ant_store::testing::test_backend("changeset_props");
    let base = if use_base { base } else { Model::new() };
    let base_tree = store_model(&b, &base).await;
    let base_id = if use_base { Some(base_commit(&b, base_tree).await) } else { None };
    // Empty tree must exist for the no-base case's diff.
    let mut seed = 0u8;
    let (ops, stored) = to_file_ops(&b, &specs, &mut seed);
    b.objects.put_many(&stored).await.unwrap();
    b.objects.put(Object::Tree(Tree::empty()).encode()).await.unwrap();

    let req = ChangesetRequest {
        base: base_id,
        ops,
        author: sig(),
        committer: None,
        message: "msg\n".into(),
    };
    let result = create_changeset(&b, req).await;
    match (oracle(&base, &specs, strict), result) {
        (Err(allowed), Err(e)) => {
            let got = match e {
                OpError::InvalidArgument(_) => Variant::Invalid,
                OpError::NotFound(_) => Variant::NotFound,
                other => return Err(TestCaseError::fail(format!("unexpected error {other}"))),
            };
            prop_assert!(allowed.contains(&got), "got {:?}, contract allows {:?}", got, allowed);
        }
        (Err(allowed), Ok(_)) => {
            return Err(TestCaseError::fail(format!("expected error in {allowed:?}, got Ok")));
        }
        (Ok(_), Err(e)) => {
            return Err(TestCaseError::fail(format!("valid ops rejected: {e}")));
        }
        (Ok(expected), Ok(commit_id)) => {
            let commit = b.objects.read_commit(&commit_id).await.unwrap();
            prop_assert_eq!(&commit.parents, &base_id.into_iter().collect::<Vec<_>>());
            prop_assert_eq!(&commit.author, &sig());
            prop_assert_eq!(&commit.committer, &sig());
            prop_assert_eq!(&commit.message, b"msg\n");

            // Tree equals the model, and is the canonical tree for it.
            let flat = flatten(&b, commit.tree).await;
            let want: BTreeMap<_, _> =
                expected.iter().map(|(p, (m, c))| (p.clone(), (*m, entry_id(c)))).collect();
            prop_assert_eq!(&flat.leaves, &want);
            prop_assert!(flat.empty_dirs.is_empty(), "empty dirs {:?}", flat.empty_dirs);
            let canonical = store_model(&b, &expected).await;
            prop_assert_eq!(commit.tree, canonical);

            // Untouched subtrees keep their ids.
            let before = flatten(&b, base_tree).await;
            let touched =
                |dir: &str| specs.iter().any(|o| o.path == dir || is_prefix(dir, &o.path));
            for (dir, id) in &before.dirs {
                if !touched(dir) {
                    prop_assert_eq!(flat.dirs.get(dir), Some(id), "untouched dir {} changed", dir);
                }
            }

            // Recorded changeset equals diff_trees and the model diff.
            let recorded = b.meta.get_changeset(&commit_id).await.unwrap().unwrap();
            let diff =
                diff_trees(b.objects.as_ref(), Some(base_tree), Some(commit.tree)).await.unwrap();
            prop_assert_eq!(&recorded, &diff);
            prop_assert_eq!(&recorded, &model_diff(&base, &expected));
        }
    }
    Ok(())
}

fn run(base: Model, use_base: bool, specs: Vec<OpSpec>) -> Result<(), TestCaseError> {
    run_with(base, use_base, specs, false)
}

fn run_with(
    base: Model,
    use_base: bool,
    specs: Vec<OpSpec>,
    strict: bool,
) -> Result<(), TestCaseError> {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(run_case(base, use_base, specs, strict))
}

proptest! {
    #[test]
    fn changeset_matches_model(
        base in model(),
        use_base in prop::bool::weighted(0.9),
        specs in prop::collection::vec(op_spec(), 0..7),
    ) {
        run(base, use_base, specs)?;
    }

    /// Same as above with the contract applied literally.
    #[test]
    fn changeset_matches_model_strict(
        base in model(),
        use_base in prop::bool::weighted(0.9),
        specs in prop::collection::vec(op_spec(), 0..7),
    ) {
        run_with(base, use_base, specs, true)?;
    }

    /// Only valid-looking ops (distinct paths, inline writes/deletes of
    /// existing files), to push the success path hard.
    #[test]
    fn changeset_valid_ops_succeed(
        base in model(),
        writes in prop::collection::vec((path(), blob_mode(), content()), 0..5),
        del_mask in prop::collection::vec(any::<bool>(), 0..9),
    ) {
        let mut specs = Vec::new();
        let mut used = BTreeSet::new();
        for (i, k) in base.keys().enumerate() {
            if del_mask.get(i).copied().unwrap_or(false) {
                used.insert(k.clone());
                specs.push(OpSpec { path: k.clone(), mode: EntryMode::File, content: vec![], kind: Kind::Delete });
            }
        }
        for (p, mode, c) in writes {
            if used.insert(p.clone()) {
                specs.push(OpSpec { path: p, mode, content: c, kind: Kind::Inline });
            }
        }
        // Whether this particular mix is valid is decided by the oracle; the
        // run itself asserts agreement either way.
        run(base, true, specs)?;
    }
}

// ---------------------------------------------------------------------------
// Fixed regression cases for contract corners
// ---------------------------------------------------------------------------

fn spec(path: &str, kind: Kind) -> OpSpec {
    OpSpec { path: path.into(), mode: EntryMode::File, content: b"n".to_vec(), kind }
}

fn m(items: &[(&str, &str)]) -> Model {
    items
        .iter()
        .map(|(p, c)| ((*p).to_string(), (EntryMode::File, c.as_bytes().to_vec())))
        .collect()
}

#[test]
fn file_to_dir_and_dir_to_file_replacements() {
    // file -> dir
    run(m(&[("a", "x")]), true, vec![spec("a", Kind::Delete), spec("a/b", Kind::Inline)]).unwrap();
    // dir -> file
    run(
        m(&[("a/b", "x"), ("a/c/d", "y")]),
        true,
        vec![spec("a/b", Kind::Delete), spec("a/c/d", Kind::Delete), spec("a", Kind::Inline)],
    )
    .unwrap(); // oracle: writing where the path "is currently a tree" is an error
}

#[test]
fn delete_last_file_prunes_to_empty_root() {
    run(m(&[("a/b/c/d", "x")]), true, vec![spec("a/b/c/d", Kind::Delete)]).unwrap();
}

#[test]
fn regression_delete_below_a_file_is_not_found() {
    run_with(m(&[("a", "x")]), true, vec![spec("a/b", Kind::Delete)], true).unwrap();
}
