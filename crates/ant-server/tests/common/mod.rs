//! Shared helpers for the property tests: an independent tree builder/reader
//! (does not use the code under test) and commit helpers.
#![allow(dead_code)]

use std::collections::BTreeMap;

use ant_core::{
    Commit, EncodedObject, EntryMode, Object, ObjectId, ObjectKind, Signature, Tree, TreeEntry,
    hash_object,
};
use ant_store::graph::write_commit;
use ant_store::{Backend, ObjectStoreExt};

pub type Model = BTreeMap<String, (EntryMode, Vec<u8>)>;

pub fn entry_id(content: &[u8]) -> ObjectId {
    hash_object(ObjectKind::Blob, content)
}

// ---------------------------------------------------------------------------
// Independent tree builder / reader (does not use the code under test)
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct Dir {
    files: BTreeMap<String, (EntryMode, Vec<u8>)>,
    pub dirs: BTreeMap<String, Dir>,
}

pub fn nest(model: &Model) -> Dir {
    let mut root = Dir::default();
    for (path, v) in model {
        let comps: Vec<&str> = path.split('/').collect();
        let (name, parents) = comps.split_last().expect("non-empty path");
        let mut d = &mut root;
        for c in parents {
            d = d.dirs.entry((*c).to_string()).or_default();
        }
        d.files.insert((*name).to_string(), v.clone());
    }
    root
}

pub fn build(d: &Dir, out: &mut Vec<EncodedObject>) -> ObjectId {
    let mut entries = Vec::new();
    for (name, (mode, content)) in &d.files {
        if *mode != EntryMode::Gitlink {
            out.push(EncodedObject::new(ObjectKind::Blob, content));
        }
        entries.push(TreeEntry { mode: *mode, name: name.clone(), id: entry_id(content) });
    }
    for (name, sub) in &d.dirs {
        let id = build(sub, out);
        entries.push(TreeEntry { mode: EntryMode::Tree, name: name.clone(), id });
    }
    let obj = Object::Tree(Tree::new(entries).expect("valid tree")).encode();
    let id = obj.id();
    out.push(obj);
    id
}

pub async fn store_model(b: &Backend, model: &Model) -> ObjectId {
    let mut objs = Vec::new();
    let id = build(&nest(model), &mut objs);
    b.objects.put_many(&objs).await.unwrap();
    id
}

pub fn sig() -> Signature {
    Signature { name: "p".into(), email: "p@x".into(), time: 1_700_000_000, tz_offset_minutes: 90 }
}

pub async fn base_commit(b: &Backend, tree: ObjectId) -> ObjectId {
    let c = Commit {
        tree,
        parents: vec![],
        author: sig(),
        committer: sig(),
        extra_headers: vec![],
        message: "base\n".into(),
    };
    write_commit(b, &c).await.unwrap()
}

pub struct Flat {
    pub leaves: BTreeMap<String, (EntryMode, ObjectId)>,
    pub dirs: BTreeMap<String, ObjectId>,
    pub empty_dirs: Vec<String>,
}

pub async fn flatten(b: &Backend, root: ObjectId) -> Flat {
    let mut flat = Flat { leaves: BTreeMap::new(), dirs: BTreeMap::new(), empty_dirs: vec![] };
    let mut stack = vec![(String::new(), root)];
    while let Some((prefix, id)) = stack.pop() {
        let t = b.objects.read_tree(&id).await.unwrap();
        if t.is_empty() && !prefix.is_empty() {
            flat.empty_dirs.push(prefix.clone());
        }
        for e in t.entries() {
            let p = if prefix.is_empty() { e.name.clone() } else { format!("{prefix}/{}", e.name) };
            if e.mode.is_tree() {
                flat.dirs.insert(p.clone(), e.id);
                stack.push((p, e.id));
            } else {
                flat.leaves.insert(p, (e.mode, e.id));
            }
        }
    }
    flat
}

pub fn is_prefix(dir: &str, path: &str) -> bool {
    path.len() > dir.len() && path.starts_with(dir) && path.as_bytes()[dir.len()] == b'/'
}

pub fn conflicts(m: &Model, p: &str) -> bool {
    m.keys().any(|k| is_prefix(k, p) || is_prefix(p, k))
}

/// Leaf contents of a tree (blob modes only), read through the object store.
pub async fn contents(b: &Backend, tree: ObjectId) -> Model {
    let flat = flatten(b, tree).await;
    let mut out = Model::new();
    for (path, (mode, id)) in flat.leaves {
        let data = if mode == EntryMode::Gitlink {
            Vec::new()
        } else {
            b.objects.read_blob(&id).await.unwrap().to_vec()
        };
        out.insert(path, (mode, data));
    }
    out
}

/// Proptest config with `default_cases` unless `PROPTEST_CASES` is set.
pub fn config(default_cases: u32) -> proptest::test_runner::Config {
    let mut c = proptest::test_runner::Config::default();
    if std::env::var("PROPTEST_CASES").is_err() {
        c.cases = default_cases;
    }
    c.failure_persistence = None;
    c.max_global_rejects = 1_000_000;
    c
}
