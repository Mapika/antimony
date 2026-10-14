//! Carrying a workspace's local edits onto a new base (`ant ws update`).
//!
//! [`plan`] decides, per path, what the workspace should hold after moving
//! from `old_tree` to `new_tree`. The paths considered are exactly the local
//! changes plus `diff_trees(old_tree, new_tree)` (one batched read per tree
//! depth), so the cost follows the size of both change sets, not of the
//! repository. Each path is classified with the server's land rules
//! ([`ant_core::merge`], base = old tree, ours = upstream, theirs = local):
//!
//! - both sides equal, or only upstream changed → [`Target::Upstream`];
//! - only the local side changed → [`Target::Local`];
//! - both changed text files → 3-way text merge, exactly as a land would
//!   rebase the local edit onto the new tip: a clean merge is
//!   [`Target::Write`]; overlapping edits are written with conflict markers
//!   (`<<<<<<< upstream`, `||||||| base`, `=======`, `>>>>>>> local`) and
//!   reported as a conflict;
//! - anything else changed on both sides (modify/delete, add/add, binary,
//!   symlinks, mode changed on both sides) keeps the local version and is
//!   reported as a conflict;
//! - a file and a path below it can not both exist: when the merged results
//!   would need that, the local side wins and the other path conflicts.
//!
//! Applying a plan is up to the workspace kind (`Workspace::update` for
//! sparse workspaces, `ant_fuse::rw::update` for lazy ones). Unresolved
//! conflicts are listed in a conflicts file ([`render_conflicts`]).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use ant_core::merge::{self, EntryMerge, TextMerge};
use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind, RepoPath, Tree};
use ant_store::ObjectStore;
use ant_store::graph::diff_trees;
use anyhow::{Context, anyhow, bail};

use crate::workspace::{LocalChange, escape_path, unescape_path};

/// A leaf entry at one path (`None`: absent).
pub type Value = Option<(EntryMode, ObjectId)>;

/// Objects per `get_many` call.
const BATCH: usize = 512;

/// What the workspace holds at a path after the update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// The new base's entry (`PathUpdate::upstream`); no local change remains.
    Upstream,
    /// The local entry, unchanged (`PathUpdate::local`).
    Local,
    /// New content: a clean merge, or the local and upstream versions with
    /// conflict markers.
    Write { mode: EntryMode, data: Vec<u8> },
}

/// One path whose content differs between the old base, the new base or the
/// local workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathUpdate {
    pub path: RepoPath,
    /// Entry in the old base tree.
    pub base: Value,
    /// Entry in the workspace before the update.
    pub local: Value,
    /// Entry in the new base tree.
    pub upstream: Value,
    pub target: Target,
    /// Needs the user's attention before it can be submitted.
    pub conflict: bool,
}

impl PathUpdate {
    /// Does the path exist after the update?
    pub fn present(&self) -> bool {
        match self.target {
            Target::Upstream => self.upstream.is_some(),
            Target::Local => self.local.is_some(),
            Target::Write { .. } => true,
        }
    }

    /// The entry the workspace holds afterwards, if it is one of the inputs.
    pub fn result(&self) -> Option<Value> {
        match self.target {
            Target::Upstream => Some(self.upstream),
            Target::Local => Some(self.local),
            Target::Write { .. } => None,
        }
    }
}

/// Compute the update of a workspace at `old_tree` with `local` changes to
/// `new_tree`. Sorted by path. Blobs are read from `objects` (local blobs
/// must be there, as `status` leaves them).
pub async fn plan(
    objects: &dyn ObjectStore,
    old_tree: ObjectId,
    new_tree: ObjectId,
    local: &[LocalChange],
) -> anyhow::Result<Vec<PathUpdate>> {
    let upstream: BTreeMap<RepoPath, (Value, Value)> =
        diff_trees(objects, Some(old_tree), Some(new_tree))
            .await
            .context("diffing the old and new base")?
            .into_iter()
            .map(|c| (c.path, (c.old, c.new)))
            .collect();
    let unknown: Vec<RepoPath> =
        local.iter().filter(|c| !upstream.contains_key(&c.path)).map(|c| c.path.clone()).collect();
    // Untouched upstream, so the old base entry is also the new one.
    let base_of_local = entries_at(objects, old_tree, &unknown).await?;
    let mut paths: BTreeMap<RepoPath, (Value, Value, Value)> = BTreeMap::new();
    for (path, (old, new)) in &upstream {
        paths.insert(path.clone(), (*old, *old, *new));
    }
    for c in local {
        let slot = paths.entry(c.path.clone()).or_insert_with(|| {
            let base = base_of_local.get(&c.path).copied().flatten();
            (base, base, base)
        });
        slot.1 = c.new;
    }
    let mut out = Vec::with_capacity(paths.len());
    let mut texts = Vec::new();
    for (path, (base, local, upstream)) in paths {
        let (target, conflict) = match merge::entries(base, upstream, local) {
            EntryMerge::Same | EntryMerge::Ours => (Target::Upstream, false),
            EntryMerge::Theirs => (Target::Local, false),
            EntryMerge::Text { mode, base, ours, theirs } => {
                texts.push((out.len(), mode, [base, ours, theirs]));
                (Target::Local, false)
            }
            EntryMerge::Conflict => (Target::Local, true),
        };
        out.push(PathUpdate { path, base, local, upstream, target, conflict });
    }
    merge_texts(objects, &mut out, texts).await?;
    shape_conflicts(&mut out);
    Ok(out)
}

async fn merge_texts(
    objects: &dyn ObjectStore,
    out: &mut [PathUpdate],
    texts: Vec<(usize, EntryMode, [ObjectId; 3])>,
) -> anyhow::Result<()> {
    if texts.is_empty() {
        return Ok(());
    }
    let ids: Vec<ObjectId> =
        texts.iter().flat_map(|(_, _, ids)| *ids).collect::<BTreeSet<_>>().into_iter().collect();
    let mut blobs: HashMap<ObjectId, EncodedObject> = HashMap::with_capacity(ids.len());
    for chunk in ids.chunks(BATCH) {
        for (id, obj) in chunk.iter().zip(objects.get_many(chunk).await?) {
            let obj = obj.ok_or_else(|| anyhow!("missing blob {id}"))?;
            if obj.kind() != ObjectKind::Blob {
                bail!("object {id} is not a blob");
            }
            blobs.insert(*id, obj);
        }
    }
    for (i, mode, [base, upstream, local]) in texts {
        let body = |id: &ObjectId| blobs.get(id).map(|o| o.body()).unwrap_or_default();
        let update = &mut out[i];
        match merge::text(body(&base), body(&upstream), body(&local)) {
            TextMerge::Clean(data) => update.target = Target::Write { mode, data },
            TextMerge::Conflict(marked) => {
                update.target = Target::Write { mode, data: relabel(&marked) };
                update.conflict = true;
            }
            TextMerge::Binary => update.conflict = true,
        }
    }
    Ok(())
}

/// Rename `ant_core::merge`'s marker labels for the workspace's point of view.
fn relabel(marked: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(marked.len() + 64);
    for line in marked.split_inclusive(|&b| b == b'\n') {
        let replaced: Option<&[u8]> = match line {
            b"<<<<<<< ours\n" => Some(b"<<<<<<< upstream\n"),
            b"||||||| original\n" => Some(b"||||||| base\n"),
            b">>>>>>> theirs\n" => Some(b">>>>>>> local\n"),
            _ => None,
        };
        out.extend_from_slice(replaced.unwrap_or(line));
    }
    out
}

/// A present path below a present file can not exist. Keep the local side:
/// the upstream-sourced one of the two becomes `Local` and conflicts.
fn shape_conflicts(out: &mut [PathUpdate]) {
    let index: HashMap<&str, usize> =
        out.iter().enumerate().map(|(i, u)| (u.path.as_str(), i)).collect();
    let mut flip = BTreeSet::new();
    for (i, u) in out.iter().enumerate() {
        if !u.present() {
            continue;
        }
        let path = u.path.as_str();
        for (cut, _) in path.match_indices('/') {
            let Some(&a) = index.get(&path[..cut]) else { continue };
            if !out[a].present() {
                continue;
            }
            // Exactly one of the two comes from upstream (the local
            // workspace and the new base are each a valid tree).
            flip.insert(if matches!(out[a].target, Target::Upstream) { a } else { i });
        }
    }
    for i in flip {
        out[i].target = Target::Local;
        out[i].conflict = true;
    }
}

/// The entries at `paths` in `tree` (absent paths are left out), reading
/// only the directories on those paths, one batched read per depth.
pub async fn entries_at(
    objects: &dyn ObjectStore,
    tree: ObjectId,
    paths: &[RepoPath],
) -> anyhow::Result<HashMap<RepoPath, Value>> {
    let mut out = HashMap::new();
    if paths.is_empty() {
        return Ok(out);
    }
    let wanted: BTreeSet<&str> = paths.iter().map(|p| p.as_str()).collect();
    let mut dirs: BTreeSet<&str> = BTreeSet::new();
    for p in &wanted {
        for (cut, _) in p.match_indices('/') {
            dirs.insert(&p[..cut]);
        }
    }
    let mut level = vec![(String::new(), tree)];
    while !level.is_empty() {
        let mut next = Vec::new();
        for chunk in level.chunks(BATCH) {
            let ids: Vec<ObjectId> = chunk.iter().map(|(_, id)| *id).collect();
            for ((prefix, id), obj) in chunk.iter().zip(objects.get_many(&ids).await?) {
                let obj = obj.ok_or_else(|| anyhow!("missing tree {id}"))?;
                if obj.kind() != ObjectKind::Tree {
                    bail!("object {id} is not a tree");
                }
                for e in Tree::decode(obj.body())?.entries() {
                    let path = if prefix.is_empty() {
                        e.name.clone()
                    } else {
                        format!("{prefix}/{}", e.name)
                    };
                    if e.mode.is_tree() {
                        if dirs.contains(path.as_str()) {
                            next.push((path, e.id));
                        }
                    } else if wanted.contains(path.as_str()) {
                        out.insert(RepoPath::new(path)?, Some((e.mode.class(), e.id)));
                    }
                }
            }
        }
        level = next;
    }
    Ok(out)
}

/// The conflicts file: one escaped repo path per line, sorted.
pub fn render_conflicts<'a>(paths: impl IntoIterator<Item = &'a str>) -> String {
    let sorted: BTreeSet<&str> = paths.into_iter().collect();
    sorted.into_iter().map(|p| format!("{}\n", escape_path(p))).collect()
}

pub fn parse_conflicts(text: &str) -> anyhow::Result<Vec<RepoPath>> {
    text.lines().filter(|l| !l.is_empty()).map(|l| Ok(RepoPath::new(unescape_path(l))?)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::LocalChangeKind;
    use ant_store::graph::build_tree;
    use ant_store::memory::MemoryObjectStore;
    use ant_store::{ObjectStore, ObjectStoreExt};

    fn p(s: &str) -> RepoPath {
        RepoPath::new(s).unwrap()
    }

    async fn blob(objs: &MemoryObjectStore, data: &[u8]) -> ObjectId {
        objs.put(EncodedObject::new(ObjectKind::Blob, data)).await.unwrap()
    }

    async fn change(
        objs: &MemoryObjectStore,
        path: &str,
        kind: LocalChangeKind,
        data: Option<&[u8]>,
    ) -> LocalChange {
        let new = match data {
            Some(d) => Some((EntryMode::File, blob(objs, d).await)),
            None => None,
        };
        LocalChange { path: p(path), kind, new }
    }

    #[tokio::test]
    async fn classifies_each_kind_of_path() {
        use LocalChangeKind::*;
        let objs = MemoryObjectStore::default();
        let old = build_tree(
            &objs,
            &[
                ("both", &b"1\n2\n3\n4\n5\n"[..]),
                ("clash", &b"1\n2\n3\n"[..]),
                ("local", &b"l"[..]),
                ("up", &b"u"[..]),
                ("same", &b"s"[..]),
                ("moddel", &b"m"[..]),
                ("bin", &b"\xff1"[..]),
                ("d/x", &b"x"[..]),
            ],
        )
        .await
        .unwrap();
        let new = build_tree(
            &objs,
            &[
                ("both", &b"one\n2\n3\n4\n5\n"[..]),
                ("clash", &b"1\nup\n3\n"[..]),
                ("local", &b"l"[..]),
                ("up", &b"u2"[..]),
                ("same", &b"s2"[..]),
                ("bin", &b"\xff2"[..]),
                ("d", &b"now a file"[..]),
                ("added", &b"a"[..]),
            ],
        )
        .await
        .unwrap();
        let local = vec![
            change(&objs, "bin", Modified, Some(b"\xff3")).await,
            change(&objs, "both", Modified, Some(b"1\n2\n3\n4\nfive\n")).await,
            change(&objs, "clash", Modified, Some(b"1\nlocal\n3\n")).await,
            change(&objs, "d/new", Added, Some(b"n")).await,
            change(&objs, "local", Modified, Some(b"l2")).await,
            change(&objs, "moddel", Modified, Some(b"m2")).await,
            change(&objs, "same", Modified, Some(b"s2")).await,
        ];
        let plan = plan(&objs, old, new, &local).await.unwrap();
        let got: Vec<(&str, &Target, bool)> =
            plan.iter().map(|u| (u.path.as_str(), &u.target, u.conflict)).collect();
        let write = |data: &[u8]| Target::Write { mode: EntryMode::File, data: data.to_vec() };
        let clash =
            write(b"1\n<<<<<<< upstream\nup\n||||||| base\n2\n=======\nlocal\n>>>>>>> local\n3\n");
        assert_eq!(
            got,
            [
                ("added", &Target::Upstream, false),
                ("bin", &Target::Local, true),
                ("both", &write(b"one\n2\n3\n4\nfive\n"), false),
                ("clash", &clash, true),
                // Upstream turned d into a file; the local new file below it wins.
                ("d", &Target::Local, true),
                ("d/new", &Target::Local, false),
                ("d/x", &Target::Upstream, false),
                ("local", &Target::Local, false),
                ("moddel", &Target::Local, true),
                ("same", &Target::Upstream, false),
                ("up", &Target::Upstream, false),
            ]
        );
        let d = plan.iter().find(|u| u.path.as_str() == "d").unwrap();
        assert!(!d.present());
    }

    #[tokio::test]
    async fn reads_only_directories_on_changed_paths() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Default)]
        struct Counting(MemoryObjectStore, AtomicUsize);
        #[async_trait::async_trait]
        impl ObjectStore for Counting {
            async fn get_many(
                &self,
                ids: &[ObjectId],
            ) -> ant_store::Result<Vec<Option<EncodedObject>>> {
                self.1.fetch_add(ids.len(), Ordering::SeqCst);
                self.0.get_many(ids).await
            }
            async fn put_many(&self, o: &[EncodedObject]) -> ant_store::Result<()> {
                self.0.put_many(o).await
            }
            async fn has_many(&self, ids: &[ObjectId]) -> ant_store::Result<Vec<bool>> {
                self.0.has_many(ids).await
            }
        }
        let objs = Counting::default();
        let mut files: Vec<(String, Vec<u8>)> =
            (0..2000).map(|i| (format!("d{}/f{i}", i % 100), b"x".to_vec())).collect();
        let refs: Vec<(&str, &[u8])> =
            files.iter().map(|(a, b)| (a.as_str(), b.as_slice())).collect();
        let old = build_tree(&objs.0, &refs).await.unwrap();
        files[5].1 = b"y".to_vec();
        let refs: Vec<(&str, &[u8])> =
            files.iter().map(|(a, b)| (a.as_str(), b.as_slice())).collect();
        let new = build_tree(&objs.0, &refs).await.unwrap();
        let local = vec![change(&objs.0, "d7/f7", LocalChangeKind::Modified, Some(b"z")).await];
        objs.1.store(0, Ordering::SeqCst);
        let plan = plan(&objs, old, new, &local).await.unwrap();
        assert_eq!(plan.len(), 2);
        // Root + one directory per side for the diff, root + d7 for the local path.
        assert!(objs.1.load(Ordering::SeqCst) <= 8, "{}", objs.1.load(Ordering::SeqCst));
    }

    #[test]
    fn conflicts_file_round_trip() {
        let text = render_conflicts(["b", "a\nb", "a"]);
        assert_eq!(text, "a\na\\nb\nb\n");
        let paths: Vec<String> =
            parse_conflicts(&text).unwrap().into_iter().map(|p| p.to_string()).collect();
        assert_eq!(paths, ["a", "a\nb", "b"]);
    }
}
