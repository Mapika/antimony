//! The mark of a garbage collection (`crate::postgres::gc`,
//! `crate::local::gc`): the closure of a set of roots (commit → tree and
//! parents, tree → entries, gitlinks skipped), in memory.
//!
//! What makes it fast on a large history:
//!
//! - **Commit records in memory.** The commit graph is walked from the
//!   records ([`CommitRecords`]: tree and parents of every recorded commit,
//!   loaded with one scan) instead of one store round trip per commit; a
//!   history is a long, narrow graph, so a frontier walk asking the store
//!   for each generation's commits makes as many round trips as there are
//!   generations. Commits without a record (pushed raw, or recorded after
//!   the scan) are looked up ([`RecordSource::records`]) and, failing that,
//!   read and parsed.
//! - **Every id once.** An id is marked when first seen and expanded once,
//!   so a subtree that thousands of commits share is read once.
//! - **Trees level by level, newest commits first.** The root trees of all
//!   marked commits form the first level, in the order `ant admin repack`
//!   lays history out (generation, then committer time, newest first); each
//!   level's children follow their parents' order. Reads of one level are
//!   therefore runs of nearby versions of the same directories, which share
//!   pack records: the packed store's frame cache decodes each record about
//!   once instead of once per object read in id order.
//! - **Parallel reads.** A level is read in parts of [`MarkOptions::batch`]
//!   ids, [`MarkOptions::reads`] parts in flight; trees are parsed on the
//!   blocking pool. Only the deduplication runs on the caller's task.
//!
//! The mark set is an in-memory hash set of ids (about 50 bytes per
//! reachable object: ~600 MB for the 11.7M objects of the Linux kernel's
//! history), plus the commit records (~150 bytes per commit).

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

use ant_core::{EncodedObject, EntryMode, ObjectId, ObjectKind};
use async_trait::async_trait;
use futures::stream::{self, StreamExt};

use crate::{ObjectStore, Result, StoreError};

/// Identity hasher for object ids (SHA-256 digests: their first 8 bytes are
/// already uniformly distributed).
#[derive(Default)]
pub(crate) struct IdHasher(u64);

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        if let Some(head) = bytes.first_chunk::<8>() {
            self.0 = u64::from_le_bytes(*head);
        } else {
            for b in bytes {
                self.0 = self.0.rotate_left(8) ^ u64::from(*b);
            }
        }
    }
}

pub(crate) type IdBuild = BuildHasherDefault<IdHasher>;
/// A set of object ids.
pub(crate) type IdSet = HashSet<ObjectId, IdBuild>;

/// What an id is known to be when it is marked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// A ref target, a derived row, a recently written row: read to find out
    /// (a commit record answers for commits).
    Unknown,
    Commit,
    Tree,
    /// Never read: blobs have no children.
    Blob,
}

/// One commit record: tree, parents and the walk order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Record {
    pub tree: ObjectId,
    pub parents: Vec<ObjectId>,
    pub generation: u64,
    pub committer_time: i64,
}

/// Commit records held in memory (see the module docs).
#[derive(Default)]
pub(crate) struct CommitRecords {
    index: HashMap<ObjectId, u32, IdBuild>,
    /// `(tree, first parent in parents, parent count, generation, time)`.
    nodes: Vec<(ObjectId, u32, u32, u64, i64)>,
    parents: Vec<ObjectId>,
}

impl CommitRecords {
    pub fn insert(&mut self, id: ObjectId, r: &Record) {
        if self.index.contains_key(&id) {
            return;
        }
        let at = self.parents.len() as u32;
        self.parents.extend_from_slice(&r.parents);
        self.index.insert(id, self.nodes.len() as u32);
        self.nodes.push((r.tree, at, r.parents.len() as u32, r.generation, r.committer_time));
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    fn get(&self, id: &ObjectId) -> Option<(ObjectId, &[ObjectId], (u64, i64))> {
        let &(tree, at, n, generation, time) = &self.nodes[*self.index.get(id)? as usize];
        Some((tree, &self.parents[at as usize..(at + n) as usize], (generation, time)))
    }
}

/// Where commit records not held in memory are looked up.
#[async_trait]
pub(crate) trait RecordSource: Send + Sync {
    /// The records of those of `ids` that have one.
    async fn records(&self, ids: &[ObjectId]) -> Result<Vec<(ObjectId, Record)>>;
}

/// Tuning of a mark.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MarkOptions {
    /// Ids per object read.
    pub batch: usize,
    /// Object reads in flight.
    pub reads: usize,
}

impl Default for MarkOptions {
    fn default() -> Self {
        Self { batch: 1000, reads: 16 }
    }
}

/// Counters of a mark (cumulative over calls).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct MarkStats {
    /// Ids marked.
    pub marked: u64,
    /// Commits expanded from a record, and from their object.
    pub commits_recorded: u64,
    pub commits_read: u64,
    /// Trees read.
    pub trees: u64,
    /// Objects read (all kinds) and object reads (calls).
    pub objects_read: u64,
    pub reads: u64,
    /// Record lookups ([`RecordSource::records`] calls).
    pub lookups: u64,
    /// Ids found missing while counting.
    pub missing: u64,
}

/// Child ids of an encoded tree body, with their kinds. Parsed without the
/// canonical-order checks of `Tree::decode`: the collector must see the
/// children of whatever is stored. Gitlinks (other repositories) are
/// skipped; legacy mode spellings ("040000", "40755", ...) behave like their
/// class, and an unparsable mode counts as a blob so its target stays.
pub(crate) fn tree_children(body: &[u8], out: &mut Vec<(ObjectId, Kind)>) -> Result<()> {
    let malformed = || StoreError::InvalidArgument("gc: malformed tree object".into());
    let mut rest = body;
    while !rest.is_empty() {
        let sp = rest.iter().position(|&b| b == b' ').ok_or_else(malformed)?;
        let mode = &rest[..sp];
        rest = &rest[sp + 1..];
        let nul = rest.iter().position(|&b| b == 0).ok_or_else(malformed)?;
        rest = &rest[nul + 1..];
        if rest.len() < ObjectId::LEN {
            return Err(malformed());
        }
        let id = ObjectId::from_bytes(&rest[..ObjectId::LEN])?;
        rest = &rest[ObjectId::LEN..];
        match EntryMode::from_octal(mode).map(EntryMode::class) {
            Ok(EntryMode::Tree) => out.push((id, Kind::Tree)),
            Ok(EntryMode::Gitlink) => {}
            _ => out.push((id, Kind::Blob)),
        }
    }
    Ok(())
}

/// Tree and parent ids of an encoded commit body (header lines only).
pub(crate) fn commit_children(body: &[u8], out: &mut Vec<(ObjectId, Kind)>) -> Result<()> {
    let malformed = || StoreError::InvalidArgument("gc: malformed commit object".into());
    for line in body.split(|&b| b == b'\n') {
        if line.is_empty() {
            break;
        }
        let (kind, hex) = if let Some(hex) = line.strip_prefix(b"tree ") {
            (Kind::Tree, hex)
        } else if let Some(hex) = line.strip_prefix(b"parent ") {
            (Kind::Commit, hex)
        } else {
            continue;
        };
        let hex = std::str::from_utf8(hex).map_err(|_| malformed())?;
        out.push((hex.parse::<ObjectId>().map_err(|_| malformed())?, kind));
    }
    Ok(())
}

/// What reading one part of ids found.
#[derive(Default)]
struct Parsed {
    /// Children of the trees and commits read, in order.
    children: Vec<(ObjectId, Kind)>,
    /// Ids not in the store.
    missing: Vec<ObjectId>,
    trees: u64,
    commits: u64,
    objects: u64,
}

/// Parse the objects read for `ids` (index-aligned).
fn parse(ids: &[ObjectId], found: Vec<Option<EncodedObject>>) -> Result<Parsed> {
    let mut p = Parsed::default();
    for (id, object) in ids.iter().zip(found) {
        let Some(object) = object else {
            p.missing.push(*id);
            continue;
        };
        p.objects += 1;
        match object.kind() {
            ObjectKind::Tree => {
                p.trees += 1;
                tree_children(object.body(), &mut p.children)
            }
            ObjectKind::Commit => {
                p.commits += 1;
                commit_children(object.body(), &mut p.children)
            }
            ObjectKind::Blob => Ok(()),
        }
        .map_err(|e| StoreError::InvalidArgument(format!("{e} ({id}); nothing was deleted")))?;
    }
    Ok(p)
}

/// The mark: the ids marked so far, and how to expand more.
pub(crate) struct Marker<'a> {
    objects: &'a dyn ObjectStore,
    records: CommitRecords,
    source: &'a dyn RecordSource,
    opts: MarkOptions,
    /// Every id marked.
    pub live: IdSet,
    pub stats: MarkStats,
}

impl<'a> Marker<'a> {
    pub fn new(
        objects: &'a dyn ObjectStore,
        records: CommitRecords,
        source: &'a dyn RecordSource,
        opts: MarkOptions,
    ) -> Self {
        Self {
            objects,
            records,
            source,
            opts: MarkOptions { batch: opts.batch.max(1), reads: opts.reads.max(1) },
            live: IdSet::default(),
            stats: MarkStats::default(),
        }
    }

    /// Mark `roots` and everything they reach that is not marked yet.
    /// `admitted` sees every newly marked id before it is read (the local
    /// collector takes them back from its condemned set there).
    /// `count_missing`: ids reached but not in the store are counted
    /// ([`MarkStats::missing`]); otherwise they are skipped silently (a
    /// writer may still be storing them).
    pub async fn mark(
        &mut self,
        roots: impl IntoIterator<Item = (ObjectId, Kind)>,
        count_missing: bool,
        admitted: &mut (dyn FnMut(&[ObjectId]) + Send),
    ) -> Result<()> {
        // Marked, not expanded yet: commits and unknown ids, then trees.
        let mut commits: Vec<ObjectId> = Vec::new();
        let mut trees: Vec<ObjectId> = Vec::new();
        let mut new: Vec<ObjectId> = Vec::new();
        self.admit(roots, &mut commits, &mut trees, &mut new);
        admitted(&new);
        // Root trees of commits, with their walk order.
        let mut ordered: Vec<((u64, i64), ObjectId)> = Vec::new();
        loop {
            if !commits.is_empty() {
                self.expand_commits(
                    std::mem::take(&mut commits),
                    count_missing,
                    &mut ordered,
                    &mut trees,
                    admitted,
                )
                .await?;
                continue;
            }
            if !ordered.is_empty() {
                // Newest first; then trees reached otherwise.
                ordered.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
                let mut level: Vec<ObjectId> = ordered.drain(..).map(|(_, t)| t).collect();
                level.append(&mut trees);
                trees = level;
            }
            if trees.is_empty() {
                return Ok(());
            }
            let level = std::mem::take(&mut trees);
            self.expand_level(&level, count_missing, &mut commits, &mut trees, admitted).await?;
        }
    }

    /// Mark the unmarked ones of `ids`, queueing commits and unknown ids
    /// into `commits` and trees into `trees`; `new` gets every new id.
    fn admit(
        &mut self,
        ids: impl IntoIterator<Item = (ObjectId, Kind)>,
        commits: &mut Vec<ObjectId>,
        trees: &mut Vec<ObjectId>,
        new: &mut Vec<ObjectId>,
    ) {
        new.clear();
        for (id, kind) in ids {
            if !self.live.insert(id) {
                continue;
            }
            new.push(id);
            match kind {
                Kind::Unknown | Kind::Commit => commits.push(id),
                Kind::Tree => trees.push(id),
                Kind::Blob => {}
            }
        }
        self.stats.marked += new.len() as u64;
    }

    /// Expand marked commits (and unknown ids): their records' trees go to
    /// `ordered`, their parents are marked; ids without a record are looked
    /// up, then read.
    async fn expand_commits(
        &mut self,
        mut todo: Vec<ObjectId>,
        count_missing: bool,
        ordered: &mut Vec<((u64, i64), ObjectId)>,
        trees: &mut Vec<ObjectId>,
        admitted: &mut (dyn FnMut(&[ObjectId]) + Send),
    ) -> Result<()> {
        let mut new = Vec::new();
        let mut parents: Vec<(ObjectId, Kind)> = Vec::new();
        while !todo.is_empty() {
            let mut unrecorded: Vec<ObjectId> = Vec::new();
            parents.clear();
            for id in todo.drain(..) {
                match self.records.get(&id) {
                    Some((tree, ps, order)) => {
                        self.stats.commits_recorded += 1;
                        if self.live.insert(tree) {
                            self.stats.marked += 1;
                            new.push(tree);
                            ordered.push((order, tree));
                        }
                        parents.extend(ps.iter().map(|p| (*p, Kind::Commit)));
                    }
                    None => unrecorded.push(id),
                }
            }
            if !new.is_empty() {
                admitted(&new);
                new.clear();
            }
            let mut queued = Vec::new();
            self.admit(parents.drain(..), &mut queued, trees, &mut new);
            admitted(&new);
            new.clear();
            todo = queued;
            if unrecorded.is_empty() {
                continue;
            }
            // Records of commits the scan did not see.
            for part in unrecorded.chunks(self.opts.batch) {
                self.stats.lookups += 1;
                for (id, r) in self.source.records(part).await? {
                    self.records.insert(id, &r);
                }
            }
            let (recorded, read): (Vec<ObjectId>, Vec<ObjectId>) =
                unrecorded.into_iter().partition(|id| self.records.get(id).is_some());
            todo.extend(recorded);
            if read.is_empty() {
                continue;
            }
            let mut children = Vec::new();
            for part in read.chunks(self.opts.batch) {
                self.stats.reads += 1;
                let found = self.objects.get_many(part).await?;
                let p = parse(part, found)?;
                self.stats.objects_read += p.objects;
                self.stats.commits_read += p.commits;
                self.stats.trees += p.trees;
                if count_missing {
                    self.count_missing(&p.missing);
                }
                children.extend(p.children);
            }
            // Children of raw commits (trees go straight to the next tree
            // level, unordered) and of trees reached as unknown ids.
            let mut queued = Vec::new();
            self.admit(children, &mut queued, trees, &mut new);
            admitted(&new);
            new.clear();
            todo.extend(queued);
        }
        Ok(())
    }

    fn count_missing(&mut self, ids: &[ObjectId]) {
        for id in ids {
            self.stats.missing += 1;
            tracing::debug!(%id, "gc: reachable object missing");
        }
    }

    /// Read one level of marked trees (in order) and mark their children:
    /// trees into `next` (in order), commits and unknown ids into `commits`.
    async fn expand_level(
        &mut self,
        level: &[ObjectId],
        count_missing: bool,
        commits: &mut Vec<ObjectId>,
        next: &mut Vec<ObjectId>,
        admitted: &mut (dyn FnMut(&[ObjectId]) + Send),
    ) -> Result<()> {
        let objects = self.objects;
        // Owned parts: the future stays `Send` (no borrowed closure
        // arguments).
        let parts: Vec<Vec<ObjectId>> = level.chunks(self.opts.batch).map(<[_]>::to_vec).collect();
        let mut reads = stream::iter(parts)
            .map(|part| async move {
                let found = objects.get_many(&part).await?;
                tokio::task::spawn_blocking(move || parse(&part, found))
                    .await
                    .map_err(|e| StoreError::backend(std::io::Error::other(e.to_string())))?
            })
            .buffered(self.opts.reads);
        let mut new = Vec::new();
        while let Some(p) = reads.next().await {
            let p = p?;
            self.stats.reads += 1;
            self.stats.objects_read += p.objects;
            self.stats.trees += p.trees;
            self.stats.commits_read += p.commits;
            if count_missing {
                self.count_missing(&p.missing);
            }
            self.admit(p.children, commits, next, &mut new);
            admitted(&new);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectStoreExt;
    use crate::memory::MemoryObjectStore;
    use ant_core::{Commit, Object, Signature, Tree, TreeEntry};
    use proptest::prelude::*;

    /// The reference: a plain closure, one id at a time.
    async fn reference(
        objects: &dyn ObjectStore,
        records: &HashMap<ObjectId, Record>,
        roots: &[(ObjectId, Kind)],
    ) -> (HashSet<ObjectId>, u64) {
        let mut live = HashSet::new();
        let mut missing = 0;
        // Every root is marked as what it is given as (a missing root given
        // as unknown is counted even if a tree also names it as a blob).
        let mut todo: Vec<(ObjectId, Kind)> =
            roots.iter().copied().filter(|(id, _)| live.insert(*id)).collect();
        while let Some((id, kind)) = todo.pop() {
            if kind == Kind::Blob {
                continue;
            }
            if matches!(kind, Kind::Commit | Kind::Unknown)
                && let Some(r) = records.get(&id)
            {
                let children = std::iter::once((r.tree, Kind::Tree))
                    .chain(r.parents.iter().map(|p| (*p, Kind::Commit)));
                todo.extend(children.filter(|(id, _)| live.insert(*id)));
                continue;
            }
            match objects.get(&id).await.unwrap() {
                None => missing += 1,
                Some(o) => {
                    let mut out = Vec::new();
                    match o.kind() {
                        ObjectKind::Tree => tree_children(o.body(), &mut out).unwrap(),
                        ObjectKind::Commit => commit_children(o.body(), &mut out).unwrap(),
                        ObjectKind::Blob => {}
                    }
                    todo.extend(out.into_iter().filter(|(id, _)| live.insert(*id)));
                }
            }
        }
        (live, missing)
    }

    fn entry(name: String, mode: EntryMode, id: ObjectId) -> TreeEntry {
        TreeEntry { mode, name, id }
    }

    fn sig(t: i64) -> Signature {
        Signature { name: b"a".to_vec(), email: b"a@x".to_vec(), time: t, tz_offset_minutes: 0 }
    }

    /// A random history (see the property test).
    struct History {
        objects: MemoryObjectStore,
        records: HashMap<ObjectId, Record>,
        commits: Vec<ObjectId>,
        all: Vec<ObjectId>,
    }

    /// One commit per op `(dir, file, flags)`: it changes one of 16 files in
    /// 4 directories, so unchanged subtrees are shared with its parent.
    /// Flags: 0x01 a merge (second parent from bits 2..); 0x02 the commit
    /// object is not stored (only its record); 0x04 no record (only the
    /// object); 0x08 a symlink entry; 0x10 the last file's blob is not
    /// stored; 0x20 a gitlink in the first directory; 0x40 a tree entry
    /// naming a blob id (a tree that is "missing" as a tree).
    async fn history(ops: &[(u8, u8, u8)]) -> History {
        let objects = MemoryObjectStore::default();
        let mut records = HashMap::new();
        let mut commits: Vec<ObjectId> = Vec::new();
        let mut all = Vec::new();
        let mut versions = [[0u8; 4]; 4];
        let mut put = Vec::new();
        for (n, &(dir, file, flags)) in ops.iter().enumerate() {
            let (d, f) = (dir as usize % 4, file as usize % 4);
            versions[d][f] = versions[d][f].wrapping_add(1);
            let mut root = Vec::new();
            for (d, files) in versions.iter().enumerate() {
                let mut entries = Vec::new();
                for (f, v) in files.iter().enumerate() {
                    let blob =
                        EncodedObject::new(ObjectKind::Blob, format!("d{d}f{f}v{v}").as_bytes());
                    all.push(blob.id());
                    if !(flags & 0x10 != 0 && f == 3) {
                        put.push(blob.clone());
                    }
                    let mode = if flags & 0x08 != 0 && f == 2 {
                        EntryMode::Symlink
                    } else {
                        EntryMode::File
                    };
                    entries.push(entry(format!("f{f}"), mode, blob.id()));
                }
                if d == 0 && flags & 0x20 != 0 {
                    entries.push(entry("m".into(), EntryMode::Gitlink, ObjectId([7; 32])));
                }
                if d == 1 && flags & 0x40 != 0 {
                    let blob = EncodedObject::new(ObjectKind::Blob, b"not a tree");
                    put.push(blob.clone());
                    all.push(blob.id());
                    entries.push(entry("t".into(), EntryMode::Tree, blob.id()));
                }
                let tree = Object::Tree(Tree::new(entries).unwrap()).encode();
                all.push(tree.id());
                put.push(tree.clone());
                root.push(entry(format!("d{d}"), EntryMode::Tree, tree.id()));
            }
            let root = Object::Tree(Tree::new(root).unwrap()).encode();
            all.push(root.id());
            put.push(root.clone());
            let mut parents: Vec<ObjectId> = commits.last().copied().into_iter().collect();
            if flags & 0x01 != 0 && commits.len() > 2 {
                parents.push(commits[(flags as usize >> 2) % (commits.len() - 1)]);
            }
            let commit = Object::Commit(Commit {
                tree: root.id(),
                parents: parents.clone(),
                author: sig(n as i64),
                committer: sig(n as i64),
                extra_headers: Vec::new(),
                message: format!("c{n}\n").into_bytes(),
            })
            .encode();
            all.push(commit.id());
            if flags & 0x02 == 0 {
                put.push(commit.clone());
            }
            if flags & 0x04 == 0 || flags & 0x02 != 0 {
                let r = Record {
                    tree: root.id(),
                    parents,
                    generation: n as u64 + 1,
                    committer_time: n as i64,
                };
                records.insert(commit.id(), r);
            }
            commits.push(commit.id());
        }
        objects.put_many(&put).await.unwrap();
        History { objects, records, commits, all }
    }

    /// No records beyond those in memory.
    struct NoRecords;

    #[async_trait]
    impl RecordSource for NoRecords {
        async fn records(&self, _ids: &[ObjectId]) -> Result<Vec<(ObjectId, Record)>> {
            Ok(Vec::new())
        }
    }

    struct Map(HashMap<ObjectId, Record>);

    #[async_trait]
    impl RecordSource for Map {
        async fn records(&self, ids: &[ObjectId]) -> Result<Vec<(ObjectId, Record)>> {
            Ok(ids.iter().filter_map(|id| self.0.get(id).map(|r| (*id, r.clone()))).collect())
        }
    }

    fn kind(k: u8) -> Kind {
        if k == 0 { Kind::Unknown } else { Kind::Commit }
    }

    /// `default_cases` unless `PROPTEST_CASES` says otherwise.
    fn config(default_cases: u32) -> ProptestConfig {
        let mut c = ProptestConfig::default();
        if std::env::var("PROPTEST_CASES").is_err() {
            c.cases = default_cases;
        }
        c.failure_persistence = None;
        c
    }

    proptest! {
        #![proptest_config(config(64))]

        /// The marker reaches exactly what a plain one-id-at-a-time walk
        /// reaches and counts the same missing ids, whatever part of the
        /// records it holds in memory and however it splits its reads; it
        /// reports every new id exactly once; and marking the roots one at a
        /// time (later marks of a run) gives the same set.
        #[test]
        fn mark_equals_the_reference_walk(
            ops in proptest::collection::vec((any::<u8>(), any::<u8>(), any::<u8>()), 1..40),
            roots in proptest::collection::vec((any::<u16>(), 0u8..5), 1..6),
            preload in 0u8..3,
            batch in 1usize..8,
            reads in 1usize..4,
        ) {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let h = history(&ops).await;
                // As the collectors' roots: commits as commits or unknown
                // ids, any object (or an id nothing stores) as unknown. (An
                // id claimed under a kind it does not have is followed by
                // whichever claim comes first, so only consistent claims
                // make the result independent of the order of the walk.)
                let roots: Vec<(ObjectId, Kind)> = roots
                    .iter()
                    .map(|&(i, k)| match k {
                        0 | 1 => (h.commits[i as usize % h.commits.len()], kind(k)),
                        2 => (ObjectId([i as u8; 32]), kind(i as u8 % 2)),
                        _ => (h.all[i as usize % h.all.len()], Kind::Unknown),
                    })
                    .collect();
                let (want, want_missing) = reference(&h.objects, &h.records, &roots).await;
                let mut memory = CommitRecords::default();
                for (n, (id, r)) in h.records.iter().enumerate() {
                    // 0: none in memory; 1: about half; 2: all.
                    if preload == 2 || (preload == 1 && n % 2 == 0) {
                        memory.insert(*id, r);
                    }
                }
                let source = Map(h.records.clone());
                let opts = MarkOptions { batch, reads };
                let mut m = Marker::new(&h.objects, memory, &source, opts);
                let mut seen: Vec<ObjectId> = Vec::new();
                m.mark(roots.clone(), true, &mut |ids| seen.extend_from_slice(ids)).await.unwrap();
                let got: HashSet<ObjectId> = m.live.iter().copied().collect();
                prop_assert_eq!(&got, &want);
                prop_assert_eq!(m.stats.missing, want_missing);
                prop_assert_eq!(seen.len(), want.len());
                prop_assert_eq!(&seen.iter().copied().collect::<HashSet<_>>(), &want);
                prop_assert_eq!(m.stats.marked, want.len() as u64);

                let mut step = Marker::new(&h.objects, CommitRecords::default(), &source, opts);
                for r in &roots {
                    step.mark([*r], false, &mut |_| {}).await.unwrap();
                }
                let got: HashSet<ObjectId> = step.live.iter().copied().collect();
                prop_assert_eq!(got, want);
                Ok(())
            })?;
        }
    }

    /// Ids a mark reaches stay readable in the store it walks: a malformed
    /// tree stops the mark (nothing is deleted on a guess).
    #[tokio::test]
    async fn malformed_trees_stop_the_mark() {
        let objects = MemoryObjectStore::default();
        let bad = EncodedObject::new(ObjectKind::Tree, b"100644 a\0short");
        objects.put(bad.clone()).await.unwrap();
        let mut m =
            Marker::new(&objects, CommitRecords::default(), &NoRecords, MarkOptions::default());
        let e = m.mark([(bad.id(), Kind::Unknown)], true, &mut |_| {}).await.unwrap_err();
        assert!(e.to_string().contains("nothing was deleted"), "{e}");
    }

    /// Legacy mode spellings must not hide subtrees from the collector.
    #[test]
    fn tree_children_classify_legacy_modes() {
        let m = |s: &str| EntryMode::from_octal(s.as_bytes()).unwrap();
        let t = Tree::new(vec![
            TreeEntry { mode: m("100664"), name: "a".into(), id: ObjectId([1; 32]) },
            TreeEntry { mode: m("040000"), name: "d".into(), id: ObjectId([2; 32]) },
            TreeEntry { mode: m("40755"), name: "e".into(), id: ObjectId([3; 32]) },
            TreeEntry { mode: m("160755"), name: "g".into(), id: ObjectId([4; 32]) },
        ])
        .unwrap();
        let o = Object::Tree(t).encode();
        let mut out = Vec::new();
        tree_children(o.body(), &mut out).unwrap();
        out.sort_by_key(|(id, _)| *id);
        assert_eq!(
            out,
            [
                (ObjectId([1; 32]), Kind::Blob),
                (ObjectId([2; 32]), Kind::Tree),
                (ObjectId([3; 32]), Kind::Tree)
            ]
        );
    }

    #[test]
    fn tree_children_follow_subtrees_and_blobs_but_not_gitlinks() {
        let t = Tree::new(vec![
            TreeEntry { mode: EntryMode::File, name: "a".into(), id: ObjectId([1; 32]) },
            TreeEntry { mode: EntryMode::Tree, name: "d".into(), id: ObjectId([2; 32]) },
            TreeEntry { mode: EntryMode::Gitlink, name: "m".into(), id: ObjectId([3; 32]) },
            TreeEntry { mode: EntryMode::Symlink, name: "s".into(), id: ObjectId([4; 32]) },
        ])
        .unwrap();
        let o = Object::Tree(t).encode();
        let mut out = Vec::new();
        tree_children(o.body(), &mut out).unwrap();
        out.sort_by_key(|(id, _)| *id);
        assert_eq!(
            out,
            [
                (ObjectId([1; 32]), Kind::Blob),
                (ObjectId([2; 32]), Kind::Tree),
                (ObjectId([4; 32]), Kind::Blob)
            ]
        );
        assert!(tree_children(b"100644 a\0short", &mut out).is_err());
        assert!(tree_children(b"100644 a", &mut out).is_err());
    }

    #[test]
    fn commit_children_are_tree_and_parents() {
        let body = format!(
            "tree {}\nparent {}\nparent {}\nauthor a <a> 1 +0000\n\ntree {}\n",
            ObjectId([1; 32]).to_hex(),
            ObjectId([2; 32]).to_hex(),
            ObjectId([3; 32]).to_hex(),
            ObjectId([9; 32]).to_hex(),
        );
        let mut out = Vec::new();
        commit_children(body.as_bytes(), &mut out).unwrap();
        assert_eq!(
            out,
            [
                (ObjectId([1; 32]), Kind::Tree),
                (ObjectId([2; 32]), Kind::Commit),
                (ObjectId([3; 32]), Kind::Commit)
            ]
        );
        assert!(commit_children(b"tree xyz\n\n", &mut out).is_err());
    }
}
