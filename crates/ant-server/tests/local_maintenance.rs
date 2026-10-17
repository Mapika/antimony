//! Garbage collection and repacking of a local data directory
//! (`ant_store::local::gc`, `ant_store::local::repack`) while it serves,
//! against the real write paths: what a collection deletes, that everything
//! reachable survives (full read-back), the grace period, dry runs, readers
//! hiding condemned data and finding it again once written or referenced,
//! pack compaction, repacked histories read back byte for byte,
//! interrupted and resumed repacks, cancelled runs and restarts, the
//! maintenance socket, and writers, readers, collections and repacks
//! racing.
//!
//! Local stores need no services: these run in the fast tier. Phases are
//! bounded by work, not by wall-clock time, so slow runners do not make
//! them grow (see `writer_pause`).

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use ant_core::{EncodedObject, EntryMode, Object, ObjectId, ObjectKind, RepoPath, Signature};
use ant_server::ops::changeset::{ChangesetRequest, FileOp, WriteContent, create_changeset};
use ant_server::ops::land::{LandOutcome, land, rebase};
use ant_server::ops::{OpError, history, query};
use ant_store::Backend;
use ant_store::local::gc::{GcOptions, GcReport, collect};
use ant_store::local::repack::{RepackOptions, RepackReport, repack};
use ant_store::local::{LocalOptions, LocalStore};
use ant_store::packed::ClusterOptions;
use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Objects larger than this go to pack files, so both tiers get history.
const INLINE: usize = 200;

struct Repo {
    store: LocalStore,
    b: Backend,
}

fn options() -> LocalOptions {
    LocalOptions { inline_threshold: INLINE, ..LocalOptions::default() }
}

fn repo() -> Repo {
    // Collections here wait little or not at all between condemning and
    // deleting, so the server caches must forget at once too.
    ant_server::ops::set_cache_max_age(Duration::ZERO);
    let store = LocalStore::open_scratch_with("maint", &options()).unwrap();
    let b = store.backend();
    Repo { store, b }
}

impl Repo {
    /// A backend with nothing cached (a server started afresh).
    fn fresh(&self) -> Backend {
        self.store.backend()
    }

    async fn gc(&self, o: &GcOptions) -> GcReport {
        collect(&self.store, o, &|_| {}).await.unwrap()
    }

    async fn repack(&self, o: &RepackOptions) -> RepackReport {
        repack(&self.store, o, &|_| {}).await.unwrap()
    }
}

/// A connection of its own to the store's SQLite database.
fn db(dir: &Path) -> rusqlite::Connection {
    let c = rusqlite::Connection::open(dir.join("meta.db")).unwrap();
    c.busy_timeout(Duration::from_secs(30)).unwrap();
    c
}

async fn sql(dir: &Path, stmt: &str) {
    let (dir, stmt) = (dir.to_owned(), stmt.to_owned());
    tokio::task::spawn_blocking(move || db(&dir).execute_batch(&stmt).unwrap()).await.unwrap();
}

async fn count(dir: &Path, q: &str) -> i64 {
    let (dir, q) = (dir.to_owned(), q.to_owned());
    tokio::task::spawn_blocking(move || db(&dir).query_row(&q, [], |r| r.get(0)).unwrap())
        .await
        .unwrap()
}

/// Pretend everything stored so far was written `days` ago.
async fn age(dir: &Path, days: u32) {
    let secs = i64::from(days) * 86_400;
    for t in ["objects", "object_index", "commits"] {
        sql(dir, &format!("UPDATE {t} SET written_at = written_at - {secs}")).await;
    }
}

fn lit(id: &ObjectId) -> String {
    format!("x'{}'", id.to_hex())
}

/// Rows anywhere (small objects, packed index, commit records) for `id`.
async fn stored(dir: &Path, id: &ObjectId) -> i64 {
    let l = lit(id);
    count(
        dir,
        &format!(
            "SELECT (SELECT count(*) FROM objects WHERE id = {l}) \
                  + (SELECT count(*) FROM object_index WHERE id = {l}) \
                  + (SELECT count(*) FROM commits WHERE id = {l})"
        ),
    )
    .await
}

/// Pack files on disk.
fn pack_files(dir: &Path) -> HashSet<String> {
    std::fs::read_dir(dir.join("packs"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".pack"))
        .collect()
}

/// Packs index rows point into.
async fn indexed_packs(dir: &Path) -> HashSet<String> {
    let dir = dir.to_owned();
    tokio::task::spawn_blocking(move || {
        let c = db(&dir);
        let mut st = c.prepare("SELECT DISTINCT lower(hex(pack)) FROM object_index").unwrap();
        st.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|n| format!("{}.pack", n.unwrap()))
            .collect()
    })
    .await
    .unwrap()
}

fn sig() -> Signature {
    Signature { name: "lm".into(), email: "lm@x".into(), time: 1_700_000_000, tz_offset_minutes: 0 }
}

fn write(path: &str, content: &[u8]) -> FileOp {
    FileOp::Write {
        path: RepoPath::new(path).unwrap(),
        mode: EntryMode::File,
        content: WriteContent::Inline(Bytes::copy_from_slice(content)),
    }
}

async fn commit_ops(b: &Backend, base: Option<ObjectId>, ops: Vec<FileOp>) -> ObjectId {
    let req = ChangesetRequest { base, ops, author: sig(), committer: None, message: "m".into() };
    create_changeset(b, req).await.unwrap()
}

async fn changeset(b: &Backend, base: Option<ObjectId>, files: &[(&str, &[u8])]) -> ObjectId {
    commit_ops(b, base, files.iter().map(|(p, c)| write(p, c)).collect()).await
}

async fn land_tip(b: &Backend, r: &str, c: ObjectId) -> ObjectId {
    match land(b, r, c).await.unwrap() {
        LandOutcome::Landed { new_tip, .. } => new_tip,
        other => panic!("{other:?}"),
    }
}

/// `land`, retried while it is `Aborted` (as clients do).
async fn land_retrying(b: &Backend, r: &str, c: ObjectId) -> Result<LandOutcome, OpError> {
    for _ in 0..50 {
        match land(b, r, c).await {
            Err(OpError::Aborted(_)) => tokio::time::sleep(Duration::from_millis(10)).await,
            other => return other,
        }
    }
    land(b, r, c).await
}

/// Every object reachable from every ref, with its bytes, after checking
/// that each commit's record and changeset are readable too. Panics on
/// anything missing.
async fn readback(b: &Backend) -> BTreeMap<ObjectId, Bytes> {
    let mut seen: BTreeMap<ObjectId, Bytes> = BTreeMap::new();
    let mut frontier: Vec<ObjectId> =
        b.refs.list_refs("").await.unwrap().into_iter().map(|(_, id)| id).collect();
    while !frontier.is_empty() {
        frontier.retain(|id| !seen.contains_key(id));
        frontier.sort();
        frontier.dedup();
        let got = b.objects.get_many(&frontier).await.unwrap();
        let mut next = Vec::new();
        let mut commits = Vec::new();
        for (id, o) in frontier.iter().zip(got) {
            let o = o.unwrap_or_else(|| panic!("reachable object {id} is missing"));
            match o.decode().unwrap() {
                Object::Commit(c) => {
                    next.push(c.tree);
                    next.extend(c.parents.iter().copied());
                    commits.push(*id);
                }
                Object::Tree(t) => next.extend(
                    t.entries().iter().filter(|e| e.mode != EntryMode::Gitlink).map(|e| e.id),
                ),
                Object::Blob(_) => {}
            }
            seen.insert(*id, o.into_bytes());
        }
        for (id, node) in commits.iter().zip(b.meta.get_commits(&commits).await.unwrap()) {
            assert!(node.is_some(), "commit record of {id} is missing");
        }
        for (id, cs) in commits.iter().zip(b.meta.get_changesets(&commits).await.unwrap()) {
            assert!(cs.is_some(), "changeset of {id} is missing");
        }
        frontier = next;
    }
    seen
}

fn gc_opts() -> GcOptions {
    GcOptions { quiesce: Duration::ZERO, batch: 7, ..GcOptions::default() }
}

/// History on `main` (nested paths, a rebased land with its landed-as
/// mapping), another ref, and the history index. Returns main's tip and the
/// rebased land's source commit.
async fn history(b: &Backend, n: usize) -> (ObjectId, ObjectId) {
    let mut tip = land_tip(b, "main", changeset(b, None, &[("README", b"hello\n")]).await).await;
    for i in 0..n {
        let path = format!("src/m{}/f{i}.rs", i % 3);
        let c = changeset(b, Some(tip), &[(&path, format!("fn f{i}() {{}}\n").as_bytes())]).await;
        tip = land_tip(b, "main", c).await;
    }
    let a = changeset(b, Some(tip), &[("docs/a.md", b"a\n")]).await;
    let source = changeset(b, Some(tip), &[("docs/b.md", b"b\n")]).await;
    land_tip(b, "main", a).await;
    tip = land_tip(b, "main", source).await;
    assert_ne!(tip, source);
    b.refs.cas_ref("release", None, Some(tip)).await.unwrap();
    history::catch_up(b, "main").await.unwrap();
    (tip, source)
}

/// A text of `lines` lines for `path`, with the lines in `edits` changed.
fn text(path: &str, lines: usize, edits: &[(usize, u32)]) -> Vec<u8> {
    let mut out = String::new();
    for line in 0..lines {
        let edit = edits.iter().rev().find(|(l, _)| *l == line).map(|(_, v)| *v);
        match edit {
            Some(v) => out.push_str(&format!("{path} line {line}: edited in version {v}\n")),
            None => out.push_str(&format!("{path} line {line}: the original text here\n")),
        }
    }
    out.into_bytes()
}

/// A random history on `main` (and a side ref): files in nested
/// directories, edited a few lines at a time, sometimes rewritten or
/// deleted, small and large.
async fn random_history(b: &Backend, rng: &mut StdRng, commits: usize) -> Vec<ObjectId> {
    let paths: Vec<String> = (0..12)
        .map(|i| match i % 4 {
            0 => format!("f{i}.txt"),
            1 => format!("src/m{}/f{i}.rs", i % 3),
            2 => format!("docs/d{i}.md"),
            _ => format!("src/m{}/deep/x{i}.c", i % 2),
        })
        .collect();
    let mut edits: Vec<Vec<(usize, u32)>> = vec![Vec::new(); paths.len()];
    let mut exists = vec![false; paths.len()];
    let lines: Vec<usize> = (0..paths.len()).map(|i| [3, 40, 120][i % 3]).collect();
    let mut tip = None;
    let mut out = Vec::new();
    for v in 0..commits as u32 {
        let mut ops = Vec::new();
        for _ in 0..rng.random_range(1..4) {
            let p = rng.random_range(0..paths.len());
            if ops.iter().any(|o: &FileOp| o.path().as_str() == paths[p]) {
                continue;
            }
            let delete = rng.random_range(0..10) == 0;
            exists[p] = !(delete && exists[p]);
            match rng.random_range(0..10) {
                _ if delete && !exists[p] => {
                    ops.push(FileOp::Delete { path: RepoPath::new(&paths[p]).unwrap() });
                    edits[p].clear();
                }
                1 => {
                    edits[p] = (0..lines[p]).map(|l| (l, v)).collect();
                    ops.push(write(&paths[p], &text(&paths[p], lines[p], &edits[p])));
                }
                _ => {
                    edits[p].push((rng.random_range(0..lines[p]), v));
                    ops.push(write(&paths[p], &text(&paths[p], lines[p], &edits[p])));
                }
            }
        }
        if ops.is_empty() {
            continue;
        }
        let c = commit_ops(b, tip, ops).await;
        match land(b, "main", c).await {
            Ok(LandOutcome::Landed { new_tip, .. }) => {
                tip = Some(new_tip);
                out.push(new_tip);
            }
            Err(OpError::InvalidArgument(_)) | Err(OpError::NotFound(_)) => {}
            other => panic!("{other:?}"),
        }
    }
    if out.len() > 2 {
        let side = out[out.len() / 2];
        b.refs.cas_ref("side", None, Some(side)).await.unwrap();
    }
    history::catch_up(b, "main").await.unwrap();
    out
}

fn repack_opts() -> RepackOptions {
    let mut o =
        RepackOptions { min_age: Duration::ZERO, keep_recent: 1, ..RepackOptions::default() };
    o.cluster.level = 3;
    o
}

/// Packed rows not in a pack a repack wrote.
async fn unrepacked(dir: &Path) -> i64 {
    count(
        dir,
        "SELECT count(*) FROM object_index x WHERE NOT EXISTS \
         (SELECT 1 FROM packs p WHERE p.pack = x.pack)",
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_gc_collects_aborted_and_abandoned_data_and_keeps_all_history() {
    let r = repo();
    let (b, dir) = (&r.b, r.store.dir());
    let (tip, source) = history(b, 12).await;
    let mut garbage = Vec::new();
    for i in 0..4 {
        let c =
            changeset(b, Some(tip), &[(&format!("junk/j{i}"), format!("junk {i}\n").as_bytes())])
                .await;
        garbage.push(c);
    }
    // An aborted land batch: rebased and written, never published.
    let side = changeset(b, Some(tip), &[("side/s", b"side\n")]).await;
    let other = changeset(b, Some(tip), &[("side/o", b"other\n")]).await;
    let other_tip = land_tip(b, "ws/x/other", other).await;
    let Ok(rebased) = rebase(b, side, other_tip).await.unwrap() else { panic!("conflict") };
    garbage.extend([side, rebased]);
    b.refs.cas_ref("ws/x/other", Some(other_tip), None).await.unwrap();
    garbage.extend([other, other_tip]);
    // Uploads nothing referenced, small and large.
    let stray = EncodedObject::new(ObjectKind::Blob, b"uploaded, never used\n");
    let big_stray = EncodedObject::new(ObjectKind::Blob, &text("stray", 40, &[]));
    b.objects.put_many(&[stray.clone(), big_stray.clone()]).await.unwrap();
    garbage.extend([stray.id(), big_stray.id()]);
    for c in [side, other] {
        let tree = b.meta.get_commits(&[c]).await.unwrap()[0].clone().unwrap().tree;
        garbage.push(tree);
    }

    let before = readback(b).await;
    age(dir, 30).await;
    let report = r.gc(&gc_opts()).await;
    assert!(report.commits >= 7 && report.objects >= 14, "{report:?}");
    assert!(report.large_objects >= 1, "{report:?}");
    assert_eq!(report.kept, 0, "{report:?}");
    for id in &garbage {
        assert_eq!(stored(dir, id).await, 0, "garbage {id} survived");
    }
    assert_eq!(readback(b).await, before);
    assert_eq!(readback(&r.fresh()).await, before);
    // The landed-as mapping's source stays: retrying that land is a no-op.
    assert!(b.meta.get_commits(&[source]).await.unwrap()[0].is_some());
    assert_eq!(land_tip(b, "main", source).await, tip);
    let log = query::log(b, "main", Some(&RepoPath::new("src/m1").unwrap()), 0).await.unwrap();
    assert_eq!(log.len(), 4);
    history::blame(b, "main", &RepoPath::new("src/m0/f0.rs").unwrap()).await.unwrap();
    let c = changeset(b, Some(tip), &[("after/gc", b"x\n")]).await;
    land_tip(b, "main", c).await;
    assert!(matches!(land(b, "main", garbage[0]).await, Err(OpError::NotFound(_))));
}

#[tokio::test]
async fn local_gc_keeps_everything_written_within_the_grace_period() {
    let r = repo();
    let (b, dir) = (&r.b, r.store.dir());
    let (tip, _) = history(b, 3).await;
    let old = changeset(b, Some(tip), &[("old", b"old\n")]).await;
    age(dir, 3).await;
    let recent = changeset(b, Some(tip), &[("recent", b"recent\n")]).await;
    let report = r.gc(&gc_opts()).await;
    assert_eq!((report.condemned, report.commits), (0, 0), "{report:?}");
    assert!(stored(dir, &old).await > 0);
    let report = r.gc(&GcOptions { grace: Duration::from_secs(2 * 86400), ..gc_opts() }).await;
    assert_eq!(report.commits, 1, "{report:?}");
    assert_eq!(stored(dir, &old).await, 0);
    assert!(stored(dir, &recent).await > 0);
    readback(b).await;
}

/// Row counts and digests of every table a collection may touch, and the
/// pack files.
async fn fingerprint(dir: &Path) -> Vec<(String, i64, String)> {
    let mut out = Vec::new();
    for (t, key) in [
        ("objects", "id"),
        ("object_index", "id || pack || off"),
        ("commits", "id || written_at"),
        ("changesets", "commit_id"),
        ("derived", "key"),
        ("refs", "target"),
        ("history", "commit_id"),
        ("packs", "pack"),
    ] {
        let q = format!(
            "SELECT count(*) || ':' || coalesce(group_concat(hex({key}), ','), '') FROM (SELECT * FROM {t} ORDER BY {key})"
        );
        let (d, q2) = (dir.to_owned(), q.clone());
        let s: String =
            tokio::task::spawn_blocking(move || db(&d).query_row(&q2, [], |r| r.get(0)).unwrap())
                .await
                .unwrap();
        let (n, digest) = s.split_once(':').unwrap();
        out.push((t.to_owned(), n.parse().unwrap(), digest.to_owned()));
    }
    let mut files: Vec<String> = pack_files(dir).into_iter().collect();
    files.sort();
    out.push(("files".into(), files.len() as i64, files.join(",")));
    out
}

#[tokio::test]
async fn local_gc_dry_run_changes_nothing_and_predicts_the_run() {
    let r = repo();
    let (b, dir) = (&r.b, r.store.dir());
    let (tip, _) = history(b, 4).await;
    for i in 0..3 {
        changeset(b, Some(tip), &[(&format!("x/{i}"), format!("{i}\n").as_bytes())]).await;
    }
    age(dir, 30).await;
    let before = fingerprint(dir).await;
    let dry = r.gc(&GcOptions { dry_run: true, ..gc_opts() }).await;
    assert!(dry.dry_run);
    assert_eq!(fingerprint(dir).await, before);
    assert!(dry.condemned > 0 && dry.commits == 3, "{dry:?}");
    let run = r.gc(&gc_opts()).await;
    assert_eq!(
        (run.condemned, run.objects, run.object_bytes, run.commits, run.changesets),
        (dry.condemned, dry.objects, dry.object_bytes, dry.commits, dry.changesets)
    );
    assert_eq!(run.live, dry.live);
}

/// While a collection waits between condemning and deleting, readers see
/// condemned data as missing and everything reachable as present; storing
/// a condemned object again, recording a condemned commit again or pointing
/// a ref at one brings it back (read your writes), and the sweep keeps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_gc_hides_condemned_data_until_written_or_referenced_again() {
    let r = repo();
    let (b, dir) = (&r.b, r.store.dir());
    let (tip, _) = history(b, 4).await;
    let mut garbage = Vec::new();
    for i in 0..6 {
        garbage
            .push(changeset(b, Some(tip), &[(&format!("g/{i}"), &text("g", 10 + i, &[]))]).await);
    }
    let blob = EncodedObject::new(ObjectKind::Blob, &text("big garbage", 30, &[]));
    let small = EncodedObject::new(ObjectKind::Blob, b"small garbage\n");
    b.objects.put_many(&[blob.clone(), small.clone()]).await.unwrap();
    let before = readback(b).await;
    age(dir, 30).await;

    let slow = GcOptions { quiesce: Duration::from_secs(3), batch: 2, ..gc_opts() };
    let store = r.store.clone();
    let run = tokio::spawn(async move { collect(&store, &slow, &|_| {}).await.unwrap() });
    // Condemned: the old garbage reads as missing.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while b.meta.get_commits(&[garbage[0]]).await.unwrap()[0].is_some() {
        assert!(std::time::Instant::now() < deadline, "nothing condemned");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(b.objects.has_many(&[blob.id(), small.id()]).await.unwrap(), [false, false]);
    assert_eq!(b.objects.get_many(&[blob.id()]).await.unwrap(), vec![None]);
    assert!(b.meta.get_changesets(&[garbage[1]]).await.unwrap()[0].is_none());
    assert_eq!(readback(b).await, before);
    assert_eq!(readback(&r.fresh()).await, before);
    // Stored again: visible at once, and kept.
    b.objects.put_many(&[blob.clone(), small.clone()]).await.unwrap();
    assert_eq!(b.objects.has_many(&[blob.id(), small.id()]).await.unwrap(), [true, true]);
    // A ref moved to a condemned commit: the commit and its tree come back
    // (marked again), and are kept.
    let target = garbage[2];
    b.refs.cas_ref("pinned", None, Some(target)).await.unwrap();
    let report = run.await.unwrap();
    assert!(report.kept >= 2, "{report:?}");
    assert_eq!(b.objects.get_many(&[blob.id()]).await.unwrap(), vec![Some(blob.clone())]);
    assert!(stored(dir, &small.id()).await > 0);
    assert!(b.meta.get_commits(&[target]).await.unwrap()[0].is_some());
    let pinned = readback(b).await;
    assert!(pinned.contains_key(&target));
    // The others are gone.
    for c in [garbage[0], garbage[1], garbage[3]] {
        assert_eq!(stored(dir, &c).await, 0, "{c} survived");
    }
}

/// Versions of one file uploaded together share a multi-object frame:
/// when most of them are collected, the ones still referenced stay readable,
/// and once most of a pack is dead it is compacted (a new pack with the
/// live records, the old one deleted) while readers keep reading.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_gc_compacts_packs_and_keeps_live_frame_members_readable() {
    let r = repo();
    let (b, dir) = (&r.b, r.store.dir());
    let (tip, _) = history(b, 2).await;
    let body = |v: usize| {
        let mut s = String::new();
        for line in 0..200 {
            let edit = if line == v { format!("edited in version {v}") } else { "kept".into() };
            s.push_str(&format!("line {line} of a long file, {edit}\n"));
        }
        s.into_bytes()
    };
    let versions: Vec<EncodedObject> =
        (0..12).map(|v| EncodedObject::new(ObjectKind::Blob, &body(v))).collect();
    b.objects.put_many(&versions).await.unwrap();
    // Incompressible dead weight in other uploads.
    for i in 0..4 {
        let noise: Vec<u8> = (0..40_000).map(|_| rand::random::<u8>()).collect();
        let o = EncodedObject::new(ObjectKind::Blob, &[format!("{i}").as_bytes(), &noise].concat());
        b.objects.put_many(std::slice::from_ref(&o)).await.unwrap();
    }
    let keep = [versions[0].id(), versions[11].id()];
    let ops = keep
        .iter()
        .enumerate()
        .map(|(i, id)| FileOp::Write {
            path: RepoPath::new(format!("v/{i}")).unwrap(),
            mode: EntryMode::File,
            content: WriteContent::Blob(*id),
        })
        .collect();
    let req = ChangesetRequest {
        base: Some(tip),
        ops,
        author: sig(),
        committer: None,
        message: "versions".into(),
    };
    let c = create_changeset(b, req).await.unwrap();
    land_tip(b, "main", c).await;
    let before = readback(b).await;
    age(dir, 30).await;

    // Readers race the collection the whole time.
    let stop = Arc::new(AtomicBool::new(false));
    let mut readers = Vec::new();
    for _ in 0..2 {
        let (store, stop, before) = (r.store.clone(), stop.clone(), before.clone());
        readers.push(tokio::spawn(async move {
            let mut n = 0;
            while !stop.load(Ordering::Relaxed) || n == 0 {
                assert_eq!(readback(&store.backend()).await, before);
                n += 1;
            }
            n
        }));
    }
    let report = r.gc(&GcOptions { compact_ratio: 0.3, ..gc_opts() }).await;
    stop.store(true, Ordering::Relaxed);
    for t in readers {
        assert!(t.await.unwrap() > 0);
    }
    for v in &versions[1..11] {
        assert_eq!(stored(dir, &v.id()).await, 0, "unused version survived");
    }
    assert!(report.large_objects >= 14, "{report:?}");
    assert!(report.packs + report.packs_compacted > 0, "{report:?}");
    assert_eq!(readback(&r.fresh()).await, before);
    let got = r.fresh().objects.get_many(&keep).await.unwrap();
    assert_eq!(got, vec![Some(versions[0].clone()), Some(versions[11].clone())]);
    // Every pack file left is one rows point into (or the active one).
    let files = pack_files(dir);
    let indexed = indexed_packs(dir).await;
    assert!(indexed.is_subset(&files));
    assert!(files.len() <= indexed.len() + 1, "{files:?} {indexed:?}");
    // Nothing left to do.
    let again = r.gc(&gc_opts()).await;
    assert_eq!((again.packs, again.packs_compacted, again.objects), (0, 0, 0), "{again:?}");
}

/// Random histories, repacked with random settings, read back byte for
/// byte from a backend with nothing cached; chains stay within their bound,
/// every packed object ends up in a repacked pack, small objects move out,
/// old packs are deleted, a second run finds nothing to do, and a full
/// repack reads back the same again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_repacked_random_histories_read_back_byte_for_byte() {
    let cases: u64 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|c| c.parse::<u64>().ok())
        .map_or(4, |c| (c / 64).clamp(4, 24));
    for seed in 0..cases {
        let mut rng = StdRng::seed_from_u64(seed);
        let r = repo();
        let dir = r.store.dir();
        let n = rng.random_range(10..50);
        random_history(&r.b, &mut rng, n).await;
        let before = readback(&r.b).await;
        let small_before = count(dir, "SELECT count(*) FROM objects").await;
        let mut o = repack_opts();
        o.keep_recent = rng.random_range(0..4);
        o.small = rng.random_bool(0.8);
        o.cluster = ClusterOptions {
            window: rng.random_range(0..12),
            depth: rng.random_range(0..8),
            cluster_raw: [600, 4096, 64 * 1024, 2 << 20][rng.random_range(0..4)],
            level: rng.random_range(1..6),
        };
        o.jobs = rng.random_range(1..5);
        o.batch = rng.random_range(1..50);
        o.pack_bytes = [1, 10_000, 1 << 26][rng.random_range(0..3)];
        let report = r.repack(&o).await;
        let ctx = format!("seed {seed}: {:?} {report:?}", o.cluster);
        assert!(report.finished && report.skipped == 0, "{ctx}");
        assert!(report.max_depth <= o.cluster.depth, "{ctx}");
        assert!(report.moved > 0, "{ctx}");
        assert_eq!(unrepacked(dir).await, 0, "{ctx}");
        if o.small {
            assert!(report.moved_small > 0, "{ctx}");
            let small = count(dir, "SELECT count(*) FROM objects").await;
            assert_eq!(small, small_before - report.moved_small as i64, "{ctx}");
        }
        assert!(report.packs_deleted > 0, "{ctx}");
        let files = pack_files(dir);
        assert!(indexed_packs(dir).await.is_subset(&files), "{ctx}");
        assert_eq!(readback(&r.fresh()).await, before, "{ctx}");
        assert_eq!(readback(&r.b).await, before, "{ctx}");

        let again = r.repack(&o).await;
        assert_eq!((again.moved, again.selected_cold), (0, 0), "seed {seed}: {again:?}");
        let all = r.repack(&RepackOptions { all: true, ..o.clone() }).await;
        assert!(all.finished && all.moved >= report.moved - report.moved_small, "{all:?}");
        assert_eq!(readback(&r.fresh()).await, before, "{ctx}");
        let tip = r.b.refs.get_ref("main").await.unwrap();
        let c = commit_ops(&r.b, tip, vec![write("after/repack", b"x\n")]).await;
        land_tip(&r.b, "main", c).await;
    }
}

/// Small objects keep their write time when they move into packs, and so
/// do moved index rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_repack_keeps_write_times() {
    let r = repo();
    let dir = r.store.dir();
    let mut rng = StdRng::seed_from_u64(7);
    random_history(&r.b, &mut rng, 20).await;
    age(dir, 3).await;
    let times = "SELECT group_concat(h, ',') FROM (SELECT hex(id) || ':' || written_at AS h \
                 FROM (SELECT id, written_at FROM objects UNION ALL \
                       SELECT id, written_at FROM object_index) ORDER BY h)";
    let read = |q: &'static str| {
        let d = dir.to_owned();
        tokio::task::spawn_blocking(move || {
            db(&d).query_row(q, [], |r| r.get::<_, String>(0)).unwrap()
        })
    };
    let before = read(times).await.unwrap();
    let report = r.repack(&RepackOptions { keep_recent: 0, ..repack_opts() }).await;
    assert!(report.finished && report.moved_small > 0, "{report:?}");
    assert_eq!(read(times).await.unwrap(), before);
}

/// Garbage collection after a repack: an object that dies while other
/// versions are stored as deltas against it leaves them readable (its bytes
/// stay in the record, which stays while any row points into it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_gc_after_repack_keeps_delta_bases() {
    let r = repo();
    let (b, dir) = (&r.b, r.store.dir());
    let path = "src/big.txt";
    let mut edits = Vec::new();
    let mut tip = None;
    let mut versions = Vec::new();
    for v in 0..12u32 {
        edits.push(((v as usize * 7) % 300, v));
        let content = text(path, 300, &edits);
        versions.push(lit(&EncodedObject::new(ObjectKind::Blob, &content).id()));
        let c = commit_ops(b, tip, vec![write(path, &content)]).await;
        tip = Some(land_tip(b, "main", c).await);
    }
    let mut main_tip = tip.unwrap();
    for i in 0..3 {
        let c = commit_ops(b, Some(main_tip), vec![write(&format!("other/{i}"), b"x\n")]).await;
        main_tip = land_tip(b, "main", c).await;
    }
    edits.push((5, 99));
    let newest = text(path, 300, &edits);
    let newest_id = EncodedObject::new(ObjectKind::Blob, &newest).id();
    versions.push(lit(&newest_id));
    let doomed = commit_ops(b, Some(main_tip), vec![write(path, &newest)]).await;
    let before_main = readback(b).await;
    b.refs.cas_ref("doomed", None, Some(doomed)).await.unwrap();
    let report = r.repack(&RepackOptions { keep_recent: 0, ..repack_opts() }).await;
    assert!(report.finished && report.deltas >= 11, "{report:?}");
    let at = |id: &ObjectId| {
        format!("SELECT hex(pack) || ':' || off FROM object_index WHERE id = {}", lit(id))
    };
    let d = dir.to_owned();
    let q = at(&newest_id);
    let base_at: String =
        tokio::task::spawn_blocking(move || db(&d).query_row(&q, [], |r| r.get(0)).unwrap())
            .await
            .unwrap();
    let mates = format!(
        "SELECT count(*) FROM object_index WHERE hex(pack) || ':' || off = '{base_at}' \
         AND id IN ({})",
        versions.join(", ")
    );
    assert_eq!(count(dir, &mates).await, 13, "the versions share the newest one's record");
    b.refs.cas_ref("doomed", Some(doomed), None).await.unwrap();
    age(dir, 30).await;
    let report = r.gc(&GcOptions { compact_ratio: 1.0, ..gc_opts() }).await;
    let base_row = format!("SELECT count(*) FROM object_index WHERE id = {}", lit(&newest_id));
    assert_eq!(count(dir, &base_row).await, 0, "the base's row is collected: {report:?}");
    assert_eq!(count(dir, &mates).await, 12, "the versions built on it stay");
    assert_eq!(readback(&r.fresh()).await, before_main);
    assert_eq!(r.fresh().objects.get_many(&[newest_id]).await.unwrap(), vec![None]);
    // Compaction copies the record whole: its members stay readable.
    r.gc(&GcOptions { compact_ratio: 0.0, ..gc_opts() }).await;
    assert_eq!(readback(&r.fresh()).await, before_main);
    // A full repack rewrites the record without the dead base.
    let all = r.repack(&RepackOptions { keep_recent: 0, all: true, ..repack_opts() }).await;
    assert!(all.finished && all.moved > 0, "{all:?}");
    assert_eq!(readback(&r.fresh()).await, before_main);
}

/// Objects of recent revisions stay one decode away; once newer commits
/// make them cold, the next incremental run delta-compresses them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_hot_objects_are_repacked_once_they_go_cold() {
    let r = repo();
    let (b, dir) = (&r.b, r.store.dir());
    let path = "src/hot.txt";
    let mut edits = Vec::new();
    let mut tip = None;
    let mut versions = Vec::new();
    for v in 0..6u32 {
        edits.push((v as usize, v));
        let content = text(path, 300, &edits);
        versions.push(EncodedObject::new(ObjectKind::Blob, &content).id());
        let c = commit_ops(b, tip, vec![write(path, &content)]).await;
        tip = Some(land_tip(b, "main", c).await);
    }
    let o = RepackOptions { keep_recent: 1, ..repack_opts() };
    let first = r.repack(&o).await;
    assert!(first.finished && first.selected_hot > 0, "{first:?}");
    let in_hot_pack = |id: &ObjectId| {
        format!(
            "SELECT count(*) FROM object_index x JOIN packs p ON p.pack = x.pack \
             WHERE x.id = {} AND p.hot = 1",
            lit(id)
        )
    };
    let newest = versions[5];
    assert_eq!(count(dir, &in_hot_pack(&newest)).await, 1, "the tip's version is hot");
    assert_eq!(count(dir, &in_hot_pack(&versions[0])).await, 0, "older ones are not");
    let again = r.repack(&o).await;
    assert_eq!((again.moved, again.selected_hot), (0, 0), "{again:?}");
    edits.push((100, 100));
    let c = commit_ops(b, tip, vec![write(path, &text(path, 300, &edits))]).await;
    land_tip(b, "main", c).await;
    let before = readback(b).await;
    let next = r.repack(&o).await;
    assert!(next.moved >= 1 && next.selected_cold >= 1, "{next:?}");
    assert_eq!(count(dir, &in_hot_pack(&newest)).await, 0, "no longer in a hot pack");
    assert_eq!(unrepacked(dir).await, 0);
    assert_eq!(readback(&r.fresh()).await, before);
}

/// Interrupted after a few packs (each one switched as it is written), a
/// repack leaves a readable store; the next run continues, and nothing is
/// moved twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_interrupted_repack_resumes() {
    let r = repo();
    let dir = r.store.dir();
    let mut rng = StdRng::seed_from_u64(3);
    random_history(&r.b, &mut rng, 50).await;
    let before = readback(&r.b).await;
    let o = RepackOptions {
        pack_bytes: 2000,
        stop_after_packs: Some(2),
        jobs: 1,
        small: false,
        cluster: ClusterOptions { cluster_raw: 1500, ..repack_opts().cluster },
        ..repack_opts()
    };
    let first = r.repack(&o).await;
    assert!(!first.finished && first.packs >= 2 && first.moved > 0, "{first:?}");
    assert!(unrepacked(dir).await > 0);
    assert_eq!(readback(&r.fresh()).await, before);
    let mut moved = first.moved;
    let mut runs = 1;
    loop {
        let next = r.repack(&RepackOptions { stop_after_packs: Some(5), ..o.clone() }).await;
        moved += next.moved;
        runs += 1;
        assert_eq!(readback(&r.fresh()).await, before);
        if next.finished {
            break;
        }
        assert!(runs < 200, "no progress");
    }
    let total = count(dir, "SELECT count(*) FROM object_index").await;
    assert_eq!(unrepacked(dir).await, 0);
    assert_eq!(moved as i64, total, "every packed object moved exactly once");
    assert_eq!(readback(&r.fresh()).await, before);
}

/// Collections and repacks cancelled at arbitrary points (their futures
/// dropped, as when a maintenance client disconnects), and the process
/// restarted (the store reopened: nothing of a run survives in memory),
/// leave a consistent directory: everything reachable reads back, the next
/// runs finish, and no pack is left that nothing points into.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_cancelled_runs_and_restarts_leave_a_consistent_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("data");
    ant_server::ops::set_cache_max_age(Duration::ZERO);
    let mut rng = StdRng::seed_from_u64(9);
    let before;
    {
        let store = LocalStore::open(&path, &options()).unwrap();
        let b = store.backend();
        random_history(&b, &mut rng, 40).await;
        let tip = b.refs.get_ref("main").await.unwrap();
        for i in 0..5 {
            changeset(&b, tip, &[(&format!("junk/{i}"), &text("junk", 20 + i, &[]))]).await;
        }
        before = readback(&b).await;
    }
    age(&path, 30).await;
    for round in 0..12u64 {
        let store = LocalStore::open(&path, &options()).unwrap();
        let o = RepackOptions {
            pack_bytes: 3000,
            all: round.is_multiple_of(3),
            cluster: ClusterOptions { cluster_raw: 2000, ..repack_opts().cluster },
            ..repack_opts()
        };
        let g = GcOptions {
            quiesce: Duration::from_millis(rng.random_range(0..30)),
            batch: 3,
            compact_ratio: 0.2,
            ..gc_opts()
        };
        let wait = Duration::from_millis(rng.random_range(0..40));
        let s = store.clone();
        let cancelled = if round.is_multiple_of(2) {
            tokio::time::timeout(wait, async move { repack(&s, &o, &|_| {}).await.map(|_| ()) })
                .await
        } else {
            tokio::time::timeout(wait, async move { collect(&s, &g, &|_| {}).await.map(|_| ()) })
                .await
        };
        if let Ok(r) = cancelled {
            r.unwrap();
        }
        assert_eq!(readback(&store.backend()).await, before, "round {round}");
        drop(store);
    }
    let store = LocalStore::open(&path, &options()).unwrap();
    assert_eq!(readback(&store.backend()).await, before);
    let report =
        collect(&store, &GcOptions { compact_ratio: 0.5, ..gc_opts() }, &|_| {}).await.unwrap();
    assert_eq!(report.missing, 0, "{report:?}");
    let r = repack(&store, &repack_opts(), &|_| {}).await.unwrap();
    assert!(r.finished, "{r:?}");
    assert_eq!(readback(&store.backend()).await, before);
    let files = pack_files(&path);
    let indexed = indexed_packs(&path).await;
    assert_eq!(files, indexed, "no unreferenced pack is left");
    assert_eq!(unrepacked(&path).await, 0);
}

/// The maintenance socket: a collection and a repack requested through it
/// run in the process that owns the store, with progress and a report; a
/// bad request is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_maintenance_socket_runs_collections_and_repacks() {
    use ant_store::local::admin;
    let r = repo();
    let (b, dir) = (&r.b, r.store.dir());
    assert!(admin::remote_gc(dir, &gc_opts(), &|_| {}).await.unwrap().is_none(), "nobody listens");
    let server = admin::serve(r.store.clone()).unwrap();
    assert!(server.path().exists());
    let (tip, _) = history(b, 3).await;
    let junk = changeset(b, Some(tip), &[("junk", b"junk\n")]).await;
    let before = readback(b).await;
    age(dir, 30).await;
    let lines = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let l = lines.clone();
    let progress = move |s: &str| l.lock().unwrap().push(s.to_owned());
    let report = admin::remote_gc(dir, &gc_opts(), &progress).await.unwrap().unwrap();
    assert!(report.commits == 1 && report.condemned > 0, "{report:?}");
    assert!(!lines.lock().unwrap().is_empty(), "progress arrives");
    assert_eq!(stored(dir, &junk).await, 0);
    let rp = admin::remote_repack(dir, &repack_opts(), &|_| {}).await.unwrap().unwrap();
    assert!(rp.finished && rp.moved > 0, "{rp:?}");
    assert_eq!(readback(&r.fresh()).await, before);
    let conn = admin::connect(dir).await.unwrap().unwrap();
    let err = admin::exchange(conn, "gc grace=1\n", &|_| {}).await.unwrap_err();
    assert!(err.to_string().contains("grace period"), "{err}");
    let conn = admin::connect(dir).await.unwrap().unwrap();
    let err = admin::exchange(conn, "gc frobnicate=1\n", &|_| {}).await.unwrap_err();
    assert!(err.to_string().contains("unknown setting"), "{err}");
    let path = server.path().to_owned();
    drop(server);
    assert!(!path.exists());
}

/// Writers keep creating and landing changesets, land changesets that are
/// older than the grace period, store blobs identical to old garbage again
/// and point refs at old commits (blind ref moves: readers may see such a
/// ref's commit as missing until the collector marks it again, as with
/// Postgres), all while collections run. Nothing reachable afterwards may
/// be missing, and garbage nobody touched is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn local_gc_races_a_fleet_of_writers_without_losing_reachable_data() {
    let r = repo();
    ant_server::ops::set_cache_max_age(Duration::from_millis(100));
    let (b, dir) = (r.b.clone(), r.store.dir().to_owned());
    let (tip, _) = history(&b, 6).await;
    let mut old_changesets = Vec::new();
    let mut old_blobs = Vec::new();
    for i in 0..24 {
        let content = format!("old garbage {i}\n").into_bytes();
        old_changesets.push(changeset(&b, Some(tip), &[(&format!("old/c{i}"), &content)]).await);
        let blob = EncodedObject::new(ObjectKind::Blob, &text(&format!("old blob {i}"), i, &[]));
        b.objects.put_many(std::slice::from_ref(&blob)).await.unwrap();
        old_blobs.push(blob);
    }
    let mut untouched = Vec::new();
    for i in 0..4 {
        let content = format!("untouched {i}\n").into_bytes();
        untouched.push(changeset(&b, Some(tip), &[(&format!("untouched/c{i}"), &content)]).await);
    }
    let stray = EncodedObject::new(ObjectKind::Blob, b"untouched blob\n");
    b.objects.put_many(std::slice::from_ref(&stray)).await.unwrap();
    untouched.push(stray.id());
    age(&dir, 30).await;

    let stop = Arc::new(AtomicBool::new(false));
    let lands = Arc::new(AtomicUsize::new(0));
    let phase_lands = Arc::new(AtomicUsize::new(0));
    let mut writers = Vec::new();
    for w in 0..6usize {
        let (b, stop, lands, phase_lands) =
            (b.clone(), stop.clone(), lands.clone(), phase_lands.clone());
        let (old_changesets, old_blobs) = (old_changesets.clone(), old_blobs.clone());
        writers.push(tokio::spawn(async move {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                let k = (w * 7 + i) % old_changesets.len();
                let tip = b.refs.get_ref("main").await.unwrap().unwrap();
                match i % 4 {
                    0 => match land_retrying(&b, "main", old_changesets[k]).await {
                        Ok(_) | Err(OpError::NotFound(_)) => {}
                        Err(e) => panic!("land old: {e}"),
                    },
                    1 => {
                        let blob = &old_blobs[k];
                        b.objects.put_many(std::slice::from_ref(blob)).await.unwrap();
                        let ops = vec![FileOp::Write {
                            path: RepoPath::new(format!("reused/w{w}/b{k}")).unwrap(),
                            mode: EntryMode::File,
                            content: WriteContent::Blob(blob.id()),
                        }];
                        let c = commit_ops(&b, Some(tip), ops).await;
                        land_retrying(&b, "main", c).await.unwrap();
                    }
                    2 => {
                        let target = old_changesets[k];
                        if b.meta.get_commits(&[target]).await.unwrap()[0].is_some() {
                            let name = format!("ws/w{w}/old");
                            let cur = b.refs.get_ref(&name).await.unwrap();
                            b.refs.cas_ref(&name, cur, Some(target)).await.unwrap();
                        }
                    }
                    _ => {
                        let path = format!("work/w{w}/f{}", i % 5);
                        let c =
                            changeset(&b, Some(tip), &[(&path, format!("{i}\n").as_bytes())]).await;
                        land_retrying(&b, "main", c).await.unwrap();
                    }
                }
                lands.fetch_add(1, Ordering::Relaxed);
                let n = phase_lands.fetch_add(1, Ordering::Relaxed) + 1;
                tokio::time::sleep(writer_pause(n)).await;
            }
        }));
    }
    let o = GcOptions { quiesce: Duration::from_millis(500), batch: 3, ..gc_opts() };
    let mut total = 0;
    for _ in 0..2 {
        phase_lands.store(0, Ordering::Relaxed);
        let report = r.gc(&o).await;
        total += report.objects + report.commits;
    }
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.await.unwrap();
    }
    assert!(total > 0, "the collections deleted nothing");
    for id in &untouched {
        assert_eq!(stored(&dir, id).await, 0, "untouched garbage {id} was kept");
    }
    assert!(lands.load(Ordering::Relaxed) > 0);
    let reachable = readback(&b).await;
    let report = r.gc(&o).await;
    assert_eq!(report.missing, 0, "{report:?}");
    assert_eq!(readback(&r.fresh()).await, reachable);
}

/// How long a writer of `local_maintenance_races_writers_and_readers`
/// pauses after the `n`th land of the current phase: past [`PHASE_LANDS`]
/// lands in one phase, writers slow down to a land a second each, so a
/// slow runner adds little history per phase however long the phase takes
/// (the mark, walk and read-back cost grows with history).
fn writer_pause(n: usize) -> Duration {
    if n <= PHASE_LANDS { Duration::from_millis(20) } else { Duration::from_secs(1) }
}

/// Lands per phase at the full pace.
const PHASE_LANDS: usize = 150;

/// Writers land changesets (some re-storing old garbage and using it by
/// id), readers read everything reachable, collections and repacks run,
/// all at once. No read may fail or miss a reachable object, and everything
/// reachable at the end is intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn local_maintenance_races_writers_and_readers() {
    let r = Arc::new(repo());
    ant_server::ops::set_cache_max_age(Duration::from_millis(100));
    let dir = r.store.dir().to_owned();
    let mut rng = StdRng::seed_from_u64(5);
    random_history(&r.b, &mut rng, 30).await;
    let tip = r.b.refs.get_ref("main").await.unwrap();
    // Old garbage: abandoned changesets, and blobs the writers bring back.
    let mut garbage = Vec::new();
    for i in 0..6 {
        let content = text(&format!("junk{i}"), 50, &[(i, i as u32)]);
        garbage.push(commit_ops(&r.b, tip, vec![write(&format!("junk/{i}"), &content)]).await);
    }
    let mut old_blobs = Vec::new();
    for i in 0..8 {
        let blob = EncodedObject::new(ObjectKind::Blob, &text(&format!("old{i}"), 1 + i * 10, &[]));
        r.b.objects.put_many(std::slice::from_ref(&blob)).await.unwrap();
        old_blobs.push(blob);
    }
    age(&dir, 30).await;

    let stop = Arc::new(AtomicBool::new(false));
    let (lands, reads, repacks) = (
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    let phase_lands = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for w in 0..3usize {
        let (r, stop, lands, phase_lands) =
            (r.clone(), stop.clone(), lands.clone(), phase_lands.clone());
        let old_blobs = old_blobs.clone();
        tasks.push(tokio::spawn(async move {
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                let tip = r.b.refs.get_ref("main").await.unwrap();
                let k = (w * 5 + i as usize) % old_blobs.len();
                let ops = match i % 5 {
                    // An old garbage blob stored again and used by id.
                    0 => {
                        r.b.objects.put_many(std::slice::from_ref(&old_blobs[k])).await.unwrap();
                        vec![FileOp::Write {
                            path: RepoPath::new(format!("reused/w{w}/b{k}")).unwrap(),
                            mode: EntryMode::File,
                            content: WriteContent::Blob(old_blobs[k].id()),
                        }]
                    }
                    _ => {
                        let path = format!("src/m{}/f{}.rs", w % 3, (w as u32 * 4 + i) % 12);
                        let content =
                            text(&path, 40 + (i as usize % 3) * 40, &[(i as usize % 40, i)]);
                        vec![write(&path, &content)]
                    }
                };
                let c = commit_ops(&r.b, tip, ops).await;
                match land_retrying(&r.b, "main", c).await {
                    Ok(_) => {}
                    Err(e) => panic!("land: {e}"),
                }
                lands.fetch_add(1, Ordering::Relaxed);
                let n = phase_lands.fetch_add(1, Ordering::Relaxed) + 1;
                tokio::time::sleep(writer_pause(n)).await;
            }
        }));
    }
    for _ in 0..2 {
        let (r, stop, reads) = (r.clone(), stop.clone(), reads.clone());
        tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                readback(&r.fresh()).await;
                readback(&r.b).await;
                reads.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    {
        let (r, stop, repacks) = (r.clone(), stop.clone(), repacks.clone());
        tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let o = RepackOptions {
                    keep_recent: 2,
                    pack_bytes: 20_000,
                    all: repacks.load(Ordering::Relaxed) % 2 == 1,
                    ..repack_opts()
                };
                let report = r.repack(&o).await;
                assert!(report.finished, "{report:?}");
                repacks.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    let gc =
        GcOptions { quiesce: Duration::from_secs(1), batch: 5, compact_ratio: 0.3, ..gc_opts() };
    let mut collected = 0;
    for _ in 0..3 {
        phase_lands.store(0, Ordering::Relaxed);
        let report = r.gc(&gc).await;
        assert_eq!(report.missing, 0, "{report:?}");
        collected += report.objects + report.large_objects + report.commits;
    }
    phase_lands.store(0, Ordering::Relaxed);
    let n = repacks.load(Ordering::Relaxed);
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    while repacks.load(Ordering::Relaxed) < n + 2 {
        assert!(std::time::Instant::now() < deadline, "repacks stopped");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await.unwrap();
    }
    eprintln!(
        "lands {}, readbacks {}, repacks {}, collected {collected}",
        lands.load(Ordering::Relaxed),
        reads.load(Ordering::Relaxed),
        repacks.load(Ordering::Relaxed)
    );
    assert!(collected > 0 && repacks.load(Ordering::Relaxed) >= 2);
    let after = readback(&r.fresh()).await;
    let report = r.gc(&gc).await;
    assert_eq!(report.missing, 0, "{report:?}");
    assert_eq!(readback(&r.fresh()).await, after);
    let refs: HashSet<ObjectId> =
        r.b.refs.list_refs("").await.unwrap().into_iter().map(|(_, id)| id).collect();
    assert!(refs.iter().all(|id| after.contains_key(id)));
    // Abandoned changesets no ref points at are gone.
    for c in &garbage {
        if !after.contains_key(c) {
            assert_eq!(stored(&dir, c).await, 0, "garbage {c} survived");
        }
    }
}

/// Environment variable that turns
/// `local_killed_processes_leave_a_consistent_directory` into its child:
/// the process that is killed.
const CRASH_CHILD: &str = "ANT_TEST_LOCAL_CRASH_CHILD";

/// The child of `local_killed_processes_leave_a_consistent_directory`:
/// lands changesets and leaves garbage on one task, collects and repacks in
/// a loop on another, until it is killed.
async fn crash_child(dir: &Path) {
    let store = LocalStore::open(dir, &options()).unwrap();
    let b = store.backend();
    let writer = {
        let b = b.clone();
        tokio::spawn(async move {
            let mut i = 0u32;
            loop {
                i += 1;
                let tip = b.refs.get_ref("main").await.unwrap();
                let path = format!("live/f{}.txt", i % 7);
                let content = text(&path, 30, &[(i as usize % 30, i)]);
                let c = commit_ops(&b, tip, vec![write(&path, &content)]).await;
                land_retrying(&b, "main", c).await.unwrap();
                // Garbage for the next collections.
                changeset(&b, tip, &[(&format!("junk/{i}"), &text("junk", 25, &[(1, i)]))]).await;
            }
        })
    };
    let mut round = 0u32;
    loop {
        round += 1;
        let o = RepackOptions {
            pack_bytes: 4000,
            all: round.is_multiple_of(2),
            cluster: ClusterOptions { cluster_raw: 3000, ..repack_opts().cluster },
            ..repack_opts()
        };
        repack(&store, &o, &|_| {}).await.unwrap();
        let g = GcOptions {
            quiesce: Duration::from_millis(20),
            batch: 4,
            compact_ratio: 0.2,
            ..gc_opts()
        };
        collect(&store, &g, &|_| {}).await.unwrap();
        assert!(!writer.is_finished(), "the writer failed");
    }
}

/// A process collecting, repacking and landing at once, killed (SIGKILL) at
/// arbitrary moments, leaves a directory that opens, reads back everything
/// reachable, and is cleaned up completely by the next runs: no land
/// acknowledged before the kill is lost, nothing reachable is missing, and
/// no pack outlives the last row pointing into it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_killed_processes_leave_a_consistent_directory() {
    if let Ok(dir) = std::env::var(CRASH_CHILD) {
        crash_child(Path::new(&dir)).await;
        return;
    }
    ant_server::ops::set_cache_max_age(Duration::ZERO);
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("data");
    let mut rng = StdRng::seed_from_u64(13);
    let release;
    {
        let store = LocalStore::open(&path, &options()).unwrap();
        let b = store.backend();
        random_history(&b, &mut rng, 30).await;
        let tip = b.refs.get_ref("main").await.unwrap().unwrap();
        b.refs.cas_ref("release", None, Some(tip)).await.unwrap();
        release = readback(&b).await;
    }
    let exe = std::env::current_exe().unwrap();
    let mut main_before: Option<ObjectId> = None;
    for round in 0..8 {
        // Everything so far is old: the child's collections condemn the
        // garbage of earlier rounds while it writes new data.
        age(&path, 30).await;
        let mut child = std::process::Command::new(&exe)
            .args(["--exact", "local_killed_processes_leave_a_consistent_directory", "--nocapture"])
            .env(CRASH_CHILD, &path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(rng.random_range(150..700))).await;
        assert!(child.try_wait().unwrap().is_none(), "round {round}: the child stopped early");
        child.kill().unwrap();
        child.wait().unwrap();
        let store = LocalStore::open(&path, &options()).unwrap();
        let b = store.backend();
        let all = readback(&b).await;
        assert!(release.keys().all(|id| all.contains_key(id)), "round {round}");
        // `main` only moves forward: whatever it had stays reachable.
        let main = b.refs.get_ref("main").await.unwrap().unwrap();
        if let Some(m) = main_before {
            assert!(ant_store::graph::is_ancestor(&b, m, main).await.unwrap(), "round {round}");
        }
        main_before = Some(main);
        eprintln!(
            "round {round}: {} objects reachable, {} pack files, {} repacked packs",
            all.len(),
            pack_files(&path).len(),
            count(&path, "SELECT count(*) FROM packs").await
        );
        drop(store);
    }
    let store = LocalStore::open(&path, &options()).unwrap();
    let b = store.backend();
    let before = readback(&b).await;
    let gc = GcOptions { compact_ratio: 0.5, ..gc_opts() };
    // A changeset being created when the process was killed may have left
    // its commit record without its objects (they are written concurrently,
    // and its creation never returned): never reachable, it is collected
    // once it is older than the grace period.
    collect(&store, &gc, &|_| {}).await.unwrap();
    drop((b, store));
    age(&path, 30).await;
    let store = LocalStore::open(&path, &options()).unwrap();
    collect(&store, &gc, &|_| {}).await.unwrap();
    let report = collect(&store, &gc, &|_| {}).await.unwrap();
    assert_eq!(report.missing, 0, "{report:?}");
    let r = repack(&store, &repack_opts(), &|_| {}).await.unwrap();
    assert!(r.finished, "{r:?}");
    assert_eq!(readback(&store.backend()).await, before);
    assert_eq!(pack_files(&path), indexed_packs(&path).await, "no unreferenced pack is left");
}

/// `ant serve --local --gc-interval`: collections run in the background on
/// their schedule, while the store serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_scheduled_collections_run_in_the_background() {
    let r = repo();
    let (b, dir) = (&r.b, r.store.dir());
    let (tip, _) = history(b, 3).await;
    let junk = changeset(b, Some(tip), &[("junk", b"junk\n")]).await;
    let before = readback(b).await;
    age(dir, 30).await;
    let task =
        ant_store::local::admin::schedule_gc(r.store.clone(), Duration::from_millis(50), gc_opts());
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while stored(dir, &junk).await > 0 {
        assert!(std::time::Instant::now() < deadline, "no scheduled collection ran");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    task.abort();
    assert_eq!(readback(&r.fresh()).await, before);
}
