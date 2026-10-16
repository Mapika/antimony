//! Repacking (`ant_store::postgres::repack`) against the real write paths:
//! random histories read back byte for byte afterwards (chains within their
//! bound), garbage collection after a repack (old packs freed, delta bases
//! of live objects kept), interruption and resumption, condemned ids and
//! write times, and a repack racing writers, readers and collections.
//!
//! Runs with `--run-ignored all` (Postgres and S3).

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use ant_core::{EncodedObject, EntryMode, Object, ObjectId, ObjectKind, RepoPath, Signature};
use ant_server::ops::changeset::{ChangesetRequest, FileOp, WriteContent, create_changeset};
use ant_server::ops::land::{LandOutcome, land};
use ant_server::ops::{OpError, history};
use ant_store::Backend;
use ant_store::packed::{ClusterOptions, PackedS3Store};
use ant_store::postgres::PgStore;
use ant_store::postgres::gc::GcOptions;
use ant_store::postgres::repack::{RepackOptions, RepackReport};
use ant_store::s3::{S3Config, S3ObjectStore};
use ant_store::tiered::TieredObjectStore;
use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sqlx::Connection;

use ant_store::testing::pg_url;

/// Objects larger than this go to S3 packs, so both tiers get history.
const INLINE: usize = 200;

struct Repo {
    b: Backend,
    pg: Arc<PgStore>,
    s3: S3ObjectStore,
    schema: String,
    _guard: S3Guard,
}

/// Deletes an S3 prefix when dropped (also on panic).
struct S3Guard(S3Config);

impl Drop for S3Guard {
    fn drop(&mut self) {
        let cfg = self.0.clone();
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let client = aws_sdk_s3::Client::from_conf(cfg.sdk_config().build());
                let mut token = None;
                loop {
                    let Ok(page) = client
                        .list_objects_v2()
                        .bucket(&cfg.bucket)
                        .prefix(format!("{}/", cfg.prefix))
                        .set_continuation_token(token)
                        .send()
                        .await
                    else {
                        return;
                    };
                    for o in page.contents() {
                        if let Some(k) = o.key() {
                            let _ = client.delete_object().bucket(&cfg.bucket).key(k).send().await;
                        }
                    }
                    match page.next_continuation_token() {
                        Some(t) if page.is_truncated() == Some(true) => token = Some(t.to_owned()),
                        _ => return,
                    }
                }
            });
        })
        .join();
    }
}

/// A store as servers run it: small objects in Postgres, large ones in S3
/// packs.
fn backend(pg: &Arc<PgStore>, s3: &S3ObjectStore) -> Backend {
    let packed = PackedS3Store::new(pg.as_ref().clone(), s3.clone());
    let objects = Arc::new(TieredObjectStore::new(pg.clone(), Arc::new(packed), INLINE));
    Backend { objects, refs: pg.clone(), meta: pg.clone() }
}

async fn repo() -> Repo {
    // Collections here do not wait for server caches.
    ant_server::ops::set_cache_max_age(Duration::ZERO);
    let schema = format!("t_rp_{}", rand::random::<u64>());
    let pg = Arc::new(PgStore::connect_scratch(&pg_url(), &schema).await.unwrap());
    let cfg = S3Config::for_tests(&format!("rp{}", rand::random::<u64>()));
    let guard = S3Guard(cfg.clone());
    let s3 = S3ObjectStore::connect(&cfg).await.unwrap();
    s3.ensure_bucket().await.unwrap();
    let b = backend(&pg, &s3);
    Repo { b, pg, s3, schema, _guard: guard }
}

impl Repo {
    /// A backend with nothing cached (a server started afresh).
    fn fresh(&self) -> Backend {
        backend(&self.pg, &self.s3)
    }

    async fn repack(&self, o: &RepackOptions) -> RepackReport {
        self.pg.repack(self.b.objects.as_ref(), o, &|_| {}).await.unwrap()
    }
}

async fn conn(schema: &str) -> sqlx::PgConnection {
    let mut c = sqlx::PgConnection::connect(&pg_url()).await.unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("SET search_path TO \"{schema}\"")))
        .execute(&mut c)
        .await
        .unwrap();
    c
}

async fn sql(schema: &str, stmt: &str) {
    let mut c = conn(schema).await;
    sqlx::raw_sql(sqlx::AssertSqlSafe(stmt.to_owned())).execute(&mut c).await.unwrap();
    c.close().await.unwrap();
}

async fn count(schema: &str, stmt: &str) -> i64 {
    let mut c = conn(schema).await;
    let n = sqlx::query_scalar(sqlx::AssertSqlSafe(stmt.to_owned())).fetch_one(&mut c).await;
    c.close().await.unwrap();
    n.unwrap()
}

/// Pretend everything stored so far was written `days` ago.
async fn age(schema: &str, days: u32) {
    for t in ["objects", "object_index", "commits", "changesets"] {
        sql(schema, &format!("UPDATE {t} SET written_at = written_at - interval '{days} days'"))
            .await;
    }
}

fn lit(id: &ObjectId) -> String {
    format!("'\\x{}'::bytea", id.to_hex())
}

fn sig() -> Signature {
    Signature { name: "rp".into(), email: "rp@x".into(), time: 1_700_000_000, tz_offset_minutes: 0 }
}

async fn commit_ops(b: &Backend, base: Option<ObjectId>, ops: Vec<FileOp>) -> ObjectId {
    let req = ChangesetRequest { base, ops, author: sig(), committer: None, message: "m".into() };
    create_changeset(b, req).await.unwrap()
}

fn write(path: &str, content: &[u8]) -> FileOp {
    FileOp::Write {
        path: RepoPath::new(path).unwrap(),
        mode: EntryMode::File,
        content: WriteContent::Inline(Bytes::copy_from_slice(content)),
    }
}

async fn land_tip(b: &Backend, r: &str, c: ObjectId) -> ObjectId {
    match land(b, r, c).await.unwrap() {
        LandOutcome::Landed { new_tip, .. } => new_tip,
        other => panic!("{other:?}"),
    }
}

/// Every object reachable from every ref, with its bytes; panics on
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
        for (id, o) in frontier.iter().zip(got) {
            let o = o.unwrap_or_else(|| panic!("reachable object {id} is missing"));
            match o.decode().unwrap() {
                Object::Commit(c) => {
                    next.push(c.tree);
                    next.extend(c.parents.iter().copied());
                }
                Object::Tree(t) => next.extend(
                    t.entries().iter().filter(|e| e.mode != EntryMode::Gitlink).map(|e| e.id),
                ),
                Object::Blob(_) => {}
            }
            seen.insert(*id, o.into_bytes());
        }
        frontier = next;
    }
    seen
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
/// directories, edited a few lines at a time (so versions delta well),
/// sometimes rewritten or deleted, small and large. Returns the commits.
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
                    // Rewritten: shares nothing with earlier versions.
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
            // Deleting a file that is not there.
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

fn options(r: &Repo) -> RepackOptions {
    let mut o = RepackOptions::new(r.s3.clone());
    o.min_age = Duration::ZERO;
    o.keep_recent = 1;
    o.cluster.level = 3;
    o
}

/// Rows not in a pack a repack wrote.
async fn unrepacked(schema: &str) -> i64 {
    count(
        schema,
        "SELECT count(*) FROM object_index x WHERE NOT EXISTS \
         (SELECT 1 FROM packs p WHERE p.pack = x.pack AND p.repacked_at IS NOT NULL)",
    )
    .await
}

/// Random histories, repacked with random settings, read back byte for byte
/// from a server with nothing cached; chains stay within their bound, every
/// packed object ends up in a repacked pack, Postgres-resident history moves
/// out, a second run finds nothing to do, and a full repack (`all`) reads
/// back the same again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs postgres and s3: docker compose up -d"]
async fn repacked_random_histories_read_back_byte_for_byte() {
    let cases: u64 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|c| c.parse::<u64>().ok())
        .map_or(4, |c| (c / 64).clamp(4, 24));
    for seed in 0..cases {
        let mut rng = StdRng::seed_from_u64(seed);
        let r = repo().await;
        let n = rng.random_range(10..60);
        random_history(&r.b, &mut rng, n).await;
        let before = readback(&r.b).await;
        let small_before = count(&r.schema, "SELECT count(*) FROM objects").await;
        let mut o = options(&r);
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
        assert_eq!(unrepacked(&r.schema).await, 0, "{ctx}");
        if o.small {
            assert!(report.moved_small > 0, "{ctx}");
            let small = count(&r.schema, "SELECT count(*) FROM objects").await;
            assert_eq!(small, small_before - report.moved_small as i64, "{ctx}");
        }
        assert_eq!(readback(&r.fresh()).await, before, "{ctx}");
        assert_eq!(readback(&r.b).await, before, "{ctx}");

        let again = r.repack(&o).await;
        assert_eq!((again.moved, again.selected_cold), (0, 0), "seed {seed}: {again:?}");
        let all = r.repack(&RepackOptions { all: true, ..o.clone() }).await;
        assert!(all.finished && all.moved >= report.moved - report.moved_small, "{all:?}");
        assert_eq!(readback(&r.fresh()).await, before, "{ctx}");
        // Writing and history queries go on.
        let tip = r.b.refs.get_ref("main").await.unwrap();
        let c = commit_ops(&r.b, tip, vec![write("after/repack", b"x\n")]).await;
        land_tip(&r.b, "main", c).await;
    }
}

/// Small objects keep their write time when they move into packs, and ids a
/// collection has condemned are not moved (they stay where the collector
/// will delete them, and stay hidden).
#[tokio::test]
#[ignore = "needs postgres and s3: docker compose up -d"]
async fn repack_keeps_write_times_and_skips_condemned_ids() {
    let r = repo().await;
    let mut rng = StdRng::seed_from_u64(7);
    random_history(&r.b, &mut rng, 20).await;
    age(&r.schema, 3).await;
    let times = "SELECT string_agg(encode(id, 'hex') || written_at::text, ',' ORDER BY id) \
                 FROM (SELECT id, written_at FROM objects UNION ALL \
                       SELECT id, written_at FROM object_index) t";
    let before_times = count_text(&r.schema, times).await;
    // Condemn one packed and one Postgres-resident object of old history.
    let packed: Vec<u8> =
        scalar_bytes(&r.schema, "SELECT id FROM object_index ORDER BY id LIMIT 1").await;
    let small: Vec<u8> =
        scalar_bytes(&r.schema, "SELECT id FROM objects ORDER BY id LIMIT 1").await;
    let (packed, small) =
        (ObjectId::from_bytes(&packed).unwrap(), ObjectId::from_bytes(&small).unwrap());
    sql(
        &r.schema,
        "INSERT INTO gc_state (phase, started_at, cutoff, grace_secs, quiesce_secs) \
         VALUES ('quiesce', now(), now() - interval '1 day', 0, 0)",
    )
    .await;
    for id in [packed, small] {
        sql(&r.schema, &format!("INSERT INTO gc_condemned VALUES ({})", lit(&id))).await;
    }
    let report = r.repack(&RepackOptions { keep_recent: 0, ..options(&r) }).await;
    assert!(report.finished && report.moved_small > 0, "{report:?}");
    // Write times: every row keeps its own (moved small objects take theirs
    // along).
    assert_eq!(count_text(&r.schema, times).await, before_times);
    // The condemned ones were not moved, and still read as missing.
    let in_old_place = format!(
        "SELECT count(*) FROM object_index x WHERE id = {} AND NOT EXISTS \
         (SELECT 1 FROM packs p WHERE p.pack = x.pack AND p.repacked_at IS NOT NULL)",
        lit(&packed)
    );
    assert_eq!(count(&r.schema, &in_old_place).await, 1);
    let still_small = format!("SELECT count(*) FROM objects WHERE id = {}", lit(&small));
    assert_eq!(count(&r.schema, &still_small).await, 1);
    assert_eq!(r.fresh().objects.get_many(&[packed, small]).await.unwrap(), vec![None, None]);
    // Aborting the collection makes them visible again, where they were.
    r.pg.gc_abort().await.unwrap();
    let got = r.fresh().objects.get_many(&[packed, small]).await.unwrap();
    assert!(got.iter().all(Option::is_some));
}

async fn count_text(schema: &str, stmt: &str) -> String {
    let mut c = conn(schema).await;
    let n: Option<String> =
        sqlx::query_scalar(sqlx::AssertSqlSafe(stmt.to_owned())).fetch_one(&mut c).await.unwrap();
    c.close().await.unwrap();
    n.unwrap_or_default()
}

async fn scalar_bytes(schema: &str, stmt: &str) -> Vec<u8> {
    let mut c = conn(schema).await;
    let v: Vec<u8> =
        sqlx::query_scalar(sqlx::AssertSqlSafe(stmt.to_owned())).fetch_one(&mut c).await.unwrap();
    c.close().await.unwrap();
    v
}

/// Garbage collection after a repack: the old packs go (once the quiesce
/// period after the move has passed), and an object that dies while other
/// versions are stored as deltas against it leaves them readable: its bytes
/// stay in the record, which stays while any row points into it.
#[tokio::test]
#[ignore = "needs postgres and s3: docker compose up -d"]
async fn gc_after_repack_frees_old_packs_and_keeps_delta_bases() {
    let r = repo().await;
    let b = &r.b;
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
    let tip = tip.unwrap();
    // A few commits on main after it, so that the branch below is newest.
    let mut main_tip = tip;
    for i in 0..3 {
        let c = commit_ops(b, Some(main_tip), vec![write(&format!("other/{i}"), b"x\n")]).await;
        main_tip = land_tip(b, "main", c).await;
    }
    // The newest version lives only on a branch that will go away: as the
    // newest, it is stored whole and the older ones as deltas against it.
    edits.push((5, 99));
    let newest = text(path, 300, &edits);
    let newest_id = EncodedObject::new(ObjectKind::Blob, &newest).id();
    versions.push(lit(&newest_id));
    let doomed = commit_ops(b, Some(main_tip), vec![write(path, &newest)]).await;
    let before_main = readback(b).await;
    b.refs.cas_ref("doomed", None, Some(doomed)).await.unwrap();
    // Nothing hot: every version is in the delta record.
    let report = r.repack(&RepackOptions { keep_recent: 0, ..options(&r) }).await;
    assert!(report.finished && report.deltas >= 11, "{report:?}");
    let record = |id: &ObjectId| {
        format!("SELECT encode(pack, 'hex') || ':' || off FROM object_index WHERE id = {}", lit(id))
    };
    let base_at = count_text(&r.schema, &record(&newest_id)).await;
    assert!(!base_at.is_empty());
    let mates = format!(
        "SELECT count(*) FROM object_index WHERE encode(pack, 'hex') || ':' || off = '{base_at}' \
         AND id IN ({})",
        versions.join(", ")
    );
    assert_eq!(count(&r.schema, &mates).await, 13, "the versions share the newest one's record");

    // The branch goes; everything ages past the grace period; collect.
    b.refs.cas_ref("doomed", Some(doomed), None).await.unwrap();
    age(&r.schema, 30).await;
    let gc = GcOptions {
        quiesce: Duration::ZERO,
        s3: Some(r.s3.clone()),
        pack_min_age: Some(Duration::ZERO),
        ..GcOptions::default()
    };
    let report = r.pg.gc(b.objects.as_ref(), &gc, &|_| {}).await.unwrap();
    assert!(report.finished, "{report:?}");
    assert!(report.packs > 0, "old packs deleted: {report:?}");
    let base_row = format!("SELECT count(*) FROM object_index WHERE id = {}", lit(&newest_id));
    assert_eq!(count(&r.schema, &base_row).await, 0, "the base's row is collected");
    assert_eq!(count(&r.schema, &mates).await, 12, "the versions built on it stay");
    // Everything reachable reads back from S3 (nothing cached); the base is
    // no object any more.
    assert_eq!(readback(&r.fresh()).await, before_main);
    assert_eq!(r.fresh().objects.get_many(&[newest_id]).await.unwrap(), vec![None]);
    // The record's pack stays: rows point into it.
    let again = r.pg.gc(b.objects.as_ref(), &gc, &|_| {}).await.unwrap();
    assert_eq!(again.packs, 0, "{again:?}");
    assert_eq!(readback(&r.fresh()).await, before_main);
    // A full repack rewrites the record without the dead base.
    let all = r.repack(&RepackOptions { keep_recent: 0, all: true, ..options(&r) }).await;
    assert!(all.finished && all.moved > 0, "{all:?}");
    assert_eq!(readback(&r.fresh()).await, before_main);
}

/// A pack a repack moved rows out of is kept for the collector's quiesce
/// period after the move, even though no row points into it any more.
#[tokio::test]
#[ignore = "needs postgres and s3: docker compose up -d"]
async fn gc_keeps_packs_rows_were_just_moved_out_of() {
    let r = repo().await;
    let mut rng = StdRng::seed_from_u64(11);
    random_history(&r.b, &mut rng, 15).await;
    let before = readback(&r.b).await;
    r.repack(&options(&r)).await;
    age(&r.schema, 30).await;
    let moved = count(&r.schema, "SELECT count(*) FROM packs WHERE moved_at IS NOT NULL").await;
    assert!(moved > 0);
    let empty = "SELECT count(*) FROM packs p WHERE moved_at IS NOT NULL \
                 AND NOT EXISTS (SELECT 1 FROM object_index x WHERE x.pack = p.pack)";
    let empty_packs = count(&r.schema, empty).await;
    assert!(empty_packs > 0);
    // Dry runs judge packs like real ones (and do not wait for quiesce).
    let gc = GcOptions {
        quiesce: Duration::from_secs(3600),
        s3: Some(r.s3.clone()),
        pack_min_age: Some(Duration::ZERO),
        dry_run: true,
        ..GcOptions::default()
    };
    let report = r.pg.gc(r.b.objects.as_ref(), &gc, &|_| {}).await.unwrap();
    assert_eq!(report.packs, 0, "moved out of within the quiesce period: {report:?}");
    sql(&r.schema, "UPDATE packs SET moved_at = moved_at - interval '2 hours'").await;
    let report = r.pg.gc(r.b.objects.as_ref(), &gc, &|_| {}).await.unwrap();
    assert_eq!(report.packs as i64, empty_packs, "{report:?}");
    // A real run deletes them and forgets them.
    let real = GcOptions { dry_run: false, quiesce: Duration::ZERO, ..gc };
    let report = r.pg.gc(r.b.objects.as_ref(), &real, &|_| {}).await.unwrap();
    assert_eq!(report.packs as i64, empty_packs, "{report:?}");
    assert_eq!(count(&r.schema, empty).await, 0);
    assert_eq!(readback(&r.fresh()).await, before);
}

/// Objects of recent revisions stay one decode away (plain frames in hot
/// packs, or Postgres rows); once newer commits make them cold, the next
/// incremental run delta-compresses them, and leaves alone what is still
/// hot.
#[tokio::test]
#[ignore = "needs postgres and s3: docker compose up -d"]
async fn hot_objects_are_repacked_once_they_go_cold() {
    let r = repo().await;
    let b = &r.b;
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
    let before = readback(b).await;
    let o = RepackOptions { keep_recent: 1, ..options(&r) };
    let first = r.repack(&o).await;
    assert!(first.finished && first.selected_hot > 0, "{first:?}");
    let in_hot_pack = |id: &ObjectId| {
        format!(
            "SELECT count(*) FROM object_index x JOIN packs p USING (pack) \
             WHERE x.id = {} AND p.hot",
            lit(id)
        )
    };
    let newest = versions[5];
    assert_eq!(count(&r.schema, &in_hot_pack(&newest)).await, 1, "the tip's version is hot");
    assert_eq!(count(&r.schema, &in_hot_pack(&versions[0])).await, 0, "older ones are not");
    // Still hot: an incremental run leaves it.
    let again = r.repack(&o).await;
    assert_eq!((again.moved, again.selected_hot), (0, 0), "{again:?}");
    // A newer version makes it cold; the next run moves it into a delta pack.
    edits.push((100, 100));
    let c = commit_ops(b, tip, vec![write(path, &text(path, 300, &edits))]).await;
    land_tip(b, "main", c).await;
    let after_land = readback(b).await;
    assert!(before.keys().all(|id| after_land.contains_key(id)));
    let before = after_land;
    let next = r.repack(&o).await;
    assert!(next.moved >= 1 && next.selected_cold >= 1, "{next:?}");
    assert_eq!(count(&r.schema, &in_hot_pack(&newest)).await, 0, "no longer in a hot pack");
    assert_eq!(unrepacked(&r.schema).await, 0);
    assert_eq!(readback(&r.fresh()).await, before);
}

/// Interrupted after a few packs (each one switched as it is written), a
/// repack leaves a readable store; the next run continues, and nothing is
/// moved twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs postgres and s3: docker compose up -d"]
async fn interrupted_repack_resumes() {
    let r = repo().await;
    let mut rng = StdRng::seed_from_u64(3);
    random_history(&r.b, &mut rng, 50).await;
    let before = readback(&r.b).await;
    let o = RepackOptions {
        pack_bytes: 2000,
        stop_after_packs: Some(2),
        jobs: 1,
        cluster: ClusterOptions { cluster_raw: 1500, ..options(&r).cluster },
        ..options(&r)
    };
    let first = r.repack(&o).await;
    assert!(!first.finished && first.packs >= 2 && first.moved > 0, "{first:?}");
    assert!(unrepacked(&r.schema).await > 0);
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
    let total = count(&r.schema, "SELECT count(*) FROM object_index").await;
    assert_eq!(unrepacked(&r.schema).await, 0);
    assert_eq!(moved as i64, total, "every packed object moved exactly once");
    assert_eq!(readback(&r.fresh()).await, before);
}

/// Writers land changesets (some re-storing old content), readers read
/// everything reachable, collections and repacks run, all at once. No read
/// may fail or miss a reachable object, and everything reachable at the end
/// is intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs postgres and s3: docker compose up -d"]
async fn repack_races_writers_readers_and_collections() {
    let r = Arc::new(repo().await);
    ant_server::ops::set_cache_max_age(Duration::from_millis(200));
    let mut rng = StdRng::seed_from_u64(5);
    random_history(&r.b, &mut rng, 40).await;
    // Garbage the collections will delete: abandoned changesets.
    let tip = r.b.refs.get_ref("main").await.unwrap();
    let mut garbage = Vec::new();
    for i in 0..6 {
        let content = text(&format!("junk{i}"), 50, &[(i, i as u32)]);
        garbage.push(commit_ops(&r.b, tip, vec![write(&format!("junk/{i}"), &content)]).await);
    }
    age(&r.schema, 30).await;

    let stop = Arc::new(AtomicBool::new(false));
    let (lands, reads, repacks) = (
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    let mut tasks = Vec::new();
    for w in 0..3usize {
        let (r, stop, lands) = (r.clone(), stop.clone(), lands.clone());
        tasks.push(tokio::spawn(async move {
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                let tip = r.b.refs.get_ref("main").await.unwrap();
                let path = format!("src/m{}/f{}.rs", w % 3, (w as u32 * 4 + i) % 12);
                let content = text(&path, 40 + (i as usize % 3) * 40, &[(i as usize % 40, i)]);
                let c = commit_ops(&r.b, tip, vec![write(&path, &content)]).await;
                loop {
                    match land(&r.b, "main", c).await {
                        Ok(_) => break,
                        Err(OpError::Aborted(_)) => {
                            tokio::time::sleep(Duration::from_millis(10)).await
                        }
                        Err(e) => panic!("land: {e}"),
                    }
                }
                lands.fetch_add(1, Ordering::Relaxed);
                // Enough history to race over, not so much that marking it
                // takes the test's time.
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }));
    }
    for _ in 0..2 {
        let (r, stop, reads) = (r.clone(), stop.clone(), reads.clone());
        tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                // A server that just started (nothing cached) and a warm one.
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
                    ..options(&r)
                };
                let report = r.repack(&o).await;
                assert!(report.finished, "{report:?}");
                repacks.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // Packs are indexed right after their upload (writers and repacks
    // alike): a pack younger than `pack_min_age` may still be about to get
    // its rows (the default is the grace period; seconds suffice here).
    let gc = GcOptions {
        quiesce: Duration::from_secs(2),
        batch: 5,
        s3: Some(r.s3.clone()),
        pack_min_age: Some(Duration::from_secs(5)),
        ..GcOptions::default()
    };
    let mut collected = 0;
    let mut packs = 0;
    for round in 0..4 {
        if round > 0 {
            // Old packs repacks moved rows out of age past the minimum.
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        let report = r.pg.gc(r.b.objects.as_ref(), &gc, &|_| {}).await.unwrap();
        packs += report.packs;
        assert!(report.finished, "{report:?}");
        collected += report.objects + report.large_objects + report.commits;
    }
    // Let a repack and some reads run after the collections too.
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
        "lands {}, readbacks {}, repacks {}, collected {collected}, packs deleted {packs}",
        lands.load(Ordering::Relaxed),
        reads.load(Ordering::Relaxed),
        repacks.load(Ordering::Relaxed)
    );
    assert!(collected > 0 && packs > 0 && repacks.load(Ordering::Relaxed) >= 2);
    let after = readback(&r.fresh()).await;
    for c in &garbage {
        assert!(!after.contains_key(c));
    }
    // Abandoned changesets are collected, never resurrected by a repack (a
    // row a repack held locked during a sweep is kept until the next run).
    let report = r.pg.gc(r.b.objects.as_ref(), &gc, &|_| {}).await.unwrap();
    assert!(report.finished, "{report:?}");
    assert_eq!(readback(&r.fresh()).await, after);
    for c in &garbage {
        let n = count(
            &r.schema,
            &format!(
                "SELECT (SELECT count(*) FROM objects WHERE id = {0}) \
                  + (SELECT count(*) FROM object_index WHERE id = {0})",
                lit(c)
            ),
        )
        .await;
        assert_eq!(n, 0, "garbage {c} survived");
    }
    let ids: HashSet<ObjectId> = after.keys().copied().collect();
    assert!(ids.len() > 50);
}
