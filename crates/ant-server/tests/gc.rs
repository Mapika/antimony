//! Garbage collection (`ant_store::postgres::gc`) against the real write
//! paths: what it deletes, that everything reachable survives (full
//! read-back), the grace period, dry runs, interruption, S3 packs, and a
//! fleet of writers racing a collection.
//!
//! Runs with `--run-ignored all` (Postgres; S3 for the pack test).

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use ant_core::{EncodedObject, EntryMode, Object, ObjectId, ObjectKind, RepoPath, Signature};
use ant_server::ops::changeset::{ChangesetRequest, FileOp, WriteContent, create_changeset};
use ant_server::ops::land::{LandOutcome, land, rebase};
use ant_server::ops::{OpError, history, query};
use ant_store::Backend;
use ant_store::packed::PackedS3Store;
use ant_store::postgres::PgStore;
use ant_store::postgres::gc::{GcOptions, GcReport};
use ant_store::s3::{S3Config, S3ObjectStore};
use ant_store::tiered::TieredObjectStore;
use bytes::Bytes;
use sqlx::Connection;

use ant_store::testing::pg_url;

struct Repo {
    b: Backend,
    pg: Arc<PgStore>,
    schema: String,
}

async fn repo() -> Repo {
    // These tests collect with no quiesce wait, so the server caches (shared
    // with the collector in this process) must not outlive it either.
    ant_server::ops::set_cache_max_age(Duration::ZERO);
    let schema = format!("t_gc_{}", rand::random::<u64>());
    let pg = Arc::new(PgStore::connect_scratch(&pg_url(), &schema).await.unwrap());
    let b = Backend { objects: pg.clone(), refs: pg.clone(), meta: pg.clone() };
    Repo { b, pg, schema }
}

/// Deletes an S3 prefix when dropped (also on panic).
struct S3Guard(S3Config);

impl Drop for S3Guard {
    fn drop(&mut self) {
        let cfg = self.0.clone();
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let client = s3_client(&cfg);
                for key in list_keys(&client, &cfg).await {
                    let _ = client.delete_object().bucket(&cfg.bucket).key(key).send().await;
                }
            });
        })
        .join();
    }
}

fn s3_client(cfg: &S3Config) -> aws_sdk_s3::Client {
    let creds =
        aws_sdk_s3::config::Credentials::new(&cfg.access_key, &cfg.secret_key, None, None, "t");
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new(cfg.region.clone()))
        .endpoint_url(&cfg.endpoint)
        .credentials_provider(creds)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

async fn list_keys(client: &aws_sdk_s3::Client, cfg: &S3Config) -> Vec<String> {
    let mut keys = Vec::new();
    let mut token = None;
    loop {
        let page = client
            .list_objects_v2()
            .bucket(&cfg.bucket)
            .prefix(format!("{}/", cfg.prefix))
            .set_continuation_token(token)
            .send()
            .await
            .unwrap();
        keys.extend(page.contents().iter().filter_map(|o| o.key().map(str::to_owned)));
        match page.next_continuation_token() {
            Some(t) if page.is_truncated() == Some(true) => token = Some(t.to_owned()),
            _ => return keys,
        }
    }
}

/// A repository whose objects larger than 64 bytes go to S3 packs.
async fn repo_s3() -> (Repo, S3ObjectStore, S3Guard) {
    let Repo { pg, schema, .. } = repo().await;
    let cfg = S3Config::for_tests(&format!("gc{}", rand::random::<u64>()));
    let guard = S3Guard(cfg.clone());
    let s3 = S3ObjectStore::connect(&cfg).await.unwrap();
    s3.ensure_bucket().await.unwrap();
    let packed = PackedS3Store::new(pg.as_ref().clone(), s3.clone());
    let objects = Arc::new(TieredObjectStore::new(pg.clone(), Arc::new(packed), 64));
    let b = Backend { objects, refs: pg.clone(), meta: pg.clone() };
    (Repo { b, pg, schema }, s3, guard)
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

/// Rows anywhere (Postgres objects, packed index, commit records) for `id`.
async fn stored(schema: &str, id: &ObjectId) -> i64 {
    let lit = format!("'\\x{}'::bytea", id.to_hex());
    count(
        schema,
        &format!(
            "SELECT (SELECT count(*) FROM objects WHERE id = {lit}) \
                  + (SELECT count(*) FROM object_index WHERE id = {lit}) \
                  + (SELECT count(*) FROM commits WHERE id = {lit})"
        ),
    )
    .await
}

fn sig() -> Signature {
    Signature { name: "gc".into(), email: "gc@x".into(), time: 1_700_000_000, tz_offset_minutes: 0 }
}

async fn changeset(b: &Backend, base: Option<ObjectId>, files: &[(&str, &[u8])]) -> ObjectId {
    let ops = files
        .iter()
        .map(|(p, c)| FileOp::Write {
            path: RepoPath::new(*p).unwrap(),
            mode: EntryMode::File,
            content: WriteContent::Inline(Bytes::copy_from_slice(c)),
        })
        .collect();
    let req = ChangesetRequest { base, ops, author: sig(), committer: None, message: "m".into() };
    create_changeset(b, req).await.unwrap()
}

async fn land_tip(b: &Backend, r: &str, c: ObjectId) -> ObjectId {
    match land(b, r, c).await.unwrap() {
        LandOutcome::Landed { new_tip, .. } => new_tip,
        other => panic!("{other:?}"),
    }
}

/// Every object reachable from every ref (commits through their parents,
/// trees through their entries), with its bytes, after checking that each
/// commit's record and changeset are readable too. Panics on anything
/// missing.
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

fn opts() -> GcOptions {
    GcOptions { quiesce: Duration::ZERO, batch: 7, ..GcOptions::default() }
}

async fn gc(r: &Repo, o: &GcOptions) -> GcReport {
    r.pg.gc(r.b.objects.as_ref(), o, &|_| {}).await.unwrap()
}

/// History on `main` (nested paths, a rebased land with its landed-as
/// mapping), another ref, and the history index. Returns main's tip, the
/// rebased land's source commit and the files at the tip.
async fn history(b: &Backend, n: usize) -> (ObjectId, ObjectId) {
    let mut tip = land_tip(b, "main", changeset(b, None, &[("README", b"hello\n")]).await).await;
    for i in 0..n {
        let path = format!("src/m{}/f{i}.rs", i % 3);
        let c = changeset(b, Some(tip), &[(&path, format!("fn f{i}() {{}}\n").as_bytes())]).await;
        tip = land_tip(b, "main", c).await;
    }
    // Two changesets on one base: the second is rebased when it lands.
    let a = changeset(b, Some(tip), &[("docs/a.md", b"a\n")]).await;
    let source = changeset(b, Some(tip), &[("docs/b.md", b"b\n")]).await;
    land_tip(b, "main", a).await;
    tip = land_tip(b, "main", source).await;
    assert_ne!(tip, source);
    b.refs.cas_ref("release", None, Some(tip)).await.unwrap();
    history::catch_up(b, "main").await.unwrap();
    (tip, source)
}

#[tokio::test]
#[ignore = "needs postgres: docker compose up -d"]
async fn gc_collects_aborted_and_abandoned_data_and_keeps_all_history() {
    let r = repo().await;
    let b = &r.b;
    let (tip, source) = history(b, 12).await;
    let mut garbage = Vec::new();
    // Abandoned changesets (never landed).
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
    // Uploads nothing referenced.
    let stray = EncodedObject::new(ObjectKind::Blob, b"uploaded, never used\n");
    b.objects.put_many(std::slice::from_ref(&stray)).await.unwrap();
    garbage.push(stray.id());
    for c in [side, other] {
        let tree = b.meta.get_commits(&[c]).await.unwrap()[0].clone().unwrap().tree;
        garbage.push(tree);
    }

    let before = readback(b).await;
    age(&r.schema, 30).await;
    let report = gc(&r, &opts()).await;
    assert!(report.finished, "{report:?}");
    assert!(report.commits >= 7 && report.objects >= 14, "{report:?}");
    assert_eq!(report.kept, 0, "{report:?}");
    for id in &garbage {
        assert_eq!(stored(&r.schema, id).await, 0, "garbage {id} survived");
    }
    // Everything reachable is intact, byte for byte, with records.
    assert_eq!(readback(b).await, before);
    // The landed-as mapping's source stays: retrying that land is a no-op.
    assert!(b.meta.get_commits(&[source]).await.unwrap()[0].is_some());
    assert_eq!(land_tip(b, "main", source).await, tip);
    // Path history and blame still work, from the index.
    let log = query::log(b, "main", Some(&RepoPath::new("src/m1").unwrap()), 0).await.unwrap();
    assert_eq!(log.len(), 4);
    history::blame(b, "main", &RepoPath::new("src/m0/f0.rs").unwrap()).await.unwrap();
    // Writing goes on.
    let c = changeset(b, Some(tip), &[("after/gc", b"x\n")]).await;
    land_tip(b, "main", c).await;
    // Unlanded changesets older than the grace period are gone for good.
    assert!(matches!(land(b, "main", garbage[0]).await, Err(OpError::NotFound(_))));
}

#[tokio::test]
#[ignore = "needs postgres: docker compose up -d"]
async fn gc_keeps_everything_written_within_the_grace_period() {
    let r = repo().await;
    let b = &r.b;
    let (tip, _) = history(b, 3).await;
    let old = changeset(b, Some(tip), &[("old", b"old\n")]).await;
    age(&r.schema, 3).await;
    let recent = changeset(b, Some(tip), &[("recent", b"recent\n")]).await;
    // 7 days: neither is old enough.
    let report = gc(&r, &opts()).await;
    assert_eq!((report.condemned, report.commits), (0, 0), "{report:?}");
    assert!(stored(&r.schema, &old).await > 0);
    // 2 days: the 3-day-old changeset goes, the recent one stays.
    let report = gc(&r, &GcOptions { grace: Duration::from_secs(2 * 86400), ..opts() }).await;
    assert_eq!(report.commits, 1, "{report:?}");
    assert_eq!(stored(&r.schema, &old).await, 0);
    assert!(stored(&r.schema, &recent).await > 0);
    readback(b).await;
}

/// Row counts and content digests of every table GC may touch.
async fn fingerprint(schema: &str) -> Vec<(String, i64, String)> {
    let mut out = Vec::new();
    for (t, key) in [
        ("objects", "id"),
        ("object_index", "id"),
        ("commits", "id"),
        ("changesets", "commit_id"),
        ("log", "commit_id"),
        ("changes", "commit_id"),
        ("derived", "key"),
        ("refs", "target"),
        ("history", "commit_id"),
        ("gc_condemned", "id"),
        ("gc_state", "phase::bytea"),
        ("gc_live", "id"),
    ] {
        let mut c = conn(schema).await;
        let (n, digest): (i64, Option<String>) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT count(*), md5(string_agg(encode({key}, 'hex'), ',' ORDER BY {key})) FROM {t}"
        )))
        .fetch_one(&mut c)
        .await
        .unwrap();
        c.close().await.unwrap();
        out.push((t.to_owned(), n, digest.unwrap_or_default()));
    }
    out
}

#[tokio::test]
#[ignore = "needs postgres: docker compose up -d"]
async fn gc_dry_run_changes_nothing_and_predicts_the_run() {
    let r = repo().await;
    let b = &r.b;
    let (tip, _) = history(b, 4).await;
    for i in 0..3 {
        changeset(b, Some(tip), &[(&format!("x/{i}"), format!("{i}\n").as_bytes())]).await;
    }
    age(&r.schema, 30).await;
    let before = fingerprint(&r.schema).await;
    let dry = gc(&r, &GcOptions { dry_run: true, ..opts() }).await;
    assert!(dry.dry_run && dry.finished);
    assert_eq!(fingerprint(&r.schema).await, before);
    assert!(dry.condemned > 0 && dry.commits == 3, "{dry:?}");
    let run = gc(&r, &opts()).await;
    assert_eq!(
        (run.condemned, run.objects, run.object_bytes, run.commits, run.changesets),
        (dry.condemned, dry.objects, dry.object_bytes, dry.commits, dry.changesets)
    );
    assert_eq!(run.live, dry.live);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs postgres: docker compose up -d"]
async fn gc_resumes_after_interruption() {
    let r = repo().await;
    let b = &r.b;
    let (tip, _) = history(b, 4).await;
    let mut garbage = Vec::new();
    for i in 0..20 {
        garbage.push(
            changeset(b, Some(tip), &[(&format!("g/{i}"), format!("{i}\n").as_bytes())]).await,
        );
    }
    let before = readback(b).await;
    age(&r.schema, 30).await;

    // Interrupted while waiting between condemning and deleting: the future
    // is dropped mid-run, as on Ctrl-C.
    let slow = GcOptions { quiesce: Duration::from_secs(2), batch: 5, ..opts() };
    let run = gc(&r, &slow);
    tokio::select! {
        report = run => panic!("finished early: {report:?}"),
        _ = async {
            while count(&r.schema, "SELECT count(*) FROM gc_state WHERE phase = 'quiesce'").await == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        } => {}
    }
    // Condemned data reads as missing meanwhile; reachable data does not.
    assert!(count(&r.schema, "SELECT count(*) FROM gc_condemned").await > 0);
    assert!(b.meta.get_commits(&[garbage[0]]).await.unwrap()[0].is_none());
    assert_eq!(readback(b).await, before);

    // Resumed, then stopped after two delete batches.
    let part = gc(&r, &GcOptions { stop_after_batches: Some(2), ..slow.clone() }).await;
    assert!(part.resumed && !part.finished, "{part:?}");
    let deleted = part.commits;
    assert!(deleted > 0 && deleted < 20, "{part:?}");
    // Resumed again: the rest, with totals over all parts.
    let rest = gc(&r, &slow).await;
    assert!(rest.resumed && rest.finished, "{rest:?}");
    assert_eq!(rest.commits, 20, "{rest:?}");
    for id in &garbage {
        assert_eq!(stored(&r.schema, id).await, 0);
    }
    assert_eq!(readback(b).await, before);
    // A fresh run afterwards starts from scratch.
    let next = gc(&r, &opts()).await;
    assert!(!next.resumed && next.commits == 0, "{next:?}");

    // Aborting a pending run makes its condemned data visible again.
    let old = changeset(b, Some(tip), &[("late", b"late\n")]).await;
    age(&r.schema, 30).await;
    let part = gc(&r, &GcOptions { stop_after_batches: Some(0), ..opts() }).await;
    assert!(!part.finished);
    assert!(b.meta.get_commits(&[old]).await.unwrap()[0].is_none());
    assert!(r.pg.gc_abort().await.unwrap());
    assert!(b.meta.get_commits(&[old]).await.unwrap()[0].is_some());
    assert!(!r.pg.gc_abort().await.unwrap());
}

#[tokio::test]
#[ignore = "needs postgres and s3: docker compose up -d"]
async fn gc_deletes_dead_packs_and_reports_dead_bytes() {
    let (r, s3, _guard) = repo_s3().await;
    let b = &r.b;
    let big = |tag: &str| format!("{tag}\n{}", "0123456789abcdef".repeat(64)).into_bytes();
    let (tip, _) = history(b, 3).await;
    let live = changeset(b, Some(tip), &[("big/live", &big("live"))]).await;
    let tip = land_tip(b, "main", live).await;
    let mut dead = Vec::new();
    for i in 0..3 {
        dead.push(
            changeset(b, Some(tip), &[(&format!("big/dead{i}"), &big(&format!("dead{i}")))]).await,
        );
    }
    // One upload holding a blob that gets used and one that does not: its
    // pack stays, partly dead.
    let used = EncodedObject::new(ObjectKind::Blob, &big("used"));
    let unused = EncodedObject::new(ObjectKind::Blob, &big("unused"));
    b.objects.put_many(&[used.clone(), unused.clone()]).await.unwrap();
    let ops = vec![FileOp::Write {
        path: RepoPath::new("big/used").unwrap(),
        mode: EntryMode::File,
        content: WriteContent::Blob(used.id()),
    }];
    let c = create_changeset(
        b,
        ChangesetRequest {
            base: Some(tip),
            ops,
            author: sig(),
            committer: None,
            message: "u".into(),
        },
    )
    .await
    .unwrap();
    land_tip(b, "main", c).await;

    let packs_before = count(&r.schema, "SELECT count(DISTINCT pack) FROM object_index").await;
    let before = readback(b).await;
    age(&r.schema, 30).await;
    let o = GcOptions { s3: Some(s3.clone()), pack_min_age: Some(Duration::ZERO), ..opts() };
    let dry = gc(&r, &GcOptions { dry_run: true, ..o.clone() }).await;
    assert!(dry.packs >= 3, "{dry:?}");
    let report = gc(&r, &o).await;
    assert_eq!((report.packs, report.pack_bytes), (dry.packs, dry.pack_bytes), "{report:?}");
    assert!(report.large_objects >= 4, "{report:?}");
    assert!(report.packs_dead_bytes > 0 && report.pack_dead_ratio() > 0.0, "{report:?}");
    assert_eq!(readback(b).await, before);
    for c in &dead {
        assert_eq!(stored(&r.schema, c).await, 0);
    }
    assert_eq!(stored(&r.schema, &unused.id()).await, 0);
    let packs_after = count(&r.schema, "SELECT count(DISTINCT pack) FROM object_index").await;
    assert_eq!(packs_after, packs_before - report.packs as i64);
    // Nothing dead is left to delete; S3 holds exactly the indexed packs.
    let again = gc(&r, &o).await;
    assert_eq!(again.packs, 0, "{again:?}");
    assert_eq!(again.packs_kept as i64, packs_after);
}

/// Versions of one file uploaded together share a multi-object frame. When
/// most of them are collected, the ones still referenced stay readable from
/// the same frame (no member depends on another's index row), the pack stays,
/// and the collected members' shares are reported as dead bytes.
#[tokio::test]
#[ignore = "needs postgres and s3: docker compose up -d"]
async fn gc_keeps_live_members_of_a_frame_whose_other_members_die() {
    let (r, s3, _guard) = repo_s3().await;
    let b = &r.b;
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
    let lits: Vec<String> =
        versions.iter().map(|v| format!("'\\x{}'::bytea", v.id().to_hex())).collect();
    let frames = count(
        &r.schema,
        &format!(
            "SELECT count(*) FROM (SELECT DISTINCT pack, off FROM object_index \
             WHERE share IS NOT NULL AND id IN ({})) f",
            lits.join(", ")
        ),
    )
    .await;
    assert_eq!(frames, 1, "the versions share one frame");
    // The first and the last version are used; the others are not.
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
    age(&r.schema, 30).await;
    let o = GcOptions { s3: Some(s3.clone()), pack_min_age: Some(Duration::ZERO), ..opts() };
    let report = gc(&r, &o).await;
    assert!(report.finished, "{report:?}");
    for v in &versions[1..11] {
        assert_eq!(stored(&r.schema, &v.id()).await, 0, "unused version survived");
    }
    for id in keep {
        assert_eq!(stored(&r.schema, &id).await, 1);
    }
    assert!(report.large_objects >= 10, "{report:?}");
    assert!(report.packs_dead_bytes > 0, "{report:?}");
    assert_eq!(readback(b).await, before);
    // A store with nothing cached decodes the frame from S3 again.
    let fresh = PackedS3Store::new(r.pg.as_ref().clone(), s3.clone());
    let fresh: Arc<dyn ant_store::ObjectStore> =
        Arc::new(TieredObjectStore::new(r.pg.clone(), Arc::new(fresh), 64));
    let got = fresh.get_many(&keep).await.unwrap();
    assert_eq!(got, vec![Some(versions[0].clone()), Some(versions[11].clone())]);
    let gone = fresh.get_many(&[versions[5].id()]).await.unwrap();
    assert_eq!(gone, vec![None]);
    // The frame's pack is not deleted while live members point into it.
    let again = gc(&r, &o).await;
    assert_eq!(again.packs, 0, "{again:?}");
    assert_eq!(fresh.get_many(&keep).await.unwrap().len(), 2);
}

/// `land`, retried while it is `Aborted` (as clients do): a batch that
/// meets data condemned while it replays fails as a whole, retryably.
async fn land_retrying(b: &Backend, r: &str, c: ObjectId) -> Result<LandOutcome, OpError> {
    for _ in 0..20 {
        match land(b, r, c).await {
            Err(OpError::Aborted(_)) => tokio::time::sleep(Duration::from_millis(20)).await,
            other => return other,
        }
    }
    land(b, r, c).await
}

/// Writers keep creating and landing changesets, land changesets that are
/// older than the grace period, store blobs identical to old garbage again
/// and point refs at old commits, all while collections run. Nothing
/// reachable afterwards may be missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs postgres: docker compose up -d"]
async fn gc_races_a_fleet_of_writers_without_losing_reachable_data() {
    let r = repo().await;
    // Caches forget faster than the collector's quiesce wait.
    ant_server::ops::set_cache_max_age(Duration::from_millis(200));
    let b = r.b.clone();
    let (tip, _) = history(&b, 6).await;
    // Old garbage the writers will try to bring back.
    let mut old_changesets = Vec::new();
    let mut old_blobs = Vec::new();
    for i in 0..24 {
        let content = format!("old garbage {i}\n").into_bytes();
        old_changesets.push(changeset(&b, Some(tip), &[(&format!("old/c{i}"), &content)]).await);
        let blob = EncodedObject::new(ObjectKind::Blob, format!("old blob {i}\n").as_bytes());
        b.objects.put_many(std::slice::from_ref(&blob)).await.unwrap();
        old_blobs.push(blob);
    }
    // Old garbage nobody touches. The writers can bring back all of theirs
    // before a collection marks (they cycle through it within a few dozen
    // operations each), so only this is certain to be deleted.
    let mut untouched = Vec::new();
    for i in 0..4 {
        let content = format!("untouched {i}\n").into_bytes();
        untouched.push(changeset(&b, Some(tip), &[(&format!("untouched/c{i}"), &content)]).await);
    }
    let stray = EncodedObject::new(ObjectKind::Blob, b"untouched blob\n");
    b.objects.put_many(std::slice::from_ref(&stray)).await.unwrap();
    untouched.push(stray.id());
    age(&r.schema, 30).await;

    let stop = Arc::new(AtomicBool::new(false));
    let (landed_old, reused, pointed, lands) = (
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    let mut writers = Vec::new();
    for w in 0..6usize {
        let (b, stop) = (b.clone(), stop.clone());
        let (old_changesets, old_blobs) = (old_changesets.clone(), old_blobs.clone());
        let (landed_old, reused, pointed, lands) =
            (landed_old.clone(), reused.clone(), pointed.clone(), lands.clone());
        writers.push(tokio::spawn(async move {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                let k = (w * 7 + i) % old_changesets.len();
                let tip = b.refs.get_ref("main").await.unwrap().unwrap();
                match i % 4 {
                    // Land an old changeset (rebased onto main), if still there.
                    0 => match land_retrying(&b, "main", old_changesets[k]).await {
                        Ok(LandOutcome::Landed { .. }) => {
                            landed_old.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(LandOutcome::Conflict { .. }) | Err(OpError::NotFound(_)) => {}
                        Err(e) => panic!("land old: {e}"),
                    },
                    // Store an old garbage blob again and use it by id.
                    1 => {
                        let blob = &old_blobs[k];
                        b.objects.put_many(std::slice::from_ref(blob)).await.unwrap();
                        let ops = vec![FileOp::Write {
                            path: RepoPath::new(format!("reused/w{w}/b{k}")).unwrap(),
                            mode: EntryMode::File,
                            content: WriteContent::Blob(blob.id()),
                        }];
                        let req = ChangesetRequest {
                            base: Some(tip),
                            ops,
                            author: sig(),
                            committer: None,
                            message: "reuse".into(),
                        };
                        let c = create_changeset(&b, req).await.unwrap();
                        land_retrying(&b, "main", c).await.unwrap();
                        reused.fetch_add(1, Ordering::Relaxed);
                    }
                    // Point a ref at an old commit that still reads as present.
                    2 => {
                        let target = old_changesets[k];
                        if b.meta.get_commits(&[target]).await.unwrap()[0].is_some() {
                            let name = format!("ws/w{w}/old");
                            let cur = b.refs.get_ref(&name).await.unwrap();
                            b.refs.cas_ref(&name, cur, Some(target)).await.unwrap();
                            pointed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    // Ordinary work.
                    _ => {
                        let path = format!("work/w{w}/f{}", i % 5);
                        let c =
                            changeset(&b, Some(tip), &[(&path, format!("{i}\n").as_bytes())]).await;
                        // The writer's own ref follows main (fast-forward).
                        if let LandOutcome::Landed { new_tip, .. } =
                            land_retrying(&b, "main", c).await.unwrap()
                        {
                            land_retrying(&b, &format!("ws/w{w}/main"), new_tip).await.unwrap();
                        }
                        lands.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    // Only data aged before the writers started is old: aging rows while
    // writers use them would break the grace period's premise.
    let o = GcOptions { quiesce: Duration::from_secs(1), batch: 3, ..opts() };
    let mut total = 0;
    for _ in 0..2 {
        let report = gc(&r, &o).await;
        assert!(report.finished, "{report:?}");
        eprintln!(
            "collection: condemned {}, deleted {} objects and {} commits, kept {}",
            report.condemned, report.objects, report.commits, report.kept
        );
        total += report.objects + report.commits;
    }
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.await.unwrap();
    }
    assert!(total > 0, "the collections deleted nothing");
    for id in &untouched {
        assert_eq!(stored(&r.schema, id).await, 0, "untouched garbage {id} was kept");
    }
    assert!(lands.load(Ordering::Relaxed) > 0 && reused.load(Ordering::Relaxed) > 0);
    eprintln!(
        "deleted {total}; landed old {}, reused blobs {}, pointed refs {}, lands {}",
        landed_old.load(Ordering::Relaxed),
        reused.load(Ordering::Relaxed),
        pointed.load(Ordering::Relaxed),
        lands.load(Ordering::Relaxed)
    );
    // Everything reachable from every ref is complete.
    let reachable = readback(&b).await;
    // And a final collection with nobody writing keeps it so.
    let report = gc(&r, &o).await;
    assert!(report.finished);
    assert_eq!(readback(&b).await, reachable);
    let refs: HashSet<ObjectId> =
        b.refs.list_refs("").await.unwrap().into_iter().map(|(_, id)| id).collect();
    assert!(refs.iter().all(|id| reachable.contains_key(id)));
}
