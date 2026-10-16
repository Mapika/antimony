//! Upgrade safety: a store written in the oldest layout still read (the
//! baseline of the Postgres migrations) opens with the current code, reads
//! back exactly, takes new writes and records its version; a store from a
//! newer version is refused.
//!
//! The old store is written by SQL fixtures mirroring the code before
//! schema versioning, S3 packs and one-row changesets: no `schema_version`,
//! `object_index` or `changesets` tables, per-path `changes` rows, large
//! objects one per S3 key (`S3ObjectStore`'s layout), small objects
//! zstd-compressed in `objects`. Nothing here goes through the code under
//! test to build the fixture.

mod common;

use std::collections::BTreeMap;

use ant_core::{Commit, EncodedObject, EntryMode, Object, ObjectId, ObjectKind, RepoPath};
use ant_server::config::StoreArgs;
use ant_server::ops::changeset::{ChangesetRequest, FileOp, WriteContent, create_changeset};
use ant_server::ops::land::{LandOutcome, land};
use ant_server::ops::{history, query};
use ant_store::postgres::{MIGRATIONS, PgStore, SCHEMA_VERSION, SchemaGuard};
use ant_store::s3::{S3Config, S3ObjectStore};
use ant_store::{Backend, ChangeKind, ObjectStore, PathChange, StoreError};
use bytes::Bytes;
use clap::Parser;
use common::pg::pg_url;
use common::{Model, build, nest, sig};
use sqlx::Connection;

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    store: StoreArgs,
}

/// `ant serve` / `ant import` store flags for the fixture's schema and prefix.
fn store_args(schema: &str, s3: &S3Config, extra: &[&str]) -> StoreArgs {
    let mut argv = vec![
        "ant".to_string(),
        "--pg-url".into(),
        pg_url(),
        "--pg-schema".into(),
        schema.into(),
        "--pg-max-connections".into(),
        "4".into(),
        "--s3-endpoint".into(),
        s3.endpoint.clone(),
        "--s3-bucket".into(),
        s3.bucket.clone(),
        "--s3-prefix".into(),
        s3.prefix.clone(),
        "--s3-access-key".into(),
        s3.access_key.clone(),
        "--s3-secret-key".into(),
        s3.secret_key.clone(),
    ];
    argv.extend(extra.iter().map(|s| s.to_string()));
    Cli::try_parse_from(argv).unwrap().store
}

/// The DDL of the last release before versioning (mirrors `PgStore::connect`
/// at that time, `CREATE ... IF NOT EXISTS` included).
fn old_ddl(qs: &str) -> Vec<String> {
    vec![
        format!("CREATE SCHEMA IF NOT EXISTS {qs}"),
        format!(
            "CREATE TABLE IF NOT EXISTS {qs}.objects (id bytea PRIMARY KEY, data bytea NOT NULL)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {qs}.refs (name text PRIMARY KEY, target bytea NOT NULL)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {qs}.commits (id bytea PRIMARY KEY, tree bytea NOT NULL, \
             parents bytea NOT NULL, generation bigint NOT NULL, committer_time bigint NOT NULL)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {qs}.log (seq bigserial PRIMARY KEY, \
             commit_id bytea NOT NULL UNIQUE)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {qs}.changes (commit_id bytea NOT NULL, idx integer NOT NULL, \
             path text NOT NULL, kind smallint NOT NULL, old_mode text, old_id bytea, \
             new_mode text, new_id bytea, PRIMARY KEY (commit_id, idx))"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {qs}.derived (kind text NOT NULL, key bytea NOT NULL, \
             value bytea NOT NULL, PRIMARY KEY (kind, key))"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {qs}.history (ref text NOT NULL, path text NOT NULL, \
             generation bigint NOT NULL, commit_id bytea NOT NULL, \
             PRIMARY KEY (ref, path, generation, commit_id))"
        ),
        format!("CREATE INDEX IF NOT EXISTS history_by_gen ON {qs}.history (ref, generation DESC)"),
        format!(
            "CREATE INDEX IF NOT EXISTS history_by_path ON {qs}.history \
             (ref, path text_pattern_ops, generation DESC)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {qs}.history_watermark (ref text PRIMARY KEY, \
             commit_id bytea NOT NULL)"
        ),
    ]
}

/// Large enough for S3 (encoded size above the 4 KiB inline threshold).
fn big(seed: u32) -> Vec<u8> {
    let mut x = 0x9E37_79B9u32.wrapping_mul(seed + 1);
    (0..20_000)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x as u8
        })
        .collect()
}

fn file(content: &[u8]) -> (EntryMode, Vec<u8>) {
    (EntryMode::File, content.to_vec())
}

fn blob_id(content: &[u8]) -> ObjectId {
    ant_core::hash_object(ObjectKind::Blob, content)
}

fn change(path: &str, kind: ChangeKind, old: Option<&[u8]>, new: Option<&[u8]>) -> PathChange {
    PathChange {
        path: RepoPath::new(path).unwrap(),
        kind,
        old: old.map(|c| (EntryMode::File, blob_id(c))),
        new: new.map(|c| (EntryMode::File, blob_id(c))),
    }
}

/// One commit of the fixture history.
struct Fixture {
    id: ObjectId,
    generation: i64,
    changes: Vec<PathChange>,
    tree: ObjectId,
    parent: Option<ObjectId>,
    time: i64,
}

/// Everything the old code would have stored for a three-commit `main` (and
/// a `feature` ref at the second commit), with `main`'s history index built
/// up to the second commit.
struct OldStore {
    commits: Vec<Fixture>,
    objects: Vec<EncodedObject>,
    models: Vec<Model>,
}

fn old_history() -> OldStore {
    let lib1: &[u8] = b"fn a() {}\n";
    let lib2: &[u8] = b"fn a() {}\nfn b() {}\n";
    let m1: Model = BTreeMap::from([
        ("README.md".to_string(), file(b"hello\n")),
        ("src/lib.rs".to_string(), file(lib1)),
        ("src/big.bin".to_string(), file(&big(1))),
    ]);
    let mut m2 = m1.clone();
    m2.insert("src/lib.rs".into(), file(lib2));
    let mut m3 = m2.clone();
    m3.remove("README.md");
    m3.insert("docs/guide.md".into(), file(b"# guide\n"));
    let changes = vec![
        vec![
            change("README.md", ChangeKind::Added, None, Some(b"hello\n")),
            change("src/big.bin", ChangeKind::Added, None, Some(&big(1))),
            change("src/lib.rs", ChangeKind::Added, None, Some(lib1)),
        ],
        vec![change("src/lib.rs", ChangeKind::Modified, Some(lib1), Some(lib2))],
        vec![
            change("README.md", ChangeKind::Deleted, Some(b"hello\n"), None),
            change("docs/guide.md", ChangeKind::Added, None, Some(b"# guide\n")),
        ],
    ];
    let mut objects = Vec::new();
    let mut commits: Vec<Fixture> = Vec::new();
    for (i, (model, changes)) in [&m1, &m2, &m3].into_iter().zip(changes).enumerate() {
        let tree = build(&nest(model), &mut objects);
        let parent = commits.last().map(|c| c.id);
        let mut who = sig();
        who.time += i as i64;
        let commit = Object::Commit(Commit {
            tree,
            parents: parent.into_iter().collect(),
            author: who.clone(),
            committer: who.clone(),
            extra_headers: vec![],
            message: format!("commit {i}\n").into_bytes(),
        })
        .encode();
        let id = commit.id();
        objects.push(commit);
        commits.push(Fixture {
            id,
            generation: i as i64 + 1,
            changes,
            tree,
            parent,
            time: who.time,
        });
    }
    objects.sort_by_key(|o| o.id());
    objects.dedup_by_key(|o| o.id());
    OldStore { commits, objects, models: vec![m1, m2, m3] }
}

/// Write `old` in the pre-versioning layout.
async fn write_old_layout(schema: &str, s3: &S3ObjectStore, old: &OldStore) {
    let qs = format!("\"{schema}\"");
    let mut conn = sqlx::PgConnection::connect(&pg_url()).await.unwrap();
    for stmt in old_ddl(&qs) {
        sqlx::query(sqlx::AssertSqlSafe(stmt)).execute(&mut conn).await.unwrap();
    }
    let ins = |stmt: &str| sqlx::AssertSqlSafe(stmt.replace("{qs}", &qs));
    // Objects: small ones zstd-compressed in Postgres, large ones one per S3 key.
    let (small, large): (Vec<_>, Vec<_>) =
        old.objects.iter().cloned().partition(|o| o.as_bytes().len() <= 4096);
    assert!(!large.is_empty() && !small.is_empty());
    for o in &small {
        sqlx::query(ins("INSERT INTO {qs}.objects (id, data) VALUES ($1, $2)"))
            .bind(o.id().as_bytes().as_slice())
            .bind(zstd::encode_all(&o.as_bytes()[..], 3).unwrap())
            .execute(&mut conn)
            .await
            .unwrap();
    }
    s3.put_many(&large).await.unwrap();
    for c in &old.commits {
        sqlx::query(ins(
            "INSERT INTO {qs}.commits (id, tree, parents, generation, committer_time) \
             VALUES ($1, $2, $3, $4, $5)",
        ))
        .bind(c.id.as_bytes().as_slice())
        .bind(c.tree.as_bytes().as_slice())
        .bind(c.parent.map(|p| p.as_bytes().to_vec()).unwrap_or_default())
        .bind(c.generation)
        .bind(c.time)
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(ins("INSERT INTO {qs}.log (commit_id) VALUES ($1)"))
            .bind(c.id.as_bytes().as_slice())
            .execute(&mut conn)
            .await
            .unwrap();
        for (idx, ch) in c.changes.iter().enumerate() {
            let side = |s: Option<(EntryMode, ObjectId)>| {
                s.map(|(m, id)| (m.as_octal().to_string(), id.as_bytes().to_vec())).unzip()
            };
            let (old_mode, old_id): (Option<String>, Option<Vec<u8>>) = side(ch.old);
            let (new_mode, new_id): (Option<String>, Option<Vec<u8>>) = side(ch.new);
            let kind: i16 = match ch.kind {
                ChangeKind::Added => 0,
                ChangeKind::Modified => 1,
                ChangeKind::Deleted => 2,
            };
            sqlx::query(ins("INSERT INTO {qs}.changes \
                 (commit_id, idx, path, kind, old_mode, old_id, new_mode, new_id) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"))
            .bind(c.id.as_bytes().as_slice())
            .bind(idx as i32)
            .bind(ch.path.as_str())
            .bind(kind)
            .bind(old_mode)
            .bind(old_id)
            .bind(new_mode)
            .bind(new_id)
            .execute(&mut conn)
            .await
            .unwrap();
        }
    }
    // History index of `main` through the second commit.
    for c in &old.commits[..2] {
        for ch in &c.changes {
            sqlx::query(ins("INSERT INTO {qs}.history (ref, path, generation, commit_id) \
                 VALUES ('main', $1, $2, $3)"))
            .bind(ch.path.as_str())
            .bind(c.generation)
            .bind(c.id.as_bytes().as_slice())
            .execute(&mut conn)
            .await
            .unwrap();
        }
    }
    sqlx::query(ins("INSERT INTO {qs}.history_watermark (ref, commit_id) VALUES ('main', $1)"))
        .bind(old.commits[1].id.as_bytes().as_slice())
        .execute(&mut conn)
        .await
        .unwrap();
    for (name, c) in [("main", &old.commits[2]), ("feature", &old.commits[1])] {
        sqlx::query(ins("INSERT INTO {qs}.refs (name, target) VALUES ($1, $2)"))
            .bind(name)
            .bind(c.id.as_bytes().as_slice())
            .execute(&mut conn)
            .await
            .unwrap();
    }
    conn.close().await.unwrap();
}

async fn count(schema: &str, table: &str) -> i64 {
    let mut conn = sqlx::PgConnection::connect(&pg_url()).await.unwrap();
    let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM \"{schema}\".{table}"
    )))
    .fetch_one(&mut conn)
    .await
    .unwrap();
    conn.close().await.unwrap();
    n
}

/// Delete everything under the test's S3 prefix (objects and packs).
async fn delete_s3_prefix(cfg: &S3Config) {
    let creds =
        aws_sdk_s3::config::Credentials::new(&cfg.access_key, &cfg.secret_key, None, None, "t");
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new(cfg.region.clone()))
        .endpoint_url(&cfg.endpoint)
        .credentials_provider(creds)
        .force_path_style(true)
        .build();
    let client = aws_sdk_s3::Client::from_conf(conf);
    let prefix = format!("{}/", cfg.prefix);
    loop {
        let listed = client
            .list_objects_v2()
            .bucket(&cfg.bucket)
            .prefix(&prefix)
            .send()
            .await
            .expect("listing test objects");
        let keys: Vec<String> =
            listed.contents().iter().filter_map(|o| o.key().map(str::to_owned)).collect();
        if keys.is_empty() {
            break;
        }
        for key in keys {
            client.delete_object().bucket(&cfg.bucket).key(key).send().await.expect("delete");
        }
    }
}

fn log_ids(entries: &[query::LogEntry]) -> Vec<ObjectId> {
    entries.iter().map(|e| e.id).collect()
}

async fn upgrade_body(schema: String, s3_cfg: S3Config) {
    let s3 = S3ObjectStore::connect(&s3_cfg).await.unwrap();
    s3.ensure_bucket().await.unwrap();
    let old = old_history();
    write_old_layout(&schema, &s3, &old).await;
    let [c1, c2, c3] = [0, 1, 2].map(|i| old.commits[i].id);

    // Unversioned: everything pending; refusing to migrate fails cleanly.
    let st = PgStore::schema_status(&pg_url(), &schema).await.unwrap();
    assert_eq!((st.version, st.pending.len()), (0, MIGRATIONS.len()));
    let err = store_args(&schema, &s3_cfg, &["--no-migrate"]).backend().await.err().unwrap();
    assert!(
        matches!(err.downcast_ref::<StoreError>(), Some(StoreError::MigrationRequired { .. })),
        "{err:#}"
    );
    assert_eq!(
        ant_store::compat_hint(err.chain()).map(|h| h.contains("admin schema --migrate")),
        Some(true)
    );

    // Open with the current code (as `ant serve` does): migrates.
    let b: Backend = store_args(&schema, &s3_cfg, &[]).backend().await.unwrap();
    let st = PgStore::schema_status(&pg_url(), &schema).await.unwrap();
    assert_eq!(st.version, SCHEMA_VERSION);
    assert!(st.pending.is_empty());
    assert_eq!(count(&schema, "schema_version").await, i64::from(SCHEMA_VERSION));

    // Objects: every one, small (Postgres) and large (S3, one per key).
    let ids: Vec<ObjectId> = old.objects.iter().map(|o| o.id()).collect();
    let got = b.objects.get_many(&ids).await.unwrap();
    for (o, g) in old.objects.iter().zip(&got) {
        assert_eq!(g.as_ref(), Some(o));
    }
    assert!(b.objects.has_many(&ids).await.unwrap().into_iter().all(|h| h));

    // Commits and changesets (per-path rows).
    let nodes = b.meta.get_commits(&[c1, c2, c3]).await.unwrap();
    for (f, n) in old.commits.iter().zip(&nodes) {
        let n = n.as_ref().unwrap();
        assert_eq!((n.tree, n.generation as i64, n.committer_time), (f.tree, f.generation, f.time));
        assert_eq!(n.parents, f.parent.into_iter().collect::<Vec<_>>());
    }
    let changesets = b.meta.get_changesets(&[c1, c2, c3]).await.unwrap();
    for (f, cs) in old.commits.iter().zip(changesets) {
        assert_eq!(cs, Some(f.changes.clone()));
    }
    assert_eq!(b.meta.tail(0, 10).await.unwrap(), vec![(1, c1), (2, c2), (3, c3)]);

    // Refs and contents.
    assert_eq!(
        b.refs.list_refs("").await.unwrap(),
        vec![("feature".to_string(), c2), ("main".to_string(), c3)]
    );
    for (rev, model) in [("main", &old.models[2]), ("feature", &old.models[1])] {
        for (path, (_, content)) in model {
            let info = query::read_path(&b, rev, &RepoPath::new(path).unwrap(), true)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(info.content.as_deref(), Some(&content[..]), "{rev}:{path}");
        }
    }
    let readme = RepoPath::new("README.md").unwrap();
    assert!(query::read_path(&b, "main", &readme, false).await.unwrap().is_none());

    // History: indexed rows (through c2) plus the unindexed c3, then a
    // catch-up over the legacy changesets.
    let lib = RepoPath::new("src/lib.rs").unwrap();
    let guide = RepoPath::new("docs/guide.md").unwrap();
    assert_eq!(log_ids(&query::log(&b, "main", Some(&lib), 10).await.unwrap()), vec![c2, c1]);
    assert_eq!(log_ids(&query::log(&b, "main", Some(&readme), 10).await.unwrap()), vec![c3, c1]);
    assert_eq!(log_ids(&query::log(&b, "main", None, 10).await.unwrap()), vec![c3, c2, c1]);
    history::catch_up(&b, "main").await.unwrap();
    assert_eq!(b.meta.history_watermark("main").await.unwrap(), Some(c3));
    assert_eq!(log_ids(&query::log(&b, "main", Some(&guide), 10).await.unwrap()), vec![c3]);
    let blame = history::blame(&b, "main", &lib).await.unwrap();
    assert_eq!(blame.iter().map(|l| l.commit).collect::<Vec<_>>(), vec![c1, c2]);

    // New writes: a changeset with a large file (an S3 pack) lands on main.
    let big2 = big(2);
    let ops = vec![
        FileOp::Write {
            path: lib.clone(),
            mode: EntryMode::File,
            content: WriteContent::Inline(Bytes::from_static(b"fn a() {}\nfn b() {}\nfn c() {}\n")),
        },
        FileOp::Write {
            path: RepoPath::new("src/big2.bin").unwrap(),
            mode: EntryMode::File,
            content: WriteContent::Inline(Bytes::from(big2.clone())),
        },
    ];
    let req = ChangesetRequest {
        base: Some(c3),
        ops,
        author: sig(),
        committer: None,
        message: "after upgrade\n".into(),
    };
    let c4 = create_changeset(&b, req).await.unwrap();
    let tip = match land(&b, "main", c4).await.unwrap() {
        LandOutcome::Landed { new_tip, .. } => new_tip,
        LandOutcome::Conflict { paths, .. } => panic!("conflict: {paths:?}"),
    };
    assert_eq!(b.refs.get_ref("main").await.unwrap(), Some(tip));
    let info = query::read_path(&b, "main", &RepoPath::new("src/big2.bin").unwrap(), true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(info.content.as_deref(), Some(&big2[..]));
    assert!(count(&schema, "object_index").await >= 1, "the large object went to a pack");
    assert!(count(&schema, "changesets").await >= 1, "new changesets are one row");
    history::catch_up(&b, "main").await.unwrap();
    assert_eq!(log_ids(&query::log(&b, "main", Some(&lib), 10).await.unwrap()), vec![tip, c2, c1]);
    // Old and new changesets side by side.
    let both = b.meta.get_changesets(&[c2, tip]).await.unwrap();
    assert_eq!(both[0], Some(old.commits[1].changes.clone()));
    assert_eq!(both[1].as_ref().map(|c| c.len()), Some(2));

    // Restart without migrating: current, so fine.
    let b2 = store_args(&schema, &s3_cfg, &["--no-migrate"]).backend().await.unwrap();
    assert_eq!(b2.refs.get_ref("main").await.unwrap(), Some(tip));

    // A newer version's schema is refused at startup with an upgrade hint.
    let mut conn = sqlx::PgConnection::connect(&pg_url()).await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO \"{schema}\".schema_version (version, name, applied_by) \
         VALUES ({}, 'future', 'ant 99')",
        SCHEMA_VERSION + 1
    )))
    .execute(&mut conn)
    .await
    .unwrap();
    conn.close().await.unwrap();
    for extra in [&[][..], &["--no-migrate"][..]] {
        let err = store_args(&schema, &s3_cfg, extra).backend().await.err().unwrap();
        let msg = format!("{err:#}");
        assert!(msg.contains("newer than this version of ant supports"), "{msg}");
        assert_eq!(ant_store::compat_hint(err.chain()), Some(ant_core::NewerFormat::HINT));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs postgres and s3: docker compose up -d"]
async fn pre_versioning_store_upgrades_in_place() {
    let schema = ant_store::testing::scratch_schema("upgrade");
    let _guard = SchemaGuard::new(&pg_url(), &schema);
    let s3_cfg = S3Config::for_tests(&format!("upgrade{}", rand::random::<u64>()));
    // Run the body as a task so the S3 data is removed even when it fails.
    let result = tokio::spawn(upgrade_body(schema.clone(), s3_cfg.clone())).await;
    delete_s3_prefix(&s3_cfg).await;
    if let Err(e) = result {
        std::panic::resume_unwind(e.into_panic());
    }
}
