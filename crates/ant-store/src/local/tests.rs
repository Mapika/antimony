use std::sync::Arc;

use super::*;
use crate::conformance;

fn fresh() -> LocalStore {
    LocalStore::open_scratch("s").unwrap()
}

#[tokio::test]
async fn local_object_store_conforms() {
    conformance::object_store(Arc::new(fresh())).await;
    // With the pack files: the tiered backend.
    conformance::object_store(fresh().backend().objects).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_ref_store_conforms() {
    conformance::ref_store(Arc::new(fresh())).await;
    let s: Arc<dyn RefStore> = Arc::new(fresh());
    conformance::ref_lock(s.clone(), s).await;
}

#[tokio::test]
async fn local_meta_store_conforms() {
    conformance::meta_store(Arc::new(fresh())).await;
    conformance::history_index(Arc::new(fresh())).await;
    conformance::meta_store_batch(Arc::new(fresh())).await;
    conformance::ancestors_above(Arc::new(fresh())).await;
}

fn id(n: u8) -> ObjectId {
    ObjectId([n; 32])
}

fn row(k: &[u8], v: &[u8]) -> (Vec<u8>, Bytes) {
    (k.to_vec(), Bytes::copy_from_slice(v))
}

#[tokio::test]
async fn local_ref_lock_publishes_ref_and_derived_rows_atomically() {
    let s = fresh();
    let mut lock = s.lock_ref("main").await.unwrap();
    assert!(lock.is_held() && lock.can_publish());
    assert_eq!(
        lock.publish(None, id(1), "k", vec![row(b"a", b"1")]).await.unwrap(),
        CasOutcome::Updated
    );
    assert_eq!(s.get_ref("main").await.unwrap(), Some(id(1)));
    assert_eq!(s.get_derived("k", b"a").await.unwrap().as_deref(), Some(&b"1"[..]));
    // A mismatch writes no rows.
    assert_eq!(
        lock.publish(Some(id(9)), id(2), "k", vec![row(b"b", b"2")]).await.unwrap(),
        CasOutcome::Mismatch { actual: Some(id(1)) }
    );
    assert_eq!(
        lock.publish(None, id(2), "k", vec![row(b"b", b"2")]).await.unwrap(),
        CasOutcome::Mismatch { actual: Some(id(1)) }
    );
    assert_eq!(s.get_derived("k", b"b").await.unwrap(), None);
    // Moving it; existing keys keep their first value.
    assert_eq!(
        lock.publish(Some(id(1)), id(2), "k", vec![row(b"b", b"2"), row(b"a", b"x")])
            .await
            .unwrap(),
        CasOutcome::Updated
    );
    assert_eq!(s.get_ref("main").await.unwrap(), Some(id(2)));
    assert_eq!(s.get_derived("k", b"a").await.unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(s.get_derived("k", b"b").await.unwrap().as_deref(), Some(&b"2"[..]));
    lock.release().await.unwrap();
}

#[tokio::test]
async fn local_ref_moves_are_announced_after_commit() {
    use crate::feed::RefNotice;
    let s = fresh();
    let mut rx = s.watch_refs().unwrap();
    let mut next = || match rx.try_recv() {
        Ok(RefNotice::Moved(m)) => Some((m.old, m.new, m.cause)),
        Ok(other) => panic!("{other:?}"),
        Err(_) => None,
    };
    assert!(matches!(
        s.cas_ref("main", Some(id(9)), Some(id(1))).await.unwrap(),
        CasOutcome::Mismatch { .. }
    ));
    assert_eq!(next(), None, "a mismatch announces nothing");
    s.cas_ref("main", None, Some(id(1))).await.unwrap();
    assert_eq!(next(), Some((None, Some(id(1)), MoveCause::Update)));
    crate::feed::landing(s.cas_ref("main", Some(id(1)), Some(id(2)))).await.unwrap();
    assert_eq!(next(), Some((Some(id(1)), Some(id(2)), MoveCause::Land)));
    let mut lock = s.lock_ref("main").await.unwrap();
    lock.publish(Some(id(2)), id(3), "k", vec![]).await.unwrap();
    lock.publish(Some(id(2)), id(4), "k", vec![]).await.unwrap();
    lock.release().await.unwrap();
    assert_eq!(next(), Some((Some(id(2)), Some(id(3)), MoveCause::Land)));
    assert_eq!(next(), None);
    // A no-op CAS (same target) moves nothing.
    s.cas_ref("main", Some(id(3)), Some(id(3))).await.unwrap();
    s.cas_ref("main", Some(id(3)), None).await.unwrap();
    assert_eq!(next(), Some((Some(id(3)), None, MoveCause::Update)));
    assert_eq!(next(), None);
}

#[tokio::test]
async fn data_survives_reopening_and_the_directory_is_exclusive() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    let big = EncodedObject::new(ant_core::ObjectKind::Blob, &vec![3u8; 100_000]);
    let small = EncodedObject::new(ant_core::ObjectKind::Blob, b"small");
    {
        let s = LocalStore::open(&path, &LocalOptions::default()).unwrap();
        let err = LocalStore::open(&path, &LocalOptions::default()).unwrap_err();
        assert!(err.to_string().contains("in use"), "{err}");
        let b = s.backend();
        b.objects.put_many(&[big.clone(), small.clone()]).await.unwrap();
        b.refs.cas_ref("main", None, Some(id(1))).await.unwrap();
        let node = CommitNode {
            id: id(5),
            tree: id(6),
            parents: vec![],
            generation: 1,
            committer_time: 7,
        };
        b.meta.put_commit(node, vec![]).await.unwrap();
    }
    let s = LocalStore::open(&path, &LocalOptions::default()).unwrap();
    let b = s.backend();
    let got = b.objects.get_many(&[big.id(), small.id()]).await.unwrap();
    assert_eq!(got, vec![Some(big.clone()), Some(small.clone())]);
    assert_eq!(b.refs.get_ref("main").await.unwrap(), Some(id(1)));
    assert_eq!(b.meta.log_seqs(&[id(5)]).await.unwrap(), vec![Some(1)]);
    // New writes go to a new pack; the old one stays readable.
    let big2 = EncodedObject::new(ant_core::ObjectKind::Blob, &vec![4u8; 100_000]);
    b.objects.put_many(std::slice::from_ref(&big2)).await.unwrap();
    assert_eq!(s.packs().list().unwrap().len(), 2);
    assert_eq!(b.objects.get_many(&[big.id()]).await.unwrap(), vec![Some(big)]);
    let (db, packs) = s.disk_usage().unwrap();
    assert!(db > 0 && packs > 0);
    assert_eq!(LocalStore::schema_status(&path).unwrap().version, SCHEMA_VERSION);
}

#[test]
fn newer_formats_are_refused_and_migration_can_be_required() {
    let dir = tempfile::tempdir().unwrap();
    // Without migrating, a new directory has no schema.
    let opts = LocalOptions { migrate: false, ..LocalOptions::default() };
    let err = LocalStore::open(dir.path(), &opts).unwrap_err();
    assert!(matches!(err, StoreError::MigrationRequired { found: 0, .. }), "{err}");
    drop(LocalStore::open(dir.path(), &LocalOptions::default()).unwrap());
    drop(LocalStore::open(dir.path(), &opts).unwrap());
    // A newer schema.
    {
        let conn = Connection::open(dir.path().join(DB_FILE)).unwrap();
        conn.execute(
            "INSERT INTO schema_version VALUES (?1, 'future', 0, 'test')",
            [SCHEMA_VERSION + 1],
        )
        .unwrap();
    }
    let err = LocalStore::open(dir.path(), &LocalOptions::default()).unwrap_err();
    assert!(matches!(err, StoreError::NewerFormat(_)), "{err}");
    assert_eq!(LocalStore::schema_status(dir.path()).unwrap().version, SCHEMA_VERSION + 1);
    // A newer directory layout.
    let other = tempfile::tempdir().unwrap();
    std::fs::write(other.path().join(FORMAT_FILE), format!("{}\n", DATA_FORMAT + 1)).unwrap();
    let err = LocalStore::open(other.path(), &LocalOptions::default()).unwrap_err();
    assert!(matches!(err, StoreError::NewerFormat(_)), "{err}");
    assert_eq!(
        std::fs::read_to_string(dir.path().join(FORMAT_FILE)).unwrap(),
        format!("{DATA_FORMAT}\n")
    );
}

/// Deferred writes commit without an fsync and become durable with the
/// next durable commit; either way they are visible at once.
#[tokio::test]
async fn deferred_writes_are_visible_and_published_durably() {
    let s = fresh();
    let b = s.backend();
    let obj = EncodedObject::new(ant_core::ObjectKind::Blob, b"deferred");
    let mut lock = b.refs.lock_ref("main").await.unwrap();
    crate::deferred_durability(async {
        b.objects.put_many(std::slice::from_ref(&obj)).await.unwrap();
        b.meta.put_derived_many("k", vec![row(b"x", b"1")]).await.unwrap();
    })
    .await;
    assert_eq!(b.objects.get_many(&[obj.id()]).await.unwrap(), vec![Some(obj.clone())]);
    assert_eq!(lock.publish(None, obj.id(), "m", vec![]).await.unwrap(), CasOutcome::Updated);
    lock.release().await.unwrap();
    assert_eq!(b.meta.get_derived("k", b"x").await.unwrap().as_deref(), Some(&b"1"[..]));
}

/// One failing job of a commit group rolls back alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_write_rolls_back_alone() {
    let s = fresh();
    let mut tasks = Vec::new();
    for i in 0..64u8 {
        let s = s.clone();
        tasks.push(tokio::spawn(async move {
            s.db()
                .write(i % 2 == 0, move |c, _| {
                    c.execute(
                        "INSERT INTO derived (kind, key, value) VALUES ('t', ?1, x'00')",
                        [vec![i]],
                    )
                    .sq()?;
                    if i % 4 == 1 {
                        return Err(StoreError::InvalidArgument("boom".into()));
                    }
                    Ok(())
                })
                .await
        }));
    }
    for (i, t) in tasks.into_iter().enumerate() {
        let r = t.await.unwrap();
        assert_eq!(r.is_err(), i % 4 == 1, "job {i}");
    }
    for i in 0..64u8 {
        let got = s.get_derived("t", &[i]).await.unwrap();
        assert_eq!(got.is_some(), i % 4 != 1, "row {i}");
    }
}

#[tokio::test]
async fn path_history_matches_prefixes_exactly() {
    let s = fresh();
    let p = |x: &str| RepoPath::new(x).unwrap();
    let rows: Vec<HistoryRow> = [("a", 1u8), ("a/b", 2), ("a-b", 3), ("a0", 4), ("ab", 5)]
        .into_iter()
        .map(|(path, n)| HistoryRow { path: p(path), generation: u64::from(n), commit: id(n) })
        .collect();
    s.index_history("main", &rows).await.unwrap();
    let mut got = Vec::new();
    for prefix in ["a", "a/b", "a-b", ""] {
        let hits = s.query_history("main", &p(prefix), u64::MAX, 10).await.unwrap();
        got.push(hits.into_iter().map(|(g, _)| g).collect::<Vec<_>>());
    }
    assert_eq!(got, vec![vec![2, 1], vec![2], vec![3], vec![5, 4, 3, 2, 1]]);
    assert_eq!(s.query_history("main", &p(""), 3, 2).await.unwrap().len(), 2);
}

/// An index whose first lookup is answered with the locations of the
/// moment, after which the pack they point into is compacted away (rows
/// moved, file deleted): the read that follows finds the file gone.
#[derive(Clone)]
struct MovingIndex {
    store: LocalStore,
    moved: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl PackIndex for MovingIndex {
    async fn lookup_index(&self, ids: &[ObjectId]) -> Result<HashMap<ObjectId, IndexEntry>> {
        let found = self.store.lookup_index(ids).await?;
        if !self.moved.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let packs = self.store.packs().clone();
            tokio::task::spawn_blocking(move || packs.seal()).await.unwrap()?;
            let records = self.store.db().read(gc::pack_records).await?;
            for (pack, recs) in records {
                gc::compact_pack(&self.store, pack, recs).await?;
            }
        }
        Ok(found)
    }

    async fn index_objects(&self, entries: &[IndexEntry]) -> Result<()> {
        self.store.index_objects(entries).await
    }

    async fn condemned_among(&self, ids: &[ObjectId]) -> Result<HashSet<ObjectId>> {
        self.store.condemned_among(ids).await
    }
}

/// A read whose looked-up pack is deleted before it reads it (a collection
/// or repack moved its rows) looks up again and finds the object at its
/// new location; nothing else ever reads as missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_retry_when_their_pack_was_deleted_under_them() {
    use crate::packed::PackedStore;
    let s = fresh();
    let objects: Vec<EncodedObject> = (0..20u8)
        .map(|i| EncodedObject::new(ant_core::ObjectKind::Blob, &vec![i; 10_000 + usize::from(i)]))
        .collect();
    let ids: Vec<ObjectId> = objects.iter().map(EncodedObject::id).collect();
    s.backend().objects.put_many(&objects).await.unwrap();
    let before = s.packs().list().unwrap();
    let index = MovingIndex { store: s.clone(), moved: Arc::default() };
    let reader = PackedStore::new(index.clone(), s.packs().clone());
    let got = reader.get_many(&ids).await.unwrap();
    assert!(index.moved.load(std::sync::atomic::Ordering::SeqCst));
    assert!(got.iter().zip(&objects).all(|(g, o)| g.as_ref() == Some(o)));
    // The pack read first is gone; the objects live elsewhere now.
    let after = s.packs().list().unwrap();
    assert!(before.keys().all(|p| !after.contains_key(p)), "{before:?} {after:?}");
    let got = s.backend().objects.get_many(&ids).await.unwrap();
    assert!(got.iter().zip(&objects).all(|(g, o)| g.as_ref() == Some(o)));
}

/// New databases free pages incrementally (`gc` returns them while
/// serving).
#[tokio::test]
async fn new_databases_use_incremental_auto_vacuum() {
    let s = fresh();
    let mode: i64 =
        s.db().read(|c| c.query_row("PRAGMA auto_vacuum", [], |r| r.get(0)).sq()).await.unwrap();
    assert_eq!(mode, 2);
}

/// Pages freed by deletions go back to the file system while serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn freed_pages_are_returned_while_serving() {
    let s = fresh();
    let objects: Vec<EncodedObject> = (0..20_000u32)
        .map(|i| EncodedObject::new(ant_core::ObjectKind::Blob, format!("{i:>2000}").as_bytes()))
        .collect();
    s.put_many(&objects).await.unwrap();
    s.db().write(true, |c, _| c.execute_batch("DELETE FROM objects").sq()).await.unwrap();
    let counts = |s: LocalStore| async move {
        s.db()
            .read(|c| {
                let free: i64 = c.query_row("PRAGMA freelist_count", [], |r| r.get(0)).sq()?;
                let pages: i64 = c.query_row("PRAGMA page_count", [], |r| r.get(0)).sq()?;
                Ok((free, pages))
            })
            .await
            .unwrap()
    };
    let (free, pages) = counts(s.clone()).await;
    assert!(free > 100, "{free}");
    assert!(gc::reclaim(&s, false).await.unwrap());
    let (free_after, pages_after) = counts(s.clone()).await;
    assert_eq!(free_after, 0);
    assert!(pages_after < pages - free + 10, "{pages} {free} {pages_after}");
}
