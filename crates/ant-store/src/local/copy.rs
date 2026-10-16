//! Copying a local data directory into another backend (`ant admin
//! copy-local`): the way from local mode to Postgres + S3.
//!
//! Everything the stores hold is copied through the store traits, in an
//! order that never lets the destination reference what it lacks: every
//! object (small and packed, reachable or not: `ant admin gc` on the
//! destination drops garbage later), then every commit record with its
//! changeset in log order (parents first), then every derived row
//! (landed-as and import mappings, blame caches, the skip index), then the
//! refs, created only where the destination has no ref of that name. The
//! path-history index is not copied: servers rebuild it on first use.
//! Copying is idempotent, so an interrupted copy can simply be run again.

use ant_core::ObjectId;

use super::{LocalStore, Sq, id_of};
use crate::{Backend, CasOutcome, Result};

/// Rows read and written per batch.
const BATCH: usize = 2000;

/// What a copy did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CopyReport {
    pub objects: u64,
    pub commits: u64,
    pub derived: u64,
    /// Refs created in the destination.
    pub refs: u64,
    /// Refs the destination already had (with this or another target):
    /// left as they were.
    pub refs_skipped: Vec<(String, ObjectId)>,
}

/// Ids of `table`'s `col` above `after`, ascending, at most [`BATCH`].
async fn ids_after(
    src: &LocalStore,
    table: &'static str,
    col: &'static str,
    after: Option<Vec<u8>>,
) -> Result<Vec<Vec<u8>>> {
    src.db()
        .read(move |c| {
            let sql = format!(
                "SELECT {col} FROM {table} WHERE ?1 IS NULL OR {col} > ?1 ORDER BY {col} LIMIT {BATCH}"
            );
            let mut st = c.prepare_cached(&sql).sq()?;
            let rows = st.query_map([after], |r| r.get::<_, Vec<u8>>(0)).sq()?;
            rows.map(|r| r.sq()).collect()
        })
        .await
}

/// Copy everything in `src` into `dst` (see the module docs).
pub async fn copy_to(
    src: &LocalStore,
    dst: &Backend,
    progress: &(dyn Fn(&str) + Sync),
) -> Result<CopyReport> {
    let mut report = CopyReport::default();
    let objects = src.backend().objects;
    for (table, col) in [("objects", "id"), ("object_index", "id")] {
        let mut after = None;
        loop {
            let batch = ids_after(src, table, col, after.clone()).await?;
            let Some(last) = batch.last().cloned() else { break };
            let ids: Vec<ObjectId> = batch.iter().map(|b| id_of(b)).collect::<Result<_>>()?;
            let found: Vec<_> = objects.get_many(&ids).await?.into_iter().flatten().collect();
            dst.objects.put_many(&found).await?;
            report.objects += found.len() as u64;
            after = Some(last);
        }
        progress(&format!("{table}: {} objects copied", report.objects));
    }
    let meta = src.backend().meta;
    let mut seq = 0u64;
    loop {
        let log = meta.tail(seq, BATCH).await?;
        let Some((last, _)) = log.last().copied() else { break };
        let ids: Vec<ObjectId> = log.iter().map(|(_, id)| *id).collect();
        let nodes = meta.get_commits(&ids).await?;
        let changesets = meta.get_changesets(&ids).await?;
        let batch: Vec<_> = nodes
            .into_iter()
            .zip(changesets)
            .filter_map(|(n, c)| Some((n?, c.unwrap_or_default())))
            .collect();
        report.commits += batch.len() as u64;
        dst.meta.put_commits(batch).await?;
        seq = last;
    }
    progress(&format!("{} commits copied", report.commits));
    let mut after: Option<(String, Vec<u8>)> = None;
    loop {
        let a = after.clone();
        let rows: Vec<(String, Vec<u8>, Vec<u8>)> = src
            .db()
            .read(move |c| {
                let mut st = c
                    .prepare_cached(&format!(
                        "SELECT kind, key, value FROM derived \
                         WHERE ?1 IS NULL OR (kind, key) > (?1, ?2) ORDER BY kind, key LIMIT {BATCH}"
                    ))
                    .sq()?;
                let (k, key) = match a {
                    Some((k, key)) => (Some(k), Some(key)),
                    None => (None, None),
                };
                let rows = st
                    .query_map(rusqlite::params![k, key], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                    .sq()?;
                rows.map(|r| r.sq()).collect()
            })
            .await?;
        let Some((k, key, _)) = rows.last().cloned() else { break };
        after = Some((k, key));
        report.derived += rows.len() as u64;
        for group in rows.chunk_by(|a, b| a.0 == b.0) {
            let entries = group.iter().map(|(_, k, v)| (k.clone(), v.clone().into())).collect();
            dst.meta.put_derived_many(&group[0].0, entries).await?;
        }
    }
    progress(&format!("{} derived rows copied", report.derived));
    for (name, target) in src.list_refs_all().await? {
        match dst.refs.cas_ref(&name, None, Some(target)).await? {
            CasOutcome::Updated => report.refs += 1,
            CasOutcome::Mismatch { .. } => report.refs_skipped.push((name, target)),
        }
    }
    Ok(report)
}

impl LocalStore {
    async fn list_refs_all(&self) -> Result<Vec<(String, ObjectId)>> {
        crate::RefStore::list_refs(self, "").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectStoreExt;

    #[tokio::test]
    async fn a_local_store_copies_into_another_backend() {
        let src = LocalStore::open_scratch("copy").unwrap();
        let b = src.backend();
        let big: Vec<u8> = (0..200_000u32).map(|i| (i * 7919 % 251) as u8).collect();
        let tree = crate::graph::build_tree(b.objects.as_ref(), &[("a", b"small"), ("big", &big)])
            .await
            .unwrap();
        let sig = ant_core::Signature {
            name: b"t".to_vec(),
            email: b"t@x".to_vec(),
            time: 1,
            tz_offset_minutes: 0,
        };
        let commit = ant_core::Commit {
            tree,
            parents: vec![],
            author: sig.clone(),
            committer: sig,
            extra_headers: vec![],
            message: b"m\n".to_vec(),
        };
        let c = crate::graph::write_commit(&b, &commit).await.unwrap();
        b.refs.cas_ref("main", None, Some(c)).await.unwrap();
        b.refs.cas_ref("other", None, Some(c)).await.unwrap();
        b.meta.put_derived("land/landed-as/v1", c.as_bytes(), "x".into()).await.unwrap();

        let dst = Backend::in_memory();
        dst.refs.cas_ref("other", None, Some(tree)).await.unwrap();
        let r = copy_to(&src, &dst, &|_| {}).await.unwrap();
        assert_eq!((r.objects, r.commits, r.derived, r.refs), (4, 1, 1, 1), "{r:?}");
        assert_eq!(r.refs_skipped, vec![("other".to_string(), c)]);
        assert_eq!(dst.refs.get_ref("main").await.unwrap(), Some(c));
        let id = ant_core::hash_object(ant_core::ObjectKind::Blob, &big);
        assert_eq!(&dst.objects.read_blob(&id).await.unwrap()[..], &big[..]);
        assert_eq!(
            dst.meta.get_commits(&[c]).await.unwrap()[0],
            b.meta.get_commits(&[c]).await.unwrap()[0]
        );
        assert_eq!(
            dst.meta.get_changeset(&c).await.unwrap(),
            b.meta.get_changeset(&c).await.unwrap()
        );
        assert!(dst.meta.get_derived("land/landed-as/v1", c.as_bytes()).await.unwrap().is_some());
        // Again: nothing breaks, nothing changes.
        let again = copy_to(&src, &dst, &|_| {}).await.unwrap();
        assert_eq!(again.refs, 0);
    }
}
