//! Garbage collection of a local data directory (`ant admin gc --local`).
//!
//! Collection is **offline**: it runs in a process that holds the
//! directory's lock (`LocalStore::open`), so no server reads or writes while
//! it runs, and no cache anywhere remembers what it deletes. That makes the
//! condemn and quiesce phases of the Postgres collector
//! (`crate::postgres::gc`) unnecessary: a run marks and sweeps.
//!
//! - **Roots**, as in Postgres: ref targets, history watermarks and every
//!   commit in the history index, both sides of the derived rows of
//!   [`GcOptions::root_kinds`] (landed-as and git-import mappings), and
//!   every object, packed-object index row and commit record written within
//!   the grace period (unlanded changesets, uploads, imports in progress;
//!   storing an existing object refreshes an old write time, see
//!   `LocalStore::put_many`).
//! - **Mark**: the closure of the roots (commit → tree and parents, tree →
//!   entries, gitlinks skipped), commit records giving tree and parents
//!   without reading the commit object.
//! - **Sweep**: rows not marked and written before the cutoff are deleted:
//!   small objects, index rows of packed objects, commit records with their
//!   changesets and commit-keyed derived rows ([`GcOptions::commit_keyed_kinds`]).
//! - **Packs**: a pack no index row points into is deleted. A pack whose
//!   dead share reaches [`GcOptions::compact_ratio`] is compacted: its live
//!   records (whole frames: records are self-contained) are appended to a
//!   new pack (fsynced), the index rows are moved to it in one durable
//!   transaction, and only then is the old file deleted. A crash at any
//!   point leaves either the old location or both, never neither.
//! - **Space**: SQLite reuses the pages of deleted rows; when more than a
//!   quarter of the database file is free the run ends with `VACUUM`.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use ant_core::{EntryMode, ObjectId, ObjectKind};

use super::{LocalStore, Sq, id_of, now_secs};
use crate::{Result, StoreError};

pub use crate::postgres::gc::{
    DEFAULT_COMMIT_KEYED_KINDS, DEFAULT_GRACE, DEFAULT_ROOT_KINDS, MIN_GRACE,
};

/// Settings of one local collection run.
#[derive(Debug, Clone)]
pub struct GcOptions {
    /// Rows written less than this long before the run are kept, with
    /// everything they reach. At least [`MIN_GRACE`].
    pub grace: Duration,
    /// Mark and count only; change nothing.
    pub dry_run: bool,
    /// Derived kinds whose 32-byte keys and values are roots.
    pub root_kinds: Vec<String>,
    /// Derived kinds keyed by `<commit id><suffix>`, deleted with their commit.
    pub commit_keyed_kinds: Vec<String>,
    /// Ids per mark read and per delete transaction.
    pub batch: usize,
    /// Compact a pack when at least this share of its bytes is dead (1.0 or
    /// more: never).
    pub compact_ratio: f64,
}

impl Default for GcOptions {
    fn default() -> Self {
        Self {
            grace: DEFAULT_GRACE,
            dry_run: false,
            root_kinds: DEFAULT_ROOT_KINDS.iter().map(|k| k.to_string()).collect(),
            commit_keyed_kinds: DEFAULT_COMMIT_KEYED_KINDS.iter().map(|k| k.to_string()).collect(),
            batch: 2000,
            compact_ratio: 0.5,
        }
    }
}

/// What a run found and did (a dry run: would do).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GcReport {
    pub dry_run: bool,
    /// Rows written before this time (Unix seconds) were candidates.
    pub cutoff: i64,
    /// Ids marked reachable.
    pub live: u64,
    /// Ids reachable but missing from the store.
    pub missing: u64,
    /// Small objects deleted: count, stored (compressed) bytes.
    pub objects: u64,
    pub object_bytes: u64,
    /// Packed objects (index rows) deleted: count, packed bytes (frame
    /// members: their share).
    pub large_objects: u64,
    pub large_object_bytes: u64,
    /// Commit records deleted.
    pub commits: u64,
    /// Changesets deleted: count, stored bytes.
    pub changesets: u64,
    pub changeset_bytes: u64,
    /// Derived rows deleted with their commits.
    pub derived: u64,
    /// Pack files deleted: count, bytes.
    pub packs: u64,
    pub pack_bytes: u64,
    /// Packs compacted: count, bytes before, bytes of their live records.
    pub packs_compacted: u64,
    pub compacted_from: u64,
    pub compacted_to: u64,
    /// Packs kept: count, bytes, bytes no index row points at.
    pub packs_kept: u64,
    pub packs_kept_bytes: u64,
    pub packs_dead_bytes: u64,
    /// The database was vacuumed.
    pub vacuumed: bool,
}

const KIND_UNKNOWN: u8 = 0;
const KIND_COMMIT: u8 = 1;
const KIND_TREE: u8 = 2;
const KIND_BLOB: u8 = 3;

/// Child ids of a tree body (gitlinks skipped; unparsable modes count as
/// blobs, so their targets stay).
fn tree_children(body: &[u8], out: &mut Vec<(ObjectId, u8)>) -> Result<()> {
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
            Ok(EntryMode::Tree) => out.push((id, KIND_TREE)),
            Ok(EntryMode::Gitlink) => {}
            _ => out.push((id, KIND_BLOB)),
        }
    }
    Ok(())
}

/// Tree and parent ids of a commit body.
fn commit_children(body: &[u8], out: &mut Vec<(ObjectId, u8)>) -> Result<()> {
    let malformed = || StoreError::InvalidArgument("gc: malformed commit object".into());
    for line in body.split(|&b| b == b'\n') {
        if line.is_empty() {
            break;
        }
        let (kind, hex) = if let Some(hex) = line.strip_prefix(b"tree ") {
            (KIND_TREE, hex)
        } else if let Some(hex) = line.strip_prefix(b"parent ") {
            (KIND_COMMIT, hex)
        } else {
            continue;
        };
        let hex = std::str::from_utf8(hex).map_err(|_| malformed())?;
        out.push((hex.parse::<ObjectId>().map_err(|_| malformed())?, kind));
    }
    Ok(())
}

/// Roots of a run: `(id, kind)`.
fn roots(c: &rusqlite::Connection, opts: &GcOptions, cutoff: i64) -> Result<Vec<(ObjectId, u8)>> {
    let mut out = Vec::new();
    let mut add = |sql: &str, kind: u8, param: Option<i64>| -> Result<()> {
        let mut st = c.prepare(sql).sq()?;
        let mut rows = match param {
            Some(p) => st.query([p]).sq()?,
            None => st.query([]).sq()?,
        };
        while let Some(r) = rows.next().sq()? {
            let b: Vec<u8> = r.get(0).sq()?;
            if b.len() == ObjectId::LEN {
                out.push((id_of(&b)?, kind));
            }
        }
        Ok(())
    };
    add("SELECT target FROM refs", KIND_UNKNOWN, None)?;
    add("SELECT commit_id FROM history_watermark", KIND_COMMIT, None)?;
    add("SELECT DISTINCT commit_id FROM history", KIND_COMMIT, None)?;
    add("SELECT id FROM commits WHERE written_at >= ?1", KIND_COMMIT, Some(cutoff))?;
    add("SELECT id FROM objects WHERE written_at >= ?1", KIND_UNKNOWN, Some(cutoff))?;
    add("SELECT id FROM object_index WHERE written_at >= ?1", KIND_UNKNOWN, Some(cutoff))?;
    let mut st = c.prepare("SELECT key, value FROM derived WHERE kind = ?1").sq()?;
    for kind in &opts.root_kinds {
        let mut rows = st.query([kind]).sq()?;
        while let Some(r) = rows.next().sq()? {
            for col in 0..2 {
                let b: Vec<u8> = r.get(col).sq()?;
                if b.len() == ObjectId::LEN {
                    out.push((id_of(&b)?, KIND_UNKNOWN));
                }
            }
        }
    }
    Ok(out)
}

/// Commit records of `ids` (tree, parents).
fn recorded(
    c: &rusqlite::Connection,
    ids: &[ObjectId],
) -> Result<HashMap<ObjectId, (ObjectId, Vec<ObjectId>)>> {
    let mut st = c.prepare_cached("SELECT tree, parents FROM commits WHERE id = ?1").sq()?;
    let mut out = HashMap::new();
    for id in ids {
        let row: Option<(Vec<u8>, Vec<u8>)> = rusqlite::OptionalExtension::optional(
            st.query_row([id.as_bytes().as_slice()], |r| Ok((r.get(0)?, r.get(1)?))),
        )
        .sq()?;
        if let Some((tree, parents)) = row {
            out.insert(*id, (id_of(&tree)?, super::decode_ids(&parents)?));
        }
    }
    Ok(out)
}

/// One sweep candidate: its id and its bytes.
type Candidates = Vec<(ObjectId, u64)>;

/// Rows of `sql` (`id, bytes`) whose id is not live.
fn dead(
    c: &rusqlite::Connection,
    sql: &str,
    cutoff: i64,
    live: &HashSet<ObjectId>,
) -> Result<Candidates> {
    let mut st = c.prepare(sql).sq()?;
    let mut rows = st.query([cutoff]).sq()?;
    let mut out = Vec::new();
    while let Some(r) = rows.next().sq()? {
        let id = id_of(&r.get::<_, Vec<u8>>(0).sq()?)?;
        if !live.contains(&id) {
            out.push((id, r.get::<_, i64>(1).sq()? as u64));
        }
    }
    Ok(out)
}

/// One pack's records that index rows point into: `(off, len)` with the
/// ids in it.
type PackRecords = HashMap<(u64, u32), Vec<ObjectId>>;

fn pack_records(c: &rusqlite::Connection) -> Result<HashMap<[u8; 16], PackRecords>> {
    let mut st = c.prepare("SELECT id, pack, off, len FROM object_index").sq()?;
    let mut rows = st.query([]).sq()?;
    let mut out: HashMap<[u8; 16], PackRecords> = HashMap::new();
    while let Some(r) = rows.next().sq()? {
        let id = id_of(&r.get::<_, Vec<u8>>(0).sq()?)?;
        let pack: Vec<u8> = r.get(1).sq()?;
        let pack: [u8; 16] = pack
            .as_slice()
            .try_into()
            .map_err(|_| StoreError::InvalidArgument("corrupt pack id".into()))?;
        let off = r.get::<_, i64>(2).sq()? as u64;
        let len = r.get::<_, i64>(3).sq()? as u32;
        out.entry(pack).or_default().entry((off, len)).or_default().push(id);
    }
    Ok(out)
}

/// Collect garbage in `store` (see the module docs). The caller must be the
/// only user of the store: open it for the run (`ant admin gc --local`),
/// never inside a serving process.
pub async fn collect(
    store: &LocalStore,
    opts: &GcOptions,
    progress: &(dyn Fn(&str) + Sync),
) -> Result<GcReport> {
    if opts.grace < MIN_GRACE {
        return Err(StoreError::InvalidArgument(format!(
            "gc grace period must be at least {} hours",
            MIN_GRACE.as_secs() / 3600
        )));
    }
    let cutoff = now_secs().saturating_sub(i64::try_from(opts.grace.as_secs()).unwrap_or(i64::MAX));
    let mut report = GcReport { dry_run: opts.dry_run, cutoff, ..GcReport::default() };
    let objects = store.backend().objects;
    let batch = opts.batch.max(1);

    // -- mark
    let o = opts.clone();
    let mut todo = store.db().read(move |c| roots(c, &o, cutoff)).await?;
    let mut live: HashSet<ObjectId> = HashSet::new();
    todo.retain(|(id, _)| live.insert(*id));
    progress(&format!("marking from {} roots", todo.len()));
    while !todo.is_empty() {
        let take = todo.len().min(batch);
        let level: Vec<(ObjectId, u8)> = todo.split_off(todo.len() - take);
        let mut children: Vec<(ObjectId, u8)> = Vec::new();
        let maybe_commits: Vec<ObjectId> = level
            .iter()
            .filter(|(_, k)| matches!(*k, KIND_COMMIT | KIND_UNKNOWN))
            .map(|(id, _)| *id)
            .collect();
        let records = store.db().read(move |c| recorded(c, &maybe_commits)).await?;
        for (tree, parents) in records.values() {
            children.push((*tree, KIND_TREE));
            children.extend(parents.iter().map(|p| (*p, KIND_COMMIT)));
        }
        let read: Vec<ObjectId> = level
            .iter()
            .filter(|(id, k)| *k != KIND_BLOB && !records.contains_key(id))
            .map(|(id, _)| *id)
            .collect();
        if !read.is_empty() {
            for (id, object) in read.iter().zip(objects.get_many(&read).await?) {
                let Some(object) = object else {
                    report.missing += 1;
                    tracing::debug!(%id, "gc: reachable object missing");
                    continue;
                };
                match object.kind() {
                    ObjectKind::Tree => tree_children(object.body(), &mut children)?,
                    ObjectKind::Commit => commit_children(object.body(), &mut children)?,
                    ObjectKind::Blob => {}
                }
            }
        }
        todo.extend(children.into_iter().filter(|(id, _)| live.insert(*id)));
    }
    report.live = live.len() as u64;
    progress(&format!("marked {} live ids", report.live));

    // -- candidates
    let live = std::sync::Arc::new(live);
    let l = live.clone();
    let (small, large, commits) = store
        .db()
        .read(move |c| {
            let small =
                dead(c, "SELECT id, length(data) FROM objects WHERE written_at < ?1", cutoff, &l)?;
            let large = dead(
                c,
                "SELECT id, coalesce(share, len) FROM object_index WHERE written_at < ?1",
                cutoff,
                &l,
            )?;
            let commits = dead(
                c,
                "SELECT c.id, coalesce(length(cs.data), 0) FROM commits c \
                 LEFT JOIN changesets cs ON cs.commit_id = c.id WHERE c.written_at < ?1",
                cutoff,
                &l,
            )?;
            Ok((small, large, commits))
        })
        .await?;
    report.objects = small.len() as u64;
    report.object_bytes = small.iter().map(|(_, b)| b).sum();
    report.large_objects = large.len() as u64;
    report.large_object_bytes = large.iter().map(|(_, b)| b).sum();
    report.commits = commits.len() as u64;
    report.changesets = commits.iter().filter(|(_, b)| *b > 0).count() as u64;
    report.changeset_bytes = commits.iter().map(|(_, b)| b).sum();

    // -- sweep
    if !opts.dry_run {
        for part in small.chunks(batch) {
            let ids: Vec<ObjectId> = part.iter().map(|(id, _)| *id).collect();
            store.db().write(true, move |c, _| delete_ids(c, "objects", "id", &ids)).await?;
        }
        for part in large.chunks(batch) {
            let ids: Vec<ObjectId> = part.iter().map(|(id, _)| *id).collect();
            store.db().write(true, move |c, _| delete_ids(c, "object_index", "id", &ids)).await?;
        }
        for part in commits.chunks(batch) {
            let ids: Vec<ObjectId> = part.iter().map(|(id, _)| *id).collect();
            let kinds = opts.commit_keyed_kinds.clone();
            report.derived += store
                .db()
                .write(true, move |c, _| {
                    delete_ids(c, "commits", "id", &ids)?;
                    delete_ids(c, "changesets", "commit_id", &ids)?;
                    let mut st = c
                        .prepare_cached(
                            "DELETE FROM derived WHERE kind = ?1 AND key >= ?2 AND key < ?3",
                        )
                        .sq()?;
                    let mut n = 0u64;
                    for id in &ids {
                        let lo = id.as_bytes().to_vec();
                        let mut hi = lo.clone();
                        hi.push(0xff);
                        for kind in &kinds {
                            n += st.execute(rusqlite::params![kind, lo, hi]).sq()? as u64;
                        }
                    }
                    Ok(n)
                })
                .await?;
        }
        progress(&format!(
            "deleted {} objects, {} packed objects, {} commits",
            report.objects, report.large_objects, report.commits
        ));
    }

    // -- packs
    packs(store, opts, &mut report, progress).await?;

    // -- space
    if !opts.dry_run {
        report.vacuumed = store
            .db()
            .write_raw(|c| {
                let free: i64 = c.query_row("PRAGMA freelist_count", [], |r| r.get(0)).sq()?;
                let pages: i64 = c.query_row("PRAGMA page_count", [], |r| r.get(0)).sq()?;
                if pages > 0 && free * 4 > pages {
                    c.execute_batch("VACUUM").sq()?;
                    return Ok(true);
                }
                Ok(false)
            })
            .await?;
    }
    Ok(report)
}

fn delete_ids(c: &rusqlite::Connection, table: &str, col: &str, ids: &[ObjectId]) -> Result<()> {
    let mut st = c.prepare_cached(&format!("DELETE FROM {table} WHERE {col} = ?1")).sq()?;
    for id in ids {
        st.execute([id.as_bytes().as_slice()]).sq()?;
    }
    Ok(())
}

/// Delete packs without live records, compact mostly-dead ones.
async fn packs(
    store: &LocalStore,
    opts: &GcOptions,
    report: &mut GcReport,
    progress: &(dyn Fn(&str) + Sync),
) -> Result<()> {
    let files = store.packs().clone();
    // Nothing may still be appending: close the active pack (if this
    // process wrote one), so every pack is complete.
    let f = files.clone();
    tokio::task::spawn_blocking(move || f.seal()).await.map_err(StoreError::backend)??;
    let f = files.clone();
    let on_disk =
        tokio::task::spawn_blocking(move || f.list()).await.map_err(StoreError::backend)??;
    let records = store.db().read(pack_records).await?;
    let mut doomed: Vec<[u8; 16]> = Vec::new();
    let mut compact: Vec<([u8; 16], u64, PackRecords)> = Vec::new();
    for (pack, size) in &on_disk {
        let Some(recs) = records.get(pack) else {
            doomed.push(*pack);
            report.packs += 1;
            report.pack_bytes += size;
            continue;
        };
        let live_bytes: u64 = recs.keys().map(|(_, len)| u64::from(*len)).sum::<u64>()
            + super::packs::PACK_MAGIC.len() as u64;
        let dead = size.saturating_sub(live_bytes);
        if *size > 0 && dead as f64 / *size as f64 >= opts.compact_ratio {
            report.packs_compacted += 1;
            report.compacted_from += size;
            report.compacted_to += live_bytes;
            compact.push((*pack, *size, recs.clone()));
        } else {
            report.packs_kept += 1;
            report.packs_kept_bytes += size;
            report.packs_dead_bytes += dead;
        }
    }
    if opts.dry_run {
        return Ok(());
    }
    for (pack, _, recs) in compact {
        // Live records, in file order, appended to a new pack (fsynced).
        let mut order: Vec<((u64, u32), Vec<ObjectId>)> = recs.into_iter().collect();
        order.sort_unstable_by_key(|(at, _)| *at);
        let f = files.clone();
        let moved: Vec<(ObjectId, [u8; 16], u64)> = tokio::task::spawn_blocking(move || {
            let mut moved = Vec::new();
            for chunk in order.chunks(256) {
                let bodies: Vec<Vec<u8>> = chunk
                    .iter()
                    .map(|((off, len), _)| f.read_blocking(&pack, *off, u64::from(*len)))
                    .collect::<Result<_>>()?;
                let placed = f.write_blocking(&bodies)?;
                for ((_, ids), (new_pack, new_off)) in chunk.iter().zip(placed) {
                    moved.extend(ids.iter().map(|id| (*id, new_pack, new_off)));
                }
            }
            Ok::<_, StoreError>(moved)
        })
        .await
        .map_err(StoreError::backend)??;
        // Index rows move in one durable transaction; then the old file goes.
        store
            .db()
            .write(true, move |c, _| {
                let mut st = c
                    .prepare_cached("UPDATE object_index SET pack = ?2, off = ?3 WHERE id = ?1")
                    .sq()?;
                for (id, p, off) in &moved {
                    st.execute(rusqlite::params![
                        id.as_bytes().as_slice(),
                        p.as_slice(),
                        *off as i64
                    ])
                    .sq()?;
                }
                Ok(())
            })
            .await?;
        doomed.push(pack);
    }
    let f = files.clone();
    tokio::task::spawn_blocking(move || {
        f.seal()?;
        f.forget(&doomed);
        for pack in &doomed {
            let path = f.path_of(pack);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(StoreError::backend(e)),
            }
        }
        super::packs::sync_dir(f.dir()).map_err(StoreError::backend)
    })
    .await
    .map_err(StoreError::backend)??;
    progress(&format!(
        "packs: {} deleted, {} compacted, {} kept",
        report.packs, report.packs_compacted, report.packs_kept
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use ant_core::{Commit, Signature};

    use super::*;
    use crate::Backend;
    use crate::graph::{build_tree, write_commit};
    use crate::local::LocalOptions;

    fn noise(seed: u64, len: usize) -> Vec<u8> {
        let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    async fn commit(b: &Backend, files: &[(&str, &[u8])], parents: Vec<ObjectId>) -> ObjectId {
        let tree = build_tree(b.objects.as_ref(), files).await.unwrap();
        let sig = Signature {
            name: b"t".to_vec(),
            email: b"t@x".to_vec(),
            time: 1,
            tz_offset_minutes: 0,
        };
        let c = Commit {
            tree,
            parents,
            author: sig.clone(),
            committer: sig,
            extra_headers: vec![],
            message: b"m\n".to_vec(),
        };
        write_commit(b, &c).await.unwrap()
    }

    /// Everything written so far looks two days old.
    async fn age(s: &LocalStore) {
        s.db()
            .write(true, |c, _| {
                for t in ["objects", "object_index", "commits"] {
                    c.execute(&format!("UPDATE {t} SET written_at = written_at - 172800"), [])
                        .sq()?;
                }
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn present(b: &Backend, ids: &[ObjectId]) -> Vec<bool> {
        b.objects.get_many(ids).await.unwrap().iter().map(Option::is_some).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreachable_old_data_goes_and_everything_else_stays() {
        let dir = tempfile::tempdir().unwrap();
        let opts = LocalOptions::default();
        let (big, big2) = (noise(1, 200_000), noise(2, 300_000));
        let (kept, gone, mapped);
        let fresh_obj = ant_core::EncodedObject::new(ObjectKind::Blob, &noise(4, 50_000));
        {
            let s = LocalStore::open(dir.path(), &opts).unwrap();
            let b = s.backend();
            let c1 = commit(&b, &[("big", &big), ("a/small", b"small")], vec![]).await;
            kept = commit(&b, &[("big", &big), ("a/small", b"small2")], vec![c1]).await;
            b.refs.cas_ref("main", None, Some(kept)).await.unwrap();
            gone = commit(&b, &[("big2", &big2), ("a/small", b"orphan")], vec![c1]).await;
            mapped = commit(&b, &[("m", b"landed elsewhere")], vec![]).await;
            let value = bytes::Bytes::copy_from_slice(mapped.as_bytes());
            b.meta.put_derived("land/landed-as/v1", c1.as_bytes(), value).await.unwrap();
            let key = [gone.as_bytes().as_slice(), b"a/small"].concat();
            b.meta.put_derived("blame/v2", &key, "x".into()).await.unwrap();
            age(&s).await;
            // Written within the grace period: kept although unreachable.
            b.objects.put_many(std::slice::from_ref(&fresh_obj)).await.unwrap();
        }
        let gone_tree = {
            let s = LocalStore::open(dir.path(), &opts).unwrap();
            s.backend().meta.get_commits(&[gone]).await.unwrap()[0].clone().unwrap().tree
        };
        let big2_id = ant_core::hash_object(ObjectKind::Blob, &big2);
        let orphan_id = ant_core::hash_object(ObjectKind::Blob, b"orphan");
        let big_id = ant_core::hash_object(ObjectKind::Blob, &big);
        let grace = Duration::from_secs(86_400);

        // A dry run counts and changes nothing.
        let s = LocalStore::open(dir.path(), &opts).unwrap();
        let dry = GcOptions { grace, dry_run: true, ..GcOptions::default() };
        let r = collect(&s, &dry, &|_| {}).await.unwrap();
        assert_eq!((r.commits, r.large_objects), (1, 1), "{r:?}");
        assert!(r.objects >= 3, "orphan blob, trees, commit object: {r:?}");
        assert_eq!(present(&s.backend(), &[gone, big2_id]).await, [true, true]);

        let r = collect(&s, &GcOptions { grace, ..GcOptions::default() }, &|_| {}).await.unwrap();
        assert_eq!(r.missing, 0);
        assert_eq!((r.commits, r.changesets, r.large_objects, r.derived), (1, 1, 1, 1), "{r:?}");
        assert_eq!(r.packs_compacted, 1, "{r:#?}");
        drop(s);

        let s = LocalStore::open(dir.path(), &opts).unwrap();
        let b = s.backend();
        assert_eq!(present(&b, &[gone, gone_tree, big2_id, orphan_id]).await, [false; 4]);
        assert_eq!(b.meta.get_commits(&[gone]).await.unwrap(), vec![None]);
        assert_eq!(b.meta.get_changeset(&gone).await.unwrap(), None);
        assert_eq!(present(&b, &[kept, mapped, big_id, fresh_obj.id()]).await, [true; 4]);
        let blob = b.objects.get_many(&[big_id]).await.unwrap().pop().flatten().unwrap();
        assert_eq!(blob.body(), &big[..]);
        assert!(b.meta.get_commits(&[kept]).await.unwrap()[0].is_some());
        // The compacted pack is gone; its live records moved.
        let (_, pack_bytes) = s.disk_usage().unwrap();
        assert!(pack_bytes < 300_000, "{pack_bytes}");
        // A second run finds nothing more.
        let r = collect(&s, &GcOptions { grace, ..GcOptions::default() }, &|_| {}).await.unwrap();
        assert_eq!((r.objects, r.large_objects, r.commits, r.packs), (0, 0, 0, 0), "{r:?}");
    }
}
