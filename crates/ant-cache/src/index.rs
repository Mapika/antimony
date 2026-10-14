//! Bundle index of the cache tier: which objects make up the tree bundle of a
//! tree.
//!
//! A bundle is stored as the objects it holds (in the ordinary disk cache,
//! so the cache budget, eviction and `ant cache stats` cover them) plus a
//! small index file `{root}/bundles/{tree-hex}`: `ANTBIDX1` followed by the
//! 32-byte ids in bundle order, the tree first. Serving a bundle reads the
//! ids from the disk cache and re-encodes them; if any was evicted the
//! bundle is a miss and is fetched again. Index files are tiny (at most
//! 8192 ids) and are themselves bounded by [`Index::prune`] (oldest first).
//! The directory is invisible to older binaries (they only read `objects/`).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use ant_core::ObjectId;

const MAGIC: &[u8; 8] = b"ANTBIDX1";
/// Index files never list more ids than this (servers bundle at most 8192 objects).
const MAX_IDS: usize = 1 << 16;

static TMP: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct Index {
    dir: PathBuf,
}

impl Index {
    pub fn open(root: &Path) -> std::io::Result<Self> {
        let dir = root.join("bundles");
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn path(&self, tree: &ObjectId) -> PathBuf {
        self.dir.join(tree.to_hex())
    }

    /// The ids of `tree`'s bundle, if indexed and well-formed (blocking).
    pub fn read(&self, tree: &ObjectId) -> Option<Vec<ObjectId>> {
        let data = std::fs::read(self.path(tree)).ok()?;
        let body = data.strip_prefix(&MAGIC[..])?;
        if body.is_empty() || body.len() % 32 != 0 || body.len() / 32 > MAX_IDS {
            return None;
        }
        let ids: Vec<ObjectId> =
            body.chunks_exact(32).filter_map(|c| ObjectId::from_bytes(c).ok()).collect();
        (ids.first() == Some(tree)).then_some(ids)
    }

    /// Record `ids` (the tree first) as the bundle of `tree`; atomic (blocking).
    pub fn write(&self, tree: &ObjectId, ids: &[ObjectId]) -> std::io::Result<()> {
        let mut data = Vec::with_capacity(MAGIC.len() + ids.len() * 32);
        data.extend_from_slice(MAGIC);
        for id in ids {
            data.extend_from_slice(id.as_bytes());
        }
        let tmp = self.dir.join(format!(
            ".tmp.{}.{}",
            std::process::id(),
            TMP.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&tmp, data)?;
        std::fs::rename(&tmp, self.path(tree)).inspect_err(|_| {
            let _ = std::fs::remove_file(&tmp);
        })
    }

    /// Bytes of all index files (blocking).
    pub fn bytes(&self) -> u64 {
        self.files().iter().map(|f| f.1).sum()
    }

    /// Delete the oldest index files until at most 90% of `max_bytes` remain,
    /// if over it (blocking). Returns the files deleted.
    pub fn prune(&self, max_bytes: u64) -> usize {
        let mut files = self.files();
        let mut total: u64 = files.iter().map(|f| f.1).sum();
        if total <= max_bytes {
            return 0;
        }
        files.sort_by_key(|f| f.2);
        let mut removed = 0;
        for (path, size, _) in files {
            if total <= max_bytes / 10 * 9 {
                break;
            }
            if std::fs::remove_file(path).is_ok() {
                total -= size;
                removed += 1;
            }
        }
        removed
    }

    fn files(&self) -> Vec<(PathBuf, u64, SystemTime)> {
        let Ok(rd) = std::fs::read_dir(&self.dir) else { return Vec::new() };
        rd.flatten()
            .filter_map(|e| {
                let m = e.metadata().ok()?;
                Some((e.path(), m.len(), m.modified().unwrap_or(SystemTime::UNIX_EPOCH)))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> ObjectId {
        ObjectId([n; 32])
    }

    #[test]
    fn roundtrip_and_validation() {
        let dir = tempfile::tempdir().unwrap();
        let ix = Index::open(dir.path()).unwrap();
        assert!(ix.read(&id(1)).is_none());
        ix.write(&id(1), &[id(1), id(2), id(3)]).unwrap();
        assert_eq!(ix.read(&id(1)).unwrap(), vec![id(1), id(2), id(3)]);
        // The tree must come first.
        ix.write(&id(4), &[id(5)]).unwrap();
        assert!(ix.read(&id(4)).is_none());
        // Garbage is a miss.
        std::fs::write(dir.path().join("bundles").join(id(6).to_hex()), b"junk").unwrap();
        assert!(ix.read(&id(6)).is_none());
    }

    #[test]
    fn prune_drops_oldest_first() {
        let dir = tempfile::tempdir().unwrap();
        let ix = Index::open(dir.path()).unwrap();
        for n in 0..10u8 {
            ix.write(&id(n), &[id(n)]).unwrap();
            let f = std::fs::File::options().write(true).open(ix.path(&id(n))).unwrap();
            f.set_modified(
                SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1000 + n as u64),
            )
            .unwrap();
        }
        assert_eq!(ix.bytes(), 10 * 40);
        assert_eq!(ix.prune(1000), 0);
        let removed = ix.prune(200);
        assert!(removed >= 5, "{removed}");
        assert!(ix.read(&id(9)).is_some(), "newest survives");
        assert!(ix.read(&id(0)).is_none(), "oldest goes first");
    }
}
