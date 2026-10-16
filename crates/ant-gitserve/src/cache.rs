//! Cached base packs: the entries of a complete clone pack, kept so that
//! later clones reuse them instead of reading and delta-compressing the
//! whole history again (git's pack reuse, without bitmaps).
//!
//! A clone (`done`, no `have`s, no shallow boundary, `ofs-delta`) without a
//! usable base builds its pack as usual and, while streaming it, keeps the
//! entries (everything between the pack header and the trailer) in an
//! unlinked temporary file, together with the sorted ids of the objects in
//! it and the commits it was built for (its *tips*: the wanted commits that
//! are not ancestors of other wanted commits). The next clone with the same
//! filter whose wants reach every tip sends
//!
//! ```text
//! header(base count + n) | base entries, verbatim | n increment entries | trailer
//! ```
//!
//! where the increment is what the wants reach beyond the base (planned like
//! a fetch whose `have`s are the base: commits walked from the wants stop at
//! the base, trees and blobs in the base are excluded by id). OFS_DELTA
//! distances are relative, so the base entries are valid at any position;
//! the increment's entries are laid out after them. The pack is therefore
//! exactly the objects the wants reach, as without the cache.
//!
//! When the wants have moved far past the base (more than
//! `REBUILD_COMMITS_FRACTION` of its commits, plus `REBUILD_COMMITS_MIN`),
//! or do not reach every tip (a ref was deleted or rewound), the clone is
//! built from scratch again and its pack replaces the base. One base is kept
//! per filter (`blob:none` or none). Bases live in the server process only:
//! nothing is written to the stores, and a restarted server rebuilds its
//! bases on the first clones.
//!
//! Disk and memory: a base is an unlinked temporary file (about the size of
//! its pack) plus 32 bytes of id per object. At most one per filter is
//! installed; a replaced base is freed when the last clone streaming it
//! finishes, and the files vanish with the process, however it ends. A ref
//! move does not drop a base (it stays exact for the commits it was built
//! for); the next clone that does not fit it replaces it.
//!
//! Access: bases are shared by every requester. That is sound because the
//! export serves one view, the whole repository (see the crate docs); an
//! export with per-principal views would have to key bases by view.
use std::{
    fs::File,
    os::unix::fs::FileExt,
    sync::{Arc, Mutex},
};

use ant_core::ObjectId;
use tokio::sync::OwnedMutexGuard;

use crate::select::Selection;

/// A new base is built once the wants are this fraction of the base's
/// commits past it (1/8), plus a constant...
pub const REBUILD_COMMITS_FRACTION: usize = 8;
/// ...so that small histories are not rebuilt on every commit.
pub const REBUILD_COMMITS_MIN: usize = 1024;

/// The entries of one complete pack and what they contain.
pub struct Base {
    pub no_blobs: bool,
    /// The commits the pack was built for: every object reachable from them
    /// (filtered) is in it, and nothing else.
    pub tips: Vec<ObjectId>,
    /// Ids of every object in the pack, sorted.
    ids: Vec<ObjectId>,
    pub commits: usize,
    pub objects: u32,
    /// Pack entries without header and trailer; `len` bytes from offset 0.
    file: File,
    pub len: u64,
}

impl Base {
    pub fn new(
        no_blobs: bool,
        tips: Vec<ObjectId>,
        mut ids: Vec<ObjectId>,
        commits: usize,
        file: File,
        len: u64,
    ) -> anyhow::Result<Self> {
        ids.sort_unstable();
        let objects = u32::try_from(ids.len())?;
        ids.dedup();
        anyhow::ensure!(ids.len() == objects as usize, "duplicate objects in a pack");
        Ok(Self { no_blobs, tips, ids, commits, objects, file, len })
    }

    /// Clones reaching more new commits than this build a new base.
    pub fn rebuild_after(&self) -> usize {
        self.commits / REBUILD_COMMITS_FRACTION + REBUILD_COMMITS_MIN
    }

    pub fn contains(&self, id: &ObjectId) -> bool {
        self.ids.binary_search(id).is_ok()
    }

    /// Fill `buf` from `offset` of the entries (blocking).
    pub fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        self.file.read_exact_at(buf, offset)
    }
}

/// The bases of one export, one per filter.
#[derive(Default)]
pub struct PackCache {
    bases: Mutex<[Option<Arc<Base>>; 2]>,
    /// Held while a base of the filter is built: clones arriving meanwhile
    /// wait for it instead of building the same pack at the same time.
    builds: [Arc<tokio::sync::Mutex<()>>; 2],
}

impl PackCache {
    pub fn get(&self, no_blobs: bool) -> Option<Arc<Base>> {
        self.bases.lock().ok()?[usize::from(no_blobs)].clone()
    }

    /// Wait until no base of this filter is being built; the guard makes
    /// the caller the builder.
    pub async fn lock_build(&self, no_blobs: bool) -> OwnedMutexGuard<()> {
        self.builds[usize::from(no_blobs)].clone().lock_owned().await
    }

    /// Replace the base of its filter. Clones still sending the old one keep
    /// it (and its file) alive until they finish.
    fn install(&self, base: Base) {
        if let Ok(mut bases) = self.bases.lock() {
            tracing::info!(
                no_blobs = base.no_blobs,
                commits = base.commits,
                objects = base.objects,
                bytes = base.len,
                "git export: new base pack cached"
            );
            let slot = usize::from(base.no_blobs);
            bases[slot] = Some(Arc::new(base));
        }
    }
}

/// A clone pack being built as the next base of its filter.
pub struct Recorder {
    cache: Arc<PackCache>,
    no_blobs: bool,
    tips: Vec<ObjectId>,
    commits: usize,
    _build: OwnedMutexGuard<()>,
}

impl Recorder {
    /// Record the pack of `selection` (planned from scratch for a clone,
    /// see `select::select`) while `build` is held; `None` when it cannot
    /// be a base (no tips, or wants without commit metadata).
    pub fn new(
        cache: Arc<PackCache>,
        selection: &Selection,
        build: OwnedMutexGuard<()>,
    ) -> Option<Self> {
        let fits = !selection.tips.is_empty()
            && selection.direct.is_empty()
            && selection.excluded.set.is_empty()
            && selection.excluded.base.is_none();
        fits.then(|| Self {
            cache,
            no_blobs: selection.no_blobs,
            tips: selection.tips.clone(),
            commits: selection.commits.len(),
            _build: build,
        })
    }

    /// Install the complete entries (`len` bytes of `file`, holding the
    /// objects `ids`) as the base, and let waiting clones go on.
    pub fn install(self, ids: Vec<ObjectId>, file: File, len: u64) -> anyhow::Result<()> {
        let base = Base::new(self.no_blobs, self.tips, ids, self.commits, file, len)?;
        self.cache.install(base);
        Ok(())
    }
}
