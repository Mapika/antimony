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
//! per filter (`blob:none` or none). Nothing is written to the stores.
//!
//! Persistence: with a server cache (`ant_store::server_cache`; `ant serve
//! --cache-dir`, `--shared-cache`) every new base is also written there in
//! the background (entry `<filter>-f<PERSIST_FORMAT>-<hash of the tips>`,
//! then the pointer `git-<filter>` to it; the entry it replaces is
//! deleted), and a server without a base of a filter loads the pointed-to
//! one before its first clone with that filter (or at start, see
//! `Export::prewarm`). The entry's SHA-256 is checked while it is loaded; a
//! damaged one is dropped and the clone builds a new base. A loaded base is
//! used under exactly the rule of one built here (`select::select_after`):
//! only when the wants reach every one of its tips and every want is a
//! commit the store still has. Garbage collection keeps the closure of
//! every object it keeps (and readers see condemned objects as missing), so
//! a want the store has reaches only live objects, and the base holds
//! exactly what its tips (reached from the wants) reach: a base, however
//! old, never serves objects garbage collection has removed.
//!
//! Disk and memory: a base is an unlinked temporary file (about the size of
//! its pack), or its entry in the disk cache, plus 32 bytes of id per
//! object. At most one per filter is installed; a replaced base is freed
//! when the last clone streaming it finishes. A ref move does not drop a
//! base (it stays exact for the commits it was built for); the next clone
//! that does not fit it replaces it.
//!
//! Access: bases are shared by every requester. That is sound because the
//! export serves one view, the whole repository (see the crate docs); an
//! export with per-principal views would have to key bases by view.
use std::{
    fs::File,
    os::unix::fs::FileExt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use ant_core::ObjectId;
use ant_store::server_cache::{Kind, ServerCache, Source};
use anyhow::{Context, bail, ensure};
use sha2::{Digest, Sha256};
use tokio::sync::OwnedMutexGuard;

use crate::select::Selection;

/// A new base is built once the wants are this fraction of the base's
/// commits past it (1/8), plus a constant...
pub const REBUILD_COMMITS_FRACTION: usize = 8;
/// ...so that small histories are not rebuilt on every commit.
pub const REBUILD_COMMITS_MIN: usize = 1024;

/// Version of a persisted base (its meta layout, and what a base holds);
/// part of every entry's key, so a change never reads older entries.
pub const PERSIST_FORMAT: u32 = 1;

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
    /// Pack entries without header and trailer: `len` bytes from `offset`.
    file: File,
    offset: u64,
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
        Ok(Self { no_blobs, tips, ids, commits, objects, file, offset: 0, len })
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
        self.file.read_exact_at(buf, self.offset + offset)
    }

    fn filter_name(no_blobs: bool) -> &'static str {
        if no_blobs { "blobless" } else { "full" }
    }

    /// The server-cache pointer to the persisted base of a filter.
    pub fn pointer_name(no_blobs: bool) -> String {
        format!("git-{}", Self::filter_name(no_blobs))
    }

    /// Key of this base in the server cache: filter, [`PERSIST_FORMAT`] and
    /// a hash of the tips.
    pub fn persist_key(&self) -> String {
        let mut tips = self.tips.clone();
        tips.sort_unstable();
        let mut h = Sha256::new();
        for tip in &tips {
            h.update(tip.0);
        }
        let digest: String = h.finalize()[..16].iter().map(|b| format!("{b:02x}")).collect();
        format!("{}-f{PERSIST_FORMAT}-{digest}", Self::filter_name(self.no_blobs))
    }

    /// The persisted description: filter (u8), commits (u64 LE), tips and
    /// object ids (each a u32 LE count and 32-byte ids; ids sorted).
    pub fn meta(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(17 + 32 * (self.tips.len() + self.ids.len()));
        out.push(u8::from(self.no_blobs));
        out.extend_from_slice(&(self.commits as u64).to_le_bytes());
        for ids in [&self.tips, &self.ids] {
            out.extend_from_slice(&(ids.len() as u32).to_le_bytes());
            for id in ids.iter() {
                out.extend_from_slice(&id.0);
            }
        }
        out
    }

    /// A base from a persisted entry: [`Self::meta`] and the entries in
    /// `file` (`len` bytes from `offset`).
    pub fn from_persisted(meta: &[u8], file: File, offset: u64, len: u64) -> anyhow::Result<Self> {
        let mut r = meta;
        fn take<'a>(r: &mut &'a [u8], n: usize) -> anyhow::Result<&'a [u8]> {
            ensure!(r.len() >= n, "persisted base: short meta");
            let (head, rest) = r.split_at(n);
            *r = rest;
            Ok(head)
        }
        fn ids(r: &mut &[u8]) -> anyhow::Result<Vec<ObjectId>> {
            let n = u32::from_le_bytes(take(r, 4)?.try_into()?) as usize;
            let raw = take(r, n.checked_mul(32).context("persisted base: too many ids")?)?;
            Ok(raw
                .chunks_exact(32)
                .filter_map(|c| <[u8; 32]>::try_from(c).ok())
                .map(ObjectId)
                .collect())
        }
        let no_blobs = match take(&mut r, 1)?[0] {
            0 => false,
            1 => true,
            _ => bail!("persisted base: bad filter"),
        };
        let commits = usize::try_from(u64::from_le_bytes(take(&mut r, 8)?.try_into()?))?;
        let tips = ids(&mut r)?;
        let ids = ids(&mut r)?;
        ensure!(r.is_empty(), "persisted base: trailing meta");
        ensure!(!tips.is_empty(), "persisted base: no tips");
        ensure!(ids.windows(2).all(|w| w[0] < w[1]), "persisted base: ids not sorted");
        let objects = u32::try_from(ids.len())?;
        Ok(Self { no_blobs, tips, ids, commits, objects, file, offset, len })
    }
}

/// The bases of one export, one per filter.
#[derive(Default)]
pub struct PackCache {
    bases: Mutex<[Option<Arc<Base>>; 2]>,
    /// Held while a base of the filter is built: clones arriving meanwhile
    /// wait for it instead of building the same pack at the same time.
    builds: [Arc<tokio::sync::Mutex<()>>; 2],
    /// Where bases are persisted (see the module docs).
    persist: Option<Arc<ServerCache>>,
    /// Whether loading the persisted base of a filter was tried.
    loaded: [AtomicBool; 2],
}

impl PackCache {
    /// A cache whose bases are persisted in (and loaded from) `persist`.
    pub fn with_persistence(persist: Option<Arc<ServerCache>>) -> Self {
        Self { persist, ..Self::default() }
    }

    pub fn get(&self, no_blobs: bool) -> Option<Arc<Base>> {
        self.bases.lock().ok()?[usize::from(no_blobs)].clone()
    }

    /// Wait until no base of this filter is being built; the guard makes
    /// the caller the builder.
    pub async fn lock_build(&self, no_blobs: bool) -> OwnedMutexGuard<()> {
        self.builds[usize::from(no_blobs)].clone().lock_owned().await
    }

    /// Install the persisted base of a filter, unless one is cached already
    /// (tried once per process and filter; call it holding the filter's
    /// build lock). An absent, damaged or unreadable entry leaves the cache
    /// as it was.
    pub async fn load_persisted(&self, no_blobs: bool) {
        let slot = usize::from(no_blobs);
        let Some(persist) = &self.persist else { return };
        if self.loaded[slot].swap(true, Ordering::AcqRel) || self.get(no_blobs).is_some() {
            return;
        }
        let started = std::time::Instant::now();
        let Some(key) = persist.pointer(&Base::pointer_name(no_blobs)).await else { return };
        let Some(entry) = persist.get(Kind::GitBase, &key).await else { return };
        let loaded = tokio::task::spawn_blocking(move || {
            let meta = entry.meta.clone();
            let (file, offset, len) = entry.into_file()?;
            Base::from_persisted(&meta, file, offset, len)
        })
        .await;
        match loaded {
            Ok(Ok(base)) if base.no_blobs == no_blobs => {
                tracing::info!(
                    no_blobs,
                    commits = base.commits,
                    objects = base.objects,
                    bytes = base.len,
                    load_ms = started.elapsed().as_millis() as u64,
                    "git export: persisted base pack loaded"
                );
                if let Ok(mut bases) = self.bases.lock()
                    && bases[slot].is_none()
                {
                    bases[slot] = Some(Arc::new(base));
                }
            }
            Ok(Ok(_)) => tracing::warn!(key, "git export: persisted base has another filter"),
            Ok(Err(e)) => tracing::warn!(key, error = %e, "git export: persisted base unusable"),
            Err(e) => tracing::warn!(key, error = %e, "git export: loading persisted base failed"),
        }
    }

    /// Replace the base of its filter (and persist it). Clones still
    /// sending the old one keep it (and its file) alive until they finish.
    fn install(&self, base: Base) {
        let base = Arc::new(base);
        self.persist(&base);
        if let Ok(mut bases) = self.bases.lock() {
            tracing::info!(
                no_blobs = base.no_blobs,
                commits = base.commits,
                objects = base.objects,
                bytes = base.len,
                "git export: new base pack cached"
            );
            let slot = usize::from(base.no_blobs);
            bases[slot] = Some(base);
        }
    }

    /// Write a new base to the server cache in the background, then point
    /// the filter's pointer at it and delete the entry it pointed to.
    fn persist(&self, base: &Arc<Base>) {
        let Some(persist) = self.persist.clone() else { return };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else { return };
        let file = match base.file.try_clone() {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(error = %e, "git export: base pack not persisted");
                return;
            }
        };
        let (key, name, meta) =
            (base.persist_key(), Base::pointer_name(base.no_blobs), base.meta());
        let source = Source::File { file, offset: base.offset, len: base.len };
        runtime.spawn(async move {
            let started = std::time::Instant::now();
            let old = persist.pointer(&name).await;
            if !persist.put(Kind::GitBase, &key, meta, source).await {
                return;
            }
            persist.set_pointer(&name, &key).await;
            if let Some(old) = old.filter(|old| *old != key) {
                persist.remove(Kind::GitBase, &old).await;
            }
            tracing::info!(
                key,
                persist_ms = started.elapsed().as_millis() as u64,
                "git export: base pack persisted"
            );
        });
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn id(i: u8) -> ObjectId {
        ObjectId([i; 32])
    }

    #[test]
    fn persisted_meta_round_trips_and_rejects_damage() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"xxENTRIES").unwrap();
        let base =
            Base::new(true, vec![id(9), id(3)], vec![id(5), id(1), id(3), id(9)], 2, file, 7)
                .unwrap();
        let meta = base.meta();
        let file = base.file.try_clone().unwrap();
        let back = Base::from_persisted(&meta, file, 2, 7).unwrap();
        assert!(back.no_blobs);
        assert_eq!((back.tips.clone(), back.commits, back.objects), (base.tips.clone(), 2, 4));
        assert!(back.contains(&id(5)) && !back.contains(&id(2)));
        let mut buf = [0u8; 7];
        back.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"ENTRIES", "offsets are relative to the entries");
        assert_eq!(back.persist_key(), base.persist_key());
        assert!(base.persist_key().starts_with("blobless-f1-"));
        assert!(ant_store::server_cache::valid_key(&base.persist_key()));

        let file = || tempfile::tempfile().unwrap();
        assert!(Base::from_persisted(&meta[..meta.len() - 1], file(), 0, 0).is_err());
        let mut long = meta.clone();
        long.push(0);
        assert!(Base::from_persisted(&long, file(), 0, 0).is_err());
        let mut bad = meta.clone();
        bad[0] = 2;
        assert!(Base::from_persisted(&bad, file(), 0, 0).is_err());
        // Ids out of order (swap the first two).
        let mut unsorted = meta.clone();
        let at = 1 + 8 + 4 + 2 * 32 + 4;
        let (a, b) = (unsorted[at..at + 32].to_vec(), unsorted[at + 32..at + 64].to_vec());
        unsorted[at..at + 32].copy_from_slice(&b);
        unsorted[at + 32..at + 64].copy_from_slice(&a);
        assert!(Base::from_persisted(&unsorted, file(), 0, 0).is_err());
    }
}
