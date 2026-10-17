//! Pack files: how the disk cache ([`crate::disk`]) stores objects that
//! arrive in large batches (a checkout's tree bundles, batched fetches).
//! One file per batch instead of one per object: a 96k-file checkout stores
//! about 160k objects, and creating, writing and renaming that many small
//! files cost the client more CPU than everything else in a cold checkout.
//!
//! Layout, in `{root}/packs/` (additive to cache format 1: older binaries
//! ignore the directory and fetch what they do not find in `objects/`):
//! - `{name}.pack`: `ANTPACK1`, then the batch's encoded objects back to
//!   back. Records are stored uncompressed: a checkout writes the same
//!   objects out as files right away, and compressing them would cost more
//!   CPU than the disk space is worth.
//! - `{name}.idx`: `ANTPIDX1`, the entry count (u32, big endian), then one
//!   48-byte entry per object, sorted by id: id (32 bytes), offset (u64 BE),
//!   length (u32 BE), flags (u32 BE; bit 0: the record is a zstd frame, as
//!   loose files are; other bits reserved, must be 0).
//! - Both are written under `.tmp.` names and renamed into place, the pack
//!   first and its index last: a pack is part of the cache once its index
//!   exists, so readers never see part of one. A pack without an index is an
//!   interrupted write; eviction removes it after an hour, like temp files.
//! - Readers load every index into memory on first use (about 70 bytes per
//!   object) and list the directory again when an object is missing and the
//!   directory has changed since, so packs written meanwhile (by this or
//!   another process) are found.
//! - Eviction treats a pack as one entry: least recently used by the pack's
//!   mtime (which reads bump at most once per touch interval, like loose
//!   objects), removed index first. A reader that loses a pack to eviction
//!   sees misses and refetches, as with loose objects.
//! - Reads keep up to [`MAX_HANDLES`] packs open, so a checkout reading
//!   ~2,000 packs in 200 batches opens each once rather than once per batch.
//!   An open pack is checked again (`fstat`: still linked? mtime to bump?)
//!   at most once per touch interval: a pack another process evicted stays
//!   readable through the open handle until then (its content is immutable),
//!   after which it is dropped and its objects are misses. Eviction by the
//!   same handle closes its open packs at once.
//! - The records one read wants from a pack are read with one `pread` per
//!   run of nearby records ([`SPAN_GAP`], [`SPAN_MAX`]), not one per object;
//!   the objects share the run's buffer.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, ErrorKind, Write};
use std::os::unix::fs::FileExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use ant_core::{EncodedObject, ObjectId};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use crate::par::par_map;

/// Directory of the packs, under the cache root.
pub(crate) const DIR: &str = "packs";
const PACK_EXT: &str = "pack";
const IDX_EXT: &str = "idx";
const PACK_MAGIC: &[u8; 8] = b"ANTPACK1";
const IDX_MAGIC: &[u8; 8] = b"ANTPIDX1";
const IDX_HEADER: usize = 12;
const ENTRY: usize = 48;
/// Entry flag: the record is a zstd frame of the encoded object.
const ZSTD: u32 = 1;
/// Largest index accepted (entries), as a guard against corrupt counts.
const MAX_ENTRIES: u32 = 1 << 24;
/// A listing of the directory less than this after its last change may
/// have missed a change within the same timestamp tick.
const LISTING_SETTLE: Duration = Duration::from_secs(2);

/// Packs kept open for reading per handle (well below the usual 1024
/// file-descriptor limit).
const MAX_HANDLES: usize = 256;
/// Records of one pack at most this far apart are read with one `pread`.
const SPAN_GAP: u64 = 64 << 10;
/// Longest run read with one `pread` (unless a single record is longer).
const SPAN_MAX: u64 = 4 << 20;

static PACK_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Index entries of one pack (location's pack number not yet assigned).
type Entries = Vec<(ObjectId, Loc)>;
/// Requested positions per pack: pack number, (name, (position, location)).
type Group = (u32, (String, Vec<(usize, Loc)>));
/// Abandoned or in-progress files with their mtimes.
pub(crate) type Abandoned = Vec<(PathBuf, SystemTime)>;

#[derive(Debug, Clone, Copy)]
struct Loc {
    pack: u32,
    off: u64,
    len: u32,
    flags: u32,
}

/// The packs of one cache directory, as this handle knows them.
pub(crate) struct Packs {
    dir: PathBuf,
    state: RwLock<State>,
    /// Packs open for reading, by pack number.
    open: Mutex<HashMap<u32, Handle>>,
}

/// An open pack and when it was last checked (still linked, mtime bumped).
struct Handle {
    file: Arc<File>,
    checked: Instant,
}

/// Why a pack could not be opened.
enum Unavailable {
    /// Removed (evicted): its objects are misses until refetched.
    Gone,
    /// Some other error: misses for this read only.
    Failed,
}

#[derive(Default)]
struct State {
    loaded: bool,
    map: HashMap<ObjectId, Loc>,
    /// Name (without extension) per pack number.
    names: Vec<String>,
    /// Packs found missing (evicted) since they were loaded.
    dead: Vec<bool>,
    known: HashSet<String>,
    /// The directory's mtime when last listed, and when that was.
    listed: Option<(SystemTime, SystemTime)>,
}

impl State {
    /// Record `id` at `loc`, unless it is already in a live pack.
    fn insert(&mut self, id: ObjectId, loc: Loc) {
        let dead = &self.dead;
        self.map
            .entry(id)
            .and_modify(|old| {
                if dead[old.pack as usize] {
                    *old = loc;
                }
            })
            .or_insert(loc);
    }
}

/// One pack on disk, for eviction and statistics.
#[derive(Debug)]
pub(crate) struct PackFile {
    pub(crate) pack: PathBuf,
    pub(crate) idx: PathBuf,
    /// Bytes of the pack and its index.
    pub(crate) size: u64,
    pub(crate) modified: SystemTime,
    pub(crate) objects: u64,
}

impl Packs {
    pub(crate) fn new(root: &Path) -> Self {
        Self {
            dir: root.join(DIR),
            state: RwLock::new(State::default()),
            open: Mutex::new(HashMap::new()),
        }
    }

    /// Close every pack held open for reading (after an eviction, so that
    /// removed packs are misses at once and their space is freed).
    pub(crate) fn close_all(&self) {
        self.open.lock().clear();
    }

    /// Blocking: pack `no` open for reading, checked (and its mtime bumped
    /// when older than `touch`) at most once per `touch`.
    fn handle(&self, no: u32, name: &str, touch: Duration) -> Result<Arc<File>, Unavailable> {
        let now = Instant::now();
        let held = self.open.lock().get(&no).map(|h| (h.file.clone(), h.checked));
        if let Some((file, checked)) = &held
            && now.duration_since(*checked) < touch
        {
            return Ok(file.clone());
        }
        let file = match held {
            Some((file, _)) => file,
            None => match File::open(self.path(name, PACK_EXT)) {
                Ok(f) => Arc::new(f),
                Err(e) if e.kind() == ErrorKind::NotFound => return Err(Unavailable::Gone),
                Err(_) => return Err(Unavailable::Failed),
            },
        };
        let meta = file.metadata().map_err(|_| Unavailable::Failed)?;
        if meta.nlink() == 0 {
            self.open.lock().remove(&no);
            return Err(Unavailable::Gone);
        }
        crate::disk::touch(&file, meta.modified().ok(), touch);
        let mut open = self.open.lock();
        if open.len() >= MAX_HANDLES && !open.contains_key(&no) {
            open.clear();
        }
        open.insert(no, Handle { file: file.clone(), checked: now });
        Ok(file)
    }

    fn path(&self, name: &str, ext: &str) -> PathBuf {
        self.dir.join(format!("{name}.{ext}"))
    }

    fn ensure_loaded(&self) {
        if !self.state.read().loaded {
            self.refresh(true);
        }
    }

    /// List the directory and load indexes not seen yet, unless (`force`
    /// false) it is unchanged since the last listing. True if any were new.
    pub(crate) fn refresh(&self, force: bool) -> bool {
        let mtime = match std::fs::metadata(&self.dir).and_then(|m| m.modified()) {
            Ok(m) => m,
            Err(_) => {
                self.state.write().loaded = true;
                return false;
            }
        };
        let now = SystemTime::now();
        {
            let st = self.state.read();
            if !force
                && st.loaded
                && st.listed.is_some_and(|(m, at)| {
                    m == mtime && at.duration_since(m).is_ok_and(|d| d > LISTING_SETTLE)
                })
            {
                return false;
            }
        }
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            self.state.write().loaded = true;
            return false;
        };
        let fresh: Vec<String> = {
            let st = self.state.read();
            entries
                .flatten()
                .filter_map(|e| {
                    let name = e.file_name().into_string().ok()?;
                    let stem = name.strip_suffix(".idx")?;
                    (!stem.starts_with('.') && !st.known.contains(stem)).then(|| stem.to_owned())
                })
                .collect()
        };
        let read: Vec<(String, Option<Entries>)> = fresh
            .into_iter()
            .map(|stem| {
                let entries = read_index(&self.path(&stem, IDX_EXT));
                (stem, entries)
            })
            .collect();
        let mut st = self.state.write();
        let mut added = false;
        for (stem, entries) in read {
            if st.known.contains(&stem) {
                continue;
            }
            st.known.insert(stem.clone());
            // A vanished or corrupt index: nothing to load (the evictor or
            // the next write deals with the files).
            let Some(entries) = entries else { continue };
            let Ok(no) = u32::try_from(st.names.len()) else { continue };
            st.names.push(stem);
            st.dead.push(false);
            for (id, mut loc) in entries {
                loc.pack = no;
                st.insert(id, loc);
            }
            added = true;
        }
        st.loaded = true;
        st.listed = Some((mtime, now));
        added
    }

    /// Blocking: per id, whether it is in a pack that still exists (each
    /// pack involved is checked on disk once: another process may have
    /// evicted it since its index was loaded).
    pub(crate) fn present(&self, ids: &[ObjectId]) -> Vec<bool> {
        self.ensure_loaded();
        let (packs, names): (Vec<Option<u32>>, HashMap<u32, String>) = {
            let st = self.state.read();
            let mut names = HashMap::new();
            let packs = ids
                .iter()
                .map(|id| {
                    let l = st.map.get(id).filter(|l| !st.dead[l.pack as usize])?;
                    names.entry(l.pack).or_insert_with(|| st.names[l.pack as usize].clone());
                    Some(l.pack)
                })
                .collect();
            (packs, names)
        };
        let gone: HashSet<u32> = names
            .into_iter()
            .filter(|(_, name)| {
                matches!(std::fs::metadata(self.path(name, PACK_EXT)), Err(e) if e.kind() == ErrorKind::NotFound)
            })
            .map(|(no, _)| no)
            .collect();
        if !gone.is_empty() {
            let mut st = self.state.write();
            for no in &gone {
                st.dead[*no as usize] = true;
            }
        }
        packs.into_iter().map(|p| p.is_some_and(|no| !gone.contains(&no))).collect()
    }

    /// Blocking: read `ids` from the packs (`None` where not packed or
    /// unreadable). Packs read from are marked used (mtime bumped) when last
    /// marked over `touch` ago.
    pub(crate) fn read(&self, ids: &[ObjectId], touch: Duration) -> Vec<Option<EncodedObject>> {
        self.ensure_loaded();
        let mut groups: HashMap<u32, (String, Vec<(usize, Loc)>)> = HashMap::new();
        {
            let st = self.state.read();
            if st.map.is_empty() {
                return vec![None; ids.len()];
            }
            for (i, id) in ids.iter().enumerate() {
                if let Some(loc) = st.map.get(id).filter(|l| !st.dead[l.pack as usize]) {
                    groups
                        .entry(loc.pack)
                        .or_insert_with(|| (st.names[loc.pack as usize].clone(), Vec::new()))
                        .1
                        .push((i, *loc));
                }
            }
        }
        let mut out = vec![None; ids.len()];
        if groups.is_empty() {
            return out;
        }
        let groups: Vec<Group> = groups.into_iter().collect();
        let results = par_map(&groups, 1, |(no, (name, locs))| {
            let file = match self.handle(*no, name, touch) {
                Ok(f) => f,
                Err(Unavailable::Gone) => return Err(*no),
                Err(Unavailable::Failed) => return Ok(Vec::new()),
            };
            Ok(read_records(&file, locs))
        });
        let mut gone = Vec::new();
        for r in results {
            match r {
                Ok(found) => {
                    for (i, o) in found {
                        out[i] = o;
                    }
                }
                Err(no) => gone.push(no),
            }
        }
        if !gone.is_empty() {
            let mut st = self.state.write();
            for no in gone {
                st.dead[no as usize] = true;
            }
        }
        out
    }

    /// Blocking: mark the packs holding `ids` as used (see [`Self::read`]).
    pub(crate) fn touch(&self, ids: &[ObjectId], touch: Duration) {
        let names: HashSet<String> = {
            let st = self.state.read();
            ids.iter()
                .filter_map(|id| st.map.get(id))
                .map(|l| st.names[l.pack as usize].clone())
                .collect()
        };
        for name in names {
            if let Ok(f) = File::open(self.path(&name, PACK_EXT)) {
                crate::disk::touch(&f, f.metadata().ok().and_then(|m| m.modified().ok()), touch);
            }
        }
    }

    /// Blocking: store `objects` (ids already established) as one new pack.
    /// Returns the bytes written (pack and index).
    pub(crate) fn write(&self, objects: &[(ObjectId, EncodedObject)]) -> std::io::Result<u64> {
        let name = format!(
            "{:x}-{:x}-{:x}",
            std::process::id(),
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_or(0, |d| d.as_nanos()),
            PACK_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let tmp = |ext: &str| self.dir.join(format!(".tmp.{name}.{ext}"));
        let (tmp_pack, tmp_idx) = (tmp(PACK_EXT), tmp(IDX_EXT));
        let result = (|| {
            let file = match File::create(&tmp_pack) {
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    std::fs::create_dir_all(&self.dir)?;
                    File::create(&tmp_pack)?
                }
                r => r?,
            };
            let mut w = BufWriter::with_capacity(1 << 20, file);
            w.write_all(PACK_MAGIC)?;
            let mut off = PACK_MAGIC.len() as u64;
            let mut entries: Vec<(ObjectId, Loc)> = Vec::with_capacity(objects.len());
            let mut seen = HashSet::with_capacity(objects.len());
            for (id, obj) in objects {
                if !seen.insert(*id) {
                    continue;
                }
                let data = obj.as_bytes();
                let len = u32::try_from(data.len())
                    .map_err(|_| std::io::Error::other("object too large for a pack"))?;
                w.write_all(data)?;
                entries.push((*id, Loc { pack: 0, off, len, flags: 0 }));
                off += u64::from(len);
            }
            w.into_inner().map_err(|e| e.into_error())?;
            entries.sort_unstable_by_key(|(id, _)| *id);
            let count = u32::try_from(entries.len())
                .ok()
                .filter(|n| *n <= MAX_ENTRIES)
                .ok_or_else(|| std::io::Error::other("too many objects for one pack"))?;
            let mut idx = Vec::with_capacity(IDX_HEADER + entries.len() * ENTRY);
            idx.extend_from_slice(IDX_MAGIC);
            idx.extend_from_slice(&count.to_be_bytes());
            for (id, loc) in &entries {
                idx.extend_from_slice(id.as_bytes());
                idx.extend_from_slice(&loc.off.to_be_bytes());
                idx.extend_from_slice(&loc.len.to_be_bytes());
                idx.extend_from_slice(&loc.flags.to_be_bytes());
            }
            std::fs::write(&tmp_idx, &idx)?;
            std::fs::rename(&tmp_pack, self.path(&name, PACK_EXT))?;
            std::fs::rename(&tmp_idx, self.path(&name, IDX_EXT))?;
            Ok((entries, off + idx.len() as u64))
        })();
        let (entries, bytes) = match result {
            Ok(r) => r,
            Err(e) => {
                let _ = std::fs::remove_file(&tmp_pack);
                let _ = std::fs::remove_file(&tmp_idx);
                let _ = std::fs::remove_file(self.path(&name, PACK_EXT));
                return Err(e);
            }
        };
        self.ensure_loaded();
        let mut st = self.state.write();
        if let Ok(no) = u32::try_from(st.names.len()) {
            st.known.insert(name.clone());
            st.names.push(name);
            st.dead.push(false);
            for (id, mut loc) in entries {
                loc.pack = no;
                st.insert(id, loc);
            }
        }
        Ok(bytes)
    }
}

/// Blocking: the records at `locs` (request positions with locations in
/// one pack), decoded, by position; `None` where unreadable or malformed.
/// Nearby records are read together: one `pread` per run.
fn read_records(file: &File, locs: &[(usize, Loc)]) -> Vec<(usize, Option<EncodedObject>)> {
    let mut sorted: Vec<&(usize, Loc)> = locs.iter().collect();
    sorted.sort_unstable_by_key(|(_, l)| l.off);
    let mut out = Vec::with_capacity(locs.len());
    let mut i = 0;
    while i < sorted.len() {
        let start = sorted[i].1.off;
        let mut end = start + u64::from(sorted[i].1.len);
        let mut j = i + 1;
        while let Some((_, l)) = sorted.get(j) {
            let l_end = l.off + u64::from(l.len);
            if l.off > end.saturating_add(SPAN_GAP) || l_end.max(end) - start > SPAN_MAX {
                break;
            }
            end = end.max(l_end);
            j += 1;
        }
        let run = usize::try_from(end - start).ok().and_then(|n| {
            let mut buf = vec![0u8; n];
            file.read_exact_at(&mut buf, start).ok().map(|()| Bytes::from(buf))
        });
        for (pos, loc) in &sorted[i..j] {
            let rel = (loc.off - start) as usize;
            let obj = run
                .as_ref()
                .and_then(|b| decode_record(b.slice(rel..rel + loc.len as usize), loc.flags));
            out.push((*pos, obj));
        }
        i = j;
    }
    out
}

/// One record's bytes, decoded; `None` if malformed.
fn decode_record(data: Bytes, flags: u32) -> Option<EncodedObject> {
    let raw = match flags {
        0 => data,
        ZSTD => Bytes::from(crate::disk::decompress(&data)?),
        _ => return None,
    };
    EncodedObject::from_bytes(raw).ok()
}

/// Blocking: the entries of the index at `path`; `None` if missing or
/// malformed.
fn read_index(path: &Path) -> Option<Entries> {
    let data = std::fs::read(path).ok()?;
    let count = parse_header(&data)?;
    let body = &data[IDX_HEADER..];
    if body.len() != count as usize * ENTRY {
        return None;
    }
    let mut out = Vec::with_capacity(count as usize);
    for e in body.chunks_exact(ENTRY) {
        let id = ObjectId::from_bytes(&e[..32]).ok()?;
        let off = u64::from_be_bytes(e[32..40].try_into().ok()?);
        let len = u32::from_be_bytes(e[40..44].try_into().ok()?);
        let flags = u32::from_be_bytes(e[44..48].try_into().ok()?);
        out.push((id, Loc { pack: 0, off, len, flags }));
    }
    Some(out)
}

fn parse_header(data: &[u8]) -> Option<u32> {
    if data.len() < IDX_HEADER || &data[..8] != IDX_MAGIC {
        return None;
    }
    let count = u32::from_be_bytes(data[8..12].try_into().ok()?);
    (count <= MAX_ENTRIES).then_some(count)
}

/// Blocking: the complete packs under `root` (for eviction and statistics),
/// plus in-progress or abandoned files (temp files, packs without an index)
/// with their mtimes.
pub(crate) fn scan(root: &Path) -> std::io::Result<(Vec<PackFile>, Abandoned)> {
    let dir = root.join(DIR);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok((Vec::new(), Vec::new())),
        Err(e) => return Err(e),
    };
    let mut packs: HashMap<String, (Option<std::fs::Metadata>, Option<std::fs::Metadata>)> =
        HashMap::new();
    let mut loose = Vec::new();
    for e in entries.flatten() {
        let Ok(name) = e.file_name().into_string() else { continue };
        let Ok(meta) = e.metadata() else { continue };
        let Ok(modified) = meta.modified() else { continue };
        if name.starts_with('.') {
            loose.push((e.path(), modified));
        } else if let Some(stem) = name.strip_suffix(".pack") {
            packs.entry(stem.to_owned()).or_default().0 = Some(meta);
        } else if let Some(stem) = name.strip_suffix(".idx") {
            packs.entry(stem.to_owned()).or_default().1 = Some(meta);
        }
    }
    let mut out = Vec::new();
    for (stem, (pack, idx)) in packs {
        let (pack_path, idx_path) =
            (dir.join(format!("{stem}.{PACK_EXT}")), dir.join(format!("{stem}.{IDX_EXT}")));
        match (pack, idx) {
            (Some(p), Some(i)) => {
                let objects = File::open(&idx_path)
                    .ok()
                    .and_then(|f| {
                        let mut head = [0u8; IDX_HEADER];
                        f.read_exact_at(&mut head, 0).ok()?;
                        parse_header(&head)
                    })
                    .map_or(0, u64::from);
                out.push(PackFile {
                    pack: pack_path,
                    idx: idx_path,
                    size: p.len() + i.len(),
                    modified: p.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                    objects,
                });
            }
            (Some(p), None) => {
                loose.push((pack_path, p.modified().unwrap_or(SystemTime::UNIX_EPOCH)))
            }
            (None, Some(i)) => {
                // An index whose pack is gone: useless, remove it with the
                // abandoned files (it is old, or its pack is being removed).
                loose.push((idx_path, i.modified().unwrap_or(SystemTime::UNIX_EPOCH)))
            }
            (None, None) => {}
        }
    }
    Ok((out, loose))
}
