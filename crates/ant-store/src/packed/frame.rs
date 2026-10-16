//! Pack records: the unit an `object_index` row points at.
//!
//! A pack is a concatenation of records. Two kinds exist, told apart by
//! their first bytes:
//!
//! - **Single object** (every pack written before 0.2.0, and objects that
//!   share a record with nothing): one zstd frame holding the encoded
//!   object. Starts with the zstd magic `28 b5 2f fd`.
//! - **Frame** (0.2.0): several objects compressed as one zstd stream. The
//!   writer puts similar objects next to each other (versions of one file or
//!   directory, see [`similarity_key`]), so zstd stores each of them as a
//!   handful of back-references to its neighbour: the effect of git's deltas
//!   without base chains. A frame decodes on its own, from its own bytes;
//!   nothing outside the record is needed, so deleting other records (or
//!   other members' index rows) never breaks it. Layout, integers little
//!   endian:
//!
//!   ```text
//!   magic "AFR1" | count u32 | raw_len u32 | count x (id[0..8], len u32) | zstd(member bytes)
//!   ```
//!
//!   Members are stored in the listed order; `raw_len` is the sum of their
//!   lengths and at most [`MAX_FRAME_RAW`]. Every member's index row
//!   carries the frame's own offset and length, and a reader picks the
//!   member out by its id prefix (verifying the full hash).
//!
//! Reading one member costs one ranged GET of the record and decompressing
//! at most `raw_len` bytes: the bound replacing a delta chain depth.
//!
//! - **Delta frame** (0.2.0, written only by `ant admin repack`): like a
//!   frame, but a member may be stored as git delta instructions
//!   ([`ant_core::delta`]) against an earlier member of the same record, so
//!   versions of a file collected from the whole history cost little more
//!   than their differences. Layout, integers little endian:
//!
//!   ```text
//!   magic "ADF1" | count u32 | raw_len u32
//!     | count x (id[0..8], base u32, len u32, size u32) | zstd(member bytes)
//!   ```
//!
//!   Member `i` occupies the next `len` bytes of the decompressed stream.
//!   With `base` = `u32::MAX` they are the encoded object (`size` is its body
//!   length); otherwise `base` < `i` names the member whose *body* the delta
//!   instructions apply to, the result is member `i`'s body (`size` bytes),
//!   and member `i` has the kind of the full member its chain starts from.
//!   Bases always come earlier in the record, so chains end, and they never
//!   leave the record: a delta frame, too, decodes from its own bytes alone.
//!   Writers bound chains (`repack --depth`); readers refuse chains longer
//!   than [`MAX_CHAIN`] and bodies larger than [`MAX_MEMBER`].
//!
//! Corrupt records are errors, never panics.

use std::collections::HashMap;

use ant_core::{EncodedObject, ObjectId, ObjectKind, delta};
use bytes::Bytes;
use parking_lot::Mutex;

use crate::{Result, StoreError, ZSTD_LEVEL};

/// First bytes of a frame record.
pub(crate) const FRAME_MAGIC: [u8; 4] = *b"AFR1";
/// First bytes of a delta frame record.
pub(crate) const DELTA_MAGIC: [u8; 4] = *b"ADF1";
/// `base` of a delta-frame member stored whole.
pub(crate) const NO_BASE: u32 = u32::MAX;
/// Bytes per member in a delta frame's header.
const DELTA_MEMBER: usize = 20;
/// Longest delta chain a reader follows (writers use far less).
pub(crate) const MAX_CHAIN: usize = 1000;
/// Largest member body a reader rebuilds.
pub(crate) const MAX_MEMBER: usize = 64 * 1024 * 1024;
/// Bytes a reader rebuilds along one chain at most: a corrupt record must not
/// make it copy gigabytes.
const MAX_CHAIN_WORK: usize = 1024 * 1024 * 1024;
/// Rebuilt bodies of base members kept per decoded delta frame, so reading
/// several members of one chain applies each delta once.
const MEMO_BYTES: usize = 8 * 1024 * 1024;
/// First bytes of a single-object record (the zstd frame magic).
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
/// Bytes of the fixed frame header (magic, count, raw length).
const HEADER: usize = 12;
/// Bytes per member in the frame header.
const MEMBER: usize = 12;
/// Uncompressed bytes a writer puts into one frame at most. Bounds the work
/// of reading one member (and the decoded-frame cache entries).
pub const FRAME_RAW: usize = 2 * 1024 * 1024;
/// Largest `raw_len` a reader accepts: a corrupt header must not make it
/// allocate gigabytes. Larger than [`FRAME_RAW`] so the writer's target can
/// grow without making older packs unreadable.
pub(crate) const MAX_FRAME_RAW: usize = 64 * 1024 * 1024;
/// Bytes of a blob or commit looked at for its similarity key.
const KEY_SCAN: usize = 64 * 1024;
/// Lines shorter than this (after trimming) are too common (`}`, `#endif`)
/// to say which file a blob is a version of.
const KEY_MIN_LINE: usize = 12;

pub(crate) fn corrupt(what: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(format!("corrupt pack record: {what}").into())
}

/// FNV-1a: cheap, stable across versions (keys are only used for grouping
/// within one write, never stored).
fn fnv(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// A key under which versions of the same file or directory are likely to
/// collide, and unrelated objects are not: a one-hash MinHash over a set of
/// features of the content.
///
/// - Trees: the smallest hash of an entry name. A directory keeps most of its
///   names from one version to the next, and the key only changes when the
///   name holding the minimum is removed or a smaller one is added.
/// - Blobs and commits: the smallest hash of a line of at least
///   [`KEY_MIN_LINE`] bytes in the first [`KEY_SCAN`] bytes; binary content
///   without such lines falls back to its first bytes.
///
/// Objects that collide by accident end up compressed together, which costs
/// a little ratio and nothing else.
pub(crate) fn similarity_key(o: &EncodedObject) -> u64 {
    let body = o.body();
    match o.kind() {
        ObjectKind::Tree => tree_key(body),
        ObjectKind::Blob | ObjectKind::Commit => text_key(body),
    }
}

fn tree_key(body: &[u8]) -> u64 {
    let mut best = u64::MAX;
    let mut rest = body;
    while !rest.is_empty() {
        let Some(sp) = rest.iter().position(|&b| b == b' ') else { break };
        let Some(nul) = rest[sp..].iter().position(|&b| b == 0).map(|n| sp + n) else { break };
        best = best.min(fnv(&rest[sp + 1..nul]));
        let next = nul + 1 + ObjectId::LEN;
        if next > rest.len() {
            break;
        }
        rest = &rest[next..];
    }
    best
}

fn text_key(body: &[u8]) -> u64 {
    let head = &body[..body.len().min(KEY_SCAN)];
    let best = head
        .split(|&b| b == b'\n')
        .map(<[u8]>::trim_ascii)
        .filter(|l| l.len() >= KEY_MIN_LINE)
        .map(fnv)
        .min();
    best.unwrap_or_else(|| fnv(&head[..head.len().min(64)]))
}

/// Which objects share a record: `objects` are `(kind, key, encoded length)`
/// in write order; the result lists, per record, indices into `objects`.
///
/// Objects are ordered by kind and key, and within one key newest first
/// (the reverse of write order, which for imports is history order), so the
/// version a checkout wants usually starts its group. A frame only ever
/// holds one group (objects with the same kind and key), so readers that
/// walk history path by path find all the versions they want in the frames
/// they decode, rather than decoding a frame of unrelated objects once per
/// path. Groups are cut greedily at [`FRAME_RAW`] (`cap`) uncompressed
/// bytes. An object without a look-alike, or of at least `cap / 2` bytes
/// (it would leave little room for neighbours, and alone it is read without
/// decoding anything else), gets a record of its own.
pub(crate) fn plan(objects: &[(ObjectKind, u64, usize)], cap: usize) -> Vec<Vec<usize>> {
    let rank = |k: ObjectKind| match k {
        ObjectKind::Tree => 0u8,
        ObjectKind::Blob => 1,
        ObjectKind::Commit => 2,
    };
    let group = |i: usize| (rank(objects[i].0), objects[i].1);
    let mut order: Vec<usize> = (0..objects.len()).collect();
    order.sort_by_key(|&i| (group(i), std::cmp::Reverse(i)));
    let mut records: Vec<Vec<usize>> = Vec::new();
    for same in order.chunk_by(|&a, &b| group(a) == group(b)) {
        let mut cur: Vec<usize> = Vec::new();
        let mut cur_len = 0usize;
        for &i in same {
            let len = objects[i].2;
            if len >= cap / 2 {
                records.push(vec![i]);
                continue;
            }
            if !cur.is_empty() && cur_len + len > cap {
                records.push(std::mem::take(&mut cur));
                cur_len = 0;
            }
            cur.push(i);
            cur_len += len;
        }
        if !cur.is_empty() {
            records.push(cur);
        }
    }
    records
}

/// Encode one record holding `members` (in this order): a single-object
/// record for one member, a frame otherwise.
pub(crate) fn encode(members: &[&EncodedObject], ids: &[ObjectId]) -> Result<Vec<u8>> {
    debug_assert_eq!(members.len(), ids.len());
    if let [only] = members {
        return zstd::bulk::compress(only.as_bytes(), ZSTD_LEVEL).map_err(StoreError::backend);
    }
    let raw_len: usize = members.iter().map(|o| o.as_bytes().len()).sum();
    if members.is_empty() || raw_len > MAX_FRAME_RAW {
        return Err(StoreError::InvalidArgument(format!(
            "frame of {} objects, {raw_len} bytes",
            members.len()
        )));
    }
    let mut raw = Vec::with_capacity(raw_len);
    for o in members {
        raw.extend_from_slice(o.as_bytes());
    }
    let z = zstd::bulk::compress(&raw, ZSTD_LEVEL).map_err(StoreError::backend)?;
    let mut out = Vec::with_capacity(HEADER + MEMBER * members.len() + z.len());
    out.extend_from_slice(&FRAME_MAGIC);
    out.extend_from_slice(&(members.len() as u32).to_le_bytes());
    out.extend_from_slice(&(raw_len as u32).to_le_bytes());
    for (o, id) in members.iter().zip(ids) {
        out.extend_from_slice(&id.as_bytes()[..8]);
        out.extend_from_slice(&(o.as_bytes().len() as u32).to_le_bytes());
    }
    out.extend_from_slice(&z);
    Ok(out)
}

/// Whether `record` is a frame (as opposed to a single object).
pub(crate) fn is_frame(record: &[u8]) -> bool {
    record.starts_with(&FRAME_MAGIC)
}

/// Whether `record` is a delta frame.
pub(crate) fn is_delta_frame(record: &[u8]) -> bool {
    record.starts_with(&DELTA_MAGIC)
}

/// The uncompressed size of a frame or delta frame record, from its header
/// (`None` for a single-object record or a truncated header).
pub(crate) fn frame_raw_len(record: &[u8]) -> Option<usize> {
    if !(is_frame(record) || is_delta_frame(record)) || record.len() < HEADER {
        return None;
    }
    Some(u32_at(record, 8) as usize)
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    let mut w = [0u8; 4];
    w.copy_from_slice(&b[at..at + 4]);
    u32::from_le_bytes(w)
}

/// A decoded frame: its members' id prefixes and byte ranges, and their
/// uncompressed bytes.
#[derive(Clone)]
pub(crate) struct Frame {
    members: Vec<([u8; 8], usize, usize)>,
    raw: Bytes,
}

impl Frame {
    /// Bytes held (for cache accounting).
    pub fn weight(&self) -> usize {
        self.raw.len() + self.members.len() * std::mem::size_of::<([u8; 8], usize, usize)>()
    }

    /// Bytes of the members with `id`'s prefix (0 if none).
    pub fn member_len(&self, id: &ObjectId) -> usize {
        let prefix = &id.as_bytes()[..8];
        self.members.iter().filter(|m| m.0 == prefix).map(|m| m.2 - m.1).sum()
    }

    /// The member `id`, verified, copied out (so the result does not pin the
    /// whole frame in memory); `None` when no member has this id.
    pub fn get(&self, id: &ObjectId) -> Option<Result<EncodedObject>> {
        let prefix = &id.as_bytes()[..8];
        let mut last_err = None;
        for &(p, start, end) in &self.members {
            if p != prefix {
                continue;
            }
            let bytes = Bytes::copy_from_slice(&self.raw[start..end]);
            match EncodedObject::from_bytes(bytes).and_then(|o| o.verify(id).map(|()| o)) {
                Ok(o) => return Some(Ok(o)),
                // Another member may share the 8-byte prefix.
                Err(e) => last_err = Some(e),
            }
        }
        last_err.map(|e| Err(corrupt(format!("object {id}: {e}"))))
    }
}

/// Decode a frame record completely.
pub(crate) fn decode_frame(record: &[u8]) -> Result<Frame> {
    if !is_frame(record) {
        return Err(corrupt("not a frame"));
    }
    if record.len() < HEADER {
        return Err(corrupt("truncated frame header"));
    }
    let count = u32_at(record, 4) as usize;
    let raw_len = u32_at(record, 8) as usize;
    if count == 0 || raw_len > MAX_FRAME_RAW {
        return Err(corrupt(format!("frame of {count} objects, {raw_len} bytes")));
    }
    let table_end = count
        .checked_mul(MEMBER)
        .and_then(|t| t.checked_add(HEADER))
        .filter(|&end| end <= record.len())
        .ok_or_else(|| corrupt(format!("truncated frame table ({count} objects)")))?;
    let mut members = Vec::with_capacity(count);
    let mut at = 0usize;
    for m in record[HEADER..table_end].chunks_exact(MEMBER) {
        let mut prefix = [0u8; 8];
        prefix.copy_from_slice(&m[..8]);
        let len = u32_at(m, 8) as usize;
        let end = at
            .checked_add(len)
            .filter(|&e| e <= raw_len)
            .ok_or_else(|| corrupt(format!("frame members exceed its {raw_len} bytes")))?;
        members.push((prefix, at, end));
        at = end;
    }
    if at != raw_len {
        return Err(corrupt(format!("frame members hold {at} of its {raw_len} bytes")));
    }
    let raw = zstd::bulk::decompress(&record[table_end..], raw_len)
        .map_err(|e| corrupt(format!("frame data: {e}")))?;
    if raw.len() != raw_len {
        return Err(corrupt(format!("frame data is {} bytes, header says {raw_len}", raw.len())));
    }
    Ok(Frame { members, raw: Bytes::from(raw) })
}

/// Decode a single-object record and verify it is `id`.
pub(crate) fn decode_single(id: ObjectId, record: &[u8]) -> Result<EncodedObject> {
    if !record.starts_with(&ZSTD_MAGIC) {
        return Err(corrupt(format!("object {id}: unknown record kind")));
    }
    crate::s3::S3ObjectStore::decode_object(id, record)
}

// -- delta frames --------------------------------------------------------

/// One member of a delta frame to write.
pub(crate) struct DeltaMember<'a> {
    pub id: ObjectId,
    /// Earlier member whose body `bytes` (delta instructions) apply to;
    /// `None`: `bytes` is the encoded object.
    pub base: Option<u32>,
    pub bytes: &'a [u8],
    /// Body length of the object.
    pub size: usize,
}

/// Encode a delta frame of `members` (in this order), compressed at zstd
/// `level`.
pub(crate) fn encode_delta(members: &[DeltaMember<'_>], level: i32) -> Result<Vec<u8>> {
    let raw_len: usize = members.iter().map(|m| m.bytes.len()).sum();
    if members.is_empty() || raw_len > MAX_FRAME_RAW || members.len() > u32::MAX as usize / 2 {
        return Err(StoreError::InvalidArgument(format!(
            "delta frame of {} objects, {raw_len} bytes",
            members.len()
        )));
    }
    let mut raw = Vec::with_capacity(raw_len);
    let mut out = Vec::with_capacity(HEADER + DELTA_MEMBER * members.len() + raw_len / 4);
    out.extend_from_slice(&DELTA_MAGIC);
    out.extend_from_slice(&(members.len() as u32).to_le_bytes());
    out.extend_from_slice(&(raw_len as u32).to_le_bytes());
    for (i, m) in members.iter().enumerate() {
        let base = match m.base {
            Some(b) if (b as usize) < i => b,
            None => NO_BASE,
            Some(b) => {
                return Err(StoreError::InvalidArgument(format!(
                    "delta frame member {i} based on member {b}"
                )));
            }
        };
        if m.size > MAX_MEMBER {
            return Err(StoreError::InvalidArgument(format!("delta frame member of {}", m.size)));
        }
        out.extend_from_slice(&m.id.as_bytes()[..8]);
        out.extend_from_slice(&base.to_le_bytes());
        out.extend_from_slice(&(m.bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&(m.size as u32).to_le_bytes());
        raw.extend_from_slice(m.bytes);
    }
    let z = zstd::bulk::compress(&raw, level).map_err(StoreError::backend)?;
    out.extend_from_slice(&z);
    Ok(out)
}

/// A member of a decoded delta frame.
#[derive(Clone, Copy)]
struct DeltaEntry {
    prefix: [u8; 8],
    base: u32,
    start: usize,
    end: usize,
    size: usize,
    /// Some later member is based on this one.
    is_base: bool,
}

/// Rebuilt bodies of base members, by member index.
#[derive(Default)]
struct Memo {
    bodies: HashMap<u32, (ObjectKind, Bytes)>,
    bytes: usize,
}

/// A decoded delta frame: its member table and decompressed stream. Members
/// are rebuilt on request ([`DeltaFrame::get`]).
pub(crate) struct DeltaFrame {
    members: Vec<DeltaEntry>,
    raw: Bytes,
    memo: Mutex<Memo>,
    memo_limit: usize,
}

impl DeltaFrame {
    /// Bytes held (for cache accounting), including the memo's budget.
    pub fn weight(&self) -> usize {
        self.raw.len() + self.members.len() * std::mem::size_of::<DeltaEntry>() + self.memo_limit
    }

    /// Bytes rebuilding the members with `id`'s prefix costs, roughly (0 if
    /// none).
    pub fn member_len(&self, id: &ObjectId) -> usize {
        let prefix = &id.as_bytes()[..8];
        self.members.iter().filter(|m| m.prefix == prefix).map(|m| m.size).sum()
    }

    /// Kind and body of member `i`, rebuilt along its chain.
    fn body(&self, i: usize) -> Result<(ObjectKind, Bytes)> {
        let mut chain: Vec<usize> = Vec::new();
        let mut work = 0usize;
        let mut cur = i;
        let (kind, mut body) = loop {
            if let Some(hit) = self.memo.lock().bodies.get(&(cur as u32)) {
                break hit.clone();
            }
            let m = self.members[cur];
            if m.base == NO_BASE {
                let o = EncodedObject::from_bytes(self.raw.slice(m.start..m.end))
                    .map_err(|e| corrupt(format!("delta frame member {cur}: {e}")))?;
                if o.body().len() != m.size {
                    return Err(corrupt(format!("delta frame member {cur}: size mismatch")));
                }
                let header = o.as_bytes().len() - m.size;
                break (o.kind(), self.raw.slice(m.start + header..m.end));
            }
            let base = m.base as usize;
            if base >= cur {
                return Err(corrupt(format!("delta frame member {cur} based on {base}")));
            }
            chain.push(cur);
            work = work.saturating_add(m.size);
            if chain.len() > MAX_CHAIN || work > MAX_CHAIN_WORK {
                return Err(corrupt(format!("delta chain of member {i} too long")));
            }
            cur = base;
        };
        for &j in chain.iter().rev() {
            let m = self.members[j];
            let instructions = &self.raw[m.start..m.end];
            if delta::lengths(instructions) != Some((body.len(), m.size)) {
                return Err(corrupt(format!("delta frame member {j}: delta does not fit")));
            }
            let mut out = Vec::new();
            delta::apply(&body, instructions, &mut out)
                .ok_or_else(|| corrupt(format!("delta frame member {j}: bad delta")))?;
            body = Bytes::from(out);
            if m.is_base && body.len() <= self.memo_limit {
                let mut memo = self.memo.lock();
                if memo.bytes + body.len() > self.memo_limit {
                    memo.bodies.clear();
                    memo.bytes = 0;
                }
                memo.bytes += body.len();
                memo.bodies.insert(j as u32, (kind, body.clone()));
            }
        }
        Ok((kind, body))
    }

    /// The member `id`, rebuilt and verified; `None` when no member has
    /// this id.
    pub fn get(&self, id: &ObjectId) -> Option<Result<EncodedObject>> {
        let prefix = &id.as_bytes()[..8];
        let mut last_err = None;
        for (i, m) in self.members.iter().enumerate() {
            if m.prefix != prefix {
                continue;
            }
            let object = self.body(i).and_then(|(kind, body)| {
                let o = EncodedObject::new(kind, &body);
                o.verify(id).map_err(|e| corrupt(format!("object {id}: {e}")))?;
                Ok(o)
            });
            match object {
                Ok(o) => return Some(Ok(o)),
                // Another member may share the 8-byte prefix.
                Err(e) => last_err = Some(e),
            }
        }
        last_err.map(Err)
    }

    /// Check that the members are exactly `ids`, in this order, each rebuilt
    /// and hashed (writers check what they wrote with it; one member is held
    /// at a time).
    pub fn check(&self, ids: &[ObjectId]) -> Result<()> {
        if ids.len() != self.members.len() {
            return Err(corrupt(format!(
                "delta frame of {} members, expected {}",
                self.members.len(),
                ids.len()
            )));
        }
        for (i, id) in ids.iter().enumerate() {
            let (kind, body) = self.body(i)?;
            EncodedObject::new(kind, &body)
                .verify(id)
                .map_err(|e| corrupt(format!("delta frame member {i}: {e}")))?;
        }
        Ok(())
    }
}

/// Decode a delta frame record (members are rebuilt lazily).
pub(crate) fn decode_delta_frame(record: &[u8]) -> Result<DeltaFrame> {
    if !is_delta_frame(record) {
        return Err(corrupt("not a delta frame"));
    }
    if record.len() < HEADER {
        return Err(corrupt("truncated delta frame header"));
    }
    let count = u32_at(record, 4) as usize;
    let raw_len = u32_at(record, 8) as usize;
    if count == 0 || raw_len > MAX_FRAME_RAW {
        return Err(corrupt(format!("delta frame of {count} objects, {raw_len} bytes")));
    }
    let table_end = count
        .checked_mul(DELTA_MEMBER)
        .and_then(|t| t.checked_add(HEADER))
        .filter(|&end| end <= record.len())
        .ok_or_else(|| corrupt(format!("truncated delta frame table ({count} objects)")))?;
    let mut members: Vec<DeltaEntry> = Vec::with_capacity(count);
    let mut at = 0usize;
    for (i, m) in record[HEADER..table_end].chunks_exact(DELTA_MEMBER).enumerate() {
        let mut prefix = [0u8; 8];
        prefix.copy_from_slice(&m[..8]);
        let base = u32_at(m, 8);
        let len = u32_at(m, 12) as usize;
        let size = u32_at(m, 16) as usize;
        let end = at
            .checked_add(len)
            .filter(|&e| e <= raw_len)
            .ok_or_else(|| corrupt(format!("delta frame members exceed its {raw_len} bytes")))?;
        if size > MAX_MEMBER || (base != NO_BASE && base as usize >= i) {
            return Err(corrupt(format!("delta frame member {i}: base {base}, size {size}")));
        }
        if base != NO_BASE {
            members[base as usize].is_base = true;
        }
        members.push(DeltaEntry { prefix, base, start: at, end, size, is_base: false });
        at = end;
    }
    if at != raw_len {
        return Err(corrupt(format!("delta frame members hold {at} of its {raw_len} bytes")));
    }
    let raw = zstd::bulk::decompress(&record[table_end..], raw_len)
        .map_err(|e| corrupt(format!("delta frame data: {e}")))?;
    if raw.len() != raw_len {
        return Err(corrupt(format!(
            "delta frame data is {} bytes, header says {raw_len}",
            raw.len()
        )));
    }
    let bases: usize = members.iter().filter(|m| m.is_base).map(|m| m.size).sum();
    Ok(DeltaFrame {
        members,
        raw: Bytes::from(raw),
        memo: Mutex::new(Memo::default()),
        memo_limit: bases.min(MEMO_BYTES),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(names: &[&str]) -> EncodedObject {
        let mut body = Vec::new();
        for n in names {
            body.extend_from_slice(format!("100644 {n}\0").as_bytes());
            body.extend_from_slice(&[7u8; 32]);
        }
        EncodedObject::new(ObjectKind::Tree, &body)
    }

    #[test]
    fn versions_of_a_directory_or_file_share_a_key() {
        // The key is the smallest name hash: it survives every change but
        // the removal of that name or the addition of a smaller one.
        let names: Vec<String> = (0..40).map(|i| format!("file{i}.c")).collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let a = tree(&names);
        let key = similarity_key(&a);
        assert_eq!(key, names.iter().map(|n| fnv(n.as_bytes())).min().unwrap());
        let added = [names.as_slice(), &["zlib.c"]].concat();
        assert_eq!(similarity_key(&tree(&added)), key.min(fnv(b"zlib.c")));
        let min_at = names.iter().position(|n| fnv(n.as_bytes()) == key).unwrap();
        let mut removed = names.clone();
        removed.remove((min_at + 1) % names.len());
        assert_eq!(similarity_key(&tree(&removed)), key);
        let other = tree(&["main.rs", "lib.rs"]);
        assert_ne!(similarity_key(&a), similarity_key(&other));

        // Files: the smallest hash of a line long enough to be distinctive.
        let lines: Vec<String> =
            (0..30).map(|i| format!("\tstatic int value_{i} = compute({i});")).collect();
        let text = |lines: &[String]| {
            let mut t = String::from("#include \"cache.h\"\n{\n}\n");
            for l in lines {
                t.push_str(l);
                t.push('\n');
            }
            EncodedObject::new(ObjectKind::Blob, t.as_bytes())
        };
        let key = similarity_key(&text(&lines));
        let long = |l: &str| l.trim_ascii().len() >= KEY_MIN_LINE;
        let min = lines
            .iter()
            .chain([&"#include \"cache.h\"".to_string()])
            .filter(|l| long(l))
            .map(|l| fnv(l.trim_ascii().as_bytes()))
            .min()
            .unwrap();
        assert_eq!(key, min, "short lines ({{, }}) are ignored");
        let mut edited = lines.clone();
        let min_at = lines.iter().position(|l| fnv(l.trim_ascii().as_bytes()) == key);
        let at = min_at.map_or(0, |i| (i + 1) % lines.len());
        edited[at] = "\tstatic int changed_line_here = 0;".into();
        let edited_key = similarity_key(&text(&edited));
        assert_eq!(edited_key, key.min(fnv(edited[at].trim_ascii().as_bytes())));
        // A commit is keyed like a file.
        let c = EncodedObject::new(ObjectKind::Commit, text(&lines).body());
        assert_eq!(similarity_key(&c), key);
        // Short or binary content still gets a key.
        let _ = similarity_key(&EncodedObject::new(ObjectKind::Blob, b""));
        let _ = similarity_key(&EncodedObject::new(ObjectKind::Blob, &[0, 1, 2, 255]));
        // A malformed tree body is not an error.
        let _ = similarity_key(&EncodedObject::new(ObjectKind::Tree, b"100644 x"));
        let _ = similarity_key(&EncodedObject::new(ObjectKind::Tree, b"100644 x\0short"));
    }

    #[test]
    fn plan_groups_by_key_newest_first_and_cuts_at_the_cap() {
        use ObjectKind::*;
        let objs = [
            (Blob, 5, 100),   // 0
            (Tree, 9, 100),   // 1
            (Blob, 5, 100),   // 2
            (Tree, 9, 100),   // 3
            (Blob, 1, 600),   // 4: at least half the cap: alone
            (Commit, 0, 100), // 5
            (Blob, 5, 100),   // 6
        ];
        let plan = plan(&objs, 1000);
        assert_eq!(plan, vec![vec![3, 1], vec![4], vec![6, 2, 0], vec![5]]);
        let small = plan_cap(&objs, 250);
        // Cut every two 100-byte objects; 600 >= 125 alone.
        assert!(small.iter().all(|r| r.len() <= 2));
        let mut all: Vec<usize> = small.concat();
        all.sort_unstable();
        assert_eq!(all, (0..objs.len()).collect::<Vec<_>>());
        assert!(super::plan(&[], 1000).is_empty());
    }

    fn plan_cap(objs: &[(ObjectKind, u64, usize)], cap: usize) -> Vec<Vec<usize>> {
        super::plan(objs, cap)
    }

    fn sample() -> Vec<EncodedObject> {
        (0..20)
            .map(|i| {
                let text =
                    format!("line one of the file\nversion {i}\nline three is long enough\n");
                EncodedObject::new(ObjectKind::Blob, text.repeat(50).as_bytes())
            })
            .collect()
    }

    #[test]
    fn frames_round_trip_and_compress_similar_objects() {
        let objs = sample();
        let ids: Vec<ObjectId> = objs.iter().map(EncodedObject::id).collect();
        let refs: Vec<&EncodedObject> = objs.iter().collect();
        let rec = encode(&refs, &ids).unwrap();
        assert!(is_frame(&rec));
        let raw: usize = objs.iter().map(|o| o.as_bytes().len()).sum();
        assert_eq!(frame_raw_len(&rec), Some(raw));
        let alone: usize = objs
            .iter()
            .map(|o| zstd::bulk::compress(o.as_bytes(), ZSTD_LEVEL).unwrap().len())
            .sum();
        assert!(rec.len() * 3 < alone, "frame {} vs alone {alone}", rec.len());
        let frame = decode_frame(&rec).unwrap();
        for (o, id) in objs.iter().zip(&ids) {
            assert_eq!(frame.get(id).unwrap().unwrap(), *o);
        }
        let absent = EncodedObject::new(ObjectKind::Blob, b"absent").id();
        assert!(frame.get(&absent).is_none());

        // One member: a plain single-object record, readable by older versions.
        let single = encode(&refs[..1], &ids[..1]).unwrap();
        assert!(!is_frame(&single));
        assert_eq!(decode_single(ids[0], &single).unwrap(), objs[0]);
        assert_eq!(frame_raw_len(&single), None);
    }

    #[test]
    fn members_sharing_an_id_prefix_are_told_apart_by_hash() {
        let objs = sample();
        let ids: Vec<ObjectId> = objs.iter().map(EncodedObject::id).collect();
        let refs: Vec<&EncodedObject> = objs.iter().collect();
        let mut rec = encode(&refs[..3], &ids[..3]).unwrap();
        // Give all three members the prefix of the last one.
        for m in 0..3 {
            let at = HEADER + m * MEMBER;
            rec[at..at + 8].copy_from_slice(&ids[2].as_bytes()[..8]);
        }
        let frame = decode_frame(&rec).unwrap();
        assert_eq!(frame.get(&ids[2]).unwrap().unwrap(), objs[2]);
        // The others are no longer findable, and a prefix match whose hash
        // fails is an error rather than a wrong object.
        assert!(frame.get(&ids[0]).is_none());
        let mut fake = *ids[2].as_bytes();
        fake[31] ^= 1;
        let fake = ObjectId::from_bytes(&fake).unwrap();
        assert!(matches!(frame.get(&fake), Some(Err(StoreError::Backend(_)))));
    }

    #[test]
    fn corrupt_records_are_errors() {
        let objs = sample();
        let ids: Vec<ObjectId> = objs.iter().map(EncodedObject::id).collect();
        let refs: Vec<&EncodedObject> = objs.iter().collect();
        let rec = encode(&refs[..4], &ids[..4]).unwrap();
        let bad = |r: &[u8]| matches!(decode_frame(r), Err(StoreError::Backend(_)));
        // Every truncation.
        for n in 0..rec.len() {
            assert!(bad(&rec[..n]), "truncated to {n}");
        }
        // Absurd sizes, counts, member lengths.
        let mut r = rec.clone();
        r[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(bad(&r));
        let mut r = rec.clone();
        r[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(bad(&r));
        let mut r = rec.clone();
        r[4..8].copy_from_slice(&0u32.to_le_bytes());
        assert!(bad(&r));
        let mut r = rec.clone();
        r[HEADER + 8..HEADER + 12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(bad(&r));
        let mut r = rec.clone();
        let l = u32_at(&r, HEADER + 8);
        r[HEADER + 8..HEADER + 12].copy_from_slice(&(l + 1).to_le_bytes());
        assert!(bad(&r));
        // Flipped data bytes: an error from zstd or from the hash check.
        let table_end = HEADER + 4 * MEMBER;
        for at in (table_end..rec.len()).step_by(7) {
            let mut r = rec.clone();
            r[at] ^= 0x55;
            match decode_frame(&r) {
                Err(StoreError::Backend(_)) => {}
                Err(e) => panic!("unexpected error kind {e}"),
                Ok(f) => {
                    for id in &ids[..4] {
                        if let Some(Ok(o)) = f.get(id) {
                            assert_eq!(o.id(), *id);
                        }
                    }
                }
            }
        }
        // Single-object records: unknown kinds and garbage.
        assert!(decode_single(ids[0], b"garbage").is_err());
        assert!(decode_single(ids[0], &[0x28, 0xb5, 0x2f, 0xfd, 1, 2, 3]).is_err());
        let single = encode(&refs[..1], &ids[..1]).unwrap();
        assert!(decode_single(ids[1], &single).is_err(), "wrong id");
    }

    #[test]
    fn oversized_frames_are_refused_by_the_writer() {
        let big = EncodedObject::new(ObjectKind::Blob, &vec![1u8; MAX_FRAME_RAW]);
        let small = EncodedObject::new(ObjectKind::Blob, b"x");
        let r = encode(&[&big, &small], &[big.id(), small.id()]);
        assert!(matches!(r, Err(StoreError::InvalidArgument(_))));
    }

    /// A delta frame of `objs`: member 0 whole, every other member a delta
    /// against the one before it (one chain), or against member 0 (`fan`).
    fn delta_record(objs: &[EncodedObject], fan: bool) -> Vec<u8> {
        let mut parts: Vec<(Option<u32>, Vec<u8>)> = Vec::new();
        for (i, o) in objs.iter().enumerate() {
            if i == 0 || o.kind() != objs[0].kind() {
                parts.push((None, o.as_bytes().to_vec()));
                continue;
            }
            let base = if fan { 0 } else { i - 1 };
            let b = objs[base].body();
            let index = delta::Index::reuse(b, Vec::new()).unwrap();
            let d = delta::encode_indexed(&index, b, o.body(), o.body().len() + 64)
                .unwrap_or_else(|| panic!("no delta for {i}"));
            parts.push((Some(base as u32), d));
        }
        let members: Vec<DeltaMember<'_>> = objs
            .iter()
            .zip(&parts)
            .map(|(o, (base, bytes))| DeltaMember {
                id: o.id(),
                base: *base,
                bytes,
                size: o.body().len(),
            })
            .collect();
        encode_delta(&members, 3).unwrap()
    }

    #[test]
    fn delta_frames_round_trip_chains_and_fans() {
        let objs = sample();
        let ids: Vec<ObjectId> = objs.iter().map(EncodedObject::id).collect();
        for fan in [false, true] {
            let rec = delta_record(&objs, fan);
            assert!(is_delta_frame(&rec) && !is_frame(&rec));
            let f = decode_delta_frame(&rec).unwrap();
            f.check(&ids).unwrap();
            // Any order, repeatedly (the memo of rebuilt bases).
            for i in (0..objs.len()).rev().chain(0..objs.len()) {
                assert_eq!(f.get(&ids[i]).unwrap().unwrap(), objs[i]);
            }
            let absent = EncodedObject::new(ObjectKind::Blob, b"absent").id();
            assert!(f.get(&absent).is_none());
            assert!(f.check(&ids[1..]).is_err(), "wrong member list");
            let mut swapped = ids.clone();
            swapped.swap(1, 2);
            assert!(f.check(&swapped).is_err(), "wrong member order");
        }
        // Deltas make the chain far smaller than the frame of whole objects.
        let refs: Vec<&EncodedObject> = objs.iter().collect();
        let plain = encode(&refs, &ids).unwrap();
        let raw_delta = frame_raw_len(&delta_record(&objs, false)).unwrap();
        assert!(raw_delta * 5 < frame_raw_len(&plain).unwrap());
        // Kinds: a chain keeps its root's kind; a tree among blobs stands alone.
        let mut mixed = objs[..4].to_vec();
        mixed.push(EncodedObject::new(ObjectKind::Tree, objs[4].body()));
        let f = decode_delta_frame(&delta_record(&mixed, false)).unwrap();
        f.check(&mixed.iter().map(EncodedObject::id).collect::<Vec<_>>()).unwrap();
    }

    #[test]
    fn delta_frame_writer_refuses_forward_bases() {
        let objs = sample();
        let m = |i: usize, base: Option<u32>| DeltaMember {
            id: objs[i].id(),
            base,
            bytes: objs[i].as_bytes(),
            size: objs[i].body().len(),
        };
        assert!(encode_delta(&[m(0, Some(0))], 3).is_err(), "self base");
        assert!(encode_delta(&[m(0, None), m(1, Some(2))], 3).is_err(), "forward base");
        assert!(encode_delta(&[], 3).is_err());
    }

    #[test]
    fn corrupt_delta_frames_are_errors() {
        let objs = sample();
        let ids: Vec<ObjectId> = objs.iter().map(EncodedObject::id).collect();
        let rec = delta_record(&objs[..6], false);
        let bad = |r: &[u8]| match decode_delta_frame(r) {
            Err(StoreError::Backend(_)) => true,
            Err(e) => panic!("unexpected error kind {e}"),
            Ok(f) => ids[..6].iter().all(|id| !matches!(f.get(id), Some(Ok(_)))),
        };
        for n in 0..rec.len() {
            assert!(bad(&rec[..n]), "truncated to {n}");
        }
        let set =
            |r: &mut Vec<u8>, at: usize, v: u32| r[at..at + 4].copy_from_slice(&v.to_le_bytes());
        let member = |i: usize| HEADER + i * DELTA_MEMBER;
        // Absurd sizes and counts.
        for (at, v) in [(4, u32::MAX), (4, 0), (8, u32::MAX)] {
            let mut r = rec.clone();
            set(&mut r, at, v);
            assert!(bad(&r), "header {at} = {v}");
        }
        // A base pointing forward, at itself, or out of range.
        for base in [3u32, 2, 100] {
            let mut r = rec.clone();
            set(&mut r, member(2) + 8, base);
            assert!(bad(&r), "base {base}");
        }
        // A member claiming a huge or wrong body size: its delta no longer
        // fits, and nothing allocates gigabytes.
        for size in [u32::MAX, (MAX_MEMBER + 1) as u32, 5] {
            let mut r = rec.clone();
            set(&mut r, member(3) + 16, size);
            let f = decode_delta_frame(&r);
            assert!(
                f.is_err() || matches!(f.unwrap().get(&ids[3]), Some(Err(StoreError::Backend(_))))
            );
        }
        // A whole member turned into a delta of something else, and a delta
        // turned whole: corrupt data, hash mismatches, never panics.
        let mut r = rec.clone();
        set(&mut r, member(0) + 8, 0);
        assert!(bad(&r));
        let mut r = rec.clone();
        set(&mut r, member(1) + 8, NO_BASE);
        let f = decode_delta_frame(&r).unwrap();
        assert!(matches!(f.get(&ids[1]), Some(Err(StoreError::Backend(_)))));
        assert!(matches!(f.get(&ids[3]), Some(Err(StoreError::Backend(_)))), "built on it");
        assert_eq!(f.get(&ids[0]).unwrap().unwrap(), objs[0], "the base is fine");
        // Flipped data bytes.
        let table_end = HEADER + 6 * DELTA_MEMBER;
        for at in (table_end..rec.len()).step_by(5) {
            let mut r = rec.clone();
            r[at] ^= 0x41;
            if let Ok(f) = decode_delta_frame(&r) {
                for id in &ids[..6] {
                    if let Some(Ok(o)) = f.get(id) {
                        assert_eq!(o.id(), *id);
                    }
                }
            }
        }
    }

    #[test]
    fn long_chains_are_refused_by_readers() {
        // MAX_CHAIN + 2 members, each a delta of the one before.
        let base = EncodedObject::new(ObjectKind::Blob, &[b'x'; 64]);
        let mut objs = vec![base];
        for i in 0..MAX_CHAIN + 1 {
            let mut body = objs[i].body().to_vec();
            body.push(b'y');
            objs.push(EncodedObject::new(ObjectKind::Blob, &body));
        }
        let rec = delta_record(&objs, false);
        let f = decode_delta_frame(&rec).unwrap();
        assert_eq!(f.get(&objs[MAX_CHAIN].id()).unwrap().unwrap(), objs[MAX_CHAIN]);
        let last = objs.last().unwrap().id();
        let fresh = decode_delta_frame(&rec).unwrap();
        assert!(matches!(fresh.get(&last), Some(Err(StoreError::Backend(_)))));
    }

    mod props {
        use proptest::prelude::*;

        use super::super::*;

        /// Objects drawn from a few "files", each a base text with edits, so
        /// that keys collide like versions do; plus random binary blobs.
        fn objects() -> impl Strategy<Value = Vec<EncodedObject>> {
            let one = (0u8..3, 0usize..4, any::<u16>(), prop::collection::vec(any::<u8>(), 0..300))
                .prop_map(|(kind, file, edit, noise)| {
                    let mut body =
                        format!("file {file} with a long enough first line\n").repeat(1 + file * 3);
                    body.push_str(&format!("edit {edit}\n"));
                    let mut body = body.into_bytes();
                    body.extend_from_slice(&noise);
                    let kind = match kind {
                        0 => ObjectKind::Blob,
                        1 => ObjectKind::Tree,
                        _ => ObjectKind::Commit,
                    };
                    EncodedObject::new(kind, &body)
                });
            prop::collection::vec(one, 0..40)
        }

        proptest! {
            /// Any batch, planned with any cap and encoded, reads back every
            /// object from its record, and every object is in exactly one
            /// record; frames never exceed the cap unless they hold one
            /// object.
            #[test]
            fn planned_records_round_trip(objs in objects(), cap in 1usize..20_000) {
                let mut seen = std::collections::HashSet::new();
                let objs: Vec<EncodedObject> =
                    objs.into_iter().filter(|o| seen.insert(o.id())).collect();
                let shape: Vec<(ObjectKind, u64, usize)> = objs
                    .iter()
                    .map(|o| (o.kind(), similarity_key(o), o.as_bytes().len()))
                    .collect();
                let records = plan(&shape, cap);
                let mut all: Vec<usize> = records.concat();
                all.sort_unstable();
                prop_assert_eq!(all, (0..objs.len()).collect::<Vec<_>>());
                for record in &records {
                    let raw: usize = record.iter().map(|&i| shape[i].2).sum();
                    prop_assert!(record.len() == 1 || raw <= cap);
                    // One group per frame: same kind, same key.
                    prop_assert!(record.iter().all(|&i| (shape[i].0, shape[i].1)
                        == (shape[record[0]].0, shape[record[0]].1)));
                    let members: Vec<&EncodedObject> = record.iter().map(|&i| &objs[i]).collect();
                    let ids: Vec<ObjectId> = members.iter().map(|o| o.id()).collect();
                    let rec = encode(&members, &ids).unwrap();
                    if record.len() == 1 {
                        prop_assert_eq!(&decode_single(ids[0], &rec).unwrap(), members[0]);
                    } else {
                        let f = decode_frame(&rec).unwrap();
                        for (o, id) in members.iter().zip(&ids) {
                            prop_assert_eq!(&f.get(id).unwrap().unwrap(), *o);
                        }
                    }
                }
            }

            /// Damaged records (bytes flipped, truncated, extended) decode to
            /// an error or to verified objects, and never panic.
            #[test]
            fn damaged_records_never_panic(
                objs in objects(),
                flips in prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 0..6),
                cut in any::<prop::sample::Index>(),
                tail in prop::collection::vec(any::<u8>(), 0..8),
            ) {
                prop_assume!(objs.len() >= 2);
                let refs: Vec<&EncodedObject> = objs.iter().collect();
                let ids: Vec<ObjectId> = objs.iter().map(EncodedObject::id).collect();
                let mut rec = encode(&refs, &ids).unwrap();
                for (at, x) in &flips {
                    let i = at.index(rec.len());
                    rec[i] ^= x;
                }
                rec.truncate(cut.index(rec.len() + 1));
                rec.extend_from_slice(&tail);
                if let Ok(f) = decode_frame(&rec) {
                    for id in &ids {
                        if let Some(Ok(o)) = f.get(id) {
                            prop_assert_eq!(o.id(), *id);
                        }
                    }
                }
                for id in &ids {
                    if let Ok(o) = decode_single(*id, &rec) {
                        prop_assert_eq!(o.id(), *id);
                    }
                }
            }

            /// Damaged delta frames (bytes flipped anywhere, header and member
            /// table included; truncated; extended) decode to an error or to
            /// verified objects, and never panic.
            #[test]
            fn damaged_delta_frames_never_panic(
                objs in objects(),
                flips in prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 0..6),
                cut in any::<prop::sample::Index>(),
                tail in prop::collection::vec(any::<u8>(), 0..8),
            ) {
                use crate::packed::cluster::{Builder, ClusterOptions};
                let opts = ClusterOptions { level: 1, ..ClusterOptions::default() };
                let mut b = Builder::new(opts);
                let mut records = Vec::new();
                for o in &objs {
                    records.extend(b.push(o.id(), o.clone()).unwrap());
                }
                records.extend(b.finish().unwrap());
                let ids: Vec<ObjectId> = objs.iter().map(EncodedObject::id).collect();
                for r in records.into_iter().filter(|r| is_delta_frame(&r.bytes)) {
                    let mut rec = r.bytes;
                    for (at, x) in &flips {
                        let i = at.index(rec.len());
                        rec[i] ^= x;
                    }
                    rec.truncate(cut.index(rec.len() + 1));
                    rec.extend_from_slice(&tail);
                    if let Ok(f) = decode_delta_frame(&rec) {
                        for id in &ids {
                            if let Some(Ok(o)) = f.get(id) {
                                prop_assert_eq!(o.id(), *id);
                            }
                        }
                        let _ = f.check(&ids);
                    }
                }
            }
        }
    }
}
