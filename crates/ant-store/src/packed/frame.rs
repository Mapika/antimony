//! Pack records: the unit an `object_index` row points at.
//!
//! A pack is a concatenation of records. Two kinds exist, told apart by
//! their first bytes:
//!
//! - **Single object** (every pack written before 0.1.5, and objects that
//!   share a record with nothing): one zstd frame holding the encoded
//!   object. Starts with the zstd magic `28 b5 2f fd`.
//! - **Frame** (0.1.5): several objects compressed as one zstd stream. The
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
//! Corrupt records are errors, never panics.

use ant_core::{EncodedObject, ObjectId, ObjectKind};
use bytes::Bytes;

use crate::{Result, StoreError, ZSTD_LEVEL};

/// First bytes of a frame record.
pub(crate) const FRAME_MAGIC: [u8; 4] = *b"AFR1";
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

/// The uncompressed size of a frame record, from its header (`None` for a
/// single-object record or a truncated header).
pub(crate) fn frame_raw_len(record: &[u8]) -> Option<usize> {
    if !is_frame(record) || record.len() < HEADER {
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
        }
    }
}
