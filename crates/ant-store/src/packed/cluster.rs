//! Building delta frames (`frame`'s `ADF1` records) from a stream of
//! objects, git-style: every object is delta-compressed against the best of
//! the last few objects before it (a sliding window), with chains bounded in
//! depth, and the record holding them is compressed as a whole. Callers feed
//! objects in the order that puts versions of one file or directory next to
//! each other, newest first, so the version readers most likely want is
//! stored whole (or close to it) and older ones as deltas.
//!
//! A record is cut once its stream reaches [`ClusterOptions::cluster_raw`]
//! bytes; the window never reaches into another record, so every record
//! decodes on its own. Objects larger than a record are written alone
//! (single-object records); commits are stored whole (they do not delta
//! well, but compress together).

use std::collections::VecDeque;

use ant_core::{EncodedObject, ObjectId, ObjectKind, delta};
use bytes::Bytes;

use super::frame::{self, DeltaMember};
use crate::{Result, StoreError};

/// Settings of a [`Builder`].
#[derive(Debug, Clone, Copy)]
pub struct ClusterOptions {
    /// Objects before the current one tried as delta bases (git's
    /// `pack.window`).
    pub window: usize,
    /// Longest delta chain (git's `pack.depth`); 0 stores everything whole.
    pub depth: u32,
    /// Bytes of a record's stream (whole objects plus delta instructions)
    /// before it is cut: what a reader decompresses for one member at most.
    pub cluster_raw: usize,
    /// zstd level of the records.
    pub level: i32,
}

impl Default for ClusterOptions {
    fn default() -> Self {
        Self { window: 10, depth: 16, cluster_raw: 2 * 1024 * 1024, level: 19 }
    }
}

/// Bytes of window candidates (and their indices) kept at most.
const WINDOW_BYTES: usize = 24 * 1024 * 1024;
/// Index tables kept for reuse, and the largest kept (in slots).
const SPARE_TABLES: usize = 16;
const SPARE_SLOTS: usize = 1 << 18;

/// A finished record: its bytes and, per member, the id and the bytes it
/// takes in the stream (for the members' shares of the record).
pub(crate) struct Record {
    pub bytes: Vec<u8>,
    pub members: Vec<(ObjectId, usize)>,
    /// Members stored as deltas, and the deepest chain.
    pub deltas: usize,
    pub max_depth: u32,
}

/// A member of the record being built: only what it stores (whole objects
/// share the caller's buffer), so a record of many versions of a large file
/// holds their deltas, not every version.
struct Member {
    id: ObjectId,
    /// Base member, if stored as a delta.
    base: Option<u32>,
    /// The encoded object, or the delta instructions.
    stored: Bytes,
    /// Body length of the object.
    size: usize,
}

struct Candidate {
    member: u32,
    object: EncodedObject,
    /// Built on first use; `None` inside when the body cannot be indexed.
    index: Option<Option<delta::Index>>,
    bytes: usize,
    depth: u32,
}

/// Accumulates objects into delta frames; see the module docs.
pub(crate) struct Builder {
    opts: ClusterOptions,
    members: Vec<Member>,
    depths: Vec<u32>,
    raw: usize,
    window: VecDeque<Candidate>,
    window_bytes: usize,
    spare: Vec<Vec<u32>>,
}

/// Records whole objects are compressed into alone (larger than a record).
fn single(object: &EncodedObject, level: i32) -> Result<Record> {
    let bytes = zstd::bulk::compress(object.as_bytes(), level).map_err(StoreError::backend)?;
    Ok(Record {
        bytes,
        members: vec![(object.id(), object.as_bytes().len())],
        deltas: 0,
        max_depth: 0,
    })
}

impl Builder {
    pub fn new(opts: ClusterOptions) -> Self {
        Self {
            opts,
            members: Vec::new(),
            depths: Vec::new(),
            raw: 0,
            window: VecDeque::new(),
            window_bytes: 0,
            spare: Vec::new(),
        }
    }

    /// Add `object` (whose id is `id`); returns the records finished on the
    /// way (the current one when `object` did not fit, or `object` alone).
    pub fn push(&mut self, id: ObjectId, object: EncodedObject) -> Result<Vec<Record>> {
        let mut out = Vec::new();
        let len = object.as_bytes().len();
        if len > self.opts.cluster_raw || len > frame::MAX_FRAME_RAW / 2 {
            out.extend(self.finish()?);
            out.push(single(&object, self.opts.level)?);
            return Ok(out);
        }
        let mut best = self.best_delta(&object);
        let stored = best.as_ref().map_or(len, |(_, d, _)| d.len());
        if !self.members.is_empty() && self.raw + stored > self.opts.cluster_raw {
            out.extend(self.finish()?);
            best = None;
        }
        let member = self.members.len() as u32;
        let depth = best.as_ref().map_or(0, |(_, _, depth)| *depth);
        let size = object.body().len();
        let m = match best {
            Some((base, d, _)) => Member { id, base: Some(base), stored: Bytes::from(d), size },
            None => Member { id, base: None, stored: object.as_bytes().clone(), size },
        };
        self.raw += m.stored.len();
        self.members.push(m);
        self.depths.push(depth);
        if self.eligible(&object) {
            self.remember(member, object, depth);
        }
        Ok(out)
    }

    fn eligible(&self, object: &EncodedObject) -> bool {
        self.opts.depth > 0 && self.opts.window > 0 && object.kind() != ObjectKind::Commit
    }

    fn remember(&mut self, member: u32, object: EncodedObject, depth: u32) {
        let bytes = object.body().len();
        self.window_bytes += bytes;
        self.window.push_back(Candidate { member, object, index: None, bytes, depth });
        while self.window.len() > self.opts.window || self.window_bytes > WINDOW_BYTES {
            let Some(evicted) = self.window.pop_front() else { break };
            self.window_bytes -= evicted.bytes;
            self.recycle(evicted);
        }
    }

    fn recycle(&mut self, candidate: Candidate) {
        if let Some(Some(index)) = candidate.index {
            let table = index.into_table();
            if self.spare.len() < SPARE_TABLES && table.capacity() <= SPARE_SLOTS {
                self.spare.push(table);
            }
        }
    }

    /// The smallest delta against the window, chosen git-style: a deeper
    /// base must beat the current best proportionally, so chains fan out
    /// instead of running to the depth limit.
    fn best_delta(&mut self, object: &EncodedObject) -> Option<(u32, Vec<u8>, u32)> {
        if !self.eligible(object) {
            return None;
        }
        let max_depth = self.opts.depth;
        let body = object.body();
        let mut best: Option<(u32, Vec<u8>, u32)> = None;
        for candidate in self.window.iter_mut().rev() {
            if candidate.object.kind() != object.kind() || candidate.depth >= max_depth {
                continue;
            }
            let base_len = candidate.object.body().len();
            let (limit, ref_depth) = match &best {
                Some((_, d, depth)) => (d.len(), *depth),
                None => ((body.len() / 2).saturating_sub(32), 1),
            };
            let limit = limit * (max_depth - candidate.depth) as usize
                / (max_depth + 1 - ref_depth.min(max_depth)) as usize;
            if limit == 0
                || body.len().saturating_sub(base_len) >= limit
                || body.len() < base_len / 32
            {
                continue;
            }
            let index = candidate.index.get_or_insert_with(|| {
                let table = self.spare.pop().unwrap_or_default();
                let index = delta::Index::reuse(candidate.object.body(), table);
                let heap = index.as_ref().map_or(0, delta::Index::heap_bytes);
                candidate.bytes += heap;
                self.window_bytes += heap;
                index
            });
            let Some(index) = index else { continue };
            if let Some(d) = delta::encode_indexed(index, candidate.object.body(), body, limit) {
                best = Some((candidate.member, d, candidate.depth + 1));
            }
        }
        best
    }

    /// Finish the current record (if it holds anything).
    pub fn finish(&mut self) -> Result<Option<Record>> {
        while let Some(c) = self.window.pop_front() {
            self.recycle(c);
        }
        self.window_bytes = 0;
        self.raw = 0;
        let members = std::mem::take(&mut self.members);
        let depths = std::mem::take(&mut self.depths);
        if members.is_empty() {
            return Ok(None);
        }
        let parts: Vec<DeltaMember<'_>> = members
            .iter()
            .map(|m| DeltaMember { id: m.id, base: m.base, bytes: &m.stored, size: m.size })
            .collect();
        let bytes = frame::encode_delta(&parts, self.opts.level)?;
        // What was written must read back: repacking replaces the only
        // other copy once the index moves. Every member is rebuilt and
        // hashed.
        let ids: Vec<ObjectId> = members.iter().map(|m| m.id).collect();
        frame::decode_delta_frame(&bytes)?.check(&ids)?;
        Ok(Some(Record {
            bytes,
            members: members.iter().map(|m| (m.id, m.stored.len())).collect(),
            deltas: members.iter().filter(|m| m.base.is_some()).count(),
            max_depth: depths.iter().copied().max().unwrap_or(0),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Versions of `files` text files (one line edited per version), newest
    /// first per file, and a few directory versions.
    fn versions(files: usize, per_file: usize) -> Vec<EncodedObject> {
        let mut out = Vec::new();
        for f in 0..files {
            for v in (0..per_file).rev() {
                let mut text = String::new();
                for line in 0..300 {
                    let word = if line == v % 300 { format!("edit {v}") } else { "same".into() };
                    text.push_str(&format!("file {f} line {line:4}: source code, {word}\n"));
                }
                out.push(EncodedObject::new(ObjectKind::Blob, text.as_bytes()));
            }
        }
        out
    }

    fn build(objects: &[EncodedObject], opts: ClusterOptions) -> Vec<Record> {
        let mut b = Builder::new(opts);
        let mut records = Vec::new();
        for o in objects {
            records.extend(b.push(o.id(), o.clone()).unwrap());
        }
        records.extend(b.finish().unwrap());
        records
    }

    fn read_back(records: &[Record]) -> Vec<EncodedObject> {
        let mut out = Vec::new();
        for r in records {
            if frame::is_delta_frame(&r.bytes) {
                let f = frame::decode_delta_frame(&r.bytes).unwrap();
                for (id, _) in &r.members {
                    out.push(f.get(id).unwrap().unwrap());
                }
            } else {
                let (id, _) = r.members[0];
                out.push(frame::decode_single(id, &r.bytes).unwrap());
            }
        }
        out
    }

    #[test]
    fn versions_become_bounded_delta_chains_and_read_back() {
        let objects = versions(3, 40);
        let opts = ClusterOptions { depth: 5, level: 3, ..ClusterOptions::default() };
        let records = build(&objects, opts);
        assert_eq!(read_back(&records), objects);
        assert!(records.iter().all(|r| r.max_depth <= 5));
        let deltas: usize = records.iter().map(|r| r.deltas).sum();
        assert!(deltas > objects.len() * 3 / 4, "{deltas} deltas");
        let stored: usize = records.iter().map(|r| r.bytes.len()).sum();
        let alone: usize =
            objects.iter().map(|o| zstd::bulk::compress(o.as_bytes(), 3).unwrap().len()).sum();
        assert!(stored * 8 < alone, "stored {stored}, alone {alone}");
    }

    #[test]
    fn records_are_cut_at_the_stream_cap_and_large_objects_stand_alone() {
        let objects = versions(2, 30);
        let opts = ClusterOptions { cluster_raw: 16_000, level: 1, ..ClusterOptions::default() };
        let records = build(&objects, opts);
        assert!(records.len() > 2);
        for r in &records {
            let raw: usize = r.members.iter().map(|(_, n)| n).sum();
            assert!(raw <= 16_000 || r.members.len() == 1, "{raw}");
        }
        assert_eq!(read_back(&records), objects);
        // Larger than a record: alone, in the single-object format.
        let big = EncodedObject::new(ObjectKind::Blob, &vec![7u8; 20_000]);
        let mut mixed = objects[..3].to_vec();
        mixed.push(big.clone());
        mixed.extend_from_slice(&objects[3..6]);
        let records = build(&mixed, opts);
        assert!(records.iter().any(|r| !frame::is_delta_frame(&r.bytes)));
        assert_eq!(read_back(&records), mixed);
    }

    #[test]
    fn depth_zero_and_commits_store_everything_whole() {
        let objects = versions(1, 10);
        let records = build(&objects, ClusterOptions { depth: 0, ..ClusterOptions::default() });
        assert!(records.iter().all(|r| r.deltas == 0));
        assert_eq!(read_back(&records), objects);
        let commits: Vec<EncodedObject> =
            objects.iter().map(|o| EncodedObject::new(ObjectKind::Commit, o.body())).collect();
        let records = build(&commits, ClusterOptions::default());
        assert!(records.iter().all(|r| r.deltas == 0));
        assert_eq!(read_back(&records), commits);
    }

    #[test]
    fn deltas_never_cross_kinds() {
        let blobs = versions(1, 5);
        let mut mixed = Vec::new();
        for b in &blobs {
            mixed.push(b.clone());
            mixed.push(EncodedObject::new(ObjectKind::Tree, b.body()));
        }
        let records = build(&mixed, ClusterOptions::default());
        assert_eq!(read_back(&records), mixed);
    }

    mod props {
        use proptest::prelude::*;

        use super::*;

        fn objects() -> impl Strategy<Value = Vec<EncodedObject>> {
            let one = (0u8..3, 0usize..3, prop::collection::vec((0usize..400, any::<u8>()), 0..6))
                .prop_map(|(kind, file, edits)| {
                    let mut body =
                        format!("file {file}: a line long enough to match\n").repeat(10 + file * 7);
                    let mut body = std::mem::take(&mut body).into_bytes();
                    for (at, b) in edits {
                        let at = at % (body.len() + 1);
                        body.insert(at, b);
                    }
                    let kind = match kind {
                        0 => ObjectKind::Blob,
                        1 => ObjectKind::Tree,
                        _ => ObjectKind::Commit,
                    };
                    EncodedObject::new(kind, &body)
                });
            prop::collection::vec(one, 0..60)
        }

        proptest! {
            /// Any stream of objects, built with any settings, reads back
            /// byte for byte, with chains within the depth bound.
            #[test]
            fn built_records_round_trip(
                objs in objects(),
                window in 0usize..12,
                depth in 0u32..6,
                cap in 200usize..20_000,
            ) {
                let opts = ClusterOptions { window, depth, cluster_raw: cap, level: 1 };
                let records = build(&objs, opts);
                prop_assert!(records.iter().all(|r| r.max_depth <= depth));
                let ids: Vec<ObjectId> =
                    records.iter().flat_map(|r| r.members.iter().map(|m| m.0)).collect();
                prop_assert_eq!(ids, objs.iter().map(EncodedObject::id).collect::<Vec<_>>());
                prop_assert_eq!(read_back(&records), objs);
            }
        }
    }
}
