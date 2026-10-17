//! Compressed pack bytes kept in memory in aligned segments, so that reads
//! of nearby records share one ranged GET even when they arrive in
//! different calls (see `PackedStore::with_segment_cache_bytes`).
//!
//! A checkout or clone against a freshly started server reads tens of
//! thousands of small records scattered over the packs of an import, in an
//! order that has nothing to do with where they are stored (directory by
//! directory, while the import wrote them in whatever order it walked
//! history). With S3, the request count, not the bytes, is what costs:
//! measured on SeaweedFS, a ranged GET costs about as much as 0.5 MiB of
//! body. So a missing record is read as the aligned [`SEGMENT`]s it lies
//! in, missing segments of one pack at most [`GAP_SEGMENTS`] apart are read
//! with one GET of at most [`SPAN_SEGMENTS`], and everything read is kept
//! (least recently used evicted first, within a byte budget). A bulk walk
//! whose packs fit the budget reads each segment about once.
//!
//! Packs are immutable (S3 packs are written once under a random id), so a
//! cached segment never goes stale; which objects exist is always asked of
//! the index first, as for decoded frames. A segment being read is shared:
//! callers that need it wait for the one read instead of issuing their own.

use std::collections::HashMap;

use bytes::Bytes;

/// Bytes per segment (alignment and caching unit).
pub const SEGMENT: u64 = 64 * 1024;
/// Missing segments of one pack at most this many segments apart are read
/// together (the gap is read and kept too)...
pub(crate) const GAP_SEGMENTS: u64 = 16;
/// ...as long as one read stays within this many segments.
pub(crate) const SPAN_SEGMENTS: u64 = 128;
/// Default budget (`PackedStore::with_segment_cache_bytes`).
pub const DEFAULT_SEGMENT_CACHE_BYTES: usize = 512 * 1024 * 1024;

/// A segment: pack id and index (`offset / SEGMENT`).
pub(crate) type Key = ([u8; 16], u64);

/// One ranged read: segments `first..=last` of `pack`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Fetch {
    pub pack: [u8; 16],
    pub first: u64,
    pub last: u64,
}

impl Fetch {
    pub fn offset(&self) -> u64 {
        self.first * SEGMENT
    }
    pub fn len(&self) -> u64 {
        (self.last - self.first + 1) * SEGMENT
    }
}

/// The segments holding bytes `off..off + len` (`len` > 0).
pub(crate) fn segments_of(off: u64, len: u64) -> std::ops::RangeInclusive<u64> {
    off / SEGMENT..=(off + len.max(1) - 1) / SEGMENT
}

/// Group missing segments into few reads: sorted, each read starts and ends
/// at a missing segment (so it never starts past the end of a pack), and
/// spans at most [`SPAN_SEGMENTS`].
pub(crate) fn plan(mut missing: Vec<Key>) -> Vec<Fetch> {
    missing.sort_unstable();
    missing.dedup();
    let mut out: Vec<Fetch> = Vec::new();
    for (pack, seg) in missing {
        match out.last_mut() {
            Some(f)
                if f.pack == pack
                    && seg <= f.last + 1 + GAP_SEGMENTS
                    && seg - f.first < SPAN_SEGMENTS =>
            {
                f.last = seg;
            }
            _ => out.push(Fetch { pack, first: seg, last: seg }),
        }
    }
    out
}

/// Least recently used segments within a byte budget.
pub(crate) struct SegmentCache {
    map: lru::LruCache<Key, Bytes>,
    bytes: usize,
    limit: usize,
}

impl SegmentCache {
    pub fn new(limit: usize) -> Self {
        Self { map: lru::LruCache::unbounded(), bytes: 0, limit }
    }

    pub fn get(&mut self, key: &Key) -> Option<Bytes> {
        self.map.get(key).cloned()
    }

    pub fn insert(&mut self, key: Key, bytes: Bytes) {
        if bytes.len() > self.limit {
            return;
        }
        self.bytes += bytes.len();
        if let Some(old) = self.map.put(key, bytes) {
            self.bytes -= old.len();
        }
        while self.bytes > self.limit {
            match self.map.pop_lru() {
                Some((_, old)) => self.bytes -= old.len(),
                None => break,
            }
        }
    }

    pub fn remove(&mut self, key: &Key) {
        if let Some(old) = self.map.pop(key) {
            self.bytes -= old.len();
        }
    }

    pub fn usage(&self) -> (usize, usize) {
        (self.map.len(), self.bytes)
    }
}

/// Split the body of `fetch` (shorter than `fetch.len()` only at the end of
/// the pack) into owned segments, so evicting one frees its bytes.
pub(crate) fn split(fetch: &Fetch, body: &[u8]) -> Vec<(Key, Bytes)> {
    body.chunks(SEGMENT as usize)
        .enumerate()
        .map(|(i, c)| ((fetch.pack, fetch.first + i as u64), Bytes::copy_from_slice(c)))
        .collect()
}

/// The bytes `off..off + len` from `segments` (which must hold every
/// segment of the range); `None` if they end early (a record past the end
/// of its pack: corruption).
pub(crate) fn assemble(
    segments: &HashMap<Key, Bytes>,
    pack: [u8; 16],
    off: u64,
    len: u64,
) -> Option<Bytes> {
    let range = segments_of(off, len);
    let (first, last) = (*range.start(), *range.end());
    let start = (off - first * SEGMENT) as usize;
    if first == last {
        let seg = segments.get(&(pack, first))?;
        return seg
            .get(start..start + len as usize)
            .map(|_| seg.slice(start..start + len as usize));
    }
    let mut out = Vec::with_capacity(len as usize);
    for s in first..=last {
        let seg = segments.get(&(pack, s))?;
        let from = if s == first { start } else { 0 };
        let want = (len as usize - out.len()).min(seg.len().saturating_sub(from));
        if want == 0 || (s < last && from + want < SEGMENT as usize) {
            return None;
        }
        out.extend_from_slice(&seg[from..from + want]);
    }
    (out.len() as u64 == len).then(|| Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_segments_are_read_in_few_bounded_ranges() {
        let k = |p: u8, s: u64| ([p; 16], s);
        let near = GAP_SEGMENTS + 1;
        let missing =
            vec![k(1, 5), k(2, 0), k(1, 0), k(1, 0), k(1, 5 + near), k(1, 5 + 2 * near + 1)];
        let got = plan(missing);
        let shape: Vec<_> = got.iter().map(|f| (f.pack[0], f.first, f.last)).collect();
        assert_eq!(shape, [(1, 0, 5 + near), (1, 5 + 2 * near + 1, 5 + 2 * near + 1), (2, 0, 0)]);
        // A dense run longer than the span is cut.
        let dense: Vec<Key> = (0..SPAN_SEGMENTS + 3).map(|s| k(3, s)).collect();
        let got = plan(dense);
        assert_eq!(got.len(), 2);
        assert_eq!((got[0].first, got[0].last), (0, SPAN_SEGMENTS - 1));
        assert!(got.iter().all(|f| f.len() <= SPAN_SEGMENTS * SEGMENT));
        assert!(plan(Vec::new()).is_empty());
    }

    #[test]
    fn records_are_assembled_across_segments_and_short_ones_are_refused() {
        // A pack of 2.5 segments, byte i = i % 251.
        let len = SEGMENT * 2 + SEGMENT / 2;
        let body: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let f = Fetch { pack: [7; 16], first: 0, last: 2 };
        let segs: HashMap<Key, Bytes> = split(&f, &body).into_iter().collect();
        assert_eq!(segs.len(), 3);
        assert_eq!(segs[&([7; 16], 2)].len() as u64, SEGMENT / 2);
        for (off, n) in [(0, 10), (SEGMENT - 3, 7), (5, 2 * SEGMENT + 10), (len - 4, 4), (0, len)] {
            let got = assemble(&segs, [7; 16], off, n).unwrap();
            assert_eq!(&got[..], &body[off as usize..(off + n) as usize], "{off} {n}");
        }
        // Past the end of the pack, or a segment missing.
        assert!(assemble(&segs, [7; 16], len - 4, 5).is_none());
        assert!(assemble(&segs, [7; 16], len, 1).is_none());
        assert!(assemble(&segs, [8; 16], 0, 1).is_none());
        let mut gap = segs.clone();
        gap.remove(&([7; 16], 1));
        assert!(assemble(&gap, [7; 16], 5, 2 * SEGMENT).is_none());
        // A short segment in the middle (never written that way) is refused.
        let mut short = segs.clone();
        short.insert(([7; 16], 1), Bytes::from_static(b"xy"));
        assert!(assemble(&short, [7; 16], 5, 2 * SEGMENT).is_none());
    }

    #[test]
    fn the_segment_cache_stays_within_its_budget() {
        let seg = Bytes::from(vec![0u8; 1000]);
        let mut c = SegmentCache::new(3000);
        for s in 0..10 {
            c.insert(([1; 16], s), seg.clone());
            assert!(c.usage().1 <= 3000);
        }
        assert_eq!(c.usage(), (3, 3000));
        assert!(c.get(&([1; 16], 9)).is_some());
        assert!(c.get(&([1; 16], 0)).is_none());
        c.insert(([1; 16], 9), seg.clone());
        assert_eq!(c.usage(), (3, 3000));
        c.remove(&([1; 16], 9));
        assert_eq!(c.usage(), (2, 2000));
        let mut tiny = SegmentCache::new(999);
        tiny.insert(([1; 16], 0), seg);
        assert_eq!(tiny.usage(), (0, 0));
    }
}
