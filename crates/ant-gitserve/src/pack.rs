//! SHA-256 packs with OFS_DELTA entries, parallel delta search and bounded
//! backpressure. No pack-sized buffer is held in memory.
//!
//! Pipeline: the export walk (`walk.rs`, every object read once, in pack
//! order) → small segments of consecutive objects → one blocking delta+zlib
//! worker per segment, several at once → one blocking writer that lays the
//! segments out in order and resolves OFS_DELTA distances. The pack header
//! holds the object count, which only the finished walk knows, so the writer
//! spills the entries (compressed, about the size of the final pack) to a
//! temporary file and then streams header, entries and trailer hash in
//! sidebands. Until then it sends keepalive packets.
//!
//! A segment cut inside one path hands the next segment the last objects of
//! that path as delta candidates ("context"), so chains continue across the
//! cut. Their real depth is only known once the previous segment is encoded,
//! so that segment keeps them at most `CONTEXT_DEPTH` deep and the next one
//! assumes exactly that bound: the depth limit holds without coordination.
use std::{
    collections::VecDeque,
    io::{self, Seek, Write},
    time::{Duration, Instant},
};

use ant_core::{EncodedObject, ObjectKind};
use ant_store::Backend;
use anyhow::{Result, anyhow, bail, ensure};
use bytes::Bytes;
use flate2::{Compress, Compression, FlushCompress, Status};
use sha2::{Digest, Sha256};
use tokio::sync::{
    mpsc::{self, Sender},
    oneshot,
};

use crate::{delta, protocol::packet, select::Selection, walk};

pub type Chunk = Result<Bytes, io::Error>;
const SIDEBAND: usize = 65515;
/// Pack entries up to this size stay in memory; larger packs spill to a
/// file in the temporary directory while the walk runs.
const SPILL_MEMORY: usize = 4 << 20;
/// An empty data sideband is sent this often while the pack is being built
/// (git's upload-pack keepalive), so idle timeouts do not cut the clone.
const KEEPALIVE: Duration = Duration::from_secs(5);
/// A segment ends at the first path change after this many body bytes...
const SEGMENT_BYTES: usize = 1 << 20;
/// ...or here, even inside one path (handing over context, see above).
const SEGMENT_MAX_BYTES: usize = 16 << 20;
const MAX_WORKERS: usize = 8;
/// Larger objects are stored whole and never used as delta bases.
const MAX_BASE: usize = 8 << 20;
const MAX_DEPTH: u8 = 50;
const CONTEXT_DEPTH: u8 = MAX_DEPTH / 2;
/// Delta candidates: the previous objects at the same path (git's default
/// `pack.window`), bounded in bytes including their block indices.
const WINDOW: usize = 10;
const WINDOW_BYTES: usize = 16 << 20;
/// zlib level of every entry (git's default `core.compression`).
const LEVEL: u32 = 6;
/// Pack entry types (git's `object_type`).
const OBJ_COMMIT: u8 = 1;
const OBJ_TREE: u8 = 2;
const OBJ_BLOB: u8 = 3;
const OBJ_OFS_DELTA: u8 = 6;

struct PackWriter {
    tx: Sender<Chunk>,
    hash: Sha256,
    position: u64,
    pending: Vec<u8>,
}

impl Write for PackWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.hash.update(bytes);
        self.position += bytes.len() as u64;
        let mut rest = bytes;
        while !rest.is_empty() {
            let take = rest.len().min(SIDEBAND - self.pending.len());
            self.pending.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.pending.len() == SIDEBAND {
                self.flush()?;
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.pending.is_empty() {
            let mut payload = Vec::with_capacity(self.pending.len() + 1);
            payload.push(1);
            payload.extend_from_slice(&self.pending);
            self.tx
                .blocking_send(Ok(packet(&payload).into()))
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "client disconnected"))?;
            self.pending.clear();
        }
        Ok(())
    }
}

fn header(out: &mut Vec<u8>, kind: u8, mut size: usize) {
    let mut byte = (kind << 4) | (size as u8 & 15);
    size >>= 4;
    while size != 0 {
        out.push(byte | 128);
        byte = size as u8 & 127;
        size >>= 7;
    }
    out.push(byte);
}

/// OFS_DELTA uses a biased, big-endian base-128 distance, not a normal varint.
fn distance(out: &mut Vec<u8>, mut offset: u64) {
    let mut bytes = [0_u8; 10];
    let mut pos = bytes.len() - 1;
    bytes[pos] = (offset & 127) as u8;
    while offset > 127 {
        offset = (offset >> 7) - 1;
        pos -= 1;
        bytes[pos] = 128 | (offset & 127) as u8;
    }
    out.extend_from_slice(&bytes[pos..]);
}

/// Where a delta base was written: an entry of this or the previous segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Base {
    Here(usize),
    Previous(usize),
}

/// One pack entry of an encoded segment; its zlib stream ends at `end` in
/// `Segment::data`.
struct Entry {
    kind: u8,
    size: usize,
    base: Option<Base>,
    end: usize,
}

#[derive(Default)]
struct Segment {
    entries: Vec<Entry>,
    data: Vec<u8>,
}

/// Input of one delta worker.
struct Work {
    objects: Vec<(u64, EncodedObject)>,
    /// Candidates at the path of `objects[0]` from the end of the previous
    /// segment, oldest first, with their entry index there.
    context: Vec<(EncodedObject, usize)>,
    /// Objects from this index on become the next segment's context.
    tail: usize,
    deltas: bool,
}

struct Candidate {
    object: EncodedObject,
    /// Built on first use; `None` inside when the body cannot be indexed.
    index: Option<Option<delta::Index>>,
    bytes: usize,
    base: Base,
    depth: u8,
}

#[derive(Default)]
struct Window {
    path: Option<u64>,
    candidates: VecDeque<Candidate>,
    bytes: usize,
    /// Index tables of evicted candidates, for reuse (see `delta::Index::reuse`).
    spare: Vec<Vec<u32>>,
}

/// Spare index tables kept per window, and the largest kept (in slots).
const SPARE_TABLES: usize = WINDOW;
const SPARE_SLOTS: usize = 1 << 16;

impl Window {
    fn recycle(&mut self, candidate: Candidate) {
        if let Some(Some(index)) = candidate.index {
            let table = index.into_table();
            if self.spare.len() < SPARE_TABLES && table.capacity() <= SPARE_SLOTS {
                self.spare.push(table);
            }
        }
    }

    fn reset(&mut self, path: u64) {
        if self.path != Some(path) {
            self.path = Some(path);
            while let Some(candidate) = self.candidates.pop_front() {
                self.recycle(candidate);
            }
            self.bytes = 0;
        }
    }

    fn push(&mut self, object: EncodedObject, base: Base, depth: u8) {
        let bytes = object.body().len();
        self.bytes += bytes;
        self.candidates.push_back(Candidate { object, index: None, bytes, base, depth });
        while self.candidates.len() > WINDOW || self.bytes > WINDOW_BYTES {
            let Some(evicted) = self.candidates.pop_front() else { break };
            self.bytes -= evicted.bytes;
            self.recycle(evicted);
        }
    }

    /// The smallest delta against the window, chosen git-style: a deeper base
    /// must beat the current best proportionally, so chains fan out instead
    /// of running to the depth limit. Results are at most `max_depth` deep.
    fn best_delta(&mut self, object: &EncodedObject, max_depth: u8) -> Option<(Vec<u8>, Base, u8)> {
        let body = object.body();
        let mut best: Option<(Vec<u8>, Base, u8)> = None;
        for candidate in self.candidates.iter_mut().rev() {
            if candidate.object.kind() != object.kind() || candidate.depth >= max_depth {
                continue;
            }
            let base_len = candidate.object.body().len();
            let (limit, ref_depth) = match &best {
                Some((delta, _, depth)) => (delta.len(), *depth),
                None => ((body.len() / 2).saturating_sub(32), 1),
            };
            let limit = limit * usize::from(MAX_DEPTH - candidate.depth)
                / usize::from(MAX_DEPTH + 1 - ref_depth);
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
                self.bytes += heap;
                index
            });
            let Some(index) = index else { continue };
            if let Some(delta) = delta::encode_indexed(index, candidate.object.body(), body, limit)
            {
                best = Some((delta, candidate.base, candidate.depth + 1));
            }
        }
        best
    }
}

fn eligible(object: &EncodedObject, deltas: bool) -> bool {
    deltas && object.kind() != ObjectKind::Commit && object.body().len() <= MAX_BASE
}

/// Append `input` as one zlib stream. The compressor (a few hundred KiB of
/// state) is reused for every entry of a segment rather than allocated per
/// object, which also keeps the allocator from fragmenting.
fn deflate(compress: &mut Compress, input: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
    compress.reset();
    loop {
        out.reserve(input.len() / 4 + 64);
        let read = usize::try_from(compress.total_in()).map_err(io::Error::other)?;
        let status = compress
            .compress_vec(&input[read..], out, FlushCompress::Finish)
            .map_err(io::Error::other)?;
        if status == Status::StreamEnd {
            return Ok(());
        }
    }
}

fn encode_segment(work: Work) -> io::Result<Segment> {
    let Work { objects, context, tail, deltas } = work;
    let mut segment = Segment { entries: Vec::with_capacity(objects.len()), data: Vec::new() };
    let mut window = Window::default();
    let mut compress = Compress::new(Compression::new(LEVEL), true);
    if let Some((path, _)) = objects.first() {
        window.reset(*path);
        for (object, entry) in context {
            window.push(object, Base::Previous(entry), CONTEXT_DEPTH);
        }
    }
    for (n, (path, object)) in objects.into_iter().enumerate() {
        window.reset(path);
        let eligible = eligible(&object, deltas);
        let max_depth = if n >= tail { CONTEXT_DEPTH } else { MAX_DEPTH };
        let best = if eligible { window.best_delta(&object, max_depth) } else { None };
        let entry = segment.entries.len();
        let (kind, size, base, depth) = match &best {
            Some((delta, base, depth)) => {
                deflate(&mut compress, delta, &mut segment.data)?;
                (OBJ_OFS_DELTA, delta.len(), Some(*base), *depth)
            }
            None => {
                deflate(&mut compress, object.body(), &mut segment.data)?;
                let kind = match object.kind() {
                    ObjectKind::Commit => OBJ_COMMIT,
                    ObjectKind::Tree => OBJ_TREE,
                    ObjectKind::Blob => OBJ_BLOB,
                };
                (kind, object.body().len(), None, 0)
            }
        };
        segment.entries.push(Entry { kind, size, base, end: segment.data.len() });
        if eligible {
            window.push(object, Base::Here(entry), depth);
        }
    }
    Ok(segment)
}

/// What a finished pack contained, for logs.
#[derive(Debug, Default, Clone, Copy)]
pub struct PackStats {
    pub objects: u32,
    pub deltas: u32,
    pub bytes: u64,
}

type Pending = oneshot::Receiver<io::Result<Segment>>;

/// From the segmenter to the writer: segments in pack order, then the number
/// of objects sent (only after a complete walk).
enum Message {
    Segment(Pending),
    Done(u64),
}

/// Pack header: signature, version 2, object count.
const HEADER: usize = 12;

fn write_pack(tx: Sender<Chunk>, mut segments: mpsc::Receiver<Message>) -> Result<PackStats> {
    let mut spill = tempfile::SpooledTempFile::new(SPILL_MEMORY);
    let mut stats = PackStats::default();
    let mut position = HEADER as u64;
    let mut offsets = Vec::new();
    let mut previous = Vec::new();
    let mut out = Vec::new();
    let mut keepalive = Instant::now();
    let count = loop {
        let pending = match segments.blocking_recv() {
            Some(Message::Segment(pending)) => pending,
            Some(Message::Done(count)) => break count,
            None => bail!("incomplete pack"),
        };
        let segment = pending.blocking_recv().map_err(|_| anyhow!("delta worker failed"))??;
        std::mem::swap(&mut offsets, &mut previous);
        offsets.clear();
        out.clear();
        let mut start = 0;
        for entry in &segment.entries {
            let offset = position + out.len() as u64;
            offsets.push(offset);
            header(&mut out, entry.kind, entry.size);
            if let Some(base) = entry.base {
                let base = match base {
                    Base::Here(n) => offsets[n],
                    Base::Previous(n) => previous[n],
                };
                distance(&mut out, offset - base);
                stats.deltas += 1;
            }
            out.extend_from_slice(&segment.data[start..entry.end]);
            start = entry.end;
            stats.objects += 1;
        }
        spill.write_all(&out)?;
        position += out.len() as u64;
        if keepalive.elapsed() >= KEEPALIVE {
            tx.blocking_send(Ok(Bytes::from_static(b"0005\x01")))
                .map_err(|_| anyhow!("client disconnected"))?;
            keepalive = Instant::now();
        }
    };
    ensure!(u64::from(stats.objects) == count, "incomplete pack");
    let mut writer =
        PackWriter { tx, hash: Sha256::new(), position: 0, pending: Vec::with_capacity(SIDEBAND) };
    writer.write_all(b"PACK")?;
    writer.write_all(&2_u32.to_be_bytes())?;
    writer.write_all(&stats.objects.to_be_bytes())?;
    spill.rewind()?;
    io::copy(&mut spill, &mut writer)?;
    ensure!(writer.position == position, "pack spill truncated");
    let digest = writer.hash.clone().finalize();
    writer.write_all(&digest)?;
    writer.flush()?;
    stats.bytes = writer.position;
    writer.tx.blocking_send(Ok(Bytes::from_static(b"0000")))?;
    Ok(stats)
}

/// Cuts the object stream into segments and starts a worker for each.
struct Segmenter {
    segments: Sender<Message>,
    deltas: bool,
    objects: Vec<(u64, EncodedObject)>,
    bytes: usize,
    context: Vec<(EncodedObject, usize)>,
}

impl Segmenter {
    async fn push(&mut self, path: u64, object: EncodedObject) -> Result<()> {
        let path_changed = self.objects.last().is_some_and(|(last, _)| *last != path);
        if self.bytes >= SEGMENT_MAX_BYTES && !path_changed {
            self.dispatch(true).await?;
        } else if self.bytes >= SEGMENT_BYTES && path_changed {
            self.dispatch(false).await?;
        }
        self.bytes += object.body().len();
        self.objects.push((path, object));
        Ok(())
    }

    /// Hand the current segment to a worker; `carry` keeps its last objects
    /// as the next segment's context (a cut inside one path).
    async fn dispatch(&mut self, carry: bool) -> Result<()> {
        let objects = std::mem::take(&mut self.objects);
        let mut tail = objects.len();
        let mut next = Vec::new();
        if carry && self.deltas {
            let path = objects.last().map(|(path, _)| *path);
            for (n, (p, object)) in objects.iter().enumerate().rev() {
                if Some(*p) != path || next.len() == WINDOW {
                    break;
                }
                tail = n;
                if eligible(object, self.deltas) {
                    next.push((object.clone(), n));
                }
            }
            next.reverse();
        }
        let work = Work {
            objects,
            context: std::mem::replace(&mut self.context, next),
            tail,
            deltas: self.deltas,
        };
        self.bytes = 0;
        let (done, pending) = oneshot::channel();
        // Waiting for queue space here bounds the segments held in memory.
        self.segments
            .send(Message::Segment(pending))
            .await
            .map_err(|_| anyhow!("pack consumer disconnected"))?;
        tokio::task::spawn_blocking(move || {
            let _ = done.send(encode_segment(work));
        });
        Ok(())
    }
}

/// Send the pack of `selection` (deltas only if the client takes
/// `ofs-delta`) as sideband packets to `tx`.
pub async fn send(
    b: Backend,
    selection: Selection,
    deltas: bool,
    tx: Sender<Chunk>,
) -> Result<PackStats> {
    let workers = std::thread::available_parallelism().map_or(1, |n| n.get()).min(MAX_WORKERS);
    let (segment_tx, segment_rx) = mpsc::channel(workers);
    let writer = tokio::task::spawn_blocking(move || write_pack(tx, segment_rx));
    let (part_tx, mut part_rx) = mpsc::channel::<walk::Part>(2);
    let encoder = async move {
        let mut segmenter = Segmenter {
            segments: segment_tx,
            deltas,
            objects: Vec::new(),
            bytes: 0,
            context: Vec::new(),
        };
        while let Some(part) = part_rx.recv().await {
            let (mut objects, merge) = part.objects();
            while let Some(batch) = objects.recv().await {
                for (path, object) in batch {
                    segmenter.push(path, object).await?;
                }
            }
            if let Some(merge) = merge {
                merge.await??;
            }
        }
        Ok::<_, anyhow::Error>(segmenter)
    };
    let (walked, encoded) = tokio::join!(walk::walk(b, selection, part_tx), encoder);
    let finished = async {
        let count = walked?;
        let mut segmenter = encoded?;
        if !segmenter.objects.is_empty() {
            segmenter.dispatch(false).await?;
        }
        segmenter
            .segments
            .send(Message::Done(count))
            .await
            .map_err(|_| anyhow!("pack consumer disconnected"))
    }
    .await;
    // On failure the segment channel is closed without `Done`: the writer
    // stops without a trailer.
    let written = writer.await;
    finished?;
    written?
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_core::ObjectId;
    use tokio::io::AsyncWriteExt;

    mod support {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/support/mod.rs"));
    }

    async fn git(dir: &std::path::Path, args: &[&str]) -> String {
        let result = support::git_command(dir).args(args).output().await.unwrap();
        assert!(
            result.status.success(),
            "git {args:?}: {}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        String::from_utf8(result.stdout).unwrap()
    }

    async fn receive(
        b: Backend,
        selection: crate::select::Selection,
        deltas: bool,
        promisor: bool,
    ) -> (tempfile::TempDir, u64) {
        let dir = tempfile::tempdir().unwrap();
        git(dir.path(), &["init", "--bare", "--object-format=sha256"]).await;
        git(dir.path(), &["symbolic-ref", "HEAD", "refs/heads/main"]).await;
        if promisor {
            git(dir.path(), &["config", "remote.origin.promisor", "true"]).await;
            git(dir.path(), &["config", "remote.origin.url", "/nonexistent-promisor"]).await;
        }
        let mut command = support::git_command(dir.path());
        command.args(["index-pack", "--stdin", "--fix-thin"]);
        if promisor {
            command.arg("--promisor");
        }
        let mut child = command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let (tx, mut rx) = mpsc::channel(8);
        let sender = tokio::spawn(send(b, selection, deltas, tx));
        let mut size = 0;
        while let Some(chunk) = rx.recv().await {
            let bytes = chunk.unwrap();
            if bytes != b"0000"[..] {
                input.write_all(&bytes[5..]).await.unwrap();
                size += (bytes.len() - 5) as u64;
            }
        }
        drop(input);
        sender.await.unwrap().unwrap();
        let result = child.wait_with_output().await.unwrap();
        assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
        (dir, size)
    }

    // This exercises the same planner and streaming encoder as HTTP, with real
    // git over stdin, so it also runs in sandboxes that prohibit listen sockets.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn synthetic_history_pack_size_time_and_peak_rss() {
        use std::{sync::atomic::Ordering, time::Instant};
        let (b, expected_undeltified) = support::history(2000, 2000).await;
        let tip = b.refs.list_refs("").await.unwrap()[0].1;
        let mut sizes = Vec::new();
        for deltas in [false, true] {
            let before = support::rss_kib();
            let (peak, monitor) = support::watch_peak_rss(before);
            let start = Instant::now();
            let selection = crate::select::select(
                &b,
                &crate::select::Fetch {
                    wants: vec![tip],
                    no_blobs: true,
                    done: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let planned = start.elapsed();
            let (dir, size) = receive(b.clone(), selection, deltas, true).await;
            monitor.abort();
            eprintln!(
                "synthetic 2000 x 2000 deltas={deltas}: pack={size} planning={planned:?} planning+transfer={:?} RSS before={before} KiB peak={} KiB",
                start.elapsed(),
                peak.load(Ordering::Relaxed)
            );
            git(dir.path(), &["update-ref", "refs/heads/main", &tip.to_string()]).await;
            git(dir.path(), &["fsck", "--full"]).await;
            sizes.push(size);
        }
        assert_eq!(sizes[0], expected_undeltified);
        assert!(sizes[1] * 100 <= sizes[0] * 30);
    }

    #[tokio::test]
    async fn real_git_delta_chains_and_path_type_changes() {
        let b = Backend::in_memory();
        let tip = support::revisions(&b, None, 0..125).await;
        let selection = crate::select::select(
            &b,
            &crate::select::Fetch { wants: vec![tip], done: true, ..Default::default() },
        )
        .await
        .unwrap();
        let old: std::collections::HashSet<_> =
            crate::walk::ids(&b, selection.clone()).await.unwrap().into_iter().collect();
        let (dir, _) = receive(b.clone(), selection, true, false).await;
        git(dir.path(), &["update-ref", "refs/heads/main", &tip.to_string()]).await;
        git(dir.path(), &["fsck", "--full"]).await;
        let packed = support::packed_objects(&dir.path().join("objects/pack")).await;
        let deltas = |kind: &str| packed.iter().filter(|(k, d)| k == kind && d.is_some()).count();
        let (trees, blobs) = (deltas("tree"), deltas("blob"));
        let depth = packed.iter().filter_map(|(_, d)| *d).max().unwrap_or(0);
        assert!(trees > 50 && blobs > 50, "trees={trees} blobs={blobs}");
        // Bases fan out (see `best_delta`); the cap itself is pinned by
        // `appended_history_stops_at_the_depth_cap`.
        assert!((2..=usize::from(MAX_DEPTH)).contains(&depth), "depth {depth}");
        let next = support::revisions(&b, Some(tip), 125..135).await;
        let selection = crate::select::select(
            &b,
            &crate::select::Fetch {
                wants: vec![next],
                haves: vec![tip],
                done: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let ids = crate::walk::ids(&b, selection.clone()).await.unwrap();
        assert_eq!(ids.len(), 30);
        assert!(ids.iter().all(|id| !old.contains(id)));
        let (incremental, _) = receive(b, selection, true, false).await;
        for entry in std::fs::read_dir(dir.path().join("objects/pack")).unwrap() {
            let path = entry.unwrap().path();
            std::fs::copy(
                &path,
                incremental.path().join("objects/pack").join(path.file_name().unwrap()),
            )
            .unwrap();
        }
        git(incremental.path(), &["update-ref", "refs/heads/main", &next.to_string()]).await;
        git(incremental.path(), &["fsck", "--full"]).await;
    }

    /// Regression guard for real histories (git/git's blobless export once
    /// was 7.5x git's own pack): deep trees, a few hot directories, merged
    /// topic branches. The blobless pack must be no larger than what git's own
    /// delta search (`repack -adf`: window 10, depth 50) makes of the same
    /// objects, and far below the undeltified size.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn merged_history_pack_is_close_to_git_repack() {
        let b = Backend::in_memory();
        let tip = support::merge_history(&b, 1200).await;
        let fetch = crate::select::Fetch {
            wants: vec![tip],
            no_blobs: true,
            done: true,
            ..Default::default()
        };
        let selection = crate::select::select(&b, &fetch).await.unwrap();
        let (plain, undeltified) = receive(b.clone(), selection, false, true).await;
        drop(plain);
        let selection = crate::select::select(&b, &fetch).await.unwrap();
        let (dir, size) = receive(b, selection, true, true).await;
        git(dir.path(), &["update-ref", "refs/heads/main", &tip.to_string()]).await;
        git(dir.path(), &["fsck", "--full"]).await;
        git(dir.path(), &["repack", "-a", "-d", "-f", "-q"]).await;
        let repacked = support::pack_bytes(&dir.path().join("objects/pack"));
        eprintln!("merged history: undeltified={undeltified} pack={size} git repack={repacked}");
        // Measured 2026-10-14: pack 30% of undeltified, 0.5x git's repack. A
        // single previous-version base in traversal order (the old encoder)
        // gives 50% and fails here.
        assert!(size <= repacked, "pack {size} larger than git's {repacked}");
        assert!(size * 100 <= undeltified * 35, "pack {size} vs undeltified {undeltified}");
    }

    /// A long chain where only the previous version is a good base: depths
    /// must reach the cap exactly and never pass it, also across segment
    /// cuts inside one path (context handed to the next worker).
    #[test]
    fn appended_history_stops_at_the_depth_cap() {
        let mut body = Vec::new();
        let mut objects = Vec::new();
        let mut state = 7_u32;
        for version in 0..300 {
            // A large first version keeps each appended delta far below the
            // depth-scaled size limit, so only the cap stops the chain.
            for _ in 0..if version == 0 { 100_000 } else { 64 } {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                body.push(state as u8);
            }
            objects.push((9, EncodedObject::new(ObjectKind::Blob, &body)));
        }
        let mut depths: Vec<u8> = Vec::new();
        let mut previous: Vec<u8> = Vec::new();
        let mut context = Vec::new();
        for (n, chunk) in objects.chunks(100).enumerate() {
            let tail = if n < 2 { chunk.len() - WINDOW } else { chunk.len() };
            let work = Work {
                objects: chunk.to_vec(),
                context: std::mem::take(&mut context),
                tail,
                deltas: true,
            };
            let segment = encode_segment(work).unwrap();
            let mut here = Vec::new();
            for entry in &segment.entries {
                let depth = match entry.base {
                    None => 0,
                    Some(Base::Here(i)) => here[i] + 1,
                    Some(Base::Previous(i)) => previous[i] + 1,
                };
                assert!(depth <= MAX_DEPTH);
                here.push(depth);
            }
            assert!(here[tail..].iter().all(|d| *d <= CONTEXT_DEPTH));
            context = (tail..chunk.len()).map(|i| (chunk[i].1.clone(), i)).collect();
            depths.extend(&here);
            previous = here;
        }
        assert_eq!(depths.iter().max(), Some(&MAX_DEPTH));
        assert!(depths.iter().filter(|d| **d == 0).count() <= 300 / usize::from(MAX_DEPTH) + 1);
    }

    #[tokio::test]
    async fn failed_reads_and_disconnected_clients_stop_the_pipeline() {
        let b = Backend::in_memory();
        let (tx, mut rx) = mpsc::channel(8);
        assert!(
            send(
                b.clone(),
                Selection { roots: vec![ObjectId::empty_tree()], ..Default::default() },
                true,
                tx
            )
            .await
            .is_err()
        );
        while let Some(chunk) = rx.recv().await {
            assert_ne!(chunk.unwrap(), b"0000"[..], "failed pack must not finish successfully");
        }
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            send(b, Selection::default(), true, tx),
        )
        .await;
        assert!(result.unwrap().is_err());
    }

    /// Counts store reads per object id.
    struct Counting {
        inner: std::sync::Arc<dyn ant_store::ObjectStore>,
        reads: std::sync::Mutex<std::collections::HashMap<ObjectId, usize>>,
    }

    #[async_trait::async_trait]
    impl ant_store::ObjectStore for Counting {
        async fn get_many(
            &self,
            ids: &[ObjectId],
        ) -> ant_store::Result<Vec<Option<EncodedObject>>> {
            {
                let mut reads = self.reads.lock().unwrap();
                for id in ids {
                    *reads.entry(*id).or_default() += 1;
                }
            }
            self.inner.get_many(ids).await
        }

        async fn put_many(&self, objects: &[EncodedObject]) -> ant_store::Result<()> {
            self.inner.put_many(objects).await
        }

        async fn has_many(&self, ids: &[ObjectId]) -> ant_store::Result<Vec<bool>> {
            self.inner.has_many(ids).await
        }
    }

    /// The export reads each object body from the store at most once per
    /// fetch: trees are not read once to plan and again to delta-compress
    /// (git/git: 3.3 GB of trees, twice, from S3). Holds for blobless and
    /// full clones and for incremental fetches, whose `have` trees are read
    /// (once) only to exclude them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_object_is_read_from_the_store_at_most_once() {
        let b = Backend::in_memory();
        let tip = support::merge_history(&b, 300).await;
        let parent = b.meta.get_commits(&[tip]).await.unwrap()[0].clone().unwrap().parents[0];
        for (no_blobs, haves) in [(true, vec![]), (false, vec![]), (false, vec![parent])] {
            let counting = std::sync::Arc::new(Counting {
                inner: b.objects.clone(),
                reads: Default::default(),
            });
            let counted = Backend { objects: counting.clone(), ..b.clone() };
            let fetch = crate::select::Fetch {
                wants: vec![tip],
                haves: haves.clone(),
                no_blobs,
                done: true,
                ..Default::default()
            };
            let selection = crate::select::select(&counted, &fetch).await.unwrap();
            let (dir, _) = receive(counted, selection, true, no_blobs).await;
            let reads = std::mem::take(&mut *counting.reads.lock().unwrap());
            let sent = support::packed_objects(&dir.path().join("objects/pack")).await;
            let trees = sent.iter().filter(|(kind, _)| kind == "tree").count();
            assert!(trees > if haves.is_empty() { 100 } else { 0 }, "{trees} trees");
            assert!(
                reads.values().all(|n| *n == 1),
                "no_blobs={no_blobs} haves={}: {} objects read more than once",
                haves.len(),
                reads.values().filter(|n| **n > 1).count()
            );
            if haves.is_empty() {
                assert_eq!(reads.len(), sent.len(), "reads only what it sends");
                git(dir.path(), &["update-ref", "refs/heads/main", &tip.to_string()]).await;
                git(dir.path(), &["fsck", "--full"]).await;
            } else {
                assert!(reads.len() > sent.len(), "the client's trees are read to exclude them");
            }
        }
    }

    #[tokio::test]
    async fn real_git_accepts_streamed_pack_across_batches_and_sidebands() {
        let b = Backend::in_memory();
        let mut objects: Vec<_> =
            (0_u32..520).map(|n| EncodedObject::new(ObjectKind::Blob, &n.to_be_bytes())).collect();
        // Incompressible payload crosses several maximum-size sidebands.
        objects.push(EncodedObject::new(ObjectKind::Blob, &support::noise(123456789, 200_000)));
        let ids = objects.iter().map(EncodedObject::id).collect();
        b.objects.put_many(&objects).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        git(dir.path(), &["init", "--bare", "--object-format=sha256"]).await;
        let mut child = support::git_command(dir.path())
            .args(["index-pack", "--stdin"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let task =
            tokio::spawn(send(b, Selection { direct: ids, ..Default::default() }, false, tx));
        let mut flushed = false;
        let mut total = 0;
        while let Some(chunk) = rx.recv().await {
            let bytes = chunk.unwrap();
            if bytes == b"0000"[..] {
                flushed = true;
                continue;
            }
            assert!(!flushed);
            let len = usize::from_str_radix(std::str::from_utf8(&bytes[..4]).unwrap(), 16).unwrap();
            assert_eq!(len, bytes.len());
            assert!(len <= 65520);
            assert_eq!(bytes[4], 1);
            input.write_all(&bytes[5..]).await.unwrap();
            total += len - 5;
        }
        drop(input);
        task.await.unwrap().unwrap();
        assert!(flushed && total > 200_000);
        let output = child.wait_with_output().await.unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    }
}
