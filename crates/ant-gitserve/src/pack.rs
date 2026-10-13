//! SHA-256 packs with path-local OFS_DELTA entries and bounded backpressure.
//! One blocking worker owns compression state; a bounded read queue overlaps
//! store I/O with compression and HTTP sending. No pack-sized buffer is built.
use std::io::{self, Write};

use ant_core::{EncodedObject, ObjectId, ObjectKind};
use ant_store::Backend;
use anyhow::{Result, anyhow, ensure};
use bytes::Bytes;
use flate2::{Compression, write::ZlibEncoder};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::{self, Sender};

use crate::{delta, protocol::packet};

pub type Chunk = Result<Bytes, io::Error>;
const SIDEBAND: usize = 65515;
const READ_BATCH: usize = 16;
const MAX_BASE: usize = 8 * 1024 * 1024;
const MAX_DEPTH: u8 = 50;

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

fn header(writer: &mut PackWriter, kind: u8, mut size: usize) -> io::Result<()> {
    let mut byte = (kind << 4) | (size as u8 & 15);
    size >>= 4;
    let mut header = Vec::with_capacity(10);
    while size != 0 {
        header.push(byte | 128);
        byte = size as u8 & 127;
        size >>= 7;
    }
    header.push(byte);
    writer.write_all(&header)
}

/// OFS_DELTA uses a biased, big-endian base-128 distance, not a normal varint.
fn distance(writer: &mut PackWriter, mut offset: u64) -> io::Result<()> {
    let mut bytes = [0_u8; 10];
    let mut pos = bytes.len() - 1;
    bytes[pos] = (offset & 127) as u8;
    while offset > 127 {
        offset = (offset >> 7) - 1;
        pos -= 1;
        bytes[pos] = 128 | (offset & 127) as u8;
    }
    writer.write_all(&bytes[pos..])
}

struct Base {
    path: u64,
    object: EncodedObject,
    offset: u64,
    depth: u8,
}

fn object(
    writer: &mut PackWriter,
    object: EncodedObject,
    path: u64,
    base: &mut Option<Base>,
    deltas: bool,
) -> io::Result<()> {
    let offset = writer.position;
    let eligible = deltas && object.kind() != ObjectKind::Commit && object.body().len() <= MAX_BASE;
    let candidate = base.as_ref().filter(|b| {
        eligible && b.path == path && b.object.kind() == object.kind() && b.depth < MAX_DEPTH
    });
    let instructions = candidate.and_then(|b| delta::encode(b.object.body(), object.body()));
    let mut depth = 0;
    let body = if let (Some(instructions), Some(base)) = (&instructions, candidate) {
        header(writer, 6, instructions.len())?;
        distance(writer, offset - base.offset)?;
        depth = base.depth + 1;
        instructions.as_slice()
    } else {
        let kind = match object.kind() {
            ObjectKind::Commit => 1,
            ObjectKind::Tree => 2,
            ObjectKind::Blob => 3,
        };
        header(writer, kind, object.body().len())?;
        object.body()
    };
    let mut encoder = ZlibEncoder::new(writer, Compression::fast());
    encoder.write_all(body)?;
    encoder.finish()?;
    *base = eligible.then_some(Base { path, object, offset, depth });
    Ok(())
}

#[cfg(test)]
pub async fn send(b: Backend, ids: Vec<ObjectId>, tx: Sender<Chunk>) -> Result<()> {
    let paths = vec![0; ids.len()];
    send_planned(b, ids, paths, false, tx).await
}

pub async fn send_planned(
    b: Backend,
    ids: Vec<ObjectId>,
    paths: Vec<u64>,
    deltas: bool,
    tx: Sender<Chunk>,
) -> Result<()> {
    ensure!(ids.len() == paths.len(), "invalid pack plan");
    let count = u32::try_from(ids.len())?;
    let (read_tx, mut read_rx) = mpsc::channel::<Vec<(u64, EncodedObject)>>(1);
    let worker = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut writer = PackWriter {
            tx,
            hash: Sha256::new(),
            position: 0,
            pending: Vec::with_capacity(SIDEBAND),
        };
        writer.write_all(b"PACK")?;
        writer.write_all(&2_u32.to_be_bytes())?;
        writer.write_all(&count.to_be_bytes())?;
        let mut base = None;
        let mut received = 0_u32;
        while let Some(objects) = read_rx.blocking_recv() {
            for (path, item) in objects {
                object(&mut writer, item, path, &mut base, deltas)?;
                received += 1;
            }
        }
        ensure!(received == count, "incomplete pack read");
        let digest = writer.hash.clone().finalize();
        writer.write_all(&digest)?;
        writer.flush()?;
        writer.tx.blocking_send(Ok(Bytes::from_static(b"0000")))?;
        Ok(())
    });
    let reader = async move {
        for (chunk, paths) in ids.chunks(READ_BATCH).zip(paths.chunks(READ_BATCH)) {
            if read_tx.is_closed() {
                return Err(anyhow!("pack consumer disconnected"));
            }
            let objects = b.objects.get_many(chunk).await?;
            ensure!(objects.len() == chunk.len(), "incomplete store batch");
            let objects: Result<Vec<_>> = paths
                .iter()
                .zip(objects)
                .map(|(path, item)| Ok((*path, item.ok_or_else(|| anyhow!("object disappeared"))?)))
                .collect();
            read_tx.send(objects?).await.map_err(|_| anyhow!("pack consumer disconnected"))?;
        }
        Ok(())
    };
    let (read_result, write_result) = tokio::join!(reader, worker);
    read_result?;
    write_result??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    mod support {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/support/mod.rs"));
    }

    async fn git(dir: &std::path::Path, args: &[&str]) -> String {
        let result = tokio::process::Command::new("git")
            .current_dir(dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .args(args)
            .output()
            .await
            .unwrap();
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
        let mut command = tokio::process::Command::new("git");
        command.current_dir(dir.path()).args(["index-pack", "--stdin", "--fix-thin"]);
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
        let sender = tokio::spawn(send_planned(b, selection.ids, selection.paths, deltas, tx));
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

    fn rss() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .unwrap_or_default()
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:")?.split_whitespace().next()?.parse().ok())
            .unwrap_or(0)
    }

    // This exercises the same planner and streaming encoder as HTTP, with real
    // git over stdin, so it also runs in sandboxes that prohibit listen sockets.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn synthetic_history_pack_size_time_and_peak_rss() {
        use std::{
            sync::{
                Arc,
                atomic::{AtomicU64, Ordering},
            },
            time::Instant,
        };
        let (b, expected_undeltified) = support::history(2000, 2000).await;
        let tip = b.refs.list_refs("").await.unwrap()[0].1;
        let mut sizes = Vec::new();
        for deltas in [false, true] {
            let before = rss();
            let peak = Arc::new(AtomicU64::new(before));
            let monitor_peak = peak.clone();
            let monitor = tokio::spawn(async move {
                loop {
                    monitor_peak.fetch_max(rss(), Ordering::Relaxed);
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            });
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
        let old: std::collections::HashSet<_> = selection.ids.iter().copied().collect();
        let (dir, _) = receive(b.clone(), selection, true, false).await;
        git(dir.path(), &["update-ref", "refs/heads/main", &tip.to_string()]).await;
        git(dir.path(), &["fsck", "--full"]).await;
        let mut trees = 0;
        let mut blobs = 0;
        let mut depth = 0;
        for entry in std::fs::read_dir(dir.path().join("objects/pack")).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "idx") {
                for line in
                    git(dir.path(), &["verify-pack", "-v", path.to_str().unwrap()]).await.lines()
                {
                    let fields: Vec<_> = line.split_whitespace().collect();
                    if fields.len() == 7 && fields[0].len() == 64 {
                        depth = depth.max(fields[5].parse::<usize>().unwrap());
                        trees += usize::from(fields[1] == "tree");
                        blobs += usize::from(fields[1] == "blob");
                    }
                }
            }
        }
        assert!(trees > 50 && blobs > 50, "trees={trees} blobs={blobs}");
        assert_eq!(depth, usize::from(MAX_DEPTH));
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
        assert_eq!(selection.ids.len(), 30);
        assert!(selection.ids.iter().all(|id| !old.contains(id)));
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

    #[tokio::test]
    async fn failed_reads_and_disconnected_clients_stop_the_pipeline() {
        let b = Backend::in_memory();
        let (tx, mut rx) = mpsc::channel(8);
        assert!(
            send_planned(b.clone(), vec![ObjectId::empty_tree()], vec![0], true, tx).await.is_err()
        );
        while let Some(chunk) = rx.recv().await {
            assert_ne!(chunk.unwrap(), b"0000"[..], "failed pack must not finish successfully");
        }
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            send_planned(b, vec![], vec![], true, tx),
        )
        .await;
        assert!(result.unwrap().is_err());
    }

    #[tokio::test]
    async fn real_git_accepts_streamed_pack_across_batches_and_sidebands() {
        let b = Backend::in_memory();
        let mut objects: Vec<_> =
            (0_u32..520).map(|n| EncodedObject::new(ObjectKind::Blob, &n.to_be_bytes())).collect();
        // Incompressible payload crosses several maximum-size sidebands.
        let mut state = 123456789_u32;
        let data: Vec<_> = (0..200_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect();
        objects.push(EncodedObject::new(ObjectKind::Blob, &data));
        let ids = objects.iter().map(EncodedObject::id).collect();
        b.objects.put_many(&objects).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let init = tokio::process::Command::new("git")
            .args(["init", "--bare", "--object-format=sha256"])
            .arg(dir.path())
            .output()
            .await
            .unwrap();
        assert!(init.status.success());
        let mut child = tokio::process::Command::new("git")
            .current_dir(dir.path())
            .args(["index-pack", "--stdin"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let task = tokio::spawn(send(b, ids, tx));
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
