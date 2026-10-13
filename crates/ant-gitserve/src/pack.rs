//! Incremental SHA-256 pack encoder with bounded side-band backpressure.
use std::io::{self, Write};

use ant_core::{EncodedObject, ObjectId, ObjectKind};
use ant_store::Backend;
use anyhow::{Result, anyhow};
use bytes::Bytes;
use flate2::{Compression, write::ZlibEncoder};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::Sender;

use crate::{protocol::packet, select::BATCH};

pub type Chunk = Result<Bytes, io::Error>;

struct PackWriter {
    tx: Sender<Chunk>,
    hash: Sha256,
}

impl Write for PackWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for chunk in bytes.chunks(65515) {
            let mut payload = Vec::with_capacity(chunk.len() + 1);
            payload.push(1);
            payload.extend_from_slice(chunk);
            self.tx
                .blocking_send(Ok(packet(&payload).into()))
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "client disconnected"))?;
            self.hash.update(chunk);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn object(writer: &mut PackWriter, object: &EncodedObject) -> io::Result<()> {
    let kind = match object.kind() {
        ObjectKind::Commit => 1,
        ObjectKind::Tree => 2,
        ObjectKind::Blob => 3,
    };
    let mut size = object.body().len();
    let mut byte = (kind << 4) | (size as u8 & 15);
    size >>= 4;
    let mut header = Vec::with_capacity(10);
    while size != 0 {
        header.push(byte | 128);
        byte = size as u8 & 127;
        size >>= 7;
    }
    header.push(byte);
    writer.write_all(&header)?;
    let mut encoder = ZlibEncoder::new(writer, Compression::fast());
    encoder.write_all(object.body())?;
    encoder.finish()?;
    Ok(())
}

pub async fn send(b: Backend, ids: Vec<ObjectId>, tx: Sender<Chunk>) -> Result<()> {
    let count = u32::try_from(ids.len())?;
    let mut writer = tokio::task::spawn_blocking(move || -> Result<_> {
        let mut writer = PackWriter { tx, hash: Sha256::new() };
        writer.write_all(b"PACK")?;
        writer.write_all(&2_u32.to_be_bytes())?;
        writer.write_all(&count.to_be_bytes())?;
        Ok(writer)
    })
    .await??;
    for chunk in ids.chunks(BATCH) {
        let objects = b.objects.get_many(chunk).await?;
        writer = tokio::task::spawn_blocking(move || -> Result<_> {
            for item in objects {
                object(&mut writer, &item.ok_or_else(|| anyhow!("object disappeared"))?)?;
            }
            Ok(writer)
        })
        .await??;
    }
    tokio::task::spawn_blocking(move || -> Result<()> {
        let digest = writer.hash.clone().finalize();
        writer.write_all(&digest)?;
        writer.tx.blocking_send(Ok(Bytes::from_static(b"0000")))?;
        Ok(())
    })
    .await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

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
