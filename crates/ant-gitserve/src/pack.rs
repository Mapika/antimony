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
        let mut writer = PackWriter {
            tx,
            hash: Sha256::new(),
        };
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
                object(
                    &mut writer,
                    &item.ok_or_else(|| anyhow!("object disappeared"))?,
                )?;
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
