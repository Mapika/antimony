//! Wire format of the HTTP tree-bundle endpoint (`GET /t/{tree}`): many
//! encoded objects in one body.
//!
//! Body = one zstd frame whose decompressed content is
//!
//! ```text
//! MAGIC ("ANTBNDL1", 8 bytes)
//! frame*  where frame = id (32 bytes) | len (u32, big endian) | encoded object (len bytes)
//! ```
//!
//! The encoded object is exactly what `GET /o/{id}` returns
//! (`"<kind> <len>\0<body>"`). Frames carry their ids so a reader can check
//! every object against the id it claims ([`decode`] does not hash; callers
//! verify, as for every remote read). The first frame of a `/t/{tree}`
//! response is the requested tree.
//!
//! The format is versioned by the magic and, for caches, by the URL: a new
//! layout gets a new magic and a new route, never a change under `/t/`.

use std::io::Read;

use ant_core::ObjectId;
use bytes::Bytes;

/// First bytes of a decompressed bundle (format version 1).
pub const MAGIC: &[u8; 8] = b"ANTBNDL1";
/// `Content-Type` of bundle responses.
pub const CONTENT_TYPE: &str = "application/x-ant-bundle";
/// Response header set by every `/t/` response of a server that has the
/// route (also on its 404s), so clients can tell "no such tree" from "old
/// server without bundles".
pub const HEADER: &str = "x-ant-bundle";
/// zstd level used for bundle bodies (fast; responses are cached anyway).
const ZSTD_LEVEL: i32 = 1;
/// Frame header bytes: id plus length.
const FRAME_HEADER: usize = 32 + 4;

#[derive(Debug)]
pub enum BundleError {
    /// Not zstd, wrong magic, or a truncated frame.
    Malformed(String),
    /// Decompressed content larger than the caller's limit.
    TooLarge(usize),
}

impl std::fmt::Display for BundleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BundleError::Malformed(m) => write!(f, "malformed bundle: {m}"),
            BundleError::TooLarge(limit) => write!(f, "bundle exceeds {limit} bytes"),
        }
    }
}

impl std::error::Error for BundleError {}

/// Encode `objects` (id, encoded bytes) as a compressed bundle body.
pub fn encode<'a>(
    objects: impl IntoIterator<Item = (&'a ObjectId, &'a [u8])>,
) -> std::io::Result<Vec<u8>> {
    let mut raw = Vec::from(&MAGIC[..]);
    for (id, data) in objects {
        let len = u32::try_from(data.len())
            .map_err(|_| std::io::Error::other("object too large for a bundle frame"))?;
        raw.extend_from_slice(id.as_bytes());
        raw.extend_from_slice(&len.to_be_bytes());
        raw.extend_from_slice(data);
    }
    zstd::bulk::compress(&raw, ZSTD_LEVEL)
}

/// Decode a compressed bundle body into (claimed id, encoded bytes) pairs,
/// in order. Refuses content that decompresses to more than `max_bytes`.
/// Ids are not checked against the content: callers verify the hashes.
pub fn decode(body: &[u8], max_bytes: usize) -> Result<Vec<(ObjectId, Bytes)>, BundleError> {
    let decoder = zstd::stream::read::Decoder::new(body)
        .map_err(|e| BundleError::Malformed(e.to_string()))?;
    let mut raw = Vec::new();
    decoder
        .take(max_bytes as u64 + 1)
        .read_to_end(&mut raw)
        .map_err(|e| BundleError::Malformed(e.to_string()))?;
    if raw.len() > max_bytes {
        return Err(BundleError::TooLarge(max_bytes));
    }
    let raw = Bytes::from(raw);
    if raw.get(..MAGIC.len()) != Some(&MAGIC[..]) {
        return Err(BundleError::Malformed("bad magic".into()));
    }
    let mut out = Vec::new();
    let mut pos = MAGIC.len();
    while pos < raw.len() {
        let header = raw
            .get(pos..pos + FRAME_HEADER)
            .ok_or_else(|| BundleError::Malformed("truncated frame header".into()))?;
        let id = ObjectId::from_bytes(&header[..32])
            .map_err(|e| BundleError::Malformed(e.to_string()))?;
        let mut len = [0u8; 4];
        len.copy_from_slice(&header[32..]);
        let len = u32::from_be_bytes(len) as usize;
        let start = pos + FRAME_HEADER;
        let end = start
            .checked_add(len)
            .filter(|&e| e <= raw.len())
            .ok_or_else(|| BundleError::Malformed("truncated frame".into()))?;
        out.push((id, raw.slice(start..end)));
        pos = end;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_core::{EncodedObject, ObjectKind};

    fn objects(n: usize) -> Vec<EncodedObject> {
        (0..n)
            .map(|i| EncodedObject::new(ObjectKind::Blob, format!("blob {i}").repeat(i).as_bytes()))
            .collect()
    }

    #[test]
    fn round_trip() {
        for n in [0, 1, 7, 300] {
            let objs = objects(n);
            let ids: Vec<ObjectId> = objs.iter().map(EncodedObject::id).collect();
            let body = encode(ids.iter().zip(objs.iter().map(|o| o.as_bytes().as_ref()))).unwrap();
            let back = decode(&body, 64 << 20).unwrap();
            assert_eq!(back.len(), n);
            for ((id, data), (want_id, obj)) in back.iter().zip(ids.iter().zip(&objs)) {
                assert_eq!(id, want_id);
                assert_eq!(data, obj.as_bytes());
                EncodedObject::from_bytes(data.clone()).unwrap().verify(id).unwrap();
            }
        }
    }

    #[test]
    fn empty_object_frames_round_trip() {
        let id = ObjectId([7; 32]);
        let body = encode([(&id, &b""[..])]).unwrap();
        assert_eq!(decode(&body, 1024).unwrap(), vec![(id, Bytes::new())]);
    }

    #[test]
    fn rejects_malformed_and_oversized() {
        let objs = objects(20);
        let ids: Vec<ObjectId> = objs.iter().map(EncodedObject::id).collect();
        let body = encode(ids.iter().zip(objs.iter().map(|o| o.as_bytes().as_ref()))).unwrap();
        assert!(matches!(decode(&body, 100), Err(BundleError::TooLarge(100))));
        assert!(matches!(decode(b"not zstd at all", 1024), Err(BundleError::Malformed(_))));

        // Valid zstd, wrong magic.
        let bad = zstd::bulk::compress(b"ANTBNDL9", 1).unwrap();
        assert!(matches!(decode(&bad, 1024), Err(BundleError::Malformed(_))));

        // Every truncation of the decompressed stream inside a frame fails.
        let mut raw = Vec::from(&MAGIC[..]);
        raw.extend_from_slice(ids[3].as_bytes());
        raw.extend_from_slice(&(objs[3].as_bytes().len() as u32).to_be_bytes());
        raw.extend_from_slice(objs[3].as_bytes());
        for cut in MAGIC.len() + 1..raw.len() {
            let body = zstd::bulk::compress(&raw[..cut], 1).unwrap();
            assert!(decode(&body, 1 << 20).is_err(), "cut at {cut} accepted");
        }
        let body = zstd::bulk::compress(&raw, 1).unwrap();
        assert_eq!(decode(&body, 1 << 20).unwrap().len(), 1);
    }
}
