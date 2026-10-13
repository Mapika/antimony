use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::{Commit, Error, ObjectId, Result, Tree};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    Blob,
    Tree,
    Commit,
}

impl ObjectKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ObjectKind::Blob => "blob",
            ObjectKind::Tree => "tree",
            ObjectKind::Commit => "commit",
        }
    }

    fn parse(s: &[u8]) -> Result<Self> {
        match s {
            b"blob" => Ok(ObjectKind::Blob),
            b"tree" => Ok(ObjectKind::Tree),
            b"commit" => Ok(ObjectKind::Commit),
            other => Err(Error::Malformed(format!(
                "unknown object kind {:?}",
                String::from_utf8_lossy(other)
            ))),
        }
    }
}

/// Hash `body` as a git object of the given kind.
pub fn hash_object(kind: ObjectKind, body: &[u8]) -> ObjectId {
    let mut h = Sha256::new();
    h.update(kind.as_str().as_bytes());
    h.update(b" ");
    h.update(body.len().to_string().as_bytes());
    h.update([0u8]);
    h.update(body);
    ObjectId(h.finalize().into())
}

/// An object in its canonical encoded form: `"<kind> <len>\0<body>"`.
///
/// This is the unit stored by every `ObjectStore` and sent over the wire.
/// Cheap to clone (backed by `Bytes`).
#[derive(Clone, PartialEq, Eq)]
pub struct EncodedObject {
    kind: ObjectKind,
    header_len: usize,
    data: Bytes,
}

impl EncodedObject {
    pub fn new(kind: ObjectKind, body: &[u8]) -> Self {
        let header = format!("{} {}\0", kind.as_str(), body.len());
        let mut data = Vec::with_capacity(header.len() + body.len());
        data.extend_from_slice(header.as_bytes());
        data.extend_from_slice(body);
        Self { kind, header_len: header.len(), data: data.into() }
    }

    /// Parse and validate framing (does not verify the hash; see [`Self::verify`]).
    pub fn from_bytes(data: Bytes) -> Result<Self> {
        let nul = data
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| Error::Malformed("missing header NUL".into()))?;
        let header = &data[..nul];
        let sp = header
            .iter()
            .position(|&b| b == b' ')
            .ok_or_else(|| Error::Malformed("missing header space".into()))?;
        let kind = ObjectKind::parse(&header[..sp])?;
        // Canonical decimal only (no sign, no leading zeros), so each object
        // has exactly one encoding.
        let digits = &header[sp + 1..];
        let canonical = !digits.is_empty()
            && digits.iter().all(u8::is_ascii_digit)
            && (digits == b"0" || digits[0] != b'0');
        let len: usize = std::str::from_utf8(digits)
            .ok()
            .filter(|_| canonical)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| Error::Malformed("bad length".into()))?;
        if data.len() - nul - 1 != len {
            return Err(Error::Malformed(format!(
                "length mismatch: header says {len}, body is {}",
                data.len() - nul - 1
            )));
        }
        Ok(Self { kind, header_len: nul + 1, data })
    }

    pub fn kind(&self) -> ObjectKind {
        self.kind
    }

    pub fn body(&self) -> &[u8] {
        &self.data[self.header_len..]
    }

    /// The full encoded bytes, header included.
    pub fn as_bytes(&self) -> &Bytes {
        &self.data
    }

    pub fn into_bytes(self) -> Bytes {
        self.data
    }

    pub fn id(&self) -> ObjectId {
        ObjectId(Sha256::digest(&self.data).into())
    }

    pub fn verify(&self, expected: &ObjectId) -> Result<()> {
        let actual = self.id();
        if &actual != expected {
            return Err(Error::Malformed(format!("hash mismatch: expected {expected}, got {actual}")));
        }
        Ok(())
    }

    pub fn decode(&self) -> Result<Object> {
        Ok(match self.kind {
            ObjectKind::Blob => Object::Blob(Bytes::copy_from_slice(self.body())),
            ObjectKind::Tree => Object::Tree(Tree::decode(self.body())?),
            ObjectKind::Commit => Object::Commit(Commit::decode(self.body())?),
        })
    }
}

impl std::fmt::Debug for EncodedObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedObject")
            .field("kind", &self.kind)
            .field("body_len", &self.body().len())
            .finish()
    }
}

/// A decoded object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Object {
    Blob(Bytes),
    Tree(Tree),
    Commit(Commit),
}

impl Object {
    pub fn kind(&self) -> ObjectKind {
        match self {
            Object::Blob(_) => ObjectKind::Blob,
            Object::Tree(_) => ObjectKind::Tree,
            Object::Commit(_) => ObjectKind::Commit,
        }
    }

    pub fn encode(&self) -> EncodedObject {
        match self {
            Object::Blob(b) => EncodedObject::new(ObjectKind::Blob, b),
            Object::Tree(t) => EncodedObject::new(ObjectKind::Tree, &t.encode()),
            Object::Commit(c) => EncodedObject::new(ObjectKind::Commit, &c.encode()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_hash_matches_git_sha256() {
        // printf 'hello\n' | git hash-object --object-format=sha256 --stdin
        let o = EncodedObject::new(ObjectKind::Blob, b"hello\n");
        assert_eq!(
            o.id().to_hex(),
            "2cf8d83d9ee29543b34a87727421fdecb7e3f3a183d337639025de576db9ebb4"
        );
        assert_eq!(o.id(), hash_object(ObjectKind::Blob, b"hello\n"));
    }

    #[test]
    fn framing_roundtrip_and_validation() {
        let o = EncodedObject::new(ObjectKind::Blob, b"abc");
        let back = EncodedObject::from_bytes(o.as_bytes().clone()).unwrap();
        assert_eq!(back, o);
        assert_eq!(back.body(), b"abc");
        assert!(EncodedObject::from_bytes(Bytes::from_static(b"blob 4\0abc")).is_err());
        assert!(EncodedObject::from_bytes(Bytes::from_static(b"nope 3\0abc")).is_err());
        assert!(EncodedObject::from_bytes(Bytes::from_static(b"blob 3abc")).is_err());
        assert!(o.verify(&o.id()).is_ok());
        assert!(o.verify(&ObjectId([0; 32])).is_err());
    }
}
