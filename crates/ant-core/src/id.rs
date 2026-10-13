use std::fmt;
use std::str::FromStr;

use crate::{Error, Result};

/// SHA-256 object id.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub [u8; 32]);

impl ObjectId {
    pub const LEN: usize = 32;
    pub const HEX_LEN: usize = 64;

    pub fn from_bytes(b: &[u8]) -> Result<Self> {
        let arr: [u8; 32] = b
            .try_into()
            .map_err(|_| Error::InvalidId(format!("expected 32 bytes, got {}", b.len())))?;
        Ok(Self(arr))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Id of the empty tree (`"tree 0\0"`).
    pub fn empty_tree() -> Self {
        crate::hash_object(crate::ObjectKind::Tree, b"")
    }
}

impl FromStr for ObjectId {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        if s.len() != Self::HEX_LEN {
            return Err(Error::InvalidId(s.to_string()));
        }
        let mut out = [0u8; 32];
        hex::decode_to_slice(s, &mut out).map_err(|_| Error::InvalidId(s.to_string()))?;
        Ok(Self(out))
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({})", &self.to_hex()[..12])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let id = ObjectId([0xab; 32]);
        assert_eq!(id.to_hex().parse::<ObjectId>().unwrap(), id);
        assert!("abc".parse::<ObjectId>().is_err());
        assert!("zz".repeat(32).parse::<ObjectId>().is_err());
    }

    #[test]
    fn empty_tree_matches_git_sha256() {
        // `git init --object-format=sha256 && git hash-object -t tree /dev/null`
        assert_eq!(
            ObjectId::empty_tree().to_hex(),
            "6ef19b41225c5369f1c104d45d8d85efa9b057b53b14b4b9b939dd74decc5321"
        );
    }
}
