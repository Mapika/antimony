//! Reading source objects with a reusable buffer.

use ant_core::{EncodedObject, ObjectKind};
use anyhow::Context as _;
use gix::prelude::Find as _;

use crate::commit::Oid;
use crate::memo::Sha1;

pub(crate) fn oid(sha: &Sha1) -> Oid {
    Oid::from(*sha)
}

pub(crate) fn sha(id: &Oid) -> Sha1 {
    let mut out = [0u8; 20];
    let b = id.as_bytes();
    let n = b.len().min(20);
    out[..n].copy_from_slice(&b[..n]);
    out
}

/// One thread's handle on the source repository.
pub(crate) struct Reader {
    repo: gix::Repository,
    buf: Vec<u8>,
}

impl Reader {
    pub fn new(repo: gix::Repository) -> Self {
        Self { repo, buf: Vec::new() }
    }

    fn find(&mut self, id: &Oid) -> anyhow::Result<&[u8]> {
        // Do not let one huge object pin its buffer for the rest of the run.
        if self.buf.capacity() > 4 << 20 {
            self.buf = Vec::new();
        }
        let data = self
            .repo
            .objects
            .try_find(id, &mut self.buf)
            .with_context(|| format!("reading object {id}"))?
            .with_context(|| format!("reading object {id}: not found"))?;
        Ok(data.data)
    }

    /// The object's body (an owned copy).
    pub fn read(&mut self, id: &Oid) -> anyhow::Result<Vec<u8>> {
        Ok(self.find(id)?.to_vec())
    }

    /// A blob as an antimony object (header added in a single copy).
    pub fn read_blob(&mut self, id: &Oid) -> anyhow::Result<EncodedObject> {
        Ok(EncodedObject::new(ObjectKind::Blob, self.find(id)?))
    }
}
