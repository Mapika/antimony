//! Typed, recursive derived data on top of the generic derived-data space
//! ([`MetaStore::get_derived`] and friends).
//!
//! A [`Derivation`] names one kind of derived value: its versioned kind
//! string (the compatibility surface: a format change gets a new version,
//! and readers of the new version never see rows of the old one), how a key
//! is laid out, how values are encoded, and the recursive rule that defines
//! the value at a node from its predecessor's value plus the node's own
//! change ([`Derivation::derive`]). The value at a node is therefore a fold
//! of `derive` over its predecessor chain, and any stored value along that
//! chain can stand in for the whole fold below it.
//!
//! Callers evaluate that recursion iteratively: walk the predecessor chain
//! newest first in batches, probing each batch for stored values with one
//! [`get_many`] (never one round trip per node, never deep recursion), stop
//! at the first stored value (or the chain's base), and fold back up.
//! [`Policy`] says when values are computed and which ones are stored.
//!
//! Values are immutable functions of their key: concurrent writers of the
//! same key write the same bytes. Commit-keyed kinds (key = commit id ++
//! suffix) are deleted with their commit by garbage collection, which must
//! list them (`postgres::gc::DEFAULT_COMMIT_KEYED_KINDS`).

use bytes::Bytes;

use crate::{MetaStore, Result};

/// When a kind's values are computed, and which are stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    /// Computed when the node is recorded (or by a fill after an import);
    /// every value along a filled chain is stored.
    Eager,
    /// Computed on the first query; only the queried node's value is
    /// stored, values below it are reused when present but not filled in.
    Deferred,
}

/// One kind of recursively derived value (see the module docs).
pub trait Derivation {
    /// Versioned kind string, e.g. `"blame/v2"`.
    const KIND: &'static str;
    const POLICY: Policy;
    type Key;
    type Value;
    /// What one node contributes on top of its predecessor's value.
    type Change;

    /// The key's bytes in the derived-data space.
    fn key(key: &Self::Key) -> Vec<u8>;
    fn encode(value: &Self::Value) -> Bytes;
    /// `None` for a malformed value: callers treat it as missing.
    fn decode(key: &Self::Key, bytes: &[u8]) -> Option<Self::Value>;
    /// The value at `key` from its predecessor's value (`None`: `key` is the
    /// base of its chain, or the chain below it is gone) and its change.
    fn derive(key: &Self::Key, change: &Self::Change, parent: Option<&Self::Value>) -> Self::Value;
}

/// Entries written per `put_derived_many` call.
const WRITE_BATCH: usize = 4096;

/// The stored value of `key`, if any.
pub async fn get<D: Derivation>(meta: &dyn MetaStore, key: &D::Key) -> Result<Option<D::Value>> {
    Ok(meta.get_derived(D::KIND, &D::key(key)).await?.and_then(|v| D::decode(key, &v)))
}

/// Index-aligned stored values of `keys`: one store round trip.
pub async fn get_many<D: Derivation>(
    meta: &dyn MetaStore,
    keys: &[D::Key],
) -> Result<Vec<Option<D::Value>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let raw: Vec<Vec<u8>> = keys.iter().map(D::key).collect();
    let refs: Vec<&[u8]> = raw.iter().map(Vec::as_slice).collect();
    let found = meta.get_derived_many(D::KIND, &refs).await?;
    Ok(keys.iter().zip(found).map(|(k, v)| v.and_then(|v| D::decode(k, &v))).collect())
}

/// Store `entries`, a few thousand per round trip.
pub async fn put_many<D: Derivation>(
    meta: &dyn MetaStore,
    entries: &[(D::Key, D::Value)],
) -> Result<()> {
    for chunk in entries.chunks(WRITE_BATCH) {
        let rows = chunk.iter().map(|(k, v)| (D::key(k), D::encode(v))).collect();
        meta.put_derived_many(D::KIND, rows).await?;
    }
    Ok(())
}

/// Store one value.
pub async fn put<D: Derivation>(
    meta: &dyn MetaStore,
    key: &D::Key,
    value: &D::Value,
) -> Result<()> {
    meta.put_derived(D::KIND, &D::key(key), D::encode(value)).await
}

/// Fold `chain` (oldest first: each node's key and change) up from `base`,
/// the stored value just below its oldest node (`None`: the chain starts at
/// a base). Returns every node's value, oldest first; what to store of them
/// is the caller's [`Policy`].
pub fn fold<D: Derivation>(
    base: Option<D::Value>,
    chain: impl IntoIterator<Item = (D::Key, D::Change)>,
) -> Vec<(D::Key, D::Value)> {
    let mut out: Vec<(D::Key, D::Value)> = Vec::new();
    let mut base = base;
    for (key, change) in chain {
        let parent = match out.last() {
            Some((_, v)) => Some(v),
            None => base.as_ref(),
        };
        let value = D::derive(&key, &change, parent);
        base = None;
        out.push((key, value));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryMetaStore;

    /// Running sum: value(n) = value(pred) + change.
    struct Sum;
    impl Derivation for Sum {
        const KIND: &'static str = "test/sum/v1";
        const POLICY: Policy = Policy::Eager;
        type Key = u32;
        type Value = u64;
        type Change = u64;
        fn key(key: &u32) -> Vec<u8> {
            key.to_be_bytes().to_vec()
        }
        fn encode(v: &u64) -> Bytes {
            Bytes::copy_from_slice(&v.to_le_bytes())
        }
        fn decode(_: &u32, b: &[u8]) -> Option<u64> {
            Some(u64::from_le_bytes(b.try_into().ok()?))
        }
        fn derive(_: &u32, change: &u64, parent: Option<&u64>) -> u64 {
            parent.copied().unwrap_or(0) + change
        }
    }

    #[tokio::test]
    async fn typed_batched_roundtrip_and_fold() {
        let meta = MemoryMetaStore::default();
        let values = fold::<Sum>(None, (0..10u32).map(|k| (k, u64::from(k))));
        assert_eq!(values.last(), Some(&(9, 45)));
        put_many::<Sum>(&meta, &values).await.unwrap();
        // Resuming from a stored value gives the same fold.
        let base = get::<Sum>(&meta, &4).await.unwrap();
        let tail = fold::<Sum>(base, (5..10u32).map(|k| (k, u64::from(k))));
        assert_eq!(tail, values[5..]);
        let got = get_many::<Sum>(&meta, &[3, 42, 9]).await.unwrap();
        assert_eq!(got, vec![Some(6), None, Some(45)]);
        // Malformed values read as missing.
        put::<Sum>(&meta, &100, &1).await.unwrap();
        meta.put_derived(Sum::KIND, &Sum::key(&101), Bytes::from_static(b"x")).await.unwrap();
        assert_eq!(get_many::<Sum>(&meta, &[100, 101]).await.unwrap(), vec![Some(1), None]);
        assert!(get_many::<Sum>(&meta, &[]).await.unwrap().is_empty());
    }
}
