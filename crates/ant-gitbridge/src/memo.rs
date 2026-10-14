//! Bounded memo of converted objects (source SHA-1 -> antimony SHA-256).
//!
//! A fixed-size, 4-way set-associative table with no per-entry allocation:
//! every slot is 56 bytes (20-byte key, 4-byte recency stamp, 32-byte value).
//! When a bucket is full the least recently used slot is replaced. A miss only
//! costs a conversion again (the importer's fallback path), so evictions never
//! change the resulting ids; memory stays flat however long the history is.

use std::hash::{BuildHasherDefault, Hasher};

use ant_core::ObjectId;

/// A source (SHA-1) object id as raw bytes.
pub(crate) type Sha1 = [u8; 20];

const WAYS: usize = 4;
const SLOT: usize = 56;
const STAMP: usize = 20;
const VAL: usize = 24;

/// Identity hasher for keys that are already uniformly distributed digests.
#[derive(Default)]
pub(crate) struct DigestHasher(u64);

impl Hasher for DigestHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        if let Some(head) = bytes.first_chunk::<8>() {
            self.0 = u64::from_le_bytes(*head);
        } else {
            for b in bytes {
                self.0 = self.0.rotate_left(8) ^ u64::from(*b);
            }
        }
    }
}

pub(crate) type DigestBuild = BuildHasherDefault<DigestHasher>;
pub(crate) type DigestMap<V> = std::collections::HashMap<Sha1, V, DigestBuild>;

pub(crate) struct Memo {
    slots: Vec<[u8; SLOT]>,
    bucket_mask: usize,
    clock: u32,
    pub hits: u64,
    pub misses: u64,
}

impl Memo {
    /// A memo holding about `entries` objects (rounded up to a power of two,
    /// at least 1024).
    pub fn with_capacity(entries: usize) -> Self {
        let buckets = (entries.max(1024) / WAYS).next_power_of_two();
        Self {
            slots: vec![[0u8; SLOT]; buckets * WAYS],
            bucket_mask: buckets - 1,
            clock: 1,
            hits: 0,
            misses: 0,
        }
    }

    #[cfg(test)]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    fn bucket(&self, key: &Sha1) -> usize {
        let h =
            u64::from_le_bytes([key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7]]);
        (h as usize & self.bucket_mask) * WAYS
    }

    fn stamp(slot: &[u8; SLOT]) -> u32 {
        u32::from_le_bytes([slot[STAMP], slot[STAMP + 1], slot[STAMP + 2], slot[STAMP + 3]])
    }

    pub fn get(&mut self, key: &Sha1) -> Option<ObjectId> {
        let b = self.bucket(key);
        let clock = self.clock;
        for slot in &mut self.slots[b..b + WAYS] {
            if Self::stamp(slot) != 0 && slot[..STAMP] == key[..] {
                slot[STAMP..STAMP + 4].copy_from_slice(&clock.to_le_bytes());
                let mut v = [0u8; 32];
                v.copy_from_slice(&slot[VAL..]);
                self.hits += 1;
                return Some(ObjectId(v));
            }
        }
        self.misses += 1;
        None
    }

    pub fn insert(&mut self, key: &Sha1, val: ObjectId) {
        let b = self.bucket(key);
        self.clock = self.clock.saturating_add(1).max(1);
        let clock = self.clock;
        let bucket = &mut self.slots[b..b + WAYS];
        let mut victim = 0;
        let mut oldest = u32::MAX;
        for (i, slot) in bucket.iter().enumerate() {
            let st = Self::stamp(slot);
            if st != 0 && slot[..STAMP] == key[..] {
                victim = i;
                break;
            }
            if st < oldest {
                oldest = st;
                victim = i;
            }
        }
        let slot = &mut bucket[victim];
        slot[..STAMP].copy_from_slice(key);
        slot[STAMP..STAMP + 4].copy_from_slice(&clock.to_le_bytes());
        slot[VAL..].copy_from_slice(val.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: u64) -> Sha1 {
        let mut k = [0u8; 20];
        // Spread like a digest would.
        k[..8].copy_from_slice(&n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
        k[8..16].copy_from_slice(&n.to_le_bytes());
        k
    }

    #[test]
    fn roundtrip_and_update() {
        let mut m = Memo::with_capacity(4096);
        assert!(m.get(&key(1)).is_none());
        m.insert(&key(1), ObjectId([7; 32]));
        assert_eq!(m.get(&key(1)), Some(ObjectId([7; 32])));
        m.insert(&key(1), ObjectId([8; 32]));
        assert_eq!(m.get(&key(1)), Some(ObjectId([8; 32])));
    }

    #[test]
    fn bounded_and_never_wrong() {
        let mut m = Memo::with_capacity(2048);
        let cap = m.capacity();
        for n in 0..(cap as u64 * 8) {
            m.insert(&key(n), ObjectId([(n % 251) as u8; 32]));
        }
        let mut present = 0;
        for n in 0..(cap as u64 * 8) {
            if let Some(v) = m.get(&key(n)) {
                assert_eq!(v, ObjectId([(n % 251) as u8; 32]), "stale value for {n}");
                present += 1;
            }
        }
        assert!(present <= cap);
        assert!(present > cap / 4, "table should retain a good share: {present}/{cap}");
    }

    #[test]
    fn recently_used_entries_survive() {
        let mut m = Memo::with_capacity(1024);
        let hot = key(1_000_000);
        m.insert(&hot, ObjectId([1; 32]));
        for n in 0..50_000u64 {
            m.insert(&key(n), ObjectId([2; 32]));
            if n % 8 == 0 {
                assert_eq!(m.get(&hot), Some(ObjectId([1; 32])), "evicted at {n}");
            }
        }
    }
}
