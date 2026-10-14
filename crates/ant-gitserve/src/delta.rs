//! Git delta instructions. A block index sized to the base (at most 2^20
//! slots) bounds both memory and collision work, including on repetitive or
//! adversarial input. An index is built once per base and reused for every
//! target tried against it (see the delta window in `pack.rs`).
const BLOCK: usize = 16;
const MIN_SLOTS: usize = 1 << 8;
const MAX_SLOTS: usize = 1 << 20;
const EMPTY: u32 = u32::MAX;

fn size(out: &mut Vec<u8>, mut n: usize) {
    loop {
        let byte = (n & 127) as u8;
        n >>= 7;
        out.push(byte | if n == 0 { 0 } else { 128 });
        if n == 0 {
            break;
        }
    }
}

fn hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0_u64, |h, &b| h.wrapping_mul(257).wrapping_add(u64::from(b)))
}

fn insert(out: &mut Vec<u8>, bytes: &[u8]) {
    for chunk in bytes.chunks(127) {
        out.push(chunk.len() as u8);
        out.extend_from_slice(chunk);
    }
}

fn copy(out: &mut Vec<u8>, mut offset: usize, mut length: usize) {
    while length > 0 {
        let count = length.min(0xff_ffff);
        let instruction = out.len();
        out.push(128);
        for (n, byte) in (offset as u32).to_le_bytes().into_iter().enumerate() {
            if byte != 0 {
                out[instruction] |= 1 << n;
                out.push(byte);
            }
        }
        // A zero encoded size means 64 KiB in git's instruction format.
        if count != 0x10000 {
            for (n, byte) in (count as u32).to_le_bytes()[..3].iter().enumerate() {
                if *byte != 0 {
                    out[instruction] |= 1 << (n + 4);
                    out.push(*byte);
                }
            }
        }
        offset += count;
        length -= count;
    }
}

/// Block positions of one base, keyed by its rolling hash. The low bits of
/// the polynomial hash are close to a plain byte sum, which collides heavily
/// on text, so slots take the high bits of a multiplicative mix instead.
pub struct Index {
    table: Vec<u32>,
    shift: u32,
}

impl Index {
    /// `None` when `base` is too small to match or too large to address.
    #[cfg(test)]
    pub fn new(base: &[u8]) -> Option<Self> {
        Self::reuse(base, Vec::new())
    }

    /// A new index in the allocation of an earlier index's table (see
    /// [`Index::into_table`]): delta search builds one index per base, and
    /// reusing tables keeps that churn away from the allocator.
    pub fn reuse(base: &[u8], mut table: Vec<u32>) -> Option<Self> {
        if base.len() < BLOCK || base.len() > u32::MAX as usize {
            return None;
        }
        let slots = (base.len() / BLOCK * 2).next_power_of_two().clamp(MIN_SLOTS, MAX_SLOTS);
        table.clear();
        table.resize(slots, EMPTY);
        let mut index = Self { table, shift: 64 - slots.trailing_zeros() };
        // Reverse order: on a collision the earliest block wins.
        for pos in (0..base.len() - BLOCK + 1).step_by(BLOCK).rev() {
            let slot = index.slot(hash(&base[pos..pos + BLOCK]));
            index.table[slot] = pos as u32;
        }
        Some(index)
    }

    fn slot(&self, rolling: u64) -> usize {
        (rolling.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> self.shift) as usize
    }

    /// Heap bytes held by the index (for window memory accounting).
    pub fn heap_bytes(&self) -> usize {
        self.table.capacity() * std::mem::size_of::<u32>()
    }

    pub fn into_table(self) -> Vec<u32> {
        self.table
    }
}

/// Delta instructions turning `base` into `target`, or `None` unless they are
/// shorter than `limit` bytes. `index` must have been built from `base`.
pub fn encode_indexed(index: &Index, base: &[u8], target: &[u8], limit: usize) -> Option<Vec<u8>> {
    if target.len() < BLOCK {
        return None;
    }
    let mut out = Vec::new();
    size(&mut out, base.len());
    size(&mut out, target.len());
    let mut pos = 0;
    let mut literal = 0;
    let power = 257_u64.wrapping_pow((BLOCK - 1) as u32);
    let mut rolling = hash(&target[..BLOCK]);
    while pos + BLOCK <= target.len() {
        let candidate = index.table[index.slot(rolling)];
        let offset = candidate as usize;
        if candidate != EMPTY && base[offset..offset + BLOCK] == target[pos..pos + BLOCK] {
            // Grow the match backwards into the pending literal, then forwards.
            let mut back = 0;
            while back < pos - literal
                && back < offset
                && base[offset - back - 1] == target[pos - back - 1]
            {
                back += 1;
            }
            insert(&mut out, &target[literal..pos - back]);
            let mut length = BLOCK;
            while offset + length < base.len()
                && pos + length < target.len()
                && base[offset + length] == target[pos + length]
            {
                length += 1;
            }
            copy(&mut out, offset - back, length + back);
            pos += length;
            literal = pos;
            if pos + BLOCK <= target.len() {
                rolling = hash(&target[pos..pos + BLOCK]);
            }
        } else {
            if pos + BLOCK < target.len() {
                rolling = rolling
                    .wrapping_sub(u64::from(target[pos]).wrapping_mul(power))
                    .wrapping_mul(257)
                    .wrapping_add(u64::from(target[pos + BLOCK]));
            }
            pos += 1;
        }
        // Pending literals cost at least their own length.
        if out.len() + pos - literal >= limit {
            return None;
        }
    }
    insert(&mut out, &target[literal..]);
    (out.len() < limit).then_some(out)
}

fn varint(input: &mut &[u8]) -> Option<usize> {
    let mut n = 0_usize;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = input.split_first()?;
        *input = rest;
        n |= usize::from(byte & 127).checked_shl(shift)?;
        if byte & 128 == 0 {
            return Some(n);
        }
    }
    None
}

/// Rebuild the target of `delta` against `base` into `out` (cleared first).
/// `None` if the instructions are malformed or do not match `base`.
pub fn apply(base: &[u8], mut delta: &[u8], out: &mut Vec<u8>) -> Option<()> {
    out.clear();
    if varint(&mut delta)? != base.len() {
        return None;
    }
    let length = varint(&mut delta)?;
    out.reserve(length);
    while let Some((&op, rest)) = delta.split_first() {
        delta = rest;
        if op & 128 == 0 {
            let count = usize::from(op);
            if count == 0 || count > delta.len() {
                return None;
            }
            out.extend_from_slice(&delta[..count]);
            delta = &delta[count..];
        } else {
            let (mut offset, mut size) = (0_usize, 0_usize);
            for n in 0..7 {
                if op & (1 << n) != 0 {
                    let (&value, rest) = delta.split_first()?;
                    delta = rest;
                    if n < 4 {
                        offset |= usize::from(value) << (8 * n);
                    } else {
                        size |= usize::from(value) << (8 * (n - 4));
                    }
                }
            }
            if size == 0 {
                size = 0x10000;
            }
            out.extend_from_slice(base.get(offset..offset.checked_add(size)?)?);
        }
        if out.len() > length {
            return None;
        }
    }
    (out.len() == length).then_some(())
}

/// One-shot delta, accepted only below half the target size.
#[cfg(test)]
pub fn encode(base: &[u8], target: &[u8]) -> Option<Vec<u8>> {
    encode_indexed(&Index::new(base)?, base, target, target.len() / 2)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn number(input: &mut &[u8]) -> usize {
        let mut n = 0;
        let mut shift = 0;
        loop {
            let byte = input[0];
            *input = &input[1..];
            n |= usize::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return n;
            }
            shift += 7;
        }
    }

    fn apply(base: &[u8], mut delta: &[u8]) -> Vec<u8> {
        assert_eq!(number(&mut delta), base.len());
        let length = number(&mut delta);
        let mut out = Vec::new();
        while !delta.is_empty() {
            let op = delta[0];
            delta = &delta[1..];
            assert_ne!(op, 0);
            if op & 128 == 0 {
                out.extend_from_slice(&delta[..op as usize]);
                delta = &delta[op as usize..];
            } else {
                let mut offset = 0;
                let mut size = 0;
                for n in 0..7 {
                    if op & (1 << n) != 0 {
                        let value = usize::from(delta[0]);
                        delta = &delta[1..];
                        if n < 4 {
                            offset |= value << (8 * n);
                        } else {
                            size |= value << (8 * (n - 4));
                        }
                    }
                }
                if size == 0 {
                    size = 65536;
                }
                out.extend_from_slice(&base[offset..offset + size]);
            }
        }
        assert_eq!(out.len(), length);
        out
    }

    #[test]
    fn shifted_blocks_literals_and_copy_sizes() {
        let mut state = 123_u32;
        let base: Vec<_> = (0..200_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect();
        for count in [16, 127, 128, 65535, 65536, 65537, 180000] {
            let mut target = b"inserted prefix".to_vec();
            target.extend_from_slice(&base[32..32 + count]);
            target.extend_from_slice(b"tail");
            // Short copies may not be worth a delta; any delta made must apply.
            let delta = encode(&base, &target);
            assert!(count < 127 || delta.is_some(), "no delta for {count} copied bytes");
            if let Some(delta) = delta {
                assert_eq!(apply(&base, &delta), target);
            }
        }
        let mut instructions = Vec::new();
        size(&mut instructions, base.len());
        size(&mut instructions, 65536);
        copy(&mut instructions, 0, 65536);
        assert_eq!(apply(&base, &instructions), base[..65536]);
        assert!(encode(b"tiny", b"different").is_none());
        assert!(encode(&base, &vec![0; 200000]).is_none());
        let mut changed = base.clone();
        changed.splice(1000..1011, vec![1; 301]);
        assert_eq!(apply(&base, &encode(&base, &changed).unwrap()), changed);
        // The checked decoder agrees with this reference and rejects
        // truncated or mismatched instructions.
        let delta = encode(&base, &changed).unwrap();
        let mut out = Vec::new();
        assert_eq!(super::apply(&base, &delta, &mut out), Some(()));
        assert_eq!(out, changed);
        assert_eq!(super::apply(&base[1..], &delta, &mut out), None);
        for cut in [1, delta.len() / 2, delta.len() - 1] {
            assert_eq!(super::apply(&base, &delta[..cut], &mut out), None);
        }
        assert_eq!(super::apply(&base, &instructions, &mut out), Some(()));
        assert_eq!(out, base[..65536]);
    }
}
