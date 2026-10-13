//! Git delta instructions. A fixed-size block index bounds both memory and
//! collision work, including on repetitive or adversarial input.
const BLOCK: usize = 16;
const SLOTS: usize = 1 << 16;

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

/// Return a delta only when its uncompressed instructions are below half the
/// target size. Caller limits base/target size and depth before attempting it.
pub fn encode(base: &[u8], target: &[u8]) -> Option<Vec<u8>> {
    if base.len() < BLOCK || target.len() < BLOCK || base.len() > u32::MAX as usize {
        return None;
    }
    let mut table = vec![u32::MAX; SLOTS];
    for pos in (0..=base.len() - BLOCK).step_by(BLOCK) {
        table[hash(&base[pos..pos + BLOCK]) as usize & (SLOTS - 1)] = pos as u32;
    }
    let mut out = Vec::new();
    size(&mut out, base.len());
    size(&mut out, target.len());
    let mut pos = 0;
    let mut literal = 0;
    let power = 257_u64.wrapping_pow((BLOCK - 1) as u32);
    let mut rolling = hash(&target[..BLOCK]);
    while pos + BLOCK <= target.len() {
        let candidate = table[rolling as usize & (SLOTS - 1)];
        let offset = candidate as usize;
        if candidate != u32::MAX && base[offset..offset + BLOCK] == target[pos..pos + BLOCK] {
            insert(&mut out, &target[literal..pos]);
            let mut length = BLOCK;
            while offset + length < base.len()
                && pos + length < target.len()
                && base[offset + length] == target[pos + length]
            {
                length += 1;
            }
            copy(&mut out, offset, length);
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
        if out.len() + pos - literal >= target.len() / 2 {
            return None;
        }
    }
    insert(&mut out, &target[literal..]);
    (out.len() < target.len() / 2).then_some(out)
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
            if count >= 127 {
                assert_eq!(apply(&base, &encode(&base, &target).unwrap()), target);
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
    }
}
