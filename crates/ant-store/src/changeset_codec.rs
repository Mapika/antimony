//! Compact binary encoding of one commit's changeset (a `Vec<PathChange>`),
//! used by the Postgres store to keep a changeset in a single row.
//!
//! Layout: a flags byte (bit 0: the rest is zstd-compressed), then the body:
//! `varint count`, and per change `kind u8`, `old mode code u8` (0 = absent),
//! `new mode code u8`, `varint path length`, the path bytes, and the 32-byte
//! ids of the present sides (old first).

use ant_core::{EntryMode, ObjectId, RepoPath};

use crate::{ChangeKind, PathChange, Result, StoreError};

/// Bodies at least this long are zstd-compressed.
const COMPRESS_FROM: usize = 512;
const ZSTD_LEVEL: i32 = 3;

fn corrupt(what: &str) -> StoreError {
    StoreError::InvalidArgument(format!("corrupt changeset: {what}"))
}

fn kind_code(k: ChangeKind) -> u8 {
    match k {
        ChangeKind::Added => 0,
        ChangeKind::Modified => 1,
        ChangeKind::Deleted => 2,
    }
}

fn kind_from(c: u8) -> Result<ChangeKind> {
    Ok(match c {
        0 => ChangeKind::Added,
        1 => ChangeKind::Modified,
        2 => ChangeKind::Deleted,
        _ => return Err(corrupt("bad kind")),
    })
}

fn mode_code(m: Option<EntryMode>) -> u8 {
    // Recorded changes carry semantic classes; `class()` never yields Legacy.
    match m.map(EntryMode::class) {
        None => 0,
        Some(EntryMode::File | EntryMode::Legacy(_)) => 1,
        Some(EntryMode::Executable) => 2,
        Some(EntryMode::Symlink) => 3,
        Some(EntryMode::Tree) => 4,
        Some(EntryMode::Gitlink) => 5,
    }
}

fn mode_from(c: u8) -> Result<Option<EntryMode>> {
    Ok(match c {
        0 => None,
        1 => Some(EntryMode::File),
        2 => Some(EntryMode::Executable),
        3 => Some(EntryMode::Symlink),
        4 => Some(EntryMode::Tree),
        5 => Some(EntryMode::Gitlink),
        _ => return Err(corrupt("bad mode")),
    })
}

fn put_varint(out: &mut Vec<u8>, mut v: usize) {
    while v >= 0x80 {
        out.push((v & 0x7f) as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(buf: &mut &[u8]) -> Result<usize> {
    let mut v = 0usize;
    for shift in (0..64).step_by(7) {
        let (&b, rest) = buf.split_first().ok_or_else(|| corrupt("truncated varint"))?;
        *buf = rest;
        v |= usize::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(v);
        }
    }
    Err(corrupt("varint too long"))
}

pub(crate) fn encode(changes: &[PathChange]) -> Result<Vec<u8>> {
    let mut body = Vec::with_capacity(8 + changes.len() * 96);
    put_varint(&mut body, changes.len());
    for c in changes {
        body.push(kind_code(c.kind));
        body.push(mode_code(c.old.map(|(m, _)| m)));
        body.push(mode_code(c.new.map(|(m, _)| m)));
        put_varint(&mut body, c.path.as_str().len());
        body.extend_from_slice(c.path.as_str().as_bytes());
        for (_, id) in c.old.iter().chain(c.new.iter()) {
            body.extend_from_slice(id.as_bytes());
        }
    }
    let mut out = Vec::with_capacity(body.len() / 2 + 1);
    if body.len() >= COMPRESS_FROM {
        out.push(1);
        out.extend_from_slice(
            &zstd::encode_all(&body[..], ZSTD_LEVEL).map_err(StoreError::backend)?,
        );
    } else {
        out.push(0);
        out.extend_from_slice(&body);
    }
    Ok(out)
}

pub(crate) fn decode(data: &[u8]) -> Result<Vec<PathChange>> {
    let (&flags, rest) = data.split_first().ok_or_else(|| corrupt("empty"))?;
    let decompressed;
    let mut buf: &[u8] = match flags {
        0 => rest,
        1 => {
            decompressed = zstd::decode_all(rest).map_err(StoreError::backend)?;
            &decompressed
        }
        _ => return Err(corrupt("bad flags")),
    };
    let n = get_varint(&mut buf)?;
    let mut out = Vec::with_capacity(n.min(1 << 16));
    for _ in 0..n {
        let [kind, old, new, ..] = buf else { return Err(corrupt("truncated change")) };
        let (kind, old, new) = (kind_from(*kind)?, mode_from(*old)?, mode_from(*new)?);
        buf = &buf[3..];
        let len = get_varint(&mut buf)?;
        if buf.len() < len {
            return Err(corrupt("truncated path"));
        }
        let path = std::str::from_utf8(&buf[..len]).map_err(|_| corrupt("non-UTF-8 path"))?;
        let path = RepoPath::new(path)?;
        buf = &buf[len..];
        let mut side = |mode: Option<EntryMode>| -> Result<Option<(EntryMode, ObjectId)>> {
            let Some(mode) = mode else { return Ok(None) };
            if buf.len() < ObjectId::LEN {
                return Err(corrupt("truncated id"));
            }
            let id = ObjectId::from_bytes(&buf[..ObjectId::LEN])?;
            buf = &buf[ObjectId::LEN..];
            Ok(Some((mode, id)))
        };
        let old = side(old)?;
        let new = side(new)?;
        out.push(PathChange { path, kind, old, new });
    }
    if !buf.is_empty() {
        return Err(corrupt("trailing bytes"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(
        path: &str,
        kind: ChangeKind,
        old: Option<EntryMode>,
        new: Option<EntryMode>,
    ) -> PathChange {
        PathChange {
            path: RepoPath::new(path).unwrap(),
            kind,
            old: old.map(|m| (m, ObjectId([1; 32]))),
            new: new.map(|m| (m, ObjectId([2; 32]))),
        }
    }

    #[test]
    fn roundtrips_small_and_large_changesets() {
        let empty: Vec<PathChange> = Vec::new();
        assert_eq!(decode(&encode(&empty).unwrap()).unwrap(), empty);
        let few = vec![
            change("a", ChangeKind::Added, None, Some(EntryMode::File)),
            change("b/c", ChangeKind::Deleted, Some(EntryMode::Symlink), None),
            change("é/ü", ChangeKind::Modified, Some(EntryMode::File), Some(EntryMode::Executable)),
            change("sub", ChangeKind::Modified, Some(EntryMode::Gitlink), Some(EntryMode::Gitlink)),
        ];
        assert_eq!(decode(&encode(&few).unwrap()).unwrap(), few);
        let many: Vec<PathChange> = (0..2000)
            .map(|i| {
                change(
                    &format!("dir/{i}/file.c"),
                    ChangeKind::Modified,
                    Some(EntryMode::File),
                    Some(EntryMode::File),
                )
            })
            .collect();
        let enc = encode(&many).unwrap();
        assert_eq!(enc[0], 1, "large bodies are compressed");
        assert_eq!(decode(&enc).unwrap(), many);
    }

    #[test]
    fn rejects_corrupt_input() {
        let enc = encode(&[change("a", ChangeKind::Added, None, Some(EntryMode::File))]).unwrap();
        for cut in 0..enc.len() {
            assert!(decode(&enc[..cut]).is_err(), "prefix of {cut} bytes");
        }
        let mut extra = enc.clone();
        extra.push(0);
        assert!(decode(&extra).is_err());
        assert!(decode(&[9, 0]).is_err());
    }
}
