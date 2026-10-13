use crate::{Error, ObjectId, Result};

/// `Name <email> <unix-seconds> <+hhmm>`
///
/// Name and email are raw bytes: the object format does not require UTF-8
/// (latin1 authors exist in real histories) and they must roundtrip
/// byte-identically.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Signature {
    pub name: Vec<u8>,
    pub email: Vec<u8>,
    pub time: i64,
    /// Offset from UTC in minutes, e.g. +0130 → 90.
    pub tz_offset_minutes: i32,
}

impl Signature {
    fn encode_into(&self, out: &mut Vec<u8>) {
        let sign = if self.tz_offset_minutes < 0 { '-' } else { '+' };
        let abs = self.tz_offset_minutes.unsigned_abs();
        out.extend_from_slice(&self.name);
        out.extend_from_slice(b" <");
        out.extend_from_slice(&self.email);
        out.extend_from_slice(format!("> {} {}{:02}{:02}", self.time, sign, abs / 60, abs % 60).as_bytes());
    }

    fn decode(s: &[u8]) -> Result<Self> {
        let bad = || Error::Malformed(format!("bad signature {:?}", String::from_utf8_lossy(s)));
        let lt = s.iter().position(|&b| b == b'<').ok_or_else(bad)?;
        let gt = s.iter().rposition(|&b| b == b'>').ok_or_else(bad)?;
        if gt < lt {
            return Err(bad());
        }
        // `encode` writes "<name> <<email>>": strip exactly that one separator so
        // names keep any whitespace of their own (hash fidelity on import).
        let name = s[..lt].strip_suffix(b" ").unwrap_or(&s[..lt]).to_vec();
        let email = s[lt + 1..gt].to_vec();
        let tail = std::str::from_utf8(&s[gt + 1..]).map_err(|_| bad())?;
        let mut rest = tail.split_whitespace();
        let time: i64 = rest.next().and_then(|t| t.parse().ok()).ok_or_else(bad)?;
        let tz = rest.next().ok_or_else(bad)?;
        // ASCII check first: the slicing below is by byte index.
        if tz.len() != 5 || !tz.is_ascii() || !matches!(&tz[..1], "+" | "-") {
            return Err(bad());
        }
        let hh: i32 = tz[1..3].parse().map_err(|_| bad())?;
        let mm: i32 = tz[3..5].parse().map_err(|_| bad())?;
        let mut off = hh * 60 + mm;
        if &tz[..1] == "-" {
            off = -off;
        }
        Ok(Self { name, email, time, tz_offset_minutes: off })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub tree: ObjectId,
    pub parents: Vec<ObjectId>,
    pub author: Signature,
    pub committer: Signature,
    /// Headers after `committer` (e.g. `encoding`, `gpgsig`, `mergetag`), kept
    /// verbatim so decode→encode is byte-identical. Keys are ASCII; values are
    /// raw bytes. Multi-line values are stored with the continuation-line
    /// leading spaces removed.
    pub extra_headers: Vec<(String, Vec<u8>)>,
    /// Raw bytes; the format does not require UTF-8 messages.
    pub message: Vec<u8>,
}

impl Commit {
    pub fn encode(&self) -> Vec<u8> {
        let mut s = Vec::with_capacity(256 + self.message.len());
        s.extend_from_slice(format!("tree {}\n", self.tree).as_bytes());
        for p in &self.parents {
            s.extend_from_slice(format!("parent {p}\n").as_bytes());
        }
        s.extend_from_slice(b"author ");
        self.author.encode_into(&mut s);
        s.extend_from_slice(b"\ncommitter ");
        self.committer.encode_into(&mut s);
        s.push(b'\n');
        for (k, v) in &self.extra_headers {
            s.extend_from_slice(k.as_bytes());
            s.push(b' ');
            for &b in v {
                s.push(b);
                if b == b'\n' {
                    s.push(b' ');
                }
            }
            s.push(b'\n');
        }
        s.push(b'\n');
        s.extend_from_slice(&self.message);
        s
    }

    pub fn decode(body: &[u8]) -> Result<Self> {
        let split = body
            .windows(2)
            .position(|w| w == b"\n\n")
            .ok_or_else(|| Error::Malformed("commit: missing header terminator".into()))?;
        let (headers, message) = (&body[..split], &body[split + 2..]);

        // Fold continuation lines (leading space) into the previous header.
        let mut fields: Vec<(&[u8], Vec<u8>)> = Vec::new();
        for line in headers.split(|&b| b == b'\n') {
            if let Some(cont) = line.strip_prefix(b" ") {
                let last = fields
                    .last_mut()
                    .ok_or_else(|| Error::Malformed("commit: orphan continuation".into()))?;
                last.1.push(b'\n');
                last.1.extend_from_slice(cont);
            } else {
                let (k, v) = match line.iter().position(|&b| b == b' ') {
                    Some(i) => (&line[..i], &line[i + 1..]),
                    None => (line, &b""[..]),
                };
                fields.push((k, v.to_vec()));
            }
        }

        let mut it = fields.into_iter().peekable();
        let id_of = |v: &[u8]| -> Result<ObjectId> {
            std::str::from_utf8(v).map_err(|_| Error::Malformed("commit: bad object id".into()))?.parse()
        };
        let tree = match it.next() {
            Some((b"tree", v)) => id_of(&v)?,
            _ => return Err(Error::Malformed("commit: missing tree".into())),
        };
        let mut parents = Vec::new();
        while let Some((b"parent", _)) = it.peek() {
            if let Some((_, v)) = it.next() {
                parents.push(id_of(&v)?);
            }
        }
        let author = match it.next() {
            Some((b"author", v)) => Signature::decode(&v)?,
            _ => return Err(Error::Malformed("commit: missing author".into())),
        };
        let committer = match it.next() {
            Some((b"committer", v)) => Signature::decode(&v)?,
            _ => return Err(Error::Malformed("commit: missing committer".into())),
        };
        let extra_headers = it
            .map(|(k, v)| {
                let k = std::str::from_utf8(k)
                    .map_err(|_| Error::Malformed("commit: non-UTF-8 header key".into()))?;
                Ok((k.to_string(), v))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { tree, parents, author, committer, extra_headers, message: message.to_vec() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig() -> Signature {
        Signature { name: b"Ada L".to_vec(), email: b"ada@x.org".to_vec(), time: 1700000000, tz_offset_minutes: -330 }
    }

    #[test]
    fn roundtrip_byte_identical() {
        let c = Commit {
            tree: ObjectId([1; 32]),
            parents: vec![ObjectId([2; 32]), ObjectId([3; 32])],
            author: sig(),
            committer: sig(),
            extra_headers: vec![("gpgsig".into(), b"-----BEGIN-----\nabc\n-----END-----".to_vec())],
            message: b"subject\n\nbody\n".to_vec(),
        };
        let enc = c.encode();
        let text = String::from_utf8(enc.clone()).unwrap();
        assert!(text.contains("author Ada L <ada@x.org> 1700000000 -0530\n"));
        assert!(text.contains("gpgsig -----BEGIN-----\n abc\n -----END-----\n"));
        let dec = Commit::decode(&enc).unwrap();
        assert_eq!(dec, c);
        assert_eq!(dec.encode(), enc);
    }

    #[test]
    fn root_commit() {
        let c = Commit {
            tree: ObjectId::empty_tree(),
            parents: vec![],
            author: sig(),
            committer: sig(),
            extra_headers: vec![],
            message: b"init\n".to_vec(),
        };
        assert_eq!(Commit::decode(&c.encode()).unwrap(), c);
    }

    #[test]
    fn non_utf8_roundtrip() {
        let mut s = sig();
        s.name = vec![b'J', 0xf6, b'r', 0xff, b'g'];
        s.email = vec![0xe9, b'@', b'x'];
        let c = Commit {
            tree: ObjectId::empty_tree(),
            parents: vec![],
            author: s.clone(),
            committer: s,
            extra_headers: vec![("encoding".into(), b"ISO-8859-1".to_vec()), ("x".into(), vec![0xff, b'\n', 0xfe])],
            message: vec![b'm', 0xe4, 0xc0, b'\n'],
        };
        let enc = c.encode();
        let dec = Commit::decode(&enc).unwrap();
        assert_eq!(dec, c);
        assert_eq!(dec.encode(), enc);
    }
}
