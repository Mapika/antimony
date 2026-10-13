use crate::{Error, ObjectId, Result};

/// `Name <email> <unix-seconds> <+hhmm>`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Signature {
    pub name: String,
    pub email: String,
    pub time: i64,
    /// Offset from UTC in minutes, e.g. +0130 → 90.
    pub tz_offset_minutes: i32,
}

impl Signature {
    fn encode(&self) -> String {
        let sign = if self.tz_offset_minutes < 0 { '-' } else { '+' };
        let abs = self.tz_offset_minutes.unsigned_abs();
        format!(
            "{} <{}> {} {}{:02}{:02}",
            self.name,
            self.email,
            self.time,
            sign,
            abs / 60,
            abs % 60
        )
    }

    fn decode(s: &str) -> Result<Self> {
        let bad = || Error::Malformed(format!("bad signature {s:?}"));
        let lt = s.find('<').ok_or_else(bad)?;
        let gt = s.rfind('>').ok_or_else(bad)?;
        if gt < lt {
            return Err(bad());
        }
        let name = s[..lt].trim_end().to_string();
        let email = s[lt + 1..gt].to_string();
        let mut rest = s[gt + 1..].split_whitespace();
        let time: i64 = rest.next().and_then(|t| t.parse().ok()).ok_or_else(bad)?;
        let tz = rest.next().ok_or_else(bad)?;
        if tz.len() != 5 || !matches!(&tz[..1], "+" | "-") {
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
    /// verbatim so decode→encode is byte-identical. Multi-line values are
    /// stored with the continuation-line leading spaces removed.
    pub extra_headers: Vec<(String, String)>,
    pub message: String,
}

impl Commit {
    pub fn encode(&self) -> Vec<u8> {
        let mut s = String::new();
        s.push_str(&format!("tree {}\n", self.tree));
        for p in &self.parents {
            s.push_str(&format!("parent {p}\n"));
        }
        s.push_str(&format!("author {}\n", self.author.encode()));
        s.push_str(&format!("committer {}\n", self.committer.encode()));
        for (k, v) in &self.extra_headers {
            s.push_str(k);
            s.push(' ');
            s.push_str(&v.replace('\n', "\n "));
            s.push('\n');
        }
        s.push('\n');
        s.push_str(&self.message);
        s.into_bytes()
    }

    pub fn decode(body: &[u8]) -> Result<Self> {
        let text = std::str::from_utf8(body)
            .map_err(|_| Error::Malformed("commit: non-UTF-8".into()))?;
        let (headers, message) = text
            .split_once("\n\n")
            .ok_or_else(|| Error::Malformed("commit: missing header terminator".into()))?;

        // Fold continuation lines (leading space) into the previous header.
        let mut fields: Vec<(String, String)> = Vec::new();
        for line in headers.split('\n') {
            if let Some(cont) = line.strip_prefix(' ') {
                let last = fields
                    .last_mut()
                    .ok_or_else(|| Error::Malformed("commit: orphan continuation".into()))?;
                last.1.push('\n');
                last.1.push_str(cont);
            } else {
                let (k, v) = line.split_once(' ').unwrap_or((line, ""));
                fields.push((k.to_string(), v.to_string()));
            }
        }

        let mut it = fields.into_iter().peekable();
        let tree = match it.next() {
            Some((k, v)) if k == "tree" => v.parse()?,
            _ => return Err(Error::Malformed("commit: missing tree".into())),
        };
        let mut parents = Vec::new();
        while let Some((k, _)) = it.peek() {
            if k != "parent" {
                break;
            }
            parents.push(it.next().unwrap().1.parse()?);
        }
        let author = match it.next() {
            Some((k, v)) if k == "author" => Signature::decode(&v)?,
            _ => return Err(Error::Malformed("commit: missing author".into())),
        };
        let committer = match it.next() {
            Some((k, v)) if k == "committer" => Signature::decode(&v)?,
            _ => return Err(Error::Malformed("commit: missing committer".into())),
        };
        Ok(Self {
            tree,
            parents,
            author,
            committer,
            extra_headers: it.collect(),
            message: message.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig() -> Signature {
        Signature { name: "Ada L".into(), email: "ada@x.org".into(), time: 1700000000, tz_offset_minutes: -330 }
    }

    #[test]
    fn roundtrip_byte_identical() {
        let c = Commit {
            tree: ObjectId([1; 32]),
            parents: vec![ObjectId([2; 32]), ObjectId([3; 32])],
            author: sig(),
            committer: sig(),
            extra_headers: vec![("gpgsig".into(), "-----BEGIN-----\nabc\n-----END-----".into())],
            message: "subject\n\nbody\n".into(),
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
            message: "init\n".into(),
        };
        assert_eq!(Commit::decode(&c.encode()).unwrap(), c);
    }
}
