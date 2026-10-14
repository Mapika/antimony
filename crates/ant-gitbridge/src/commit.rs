//! Raw commit parsing (names, emails and messages stay raw bytes).

use ant_core::Signature;
use ant_core::commit::header_fields;
use anyhow::{anyhow, bail};

/// A source object id (SHA-1 or SHA-256 git).
pub(crate) type Oid = gix::ObjectId;

/// A commit parsed from git's raw encoding.
pub(crate) struct RawCommit {
    pub tree: Oid,
    pub parents: Vec<Oid>,
    pub author: Signature,
    pub committer: Signature,
    pub extra_headers: Vec<(String, Vec<u8>)>,
    pub message: Vec<u8>,
}

pub(crate) fn parse_commit(id: gix::ObjectId, raw: &[u8]) -> anyhow::Result<RawCommit> {
    let bad = |what: &str| anyhow!("commit {id}: {what}");
    let (head, message) = match raw.windows(2).position(|w| w == b"\n\n") {
        Some(i) => (&raw[..i], &raw[i + 2..]),
        None => (raw, &raw[raw.len()..]),
    };

    let fields = header_fields(head).map_err(|e| anyhow!("commit {id}: {e}"))?;
    let hex_id = |v: &[u8], what: &str| -> anyhow::Result<gix::ObjectId> {
        gix::ObjectId::from_hex(v).map_err(|_| bad(what))
    };
    let mut it = fields.into_iter().peekable();
    let tree = match it.next() {
        Some((b"tree", v)) => hex_id(&v, "bad tree id")?,
        _ => return Err(bad("missing tree")),
    };
    let mut parents = Vec::new();
    while matches!(it.peek(), Some((b"parent", _))) {
        if let Some((_, v)) = it.next() {
            parents.push(hex_id(&v, "bad parent id")?);
        }
    }
    let author = match it.next() {
        Some((b"author", v)) => parse_signature(id, &v)?,
        _ => return Err(bad("missing author")),
    };
    let committer = match it.next() {
        Some((b"committer", v)) => parse_signature(id, &v)?,
        _ => return Err(bad("missing committer")),
    };
    let extra_headers = it
        .map(|(k, v)| {
            let k = std::str::from_utf8(k).map_err(|_| bad("non-UTF-8 header name"))?;
            Ok((k.to_string(), v))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RawCommit { tree, parents, author, committer, extra_headers, message: message.to_vec() })
}

/// `Name <email> <unix-seconds> <+hhmm>`, same rules as `Signature` decoding in
/// ant-core, but name and email stay raw bytes.
fn parse_signature(id: gix::ObjectId, s: &[u8]) -> anyhow::Result<Signature> {
    let bad = || anyhow!("commit {id}: bad signature {:?}", String::from_utf8_lossy(s));
    let lt = s.iter().position(|&b| b == b'<').ok_or_else(bad)?;
    let gt = s.iter().rposition(|&b| b == b'>').ok_or_else(bad)?;
    if gt < lt {
        bail!(bad());
    }
    let mut name_end = lt;
    while name_end > 0 && s[name_end - 1] == b' ' {
        name_end -= 1;
    }
    let name = s[..name_end].to_vec();
    let email = s[lt + 1..gt].to_vec();
    let tail = std::str::from_utf8(&s[gt + 1..]).map_err(|_| bad())?;
    let mut rest = tail.split_whitespace();
    let time: i64 = rest.next().and_then(|t| t.parse().ok()).ok_or_else(bad)?;
    let tz = rest.next().ok_or_else(bad)?;
    if tz.len() != 5 || !tz.is_ascii() || !matches!(&tz[..1], "+" | "-") {
        bail!(bad());
    }
    let hh: i32 = tz[1..3].parse().map_err(|_| bad())?;
    let mm: i32 = tz[3..5].parse().map_err(|_| bad())?;
    let off = if &tz[..1] == "-" { -(hh * 60 + mm) } else { hh * 60 + mm };
    Ok(Signature { name, email, time, tz_offset_minutes: off })
}
