//! Raw commit handling. A commit is converted by copying its text verbatim and
//! replacing only the object ids of the `tree` and `parent` lines, which is
//! what git's own SHA-256 conversion does: names, emails, timestamps, zones,
//! extra headers and the message are never parsed into a structure and
//! re-encoded, because anything unusual in them would change the commit id.

use ant_core::{ObjectId, Signature, commit::header_fields};
use anyhow::anyhow;

/// A source object id (SHA-1 or SHA-256 git).
pub(crate) type Oid = gix::ObjectId;

/// A commit as read from the source repository.
pub(crate) struct RawCommit {
    pub tree: Oid,
    pub parents: Vec<Oid>,
    /// Committer timestamp (ordering hint only; 0 when unreadable).
    pub committer_time: i64,
    /// The commit text exactly as stored in the source repository.
    pub text: Vec<u8>,
}

pub(crate) fn parse_commit(id: gix::ObjectId, raw: &[u8]) -> anyhow::Result<RawCommit> {
    let bad = |what: &str| anyhow!("commit {id}: {what}");
    let head = &raw[..header_end(raw)];
    let fields = header_fields(head).map_err(|e| anyhow!("commit {id}: {e}"))?;
    let hex_id = |v: &[u8], what: &str| -> anyhow::Result<gix::ObjectId> {
        gix::ObjectId::from_hex(v).map_err(|_| bad(what))
    };
    let mut it = fields.iter().peekable();
    let tree = match it.next() {
        Some((b"tree", v)) => hex_id(v, "bad tree id")?,
        _ => return Err(bad("missing tree")),
    };
    let mut parents = Vec::new();
    while let Some((b"parent", v)) = it.peek() {
        parents.push(hex_id(v, "bad parent id")?);
        it.next();
    }
    // The signature lines are only read for the committer time: whatever git
    // tolerates there is copied through untouched.
    let committer_time = fields
        .iter()
        .find(|(k, _)| *k == b"committer")
        .map_or(0, |(_, v)| Signature::decode_lenient(v).time);
    Ok(RawCommit { tree, parents, committer_time, text: raw.to_vec() })
}

/// Length of the header block: up to the first blank line, or the whole text
/// when there is none.
fn header_end(raw: &[u8]) -> usize {
    raw.windows(2).position(|w| w == b"\n\n").unwrap_or(raw.len())
}

/// `raw.text` with the `tree` and `parent` ids replaced by `tree` and
/// `parents` (antimony ids); every other byte is copied as is.
pub(crate) fn rewrite_ids(
    raw: &RawCommit,
    tree: &ObjectId,
    parents: &[ObjectId],
) -> anyhow::Result<Vec<u8>> {
    if parents.len() != raw.parents.len() {
        return Err(anyhow!(
            "commit has {} parent lines but {} converted parents",
            raw.parents.len(),
            parents.len()
        ));
    }
    let text = &raw.text;
    // The id lines are the first `1 + parents` lines; each is `key SP hex LF`
    // (`parse_commit` accepted them, and the header block ends in a newline or
    // at the end of the text).
    let mut out = Vec::with_capacity(text.len() + 24 * (1 + parents.len()));
    let mut pos = 0;
    for (key, id) in std::iter::once(("tree", tree)).chain(parents.iter().map(|p| ("parent", p))) {
        let line_end = text[pos..].iter().position(|&b| b == b'\n').map_or(text.len(), |i| pos + i);
        out.extend_from_slice(format!("{key} {id}").as_bytes());
        if line_end < text.len() {
            out.push(b'\n');
        }
        pos = (line_end + 1).min(text.len());
    }
    out.extend_from_slice(&text[pos..]);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(c: char) -> gix::ObjectId {
        gix::ObjectId::from_hex(c.to_string().repeat(40).as_bytes()).unwrap()
    }

    /// Rewrites with fixed antimony ids and returns the new text.
    fn rewrite(text: &[u8], parents: usize) -> Vec<u8> {
        let raw = parse_commit(oid('a'), text).unwrap();
        assert_eq!(raw.parents.len(), parents);
        let ids: Vec<_> = (0..parents).map(|i| ObjectId([i as u8 + 2; 32])).collect();
        rewrite_ids(&raw, &ObjectId([1; 32]), &ids).unwrap()
    }

    #[test]
    fn only_ids_change() {
        let t = format!(
            "tree {}\nparent {}\nparent {}\nauthor N  <e> 1 -0000\ncommitter  <>\nx\n y\n\nm \n\n",
            "a".repeat(40),
            "b".repeat(40),
            "c".repeat(40)
        );
        let out = rewrite(t.as_bytes(), 2);
        let want = format!(
            "tree {}\nparent {}\nparent {}\nauthor N  <e> 1 -0000\ncommitter  <>\nx\n y\n\nm \n\n",
            "01".repeat(32),
            "02".repeat(32),
            "03".repeat(32)
        );
        assert_eq!(String::from_utf8(out).unwrap(), want);
    }

    #[test]
    fn headers_only_without_trailing_newline() {
        let t = format!("tree {}\ncommitter x", "a".repeat(40));
        let out = rewrite(t.as_bytes(), 0);
        assert_eq!(out, format!("tree {}\ncommitter x", "01".repeat(32)).as_bytes());
        let t = format!("tree {}", "a".repeat(40));
        assert_eq!(rewrite(t.as_bytes(), 0), format!("tree {}", "01".repeat(32)).as_bytes());
    }

    #[test]
    fn committer_time_is_best_effort() {
        let t = |sig: &str| {
            let text = format!("tree {}\nauthor {sig}\ncommitter {sig}\n\nm", "a".repeat(40));
            parse_commit(oid('a'), text.as_bytes()).unwrap().committer_time
        };
        assert_eq!(t("N <e> 1700000000 +0100"), 1700000000);
        assert_eq!(t("N  <e>   17   +05"), 17);
        assert_eq!(t("N <e> soon"), 0);
        assert_eq!(t("garbage"), 0);
    }

    #[test]
    fn broken_id_lines_are_rejected() {
        assert!(parse_commit(oid('a'), b"author x\n\nm").is_err());
        assert!(parse_commit(oid('a'), b"tree xyz\n\nm").is_err());
        let t = format!("tree {}\nparent nope\n\nm", "a".repeat(40));
        assert!(parse_commit(oid('a'), t.as_bytes()).is_err());
    }
}
