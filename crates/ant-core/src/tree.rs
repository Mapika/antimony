use std::borrow::Cow;
use std::cmp::Ordering;

use crate::{Error, ObjectId, Result};

/// A tree entry's mode.
///
/// The five canonical variants are the only modes antimony ever *creates*.
/// [`EntryMode::Legacy`] exists for trees imported from old git histories that
/// carry other spellings (`100664`, `100640`, zero-padded `040000`, `40755`,
/// ...). git's own SHA-1 to SHA-256 conversion copies a tree entry's mode
/// text verbatim (`convert_tree_object` in `object-file-convert.c`; its
/// `parse_mode` accepts any string of octal digits), so ids only match git's
/// conversion if the exact spelling is kept.
///
/// Code that decides behaviour (checkout, diff, merge, export filters) must
/// use [`EntryMode::class`] rather than matching variants directly: a legacy
/// mode behaves like its semantic class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryMode {
    File,
    Executable,
    Symlink,
    Tree,
    /// Submodule commit pointer. Carried through, never fetched.
    Gitlink,
    /// A non-canonical spelling that git tolerates, preserved exactly.
    Legacy(LegacyMode),
}

/// A non-canonical mode spelling: `pad` leading `'0'` characters followed by
/// the minimal octal digits of `value`. Every digit string of 1 to
/// [`LegacyMode::MAX_DIGITS`] octal digits other than the five canonical
/// spellings has exactly one such form, so the string and the value
/// correspond one-to-one and tree encoding stays injective. Only
/// [`EntryMode::from_octal`] constructs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LegacyMode {
    value: u32,
    pad: u8,
}

impl LegacyMode {
    /// Longest accepted spelling. Real modes have at most 6 digits; the slack
    /// covers zero-padding.
    pub const MAX_DIGITS: usize = 10;

    /// The numeric value (e.g. `0o100664`).
    pub fn value(self) -> u32 {
        self.value
    }
}

impl EntryMode {
    /// Octal spelling as stored in a tree object.
    pub fn as_octal(self) -> Cow<'static, str> {
        Cow::Borrowed(match self {
            EntryMode::File => "100644",
            EntryMode::Executable => "100755",
            EntryMode::Symlink => "120000",
            EntryMode::Tree => "40000",
            EntryMode::Gitlink => "160000",
            EntryMode::Legacy(m) => {
                return Cow::Owned(format!("{}{:o}", "0".repeat(usize::from(m.pad)), m.value));
            }
        })
    }

    /// Parse a mode spelling: any 1 to [`LegacyMode::MAX_DIGITS`] octal digits,
    /// like git's tree parser. The exact spelling is kept, so distinct byte
    /// strings give distinct modes.
    pub fn from_octal(s: &[u8]) -> Result<Self> {
        match s {
            b"100644" => return Ok(EntryMode::File),
            b"100755" => return Ok(EntryMode::Executable),
            b"120000" => return Ok(EntryMode::Symlink),
            b"40000" => return Ok(EntryMode::Tree),
            b"160000" => return Ok(EntryMode::Gitlink),
            _ => {}
        }
        if s.is_empty()
            || s.len() > LegacyMode::MAX_DIGITS
            || !s.iter().all(|b| (b'0'..=b'7').contains(b))
        {
            return Err(Error::Malformed(format!(
                "bad tree entry mode {:?}",
                String::from_utf8_lossy(s)
            )));
        }
        let zeros = s.iter().take_while(|&&b| b == b'0').count();
        // An all-zero string is the value 0 spelled with its last `0`.
        let pad = zeros.min(s.len() - 1);
        let value = s[pad..].iter().fold(0u32, |a, b| a * 8 + u32::from(b - b'0'));
        Ok(EntryMode::Legacy(LegacyMode { value, pad: pad as u8 }))
    }

    /// The canonical mode with the same behaviour. Identity for canonical
    /// modes. Legacy modes follow git's `canon_mode` on the 16-bit value:
    /// regular file (executable if the owner-execute bit is set), symlink,
    /// directory, and anything else is a gitlink.
    pub fn class(self) -> EntryMode {
        match self {
            EntryMode::Legacy(m) => {
                let v = m.value as u16;
                match v & 0o170000 {
                    0o100000 if v & 0o100 != 0 => EntryMode::Executable,
                    0o100000 => EntryMode::File,
                    0o120000 => EntryMode::Symlink,
                    0o040000 => EntryMode::Tree,
                    _ => EntryMode::Gitlink,
                }
            }
            m => m,
        }
    }

    pub fn is_legacy(self) -> bool {
        matches!(self, EntryMode::Legacy(_))
    }

    pub fn is_tree(self) -> bool {
        self.class() == EntryMode::Tree
    }

    /// Entries whose id points at a blob.
    pub fn is_blob(self) -> bool {
        matches!(self.class(), EntryMode::File | EntryMode::Executable | EntryMode::Symlink)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TreeEntry {
    pub mode: EntryMode,
    pub name: String,
    pub id: ObjectId,
}

/// git's tree ordering: names compare bytewise, with tree names treated as if
/// suffixed by `/`.
fn git_cmp(a: &TreeEntry, b: &TreeEntry) -> Ordering {
    let ak = a.name.as_bytes().iter().copied().chain(a.mode.is_tree().then_some(b'/'));
    let bk = b.name.as_bytes().iter().copied().chain(b.mode.is_tree().then_some(b'/'));
    ak.cmp(bk)
}

/// A directory listing. Entries are always kept in git order with unique names.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Tree {
    entries: Vec<TreeEntry>,
}

impl Tree {
    /// Build a tree from entries in any order. Fails on duplicate or invalid names.
    pub fn new(mut entries: Vec<TreeEntry>) -> Result<Self> {
        for e in &entries {
            if e.name.is_empty() || e.name == "." || e.name == ".." || e.name.contains(['/', '\0'])
            {
                return Err(Error::Malformed(format!("bad tree entry name {:?}", e.name)));
            }
        }
        entries.sort_by(git_cmp);
        let mut names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        names.sort_unstable();
        if names.windows(2).any(|w| w[0] == w[1]) {
            return Err(Error::Malformed("duplicate tree entry name".into()));
        }
        Ok(Self { entries })
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn entries(&self) -> &[TreeEntry] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, name: &str) -> Option<&TreeEntry> {
        self.entries.iter().find(|e| e.name == name)
    }

    /// Insert or replace an entry by name.
    pub fn upsert(&mut self, entry: TreeEntry) -> Result<()> {
        let mut entries = std::mem::take(&mut self.entries);
        entries.retain(|e| e.name != entry.name);
        entries.push(entry);
        *self = Tree::new(entries)?;
        Ok(())
    }

    /// Remove an entry by name, returning it if present.
    pub fn remove(&mut self, name: &str) -> Option<TreeEntry> {
        let idx = self.entries.iter().position(|e| e.name == name)?;
        Some(self.entries.remove(idx))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.entries.len() * 64);
        for e in &self.entries {
            out.extend_from_slice(e.mode.as_octal().as_bytes());
            out.push(b' ');
            out.extend_from_slice(e.name.as_bytes());
            out.push(0);
            out.extend_from_slice(e.id.as_bytes());
        }
        out
    }

    pub fn decode(mut body: &[u8]) -> Result<Self> {
        let mut entries = Vec::new();
        while !body.is_empty() {
            let sp = body
                .iter()
                .position(|&b| b == b' ')
                .ok_or_else(|| Error::Malformed("tree entry: missing space".into()))?;
            let mode = EntryMode::from_octal(&body[..sp])?;
            body = &body[sp + 1..];
            let nul = body
                .iter()
                .position(|&b| b == 0)
                .ok_or_else(|| Error::Malformed("tree entry: missing NUL".into()))?;
            let name = std::str::from_utf8(&body[..nul])
                .map_err(|_| Error::Malformed("tree entry: non-UTF-8 name".into()))?
                .to_string();
            body = &body[nul + 1..];
            if body.len() < ObjectId::LEN {
                return Err(Error::Malformed("tree entry: truncated id".into()));
            }
            let id = ObjectId::from_bytes(&body[..ObjectId::LEN])?;
            body = &body[ObjectId::LEN..];
            entries.push(TreeEntry { mode, name, id });
        }
        // Canonical form only: strictly increasing git order (which also rules
        // out duplicates). Re-sorting here would make ids malleable.
        if entries.windows(2).any(|w| git_cmp(&w[0], &w[1]) != Ordering::Less) {
            return Err(Error::Malformed("tree entries not in canonical order".into()));
        }
        Tree::new(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EncodedObject, ObjectKind};

    fn e(mode: EntryMode, name: &str, b: u8) -> TreeEntry {
        TreeEntry { mode, name: name.into(), id: ObjectId([b; 32]) }
    }

    #[test]
    fn git_ordering_treats_trees_as_slash_suffixed() {
        // "foo.c" < "foo/" because '.' (0x2e) < '/' (0x2f); "foo-" < "foo.c".
        let t = Tree::new(vec![
            e(EntryMode::Tree, "foo", 1),
            e(EntryMode::File, "foo.c", 2),
            e(EntryMode::File, "foo-", 3),
        ])
        .unwrap();
        let names: Vec<_> = t.entries().iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["foo-", "foo.c", "foo"]);
    }

    #[test]
    fn rejects_duplicates_and_bad_names() {
        assert!(Tree::new(vec![e(EntryMode::File, "a", 1), e(EntryMode::Tree, "a", 2)]).is_err());
        assert!(Tree::new(vec![e(EntryMode::File, "a/b", 1)]).is_err());
        assert!(Tree::new(vec![e(EntryMode::File, "", 1)]).is_err());
    }

    #[test]
    fn encode_decode_roundtrip() {
        let t = Tree::new(vec![
            e(EntryMode::File, "README", 1),
            e(EntryMode::Executable, "run.sh", 2),
            e(EntryMode::Symlink, "link", 3),
            e(EntryMode::Tree, "src", 4),
            e(EntryMode::Gitlink, "vendor", 5),
        ])
        .unwrap();
        assert_eq!(Tree::decode(&t.encode()).unwrap(), t);
    }

    #[test]
    fn empty_tree_id() {
        let id = EncodedObject::new(ObjectKind::Tree, &Tree::empty().encode()).id();
        assert_eq!(id, ObjectId::empty_tree());
    }

    #[test]
    fn upsert_and_remove() {
        let mut t = Tree::empty();
        t.upsert(e(EntryMode::File, "b", 1)).unwrap();
        t.upsert(e(EntryMode::File, "a", 2)).unwrap();
        t.upsert(e(EntryMode::File, "b", 3)).unwrap();
        assert_eq!(t.entries().len(), 2);
        assert_eq!(t.get("b").unwrap().id, ObjectId([3; 32]));
        assert_eq!(t.remove("a").unwrap().id, ObjectId([2; 32]));
        assert!(t.remove("a").is_none());
    }
}
