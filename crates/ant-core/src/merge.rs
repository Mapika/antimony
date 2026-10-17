//! Three-way merge of tree entries and file texts, shared by the server's
//! land rebase (`ant-server` `ops::land::replay`) and client workspace
//! updates (`antd::update`), so both resolve the same inputs identically.
//!
//! Naming follows the server's rebase: `base` is the common ancestor's entry,
//! `ours` the entry already on the target (the ref tip, i.e. upstream) and
//! `theirs` the entry being carried over (the replayed change, i.e. the
//! local edit).

use crate::{EntryMode, ObjectId};

/// A leaf entry at one path, `None` when the path is absent.
pub type Entry = Option<(EntryMode, ObjectId)>;

/// How the entries at one path combine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryMerge {
    /// Both sides agree (`ours == theirs`): nothing to carry over.
    Same,
    /// Only `theirs` changed (`ours == base`): take `theirs`.
    Theirs,
    /// Only `ours` changed (`theirs == base`): keep `ours`.
    Ours,
    /// Both changed regular files whose mode changed on at most one side:
    /// merge the texts and use `mode`.
    Text { mode: EntryMode, base: ObjectId, ours: ObjectId, theirs: ObjectId },
    /// Anything else (modify/delete, add/add, symlinks, mode changed on both
    /// sides, a directory on either side): the path conflicts.
    Conflict,
}

/// Classify one path. The checks run in the order listed on [`EntryMerge`].
pub fn entries(base: Entry, ours: Entry, theirs: Entry) -> EntryMerge {
    if ours == theirs {
        return EntryMerge::Same;
    }
    if ours == base {
        return EntryMerge::Theirs;
    }
    if theirs == base {
        return EntryMerge::Ours;
    }
    if let (Some(base), Some(ours), Some(theirs)) = (base, ours, theirs)
        && [base.0, ours.0, theirs.0]
            .into_iter()
            .all(|m| matches!(m.class(), EntryMode::File | EntryMode::Executable))
        && (ours.0.class() == base.0.class() || theirs.0.class() == base.0.class())
    {
        // Legacy spellings behave like their class; a mode the other side
        // changed wins, and an unchanged side's exact spelling is dropped
        // (git also writes canonical modes when it rewrites a tree).
        let mode = if ours.0.class() == base.0.class() { theirs.0 } else { ours.0 };
        return EntryMerge::Text { mode, base: base.1, ours: ours.1, theirs: theirs.1 };
    }
    EntryMerge::Conflict
}

/// Result of a 3-way text merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextMerge {
    /// Clean merge: the merged bytes.
    Clean(Vec<u8>),
    /// Overlapping edits: the merged bytes with diff3-style conflict markers
    /// (`<<<<<<< ours`, `||||||| original`, `=======`, `>>>>>>> theirs`, each
    /// at the start of a line).
    Conflict(Vec<u8>),
    /// At least one side is binary ([`is_binary`]): not merged.
    Binary,
}

/// Binary content for merging: a NUL byte in content that is not UTF-8.
/// Content that is not UTF-8 is otherwise text (Latin-1 files merge like
/// any others, as in git); UTF-8 content with a NUL stays mergeable, as it
/// always was, so servers and clients of different versions agree on every
/// input older ones merged.
pub fn is_binary(content: &[u8]) -> bool {
    content.contains(&0) && std::str::from_utf8(content).is_err()
}

/// Line-based 3-way merge of the bytes (`diffy::merge_bytes`: lines split
/// after each `\n`; on UTF-8 input byte-for-byte what `diffy::merge` gives).
pub fn text(base: &[u8], ours: &[u8], theirs: &[u8]) -> TextMerge {
    if [base, ours, theirs].into_iter().any(is_binary) {
        return TextMerge::Binary;
    }
    match diffy::merge_bytes(base, ours, theirs) {
        Ok(merged) => TextMerge::Clean(merged),
        Err(marked) => TextMerge::Conflict(marked),
    }
}

/// Does `text` contain a conflict marker line (`<<<<<<< ` or `>>>>>>> ` at
/// the start of a line, or a line that is exactly `<<<<<<<` / `>>>>>>>`)?
pub fn has_conflict_markers(text: &[u8]) -> bool {
    text.split(|&b| b == b'\n').any(|line| {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        [b"<<<<<<<", b">>>>>>>"].iter().any(|m| {
            line.strip_prefix(&m[..]).is_some_and(|rest| rest.is_empty() || rest[0] == b' ')
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const F: EntryMode = EntryMode::File;
    const X: EntryMode = EntryMode::Executable;
    fn id(n: u8) -> ObjectId {
        ObjectId([n; 32])
    }

    #[test]
    fn entry_classification() {
        let (a, b, c) = (Some((F, id(1))), Some((F, id(2))), Some((F, id(3))));
        assert_eq!(entries(a, b, b), EntryMerge::Same);
        assert_eq!(entries(a, a, b), EntryMerge::Theirs);
        assert_eq!(entries(a, b, a), EntryMerge::Ours);
        assert_eq!(entries(a, None, None), EntryMerge::Same);
        assert_eq!(
            entries(a, b, c),
            EntryMerge::Text { mode: F, base: id(1), ours: id(2), theirs: id(3) }
        );
        // Mode change on one side is carried into the merge.
        assert_eq!(
            entries(a, Some((X, id(2))), c),
            EntryMerge::Text { mode: X, base: id(1), ours: id(2), theirs: id(3) }
        );
        assert_eq!(
            entries(a, b, Some((X, id(3)))),
            EntryMerge::Text { mode: X, base: id(1), ours: id(2), theirs: id(3) }
        );
        // Mode changed on both sides, modify/delete, add/add, symlinks.
        let xa = Some((X, id(1)));
        assert_eq!(entries(xa, Some((F, id(2))), Some((F, id(3)))), EntryMerge::Conflict);
        let link = Some((EntryMode::Symlink, id(3)));
        assert_eq!(entries(a, b, link), EntryMerge::Conflict);
        assert_eq!(entries(a, b, None), EntryMerge::Conflict);
        assert_eq!(entries(None, b, c), EntryMerge::Conflict);
        assert_eq!(entries(a, Some((X, id(2))), Some((X, id(3)))), EntryMerge::Conflict);
    }

    #[test]
    fn text_merges() {
        let base = b"1\n2\n3\n4\n5\n";
        assert_eq!(
            text(base, b"one\n2\n3\n4\n5\n", b"1\n2\n3\n4\nfive\n"),
            TextMerge::Clean(b"one\n2\n3\n4\nfive\n".to_vec())
        );
        let TextMerge::Conflict(marked) =
            text(base, b"1\n2\nours\n4\n5\n", b"1\n2\ntheirs\n4\n5\n")
        else {
            panic!("expected a conflict");
        };
        assert!(has_conflict_markers(&marked));
        let marked = String::from_utf8(marked).unwrap();
        assert!(marked.contains("<<<<<<< ours\nours\n"), "{marked}");
        assert!(marked.contains(">>>>>>> theirs\n"), "{marked}");
        assert_eq!(text(b"\xff\0", b"a", b"b"), TextMerge::Binary);
        assert_eq!(text(b"a", b"a\0\xff", b"b"), TextMerge::Binary);
        // UTF-8 with a NUL still merges.
        assert_eq!(text(b"\0\n", b"\0\nx\n", b"\0\n"), TextMerge::Clean(b"\0\nx\n".to_vec()));
    }

    /// Content that is not UTF-8 merges on its bytes, like git.
    #[test]
    fn non_utf8_texts_merge() {
        let base = b"J\xf6rg\n2\n3\n4\n\xe9\n";
        assert_eq!(
            text(base, b"J\xc3\xb6rg\n2\n3\n4\n\xe9\n", b"J\xf6rg\n2\n3\n4\n\xe9\xff\n"),
            TextMerge::Clean(b"J\xc3\xb6rg\n2\n3\n4\n\xe9\xff\n".to_vec())
        );
        let TextMerge::Conflict(marked) =
            text(base, b"\xe0\n2\n3\n4\n\xe9\n", b"\xe1\n2\n3\n4\n\xe9\n")
        else {
            panic!("expected a conflict");
        };
        assert!(has_conflict_markers(&marked));
        assert!(marked.starts_with(b"<<<<<<< ours\n\xe0\n"));
        assert!(marked.windows(4).any(|w| w == b"\n\xe1\n>"));
    }

    proptest::proptest! {
        /// On UTF-8 input the byte merge is exactly the string merge used
        /// before (so land and `ws update` resolve as they did).
        #[test]
        fn byte_merge_equals_str_merge_on_utf8(
            base in proptest::collection::vec(0u8..5, 0..12),
            ours in proptest::collection::vec(0u8..5, 0..12),
            theirs in proptest::collection::vec(0u8..5, 0..12),
        ) {
            let render = |v: &[u8]| -> String {
                v.iter().map(|&c| ["a\n", "\u{e9}\n", "b\r\n", "\n", "c"][usize::from(c)]).collect()
            };
            let (b, o, t) = (render(&base), render(&ours), render(&theirs));
            let want = match diffy::merge(&b, &o, &t) {
                Ok(m) => TextMerge::Clean(m.into_bytes()),
                Err(m) => TextMerge::Conflict(m.into_bytes()),
            };
            proptest::prop_assert_eq!(text(b.as_bytes(), o.as_bytes(), t.as_bytes()), want);
        }
    }

    #[test]
    fn marker_detection() {
        assert!(has_conflict_markers(b"a\n<<<<<<< local\nb\n"));
        assert!(has_conflict_markers(b">>>>>>>"));
        assert!(has_conflict_markers(b"x\r\n>>>>>>> upstream\r\n"));
        assert!(!has_conflict_markers(b"a\n <<<<<<< indented\n<<<<<<<<x\n=======\n"));
        assert!(!has_conflict_markers(b"plain text\n"));
    }
}
