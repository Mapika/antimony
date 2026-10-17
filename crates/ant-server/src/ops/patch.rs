//! Line diffs of file contents for `Diff` with `patch` (server side, so CI
//! and review tooling need no checkout).
//!
//! - [`text_patch`]: unified-diff hunks between two versions of a file
//!   (`b""` for the missing side of an add or delete). Lines are split after
//!   each `\n` and compared exactly (a `\r\n` line differs from a `\n` one;
//!   a last line without newline differs from the same line with one). The
//!   line alignment is the blame diff's (`history::line_map_ids`: common
//!   prefix/suffix, then a bit-parallel LCS on the middle; a middle over
//!   the LCS cell cap counts as fully replaced). Within a changed block,
//!   removed lines come before added ones. Hunks carry `context` lines on
//!   both sides and merge when their contexts touch, as `diff -u` does. A
//!   side's last line without newline is followed by the line
//!   `\ No newline at end of file`.
//! - Content is bytes, as in git: a side containing a NUL byte is binary
//!   (no hunks); any other content is text, UTF-8 or not (Latin-1 lines
//!   diff like any others). Hunk lines carry the raw bytes.
//! - [`patches`]: the patches of a diff's changes, with limits: a side over
//!   [`MAX_FILE_BYTES`] is `too_large`; once the hunk lines of the response
//!   exceed [`MAX_PATCH_BYTES`] the remaining files are `too_large` (without
//!   reading their blobs) and the result is marked truncated. Blobs are read
//!   in chunks of [`BLOB_CHUNK`] ids (one `get_many` each); the diffs run on
//!   a blocking thread.
//! - Only regular files and symlinks (their target text) get a patch;
//!   gitlinks get none. Callers filter unreadable paths before (`acl`).

use std::collections::HashMap;

use ant_core::{EncodedObject, EntryMode, ObjectId};
use ant_store::{Backend, PathChange};
use bytes::Bytes;

use super::history::line_map_ids;
use super::{OpError, OpResult};

/// Largest side of a file diffed (bytes).
pub const MAX_FILE_BYTES: usize = 4 << 20;
/// Hunk-line bytes per response before the remaining files are omitted.
pub const MAX_PATCH_BYTES: usize = 32 << 20;
/// Context lines when the request does not say.
pub const DEFAULT_CONTEXT: usize = 3;
/// Most context lines a request may ask for.
pub const MAX_CONTEXT: usize = 100_000;
/// Blob ids per `get_many` while computing patches.
const BLOB_CHUNK: usize = 64;

const NO_NEWLINE: &[u8] = b"\\ No newline at end of file";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilePatch {
    pub binary: bool,
    pub too_large: bool,
    pub hunks: Vec<Hunk>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hunk {
    pub old_start: u32,
    pub old_lines: u32,
    pub new_start: u32,
    pub new_lines: u32,
    /// `' '`, `'-'` or `'+'` and the line's bytes without its terminator
    /// (not necessarily UTF-8), or [`NO_NEWLINE`].
    pub lines: Vec<Vec<u8>>,
}

impl FilePatch {
    fn too_large() -> Self {
        Self { too_large: true, ..Self::default() }
    }

    fn binary() -> Self {
        Self { binary: true, ..Self::default() }
    }

    /// Bytes of hunk lines (what the response budget counts).
    pub fn weight(&self) -> usize {
        self.hunks.iter().flat_map(|h| &h.lines).map(|l| l.len() + 1).sum()
    }
}

/// Binary content: contains a NUL byte (git looks at the first 8000 bytes;
/// the whole side is checked here, as before).
fn is_binary(b: &[u8]) -> bool {
    b.contains(&0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Same(usize, usize),
    Del(usize),
    Ins(usize),
}

/// Unified-diff hunks from `old` to `new` (see the module docs).
pub fn text_patch(old: &[u8], new: &[u8], context: usize) -> FilePatch {
    if is_binary(old) || is_binary(new) {
        return FilePatch::binary();
    }
    let ol: Vec<&[u8]> = old.split_inclusive(|&c| c == b'\n').collect();
    let nl: Vec<&[u8]> = new.split_inclusive(|&c| c == b'\n').collect();
    let mut ids: HashMap<&[u8], u32> = HashMap::with_capacity(ol.len() + nl.len());
    let mut interned = [Vec::with_capacity(ol.len()), Vec::with_capacity(nl.len())];
    for (side, lines) in interned.iter_mut().zip([&ol, &nl]) {
        for l in lines.iter() {
            let next = ids.len() as u32;
            side.push(*ids.entry(*l).or_insert(next));
        }
    }
    let [oi, ni] = interned;
    let map = line_map_ids(&oi, &ni);

    // Edit script; per changed block: deletions, then insertions.
    let mut ops = Vec::with_capacity(ol.len().max(nl.len()));
    let (mut dels, mut ins) = (Vec::new(), Vec::new());
    let mut o = 0;
    let flush = |ops: &mut Vec<Op>, dels: &mut Vec<Op>, ins: &mut Vec<Op>| {
        ops.append(dels);
        ops.append(ins);
    };
    for (n, m) in map.iter().enumerate() {
        match *m {
            Some(k) => {
                dels.extend((o..k).map(Op::Del));
                flush(&mut ops, &mut dels, &mut ins);
                ops.push(Op::Same(k, n));
                o = k + 1;
            }
            None => ins.push(Op::Ins(n)),
        }
    }
    dels.extend((o..ol.len()).map(Op::Del));
    flush(&mut ops, &mut dels, &mut ins);

    let changed: Vec<usize> = ops
        .iter()
        .enumerate()
        .filter(|(_, op)| !matches!(op, Op::Same(..)))
        .map(|(i, _)| i)
        .collect();
    let mut hunks = Vec::new();
    let mut k = 0;
    while k < changed.len() {
        let first = changed[k];
        let mut last = first;
        while k + 1 < changed.len() && changed[k + 1] - last <= 2 * context + 1 {
            k += 1;
            last = changed[k];
        }
        k += 1;
        let start = first.saturating_sub(context);
        let end = (last + context + 1).min(ops.len());
        hunks.push(hunk(&ops[start..end], &ops[..start], &ol, &nl));
    }
    FilePatch { binary: false, too_large: false, hunks }
}

/// One hunk over `ops`; `before` are the ops preceding it (for the starts).
fn hunk(ops: &[Op], before: &[Op], ol: &[&[u8]], nl: &[&[u8]]) -> Hunk {
    let old_before = before.iter().filter(|o| !matches!(o, Op::Ins(_))).count();
    let new_before = before.iter().filter(|o| !matches!(o, Op::Del(_))).count();
    let old_lines = ops.iter().filter(|o| !matches!(o, Op::Ins(_))).count();
    let new_lines = ops.iter().filter(|o| !matches!(o, Op::Del(_))).count();
    let start = |before: usize, lines: usize| before + usize::from(lines > 0);
    let no_nl = |lines: &[&[u8]], i: usize| i + 1 == lines.len() && !lines[i].ends_with(b"\n");
    let text = |l: &'_ [u8]| l.strip_suffix(b"\n").unwrap_or(l).to_vec();
    let mut lines = Vec::with_capacity(ops.len());
    for op in ops {
        let (prefix, line, missing) = match *op {
            Op::Same(o, n) => (b' ', ol[o], no_nl(ol, o) || no_nl(nl, n)),
            Op::Del(o) => (b'-', ol[o], no_nl(ol, o)),
            Op::Ins(n) => (b'+', nl[n], no_nl(nl, n)),
        };
        let mut l = text(line);
        l.insert(0, prefix);
        lines.push(l);
        if missing {
            lines.push(NO_NEWLINE.to_vec());
        }
    }
    let n32 = |v: usize| u32::try_from(v).unwrap_or(u32::MAX);
    Hunk {
        old_start: n32(start(old_before, old_lines)),
        old_lines: n32(old_lines),
        new_start: n32(start(new_before, new_lines)),
        new_lines: n32(new_lines),
        lines,
    }
}

/// The blob side of a change, if it has content to diff.
fn blob_side(side: Option<(EntryMode, ObjectId)>) -> Option<ObjectId> {
    side.filter(|(m, _)| m.is_blob() && m.class() != EntryMode::Gitlink).map(|(_, id)| id)
}

fn body(o: &EncodedObject) -> Bytes {
    let all = o.as_bytes();
    all.slice(all.len() - o.body().len()..)
}

/// Index-aligned patches of `changes` (`None` for gitlinks), and whether
/// the response budget truncated them (see the module docs).
pub async fn patches(
    b: &Backend,
    changes: &[PathChange],
    context: usize,
) -> OpResult<(Vec<Option<FilePatch>>, bool)> {
    let mut out: Vec<Option<FilePatch>> = Vec::with_capacity(changes.len());
    let mut budget = MAX_PATCH_BYTES;
    let mut truncated = false;
    for chunk in changes.chunks(BLOB_CHUNK) {
        if truncated {
            out.extend(chunk.iter().map(|c| {
                (blob_side(c.old).is_some() || blob_side(c.new).is_some())
                    .then(FilePatch::too_large)
            }));
            continue;
        }
        let mut ids: Vec<ObjectId> =
            chunk.iter().flat_map(|c| [blob_side(c.old), blob_side(c.new)]).flatten().collect();
        ids.sort_unstable();
        ids.dedup();
        let blobs: HashMap<ObjectId, Bytes> = ids
            .iter()
            .zip(b.objects.get_many(&ids).await?)
            .map(|(id, o)| o.map(|o| (*id, body(&o))).ok_or_else(|| OpError::not_found_object(*id)))
            .collect::<OpResult<_>>()?;
        let pairs: Vec<Option<(Bytes, Bytes)>> = chunk
            .iter()
            .map(|c| {
                let (o, n) = (blob_side(c.old), blob_side(c.new));
                if o.is_none() && n.is_none() {
                    return None;
                }
                let side =
                    |id: Option<ObjectId>| id.map(|id| blobs[&id].clone()).unwrap_or_default();
                Some((side(o), side(n)))
            })
            .collect();
        let computed = tokio::task::spawn_blocking(move || {
            pairs
                .into_iter()
                .map(|p| {
                    p.map(|(o, n)| {
                        if o.len() > MAX_FILE_BYTES || n.len() > MAX_FILE_BYTES {
                            FilePatch::too_large()
                        } else {
                            text_patch(&o, &n, context)
                        }
                    })
                })
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|e| OpError::Aborted(format!("patch computation failed: {e}")))?;
        for p in computed {
            out.push(p.map(|p| {
                if truncated {
                    return FilePatch::too_large();
                }
                let w = p.weight();
                if w > budget {
                    truncated = true;
                    return FilePatch::too_large();
                }
                budget -= w;
                p
            }));
        }
    }
    Ok((out, truncated))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(p: &FilePatch) -> String {
        let mut s = String::new();
        for h in &p.hunks {
            s.push_str(&format!(
                "@@ -{},{} +{},{} @@\n",
                h.old_start, h.old_lines, h.new_start, h.new_lines
            ));
            for l in &h.lines {
                s.push_str(&String::from_utf8_lossy(l));
                s.push('\n');
            }
        }
        s
    }

    fn lines(n: std::ops::Range<usize>) -> String {
        n.map(|i| format!("l{i}\n")).collect()
    }

    #[test]
    fn modify_add_delete() {
        let p = text_patch(b"a\nb\nc\n", b"a\nB\nc\n", 3);
        assert_eq!(render(&p), "@@ -1,3 +1,3 @@\n a\n-b\n+B\n c\n");
        let p = text_patch(b"", b"x\ny\n", 3);
        assert_eq!(render(&p), "@@ -0,0 +1,2 @@\n+x\n+y\n");
        let p = text_patch(b"x\ny\n", b"", 3);
        assert_eq!(render(&p), "@@ -1,2 +0,0 @@\n-x\n-y\n");
        assert!(text_patch(b"same\n", b"same\n", 3).hunks.is_empty());
        assert!(text_patch(b"", b"", 3).hunks.is_empty());
    }

    #[test]
    fn hunks_split_and_merge_by_context() {
        let old = lines(0..30);
        let new = old.replace("l2\n", "L2\n").replace("l25\n", "L25\n");
        let p = text_patch(old.as_bytes(), new.as_bytes(), 3);
        assert_eq!(p.hunks.len(), 2);
        assert_eq!((p.hunks[0].old_start, p.hunks[0].old_lines, p.hunks[0].new_start), (1, 6, 1));
        assert_eq!((p.hunks[1].old_start, p.hunks[1].old_lines), (23, 7));
        assert_eq!(p.hunks[1].lines.first().unwrap(), b" l22");
        assert_eq!(p.hunks[1].lines.last().unwrap(), b" l28");
        // Changes 7 lines apart: contexts of 3 touch, one hunk.
        let new = old.replace("l10\n", "X\n").replace("l17\n", "Y\n");
        let p = text_patch(old.as_bytes(), new.as_bytes(), 3);
        assert_eq!(p.hunks.len(), 1, "{}", render(&p));
        assert_eq!((p.hunks[0].old_start, p.hunks[0].old_lines), (8, 14));
        // 8 apart: two hunks.
        let new = old.replace("l10\n", "X\n").replace("l18\n", "Y\n");
        assert_eq!(text_patch(old.as_bytes(), new.as_bytes(), 3).hunks.len(), 2);
        // Zero context.
        let p = text_patch(old.as_bytes(), new.as_bytes(), 0);
        assert_eq!(render(&p), "@@ -11,1 +11,1 @@\n-l10\n+X\n@@ -19,1 +19,1 @@\n-l18\n+Y\n");
        // Pure insertion with zero context: the old side starts before it.
        let p = text_patch(b"a\nb\n", b"a\nnew\nb\n", 0);
        assert_eq!(render(&p), "@@ -1,0 +2,1 @@\n+new\n");
    }

    #[test]
    fn deletions_before_insertions_within_a_block() {
        let p = text_patch(b"k\na\nb\nk2\n", b"k\nx\ny\nk2\n", 1);
        assert_eq!(render(&p), "@@ -1,4 +1,4 @@\n k\n-a\n-b\n+x\n+y\n k2\n");
    }

    #[test]
    fn missing_newline_at_end() {
        let p = text_patch(b"a\nb", b"a\nb\n", 3);
        assert_eq!(render(&p), "@@ -1,2 +1,2 @@\n a\n-b\n\\ No newline at end of file\n+b\n");
        let p = text_patch(b"a\nb", b"A\nb", 3);
        assert_eq!(render(&p), "@@ -1,2 +1,2 @@\n-a\n+A\n b\n\\ No newline at end of file\n");
        // CRLF lines are compared exactly and keep their \r.
        let p = text_patch(b"a\r\n", b"a\n", 3);
        assert_eq!(render(&p), "@@ -1,1 +1,1 @@\n-a\r\n+a\n");
    }

    #[test]
    fn binary_sides() {
        assert!(text_patch(b"a\0b", b"a", 3).binary);
        assert!(text_patch(b"a", b"\xff\0", 3).binary);
        assert!(text_patch(b"a", b"\xff\0", 3).hunks.is_empty());
    }

    /// Content that is not UTF-8 (Latin-1, stray bytes) diffs as text, the
    /// hunk lines carrying the bytes, like git.
    #[test]
    fn non_utf8_text_diffs_bytewise() {
        let p = text_patch(b"one\nJ\xf6rg\n", b"one\nJ\xc3\xb6rg\n\xff", 3);
        assert!(!p.binary);
        let want: Vec<&[u8]> = vec![b" one", b"-J\xf6rg", b"+J\xc3\xb6rg", b"+\xff", NO_NEWLINE];
        assert_eq!(p.hunks.len(), 1);
        assert_eq!(p.hunks[0].lines, want);
        assert_eq!((p.hunks[0].old_lines, p.hunks[0].new_lines), (2, 3));
        // Same bytes: no hunks; a one-byte change inside a UTF-8 sequence
        // changes the line.
        assert!(text_patch(b"\xe9\n", b"\xe9\n", 3).hunks.is_empty());
        let p = text_patch(b"\xc3\xa9\n", b"\xc3\xa8\n", 0);
        assert_eq!(p.hunks[0].lines, [&b"-\xc3\xa9"[..], b"+\xc3\xa8"]);
    }

    /// Applying the hunks to the old text gives the new text.
    fn apply(old: &[u8], p: &FilePatch) -> Vec<u8> {
        let old: Vec<&[u8]> = crate::ops::history::byte_lines(old).collect();
        let mut out: Vec<Vec<u8>> = Vec::new();
        let mut at = 0;
        for h in &p.hunks {
            let start =
                if h.old_lines == 0 { h.old_start as usize } else { h.old_start as usize - 1 };
            out.extend(old[at..start].iter().map(|s| s.to_vec()));
            at = start;
            for l in &h.lines {
                match l[0] {
                    b' ' => {
                        out.push(l[1..].to_vec());
                        at += 1;
                    }
                    b'-' => at += 1,
                    b'+' => out.push(l[1..].to_vec()),
                    _ => {}
                }
            }
        }
        out.extend(old[at..].iter().map(|s| s.to_vec()));
        out.join(&b'\n')
    }

    proptest::proptest! {
        #[test]
        fn hunks_apply(
            old in proptest::collection::vec(0u8..6, 0..40),
            edits in proptest::collection::vec((0usize..40, 0u8..8), 0..8),
            context in 0usize..4,
        ) {
            let mut new = old.clone();
            for (at, v) in edits {
                let at = at.min(new.len());
                match v {
                    6 if at < new.len() => { new.remove(at); }
                    7 => new.insert(at, 9),
                    v => if at < new.len() { new[at] = v } else { new.push(v) },
                }
            }
            // Symbols 0-5; with an odd context, two of them are not UTF-8.
            let text = |v: &[u8]| -> Vec<u8> {
                v.iter()
                    .flat_map(|&c| match c {
                        3 if context % 2 == 1 => b"\xe9x\n".to_vec(),
                        4 if context % 2 == 1 => b"J\xf6rg\xff\n".to_vec(),
                        c => format!("x{c}\n").into_bytes(),
                    })
                    .collect()
            };
            let (o, n) = (text(&old), text(&new));
            let p = text_patch(&o, &n, context);
            proptest::prop_assert!(!p.binary);
            let want = crate::ops::history::byte_lines(&n).collect::<Vec<_>>().join(&b'\n');
            proptest::prop_assert_eq!(apply(&o, &p), want);
            // Counts in the headers match the lines.
            for h in &p.hunks {
                let olds = h.lines.iter().filter(|l| !l.starts_with(b"+")).count();
                let news = h.lines.iter().filter(|l| !l.starts_with(b"-")).count();
                proptest::prop_assert_eq!(olds as u32, h.old_lines);
                proptest::prop_assert_eq!(news as u32, h.new_lines);
            }
        }
    }
}
