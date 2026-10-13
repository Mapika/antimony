//! Compact parsed source trees and a byte-bounded cache of them.

use std::cmp::Ordering;
use std::sync::Arc;

use ant_core::EntryMode;
use anyhow::{anyhow, bail};

use crate::commit::Oid;
use crate::memo::{DigestMap, Sha1};

/// One tree entry: names are ranges into [`Parsed::raw`], already checked to be UTF-8.
#[derive(Clone, Copy)]
pub(crate) struct Ent {
    pub mode: EntryMode,
    name_start: u32,
    name_end: u32,
    pub id: Sha1,
}

/// A source tree in git order: the raw bytes plus an index over them. No
/// per-entry heap allocation.
pub(crate) struct Parsed {
    raw: Vec<u8>,
    pub ents: Vec<Ent>,
}

impl Parsed {
    pub fn empty() -> Arc<Self> {
        Arc::new(Self { raw: Vec::new(), ents: Vec::new() })
    }

    pub fn name_bytes(&self, e: &Ent) -> &[u8] {
        &self.raw[e.name_start as usize..e.name_end as usize]
    }

    pub fn name(&self, e: &Ent) -> &str {
        std::str::from_utf8(self.name_bytes(e)).unwrap_or_default()
    }

    /// Approximate heap footprint, for cache accounting.
    pub fn weight(&self) -> usize {
        self.raw.len() + self.ents.len() * std::mem::size_of::<Ent>() + 64
    }

    /// The entry called `name`, as a tree (`is_tree`) or as a non-tree.
    pub fn find(&self, name: &[u8], is_tree: bool) -> Option<&Ent> {
        self.ents
            .binary_search_by(|e| key_cmp(self.name_bytes(e), e.mode.is_tree(), name, is_tree))
            .ok()
            .map(|i| &self.ents[i])
    }
}

/// git's tree order: names compare bytewise, with tree names treated as if
/// suffixed by `/`.
pub(crate) fn key_cmp(a: &[u8], a_tree: bool, b: &[u8], b_tree: bool) -> Ordering {
    let n = a.len().min(b.len());
    match a[..n].cmp(&b[..n]) {
        Ordering::Equal => {}
        o => return o,
    }
    let ac = a.get(n).copied().or(a_tree.then_some(b'/'));
    let bc = b.get(n).copied().or(b_tree.then_some(b'/'));
    match (ac, bc) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) => x.cmp(&y),
    }
}

/// Parse a raw git tree. `path` (the directory) and `commit` only appear in
/// error messages.
pub(crate) fn parse_tree(
    id: &Oid,
    raw: Vec<u8>,
    path: &str,
    commit: Option<&Oid>,
) -> anyhow::Result<Parsed> {
    let malformed = || anyhow!("malformed tree {id}");
    let mut ents = Vec::with_capacity(raw.len() / 40 + 1);
    let mut pos = 0usize;
    while pos < raw.len() {
        let rest = &raw[pos..];
        let sp = rest.iter().position(|&b| b == b' ').ok_or_else(malformed)?;
        let nul = sp + 1 + rest[sp + 1..].iter().position(|&b| b == 0).ok_or_else(malformed)?;
        let end = nul + 1 + 20;
        if rest.len() < end {
            bail!("malformed tree {id}");
        }
        if std::str::from_utf8(&rest[sp + 1..nul]).is_err() {
            let commit = commit.map_or_else(|| "?".to_string(), |c| c.to_string());
            bail!(
                "non-UTF-8 file name {:?} in directory {:?} (tree {id}, commit {commit}): \
                 antimony tree entry names must be UTF-8",
                String::from_utf8_lossy(&rest[sp + 1..nul]),
                if path.is_empty() { "/" } else { path },
            );
        }
        let mode = EntryMode::from_octal_lenient(&rest[..sp])?;
        let mut sha = [0u8; 20];
        sha.copy_from_slice(&rest[nul + 1..end]);
        ents.push(Ent {
            mode,
            name_start: (pos + sp + 1) as u32,
            name_end: (pos + nul) as u32,
            id: sha,
        });
        pos += end;
    }
    ents.shrink_to_fit();
    Ok(Parsed { raw, ents })
}

/// Two-generation cache of parsed trees, bounded by bytes.
pub(crate) struct TreeCache {
    hot: DigestMap<Arc<Parsed>>,
    cold: DigestMap<Arc<Parsed>>,
    hot_bytes: usize,
    limit: usize,
}

impl TreeCache {
    /// `limit` bytes per generation (so up to twice that in total).
    pub fn new(limit: usize) -> Self {
        Self { hot: DigestMap::default(), cold: DigestMap::default(), hot_bytes: 0, limit }
    }

    pub fn get(&mut self, id: &Sha1) -> Option<Arc<Parsed>> {
        if let Some(e) = self.hot.get(id) {
            return Some(e.clone());
        }
        let e = self.cold.remove(id)?;
        self.insert(*id, e.clone());
        Some(e)
    }

    pub fn insert(&mut self, id: Sha1, tree: Arc<Parsed>) {
        if self.hot_bytes >= self.limit {
            self.cold = std::mem::take(&mut self.hot);
            self.hot_bytes = 0;
        }
        self.hot_bytes += tree.weight();
        self.hot.insert(id, tree);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_order_matches_git() {
        // "foo-" < "foo.c" < "foo/" (tree "foo").
        assert_eq!(key_cmp(b"foo-", false, b"foo.c", false), Ordering::Less);
        assert_eq!(key_cmp(b"foo.c", false, b"foo", true), Ordering::Less);
        assert_eq!(key_cmp(b"foo", true, b"foo", false), Ordering::Greater);
        assert_eq!(key_cmp(b"foo", false, b"foo", false), Ordering::Equal);
        assert_eq!(key_cmp(b"a", false, b"ab", false), Ordering::Less);
        assert_eq!(key_cmp(b"a", true, b"ab", false), Ordering::Less);
    }

    #[test]
    fn parse_and_find() {
        let id = Oid::null(gix::hash::Kind::Sha1);
        let mut raw = Vec::new();
        for (mode, name) in [("100644", "a.c"), ("40000", "a"), ("100644", "b")] {
            raw.extend_from_slice(format!("{mode} {name}\0").as_bytes());
            raw.extend_from_slice(&[9; 20]);
        }
        let t = parse_tree(&id, raw, "", None).unwrap();
        assert_eq!(t.ents.len(), 3);
        assert!(t.find(b"a", true).is_some());
        assert!(t.find(b"a", false).is_none());
        assert_eq!(t.name(t.find(b"b", false).unwrap()), "b");
    }
}
