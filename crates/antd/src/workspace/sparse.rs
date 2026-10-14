//! Sparse patterns: which paths a sparse workspace materializes.

use ant_core::RepoPath;
use anyhow::{Context, bail};
use globset::GlobBuilder;

use crate::files::is_under as under;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sparse {
    pub patterns: Vec<String>,
}

impl Sparse {
    pub fn all() -> Self {
        Self { patterns: vec![] }
    }

    pub fn new(patterns: Vec<String>) -> anyhow::Result<Self> {
        let patterns =
            patterns.into_iter().map(normalize_pattern).collect::<anyhow::Result<Vec<_>>>()?;
        let sparse = Self { patterns };
        // Validates glob syntax up front.
        sparse.compile()?;
        Ok(sparse)
    }

    pub fn matches(&self, path: &RepoPath) -> bool {
        // Patterns from `new` are always valid, so compilation cannot fail here.
        // A hand-built invalid glob matches nothing.
        self.compile().map(|m| m.matches(path)).unwrap_or(false)
    }

    /// Could any path under directory `dir` match?
    pub fn may_contain_under(&self, dir: &RepoPath) -> bool {
        self.compile().map(|m| m.may_contain_under(dir)).unwrap_or(false)
    }

    /// Compile once for hot loops (walks over many entries).
    pub(super) fn compile(&self) -> anyhow::Result<Matcher> {
        let mut prefixes = Vec::new();
        let mut globs = Vec::new();
        for p in &self.patterns {
            if is_glob(p) {
                let glob = GlobBuilder::new(p)
                    .literal_separator(true)
                    .build()
                    .with_context(|| format!("invalid sparse glob {p:?}"))?;
                globs.push(glob.compile_matcher());
            } else {
                prefixes.push(p.clone());
            }
        }
        Ok(Matcher { all: self.patterns.is_empty(), prefixes, globs })
    }
}

pub(super) fn is_glob(pattern: &str) -> bool {
    pattern.contains(['*', '?', '[', '{'])
}

/// Validates a pattern and canonicalizes prefix patterns (no trailing `/`).
pub(super) fn normalize_pattern(pattern: String) -> anyhow::Result<String> {
    if pattern.is_empty() || pattern.contains(['\n', '\0']) {
        bail!("invalid sparse pattern {pattern:?}");
    }
    if is_glob(&pattern) {
        return Ok(pattern);
    }
    let trimmed = pattern.trim_end_matches('/').to_string();
    if trimmed.is_empty() {
        bail!("invalid sparse pattern {pattern:?}");
    }
    RepoPath::new(trimmed.as_str())
        .with_context(|| format!("invalid sparse pattern {pattern:?}"))?;
    Ok(trimmed)
}

pub(super) struct Matcher {
    all: bool,
    prefixes: Vec<String>,
    globs: Vec<globset::GlobMatcher>,
}

impl Matcher {
    pub(super) fn matches(&self, path: &RepoPath) -> bool {
        if self.all {
            return true;
        }
        let s = path.as_str();
        self.prefixes.iter().any(|p| under(s, p)) || self.globs.iter().any(|g| g.is_match(s))
    }

    pub(super) fn may_contain_under(&self, dir: &RepoPath) -> bool {
        if self.all || dir.is_root() || !self.globs.is_empty() {
            return true;
        }
        let d = dir.as_str();
        self.prefixes.iter().any(|p| under(p, d) || under(d, p))
    }
}
