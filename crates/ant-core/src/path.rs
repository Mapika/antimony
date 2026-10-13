use std::fmt;

use crate::{Error, Result};

/// A normalized, repo-relative path: `/`-separated, no leading or trailing
/// slash, no empty, `.` or `..` components, no NUL. The empty path is the root.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct RepoPath(String);

impl RepoPath {
    pub fn root() -> Self {
        Self(String::new())
    }

    pub fn new(s: impl Into<String>) -> Result<Self> {
        let s = s.into();
        if s.is_empty() {
            return Ok(Self(s));
        }
        for comp in s.split('/') {
            if comp.is_empty() || comp == "." || comp == ".." || comp.contains('\0') {
                return Err(Error::InvalidPath(s));
            }
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub fn components(&self) -> impl Iterator<Item = &str> {
        self.0.split('/').filter(|c| !c.is_empty())
    }

    /// Split into (parent, file name). Root has no file name.
    pub fn split_last(&self) -> Option<(RepoPath, &str)> {
        if self.is_root() {
            return None;
        }
        Some(match self.0.rsplit_once('/') {
            Some((parent, name)) => (RepoPath(parent.to_string()), name),
            None => (RepoPath::root(), self.0.as_str()),
        })
    }

    pub fn join(&self, name: &str) -> Result<RepoPath> {
        if self.is_root() {
            RepoPath::new(name)
        } else {
            RepoPath::new(format!("{}/{}", self.0, name))
        }
    }

    /// True if `self` equals `other` or lies underneath it.
    pub fn starts_with(&self, other: &RepoPath) -> bool {
        other.is_root()
            || self.0 == other.0
            || (self.0.starts_with(&other.0) && self.0.as_bytes().get(other.0.len()) == Some(&b'/'))
    }
}

impl fmt::Display for RepoPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for RepoPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation() {
        assert!(RepoPath::new("a/b/c.rs").is_ok());
        for bad in ["/a", "a/", "a//b", "./a", "a/../b", "a\0b"] {
            assert!(RepoPath::new(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn helpers() {
        let p = RepoPath::new("a/b/c").unwrap();
        let (parent, name) = p.split_last().unwrap();
        assert_eq!(parent.as_str(), "a/b");
        assert_eq!(name, "c");
        assert!(p.starts_with(&RepoPath::new("a/b").unwrap()));
        assert!(!p.starts_with(&RepoPath::new("a/bc").unwrap()));
        assert!(p.starts_with(&RepoPath::root()));
        assert_eq!(RepoPath::root().join("x").unwrap().as_str(), "x");
    }
}
