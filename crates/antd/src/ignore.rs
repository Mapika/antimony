//! Ignore rules for untracked workspace files, shared by sparse workspaces
//! ([`crate::workspace`]) and lazy ones (`ant_fuse::rw`).
//!
//! Sources, from lowest to highest precedence:
//! 1. the workspace-local ignore file (`.ant/ignore` as the user sees it; in
//!    a lazy workspace's state directory `meta/ignore`), whose patterns are
//!    anchored at the workspace root like a root `.gitignore`;
//! 2. the `.gitignore` of each directory, applying to that directory's
//!    subtree; a deeper file overrides a shallower one.
//!
//! Matching is `.gitignore` semantics (the `ignore` crate's matcher): `#`
//! comments, `!` negation (the last matching pattern of the deepest file wins),
//! `dir/` patterns match directories only, a pattern with a `/` in front or
//! in the middle is anchored at its file's directory, `**`. As in git, an
//! ignored directory is never entered, so a file below it cannot be
//! re-included by a negation. Global excludes (`core.excludesFile`,
//! `$XDG_CONFIG_HOME/git/ignore`) and `.git/info/exclude` are deliberately
//! not read: a workspace behaves the same on every machine.
//!
//! Rules only decide about *untracked* paths. A path that is tracked (in the
//! index or the base tree) stays tracked and is reported when it changes,
//! whatever the rules say.

use std::sync::Arc;

use ::ignore::Match;
use ::ignore::gitignore::{Gitignore, GitignoreBuilder};

/// File name of the per-directory rule files.
pub const GITIGNORE: &str = ".gitignore";

struct Node {
    parent: Option<Arc<Node>>,
    rules: Gitignore,
}

/// The rules in force inside one directory: the chain of rule files from the
/// workspace root down to it. Cheap to clone and share between threads.
#[derive(Clone, Default)]
pub struct Rules {
    chain: Option<Arc<Node>>,
}

impl Rules {
    /// No rules: nothing is ignored.
    pub fn none() -> Self {
        Self::default()
    }

    /// The rules of directory `dir` (repo path, "" for the root) given its
    /// parent's rules `self` and the text of its rule file, if any.
    pub fn child(&self, dir: &str, text: Option<&str>) -> Rules {
        let Some(text) = text else { return self.clone() };
        let mut builder = GitignoreBuilder::new(format!("/{dir}"));
        for line in text.strip_prefix('\u{feff}').unwrap_or(text).lines() {
            // A line that does not parse as a glob is skipped, like git does.
            let _ = builder.add_line(None, line);
        }
        match builder.build() {
            Ok(rules) if !rules.is_empty() => {
                Rules { chain: Some(Arc::new(Node { parent: self.chain.clone(), rules })) }
            }
            _ => self.clone(),
        }
    }

    /// Whether the untracked path `path` (repo path; `is_dir` for a
    /// directory) is ignored. Its ancestors must already be known not to be
    /// ignored (callers do not descend into ignored directories).
    pub fn is_ignored(&self, path: &str, is_dir: bool) -> bool {
        let abs = format!("/{path}");
        let mut node = self.chain.as_deref();
        while let Some(n) = node {
            match n.rules.matched(&abs, is_dir) {
                Match::Ignore(_) => return true,
                Match::Whitelist(_) => return false,
                Match::None => {}
            }
            node = n.parent.as_deref();
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rule files given shallow to deep; each one applies below the previous.
    fn rules(files: &[(&str, &str)]) -> impl Fn(&str, bool) -> bool {
        let mut r = Rules::none();
        for (dir, text) in files {
            r = r.child(dir, Some(text));
        }
        move |p, d| r.is_ignored(p, d)
    }

    #[test]
    fn basic_globs_and_comments() {
        let is = rules(&[("", "# comment\n*.o\n\ntarget/\n")]);
        assert!(is("a.o", false));
        assert!(is("src/deep/a.o", false));
        assert!(!is("a.c", false));
        assert!(is("target", true));
        assert!(is("crates/x/target", true));
        // Directory-only: a file named `target` is not matched.
        assert!(!is("target", false));
    }

    #[test]
    fn anchoring() {
        let is = rules(&[("", "/build\ndocs/gen\n")]);
        assert!(is("build", true));
        assert!(!is("sub/build", true));
        // A slash in the middle anchors too.
        assert!(is("docs/gen", false));
        assert!(!is("x/docs/gen", false));
    }

    #[test]
    fn negation_last_match_wins() {
        let is = rules(&[("", "*.log\n!keep.log\n")]);
        assert!(is("a.log", false));
        assert!(!is("keep.log", false));
        assert!(!is("d/keep.log", false));
    }

    #[test]
    fn nested_files_are_relative_to_their_directory() {
        let is = rules(&[("", "*.tmp\n"), ("sub", "/only-here\n!x.tmp\n")]);
        assert!(is("sub/only-here", false));
        assert!(!is("only-here", false));
        assert!(!is("sub/deeper/only-here", false));
        // The deeper file overrides the root one.
        assert!(is("a.tmp", false));
        assert!(!is("sub/x.tmp", false));
        assert!(is("sub/y.tmp", false));
    }

    #[test]
    fn double_star_and_escapes() {
        let is = rules(&[("", "a/**/z\n\\#hash\n\\!bang\n")]);
        assert!(is("a/z", false));
        assert!(is("a/b/c/z", false));
        assert!(is("#hash", false));
        assert!(is("!bang", false));
    }

    #[test]
    fn crlf_bom_and_garbage_lines() {
        let is = rules(&[("", "\u{feff}*.o\r\n[\r\nbad[\r\n*.a\r\n")]);
        assert!(is("x.o", false));
        assert!(is("x.a", false));
    }

    #[test]
    fn no_rules_ignores_nothing() {
        assert!(!Rules::none().is_ignored("anything", false));
        assert!(!Rules::none().child("d", Some("# only a comment")).is_ignored("d/x", false));
    }
}
