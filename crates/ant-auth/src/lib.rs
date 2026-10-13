//! Authentication and path/ref authorization policy.
//!
//! A policy is a TOML file:
//!
//! ```toml
//! [[principal]]
//! name = "agent-7"
//! token_sha256 = "<64 hex chars: sha256 of the bearer token>"
//! groups = ["agents"]
//! # admin = true      # bypasses every check
//!
//! [[path]]
//! prefix = ""                    # "" = the whole repository
//! read = ["*"]                   # "*", a principal name, or "group:<g>"
//! write = ["group:agents"]
//!
//! [[path]]
//! prefix = "secrets"
//! read = ["group:security"]
//! write = ["group:security"]
//!
//! [[ref]]
//! pattern = "main"               # exact name, or a prefix glob like "ws/*"
//! update = ["group:admins"]      # direct UpdateRef (create / move / delete)
//! land = ["*"]                   # Land onto the ref
//! ```
//!
//! Path rules: the rule with the longest `prefix` that the path lies under
//! (`RepoPath::starts_with`) decides both read and write; rules do not merge.
//! No matching rule → denied. Ref rules: the first matching rule in file
//! order decides; none → denied.
//!
//! Zones: the prefix of the rule governing a path is that path's *zone*.
//! Content-addressed objects are tagged with the zones they were written
//! under (see `ant-server` ACL docs); [`Policy::can_read_object`] decides
//! access from those tags so a caller cannot fetch a restricted blob just by
//! knowing its id.

use std::collections::HashSet;

use ant_core::RepoPath;
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("policy parse error: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid policy: {0}")]
    Invalid(String),
}

/// An authenticated caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub name: String,
    pub groups: Vec<String>,
    pub admin: bool,
}

/// A zone is identified by the prefix of the path rule that governs it.
pub type Zone = String;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPrincipal {
    name: String,
    token_sha256: String,
    #[serde(default)]
    groups: Vec<String>,
    #[serde(default)]
    admin: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPath {
    prefix: String,
    #[serde(default)]
    read: Vec<String>,
    #[serde(default)]
    write: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRef {
    pattern: String,
    #[serde(default)]
    update: Vec<String>,
    #[serde(default)]
    land: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPolicy {
    #[serde(default, rename = "principal")]
    principals: Vec<RawPrincipal>,
    #[serde(default, rename = "path")]
    paths: Vec<RawPath>,
    #[serde(default, rename = "ref")]
    refs: Vec<RawRef>,
}

#[derive(Debug, Clone)]
struct PathRule {
    prefix: RepoPath,
    read: Vec<String>,
    write: Vec<String>,
}

#[derive(Debug, Clone)]
struct RefRule {
    pattern: String,
    update: Vec<String>,
    land: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Policy {
    /// (sha256 of token, principal)
    principals: Vec<([u8; 32], Principal)>,
    /// Sorted by prefix length, longest first.
    paths: Vec<PathRule>,
    refs: Vec<RefRule>,
}

fn matches(who: &[String], p: &Principal) -> bool {
    who.iter().any(|w| {
        w == "*"
            || *w == p.name
            || w.strip_prefix("group:").is_some_and(|g| p.groups.iter().any(|pg| pg == g))
    })
}

fn ref_matches(pattern: &str, name: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => name.starts_with(prefix),
        None => pattern == name,
    }
}

pub fn hash_token(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

impl Policy {
    pub fn parse(text: &str) -> Result<Self, PolicyError> {
        let raw: RawPolicy = toml::from_str(text)?;
        let mut names = HashSet::new();
        let mut principals = Vec::new();
        for p in raw.principals {
            if !names.insert(p.name.clone()) {
                return Err(PolicyError::Invalid(format!("duplicate principal {:?}", p.name)));
            }
            let mut hash = [0u8; 32];
            hex::decode_to_slice(&p.token_sha256, &mut hash).map_err(|_| {
                PolicyError::Invalid(format!("principal {:?}: token_sha256 must be 64 hex chars", p.name))
            })?;
            principals.push((hash, Principal { name: p.name, groups: p.groups, admin: p.admin }));
        }
        let mut prefixes = HashSet::new();
        let mut paths = Vec::new();
        for r in raw.paths {
            let prefix = RepoPath::new(r.prefix.clone())
                .map_err(|_| PolicyError::Invalid(format!("bad path prefix {:?}", r.prefix)))?;
            if !prefixes.insert(prefix.clone()) {
                return Err(PolicyError::Invalid(format!("duplicate path prefix {:?}", r.prefix)));
            }
            paths.push(PathRule { prefix, read: r.read, write: r.write });
        }
        paths.sort_by_key(|r| std::cmp::Reverse(r.prefix.as_str().len()));
        let refs = raw
            .refs
            .into_iter()
            .map(|r| RefRule { pattern: r.pattern, update: r.update, land: r.land })
            .collect();
        Ok(Self { principals, paths, refs })
    }

    /// Look up the principal for a bearer token (constant-time compare not
    /// needed: we compare hashes).
    pub fn authenticate(&self, token: &str) -> Option<&Principal> {
        let h = hash_token(token);
        self.principals.iter().find(|(ph, _)| *ph == h).map(|(_, p)| p)
    }

    fn rule(&self, path: &RepoPath) -> Option<&PathRule> {
        self.paths.iter().find(|r| path.starts_with(&r.prefix))
    }

    /// The zone (governing rule prefix) of `path`; `None` if no rule matches.
    pub fn zone_of(&self, path: &RepoPath) -> Option<Zone> {
        self.rule(path).map(|r| r.prefix.as_str().to_string())
    }

    /// The zone of the repository root, if any rule covers it.
    pub fn root_zone(&self) -> Option<Zone> {
        self.zone_of(&RepoPath::root())
    }

    /// All zones defined by the policy.
    pub fn zones(&self) -> Vec<Zone> {
        self.paths.iter().map(|r| r.prefix.as_str().to_string()).collect()
    }

    pub fn can_read(&self, p: &Principal, path: &RepoPath) -> bool {
        p.admin || self.rule(path).is_some_and(|r| matches(&r.read, p))
    }

    pub fn can_write(&self, p: &Principal, path: &RepoPath) -> bool {
        p.admin || self.rule(path).is_some_and(|r| matches(&r.write, p))
    }

    pub fn can_read_zone(&self, p: &Principal, zone: &str) -> bool {
        p.admin
            || self
                .paths
                .iter()
                .find(|r| r.prefix.as_str() == zone)
                .is_some_and(|r| matches(&r.read, p))
    }

    /// True if `p` can read every zone (lets servers skip per-object checks).
    pub fn can_read_everything(&self, p: &Principal) -> bool {
        p.admin || self.paths.iter().all(|r| matches(&r.read, p))
    }

    /// Object access from its zone tags. Untagged objects belong to the root
    /// zone. Tagged objects are readable if ANY tag's zone is readable (the
    /// same bytes stored in a readable place are not secret).
    pub fn can_read_object(&self, p: &Principal, tags: &[Zone]) -> bool {
        if p.admin {
            return true;
        }
        if tags.is_empty() {
            return self.root_zone().is_some_and(|z| self.can_read_zone(p, &z));
        }
        tags.iter().any(|z| self.can_read_zone(p, z))
    }

    fn ref_rule(&self, name: &str) -> Option<&RefRule> {
        self.refs.iter().find(|r| ref_matches(&r.pattern, name))
    }

    pub fn can_update_ref(&self, p: &Principal, name: &str) -> bool {
        p.admin || self.ref_rule(name).is_some_and(|r| matches(&r.update, p))
    }

    pub fn can_land(&self, p: &Principal, name: &str) -> bool {
        p.admin || self.ref_rule(name).is_some_and(|r| matches(&r.land, p))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(t: &str) -> String {
        hex::encode(hash_token(t))
    }

    fn policy() -> Policy {
        Policy::parse(&format!(
            r#"
            [[principal]]
            name = "agent"
            token_sha256 = "{}"
            groups = ["agents"]

            [[principal]]
            name = "sec"
            token_sha256 = "{}"
            groups = ["security", "agents"]

            [[principal]]
            name = "root"
            token_sha256 = "{}"
            admin = true

            [[path]]
            prefix = ""
            read = ["*"]
            write = ["group:agents"]

            [[path]]
            prefix = "secrets"
            read = ["group:security"]
            write = ["group:security"]

            [[path]]
            prefix = "secrets/public"
            read = ["*"]
            write = []

            [[ref]]
            pattern = "main"
            update = []
            land = ["group:agents"]

            [[ref]]
            pattern = "ws/*"
            update = ["*"]
            land = ["*"]
            "#,
            h("t-agent"),
            h("t-sec"),
            h("t-root")
        ))
        .unwrap()
    }

    fn p(s: &str) -> RepoPath {
        RepoPath::new(s).unwrap()
    }

    #[test]
    fn authenticate() {
        let pol = policy();
        assert_eq!(pol.authenticate("t-agent").unwrap().name, "agent");
        assert!(pol.authenticate("wrong").is_none());
    }

    #[test]
    fn longest_prefix_decides() {
        let pol = policy();
        let agent = pol.authenticate("t-agent").unwrap().clone();
        let sec = pol.authenticate("t-sec").unwrap().clone();
        assert!(pol.can_read(&agent, &p("src/a.rs")));
        assert!(pol.can_write(&agent, &p("src/a.rs")));
        assert!(!pol.can_read(&agent, &p("secrets/key")));
        assert!(!pol.can_write(&agent, &p("secrets/key")));
        assert!(pol.can_read(&agent, &p("secrets/public/readme")));
        assert!(!pol.can_write(&sec, &p("secrets/public/readme")));
        assert!(pol.can_read(&sec, &p("secrets/key")));
        // "secretsX" is not under "secrets".
        assert!(pol.can_read(&agent, &p("secretsX/a")));
        assert_eq!(pol.zone_of(&p("secrets/k")).as_deref(), Some("secrets"));
        assert_eq!(pol.zone_of(&p("src")).as_deref(), Some(""));
        assert!(!pol.can_read_everything(&agent));
        assert!(pol.can_read_everything(&sec));
    }

    #[test]
    fn object_tags() {
        let pol = policy();
        let agent = pol.authenticate("t-agent").unwrap().clone();
        assert!(pol.can_read_object(&agent, &[]));
        assert!(!pol.can_read_object(&agent, &["secrets".into()]));
        assert!(pol.can_read_object(&agent, &["secrets".into(), "".into()]));
        assert!(pol.can_read_object(&agent, &["secrets/public".into()]));
    }

    #[test]
    fn refs_and_admin() {
        let pol = policy();
        let agent = pol.authenticate("t-agent").unwrap().clone();
        let root = pol.authenticate("t-root").unwrap().clone();
        assert!(pol.can_land(&agent, "main"));
        assert!(!pol.can_update_ref(&agent, "main"));
        assert!(pol.can_update_ref(&agent, "ws/agent/x"));
        assert!(!pol.can_land(&agent, "release"));
        assert!(pol.can_update_ref(&root, "release"));
        assert!(pol.can_read(&root, &p("secrets/k")));
    }

    #[test]
    fn no_rule_denies() {
        let pol = Policy::parse(
            r#"
            [[path]]
            prefix = "open"
            read = ["*"]
            "#,
        )
        .unwrap();
        let anon = Principal { name: "x".into(), groups: vec![], admin: false };
        assert!(!pol.can_read(&anon, &p("other")));
        assert!(pol.can_read(&anon, &p("open/x")));
        assert!(!pol.can_read_object(&anon, &[]));
    }

    #[test]
    fn invalid_policies() {
        assert!(Policy::parse("[[path]]\nprefix = \"/abs\"\nread=[]").is_err());
        assert!(Policy::parse("[[path]]\nprefix = \"a\"\n[[path]]\nprefix = \"a\"").is_err());
        assert!(Policy::parse("[[principal]]\nname=\"a\"\ntoken_sha256=\"zz\"").is_err());
        assert!(Policy::parse("bogus = 1").is_err());
    }
}
