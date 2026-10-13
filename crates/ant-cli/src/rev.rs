//! `<rev>[:<path>]` parsing.

use anyhow::{Result, bail};

/// Split `<rev>[:<path>]`. The path has surrounding slashes trimmed.
pub fn parse_rev_path(s: &str) -> Result<(&str, Option<&str>)> {
    let (rev, path) = match s.split_once(':') {
        Some((r, p)) => (r, Some(p.trim_matches('/'))),
        None => (s, None),
    };
    if rev.is_empty() {
        bail!("empty revision in {s:?}");
    }
    Ok((rev, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits() {
        assert_eq!(parse_rev_path("main").unwrap(), ("main", None));
        assert_eq!(parse_rev_path("main:").unwrap(), ("main", Some("")));
        assert_eq!(parse_rev_path("main:src/a.rs").unwrap(), ("main", Some("src/a.rs")));
        assert_eq!(parse_rev_path("main~2:/src/").unwrap(), ("main~2", Some("src")));
        assert_eq!(parse_rev_path("a:b:c").unwrap(), ("a", Some("b:c")));
        assert!(parse_rev_path(":x").is_err());
        assert!(parse_rev_path("").is_err());
    }
}
