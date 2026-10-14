//! Format versions of persistent data (store schemas, client caches,
//! workspace state).
//!
//! Every persistent format carries a version number. A reader accepts every
//! version up to the one it was built for and refuses newer ones with
//! [`NewerFormat`], so an older binary never silently misreads data written
//! by a newer one. A missing marker means the format's baseline version
//! (data written before markers existed).

/// Data written by a newer version of antimony than this binary supports.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{what} {found} is newer than this version of ant supports (up to {supported})")]
pub struct NewerFormat {
    /// What was being read, naming the number, e.g. `Postgres schema "ant"
    /// version` or `disk cache /x format`.
    pub what: String,
    pub found: u64,
    pub supported: u64,
}

impl NewerFormat {
    /// The remedy, for a `hint:` line.
    pub const HINT: &'static str =
        "upgrade ant: this data was written by a newer version, which older ones never read";
}

/// `Ok` if a reader supporting versions up to `supported` can read `found`.
pub fn check_format(
    what: impl FnOnce() -> String,
    found: u64,
    supported: u64,
) -> Result<(), NewerFormat> {
    if found > supported {
        return Err(NewerFormat { what: what(), found, supported });
    }
    Ok(())
}

/// Parse the content of a one-line version marker file (`"<n>\n"`).
pub fn parse_format_marker(text: &str) -> Option<u64> {
    text.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_versions_are_refused() {
        assert!(check_format(|| "x".into(), 1, 1).is_ok());
        assert!(check_format(|| "x".into(), 0, 1).is_ok());
        let e = check_format(|| "disk cache /c format".into(), 3, 1).unwrap_err();
        assert_eq!(e, NewerFormat { what: "disk cache /c format".into(), found: 3, supported: 1 });
        assert!(e.to_string().starts_with("disk cache /c format 3 is newer"), "{e}");
        assert_eq!(parse_format_marker("2\n"), Some(2));
        assert_eq!(parse_format_marker("two"), None);
    }
}
