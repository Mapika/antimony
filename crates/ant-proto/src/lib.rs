//! Generated gRPC types for `proto/ant.proto`, plus conversions to and from
//! the `ant_core` types they carry; [`bundle`] is the framing of the HTTP
//! tree-bundle endpoint.

pub mod bundle;

pub mod v1 {
    tonic::include_proto!("ant.v1");
}

/// Max gRPC message size for both directions (large blobs inline).
pub const MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

impl From<ant_core::EntryMode> for v1::EntryMode {
    fn from(mode: ant_core::EntryMode) -> Self {
        // Legacy modes travel as their semantic class; their exact spelling
        // is only visible in the tree objects themselves.
        match mode.class() {
            ant_core::EntryMode::File | ant_core::EntryMode::Legacy(_) => v1::EntryMode::File,
            ant_core::EntryMode::Executable => v1::EntryMode::Executable,
            ant_core::EntryMode::Symlink => v1::EntryMode::Symlink,
            ant_core::EntryMode::Tree => v1::EntryMode::Tree,
            ant_core::EntryMode::Gitlink => v1::EntryMode::Gitlink,
        }
    }
}

/// The core mode of a protobuf mode; `None` for `ENTRY_MODE_UNSPECIFIED`.
pub fn entry_mode(mode: v1::EntryMode) -> Option<ant_core::EntryMode> {
    Some(match mode {
        v1::EntryMode::File => ant_core::EntryMode::File,
        v1::EntryMode::Executable => ant_core::EntryMode::Executable,
        v1::EntryMode::Symlink => ant_core::EntryMode::Symlink,
        v1::EntryMode::Tree => ant_core::EntryMode::Tree,
        v1::EntryMode::Gitlink => ant_core::EntryMode::Gitlink,
        v1::EntryMode::Unspecified => return None,
    })
}

/// The core mode of a protobuf mode field's raw value; `None` for unknown
/// values and `ENTRY_MODE_UNSPECIFIED`.
pub fn entry_mode_from_i32(mode: i32) -> Option<ant_core::EntryMode> {
    v1::EntryMode::try_from(mode).ok().and_then(entry_mode)
}

impl From<&ant_core::Signature> for v1::Signature {
    fn from(s: &ant_core::Signature) -> Self {
        v1::Signature {
            name: s.name.clone(),
            email: s.email.clone(),
            time: s.time,
            tz_offset_minutes: s.tz_offset_minutes,
        }
    }
}

impl From<v1::Signature> for ant_core::Signature {
    fn from(s: v1::Signature) -> Self {
        ant_core::Signature {
            name: s.name,
            email: s.email,
            time: s.time,
            tz_offset_minutes: s.tz_offset_minutes,
        }
    }
}

// File content travels in `string` fields that predate content that is not
// UTF-8 (`BlameLine.line`, `Hunk.lines`); the exact bytes go in an added
// `bytes` field only when the text is not UTF-8, so UTF-8 content costs
// nothing extra and older clients get a lossy rendering.

impl v1::BlameLine {
    /// A line attributed to `commit`, from the line's bytes.
    pub fn from_bytes(commit: Vec<u8>, line: Vec<u8>) -> Self {
        match String::from_utf8(line) {
            Ok(line) => v1::BlameLine { commit, line, raw: None },
            Err(e) => {
                let raw = e.into_bytes();
                v1::BlameLine {
                    commit,
                    line: String::from_utf8_lossy(&raw).into_owned(),
                    raw: Some(raw),
                }
            }
        }
    }

    /// The line's exact bytes (`raw` when set, else `line`).
    pub fn bytes(&self) -> &[u8] {
        self.raw.as_deref().unwrap_or(self.line.as_bytes())
    }
}

impl v1::Hunk {
    /// Fills `lines` (and `raw_lines` when one is not UTF-8) from the lines'
    /// bytes, each with its `' '`/`'-'`/`'+'` prefix.
    pub fn set_lines(&mut self, lines: Vec<Vec<u8>>) {
        if lines.iter().all(|l| std::str::from_utf8(l).is_ok()) {
            self.lines =
                lines.into_iter().map(|l| String::from_utf8(l).unwrap_or_default()).collect();
            self.raw_lines = Vec::new();
        } else {
            self.lines = lines.iter().map(|l| String::from_utf8_lossy(l).into_owned()).collect();
            self.raw_lines = lines;
        }
    }

    /// The lines' exact bytes (`raw_lines` when set, else `lines`).
    pub fn line_bytes(&self) -> Vec<&[u8]> {
        if self.raw_lines.is_empty() {
            self.lines.iter().map(String::as_bytes).collect()
        } else {
            self.raw_lines.iter().map(Vec::as_slice).collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_lines_carry_bytes_only_when_not_utf8() {
        let l = v1::BlameLine::from_bytes(vec![1], "caf\u{e9}".into());
        assert_eq!(
            (l.line.as_str(), l.raw.as_ref(), l.bytes()),
            ("caf\u{e9}", None, "caf\u{e9}".as_bytes())
        );
        let l = v1::BlameLine::from_bytes(vec![1], b"caf\xe9".to_vec());
        assert_eq!((l.line.as_str(), l.bytes()), ("caf\u{fffd}", &b"caf\xe9"[..]));
        let l = v1::BlameLine::from_bytes(vec![1], Vec::new());
        assert_eq!((l.raw.as_ref(), l.bytes()), (None, &b""[..]));

        let mut h = v1::Hunk::default();
        h.set_lines(vec![b" a".to_vec(), b"+\xc3\xa9".to_vec()]);
        assert_eq!((h.lines.len(), h.raw_lines.len()), (2, 0));
        assert_eq!(h.line_bytes(), [&b" a"[..], b"+\xc3\xa9"]);
        h.set_lines(vec![b" a".to_vec(), b"-J\xf6rg".to_vec(), b"+".to_vec()]);
        assert_eq!(h.lines, [" a", "-J\u{fffd}rg", "+"]);
        assert_eq!(h.line_bytes(), [&b" a"[..], b"-J\xf6rg", b"+"]);
    }

    #[test]
    fn entry_modes_round_trip_and_reject_unknown_values() {
        for mode in [
            ant_core::EntryMode::File,
            ant_core::EntryMode::Executable,
            ant_core::EntryMode::Symlink,
            ant_core::EntryMode::Tree,
            ant_core::EntryMode::Gitlink,
        ] {
            assert_eq!(entry_mode_from_i32(v1::EntryMode::from(mode) as i32), Some(mode));
        }
        assert_eq!(entry_mode_from_i32(v1::EntryMode::Unspecified as i32), None);
        assert_eq!(entry_mode_from_i32(-1), None);
        assert_eq!(entry_mode_from_i32(1000), None);
    }
}
