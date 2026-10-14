//! Generated gRPC types for `proto/ant.proto`, plus conversions to and from
//! the `ant_core` types they carry.

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

#[cfg(test)]
mod tests {
    use super::*;

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
