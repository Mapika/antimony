//! Protobuf request builders for the gRPC transport tests.

use ant_core::ObjectId;
use ant_proto::v1 as pb;

pub fn sig(name: &str) -> pb::Signature {
    pb::Signature {
        name: name.into(),
        email: format!("{name}@example.com").into_bytes(),
        time: 1_700_000_000,
        tz_offset_minutes: 0,
    }
}

fn write(path: &str, content: pb::write_op::Content) -> pb::FileOp {
    pb::FileOp {
        path: path.into(),
        op: Some(pb::file_op::Op::Write(pb::WriteOp {
            mode: pb::EntryMode::File as i32,
            content: Some(content),
        })),
    }
}

/// Write `content` inline as a regular file.
pub fn write_file(path: &str, content: &str) -> pb::FileOp {
    write(path, pb::write_op::Content::Inline(content.as_bytes().to_vec()))
}

/// Write an uploaded blob as a regular file.
pub fn blob_file(path: &str, id: ObjectId) -> pb::FileOp {
    write(path, pb::write_op::Content::BlobId(id.0.to_vec()))
}

pub fn delete_file(path: &str) -> pb::FileOp {
    pb::FileOp { path: path.into(), op: Some(pb::file_op::Op::Delete(pb::DeleteOp {})) }
}

/// Lowercase hex of raw id bytes, as revs are spelled.
pub fn hex(id: &[u8]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}
