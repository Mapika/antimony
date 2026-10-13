//! Helpers shared by the `ops` unit tests.

use ant_core::{EntryMode, ObjectId, RepoPath, Signature};
use ant_store::graph::lookup_path;
use ant_store::{Backend, ObjectStoreExt};
use bytes::Bytes;

use super::OpResult;
use super::changeset::{ChangesetRequest, FileOp, WriteContent, create_changeset};

pub fn sig() -> Signature {
    Signature {
        name: "agent".into(),
        email: "a@x".into(),
        time: 1_700_000_000,
        tz_offset_minutes: 0,
    }
}

pub fn write(path: &str, content: &str) -> FileOp {
    FileOp::Write {
        path: RepoPath::new(path).unwrap(),
        mode: EntryMode::File,
        content: WriteContent::Inline(Bytes::copy_from_slice(content.as_bytes())),
    }
}

pub fn delete(path: &str) -> FileOp {
    FileOp::Delete { path: RepoPath::new(path).unwrap() }
}

pub async fn commit(b: &Backend, base: Option<ObjectId>, ops: Vec<FileOp>) -> OpResult<ObjectId> {
    create_changeset(
        b,
        ChangesetRequest { base, ops, author: sig(), committer: None, message: "msg\n".into() },
    )
    .await
}

pub async fn read(b: &Backend, commit_id: ObjectId, path: &str) -> Option<String> {
    let c = b.objects.read_commit(&commit_id).await.unwrap();
    let (mode, id) =
        lookup_path(b.objects.as_ref(), c.tree, &RepoPath::new(path).unwrap()).await.unwrap()?;
    assert!(mode.is_blob(), "{path} is not a blob");
    Some(String::from_utf8(b.objects.read_blob(&id).await.unwrap().to_vec()).unwrap())
}
