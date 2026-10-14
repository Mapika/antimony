//! gRPC helpers shared by the `fleet` and `soak` agents.

use std::time::{SystemTime, UNIX_EPOCH};

use ant_proto::MAX_MESSAGE_BYTES;
use ant_proto::v1 as pb;
use ant_proto::v1::ant_client::AntClient;
use anyhow::{Context as _, Result};
use rand::SeedableRng;
use rand::rngs::StdRng;
use tonic::transport::Channel;

pub type Client = AntClient<Channel>;

/// A client on `channel` accepting messages up to [`MAX_MESSAGE_BYTES`].
pub fn client(channel: Channel) -> Client {
    AntClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
}

pub async fn channel(url: &str) -> Result<Channel> {
    Channel::from_shared(url.to_owned())?
        .connect()
        .await
        .with_context(|| format!("connecting to {url}"))
}

pub async fn connect(url: &str) -> Result<Client> {
    Ok(client(channel(url).await?))
}

/// Signature `<name> <<name>@example.com>` at the current time.
pub fn sig(name: &str) -> pb::Signature {
    let time =
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
    pb::Signature {
        name: name.into(),
        email: format!("{name}@example.com").into(),
        time,
        tz_offset_minutes: 0,
    }
}

/// Write a regular file with inline content.
pub fn write_op(path: String, content: Vec<u8>) -> pb::FileOp {
    pb::FileOp {
        path,
        op: Some(pb::file_op::Op::Write(pb::WriteOp {
            mode: pb::EntryMode::File as i32,
            content: Some(pb::write_op::Content::Inline(content)),
        })),
    }
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Deterministic per-agent random stream for run seed `seed`.
pub fn agent_rng(seed: u64, agent: usize) -> StdRng {
    StdRng::seed_from_u64(seed.wrapping_mul(1_000_003).wrapping_add(agent as u64))
}

/// Create a root commit (no base) with `ops` and point the new ref
/// `ref_name` at it. Returns the commit and whether the ref was created
/// (false: it already existed).
pub async fn seed_ref(
    client: &mut Client,
    ref_name: &str,
    ops: Vec<pb::FileOp>,
    author: &str,
    message: &str,
) -> Result<(Vec<u8>, bool)> {
    let commit = client
        .create_changeset(pb::CreateChangesetRequest {
            base: None,
            ops,
            author: Some(sig(author)),
            committer: None,
            message: message.into(),
        })
        .await?
        .into_inner()
        .commit;
    let r = client
        .update_ref(pb::UpdateRefRequest {
            name: ref_name.into(),
            expected: None,
            new_target: Some(commit.clone()),
        })
        .await?
        .into_inner();
    Ok((commit, r.updated))
}

/// Read every `(path, expected content)` of `items` at `rev`, 64 requests at
/// a time, and hand each path, its expected content and the reply to `check`.
pub async fn read_back(
    client: &Client,
    rev: &str,
    items: &[(String, Vec<u8>)],
    mut check: impl FnMut(&str, &[u8], Result<pb::ReadPathResponse, tonic::Status>),
) -> Result<()> {
    for chunk in items.chunks(64) {
        let mut set = tokio::task::JoinSet::new();
        for (path, content) in chunk.iter().cloned() {
            let mut c = client.clone();
            let rev = rev.to_owned();
            set.spawn(async move {
                let r = c
                    .read_path(pb::ReadPathRequest {
                        rev,
                        path: path.clone(),
                        include_content: true,
                    })
                    .await
                    .map(|r| r.into_inner());
                (path, content, r)
            });
        }
        while let Some(joined) = set.join_next().await {
            let (path, content, r) = joined?;
            check(&path, &content, r);
        }
    }
    Ok(())
}
