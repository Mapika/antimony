//! S3-compatible `ObjectStore` for large objects.
//!
//! - Key layout: `{prefix}/{hex[0..2]}/{hex}` where hex is the object id.
//! - Values are the zstd-compressed (level 3) encoded object bytes. The hash
//!   is verified after decompressing on `get_many`; a mismatch is a
//!   `Backend` error.
//! - `get_many`/`has_many`/`put_many` fan out with bounded concurrency
//!   (`futures::stream::iter(..).buffered(64)`), preserving input order.
//! - `put_many` skips ids repeated within the batch; a plain PUT is
//!   idempotent (content is identical by construction).
//! - A missing key (`NoSuchKey` / 404) is `None`, not an error. `has_many`
//!   uses HEAD.
//! - `connect` builds a client with `force_path_style(true)` (SeaweedFS/MinIO
//!   need it), static credentials, the given region and endpoint.
//! - `ensure_bucket` creates the bucket if it does not exist ("already owned
//!   by you" counts as success).

use std::collections::HashSet;

use ant_core::{EncodedObject, ObjectId};
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};

use crate::{ObjectStore, Result, StoreError};

/// Maximum number of concurrent S3 requests per batch call.
const FANOUT: usize = 64;
/// zstd level for stored object bodies.
const ZSTD_LEVEL: i32 = 3;
/// Bodies larger than this are compressed/decompressed/hashed on the blocking
/// pool instead of an async worker thread.
const BLOCKING_THRESHOLD: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
}

impl S3Config {
    /// Local S3 (SeaweedFS) from docker-compose.yml, overridable via `ANT_TEST_S3_*`.
    pub fn for_tests(prefix: &str) -> Self {
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        Self {
            endpoint: env("ANT_TEST_S3_ENDPOINT", "http://localhost:9009"),
            region: "us-east-1".into(),
            bucket: env("ANT_TEST_S3_BUCKET", "ant-test"),
            prefix: prefix.to_string(),
            access_key: env("ANT_TEST_S3_ACCESS_KEY", "ant"),
            secret_key: env("ANT_TEST_S3_SECRET_KEY", "ant"),
        }
    }
}

#[derive(Clone)]
pub struct S3ObjectStore {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

fn corrupt(msg: impl Into<String>) -> StoreError {
    StoreError::Backend(msg.into().into())
}

impl S3ObjectStore {
    pub async fn connect(cfg: &S3Config) -> Result<Self> {
        let credentials = aws_sdk_s3::config::Credentials::new(
            &cfg.access_key,
            &cfg.secret_key,
            None,
            None,
            "ant-static",
        );
        let conf = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new(cfg.region.clone()))
            .endpoint_url(&cfg.endpoint)
            .credentials_provider(credentials)
            .force_path_style(true)
            .build();
        Ok(Self {
            client: aws_sdk_s3::Client::from_conf(conf),
            bucket: cfg.bucket.clone(),
            prefix: cfg.prefix.clone(),
        })
    }

    pub async fn ensure_bucket(&self) -> Result<()> {
        match self.client.create_bucket().bucket(&self.bucket).send().await {
            Ok(_) => Ok(()),
            // SeaweedFS (no identities configured) reports an existing bucket as
            // `BucketAlreadyExists` rather than `BucketAlreadyOwnedByYou`; both mean
            // the bucket is usable, so creation is idempotent.
            Err(e) => match e.as_service_error() {
                Some(s) if s.is_bucket_already_owned_by_you() || s.is_bucket_already_exists() => {
                    Ok(())
                }
                _ => Err(StoreError::backend(e)),
            },
        }
    }

    /// Object key for `id`.
    pub fn key(&self, id: &ObjectId) -> String {
        let hex = id.to_hex();
        if self.prefix.is_empty() {
            format!("{}/{}", &hex[..2], hex)
        } else {
            format!("{}/{}/{}", self.prefix.trim_end_matches('/'), &hex[..2], hex)
        }
    }

    async fn get_one(&self, id: ObjectId) -> Result<Option<EncodedObject>> {
        let resp =
            match self.client.get_object().bucket(&self.bucket).key(self.key(&id)).send().await {
                Ok(r) => r,
                Err(e) => {
                    if e.as_service_error().is_some_and(|s| s.is_no_such_key()) {
                        return Ok(None);
                    }
                    return Err(StoreError::backend(e));
                }
            };
        let compressed: Bytes =
            resp.body.collect().await.map_err(StoreError::backend)?.into_bytes();
        let compressed_len = compressed.len();
        let decode = move || -> Result<EncodedObject> {
            let raw = zstd::stream::decode_all(&compressed[..]).map_err(StoreError::backend)?;
            let obj = EncodedObject::from_bytes(Bytes::from(raw))
                .map_err(|e| corrupt(format!("object {id}: {e}")))?;
            obj.verify(&id).map_err(|e| corrupt(format!("object {id}: {e}")))?;
            Ok(obj)
        };
        // Large bodies are CPU-heavy to decompress and hash; keep them off the
        // async worker threads.
        let obj = if compressed_len > BLOCKING_THRESHOLD {
            tokio::task::spawn_blocking(decode).await.map_err(StoreError::backend)??
        } else {
            decode()?
        };
        Ok(Some(obj))
    }

    async fn put_one(&self, id: ObjectId, obj: &EncodedObject) -> Result<()> {
        let compressed = if obj.as_bytes().len() > BLOCKING_THRESHOLD {
            let body = obj.clone();
            tokio::task::spawn_blocking(move || {
                zstd::stream::encode_all(&body.as_bytes()[..], ZSTD_LEVEL)
            })
            .await
            .map_err(StoreError::backend)?
            .map_err(StoreError::backend)?
        } else {
            zstd::stream::encode_all(&obj.as_bytes()[..], ZSTD_LEVEL)
                .map_err(StoreError::backend)?
        };
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(self.key(&id))
            .body(ByteStream::from(compressed))
            .send()
            .await
            .map_err(StoreError::backend)?;
        Ok(())
    }

    async fn has_one(&self, id: ObjectId) -> Result<bool> {
        match self.client.head_object().bucket(&self.bucket).key(self.key(&id)).send().await {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.as_service_error().is_some_and(|s| s.is_not_found()) {
                    Ok(false)
                } else {
                    Err(StoreError::backend(e))
                }
            }
        }
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        stream::iter(ids.iter().copied())
            .map(|id| self.get_one(id))
            .buffered(FANOUT)
            .try_collect()
            .await
    }

    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        let mut seen = HashSet::with_capacity(objects.len());
        // `EncodedObject` clones are cheap (shared `Bytes`); owning the pairs
        // keeps the buffered stream's closure free of higher-ranked borrows.
        let unique: Vec<(ObjectId, EncodedObject)> = objects
            .iter()
            .filter_map(|o| {
                let id = o.id();
                seen.insert(id).then(|| (id, o.clone()))
            })
            .collect();
        stream::iter(unique)
            .map(|(id, o)| async move { self.put_one(id, &o).await })
            .buffered(FANOUT)
            .try_collect::<Vec<()>>()
            .await?;
        Ok(())
    }

    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        stream::iter(ids.iter().copied())
            .map(|id| self.has_one(id))
            .buffered(FANOUT)
            .try_collect()
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::conformance;

    async fn fresh() -> S3ObjectStore {
        let cfg = S3Config::for_tests(&format!("t{}", rand::random::<u64>()));
        let s = S3ObjectStore::connect(&cfg).await.expect("connect");
        s.ensure_bucket().await.expect("bucket (is `docker compose up -d` running?)");
        s
    }

    #[tokio::test]
    #[ignore = "needs s3: docker compose up -d"]
    async fn s3_object_store_conforms() {
        conformance::object_store(Arc::new(fresh().await)).await;
    }

    #[tokio::test]
    #[ignore = "needs s3: docker compose up -d"]
    async fn key_layout() {
        let s = fresh().await;
        let id = ObjectId([0xab; 32]);
        let k = s.key(&id);
        assert!(k.ends_with(&format!("/ab/{}", id.to_hex())), "{k}");
        assert!(k.starts_with(&s.prefix));
    }
}
