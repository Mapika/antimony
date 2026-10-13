//! S3-compatible `ObjectStore` for large objects.
//!
//! TASK (store-s3): implement everything marked `todo!()`.
//!
//! Requirements:
//! - Key layout: `{prefix}/{hex[0..2]}/{hex}` where hex is the object id.
//! - Values are the zstd-compressed (level 3) encoded object bytes. Verify the
//!   hash after decompressing on `get_many`; a mismatch is a `Backend` error.
//! - `get_many`/`has_many`/`put_many` fan out with bounded concurrency
//!   (`futures::stream::iter(..).buffered(64)`), preserving input order.
//! - `put_many`: skip ids already present in the same batch; a plain PUT is
//!   fine for idempotency (content is identical by construction).
//! - A missing key (`NoSuchKey` / 404) is `None`, not an error. `has_many`
//!   uses HEAD.
//! - `connect` builds a client with `force_path_style(true)` (SeaweedFS/MinIO need
//!   it), static credentials, the given region and endpoint.
//! - `ensure_bucket` creates the bucket if it does not exist (treat
//!   "already owned by you" as success).

use ant_core::{EncodedObject, ObjectId};
use async_trait::async_trait;

use crate::{ObjectStore, Result};

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
    #[allow(dead_code)]
    client: aws_sdk_s3::Client,
    #[allow(dead_code)]
    bucket: String,
    #[allow(dead_code)]
    prefix: String,
}

impl S3ObjectStore {
    pub async fn connect(cfg: &S3Config) -> Result<Self> {
        let _ = cfg;
        todo!("store-s3")
    }

    pub async fn ensure_bucket(&self) -> Result<()> {
        todo!("store-s3")
    }

    /// Object key for `id`.
    pub fn key(&self, id: &ObjectId) -> String {
        let _ = id;
        todo!("store-s3")
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn get_many(&self, ids: &[ObjectId]) -> Result<Vec<Option<EncodedObject>>> {
        let _ = ids;
        todo!("store-s3")
    }
    async fn put_many(&self, objects: &[EncodedObject]) -> Result<()> {
        let _ = objects;
        todo!("store-s3")
    }
    async fn has_many(&self, ids: &[ObjectId]) -> Result<Vec<bool>> {
        let _ = ids;
        todo!("store-s3")
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
