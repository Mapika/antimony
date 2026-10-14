#![allow(dead_code)]
//! Cleanup helper for soak runs: deletes everything a run wrote to S3.
//!
//! The client is built exactly like `ant_store::s3::S3ObjectStore::connect`
//! (static credentials, region, endpoint, path-style addressing).

use anyhow::{Context, Result, bail};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};

use ant_store::s3::S3Config;

/// Maximum keys per `DeleteObjects` request (S3 limit).
const DELETE_BATCH: usize = 1000;

fn client(cfg: &S3Config) -> aws_sdk_s3::Client {
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
    aws_sdk_s3::Client::from_conf(conf)
}

/// Delete every object under `cfg.prefix` in `cfg.bucket` (all objects in the bucket when the prefix is empty), then
/// try to delete the bucket itself when `delete_bucket` is true (ignore a BucketNotEmpty/any error on the bucket
/// delete, but return it in the count's Err only if listing/deleting objects fails). Returns the number of objects deleted.
pub async fn delete_prefix(cfg: &S3Config, delete_bucket: bool) -> Result<u64> {
    let client = client(cfg);
    // Trailing slash so that prefix "run" does not also match sibling "run2/...".
    let prefix = match cfg.prefix.trim_end_matches('/') {
        "" => String::new(),
        p => format!("{p}/"),
    };

    let mut deleted = 0u64;
    let mut token: Option<String> = None;
    loop {
        let mut req = client.list_objects_v2().bucket(&cfg.bucket).prefix(&prefix);
        if let Some(t) = token.take() {
            req = req.continuation_token(t);
        }
        let page = match req.send().await {
            Ok(p) => p,
            Err(e) if e.code() == Some("NoSuchBucket") => return Ok(0),
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("listing objects in bucket {} prefix {prefix:?}", cfg.bucket)
                });
            }
        };

        let keys: Vec<String> =
            page.contents().iter().filter_map(|o| o.key().map(str::to_owned)).collect();
        for chunk in keys.chunks(DELETE_BATCH) {
            deleted += delete_batch(&client, &cfg.bucket, chunk).await?;
        }

        match page.next_continuation_token() {
            Some(t) if page.is_truncated().unwrap_or(false) => token = Some(t.to_owned()),
            _ => break,
        }
    }

    if delete_bucket {
        // Best effort: the bucket may be non-empty (other prefixes) or already gone.
        let _ = client.delete_bucket().bucket(&cfg.bucket).send().await;
    }
    Ok(deleted)
}

async fn delete_batch(client: &aws_sdk_s3::Client, bucket: &str, keys: &[String]) -> Result<u64> {
    if keys.is_empty() {
        return Ok(0);
    }
    let objects = keys
        .iter()
        .map(|k| {
            ObjectIdentifier::builder()
                .key(k)
                .build()
                .with_context(|| format!("building object identifier for key {k:?}"))
        })
        .collect::<Result<Vec<_>>>()?;
    let delete =
        Delete::builder().set_objects(Some(objects)).build().context("building delete request")?;
    let out = client
        .delete_objects()
        .bucket(bucket)
        .delete(delete)
        .send()
        .await
        .with_context(|| format!("deleting {} objects in bucket {bucket}", keys.len()))?;
    if !out.errors().is_empty() {
        let first = &out.errors()[0];
        bail!(
            "{} of {} deletes failed in bucket {bucket}; first: key {:?}: {:?} {:?}",
            out.errors().len(),
            keys.len(),
            first.key(),
            first.code(),
            first.message()
        );
    }
    Ok(out.deleted().len() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_core::{EncodedObject, ObjectKind};
    use ant_store::ObjectStore;
    use ant_store::s3::S3ObjectStore;

    #[tokio::test]
    #[ignore = "needs S3 (scripts/dev-services.sh)"]
    async fn s3_delete_prefix_removes_objects() {
        let cfg = S3Config::for_tests(&format!("s3clean-{}", rand::random::<u64>()));
        let store = S3ObjectStore::connect(&cfg).await.expect("connect");
        store.ensure_bucket().await.expect("bucket");

        let objs: Vec<EncodedObject> = (0..3)
            .map(|i| EncodedObject::new(ObjectKind::Blob, format!("s3clean blob {i}\n").as_bytes()))
            .collect();
        let ids: Vec<_> = objs.iter().map(|o| o.id()).collect();
        store.put_many(&objs).await.expect("put_many");
        assert_eq!(store.has_many(&ids).await.expect("has_many"), vec![true; 3]);

        assert_eq!(delete_prefix(&cfg, false).await.expect("delete_prefix"), 3);
        assert_eq!(store.has_many(&ids).await.expect("has_many"), vec![false; 3]);
        assert_eq!(delete_prefix(&cfg, false).await.expect("delete_prefix again"), 0);
    }
}
