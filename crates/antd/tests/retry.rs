//! `RemoteObjectStore` against a server that rate-limits and sheds load: it
//! honours the retry hint (gRPC and HTTP) and gives up within its budget.

use std::time::{Duration, Instant};

use ant_auth::RateSpec;
use ant_core::{EncodedObject, ObjectKind};
use ant_proto::v1 as pb;
use ant_server::acl::Acl;
use ant_server::limits::LimitConfig;
use ant_server::metrics::Metrics;
use ant_server::serve::{LocalServer, spawn_local_with_limits};
use ant_store::{Backend, ObjectStore};
use antd::remote::RemoteObjectStore;
use antd::retry::RetryPolicy;
use tonic::Code;

fn blob(body: &str) -> EncodedObject {
    EncodedObject::new(ObjectKind::Blob, body.as_bytes())
}

async fn server(cfg: LimitConfig) -> (LocalServer, Backend, std::sync::Arc<Metrics>) {
    let backend = Backend::in_memory();
    let metrics = Metrics::new();
    let s =
        spawn_local_with_limits(backend.clone(), Acl::open(), metrics.clone(), cfg).await.unwrap();
    (s, backend, metrics)
}

fn read_rate(rps: f64, burst: f64) -> LimitConfig {
    let mut cfg = LimitConfig::default();
    cfg.anonymous[0] = Some(RateSpec { rps, burst });
    cfg
}

fn rejected(metrics: &Metrics, class: &str, reason: &str) -> u64 {
    let key = format!(r#"ant_ratelimit_rejected_total{{class="{class}",reason="{reason}"}} "#);
    metrics
        .render()
        .lines()
        .find_map(|l| l.strip_prefix(&key).and_then(|v| v.trim().parse().ok()))
        .unwrap()
}

#[tokio::test]
async fn grpc_calls_wait_out_rate_limits_and_succeed() {
    let (s, backend, metrics) = server(read_rate(5.0, 1.0)).await;
    let obj = blob("hello");
    backend.objects.put_many(std::slice::from_ref(&obj)).await.unwrap();
    let store = RemoteObjectStore::connect(&s.grpc_url(), None).await.unwrap();

    // Five back-to-back reads against a 5/s bucket: four are rejected at
    // first, all are eventually served, taking roughly 4 x 200 ms.
    let t = Instant::now();
    for _ in 0..5 {
        assert_eq!(store.has_many(&[obj.id()]).await.unwrap(), vec![true]);
    }
    let took = t.elapsed();
    assert!(took >= Duration::from_millis(600), "{took:?}");
    assert!(took < Duration::from_secs(10), "{took:?}");
    assert!(rejected(&metrics, "read", "rate") >= 3);
}

#[tokio::test]
async fn without_retries_the_rejection_surfaces() {
    let (s, _, _) = server(read_rate(0.5, 1.0)).await;
    let store =
        RemoteObjectStore::connect_with_retry(&s.grpc_url(), None, None, RetryPolicy::none())
            .await
            .unwrap();
    let mut c = store.client();
    c.list_refs(pb::ListRefsRequest { prefix: String::new() }).await.unwrap();
    let e = c.list_refs(pb::ListRefsRequest { prefix: String::new() }).await.unwrap_err();
    assert_eq!(e.code(), Code::ResourceExhausted);
    assert!(e.metadata().get("retry-after-ms").is_some());
}

#[tokio::test]
async fn gives_up_when_the_budget_is_spent() {
    // One token per 4 s; the budget allows ~300 ms of waiting.
    let (s, _, metrics) = server(read_rate(0.25, 1.0)).await;
    let policy = RetryPolicy {
        max_retries: 5,
        max_delay: Duration::from_millis(100),
        max_total: Duration::from_millis(300),
    };
    let store =
        RemoteObjectStore::connect_with_retry(&s.grpc_url(), None, None, policy).await.unwrap();
    let mut c = store.client();
    c.list_refs(pb::ListRefsRequest { prefix: String::new() }).await.unwrap();
    let t = Instant::now();
    let e = c.list_refs(pb::ListRefsRequest { prefix: String::new() }).await.unwrap_err();
    assert_eq!(e.code(), Code::ResourceExhausted);
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
    // It did try again before giving up.
    assert!(rejected(&metrics, "read", "rate") >= 2);
}

#[tokio::test]
async fn non_idempotent_writes_are_retried_safely_and_apply_once() {
    let mut cfg = LimitConfig::default();
    cfg.anonymous[1] = Some(RateSpec { rps: 5.0, burst: 1.0 });
    let (s, _, metrics) = server(cfg).await;
    let store = RemoteObjectStore::connect(&s.grpc_url(), None).await.unwrap();
    let mut c = store.client();
    let mut commits = Vec::new();
    for i in 0..4 {
        let r = c
            .create_changeset(pb::CreateChangesetRequest {
                base: None,
                ops: vec![pb::FileOp {
                    path: format!("f{i}"),
                    op: Some(pb::file_op::Op::Write(pb::WriteOp {
                        mode: pb::EntryMode::File as i32,
                        content: Some(pb::write_op::Content::Inline(b"x".to_vec())),
                    })),
                }],
                author: Some(pb::Signature {
                    name: "a".into(),
                    email: "a@x".into(),
                    time: 1_700_000_000,
                    tz_offset_minutes: 0,
                }),
                committer: None,
                message: format!("m{i}"),
            })
            .await
            .unwrap()
            .into_inner();
        commits.push(r.commit);
    }
    assert!(rejected(&metrics, "write", "rate") >= 2, "writes were rate limited");
    commits.sort();
    commits.dedup();
    assert_eq!(commits.len(), 4);
    // Landing goes through the same retrying channel (write bucket, drained).
    for commit in &commits[..1] {
        c.land(pb::LandRequest { ref_name: "main".into(), commit: commit.clone() }).await.unwrap();
    }
}

#[tokio::test]
async fn http_object_reads_retry_after_429() {
    let (s, backend, metrics) = server(read_rate(4.0, 1.0)).await;
    let objs: Vec<EncodedObject> = (0..4).map(|i| blob(&format!("object {i}"))).collect();
    backend.objects.put_many(&objs).await.unwrap();
    let store = RemoteObjectStore::connect(&s.grpc_url(), Some(&s.http_url())).await.unwrap();
    let ids: Vec<_> = objs.iter().map(EncodedObject::id).collect();

    let t = Instant::now();
    let got = store.get_many(&ids).await.unwrap();
    assert!(got.iter().all(Option::is_some));
    // Served over HTTP after waiting, not through the gRPC fallback: that
    // would have been limited (and retried) as well, but this counts requests.
    assert_eq!(store.http_requests().objects, 4);
    assert!(rejected(&metrics, "read", "rate") >= 2);
    assert!(t.elapsed() >= Duration::from_millis(400), "{:?}", t.elapsed());
}
