//! Rate limits and load shedding (`ant_server::limits`) through the real gRPC
//! and HTTP front ends, against in-memory backends on ephemeral ports.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ant_auth::{Policy, RateSpec, hash_token};
use ant_core::{EncodedObject, ObjectId, ObjectKind};
use ant_proto::v1 as pb;
use ant_proto::v1::ant_client::AntClient;
use ant_server::acl::Acl;
use ant_server::limits::{LimitConfig, RETRY_AFTER_MS};
use ant_server::metrics::Metrics;
use ant_server::serve::{LocalServer, spawn_local_with_limits};
use ant_store::Backend;
use common::wire::{hex, sig, write_file};
use tonic::Code;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

#[derive(Clone)]
struct Auth(Option<MetadataValue<Ascii>>);

impl Interceptor for Auth {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(v) = &self.0 {
            req.metadata_mut().insert("authorization", v.clone());
        }
        Ok(req)
    }
}

type Client = AntClient<InterceptedService<Channel, Auth>>;

struct Server {
    backend: Backend,
    addrs: LocalServer,
    metrics: Arc<Metrics>,
}

impl Server {
    async fn start(acl: Acl, cfg: LimitConfig) -> Self {
        let backend = Backend::in_memory();
        let metrics = Metrics::new();
        let addrs =
            spawn_local_with_limits(backend.clone(), acl, metrics.clone(), cfg).await.unwrap();
        Self { backend, addrs, metrics }
    }

    async fn client(&self, token: Option<&str>) -> Client {
        let channel = Channel::from_shared(self.addrs.grpc_url()).unwrap().connect().await.unwrap();
        let auth = Auth(token.map(|t| format!("Bearer {t}").parse().unwrap()));
        AntClient::with_interceptor(channel, auth)
    }

    fn rejected(&self, class: &str, reason: &str) -> u64 {
        let text = self.metrics.render();
        let key = format!(r#"ant_ratelimit_rejected_total{{class="{class}",reason="{reason}"}} "#);
        text.lines()
            .find_map(|l| l.strip_prefix(&key))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or_else(|| panic!("no {key} in\n{text}"))
    }

    fn gauge(&self, name: &str, class: &str) -> i64 {
        let text = self.metrics.render();
        let key = format!(r#"{name}{{class="{class}"}} "#);
        text.lines()
            .find_map(|l| l.strip_prefix(&key))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or_else(|| panic!("no {key} in\n{text}"))
    }
}

fn retry_ms(status: &tonic::Status) -> u64 {
    status
        .metadata()
        .get(RETRY_AFTER_MS)
        .unwrap_or_else(|| panic!("no {RETRY_AFTER_MS} in {status:?}"))
        .to_str()
        .unwrap()
        .parse()
        .unwrap()
}

fn list_refs() -> pb::ListRefsRequest {
    pb::ListRefsRequest { prefix: String::new() }
}

#[tokio::test]
async fn rate_limit_round_trip_grpc_and_http() {
    let mut cfg = LimitConfig::default();
    cfg.anonymous[0] = Some(RateSpec { rps: 2.0, burst: 2.0 });
    let s = Server::start(Acl::open(), cfg).await;
    let mut c = s.client(None).await;
    let http = reqwest::Client::new();

    // The burst passes, the next read is rejected with a hint.
    c.list_refs(list_refs()).await.unwrap();
    c.list_refs(list_refs()).await.unwrap();
    let e = c.list_refs(list_refs()).await.unwrap_err();
    assert_eq!(e.code(), Code::ResourceExhausted, "{e:?}");
    let ms = retry_ms(&e);
    assert!((1..=500).contains(&ms), "{ms}");
    // The HTTP side shares the bucket (same client IP).
    let url = format!("{}/o/{}", s.addrs.http_url(), "00".repeat(32));
    let r = http.get(&url).send().await.unwrap();
    assert_eq!(r.status(), 429);
    assert_eq!(r.headers()["retry-after"], "1");
    assert!(r.headers()[RETRY_AFTER_MS].to_str().unwrap().parse::<u64>().unwrap() >= 1);
    assert_eq!(r.headers()["cache-control"], "no-store");
    // Health, readiness and metrics are never limited.
    for path in ["healthz", "readyz", "metrics"] {
        let r = http.get(format!("{}/{path}", s.addrs.http_url())).send().await.unwrap();
        assert_eq!(r.status(), 200, "{path}");
    }
    // Other classes have their own bucket.
    let e = c.get_ref(pb::GetRefRequest { name: "nope".into() }).await.unwrap_err();
    assert_eq!(e.code(), Code::ResourceExhausted, "read class: {e:?}");
    c.create_changeset(pb::CreateChangesetRequest {
        base: None,
        ops: vec![write_file("a", "1")],
        author: Some(sig("t")),
        committer: None,
        message: "m".into(),
    })
    .await
    .unwrap();
    // After the hinted wait the caller is served again.
    tokio::time::sleep(Duration::from_millis(ms + 50)).await;
    c.list_refs(list_refs()).await.unwrap();
    assert!(s.rejected("read", "rate") >= 3);
    assert_eq!(s.rejected("write", "rate"), 0);
}

fn policy_with_hostile_limit() -> Acl {
    let text = format!(
        r#"
        [[principal]]
        name = "victim"
        token_sha256 = "{}"
        groups = ["agents"]

        [[principal]]
        name = "hostile"
        token_sha256 = "{}"
        groups = ["agents"]
        [principal.limits]
        read = {{ rps = 50, burst = 50 }}

        [[principal]]
        name = "bulk"
        token_sha256 = "{}"
        groups = ["agents"]
        [principal.limits]
        read = {{ rps = 0 }}

        [[path]]
        prefix = ""
        read = ["*"]
        write = ["group:agents"]
        "#,
        hex(&hash_token("victim")),
        hex(&hash_token("hostile")),
        hex(&hash_token("bulk")),
    );
    Acl::new(Policy::parse(&text).unwrap())
}

async fn victim_latencies(c: &mut Client, n: usize) -> (Vec<Duration>, usize) {
    let mut lat = Vec::new();
    let mut errors = 0;
    for _ in 0..n {
        let t = Instant::now();
        match c.list_refs(list_refs()).await {
            Ok(_) => lat.push(t.elapsed()),
            Err(_) => errors += 1,
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    lat.sort();
    (lat, errors)
}

fn p99(lat: &[Duration]) -> Duration {
    lat[(lat.len() * 99 / 100).min(lat.len() - 1)]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hostile_principal_does_not_slow_others() {
    let s = Server::start(policy_with_hostile_limit(), LimitConfig::default()).await;
    let mut victim = s.client(Some("victim")).await;
    let (base, errors) = victim_latencies(&mut victim, 200).await;
    assert_eq!(errors, 0);
    let base99 = p99(&base);

    let stop = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicU64::new(0));
    let ok = Arc::new(AtomicU64::new(0));
    let mut flooders = Vec::new();
    for _ in 0..8 {
        let mut c = s.client(Some("hostile")).await;
        let (stop, sent, ok) = (stop.clone(), sent.clone(), ok.clone());
        flooders.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                sent.fetch_add(1, Ordering::Relaxed);
                match c.list_refs(list_refs()).await {
                    Ok(_) => {
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => assert_eq!(e.code(), Code::ResourceExhausted, "{e:?}"),
                }
            }
        }));
    }
    let started = Instant::now();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (flooded, errors) = victim_latencies(&mut victim, 200).await;
    stop.store(true, Ordering::Relaxed);
    for f in flooders {
        f.await.unwrap();
    }
    let secs = started.elapsed().as_secs_f64();
    let flood99 = p99(&flooded);
    eprintln!(
        "victim p99: baseline {base99:?}, under flood {flood99:?}; hostile sent {} accepted {}",
        sent.load(Ordering::Relaxed),
        ok.load(Ordering::Relaxed)
    );

    assert_eq!(errors, 0, "the victim is never rejected");
    assert!(
        flood99 < base99 * 10 + Duration::from_millis(100),
        "victim p99 {flood99:?} vs baseline {base99:?}"
    );
    // The hostile principal got at most its budget (burst + rate x time).
    let budget = 50.0 + 50.0 * secs;
    let accepted = ok.load(Ordering::Relaxed) as f64;
    assert!(accepted <= budget * 1.1 + 5.0, "accepted {accepted}, budget {budget}");
    assert!(sent.load(Ordering::Relaxed) as f64 > accepted * 2.0, "flood was mostly rejected");
    assert!(s.rejected("read", "rate") > 100);

    // A principal with `rps = 0` is unlimited.
    let mut bulk = s.client(Some("bulk")).await;
    for _ in 0..300 {
        bulk.list_refs(list_refs()).await.unwrap();
    }
}

/// A blob of 1 MiB per id: a `GetObjects` stream the client does not read
/// stalls on HTTP/2 flow control and so keeps its concurrency slot.
async fn big_blobs(s: &Server, n: usize) -> Vec<Vec<u8>> {
    let objs: Vec<EncodedObject> = (0..n)
        .map(|i| {
            let mut data = vec![b'x'; 1 << 20];
            data[..8].copy_from_slice(&(i as u64).to_le_bytes());
            EncodedObject::new(ObjectKind::Blob, &data)
        })
        .collect();
    s.backend.objects.put_many(&objs).await.unwrap();
    objs.iter().map(|o| o.id().0.to_vec()).collect()
}

fn get_objects(ids: &[Vec<u8>]) -> pb::GetObjectsRequest {
    pb::GetObjectsRequest { ids: ids.to_vec() }
}

#[tokio::test]
async fn full_concurrency_gate_sheds_with_retry_after_and_recovers() {
    let mut cfg = LimitConfig::default();
    cfg.max_inflight[0] = 2;
    cfg.queue[0] = 0;
    let s = Server::start(Acl::open(), cfg).await;
    let ids = big_blobs(&s, 8).await;
    let mut c = s.client(None).await;

    // Two unread streams hold both read slots.
    let s1 = c.get_objects(get_objects(&ids)).await.unwrap();
    let s2 = c.get_objects(get_objects(&ids)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(s.gauge("ant_ratelimit_inflight", "read"), 2);
    assert_eq!(s.gauge("ant_ratelimit_inflight_limit", "read"), 2);

    // Everything read-class is shed at once...
    let t = Instant::now();
    let e = c.list_refs(list_refs()).await.unwrap_err();
    assert_eq!(e.code(), Code::Unavailable, "{e:?}");
    assert!(retry_ms(&e) >= 50);
    assert!(t.elapsed() < Duration::from_millis(500), "shed without waiting");
    let http = reqwest::Client::new();
    let r = http.get(format!("{}/o/{}", s.addrs.http_url(), "00".repeat(32))).send().await.unwrap();
    assert_eq!(r.status(), 503);
    assert!(r.headers().contains_key("retry-after"));
    // ...while writes and lands have their own gates (the pool is not shared).
    c.create_changeset(pb::CreateChangesetRequest {
        base: None,
        ops: vec![write_file("a", "1")],
        author: Some(sig("t")),
        committer: None,
        message: "m".into(),
    })
    .await
    .unwrap();
    assert_eq!(s.rejected("read", "queue_full"), 2);

    // Slots come back when the streams end.
    drop(s1);
    drop(s2);
    let mut served = false;
    for _ in 0..50 {
        if c.list_refs(list_refs()).await.is_ok() {
            served = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(served, "read slots were not released");
    assert_eq!(s.gauge("ant_ratelimit_inflight", "read"), 0);
}

#[tokio::test]
async fn queued_request_waits_for_a_slot_then_times_out() {
    let mut cfg = LimitConfig::default();
    cfg.max_inflight[0] = 1;
    cfg.queue[0] = 1;
    cfg.shed_wait = Duration::from_millis(300);
    let s = Server::start(Acl::open(), cfg).await;
    let ids = big_blobs(&s, 8).await;
    let mut c = s.client(None).await;
    let held = c.get_objects(get_objects(&ids)).await.unwrap();

    // Queued, never served: shed after about shed_wait with a hint.
    let t = Instant::now();
    let e = c.list_refs(list_refs()).await.unwrap_err();
    let waited = t.elapsed();
    assert_eq!(e.code(), Code::Unavailable);
    assert!(waited >= Duration::from_millis(250), "{waited:?}");
    assert_eq!(s.rejected("read", "queue_timeout"), 1);

    // Queued, then served when the slot frees up in time.
    let mut c2 = c.clone();
    let waiter = tokio::spawn(async move { c2.list_refs(list_refs()).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(s.gauge("ant_ratelimit_queued", "read"), 1);
    drop(held);
    waiter.await.unwrap().unwrap();
    assert_eq!(s.gauge("ant_ratelimit_queued", "read"), 0);
}

/// Landing keeps group-committing under concurrency limits: many agents
/// landing on one ref at once are all accepted and batched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_lands_still_batch_under_default_limits() {
    let s = Server::start(Acl::open(), LimitConfig::default()).await;
    let mut c = s.client(None).await;
    let root = c
        .create_changeset(pb::CreateChangesetRequest {
            base: None,
            ops: vec![write_file("root", "r")],
            author: Some(sig("t")),
            committer: None,
            message: "root".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .commit;
    c.land(pb::LandRequest { ref_name: "main".into(), commit: root.clone() }).await.unwrap();

    const AGENTS: usize = 96;
    let mut tasks = Vec::new();
    for i in 0..AGENTS {
        let mut c = s.client(None).await;
        let root = root.clone();
        tasks.push(tokio::spawn(async move {
            let commit = c
                .create_changeset(pb::CreateChangesetRequest {
                    base: Some(root),
                    ops: vec![write_file(&format!("agents/{i}"), "x")],
                    author: Some(sig("t")),
                    committer: None,
                    message: format!("c{i}"),
                })
                .await
                .unwrap()
                .into_inner()
                .commit;
            c.land(pb::LandRequest { ref_name: "main".into(), commit }).await.unwrap()
        }));
    }
    for t in tasks {
        match t.await.unwrap().into_inner().outcome {
            Some(pb::land_response::Outcome::Landed(_)) => {}
            other => panic!("not landed: {other:?}"),
        }
    }
    let text = s.metrics.render();
    let value = |name: &str| -> f64 {
        text.lines()
            .find_map(|l| l.strip_prefix(&format!("{name} ")))
            .map(|v| v.trim().parse().unwrap())
            .unwrap_or_else(|| panic!("no {name}"))
    };
    let lands = value("ant_land_batch_size_sum");
    let batches = value("ant_land_batch_size_count");
    assert!(lands >= AGENTS as f64);
    assert!(batches < lands, "no batching: {batches} batches for {lands} lands");
    for class in ["read", "write", "land"] {
        for reason in ["rate", "queue_full", "queue_timeout"] {
            assert_eq!(s.rejected(class, reason), 0, "{class} {reason}");
        }
    }
    let tip = c.get_ref(pb::GetRefRequest { name: "main".into() }).await.unwrap().into_inner();
    assert!(tip.target.is_some());
    let _ = ObjectId::from_bytes(&tip.target.unwrap()).unwrap();
}

#[tokio::test]
async fn no_limits_switch_disables_everything() {
    let mut cfg = LimitConfig::disabled();
    cfg.anonymous[0] = Some(RateSpec { rps: 1.0, burst: 1.0 });
    cfg.max_inflight[0] = 1;
    let s = Server::start(Acl::open(), cfg).await;
    let mut c = s.client(None).await;
    for _ in 0..50 {
        c.list_refs(list_refs()).await.unwrap();
    }
}
