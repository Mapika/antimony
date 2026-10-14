//! Metrics, request ids and slow-request logs end to end (`ant_server::metrics`,
//! `ant_server::observe`), against in-memory backends on ephemeral ports.

use std::collections::HashSet;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ant_proto::v1 as pb;
use ant_proto::v1::ant_client::AntClient;
use ant_server::acl::Acl;
use ant_server::metrics::Metrics;
use ant_server::observe::{Observer, http_observe};
use ant_server::serve::{LocalServer, spawn_local_with_metrics};
use ant_store::Backend;
use prometheus_parse::{Scrape, Value};
use tonic::transport::Channel;

struct Server {
    grpc: AntClient<Channel>,
    addrs: LocalServer,
    http: reqwest::Client,
    metrics: Arc<Metrics>,
}

async fn spawn_server() -> Server {
    let metrics = Metrics::new();
    let addrs =
        spawn_local_with_metrics(Backend::in_memory(), Acl::open(), metrics.clone()).await.unwrap();
    Server {
        grpc: AntClient::connect(addrs.grpc_url()).await.unwrap(),
        addrs,
        http: reqwest::Client::new(),
        metrics,
    }
}

fn sig(name: &str) -> pb::Signature {
    pb::Signature {
        name: name.into(),
        email: format!("{name}@example.com").into_bytes(),
        time: 1_700_000_000,
        tz_offset_minutes: 0,
    }
}

fn write_file(path: &str, content: &str) -> pb::FileOp {
    pb::FileOp {
        path: path.into(),
        op: Some(pb::file_op::Op::Write(pb::WriteOp {
            mode: pb::EntryMode::File as i32,
            content: Some(pb::write_op::Content::Inline(content.as_bytes().to_vec())),
        })),
    }
}

async fn commit(s: &mut Server, base: Option<Vec<u8>>, ops: Vec<pb::FileOp>) -> Vec<u8> {
    s.grpc
        .create_changeset(pb::CreateChangesetRequest {
            base,
            ops,
            author: Some(sig("alice")),
            committer: None,
            message: "m".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .commit
}

async fn land(s: &mut Server, commit: &[u8]) -> pb::LandResponse {
    s.grpc
        .land(pb::LandRequest { ref_name: "main".into(), commit: commit.to_vec() })
        .await
        .unwrap()
        .into_inner()
}

/// create, land, rebased land, conflict, object reads, git clone.
async fn scripted_session(s: &mut Server) {
    let base = commit(s, None, vec![write_file("a.txt", "one\n")]).await;
    land(s, &base).await;
    let left = commit(s, Some(base.clone()), vec![write_file("left.txt", "l")]).await;
    let right = commit(s, Some(base.clone()), vec![write_file("right.txt", "r")]).await;
    land(s, &left).await;
    land(s, &right).await; // rebased
    let c1 = commit(s, Some(base.clone()), vec![write_file("a.txt", "x\n")]).await;
    let c2 = commit(s, Some(base.clone()), vec![write_file("a.txt", "y\n")]).await;
    land(s, &c1).await;
    let conflict = land(s, &c2).await;
    assert!(matches!(conflict.outcome, Some(pb::land_response::Outcome::Conflict(_))));

    // Object reads over gRPC and HTTP, an error, and the other routes.
    let tip = s
        .grpc
        .resolve_rev(pb::ResolveRevRequest { rev: "main".into() })
        .await
        .unwrap()
        .into_inner()
        .commit;
    let hex: String = tip.iter().map(|b| format!("{b:02x}")).collect();
    let url = s.addrs.http_url();
    assert_eq!(s.http.get(format!("{url}/o/{hex}")).send().await.unwrap().status(), 200);
    assert_eq!(s.http.get(format!("{url}/healthz")).send().await.unwrap().status(), 200);
    let bad = s.grpc.resolve_rev(pb::ResolveRevRequest { rev: "nope".into() }).await;
    assert_eq!(bad.unwrap_err().code(), tonic::Code::NotFound);

    // git clone (skipped when git is unavailable).
    let tmp = tempfile::tempdir().unwrap();
    let cloned = tokio::process::Command::new("git")
        .current_dir(tmp.path())
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .args(["-c", "protocol.version=2", "clone", &format!("{url}/repo.git"), "c"])
        .output()
        .await;
    if let Ok(out) = cloned {
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    }
}

async fn scrape(s: &Server) -> String {
    let r = s.http.get(format!("{}/metrics", s.addrs.http_url())).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert!(
        r.headers()["content-type"].to_str().unwrap().starts_with("application/openmetrics-text")
    );
    r.text().await.unwrap()
}

fn parse(text: &str) -> Scrape {
    Scrape::parse(text.lines().map(|l| Ok(l.to_owned()))).unwrap()
}

/// Family a sample belongs to (histogram and counter suffixes stripped).
fn family(name: &str) -> &str {
    for suffix in ["_bucket", "_sum", "_count", "_total"] {
        if let Some(base) = name.strip_suffix(suffix) {
            // `ant_x_seconds_sum` -> `ant_x_seconds`; `ant_x_total` keeps `_total`.
            return if suffix == "_total" { name } else { base };
        }
    }
    name
}

/// Value of the sample `name` whose labels include all of `labels`.
fn sample(scrape: &Scrape, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    scrape
        .samples
        .iter()
        .find(|s| s.metric == name && labels.iter().all(|(k, v)| s.labels.get(k) == Some(*v)))
        .map(|s| match s.value {
            Value::Counter(v) | Value::Gauge(v) | Value::Untyped(v) => v,
            _ => f64::NAN,
        })
}

async fn eventually(s: &Server, name: &str, labels: &[(&str, &str)]) -> Scrape {
    for _ in 0..100 {
        let scrape = parse(&scrape(s).await);
        if sample(&scrape, name, labels).is_some() {
            return scrape;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no sample {name}{labels:?}");
}

#[tokio::test]
async fn metrics_has_every_required_family_after_a_session() {
    let mut s = spawn_server().await;
    scripted_session(&mut s).await;
    // The history catch-up runs in the background.
    let scrape = eventually(&s, "ant_history_lag_commits", &[("ref", "main")]).await;

    let families: HashSet<&str> = scrape.samples.iter().map(|x| family(&x.metric)).collect();
    for required in [
        "ant_rpc_requests_total",
        "ant_rpc_duration_seconds",
        "ant_http_requests_total",
        "ant_http_duration_seconds",
        "ant_land_batch_size",
        "ant_land_outcomes_total",
        "ant_land_cas_retries_total",
        "ant_land_queue_depth",
        "ant_store_op_duration_seconds",
        "ant_store_errors_total",
        "ant_cache_hits_total",
        "ant_cache_misses_total",
        "ant_cache_bytes",
        "ant_history_lag_commits",
        "ant_history_catch_up_seconds",
        "ant_build_info",
        "ant_pg_pool_size",
        "ant_pg_pool_idle",
        "ant_pg_pool_in_use",
    ] {
        assert!(families.contains(required), "missing {required}; have {families:?}");
    }
    assert_eq!(sample(&scrape, "ant_build_info", &[]), Some(1.0));
    // Routes seen by the session.
    let scrape = parse(&self::scrape(&s).await); // now includes the earlier /metrics scrape
    for route in ["objects", "git", "healthz", "metrics"] {
        let labels = [("route", route), ("status", "200")];
        assert!(sample(&scrape, "ant_http_requests_total", &labels).is_some(), "route {route}");
    }
    assert_eq!(sample(&scrape, "ant_history_lag_commits", &[("ref", "main")]), Some(0.0));
}

#[tokio::test]
async fn counters_move_as_expected() {
    let mut s = spawn_server().await;
    let before = parse(&s.metrics.render());
    let get = |sc: &Scrape, name: &str, l: &[(&str, &str)]| sample(sc, name, l).unwrap_or(0.0);
    assert_eq!(get(&before, "ant_land_outcomes_total", &[("outcome", "conflict")]), 0.0);

    let base = commit(&mut s, None, vec![write_file("a.txt", "one\n")]).await;
    land(&mut s, &base).await;
    let c1 = commit(&mut s, Some(base.clone()), vec![write_file("a.txt", "x\n")]).await;
    let c2 = commit(&mut s, Some(base.clone()), vec![write_file("a.txt", "y\n")]).await;
    land(&mut s, &c1).await;
    let after_land = parse(&s.metrics.render());
    land(&mut s, &c2).await; // conflicts
    let bad = s.grpc.resolve_rev(pb::ResolveRevRequest { rev: "nope".into() }).await;
    assert!(bad.is_err());

    let after = parse(&s.metrics.render());
    let out = |sc: &Scrape, o: &str| get(sc, "ant_land_outcomes_total", &[("outcome", o)]);
    assert_eq!(out(&after_land, "conflict"), 0.0);
    assert_eq!(out(&after, "conflict"), 1.0, "one conflicting land");
    assert_eq!(out(&after, "landed"), 2.0);
    assert_eq!(out(&after, "rebased"), 0.0);
    assert_eq!(out(&after, "denied") + out(&after, "aborted"), 0.0);

    let rpc = |sc: &Scrape, m: &str, c: &str| {
        get(sc, "ant_rpc_requests_total", &[("method", m), ("code", c)])
    };
    assert_eq!(rpc(&after, "Land", "ok"), 3.0);
    assert_eq!(rpc(&after, "CreateChangeset", "ok"), 3.0);
    assert_eq!(rpc(&after, "ResolveRev", "not_found"), 1.0);
    assert_eq!(
        get(&after, "ant_rpc_duration_seconds_count", &[("method", "Land")]),
        3.0,
        "one duration observation per request"
    );
    assert!(get(&after, "ant_land_batch_size_count", &[]) >= 3.0);
    assert!(
        get(&after, "ant_store_op_duration_seconds_count", &[("store", "refs"), ("op", "cas_ref")])
            >= 2.0
    );
    assert_eq!(get(&after, "ant_store_errors_total", &[("store", "refs"), ("op", "cas_ref")]), 0.0);
    // The land path reads trees/changesets through the caches.
    let lookups: f64 = ["trees", "commits", "nodes", "changesets"]
        .iter()
        .map(|c| {
            get(&after, "ant_cache_hits_total", &[("cache", c)])
                + get(&after, "ant_cache_misses_total", &[("cache", c)])
        })
        .sum();
    assert!(lookups > 0.0);
}

#[tokio::test]
async fn request_id_round_trips_over_grpc_and_http() {
    let mut s = spawn_server().await;

    let mut req = tonic::Request::new(pb::GetRefRequest { name: "main".into() });
    req.metadata_mut().insert("x-request-id", "req-grpc-42".parse().unwrap());
    let res = s.grpc.get_ref(req).await.unwrap();
    assert_eq!(res.metadata().get("x-request-id").unwrap(), "req-grpc-42");

    let res = s.grpc.get_ref(pb::GetRefRequest { name: "main".into() }).await.unwrap();
    let generated = res.metadata().get("x-request-id").unwrap().to_str().unwrap().to_owned();
    assert_eq!(generated.len(), 26, "ULID: {generated}");

    // Errors carry it too (trailers-only responses).
    let mut req = tonic::Request::new(pb::ResolveRevRequest { rev: "nope".into() });
    req.metadata_mut().insert("x-request-id", "req-grpc-err".parse().unwrap());
    let err = s.grpc.resolve_rev(req).await.unwrap_err();
    assert_eq!(err.metadata().get("x-request-id").unwrap(), "req-grpc-err");

    let url = format!("{}/healthz", s.addrs.http_url());
    let res = s.http.get(&url).header("x-request-id", "req-http-7").send().await.unwrap();
    assert_eq!(res.headers()["x-request-id"], "req-http-7");
    let res = s.http.get(&url).send().await.unwrap();
    assert_eq!(res.headers()["x-request-id"].to_str().unwrap().len(), 26);
    // Unusable ids are replaced, not echoed.
    let res = s.http.get(&url).header("x-request-id", "bad id").send().await.unwrap();
    assert_ne!(res.headers()["x-request-id"], "bad id");
}

#[tokio::test]
async fn metrics_listen_moves_the_endpoint() {
    let metrics = Metrics::new();
    let main = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let main_url = format!("http://{}", main.local_addr().unwrap());
    let own = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let own_url = format!("http://{}", own.local_addr().unwrap());
    tokio::spawn(ant_server::http::serve_with_metrics(
        Backend::in_memory(),
        Acl::open(),
        main,
        metrics.clone(),
        false,
    ));
    tokio::spawn(ant_server::http::serve_metrics(metrics, own));

    let http = reqwest::Client::new();
    assert_eq!(http.get(format!("{main_url}/metrics")).send().await.unwrap().status(), 404);
    let res = http.get(format!("{own_url}/metrics")).send().await.unwrap();
    assert_eq!(res.status(), 200);
    assert!(res.text().await.unwrap().contains("ant_build_info"));
    // The 404 above was counted under the closed `metrics` route label.
    let text = http.get(format!("{own_url}/metrics")).send().await.unwrap().text().await.unwrap();
    assert!(text.contains(r#"ant_http_requests_total{route="metrics",status="404"} 1"#), "{text}");
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn slow_requests_are_logged_with_the_request_id_in_json() {
    use tower::Service;

    let capture = Capture::default();
    let sink = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_writer(move || sink.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let metrics = Metrics::new();
    let observer =
        Arc::new(Observer::new(metrics, Acl::open()).with_slow_threshold(Duration::ZERO));
    let mut app = axum::Router::new()
        .route(
            "/healthz",
            axum::routing::get(|| async {
                tracing::info!("inside handler");
                "ok"
            }),
        )
        .layer(axum::middleware::from_fn_with_state(observer, http_observe));
    let req = http::Request::builder()
        .uri("/healthz")
        .header("x-request-id", "slow-1")
        .body(axum::body::Body::empty())
        .unwrap();
    let res = app.call(req).await.unwrap();
    assert_eq!(res.headers()["x-request-id"], "slow-1");

    let logs = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    let handler = logs.lines().find(|l| l.contains("inside handler")).expect(&logs);
    assert!(handler.contains(r#""request_id":"slow-1""#), "span fields on handler logs: {handler}");
    let slow = logs.lines().find(|l| l.contains("slow request")).expect(&logs);
    assert!(slow.contains(r#""level":"WARN""#), "{slow}");
    assert!(slow.contains(r#""request_id":"slow-1""#), "{slow}");
    assert!(slow.contains(r#""duration_ms""#) && slow.contains("healthz"), "{slow}");
}
