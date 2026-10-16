//! Health and admin endpoints of the HTTP port: `/healthz` (liveness),
//! `/readyz` (dependency readiness), `/debug/stats` (admin-only under a
//! policy) and the 404 of a `/metrics` moved to its own listener.
//!
//! The test using live services runs with `--run-ignored all`.

mod common;

use std::sync::Arc;

use ant_auth::{Policy, hash_token};
use ant_core::ObjectId;
use ant_server::acl::Acl;
use ant_server::health::Probes;
use ant_server::metrics::Metrics;
use ant_store::Backend;
use ant_store::postgres::PgStore;
use ant_store::s3::{S3Config, S3ObjectStore};

const READER: &str = "t-reader";
const ADMIN: &str = "t-admin";

fn policy() -> Acl {
    let hex = |t: &str| ObjectId(hash_token(t)).to_hex();
    let text = format!(
        r#"
        [[principal]]
        name = "reader"
        token_sha256 = "{}"
        groups = ["agents"]

        [[principal]]
        name = "admin"
        token_sha256 = "{}"
        admin = true

        [[path]]
        prefix = ""
        read = ["*"]
        write = ["group:agents"]
        "#,
        hex(READER),
        hex(ADMIN),
    );
    Acl::new(Policy::parse(&text).unwrap())
}

/// Serve the HTTP port with `probes`; returns its base URL and the metrics.
async fn serve(acl: Acl, probes: Probes, expose_metrics: bool) -> (String, Arc<Metrics>) {
    let metrics = Metrics::new();
    metrics.set_probes(probes);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(ant_server::http::serve_with_metrics(
        Backend::in_memory(),
        acl,
        listener,
        metrics.clone(),
        expose_metrics,
    ));
    (url, metrics)
}

async fn get(url: &str, auth: Option<&str>) -> (u16, String) {
    let mut req = reqwest::Client::new().get(url);
    if let Some(a) = auth {
        req = req.header("authorization", a);
    }
    let res = req.send().await.unwrap();
    (res.status().as_u16(), res.text().await.unwrap())
}

#[tokio::test]
async fn local_mode_reports_its_store() {
    let local = ant_store::local::LocalStore::open_scratch("health").unwrap();
    let probes = Probes { local: Some(local), ..Probes::default() };
    let (url, _) = serve(Acl::open(), probes, true).await;
    assert_eq!(
        get(&format!("{url}/readyz"), None).await,
        (200, r#"{"status":"ok","local":"ok"}"#.into())
    );
}

#[tokio::test]
async fn in_memory_is_ready_and_counted_under_its_own_route() {
    let (url, metrics) = serve(Acl::open(), Probes::default(), true).await;
    assert_eq!(get(&format!("{url}/healthz"), None).await, (200, "ok".into()));
    assert_eq!(get(&format!("{url}/readyz"), None).await, (200, r#"{"status":"ok"}"#.into()));
    let text = metrics.render();
    assert!(text.contains(r#"ant_http_requests_total{route="readyz",status="200"} 1"#), "{text}");
}

#[tokio::test]
async fn unreachable_postgres_is_not_ready_but_stays_live() {
    let pg = PgStore::connect_lazy_unchecked("postgres://ant:ant@127.0.0.1:1/ant", "ant").unwrap();
    let probes = Probes { pg: Some(Arc::new(pg)), ..Probes::default() };
    let (url, metrics) = serve(Acl::open(), probes, true).await;

    let (status, body) = get(&format!("{url}/readyz"), None).await;
    assert_eq!(status, 503);
    assert!(body.starts_with(r#"{"status":"unavailable","postgres":""#), "{body}");
    assert!(!body.contains(r#""postgres":"ok""#), "{body}");
    // Repeated probes do not wedge the server, and liveness is unaffected.
    assert_eq!(get(&format!("{url}/readyz"), None).await.0, 503);
    assert_eq!(get(&format!("{url}/healthz"), None).await, (200, "ok".into()));
    let text = metrics.render();
    assert!(text.contains(r#"ant_http_requests_total{route="readyz",status="503"} 2"#), "{text}");
}

#[tokio::test]
async fn unreachable_s3_is_not_ready() {
    let cfg = S3Config {
        endpoint: "http://127.0.0.1:1".into(),
        region: "us-east-1".into(),
        bucket: "nope".into(),
        prefix: String::new(),
        access_key: "a".into(),
        secret_key: "b".into(),
        timeouts: Default::default(),
    };
    let s3 = S3ObjectStore::connect(&cfg).await.unwrap();
    let (url, _) = serve(Acl::open(), Probes { s3: Some(s3), ..Probes::default() }, true).await;
    let (status, body) = get(&format!("{url}/readyz"), None).await;
    assert_eq!(status, 503);
    assert!(body.starts_with(r#"{"status":"unavailable","s3":""#), "{body}");
    assert!(!body.contains(r#""s3":"ok""#), "{body}");
}

#[tokio::test]
#[ignore = "needs postgres and s3: docker compose up -d"]
async fn ready_with_postgres_and_s3() {
    let schema = common::pg::scratch_schema("health");
    let pg = PgStore::connect_scratch(&common::pg::pg_url(), &schema).await.unwrap();
    let s3 = S3ObjectStore::connect_scratch(&S3Config::for_tests("health")).await.unwrap();
    s3.ensure_bucket().await.unwrap();
    let probes = Probes { pg: Some(Arc::new(pg)), s3: Some(s3), ..Probes::default() };

    // Readiness needs no credentials, even under a policy.
    let (http, _) = serve(policy(), probes, true).await;
    let (status, body) = get(&format!("{http}/readyz"), None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, r#"{"status":"ok","postgres":"ok","s3":"ok"}"#);
}

#[tokio::test]
async fn debug_stats_needs_an_admin_under_a_policy() {
    let (open, _) = serve(Acl::open(), Probes::default(), true).await;
    let (status, body) = get(&format!("{open}/debug/stats"), None).await;
    assert_eq!(status, 200);
    assert!(body.starts_with(r#"{"caches":"#), "{body}");

    let (url, _) = serve(policy(), Probes::default(), true).await;
    let stats = format!("{url}/debug/stats");
    assert_eq!(get(&stats, None).await.0, 401);
    assert_eq!(get(&stats, Some("Bearer wrong")).await.0, 401);
    assert_eq!(get(&stats, Some(&format!("Bearer {READER}"))).await.0, 403);
    let (status, body) = get(&stats, Some(&format!("Bearer {ADMIN}"))).await;
    assert_eq!(status, 200);
    assert!(body.starts_with(r#"{"caches":"#), "{body}");
    // Basic with the token as password, as git clients send it.
    let res =
        reqwest::Client::new().get(&stats).basic_auth("anyone", Some(ADMIN)).send().await.unwrap();
    assert_eq!(res.status(), 200);

    // Liveness and readiness stay open.
    assert_eq!(get(&format!("{url}/healthz"), None).await.0, 200);
    assert_eq!(get(&format!("{url}/readyz"), None).await.0, 200);
}

#[tokio::test]
async fn moved_metrics_and_unknown_paths_are_404_under_a_policy() {
    let (url, _) = serve(policy(), Probes::default(), false).await;
    assert_eq!(get(&format!("{url}/metrics"), None).await.0, 404);
    assert_eq!(get(&format!("{url}/nope"), None).await.0, 404);
    // The git export is still guarded.
    assert_eq!(get(&format!("{url}/repo.git/info/refs"), None).await.0, 401);
    assert_eq!(get(&format!("{url}/repo.git/other"), None).await.0, 401);
    // Exposed on this port, metrics remain open as documented.
    let (url, _) = serve(policy(), Probes::default(), true).await;
    assert_eq!(get(&format!("{url}/metrics"), None).await.0, 200);
}
