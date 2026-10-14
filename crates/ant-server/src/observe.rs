//! Request observation shared by the gRPC and HTTP front ends: request ids,
//! per-request metrics and slow-request logs. Handlers stay untouched; both
//! transports wrap their whole service.
//!
//! - **Request id**: an incoming `x-request-id` (1..=128 visible ASCII bytes)
//!   is kept, anything else gets a fresh ULID. It is a field of the `rpc` /
//!   `http` tracing span that covers the handler (so every log line of the
//!   request carries it) and is echoed in the `x-request-id` response header.
//! - **Metrics**: `ant_rpc_*` from [`GrpcObserveLayer`], `ant_http_*` from
//!   [`http_observe`] (see `crate::metrics`).
//! - **Slow requests**: anything taking at least [`SLOW_REQUEST`] is logged at
//!   WARN (`slow request`) with the method or route, duration and, when the
//!   request carried valid credentials under a policy, the principal;
//!   except `WatchRef`, whose stream stays open by design.
//!
//! gRPC durations and status codes are taken when the response body ends:
//! the status of a successful unary call only arrives in the trailers, and
//! streaming calls last as long as their stream. A request dropped before
//! that (client went away) is recorded with code `cancelled`.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use futures::future::BoxFuture;
use http::{HeaderMap, HeaderValue, header};
use http_body::{Body, Frame};
use pin_project_lite::pin_project;
use tower::{Layer, Service};
use tracing::{Instrument, Span};

use crate::acl::Acl;
use crate::metrics::Metrics;

/// Requests at least this slow are logged at WARN.
const SLOW_REQUEST: Duration = Duration::from_secs(1);

/// Request id header, honoured on requests and set on responses.
const REQUEST_ID_HEADER: &str = "x-request-id";

const MAX_REQUEST_ID_LEN: usize = 128;

/// gRPC methods of `ant.v1.Ant` (kept equal to `proto/ant.proto` by a test);
/// any other path is recorded as `unknown` to keep label cardinality closed.
const GRPC_METHODS: [&str; 18] = [
    "GetObjects",
    "HasObjects",
    "PutObjects",
    "GetRef",
    "ListRefs",
    "UpdateRef",
    "CreateChangeset",
    "Land",
    "ResolveRev",
    "ReadPath",
    "Log",
    "Diff",
    "Blame",
    "MergeBase",
    "IsAncestor",
    "LogRange",
    "LandedAs",
    "WatchRef",
];

/// Streams that stay open by design (never logged as slow).
const LONG_LIVED: [&str; 1] = ["WatchRef"];

const GRPC_PREFIX: &str = "/ant.v1.Ant/";

/// Shared state of the observation layers.
pub struct Observer {
    metrics: Arc<Metrics>,
    acl: Acl,
    slow: Duration,
}

impl Observer {
    pub fn new(metrics: Arc<Metrics>, acl: Acl) -> Self {
        Self { metrics, acl, slow: SLOW_REQUEST }
    }

    /// Log requests at least `slow` long (default [`SLOW_REQUEST`]).
    pub fn with_slow_threshold(mut self, slow: Duration) -> Self {
        self.slow = slow;
        self
    }

    /// WARN when `elapsed` reaches the threshold. The principal is resolved
    /// only then, from the request's `authorization` header.
    fn log_if_slow(
        &self,
        span: &Span,
        what: &str,
        auth: Option<&HeaderValue>,
        elapsed: Duration,
        outcome: &str,
    ) {
        if elapsed < self.slow || LONG_LIVED.contains(&what) {
            return;
        }
        let principal = if self.acl.is_open() {
            None
        } else {
            let header = auth.and_then(|v| v.to_str().ok());
            self.acl.authenticate_header(header).ok().flatten().map(|a| a.principal.name)
        };
        tracing::warn!(
            parent: span,
            what,
            outcome,
            principal = principal.as_deref(),
            duration_ms = elapsed.as_millis() as u64,
            "slow request"
        );
    }
}

/// The caller's request id, or a fresh ULID.
fn request_id(headers: &HeaderMap) -> HeaderValue {
    headers
        .get(REQUEST_ID_HEADER)
        .filter(|v| {
            let b = v.as_bytes();
            !b.is_empty() && b.len() <= MAX_REQUEST_ID_LEN && b.iter().all(u8::is_ascii_graphic)
        })
        .cloned()
        .unwrap_or_else(|| {
            // A ULID is 26 ASCII characters: always a valid header value.
            HeaderValue::from_str(&ulid::Ulid::new().to_string())
                .unwrap_or_else(|_| HeaderValue::from_static("unknown"))
        })
}

fn id_str(id: &HeaderValue) -> &str {
    id.to_str().unwrap_or("-")
}

// ---------------------------------------------------------------------------
// gRPC
// ---------------------------------------------------------------------------

/// Closed-set method label for a gRPC request path.
fn grpc_method(path: &str) -> &'static str {
    path.strip_prefix(GRPC_PREFIX)
        .and_then(|m| GRPC_METHODS.iter().find(|known| **known == m))
        .copied()
        .unwrap_or("unknown")
}

/// snake_case name of a gRPC status code number.
fn code_name(code: i32) -> &'static str {
    use tonic::Code::*;
    match tonic::Code::from(code) {
        Ok => "ok",
        Cancelled => "cancelled",
        Unknown => "unknown",
        InvalidArgument => "invalid_argument",
        DeadlineExceeded => "deadline_exceeded",
        NotFound => "not_found",
        AlreadyExists => "already_exists",
        PermissionDenied => "permission_denied",
        ResourceExhausted => "resource_exhausted",
        FailedPrecondition => "failed_precondition",
        Aborted => "aborted",
        OutOfRange => "out_of_range",
        Unimplemented => "unimplemented",
        Internal => "internal",
        Unavailable => "unavailable",
        DataLoss => "data_loss",
        Unauthenticated => "unauthenticated",
    }
}

fn grpc_status(headers: &HeaderMap) -> Option<&'static str> {
    let value = headers.get("grpc-status")?.to_str().ok()?.parse::<i32>().ok()?;
    Some(code_name(value))
}

/// Tower layer around the whole gRPC service.
#[derive(Clone)]
pub struct GrpcObserveLayer {
    observer: Arc<Observer>,
}

impl GrpcObserveLayer {
    pub fn new(observer: Observer) -> Self {
        Self { observer: Arc::new(observer) }
    }
}

impl<S> Layer<S> for GrpcObserveLayer {
    type Service = GrpcObserve<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcObserve { inner, observer: self.observer.clone() }
    }
}

#[derive(Clone)]
pub struct GrpcObserve<S> {
    inner: S,
    observer: Arc<Observer>,
}

impl<S, B, ResB> Service<http::Request<B>> for GrpcObserve<S>
where
    S: Service<http::Request<B>, Response = http::Response<ResB>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
    ResB: Body,
{
    type Response = http::Response<ObservedBody<ResB>>;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        // Keep the readied service for this call (tower's clone/replace idiom).
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let method = grpc_method(req.uri().path());
        let id = request_id(req.headers());
        let auth = req.headers().get(header::AUTHORIZATION).cloned();
        let span = tracing::info_span!("rpc", request_id = id_str(&id), method);
        let observer = self.observer.clone();
        let start = Instant::now();
        let fut = inner.call(req);
        Box::pin(
            async move {
                let result = fut.await;
                let mut res = result?;
                res.headers_mut().insert(REQUEST_ID_HEADER, id);
                // Trailers-only responses (errors) carry the status already.
                let code = grpc_status(res.headers());
                let span = Span::current();
                let guard = Guard { observer, method, auth, start, span, code, done: false };
                Ok(res.map(|inner| ObservedBody { inner, guard }))
            }
            .instrument(span),
        )
    }
}

/// Records the request when it is over (or abandoned).
struct Guard {
    observer: Arc<Observer>,
    method: &'static str,
    auth: Option<HeaderValue>,
    start: Instant,
    span: Span,
    code: Option<&'static str>,
    done: bool,
}

impl Guard {
    fn finish(&mut self) {
        if std::mem::replace(&mut self.done, true) {
            return;
        }
        let code = self.code.unwrap_or("cancelled");
        let elapsed = self.start.elapsed();
        self.observer.metrics.record_rpc(self.method, code, elapsed.as_secs_f64());
        self.observer.log_if_slow(&self.span, self.method, self.auth.as_ref(), elapsed, code);
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.finish();
    }
}

pin_project! {
    /// Response body that reports the request when it ends.
    pub struct ObservedBody<B> {
        #[pin]
        inner: B,
        guard: Guard,
    }
}

impl<B: Body> Body for ObservedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let polled = this.inner.poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                // The status of a streamed or successful response; nothing
                // follows the trailers, so the request ends here.
                if let Some(trailers) = frame.trailers_ref() {
                    this.guard.code = grpc_status(trailers).or(this.guard.code).or(Some("unknown"));
                    this.guard.finish();
                }
            }
            Poll::Ready(Some(Err(_))) => {
                this.guard.code = this.guard.code.or(Some("unknown"));
                this.guard.finish();
            }
            Poll::Ready(None) => {
                this.guard.code = this.guard.code.or(Some("unknown"));
                this.guard.finish();
            }
            Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

/// Closed-set route label for an HTTP path.
fn http_route(path: &str) -> &'static str {
    if path.starts_with("/o/") {
        "objects"
    } else if path.starts_with("/t/") {
        "tree_bundles"
    } else if path == "/repo.git" || path.starts_with("/repo.git/") {
        "git"
    } else if path == "/healthz" {
        "healthz"
    } else if path == "/readyz" {
        "readyz"
    } else if path == "/metrics" {
        "metrics"
    } else {
        "other"
    }
}

/// axum middleware (`from_fn_with_state`) for the HTTP front end.
pub async fn http_observe(
    State(observer): State<Arc<Observer>>,
    req: Request,
    next: Next,
) -> Response {
    let route = http_route(req.uri().path());
    let id = request_id(req.headers());
    let auth = req.headers().get(header::AUTHORIZATION).cloned();
    let span = tracing::info_span!("http", request_id = id_str(&id), route, method = %req.method());
    let start = Instant::now();
    let mut res = next.run(req).instrument(span.clone()).await;
    let elapsed = start.elapsed();
    let status = res.status();
    observer.metrics.record_http(route, status.as_u16(), elapsed.as_secs_f64());
    observer.log_if_slow(&span, route, auth.as_ref(), elapsed, status.as_str());
    res.headers_mut().insert(REQUEST_ID_HEADER, id);
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_method_list_matches_proto() {
        let proto = include_str!("../../../proto/ant.proto");
        let mut declared: Vec<&str> = proto
            .lines()
            .filter_map(|l| l.trim().strip_prefix("rpc "))
            .filter_map(|l| l.split('(').next())
            .collect();
        let mut known = GRPC_METHODS.to_vec();
        declared.sort_unstable();
        known.sort_unstable();
        assert_eq!(declared, known, "update GRPC_METHODS");
        assert_eq!(grpc_method("/ant.v1.Ant/Land"), "Land");
        assert_eq!(grpc_method("/ant.v1.Ant/Nope"), "unknown");
        assert_eq!(grpc_method("/other.Service/Land"), "unknown");
    }

    #[test]
    fn request_id_honours_valid_and_replaces_bad() {
        let mut h = HeaderMap::new();
        assert_eq!(request_id(&h).len(), 26, "ULID");
        h.insert(REQUEST_ID_HEADER, HeaderValue::from_static("abc-123"));
        assert_eq!(request_id(&h), "abc-123");
        h.insert(REQUEST_ID_HEADER, HeaderValue::from_static("has space"));
        assert_eq!(request_id(&h).len(), 26);
        h.insert(REQUEST_ID_HEADER, HeaderValue::from_str(&"x".repeat(129)).unwrap());
        assert_eq!(request_id(&h).len(), 26);
    }

    #[test]
    fn http_routes_are_a_closed_set() {
        assert_eq!(http_route("/o/abcd"), "objects");
        assert_eq!(http_route("/t/abcd"), "tree_bundles");
        assert_eq!(http_route("/repo.git/info/refs"), "git");
        assert_eq!(http_route("/healthz"), "healthz");
        assert_eq!(http_route("/readyz"), "readyz");
        assert_eq!(http_route("/metrics"), "metrics");
        assert_eq!(http_route("/debug/stats"), "other");
        assert_eq!(http_route("/random/\u{1F600}"), "other");
    }

    #[test]
    fn code_names_cover_all_codes() {
        for n in 0..=16 {
            assert_ne!(code_name(n), "", "{n}");
        }
        assert_eq!(code_name(5), "not_found");
        assert_eq!(code_name(0), "ok");
    }
}
