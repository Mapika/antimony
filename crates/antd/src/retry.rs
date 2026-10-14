//! Client side of the server's rate limits and load shedding
//! (`ant_server::limits`): retry rejected requests after the server's hint,
//! with jittered, bounded backoff.
//!
//! # What is retried
//! Only responses that carry the server's retry hint:
//! - gRPC `RESOURCE_EXHAUSTED` (rate limit) or `UNAVAILABLE` (load shed)
//!   with the `retry-after-ms` trailer metadata;
//! - HTTP `429` / `503` with `retry-after-ms` or `Retry-After` (idempotent
//!   `GET`s only).
//!
//! The server sends the hint exclusively when it rejects a request *before*
//! running it, so retrying is safe for every RPC, `Land`, `CreateChangeset`
//! and `UpdateRef` included: nothing was executed. A bare `UNAVAILABLE`
//! (connection refused, reset mid-request) carries no hint and is never
//! retried here, because the request may have run. Land and CreateChangeset
//! are additionally idempotent by construction (a land is looked up with
//! `LandedAs`; a changeset is content-addressed).
//!
//! # Backoff
//! Attempt `n` (from 0) waits `max(hint, 50 ms * 2^n)` scaled by a random
//! factor in `[1, 1.5)` (so a herd released together spreads out), capped at
//! [`RetryPolicy::max_delay`]. Retrying stops after
//! [`RetryPolicy::max_retries`] attempts or once the sleeps would exceed
//! [`RetryPolicy::max_total`]; the last rejection is then returned as is.
//!
//! The gRPC retry layer buffers each request body once (needed to replay it);
//! responses are untouched.

use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::BoxFuture;
use http_body_util::{BodyExt, Full};
use tonic::body::Body;
use tower::Service;

/// Header / trailer carrying the server's hint in milliseconds.
pub const RETRY_AFTER_MS: &str = "retry-after-ms";

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// How hard to retry rejected requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Retries after the first attempt; 0 disables retrying.
    pub max_retries: u32,
    /// Longest single sleep.
    pub max_delay: Duration,
    /// Longest total sleep across the retries of one request.
    pub max_total: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 8,
            max_delay: Duration::from_secs(5),
            max_total: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// Never retry.
    pub fn none() -> Self {
        Self { max_retries: 0, ..Self::default() }
    }

    /// The sleep before retry number `attempt` (0-based) given the server's
    /// `hint`; `jitter` in `[0, 1)` picks the position in the 1.0..1.5 spread.
    pub fn delay(&self, attempt: u32, hint: Duration, jitter: f64) -> Duration {
        let backoff = Duration::from_millis(50).saturating_mul(1u32 << attempt.min(16));
        let base = hint.max(backoff);
        base.mul_f64(1.0 + 0.5 * jitter.clamp(0.0, 1.0)).min(self.max_delay)
    }

    /// Whether another retry fits the budget after `slept` so far.
    fn allows(&self, attempt: u32, slept: Duration, next: Duration) -> bool {
        attempt < self.max_retries && slept + next <= self.max_total
    }
}

fn jitter() -> f64 {
    rand::random::<f64>()
}

/// The server's retry hint of a rejected gRPC response (trailers-only
/// responses carry status and metadata in the headers).
fn grpc_hint(headers: &http::HeaderMap) -> Option<Duration> {
    let code: i32 = headers.get("grpc-status")?.to_str().ok()?.parse().ok()?;
    // RESOURCE_EXHAUSTED = 8, UNAVAILABLE = 14.
    if code != 8 && code != 14 {
        return None;
    }
    let ms: u64 = headers.get(RETRY_AFTER_MS)?.to_str().ok()?.parse().ok()?;
    Some(Duration::from_millis(ms))
}

/// Tower layer-less wrapper: [`Retrying::new`] around a tonic `Channel`.
#[derive(Clone)]
pub struct Retrying<S> {
    inner: S,
    policy: RetryPolicy,
}

impl<S> Retrying<S> {
    pub fn new(inner: S, policy: RetryPolicy) -> Self {
        Self { inner, policy }
    }
}

impl<S, ResB> Service<http::Request<Body>> for Retrying<S>
where
    S: Service<http::Request<Body>, Response = http::Response<ResB>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Send + 'static,
    ResB: Send + 'static,
{
    type Response = http::Response<ResB>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let policy = self.policy;
        if policy.max_retries == 0 {
            let fut = inner.call(req);
            return Box::pin(async move { fut.await.map_err(Into::into) });
        }
        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let bytes: Bytes = body.collect().await.map_err(BoxError::from)?.to_bytes();
            let mut attempt = 0u32;
            let mut slept = Duration::ZERO;
            loop {
                let mut req = http::Request::new(Body::new(Full::new(bytes.clone())));
                *req.method_mut() = parts.method.clone();
                *req.uri_mut() = parts.uri.clone();
                *req.version_mut() = parts.version;
                *req.headers_mut() = parts.headers.clone();
                // Subsequent attempts need a ready service too.
                futures::future::poll_fn(|cx| inner.poll_ready(cx)).await.map_err(Into::into)?;
                let res = inner.call(req).await.map_err(Into::into)?;
                let Some(hint) = grpc_hint(res.headers()) else { return Ok(res) };
                let delay = policy.delay(attempt, hint, jitter());
                if !policy.allows(attempt, slept, delay) {
                    return Ok(res);
                }
                tracing::debug!(
                    path = %parts.uri.path(),
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    "server asked to retry"
                );
                drop(res);
                tokio::time::sleep(delay).await;
                slept += delay;
                attempt += 1;
            }
        })
    }
}

/// The server's retry hint of a rejected HTTP response: `429` or `503` with
/// `retry-after-ms` or `Retry-After` (seconds).
pub fn http_hint(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
) -> Option<Duration> {
    if status != reqwest::StatusCode::TOO_MANY_REQUESTS
        && status != reqwest::StatusCode::SERVICE_UNAVAILABLE
    {
        return None;
    }
    if let Some(ms) = headers.get(RETRY_AFTER_MS).and_then(|v| v.to_str().ok()?.parse::<u64>().ok())
    {
        return Some(Duration::from_millis(ms));
    }
    let secs =
        headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?.trim().parse::<u64>().ok()?;
    Some(Duration::from_secs(secs))
}

/// Send a `GET` (cloneable, idempotent), retrying 429/503 answers that carry
/// a hint per `policy`. Returns the last response when retries run out.
pub async fn send_get(
    req: reqwest::RequestBuilder,
    policy: &RetryPolicy,
) -> reqwest::Result<reqwest::Response> {
    let mut attempt = 0u32;
    let mut slept = Duration::ZERO;
    let started = Instant::now();
    loop {
        let Some(this) = req.try_clone() else { return req.send().await };
        let res = this.send().await?;
        let Some(hint) = http_hint(res.status(), res.headers()) else { return Ok(res) };
        let delay = policy.delay(attempt, hint, jitter());
        if !policy.allows(attempt, slept, delay) {
            return Ok(res);
        }
        tracing::debug!(
            attempt,
            delay_ms = delay.as_millis() as u64,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "server asked to retry"
        );
        drop(res);
        tokio::time::sleep(delay).await;
        slept += delay;
        attempt += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn delay_follows_hint_backoff_jitter_and_cap() {
        let p = RetryPolicy::default();
        // Hint dominates a small backoff; jitter spreads 1.0..1.5.
        assert_eq!(p.delay(0, ms(200), 0.0), ms(200));
        assert_eq!(p.delay(0, ms(200), 1.0), ms(300));
        // Exponential backoff dominates a small hint as attempts grow.
        assert_eq!(p.delay(0, ms(1), 0.0), ms(50));
        assert_eq!(p.delay(3, ms(1), 0.0), ms(400));
        // Capped.
        assert_eq!(p.delay(10, ms(1), 1.0), p.max_delay);
        assert_eq!(p.delay(0, Duration::from_secs(600), 0.0), p.max_delay);
        // Absurd attempts do not overflow.
        assert_eq!(p.delay(u32::MAX, ms(1), 0.5), p.max_delay);
    }

    #[test]
    fn budget_bounds_retries() {
        let p = RetryPolicy { max_retries: 3, max_delay: ms(100), max_total: ms(250) };
        assert!(p.allows(0, ms(0), ms(100)));
        assert!(p.allows(1, ms(100), ms(100)));
        assert!(!p.allows(2, ms(200), ms(100)), "total budget");
        assert!(!p.allows(3, ms(0), ms(1)), "retry count");
        assert!(!RetryPolicy::none().allows(0, ms(0), ms(0)));
    }

    #[test]
    fn hints_only_from_server_rejections() {
        let mut h = http::HeaderMap::new();
        assert_eq!(grpc_hint(&h), None);
        h.insert("grpc-status", "14".parse().unwrap());
        assert_eq!(grpc_hint(&h), None, "bare UNAVAILABLE may have run");
        h.insert(RETRY_AFTER_MS, "120".parse().unwrap());
        assert_eq!(grpc_hint(&h), Some(ms(120)));
        h.insert("grpc-status", "8".parse().unwrap());
        assert_eq!(grpc_hint(&h), Some(ms(120)));
        h.insert("grpc-status", "13".parse().unwrap());
        assert_eq!(grpc_hint(&h), None);
        h.insert("grpc-status", "8".parse().unwrap());
        h.insert(RETRY_AFTER_MS, "soon".parse().unwrap());
        assert_eq!(grpc_hint(&h), None);

        let mut h = reqwest::header::HeaderMap::new();
        let busy = reqwest::StatusCode::SERVICE_UNAVAILABLE;
        assert_eq!(http_hint(busy, &h), None);
        h.insert("retry-after", "2".parse().unwrap());
        assert_eq!(http_hint(busy, &h), Some(Duration::from_secs(2)));
        h.insert(RETRY_AFTER_MS, "250".parse().unwrap());
        assert_eq!(http_hint(reqwest::StatusCode::TOO_MANY_REQUESTS, &h), Some(ms(250)));
        assert_eq!(http_hint(reqwest::StatusCode::NOT_FOUND, &h), None);
        assert_eq!(http_hint(reqwest::StatusCode::INTERNAL_SERVER_ERROR, &h), None);
    }
}
