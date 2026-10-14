//! Readiness probing for `GET /readyz` (`crate::http`).
//!
//! [`Probes`] holds the dependencies a server was started with: the Postgres
//! store (a trivial query on the query pool) and the S3 store (`HEAD` on the
//! bucket). In-memory servers have none and are always ready. Each probe is
//! bounded by [`PROBE_TIMEOUT`], runs concurrently with the other, and keeps
//! no connection beyond the probe itself.

use std::sync::Arc;
use std::time::Duration;

use ant_store::postgres::PgStore;
use ant_store::s3::S3ObjectStore;

/// Longest a single dependency probe may take before it counts as failed.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// The external dependencies of a running server.
#[derive(Clone, Default)]
pub struct Probes {
    pub pg: Option<Arc<PgStore>>,
    pub s3: Option<S3ObjectStore>,
}

/// Result of one dependency probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Ok,
    Down,
    Timeout,
}

impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Status::Ok => "ok",
            Status::Down => "down",
            Status::Timeout => "timeout",
        }
    }
}

/// Per-dependency readiness; `None` = not configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Readiness {
    pub postgres: Option<Status>,
    pub s3: Option<Status>,
}

impl Readiness {
    pub fn is_ready(&self) -> bool {
        [self.postgres, self.s3].into_iter().flatten().all(|s| s == Status::Ok)
    }

    /// `{"status":"ok","postgres":"ok","s3":"ok"}`; unconfigured
    /// dependencies are omitted, failing ones read `down` or `timeout`.
    pub fn json(&self) -> String {
        let overall = if self.is_ready() { "ok" } else { "unavailable" };
        let mut out = format!(r#"{{"status":"{overall}""#);
        for (name, status) in [("postgres", self.postgres), ("s3", self.s3)] {
            if let Some(s) = status {
                out.push_str(&format!(r#","{name}":"{}""#, s.as_str()));
            }
        }
        out.push('}');
        out
    }
}

async fn bounded<F>(name: &str, fut: F) -> Status
where
    F: std::future::Future<Output = Result<(), ant_store::StoreError>>,
{
    match tokio::time::timeout(PROBE_TIMEOUT, fut).await {
        Ok(Ok(())) => Status::Ok,
        Ok(Err(e)) => {
            tracing::warn!(dependency = name, error = %e, "readiness probe failed");
            Status::Down
        }
        Err(_) => {
            tracing::warn!(dependency = name, "readiness probe timed out");
            Status::Timeout
        }
    }
}

impl Probes {
    /// Probe every configured dependency concurrently.
    pub async fn check(&self) -> Readiness {
        let pg = async {
            match &self.pg {
                Some(pg) => Some(bounded("postgres", pg.ping()).await),
                None => None,
            }
        };
        let s3 = async {
            match &self.s3 {
                Some(s3) => Some(bounded("s3", s3.ping()).await),
                None => None,
            }
        };
        let (postgres, s3) = tokio::join!(pg, s3);
        Readiness { postgres, s3 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_names_failing_dependencies() {
        let ok = Readiness { postgres: None, s3: None };
        assert!(ok.is_ready());
        assert_eq!(ok.json(), r#"{"status":"ok"}"#);
        let bad = Readiness { postgres: Some(Status::Down), s3: Some(Status::Ok) };
        assert!(!bad.is_ready());
        assert_eq!(bad.json(), r#"{"status":"unavailable","postgres":"down","s3":"ok"}"#);
    }
}
