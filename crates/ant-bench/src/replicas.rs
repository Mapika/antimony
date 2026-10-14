//! Replica process plumbing for the soak harness.
//!
//! - [`Replica`] spawns `<exe> serve ...` child processes on free loopback
//!   ports, waits for `/healthz`, and kills them on drop.
//! - [`PgMonitor`] keeps one Postgres connection open to watch server
//!   connection counts per `application_name`.
//! - [`proc_rss_bytes`] reads a process's resident set size from `/proc`.

use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::Stdio;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use sqlx::{Connection, Row};
use tokio::process::Child;

/// Shared HTTP client for replica probes.
static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

/// How long a freshly spawned replica may take to answer `/healthz`.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Resident set size of a process from `/proc/<pid>/status` (`VmRSS`), in bytes.
pub fn proc_rss_bytes(pid: u32) -> Option<u64> {
    crate::util::proc_status_bytes(&format!("/proc/{pid}/status"), "VmRSS")
}

/// One Postgres connection used to watch server connection counts.
pub struct PgMonitor {
    conn: sqlx::PgConnection,
}

impl PgMonitor {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let conn =
            sqlx::PgConnection::connect(url).await.context("connecting PgMonitor to Postgres")?;
        Ok(Self { conn })
    }

    /// Connections to the current database grouped by `application_name`,
    /// sorted by name. Includes this monitor's own connection.
    pub async fn connections(&mut self) -> anyhow::Result<Vec<(String, i64)>> {
        let rows = sqlx::query(
            "SELECT application_name, count(*) FROM pg_stat_activity \
             WHERE datname = current_database() GROUP BY 1 ORDER BY 1",
        )
        .fetch_all(&mut self.conn)
        .await
        .context("querying pg_stat_activity")?;
        rows.iter()
            .map(|row| {
                let name: Option<String> = row.try_get(0).context("reading application_name")?;
                let count: i64 = row.try_get(1).context("reading connection count")?;
                Ok((name.unwrap_or_default(), count))
            })
            .collect()
    }
}

/// A spawned server replica process (`<exe> serve ...`), killed on drop.
pub struct Replica {
    pub index: usize,
    pub grpc_url: String,
    pub http_url: String,
    pub pid: u32,
    child: Child,
}

impl Replica {
    /// Spawn `exe serve --grpc-listen 127.0.0.1:<p1> --http-listen 127.0.0.1:<p2>
    /// <extra_args...>` with free loopback ports, stdout and stderr appended to
    /// `log_path`, and extra `env` variables. Returns once `/healthz` answers.
    pub async fn spawn(
        index: usize,
        exe: &Path,
        extra_args: &[String],
        env: &[(String, String)],
        log_path: &Path,
    ) -> anyhow::Result<Replica> {
        let grpc_addr = free_loopback_addr()?;
        let http_addr = free_loopback_addr()?;
        let open_log = || {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path)
                .with_context(|| format!("opening replica log {}", log_path.display()))
        };
        let stdout = open_log()?;
        let stderr = stdout.try_clone().context("duplicating replica log handle")?;

        let mut child = tokio::process::Command::new(exe)
            .arg("serve")
            .arg("--grpc-listen")
            .arg(grpc_addr.to_string())
            .arg("--http-listen")
            .arg(http_addr.to_string())
            .args(extra_args)
            .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawning replica {index} from {}", exe.display()))?;
        let pid =
            child.id().ok_or_else(|| anyhow!("replica {index} exited before its pid was read"))?;
        let http_url = format!("http://{http_addr}");
        wait_ready(&mut child, &http_url, log_path, READY_TIMEOUT)
            .await
            .with_context(|| format!("replica {index} did not become ready"))?;
        Ok(Replica { index, grpc_url: format!("http://{grpc_addr}"), http_url, pid, child })
    }

    /// Body of `GET <http_url>/debug/stats`.
    pub async fn stats(&self) -> anyhow::Result<String> {
        let url = format!("{}/debug/stats", self.http_url);
        CLIENT
            .get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .with_context(|| format!("GET {url}"))?
            .error_for_status()
            .with_context(|| format!("GET {url}"))?
            .text()
            .await
            .with_context(|| format!("reading body of {url}"))
    }

    pub fn rss_bytes(&self) -> Option<u64> {
        proc_rss_bytes(self.pid)
    }

    /// True while the process is still running.
    pub fn alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Kill the process and reap it, ignoring errors.
    pub async fn shutdown(mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

/// Poll `GET <http_url>/healthz` every 100 ms until it returns 200. Fails early
/// if the child exits, or after `timeout`; the error includes the last 20 lines
/// of `log_path`.
async fn wait_ready(
    child: &mut Child,
    http_url: &str,
    log_path: &Path,
    timeout: Duration,
) -> anyhow::Result<()> {
    let url = format!("{http_url}/healthz");
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            bail_with_log(log_path, format!("replica exited with {status} before becoming ready"))?;
        }
        if let Ok(resp) = CLIENT.get(&url).timeout(Duration::from_secs(2)).send().await
            && resp.status() == reqwest::StatusCode::OK
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail_with_log(log_path, format!("timed out after {timeout:?} waiting for {url}"))?;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Return an error whose message is `summary` followed by the log tail.
fn bail_with_log(log_path: &Path, summary: String) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(log_path).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    let tail = lines[lines.len().saturating_sub(20)..].join("\n");
    Err(anyhow!("{summary}; last log lines from {}:\n{tail}", log_path.display()))
}

/// Bind `127.0.0.1:0`, read the assigned port, and release it.
fn free_loopback_addr() -> anyhow::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").context("reserving a loopback port")?;
    let addr = listener.local_addr().context("reading reserved port")?;
    drop(listener);
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proc_rss_bytes_reads_self_and_rejects_missing_pid() {
        let rss = proc_rss_bytes(std::process::id()).expect("own RSS");
        assert!(rss > 1024 * 1024, "rss {rss} unexpectedly small");
        assert_eq!(proc_rss_bytes(u32::MAX), None);
    }

    #[tokio::test]
    async fn wait_ready_reports_child_log_on_early_exit() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let log = std::env::temp_dir()
            .join(format!("ant-bench-replicas-test-{}-{nanos}.log", std::process::id()));
        let closed = free_loopback_addr().expect("free port");
        let script = format!("echo boom >> {}; exit 3", log.display());
        let mut child =
            tokio::process::Command::new("sh").arg("-c").arg(script).spawn().expect("spawn sh");
        let result =
            wait_ready(&mut child, &format!("http://{closed}"), &log, Duration::from_secs(10))
                .await;
        let _ = std::fs::remove_file(&log);
        let err = format!("{:#}", result.expect_err("child exits early, so not ready"));
        assert!(err.contains("boom"), "error lacks log contents: {err}");
    }

    #[tokio::test]
    #[ignore = "needs Postgres (scripts/dev-services.sh)"]
    async fn pg_monitor_counts_connections() {
        let mut mon =
            PgMonitor::connect("postgres://ant:ant@127.0.0.1:54329/ant").await.expect("connect");
        let rows = mon.connections().await.expect("query");
        assert!(rows.iter().any(|(_, n)| *n >= 1), "no connection rows: {rows:?}");
    }
}
