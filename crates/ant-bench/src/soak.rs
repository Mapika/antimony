//! `soak` subcommand: N server replica processes on one Postgres schema (plus
//! optional S3) driven by up to thousands of simulated agents for a long time,
//! watching for leaks, drift and errors.
//!
//! - Replicas are spawned as `<exe> serve ...` (default: this binary's hidden
//!   `serve`, identical to `ant serve`), each with its own Postgres pools
//!   (`application_name=soak-r<i>`) sized so all replicas fit under
//!   `--pg-budget` connections.
//! - Each agent is a cheap task looping "think, then one operation", picking
//!   from: create+land (files under `agents/<shard>/<i>/`, sometimes also a
//!   one-line shared file, which conflicts when edited concurrently), cat
//!   (`ReadPath` with content), ls (`ReadPath` of a directory), log (`Log`,
//!   half of them path-filtered) and status (`GetRef` + `Diff` from the
//!   agent's base, then the base moves to the tip). Agent `i` talks to
//!   replica `i % N`.
//! - Every `--window` seconds one line is printed (and kept for `--json`):
//!   throughput, p50/p99 per operation, errors/aborts/conflicts, each
//!   replica's RSS, cache weight and land queue length (`/debug/stats`), and
//!   Postgres connections per replica.
//! - Fails (exit 1) on: invariant violations (the ref's first-parent history
//!   must be linear and grow by exactly the number of successful lands, and
//!   every agent file must hold its last landed content), a dead replica, an
//!   error rate above `--max-error-rate` (conflicts are outcomes, not
//!   errors), an RSS growth trend over the second half of the run above
//!   `--max-rss-growth-mib-per-hour`, or any operation's window p99 above
//!   `--max-p99-drift` × its first-window p99 (and above `--p99-floor-ms`).
//! - Afterwards the schema is dropped and the run's S3 prefix deleted
//!   (unless `--keep`).

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ant_core::ObjectId;
use ant_proto::MAX_MESSAGE_BYTES;
use ant_proto::v1 as pb;
use ant_proto::v1::ant_client::AntClient;
use ant_store::MetaStore;
use ant_store::postgres::{PgOptions, PgStore};
use ant_store::s3::S3Config;
use anyhow::{Context as _, Result, anyhow, bail};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sqlx::Connection as _;
use tonic::transport::Channel;

use crate::json::J;
use crate::replicas::{PgMonitor, Replica};
use crate::util::{Lat, mib, write_json};

/// Agent directories are sharded so no tree grows past a few KiB.
const SHARDS: usize = 32;
const FILES_PER_AGENT: usize = 4;
const SHARED_FILES: usize = 8;
const REF: &str = "main";
const OPS: [&str; 7] = ["land", "create", "cat", "ls", "log", "status", "get_ref"];

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Server replica processes.
    #[arg(long, default_value_t = 2)]
    pub replicas: usize,
    /// Simulated agents (async tasks).
    #[arg(long, default_value_t = 300)]
    pub agents: usize,
    /// Run duration in seconds.
    #[arg(long, default_value_t = 600)]
    pub duration: u64,
    /// Seconds per reporting window.
    #[arg(long, default_value_t = 30)]
    pub window: u64,
    /// Mean agent think time between operations (exponential), ms.
    #[arg(long, default_value_t = 500)]
    pub think_ms: u64,
    /// Probability that a change also rewrites a shared one-line file.
    #[arg(long, default_value_t = 0.02)]
    pub conflict_rate: f64,
    /// gRPC connections per replica shared by its agents.
    #[arg(long, default_value_t = 4)]
    pub channels_per_replica: usize,
    #[arg(long, env = "ANT_PG_URL")]
    pub pg_url: String,
    /// Schema for this run (default: soak_<random>); dropped afterwards.
    #[arg(long)]
    pub pg_schema: Option<String>,
    /// Postgres connections all replicas together may open.
    #[arg(long, default_value_t = 400)]
    pub pg_budget: u32,
    /// Ref-lock connections per replica (part of the budget; 0 = no ref
    /// locks, CAS only, for comparison).
    #[arg(long, default_value_t = 4)]
    pub pg_lock_connections: u32,
    /// S3 endpoint for large objects (optional).
    #[arg(long)]
    pub s3_endpoint: Option<String>,
    #[arg(long, default_value = "ant-soak")]
    pub s3_bucket: String,
    #[arg(long, default_value = "soak")]
    pub s3_access_key: String,
    #[arg(long, default_value = "soak")]
    pub s3_secret_key: String,
    /// Server executable run as `<exe> serve ...` (default: this binary).
    #[arg(long)]
    pub server_exe: Option<PathBuf>,
    /// Directory for replica logs (default: the system temp dir).
    #[arg(long)]
    pub log_dir: Option<PathBuf>,
    /// Keep the schema and S3 data afterwards.
    #[arg(long)]
    pub keep: bool,
    #[arg(long, default_value_t = 0.001)]
    pub max_error_rate: f64,
    #[arg(long, default_value_t = 2.0)]
    pub max_p99_drift: f64,
    /// Window p99s below this never count as drift.
    #[arg(long, default_value_t = 5.0)]
    pub p99_floor_ms: f64,
    #[arg(long, default_value_t = 256.0)]
    pub max_rss_growth_mib_per_hour: f64,
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    #[arg(long)]
    pub json: Option<PathBuf>,
}

type Client = AntClient<Channel>;

fn client(channel: Channel) -> Client {
    AntClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
}

async fn connect(url: &str) -> Result<Channel> {
    Channel::from_shared(url.to_owned())?
        .connect()
        .await
        .with_context(|| format!("connecting to {url}"))
}

fn sig() -> pb::Signature {
    let time =
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
    pb::Signature {
        name: "soak".into(),
        email: "soak@example.com".into(),
        time,
        tz_offset_minutes: 0,
    }
}

fn write_op(path: String, content: Vec<u8>) -> pb::FileOp {
    pb::FileOp {
        path,
        op: Some(pb::file_op::Op::Write(pb::WriteOp {
            mode: pb::EntryMode::File as i32,
            content: Some(pb::write_op::Content::Inline(content)),
        })),
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn agent_dir(i: usize) -> String {
    format!("agents/{:02}/{i}", i % SHARDS)
}

// ---------------------------------------------------------------------------
// Recording
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Window {
    lat: HashMap<&'static str, Vec<u64>>,
    ops: u64,
    errors: u64,
    aborts: u64,
    conflicts: u64,
    landed: u64,
    first_errors: Vec<String>,
}

#[derive(Default)]
struct Recorder {
    window: Mutex<Window>,
    stop: AtomicBool,
}

impl Recorder {
    fn with<T>(&self, f: impl FnOnce(&mut Window) -> T) -> T {
        f(&mut self.window.lock().unwrap_or_else(|e| e.into_inner()))
    }

    fn ok(&self, op: &'static str, start: Instant) {
        let micros = start.elapsed().as_micros() as u64;
        self.with(|w| {
            w.ops += 1;
            w.lat.entry(op).or_default().push(micros);
        });
    }

    fn err(&self, op: &'static str, e: &tonic::Status) {
        self.with(|w| {
            w.ops += 1;
            w.errors += 1;
            if e.code() == tonic::Code::Aborted {
                w.aborts += 1;
            }
            if w.first_errors.len() < 5 {
                w.first_errors.push(format!("{op}: {e}"));
            }
        });
    }

    fn take(&self) -> Window {
        self.with(std::mem::take)
    }
}

// ---------------------------------------------------------------------------
// Agents
// ---------------------------------------------------------------------------

#[derive(Default)]
struct AgentOut {
    landed: u64,
    /// Lands whose outcome is unknown (the RPC failed).
    uncertain: u64,
    /// path -> content of the last successful land touching it.
    last: HashMap<String, Vec<u8>>,
    /// Paths written by uncertain lands (not verified).
    unknown: Vec<String>,
}

struct AgentCfg {
    think_ms: u64,
    conflict_rate: f64,
    seed: u64,
    start: Vec<u8>,
}

async fn agent(
    i: usize,
    mut c: Client,
    cfg: Arc<AgentCfg>,
    rec: Arc<Recorder>,
    agents: usize,
) -> AgentOut {
    let mut rng = StdRng::seed_from_u64(cfg.seed.wrapping_mul(1_000_003).wrapping_add(i as u64));
    let mut out = AgentOut::default();
    let mut base = cfg.start.clone();
    let mut seq = 0u64;
    let dir = agent_dir(i);
    // Spread the start so agents do not move in lockstep.
    let jitter = rng.random_range(0..=cfg.think_ms.max(1));
    tokio::time::sleep(Duration::from_millis(jitter)).await;
    while !rec.stop.load(Ordering::Relaxed) {
        let u: f64 = rng.random_range(f64::EPSILON..1.0);
        let think = (-u.ln() * cfg.think_ms as f64).min(cfg.think_ms as f64 * 10.0);
        tokio::time::sleep(Duration::from_micros((think * 1000.0) as u64)).await;
        if rec.stop.load(Ordering::Relaxed) {
            break;
        }
        seq += 1;
        let pick = rng.random_range(0..100);
        if pick < 35 {
            let mut ops = Vec::new();
            let mut written = Vec::new();
            let first = rng.random_range(0..FILES_PER_AGENT);
            for k in 0..rng.random_range(1..=2) {
                let path = format!("{dir}/f{}.txt", (first + k) % FILES_PER_AGENT);
                let content = format!("agent={i} seq={seq}\n").into_bytes();
                ops.push(write_op(path.clone(), content.clone()));
                written.push((path, content));
            }
            if rng.random_bool(cfg.conflict_rate.clamp(0.0, 1.0)) {
                let path = format!("shared/c{}.txt", rng.random_range(0..SHARED_FILES));
                ops.push(write_op(path, format!("agent {i} seq {seq}\n").into_bytes()));
            }
            let t = Instant::now();
            let created = c
                .create_changeset(pb::CreateChangesetRequest {
                    base: Some(base.clone()),
                    ops,
                    author: Some(sig()),
                    committer: None,
                    message: format!("agent {i} change {seq}"),
                })
                .await;
            let commit = match created {
                Ok(r) => {
                    rec.ok("create", t);
                    r.into_inner().commit
                }
                Err(e) => {
                    rec.err("create", &e);
                    continue;
                }
            };
            let t = Instant::now();
            match c.land(pb::LandRequest { ref_name: REF.into(), commit }).await {
                Ok(r) => {
                    rec.ok("land", t);
                    match r.into_inner().outcome {
                        Some(pb::land_response::Outcome::Landed(l)) => {
                            rec.with(|w| w.landed += 1);
                            out.landed += 1;
                            base = l.new_tip;
                            out.last.extend(written);
                        }
                        Some(pb::land_response::Outcome::Conflict(conflict)) => {
                            rec.with(|w| w.conflicts += 1);
                            base = conflict.tip;
                        }
                        None => rec.with(|w| {
                            w.errors += 1;
                            w.first_errors.push("land: empty outcome".into());
                        }),
                    }
                }
                Err(e) => {
                    rec.err("land", &e);
                    out.uncertain += 1;
                    out.unknown.extend(written.into_iter().map(|(p, _)| p));
                }
            }
        } else if pick < 55 {
            let other = rng.random_range(0..agents);
            let path = if rng.random_bool(0.1) {
                "README.md".to_string()
            } else {
                format!("{}/f{}.txt", agent_dir(other), rng.random_range(0..FILES_PER_AGENT))
            };
            let t = Instant::now();
            let req = pb::ReadPathRequest { rev: REF.into(), path, include_content: true };
            match c.read_path(req).await {
                Ok(_) => rec.ok("cat", t),
                Err(e) => rec.err("cat", &e),
            }
        } else if pick < 70 {
            let path = format!("agents/{:02}", rng.random_range(0..SHARDS));
            let t = Instant::now();
            let req = pb::ReadPathRequest { rev: REF.into(), path, include_content: false };
            match c.read_path(req).await {
                Ok(_) => rec.ok("ls", t),
                Err(e) => rec.err("ls", &e),
            }
        } else if pick < 80 {
            let path = rng.random_bool(0.5).then(|| dir.clone());
            let t = Instant::now();
            match c.log(pb::LogRequest { rev: REF.into(), path, limit: 20 }).await {
                Ok(_) => rec.ok("log", t),
                Err(e) => rec.err("log", &e),
            }
        } else {
            // Workspace status: where is the ref, what changed since my base.
            let t = Instant::now();
            let tip = match c.get_ref(pb::GetRefRequest { name: REF.into() }).await {
                Ok(r) => {
                    rec.ok("get_ref", t);
                    r.into_inner().target
                }
                Err(e) => {
                    rec.err("get_ref", &e);
                    continue;
                }
            };
            let Some(tip) = tip else { continue };
            let t = Instant::now();
            let req = pb::DiffRequest { from_rev: hex(&base), to_rev: hex(&tip) };
            match c.diff(req).await {
                Ok(_) => {
                    rec.ok("status", t);
                    base = tip;
                }
                Err(e) => rec.err("status", &e),
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Monitoring
// ---------------------------------------------------------------------------

struct ReplicaSample {
    rss: Option<u64>,
    alive: bool,
    pg_conns: i64,
    cache_bytes: Option<u64>,
    cache_entries: Option<u64>,
    queued: Option<u64>,
    catch_ups: Option<u64>,
}

struct WindowReport {
    end_s: f64,
    secs: f64,
    ops: u64,
    errors: u64,
    aborts: u64,
    conflicts: u64,
    landed: u64,
    lat: BTreeMap<&'static str, Lat>,
    replicas: Vec<ReplicaSample>,
    pg_total: i64,
    first_errors: Vec<String>,
}

/// Sum of every `"<key>":<number>` in a flat JSON text.
fn json_sum(text: &str, key: &str) -> Option<u64> {
    let needle = format!("\"{key}\":");
    let mut total = 0;
    let mut found = false;
    for part in text.split(&needle).skip(1) {
        let digits: String = part.chars().take_while(char::is_ascii_digit).collect();
        total += digits.parse::<u64>().ok()?;
        found = true;
    }
    found.then_some(total)
}

async fn sample(replicas: &mut [Replica], pg: &mut Option<PgMonitor>) -> (Vec<ReplicaSample>, i64) {
    let conns: Vec<(String, i64)> = match pg {
        Some(m) => m.connections().await.unwrap_or_default(),
        None => Vec::new(),
    };
    let pg_total = conns.iter().map(|(_, n)| n).sum();
    let mut out = Vec::new();
    for r in replicas.iter_mut() {
        let app = format!("soak-r{}", r.index);
        let stats = r.stats().await.ok();
        let stats = stats.as_deref();
        out.push(ReplicaSample {
            rss: r.rss_bytes(),
            alive: r.alive(),
            pg_conns: conns.iter().filter(|(a, _)| *a == app).map(|(_, n)| n).sum(),
            cache_bytes: stats.and_then(|s| json_sum(s, "weight_bytes")),
            cache_entries: stats.and_then(|s| json_sum(s, "entries")),
            queued: stats.and_then(|s| json_sum(s, "requests")),
            catch_ups: stats.and_then(|s| json_sum(s, "catch_ups")),
        });
    }
    (out, pg_total)
}

fn print_header(n: usize) {
    let rss: Vec<String> = (0..n).map(|i| format!("rss{i}")).collect();
    println!(
        "{:>6} {:>8} {:>7} {:>5} {:>5} {:>6} | {:>15} {:>8} {:>8} {:>8} {:>8} {:>8} | {} | pg(per replica/total) | cache MiB | queued",
        "t(s)",
        "ops/s",
        "lands/s",
        "err",
        "abrt",
        "confl",
        "land p50/p99",
        "create99",
        "cat99",
        "ls99",
        "log99",
        "status99",
        rss.join(" ")
    );
}

fn print_window(w: &WindowReport) {
    let p99 = |op: &str| w.lat.get(op).map_or(0.0, |l| l.p99_ms);
    let land = w.lat.get("land").cloned().unwrap_or_default();
    let rss: Vec<String> =
        w.replicas.iter().map(|r| format!("{:>5.0}", r.rss.map_or(0.0, mib))).collect();
    let pg: Vec<String> = w.replicas.iter().map(|r| r.pg_conns.to_string()).collect();
    let cache: Vec<String> =
        w.replicas.iter().map(|r| format!("{:.0}", r.cache_bytes.map_or(0.0, mib))).collect();
    let queued: Vec<String> =
        w.replicas.iter().map(|r| r.queued.map_or("-".into(), |q| q.to_string())).collect();
    println!(
        "{:>6.0} {:>8.1} {:>7.1} {:>5} {:>5} {:>6} | {:>6.1}/{:>7.1} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8.1} | {} | {}/{} | {} | {}",
        w.end_s,
        w.ops as f64 / w.secs,
        w.landed as f64 / w.secs,
        w.errors,
        w.aborts,
        w.conflicts,
        land.p50_ms,
        land.p99_ms,
        p99("create"),
        p99("cat"),
        p99("ls"),
        p99("log"),
        p99("status"),
        rss.join(" "),
        pg.join(","),
        w.pg_total,
        cache.join(","),
        queued.join(","),
    );
    for e in &w.first_errors {
        println!("    error: {e}");
    }
}

/// Least-squares slope of `(x, y)` points.
fn slope(points: &[(f64, f64)]) -> f64 {
    let n = points.len() as f64;
    if n < 2.0 {
        return 0.0;
    }
    let mx = points.iter().map(|p| p.0).sum::<f64>() / n;
    let my = points.iter().map(|p| p.1).sum::<f64>() / n;
    let num: f64 = points.iter().map(|(x, y)| (x - mx) * (y - my)).sum();
    let den: f64 = points.iter().map(|(x, _)| (x - mx) * (x - mx)).sum();
    if den == 0.0 { 0.0 } else { num / den }
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

fn with_param(url: &str, key: &str, value: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}{key}={value}")
}

pub async fn run(args: Args) -> Result<i32> {
    if args.replicas == 0 || args.agents == 0 || args.window == 0 {
        bail!("--replicas, --agents and --window must be positive");
    }
    if let Ok(home) = std::env::var("HOME")
        && std::path::Path::new(&home).join(".local/share/antimony-services/DISK_LOW").exists()
    {
        bail!("disk is low (antimony-services DISK_LOW marker present); not starting a soak");
    }
    let schema =
        args.pg_schema.clone().unwrap_or_else(|| format!("soak_{}", rand::random::<u32>()));
    if !schema.starts_with("soak_") {
        bail!("--pg-schema must start with soak_ (it is dropped afterwards)");
    }
    let per_replica = args.pg_budget / args.replicas as u32;
    let lock_conns = args.pg_lock_connections;
    let max_conns = per_replica.saturating_sub(lock_conns).min(64);
    if max_conns < 4 {
        bail!("--pg-budget {} is too small for {} replicas", args.pg_budget, args.replicas);
    }
    let s3 = args.s3_endpoint.as_ref().map(|endpoint| S3Config {
        endpoint: endpoint.clone(),
        region: "us-east-1".into(),
        bucket: args.s3_bucket.clone(),
        prefix: format!("{schema}/"),
        access_key: args.s3_access_key.clone(),
        secret_key: args.s3_secret_key.clone(),
    });
    let exe = match &args.server_exe {
        Some(p) => p.clone(),
        None => std::env::current_exe().context("locating this executable")?,
    };
    let log_dir = args.log_dir.clone().unwrap_or_else(std::env::temp_dir);
    std::fs::create_dir_all(&log_dir).context("creating log dir")?;

    println!(
        "soak: replicas={} agents={} duration={}s window={}s think={}ms schema={schema} s3={} \
         pg pools per replica: {max_conns} query + {lock_conns} lock (budget {})",
        args.replicas,
        args.agents,
        args.duration,
        args.window,
        args.think_ms,
        s3.as_ref().map_or("off".to_string(), |c| format!("{}/{}", c.bucket, c.prefix)),
        args.pg_budget,
    );

    let result = drive(&args, &schema, &s3, &exe, &log_dir, max_conns, lock_conns).await;

    if !args.keep {
        if let Err(e) = drop_schema(&args.pg_url, &schema).await {
            eprintln!("warning: dropping schema {schema}: {e:#}");
        }
        if let Some(cfg) = &s3 {
            match crate::s3clean::delete_prefix(cfg, false).await {
                Ok(n) => println!("cleanup: dropped schema {schema}, deleted {n} S3 objects"),
                Err(e) => eprintln!("warning: deleting S3 prefix {}: {e:#}", cfg.prefix),
            }
        } else {
            println!("cleanup: dropped schema {schema}");
        }
    }
    result
}

async fn drop_schema(url: &str, schema: &str) -> Result<()> {
    let mut conn = sqlx::PgConnection::connect(url).await?;
    let stmt = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema.replace('"', "\"\""));
    sqlx::query(sqlx::AssertSqlSafe(stmt)).execute(&mut conn).await?;
    conn.close().await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn drive(
    args: &Args,
    schema: &str,
    s3: &Option<S3Config>,
    exe: &std::path::Path,
    log_dir: &std::path::Path,
    max_conns: u32,
    lock_conns: u32,
) -> Result<i32> {
    // ---- replicas ----
    let mut replicas = Vec::with_capacity(args.replicas);
    for i in 0..args.replicas {
        let mut argv = vec![
            "--pg-url".to_string(),
            with_param(&args.pg_url, "application_name", &format!("soak-r{i}")),
            "--pg-schema".into(),
            schema.into(),
            "--pg-max-connections".into(),
            max_conns.to_string(),
            "--pg-lock-connections".into(),
            lock_conns.to_string(),
        ];
        if let Some(cfg) = s3 {
            argv.extend([
                "--s3-endpoint".into(),
                cfg.endpoint.clone(),
                "--s3-bucket".into(),
                cfg.bucket.clone(),
                "--s3-prefix".into(),
                cfg.prefix.clone(),
                "--s3-access-key".into(),
                cfg.access_key.clone(),
                "--s3-secret-key".into(),
                cfg.secret_key.clone(),
            ]);
        }
        let log = log_dir.join(format!("{schema}-r{i}.log"));
        let env = [("RUST_LOG".to_string(), "warn".to_string())];
        replicas.push(Replica::spawn(i, exe, &argv, &env, &log).await?);
        println!(
            "replica {i}: pid {} grpc {} log {}",
            replicas[i].pid,
            replicas[i].grpc_url,
            log.display()
        );
    }
    let mut pg = PgMonitor::connect(&args.pg_url).await.ok();

    // ---- channels and seed ----
    let mut channels = Vec::new();
    for r in &replicas {
        let mut per = Vec::new();
        for _ in 0..args.channels_per_replica.max(1) {
            per.push(connect(&r.grpc_url).await?);
        }
        channels.push(per);
    }
    let mut admin = client(channels[0][0].clone());
    let mut ops = vec![write_op("README.md".into(), b"soak seed\n".to_vec())];
    for k in 0..SHARED_FILES {
        ops.push(write_op(format!("shared/c{k}.txt"), b"init\n".to_vec()));
    }
    let seed = admin
        .create_changeset(pb::CreateChangesetRequest {
            base: None,
            ops,
            author: Some(sig()),
            committer: None,
            message: "soak seed".into(),
        })
        .await?
        .into_inner()
        .commit;
    let r = admin
        .update_ref(pb::UpdateRefRequest {
            name: REF.into(),
            expected: None,
            new_target: Some(seed.clone()),
        })
        .await?
        .into_inner();
    if !r.updated {
        bail!("schema {schema} already has ref {REF}; use a fresh --pg-schema");
    }

    // ---- agents ----
    let rec = Arc::new(Recorder::default());
    let cfg = Arc::new(AgentCfg {
        think_ms: args.think_ms,
        conflict_rate: args.conflict_rate,
        seed: args.seed,
        start: seed.clone(),
    });
    let start = Instant::now();
    let handles: Vec<_> = (0..args.agents)
        .map(|i| {
            let r = i % args.replicas;
            let per = &channels[r];
            let ch = per[(i / args.replicas) % per.len()].clone();
            tokio::spawn(agent(i, client(ch), cfg.clone(), rec.clone(), args.agents))
        })
        .collect();

    // ---- windows ----
    print_header(args.replicas);
    let mut windows: Vec<WindowReport> = Vec::new();
    let n_windows = args.duration.div_ceil(args.window);
    let mut last = start;
    for k in 1..=n_windows {
        let end = start + Duration::from_secs((k * args.window).min(args.duration));
        tokio::time::sleep_until(end.into()).await;
        let w = rec.take();
        let now = Instant::now();
        let secs = now.duration_since(last).as_secs_f64();
        last = now;
        let (samples, pg_total) = sample(&mut replicas, &mut pg).await;
        let lat = w.lat.into_iter().map(|(op, v)| (op, Lat::from_micros(v))).collect();
        let report = WindowReport {
            end_s: now.duration_since(start).as_secs_f64(),
            secs,
            ops: w.ops,
            errors: w.errors,
            aborts: w.aborts,
            conflicts: w.conflicts,
            landed: w.landed,
            lat,
            replicas: samples,
            pg_total,
            first_errors: w.first_errors,
        };
        print_window(&report);
        windows.push(report);
        if windows.last().is_some_and(|w| w.replicas.iter().any(|r| !r.alive)) {
            println!("a replica died; stopping early");
            break;
        }
    }
    rec.stop.store(true, Ordering::Relaxed);
    let mut outs = Vec::new();
    let mut failures = Vec::new();
    for (i, h) in handles.into_iter().enumerate() {
        match tokio::time::timeout(Duration::from_secs(120), h).await {
            Ok(Ok(o)) => outs.push(o),
            Ok(Err(e)) => failures.push(format!("agent {i} panicked: {e}")),
            Err(_) => failures.push(format!("agent {i} did not stop within 120 s")),
        }
    }
    // Operations finishing after the last window count toward the totals.
    let tail = rec.take();
    let wall = start.elapsed().as_secs_f64();

    // ---- totals ----
    let ops: u64 = windows.iter().map(|w| w.ops).sum::<u64>() + tail.ops;
    let errors: u64 = windows.iter().map(|w| w.errors).sum::<u64>() + tail.errors;
    let aborts: u64 = windows.iter().map(|w| w.aborts).sum::<u64>() + tail.aborts;
    let conflicts: u64 = windows.iter().map(|w| w.conflicts).sum::<u64>() + tail.conflicts;
    let landed: u64 = outs.iter().map(|o| o.landed).sum();
    let uncertain: u64 = outs.iter().map(|o| o.uncertain).sum();
    let error_rate = if ops == 0 { 0.0 } else { errors as f64 / ops as f64 };

    // ---- invariants ----
    let final_tip = admin
        .get_ref(pb::GetRefRequest { name: REF.into() })
        .await?
        .into_inner()
        .target
        .ok_or_else(|| anyhow!("ref {REF} vanished"))?;
    let verify_start = Instant::now();
    let store = PgStore::connect_with(
        &args.pg_url,
        schema,
        &PgOptions { max_connections: 4, ..PgOptions::default() },
    )
    .await
    .context("connecting the verifier")?;
    let (growth, linear) =
        chain_growth(&store, ObjectId::from_bytes(&final_tip)?, ObjectId::from_bytes(&seed)?)
            .await?;
    match growth {
        Some(g) if g >= landed && g <= landed + uncertain => {}
        Some(g) => failures.push(format!(
            "first-parent history grew by {g}, expected {landed} successful lands (+ up to {uncertain} uncertain)"
        )),
        None => failures.push("seed commit is not on the ref's first-parent chain".into()),
    }
    if !linear {
        failures.push("history is not linear (a commit with != 1 parent)".into());
    }
    let unknown: std::collections::HashSet<String> =
        outs.iter().flat_map(|o| o.unknown.iter().cloned()).collect();
    let mut expected: Vec<(String, Vec<u8>)> = outs
        .into_iter()
        .flat_map(|o| o.last.into_iter())
        .filter(|(p, _)| !unknown.contains(p))
        .collect();
    expected.sort();
    let final_hex = hex(&final_tip);
    let mut bad = 0u64;
    for chunk in expected.chunks(64) {
        let mut set = tokio::task::JoinSet::new();
        for (path, content) in chunk.iter().cloned() {
            let mut c = admin.clone();
            let rev = final_hex.clone();
            set.spawn(async move {
                let r = c
                    .read_path(pb::ReadPathRequest {
                        rev,
                        path: path.clone(),
                        include_content: true,
                    })
                    .await;
                (path, content, r.map(|r| r.into_inner()))
            });
        }
        while let Some(joined) = set.join_next().await {
            let (path, want, r) = joined?;
            match r {
                Ok(r) if r.found && r.content == want => {}
                Ok(_) => {
                    bad += 1;
                    if bad <= 3 {
                        failures.push(format!("{path}: missing or not its last landed content"));
                    }
                }
                Err(e) => failures.push(format!("{path}: read failed: {e}")),
            }
        }
    }
    if bad > 0 {
        failures.push(format!("{bad} of {} agent files wrong at the final tip", expected.len()));
    }
    let verify_s = verify_start.elapsed().as_secs_f64();

    // ---- health criteria ----
    if error_rate > args.max_error_rate {
        failures.push(format!(
            "error rate {:.4}% > {:.4}% ({errors} errors, {aborts} aborts of {ops} ops)",
            error_rate * 100.0,
            args.max_error_rate * 100.0
        ));
    }
    for (i, r) in replicas.iter_mut().enumerate() {
        if !r.alive() {
            failures.push(format!("replica {i} died"));
        }
    }
    let half = windows.len() / 2;
    let mut rss_slopes = Vec::new();
    for i in 0..args.replicas {
        let pts: Vec<(f64, f64)> = windows[half..]
            .iter()
            .filter_map(|w| w.replicas.get(i)?.rss.map(|r| (w.end_s, mib(r))))
            .collect();
        let per_hour = if pts.len() >= 3 { slope(&pts) * 3600.0 } else { 0.0 };
        rss_slopes.push(per_hour);
        if per_hour > args.max_rss_growth_mib_per_hour {
            failures.push(format!(
                "replica {i} RSS grows {per_hour:.0} MiB/h over the second half (limit {:.0})",
                args.max_rss_growth_mib_per_hour
            ));
        }
    }
    if let Some(first) = windows.first() {
        for op in OPS {
            let Some(base) = first.lat.get(op).filter(|l| l.n >= 50) else { continue };
            for w in &windows[1..] {
                if let Some(l) = w.lat.get(op).filter(|l| l.n >= 50)
                    && l.p99_ms > base.p99_ms * args.max_p99_drift
                    && l.p99_ms > args.p99_floor_ms
                {
                    failures.push(format!(
                        "{op} p99 {:.1} ms at t={:.0}s > {}x first window ({:.1} ms)",
                        l.p99_ms, w.end_s, args.max_p99_drift, base.p99_ms
                    ));
                }
            }
        }
    }

    for r in replicas {
        r.shutdown().await;
    }

    // ---- report ----
    let pass = failures.is_empty();
    println!();
    println!("== soak summary ==");
    println!("wall            {wall:.0}s (verification {verify_s:.1}s)");
    println!("ops             {ops} ({:.1}/s)", ops as f64 / wall);
    println!("lands           {landed} ({:.1}/s), uncertain {uncertain}", landed as f64 / wall);
    println!("history growth  {}", growth.map_or("-".into(), |g| g.to_string()));
    println!("conflicts       {conflicts}");
    println!("errors          {errors} ({:.4}%), aborts {aborts}", error_rate * 100.0);
    let slopes: Vec<String> = rss_slopes.iter().map(|s| format!("{s:.0}")).collect();
    println!("RSS trend       {} MiB/h (second half, per replica)", slopes.join(", "));
    println!("files checked   {}", expected.len());
    println!("result: {}", if pass { "PASS" } else { "FAIL" });
    for f in &failures {
        println!("  - {f}");
    }

    let windows_json: Vec<J> = windows
        .iter()
        .map(|w| {
            let mut lat = J::obj();
            for (op, l) in &w.lat {
                lat = lat.with(op, l.json());
            }
            let reps: Vec<J> = w
                .replicas
                .iter()
                .map(|r| {
                    J::obj()
                        .with("rss_bytes", r.rss)
                        .with("alive", r.alive)
                        .with("pg_connections", r.pg_conns)
                        .with("cache_bytes", r.cache_bytes)
                        .with("cache_entries", r.cache_entries)
                        .with("land_queued", r.queued)
                        .with("catch_ups", r.catch_ups)
                })
                .collect();
            J::obj()
                .with("end_s", w.end_s)
                .with("ops_per_s", w.ops as f64 / w.secs)
                .with("lands_per_s", w.landed as f64 / w.secs)
                .with("errors", w.errors)
                .with("aborts", w.aborts)
                .with("conflicts", w.conflicts)
                .with("latency", lat)
                .with("replicas", J::Arr(reps))
                .with("pg_connections_total", w.pg_total)
        })
        .collect();
    let json = J::obj()
        .with("command", "soak")
        .with("replicas", args.replicas)
        .with("agents", args.agents)
        .with("duration_s", args.duration)
        .with("think_ms", args.think_ms)
        .with("pg_max_connections_per_replica", max_conns)
        .with("pg_lock_connections_per_replica", lock_conns)
        .with("wall_s", wall)
        .with("ops", ops)
        .with("landed", landed)
        .with("uncertain", uncertain)
        .with("history_growth", growth)
        .with("conflicts", conflicts)
        .with("errors", errors)
        .with("aborts", aborts)
        .with("error_rate", error_rate)
        .with("rss_growth_mib_per_hour", J::Arr(rss_slopes.into_iter().map(J::from).collect()))
        .with("windows", J::Arr(windows_json))
        .with("pass", pass)
        .with("failures", J::Arr(failures.iter().map(|f| J::from(f.as_str())).collect()));
    write_json(&args.json, &json)?;
    Ok(if pass { 0 } else { 1 })
}

/// Commits on the first-parent chain from `tip` down to `seed` (exclusive),
/// and whether every one of them has exactly one parent. `None` when `seed`
/// is not on the chain.
async fn chain_growth(
    store: &PgStore,
    tip: ObjectId,
    seed: ObjectId,
) -> Result<(Option<u64>, bool)> {
    const CHUNK: usize = 8192;
    let mut count = 0u64;
    let mut linear = true;
    let mut from = tip;
    loop {
        if from == seed {
            return Ok((Some(count), linear));
        }
        let chain = store.first_parent_chain(from, Some(seed), CHUNK).await?;
        let Some(last) = chain.last() else { return Ok((None, linear)) };
        linear &= chain.iter().all(|n| n.parents.len() == 1);
        count += chain.len() as u64;
        match last.parents.first() {
            Some(p) if *p == seed => return Ok((Some(count), linear)),
            Some(p) if chain.len() == CHUNK => from = *p,
            _ => return Ok((None, linear)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_sum_adds_every_occurrence() {
        let s = r#"{"caches":[{"name":"trees","entries":2,"weight_bytes":10},{"name":"nodes","entries":3,"weight_bytes":5}],"land_queues":{"queues":1,"requests":7},"catch_ups":0}"#;
        assert_eq!(json_sum(s, "weight_bytes"), Some(15));
        assert_eq!(json_sum(s, "entries"), Some(5));
        assert_eq!(json_sum(s, "requests"), Some(7));
        assert_eq!(json_sum(s, "catch_ups"), Some(0));
        assert_eq!(json_sum(s, "missing"), None);
    }

    #[test]
    fn slope_fits_a_line() {
        let pts: Vec<_> = (0..5).map(|x| (x as f64, 3.0 * x as f64 + 1.0)).collect();
        assert!((slope(&pts) - 3.0).abs() < 1e-9);
        assert_eq!(slope(&[(1.0, 2.0)]), 0.0);
    }

    #[test]
    fn url_params_append() {
        assert_eq!(with_param("postgres://h/db", "a", "b"), "postgres://h/db?a=b");
        assert_eq!(with_param("postgres://h/db?x=1", "a", "b"), "postgres://h/db?x=1&a=b");
    }
}
