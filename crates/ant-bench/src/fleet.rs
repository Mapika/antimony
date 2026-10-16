//! `fleet` subcommand: N simulated agents racing to land on one ref.
//!
//! Each agent loops: GetRef -> (optional ReadPath of the hot file) ->
//! CreateChangeset(base = tip, K inline writes under `agents/<i>/`, maybe one
//! rewritten line of `hot/counter.txt`) -> Land. After the deadline the run is
//! verified: the first-parent history must have grown by exactly the number of
//! successful lands, and every landed agent file must hold its last-landed
//! content at the final tip.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ant_core::ObjectId;
use ant_proto::v1 as pb;
use ant_server::acl::Acl;
use ant_server::config::StoreArgs;
use ant_server::serve::spawn_local_with_metrics;
use anyhow::{Result, anyhow, bail};
use rand::Rng;
use rand::rngs::StdRng;

use crate::json::J;
use crate::rpc::{Client, agent_rng, connect, hex, read_back, seed_ref, sig, write_op};
use crate::util::{
    Lat, Metrics, cpu_seconds, instrument, mib, peak_rss_bytes, proc_cpu_seconds,
    proc_status_bytes, write_json,
};

const HOT_PATH: &str = "hot/counter.txt";
const HOT_LINES: usize = 50;

#[derive(clap::Args, Debug)]
pub struct Args {
    #[arg(long, default_value_t = 16)]
    pub agents: usize,
    /// Run duration in seconds.
    #[arg(long, default_value_t = 20)]
    pub duration: u64,
    /// gRPC URL of a running server (e.g. http://127.0.0.1:7420).
    #[arg(long)]
    pub server: Option<String>,
    /// Serve in-process using the store flags below.
    #[arg(long)]
    pub embedded: bool,
    /// Pid of the server process (with `--server`): report its RSS and CPU
    /// time over the run (e.g. `ant local start` prints it).
    #[arg(long, requires = "server")]
    pub server_pid: Option<u32>,
    /// Files written per changeset.
    #[arg(long, default_value_t = 4)]
    pub files_per_change: usize,
    /// Probability that a change also rewrites one line of the shared hot file.
    #[arg(long, default_value_t = 0.0)]
    pub hot_rate: f64,
    #[arg(long = "ref", default_value = "main")]
    pub ref_name: String,
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    #[arg(long)]
    pub json: Option<PathBuf>,
    #[command(flatten)]
    pub store: StoreArgs,
}

/// Create `ref_name` with a small initial tree when it does not exist.
async fn seed_if_missing(client: &mut Client, ref_name: &str) -> Result<()> {
    let existing =
        client.get_ref(pb::GetRefRequest { name: ref_name.into() }).await?.into_inner().target;
    if existing.is_some() {
        return Ok(());
    }
    let hot: String = (0..HOT_LINES).map(|i| format!("line {i} init\n")).collect();
    let ops = vec![
        write_op(HOT_PATH.into(), hot.into_bytes()),
        write_op("README.md".into(), b"ant-bench seed\n".to_vec()),
    ];
    let (_, created) = seed_ref(client, ref_name, ops, "bench", "bench seed").await?;
    if !created {
        // Someone else seeded concurrently; that's fine.
        eprintln!("note: seed raced with another seeder");
    }
    Ok(())
}

#[derive(Default)]
struct AgentOut {
    get_ref: Vec<u64>,
    read: Vec<u64>,
    create: Vec<u64>,
    land: Vec<u64>,
    /// land latencies split by whether the server rebased.
    land_ff: Vec<u64>,
    land_rebased: Vec<u64>,
    /// whole change, `get_ref` to a successful land (comparable to a git commit+push).
    change: Vec<u64>,
    landed: u64,
    rebased: u64,
    conflicts: u64,
    aborts: u64,
    errors: u64,
    hot_attempts: u64,
    hot_landed: u64,
    first_error: Option<String>,
    /// path -> content of the last successful land touching it.
    last: HashMap<String, Vec<u8>>,
}

impl AgentOut {
    fn error(&mut self, msg: String) {
        self.errors += 1;
        self.first_error.get_or_insert(msg);
    }

    /// Add `o`'s counts and samples to `self` (the first error seen wins).
    fn merge(&mut self, o: AgentOut) {
        self.get_ref.extend(o.get_ref);
        self.read.extend(o.read);
        self.create.extend(o.create);
        self.land.extend(o.land);
        self.land_ff.extend(o.land_ff);
        self.land_rebased.extend(o.land_rebased);
        self.change.extend(o.change);
        self.landed += o.landed;
        self.rebased += o.rebased;
        self.conflicts += o.conflicts;
        self.aborts += o.aborts;
        self.errors += o.errors;
        self.hot_attempts += o.hot_attempts;
        self.hot_landed += o.hot_landed;
        self.first_error = self.first_error.take().or(o.first_error);
        self.last.extend(o.last);
    }
}

fn micros(start: Instant) -> u64 {
    start.elapsed().as_micros() as u64
}

#[derive(Clone)]
struct AgentCfg {
    ref_name: String,
    files: usize,
    hot_rate: f64,
    seed: u64,
}

/// Read the hot file at `tip` and return a write op rewriting one random line
/// of it (`None` when the file is missing or empty).
async fn hot_rewrite(
    client: &mut Client,
    rng: &mut StdRng,
    out: &mut AgentOut,
    tip: &[u8],
    agent: usize,
    seq: u64,
) -> Result<Option<pb::FileOp>, tonic::Status> {
    let t = Instant::now();
    let rd = client
        .read_path(pb::ReadPathRequest {
            rev: hex(tip),
            path: HOT_PATH.into(),
            include_content: true,
        })
        .await;
    out.read.push(micros(t));
    let r = rd?.into_inner();
    if !r.found {
        return Ok(None);
    }
    let text = String::from_utf8_lossy(&r.content).into_owned();
    let mut lines: Vec<String> = text.lines().map(str::to_owned).collect();
    if lines.is_empty() {
        return Ok(None);
    }
    let k = rng.random_range(0..lines.len());
    lines[k] = format!("line {k} agent={agent} seq={seq}");
    let mut new = lines.join("\n");
    new.push('\n');
    Ok(Some(write_op(HOT_PATH.into(), new.into_bytes())))
}

async fn agent(i: usize, url: String, a: AgentCfg, deadline: Instant) -> Result<AgentOut> {
    let mut client = connect(&url).await?;
    let mut rng = agent_rng(a.seed, i);
    let mut out = AgentOut::default();
    let mut seq = 0u64;
    while Instant::now() < deadline {
        seq += 1;
        let t = Instant::now();
        let change_start = t;
        let tip = match client.get_ref(pb::GetRefRequest { name: a.ref_name.clone() }).await {
            Ok(r) => r.into_inner().target,
            Err(e) => {
                out.error(format!("get_ref: {e}"));
                continue;
            }
        };
        out.get_ref.push(micros(t));
        let Some(tip) = tip else {
            bail!("ref {} vanished", a.ref_name);
        };

        let mut ops = Vec::with_capacity(a.files + 1);
        let mut written: Vec<(String, Vec<u8>)> = Vec::with_capacity(a.files);
        for j in 0..a.files {
            let path = format!("agents/{i}/f{j}.txt");
            let content = format!("agent={i} seq={seq} file={j}\n").into_bytes();
            ops.push(write_op(path.clone(), content.clone()));
            written.push((path, content));
        }

        let hot = a.hot_rate > 0.0 && rng.random_bool(a.hot_rate.clamp(0.0, 1.0));
        if hot {
            out.hot_attempts += 1;
            match hot_rewrite(&mut client, &mut rng, &mut out, &tip, i, seq).await {
                Ok(op) => ops.extend(op),
                Err(e) => {
                    out.error(format!("read_path: {e}"));
                    continue;
                }
            }
        }
        let has_hot_op = ops.len() > a.files;

        let t = Instant::now();
        let created = client
            .create_changeset(pb::CreateChangesetRequest {
                base: Some(tip),
                ops,
                author: Some(sig("bench")),
                committer: None,
                message: format!("agent {i} change {seq}"),
            })
            .await;
        let commit = match created {
            Ok(r) => {
                out.create.push(micros(t));
                r.into_inner().commit
            }
            Err(e) => {
                out.error(format!("create_changeset: {e}"));
                continue;
            }
        };

        let t = Instant::now();
        let landed = client.land(pb::LandRequest { ref_name: a.ref_name.clone(), commit }).await;
        let lat = micros(t);
        match landed {
            Ok(r) => match r.into_inner().outcome {
                Some(pb::land_response::Outcome::Landed(l)) => {
                    out.land.push(lat);
                    out.change.push(micros(change_start));
                    out.landed += 1;
                    if l.rebased {
                        out.rebased += 1;
                        out.land_rebased.push(lat);
                    } else {
                        out.land_ff.push(lat);
                    }
                    if has_hot_op {
                        out.hot_landed += 1;
                    }
                    out.last.extend(written);
                }
                Some(pb::land_response::Outcome::Conflict(_)) => out.conflicts += 1,
                None => out.error("land: empty outcome".to_string()),
            },
            Err(e) if e.code() == tonic::Code::Aborted => {
                out.aborts += 1;
                out.first_error.get_or_insert(format!("land: {e}"));
            }
            Err(e) => out.error(format!("land: {e}")),
        }
    }
    Ok(out)
}

/// Outcome of the post-run invariant checks.
struct Verified {
    failures: Vec<String>,
    history_growth: Option<usize>,
    files_checked: usize,
    secs: f64,
}

/// Check the invariants against the final tip: history grew by exactly the
/// successful lands, every landed agent file holds its last landed content,
/// and the hot file still has all its lines.
async fn verify(
    admin: &mut Client,
    ref_name: &str,
    start_tip: ObjectId,
    tot: &AgentOut,
    items: Vec<(String, Vec<u8>)>,
    mut failures: Vec<String>,
) -> Result<Verified> {
    let final_tip = admin
        .get_ref(pb::GetRefRequest { name: ref_name.into() })
        .await?
        .into_inner()
        .target
        .ok_or_else(|| anyhow!("ref vanished"))?;
    let final_hex = hex(&final_tip);
    let log = admin
        .log(pb::LogRequest {
            rev: final_hex.clone(),
            path: None,
            limit: (tot.landed + 2).min(u32::MAX as u64) as u32,
        })
        .await?
        .into_inner()
        .commits;
    let history_growth = log.iter().position(|c| c.id == start_tip.0.to_vec());
    match history_growth {
        Some(n) if n as u64 == tot.landed => {}
        Some(n) => failures.push(format!("first-parent history grew by {n}, expected {} successful lands", tot.landed)),
        None => failures.push(format!(
            "start tip {start_tip} not found within {} first-parent commits of the final tip (expected growth {})",
            log.len(),
            tot.landed
        )),
    }
    let mut missing = 0u64;
    let mut wrong = 0u64;
    let start = Instant::now();
    read_back(admin, &final_hex, &items, |path, expected, r| match r {
        Ok(r) if r.found && r.content == expected => {}
        Ok(r) if r.found => {
            wrong += 1;
            if wrong <= 3 {
                failures.push(format!("{path}: content differs from last landed"));
            }
        }
        Ok(_) => {
            missing += 1;
            if missing <= 3 {
                failures.push(format!("{path}: missing at final tip"));
            }
        }
        Err(e) => failures.push(format!("{path}: read failed: {e}")),
    })
    .await?;
    if missing + wrong > 0 {
        failures.push(format!(
            "{missing} agent files missing, {wrong} with wrong content (of {} checked)",
            items.len()
        ));
    }
    if tot.hot_attempts > 0 {
        let hot = admin
            .read_path(pb::ReadPathRequest {
                rev: final_hex.clone(),
                path: HOT_PATH.into(),
                include_content: true,
            })
            .await?
            .into_inner();
        let lines = String::from_utf8_lossy(&hot.content).lines().count();
        if !hot.found || lines != HOT_LINES {
            failures.push(format!("hot file has {lines} lines, expected {HOT_LINES}"));
        }
    }
    Ok(Verified {
        failures,
        history_growth,
        files_checked: items.len(),
        secs: start.elapsed().as_secs_f64(),
    })
}

pub async fn run(args: Args) -> Result<i32> {
    if args.agents == 0 || args.files_per_change == 0 {
        bail!("--agents and --files-per-change must be positive");
    }
    let metrics = Metrics::default();
    let (url, mode) = match (&args.server, args.embedded) {
        (Some(_), true) => bail!("--server and --embedded are mutually exclusive"),
        (None, false) => bail!("give --server URL or --embedded (with store flags)"),
        (Some(u), false) => (u.clone(), "server"),
        (None, true) => {
            let backend = args.store.backend().await?;
            let backend = instrument(&backend, &metrics, "server.store");
            // Full production instrumentation (metrics layers, store timing).
            let server =
                spawn_local_with_metrics(backend, Acl::open(), ant_server::metrics::Metrics::new())
                    .await?;
            (server.grpc_url(), "embedded")
        }
    };
    let mut admin = connect(&url).await?;
    seed_if_missing(&mut admin, &args.ref_name).await?;
    let start_tip = admin
        .get_ref(pb::GetRefRequest { name: args.ref_name.clone() })
        .await?
        .into_inner()
        .target
        .ok_or_else(|| anyhow!("ref {} missing after seeding", args.ref_name))?;
    let start_tip = ObjectId::from_bytes(&start_tip)?;

    println!(
        "fleet: mode={mode} url={url} agents={} duration={}s files/change={} hot-rate={} ref={}",
        args.agents, args.duration, args.files_per_change, args.hot_rate, args.ref_name
    );
    let cfg = AgentCfg {
        ref_name: args.ref_name.clone(),
        files: args.files_per_change,
        hot_rate: args.hot_rate,
        seed: args.seed,
    };
    let cpu0 = cpu_seconds().unwrap_or((0.0, 0.0));
    let server_pid = args.server_pid.map(|p| p.to_string());
    let server_status = server_pid.as_ref().map(|p| format!("/proc/{p}/status"));
    let server_cpu0 = server_pid.as_deref().and_then(proc_cpu_seconds);
    let server_rss0 = server_status.as_deref().and_then(|s| proc_status_bytes(s, "VmRSS"));
    // The server's peak RSS during the run, sampled.
    let sample_until = Instant::now() + Duration::from_secs(args.duration);
    let sampler = server_status.clone().map(|path| {
        tokio::spawn(async move {
            let mut peak = 0u64;
            loop {
                if let Some(rss) = proc_status_bytes(&path, "VmRSS") {
                    peak = peak.max(rss);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                if Instant::now() >= sample_until {
                    break peak;
                }
            }
        })
    });
    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.duration);
    let handles: Vec<_> = (0..args.agents)
        .map(|i| tokio::spawn(agent(i, url.clone(), cfg.clone(), deadline)))
        .collect();
    let mut tot = AgentOut::default();
    let mut agent_failures = Vec::new();
    for (i, h) in handles.into_iter().enumerate() {
        match h.await {
            Ok(Ok(o)) => tot.merge(o),
            Ok(Err(e)) => agent_failures.push(format!("agent {i}: {e:#}")),
            Err(e) => agent_failures.push(format!("agent {i} panicked: {e}")),
        }
    }
    let wall = start.elapsed().as_secs_f64();
    let cpu1 = cpu_seconds().unwrap_or((0.0, 0.0));
    let server_cpu1 = server_pid.as_deref().and_then(proc_cpu_seconds);
    let server_peak = match sampler {
        Some(h) => h.await.ok(),
        None => None,
    };
    let server_hwm = server_status.as_deref().and_then(|s| proc_status_bytes(s, "VmHWM"));
    let server_cpu = server_cpu0.zip(server_cpu1).map(|(a, b)| (b.0 - a.0, b.1 - a.1));

    let last: Vec<(String, Vec<u8>)> = std::mem::take(&mut tot.last).into_iter().collect();
    let v = verify(&mut admin, &args.ref_name, start_tip, &tot, last, agent_failures).await?;

    // ---- report ----
    let create = Lat::from_micros(tot.create);
    let land = Lat::from_micros(tot.land);
    let land_ff = Lat::from_micros(tot.land_ff);
    let land_rb = Lat::from_micros(tot.land_rebased);
    let change = Lat::from_micros(std::mem::take(&mut tot.change));
    let get_ref = Lat::from_micros(tot.get_ref);
    let read = Lat::from_micros(tot.read);
    let lands_per_s = tot.landed as f64 / wall;
    let rebased_frac = if tot.landed == 0 { 0.0 } else { tot.rebased as f64 / tot.landed as f64 };
    let pass = v.failures.is_empty();

    println!();
    println!("== fleet summary ({mode}) ==");
    println!("wall            {wall:.2}s   (verification {:.2}s)", v.secs);
    println!("lands           {} ({lands_per_s:.1}/s)", tot.landed);
    println!("rebased         {} ({:.1}%)", tot.rebased, rebased_frac * 100.0);
    println!("conflicts       {}", tot.conflicts);
    println!("aborts          {}", tot.aborts);
    println!("errors          {}", tot.errors);
    if let Some(e) = &tot.first_error {
        println!("first error     {e}");
    }
    println!("hot edits       {} attempted, {} landed", tot.hot_attempts, tot.hot_landed);
    println!("latency:");
    println!("{}", get_ref.line("get_ref"));
    if read.n > 0 {
        println!("{}", read.line("read_path"));
    }
    println!("{}", create.line("create"));
    println!("{}", land.line("land"));
    println!("{}", land_ff.line("land(ff)"));
    println!("{}", land_rb.line("land(reb)"));
    println!("{}", change.line("change"));
    if let Some(r) = peak_rss_bytes() {
        println!("peak RSS        {:.1} MiB (client + embedded server)", mib(r));
    }
    println!("cpu             user {:.1}s sys {:.1}s", cpu1.0 - cpu0.0, cpu1.1 - cpu0.1);
    if let Some(pid) = &server_pid {
        println!(
            "server {pid}      RSS before {:.1} MiB, peak during run {:.1} MiB (VmHWM {:.1} MiB)",
            mib(server_rss0.unwrap_or(0)),
            mib(server_peak.unwrap_or(0)),
            mib(server_hwm.unwrap_or(0))
        );
        if let Some((u, s)) = server_cpu {
            println!(
                "server cpu      user {u:.1}s sys {s:.1}s ({:.2} cores)",
                (u + s) / wall.max(1e-9)
            );
        }
    }
    if mode == "embedded" {
        metrics.print("server-side store timing (calls from all RPCs)");
    }
    println!("invariants: {}", if pass { "PASS" } else { "FAIL" });
    for f in &v.failures {
        println!("  - {f}");
    }

    let json = J::obj()
        .with("command", "fleet")
        .with("mode", mode)
        .with("agents", args.agents)
        .with("duration_s", args.duration)
        .with("files_per_change", args.files_per_change)
        .with("hot_rate", args.hot_rate)
        .with("wall_s", wall)
        .with("landed", tot.landed)
        .with("lands_per_s", lands_per_s)
        .with("rebased", tot.rebased)
        .with("rebased_fraction", rebased_frac)
        .with("conflicts", tot.conflicts)
        .with("aborts", tot.aborts)
        .with("errors", tot.errors)
        .with("first_error", tot.first_error)
        .with("hot_attempts", tot.hot_attempts)
        .with("hot_landed", tot.hot_landed)
        .with("latency_get_ref", get_ref.json())
        .with("latency_read_path", read.json())
        .with("latency_create", create.json())
        .with("latency_land", land.json())
        .with("latency_land_ff", land_ff.json())
        .with("latency_land_rebased", land_rb.json())
        .with("latency_change", change.json())
        .with("peak_rss_bytes", peak_rss_bytes())
        .with("cpu_user_s", cpu1.0 - cpu0.0)
        .with("cpu_sys_s", cpu1.1 - cpu0.1)
        .with("server_rss_before_bytes", server_rss0)
        .with("server_rss_peak_bytes", server_peak)
        .with("server_vmhwm_bytes", server_hwm)
        .with("server_cpu_user_s", server_cpu.map(|c| c.0))
        .with("server_cpu_sys_s", server_cpu.map(|c| c.1))
        .with("history_growth", v.history_growth.map(|n| n as u64))
        .with("files_checked", v.files_checked)
        .with("invariants_pass", pass)
        .with(
            "invariant_failures",
            J::Arr(v.failures.iter().map(|f| J::from(f.as_str())).collect()),
        )
        .with("server_store_calls", metrics.json());
    write_json(&args.json, &json)?;
    Ok(if pass { 0 } else { 1 })
}
