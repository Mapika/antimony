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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ant_core::ObjectId;
use ant_proto::MAX_MESSAGE_BYTES;
use ant_proto::v1 as pb;
use ant_proto::v1::ant_client::AntClient;
use ant_server::config::StoreArgs;
use ant_server::{http, rpc};
use anyhow::{Context as _, Result, anyhow, bail};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tonic::transport::Channel;

use crate::json::J;
use crate::util::{Lat, Metrics, cpu_seconds, instrument, peak_rss_bytes, write_json};

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

type Client = AntClient<Channel>;

async fn connect(url: &str) -> Result<Client> {
    let channel = Channel::from_shared(url.to_owned())?
        .connect()
        .await
        .with_context(|| format!("connecting to {url}"))?;
    Ok(AntClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES))
}

fn sig() -> pb::Signature {
    let time =
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
    pb::Signature {
        name: "bench".into(),
        email: "bench@example.com".into(),
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

/// Create `ref_name` with a small initial tree when it does not exist.
async fn seed_if_missing(client: &mut Client, ref_name: &str) -> Result<()> {
    let existing =
        client.get_ref(pb::GetRefRequest { name: ref_name.into() }).await?.into_inner().target;
    if existing.is_some() {
        return Ok(());
    }
    let hot: String = (0..HOT_LINES).map(|i| format!("line {i} init\n")).collect();
    let commit = client
        .create_changeset(pb::CreateChangesetRequest {
            base: None,
            ops: vec![
                write_op(HOT_PATH.into(), hot.into_bytes()),
                write_op("README.md".into(), b"ant-bench seed\n".to_vec()),
            ],
            author: Some(sig()),
            committer: None,
            message: "bench seed".into(),
        })
        .await?
        .into_inner()
        .commit;
    let r = client
        .update_ref(pb::UpdateRefRequest {
            name: ref_name.into(),
            expected: None,
            new_target: Some(commit),
        })
        .await?
        .into_inner();
    if !r.updated {
        // Someone else seeded concurrently; that's fine.
        tracing_note("seed raced with another seeder");
    }
    Ok(())
}

fn tracing_note(msg: &str) {
    eprintln!("note: {msg}");
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

fn micros(start: Instant) -> u64 {
    start.elapsed().as_micros() as u64
}

async fn agent(i: usize, url: String, a: AgentCfg, deadline: Instant) -> Result<AgentOut> {
    let mut client = connect(&url).await?;
    let mut rng = StdRng::seed_from_u64(a.seed.wrapping_mul(1_000_003).wrapping_add(i as u64));
    let mut out = AgentOut::default();
    let mut seq = 0u64;
    while Instant::now() < deadline {
        seq += 1;
        let t = Instant::now();
        let tip = match client.get_ref(pb::GetRefRequest { name: a.ref_name.clone() }).await {
            Ok(r) => r.into_inner().target,
            Err(e) => {
                out.errors += 1;
                out.first_error.get_or_insert(format!("get_ref: {e}"));
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
            let t = Instant::now();
            let rd = client
                .read_path(pb::ReadPathRequest {
                    rev: hex(&tip),
                    path: HOT_PATH.into(),
                    include_content: true,
                })
                .await;
            out.read.push(micros(t));
            match rd {
                Ok(r) => {
                    let r = r.into_inner();
                    if r.found {
                        let text = String::from_utf8_lossy(&r.content).into_owned();
                        let mut lines: Vec<String> = text.lines().map(str::to_owned).collect();
                        if !lines.is_empty() {
                            let k = rng.random_range(0..lines.len());
                            lines[k] = format!("line {k} agent={i} seq={seq}");
                            let mut new = lines.join("\n");
                            new.push('\n');
                            ops.push(write_op(HOT_PATH.into(), new.into_bytes()));
                        }
                    }
                }
                Err(e) => {
                    out.errors += 1;
                    out.first_error.get_or_insert(format!("read_path: {e}"));
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
                author: Some(sig()),
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
                out.errors += 1;
                out.first_error.get_or_insert(format!("create_changeset: {e}"));
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
                    for (p, c) in written {
                        out.last.insert(p, c);
                    }
                }
                Some(pb::land_response::Outcome::Conflict(_)) => out.conflicts += 1,
                None => {
                    out.errors += 1;
                    out.first_error.get_or_insert("land: empty outcome".to_string());
                }
            },
            Err(e) if e.code() == tonic::Code::Aborted => {
                out.aborts += 1;
                out.first_error.get_or_insert(format!("land: {e}"));
            }
            Err(e) => {
                out.errors += 1;
                out.first_error.get_or_insert(format!("land: {e}"));
            }
        }
    }
    Ok(out)
}

#[derive(Clone)]
struct AgentCfg {
    ref_name: String,
    files: usize,
    hot_rate: f64,
    seed: u64,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
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
            let grpc = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let http_l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let addr = grpc.local_addr()?;
            tokio::spawn(rpc::serve_on(backend.clone(), grpc));
            tokio::spawn(http::serve_on(backend, http_l));
            (format!("http://{addr}"), "embedded")
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
    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.duration);
    let handles: Vec<_> = (0..args.agents)
        .map(|i| tokio::spawn(agent(i, url.clone(), cfg.clone(), deadline)))
        .collect();
    let mut outs = Vec::with_capacity(handles.len());
    let mut agent_failures = Vec::new();
    for (i, h) in handles.into_iter().enumerate() {
        match h.await {
            Ok(Ok(o)) => outs.push(o),
            Ok(Err(e)) => agent_failures.push(format!("agent {i}: {e:#}")),
            Err(e) => agent_failures.push(format!("agent {i} panicked: {e}")),
        }
    }
    let wall = start.elapsed().as_secs_f64();
    let cpu1 = cpu_seconds().unwrap_or((0.0, 0.0));

    // ---- aggregate ----
    let mut tot = AgentOut::default();
    let mut last: HashMap<String, Vec<u8>> = HashMap::new();
    let mut first_error = None;
    for o in outs {
        tot.get_ref.extend(o.get_ref);
        tot.read.extend(o.read);
        tot.create.extend(o.create);
        tot.land.extend(o.land);
        tot.land_ff.extend(o.land_ff);
        tot.land_rebased.extend(o.land_rebased);
        tot.landed += o.landed;
        tot.rebased += o.rebased;
        tot.conflicts += o.conflicts;
        tot.aborts += o.aborts;
        tot.errors += o.errors;
        tot.hot_attempts += o.hot_attempts;
        tot.hot_landed += o.hot_landed;
        first_error = first_error.or(o.first_error);
        last.extend(o.last);
    }

    // ---- invariants ----
    let mut failures: Vec<String> = agent_failures;
    let final_tip = admin
        .get_ref(pb::GetRefRequest { name: args.ref_name.clone() })
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
    let verify_start = Instant::now();
    let items: Vec<(String, Vec<u8>)> = last.into_iter().collect();
    let checked = items.len();
    for chunk in items.chunks(64) {
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
                    .await
                    .map(|r| r.into_inner());
                (path, content, r)
            });
        }
        while let Some(joined) = set.join_next().await {
            let (path, expected, r) = joined?;
            match r {
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
            }
        }
    }
    if missing + wrong > 0 {
        failures.push(format!(
            "{missing} agent files missing, {wrong} with wrong content (of {checked} checked)"
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
    let verify_s = verify_start.elapsed().as_secs_f64();

    // ---- report ----
    let create = Lat::from_micros(tot.create);
    let land = Lat::from_micros(tot.land);
    let land_ff = Lat::from_micros(tot.land_ff);
    let land_rb = Lat::from_micros(tot.land_rebased);
    let get_ref = Lat::from_micros(tot.get_ref);
    let read = Lat::from_micros(tot.read);
    let lands_per_s = tot.landed as f64 / wall;
    let rebased_frac = if tot.landed == 0 { 0.0 } else { tot.rebased as f64 / tot.landed as f64 };
    let pass = failures.is_empty();

    println!();
    println!("== fleet summary ({mode}) ==");
    println!("wall            {wall:.2}s   (verification {verify_s:.2}s)");
    println!("lands           {} ({lands_per_s:.1}/s)", tot.landed);
    println!("rebased         {} ({:.1}%)", tot.rebased, rebased_frac * 100.0);
    println!("conflicts       {}", tot.conflicts);
    println!("aborts          {}", tot.aborts);
    println!("errors          {}", tot.errors);
    if let Some(e) = &first_error {
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
    if let Some(r) = peak_rss_bytes() {
        println!("peak RSS        {:.1} MiB (client + embedded server)", r as f64 / 1048576.0);
    }
    println!("cpu             user {:.1}s sys {:.1}s", cpu1.0 - cpu0.0, cpu1.1 - cpu0.1);
    if mode == "embedded" {
        metrics.print("server-side store timing (calls from all RPCs)");
    }
    println!("invariants: {}", if pass { "PASS" } else { "FAIL" });
    for f in &failures {
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
        .with("first_error", first_error)
        .with("hot_attempts", tot.hot_attempts)
        .with("hot_landed", tot.hot_landed)
        .with("latency_get_ref", get_ref.json())
        .with("latency_read_path", read.json())
        .with("latency_create", create.json())
        .with("latency_land", land.json())
        .with("latency_land_ff", land_ff.json())
        .with("latency_land_rebased", land_rb.json())
        .with("peak_rss_bytes", peak_rss_bytes())
        .with("cpu_user_s", cpu1.0 - cpu0.0)
        .with("cpu_sys_s", cpu1.1 - cpu0.1)
        .with("history_growth", history_growth.map(|n| n as u64))
        .with("files_checked", checked)
        .with("invariants_pass", pass)
        .with("invariant_failures", J::Arr(failures.iter().map(|f| J::from(f.as_str())).collect()))
        .with("server_store_calls", metrics.json());
    write_json(&args.json, &json)?;
    Ok(if pass { 0 } else { 1 })
}
