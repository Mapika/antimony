//! `checkout` subcommand: time `Workspace::materialize` cold (empty disk cache)
//! and warm (cache populated), plus `status` on the unchanged workspace.
//!
//! Object traffic is counted at two layers: what the workspace asks of the
//! read-through store (`ws.*`) and what actually reaches the server (`net.*`).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use ant_core::ObjectId;
use ant_proto::v1 as pb;
use ant_store::{ObjectStore, ObjectStoreExt};
use antd::disk::DiskObjectStore;
use antd::readthrough::ReadThrough;
use antd::remote::RemoteObjectStore;
use antd::workspace::{Sparse, Workspace};
use anyhow::{Context as _, Result};

use crate::json::J;
use crate::util::{Metrics, cpu_seconds, mib, timed_store};
use crate::{dir_size, dir_size_skip, write_json};

pub struct Args {
    pub server: String,
    pub http: Option<String>,
    pub rev: String,
    pub sparse: Vec<String>,
    pub iterations: usize,
    pub cache: PathBuf,
    pub workdir: Option<PathBuf>,
    pub json: Option<PathBuf>,
}

struct Iter {
    cold: Phase,
    warm: Phase,
    status1: f64,
    status2: f64,
    changes: usize,
    cache_files: u64,
    cache_bytes: u64,
}

struct Phase {
    secs: f64,
    cpu: f64,
    files: u64,
    bytes: u64,
    ws_calls: u64,
    ws_objects: u64,
    net_calls: u64,
    net_objects: u64,
    net_bytes: u64,
    net_secs: f64,
}

async fn materialize(
    a: &Args,
    remote: &RemoteObjectStore,
    work: &Path,
    commit: ObjectId,
    sparse: Sparse,
) -> Result<(Phase, Workspace, Arc<dyn ObjectStore>)> {
    let metrics = Metrics::default();
    let disk = Arc::new(DiskObjectStore::open(&a.cache, u64::MAX / 2).context("opening cache")?);
    let net = timed_store(Arc::new(remote.clone()), &metrics, "net");
    let rt = timed_store(Arc::new(ReadThrough::new(disk, net)), &metrics, "ws");
    let tree = rt.read_commit(&commit).await?.tree;
    let cpu0 = cpu_seconds().map_or(0.0, |c| c.0 + c.1);
    let start = Instant::now();
    let ws = Workspace::materialize(rt.as_ref(), work, commit, tree, sparse).await?;
    let secs = start.elapsed().as_secs_f64();
    let cpu = cpu_seconds().map_or(0.0, |c| c.0 + c.1) - cpu0;
    let w = work.to_path_buf();
    let (files, bytes) = tokio::task::spawn_blocking(move || dir_size_skip(&w, Some(".ant"))).await?;
    let (wsc, nc) = (metrics.counter("ws.get_many"), metrics.counter("net.get_many"));
    Ok((
        Phase {
            secs,
            cpu,
            files,
            bytes,
            ws_calls: wsc.calls(),
            ws_objects: wsc.items(),
            net_calls: nc.calls(),
            net_objects: nc.items(),
            net_bytes: nc.bytes(),
            net_secs: nc.secs(),
        },
        ws,
        rt,
    ))
}

fn phase_json(p: &Phase) -> J {
    J::obj()
        .with("secs", p.secs)
        .with("cpu_s", p.cpu)
        .with("files", p.files)
        .with("bytes", p.bytes)
        .with("files_per_s", p.files as f64 / p.secs)
        .with("bytes_per_s", p.bytes as f64 / p.secs)
        .with("ws_get_many_calls", p.ws_calls)
        .with("ws_objects", p.ws_objects)
        .with("net_get_many_calls", p.net_calls)
        .with("net_objects", p.net_objects)
        .with("net_bytes", p.net_bytes)
        .with("net_secs_sum", p.net_secs)
}

fn print_phase(name: &str, p: &Phase) {
    println!(
        "  {name:<5} {:>8.3}s  {:>8} files  {:>9.1} MiB  {:>9.0} files/s  {:>8.1} MiB/s  cpu {:.2}s | ws: {} get_many/{} obj | net: {} calls/{} obj/{:.1} MiB, call-time sum {:.2}s",
        p.secs,
        p.files,
        mib(p.bytes),
        p.files as f64 / p.secs,
        mib(p.bytes) / p.secs,
        p.cpu,
        p.ws_calls,
        p.ws_objects,
        p.net_calls,
        p.net_objects,
        mib(p.net_bytes),
        p.net_secs
    );
}

pub async fn run(a: Args) -> Result<()> {
    let work = a.workdir.clone().unwrap_or_else(|| {
        let mut s = a.cache.clone().into_os_string();
        s.push(".ws");
        PathBuf::from(s)
    });
    let remote = RemoteObjectStore::connect(&a.server, a.http.as_deref()).await?;
    let t = Instant::now();
    let commit = remote
        .client()
        .resolve_rev(pb::ResolveRevRequest { rev: a.rev.clone() })
        .await
        .map_err(|s| anyhow::anyhow!("resolve_rev: {}", s.message()))?
        .into_inner()
        .commit;
    let resolve_ms = t.elapsed().as_secs_f64() * 1000.0;
    let commit = ObjectId::from_bytes(&commit)?;
    let sparse = if a.sparse.is_empty() { Sparse::all() } else { Sparse::new(a.sparse.clone())? };
    println!(
        "checkout: server={} http={:?} rev={} ({commit}) sparse={:?} iterations={}",
        a.server, a.http, a.rev, a.sparse, a.iterations
    );

    let mut iters = Vec::new();
    for it in 0..a.iterations.max(1) {
        // Cold: no cache, no workspace.
        wipe(&a.cache)?;
        wipe(&work)?;
        let (cold, _ws, _) = materialize(&a, &remote, &work, commit, sparse.clone()).await?;
        let (cache_files, cache_bytes) = dir_size(&a.cache);

        // Warm: cache kept, fresh workspace directory.
        wipe(&work)?;
        let (warm, ws, rt) = materialize(&a, &remote, &work, commit, sparse.clone()).await?;

        // status on the unchanged workspace (twice: first may touch more state).
        let t = Instant::now();
        let changes = ws.status(rt.as_ref()).await?;
        let status1 = t.elapsed().as_secs_f64();
        let t = Instant::now();
        let changes2 = ws.status(rt.as_ref()).await?;
        let status2 = t.elapsed().as_secs_f64();
        println!("iteration {}", it + 1);
        print_phase("cold", &cold);
        print_phase("warm", &warm);
        println!(
            "  status {status1:.3}s then {status2:.3}s ({} / {} changes reported on an unchanged tree); cache after cold: {cache_files} files, {:.1} MiB",
            changes.len(),
            changes2.len(),
            mib(cache_bytes)
        );
        iters.push(Iter {
            cold,
            warm,
            status1,
            status2,
            changes: changes.len(),
            cache_files,
            cache_bytes,
        });
    }

    let n = iters.len() as f64;
    let mean = |f: &dyn Fn(&Iter) -> f64| iters.iter().map(f).sum::<f64>() / n;
    println!(
        "mean over {} iteration(s): cold {:.3}s, warm {:.3}s, status {:.3}s / {:.3}s; resolve_rev {resolve_ms:.1}ms",
        iters.len(),
        mean(&|i| i.cold.secs),
        mean(&|i| i.warm.secs),
        mean(&|i| i.status1),
        mean(&|i| i.status2)
    );

    let json = J::obj()
        .with("command", "checkout")
        .with("server", a.server.as_str())
        .with("http", a.http.clone())
        .with("rev", a.rev.as_str())
        .with("commit", commit.to_hex())
        .with("sparse", J::Arr(a.sparse.iter().map(|s| J::from(s.as_str())).collect()))
        .with("resolve_rev_ms", resolve_ms)
        .with(
            "iterations",
            J::Arr(
                iters
                    .iter()
                    .map(|i| {
                        J::obj()
                            .with("cold", phase_json(&i.cold))
                            .with("warm", phase_json(&i.warm))
                            .with("status_first_s", i.status1)
                            .with("status_second_s", i.status2)
                            .with("status_changes", i.changes)
                            .with("cache_files", i.cache_files)
                            .with("cache_bytes", i.cache_bytes)
                    })
                    .collect(),
            ),
        )
        .with("mean_cold_s", mean(&|i| i.cold.secs))
        .with("mean_warm_s", mean(&|i| i.warm.secs))
        .with("mean_status_s", mean(&|i| i.status1));
    write_json(&a.json, &json)
}

fn wipe(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing {}", path.display())),
    }
}
