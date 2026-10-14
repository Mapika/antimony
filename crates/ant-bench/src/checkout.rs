//! `checkout` subcommand: time `Workspace::materialize` cold (empty disk cache)
//! and warm (cache populated), plus `status` on the unchanged workspace.
//!
//! Object traffic is counted at two layers: what the workspace asks of the
//! read-through store (`ws.*`) and what actually reaches the server (`net.*`;
//! tree bundles bypass it), plus HTTP requests by kind (`/o/` objects, `/t/`
//! bundles). With `--cdn` an in-process shared cache (`crate::cdn`) sits in
//! front of the HTTP endpoint and each iteration adds a `proxy` phase: an
//! empty client cache against a warm proxy (requests reaching the origin are
//! counted).

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
use crate::util::{Metrics, TimedObjects, cpu_total, dir_size, mib, write_json};

#[derive(clap::Args, Debug)]
pub struct Args {
    /// gRPC URL of the server; omit with `--synthetic-files`.
    #[arg(long)]
    pub server: Option<String>,
    #[arg(long)]
    pub http: Option<String>,
    /// Serve a synthetic kernel-shaped tree with this many files from an
    /// in-process in-memory server.
    #[arg(long)]
    pub synthetic_files: Option<usize>,
    /// Directories in the synthetic tree (default 70% of files).
    #[arg(long)]
    pub synthetic_dirs: Option<usize>,
    /// With --synthetic-files, fetch over the in-process HTTP endpoint, not gRPC.
    #[arg(long)]
    pub use_http: bool,
    /// Fetch per object over HTTP, without tree bundles (`GET /t/`).
    #[arg(long)]
    pub no_tree_bundles: bool,
    /// Put an in-process caching proxy in front of the HTTP endpoint and
    /// measure an extra `proxy` phase (empty client cache, warm proxy).
    #[arg(long)]
    pub cdn: bool,
    #[arg(long, default_value = "main")]
    pub rev: String,
    /// Sparse pattern (repeatable); none = full checkout.
    #[arg(long)]
    pub sparse: Vec<String>,
    #[arg(long, default_value_t = 1)]
    pub iterations: usize,
    /// Disk cache directory (wiped for cold runs).
    #[arg(long)]
    pub cache: PathBuf,
    /// Cache size budget in bytes (default: effectively unlimited, so no
    /// accounting; pass e.g. 21474836480 to measure the budget machinery).
    #[arg(long)]
    pub cache_bytes: Option<u64>,
    /// Workspace directory (default: `<cache>.ws`).
    #[arg(long)]
    pub workdir: Option<PathBuf>,
    /// Sleep this long between the cold and the warm checkout, so the cached
    /// objects are older than the disk cache's recency-touch interval (60 s)
    /// and the warm read pays the mtime bumps a long-lived cache would.
    #[arg(long, default_value_t = 0)]
    pub warm_delay_secs: u64,
    #[arg(long)]
    pub json: Option<PathBuf>,
}

struct Iter {
    cold: Phase,
    proxy: Option<Phase>,
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
    http_objects: u64,
    http_bundles: u64,
    /// Requests the caching proxy forwarded to the origin (`--cdn`).
    origin_requests: u64,
}

async fn materialize(
    a: &Args,
    remote: &RemoteObjectStore,
    cdn: Option<&crate::cdn::Cdn>,
    work: &Path,
    commit: ObjectId,
    sparse: Sparse,
) -> Result<(Phase, Workspace, Arc<dyn ObjectStore>)> {
    let metrics = Metrics::default();
    let disk = Arc::new(
        DiskObjectStore::open(&a.cache, a.cache_bytes.unwrap_or(u64::MAX / 2))
            .context("opening cache")?,
    );
    let net = Arc::new(TimedObjects::new(Arc::new(remote.clone()), &metrics, "net"));
    let mut read_through = ReadThrough::over_disk(disk, net).verified_upstream();
    if !a.no_tree_bundles {
        read_through = read_through.with_tree_bundles(remote.clone());
    }
    let read_through = Arc::new(read_through);
    let rt: Arc<dyn ObjectStore> =
        Arc::new(TimedObjects::new(read_through.clone(), &metrics, "ws"));
    let tree = rt.read_commit(&commit).await?.tree;
    let http0 = remote.http_requests();
    let origin0 = cdn.map_or(0, |c| c.origin_requests());
    let cpu0 = cpu_total();
    let start = Instant::now();
    let ws = Workspace::materialize_with(
        rt.as_ref(),
        Some(read_through.as_ref()),
        work,
        commit,
        tree,
        sparse,
    )
    .await?;
    let secs = start.elapsed().as_secs_f64();
    let http = remote.http_requests();
    let origin_requests = cdn.map_or(0, |c| c.origin_requests()) - origin0;
    let cpu = cpu_total() - cpu0;
    let w = work.to_path_buf();
    let (files, bytes) = tokio::task::spawn_blocking(move || dir_size(&w, Some(".ant"))).await?;
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
            http_objects: http.objects - http0.objects,
            http_bundles: http.bundles - http0.bundles,
            origin_requests,
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
        .with("http_object_requests", p.http_objects)
        .with("http_bundle_requests", p.http_bundles)
        .with("origin_requests", p.origin_requests)
}

fn iter_json(i: &Iter) -> J {
    J::obj()
        .with("cold", phase_json(&i.cold))
        .with("proxy", i.proxy.as_ref().map_or(J::Null, phase_json))
        .with("warm", phase_json(&i.warm))
        .with("status_first_s", i.status1)
        .with("status_second_s", i.status2)
        .with("status_changes", i.changes)
        .with("cache_files", i.cache_files)
        .with("cache_bytes", i.cache_bytes)
}

fn print_phase(name: &str, p: &Phase) {
    println!(
        "  {name:<5} {:>8.3}s  {:>8} files  {:>9.1} MiB  {:>9.0} files/s  {:>8.1} MiB/s  cpu {:.2}s | ws: {} get_many/{} obj | net: {} calls/{} obj/{:.1} MiB, call-time sum {:.2}s | http: {} /o/ + {} /t/ requests, {} reached origin",
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
        p.net_secs,
        p.http_objects,
        p.http_bundles,
        p.origin_requests
    );
}

pub async fn run(a: Args) -> Result<()> {
    let work = a.workdir.clone().unwrap_or_else(|| {
        let mut s = a.cache.clone().into_os_string();
        s.push(".ws");
        PathBuf::from(s)
    });
    let (server_url, http_url) = match (&a.server, a.synthetic_files) {
        (Some(s), None) => (s.clone(), a.http.clone()),
        (None, Some(files)) => {
            let backend = ant_store::Backend::in_memory();
            let dirs = a.synthetic_dirs.unwrap_or(files / 10 * 7);
            let t = Instant::now();
            let bytes = crate::synth::build(&backend, files, dirs, &a.rev).await?;
            println!(
                "synthetic: {files} files, {dirs} dirs, {:.1} MiB, built in {:.1}s",
                mib(bytes),
                t.elapsed().as_secs_f64()
            );
            let srv = ant_server::serve::spawn_local(backend, ant_server::acl::Acl::open()).await?;
            (srv.grpc_url(), a.use_http.then(|| srv.http_url()))
        }
        _ => anyhow::bail!("give exactly one of --server or --synthetic-files"),
    };
    let cdn = match (&http_url, a.cdn) {
        (Some(origin), true) => Some(crate::cdn::spawn(origin).await?),
        (None, true) => anyhow::bail!("--cdn needs an HTTP endpoint (--http or --use-http)"),
        _ => None,
    };
    let client_http = cdn.as_ref().map(|c| c.url.clone()).or(http_url.clone());
    let remote = RemoteObjectStore::connect(&server_url, client_http.as_deref()).await?;
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
        server_url, http_url, a.rev, a.sparse, a.iterations
    );

    let mut iters = Vec::new();
    for it in 0..a.iterations.max(1) {
        // Cold: no cache, no workspace.
        wipe(&a.cache)?;
        wipe(&work)?;
        settle("before cold").await;
        let (cold, _ws, _) =
            materialize(&a, &remote, cdn.as_ref(), &work, commit, sparse.clone()).await?;
        let (cache_files, cache_bytes) = dir_size(&a.cache, None);

        // Proxy: empty client cache, warm shared cache in front of the origin.
        let proxy = match &cdn {
            Some(cdn) => {
                wipe(&a.cache)?;
                wipe(&work)?;
                settle("before proxy").await;
                Some(materialize(&a, &remote, Some(cdn), &work, commit, sparse.clone()).await?.0)
            }
            None => None,
        };

        // Warm: cache kept, fresh workspace directory.
        wipe(&work)?;
        if a.warm_delay_secs > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(a.warm_delay_secs)).await;
        }
        settle("before warm").await;
        let (warm, ws, rt) =
            materialize(&a, &remote, cdn.as_ref(), &work, commit, sparse.clone()).await?;

        // status on the unchanged workspace (twice: first may touch more state).
        let t = Instant::now();
        let changes = ws.status(rt.as_ref()).await?;
        let status1 = t.elapsed().as_secs_f64();
        let t = Instant::now();
        let changes2 = ws.status(rt.as_ref()).await?;
        let status2 = t.elapsed().as_secs_f64();
        println!("iteration {}", it + 1);
        print_phase("cold", &cold);
        if let Some(p) = &proxy {
            print_phase("proxy", p);
        }
        print_phase("warm", &warm);
        println!(
            "  status {status1:.3}s then {status2:.3}s ({} / {} changes reported on an unchanged tree); cache after cold: {cache_files} files, {:.1} MiB",
            changes.len(),
            changes2.len(),
            mib(cache_bytes)
        );
        iters.push(Iter {
            cold,
            proxy,
            warm,
            status1,
            status2,
            changes: changes.len(),
            cache_files,
            cache_bytes,
        });
    }

    if let Some(c) = &cdn {
        println!("cdn: {} hits, {} origin requests in total", c.hits(), c.origin_requests());
    }
    let n = iters.len() as f64;
    let mean = |f: &dyn Fn(&Iter) -> f64| iters.iter().map(f).sum::<f64>() / n;
    let series = |f: &dyn Fn(&Iter) -> f64| iters.iter().map(f).collect::<Vec<f64>>();
    let (cold_s, warm_s) = (series(&|i| i.cold.secs), series(&|i| i.warm.secs));
    let (status_s, status2_s) = (series(&|i| i.status1), series(&|i| i.status2));
    println!(
        "over {} iteration(s), median (min / mean): cold {:.3}s ({:.3} / {:.3}), warm {:.3}s ({:.3} / {:.3}), status {:.3}s / {:.3}s; resolve_rev {resolve_ms:.1}ms",
        iters.len(),
        median(&cold_s),
        min(&cold_s),
        mean(&|i| i.cold.secs),
        median(&warm_s),
        min(&warm_s),
        mean(&|i| i.warm.secs),
        median(&status_s),
        median(&status2_s)
    );

    let json = J::obj()
        .with("command", "checkout")
        .with("server", server_url.as_str())
        .with("http", http_url.clone())
        .with("rev", a.rev.as_str())
        .with("commit", commit.to_hex())
        .with("sparse", J::Arr(a.sparse.iter().map(|s| J::from(s.as_str())).collect()))
        .with("resolve_rev_ms", resolve_ms)
        .with("iterations", J::Arr(iters.iter().map(iter_json).collect()))
        // Headline numbers are medians: one outlier iteration (page-cache or
        // writeback noise) must not decide a target. Per-iteration values are
        // in `cold_s`/`warm_s`/`status_s` and in `iterations`.
        .with("median_cold_s", median(&cold_s))
        .with("median_warm_s", median(&warm_s))
        .with("median_status_s", median(&status_s))
        .with("min_cold_s", min(&cold_s))
        .with("min_warm_s", min(&warm_s))
        .with("min_status_s", min(&status_s))
        .with("cold_s", floats(&cold_s))
        .with("warm_s", floats(&warm_s))
        .with("status_s", floats(&status_s))
        .with("status_second_s", floats(&status2_s))
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

fn median(v: &[f64]) -> f64 {
    let mut s = v.to_vec();
    s.sort_by(f64::total_cmp);
    match s.len() {
        0 => f64::NAN,
        n if n % 2 == 1 => s[n / 2],
        n => (s[n / 2 - 1] + s[n / 2]) / 2.0,
    }
}

fn min(v: &[f64]) -> f64 {
    v.iter().copied().fold(f64::INFINITY, f64::min)
}

fn floats(v: &[f64]) -> J {
    J::Arr(v.iter().map(|x| J::from(*x)).collect())
}

/// Flush dirty pages (the previous phase's writes and the wipe's deletions)
/// before the next timed phase, so deferred writeback of work done outside the
/// timing is not charged to it. A cold phase right after a heavy import was
/// measured at 5x its settled time otherwise. The wait is printed, not timed.
async fn settle(what: &str) {
    let t = Instant::now();
    let _ = tokio::task::spawn_blocking(|| unsafe { libc::sync() }).await;
    let s = t.elapsed().as_secs_f64();
    if s > 0.5 {
        println!("  (sync {what}: {s:.1}s)");
    }
}
