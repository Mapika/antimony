//! `git-fleet` subcommand: the plain-git counterpart of `fleet`, for the
//! head-to-head benchmarks against git.
//!
//! N agents, each with its own clone of one remote repository (a `git://`
//! daemon or `file://` bare repo), loop: write its own file, `git add`, `git
//! commit`, `git push origin HEAD:<branch>`; when the push is rejected (the
//! branch moved) or fails (ref lock), `git pull --rebase` (fetch + rebase) and
//! push again, with no back-off. A change counts when its push succeeds;
//! its latency runs from the start of `git add` to the successful push and
//! includes every retry. After the deadline in-flight changes are abandoned.
//! The run is verified: the branch must have grown by exactly the number of
//! successful pushes (linear history) and hold every agent's last content.
//!
//! Output matches `fleet --json` where the notions coincide (`landed`,
//! `lands_per_s`, `latency_change`), plus `push_attempts` and `rejected`.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};

use crate::json::J;
use crate::util::{Lat, write_json};

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Remote to clone and push to (`git://host/repo.git`, `file:///...`).
    #[arg(long)]
    pub repo_url: String,
    #[arg(long, default_value_t = 16)]
    pub agents: usize,
    /// Run duration in seconds.
    #[arg(long, default_value_t = 20)]
    pub duration: u64,
    /// Branch all agents push to.
    #[arg(long, default_value = "main")]
    pub branch: String,
    /// Directory for the agents' clones (created, removed afterwards).
    #[arg(long)]
    pub workdir: PathBuf,
    /// Clone with `--depth 1` (large repositories: no history download).
    #[arg(long)]
    pub shallow: bool,
    /// Keep the clones.
    #[arg(long)]
    pub keep: bool,
    #[arg(long)]
    pub json: Option<PathBuf>,
}

fn git(dir: &Path, args: &[&str]) -> std::io::Result<std::process::Output> {
    Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_EDITOR", "true")
        .stdin(Stdio::null())
        .output()
}

fn git_ok(dir: &Path, args: &[&str]) -> bool {
    git(dir, args).map(|o| o.status.success()).unwrap_or(false)
}

#[derive(Default)]
struct Out {
    lat: Vec<u64>,
    landed: u64,
    attempts: u64,
    rejected: u64,
    pull_failures: u64,
    errors: u64,
    first_error: Option<String>,
    last: Option<(String, String)>,
}

fn agent(i: usize, dir: PathBuf, branch: String, deadline: Instant) -> Out {
    let mut out = Out::default();
    let rel = format!("agents/{i}/f0.txt");
    let mut seq = 0u64;
    'changes: while Instant::now() < deadline {
        seq += 1;
        let t0 = Instant::now();
        let content = format!("agent={i} seq={seq} file=0\n");
        let path = dir.join(&rel);
        let wrote = std::fs::create_dir_all(path.parent().unwrap_or(&dir))
            .and_then(|()| std::fs::write(&path, &content));
        if let Err(e) = wrote {
            out.errors += 1;
            out.first_error.get_or_insert(format!("write: {e}"));
            break;
        }
        let msg = format!("agent {i} change {seq}");
        if !git_ok(&dir, &["add", "--", &rel]) || !git_ok(&dir, &["commit", "-q", "-m", &msg]) {
            out.errors += 1;
            out.first_error.get_or_insert("add/commit failed".into());
            break;
        }
        let refspec = format!("HEAD:refs/heads/{branch}");
        loop {
            if Instant::now() >= deadline {
                break 'changes;
            }
            out.attempts += 1;
            match git(&dir, &["push", "-q", "origin", &refspec]) {
                Ok(o) if o.status.success() => {
                    out.lat.push(t0.elapsed().as_micros() as u64);
                    out.landed += 1;
                    out.last = Some((rel.clone(), content));
                    break;
                }
                Ok(_) => out.rejected += 1,
                Err(e) => {
                    out.errors += 1;
                    out.first_error.get_or_insert(format!("spawn: {e}"));
                    break 'changes;
                }
            }
            // Rejected: fetch + rebase onto the new tip, then retry.
            if !git_ok(&dir, &["pull", "-q", "--rebase", "origin", &branch]) {
                // Transient (daemon dropped the connection, ref lock held):
                // clean up and go round again, like a script with retries.
                out.pull_failures += 1;
                let _ = git(&dir, &["rebase", "--abort"]);
            }
        }
    }
    out
}

pub async fn run(args: Args) -> Result<i32> {
    if args.agents == 0 {
        bail!("--agents must be positive");
    }
    let _ = std::fs::remove_dir_all(&args.workdir);
    std::fs::create_dir_all(&args.workdir)?;
    let clone_start = Instant::now();
    let mut dirs = Vec::new();
    // Clone in batches of 8 threads (a daemon resets a burst of 64 connections);
    // clone time is not measured.
    let indices: Vec<usize> = (0..args.agents).collect();
    for chunk in indices.chunks(8) {
        let mut handles = Vec::new();
        for &i in chunk {
            let dir = args.workdir.join(format!("a{i}"));
            let url = args.repo_url.clone();
            let shallow = args.shallow;
            let branch = args.branch.clone();
            handles.push(std::thread::spawn(move || -> Result<PathBuf> {
                let mut cloned = false;
                for _ in 0..3 {
                    let _ = std::fs::remove_dir_all(&dir);
                    let mut c = Command::new("git");
                    c.args(["clone", "-q", "--single-branch", "--no-tags", "-b", &branch]);
                    if shallow {
                        c.args(["--depth", "1"]);
                    }
                    let st = c
                        .arg(&url)
                        .arg(&dir)
                        .env("GIT_TERMINAL_PROMPT", "0")
                        .stdin(Stdio::null())
                        .status()?;
                    if st.success() {
                        cloned = true;
                        break;
                    }
                }
                if !cloned {
                    bail!("clone {url} failed");
                }
                for (k, v) in [
                    ("user.name", format!("agent{i}")),
                    ("user.email", format!("agent{i}@bench")),
                    ("gc.auto", "0".into()),
                    ("maintenance.auto", "false".into()),
                    ("pull.rebase", "true".into()),
                ] {
                    if !git_ok(&dir, &["config", k, &v]) {
                        bail!("git config {k} failed");
                    }
                }
                Ok(dir)
            }));
        }
        for h in handles {
            dirs.push(h.join().map_err(|_| anyhow::anyhow!("clone thread panicked"))??);
        }
    }
    println!(
        "git-fleet: {} clones of {} ready in {:.1}s; agents={} duration={}s branch={}",
        dirs.len(),
        args.repo_url,
        clone_start.elapsed().as_secs_f64(),
        args.agents,
        args.duration,
        args.branch
    );
    let probe = args.workdir.join("probe");
    let start_len = {
        // Commits on the branch at the start (full history unless shallow).
        let st = Command::new("git")
            .args([
                "clone",
                "-q",
                "--single-branch",
                "--bare",
                "--filter=blob:none",
                "-b",
                &args.branch,
            ])
            .arg(&args.repo_url)
            .arg(&probe)
            .status()?;
        if !st.success() {
            bail!("probe clone failed");
        }
        rev_count(&probe, &args.branch)?
    };

    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.duration);
    let threads: Vec<_> = dirs
        .iter()
        .enumerate()
        .map(|(i, d)| {
            let (d, b) = (d.clone(), args.branch.clone());
            std::thread::spawn(move || agent(i, d, b, deadline))
        })
        .collect();
    let mut tot = Out::default();
    let mut lasts = Vec::new();
    for t in threads {
        let o = t.join().map_err(|_| anyhow::anyhow!("agent thread panicked"))?;
        tot.lat.extend(o.lat);
        tot.landed += o.landed;
        tot.attempts += o.attempts;
        tot.rejected += o.rejected;
        tot.pull_failures += o.pull_failures;
        tot.errors += o.errors;
        tot.first_error = tot.first_error.take().or(o.first_error);
        lasts.extend(o.last);
    }
    let wall = start.elapsed().as_secs_f64();

    // Verify against a fresh clone of the remote.
    let verify = args.workdir.join("verify");
    let st = Command::new("git")
        .args([
            "clone",
            "-q",
            "--single-branch",
            "--bare",
            "--filter=blob:none",
            "-b",
            &args.branch,
        ])
        .arg(&args.repo_url)
        .arg(&verify)
        .status()?;
    if !st.success() {
        bail!("verify clone failed");
    }
    let mut failures = Vec::new();
    let end_len = rev_count(&verify, &args.branch)?;
    let growth = end_len.saturating_sub(start_len);
    if growth != tot.landed {
        failures
            .push(format!("branch grew by {growth}, expected {} successful pushes", tot.landed));
    }
    for (path, content) in &lasts {
        let spec = format!("{}:{path}", args.branch);
        let o = git(&verify, &["show", &spec])?;
        if !o.status.success() || o.stdout != content.as_bytes() {
            failures.push(format!("{path}: content differs from last pushed"));
        }
    }

    let lat = Lat::from_micros(tot.lat);
    let per_s = tot.landed as f64 / wall;
    let pass = failures.is_empty() && tot.errors == 0;
    println!();
    println!("== git-fleet summary ==");
    println!("wall            {wall:.2}s");
    println!("landed          {} ({per_s:.1}/s)", tot.landed);
    println!(
        "push attempts   {} ({} rejected or failed, {:.2} attempts/change)",
        tot.attempts,
        tot.rejected,
        if tot.landed == 0 { 0.0 } else { tot.attempts as f64 / tot.landed as f64 }
    );
    println!("pull failures   {} (retried)", tot.pull_failures);
    println!("errors          {}", tot.errors);
    if let Some(e) = &tot.first_error {
        println!("first error     {e}");
    }
    println!("{}", lat.line("change"));
    println!("invariants: {}", if pass { "PASS" } else { "FAIL" });
    for f in &failures {
        println!("  - {f}");
    }
    let json = J::obj()
        .with("command", "git-fleet")
        .with("agents", args.agents)
        .with("duration_s", args.duration)
        .with("shallow", args.shallow)
        .with("wall_s", wall)
        .with("landed", tot.landed)
        .with("lands_per_s", per_s)
        .with("push_attempts", tot.attempts)
        .with("rejected", tot.rejected)
        .with("pull_failures", tot.pull_failures)
        .with("errors", tot.errors)
        .with("first_error", tot.first_error)
        .with("latency_change", lat.json())
        .with("invariants_pass", pass)
        .with("invariant_failures", J::Arr(failures.iter().map(|f| J::from(f.as_str())).collect()));
    write_json(&args.json, &json)?;
    if !args.keep {
        let _ = std::fs::remove_dir_all(&args.workdir);
    }
    Ok(if pass { 0 } else { 1 })
}

fn rev_count(repo: &Path, branch: &str) -> Result<u64> {
    let o = git(repo, &["rev-list", "--count", branch]).context("running git rev-list")?;
    if !o.status.success() {
        bail!("git rev-list failed: {}", String::from_utf8_lossy(&o.stderr));
    }
    String::from_utf8_lossy(&o.stdout).trim().parse().context("parsing rev-list count")
}
