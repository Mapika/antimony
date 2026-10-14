//! Crash test for `ant_fuse::rw::update`: SIGKILL the updating process at
//! random points while the workspace is mounted. See
//! `crash_update_kill_is_all_or_nothing`.

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use ant_core::RepoPath;

use super::{Rng, Tree, snapshot};
use crate::scratch;
use crate::support::{Daemon, build_update, runtime};

/// Files merged on both sides (one 3-way merge and one staged file each).
const MERGED: usize = 300;
const CONFLICTED: usize = 5;

/// The `update` fixture's old (`new == false`) or new base.
pub fn base_files(new: bool) -> Vec<(String, Vec<u8>)> {
    let mut files = Vec::new();
    for i in 0..MERGED {
        let mut lines: Vec<String> = (1..=9).map(|l| format!("line {l} of {i}")).collect();
        if new {
            lines[0] = format!("upstream {i}");
        }
        files.push((format!("m/d{}/f{i}", i / 30), (lines.join("\n") + "\n").into_bytes()));
    }
    for j in 0..CONFLICTED {
        let mid = if new { format!("upstream {j}") } else { "c2".into() };
        files.push((format!("c/x{j}"), format!("c1\n{mid}\nc3\n").into_bytes()));
    }
    for k in 0..20 {
        let v = if new { " v2" } else { "" };
        files.push((format!("u/f{k}"), format!("u{k}{v}\n").into_bytes()));
    }
    for k in 0..10 {
        files.push((format!("keep/k{k}"), format!("k{k}\n").into_bytes()));
    }
    for k in 0..5 {
        if new {
            files.push((format!("added/n{k}"), format!("n{k}\n").into_bytes()));
        } else {
            files.push((format!("gone/g{k}"), format!("g{k}\n").into_bytes()));
        }
    }
    files
}

/// The local edits, made through the mount.
fn edit(mount: &Path) {
    for i in 0..MERGED {
        let mut lines: Vec<String> = (1..=9).map(|l| format!("line {l} of {i}")).collect();
        lines[8] = format!("local {i}");
        fs::write(mount.join(format!("m/d{}/f{i}", i / 30)), lines.join("\n") + "\n").unwrap();
    }
    for j in 0..CONFLICTED {
        fs::write(mount.join(format!("c/x{j}")), format!("c1\nlocal {j}\nc3\n")).unwrap();
    }
    fs::create_dir(mount.join("local")).unwrap();
    fs::write(mount.join("local/new.txt"), b"mine\n").unwrap();
    fs::remove_file(mount.join("u/f0")).unwrap(); // modify/delete conflict
    fs::remove_file(mount.join("keep/k0")).unwrap(); // local-only deletion
}

fn expected_conflicts() -> Vec<RepoPath> {
    let mut out: Vec<RepoPath> =
        (0..CONFLICTED).map(|j| RepoPath::new(format!("c/x{j}")).unwrap()).collect();
    out.push(RepoPath::new("u/f0").unwrap());
    out
}

fn spawn_update(state: &Path) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["update_child", "--exact", "--nocapture", "--test-threads=1"])
        .env("ANT_RW_UPDATE", "1")
        .env("ANT_RW_STATE", state)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .unwrap()
}

pub fn run(seed: u64, rounds: usize) {
    eprintln!("update crash test seed {seed} ({rounds} rounds)");
    let mut rng = Rng(seed);
    let rt = runtime();
    let (objects, old_tree, new_tree) = build_update(&rt);
    let mut new_view: Option<Tree> = None;
    // Reference timings from `start`: the commit record, and the end.
    let (mut commit, mut took) = (Duration::ZERO, Duration::ZERO);
    let (mut olds, mut news) = (0, 0);
    for round in 0..=rounds {
        let tmp = scratch("rwupdate");
        let mount = tmp.path().join("mnt");
        let state = tmp.path().join("state");
        fs::create_dir(&mount).unwrap();
        let daemon = Daemon::start("update", &mount, &state);
        edit(&mount);
        let old_view = snapshot(&mount);
        let mut child = spawn_update(&state);
        // Aim at the update's writes: from the staging directory's
        // appearance (prepare) to the end.
        let start = loop {
            if state.join("staging").exists() || child.try_wait().unwrap().is_some() {
                break Instant::now();
            }
            std::thread::sleep(Duration::from_micros(20));
        };
        if round == 0 {
            // Uninterrupted reference run; note when the record lands.
            let journal = |s: &Path| fs::metadata(s.join("ops.log")).map(|m| m.len()).unwrap_or(0);
            let before = journal(&state);
            while journal(&state) == before && child.try_wait().unwrap().is_none() {
                std::thread::sleep(Duration::from_micros(20));
            }
            commit = start.elapsed();
            assert!(child.wait().unwrap().success(), "update failed");
            took = start.elapsed();
            let view = snapshot(&mount);
            let f0 = String::from_utf8(view["m/d0/f0"].clone().unwrap().0).unwrap();
            assert!(f0.starts_with("upstream 0\n") && f0.ends_with("local 0\n"), "{f0}");
            let x0 = String::from_utf8(view["c/x0"].clone().unwrap().0).unwrap();
            assert!(x0.contains("<<<<<<< upstream\nupstream 0\n"), "{x0}");
            assert!(!view.contains_key("u/f0") && !view.contains_key("keep/k0"));
            assert!(view.contains_key("added/n0") && !view.contains_key("gone/g0"));
            assert_eq!(view["local/new.txt"], Some((b"mine\n".to_vec(), false)));
            eprintln!(
                "uninterrupted update from staging on: commit at {commit:?}, done at {took:?}"
            );
            new_view = Some(view);
        } else {
            // Half the kills anywhere, half around the commit point and
            // the roll-forward after it.
            let (from, to) = if round % 2 == 1 {
                (0, took.as_micros() as u64 * 6 / 5)
            } else {
                (commit.as_micros() as u64 * 9 / 10, took.as_micros() as u64 * 11 / 10)
            };
            let delay = Duration::from_micros(from + rng.next() % (to - from + 1));
            std::thread::sleep(delay.saturating_sub(start.elapsed()));
            let _ = child.kill();
            child.wait().unwrap();
        }
        let new_view = new_view.as_ref().unwrap();
        // The live mount reloads (and recovers) on its next operation.
        let got = snapshot(&mount);
        let at_new = if got == *new_view {
            true
        } else {
            assert!(
                got == old_view,
                "seed {seed} round {round}: neither the old nor the new state"
            );
            false
        };
        let (_, tree) = ant_fuse::rw::base(&state).unwrap();
        assert_eq!(tree, if at_new { new_tree } else { old_tree }, "round {round}: base");
        let conflicts = ant_fuse::rw::conflicts(&state).unwrap();
        assert_eq!(conflicts, if at_new { expected_conflicts() } else { vec![] }, "round {round}");
        // Status agrees with the view: every local edit is still reported.
        let changes = rt.block_on(ant_fuse::rw::changes(objects.clone(), &state)).unwrap();
        let changed: Vec<String> = changes.iter().map(|c| c.path.to_string()).collect();
        assert!(changed.contains(&"local/new.txt".to_string()) && changed.len() > MERGED);
        // A remount after the daemon is killed too shows the same state.
        daemon.kill();
        let daemon = Daemon::start("update", &mount, &state);
        assert!(snapshot(&mount) == got, "seed {seed} round {round}: remount differs");
        daemon.stop();
        if at_new { news += 1 } else { olds += 1 }
    }
    eprintln!("update crash test: {olds} rounds at the old base, {news} at the new one");
}
