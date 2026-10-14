//! Crash tests: kill the daemon at random points of a scripted workload.

pub mod update;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::Path;
use std::time::Duration;

use super::support::{self, Daemon, build, runtime};

/// The visible workspace: directories (`None`) and files (content, exec bit).
pub type Tree = BTreeMap<String, Option<(Vec<u8>, bool)>>;

pub fn snapshot(root: &Path) -> Tree {
    let mut out = Tree::new();
    let mut todo = vec![String::new()];
    while let Some(rel) = todo.pop() {
        for e in fs::read_dir(root.join(&rel)).unwrap() {
            let e = e.unwrap();
            let name = e.file_name().into_string().unwrap();
            let path = if rel.is_empty() { name } else { format!("{rel}/{name}") };
            let m = fs::symlink_metadata(root.join(&path)).unwrap();
            if m.is_dir() {
                out.insert(path.clone(), None);
                todo.push(path);
            } else {
                let exec = m.permissions().mode() & 0o100 != 0;
                out.insert(path.clone(), Some((fs::read(root.join(&path)).unwrap(), exec)));
            }
        }
    }
    out
}

fn base_tree(fixture: &str) -> Tree {
    let mut out = Tree::new();
    for (path, data) in support::fixture(fixture) {
        let parts: Vec<&str> = path.split('/').collect();
        for i in 1..parts.len() {
            out.insert(parts[..i].join("/"), None);
        }
        out.insert(path, Some((data, false)));
    }
    out
}

/// splitmix64
pub struct Rng(pub u64);
impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn pick<T: Clone>(&mut self, items: &[T]) -> Option<T> {
        if items.is_empty() { None } else { Some(items[self.below(items.len())].clone()) }
    }
}

#[derive(Debug, Clone)]
enum Op {
    /// create + one write + close
    WriteNew {
        path: String,
        data: Vec<u8>,
    },
    /// open for write (copy-up) + positional write + close
    Overwrite {
        path: String,
        offset: u64,
        data: Vec<u8>,
    },
    /// open for write + set_len
    Truncate {
        path: String,
        len: u64,
    },
    Unlink(String),
    Rename(String, String),
    Mkdir(String),
    Rmdir(String),
    Chmod(String, bool),
    Fsync(String),
}

/// Close and report errors: a write is complete once the file is closed
/// (FUSE flush) or synced, not when `write` returns.
fn close(f: fs::File) -> std::io::Result<()> {
    use std::os::fd::IntoRawFd;
    // SAFETY: we own the descriptor and close it exactly once.
    if unsafe { libc::close(f.into_raw_fd()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn under(path: &str, dir: &str) -> bool {
    path == dir || path.strip_prefix(dir).is_some_and(|r| r.starts_with('/'))
}

fn overlay_at(content: &mut Vec<u8>, offset: u64, data: &[u8]) {
    let end = offset as usize + data.len();
    if content.len() < end {
        content.resize(end, 0);
    }
    content[offset as usize..end].copy_from_slice(data);
}

impl Op {
    fn apply(&self, t: &mut Tree) {
        match self {
            Op::WriteNew { path, data } => {
                t.insert(path.clone(), Some((data.clone(), false)));
            }
            Op::Overwrite { path, offset, data } => {
                if let Some(Some((content, _))) = t.get_mut(path) {
                    overlay_at(content, *offset, data);
                }
            }
            Op::Truncate { path, len } => {
                if let Some(Some((content, _))) = t.get_mut(path) {
                    content.resize(*len as usize, 0);
                }
            }
            Op::Unlink(path) | Op::Rmdir(path) => {
                t.remove(path);
            }
            Op::Rename(from, to) => {
                let moved: Vec<_> = t.keys().filter(|k| under(k, from)).cloned().collect();
                t.remove(to);
                for k in moved {
                    let v = t.remove(&k).unwrap();
                    t.insert(format!("{to}{}", &k[from.len()..]), v);
                }
            }
            Op::Mkdir(path) => {
                t.insert(path.clone(), None);
            }
            Op::Chmod(path, exec) => {
                if let Some(Some((_, e))) = t.get_mut(path) {
                    *e = *exec;
                }
            }
            Op::Fsync(_) => {}
        }
    }

    fn run(&self, root: &Path) -> std::io::Result<()> {
        match self {
            Op::WriteNew { path, data } => {
                let mut f = fs::File::create(root.join(path))?;
                f.write_all(data)?;
                close(f)?;
            }
            Op::Overwrite { path, offset, data } => {
                let f = fs::OpenOptions::new().write(true).open(root.join(path))?;
                f.write_all_at(data, *offset)?;
                close(f)?;
            }
            Op::Truncate { path, len } => {
                fs::OpenOptions::new().write(true).open(root.join(path))?.set_len(*len)?;
            }
            Op::Unlink(path) => fs::remove_file(root.join(path))?,
            Op::Rename(from, to) => fs::rename(root.join(from), root.join(to))?,
            Op::Mkdir(path) => fs::create_dir(root.join(path))?,
            Op::Rmdir(path) => fs::remove_dir(root.join(path))?,
            Op::Chmod(path, exec) => fs::set_permissions(
                root.join(path),
                fs::Permissions::from_mode(if *exec { 0o755 } else { 0o644 }),
            )?,
            Op::Fsync(path) => {
                fs::OpenOptions::new().write(true).open(root.join(path))?.sync_all()?
            }
        }
        Ok(())
    }

    /// States an interrupted run of this op may leave besides "before" and
    /// "after": data operations are several syscalls, and a write cut short
    /// is the application's own partial write. Nothing else may differ.
    fn partial_ok(&self, before: &Tree, got: &Tree) -> bool {
        let path = match self {
            Op::WriteNew { path, .. } | Op::Overwrite { path, .. } => path,
            _ => return false,
        };
        let without = |t: &Tree| {
            let mut t = t.clone();
            t.remove(path);
            t
        };
        if without(before) != without(got) {
            return false;
        }
        // A data write never changes the mode: new files are not executable.
        let exec = matches!(before.get(path), Some(Some((_, true))));
        let content = match got.get(path) {
            Some(Some((c, x))) if *x == exec => Some(c),
            None => None,
            _ => return false,
        };
        match self {
            Op::WriteNew { data, .. } => content.is_none_or(|c| data.starts_with(c)),
            Op::Overwrite { offset, data, .. } => {
                let (Some(Some((old, _))), Some(c)) = (before.get(path), content) else {
                    return false;
                };
                (0..=data.len()).any(|j| {
                    let mut want = old.clone();
                    overlay_at(&mut want, *offset, &data[..j]);
                    want == *c
                })
            }
            _ => false,
        }
    }
}

fn generate(rng: &mut Rng, model: &Tree, n: usize, counter: &mut usize) -> Vec<Op> {
    let mut t = model.clone();
    let mut ops = Vec::new();
    while ops.len() < n {
        let files: Vec<String> =
            t.iter().filter(|(_, v)| v.is_some()).map(|(k, _)| k.clone()).collect();
        let dirs: Vec<String> =
            t.iter().filter(|(_, v)| v.is_none()).map(|(k, _)| k.clone()).collect();
        let parent = if rng.below(3) == 0 { None } else { rng.pick(&dirs) };
        *counter += 1;
        let fresh = match &parent {
            Some(p) => format!("{p}/n{counter}"),
            None => format!("n{counter}"),
        };
        let op = match rng.below(12) {
            0 | 1 => {
                let len = [0usize, 10, 5000, 200_000][rng.below(4)];
                let seed = rng.next() as usize % 1000;
                Op::WriteNew { path: fresh, data: support::big(seed, len) }
            }
            2 | 3 => {
                let Some(path) = rng.pick(&files) else { continue };
                let Some(Some((c, _))) = t.get(&path) else { continue };
                let offset = rng.below(c.len() + 1) as u64;
                let seed = rng.next() as usize % 1000;
                Op::Overwrite { path, offset, data: support::big(seed, 1 + rng.below(70_000)) }
            }
            4 => {
                let Some(path) = rng.pick(&files) else { continue };
                Op::Truncate { path, len: rng.below(10_000) as u64 }
            }
            5 | 6 => {
                let Some(path) = rng.pick(&files) else { continue };
                Op::Unlink(path)
            }
            7 | 8 => {
                // Files or whole directories (lower ones are copied up).
                let all: Vec<String> = t.keys().cloned().collect();
                let Some(from) = rng.pick(&all) else { continue };
                let to = if rng.below(3) == 0 && t.get(&from).is_some_and(|v| v.is_some()) {
                    // Replace an existing file (editor save).
                    let Some(to) = rng.pick(&files) else { continue };
                    to
                } else {
                    fresh
                };
                if under(&to, &from) || under(&from, &to) {
                    continue;
                }
                Op::Rename(from, to)
            }
            9 => Op::Mkdir(fresh),
            10 => {
                let empty: Vec<String> = dirs
                    .iter()
                    .filter(|d| !t.keys().any(|k| k != *d && under(k, d)))
                    .cloned()
                    .collect();
                let Some(path) = rng.pick(&empty) else { continue };
                Op::Rmdir(path)
            }
            _ => {
                let Some(path) = rng.pick(&files) else { continue };
                if rng.below(2) == 0 { Op::Chmod(path, rng.below(2) == 0) } else { Op::Fsync(path) }
            }
        };
        op.apply(&mut t);
        ops.push(op);
    }
    ops
}

fn expected_changes(base: &Tree, now: &Tree) -> Vec<(String, char)> {
    let mut out = Vec::new();
    let paths: BTreeSet<&String> = base.keys().chain(now.keys()).collect();
    for path in paths {
        match (base.get(path).cloned().flatten(), now.get(path).cloned().flatten()) {
            (None, Some(_)) => out.push((path.clone(), 'A')),
            (Some(_), None) => out.push((path.clone(), 'D')),
            (Some(x), Some(y)) if x != y => out.push((path.clone(), 'M')),
            _ => {}
        }
    }
    out
}

fn describe(got: &Tree, want: &Tree) -> String {
    let show = |v: Option<&Option<(Vec<u8>, bool)>>| match v {
        None => "absent".to_string(),
        Some(None) => "dir".to_string(),
        Some(Some((c, x))) => format!("{} bytes exec={x}", c.len()),
    };
    let keys: BTreeSet<&String> = got.keys().chain(want.keys()).collect();
    keys.into_iter()
        .filter(|k| got.get(*k) != want.get(*k))
        .map(|k| format!("{k}: got {} want {}", show(got.get(k)), show(want.get(k))))
        .collect::<Vec<_>>()
        .join("\n")
}

/// See `crash_daemon_kill_preserves_completed_operations`.
pub fn run(seed: u64, rounds: usize) {
    eprintln!("crash test seed {seed} ({rounds} rounds)");
    let mut rng = Rng(seed);
    let tmp = super::scratch("rwcrash");
    let mount = tmp.path().join("mnt");
    let state = tmp.path().join("state");
    fs::create_dir(&mount).unwrap();
    let rt = runtime();
    let (objects, _) = build(&rt, "crash");
    let base = base_tree("crash");
    let mut model = base.clone();
    let mut counter = 0;
    let mut daemon = Daemon::start("crash", &mount, &state);
    assert_eq!(snapshot(&mount), model);
    let (mut interrupted, mut completed) = (0, 0);
    for round in 0..rounds {
        let ops = generate(&mut rng, &model, 150, &mut counter);
        let delay = Duration::from_micros(rng.next() % 25_000);
        let pid = daemon.child.id() as libc::pid_t;
        let killer = std::thread::spawn(move || {
            std::thread::sleep(delay);
            // SAFETY: plain kill(2) of our own child process.
            unsafe { libc::kill(pid, libc::SIGKILL) };
        });
        let mut done = 0;
        for op in &ops {
            if op.run(&mount).is_err() {
                break;
            }
            done += 1;
        }
        killer.join().unwrap();
        daemon.kill();
        daemon = Daemon::start("crash", &mount, &state);
        let got = snapshot(&mount);
        let mut states = vec![model.clone()];
        for op in &ops {
            let mut next = states.last().unwrap().clone();
            op.apply(&mut next);
            states.push(next);
        }
        let ok = got == states[done]
            || (done < ops.len()
                && (got == states[done + 1] || ops[done].partial_ok(&states[done], &got)));
        assert!(
            ok,
            "seed {seed} round {round}: state after {done} completed ops is not a prefix\n\
             in-flight: {}\n{}",
            ops.get(done)
                .map(|o| format!("{o:?}").chars().take(200).collect::<String>())
                .unwrap_or_default(),
            describe(&got, &states[done])
        );
        if done < ops.len() {
            interrupted += 1;
        }
        completed += done;
        let changes = rt.block_on(ant_fuse::rw::changes(objects.clone(), &state)).unwrap();
        let summary: Vec<(String, char)> = changes
            .iter()
            .map(|c| {
                let kind = match c.kind {
                    antd::workspace::LocalChangeKind::Added => 'A',
                    antd::workspace::LocalChangeKind::Modified => 'M',
                    antd::workspace::LocalChangeKind::Deleted => 'D',
                };
                (c.path.to_string(), kind)
            })
            .collect();
        assert_eq!(summary, expected_changes(&base, &got), "seed {seed} round {round}: status");
        model = got;
    }
    // A clean unmount compacts; the next mount shows the same state.
    daemon.stop();
    assert_eq!(fs::metadata(state.join("ops.log")).map(|m| m.len()).unwrap_or(0), 0);
    let daemon = Daemon::start("crash", &mount, &state);
    assert_eq!(snapshot(&mount), model, "seed {seed}: after clean remount");
    daemon.stop();
    eprintln!("crash test: {rounds} kills, {interrupted} mid-workload, {completed} ops completed");
}
