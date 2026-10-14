//! Per-tier object counters of the client cache.
//!
//! [`ReadThrough`](crate::readthrough::ReadThrough) counts, per process, how
//! each object it was asked for was served: from the local disk cache, from
//! another process's fetch (cross-process claim), joined in-process, as part
//! of a tree bundle, or fetched individually from upstream. On drop the
//! counts are added into `{root}/gc/tiers` (`name value` lines, updated
//! atomically under the flock `{root}/gc/tiers.lock`), which `ant cache stats
//! --tiers` prints: lifetime totals of the cache directory. Like the rest of
//! `gc/`, the file is invisible to older binaries. Best effort: failures to
//! persist are ignored.

use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Counter names, in print order.
pub const NAMES: [&str; 7] = [
    "local_hits",
    "waited_hits",
    "joined",
    "bundle_requests",
    "bundled_objects",
    "fetched_requests",
    "fetched_objects",
];

/// Live counters of one process.
#[derive(Debug, Default)]
pub struct TierCounters {
    /// Objects found in the disk cache.
    pub local_hits: AtomicU64,
    /// Objects another process fetched while this one waited on its claim.
    pub waited_hits: AtomicU64,
    /// Objects joined to an in-process fetch already in flight.
    pub joined: AtomicU64,
    /// Tree bundles requested from upstream.
    pub bundle_requests: AtomicU64,
    /// Objects received inside bundles.
    pub bundled_objects: AtomicU64,
    /// Upstream `get_many` calls for individual objects.
    pub fetched_requests: AtomicU64,
    /// Objects fetched individually from upstream.
    pub fetched_objects: AtomicU64,
}

impl TierCounters {
    pub fn add(&self, which: &AtomicU64, n: usize) {
        which.fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Current values in [`NAMES`] order.
    pub fn snapshot(&self) -> [u64; 7] {
        [
            &self.local_hits,
            &self.waited_hits,
            &self.joined,
            &self.bundle_requests,
            &self.bundled_objects,
            &self.fetched_requests,
            &self.fetched_objects,
        ]
        .map(|a| a.load(Ordering::Relaxed))
    }

    /// Add this process's counts to the cache directory's totals.
    pub fn persist(&self, root: &Path) {
        let mine = self.snapshot();
        if mine.iter().all(|&v| v == 0) {
            return;
        }
        let _ = persist(root, mine);
    }
}

fn persist(root: &Path, mine: [u64; 7]) -> std::io::Result<()> {
    let dir = root.join("gc");
    std::fs::create_dir_all(&dir)?;
    let lock = File::options().create(true).append(true).open(dir.join("tiers.lock"))?;
    lock.lock()?;
    let mut total = read(root);
    for (t, m) in total.iter_mut().zip(mine) {
        *t = t.saturating_add(m);
    }
    let tmp = dir.join(format!("tiers.tmp.{}", std::process::id()));
    let text: String = NAMES.iter().zip(total).map(|(n, v)| format!("{n} {v}\n")).collect();
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, dir.join("tiers"))
}

/// Lifetime totals of the cache at `root` (zeros when none were recorded).
pub fn read(root: &Path) -> [u64; 7] {
    let mut out = [0u64; 7];
    let Ok(text) = std::fs::read_to_string(root.join("gc/tiers")) else { return out };
    for line in text.lines() {
        if let Some((name, v)) = line.split_once(' ')
            && let (Some(i), Ok(v)) = (NAMES.iter().position(|n| *n == name), v.parse())
        {
            out[i] = v;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn totals_accumulate_across_processes() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read(dir.path()), [0; 7]);
        let a = TierCounters::default();
        a.add(&a.local_hits, 5);
        a.add(&a.fetched_objects, 2);
        a.persist(dir.path());
        let b = TierCounters::default();
        b.add(&b.local_hits, 1);
        b.add(&b.bundled_objects, 9);
        b.persist(dir.path());
        assert_eq!(read(dir.path()), [6, 0, 0, 0, 9, 0, 2]);
    }
}
