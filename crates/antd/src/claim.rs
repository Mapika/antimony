//! Cross-process fetch claims in the shared disk cache.
//!
//! Many `ant` processes on one node share a cache directory. Without
//! coordination every process that misses the same object fetches it from
//! the server. A *claim* makes the first one the fetcher; the others wait
//! until it is done and then read the object from the cache.
//!
//! - A claim is an `flock` on `{root}/claims/{key}` (`key`: an object id in
//!   hex, or `b-` + a tree id for a bundle fetch). The kernel drops the lock
//!   when its holder dies, so a crash never leaves a claim behind; the file
//!   itself is unlinked (before unlocking) by the holder when it is done, and
//!   files a crash abandoned are swept when a cache is opened
//!   ([`sweep`], older than an hour).
//! - [`Claims::try_claim`] never waits. [`Claims::wait_released`] polls for
//!   the lock for at most a timeout and returns whether the holder finished;
//!   either way the caller re-reads the cache and fetches what is still
//!   missing itself, so a claim is only an optimization: a stuck holder
//!   costs a timeout, a race (two claimants on an unlinked file) costs a
//!   duplicate fetch, and neither can corrupt anything (objects are
//!   immutable and written by rename).
//! - Holders fetch and store *before* waiting on anyone else's claim, so
//!   claims cannot deadlock.
//! - The directory is additive: older binaries ignore it, so the cache
//!   format version stays 1.

use std::fs::{File, TryLockError};
use std::io::ErrorKind;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

const DIR: &str = "claims";
/// Poll interval while waiting for another process.
const POLL: Duration = Duration::from_millis(4);
/// Claim files older than this belong to a crashed process.
const STALE: Duration = Duration::from_secs(3600);

/// Claims inside one cache directory.
#[derive(Debug, Clone)]
pub struct Claims {
    dir: PathBuf,
}

/// Exclusive right to fetch one key; released (and its file removed) on drop.
#[derive(Debug)]
pub struct Claim {
    file: File,
    path: PathBuf,
}

impl Drop for Claim {
    fn drop(&mut self) {
        // Unlink first: a waiter then locks an orphan inode, and a new
        // claimant creates a fresh file.
        let _ = std::fs::remove_file(&self.path);
        let _ = self.file.unlock();
    }
}

impl Claims {
    pub fn new(root: &Path) -> Self {
        Self { dir: root.join(DIR) }
    }

    fn path(&self, key: &str) -> PathBuf {
        self.dir.join(key)
    }

    fn open(&self, key: &str) -> std::io::Result<(File, PathBuf)> {
        let path = self.path(key);
        let open = || File::options().create(true).append(true).open(&path);
        let file = match open() {
            Err(e) if e.kind() == ErrorKind::NotFound => {
                std::fs::create_dir_all(&self.dir)?;
                open()?
            }
            r => r?,
        };
        Ok((file, path))
    }

    /// Claim `key` unless another holder has it. Blocking (a few syscalls).
    pub fn try_claim(&self, key: &str) -> std::io::Result<Option<Claim>> {
        for _ in 0..4 {
            let (file, path) = self.open(key)?;
            match file.try_lock() {
                Ok(()) => {}
                Err(TryLockError::WouldBlock) => return Ok(None),
                Err(TryLockError::Error(e)) => return Err(e),
            }
            // The previous holder may have unlinked the file between our
            // open and our lock: then we hold an orphan, so try again.
            let same = std::fs::metadata(&path).is_ok_and(|m| m.ino() == file_ino(&file));
            if same {
                return Ok(Some(Claim { file, path }));
            }
        }
        Ok(None)
    }

    /// Block until nobody holds `key` any more (`true`) or `timeout` passes
    /// (`false`). Does not claim anything.
    pub fn wait_released(&self, key: &str, timeout: Duration) -> bool {
        let path = self.path(key);
        let deadline = Instant::now() + timeout;
        loop {
            let file = match File::options().read(true).open(&path) {
                Ok(f) => f,
                // No file: no holder.
                Err(e) if e.kind() == ErrorKind::NotFound => return true,
                Err(_) => return false,
            };
            match file.try_lock_shared() {
                Ok(()) => return true,
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Error(_)) => return false,
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(POLL);
        }
    }
}

fn file_ino(file: &File) -> u64 {
    file.metadata().map_or(0, |m| m.ino())
}

/// Remove claim files abandoned by crashed processes (best effort).
pub fn sweep(root: &Path) {
    let Ok(entries) = std::fs::read_dir(root.join(DIR)) else { return };
    let cutoff = SystemTime::now() - STALE;
    for e in entries.flatten() {
        if e.metadata().and_then(|m| m.modified()).is_ok_and(|m| m < cutoff) {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_holder_others_wait() {
        let dir = tempfile::tempdir().unwrap();
        let claims = Claims::new(dir.path());
        let a = claims.try_claim("k").unwrap().expect("first claim");
        assert!(claims.try_claim("k").unwrap().is_none());
        assert!(claims.try_claim("other").unwrap().is_some());
        assert!(!claims.wait_released("k", Duration::from_millis(30)));
        let c2 = claims.clone();
        let waiter = std::thread::spawn(move || c2.wait_released("k", Duration::from_secs(5)));
        std::thread::sleep(Duration::from_millis(30));
        drop(a);
        assert!(waiter.join().unwrap());
        assert!(!dir.path().join("claims/k").exists(), "file removed with the claim");
        assert!(claims.try_claim("k").unwrap().is_some());
    }

    #[test]
    fn sweep_removes_only_stale_files() {
        let dir = tempfile::tempdir().unwrap();
        let claims = Claims::new(dir.path());
        let _live = claims.try_claim("live").unwrap().unwrap();
        let old = claims.dir.join("old");
        let f = File::create(&old).unwrap();
        f.set_modified(SystemTime::now() - 2 * STALE).unwrap();
        sweep(dir.path());
        assert!(!old.exists());
        assert!(claims.dir.join("live").exists());
        // An abandoned file (no lock holder) can be claimed again.
        assert!(claims.try_claim("old").unwrap().is_some());
    }
}
