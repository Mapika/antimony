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
//! - Claims are only taken while another process may be fetching into the
//!   same cache: each handle ([`Claims::new`]) registers itself for its
//!   lifetime as a file under `{root}/live/` that it holds an exclusive
//!   `flock` on, and [`Claims::peers_active`] (checked once per fetch batch)
//!   reports whether another registered handle is alive, or any claim file
//!   exists (an older binary, which claims without registering, is
//!   fetching). A process alone on its cache therefore pays no per-object
//!   file work (a claim is a create, a lock, a stat and an unlink, which on
//!   a journaling filesystem cost a 96k-file cold checkout about 2 s). A peer
//!   that appears while a batch is already being fetched unclaimed may fetch
//!   some of the same objects once more (a duplicate fetch, as without
//!   dedup); every later batch of both sees the other. A handle that cannot
//!   register (read-only cache) always claims, as before. Registrations
//!   whose holder died are unlocked, so they never count, and [`sweep`]
//!   removes them.
//! - The directories are additive: older binaries ignore them, so the cache
//!   format version stays 1.

use std::fs::{File, TryLockError};
use std::io::ErrorKind;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

const DIR: &str = "claims";
/// Registrations of live handles (see the module docs).
const LIVE_DIR: &str = "live";
/// An unlocked registration older than this belongs to a dead process (the
/// margin covers the moment between creating the file and locking it).
const LIVE_STALE: Duration = Duration::from_secs(60);
/// Poll interval while waiting for another process.
const POLL: Duration = Duration::from_millis(4);
/// Claim files older than this belong to a crashed process.
const STALE: Duration = Duration::from_secs(3600);

/// Claims inside one cache directory.
#[derive(Debug, Clone)]
pub struct Claims {
    dir: PathBuf,
    live: PathBuf,
    /// This handle's registration (`None`: registering failed).
    me: Option<Arc<Registration>>,
    /// Claims taken through this handle and its clones.
    taken: Arc<AtomicU64>,
}

/// A handle's entry in `{root}/live/`: exclusively locked while it lives,
/// unlinked (then unlocked) on drop.
#[derive(Debug)]
struct Registration {
    file: File,
    path: PathBuf,
    name: std::ffi::OsString,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = self.file.unlock();
    }
}

impl Registration {
    fn create(live: &Path) -> std::io::Result<Self> {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        let name =
            format!("{}-{}-{nanos}", std::process::id(), SEQ.fetch_add(1, Ordering::Relaxed));
        let path = live.join(&name);
        let open = || File::options().write(true).create_new(true).open(&path);
        let file = match open() {
            Err(e) if e.kind() == ErrorKind::NotFound => {
                std::fs::create_dir_all(live)?;
                open()?
            }
            r => r?,
        };
        if let Err(e) = file.lock() {
            let _ = std::fs::remove_file(&path);
            return Err(e);
        }
        Ok(Self { file, path, name: name.into() })
    }
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
    /// Claims in the cache at `root`, registering this handle as live there
    /// until the last clone is dropped (blocking: a few syscalls).
    pub fn new(root: &Path) -> Self {
        let live = root.join(LIVE_DIR);
        let me = Registration::create(&live).ok().map(Arc::new);
        Self { dir: root.join(DIR), live, me, taken: Arc::default() }
    }

    /// Whether another process may be fetching into this cache: another
    /// handle is registered and alive, or a claim file exists. Always `true`
    /// when this handle could not register (peers cannot see it). Blocking:
    /// one directory listing plus a lock probe per other registration.
    pub fn peers_active(&self) -> bool {
        let Some(me) = &self.me else { return true };
        let Ok(entries) = std::fs::read_dir(&self.live) else { return true };
        for e in entries.flatten() {
            if e.file_name() == me.name {
                continue;
            }
            let Ok(f) = File::open(e.path()) else { continue };
            if matches!(f.try_lock_shared(), Err(TryLockError::WouldBlock)) {
                return true;
            }
        }
        std::fs::read_dir(&self.dir).is_ok_and(|mut d| d.next().is_some())
    }

    /// How many claims were taken through this handle (and its clones).
    pub fn taken(&self) -> u64 {
        self.taken.load(Ordering::Relaxed)
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
                self.taken.fetch_add(1, Ordering::Relaxed);
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

/// Remove claim files and registrations abandoned by crashed processes
/// (best effort).
pub fn sweep(root: &Path) {
    let older = |e: &std::fs::DirEntry, age: Duration| {
        e.metadata().and_then(|m| m.modified()).is_ok_and(|m| m < SystemTime::now() - age)
    };
    if let Ok(entries) = std::fs::read_dir(root.join(DIR)) {
        for e in entries.flatten() {
            if older(&e, STALE) {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }
    if let Ok(entries) = std::fs::read_dir(root.join(LIVE_DIR)) {
        for e in entries.flatten() {
            // A live holder keeps its lock; a dead one's file is unlocked.
            let unlocked = File::open(e.path()).is_ok_and(|f| f.try_lock_shared().is_ok());
            if unlocked && older(&e, LIVE_STALE) {
                let _ = std::fs::remove_file(e.path());
            }
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
    fn live_handles_see_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let a = Claims::new(dir.path());
        assert!(!a.peers_active(), "alone");
        let b = Claims::new(dir.path());
        assert!(a.peers_active() && b.peers_active());
        // Clones are the same handle, not a peer.
        let a2 = a.clone();
        drop(b);
        assert!(!a.peers_active() && !a2.peers_active());
        // A claim file means someone (e.g. an older binary) is fetching.
        std::fs::create_dir_all(dir.path().join(DIR)).unwrap();
        File::create(dir.path().join(DIR).join("k")).unwrap();
        assert!(a.peers_active());
        std::fs::remove_file(dir.path().join(DIR).join("k")).unwrap();
        // A dead process's registration (nobody holds its lock) is no peer.
        File::create(dir.path().join(LIVE_DIR).join("1-0-0")).unwrap();
        assert!(!a.peers_active());
        drop((a, a2));
        let left = std::fs::read_dir(dir.path().join(LIVE_DIR)).unwrap().flatten().count();
        assert_eq!(left, 1, "only the dead registration is left");
    }

    #[test]
    fn unregistered_handles_always_claim() {
        let dir = tempfile::tempdir().unwrap();
        // `live` cannot be created: a file is in the way.
        std::fs::write(dir.path().join(LIVE_DIR), b"").unwrap();
        let a = Claims::new(dir.path());
        assert!(a.peers_active());
    }

    #[test]
    fn sweep_removes_dead_registrations_only() {
        let dir = tempfile::tempdir().unwrap();
        let live = Claims::new(dir.path());
        let dead = dir.path().join(LIVE_DIR).join("1-0-0");
        File::create(&dead).unwrap().set_modified(SystemTime::now() - 2 * LIVE_STALE).unwrap();
        let fresh = dir.path().join(LIVE_DIR).join("1-1-0");
        File::create(&fresh).unwrap();
        let me = live.me.as_ref().unwrap().path.clone();
        File::options()
            .write(true)
            .open(&me)
            .unwrap()
            .set_modified(SystemTime::now() - 2 * LIVE_STALE)
            .unwrap();
        sweep(dir.path());
        assert!(!dead.exists(), "dead and old: removed");
        assert!(fresh.exists(), "maybe just created, not locked yet: kept");
        assert!(me.exists(), "live (locked): kept");
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
