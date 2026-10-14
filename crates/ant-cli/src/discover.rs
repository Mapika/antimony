//! Workspace discovery.

use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};

/// Walk up from `start` to the first directory containing `.ant/workspace`.
pub fn find_workspace(start: &Path) -> Option<PathBuf> {
    start.ancestors().find(|d| d.join(".ant").join("workspace").is_file()).map(Path::to_path_buf)
}

/// A lazy workspace whose FUSE daemon is gone: the mountpoint still exists
/// in the kernel's mount table but every access fails with `ENOTCONN`.
#[derive(Debug)]
pub struct DeadMount(pub PathBuf);

impl std::fmt::Display for DeadMount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "lazy workspace mount {} is dead: its FUSE daemon is gone (Transport endpoint is not connected)",
            self.0.display()
        )
    }
}
impl std::error::Error for DeadMount {}

/// Does `dir` itself fail with `ENOTCONN`?
pub fn is_dead(dir: &Path) -> bool {
    std::fs::metadata(dir).is_err_and(|e| e.kind() == std::io::ErrorKind::NotConnected)
}

/// The outermost directory of `start`'s ancestry that is a dead FUSE mount.
pub fn find_dead_mount(start: &Path) -> Option<PathBuf> {
    start.ancestors().filter(|d| is_dead(d)).last().map(Path::to_path_buf)
}

/// Where workspace discovery starts: `-C <dir>` (made absolute), else cwd.
pub fn start_dir(override_dir: Option<&Path>) -> Result<PathBuf> {
    Ok(match override_dir {
        Some(d) => std::path::absolute(d)?,
        None => std::env::current_dir()?,
    })
}

/// Like [`find_workspace`] with `-C` / cwd handling and an error on miss.
pub fn require_workspace(override_dir: Option<&Path>) -> Result<PathBuf> {
    let start = start_dir(override_dir)?;
    if let Some(root) = find_workspace(&start) {
        return Ok(root);
    }
    // A killed lazy-workspace daemon leaves a mount whose files all fail with
    // ENOTCONN, which would otherwise look like "no workspace here".
    if let Some(dead) = find_dead_mount(&start) {
        return Err(DeadMount(dead).into());
    }
    Err(anyhow!("not inside an ant workspace (no .ant/workspace found from {})", start.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walks_up() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path();
        std::fs::create_dir_all(root.join("ws/.ant")).unwrap();
        std::fs::write(root.join("ws/.ant/workspace"), "x").unwrap();
        std::fs::create_dir_all(root.join("ws/a/b")).unwrap();
        assert_eq!(
            find_workspace(&root.join("ws/a/b")).as_deref(),
            Some(root.join("ws").as_path())
        );
        assert_eq!(find_workspace(&root.join("ws")).as_deref(), Some(root.join("ws").as_path()));
        assert_eq!(find_workspace(root), None);
        // `.ant` without the workspace file does not count.
        std::fs::create_dir_all(root.join("other/.ant")).unwrap();
        assert_eq!(find_workspace(&root.join("other")), None);
    }
}
