//! Workspace discovery.

use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};

/// Walk up from `start` to the first directory containing `.ant/workspace`.
pub fn find_workspace(start: &Path) -> Option<PathBuf> {
    start.ancestors().find(|d| d.join(".ant").join("workspace").is_file()).map(Path::to_path_buf)
}

/// Like [`find_workspace`] with `-C` / cwd handling and an error on miss.
pub fn require_workspace(override_dir: Option<&Path>) -> Result<PathBuf> {
    let start = match override_dir {
        Some(d) => std::path::absolute(d)?,
        None => std::env::current_dir()?,
    };
    find_workspace(&start).ok_or_else(|| {
        anyhow!("not inside an ant workspace (no .ant/workspace found from {})", start.display())
    })
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
