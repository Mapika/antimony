//! The `.ant/` state files: the stat index, the config, and their
//! (unsynced) write-temp-then-rename replacement.

use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use ant_core::{EntryMode, ObjectId};
use anyhow::{Context, bail};

use super::{CONFIG_FILE, INDEX_FILE, STATE_DIR, WORKSPACE_FORMAT};

/// The filesystem stat fields recorded per indexed entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Stat {
    pub(super) size: u64,
    pub(super) mtime_ns: i64,
    pub(super) ino: u64,
    pub(super) st_mode: u32,
}

pub(super) fn stat_of(m: &fs::Metadata) -> Stat {
    Stat {
        size: m.len(),
        mtime_ns: m.mtime().saturating_mul(1_000_000_000).saturating_add(m.mtime_nsec()),
        ino: m.ino(),
        st_mode: m.mode(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Entry {
    pub(super) mode: EntryMode,
    pub(super) id: ObjectId,
    pub(super) stat: Stat,
}

/// Indexed (materialized, non-tree) entries keyed by path, sorted.
pub(super) type Index = BTreeMap<String, Entry>;

pub(crate) fn escape_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for c in path.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

pub(crate) fn unescape_path(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

pub(super) fn state_dir(dir: &Path) -> PathBuf {
    dir.join(STATE_DIR)
}

pub(super) fn read_index(dir: &Path) -> anyhow::Result<Index> {
    let path = state_dir(dir).join(INDEX_FILE);
    let text = match fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Index::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let mut index = Index::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let f: Vec<&str> = line.splitn(7, '\t').collect();
        if f.len() != 7 {
            bail!("corrupt workspace index line {line:?}");
        }
        let entry = Entry {
            mode: EntryMode::from_octal(f[0].as_bytes())?,
            id: f[1].parse::<ObjectId>()?,
            stat: Stat {
                size: f[2].parse()?,
                mtime_ns: f[3].parse()?,
                ino: f[4].parse()?,
                st_mode: f[5].parse()?,
            },
        };
        index.insert(unescape_path(f[6]), entry);
    }
    Ok(index)
}

/// Mtime of the index file in ns, used to detect "racy" entries whose
/// content may have changed within the same timestamp tick as indexing.
pub(super) fn index_mtime(dir: &Path) -> anyhow::Result<Option<i64>> {
    match fs::symlink_metadata(state_dir(dir).join(INDEX_FILE)) {
        Ok(m) => Ok(Some(stat_of(&m).mtime_ns)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub(super) fn render_index(index: &Index) -> String {
    use std::fmt::Write as _;
    // One buffer, no allocation per line (a 96k-file index is ~15 MB).
    let mut out = String::with_capacity(index.keys().map(|p| p.len() + 120).sum());
    for (path, e) in index {
        // Writing to a String cannot fail.
        out.push_str(&e.mode.as_octal());
        out.push('\t');
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for b in e.id.as_bytes() {
            out.push(char::from(HEX[usize::from(b >> 4)]));
            out.push(char::from(HEX[usize::from(b & 15)]));
        }
        let _ = write!(
            out,
            "\t{}\t{}\t{}\t{}\t",
            e.stat.size, e.stat.mtime_ns, e.stat.ino, e.stat.st_mode,
        );
        if path.contains(['\\', '\n']) {
            out.push_str(&escape_path(path));
        } else {
            out.push_str(path);
        }
        out.push('\n');
    }
    out
}

pub(super) fn atomic_write(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, data).with_context(|| format!("writing {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("renaming into {}", path.display()))?;
    Ok(())
}

/// Persist config and index. Both are replaced atomically per file.
pub(super) fn save_state(
    dir: &Path,
    commit: ObjectId,
    tree: ObjectId,
    patterns: &[String],
    index: &Index,
) -> anyhow::Result<()> {
    let state = state_dir(dir);
    fs::create_dir_all(&state)?;
    let mut cfg = format!(
        "format={WORKSPACE_FORMAT}\nbase_commit={}\nbase_tree={}\n",
        commit.to_hex(),
        tree.to_hex()
    );
    for p in patterns {
        cfg.push_str(&format!("sparse={p}\n"));
    }
    atomic_write(&state.join(CONFIG_FILE), cfg.as_bytes())?;
    atomic_write(&state.join(INDEX_FILE), render_index(index).as_bytes())?;
    Ok(())
}

/// Index stat that never matches a file on disk: the entry is re-hashed (and
/// reported as modified) or, when the path is missing, reported as deleted.
pub(super) const STALE: Stat = Stat { size: 0, mtime_ns: 0, ino: 0, st_mode: 0 };
