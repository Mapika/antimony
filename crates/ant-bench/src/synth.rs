//! Synthetic kernel-shaped repository for `checkout --synthetic-files`.
//!
//! `files` blobs with log-normal sizes (median ~3 KiB, mean ~16 KiB, so
//! 96k files is ~1.5 GiB like the Linux kernel) spread over `dirs`
//! directories arranged as a fanout-6 tree. Two percent of files are
//! executable and two percent symlinks. Contents are deterministic per index
//! and partly compressible (text-like), unique per file.

use ant_core::{Commit, EncodedObject, EntryMode, Object, ObjectKind, Signature, Tree, TreeEntry};
use ant_store::Backend;
use ant_store::graph::write_commit;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::path::{Path, PathBuf};

const FANOUT: usize = 6;
const BATCH: usize = 1024;

fn body(rng: &mut StdRng, size: usize, index: usize) -> Vec<u8> {
    // Words from a small vocabulary give zstd something to chew on, like source code.
    const WORDS: [&str; 16] = [
        "static ",
        "int ",
        "struct ",
        "return ",
        "if (",
        ") {\n",
        "}\n",
        "\t",
        "void ",
        "u32 ",
        "/* ",
        " */\n",
        "err = ",
        "NULL",
        ";\n",
        "#include <linux/",
    ];
    let mut out = Vec::with_capacity(size + 32);
    out.extend_from_slice(format!("/* file {index} */\n").as_bytes());
    while out.len() < size {
        if rng.random_bool(0.7) {
            out.extend_from_slice(WORDS[rng.random_range(0..WORDS.len())].as_bytes());
        } else {
            let n = rng.random_range(1..8);
            for _ in 0..n {
                out.push(b'a' + rng.random_range(0..26u8));
            }
        }
    }
    out.truncate(size.max(1));
    out
}

/// Relative path of directory `d` (empty for the root; `d3/d19` and so on).
fn dir_rel_path(d: usize) -> PathBuf {
    let mut parts = Vec::new();
    let mut cur = d;
    while cur != 0 {
        parts.push(format!("d{cur}"));
        cur = (cur - 1) / FANOUT;
    }
    parts.iter().rev().collect()
}

fn export_file(
    root: &Path,
    dir: usize,
    name: &str,
    mode: EntryMode,
    content: &[u8],
) -> anyhow::Result<()> {
    use anyhow::Context as _;
    use std::os::unix::fs::{PermissionsExt, symlink};
    let dir_path = root.join(dir_rel_path(dir));
    std::fs::create_dir_all(&dir_path).with_context(|| format!("mkdir {}", dir_path.display()))?;
    let path = dir_path.join(name);
    if mode == EntryMode::Symlink {
        symlink(String::from_utf8_lossy(content).as_ref(), &path)
            .with_context(|| format!("symlink {}", path.display()))?;
    } else {
        std::fs::write(&path, content).with_context(|| format!("write {}", path.display()))?;
        if mode == EntryMode::Executable {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))?;
        }
    }
    Ok(())
}

/// `ant-bench synth`: build the synthetic tree into a store (and optionally
/// export identical files to a directory, for head-to-head runs).
#[derive(clap::Args, Debug)]
pub struct Args {
    /// Files in the tree.
    #[arg(long, default_value_t = 96000)]
    pub files: usize,
    /// Directories (default 70% of files).
    #[arg(long)]
    pub dirs: Option<usize>,
    /// Ref to create at the synthetic commit.
    #[arg(long = "ref", default_value = "synth")]
    pub ref_name: String,
    /// Also write the files into this (new or empty) directory.
    #[arg(long)]
    pub export: Option<PathBuf>,
    #[command(flatten)]
    pub store: ant_server::config::StoreArgs,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let backend = args.store.backend().await?;
    let dirs = args.dirs.unwrap_or(args.files * 7 / 10);
    let start = std::time::Instant::now();
    let bytes = build(&backend, args.files, dirs, &args.ref_name, args.export.as_deref()).await?;
    println!(
        "synthetic tree: {} files, {} dirs, {:.1} MiB of blobs, ref {} in {:.1}s",
        args.files,
        dirs,
        bytes as f64 / 1048576.0,
        args.ref_name,
        start.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Populate `backend` with the synthetic tree, a commit and the ref `ref_name`.
/// With `export`, the same files are also written under that directory.
/// Returns the total blob bytes.
pub async fn build(
    backend: &Backend,
    files: usize,
    dirs: usize,
    ref_name: &str,
    export: Option<&Path>,
) -> anyhow::Result<u64> {
    let dirs = dirs.max(1);
    let mut rng = StdRng::seed_from_u64(0xa17);
    let mut dir_files: Vec<Vec<TreeEntry>> = (0..dirs).map(|_| Vec::new()).collect();
    let mut batch: Vec<EncodedObject> = Vec::new();
    let mut total = 0u64;
    for i in 0..files {
        // Box-Muller log-normal, sigma 1.8, median 3 KiB, clamped to 8 MiB.
        let (u1, u2): (f64, f64) = (rng.random_range(1e-12..1.0), rng.random());
        let z = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
        let size = (3072.0 * (1.8 * z).exp()).clamp(1.0, 8.0 * 1024.0 * 1024.0) as usize;
        let d = rng.random_range(0..dirs);
        let (mode, content) = match i % 50 {
            0 => (EntryMode::Executable, body(&mut rng, size, i)),
            1 => (EntryMode::Symlink, format!("../target{i}").into_bytes()),
            _ => (EntryMode::File, body(&mut rng, size, i)),
        };
        total += content.len() as u64;
        if let Some(root) = export {
            tokio::task::block_in_place(|| {
                export_file(root, d, &format!("f{i}.c"), mode, &content)
            })?;
        }
        let obj = EncodedObject::new(ObjectKind::Blob, &content);
        dir_files[d].push(TreeEntry { mode, name: format!("f{i}.c"), id: obj.id() });
        batch.push(obj);
        if batch.len() >= BATCH {
            backend.objects.put_many(&batch).await?;
            batch.clear();
        }
    }
    backend.objects.put_many(&batch).await?;

    // Children have larger indices than parents: fold from the back.
    let mut root = None;
    let mut sub: Vec<Vec<TreeEntry>> = (0..dirs).map(|_| Vec::new()).collect();
    let mut trees = Vec::new();
    for d in (0..dirs).rev() {
        let mut entries = std::mem::take(&mut dir_files[d]);
        entries.append(&mut sub[d]);
        let obj = Object::Tree(Tree::new(entries)?).encode();
        let id = obj.id();
        trees.push(obj);
        if d == 0 {
            root = Some(id);
        } else {
            sub[(d - 1) / FANOUT].push(TreeEntry {
                mode: EntryMode::Tree,
                name: format!("d{d}"),
                id,
            });
        }
    }
    for chunk in trees.chunks(BATCH) {
        backend.objects.put_many(chunk).await?;
    }
    let tree = root.ok_or_else(|| anyhow::anyhow!("no root"))?;
    let sig = Signature {
        name: b"bench".to_vec(),
        email: b"b@b".to_vec(),
        time: 1,
        tz_offset_minutes: 0,
    };
    let commit = Commit {
        tree,
        parents: vec![],
        author: sig.clone(),
        committer: sig,
        extra_headers: vec![],
        message: b"synthetic\n".to_vec(),
    };
    let id = write_commit(backend, &commit).await?;
    backend.refs.cas_ref(ref_name, None, Some(id)).await?;
    Ok(total)
}
