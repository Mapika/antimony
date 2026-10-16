// Fixtures and git helpers shared by the integration tests (`tests/*.rs`) and,
// through `include!`, by the pack unit tests (`src/pack.rs`); every item is
// used in both, so neither sees dead code.

use ant_core::{Commit, EncodedObject, EntryMode, ObjectId, ObjectKind, Signature, TreeEntry};
use ant_store::{Backend, ObjectStoreExt, graph::write_commit};
use flate2::{Compress, Compression, FlushCompress, Status};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// A commit of `tree` by `who` (name, email) as author and committer.
pub fn commit_of(
    tree: ObjectId,
    parents: Vec<ObjectId>,
    who: (&str, &str),
    time: i64,
    message: String,
) -> Commit {
    let sig = Signature { name: who.0.into(), email: who.1.into(), time, tz_offset_minutes: 0 };
    Commit {
        tree,
        parents,
        author: sig.clone(),
        committer: sig,
        extra_headers: vec![],
        message: message.into_bytes(),
    }
}

/// `len` deterministic, incompressible bytes (xorshift32 from `seed`).
pub fn noise(seed: u32, len: usize) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect()
}

/// `git` in `dir`, isolated from the user's and system configuration.
pub fn git_command(dir: &Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new("git");
    command
        .current_dir(dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0");
    command
}

/// Kind and delta depth (`None`: stored whole) of every object in the packs
/// under `pack_dir`, from `git verify-pack -v`.
pub async fn packed_objects(pack_dir: &Path) -> Vec<(String, Option<usize>)> {
    let mut objects = Vec::new();
    for entry in std::fs::read_dir(pack_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|e| e != "idx") {
            continue;
        }
        let output = git_command(pack_dir)
            .args(["verify-pack", "-v", path.to_str().unwrap()])
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        for line in String::from_utf8(output.stdout).unwrap().lines() {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.first().is_some_and(|id| id.len() == 64) {
                let depth = (fields.len() == 7).then(|| fields[5].parse().unwrap());
                objects.push((fields[1].to_owned(), depth));
            }
        }
    }
    objects
}

/// Total size of the `.pack` files under `pack_dir`.
pub fn pack_bytes(pack_dir: &Path) -> u64 {
    std::fs::read_dir(pack_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "pack"))
        .map(|p| std::fs::metadata(p).unwrap().len())
        .sum()
}

/// This process's resident set size in KiB (0 when unknown).
pub fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:")?.split_whitespace().next()?.parse().ok())
        .unwrap_or(0)
}

/// Sample [`rss_kib`] every 5 ms into a running maximum, starting at `before`,
/// until the returned task is aborted.
pub fn watch_peak_rss(before: u64) -> (Arc<AtomicU64>, tokio::task::JoinHandle<()>) {
    let peak = Arc::new(AtomicU64::new(before));
    let monitor_peak = peak.clone();
    let monitor = tokio::spawn(async move {
        loop {
            monitor_peak.fetch_max(rss_kib(), Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    (peak, monitor)
}

pub async fn history(revisions: usize, width: usize) -> (Backend, u64) {
    let b = Backend::in_memory();
    let blobs: Vec<_> = (0..width)
        .map(|n| EncodedObject::new(ObjectKind::Blob, format!("initial file {n}\n").as_bytes()))
        .collect();
    b.objects.put_many(&blobs).await.unwrap();
    let mut entries: Vec<_> = blobs
        .iter()
        .enumerate()
        .map(|(n, blob)| TreeEntry {
            mode: EntryMode::File,
            name: format!("file{n:04}"),
            id: blob.id(),
        })
        .collect();
    let mut parent = None;
    let mut undeltified = 44_u64; // pack header and SHA-256 trailer
    for revision in 0..revisions {
        let blob =
            EncodedObject::new(ObjectKind::Blob, format!("revision {revision}\n").as_bytes());
        let index = revision % entries.len();
        entries[index].id = b.objects.put(blob).await.unwrap();
        let tree = ant_core::Tree::new(entries.clone()).unwrap();
        let tree = EncodedObject::new(ObjectKind::Tree, &tree.encode());
        let commit = commit_of(
            tree.id(),
            parent.into_iter().collect(),
            ("bench", "bench@test"),
            revision as i64,
            format!("revision {revision}\n"),
        );
        let encoded = EncodedObject::new(ObjectKind::Commit, &commit.encode());
        // Exactly the encoder's undeltified entries: zlib level 6 (`pack::LEVEL`,
        // git's default), all input in one call as the encoder does (zlib-rs
        // compresses input fed in pieces slightly differently), and the pack
        // object header size.
        for object in [&tree, &encoded] {
            let mut compress = Compress::new(Compression::new(6), true);
            let mut out = Vec::with_capacity(object.body().len() * 2 + 64);
            let status =
                compress.compress_vec(object.body(), &mut out, FlushCompress::Finish).unwrap();
            assert_eq!(status, Status::StreamEnd);
            undeltified += out.len() as u64;
            let mut size = object.body().len() >> 4;
            undeltified += 1;
            while size != 0 {
                undeltified += 1;
                size >>= 7;
            }
        }
        b.objects.put(tree).await.unwrap();
        parent = Some(write_commit(&b, &commit).await.unwrap());
    }
    b.refs.cas_ref("main", None, parent).await.unwrap();
    (b, undeltified)
}

pub async fn revisions(
    b: &Backend,
    mut parent: Option<ObjectId>,
    range: std::ops::Range<usize>,
) -> ObjectId {
    use ant_store::graph::{build_tree, write_tree};
    let fixed: Vec<_> = (0..64).map(|n| (format!("fixed{n:03}"), format!("file {n}"))).collect();
    let root = build_tree(
        b.objects.as_ref(),
        &fixed.iter().map(|(name, body)| (name.as_str(), body.as_bytes())).collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    let tree = b.objects.get(&root).await.unwrap().unwrap();
    let fixed = ant_core::Tree::decode(tree.body()).unwrap();
    let content = noise(123456789, 100_000);
    for n in range {
        let mut content = content.clone();
        content[1000..1008].copy_from_slice(&(n as u64).to_le_bytes());
        let blob = b.objects.put(EncodedObject::new(ObjectKind::Blob, &content)).await.unwrap();
        let (mode, id) = if n % 40 < 2 {
            (
                EntryMode::Tree,
                write_tree(
                    b.objects.as_ref(),
                    vec![TreeEntry { name: "child".into(), mode: EntryMode::File, id: blob }],
                )
                .await
                .unwrap(),
            )
        } else {
            (EntryMode::File, blob)
        };
        let mut entries = fixed.entries().to_vec();
        entries.push(TreeEntry { name: "changing".into(), mode, id });
        let tree = write_tree(b.objects.as_ref(), entries).await.unwrap();
        let commit = commit_of(
            tree,
            parent.into_iter().collect(),
            ("test", "test@test"),
            n as i64,
            format!("{n}\n"),
        );
        parent = Some(write_commit(b, &commit).await.unwrap());
    }
    parent.unwrap()
}

/// Fan-out of the fixture's directory levels and files per leaf directory.
const FAN: usize = 5;
const LEAF_FILES: usize = 20;
const ROOT_FILES: usize = 40;

/// Every file and directory id of one revision of [`merge_history`]'s tree:
/// `top/mid/leaf/file` directories `FAN` wide, `LEAF_FILES` per leaf.
#[derive(Clone)]
struct Snapshot {
    root_files: Vec<ObjectId>,
    files: Vec<ObjectId>,
    leaves: Vec<ObjectId>,
    mids: Vec<ObjectId>,
    tops: Vec<ObjectId>,
    root: ObjectId,
}

fn tree_of(entries: Vec<TreeEntry>) -> EncodedObject {
    EncodedObject::new(ObjectKind::Tree, &ant_core::Tree::new(entries).unwrap().encode())
}

fn file(name: String, id: ObjectId) -> TreeEntry {
    TreeEntry { mode: EntryMode::File, name, id }
}

fn dir(name: String, id: ObjectId) -> TreeEntry {
    TreeEntry { mode: EntryMode::Tree, name, id }
}

impl Snapshot {
    /// Recompute the trees above `leaf` (and the root), returning new trees.
    fn rehash(&mut self, leaf: usize) -> Vec<EncodedObject> {
        let (top, mid) = (leaf / (FAN * FAN), leaf / FAN);
        let leaf_tree = tree_of(
            (0..LEAF_FILES)
                .map(|n| file(format!("source_file_{n:02}.c"), self.files[leaf * LEAF_FILES + n]))
                .collect(),
        );
        self.leaves[leaf] = leaf_tree.id();
        let mid_tree = tree_of(
            (0..FAN).map(|n| dir(format!("leaf{n}"), self.leaves[mid * FAN + n])).collect(),
        );
        self.mids[mid] = mid_tree.id();
        let top_tree =
            tree_of((0..FAN).map(|n| dir(format!("mid{n}"), self.mids[top * FAN + n])).collect());
        self.tops[top] = top_tree.id();
        let mut entries: Vec<_> = (0..FAN).map(|n| dir(format!("top{n}"), self.tops[n])).collect();
        entries.extend(
            self.root_files.iter().enumerate().map(|(n, id)| file(format!("README-{n:02}"), *id)),
        );
        let root = tree_of(entries);
        self.root = root.id();
        vec![leaf_tree, mid_tree, top_tree, root]
    }
}

/// A realistic history shape: `revisions` mainline commits that each touch
/// two files of one (mostly "hot") leaf directory three levels deep, plus a
/// short topic branch forked 40 commits back and merged every 8 commits.
/// Branch interleaving is what defeats "delta against the previous version
/// seen": consecutive versions of a directory come from different branches.
pub async fn merge_history(b: &Backend, revisions: usize) -> ObjectId {
    let leaves = FAN * FAN * FAN;
    let mut state = 0x2545_f491_u32;
    let mut random = move |n: usize| {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        state as usize % n
    };
    let mut version = 0;
    let mut blob = |objects: &mut Vec<EncodedObject>| {
        version += 1;
        let object =
            EncodedObject::new(ObjectKind::Blob, format!("version {version}\n").as_bytes());
        let id = object.id();
        objects.push(object);
        id
    };
    let mut objects = Vec::new();
    let initial = blob(&mut objects);
    let mut main = Snapshot {
        root_files: vec![initial; ROOT_FILES],
        files: vec![initial; leaves * LEAF_FILES],
        leaves: vec![initial; leaves],
        mids: vec![initial; FAN * FAN],
        tops: vec![initial; FAN],
        root: initial,
    };
    for leaf in 0..leaves {
        objects.extend(main.rehash(leaf));
    }
    let mut history = std::collections::VecDeque::new();
    let mut parent: Option<ObjectId> = None;
    let mut time = 0;
    let mut commit = |tree, parents: Vec<ObjectId>| {
        time += 1;
        commit_of(tree, parents, ("dev", "dev@example.com"), time, format!("change {time}\n"))
    };
    for revision in 0..revisions {
        // Most work happens in a few hot directories.
        let leaf = if random(4) == 0 { random(leaves) } else { random(8) * 13 % leaves };
        for _ in 0..2 {
            main.files[leaf * LEAF_FILES + random(LEAF_FILES)] = blob(&mut objects);
        }
        if random(10) == 0 {
            main.root_files[random(ROOT_FILES)] = blob(&mut objects);
        }
        objects.extend(main.rehash(leaf));
        b.objects.put_many(&std::mem::take(&mut objects)).await.unwrap();
        let next = commit(main.root, parent.into_iter().collect());
        parent = Some(write_commit(b, &next).await.unwrap());
        history.push_back((main.clone(), parent.unwrap()));
        if history.len() > 40 {
            history.pop_front();
        }
        if revision % 8 == 7 {
            let (mut topic, mut topic_parent) = history.front().cloned().unwrap();
            let leaf = random(leaves);
            for _ in 0..3 {
                topic.files[leaf * LEAF_FILES + random(LEAF_FILES)] = blob(&mut objects);
                objects.extend(topic.rehash(leaf));
                b.objects.put_many(&std::mem::take(&mut objects)).await.unwrap();
                let next = commit(topic.root, vec![topic_parent]);
                topic_parent = write_commit(b, &next).await.unwrap();
            }
            let range = leaf * LEAF_FILES..(leaf + 1) * LEAF_FILES;
            main.files[range.clone()].copy_from_slice(&topic.files[range]);
            objects.extend(main.rehash(leaf));
            b.objects.put_many(&std::mem::take(&mut objects)).await.unwrap();
            let merge = commit(main.root, vec![parent.unwrap(), topic_parent]);
            parent = Some(write_commit(b, &merge).await.unwrap());
        }
    }
    parent.unwrap()
}
