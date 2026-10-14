use ant_core::{Commit, EncodedObject, EntryMode, ObjectKind, Signature, TreeEntry};
use ant_store::{Backend, ObjectStoreExt, graph::write_commit};
use flate2::{Compression, write::ZlibEncoder};
use std::io::Write;

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
        let sig = Signature {
            name: "bench".into(),
            email: "bench@test".into(),
            time: revision as i64,
            tz_offset_minutes: 0,
        };
        let commit = Commit {
            tree: tree.id(),
            parents: parent.into_iter().collect(),
            author: sig.clone(),
            committer: sig,
            extra_headers: vec![],
            message: format!("revision {revision}\n").into_bytes(),
        };
        let encoded = EncodedObject::new(ObjectKind::Commit, &commit.encode());
        // Exactly the encoder's undeltified entries: zlib level 6 (`pack::LEVEL`,
        // git's default) and the pack object header size.
        for object in [&tree, &encoded] {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(6));
            encoder.write_all(object.body()).unwrap();
            undeltified += encoder.finish().unwrap().len() as u64;
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
    mut parent: Option<ant_core::ObjectId>,
    range: std::ops::Range<usize>,
) -> ant_core::ObjectId {
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
    let mut state = 123456789_u32;
    let content: Vec<_> = (0..100_000)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect();
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
        let sig = Signature {
            name: "test".into(),
            email: "test@test".into(),
            time: n as i64,
            tz_offset_minutes: 0,
        };
        parent = Some(
            write_commit(
                b,
                &Commit {
                    tree,
                    parents: parent.into_iter().collect(),
                    author: sig.clone(),
                    committer: sig,
                    extra_headers: vec![],
                    message: format!("{n}\n").into_bytes(),
                },
            )
            .await
            .unwrap(),
        );
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
    root_files: Vec<ant_core::ObjectId>,
    files: Vec<ant_core::ObjectId>,
    leaves: Vec<ant_core::ObjectId>,
    mids: Vec<ant_core::ObjectId>,
    tops: Vec<ant_core::ObjectId>,
    root: ant_core::ObjectId,
}

fn tree_of(entries: Vec<TreeEntry>) -> EncodedObject {
    EncodedObject::new(ObjectKind::Tree, &ant_core::Tree::new(entries).unwrap().encode())
}

fn file(name: String, id: ant_core::ObjectId) -> TreeEntry {
    TreeEntry { mode: EntryMode::File, name, id }
}

fn dir(name: String, id: ant_core::ObjectId) -> TreeEntry {
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
pub async fn merge_history(b: &Backend, revisions: usize) -> ant_core::ObjectId {
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
    let mut parent: Option<ant_core::ObjectId> = None;
    let mut time = 0;
    let mut commit = |tree, parents: Vec<ant_core::ObjectId>| {
        time += 1;
        let sig = Signature {
            name: "dev".into(),
            email: "dev@example.com".into(),
            time,
            tz_offset_minutes: 0,
        };
        Commit {
            tree,
            parents,
            author: sig.clone(),
            committer: sig,
            extra_headers: vec![],
            message: format!("change {time}\n").into_bytes(),
        }
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
