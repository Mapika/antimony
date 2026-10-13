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
        // Exactly the old encoder's compression and object header size.
        for object in [&tree, &encoded] {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
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
