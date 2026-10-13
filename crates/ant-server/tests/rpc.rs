//! End-to-end tests for the gRPC and HTTP transports, run against
//! `Backend::in_memory()` on ephemeral ports.

use std::collections::HashMap;
use std::future::IntoFuture;

use ant_core::{EncodedObject, ObjectId, ObjectKind};
use ant_proto::v1 as pb;
use ant_proto::v1::ant_client::AntClient;
use ant_server::{http, rpc};
use ant_store::{Backend, ObjectStoreExt};
use tokio::net::TcpListener;
use tonic::Code;
use tonic::transport::Channel;

struct Server {
    grpc: AntClient<Channel>,
    http_base: String,
    http: reqwest::Client,
    backend: Backend,
}

async fn spawn_server() -> Server {
    let backend = Backend::in_memory();

    let grpc_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();
    tokio::spawn(rpc::serve_on(backend.clone(), grpc_listener));

    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    tokio::spawn(axum::serve(http_listener, http::router(backend.clone())).into_future());

    let grpc = AntClient::connect(format!("http://{grpc_addr}")).await.unwrap();
    Server {
        grpc,
        http_base: format!("http://{http_addr}"),
        http: reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap(),
        backend,
    }
}

fn blob(body: &str) -> EncodedObject {
    EncodedObject::new(ObjectKind::Blob, body.as_bytes())
}

fn ids_of(objs: &[EncodedObject]) -> Vec<Vec<u8>> {
    objs.iter().map(|o| o.id().0.to_vec()).collect()
}

async fn get_objects(s: &mut Server, ids: Vec<Vec<u8>>) -> Result<Vec<pb::ObjectResult>, tonic::Status> {
    let mut stream = s.grpc.get_objects(pb::GetObjectsRequest { ids }).await?.into_inner();
    let mut out = Vec::new();
    while let Some(item) = stream.message().await? {
        out.push(item);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// objects
// ---------------------------------------------------------------------------

#[tokio::test]
async fn put_has_get_roundtrip() {
    let mut s = spawn_server().await;
    let a = blob("alpha");
    let b = blob("beta");
    let missing = blob("never stored");

    let ids = s
        .grpc
        .put_objects(pb::PutObjectsRequest { data: vec![a.as_bytes().to_vec(), b.as_bytes().to_vec()] })
        .await
        .unwrap()
        .into_inner()
        .ids;
    assert_eq!(ids, ids_of(&[a.clone(), b.clone()]));

    let present = s
        .grpc
        .has_objects(pb::HasObjectsRequest { ids: ids_of(&[b.clone(), missing.clone(), a.clone()]) })
        .await
        .unwrap()
        .into_inner()
        .present;
    assert_eq!(present, [true, false, true]);

    // Request order is preserved, missing ids come back found=false.
    let got = get_objects(&mut s, ids_of(&[b.clone(), missing.clone(), a.clone()])).await.unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].id, b.id().0);
    assert!(got[0].found);
    assert_eq!(got[0].data, b.as_bytes().to_vec());
    assert_eq!(got[1].id, missing.id().0);
    assert!(!got[1].found);
    assert!(got[1].data.is_empty());
    assert_eq!(got[2].data, a.as_bytes().to_vec());
}

#[tokio::test]
async fn get_objects_chunks_large_requests() {
    let mut s = spawn_server().await;
    let objs: Vec<EncodedObject> = (0..600).map(|i| blob(&format!("blob {i}"))).collect();
    s.grpc
        .put_objects(pb::PutObjectsRequest {
            data: objs.iter().map(|o| o.as_bytes().to_vec()).collect(),
        })
        .await
        .unwrap();

    // Interleave misses so chunk boundaries (256) are crossed by both kinds.
    let mut want = Vec::new();
    for (i, o) in objs.iter().enumerate() {
        want.push(o.id());
        if i % 7 == 0 {
            want.push(blob(&format!("absent {i}")).id());
        }
    }
    let got = get_objects(&mut s, want.iter().map(|i| i.0.to_vec()).collect()).await.unwrap();
    assert_eq!(got.len(), want.len());
    let by_id: HashMap<ObjectId, &EncodedObject> = objs.iter().map(|o| (o.id(), o)).collect();
    for (id, item) in want.iter().zip(&got) {
        assert_eq!(item.id, id.0);
        match by_id.get(id) {
            Some(o) => {
                assert!(item.found);
                assert_eq!(item.data, o.as_bytes().to_vec());
            }
            None => assert!(!item.found),
        }
    }
}

#[tokio::test]
async fn object_rpcs_reject_bad_input() {
    let mut s = spawn_server().await;

    let bad_id = s.grpc.get_objects(pb::GetObjectsRequest { ids: vec![vec![1, 2, 3]] }).await;
    assert_eq!(bad_id.unwrap_err().code(), Code::InvalidArgument);

    let too_many = vec![vec![0u8; 32]; 100_001];
    let err = s.grpc.has_objects(pb::HasObjectsRequest { ids: too_many }).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let err = s
        .grpc
        .put_objects(pb::PutObjectsRequest { data: vec![b"not an object".to_vec()] })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    // A malformed id mid-stream must not panic the server; a fresh request still works.
    let err = s
        .grpc
        .has_objects(pb::HasObjectsRequest { ids: vec![vec![0u8; 32], vec![9u8; 5]] })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(s.grpc.has_objects(pb::HasObjectsRequest { ids: vec![] }).await.is_ok());
}

// ---------------------------------------------------------------------------
// refs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ref_crud_and_cas() {
    let mut s = spawn_server().await;
    let a = ObjectId::from_bytes(&[1u8; 32]).unwrap();
    let b = ObjectId::from_bytes(&[2u8; 32]).unwrap();

    let get = |name: &str| pb::GetRefRequest { name: name.to_owned() };
    assert_eq!(s.grpc.get_ref(get("main")).await.unwrap().into_inner().target, None);

    let create = |new: ObjectId| pb::UpdateRefRequest {
        name: "main".into(),
        expected: None,
        new_target: Some(new.0.to_vec()),
    };
    let r = s.grpc.update_ref(create(a)).await.unwrap().into_inner();
    assert!(r.updated);
    assert_eq!(r.actual, None);

    // Creating again with expected = None must lose and report the actual tip.
    let r = s.grpc.update_ref(create(b)).await.unwrap().into_inner();
    assert!(!r.updated);
    assert_eq!(r.actual, Some(a.0.to_vec()));

    let r = s
        .grpc
        .update_ref(pb::UpdateRefRequest {
            name: "main".into(),
            expected: Some(a.0.to_vec()),
            new_target: Some(b.0.to_vec()),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.updated);
    assert_eq!(s.grpc.get_ref(get("main")).await.unwrap().into_inner().target, Some(b.0.to_vec()));

    s.grpc
        .update_ref(pb::UpdateRefRequest {
            name: "release/1.0".into(),
            expected: None,
            new_target: Some(a.0.to_vec()),
        })
        .await
        .unwrap();
    let refs = s
        .grpc
        .list_refs(pb::ListRefsRequest { prefix: "release/".into() })
        .await
        .unwrap()
        .into_inner()
        .refs;
    assert_eq!(refs, [pb::RefEntry { name: "release/1.0".into(), target: a.0.to_vec() }]);
    let all = s.grpc.list_refs(pb::ListRefsRequest { prefix: String::new() }).await.unwrap().into_inner().refs;
    assert_eq!(all.len(), 2);

    // Delete: absent new_target removes the ref.
    let r = s
        .grpc
        .update_ref(pb::UpdateRefRequest {
            name: "main".into(),
            expected: Some(b.0.to_vec()),
            new_target: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.updated);
    assert_eq!(s.grpc.get_ref(get("main")).await.unwrap().into_inner().target, None);
}

#[tokio::test]
async fn ref_names_are_validated() {
    let mut s = spawn_server().await;
    for name in ["", "a//b", "../x", "has space", "tilde~1"] {
        let err = s.grpc.get_ref(pb::GetRefRequest { name: name.into() }).await.unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "get_ref({name:?})");
        let err = s
            .grpc
            .update_ref(pb::UpdateRefRequest {
                name: name.into(),
                expected: None,
                new_target: Some(vec![0u8; 32]),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "update_ref({name:?})");
    }
    let err = s
        .grpc
        .update_ref(pb::UpdateRefRequest {
            name: "main".into(),
            expected: None,
            new_target: Some(vec![0u8; 7]),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

// ---------------------------------------------------------------------------
// HTTP object endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_get_object_200_304_404_400() {
    let s = spawn_server().await;
    let obj = blob("over http");
    s.backend.objects.put(obj.clone()).await.unwrap();
    let url = format!("{}/o/{}", s.http_base, obj.id().to_hex());
    let etag = format!("\"{}\"", obj.id().to_hex());

    let resp = s.http.get(&url).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.headers()["content-type"], "application/x-ant-object");
    assert_eq!(resp.headers()["cache-control"], "public, max-age=31536000, immutable");
    assert_eq!(resp.headers()["etag"], etag.as_str());
    assert_eq!(resp.bytes().await.unwrap().as_ref(), obj.as_bytes().as_ref());

    let resp = s.http.get(&url).header("if-none-match", &etag).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 304);
    assert!(resp.bytes().await.unwrap().is_empty());

    // Weak validator and lists containing the etag also match.
    let resp = s
        .http
        .get(&url)
        .header("if-none-match", format!("\"other\", W/{etag}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 304);

    // A non-matching validator still gets the body.
    let resp = s.http.get(&url).header("if-none-match", "\"other\"").send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    let absent = blob("not stored").id();
    let resp = s.http.get(format!("{}/o/{}", s.http_base, absent.to_hex())).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    assert_eq!(resp.headers()["cache-control"], "no-store");

    for bad in ["/o/xyz", "/o/abcd", &format!("/o/{}0", obj.id().to_hex())] {
        let resp = s.http.get(format!("{}{bad}", s.http_base)).send().await.unwrap();
        assert_eq!(resp.status(), 400, "GET {bad}");
    }

    let resp = s.http.get(format!("{}/healthz", s.http_base)).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}

#[tokio::test]
async fn grpc_and_http_see_the_same_objects() {
    let mut s = spawn_server().await;
    let obj = blob("shared view");
    s.grpc
        .put_objects(pb::PutObjectsRequest { data: vec![obj.as_bytes().to_vec()] })
        .await
        .unwrap();
    let resp = s.http.get(format!("{}/o/{}", s.http_base, obj.id().to_hex())).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), obj.as_bytes().as_ref());
}

// ---------------------------------------------------------------------------
// queries and writes (depend on the ops layer)
// ---------------------------------------------------------------------------

fn sig(name: &str) -> pb::Signature {
    pb::Signature { name: name.into(), email: format!("{name}@example.com").into_bytes(), time: 1_700_000_000, tz_offset_minutes: 0 }
}

fn write_file(path: &str, content: &str) -> pb::FileOp {
    pb::FileOp {
        path: path.into(),
        op: Some(pb::file_op::Op::Write(pb::WriteOp {
            mode: pb::EntryMode::File as i32,
            content: Some(pb::write_op::Content::Inline(content.as_bytes().to_vec())),
        })),
    }
}

fn delete_file(path: &str) -> pb::FileOp {
    pb::FileOp { path: path.into(), op: Some(pb::file_op::Op::Delete(pb::DeleteOp {})) }
}

#[tokio::test]
async fn delete_op_removes_entry() {
    let mut s = spawn_server().await;
    let c1 = commit(&mut s, None, vec![write_file("keep.txt", "k"), write_file("gone.txt", "g")], "base").await;
    let c2 = commit(&mut s, Some(c1), vec![delete_file("gone.txt")], "delete").await;
    let rev: String = c2.iter().map(|b| format!("{b:02x}")).collect();
    let gone = s
        .grpc
        .read_path(pb::ReadPathRequest { rev, path: "gone.txt".into(), include_content: false })
        .await
        .unwrap()
        .into_inner();
    assert!(!gone.found);
}

async fn commit(s: &mut Server, base: Option<Vec<u8>>, ops: Vec<pb::FileOp>, message: &str) -> Vec<u8> {
    s.grpc
        .create_changeset(pb::CreateChangesetRequest {
            base,
            ops,
            author: Some(sig("alice")),
            committer: None,
            message: message.into(),
        })
        .await
        .unwrap()
        .into_inner()
        .commit
}

#[tokio::test]
async fn changeset_resolve_read_log_diff_land() {
    let mut s = spawn_server().await;
    let c1 = commit(&mut s, None, vec![write_file("src/a.rs", "a1"), write_file("README", "r")], "first").await;
    let c2 = commit(&mut s, Some(c1.clone()), vec![write_file("src/a.rs", "a2")], "second").await;

    // Landing creates the ref at c2 and fast-forwards nothing else.
    let landed = s
        .grpc
        .land(pb::LandRequest { ref_name: "main".into(), commit: c2.clone() })
        .await
        .unwrap()
        .into_inner();
    match landed.outcome {
        Some(pb::land_response::Outcome::Landed(l)) => {
            assert_eq!(l.new_tip, c2);
            assert!(!l.rebased);
        }
        other => panic!("expected Landed, got {other:?}"),
    }

    let resolved = s.grpc.resolve_rev(pb::ResolveRevRequest { rev: "main~1".into() }).await.unwrap().into_inner();
    assert_eq!(resolved.commit, c1);
    let err = s.grpc.resolve_rev(pb::ResolveRevRequest { rev: "nope".into() }).await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    let err = s.grpc.resolve_rev(pb::ResolveRevRequest { rev: "main~x".into() }).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let file = s
        .grpc
        .read_path(pb::ReadPathRequest {
            rev: "main".into(),
            path: "src/a.rs".into(),
            include_content: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(file.found);
    assert_eq!(file.mode, pb::EntryMode::File as i32);
    assert_eq!(file.content, b"a2");
    assert!(file.entries.is_empty());

    let root = s
        .grpc
        .read_path(pb::ReadPathRequest { rev: "main".into(), path: String::new(), include_content: false })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(root.mode, pb::EntryMode::Tree as i32);
    let names: Vec<_> = root.entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, ["README", "src"]);

    let missing = s
        .grpc
        .read_path(pb::ReadPathRequest { rev: "main".into(), path: "nope".into(), include_content: false })
        .await
        .unwrap()
        .into_inner();
    assert!(!missing.found);

    let log = s
        .grpc
        .log(pb::LogRequest { rev: "main".into(), path: None, limit: 0 })
        .await
        .unwrap()
        .into_inner()
        .commits;
    let ids: Vec<_> = log.iter().map(|c| c.id.clone()).collect();
    assert_eq!(ids, [c2.clone(), c1.clone()]);
    assert_eq!(log[0].message, b"second");
    assert_eq!(log[0].parents, std::slice::from_ref(&c1));
    assert_eq!(log[0].author.as_ref().unwrap().name, b"alice");

    let log = s
        .grpc
        .log(pb::LogRequest { rev: "main".into(), path: Some("README".into()), limit: 0 })
        .await
        .unwrap()
        .into_inner()
        .commits;
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].id, c1);

    let diff = s
        .grpc
        .diff(pb::DiffRequest { from_rev: "main~1".into(), to_rev: "main".into() })
        .await
        .unwrap()
        .into_inner()
        .changes;
    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].path, "src/a.rs");
    assert_eq!(diff[0].kind, pb::ChangeKind::Modified as i32);

    let from_empty = s
        .grpc
        .diff(pb::DiffRequest { from_rev: String::new(), to_rev: "main".into() })
        .await
        .unwrap()
        .into_inner()
        .changes;
    assert_eq!(from_empty.len(), 2);
    assert!(from_empty.iter().all(|c| c.kind == pb::ChangeKind::Added as i32));
}

#[tokio::test]
async fn land_rebases_and_reports_conflicts() {
    let mut s = spawn_server().await;
    let base = commit(&mut s, None, vec![write_file("a.txt", "one\n")], "base").await;
    s.grpc
        .land(pb::LandRequest { ref_name: "main".into(), commit: base.clone() })
        .await
        .unwrap();

    // Two non-conflicting changes based on the same parent.
    let left = commit(&mut s, Some(base.clone()), vec![write_file("left.txt", "l")], "left").await;
    let right = commit(&mut s, Some(base.clone()), vec![write_file("right.txt", "r")], "right").await;
    let first = s
        .grpc
        .land(pb::LandRequest { ref_name: "main".into(), commit: left.clone() })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(first.outcome, Some(pb::land_response::Outcome::Landed(ref l)) if l.new_tip == left && !l.rebased));

    let second = s
        .grpc
        .land(pb::LandRequest { ref_name: "main".into(), commit: right.clone() })
        .await
        .unwrap()
        .into_inner();
    let rebased_tip = match second.outcome {
        Some(pb::land_response::Outcome::Landed(l)) => {
            assert!(l.rebased);
            assert_ne!(l.new_tip, right);
            l.new_tip
        }
        other => panic!("expected rebased Landed, got {other:?}"),
    };
    let tip = s.grpc.resolve_rev(pb::ResolveRevRequest { rev: "main".into() }).await.unwrap().into_inner();
    assert_eq!(tip.commit, rebased_tip);

    // Conflicting edit of a.txt against the current tip.
    let c1 = commit(&mut s, Some(base.clone()), vec![write_file("a.txt", "x\n")], "x").await;
    let c2 = commit(&mut s, Some(base.clone()), vec![write_file("a.txt", "y\n")], "y").await;
    s.grpc.land(pb::LandRequest { ref_name: "main".into(), commit: c1 }).await.unwrap();
    let outcome = s
        .grpc
        .land(pb::LandRequest { ref_name: "main".into(), commit: c2 })
        .await
        .unwrap()
        .into_inner();
    match outcome.outcome {
        Some(pb::land_response::Outcome::Conflict(c)) => {
            assert_eq!(c.paths, ["a.txt"]);
            assert!(!c.tip.is_empty());
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
}

#[tokio::test]
async fn changeset_rejects_bad_requests() {
    let mut s = spawn_server().await;
    let err = s
        .grpc
        .create_changeset(pb::CreateChangesetRequest {
            base: None,
            ops: vec![pb::FileOp { path: "../escape".into(), op: None }],
            author: Some(sig("alice")),
            committer: None,
            message: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let err = s
        .grpc
        .create_changeset(pb::CreateChangesetRequest {
            base: None,
            ops: vec![],
            author: None,
            committer: None,
            message: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let err = s
        .grpc
        .create_changeset(pb::CreateChangesetRequest {
            base: Some(vec![7u8; 32]),
            ops: vec![],
            author: Some(sig("alice")),
            committer: None,
            message: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn blame_rpc() {
    let mut s = spawn_server().await;
    let c1 = commit(&mut s, None, vec![write_file("f", "a\nb\n")], "first").await;
    let c2 = commit(&mut s, Some(c1.clone()), vec![write_file("f", "a\nB\nc\n")], "second").await;
    s.grpc.land(pb::LandRequest { ref_name: "main".into(), commit: c2.clone() }).await.unwrap();
    let r = s.grpc.blame(pb::BlameRequest { rev: "main".into(), path: "f".into() }).await.unwrap().into_inner();
    let got: Vec<_> = r.lines.iter().map(|l| (l.commit.clone(), l.line.as_str())).collect();
    assert_eq!(got, vec![(c1.clone(), "a"), (c2.clone(), "B"), (c2.clone(), "c")]);
    let e = s.grpc.blame(pb::BlameRequest { rev: "main".into(), path: "nope".into() }).await.unwrap_err();
    assert_eq!(e.code(), Code::NotFound);
}
