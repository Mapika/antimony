//! `RemoteObjectStore` against a real `ant-server` on ephemeral ports.

use ant_core::{EncodedObject, ObjectId, ObjectKind};
use ant_proto::v1 as pb;
use ant_server::acl::Acl;
use ant_server::serve::spawn_local;
use ant_store::{Backend, ObjectStore, ObjectStoreExt};
use antd::remote::RemoteObjectStore;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

struct Running {
    grpc_url: String,
    http_url: String,
    backend: Backend,
}

async fn spawn_server() -> Running {
    let backend = Backend::in_memory();
    let server = spawn_local(backend.clone(), Acl::open()).await.unwrap();
    Running { grpc_url: server.grpc_url(), http_url: server.http_url(), backend }
}

fn blob(body: &str) -> EncodedObject {
    EncodedObject::new(ObjectKind::Blob, body.as_bytes())
}

/// A dead HTTP base: connection refused, so every HTTP read is a transport error.
fn dead_http_base() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    format!("http://{addr}")
}

/// A bare HTTP/1.1 responder for simulating misbehaving caches. `respond`
/// maps the request path to (status, body); one request per connection.
async fn fake_http(respond: impl Fn(&str) -> (u16, Vec<u8>) + Send + Sync + 'static) -> String {
    let respond = Arc::new(respond);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let respond = respond.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).into_owned();
                let path = req.split_whitespace().nth(1).unwrap_or("/").to_owned();
                let (status, body) = respond(&path);
                let head = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nContent-Type: application/x-ant-object\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(&body).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn grpc_put_has_get_in_order() {
    let r = spawn_server().await;
    let store = RemoteObjectStore::connect(&r.grpc_url, None).await.unwrap();

    let a = blob("alpha");
    let b = blob("beta");
    let absent = blob("absent");
    store.put_many(&[a.clone(), b.clone()]).await.unwrap();
    // Idempotent re-put.
    store.put_many(std::slice::from_ref(&a)).await.unwrap();
    store.put_many(&[]).await.unwrap();

    assert_eq!(store.has_many(&[b.id(), absent.id(), a.id()]).await.unwrap(), [true, false, true]);

    let got = store.get_many(&[b.id(), absent.id(), a.id(), b.id()]).await.unwrap();
    assert_eq!(got.len(), 4);
    assert_eq!(got[0].as_ref().unwrap().as_bytes(), b.as_bytes());
    assert!(got[1].is_none());
    assert_eq!(got[2].as_ref().unwrap().as_bytes(), a.as_bytes());
    assert_eq!(got[3].as_ref().unwrap().as_bytes(), b.as_bytes());

    assert!(store.get_many(&[]).await.unwrap().is_empty());
    assert!(store.has_many(&[]).await.unwrap().is_empty());
}

#[tokio::test]
async fn grpc_chunks_large_batches() {
    let r = spawn_server().await;
    let store = RemoteObjectStore::connect(&r.grpc_url, None).await.unwrap();

    // More than one GetObjects/HasObjects request worth of ids.
    let objs: Vec<EncodedObject> = (0..5000).map(|i| blob(&format!("object {i}"))).collect();
    store.put_many(&objs).await.unwrap();

    let mut ids: Vec<ObjectId> = objs.iter().map(EncodedObject::id).collect();
    let missing = blob("missing").id();
    ids.insert(4097, missing);

    let present = store.has_many(&ids).await.unwrap();
    assert_eq!(present.iter().filter(|p| **p).count(), objs.len());
    assert!(!present[4097]);

    let got = store.get_many(&ids).await.unwrap();
    assert_eq!(got.len(), ids.len());
    assert!(got[4097].is_none());
    for (i, g) in got.iter().enumerate() {
        if i == 4097 {
            continue;
        }
        let o = &objs[if i > 4097 { i - 1 } else { i }];
        assert_eq!(g.as_ref().unwrap().as_bytes(), o.as_bytes());
    }
}

#[tokio::test]
async fn http_reads_are_used_when_available() {
    let r = spawn_server().await;
    let store = RemoteObjectStore::connect(&r.grpc_url, Some(&r.http_url)).await.unwrap();

    let a = blob("via http");
    let b = blob("also via http");
    store.put_many(&[a.clone(), b.clone()]).await.unwrap();
    let absent = blob("nowhere").id();

    let got = store.get_many(&[a.id(), absent, b.id()]).await.unwrap();
    assert_eq!(got[0].as_ref().unwrap().as_bytes(), a.as_bytes());
    assert!(got[1].is_none());
    assert_eq!(got[2].as_ref().unwrap().as_bytes(), b.as_bytes());

    // Trailing slash on the HTTP base is tolerated.
    let store =
        RemoteObjectStore::connect(&r.grpc_url, Some(&format!("{}/", r.http_url))).await.unwrap();
    assert!(store.get(&a.id()).await.unwrap().is_some());
}

#[tokio::test]
async fn falls_back_to_grpc_on_http_transport_error() {
    let r = spawn_server().await;
    let writer = RemoteObjectStore::connect(&r.grpc_url, None).await.unwrap();
    let a = blob("only reachable over grpc in this test");
    writer.put_many(std::slice::from_ref(&a)).await.unwrap();

    let store = RemoteObjectStore::connect(&r.grpc_url, Some(&dead_http_base())).await.unwrap();
    let got = store.get_many(&[a.id(), blob("nope").id()]).await.unwrap();
    assert_eq!(got[0].as_ref().unwrap().as_bytes(), a.as_bytes());
    assert!(got[1].is_none());
}

#[tokio::test]
async fn falls_back_to_grpc_on_corrupt_http_object() {
    // A cache that serves the wrong bytes must not be trusted: hashes are
    // verified and the read goes to gRPC instead.
    let r = spawn_server().await;
    let a = blob("the real object");
    r.backend.objects.put(a.clone()).await.unwrap();

    let liar_body = blob("some other object").as_bytes().to_vec();
    let liar = fake_http(move |_| (200, liar_body.clone())).await;
    let store = RemoteObjectStore::connect(&r.grpc_url, Some(&liar)).await.unwrap();
    let got = store.get_many(&[a.id()]).await.unwrap();
    assert_eq!(got[0].as_ref().unwrap().as_bytes(), a.as_bytes());
}

#[tokio::test]
async fn http_404_is_none_and_500_falls_back() {
    let r = spawn_server().await;

    // Find an object whose id starts with '0' so a 500 can target it.
    let zero = (0..10_000u32)
        .map(|i| blob(&format!("zero prefix {i}")))
        .find(|o| o.id().to_hex().starts_with('0'))
        .expect("a 0-prefixed id exists");
    r.backend.objects.put(zero.clone()).await.unwrap();

    // 404 from the cache is an answer: the object is reported absent.
    let flaky =
        fake_http(
            |path| {
                if path.starts_with("/o/0") { (500, Vec::new()) } else { (404, Vec::new()) }
            },
        )
        .await;
    let store = RemoteObjectStore::connect(&r.grpc_url, Some(&flaky)).await.unwrap();
    let got = store.get_many(&[blob("unknown to cache").id()]).await.unwrap();
    assert!(got[0].is_none());

    // A 500 is an error: the whole batch falls back to gRPC and finds it.
    let got = store.get_many(&[zero.id()]).await.unwrap();
    assert_eq!(got[0].as_ref().unwrap().as_bytes(), zero.as_bytes());
}

#[tokio::test]
async fn client_exposes_raw_rpcs() {
    let r = spawn_server().await;
    let store = RemoteObjectStore::connect(&r.grpc_url, None).await.unwrap();
    let mut client = store.client();

    let target = blob("tip").id();
    let resp = client
        .update_ref(pb::UpdateRefRequest {
            name: "main".into(),
            expected: None,
            new_target: Some(target.0.to_vec()),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.updated);
    let refs = client
        .list_refs(pb::ListRefsRequest { prefix: String::new() })
        .await
        .unwrap()
        .into_inner()
        .refs;
    assert_eq!(refs, [pb::RefEntry { name: "main".into(), target: target.0.to_vec() }]);
}
