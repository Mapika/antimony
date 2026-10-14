//! Access-control enforcement (`ant_server::acl`) through the real gRPC and
//! HTTP front ends, against `Backend::in_memory()` (plus one Postgres-backed
//! test, `--include-ignored`).
//!
//! Policy: the whole repo is readable by everyone and writable by agents;
//! `secrets/` is readable and writable only by the security group.
//! `reader` is an agent without security access, `writer` an agent with it,
//! `admin` bypasses everything.

mod common;

use std::path::Path;
use std::time::Duration;

use ant_auth::{Policy, hash_token};
use ant_core::{
    Commit, EncodedObject, EntryMode, Object, ObjectId, ObjectKind, RepoPath, Signature, Tree,
    TreeEntry,
};
use ant_proto::v1 as pb;
use ant_proto::v1::ant_client::AntClient;
use ant_server::acl::{self, Access, Acl};
use ant_server::ops::changeset::{self, ChangesetRequest, FileOp, WriteContent};
use ant_server::ops::land::{self, LandOutcome};
use ant_server::ops::{OpError, query};
use ant_server::serve::spawn_local;
use ant_store::Backend;
use ant_store::graph::{build_tree, write_commit};
use bytes::Bytes;
use common::wire::{blob_file, delete_file, write_file};
use tonic::Code;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

const READER: &str = "t-reader";
const WRITER: &str = "t-writer";
const ADMIN: &str = "t-admin";
const SECRET: &str = "TOPSECRET\n";

fn policy_text() -> String {
    format!(
        r#"
        [[principal]]
        name = "reader"
        token_sha256 = "{}"
        groups = ["agents"]

        [[principal]]
        name = "writer"
        token_sha256 = "{}"
        groups = ["agents", "security"]

        [[principal]]
        name = "admin"
        token_sha256 = "{}"
        admin = true

        [[path]]
        prefix = ""
        read = ["*"]
        write = ["group:agents"]

        [[path]]
        prefix = "secrets"
        read = ["group:security"]
        write = ["group:security"]

        [[ref]]
        pattern = "main"
        update = []
        land = ["group:agents"]

        [[ref]]
        pattern = "ws/*"
        update = ["*"]
        land = ["*"]
        "#,
        hex(hash_token(READER)),
        hex(hash_token(WRITER)),
        hex(hash_token(ADMIN)),
    )
}

fn hex(b: [u8; 32]) -> String {
    ObjectId(b).to_hex()
}

fn acl() -> Acl {
    Acl::new(Policy::parse(&policy_text()).unwrap())
}

fn access(token: &str) -> Access {
    acl().authenticate(Some(token)).unwrap().unwrap()
}

fn p(s: &str) -> RepoPath {
    RepoPath::new(s).unwrap()
}

fn sig() -> Signature {
    Signature { name: "a".into(), email: "a@x".into(), time: 1_700_000_000, tz_offset_minutes: 0 }
}

fn blob_id(content: &str) -> ObjectId {
    EncodedObject::new(ObjectKind::Blob, content.as_bytes()).id()
}

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Auth(Option<MetadataValue<Ascii>>);

impl Interceptor for Auth {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(v) = &self.0 {
            req.metadata_mut().insert("authorization", v.clone());
        }
        Ok(req)
    }
}

type Client = AntClient<InterceptedService<Channel, Auth>>;

struct Server {
    backend: Backend,
    channel: Channel,
    http_base: String,
    http: reqwest::Client,
}

impl Server {
    async fn start(acl: Acl) -> Self {
        Self::start_on(Backend::in_memory(), acl).await
    }

    async fn start_on(backend: Backend, acl: Acl) -> Self {
        let server = spawn_local(backend.clone(), acl).await.unwrap();
        let channel = Channel::from_shared(server.grpc_url()).unwrap().connect().await.unwrap();
        Self { backend, channel, http_base: server.http_url(), http: reqwest::Client::new() }
    }

    fn client(&self, token: Option<&str>) -> Client {
        let auth = Auth(token.map(|t| format!("Bearer {t}").parse().unwrap()));
        AntClient::with_interceptor(self.channel.clone(), auth)
    }

    async fn http_get(&self, id: &ObjectId, token: Option<&str>) -> reqwest::Response {
        let mut req = self.http.get(format!("{}/o/{}", self.http_base, id.to_hex()));
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        req.send().await.unwrap()
    }
}

fn pb_sig() -> pb::Signature {
    pb::Signature {
        name: "a".into(),
        email: "a@x".into(),
        time: 1_700_000_000,
        tz_offset_minutes: 0,
    }
}

async fn changeset(
    c: &mut Client,
    base: Option<ObjectId>,
    ops: Vec<pb::FileOp>,
) -> Result<ObjectId, tonic::Status> {
    let r = c
        .create_changeset(pb::CreateChangesetRequest {
            base: base.map(|b| b.0.to_vec()),
            ops,
            author: Some(pb_sig()),
            committer: None,
            message: "m\n".into(),
        })
        .await?;
    Ok(ObjectId::from_bytes(&r.into_inner().commit).unwrap())
}

async fn land_rpc(
    c: &mut Client,
    r: &str,
    commit: ObjectId,
) -> Result<pb::land_response::Outcome, tonic::Status> {
    let resp = c.land(pb::LandRequest { ref_name: r.into(), commit: commit.0.to_vec() }).await?;
    Ok(resp.into_inner().outcome.unwrap())
}

async fn landed_tip(c: &mut Client, r: &str, commit: ObjectId) -> ObjectId {
    match land_rpc(c, r, commit).await.unwrap() {
        pb::land_response::Outcome::Landed(l) => ObjectId::from_bytes(&l.new_tip).unwrap(),
        other => panic!("{other:?}"),
    }
}

async fn get_objects(
    c: &mut Client,
    ids: &[ObjectId],
) -> Result<Vec<pb::ObjectResult>, tonic::Status> {
    let mut s = c
        .get_objects(pb::GetObjectsRequest { ids: ids.iter().map(|i| i.0.to_vec()).collect() })
        .await?
        .into_inner();
    let mut out = Vec::new();
    while let Some(r) = s.message().await? {
        out.push(r);
    }
    Ok(out)
}

/// `found` flags from GetObjects, checking that unfound results look
/// exactly alike (no data).
async fn found(c: &mut Client, ids: &[ObjectId]) -> Vec<bool> {
    get_objects(c, ids)
        .await
        .unwrap()
        .into_iter()
        .map(|r| {
            if !r.found {
                assert!(r.data.is_empty());
            }
            r.found
        })
        .collect()
}

async fn has(c: &mut Client, ids: &[ObjectId]) -> Vec<bool> {
    c.has_objects(pb::HasObjectsRequest { ids: ids.iter().map(|i| i.0.to_vec()).collect() })
        .await
        .unwrap()
        .into_inner()
        .present
}

async fn read_path(
    c: &mut Client,
    rev: &str,
    path: &str,
) -> Result<pb::ReadPathResponse, tonic::Status> {
    Ok(c.read_path(pb::ReadPathRequest {
        rev: rev.into(),
        path: path.into(),
        include_content: true,
    })
    .await?
    .into_inner())
}

async fn path_id(c: &mut Client, rev: &str, path: &str) -> ObjectId {
    let r = read_path(c, rev, path).await.unwrap();
    assert!(r.found, "{path} not found");
    ObjectId::from_bytes(&r.id).unwrap()
}

async fn put_blob(c: &mut Client, content: &str) -> Result<ObjectId, tonic::Status> {
    let o = EncodedObject::new(ObjectKind::Blob, content.as_bytes());
    c.put_objects(pb::PutObjectsRequest { data: vec![o.as_bytes().to_vec()] }).await?;
    Ok(o.id())
}

async fn log_paths(c: &mut Client, rev: &str, path: Option<&str>) -> Vec<ObjectId> {
    c.log(pb::LogRequest { rev: rev.into(), path: path.map(Into::into), limit: 0 })
        .await
        .unwrap()
        .into_inner()
        .commits
        .into_iter()
        .map(|e| ObjectId::from_bytes(&e.id).unwrap())
        .collect()
}

async fn diff_paths(c: &mut Client, from: &str, to: &str) -> Vec<String> {
    c.diff(pb::DiffRequest { from_rev: from.into(), to_rev: to.into(), ..Default::default() })
        .await
        .unwrap()
        .into_inner()
        .changes
        .into_iter()
        .map(|c| c.path)
        .collect()
}

/// A server with `main` = [src/a.rs, README, secrets/key, secrets/dir/x]
/// landed by the writer. Returns (server, main tip).
async fn seeded() -> (Server, ObjectId) {
    let s = Server::start(acl()).await;
    let mut w = s.client(Some(WRITER));
    let c = changeset(
        &mut w,
        None,
        vec![
            write_file("src/a.rs", "pub fn a() {}\n"),
            write_file("README", "readme\n"),
            write_file("secrets/key", SECRET),
            write_file("secrets/dir/x", "nested secret\n"),
        ],
    )
    .await
    .unwrap();
    let tip = landed_tip(&mut w, "main", c).await;
    (s, tip)
}

// ---------------------------------------------------------------------------
// authentication
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unauthenticated_and_unknown_tokens() {
    let (s, tip) = seeded().await;
    for token in [None, Some("bogus")] {
        let mut c = s.client(token);
        let e = c.get_ref(pb::GetRefRequest { name: "main".into() }).await.unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated, "{token:?}");
        let e = c.list_refs(pb::ListRefsRequest { prefix: String::new() }).await.unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = get_objects(&mut c, &[tip]).await.unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e =
            c.has_objects(pb::HasObjectsRequest { ids: vec![tip.0.to_vec()] }).await.unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = put_blob(&mut c, "x").await.unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = c.resolve_rev(pb::ResolveRevRequest { rev: "main".into() }).await.unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = read_path(&mut c, "main", "README").await.unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = changeset(&mut c, Some(tip), vec![write_file("f", "x")]).await.unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = land_rpc(&mut c, "main", tip).await.unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = c
            .update_ref(pb::UpdateRefRequest {
                name: "ws/x".into(),
                expected: None,
                new_target: Some(tip.0.to_vec()),
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e =
            c.log(pb::LogRequest { rev: "main".into(), path: None, limit: 0 }).await.unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = c
            .diff(pb::DiffRequest {
                from_rev: String::new(),
                to_rev: "main".into(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = c
            .blame(pb::BlameRequest { rev: "main".into(), path: "README".into() })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = c
            .merge_base(pb::MergeBaseRequest { a: "main".into(), b: "main".into() })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = c
            .is_ancestor(pb::IsAncestorRequest {
                ancestor: "main".into(),
                descendant: "main".into(),
                first_parent_only: false,
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = c
            .log_range(pb::LogRangeRequest { to: "main".into(), ..Default::default() })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        let e = c
            .landed_as(pb::LandedAsRequest { commit: tip.to_hex(), ref_name: String::new() })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);

        let r = s.http_get(&tip, token).await;
        assert_eq!(r.status(), 401);
        assert!(r.headers().get("www-authenticate").is_some());
    }
    // Basic auth with the token as password also works on /o.
    let r = s
        .http
        .get(format!("{}/o/{}", s.http_base, tip.to_hex()))
        .basic_auth("anyone", Some(READER))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    // Health checks stay open.
    let r = s.http.get(format!("{}/healthz", s.http_base)).send().await.unwrap();
    assert_eq!(r.status(), 200);
    // A known token works.
    let mut c = s.client(Some(READER));
    c.get_ref(pb::GetRefRequest { name: "main".into() }).await.unwrap();
}

// ---------------------------------------------------------------------------
// reads by id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn restricted_objects_look_missing_by_id() {
    let (s, _) = seeded().await;
    let mut r = s.client(Some(READER));
    let mut w = s.client(Some(WRITER));
    let mut a = s.client(Some(ADMIN));
    let secret = path_id(&mut w, "main", "secrets/key").await;
    let nested = path_id(&mut w, "main", "secrets/dir/x").await;
    let secrets_tree = path_id(&mut w, "main", "secrets").await;
    let secrets_dir = path_id(&mut w, "main", "secrets/dir").await;
    let public = path_id(&mut r, "main", "src/a.rs").await;
    let root_tree = path_id(&mut r, "main", "").await;
    let missing = blob_id("never stored");
    let ids = [secret, nested, secrets_tree, secrets_dir, public, root_tree, missing];

    assert_eq!(found(&mut r, &ids).await, [false, false, false, false, true, true, false]);
    assert_eq!(has(&mut r, &ids).await, [false, false, false, false, true, true, false]);
    assert_eq!(found(&mut w, &ids).await, [true, true, true, true, true, true, false]);
    assert_eq!(found(&mut a, &ids).await, [true, true, true, true, true, true, false]);
    assert_eq!(has(&mut a, &ids).await, [true, true, true, true, true, true, false]);

    // v1 limitation: the readable root tree still names the restricted
    // subtree and its id.
    let root = read_path(&mut r, "main", "").await.unwrap();
    assert!(root.entries.iter().any(|e| e.name == "secrets" && e.id == secrets_tree.0.to_vec()));

    // HTTP: same 404 as a missing object, never cacheable.
    let denied = s.http_get(&secret, Some(READER)).await;
    let absent = s.http_get(&missing, Some(READER)).await;
    assert_eq!(denied.status(), 404);
    assert_eq!(absent.status(), 404);
    assert_eq!(denied.headers().get("cache-control"), absent.headers().get("cache-control"));
    assert_eq!(denied.text().await.unwrap(), absent.text().await.unwrap());
    // Even with a matching If-None-Match.
    let r304 = s
        .http
        .get(format!("{}/o/{}", s.http_base, secret.to_hex()))
        .bearer_auth(READER)
        .header("if-none-match", format!("\"{}\"", secret.to_hex()))
        .send()
        .await
        .unwrap();
    assert_eq!(r304.status(), 404);

    let ok = s.http_get(&secret, Some(WRITER)).await;
    assert_eq!(ok.status(), 200);
    let cc = ok.headers().get("cache-control").unwrap().to_str().unwrap().to_string();
    assert!(cc.starts_with("private"), "{cc}");
    assert_eq!(
        ok.bytes().await.unwrap().as_ref(),
        EncodedObject::new(ObjectKind::Blob, SECRET.as_bytes()).as_bytes().as_ref()
    );
    let pubr = s.http_get(&public, Some(READER)).await;
    assert_eq!(pubr.status(), 200);
    let cc = pubr.headers().get("cache-control").unwrap().to_str().unwrap().to_string();
    assert!(cc.starts_with("public"), "{cc}");
}

#[tokio::test]
async fn same_bytes_at_public_path_become_readable() {
    let (s, tip) = seeded().await;
    let mut r = s.client(Some(READER));
    let mut w = s.client(Some(WRITER));
    let secret = blob_id(SECRET);
    assert_eq!(found(&mut r, &[secret]).await, [false]);
    // Someone who can read it copies it to a public path by id.
    let c = changeset(&mut w, Some(tip), vec![blob_file("src/leaked", secret)]).await.unwrap();
    landed_tip(&mut w, "main", c).await;
    assert_eq!(found(&mut r, &[secret]).await, [true]);

    // Same for inline bytes: a reader who already knows content makes it public.
    let other = "another secret\n";
    let c = changeset(&mut w, Some(tip), vec![write_file("secrets/other", other)]).await.unwrap();
    assert_eq!(found(&mut r, &[blob_id(other)]).await, [false]);
    changeset(&mut r, Some(c), vec![write_file("src/guess", other)]).await.unwrap();
    assert_eq!(found(&mut r, &[blob_id(other)]).await, [true]);
}

// ---------------------------------------------------------------------------
// path queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn read_path_log_diff_blame_filtering() {
    let (s, c1) = seeded().await;
    let mut r = s.client(Some(READER));
    let mut w = s.client(Some(WRITER));
    // c2 touches only secrets, c3 only src.
    let c2 =
        changeset(&mut w, Some(c1), vec![write_file("secrets/key", "rotated\n")]).await.unwrap();
    let c2 = landed_tip(&mut w, "main", c2).await;
    let c3 =
        changeset(&mut r, Some(c2), vec![write_file("src/a.rs", "pub fn b() {}\n")]).await.unwrap();
    let c3 = landed_tip(&mut r, "main", c3).await;

    // ReadPath: denied == missing.
    for path in ["secrets/key", "secrets", "secrets/dir", "secrets/dir/x"] {
        let res = read_path(&mut r, "main", path).await.unwrap();
        assert_eq!(res, pb::ReadPathResponse::default(), "{path}");
        assert!(read_path(&mut w, "main", path).await.unwrap().found, "{path}");
    }
    assert_eq!(
        read_path(&mut r, "main", "secrets/nope").await.unwrap(),
        pb::ReadPathResponse::default()
    );
    let f = read_path(&mut r, "main", "src/a.rs").await.unwrap();
    assert_eq!(f.content, b"pub fn b() {}\n");
    // Unknown revs still error the same way for everyone.
    let e = read_path(&mut r, "nope", "secrets/key").await.unwrap_err();
    assert_eq!(e.code(), Code::NotFound);

    // Log: whole history is visible, but path filters only count readable changes.
    assert_eq!(log_paths(&mut r, "main", None).await, [c3, c2, c1]);
    assert_eq!(log_paths(&mut r, "main", Some("secrets")).await, Vec::<ObjectId>::new());
    assert_eq!(log_paths(&mut r, "main", Some("secrets/key")).await, Vec::<ObjectId>::new());
    assert_eq!(log_paths(&mut r, "main", Some("")).await, [c3, c1]);
    assert_eq!(log_paths(&mut r, "main", Some("src")).await, [c3, c1]);
    assert_eq!(log_paths(&mut w, "main", Some("secrets")).await, [c2, c1]);
    assert_eq!(log_paths(&mut w, "main", Some("")).await, [c3, c2, c1]);
    // Same through the history index once it caught up.
    ant_server::ops::history::catch_up(&s.backend, "main").await.unwrap();
    assert_eq!(log_paths(&mut r, "main", Some("secrets")).await, Vec::<ObjectId>::new());
    assert_eq!(log_paths(&mut w, "main", Some("secrets")).await, [c2, c1]);
    assert_eq!(log_paths(&mut r, "main", Some("")).await, [c3, c1]);

    // Diff drops hidden paths.
    assert_eq!(diff_paths(&mut r, "", "main").await, ["README", "src/a.rs"]);
    assert_eq!(diff_paths(&mut r, "main~2", "main~1").await, Vec::<String>::new());
    assert_eq!(diff_paths(&mut w, "main~2", "main~1").await, ["secrets/key"]);
    assert_eq!(diff_paths(&mut w, "", "main").await.len(), 4);

    // Blame: unreadable == missing.
    let e = r
        .blame(pb::BlameRequest { rev: "main".into(), path: "secrets/key".into() })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::NotFound);
    let e2 = r
        .blame(pb::BlameRequest { rev: "main".into(), path: "secrets/nope".into() })
        .await
        .unwrap_err();
    assert_eq!(e2.code(), Code::NotFound);
    let lines =
        w.blame(pb::BlameRequest { rev: "main".into(), path: "secrets/key".into() }).await.unwrap();
    assert_eq!(lines.into_inner().lines[0].commit, c2.0.to_vec());
    r.blame(pb::BlameRequest { rev: "main".into(), path: "src/a.rs".into() }).await.unwrap();
}

async fn range_ids(c: &mut Client, from: &str, to: &str, path: Option<&str>) -> Vec<ObjectId> {
    let (mut out, mut token) = (Vec::new(), String::new());
    loop {
        let r = c
            .log_range(pb::LogRangeRequest {
                from_exclusive: from.into(),
                to: to.into(),
                path: path.map(Into::into),
                limit: 1,
                page_token: token,
            })
            .await
            .unwrap()
            .into_inner();
        out.extend(r.commits.iter().map(|e| ObjectId::from_bytes(&e.id).unwrap()));
        if r.next_page_token.is_empty() {
            return out;
        }
        token = r.next_page_token;
    }
}

/// Paths and patch text of a `Diff` with the given options.
async fn diff_patch(
    c: &mut Client,
    from: &str,
    to: &str,
    prefixes: &[&str],
    merge_base: bool,
) -> (Vec<String>, String) {
    let r = c
        .diff(pb::DiffRequest {
            from_rev: from.into(),
            to_rev: to.into(),
            path_prefixes: prefixes.iter().map(|p| p.to_string()).collect(),
            merge_base,
            patch: true,
            context_lines: None,
        })
        .await
        .unwrap()
        .into_inner();
    let paths = r.changes.iter().map(|c| c.path.clone()).collect();
    let text = r
        .changes
        .iter()
        .flat_map(|c| c.patch.iter().flat_map(|p| &p.hunks).flat_map(|h| &h.lines))
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    (paths, text)
}

/// The graph queries see the whole history (commits are not path
/// restricted, as with `Log`); path-filtered ranges count only readable
/// changes; diffs with prefixes, merge bases and patches never list or
/// read restricted files; unknown commits are missing for everyone.
#[tokio::test]
async fn graph_queries_ranges_and_patches_filtering() {
    let (s, c1) = seeded().await;
    let mut r = s.client(Some(READER));
    let mut w = s.client(Some(WRITER));
    let c2 =
        changeset(&mut w, Some(c1), vec![write_file("secrets/key", "rotated\n")]).await.unwrap();
    let c2 = landed_tip(&mut w, "main", c2).await;
    let c3 =
        changeset(&mut r, Some(c2), vec![write_file("src/a.rs", "pub fn b() {}\n")]).await.unwrap();
    let c3 = landed_tip(&mut r, "main", c3).await;
    // A reader's branch off c1, touching src/ only.
    let f = changeset(&mut r, Some(c1), vec![write_file("src/f.rs", "f\n")]).await.unwrap();
    let fh = f.to_hex();

    for c in [&mut r, &mut w] {
        let mb = c
            .merge_base(pb::MergeBaseRequest { a: "main".into(), b: fh.clone() })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(mb.commit, Some(c1.0.to_vec()));
        let anc = c
            .is_ancestor(pb::IsAncestorRequest {
                ancestor: c2.to_hex(),
                descendant: "main".into(),
                first_parent_only: true,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(anc.is_ancestor);
        let landed = c
            .landed_as(pb::LandedAsRequest { commit: c3.to_hex(), ref_name: "main".into() })
            .await
            .unwrap()
            .into_inner();
        assert!(landed.landed);
        assert_eq!(landed.landed_commit, c3.0.to_vec());
        // Unknown commits: not found, whoever asks.
        let ghost = ObjectId([7; 32]).to_hex();
        let e = c
            .merge_base(pb::MergeBaseRequest { a: ghost.clone(), b: "main".into() })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::NotFound);
        let e = c
            .is_ancestor(pb::IsAncestorRequest {
                ancestor: ghost.clone(),
                descendant: "main".into(),
                first_parent_only: false,
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::NotFound);
        let e = c
            .landed_as(pb::LandedAsRequest { commit: ghost.clone(), ref_name: String::new() })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::NotFound);
        let e = c
            .log_range(pb::LogRangeRequest {
                from_exclusive: ghost.clone(),
                to: "main".into(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::NotFound);
    }

    // LogRange: the whole range is visible; a path counts readable changes
    // only, by scan and through the index.
    let c1h = c1.to_hex();
    for indexed in [false, true] {
        if indexed {
            ant_server::ops::history::catch_up(&s.backend, "main").await.unwrap();
        }
        assert_eq!(range_ids(&mut r, &c1h, "main", None).await, [c3, c2]);
        assert_eq!(range_ids(&mut r, &c1h, "main", Some("secrets")).await, Vec::<ObjectId>::new());
        assert_eq!(range_ids(&mut r, &c1h, "main", Some("secrets/key")).await, []);
        assert_eq!(range_ids(&mut r, &c1h, "main", Some("")).await, [c3]);
        assert_eq!(range_ids(&mut w, &c1h, "main", Some("secrets")).await, [c2]);
        assert_eq!(range_ids(&mut w, &c1h, "main", Some("")).await, [c3, c2]);
        assert_eq!(range_ids(&mut r, "", "main", Some("secrets")).await, []);
        assert_eq!(range_ids(&mut w, "", "main", Some("secrets")).await, [c2, c1]);
    }

    // Diff with patches: restricted files are neither listed nor diffed.
    let (paths, text) = diff_patch(&mut r, "", "main", &[], false).await;
    assert_eq!(paths, ["README", "src/a.rs"]);
    assert!(!text.contains("TOPSECRET") && !text.contains("rotated"), "{text}");
    let (paths, text) = diff_patch(&mut w, "", "main", &[], false).await;
    assert_eq!(paths.len(), 4);
    assert!(text.contains("+rotated"), "{text}");
    // Prefixes: the restricted subtree is empty for the reader.
    assert_eq!(diff_patch(&mut r, "", "main", &["secrets"], false).await.0, Vec::<String>::new());
    assert_eq!(
        diff_patch(&mut r, "", "main", &["secrets/key"], false).await.0,
        Vec::<String>::new()
    );
    assert_eq!(
        diff_patch(&mut w, "", "main", &["secrets", "src"], false).await.0,
        ["secrets/dir/x", "secrets/key", "src/a.rs"]
    );
    // Merge-base diffs: main...f is f's own change; f...main hides secrets.
    assert_eq!(diff_patch(&mut r, "main", &fh, &[], true).await.0, ["src/f.rs"]);
    let (paths, text) = diff_patch(&mut r, &fh, "main", &[], true).await;
    assert_eq!(paths, ["src/a.rs"]);
    assert!(!text.contains("rotated"), "{text}");
    assert_eq!(diff_patch(&mut w, &fh, "main", &[], true).await.0, ["secrets/key", "src/a.rs"]);
}

/// The ops-level API applies the same filters (`log_range_as`, `diff_with`).
#[tokio::test]
async fn ops_level_ranges_and_patches() {
    let (s, c1) = seeded().await;
    let reader = access(READER);
    let writer = access(WRITER);
    let b = &s.backend;
    let secrets = p("secrets");
    let page = ant_server::ops::ancestry::log_range_as(
        b,
        Some(&reader),
        None,
        "main",
        Some(&secrets),
        0,
        None,
    )
    .await
    .unwrap();
    assert!(page.entries.is_empty());
    let page = ant_server::ops::ancestry::log_range_as(
        b,
        Some(&writer),
        None,
        "main",
        Some(&secrets),
        0,
        None,
    )
    .await
    .unwrap();
    assert_eq!(page.entries.iter().map(|e| e.id).collect::<Vec<_>>(), [c1]);
    let opts =
        query::DiffOptions { prefixes: vec![secrets.clone()], merge_base: false, patch: Some(3) };
    let d = query::diff_with(b, Some(&reader), None, "main", &opts).await.unwrap();
    assert!(d.changes.is_empty() && d.patches.unwrap().is_empty());
    let d = query::diff_with(b, Some(&writer), None, "main", &opts).await.unwrap();
    assert_eq!(d.changes.len(), 2);
    assert!(d.patches.unwrap().iter().flatten().any(|p| p.hunks[0].lines[0] == "+TOPSECRET"));
}

// ---------------------------------------------------------------------------
// writes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_changeset_path_and_blob_checks() {
    let (s, tip) = seeded().await;
    let mut r = s.client(Some(READER));
    let mut w = s.client(Some(WRITER));
    let mut a = s.client(Some(ADMIN));
    for ops in [
        vec![write_file("secrets/new", "x")],
        vec![delete_file("secrets/key")],
        vec![write_file("src/ok", "x"), write_file("secrets/dir/y", "x")],
        vec![write_file("secrets", "file over dir")],
    ] {
        let e = changeset(&mut r, Some(tip), ops.clone()).await.unwrap_err();
        assert_eq!(e.code(), Code::PermissionDenied, "{ops:?}");
    }
    changeset(&mut r, Some(tip), vec![write_file("src/ok", "x")]).await.unwrap();
    changeset(&mut w, Some(tip), vec![write_file("secrets/new", "x")]).await.unwrap();
    changeset(&mut a, Some(tip), vec![write_file("secrets/admin", "x")]).await.unwrap();
    // Base containing hidden content is fine.
    changeset(&mut r, Some(tip), vec![delete_file("README")]).await.unwrap();

    // Read-by-copy: referencing a restricted blob by id is reported exactly
    // like referencing a blob that does not exist.
    let secret = blob_id(SECRET);
    let e1 = changeset(&mut r, Some(tip), vec![blob_file("src/copy", secret)]).await.unwrap_err();
    let e2 = changeset(&mut r, Some(tip), vec![blob_file("src/copy", blob_id("nope"))])
        .await
        .unwrap_err();
    assert_eq!(e1.code(), Code::NotFound);
    assert_eq!(e2.code(), Code::NotFound);
    // Tree ids are no way around it either.
    let tree = path_id(&mut w, "main", "secrets").await;
    let e = changeset(&mut r, Some(tip), vec![blob_file("src/copy", tree)]).await.unwrap_err();
    assert_eq!(e.code(), Code::NotFound);
    assert_eq!(found(&mut r, &[secret]).await, [false]);
}

#[tokio::test]
async fn uploads_stay_hidden_until_placed() {
    let (s, tip) = seeded().await;
    let mut r = s.client(Some(READER));
    let mut w = s.client(Some(WRITER));
    // Writer uploads a new secret first (the `ant submit` flow), then commits
    // it under secrets/. It must never become public.
    let up = put_blob(&mut w, "uploaded secret\n").await.unwrap();
    assert_eq!(found(&mut r, &[up]).await, [false]);
    assert_eq!(has(&mut r, &[up]).await, [false]);
    // Nobody else can reference the pending upload by id.
    let e = changeset(&mut r, Some(tip), vec![blob_file("src/steal", up)]).await.unwrap_err();
    assert_eq!(e.code(), Code::NotFound);
    let c = changeset(&mut w, Some(tip), vec![blob_file("secrets/up", up)]).await.unwrap();
    landed_tip(&mut w, "main", c).await;
    assert_eq!(found(&mut r, &[up]).await, [false]);
    assert_eq!(found(&mut w, &[up]).await, [true]);
    let e = changeset(&mut r, Some(tip), vec![blob_file("src/steal", up)]).await.unwrap_err();
    assert_eq!(e.code(), Code::NotFound);

    // A reader's own upload placed at a public path becomes public.
    let mine = put_blob(&mut r, "public upload\n").await.unwrap();
    assert_eq!(found(&mut r, &[mine]).await, [false]);
    changeset(&mut r, Some(tip), vec![blob_file("src/mine", mine)]).await.unwrap();
    assert_eq!(found(&mut r, &[mine]).await, [true]);

    // Re-uploading a secret's bytes proves knowledge of them and allows
    // referencing (but the secret stays hidden until placed publicly).
    let known = put_blob(&mut r, SECRET).await.unwrap();
    assert_eq!(found(&mut r, &[known]).await, [false]);
    changeset(&mut r, Some(tip), vec![blob_file("src/known", known)]).await.unwrap();
    assert_eq!(found(&mut r, &[known]).await, [true]);

    // Re-uploading an existing public blob keeps it public.
    let readme = blob_id("readme\n");
    put_blob(&mut r, "readme\n").await.unwrap();
    assert_eq!(found(&mut r, &[readme]).await, [true]);
}

#[tokio::test]
async fn put_objects_only_blobs_unless_admin() {
    let (s, tip) = seeded().await;
    let mut r = s.client(Some(READER));
    let mut a = s.client(Some(ADMIN));
    let tree = Object::Tree(
        Tree::new(vec![TreeEntry { mode: EntryMode::File, name: "f".into(), id: blob_id(SECRET) }])
            .unwrap(),
    )
    .encode();
    let commit = Object::Commit(Commit {
        tree: tree.id(),
        parents: vec![tip],
        author: sig(),
        committer: sig(),
        extra_headers: vec![],
        message: "smuggled\n".into(),
    })
    .encode();
    for o in [&tree, &commit] {
        let blob = EncodedObject::new(ObjectKind::Blob, b"fine");
        let e = r
            .put_objects(pb::PutObjectsRequest {
                data: vec![blob.as_bytes().to_vec(), o.as_bytes().to_vec()],
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::PermissionDenied);
        // Nothing of the batch was stored.
        assert!(!s.backend.objects.has_many(&[blob.id()]).await.unwrap()[0]);
    }
    a.put_objects(pb::PutObjectsRequest {
        data: vec![tree.as_bytes().to_vec(), commit.as_bytes().to_vec()],
    })
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------
// refs and land
// ---------------------------------------------------------------------------

async fn update_ref(
    c: &mut Client,
    name: &str,
    expected: Option<ObjectId>,
    new: Option<ObjectId>,
) -> Result<bool, tonic::Status> {
    Ok(c.update_ref(pb::UpdateRefRequest {
        name: name.into(),
        expected: expected.map(|e| e.0.to_vec()),
        new_target: new.map(|n| n.0.to_vec()),
    })
    .await?
    .into_inner()
    .updated)
}

#[tokio::test]
async fn update_ref_checks() {
    let (s, tip) = seeded().await;
    let mut r = s.client(Some(READER));
    let mut w = s.client(Some(WRITER));
    let mut a = s.client(Some(ADMIN));
    let src = changeset(&mut r, Some(tip), vec![write_file("src/x", "x")]).await.unwrap();
    let sec =
        changeset(&mut w, Some(tip), vec![write_file("secrets/key", "changed\n")]).await.unwrap();

    // No update rule for main except admins.
    for c in [&mut r, &mut w] {
        let e = update_ref(c, "main", Some(tip), Some(src)).await.unwrap_err();
        assert_eq!(e.code(), Code::PermissionDenied);
    }
    let e = update_ref(&mut r, "release", None, Some(tip)).await.unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied);
    // ws/* is open; creating needs no path check.
    assert!(update_ref(&mut r, "ws/r/x", None, Some(tip)).await.unwrap());
    // Moving to a commit that changes paths the caller cannot write is denied.
    let e = update_ref(&mut r, "ws/r/x", Some(tip), Some(sec)).await.unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied);
    assert_eq!(s.backend.refs.get_ref("ws/r/x").await.unwrap(), Some(tip));
    // ... in either direction (reverting someone's secret change).
    assert!(update_ref(&mut w, "ws/w/y", None, Some(sec)).await.unwrap());
    let e = update_ref(&mut r, "ws/w/y", Some(sec), Some(tip)).await.unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied);
    assert!(update_ref(&mut w, "ws/w/y", Some(sec), Some(tip)).await.unwrap());
    // Writable changes are fine; a CAS mismatch is still just "not updated".
    assert!(update_ref(&mut r, "ws/r/x", Some(tip), Some(src)).await.unwrap());
    assert!(!update_ref(&mut r, "ws/r/x", Some(tip), Some(src)).await.unwrap());
    // Targets must be recorded commits.
    let e = update_ref(&mut r, "ws/r/x", Some(src), Some(blob_id(SECRET))).await.unwrap_err();
    assert_eq!(e.code(), Code::NotFound);
    // Delete.
    assert!(update_ref(&mut r, "ws/r/x", Some(src), None).await.unwrap());
    // Admin can do anything.
    assert!(update_ref(&mut a, "main", Some(tip), Some(sec)).await.unwrap());
}

#[tokio::test]
async fn land_checks() {
    let (s, tip) = seeded().await;
    let mut r = s.client(Some(READER));
    let mut w = s.client(Some(WRITER));
    let mine = changeset(&mut r, Some(tip), vec![write_file("src/r", "r")]).await.unwrap();
    let e = land_rpc(&mut r, "release", mine).await.unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied);

    // A writer's unlanded secret change cannot be landed by the reader,
    // neither alone nor underneath the reader's own commit.
    let theirs =
        changeset(&mut w, Some(tip), vec![write_file("secrets/key", "rotated\n")]).await.unwrap();
    let e = land_rpc(&mut r, "main", theirs).await.unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied);
    let stacked = changeset(&mut r, Some(theirs), vec![write_file("src/s", "s")]).await.unwrap();
    let e = land_rpc(&mut r, "main", stacked).await.unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied);
    assert_eq!(s.backend.refs.get_ref("main").await.unwrap(), Some(tip));

    // Fast-forward and rebase of the reader's own work are fine.
    let t1 = landed_tip(&mut r, "main", mine).await;
    assert_eq!(t1, mine);
    let other = changeset(&mut r, Some(tip), vec![write_file("src/o", "o")]).await.unwrap();
    let t2 = landed_tip(&mut r, "main", other).await;
    assert_ne!(t2, other);
    // Once the writer landed theirs, the reader's stacked commit only
    // replays its own change.
    let t3 = landed_tip(&mut w, "main", theirs).await;
    let t4 = landed_tip(&mut r, "main", stacked).await;
    assert_ne!(t3, t4);
    // Re-landing an already landed commit is a no-op for anyone allowed to land.
    landed_tip(&mut r, "main", theirs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn denied_land_does_not_affect_its_batch() {
    let (s, tip) = seeded().await;
    let mut r = s.client(Some(READER));
    let mut w = s.client(Some(WRITER));
    let bad = changeset(&mut w, Some(tip), vec![write_file("secrets/bad", "x")]).await.unwrap();
    let mut good = Vec::new();
    for i in 0..16 {
        good.push(
            changeset(&mut r, Some(tip), vec![write_file(&format!("src/g{i}"), "g")])
                .await
                .unwrap(),
        );
    }
    let mut tasks = Vec::new();
    for (i, c) in good.iter().copied().enumerate() {
        let mut client = s.client(Some(READER));
        tasks.push(tokio::spawn(async move { (true, land_rpc(&mut client, "main", c).await) }));
        if i % 4 == 0 {
            let mut client = s.client(Some(READER));
            tasks.push(tokio::spawn(
                async move { (false, land_rpc(&mut client, "main", bad).await) },
            ));
        }
    }
    for t in tasks {
        let (ok, res) = t.await.unwrap();
        if ok {
            assert!(matches!(res, Ok(pb::land_response::Outcome::Landed(_))), "{res:?}");
        } else {
            assert_eq!(res.unwrap_err().code(), Code::PermissionDenied);
        }
    }
    let tip = s.backend.refs.get_ref("main").await.unwrap().unwrap();
    for i in 0..16 {
        assert!(read_path(&mut r, &tip.to_hex(), &format!("src/g{i}")).await.unwrap().found);
    }
    assert!(!read_path(&mut w, &tip.to_hex(), "secrets/bad").await.unwrap().found);
}

/// Merge commits cannot be created through the API by non-admins, but may
/// exist (imports): a fast-forward through them is judged by the net diff.
#[tokio::test]
async fn land_fast_forward_through_merges_uses_net_diff() {
    let b = Backend::in_memory();
    let reader = access(READER);
    let writer = access(WRITER);
    let base = snapshot(&b, None, &[("src/a", "a"), ("secrets/k", "k1")]).await;
    land::land_as(&b, Some(&writer), "main", base).await.unwrap();
    let sec = snapshot(&b, Some(vec![base]), &[("src/a", "a"), ("secrets/k", "k2")]).await;
    land::land_as(&b, Some(&writer), "main", sec).await.unwrap();
    let mine = snapshot(&b, Some(vec![base]), &[("src/a", "a2"), ("secrets/k", "k1")]).await;

    // Merge of main into my branch: first-parent changeset carries the
    // writer's change, but the net diff from the tip is only src/a.
    let merge = snapshot(&b, Some(vec![mine, sec]), &[("src/a", "a2"), ("secrets/k", "k2")]).await;
    // Revert disguised as a merge: no first-parent change at all, yet it
    // would undo secrets/k on main.
    let revert = snapshot(&b, Some(vec![base, sec]), &[("src/a", "a"), ("secrets/k", "k1")]).await;
    let e = land::land_as(&b, Some(&reader), "main", revert).await.unwrap_err();
    assert!(matches!(e, OpError::PermissionDenied(_)), "{e}");
    let out = land::land_as(&b, Some(&reader), "main", merge).await.unwrap();
    assert_eq!(out, LandOutcome::Landed { new_tip: merge, rebased: false });
    // The admin could have landed the revert.
    let admin = access(ADMIN);
    let revert2 =
        snapshot(&b, Some(vec![base, merge]), &[("src/a", "a2"), ("secrets/k", "k1")]).await;
    land::land_as(&b, Some(&admin), "main", revert2).await.unwrap();
}

async fn snapshot(b: &Backend, parents: Option<Vec<ObjectId>>, files: &[(&str, &str)]) -> ObjectId {
    let files: Vec<(&str, &[u8])> = files.iter().map(|(p, c)| (*p, c.as_bytes())).collect();
    let tree = build_tree(b.objects.as_ref(), &files).await.unwrap();
    write_commit(
        b,
        &Commit {
            tree,
            parents: parents.unwrap_or_default(),
            author: sig(),
            committer: sig(),
            extra_headers: vec![],
            message: "snapshot\n".into(),
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn land_replays_tag_merged_objects() {
    let (s, tip) = seeded().await;
    let mut r = s.client(Some(READER));
    let mut w = s.client(Some(WRITER));
    let text = "l1\nl2\nl3\nl4\nl5\nl6\n";
    let base = changeset(&mut w, Some(tip), vec![write_file("secrets/doc", text)]).await.unwrap();
    let base = landed_tip(&mut w, "main", base).await;
    let x =
        changeset(&mut w, Some(base), vec![write_file("secrets/doc", "L1\nl2\nl3\nl4\nl5\nl6\n")])
            .await
            .unwrap();
    let y =
        changeset(&mut w, Some(base), vec![write_file("secrets/doc", "l1\nl2\nl3\nl4\nl5\nL6\n")])
            .await
            .unwrap();
    landed_tip(&mut w, "main", x).await;
    let merged_tip = landed_tip(&mut w, "main", y).await;
    let merged = read_path(&mut w, &merged_tip.to_hex(), "secrets/doc").await.unwrap();
    assert_eq!(merged.content, b"L1\nl2\nl3\nl4\nl5\nL6\n");
    let merged_id = ObjectId::from_bytes(&merged.id).unwrap();
    // The server-created merge result is as hidden as its inputs.
    assert_eq!(found(&mut r, &[merged_id]).await, [false]);
    assert_eq!(found(&mut w, &[merged_id]).await, [true]);
    let sec_tree = path_id(&mut w, &merged_tip.to_hex(), "secrets").await;
    assert_eq!(found(&mut r, &[sec_tree]).await, [false]);
}

// ---------------------------------------------------------------------------
// git export
// ---------------------------------------------------------------------------

async fn git(dir: &Path, args: &[&str]) -> std::process::Output {
    tokio::time::timeout(
        Duration::from_secs(60),
        tokio::process::Command::new("git")
            .current_dir(dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_ASKPASS", "/bin/false")
            .args(["-c", "protocol.version=2", "-c", "credential.helper="])
            .args(args)
            .output(),
    )
    .await
    .expect("git timed out")
    .unwrap()
}

#[tokio::test]
async fn git_clone_requires_full_read() {
    let (s, _) = seeded().await;
    let tmp = tempfile::tempdir().unwrap();
    let host = s.http_base.trim_start_matches("http://");
    let ok =
        git(tmp.path(), &["clone", &format!("http://writer:{WRITER}@{host}/repo.git"), "w"]).await;
    assert!(ok.status.success(), "{}", String::from_utf8_lossy(&ok.stderr));
    assert_eq!(std::fs::read(tmp.path().join("w/secrets/key")).unwrap(), SECRET.as_bytes());

    let denied =
        git(tmp.path(), &["clone", &format!("http://reader:{READER}@{host}/repo.git"), "r"]).await;
    assert!(!denied.status.success());
    assert!(
        String::from_utf8_lossy(&denied.stderr).contains("403"),
        "{}",
        String::from_utf8_lossy(&denied.stderr)
    );
    assert!(!tmp.path().join("r/secrets").exists());

    let anon = git(tmp.path(), &["clone", &format!("{}/repo.git", s.http_base), "a"]).await;
    assert!(!anon.status.success());

    // Raw protocol: 401 with a Basic challenge, 403 for restricted readers.
    let url = format!("{}/repo.git/info/refs?service=git-upload-pack", s.http_base);
    let r = s.http.get(&url).send().await.unwrap();
    assert_eq!(r.status(), 401);
    assert!(r.headers()["www-authenticate"].to_str().unwrap().starts_with("Basic"));
    let r = s.http.get(&url).basic_auth("x", Some("bogus")).send().await.unwrap();
    assert_eq!(r.status(), 401);
    let r = s.http.get(&url).basic_auth("x", Some(READER)).send().await.unwrap();
    assert_eq!(r.status(), 403);
    let r = s
        .http
        .post(format!("{}/repo.git/git-upload-pack", s.http_base))
        .basic_auth("x", Some(READER))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let r = s.http.get(&url).basic_auth("x", Some(ADMIN)).send().await.unwrap();
    assert_eq!(r.status(), 200);
}

// ---------------------------------------------------------------------------
// open mode
// ---------------------------------------------------------------------------

#[tokio::test]
async fn open_mode_is_unchanged() {
    let s = Server::start(Acl::open()).await;
    let mut c = s.client(None);
    let commit = changeset(&mut c, None, vec![write_file("secrets/key", SECRET)]).await.unwrap();
    landed_tip(&mut c, "main", commit).await;
    assert_eq!(found(&mut c, &[blob_id(SECRET)]).await, [true]);
    assert!(read_path(&mut c, "main", "secrets/key").await.unwrap().found);
    // Any token is ignored, trees/commits may be uploaded, refs moved.
    let mut junk = s.client(Some("whatever"));
    let tree = Object::Tree(Tree::empty()).encode();
    junk.put_objects(pb::PutObjectsRequest { data: vec![tree.as_bytes().to_vec()] }).await.unwrap();
    assert!(update_ref(&mut junk, "main", Some(commit), None).await.unwrap());
    let r = s.http_get(&blob_id(SECRET), None).await;
    assert_eq!(r.status(), 200);
    assert!(r.headers()["cache-control"].to_str().unwrap().starts_with("public"));
    let tmp = tempfile::tempdir().unwrap();
    assert!(update_ref(&mut c, "main", None, Some(commit)).await.unwrap());
    let ok = git(tmp.path(), &["clone", &format!("{}/repo.git", s.http_base), "o"]).await;
    assert!(ok.status.success(), "{}", String::from_utf8_lossy(&ok.stderr));
    // No tags are written in open mode.
    assert!(
        acl::object_tags(&s.backend, &Policy::parse(&policy_text()).unwrap(), &[blob_id(SECRET)])
            .await
            .unwrap()[0]
            .is_empty()
    );
}

// ---------------------------------------------------------------------------
// reindex (imports, policy changes)
// ---------------------------------------------------------------------------

async fn reindex_flow(b: &Backend) {
    let policy = Policy::parse(&policy_text()).unwrap();
    let reader = access(READER);
    // An "import": objects straight into the store, no tags.
    let c1 = snapshot(
        b,
        None,
        &[
            ("src/a", "a"),
            ("src/shared", "shared bytes"),
            ("secrets/k", "imported secret"),
            ("secrets/shared", "shared bytes"),
            ("secrets/deep/d", "deep secret"),
            ("docs/x", "x"),
        ],
    )
    .await;
    let secret = blob_id("imported secret");
    let deep = blob_id("deep secret");
    let shared = blob_id("shared bytes");
    let ids = [secret, deep, shared, blob_id("a")];
    // Before reindexing, imported secrets are (documented) still public.
    assert_eq!(acl::readable(b, &policy, &reader.principal, &ids).await.unwrap(), [true; 4]);
    let st = acl::reindex(b, &policy, c1).await.unwrap();
    assert!(st.tags_written > 0);
    assert_eq!(st.trees_skipped, 0);
    assert_eq!(
        acl::readable(b, &policy, &reader.principal, &ids).await.unwrap(),
        [false, false, true, true]
    );
    let writer = access(WRITER);
    assert_eq!(acl::readable(b, &policy, &writer.principal, &ids).await.unwrap(), [true; 4]);

    // Second run: everything skipped at the root.
    let st = acl::reindex(b, &policy, c1).await.unwrap();
    assert_eq!((st.trees_walked, st.trees_skipped, st.tags_written), (0, 1, 0));

    // A new commit changing only docs/: the secrets subtree is skipped.
    let c2 = snapshot(
        b,
        Some(vec![c1]),
        &[
            ("src/a", "a"),
            ("src/shared", "shared bytes"),
            ("secrets/k", "imported secret"),
            ("secrets/shared", "shared bytes"),
            ("secrets/deep/d", "deep secret"),
            ("docs/x", "x2"),
        ],
    )
    .await;
    let st = acl::reindex(b, &policy, c2).await.unwrap();
    assert_eq!(st.trees_walked, 2, "{st:?}"); // root + docs
    assert_eq!(st.trees_skipped, 2, "{st:?}"); // src + secrets
    // --full re-walks everything and changes nothing.
    let st = acl::reindex_with(b, &policy, c2, true).await.unwrap();
    assert_eq!((st.trees_walked, st.tags_written), (5, 0));

    // Changing zone prefixes invalidates the markers.
    let text = format!(
        "{}\n[[path]]\nprefix = \"docs\"\nread = [\"group:security\"]\nwrite = []\n",
        policy_text()
    );
    let stricter = Policy::parse(&text).unwrap();
    let st = acl::reindex(b, &stricter, c2).await.unwrap();
    assert_eq!(st.trees_skipped, 0);
    assert_eq!(
        acl::readable(b, &stricter, &reader.principal, &[blob_id("x2")]).await.unwrap(),
        [false]
    );

    // History: indexing only the tip leaves older versions of a secret
    // fetchable by id; `reindex_history` covers every ancestor.
    let h1 = snapshot(b, None, &[("src/h", "h"), ("secrets/h", "old secret")]).await;
    let h2 = snapshot(b, Some(vec![h1]), &[("src/h", "h"), ("secrets/h", "new secret")]).await;
    let h3 = snapshot(b, Some(vec![h2]), &[("src/h", "h2"), ("secrets/h", "new secret")]).await;
    acl::reindex(b, &policy, h3).await.unwrap();
    let old = [blob_id("old secret"), blob_id("new secret")];
    assert_eq!(acl::readable(b, &policy, &reader.principal, &old).await.unwrap(), [true, false]);
    let st = acl::reindex_history(b, &policy, h3, false).await.unwrap();
    assert!(st.trees_skipped >= 2, "{st:?}"); // h3's root, h2's src/secrets
    assert_eq!(acl::readable(b, &policy, &reader.principal, &old).await.unwrap(), [false, false]);
    let st = acl::reindex_history(b, &policy, h3, false).await.unwrap();
    assert_eq!((st.trees_walked, st.trees_skipped, st.tags_written), (0, 3, 0));
}

#[tokio::test]
async fn reindex_tags_imported_history() {
    reindex_flow(&Backend::in_memory()).await;
}

/// The ops-level flows against Postgres (batched derived lookups).
#[tokio::test]
#[ignore = "needs docker compose"]
async fn acl_postgres() {
    let b = common::pg::scratch_backend("acl").await;
    let result = tokio::spawn({
        let b = b.clone();
        async move {
            reindex_flow(&b).await;
            // Through the RPC layer too.
            let s = Server::start_on(b, acl()).await;
            let mut r = s.client(Some(READER));
            let mut w = s.client(Some(WRITER));
            let up = put_blob(&mut w, "pg secret\n").await.unwrap();
            let c = changeset(
                &mut w,
                None,
                vec![blob_file("secrets/pg", up), write_file("src/pg", "pub\n")],
            )
            .await
            .unwrap();
            landed_tip(&mut w, "ws/pg", c).await;
            assert_eq!(found(&mut r, &[up, blob_id("pub\n")]).await, [false, true]);
            assert_eq!(found(&mut w, &[up, blob_id("pub\n")]).await, [true, true]);
            let e = changeset(&mut r, Some(c), vec![blob_file("src/steal", up)]).await.unwrap_err();
            assert_eq!(e.code(), Code::NotFound);
        }
    })
    .await;
    // The scratch store drops the schema with the last handle to it.
    drop(b);
    if let Err(e) = result {
        std::panic::resume_unwind(e.into_panic());
    }
}

// ---------------------------------------------------------------------------
// ops-level API
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ops_level_checks() {
    let b = Backend::in_memory();
    let reader = access(READER);
    let writer = access(WRITER);
    let req = |ops: Vec<FileOp>, base: Option<ObjectId>| ChangesetRequest {
        base,
        ops,
        author: sig(),
        committer: None,
        message: "m\n".into(),
    };
    let w = |path: &str, c: &str| FileOp::Write {
        path: p(path),
        mode: EntryMode::File,
        content: WriteContent::Inline(Bytes::copy_from_slice(c.as_bytes())),
    };
    let c = changeset::create_changeset_as(
        &b,
        Some(&writer),
        req(vec![w("secrets/a", "s"), w("pub/b", "p")], None),
    )
    .await
    .unwrap();
    let e =
        changeset::create_changeset_as(&b, Some(&reader), req(vec![w("secrets/a", "x")], Some(c)))
            .await
            .unwrap_err();
    assert!(matches!(e, OpError::PermissionDenied(_)));
    assert!(
        query::read_path_as(&b, Some(&reader), &c.to_hex(), &p("secrets/a"), true)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        query::read_path_as(&b, Some(&writer), &c.to_hex(), &p("secrets/a"), true)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        query::read_path_as(&b, None, &c.to_hex(), &p("secrets/a"), true).await.unwrap().is_some()
    );
    let d = query::diff_as(&b, Some(&reader), None, &c.to_hex()).await.unwrap();
    assert_eq!(d.iter().map(|c| c.path.as_str()).collect::<Vec<_>>(), ["pub/b"]);
    let e = ant_server::ops::history::blame_as(&b, Some(&reader), &c.to_hex(), &p("secrets/a"))
        .await
        .unwrap_err();
    assert!(matches!(e, OpError::NotFound(_)));
    // Trees written under secrets/ are tagged, the root tree is not.
    let policy = &writer.policy;
    let tree = b.meta.get_commits(&[c]).await.unwrap()[0].as_ref().unwrap().tree;
    let sec = ant_store::graph::lookup_path(b.objects.as_ref(), tree, &p("secrets"))
        .await
        .unwrap()
        .unwrap()
        .1;
    let tags =
        acl::object_tags(&b, policy, &[tree, sec, blob_id("s"), blob_id("p")]).await.unwrap();
    assert!(tags[0].is_empty());
    assert_eq!(tags[1].iter().map(String::as_str).collect::<Vec<_>>(), ["secrets"]);
    assert_eq!(tags[2].iter().map(String::as_str).collect::<Vec<_>>(), ["secrets"]);
    assert!(tags[3].is_empty());
    // A public blob later also written under secrets/ keeps being public.
    let c2 = changeset::create_changeset_as(
        &b,
        Some(&writer),
        req(vec![w("secrets/copy", "p")], Some(c)),
    )
    .await
    .unwrap();
    let tags = acl::object_tags(&b, policy, &[blob_id("p")]).await.unwrap();
    assert_eq!(tags[0].iter().map(String::as_str).collect::<Vec<_>>(), ["", "secrets"]);
    assert!(acl::readable(&b, policy, &reader.principal, &[blob_id("p")]).await.unwrap()[0]);
    let _ = c2;
    // Land via ops: denied before any batch.
    let e = land::land_as(&b, Some(&reader), "main", c).await;
    assert!(matches!(e, Ok(LandOutcome::Landed { .. })), "creating a ref: {e:?}");
    let sneaky =
        changeset::create_changeset_as(&b, Some(&writer), req(vec![w("secrets/a", "t")], Some(c)))
            .await
            .unwrap();
    let e = land::land_as(&b, Some(&reader), "main", sneaky).await.unwrap_err();
    assert!(matches!(e, OpError::PermissionDenied(_)));
    let ok = land::land_as(&b, Some(&writer), "main", sneaky).await.unwrap();
    assert!(matches!(ok, LandOutcome::Landed { .. }));
}
