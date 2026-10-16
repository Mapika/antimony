//! Read-only Git protocol-v2 smart HTTP export of an antimony backend.
//!
//! Serves SHA-256 `upload-pack` (`ls-refs`, `fetch` with `shallow` and
//! `filter blob:none`) at `/repo.git`; `receive-pack` is refused. Packs are
//! streamed with bounded memory and delta-compress trees and blobs against a
//! window of other versions of the same path (see `pack.rs`, `README.md`).
//! Complete clones reuse a cached base pack (`cache.rs`). Commits served must
//! be recorded in the MetaStore (generation numbers bound the `have` walk).
//!
//! Every request sees the whole repository: the export has no per-principal
//! view, and a server with an access policy admits only principals that can
//! read everything (`ant-server`'s `git_auth`), so packs (cached or not) do
//! not depend on who asks.
mod cache;
mod pack;
mod protocol;
mod select;
mod walk;

use std::{collections::HashMap, sync::Arc};

use ant_store::Backend;
use anyhow::Result;
use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use tokio_stream::wrappers::ReceiverStream;

use protocol::packet;
use select::Fetch;

/// Export a backend at `/repo.git`. Every write endpoint returns HTTP 403.
pub fn router(backend: Backend) -> Router {
    let state = Export { backend, cache: Arc::default() };
    Router::new()
        .route("/repo.git/info/refs", get(advertise))
        .route("/repo.git/git-upload-pack", post(upload))
        .route("/repo.git/git-receive-pack", axum::routing::any(|| async { StatusCode::FORBIDDEN }))
        .with_state(state)
}

/// One export: its backend and its cached base packs (`cache.rs`).
#[derive(Clone)]
struct Export {
    backend: Backend,
    cache: Arc<cache::PackCache>,
}

fn response(body: impl Into<Body>, advertisement: bool) -> Response {
    let content_type = if advertisement {
        "application/x-git-upload-pack-advertisement"
    } else {
        "application/x-git-upload-pack-result"
    };
    ([(header::CONTENT_TYPE, content_type), (header::CACHE_CONTROL, "no-cache")], body.into())
        .into_response()
}

async fn advertise(Query(query): Query<HashMap<String, String>>) -> Response {
    match query.get("service").map(String::as_str) {
        Some("git-receive-pack") => return StatusCode::FORBIDDEN.into_response(),
        Some("git-upload-pack") => {}
        _ => return StatusCode::BAD_REQUEST.into_response(),
    }
    let mut body = Vec::new();
    for line in [
        "version 2\n",
        "ls-refs=unborn\n",
        "fetch=shallow filter\n",
        "server-option\n",
        "object-format=sha256\n",
    ] {
        body.extend(packet(line.as_bytes()));
    }
    body.extend_from_slice(b"0000");
    response(body, true)
}

async fn upload(State(export): State<Export>, headers: HeaderMap, body: Bytes) -> Response {
    let body = match decode_body(&headers, body) {
        Ok(body) => body,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let request = match protocol::parse(&body) {
        Ok(request) => request,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    match request.command {
        "ls-refs" => match refs(&export.backend, &request.args).await {
            Ok(body) => response(body, false),
            Err(error) => {
                tracing::warn!(error = format!("{error:#}"), "git ls-refs failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        },
        "fetch" => {
            let fetch = match Fetch::parse(&request.args) {
                Ok(fetch) => fetch,
                Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
            };
            match fetch_response(export, fetch).await {
                Ok(response) => response,
                Err(error) => {
                    tracing::warn!(error = format!("{error:#}"), "git fetch failed");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
}

/// The clone `fetch` planned on top of the cached base of its filter, if
/// there is one and it fits (`select::select_after`).
async fn plan_on_base(
    b: &Backend,
    fetch: &Fetch,
    cache: &cache::PackCache,
) -> Result<Option<select::Selection>> {
    match cache.get(fetch.no_blobs) {
        Some(base) => select::select_after(b, fetch, &base, base.rebuild_after()).await,
        None => Ok(None),
    }
}

/// git gzips large request bodies (e.g. many `want` lines during a lazy
/// partial-clone checkout) and says so with `Content-Encoding: gzip`.
fn decode_body(headers: &HeaderMap, body: Bytes) -> std::io::Result<Bytes> {
    let gzip = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("gzip") || v.eq_ignore_ascii_case("x-gzip"));
    if !gzip {
        return Ok(body);
    }
    let mut out = Vec::with_capacity(body.len() * 4);
    std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(&body[..]), &mut out)?;
    Ok(out.into())
}

async fn refs(b: &Backend, args: &[&str]) -> Result<Vec<u8>> {
    let refs = b.refs.list_refs("").await?;
    let prefixes: Vec<_> = args.iter().filter_map(|arg| arg.strip_prefix("ref-prefix ")).collect();
    let include =
        |name: &str| prefixes.is_empty() || prefixes.iter().any(|prefix| name.starts_with(prefix));
    let symrefs = args.contains(&"symrefs");
    let mut body = Vec::new();
    if include("HEAD") {
        let target = if symrefs { " symref-target:refs/heads/main" } else { "" };
        if let Some((_, id)) = refs.iter().find(|(name, _)| name == "main") {
            body.extend(packet(format!("{id} HEAD{target}\n").as_bytes()));
        } else if args.contains(&"unborn") {
            body.extend(packet(format!("unborn HEAD{target}\n").as_bytes()));
        }
    }
    for (name, id) in refs {
        let name = format!("refs/heads/{name}");
        if include(&name) {
            body.extend(packet(format!("{id} {name}\n").as_bytes()));
        }
    }
    body.extend_from_slice(b"0000");
    Ok(body)
}

async fn fetch_response(export: Export, fetch: Fetch) -> Result<Response> {
    let Export { backend: b, cache } = export;
    let mut prefix = Vec::new();
    if !fetch.done {
        prefix.extend(packet(b"acknowledgments\n"));
        let mut acknowledged = false;
        for chunk in fetch.haves.chunks(select::BATCH) {
            for (id, exists) in chunk.iter().zip(b.objects.has_many(chunk).await?) {
                if exists {
                    prefix.extend(packet(format!("ACK {id}\n").as_bytes()));
                    acknowledged = true;
                }
            }
        }
        if !acknowledged {
            prefix.extend(packet(b"NAK\n"));
        }
        prefix.extend_from_slice(b"0000");
        return Ok(response(prefix, false));
    }
    let started = std::time::Instant::now();
    // A clone is planned on top of the cached base pack of its filter when
    // that fits. Otherwise it waits for a base being built (which may fit),
    // or is planned from scratch and becomes the new base.
    let mut on_base = None;
    let mut build = None;
    if fetch.is_clone() {
        on_base = plan_on_base(&b, &fetch, &cache).await?;
        if on_base.is_none() {
            let guard = cache.lock_build(fetch.no_blobs).await;
            on_base = plan_on_base(&b, &fetch, &cache).await?;
            build = on_base.is_none().then_some(guard);
        }
    }
    let mut selection = match on_base {
        Some(selection) => selection,
        None => select::select(&b, &fetch).await?,
    };
    let record = build.and_then(|guard| cache::Recorder::new(cache.clone(), &selection, guard));
    let planned = started.elapsed();
    if fetch.depth.is_some() {
        prefix.extend(packet(b"shallow-info\n"));
        for id in std::mem::take(&mut selection.shallow) {
            prefix.extend(packet(format!("shallow {id}\n").as_bytes()));
        }
        for id in std::mem::take(&mut selection.unshallow) {
            prefix.extend(packet(format!("unshallow {id}\n").as_bytes()));
        }
        prefix.extend_from_slice(b"0001");
    }
    prefix.extend(packet(b"packfile\n"));
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(Ok(prefix.into())).await?;
    tokio::spawn(async move {
        match pack::send_with(b, selection, fetch.ofs_delta, record, tx.clone()).await {
            Ok(stats) => {
                tracing::info!(
                    objects = stats.objects,
                    deltas = stats.deltas,
                    reused = stats.reused,
                    bytes = stats.bytes,
                    planning_ms = planned.as_millis() as u64,
                    total_ms = started.elapsed().as_millis() as u64,
                    "git pack sent"
                );
            }
            Err(error) => {
                tracing::warn!(error = format!("{error:#}"), "git pack failed");
                // Sideband 3 (git prints "remote error: ..." and fails), then
                // the body ends without a pack trailer or flush packet. An
                // aborted body would hide the message from the client.
                let mut message = vec![3];
                let text = format!("{error:#}");
                message.extend_from_slice(&text.as_bytes()[..text.len().min(1000)]);
                message.push(b'\n');
                let _ = tx.send(Ok(packet(&message).into())).await;
            }
        }
    });
    Ok(response(Body::from_stream(ReceiverStream::new(rx)), false))
}
