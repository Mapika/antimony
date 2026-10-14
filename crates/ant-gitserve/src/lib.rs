//! Read-only Git protocol-v2 smart HTTP export of an antimony backend.
//!
//! Serves SHA-256 `upload-pack` (`ls-refs`, `fetch` with `shallow` and
//! `filter blob:none`) at `/repo.git`; `receive-pack` is refused. Packs are
//! streamed with bounded memory and delta-compress trees and blobs against a
//! window of other versions of the same path (see `pack.rs`, `README.md`).
//! Commits served must be recorded in the MetaStore (generation numbers bound
//! the `have` walk).
mod delta;
mod pack;
mod protocol;
mod select;

use std::collections::HashMap;

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
    Router::new()
        .route("/repo.git/info/refs", get(advertise))
        .route("/repo.git/git-upload-pack", post(upload))
        .route("/repo.git/git-receive-pack", axum::routing::any(|| async { StatusCode::FORBIDDEN }))
        .with_state(backend)
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

async fn upload(State(backend): State<Backend>, headers: HeaderMap, body: Bytes) -> Response {
    let body = match decode_body(&headers, body) {
        Ok(body) => body,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let request = match protocol::parse(&body) {
        Ok(request) => request,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    match request.command {
        "ls-refs" => match refs(&backend, &request.args).await {
            Ok(body) => response(body, false),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        },
        "fetch" => {
            let fetch = match Fetch::parse(&request.args) {
                Ok(fetch) => fetch,
                Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
            };
            match fetch_response(backend, fetch).await {
                Ok(response) => response,
                Err(error) => {
                    tracing::warn!(error = format!("{error:?}"), "git fetch failed");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }
        _ => StatusCode::BAD_REQUEST.into_response(),
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

async fn fetch_response(b: Backend, fetch: Fetch) -> Result<Response> {
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
    let selection = select::select(&b, &fetch).await?;
    let planned = started.elapsed();
    if fetch.depth.is_some() {
        prefix.extend(packet(b"shallow-info\n"));
        for id in selection.shallow {
            prefix.extend(packet(format!("shallow {id}\n").as_bytes()));
        }
        for id in selection.unshallow {
            prefix.extend(packet(format!("unshallow {id}\n").as_bytes()));
        }
        prefix.extend_from_slice(b"0001");
    }
    prefix.extend(packet(b"packfile\n"));
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(Ok(prefix.into())).await?;
    tokio::spawn(async move {
        match pack::send_planned(b, selection.objects, fetch.ofs_delta, tx.clone()).await {
            Ok(stats) => tracing::info!(
                objects = stats.objects,
                deltas = stats.deltas,
                bytes = stats.bytes,
                planning_ms = planned.as_millis() as u64,
                total_ms = started.elapsed().as_millis() as u64,
                "git pack sent"
            ),
            Err(error) => {
                tracing::warn!(%error, "git pack failed");
                let _ = tx.send(Err(std::io::Error::other(error.to_string()))).await;
            }
        }
    });
    Ok(response(Body::from_stream(ReceiverStream::new(rx)), false))
}
