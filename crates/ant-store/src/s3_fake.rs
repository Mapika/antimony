//! A scripted fake S3 endpoint for tests: a tiny HTTP/1.1 server that answers
//! the n-th request with the n-th scripted behaviour (the last repeats).
//! GET bodies are `byte(i) = i % 251` over the requested range.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::s3::{S3Config, S3Timeouts};

#[derive(Debug, Clone, Copy)]
pub(crate) enum Act {
    /// Full, correct response.
    Ok,
    /// Headers and half the body, then silence forever.
    Stall,
    /// Headers and half the body, then a clean close.
    Truncate,
    /// Read the request, then reset the connection.
    Reset,
    /// 429 `SlowDown`, with `Retry-After` seconds when given.
    Throttle(Option<u32>),
    /// A non-retryable status.
    Status(u16),
}

pub(crate) struct Fake {
    pub addr: SocketAddr,
    pub hits: Arc<AtomicUsize>,
}

pub(crate) fn byte_at(i: u64) -> u8 {
    (i % 251) as u8
}

pub(crate) async fn spawn(script: Vec<Act>) -> Fake {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { return };
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let act = script[n.min(script.len() - 1)];
            tokio::spawn(handle(stream, act));
        }
    });
    Fake { addr, hits }
}

/// Fast timeouts so failure paths take fractions of a second.
pub(crate) fn fast_timeouts(max_attempts: u32) -> S3Timeouts {
    S3Timeouts {
        connect: Duration::from_millis(500),
        attempt: Duration::from_millis(300),
        min_throughput: 1_000_000_000,
        operation: Duration::from_secs(20),
        max_attempts,
    }
}

pub(crate) fn config(fake: &Fake, timeouts: S3Timeouts) -> S3Config {
    S3Config {
        endpoint: format!("http://{}", fake.addr),
        region: "us-east-1".into(),
        bucket: "b".into(),
        prefix: "p".into(),
        access_key: "k".into(),
        secret_key: "s".into(),
        timeouts,
    }
}

async fn handle(mut stream: TcpStream, act: Act) {
    // Request head (and, for PUT, whatever body is already in flight).
    let mut head = Vec::new();
    let mut buf = [0u8; 4096];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => head.extend_from_slice(&buf[..n]),
        }
    }
    let text = String::from_utf8_lossy(&head).to_string();
    let is_get = text.starts_with("GET");
    // Let a PUT body arrive before answering.
    if !is_get {
        while let Ok(Ok(n)) =
            tokio::time::timeout(Duration::from_millis(50), stream.read(&mut buf)).await
        {
            if n == 0 {
                break;
            }
        }
    }
    let range = text.lines().find_map(|l| {
        let l = l.to_ascii_lowercase();
        let r = l.strip_prefix("range: bytes=")?.to_owned();
        let (a, b) = r.split_once('-')?;
        Some((a.trim().parse::<u64>().ok()?, b.trim().parse::<u64>().ok()?))
    });
    let (start, len) = range.map_or((0, 0), |(a, b)| (a, b - a + 1));
    let body: Vec<u8> = (start..start + len).map(byte_at).collect();
    let header = |status: &str, extra: &str, len: usize| {
        format!("HTTP/1.1 {status}\r\nContent-Length: {len}\r\nConnection: close\r\n{extra}\r\n")
    };
    match act {
        Act::Ok if is_get => {
            let h = header("206 Partial Content", "", body.len());
            let _ = stream.write_all(h.as_bytes()).await;
            let _ = stream.write_all(&body).await;
        }
        Act::Ok => {
            let _ = stream.write_all(header("200 OK", "", 0).as_bytes()).await;
        }
        Act::Stall | Act::Truncate => {
            let h = header("206 Partial Content", "", body.len());
            let _ = stream.write_all(h.as_bytes()).await;
            let _ = stream.write_all(&body[..body.len() / 2]).await;
            let _ = stream.flush().await;
            if matches!(act, Act::Stall) {
                std::future::pending::<()>().await;
            }
        }
        Act::Reset => {
            #[allow(deprecated)] // RST rather than FIN is the point
            let _ = stream.set_linger(Some(Duration::ZERO));
        }
        Act::Throttle(after) => {
            let xml = "<Error><Code>SlowDown</Code><Message>slow down</Message></Error>";
            let extra = after.map_or(String::new(), |s| format!("Retry-After: {s}\r\n"));
            let h = header("429 Too Many Requests", &extra, xml.len());
            let _ = stream.write_all(h.as_bytes()).await;
            let _ = stream.write_all(xml.as_bytes()).await;
        }
        Act::Status(code) => {
            let xml = "<Error><Code>AccessDenied</Code><Message>no</Message></Error>";
            let h = header(&format!("{code} Error"), "", xml.len());
            let _ = stream.write_all(h.as_bytes()).await;
            let _ = stream.write_all(xml.as_bytes()).await;
        }
    }
}
