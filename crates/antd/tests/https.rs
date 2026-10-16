//! `https://` server URLs negotiate TLS (servers sit behind a TLS-terminating
//! proxy; `ant serve` itself speaks cleartext gRPC).

use antd::remote::RemoteObjectStore;
use antd::retry::RetryPolicy;

/// Against a peer that does not speak TLS, an `https://` URL must fail with a
/// connection error (a TLS handshake that gets nothing back), not be sent as
/// cleartext HTTP/2 and not report that TLS support is missing.
#[tokio::test]
async fn https_url_attempts_a_tls_handshake() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        // Accept and hang up: no TLS server hello ever comes back.
        while let Ok((socket, _)) = listener.accept().await {
            drop(socket);
        }
    });
    let url = format!("https://127.0.0.1:{port}");
    let err = RemoteObjectStore::connect_with_retry(&url, None, None, RetryPolicy::none())
        .await
        .err()
        .expect("a peer without TLS must not connect");
    let msg = format!("{err:#}");
    assert!(!msg.contains("without TLS enabled"), "TLS support is compiled in: {msg}");
}
