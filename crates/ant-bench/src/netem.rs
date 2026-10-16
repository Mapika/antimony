//! A userspace network emulator: a TCP relay that delays every byte by a
//! fixed one-way latency in each direction and optionally caps the bandwidth
//! of each direction (shared by all connections through the relay, like one
//! link). Lets checkout and clone benchmarks reproduce a LAN or WAN round
//! trip on one machine without root (`tc netem` needs it).
//!
//! Model: a chunk read from one side at time `t` leaves the link at
//! `max(t, link_free) + len / bandwidth` and is written to the other side
//! `delay` after that. Data in flight is bounded per connection and
//! direction ([`WINDOW`]), so a slow reader eventually pushes back on the
//! writer, as a TCP window would. The relay itself sits on loopback on both
//! sides, so TCP slow start and loss are not reproduced: the numbers are a
//! lower bound for a real link with the same RTT and bandwidth.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
use tokio::time::Instant;

/// Bytes in flight per connection and direction.
const WINDOW: usize = 16 << 20;
/// Largest chunk read at once.
const CHUNK: usize = 64 << 10;

#[derive(Debug, Clone, Copy, Default)]
pub struct Link {
    /// One-way delay (half the round trip added by the relay).
    pub one_way: Duration,
    /// Bytes per second per direction; `None`: unlimited.
    pub bytes_per_sec: Option<u64>,
}

impl Link {
    /// A link with round trip `rtt_ms` and optional `mbit` per second.
    pub fn new(rtt_ms: f64, mbit: Option<f64>) -> Self {
        Self {
            one_way: Duration::from_secs_f64(rtt_ms.max(0.0) / 2000.0),
            bytes_per_sec: mbit.filter(|m| *m > 0.0).map(|m| (m * 1e6 / 8.0) as u64),
        }
    }

    pub fn is_noop(&self) -> bool {
        self.one_way.is_zero() && self.bytes_per_sec.is_none()
    }
}

/// When each direction of the link is next free (shared by connections).
#[derive(Default)]
struct Wire {
    up: Mutex<Option<Instant>>,
    down: Mutex<Option<Instant>>,
}

/// A running relay; it stops when the runtime does.
pub struct Relay {
    pub addr: SocketAddr,
}

/// Listen on `127.0.0.1:0` (or `listen`) and relay every connection to
/// `target` through `link`.
pub async fn spawn(target: SocketAddr, link: Link, listen: Option<SocketAddr>) -> Result<Relay> {
    let listener = TcpListener::bind(listen.unwrap_or_else(|| ([127, 0, 0, 1], 0).into()))
        .await
        .context("binding the relay")?;
    let addr = listener.local_addr()?;
    let wire = Arc::new(Wire::default());
    tokio::spawn(async move {
        loop {
            let Ok((client, _)) = listener.accept().await else { continue };
            let wire = wire.clone();
            tokio::spawn(async move {
                if let Ok(server) = TcpStream::connect(target).await {
                    let _ = client.set_nodelay(true);
                    let _ = server.set_nodelay(true);
                    let (cr, cw) = client.into_split();
                    let (sr, sw) = server.into_split();
                    let up = pump(cr, sw, link, wire.clone(), true);
                    let down = pump(sr, cw, link, wire, false);
                    let _ = tokio::join!(up, down);
                }
            });
        }
    });
    Ok(Relay { addr })
}

/// Copy `from` to `to`, delaying each chunk per `link`.
async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    link: Link,
    wire: Arc<Wire>,
    upstream: bool,
) {
    let window = Arc::new(Semaphore::new(WINDOW));
    let (tx, mut rx) =
        mpsc::unbounded_channel::<(Instant, Vec<u8>, tokio::sync::OwnedSemaphorePermit)>();
    let reader = {
        let window = window.clone();
        async move {
            let mut buf = vec![0u8; CHUNK];
            loop {
                let n = match from.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                let Ok(permit) = window.clone().acquire_many_owned(n as u32).await else { break };
                let now = Instant::now();
                let left = match link.bytes_per_sec {
                    None => now,
                    Some(bps) => {
                        let tx_time = Duration::from_secs_f64(n as f64 / bps as f64);
                        let slot = if upstream { &wire.up } else { &wire.down };
                        let mut free = slot.lock();
                        let start = free.map_or(now, |f| f.max(now));
                        let done = start + tx_time;
                        *free = Some(done);
                        done
                    }
                };
                if tx.send((left + link.one_way, buf[..n].to_vec(), permit)).is_err() {
                    break;
                }
            }
        }
    };
    let writer = async move {
        while let Some((at, data, permit)) = rx.recv().await {
            tokio::time::sleep_until(at).await;
            if to.write_all(&data).await.is_err() {
                return;
            }
            drop(permit);
        }
        let _ = to.shutdown().await;
    };
    tokio::join!(reader, writer);
}

/// Put a relay in front of the host and port of `url` (`http://host:port`)
/// and return the same URL pointing at the relay.
pub async fn wrap_url(url: &str, link: Link) -> Result<String> {
    let (scheme, rest) = url.split_once("://").context("URL without a scheme")?;
    let (hostport, path) = match rest.find('/') {
        Some(i) => rest.split_at(i),
        None => (rest, ""),
    };
    let target = tokio::net::lookup_host(hostport)
        .await
        .with_context(|| format!("resolving {hostport}"))?
        .next()
        .with_context(|| format!("no address for {hostport}"))?;
    let relay = spawn(target, link, None).await?;
    Ok(format!("{scheme}://{}{path}", relay.addr))
}

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Address to listen on (e.g. 127.0.0.1:7001).
    #[arg(long)]
    pub listen: SocketAddr,
    /// Address to relay to (host:port).
    #[arg(long)]
    pub target: String,
    /// Round trip time added, in milliseconds.
    #[arg(long, default_value_t = 0.0)]
    pub rtt_ms: f64,
    /// Bandwidth cap per direction in Mbit/s (default: none).
    #[arg(long)]
    pub mbit: Option<f64>,
}

/// `ant-bench netem`: run a relay until interrupted.
pub async fn run(a: Args) -> Result<()> {
    let target = tokio::net::lookup_host(&a.target)
        .await?
        .next()
        .with_context(|| format!("no address for {}", a.target))?;
    let relay = spawn(target, Link::new(a.rtt_ms, a.mbit), Some(a.listen)).await?;
    println!("relaying {} -> {target} (rtt +{} ms, {:?} Mbit/s)", relay.addr, a.rtt_ms, a.mbit);
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Echo server behind a relay: a round trip takes at least the RTT and
    /// the bytes come back unchanged.
    #[tokio::test]
    async fn adds_the_round_trip_and_keeps_bytes() {
        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = echo.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = echo.accept().await.unwrap();
            let mut buf = vec![0u8; 1 << 16];
            loop {
                let n = s.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                s.write_all(&buf[..n]).await.unwrap();
            }
        });
        let relay = spawn(target, Link::new(40.0, None), None).await.unwrap();
        let mut c = TcpStream::connect(relay.addr).await.unwrap();
        let payload: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let t = std::time::Instant::now();
        c.write_all(&payload).await.unwrap();
        let mut back = vec![0u8; payload.len()];
        c.read_exact(&mut back).await.unwrap();
        assert!(t.elapsed() >= Duration::from_millis(40), "{:?}", t.elapsed());
        assert_eq!(back, payload);
    }
}
