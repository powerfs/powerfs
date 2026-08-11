//! Minimal admin/debug HTTP server for the FUSE client.
//!
//! Exposes request statistics and in-flight request tracking for debugging
//! hangs and monitoring performance. Designed to be lightweight — no external
//! HTTP framework, just raw `tokio::net::TcpListener` with manual HTTP/1.0
//! response parsing.
//!
//! # Endpoints
//!
//! - `GET /stats` — JSON snapshot of all request statistics (per-msg_type
//!   counters, in-flight requests sorted by age, error breakdown)
//! - `GET /health` — Simple `{"status":"ok"}` health check
//!
//! # Usage
//!
//! ```ignore
//! let stats = Arc::new(RequestStats::new());
//! AdminServer::start("0.0.0.0:9999", stats);
//! ```

use std::sync::Arc;

use log::{error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use powerfs_fuse_core::RequestStats;

/// Admin HTTP server.
pub struct AdminServer;

impl AdminServer {
    /// Start the admin server on the given bind address.
    ///
    /// Spawns a background tokio task that accepts connections and handles
    /// requests. The task runs until the tokio runtime is shut down.
    pub fn start(bind_addr: String, stats: Arc<RequestStats>) {
        if bind_addr.is_empty() {
            return;
        }

        tokio::spawn(async move {
            let listener = match TcpListener::bind(&bind_addr).await {
                Ok(l) => {
                    info!("Admin server listening on {}", bind_addr);
                    l
                }
                Err(e) => {
                    error!(
                        "Admin server failed to bind {}: {} — stats endpoint disabled",
                        bind_addr, e
                    );
                    return;
                }
            };

            loop {
                match listener.accept().await {
                    Ok((mut stream, peer)) => {
                        let stats = stats.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(&mut stream, &stats).await {
                                warn!("Admin connection from {} error: {}", peer, e);
                            }
                        });
                    }
                    Err(e) => {
                        warn!("Admin server accept error: {}", e);
                    }
                }
            }
        });
    }
}

async fn handle_connection(
    stream: &mut tokio::net::TcpStream,
    stats: &RequestStats,
) -> std::io::Result<()> {
    // Read request (up to 4KB — we only care about the request line)
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buf[..n]);
    let request_line = request.lines().next().unwrap_or("");

    // Parse: "GET /path HTTP/1.1"
    let path = request_line.split_whitespace().nth(1).unwrap_or("/");

    let (status, body) = match path {
        "/stats" => {
            let snap = stats.snapshot();
            match serde_json::to_string(&snap) {
                Ok(json) => ("200 OK", json),
                Err(e) => ("500 Internal Server Error", format!("{{\"error\":\"{}\"}}", e)),
            }
        }
        "/health" => ("200 OK", r#"{"status":"ok"}"#.to_string()),
        _ => (
            "404 Not Found",
            r#"{"error":"not found","endpoints":["/stats","/health"]}"#.to_string(),
        ),
    };

    let response = format!(
        "HTTP/1.0 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        body.len(),
        body
    );

    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}
