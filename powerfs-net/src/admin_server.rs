//! Lightweight HTTP admin server for operational management
//!
//! Exposes ServerConnectionManager's admin APIs via HTTP endpoints:
//! - GET /health       → HealthStatus
//! - GET /metrics      → MetricsSnapshot
//! - GET /sessions     → list of client sessions
//! - GET /sessions/:id → single session details
//! - POST /disconnect  → force disconnect a client
//!
//! Uses a minimal HTTP/1.1 implementation over tokio for zero-framework overhead.

use std::net::SocketAddr;
use std::sync::Arc;

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::server_connection::{HealthStatus, MetricsSnapshot, ServerConnectionManager};

/// Admin server configuration
#[derive(Debug, Clone)]
pub struct AdminServerConfig {
    pub addr: String,
    pub port: u16,
}

impl Default for AdminServerConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1".into(),
            port: 9334,
        }
    }
}

/// HTTP admin server
pub struct AdminServer {
    config: AdminServerConfig,
    manager: Arc<ServerConnectionManager>,
    shutdown: Arc<RwLock<bool>>,
}

impl AdminServer {
    pub fn new(config: AdminServerConfig, manager: Arc<ServerConnectionManager>) -> Self {
        Self {
            config,
            manager,
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    /// Start serving (runs until shutdown is signaled)
    pub async fn serve(&self) -> std::io::Result<()> {
        let addr: SocketAddr = format!("{}:{}", self.config.addr, self.config.port)
            .parse()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let listener = TcpListener::bind(addr).await?;
        log::info!("Admin HTTP server listening on http://{}", addr);

        loop {
            if *self.shutdown.read().await {
                log::info!("Admin server shutting down");
                break;
            }

            match tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                .await
            {
                Ok(Ok((stream, peer))) => {
                    log::debug!("Admin: new connection from {}", peer);
                    let manager = self.manager.clone();
                    let shutdown = self.shutdown.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, manager, shutdown).await {
                            log::error!("Admin connection error: {:?}", e);
                        }
                    });
                }
                Ok(Err(e)) => {
                    log::error!("Admin accept error: {:?}", e);
                }
                Err(_elapsed) => {
                    // Timeout — loop back to check shutdown flag
                    continue;
                }
            }
        }

        Ok(())
    }

    /// Signal the admin server to shut down
    pub async fn shutdown(&self) {
        let mut s = self.shutdown.write().await;
        *s = true;
    }

    async fn handle_connection(
        stream: TcpStream,
        manager: Arc<ServerConnectionManager>,
        shutdown: Arc<RwLock<bool>>,
    ) -> std::io::Result<()> {
        let mut stream = stream;

        // Read request
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }

        let request = String::from_utf8_lossy(&buf[..n]);
        let (method, path) = parse_request_line(&request);

        let response = match (method.as_str(), path.as_str()) {
            ("GET", "/health") => handle_health(&manager).await,
            ("GET", "/metrics") => handle_metrics(&manager).await,
            ("GET", "/sessions") => handle_sessions(&manager).await,
            ("GET", p) if p.starts_with("/sessions/") => {
                let id = &p["/sessions/".len()..];
                handle_session_detail(&manager, id).await
            }
            ("POST", p) if p.starts_with("/disconnect") => {
                let id = extract_query_param(p, "client_id");
                handle_disconnect(&manager, id).await
            }
            ("GET", "/") => handle_root(),
            _ => handle_not_found(),
        };

        stream.write_all(response.as_bytes()).await?;
        stream.flush().await?;

        // Check shutdown
        if *shutdown.read().await {
            return Ok(());
        }

        Ok(())
    }
}

fn parse_request_line(request: &str) -> (String, String) {
    let first_line = request.lines().next().unwrap_or("");
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("GET").to_string();
    let path = parts.next().unwrap_or("/").to_string();
    (method, path)
}

fn extract_query_param(path: &str, param: &str) -> Option<String> {
    let query_start = path.find('?')?;
    let query = &path[query_start + 1..];
    for pair in query.split('&') {
        let mut kv = pair.splitn(2, '=');
        if kv.next() == Some(param) {
            return kv.next().map(|v| v.to_string());
        }
    }
    None
}

fn build_response(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{}",
        status,
        content_type,
        body.len(),
        body
    )
}

fn json_response<T: Serialize>(status: &str, data: &T) -> String {
    let body = serde_json::to_string_pretty(data).unwrap_or_else(|_| "{}".into());
    build_response(status, "application/json", &body)
}

async fn handle_health(manager: &Arc<ServerConnectionManager>) -> String {
    let health = manager.health_check().await;
    json_response("200 OK", &health)
}

async fn handle_metrics(manager: &Arc<ServerConnectionManager>) -> String {
    let snapshot = manager.get_metrics_snapshot().await;
    json_response("200 OK", &snapshot)
}

async fn handle_sessions(manager: &Arc<ServerConnectionManager>) -> String {
    let sessions = manager.registry().list().await;
    let ids: Vec<u64> = sessions.iter().map(|s| s.id).collect();
    json_response("200 OK", &ids)
}

async fn handle_session_detail(manager: &Arc<ServerConnectionManager>, id_str: &str) -> String {
    let id: u64 = match id_str.parse() {
        Ok(v) => v,
        Err(_) => return build_response("400 Bad Request", "text/plain", "Invalid client ID"),
    };
    let conn = manager.registry().get(id);
    match conn {
        Some(conn) => {
            let state = format!("{:?}", *conn.state.read().await);
            let stats = conn.stats.read().await;
            let view = ClientSessionView {
                client_id: conn.id,
                client_type: format!("{:?}", conn.client_type),
                address: conn.addr.to_string(),
                state,
                request_count: stats.request_count,
                error_count: stats.error_count,
                connected_ms_ago: stats.connected_at.elapsed().as_millis() as u64,
            };
            json_response("200 OK", &view)
        }
        None => build_response("404 Not Found", "text/plain", "Session not found"),
    }
}

async fn handle_disconnect(
    manager: &Arc<ServerConnectionManager>,
    id_opt: Option<String>,
) -> String {
    let id_str = match id_opt {
        Some(v) => v,
        None => {
            return build_response(
                "400 Bad Request",
                "text/plain",
                "Missing client_id parameter",
            )
        }
    };
    let id: u64 = match id_str.parse() {
        Ok(v) => v,
        Err(_) => return build_response("400 Bad Request", "text/plain", "Invalid client ID"),
    };
    let ok = manager.force_disconnect(id).await;
    if ok {
        build_response("200 OK", "text/plain", "Disconnected")
    } else {
        build_response("404 Not Found", "text/plain", "Session not found")
    }
}

fn handle_root() -> String {
    let body = r#"{
  "service": "PowerFS Admin API",
  "endpoints": {
    "health": "GET /health",
    "metrics": "GET /metrics",
    "sessions": "GET /sessions",
    "session_detail": "GET /sessions/:id",
    "disconnect": "POST /disconnect?client_id=:id"
  }
}"#;
    build_response("200 OK", "application/json", body)
}

fn handle_not_found() -> String {
    build_response("404 Not Found", "text/plain", "Not Found")
}

// Add Serialize implementations for admin types
impl Serialize for HealthStatus {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("HealthStatus", 3)?;
        s.serialize_field("healthy", &self.healthy)?;
        s.serialize_field("active_sessions", &self.active_sessions)?;
        s.serialize_field("total_sessions", &self.total_sessions)?;
        s.end()
    }
}

impl Serialize for MetricsSnapshot {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("MetricsSnapshot", 7)?;
        s.serialize_field("total_requests", &self.total_requests)?;
        s.serialize_field("successful_requests", &self.successful_requests)?;
        s.serialize_field("failed_requests", &self.failed_requests)?;
        s.serialize_field("total_latency_us", &self.total_latency_us)?;
        s.serialize_field("active_sessions", &self.active_sessions)?;
        s.serialize_field("total_sessions", &self.total_sessions)?;
        s.serialize_field("avg_latency_us", &self.avg_latency_us())?;
        s.serialize_field("success_rate", &self.success_rate())?;
        s.end()
    }
}

/// Serializable view of a client connection for the admin API.
///
/// `ClientConn`'s mutable fields live behind `RwLock`s and cannot implement
/// the synchronous `Serialize` trait directly, so we snapshot the relevant
/// fields into this plain struct before serializing.
#[derive(Serialize)]
struct ClientSessionView {
    client_id: u64,
    client_type: String,
    address: String,
    state: String,
    request_count: u64,
    error_count: u64,
    connected_ms_ago: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_conn::{ClientConn, ConnRegistry};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[test]
    fn test_parse_request_line_get() {
        let (method, path) = parse_request_line("GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n");
        assert_eq!(method, "GET");
        assert_eq!(path, "/health");
    }

    #[test]
    fn test_parse_request_line_post() {
        let (method, path) = parse_request_line("POST /disconnect?client_id=42 HTTP/1.1\r\n\r\n");
        assert_eq!(method, "POST");
        assert_eq!(path, "/disconnect?client_id=42");
    }

    #[test]
    fn test_extract_query_param() {
        assert_eq!(
            extract_query_param("/disconnect?client_id=42", "client_id"),
            Some("42".to_string())
        );
        assert_eq!(
            extract_query_param("/disconnect?client_id=42&force=true", "client_id"),
            Some("42".to_string())
        );
        assert_eq!(extract_query_param("/disconnect", "client_id"), None);
    }

    #[tokio::test]
    async fn test_admin_server_endpoints() {
        let registry = Arc::new(ConnRegistry::new());
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let conn = ClientConn::new(
            1,
            "127.0.0.1:19334".parse().unwrap(),
            crate::protocol::ClientType::Fuse,
            tx,
        );
        registry.register(conn).await;
        let manager = Arc::new(ServerConnectionManager::new(registry));

        let config = AdminServerConfig {
            addr: "127.0.0.1".into(),
            port: 19336,
        };
        let server = AdminServer::new(config, manager);

        let shutdown_flag = server.shutdown.clone();
        let handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Test /health
        let mut stream = TcpStream::connect("127.0.0.1:19336").await.unwrap();
        stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let resp = String::from_utf8_lossy(&buf[..n]);
        assert!(resp.contains("HTTP/1.1 200 OK"));
        assert!(resp.contains("\"healthy\""));

        // Test /metrics
        let mut stream = TcpStream::connect("127.0.0.1:19336").await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let resp = String::from_utf8_lossy(&buf[..n]);
        assert!(resp.contains("HTTP/1.1 200 OK"));
        assert!(resp.contains("total_requests"));

        // Test /sessions
        let mut stream = TcpStream::connect("127.0.0.1:19336").await.unwrap();
        stream
            .write_all(b"GET /sessions HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let resp = String::from_utf8_lossy(&buf[..n]);
        assert!(resp.contains("HTTP/1.1 200 OK"));

        // Test root endpoint
        let mut stream = TcpStream::connect("127.0.0.1:19336").await.unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let resp = String::from_utf8_lossy(&buf[..n]);
        assert!(resp.contains("PowerFS Admin API"));

        // Test 404
        let mut stream = TcpStream::connect("127.0.0.1:19336").await.unwrap();
        stream
            .write_all(b"GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        let resp = String::from_utf8_lossy(&buf[..n]);
        assert!(resp.contains("404 Not Found"));

        // Shutdown
        {
            let mut s = shutdown_flag.write().await;
            *s = true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        handle.await.unwrap();
    }
}
