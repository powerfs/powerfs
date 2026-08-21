//! Shared HTTP client helper for CLI commands that talk to admin REST endpoints.
//!
//! Uses raw TCP to avoid pulling in reqwest/hyper as a dependency. Sufficient
//! for the simple GET/PUT/DELETE JSON calls used by master/filer/monitor admin APIs.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use powerfs_common::error::{PowerFsError, Result};

/// Perform an HTTP request and return the response body as a String.
///
/// `method` must be uppercase. `body` is sent as-is (caller responsible for
/// JSON encoding). `body` is ignored for GET/DELETE.
pub fn http_request(addr: &str, method: &str, path: &str, body: Option<&str>) -> Result<String> {
    let socket_addr = addr
        .to_socket_addrs()
        .map_err(|e| PowerFsError::Internal(format!("invalid addr '{}': {}", addr, e)))?
        .next()
        .ok_or_else(|| PowerFsError::Internal(format!("no address resolved for '{}'", addr)))?;

    let mut stream = TcpStream::connect_timeout(&socket_addr, Duration::from_secs(5))
        .map_err(|e| PowerFsError::Internal(format!("connect {} failed: {}", addr, e)))?;

    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| PowerFsError::Internal(format!("set_read_timeout: {}", e)))?;

    let content_length = body.map(|b| b.len()).unwrap_or(0);
    let mut request = format!(
        "{} {} HTTP/1.0\r\nHost: powerfs-cli\r\nConnection: close\r\n",
        method, path
    );
    if body.is_some() {
        request.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            content_length
        ));
    }
    request.push_str("\r\n");
    if let Some(b) = body {
        request.push_str(b);
    }

    stream
        .write_all(request.as_bytes())
        .map_err(|e| PowerFsError::Internal(format!("write request: {}", e)))?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|e| PowerFsError::Internal(format!("read response: {}", e)))?;

    let response_str = String::from_utf8_lossy(&response);
    let body_start = response_str
        .find("\r\n\r\n")
        .ok_or_else(|| PowerFsError::Internal("malformed HTTP response".into()))?;

    let status_line = response_str.lines().next().unwrap_or("");
    if !status_line.contains("200") && !status_line.contains("204") {
        let body_text = &response_str[body_start + 4..];
        return Err(PowerFsError::Internal(format!(
            "HTTP {} → {}",
            status_line,
            body_text.trim()
        )));
    }

    Ok(response_str[body_start + 4..].trim().to_string())
}

/// Convenience: HTTP GET returning JSON body.
pub fn http_get(addr: &str, path: &str) -> Result<String> {
    http_request(addr, "GET", path, None)
}

/// Convenience: HTTP PUT with JSON body.
pub fn http_put(addr: &str, path: &str, body: &str) -> Result<String> {
    http_request(addr, "PUT", path, Some(body))
}

/// Convenience: HTTP DELETE.
pub fn http_delete(addr: &str, path: &str) -> Result<String> {
    http_request(addr, "DELETE", path, None)
}
