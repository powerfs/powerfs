//! Docker Compose driver — shells out to `docker compose` via
//! tokio::process::Command. Trait-based so tests inject a MockComposeDriver.

use async_trait::async_trait;
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;

/// One row of `docker compose ps --format json`.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ServiceStatus {
    pub service: String,
    pub name: String,
    /// "running", "exited", etc.
    #[serde(default)]
    pub state: String,
    /// Human-readable status string ("Up 2 minutes (healthy)").
    #[serde(default)]
    pub status: String,
    /// "healthy" / "unhealthy" / "starting" / "" when absent.
    #[serde(default)]
    pub health: String,
}

#[async_trait]
pub trait ComposeDriver: Send + Sync {
    async fn up(&self, compose_file: &Path, services: &[&str]) -> Result<(), String>;
    async fn down(&self, compose_file: &Path, purge: bool) -> Result<(), String>;
    async fn ps(&self, compose_file: &Path) -> Result<Vec<ServiceStatus>, String>;
}

/// Real driver: invokes `docker compose` in a child process.
pub struct DockerComposeDriver;

impl DockerComposeDriver {
    pub fn new() -> Self {
        Self
    }

    fn base_cmd(&self, compose_file: &Path) -> Command {
        let mut c = Command::new("docker");
        c.arg("compose")
            .arg("--ansi")
            .arg("never")
            .arg("-f")
            .arg(compose_file)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        c
    }
}

#[async_trait]
impl ComposeDriver for DockerComposeDriver {
    async fn up(&self, compose_file: &Path, services: &[&str]) -> Result<(), String> {
        let mut c = self.base_cmd(compose_file);
        c.arg("up").arg("-d").args(services);
        let out = c
            .output()
            .await
            .map_err(|e| format!("spawn docker compose up: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "docker compose up failed (exit {:?}): {}",
                out.status.code(),
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(())
    }

    async fn down(&self, compose_file: &Path, purge: bool) -> Result<(), String> {
        let mut c = self.base_cmd(compose_file);
        c.arg("down");
        if purge {
            c.arg("-v");
        }
        let out = c
            .output()
            .await
            .map_err(|e| format!("spawn docker compose down: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "docker compose down failed (exit {:?}): {}",
                out.status.code(),
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(())
    }

    async fn ps(&self, compose_file: &Path) -> Result<Vec<ServiceStatus>, String> {
        let mut c = self.base_cmd(compose_file);
        c.arg("ps").arg("--format").arg("json");
        let out = c
            .output()
            .await
            .map_err(|e| format!("spawn docker compose ps: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "docker compose ps failed (exit {:?}): {}",
                out.status.code(),
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        parse_ps_json(&out.stdout)
    }
}

/// Parse NDJSON output of `docker compose ps --format json`.
/// Each line is a standalone JSON object (NOT a JSON array).
pub fn parse_ps_json(stdout: &[u8]) -> Result<Vec<ServiceStatus>, String> {
    let text = std::str::from_utf8(stdout).map_err(|e| format!("ps stdout utf8: {e}"))?;
    let mut rows = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let s: ServiceStatus = serde_json::from_str(line)
            .map_err(|e| format!("ps json line parse: {e} (line: {line})"))?;
        rows.push(s);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// In-memory driver for testing handlers without touching docker.
    pub struct MockComposeDriver {
        pub calls: Mutex<Vec<String>>,
        pub ps_rows: Vec<ServiceStatus>,
    }
    impl MockComposeDriver {
        pub fn new(ps_rows: Vec<ServiceStatus>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                ps_rows,
            }
        }
        fn record(&self, msg: String) {
            self.calls.lock().unwrap().push(msg);
        }
    }
    #[async_trait]
    impl ComposeDriver for MockComposeDriver {
        async fn up(&self, _f: &Path, services: &[&str]) -> Result<(), String> {
            self.record(format!("up {}", services.join(",")));
            Ok(())
        }
        async fn down(&self, _f: &Path, purge: bool) -> Result<(), String> {
            self.record(if purge {
                "down -v".into()
            } else {
                "down".into()
            });
            Ok(())
        }
        async fn ps(&self, _f: &Path) -> Result<Vec<ServiceStatus>, String> {
            self.record("ps".into());
            Ok(self.ps_rows.clone())
        }
    }

    #[test]
    fn ps_parses_json() {
        // Two NDJSON rows, one with health, one without (field absent).
        let body = concat!(
            r#"{"Service":"master-1","Name":"master-1","State":"running","Status":"Up 2 minutes (healthy)","Health":"healthy"}"#,
            "\n",
            r#"{"Service":"redis","Name":"redis","State":"running","Status":"Up 2 minutes"}"#,
        );
        let rows = parse_ps_json(body.as_bytes()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].service, "master-1");
        assert_eq!(rows[0].health, "healthy");
        assert_eq!(rows[1].service, "redis");
        assert_eq!(rows[1].health, ""); // default for absent field
    }

    #[tokio::test]
    async fn down_purge_adds_v_flag() {
        let d = MockComposeDriver::new(vec![]);
        d.down(Path::new("/x.yml"), true).await.unwrap();
        let calls = d.calls.lock().unwrap().clone();
        assert!(calls.iter().any(|c| c == "down -v"));
    }

    #[tokio::test]
    async fn up_passes_service_list() {
        let d = MockComposeDriver::new(vec![]);
        let svcs = ["master-1", "master-2"];
        d.up(Path::new("/x.yml"), &svcs).await.unwrap();
        let calls = d.calls.lock().unwrap().clone();
        assert!(calls.iter().any(|c| c.contains("master-1,master-2")));
    }
}
