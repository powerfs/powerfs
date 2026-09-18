//! HTTP probe for master /metrics + /healthz. Parses Prometheus text
//! exposition format by hand (we only need 4 gauges; pulling a full prom
//! parser crate would be overkill). The parser is split out as a pure fn
//! so tests can feed canned bodies without touching the network.

use crate::health::MasterMetrics;

pub struct ReqwestProbe {
    client: reqwest::Client,
}

impl ReqwestProbe {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(3))
                .build()
                .expect("reqwest client build"),
        }
    }

    async fn fetch_text(&self, ip: &str, port: u16, path: &str) -> Result<String, String> {
        let url = format!("http://{ip}:{port}{path}");
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url}: {e}"))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| format!("read {url} body: {e}"))?;
        if !status.is_success() {
            return Err(format!("GET {url} → HTTP {}", status));
        }
        Ok(body)
    }
}

#[async_trait::async_trait]
impl crate::health::MetricsProbe for ReqwestProbe {
    async fn fetch(&self, ip: &str, port: u16) -> Result<MasterMetrics, String> {
        let metrics_body = self.fetch_text(ip, port, "/metrics").await?;
        let mut m = parse_prometheus(&metrics_body)?;
        // healthz is a separate probe: 200 == raft available, 503 == fake leader.
        let healthz_ok = match self
            .client
            .get(format!("http://{ip}:{port}/healthz"))
            .send()
            .await
        {
            Ok(r) => r.status().is_success(),
            Err(_) => false,
        };
        m.healthz_ok = healthz_ok;
        Ok(m)
    }
}

/// Parse the subset of Prometheus exposition text we care about.
/// Looks for gauge lines named exactly:
///   powerfs_is_leader, powerfs_raft_term,
///   powerfs_raft_commit_index, powerfs_raft_last_applied
///
/// Format per line:  NAME[{LABELS}] VALUE  — the value is always the last
/// whitespace-separated token; the metric name is the first token, with any
/// `{...}` label block stripped. Comment lines (`#`) are skipped.
pub fn parse_prometheus(text: &str) -> Result<MasterMetrics, String> {
    let mut is_leader: Option<f64> = None;
    let mut term: Option<f64> = None;
    let mut commit: Option<f64> = None;
    let mut applied: Option<f64> = None;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // value = last whitespace token on the line.
        let value_tok = match line.split_whitespace().last() {
            Some(v) => v,
            None => continue,
        };
        let val: f64 = match value_tok.parse() {
            Ok(x) => x,
            Err(_) => continue, // not a sample line (e.g. HELP/TYPE handled above)
        };
        // metric name = first token, with any `{...}` stripped.
        let name = line.split_whitespace().next().unwrap_or("");
        let name = name.split('{').next().unwrap_or("");
        match name {
            "powerfs_is_leader" => is_leader = Some(val),
            "powerfs_raft_term" => term = Some(val),
            "powerfs_raft_commit_index" => commit = Some(val),
            "powerfs_raft_last_applied" => applied = Some(val),
            _ => {}
        }
    }

    let take = |opt: Option<f64>, field: &str| -> Result<f64, String> {
        opt.ok_or_else(|| format!("metric {field} missing from /metrics"))
    };
    Ok(MasterMetrics {
        is_leader: take(is_leader, "powerfs_is_leader")? != 0.0,
        term: take(term, "powerfs_raft_term")? as u64,
        commit_index: take(commit, "powerfs_raft_commit_index")? as u64,
        last_applied: take(applied, "powerfs_raft_last_applied")? as u64,
        healthz_ok: false, // filled by fetch()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_NO_LABELS: &str = r#"# HELP powerfs_raft_term Current Raft term
# TYPE powerfs_raft_term gauge
powerfs_raft_term 4
# HELP powerfs_is_leader 1 if this node is leader, 0 otherwise
# TYPE powerfs_is_leader gauge
powerfs_is_leader 1
# HELP powerfs_raft_commit_index Raft commit index (local node view)
# TYPE powerfs_raft_commit_index gauge
powerfs_raft_commit_index 7
# HELP powerfs_raft_last_applied Last applied log index (local node view)
# TYPE powerfs_raft_last_applied gauge
powerfs_raft_last_applied 7
"#;

    // Hypothetical label variant — our parser must still pick the value.
    const SAMPLE_WITH_LABELS: &str = r#"powerfs_raft_term{node="m1"} 5
powerfs_is_leader{node="m1"} 1
powerfs_raft_commit_index{node="m1"} 9
powerfs_raft_last_applied{node="m1"} 9
"#;

    #[test]
    fn parse_no_labels() {
        let m = parse_prometheus(SAMPLE_NO_LABELS).unwrap();
        assert!(m.is_leader);
        assert_eq!(m.term, 4);
        assert_eq!(m.commit_index, 7);
        assert_eq!(m.last_applied, 7);
        assert!(!m.healthz_ok); // default until fetch() fills it
    }

    #[test]
    fn parse_with_labels() {
        let m = parse_prometheus(SAMPLE_WITH_LABELS).unwrap();
        assert!(m.is_leader);
        assert_eq!(m.term, 5);
        assert_eq!(m.commit_index, 9);
        assert_eq!(m.last_applied, 9);
    }

    #[test]
    fn parse_missing_metric_errors() {
        let body = "powerfs_raft_term 1\n";
        let err = parse_prometheus(body).unwrap_err();
        assert!(err.contains("powerfs_is_leader"), "got: {err}");
    }

    #[test]
    fn parse_follower_view() {
        let body = "powerfs_raft_term 3\npowerfs_is_leader 0\n\
                    powerfs_raft_commit_index 2\npowerfs_raft_last_applied 2\n";
        let m = parse_prometheus(body).unwrap();
        assert!(!m.is_leader);
    }
}
