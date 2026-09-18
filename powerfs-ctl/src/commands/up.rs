//! `powerfs-ctl up` — render-if-stale → docker compose up -d → health gate.

use crate::compose::ComposeDriver;
use crate::health::{HealthGate, MetricsProbe};
use crate::home::Home;
use crate::schema::ResolvedCluster;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Map a `--role` filter (e.g. "master", "master-1", "volume") into concrete
/// compose service names. Empty role = all services (empty slice → compose up
/// with no service filter starts everything).
pub fn services_for_role(role: &Option<String>, rc: &ResolvedCluster) -> Vec<String> {
    let Some(r) = role else {
        return Vec::new();
    };
    // exact service name (e.g. "master-1") → pass through
    if r.contains('-') {
        return vec![r.clone()];
    }
    match r.as_str() {
        "master" => (0..rc.master_ips.len())
            .map(|i| format!("master-{}", i + 1))
            .collect(),
        "volume" => (0..rc.volume_ips.len())
            .map(|i| format!("volume-{}", i + 1))
            .collect(),
        "filer" => (0..rc.filer_ips.len())
            .map(|i| format!("filer-{}", i + 1))
            .collect(),
        "redis" => vec!["redis".into()],
        "monitor" => vec!["monitor".into()],
        "s3" => vec!["s3".into()],
        _ => vec![r.clone()],
    }
}

/// True iff cluster.toml is newer than the rendered compose file, or the
/// compose file doesn't exist — either way we need to re-render before up.
pub(crate) fn render_is_stale(home: &Home) -> bool {
    let compose = home.rendered_compose();
    let cluster = home.cluster_toml();
    match (compose.metadata().ok(), cluster.metadata().ok()) {
        (None, _) => true,
        (_, None) => false,
        (Some(c), Some(k)) => c.modified().ok() < k.modified().ok(),
    }
}

pub async fn run<P: MetricsProbe>(
    home: &Home,
    role: Option<String>,
    driver: &dyn ComposeDriver,
    probe: &P,
) -> Result<(), String> {
    let cfg = home.load_cluster()?;
    let rc = cfg.validate().map_err(|e| e.to_string())?;

    if render_is_stale(home) {
        println!("• rendered compose is stale or missing; re-rendering...");
        super::config_render::run(home).await?;
    }
    let compose = home.rendered_compose();
    if !compose.exists() {
        return Err(format!(
            "{} missing — run `powerfs-ctl config render` first",
            compose.display()
        ));
    }

    let svcs = services_for_role(&role, &rc);
    let svc_args: Vec<&str> = svcs.iter().map(|s| s.as_str()).collect();
    println!("• docker compose up -d {}", svcs.join(" "));
    driver.up(&compose, &svc_args).await?;

    // Health gate only meaningful when the full master quorum is started.
    // Partial `--role master-1` can't form quorum, so skip and warn.
    let full_master = role.is_none()
        || svcs.len() == rc.master_ips.len() && svcs.iter().all(|s| s.starts_with("master-"));
    if !full_master {
        println!(
            "⚠ partial start (--role {}); skipping health gate (no quorum)",
            role.unwrap()
        );
        return Ok(());
    }

    println!("• waiting for master quorum to elect a healthy leader...");
    let gate = HealthGate::new(9300);
    let info = gate.run(probe, &rc.master_ips).await?;
    println!(
        "✓ leader elected: {} (term={}, commit_index={})",
        info.ip, info.term, info.commit_index
    );

    write_state(home, &info).map_err(|e| format!("write state.json: {e}"))?;
    Ok(())
}

fn write_state(home: &Home, info: &crate::health::LeaderInfo) -> std::io::Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let body = serde_json::json!({
        "started_at": now,
        "leader_ip": info.ip,
        "leader_term": info.term,
        "leader_commit_index": info.commit_index,
    });
    home.write_file(Path::new("state.json"), &body.to_string())
}
