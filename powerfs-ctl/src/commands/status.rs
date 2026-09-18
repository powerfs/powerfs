//! `powerfs-ctl status` — docker compose ps + per-master raft health probe.

use crate::compose::{ComposeDriver, DockerComposeDriver};
use crate::health::{MasterMetrics, MetricsProbe, ReqwestProbe};
use crate::home::Home;

pub async fn run(home: &Home) -> Result<(), String> {
    let cfg = home.load_cluster()?;
    let rc = cfg.validate().map_err(|e| e.to_string())?;
    let compose = home.rendered_compose();
    if !compose.exists() {
        return Err(format!(
            "{} missing — run `powerfs-ctl config render` first",
            compose.display()
        ));
    }

    let driver = DockerComposeDriver::new();
    let rows = driver.ps(&compose).await?;
    if rows.is_empty() {
        println!("no running services (cluster is down)");
    } else {
        println!("{:<14} {:<20} {:<10} HEALTH", "SERVICE", "NAME", "STATE");
        for r in &rows {
            println!(
                "{:<14} {:<20} {:<10} {}",
                r.service, r.name, r.state, r.health
            );
        }
    }

    println!("\nmaster raft health:");
    let probe = ReqwestProbe::new();
    for ip in &rc.master_ips {
        match probe.fetch(ip, 9300).await {
            Ok(m) => print_master(ip, &m),
            Err(e) => println!("  {:<16} UNREACHABLE ({})", ip, e),
        }
    }
    Ok(())
}

fn print_master(ip: &str, m: &MasterMetrics) {
    let role = if m.is_leader { "LEADER" } else { "follower" };
    let healthy = if m.healthz_ok { "ok" } else { "unhealthy" };
    println!(
        "  {:<16} {:<8} term={:<4} commit={:<4} applied={:<4} healthz={}",
        ip, role, m.term, m.commit_index, m.last_applied, healthy
    );
}
