//! `powerfs-ctl up` — render-if-stale → docker compose up -d → health gate.

use crate::compose::ComposeDriver;
use crate::health::{HealthGate, MetricsProbe};
use crate::home::Home;
use crate::schema::ResolvedCluster;
use std::collections::BTreeSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Host binaries the rendered compose bind-mounts into containers.
/// redis comes from a stock image and has no local binary.
/// FUSE is started outside compose (manual `docker run`), checked by enroll.
const SERVICE_BINARIES: &[(&str, &str)] = &[
    ("master-", "powerfs-master"),
    ("volume-", "powerfs-volume"),
    ("filer-", "powerfs-filer"),
    ("monitor", "powerfs-monitor"),
    ("s3", "powerfs-s3"),
];

fn binary_for_service(svc: &str) -> Option<&'static str> {
    SERVICE_BINARIES
        .iter()
        .find(|(prefix, _)| svc.starts_with(prefix))
        .map(|(_, bin)| *bin)
}

/// Locate the host `target/release` directory whose binaries the rendered
/// compose bind-mounts. Candidates, in order:
///   1. `POWERFS_BIN_DIR` (explicit override, also used by tests)
///   2. directory holding this very executable (installed layout / run from
///      target/release)
///   3. `./target/release` from the current directory (run from repo root)
///   4. `../target/release` (run from a workspace crate directory)
fn resolve_bin_dir() -> Option<std::path::PathBuf> {
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    if let Ok(d) = std::env::var("POWERFS_BIN_DIR") {
        candidates.push(d.into());
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.to_path_buf());
        }
    }
    candidates.push(Path::new("target/release").to_path_buf());
    candidates.push(Path::new("../target/release").to_path_buf());

    // A candidate counts only if at least one powerfs service binary is in
    // it — an existing but unrelated directory must not shadow later picks.
    candidates
        .into_iter()
        .find(|d| d.is_dir() && SERVICE_BINARIES.iter().any(|(_, b)| d.join(b).is_file()))
}

/// Fail early when a host binary bind-mounted by the rendered compose is
/// missing. Without this, Docker silently turns a missing bind-mount source
/// into a root-owned directory and the container dies with an opaque exec
/// error; a stale binary instead causes silent version skew (the ctl health
/// gate waiting on metrics an older master does not emit).
pub(crate) fn ensure_service_binaries(services: &[String]) -> Result<(), String> {
    let Some(bin_dir) = resolve_bin_dir() else {
        return Err(
            "could not find a target/release directory with powerfs binaries; \
             build all services from the repo root:\n  cargo build --release"
                .to_string(),
        );
    };
    check_binaries(services, &bin_dir)
}

/// Core check against an explicit bin directory (split out so tests can
/// inject a dir without mutating process-global environment).
fn check_binaries(services: &[String], bin_dir: &Path) -> Result<(), String> {
    // Empty service list means an unfiltered `compose up` — every binary
    // bind-mounted anywhere in the rendered file must exist.
    let needed: BTreeSet<&str> = if services.is_empty() {
        SERVICE_BINARIES.iter().map(|(_, b)| *b).collect()
    } else {
        services
            .iter()
            .filter_map(|s| binary_for_service(s))
            .collect()
    };

    let missing: Vec<String> = needed
        .into_iter()
        .map(|b| bin_dir.join(b))
        .filter(|p| !p.is_file())
        .map(|p| p.display().to_string())
        .collect();

    if missing.is_empty() {
        return Ok(());
    }
    Err(format!(
        "missing service binaries:\n  {}\nbin dir: {}\nthe rendered compose bind-mounts host binaries — build all services from the repo root:\n  cargo build --release",
        missing.join("\n  "),
        bin_dir.display()
    ))
}

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
    ensure_service_binaries(&svcs)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_prefix_maps_to_binary() {
        assert_eq!(binary_for_service("master-2"), Some("powerfs-master"));
        assert_eq!(binary_for_service("volume-7"), Some("powerfs-volume"));
        assert_eq!(binary_for_service("filer-1"), Some("powerfs-filer"));
        assert_eq!(binary_for_service("monitor"), Some("powerfs-monitor"));
        assert_eq!(binary_for_service("s3"), Some("powerfs-s3"));
        assert_eq!(binary_for_service("redis"), None);
    }

    fn temp_bin_dir(name: &str) -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!("{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn preflight_passes_when_all_bins_present() {
        let dir = temp_bin_dir("powerfs-preflight-ok");
        for (_, b) in SERVICE_BINARIES {
            std::fs::write(dir.join(b), b"").unwrap();
        }
        check_binaries(&[], &dir).unwrap();
        check_binaries(
            &[
                "master-1".into(),
                "filer-2".into(),
                "volume-3".into(),
                "redis".into(), // stock image, no binary required
            ],
            &dir,
        )
        .unwrap();
    }

    #[test]
    fn preflight_reports_missing_binaries_for_full_up() {
        // Dir containing only master leaves the rest missing for a full up.
        let dir = temp_bin_dir("powerfs-preflight-neg");
        std::fs::write(dir.join("powerfs-master"), b"").unwrap();

        let err = check_binaries(&[], &dir).unwrap_err();
        assert!(err.contains("powerfs-filer"), "{err}");
        assert!(err.contains("powerfs-volume"), "{err}");
        assert!(err.contains("cargo build --release"), "{err}");
        // master itself must NOT be listed
        assert!(!err.contains("powerfs-master\n"), "{err}");
    }
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
