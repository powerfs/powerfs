//! `powerfs-ctl doctor` — read-only cluster diagnostics.
//!
//! Every check produces one Finding (Ok / Warn / Error). The command never
//! mutates cluster state: `--fix` only re-renders stale configs (a local,
//! side-effect-free file regeneration). Findings are aggregated into a
//! report; any Error makes the process exit 1 so scripts/CI can consume it.

use crate::cert::{unix_to_ymd, ClientRegistry};
use crate::compose::ComposeDriver;
use crate::health::{inspect, RaftFinding};
use crate::home::Home;
use crate::schema::ResolvedCluster;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const METRICS_PORT: u16 = 9300;
const INSPECT_SETTLE: Duration = Duration::from_secs(1);
/// Cert lifetimes below this are flagged Warn even when still valid.
const CERT_WARN_WINDOW_SECS: u64 = 30 * 24 * 60 * 60;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Level {
    Ok,
    Warn,
    Error,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Finding {
    pub level: Level,
    pub check_name: &'static str,
    pub detail: String,
    pub hint: Option<String>,
}

impl Finding {
    fn ok(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            level: Level::Ok,
            check_name: name,
            detail: detail.into(),
            hint: None,
        }
    }
    fn warn(name: &'static str, detail: impl Into<String>, hint: &str) -> Self {
        Self {
            level: Level::Warn,
            check_name: name,
            detail: detail.into(),
            hint: Some(hint.to_string()),
        }
    }
    fn error(name: &'static str, detail: impl Into<String>, hint: &str) -> Self {
        Self {
            level: Level::Error,
            check_name: name,
            detail: detail.into(),
            hint: Some(hint.to_string()),
        }
    }
}

#[derive(Debug)]
pub struct Report {
    pub findings: Vec<Finding>,
}

impl Report {
    pub fn error_count(&self) -> usize {
        self.findings
            .iter()
            .filter(|f| f.level == Level::Error)
            .count()
    }
    pub fn warn_count(&self) -> usize {
        self.findings
            .iter()
            .filter(|f| f.level == Level::Warn)
            .count()
    }

    pub fn print(&self) {
        for f in &self.findings {
            let (icon, name) = match f.level {
                Level::Ok => ("✓", f.check_name),
                Level::Warn => ("⚠", f.check_name),
                Level::Error => ("✗", f.check_name),
            };
            println!("{icon} {name:<16} — {}", f.detail);
            if let Some(hint) = &f.hint {
                if f.level != Level::Ok {
                    println!("  → {hint}");
                }
            }
        }
        println!(
            "\n{} error(s), {} warning(s)",
            self.error_count(),
            self.warn_count()
        );
    }

    /// Test helper: all findings of one check at a given level.
    #[cfg(test)]
    pub fn findings_for(&self, name: &str, level: Level) -> Vec<&Finding> {
        self.findings
            .iter()
            .filter(|f| f.check_name == name && f.level == level)
            .collect()
    }
}

/// Entry point used by dispatch: diagnose, print, and return Err when any
/// Error-level finding exists (so main exits non-zero).
pub async fn run(
    home: &Home,
    fix: bool,
    driver: &dyn ComposeDriver,
    probe: &dyn crate::health::MetricsProbe,
) -> Result<(), String> {
    let report = diagnose(home, fix, driver, probe).await?;
    report.print();
    let (errors, warns) = (report.error_count(), report.warn_count());
    if errors > 0 {
        return Err(format!(
            "doctor found {errors} error(s), {warns} warning(s)"
        ));
    }
    Ok(())
}

/// Run all checks. Returns findings even when problems are found; Err is
/// reserved for unexpected driver/IO failures (most failures are recorded
/// as findings instead so the report stays complete).
pub async fn diagnose(
    home: &Home,
    fix: bool,
    driver: &dyn ComposeDriver,
    probe: &dyn crate::health::MetricsProbe,
) -> Result<Report, String> {
    let mut findings = Vec::new();

    // --- R1: cluster.toml -------------------------------------------------
    let rc = check_cluster_config(home, &mut findings);

    // --- R2: rendered config ---------------------------------------------
    let compose_ready = check_rendered(home, fix, &mut findings).await;

    // --- R3: docker engine + containers ----------------------------------
    let master_container_running = if compose_ready {
        check_containers(home, driver, rc.as_ref(), &mut findings).await?
    } else {
        false
    };

    // --- R4: master raft --------------------------------------------------
    if let Some(rc) = rc.as_ref() {
        let finding = inspect(probe, METRICS_PORT, &rc.master_ips, INSPECT_SETTLE).await;
        classify_raft(finding, master_container_running, &mut findings);
    }

    // --- R5: CA certificate ----------------------------------------------
    check_ca(home, &mut findings);

    // --- R6: issued cert expiry / revocation -----------------------------
    check_cert_registry(home, &mut findings);

    Ok(Report { findings })
}

fn check_cluster_config(home: &Home, out: &mut Vec<Finding>) -> Option<ResolvedCluster> {
    match home.load_cluster() {
        Err(e) => {
            out.push(Finding::error(
                "cluster.toml",
                e,
                "run `powerfs-ctl init` to generate the skeleton",
            ));
            None
        }
        Ok(cfg) => match cfg.validate() {
            Err(e) => {
                out.push(Finding::error(
                    "cluster.toml",
                    e.to_string(),
                    "fix the invalid field; see `powerfs-ctl config check`",
                ));
                None
            }
            Ok(rc) => {
                out.push(Finding::ok(
                    "cluster.toml",
                    format!(
                        "valid ({} profile, {} master / {} filer / {} volume)",
                        profile_name(&rc),
                        rc.master_ips.len(),
                        rc.filer_ips.len(),
                        rc.volume_ips.len()
                    ),
                ));
                Some(rc)
            }
        },
    }
}

fn profile_name(rc: &ResolvedCluster) -> &'static str {
    match rc.cfg.cluster.profile {
        crate::schema::Profile::Simple => "simple",
        crate::schema::Profile::Ha => "ha",
        crate::schema::Profile::Rdma => "rdma",
    }
}

/// Returns true when the compose file exists and the remaining runtime
/// checks can proceed.
async fn check_rendered(home: &Home, fix: bool, out: &mut Vec<Finding>) -> bool {
    let compose = home.rendered_compose();
    if !compose.exists() {
        out.push(Finding::error(
            "rendered config",
            format!("{} is missing", compose.display()),
            "run `powerfs-ctl config render`",
        ));
        return false;
    }
    if !crate::commands::up::render_is_stale(home) {
        out.push(Finding::ok("rendered config", "up to date"));
        return true;
    }
    if !fix {
        out.push(Finding::warn(
            "rendered config",
            "stale: cluster.toml is newer than the rendered compose file",
            "run `powerfs-ctl config render`, or re-run with --fix",
        ));
        return true;
    }
    // The only thing --fix is allowed to touch: local file regeneration.
    match crate::commands::config_render::run(home).await {
        Ok(()) => {
            out.push(Finding::ok(
                "rendered config",
                "re-rendered (was stale; fixed)",
            ));
            true
        }
        Err(e) => {
            out.push(Finding::error(
                "rendered config",
                format!("stale and --fix re-render failed: {e}"),
                "run `powerfs-ctl config render` to see the render error",
            ));
            true
        }
    }
}

/// Returns true when at least one master-* container reports running.
async fn check_containers(
    home: &Home,
    driver: &dyn ComposeDriver,
    rc: Option<&ResolvedCluster>,
    out: &mut Vec<Finding>,
) -> Result<bool, String> {
    let compose = home.rendered_compose();
    let rows = match driver.ps(&compose).await {
        Ok(rows) => rows,
        Err(e) => {
            out.push(Finding::error(
                "docker",
                format!("`docker compose ps` failed: {e}"),
                "check the docker daemon and current user's permissions",
            ));
            return Ok(false);
        }
    };

    let present: std::collections::HashSet<&str> =
        rows.iter().map(|r| r.service.as_str()).collect();
    let mut master_running = false;
    let mut problems = Vec::new();
    for r in &rows {
        if r.service.starts_with("master-") && r.state == "running" {
            master_running = true;
        }
        if r.state != "running" {
            problems.push(format!("{} is {}", r.service, r.state));
        }
        if r.health == "unhealthy" {
            problems.push(format!("{} reports unhealthy", r.service));
        }
    }

    if let Some(rc) = rc {
        for expected in expected_services(rc) {
            if !present.contains(expected.as_str()) {
                out.push(Finding::warn(
                    "containers",
                    format!("expected service '{expected}' is not present"),
                    "a partial start is fine; otherwise run `powerfs-ctl up`",
                ));
            }
        }
    }

    if problems.is_empty() {
        out.push(Finding::ok(
            "containers",
            format!("{} service(s) reported by compose", rows.len()),
        ));
    } else {
        for p in problems {
            out.push(Finding::error(
                "containers",
                p,
                "inspect with `powerfs-ctl logs --role <service>`",
            ));
        }
    }
    Ok(master_running)
}

/// Service names the rendered project is expected to contain. The compose
/// template always renders redis/monitor/s3 and one service per resolved IP
/// for master/volume/filer, so this is profile-independent.
fn expected_services(rc: &ResolvedCluster) -> Vec<String> {
    let mut names = Vec::new();
    for (prefix, len) in [
        ("master-", rc.master_ips.len()),
        ("volume-", rc.volume_ips.len()),
        ("filer-", rc.filer_ips.len()),
    ] {
        for i in 1..=len {
            names.push(format!("{prefix}{i}"));
        }
    }
    names.extend(["redis", "monitor", "s3"].into_iter().map(str::to_string));
    names
}

fn classify_raft(finding: RaftFinding, master_container_running: bool, out: &mut Vec<Finding>) {
    let name = "master raft";
    match finding {
        RaftFinding::Healthy {
            leader,
            term,
            commit_index,
        } => out.push(Finding::ok(
            name,
            format!("leader {leader} term={term} commit_index={commit_index}"),
        )),
        RaftFinding::Zombie { leader } => out.push(Finding::error(
            name,
            format!(
                "zombie/fake leader {leader}: it claims leadership but cannot \
                 commit (heartbeats round-trip while data packets are dropped)"
            ),
            "check MTU/firewall asymmetry; see M2 health-gate runbook",
        )),
        RaftFinding::SplitBrain { leaders } => out.push(Finding::error(
            name,
            format!("split-brain: {} masters claim leadership", leaders.join(", ")),
            "quorum/network partition — do not restart services blindly",
        )),
        RaftFinding::ApplierLag {
            leader,
            last_applied,
            commit_index,
        } => out.push(Finding::error(
            name,
            format!(
                "state-machine applier stalled on {leader}: last_applied={last_applied} < commit_index={commit_index}"
            ),
            "check leader rocksdb IO and logs",
        )),
        RaftFinding::ElectionStorm {
            leader,
            term_from,
            term_to,
        } => out.push(Finding::error(
            name,
            format!("election storm on {leader}: term moved {term_from}->{term_to} between samples"),
            "check connectivity between master nodes",
        )),
        RaftFinding::NoLeader => {
            if master_container_running {
                out.push(Finding::error(
                    name,
                    "no leader elected even though master containers are running",
                    "quorum may be lost; inspect master logs",
                ));
            } else {
                out.push(Finding::warn(
                    name,
                    "no leader (no running master containers — cluster stopped?)",
                    "start with `powerfs-ctl up`",
                ));
            }
        }
        RaftFinding::ProbeFailure { errors } => {
            let detail = errors
                .first()
                .map(|(ip, e)| format!("all master probes failed ({ip}: {e})"))
                .unwrap_or_else(|| "all master probes failed".to_string());
            if master_container_running {
                out.push(Finding::error(
                    name,
                    detail,
                    "metrics port unreachable on a running master?",
                ));
            } else {
                out.push(Finding::warn(name, format!("{detail} (masters not running)"), ""));
            }
        }
    }
}

fn check_ca(home: &Home, out: &mut Vec<Finding>) {
    let ca = home.certs_dir().join("ca.crt");
    if ca.exists() {
        out.push(Finding::ok("CA certificate", "ca.crt present"));
    } else {
        out.push(Finding::warn(
            "CA certificate",
            "ca.crt is missing",
            "run `powerfs-ctl cert init-ca` once the master is up",
        ));
    }
}

fn check_cert_registry(home: &Home, out: &mut Vec<Finding>) {
    let p = home.certs_dir().join("client_registry.json");
    // No registry is normal outside the master host — skip silently, exactly
    // like `cert list`'s deployment constraint.
    if !p.exists() {
        return;
    }
    let body = match std::fs::read_to_string(&p) {
        Ok(s) => s,
        Err(e) => {
            out.push(Finding::warn(
                "cert expiry",
                format!("cannot read {}: {e}", p.display()),
                "",
            ));
            return;
        }
    };
    let reg: ClientRegistry = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => {
            out.push(Finding::warn(
                "cert expiry",
                format!("cannot parse client_registry.json: {e}"),
                "master may have changed the registry schema",
            ));
            return;
        }
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let entries = reg.sorted_entries();
    let mut problems = 0;
    for e in &entries {
        let (y, m, d) = unix_to_ymd(e.expires_at);
        let when = format!("{y}-{m:02}-{d:02}");
        if e.revoked {
            out.push(Finding::warn(
                "cert expiry",
                format!("{} is revoked (still listed in registry)", e.client_name),
                "",
            ));
            problems += 1;
        } else if e.expires_at < now {
            out.push(Finding::error(
                "cert expiry",
                format!("{} EXPIRED on {when}", e.client_name),
                "issue a replacement with `powerfs-ctl cert issue`",
            ));
            problems += 1;
        } else if e.expires_at < now + CERT_WARN_WINDOW_SECS {
            out.push(Finding::warn(
                "cert expiry",
                format!("{} expires on {when} (within 30 days)", e.client_name),
                "plan renewal; master has no renew endpoint yet",
            ));
            problems += 1;
        }
    }
    if problems == 0 {
        out.push(Finding::ok(
            "cert expiry",
            format!("{} issued cert(s) within validity", entries.len()),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::test_support::test_probe::TestProbe;
    use crate::compose::tests::MockComposeDriver;
    use crate::compose::ServiceStatus;

    fn row(service: &str, state: &str, health: &str) -> ServiceStatus {
        ServiceStatus {
            service: service.into(),
            name: service.into(),
            state: state.into(),
            status: String::new(),
            health: health.into(),
        }
    }

    /// HA home: full healthy service set + compose placeholder + CA + a
    /// long-lived registry entry. Caller may pass extras to override.
    fn healthy_ha_home() -> Home {
        let (home, _dir) = crate::commands::test_support::home_with_cluster();
        std::fs::write(home.rendered_compose(), "# placeholder").unwrap();
        std::fs::write(home.certs_dir().join("ca.crt"), "CA").unwrap();
        // expires far in the future (year 2030).
        write_registry(
            &home,
            r#"{"by_fingerprint":{"fp":{"client_name":"filer-1","san_ips":["172.30.0.31"],"mount_dirs":[],"issued_at":0,"expires_at":1893456000,"cert_fingerprint_sha256":"fp","revoked":false}},"by_client_name":{}}"#,
        );
        home
    }

    fn ha_ps_rows() -> Vec<ServiceStatus> {
        let mut rows = Vec::new();
        for s in ["master-1", "master-2", "master-3"] {
            rows.push(row(s, "running", "healthy"));
        }
        for i in 1..=6 {
            rows.push(row(&format!("volume-{i}"), "running", ""));
        }
        for i in 1..=3 {
            rows.push(row(&format!("filer-{i}"), "running", "healthy"));
        }
        for s in ["redis", "monitor", "s3"] {
            rows.push(row(s, "running", ""));
        }
        rows
    }

    fn write_registry(home: &Home, json: &str) {
        std::fs::write(home.certs_dir().join("client_registry.json"), json).unwrap();
    }

    fn registry_entry(name: &str, expires_at: u64, revoked: bool) -> String {
        format!(
            r#"{{"by_fingerprint":{{"fp_{name}":{{"client_name":"{name}","san_ips":["172.30.0.99"],"mount_dirs":["/mnt/powerfs"],"issued_at":0,"expires_at":{expires_at},"cert_fingerprint_sha256":"fp_{name}","revoked":{revoked}}}}},"by_client_name":{{}}}}"#,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn doctor_clean_cluster_ok() {
        let home = healthy_ha_home();
        let driver = MockComposeDriver::new(ha_ps_rows());
        let probe = TestProbe::new(&["172.30.0.12"], false);

        let report = diagnose(&home, false, &driver, &probe).await.unwrap();
        assert_eq!(report.error_count(), 0, "errors: {:#?}", report.findings);
        assert_eq!(report.warn_count(), 0, "warnings: {:#?}", report.findings);
        assert!(report
            .findings
            .iter()
            .any(|f| f.check_name == "master raft" && f.level == Level::Ok));
    }

    #[tokio::test(start_paused = true)]
    async fn doctor_missing_cluster_toml() {
        let (home, _dir) = crate::commands::test_support::home_empty();
        let driver = MockComposeDriver::new(vec![]);
        let probe = TestProbe::new(&[], false);

        let report = diagnose(&home, false, &driver, &probe).await.unwrap();
        let errs = report.findings_for("cluster.toml", Level::Error);
        assert_eq!(errs.len(), 1);
        assert!(errs[0].hint.as_ref().unwrap().contains("init"));
    }

    #[tokio::test(start_paused = true)]
    async fn doctor_stale_render_warn_then_fix() {
        let (home, _dir) = crate::commands::test_support::home_with_cluster();
        // compose written first, then cluster.toml touched later → stale.
        std::fs::write(home.rendered_compose(), "# old").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let toml = std::fs::read_to_string(home.cluster_toml()).unwrap();
        std::fs::write(home.cluster_toml(), toml).unwrap();

        // without --fix: one Warn.
        let driver = MockComposeDriver::new(vec![]);
        let probe = TestProbe::new(&[], false);
        let report = diagnose(&home, false, &driver, &probe).await.unwrap();
        assert_eq!(report.findings_for("rendered config", Level::Warn).len(), 1);

        // with --fix: re-rendered, finding is Ok and file actually replaced.
        let report = diagnose(&home, true, &driver, &probe).await.unwrap();
        let fixed = report.findings_for("rendered config", Level::Ok);
        assert_eq!(fixed.len(), 1, "got: {:#?}", report.findings);
        assert!(fixed[0].detail.contains("fixed"));
        assert!(!std::fs::read_to_string(home.rendered_compose())
            .unwrap()
            .contains("# old"));
    }

    #[tokio::test(start_paused = true)]
    async fn doctor_unhealthy_container_error() {
        let home = healthy_ha_home();
        let mut rows = ha_ps_rows();
        rows.push(row("filer-2", "running", "unhealthy"));
        // replace the healthy filer-2 row too
        rows.retain(|r| !(r.service == "filer-2" && r.health == "healthy"));
        let driver = MockComposeDriver::new(rows);
        let probe = TestProbe::new(&["172.30.0.12"], false);

        let report = diagnose(&home, false, &driver, &probe).await.unwrap();
        let errs = report.findings_for("containers", Level::Error);
        assert!(
            errs.iter().any(|f| f.detail.contains("unhealthy")),
            "got: {errs:?}"
        );
        // same setup through run(): Error findings must surface as Err/exit 1
        assert!(run(&home, false, &driver, &probe).await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn doctor_zombie_is_error() {
        let home = healthy_ha_home();
        let driver = MockComposeDriver::new(ha_ps_rows());
        // leader exists but commit_index == 0.
        let probe = TestProbe::new(&["172.30.0.12"], true);

        let report = diagnose(&home, false, &driver, &probe).await.unwrap();
        let errs = report.findings_for("master raft", Level::Error);
        assert_eq!(errs.len(), 1, "got: {:#?}", report.findings);
        assert!(errs[0].detail.contains("zombie"), "got: {errs:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn doctor_no_leader_without_containers_is_only_warn() {
        let home = healthy_ha_home();
        // no rows → nothing running → NoLeader should degrade to Warn.
        let driver = MockComposeDriver::new(vec![]);
        let probe = TestProbe::new(&[], false);

        let report = diagnose(&home, false, &driver, &probe).await.unwrap();
        assert_eq!(
            report.findings_for("master raft", Level::Warn).len(),
            1,
            "got: {:#?}",
            report.findings
        );
        assert_eq!(report.findings_for("master raft", Level::Error).len(), 0);
        // missing expected services are also only warnings
        assert!(report.error_count() == 0);
    }

    #[tokio::test(start_paused = true)]
    async fn doctor_all_probes_failing_without_containers_warns() {
        let home = healthy_ha_home();
        let driver = MockComposeDriver::new(vec![]);
        let probe = TestProbe::unreachable();

        let report = diagnose(&home, false, &driver, &probe).await.unwrap();
        let warns = report.findings_for("master raft", Level::Warn);
        assert_eq!(warns.len(), 1, "got: {:#?}", report.findings);
        assert!(
            warns[0].detail.contains("connection refused"),
            "got: {warns:?}"
        );
        assert_eq!(report.findings_for("master raft", Level::Error).len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn doctor_probe_failure_with_running_masters_is_error() {
        let home = healthy_ha_home();
        let driver = MockComposeDriver::new(ha_ps_rows());
        let probe = TestProbe::unreachable();

        let report = diagnose(&home, false, &driver, &probe).await.unwrap();
        let errs = report.findings_for("master raft", Level::Error);
        assert_eq!(errs.len(), 1, "got: {:#?}", report.findings);
        assert!(errs[0].detail.contains("connection refused"));
    }

    #[tokio::test(start_paused = true)]
    async fn doctor_expired_cert_error() {
        let home = healthy_ha_home();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        write_registry(&home, &registry_entry("fuse-old", now - 10, false));
        let driver = MockComposeDriver::new(ha_ps_rows());
        let probe = TestProbe::new(&["172.30.0.12"], false);

        let report = diagnose(&home, false, &driver, &probe).await.unwrap();
        let errs = report.findings_for("cert expiry", Level::Error);
        assert!(
            errs.iter().any(|f| f.detail.contains("EXPIRED")),
            "got: {errs:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn doctor_expiring_soon_warn() {
        let home = healthy_ha_home();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        write_registry(&home, &registry_entry("fuse-soon", now + 10 * 86400, false));
        let driver = MockComposeDriver::new(ha_ps_rows());
        let probe = TestProbe::new(&["172.30.0.12"], false);

        let report = diagnose(&home, false, &driver, &probe).await.unwrap();
        assert_eq!(
            report.findings_for("cert expiry", Level::Warn).len(),
            1,
            "got: {:#?}",
            report.findings
        );
        assert_eq!(report.findings_for("cert expiry", Level::Error).len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn doctor_error_and_warn_totals() {
        let home = healthy_ha_home();
        let mut rows = ha_ps_rows();
        rows.push(row("filer-9", "exited", ""));
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        write_registry(&home, &registry_entry("fuse-soon", now + 5 * 86400, false));
        let driver = MockComposeDriver::new(rows);
        let probe = TestProbe::new(&["172.30.0.12"], false);

        let report = diagnose(&home, false, &driver, &probe).await.unwrap();
        // filer-9 exited → Error; expiring cert → Warn; filer-9 also missing
        // from expected set? no — it's present but exited.
        assert!(report.error_count() >= 1);
        assert!(report.warn_count() >= 1);
    }
}
