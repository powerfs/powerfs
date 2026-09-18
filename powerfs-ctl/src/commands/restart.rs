//! `powerfs-ctl restart` — rolling restart with a raft health gate between
//! master nodes. Followers go first, the current leader last (same discipline
//! as etcd/Ceph rollouts), so the cluster goes through at most one election
//! instead of two. Non-master roles restart in one batch without a raft gate
//! (M4 only gates the master quorum; filer-local raft is out of scope).

use crate::commands::up::services_for_role;
use crate::compose::ComposeDriver;
use crate::health::{sample_all, HealthGate, MetricsProbe};
use crate::home::Home;
use crate::schema::ResolvedCluster;
use std::path::Path;

const METRICS_PORT: u16 = 9300;

pub async fn run(
    home: &Home,
    role: Option<String>,
    force: bool,
    driver: &dyn ComposeDriver,
    probe: &dyn MetricsProbe,
) -> Result<(), String> {
    let cfg = home.load_cluster()?;
    let rc = cfg.validate().map_err(|e| e.to_string())?;
    let compose = home.rendered_compose();
    if !compose.exists() {
        return Err(format!(
            "{} missing — run `powerfs-ctl config render` first",
            compose.display()
        ));
    }

    match role.as_deref() {
        None => {
            // whole cluster: gated master rollout, then whatever data-plane
            // services compose actually reports (profile-agnostic).
            let all_masters: Vec<String> = (0..rc.master_ips.len())
                .map(|i| format!("master-{}", i + 1))
                .collect();
            roll_masters(driver, probe, &rc, &compose, &all_masters, force).await?;
            restart_running_non_masters(driver, &compose).await;
            println!("✓ cluster restarted");
        }
        Some("master") => {
            let all_masters: Vec<String> = (0..rc.master_ips.len())
                .map(|i| format!("master-{}", i + 1))
                .collect();
            roll_masters(driver, probe, &rc, &compose, &all_masters, force).await?;
            println!("✓ master quorum restarted");
        }
        Some(name) if name.starts_with("master-") => {
            // one specific master — gate the full quorum afterwards anyway.
            roll_masters(driver, probe, &rc, &compose, &[name.to_string()], force).await?;
            println!("✓ {name} restarted");
        }
        Some(_) => {
            let svcs = services_for_role(&role, &rc);
            let args: Vec<&str> = svcs.iter().map(|s| s.as_str()).collect();
            println!("• docker compose restart {}", svcs.join(" "));
            driver.restart(&compose, &args).await?;
            println!("⚠ non-master role restarted without a raft health gate");
        }
    }
    Ok(())
}

/// Restart the given master services one at a time, followers before the
/// leader, gating the quorum after each one (unless `force`).
async fn roll_masters(
    driver: &dyn ComposeDriver,
    probe: &dyn MetricsProbe,
    rc: &ResolvedCluster,
    compose: &Path,
    targets: &[String],
    force: bool,
) -> Result<(), String> {
    let leader = locate_leader_service(probe, &rc.master_ips).await;
    if leader.is_none() {
        eprintln!(
            "  warn: could not identify the current leader; rolling in \
             service-name order"
        );
    }
    let order = follower_first_order(targets, leader.as_deref());

    if force {
        eprintln!("  warn: --force skips the per-node health gate");
    }

    let mut done: Vec<String> = Vec::new();
    for svc in order {
        println!("• restart {svc}");
        driver.restart(compose, &[svc.as_str()]).await?;
        done.push(svc.clone());

        if force {
            continue;
        }
        // wait for the *whole quorum* to recover, not just this node.
        let gate = HealthGate::new(METRICS_PORT);
        if let Err(e) = gate.run(probe, &rc.master_ips).await {
            let pending: Vec<&str> = targets
                .iter()
                .filter(|t| !done.contains(t))
                .map(|t| t.as_str())
                .collect();
            return Err(format!(
                "rolling restart halted after '{svc}': health gate failed: {e}\n\
                 already restarted: [{}]\n  not yet restarted: [{}]\n\
                 fix the raft problem, then resume with \
                 `powerfs-ctl restart --role <next-service>`",
                done.join(", "),
                pending.join(", ")
            ));
        }
        println!("  ✓ quorum healthy after {svc}");
    }
    Ok(())
}

/// Best-effort lookup of which compose service currently hosts the leader.
/// Returns None on probe failure or when no node claims leadership.
async fn locate_leader_service(probe: &dyn MetricsProbe, master_ips: &[String]) -> Option<String> {
    let samples = sample_all(probe, METRICS_PORT, master_ips).await;
    for (ip, res) in samples {
        if let Ok(m) = res {
            if m.is_leader {
                let idx = master_ips.iter().position(|x| x == &ip)?;
                return Some(format!("master-{}", idx + 1));
            }
        }
    }
    None
}

/// Sort restart targets so the leader (if it is among them) goes last.
fn follower_first_order(targets: &[String], leader: Option<&str>) -> Vec<String> {
    let mut followers: Vec<String> = targets
        .iter()
        .filter(|t| Some(t.as_str()) != leader)
        .cloned()
        .collect();
    followers.sort();
    if let Some(l) = leader {
        if targets.iter().any(|t| t == l) {
            followers.push(l.to_string());
        }
    }
    followers
}

/// Restart every *running* non-master service reported by `ps`, in three
/// batches: filers, volumes, then infra (redis/monitor/s3/...). Services the
/// compose project doesn't declare never appear in ps, so this works across
/// simple/ha profiles without hard-coding service names. Stopped containers
/// are left alone — bringing services up is `powerfs-ctl up`'s job.
async fn restart_running_non_masters(driver: &dyn ComposeDriver, compose: &Path) {
    let rows = match driver.ps(compose).await {
        Ok(rows) => rows,
        Err(e) => {
            eprintln!("  warn: `ps` failed ({e}); skipping data-plane restart phase");
            return;
        }
    };
    let mut filers = Vec::new();
    let mut volumes = Vec::new();
    let mut others = Vec::new();
    for r in rows {
        if r.state != "running" || r.service.starts_with("master-") {
            continue;
        }
        if r.service.starts_with("filer-") {
            filers.push(r.service);
        } else if r.service.starts_with("volume-") {
            volumes.push(r.service);
        } else {
            others.push(r.service);
        }
    }
    for (label, batch) in [("filer", filers), ("volume", volumes), ("infra", others)] {
        if batch.is_empty() {
            continue;
        }
        let args: Vec<&str> = batch.iter().map(|s| s.as_str()).collect();
        println!("• restart {label} services: {}", batch.join(" "));
        if let Err(e) = driver.restart(compose, &args).await {
            eprintln!("  warn: {label} restart failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::test_support::test_probe::TestProbe;
    use crate::compose::tests::MockComposeDriver;
    use crate::compose::ServiceStatus;
    use std::sync::atomic::Ordering;

    fn ps_row(service: &str, state: &str) -> ServiceStatus {
        ServiceStatus {
            service: service.into(),
            name: service.into(),
            state: state.into(),
            status: String::new(),
            health: if state == "running" {
                "healthy".into()
            } else {
                String::new()
            },
        }
    }

    /// HA home with a pre-rendered compose placeholder.
    fn ha_home() -> (Home, crate::schema::ResolvedCluster) {
        let (home, _dir) = crate::commands::test_support::home_with_cluster();
        std::fs::write(home.rendered_compose(), "# placeholder").unwrap();
        let cfg = home.load_cluster().unwrap();
        let rc = cfg.validate().unwrap();
        assert_eq!(rc.master_ips.len(), 3, "fixture must be the HA profile");
        (home, rc)
    }

    fn restart_calls(calls: &[String]) -> Vec<String> {
        calls
            .iter()
            .filter(|c| c.starts_with("restart "))
            .cloned()
            .collect()
    }

    #[test]
    fn follower_first_puts_leader_last() {
        let targets = vec![
            "master-1".to_string(),
            "master-2".to_string(),
            "master-3".to_string(),
        ];
        let order = follower_first_order(&targets, Some("master-2"));
        assert_eq!(order, vec!["master-1", "master-3", "master-2"]);

        // leader outside the target set (single-follower restart) → sorted
        let order = follower_first_order(&["master-3".to_string()], Some("master-2"));
        assert_eq!(order, vec!["master-3"]);

        // unknown leader → plain sorted order
        let order = follower_first_order(&targets, None);
        assert_eq!(order, vec!["master-1", "master-2", "master-3"]);
    }

    // start_paused: gate sleeps elapse instantly under virtual time.
    #[tokio::test(start_paused = true)]
    async fn restart_followers_before_leader() {
        let (home, rc) = ha_home();
        let driver = MockComposeDriver::new(vec![]);
        // master-2 is the incumbent leader.
        let probe = TestProbe::new(&["172.30.0.12"], false);

        run(&home, Some("master".into()), false, &driver, &probe)
            .await
            .unwrap();

        let calls = driver.calls.lock().unwrap().clone();
        let restarts = restart_calls(&calls);
        assert_eq!(
            restarts,
            vec!["restart master-1", "restart master-3", "restart master-2"],
            "followers must restart before the leader"
        );
        // quorum probed well beyond the initial locate() sweep: one full gate
        // (4 leader-bearing fetches: 3-sample round + confirm) per node.
        assert!(probe.fetches.load(Ordering::SeqCst) > rc.master_ips.len());
    }

    #[tokio::test(start_paused = true)]
    async fn restart_single_master_profile() {
        let (home, _dir) = crate::commands::test_support::home_empty();
        // simple profile: exactly one master.
        let toml = crate::commands::init::DEFAULT_CLUSTER_TOML
            .replace("profile = \"ha\"", "profile = \"simple\"");
        std::fs::write(home.cluster_toml(), toml).unwrap();
        std::fs::write(home.rendered_compose(), "# x").unwrap();

        let driver = MockComposeDriver::new(vec![]);
        let probe = TestProbe::new(&["172.30.0.11"], false);
        run(&home, Some("master".into()), false, &driver, &probe)
            .await
            .unwrap();
        let calls = driver.calls.lock().unwrap().clone();
        assert_eq!(restart_calls(&calls), vec!["restart master-1"]);
    }

    #[tokio::test(start_paused = true)]
    async fn restart_force_skips_gate() {
        let (home, _rc) = ha_home();
        let driver = MockComposeDriver::new(vec![]);
        let probe = TestProbe::new(&["172.30.0.12"], false);

        run(&home, Some("master".into()), true, &driver, &probe)
            .await
            .unwrap();

        let calls = driver.calls.lock().unwrap().clone();
        assert_eq!(
            restart_calls(&calls),
            vec!["restart master-1", "restart master-3", "restart master-2"]
        );
        // only the single locate() sweep, no gate polls.
        assert_eq!(probe.fetches.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn restart_aborts_when_gate_fails_after_first_node() {
        let (home, _rc) = ha_home();
        let driver = MockComposeDriver::new(vec![]);
        // stalled zombie leader: the gate after restarting master-1 must fail.
        let probe = TestProbe::new(&["172.30.0.12"], true);

        let err = run(&home, Some("master".into()), false, &driver, &probe)
            .await
            .unwrap_err();
        assert!(err.contains("rolling restart halted"), "got: {err}");
        assert!(err.contains("master-1"));
        assert!(err.contains("not yet restarted"));

        let calls = driver.calls.lock().unwrap().clone();
        assert_eq!(restart_calls(&calls), vec!["restart master-1"]);
    }

    #[tokio::test(start_paused = true)]
    async fn restart_non_master_role_is_one_batch_without_probes() {
        let (home, _rc) = ha_home();
        let driver = MockComposeDriver::new(vec![]);
        let probe = TestProbe::new(&["172.30.0.12"], false);

        run(&home, Some("filer".into()), false, &driver, &probe)
            .await
            .unwrap();

        let calls = driver.calls.lock().unwrap().clone();
        assert_eq!(
            restart_calls(&calls),
            vec!["restart filer-1,filer-2,filer-3"]
        );
        assert_eq!(probe.fetches.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn restart_full_cluster_rolls_masters_then_discovered_services() {
        let (home, _rc) = ha_home();
        let driver = MockComposeDriver::new(vec![
            ps_row("master-1", "running"),
            ps_row("master-2", "running"),
            ps_row("master-3", "running"),
            ps_row("filer-1", "running"),
            ps_row("filer-2", "exited"), // stopped → left alone
            ps_row("redis", "running"),
        ]);
        let probe = TestProbe::new(&["172.30.0.12"], false);

        run(&home, None, false, &driver, &probe).await.unwrap();

        let calls = driver.calls.lock().unwrap().clone();
        let restarts = restart_calls(&calls);
        assert_eq!(
            restarts[..3],
            ["restart master-1", "restart master-3", "restart master-2"]
        );
        // only running non-master services, batched filer-before-infra
        assert!(restarts.contains(&"restart filer-1".to_string()));
        assert!(restarts.contains(&"restart redis".to_string()));
        assert!(
            !restarts.iter().any(|c| c.contains("filer-2")),
            "stopped container must not be restarted: {restarts:?}"
        );
        assert!(
            !restarts
                .iter()
                .any(|c| c.contains("master-1,") || c.contains("monitor")),
            "masters already rolled; undeclared services must not appear: {restarts:?}"
        );
    }
}
