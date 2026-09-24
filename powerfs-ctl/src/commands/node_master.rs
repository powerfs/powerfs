//! `powerfs-ctl node master` — raft membership lifecycle.
//! - `add`    → full declarative: render → cert → up → wait → raft join → gate
//!   (`--raft-only` skips to just the raft join)
//! - `remove` → DELETE /api/admin/masters/{id}     (remove voter)
//! - `list`   → GET    /api/admin/masters          (membership snapshot)
//!
//! `add` (default) and `remove` are raft-mutating → leader-only (dispatch
//! discovers the leader before calling). `list` works on any master.

use crate::admin::{AddMasterRequest, AdminClient, MastersSnapshot};
use crate::cert::CertClient;
use crate::compose::ComposeDriver;
use crate::health::{HealthGate, MetricsProbe};
use crate::home::Home;
use std::time::{Duration, Instant};

const METRICS_PORT: u16 = 9300;
const RAFT_PORT: &str = "9335";

#[allow(clippy::too_many_arguments)] // mirrors bootstrap::run's trait-object set
pub async fn add<A, C, P>(
    home: &Home,
    admin_client: &A,
    cert_client: &C,
    driver: &dyn ComposeDriver,
    probe: &P,
    id: u64,
    addr_override: Option<&str>,
    raft_only: bool,
) -> Result<(), String>
where
    A: AdminClient,
    C: CertClient,
    P: MetricsProbe,
{
    let cfg = home.load_cluster()?;
    let rc = cfg.validate().map_err(|e| e.to_string())?;

    if id == 0 || id as usize > rc.master_ips.len() {
        return Err(format!(
            "master-{id} not declared in cluster.toml (currently {} masters). \
             Edit [nodes.master] count/ips in cluster.toml first, then re-run.",
            rc.master_ips.len()
        ));
    }

    let new_ip = rc.master_ips[id as usize - 1].clone();
    let addr = match addr_override {
        Some(a) => a.to_string(),
        None => format!("{new_ip}:{RAFT_PORT}"),
    };

    if raft_only {
        let (leader_api, admin_token) = super::leader_api_and_token(home, probe).await?;
        let req = AddMasterRequest {
            id,
            addr: addr.clone(),
        };
        admin_client
            .add_master(&leader_api, &admin_token, &req)
            .await?;
        println!("✓ master {id} raft-joined at {addr}");
        return Ok(());
    }

    // Full provisioning flow.

    // 1. Render configs — compose.yml picks up the new master-N service.
    super::config_render::run(home).await?;

    // 2. Issue node cert for master-{id}. Cert API is not raft-mutating so
    //    any running master can sign — use the first declared master.
    let (cert_api, cert_tok) = super::master_api_and_token(home)?;
    super::cert_issue::run(
        home,
        cert_client,
        &cert_api,
        &cert_tok,
        &format!("master-{id}"),
        std::slice::from_ref(&new_ip),
        &[],
        true,
    )
    .await?;

    // 3. Start the new master container.
    let compose = home.rendered_compose();
    if !compose.exists() {
        return Err(format!(
            "{} missing — run `powerfs-ctl config render`",
            compose.display()
        ));
    }
    let svc = format!("master-{id}");
    super::up::ensure_service_binaries(std::slice::from_ref(&svc))?;
    println!("• docker compose up -d {svc}");
    driver.up(&compose, &[svc.as_str()]).await?;

    // 4. Wait for the new master to respond on /metrics.
    println!("• waiting for {svc} to boot...");
    wait_for_master(probe, &new_ip, Duration::from_secs(60)).await?;

    // 5. Raft join: POST /api/admin/masters {id, addr} to the leader.
    let (leader_api, admin_token) = super::leader_api_and_token(home, probe).await?;
    let req = AddMasterRequest {
        id,
        addr: addr.clone(),
    };
    admin_client
        .add_master(&leader_api, &admin_token, &req)
        .await?;
    println!("✓ master {id} raft-joined at {addr}");

    // 6. Health gate — verify full quorum including the new member.
    println!("• verifying quorum health...");
    let gate = HealthGate::new(METRICS_PORT);
    let info = gate.run(probe, &rc.master_ips).await?;
    println!(
        "✓ quorum healthy: leader {} (term={}, commit={})",
        info.ip, info.term, info.commit_index
    );
    Ok(())
}

/// Poll the new master's /metrics until it responds (or timeout). In tests
/// with TestProbe this returns instantly; in production ReqwestProbe retries
/// every 2s until the container boots.
async fn wait_for_master(
    probe: &dyn MetricsProbe,
    ip: &str,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        match probe.fetch(ip, METRICS_PORT).await {
            Ok(_) => return Ok(()),
            Err(_) if Instant::now() < deadline => {
                eprintln!("  waiting for master {ip} to boot...");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(format!("master {ip} not responding after {timeout:?}: {e}")),
        }
    }
}

pub async fn remove<A: AdminClient>(
    client: &A,
    master_api: &str,
    admin_token: &str,
    id: &str,
    force: bool,
) -> Result<(), String> {
    client
        .remove_master(master_api, admin_token, id, force)
        .await?;
    println!("✓ master {} removed (force={})", id, force);
    Ok(())
}

pub async fn list<A: AdminClient>(
    client: &A,
    master_api: &str,
    admin_token: &str,
) -> Result<(), String> {
    let snap = client.list_masters(master_api, admin_token).await?;
    print_membership(&snap);
    Ok(())
}

fn print_membership(snap: &MastersSnapshot) {
    let leader = snap.leader.as_deref().unwrap_or("(none)");
    println!("RAFT MEMBERSHIP  (local={}, leader={})", snap.local, leader);
    println!();
    println!("{:<6} {:<24} {:<10}", "ID", "ADDR", "ROLE");
    for m in &snap.members {
        let marker = if snap.leader.as_deref() == Some(m.id.as_str()) {
            " ← leader"
        } else {
            ""
        };
        println!("{:<6} {:<24} {:<10}{}", m.id, m.addr, m.role, marker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::MockAdminClient;
    use crate::cert::{IssuedCert, MockCertClient};
    use crate::commands::test_support;
    use crate::compose::tests::MockComposeDriver;

    fn issued() -> IssuedCert {
        IssuedCert {
            cert: "C".into(),
            key: "K".into(),
        }
    }

    /// HA cluster.toml with 4 masters declared (count=4).
    fn ha4_home() -> (Home, std::path::PathBuf) {
        let (home, dir) = test_support::home_with_cluster();
        let toml = std::fs::read_to_string(home.cluster_toml()).unwrap();
        let toml = toml.replace("[nodes.master]\n# count = 3", "[nodes.master]\ncount = 4");
        std::fs::write(home.cluster_toml(), toml).unwrap();
        // Pre-render so compose exists.
        std::fs::write(home.rendered_compose(), "# placeholder").unwrap();
        (home, dir)
    }

    /// HA cluster.toml with the default 3 masters (for undeclared-error tests).
    fn ha3_home() -> (Home, std::path::PathBuf) {
        let (home, dir) = test_support::home_with_cluster();
        std::fs::write(home.rendered_compose(), "# placeholder").unwrap();
        (home, dir)
    }

    // ---- full flow ----

    #[tokio::test(start_paused = true)]
    async fn add_full_flow_renders_certs_ups_joins_and_gates() {
        let (home, _dir) = ha4_home();
        let driver = MockComposeDriver::new(vec![]);
        // master-1 (.11) is the leader; new master-4 (.14) responds as follower.
        let probe = test_support::test_probe::TestProbe::new(&["172.30.0.11"], false);
        let cert = MockCertClient::new().with_sign(Ok(issued()));
        let admin = MockAdminClient::new().with_add(Ok(()));

        add(&home, &admin, &cert, &driver, &probe, 4, None, false)
            .await
            .unwrap();

        // compose up called with master-4
        let calls = driver.calls.lock().unwrap().clone();
        assert!(
            calls.iter().any(|c| c.contains("master-4")),
            "expected up master-4: {calls:?}"
        );

        // cert signed for master-4 with san-ip 172.30.0.14
        let signs = cert.sign_calls();
        assert_eq!(signs.len(), 1);
        assert_eq!(signs[0].client_name, "master-4");
        assert!(signs[0].san_ips.contains(&"172.30.0.14".to_string()));

        // raft join posted {id:4, addr:172.30.0.14:9335}
        let adds = admin.add_calls();
        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].id, 4);
        assert_eq!(adds[0].addr, "172.30.0.14:9335");
    }

    #[tokio::test(start_paused = true)]
    async fn add_undeclared_master_errors() {
        let (home, _dir) = ha3_home();
        let driver = MockComposeDriver::new(vec![]);
        let probe = test_support::test_probe::TestProbe::new(&["172.30.0.11"], false);
        let cert = MockCertClient::new();
        let admin = MockAdminClient::new();

        let err = add(&home, &admin, &cert, &driver, &probe, 4, None, false)
            .await
            .unwrap_err();
        assert!(err.contains("not declared"), "got: {err}");
        assert!(err.contains("3 masters"), "got: {err}");
        // nothing provisioned
        assert!(admin.add_calls().is_empty());
        assert!(cert.sign_calls().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn add_zero_id_errors() {
        let (home, _dir) = ha3_home();
        let driver = MockComposeDriver::new(vec![]);
        let probe = test_support::test_probe::TestProbe::new(&["172.30.0.11"], false);
        let cert = MockCertClient::new();
        let admin = MockAdminClient::new();

        let err = add(&home, &admin, &cert, &driver, &probe, 0, None, false)
            .await
            .unwrap_err();
        assert!(err.contains("not declared"), "got: {err}");
    }

    #[tokio::test(start_paused = true)]
    async fn add_raft_only_skips_provisioning() {
        let (home, _dir) = ha4_home();
        let driver = MockComposeDriver::new(vec![]);
        let probe = test_support::test_probe::TestProbe::new(&["172.30.0.11"], false);
        let cert = MockCertClient::new();
        let admin = MockAdminClient::new().with_add(Ok(()));

        add(&home, &admin, &cert, &driver, &probe, 4, None, true)
            .await
            .unwrap();

        // no compose up, no cert issued
        let calls = driver.calls.lock().unwrap().clone();
        assert!(
            calls.is_empty(),
            "raft-only should not touch compose: {calls:?}"
        );
        assert!(cert.sign_calls().is_empty());

        // raft join still happened
        let adds = admin.add_calls();
        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].id, 4);
        assert_eq!(adds[0].addr, "172.30.0.14:9335");
    }

    #[tokio::test(start_paused = true)]
    async fn add_raft_only_with_addr_override() {
        let (home, _dir) = ha3_home();
        let driver = MockComposeDriver::new(vec![]);
        let probe = test_support::test_probe::TestProbe::new(&["172.30.0.11"], false);
        let cert = MockCertClient::new();
        let admin = MockAdminClient::new().with_add(Ok(()));

        add(
            &home,
            &admin,
            &cert,
            &driver,
            &probe,
            2,
            Some("10.0.0.2:9335"),
            true,
        )
        .await
        .unwrap();

        let adds = admin.add_calls();
        assert_eq!(adds[0].addr, "10.0.0.2:9335");
    }

    #[tokio::test(start_paused = true)]
    async fn add_gate_fails_after_raft_join() {
        let (home, _dir) = ha4_home();
        let driver = MockComposeDriver::new(vec![]);
        // stalled zombie leader → gate fails on the second sample
        let probe = test_support::test_probe::TestProbe::new(&["172.30.0.11"], true);
        let cert = MockCertClient::new().with_sign(Ok(issued()));
        let admin = MockAdminClient::new().with_add(Ok(()));

        let err = add(&home, &admin, &cert, &driver, &probe, 4, None, false)
            .await
            .unwrap_err();
        assert!(
            err.contains("zombie") || err.contains("health gate"),
            "got: {err}"
        );
        // raft join succeeded before the gate
        assert_eq!(admin.add_calls().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn add_cert_failure_aborts_before_up() {
        let (home, _dir) = ha4_home();
        let driver = MockComposeDriver::new(vec![]);
        let probe = test_support::test_probe::TestProbe::new(&["172.30.0.11"], false);
        let cert = MockCertClient::new().with_sign(Err("HTTP 500: CA broken".into()));
        let admin = MockAdminClient::new().with_add(Ok(()));

        let err = add(&home, &admin, &cert, &driver, &probe, 4, None, false)
            .await
            .unwrap_err();
        assert!(err.contains("CA broken"), "got: {err}");
        // no compose up, no raft join
        let calls = driver.calls.lock().unwrap().clone();
        assert!(calls.is_empty());
        assert!(admin.add_calls().is_empty());
    }

    // ---- existing low-level tests (unchanged) ----

    #[tokio::test]
    async fn master_remove_sends_id_and_force() {
        let client = MockAdminClient::new().with_remove(Ok(()));
        remove(&client, "m:9300", "tok", "2", true).await.unwrap();
        let calls = client.remove_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "2");
        assert!(calls[0].force);
    }

    #[tokio::test]
    async fn master_remove_409_leader_without_force() {
        let client = MockAdminClient::new()
            .with_remove(Err("HTTP 409: cannot remove leader without --force".into()));
        let err = remove(&client, "m:9300", "tok", "1", false)
            .await
            .unwrap_err();
        assert!(err.contains("409"), "got: {err}");
    }

    #[tokio::test]
    async fn master_list_prints_table() {
        let client =
            MockAdminClient::new().with_list(Ok(crate::admin::mock::three_voter_snapshot()));
        list(&client, "m:9300", "tok").await.unwrap();
    }

    #[tokio::test]
    async fn master_list_error_propagates() {
        let client = MockAdminClient::new().with_list(Err("HTTP 401".into()));
        let err = list(&client, "m:9300", "tok").await.unwrap_err();
        assert!(err.contains("401"), "got: {err}");
    }

    #[tokio::test]
    async fn leader_api_and_token_finds_leader() {
        let (home, _dir) = test_support::home_with_cluster();
        let probe = test_support::test_probe::TestProbe::new(&["172.30.0.11"], false);
        let (api, tok) = crate::commands::leader_api_and_token(&home, &probe)
            .await
            .unwrap();
        assert_eq!(api, "172.30.0.11:9300");
        assert!(!tok.is_empty());
    }

    #[tokio::test]
    async fn leader_api_and_token_no_leader() {
        let (home, _dir) = test_support::home_with_cluster();
        let probe = test_support::test_probe::TestProbe::new(&[], false);
        let err = crate::commands::leader_api_and_token(&home, &probe)
            .await
            .unwrap_err();
        assert!(err.contains("no raft leader"), "got: {err}");
    }

    #[tokio::test]
    async fn leader_api_and_token_split_brain() {
        let (home, _dir) = test_support::home_with_cluster();
        let probe =
            test_support::test_probe::TestProbe::new(&["172.30.0.11", "172.30.0.12"], false);
        let err = crate::commands::leader_api_and_token(&home, &probe)
            .await
            .unwrap_err();
        assert!(err.contains("split-brain"), "got: {err}");
    }

    #[tokio::test]
    async fn leader_api_and_token_all_unreachable() {
        let (home, _dir) = test_support::home_with_cluster();
        let probe = test_support::test_probe::TestProbe::unreachable();
        let err = crate::commands::leader_api_and_token(&home, &probe)
            .await
            .unwrap_err();
        assert!(err.contains("no raft leader"), "got: {err}");
    }
}
