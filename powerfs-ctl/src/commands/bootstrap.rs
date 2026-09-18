//! `powerfs-ctl bootstrap` — one-shot cluster bring-up.
//!
//! Sequence: init (if missing) → render → up master quorum → cert init-ca →
//! issue node certs (filer/volume) → issue declared client certs → up full.
//! Any step failure prints a concrete retry hint (master is already up, so
//! the user can resume from the failed step rather than re-running all).

use crate::cert::CertClient;
use crate::compose::ComposeDriver;
use crate::health::MetricsProbe;
use crate::home::Home;
use crate::schema::Profile;

pub async fn run<C, P>(
    home: &Home,
    profile: Profile,
    network: &str,
    nodes: Option<u8>,
    driver: &dyn ComposeDriver,
    probe: &P,
    cert_client: &C,
) -> Result<(), String>
where
    C: CertClient,
    P: MetricsProbe,
{
    // 1. init (if cluster.toml missing). --profile/--network/--nodes only
    //    apply on first init; an existing file is used as-is (user edits win).
    ensure_cluster_toml(home, profile, network, nodes)?;

    // 2. render configs + compose.
    super::config_render::run(home).await?;

    // 3. up master quorum + health gate. The leader must prove it can commit
    //    before we sign certs against it (zombie leader can't sign).
    super::up::run(home, Some("master".into()), driver, probe).await?;

    // reload to pick up resolved IPs + admin_token (validate may have allocated).
    let cfg = home.load_cluster()?;
    let rc = cfg.validate().map_err(|e| e.to_string())?;
    let master_api = format!("{}:9300", rc.master_ips[0]);
    let admin_token = rc.cfg.cluster.admin_token.clone();

    // 4. cert init-ca (pull CA cert down to this host).
    super::cert_init_ca::run(home, cert_client, &master_api, &admin_token)
        .await
        .map_err(|e| {
            format!("{e}\n  master is up — retry `powerfs-ctl cert init-ca`, then continue")
        })?;

    // 5. issue node certs (filer/volume). mount_dirs=[] for node certs.
    for (i, ip) in rc.filer_ips.iter().enumerate() {
        let name = format!("filer-{}", i + 1);
        super::cert_issue::run(
            home,
            cert_client,
            &master_api,
            &admin_token,
            &name,
            std::slice::from_ref(ip),
            &[],
            true,
        )
        .await
        .map_err(|e| retry_hint(&name, ip, &e))?;
    }
    for (i, ip) in rc.volume_ips.iter().enumerate() {
        let name = format!("volume-{}", i + 1);
        super::cert_issue::run(
            home,
            cert_client,
            &master_api,
            &admin_token,
            &name,
            std::slice::from_ref(ip),
            &[],
            true,
        )
        .await
        .map_err(|e| retry_hint(&name, ip, &e))?;
    }

    // 6. issue declared client certs (only those with an IP in cluster.toml).
    //    Skip the cluster.toml append — bootstrap owns the file. Clients
    //    without an IP are left for `client enroll <name> --ip <ip>`.
    for (name, spec) in &rc.cfg.client {
        let Some(ip) = &spec.ip else {
            println!(
                "• skipping client {name} (no ip in cluster.toml — use `client enroll {name} --ip <ip>`)"
            );
            continue;
        };
        super::client_enroll::issue_client_cert(
            home,
            cert_client,
            &master_api,
            &admin_token,
            name,
            ip,
        )
        .await
        .map_err(|e| retry_hint(name, ip, &e))?;
        super::client_enroll::render_client_config(&rc, home, name, spec.kind)
            .map_err(|e| format!("render client config for {name}: {e}"))?;
    }

    // 7. up full (master is no-op; starts volume/filer/redis/monitor/s3).
    //    Health gate runs again — idempotent confirmation of full quorum.
    super::up::run(home, None, driver, probe).await?;

    println!("✓ bootstrap complete — cluster is up");
    Ok(())
}

fn retry_hint(name: &str, ip: &str, err: &str) -> String {
    format!(
        "{err}\n  master is up — retry `powerfs-ctl cert issue --name {name} --san-ip {ip} --node`, then `powerfs-ctl up`"
    )
}

/// Generate cluster.toml if missing, substituting profile/network/nodes into
/// the DEFAULT_CLUSTER_TOML template. Existing file → args ignored + warning.
fn ensure_cluster_toml(
    home: &Home,
    profile: Profile,
    network: &str,
    nodes: Option<u8>,
) -> Result<(), String> {
    let p = home.cluster_toml();
    if p.exists() {
        if profile != Profile::Ha || network != "172.30.0.0/16" || nodes.is_some() {
            eprintln!(
                "⚠ cluster.toml exists — ignoring --profile/--network/--nodes (edit cluster.toml directly to change)"
            );
        }
        return Ok(());
    }
    let toml = cluster_toml_for(profile, network, nodes);
    home.ensure_skeleton()
        .map_err(|e| format!("create state dirs: {e}"))?;
    std::fs::write(&p, &toml).map_err(|e| format!("write {}: {}", p.display(), e))?;
    println!("✓ Wrote {}", p.display());
    Ok(())
}

fn cluster_toml_for(profile: Profile, network: &str, nodes: Option<u8>) -> String {
    let mut s = super::init::DEFAULT_CLUSTER_TOML.to_string();
    let profile_str = match profile {
        Profile::Simple => "simple",
        Profile::Ha => "ha",
        Profile::Rdma => "rdma",
    };
    s = s.replace("profile = \"ha\"", &format!("profile = \"{profile_str}\""));
    s = s.replace(
        "subnet = \"172.30.0.0/16\"",
        &format!("subnet = \"{network}\""),
    );
    if let Some(n) = nodes {
        // Uncomment + set count under [nodes.master]. The template's comment
        // block is `[nodes.master]\n# count = 3` — replace just the master
        // count line (filer also has `# count = 3`, so pin to the master header).
        s = s.replace(
            "[nodes.master]\n# count = 3",
            &format!("[nodes.master]\ncount = {n}"),
        );
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cert::{IssuedCert, MockCertClient};
    use crate::compose::tests::MockComposeDriver;
    use crate::health::tests::MockProbe;
    use crate::health::MasterMetrics;
    use crate::schema::Profile;

    fn healthy_leader() -> MasterMetrics {
        MasterMetrics {
            is_leader: true,
            term: 2,
            commit_index: 5,
            last_applied: 5,
            healthz_ok: true,
        }
    }

    fn issued() -> IssuedCert {
        IssuedCert {
            cert: "C".into(),
            key: "K".into(),
        }
    }

    // start_paused so the health gate's 2s poll_interval sleeps complete
    // instantly in virtual time — otherwise each up::run call would block ~2s.
    #[tokio::test(start_paused = true)]
    async fn bootstrap_happy_path() {
        let (home, _dir) = crate::commands::test_support::home_empty();
        let driver = MockComposeDriver::new(vec![]);
        let probe = MockProbe::fixed(healthy_leader());
        let cert = MockCertClient::new()
            .with_ca(Ok("CA-PEM".into()))
            .with_sign(Ok(issued()));
        run(
            &home,
            Profile::Simple,
            "172.30.0.0/16",
            None,
            &driver,
            &probe,
            &cert,
        )
        .await
        .unwrap();

        // compose driver saw up(master) then up(full)
        let calls = driver.calls.lock().unwrap().clone();
        assert!(
            calls.iter().any(|c| c.contains("master-1")),
            "master up: {calls:?}"
        );
        assert!(calls.iter().any(|c| c == "up "), "full up: {calls:?}");

        // CA + 2 node certs (filer-1 + volume-1), no client certs
        assert_eq!(
            std::fs::read_to_string(home.certs_dir().join("ca.crt")).unwrap(),
            "CA-PEM"
        );
        let signs = cert.sign_calls();
        assert_eq!(signs.len(), 2, "expected filer-1 + volume-1, got {signs:?}");
        assert!(signs.iter().all(|s| s.mount_dirs.is_empty()));
        assert!(signs.iter().any(|s| s.client_name == "filer-1"));
        assert!(signs.iter().any(|s| s.client_name == "volume-1"));
    }

    #[tokio::test(start_paused = true)]
    async fn bootstrap_fails_when_quorum_fails() {
        // HA profile → 3 masters, MockProbe returns leader for all 3 →
        // split-brain detected instantly at the up-master step.
        let (home, _dir) = crate::commands::test_support::home_empty();
        let driver = MockComposeDriver::new(vec![]);
        let probe = MockProbe::fixed(healthy_leader());
        let cert = MockCertClient::new()
            .with_ca(Ok("CA-PEM".into()))
            .with_sign(Ok(issued()));
        let err = run(
            &home,
            Profile::Ha,
            "172.30.0.0/16",
            None,
            &driver,
            &probe,
            &cert,
        )
        .await
        .unwrap_err();
        assert!(err.contains("split-brain"), "got: {err}");
        // no certs signed — failure was before cert init-ca
        assert!(cert.sign_calls().is_empty());
        assert!(!home.certs_dir().join("ca.crt").exists());
    }

    #[tokio::test(start_paused = true)]
    async fn bootstrap_fails_when_init_ca_fails() {
        let (home, _dir) = crate::commands::test_support::home_empty();
        let driver = MockComposeDriver::new(vec![]);
        let probe = MockProbe::fixed(healthy_leader());
        let cert = MockCertClient::new()
            .with_ca(Err("ca boom".into()))
            .with_sign(Ok(issued()));
        let err = run(
            &home,
            Profile::Simple,
            "172.30.0.0/16",
            None,
            &driver,
            &probe,
            &cert,
        )
        .await
        .unwrap_err();
        assert!(err.contains("ca boom"), "got: {err}");
        // master came up but cert init-ca failed → no node certs signed
        assert!(cert.sign_calls().is_empty());
    }
}
