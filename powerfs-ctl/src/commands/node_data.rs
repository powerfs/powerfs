//! `powerfs-ctl node data` — data-node lifecycle.
//! - `add`        → render → cert → up (declarative provisioning)
//! - `maintenance` → POST   /api/admin/nodes/{name}/maintenance  (toggle)
//! - `remove`       → DELETE /api/admin/nodes/{name}             (remove node)
//!
//! `maintenance` and `remove` are raft-mutating → leader-only (dispatch
//! discovers the leader before calling). `add` is declarative (no raft
//! interaction — data nodes auto-register with master via registration_token).

use crate::cert::CertClient;
use crate::compose::ComposeDriver;
use crate::home::Home;

pub async fn add<C: CertClient>(
    home: &Home,
    cert_client: &C,
    driver: &dyn ComposeDriver,
    name: &str,
) -> Result<(), String> {
    let (role, idx) = parse_node_name(name)?;

    let cfg = home.load_cluster()?;
    let rc = cfg.validate().map_err(|e| e.to_string())?;

    let (ip, declared) = match role {
        "volume" => (rc.volume_ips.get(idx - 1), rc.volume_ips.len()),
        "filer" => (rc.filer_ips.get(idx - 1), rc.filer_ips.len()),
        // parse_node_name already validated, but the compiler can't know.
        _ => return Err(format!("unknown role '{role}'")),
    };
    let ip = ip.ok_or_else(|| {
        format!(
            "{name} not declared in cluster.toml (currently {} {role}s). \
             Edit [nodes.{role}] count first, then re-run.",
            declared
        )
    })?;

    // 1. Render configs — compose.yml picks up the new node service.
    super::config_render::run(home).await?;

    // 2. Issue node cert. Cert API is not raft-mutating so any running
    //    master can sign — use the first declared master.
    let (master_api, admin_token) = super::master_api_and_token(home)?;
    super::cert_issue::run(
        home,
        cert_client,
        &master_api,
        &admin_token,
        name,
        std::slice::from_ref(ip),
        &[],
        true,
    )
    .await?;

    // 3. Start the container. Data nodes auto-register with master via
    //    registration_token — no raft join or health gate needed.
    let compose = home.rendered_compose();
    if !compose.exists() {
        return Err(format!(
            "{} missing — run `powerfs-ctl config render`",
            compose.display()
        ));
    }
    super::up::ensure_service_binaries(&[name.to_string()])?;
    println!("• docker compose up -d {name}");
    driver.up(&compose, &[name]).await?;

    println!("✓ {name} provisioned (ip={ip})");
    println!("  data nodes auto-register with master via registration_token");
    Ok(())
}

/// Parse "volume-7" → ("volume", 7). Accepts volume/filer roles only.
fn parse_node_name(name: &str) -> Result<(&str, usize), String> {
    let (role, idx_str) = name.split_once('-').ok_or_else(|| {
        format!("invalid node name '{name}' — expected format like 'volume-7' or 'filer-7'")
    })?;
    let idx: usize = idx_str
        .parse()
        .map_err(|_| format!("invalid node index '{idx_str}' in '{name}' — expected a number"))?;
    if idx == 0 {
        return Err(format!(
            "invalid node index 0 in '{name}' — indices start at 1"
        ));
    }
    match role {
        "volume" | "filer" => Ok((role, idx)),
        _ => Err(format!(
            "unknown data node role '{role}' — expected 'volume' or 'filer'"
        )),
    }
}

pub async fn maintenance<A: crate::admin::AdminClient>(
    client: &A,
    master_api: &str,
    admin_token: &str,
    name: &str,
    enabled: bool,
) -> Result<(), String> {
    client
        .set_maintenance(master_api, admin_token, name, enabled)
        .await?;
    let state = if enabled { "ON" } else { "OFF" };
    println!("✓ maintenance {} for data node '{}'", state, name);
    Ok(())
}

pub async fn remove<A: crate::admin::AdminClient>(
    client: &A,
    master_api: &str,
    admin_token: &str,
    name: &str,
    force: bool,
) -> Result<(), String> {
    client
        .remove_node(master_api, admin_token, name, force)
        .await?;
    println!("✓ data node '{}' removed (force={})", name, force);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cert::{IssuedCert, MockCertClient};
    use crate::commands::test_support;
    use crate::compose::tests::MockComposeDriver;

    fn issued() -> IssuedCert {
        IssuedCert {
            cert: "C".into(),
            key: "K".into(),
        }
    }

    /// HA cluster.toml with volume count=7 and filer count=4 (so volume-7
    /// and filer-4 are declared).
    fn ha_extended_home() -> (Home, std::path::PathBuf) {
        let (home, dir) = test_support::home_with_cluster();
        let toml = std::fs::read_to_string(home.cluster_toml()).unwrap();
        let toml = toml.replace("[nodes.volume]\n# count = 6", "[nodes.volume]\ncount = 7");
        let toml = toml.replace("[nodes.filer]\n# count = 3", "[nodes.filer]\ncount = 4");
        std::fs::write(home.cluster_toml(), toml).unwrap();
        std::fs::write(home.rendered_compose(), "# placeholder").unwrap();
        (home, dir)
    }

    /// Default HA cluster.toml (volume=6, filer=3 — for undeclared tests).
    fn ha_default_home() -> (Home, std::path::PathBuf) {
        let (home, dir) = test_support::home_with_cluster();
        std::fs::write(home.rendered_compose(), "# placeholder").unwrap();
        (home, dir)
    }

    #[tokio::test]
    async fn add_volume_provisions_node() {
        let (home, _dir) = ha_extended_home();
        let driver = MockComposeDriver::new(vec![]);
        let cert = MockCertClient::new().with_sign(Ok(issued()));

        add(&home, &cert, &driver, "volume-7").await.unwrap();

        // compose up called with volume-7
        let calls = driver.calls.lock().unwrap().clone();
        assert!(
            calls.iter().any(|c| c.contains("volume-7")),
            "expected up volume-7: {calls:?}"
        );

        // cert signed for volume-7 (node cert, empty mount_dirs)
        let signs = cert.sign_calls();
        assert_eq!(signs.len(), 1);
        assert_eq!(signs[0].client_name, "volume-7");
        assert!(signs[0].mount_dirs.is_empty());
        // volume IPs start at .21, so volume-7 = 172.30.0.27
        assert!(signs[0].san_ips.contains(&"172.30.0.27".to_string()));
    }

    #[tokio::test]
    async fn add_filer_provisions_node() {
        let (home, _dir) = ha_extended_home();
        let driver = MockComposeDriver::new(vec![]);
        let cert = MockCertClient::new().with_sign(Ok(issued()));

        add(&home, &cert, &driver, "filer-4").await.unwrap();

        let calls = driver.calls.lock().unwrap().clone();
        assert!(calls.iter().any(|c| c.contains("filer-4")));

        let signs = cert.sign_calls();
        assert_eq!(signs[0].client_name, "filer-4");
        // filer IPs start at .31, so filer-4 = 172.30.0.34
        assert!(signs[0].san_ips.contains(&"172.30.0.34".to_string()));
    }

    #[tokio::test]
    async fn add_undeclared_volume_errors() {
        let (home, _dir) = ha_default_home();
        let driver = MockComposeDriver::new(vec![]);
        let cert = MockCertClient::new();

        let err = add(&home, &cert, &driver, "volume-7").await.unwrap_err();
        assert!(err.contains("not declared"), "got: {err}");
        assert!(err.contains("6 volumes"), "got: {err}");
        assert!(cert.sign_calls().is_empty());
    }

    #[tokio::test]
    async fn add_undeclared_filer_errors() {
        let (home, _dir) = ha_default_home();
        let driver = MockComposeDriver::new(vec![]);
        let cert = MockCertClient::new();

        let err = add(&home, &cert, &driver, "filer-4").await.unwrap_err();
        assert!(err.contains("not declared"), "got: {err}");
        assert!(err.contains("3 filers"), "got: {err}");
    }

    #[tokio::test]
    async fn add_bad_name_errors() {
        let (home, _dir) = ha_default_home();
        let driver = MockComposeDriver::new(vec![]);
        let cert = MockCertClient::new();

        let err = add(&home, &cert, &driver, "master-1").await.unwrap_err();
        assert!(
            err.contains("unknown data node role 'master'"),
            "got: {err}"
        );

        let err = add(&home, &cert, &driver, "volume").await.unwrap_err();
        assert!(err.contains("expected format"), "got: {err}");

        let err = add(&home, &cert, &driver, "volume-0").await.unwrap_err();
        assert!(err.contains("indices start at 1"), "got: {err}");

        let err = add(&home, &cert, &driver, "volume-x").await.unwrap_err();
        assert!(err.contains("invalid node index"), "got: {err}");
    }

    #[tokio::test]
    async fn add_cert_failure_aborts_before_up() {
        let (home, _dir) = ha_extended_home();
        let driver = MockComposeDriver::new(vec![]);
        let cert = MockCertClient::new().with_sign(Err("HTTP 500".into()));

        let err = add(&home, &cert, &driver, "volume-7").await.unwrap_err();
        assert!(err.contains("500"), "got: {err}");
        let calls = driver.calls.lock().unwrap().clone();
        assert!(calls.is_empty());
    }

    #[test]
    fn parse_node_name_valid() {
        assert_eq!(parse_node_name("volume-7").unwrap(), ("volume", 7));
        assert_eq!(parse_node_name("filer-1").unwrap(), ("filer", 1));
    }

    #[test]
    fn parse_node_name_invalid() {
        assert!(parse_node_name("master-1").is_err());
        assert!(parse_node_name("volume").is_err());
        assert!(parse_node_name("volume-0").is_err());
        assert!(parse_node_name("volume-x").is_err());
        assert!(parse_node_name("redis-1").is_err());
    }

    #[tokio::test]
    async fn maintenance_on_sends_enabled_true() {
        let client = crate::admin::MockAdminClient::new().with_maintenance(Ok(()));
        maintenance(&client, "m:9300", "tok", "volume-1", true)
            .await
            .unwrap();
        let calls = client.maintenance_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "volume-1");
        assert!(calls[0].enabled);
    }

    #[tokio::test]
    async fn maintenance_off_sends_enabled_false() {
        let client = crate::admin::MockAdminClient::new().with_maintenance(Ok(()));
        maintenance(&client, "m:9300", "tok", "volume-1", false)
            .await
            .unwrap();
        let calls = client.maintenance_calls();
        assert!(!calls[0].enabled);
    }

    #[tokio::test]
    async fn maintenance_409_node_owns_routes() {
        let client = crate::admin::MockAdminClient::new()
            .with_maintenance(Err("HTTP 409: node owns active routes".into()));
        let err = maintenance(&client, "m:9300", "tok", "volume-1", true)
            .await
            .unwrap_err();
        assert!(err.contains("409"), "got: {err}");
    }

    #[tokio::test]
    async fn data_remove_sends_name_and_force() {
        let client = crate::admin::MockAdminClient::new().with_remove_node(Ok(()));
        remove(&client, "m:9300", "tok", "volume-2", true)
            .await
            .unwrap();
        let calls = client.remove_node_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "volume-2");
        assert!(calls[0].force);
    }

    #[tokio::test]
    async fn data_remove_409_without_force() {
        let client = crate::admin::MockAdminClient::new()
            .with_remove_node(Err("HTTP 409: node owns routes; use --force".into()));
        let err = remove(&client, "m:9300", "tok", "volume-2", false)
            .await
            .unwrap_err();
        assert!(err.contains("409"), "got: {err}");
    }
}
