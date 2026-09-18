//! `powerfs-ctl cert list` — query master's `GET /api/cert/list` and print a
//! sorted table. Master is the CA; its in-memory registry tracks every issued
//! client/node cert. The HTTP endpoint was added in M5 Part 1, replacing the
//! old stopgap that read `client_registry.json` off the local ca_dir mount.

use crate::cert::{unix_to_ymd, CertClient, RegistryEntry};
use crate::home::Home;

pub async fn run<C: CertClient>(
    _home: &Home,
    client: &C,
    master_api: &str,
    admin_token: &str,
) -> Result<(), String> {
    let mut entries = client.list_certs(master_api, admin_token).await?;
    entries.sort_by(|a, b| a.client_name.cmp(&b.client_name));
    if entries.is_empty() {
        println!("(no certificates issued yet)");
        return Ok(());
    }
    println!(
        "{:<20} {:<28} {:<22} {:<14} EXPIRES",
        "NAME", "SAN_IPS", "MOUNT_DIRS", "FINGERPRINT"
    );
    for e in &entries {
        print_entry(e);
    }
    Ok(())
}

fn print_entry(e: &RegistryEntry) {
    let san = e.san_ips.join(",");
    let mount = if e.mount_dirs.is_empty() {
        "(node)".to_string()
    } else {
        e.mount_dirs.join(",")
    };
    let fp: String = e.cert_fingerprint_sha256.chars().take(12).collect();
    let (y, m, d) = unix_to_ymd(e.expires_at);
    let flag = if e.revoked { " [REVOKED]" } else { "" };
    println!(
        "{:<20} {:<28} {:<22} {:<14} {}-{:02}-{:02}{}",
        e.client_name, san, mount, fp, y, m, d, flag
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cert::MockCertClient;
    use crate::commands::test_support;

    fn entry(name: &str, fp: &str, revoked: bool) -> RegistryEntry {
        RegistryEntry {
            client_name: name.into(),
            client_id: None,
            san_ips: vec!["172.30.0.41".into()],
            mount_dirs: if name.starts_with("filer") {
                vec![]
            } else {
                vec!["/mnt/powerfs".into()]
            },
            issued_at: 0,
            expires_at: 1735689600, // 2025-01-01
            cert_fingerprint_sha256: fp.into(),
            revoked,
        }
    }

    #[tokio::test]
    async fn cert_list_http_sorts_and_prints() {
        let (home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_list(Ok(vec![
            entry("fuse-c1", "fp_b", false),
            entry("filer-1", "fp_a", false),
        ]));
        run(&home, &client, "m:9300", "tok").await.unwrap();
        // mock recorded no calls for list (no request struct), but response was served
    }

    #[tokio::test]
    async fn cert_list_empty_registry() {
        let (home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_list(Ok(vec![]));
        run(&home, &client, "m:9300", "tok").await.unwrap();
    }

    #[tokio::test]
    async fn cert_list_http_error() {
        let (home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_list(Err("HTTP 503".into()));
        let err = run(&home, &client, "m:9300", "tok").await.unwrap_err();
        assert!(err.contains("503"), "got: {err}");
    }
}
