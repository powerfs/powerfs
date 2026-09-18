//! `powerfs-ctl cert issue` — sign a node OR client cert against master's CA.
//!
//! Node certs (`--node`): `mount_dirs=[]`, CN=client_name=node_id. Master later
//! validates `client_name` against the node_id reported in RegisterFiler/
//! Heartbeat — not against mount_dirs — so an empty mount_dirs is correct.
//!
//! Client certs (no `--node`): require at least one `--san-ip` and one
//! `--mount-dir`. Matches powerfs-cli's "AT LEAST ONE IS REQUIRED" semantics.

use crate::cert::{CertClient, IssuedCert, SignClientRequest};
use crate::home::Home;
use std::os::unix::fs::OpenOptionsExt;

#[allow(clippy::too_many_arguments)] // 8 args mirror powerfs-cli cert sign-node's flag set
pub async fn run<C: CertClient>(
    home: &Home,
    client: &C,
    master_api: &str,
    admin_token: &str,
    name: &str,
    san_ips: &[String],
    mount_dirs: &[String],
    node: bool,
) -> Result<(), String> {
    if san_ips.is_empty() {
        return Err("--san-ip is required".into());
    }
    if !node && mount_dirs.is_empty() {
        return Err(
            "--mount-dir is required for client certs (or pass --node for a node cert)".into(),
        );
    }
    let req = SignClientRequest {
        common_name: name.into(),
        client_name: name.into(),
        client_id: None,
        san_ips: san_ips.to_vec(),
        mount_dirs: if node {
            Vec::new()
        } else {
            mount_dirs.to_vec()
        },
    };
    let issued = client.sign_client(master_api, admin_token, &req).await?;
    write_cert(home, name, &issued)
}

/// Write `<certs_dir>/<name>.crt` + `<name>.key`. The key is chmod 0600 to
/// match powerfs-cli's behavior (private key must not be world-readable).
pub fn write_cert(home: &Home, name: &str, issued: &IssuedCert) -> Result<(), String> {
    let dir = home.certs_dir();
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir {}: {}", dir.display(), e))?;
    let crt = dir.join(format!("{name}.crt"));
    let key = dir.join(format!("{name}.key"));
    std::fs::write(&crt, &issued.cert).map_err(|e| format!("write {}: {}", crt.display(), e))?;
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&key)
        .and_then(|mut f| std::io::Write::write_all(&mut f, issued.key.as_bytes()))
        .map_err(|e| format!("write {}: {}", key.display(), e))?;
    println!(
        "✓ {} certificate saved: {} + {}",
        name,
        crt.display(),
        key.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cert::{IssuedCert, MockCertClient};
    use crate::commands::test_support;
    use std::os::unix::fs::PermissionsExt;

    fn issued() -> IssuedCert {
        IssuedCert {
            cert: "NODE-CRT".into(),
            key: "NODE-KEY".into(),
        }
    }

    #[tokio::test]
    async fn cert_issue_node_writes_node_cert() {
        let (home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_sign(Ok(issued()));
        run(
            &home,
            &client,
            "m:9300",
            "tok",
            "filer-1",
            &["1.2.3.4".into()],
            &[],
            true,
        )
        .await
        .unwrap();
        let crt = std::fs::read_to_string(home.certs_dir().join("filer-1.crt")).unwrap();
        assert_eq!(crt, "NODE-CRT");
        let key_path = home.certs_dir().join("filer-1.key");
        let mode = std::fs::metadata(&key_path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "key file must be 0600");
        // node cert → mount_dirs sent empty
        let calls = client.sign_calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].mount_dirs.is_empty());
    }

    #[tokio::test]
    async fn cert_issue_client_requires_mount_dir() {
        let (home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new();
        let err = run(
            &home,
            &client,
            "m:9300",
            "tok",
            "fuse-1",
            &["1.2.3.4".into()],
            &[],
            false,
        )
        .await
        .unwrap_err();
        assert!(err.contains("--mount-dir"), "got: {err}");
    }
}
