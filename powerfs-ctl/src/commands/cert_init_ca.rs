//! `powerfs-ctl cert init-ca` — fetch the cluster CA from master and persist
//! it as `<home>/certs/ca.crt`. Master is the CA (signs every node/client cert),
//! so "init-ca" really means "pull the CA cert down to this host so other hosts
//! (and the local fuse client) can trust it".

use crate::cert::CertClient;
use crate::home::Home;

pub async fn run<C: CertClient>(
    home: &Home,
    client: &C,
    master_api: &str,
    admin_token: &str,
) -> Result<(), String> {
    let pem = client.fetch_ca(master_api, admin_token).await?;
    let dir = home.certs_dir();
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir {}: {}", dir.display(), e))?;
    let p = dir.join("ca.crt");
    std::fs::write(&p, &pem).map_err(|e| format!("write {}: {}", p.display(), e))?;
    println!("✓ CA certificate saved to {}", p.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cert::MockCertClient;
    use crate::commands::test_support;

    #[tokio::test]
    async fn cert_fetch_ca_writes_ca_crt() {
        let (home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_ca(Ok("FAKE-CA-PEM".into()));
        run(&home, &client, "127.0.0.1:9300", "tok").await.unwrap();
        let ca = std::fs::read_to_string(home.certs_dir().join("ca.crt")).unwrap();
        assert_eq!(ca, "FAKE-CA-PEM");
    }
}
