//! `powerfs-ctl cert renew` — re-sign an existing client cert via master's
//! `POST /api/cert/renew`. The master looks up the stored bindings (SAN IPs,
//! mount dirs) by `client_name` and returns a freshly signed cert+key pair.
//! Optionally revoke the old cert immediately (`--revoke-old`); the default is
//! a grace rollover window (old cert stays valid until expiry).

use crate::cert::{CertClient, RenewRequest};
use crate::commands::cert_issue;
use crate::home::Home;

pub async fn run<C: CertClient>(
    home: &Home,
    client: &C,
    master_api: &str,
    admin_token: &str,
    client_name: &str,
    revoke_old: bool,
) -> Result<(), String> {
    let req = RenewRequest {
        client_name: client_name.into(),
        // Omit the field entirely when the flag is absent — master treats
        // absent as false (grace period). Only send true when explicitly asked.
        revoke_old: if revoke_old { Some(true) } else { None },
    };
    let issued = client.renew(master_api, admin_token, &req).await?;
    cert_issue::write_cert(home, client_name, &issued)?;
    println!(
        "✓ renewed certificate for '{}' (revoke_old={})",
        client_name, revoke_old
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cert::{IssuedCert, MockCertClient};
    use crate::commands::test_support;

    fn issued() -> IssuedCert {
        IssuedCert {
            cert: "RENEWED-CRT".into(),
            key: "RENEWED-KEY".into(),
        }
    }

    #[tokio::test]
    async fn renew_writes_new_cert() {
        let (home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_renew(Ok(issued()));
        run(&home, &client, "m:9300", "tok", "fuse-c1", false)
            .await
            .unwrap();
        let crt = std::fs::read_to_string(home.certs_dir().join("fuse-c1.crt")).unwrap();
        assert_eq!(crt, "RENEWED-CRT");
        let calls = client.renew_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].client_name, "fuse-c1");
        // revoke_old should be None when flag is false
        assert_eq!(calls[0].revoke_old, None);
    }

    #[tokio::test]
    async fn renew_with_revoke_old_sends_true() {
        let (home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_renew(Ok(issued()));
        run(&home, &client, "m:9300", "tok", "fuse-c1", true)
            .await
            .unwrap();
        let calls = client.renew_calls();
        assert_eq!(calls[0].revoke_old, Some(true));
    }

    #[tokio::test]
    async fn renew_404_unknown_client() {
        let (home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_renew(Err("HTTP 404: unknown".into()));
        let err = run(&home, &client, "m:9300", "tok", "nope", false)
            .await
            .unwrap_err();
        assert!(err.contains("404"), "got: {err}");
    }
}
