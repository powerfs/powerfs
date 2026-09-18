//! `powerfs-ctl cert revoke` — revoke a client/node cert by `client_name`
//! or `fingerprint` via master's `POST /api/cert/revoke`. Exactly one selector
//! must be provided; this handler validates that before hitting the network
//! (master enforces too — returns 400 otherwise).

use crate::cert::{CertClient, RevokeRequest};

pub async fn run<C: CertClient>(
    client: &C,
    master_api: &str,
    admin_token: &str,
    client_name: Option<String>,
    fingerprint: Option<String>,
) -> Result<(), String> {
    let (cn, fp) = match (client_name, fingerprint) {
        (Some(cn), None) if !cn.is_empty() => (Some(cn), None),
        (None, Some(fp)) if !fp.is_empty() => (None, Some(fp)),
        (Some(_), Some(_)) => {
            return Err("specify exactly one of --client-name or --fingerprint (not both)".into());
        }
        (None, None) => {
            return Err("must specify --client-name or --fingerprint to identify the cert".into());
        }
        (Some(cn), None) => return Err(format!("--client-name is empty: '{cn}'")),
        (None, Some(fp)) => return Err(format!("--fingerprint is empty: '{fp}'")),
    };
    let req = RevokeRequest {
        client_name: cn,
        fingerprint: fp,
    };
    client.revoke(master_api, admin_token, &req).await?;
    println!("✓ revocation recorded (idempotent — re-revoking is a no-op)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cert::MockCertClient;
    use crate::commands::test_support;

    #[tokio::test]
    async fn revoke_by_client_name() {
        let (_home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_revoke(Ok(()));
        run(&client, "m:9300", "tok", Some("fuse-c1".into()), None)
            .await
            .unwrap();
        let calls = client.revoke_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].client_name.as_deref(), Some("fuse-c1"));
        assert!(calls[0].fingerprint.is_none());
    }

    #[tokio::test]
    async fn revoke_by_fingerprint() {
        let (_home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_revoke(Ok(()));
        run(&client, "m:9300", "tok", None, Some("ab:cd:ef".into()))
            .await
            .unwrap();
        let calls = client.revoke_calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].client_name.is_none());
        assert_eq!(calls[0].fingerprint.as_deref(), Some("ab:cd:ef"));
    }

    #[tokio::test]
    async fn revoke_both_selectors_rejected() {
        let (_home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_revoke(Ok(()));
        let err = run(
            &client,
            "m:9300",
            "tok",
            Some("fuse-c1".into()),
            Some("ab:cd".into()),
        )
        .await
        .unwrap_err();
        assert!(err.contains("exactly one"), "got: {err}");
    }

    #[tokio::test]
    async fn revoke_no_selector_rejected() {
        let (_home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_revoke(Ok(()));
        let err = run(&client, "m:9300", "tok", None, None).await.unwrap_err();
        assert!(err.contains("must specify"), "got: {err}");
    }
}
