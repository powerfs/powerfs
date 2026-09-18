//! Certificate client — talks to master's `/api/cert/*` HTTP endpoints
//! (master acts as cluster CA). Trait-based so tests inject a MockCertClient;
//! the real `MasterCertClient` lives in `reqwest_client.rs`.
//!
//! Only the three endpoints master actually exposes are wrapped:
//!   GET  /api/cert/ca           — fetch CA cert PEM
//!   POST /api/cert/sign-client  — issue a client OR node cert (mount_dirs=[]
//!                                 for node certs, non-empty for client certs)
//!   POST /api/cert/sign-server  — issue a server cert (not yet wired to a
//!                                 command; reserved for later if needed)
//! `cert renew` / `cert revoke` are intentionally absent — master has no
//! HTTP endpoints for them (the `revoked` field exists in `IssuedClientCert`
//! but no route flips it). They return once master grows
//! `POST /api/cert/revoke` + `GET /api/cert/list` in M5+.

pub mod reqwest_client;
pub use reqwest_client::MasterCertClient;

use async_trait::async_trait;
use std::collections::HashMap;

/// master_api shape: `"host:metrics_port"` (e.g. "172.30.0.11:9300").
/// Handlers construct the full URL as `http://<master_api>/api/cert/...`.
/// Internal metrics port is plain HTTP (no TLS) — matches `ReqwestProbe`.
#[async_trait]
pub trait CertClient: Send + Sync {
    async fn fetch_ca(&self, master_api: &str, admin_token: &str) -> Result<String, String>;
    async fn sign_client(
        &self,
        master_api: &str,
        admin_token: &str,
        req: &SignClientRequest,
    ) -> Result<IssuedCert, String>;
}

/// Body for `POST /api/cert/sign-client`. Mirrors master's `SignClientRequestV2`
/// (powerfs-master/src/ca_manager.rs). For node certs, `mount_dirs` is empty
/// and `client_name == node_id` — the master validates `client_name` against
/// the node_id reported in RegisterFiler/Heartbeat (not against mount_dirs).
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct SignClientRequest {
    pub common_name: String,
    pub client_name: String,
    pub client_id: Option<String>,
    pub san_ips: Vec<String>,
    pub mount_dirs: Vec<String>,
}

/// Decoded `POST /api/cert/sign-{client,server}` response.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct IssuedCert {
    pub cert: String,
    pub key: String,
}

/// One entry of master's `client_registry.json`. Mirrors
/// `IssuedClientCert` from powerfs-master/src/ca_manager.rs (subset — we
/// deserialize what's on disk, `#[serde(default)]` absorbs future field
/// additions on the master side). Fields not yet read by `cert list` are
/// kept for schema fidelity (silenced below).
#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)]
pub struct RegistryEntry {
    pub client_name: String,
    #[serde(default)]
    pub client_id: Option<String>,
    pub san_ips: Vec<String>,
    pub mount_dirs: Vec<String>,
    pub issued_at: u64,
    pub expires_at: u64,
    pub cert_fingerprint_sha256: String,
    #[serde(default)]
    pub revoked: bool,
}

/// On-disk registry written by master into `<ca_dir>/client_registry.json`.
/// Master's `ca_dir` is mounted from the host's certs directory
/// (docker/certs-default/ or docker/certs/), so we can read it locally.
#[derive(Debug, Default, serde::Deserialize)]
#[allow(dead_code)]
pub struct ClientRegistry {
    #[serde(default)]
    pub by_fingerprint: HashMap<String, RegistryEntry>,
    #[serde(default)]
    pub by_client_name: HashMap<String, String>,
}

impl ClientRegistry {
    /// Sorted list of entries by client_name. Used by `cert list`.
    pub fn sorted_entries(&self) -> Vec<&RegistryEntry> {
        let mut entries: Vec<&RegistryEntry> = self.by_fingerprint.values().collect();
        entries.sort_by(|a, b| a.client_name.cmp(&b.client_name));
        entries
    }
}

/// Convert a unix timestamp (seconds, UTC) to `(year, month, day)`.
/// Uses Howard Hinnant's days-from-civil algorithm — no chrono dependency.
pub fn unix_to_ymd(unix_secs: u64) -> (u32, u32, u32) {
    let days = (unix_secs / 86400) as i64;
    // Howard Hinnant's civil_from_days
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    ((y + if m <= 2 { 1 } else { 0 }) as u32, m as u32, d as u32)
}

#[cfg(test)]
pub mod mock;
#[cfg(test)]
pub use mock::MockCertClient;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_sorted_by_client_name() {
        let mut reg = ClientRegistry::default();
        reg.by_fingerprint.insert(
            "fp_b".into(),
            RegistryEntry {
                client_name: "fuse-client-2".into(),
                client_id: None,
                san_ips: vec!["172.30.0.42".into()],
                mount_dirs: vec!["/mnt/powerfs".into()],
                issued_at: 0,
                expires_at: 0,
                cert_fingerprint_sha256: "fp_b".into(),
                revoked: false,
            },
        );
        reg.by_fingerprint.insert(
            "fp_a".into(),
            RegistryEntry {
                client_name: "filer-1".into(),
                client_id: None,
                san_ips: vec!["172.30.0.31".into()],
                mount_dirs: vec![],
                issued_at: 0,
                expires_at: 0,
                cert_fingerprint_sha256: "fp_a".into(),
                revoked: false,
            },
        );
        let sorted = reg.sorted_entries();
        assert_eq!(sorted[0].client_name, "filer-1");
        assert_eq!(sorted[1].client_name, "fuse-client-2");
    }

    #[test]
    fn unix_to_ymd_known_dates() {
        // 1970-01-01 = epoch 0
        assert_eq!(unix_to_ymd(0), (1970, 1, 1));
        // 2025-01-01 00:00 UTC = 1735689600
        assert_eq!(unix_to_ymd(1735689600), (2025, 1, 1));
        // 2025-12-31 00:00 UTC = 1767139200
        assert_eq!(unix_to_ymd(1767139200), (2025, 12, 31));
        // 2024-02-29 (leap day) = 1709164800
        assert_eq!(unix_to_ymd(1709164800), (2024, 2, 29));
        // 2100-03-01 (2100 is NOT a leap year — Gregorian century rule) = 4107542400
        assert_eq!(unix_to_ymd(4107542400), (2100, 3, 1));
    }

    #[test]
    fn registry_entry_absorbs_unknown_fields() {
        // master might add fields later; serde should not fail.
        let json = r#"{
            "client_name": "x",
            "client_id": null,
            "san_ips": [],
            "mount_dirs": [],
            "issued_at": 1,
            "expires_at": 2,
            "cert_fingerprint_sha256": "fp",
            "revoked": false,
            "future_field": "ignored"
        }"#;
        let e: RegistryEntry = serde_json::from_str(json).unwrap();
        assert_eq!(e.client_name, "x");
    }

    #[test]
    fn client_registry_default_is_empty() {
        let r = ClientRegistry::default();
        assert!(r.by_fingerprint.is_empty());
        assert!(r.by_client_name.is_empty());
        assert!(r.sorted_entries().is_empty());
    }
}
