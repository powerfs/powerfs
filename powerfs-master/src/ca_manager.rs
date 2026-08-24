//! Master Certificate Authority manager and certificate signing HTTP API.
//!
//! The master acts as the cluster's Certificate Authority, issuing TLS
//! certificates for volume servers, filers, and FUSE clients. The CA
//! certificate and private key are persisted to `ca_dir` on first start
//! and reused across restarts.

use std::path::Path;
use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use log::info;
use rcgen::{
    CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType,
};
use serde::{Deserialize, Serialize};

/// Master CA manager. Loads (or generates on first run) a self-signed CA
/// certificate and key, and signs client/server certificates on demand.
///
/// A single `CaManager` is shared (via `Arc`) with the HTTP handlers that
/// serve the `/api/cert/*` endpoints on the master's metrics/admin HTTP
/// server.
pub struct CaManager {
    /// Reconstructed in-memory CA certificate (used as the issuer when
    /// signing leaf certs).
    ca_cert: rcgen::Certificate,
    /// CA private key (used to sign leaf certs).
    ca_key: KeyPair,
    /// Cached CA certificate PEM (returned by `GET /api/cert/ca`).
    ca_cert_pem: String,
    /// Optional admin bearer token. When `None` or empty, admin endpoints
    /// run in dev mode (no auth). When set, callers must send
    /// `Authorization: Bearer <token>`.
    admin_token: Option<String>,
}

impl CaManager {
    /// Create a new `CaManager`. If `ca.crt` and `ca.key` already exist in
    /// `ca_dir`, they are loaded. Otherwise a fresh self-signed CA cert
    /// and key are generated and persisted (with `0600` perms on the key).
    ///
    /// `admin_token` is stored for bearer-token validation by the HTTP
    /// handlers; pass `None` for dev mode.
    pub fn new(
        ca_dir: impl AsRef<Path>,
        admin_token: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let ca_dir = ca_dir.as_ref();
        std::fs::create_dir_all(ca_dir)?;

        let cert_path = ca_dir.join("ca.crt");
        let key_path = ca_dir.join("ca.key");

        let (ca_cert, ca_key, ca_cert_pem) = if cert_path.exists() && key_path.exists() {
            let cert_pem = std::fs::read_to_string(&cert_path)?;
            let key_pem = std::fs::read_to_string(&key_path)?;
            let ca_key = KeyPair::from_pem(&key_pem)?;
            let params = CertificateParams::from_ca_cert_pem(&cert_pem)?;
            let ca_cert = params.self_signed(&ca_key)?;
            info!(
                "CaManager: loaded existing CA cert from {}",
                cert_path.display()
            );
            (ca_cert, ca_key, cert_pem)
        } else {
            let mut params = CertificateParams::new(vec![])?;
            params.distinguished_name = DistinguishedName::new();
            params
                .distinguished_name
                .push(DnType::CommonName, "PowerFS Master CA");
            params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let now = time::OffsetDateTime::now_utc();
            params.not_before = now;
            // CA certs are long-lived: 10 years.
            params.not_after = now + time::Duration::days(3650);

            let ca_key = KeyPair::generate()?;
            let ca_cert = params.self_signed(&ca_key)?;
            let cert_pem = ca_cert.pem();
            let key_pem = ca_key.serialize_pem();
            std::fs::write(&cert_path, &cert_pem)?;
            std::fs::write(&key_path, &key_pem)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600));
            }
            info!(
                "CaManager: generated new self-signed CA cert at {}",
                cert_path.display()
            );
            (ca_cert, ca_key, cert_pem)
        };

        Ok(Self {
            ca_cert,
            ca_key,
            ca_cert_pem,
            admin_token,
        })
    }

    /// Return the CA certificate PEM (so clients can trust it and verify
    /// server certs issued by this master).
    pub fn get_ca_cert_pem(&self) -> String {
        self.ca_cert_pem.clone()
    }

    /// Sign a client certificate. Uses `ExtendedKeyUsagePurpose::ClientAuth`.
    /// If `client_id` is provided, it is embedded as a URI SAN so the master
    /// can identify the cert holder.
    pub fn sign_client_cert(
        &self,
        cn: &str,
        client_id: Option<&str>,
    ) -> Result<(String, String), Box<dyn std::error::Error>> {
        // CertificateParams::new takes Vec<String> (auto IP/DNS detection),
        // so for a URI SAN we must build SanType entries and assign them to
        // the subject_alt_names field directly.
        let mut sans = Vec::new();
        if let Some(id) = client_id {
            sans.push(SanType::URI(id.try_into()?));
        }
        let mut params = CertificateParams::new(vec![])?;
        params.subject_alt_names = sans;
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, cn);
        let now = time::OffsetDateTime::now_utc();
        params.not_before = now;
        params.not_after = now + time::Duration::days(365);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];

        let client_key = KeyPair::generate()?;
        let client_cert = params.signed_by(&client_key, &self.ca_cert, &self.ca_key)?;
        let cert_pem = client_cert.pem();
        let key_pem = client_key.serialize_pem();
        Ok((cert_pem, key_pem))
    }

    /// Sign a server certificate. Uses `ExtendedKeyUsagePurpose::ServerAuth`.
    /// Each SAN is auto-detected as an IP address or DNS name.
    pub fn sign_server_cert(
        &self,
        cn: &str,
        sans: &[String],
    ) -> Result<(String, String), Box<dyn std::error::Error>> {
        let mut san_types = Vec::with_capacity(sans.len());
        for san in sans {
            if let Ok(ip) = san.parse::<std::net::IpAddr>() {
                san_types.push(SanType::IpAddress(ip));
            } else {
                // Ia5String implements TryFrom<&str> (not &String), so deref.
                san_types.push(SanType::DnsName(san.as_str().try_into()?));
            }
        }
        let mut params = CertificateParams::new(vec![])?;
        params.subject_alt_names = san_types;
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, cn);
        let now = time::OffsetDateTime::now_utc();
        params.not_before = now;
        params.not_after = now + time::Duration::days(365);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];

        let server_key = KeyPair::generate()?;
        let server_cert = params.signed_by(&server_key, &self.ca_cert, &self.ca_key)?;
        let cert_pem = server_cert.pem();
        let key_pem = server_key.serialize_pem();
        Ok((cert_pem, key_pem))
    }

    /// Verify a provided admin bearer token.
    ///
    /// Returns `true` (dev mode) when no admin token is configured or it is
    /// empty. Otherwise compares the provided token against the expected one
    /// using a constant-time comparison to mitigate timing attacks.
    pub fn verify_admin_token(&self, provided: &str) -> bool {
        match &self.admin_token {
            Some(expected) if !expected.is_empty() => {
                let a = provided.as_bytes();
                let b = expected.as_bytes();
                if a.len() != b.len() {
                    return false;
                }
                let mut diff: u8 = 0;
                for (x, y) in a.iter().zip(b.iter()) {
                    diff |= x ^ y;
                }
                diff == 0
            }
            _ => true,
        }
    }
}

// ===========================================================================
// HTTP API handlers (axum)
// ===========================================================================
//
// These handlers are mounted on the master's metrics/admin HTTP server (the
// same axum `Router` that serves `/metrics`). The router is assembled in
// `crate::metrics::start_metrics_server`; the handlers themselves live here
// to keep all CA/cert logic in one module.

/// Request body for `POST /api/cert/sign-client`.
#[derive(Deserialize)]
pub struct SignClientRequest {
    pub common_name: String,
    pub client_id: Option<String>,
}

/// Request body for `POST /api/cert/sign-server`.
#[derive(Deserialize)]
pub struct SignServerRequest {
    pub common_name: String,
    pub sans: Vec<String>,
}

/// Response body for the sign endpoints.
#[derive(Serialize)]
pub struct SignResponse {
    pub cert: String,
    pub key: String,
}

/// `GET /api/cert/ca` — return the CA certificate PEM as plain text.
/// No auth: clients need the CA cert to verify server certs.
pub async fn get_ca_cert(State(ca): State<Arc<CaManager>>) -> String {
    ca.get_ca_cert_pem()
}

/// `POST /api/cert/sign-client` — sign a client cert. Requires
/// `Authorization: Bearer <admin_token>` when admin token is configured.
pub async fn sign_client(
    State(ca): State<Arc<CaManager>>,
    headers: HeaderMap,
    Json(req): Json<SignClientRequest>,
) -> Result<Json<SignResponse>, (StatusCode, String)> {
    if !check_admin_auth(&ca, &headers) {
        return Err((StatusCode::UNAUTHORIZED, "unauthorized".to_string()));
    }
    let (cert, key) = ca
        .sign_client_cert(&req.common_name, req.client_id.as_deref())
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(SignResponse { cert, key }))
}

/// `POST /api/cert/sign-server` — sign a server cert. Requires
/// `Authorization: Bearer <admin_token>` when admin token is configured.
pub async fn sign_server(
    State(ca): State<Arc<CaManager>>,
    headers: HeaderMap,
    Json(req): Json<SignServerRequest>,
) -> Result<Json<SignResponse>, (StatusCode, String)> {
    if !check_admin_auth(&ca, &headers) {
        return Err((StatusCode::UNAUTHORIZED, "unauthorized".to_string()));
    }
    let (cert, key) = ca
        .sign_server_cert(&req.common_name, &req.sans)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(SignResponse { cert, key }))
}

/// Extract the `Bearer <token>` from the `Authorization` header and validate
/// it via `CaManager::verify_admin_token`. When no admin token is configured
/// (dev mode), `verify_admin_token` returns `true` for any input.
fn check_admin_auth(ca: &CaManager, headers: &HeaderMap) -> bool {
    let provided = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    ca.verify_admin_token(provided)
}
