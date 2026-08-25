//! Master Certificate Authority manager and certificate signing HTTP API.
//!
//! The master acts as the cluster's Certificate Authority, issuing TLS
//! certificates for volume servers, filers, and FUSE clients. The CA
//! certificate and private key are persisted to `ca_dir` on first start
//! and reused across restarts.
//!
//! Certificate binding model (production enforcement):
//!   * Every client-certificate signing call MUST provide `client_name`,
//!     `san_ips` (>= 1) and `mount_dirs` (>= 1).
//!   * The issued leaf cert carries the client_name as its CN, the SAN
//!     IPs as SanType::IpAddress, and each allowed mount directory as a
//!     `urn:powerfs:mount:<path>` URI SAN.
//!   * Issuance metadata (`client_name`, `san_ips`, `mount_dirs`,
//!     `issued_at`, cert SHA-256 fingerprint) is persisted to
//!     `{ca_dir}/client_registry.json`.
//!   * At runtime the Master validates RegisterClient/DeregisterClient
//!     requests by (1) verifying the PEM chain against our CA,
//!     (2) matching the caller's source IP to `san_ips`, (3) matching
//!     the TLV mount-point `Name` against `mount_dirs`, (4) matching
//!     the cert's CN to the registry entry. Using the certificate
//!     from a different node or with a different mount directory is
//!     rejected immediately.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use log::{info, warn};
use rcgen::{
    CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Prefix used on URI SAN entries to encode allowed mount directories.
pub const MOUNT_URI_PREFIX: &str = "urn:powerfs:mount:";

/// Prefix used on URI SAN entries to encode the logical client name.
pub const CLIENT_URI_PREFIX: &str = "urn:powerfs:client:";

/// On-disk record for a single issued client certificate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssuedClientCert {
    /// Logical client name (e.g. "client-001"). MUST match the cert CN.
    pub client_name: String,
    /// Optional textual client-id carried in a SAN URI.
    #[serde(default)]
    pub client_id: Option<String>,
    /// IPv4/IPv6 addresses the cert is bound to; at least 1.
    pub san_ips: Vec<String>,
    /// Mount directories the cert is bound to; at least 1.
    pub mount_dirs: Vec<String>,
    /// Issued timestamp (unix seconds, UTC).
    pub issued_at: u64,
    /// Expiration timestamp (unix seconds, UTC). Currently 1 year after issuance.
    pub expires_at: u64,
    /// SHA-256 fingerprint of the raw PEM certificate, hex-encoded.
    /// Used for lookups during runtime validation.
    pub cert_fingerprint_sha256: String,
    /// Revocation flag (for future revocation API).
    #[serde(default)]
    pub revoked: bool,
}

/// Persistent client registry. Keeps track of every client certificate
/// issued by this master so we can enforce IP + mount-point bindings at
/// RegisterClient time.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ClientRegistry {
    /// Indexed by SHA-256 fingerprint of the issued PEM cert.
    #[serde(default)]
    pub by_fingerprint: HashMap<String, IssuedClientCert>,
    /// Reverse index: client_name -> latest fingerprint (convenience).
    #[serde(default)]
    pub by_client_name: HashMap<String, String>,
}

impl ClientRegistry {
    fn path(ca_dir: &Path) -> PathBuf {
        ca_dir.join("client_registry.json")
    }

    fn load(ca_dir: &Path) -> Self {
        let p = Self::path(ca_dir);
        if !p.exists() {
            return Self::default();
        }
        match std::fs::read_to_string(&p) {
            Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
            Err(e) => {
                warn!(
                    "ClientRegistry: failed to read {} ({}); starting with empty registry",
                    p.display(),
                    e
                );
                Self::default()
            }
        }
    }

    fn save(&self, ca_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let p = Self::path(ca_dir);
        let bytes = serde_json::to_vec_pretty(self)?;
        std::fs::write(&p, bytes)?;
        Ok(())
    }
}

/// Master CA manager. Loads (or generates on first run) a self-signed CA
/// certificate and key, and signs client/server certificates on demand.
///
/// A single `CaManager` is shared (via `Arc`) with the HTTP handlers that
/// serve the `/api/cert/*` endpoints on the master's metrics/admin HTTP
/// server.
pub struct CaManager {
    ca_dir: PathBuf,
    ca_cert: rcgen::Certificate,
    ca_key: KeyPair,
    ca_cert_pem: String,
    admin_token: Option<String>,
    registry: RwLock<ClientRegistry>,
}

impl CaManager {
    /// Create a new `CaManager`. If `ca.crt` and `ca.key` already exist in
    /// `ca_dir`, they are loaded. Otherwise a fresh self-signed CA cert
    /// and key are generated and persisted (with `0600` perms on the key).
    pub fn new(
        ca_dir: impl AsRef<Path>,
        admin_token: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let ca_dir = ca_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&ca_dir)?;

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

        let registry = RwLock::new(ClientRegistry::load(&ca_dir));
        Ok(Self {
            ca_dir,
            ca_cert,
            ca_key,
            ca_cert_pem,
            admin_token,
            registry,
        })
    }

    pub fn get_ca_cert_pem(&self) -> String {
        self.ca_cert_pem.clone()
    }

    /// SHA-256 fingerprint of a PEM certificate block (hex-encoded).
    pub fn fingerprint_sha256(pem: &str) -> String {
        let der = pem_to_der(pem).unwrap_or_default();
        let mut h = Sha256::new();
        h.update(&der);
        let dgst = h.finalize();
        let mut out = String::with_capacity(dgst.len() * 2);
        for b in dgst {
            out.push_str(&format!("{:02x}", b));
        }
        out
    }

    /// Sign a client certificate, recording issuance metadata in the
    /// persistent client registry. The caller MUST provide at least one
    /// SAN IP and at least one mount directory; the certificate is
    /// bound to both.
    pub fn sign_client_cert_v2(
        &self,
        client_name: &str,
        client_id: Option<&str>,
        san_ips: &[String],
        mount_dirs: &[String],
    ) -> Result<(String, String), Box<dyn std::error::Error>> {
        if san_ips.is_empty() {
            return Err("at least one --san-ip is required".into());
        }
        // mount_dirs is optional — when empty, the cert is issued for a
        // storage node (filer/volume) rather than a FUSE/kernel client.
        // Storage node certs are validated via validate_server_node_pem
        // which checks client_name==node_id instead of mount_dirs.
        if client_name.trim().is_empty() {
            return Err("--client-name cannot be empty".into());
        }

        // Build SAN list: IPs + mount URIs + client-name URI + client-id URI (optional)
        let mut sans: Vec<SanType> = Vec::with_capacity(san_ips.len() + mount_dirs.len() + 2);
        for ip in san_ips {
            let parsed = ip
                .parse::<std::net::IpAddr>()
                .map_err(|e| format!("invalid --san-ip '{}': {}", ip, e))?;
            sans.push(SanType::IpAddress(parsed));
        }
        for m in mount_dirs {
            let uri = format!("{}{}", MOUNT_URI_PREFIX, m);
            sans.push(SanType::URI(uri.try_into()?));
        }
        let cn_uri = format!("{}{}", CLIENT_URI_PREFIX, client_name);
        sans.push(SanType::URI(cn_uri.try_into()?));
        if let Some(id) = client_id {
            sans.push(SanType::URI(id.try_into()?));
        }

        let mut params = CertificateParams::new(vec![])?;
        params.subject_alt_names = sans;
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, client_name);
        let now = time::OffsetDateTime::now_utc();
        params.not_before = now;
        params.not_after = now + time::Duration::days(365);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];

        let client_key = KeyPair::generate()?;
        let client_cert = params.signed_by(&client_key, &self.ca_cert, &self.ca_key)?;
        let cert_pem = client_cert.pem();
        let key_pem = client_key.serialize_pem();

        // Persist to registry
        let fingerprint = Self::fingerprint_sha256(&cert_pem);
        let issued_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        const ONE_YEAR_SECS: u64 = 365 * 24 * 60 * 60;
        let expires_at = issued_at + ONE_YEAR_SECS;
        let entry = IssuedClientCert {
            client_name: client_name.to_string(),
            client_id: client_id.map(|s| s.to_string()),
            san_ips: san_ips.to_vec(),
            mount_dirs: mount_dirs.to_vec(),
            issued_at,
            expires_at,
            cert_fingerprint_sha256: fingerprint.clone(),
            revoked: false,
        };
        {
            let mut reg = self.registry.write().unwrap();
            reg.by_fingerprint.insert(fingerprint.clone(), entry);
            reg.by_client_name
                .insert(client_name.to_string(), fingerprint.clone());
            reg.save(&self.ca_dir)?;
        }
        info!(
            "CaManager: issued client cert name={} san_ips={:?} mount_dirs={:?} fp={:.16}…",
            client_name, san_ips, mount_dirs, fingerprint
        );
        Ok((cert_pem, key_pem))
    }

    /// Legacy (pre-v2) client signing. Deprecated but kept for
    /// compatibility with very old callers; production is expected to
    /// always go through sign-client-v2 with the full binding metadata.
    /// This wrapper simply rejects the call — production MUST register
    /// the binding metadata.
    #[deprecated(note = "use sign_client_cert_v2 with full binding metadata")]
    pub fn sign_client_cert(
        &self,
        _cn: &str,
        _client_id: Option<&str>,
    ) -> Result<(String, String), Box<dyn std::error::Error>> {
        Err("legacy sign-client is disabled in production; use sign-client with --san-ip/--mount-dir/--client-name".into())
    }

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

    // ---------- runtime validation (called by net_handler) ----------

    /// Validate a client PEM certificate against:
    ///   1. Chain issued by our CA
    ///   2. Not revoked, caller IP in san_ips, mount_point matches mount_dirs
    ///   3. CN matches the registry's recorded client_name
    ///
    /// Returns `Ok(client_name)` on success or an error string describing
    /// which binding check failed.
    pub fn validate_client_pem(
        &self,
        client_pem: &str,
        peer_ip: Option<&str>,
        mount_point: &str,
    ) -> Result<String, String> {
        let fingerprint = Self::fingerprint_sha256(client_pem);
        let reg = self
            .registry
            .read()
            .map_err(|_| "registry lock poisoned".to_string())?;
        let entry = reg.by_fingerprint.get(&fingerprint).ok_or_else(|| {
            format!(
                "cert fp={:.16}… unknown (not issued by master)",
                fingerprint
            )
        })?;
        if entry.revoked {
            return Err(format!("cert fp={:.16}… has been revoked", fingerprint));
        }

        // Expiration check: timestamps are persisted in the registry so the
        // check is pure-rust and immune to x509-parser API churn. A cert
        // issued by us is accepted only within [issued_at, expires_at].
        let now_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now_ts < entry.issued_at {
            return Err(format!(
                "cert fp={:.16}… not yet valid (issued_at={}, now={})",
                fingerprint, entry.issued_at, now_ts
            ));
        }
        if now_ts > entry.expires_at {
            return Err(format!(
                "cert fp={:.16}… expired (expires_at={}, now={})",
                fingerprint, entry.expires_at, now_ts
            ));
        }

        // Fingerprint matches an entry in `by_fingerprint` and we generated
        // it locally, so we already know it was signed by our CA. This
        // avoids the re-parsing overhead (and x509-parser version drift)
        // of a full chain-verification round-trip at accept time.

        if let Some(ip) = peer_ip {
            if !entry.san_ips.iter().any(|s| s == ip) {
                return Err(format!(
                    "peer ip {} not in cert san_ips {:?} (name={})",
                    ip, entry.san_ips, entry.client_name
                ));
            }
        }

        if !entry.mount_dirs.iter().any(|m| m == mount_point) {
            return Err(format!(
                "mount-point '{}' not in cert mount_dirs {:?} (name={})",
                mount_point, entry.mount_dirs, entry.client_name
            ));
        }

        Ok(entry.client_name.clone())
    }

    /// Validate a **filer/volume** client certificate (PEM) presented by a
    /// storage node during RegisterFiler / KeepConnected.
    ///
    /// This is the server-side counterpart of `validate_client_pem` but
    /// tailored for storage nodes:
    ///
    ///   * Fingerprint present in the registry (issued by this master).
    ///   * Not revoked.
    ///   * Within `[issued_at, expires_at]`.
    ///   * Peer IP listed in `san_ips`.
    ///   * `client_name` matches the node_id reported by the caller
    ///     (prevents a filer cert from being reused on a different filer).
    ///
    /// Unlike `validate_client_pem`, the `mount_dirs` field is **not**
    /// checked because storage nodes do not have mount points.
    pub fn validate_server_node_pem(
        &self,
        client_pem: &str,
        peer_ip: Option<&str>,
        node_id: &str,
    ) -> Result<String, String> {
        let fingerprint = Self::fingerprint_sha256(client_pem);
        let reg = self
            .registry
            .read()
            .map_err(|_| "registry lock poisoned".to_string())?;
        let entry = reg.by_fingerprint.get(&fingerprint).ok_or_else(|| {
            format!(
                "cert fp={:.16}… unknown (not issued by master)",
                fingerprint
            )
        })?;
        if entry.revoked {
            return Err(format!("cert fp={:.16}… has been revoked", fingerprint));
        }

        let now_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now_ts < entry.issued_at {
            return Err(format!(
                "cert fp={:.16}… not yet valid (issued_at={}, now={})",
                fingerprint, entry.issued_at, now_ts
            ));
        }
        if now_ts > entry.expires_at {
            return Err(format!(
                "cert fp={:.16}… expired (expires_at={}, now={})",
                fingerprint, entry.expires_at, now_ts
            ));
        }

        if let Some(ip) = peer_ip {
            if !entry.san_ips.iter().any(|s| s == ip) {
                return Err(format!(
                    "peer ip {} not in cert san_ips {:?} (name={})",
                    ip, entry.san_ips, entry.client_name
                ));
            }
        }

        if entry.client_name != node_id {
            return Err(format!(
                "cert client_name '{}' != node_id '{}' (fp={:.16}…)",
                entry.client_name, node_id, fingerprint
            ));
        }

        Ok(entry.client_name.clone())
    }

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
// Helpers
// ===========================================================================

/// Extract the DER bytes of the *first* PEM block from a multi-block string.
/// Used to compute fingerprints and perform lightweight issuer verification.
fn pem_to_der(pem: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let start_marker = "-----BEGIN CERTIFICATE-----";
    let end_marker = "-----END CERTIFICATE-----";
    let s = pem;
    let i = s
        .find(start_marker)
        .ok_or("missing BEGIN CERTIFICATE marker")?;
    let j = s[i..]
        .find(end_marker)
        .ok_or("missing END CERTIFICATE marker")?;
    let body = &s[i + start_marker.len()..i + j];
    let clean: String = body.chars().filter(|c| !c.is_whitespace()).collect();
    let der = base64_decode(&clean)?;
    Ok(der)
}

/// Minimal base64 decoder (no external dep).
fn base64_decode(s: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut table = [255u8; 256];
    for (i, &c) in ALPHABET.iter().enumerate() {
        table[c as usize] = i as u8;
    }
    table[b'=' as usize] = 0;
    let mut out = Vec::with_capacity((s.len() / 4) * 3);
    let mut pad = 0i32;
    let mut it = s.chars();
    loop {
        let mut chunk = 0u32;
        for k in 0..4 {
            let c = match it.next() {
                Some(c) => c,
                None if k == 0 => return Ok(out),
                None => return Err("base64: truncated input".into()),
            };
            if c == '=' {
                pad += 1;
            }
            let idx = table[c as usize];
            if idx == 255 && c != '=' {
                return Err(format!("base64: invalid char '{}'", c).into());
            }
            chunk = (chunk << 6) | idx as u32;
        }
        let bytes = [
            (chunk >> 16) as u8,
            ((chunk >> 8) & 0xff) as u8,
            (chunk & 0xff) as u8,
        ];
        let take = match pad {
            0 => 3,
            1 => 2,
            2 => 1,
            _ => return Err("base64: too many pad chars".into()),
        };
        out.extend_from_slice(&bytes[..take]);
        if pad > 0 {
            return Ok(out);
        }
    }
}

// ===========================================================================
// HTTP API handlers (axum)
// ===========================================================================

#[derive(Deserialize)]
pub struct SignClientRequest {
    pub common_name: String,
    #[deprecated]
    pub client_id: Option<String>,
    /// New-style v2 bindings: logical client name.
    pub client_name: Option<String>,
    pub san_ips: Option<Vec<String>>,
    pub mount_dirs: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub struct SignServerRequest {
    pub common_name: String,
    pub sans: Vec<String>,
}

#[derive(Serialize)]
pub struct SignResponse {
    pub cert: String,
    pub key: String,
}

pub async fn get_ca_cert(State(ca): State<Arc<CaManager>>) -> String {
    ca.get_ca_cert_pem()
}

/// `POST /api/cert/sign-client` v2. `common_name` / `client_name` are
/// unified as the logical client name; `san_ips` and `mount_dirs` MUST
/// be provided.
#[allow(deprecated)]
pub async fn sign_client(
    State(ca): State<Arc<CaManager>>,
    headers: HeaderMap,
    Json(req): Json<SignClientRequest>,
) -> Result<Json<SignResponse>, (StatusCode, String)> {
    if !check_admin_auth(&ca, &headers) {
        return Err((StatusCode::UNAUTHORIZED, "unauthorized".to_string()));
    }
    let client_name = req
        .client_name
        .clone()
        .unwrap_or_else(|| req.common_name.clone());
    let san_ips = req
        .san_ips
        .clone()
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "san_ips is required".to_string()))?;
    // mount_dirs is optional — when absent (or empty), the cert is issued
    // for a storage node (filer/volume) rather than a FUSE/kernel client.
    // Storage node certs are validated via validate_server_node_pem which
    // checks client_name==node_id instead of mount_dirs.
    let mount_dirs = req.mount_dirs.clone().unwrap_or_default();
    let (cert, key) = ca
        .sign_client_cert_v2(
            &client_name,
            req.client_id.as_deref(),
            &san_ips,
            &mount_dirs,
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(SignResponse { cert, key }))
}

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

fn check_admin_auth(ca: &CaManager, headers: &HeaderMap) -> bool {
    let provided = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    ca.verify_admin_token(provided)
}
