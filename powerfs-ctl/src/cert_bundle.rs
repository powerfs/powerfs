//! Offline demo certificate bundle generator.
//!
//! `cert demo-bundle` mints a complete, self-contained certificate set for
//! the single-container demo image (docker/demo/Dockerfile): a fresh CA plus
//! one filer node cert, one volume node cert, and one FUSE client cert, all
//! bound to 127.0.0.1, together with the client_registry.json the master
//! loads from its ca_dir.
//!
//! Every leaf — storage nodes included — is signed exactly like
//! CaManager::sign_client_cert_v2 and entered into the registry: the
//! master's validate_server_node_pem looks node certs up BY FINGERPRINT in
//! the same registry and checks san_ips and client_name==node_id, so merely
//! chaining the cert against the CA is not enough.
//!
//! The output format MUST stay byte-compatible with what powerfs-master's
//! CaManager reads (powerfs-master/src/ca_manager.rs): the registry JSON
//! schema and the SAN URI conventions (`urn:powerfs:mount:<path>` and
//! `urn:powerfs:client:<name>`). If CaManager changes, mirror it here.
//!
//! SECURITY: the CA private key is written into the bundle and ends up
//! baked into the demo image. That is acceptable for an ephemeral,
//! loopback-only trial image and matches the docker/certs-default precedent;
//! it must never be used for a real deployment.

use rcgen::{
    CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType,
};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use time::OffsetDateTime;

const MOUNT_URI_PREFIX: &str = "urn:powerfs:mount:";
const CLIENT_URI_PREFIX: &str = "urn:powerfs:client:";
const ONE_YEAR_SECS: u64 = 365 * 24 * 60 * 60;

/// Parameters of the loopback demo bundle. Kept explicit so the Dockerfile
/// build and anyone regenerating the bundle see exactly what it contains.
pub struct DemoBundleSpec {
    pub filer_name: String,
    pub volume_name: String,
    pub client_name: String,
    pub san_ip: String,
    pub mount_dir: String,
}

impl Default for DemoBundleSpec {
    fn default() -> Self {
        Self {
            filer_name: "filer-1".into(),
            volume_name: "volume-1".into(),
            client_name: "fuse-demo".into(),
            san_ip: "127.0.0.1".into(),
            mount_dir: "/mnt/powerfs".into(),
        }
    }
}

struct Leaf {
    name: String,
    pem: String,
    key_pem: String,
    fingerprint: String,
    mount_dirs: Vec<String>,
}

/// Generate the bundle into `dir` (created if missing). Existing files are
/// overwritten — the bundle is a derived artifact, never state to merge.
pub fn generate(dir: &Path, spec: &DemoBundleSpec) -> Result<(), String> {
    fs::create_dir_all(dir).map_err(|e| format!("create {}: {}", dir.display(), e))?;

    // ---- CA (mirrors CaManager::new self-signed branch) ----
    let mut ca_params = CertificateParams::new(vec![]).map_err(|e| e.to_string())?;
    ca_params.distinguished_name = DistinguishedName::new();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "PowerFS Master CA");
    ca_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let now = OffsetDateTime::now_utc();
    ca_params.not_before = now;
    ca_params.not_after = now + time::Duration::days(3650);
    let ca_key = KeyPair::generate().map_err(|e| e.to_string())?;
    let ca_cert = ca_params.self_signed(&ca_key).map_err(|e| e.to_string())?;
    write_file(dir, "ca.crt", &ca_cert.pem())?;
    write_secret(dir, "ca.key", &ca_key.serialize_pem())?;

    let ip: std::net::IpAddr = spec
        .san_ip
        .parse()
        .map_err(|e| format!("invalid san ip {}: {}", spec.san_ip, e))?;

    // ---- leaves: two storage nodes (mount_dirs=[]) + one fuse client ----
    let filer = sign_v2(&ca_cert, &ca_key, &spec.filer_name, ip, &[])?;
    let volume = sign_v2(&ca_cert, &ca_key, &spec.volume_name, ip, &[])?;
    let fuse = sign_v2(
        &ca_cert,
        &ca_key,
        &spec.client_name,
        ip,
        &[spec.mount_dir.as_str()],
    )?;

    for leaf in [&filer, &volume, &fuse] {
        write_file(dir, &format!("{}.crt", leaf.name), &leaf.pem)?;
        write_secret(dir, &format!("{}.key", leaf.name), &leaf.key_pem)?;
    }

    // ---- client_registry.json (schema mirrors ClientRegistry in ca_manager.rs) ----
    let issued_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut by_fingerprint = Map::new();
    let mut by_client_name = Map::new();
    for leaf in [&filer, &volume, &fuse] {
        let entry = json!({
            "client_name": leaf.name,
            "client_id": null,
            "san_ips": [spec.san_ip],
            "mount_dirs": leaf.mount_dirs,
            "issued_at": issued_at,
            "expires_at": issued_at + ONE_YEAR_SECS,
            "cert_fingerprint_sha256": leaf.fingerprint,
            "revoked": false
        });
        by_fingerprint.insert(leaf.fingerprint.clone(), entry);
        by_client_name.insert(leaf.name.clone(), Value::String(leaf.fingerprint.clone()));
    }
    let registry = json!({
        "by_fingerprint": by_fingerprint,
        "by_client_name": by_client_name
    });
    write_file(
        dir,
        "client_registry.json",
        serde_json::to_vec_pretty(&registry).map_err(|e| e.to_string())?,
    )?;

    Ok(())
}

/// Sign one leaf the way CaManager::sign_client_cert_v2 does:
/// SAN IPs + one `urn:powerfs:client:<name>` URI + one
/// `urn:powerfs:mount:<dir>` URI per mount, CN=name, ClientAuth EKU.
/// Storage nodes pass `mount_dirs=[]` and are validated at runtime through
/// validate_server_node_pem against the same registry.
fn sign_v2(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    name: &str,
    ip: std::net::IpAddr,
    mount_dirs: &[&str],
) -> Result<Leaf, String> {
    let mut sans = Vec::with_capacity(mount_dirs.len() + 2);
    sans.push(SanType::IpAddress(ip));
    for m in mount_dirs {
        sans.push(SanType::URI(
            format!("{}{}", MOUNT_URI_PREFIX, m)
                .try_into()
                .map_err(|e: rcgen::Error| e.to_string())?,
        ));
    }
    sans.push(SanType::URI(
        format!("{}{}", CLIENT_URI_PREFIX, name)
            .try_into()
            .map_err(|e: rcgen::Error| e.to_string())?,
    ));

    let mut params = CertificateParams::new(vec![]).map_err(|e| e.to_string())?;
    params.subject_alt_names = sans;
    params.distinguished_name = DistinguishedName::new();
    params.distinguished_name.push(DnType::CommonName, name);
    let now = OffsetDateTime::now_utc();
    params.not_before = now;
    params.not_after = now + time::Duration::days(365);
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];

    let key = KeyPair::generate().map_err(|e| e.to_string())?;
    let cert = params
        .signed_by(&key, ca_cert, ca_key)
        .map_err(|e| e.to_string())?;
    Ok(Leaf {
        name: name.to_string(),
        pem: cert.pem(),
        key_pem: key.serialize_pem(),
        fingerprint: fingerprint_der(cert.der()),
        mount_dirs: mount_dirs.iter().map(|s| s.to_string()).collect(),
    })
}

fn fingerprint_der(der: &[u8]) -> String {
    let dgst = Sha256::digest(der);
    let mut out = String::with_capacity(dgst.len() * 2);
    for b in dgst {
        use std::fmt::Write;
        let _ = write!(out, "{:02x}", b);
    }
    out
}

fn write_file(dir: &Path, name: &str, content: impl AsRef<[u8]>) -> Result<(), String> {
    let p = dir.join(name);
    fs::write(&p, content).map_err(|e| format!("write {}: {}", p.display(), e))?;
    Ok(())
}

fn write_secret(dir: &Path, name: &str, content: impl AsRef<[u8]>) -> Result<(), String> {
    write_file(dir, name, content.as_ref())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let p = dir.join(name);
        fs::set_permissions(&p, fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("chmod {}: {}", p.display(), e))?;
    }
    Ok(())
}
