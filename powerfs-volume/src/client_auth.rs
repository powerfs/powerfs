//! Phase D.5: connection-level authenticator for external KV clients.
//!
//! Unlike the Master, the Volume has no local copy of the certificate
//! registry, so authentication is purely cryptographic: the presented
//! certificate MUST (1) be signed by the cluster CA whose certificate is
//! installed on the node, (2) currently be within its validity period,
//! (3) carry the caller's source IP among its SAN IPs. The registration
//! token provides a second, cluster-wide shared secret (RFC §5.8).
//!
//! Revocation is deliberately not checked here: authorization to read a
//! specific block is gated in real time by the Master's GetBlockMeta — a
//! revoked client receives no locations. The residual window is only the
//! seconds for which a client may cache an already-returned location
//! (RFC §5.6).

use log::warn;
use std::net::IpAddr;
use x509_parser::certificate::X509Certificate;
use x509_parser::extensions::{GeneralName, ParsedExtension};
use x509_parser::pem::parse_x509_pem;

use powerfs_net::{STATUS_ERR_PERMISSION_DENIED, STATUS_ERR_SERVER_ERROR};

/// Verifies KvClient credentials. Cheaply clonable (holds only two
/// strings); shared via `Arc` inside the net handler.
#[derive(Clone)]
pub struct ClientAuthenticator {
    /// PEM-encoded cluster CA certificate (loaded once at startup).
    ca_pem: String,
    /// Expected cluster registration token.
    token: String,
}

/// A failed authentication attempt. `reason` is detailed material for
/// local logs only; the handler decides what (if anything) is returned to
/// the peer.
#[derive(Debug)]
pub struct AuthFailure {
    /// Response status the handler must return.
    pub status: u16,
    /// Detailed reason — logs only, never sent verbatim to the peer.
    pub reason: String,
}

impl AuthFailure {
    /// Peer-caused failure: bad token, bad/foreign/expired cert, wrong IP.
    fn denied(reason: impl Into<String>) -> Self {
        Self {
            status: STATUS_ERR_PERMISSION_DENIED,
            reason: reason.into(),
        }
    }

    /// Local/server-side failure (e.g. broken trust anchor): the peer gets
    /// no internal detail.
    fn server(reason: impl Into<String>) -> Self {
        Self {
            status: STATUS_ERR_SERVER_ERROR,
            reason: reason.into(),
        }
    }
}

impl ClientAuthenticator {
    /// Build the authenticator from locally configured material. Fails
    /// fast at startup if the CA PEM cannot be parsed — a node must never
    /// run with a broken trust anchor (it would reject every client).
    /// An empty registration token is also rejected: the token comparison
    /// alone would otherwise accept an empty client token.
    pub fn new(ca_pem: String, token: String) -> Result<Self, String> {
        if token.is_empty() {
            return Err("registration token must not be empty".into());
        }
        // Validate the trust anchor up front: parse PEM then X.509.
        let (_, ca_pem_block) = parse_x509_pem(ca_pem.as_bytes())
            .map_err(|e| format!("invalid PEM CA certificate: {e}"))?;
        let ca = ca_pem_block
            .parse_x509()
            .map_err(|e| format!("invalid X.509 CA certificate: {e}"))?;
        // A trust anchor with no usable public key cannot verify anything.
        if ca.public_key().subject_public_key.data.is_empty() {
            return Err("CA cert carries an empty public key".into());
        }
        Ok(Self { ca_pem, token })
    }

    /// Authenticate one connection. On success the caller marks its
    /// connection authenticated; on error the returned `AuthFailure`
    /// carries the response status and a log-only reason. Internal details
    /// are never sent to the peer.
    pub fn authenticate(
        &self,
        client_pem: &str,
        provided_token: &str,
        peer_ip: IpAddr,
    ) -> Result<(), AuthFailure> {
        // 1) Registration token — non-empty and constant-time equal.
        if provided_token.is_empty() {
            return Err(AuthFailure::denied("empty registration token"));
        }
        if !constant_time_eq(provided_token.as_bytes(), self.token.as_bytes()) {
            return Err(AuthFailure::denied("invalid registration token"));
        }

        // 2) Parse both certificates. PemBlocks own the decoded bytes and
        //    must outlive the borrowed cert references, so parse inline.
        //    Failures of the LOCAL trust anchor are server-side errors
        //    (their details must not leave this node).
        let (_, ca_pem_block) = parse_x509_pem(self.ca_pem.as_bytes())
            .map_err(|e| AuthFailure::server(format!("invalid local CA PEM: {e}")))?;
        let ca = ca_pem_block
            .parse_x509()
            .map_err(|e| AuthFailure::server(format!("invalid local CA cert: {e}")))?;
        let (_, client_pem_block) = parse_x509_pem(client_pem.as_bytes())
            .map_err(|e| AuthFailure::denied(format!("invalid PEM certificate: {e}")))?;
        let client = client_pem_block
            .parse_x509()
            .map_err(|e| AuthFailure::denied(format!("invalid X.509 certificate: {e}")))?;

        // 3) Chain: the leaf MUST be signed by our CA. This also rejects
        //    foreign/self-signed certificates.
        client
            .verify_signature(Some(ca.public_key()))
            .map_err(|_| AuthFailure::denied("certificate not signed by cluster CA"))?;

        // 4) Issuer name must name our CA (defense in depth alongside the
        //    cryptographic check).
        if client.issuer() != ca.subject() {
            return Err(AuthFailure::denied(
                "certificate issuer does not match cluster CA",
            ));
        }

        // 5) Validity period (epoch seconds; no external time dependency).
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        if now_secs < client.tbs_certificate.validity.not_before.timestamp() {
            return Err(AuthFailure::denied("certificate not yet valid"));
        }
        if now_secs > client.tbs_certificate.validity.not_after.timestamp() {
            return Err(AuthFailure::denied("certificate expired"));
        }

        // 6) Source IP must be listed in the certificate's SAN IPs.
        match cert_contains_san_ip(&client, &peer_ip) {
            Ok(true) => Ok(()),
            Ok(false) => Err(AuthFailure::denied(format!(
                "peer ip {peer_ip} not bound by certificate"
            ))),
            Err(reason) => Err(AuthFailure::server(reason)),
        }
    }
}

/// Whether `cert` lists `ip` in its subjectAltName extension.
fn cert_contains_san_ip(cert: &X509Certificate<'_>, ip: &IpAddr) -> Result<bool, String> {
    for ext in cert.extensions() {
        if let ParsedExtension::SubjectAlternativeName(san) = ext.parsed_extension() {
            for name in &san.general_names {
                if let GeneralName::IPAddress(bytes) = name {
                    if let Some(san_ip) = bytes_to_ip(bytes) {
                        if &san_ip == ip {
                            return Ok(true);
                        }
                    }
                }
            }
        }
    }
    Ok(false)
}

/// Interpret 4/16 raw bytes as an IPv4/IPv6 address.
fn bytes_to_ip(bytes: &[u8]) -> Option<IpAddr> {
    use std::net::{Ipv4Addr, Ipv6Addr};
    match bytes.len() {
        4 => {
            let mut a = [0u8; 4];
            a.copy_from_slice(bytes);
            Some(IpAddr::V4(Ipv4Addr::from(a)))
        }
        16 => {
            let mut a = [0u8; 16];
            a.copy_from_slice(bytes);
            Some(IpAddr::V6(Ipv6Addr::from(a)))
        }
        _ => None,
    }
}

/// Length-leak-resistant byte comparison: returns false on length
/// mismatch without short-circuiting the XOR accumulation itself.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Log helper keeping call sites terse.
pub fn log_auth_failure(peer: &str, reason: &str) {
    warn!("CLIENT_AUTH: rejected KvClient peer={peer}: {reason}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{
        date_time_ymd, BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa,
        KeyPair, SanType,
    };
    use std::net::{Ipv4Addr, Ipv6Addr};

    const TOKEN: &str = "test-cluster-token";

    /// A throwaway self-signed CA with the rcgen handles needed to sign
    /// leaf fixtures.
    struct TestCa {
        pem: String,
        cert: rcgen::Certificate,
        key: KeyPair,
    }

    fn make_ca(common_name: &str) -> TestCa {
        let mut params = CertificateParams::new(vec![]).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.not_before = date_time_ymd(2020, 1, 1);
        params.not_after = date_time_ymd(2040, 1, 1);
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        TestCa {
            pem: cert.pem(),
            cert,
            key,
        }
    }

    /// Validity-window fixture selector for signed leaves.
    #[derive(Clone, Copy)]
    enum Validity {
        Normal,
        Expired,
        Future,
    }

    /// Sign a leaf with explicit SANs and validity window under `ca`.
    fn sign_leaf(ca: &TestCa, common_name: &str, sans: Vec<SanType>, validity: Validity) -> String {
        let mut params = CertificateParams::new(vec![]).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        params.subject_alt_names = sans;
        let (not_before, not_after) = match validity {
            Validity::Normal => (date_time_ymd(2020, 1, 1), date_time_ymd(2040, 1, 1)),
            Validity::Expired => (date_time_ymd(2019, 1, 1), date_time_ymd(2020, 1, 1)),
            Validity::Future => (date_time_ymd(2030, 1, 1), date_time_ymd(2031, 1, 1)),
        };
        params.not_before = not_before;
        params.not_after = not_after;
        let leaf_key = KeyPair::generate().unwrap();
        params
            .signed_by(&leaf_key, &ca.cert, &ca.key)
            .unwrap()
            .pem()
    }

    fn make_authenticator(ca: &TestCa) -> ClientAuthenticator {
        ClientAuthenticator::new(ca.pem.clone(), TOKEN.to_string()).unwrap()
    }

    #[test]
    fn test_construction_rejects_broken_ca() {
        assert!(ClientAuthenticator::new("not a pem".into(), TOKEN.into()).is_err());
    }

    #[test]
    fn test_construction_rejects_empty_token() {
        let ca = make_ca("Test CA");
        assert!(ClientAuthenticator::new(ca.pem.clone(), String::new()).is_err());
    }

    #[test]
    fn test_valid_credentials_accepted() {
        let ca = make_ca("Test CA");
        let peer = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let leaf = sign_leaf(
            &ca,
            "kv-client",
            vec![SanType::IpAddress(peer)],
            Validity::Normal,
        );
        make_authenticator(&ca)
            .authenticate(&leaf, TOKEN, peer)
            .unwrap();
    }

    #[test]
    fn test_valid_ipv6_san_accepted() {
        let ca = make_ca("Test CA");
        let peer = IpAddr::V6(Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 1));
        let leaf = sign_leaf(
            &ca,
            "kv-client-v6",
            vec![SanType::IpAddress(peer)],
            Validity::Normal,
        );
        make_authenticator(&ca)
            .authenticate(&leaf, TOKEN, peer)
            .unwrap();
    }

    #[test]
    fn test_wrong_token_rejected() {
        let ca = make_ca("Test CA");
        let peer = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let leaf = sign_leaf(
            &ca,
            "kv-client",
            vec![SanType::IpAddress(peer)],
            Validity::Normal,
        );
        let failure = make_authenticator(&ca)
            .authenticate(&leaf, "wrong-token", peer)
            .unwrap_err();
        assert!(failure.reason.contains("token"), "got: {}", failure.reason);
        assert_eq!(failure.status, STATUS_ERR_PERMISSION_DENIED);
    }

    #[test]
    fn test_empty_token_rejected() {
        let ca = make_ca("Test CA");
        let peer = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let leaf = sign_leaf(
            &ca,
            "kv-client",
            vec![SanType::IpAddress(peer)],
            Validity::Normal,
        );
        let failure = make_authenticator(&ca)
            .authenticate(&leaf, "", peer)
            .unwrap_err();
        assert_eq!(failure.status, STATUS_ERR_PERMISSION_DENIED);
    }

    #[test]
    fn test_foreign_ca_rejected() {
        let real_ca = make_ca("Real CA");
        let foreign_ca = make_ca("Foreign CA");
        let peer = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let leaf = sign_leaf(
            &foreign_ca,
            "kv-client",
            vec![SanType::IpAddress(peer)],
            Validity::Normal,
        );
        let failure = make_authenticator(&real_ca)
            .authenticate(&leaf, TOKEN, peer)
            .unwrap_err();
        // Token is right; the cryptographic chain must fail.
        assert!(
            failure.reason.contains("CA") || failure.reason.contains("issuer"),
            "got: {}",
            failure.reason
        );
        assert_eq!(failure.status, STATUS_ERR_PERMISSION_DENIED);
    }

    #[test]
    fn test_expired_certificate_rejected() {
        let ca = make_ca("Test CA");
        let peer = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let leaf = sign_leaf(
            &ca,
            "kv-client",
            vec![SanType::IpAddress(peer)],
            Validity::Expired,
        );
        let failure = make_authenticator(&ca)
            .authenticate(&leaf, TOKEN, peer)
            .unwrap_err();
        assert!(
            failure.reason.contains("expired"),
            "got: {}",
            failure.reason
        );
    }

    #[test]
    fn test_not_yet_valid_certificate_rejected() {
        let ca = make_ca("Test CA");
        let peer = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let leaf = sign_leaf(
            &ca,
            "kv-client",
            vec![SanType::IpAddress(peer)],
            Validity::Future,
        );
        let failure = make_authenticator(&ca)
            .authenticate(&leaf, TOKEN, peer)
            .unwrap_err();
        assert!(
            failure.reason.contains("not yet valid"),
            "got: {}",
            failure.reason
        );
    }

    #[test]
    fn test_peer_ip_not_in_san_rejected() {
        let ca = make_ca("Test CA");
        let bound = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let peer = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let leaf = sign_leaf(
            &ca,
            "kv-client",
            vec![SanType::IpAddress(bound)],
            Validity::Normal,
        );
        let failure = make_authenticator(&ca)
            .authenticate(&leaf, TOKEN, peer)
            .unwrap_err();
        assert!(
            failure.reason.contains("not bound"),
            "got: {}",
            failure.reason
        );
    }

    #[test]
    fn test_malformed_client_cert_rejected() {
        let ca = make_ca("Test CA");
        let peer = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        assert!(make_authenticator(&ca)
            .authenticate("not a certificate", TOKEN, peer)
            .is_err());
    }
}
