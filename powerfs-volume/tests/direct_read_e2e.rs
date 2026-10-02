//! Phase D.5 container E2E: KvClient connection-level authentication and
//! the protected DirectReadNeedle entry point.
//!
//! This test is `#[ignore]`d: it requires a live volume reachable over the
//! docker network and credentials/material prepared by the driving shell
//! script, so it never runs during a normal `cargo test`.
//!
//! Inputs via environment variables (see the driving D.5 E2E script):
//! E2E_TARGET, E2E_TOKEN, E2E_CLIENT_CERT, E2E_FID, E2E_EXPECTED_FILE,
//! E2E_NONLOCAL_FID, E2E_MISSING_FID, E2E_SOURCE_IP.

use std::time::Duration;

use powerfs_net::client::{ClientConfig, PowerFsNetClient};
use powerfs_net::serialize::TlvEncoder;
use powerfs_net::{
    ClientType, FieldId, MsgType, STATUS_ERR_NOT_FOUND, STATUS_ERR_PERMISSION_DENIED, STATUS_OK,
};

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing env {name}"))
}

/// Encode an Authenticate request body in the fixed wire order
/// (RegistrationToken, then ClientCert).
fn auth_body(token: &str, cert_pem: &str) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.add_string(FieldId::RegistrationToken, token).unwrap();
    enc.add_string(FieldId::ClientCert, cert_pem).unwrap();
    enc.into_bytes()
}

/// Encode a DirectReadNeedle request body carrying the full fid string.
fn direct_body(fid: &str) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.add_string(FieldId::Fid, fid).unwrap();
    enc.into_bytes()
}

/// Build and connect a fresh KvClient connection to the target volume.
async fn connect_kv(client_id: u64, target_addr: &str, target_port: u16) -> PowerFsNetClient {
    let config = ClientConfig {
        addr: target_addr.to_string(),
        port: target_port,
        client_id,
        client_type: ClientType::KvClient,
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(10),
        max_retries: 1,
        retry_delay: Duration::from_millis(100),
        ..Default::default()
    };
    let client = PowerFsNetClient::new(config);
    client.connect().await.expect("connect to volume");
    client
}

/// Produce a leaf signed by a throwaway foreign CA (not the cluster CA),
/// binding the given source IP. Used for the "not signed by cluster CA"
/// rejection path.
fn foreign_ca_leaf(source_ip: std::net::IpAddr) -> String {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};

    let mut ca_params = CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let mut leaf_params = CertificateParams::new(vec![]).unwrap();
    leaf_params.subject_alt_names = vec![SanType::IpAddress(source_ip)];
    let leaf_key = KeyPair::generate().unwrap();
    leaf_params
        .signed_by(&leaf_key, &ca_cert, &ca_key)
        .unwrap()
        .pem()
}

#[tokio::test]
#[ignore = "container-only; driven by the D.5 E2E script"]
async fn e2e_kv_auth_and_direct_read() {
    let target = env("E2E_TARGET");
    let (target_addr, target_port) = target
        .rsplit_once(':')
        .expect("E2E_TARGET must be host:port");
    let target_port: u16 = target_port.parse().unwrap();

    let token = env("E2E_TOKEN");
    let source_ip: std::net::IpAddr = env("E2E_SOURCE_IP").parse().unwrap();
    let good_cert_pem = std::fs::read_to_string(env("E2E_CLIENT_CERT")).expect("read cert file");
    let good_fid = env("E2E_FID");
    let expected = std::fs::read(env("E2E_EXPECTED_FILE")).expect("read expected payload");
    let nonlocal_fid = env("E2E_NONLOCAL_FID");
    let missing_fid = env("E2E_MISSING_FID");

    let client = connect_kv(900_001, target_addr, target_port).await;

    // ── 1. Ping is permitted without authenticating ──────────────────────
    let resp = client
        .send_request(MsgType::Ping, &[], &[])
        .await
        .expect("ping");
    assert_eq!(
        resp.header.status, STATUS_OK,
        "check 1: ping before auth must succeed"
    );

    // ── 2. Business messages are refused before authentication ───────────
    let resp = client
        .send_request(MsgType::DirectReadNeedle, &direct_body(&good_fid), &[])
        .await
        .expect("direct read frame");
    assert_eq!(
        resp.header.status, STATUS_ERR_PERMISSION_DENIED,
        "check 2: DirectReadNeedle before auth must be denied"
    );

    let resp = client
        .send_request(MsgType::StatFs, &[], &[])
        .await
        .expect("statfs frame");
    assert_eq!(
        resp.header.status, STATUS_ERR_PERMISSION_DENIED,
        "check 3: StatFs before auth must be denied"
    );

    // ── 3. Wrong token rejects and does not authenticate the connection ──
    let resp = client
        .send_request(
            MsgType::Authenticate,
            &auth_body("definitely-wrong-token", &good_cert_pem),
            &[],
        )
        .await
        .expect("auth frame");
    assert_eq!(
        resp.header.status, STATUS_ERR_PERMISSION_DENIED,
        "check 4: wrong token must be denied"
    );

    let resp = client
        .send_request(MsgType::DirectReadNeedle, &direct_body(&good_fid), &[])
        .await
        .expect("direct read frame");
    assert_eq!(
        resp.header.status, STATUS_ERR_PERMISSION_DENIED,
        "check 5: still denied after failed token auth"
    );

    // ── 4. Correct token but a certificate from a foreign CA rejects ─────
    let foreign_leaf = foreign_ca_leaf(source_ip);
    let resp = client
        .send_request(
            MsgType::Authenticate,
            &auth_body(&token, &foreign_leaf),
            &[],
        )
        .await
        .expect("auth frame");
    assert_eq!(
        resp.header.status, STATUS_ERR_PERMISSION_DENIED,
        "check 6: foreign-CA cert must be denied"
    );

    let resp = client
        .send_request(MsgType::DirectReadNeedle, &direct_body(&good_fid), &[])
        .await
        .expect("direct read frame");
    assert_eq!(
        resp.header.status, STATUS_ERR_PERMISSION_DENIED,
        "check 7: still denied after foreign-CA auth"
    );

    // ── 5. Correct credentials on the SAME connection authenticate it ────
    let resp = client
        .send_request(
            MsgType::Authenticate,
            &auth_body(&token, &good_cert_pem),
            &[],
        )
        .await
        .expect("auth frame");
    assert_eq!(
        resp.header.status, STATUS_OK,
        "check 8: valid credentials must authenticate (retry on same conn)"
    );

    // ── 6. DirectReadNeedle returns the exact stored bytes ───────────────
    let resp = client
        .send_request(MsgType::DirectReadNeedle, &direct_body(&good_fid), &[])
        .await
        .expect("direct read frame");
    assert_eq!(
        resp.header.status, STATUS_OK,
        "check 9: authenticated DirectReadNeedle must succeed"
    );
    assert_eq!(
        resp.data, expected,
        "check 10: direct-read bytes must match the stored payload"
    );

    // ── 6b. Least privilege (M-2): an authenticated KvClient cannot reach
    // internal business messages (StatFs / WriteNeedle).
    let resp = client
        .send_request(MsgType::StatFs, &[], &[])
        .await
        .expect("statfs frame");
    assert_eq!(
        resp.header.status, STATUS_ERR_PERMISSION_DENIED,
        "check 11: StatFs must be denied even after authentication"
    );

    let resp = client
        .send_request(MsgType::WriteNeedle, &[], &[])
        .await
        .expect("write frame");
    assert_eq!(
        resp.header.status, STATUS_ERR_PERMISSION_DENIED,
        "check 12: WriteNeedle must be denied to an authenticated KV client"
    );

    // ── 7. Malformed fid rejected ────────────────────────────────────────
    let resp = client
        .send_request(
            MsgType::DirectReadNeedle,
            &direct_body("not-a-valid-fid"),
            &[],
        )
        .await
        .expect("direct read frame");
    assert_eq!(
        resp.header.status, STATUS_ERR_PERMISSION_DENIED,
        "check 13: malformed fid must be denied"
    );

    // ── 8. Volume not hosted on this node rejected ───────────────────────
    let resp = client
        .send_request(MsgType::DirectReadNeedle, &direct_body(&nonlocal_fid), &[])
        .await
        .expect("direct read frame");
    assert_eq!(
        resp.header.status, STATUS_ERR_PERMISSION_DENIED,
        "check 14: non-local volume fid must be denied"
    );

    // ── 9. Existing volume, missing needle → NOT_FOUND ───────────────────
    let resp = client
        .send_request(MsgType::DirectReadNeedle, &direct_body(&missing_fid), &[])
        .await
        .expect("direct read frame");
    assert_eq!(
        resp.header.status, STATUS_ERR_NOT_FOUND,
        "check 15: missing needle must return NOT_FOUND"
    );

    // ── 10. A brand-new connection starts unauthenticated ────────────────
    let fresh = connect_kv(900_002, target_addr, target_port).await;
    let resp = fresh
        .send_request(MsgType::DirectReadNeedle, &direct_body(&good_fid), &[])
        .await
        .expect("direct read frame");
    assert_eq!(
        resp.header.status, STATUS_ERR_PERMISSION_DENIED,
        "check 16: a new connection must re-authenticate"
    );
}
