use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::CommandResult;
use crate::http::http_request_with_headers;

/// Certificate management subcommands (call master CA API).
#[derive(Subcommand, Debug)]
pub enum CertSubcommand {
    /// Fetch the master CA certificate and save it locally.
    InitCa(InitCaArgs),
    /// Request the master to sign a client certificate bound to specific
    /// IP addresses + mount directories. The issued certificate is INVALID
    /// if used from a different host (source IP not in --san-ip) or
    /// mounted on a directory not in --mount-dir.
    SignClient(SignClientArgs),
    /// Request the master to sign a server certificate.
    SignServer(SignServerArgs),
    /// Request the master to sign a **storage node** certificate (filer or
    /// volume server) bound to specific IP addresses + node_id. Unlike
    /// sign-client, no mount directory is required — the cert is validated
    /// by the master against the node_id reported in RegisterFiler /
    /// Heartbeat.
    SignNode(SignNodeArgs),
}

#[derive(Args, Debug)]
pub struct InitCaArgs {
    /// Master admin API address (host:metrics_port)
    #[arg(long, required = true)]
    master_api: String,
    /// Admin token for authentication
    #[arg(long, required = true)]
    admin_token: String,
    /// Output directory for ca.crt
    #[arg(short, long, default_value = ".")]
    output_dir: PathBuf,
    /// Output filename (default: ca.crt)
    #[arg(long, default_value = "ca.crt")]
    ca_name: String,
}

#[derive(Args, Debug)]
pub struct SignClientArgs {
    /// Logical client name (CN, output filename prefix, e.g. "client-001").
    /// Required.
    #[arg(long = "client-name", required = true)]
    client_name: String,
    /// Optional client-id to embed as a SAN URI.
    #[arg(long = "client-id")]
    client_id: Option<String>,
    /// Master admin API address (host:metrics_port).
    #[arg(long, required = true)]
    master_api: String,
    /// Admin token for authentication.
    #[arg(long, required = true)]
    admin_token: String,
    /// IPv4/IPv6 address(es) the certificate is bound to. Repeat this
    /// option for multiple IPs. AT LEAST ONE IS REQUIRED. Using the
    /// certificate from any other IP will be rejected by the master.
    #[arg(long = "san-ip", required = true)]
    san_ips: Vec<String>,
    /// Mount directory/directories the certificate is bound to. Repeat
    /// this option for multiple mount directories. AT LEAST ONE IS
    /// REQUIRED. Mounting on any other path will be rejected.
    #[arg(long = "mount-dir", required = true)]
    mount_dirs: Vec<String>,
    /// Output directory for `<client-name>.crt` + `<client-name>.key`.
    #[arg(short, long, default_value = ".")]
    output_dir: PathBuf,
}

#[derive(Args, Debug)]
pub struct SignNodeArgs {
    /// Storage node ID (e.g. "filer-1", "volume-server-1"). This MUST
    /// match the node_id reported by the filer/volume server in its
    /// RegisterFiler/Heartbeat TLV. The master rejects a cert whose
    /// client_name does not match the reported node_id.
    #[arg(long = "node-id", required = true)]
    node_id: String,
    /// Master admin API address (host:metrics_port).
    #[arg(long, required = true)]
    master_api: String,
    /// Admin token for authentication.
    #[arg(long, required = true)]
    admin_token: String,
    /// IPv4/IPv6 address(es) the certificate is bound to. Repeat this
    /// option for multiple IPs. AT LEAST ONE IS REQUIRED. Using the
    /// certificate from any other IP will be rejected by the master.
    #[arg(long = "san-ip", required = true)]
    san_ips: Vec<String>,
    /// Output directory for `<node-id>.crt` + `<node-id>.key`.
    #[arg(short, long, default_value = ".")]
    output_dir: PathBuf,
}

#[derive(Args, Debug)]
pub struct SignServerArgs {
    /// Common Name for the server (e.g., "filer-1")
    #[arg(long, required = true)]
    common_name: String,
    /// Subject Alternative Names (DNS names or IPs). Repeatable.
    #[arg(long = "san")]
    sans: Vec<String>,
    /// Master admin API address (host:metrics_port)
    #[arg(long, required = true)]
    master_api: String,
    /// Admin token for authentication
    #[arg(long, required = true)]
    admin_token: String,
    /// Output directory for server.crt and server.key
    #[arg(short, long, default_value = ".")]
    output_dir: PathBuf,
}

#[derive(Serialize)]
struct SignClientRequestV2 {
    common_name: String,
    client_name: String,
    client_id: Option<String>,
    san_ips: Vec<String>,
    mount_dirs: Vec<String>,
}

#[derive(Serialize)]
struct SignServerRequest {
    common_name: String,
    sans: Vec<String>,
}

#[derive(Deserialize)]
struct SignResponse {
    cert: String,
    key: String,
}

pub fn cert(command: CertSubcommand) -> CommandResult {
    match command {
        CertSubcommand::InitCa(args) => init_ca(&args),
        CertSubcommand::SignClient(args) => sign_client(&args),
        CertSubcommand::SignServer(args) => sign_server(&args),
        CertSubcommand::SignNode(args) => sign_node(&args),
    }
}

fn init_ca(args: &InitCaArgs) -> CommandResult {
    println!("Fetching CA certificate from master at {}", args.master_api);

    let auth = format!("Bearer {}", args.admin_token);
    let ca_pem = http_request_with_headers(
        &args.master_api,
        "GET",
        "/api/cert/ca",
        None,
        &[("Authorization", auth.as_str())],
    )?;

    std::fs::create_dir_all(&args.output_dir)?;
    let path = args.output_dir.join(&args.ca_name);
    std::fs::write(&path, &ca_pem)?;

    println!("CA certificate saved to {}", path.display());
    println!();
    println!(
        "Distribute {} to all nodes that need to verify",
        args.ca_name
    );
    println!("certificates (FUSE clients, kernel clients, filers, volume servers).");
    println!();
    println!("Next step: issue a client certificate, e.g.");
    println!("  powerfs-cli cert sign-client \\");
    println!("    --client-name client-001 --san-ip 172.20.0.41 --mount-dir /mnt/powerfs \\");
    println!(
        "    --master-api {} --admin-token <token> -o /etc/powerfs/certs/",
        args.master_api
    );

    Ok(())
}

fn sign_client(args: &SignClientArgs) -> CommandResult {
    if args.san_ips.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--san-ip is required (repeat for multiple IPs)",
        )
        .into());
    }
    if args.mount_dirs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--mount-dir is required (repeat for multiple dirs)",
        )
        .into());
    }

    println!(
        "Requesting master to sign client certificate: name={} san_ips={:?} mount_dirs={:?}",
        args.client_name, args.san_ips, args.mount_dirs
    );

    let req = SignClientRequestV2 {
        common_name: args.client_name.clone(),
        client_name: args.client_name.clone(),
        client_id: args.client_id.clone(),
        san_ips: args.san_ips.clone(),
        mount_dirs: args.mount_dirs.clone(),
    };
    let body = serde_json::to_string(&req)?;

    let auth = format!("Bearer {}", args.admin_token);
    let resp = http_request_with_headers(
        &args.master_api,
        "POST",
        "/api/cert/sign-client",
        Some(&body),
        &[("Authorization", auth.as_str())],
    )?;

    let sign_resp: SignResponse = serde_json::from_str(&resp)?;

    std::fs::create_dir_all(&args.output_dir)?;
    let cert_filename = format!("{}.crt", args.client_name);
    let key_filename = format!("{}.key", args.client_name);
    let cert_path = args.output_dir.join(&cert_filename);
    let key_path = args.output_dir.join(&key_filename);

    std::fs::write(&cert_path, &sign_resp.cert)?;
    std::fs::write(&key_path, &sign_resp.key)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600));
    }

    println!("Client certificate saved to {}", cert_path.display());
    println!(
        "Client private key saved to  {} (mode 0600)",
        key_path.display()
    );
    println!();
    println!("Issuance metadata (san_ips / mount_dirs / client_name / fingerprint)");
    println!("has been persisted on the master as a binding record. The master");
    println!("will REJECT the mount if the caller's source IP is not in san_ips");
    println!("or if the mount-point Name differs from one of --mount-dir.");
    println!();
    println!("Deploy these files to the client node and mount:");
    println!("  (FUSE)   powerfs-fuse --master <..> --mount-point <dir> \\");
    println!("             --ca-crt   /etc/powerfs/certs/ca.crt \\");
    println!("             --client-crt {} \\", cert_path.display());
    println!("             --client-key {}", key_path.display());
    println!("  (Kernel) mount -t powerfs -o 'master_addr=...,ca_crt=/etc/powerfs/certs/ca.crt,\\");
    println!(
        "             client_crt={},client_key={}' none <mount-point>",
        cert_path.display(),
        key_path.display()
    );

    Ok(())
}

fn sign_node(args: &SignNodeArgs) -> CommandResult {
    if args.san_ips.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--san-ip is required (repeat for multiple IPs)",
        )
        .into());
    }

    println!(
        "Requesting master to sign storage node certificate: node_id={} san_ips={:?}",
        args.node_id, args.san_ips
    );

    // Reuse the sign-client API with empty mount_dirs — the master stores
    // client_name=node_id, san_ips=node IPs, mount_dirs=[].  Validation in
    // validate_server_node_pem checks client_name==node_id (not mount_dirs).
    let req = SignClientRequestV2 {
        common_name: args.node_id.clone(),
        client_name: args.node_id.clone(),
        client_id: None,
        san_ips: args.san_ips.clone(),
        mount_dirs: Vec::new(),
    };
    let body = serde_json::to_string(&req)?;

    let auth = format!("Bearer {}", args.admin_token);
    let resp = http_request_with_headers(
        &args.master_api,
        "POST",
        "/api/cert/sign-client",
        Some(&body),
        &[("Authorization", auth.as_str())],
    )?;

    let sign_resp: SignResponse = serde_json::from_str(&resp)?;

    std::fs::create_dir_all(&args.output_dir)?;
    let cert_filename = format!("{}.crt", args.node_id);
    let key_filename = format!("{}.key", args.node_id);
    let cert_path = args.output_dir.join(&cert_filename);
    let key_path = args.output_dir.join(&key_filename);

    std::fs::write(&cert_path, &sign_resp.cert)?;
    std::fs::write(&key_path, &sign_resp.key)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600));
    }

    println!("Storage node certificate saved to {}", cert_path.display());
    println!(
        "Storage node private key saved to  {} (mode 0600)",
        key_path.display()
    );
    println!();
    println!(
        "Deploy these files to the {} node.  Add to its config:",
        args.node_id
    );
    println!(
        "  [filer]   client_crt = {}  (path inside the node)",
        cert_path.display()
    );
    println!(
        "  [volume]   client_crt = {}  (path inside the node)",
        cert_path.display()
    );
    println!();
    println!(
        "The master will validate this cert against node_id='{}' and",
        args.node_id
    );
    println!(
        "the caller's source IP ({:?}).  A cert stolen to a different",
        args.san_ips
    );
    println!("IP or used with a different node_id will be rejected.");

    Ok(())
}

fn sign_server(args: &SignServerArgs) -> CommandResult {
    println!(
        "Requesting master to sign server certificate: CN={}",
        args.common_name
    );

    let req = SignServerRequest {
        common_name: args.common_name.clone(),
        sans: args.sans.clone(),
    };
    let body = serde_json::to_string(&req)?;

    let auth = format!("Bearer {}", args.admin_token);
    let resp = http_request_with_headers(
        &args.master_api,
        "POST",
        "/api/cert/sign-server",
        Some(&body),
        &[("Authorization", auth.as_str())],
    )?;

    let sign_resp: SignResponse = serde_json::from_str(&resp)?;

    std::fs::create_dir_all(&args.output_dir)?;
    let cert_path = args.output_dir.join("server.crt");
    let key_path = args.output_dir.join("server.key");

    std::fs::write(&cert_path, &sign_resp.cert)?;
    std::fs::write(&key_path, &sign_resp.key)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600));
    }

    println!("Server certificate saved to {}", cert_path.display());
    println!("Server private key saved to {}", key_path.display());
    if !args.sans.is_empty() {
        println!("SANs: {}", args.sans.join(", "));
    }
    println!();
    println!("Deploy server.crt + server.key + ca.crt to the server node.");

    Ok(())
}
