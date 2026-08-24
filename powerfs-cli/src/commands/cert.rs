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
    /// Request the master to sign a client certificate.
    SignClient(SignClientArgs),
    /// Request the master to sign a server certificate.
    SignServer(SignServerArgs),
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
}

#[derive(Args, Debug)]
pub struct SignClientArgs {
    /// Common Name for the client (e.g., "fuse-client-1")
    #[arg(long, required = true)]
    common_name: String,
    /// Client ID to embed as a SAN URI (e.g., "client-001")
    #[arg(long)]
    client_id: Option<String>,
    /// Master admin API address (host:metrics_port)
    #[arg(long, required = true)]
    master_api: String,
    /// Admin token for authentication
    #[arg(long, required = true)]
    admin_token: String,
    /// Output directory for client.crt and client.key
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
struct SignClientRequest {
    common_name: String,
    client_id: Option<String>,
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
    let path = args.output_dir.join("ca.crt");
    std::fs::write(&path, &ca_pem)?;

    println!("CA certificate saved to {}", path.display());
    println!();
    println!("Distribute ca.crt to all nodes that need to verify");
    println!("certificates (FUSE clients, filers, volume servers).");

    Ok(())
}

fn sign_client(args: &SignClientArgs) -> CommandResult {
    println!(
        "Requesting master to sign client certificate: CN={}",
        args.common_name
    );

    let req = SignClientRequest {
        common_name: args.common_name.clone(),
        client_id: args.client_id.clone(),
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
    let cert_path = args.output_dir.join("client.crt");
    let key_path = args.output_dir.join("client.key");

    std::fs::write(&cert_path, &sign_resp.cert)?;
    std::fs::write(&key_path, &sign_resp.key)?;

    println!("Client certificate saved to {}", cert_path.display());
    println!("Client private key saved to {}", key_path.display());
    if args.client_id.is_some() {
        println!("Client ID embedded as SAN URI");
    }
    println!();
    println!("Deploy client.crt + client.key + ca.crt to the client node.");

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

    println!("Server certificate saved to {}", cert_path.display());
    println!("Server private key saved to {}", key_path.display());
    if !args.sans.is_empty() {
        println!("SANs: {}", args.sans.join(", "));
    }
    println!();
    println!("Deploy server.crt + server.key + ca.crt to the server node.");

    Ok(())
}
