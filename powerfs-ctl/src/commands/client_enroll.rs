//! `powerfs-ctl client enroll` — issue a fuse/kernel client cert, render the
//! client config, and register the client in cluster.toml.
//!
//! Split into three reusable internal fns so `bootstrap` can reuse the first
//! two (cert issue + config render) without appending to cluster.toml (it
//! owns the file).

use crate::cert::CertClient;
use crate::cli::ClientKindArg;
use crate::home::Home;
use crate::render;
use crate::schema::{ClientKind, ResolvedCluster};

const DEFAULT_CLIENT_MOUNT: &str = "/mnt/powerfs";

pub async fn run<C: CertClient>(
    home: &Home,
    client: &C,
    name: &str,
    ip: &str,
    kind: ClientKindArg,
) -> Result<(), String> {
    let cfg = home.load_cluster()?;
    let rc = cfg.validate().map_err(|e| e.to_string())?;
    let master_api = format!("{}:9300", rc.master_ips[0]);
    let admin_token = rc.cfg.cluster.admin_token.clone();
    let k = to_client_kind(kind);
    issue_client_cert(home, client, &master_api, &admin_token, name, ip).await?;
    render_client_config(&rc, home, name, k)?;
    append_cluster_toml_client(home, name, k)?;
    Ok(())
}

fn to_client_kind(k: ClientKindArg) -> ClientKind {
    match k {
        ClientKindArg::Fuse => ClientKind::Fuse,
        ClientKindArg::Kernel => ClientKind::Kernel,
    }
}

/// Issue a client cert with the single default mount dir. Reused by `bootstrap`
/// (which skips the cluster.toml append since it owns the file).
pub async fn issue_client_cert<C: CertClient>(
    home: &Home,
    client: &C,
    master_api: &str,
    admin_token: &str,
    name: &str,
    ip: &str,
) -> Result<(), String> {
    let mount = vec![DEFAULT_CLIENT_MOUNT.to_string()];
    super::cert_issue::run(
        home,
        client,
        master_api,
        admin_token,
        name,
        &[ip.to_string()],
        &mount,
        false, // client mode (not --node)
    )
    .await
}

/// Render the client config. Fuse: write `<certs_dir>/client-<name>.toml`.
/// Kernel: print a `mount -t powerfs` command (no standalone config file).
pub fn render_client_config(
    rc: &ResolvedCluster,
    home: &Home,
    name: &str,
    kind: ClientKind,
) -> Result<(), String> {
    let certs = home.certs_dir();
    match kind {
        ClientKind::Fuse => {
            let toml = render::render_fuse_client(rc)
                .map_err(|e| format!("render fuse client.toml: {e}"))?;
            let p = certs.join(format!("client-{name}.toml"));
            std::fs::write(&p, &toml).map_err(|e| format!("write {}: {}", p.display(), e))?;
            println!("✓ fuse client config saved to {}", p.display());
            println!("  CA: {}/ca.crt", certs.display());
            println!(
                "  mount with: powerfs-fuse --config {} mount {}",
                p.display(),
                DEFAULT_CLIENT_MOUNT
            );
        }
        ClientKind::Kernel => {
            let masters: Vec<String> = rc
                .master_ips
                .iter()
                .map(|ip| format!("{ip}:9334"))
                .collect();
            println!(
                "✓ kernel client cert: {}/{}.crt + {}/{}.key",
                certs.display(),
                name,
                certs.display(),
                name
            );
            println!("  mount with:");
            println!(
                "    mount -t powerfs -o masters={},ca_crt={}/ca.crt,client_crt={}/{}.crt,client_key={}/{}.key {}",
                masters.join(","),
                certs.display(),
                certs.display(),
                name,
                certs.display(),
                name,
                DEFAULT_CLIENT_MOUNT
            );
        }
    }
    Ok(())
}

/// Append `[client.<name>] type = "..."` to cluster.toml if not already
/// present. Parses the existing file to check (avoids duplicate appends); on
/// parse failure warns and skips rather than risk corrupting the user's file.
pub fn append_cluster_toml_client(home: &Home, name: &str, kind: ClientKind) -> Result<(), String> {
    let p = home.cluster_toml();
    let s = std::fs::read_to_string(&p).map_err(|e| format!("read {}: {}", p.display(), e))?;
    match toml::from_str::<crate::schema::ClusterConfig>(&s) {
        Ok(cfg) if cfg.client.contains_key(name) => {
            println!("• [client.{name}] already declared in cluster.toml — skipping append");
            return Ok(());
        }
        Ok(_) => {}
        Err(e) => {
            eprintln!(
                "⚠ cluster.toml parse failed ({e}); skipping [client.{name}] append — fix cluster.toml and re-enroll"
            );
            return Ok(());
        }
    }
    let kind_str = match kind {
        ClientKind::Fuse => "fuse",
        ClientKind::Kernel => "kernel",
    };
    let mut s = s;
    if !s.ends_with('\n') {
        s.push('\n');
    }
    s.push_str(&format!("\n[client.{name}]\ntype = \"{kind_str}\"\n"));
    std::fs::write(&p, s).map_err(|e| format!("write {}: {}", p.display(), e))?;
    println!("✓ Appended [client.{name}] to {}", p.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cert::{IssuedCert, MockCertClient};
    use crate::cli::ClientKindArg;
    use crate::commands::test_support;

    fn issued() -> IssuedCert {
        IssuedCert {
            cert: "C".into(),
            key: "K".into(),
        }
    }

    #[tokio::test]
    async fn client_enroll_renders_fuse_toml() {
        let (home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_sign(Ok(issued()));
        run(&home, &client, "fuse-1", "172.30.0.99", ClientKindArg::Fuse)
            .await
            .unwrap();
        let p = home.certs_dir().join("client-fuse-1.toml");
        assert!(p.exists(), "client-fuse-1.toml should exist");
        let body = std::fs::read_to_string(&p).unwrap();
        assert!(body.contains("master_addresses"));
        // cluster.toml got [client.fuse-1] type = "fuse"
        let s = std::fs::read_to_string(home.cluster_toml()).unwrap();
        assert!(s.contains("[client.fuse-1]"));
        assert!(s.contains("type = \"fuse\""));
        // sign request used client mode (non-empty mount_dirs)
        let calls = client.sign_calls();
        assert_eq!(calls.len(), 1);
        assert!(!calls[0].mount_dirs.is_empty());
    }

    #[tokio::test]
    async fn client_enroll_kernel_prints_mount_cmd() {
        let (home, _dir) = test_support::home_with_cluster();
        let client = MockCertClient::new().with_sign(Ok(issued()));
        run(&home, &client, "k-1", "172.30.0.55", ClientKindArg::Kernel)
            .await
            .unwrap();
        // cert written; no client-<name>.toml for kernel
        assert!(home.certs_dir().join("k-1.crt").exists());
        assert!(!home.certs_dir().join("client-k-1.toml").exists());
        // cluster.toml got [client.k-1] type = "kernel"
        let s = std::fs::read_to_string(home.cluster_toml()).unwrap();
        assert!(s.contains("[client.k-1]"));
        assert!(s.contains("type = \"kernel\""));
    }
}
