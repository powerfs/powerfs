//! `powerfs-ctl cert list` — read the master-side `client_registry.json` and
//! print a sorted table. Master writes this file into `<ca_dir>` (which is
//! mounted from the host's certs dir), so `cert list` only works when
//! `home.certs_dir()` is that mount point — i.e. run on the master host, or
//! copy the file down. There is no HTTP `list` endpoint on master (only
//! `ca` / `sign-client` / `sign-server`), so we can't query it remotely.

use crate::cert::{unix_to_ymd, ClientRegistry};
use crate::home::Home;

pub async fn run(home: &Home) -> Result<(), String> {
    let p = home.certs_dir().join("client_registry.json");
    if !p.exists() {
        return Err(format!(
            "{} not found — `cert list` reads the master-side registry, which \
             only exists when home.certs_dir() is mounted from master's ca_dir. \
             Either run on the master host, or copy client_registry.json down.",
            p.display()
        ));
    }
    let s = std::fs::read_to_string(&p).map_err(|e| format!("read {}: {}", p.display(), e))?;
    let reg: ClientRegistry =
        serde_json::from_str(&s).map_err(|e| format!("parse {}: {}", p.display(), e))?;
    let entries = reg.sorted_entries();
    if entries.is_empty() {
        println!("(registry is empty — no certs issued yet)");
        return Ok(());
    }
    println!(
        "{:<20} {:<28} {:<22} {:<14} EXPIRES",
        "NAME", "SAN_IPS", "MOUNT_DIRS", "FINGERPRINT"
    );
    for e in entries {
        let san = e.san_ips.join(",");
        let mount = if e.mount_dirs.is_empty() {
            "(node)".to_string()
        } else {
            e.mount_dirs.join(",")
        };
        let fp: String = e.cert_fingerprint_sha256.chars().take(12).collect();
        let (y, m, d) = unix_to_ymd(e.expires_at);
        let flag = if e.revoked { " [REVOKED]" } else { "" };
        println!(
            "{:<20} {:<28} {:<22} {:<14} {}-{:02}-{:02}{}",
            e.client_name, san, mount, fp, y, m, d, flag
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::test_support;

    #[tokio::test]
    async fn cert_list_parses_registry() {
        let (home, _dir) = test_support::home_with_cluster();
        let json = r#"{
            "by_fingerprint": {
                "fp_a": {"client_name":"filer-1","san_ips":["172.30.0.31"],"mount_dirs":[],"issued_at":0,"expires_at":1735689600,"cert_fingerprint_sha256":"fp_a","revoked":false},
                "fp_b": {"client_name":"fuse-c1","san_ips":["172.30.0.41"],"mount_dirs":["/mnt/powerfs"],"issued_at":0,"expires_at":1735689600,"cert_fingerprint_sha256":"fp_b","revoked":false}
            },
            "by_client_name": {}
        }"#;
        std::fs::write(home.certs_dir().join("client_registry.json"), json).unwrap();
        run(&home).await.unwrap();
    }

    #[tokio::test]
    async fn cert_list_missing_registry() {
        let (home, _dir) = test_support::home_with_cluster();
        let err = run(&home).await.unwrap_err();
        assert!(
            err.contains("master-side registry") || err.contains("not found"),
            "got: {err}"
        );
    }
}
