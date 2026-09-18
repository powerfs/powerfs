//! `powerfs-ctl init` — generate cluster.toml + .powerfs/ skeleton.

use crate::home::Home;

const DEFAULT_CLUSTER_TOML: &str = r#"# PowerFS cluster declaration — the single hand-written source of truth.
# Edit this file, then run `powerfs-ctl config render` to regenerate all
# docker-compose.yml + per-role TOML configs under .powerfs/rendered/.

[cluster]
name = "powerfs"
# Deployment profile: simple | ha | rdma
profile = "ha"
# Cluster-level constant. All filers must match; master enforces it.
shard_count = 3
data_root = "/var/lib/powerfs"
image_tag = "ghcr.io/powerfs/powerfs:latest"
registration_token = "powerfs-cluster"
admin_token = "powerfs-admin"

[network]
subnet = "172.30.0.0/16"
# gateway = "172.30.0.1"   # auto-derived from subnet if omitted
bridge = "powerfs-br0"

# Node counts. Omit to use profile defaults (ha: master=3, volume=6, filer=3).
# To pin specific IPs instead of auto-allocating, set `ips = [...]`.
[nodes.master]
# count = 3
# ips = ["172.30.0.11", "172.30.0.12", "172.30.0.13"]

[nodes.volume]
# count = 6

[nodes.filer]
# count = 3

[ca]
organization = "PowerFS"
validity_days = 3650

# Pre-declared clients (optional). `powerfs-ctl client enroll NAME` also works.
# [client.kernel-default]
# type = "kernel"
"#;

pub async fn run(home: &Home, force: bool) -> Result<(), String> {
    home.ensure_skeleton()
        .map_err(|e| format!("create state dirs: {}", e))?;
    let p = home.cluster_toml();
    if p.exists() && !force {
        return Err(format!(
            "{} already exists. Re-run with --force to overwrite (your edits will be lost).",
            p.display()
        ));
    }
    std::fs::write(&p, DEFAULT_CLUSTER_TOML)
        .map_err(|e| format!("write {}: {}", p.display(), e))?;
    println!("✓ Wrote {}", p.display());
    println!("  Next: edit cluster.toml, then `powerfs-ctl config render`");
    Ok(())
}
