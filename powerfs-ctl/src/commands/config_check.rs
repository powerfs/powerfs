//! `powerfs-ctl config check` — validate cluster.toml (static checks only in M1;
//! running-state diff is M2).

use crate::home::Home;
use crate::schema::ClusterConfig;

pub async fn run(home: &Home) -> Result<(), String> {
    let cfg: ClusterConfig = home.load_cluster()?;
    let resolved = cfg.validate().map_err(|e| format!("FAIL schema: {}", e))?;

    println!("✓ cluster.toml is valid");
    println!(
        "  cluster:      {} ({:?})",
        resolved.cfg.cluster.name, resolved.cfg.cluster.profile
    );
    println!("  shard_count:  {}", resolved.cfg.cluster.shard_count);
    println!("  subnet:       {}", resolved.cfg.network.subnet);
    println!(
        "  master:       {} ({})",
        resolved.master_ips.len(),
        resolved.master_ips.join(", ")
    );
    println!("  volume:       {} nodes", resolved.volume_ips.len());
    println!("  filer:        {} nodes", resolved.filer_ips.len());
    println!("  monitor:      {}", resolved.monitor_ip);
    println!("  redis:        {}", resolved.redis_ip);
    println!("  s3:           {}", resolved.s3_ip);

    // Verify every allocated IP is unique (no overlap in the subnet).
    let mut all = vec![resolved.monitor_ip, resolved.redis_ip, resolved.s3_ip];
    all.extend(resolved.master_ips.clone());
    all.extend(resolved.volume_ips.clone());
    all.extend(resolved.filer_ips.clone());
    let before = all.len();
    all.sort();
    all.dedup();
    if all.len() != before {
        return Err("FAIL: IP allocation produced duplicate addresses".into());
    }
    println!("✓ No IP overlap ({} unique addresses)", all.len());

    println!("\n  (running-state drift detection comes in M2)");
    Ok(())
}
