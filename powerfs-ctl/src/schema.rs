//! cluster.toml schema — the single hand-written source of truth for a
//! PowerFS deployment. Rendered into docker-compose.yml + per-role TOML.

use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr};

/// Top-level cluster declaration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub cluster: Cluster,
    pub network: Network,
    #[serde(default)]
    pub nodes: Nodes,
    #[serde(default)]
    pub ca: Ca,
    #[serde(default)]
    pub client: std::collections::BTreeMap<String, ClientSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
    pub name: String,
    /// Deployment profile — maps to a docker compose profile set.
    #[serde(default = "default_profile")]
    pub profile: Profile,
    /// Cluster-level constant. Every filer must match this value
    /// (master hard constraint); rendering writes it into every filer TOML.
    pub shard_count: u32,
    /// Host path prefix for all service data volumes.
    #[serde(default = "default_data_root")]
    pub data_root: String,
    /// Container image tag for all services.
    #[serde(default = "default_image_tag")]
    pub image_tag: String,
    /// Shared registration token for filer/volume → master join.
    #[serde(default = "default_registration_token")]
    pub registration_token: String,
    /// Admin API token (used by powerfs-cli cert subcommands).
    #[serde(default = "default_admin_token")]
    pub admin_token: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Profile {
    Simple,
    Ha,
    Rdma,
}

impl Profile {
    /// Required node counts for the profile.
    pub fn required_counts(&self) -> NodeCounts {
        match self {
            Profile::Simple => NodeCounts {
                master: 1,
                volume: 1,
                filer: 1,
            },
            Profile::Ha | Profile::Rdma => NodeCounts {
                master: 3,
                volume: 6,
                filer: 3,
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NodeCounts {
    pub master: u8,
    pub volume: u8,
    pub filer: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Network {
    /// CIDR subnet, e.g. "172.30.0.0/16".
    pub subnet: String,
    /// Bridge gateway, e.g. "172.30.0.1".
    #[serde(default)]
    pub gateway: Option<String>,
    /// Linux bridge name.
    #[serde(default = "default_bridge")]
    pub bridge: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Nodes {
    #[serde(default)]
    pub master: NodeGroup,
    #[serde(default)]
    pub volume: NodeGroup,
    #[serde(default)]
    pub filer: NodeGroup,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeGroup {
    /// Number of nodes. If omitted, derived from profile.
    #[serde(default)]
    pub count: Option<u8>,
    /// Optional explicit IP override (length must equal count).
    #[serde(default)]
    pub ips: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ca {
    #[serde(default = "default_org")]
    pub organization: String,
    #[serde(default = "default_validity_days")]
    pub validity_days: u32,
}

impl Default for Ca {
    fn default() -> Self {
        Self {
            organization: default_org(),
            validity_days: default_validity_days(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientSpec {
    #[serde(rename = "type")]
    pub kind: ClientKind,
    /// Optional client IP. When set in cluster.toml, `bootstrap` issues a
    /// client cert for this client; otherwise bootstrap skips it and the
    /// user runs `client enroll <name> --ip <ip>` on the fly.
    #[serde(default)]
    pub ip: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ClientKind {
    Kernel,
    Fuse,
}

// ---- defaults ----

fn default_profile() -> Profile {
    Profile::Ha
}
fn default_data_root() -> String {
    "/var/lib/powerfs".into()
}
fn default_image_tag() -> String {
    "ghcr.io/powerfs/powerfs:latest".into()
}
fn default_registration_token() -> String {
    "powerfs-cluster".into()
}
fn default_admin_token() -> String {
    "powerfs-admin".into()
}
fn default_bridge() -> String {
    "powerfs-br0".into()
}
fn default_org() -> String {
    "PowerFS".into()
}
fn default_validity_days() -> u32 {
    3650
}

// ---- validation + allocation ----

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("shard_count must be > 0")]
    ZeroShardCount,
    #[error("invalid subnet CIDR: {0}")]
    BadSubnet(String),
    #[error("ips length ({got}) must equal count ({count})")]
    IpsCountMismatch { count: u8, got: usize },
    #[error("invalid IP in {role}: {ip}")]
    BadIp { role: &'static str, ip: String },
    #[error("role {role} count 0 not allowed for profile {profile:?}")]
    ZeroCount {
        role: &'static str,
        profile: Profile,
    },
}

impl ClusterConfig {
    /// Validate and resolve the config (fill in derived counts / allocate IPs).
    pub fn validate(&self) -> Result<ResolvedCluster, SchemaError> {
        if self.cluster.shard_count == 0 {
            return Err(SchemaError::ZeroShardCount);
        }
        let (subnet_net, prefix) = parse_subnet(&self.network.subnet)?;

        let required = self.cluster.profile.required_counts();
        let master_count = self.nodes.master.count.unwrap_or(required.master);
        let volume_count = self.nodes.volume.count.unwrap_or(required.volume);
        let filer_count = self.nodes.filer.count.unwrap_or(required.filer);

        for (role, count) in [
            ("master", master_count),
            ("volume", volume_count),
            ("filer", filer_count),
        ] {
            if count == 0 {
                return Err(SchemaError::ZeroCount {
                    role,
                    profile: self.cluster.profile,
                });
            }
        }

        // Reserve IPs in the subnet following the existing HA layout:
        //   master  .11..  volume .21..  filer .31..  monitor .30  redis .50  s3 .40
        let mut alloc = IpAllocator::new(subnet_net, prefix);
        let master_ips = resolve_ips(&self.nodes.master, master_count, 11, "master", &mut alloc)?;
        let volume_ips = resolve_ips(&self.nodes.volume, volume_count, 21, "volume", &mut alloc)?;
        let filer_ips = resolve_ips(&self.nodes.filer, filer_count, 31, "filer", &mut alloc)?;
        let monitor_ip = alloc.reserve(30)?;
        let redis_ip = alloc.reserve(50)?;
        let s3_ip = alloc.reserve(40)?;

        Ok(ResolvedCluster {
            cfg: self.clone(),
            master_ips,
            volume_ips,
            filer_ips,
            monitor_ip,
            redis_ip,
            s3_ip,
            gateway: self
                .network
                .gateway
                .clone()
                .unwrap_or_else(|| alloc.gateway()),
        })
    }
}

/// Fully-resolved cluster with concrete IPs for every node.
#[derive(Debug, Clone)]
pub struct ResolvedCluster {
    pub cfg: ClusterConfig,
    pub master_ips: Vec<String>,
    pub volume_ips: Vec<String>,
    pub filer_ips: Vec<String>,
    pub monitor_ip: String,
    pub redis_ip: String,
    pub s3_ip: String,
    pub gateway: String,
}

fn resolve_ips(
    group: &NodeGroup,
    count: u8,
    start_octet: u8,
    role: &'static str,
    alloc: &mut IpAllocator,
) -> Result<Vec<String>, SchemaError> {
    if let Some(ips) = &group.ips {
        if ips.len() != count as usize {
            return Err(SchemaError::IpsCountMismatch {
                count,
                got: ips.len(),
            });
        }
        for ip in ips {
            ip.parse::<IpAddr>().map_err(|_| SchemaError::BadIp {
                role,
                ip: ip.clone(),
            })?;
        }
        return Ok(ips.clone());
    }
    (0..count).map(|i| alloc.reserve(start_octet + i)).collect()
}

fn parse_subnet(s: &str) -> Result<(Ipv4Addr, u8), SchemaError> {
    let (addr, prefix) = s
        .split_once('/')
        .ok_or_else(|| SchemaError::BadSubnet(s.into()))?;
    let net: Ipv4Addr = addr.parse().map_err(|_| SchemaError::BadSubnet(s.into()))?;
    let prefix: u8 = prefix
        .parse()
        .map_err(|_| SchemaError::BadSubnet(s.into()))?;
    if prefix > 32 {
        return Err(SchemaError::BadSubnet(s.into()));
    }
    Ok((net, prefix))
}

/// Simple IP allocator over a /16 subnet. Reserves by last octet.
pub struct IpAllocator {
    net: Ipv4Addr,
}

impl IpAllocator {
    fn new(net: Ipv4Addr, _prefix: u8) -> Self {
        Self { net }
    }

    fn reserve(&self, last_octet: u8) -> Result<String, SchemaError> {
        // Only /16 layout is assumed (existing HA uses 172.30.0.0/16).
        let [a, b, _, _] = self.net.octets();
        Ok(format!("{}.{}.0.{}", a, b, last_octet))
    }

    fn gateway(&self) -> String {
        let [a, b, _, _] = self.net.octets();
        format!("{}.{}.0.1", a, b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ha_default_ip_layout_matches_existing() {
        let cfg: ClusterConfig = toml::from_str(
            r#"
            [cluster]
            name = "t"
            profile = "ha"
            shard_count = 3

            [network]
            subnet = "172.30.0.0/16"
        "#,
        )
        .unwrap();
        let r = cfg.validate().unwrap();
        assert_eq!(
            r.master_ips,
            vec!["172.30.0.11", "172.30.0.12", "172.30.0.13"]
        );
        assert_eq!(r.volume_ips[0], "172.30.0.21");
        assert_eq!(
            r.filer_ips,
            vec!["172.30.0.31", "172.30.0.32", "172.30.0.33"]
        );
        assert_eq!(r.monitor_ip, "172.30.0.30");
        assert_eq!(r.redis_ip, "172.30.0.50");
        assert_eq!(r.s3_ip, "172.30.0.40");
    }

    #[test]
    fn rejects_zero_shard_count() {
        let cfg: ClusterConfig = toml::from_str(
            r#"
            [cluster]
            name = "t"
            shard_count = 0
            [network]
            subnet = "172.30.0.0/16"
        "#,
        )
        .unwrap();
        assert!(matches!(cfg.validate(), Err(SchemaError::ZeroShardCount)));
    }
}
