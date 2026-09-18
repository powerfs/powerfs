//! Render `ResolvedCluster` into docker-compose.yml + per-role TOML configs.

use crate::schema::{ClientKind, ResolvedCluster};
use minijinja::Environment;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

/// Rendered deployment artifacts.
#[derive(Debug)]
pub struct Rendered {
    pub compose_yaml: String,
    /// role-name -> TOML content (e.g. "master-1", "volume-3", "filer-2", "monitor")
    pub configs: BTreeMap<String, String>,
}

pub fn render(rc: &ResolvedCluster) -> Result<Rendered, RenderError> {
    let mut env = Environment::new();
    env.add_template("compose", COMPOSE_TEMPLATE)?;
    env.add_template("master", MASTER_TEMPLATE)?;
    env.add_template("volume", VOLUME_TEMPLATE)?;
    env.add_template("filer", FILER_TEMPLATE)?;
    env.add_template("monitor", MONITOR_TEMPLATE)?;
    env.add_template("fuse", FUSE_TEMPLATE)?;

    let ctx = build_context(rc)?;

    let compose_yaml = env.get_template("compose")?.render(&ctx)?;
    let mut configs = BTreeMap::new();

    for (i, _ip) in rc.master_ips.iter().enumerate() {
        let name = format!("master-{}", i + 1);
        let c = ctx_for_node(&ctx, "master", i + 1, rc)?;
        configs.insert(name, env.get_template("master")?.render(&c)?);
    }
    for (i, _ip) in rc.volume_ips.iter().enumerate() {
        let name = format!("volume-{}", i + 1);
        let c = ctx_for_node(&ctx, "volume", i + 1, rc)?;
        configs.insert(name, env.get_template("volume")?.render(&c)?);
    }
    for (i, _ip) in rc.filer_ips.iter().enumerate() {
        let name = format!("filer-{}", i + 1);
        let c = ctx_for_node(&ctx, "filer", i + 1, rc)?;
        configs.insert(name, env.get_template("filer")?.render(&c)?);
    }
    configs.insert("monitor".into(), env.get_template("monitor")?.render(&ctx)?);
    configs.insert("fuse".into(), env.get_template("fuse")?.render(&ctx)?);

    Ok(Rendered {
        compose_yaml,
        configs,
    })
}

/// Render only the fuse client TOML — used by `client enroll` to emit
/// `<certs_dir>/client-<name>.toml` without re-rendering the whole cluster.
/// The fuse template carries the cluster's master_net_addrs + redis_url and
/// has no per-client variation, so every fuse client in the same cluster
/// gets identical content.
pub fn render_fuse_client(rc: &ResolvedCluster) -> Result<String, RenderError> {
    let mut env = Environment::new();
    env.add_template("fuse", FUSE_TEMPLATE)?;
    let ctx = build_context(rc)?;
    Ok(env.get_template("fuse")?.render(&ctx)?)
}

fn build_context(rc: &ResolvedCluster) -> Result<Map<String, Value>, RenderError> {
    let master_addrs: Vec<String> = rc
        .master_ips
        .iter()
        .map(|ip| format!("{}:9333", ip))
        .collect();
    let master_net_addrs: Vec<String> = rc.master_ips.clone();
    let master_raft_addrs: Vec<String> = rc
        .master_ips
        .iter()
        .map(|ip| format!("{}:9335", ip))
        .collect();
    let filer_raft_addrs: Vec<String> = rc
        .filer_ips
        .iter()
        .map(|ip| format!("{}:8889", ip))
        .collect();
    let clients: Vec<Value> = rc
        .cfg
        .client
        .iter()
        .map(|(k, v)| {
            json!({
                "name": k,
                "kind": match v.kind { ClientKind::Kernel => "kernel", ClientKind::Fuse => "fuse" },
            })
        })
        .collect();

    let mut m = Map::new();
    m.insert("cluster_name".into(), json!(rc.cfg.cluster.name));
    m.insert(
        "profile".into(),
        json!(format!("{:?}", rc.cfg.cluster.profile).to_lowercase()),
    );
    m.insert("image_tag".into(), json!(rc.cfg.cluster.image_tag));
    m.insert("subnet".into(), json!(rc.cfg.network.subnet));
    m.insert("gateway".into(), json!(rc.gateway));
    m.insert("bridge".into(), json!(rc.cfg.network.bridge));
    m.insert("shard_count".into(), json!(rc.cfg.cluster.shard_count));
    m.insert("data_root".into(), json!(rc.cfg.cluster.data_root));
    m.insert(
        "registration_token".into(),
        json!(rc.cfg.cluster.registration_token),
    );
    m.insert("admin_token".into(), json!(rc.cfg.cluster.admin_token));
    m.insert("master_ips".into(), json!(rc.master_ips));
    m.insert("volume_ips".into(), json!(rc.volume_ips));
    m.insert("filer_ips".into(), json!(rc.filer_ips));
    m.insert("master_addrs".into(), json!(master_addrs));
    m.insert("master_net_addrs".into(), json!(master_net_addrs));
    m.insert("master_raft_addrs".into(), json!(master_raft_addrs));
    m.insert("filer_raft_addrs".into(), json!(filer_raft_addrs));
    m.insert("monitor_ip".into(), json!(rc.monitor_ip));
    m.insert("redis_ip".into(), json!(rc.redis_ip));
    m.insert("s3_ip".into(), json!(rc.s3_ip));
    m.insert(
        "redis_url".into(),
        json!(format!("redis://{}:6379", rc.redis_ip)),
    );
    m.insert("clients".into(), json!(clients));
    Ok(m)
}

fn ctx_for_node(
    base: &Map<String, Value>,
    role: &str,
    idx: usize,
    rc: &ResolvedCluster,
) -> Result<Value, RenderError> {
    let (ip, raft_id) = match role {
        "master" => (rc.master_ips[idx - 1].clone(), idx as u64),
        "volume" => (rc.volume_ips[idx - 1].clone(), idx as u64),
        "filer" => (rc.filer_ips[idx - 1].clone(), idx as u64),
        _ => return Err(RenderError::UnknownRole(role.into())),
    };
    let node_id = format!("{}-server-{}", role, idx);
    let mut m = base.clone();
    m.insert("idx".into(), json!(idx));
    m.insert("role".into(), json!(role));
    m.insert("ip".into(), json!(ip));
    m.insert("raft_id".into(), json!(raft_id));
    m.insert("node_id".into(), json!(node_id));
    m.insert("advertise_addr".into(), json!(ip));
    Ok(Value::Object(m))
}

#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    #[error("template error: {0}")]
    Template(#[from] minijinja::Error),
    #[error("unknown role: {0}")]
    UnknownRole(String),
}

// ---- templates ----

const COMPOSE_TEMPLATE: &str = include_str!("templates/docker-compose.yml.j2");
const MASTER_TEMPLATE: &str = include_str!("templates/master.toml.j2");
const VOLUME_TEMPLATE: &str = include_str!("templates/volume.toml.j2");
const FILER_TEMPLATE: &str = include_str!("templates/filer.toml.j2");
const MONITOR_TEMPLATE: &str = include_str!("templates/monitor.toml.j2");
const FUSE_TEMPLATE: &str = include_str!("templates/fuse.toml.j2");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ClusterConfig;
    use powerfs_common::config::{PowerFsConfig, ServiceType};

    fn sample() -> ResolvedCluster {
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
        cfg.validate().unwrap()
    }

    /// Every rendered role TOML must be parseable by the real PowerFsConfig
    /// (the runtime parser), not just syntactically valid TOML.
    #[test]
    fn rendered_toml_parses_as_powerfs_config() {
        let rc = sample();
        let rendered = render(&rc).unwrap();
        for (name, toml_str) in &rendered.configs {
            // Parse the same way each service does (toml::from_str), then run
            // the role-appropriate validator (master uses full validate(), the
            // rest use validate_for(service)). load_from_string is not used
            // here because it unconditionally calls the full validate().
            let cfg: PowerFsConfig = toml::from_str(toml_str)
                .unwrap_or_else(|e| panic!("rendered {}.toml failed to deserialize: {}", name, e));
            let svc = match name.split('-').next().unwrap() {
                "master" => ServiceType::Master,
                "volume" => ServiceType::Volume,
                "filer" => ServiceType::Filer,
                "monitor" => ServiceType::Monitor,
                "fuse" => ServiceType::Fuse,
                other => panic!("unexpected rendered role: {}", other),
            };
            cfg.validate_for(svc).unwrap_or_else(|e| {
                panic!("rendered {}.toml failed {:?} validation: {}", name, svc, e)
            });
        }
    }
}
