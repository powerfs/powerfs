//! Command handlers. M1 implements init + config (render/check/show);
//! M2 adds up/down/status with compose driver + zombie-leader health gate;
//! M3 adds cert (init-ca/issue/list) + client enroll + bootstrap one-shot;
//! M4 adds rolling restart (per-node gate), read-only doctor, and logs.
//! node lifecycle remains a placeholder pending master admin HTTP APIs (M5).

mod bootstrap;
mod cert_init_ca;
mod cert_issue;
mod cert_list;
mod client_enroll;
mod config_check;
mod config_render;
mod config_show;
mod doctor;
mod down;
mod init;
mod logs;
mod restart;
mod status;
mod up;

use crate::cert::MasterCertClient;
use crate::cli::{CertAction, ClientAction, Commands, ConfigAction, NodeAction, ProfileArg};
use crate::compose::DockerComposeDriver;
use crate::health::ReqwestProbe;
use crate::home::Home;
use crate::schema::Profile;

pub async fn dispatch(cmd: Commands, home: &Home) -> Result<(), String> {
    match cmd {
        Commands::Init { force } => init::run(home, force).await,
        Commands::Config { action } => match action {
            ConfigAction::Render => config_render::run(home).await,
            ConfigAction::Check => config_check::run(home).await,
            ConfigAction::Show => config_show::run(home).await,
        },
        Commands::Bootstrap {
            profile,
            network,
            nodes,
        } => {
            let profile = match profile {
                ProfileArg::Simple => Profile::Simple,
                ProfileArg::Ha => Profile::Ha,
                ProfileArg::Rdma => Profile::Rdma,
            };
            bootstrap::run(
                home,
                profile,
                &network,
                nodes,
                &DockerComposeDriver::new(),
                &ReqwestProbe::new(),
                &MasterCertClient::new(),
            )
            .await
        }
        Commands::Up { role } => {
            up::run(
                home,
                role,
                &DockerComposeDriver::new(),
                &ReqwestProbe::new(),
            )
            .await
        }
        Commands::Down { purge, role } => down::run_default(home, purge, role).await,
        Commands::Restart { role, force } => {
            restart::run(
                home,
                role,
                force,
                &DockerComposeDriver::new(),
                &ReqwestProbe::new(),
            )
            .await
        }
        Commands::Status => status::run(home).await,
        Commands::Cert { action } => match action {
            CertAction::InitCa => {
                let (api, tok) = master_api_and_token(home)?;
                cert_init_ca::run(home, &MasterCertClient::new(), &api, &tok).await
            }
            CertAction::Issue {
                name,
                san_ips,
                mount_dirs,
                node,
            } => {
                let (api, tok) = master_api_and_token(home)?;
                cert_issue::run(
                    home,
                    &MasterCertClient::new(),
                    &api,
                    &tok,
                    &name,
                    &san_ips,
                    &mount_dirs,
                    node,
                )
                .await
            }
            CertAction::List => cert_list::run(home).await,
        },
        Commands::Node { action } => match action {
            NodeAction::Add { .. } => not_impl("node add"),
            NodeAction::Remove { .. } => not_impl("node remove"),
            NodeAction::Maintenance { .. } => not_impl("node maintenance"),
        },
        Commands::Client { action } => match action {
            ClientAction::Enroll { name, ip, kind } => {
                client_enroll::run(home, &MasterCertClient::new(), &name, &ip, kind).await
            }
        },
        Commands::Doctor { fix } => {
            doctor::run(home, fix, &DockerComposeDriver::new(), &ReqwestProbe::new()).await
        }
        Commands::Logs { role, follow } => {
            logs::run(home, role, follow, &DockerComposeDriver::new()).await
        }
    }
}

/// Load cluster.toml, validate, and return (master_api, admin_token) for
/// cert/client handlers that need to talk to master's HTTP API.
fn master_api_and_token(home: &Home) -> Result<(String, String), String> {
    let cfg = home.load_cluster()?;
    let rc = cfg.validate().map_err(|e| e.to_string())?;
    Ok((
        format!("{}:9300", rc.master_ips[0]),
        rc.cfg.cluster.admin_token.clone(),
    ))
}

fn not_impl(name: &str) -> Result<(), String> {
    Err(format!(
        "`powerfs-ctl {name}` is not implemented yet — planned for M5 \
         (requires new master admin HTTP APIs for raft membership)"
    ))
}

#[cfg(test)]
pub(crate) mod test_support {
    use crate::home::Home;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn fresh_dir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let n = SEQ.fetch_add(1, Ordering::SeqCst);
        dir.push(format!("powerfs-ctl-test-{}-{}", std::process::id(), n));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Home with skeleton + a pre-written HA cluster.toml (for cert/client
    /// tests that need load_cluster to succeed).
    pub fn home_with_cluster() -> (Home, PathBuf) {
        let dir = fresh_dir();
        let home = Home { root: dir.clone() };
        home.ensure_skeleton().unwrap();
        std::fs::write(
            home.cluster_toml(),
            crate::commands::init::DEFAULT_CLUSTER_TOML,
        )
        .unwrap();
        (home, dir)
    }

    /// Home with skeleton only (no cluster.toml) — for bootstrap tests where
    /// bootstrap generates the cluster.toml from --profile/--network.
    pub fn home_empty() -> (Home, PathBuf) {
        let dir = fresh_dir();
        let home = Home { root: dir.clone() };
        home.ensure_skeleton().unwrap();
        (home, dir)
    }

    /// Per-IP metrics probe for handler tests: IPs in `leaders` report a
    /// healthy leader, everyone else reports follower. `stall` makes the
    /// leader report commit_index==0 (zombie); `unreachable()` fails every
    /// fetch (daemon down / network cut).
    pub mod test_probe {
        use crate::health::{MasterMetrics, MetricsProbe};
        use async_trait::async_trait;
        use std::collections::HashSet;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Mutex;

        pub struct TestProbe {
            leaders: Mutex<HashSet<String>>,
            stall: bool,
            fail: bool,
            pub fetches: AtomicUsize,
        }

        impl TestProbe {
            pub fn new(leaders: &[&str], stall: bool) -> Self {
                Self {
                    leaders: Mutex::new(leaders.iter().map(|s| s.to_string()).collect()),
                    stall,
                    fail: false,
                    fetches: AtomicUsize::new(0),
                }
            }

            pub fn unreachable() -> Self {
                Self {
                    leaders: Mutex::new(HashSet::new()),
                    stall: false,
                    fail: true,
                    fetches: AtomicUsize::new(0),
                }
            }
        }

        #[async_trait]
        impl MetricsProbe for TestProbe {
            async fn fetch(&self, ip: &str, _port: u16) -> Result<MasterMetrics, String> {
                self.fetches.fetch_add(1, Ordering::SeqCst);
                if self.fail {
                    return Err("connection refused".into());
                }
                let is_leader = self.leaders.lock().unwrap().contains(ip);
                if is_leader {
                    Ok(MasterMetrics {
                        is_leader: true,
                        term: 2,
                        commit_index: if self.stall { 0 } else { 5 },
                        last_applied: 5,
                        healthz_ok: true,
                    })
                } else {
                    Ok(MasterMetrics {
                        is_leader: false,
                        term: 2,
                        commit_index: 0,
                        last_applied: 0,
                        healthz_ok: true,
                    })
                }
            }
        }
    }
}
