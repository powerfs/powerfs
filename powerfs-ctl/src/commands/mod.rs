//! Command handlers. M1 implements init + config (render/check/show);
//! M2 adds up/down/status with compose driver + zombie-leader health gate;
//! M3 adds cert (init-ca/issue/list) + client enroll + bootstrap one-shot.
//! The rest print a clear "not implemented in M3" message so the command
//! surface is fully discoverable via --help.

mod bootstrap;
mod cert_init_ca;
mod cert_issue;
mod cert_list;
mod client_enroll;
mod config_check;
mod config_render;
mod config_show;
mod down;
mod init;
mod status;
mod up;

use crate::cert::MasterCertClient;
use crate::cli::{CertAction, ClientAction, Commands, ConfigAction, NodeAction, ProfileArg};
use crate::compose::DockerComposeDriver;
use crate::health::ReqwestProbe;
use crate::home::Home;
use crate::schema::Profile;

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
}

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
        Commands::Restart { .. } => not_impl("restart"),
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
        Commands::Doctor { .. } => not_impl("doctor"),
        Commands::Logs { .. } => not_impl("logs"),
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
        "`powerfs-ctl {}` is not implemented in M3 (see .trae/documents/powerfs-ctl_M3_plan.md)",
        name
    ))
}
