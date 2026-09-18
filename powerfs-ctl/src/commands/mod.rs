//! Command handlers. M1 implements init + config (render/check/show);
//! the rest print a clear "not implemented in M1" message so the command
//! surface is fully discoverable via --help.

mod config_check;
mod config_render;
mod config_show;
mod init;

use crate::cli::{CertAction, ClientAction, Commands, ConfigAction, NodeAction};
use crate::home::Home;

pub async fn dispatch(cmd: Commands, home: &Home) -> Result<(), String> {
    match cmd {
        Commands::Init { force } => init::run(home, force).await,
        Commands::Config { action } => match action {
            ConfigAction::Render => config_render::run(home).await,
            ConfigAction::Check => config_check::run(home).await,
            ConfigAction::Show => config_show::run(home).await,
        },
        Commands::Bootstrap { .. } => not_impl("bootstrap"),
        Commands::Up { .. } => not_impl("up"),
        Commands::Down { .. } => not_impl("down"),
        Commands::Restart { .. } => not_impl("restart"),
        Commands::Status => not_impl("status"),
        Commands::Cert { action } => match action {
            CertAction::InitCa => not_impl("cert init-ca"),
            CertAction::Issue { .. } => not_impl("cert issue"),
            CertAction::Renew { .. } => not_impl("cert renew"),
            CertAction::Revoke { .. } => not_impl("cert revoke"),
            CertAction::List => not_impl("cert list"),
        },
        Commands::Node { action } => match action {
            NodeAction::Add { .. } => not_impl("node add"),
            NodeAction::Remove { .. } => not_impl("node remove"),
            NodeAction::Maintenance { .. } => not_impl("node maintenance"),
        },
        Commands::Client { action } => match action {
            ClientAction::Enroll { .. } => not_impl("client enroll"),
        },
        Commands::Doctor { .. } => not_impl("doctor"),
        Commands::Logs { .. } => not_impl("logs"),
    }
}

fn not_impl(name: &str) -> Result<(), String> {
    Err(format!(
        "`powerfs-ctl {}` is not implemented in M1 (see .trae/documents/powerfs-ctl_M1_plan.md)",
        name
    ))
}
