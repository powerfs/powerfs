//! `powerfs-ctl down` — docker compose down [--purge].

use crate::compose::{ComposeDriver, DockerComposeDriver};
use crate::home::Home;

pub async fn run(
    home: &Home,
    purge: bool,
    _role: Option<String>,
    driver: &dyn ComposeDriver,
) -> Result<(), String> {
    let compose = home.rendered_compose();
    if !compose.exists() {
        let _ = std::fs::remove_file(home.state_file());
        return Err(format!(
            "{} missing — nothing to bring down",
            compose.display()
        ));
    }
    println!(
        "• docker compose down{}",
        if purge { " -v (purge volumes)" } else { "" }
    );
    driver.down(&compose, purge).await?;

    let _ = std::fs::remove_file(home.state_file());
    println!("✓ cluster stopped");
    Ok(())
}

/// Default-driver entry point used by dispatch.
pub async fn run_default(home: &Home, purge: bool, role: Option<String>) -> Result<(), String> {
    run(home, purge, role, &DockerComposeDriver::new()).await
}
