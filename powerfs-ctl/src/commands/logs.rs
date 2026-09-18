//! `powerfs-ctl logs` — thin wrapper over `docker compose logs` with the
//! same `--role` filter as up/down/restart. stdio is inherited by the
//! driver, so `--follow` behaves exactly like the raw docker CLI.

use crate::commands::up::services_for_role;
use crate::compose::ComposeDriver;
use crate::home::Home;

pub async fn run(
    home: &Home,
    role: Option<String>,
    follow: bool,
    driver: &dyn ComposeDriver,
) -> Result<(), String> {
    let cfg = home.load_cluster()?;
    let rc = cfg.validate().map_err(|e| e.to_string())?;
    let compose = home.rendered_compose();
    if !compose.exists() {
        return Err(format!(
            "{} missing — run `powerfs-ctl config render` first",
            compose.display()
        ));
    }

    // Empty filter → empty service list → logs for every service.
    let svcs = services_for_role(&role, &rc);
    let args: Vec<&str> = svcs.iter().map(|s| s.as_str()).collect();
    driver.logs(&compose, &args, follow).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compose::tests::MockComposeDriver;

    fn ha_home_with_compose() -> Home {
        let (home, _dir) = crate::commands::test_support::home_with_cluster();
        std::fs::write(home.rendered_compose(), "# placeholder").unwrap();
        home
    }

    #[tokio::test]
    async fn logs_without_role_covers_all_services() {
        let home = ha_home_with_compose();
        let driver = MockComposeDriver::new(vec![]);
        run(&home, None, false, &driver).await.unwrap();
        let calls = driver.calls.lock().unwrap().clone();
        assert_eq!(calls, vec!["logs"]);
    }

    #[tokio::test]
    async fn logs_role_and_follow_are_forwarded() {
        let home = ha_home_with_compose();
        let driver = MockComposeDriver::new(vec![]);
        run(&home, Some("master".into()), true, &driver)
            .await
            .unwrap();
        let calls = driver.calls.lock().unwrap().clone();
        assert_eq!(calls, vec!["logs -f master-1,master-2,master-3"]);
    }

    #[tokio::test]
    async fn logs_without_compose_errors() {
        let (home, _dir) = crate::commands::test_support::home_with_cluster();
        let driver = MockComposeDriver::new(vec![]);
        let err = run(&home, None, false, &driver).await.unwrap_err();
        assert!(err.contains("config render"), "got: {err}");
    }
}
