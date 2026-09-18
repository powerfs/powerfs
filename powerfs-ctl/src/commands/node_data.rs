//! `powerfs-ctl node data` — data-node lifecycle.
//! - `maintenance` → POST   /api/admin/nodes/{name}/maintenance  (toggle)
//! - `remove`       → DELETE /api/admin/nodes/{name}             (remove node)
//!
//! Both are raft-mutating → leader-only (dispatch discovers the leader before
//! calling).

use crate::admin::AdminClient;

pub async fn maintenance<A: AdminClient>(
    client: &A,
    master_api: &str,
    admin_token: &str,
    name: &str,
    enabled: bool,
) -> Result<(), String> {
    client
        .set_maintenance(master_api, admin_token, name, enabled)
        .await?;
    let state = if enabled { "ON" } else { "OFF" };
    println!("✓ maintenance {} for data node '{}'", state, name);
    Ok(())
}

pub async fn remove<A: AdminClient>(
    client: &A,
    master_api: &str,
    admin_token: &str,
    name: &str,
    force: bool,
) -> Result<(), String> {
    client
        .remove_node(master_api, admin_token, name, force)
        .await?;
    println!("✓ data node '{}' removed (force={})", name, force);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::MockAdminClient;

    #[tokio::test]
    async fn maintenance_on_sends_enabled_true() {
        let client = MockAdminClient::new().with_maintenance(Ok(()));
        maintenance(&client, "m:9300", "tok", "volume-1", true)
            .await
            .unwrap();
        let calls = client.maintenance_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "volume-1");
        assert!(calls[0].enabled);
    }

    #[tokio::test]
    async fn maintenance_off_sends_enabled_false() {
        let client = MockAdminClient::new().with_maintenance(Ok(()));
        maintenance(&client, "m:9300", "tok", "volume-1", false)
            .await
            .unwrap();
        let calls = client.maintenance_calls();
        assert!(!calls[0].enabled);
    }

    #[tokio::test]
    async fn maintenance_409_node_owns_routes() {
        let client = MockAdminClient::new()
            .with_maintenance(Err("HTTP 409: node owns active routes".into()));
        let err = maintenance(&client, "m:9300", "tok", "volume-1", true)
            .await
            .unwrap_err();
        assert!(err.contains("409"), "got: {err}");
    }

    #[tokio::test]
    async fn data_remove_sends_name_and_force() {
        let client = MockAdminClient::new().with_remove_node(Ok(()));
        remove(&client, "m:9300", "tok", "volume-2", true)
            .await
            .unwrap();
        let calls = client.remove_node_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "volume-2");
        assert!(calls[0].force);
    }

    #[tokio::test]
    async fn data_remove_409_without_force() {
        let client = MockAdminClient::new()
            .with_remove_node(Err("HTTP 409: node owns routes; use --force".into()));
        let err = remove(&client, "m:9300", "tok", "volume-2", false)
            .await
            .unwrap_err();
        assert!(err.contains("409"), "got: {err}");
    }
}
