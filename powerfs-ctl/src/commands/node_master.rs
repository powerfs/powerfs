//! `powerfs-ctl node master` — raft membership lifecycle.
//! - `add`    → POST   /api/admin/masters          (add voter)
//! - `remove` → DELETE /api/admin/masters/{id}     (remove voter)
//! - `list`   → GET    /api/admin/masters          (membership snapshot)
//!
//! `add` and `remove` are raft-mutating → leader-only (dispatch discovers the
//! leader before calling). `list` works on any master.

use crate::admin::{AddMasterRequest, AdminClient, MastersSnapshot};

pub async fn add<A: AdminClient>(
    client: &A,
    master_api: &str,
    admin_token: &str,
    id: u64,
    addr: &str,
) -> Result<(), String> {
    let req = AddMasterRequest {
        id,
        addr: addr.into(),
    };
    client.add_master(master_api, admin_token, &req).await?;
    println!("✓ master {} added at {}", id, addr);
    Ok(())
}

pub async fn remove<A: AdminClient>(
    client: &A,
    master_api: &str,
    admin_token: &str,
    id: &str,
    force: bool,
) -> Result<(), String> {
    client
        .remove_master(master_api, admin_token, id, force)
        .await?;
    println!("✓ master {} removed (force={})", id, force);
    Ok(())
}

pub async fn list<A: AdminClient>(
    client: &A,
    master_api: &str,
    admin_token: &str,
) -> Result<(), String> {
    let snap = client.list_masters(master_api, admin_token).await?;
    print_membership(&snap);
    Ok(())
}

fn print_membership(snap: &MastersSnapshot) {
    let leader = snap.leader.as_deref().unwrap_or("(none)");
    println!("RAFT MEMBERSHIP  (local={}, leader={})", snap.local, leader);
    println!();
    println!("{:<6} {:<24} {:<10}", "ID", "ADDR", "ROLE");
    for m in &snap.members {
        let marker = if snap.leader.as_deref() == Some(m.id.as_str()) {
            " ← leader"
        } else {
            ""
        };
        println!("{:<6} {:<24} {:<10}{}", m.id, m.addr, m.role, marker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::{mock::three_voter_snapshot, MockAdminClient};
    use crate::commands::test_support;

    #[tokio::test]
    async fn master_add_sends_request() {
        let client = MockAdminClient::new().with_add(Ok(()));
        add(&client, "m:9300", "tok", 4, "172.30.0.14:9335")
            .await
            .unwrap();
        let calls = client.add_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, 4);
        assert_eq!(calls[0].addr, "172.30.0.14:9335");
    }

    #[tokio::test]
    async fn master_add_500_propagates() {
        let client = MockAdminClient::new().with_add(Err("HTTP 500: raft join failed".into()));
        let err = add(&client, "m:9300", "tok", 4, "1.2.3.4:9335")
            .await
            .unwrap_err();
        assert!(err.contains("500"), "got: {err}");
    }

    #[tokio::test]
    async fn master_remove_sends_id_and_force() {
        let client = MockAdminClient::new().with_remove(Ok(()));
        remove(&client, "m:9300", "tok", "2", true).await.unwrap();
        let calls = client.remove_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "2");
        assert!(calls[0].force);
    }

    #[tokio::test]
    async fn master_remove_409_leader_without_force() {
        let client = MockAdminClient::new()
            .with_remove(Err("HTTP 409: cannot remove leader without --force".into()));
        let err = remove(&client, "m:9300", "tok", "1", false)
            .await
            .unwrap_err();
        assert!(err.contains("409"), "got: {err}");
    }

    #[tokio::test]
    async fn master_list_prints_table() {
        let client = MockAdminClient::new().with_list(Ok(three_voter_snapshot()));
        list(&client, "m:9300", "tok").await.unwrap();
    }

    #[tokio::test]
    async fn master_list_error_propagates() {
        let client = MockAdminClient::new().with_list(Err("HTTP 401".into()));
        let err = list(&client, "m:9300", "tok").await.unwrap_err();
        assert!(err.contains("401"), "got: {err}");
    }

    #[tokio::test]
    async fn leader_api_and_token_finds_leader() {
        let (home, _dir) = test_support::home_with_cluster();
        let probe = test_support::test_probe::TestProbe::new(&["172.30.0.11"], false);
        let (api, tok) = crate::commands::leader_api_and_token(&home, &probe)
            .await
            .unwrap();
        assert_eq!(api, "172.30.0.11:9300");
        assert!(!tok.is_empty());
    }

    #[tokio::test]
    async fn leader_api_and_token_no_leader() {
        let (home, _dir) = test_support::home_with_cluster();
        // TestProbe with no leaders → every IP reports follower
        let probe = test_support::test_probe::TestProbe::new(&[], false);
        let err = crate::commands::leader_api_and_token(&home, &probe)
            .await
            .unwrap_err();
        assert!(err.contains("no raft leader"), "got: {err}");
    }

    #[tokio::test]
    async fn leader_api_and_token_split_brain() {
        let (home, _dir) = test_support::home_with_cluster();
        // both IPs report as leader → split-brain
        let probe =
            test_support::test_probe::TestProbe::new(&["172.30.0.11", "172.30.0.12"], false);
        let err = crate::commands::leader_api_and_token(&home, &probe)
            .await
            .unwrap_err();
        assert!(err.contains("split-brain"), "got: {err}");
    }

    #[tokio::test]
    async fn leader_api_and_token_all_unreachable() {
        let (home, _dir) = test_support::home_with_cluster();
        let probe = test_support::test_probe::TestProbe::unreachable();
        let err = crate::commands::leader_api_and_token(&home, &probe)
            .await
            .unwrap_err();
        assert!(err.contains("no raft leader"), "got: {err}");
    }
}
