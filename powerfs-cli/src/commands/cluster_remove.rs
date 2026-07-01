use crate::client::{MasterClient, RemoveNodeRequest, RemoveNodeResponse};
use clap::Args;

#[derive(Args)]
pub struct ClusterRemoveArgs {
    #[arg(long)]
    node_id: u64,
}

pub async fn cluster_remove(
    mut client: MasterClient,
    args: ClusterRemoveArgs,
) -> super::CommandResult {
    println!("Removing node {} from cluster", args.node_id);

    let mut service = client.raft_service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let request = RemoveNodeRequest {
        node_id: args.node_id,
    };

    let response = service
        .remove_node(tonic::Request::new(request))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(e))?;

    let result: RemoveNodeResponse = response.into_inner();

    if result.success {
        println!("Node removed successfully");
    } else {
        return Err(powerfs_common::error::PowerFsError::Internal(format!(
            "Failed to remove node: {}",
            result.error
        )));
    }

    Ok(())
}
