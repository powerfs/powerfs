use crate::client::{AddNodeRequest, AddNodeResponse, MasterClient};
use clap::Args;

#[derive(Args)]
pub struct ClusterAddArgs {
    #[arg(long)]
    node_id: u64,

    #[arg(long)]
    address: String,
}

pub async fn cluster_add(mut client: MasterClient, args: ClusterAddArgs) -> super::CommandResult {
    println!(
        "Adding node {} at {} to cluster",
        args.node_id, args.address
    );

    let mut service = client.raft_service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let request = AddNodeRequest {
        node_id: args.node_id,
        address: args.address,
    };

    let response = service
        .add_node(tonic::Request::new(request))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(e))?;

    let result: AddNodeResponse = response.into_inner();

    if result.success {
        println!("Node added successfully");
    } else {
        return Err(powerfs_common::error::PowerFsError::Internal(format!(
            "Failed to add node: {}",
            result.error
        )));
    }

    Ok(())
}
