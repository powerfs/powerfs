use crate::client::{MasterClient, TransferLeaderRequest, TransferLeaderResponse};
use clap::Args;

#[derive(Args)]
pub struct ClusterTransferArgs {
    #[arg(long)]
    target_node_id: u64,
}

pub async fn cluster_transfer(
    mut client: MasterClient,
    args: ClusterTransferArgs,
) -> super::CommandResult {
    println!("Transferring leadership to node {}", args.target_node_id);

    let mut service = client.raft_service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let request = TransferLeaderRequest {
        target_node_id: args.target_node_id,
    };

    let response = service
        .transfer_leader(tonic::Request::new(request))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let result: TransferLeaderResponse = response.into_inner();

    if result.success {
        println!("Leadership transfer initiated");
    } else {
        return Err(powerfs_common::error::PowerFsError::Internal(format!(
            "Failed to transfer leadership: {}",
            result.error
        )));
    }

    Ok(())
}
