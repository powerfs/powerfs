use crate::client::MasterClient;
use clap::Args;

/// Show master status
#[derive(Args)]
pub struct StatusArgs {
    /// Show detailed information
    #[arg(short, long)]
    detailed: bool,
}

pub async fn status(mut client: MasterClient, args: StatusArgs) -> super::CommandResult {
    println!("Master status for: {}", client.address);

    // Try to get volume list as status indicator
    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    // Get volume list
    let volume_list = service
        .volume_list(tonic::Request::new(
            powerfs_master::proto::VolumeListRequest {},
        ))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let response = volume_list.into_inner();

    println!("\n=== Master Status ===");
    println!("Volume size limit: {} bytes", response.volume_size_limit);
    println!("Data nodes: {}", response.data_nodes.len());

    if args.detailed {
        println!("\n=== Data Nodes ===");
        for node in response.data_nodes {
            println!("  Node ID: {}", node.id);
            println!("    Address: {}", node.address);
            println!("    GRPC Port: {}", node.grpc_port);
            println!("    Data Center: {}", node.data_center);
            println!("    Rack: {}", node.rack);
            println!("    Volumes: {}", node.volumes.len());

            for vol in node.volumes {
                println!(
                    "      Volume {}: size={}, readonly={}, collection={}",
                    vol.volume_id, vol.size, vol.read_only, vol.collection
                );
            }
        }
    } else {
        println!(
            "Total volumes: {}",
            response
                .data_nodes
                .iter()
                .map(|n| n.volumes.len())
                .sum::<usize>()
        );
    }

    Ok(())
}
