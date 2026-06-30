use crate::client::MasterClient;
use clap::Args;

/// List all volumes and nodes
#[derive(Args)]
pub struct VolumeListArgs {
    /// Show detailed volume information
    #[arg(short, long)]
    detailed: bool,
}

pub async fn volume_list(mut client: MasterClient, args: VolumeListArgs) -> super::CommandResult {
    let mut service = client.service().await
        .map_err(|e| powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e)))?;
    
    let response = service.volume_list(tonic::Request::new(powerfs_master::proto::VolumeListRequest {}))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(e))?;
    
    let result = response.into_inner();
    
    println!("=== Volume List ===");
    println!("Volume size limit: {} bytes\n", result.volume_size_limit);
    
    for node in result.data_nodes {
        println!("Node: {}", node.id);
        println!("  Address: {} (grpc: {})", node.address, node.grpc_port);
        println!("  Location: dc={}, rack={}", node.data_center, node.rack);
        
        if node.volumes.is_empty() {
            println!("  No volumes");
        } else {
            println!("  Volumes: {} total", node.volumes.len());
            if args.detailed {
                for vol in node.volumes {
                    println!("    {}: {} bytes, collection={}, readonly={}", 
                        vol.volume_id, vol.size, vol.collection, vol.read_only);
                }
            }
        }
        println!();
    }
    
    Ok(())
}