use crate::client::MasterClient;
use clap::Args;

#[derive(Args)]
pub struct StatusArgs {
    #[arg(short, long)]
    detailed: bool,
}

pub async fn status(mut client: MasterClient, args: StatusArgs) -> super::CommandResult {
    println!("Master status for: {}", client.address);

    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let cluster_info = service
        .get_cluster_info(tonic::Request::new(
            powerfs_master::proto::ClusterInfoRequest {},
        ))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let cluster = cluster_info.into_inner();

    let volume_list = service
        .volume_list(tonic::Request::new(
            powerfs_master::proto::VolumeListRequest {},
        ))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let response = volume_list.into_inner();

    let total_volumes: usize = response.data_nodes.iter().map(|n| n.volumes.len()).sum();

    println!("\n=== Cluster Status ===");
    println!("Leader: {}", if cluster.is_leader { "Yes" } else { "No" });
    println!("Node ID: {}", cluster.node_id);
    println!("Address: {}", cluster.address);
    println!("Raft Term: {}", cluster.term);
    println!("Peers: {}", cluster.peers.len());
    for peer in cluster.peers {
        println!("  - {}", peer);
    }

    println!("\n=== Master Status ===");
    println!("Volume size limit: {} bytes", response.volume_size_limit);
    println!("Data nodes: {}", response.data_nodes.len());
    println!("Total volumes: {}", total_volumes);

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
    }

    Ok(())
}
