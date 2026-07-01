use crate::client::MasterClient;
use clap::Args;

#[derive(Args)]
pub struct ClusterStatusArgs {}

pub async fn cluster_status(
    mut client: MasterClient,
    _args: ClusterStatusArgs,
) -> super::CommandResult {
    println!("Cluster status for: {}", client.address);

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

    let statistics = service
        .get_statistics(tonic::Request::new(
            powerfs_master::proto::StatisticsRequest::default(),
        ))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let stats = statistics.into_inner();

    println!("\n=== Cluster Overview ===");
    println!("Leader: {}", if cluster.is_leader { "Yes" } else { "No" });
    println!("Current Node ID: {}", cluster.node_id);
    println!("Current Node Address: {}", cluster.address);
    println!("Raft Term: {}", cluster.term);
    println!("Total Nodes: {}", stats.total_node_count);
    println!("Total Volumes: {}", stats.total_volume_count);
    println!("Total Data Centers: {}", stats.total_data_center_count);
    println!("Total Racks: {}", stats.total_rack_count);

    println!("\n=== Volume Statistics ===");
    println!("Total Volume Size: {} bytes", stats.total_volume_size);
    println!("Used Volume Size: {} bytes", stats.total_used_size);
    println!("Available Volumes: {}", stats.available_volume_count);
    println!("Full Volumes: {}", stats.full_volume_count);
    println!("Read-Only Volumes: {}", stats.read_only_volume_count);

    if !stats.collection_stats.is_empty() {
        println!("\n=== Collection Statistics ===");
        for coll in stats.collection_stats {
            println!(
                "  {}: {} volumes, {} bytes total, {} bytes used",
                coll.name, coll.volume_count, coll.total_size, coll.used_size
            );
        }
    }

    if !stats.data_center_stats.is_empty() {
        println!("\n=== Data Center Statistics ===");
        for dc in stats.data_center_stats {
            println!(
                "  {}: {} nodes, {} volumes",
                dc.name, dc.node_count, dc.volume_count
            );
        }
    }

    if !stats.rack_stats.is_empty() {
        println!("\n=== Rack Statistics ===");
        for rack in stats.rack_stats {
            println!(
                "  {} ({}): {} nodes, {} volumes",
                rack.name, rack.data_center, rack.node_count, rack.volume_count
            );
        }
    }

    Ok(())
}
