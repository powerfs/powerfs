use crate::client::MasterClient;
use clap::Args;

/// Assign a new file ID (FID)
#[derive(Args)]
pub struct AssignArgs {
    /// Replication strategy (e.g., "001" for 1 copy)
    #[arg(short, long, default_value = "001")]
    replication: String,

    /// Collection name
    #[arg(short, long, default_value = "default")]
    collection: String,

    /// Number of IDs to assign
    #[arg(long, default_value = "1")]
    count: u64,
}

pub async fn assign(mut client: MasterClient, args: AssignArgs) -> super::CommandResult {
    println!(
        "Assigning FID with replication={}, collection={}",
        args.replication, args.collection
    );

    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let request = powerfs_master::proto::AssignRequest {
        count: args.count,
        replication: args.replication,
        collection: args.collection,
        ttl: String::new(),
        data_center: String::new(),
        rack: String::new(),
        data_node: String::new(),
        disk_type: String::new(),
        stripe_count: 1,
        stripe_size: 64 * 1024 * 1024,
    };

    let response = service
        .assign(tonic::Request::new(request))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let result = response.into_inner();

    if !result.error.is_empty() {
        println!("Error: {}", result.error);
        return Err(powerfs_common::error::PowerFsError::InvalidRequest(
            result.error,
        ));
    }

    println!("\n=== Assigned FID ===");
    println!("FID: {}", result.fid);
    println!("Count: {}", result.count);

    if let Some(location) = result.location {
        println!("Primary location: {}", location.url);
        println!("  Public URL: {}", location.public_url);
        println!("  Data Center: {}", location.data_center);
    }

    if !result.replicas.is_empty() {
        println!("\nReplicas:");
        for (i, replica) in result.replicas.iter().enumerate() {
            println!("  {}: {} (dc: {})", i + 1, replica.url, replica.data_center);
        }
    }

    Ok(())
}
