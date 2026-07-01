use crate::client::MasterClient;
use clap::Args;

#[derive(Args)]
pub struct GrowArgs {
    #[arg(short, long, default_value = "001")]
    replication: String,

    #[arg(short, long, default_value = "default")]
    collection: String,

    #[arg(short = 'n', long, default_value = "1")]
    count: u32,

    #[arg(long, default_value = "")]
    data_node: String,
}

pub async fn grow(mut client: MasterClient, args: GrowArgs) -> super::CommandResult {
    println!(
        "Requesting volume growth: {} volumes with replication {}",
        args.count, args.replication
    );

    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let request = powerfs_master::proto::VolumeGrowRequest {
        replication: args.replication,
        collection: args.collection,
        ttl: String::new(),
        data_center: String::new(),
        rack: String::new(),
        data_node: args.data_node,
        disk_type: String::new(),
        count: args.count,
    };

    let response = service
        .volume_grow(tonic::Request::new(request))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let result = response.into_inner();

    if !result.error.is_empty() {
        println!("Error: {}", result.error);
    } else {
        println!("\nNew volume IDs:");
        for vid in &result.new_volume_ids {
            println!("  - {}", vid);
        }
        println!("\nLocations:");
        for loc in &result.locations {
            println!(
                "  - URL: {}, Public URL: {}, DC: {}",
                loc.url, loc.public_url, loc.data_center
            );
        }
    }

    Ok(())
}
