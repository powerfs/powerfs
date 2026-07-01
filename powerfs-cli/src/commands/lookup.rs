use crate::client::MasterClient;
use clap::Args;

/// Lookup volume location
#[derive(Args)]
pub struct LookupArgs {
    /// Volume ID or FID to lookup
    #[arg(short, long)]
    volume_id: Option<u32>,

    /// FID string (e.g., "1,12345,100")
    #[arg(long)]
    fid: Option<String>,
}

pub async fn lookup(mut client: MasterClient, args: LookupArgs) -> super::CommandResult {
    let volume_or_file_id = if let Some(vid) = args.volume_id {
        vid.to_string()
    } else if let Some(fid) = args.fid {
        fid
    } else {
        return Err(powerfs_common::error::PowerFsError::InvalidRequest(
            "Either --volume-id or --fid must be specified".to_string(),
        ));
    };

    println!("Looking up: {}", volume_or_file_id);

    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let request = powerfs_master::proto::LookupVolumeRequest {
        volume_or_file_ids: vec![volume_or_file_id.clone()],
        collection: String::new(),
    };

    let response = service
        .lookup_volume(tonic::Request::new(request))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let result = response.into_inner();

    for loc in result.volume_id_locations {
        println!("\n=== Volume/File ID: {} ===", loc.volume_or_file_id);

        if !loc.error.is_empty() {
            println!("Error: {}", loc.error);
            continue;
        }

        if loc.locations.is_empty() {
            println!("No locations found");
        } else {
            println!("Locations:");
            for (i, location) in loc.locations.iter().enumerate() {
                println!("  {}: {}", i + 1, location.url);
                println!("     Public URL: {}", location.public_url);
                println!("     Data Center: {}", location.data_center);
            }
        }
    }

    Ok(())
}
