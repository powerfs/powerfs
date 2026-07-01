use crate::volume_client::VolumeServerClient;
use clap::Args;

#[derive(Args)]
pub struct ReadArgs {
    #[arg(long, default_value = "localhost:8081")]
    volume_server: String,

    #[arg(short = 'i', long)]
    volume_id: u32,

    #[arg(long)]
    file_key: u64,

    #[arg(short, long)]
    output: Option<String>,
}

pub async fn read(args: ReadArgs) -> super::CommandResult {
    println!(
        "Reading from volume {} file_key {}",
        args.volume_id, args.file_key
    );

    let mut client = VolumeServerClient::new(&args.volume_server);
    let data = client
        .read_needle(args.volume_id, args.file_key)
        .await
        .map_err(|e| {
            powerfs_common::error::PowerFsError::Internal(format!("Read failed: {}", e))
        })?;

    println!("Read {} bytes", data.len());

    if let Some(output) = args.output {
        std::fs::write(&output, &data).map_err(powerfs_common::error::PowerFsError::Io)?;
        println!("Saved to file: {}", output);
    } else {
        if data.len() <= 1024 {
            if let Ok(text) = String::from_utf8(data.clone()) {
                println!("Content:\n{}", text);
            } else {
                println!(
                    "Binary data (first 100 bytes): {:?}",
                    &data[..std::cmp::min(data.len(), 100)]
                );
            }
        } else {
            println!("Binary data (first 100 bytes): {:?}", &data[..100]);
            println!("(Content truncated, use --output to save to file)");
        }
    }

    Ok(())
}
