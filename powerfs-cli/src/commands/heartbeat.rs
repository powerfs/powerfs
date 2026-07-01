use crate::client::MasterClient;
use clap::Args;
use std::time::Duration;
use tokio_stream::StreamExt as _;

/// Send heartbeat to master (simulate volume server)
#[derive(Args)]
pub struct HeartbeatArgs {
    /// Volume server ID
    #[arg(short, long, default_value = "test-volume-server")]
    id: String,

    /// IP address
    #[arg(long, default_value = "127.0.0.1")]
    ip: String,

    /// HTTP port
    #[arg(short, long, default_value = "8080")]
    port: u32,

    /// GRPC port
    #[arg(long, default_value = "8081")]
    grpc_port: u32,

    /// Rack name
    #[arg(long, default_value = "default")]
    rack: String,

    /// Data center name
    #[arg(long, default_value = "default")]
    data_center: String,

    /// Public URL
    #[arg(long, default_value = "")]
    public_url: String,

    /// Number of heartbeats to send (0 for continuous)
    #[arg(short, long, default_value = "3")]
    count: u32,

    /// Interval between heartbeats (seconds)
    #[arg(long, default_value = "5")]
    interval: u64,
}

pub async fn heartbeat(mut client: MasterClient, args: HeartbeatArgs) -> super::CommandResult {
    println!("Sending heartbeat from volume server: {}", args.id);
    println!(
        "  IP: {}, Port: {}, GRPC: {}",
        args.ip, args.port, args.grpc_port
    );
    println!("  Data Center: {}, Rack: {}", args.data_center, args.rack);
    println!(
        "  Sending {} heartbeats with {}s interval\n",
        args.count, args.interval
    );

    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    // Create heartbeat stream
    let (tx, rx) = tokio::sync::mpsc::channel(10);

    // Spawn task to send heartbeats
    let id = args.id.clone();
    let ip = args.ip.clone();
    let port = args.port;
    let grpc_port = args.grpc_port;
    let rack = args.rack.clone();
    let data_center = args.data_center.clone();
    let public_url = args.public_url.clone();
    let count = args.count;
    let interval = args.interval;

    tokio::spawn(async move {
        let mut sent = 0u32;

        while count == 0 || sent < count {
            let heartbeat = powerfs_master::proto::Heartbeat {
                ip: ip.clone(),
                port,
                public_url: public_url.clone(),
                max_file_key: 0,
                data_center: data_center.clone(),
                rack: rack.clone(),
                admin_port: 0,
                volumes: vec![],
                new_volumes: vec![],
                deleted_volumes: vec![],
                has_no_volumes: false,
                grpc_port,
                id: id.clone(),
            };

            if tx.send(heartbeat).await.is_err() {
                break;
            }

            sent += 1;
            if count > 0 && sent >= count {
                break;
            }

            tokio::time::sleep(Duration::from_secs(interval)).await;
        }
    });

    // Convert mpsc receiver to stream
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    // Send heartbeat stream to master
    let response_stream = service
        .send_heartbeat(tonic::Request::new(stream))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    // Read responses
    let mut responses = response_stream.into_inner();
    let mut received = 0u32;

    while let Some(response) = responses.next().await {
        match response {
            Ok(resp) => {
                received += 1;
                println!("Heartbeat #{} response:", received);
                println!("  Leader: {}", resp.leader);
                println!("  Volume size limit: {}", resp.volume_size_limit);
            }
            Err(e) => {
                println!("Error receiving response: {}", e);
                break;
            }
        }

        if count > 0 && received >= count {
            break;
        }
    }

    let sent_str = if count == 0 {
        "continuous"
    } else {
        &count.to_string()
    };
    println!(
        "\nCompleted: sent {} heartbeats, received {} responses",
        sent_str, received
    );

    Ok(())
}
