use powerfs_filer::powerfs::posix_meta_service_client::PosixMetaServiceClient;
use powerfs_filer::powerfs::ListEntriesRequest;
use std::time::Instant;
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let addr = if args.len() > 1 {
        args[1].clone()
    } else {
        "127.0.0.1:18889".to_string()
    };

    println!("Connecting to {}...", addr);
    let mut client = PosixMetaServiceClient::connect(format!("http://{}", addr)).await?;
    println!("Connected!\n");

    // Test: list_entries
    println!("--- Test: list_entries ---");
    let start = Instant::now();
    let resp = client
        .list_entries(Request::new(ListEntriesRequest {
            parent_ino: 1,
            limit: 100,
            last_name: "".to_string(),
        }))
        .await;
    let elapsed = start.elapsed();
    match resp {
        Ok(r) => {
            let r = r.into_inner();
            println!("  Response: entries={}, error={}", r.entries.len(), r.error);
            println!("  Time: {:.2?}", elapsed);
        }
        Err(e) => {
            println!("  Error: {}", e);
            println!("  Time: {:.2?}", elapsed);
        }
    }

    Ok(())
}
