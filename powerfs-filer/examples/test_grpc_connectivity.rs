use powerfs_filer::powerfs::posix_meta_service_client::PosixMetaServiceClient;
use powerfs_filer::powerfs::GetEntryByInodeRequest;
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

    // Test 1: get_entry_by_inode (should be fast, just local read)
    println!("--- Test: get_entry_by_inode ---");
    let start = Instant::now();
    let resp = client
        .get_entry_by_inode(Request::new(GetEntryByInodeRequest { inode: 1 }))
        .await;
    let elapsed = start.elapsed();
    match resp {
        Ok(r) => {
            let r = r.into_inner();
            println!("  Response: found={}, error={}", r.found, r.error);
            println!("  Time: {:.2?}", elapsed);
        }
        Err(e) => {
            println!("  Error: {}", e);
            println!("  Time: {:.2?}", elapsed);
        }
    }

    Ok(())
}
