use powerfs_filer::powerfs::filer_meta_service_client::FilerMetaServiceClient;
use powerfs_filer::powerfs::GetEntryRequest;
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
    let mut client = FilerMetaServiceClient::connect(format!("http://{}", addr)).await?;
    println!("Connected!\n");

    // Test: get_entry
    println!("--- Test: FilerMetaService.get_entry ---");
    let start = Instant::now();
    let resp = client
        .get_entry(Request::new(GetEntryRequest {
            path: "/default".to_string(),
        }))
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
