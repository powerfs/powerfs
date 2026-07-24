use powerfs_filer::powerfs::delta_op::Op;
use powerfs_filer::powerfs::{
    delta_op, filer_meta_service_client::FilerMetaServiceClient, DeltaOp, DirEntryOrset, EntryId,
    PullDeltaRequest, PushDeltaRequest,
};
use std::time::Instant;
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let filer_addr = if args.len() > 1 {
        args[1].clone()
    } else {
        "127.0.0.1:18889".to_string()
    };

    let num_ops: u64 = if args.len() > 2 {
        args[2].parse().unwrap_or(1000)
    } else {
        1000
    };

    println!("=========================================");
    println!("CRDT Metadata Performance Benchmark");
    println!("=========================================");
    println!("Filer: {}", filer_addr);
    println!("Operations: {}", num_ops);
    println!();

    let mut client = FilerMetaServiceClient::connect(format!("http://{}", filer_addr)).await?;
    println!("Connected to Filer successfully\n");

    let shard_id = 0u64;
    let parent_ino = 1u64;
    let client_id = "bench-client-1";

    // ============================================
    // Benchmark 1: Sequential Add (Create)
    // ============================================
    println!("--- Benchmark 1: Sequential Add (Create) ---");
    let start = Instant::now();
    for i in 1..=num_ops {
        let entry = DirEntryOrset {
            parent_ino,
            name: format!("file_{}.txt", i),
            inode: 10000 + i,
            mode: 0o644,
            seq: i,
            client_id: 1,
        };
        let delta = DeltaOp {
            op: Some(Op::Add(entry)),
        };
        let request = PushDeltaRequest {
            shard_id,
            client_id: client_id.to_string(),
            deltas: vec![delta],
            client_vclock: None,
        };
        let resp = client.push_delta(Request::new(request)).await?;
        let resp = resp.into_inner();
        if !resp.success {
            eprintln!("Add failed at i={}: {}", i, resp.error);
            break;
        }
    }
    let elapsed = start.elapsed();
    let ops_per_sec = num_ops as f64 / elapsed.as_secs_f64();
    let avg_us = elapsed.as_secs_f64() * 1_000_000.0 / num_ops as f64;
    println!("  Total time: {:.2?}", elapsed);
    println!("  Throughput: {:.2} ops/sec", ops_per_sec);
    println!("  Average latency: {:.2} us/op", avg_us);
    println!();

    // ============================================
    // Benchmark 2: Batched Add (10 per batch)
    // ============================================
    println!("--- Benchmark 2: Batched Add (10 per batch) ---");
    let batch_size = 10u64;
    let num_batches = num_ops / batch_size;
    let start = Instant::now();
    for batch in 0..num_batches {
        let mut deltas = Vec::with_capacity(batch_size as usize);
        for j in 0..batch_size {
            let i = batch * batch_size + j + num_ops + 1000;
            let entry = DirEntryOrset {
                parent_ino,
                name: format!("batch_file_{}.txt", i),
                inode: 50000 + i,
                mode: 0o644,
                seq: i,
                client_id: 1,
            };
            deltas.push(DeltaOp {
                op: Some(Op::Add(entry)),
            });
        }
        let request = PushDeltaRequest {
            shard_id,
            client_id: client_id.to_string(),
            deltas,
            client_vclock: None,
        };
        let resp = client.push_delta(Request::new(request)).await?;
        let resp = resp.into_inner();
        if !resp.success {
            eprintln!("Batch add failed at batch={}: {}", batch, resp.error);
            break;
        }
    }
    let elapsed = start.elapsed();
    let total_ops = num_batches * batch_size;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    let avg_us = elapsed.as_secs_f64() * 1_000_000.0 / total_ops as f64;
    println!("  Total time: {:.2?}", elapsed);
    println!("  Throughput: {:.2} ops/sec", ops_per_sec);
    println!("  Average latency: {:.2} us/op", avg_us);
    println!();

    // ============================================
    // Benchmark 3: Pull Delta
    // ============================================
    println!("--- Benchmark 3: Pull Delta (incremental sync) ---");
    let server_vclock = {
        let resp = client
            .pull_delta(Request::new(PullDeltaRequest {
                shard_id,
                client_id: client_id.to_string(),
                client_vclock: None,
            }))
            .await?;
        resp.into_inner().server_vclock
    };
    let start = Instant::now();
    let mut total_deltas = 0u64;
    for i in 0..100 {
        let resp = client
            .pull_delta(Request::new(PullDeltaRequest {
                shard_id,
                client_id: client_id.to_string(),
                client_vclock: server_vclock.clone(),
            }))
            .await?;
        let resp = resp.into_inner();
        total_deltas += resp.deltas.len() as u64;
        if i == 0 && total_deltas == 0 {
            println!("  (no new deltas after initial pull, testing empty pull)");
        }
    }
    let elapsed = start.elapsed();
    let pulls_per_sec = 100.0 / elapsed.as_secs_f64();
    let avg_us = elapsed.as_secs_f64() * 1_000_000.0 / 100.0;
    println!("  Total time for 100 pulls: {:.2?}", elapsed);
    println!("  Throughput: {:.2} pulls/sec", pulls_per_sec);
    println!("  Average latency: {:.2} us/pull", avg_us);
    println!("  Total deltas pulled: {}", total_deltas);
    println!();

    // ============================================
    // Benchmark 4: Remove
    // ============================================
    println!("--- Benchmark 4: Sequential Remove (Delete) ---");
    let start = Instant::now();
    for i in 1..=num_ops {
        let entry_id = EntryId {
            parent_ino,
            name: format!("file_{}.txt", i),
        };
        let delta = DeltaOp {
            op: Some(Op::Remove(entry_id)),
        };
        let request = PushDeltaRequest {
            shard_id,
            client_id: client_id.to_string(),
            deltas: vec![delta],
            client_vclock: None,
        };
        let resp = client.push_delta(Request::new(request)).await?;
        let resp = resp.into_inner();
        if !resp.success {
            eprintln!("Remove failed at i={}: {}", i, resp.error);
            break;
        }
    }
    let elapsed = start.elapsed();
    let ops_per_sec = num_ops as f64 / elapsed.as_secs_f64();
    let avg_us = elapsed.as_secs_f64() * 1_000_000.0 / num_ops as f64;
    println!("  Total time: {:.2?}", elapsed);
    println!("  Throughput: {:.2} ops/sec", ops_per_sec);
    println!("  Average latency: {:.2} us/op", avg_us);
    println!();

    // ============================================
    // Benchmark 5: Mixed Workload (70% read, 20% write, 10% delete)
    // ============================================
    println!("--- Benchmark 5: Mixed Workload (pull=70%, add=20%, remove=10%) ---");
    let mixed_ops = num_ops;
    let start = Instant::now();
    let mut add_count = 0u64;
    let mut remove_count = 0u64;
    let mut pull_count = 0u64;

    for i in 0..mixed_ops {
        let op_type = i % 10;
        if op_type < 7 {
            // Pull (read)
            let _ = client
                .pull_delta(Request::new(PullDeltaRequest {
                    shard_id,
                    client_id: client_id.to_string(),
                    client_vclock: None,
                }))
                .await?;
            pull_count += 1;
        } else if op_type < 9 {
            // Add (write)
            let file_idx = 100000 + i;
            let entry = DirEntryOrset {
                parent_ino,
                name: format!("mixed_file_{}.txt", file_idx),
                inode: 200000 + file_idx,
                mode: 0o644,
                seq: file_idx,
                client_id: 1,
            };
            let delta = DeltaOp {
                op: Some(Op::Add(entry)),
            };
            let request = PushDeltaRequest {
                shard_id,
                client_id: client_id.to_string(),
                deltas: vec![delta],
                client_vclock: None,
            };
            let _ = client.push_delta(Request::new(request)).await?;
            add_count += 1;
        } else {
            // Remove (delete)
            let file_idx = 100000 + i - (i / 10);
            let entry_id = EntryId {
                parent_ino,
                name: format!("mixed_file_{}.txt", file_idx),
            };
            let delta = DeltaOp {
                op: Some(Op::Remove(entry_id)),
            };
            let request = PushDeltaRequest {
                shard_id,
                client_id: client_id.to_string(),
                deltas: vec![delta],
                client_vclock: None,
            };
            let _ = client.push_delta(Request::new(request)).await?;
            remove_count += 1;
        }
    }
    let elapsed = start.elapsed();
    let ops_per_sec = mixed_ops as f64 / elapsed.as_secs_f64();
    let avg_us = elapsed.as_secs_f64() * 1_000_000.0 / mixed_ops as f64;
    println!("  Total time: {:.2?}", elapsed);
    println!(
        "  Total ops: {} (add={}, remove={}, pull={})",
        mixed_ops, add_count, remove_count, pull_count
    );
    println!("  Throughput: {:.2} ops/sec", ops_per_sec);
    println!("  Average latency: {:.2} us/op", avg_us);
    println!();

    println!("=========================================");
    println!("Benchmark Complete!");
    println!("=========================================");

    Ok(())
}
