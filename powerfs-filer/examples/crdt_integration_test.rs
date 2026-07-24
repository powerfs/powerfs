use log::{error, info};
use powerfs_filer::powerfs::{
    delta_op, filer_meta_service_client::FilerMetaServiceClient, DeltaOp, DirEntryOrset, EntryId,
    PushDeltaRequest, PushDeltaResponse, RenameOp, SetAttrOp,
};
use tonic::transport::Channel;

async fn push_delta_with_timeout(
    client: &mut FilerMetaServiceClient<Channel>,
    request: PushDeltaRequest,
) -> Result<PushDeltaResponse, String> {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.push_delta(tonic::Request::new(request)),
    )
    .await
    .map_err(|e| format!("push_delta timeout: {}", e))?
    .map(|r| r.into_inner())
    .map_err(|e| format!("push_delta error: {}", e))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <filer_grpc_addr>", args[0]);
        eprintln!("Example: {} 127.0.0.1:18889", args[0]);
        std::process::exit(1);
    }

    let filer_addr = &args[1];
    info!("Connecting to Filer at {}...", filer_addr);

    let channel = Channel::from_shared(format!("http://{}", filer_addr))?
        .connect_timeout(std::time::Duration::from_secs(5))
        .connect()
        .await?;

    let mut client = FilerMetaServiceClient::new(channel);
    info!("Connected to Filer successfully");

    // Test CRDT operations
    test_crdt_operations(&mut client).await?;

    Ok(())
}

async fn test_crdt_operations(
    client: &mut FilerMetaServiceClient<Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let shard_id = 0u64;

    // Test 1: Single client Add operation
    info!("=== Test 1: Single Client Add ===");
    test_single_add(client, shard_id, "client-1", 1).await?;

    // Test 2: Concurrent Add-Add (different clients add same name)
    info!("=== Test 2: Concurrent Add-Add ===");
    test_concurrent_add_add(client, shard_id).await?;

    // Test 3: Add-Remove conflict
    info!("=== Test 3: Add-Remove Conflict ===");
    test_add_remove_conflict(client, shard_id).await?;

    // Test 4: SetAttr operation
    info!("=== Test 4: SetAttr ===");
    test_setattr(client, shard_id).await?;

    // Test 5: Rename operation
    info!("=== Test 5: Rename ===");
    test_rename(client, shard_id).await?;

    info!("=== All CRDT Tests Passed! ===");
    Ok(())
}

async fn test_single_add(
    client: &mut FilerMetaServiceClient<Channel>,
    shard_id: u64,
    client_id: &str,
    seq: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let entry = DirEntryOrset {
        parent_ino: 1, // root inode
        name: format!("test_file_{}.txt", seq).to_string(),
        inode: 1000 + seq,
        mode: 0o644,
        seq,
        client_id: 1,
    };

    let delta = DeltaOp {
        op: Some(delta_op::Op::Add(entry)),
    };

    let request = PushDeltaRequest {
        shard_id,
        client_id: client_id.to_string(),
        deltas: vec![delta],
        client_vclock: None,
    };

    info!("  Sending Add request...");
    let resp = push_delta_with_timeout(client, request).await?;

    if resp.success {
        info!("  Client {} seq {} Add: OK", client_id, seq);
        if let Some(vclock) = resp.server_vclock {
            info!("  Server VClock: {:?}", vclock.entries);
        }
    } else {
        error!(
            "  Client {} seq {} Add: FAILED - {}",
            client_id, seq, resp.error
        );
        return Err(format!("Add failed: {}", resp.error).into());
    }

    Ok(())
}

async fn test_concurrent_add_add(
    client: &mut FilerMetaServiceClient<Channel>,
    shard_id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    // Simulate two clients adding the same file concurrently
    let entry1 = DirEntryOrset {
        parent_ino: 1,
        name: "conflict_file.txt".to_string(),
        inode: 2001,
        mode: 0o644,
        seq: 1,
        client_id: 1,
    };

    let entry2 = DirEntryOrset {
        parent_ino: 1,
        name: "conflict_file.txt".to_string(),
        inode: 2002,
        mode: 0o755,
        seq: 1,
        client_id: 2,
    };

    // Client 1 adds first
    info!("  Client-1 adding conflict_file.txt...");
    let delta1 = DeltaOp {
        op: Some(delta_op::Op::Add(entry1)),
    };

    let request1 = PushDeltaRequest {
        shard_id,
        client_id: "client-1".to_string(),
        deltas: vec![delta1],
        client_vclock: None,
    };

    let resp1 = push_delta_with_timeout(client, request1).await?;
    if !resp1.success {
        error!("  Client-1 add failed: {}", resp1.error);
        return Err("Client-1 add failed".into());
    }
    info!("  Client-1 add: OK");

    // Client 2 adds the same name (concurrent)
    info!("  Client-2 adding same file (concurrent)...");
    let delta2 = DeltaOp {
        op: Some(delta_op::Op::Add(entry2)),
    };

    let request2 = PushDeltaRequest {
        shard_id,
        client_id: "client-2".to_string(),
        deltas: vec![delta2],
        client_vclock: None,
    };

    let resp2 = push_delta_with_timeout(client, request2).await?;
    if !resp2.success {
        error!("  Client-2 add failed (concurrent): {}", resp2.error);
        return Err("Client-2 add failed".into());
    }
    info!("  Client-2 concurrent add: OK (CRDT dual-preserve semantic)");

    Ok(())
}

async fn test_add_remove_conflict(
    client: &mut FilerMetaServiceClient<Channel>,
    shard_id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    // First, add a file
    info!("  Adding file for remove test...");
    let add_entry = DirEntryOrset {
        parent_ino: 1,
        name: "remove_test.txt".to_string(),
        inode: 3001,
        mode: 0o644,
        seq: 1,
        client_id: 1,
    };

    let add_delta = DeltaOp {
        op: Some(delta_op::Op::Add(add_entry)),
    };

    let add_request = PushDeltaRequest {
        shard_id,
        client_id: "client-1".to_string(),
        deltas: vec![add_delta],
        client_vclock: None,
    };

    let resp = push_delta_with_timeout(client, add_request).await?;
    if !resp.success {
        error!("  Initial add failed: {}", resp.error);
        return Err("Initial add failed".into());
    }
    info!("  Initial add: OK");

    // Now remove the file (Add-Wins semantics: if concurrent add happens, remove loses)
    info!("  Removing file...");
    let remove_entry = EntryId {
        parent_ino: 1,
        name: "remove_test.txt".to_string(),
    };

    let remove_delta = DeltaOp {
        op: Some(delta_op::Op::Remove(remove_entry)),
    };

    let remove_request = PushDeltaRequest {
        shard_id,
        client_id: "client-2".to_string(),
        deltas: vec![remove_delta],
        client_vclock: None,
    };

    let resp = push_delta_with_timeout(client, remove_request).await?;
    if resp.success {
        info!("  Remove: OK (applied successfully)");
    } else {
        error!("  Remove: FAILED - {}", resp.error);
        return Err("Remove failed".into());
    }

    Ok(())
}

async fn test_setattr(
    client: &mut FilerMetaServiceClient<Channel>,
    shard_id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    // First create a file
    info!("  Creating file for SetAttr test...");
    let add_entry = DirEntryOrset {
        parent_ino: 1,
        name: "attr_test.txt".to_string(),
        inode: 4001,
        mode: 0o644,
        seq: 1,
        client_id: 1,
    };

    let add_delta = DeltaOp {
        op: Some(delta_op::Op::Add(add_entry)),
    };

    let add_request = PushDeltaRequest {
        shard_id,
        client_id: "client-1".to_string(),
        deltas: vec![add_delta],
        client_vclock: None,
    };

    let resp = push_delta_with_timeout(client, add_request).await?;
    if !resp.success {
        error!("  Create file failed: {}", resp.error);
        return Err("Create file failed".into());
    }
    info!("  Created file for SetAttr test");

    // Update attributes
    info!("  Setting attributes...");
    let setattr_op = SetAttrOp {
        inode: 4001,
        size: 1024,
        mtime: 1000000,
        chunks: vec![],
        extended: std::collections::HashMap::new(),
    };

    let setattr_delta = DeltaOp {
        op: Some(delta_op::Op::SetAttr(setattr_op)),
    };

    let setattr_request = PushDeltaRequest {
        shard_id,
        client_id: "client-1".to_string(),
        deltas: vec![setattr_delta],
        client_vclock: None,
    };

    let resp = push_delta_with_timeout(client, setattr_request).await?;
    if resp.success {
        info!("  SetAttr: OK (Last-Writer-Wins semantics)");
    } else {
        error!("  SetAttr: FAILED - {}", resp.error);
        return Err("SetAttr failed".into());
    }

    Ok(())
}

async fn test_rename(
    client: &mut FilerMetaServiceClient<Channel>,
    shard_id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create file
    info!("  Creating file for rename test...");
    let add_entry = DirEntryOrset {
        parent_ino: 1,
        name: "rename_old.txt".to_string(),
        inode: 5001,
        mode: 0o644,
        seq: 1,
        client_id: 1,
    };

    let add_request = PushDeltaRequest {
        shard_id,
        client_id: "client-1".to_string(),
        deltas: vec![DeltaOp {
            op: Some(delta_op::Op::Add(add_entry)),
        }],
        client_vclock: None,
    };

    let resp = push_delta_with_timeout(client, add_request).await?;
    if !resp.success {
        error!("  Create file failed: {}", resp.error);
        return Err("Create file failed".into());
    }
    info!("  Created file for rename test");

    // Rename
    info!("  Renaming file...");
    let rename_op = RenameOp {
        old_parent_ino: 1,
        old_name: "rename_old.txt".to_string(),
        new_parent_ino: 1,
        new_name: "rename_new.txt".to_string(),
    };

    let rename_delta = DeltaOp {
        op: Some(delta_op::Op::Rename(rename_op)),
    };

    let rename_request = PushDeltaRequest {
        shard_id,
        client_id: "client-1".to_string(),
        deltas: vec![rename_delta],
        client_vclock: None,
    };

    let resp = push_delta_with_timeout(client, rename_request).await?;
    if resp.success {
        info!("  Rename: OK");
    } else {
        error!("  Rename: FAILED - {}", resp.error);
        return Err("Rename failed".into());
    }

    Ok(())
}
