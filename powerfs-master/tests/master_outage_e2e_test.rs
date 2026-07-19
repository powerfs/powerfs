// End-to-end test: Master node outage simulation
//
// Verifies the four failover fixes across a real Master crash-and-recover cycle:
//   1. Epoch mechanism: epoch increments on restart, old leases invalidated
//   2. Lease renewal: new leases acquired after restart are renewable
//   3. JOB_COMPLETE batch invalidation: notification reaches clients via gRPC stream
//   4. Generation tracking: metadata changes publish incrementing generation numbers
//
// Tests 1-3 use DirectoryTree API directly to simulate Master restart (dropping and
// re-opening the same RocksDB path). This faithfully reproduces Master process
// restart because DirectoryTree is the component that owns epoch and lease state.
// Tests 4-6 use a live gRPC server to validate the wire-level behavior that
// clients experience (notifications, RPC responses).

use powerfs_master::directory_tree::DirectoryTree;
use powerfs_master::master::MasterNode;
use powerfs_master::proto::powerfs::master_service_client::MasterServiceClient;
use powerfs_master::proto::powerfs::metadata_notification::EventType;
use powerfs_master::proto::powerfs::*;
use powerfs_master::proto::{Entry, FuseAttributes};
use powerfs_master::server::MasterGrpcServer;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tonic::{transport::Channel, Request};

const TEST_CLIENT_ID: &str = "outage-test-client";
const TEST_PATH: &str = "/test/outage/file.txt";

// ----------------------------------------------------------------
// gRPC helpers
// ----------------------------------------------------------------

async fn get_available_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind for port discovery");
    let addr = listener.local_addr().expect("Failed to get local addr");
    drop(listener);
    addr.to_string()
}

struct MasterInstance {
    master: Arc<MasterNode>,
    server_handle: tokio::task::JoinHandle<()>,
}

impl Drop for MasterInstance {
    fn drop(&mut self) {
        self.server_handle.abort();
    }
}

async fn start_master(bind_addr: &str, raft_path: &str) -> MasterInstance {
    let master = Arc::new(
        MasterNode::new(bind_addr, bind_addr, None, raft_path, 1, vec![])
            .await
            .expect("Failed to create MasterNode"),
    );
    let grpc_server = MasterGrpcServer::new(master.clone(), master.kv_cache.clone());
    let addr: std::net::SocketAddr = bind_addr.parse().expect("Invalid bind address");
    let server_handle = tokio::spawn(async move {
        let _ = grpc_server.start(addr).await;
    });

    let endpoint = format!("http://{}", bind_addr);
    for _ in 0..50 {
        if MasterServiceClient::connect(endpoint.clone()).await.is_ok() {
            return MasterInstance {
                master,
                server_handle,
            };
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("Master gRPC server failed to start within 5s");
}

async fn get_client(addr: &str) -> MasterServiceClient<Channel> {
    MasterServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("Failed to connect to Master gRPC server")
}

fn make_test_entry(name: &str, directory: &str) -> Entry {
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
    Entry {
        name: name.to_string(),
        directory: directory.to_string(),
        attributes: Some(FuseAttributes {
            ino: 0,
            mode: 0o100644,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            size: 0,
            blksize: 4096,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            perm: 0o644,
        }),
        chunks: vec![],
        hard_link_id: String::new(),
        hard_link_counter: 0,
        extended: HashMap::new(),
        content_size: 0,
        disk_size: 0,
        ttl: String::new(),
        symlink_target: String::new(),
        owner: String::new(),
        generation: 0,
    }
}

// ================================================================
// Part A: Epoch + lease invalidation via DirectoryTree restart
// (simulates Master process crash and recovery at the storage layer)
// ================================================================

// ----------------------------------------------------------------
// Test 1: Epoch increments on DirectoryTree reopen (Master restart)
// ----------------------------------------------------------------

#[test]
fn test_master_outage_epoch_increments_on_restart() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("directory_tree");

    // First start: epoch should be 1
    let tree1 = DirectoryTree::new(&db_path).expect("Failed to create first DirectoryTree");
    let epoch1 = tree1.get_epoch();
    assert_eq!(epoch1, 1, "First start epoch should be 1, got {}", epoch1);

    drop(tree1);

    // Restart: epoch should increment to 2
    let tree2 = DirectoryTree::new(&db_path).expect("Failed to create second DirectoryTree");
    let epoch2 = tree2.get_epoch();
    assert_eq!(
        epoch2, 2,
        "Epoch should increment to 2 after restart, got {}",
        epoch2
    );

    drop(tree2);

    // Another restart: epoch should be 3
    let tree3 = DirectoryTree::new(&db_path).expect("Failed to create third DirectoryTree");
    let epoch3 = tree3.get_epoch();
    assert_eq!(epoch3, 3, "Epoch should be 3 after second restart");
}

// ----------------------------------------------------------------
// Test 2: Old-epoch lease is invalidated after restart
//   - acquire lease with epoch N
//   - restart (epoch becomes N+1)
//   - simulate a client that still holds the old-epoch lease
//   - has_active_lease must return false (epoch mismatch)
// ----------------------------------------------------------------

#[test]
fn test_master_outage_old_lease_invalidated_by_epoch() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("directory_tree");

    // Phase 1: First Master instance
    let tree1 = DirectoryTree::new(&db_path).expect("Failed to create first DirectoryTree");
    let epoch1 = tree1.get_epoch();

    let lease_id = tree1.acquire_lease(TEST_PATH, TEST_CLIENT_ID, 60000);
    assert!(!lease_id.is_empty(), "Lease ID should not be empty");

    // Verify the lease carries the current epoch
    {
        let leases = tree1.leases.read().unwrap();
        let lease = leases.get(&lease_id).expect("Lease should exist");
        assert_eq!(
            lease.epoch, epoch1,
            "Lease epoch must match Master epoch at acquisition time"
        );
    }
    assert!(
        tree1.has_active_lease(TEST_PATH),
        "Lease should be active in the same epoch"
    );

    drop(tree1);

    // Phase 2: Master restarts — epoch increments
    let tree2 = DirectoryTree::new(&db_path).expect("Failed to create second DirectoryTree");
    let epoch2 = tree2.get_epoch();
    assert_eq!(epoch2, epoch1 + 1, "Epoch must increment by 1 on restart");

    // After restart all in-memory leases are gone, so no active lease
    assert!(
        !tree2.has_active_lease(TEST_PATH),
        "No lease should be active immediately after restart (in-memory state lost)"
    );

    // Simulate a client that believes it still holds a valid lease from the old epoch.
    // We acquire a fresh lease then tamper its epoch to the old value.
    let new_lease_id = tree2.acquire_lease(TEST_PATH, TEST_CLIENT_ID, 60000);
    assert!(
        tree2.has_active_lease(TEST_PATH),
        "New lease with current epoch should be active"
    );

    // Tamper: set the lease epoch to the pre-restart value
    {
        let mut leases = tree2.leases.write().unwrap();
        let lease = leases
            .get_mut(&new_lease_id)
            .expect("New lease should exist");
        lease.epoch = epoch1;
    }
    assert!(
        !tree2.has_active_lease(TEST_PATH),
        "Lease with stale (pre-restart) epoch must be treated as invalid — \
         this is the core epoch-mismatch protection"
    );

    // Restore correct epoch → lease becomes active again
    {
        let mut leases = tree2.leases.write().unwrap();
        let lease = leases
            .get_mut(&new_lease_id)
            .expect("New lease should exist");
        lease.epoch = epoch2;
    }
    assert!(
        tree2.has_active_lease(TEST_PATH),
        "Lease with current epoch should be active after restoring epoch"
    );
}

// ----------------------------------------------------------------
// Test 3: After restart, new lease is renewable
//   - restart Master
//   - acquire new lease
//   - renew it successfully
//   - verify renewal preserves the current epoch
// ----------------------------------------------------------------

#[test]
fn test_master_outage_new_lease_renewable_after_restart() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("directory_tree");

    // First instance: acquire and release a lease
    let tree1 = DirectoryTree::new(&db_path).expect("Failed to create first DirectoryTree");
    let epoch1 = tree1.get_epoch();
    let old_lease_id = tree1.acquire_lease(TEST_PATH, TEST_CLIENT_ID, 1000);
    drop(tree1);

    // Restart: epoch increments
    let tree2 = DirectoryTree::new(&db_path).expect("Failed to create second DirectoryTree");
    let epoch2 = tree2.get_epoch();
    assert_eq!(epoch2, epoch1 + 1);

    // Old lease ID is gone after restart; renewing it must fail
    let renew_old = tree2.renew_lease(&old_lease_id, 30000);
    assert!(
        renew_old.is_none(),
        "Renewing a pre-restart lease ID must fail (lease lost from memory)"
    );

    // Acquire a new lease and renew it
    let new_lease_id = tree2.acquire_lease(TEST_PATH, TEST_CLIENT_ID, 1000);
    assert!(!new_lease_id.is_empty());

    let renew_result = tree2.renew_lease(&new_lease_id, 30000);
    assert!(
        renew_result.is_some(),
        "New lease must be renewable after restart"
    );
    assert_eq!(
        renew_result.unwrap(),
        epoch2,
        "Renewed lease epoch must match current Master epoch (not the old one)"
    );

    // Verify the lease is still active after renewal
    assert!(
        tree2.has_active_lease(TEST_PATH),
        "Lease should remain active after renewal"
    );

    // Release the lease
    let released = tree2.release_lease(&new_lease_id);
    assert!(released, "Lease release should succeed");
    assert!(
        !tree2.has_active_lease(TEST_PATH),
        "Lease should not be active after release"
    );
}

// ----------------------------------------------------------------
// Test 4: Multiple restarts keep epoch strictly increasing
//   - restart 3 times, epoch must be 1 → 2 → 3 → 4
//   - old-epoch leases are never active across restarts
// ----------------------------------------------------------------

#[test]
fn test_multiple_master_restarts_epoch_stability() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("directory_tree");

    let mut epochs: Vec<u64> = Vec::new();
    let mut last_lease_id: Option<String> = None;

    for i in 0..4 {
        let tree = DirectoryTree::new(&db_path).expect("Failed to create DirectoryTree");
        let epoch = tree.get_epoch();
        epochs.push(epoch);

        // Acquire a lease in this epoch
        let lease_id = tree.acquire_lease(
            &format!("/test/restart/{}", i),
            &format!("client-{}", i),
            60000,
        );
        assert!(!lease_id.is_empty());

        // Verify the lease is active
        assert!(
            tree.has_active_lease(&format!("/test/restart/{}", i)),
            "Lease should be active in epoch {}",
            epoch
        );

        // If we had a lease from a previous epoch, its ID is now meaningless
        if let Some(old_id) = &last_lease_id {
            assert!(
                tree.renew_lease(old_id, 30000).is_none(),
                "Lease from previous epoch must not be renewable after restart"
            );
        }

        last_lease_id = Some(lease_id);
        drop(tree);
    }

    // Verify epoch strictly increased by 1 on each restart
    for i in 1..epochs.len() {
        assert_eq!(
            epochs[i],
            epochs[i - 1] + 1,
            "Epoch must increment by 1 on restart #{}: {} -> {}",
            i,
            epochs[i - 1],
            epochs[i]
        );
    }

    assert_eq!(epochs[0], 1, "First start epoch should be 1");
    assert_eq!(
        *epochs.last().unwrap(),
        4,
        "After 3 restarts epoch should be 4"
    );
}

// ================================================================
// Part B: Wire-level behavior via live gRPC server
// (validates what FUSE clients actually observe)
// ================================================================

// ----------------------------------------------------------------
// Test 5: Lease lifecycle through gRPC (acquire → renew → release)
// ----------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lease_lifecycle_via_grpc() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let raft_path = temp_dir.path().join("master_data");
    let addr = get_available_addr().await;
    let instance = start_master(&addr, &raft_path.to_string_lossy()).await;
    let epoch = instance.master.directory_tree.get_epoch();

    let mut client = get_client(&addr).await;

    // Acquire
    let acquire_resp = client
        .acquire_lease(Request::new(LeaseRequest {
            path: TEST_PATH.to_string(),
            duration_ms: 60000,
            client_id: TEST_CLIENT_ID.to_string(),
        }))
        .await
        .expect("acquire_lease RPC failed")
        .into_inner();

    assert!(acquire_resp.success, "Lease acquisition should succeed");
    assert!(
        !acquire_resp.lease_id.is_empty(),
        "Lease ID should not be empty"
    );
    assert_eq!(
        acquire_resp.epoch, epoch,
        "Lease epoch must match Master epoch"
    );

    let lease_id = acquire_resp.lease_id;

    assert!(
        instance.master.directory_tree.has_active_lease(TEST_PATH),
        "Lease should be active on Master side"
    );

    // Renew
    let renew_resp = client
        .renew_lease(Request::new(LeaseRenewRequest {
            lease_id: lease_id.clone(),
            duration_ms: 30000,
        }))
        .await
        .expect("renew_lease RPC failed")
        .into_inner();

    assert!(renew_resp.success, "Lease renewal should succeed");
    assert_eq!(
        renew_resp.epoch, epoch,
        "Renewed lease epoch must match Master epoch"
    );

    // Renew a non-existent lease → must fail
    let renew_bad = client
        .renew_lease(Request::new(LeaseRenewRequest {
            lease_id: "nonexistent-lease-id".to_string(),
            duration_ms: 30000,
        }))
        .await
        .expect("renew_lease RPC for bad lease should still respond")
        .into_inner();
    assert!(
        !renew_bad.success,
        "Renewing a non-existent lease must fail"
    );

    // Release
    let release_resp = client
        .release_lease(Request::new(LeaseReleaseRequest { lease_id }))
        .await
        .expect("release_lease RPC failed")
        .into_inner();
    assert!(release_resp.success, "Lease release should succeed");

    assert!(
        !instance.master.directory_tree.has_active_lease(TEST_PATH),
        "Lease should not be active after release"
    );
}

// ----------------------------------------------------------------
// Test 6: JOB_COMPLETE notification reaches clients via gRPC stream
//   - notification carries correct job_id and epoch
//   - notification has EventType::JOB_COMPLETE
// ----------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_job_complete_notification_via_grpc() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let raft_path = temp_dir.path().join("master_data");
    let addr = get_available_addr().await;
    let instance = start_master(&addr, &raft_path.to_string_lossy()).await;
    let epoch = instance.master.directory_tree.get_epoch();

    let mut client = get_client(&addr).await;

    // Subscribe to metadata notifications BEFORE completing the job
    let stream = client
        .subscribe_metadata(Request::new(SubscribeMetadataRequest {
            path_prefix: String::new(),
        }))
        .await
        .expect("subscribe_metadata RPC failed")
        .into_inner();

    // Register a job (sets current_job_id so notifications carry it)
    let job_id = "outage-test-job-001";
    let reg_resp = client
        .register_job_client(Request::new(JobRegistrationRequest {
            job_id: job_id.to_string(),
            job_name: "outage-e2e-test".to_string(),
            client_id: TEST_CLIENT_ID.to_string(),
        }))
        .await
        .expect("register_job_client RPC failed")
        .into_inner();
    assert!(
        reg_resp.success,
        "Job registration should succeed: {}",
        reg_resp.error
    );

    // Complete the job (publishes JOB_COMPLETE notification)
    let complete_resp = client
        .complete_job(Request::new(JobCompletionRequest {
            job_id: job_id.to_string(),
        }))
        .await
        .expect("complete_job RPC failed")
        .into_inner();
    assert!(complete_resp.success, "Job completion should succeed");

    // Wait for JOB_COMPLETE notification via the gRPC stream
    let mut stream = stream;
    let mut received_job_complete = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(notif))) => {
                if notif.event_type == EventType::JobComplete as i32 {
                    assert_eq!(
                        notif.job_id, job_id,
                        "JOB_COMPLETE notification must carry the correct job_id"
                    );
                    assert_eq!(
                        notif.epoch, epoch,
                        "Notification epoch must match current Master epoch"
                    );
                    assert_eq!(
                        notif.path, "/",
                        "JOB_COMPLETE notification should target root path"
                    );
                    received_job_complete = true;
                    break;
                }
            }
            Ok(None) => break,  // stream closed
            Err(_) => continue, // timeout, keep waiting
            Ok(Some(Err(e))) => {
                panic!("gRPC stream error while waiting for notification: {}", e);
            }
        }
    }

    assert!(
        received_job_complete,
        "Must receive JOB_COMPLETE notification via gRPC stream within 5s"
    );
}

// ----------------------------------------------------------------
// Test 7: Completing a non-existent job returns NOT_FOUND
//   - verifies error handling on the wire
// ----------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_complete_nonexistent_job_returns_error() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let raft_path = temp_dir.path().join("master_data");
    let addr = get_available_addr().await;
    let _instance = start_master(&addr, &raft_path.to_string_lossy()).await;

    let mut client = get_client(&addr).await;

    let result = client
        .complete_job(Request::new(JobCompletionRequest {
            job_id: "does-not-exist".to_string(),
        }))
        .await;

    assert!(
        result.is_err(),
        "Completing a non-existent job must return an error"
    );
    let status = result.unwrap_err();
    assert_eq!(
        status.code(),
        tonic::Code::NotFound,
        "Error should be NOT_FOUND, got {:?}: {}",
        status.code(),
        status.message()
    );
}

// ----------------------------------------------------------------
// Test 8: Generation increments on metadata changes and reaches
//         subscribers with correct values via gRPC stream.
//   - create_entry publishes CREATE with generation N
//   - update_entry publishes UPDATE with generation N+1
//   - delete_entry publishes DELETE
//   - notification generations match stored entry generations
// ----------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_generation_increments_on_metadata_changes() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let raft_path = temp_dir.path().join("master_data");
    let addr = get_available_addr().await;
    let instance = start_master(&addr, &raft_path.to_string_lossy()).await;

    let mut client = get_client(&addr).await;

    // Subscribe to metadata notifications
    let stream = client
        .subscribe_metadata(Request::new(SubscribeMetadataRequest {
            path_prefix: String::new(),
        }))
        .await
        .expect("subscribe_metadata RPC failed")
        .into_inner();

    let dir_tree = instance.master.directory_tree.clone();

    // Ensure parent directory exists
    let _ = dir_tree.create_directory("/test");

    // Create an entry
    let entry = make_test_entry("gen_test_file", "/test");
    let inode = dir_tree
        .create_entry(entry.clone(), "test_client")
        .expect("create_entry failed");

    let stored_after_create = dir_tree
        .get_entry("/test/gen_test_file")
        .expect("Entry should exist after create_entry");
    let created_gen = stored_after_create.generation;
    assert!(
        created_gen > 0,
        "Generation after create should be > 0, got {}",
        created_gen
    );

    // Update the entry
    let mut updated_entry = entry.clone();
    if let Some(attrs) = &mut updated_entry.attributes {
        attrs.ino = inode;
    }
    updated_entry.generation = 0; // update_entry will assign a new generation
    dir_tree
        .update_entry(updated_entry, "test_client", 0, false)
        .expect("update_entry failed");

    let stored_after_update = dir_tree
        .get_entry("/test/gen_test_file")
        .expect("Entry should exist after update_entry");
    let updated_gen = stored_after_update.generation;
    assert!(
        updated_gen > created_gen,
        "Generation must increase after update: created={}, updated={}",
        created_gen,
        updated_gen
    );

    // Collect CREATE + UPDATE notifications from the stream
    let mut stream = stream;
    let mut generations: Vec<(EventType, u64)> = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);

    while tokio::time::Instant::now() < deadline && generations.len() < 2 {
        match tokio::time::timeout(std::time::Duration::from_secs(1), stream.next()).await {
            Ok(Some(Ok(notif))) => {
                if notif.path == "/test/gen_test_file" {
                    let et = EventType::try_from(notif.event_type)
                        .expect("Invalid event_type in notification");
                    generations.push((et, notif.generation));
                }
            }
            _ => break,
        }
    }

    assert!(
        generations.len() >= 2,
        "Should receive at least 2 notifications (CREATE + UPDATE), got {}",
        generations.len()
    );

    assert_eq!(
        generations[0].0,
        EventType::Create,
        "First notification should be CREATE"
    );
    assert_eq!(
        generations[1].0,
        EventType::Update,
        "Second notification should be UPDATE"
    );
    assert!(
        generations[1].1 > generations[0].1,
        "UPDATE generation ({}) must be > CREATE generation ({})",
        generations[1].1,
        generations[0].1
    );
    assert_eq!(
        generations[0].1, created_gen,
        "CREATE notification generation must match stored entry generation"
    );
    assert_eq!(
        generations[1].1, updated_gen,
        "UPDATE notification generation must match stored entry generation"
    );

    // Delete the entry (publishes DELETE)
    let ino = dir_tree
        .get_entry("/test/gen_test_file")
        .unwrap()
        .attributes
        .unwrap()
        .ino;
    let deleted = dir_tree
        .delete_entry(ino, "test_client")
        .expect("delete_entry failed");
    assert!(deleted, "delete_entry should report the entry was deleted");

    let mut got_delete = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_secs(1), stream.next()).await {
            Ok(Some(Ok(notif))) => {
                if notif.path == "/test/gen_test_file"
                    && notif.event_type == EventType::Delete as i32
                {
                    got_delete = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(
        got_delete,
        "Should receive DELETE notification for deleted entry"
    );
}
