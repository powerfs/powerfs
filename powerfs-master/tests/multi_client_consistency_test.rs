use powerfs_master::directory_tree::DirectoryTree;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

fn create_test_dir_tree() -> (DirectoryTree, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let tree = DirectoryTree::new(temp_dir.path()).expect("Failed to create DirectoryTree");
    (tree, temp_dir)
}

#[test]
fn test_single_client_lease_acquisition() {
    let (tree, _temp) = create_test_dir_tree();

    let lease_id = tree.acquire_lease("/test/file.txt", "client-1", 60000);

    assert!(!lease_id.is_empty(), "Lease ID should not be empty");
    assert!(
        tree.has_active_lease("/test/file.txt"),
        "Should have active lease"
    );
}

#[test]
fn test_second_client_cannot_acquire_existing_lease() {
    let (tree, _temp) = create_test_dir_tree();

    let lease_id1 = tree.acquire_lease("/test/file.txt", "client-1", 60000);
    assert!(!lease_id1.is_empty(), "First client should acquire lease");

    let lease_id2 = tree.acquire_lease("/test/file.txt", "client-2", 60000);
    assert!(
        lease_id2.is_empty(),
        "Second client should NOT acquire lease"
    );
}

#[test]
fn test_second_client_can_acquire_after_first_releases() {
    let (tree, _temp) = create_test_dir_tree();

    let lease_id1 = tree.acquire_lease("/test/file.txt", "client-1", 60000);
    assert!(!lease_id1.is_empty(), "First client should acquire lease");

    tree.release_lease(&lease_id1);

    let lease_id2 = tree.acquire_lease("/test/file.txt", "client-2", 60000);
    assert!(
        !lease_id2.is_empty(),
        "Second client should acquire lease after first releases"
    );
}

#[test]
fn test_concurrent_lease_acquisition_single_winner() {
    let (tree, _temp) = create_test_dir_tree();
    let tree = Arc::new(tree);

    let barrier = Arc::new(Barrier::new(2));
    let mut results = vec![];

    for client_id in ["client-1", "client-2"].iter() {
        let tree_clone = tree.clone();
        let barrier_clone = barrier.clone();
        let client_id_clone = client_id.to_string();

        let handle = thread::spawn(move || {
            barrier_clone.wait();
            let lease_id = tree_clone.acquire_lease("/test/file.txt", &client_id_clone, 60000);
            !lease_id.is_empty()
        });

        results.push(handle);
    }

    let success_count = results
        .into_iter()
        .filter_map(|h| h.join().ok())
        .filter(|&s| s)
        .count();

    assert_eq!(
        success_count, 1,
        "Exactly one client should acquire lease in concurrent scenario"
    );
}

#[test]
fn test_renew_lease_extends_expiration() {
    let (tree, _temp) = create_test_dir_tree();

    let lease_id = tree.acquire_lease("/test/file.txt", "client-1", 1000);
    assert!(!lease_id.is_empty(), "Should acquire lease");

    let epoch = tree.renew_lease(&lease_id, 1000);
    assert!(epoch.is_some(), "Lease renew should succeed");

    assert!(
        tree.has_active_lease("/test/file.txt"),
        "Lease should still be active after renew"
    );
}

#[test]
fn test_renew_lease_fails_for_released_lease() {
    let (tree, _temp) = create_test_dir_tree();

    let lease_id = tree.acquire_lease("/test/file.txt", "client-1", 60000);
    assert!(!lease_id.is_empty(), "Should acquire lease");

    tree.release_lease(&lease_id);

    let epoch = tree.renew_lease(&lease_id, 60000);
    assert!(epoch.is_none(), "Renew should fail for released lease");
}

#[test]
fn test_lease_expires_after_timeout() {
    let (tree, _temp) = create_test_dir_tree();

    let lease_id = tree.acquire_lease("/test/file.txt", "client-1", 50);
    assert!(!lease_id.is_empty(), "Should acquire lease");

    assert!(
        tree.has_active_lease("/test/file.txt"),
        "Lease should be active immediately after acquire"
    );

    thread::sleep(std::time::Duration::from_millis(100));

    assert!(
        !tree.has_active_lease("/test/file.txt"),
        "Lease should expire after timeout"
    );
}

#[test]
fn test_client_can_acquire_expired_lease() {
    let (tree, _temp) = create_test_dir_tree();

    let lease_id = tree.acquire_lease("/test/file.txt", "client-1", 50);
    assert!(!lease_id.is_empty(), "First client should acquire lease");

    thread::sleep(std::time::Duration::from_millis(100));

    let new_lease_id = tree.acquire_lease("/test/file.txt", "client-2", 60000);
    assert!(
        !new_lease_id.is_empty(),
        "Second client should acquire expired lease"
    );
    assert_ne!(lease_id, new_lease_id, "New lease should have different ID");
}

#[test]
fn test_multiple_files_independent_leases() {
    let (tree, _temp) = create_test_dir_tree();

    let lease_id1 = tree.acquire_lease("/test/file1.txt", "client-1", 60000);
    let lease_id2 = tree.acquire_lease("/test/file2.txt", "client-2", 60000);

    assert!(
        !lease_id1.is_empty(),
        "Client-1 should acquire lease for file1"
    );
    assert!(
        !lease_id2.is_empty(),
        "Client-2 should acquire lease for file2"
    );

    assert!(tree.has_active_lease("/test/file1.txt"));
    assert!(tree.has_active_lease("/test/file2.txt"));

    let lease_id3 = tree.acquire_lease("/test/file1.txt", "client-2", 60000);
    assert!(
        lease_id3.is_empty(),
        "Client-2 should NOT acquire lease for file1"
    );
}

#[test]
fn test_lease_release_allows_new_acquisition() {
    let (tree, _temp) = create_test_dir_tree();

    let lease_id1 = tree.acquire_lease("/test/file.txt", "client-1", 60000);
    assert!(!lease_id1.is_empty());

    tree.release_lease(&lease_id1);

    let lease_id2 = tree.acquire_lease("/test/file.txt", "client-1", 60000);
    assert!(
        !lease_id2.is_empty(),
        "Same client should acquire lease after releasing"
    );
    assert_ne!(lease_id1, lease_id2, "New lease should have different ID");
}
