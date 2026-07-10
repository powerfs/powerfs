use powerfs_master::directory_tree::DirectoryTree;
use powerfs_master::proto::{Entry, FuseAttributes};
use tempfile::TempDir;

fn create_test_entry(name: &str, directory: &str, mode: u32) -> Entry {
    Entry {
        name: name.to_string(),
        directory: directory.to_string(),
        attributes: Some(FuseAttributes {
            ino: 0,
            mode,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            size: 0,
            blksize: 4096,
            blocks: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            crtime: 0,
            perm: 0,
        }),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        extended: std::collections::HashMap::new(),
        content_size: 0,
        disk_size: 0,
        generation: 0,
        ttl: String::new(),
        symlink_target: String::new(),
        owner: String::new(),
    }
}

fn setup_tree() -> (DirectoryTree, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let tree = DirectoryTree::new(temp_dir.path()).unwrap();
    tree.init_root().unwrap();
    (tree, temp_dir)
}

#[test]
fn test_acquire_lease_returns_id() {
    let (tree, _td) = setup_tree();

    let entry = create_test_entry("lease_test.txt", "/", 0o100644);
    tree.create_entry(entry).unwrap();

    let lease_id = tree.acquire_lease("/lease_test.txt", "client-1", 60000);
    assert!(!lease_id.is_empty());
}

#[test]
fn test_has_active_lease_after_acquire() {
    let (tree, _td) = setup_tree();

    let entry = create_test_entry("active.txt", "/", 0o100644);
    tree.create_entry(entry).unwrap();

    assert!(!tree.has_active_lease("/active.txt"));

    tree.acquire_lease("/active.txt", "client-1", 60000);

    assert!(tree.has_active_lease("/active.txt"));
}

#[test]
fn test_release_lease_removes_lease() {
    let (tree, _td) = setup_tree();

    let entry = create_test_entry("release_test.txt", "/", 0o100644);
    tree.create_entry(entry).unwrap();

    let lease_id = tree.acquire_lease("/release_test.txt", "client-1", 60000);
    assert!(tree.has_active_lease("/release_test.txt"));

    let released = tree.release_lease(&lease_id);
    assert!(released);

    assert!(!tree.has_active_lease("/release_test.txt"));
}

#[test]
fn test_release_nonexistent_lease_returns_false() {
    let (tree, _td) = setup_tree();

    let result = tree.release_lease("nonexistent-lease-id");
    assert!(!result);
}

#[test]
fn test_multiple_leases_on_same_path() {
    let (tree, _td) = setup_tree();

    let entry = create_test_entry("multi.txt", "/", 0o100644);
    tree.create_entry(entry).unwrap();

    let lease1 = tree.acquire_lease("/multi.txt", "client-1", 60000);
    let lease2 = tree.acquire_lease("/multi.txt", "client-2", 60000);

    assert_ne!(lease1, lease2);
    assert!(tree.has_active_lease("/multi.txt"));

    tree.release_lease(&lease1);
    assert!(tree.has_active_lease("/multi.txt"));

    tree.release_lease(&lease2);
    assert!(!tree.has_active_lease("/multi.txt"));
}

#[test]
fn test_has_active_lease_on_nonexistent_path() {
    let (tree, _td) = setup_tree();

    assert!(!tree.has_active_lease("/no/such/path.txt"));
}

#[test]
fn test_lease_expires_cleanup() {
    let (tree, _td) = setup_tree();

    let entry = create_test_entry("expire.txt", "/", 0o100644);
    tree.create_entry(entry).unwrap();

    tree.acquire_lease("/expire.txt", "client-1", 1);

    std::thread::sleep(std::time::Duration::from_millis(50));

    tree.cleanup_expired_leases();

    assert!(!tree.has_active_lease("/expire.txt"));
}

#[test]
fn test_opportunistic_cleanup_on_acquire() {
    let (tree, _td) = setup_tree();

    let entry = create_test_entry("opportune.txt", "/", 0o100644);
    tree.create_entry(entry).unwrap();

    tree.acquire_lease("/opportune.txt", "client-old", 1);
    std::thread::sleep(std::time::Duration::from_millis(50));

    tree.acquire_lease("/opportune.txt", "client-new", 60000);

    assert!(tree.has_active_lease("/opportune.txt"));
}

#[test]
fn test_lease_independent_per_path() {
    let (tree, _td) = setup_tree();

    let entry1 = create_test_entry("a.txt", "/", 0o100644);
    let entry2 = create_test_entry("b.txt", "/", 0o100644);
    tree.create_entry(entry1).unwrap();
    tree.create_entry(entry2).unwrap();

    let lease_a = tree.acquire_lease("/a.txt", "client-1", 60000);

    assert!(tree.has_active_lease("/a.txt"));
    assert!(!tree.has_active_lease("/b.txt"));

    tree.release_lease(&lease_a);

    assert!(!tree.has_active_lease("/a.txt"));
    assert!(!tree.has_active_lease("/b.txt"));
}

#[test]
fn test_release_one_lease_does_not_affect_others() {
    let (tree, _td) = setup_tree();

    let entry = create_test_entry("shared.txt", "/", 0o100644);
    tree.create_entry(entry).unwrap();

    let lease1 = tree.acquire_lease("/shared.txt", "client-a", 60000);
    let lease2 = tree.acquire_lease("/shared.txt", "client-b", 60000);
    let lease3 = tree.acquire_lease("/shared.txt", "client-c", 60000);

    tree.release_lease(&lease2);
    assert!(tree.has_active_lease("/shared.txt"));

    tree.release_lease(&lease1);
    tree.release_lease(&lease3);
    assert!(!tree.has_active_lease("/shared.txt"));
}

#[test]
fn test_notification_always_published_even_with_lease() {
    let (tree, _td) = setup_tree();

    tree.add_subscriber("/");
    let mut rx = tree.subscribe();

    let entry = create_test_entry("notify1.txt", "/", 0o100644);
    tree.create_entry(entry).unwrap();

    tree.acquire_lease("/notify1.txt", "client-1", 60000);

    let entry2 = create_test_entry("notify2.txt", "/", 0o100644);
    tree.create_entry(entry2).unwrap();

    let mut count = 0;
    loop {
        match rx.try_recv() {
            Ok(_) => count += 1,
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => break,
        }
    }
    assert!(
        count >= 2,
        "Expected at least 2 notifications, got {}",
        count
    );
}

#[test]
fn test_cleanup_expired_leases_multiple() {
    let (tree, _td) = setup_tree();

    for i in 0..5 {
        let name = format!("expire_{}.txt", i);
        let entry = create_test_entry(&name, "/", 0o100644);
        tree.create_entry(entry).unwrap();
        tree.acquire_lease(&format!("/{}", name), &format!("client-{}", i), 1);
    }

    std::thread::sleep(std::time::Duration::from_millis(50));

    for i in 0..5 {
        let name = format!("stay_{}.txt", i);
        let entry = create_test_entry(&name, "/", 0o100644);
        tree.create_entry(entry).unwrap();
        tree.acquire_lease(&format!("/{}", name), &format!("client-s{}", i), 60000);
    }

    tree.cleanup_expired_leases();

    for i in 0..5 {
        assert!(
            !tree.has_active_lease(&format!("/expire_{}.txt", i)),
            "expire_{}.txt should have no active lease",
            i
        );
    }

    for i in 0..5 {
        assert!(
            tree.has_active_lease(&format!("/stay_{}.txt", i)),
            "stay_{}.txt should have active lease",
            i
        );
    }
}
