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
fn test_filer_lookup() {
    let (tree, _temp_dir) = setup_tree();

    let file_entry = create_test_entry("test.txt", "/", 0o100644);
    tree.create_entry(file_entry, "test_client").unwrap();

    let found = tree.lookup(1, "test.txt");
    assert!(found.is_some());
    assert_eq!(found.unwrap().name, "test.txt");

    let not_found = tree.lookup(1, "nonexistent.txt");
    assert!(not_found.is_none());
}

#[test]
fn test_filer_get_entry() {
    let (tree, _temp_dir) = setup_tree();

    let file_entry = create_test_entry("get_test.txt", "/", 0o100644);
    tree.create_entry(file_entry, "test_client").unwrap();

    let found = tree.get_entry("/get_test.txt");
    assert!(found.is_some());
    assert_eq!(found.unwrap().name, "get_test.txt");

    let not_found = tree.get_entry("/nonexistent.txt");
    assert!(not_found.is_none());
}

#[test]
fn test_filer_create_entry() {
    let (tree, _temp_dir) = setup_tree();

    let file_entry = create_test_entry("create_test.txt", "/", 0o100644);
    let inode = tree.create_entry(file_entry, "test_client").unwrap();

    assert!(inode > 0);

    let found = tree.lookup(1, "create_test.txt");
    assert!(found.is_some());
    assert_eq!(found.unwrap().attributes.unwrap().ino, inode);

    let dir_entry = create_test_entry("subdir", "/", 0o040755);
    let dir_inode = tree.create_entry(dir_entry, "test_client").unwrap();
    assert!(dir_inode > inode);
}

#[test]
fn test_filer_update_entry() {
    let (tree, _temp_dir) = setup_tree();

    let mut file_entry = create_test_entry("update_test.txt", "/", 0o100644);
    tree.create_entry(file_entry.clone(), "test_client")
        .unwrap();

    if let Some(attrs) = &mut file_entry.attributes {
        attrs.size = 1024;
        attrs.mode = 0o100755;
    }
    file_entry.content_size = 1024;

    let result = tree.update_entry(file_entry, "test_client");
    assert!(result.is_ok());

    let found = tree.lookup(1, "update_test.txt");
    assert!(found.is_some());
    let entry = found.unwrap();
    let attrs = entry.attributes.unwrap();
    assert_eq!(attrs.size, 1024);
    assert_eq!(attrs.mode, 0o100755);
}

#[test]
fn test_filer_delete_entry() {
    let (tree, _temp_dir) = setup_tree();

    let file_entry = create_test_entry("delete_test.txt", "/", 0o100644);
    tree.create_entry(file_entry, "test_client").unwrap();

    let found = tree.lookup(1, "delete_test.txt");
    assert!(found.is_some());
    let ino = found.unwrap().attributes.unwrap().ino;

    let result = tree.delete_entry(ino, "test_client");
    assert!(result.is_ok());
    assert!(result.unwrap());

    let found = tree.lookup(1, "delete_test.txt");
    assert!(found.is_none());

    let result = tree.delete_entry(99999, "test_client");
    assert!(result.is_ok());
    assert!(!result.unwrap());
}

#[test]
fn test_filer_list_entries() {
    let (tree, _temp_dir) = setup_tree();

    for i in 0..5 {
        let file_entry = create_test_entry(&format!("file{}.txt", i), "/", 0o100644);
        tree.create_entry(file_entry, "test_client").unwrap();
    }

    let entries = tree.list_entries(1, 3, "");
    assert_eq!(entries.len(), 3);

    let entries = tree.list_entries(1, 10, "");
    assert_eq!(entries.len(), 5);

    let subdir = create_test_entry("subdir", "/", 0o040755);
    tree.create_entry(subdir, "test_client").unwrap();

    let entries = tree.list_entries(1, 10, "");
    assert_eq!(entries.len(), 6);
}

#[test]
fn test_filer_stream_mutate() {
    let (tree, _temp_dir) = setup_tree();

    let entries = vec![
        create_test_entry("stream1.txt", "/", 0o100644),
        create_test_entry("stream2.txt", "/", 0o100644),
        create_test_entry("stream3.txt", "/", 0o100644),
    ];

    for entry in entries {
        tree.create_entry(entry, "test_client").unwrap();
    }

    let found1 = tree.lookup(1, "stream1.txt");
    let found2 = tree.lookup(1, "stream2.txt");
    let found3 = tree.lookup(1, "stream3.txt");

    assert!(found1.is_some());
    assert!(found2.is_some());
    assert!(found3.is_some());
}

#[test]
fn test_filer_subscribe_metadata() {
    let (tree, _temp_dir) = setup_tree();

    let mut rx = tree.subscribe();

    let file_entry = create_test_entry("subscribe_test.txt", "/", 0o100644);
    tree.create_entry(file_entry, "test_client").unwrap();

    let notification = rx.try_recv();
    assert!(notification.is_ok());
    let notification = notification.unwrap();
    assert_eq!(
        notification.event_type,
        powerfs_master::proto::powerfs::metadata_notification::EventType::Create as i32
    );
    assert!(notification.path.contains("subscribe_test.txt"));
}
