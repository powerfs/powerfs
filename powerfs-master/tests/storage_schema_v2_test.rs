use powerfs_master::directory_tree::DirectoryTree;
use powerfs_master::proto::{Entry, FuseAttributes};
use std::collections::HashMap;
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
            size: 1024,
            blksize: 4096,
            blocks: 1,
            atime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            mtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            ctime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            crtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
            perm: mode & 0o777,
        }),
        chunks: vec![],
        hard_link_id: "".to_string(),
        hard_link_counter: 0,
        extended: HashMap::new(),
        content_size: 1024,
        disk_size: 1024,
        ttl: "".to_string(),
        symlink_target: "".to_string(),
        owner: String::new(),
        generation: 0,
    }
}

fn setup_tree() -> (DirectoryTree, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let tree = DirectoryTree::new(temp_dir.path()).unwrap();
    tree.init_root().unwrap();
    (tree, temp_dir)
}

#[test]
fn test_storage_schema_v2_basic_operations() {
    let (tree, _temp_dir) = setup_tree();

    tree.create_directory("/test").unwrap();
    assert!(tree.get_entry("/test").is_some());

    let file_entry = create_test_entry("file.txt", "/test", 0o100644);
    tree.create_entry(file_entry, "test_client").unwrap();

    assert!(tree.get_entry("/test/file.txt").is_some());
    assert!(tree.lookup(2, "file.txt").is_some());

    let entries = tree.list_entries(2, 10, "");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "file.txt");

    tree.rename_entry(2, "file.txt", 2, "renamed.txt", "test_client")
        .unwrap();

    assert!(tree.get_entry("/test/file.txt").is_none());
    assert!(tree.get_entry("/test/renamed.txt").is_some());

    tree.delete_entry_by_path("/test/renamed.txt", "test_client")
        .unwrap();

    assert!(tree.get_entry("/test/renamed.txt").is_none());
    let entries_after_delete = tree.list_entries(2, 10, "");
    assert_eq!(entries_after_delete.len(), 0);
}

#[test]
fn test_storage_schema_v2_nested_directories() {
    let (tree, _temp_dir) = setup_tree();

    tree.create_directory("/a/b/c").unwrap();

    assert!(tree.get_entry("/a").is_some());
    assert!(tree.get_entry("/a/b").is_some());
    assert!(tree.get_entry("/a/b/c").is_some());

    let a_ino = tree.get_entry("/a").unwrap().attributes.unwrap().ino;
    let b_ino = tree.get_entry("/a/b").unwrap().attributes.unwrap().ino;
    let _c_ino = tree.get_entry("/a/b/c").unwrap().attributes.unwrap().ino;

    assert!(tree.lookup(1, "a").is_some());
    assert!(tree.lookup(a_ino, "b").is_some());
    assert!(tree.lookup(b_ino, "c").is_some());

    let entries_a = tree.list_entries(a_ino, 10, "");
    assert_eq!(entries_a.len(), 1);
    assert_eq!(entries_a[0].name, "b");

    let entries_b = tree.list_entries(b_ino, 10, "");
    assert_eq!(entries_b.len(), 1);
    assert_eq!(entries_b[0].name, "c");

    tree.delete_entry_by_path("/a", "test_client").unwrap();

    assert!(tree.get_entry("/a").is_none());
    assert!(tree.get_entry("/a/b").is_none());
    assert!(tree.get_entry("/a/b/c").is_none());
}

#[test]
fn test_storage_schema_v2_path_index_consistency() {
    let (tree, _temp_dir) = setup_tree();

    let file_entry = create_test_entry("testfile.txt", "/", 0o100644);
    let ino = tree.create_entry(file_entry, "test_client").unwrap();

    let entry_by_path = tree.get_entry("/testfile.txt");
    assert!(entry_by_path.is_some());
    assert_eq!(entry_by_path.unwrap().attributes.unwrap().ino, ino);

    let entry_by_inode = tree.get_entry_by_inode(ino);
    assert!(entry_by_inode.is_some());
    assert_eq!(entry_by_inode.unwrap().0.name, "testfile.txt");

    let entry_by_inode = tree.get_entry_by_inode(ino).unwrap();
    assert_eq!(entry_by_inode.1, "/testfile.txt");
}

#[test]
fn test_storage_schema_v2_root_inode() {
    let (tree, _temp_dir) = setup_tree();

    let root_entry = tree.get_entry("/");
    assert!(root_entry.is_some());
    assert_eq!(root_entry.unwrap().attributes.unwrap().ino, 1);

    let root_by_inode = tree.get_entry_by_inode(1);
    assert!(root_by_inode.is_some());

    let root_entry = tree.get_entry_by_inode(1).unwrap();
    assert_eq!(root_entry.1, "/");
}

#[test]
fn test_storage_schema_v2_create_directory_idempotent() {
    let (tree, _temp_dir) = setup_tree();

    let ino1 = tree.create_directory("/idempotent").unwrap();
    let ino2 = tree.create_directory("/idempotent").unwrap();

    assert_eq!(ino1, ino2);

    let entries = tree.list_entries(1, 10, "");
    assert_eq!(entries.len(), 1);
}

#[test]
fn test_storage_schema_v2_delete_nonexistent() {
    let (tree, _temp_dir) = setup_tree();

    let result = tree.delete_entry_by_path("/nonexistent", "test_client");
    assert!(result.is_ok());
    assert!(!result.unwrap());

    let result = tree.delete_entry(99999, "test_client");
    assert!(result.is_ok());
    assert!(!result.unwrap());
}
