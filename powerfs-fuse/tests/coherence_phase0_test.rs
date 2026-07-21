use powerfs_fuse::cache::{CachedEntry, MetadataCache};

fn make_entry(inode: u64, parent: u64, name: &str, is_dir: bool) -> CachedEntry {
    CachedEntry {
        inode,
        parent,
        name: name.to_string(),
        is_dir,
        is_symlink: false,
        symlink_target: None,
        nlink: if is_dir { 2 } else { 1 },
        fid: None,
        size: 0,
        mode: if is_dir { 0o40755 } else { 0o100644 },
        uid: 0,
        gid: 0,
        atime: 0,
        mtime: 0,
        ctime: 0,
        xattrs: std::collections::HashMap::new(),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        content_size: 0,
        disk_size: 0,
        generation: 1,
    }
}

#[test]
fn test_cache_initialization() {
    let cache = MetadataCache::new();
    assert!(cache.get_inode(100).is_none());
    assert!(cache.get_path("/test.txt").is_none());
}

#[test]
fn test_insert_and_get_by_inode() {
    let cache = MetadataCache::new();

    let entry = make_entry(100, 1, "test.txt", false);
    cache.insert(entry);

    let retrieved = cache.get_inode(100);
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.inode, 100);
    assert_eq!(retrieved.name, "test.txt");
}

#[test]
fn test_insert_and_get_by_path() {
    let cache = MetadataCache::new();

    let entry = make_entry(100, 1, "test.txt", false);
    cache.insert(entry);

    let inode = cache.get_path("/test.txt");
    assert!(inode.is_some());
    assert_eq!(inode.unwrap(), 100);
}

#[test]
fn test_insert_directory() {
    let cache = MetadataCache::new();

    let entry = make_entry(10, 1, "dir", true);
    cache.insert(entry);

    let retrieved = cache.get_inode(10);
    assert!(retrieved.is_some());
    assert!(retrieved.unwrap().is_dir);
}

#[test]
fn test_overwrite_existing_entry() {
    let cache = MetadataCache::new();

    let entry1 = make_entry(100, 1, "test.txt", false);
    cache.insert(entry1);

    let entry2 = make_entry(100, 1, "renamed.txt", false);
    cache.insert(entry2);

    let retrieved = cache.get_inode(100);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().name, "renamed.txt");
}

#[test]
fn test_get_path_nonexistent() {
    let cache = MetadataCache::new();

    let entry = make_entry(100, 1, "test.txt", false);
    cache.insert(entry);

    assert!(cache.get_path("/nonexistent.txt").is_none());
}

#[test]
fn test_root_directory_exists() {
    let cache = MetadataCache::new();

    let root = cache.get_inode(1);
    assert!(root.is_some());
    assert!(root.unwrap().is_dir);
}

#[test]
fn test_allocate_inode() {
    let cache = MetadataCache::new();

    let inode1 = cache.allocate_inode();
    let inode2 = cache.allocate_inode();
    let inode3 = cache.allocate_inode();

    assert_eq!(inode1, 2);
    assert_eq!(inode2, 3);
    assert_eq!(inode3, 4);
}

#[test]
fn test_cache_preserves_dir_attributes() {
    let cache = MetadataCache::new();

    let entry = make_entry(10, 1, "dir", true);
    cache.insert(entry);

    let retrieved = cache.get_inode(10).unwrap();
    assert!(retrieved.is_dir);
    assert_eq!(retrieved.nlink, 2);
    assert_eq!(retrieved.mode, 0o40755);
}

#[test]
fn test_cache_preserves_file_attributes() {
    let cache = MetadataCache::new();

    let entry = make_entry(100, 1, "file.txt", false);
    cache.insert(entry);

    let retrieved = cache.get_inode(100).unwrap();
    assert!(!retrieved.is_dir);
    assert_eq!(retrieved.nlink, 1);
    assert_eq!(retrieved.mode, 0o100644);
}

#[test]
fn test_get_path_by_parent_chain() {
    let cache = MetadataCache::new();

    let dir_entry = make_entry(10, 1, "mydir", true);
    cache.insert(dir_entry);

    let file_entry = make_entry(100, 10, "myfile.txt", false);
    cache.insert(file_entry);

    let path = cache.get_path_by_parent_chain(100);
    assert!(path.is_some());
    assert_eq!(path.unwrap(), "/mydir/myfile.txt");
}

#[test]
fn test_get_path_by_parent_chain_root() {
    let cache = MetadataCache::new();

    let path = cache.get_path_by_parent_chain(1);
    assert!(path.is_some());
    assert_eq!(path.unwrap(), "/");
}