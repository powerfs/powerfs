use powerfs_fuse::cache::{CachedEntry, MetadataCache};

#[test]
fn test_cache_insert_and_get() {
    let cache = MetadataCache::new();
    let entry = CachedEntry {
        inode: 1,
        parent: 0,
        name: "test".to_string(),
        is_dir: false,
        is_symlink: false,
        symlink_target: None,
        nlink: 1,
        fid: None,
        size: 100,
        mode: 0o644,
        uid: 0,
        gid: 0,
        atime: 0,
        mtime: 0,
        ctime: 0,
        xattrs: std::collections::HashMap::new(),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        content_size: 100,
        disk_size: 100,
        generation: 0,
    };

    cache.insert(entry.clone());
    let retrieved = cache.get_inode(1).unwrap();

    assert_eq!(retrieved.name, entry.name);
    assert_eq!(retrieved.size, entry.size);
}

#[test]
fn test_cache_remove() {
    let cache = MetadataCache::new();
    let entry = CachedEntry {
        inode: 1,
        parent: 0,
        name: "test".to_string(),
        is_dir: false,
        is_symlink: false,
        symlink_target: None,
        nlink: 1,
        fid: None,
        size: 100,
        mode: 0o644,
        uid: 0,
        gid: 0,
        atime: 0,
        mtime: 0,
        ctime: 0,
        xattrs: std::collections::HashMap::new(),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        content_size: 100,
        disk_size: 100,
        generation: 0,
    };

    cache.insert(entry);
    assert!(cache.get_inode(1).is_some());

    cache.remove(1);
    assert!(cache.get_inode(1).is_none());
}

#[test]
fn test_write_buffer_add_and_take() {
    let buffer = powerfs_fuse::fuser_fs::WriteBuffer::new(4);

    let should_flush = buffer.add(1, 0, &[1, 2, 3]);
    assert!(!should_flush);

    let should_flush = buffer.add(1, 3, &[4, 5, 6]);
    assert!(!should_flush);

    let should_flush = buffer.add(1, 6, &[7, 8, 9]);
    assert!(!should_flush);

    let should_flush = buffer.add(1, 9, &[10, 11, 12]);
    assert!(should_flush);

    let entries = buffer.take(1);
    assert_eq!(entries.len(), 4);
}

#[test]
fn test_write_buffer_multiple_inodes() {
    let buffer = powerfs_fuse::fuser_fs::WriteBuffer::new(4);

    buffer.add(1, 0, &[1, 2, 3]);
    buffer.add(2, 0, &[4, 5, 6]);

    let entries1 = buffer.take(1);
    assert_eq!(entries1.len(), 1);

    let entries2 = buffer.take(2);
    assert_eq!(entries2.len(), 1);
}

#[test]
fn test_chunk_cache_put_get() {
    let chunk_cache = powerfs_fuse::cache::ChunkCache::with_defaults();

    let data = vec![1u8; 1024];
    chunk_cache.put(1, 0, data.clone(), 0, 0);

    let chunk = chunk_cache.get(1, 0);
    assert!(chunk.is_some());
    assert_eq!(chunk.unwrap().data, data);
}

#[test]
fn test_chunk_cache_nonexistent() {
    let chunk_cache = powerfs_fuse::cache::ChunkCache::with_defaults();

    let chunk = chunk_cache.get(1, 0);
    assert!(chunk.is_none());
}

#[test]
fn test_metadata_cache_lookup() {
    let cache = MetadataCache::new();
    let entry = CachedEntry {
        inode: 10,
        parent: 1,
        name: "testfile".to_string(),
        is_dir: false,
        is_symlink: false,
        symlink_target: None,
        nlink: 1,
        fid: None,
        size: 0,
        mode: 0o644,
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
        generation: 0,
    };

    cache.insert(entry);

    let found = cache.lookup_in_cache(1, "testfile");
    assert!(found.is_some());
    assert_eq!(found.unwrap().name, "testfile");

    let not_found = cache.lookup_in_cache(1, "nonexistent");
    assert!(not_found.is_none());
}

#[test]
fn test_metadata_cache_list_children() {
    let cache = MetadataCache::new();

    let entry1 = CachedEntry {
        inode: 10,
        parent: 1,
        name: "file1".to_string(),
        is_dir: false,
        is_symlink: false,
        symlink_target: None,
        nlink: 1,
        fid: None,
        size: 0,
        mode: 0o644,
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
        generation: 0,
    };
    let entry2 = CachedEntry {
        inode: 11,
        parent: 1,
        name: "file2".to_string(),
        is_dir: false,
        is_symlink: false,
        symlink_target: None,
        nlink: 1,
        fid: None,
        size: 0,
        mode: 0o644,
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
        generation: 0,
    };

    cache.insert(entry1);
    cache.insert(entry2);

    let children = cache.list_children(1);
    assert_eq!(children.len(), 2);
}

#[test]
fn test_metadata_cache_update_size() {
    let cache = MetadataCache::new();
    let entry = CachedEntry {
        inode: 1,
        parent: 0,
        name: "test".to_string(),
        is_dir: false,
        is_symlink: false,
        symlink_target: None,
        nlink: 1,
        fid: None,
        size: 100,
        mode: 0o644,
        uid: 0,
        gid: 0,
        atime: 0,
        mtime: 0,
        ctime: 0,
        xattrs: std::collections::HashMap::new(),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        content_size: 100,
        disk_size: 100,
        generation: 0,
    };

    cache.insert(entry);

    cache.update_size(1, 200);
    let updated = cache.get_inode(1).unwrap();
    assert_eq!(updated.size, 200);
}

#[test]
fn test_metadata_cache_update_attr() {
    let cache = MetadataCache::new();
    let entry = CachedEntry {
        inode: 1,
        parent: 0,
        name: "test".to_string(),
        is_dir: false,
        is_symlink: false,
        symlink_target: None,
        nlink: 1,
        fid: None,
        size: 100,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        atime: 100,
        mtime: 200,
        ctime: 300,
        xattrs: std::collections::HashMap::new(),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        content_size: 100,
        disk_size: 100,
        generation: 0,
    };

    cache.insert(entry);

    cache.update_attr(
        1,
        powerfs_fuse::cache::UpdateAttrParams {
            mode: Some(0o755),
            size: Some(200),
            uid: Some(2000),
            gid: Some(2000),
            atime: Some(1000),
            mtime: Some(2000),
        },
    );

    let updated = cache.get_inode(1).unwrap();
    assert_eq!(updated.mode, 0o755);
    assert_eq!(updated.size, 200);
    assert_eq!(updated.uid, 2000);
    assert_eq!(updated.gid, 2000);
    assert_eq!(updated.atime, 1000);
    assert_eq!(updated.mtime, 2000);
}
