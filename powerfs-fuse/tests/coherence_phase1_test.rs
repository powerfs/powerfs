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
fn test_invalidate_path_removes_entry() {
    let cache = MetadataCache::new();

    let entry = make_entry(100, 1, "test.txt", false);
    cache.insert(entry);

    assert!(cache.get_inode(100).is_some());
    assert!(cache.get_path("/test.txt").is_some());

    cache.invalidate_path("/test.txt");

    assert!(cache.get_inode(100).is_none());
    assert!(cache.get_path("/test.txt").is_none());
}

#[test]
fn test_invalidate_path_invalidates_parent_dir_listing() {
    let cache = MetadataCache::new();

    let parent = make_entry(10, 1, "dir", true);
    cache.insert(parent);

    let child = make_entry(100, 10, "file.txt", false);
    cache.insert(child);

    let listing = vec![(100u64, "file.txt".to_string(), false)];
    cache.set_dir_listing(10, listing);
    assert!(cache.get_dir_listing(10).is_some());

    cache.invalidate_path("/dir/file.txt");

    assert!(cache.get_dir_listing(10).is_none());
    assert!(cache.get_inode(10).is_some());
}

#[test]
fn test_invalidate_nonexistent_path_no_op() {
    let cache = MetadataCache::new();
    let entry = make_entry(100, 1, "exists.txt", false);
    cache.insert(entry);

    cache.invalidate_path("/nonexistent.txt");

    assert!(cache.get_inode(100).is_some());
}

#[test]
fn test_invalidate_root_child() {
    let cache = MetadataCache::new();

    let entry = make_entry(200, 1, "root_file.txt", false);
    cache.insert(entry);

    let listing = vec![];
    cache.set_dir_listing(1, listing);

    cache.invalidate_path("/root_file.txt");

    assert!(cache.get_inode(200).is_none());
    assert!(cache.get_dir_listing(1).is_none());
}

#[test]
fn test_invalidate_deep_nested_path() {
    let cache = MetadataCache::new();

    let d1 = make_entry(10, 1, "a", true);
    let d2 = make_entry(20, 10, "b", true);
    let d3 = make_entry(30, 20, "c", true);
    let file = make_entry(100, 30, "deep.txt", false);

    cache.insert(d1);
    cache.insert(d2);
    cache.insert(d3);
    cache.insert(file);

    let listing = vec![(100u64, "deep.txt".to_string(), false)];
    cache.set_dir_listing(30, listing);

    cache.invalidate_path("/a/b/c/deep.txt");

    assert!(cache.get_inode(100).is_none());
    assert!(cache.get_dir_listing(30).is_none());
    assert!(cache.get_inode(30).is_some());
    assert!(cache.get_inode(20).is_some());
    assert!(cache.get_inode(10).is_some());
}

#[test]
fn test_generation_field_stored_and_retrieved() {
    let cache = MetadataCache::new();

    let mut entry = make_entry(100, 1, "gen_test.txt", false);
    entry.generation = 42;
    cache.insert(entry);

    let cached = cache.get_inode(100).unwrap();
    assert_eq!(cached.generation, 42);
}

#[test]
fn test_invalidate_directory_itself() {
    let cache = MetadataCache::new();

    let dir = make_entry(50, 1, "mydir", true);
    cache.insert(dir);

    let child = make_entry(51, 50, "inside.txt", false);
    cache.insert(child);

    cache.invalidate_path("/mydir");

    assert!(cache.get_inode(50).is_none());
    assert!(cache.get_path("/mydir").is_none());
}

#[test]
fn test_multiple_invalidations() {
    let cache = MetadataCache::new();

    for i in 0..10 {
        let entry = make_entry(100 + i, 1, &format!("file_{}.txt", i), false);
        cache.insert(entry);
    }

    for i in 0..5 {
        cache.invalidate_path(&format!("/file_{}.txt", i));
    }

    for i in 0..5 {
        assert!(cache.get_inode(100 + i).is_none());
    }
    for i in 5..10 {
        assert!(cache.get_inode(100 + i).is_some());
    }
}

#[test]
fn test_dir_listing_repopulated_after_invalidation() {
    let cache = MetadataCache::new();

    let dir = make_entry(200, 1, "parent", true);
    cache.insert(dir);

    let child = make_entry(201, 200, "child.txt", false);
    cache.insert(child);

    let listing1 = vec![(201u64, "child.txt".to_string(), false)];
    cache.set_dir_listing(200, listing1);
    assert!(cache.get_dir_listing(200).is_some());

    cache.invalidate_path("/parent/child.txt");
    assert!(cache.get_dir_listing(200).is_none());

    let listing2 = vec![
        (201u64, "child.txt".to_string(), false),
        (202u64, "child2.txt".to_string(), false),
    ];
    cache.set_dir_listing(200, listing2);
    assert!(cache.get_dir_listing(200).is_some());
    assert_eq!(cache.get_dir_listing(200).unwrap().len(), 2);
}

#[test]
fn test_lookup_in_cache_after_invalidation() {
    let cache = MetadataCache::new();

    let dir = make_entry(300, 1, "lookup_dir", true);
    cache.insert(dir);

    let child = make_entry(301, 300, "target.txt", false);
    cache.insert(child);

    assert!(cache.lookup_in_cache(300, "target.txt").is_some());

    cache.invalidate_path("/lookup_dir/target.txt");

    assert!(cache.lookup_in_cache(300, "target.txt").is_none());
}
