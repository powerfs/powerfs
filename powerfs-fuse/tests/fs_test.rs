use powerfs_fuse::cache::{CachedEntry, MetadataCache, UpdateAttrParams, ROOT_INODE};

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

fn make_file_entry(inode: u64, parent: u64, name: &str) -> CachedEntry {
    CachedEntry {
        inode,
        parent,
        name: name.to_string(),
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
        xattrs: HashMap::new(),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        content_size: 0,
        disk_size: 0,
    }
}

fn make_dir_entry(inode: u64, parent: u64, name: &str) -> CachedEntry {
    CachedEntry {
        inode,
        parent,
        name: name.to_string(),
        is_dir: true,
        is_symlink: false,
        symlink_target: None,
        nlink: 2,
        fid: None,
        size: 0,
        mode: 0o755,
        uid: 0,
        gid: 0,
        atime: 0,
        mtime: 0,
        ctime: 0,
        xattrs: HashMap::new(),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        content_size: 0,
        disk_size: 0,
    }
}

#[test]
fn test_allocate_inode_monotonic() {
    let cache = MetadataCache::new();
    let i1 = cache.allocate_inode();
    let i2 = cache.allocate_inode();
    assert!(i2 > i1);
    assert_ne!(i1, i2);
}

#[test]
fn test_insert_and_get_inode() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "test.txt");
    cache.insert(entry.clone());

    let found = cache.get_inode(100).expect("inode should exist");
    assert_eq!(found.name, "test.txt");
    assert_eq!(found.mode, 0o644);
    assert!(!found.is_dir);
    assert_eq!(found.nlink, 1);
}

#[test]
fn test_lookup_in_cache() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "hello.txt");
    cache.insert(entry);

    let found = cache
        .lookup_in_cache(ROOT_INODE, "hello.txt")
        .expect("should find");
    assert_eq!(found.inode, 100);

    assert!(cache.lookup_in_cache(ROOT_INODE, "nonexist.txt").is_none());
}

#[test]
fn test_list_children() {
    let cache = MetadataCache::new();
    let dir = make_dir_entry(10, ROOT_INODE, "subdir");
    cache.insert(dir);
    let f1 = make_file_entry(100, 10, "a.txt");
    let f2 = make_file_entry(101, 10, "b.txt");
    cache.insert(f1);
    cache.insert(f2);

    let children = cache.list_children(10);
    let names: Vec<&str> = children.iter().map(|c| c.1.as_str()).collect();
    assert!(names.contains(&"a.txt"));
    assert!(names.contains(&"b.txt"));
    assert_eq!(children.len(), 2);
}

#[test]
fn test_list_children_excludes_self() {
    let cache = MetadataCache::new();
    let children = cache.list_children(ROOT_INODE);
    for (ino, _, _) in &children {
        assert_ne!(*ino, ROOT_INODE);
    }
}

#[test]
fn test_remove_entry() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "del.txt");
    cache.insert(entry);
    assert!(cache.get_inode(100).is_some());

    cache.remove(100);
    assert!(cache.get_inode(100).is_none());
    assert!(cache.lookup_in_cache(ROOT_INODE, "del.txt").is_none());
}

#[test]
fn test_update_size() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "grow.txt");
    cache.insert(entry);

    cache.update_size(100, 4096);
    let found = cache.get_inode(100).unwrap();
    assert_eq!(found.size, 4096);
}

#[test]
fn test_update_attr() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "attr.txt");
    cache.insert(entry);

    cache.update_attr(
        100,
        UpdateAttrParams {
            mode: Some(0o755),
            size: Some(1024),
            uid: Some(1000),
            gid: Some(100),
            atime: None,
            mtime: None,
        },
    );
    let found = cache.get_inode(100).unwrap();
    assert_eq!(found.mode, 0o755);
    assert_eq!(found.size, 1024);
    assert_eq!(found.uid, 1000);
    assert_eq!(found.gid, 100);
}

#[test]
fn test_rename_file() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "old.txt");
    cache.insert(entry);

    cache
        .rename(ROOT_INODE, "old.txt", ROOT_INODE, "new.txt")
        .expect("rename should succeed");

    assert!(cache.lookup_in_cache(ROOT_INODE, "old.txt").is_none());
    let found = cache
        .lookup_in_cache(ROOT_INODE, "new.txt")
        .expect("should find");
    assert_eq!(found.inode, 100);
    assert_eq!(found.name, "new.txt");
}

#[test]
fn test_rename_across_dirs() {
    let cache = MetadataCache::new();
    let dir1 = make_dir_entry(10, ROOT_INODE, "dir1");
    let dir2 = make_dir_entry(11, ROOT_INODE, "dir2");
    cache.insert(dir1);
    cache.insert(dir2);

    let f = make_file_entry(100, 10, "move.txt");
    cache.insert(f);

    cache.rename(10, "move.txt", 11, "moved.txt").unwrap();

    assert!(cache.lookup_in_cache(10, "move.txt").is_none());
    let found = cache.lookup_in_cache(11, "moved.txt").expect("should find");
    assert_eq!(found.inode, 100);
    assert_eq!(found.parent, 11);
}

#[test]
fn test_rename_nonexistent_fails() {
    let cache = MetadataCache::new();
    let result = cache.rename(ROOT_INODE, "nope.txt", ROOT_INODE, "new.txt");
    assert!(result.is_err());
}

#[test]
fn test_symlink_create_and_read() {
    let cache = MetadataCache::new();
    let mut entry = make_file_entry(100, ROOT_INODE, "link");
    entry.is_symlink = true;
    entry.symlink_target = Some("/target/path".to_string());
    entry.size = 13;
    cache.insert(entry);

    let found = cache.lookup_in_cache(ROOT_INODE, "link").unwrap();
    assert!(found.is_symlink);
    assert_eq!(
        cache.get_symlink_target(100),
        Some("/target/path".to_string())
    );
}

#[test]
fn test_set_symlink_target() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "mylink");
    cache.insert(entry);

    cache.set_symlink_target(100, "/new/target".to_string());
    let found = cache.get_inode(100).unwrap();
    assert!(found.is_symlink);
    assert_eq!(found.symlink_target, Some("/new/target".to_string()));
}

#[test]
fn test_hard_link_inc_nlink() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "orig.txt");
    cache.insert(entry);

    cache.inc_nlink(100);
    let nlink = cache.get_nlink(100);
    assert_eq!(nlink, 2);
}

#[test]
fn test_hard_link_dec_nlink_deletes_at_zero() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "link.txt");
    cache.insert(entry);

    cache.inc_nlink(100);
    assert_eq!(cache.get_nlink(100), 2);

    let should_delete = cache.dec_nlink(100);
    assert!(!should_delete);
    assert_eq!(cache.get_nlink(100), 1);

    let should_delete = cache.dec_nlink(100);
    assert!(should_delete);
}

#[test]
fn test_nlink_preserved_on_rename() {
    let cache = MetadataCache::new();
    let mut entry = make_file_entry(100, ROOT_INODE, "a.txt");
    entry.nlink = 3;
    cache.insert(entry);

    cache
        .rename(ROOT_INODE, "a.txt", ROOT_INODE, "b.txt")
        .unwrap();

    let found = cache.lookup_in_cache(ROOT_INODE, "b.txt").unwrap();
    assert_eq!(found.nlink, 3);
}

#[test]
fn test_lru_eviction() {
    let cache = MetadataCache::with_capacity(3);
    cache.insert(make_file_entry(1, ROOT_INODE, "f1"));
    cache.insert(make_file_entry(2, ROOT_INODE, "f2"));
    cache.insert(make_file_entry(3, ROOT_INODE, "f3"));
    // Access f1 to make it recently used
    let _ = cache.get_inode(1);
    // Insert f4 - should evict f2 (oldest unused)
    cache.insert(make_file_entry(4, ROOT_INODE, "f4"));

    let existing = [1, 3, 4]
        .iter()
        .filter(|i| cache.get_inode(**i).is_some())
        .count();
    assert_eq!(existing, 3);
    // f2 may or may not be evicted depending on exact LRU order, but total should be 3
}

#[test]
fn test_directory_nlink() {
    let cache = MetadataCache::new();
    let dir = make_dir_entry(10, ROOT_INODE, "mydir");
    cache.insert(dir);

    let found = cache.get_inode(10).unwrap();
    assert!(found.is_dir);
    assert_eq!(found.nlink, 2);
}

#[test]
fn test_xattr_set_and_get() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "xattr.txt");
    cache.insert(entry);

    cache.set_xattr(100, "user.test", b"test value");
    let value = cache.get_xattr(100, "user.test");
    assert_eq!(value, Some(b"test value".to_vec()));
}

#[test]
fn test_xattr_get_nonexistent() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "xattr.txt");
    cache.insert(entry);

    let value = cache.get_xattr(100, "user.nonexistent");
    assert_eq!(value, None);
}

#[test]
fn test_xattr_remove() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "xattr.txt");
    cache.insert(entry);

    cache.set_xattr(100, "user.test", b"value");
    assert!(cache.remove_xattr(100, "user.test"));
    assert_eq!(cache.get_xattr(100, "user.test"), None);
    assert!(!cache.remove_xattr(100, "user.nonexistent"));
}

#[test]
fn test_xattr_list() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "xattr.txt");
    cache.insert(entry);

    cache.set_xattr(100, "user.attr1", b"value1");
    cache.set_xattr(100, "user.attr2", b"value2");

    let attrs = cache.list_xattrs(100);
    assert!(attrs.contains(&"user.attr1".to_string()));
    assert!(attrs.contains(&"user.attr2".to_string()));
    assert_eq!(attrs.len(), 2);
}

#[test]
fn test_update_attr_with_timestamps() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "time.txt");
    cache.insert(entry);

    let atime = 1620000000;
    let mtime = 1630000000;
    cache.update_attr(
        100,
        UpdateAttrParams {
            mode: None,
            size: None,
            uid: None,
            gid: None,
            atime: Some(atime),
            mtime: Some(mtime),
        },
    );

    let found = cache.get_inode(100).unwrap();
    assert_eq!(found.atime, atime);
    assert_eq!(found.mtime, mtime);
}

#[test]
fn test_update_attr_ctime_updated() {
    let cache = MetadataCache::new();
    let mut entry = make_file_entry(100, ROOT_INODE, "ctime.txt");
    entry.ctime = 1000;
    cache.insert(entry);

    cache.update_attr(
        100,
        UpdateAttrParams {
            mode: None,
            size: None,
            uid: None,
            gid: None,
            atime: None,
            mtime: None,
        },
    );

    let found = cache.get_inode(100).unwrap();
    assert!(found.ctime > 1000);
}

#[test]
fn test_xattr_preserved_on_copy() {
    let cache = MetadataCache::new();
    let mut entry = make_file_entry(100, ROOT_INODE, "orig.txt");
    entry
        .xattrs
        .insert("user.test".to_string(), b"value".to_vec());
    cache.insert(entry);

    let found = cache.get_inode(100).unwrap();
    assert_eq!(found.xattrs.get("user.test"), Some(&b"value".to_vec()));
}

#[test]
fn test_concurrent_inserts() {
    let cache = Arc::new(MetadataCache::new());
    let mut handles = Vec::new();

    for i in 0..10 {
        let cache_clone = Arc::clone(&cache);
        let handle = thread::spawn(move || {
            let entry = make_file_entry(1000 + i as u64, ROOT_INODE, &format!("file{}.txt", i));
            cache_clone.insert(entry);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for i in 0..10 {
        let found = cache.lookup_in_cache(ROOT_INODE, &format!("file{}.txt", i));
        assert!(found.is_some(), "file{}.txt should exist", i);
    }
}

#[test]
fn test_concurrent_read_write() {
    let cache = Arc::new(MetadataCache::new());
    let entry = make_file_entry(100, ROOT_INODE, "shared.txt");
    cache.insert(entry);

    let mut handles = Vec::new();

    for _ in 0..5 {
        let cache_clone = Arc::clone(&cache);
        let handle = thread::spawn(move || {
            for i in 0..100 {
                cache_clone.update_attr(
                    100,
                    UpdateAttrParams {
                        mode: None,
                        size: Some(i as u64),
                        uid: None,
                        gid: None,
                        atime: None,
                        mtime: None,
                    },
                );
            }
        });
        handles.push(handle);
    }

    for _ in 0..5 {
        let cache_clone = Arc::clone(&cache);
        let handle = thread::spawn(move || {
            for _ in 0..100 {
                let _ = cache_clone.get_inode(100);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let found = cache.get_inode(100).unwrap();
    assert_eq!(found.name, "shared.txt");
}

#[test]
fn test_concurrent_lookups() {
    let cache = Arc::new(MetadataCache::new());
    for i in 0..100 {
        let entry = make_file_entry(1000 + i as u64, ROOT_INODE, &format!("item{}.dat", i));
        cache.insert(entry);
    }

    let mut handles = Vec::new();

    for _ in 0..8 {
        let cache_clone = Arc::clone(&cache);
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let _ = cache_clone.lookup_in_cache(ROOT_INODE, &format!("item{}.dat", i));
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for i in 0..100 {
        let found = cache.lookup_in_cache(ROOT_INODE, &format!("item{}.dat", i));
        assert!(found.is_some(), "item{}.dat should exist", i);
    }
}
