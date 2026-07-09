use powerfs_fuse::cache::{CachedEntry, ChunkCache, MetadataCache, DEFAULT_CHUNK_SIZE, ROOT_INODE};
use powerfs_fuse::fuser_fs::WriteBuffer;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

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

#[test]
fn test_write_buffer_add_and_take() {
    let write_buffer = WriteBuffer::new(64);

    let inode = 100;
    write_buffer.add(inode, 0, b"Hello");
    write_buffer.add(inode, 5, b" World");

    let entries = write_buffer.take(inode);
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].offset, 0);
    assert_eq!(entries[0].data, b"Hello");
    assert_eq!(entries[1].offset, 5);
    assert_eq!(entries[1].data, b" World");

    let entries_after = write_buffer.take(inode);
    assert_eq!(entries_after.len(), 0);
}

#[test]
fn test_write_buffer_multiple_inodes() {
    let write_buffer = WriteBuffer::new(64);

    write_buffer.add(100, 0, b"File 1");
    write_buffer.add(200, 0, b"File 2");

    let entries1 = write_buffer.take(100);
    assert_eq!(entries1.len(), 1);
    assert_eq!(entries1[0].data, b"File 1");

    let entries2 = write_buffer.take(200);
    assert_eq!(entries2.len(), 1);
    assert_eq!(entries2[0].data, b"File 2");
}

#[test]
fn test_write_buffer_flush_when_full() {
    let write_buffer = WriteBuffer::new(2);

    let inode = 100;
    let should_flush1 = write_buffer.add(inode, 0, b"First");
    assert_eq!(should_flush1, false);

    let should_flush2 = write_buffer.add(inode, 5, b"Second");
    assert_eq!(should_flush2, true);

    let entries = write_buffer.take(inode);
    assert_eq!(entries.len(), 2);
}

#[test]
fn test_write_buffer_empty_take() {
    let write_buffer = WriteBuffer::new(64);

    let entries = write_buffer.take(100);
    assert_eq!(entries.len(), 0);
}

#[test]
fn test_chunk_cache_put_and_get() {
    let chunk_cache = ChunkCache::with_defaults();

    let inode = 100;
    let mut data = vec![0u8; DEFAULT_CHUNK_SIZE as usize];
    data[0..9].copy_from_slice(b"Test data");
    chunk_cache.put(inode, 0, data, 12345, 0x12345678);

    let retrieved = chunk_cache.get(inode, 0);
    assert!(retrieved.is_some());

    let chunk_data = retrieved.unwrap();
    assert_eq!(chunk_data.data.len(), DEFAULT_CHUNK_SIZE as usize);

    let content = String::from_utf8_lossy(&chunk_data.data[0..9]);
    assert_eq!(content, "Test data");
}

#[test]
fn test_chunk_cache_get_nonexistent() {
    let chunk_cache = ChunkCache::with_defaults();

    let retrieved = chunk_cache.get(100, 0);
    assert!(retrieved.is_none());
}

#[test]
fn test_chunk_cache_multiple_chunks() {
    let chunk_cache = ChunkCache::with_defaults();
    let chunk_size = chunk_cache.chunk_size();

    let inode = 100;

    let mut data0 = vec![0u8; chunk_size as usize];
    data0[0..7].copy_from_slice(b"Chunk 0");
    chunk_cache.put(inode, 0, data0, 1, 0);

    let mut data1 = vec![0u8; chunk_size as usize];
    data1[0..7].copy_from_slice(b"Chunk 1");
    chunk_cache.put(inode, chunk_size, data1, 2, 0);

    let chunk0 = chunk_cache.get(inode, 0).unwrap();
    let chunk1 = chunk_cache.get(inode, chunk_size).unwrap();

    let content0 = String::from_utf8_lossy(&chunk0.data[0..7]);
    let content1 = String::from_utf8_lossy(&chunk1.data[0..7]);

    assert_eq!(content0, "Chunk 0");
    assert_eq!(content1, "Chunk 1");
}

#[test]
fn test_dirty_chunks_tracking() {
    let dirty_chunks = Arc::new(RwLock::new(HashSet::new()));
    let inode = 100;

    {
        let mut write_guard = dirty_chunks.write().unwrap();
        write_guard.insert((inode, 0));
        write_guard.insert((inode, 1));
        write_guard.insert((200, 0));
    }

    {
        let read_guard = dirty_chunks.read().unwrap();
        assert!(read_guard.contains(&(inode, 0)));
        assert!(read_guard.contains(&(inode, 1)));
        assert!(read_guard.contains(&(200, 0)));
    }

    {
        let mut write_guard = dirty_chunks.write().unwrap();
        write_guard.retain(|(ino, _)| *ino != inode);
    }

    {
        let read_guard = dirty_chunks.read().unwrap();
        assert!(!read_guard.contains(&(inode, 0)));
        assert!(!read_guard.contains(&(inode, 1)));
        assert!(read_guard.contains(&(200, 0)));
    }
}

#[test]
fn test_metadata_cache_insert_and_get() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "test.txt");
    cache.insert(entry);

    let retrieved = cache.get_inode(100);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().name, "test.txt");
}

#[test]
fn test_metadata_cache_lookup() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "lookup.txt");
    cache.insert(entry);

    let found = cache.lookup_in_cache(ROOT_INODE, "lookup.txt");
    assert!(found.is_some());
    assert_eq!(found.unwrap().inode, 100);

    let not_found = cache.lookup_in_cache(ROOT_INODE, "notexist.txt");
    assert!(not_found.is_none());
}

#[test]
fn test_metadata_cache_update_size() {
    let cache = MetadataCache::new();
    let entry = make_file_entry(100, ROOT_INODE, "size.txt");
    cache.insert(entry);

    cache.update_size(100, 4096);
    let retrieved = cache.get_inode(100).unwrap();
    assert_eq!(retrieved.size, 4096);
}

#[test]
fn test_metadata_cache_list_children() {
    let cache = MetadataCache::new();

    let dir = CachedEntry {
        inode: 10,
        parent: ROOT_INODE,
        name: "subdir".to_string(),
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
    };
    cache.insert(dir);

    let f1 = make_file_entry(100, 10, "a.txt");
    let f2 = make_file_entry(101, 10, "b.txt");
    cache.insert(f1);
    cache.insert(f2);

    let children = cache.list_children(10);
    let names: Vec<&str> = children.iter().map(|c| c.1.as_str()).collect();
    assert!(names.contains(&"a.txt"));
    assert!(names.contains(&"b.txt"));
}

#[test]
fn test_concurrent_write_buffer_access() {
    let write_buffer = Arc::new(WriteBuffer::new(100));
    let mut handles = Vec::new();

    for i in 0..10 {
        let write_buffer_clone = Arc::clone(&write_buffer);
        let handle = std::thread::spawn(move || {
            for j in 0..10 {
                write_buffer_clone.add(i as u64, j as u64 * 10, &format!("data{}", j).as_bytes());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for i in 0..10 {
        let entries = write_buffer.take(i as u64);
        assert_eq!(entries.len(), 10);
    }
}

#[test]
fn test_concurrent_chunk_cache_access() {
    let chunk_cache = Arc::new(ChunkCache::with_defaults());
    let chunk_size = chunk_cache.chunk_size();
    let mut handles = Vec::new();

    for i in 0..5 {
        let chunk_cache_clone = Arc::clone(&chunk_cache);
        let handle = std::thread::spawn(move || {
            let mut data = vec![0u8; chunk_size as usize];
            data[0] = i as u8;
            chunk_cache_clone.put(i as u64, 0, data, i as u64, 0);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for i in 0..5 {
        let data = chunk_cache.get(i as u64, 0);
        assert!(data.is_some());
        assert_eq!(data.unwrap().data[0], i as u8);
    }
}

#[test]
fn test_concurrent_dirty_chunks_access() {
    let dirty_chunks = Arc::new(RwLock::new(HashSet::new()));
    let mut handles = Vec::new();

    for i in 0..10 {
        let dirty_chunks_clone = Arc::clone(&dirty_chunks);
        let handle = std::thread::spawn(move || {
            dirty_chunks_clone.write().unwrap().insert((100, i as u64));
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let dirty_set = dirty_chunks.read().unwrap();
    assert_eq!(dirty_set.len(), 10);
}

#[test]
fn test_chunk_size_calculations() {
    let chunk_size = DEFAULT_CHUNK_SIZE;

    assert_eq!(0 / chunk_size, 0);
    assert_eq!(1 / chunk_size, 0);
    assert_eq!(chunk_size / chunk_size, 1);
    assert_eq!((chunk_size + 1) / chunk_size, 1);

    assert_eq!(0 % chunk_size, 0);
    assert_eq!(100 % chunk_size, 100);
    assert_eq!(chunk_size % chunk_size, 0);

    let data_size: u64 = 100;
    let start_chunk_idx = 0 / chunk_size;
    let end_chunk_idx = (0 + data_size).div_ceil(chunk_size);
    assert_eq!(start_chunk_idx, 0);
    assert_eq!(end_chunk_idx, 1);
}

#[test]
fn test_write_buffer_entry_order() {
    let write_buffer = WriteBuffer::new(64);

    write_buffer.add(100, 100, b"Third");
    write_buffer.add(100, 0, b"First");
    write_buffer.add(100, 50, b"Second");

    let entries = write_buffer.take(100);
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].offset, 100);
    assert_eq!(entries[1].offset, 0);
    assert_eq!(entries[2].offset, 50);
}
