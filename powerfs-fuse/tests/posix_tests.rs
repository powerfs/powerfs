use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::FromRawFd;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

#[path = "test_harness.rs"]
mod test_harness;

fn fuse_mount() -> String {
    test_harness::get_fuse_mount()
}

macro_rules! test_path {
    ($name:expr) => {
        Path::new(&fuse_mount()).join($name)
    };
}

fn setup() {
    if let Err(e) = test_harness::ensure_fuse_mounted() {
        eprintln!("Skipping test: {}", e);
        std::process::exit(0);
    }
}

#[test]
fn test_open_single() {
    setup();
    let path = test_path!("test_open_single.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"hello").unwrap();
    drop(f);

    let mut f = File::open(&path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "hello");
}

#[test]
fn test_open_multiple_readers() {
    setup();
    let path = test_path!("test_open_multi.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"shared content").unwrap();
    drop(f);

    let mut f1 = File::open(&path).unwrap();
    let mut f2 = File::open(&path).unwrap();

    let mut buf1 = String::new();
    let mut buf2 = String::new();
    f1.read_to_string(&mut buf1).unwrap();
    f2.read_to_string(&mut buf2).unwrap();

    assert_eq!(buf1, "shared content");
    assert_eq!(buf2, "shared content");

    drop(f1);
    drop(f2);
}

#[test]
fn test_open_multiple_writers_append() {
    setup();
    let path = test_path!("test_open_multi_write.txt");

    let _ = fs::remove_file(&path);
    let mut f1 = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap();
    let mut f2 = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap();

    f1.write_all(b"first\n").unwrap();
    f2.write_all(b"second\n").unwrap();
    f1.write_all(b"third\n").unwrap();
    f2.write_all(b"fourth\n").unwrap();

    drop(f1);
    drop(f2);

    let mut f = File::open(&path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();

    assert!(buf.contains("first"));
    assert!(buf.contains("second"));
    assert!(buf.contains("third"));
    assert!(buf.contains("fourth"));
    assert_eq!(buf.lines().count(), 4);

    let _ = fs::remove_file(&path);
}

#[test]
fn test_open_exclusive_create() {
    setup();
    let path = test_path!("test_excl.txt");

    let _ = fs::remove_file(&path);
    let _f1 = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap();

    let result = OpenOptions::new().create_new(true).write(true).open(&path);

    assert!(result.is_err());

    let _ = fs::remove_file(&path);
}

#[test]
fn test_write_at_offset() {
    setup();
    let path = test_path!("test_offset_write.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"0000000000").unwrap();
    drop(f);

    let mut f = OpenOptions::new().write(true).open(&path).unwrap();
    f.seek(SeekFrom::Start(2)).unwrap();
    f.write_all(b"AAA").unwrap();
    drop(f);

    let mut f = File::open(&path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "00AAA00000");
}

#[test]
fn test_write_concurrent_offset() {
    setup();
    let path = test_path!("test_concurrent_offset.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(&[0u8; 100]).unwrap();
    drop(f);

    let barrier = Arc::new(Barrier::new(2));
    let results = Arc::new(Mutex::new(Vec::new()));

    let t1 = thread::spawn({
        let path = path.clone();
        let barrier = barrier.clone();
        let results = results.clone();
        move || {
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            barrier.wait();
            f.seek(SeekFrom::Start(10)).unwrap();
            f.write_all(b"AAAAA").unwrap();
            results.lock().unwrap().push(1);
        }
    });

    let t2 = thread::spawn({
        let path = path.clone();
        let barrier = barrier.clone();
        let results = results.clone();
        move || {
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            barrier.wait();
            f.seek(SeekFrom::Start(20)).unwrap();
            f.write_all(b"BBBBB").unwrap();
            results.lock().unwrap().push(2);
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    let mut f = File::open(&path).unwrap();
    let mut buf = vec![0u8; 100];
    f.read_exact(&mut buf).unwrap();

    assert_eq!(&buf[10..15], b"AAAAA");
    assert_eq!(&buf[20..25], b"BBBBB");
}

#[test]
fn test_read_write_roundtrip() {
    setup();
    let path = test_path!("test_roundtrip.txt");

    let data = b"The quick brown fox jumps over the lazy dog";
    let mut f = File::create(&path).unwrap();
    f.write_all(data).unwrap();
    drop(f);

    let mut f = File::open(&path).unwrap();
    let mut buf = vec![0u8; data.len()];
    f.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, data);
}

#[test]
fn test_read_partial() {
    setup();
    let path = test_path!("test_partial_read.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"1234567890").unwrap();
    drop(f);

    let mut f = File::open(&path).unwrap();
    let mut buf = vec![0u8; 3];
    f.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"123");

    let mut buf2 = vec![0u8; 4];
    f.read_exact(&mut buf2).unwrap();
    assert_eq!(&buf2, b"4567");
}

#[test]
fn test_seek_read() {
    setup();
    let path = test_path!("test_seek.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"abcdefghijklmnopqrstuvwxyz").unwrap();
    drop(f);

    let mut f = File::open(&path).unwrap();
    f.seek(SeekFrom::Start(10)).unwrap();
    let mut buf = vec![0u8; 5];
    f.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"klmno");

    f.seek(SeekFrom::End(-5)).unwrap();
    f.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"vwxyz");

    f.seek(SeekFrom::Current(-10)).unwrap();
    f.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"qrstu");
}

#[test]
fn test_mkdir_rmdir() {
    setup();
    let dir = test_path!("test_dir");

    fs::create_dir(&dir).unwrap();
    assert!(dir.exists());
    assert!(dir.is_dir());

    fs::remove_dir(&dir).unwrap();
    assert!(!dir.exists());
}

#[test]
fn test_mkdir_nested() {
    setup();
    let dir = test_path!("test_nested/a/b/c");

    fs::create_dir_all(&dir).unwrap();
    assert!(dir.exists());

    let file = dir.join("file.txt");
    let mut f = File::create(&file).unwrap();
    f.write_all(b"nested").unwrap();
    drop(f);

    fs::remove_dir_all(test_path!("test_nested")).unwrap();
    assert!(!test_path!("test_nested").exists());
}

#[test]
fn test_readdir() {
    setup();
    let dir = test_path!("test_readdir");

    let _ = fs::remove_dir_all(&dir);
    fs::create_dir(&dir).unwrap();
    for i in 0..5 {
        let mut f = File::create(dir.join(format!("file{}.txt", i))).unwrap();
        f.write_all(&[i as u8]).unwrap();
    }

    let entries: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();

    assert_eq!(entries.len(), 5);
    for i in 0..5 {
        assert!(entries.contains(&format!("file{}.txt", i)));
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_rename() {
    setup();
    let old_path = test_path!("test_rename_old.txt");
    let new_path = test_path!("test_rename_new.txt");

    let mut f = File::create(&old_path).unwrap();
    f.write_all(b"rename test").unwrap();
    drop(f);

    fs::rename(&old_path, &new_path).unwrap();

    assert!(!old_path.exists());
    assert!(new_path.exists());

    let mut f = File::open(&new_path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "rename test");
}

#[test]
fn test_rename_dir() {
    setup();
    let old_dir = test_path!("test_rename_dir_old");
    let new_dir = test_path!("test_rename_dir_new");

    let _ = fs::remove_dir_all(&old_dir);
    let _ = fs::remove_dir_all(&new_dir);
    fs::create_dir(&old_dir).unwrap();
    let mut f = File::create(old_dir.join("file.txt")).unwrap();
    f.write_all(b"in dir").unwrap();
    drop(f);

    fs::rename(&old_dir, &new_dir).unwrap();

    assert!(!old_dir.exists());
    assert!(new_dir.exists());
    assert!(new_dir.join("file.txt").exists());

    let _ = fs::remove_dir_all(&new_dir);
}

#[test]
fn test_symlink() {
    setup();
    let target = test_path!("test_symlink_target.txt");
    let link = test_path!("test_symlink_link");

    let _ = fs::remove_file(&link);
    let _ = fs::remove_file(&target);
    let mut f = File::create(&target).unwrap();
    f.write_all(b"symlink target").unwrap();
    drop(f);

    symlink(&target, &link).unwrap();

    let mut f = File::open(&link).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "symlink target");

    let read_link = fs::read_link(&link).unwrap();
    assert!(read_link.ends_with("test_symlink_target.txt"));

    let _ = fs::remove_file(&link);
    let _ = fs::remove_file(&target);
}

#[test]
fn test_truncate() {
    setup();
    let path = test_path!("test_truncate.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"1234567890").unwrap();
    drop(f);

    let f = OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(5).unwrap();
    drop(f);

    let mut f = File::open(&path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "12345");

    let f = OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(0).unwrap();
    drop(f);

    let metadata = fs::metadata(&path).unwrap();
    assert_eq!(metadata.len(), 0);
}

#[test]
fn test_file_metadata() {
    setup();
    let path = test_path!("test_metadata.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"test").unwrap();
    drop(f);

    let metadata = fs::metadata(&path).unwrap();
    assert_eq!(metadata.len(), 4);
    assert!(!metadata.is_dir());
    assert!(metadata.is_file());
}

#[test]
fn test_chmod() {
    setup();
    let path = test_path!("test_chmod.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"chmod test").unwrap();
    drop(f);

    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

    let permissions = fs::metadata(&path).unwrap().permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(permissions.mode(), 0o100600);
    }
}

#[test]
fn test_remove_file() {
    setup();
    let path = test_path!("test_remove.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"to be removed").unwrap();
    drop(f);

    assert!(path.exists());
    fs::remove_file(&path).unwrap();
    assert!(!path.exists());
}

#[test]
fn test_concurrent_create() {
    setup();
    let results = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    for i in 0..5 {
        let results = results.clone();
        let handle = thread::spawn(move || {
            let path = test_path!(format!("concurrent_{}.txt", i));
            let mut f = File::create(&path).unwrap();
            f.write_all(&[i as u8]).unwrap();
            results.lock().unwrap().push(i);
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(results.lock().unwrap().len(), 5);

    for i in 0..5 {
        let path = test_path!(format!("concurrent_{}.txt", i));
        assert!(path.exists());
        let mut f = File::open(&path).unwrap();
        let mut buf = [0u8; 1];
        f.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], i as u8);
    }
}

#[test]
fn test_concurrent_read() {
    setup();
    let path = test_path!("test_concurrent_read.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"concurrent read test content").unwrap();
    drop(f);

    let results = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    for _ in 0..3 {
        let results = results.clone();
        let path = path.clone();
        let handle = thread::spawn(move || {
            let mut f = File::open(&path).unwrap();
            let mut buf = String::new();
            f.read_to_string(&mut buf).unwrap();
            results.lock().unwrap().push(buf);
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    let all_results = results.lock().unwrap();
    assert_eq!(all_results.len(), 3);
    for r in all_results.iter() {
        assert_eq!(*r, "concurrent read test content");
    }
}

#[test]
fn test_pread_pwrite() {
    setup();
    let path = test_path!("test_pread_pwrite.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(&[0u8; 100]).unwrap();
    drop(f);

    let fd = OpenOptions::new().write(true).open(&path).unwrap();
    unsafe {
        libc::pwrite(
            fd.as_raw_fd(),
            b"PWRITE" as *const u8 as *const libc::c_void,
            6,
            10,
        );
    }
    drop(fd);

    let fd = File::open(&path).unwrap();
    let mut buf = [0u8; 6];
    unsafe {
        libc::pread(fd.as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, 6, 10);
    }
    assert_eq!(&buf, b"PWRITE");
}

#[test]
fn test_dup() {
    setup();
    let path = test_path!("test_dup.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"dup test").unwrap();
    drop(f);

    let mut f1 = File::open(&path).unwrap();
    let mut f2 = unsafe { File::from_raw_fd(libc::dup(f1.as_raw_fd())) };

    f1.seek(SeekFrom::Start(4)).unwrap();
    let mut buf1 = [0u8; 4];
    f1.read_exact(&mut buf1).unwrap();
    assert_eq!(&buf1, b"test");

    f2.seek(SeekFrom::Start(0)).unwrap();
    let mut buf2 = [0u8; 3];
    f2.read_exact(&mut buf2).unwrap();
    assert_eq!(&buf2, b"dup");
}

#[test]
fn test_fsync() {
    setup();
    let path = test_path!("test_fsync.txt");

    let _ = fs::remove_file(&path);
    let mut f = File::create(&path).unwrap();
    f.write_all(b"fsync test").unwrap();
    f.sync_all().unwrap();
    drop(f);

    let mut f = File::open(&path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "fsync test");

    let _ = fs::remove_file(&path);
}

#[test]
fn test_ftruncate() {
    setup();
    let path = test_path!("test_ftruncate.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"1234567890").unwrap();
    unsafe {
        libc::ftruncate(f.as_raw_fd(), 5);
    }
    drop(f);

    let mut f = File::open(&path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "12345");
}

#[test]
fn test_file_rename_overwrite() {
    setup();
    let src = test_path!("test_rename_src.txt");
    let dst = test_path!("test_rename_dst.txt");

    let _ = fs::remove_file(&src);
    let _ = fs::remove_file(&dst);
    let mut f = File::create(&src).unwrap();
    f.write_all(b"source").unwrap();
    drop(f);

    let mut f = File::create(&dst).unwrap();
    f.write_all(b"dest").unwrap();
    drop(f);

    fs::rename(&src, &dst).unwrap();

    assert!(!src.exists());
    assert!(dst.exists());

    let mut f = File::open(&dst).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "source");

    let _ = fs::remove_file(&dst);
}

#[test]
fn test_empty_file() {
    setup();
    let path = test_path!("test_empty.txt");

    File::create(&path).unwrap();

    let metadata = fs::metadata(&path).unwrap();
    assert_eq!(metadata.len(), 0);

    let mut f = File::open(&path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "");
}

#[test]
fn test_large_write() {
    setup();
    let path = test_path!("test_large.txt");

    let data = vec![0xAAu8; 1024 * 1024];
    let mut f = File::create(&path).unwrap();
    f.write_all(&data).unwrap();
    drop(f);

    let metadata = fs::metadata(&path).unwrap();
    assert_eq!(metadata.len(), 1024 * 1024);

    let mut f = File::open(&path).unwrap();
    let mut buf = vec![0u8; 1024 * 1024];
    f.read_exact(&mut buf).unwrap();
    assert_eq!(buf, data);
}

#[test]
fn test_multiple_writes() {
    setup();
    let path = test_path!("test_multi_write.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"Hello").unwrap();
    f.write_all(b" ").unwrap();
    f.write_all(b"World").unwrap();
    f.write_all(b"!").unwrap();
    drop(f);

    let mut f = File::open(&path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "Hello World!");
}

#[test]
fn test_append_mode() {
    setup();
    let path = test_path!("test_append.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"first").unwrap();
    drop(f);

    let mut f = OpenOptions::new().append(true).open(&path).unwrap();
    f.write_all(b"second").unwrap();
    drop(f);

    let mut f = File::open(&path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "firstsecond");
}

#[test]
fn test_truncate_on_open() {
    setup();
    let path = test_path!("test_truncate_open.txt");

    let mut f = File::create(&path).unwrap();
    f.write_all(b"original content").unwrap();
    drop(f);

    let mut f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    f.write_all(b"new content").unwrap();
    drop(f);

    let mut f = File::open(&path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "new content");
}

#[test]
fn test_directory_permissions() {
    setup();
    let dir = test_path!("test_dir_perm");

    let _ = fs::remove_dir_all(&dir);
    fs::create_dir(&dir).unwrap();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();

    let file = dir.join("file.txt");
    let mut f = File::create(&file).unwrap();
    f.write_all(b"in dir").unwrap();
    drop(f);

    assert!(file.exists());

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_readdir_empty() {
    setup();
    let dir = test_path!("test_empty_dir");

    let _ = fs::remove_dir_all(&dir);
    fs::create_dir(&dir).unwrap();

    let entries: Vec<_> = fs::read_dir(&dir).unwrap().collect();
    assert_eq!(entries.len(), 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_path_traversal() {
    setup();
    let dir = test_path!("test_path_traversal");
    let subdir = dir.join("sub");

    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&subdir).unwrap();
    let mut f = File::create(subdir.join("file.txt")).unwrap();
    f.write_all(b"traversal test").unwrap();
    drop(f);

    let path = test_path!("test_path_traversal/../test_path_traversal/sub/file.txt");
    let mut f = File::open(&path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "traversal test");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_fdatasync() {
    setup();
    let path = test_path!("test_fdatasync.txt");

    let _ = fs::remove_file(&path);
    let mut f = File::create(&path).unwrap();
    f.write_all(b"fdatasync test").unwrap();
    f.sync_data().unwrap();
    drop(f);

    let mut retries = 3;
    let mut f = loop {
        match File::open(&path) {
            Ok(f) => break f,
            Err(_) if retries > 0 => {
                retries -= 1;
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            Err(e) => panic!("Failed to open file after retries: {}", e),
        }
    };
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "fdatasync test");

    let _ = fs::remove_file(&path);
}

#[test]
fn test_file_sync_rename() {
    setup();
    let tmp_path = test_path!("test_sync_rename.tmp");
    let final_path = test_path!("test_sync_rename.txt");

    let _ = fs::remove_file(&tmp_path);
    let _ = fs::remove_file(&final_path);
    let mut f = File::create(&tmp_path).unwrap();
    f.write_all(b"atomic write").unwrap();
    f.sync_all().unwrap();
    drop(f);

    let mut retries = 3;
    loop {
        match fs::rename(&tmp_path, &final_path) {
            Ok(_) => break,
            Err(_) if retries > 0 => {
                retries -= 1;
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            Err(e) => panic!("Failed to rename file after retries: {}", e),
        }
    }

    assert!(!tmp_path.exists());
    assert!(final_path.exists());

    let mut f = File::open(&final_path).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "atomic write");

    let _ = fs::remove_file(&final_path);
}

#[test]
fn test_directory_recursive_copy() {
    setup();
    let src_dir = test_path!("test_copy_src");
    let dst_dir = test_path!("test_copy_dst");
    let dst_subdir = dst_dir.join("test_copy_src");

    let _ = fs::remove_dir_all(&src_dir);
    let _ = fs::remove_dir_all(&dst_dir);

    fs::create_dir_all(&src_dir).unwrap();

    let mut f = File::create(src_dir.join("file1.txt")).unwrap();
    f.write_all(b"file1 content").unwrap();
    drop(f);

    let mut f = File::create(src_dir.join("file2.txt")).unwrap();
    f.write_all(b"file2 content").unwrap();
    drop(f);

    let subdir1 = src_dir.join("subdir1");
    fs::create_dir(&subdir1).unwrap();
    let mut f = File::create(subdir1.join("nested1.txt")).unwrap();
    f.write_all(b"nested1 content").unwrap();
    drop(f);

    let subdir2 = src_dir.join("subdir1/subdir2");
    fs::create_dir_all(&subdir2).unwrap();
    let mut f = File::create(subdir2.join("deep.txt")).unwrap();
    f.write_all(b"deep nested content").unwrap();
    drop(f);

    fs::create_dir_all(&dst_dir).unwrap();

    copy_directory(&src_dir, &dst_subdir);

    assert!(dst_subdir.exists(), "Destination subdir should exist");
    assert!(
        dst_subdir.join("file1.txt").exists(),
        "file1.txt should exist in dest"
    );
    assert!(
        dst_subdir.join("file2.txt").exists(),
        "file2.txt should exist in dest"
    );
    assert!(
        dst_subdir.join("subdir1").exists(),
        "subdir1 should exist in dest"
    );
    assert!(
        dst_subdir.join("subdir1/nested1.txt").exists(),
        "nested1.txt should exist"
    );
    assert!(
        dst_subdir.join("subdir1/subdir2").exists(),
        "subdir2 should exist"
    );
    assert!(
        dst_subdir.join("subdir1/subdir2/deep.txt").exists(),
        "deep.txt should exist"
    );

    let mut f = File::open(dst_subdir.join("file1.txt")).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "file1 content");

    let mut f = File::open(dst_subdir.join("subdir1/nested1.txt")).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "nested1 content");

    let mut f = File::open(dst_subdir.join("subdir1/subdir2/deep.txt")).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "deep nested content");

    let _ = fs::remove_dir_all(&src_dir);
    let _ = fs::remove_dir_all(&dst_dir);
}

#[test]
fn test_directory_copy_to_current_dir() {
    setup();
    let src_dir = test_path!("powerfs-core");
    let dst_dir = test_path!("powerfs-core-dst");

    let _ = fs::remove_dir_all(&src_dir);
    let _ = fs::remove_dir_all(&dst_dir);

    fs::create_dir_all(&src_dir).unwrap();

    let mut f = File::create(src_dir.join("Cargo.toml")).unwrap();
    f.write_all(b"[package]\nname = \"powerfs-core\"\n")
        .unwrap();
    drop(f);

    let subdir = src_dir.join("src");
    fs::create_dir_all(&subdir).unwrap();

    let mut f = File::create(subdir.join("lib.rs")).unwrap();
    f.write_all(b"pub fn hello() {}\n").unwrap();
    drop(f);

    let mut f = File::create(subdir.join("mod.rs")).unwrap();
    f.write_all(b"pub mod inner;\n").unwrap();
    drop(f);

    fs::create_dir_all(&dst_dir).unwrap();

    let dst_subdir = dst_dir.join("powerfs-core");
    copy_directory(&src_dir, &dst_subdir);

    assert!(dst_subdir.exists(), "powerfs-core should exist in dst dir");
    assert!(
        dst_subdir.join("Cargo.toml").exists(),
        "Cargo.toml should exist"
    );
    assert!(
        dst_subdir.join("src/lib.rs").exists(),
        "src/lib.rs should exist"
    );
    assert!(
        dst_subdir.join("src/mod.rs").exists(),
        "src/mod.rs should exist"
    );

    let mut f = File::open(dst_subdir.join("Cargo.toml")).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "[package]\nname = \"powerfs-core\"\n");

    let _ = fs::remove_dir_all(&src_dir);
    let _ = fs::remove_dir_all(&dst_dir);
}

fn copy_directory(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if entry.file_type().unwrap().is_dir() {
            copy_directory(&src_path, &dst_path);
        } else {
            let mut src_file = File::open(&src_path).unwrap();
            let mut dst_file = File::create(&dst_path).unwrap();
            let mut buf = vec![0u8; 4096];
            loop {
                let bytes_read = src_file.read(&mut buf).unwrap();
                if bytes_read == 0 {
                    break;
                }
                dst_file.write_all(&buf[..bytes_read]).unwrap();
            }
            drop(src_file);
            drop(dst_file);
        }
    }
}
