use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn get_mount_path() -> String {
    std::env::var("POWERFS_MOUNT").unwrap_or_else(|_| "/tmp/powerfs-test".to_string())
}

fn get_test_dir_name() -> String {
    let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let test_name = std::thread::current()
        .name()
        .unwrap_or("unknown")
        .to_string();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() % 1_000_000)
        .unwrap_or(0);
    format!("test_{}_{}_{}", counter, test_name, timestamp)
}

fn assert_powerfs_mounted() {
    let mount_path = get_mount_path();
    if let Ok(content) = std::fs::read_to_string("/proc/mounts") {
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 && parts[1] == mount_path {
                let fstype = parts[2];
                // 接受 "fuse"、"fuse.powerfs-fuse" 以及任何 "fuse.*" 形式
                if fstype == "fuse" || fstype == "fuse.powerfs-fuse" || fstype.starts_with("fuse.")
                {
                    return;
                }
            }
        }
    }
    panic!(
        "Mount path '{}' is not a PowerFS FUSE mount! Tests must run against PowerFS.",
        mount_path
    );
}

#[test]
fn test_create_file_open_close_unlink() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("test_file.txt");

    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"Hello").expect("Failed to write");
    drop(file);

    assert!(file_path.exists(), "File should exist");

    let mut file = File::open(&file_path).expect("Failed to open file");
    let mut content = String::new();
    file.read_to_string(&mut content).expect("Failed to read");
    assert_eq!(content, "Hello", "Content mismatch");
    drop(file);

    fs::remove_file(&file_path).expect("Failed to remove file");
    assert!(!file_path.exists(), "File should not exist after unlink");

    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_mkdir_readdir_rmdir() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let subdir = test_dir.join("subdir");
    fs::create_dir(&subdir).expect("Failed to create subdir");

    let file_in_subdir = subdir.join("file.txt");
    File::create(&file_in_subdir).expect("Failed to create file in subdir");
    drop(File::open(&file_in_subdir).unwrap());

    let entries: Vec<_> = fs::read_dir(&test_dir)
        .expect("Failed to read dir")
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        entries.contains(&"subdir".to_string()),
        "subdir should be listed"
    );

    let sub_entries: Vec<_> = fs::read_dir(&subdir)
        .expect("Failed to read subdir")
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        sub_entries.contains(&"file.txt".to_string()),
        "file.txt should be listed"
    );

    fs::remove_file(&file_in_subdir).expect("Failed to remove file in subdir");
    fs::remove_dir(&subdir).expect("Failed to remove subdir");
    assert!(!subdir.exists(), "Subdir should not exist");

    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_write_read_small_file() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("small.txt");

    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"Small test content")
        .expect("Failed to write");
    drop(file);

    let metadata = fs::metadata(&file_path).expect("Failed to get metadata");
    assert_eq!(metadata.len(), 18, "File size should be 18");

    let mut file = File::open(&file_path).expect("Failed to open file");
    let mut content = String::new();
    file.read_to_string(&mut content).expect("Failed to read");
    assert_eq!(content, "Small test content", "Content mismatch");

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_multilevel_directory_structure() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let level1 = test_dir.join("level1");
    let level2 = level1.join("level2");
    let level3 = level2.join("level3");

    fs::create_dir(&level1).expect("Failed to create level1");
    fs::create_dir(&level2).expect("Failed to create level2");
    fs::create_dir(&level3).expect("Failed to create level3");

    let file1 = level1.join("file1.txt");
    let file2 = level2.join("file2.txt");
    let file3 = level3.join("file3.txt");

    let mut f1 = File::create(&file1).expect("Failed to create file1");
    f1.write_all(b"Level 1").expect("Failed to write file1");
    drop(f1);

    let mut f2 = File::create(&file2).expect("Failed to create file2");
    f2.write_all(b"Level 2").expect("Failed to write file2");
    drop(f2);

    let mut f3 = File::create(&file3).expect("Failed to create file3");
    f3.write_all(b"Level 3").expect("Failed to write file3");
    drop(f3);

    assert!(file1.exists(), "file1 should exist");
    assert!(file2.exists(), "file2 should exist");
    assert!(file3.exists(), "file3 should exist");

    let mut content = String::new();
    File::open(&file3)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "Level 3", "Content mismatch");

    fs::remove_file(&file3).expect("Failed to remove file3");
    fs::remove_dir(&level3).expect("Failed to remove level3");
    fs::remove_file(&file2).expect("Failed to remove file2");
    fs::remove_dir(&level2).expect("Failed to remove level2");
    fs::remove_file(&file1).expect("Failed to remove file1");
    fs::remove_dir(&level1).expect("Failed to remove level1");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_append_write() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("append.txt");

    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"First part")
        .expect("Failed to write first part");
    drop(file);

    let mut file = File::options()
        .append(true)
        .open(&file_path)
        .expect("Failed to open file for append");
    file.write_all(b" Second part").expect("Failed to append");
    drop(file);

    let mut content = String::new();
    File::open(&file_path)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "First part Second part", "Append content mismatch");

    let metadata = fs::metadata(&file_path).expect("Failed to get metadata");
    assert_eq!(metadata.len(), 22, "File size should be 22");

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_truncate_file() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("truncate.txt");

    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"This is a longer text")
        .expect("Failed to write");
    drop(file);

    let metadata = fs::metadata(&file_path).expect("Failed to get metadata");
    assert_eq!(metadata.len(), 21, "File size should be 21");

    File::options()
        .write(true)
        .open(&file_path)
        .expect("Failed to open file for truncate")
        .set_len(5)
        .expect("Failed to truncate");

    let metadata = fs::metadata(&file_path).expect("Failed to get metadata");
    assert_eq!(metadata.len(), 5, "File size should be 5 after truncate");

    let mut content = String::new();
    File::open(&file_path)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "This ", "Truncated content mismatch");

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_readdir_contains_dot_and_dotdot() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    // Create a subdirectory and a file
    let subdir = test_dir.join("subdir");
    fs::create_dir(&subdir).expect("Failed to create subdir");

    let file_path = test_dir.join("file.txt");
    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"test").expect("Failed to write");
    drop(file);

    // Read directory entries using `ls -a` (std::fs::read_dir filters . and ..)
    let output = std::process::Command::new("ls")
        .arg("-a")
        .arg(&test_dir)
        .output()
        .expect("Failed to run ls -a");
    assert!(output.status.success(), "ls -a failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let entries: Vec<&str> = stdout.trim().split('\n').collect();

    // Verify . and .. are present (POSIX compliance)
    assert!(
        entries.contains(&"."),
        "readdir should contain '.' (current directory)"
    );
    assert!(
        entries.contains(&".."),
        "readdir should contain '..' (parent directory)"
    );
    assert!(
        entries.contains(&"subdir"),
        "readdir should contain 'subdir'"
    );
    assert!(
        entries.contains(&"file.txt"),
        "readdir should contain 'file.txt'"
    );

    // Cleanup
    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&subdir).expect("Failed to remove subdir");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_rename_file() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let old_path = test_dir.join("old_name.txt");
    let new_path = test_dir.join("new_name.txt");

    let mut file = File::create(&old_path).expect("Failed to create file");
    file.write_all(b"rename content").expect("Failed to write");
    drop(file);

    // Rename the file
    fs::rename(&old_path, &new_path).expect("Failed to rename file");

    // Old path should not exist
    assert!(!old_path.exists(), "Old path should not exist after rename");

    // New path should exist and have correct content
    assert!(new_path.exists(), "New path should exist after rename");

    let mut content = String::new();
    File::open(&new_path)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "rename content", "Content mismatch after rename");

    // Cleanup
    fs::remove_file(&new_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_rename_directory() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let old_dir = test_dir.join("old_dir");
    let new_dir = test_dir.join("new_dir");

    fs::create_dir(&old_dir).expect("Failed to create old_dir");

    // Create a file inside the directory
    let inner_file = old_dir.join("inner.txt");
    let mut file = File::create(&inner_file).expect("Failed to create inner file");
    file.write_all(b"inner content").expect("Failed to write");
    drop(file);

    // Rename the directory
    fs::rename(&old_dir, &new_dir).expect("Failed to rename directory");

    // Old directory should not exist
    assert!(!old_dir.exists(), "Old dir should not exist after rename");

    // New directory should exist with contents
    assert!(new_dir.exists(), "New dir should exist after rename");
    assert!(new_dir.is_dir(), "New path should be a directory");

    let new_inner = new_dir.join("inner.txt");
    assert!(
        new_inner.exists(),
        "Inner file should exist after dir rename"
    );

    let mut content = String::new();
    File::open(&new_inner)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(
        content, "inner content",
        "Inner file content mismatch after dir rename"
    );

    // Cleanup
    fs::remove_file(&new_inner).expect("Failed to remove inner file");
    fs::remove_dir(&new_dir).expect("Failed to remove new dir");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_stat_and_chmod() {
    use std::os::unix::fs::PermissionsExt;

    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("chmod_test.txt");
    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"chmod content").expect("Failed to write");
    drop(file);

    // Get initial metadata (stat)
    let metadata = fs::metadata(&file_path).expect("Failed to get metadata");
    assert_eq!(metadata.len(), 13, "File size should be 13");
    assert!(metadata.is_file(), "Should be a file");

    // Get initial permissions
    let initial_mode = metadata.permissions().mode() & 0o7777;

    // Change permissions to 0o600
    fs::set_permissions(&file_path, fs::Permissions::from_mode(0o600))
        .expect("Failed to set permissions");

    // Verify permissions changed
    let new_metadata = fs::metadata(&file_path).expect("Failed to get metadata after chmod");
    let new_mode = new_metadata.permissions().mode() & 0o7777;
    assert_eq!(
        new_mode, 0o600,
        "Permissions should be 0o600 after chmod, got {:o}",
        new_mode
    );
    assert_ne!(new_mode, initial_mode, "Permissions should have changed");

    // Cleanup
    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_unlink_file() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("unlink_test.txt");

    // Create and verify file exists
    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"unlink me").expect("Failed to write");
    drop(file);
    assert!(file_path.exists(), "File should exist before unlink");

    // Unlink (remove) the file
    fs::remove_file(&file_path).expect("Failed to unlink file");

    // Verify file no longer exists
    assert!(!file_path.exists(), "File should not exist after unlink");

    // Verify readdir does not contain the file
    let entries: Vec<String> = fs::read_dir(&test_dir)
        .expect("Failed to read dir")
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        !entries.contains(&"unlink_test.txt".to_string()),
        "readdir should not contain unlinked file"
    );

    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_large_file_multiblock_write() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("large_file.bin");

    // Write 1MB of data (verifies multi-block writes even within a single chunk)
    let data_size = 1024 * 1024; // 1MB
    let data: Vec<u8> = (0..data_size).map(|i| (i % 256) as u8).collect();

    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(&data).expect("Failed to write large data");
    drop(file);

    // Verify file size
    let metadata = fs::metadata(&file_path).expect("Failed to get metadata");
    assert_eq!(
        metadata.len(),
        data_size as u64,
        "File size should be {}",
        data_size
    );

    // Read back and verify content
    let mut file = File::open(&file_path).expect("Failed to open file");
    let mut read_data = Vec::new();
    file.read_to_end(&mut read_data).expect("Failed to read");
    assert_eq!(read_data.len(), data_size, "Read data length mismatch");
    assert_eq!(read_data, data, "Data content mismatch");

    // Cleanup
    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_rmdir_non_empty_fails() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let subdir = test_dir.join("nonempty_dir");
    fs::create_dir(&subdir).expect("Failed to create subdir");

    // Create a file inside subdir
    let inner_file = subdir.join("inner.txt");
    File::create(&inner_file).expect("Failed to create inner file");

    // rmdir on non-empty directory should fail
    let result = fs::remove_dir(&subdir);
    assert!(result.is_err(), "rmdir on non-empty directory should fail");

    // Cleanup: remove file first, then dir
    fs::remove_file(&inner_file).expect("Failed to remove inner file");
    fs::remove_dir(&subdir).expect("Failed to remove subdir after emptying");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}
