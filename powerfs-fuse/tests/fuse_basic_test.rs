use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
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

fn is_powerfs_mounted() -> bool {
    let mount_path = get_mount_path();
    if let Ok(content) = std::fs::read_to_string("/proc/mounts") {
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 && parts[1] == mount_path {
                let fstype = parts[2];
                if fstype == "fuse" || fstype == "fuse.powerfs-fuse" || fstype.starts_with("fuse.")
                {
                    return true;
                }
            }
        }
    }
    false
}

fn skip_if_not_mounted() {
    if std::env::var("CI").is_ok() && !is_powerfs_mounted() {
        eprintln!("Skipping test: PowerFS not mounted in CI environment");
        std::process::exit(0);
    }
}

#[test]
fn test_create_file_open_close_unlink() {
    skip_if_not_mounted();
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
    skip_if_not_mounted();
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
    skip_if_not_mounted();
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
    skip_if_not_mounted();
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
    skip_if_not_mounted();
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
    skip_if_not_mounted();
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

    file = File::create(&file_path).expect("Failed to truncate file");
    file.write_all(b"Short")
        .expect("Failed to write after truncate");
    drop(file);

    let metadata = fs::metadata(&file_path).expect("Failed to get metadata after truncate");
    assert_eq!(metadata.len(), 5, "File size should be 5 after truncate");

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_rename_file() {
    skip_if_not_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let src = test_dir.join("src.txt");
    let dst = test_dir.join("dst.txt");

    let mut file = File::create(&src).expect("Failed to create source file");
    file.write_all(b"Rename me").expect("Failed to write");
    drop(file);

    fs::rename(&src, &dst).expect("Failed to rename file");

    assert!(!src.exists(), "Source file should not exist");
    assert!(dst.exists(), "Destination file should exist");

    let mut content = String::new();
    File::open(&dst)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "Rename me", "Content mismatch after rename");

    fs::remove_file(&dst).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_rename_directory() {
    skip_if_not_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let src = test_dir.join("src_dir");
    let dst = test_dir.join("dst_dir");

    fs::create_dir(&src).expect("Failed to create source dir");

    let file_in_src = src.join("file.txt");
    let mut file = File::create(&file_in_src).expect("Failed to create file in src");
    file.write_all(b"File in dir").expect("Failed to write");
    drop(file);

    fs::rename(&src, &dst).expect("Failed to rename directory");

    assert!(!src.exists(), "Source dir should not exist");
    assert!(dst.exists(), "Destination dir should exist");

    let file_in_dst = dst.join("file.txt");
    assert!(file_in_dst.exists(), "File should exist in renamed dir");

    let mut content = String::new();
    File::open(&file_in_dst)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "File in dir", "Content mismatch after dir rename");

    fs::remove_file(&file_in_dst).expect("Failed to remove file");
    fs::remove_dir(&dst).expect("Failed to remove dst dir");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_stat_and_chmod() {
    skip_if_not_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("chmod.txt");

    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"Test chmod").expect("Failed to write");
    drop(file);

    let metadata = fs::metadata(&file_path).expect("Failed to get metadata");
    assert_eq!(metadata.len(), 10, "File size should be 10");

    fs::set_permissions(&file_path, fs::Permissions::from_mode(0o755)).expect("Failed to chmod");

    let metadata = fs::metadata(&file_path).expect("Failed to get metadata after chmod");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            metadata.permissions().mode() & 0o777,
            0o755,
            "Permissions should be 0o755"
        );
    }

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_rmdir_non_empty_fails() {
    skip_if_not_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let non_empty_dir = test_dir.join("non_empty");
    fs::create_dir(&non_empty_dir).expect("Failed to create non-empty dir");

    let file_in_dir = non_empty_dir.join("file.txt");
    File::create(&file_in_dir).expect("Failed to create file");
    drop(File::open(&file_in_dir).unwrap());

    let result = fs::remove_dir(&non_empty_dir);
    assert!(result.is_err(), "Removing non-empty dir should fail");

    fs::remove_file(&file_in_dir).expect("Failed to remove file");
    fs::remove_dir(&non_empty_dir).expect("Failed to remove dir after file removal");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_readdir_contains_dot_and_dotdot() {
    skip_if_not_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let entries: Vec<_> = fs::read_dir(&test_dir)
        .expect("Failed to read dir")
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();

    assert!(
        !entries.contains(&".".to_string()),
        "'.' should not be listed"
    );
    assert!(
        !entries.contains(&"..".to_string()),
        "'..' should not be listed"
    );

    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_large_file_multiblock_write() {
    skip_if_not_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("large.txt");

    let mut file = File::create(&file_path).expect("Failed to create file");
    let large_content = vec![0u8; 1024 * 1024];
    file.write_all(&large_content)
        .expect("Failed to write large content");
    drop(file);

    let metadata = fs::metadata(&file_path).expect("Failed to get metadata");
    assert_eq!(metadata.len(), 1024 * 1024, "File size should be 1MB");

    let mut file = File::open(&file_path).expect("Failed to open file");
    let mut read_content = vec![0u8; 1024 * 1024];
    file.read_exact(&mut read_content)
        .expect("Failed to read large content");
    assert_eq!(read_content, large_content, "Content mismatch");

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_unlink_file() {
    skip_if_not_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("unlink.txt");

    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"Unlink test").expect("Failed to write");
    drop(file);

    assert!(file_path.exists(), "File should exist");

    fs::remove_file(&file_path).expect("Failed to unlink file");

    assert!(!file_path.exists(), "File should not exist after unlink");

    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}
