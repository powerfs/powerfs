use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

fn get_mount_path() -> String {
    std::env::var("POWERFS_MOUNT").unwrap_or_else(|_| "/tmp/powerfs-test".to_string())
}

fn get_test_dir_name() -> String {
    format!("test_{}", std::process::id())
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
    assert_eq!(metadata.len(), 26, "File size should be 26");

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
