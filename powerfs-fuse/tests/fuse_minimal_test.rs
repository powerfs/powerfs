use std::fs::{self, File};
use std::path::Path;

fn get_mount_path() -> String {
    std::env::var("POWERFS_MOUNT").unwrap_or_else(|_| "/tmp/powerfs-test".to_string())
}

#[test]
fn test_minimal_create_open() {
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join("minimal_test");
    
    fs::create_dir_all(&test_dir).expect("Failed to create test dir");
    
    let file_path = test_dir.join("minimal.txt");
    
    let _file = File::create(&file_path).expect("Failed to create file");
    
    assert!(file_path.exists(), "File should exist");
    
    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}