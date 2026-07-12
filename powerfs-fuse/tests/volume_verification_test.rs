use std::env;
use std::fs;
use std::path::Path;

fn get_mount_path() -> String {
    env::var("POWERFS_MOUNT").unwrap_or_else(|_| "/tmp/powerfs-test/mount".to_string())
}

#[test]
fn test_write_immediate_read() {
    let mount_path = get_mount_path();

    let test_file = Path::new(&mount_path).join("immediate_test.txt");
    let content = "Immediate test content";

    fs::write(&test_file, content).expect("Failed to write");

    let read_content = fs::read_to_string(&test_file).expect("Failed to read");
    assert_eq!(
        read_content, content,
        "Content should be readable immediately after write"
    );
}

#[test]
fn test_write_flush_read() {
    let mount_path = get_mount_path();

    let test_file = Path::new(&mount_path).join("flush_test.txt");
    let content = "Flush test content";

    {
        let mut file = fs::File::create(&test_file).expect("Failed to create");
        use std::io::Write;
        file.write_all(content.as_bytes()).expect("Failed to write");
        file.flush().expect("Failed to flush");
    }

    let read_content = fs::read_to_string(&test_file).expect("Failed to read");
    assert_eq!(
        read_content, content,
        "Content should be readable after flush"
    );
}

#[test]
fn test_file_size_after_write() {
    let mount_path = get_mount_path();

    let test_file = Path::new(&mount_path).join("size_test.txt");

    let _ = fs::remove_file(&test_file);

    let content = "Size test";

    fs::write(&test_file, content).expect("Failed to write");

    let metadata = fs::metadata(&test_file).expect("Failed to get metadata");
    assert_eq!(
        metadata.len(),
        content.len() as u64,
        "File size should match content length"
    );
}
