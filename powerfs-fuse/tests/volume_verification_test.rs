use std::env;
use std::fs;
use std::io::Write;
use std::path::Path;

fn get_mount_path() -> String {
    env::var("POWERFS_MOUNT").unwrap_or_else(|_| "/tmp/powerfs-test/mount".to_string())
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
    if !is_powerfs_mounted() {
        eprintln!(
            "Skipping test: PowerFS not mounted at '{}'",
            get_mount_path()
        );
        std::process::exit(0);
    }
}

#[test]
fn test_write_immediate_read() {
    skip_if_not_mounted();
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
    skip_if_not_mounted();
    let mount_path = get_mount_path();

    let test_file = Path::new(&mount_path).join("flush_test.txt");
    let content = "Flush test content";

    {
        let mut file = fs::File::create(&test_file).expect("Failed to create");
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
    skip_if_not_mounted();
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
