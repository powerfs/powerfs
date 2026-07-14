use log::info;
use std::env;
use std::fs;

fn get_mount_path() -> String {
    env::var("POWERFS_MOUNT").unwrap_or_else(|_| "/mnt/powerfs".to_string())
}

fn is_powerfs_mounted(mount_path: &str) -> bool {
    if !fs::metadata(mount_path)
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        return false;
    }

    match fs::read_to_string("/proc/mounts") {
        Ok(content) => {
            for line in content.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 3 && parts[1] == mount_path {
                    let fstype = parts[2];
                    info!("Found mount: {} type={}", mount_path, fstype);
                    // 接受 "fuse"、"fuse.powerfs-fuse" 以及任何 "fuse.*" 形式
                    return fstype == "fuse"
                        || fstype == "fuse.powerfs-fuse"
                        || fstype.starts_with("fuse.");
                }
            }
            false
        }
        Err(_) => false,
    }
}

fn skip_if_not_mounted() {
    if !is_powerfs_mounted(&get_mount_path()) {
        eprintln!(
            "Skipping test: PowerFS not mounted at '{}'",
            get_mount_path()
        );
        std::process::exit(0);
    }
}

#[test]
fn test_mount_point_is_powerfs() {
    skip_if_not_mounted();
    let mount_path = get_mount_path();
    assert!(
        is_powerfs_mounted(&mount_path),
        "Mount path '{}' is not a PowerFS FUSE mount. Tests will run against local filesystem instead!",
        mount_path
    );
}

#[test]
fn test_directory_operations_go_through_fuse() {
    skip_if_not_mounted();
    let mount_path = get_mount_path();

    assert!(
        is_powerfs_mounted(&mount_path),
        "Mount path '{}' is not a PowerFS FUSE mount!",
        mount_path
    );

    let test_dir_name = format!("mount_verify_test_{}", std::process::id());
    let test_dir = std::path::Path::new(&mount_path).join(test_dir_name);

    fs::create_dir(&test_dir).expect("Failed to create test directory");
    assert!(test_dir.exists(), "Test directory should exist");

    let file_path = test_dir.join("verify.txt");
    fs::write(&file_path, "verification content").expect("Failed to write verification file");
    assert!(file_path.exists(), "Verification file should exist");

    let content = fs::read_to_string(&file_path).expect("Failed to read verification file");
    assert_eq!(content, "verification content", "Content mismatch");

    fs::remove_file(&file_path).expect("Failed to remove verification file");
    fs::remove_dir(&test_dir).expect("Failed to remove test directory");

    assert!(!test_dir.exists(), "Test directory should not exist");
}
