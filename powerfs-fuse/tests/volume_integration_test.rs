use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

fn get_mount_path() -> String {
    env::var("POWERFS_MOUNT").unwrap_or_else(|_| "/mnt/powerfs".to_string())
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

fn docker_exec(cmd: &str) -> String {
    let output = Command::new("docker")
        .args(["exec", "powerfs-test-volume", "sh", "-c", cmd])
        .output()
        .expect("Failed to execute docker command");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn container_exists(name: &str) -> bool {
    let output = Command::new("docker")
        .args(["inspect", "-f", "{{.State.Running}}", name])
        .output()
        .expect("Failed to check container");
    String::from_utf8_lossy(&output.stdout).trim() == "true"
}

fn restart_fuse() {
    let _ = Command::new("docker")
        .args([
            "compose",
            "-f",
            "/home/portion/powerfs/docker/docker-compose.test.yml",
            "restart",
            "fuse-test",
        ])
        .output();
    std::thread::sleep(Duration::from_secs(5));
}

#[test]
fn test_data_persists_across_fuse_restart() {
    skip_if_not_mounted();
    let mount_path = get_mount_path();

    let test_file = Path::new(&mount_path).join("persistence_test.txt");
    let expected_content = "Data that should persist in Volume";

    fs::write(&test_file, expected_content).expect("Failed to write test file");

    let before_restart = fs::read_to_string(&test_file).expect("Failed to read before restart");
    assert_eq!(
        before_restart, expected_content,
        "Content mismatch before restart"
    );

    restart_fuse();

    let after_restart = fs::read_to_string(&test_file).expect("Failed to read after restart");
    assert_eq!(
        after_restart, expected_content,
        "Data lost after FUSE restart! Volume not working."
    );
}

#[test]
fn test_volume_has_data_files() {
    if !container_exists("powerfs-test-volume") {
        eprintln!("Skipping: powerfs-test-volume container not running");
        return;
    }

    let volume_dirs = docker_exec("ls -la /data/");
    assert!(
        volume_dirs.contains("volume_"),
        "Volume directories not found"
    );

    let volume1_files = docker_exec("ls -la /data/volume_1/");
    assert!(volume1_files.contains("data"), "Volume data file not found");

    let volume1_index = docker_exec("ls -la /data/volume_1/index/");
    assert!(volume1_index.contains("index"), "Volume index not found");
}

#[test]
fn test_write_and_read_cycle() {
    skip_if_not_mounted();
    let mount_path = get_mount_path();

    let test_file = Path::new(&mount_path).join("write_read_test.txt");

    fs::write(&test_file, "initial content").expect("Failed to write");

    let content = fs::read_to_string(&test_file).expect("Failed to read");
    assert_eq!(content, "initial content");

    fs::write(&test_file, "updated content").expect("Failed to update");

    let updated = fs::read_to_string(&test_file).expect("Failed to read updated");
    assert_eq!(updated, "updated content");

    fs::remove_file(&test_file).expect("Failed to delete");
    assert!(!test_file.exists(), "File should be deleted");
}
