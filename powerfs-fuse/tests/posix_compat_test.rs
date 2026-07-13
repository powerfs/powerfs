//! POSIX 兼容性测试：验证 Unix 工具（ls/cp/rm/mv/cat/find/grep）与 PowerFS 兼容
//!
//! 这些测试通过调用标准 Unix 命令行工具来验证 PowerFS 的 POSIX 兼容性。
//! 所有测试必须在 PowerFS FUSE 挂载点上运行。

use std::fs;
use std::path::Path;
use std::process::Command;

fn get_mount_path() -> String {
    std::env::var("POWERFS_MOUNT").unwrap_or_else(|_| "/tmp/powerfs-test".to_string())
}

fn get_test_dir_name() -> String {
    format!("posix_test_{}", std::process::id())
}

fn assert_powerfs_mounted() {
    let mount_path = get_mount_path();
    if let Ok(content) = std::fs::read_to_string("/proc/mounts") {
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 && parts[1] == mount_path {
                let fstype = parts[2];
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

fn run_cmd(cmd: &str, args: &[&str]) -> (bool, String, String) {
    let output = Command::new(cmd)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("Failed to execute {}: {}", cmd, e));
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

#[test]
fn test_ls_command() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    // Create files
    let file1 = test_dir.join("file1.txt");
    let file2 = test_dir.join("file2.txt");
    fs::write(&file1, "content1").expect("Failed to write file1");
    fs::write(&file2, "content2").expect("Failed to write file2");

    // Run ls
    let (success, stdout, _stderr) = run_cmd("ls", &[test_dir.to_str().unwrap()]);
    assert!(success, "ls command should succeed");

    // ls output should contain both files
    assert!(
        stdout.contains("file1.txt"),
        "ls output should contain file1.txt: {}",
        stdout
    );
    assert!(
        stdout.contains("file2.txt"),
        "ls output should contain file2.txt: {}",
        stdout
    );

    // Test ls -la (long format with hidden files)
    let (success, stdout, _stderr) = run_cmd("ls", &["-la", test_dir.to_str().unwrap()]);
    assert!(success, "ls -la command should succeed");
    assert!(
        stdout.contains("file1.txt") && stdout.contains("file2.txt"),
        "ls -la output should list both files"
    );

    // Cleanup
    fs::remove_file(&file1).expect("Failed to remove file1");
    fs::remove_file(&file2).expect("Failed to remove file2");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_cp_command() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let src_file = test_dir.join("src.txt");
    let dst_file = test_dir.join("dst.txt");
    fs::write(&src_file, "copy this content").expect("Failed to write src file");

    // Run cp
    let (success, _stdout, _stderr) = run_cmd(
        "cp",
        &[src_file.to_str().unwrap(), dst_file.to_str().unwrap()],
    );
    assert!(success, "cp command should succeed");

    // Verify destination file exists and has correct content
    assert!(dst_file.exists(), "Destination file should exist after cp");

    let content = fs::read_to_string(&dst_file).expect("Failed to read dst file");
    assert_eq!(content, "copy this content", "Copied file content mismatch");

    // Cleanup
    fs::remove_file(&src_file).expect("Failed to remove src file");
    fs::remove_file(&dst_file).expect("Failed to remove dst file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_rm_command() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("rm_test.txt");
    fs::write(&file_path, "remove me").expect("Failed to write file");

    assert!(file_path.exists(), "File should exist before rm");

    // Run rm
    let (success, _stdout, _stderr) = run_cmd("rm", &[file_path.to_str().unwrap()]);
    assert!(success, "rm command should succeed");

    assert!(!file_path.exists(), "File should not exist after rm");

    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_mv_command() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let src_file = test_dir.join("mv_src.txt");
    let dst_file = test_dir.join("mv_dst.txt");
    fs::write(&src_file, "move this content").expect("Failed to write src file");

    // Run mv
    let (success, _stdout, _stderr) = run_cmd(
        "mv",
        &[src_file.to_str().unwrap(), dst_file.to_str().unwrap()],
    );
    assert!(success, "mv command should succeed");

    assert!(!src_file.exists(), "Source should not exist after mv");
    assert!(dst_file.exists(), "Destination should exist after mv");

    let content = fs::read_to_string(&dst_file).expect("Failed to read dst file");
    assert_eq!(content, "move this content", "Moved file content mismatch");

    // Cleanup
    fs::remove_file(&dst_file).expect("Failed to remove dst file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_cat_command() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("cat_test.txt");
    let content = "Hello from cat test!\nLine 2\nLine 3";
    fs::write(&file_path, content).expect("Failed to write file");

    // Run cat
    let (success, stdout, _stderr) = run_cmd("cat", &[file_path.to_str().unwrap()]);
    assert!(success, "cat command should succeed");
    assert_eq!(
        stdout.trim(),
        content.trim(),
        "cat output should match file content"
    );

    // Cleanup
    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_find_command() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    // Create nested structure
    let subdir1 = test_dir.join("subdir1");
    let subdir2 = subdir1.join("subdir2");
    fs::create_dir_all(&subdir2).expect("Failed to create nested dirs");

    let file1 = test_dir.join("find_me.txt");
    let file2 = subdir1.join("find_me.txt");
    let file3 = subdir2.join("find_me.txt");
    fs::write(&file1, "1").expect("Failed to write file1");
    fs::write(&file2, "2").expect("Failed to write file2");
    fs::write(&file3, "3").expect("Failed to write file3");

    // Run find -name find_me.txt
    let (success, stdout, _stderr) = run_cmd(
        "find",
        &[test_dir.to_str().unwrap(), "-name", "find_me.txt"],
    );
    assert!(success, "find command should succeed");

    // Should find all 3 files
    let count = stdout.matches("find_me.txt").count();
    assert_eq!(
        count, 3,
        "find should locate 3 files named find_me.txt, found {}: {}",
        count, stdout
    );

    // Cleanup
    fs::remove_file(&file1).expect("Failed to remove file1");
    fs::remove_file(&file2).expect("Failed to remove file2");
    fs::remove_file(&file3).expect("Failed to remove file3");
    fs::remove_dir(&subdir2).expect("Failed to remove subdir2");
    fs::remove_dir(&subdir1).expect("Failed to remove subdir1");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_grep_command() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("grep_test.txt");
    let content = "line one\nSEARCH_TERM here\nline three\nSEARCH_TERM again";
    fs::write(&file_path, content).expect("Failed to write file");

    // Run grep SEARCH_TERM
    let (success, stdout, _stderr) = run_cmd("grep", &["SEARCH_TERM", file_path.to_str().unwrap()]);
    assert!(success, "grep command should succeed");

    // Should find 2 matching lines
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "grep should find 2 matching lines, found {}: {}",
        lines.len(),
        stdout
    );
    assert!(
        stdout.contains("SEARCH_TERM here"),
        "grep output should contain first match"
    );
    assert!(
        stdout.contains("SEARCH_TERM again"),
        "grep output should contain second match"
    );

    // Cleanup
    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_mkdir_rmdir_commands() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let new_dir = test_dir.join("mkdir_test");

    // Run mkdir
    let (success, _stdout, _stderr) = run_cmd("mkdir", &[new_dir.to_str().unwrap()]);
    assert!(success, "mkdir command should succeed");
    assert!(new_dir.exists(), "Directory should exist after mkdir");
    assert!(new_dir.is_dir(), "Path should be a directory");

    // Run rmdir
    let (success, _stdout, _stderr) = run_cmd("rmdir", &[new_dir.to_str().unwrap()]);
    assert!(success, "rmdir command should succeed");
    assert!(!new_dir.exists(), "Directory should not exist after rmdir");

    // Test mkdir -p (recursive)
    let deep_dir = test_dir.join("a").join("b").join("c");
    let (success, _stdout, _stderr) = run_cmd("mkdir", &["-p", deep_dir.to_str().unwrap()]);
    assert!(success, "mkdir -p command should succeed");
    assert!(
        deep_dir.exists(),
        "Deep directory should exist after mkdir -p"
    );

    // Cleanup
    fs::remove_dir(&deep_dir).expect("Failed to remove deep dir");
    fs::remove_dir(test_dir.join("a").join("b")).expect("Failed to remove b");
    fs::remove_dir(test_dir.join("a")).expect("Failed to remove a");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_stat_command() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("stat_test.txt");
    fs::write(&file_path, "stat content").expect("Failed to write file");

    // Run stat
    let (success, stdout, _stderr) = run_cmd("stat", &[file_path.to_str().unwrap()]);
    assert!(success, "stat command should succeed");

    // stat output should contain file size and file name
    assert!(
        stdout.contains("stat_test.txt"),
        "stat output should contain file name: {}",
        stdout
    );
    assert!(
        stdout.contains("13") || stdout.contains("Size: 13"),
        "stat output should contain file size (13): {}",
        stdout
    );

    // Cleanup
    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_touch_and_echo_command() {
    assert_powerfs_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("touch_test.txt");

    // Run touch to create empty file
    let (success, _stdout, _stderr) = run_cmd("touch", &[file_path.to_str().unwrap()]);
    assert!(success, "touch command should succeed");
    assert!(file_path.exists(), "File should exist after touch");

    // Verify it's empty
    let metadata = fs::metadata(&file_path).expect("Failed to get metadata");
    assert_eq!(metadata.len(), 0, "Touched file should be empty");

    // Use echo to write content (via shell redirection)
    let output = Command::new("bash")
        .arg("-c")
        .arg(format!(
            "echo 'echo content' > {}",
            file_path.to_str().unwrap()
        ))
        .output()
        .unwrap_or_else(|e| panic!("Failed to execute echo: {}", e));

    assert!(output.status.success(), "echo command should succeed");

    // Verify content
    let content = fs::read_to_string(&file_path).expect("Failed to read file");
    assert_eq!(
        content.trim(),
        "echo content",
        "File content should match echo output"
    );

    // Cleanup
    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}
