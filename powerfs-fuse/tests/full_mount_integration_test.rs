use std::collections::hash_map::DefaultHasher;
use std::env;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

const TEST_MOUNT: &str = "/tmp/powerfs-full-integration";
const TEST_FILE_SIZE: usize = 1024 * 1024;

fn get_mount_path() -> String {
    env::var("POWERFS_MOUNT").unwrap_or_else(|_| TEST_MOUNT.to_string())
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
                    return fstype == "fuse" || fstype.starts_with("fuse.");
                }
            }
            false
        }
        Err(_) => false,
    }
}

fn skip_if_not_mounted() -> bool {
    if env::var("POWERFS_SKIP_MOUNT_TESTS").is_ok() {
        return true;
    }
    let mount_path = get_mount_path();
    !is_powerfs_mounted(&mount_path)
}

fn get_test_unique_name(prefix: &str) -> String {
    let thread_id = thread::current().id();
    let mut hasher = DefaultHasher::new();
    thread_id.hash(&mut hasher);
    format!("{}_{}_{}", prefix, std::process::id(), hasher.finish())
}

macro_rules! skip_unless_mounted {
    () => {{
        if skip_if_not_mounted() {
            eprintln!("Skipping test: PowerFS not mounted. Set POWERFS_MOUNT to run.");
            return;
        }
    }};
}

mod basic_file_operations {
    use super::*;

    #[test]
    fn test_create_read_delete_file() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();
        let test_name = get_test_unique_name("create_read_delete");

        let file_path = Path::new(&mount_path).join(&test_name);

        if file_path.exists() {
            fs::remove_file(&file_path).unwrap();
        }

        let mut file = File::create(&file_path).expect("Failed to create file");
        file.write_all(b"Hello PowerFS!")
            .expect("Failed to write to file");
        drop(file);

        assert!(file_path.exists(), "File should exist after creation");

        let mut content = String::new();
        File::open(&file_path)
            .expect("Failed to open file")
            .read_to_string(&mut content)
            .expect("Failed to read file");
        assert_eq!(content, "Hello PowerFS!", "File content mismatch");

        fs::remove_file(&file_path).expect("Failed to remove file");
        assert!(!file_path.exists(), "File should not exist after deletion");
    }

    #[test]
    fn test_file_size_and_seek() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();
        let test_name = get_test_unique_name("seek_test");

        let file_path = Path::new(&mount_path).join(&test_name);

        if file_path.exists() {
            fs::remove_file(&file_path).unwrap();
        }

        let mut file = File::create(&file_path).expect("Failed to create file");
        let test_data = vec![0u8; TEST_FILE_SIZE];
        file.write_all(&test_data)
            .expect("Failed to write test data");
        drop(file);

        let metadata = fs::metadata(&file_path).expect("Failed to get metadata");
        assert_eq!(metadata.len(), TEST_FILE_SIZE as u64, "File size mismatch");

        let mut file = File::open(&file_path).expect("Failed to open file");
        file.seek(SeekFrom::Start(512)).expect("Failed to seek");

        let mut buf = [0u8; 100];
        file.read_exact(&mut buf)
            .expect("Failed to read after seek");
        assert_eq!(buf, [0u8; 100], "Data after seek should be zeros");

        fs::remove_file(&file_path).unwrap();
    }

    #[test]
    fn test_truncate() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();
        let test_name = get_test_unique_name("truncate_test");

        let file_path = Path::new(&mount_path).join(&test_name);

        if file_path.exists() {
            fs::remove_file(&file_path).unwrap();
        }

        let mut file = File::create(&file_path).expect("Failed to create file");
        file.write_all(b"Hello World")
            .expect("Failed to write initial content");
        drop(file);

        let file = File::options()
            .write(true)
            .open(&file_path)
            .expect("Failed to open file for truncation");
        file.set_len(5).expect("Failed to truncate file");
        drop(file);

        let mut content = String::new();
        File::open(&file_path)
            .expect("Failed to open file")
            .read_to_string(&mut content)
            .expect("Failed to read truncated file");
        assert_eq!(content, "Hello", "Truncated content mismatch");

        fs::remove_file(&file_path).unwrap();
    }
}

mod directory_operations {
    use super::*;

    #[test]
    fn test_create_read_delete_directory() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();
        let test_name = get_test_unique_name("dir_test");

        let dir_path = Path::new(&mount_path).join(&test_name);

        if dir_path.exists() {
            fs::remove_dir_all(&dir_path).unwrap();
        }

        fs::create_dir(&dir_path).expect("Failed to create directory");
        assert!(dir_path.exists(), "Directory should exist after creation");

        let file_path = dir_path.join("nested_file.txt");
        fs::write(&file_path, "test content").expect("Failed to write file in directory");
        assert!(file_path.exists(), "File should exist in directory");

        let entries: Vec<_> = fs::read_dir(&dir_path)
            .expect("Failed to read directory")
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(
            entries,
            vec!["nested_file.txt"],
            "Directory entries mismatch"
        );

        fs::remove_file(&file_path).unwrap();
        fs::remove_dir(&dir_path).unwrap();
        assert!(
            !dir_path.exists(),
            "Directory should not exist after deletion"
        );
    }

    #[test]
    fn test_nested_directories() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();
        let test_name = get_test_unique_name("nested_dir");

        let nested_path = Path::new(&mount_path)
            .join(&test_name)
            .join("level1")
            .join("level2");

        if nested_path.exists() {
            fs::remove_dir_all(nested_path.parent().unwrap().parent().unwrap()).unwrap();
        }

        fs::create_dir_all(&nested_path).expect("Failed to create nested directories");
        assert!(nested_path.exists(), "Nested directory should exist");

        let file_path = nested_path.join("deep_file.txt");
        fs::write(&file_path, "deep content").expect("Failed to write file in nested dir");

        let content = fs::read_to_string(&file_path).expect("Failed to read file");
        assert_eq!(content, "deep content", "Content in nested dir mismatch");

        fs::remove_dir_all(Path::new(&mount_path).join(&test_name)).unwrap();
    }

    #[test]
    fn test_rename_file() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();
        let test_name = get_test_unique_name("rename_test");

        let old_path = Path::new(&mount_path).join(format!("{}_old.txt", test_name));
        let new_path = Path::new(&mount_path).join(format!("{}_new.txt", test_name));

        if old_path.exists() {
            fs::remove_file(&old_path).unwrap();
        }
        if new_path.exists() {
            fs::remove_file(&new_path).unwrap();
        }

        fs::write(&old_path, "content to rename").expect("Failed to write file");
        fs::rename(&old_path, &new_path).expect("Failed to rename file");

        assert!(!old_path.exists(), "Old path should not exist");
        assert!(new_path.exists(), "New path should exist");

        let content = fs::read_to_string(&new_path).expect("Failed to read renamed file");
        assert_eq!(
            content, "content to rename",
            "Content after rename mismatch"
        );

        fs::remove_file(&new_path).unwrap();
    }
}

mod permission_and_metadata {
    use super::*;

    #[test]
    fn test_file_permissions() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();
        let test_name = get_test_unique_name("perm_test");

        let file_path = Path::new(&mount_path).join(&test_name);

        if file_path.exists() {
            fs::remove_file(&file_path).unwrap();
        }

        fs::write(&file_path, "permission test").expect("Failed to create file");

        let mut perms = fs::metadata(&file_path)
            .expect("Failed to get metadata")
            .permissions();
        perms.set_mode(0o644);
        fs::set_permissions(&file_path, perms).expect("Failed to set permissions");

        let metadata = fs::metadata(&file_path).expect("Failed to get metadata after chmod");
        assert_eq!(
            metadata.permissions().mode() & 0o777,
            0o644,
            "Permissions mismatch"
        );

        fs::remove_file(&file_path).unwrap();
    }

    #[test]
    fn test_symlink() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();
        let test_name = get_test_unique_name("symlink_test");

        let target_path = Path::new(&mount_path).join(format!("{}_target.txt", test_name));
        let link_path = Path::new(&mount_path).join(format!("{}_link", test_name));

        if target_path.exists() {
            fs::remove_file(&target_path).unwrap();
        }
        if link_path.exists() {
            fs::remove_file(&link_path).unwrap();
        }

        fs::write(&target_path, "symlink target").expect("Failed to create target file");
        std::os::unix::fs::symlink(&target_path, &link_path).expect("Failed to create symlink");

        assert!(link_path.exists(), "Symlink should exist");
        assert!(
            link_path
                .symlink_metadata()
                .unwrap()
                .file_type()
                .is_symlink(),
            "Should be symlink"
        );

        let content = fs::read_to_string(&link_path).expect("Failed to read symlink target");
        assert_eq!(content, "symlink target", "Symlink target content mismatch");

        fs::remove_file(&link_path).unwrap();
        fs::remove_file(&target_path).unwrap();
    }
}

mod concurrent_access {
    use super::*;

    #[test]
    fn test_concurrent_writes() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();
        let test_name = get_test_unique_name("concurrent_write");

        let file_path = Path::new(&mount_path).join(&test_name);

        if file_path.exists() {
            fs::remove_file(&file_path).unwrap();
        }

        let file_path = Arc::new(file_path);
        let thread_count = 4;
        let barrier = Arc::new(Barrier::new(thread_count));

        let mut handles = vec![];
        for i in 0..thread_count {
            let path = file_path.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                for j in 0..100 {
                    let mut file = File::options()
                        .create(true)
                        .append(true)
                        .open(&*path)
                        .unwrap();
                    file.write_all(format!("Thread {} Write {}\n", i, j).as_bytes())
                        .unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let content = fs::read_to_string(&*file_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(
            lines.len(),
            400,
            "Expected 400 lines from 4 threads x 100 writes"
        );

        fs::remove_file(&*file_path).unwrap();
    }

    #[test]
    fn test_concurrent_reads_while_writing() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();
        let test_name = get_test_unique_name("read_while_write");

        let file_path = Path::new(&mount_path).join(&test_name);

        if file_path.exists() {
            fs::remove_file(&file_path).unwrap();
        }

        let file_path = Arc::new(file_path);
        let barrier = Arc::new(Barrier::new(3));

        let writer = thread::spawn({
            let path = file_path.clone();
            let barrier = barrier.clone();
            move || {
                barrier.wait();
                let mut file = File::create(&*path).unwrap();
                for i in 0..1000 {
                    file.write_all(format!("Line {}\n", i).as_bytes()).unwrap();
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });

        let mut readers = vec![];
        for _ in 0..2 {
            let path = file_path.clone();
            let barrier = barrier.clone();
            readers.push(thread::spawn(move || {
                barrier.wait();
                thread::sleep(Duration::from_millis(5));
                for _ in 0..10 {
                    if let Ok(mut file) = File::open(&*path) {
                        let mut content = String::new();
                        let _ = file.read_to_string(&mut content);
                    }
                    thread::sleep(Duration::from_millis(5));
                }
            }));
        }

        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }

        let content = fs::read_to_string(&*file_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1000, "Expected 1000 lines");

        fs::remove_file(&*file_path).unwrap();
    }
}

mod stress_test {
    use super::*;

    #[test]
    fn test_large_file_write() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();
        let test_name = get_test_unique_name("large_file");

        let file_path = Path::new(&mount_path).join(&test_name);

        if file_path.exists() {
            fs::remove_file(&file_path).unwrap();
        }

        let start = std::time::Instant::now();
        let mut file = File::create(&file_path).expect("Failed to create large file");

        let mut buffer = vec![0u8; 64 * 1024];
        for i in 0..buffer.len() {
            buffer[i] = (i % 256) as u8;
        }

        for _ in 0..(TEST_FILE_SIZE / (64 * 1024)) {
            file.write_all(&buffer).expect("Failed to write chunk");
        }
        drop(file);
        let elapsed = start.elapsed();

        let metadata = fs::metadata(&file_path).expect("Failed to get metadata");
        assert_eq!(
            metadata.len(),
            TEST_FILE_SIZE as u64,
            "Large file size mismatch"
        );

        let throughput = (TEST_FILE_SIZE as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();
        println!("Large file write throughput: {:.2} MB/s", throughput);

        let mut file = File::open(&file_path).expect("Failed to open large file");
        let mut verify_buf = vec![0u8; 64 * 1024];
        for i in 0..(TEST_FILE_SIZE / (64 * 1024)) {
            file.read_exact(&mut verify_buf)
                .expect("Failed to read chunk");
            for j in 0..verify_buf.len() {
                assert_eq!(
                    verify_buf[j],
                    (j % 256) as u8,
                    "Data corruption in chunk {}",
                    i
                );
            }
        }

        fs::remove_file(&file_path).unwrap();
    }

    #[test]
    fn test_many_small_files() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();
        let test_name = get_test_unique_name("many_small_files");

        let dir_path = Path::new(&mount_path).join(&test_name);

        if dir_path.exists() {
            fs::remove_dir_all(&dir_path).unwrap();
        }

        fs::create_dir(&dir_path).expect("Failed to create test directory");

        let file_count = 100;
        for i in 0..file_count {
            let file_path = dir_path.join(format!("file_{:04}.txt", i));
            fs::write(&file_path, format!("Content for file {}", i))
                .expect("Failed to create small file");
        }

        let entries: Vec<_> = fs::read_dir(&dir_path)
            .expect("Failed to read directory")
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(
            entries.len(),
            file_count,
            "Expected {} small files",
            file_count
        );

        for i in 0..file_count {
            let file_path = dir_path.join(format!("file_{:04}.txt", i));
            let content = fs::read_to_string(&file_path).expect("Failed to read small file");
            assert_eq!(
                content,
                format!("Content for file {}", i),
                "Content mismatch for file {}",
                i
            );
        }

        fs::remove_dir_all(&dir_path).unwrap();
    }
}

mod fuse_mount_validation {
    use super::*;

    #[test]
    fn test_mount_is_fuse() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();

        let mounts = fs::read_to_string("/proc/mounts").expect("Failed to read /proc/mounts");
        let mut found = false;
        let mut fstype = String::new();

        for line in mounts.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 && parts[1] == mount_path {
                found = true;
                fstype = parts[2].to_string();
                break;
            }
        }

        assert!(found, "Mount path {} not found in /proc/mounts", mount_path);
        assert!(
            fstype == "fuse" || fstype.starts_with("fuse."),
            "Expected FUSE filesystem, got {}",
            fstype
        );
        println!("Confirmed FUSE mount: {} (type: {})", mount_path, fstype);
    }

    #[test]
    fn test_df_command_recognizes_mount() {
        skip_unless_mounted!();
        let mount_path = get_mount_path();

        let output = Command::new("df")
            .arg("-h")
            .output()
            .expect("Failed to run df command");

        let df_output = String::from_utf8_lossy(&output.stdout);
        assert!(
            df_output.contains(&mount_path),
            "df command output does not contain mount path {}",
            mount_path
        );
        println!("df output contains mount: {}", mount_path);
    }
}
