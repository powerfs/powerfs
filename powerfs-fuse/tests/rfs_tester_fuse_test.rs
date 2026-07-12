use rfs_tester::FsTester;
use std::env;
use std::fs;
use std::path::Path;

fn get_mount_path() -> String {
    env::var("POWERFS_MOUNT").unwrap_or_else(|_| "/mnt/powerfs".to_string())
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

fn setup() {
    assert_powerfs_mounted();
}

#[test]
fn test_rfs_tester_fuse_basic_operations() {
    setup();

    let mount_path = get_mount_path();

    const YAML_CONFIG: &str = r#"
    - !directory
        name: rfs_test_basic
        content:
          - !file
              name: hello.txt
              content:
                !inline_text "Hello from rfs_tester!"
          - !directory
              name: subdir
              content:
                - !file
                    name: nested.txt
                    content:
                      !inline_text "Nested file content"
    "#;

    let tester = FsTester::new(YAML_CONFIG, &mount_path).expect("Incorrect configuration");
    tester.perform_fs_test(|dirname| {
        let hello_path = Path::new(dirname).join("hello.txt");
        let content = fs::read_to_string(&hello_path)?;
        assert_eq!(content, "Hello from rfs_tester!");

        let nested_path = Path::new(dirname).join("subdir").join("nested.txt");
        let nested_content = fs::read_to_string(&nested_path)?;
        assert_eq!(nested_content, "Nested file content");

        let entries: Vec<_> = fs::read_dir(dirname)?
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(entries.contains(&"hello.txt".to_string()));
        assert!(entries.contains(&"subdir".to_string()));

        Ok(())
    });
}

#[test]
fn test_rfs_tester_fuse_file_creation_and_deletion() {
    setup();

    let mount_path = get_mount_path();

    const YAML_CONFIG: &str = r#"
    - !directory
        name: rfs_test_create_delete
        content:
          - !file
              name: to_delete.txt
              content:
                !inline_text "Will be deleted"
    "#;

    let tester = FsTester::new(YAML_CONFIG, &mount_path).expect("Incorrect configuration");
    tester.perform_fs_test(|dirname| {
        let file_path = Path::new(dirname).join("to_delete.txt");
        assert!(file_path.exists());

        fs::remove_file(&file_path)?;
        assert!(!file_path.exists());

        Ok(())
    });
}

#[test]
fn test_rfs_tester_fuse_directory_operations() {
    setup();

    let mount_path = get_mount_path();

    const YAML_CONFIG: &str = r#"
    - !directory
        name: rfs_test_dir_ops
        content: []
    "#;

    let tester = FsTester::new(YAML_CONFIG, &mount_path).expect("Incorrect configuration");
    tester.perform_fs_test(|dirname| {
        let new_dir = Path::new(dirname).join("new_subdir");
        fs::create_dir(&new_dir)?;
        assert!(new_dir.exists());
        assert!(new_dir.is_dir());

        let file_in_new_dir = new_dir.join("file.txt");
        fs::write(&file_in_new_dir, "Content in new dir")?;
        assert!(file_in_new_dir.exists());

        let entries: Vec<_> = fs::read_dir(dirname)?
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(entries.contains(&"new_subdir".to_string()));

        Ok(())
    });
}

#[test]
fn test_rfs_tester_fuse_file_content_types() {
    setup();

    let mount_path = get_mount_path();

    const YAML_CONFIG: &str = r#"
    - !directory
        name: rfs_test_content_types
        content:
          - !file
              name: text_file.txt
              content:
                !inline_text "Plain text content"
          - !file
              name: binary_file.bin
              content:
                !inline_bytes [0x48, 0x45, 0x4C, 0x4C, 0x4F]
          - !file
              name: empty_file.txt
              content: empty
    "#;

    let tester = FsTester::new(YAML_CONFIG, &mount_path).expect("Incorrect configuration");
    tester.perform_fs_test(|dirname| {
        let text_content = fs::read_to_string(Path::new(dirname).join("text_file.txt"))?;
        assert_eq!(text_content, "Plain text content");

        let binary_content = fs::read(Path::new(dirname).join("binary_file.bin"))?;
        assert_eq!(binary_content, b"HELLO");

        let empty_content = fs::read(Path::new(dirname).join("empty_file.txt"))?;
        assert_eq!(empty_content, b"");

        Ok(())
    });
}
