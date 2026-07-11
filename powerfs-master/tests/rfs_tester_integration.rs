use powerfs_master::directory_tree::DirectoryTree;
use rfs_tester::FsTester;
use std::fs;
use tempfile::TempDir;

fn setup_tree() -> (DirectoryTree, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let tree = DirectoryTree::new(temp_dir.path()).unwrap();
    tree.init_root().unwrap();
    (tree, temp_dir)
}

#[test]
fn test_rfs_tester_directory_tree_basic() {
    const YAML_CONFIG: &str = r#"
    - !directory
        name: test_root
        content:
          - !file
              name: test.txt
              content:
                !inline_text "Hello, PowerFS!"
          - !directory
              name: subdir
              content:
                - !file
                    name: nested.txt
                    content:
                      !inline_text "Nested content"
    "#;

    let tester = FsTester::new(YAML_CONFIG, ".").expect("Incorrect configuration");
    tester.perform_fs_test(|dirname| {
        let test_txt_path = std::path::PathBuf::from(dirname).join("test.txt");
        let test_txt_content = fs::read_to_string(&test_txt_path)?;
        assert_eq!(test_txt_content, "Hello, PowerFS!");

        let nested_txt_path = std::path::PathBuf::from(dirname)
            .join("subdir")
            .join("nested.txt");
        let nested_txt_content = fs::read_to_string(&nested_txt_path)?;
        assert_eq!(nested_txt_content, "Nested content");

        Ok(())
    });
}

#[test]
fn test_rfs_tester_file_content_types() {
    const YAML_CONFIG: &str = r#"
    - !directory
        name: content_types
        content:
          - !file
              name: inline_text.txt
              content:
                !inline_text "This is inline text"
          - !file
              name: inline_bytes.txt
              content:
                !inline_bytes [72, 101, 108, 108, 111]
          - !file
              name: empty.txt
              content: empty
    "#;

    let tester = FsTester::new(YAML_CONFIG, ".").expect("Incorrect configuration");
    tester.perform_fs_test(|dirname| {
        let inline_text_content =
            fs::read_to_string(std::path::PathBuf::from(dirname).join("inline_text.txt"))?;
        assert_eq!(inline_text_content, "This is inline text");

        let inline_bytes_content =
            fs::read(std::path::PathBuf::from(dirname).join("inline_bytes.txt"))?;
        assert_eq!(inline_bytes_content, b"Hello");

        let empty_content = fs::read(std::path::PathBuf::from(dirname).join("empty.txt"))?;
        assert_eq!(empty_content, b"");

        Ok(())
    });
}

#[test]
fn test_rfs_tester_empty_directory() {
    const YAML_CONFIG: &str = r#"
    - !directory
        name: empty_test
        content: []
    "#;

    let tester = FsTester::new(YAML_CONFIG, ".").expect("Incorrect configuration");
    tester.perform_fs_test(|dirname| {
        let entries: Vec<_> = fs::read_dir(dirname)?.collect();
        assert_eq!(entries.len(), 0);

        Ok(())
    });
}

#[test]
fn test_rfs_tester_with_directory_tree() {
    const YAML_CONFIG: &str = r#"
    - !directory
        content:
          - !file
              name: test_file.txt
              content:
                !inline_text "Test content"
          - !directory
              name: test_dir
              content:
                - !file
                    name: nested_file.txt
                    content:
                      !inline_text "Nested content"
    "#;

    let (tree, _temp_dir) = setup_tree();

    let tester = FsTester::new(YAML_CONFIG, ".").expect("Incorrect configuration");
    tester.perform_fs_test(|dirname| {
        let test_file_path = std::path::PathBuf::from(dirname).join("test_file.txt");
        let content = fs::read_to_string(&test_file_path)?;

        let entry = powerfs_master::proto::Entry {
            name: "test_file.txt".to_string(),
            directory: "/".to_string(),
            attributes: Some(powerfs_master::proto::FuseAttributes {
                ino: 0,
                mode: 0o100644,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                size: content.len() as u64,
                blksize: 4096,
                blocks: 1,
                atime: 0,
                mtime: 0,
                ctime: 0,
                crtime: 0,
                perm: 0o644,
            }),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            extended: std::collections::HashMap::new(),
            content_size: content.len() as u64,
            disk_size: content.len() as u64,
            ttl: String::new(),
            symlink_target: String::new(),
            owner: String::new(),
            generation: 0,
        };
        tree.create_entry(entry, "rfs_tester").unwrap();

        assert!(tree.get_entry("/test_file.txt").is_some());

        let entries = tree.list_entries(1, 10, "");
        assert!(entries.iter().any(|e| e.name == "test_file.txt"));

        Ok(())
    });
}
