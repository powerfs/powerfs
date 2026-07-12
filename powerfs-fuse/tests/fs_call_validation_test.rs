use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[path = "test_harness.rs"]
mod test_harness;

fn get_mount_path() -> String {
    test_harness::get_fuse_mount()
}

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn get_test_dir_name() -> String {
    let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("test_{}_{}", std::process::id(), id)
}

fn ensure_fuse_mounted() {
    if let Err(e) = test_harness::ensure_fuse_mounted() {
        eprintln!("Skipping test: {}", e);
        std::process::exit(0);
    }
}

#[test]
fn test_open_readonly_existing_file() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("readonly.txt");
    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"test content").expect("Failed to write");
    drop(file);

    let mut file = OpenOptions::new()
        .read(true)
        .open(&file_path)
        .expect("Failed to open file in read-only mode");

    let mut content = String::new();
    file.read_to_string(&mut content).expect("Failed to read");
    assert_eq!(content, "test content");

    assert!(
        file.write_all(b"should fail").is_err(),
        "Write to read-only file should fail"
    );

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_open_writeonly_existing_file() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("writeonly.txt");
    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"original").expect("Failed to write");
    drop(file);

    let mut file = OpenOptions::new()
        .write(true)
        .open(&file_path)
        .expect("Failed to open file in write-only mode");

    file.write_all(b"overwritten").expect("Failed to write");
    drop(file);

    let mut content = String::new();
    File::open(&file_path)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "overwritten");

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_open_readwrite_existing_file() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("readwrite.txt");
    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"hello").expect("Failed to write");
    drop(file);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .expect("Failed to open file in read-write mode");

    let mut content = String::new();
    file.read_to_string(&mut content).expect("Failed to read");
    assert_eq!(content, "hello");

    file.write_all(b" world").expect("Failed to write");
    drop(file);

    let mut content = String::new();
    File::open(&file_path)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "hello world");

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_open_nonexistent_file_fails() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("nonexistent.txt");
    assert!(
        File::open(&file_path).is_err(),
        "Opening nonexistent file should fail"
    );

    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_open_creat_new_file() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("creat_new.txt");
    assert!(!file_path.exists(), "File should not exist before create");

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&file_path)
        .expect("Failed to create new file");

    file.write_all(b"created").expect("Failed to write");
    drop(file);

    assert!(file_path.exists(), "File should exist after create");

    let mut content = String::new();
    File::open(&file_path)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "created");

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_open_creat_existing_file() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("creat_existing.txt");
    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"original").expect("Failed to write");
    drop(file);

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&file_path)
        .expect("Failed to open existing file with create");

    file.write_all(b"new content").expect("Failed to write");
    file.sync_all().expect("Failed to fsync");
    drop(file);

    let mut content = String::new();
    File::open(&file_path)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "new content");

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_open_creat_excl_nonexistent() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("creat_excl_new.txt");
    assert!(!file_path.exists(), "File should not exist");

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&file_path)
        .expect("Failed to create exclusive new file");

    file.write_all(b"exclusive").expect("Failed to write");
    drop(file);

    assert!(file_path.exists(), "File should exist after create_new");

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_open_creat_excl_existing_fails() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("creat_excl_existing.txt");
    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"existing").expect("Failed to write");
    drop(file);

    assert!(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&file_path)
            .is_err(),
        "create_new on existing file should fail"
    );

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_open_no_permission_fails() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("no_perm.txt");
    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"secret").expect("Failed to write");
    drop(file);

    fs::set_permissions(&file_path, fs::Permissions::from_mode(0o000))
        .expect("Failed to set permissions");

    assert!(
        File::open(&file_path).is_err(),
        "Opening file with no permissions should fail"
    );

    fs::set_permissions(&file_path, fs::Permissions::from_mode(0o644))
        .expect("Failed to restore permissions");
    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_open_directory_fails() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let subdir = test_dir.join("subdir");
    fs::create_dir(&subdir).expect("Failed to create subdir");

    use std::os::unix::ffi::OsStrExt;
    let c_str = std::ffi::CString::new(subdir.as_os_str().as_bytes()).unwrap();
    let fd = unsafe { libc::open(c_str.as_ptr(), libc::O_RDWR) };
    eprintln!("libc::open directory with O_RDWR result: fd={}", fd);
    assert!(fd < 0, "Opening directory with O_RDWR should fail");
    let err = unsafe { *libc::__errno_location() };
    eprintln!("errno: {}", err);
    assert_eq!(err, libc::EISDIR, "Should return EISDIR error");

    fs::remove_dir(&subdir).expect("Failed to remove subdir");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_stat_file() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("stat.txt");
    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"test").expect("Failed to write");
    drop(file);

    let metadata = fs::metadata(&file_path).expect("Failed to get metadata");
    assert_eq!(metadata.len(), 4);
    assert!(!metadata.is_dir());
    assert!(metadata.is_file());

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_stat_directory() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let metadata = fs::metadata(&test_dir).expect("Failed to get metadata");
    assert!(metadata.is_dir());
    assert!(!metadata.is_file());

    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_stat_nonexistent_fails() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("nonexistent.txt");
    assert!(
        fs::metadata(&file_path).is_err(),
        "stat on nonexistent file should fail"
    );

    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_chmod_file() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("chmod.txt");
    let mut file = File::create(&file_path).expect("Failed to create file");
    file.write_all(b"test").expect("Failed to write");
    drop(file);

    fs::set_permissions(&file_path, fs::Permissions::from_mode(0o755)).expect("Failed to chmod");

    let metadata = fs::metadata(&file_path).expect("Failed to get metadata");
    let new_mode = metadata.permissions().mode() & 0o777;
    assert_eq!(new_mode, 0o755);

    fs::remove_file(&file_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_rename_file() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let old_path = test_dir.join("old.txt");
    let new_path = test_dir.join("new.txt");

    let mut file = File::create(&old_path).expect("Failed to create file");
    file.write_all(b"content").expect("Failed to write");
    drop(file);

    fs::rename(&old_path, &new_path).expect("Failed to rename");

    assert!(!old_path.exists(), "Old file should not exist");
    assert!(new_path.exists(), "New file should exist");

    let mut content = String::new();
    File::open(&new_path)
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "content");

    fs::remove_file(&new_path).expect("Failed to remove file");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_rename_nonexistent_fails() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let old_path = test_dir.join("nonexistent.txt");
    let new_path = test_dir.join("new.txt");

    assert!(
        fs::rename(&old_path, &new_path).is_err(),
        "rename nonexistent file should fail"
    );

    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_symlink_create_and_read() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let target_path = test_dir.join("target.txt");
    let mut file = File::create(&target_path).expect("Failed to create target");
    file.write_all(b"target content").expect("Failed to write");
    drop(file);

    let link_path = test_dir.join("link.txt");
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        symlink(&target_path, &link_path).expect("Failed to create symlink");

        assert!(link_path
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink());

        let mut content = String::new();
        File::open(&link_path)
            .unwrap()
            .read_to_string(&mut content)
            .unwrap();
        assert_eq!(content, "target content");
    }

    #[cfg(unix)]
    fs::remove_file(&link_path).expect("Failed to remove symlink");
    fs::remove_file(&target_path).expect("Failed to remove target");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_unlink_nonexistent_fails() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let file_path = test_dir.join("nonexistent.txt");
    assert!(
        fs::remove_file(&file_path).is_err(),
        "unlink nonexistent file should fail"
    );

    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_rmdir_nonexistent_fails() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let subdir = test_dir.join("nonexistent");
    assert!(
        fs::remove_dir(&subdir).is_err(),
        "rmdir nonexistent dir should fail"
    );

    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_rmdir_non_empty_fails() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let subdir = test_dir.join("non_empty");
    fs::create_dir(&subdir).expect("Failed to create subdir");

    let file_in_subdir = subdir.join("file.txt");
    File::create(&file_in_subdir).expect("Failed to create file");

    assert!(
        fs::remove_dir(&subdir).is_err(),
        "rmdir non-empty dir should fail"
    );

    fs::remove_file(&file_in_subdir).expect("Failed to remove file");
    fs::remove_dir(&subdir).expect("Failed to remove subdir");
    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}

#[test]
fn test_readdir_empty_directory() {
    ensure_fuse_mounted();
    let mount_path = get_mount_path();
    let test_dir = Path::new(&mount_path).join(get_test_dir_name());

    fs::create_dir_all(&test_dir).expect("Failed to create test dir");

    let entries: Vec<_> = fs::read_dir(&test_dir)
        .expect("Failed to read dir")
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();

    assert_eq!(entries.len(), 0, "Empty directory should have no entries");

    fs::remove_dir(&test_dir).expect("Failed to remove test dir");
}
