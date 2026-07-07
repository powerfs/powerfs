use std::env;
use std::fs;
use std::io::{self};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

const FUSE_MOUNT: &str = "/tmp/powerfs-posix-test";
const TEST_DATA_DIR: &str = "/tmp/powerfs-test-data";

#[allow(dead_code)]
struct TestEnvironment {
    master_process: Child,
    volume_process: Child,
    fuse_process: Child,
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        force_cleanup();
    }
}

static TEST_ENV: OnceLock<TestEnvironment> = OnceLock::new();

fn force_cleanup() {
    let _ = Command::new("fusermount3")
        .arg("-u")
        .arg(FUSE_MOUNT)
        .status();

    let _ = Command::new("fusermount3")
        .arg("-zu")
        .arg(FUSE_MOUNT)
        .status();

    let _ = Command::new("pkill")
        .arg("-9")
        .arg("-f")
        .arg("powerfs master")
        .status();
    let _ = Command::new("pkill")
        .arg("-9")
        .arg("-f")
        .arg("powerfs-volume")
        .status();
    let _ = Command::new("pkill")
        .arg("-9")
        .arg("-f")
        .arg("powerfs fuse")
        .status();

    thread::sleep(Duration::from_secs(2));

    let _ = fs::remove_dir_all(TEST_DATA_DIR);
}

fn register_cleanup_handler() {}

fn find_target_dir() -> Option<String> {
    if let Ok(target_dir) = env::var("CARGO_TARGET_DIR") {
        let debug_dir = Path::new(&target_dir).join("debug");
        if debug_dir.exists() {
            return debug_dir.to_str().map(|s| s.to_string());
        }
        return Some(target_dir);
    }
    if let Ok(pwd) = env::current_dir() {
        let target_debug = pwd.join("target").join("debug");
        if target_debug.exists() {
            return target_debug.to_str().map(|s| s.to_string());
        }
        let workspace_target = pwd
            .parent()
            .map(|p| p.join("target").join("debug"));
        if let Some(workspace_target) = workspace_target {
            if workspace_target.exists() {
                return workspace_target.to_str().map(|s| s.to_string());
            }
        }
    }
    None
}

fn get_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .map(|listener| listener.local_addr().unwrap().port())
        .unwrap_or_else(|_| 15000 + rand::random::<u16>() % 10000)
}

fn is_port_open(addr: &str) -> bool {
    std::net::TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_millis(100))
        .is_ok()
}

fn is_fuse_available() -> bool {
    Path::new("/dev/fuse").exists()
        && Command::new("fusermount3")
            .arg("--version")
            .status()
            .is_ok()
}

fn wait_for_port(addr: &str, timeout_secs: u64) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed().as_secs() < timeout_secs {
        if is_port_open(addr) {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

fn wait_for_mount(mount_path: &str, timeout_secs: u64) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed().as_secs() < timeout_secs {
        if Path::new(mount_path).exists() {
            if let Ok(m) = fs::metadata(mount_path) {
                if m.is_dir() {
                    return true;
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

fn spawn_master(target_dir: &str, port: u16) -> io::Result<Child> {
    let master_dir = format!("{}/master", TEST_DATA_DIR);
    let _ = fs::create_dir_all(&master_dir);

    Command::new(format!("{}/powerfs", target_dir))
        .arg("master")
        .arg("--port")
        .arg(port.to_string())
        .arg("--dir")
        .arg(&master_dir)
        .arg("--ip")
        .arg("127.0.0.1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

fn spawn_volume(target_dir: &str, port: u16, master_addr: &str) -> io::Result<Child> {
    let data_dir = format!("{}/volume1", TEST_DATA_DIR);
    let _ = fs::create_dir_all(&data_dir);

    Command::new(format!("{}/powerfs-volume", target_dir))
        .arg("--grpc-address")
        .arg(format!("127.0.0.1:{}", port))
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--master-address")
        .arg(master_addr)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

fn spawn_fuse(target_dir: &str, master_addr: &str) -> io::Result<Child> {
    let _ = fs::create_dir_all(FUSE_MOUNT);

    Command::new(format!("{}/powerfs", target_dir))
        .arg("fuse")
        .arg("--dir")
        .arg(FUSE_MOUNT)
        .arg("--master")
        .arg(master_addr)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

pub fn ensure_fuse_mounted() {
    if !is_fuse_available() {
        eprintln!("FUSE not available, skipping tests");
        std::process::exit(0);
    }

    register_cleanup_handler();

    TEST_ENV.get_or_init(|| {
        force_cleanup();

        let target_dir = find_target_dir().expect("Cannot find target directory");

        let master_port = get_free_port();
        let volume_port = get_free_port();
        let master_addr = format!("127.0.0.1:{}", master_port);

        let _ = fs::create_dir_all(TEST_DATA_DIR);

        let master_process =
            spawn_master(&target_dir, master_port).expect("Failed to start master");

        assert!(
            wait_for_port(&master_addr, 60),
            "Master did not start in time"
        );

        let volume_process =
            spawn_volume(&target_dir, volume_port, &master_addr).expect("Failed to start volume");

        thread::sleep(Duration::from_secs(3));

        let fuse_process = spawn_fuse(&target_dir, &master_addr).expect("Failed to start fuse");

        assert!(wait_for_mount(FUSE_MOUNT, 30), "FUSE did not mount in time");

        TestEnvironment {
            master_process,
            volume_process,
            fuse_process,
        }
    });
}
