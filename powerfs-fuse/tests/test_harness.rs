use std::env;
use std::fs;
use std::io::{self};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command};
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

const FUSE_MOUNT: &str = "/tmp/powerfs-posix-test";
const TEST_DATA_DIR: &str = "/tmp/powerfs-test-data";

struct TestEnvironment {
    master_process: Child,
    volume_process: Child,
    fuse_process: Child,
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        let _ = Command::new("fusermount")
            .arg("-u")
            .arg(FUSE_MOUNT)
            .status();

        let _ = self.fuse_process.kill();
        let _ = self.volume_process.kill();
        let _ = self.master_process.kill();

        let _ = fs::remove_dir_all(TEST_DATA_DIR);
        let _ = fs::remove_dir_all(FUSE_MOUNT);
    }
}

static TEST_ENV: OnceLock<TestEnvironment> = OnceLock::new();

fn find_target_dir() -> Option<String> {
    env::current_exe()
        .ok()?
        .parent()?
        .parent()?
        .to_str()
        .map(|s| s.to_string())
}

fn get_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .map(|listener| listener.local_addr().unwrap().port())
        .unwrap_or_else(|_| 15000 + rand::random::<u16>() % 10000)
}

fn is_port_open(addr: &str) -> bool {
    match std::net::TcpStream::connect_timeout(
        &addr.parse().unwrap(),
        Duration::from_millis(100),
    ) {
        Ok(_) => true,
        Err(_) => false,
    }
}

fn is_fuse_available() -> bool {
    Path::new("/dev/fuse").exists()
        && Command::new("fusermount")
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
            match fs::metadata(mount_path) {
                Ok(m) => {
                    if m.is_dir() {
                        return true;
                    }
                }
                Err(_) => {}
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
        .spawn()
}

pub fn ensure_fuse_mounted() {
    if !is_fuse_available() {
        eprintln!("FUSE not available, skipping tests");
        std::process::exit(0);
    }

    TEST_ENV.get_or_init(|| {
        let target_dir = find_target_dir().expect("Cannot find target directory");

        let master_port = get_free_port();
        let volume_port = get_free_port();
        let master_addr = format!("127.0.0.1:{}", master_port);
        let volume_addr = format!("127.0.0.1:{}", volume_port);

        let _ = fs::remove_dir_all(TEST_DATA_DIR);
        let _ = fs::remove_dir_all(FUSE_MOUNT);
        let _ = fs::create_dir_all(TEST_DATA_DIR);

        let master_process = spawn_master(&target_dir, master_port).expect("Failed to start master");
        eprintln!("Started master on {}", master_addr);

        eprintln!("Waiting for master to be ready...");
        assert!(
            wait_for_port(&master_addr, 30),
            "Master did not start in time"
        );
        eprintln!("Master is ready");

        let volume_process = spawn_volume(&target_dir, volume_port, &master_addr)
            .expect("Failed to start volume");
        eprintln!("Started volume on {}", volume_addr);

        thread::sleep(Duration::from_secs(3));

        let fuse_process = spawn_fuse(&target_dir, &master_addr).expect("Failed to start fuse");
        eprintln!("Started fuse");

        eprintln!("Waiting for FUSE mount...");
        assert!(
            wait_for_mount(FUSE_MOUNT, 30),
            "FUSE did not mount in time"
        );

        eprintln!("FUSE mounted at {}", FUSE_MOUNT);

        TestEnvironment {
            master_process,
            volume_process,
            fuse_process,
        }
    });
}

