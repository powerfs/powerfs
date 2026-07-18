#[cfg(feature = "spdk")]
use powerfs_core::storage_backend::{SpdkBackend, SpdkDeviceConfig, StorageBackend};

#[cfg(feature = "spdk")]
#[tokio::main]
async fn main() {
    println!("=== SPDK Backend Test ===");

    let backend = SpdkBackend::new("test-node", Some("/var/tmp/spdk.sock"));
    match backend {
        Ok(backend) => {
            println!("SPDK environment initialized successfully");

            let devices = vec![
                SpdkDeviceConfig {
                    name: "Nvme1".to_string(),
                    transport_string: "0000:03:00.0".to_string(),
                    capacity: None,
                },
                SpdkDeviceConfig {
                    name: "Nvme2".to_string(),
                    transport_string: "0000:04:00.0".to_string(),
                    capacity: None,
                },
                SpdkDeviceConfig {
                    name: "Nvme3".to_string(),
                    transport_string: "0000:05:00.0".to_string(),
                    capacity: None,
                },
            ];

            println!("\nAttaching devices via RPC...");
            let results = backend
                .attach_devices_from_config(&devices, Some("/var/tmp/spdk.sock"))
                .await;

            for result in results {
                if result.success {
                    println!(
                        "  Device {} attached successfully: {:?}",
                        result.device_name, result.device_id
                    );
                } else {
                    println!(
                        "  Device {} attach failed: {:?}",
                        result.device_name, result.error
                    );
                }
            }

            let all_devices = backend.list_devices().unwrap();
            println!("\nTotal devices: {}", all_devices.len());

            if !all_devices.is_empty() {
                let volume_size = 1024 * 1024 * 100;

                println!("\n=== Testing Volume Operations ===");

                match backend.allocate_volume(1, volume_size, None) {
                    Ok(result) => {
                        println!("Volume 1 allocated on device: {}", result.device_id);
                        println!(
                            "Allocated size: {} MB",
                            result.allocated_size / (1024 * 1024)
                        );

                        let test_data = vec![0xAAu8; 1024];
                        match backend.write_needle(1, 0, &test_data) {
                            Ok(size) => println!("Wrote {} bytes to volume 1", size),
                            Err(e) => println!("Write failed: {}", e),
                        }

                        match backend.read_needle(1, 0, 1024) {
                            Ok(data) => {
                                if data == test_data {
                                    println!("Read data matches (1024 bytes)");
                                } else {
                                    println!("Read data mismatch!");
                                }
                            }
                            Err(e) => println!("Read failed: {}", e),
                        }

                        match backend.sync_volume(1) {
                            Ok(_) => println!("Volume 1 synced"),
                            Err(e) => println!("Sync failed: {}", e),
                        }

                        match backend.delete_volume(1) {
                            Ok(_) => println!("Volume 1 deleted"),
                            Err(e) => println!("Delete failed: {}", e),
                        }
                    }
                    Err(e) => {
                        println!("Failed to allocate volume: {}", e);
                    }
                }
            }

            println!("\n=== Test Completed ===");
        }
        Err(e) => {
            println!("Failed to initialize SPDK backend: {}", e);
            println!("Possible reasons:");
            println!("1. Need root privileges (sudo)");
            println!("2. NVMe devices may be in use by kernel driver");
            println!("3. Need to unbind devices from nvme driver first");
        }
    }
}

#[cfg(not(feature = "spdk"))]
fn main() {
    println!("SPDK feature not enabled. Use --features spdk to build.");
}
