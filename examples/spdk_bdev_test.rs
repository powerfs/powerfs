#[cfg(feature = "spdk")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use powerfs_core::storage_backend::{SpdkBackend, StorageBackend};

    println!("=== SPDK BDEV Test for NVMe Devices ===");
    println!();

    let backend = SpdkBackend::new("node-1")?;
    println!("SPDK environment initialized");

    let bdevs_before = backend.list_bdevs();
    println!("Available bdevs before attaching: {:?}", bdevs_before);
    println!();

    let pci_addrs = ["0000:03:00.0", "0000:04:00.0", "0000:05:00.0"];
    
    for (i, addr) in pci_addrs.iter().enumerate() {
        match backend.attach_nvme_controller(&format!("Nvme{}", i + 1), addr) {
            Ok(_) => println!("Attached NVMe controller {} at {}", i + 1, addr),
            Err(e) => println!("Failed to attach NVMe controller {} at {}: {}", i + 1, addr, e),
        }
    }

    println!();
    let bdevs_after = backend.list_bdevs();
    println!("Available bdevs after attaching: {:?}", bdevs_after);
    println!();

    let mut device_ids = Vec::new();
    for (i, bdev) in bdevs_after.iter().enumerate() {
        if bdev.starts_with("Nvme") {
            match backend.add_device(&format!("nvme{}", i + 1), bdev, None) {
                Ok(id) => {
                    println!("Added device {} with ID: {}", bdev, id);
                    device_ids.push(id);
                }
                Err(e) => println!("Failed to add device {}: {}", bdev, e),
            }
        }
    }

    println!();
    let devices = backend.list_devices()?;
    println!("List of devices:");
    for dev in devices {
        println!("  - {} ({}, {} GB / {} GB free)", 
            dev.device_id, dev.name, 
            dev.total_capacity / 1024 / 1024 / 1024,
            dev.free_space / 1024 / 1024 / 1024);
    }

    Ok(())
}

#[cfg(not(feature = "spdk"))]
fn main() {
    println!("SPDK feature not enabled. Build with --features spdk");
}
