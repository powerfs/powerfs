use powerfs_core::storage_backend::{SpdkBackend, StorageBackend};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== PowerFS SPDK Wrapper Test ===\n");

    println!("1. Creating SPDK backend...");
    let backend = SpdkBackend::new("node-0", None)?;
    println!("SUCCESS");

    println!("\n2. Attaching NVMe controllers...");
    let controllers = [
        ("nvme1", "0000:03:00.0"),
        ("nvme2", "0000:04:00.0"),
        ("nvme3", "0000:05:00.0"),
    ];

    for (name, traddr) in controllers.iter() {
        match backend.attach_nvme_controller(name, traddr) {
            Ok(_) => println!("  Controller {} ({}): OK", name, traddr),
            Err(e) => println!("  Controller {} ({}): FAILED - {}", name, traddr, e),
        }
    }

    println!("\n3. Listing bdevs...");
    let bdevs = backend.list_bdevs();
    println!("Found {} bdev(s):", bdevs.len());
    for (i, bdev) in bdevs.iter().enumerate() {
        println!("  [{}] {}", i, bdev);
    }

    if !bdevs.is_empty() {
        println!("\n4. Adding device...");
        let device_id = backend.add_device("test-device", &bdevs[0], None)?;
        println!("SUCCESS: device_id = {}", device_id);

        println!("\n5. Getting device info...");
        let device = StorageBackend::get_device(&backend, &device_id)?;
        println!(
            "Device: {} ({} bytes)",
            device.device_id, device.total_capacity
        );

        println!("\n6. Allocating volume...");
        let vol_result = StorageBackend::allocate_volume(&backend, 1, 1024 * 1024 * 50, None)?;
        println!(
            "SUCCESS: volume_id = {}, device_id = {}, size = {}",
            vol_result.volume_id, vol_result.device_id, vol_result.allocated_size
        );

        println!("\n7. Writing needle...");
        let data = vec![0xBBu8; 1024];
        let written = StorageBackend::write_needle(&backend, 1, 0, &data)?;
        println!("SUCCESS: written {} bytes", written);

        println!("\n8. Reading needle...");
        let read_data = StorageBackend::read_needle(&backend, 1, 0, 1024)?;
        println!("SUCCESS: read {} bytes", read_data.len());

        let all_bb = read_data.iter().all(|&b| b == 0xBB);
        println!("Data integrity: {}", if all_bb { "PASS" } else { "FAIL" });

        println!("\n9. Getting volume info...");
        let vol_info = StorageBackend::get_volume_info(&backend, 1)?;
        println!(
            "Volume: total={}, used={}",
            vol_info.total_size, vol_info.used_size
        );

        println!("\n10. Deleting volume...");
        StorageBackend::delete_volume(&backend, 1)?;
        println!("SUCCESS");

        println!("\n11. Getting device health...");
        let health = StorageBackend::get_device_health(&backend, &device_id)?;
        println!(
            "Health: status={}, utilization={:.1}%",
            health.health_status, health.utilization_percent
        );
    } else {
        println!("\nNo bdev devices found.");
    }

    println!("\n=== All tests completed ===");
    Ok(())
}
