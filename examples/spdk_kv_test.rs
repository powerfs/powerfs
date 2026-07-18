use powerfs_kv_client::SpdkTestClient;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <command> [args...]", args[0]);
        eprintln!("Commands:");
        eprintln!("  put <volume_id> <key> <value>    - Write a key-value pair");
        eprintln!("  get <volume_id> <key>            - Read a value by key");
        eprintln!("  batch <volume_id>                - Run batch test");
        eprintln!("  benchmark <volume_id> <count>    - Run benchmark");
        std::process::exit(1);
    }

    let client = match SpdkTestClient::new("test-client") {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to create client: {}", e);
            std::process::exit(1);
        }
    };

    let command = &args[1];

    match command.as_str() {
        "put" => {
            if args.len() < 5 {
                eprintln!("Usage: {} put <volume_id> <key> <value>", args[0]);
                std::process::exit(1);
            }
            let volume_id: u64 = args[2].parse().unwrap();
            let key = &args[3];
            let value = &args[4];

            match client.write_key(volume_id, key, value.as_bytes()) {
                Ok(_) => println!("Successfully wrote key '{}'", key),
                Err(e) => eprintln!("Failed to write: {}", e),
            }
        }
        "get" => {
            if args.len() < 4 {
                eprintln!("Usage: {} get <volume_id> <key>", args[0]);
                std::process::exit(1);
            }
            let volume_id: u64 = args[2].parse().unwrap();
            let key = &args[3];

            match client.read_key(volume_id, key) {
                Ok(Some(value)) => {
                    println!("Value for '{}': {}", key, String::from_utf8_lossy(&value))
                }
                Ok(None) => println!("Key '{}' not found", key),
                Err(e) => eprintln!("Failed to read: {}", e),
            }
        }
        "batch" => {
            if args.len() < 3 {
                eprintln!("Usage: {} batch <volume_id>", args[0]);
                std::process::exit(1);
            }
            let volume_id: u64 = args[2].parse().unwrap();

            let entries = [
                ("key1", b"value1"),
                ("key2", b"value2"),
                ("key3", b"value3"),
                ("key4", b"value4"),
                ("key5", b"value5"),
            ];

            println!("Writing batch entries...");
            let write_results = client.batch_write(volume_id, &entries).unwrap();
            println!("Write results: {:?}", write_results);

            let keys = ["key1", "key2", "key3", "key4", "key5"];
            println!("Reading batch entries...");
            let read_results = client.batch_read(volume_id, &keys).unwrap();

            for (i, result) in read_results.iter().enumerate() {
                match result {
                    Some(v) => println!("key{}: {}", i + 1, String::from_utf8_lossy(v)),
                    None => println!("key{}: not found", i + 1),
                }
            }
        }
        "benchmark" => {
            if args.len() < 4 {
                eprintln!("Usage: {} benchmark <volume_id> <count>", args[0]);
                std::process::exit(1);
            }
            let volume_id: u64 = args[2].parse().unwrap();
            let count: usize = args[3].parse().unwrap();

            println!("Running benchmark with {} operations...", count);

            let start = std::time::Instant::now();

            for i in 0..count {
                let key = format!("bench_key_{}", i);
                let value = format!("bench_value_{}", i);
                client.write_key(volume_id, &key, value.as_bytes()).unwrap();
            }

            let write_time = start.elapsed();

            let start = std::time::Instant::now();

            for i in 0..count {
                let key = format!("bench_key_{}", i);
                client.read_key(volume_id, &key).unwrap();
            }

            let read_time = start.elapsed();

            println!(
                "Write {} keys in {:?} ({:.2} ops/s)",
                count,
                write_time,
                count as f64 / write_time.as_secs_f64()
            );
            println!(
                "Read {} keys in {:?} ({:.2} ops/s)",
                count,
                read_time,
                count as f64 / read_time.as_secs_f64()
            );
        }
        _ => {
            eprintln!("Unknown command: {}", command);
            std::process::exit(1);
        }
    }
}
