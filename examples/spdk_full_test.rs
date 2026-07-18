use powerfs_kv_client::{S3TestClient, SpdkTestClient};
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <test_type>", args[0]);
        eprintln!("Test types:");
        eprintln!("  kv          - Run KV client tests");
        eprintln!("  s3          - Run S3 client tests");
        eprintln!("  all         - Run all tests");
        std::process::exit(1);
    }

    let test_type = &args[1];

    match test_type.as_str() {
        "kv" => run_kv_tests(),
        "s3" => run_s3_tests(),
        "all" => {
            run_kv_tests();
            run_s3_tests();
        }
        _ => {
            eprintln!("Unknown test type: {}", test_type);
            std::process::exit(1);
        }
    }
}

fn run_kv_tests() {
    println!("=== Running KV Client Tests ===\n");

    let client = match SpdkTestClient::new("kv-test-node") {
        Ok(c) => c,
        Err(e) => {
            eprintln!("FAIL: Failed to create client: {}", e);
            return;
        }
    };

    let tests = [
        ("Basic Write/Read", || {
            client.write_key(1, "test_key", b"test_value").unwrap();
            let value = client.read_key(1, "test_key").unwrap();
            assert!(value.is_some());
            assert_eq!(value.unwrap(), b"test_value");
        }),
        ("Batch Operations", || {
            let entries = [
                ("key1", b"value1"),
                ("key2", b"value2"),
                ("key3", b"value3"),
            ];
            let write_results = client.batch_write(2, &entries).unwrap();
            assert!(write_results.iter().all(|&r| r));

            let keys = ["key1", "key2", "key3"];
            let read_results = client.batch_read(2, &keys).unwrap();
            assert_eq!(read_results[0].as_ref().unwrap(), b"value1");
            assert_eq!(read_results[1].as_ref().unwrap(), b"value2");
            assert_eq!(read_results[2].as_ref().unwrap(), b"value3");
        }),
        ("Nonexistent Key", || {
            let value = client.read_key(1, "nonexistent").unwrap();
            assert!(value.is_none());
        }),
    ];

    let mut passed = 0;
    let mut failed = 0;

    for (name, test) in tests.iter() {
        print!("Test: {}... ", name);
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(test)) {
            Ok(_) => {
                println!("PASS");
                passed += 1;
            }
            Err(_) => {
                println!("FAIL");
                failed += 1;
            }
        }
    }

    println!("\nKV Tests Summary: {} passed, {} failed\n", passed, failed);
}

fn run_s3_tests() {
    println!("=== Running S3 Client Tests ===\n");

    let mut client = match S3TestClient::new("s3-test-node") {
        Ok(c) => c,
        Err(e) => {
            eprintln!("FAIL: Failed to create client: {}", e);
            return;
        }
    };

    let tests = [
        ("Bucket Creation/Deletion", || {
            client.create_bucket("testbucket").unwrap();
            assert!(client.list_buckets().contains(&"testbucket".to_string()));
            client.delete_bucket("testbucket").unwrap();
            assert!(!client.list_buckets().contains(&"testbucket".to_string()));
        }),
        ("Object Put/Get", || {
            client.create_bucket("mybucket").unwrap();
            client
                .put_object("mybucket", "test.txt", b"Hello, World!")
                .unwrap();
            let data = client.get_object("mybucket", "test.txt").unwrap();
            assert!(data.is_some());
            assert_eq!(data.unwrap(), b"Hello, World!");
        }),
        ("Object Not Found", || {
            client.create_bucket("emptybucket").unwrap();
            let data = client.get_object("emptybucket", "nonexistent.txt").unwrap();
            assert!(data.is_none());
        }),
        ("List Objects", || {
            client.create_bucket("listbucket").unwrap();
            client
                .put_object("listbucket", "file1.txt", b"data1")
                .unwrap();
            let objects = client.list_objects("listbucket").unwrap();
            assert!(!objects.is_empty());
        }),
        ("Invalid Bucket", || {
            let result = client.put_object("nonexistent", "key", b"data");
            assert!(result.is_err());
        }),
    ];

    let mut passed = 0;
    let mut failed = 0;

    for (name, test) in tests.iter() {
        print!("Test: {}... ", name);
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(test)) {
            Ok(_) => {
                println!("PASS");
                passed += 1;
            }
            Err(_) => {
                println!("FAIL");
                failed += 1;
            }
        }
    }

    println!("\nS3 Tests Summary: {} passed, {} failed\n", passed, failed);
}
