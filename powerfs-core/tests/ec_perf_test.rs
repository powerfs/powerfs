use powerfs_core::ec_thread::{EcConfig, EcEncoder, EcThreadPool, SimdBackend};
use std::time::{Duration, Instant};

fn benchmark_encode_single(
    data_size: usize,
    iterations: usize,
    config: &EcConfig,
) -> (f64, f64, f64) {
    let encoder = EcEncoder::new(config.clone());
    let data: Vec<u8> = (0..data_size).map(|i| i as u8).collect();

    let mut total_time = Duration::new(0, 0);

    for _ in 0..iterations {
        let start = Instant::now();
        let _shards = encoder.encode(&data);
        let elapsed = start.elapsed();
        total_time += elapsed;
    }

    let avg_time_ms = (total_time.as_nanos() as f64 / iterations as f64) / 1_000_000.0;
    let throughput_mbps =
        (data_size as f64 * 8.0 * iterations as f64) / (total_time.as_nanos() as f64 / 1_000.0);
    let ops_per_sec = iterations as f64 / total_time.as_secs_f64();

    (avg_time_ms, throughput_mbps, ops_per_sec)
}

#[tokio::test]
async fn test_ec_performance_benchmark() {
    let data_sizes = vec![1024 * 1024, 4 * 1024 * 1024];
    let iterations = 3;

    let backends = vec![
        ("None", SimdBackend::None),
        ("SSE4.1", SimdBackend::Sse41),
        ("AVX2", SimdBackend::Avx2),
        ("Neon", SimdBackend::Neon),
        ("Auto", SimdBackend::Auto),
    ];

    println!("\n=== EC Encoding Performance Benchmark ===\n");
    println!("Data sizes: {:?} bytes", data_sizes);
    println!("Iterations per test: {}\n", iterations);
    println!(
        "{:<12} | {:<12} | {:<10} | {:<12} | {:<12} | {:<10}",
        "Backend", "Effective", "Data Size", "Avg Time (ms)", "Throughput (Mbps)", "Ops/s"
    );
    println!(
        "-------------------------------------------------------------------------------------"
    );

    let mut results = Vec::new();

    for (name, backend) in backends {
        let config = EcConfig {
            simd_backend: backend.clone(),
            min_small_file_size: 0,
            ..Default::default()
        };

        let effective = backend.effective_backend();

        for &size in &data_sizes {
            let (avg_time, throughput, ops) = benchmark_encode_single(size, iterations, &config);

            results.push((
                name.to_string(),
                format!("{:?}", effective),
                size,
                avg_time,
                throughput,
                ops,
            ));

            println!(
                "{:<12} | {:<12} | {:<10} | {:<12.3} | {:<12.3} | {:<10.3}",
                name,
                format!("{:?}", effective).split("(").next().unwrap_or(""),
                format!("{}MB", size / (1024 * 1024)),
                avg_time,
                throughput,
                ops
            );
        }
    }

    println!("\n=== Summary ===");
    let best = results
        .iter()
        .max_by(|a, b| a.4.partial_cmp(&b.4).unwrap_or(std::cmp::Ordering::Equal));

    if let Some(b) = best {
        println!(
            "Best Throughput: {:.2} Mbps (Backend: {}, Data: {}MB)",
            b.4,
            b.0,
            b.2 / (1024 * 1024)
        );
    }

    println!();
}

#[tokio::test]
async fn test_ec_parallel_vs_serial() {
    let data_size = 4 * 1024 * 1024;
    let iterations = 3;

    let parallel_config = EcConfig {
        min_small_file_size: 0,
        parallel_encoding: true,
        ..Default::default()
    };

    let serial_config = EcConfig {
        min_small_file_size: 0,
        parallel_encoding: false,
        ..Default::default()
    };

    let (parallel_time, parallel_throughput, _) =
        benchmark_encode_single(data_size, iterations, &parallel_config);
    let (serial_time, serial_throughput, _) =
        benchmark_encode_single(data_size, iterations, &serial_config);

    println!("\n=== Parallel vs Serial Comparison ===");
    println!("Data Size: {} MB", data_size / (1024 * 1024));
    println!("Iterations: {}\n", iterations);
    println!(
        "{:<15} | {:<15} | {:<15}",
        "Mode", "Avg Time (ms)", "Throughput (Mbps)"
    );
    println!("----------------------------------------------");
    println!(
        "{:<15} | {:<15.3} | {:<15.3}",
        "Parallel", parallel_time, parallel_throughput
    );
    println!(
        "{:<15} | {:<15.3} | {:<15.3}",
        "Serial", serial_time, serial_throughput
    );
    println!();

    if serial_throughput > 0.0 {
        let improvement = ((parallel_throughput - serial_throughput) / serial_throughput) * 100.0;
        println!("Parallel improvement: {:.2}%", improvement);
    }
}

#[tokio::test]
async fn test_ec_thread_pool_benchmark() {
    let data_size = 2 * 1024 * 1024;
    let iterations = 5;

    let configs = vec![
        ("SIMD Auto", SimdBackend::Auto),
        ("SIMD None", SimdBackend::None),
    ];

    println!("\n=== EC Thread Pool Benchmark ===");
    println!("Data Size: {} MB", data_size / (1024 * 1024));
    println!("Iterations: {}\n", iterations);

    for (name, backend) in configs {
        let config = EcConfig {
            simd_backend: backend,
            min_small_file_size: 0,
            ..Default::default()
        };

        let ec_pool = EcThreadPool::start(config.clone());
        let data: Vec<u8> = (0..data_size).map(|i| i as u8).collect();

        let start = Instant::now();
        for _ in 0..iterations {
            let _ = ec_pool.encode(data.clone(), config.clone()).await.unwrap();
        }
        let elapsed = start.elapsed();

        let avg_time_ms = elapsed.as_nanos() as f64 / (iterations as f64 * 1_000_000.0);
        let throughput_mbps =
            (data_size as f64 * 8.0 * iterations as f64) / (elapsed.as_nanos() as f64 / 1_000.0);

        println!("{}:", name);
        println!("  Avg Time: {:.3} ms", avg_time_ms);
        println!("  Throughput: {:.3} Mbps", throughput_mbps);
        println!();
    }
}
