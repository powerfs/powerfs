use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use powerfs_core::kv_cache::KVCacheEngine;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;

fn make_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut rng = rand::thread_rng();
    for _ in 0..size {
        data.push(rng.gen());
    }
    data
}

fn make_engine() -> KVCacheEngine {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    KVCacheEngine::new_with_db(100 * 1024 * 1024, 10 * 1024 * 1024, db_path).unwrap()
}

struct ModelConfig {
    name: String,
    num_layers: usize,
    num_heads: usize,
    head_dim: usize,
    page_size_tokens: usize,
}

fn get_model_config(model: &str) -> ModelConfig {
    match model {
        "glm5" => ModelConfig {
            name: "glm5".to_string(),
            num_layers: 80,
            num_heads: 64,
            head_dim: 128,
            page_size_tokens: 512,
        },
        "kimi-k2.6" => ModelConfig {
            name: "kimi-k2.6".to_string(),
            num_layers: 72,
            num_heads: 64,
            head_dim: 128,
            page_size_tokens: 512,
        },
        "qwen2.5-7b" => ModelConfig {
            name: "qwen2.5-7b".to_string(),
            num_layers: 32,
            num_heads: 32,
            head_dim: 128,
            page_size_tokens: 512,
        },
        _ => ModelConfig {
            name: "default".to_string(),
            num_layers: 32,
            num_heads: 32,
            head_dim: 128,
            page_size_tokens: 512,
        },
    }
}

fn get_page_size_bytes(model_config: &ModelConfig) -> usize {
    model_config.page_size_tokens * model_config.num_layers * model_config.num_heads * model_config.head_dim * 2
}

fn vllm_prefill_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_vllm_prefill", "vllm_prefill", "user1")
        .unwrap();

    let model_config = get_model_config("qwen2.5-7b");
    let page_size_bytes = get_page_size_bytes(&model_config);
    let input_lengths = vec![1024, 2048, 4096, 8192];

    let mut group = c.benchmark_group("vllm_prefill");
    group.measurement_time(Duration::from_secs(15));

    for input_len in &input_lengths {
        let pages_per_layer = (input_len + model_config.page_size_tokens - 1) / model_config.page_size_tokens;
        let total_pages = pages_per_layer * model_config.num_layers;
        let total_bytes = total_pages * page_size_bytes;

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(BenchmarkId::new("prefill_write", input_len), input_len, |b, &input_len| {
            let engine = engine.clone();
            let model_config = model_config.clone();
            b.iter(|| {
                let request_id = rand::thread_rng().gen::<u64>();
                let pages_per_layer = (input_len + model_config.page_size_tokens - 1) / model_config.page_size_tokens;

                for layer_idx in 0..model_config.num_layers {
                    for page_idx in 0..pages_per_layer {
                        let key = format!("kv_cache_{}_{}_{}", request_id, layer_idx, page_idx);
                        let data = make_data(page_size_bytes);
                        engine.kv_put("ns_vllm_prefill", &key, &data, "user1").unwrap();
                    }
                }
            });
        });
    }
    group.finish();
}

fn vllm_decode_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_vllm_decode", "vllm_decode", "user1")
        .unwrap();

    let model_config = get_model_config("qwen2.5-7b");
    let page_size_bytes = get_page_size_bytes(&model_config);

    for request_id in 0..100 {
        for layer_idx in 0..model_config.num_layers {
            for page_idx in 0..10 {
                let key = format!("kv_cache_{}_{}_{}", request_id, layer_idx, page_idx);
                let data = make_data(page_size_bytes);
                engine.kv_put("ns_vllm_decode", &key, &data, "user1").unwrap();
            }
        }
    }

    let mut group = c.benchmark_group("vllm_decode");
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("decode_read", |b| {
        let engine = engine.clone();
        let model_config = model_config.clone();
        b.iter(|| {
            let request_id = rand::thread_rng().gen::<u64>() % 100;

            for layer_idx in 0..model_config.num_layers {
                let page_idx = rand::thread_rng().gen::<usize>() % 10;
                let key = format!("kv_cache_{}_{}_{}", request_id, layer_idx, page_idx);
                let result = engine.kv_get("ns_vllm_decode", &key).unwrap();
                assert!(result.is_some());
            }
        });
    });

    group.finish();
}

fn vllm_prefill_decode_mixed_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_vllm_mixed", "vllm_mixed", "user1")
        .unwrap();

    let model_config = get_model_config("qwen2.5-7b");
    let page_size_bytes = get_page_size_bytes(&model_config);

    for request_id in 0..50 {
        for layer_idx in 0..model_config.num_layers {
            for page_idx in 0..5 {
                let key = format!("kv_cache_{}_{}_{}", request_id, layer_idx, page_idx);
                let data = make_data(page_size_bytes);
                engine.kv_put("ns_vllm_mixed", &key, &data, "user1").unwrap();
            }
        }
    }

    let mut group = c.benchmark_group("vllm_prefill_decode_mixed");
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("mixed_prefill_decode", |b| {
        let engine = engine.clone();
        let model_config = model_config.clone();
        b.iter(|| {
            let rng = rand::thread_rng().gen::<u64>();

            if rng % 10 == 0 {
                let request_id = rand::thread_rng().gen::<u64>();
                for layer_idx in 0..model_config.num_layers {
                    let key = format!("kv_cache_{}_{}_0", request_id, layer_idx);
                    let data = make_data(page_size_bytes);
                    engine.kv_put("ns_vllm_mixed", &key, &data, "user1").unwrap();
                }
            } else {
                let request_id = rand::thread_rng().gen::<u64>() % 50;
                for layer_idx in 0..model_config.num_layers {
                    let page_idx = rand::thread_rng().gen::<usize>() % 5;
                    let key = format!("kv_cache_{}_{}_{}", request_id, layer_idx, page_idx);
                    let result = engine.kv_get("ns_vllm_mixed", &key).unwrap();
                    assert!(result.is_some());
                }
            }
        });
    });

    group.finish();
}

fn vllm_concurrent_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_vllm_concurrent", "vllm_concurrent", "user1")
        .unwrap();

    let model_config = get_model_config("qwen2.5-7b");
    let page_size_bytes = get_page_size_bytes(&model_config);

    for request_id in 0..100 {
        for layer_idx in 0..model_config.num_layers {
            for page_idx in 0..5 {
                let key = format!("kv_cache_{}_{}_{}", request_id, layer_idx, page_idx);
                let data = make_data(page_size_bytes);
                engine.kv_put("ns_vllm_concurrent", &key, &data, "user1").unwrap();
            }
        }
    }

    let thread_counts = vec![1, 2, 4, 8, 16];

    let mut group = c.benchmark_group("vllm_concurrent");
    group.measurement_time(Duration::from_secs(20));

    for threads in &thread_counts {
        group.bench_with_input(BenchmarkId::new("concurrent_prefill_decode", threads), threads, |b, &threads| {
            let engine = engine.clone();
            let model_config = model_config.clone();
            b.iter_custom(|iters| {
                let start = std::time::Instant::now();
                let mut handles = Vec::new();

                for _ in 0..threads {
                    let engine = engine.clone();
                    handles.push(std::thread::spawn(move || {
                        let requests_per_thread = iters / threads as u64;
                        for _ in 0..requests_per_thread {
                            let rng = rand::thread_rng().gen::<u64>();

                            if rng % 10 == 0 {
                                let request_id = rand::thread_rng().gen::<u64>();
                                for layer_idx in 0..model_config.num_layers {
                                    let key = format!("kv_cache_{}_{}_0", request_id, layer_idx);
                                    let data = make_data(page_size_bytes);
                                    engine.kv_put("ns_vllm_concurrent", &key, &data, "user1").unwrap();
                                }
                            } else {
                                let request_id = rand::thread_rng().gen::<u64>() % 100;
                                for layer_idx in 0..model_config.num_layers {
                                    let page_idx = rand::thread_rng().gen::<usize>() % 5;
                                    let key = format!("kv_cache_{}_{}_{}", request_id, layer_idx, page_idx);
                                    let result = engine.kv_get("ns_vllm_concurrent", &key).unwrap();
                                    assert!(result.is_some());
                                }
                            }
                        }
                    }));
                }

                for handle in handles {
                    handle.join().unwrap();
                }

                start.elapsed()
            });
        });
    }
    group.finish();
}

fn vllm_different_models_benchmark(c: &mut Criterion) {
    let models = vec!["glm5", "kimi-k2.6", "qwen2.5-7b"];

    let mut group = c.benchmark_group("vllm_different_models");
    group.measurement_time(Duration::from_secs(10));

    for model_name in &models {
        let model_config = get_model_config(model_name);
        let page_size_bytes = get_page_size_bytes(&model_config);

        let engine = Arc::new(make_engine());
        engine
            .create_namespace(&format!("ns_{}", model_name), model_name, "user1")
            .unwrap();

        group.bench_with_input(BenchmarkId::new("model_prefill", model_name), model_name, |b, _| {
            let engine = engine.clone();
            let model_config = model_config.clone();
            b.iter(|| {
                let request_id = rand::thread_rng().gen::<u64>();
                for layer_idx in 0..model_config.num_layers {
                    let key = format!("kv_cache_{}_{}_0", request_id, layer_idx);
                    let data = make_data(page_size_bytes);
                    engine.kv_put(&format!("ns_{}", model_name), &key, &data, "user1").unwrap();
                }
            });
        });
    }

    group.finish();
}

fn vllm_cache_hit_rate_benchmark(c: &mut Criterion) {
    let engine = Arc::new(make_engine());
    engine
        .create_namespace("ns_vllm_hit_rate", "vllm_hit_rate", "user1")
        .unwrap();

    let model_config = get_model_config("qwen2.5-7b");
    let page_size_bytes = get_page_size_bytes(&model_config);

    let hit_rates = vec![0.1, 0.3, 0.5, 0.7, 0.9];

    let mut group = c.benchmark_group("vllm_cache_hit_rate");
    group.measurement_time(Duration::from_secs(10));

    for hit_rate in &hit_rates {
        for i in 0..1000 {
            let key = format!("cache_key_{}", i);
            let data = make_data(page_size_bytes);
            engine.kv_put("ns_vllm_hit_rate", &key, &data, "user1").unwrap();
        }

        group.bench_with_input(BenchmarkId::new("hit_rate", format!("{}%", hit_rate * 100)), hit_rate, |b, &hit_rate| {
            let engine = engine.clone();
            let model_config = model_config.clone();
            b.iter(|| {
                let rng = rand::thread_rng().gen::<f64>();

                if rng < hit_rate {
                    let key_idx = rand::thread_rng().gen::<usize>() % 1000;
                    let key = format!("cache_key_{}", key_idx);
                    let result = engine.kv_get("ns_vllm_hit_rate", &key).unwrap();
                    assert!(result.is_some());
                } else {
                    let key = format!("new_key_{}", rand::thread_rng().gen::<u64>());
                    let data = make_data(page_size_bytes);
                    engine.kv_put("ns_vllm_hit_rate", &key, &data, "user1").unwrap();
                }
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    vllm_prefill_benchmark,
    vllm_decode_benchmark,
    vllm_prefill_decode_mixed_benchmark,
    vllm_concurrent_benchmark,
    vllm_different_models_benchmark,
    vllm_cache_hit_rate_benchmark
);
criterion_main!(benches);