use powerfs_core::kv_cache::{KVCacheEngine, KVDtype};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn make_engine(max_mb: u64) -> KVCacheEngine {
    KVCacheEngine::new(max_mb * 1024 * 1024, 1024 * 1024) // 1MB blocks
}

fn make_data(size: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(size);
    for i in 0..size {
        v.push((i % 256) as u8);
    }
    v
}

fn put_block_helper(engine: &KVCacheEngine, session_id: &str, layer_id: u32, num_tokens: u32, data: &[u8]) -> u64 {
    engine.put_block(session_id, layer_id, num_tokens, data, "", layer_id).unwrap()
}

#[test]
fn test_create_delete_session() {
    let engine = make_engine(10);
    assert!(engine
        .create_session("s1", "llama-7b", 32, 32, 128, KVDtype::FP16, 0)
        .is_ok());

    let sess = engine.get_session("s1").unwrap();
    assert_eq!(sess.model_name, "llama-7b");
    assert_eq!(sess.num_layers, 32);
    assert_eq!(sess.dtype, KVDtype::FP16);

    assert!(engine.delete_session("s1").is_ok());
    assert!(engine.get_session("s1").is_none());
}

#[test]
fn test_create_duplicate_session_fails() {
    let engine = make_engine(10);
    assert!(engine
        .create_session("s1", "model-a", 1, 1, 1, KVDtype::FP16, 0)
        .is_ok());
    assert!(engine
        .create_session("s1", "model-b", 2, 2, 2, KVDtype::FP8, 0)
        .is_err());
}

#[test]
fn test_put_get_block() {
    let engine = make_engine(10);
    engine
        .create_session("s1", "llama", 32, 32, 128, KVDtype::FP16, 0)
        .unwrap();

    let data = make_data(1024);
    let block_id = put_block_helper(&engine, "s1", 0, 128, &data);
    assert!(block_id > 0);

    let (meta, read_data) = engine.get_block_data(block_id).unwrap();
    assert_eq!(meta.layer_id, 0);
    assert_eq!(meta.num_tokens, 128);
    assert_eq!(read_data.len(), 1024);
    assert_eq!(read_data, data);
}

#[test]
fn test_put_block_without_session_fails() {
    let engine = make_engine(10);
    let data = make_data(100);
    assert!(engine.put_block("nonexist", 0, 10, &data, "", 0).is_err());
}

#[test]
fn test_batch_put_get() {
    let engine = make_engine(10);
    engine
        .create_session("s1", "llama", 32, 32, 128, KVDtype::FP16, 0)
        .unwrap();

    let data1 = make_data(512);
    let data2 = make_data(1024);
    let requests = vec![
        ("s1".to_string(), 0u32, 64u32, data1.clone(), "".to_string(), 0u32),
        ("s1".to_string(), 1u32, 128u32, data2.clone(), "".to_string(), 1u32),
    ];

    let results = engine.batch_put(&requests);
    assert_eq!(results.len(), 2);
    assert!(results[0].is_ok());
    assert!(results[1].is_ok());

    let id1 = results[0].as_ref().unwrap();
    let id2 = results[1].as_ref().unwrap();

    let batch_results = engine.batch_get(&[*id1, *id2, 9999]);
    assert_eq!(batch_results.len(), 3);
    assert!(batch_results[0].is_some());
    assert!(batch_results[1].is_some());
    assert!(batch_results[2].is_none());

    assert_eq!(batch_results[0].as_ref().unwrap().1, data1);
    assert_eq!(batch_results[1].as_ref().unwrap().1, data2);
}

#[test]
fn test_lru_eviction() {
    let engine = make_engine(5); // 5MB limit, 1MB blocks
    engine
        .create_session("s1", "llama", 32, 32, 128, KVDtype::FP16, 0)
        .unwrap();

    let data = make_data(1024 * 1024); // 1MB
    let mut ids = Vec::new();
    for i in 0..8 {
        let id = put_block_helper(&engine, "s1", i as u32, 128, &data);
        ids.push(id);
    }

    let stats = engine.stats();
    assert!(stats.evictions > 0);
    assert!(stats.used_memory_bytes <= 5 * 1024 * 1024);

    let mut found = 0;
    for id in &ids {
        if engine.get_block(*id).is_some() {
            found += 1;
        }
    }
    assert!(found <= 5);
}

#[test]
fn test_stats_counter() {
    let engine = make_engine(10);
    engine
        .create_session("s1", "llama", 32, 32, 128, KVDtype::FP16, 0)
        .unwrap();

    let data = make_data(1024);
    let id = put_block_helper(&engine, "s1", 0, 128, &data);

    let stats_before = engine.stats();
    assert_eq!(stats_before.total_sessions, 1);
    assert_eq!(stats_before.total_blocks, 1);
    assert_eq!(stats_before.hits, 0);

    let _ = engine.get_block_data(id);
    let _ = engine.get_block_data(id);

    let stats_after = engine.stats();
    assert_eq!(stats_after.hits, 2);
}

#[test]
fn test_session_block_list() {
    let engine = make_engine(10);
    engine
        .create_session("s1", "llama", 32, 32, 128, KVDtype::FP16, 0)
        .unwrap();

    let data = make_data(100);
    for i in 0..5 {
        put_block_helper(&engine, "s1", i as u32, 10, &data);
    }

    let blocks = engine.get_session_blocks("s1");
    assert_eq!(blocks.len(), 5);
}

#[test]
fn test_list_sessions() {
    let engine = make_engine(10);
    engine
        .create_session("alpha-1", "m1", 1, 1, 1, KVDtype::FP16, 0)
        .unwrap();
    engine
        .create_session("alpha-2", "m2", 1, 1, 1, KVDtype::FP16, 0)
        .unwrap();
    engine
        .create_session("beta-1", "m3", 1, 1, 1, KVDtype::FP16, 0)
        .unwrap();

    let (ids, total) = engine.list_sessions(100, "");
    assert_eq!(total, 3);
    assert_eq!(ids.len(), 3);

    let (ids2, total2) = engine.list_sessions(100, "alpha");
    assert_eq!(total2, 2);
    assert_eq!(ids2.len(), 2);

    let (ids3, total3) = engine.list_sessions(1, "");
    assert_eq!(total3, 3);
    assert_eq!(ids3.len(), 1);
}

#[test]
fn test_concurrent_access() {
    let engine = Arc::new(make_engine(50));
    engine
        .create_session("s1", "llama", 32, 32, 128, KVDtype::FP16, 0)
        .unwrap();

    let data = Arc::new(make_data(4096));
    let mut handles = Vec::new();

    for i in 0..10 {
        let eng = engine.clone();
        let d = data.clone();
        handles.push(thread::spawn(move || {
            let mut ids = Vec::new();
            for j in 0..20 {
                let id = eng.put_block("s1", (i * 20 + j) as u32, 10, &d, "", (i * 20 + j) as u32).unwrap();
                ids.push(id);
            }
            for id in &ids {
                assert!(eng.get_block(*id).is_some());
            }
            ids.len()
        }));
    }

    for h in handles {
        let n = h.join().unwrap();
        assert_eq!(n, 20);
    }

    let stats = engine.stats();
    assert_eq!(stats.total_sessions, 1);
    assert!(stats.hits >= 200);
}

#[test]
fn test_dtype_from_str() {
    assert_eq!(KVDtype::parse("fp32"), Some(KVDtype::FP32));
    assert_eq!(KVDtype::parse("FP16"), Some(KVDtype::FP16));
    assert_eq!(KVDtype::parse("bf16"), Some(KVDtype::BF16));
    assert_eq!(KVDtype::parse("FP8"), Some(KVDtype::FP8));
    assert_eq!(KVDtype::parse("int8"), Some(KVDtype::INT8));
    assert_eq!(KVDtype::parse("invalid"), None);
}

#[test]
fn test_ttl_expiry() {
    let engine = make_engine(10);
    engine
        .create_session("s1", "llama", 32, 32, 128, KVDtype::FP16, 1)
        .unwrap();

    let data = make_data(1024);
    let _ = put_block_helper(&engine, "s1", 0, 128, &data);

    assert!(engine.get_session("s1").is_some());

    thread::sleep(Duration::from_millis(1100));

    let cleaned = engine.cleanup_expired();
    assert!(cleaned >= 1);

    assert!(engine.get_session("s1").is_none());
}

#[test]
fn test_block_meta_fid_field() {
    let engine = make_engine(10);
    engine
        .create_session("s1", "llama", 32, 32, 128, KVDtype::FP16, 0)
        .unwrap();

    let data = make_data(1024);
    let block_id = engine.put_block("s1", 0, 128, &data, "1,2:3", 5).unwrap();

    let (meta, _) = engine.get_block_data(block_id).unwrap();
    assert_eq!(meta.fid, "1,2:3");
    assert_eq!(meta.block_index, 5);
}

#[test]
fn test_block_meta_index_field() {
    let engine = make_engine(10);
    engine
        .create_session("s1", "llama", 32, 32, 128, KVDtype::FP16, 0)
        .unwrap();

    let data = make_data(1024);
    for i in 0..5 {
        let block_id = engine.put_block("s1", i, 128, &data, &format!("vol1,{}:{}", i, i*100), i*10).unwrap();
        let (meta, _) = engine.get_block_data(block_id).unwrap();
        assert_eq!(meta.block_index, i*10);
        assert_eq!(meta.fid, format!("vol1,{}:{}", i, i*100));
    }
}

#[test]
fn test_block_id_mapping_add_get() {
    let engine = make_engine(10);
    engine
        .create_session("s1", "llama", 32, 32, 128, KVDtype::FP16, 0)
        .unwrap();

    let data = make_data(1024);
    let block_id = engine.put_block("s1", 0, 128, &data, "1,2:3", 0).unwrap();

    let fid = engine.get_fid_by_block_id(block_id);
    assert_eq!(fid, Some("1,2:3".to_string()));
}

#[test]
fn test_block_id_mapping_remove() {
    let engine = make_engine(10);
    engine
        .create_session("s1", "llama", 32, 32, 128, KVDtype::FP16, 0)
        .unwrap();

    let data = make_data(1024);
    let block_id = engine.put_block("s1", 0, 128, &data, "1,2:3", 0).unwrap();

    assert!(engine.get_fid_by_block_id(block_id).is_some());

    engine.remove_block_id_mapping(block_id);
    assert!(engine.get_fid_by_block_id(block_id).is_none());
}

#[test]
fn test_block_id_mapping_persistence() {
    let engine = make_engine(10);
    engine
        .create_session("s1", "llama", 32, 32, 128, KVDtype::FP16, 0)
        .unwrap();

    let data = make_data(1024);
    let block_id = engine.put_block("s1", 0, 128, &data, "1,2:3", 0).unwrap();

    assert!(engine.get_fid_by_block_id(block_id).is_some());

    engine.delete_session("s1").unwrap();

    assert!(engine.get_fid_by_block_id(block_id).is_none());
}
