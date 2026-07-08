use powerfs_core::kv_cache::KVCacheEngine;
use rand::Rng;
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

fn make_engine_with_db(db_path: &str) -> KVCacheEngine {
    KVCacheEngine::new_with_db(10 * 1024 * 1024, 1024 * 1024, db_path).unwrap()
}

fn make_data(size: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(size);
    for i in 0..size {
        v.push((i % 256) as u8);
    }
    v
}

#[test]
fn test_kv_namespace_management() {
    let dir = tempdir().unwrap();
    let engine = make_engine_with_db(dir.path().to_str().unwrap());

    assert!(engine
        .create_namespace("ns1", "test_namespace", "user1")
        .is_ok());
    assert!(engine.get_namespace("ns1").is_some());
    assert_eq!(engine.get_namespace("ns1").unwrap().name, "test_namespace");

    let namespaces = engine.list_namespaces("user1");
    assert!(!namespaces.is_empty());

    assert!(engine.delete_namespace("ns1", "user1").is_ok());
    assert!(engine.get_namespace("ns1").is_none());
}

#[test]
fn test_kv_put_get() {
    let dir = tempdir().unwrap();
    let engine = make_engine_with_db(dir.path().to_str().unwrap());

    assert!(engine
        .create_namespace("ns_put_get", "test", "user1")
        .is_ok());

    let key = "test_key";
    let value = make_data(1024);

    assert!(engine.kv_put("ns_put_get", key, &value, "user1").is_ok());

    let result = engine.kv_get("ns_put_get", key);
    assert!(result.is_ok());
    assert!(result.as_ref().unwrap().is_some());
    assert_eq!(result.unwrap().unwrap().data, value);
}

#[test]
fn test_kv_put_get_large_value() {
    let dir = tempdir().unwrap();
    let engine = make_engine_with_db(dir.path().to_str().unwrap());

    assert!(engine.create_namespace("ns_large", "test", "user1").is_ok());

    let key = "large_key";
    let value = make_data(1024 * 1024);

    assert!(engine.kv_put("ns_large", key, &value, "user1").is_ok());

    let result = engine.kv_get("ns_large", key);
    assert!(result.is_ok());
    assert!(result.as_ref().unwrap().is_some());
    assert_eq!(result.unwrap().unwrap().data, value);
}

#[test]
fn test_kv_update() {
    let dir = tempdir().unwrap();
    let engine = make_engine_with_db(dir.path().to_str().unwrap());

    assert!(engine
        .create_namespace("ns_update", "test", "user1")
        .is_ok());

    let key = "update_key";
    let value1 = b"first_value";
    let value2 = b"second_value";

    assert!(engine.kv_put("ns_update", key, value1, "user1").is_ok());
    assert!(engine.kv_put("ns_update", key, value2, "user1").is_ok());

    let result = engine.kv_get("ns_update", key);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().unwrap().data, value2);
}

#[test]
fn test_kv_delete() {
    let dir = tempdir().unwrap();
    let engine = make_engine_with_db(dir.path().to_str().unwrap());

    assert!(engine
        .create_namespace("ns_delete", "test", "user1")
        .is_ok());

    let key = "delete_key";
    let value = b"test_value";

    assert!(engine.kv_put("ns_delete", key, value, "user1").is_ok());
    assert!(engine.kv_delete("ns_delete", key).is_ok());

    let result = engine.kv_get("ns_delete", key);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_kv_exists() {
    let dir = tempdir().unwrap();
    let engine = make_engine_with_db(dir.path().to_str().unwrap());

    assert!(engine
        .create_namespace("ns_exists", "test", "user1")
        .is_ok());

    let key = "exists_key";
    let value = b"test_value";

    assert!(!engine.kv_exists("ns_exists", key).unwrap());
    assert!(engine.kv_put("ns_exists", key, value, "user1").is_ok());
    assert!(engine.kv_exists("ns_exists", key).unwrap());
    assert!(engine.kv_delete("ns_exists", key).is_ok());
    assert!(!engine.kv_exists("ns_exists", key).unwrap());
}

#[test]
fn test_kv_list() {
    let dir = tempdir().unwrap();
    let engine = make_engine_with_db(dir.path().to_str().unwrap());

    assert!(engine.create_namespace("ns_list", "test", "user1").is_ok());

    for i in 0..5 {
        let key = format!("key_{}", i);
        let value = make_data(100);
        assert!(engine.kv_put("ns_list", &key, &value, "user1").is_ok());
    }

    let keys = engine.kv_list("ns_list", None).unwrap();
    assert_eq!(keys.len(), 5);

    let keys_with_prefix = engine.kv_list("ns_list", Some("key_1")).unwrap();
    assert_eq!(keys_with_prefix.len(), 1);
    assert_eq!(keys_with_prefix[0], "key_1");
}

#[test]
fn test_kv_remove_by_regex() {
    let dir = tempdir().unwrap();
    let engine = make_engine_with_db(dir.path().to_str().unwrap());

    assert!(engine.create_namespace("ns_regex", "test", "user1").is_ok());

    for i in 0..5 {
        let key = format!("test_key_{}", i);
        let value = make_data(100);
        assert!(engine.kv_put("ns_regex", &key, &value, "user1").is_ok());
    }

    assert!(engine
        .kv_remove_by_regex("ns_regex", "test_key_[0-2]")
        .is_ok());

    let keys = engine.kv_list("ns_regex", None).unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"test_key_3".to_string()));
    assert!(keys.contains(&"test_key_4".to_string()));
}

#[test]
fn test_kv_remove_all() {
    let dir = tempdir().unwrap();
    let engine = make_engine_with_db(dir.path().to_str().unwrap());

    assert!(engine
        .create_namespace("ns_remove_all", "test", "user1")
        .is_ok());

    for i in 0..5 {
        let key = format!("key_{}", i);
        let value = make_data(100);
        assert!(engine
            .kv_put("ns_remove_all", &key, &value, "user1")
            .is_ok());
    }

    assert!(engine.kv_remove_all("ns_remove_all").is_ok());

    let keys = engine.kv_list("ns_remove_all", None).unwrap();
    assert!(keys.is_empty());
}

#[test]
fn test_kv_persistence_restart() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_str().unwrap();

    {
        let engine = make_engine_with_db(db_path);

        assert!(engine
            .create_namespace("ns_persist", "test", "user1")
            .is_ok());

        for i in 0..10 {
            let key = format!("persist_key_{}", i);
            let value = make_data(1024);
            assert!(engine.kv_put("ns_persist", &key, &value, "user1").is_ok());
        }
    }

    let engine2 = make_engine_with_db(db_path);

    assert!(engine2.get_namespace("ns_persist").is_some());

    for i in 0..10 {
        let key = format!("persist_key_{}", i);
        let expected_value = make_data(1024);
        let result = engine2.kv_get("ns_persist", &key);
        assert!(result.is_ok());
        assert!(result.as_ref().unwrap().is_some());
        assert_eq!(result.unwrap().unwrap().data, expected_value);
    }
}

#[test]
fn test_kv_concurrent_access() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(make_engine_with_db(dir.path().to_str().unwrap()));

    assert!(engine
        .create_namespace("ns_concurrent", "test", "user1")
        .is_ok());

    let data = Arc::new(make_data(4096));
    let mut handles = Vec::new();

    for i in 0..5 {
        let eng = engine.clone();
        let d = data.clone();
        handles.push(thread::spawn(move || {
            let mut keys = Vec::new();
            for j in 0..10 {
                let key = format!("concurrent_{}_{}", i, j);
                assert!(eng.kv_put("ns_concurrent", &key, &d, "user1").is_ok());
                keys.push(key);
            }
            for key in &keys {
                let result = eng.kv_get("ns_concurrent", key);
                assert!(result.is_ok());
                assert!(result.as_ref().unwrap().is_some());
                assert_eq!(result.unwrap().unwrap().data, *d);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let keys = engine.kv_list("ns_concurrent", None).unwrap();
    assert_eq!(keys.len(), 50);
}

#[test]
fn test_kv_data_integrity() {
    let dir = tempdir().unwrap();
    let engine = make_engine_with_db(dir.path().to_str().unwrap());

    assert!(engine
        .create_namespace("ns_integrity", "test", "user1")
        .is_ok());

    let mut rng = rand::thread_rng();
    for i in 0..100 {
        let key = format!("integrity_key_{}", i);
        let mut value = vec![0u8; 100];
        for byte in value.iter_mut() {
            *byte = rng.gen();
        }
        assert!(engine.kv_put("ns_integrity", &key, &value, "user1").is_ok());

        let result = engine.kv_get("ns_integrity", &key);
        assert!(result.is_ok());
        assert!(result.as_ref().unwrap().is_some());
        assert_eq!(result.unwrap().unwrap().data, value);
    }
}

#[test]
fn test_kv_invalid_namespace() {
    let dir = tempdir().unwrap();
    let engine = make_engine_with_db(dir.path().to_str().unwrap());

    let result = engine.kv_put("nonexistent", "key", b"value", "user1");
    assert!(result.is_err());

    let result = engine.kv_get("nonexistent", "key");
    assert!(result.is_err());

    let result = engine.kv_delete("nonexistent", "key");
    assert!(result.is_err());
}

#[test]
fn test_kv_owner_validation() {
    let dir = tempdir().unwrap();
    let engine = make_engine_with_db(dir.path().to_str().unwrap());

    assert!(engine.create_namespace("ns_owner", "test", "user1").is_ok());

    assert!(engine.kv_put("ns_owner", "key", b"value", "user1").is_ok());
    assert!(engine.kv_put("ns_owner", "key", b"value2", "user2").is_ok());

    let result = engine.kv_get("ns_owner", "key");
    assert!(result.is_ok());
    assert_eq!(result.unwrap().unwrap().owner_id, "user2");
}
