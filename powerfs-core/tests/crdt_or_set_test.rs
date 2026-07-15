use powerfs_core::crdt::or_set::{ORSet, ORSetKVStore, ReplicatedORSet, Tag};

#[test]
fn test_or_set_basic_insert() {
    let mut set: ORSet<String> = ORSet::new();
    let tag1 = Tag::new("replica1", 1);
    let tag2 = Tag::new("replica1", 2);

    set.insert("foo".to_string(), tag1);
    set.insert("bar".to_string(), tag2);

    assert!(set.contains(&"foo".to_string()));
    assert!(set.contains(&"bar".to_string()));
    assert!(!set.contains(&"baz".to_string()));
    assert_eq!(set.len(), 2);
}

#[test]
fn test_or_set_remove() {
    let mut set: ORSet<String> = ORSet::new();
    let tag = Tag::new("replica1", 1);

    set.insert("foo".to_string(), tag.clone());
    assert!(set.contains(&"foo".to_string()));

    set.remove_with_tag("foo".to_string(), tag);
    assert!(!set.contains(&"foo".to_string()));
    assert_eq!(set.len(), 0);
}

#[test]
fn test_or_set_merge_concurrent_adds() {
    let mut set1: ORSet<String> = ORSet::new();
    let mut set2: ORSet<String> = ORSet::new();

    set1.insert("foo".to_string(), Tag::new("replica1", 1));
    set1.insert("bar".to_string(), Tag::new("replica1", 2));

    set2.insert("foo".to_string(), Tag::new("replica2", 1));
    set2.insert("baz".to_string(), Tag::new("replica2", 2));

    set1.merge(&set2);

    assert!(set1.contains(&"foo".to_string()));
    assert!(set1.contains(&"bar".to_string()));
    assert!(set1.contains(&"baz".to_string()));
    assert_eq!(set1.len(), 3);
}

#[test]
fn test_or_set_merge_with_removal() {
    let mut set1: ORSet<String> = ORSet::new();
    let mut set2: ORSet<String> = ORSet::new();

    let tag1 = Tag::new("replica1", 1);
    let tag2 = Tag::new("replica2", 1);
    set1.insert("foo".to_string(), tag1.clone());
    set1.insert("bar".to_string(), Tag::new("replica1", 2));

    set2.insert("foo".to_string(), tag2.clone());
    set2.remove_with_tag("foo".to_string(), tag2);

    set1.merge(&set2);

    assert!(set1.contains(&"foo".to_string()));
    assert!(set1.contains(&"bar".to_string()));
    assert_eq!(set1.len(), 2);

    set1.remove_with_tag("foo".to_string(), tag1);

    assert!(!set1.contains(&"foo".to_string()));
    assert!(set1.contains(&"bar".to_string()));
    assert_eq!(set1.len(), 1);
}

#[test]
fn test_or_set_remove_all_tags() {
    let mut set: ORSet<String> = ORSet::new();

    set.insert("foo".to_string(), Tag::new("replica1", 1));
    set.insert("foo".to_string(), Tag::new("replica2", 1));

    assert!(set.contains(&"foo".to_string()));
    assert_eq!(set.len(), 1);

    set.remove(&"foo".to_string());

    assert!(!set.contains(&"foo".to_string()));
    assert_eq!(set.len(), 0);
}

#[test]
fn test_or_set_values() {
    let mut set: ORSet<String> = ORSet::new();

    set.insert("a".to_string(), Tag::new("r1", 1));
    set.insert("b".to_string(), Tag::new("r1", 2));
    set.insert("c".to_string(), Tag::new("r1", 3));

    let values = set.values();
    assert_eq!(values.len(), 3);
    assert!(values.contains(&"a".to_string()));
    assert!(values.contains(&"b".to_string()));
    assert!(values.contains(&"c".to_string()));
}

#[test]
fn test_replicated_or_set() {
    let set = ReplicatedORSet::new("replica1");

    set.insert("foo".to_string());
    set.insert("bar".to_string());

    assert!(set.contains(&"foo".to_string()));
    assert!(set.contains(&"bar".to_string()));
    assert_eq!(set.len(), 2);

    set.remove(&"foo".to_string());
    assert!(!set.contains(&"foo".to_string()));
    assert_eq!(set.len(), 1);
}

#[test]
fn test_replicated_or_set_counter() {
    let set = ReplicatedORSet::new("replica1");

    assert_eq!(set.get_counter(), 0);

    set.insert("a".to_string());
    assert_eq!(set.get_counter(), 1);

    set.insert("b".to_string());
    assert_eq!(set.get_counter(), 2);
}

#[test]
fn test_replicated_or_set_snapshot() {
    let set = ReplicatedORSet::new("replica1");

    set.insert("foo".to_string());
    set.insert("bar".to_string());

    let snapshot = set.snapshot();

    assert!(snapshot.contains(&"foo".to_string()));
    assert!(snapshot.contains(&"bar".to_string()));
    assert_eq!(snapshot.len(), 2);
}

#[test]
fn test_or_set_kv_store() {
    let store = ORSetKVStore::<String, String>::new("replica1");

    store.insert("key1".to_string(), "val1".to_string());
    store.insert("key1".to_string(), "val2".to_string());
    store.insert("key2".to_string(), "val3".to_string());

    assert!(store.contains(&"key1".to_string(), &"val1".to_string()));
    assert!(store.contains(&"key1".to_string(), &"val2".to_string()));
    assert!(store.contains(&"key2".to_string(), &"val3".to_string()));
    assert!(!store.contains(&"key1".to_string(), &"val3".to_string()));

    let values = store.get(&"key1".to_string()).unwrap();
    assert_eq!(values.len(), 2);
}

#[test]
fn test_or_set_kv_store_remove() {
    let store = ORSetKVStore::<String, String>::new("replica1");

    store.insert("key1".to_string(), "val1".to_string());
    store.insert("key1".to_string(), "val2".to_string());

    store.remove(&"key1".to_string(), &"val1".to_string());

    assert!(!store.contains(&"key1".to_string(), &"val1".to_string()));
    assert!(store.contains(&"key1".to_string(), &"val2".to_string()));
}

#[test]
fn test_or_set_merge_preserves_liveness() {
    let mut set1: ORSet<String> = ORSet::new();
    let mut set2: ORSet<String> = ORSet::new();

    let tag1 = Tag::new("r1", 1);
    let tag2 = Tag::new("r2", 1);

    set1.insert("foo".to_string(), tag1.clone());
    set2.insert("foo".to_string(), tag2.clone());

    set1.remove_with_tag("foo".to_string(), tag1);

    set1.merge(&set2);

    assert!(set1.contains(&"foo".to_string()));
}

#[test]
fn test_or_set_diff() {
    let mut set1: ORSet<String> = ORSet::new();
    let mut set2: ORSet<String> = ORSet::new();

    set1.insert("a".to_string(), Tag::new("r1", 1));
    set1.insert("b".to_string(), Tag::new("r1", 2));
    set1.insert("c".to_string(), Tag::new("r1", 3));

    set2.insert("a".to_string(), Tag::new("r1", 1));
    set2.insert("c".to_string(), Tag::new("r1", 3));
    set2.insert("d".to_string(), Tag::new("r1", 4));

    let diff = set1.diff(&set2);

    assert_eq!(diff, vec!["b".to_string()]);
}

#[test]
fn test_or_set_purge_removed() {
    let mut set: ORSet<String> = ORSet::new();

    let tag = Tag::new("r1", 1);
    set.insert("foo".to_string(), tag.clone());
    set.remove_with_tag("foo".to_string(), tag);

    assert_eq!(set.len(), 0);
    assert!(set.is_empty());
}

#[test]
fn test_or_set_clear() {
    let mut set: ORSet<String> = ORSet::new();

    set.insert("a".to_string(), Tag::new("r1", 1));
    set.insert("b".to_string(), Tag::new("r1", 2));

    set.clear();

    assert!(set.is_empty());
    assert_eq!(set.len(), 0);
}
