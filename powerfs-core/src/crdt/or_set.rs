use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Tag {
    replica_id: String,
    counter: u64,
}

impl Tag {
    pub fn new(replica_id: &str, counter: u64) -> Self {
        Self {
            replica_id: replica_id.to_string(),
            counter,
        }
    }

    pub fn replica_id(&self) -> &str {
        &self.replica_id
    }

    pub fn counter(&self) -> u64 {
        self.counter
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.replica_id, self.counter)
    }
}

#[derive(Debug, Clone)]
pub struct ORSet<T: Eq + Hash + Clone> {
    adds: HashMap<T, HashSet<Tag>>,
    removals: HashMap<T, HashSet<Tag>>,
}

impl<T: Eq + Hash + Clone> ORSet<T> {
    pub fn new() -> Self {
        Self {
            adds: HashMap::new(),
            removals: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.adds.is_empty()
    }

    pub fn len(&self) -> usize {
        self.adds
            .iter()
            .filter(|(value, tags)| {
                if let Some(removed) = self.removals.get(value) {
                    !tags.is_subset(removed)
                } else {
                    true
                }
            })
            .count()
    }

    pub fn contains(&self, value: &T) -> bool {
        if let Some(tags) = self.adds.get(value) {
            if let Some(removed) = self.removals.get(value) {
                !tags.is_subset(removed)
            } else {
                true
            }
        } else {
            false
        }
    }

    pub fn insert(&mut self, value: T, tag: Tag) {
        self.adds.entry(value).or_default().insert(tag);
    }

    pub fn insert_with_counter(&mut self, value: T, replica_id: &str, counter: u64) {
        let tag = Tag::new(replica_id, counter);
        self.insert(value, tag);
    }

    pub fn remove(&mut self, value: &T) {
        if let Some(tags) = self.adds.get(value) {
            let tags_clone: HashSet<Tag> = tags.clone();
            self.removals
                .entry(value.clone())
                .or_default()
                .extend(tags_clone);
        }
    }

    pub fn remove_with_tag(&mut self, value: T, tag: Tag) {
        let value_clone = value.clone();
        self.removals.entry(value).or_default().insert(tag);
        self.purge_removed_value(&value_clone);
    }

    fn purge_removed_value(&mut self, value: &T) {
        if let Some(tags) = self.adds.get(value) {
            if let Some(removed) = self.removals.get(value) {
                if tags.is_subset(removed) {
                    self.adds.remove(value);
                    self.removals.remove(value);
                }
            }
        }
    }

    pub fn values(&self) -> Vec<T> {
        self.adds
            .iter()
            .filter_map(|(value, tags)| {
                if let Some(removed) = self.removals.get(value) {
                    if !tags.is_subset(removed) {
                        Some(value.clone())
                    } else {
                        None
                    }
                } else {
                    Some(value.clone())
                }
            })
            .collect()
    }

    pub fn merge(&mut self, other: &Self) {
        for (value, tags) in &other.adds {
            let entry = self.adds.entry(value.clone()).or_default();
            for tag in tags {
                entry.insert(tag.clone());
            }
        }

        for (value, tags) in &other.removals {
            let entry = self.removals.entry(value.clone()).or_default();
            for tag in tags {
                entry.insert(tag.clone());
            }
        }

        self.purge_removed();
    }

    pub fn purge_removed(&mut self) {
        let mut to_remove = Vec::new();

        for (value, tags) in &self.adds {
            if let Some(removed) = self.removals.get(value) {
                if tags.is_subset(removed) {
                    to_remove.push(value.clone());
                }
            }
        }

        for value in to_remove {
            self.adds.remove(&value);
            self.removals.remove(&value);
        }
    }

    pub fn clear(&mut self) {
        self.adds.clear();
        self.removals.clear();
    }

    pub fn diff(&self, other: &Self) -> Vec<T> {
        let mut added = Vec::new();

        for (value, tags) in &self.adds {
            let is_present = if let Some(removed) = self.removals.get(value) {
                !tags.is_subset(removed)
            } else {
                true
            };

            let other_present = other.contains(value);

            if is_present && !other_present {
                added.push(value.clone());
            }
        }

        added
    }

    pub fn get_tags(&self, value: &T) -> Option<&HashSet<Tag>> {
        self.adds.get(value)
    }

    pub fn get_removals(&self, value: &T) -> Option<&HashSet<Tag>> {
        self.removals.get(value)
    }
}

impl<T: Eq + Hash + Clone> Default for ORSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Eq + Hash + Clone + fmt::Debug> fmt::Display for ORSet<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let values = self.values();
        write!(f, "ORSet({:?})", values)
    }
}

#[derive(Debug, Clone)]
pub struct ORSetDiff<T: Eq + Hash + Clone> {
    pub added: Vec<T>,
    pub removed: Vec<T>,
}

pub struct ReplicatedORSet<T: Eq + Hash + Clone + Send + Sync> {
    inner: Arc<RwLock<ORSet<T>>>,
    replica_id: String,
    counter: Arc<RwLock<u64>>,
}

impl<T: Eq + Hash + Clone + Send + Sync> ReplicatedORSet<T> {
    pub fn new(replica_id: &str) -> Self {
        Self {
            inner: Arc::new(RwLock::new(ORSet::new())),
            replica_id: replica_id.to_string(),
            counter: Arc::new(RwLock::new(0)),
        }
    }

    pub fn from_or_set(replica_id: &str, or_set: &ORSet<T>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(or_set.clone())),
            replica_id: replica_id.to_string(),
            counter: Arc::new(RwLock::new(0)),
        }
    }

    pub fn replica_id(&self) -> &str {
        &self.replica_id
    }

    pub fn insert(&self, value: T) {
        let mut counter = self.counter.write().unwrap();
        *counter += 1;
        let tag = Tag::new(&self.replica_id, *counter);
        drop(counter);

        let mut inner = self.inner.write().unwrap();
        inner.insert(value, tag);
    }

    pub fn insert_with_tag(&self, value: T, tag: Tag) {
        let mut inner = self.inner.write().unwrap();
        inner.insert(value, tag);
    }

    pub fn remove(&self, value: &T) {
        let mut inner = self.inner.write().unwrap();
        inner.remove(value);
    }

    pub fn remove_with_tag(&self, value: &T, tag: Tag) {
        let mut inner = self.inner.write().unwrap();
        inner.remove_with_tag(value.clone(), tag);
    }

    pub fn contains(&self, value: &T) -> bool {
        let inner = self.inner.read().unwrap();
        inner.contains(value)
    }

    pub fn values(&self) -> Vec<T> {
        let inner = self.inner.read().unwrap();
        inner.values()
    }

    pub fn len(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.len()
    }

    pub fn is_empty(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner.is_empty()
    }

    pub fn merge(&self, other: &ORSet<T>) {
        let mut inner = self.inner.write().unwrap();
        inner.merge(other);
    }

    pub fn snapshot(&self) -> ORSet<T> {
        let inner = self.inner.read().unwrap();
        inner.clone()
    }

    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.clear();
        let mut counter = self.counter.write().unwrap();
        *counter = 0;
    }

    pub fn get_counter(&self) -> u64 {
        *self.counter.read().unwrap()
    }
}

impl<T: Eq + Hash + Clone + Send + Sync> Clone for ReplicatedORSet<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            replica_id: self.replica_id.clone(),
            counter: Arc::clone(&self.counter),
        }
    }
}

pub struct ORSetKVStore<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + fmt::Debug,
    V: Eq + Hash + Clone + Send + Sync,
{
    data: Arc<RwLock<HashMap<K, ReplicatedORSet<V>>>>,
    replica_id: String,
}

impl<K, V> ORSetKVStore<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + fmt::Debug,
    V: Eq + Hash + Clone + Send + Sync,
{
    pub fn new(replica_id: &str) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            replica_id: replica_id.to_string(),
        }
    }

    pub fn insert(&self, key: K, value: V) {
        let mut data = self.data.write().unwrap();
        let or_set = data
            .entry(key)
            .or_insert_with(|| ReplicatedORSet::new(&self.replica_id));
        or_set.insert(value);
    }

    pub fn remove(&self, key: &K, value: &V) {
        let data = self.data.read().unwrap();
        if let Some(or_set) = data.get(key) {
            or_set.remove(value);
        }
    }

    pub fn contains(&self, key: &K, value: &V) -> bool {
        let data = self.data.read().unwrap();
        data.get(key).map(|s| s.contains(value)).unwrap_or(false)
    }

    pub fn get(&self, key: &K) -> Option<Vec<V>> {
        let data = self.data.read().unwrap();
        data.get(key).map(|s| s.values())
    }

    pub fn remove_key(&self, key: &K) {
        let mut data = self.data.write().unwrap();
        data.remove(key);
    }

    pub fn keys(&self) -> Vec<K> {
        let data = self.data.read().unwrap();
        data.keys().cloned().collect()
    }

    pub fn merge(&self, other: &HashMap<K, ORSet<V>>) {
        let mut data = self.data.write().unwrap();

        for (key, other_set) in other {
            let or_set = data
                .entry(key.clone())
                .or_insert_with(|| ReplicatedORSet::new(&self.replica_id));
            or_set.merge(other_set);
        }
    }

    pub fn snapshot(&self) -> HashMap<K, ORSet<V>> {
        let data = self.data.read().unwrap();
        data.iter()
            .map(|(k, v)| (k.clone(), v.snapshot()))
            .collect()
    }

    pub fn len(&self) -> usize {
        let data = self.data.read().unwrap();
        data.len()
    }

    pub fn is_empty(&self) -> bool {
        let data = self.data.read().unwrap();
        data.is_empty()
    }
}

mod serde_impl {
    use super::*;
    use serde::{Deserialize, Serialize};

    impl Serialize for Tag {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            serializer.serialize_str(&format!("{}:{}", self.replica_id, self.counter))
        }
    }

    impl<'de> Deserialize<'de> for Tag {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let s: String = Deserialize::deserialize(deserializer)?;
            let parts: Vec<&str> = s.splitn(2, ':').collect();
            if parts.len() == 2 {
                let replica_id = parts[0].to_string();
                let counter = parts[1]
                    .parse::<u64>()
                    .map_err(|e| serde::de::Error::custom(format!("Invalid counter: {}", e)))?;
                Ok(Tag {
                    replica_id,
                    counter,
                })
            } else {
                Err(serde::de::Error::custom("Invalid Tag format"))
            }
        }
    }

    impl<T: Eq + Hash + Clone + Serialize + for<'a> Deserialize<'a>> Serialize for ORSet<T> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            use serde::ser::SerializeStruct;
            let mut state = serializer.serialize_struct("ORSet", 2)?;
            state.serialize_field("adds", &self.adds)?;
            state.serialize_field("removals", &self.removals)?;
            state.end()
        }
    }

    impl<'de, T: Eq + Hash + Clone + Serialize + for<'a> Deserialize<'a>> Deserialize<'de>
        for ORSet<T>
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            #[derive(Deserialize)]
            #[serde(bound(deserialize = "T: Deserialize<'de> + Eq + Hash"))]
            struct ORSetSerdeRepr<T> {
                adds: HashMap<T, HashSet<Tag>>,
                removals: HashMap<T, HashSet<Tag>>,
            }
            let repr: ORSetSerdeRepr<T> = Deserialize::deserialize(deserializer)?;
            Ok(ORSet {
                adds: repr.adds,
                removals: repr.removals,
            })
        }
    }
}
