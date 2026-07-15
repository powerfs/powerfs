use crate::crdt::or_set::{ORSet, ReplicatedORSet, Tag};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaId(pub String);

impl ReplicaId {
    pub fn new(id: &str) -> Self {
        Self(id.to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorClock {
    entries: HashMap<ReplicaId, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn increment(&mut self, replica: &ReplicaId) {
        *self.entries.entry(replica.clone()).or_insert(0) += 1;
    }

    pub fn get(&self, replica: &ReplicaId) -> u64 {
        *self.entries.get(replica).unwrap_or(&0)
    }

    pub fn merge(&mut self, other: &VectorClock) {
        for (replica, &counter) in &other.entries {
            self.entries
                .entry(replica.clone())
                .and_modify(|c| *c = std::cmp::max(*c, counter))
                .or_insert(counter);
        }
    }

    pub fn is_greater_than(&self, other: &VectorClock) -> bool {
        let mut has_greater = false;
        for (replica, &counter) in &self.entries {
            let other_counter = other.get(replica);
            if counter < other_counter {
                return false;
            }
            if counter > other_counter {
                has_greater = true;
            }
        }
        has_greater || self.entries.len() > other.entries.len()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeltaOperation<K, V> {
    pub key: K,
    pub value: V,
    pub operation: DeltaOpType,
    pub tag: Tag,
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaOpType {
    Insert,
    Remove,
}

#[derive(Debug, Clone)]
pub struct KVReplica<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync,
    V: Eq + std::hash::Hash + Clone + Send + Sync,
{
    replica_id: ReplicaId,
    data: Arc<RwLock<HashMap<K, ReplicatedORSet<V>>>>,
    vector_clock: Arc<RwLock<VectorClock>>,
    pending_deltas: Arc<RwLock<Vec<DeltaOperation<K, V>>>>,
    listeners: Arc<RwLock<Vec<Box<dyn Fn(&DeltaOperation<K, V>) + Send + Sync + 'static>>>>,
}

impl<K, V> KVReplica<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + std::fmt::Debug,
    V: Eq + std::hash::Hash + Clone + Send + Sync,
{
    pub fn new(replica_id: &str) -> Self {
        Self {
            replica_id: ReplicaId::new(replica_id),
            data: Arc::new(RwLock::new(HashMap::new())),
            vector_clock: Arc::new(RwLock::new(VectorClock::new())),
            pending_deltas: Arc::new(RwLock::new(Vec::new())),
            listeners: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn get_replica_id(&self) -> &ReplicaId {
        &self.replica_id
    }

    pub fn insert(&self, key: K, value: V) {
        let mut data = self.data.write().unwrap();
        let mut clock = self.vector_clock.write().unwrap();
        let mut deltas = self.pending_deltas.write().unwrap();

        clock.increment(&self.replica_id);
        let counter = clock.get(&self.replica_id);
        let tag = Tag::new(&self.replica_id.0, counter);

        let or_set = data
            .entry(key.clone())
            .or_insert_with(|| ReplicatedORSet::new(&self.replica_id.0));
        or_set.insert_with_tag(value.clone(), tag.clone());

        let delta = DeltaOperation {
            key,
            value,
            operation: DeltaOpType::Insert,
            tag,
            timestamp: Self::current_timestamp(),
        };

        deltas.push(delta.clone());
        self.notify_listeners(&delta);
    }

    pub fn remove(&self, key: &K, value: &V) {
        let data = self.data.read().unwrap();
        if let Some(or_set) = data.get(key) {
            let mut clock = self.vector_clock.write().unwrap();
            let mut deltas = self.pending_deltas.write().unwrap();

            clock.increment(&self.replica_id);
            let counter = clock.get(&self.replica_id);
            let tag = Tag::new(&self.replica_id.0, counter);

            or_set.remove_with_tag(value, tag.clone());

            let delta = DeltaOperation {
                key: key.clone(),
                value: value.clone(),
                operation: DeltaOpType::Remove,
                tag,
                timestamp: Self::current_timestamp(),
            };

            deltas.push(delta.clone());
            self.notify_listeners(&delta);
        }
    }

    pub fn get(&self, key: &K) -> Option<Vec<V>> {
        let data = self.data.read().unwrap();
        data.get(key).map(|s| s.values())
    }

    pub fn contains(&self, key: &K, value: &V) -> bool {
        let data = self.data.read().unwrap();
        data.get(key).map(|s| s.contains(value)).unwrap_or(false)
    }

    pub fn get_pending_deltas(&self) -> Vec<DeltaOperation<K, V>> {
        let deltas = self.pending_deltas.read().unwrap();
        deltas.clone()
    }

    pub fn clear_pending_deltas(&self) {
        let mut deltas = self.pending_deltas.write().unwrap();
        deltas.clear();
    }

    pub fn sync_from(&self, source: &KVReplica<K, V>) {
        let source_deltas = source.get_pending_deltas();
        self.apply_deltas(&source_deltas);
    }

    pub fn apply_deltas(&self, deltas: &[DeltaOperation<K, V>]) {
        let mut data = self.data.write().unwrap();
        let mut clock = self.vector_clock.write().unwrap();
        let mut pending_deltas = self.pending_deltas.write().unwrap();

        for delta in deltas {
            match delta.operation {
                DeltaOpType::Insert => {
                    let or_set = data
                        .entry(delta.key.clone())
                        .or_insert_with(|| ReplicatedORSet::new(&self.replica_id.0));
                    or_set.insert_with_tag(delta.value.clone(), delta.tag.clone());
                }
                DeltaOpType::Remove => {
                    if let Some(or_set) = data.get_mut(&delta.key) {
                        or_set.remove_with_tag(&delta.value, delta.tag.clone());
                    }
                }
            }

            clock.merge(&VectorClock {
                entries: [(self.replica_id.clone(), delta.timestamp)].iter().cloned().collect(),
            });

            pending_deltas.push(delta.clone());
        }
    }

    pub fn snapshot(&self) -> (HashMap<K, ORSet<V>>, VectorClock) {
        let data = self.data.read().unwrap();
        let clock = self.vector_clock.read().unwrap();

        let snapshot_data: HashMap<K, ORSet<V>> = data
            .iter()
            .map(|(k, v)| (k.clone(), v.snapshot()))
            .collect();

        (snapshot_data, clock.clone())
    }

    pub fn restore_from_snapshot(&self, snapshot: &HashMap<K, ORSet<V>>, clock: &VectorClock) {
        let mut data = self.data.write().unwrap();
        let mut vector_clock = self.vector_clock.write().unwrap();
        let mut pending_deltas = self.pending_deltas.write().unwrap();

        data.clear();
        for (key, or_set) in snapshot {
            let replicated = ReplicatedORSet::from_or_set(&self.replica_id.0, or_set);
            data.insert(key.clone(), replicated);
        }

        vector_clock.merge(clock);
        pending_deltas.clear();
    }

    pub fn get_vector_clock(&self) -> VectorClock {
        let clock = self.vector_clock.read().unwrap();
        clock.clone()
    }

    pub fn subscribe<F>(&self, listener: F)
    where
        F: Fn(&DeltaOperation<K, V>) + Send + Sync + 'static,
    {
        let mut listeners = self.listeners.write().unwrap();
        listeners.push(Box::new(listener));
    }

    fn notify_listeners(&self, delta: &DeltaOperation<K, V>) {
        let listeners = self.listeners.read().unwrap();
        for listener in &*listeners {
            listener(delta);
        }
    }

    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
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

#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    pub replica_id: String,
    pub sync_interval_ms: u64,
    pub max_pending_deltas: usize,
    pub enable_delta_sync: bool,
}

impl Default for ReplicaConfig {
    fn default() -> Self {
        Self {
            replica_id: "replica-1".to_string(),
            sync_interval_ms: 2000,
            max_pending_deltas: 10000,
            enable_delta_sync: true,
        }
    }
}
