//! Server-side lease store: generic over a [`LeaseKey`] implementation.
//!
//! [`MemoryLeaseStore`] is the in-memory implementation, generalized from
//! PowerFS's `RangeLeaseManager`. It maintains three indexes for fast lookup:
//! - `leases`: token → entry
//! - `group_index`: group_id → tokens (for conflict checking within a group)
//! - `holder_index`: holder → tokens (for fast disconnect cleanup)

use crate::error::LeaseError;
use crate::persistence::{decode_entry, encode_entry, LeasePersistence};
use crate::token::LeaseMode;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Lease store statistics (snapshot at query time).
#[derive(Debug, Clone, Default)]
pub struct LeaseStats {
    /// Currently active (non-expired) leases.
    pub active_count: usize,
    /// Currently active unique holders.
    pub active_holders: u64,
    /// Total acquire calls (success + conflict).
    pub acquire_total: u64,
    /// Acquire calls that resulted in conflict.
    pub acquire_conflict_total: u64,
    /// Total successful renew calls.
    pub renew_total: u64,
    /// Total successful release calls.
    pub release_total: u64,
    /// Total leases removed by cleanup_expired.
    pub expired_total: u64,
    /// Total leases removed by disconnect_holder.
    pub disconnected_total: u64,
}

/// Trait for resource keys managed by the lease store.
///
/// Implementors define:
/// - `group_id`: a coarse grouping (e.g., inode number) for indexing — keys
///   in different groups never conflict, so conflict checks only scan the
///   same group.
/// - `conflicts`: whether two keys in the same group conflict (e.g., overlapping
///   stripe ranges).
/// - `encode`/`decode`: binary serialization for optional persistence.
pub trait LeaseKey: Clone + Eq + Hash + Send + Sync + 'static {
    /// Coarse group identifier for indexing (e.g., inode number).
    fn group_id(&self) -> u64;

    /// Whether this key conflicts with another key.
    /// Only called for keys in the same group.
    fn conflicts(&self, other: &Self) -> bool;

    /// Encode to bytes for persistence.
    fn encode(&self) -> Vec<u8>;

    /// Decode from bytes (for persistence load on startup).
    fn decode(data: &[u8]) -> Result<Self, crate::LeaseError>;
}

/// A granted lease entry.
#[derive(Debug, Clone)]
pub struct LeaseEntry<K: LeaseKey> {
    pub key: K,
    pub holder: String,
    pub token: String,
    pub mode: LeaseMode,
    pub acquired_at: Instant,
    pub expire_at: Instant,
    pub epoch: u64,
}

impl<K: LeaseKey> LeaseEntry<K> {
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.expire_at
    }

    pub fn is_expired_beyond(&self, grace: Duration) -> bool {
        Instant::now() > self.expire_at + grace
    }
}

/// Trait for server-side lease stores.
///
/// All methods are synchronous (designed to be called under a lock on the
/// volume server's request handler). The in-memory implementation is
/// [`MemoryLeaseStore`]; a persistent implementation can be added later.
pub trait LeaseStore<K: LeaseKey>: Send + Sync {
    fn acquire(
        &self,
        key: K,
        holder: &str,
        mode: LeaseMode,
        duration: Duration,
    ) -> Result<LeaseEntry<K>, LeaseError>;

    /// Atomically acquire leases for multiple keys.
    ///
    /// All-or-nothing: if any key conflicts with an existing lease held by
    /// a different holder, the entire batch fails and no leases are granted.
    /// This prevents partial acquisition where a client obtains some leases
    /// but not others, which can lead to deadlocks or inconsistent state.
    ///
    /// Default implementation loops over `acquire` (non-atomic); stores that
    /// support atomic batch acquire should override this.
    fn acquire_batch(
        &self,
        keys: &[K],
        holder: &str,
        mode: LeaseMode,
        duration: Duration,
    ) -> Result<Vec<LeaseEntry<K>>, LeaseError> {
        let mut entries = Vec::with_capacity(keys.len());
        for key in keys {
            match self.acquire(key.clone(), holder, mode, duration) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    // Rollback: release already-acquired leases
                    for entry in &entries {
                        let _ = self.release(&entry.token, holder);
                    }
                    return Err(e);
                }
            }
        }
        Ok(entries)
    }

    fn renew(&self, token: &str, holder: &str, duration: Duration) -> Result<(), LeaseError>;

    fn release(&self, token: &str, holder: &str) -> Result<(), LeaseError>;

    fn validate_token(&self, token: &str, holder: &str) -> Result<(), LeaseError>;

    fn validate_token_with_grace(
        &self,
        token: &str,
        holder: &str,
        grace: Duration,
    ) -> Result<(), LeaseError>;

    fn get_entry(&self, token: &str) -> Option<LeaseEntry<K>>;

    fn get_entries_by_group(&self, group_id: u64) -> Vec<LeaseEntry<K>>;

    fn get_entries_by_holder(&self, holder: &str) -> Vec<LeaseEntry<K>>;

    fn disconnect_holder(&self, holder: &str) -> usize;

    fn cleanup_expired(&self) -> usize;

    fn active_count(&self) -> usize;

    fn active_holders_count(&self) -> u64;

    fn shutdown_flag(&self) -> Arc<AtomicBool>;

    fn request_shutdown(&self);
}

/// In-memory lease store, generic over key type `K`.
///
/// Generalized from PowerFS's `RangeLeaseManager`. Maintains three indexes
/// for O(1) token lookup and O(leases_per_group) conflict checking.
pub struct MemoryLeaseStore<K: LeaseKey> {
    leases: Arc<RwLock<HashMap<String, LeaseEntry<K>>>>,
    group_index: Arc<RwLock<HashMap<u64, Vec<String>>>>,
    holder_index: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    holder_count: Arc<AtomicU64>,
    epoch_counter: Arc<AtomicU64>,
    shutdown_flag: Arc<AtomicBool>,
    /// Grace period for cleanup_expired: leases expired within this window
    /// are NOT removed, so validate_token_with_grace can still find them.
    cleanup_grace: Duration,
    /// Optional persistence backend. When present, acquire/renew/release
    /// operations are also persisted for crash recovery.
    persistence: Option<Arc<dyn LeasePersistence>>,
    // --- Monitoring counters ---
    acquire_total: AtomicU64,
    acquire_conflict_total: AtomicU64,
    renew_total: AtomicU64,
    release_total: AtomicU64,
    expired_total: AtomicU64,
    disconnected_total: AtomicU64,
}

impl<K: LeaseKey> MemoryLeaseStore<K> {
    pub fn new() -> Self {
        Self {
            leases: Arc::new(RwLock::new(HashMap::new())),
            group_index: Arc::new(RwLock::new(HashMap::new())),
            holder_index: Arc::new(RwLock::new(HashMap::new())),
            holder_count: Arc::new(AtomicU64::new(0)),
            epoch_counter: Arc::new(AtomicU64::new(0)),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            cleanup_grace: Duration::from_millis(5000),
            persistence: None,
            acquire_total: AtomicU64::new(0),
            acquire_conflict_total: AtomicU64::new(0),
            renew_total: AtomicU64::new(0),
            release_total: AtomicU64::new(0),
            expired_total: AtomicU64::new(0),
            disconnected_total: AtomicU64::new(0),
        }
    }

    /// Get current lease statistics (snapshot).
    ///
    /// Active counts (`active_count`, `active_holders`) are computed live;
    /// counters (`*_total`) are monotonically increasing cumulative totals
    /// since store creation.
    pub fn stats(&self) -> LeaseStats {
        LeaseStats {
            active_count: self.active_count(),
            active_holders: self.active_holders_count(),
            acquire_total: self.acquire_total.load(Ordering::Relaxed),
            acquire_conflict_total: self.acquire_conflict_total.load(Ordering::Relaxed),
            renew_total: self.renew_total.load(Ordering::Relaxed),
            release_total: self.release_total.load(Ordering::Relaxed),
            expired_total: self.expired_total.load(Ordering::Relaxed),
            disconnected_total: self.disconnected_total.load(Ordering::Relaxed),
        }
    }

    pub fn with_cleanup_grace(mut self, grace: Duration) -> Self {
        self.cleanup_grace = grace;
        self
    }

    /// Enable persistence backend. After this, acquire/renew/release will
    /// also persist to the backend, and `load_from_persistence` can be used
    /// on startup to recover lease state.
    pub fn with_persistence<P: LeasePersistence + 'static>(mut self, backend: P) -> Self {
        self.persistence = Some(Arc::new(backend));
        self
    }

    /// Load non-expired leases from the persistence backend.
    /// Called on Volume Server startup to recover lease state after crash.
    ///
    /// Also restores the epoch counter to prevent fence token ABA.
    pub fn load_from_persistence(&self) -> Result<usize, LeaseError> {
        let persistence = self
            .persistence
            .as_ref()
            .ok_or_else(|| LeaseError::Internal("no persistence backend configured".into()))?;

        // Restore epoch counter
        if let Ok(stored_epoch) = persistence.load_epoch() {
            // Set epoch_counter to max(current, stored) + 1 to avoid reuse
            let current = self.epoch_counter.load(Ordering::Relaxed);
            if stored_epoch > current {
                self.epoch_counter
                    .store(stored_epoch + 1, Ordering::Relaxed);
            }
        }

        // Load all lease entries
        let entries = persistence.load_all()?;
        let mut restored = 0usize;

        for (token, data) in entries {
            match decode_entry::<K>(&data) {
                Ok(Some(entry)) => {
                    let group_id = entry.key.group_id();
                    let mut leases = self.leases.write().unwrap();
                    let mut group_index = self.group_index.write().unwrap();
                    let mut holder_index = self.holder_index.write().unwrap();

                    // Update epoch counter if loaded entry has higher epoch
                    if entry.epoch >= self.epoch_counter.load(Ordering::Relaxed) {
                        self.epoch_counter.store(entry.epoch + 1, Ordering::Relaxed);
                    }

                    let holder_name = entry.holder.clone();
                    let tok = entry.token.clone();

                    leases.insert(token.clone(), entry);
                    group_index.entry(group_id).or_default().push(token.clone());

                    let holder_set = holder_index.entry(holder_name).or_default();
                    let is_new = holder_set.is_empty();
                    holder_set.insert(tok);
                    if is_new {
                        self.holder_count.fetch_add(1, Ordering::Relaxed);
                    }

                    restored += 1;
                }
                Ok(None) => {
                    // Expired — delete from persistence
                    let _ = persistence.delete(&token);
                }
                Err(e) => {
                    log::warn!("Failed to decode lease entry on load: {}", e);
                }
            }
        }

        log::info!("Loaded {} lease entries from persistence", restored);
        Ok(restored)
    }

    fn generate_token(&self) -> (String, u64) {
        let epoch = self.epoch_counter.fetch_add(1, Ordering::Relaxed);
        let id = uuid::Uuid::new_v4();
        (format!("lease-{}-{}", epoch, id), epoch)
    }

    /// Persist epoch counter to backend (best-effort, called periodically).
    pub fn persist_epoch(&self) -> Result<(), LeaseError> {
        if let Some(p) = &self.persistence {
            let epoch = self.epoch_counter.load(Ordering::Relaxed);
            p.save_epoch(epoch)?;
        }
        Ok(())
    }
}

impl<K: LeaseKey> Default for MemoryLeaseStore<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: LeaseKey> LeaseStore<K> for MemoryLeaseStore<K> {
    fn acquire(
        &self,
        key: K,
        holder: &str,
        mode: LeaseMode,
        duration: Duration,
    ) -> Result<LeaseEntry<K>, LeaseError> {
        let now = Instant::now();
        let expire_at = now + duration;
        let group_id = key.group_id();

        let mut leases = self.leases.write().unwrap();
        let mut group_index = self.group_index.write().unwrap();
        let mut holder_index = self.holder_index.write().unwrap();

        // Check for conflicts with existing leases in the same group
        if let Some(existing_tokens) = group_index.get(&group_id) {
            for token in existing_tokens.iter() {
                if let Some(existing) = leases.get(token) {
                    if existing.is_expired() {
                        continue;
                    }
                    if existing.holder == holder {
                        continue;
                    }
                    // Conflict if either side is exclusive AND keys overlap
                    if (existing.mode.is_exclusive() || mode.is_exclusive())
                        && existing.key.conflicts(&key)
                    {
                        self.acquire_total.fetch_add(1, Ordering::Relaxed);
                        self.acquire_conflict_total.fetch_add(1, Ordering::Relaxed);
                        return Err(LeaseError::Conflict(format!(
                            "key group {} conflicts with existing lease held by {}",
                            group_id, existing.holder
                        )));
                    }
                }
            }
        }

        // Clean up expired leases for this group (inline housekeeping)
        if let Some(tokens) = group_index.get_mut(&group_id) {
            tokens.retain(|t| leases.get(t).map(|l| !l.is_expired()).unwrap_or(false));
        }

        let (token, epoch) = self.generate_token();
        let entry = LeaseEntry {
            key: key.clone(),
            holder: holder.to_string(),
            token: token.clone(),
            mode,
            acquired_at: now,
            expire_at,
            epoch,
        };

        leases.insert(token.clone(), entry.clone());
        group_index.entry(group_id).or_default().push(token.clone());

        // Update holder index
        let holder_entry = holder_index.entry(holder.to_string()).or_default();
        let is_new_holder = holder_entry.is_empty();
        holder_entry.insert(token.clone());
        if is_new_holder {
            self.holder_count.fetch_add(1, Ordering::Relaxed);
        }

        // Persist to backend (best-effort)
        if let Some(p) = &self.persistence {
            let data = encode_entry(&entry);
            if let Err(e) = p.save(&token, &data) {
                log::warn!("lease persistence save failed on acquire: {}", e);
            }
        }

        self.acquire_total.fetch_add(1, Ordering::Relaxed);
        Ok(entry)
    }

    /// Atomic batch acquire: all keys are checked for conflicts in a single
    /// lock scope. If any key conflicts, NONE are acquired (all-or-nothing).
    ///
    /// Also checks for internal conflicts between batch keys themselves
    /// (e.g., two keys in the batch that overlap with each other in
    /// exclusive mode).
    fn acquire_batch(
        &self,
        keys: &[K],
        holder: &str,
        mode: LeaseMode,
        duration: Duration,
    ) -> Result<Vec<LeaseEntry<K>>, LeaseError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let now = Instant::now();
        let expire_at = now + duration;

        let mut leases = self.leases.write().unwrap();
        let mut group_index = self.group_index.write().unwrap();
        let mut holder_index = self.holder_index.write().unwrap();

        // Phase 1: Check each key against existing leases AND against other
        // keys in this batch (internal conflict check).
        //
        // Track pending entries separately so we can abort without side
        // effects if any conflict is found.
        let mut pending: Vec<LeaseEntry<K>> = Vec::with_capacity(keys.len());

        for key in keys {
            let group_id = key.group_id();

            // Check against existing leases in the same group
            if let Some(existing_tokens) = group_index.get(&group_id) {
                for token in existing_tokens.iter() {
                    if let Some(existing) = leases.get(token) {
                        if existing.is_expired() {
                            continue;
                        }
                        if existing.holder == holder {
                            continue;
                        }
                        if (existing.mode.is_exclusive() || mode.is_exclusive())
                            && existing.key.conflicts(key)
                        {
                            self.acquire_total.fetch_add(1, Ordering::Relaxed);
                            self.acquire_conflict_total.fetch_add(1, Ordering::Relaxed);
                            return Err(LeaseError::Conflict(format!(
                                "batch: key group {} conflicts with existing lease held by {}",
                                group_id, existing.holder
                            )));
                        }
                    }
                }
            }

            // Check against already-pending keys in this batch (internal
            // conflict). Two exclusive keys that overlap cannot both be
            // granted.
            if mode.is_exclusive() {
                for p in &pending {
                    if p.key.group_id() == group_id && p.key.conflicts(key) {
                        self.acquire_total.fetch_add(1, Ordering::Relaxed);
                        self.acquire_conflict_total.fetch_add(1, Ordering::Relaxed);
                        return Err(LeaseError::Conflict(format!(
                            "batch: internal conflict between keys in group {}",
                            group_id
                        )));
                    }
                }
            }

            let (token, epoch) = self.generate_token();
            let entry = LeaseEntry {
                key: key.clone(),
                holder: holder.to_string(),
                token: token.clone(),
                mode,
                acquired_at: now,
                expire_at,
                epoch,
            };
            pending.push(entry);
        }

        // Phase 2: All checks passed — commit all pending entries.
        let mut result = Vec::with_capacity(pending.len());
        let mut new_holder = false;
        for entry in pending {
            let token = entry.token.clone();
            let group_id = entry.key.group_id();

            leases.insert(token.clone(), entry.clone());
            group_index.entry(group_id).or_default().push(token.clone());

            let holder_entry = holder_index.entry(holder.to_string()).or_default();
            if holder_entry.is_empty() {
                new_holder = true;
            }
            holder_entry.insert(token.clone());

            // Persist (best-effort)
            if let Some(p) = &self.persistence {
                let data = encode_entry(&entry);
                if let Err(e) = p.save(&token, &data) {
                    log::warn!("lease persistence save failed on acquire_batch: {}", e);
                }
            }

            result.push(entry);
        }

        if new_holder {
            self.holder_count.fetch_add(1, Ordering::Relaxed);
        }
        self.acquire_total
            .fetch_add(result.len() as u64, Ordering::Relaxed);
        Ok(result)
    }

    fn renew(&self, token: &str, holder: &str, duration: Duration) -> Result<(), LeaseError> {
        let mut leases = self.leases.write().unwrap();
        match leases.get_mut(token) {
            Some(entry) => {
                if entry.holder != holder {
                    return Err(LeaseError::HolderMismatch {
                        expected: entry.holder.clone(),
                        actual: holder.to_string(),
                    });
                }
                entry.expire_at = Instant::now() + duration;
                entry.epoch = self.epoch_counter.fetch_add(1, Ordering::Relaxed);

                // Persist updated entry (best-effort)
                if let Some(p) = &self.persistence {
                    let data = encode_entry(entry);
                    if let Err(e) = p.save(token, &data) {
                        log::warn!("lease persistence save failed on renew: {}", e);
                    }
                }

                self.renew_total.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            None => Err(LeaseError::NotFound),
        }
    }

    fn release(&self, token: &str, holder: &str) -> Result<(), LeaseError> {
        let group_id = {
            let mut leases = self.leases.write().unwrap();
            let entry = leases.get(token).ok_or(LeaseError::NotFound)?;

            if entry.holder != holder {
                return Err(LeaseError::HolderMismatch {
                    expected: entry.holder.clone(),
                    actual: holder.to_string(),
                });
            }

            let group_id = entry.key.group_id();
            leases.remove(token);
            group_id
        };

        // Update group index
        if let Some(tokens) = self.group_index.write().unwrap().get_mut(&group_id) {
            tokens.retain(|t| t != token);
        }

        // Update holder index
        let mut holder_index = self.holder_index.write().unwrap();
        if let Some(tokens) = holder_index.get_mut(holder) {
            tokens.remove(token);
            if tokens.is_empty() {
                holder_index.remove(holder);
                self.holder_count.fetch_sub(1, Ordering::Relaxed);
            }
        }

        // Persist deletion (best-effort)
        if let Some(p) = &self.persistence {
            if let Err(e) = p.delete(token) {
                log::warn!("lease persistence delete failed on release: {}", e);
            }
        }

        self.release_total.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn validate_token(&self, token: &str, holder: &str) -> Result<(), LeaseError> {
        let leases = self.leases.read().unwrap();
        let entry = leases.get(token).ok_or(LeaseError::NotFound)?;

        if entry.is_expired() {
            return Err(LeaseError::Expired);
        }
        if entry.holder != holder {
            return Err(LeaseError::HolderMismatch {
                expected: entry.holder.clone(),
                actual: holder.to_string(),
            });
        }
        Ok(())
    }

    fn validate_token_with_grace(
        &self,
        token: &str,
        holder: &str,
        grace: Duration,
    ) -> Result<(), LeaseError> {
        let leases = self.leases.read().unwrap();
        let entry = leases.get(token).ok_or(LeaseError::NotFound)?;

        if entry.holder != holder {
            return Err(LeaseError::HolderMismatch {
                expected: entry.holder.clone(),
                actual: holder.to_string(),
            });
        }

        if Instant::now() > entry.expire_at + grace {
            return Err(LeaseError::ExpiredBeyondGrace);
        }
        Ok(())
    }

    fn get_entry(&self, token: &str) -> Option<LeaseEntry<K>> {
        self.leases.read().unwrap().get(token).cloned()
    }

    fn get_entries_by_group(&self, group_id: u64) -> Vec<LeaseEntry<K>> {
        let leases = self.leases.read().unwrap();
        let group_index = self.group_index.read().unwrap();
        let mut result = Vec::new();

        if let Some(tokens) = group_index.get(&group_id) {
            for token in tokens {
                if let Some(entry) = leases.get(token) {
                    if !entry.is_expired() {
                        result.push(entry.clone());
                    }
                }
            }
        }
        result
    }

    fn get_entries_by_holder(&self, holder: &str) -> Vec<LeaseEntry<K>> {
        let leases = self.leases.read().unwrap();
        let holder_index = self.holder_index.read().unwrap();
        let mut result = Vec::new();

        if let Some(tokens) = holder_index.get(holder) {
            for token in tokens {
                if let Some(entry) = leases.get(token) {
                    if !entry.is_expired() {
                        result.push(entry.clone());
                    }
                }
            }
        }
        result
    }

    fn disconnect_holder(&self, holder: &str) -> usize {
        let mut leases = self.leases.write().unwrap();
        let mut group_index = self.group_index.write().unwrap();
        let mut holder_index = self.holder_index.write().unwrap();

        let tokens_to_remove: Vec<String> = holder_index
            .get(holder)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();

        let mut removed = 0usize;
        let mut removed_tokens: Vec<String> = Vec::new();
        for token in &tokens_to_remove {
            if let Some(entry) = leases.remove(token) {
                removed += 1;
                removed_tokens.push(token.clone());
                let group_id = entry.key.group_id();
                if let Some(tokens) = group_index.get_mut(&group_id) {
                    tokens.retain(|t| t != token);
                    if tokens.is_empty() {
                        group_index.remove(&group_id);
                    }
                }
            }
        }

        // Clean up holder entry if empty
        if let Some(tokens) = holder_index.get_mut(holder) {
            if tokens.is_empty() {
                holder_index.remove(holder);
                self.holder_count.fetch_sub(1, Ordering::Relaxed);
            }
        }

        // Persist deletions (best-effort)
        if let Some(p) = &self.persistence {
            for token in &removed_tokens {
                let _ = p.delete(token);
            }
        }

        if removed > 0 {
            self.disconnected_total
                .fetch_add(removed as u64, Ordering::Relaxed);
        }
        removed
    }

    fn cleanup_expired(&self) -> usize {
        let mut leases = self.leases.write().unwrap();
        let mut group_index = self.group_index.write().unwrap();
        let mut holder_index = self.holder_index.write().unwrap();
        let mut removed = 0usize;

        // Only remove leases expired BEYOND the grace period.
        // Leases within the grace period are kept so that
        // validate_token_with_grace can still find them.
        let grace = self.cleanup_grace;
        let now = Instant::now();
        let expired_tokens: Vec<String> = leases
            .iter()
            .filter(|(_, e)| now > e.expire_at + grace)
            .map(|(t, _)| t.clone())
            .collect();

        for token in expired_tokens {
            if let Some(entry) = leases.remove(&token) {
                removed += 1;
                let group_id = entry.key.group_id();
                // Remove from group index
                if let Some(tokens) = group_index.get_mut(&group_id) {
                    tokens.retain(|t| t != &token);
                    if tokens.is_empty() {
                        group_index.remove(&group_id);
                    }
                }
                // Remove from holder index
                if let Some(tokens) = holder_index.get_mut(&entry.holder) {
                    tokens.remove(&token);
                    if tokens.is_empty() {
                        holder_index.remove(&entry.holder);
                        self.holder_count.fetch_sub(1, Ordering::Relaxed);
                    }
                }

                // Persist deletion (best-effort)
                if let Some(p) = &self.persistence {
                    let _ = p.delete(&token);
                }
            }
        }
        if removed > 0 {
            self.expired_total
                .fetch_add(removed as u64, Ordering::Relaxed);
        }
        removed
    }

    fn active_count(&self) -> usize {
        self.leases
            .read()
            .unwrap()
            .values()
            .filter(|e| !e.is_expired())
            .count()
    }

    fn active_holders_count(&self) -> u64 {
        self.holder_count.load(Ordering::Relaxed)
    }

    fn shutdown_flag(&self) -> Arc<AtomicBool> {
        self.shutdown_flag.clone()
    }

    fn request_shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    struct TestKey {
        id: u64,
        start: u64,
        count: u64,
    }

    impl LeaseKey for TestKey {
        fn group_id(&self) -> u64 {
            self.id
        }
        fn conflicts(&self, other: &Self) -> bool {
            if self.id != other.id {
                return false;
            }
            let self_end = self.start + self.count;
            let other_end = other.start + other.count;
            self.start < other_end && other.start < self_end
        }
        fn encode(&self) -> Vec<u8> {
            let mut buf = Vec::with_capacity(24);
            buf.extend_from_slice(&self.id.to_le_bytes());
            buf.extend_from_slice(&self.start.to_le_bytes());
            buf.extend_from_slice(&self.count.to_le_bytes());
            buf
        }
        fn decode(data: &[u8]) -> Result<Self, LeaseError> {
            if data.len() < 24 {
                return Err(LeaseError::Internal("key too short".into()));
            }
            Ok(Self {
                id: u64::from_le_bytes(data[0..8].try_into().unwrap()),
                start: u64::from_le_bytes(data[8..16].try_into().unwrap()),
                count: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            })
        }
    }

    #[test]
    fn test_acquire_and_release() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let entry = store
            .acquire(
                key,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        assert_eq!(entry.holder, "client-a");

        store.release(&entry.token, "client-a").unwrap();
        assert_eq!(store.active_count(), 0);
    }

    #[test]
    fn test_conflict_detection() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key1 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key2 = TestKey {
            id: 1,
            start: 2,
            count: 4,
        };
        store
            .acquire(
                key1,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        let result = store.acquire(
            key2,
            "client-b",
            LeaseMode::Exclusive,
            Duration::from_secs(30),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_same_holder_no_conflict() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key1 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key2 = TestKey {
            id: 1,
            start: 2,
            count: 4,
        };
        let e1 = store
            .acquire(
                key1,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        let e2 = store
            .acquire(
                key2,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        assert_ne!(e1.token, e2.token);
    }

    #[test]
    fn test_non_overlapping_no_conflict() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key1 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key2 = TestKey {
            id: 1,
            start: 4,
            count: 4,
        };
        store
            .acquire(
                key1,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        let result = store.acquire(
            key2,
            "client-b",
            LeaseMode::Exclusive,
            Duration::from_secs(30),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validation() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let entry = store
            .acquire(
                key,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();

        assert!(store.validate_token(&entry.token, "client-a").is_ok());
        assert!(store.validate_token(&entry.token, "client-b").is_err());
        assert!(store.validate_token("bad-token", "client-a").is_err());
    }

    #[test]
    fn test_renew() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let entry = store
            .acquire(
                key,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_millis(1000),
            )
            .unwrap();
        store
            .renew(&entry.token, "client-a", Duration::from_secs(30))
            .unwrap();
        assert_eq!(store.active_count(), 1);
    }

    #[test]
    fn test_expired_cleanup() {
        let store = MemoryLeaseStore::<TestKey>::new().with_cleanup_grace(Duration::from_millis(0));
        let key = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let _entry = store
            .acquire(
                key,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_millis(1),
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(10));
        let removed = store.cleanup_expired();
        assert!(removed >= 1);
        assert_eq!(store.active_count(), 0);
    }

    #[test]
    fn test_shared_lease_multiple_holders() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key1 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key2 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        // Shared (read) lease should allow multiple holders
        let _l1 = store
            .acquire(key1, "client-a", LeaseMode::Shared, Duration::from_secs(30))
            .unwrap();
        let _l2 = store
            .acquire(key2, "client-b", LeaseMode::Shared, Duration::from_secs(30))
            .unwrap();
        assert_eq!(store.active_count(), 2);
    }

    #[test]
    fn test_disconnect_holder() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key1 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key2 = TestKey {
            id: 2,
            start: 0,
            count: 4,
        };
        store
            .acquire(
                key1,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        store
            .acquire(
                key2,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        assert_eq!(store.active_count(), 2);

        let removed = store.disconnect_holder("client-a");
        assert_eq!(removed, 2);
        assert_eq!(store.active_count(), 0);
    }

    #[test]
    fn test_get_entries_by_group() {
        let store = MemoryLeaseStore::<TestKey>::new();
        let key1 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key2 = TestKey {
            id: 1,
            start: 4,
            count: 4,
        };
        let key3 = TestKey {
            id: 2,
            start: 0,
            count: 4,
        };
        store
            .acquire(
                key1,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        store
            .acquire(
                key2,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        store
            .acquire(
                key3,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();

        assert_eq!(store.get_entries_by_group(1).len(), 2);
        assert_eq!(store.get_entries_by_group(2).len(), 1);
        assert_eq!(store.get_entries_by_group(3).len(), 0);
    }

    #[test]
    fn test_stats_counters() {
        let store = MemoryLeaseStore::<TestKey>::new().with_cleanup_grace(Duration::from_millis(0));
        let key_a = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key_b = TestKey {
            id: 1,
            start: 2,
            count: 4,
        };

        // Initial stats: all zero
        let s0 = store.stats();
        assert_eq!(s0.active_count, 0);
        assert_eq!(s0.active_holders, 0);
        assert_eq!(s0.acquire_total, 0);
        assert_eq!(s0.acquire_conflict_total, 0);
        assert_eq!(s0.renew_total, 0);
        assert_eq!(s0.release_total, 0);
        assert_eq!(s0.expired_total, 0);
        assert_eq!(s0.disconnected_total, 0);

        // acquire (success)
        let entry = store
            .acquire(
                key_a,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();

        // acquire (conflict)
        let conflict = store.acquire(
            key_b,
            "client-b",
            LeaseMode::Exclusive,
            Duration::from_secs(30),
        );
        assert!(conflict.is_err());

        let s1 = store.stats();
        assert_eq!(s1.active_count, 1);
        assert_eq!(s1.active_holders, 1);
        assert_eq!(s1.acquire_total, 2); // success + conflict
        assert_eq!(s1.acquire_conflict_total, 1);
        assert_eq!(s1.renew_total, 0);
        assert_eq!(s1.release_total, 0);

        // renew
        store
            .renew(&entry.token, "client-a", Duration::from_secs(30))
            .unwrap();
        let s2 = store.stats();
        assert_eq!(s2.renew_total, 1);

        // release
        store.release(&entry.token, "client-a").unwrap();
        let s3 = store.stats();
        assert_eq!(s3.release_total, 1);
        assert_eq!(s3.active_count, 0);
        assert_eq!(s3.active_holders, 0);
    }

    #[test]
    fn test_stats_expired_and_disconnected() {
        let store = MemoryLeaseStore::<TestKey>::new().with_cleanup_grace(Duration::from_millis(0));
        let key1 = TestKey {
            id: 1,
            start: 0,
            count: 4,
        };
        let key2 = TestKey {
            id: 2,
            start: 0,
            count: 4,
        };

        // Two leases, different groups, same holder
        store
            .acquire(
                key1,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_millis(1),
            )
            .unwrap();
        store
            .acquire(
                key2,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();

        // Wait for first to expire
        std::thread::sleep(Duration::from_millis(10));
        let removed = store.cleanup_expired();
        assert!(removed >= 1);
        assert!(store.stats().expired_total >= 1);

        // disconnect_holder removes the remaining active lease
        let removed = store.disconnect_holder("client-a");
        assert_eq!(removed, 1);
        assert_eq!(store.stats().disconnected_total, 1);
        assert_eq!(store.stats().active_count, 0);
    }

    #[test]
    fn test_acquire_batch_success() {
        let store = MemoryLeaseStore::<TestKey>::new().with_cleanup_grace(Duration::from_millis(0));

        // Acquire 3 non-overlapping stripes in one batch
        let keys = vec![
            TestKey {
                id: 1,
                start: 0,
                count: 4,
            },
            TestKey {
                id: 1,
                start: 4,
                count: 4,
            },
            TestKey {
                id: 1,
                start: 8,
                count: 4,
            },
        ];

        let entries = store
            .acquire_batch(
                &keys,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();

        assert_eq!(entries.len(), 3);
        assert!(store.stats().active_count >= 3);
        assert!(store.stats().acquire_total >= 3);

        // All tokens should be distinct
        assert_ne!(entries[0].token, entries[1].token);
        assert_ne!(entries[1].token, entries[2].token);
    }

    #[test]
    fn test_acquire_batch_conflict_all_or_nothing() {
        let store = MemoryLeaseStore::<TestKey>::new().with_cleanup_grace(Duration::from_millis(0));

        // Client A acquires stripe [0, 4)
        store
            .acquire(
                TestKey {
                    id: 1,
                    start: 0,
                    count: 4,
                },
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();

        // Client B tries batch: [8, 12) + [2, 6) — second conflicts with A
        let keys = vec![
            TestKey {
                id: 1,
                start: 8,
                count: 4,
            },
            TestKey {
                id: 1,
                start: 2,
                count: 4,
            }, // overlaps with [0, 4)
        ];

        let result = store.acquire_batch(
            &keys,
            "client-b",
            LeaseMode::Exclusive,
            Duration::from_secs(30),
        );
        assert!(result.is_err());

        // All-or-nothing: the non-conflicting key [8, 12) must NOT be acquired
        assert_eq!(store.active_count(), 1); // only client-a's lease
        assert!(store.stats().acquire_conflict_total >= 1);

        // Client B can now acquire [8, 12) alone (no conflict)
        store
            .acquire(
                TestKey {
                    id: 1,
                    start: 8,
                    count: 4,
                },
                "client-b",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        assert_eq!(store.active_count(), 2);
    }

    #[test]
    fn test_acquire_batch_internal_conflict() {
        let store = MemoryLeaseStore::<TestKey>::new().with_cleanup_grace(Duration::from_millis(0));

        // Batch with two overlapping exclusive keys — internal conflict
        let keys = vec![
            TestKey {
                id: 1,
                start: 0,
                count: 4,
            },
            TestKey {
                id: 1,
                start: 2,
                count: 4,
            }, // overlaps with [0, 4)
        ];

        let result = store.acquire_batch(
            &keys,
            "client-a",
            LeaseMode::Exclusive,
            Duration::from_secs(30),
        );
        assert!(result.is_err());
        assert_eq!(store.active_count(), 0); // nothing acquired

        // Shared mode: overlapping shared keys are OK (no conflict)
        let result = store.acquire_batch(
            &keys,
            "client-a",
            LeaseMode::Shared,
            Duration::from_secs(30),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_acquire_batch_empty() {
        let store = MemoryLeaseStore::<TestKey>::new().with_cleanup_grace(Duration::from_millis(0));
        let entries = store
            .acquire_batch(
                &[],
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_acquire_batch_different_groups() {
        let store = MemoryLeaseStore::<TestKey>::new().with_cleanup_grace(Duration::from_millis(0));

        // Keys in different groups (different inode) never conflict
        let keys = vec![
            TestKey {
                id: 1,
                start: 0,
                count: 4,
            },
            TestKey {
                id: 2,
                start: 0,
                count: 4,
            }, // same range, different group
        ];

        let entries = store
            .acquire_batch(
                &keys,
                "client-a",
                LeaseMode::Exclusive,
                Duration::from_secs(30),
            )
            .unwrap();
        assert_eq!(entries.len(), 2);
    }
}
