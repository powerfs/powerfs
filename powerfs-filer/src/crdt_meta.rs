//! CRDT Meta Delta - Conflict-free Replicated Data Type for metadata attributes
//!
//! This module provides CRDT-based merge logic for file metadata attributes
//! (mode, uid, gid, mtime, atime, nlink) using different strategies:
//! - LWW (Last Write Wins) for mode, uid, gid
//! - Max strategy for timestamps (mtime, atime, ctime)
//! - Counter for nlink

use serde::{Deserialize, Serialize};

/// A delta operation for metadata attributes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaDelta {
    /// Set file mode (LWW merge based on timestamp)
    SetMode {
        inode: u64,
        mode: u32,
        timestamp: u64,
        client_id: String,
    },
    /// Set file owner uid (LWW merge based on timestamp)
    SetUid {
        inode: u64,
        uid: u32,
        timestamp: u64,
        client_id: String,
    },
    /// Set file group gid (LWW merge based on timestamp)
    SetGid {
        inode: u64,
        gid: u32,
        timestamp: u64,
        client_id: String,
    },
    /// Set modification time (Max merge)
    SetMtime {
        inode: u64,
        mtime: u64,
        timestamp: u64,
        client_id: String,
    },
    /// Set access time (Max merge)
    SetAtime {
        inode: u64,
        atime: u64,
        timestamp: u64,
        client_id: String,
    },
    /// Set creation time (Max merge)
    SetCtime {
        inode: u64,
        ctime: u64,
        timestamp: u64,
        client_id: String,
    },
    /// Increment nlink counter
    IncNlink { inode: u64, delta: i32 },
    /// Decrement nlink counter
    DecNlink { inode: u64, delta: i32 },
}

/// The result of a merge operation
#[derive(Debug, Clone, PartialEq)]
pub enum MergeResult {
    /// The delta was applied successfully
    Applied,
    /// The delta was ignored (older timestamp, same state)
    Idempotent,
    /// The delta could not be applied (missing inode, etc.)
    Conflict,
}

impl MetaDelta {
    /// Get the inode targeted by this delta
    pub fn inode(&self) -> u64 {
        match self {
            MetaDelta::SetMode { inode, .. } => *inode,
            MetaDelta::SetUid { inode, .. } => *inode,
            MetaDelta::SetGid { inode, .. } => *inode,
            MetaDelta::SetMtime { inode, .. } => *inode,
            MetaDelta::SetAtime { inode, .. } => *inode,
            MetaDelta::SetCtime { inode, .. } => *inode,
            MetaDelta::IncNlink { inode, .. } => *inode,
            MetaDelta::DecNlink { inode, .. } => *inode,
        }
    }

    /// Get the timestamp of this delta (for LWW comparison)
    pub fn timestamp(&self) -> Option<u64> {
        match self {
            MetaDelta::SetMode { timestamp, .. } => Some(*timestamp),
            MetaDelta::SetUid { timestamp, .. } => Some(*timestamp),
            MetaDelta::SetGid { timestamp, .. } => Some(*timestamp),
            MetaDelta::SetMtime { timestamp, .. } => Some(*timestamp),
            MetaDelta::SetAtime { timestamp, .. } => Some(*timestamp),
            MetaDelta::SetCtime { timestamp, .. } => Some(*timestamp),
            MetaDelta::IncNlink { .. } => None,
            MetaDelta::DecNlink { .. } => None,
        }
    }

    /// Get the client_id of this delta
    pub fn client_id(&self) -> Option<&str> {
        match self {
            MetaDelta::SetMode { client_id, .. } => Some(client_id),
            MetaDelta::SetUid { client_id, .. } => Some(client_id),
            MetaDelta::SetGid { client_id, .. } => Some(client_id),
            MetaDelta::SetMtime { client_id, .. } => Some(client_id),
            MetaDelta::SetAtime { client_id, .. } => Some(client_id),
            MetaDelta::SetCtime { client_id, .. } => Some(client_id),
            MetaDelta::IncNlink { .. } => None,
            MetaDelta::DecNlink { .. } => None,
        }
    }
}

/// Per-attribute state for CRDT merge
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetaState {
    /// Current mode value
    pub mode: Option<u32>,
    /// UID value
    pub uid: Option<u32>,
    /// GID value
    pub gid: Option<u32>,
    /// Modification time
    pub mtime: Option<u64>,
    /// Access time
    pub atime: Option<u64>,
    /// Creation time
    pub ctime: Option<u64>,
    /// Link count (Counter CRDT)
    pub nlink: Option<i32>,
    /// Last timestamp for each attribute (for LWW comparison)
    pub mode_timestamp: u64,
    pub uid_timestamp: u64,
    pub gid_timestamp: u64,
    pub mtime_timestamp: u64,
    pub atime_timestamp: u64,
    pub ctime_timestamp: u64,
    /// Delta accumulator for nlink
    pub nlink_delta: i32,
}

impl MetaState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a MetaDelta to this state using CRDT merge rules
    pub fn apply_delta(&mut self, delta: &MetaDelta) -> MergeResult {
        match delta {
            MetaDelta::SetMode {
                mode, timestamp, ..
            } => {
                // LWW: update only if timestamp is strictly newer
                if *timestamp > self.mode_timestamp {
                    self.mode = Some(*mode);
                    self.mode_timestamp = *timestamp;
                    MergeResult::Applied
                } else {
                    MergeResult::Idempotent
                }
            }
            MetaDelta::SetUid { uid, timestamp, .. } => {
                if *timestamp > self.uid_timestamp {
                    self.uid = Some(*uid);
                    self.uid_timestamp = *timestamp;
                    MergeResult::Applied
                } else {
                    MergeResult::Idempotent
                }
            }
            MetaDelta::SetGid { gid, timestamp, .. } => {
                if *timestamp > self.gid_timestamp {
                    self.gid = Some(*gid);
                    self.gid_timestamp = *timestamp;
                    MergeResult::Applied
                } else {
                    MergeResult::Idempotent
                }
            }
            MetaDelta::SetMtime {
                mtime, timestamp, ..
            } => {
                // Max strategy: update if timestamp is newer or value is larger
                let new_val = *mtime;
                let current = self.mtime.unwrap_or(0);
                if new_val > current || *timestamp > self.mtime_timestamp {
                    self.mtime = Some(new_val.max(current));
                    self.mtime_timestamp = *timestamp;
                    MergeResult::Applied
                } else {
                    MergeResult::Idempotent
                }
            }
            MetaDelta::SetAtime {
                atime, timestamp, ..
            } => {
                let new_val = *atime;
                let current = self.atime.unwrap_or(0);
                if new_val > current || *timestamp > self.atime_timestamp {
                    self.atime = Some(new_val.max(current));
                    self.atime_timestamp = *timestamp;
                    MergeResult::Applied
                } else {
                    MergeResult::Idempotent
                }
            }
            MetaDelta::SetCtime {
                ctime, timestamp, ..
            } => {
                let new_val = *ctime;
                let current = self.ctime.unwrap_or(0);
                if new_val > current || *timestamp > self.ctime_timestamp {
                    self.ctime = Some(new_val.max(current));
                    self.ctime_timestamp = *timestamp;
                    MergeResult::Applied
                } else {
                    MergeResult::Idempotent
                }
            }
            MetaDelta::IncNlink { delta, .. } => {
                // Counter CRDT: add delta
                self.nlink_delta += delta;
                self.nlink = Some(self.nlink.unwrap_or(0) + delta);
                MergeResult::Applied
            }
            MetaDelta::DecNlink { delta, .. } => {
                // Counter CRDT: subtract delta
                self.nlink_delta -= delta;
                self.nlink = Some(self.nlink.unwrap_or(0) - delta);
                MergeResult::Applied
            }
        }
    }

    /// Merge another MetaState into this one (state-based CRDT merge)
    pub fn merge(&mut self, other: &MetaState) {
        // LWW for mode/uid/gid
        if other.mode_timestamp > self.mode_timestamp {
            self.mode = other.mode;
            self.mode_timestamp = other.mode_timestamp;
        }
        if other.uid_timestamp > self.uid_timestamp {
            self.uid = other.uid;
            self.uid_timestamp = other.uid_timestamp;
        }
        if other.gid_timestamp > self.gid_timestamp {
            self.gid = other.gid;
            self.gid_timestamp = other.gid_timestamp;
        }
        // Max for timestamps - only update if other has a value and it's larger or newer
        if other.mtime.is_some()
            && (other.mtime.unwrap_or(0) > self.mtime.unwrap_or(0)
                || other.mtime_timestamp > self.mtime_timestamp)
        {
            self.mtime = Some(other.mtime.unwrap_or(0).max(self.mtime.unwrap_or(0)));
            self.mtime_timestamp = other.mtime_timestamp;
        }
        if other.atime.is_some()
            && (other.atime.unwrap_or(0) > self.atime.unwrap_or(0)
                || other.atime_timestamp > self.atime_timestamp)
        {
            self.atime = Some(other.atime.unwrap_or(0).max(self.atime.unwrap_or(0)));
            self.atime_timestamp = other.atime_timestamp;
        }
        if other.ctime.is_some()
            && (other.ctime.unwrap_or(0) > self.ctime.unwrap_or(0)
                || other.ctime_timestamp > self.ctime_timestamp)
        {
            self.ctime = Some(other.ctime.unwrap_or(0).max(self.ctime.unwrap_or(0)));
            self.ctime_timestamp = other.ctime_timestamp;
        }
        // Counter for nlink
        if other.nlink_delta != 0 {
            self.nlink_delta += other.nlink_delta;
            self.nlink = Some(self.nlink.unwrap_or(0) + other.nlink_delta);
        }
    }

    /// Apply a batch of deltas at once, returning the count of newly applied deltas.
    /// This is more efficient than applying one-by-one when many deltas arrive together
    /// (e.g. from concurrent setattr operations).
    pub fn apply_deltas(&mut self, deltas: &[MetaDelta]) -> usize {
        let mut applied = 0usize;
        for delta in deltas {
            if self.apply_delta(delta) == MergeResult::Applied {
                applied += 1;
            }
        }
        applied
    }

    /// Compact this MetaState by collapsing any redundant internal state.
    ///
    /// After compaction, `nlink_delta` is reset to zero since the cumulative value
    /// has been folded into `nlink`.  Timestamp tracks are preserved (they represent
    /// the "winner" timestamps and are needed for future LWW comparisons).
    ///
    /// Returns the number of "collapsed" counter operations (always `nlink_delta.abs()`
    /// for reporting purposes).
    pub fn compact(&mut self) -> i32 {
        let collapsed = self.nlink_delta.abs();
        self.nlink_delta = 0;
        collapsed
    }

    /// Returns the number of pending counter deltas that have not been compacted
    /// into the final state.  Useful for deciding whether `compact()` is worthwhile.
    pub fn pending_delta_count(&self) -> i32 {
        self.nlink_delta.abs()
    }

    /// Returns true if this state is empty (no attributes have been set)
    pub fn is_empty(&self) -> bool {
        self.mode.is_none()
            && self.uid.is_none()
            && self.gid.is_none()
            && self.mtime.is_none()
            && self.atime.is_none()
            && self.ctime.is_none()
            && self.nlink.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lww_mode_newer_wins() {
        let mut state = MetaState::new();
        let delta1 = MetaDelta::SetMode {
            inode: 1,
            mode: 0o644,
            timestamp: 100,
            client_id: "client-a".to_string(),
        };
        let delta2 = MetaDelta::SetMode {
            inode: 1,
            mode: 0o755,
            timestamp: 200,
            client_id: "client-b".to_string(),
        };

        assert_eq!(state.apply_delta(&delta1), MergeResult::Applied);
        assert_eq!(state.mode, Some(0o644));

        assert_eq!(state.apply_delta(&delta2), MergeResult::Applied);
        assert_eq!(state.mode, Some(0o755));
    }

    #[test]
    fn test_lww_mode_older_ignored() {
        let mut state = MetaState::new();
        let delta1 = MetaDelta::SetMode {
            inode: 1,
            mode: 0o644,
            timestamp: 200,
            client_id: "client-a".to_string(),
        };
        let delta2 = MetaDelta::SetMode {
            inode: 1,
            mode: 0o755,
            timestamp: 100,
            client_id: "client-b".to_string(),
        };

        assert_eq!(state.apply_delta(&delta1), MergeResult::Applied);
        assert_eq!(state.apply_delta(&delta2), MergeResult::Idempotent);
        assert_eq!(state.mode, Some(0o644));
    }

    #[test]
    fn test_max_mtime() {
        let mut state = MetaState::new();
        let delta1 = MetaDelta::SetMtime {
            inode: 1,
            mtime: 1000,
            timestamp: 100,
            client_id: "client-a".to_string(),
        };
        let delta2 = MetaDelta::SetMtime {
            inode: 1,
            mtime: 500,
            timestamp: 200,
            client_id: "client-b".to_string(),
        };

        assert_eq!(state.apply_delta(&delta1), MergeResult::Applied);
        assert_eq!(state.mtime, Some(1000));

        // Second delta has lower value but higher timestamp
        // Max strategy: take the maximum
        assert_eq!(state.apply_delta(&delta2), MergeResult::Applied);
        assert_eq!(state.mtime, Some(1000)); // Max of 1000 and 500
    }

    #[test]
    fn test_nlink_counter() {
        let mut state = MetaState::new();
        let delta1 = MetaDelta::IncNlink { inode: 1, delta: 1 };
        let delta2 = MetaDelta::IncNlink { inode: 1, delta: 1 };
        let delta3 = MetaDelta::DecNlink { inode: 1, delta: 1 };

        assert_eq!(state.apply_delta(&delta1), MergeResult::Applied);
        assert_eq!(state.nlink, Some(1));

        assert_eq!(state.apply_delta(&delta2), MergeResult::Applied);
        assert_eq!(state.nlink, Some(2));

        assert_eq!(state.apply_delta(&delta3), MergeResult::Applied);
        assert_eq!(state.nlink, Some(1));
    }

    #[test]
    fn test_state_merge() {
        let mut state1 = MetaState::new();
        state1.apply_delta(&MetaDelta::SetMode {
            inode: 1,
            mode: 0o644,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });
        state1.apply_delta(&MetaDelta::SetMtime {
            inode: 1,
            mtime: 500,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });

        let mut state2 = MetaState::new();
        state2.apply_delta(&MetaDelta::SetMode {
            inode: 1,
            mode: 0o755,
            timestamp: 200,
            client_id: "client-b".to_string(),
        });
        state2.apply_delta(&MetaDelta::SetMtime {
            inode: 1,
            mtime: 1000,
            timestamp: 200,
            client_id: "client-b".to_string(),
        });

        // Merge state2 into state1
        state1.merge(&state2);

        // mode: state2 has higher timestamp → should win
        assert_eq!(state1.mode, Some(0o755));
        // mtime: max of 500 and 1000
        assert_eq!(state1.mtime, Some(1000));
    }

    #[test]
    fn test_concurrent_chmod() {
        // Simulate concurrent chmod from two clients
        let mut state = MetaState::new();

        // Client A: chmod 644 at T1
        state.apply_delta(&MetaDelta::SetMode {
            inode: 1,
            mode: 0o644,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });

        // Client B: chmod 755 at T2 (T2 > T1)
        state.apply_delta(&MetaDelta::SetMode {
            inode: 1,
            mode: 0o755,
            timestamp: 200,
            client_id: "client-b".to_string(),
        });

        // Client B's operation wins (LWW)
        assert_eq!(state.mode, Some(0o755));
    }

    #[test]
    fn test_concurrent_utimes() {
        let mut state = MetaState::new();

        // Two clients update mtime concurrently
        state.apply_delta(&MetaDelta::SetMtime {
            inode: 1,
            mtime: 2000,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });
        state.apply_delta(&MetaDelta::SetMtime {
            inode: 1,
            mtime: 5000,
            timestamp: 200,
            client_id: "client-b".to_string(),
        });

        // Max strategy takes the maximum
        assert_eq!(state.mtime, Some(5000));
    }

    #[test]
    fn test_uid_gid_lww() {
        let mut state = MetaState::new();

        state.apply_delta(&MetaDelta::SetUid {
            inode: 1,
            uid: 1000,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });
        state.apply_delta(&MetaDelta::SetGid {
            inode: 1,
            gid: 2000,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });

        assert_eq!(state.uid, Some(1000));
        assert_eq!(state.gid, Some(2000));

        // Older timestamp update should be ignored
        state.apply_delta(&MetaDelta::SetUid {
            inode: 1,
            uid: 9999,
            timestamp: 50, // Older
            client_id: "client-b".to_string(),
        });
        assert_eq!(state.uid, Some(1000)); // Unchanged

        // Newer update should win
        state.apply_delta(&MetaDelta::SetUid {
            inode: 1,
            uid: 3000,
            timestamp: 200,
            client_id: "client-b".to_string(),
        });
        assert_eq!(state.uid, Some(3000));
    }

    #[test]
    fn test_atime_max_strategy() {
        let mut state = MetaState::new();

        state.apply_delta(&MetaDelta::SetAtime {
            inode: 1,
            atime: 500,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });
        assert_eq!(state.atime, Some(500));

        // Smaller value with later timestamp → should apply but Max takes the larger
        state.apply_delta(&MetaDelta::SetAtime {
            inode: 1,
            atime: 300,
            timestamp: 200,
            client_id: "client-b".to_string(),
        });
        assert_eq!(state.atime, Some(500)); // Max(500, 300) = 500

        // Larger value → should apply
        state.apply_delta(&MetaDelta::SetAtime {
            inode: 1,
            atime: 800,
            timestamp: 300,
            client_id: "client-a".to_string(),
        });
        assert_eq!(state.atime, Some(800));
    }

    #[test]
    fn test_multiple_properties_single_delta() {
        let mut state = MetaState::new();

        // Simulate a SetAttr call that sets mode, uid, gid, and mtime
        state.apply_delta(&MetaDelta::SetMode {
            inode: 1,
            mode: 0o644,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });
        state.apply_delta(&MetaDelta::SetUid {
            inode: 1,
            uid: 1000,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });
        state.apply_delta(&MetaDelta::SetGid {
            inode: 1,
            gid: 1000,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });
        state.apply_delta(&MetaDelta::SetMtime {
            inode: 1,
            mtime: 5000,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });

        assert_eq!(state.mode, Some(0o644));
        assert_eq!(state.uid, Some(1000));
        assert_eq!(state.gid, Some(1000));
        assert_eq!(state.mtime, Some(5000));
    }

    #[test]
    fn test_counter_nlink_concurrent() {
        // Simulate concurrent link/unlink operations
        let mut state = MetaState::new();

        // Two concurrent link operations
        state.apply_delta(&MetaDelta::IncNlink { inode: 1, delta: 1 });
        state.apply_delta(&MetaDelta::IncNlink { inode: 1, delta: 1 });
        assert_eq!(state.nlink, Some(2));

        // One unlink
        state.apply_delta(&MetaDelta::DecNlink { inode: 1, delta: 1 });
        assert_eq!(state.nlink, Some(1));

        // Multiple concurrent operations
        state.apply_delta(&MetaDelta::IncNlink { inode: 1, delta: 5 });
        state.apply_delta(&MetaDelta::DecNlink { inode: 1, delta: 3 });
        assert_eq!(state.nlink, Some(3));
    }

    #[test]
    fn test_state_merge_empty() {
        let state1 = MetaState::new();
        let state2 = MetaState::new();

        let mut merged = state1.clone();
        merged.merge(&state2);

        assert_eq!(merged.mode, None);
        assert_eq!(merged.uid, None);
        assert_eq!(merged.mtime, None);
    }

    #[test]
    fn test_state_merge_uid_gid_independent() {
        let mut state1 = MetaState::new();
        state1.apply_delta(&MetaDelta::SetUid {
            inode: 1,
            uid: 1000,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });

        let mut state2 = MetaState::new();
        state2.apply_delta(&MetaDelta::SetGid {
            inode: 1,
            gid: 2000,
            timestamp: 200,
            client_id: "client-b".to_string(),
        });

        state1.merge(&state2);

        // uid from state1 should survive (not overwritten by state2 since state2 didn't set uid)
        assert_eq!(state1.uid, Some(1000));
        // gid from state2 should be merged
        assert_eq!(state1.gid, Some(2000));
    }

    #[test]
    fn test_merge_result_idempotent() {
        let mut state = MetaState::new();
        let delta = MetaDelta::SetMode {
            inode: 1,
            mode: 0o644,
            timestamp: 100,
            client_id: "client-a".to_string(),
        };

        assert_eq!(state.apply_delta(&delta), MergeResult::Applied);
        // Same delta with same timestamp should be idempotent
        assert_eq!(state.apply_delta(&delta), MergeResult::Idempotent);
    }

    #[test]
    fn test_delta_inode_accessor() {
        let delta = MetaDelta::SetMode {
            inode: 42,
            mode: 0o644,
            timestamp: 100,
            client_id: "client-a".to_string(),
        };
        assert_eq!(delta.inode(), 42);
        assert_eq!(delta.timestamp(), Some(100));
        assert_eq!(delta.client_id(), Some("client-a"));

        let delta_counter = MetaDelta::IncNlink {
            inode: 99,
            delta: 1,
        };
        assert_eq!(delta_counter.inode(), 99);
        assert_eq!(delta_counter.timestamp(), None);
        assert_eq!(delta_counter.client_id(), None);
    }

    #[test]
    fn test_ctime_max_strategy() {
        let mut state = MetaState::new();

        state.apply_delta(&MetaDelta::SetCtime {
            inode: 1,
            ctime: 1000,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });
        assert_eq!(state.ctime, Some(1000));

        // Smaller value should not override (Max strategy)
        state.apply_delta(&MetaDelta::SetCtime {
            inode: 1,
            ctime: 500,
            timestamp: 200,
            client_id: "client-b".to_string(),
        });
        assert_eq!(state.ctime, Some(1000));

        // Larger value should override
        state.apply_delta(&MetaDelta::SetCtime {
            inode: 1,
            ctime: 2000,
            timestamp: 300,
            client_id: "client-c".to_string(),
        });
        assert_eq!(state.ctime, Some(2000));
    }

    #[test]
    fn test_apply_deltas_batch() {
        let mut state = MetaState::new();

        let deltas = vec![
            MetaDelta::SetMode {
                inode: 1,
                mode: 0o644,
                timestamp: 100,
                client_id: "client-a".to_string(),
            },
            MetaDelta::SetUid {
                inode: 1,
                uid: 1000,
                timestamp: 100,
                client_id: "client-a".to_string(),
            },
            MetaDelta::SetMtime {
                inode: 1,
                mtime: 5000,
                timestamp: 100,
                client_id: "client-a".to_string(),
            },
        ];

        let applied = state.apply_deltas(&deltas);
        assert_eq!(applied, 3);
        assert_eq!(state.mode, Some(0o644));
        assert_eq!(state.uid, Some(1000));
        assert_eq!(state.mtime, Some(5000));

        // Same deltas again should all be idempotent
        let applied = state.apply_deltas(&deltas);
        assert_eq!(applied, 0);
    }

    #[test]
    fn test_compact_resets_nlink_delta() {
        let mut state = MetaState::new();
        state.apply_delta(&MetaDelta::IncNlink { inode: 1, delta: 3 });
        state.apply_delta(&MetaDelta::IncNlink { inode: 1, delta: 2 });
        state.apply_delta(&MetaDelta::DecNlink { inode: 1, delta: 1 });

        assert_eq!(state.nlink, Some(4));
        assert_eq!(state.nlink_delta, 4); // 3 + 2 - 1

        let collapsed = state.compact();
        assert_eq!(collapsed, 4);
        assert_eq!(state.nlink_delta, 0);
        // Final state preserved
        assert_eq!(state.nlink, Some(4));
    }

    #[test]
    fn test_pending_delta_count() {
        let mut state = MetaState::new();
        assert_eq!(state.pending_delta_count(), 0);

        state.apply_delta(&MetaDelta::IncNlink { inode: 1, delta: 5 });
        assert_eq!(state.pending_delta_count(), 5);

        state.compact();
        assert_eq!(state.pending_delta_count(), 0);
    }

    #[test]
    fn test_is_empty() {
        let state = MetaState::new();
        assert!(state.is_empty());

        let mut state = MetaState::new();
        state.apply_delta(&MetaDelta::SetMode {
            inode: 1,
            mode: 0o644,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });
        assert!(!state.is_empty());
    }

    #[test]
    fn test_compact_preserves_timestamps() {
        let mut state = MetaState::new();
        state.apply_delta(&MetaDelta::SetMode {
            inode: 1,
            mode: 0o644,
            timestamp: 100,
            client_id: "client-a".to_string(),
        });
        state.apply_delta(&MetaDelta::IncNlink { inode: 1, delta: 2 });

        // Compact should preserve the LWW timestamp tracks
        state.compact();
        assert_eq!(state.mode, Some(0o644));
        assert_eq!(state.mode_timestamp, 100);
        assert_eq!(state.nlink_delta, 0);
        assert_eq!(state.nlink, Some(2));

        // Subsequent older LWW update should still be rejected
        let older = MetaDelta::SetMode {
            inode: 1,
            mode: 0o755,
            timestamp: 50,
            client_id: "client-b".to_string(),
        };
        assert_eq!(state.apply_delta(&older), MergeResult::Idempotent);
        assert_eq!(state.mode, Some(0o644));

        // Newer LWW update should still apply
        let newer = MetaDelta::SetMode {
            inode: 1,
            mode: 0o755,
            timestamp: 200,
            client_id: "client-b".to_string(),
        };
        assert_eq!(state.apply_delta(&newer), MergeResult::Applied);
        assert_eq!(state.mode, Some(0o755));
    }
}
