//! Core types for the unified lock interface.
//!
//! These types are intentionally client-form agnostic: they serve both the
//! FUSE userspace Rust client (via `powerfs-lock-fuse`) and the in-kernel C
//! client (via `docs/lock-protocol.md` byte-level spec). The C client does
//! not link this crate; it implements the same wire format independently.

use std::time::Duration;

/// A byte range within an inode, used for range-mode locks (flock/OFD style).
///
/// `start` is inclusive, `end` is exclusive: `[start, end)`.
/// `end == None` means "to EOF".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Range {
    /// Inclusive start byte offset.
    pub start: u64,
    /// Exclusive end byte offset. `None` means "to EOF".
    pub end: Option<u64>,
}

impl Range {
    pub fn new(start: u64, end: Option<u64>) -> Self {
        Self { start, end }
    }

    /// Full-inode range: `[0, EOF)`.
    pub fn full() -> Self {
        Self {
            start: 0,
            end: None,
        }
    }

    /// Whether this range overlaps with another range.
    ///
    /// Two full-EOF ranges always overlap. A bounded range overlaps an
    /// EOF-terminated range if its start is `<` the bounded end.
    pub fn overlaps(&self, other: &Range) -> bool {
        let self_end = self.end.unwrap_or(u64::MAX);
        let other_end = other.end.unwrap_or(u64::MAX);
        self.start < other_end && other.start < self_end
    }
}

/// Lock mode (simplified from DLM's PR/PW/EX/CW four modes).
///
/// See `docs/lock-optimization-plan.md` §3.2 decision 3.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockMode {
    /// Read shared — multiple holders allowed on non-conflicting ranges.
    Shared,
    /// Write exclusive — no other holder allowed on the same inode/range.
    Exclusive,
    /// Range write — flock/OFD-style range lock. Carries its own range;
    /// when this mode is used, `LockRequest::range` should match.
    Range(Range),
}

impl LockMode {
    /// Whether this mode grants exclusive access (no other holder allowed).
    pub fn is_exclusive(&self) -> bool {
        matches!(self, LockMode::Exclusive | LockMode::Range(_))
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            LockMode::Shared => "shared",
            LockMode::Exclusive => "exclusive",
            LockMode::Range(_) => "range",
        }
    }
}

/// A lock acquire request.
#[derive(Clone, Debug)]
pub struct LockRequest {
    /// Inode number identifying the file/dir.
    pub inode: u64,
    /// Lock mode (and range info for `Range` mode).
    pub mode: LockMode,
    /// Optional explicit range. When `None`, the lock is inode-level (routed
    /// to Filer's `InodeLeaseStore`). When `Some`, the lock is range-level
    /// (routed to Volume's `RangeLeaseStore`). If `mode` is `Range(r)`,
    /// this should equal `Some(r)`.
    pub range: Option<Range>,
    /// Requested lease duration.
    pub timeout: Duration,
}

impl LockRequest {
    /// Create an inode-level request (no range, routed to Filer).
    pub fn new(inode: u64, mode: LockMode, timeout: Duration) -> Self {
        Self {
            inode,
            mode,
            range: None,
            timeout,
        }
    }

    /// Attach an explicit range (routes the request to Volume).
    pub fn with_range(mut self, range: Range) -> Self {
        self.range = Some(range);
        self
    }

    /// Whether this request targets an inode-level lock (routed to Filer).
    pub fn is_inode_level(&self) -> bool {
        self.range.is_none() && !matches!(self.mode, LockMode::Range(_))
    }

    /// Whether this request targets a range-level lock (routed to Volume).
    pub fn is_range_level(&self) -> bool {
        self.range.is_some() || matches!(self.mode, LockMode::Range(_))
    }

    /// Effective range for this request, if range-level.
    pub fn effective_range(&self) -> Option<Range> {
        if let Some(r) = self.range {
            return Some(r);
        }
        match self.mode {
            LockMode::Range(r) => Some(r),
            _ => None,
        }
    }
}

/// A granted lock. Returned by `LockManager::acquire`.
#[derive(Clone, Debug)]
pub struct LockGrant {
    /// Inode the lock protects.
    pub inode: u64,
    /// Opaque lease token; must be presented on release/renew.
    pub token: String,
    /// Global sequence number (reserved for Early Grant / SN ordering).
    /// Set to 0 during the modularization phase; the optimization phase
    /// (Leader-optimistic `AtomicU64::fetch_add` + async Raft batch) fills
    /// it in. See `docs/lock-optimization-plan.md` §3.2 decision 2, §5.3.
    pub sn: u64,
    /// Granted lease duration in milliseconds.
    pub lease_ms: u64,
    /// Granted mode (echoes request).
    pub mode: LockMode,
    /// Effective range, if range-level.
    pub range: Option<Range>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Range::overlaps ---

    #[test]
    fn test_range_overlap_both_bounded() {
        let a = Range::new(0, Some(100));
        let b = Range::new(50, Some(150));
        assert!(a.overlaps(&b));
        assert!(b.overlaps(&a));
    }

    #[test]
    fn test_range_overlap_adjacent_no_overlap() {
        // [0,100) and [100,200) are adjacent — no overlap
        let a = Range::new(0, Some(100));
        let b = Range::new(100, Some(200));
        assert!(!a.overlaps(&b));
        assert!(!b.overlaps(&a));
    }

    #[test]
    fn test_range_overlap_disjoint() {
        let a = Range::new(0, Some(50));
        let b = Range::new(100, Some(200));
        assert!(!a.overlaps(&b));
    }

    #[test]
    fn test_range_overlap_eof_with_bounded() {
        // Two full-EOF ranges always overlap.
        let a = Range::full(); // [0, EOF)
        let b = Range::new(0, Some(u64::MAX));
        assert!(a.overlaps(&b));

        // EOF-terminated range [1000, EOF) overlaps a bounded range that
        // extends past its start: [500, 1100) overlaps [1000, EOF).
        let eof = Range::new(1000, None);
        let crosses_start = Range::new(500, Some(1100));
        assert!(eof.overlaps(&crosses_start));

        // Bounded range fully inside the EOF range also overlaps.
        let inside = Range::new(1500, Some(1600));
        assert!(eof.overlaps(&inside));

        // Bounded range entirely before the EOF range start does NOT overlap.
        let before = Range::new(500, Some(600));
        assert!(!eof.overlaps(&before));

        // Bounded range ending exactly at the EOF range start does NOT overlap.
        let adjacent = Range::new(500, Some(1000));
        assert!(!eof.overlaps(&adjacent));
    }

    #[test]
    fn test_range_overlap_two_eof() {
        let a = Range::new(0, None);
        let b = Range::new(100, None);
        assert!(a.overlaps(&b));
    }

    // --- LockMode::is_exclusive ---

    #[test]
    fn test_lock_mode_is_exclusive() {
        assert!(!LockMode::Shared.is_exclusive());
        assert!(LockMode::Exclusive.is_exclusive());
        assert!(LockMode::Range(Range::full()).is_exclusive());
    }

    #[test]
    fn test_lock_mode_as_str() {
        assert_eq!(LockMode::Shared.as_str(), "shared");
        assert_eq!(LockMode::Exclusive.as_str(), "exclusive");
        assert_eq!(LockMode::Range(Range::full()).as_str(), "range");
    }

    // --- LockRequest routing ---

    #[test]
    fn test_request_inode_level_shared() {
        let req = LockRequest::new(42, LockMode::Shared, Duration::from_secs(30));
        assert!(req.is_inode_level());
        assert!(!req.is_range_level());
        assert!(req.effective_range().is_none());
    }

    #[test]
    fn test_request_inode_level_exclusive() {
        let req = LockRequest::new(42, LockMode::Exclusive, Duration::from_secs(30));
        assert!(req.is_inode_level());
        assert!(!req.is_range_level());
    }

    #[test]
    fn test_request_range_level_via_with_range() {
        let req = LockRequest::new(42, LockMode::Exclusive, Duration::from_secs(30))
            .with_range(Range::new(0, Some(4096)));
        assert!(!req.is_inode_level());
        assert!(req.is_range_level());
        assert_eq!(req.effective_range(), Some(Range::new(0, Some(4096))));
    }

    #[test]
    fn test_request_range_level_via_range_mode() {
        let r = Range::new(0, Some(4096));
        let req = LockRequest::new(42, LockMode::Range(r), Duration::from_secs(30));
        // Range mode with no explicit range field still routes to range level
        assert!(!req.is_inode_level());
        assert!(req.is_range_level());
        // effective_range falls back to the mode's embedded range
        assert_eq!(req.effective_range(), Some(r));
    }

    #[test]
    fn test_request_explicit_range_takes_precedence() {
        // If both mode::Range(r1) and range=Some(r2) are set, explicit range wins
        let r1 = Range::new(0, Some(100));
        let r2 = Range::new(200, Some(300));
        let req = LockRequest::new(42, LockMode::Range(r1), Duration::from_secs(30)).with_range(r2);
        assert_eq!(req.effective_range(), Some(r2));
    }
}
