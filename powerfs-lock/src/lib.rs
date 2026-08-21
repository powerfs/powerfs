//! powerfs-lock: Unified lock manager interface layer for PowerFS.
//!
//! This crate defines the client-form-agnostic lock interface (`LockManager`
//! trait) and core types (`LockRequest`, `LockGrant`, `LockMode`, `Range`).
//! It is the single entry point for both the FUSE userspace Rust client and
//! the in-kernel C client (which implements the same wire protocol
//! independently — see `docs/lock-protocol.md`).
//!
//! # Architecture (see `docs/lock-optimization-plan.md` §3.1, §7.1)
//!
//! - **Interface layer** (this crate): `LockManager` trait + types.
//! - **Protocol layer** (`powerfs-lock-net`): TLV encoding/decoding for wire.
//! - **Filer server**: `InodeLeaseStore` (reuses `powerfs-lease`).
//! - **Volume server**: `RangeLeaseStore` (reuses `powerfs-lease`).
//! - **FUSE client** (`powerfs-lock-fuse`): Rust impl of `LockManager`,
//!   routes inode/range requests to the right server.
//! - **Kernel client** (`powerfs-kernel`, C): independent impl, same wire.
//!
//! # Lock modes (simplified from DLM's PR/PW/EX/CW — see §3.2 decision 3)
//!
//! - `Shared`: read, multiple holders on non-conflicting ranges.
//! - `Exclusive`: write, no other holder on same inode/range.
//! - `Range(Range)`: flock/OFD-style range write.
//!
//! # Routing (see §7.1)
//!
//! - Inode-level request (`range == None` and mode is `Shared`/`Exclusive`)
//!   → Filer's `InodeLeaseStore` (reuses `powerfs-lease` with `InodeKey`).
//! - Range-level request (`range == Some` or mode is `Range(_)`)
//!   → Volume's `RangeLeaseStore` (reuses `powerfs-lease` with `StripeKey`).
//!
//! The two backends are mutually exclusive at runtime (selected by client
//! config `lease_mode = "inode" | "range"`), so no cross-server lock
//! coordination is needed.

pub mod error;
pub mod event;
pub mod manager;
pub mod types;

pub use error::LockError;
pub use event::LockEventHandler;
pub use manager::LockManager;
pub use types::{LockGrant, LockMode, LockRequest, Range};
