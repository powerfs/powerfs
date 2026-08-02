//! Lease management primitives for PowerFS.
//!
//! This crate provides lease (distributed lock) primitives extracted from
//! PowerFS's volume server and FUSE client. It is intentionally free of
//! PowerFS business dependencies (no inode/stripe/filer concepts) so it can
//! be unit-tested in isolation and reused by other subsystems.
//!
//! # Architecture
//!
//! - **Server-side**: [`store::MemoryLeaseStore`] holds lease state in memory,
//!   indexed by token, resource group, and holder. Generic over a [`store::LeaseKey`]
//!   implementation (e.g., `StripeKey` in powerfs-volume).
//! - **Client-side**: [`manager::LeaseManager`] trait provides async acquire/release
//!   with optional caching. [`guard::LeaseGuard`] provides RAII semantics.
//!
//! # Quick example
//!
//! ```
//! use powerfs_lease::{LeaseMode, MemoryLeaseStore, LeaseKey, LeaseStore, LeaseError};
//! use std::time::Duration;
//!
//! #[derive(Clone, PartialEq, Eq, Hash)]
//! struct MyKey { id: u64 }
//! impl LeaseKey for MyKey {
//!     fn group_id(&self) -> u64 { self.id }
//!     fn conflicts(&self, other: &Self) -> bool { self.id == other.id }
//!     fn encode(&self) -> Vec<u8> { self.id.to_le_bytes().to_vec() }
//!     fn decode(data: &[u8]) -> Result<Self, LeaseError> {
//!         if data.len() < 8 { return Err(LeaseError::Internal("too short".into())); }
//!         Ok(Self { id: u64::from_le_bytes(data[0..8].try_into().unwrap()) })
//!     }
//! }
//!
//! let store = MemoryLeaseStore::<MyKey>::new();
//! let lease = store.acquire(MyKey { id: 1 }, "client-a", LeaseMode::Exclusive, Duration::from_secs(30)).unwrap();
//! assert!(store.acquire(MyKey { id: 1 }, "client-b", LeaseMode::Exclusive, Duration::from_secs(30)).is_err());
//! store.release(&lease.token, "client-a").unwrap();
//! ```

pub mod error;
pub mod guard;
pub mod manager;
pub mod persistence;
pub mod store;
pub mod token;

pub use error::LeaseError;
pub use guard::LeaseGuard;
pub use manager::{LeaseManager, LeaseState};
pub use persistence::{decode_entry, encode_entry, LeasePersistence};
pub use store::{LeaseEntry, LeaseKey, LeaseStats, LeaseStore, MemoryLeaseStore};
pub use token::{LeaseMode, LeaseToken};
