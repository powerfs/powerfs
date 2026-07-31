//! Re-export of the shared `ResilientMasterClient` from `powerfs-master`.
//!
//! The implementation lives in `powerfs_master::resilient_client` so
//! that every downstream client (monitor, filer, volume server, CLI,
//! KV client) can share the same leader-discovery logic.

pub use powerfs_master::resilient_client::{ResilientMasterClient, SharedMasterClient};
