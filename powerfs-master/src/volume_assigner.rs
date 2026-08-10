//! Re-exports: VolumeAssigner has been moved to `powerfs-allocator`.
//!
//! This file preserves the `crate::volume_assigner::` import path for existing
//! code in `powerfs-master`. New code should import from `powerfs_allocator`.

pub use powerfs_allocator::volume_assigner::{
    AssignContext, AssignerType, ConsistentHashAssigner, RoundRobinAssigner, SmartVolumeAssigner,
    VolumeAssigner,
};
