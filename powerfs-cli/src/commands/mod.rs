mod assign;
mod cluster_add;
mod cluster_remove;
mod cluster_status;
mod cluster_transfer;
mod grow;
mod heartbeat;
mod kv;
mod lookup;
mod mount;
mod read;
mod status;
mod volume_list;
mod write;

pub use assign::{assign, AssignArgs};
pub use cluster_add::{cluster_add, ClusterAddArgs};
pub use cluster_remove::{cluster_remove, ClusterRemoveArgs};
pub use cluster_status::{cluster_status, ClusterStatusArgs};
pub use cluster_transfer::{cluster_transfer, ClusterTransferArgs};
pub use grow::{grow, GrowArgs};
pub use heartbeat::{heartbeat, HeartbeatArgs};
pub use kv::{kv, KvArgs};
pub use lookup::{lookup, LookupArgs};
pub use mount::{mount, MountArgs};
pub use read::{read, ReadArgs};
pub use status::{status, StatusArgs};
pub use volume_list::{volume_list, VolumeListArgs};
pub use write::{write, WriteArgs};

use powerfs_common::error::Result;

/// Common result type for commands
pub type CommandResult = Result<()>;
