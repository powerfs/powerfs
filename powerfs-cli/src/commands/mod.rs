mod status;
mod assign;
mod lookup;
mod volume_list;
mod heartbeat;
mod grow;
mod write;
mod read;

pub use status::{status, StatusArgs};
pub use assign::{assign, AssignArgs};
pub use lookup::{lookup, LookupArgs};
pub use volume_list::{volume_list, VolumeListArgs};
pub use heartbeat::{heartbeat, HeartbeatArgs};
pub use grow::{grow, GrowArgs};
pub use write::{write, WriteArgs};
pub use read::{read, ReadArgs};

use powerfs_common::error::Result;

/// Common result type for commands
pub type CommandResult = Result<()>;