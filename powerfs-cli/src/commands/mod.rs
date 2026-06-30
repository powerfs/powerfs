mod status;
mod assign;
mod lookup;
mod volume_list;
mod heartbeat;
mod grow;

pub use status::{status, StatusArgs};
pub use assign::{assign, AssignArgs};
pub use lookup::{lookup, LookupArgs};
pub use volume_list::{volume_list, VolumeListArgs};
pub use heartbeat::{heartbeat, HeartbeatArgs};
pub use grow::{grow, GrowArgs};

use powerfs_common::error::Result;

/// Common result type for commands
pub type CommandResult = Result<()>;