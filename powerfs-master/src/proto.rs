pub mod powerfs {
    tonic::include_proto!("powerfs");
}

pub use powerfs::lookup_volume_response::VolumeIdLocation;
pub use powerfs::master_service_server::{MasterService, MasterServiceServer};
pub use powerfs::volume_list_response::DataNodeInfo;
pub use powerfs::{
    AddNodeRequest, AddNodeResponse, AssignRequest, AssignResponse, ClusterInfoRequest,
    ClusterInfoResponse, Heartbeat, HeartbeatResponse, KeepConnectedRequest,
    KeepConnectedResponse, Location, LookupVolumeRequest, LookupVolumeResponse, PingRequest,
    PingResponse, ProposeRequest, ProposeResponse, RaftMessage, VolumeGrowRequest,
    VolumeGrowResponse, VolumeListRequest, VolumeListResponse, VolumeLocation, VolumeShortInfo,
};
pub use powerfs::raft_service_server::{RaftService, RaftServiceServer};
pub use powerfs::raft_service_client::RaftServiceClient;
