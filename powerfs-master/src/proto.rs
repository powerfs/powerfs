pub mod powerfs {
    tonic::include_proto!("powerfs");
}

pub use powerfs::lookup_volume_response::VolumeIdLocation;
pub use powerfs::master_service_server::{MasterService, MasterServiceServer};
pub use powerfs::volume_list_response::DataNodeInfo;
pub use powerfs::{
    AssignRequest, AssignResponse, Heartbeat, HeartbeatResponse, KeepConnectedRequest,
    KeepConnectedResponse, Location, LookupVolumeRequest, LookupVolumeResponse, PingRequest,
    PingResponse, VolumeListRequest, VolumeListResponse, VolumeLocation, VolumeShortInfo,
    VolumeGrowRequest, VolumeGrowResponse,
};
