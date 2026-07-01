pub mod powerfs {
    tonic::include_proto!("powerfs");
}

pub use powerfs::lookup_volume_response::VolumeIdLocation;
pub use powerfs::master_service_server::{MasterService, MasterServiceServer};
pub use powerfs::raft_service_client::RaftServiceClient;
pub use powerfs::raft_service_server::{RaftService, RaftServiceServer};
pub use powerfs::volume_list_response::DataNodeInfo;
pub use powerfs::{
    AddNodeRequest, AddNodeResponse, AssignRequest, AssignResponse, ClusterInfoRequest,
    ClusterInfoResponse, CollectionInfo, CollectionStats, CreateCollectionRequest,
    CreateCollectionResponse, DataCenterStats, DeleteCollectionRequest, DeleteCollectionResponse,
    DeleteVolumeRequest, DeleteVolumeResponse, GetCollectionRequest, GetCollectionResponse,
    Heartbeat, HeartbeatResponse, KeepConnectedRequest, KeepConnectedResponse,
    ListCollectionsRequest, ListCollectionsResponse, Location, LookupVolumeRequest,
    LookupVolumeResponse, PingRequest, PingResponse, ProposeRequest, ProposeResponse, RackStats,
    RaftMessage, RaftMessageResponse, RemoveNodeRequest, RemoveNodeResponse, StatisticsRequest,
    StatisticsResponse, TransferLeaderRequest, TransferLeaderResponse, VolumeGrowRequest,
    VolumeGrowResponse, VolumeListRequest, VolumeListResponse, VolumeLocation, VolumeShortInfo,
};
