pub mod powerfs {
    tonic::include_proto!("powerfs");
}

pub use powerfs::kv_cache_service_server::{KvCacheService, KvCacheServiceServer};
pub use powerfs::lookup_volume_response::VolumeIdLocation;
pub use powerfs::master_service_server::{MasterService, MasterServiceServer};
pub use powerfs::raft_service_client::RaftServiceClient;
pub use powerfs::raft_service_server::{RaftService, RaftServiceServer};
pub use powerfs::volume_list_response::DataNodeInfo;
pub use powerfs::{
    AddNodeRequest, AddNodeResponse, AssignRequest, AssignResponse, BatchGetRequest,
    BatchGetResponse, BatchPutRequest, BatchPutResponse, ClusterInfoRequest, ClusterInfoResponse,
    CollectionInfo, CollectionStats, CreateCollectionRequest, CreateCollectionResponse,
    CreateSessionRequest, CreateSessionResponse, DataCenterStats, DeleteCollectionRequest,
    DeleteCollectionResponse, DeleteSessionRequest, DeleteSessionResponse, DeleteVolumeRequest,
    DeleteVolumeResponse, GetBlockRequest, GetBlockResponse, GetCollectionRequest,
    GetCollectionResponse, GetSessionRequest, GetSessionResponse, GetStatsRequest,
    GetStatsResponse, Heartbeat, HeartbeatResponse, KeepConnectedRequest, KeepConnectedResponse,
    ListCollectionsRequest, ListCollectionsResponse, ListSessionsRequest, ListSessionsResponse,
    Location, LookupVolumeRequest, LookupVolumeResponse, PingRequest, PingResponse, ProposeRequest,
    ProposeResponse, PutBlockRequest, PutBlockResponse, RackStats, RaftMessage,
    RaftMessageResponse, RemoveNodeRequest, RemoveNodeResponse, StatisticsRequest,
    StatisticsResponse, TransferLeaderRequest, TransferLeaderResponse, VolumeGrowRequest,
    VolumeGrowResponse, VolumeListRequest, VolumeListResponse, VolumeLocation, VolumeShortInfo,
};
