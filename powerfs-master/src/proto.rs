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
    CreateEntryRequest, CreateEntryResponse, CreateSessionRequest, CreateSessionResponse,
    DataCenterStats, DeleteCollectionRequest, DeleteCollectionResponse, DeleteEntryRequest,
    DeleteEntryResponse, DeleteSessionRequest, DeleteSessionResponse, DeleteVolumeRequest,
    DeleteVolumeResponse, DeltaOp, DirEntryOrset, Entry, EntryId, FileChunk, FuseAttributes,
    FuseClientsRequest, FuseClientsResponse, GetBlockRequest, GetBlockResponse,
    GetCollectionRequest, GetCollectionResponse, GetEntryByInodeRequest, GetEntryByInodeResponse,
    GetEntryRequest, GetEntryResponse, GetSessionRequest, GetSessionResponse, GetStatsRequest,
    GetStatsResponse, Heartbeat, HeartbeatResponse, KeepConnectedRequest, KeepConnectedResponse,
    ListCollectionsRequest, ListCollectionsResponse, ListEntriesRequest, ListEntriesResponse,
    ListSessionsRequest, ListSessionsResponse, Location, LookupDirectoryEntryRequest,
    LookupDirectoryEntryResponse, LookupVolumeRequest, LookupVolumeResponse, MetadataNotification,
    MutateEntryRequest, MutateEntryResponse, PingRequest, PingResponse, ProposeRequest,
    ProposeResponse, PullDeltaRequest, PullDeltaResponse, PushDeltaRequest, PushDeltaResponse,
    PutBlockRequest, PutBlockResponse, RackStats, RaftMessage, RaftMessageResponse,
    RemoveNodeRequest, RemoveNodeResponse, RenameOp, SetAttrOp, StatisticsRequest,
    StatisticsResponse, SubscribeMetadataRequest, TransferLeaderRequest, TransferLeaderResponse,
    UpdateEntryRequest, UpdateEntryResponse, VectorClock, VectorClockEntry, VolumeGrowRequest,
    VolumeGrowResponse, VolumeListRequest, VolumeListResponse, VolumeLocation, VolumeShortInfo,
};
