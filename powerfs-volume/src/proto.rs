pub mod powerfs {
    tonic::include_proto!("powerfs");
}

pub use powerfs::volume_service_client::VolumeServiceClient;
pub use powerfs::volume_service_server::{VolumeService, VolumeServiceServer};
pub use powerfs::{
    BatchDeleteRequest, BatchDeleteResponse, BatchWriteNeedleBlobRequest,
    BatchWriteNeedleBlobResponse, CreateVolumeRequest, CreateVolumeResponse, DeleteNeedleRequest,
    DeleteNeedleResponse, DeleteResult, DeleteVolumeRequest, DeleteVolumeResponse,
    GetNodeInfoRequest, GetNodeInfoResponse, ListVolumesRequest, ListVolumesResponse,
    RangeLeaseReleaseRequest, RangeLeaseReleaseResponse, RangeLeaseRenewRequest,
    RangeLeaseRenewResponse, RangeLeaseRequest, RangeLeaseResponse, ReadNeedleBlobRequest,
    ReadNeedleBlobResponse, ReadNeedleMetaRequest, ReadNeedleMetaResponse, ReadNeedleRequest,
    ReadNeedleResponse, RestoreNeedleRequest, RestoreNeedleResponse, VolumeInfo,
    VolumeStatusRequest, VolumeStatusResponse, WormLockRequest, WormLockResponse,
    WriteNeedleBlobLeaseRequest, WriteNeedleBlobLeaseResponse, WriteNeedleBlobRequest,
    WriteNeedleBlobResponse, WriteNeedleRequest, WriteNeedleResponse,
};
