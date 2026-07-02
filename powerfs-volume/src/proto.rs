pub mod powerfs {
    tonic::include_proto!("powerfs");
}

pub use powerfs::volume_service_server::{VolumeService, VolumeServiceServer};
pub use powerfs::{
    BatchDeleteRequest, BatchDeleteResponse, CreateVolumeRequest, CreateVolumeResponse,
    DeleteNeedleRequest, DeleteNeedleResponse, DeleteResult, DeleteVolumeRequest,
    DeleteVolumeResponse, GetNodeInfoRequest, GetNodeInfoResponse, ListVolumesRequest,
    ListVolumesResponse, ReadNeedleBlobRequest, ReadNeedleBlobResponse, ReadNeedleMetaRequest,
    ReadNeedleMetaResponse, ReadNeedleRequest, ReadNeedleResponse, VolumeInfo, VolumeStatusRequest,
    VolumeStatusResponse, WriteNeedleBlobRequest, WriteNeedleBlobResponse, WriteNeedleRequest,
    WriteNeedleResponse,
};
