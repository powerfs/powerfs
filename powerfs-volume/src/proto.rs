pub mod powerfs {
    tonic::include_proto!("powerfs");
}

pub use powerfs::volume_service_server::{VolumeService, VolumeServiceServer};
pub use powerfs::{
    CreateVolumeRequest, CreateVolumeResponse, DeleteNeedleRequest, DeleteNeedleResponse,
    DeleteVolumeRequest, DeleteVolumeResponse, GetNodeInfoRequest, GetNodeInfoResponse,
    ListVolumesRequest, ListVolumesResponse, ReadNeedleRequest, ReadNeedleResponse, VolumeInfo,
    WriteNeedleRequest, WriteNeedleResponse,
};
