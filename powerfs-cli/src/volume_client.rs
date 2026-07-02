use powerfs_volume::proto::{
    powerfs::volume_service_client::VolumeServiceClient, CreateVolumeRequest, DeleteNeedleRequest,
    ReadNeedleRequest, WriteNeedleRequest,
};
use tonic::transport::Channel;

pub struct VolumeServerClient {
    channel: Option<Channel>,
    address: String,
}

impl VolumeServerClient {
    pub fn new(address: &str) -> Self {
        Self {
            channel: None,
            address: address.to_string(),
        }
    }

    pub async fn connect(&mut self) -> Result<Channel, Box<dyn std::error::Error>> {
        let addr = format!("http://{}", self.address);
        let channel = Channel::from_shared(addr)?.connect().await?;
        self.channel = Some(channel.clone());
        Ok(channel)
    }

    pub async fn channel(&mut self) -> Result<Channel, Box<dyn std::error::Error>> {
        if let Some(ch) = &self.channel {
            Ok(ch.clone())
        } else {
            self.connect().await
        }
    }

    pub async fn service(
        &mut self,
    ) -> Result<VolumeServiceClient<Channel>, Box<dyn std::error::Error>> {
        let channel = self.channel().await?;
        Ok(VolumeServiceClient::new(channel))
    }

    #[allow(dead_code)]
    pub async fn create_volume(
        &mut self,
        volume_id: u32,
        size: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut service = self.service().await?;
        let request = CreateVolumeRequest { volume_id, size };
        let response = service.create_volume(tonic::Request::new(request)).await?;
        let result = response.into_inner();
        if result.success {
            Ok(())
        } else {
            Err("create volume failed".into())
        }
    }

    pub async fn write_needle(
        &mut self,
        volume_id: u32,
        file_key: u64,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut service = self.service().await?;
        let request = WriteNeedleRequest {
            volume_id,
            file_key,
            data: data.to_vec(),
            cookie: 0,
            ttl: "".to_string(),
        };
        let response = service.write_needle(tonic::Request::new(request)).await?;
        let result = response.into_inner();
        if result.success {
            println!(
                "Written: volume={}, file_key={}, offset={}",
                result.volume_id, result.file_key, result.offset
            );
            Ok(())
        } else {
            Err("write failed".into())
        }
    }

    pub async fn read_needle(
        &mut self,
        volume_id: u32,
        file_key: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut service = self.service().await?;
        let request = ReadNeedleRequest {
            volume_id,
            file_key,
            cookie: 0,
        };
        let response = service.read_needle(tonic::Request::new(request)).await?;
        let result = response.into_inner();
        if result.success {
            Ok(result.data)
        } else {
            Err("read failed".into())
        }
    }

    #[allow(dead_code)]
    pub async fn delete_needle(
        &mut self,
        volume_id: u32,
        file_key: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut service = self.service().await?;
        let request = DeleteNeedleRequest {
            volume_id,
            file_key,
            cookie: 0,
        };
        let response = service.delete_needle(tonic::Request::new(request)).await?;
        let result = response.into_inner();
        if result.success {
            Ok(())
        } else {
            Err("delete failed".into())
        }
    }
}
