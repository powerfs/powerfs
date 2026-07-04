use crate::volume_proto::powerfs::volume_service_client::VolumeServiceClient;
use crate::volume_proto::powerfs::{ReadNeedleRequest, WriteNeedleRequest};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use tonic::transport::Channel;

pub struct VolumeClient {
    channel: Option<Channel>,
    address: String,
}

impl VolumeClient {
    pub fn new(address: &str) -> Self {
        Self {
            channel: None,
            address: address.to_string(),
        }
    }

    pub async fn connect(&mut self) -> Result<Channel, String> {
        let addr = format!("http://{}", self.address);
        let channel = Channel::from_shared(addr).map_err(|e| format!("invalid address: {}", e))?;
        let channel = channel.connect().await.map_err(|e| format!("connect failed: {}", e))?;
        self.channel = Some(channel.clone());
        Ok(channel)
    }

    pub async fn channel(&mut self) -> Result<Channel, String> {
        if let Some(ch) = &self.channel {
            Ok(ch.clone())
        } else {
            self.connect().await
        }
    }

    pub async fn service(&mut self) -> Result<VolumeServiceClient<Channel>, String> {
        let channel = self.channel().await?;
        Ok(VolumeServiceClient::new(channel))
    }

    pub async fn write_needle(
        &mut self,
        volume_id: u32,
        file_key: u64,
        data: &[u8],
    ) -> Result<(), String> {
        let mut service = self.service().await?;
        let request = WriteNeedleRequest {
            volume_id,
            file_key,
            data: data.to_vec(),
            cookie: 0,
            ttl: "".to_string(),
        };
        let response = service.write_needle(tonic::Request::new(request)).await.map_err(|e| format!("write_needle failed: {}", e))?;
        let result = response.into_inner();
        if result.success {
            Ok(())
        } else {
            Err("write_needle failed".to_string())
        }
    }

    pub async fn read_needle(
        &mut self,
        volume_id: u32,
        file_key: u64,
    ) -> Result<Vec<u8>, String> {
        let mut service = self.service().await?;
        let request = ReadNeedleRequest {
            volume_id,
            file_key,
            cookie: 0,
        };
        let response = service.read_needle(tonic::Request::new(request)).await.map_err(|e| format!("read_needle failed: {}", e))?;
        let result = response.into_inner();
        if result.success {
            Ok(result.data)
        } else {
            Err("read_needle failed".to_string())
        }
    }
}

pub struct VolumeClientPool {
    clients: RwLock<HashMap<String, Arc<Mutex<VolumeClient>>>>,
}

impl VolumeClientPool {
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_or_create(&self, address: &str) -> Arc<Mutex<VolumeClient>> {
        let mut clients = self.clients.write().unwrap();
        clients.entry(address.to_string()).or_insert_with(|| {
            Arc::new(Mutex::new(VolumeClient::new(address)))
        }).clone()
    }

    pub async fn write_needle(
        &self,
        address: &str,
        volume_id: u32,
        file_key: u64,
        data: &[u8],
    ) -> Result<(), String> {
        let client = self.get_or_create(address);
        let mut guard = client.lock().await;
        guard.write_needle(volume_id, file_key, data).await
    }

    pub async fn read_needle(
        &self,
        address: &str,
        volume_id: u32,
        file_key: u64,
    ) -> Result<Vec<u8>, String> {
        let client = self.get_or_create(address);
        let mut guard = client.lock().await;
        guard.read_needle(volume_id, file_key).await
    }
}