use log::{debug, warn};
use powerfs_common::types::{Fid, VolumeId};
use powerfs_master::proto::powerfs::{
    master_service_client::MasterServiceClient, AssignRequest, LookupVolumeRequest,
};
use powerfs_volume::proto::powerfs::{
    volume_service_client::VolumeServiceClient, DeleteNeedleRequest, ReadNeedleRequest,
    WriteNeedleRequest,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use powerfs_master::proto::powerfs::Location;

/// gRPC client for communicating with Master and Volume servers
pub struct PowerFuseClient {
    master_addr: String,
    master_channel: RwLock<Option<Channel>>,
    volume_channels: RwLock<HashMap<String, Channel>>,
    runtime_handle: Handle,
}

impl PowerFuseClient {
    pub fn new(master_addr: &str, runtime_handle: Handle) -> Arc<Self> {
        Arc::new(PowerFuseClient {
            master_addr: master_addr.to_string(),
            master_channel: RwLock::new(None),
            volume_channels: RwLock::new(HashMap::new()),
            runtime_handle,
        })
    }

    async fn ensure_master_channel(&self) -> Result<Channel, String> {
        {
            let channel = self.master_channel.read().await;
            if let Some(ch) = &*channel {
                return Ok(ch.clone());
            }
        }
        let addr = format!("http://{}", self.master_addr);
        let ch = Channel::from_shared(addr)
            .map_err(|e| format!("invalid master address: {}", e))?
            .connect()
            .await
            .map_err(|e| format!("failed to connect to master: {}", e))?;
        let mut channel = self.master_channel.write().await;
        *channel = Some(ch.clone());
        Ok(ch)
    }

    async fn get_volume_channel(&self, addr: &str) -> Result<Channel, String> {
        {
            let channels = self.volume_channels.read().await;
            if let Some(ch) = channels.get(addr) {
                return Ok(ch.clone());
            }
        }
        let grpc_addr = format!("http://{}", addr);
        let ch = Channel::from_shared(grpc_addr)
            .map_err(|e| format!("invalid volume address: {}", e))?
            .connect()
            .await
            .map_err(|e| format!("failed to connect to volume server {}: {}", addr, e))?;
        let mut channels = self.volume_channels.write().await;
        channels.insert(addr.to_string(), ch.clone());
        Ok(ch)
    }

    /// Assign a new FID from Master
    pub async fn assign_fid(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<(Fid, Option<Location>), String> {
        let channel = self.ensure_master_channel().await?;
        let mut client = MasterServiceClient::new(channel);
        let request = AssignRequest {
            count: 1,
            replication: replication.to_string(),
            collection: collection.to_string(),
            ttl: String::new(),
            data_center: String::new(),
            rack: String::new(),
            data_node: String::new(),
            disk_type: String::new(),
        };
        let response = client
            .assign(tonic::Request::new(request))
            .await
            .map_err(|e| format!("assign failed: {}", e))?;
        let resp = response.into_inner();
        if !resp.error.is_empty() {
            return Err(resp.error);
        }
        let fid = Fid::from_string(&resp.fid).map_err(|e| format!("invalid fid: {}", e))?;
        Ok((fid, resp.location))
    }

    /// Lookup volume locations from Master
    pub async fn lookup_volume(&self, volume_id: VolumeId) -> Result<Vec<Location>, String> {
        let channel = self.ensure_master_channel().await?;
        let mut client = MasterServiceClient::new(channel);
        let request = LookupVolumeRequest {
            volume_or_file_ids: vec![volume_id.to_string()],
            collection: String::new(),
        };
        let response = client
            .lookup_volume(tonic::Request::new(request))
            .await
            .map_err(|e| format!("lookup_volume failed: {}", e))?;
        let resp = response.into_inner();
        if let Some(loc) = resp.volume_id_locations.first() {
            if !loc.error.is_empty() {
                return Err(loc.error.clone());
            }
            return Ok(loc.locations.clone());
        }
        Err("volume not found".to_string())
    }

    /// Write data to a Volume Server
    pub async fn write_data(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
        data: Vec<u8>,
    ) -> Result<(), String> {
        debug!(
            "write_data: addr={}, volume_id={}, file_key={}, size={}",
            volume_addr,
            volume_id,
            file_key,
            data.len()
        );
        let channel = self.get_volume_channel(volume_addr).await?;
        let mut client = VolumeServiceClient::new(channel);
        let request = WriteNeedleRequest {
            volume_id,
            file_key,
            data,
        };
        let response = client
            .write_needle(tonic::Request::new(request))
            .await
            .map_err(|e| format!("write_needle failed: {}", e))?;
        let resp = response.into_inner();
        if !resp.success {
            return Err("write failed: volume server returned failure".to_string());
        }
        Ok(())
    }

    /// Read data from a Volume Server
    pub async fn read_data(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
    ) -> Result<Vec<u8>, String> {
        debug!(
            "read_data: addr={}, volume_id={}, file_key={}",
            volume_addr, volume_id, file_key
        );
        let channel = self.get_volume_channel(volume_addr).await?;
        let mut client = VolumeServiceClient::new(channel);
        let request = ReadNeedleRequest {
            volume_id,
            file_key,
        };
        let response = client
            .read_needle(tonic::Request::new(request))
            .await
            .map_err(|e| format!("read_needle failed: {}", e))?;
        let resp = response.into_inner();
        if !resp.success {
            return Err("read failed: volume server returned failure".to_string());
        }
        Ok(resp.data)
    }

    /// Delete data from a Volume Server
    pub async fn delete_data(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
    ) -> Result<(), String> {
        debug!(
            "delete_data: addr={}, volume_id={}, file_key={}",
            volume_addr, volume_id, file_key
        );
        let channel = self.get_volume_channel(volume_addr).await?;
        let mut client = VolumeServiceClient::new(channel);
        let request = DeleteNeedleRequest {
            volume_id,
            file_key,
        };
        let response = client
            .delete_needle(tonic::Request::new(request))
            .await
            .map_err(|e| format!("delete_needle failed: {}", e))?;
        let resp = response.into_inner();
        if !resp.success {
            return Err("delete failed: volume server returned failure".to_string());
        }
        Ok(())
    }

    /// Get the gRPC address from a Location
    pub fn location_to_grpc_addr(location: &Location) -> String {
        if location.grpc_port > 0 {
            // Extract host from url (format: "ip:http_port")
            let host = location.url.split(':').next().unwrap_or(&location.url);
            format!("{}:{}", host, location.grpc_port)
        } else {
            // Fall back to url if grpc_port is not set
            location.url.clone()
        }
    }

    /// Get the tokio runtime handle for block_on calls from sync context
    pub fn runtime_handle(&self) -> &Handle {
        &self.runtime_handle
    }

    /// Invalidate a cached volume channel (on connection error)
    pub async fn invalidate_volume_channel(&self, addr: &str) {
        let mut channels = self.volume_channels.write().await;
        channels.remove(addr);
        warn!("Invalidated volume channel: {}", addr);
    }
}

// ============ Synchronous wrappers for FUSE ============

/// Wrapper to call async methods from sync FUSE context
pub struct SyncFuseClient {
    client: Arc<PowerFuseClient>,
}

impl SyncFuseClient {
    pub fn new(client: Arc<PowerFuseClient>) -> Self {
        SyncFuseClient { client }
    }

    pub fn assign_fid(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<(Fid, Option<Location>), String> {
        self.client
            .runtime_handle
            .block_on(self.client.assign_fid(collection, replication))
    }

    pub fn lookup_volume(&self, volume_id: VolumeId) -> Result<Vec<Location>, String> {
        self.client
            .runtime_handle
            .block_on(self.client.lookup_volume(volume_id))
    }

    pub fn write_data(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
        data: Vec<u8>,
    ) -> Result<(), String> {
        self.client.runtime_handle.block_on(self.client.write_data(
            volume_addr,
            volume_id,
            file_key,
            data,
        ))
    }

    pub fn read_data(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
    ) -> Result<Vec<u8>, String> {
        self.client
            .runtime_handle
            .block_on(self.client.read_data(volume_addr, volume_id, file_key))
    }

    pub fn delete_data(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
    ) -> Result<(), String> {
        self.client.runtime_handle.block_on(self.client.delete_data(
            volume_addr,
            volume_id,
            file_key,
        ))
    }
}
