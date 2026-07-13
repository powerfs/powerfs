use log::{debug, error, info, warn};
use powerfs_common::types::{Fid, VolumeId};
use powerfs_master::proto::powerfs::{
    master_service_client::MasterServiceClient, AssignRequest, CreateEntryRequest,
    DeleteEntryRequest, DeltaOp, Entry, GetEntryByInodeRequest, GetEntryRequest,
    JobCompletionRequest, JobDeregistrationRequest, JobRegistrationRequest, KeepConnectedRequest,
    LeaseReleaseRequest, LeaseRenewRequest, LeaseRequest,
    ListEntriesRequest, LookupDirectoryEntryRequest, LookupVolumeRequest, MetadataNotification,
    PullDeltaRequest, PullDeltaResponse, PushDeltaRequest, PushDeltaResponse, RenameEntryRequest,
    StatisticsRequest, StatisticsResponse, SubscribeMetadataRequest, UpdateEntryRequest,
    VectorClock,
};
use powerfs_volume::proto::powerfs::{
    volume_service_client::VolumeServiceClient, DeleteNeedleRequest, ReadNeedleBlobRequest,
    ReadNeedleRequest, WriteNeedleBlobRequest, WriteNeedleRequest,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use powerfs_master::proto::powerfs::Location;

type AssignFidResult = (Fid, Option<Location>, Vec<String>, Vec<Location>);

#[derive(Debug)]
pub struct WriteBlobParams {
    pub volume_id: u32,
    pub file_key: u64,
    pub offset: i64,
    pub size: i32,
    pub data: Vec<u8>,
    pub cookie: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct GrpcConfig {
    pub keepalive_interval: Duration,
    pub keepalive_timeout: Duration,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_retry_count: usize,
    pub retry_delay: Duration,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        GrpcConfig {
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(60),
            max_retry_count: 3,
            retry_delay: Duration::from_millis(500),
        }
    }
}

pub struct PowerFuseClient {
    master_addr: String,
    master_channel: RwLock<Option<Channel>>,
    volume_channels: RwLock<HashMap<String, Channel>>,
    runtime_handle: Handle,
    config: GrpcConfig,
}

impl PowerFuseClient {
    pub fn new(master_addr: &str, runtime_handle: Handle) -> Arc<Self> {
        Arc::new(PowerFuseClient {
            master_addr: master_addr.to_string(),
            master_channel: RwLock::new(None),
            volume_channels: RwLock::new(HashMap::new()),
            runtime_handle,
            config: GrpcConfig::default(),
        })
    }

    pub fn with_config(master_addr: &str, runtime_handle: Handle, config: GrpcConfig) -> Arc<Self> {
        Arc::new(PowerFuseClient {
            master_addr: master_addr.to_string(),
            master_channel: RwLock::new(None),
            volume_channels: RwLock::new(HashMap::new()),
            runtime_handle,
            config,
        })
    }

    async fn get_or_create_master_channel(&self) -> Result<Channel, String> {
        {
            let channel = self.master_channel.read().await;
            if let Some(ch) = &*channel {
                return Ok(ch.clone());
            }
        }

        info!("Creating new master channel to: {}", self.master_addr);
        let channel = self.create_channel(&self.master_addr).await?;

        let mut master_channel = self.master_channel.write().await;
        *master_channel = Some(channel.clone());

        Ok(channel)
    }

    async fn get_or_create_volume_channel(&self, addr: &str) -> Result<Channel, String> {
        {
            let channels = self.volume_channels.read().await;
            if let Some(ch) = channels.get(addr) {
                return Ok(ch.clone());
            }
        }

        info!("Creating new volume channel to: {}", addr);
        let channel = self.create_channel(addr).await?;

        let mut volume_channels = self.volume_channels.write().await;
        volume_channels.insert(addr.to_string(), channel.clone());

        Ok(channel)
    }

    async fn create_channel(&self, addr: &str) -> Result<Channel, String> {
        let mut backoff = Duration::from_millis(100);
        let max_backoff = Duration::from_secs(10);
        let max_attempts = 5;

        for attempt in 1..=max_attempts {
            let grpc_addr = format!("http://{}", addr);
            match Channel::from_shared(grpc_addr)
                .map_err(|e| format!("invalid address: {}", e))?
                .http2_keep_alive_interval(self.config.keepalive_interval)
                .keep_alive_timeout(self.config.keepalive_timeout)
                .connect_timeout(self.config.connect_timeout)
                .connect()
                .await
            {
                Ok(ch) => {
                    info!("Connected to: {}", addr);
                    return Ok(ch);
                }
                Err(e) => {
                    let msg = format!("failed to connect to {}: {}", addr, e);
                    if attempt == max_attempts {
                        error!("{}", msg);
                        return Err(msg);
                    }
                    warn!("{} (retrying in {:?})", msg, backoff);
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                }
            }
        }

        Err(format!(
            "failed to connect to {} after {} attempts",
            addr, max_attempts
        ))
    }

    pub async fn invalidate_master_channel(&self) {
        let mut master_channel = self.master_channel.write().await;
        *master_channel = None;
        warn!("Invalidated master channel");
    }

    pub async fn invalidate_volume_channel(&self, addr: &str) {
        let mut volume_channels = self.volume_channels.write().await;
        volume_channels.remove(addr);
        warn!("Invalidated volume channel: {}", addr);
    }

    pub async fn assign_fid(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<AssignFidResult, String> {
        debug!(
            "assign_fid: collection={}, replication={}",
            collection, replication
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_master_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get master channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

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
                stripe_count: 1,
                stripe_size: 64 * 1024 * 1024,
            };

            match client.assign(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.error.is_empty() {
                        return Err(resp.error);
                    }
                    let fid =
                        Fid::from_string(&resp.fid).map_err(|e| format!("invalid fid: {}", e))?;
                    debug!("assign_fid succeeded: fid={}", fid);
                    return Ok((fid, resp.location, resp.stripe_fids, resp.stripe_locations));
                }
                Err(e) => {
                    let msg = format!("assign_fid failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_master_channel().await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("assign_fid failed after max retries".to_string())
    }

    pub async fn lookup_volume(&self, volume_id: VolumeId) -> Result<Vec<Location>, String> {
        debug!("lookup_volume: volume_id={}", volume_id.0);

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_master_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get master channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = MasterServiceClient::new(channel);
            let request = LookupVolumeRequest {
                volume_or_file_ids: vec![volume_id.to_string()],
                collection: String::new(),
            };

            match client.lookup_volume(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    let locations: Vec<Location> = resp
                        .volume_id_locations
                        .into_iter()
                        .flat_map(|vil| vil.locations)
                        .collect();
                    debug!("lookup_volume succeeded: {} locations", locations.len());
                    return Ok(locations);
                }
                Err(e) => {
                    let msg = format!("lookup_volume failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_master_channel().await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("lookup_volume failed after max retries".to_string())
    }

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

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_volume_channel(volume_addr).await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get volume channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = VolumeServiceClient::new(channel)
                .max_decoding_message_size(256 * 1024 * 1024)
                .max_encoding_message_size(256 * 1024 * 1024);
            let request = WriteNeedleRequest {
                volume_id,
                file_key,
                data: data.clone(),
                cookie: 0,
                ttl: "".to_string(),
            };

            match client.write_needle(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.success {
                        return Err("write failed: volume server returned failure".to_string());
                    }
                    debug!(
                        "write_data succeeded: volume_id={}, file_key={}",
                        volume_id, file_key
                    );
                    return Ok(());
                }
                Err(e) => {
                    let msg = format!("write_data failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_volume_channel(volume_addr).await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("write_data failed after max retries".to_string())
    }

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

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_volume_channel(volume_addr).await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get volume channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = VolumeServiceClient::new(channel)
                .max_decoding_message_size(256 * 1024 * 1024)
                .max_encoding_message_size(256 * 1024 * 1024);
            let request = ReadNeedleRequest {
                volume_id,
                file_key,
                cookie: 0,
            };

            match client.read_needle(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.success {
                        return Err("read failed: volume server returned failure".to_string());
                    }
                    debug!(
                        "read_data succeeded: volume_id={}, file_key={}, size={}",
                        volume_id,
                        file_key,
                        resp.data.len()
                    );
                    return Ok(resp.data);
                }
                Err(e) => {
                    let msg = format!("read_data failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_volume_channel(volume_addr).await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("read_data failed after max retries".to_string())
    }

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

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_volume_channel(volume_addr).await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get volume channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = VolumeServiceClient::new(channel)
                .max_decoding_message_size(256 * 1024 * 1024)
                .max_encoding_message_size(256 * 1024 * 1024);
            let request = DeleteNeedleRequest {
                volume_id,
                file_key,
                cookie: 0,
            };

            match client.delete_needle(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.success {
                        return Err("delete failed: volume server returned failure".to_string());
                    }
                    debug!(
                        "delete_data succeeded: volume_id={}, file_key={}",
                        volume_id, file_key
                    );
                    return Ok(());
                }
                Err(e) => {
                    let msg = format!("delete_data failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_volume_channel(volume_addr).await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("delete_data failed after max retries".to_string())
    }

    pub fn location_to_grpc_addr(location: &Location) -> String {
        if location.grpc_port > 0 {
            let host = location.url.split(':').next().unwrap_or(&location.url);
            format!("{}:{}", host, location.grpc_port)
        } else {
            format!("{}:{}", location.url, location.grpc_port)
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn write_blob(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: i32,
        data: Vec<u8>,
        cookie: u32,
    ) -> Result<(), String> {
        debug!(
            "write_blob: addr={}, volume_id={}, file_key={}, offset={}, size={}",
            volume_addr, volume_id, file_key, offset, size
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_volume_channel(volume_addr).await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get volume channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = VolumeServiceClient::new(channel)
                .max_decoding_message_size(256 * 1024 * 1024)
                .max_encoding_message_size(256 * 1024 * 1024);
            let request = WriteNeedleBlobRequest {
                volume_id,
                file_key,
                offset,
                size,
                needle_blob: data.clone(),
                cookie,
            };

            match client.write_needle_blob(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.success {
                        return Err("write_blob failed: volume server returned failure".to_string());
                    }
                    debug!(
                        "write_blob succeeded: volume_id={}, file_key={}",
                        volume_id, file_key
                    );
                    return Ok(());
                }
                Err(e) => {
                    let msg = format!("write_blob failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_volume_channel(volume_addr).await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("write_blob failed after max retries".to_string())
    }

    pub async fn batch_write_blob(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
        entries: Vec<(i64, i32, Vec<u8>, u32)>,
    ) -> Result<(), String> {
        debug!(
            "batch_write_blob: addr={}, volume_id={}, file_key={}, entries={}",
            volume_addr,
            volume_id,
            file_key,
            entries.len()
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_volume_channel(volume_addr).await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get volume channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = VolumeServiceClient::new(channel)
                .max_decoding_message_size(256 * 1024 * 1024)
                .max_encoding_message_size(256 * 1024 * 1024);

            let needle_entries: Vec<powerfs_volume::proto::powerfs::NeedleBlobEntry> = entries
                .iter()
                .map(|(offset, size, data, cookie)| {
                    powerfs_volume::proto::powerfs::NeedleBlobEntry {
                        offset: *offset,
                        size: *size,
                        needle_blob: data.clone(),
                        cookie: *cookie,
                    }
                })
                .collect();

            let request = powerfs_volume::proto::powerfs::BatchWriteNeedleBlobRequest {
                volume_id,
                file_key,
                entries: needle_entries,
            };

            match client
                .batch_write_needle_blob(tonic::Request::new(request))
                .await
            {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.success {
                        return Err(format!(
                            "batch_write_blob partial failure: {}/{} succeeded",
                            resp.success_count,
                            entries.len()
                        ));
                    }
                    debug!(
                        "batch_write_blob succeeded: volume_id={}, file_key={}",
                        volume_id, file_key
                    );
                    return Ok(());
                }
                Err(e) => {
                    let msg = format!("batch_write_blob failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_volume_channel(volume_addr).await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("batch_write_blob failed after max retries".to_string())
    }

    pub async fn read_blob(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: i32,
    ) -> Result<Vec<u8>, String> {
        debug!(
            "read_blob: addr={}, volume_id={}, file_key={}, offset={}, size={}",
            volume_addr, volume_id, file_key, offset, size
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_volume_channel(volume_addr).await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get volume channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = VolumeServiceClient::new(channel)
                .max_decoding_message_size(256 * 1024 * 1024)
                .max_encoding_message_size(256 * 1024 * 1024);
            let request = ReadNeedleBlobRequest {
                volume_id,
                file_key,
                offset,
                size,
            };

            match client.read_needle_blob(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.success {
                        return Err("read_blob failed: volume server returned failure".to_string());
                    }
                    debug!(
                        "read_blob succeeded: volume_id={}, file_key={}, size={}",
                        volume_id,
                        file_key,
                        resp.needle_blob.len()
                    );
                    return Ok(resp.needle_blob);
                }
                Err(e) => {
                    let msg = format!("read_blob failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_volume_channel(volume_addr).await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("read_blob failed after max retries".to_string())
    }

    pub async fn create_entry(&self, entry: Entry, client_id: &str) -> Result<u64, String> {
        debug!(
            "create_entry: name={}, directory={}",
            entry.name, entry.directory
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_master_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get master channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = MasterServiceClient::new(channel);
            let request = CreateEntryRequest {
                entry: Some(entry.clone()),
                client_id: client_id.to_string(),
            };

            match client.create_entry(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.error.is_empty() {
                        return Err(resp.error);
                    }
                    debug!("create_entry succeeded: inode={}", resp.inode);
                    return Ok(resp.inode);
                }
                Err(e) => {
                    let msg = format!("create_entry failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_master_channel().await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("create_entry failed after max retries".to_string())
    }

    pub async fn update_entry(&self, entry: &Entry, client_id: &str) -> Result<(), String> {
        debug!(
            "update_entry: name={}, directory={}",
            entry.name, entry.directory
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_master_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get master channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = MasterServiceClient::new(channel);
            let request = UpdateEntryRequest {
                entry: Some(entry.clone()),
                client_id: client_id.to_string(),
            };

            match client.update_entry(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.success {
                        return Err(
                            "update_entry failed: master server returned failure".to_string()
                        );
                    }
                    debug!("update_entry succeeded");
                    return Ok(());
                }
                Err(e) => {
                    let msg = format!("update_entry failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_master_channel().await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("update_entry failed after max retries".to_string())
    }

    pub async fn get_entry(&self, path: &str) -> Result<Option<Entry>, String> {
        debug!("get_entry: path={}", path);

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_master_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get master channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = MasterServiceClient::new(channel);
            let request = GetEntryRequest {
                path: path.to_string(),
            };

            match client.get_entry(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.error.is_empty() {
                        return Err(resp.error);
                    }
                    debug!("get_entry succeeded: found={}", resp.entry.is_some());
                    return Ok(resp.entry);
                }
                Err(e) => {
                    let msg = format!("get_entry failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_master_channel().await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("get_entry failed after max retries".to_string())
    }

    pub async fn get_entry_by_inode(&self, inode: u64) -> Result<Option<(Entry, String)>, String> {
        debug!("get_entry_by_inode: inode={}", inode);

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_master_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get master channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = MasterServiceClient::new(channel);
            let request = GetEntryByInodeRequest { inode };

            match client
                .get_entry_by_inode(tonic::Request::new(request))
                .await
            {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.error.is_empty() {
                        return Err(resp.error);
                    }
                    debug!("get_entry_by_inode succeeded: found={}", resp.found);
                    if resp.found {
                        return Ok(Some((resp.entry.unwrap(), resp.path)));
                    }
                    return Ok(None);
                }
                Err(e) => {
                    let msg = format!("get_entry_by_inode failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_master_channel().await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("get_entry_by_inode failed after max retries".to_string())
    }

    pub async fn delete_entry(
        &self,
        ino: u64,
        is_directory: bool,
        client_id: &str,
    ) -> Result<bool, String> {
        debug!("delete_entry: ino={}, is_directory={}", ino, is_directory);

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_master_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get master channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = MasterServiceClient::new(channel);
            let request = DeleteEntryRequest {
                ino,
                is_directory,
                client_id: client_id.to_string(),
            };

            match client.delete_entry(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.error.is_empty() {
                        return Err(resp.error);
                    }
                    debug!("delete_entry succeeded: success={}", resp.success);
                    return Ok(resp.success);
                }
                Err(e) => {
                    let msg = format!("delete_entry failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_master_channel().await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("delete_entry failed after max retries".to_string())
    }

    pub async fn rename_entry(
        &self,
        old_parent_ino: u64,
        old_name: &str,
        new_parent_ino: u64,
        new_name: &str,
        client_id: &str,
    ) -> Result<bool, String> {
        debug!(
            "rename_entry: old_parent_ino={}, old_name={}, new_parent_ino={}, new_name={}",
            old_parent_ino, old_name, new_parent_ino, new_name
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_master_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get master channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = MasterServiceClient::new(channel);
            let request = RenameEntryRequest {
                old_parent_ino,
                old_name: old_name.to_string(),
                new_parent_ino,
                new_name: new_name.to_string(),
                client_id: client_id.to_string(),
            };

            match client.rename_entry(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.error.is_empty() {
                        return Err(resp.error);
                    }
                    debug!("rename_entry succeeded");
                    return Ok(resp.success);
                }
                Err(e) => {
                    let msg = format!("rename_entry failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_master_channel().await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("rename_entry failed after max retries".to_string())
    }

    pub async fn list_entries(
        &self,
        parent_ino: u64,
        limit: u64,
        start_after: &str,
    ) -> Result<Vec<Entry>, String> {
        debug!(
            "list_entries: parent_ino={}, limit={}, start_after={}",
            parent_ino, limit, start_after
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_master_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get master channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = MasterServiceClient::new(channel);
            let request = ListEntriesRequest {
                parent_ino,
                limit,
                last_name: start_after.to_string(),
            };

            match client.list_entries(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    debug!("list_entries succeeded: {} entries", resp.entries.len());
                    return Ok(resp.entries);
                }
                Err(e) => {
                    let msg = format!("list_entries failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_master_channel().await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("list_entries failed after max retries".to_string())
    }

    pub async fn lookup_directory_entry(
        &self,
        parent_ino: u64,
        name: &str,
    ) -> Result<Option<Entry>, String> {
        debug!(
            "lookup_directory_entry: parent_ino={}, name={}",
            parent_ino, name
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_master_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get master channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = MasterServiceClient::new(channel);
            let request = LookupDirectoryEntryRequest {
                parent_ino,
                name: name.to_string(),
            };

            match client
                .lookup_directory_entry(tonic::Request::new(request))
                .await
            {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.error.is_empty() {
                        return Err(resp.error);
                    }
                    debug!(
                        "lookup_directory_entry succeeded: found={}",
                        resp.entry.is_some()
                    );
                    return Ok(resp.entry);
                }
                Err(e) => {
                    let msg = format!("lookup_directory_entry failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_master_channel().await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("lookup_directory_entry failed after max retries".to_string())
    }

    pub async fn subscribe_metadata(
        &self,
        path_prefix: &str,
    ) -> Result<tonic::Streaming<MetadataNotification>, String> {
        debug!("subscribe_metadata: path_prefix={}", path_prefix);

        let channel = match self.get_or_create_master_channel().await {
            Ok(ch) => ch,
            Err(e) => return Err(e),
        };

        let mut client = MasterServiceClient::new(channel);
        let request = SubscribeMetadataRequest {
            path_prefix: path_prefix.to_string(),
        };

        match client
            .subscribe_metadata(tonic::Request::new(request))
            .await
        {
            Ok(response) => {
                debug!("subscribe_metadata succeeded");
                Ok(response.into_inner())
            }
            Err(e) => {
                let msg = format!("subscribe_metadata failed: {}", e);
                error!("{}", msg);
                self.invalidate_master_channel().await;
                Err(msg)
            }
        }
    }

    pub async fn acquire_lease(
        &self,
        path: &str,
        client_id: &str,
        duration_ms: u64,
    ) -> Result<(String, u64), String> {
        debug!(
            "acquire_lease: path={}, client_id={}, duration_ms={}",
            path, client_id, duration_ms
        );

        let channel = match self.get_or_create_master_channel().await {
            Ok(ch) => ch,
            Err(e) => return Err(e),
        };

        let mut client = MasterServiceClient::new(channel);
        let request = LeaseRequest {
            path: path.to_string(),
            client_id: client_id.to_string(),
            duration_ms,
        };

        match client.acquire_lease(tonic::Request::new(request)).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    debug!(
                        "acquire_lease succeeded: lease_id={}, epoch={}",
                        resp.lease_id, resp.epoch
                    );
                    Ok((resp.lease_id, resp.epoch))
                } else {
                    let msg = format!("acquire_lease failed: {}", resp.error);
                    error!("{}", msg);
                    Err(msg)
                }
            }
            Err(e) => {
                let msg = format!("acquire_lease failed: {}", e);
                error!("{}", msg);
                self.invalidate_master_channel().await;
                Err(msg)
            }
        }
    }

    pub async fn release_lease(&self, lease_id: &str) -> Result<bool, String> {
        debug!("release_lease: lease_id={}", lease_id);

        let channel = match self.get_or_create_master_channel().await {
            Ok(ch) => ch,
            Err(e) => return Err(e),
        };

        let mut client = MasterServiceClient::new(channel);
        let request = LeaseReleaseRequest {
            lease_id: lease_id.to_string(),
        };

        match client.release_lease(tonic::Request::new(request)).await {
            Ok(response) => {
                let resp = response.into_inner();
                debug!("release_lease succeeded: success={}", resp.success);
                Ok(resp.success)
            }
            Err(e) => {
                let msg = format!("release_lease failed: {}", e);
                error!("{}", msg);
                self.invalidate_master_channel().await;
                Err(msg)
            }
        }
    }

    pub async fn renew_lease(
        &self,
        lease_id: &str,
        duration_ms: u64,
    ) -> Result<(bool, u64), String> {
        debug!(
            "renew_lease: lease_id={}, duration_ms={}",
            lease_id, duration_ms
        );

        let channel = match self.get_or_create_master_channel().await {
            Ok(ch) => ch,
            Err(e) => return Err(e),
        };

        let mut client = MasterServiceClient::new(channel);
        let request = LeaseRenewRequest {
            lease_id: lease_id.to_string(),
            duration_ms,
        };

        match client.renew_lease(tonic::Request::new(request)).await {
            Ok(response) => {
                let resp = response.into_inner();
                debug!(
                    "renew_lease succeeded: success={}, epoch={}",
                    resp.success, resp.epoch
                );
                Ok((resp.success, resp.epoch))
            }
            Err(e) => {
                let msg = format!("renew_lease failed: {}", e);
                error!("{}", msg);
                self.invalidate_master_channel().await;
                Err(msg)
            }
        }
    }

    pub async fn register_job_client(
        &self,
        job_id: &str,
        job_name: &str,
        client_id: &str,
    ) -> Result<bool, String> {
        debug!(
            "register_job_client: job_id={}, job_name={}, client_id={}",
            job_id, job_name, client_id
        );

        let channel = match self.get_or_create_master_channel().await {
            Ok(ch) => ch,
            Err(e) => return Err(e),
        };

        let mut client = MasterServiceClient::new(channel);
        let request = JobRegistrationRequest {
            job_id: job_id.to_string(),
            job_name: job_name.to_string(),
            client_id: client_id.to_string(),
        };

        match client
            .register_job_client(tonic::Request::new(request))
            .await
        {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    debug!("register_job_client succeeded: job_id={}", job_id);
                    Ok(true)
                } else {
                    let msg = format!("register_job_client failed: {}", resp.error);
                    error!("{}", msg);
                    Err(msg)
                }
            }
            Err(e) => {
                let msg = format!("register_job_client failed: {}", e);
                error!("{}", msg);
                self.invalidate_master_channel().await;
                Err(msg)
            }
        }
    }

    pub async fn deregister_job_client(
        &self,
        job_id: &str,
        client_id: &str,
    ) -> Result<bool, String> {
        debug!(
            "deregister_job_client: job_id={}, client_id={}",
            job_id, client_id
        );

        let channel = match self.get_or_create_master_channel().await {
            Ok(ch) => ch,
            Err(e) => return Err(e),
        };

        let mut client = MasterServiceClient::new(channel);
        let request = JobDeregistrationRequest {
            job_id: job_id.to_string(),
            client_id: client_id.to_string(),
        };

        match client
            .deregister_job_client(tonic::Request::new(request))
            .await
        {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    debug!("deregister_job_client succeeded: job_id={}", job_id);
                    Ok(true)
                } else {
                    let msg = format!("deregister_job_client failed: {}", resp.error);
                    error!("{}", msg);
                    Err(msg)
                }
            }
            Err(e) => {
                let msg = format!("deregister_job_client failed: {}", e);
                error!("{}", msg);
                self.invalidate_master_channel().await;
                Err(msg)
            }
        }
    }

    pub async fn complete_job(&self, job_id: &str) -> Result<u64, String> {
        debug!("complete_job: job_id={}", job_id);

        let channel = match self.get_or_create_master_channel().await {
            Ok(ch) => ch,
            Err(e) => return Err(e),
        };

        let mut client = MasterServiceClient::new(channel);
        let request = JobCompletionRequest {
            job_id: job_id.to_string(),
        };

        match client.complete_job(tonic::Request::new(request)).await {
            Ok(response) => {
                let resp = response.into_inner();
                debug!("complete_job succeeded: job_id={}", job_id);
                Ok(resp.invalidated_entries)
            }
            Err(e) => {
                let msg = format!("complete_job failed: {}", e);
                error!("{}", msg);
                self.invalidate_master_channel().await;
                Err(msg)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn keep_connected(
        &self,
        client_type: &str,
        mount_point: &str,
        collection: &str,
        replication: &str,
        pid: u64,
        host: &str,
        dirty_chunks: u64,
        dirty_bytes: u64,
    ) -> Result<(), String> {
        debug!(
            "keep_connected: client_type={}, mount_point={}, collection={}",
            client_type, mount_point, collection
        );

        let channel = match self.get_or_create_master_channel().await {
            Ok(ch) => ch,
            Err(e) => return Err(e),
        };

        let mut client = MasterServiceClient::new(channel);

        let request_stream = tokio_stream::iter(vec![KeepConnectedRequest {
            client_type: client_type.to_string(),
            client_address: String::new(),
            version: String::new(),
            filer_group: String::new(),
            data_center: String::new(),
            rack: String::new(),
            mount_point: mount_point.to_string(),
            collection: collection.to_string(),
            replication: replication.to_string(),
            pid,
            host: host.to_string(),
            dirty_chunks,
            dirty_bytes,
        }]);

        match client
            .keep_connected(tonic::Request::new(request_stream))
            .await
        {
            Ok(_response) => {
                debug!("keep_connected succeeded");
                Ok(())
            }
            Err(e) => {
                let msg = format!("keep_connected failed: {}", e);
                error!("{}", msg);
                self.invalidate_master_channel().await;
                Err(msg)
            }
        }
    }

    pub async fn push_delta(
        &self,
        client_id: &str,
        deltas: &[DeltaOp],
        client_vclock: &VectorClock,
    ) -> Result<PushDeltaResponse, String> {
        debug!(
            "push_delta: client_id={}, delta_count={}",
            client_id,
            deltas.len()
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_master_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get master channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = MasterServiceClient::new(channel);
            let request = PushDeltaRequest {
                client_id: client_id.to_string(),
                deltas: deltas.to_vec(),
                client_vclock: Some(client_vclock.clone()),
            };

            match client.push_delta(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    debug!("push_delta succeeded");
                    return Ok(resp);
                }
                Err(e) => {
                    let msg = format!("push_delta failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_master_channel().await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("push_delta failed after max retries".to_string())
    }

    pub async fn pull_delta(
        &self,
        client_id: &str,
        client_vclock: &VectorClock,
    ) -> Result<PullDeltaResponse, String> {
        debug!("pull_delta: client_id={}", client_id);

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_master_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get master channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = MasterServiceClient::new(channel);
            let request = PullDeltaRequest {
                client_id: client_id.to_string(),
                client_vclock: Some(client_vclock.clone()),
            };

            match client.pull_delta(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    debug!("pull_delta succeeded: delta_count={}", resp.deltas.len());
                    return Ok(resp);
                }
                Err(e) => {
                    let msg = format!("pull_delta failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_master_channel().await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("pull_delta failed after max retries".to_string())
    }

    pub async fn get_statistics(&self, collection: &str) -> Result<StatisticsResponse, String> {
        debug!("get_statistics: collection={}", collection);

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.get_or_create_master_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    if attempt == self.config.max_retry_count {
                        return Err(e);
                    }
                    warn!("Failed to get master channel (attempt {}): {}", attempt, e);
                    tokio::time::sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let mut client = MasterServiceClient::new(channel);
            let request = StatisticsRequest {
                collection: collection.to_string(),
                data_center: String::new(),
                rack: String::new(),
            };

            match client.get_statistics(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.error.is_empty() {
                        return Err(resp.error);
                    }
                    debug!(
                        "get_statistics succeeded: total_size={}, used_size={}",
                        resp.total_volume_size, resp.total_used_size
                    );
                    return Ok(resp);
                }
                Err(e) => {
                    let msg = format!("get_statistics failed (attempt {}): {}", attempt, e);
                    warn!("{}", msg);
                    self.invalidate_master_channel().await;
                    if attempt == self.config.max_retry_count {
                        return Err(msg);
                    }
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }

        Err("get_statistics failed after max retries".to_string())
    }
}

pub struct SyncFuseClient {
    client: Arc<PowerFuseClient>,
}

const GRPC_CALL_TIMEOUT: Duration = Duration::from_secs(15);

impl SyncFuseClient {
    pub fn new(client: Arc<PowerFuseClient>) -> Self {
        SyncFuseClient { client }
    }

    fn block_with_timeout<F, T>(&self, future: F) -> Result<T, String>
    where
        F: std::future::Future<Output = Result<T, String>>,
    {
        self.client.runtime_handle.block_on(async {
            match tokio::time::timeout(GRPC_CALL_TIMEOUT, future).await {
                Ok(result) => result,
                Err(_) => Err(format!(
                    "gRPC call timed out after {}s",
                    GRPC_CALL_TIMEOUT.as_secs()
                )),
            }
        })
    }

    pub fn assign_fid(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<AssignFidResult, String> {
        self.block_with_timeout(self.client.assign_fid(collection, replication))
    }

    pub fn lookup_volume(&self, volume_id: VolumeId) -> Result<Vec<Location>, String> {
        self.block_with_timeout(self.client.lookup_volume(volume_id))
    }

    pub fn write_data(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
        data: Vec<u8>,
    ) -> Result<(), String> {
        self.block_with_timeout(
            self.client
                .write_data(volume_addr, volume_id, file_key, data),
        )
    }

    pub fn read_data(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
    ) -> Result<Vec<u8>, String> {
        self.block_with_timeout(self.client.read_data(volume_addr, volume_id, file_key))
    }

    pub fn delete_data(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
    ) -> Result<(), String> {
        self.block_with_timeout(self.client.delete_data(volume_addr, volume_id, file_key))
    }

    pub fn batch_write_blob(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
        entries: Vec<(i64, i32, Vec<u8>, u32)>,
    ) -> Result<(), String> {
        self.block_with_timeout(self.client.batch_write_blob(
            volume_addr,
            volume_id,
            file_key,
            entries,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write_blob(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: i32,
        data: Vec<u8>,
        cookie: u32,
    ) -> Result<(), String> {
        self.block_with_timeout(self.client.write_blob(
            volume_addr,
            volume_id,
            file_key,
            offset,
            size,
            data,
            cookie,
        ))
    }

    pub fn read_blob(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: i32,
    ) -> Result<Vec<u8>, String> {
        self.block_with_timeout(self.client.read_blob(
            volume_addr,
            volume_id,
            file_key,
            offset,
            size,
        ))
    }

    pub fn create_entry(&self, entry: Entry, client_id: &str) -> Result<u64, String> {
        self.block_with_timeout(self.client.create_entry(entry, client_id))
    }

    pub fn update_entry(&self, entry: &Entry, client_id: &str) -> Result<(), String> {
        self.block_with_timeout(self.client.update_entry(entry, client_id))
    }

    pub fn get_entry(&self, path: &str) -> Result<Option<Entry>, String> {
        self.block_with_timeout(self.client.get_entry(path))
    }

    pub fn get_entry_by_inode(&self, inode: u64) -> Result<Option<(Entry, String)>, String> {
        self.block_with_timeout(self.client.get_entry_by_inode(inode))
    }

    pub fn delete_entry(
        &self,
        ino: u64,
        is_directory: bool,
        client_id: &str,
    ) -> Result<bool, String> {
        self.block_with_timeout(self.client.delete_entry(ino, is_directory, client_id))
    }

    pub fn rename_entry(
        &self,
        old_parent_ino: u64,
        old_name: &str,
        new_parent_ino: u64,
        new_name: &str,
        client_id: &str,
    ) -> Result<bool, String> {
        self.block_with_timeout(self.client.rename_entry(
            old_parent_ino,
            old_name,
            new_parent_ino,
            new_name,
            client_id,
        ))
    }

    pub fn list_entries(
        &self,
        parent_ino: u64,
        limit: u64,
        start_after: &str,
    ) -> Result<Vec<Entry>, String> {
        self.block_with_timeout(self.client.list_entries(parent_ino, limit, start_after))
    }

    pub fn lookup_directory_entry(
        &self,
        parent_ino: u64,
        name: &str,
    ) -> Result<Option<Entry>, String> {
        self.block_with_timeout(self.client.lookup_directory_entry(parent_ino, name))
    }

    pub fn invalidate_volume_channel(&self, addr: &str) {
        self.client
            .runtime_handle
            .block_on(self.client.invalidate_volume_channel(addr));
    }

    pub fn acquire_lease(
        &self,
        path: &str,
        client_id: &str,
        duration_ms: u64,
    ) -> Result<(String, u64), String> {
        self.block_with_timeout(self.client.acquire_lease(path, client_id, duration_ms))
    }

    pub fn release_lease(&self, lease_id: &str) -> Result<bool, String> {
        self.block_with_timeout(self.client.release_lease(lease_id))
    }

    pub fn renew_lease(&self, lease_id: &str, duration_ms: u64) -> Result<(bool, u64), String> {
        self.block_with_timeout(self.client.renew_lease(lease_id, duration_ms))
    }

    pub fn register_job_client(
        &self,
        job_id: &str,
        job_name: &str,
        client_id: &str,
    ) -> Result<bool, String> {
        self.block_with_timeout(self.client.register_job_client(job_id, job_name, client_id))
    }

    pub fn deregister_job_client(&self, job_id: &str, client_id: &str) -> Result<bool, String> {
        self.block_with_timeout(self.client.deregister_job_client(job_id, client_id))
    }

    pub fn complete_job(&self, job_id: &str) -> Result<u64, String> {
        self.block_with_timeout(self.client.complete_job(job_id))
    }

    pub fn get_statistics(&self, collection: &str) -> Result<StatisticsResponse, String> {
        self.block_with_timeout(self.client.get_statistics(collection))
    }
}
