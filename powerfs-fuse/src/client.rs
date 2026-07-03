use log::{debug, error, info, warn};
use powerfs_common::types::{Fid, VolumeId};
use powerfs_master::proto::powerfs::{
    master_service_client::MasterServiceClient, AssignRequest, CreateEntryRequest,
    DeleteEntryRequest, Entry, GetEntryRequest, ListEntriesRequest, LookupDirectoryEntryRequest,
    LookupVolumeRequest, UpdateEntryRequest,
};
use powerfs_volume::proto::powerfs::{
    volume_service_client::VolumeServiceClient, DeleteNeedleRequest, ReadNeedleBlobRequest,
    ReadNeedleRequest, WriteNeedleBlobRequest, WriteNeedleRequest,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{RwLock, Semaphore};
use tonic::transport::Channel;

use powerfs_master::proto::powerfs::Location;

type AssignFidResult = (Fid, Option<Location>, Vec<String>, Vec<Location>);

struct ConnectionPool {
    channels: RwLock<Vec<Channel>>,
    addr: String,
    max_size: usize,
    semaphore: Arc<Semaphore>,
    config: GrpcConfig,
}

impl ConnectionPool {
    fn new(addr: String, max_size: usize, config: GrpcConfig) -> Self {
        Self {
            channels: RwLock::new(Vec::new()),
            addr,
            max_size,
            semaphore: Arc::new(Semaphore::new(max_size)),
            config,
        }
    }

    async fn get(&self) -> Result<Channel, String> {
        let _permit = self.semaphore.acquire().await.unwrap();

        let mut channels = self.channels.write().await;
        if let Some(ch) = channels.pop() {
            return Ok(ch);
        }

        info!("Creating new connection to: {}", self.addr);
        let grpc_addr = format!("http://{}", self.addr);
        let ch = Channel::from_shared(grpc_addr)
            .map_err(|e| format!("invalid address: {}", e))?
            .http2_keep_alive_interval(self.config.keepalive_interval)
            .keep_alive_timeout(self.config.keepalive_timeout)
            .connect_timeout(self.config.connect_timeout)
            .connect()
            .await
            .map_err(|e| {
                let msg = format!("failed to connect to {}: {}", self.addr, e);
                error!("{}", msg);
                msg
            })?;

        info!("Connected to: {}", self.addr);
        Ok(ch)
    }

    async fn put(&self, ch: Channel) {
        let mut channels = self.channels.write().await;
        if channels.len() < self.max_size {
            channels.push(ch);
        }
    }

    async fn clear(&self) {
        let mut channels = self.channels.write().await;
        channels.clear();
    }
}

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
    master_pool: RwLock<Option<Arc<ConnectionPool>>>,
    volume_pools: RwLock<HashMap<String, Arc<ConnectionPool>>>,
    runtime_handle: Handle,
    config: GrpcConfig,
    pool_max_size: usize,
}

impl PowerFuseClient {
    pub fn new(master_addr: &str, runtime_handle: Handle) -> Arc<Self> {
        Arc::new(PowerFuseClient {
            master_addr: master_addr.to_string(),
            master_pool: RwLock::new(None),
            volume_pools: RwLock::new(HashMap::new()),
            runtime_handle,
            config: GrpcConfig::default(),
            pool_max_size: 8,
        })
    }

    pub fn with_config(master_addr: &str, runtime_handle: Handle, config: GrpcConfig) -> Arc<Self> {
        Arc::new(PowerFuseClient {
            master_addr: master_addr.to_string(),
            master_pool: RwLock::new(None),
            volume_pools: RwLock::new(HashMap::new()),
            runtime_handle,
            config,
            pool_max_size: 8,
        })
    }

    async fn ensure_master_channel(&self) -> Result<Channel, String> {
        {
            let pool = self.master_pool.read().await;
            if let Some(p) = &*pool {
                return p.get().await;
            }
        }

        let pool = Arc::new(ConnectionPool::new(
            self.master_addr.clone(),
            self.pool_max_size,
            self.config,
        ));

        {
            let mut master_pool = self.master_pool.write().await;
            *master_pool = Some(pool.clone());
        }

        pool.get().await
    }

    async fn return_master_channel(&self, ch: Channel) {
        if let Some(pool) = &*self.master_pool.read().await {
            pool.put(ch).await;
        }
    }

    async fn get_volume_channel(&self, addr: &str) -> Result<Channel, String> {
        {
            let pools = self.volume_pools.read().await;
            if let Some(pool) = pools.get(addr) {
                return pool.get().await;
            }
        }

        let pool = Arc::new(ConnectionPool::new(
            addr.to_string(),
            self.pool_max_size,
            self.config,
        ));

        {
            let mut pools = self.volume_pools.write().await;
            pools.insert(addr.to_string(), pool.clone());
        }

        pool.get().await
    }

    async fn return_volume_channel(&self, addr: &str, ch: Channel) {
        if let Some(pool) = self.volume_pools.read().await.get(addr) {
            pool.put(ch).await;
        }
    }

    pub async fn invalidate_master_channel(&self) {
        let mut pool = self.master_pool.write().await;
        if let Some(p) = &*pool {
            p.clear().await;
        }
        *pool = None;
        warn!("Invalidated master channel");
    }

    pub async fn invalidate_volume_channel(&self, addr: &str) {
        let mut pools = self.volume_pools.write().await;
        if let Some(pool) = pools.get(addr) {
            pool.clear().await;
        }
        pools.remove(addr);
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
            let channel = match self.ensure_master_channel().await {
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

            let mut client = MasterServiceClient::new(channel.clone());
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
                        self.return_master_channel(channel).await;
                        return Err(resp.error);
                    }
                    let fid =
                        Fid::from_string(&resp.fid).map_err(|e| format!("invalid fid: {}", e))?;
                    debug!("assign_fid succeeded: fid={}", fid);
                    self.return_master_channel(channel).await;
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
            let channel = match self.ensure_master_channel().await {
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

            let mut client = MasterServiceClient::new(channel.clone());
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
                    self.return_master_channel(channel).await;
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
            let channel = match self.get_volume_channel(volume_addr).await {
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

            let mut client = VolumeServiceClient::new(channel.clone())
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
                        self.return_volume_channel(volume_addr, channel).await;
                        return Err("write failed: volume server returned failure".to_string());
                    }
                    debug!(
                        "write_data succeeded: volume_id={}, file_key={}",
                        volume_id, file_key
                    );
                    self.return_volume_channel(volume_addr, channel).await;
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
            let channel = match self.get_volume_channel(volume_addr).await {
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

            let mut client = VolumeServiceClient::new(channel.clone())
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
                        self.return_volume_channel(volume_addr, channel).await;
                        return Err("read failed: volume server returned failure".to_string());
                    }
                    debug!(
                        "read_data succeeded: volume_id={}, file_key={}, size={}",
                        volume_id,
                        file_key,
                        resp.data.len()
                    );
                    self.return_volume_channel(volume_addr, channel).await;
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
            let channel = match self.get_volume_channel(volume_addr).await {
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

            let mut client = VolumeServiceClient::new(channel.clone())
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
                        self.return_volume_channel(volume_addr, channel).await;
                        return Err("delete failed: volume server returned failure".to_string());
                    }
                    debug!(
                        "delete_data succeeded: volume_id={}, file_key={}",
                        volume_id, file_key
                    );
                    self.return_volume_channel(volume_addr, channel).await;
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
            let channel = match self.get_volume_channel(volume_addr).await {
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
            let channel = match self.get_volume_channel(volume_addr).await {
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

    pub async fn create_entry(&self, entry: Entry) -> Result<u64, String> {
        debug!(
            "create_entry: name={}, directory={}",
            entry.name, entry.directory
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.ensure_master_channel().await {
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

    pub async fn update_entry(&self, entry: &Entry) -> Result<(), String> {
        debug!(
            "update_entry: name={}, directory={}",
            entry.name, entry.directory
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.ensure_master_channel().await {
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
            let channel = match self.ensure_master_channel().await {
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

    pub async fn delete_entry(&self, path: &str, is_directory: bool) -> Result<bool, String> {
        debug!("delete_entry: path={}, is_directory={}", path, is_directory);

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.ensure_master_channel().await {
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
                path: path.to_string(),
                is_directory,
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

    pub async fn list_entries(
        &self,
        path: &str,
        limit: u64,
        start_after: &str,
    ) -> Result<Vec<Entry>, String> {
        debug!(
            "list_entries: path={}, limit={}, start_after={}",
            path, limit, start_after
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.ensure_master_channel().await {
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
                directory: path.to_string(),
                limit,
                last_name: start_after.to_string(),
            };

            match client.list_entries(tonic::Request::new(request)).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if !resp.error.is_empty() {
                        return Err(resp.error);
                    }
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
        directory: &str,
        name: &str,
    ) -> Result<Option<Entry>, String> {
        debug!(
            "lookup_directory_entry: directory={}, name={}",
            directory, name
        );

        for attempt in 1..=self.config.max_retry_count {
            let channel = match self.ensure_master_channel().await {
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
                directory: directory.to_string(),
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
}

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
    ) -> Result<AssignFidResult, String> {
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
        self.client.runtime_handle.block_on(self.client.write_blob(
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
        self.client.runtime_handle.block_on(self.client.read_blob(
            volume_addr,
            volume_id,
            file_key,
            offset,
            size,
        ))
    }

    pub fn create_entry(&self, entry: Entry) -> Result<u64, String> {
        self.client
            .runtime_handle
            .block_on(self.client.create_entry(entry))
    }

    pub fn update_entry(&self, entry: &Entry) -> Result<(), String> {
        self.client
            .runtime_handle
            .block_on(self.client.update_entry(entry))
    }

    pub fn get_entry(&self, path: &str) -> Result<Option<Entry>, String> {
        self.client
            .runtime_handle
            .block_on(self.client.get_entry(path))
    }

    pub fn delete_entry(&self, path: &str, is_directory: bool) -> Result<bool, String> {
        self.client
            .runtime_handle
            .block_on(self.client.delete_entry(path, is_directory))
    }

    pub fn list_entries(
        &self,
        path: &str,
        limit: u64,
        start_after: &str,
    ) -> Result<Vec<Entry>, String> {
        self.client
            .runtime_handle
            .block_on(self.client.list_entries(path, limit, start_after))
    }

    pub fn lookup_directory_entry(
        &self,
        directory: &str,
        name: &str,
    ) -> Result<Option<Entry>, String> {
        self.client
            .runtime_handle
            .block_on(self.client.lookup_directory_entry(directory, name))
    }

    pub fn invalidate_volume_channel(&self, addr: &str) {
        self.client
            .runtime_handle
            .block_on(self.client.invalidate_volume_channel(addr));
    }
}
