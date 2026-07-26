//! PowerFS Net Client - powerfs-net binary protocol client for FUSE
//!
//! This module provides a client that communicates with PowerFS Master/Volume
//! servers using the lightweight powerfs-net binary protocol instead of gRPC.

use log::info;
use powerfs_common::types::{Fid, VolumeId};
use powerfs_master::proto::powerfs::{Entry, FuseAttributes, Location};
use powerfs_net::serialize::{EntryInfo, TlvDecoder, TlvEncoder};
use powerfs_net::{
    ClientConfig, ClientType, FieldId, MsgType, NetMessage, NetResult, PowerFsNetClient,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Result type for assign_fid: (fid, primary_location, stripe_fids, stripe_locations)
pub type AssignFidResult = (Fid, Option<Location>, Vec<String>, Vec<Location>);

/// Configuration for the net client
#[derive(Debug, Clone)]
pub struct NetClientConfig {
    pub master_addr: String,
    pub master_net_port: u16,
    pub volume_net_port: u16,
    pub client_id: u64,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl Default for NetClientConfig {
    fn default() -> Self {
        Self {
            master_addr: "127.0.0.1".into(),
            master_net_port: 9334,
            volume_net_port: 8081,
            client_id: 0,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
        }
    }
}

/// Decoded assign result
#[derive(Debug, Clone, Default)]
pub struct AssignResult {
    pub fid: String,
    pub location_url: String,
    pub replica_count: usize,
}

/// Decoded volume location
#[derive(Debug, Clone, Default)]
pub struct VolumeLocation {
    pub url: String,
    pub data_center: String,
}

/// A unified client that can talk to both Master and Volume via powerfs-net
#[derive(Clone)]
pub struct PowerFuseNetClient {
    master_client: Arc<PowerFsNetClient>,
    volume_clients: Arc<RwLock<Vec<Arc<PowerFsNetClient>>>>,
    config: NetClientConfig,
}

impl PowerFuseNetClient {
    pub async fn new(config: NetClientConfig) -> NetResult<Self> {
        let master_client = Arc::new(PowerFsNetClient::new(ClientConfig {
            addr: config.master_addr.clone(),
            port: config.master_net_port,
            client_id: config.client_id,
            client_type: ClientType::Fuse,
            connect_timeout: config.connect_timeout,
            request_timeout: config.request_timeout,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(30),
            max_inflight_requests: 256,
        }));

        master_client.connect().await?;
        info!(
            "PowerFuseNetClient connected to Master at {}:{}",
            config.master_addr, config.master_net_port
        );

        Ok(Self {
            master_client,
            volume_clients: Arc::new(RwLock::new(Vec::new())),
            config,
        })
    }

    /// Get or create a volume client for the given address and port
    pub async fn get_volume_client(
        &self,
        addr: &str,
        port: u16,
    ) -> NetResult<Arc<PowerFsNetClient>> {
        {
            let clients = self.volume_clients.read().await;
            for client in clients.iter() {
                if client.is_connected() {
                    let cfg = &client.config;
                    if cfg.addr == addr && cfg.port == port {
                        return Ok(client.clone());
                    }
                }
            }
        }

        let new_client = Arc::new(PowerFsNetClient::new(ClientConfig {
            addr: addr.to_string(),
            port,
            client_id: self.config.client_id,
            client_type: ClientType::Fuse,
            connect_timeout: self.config.connect_timeout,
            request_timeout: self.config.request_timeout,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(30),
            max_inflight_requests: 256,
        }));

        new_client.connect().await?;

        self.volume_clients.write().await.push(new_client.clone());
        Ok(new_client)
    }

    // ========================================================================
    // Master operations
    // ========================================================================

    /// Assign a volume from Master
    pub async fn assign_volume(
        &self,
        collection: &str,
        replication: &str,
    ) -> NetResult<AssignResult> {
        let body = Self::encode_assign_req(collection, replication, 0, 0)?;

        let resp = self
            .master_client
            .send_request(MsgType::Assign, &body, &[])
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError(format!(
                "assign failed with status: {}",
                resp.header.status
            )));
        }

        Self::decode_assign_resp(&resp)
    }

    /// Lookup a volume location from Master
    pub async fn lookup_volume(&self, volume_id: u32) -> NetResult<Vec<VolumeLocation>> {
        let body = Self::encode_lookup_volume_req(&[volume_id.to_string()])?;

        let resp = self
            .master_client
            .send_request(MsgType::LookupVolume, &body, &[])
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError(
                "lookup_volume failed".into(),
            ));
        }

        Self::decode_lookup_volume_resp(&resp)
    }

    /// Lookup a directory entry
    pub async fn lookup(&self, parent_ino: u64, name: &str) -> NetResult<EntryInfo> {
        let body = powerfs_net::serialize::encode_lookup_req(parent_ino, name)?;

        let resp = self
            .master_client
            .send_request(MsgType::Lookup, &body, &[])
            .await?;

        if resp.header.status == powerfs_net::STATUS_ERR_NOT_FOUND {
            return Err(powerfs_net::NetError::ServerError("not found".into()));
        }

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError("lookup failed".into()));
        }

        let info = powerfs_net::serialize::decode_entry_resp(&resp.body)?;
        Ok(info)
    }

    /// Create a file
    pub async fn create_file(
        &self,
        parent_ino: u64,
        name: &str,
        mode: u64,
        uid: u64,
        gid: u64,
    ) -> NetResult<u64> {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ParentIno, parent_ino);
        enc.add_string(FieldId::Name, name)?;
        enc.add_u64(FieldId::Mode, mode);
        enc.add_u64(FieldId::Uid, uid);
        enc.add_u64(FieldId::Gid, gid);

        let resp = self
            .master_client
            .send_request(MsgType::Create, enc.into_bytes().as_slice(), &[])
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError("create failed".into()));
        }

        let mut dec = TlvDecoder::new(&resp.body);
        let ino = dec.next_u64(FieldId::Ino)?;
        Ok(ino)
    }

    /// Create a directory
    pub async fn mkdir(
        &self,
        parent_ino: u64,
        name: &str,
        mode: u64,
        uid: u64,
        gid: u64,
    ) -> NetResult<u64> {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ParentIno, parent_ino);
        enc.add_string(FieldId::Name, name)?;
        enc.add_u64(FieldId::Mode, mode);
        enc.add_u64(FieldId::Uid, uid);
        enc.add_u64(FieldId::Gid, gid);

        let resp = self
            .master_client
            .send_request(MsgType::Mkdir, enc.into_bytes().as_slice(), &[])
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError("mkdir failed".into()));
        }

        let mut dec = TlvDecoder::new(&resp.body);
        let ino = dec.next_u64(FieldId::Ino)?;
        Ok(ino)
    }

    /// Delete a file
    pub async fn unlink(&self, parent_ino: u64, name: &str) -> NetResult<()> {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ParentIno, parent_ino);
        enc.add_string(FieldId::Name, name)?;

        let resp = self
            .master_client
            .send_request(MsgType::Unlink, enc.into_bytes().as_slice(), &[])
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError("unlink failed".into()));
        }
        Ok(())
    }

    /// Delete a directory
    pub async fn rmdir(&self, parent_ino: u64, name: &str) -> NetResult<()> {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ParentIno, parent_ino);
        enc.add_string(FieldId::Name, name)?;

        let resp = self
            .master_client
            .send_request(MsgType::Rmdir, enc.into_bytes().as_slice(), &[])
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError("rmdir failed".into()));
        }
        Ok(())
    }

    /// List directory entries
    pub async fn readdir(
        &self,
        parent_ino: u64,
        limit: u64,
        last_name: &str,
    ) -> NetResult<Vec<EntryInfo>> {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ParentIno, parent_ino);
        enc.add_u64(FieldId::Limit, limit);
        enc.add_string(FieldId::LastName, last_name)?;

        let resp = self
            .master_client
            .send_request(MsgType::ReadDir, enc.into_bytes().as_slice(), &[])
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError("readdir failed".into()));
        }

        let mut dec = TlvDecoder::new(&resp.body);
        let count = dec.next_u64(FieldId::Limit).unwrap_or(0) as usize;
        let mut entries = Vec::with_capacity(count);

        for _ in 0..count {
            let name = dec.next_string(FieldId::Name).unwrap_or_default();
            let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);
            let mode = dec.next_u64(FieldId::Mode).unwrap_or(0);
            let mut entry = EntryInfo::default_entry();
            entry.ino = ino;
            entry.mode = mode as u32;
            entry.name = name;
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Set attributes
    pub async fn setattr(
        &self,
        ino: u64,
        size: Option<u64>,
        mode: Option<u64>,
        uid: Option<u64>,
        gid: Option<u64>,
    ) -> NetResult<()> {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Ino, ino);
        if let Some(s) = size {
            enc.add_u64(FieldId::Size, s);
        }
        if let Some(m) = mode {
            enc.add_u64(FieldId::Mode, m);
        }
        if let Some(u) = uid {
            enc.add_u64(FieldId::Uid, u);
        }
        if let Some(g) = gid {
            enc.add_u64(FieldId::Gid, g);
        }

        let resp = self
            .master_client
            .send_request(MsgType::SetAttr, enc.into_bytes().as_slice(), &[])
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError("setattr failed".into()));
        }
        Ok(())
    }

    // ========================================================================
    // Volume operations
    // ========================================================================

    /// Write data to Volume
    pub async fn write_data(
        &self,
        volume_addr: &str,
        volume_port: u16,
        volume_id: u32,
        file_key: u64,
        data: Vec<u8>,
    ) -> NetResult<()> {
        let vol_client = self.get_volume_client(volume_addr, volume_port).await?;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Ino, volume_id as u64);
        enc.add_u64(FieldId::Name, file_key);

        let resp = vol_client
            .send_request(MsgType::WriteNeedle, enc.into_bytes().as_slice(), &data)
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError("write failed".into()));
        }
        Ok(())
    }

    /// Read data from Volume
    pub async fn read_data(
        &self,
        volume_addr: &str,
        volume_port: u16,
        volume_id: u32,
        file_key: u64,
    ) -> NetResult<Vec<u8>> {
        let vol_client = self.get_volume_client(volume_addr, volume_port).await?;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Ino, volume_id as u64);
        enc.add_u64(FieldId::Name, file_key);

        let resp = vol_client
            .send_request(MsgType::ReadNeedle, enc.into_bytes().as_slice(), &[])
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError("read failed".into()));
        }

        Ok(resp.data)
    }

    /// Read blob data from Volume (with offset/size)
    pub async fn read_blob(
        &self,
        volume_addr: &str,
        volume_port: u16,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: u64,
    ) -> NetResult<Vec<u8>> {
        let vol_client = self.get_volume_client(volume_addr, volume_port).await?;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Ino, volume_id as u64);
        enc.add_u64(FieldId::Name, file_key);
        enc.add_u64(FieldId::Offset, offset as u64);
        enc.add_u64(FieldId::Size, size);

        let resp = vol_client
            .send_request(MsgType::ReadNeedleBlob, enc.into_bytes().as_slice(), &[])
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError(
                "read_blob failed".into(),
            ));
        }

        Ok(resp.data)
    }

    /// Write blob data to Volume (with offset/size)
    #[allow(clippy::too_many_arguments)]
    pub async fn write_blob(
        &self,
        volume_addr: &str,
        volume_port: u16,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: u64,
        data: Vec<u8>,
    ) -> NetResult<()> {
        let vol_client = self.get_volume_client(volume_addr, volume_port).await?;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Ino, volume_id as u64);
        enc.add_u64(FieldId::Name, file_key);
        enc.add_u64(FieldId::Offset, offset as u64);
        enc.add_u64(FieldId::Size, size);

        let resp = vol_client
            .send_request(MsgType::WriteNeedle, enc.into_bytes().as_slice(), &data)
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError(
                "write_blob failed".into(),
            ));
        }
        Ok(())
    }

    /// Delete data from Volume
    pub async fn delete_data(
        &self,
        volume_addr: &str,
        volume_port: u16,
        volume_id: u32,
        file_key: u64,
    ) -> NetResult<()> {
        let vol_client = self.get_volume_client(volume_addr, volume_port).await?;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Ino, volume_id as u64);
        enc.add_u64(FieldId::Name, file_key);

        let resp = vol_client
            .send_request(MsgType::DeleteNeedle, enc.into_bytes().as_slice(), &[])
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError("delete failed".into()));
        }
        Ok(())
    }

    /// Acquire a range lease
    #[allow(clippy::too_many_arguments)]
    pub async fn acquire_range_lease(
        &self,
        volume_addr: &str,
        volume_port: u16,
        inode: u64,
        stripe_start: u64,
        stripe_count: u64,
        client_id: &str,
        exclusive: bool,
        duration_ms: u64,
    ) -> NetResult<(String, u64)> {
        let vol_client = self.get_volume_client(volume_addr, volume_port).await?;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Ino, inode);
        enc.add_u64(FieldId::Offset, stripe_start);
        enc.add_u64(FieldId::Limit, stripe_count);
        enc.add_string(FieldId::ClientId, client_id)?;
        enc.add_u64(FieldId::Mode, if exclusive { 1 } else { 0 });
        enc.add_u64(FieldId::LeaseDuration, duration_ms);

        let resp = vol_client
            .send_request(MsgType::RangeLease, enc.into_bytes().as_slice(), &[])
            .await?;

        if !resp.is_ok() {
            return Err(powerfs_net::NetError::ServerError(
                "range_lease failed".into(),
            ));
        }

        let mut dec = TlvDecoder::new(&resp.body);
        let lease_id = dec
            .next_string(FieldId::LeaseId)
            .unwrap_or_else(|_| String::new());
        let epoch = dec.next_u64(FieldId::LeaseEpoch).unwrap_or(0);

        Ok((lease_id, epoch))
    }

    // ========================================================================
    // Utility methods
    // ========================================================================

    /// Check if master connection is alive
    pub fn is_connected(&self) -> bool {
        self.master_client.is_connected()
    }

    /// Get the master address
    pub fn master_addr(&self) -> &str {
        &self.config.master_addr
    }

    /// Get the master net port
    pub fn master_net_port(&self) -> u16 {
        self.config.master_net_port
    }

    /// Get the volume net port
    pub fn volume_net_port(&self) -> u16 {
        self.config.volume_net_port
    }

    // ========================================================================
    // Encode/decode helpers
    // ========================================================================

    /// Encode an Assign request
    pub fn encode_assign_req(
        collection: &str,
        replication: &str,
        stripe_count: u32,
        stripe_size: u64,
    ) -> NetResult<Vec<u8>> {
        let mut enc = TlvEncoder::new();
        enc.add_string(FieldId::Name, collection)?;
        enc.add_string(FieldId::Backend, replication)?;
        enc.add_u64(FieldId::Limit, stripe_count as u64);
        enc.add_u64(FieldId::ContentSize, stripe_size);
        Ok(enc.into_bytes())
    }

    /// Decode an Assign response
    pub fn decode_assign_resp(msg: &NetMessage) -> NetResult<AssignResult> {
        let mut dec = TlvDecoder::new(&msg.body);
        let fid = dec
            .next_string(FieldId::Name)
            .unwrap_or_else(|_| String::new());
        let location_url = dec
            .next_string(FieldId::Owner)
            .unwrap_or_else(|_| String::new());
        let replica_count = dec.next_u64(FieldId::Entries).unwrap_or(0) as usize;
        Ok(AssignResult {
            fid,
            location_url,
            replica_count,
        })
    }

    /// Encode a LookupVolume request
    pub fn encode_lookup_volume_req(volume_ids: &[String]) -> NetResult<Vec<u8>> {
        let mut enc = TlvEncoder::new();
        for vid in volume_ids {
            enc.add_string(FieldId::Name, vid)?;
        }
        Ok(enc.into_bytes())
    }

    /// Decode a LookupVolume response
    pub fn decode_lookup_volume_resp(msg: &NetMessage) -> NetResult<Vec<VolumeLocation>> {
        let mut dec = TlvDecoder::new(&msg.body);
        let count = dec.next_u64(FieldId::Limit).unwrap_or(0) as usize;
        let mut locations = Vec::with_capacity(count);

        for _ in 0..count {
            let url = dec
                .next_string(FieldId::Owner)
                .unwrap_or_else(|_| String::new());
            let data_center = dec
                .next_string(FieldId::Backend)
                .unwrap_or_else(|_| String::new());
            locations.push(VolumeLocation { url, data_center });
        }
        Ok(locations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_assign_req() {
        let body =
            PowerFuseNetClient::encode_assign_req("test_collection", "000", 1, 64 * 1024 * 1024)
                .unwrap();
        assert!(!body.is_empty());

        let mut dec = TlvDecoder::new(&body);
        let name = dec.next_string(FieldId::Name).unwrap();
        assert_eq!(name, "test_collection");
        let backend = dec.next_string(FieldId::Backend).unwrap();
        assert_eq!(backend, "000");
    }

    #[test]
    fn test_encode_lookup_volume_req() {
        let body = PowerFuseNetClient::encode_lookup_volume_req(&["1".to_string()]).unwrap();
        assert!(!body.is_empty());

        let mut dec = TlvDecoder::new(&body);
        let vid = dec.next_string(FieldId::Name).unwrap();
        assert_eq!(vid, "1");
    }
}

// ============================================================================
// SyncFuseNetClient - synchronous wrapper for PowerFuseNetClient
// ============================================================================

/// Synchronous wrapper around PowerFuseNetClient for use in FUSE callbacks.
/// This provides the same blocking interface as SyncFuseClient but uses
/// the powerfs-net binary protocol instead of gRPC.
pub struct SyncFuseNetClient {
    client: Arc<PowerFuseNetClient>,
    config: NetClientConfig,
}

impl SyncFuseNetClient {
    pub fn new(client: Arc<PowerFuseNetClient>, config: NetClientConfig) -> Self {
        Self { client, config }
    }

    pub fn inner(&self) -> &Arc<PowerFuseNetClient> {
        &self.client
    }

    /// Create a new SyncFuseNetClient from config (connects asynchronously)
    pub async fn connect(config: NetClientConfig) -> NetResult<Self> {
        let client = PowerFuseNetClient::new(config.clone()).await?;

        info!(
            "SyncFuseNetClient connected to Master at {}:{}",
            config.master_addr, config.master_net_port
        );

        Ok(Self {
            client: Arc::new(client),
            config,
        })
    }

    fn block_with_timeout<F, T>(&self, future: F, timeout: Duration) -> NetResult<T>
    where
        F: std::future::Future<Output = NetResult<T>>,
    {
        thread_local! {
            static BLOCKING_RUNTIME: std::cell::RefCell<Option<tokio::runtime::Runtime>> =
                const { std::cell::RefCell::new(None) };
        }

        BLOCKING_RUNTIME.with(|rt| {
            let mut rt = rt.borrow_mut();
            if rt.is_none() {
                *rt = Some(tokio::runtime::Runtime::new().unwrap());
            }
            rt.as_ref().unwrap().block_on(async {
                match tokio::time::timeout(timeout, future).await {
                    Ok(result) => result,
                    Err(_) => Err(powerfs_net::NetError::Timeout),
                }
            })
        })
    }

    fn block_with_default_timeout<F, T>(&self, future: F) -> NetResult<T>
    where
        F: std::future::Future<Output = NetResult<T>>,
    {
        self.block_with_timeout(future, self.config.request_timeout)
    }

    // ========================================================================
    // Volume operations (sync)
    // ========================================================================

    /// Write data to Volume (sync)
    pub fn write_data(
        &self,
        volume_addr: &str,
        volume_port: u16,
        volume_id: u32,
        file_key: u64,
        data: Vec<u8>,
    ) -> NetResult<()> {
        self.block_with_default_timeout(self.client.write_data(
            volume_addr,
            volume_port,
            volume_id,
            file_key,
            data,
        ))
    }

    /// Read data from Volume (sync)
    pub fn read_data(
        &self,
        volume_addr: &str,
        volume_port: u16,
        volume_id: u32,
        file_key: u64,
    ) -> NetResult<Vec<u8>> {
        self.block_with_default_timeout(self.client.read_data(
            volume_addr,
            volume_port,
            volume_id,
            file_key,
        ))
    }

    // ========================================================================
    // Metadata operations (sync)
    // ========================================================================

    /// Lookup a directory entry (sync)
    pub fn lookup(&self, parent_ino: u64, name: &str) -> NetResult<EntryInfo> {
        self.block_with_default_timeout(self.client.lookup(parent_ino, name))
    }

    /// Create a file (sync)
    pub fn create_file(
        &self,
        parent_ino: u64,
        name: &str,
        mode: u64,
        uid: u64,
        gid: u64,
    ) -> NetResult<u64> {
        self.block_with_default_timeout(self.client.create_file(parent_ino, name, mode, uid, gid))
    }

    /// Create a directory (sync)
    pub fn mkdir(
        &self,
        parent_ino: u64,
        name: &str,
        mode: u64,
        uid: u64,
        gid: u64,
    ) -> NetResult<u64> {
        self.block_with_default_timeout(self.client.mkdir(parent_ino, name, mode, uid, gid))
    }

    /// Delete a file (sync)
    pub fn unlink(&self, parent_ino: u64, name: &str) -> NetResult<()> {
        self.block_with_default_timeout(self.client.unlink(parent_ino, name))
    }

    /// Delete a directory (sync)
    pub fn rmdir(&self, parent_ino: u64, name: &str) -> NetResult<()> {
        self.block_with_default_timeout(self.client.rmdir(parent_ino, name))
    }

    /// List directory entries (sync)
    pub fn readdir(
        &self,
        parent_ino: u64,
        limit: u64,
        last_name: &str,
    ) -> NetResult<Vec<EntryInfo>> {
        self.block_with_default_timeout(self.client.readdir(parent_ino, limit, last_name))
    }

    /// Set attributes (sync)
    pub fn setattr(
        &self,
        ino: u64,
        size: Option<u64>,
        mode: Option<u64>,
        uid: Option<u64>,
        gid: Option<u64>,
    ) -> NetResult<()> {
        self.block_with_default_timeout(self.client.setattr(ino, size, mode, uid, gid))
    }

    // ========================================================================
    // Lease operations (sync)
    // ========================================================================

    /// Acquire a range lease (sync)
    #[allow(clippy::too_many_arguments)]
    pub fn acquire_range_lease(
        &self,
        volume_addr: &str,
        volume_port: u16,
        inode: u64,
        stripe_start: u64,
        stripe_count: u64,
        client_id: &str,
        exclusive: bool,
        duration_ms: u64,
    ) -> NetResult<(String, u64)> {
        self.block_with_default_timeout(self.client.acquire_range_lease(
            volume_addr,
            volume_port,
            inode,
            stripe_start,
            stripe_count,
            client_id,
            exclusive,
            duration_ms,
        ))
    }

    // ========================================================================
    // Utility methods
    // ========================================================================

    /// Check if connected to master
    pub fn is_connected(&self) -> bool {
        self.client.is_connected()
    }

    /// Get master address
    pub fn master_addr(&self) -> &str {
        &self.config.master_addr
    }

    /// Get master net port
    pub fn master_net_port(&self) -> u16 {
        self.config.master_net_port
    }

    /// Get volume net port
    pub fn volume_net_port(&self) -> u16 {
        self.config.volume_net_port
    }

    // ========================================================================
    // SyncFuseClient-compatible API
    // ========================================================================

    pub fn assign_fid(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<AssignFidResult, String> {
        let result = self
            .block_with_default_timeout(self.client.assign_volume(collection, replication))
            .map_err(|e| format!("net assign_volume failed: {}", e))?;

        let fid = Fid::from_string(&result.fid).unwrap_or(Fid {
            volume_id: VolumeId(0),
            cookie: 0,
            file_key: 0,
        });

        let location = if result.location_url.is_empty() {
            None
        } else {
            Some(Location {
                url: result.location_url.clone(),
                public_url: result.location_url.clone(),
                grpc_port: self.config.volume_net_port as u32,
                data_center: String::new(),
            })
        };

        Ok((fid, location, Vec::new(), Vec::new()))
    }

    pub fn lookup_volume(&self, volume_id: VolumeId) -> Result<Vec<Location>, String> {
        let locations = self
            .block_with_default_timeout(self.client.lookup_volume(volume_id.0))
            .map_err(|e| format!("net lookup_volume failed: {}", e))?;

        Ok(locations
            .into_iter()
            .map(volume_location_to_location)
            .collect())
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
        _cookie: u32,
    ) -> Result<(), String> {
        let port = extract_port_from_url(volume_addr).unwrap_or(self.config.volume_net_port);
        let addr = extract_addr_from_url(volume_addr);

        self.block_with_default_timeout(self.client.write_blob(
            &addr,
            port,
            volume_id,
            file_key,
            offset,
            size as u64,
            data,
        ))
        .map_err(|e| format!("net write_blob failed: {}", e))
    }

    pub fn read_blob(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: i32,
    ) -> Result<Vec<u8>, String> {
        let port = extract_port_from_url(volume_addr).unwrap_or(self.config.volume_net_port);
        let addr = extract_addr_from_url(volume_addr);

        self.block_with_default_timeout(self.client.read_blob(
            &addr,
            port,
            volume_id,
            file_key,
            offset,
            size as u64,
        ))
        .map_err(|e| format!("net read_blob failed: {}", e))
    }

    pub fn delete_data(
        &self,
        volume_addr: &str,
        volume_id: u32,
        file_key: u64,
    ) -> Result<(), String> {
        let port = extract_port_from_url(volume_addr).unwrap_or(self.config.volume_net_port);
        let addr = extract_addr_from_url(volume_addr);

        self.block_with_default_timeout(self.client.delete_data(&addr, port, volume_id, file_key))
            .map_err(|e| format!("net delete_data failed: {}", e))
    }

    pub fn get_entry(&self, path: &str) -> Result<Option<Entry>, String> {
        let (parent_ino, name) = parse_path_to_parent_name(path);
        match self.block_with_default_timeout(self.client.lookup(parent_ino, name)) {
            Ok(info) => Ok(Some(entry_info_to_entry(&info, path))),
            Err(_) => Ok(None),
        }
    }

    pub fn get_entry_by_inode(&self, inode: u64) -> Result<Option<(Entry, String)>, String> {
        let info = self
            .block_with_default_timeout(self.client.lookup(inode, ""))
            .map_err(|e| format!("net get_entry_by_inode failed: {}", e))?;
        let path = format!("/{}", info.name);
        Ok(Some((entry_info_to_entry(&info, &path), path)))
    }

    pub fn create_entry(&self, entry: Entry, _client_id: &str) -> Result<u64, String> {
        let name = entry.name.clone();
        let parent_path = entry.directory.clone();
        let parent_ino = if parent_path.is_empty() || parent_path == "/" {
            1
        } else {
            self.block_with_default_timeout(self.client.lookup(1, &parent_path))
                .map_err(|e| format!("net lookup parent failed: {}", e))?
                .ino
        };

        let mode = entry
            .attributes
            .as_ref()
            .map(|a| a.mode as u64)
            .unwrap_or(0o644);
        let uid = entry.attributes.as_ref().map(|a| a.uid as u64).unwrap_or(0);
        let gid = entry.attributes.as_ref().map(|a| a.gid as u64).unwrap_or(0);
        let is_dir = (mode & 0o170000) == 0o040000;

        let ino = if is_dir {
            self.block_with_default_timeout(self.client.mkdir(parent_ino, &name, mode, uid, gid))
                .map_err(|e| format!("net mkdir failed: {}", e))?
        } else {
            self.block_with_default_timeout(
                self.client.create_file(parent_ino, &name, mode, uid, gid),
            )
            .map_err(|e| format!("net create failed: {}", e))?
        };

        Ok(ino)
    }

    pub fn update_entry(
        &self,
        entry: &Entry,
        _client_id: &str,
        _expected_size: u64,
        _is_truncate: bool,
    ) -> Result<u64, String> {
        let ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
        let mode = entry.attributes.as_ref().map(|a| a.mode as u64);
        let size = entry.attributes.as_ref().map(|a| a.size);
        let uid = entry.attributes.as_ref().map(|a| a.uid as u64);
        let gid = entry.attributes.as_ref().map(|a| a.gid as u64);

        self.block_with_default_timeout(self.client.setattr(ino, size, mode, uid, gid))
            .map_err(|e| format!("net setattr failed: {}", e))?;

        Ok(ino)
    }

    pub fn delete_entry(
        &self,
        ino: u64,
        is_directory: bool,
        _client_id: &str,
    ) -> Result<bool, String> {
        let info = self
            .block_with_default_timeout(self.client.lookup(ino, ""))
            .map_err(|e| format!("net lookup for delete failed: {}", e))?;

        if info.name.is_empty() {
            return Err("cannot delete: empty name".into());
        }

        let parent_ino = 1; // Default to root
        if is_directory {
            self.block_with_default_timeout(self.client.rmdir(parent_ino, &info.name))
                .map_err(|e| format!("net rmdir failed: {}", e))?;
        } else {
            self.block_with_default_timeout(self.client.unlink(parent_ino, &info.name))
                .map_err(|e| format!("net unlink failed: {}", e))?;
        }

        Ok(true)
    }

    pub fn list_entries(
        &self,
        parent_ino: u64,
        limit: u64,
        start_after: &str,
    ) -> Result<Vec<Entry>, String> {
        let entries = self
            .block_with_default_timeout(self.client.readdir(parent_ino, limit, start_after))
            .map_err(|e| format!("net readdir failed: {}", e))?;

        Ok(entries
            .into_iter()
            .map(|info| entry_info_to_entry(&info, &info.name))
            .collect())
    }

    pub fn rename_entry(
        &self,
        old_parent_ino: u64,
        old_name: &str,
        new_parent_ino: u64,
        new_name: &str,
        _client_id: &str,
    ) -> Result<bool, String> {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ParentIno, old_parent_ino);
        enc.add_string(FieldId::Name, old_name)
            .map_err(|e| e.to_string())?;
        enc.add_u64(FieldId::NewParentIno, new_parent_ino);
        enc.add_string(FieldId::NewName, new_name)
            .map_err(|e| e.to_string())?;

        let body = enc.into_bytes();
        let resp = self
            .block_with_default_timeout(self.client.master_client.send_request(
                MsgType::Rename,
                &body,
                &[],
            ))
            .map_err(|e| format!("net rename failed: {}", e))?;

        Ok(resp.is_ok())
    }

    /// Convert a protobuf Location to a gRPC address string
    pub fn location_to_grpc_addr(loc: &Location) -> String {
        if loc.url.is_empty() {
            String::new()
        } else {
            loc.url.clone()
        }
    }
}

// ============================================================================
// Conversion utilities
// ============================================================================

/// Convert EntryInfo (net) to Entry (protobuf)
fn entry_info_to_entry(info: &EntryInfo, path: &str) -> Entry {
    Entry {
        name: info.name.clone(),
        directory: path.to_string(),
        attributes: Some(FuseAttributes {
            ino: info.ino,
            mode: info.mode,
            nlink: info.nlink,
            uid: info.uid,
            gid: info.gid,
            rdev: 0,
            size: info.size,
            blksize: 4096,
            blocks: info.size.div_ceil(512),
            atime: info.atime,
            mtime: info.mtime,
            ctime: info.ctime,
            crtime: info.ctime,
            perm: 0,
        }),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        extended: HashMap::new(),
        content_size: info.size,
        disk_size: info.size,
        ttl: String::new(),
        symlink_target: info.symlink_target.clone().unwrap_or_default(),
        owner: String::new(),
        generation: 0,
    }
}

/// Convert VolumeLocation (net) to Location (protobuf)
fn volume_location_to_location(loc: VolumeLocation) -> Location {
    Location {
        url: loc.url.clone(),
        public_url: loc.url,
        grpc_port: 0,
        data_center: loc.data_center,
    }
}

/// Parse a path into (parent_ino, name)
/// Root inode is 1. Top-level paths like "/foo" have parent_ino=1, name="foo".
fn parse_path_to_parent_name(path: &str) -> (u64, &str) {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return (1, "");
    }
    match trimmed.find('/') {
        Some(idx) => (1, &trimmed[..idx]),
        None => (1, trimmed),
    }
}

/// Extract address from URL (e.g. "http://127.0.0.1:9334" -> "127.0.0.1")
#[allow(clippy::double_ended_iterator_last)]
fn extract_addr_from_url(url: &str) -> String {
    url.split("://")
        .last()
        .and_then(|s| s.split(':').next())
        .unwrap_or(url)
        .to_string()
}

/// Extract port from URL (e.g. "http://127.0.0.1:9334" -> Some(9334))
#[allow(clippy::double_ended_iterator_last)]
fn extract_port_from_url(url: &str) -> Option<u16> {
    url.split(':').last().and_then(|s| s.parse::<u16>().ok())
}
