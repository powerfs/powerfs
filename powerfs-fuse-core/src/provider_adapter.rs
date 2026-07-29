use async_trait::async_trait;
use chrono::Utc;
use powerfs_common::{
    error::{PowerFsError, Result},
    traits::{
        Entry, EntryAttributes, Location, MetadataProvider, NodeStats, StorageProvider,
        VolumeFilters, VolumeProvider,
    },
    types::{Fid, NodeId, VolumeId, VolumeInfo},
};
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::FieldId;
use std::sync::Arc;
use std::time::Duration;

/// Helper: hex dump first N bytes for debugging
fn hex_dump(bytes: &[u8]) -> String {
    let n = bytes.len().min(128);
    bytes[..n]
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ")
}

// ============================================================================
// TLV encoding/decoding helpers for metadata operations
// ============================================================================

/// Encode a Lookup request body in TLV format
fn build_lookup_tlv(parent_ino: u64, name: &str) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    let _ = enc.add_u64(FieldId::ParentIno, parent_ino);
    let _ = enc.add_string(FieldId::Name, name);
    enc.into_bytes()
}

/// Encode a Create/Mkdir request body in TLV format
fn build_create_tlv(parent_ino: u64, name: &str, mode: u64, uid: u64, gid: u64) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    let _ = enc.add_u64(FieldId::ParentIno, parent_ino);
    let _ = enc.add_string(FieldId::Name, name);
    let _ = enc.add_u64(FieldId::Mode, mode);
    let _ = enc.add_u64(FieldId::Uid, uid);
    let _ = enc.add_u64(FieldId::Gid, gid);
    enc.into_bytes()
}

/// Encode a SetAttr request body in TLV format
fn build_setattr_tlv(
    ino: u64,
    size: u64,
    mode: Option<u64>,
    uid: Option<u64>,
    gid: Option<u64>,
) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    let _ = enc.add_u64(FieldId::Ino, ino);
    let _ = enc.add_u64(FieldId::Size, size);
    if let Some(m) = mode {
        let _ = enc.add_u64(FieldId::Mode, m);
    }
    if let Some(u) = uid {
        let _ = enc.add_u64(FieldId::Uid, u);
    }
    if let Some(g) = gid {
        let _ = enc.add_u64(FieldId::Gid, g);
    }
    enc.into_bytes()
}

/// Encode an Unlink/Rmdir request body in TLV format
fn build_metadata_delete_tlv(ino: u64, name: &str) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    let _ = enc.add_u64(FieldId::Ino, ino);
    let _ = enc.add_string(FieldId::Name, name);
    enc.into_bytes()
}

/// Encode a ReadDir request body in TLV format
fn build_readdir_tlv(parent_ino: u64, offset: u64) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    let _ = enc.add_u64(FieldId::ParentIno, parent_ino);
    let _ = enc.add_u64(FieldId::Offset, offset);
    enc.into_bytes()
}

/// Parse TLV response body into an Entry
fn parse_entry_from_tlv(data: &[u8], path: &str) -> Option<Entry> {
    let mut dec = TlvDecoder::new(data);
    let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);
    let mode = dec.next_u64(FieldId::Mode).unwrap_or(0o644) as u32;
    let uid = dec.next_u64(FieldId::Uid).unwrap_or(0) as u32;
    let gid = dec.next_u64(FieldId::Gid).unwrap_or(0) as u32;
    let size = dec.next_u64(FieldId::Size).unwrap_or(0);
    let _nlink = dec.next_u64(FieldId::Nlink).unwrap_or(1) as u32;
    let mtime = dec.next_u64(FieldId::Mtime).unwrap_or(0);
    let atime = dec.next_u64(FieldId::Atime).unwrap_or(0);
    let ctime = dec.next_u64(FieldId::Ctime).unwrap_or(0);
    let name = dec.next_string(FieldId::Name).unwrap_or_default();

    let entry_name = if name.is_empty() {
        let path_name = path.rsplit('/').next().unwrap_or(path);
        path_name.to_string()
    } else {
        name
    };

    let default_time = chrono::DateTime::<Utc>::from_timestamp(0, 0).unwrap_or_else(Utc::now);

    let attributes = EntryAttributes {
        ino,
        mode,
        uid,
        gid,
        atime: parse_unix_time(atime).unwrap_or(default_time),
        mtime: parse_unix_time(mtime).unwrap_or(default_time),
        ctime: parse_unix_time(ctime).unwrap_or(default_time),
        crtime: default_time,
    };

    Some(Entry {
        name: entry_name,
        directory: String::new(),
        attributes: Some(attributes),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        extended: std::collections::HashMap::new(),
        content_size: size,
        disk_size: size,
        ttl: String::new(),
        symlink_target: String::new(),
        owner: String::new(),
        generation: 0,
    })
}

/// Parse unix timestamp to chrono DateTime
fn parse_unix_time(secs: u64) -> Option<chrono::DateTime<Utc>> {
    if secs == 0 {
        return None;
    }
    chrono::DateTime::from_timestamp(secs as i64, 0)
}

/// Parse TLV response for create - returns ino
fn parse_create_response_tlv(data: &[u8]) -> u64 {
    let mut dec = TlvDecoder::new(data);
    dec.next_u64(FieldId::Ino).unwrap_or(0)
}

/// Parse TLV response for readdir - returns vector of Entries
fn parse_readdir_response_tlv(data: &[u8]) -> Vec<Entry> {
    let mut dec = TlvDecoder::new(data);
    let count = dec.next_u64(FieldId::Count).unwrap_or(0) as usize;
    let mut entries = Vec::new();

    for _i in 0..count {
        if let Ok(entry_data) = dec.next_bytes(FieldId::Entry) {
            if let Some(entry) = parse_entry_from_tlv(&entry_data, "") {
                entries.push(entry);
            }
        }
    }
    entries
}

// ============================================================================
// Helper functions
// ============================================================================

/// Parse a file path into (parent_ino, name)
/// Format: "/dir1/dir2/file" -> parent_ino from dir2 inode, name = "file"
fn parse_path_to_parent_name(path: &str) -> (u64, String) {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return (1, String::new()); // Root
    }

    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() == 1 {
        return (1, parts[0].to_string()); // Direct child of root
    }

    // For now, return root inode (1) and the last component
    // In a full implementation, we'd need to resolve parent inode
    let name = parts.last().unwrap_or(&"").to_string();
    (1, name)
}

#[allow(dead_code)]
/// Parse a volume URL to extract addr and port
/// URL format: "host:port" or just "host" (uses default port)
fn parse_volume_url(url: &str, default_port: u16) -> (String, u16) {
    if let Some((host, port_str)) = url.split_once(':') {
        let port = port_str.parse().unwrap_or(default_port);
        (host.to_string(), port)
    } else {
        (url.to_string(), default_port)
    }
}

// ============================================================================
// Facade-based providers (using FuseClientFacade)
// ============================================================================

/// 通用 JSON 反序列化响应
#[derive(serde::Deserialize, Debug)]
pub struct FacadeResponse {
    pub success: bool,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
    #[serde(default)]
    pub error: Option<String>,
}

/// 构建写操作的 TLV 请求体
fn build_write_tlv(
    volume_id: u32,
    file_key: u64,
    data: &[u8],
    lease_token: Option<&str>,
    client_id: Option<&str>,
) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, volume_id as u64);
    enc.add_u64(FieldId::Name, file_key);
    let _ = enc.add_bytes(FieldId::DataLen, data);
    if let Some(token) = lease_token {
        if !token.is_empty() {
            let _ = enc.add_string(FieldId::LeaseToken, token);
        }
    }
    if let Some(cid) = client_id {
        if !cid.is_empty() {
            let _ = enc.add_string(FieldId::ClientId, cid);
        }
    }
    enc.into_bytes()
}

/// 构建读操作的 TLV 请求体
fn build_read_tlv(volume_id: u32, file_key: u64, offset: i64, size: i32) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, volume_id as u64);
    enc.add_u64(FieldId::Name, file_key);
    enc.add_u64(FieldId::Offset, offset as u64);
    enc.add_u64(FieldId::Size, size as u64);
    enc.into_bytes()
}

/// 构建批量写操作的 TLV 请求体
fn build_batch_write_tlv(
    volume_id: u32,
    file_key: u64,
    entries_count: usize,
    data: &[u8],
    lease_token: Option<&str>,
    client_id: Option<&str>,
) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, volume_id as u64);
    enc.add_u64(FieldId::Name, file_key);
    enc.add_u64(FieldId::Entries, entries_count as u64);
    let _ = enc.add_bytes(FieldId::DataLen, data);
    if let Some(token) = lease_token {
        if !token.is_empty() {
            let _ = enc.add_string(FieldId::LeaseToken, token);
        }
    }
    if let Some(cid) = client_id {
        if !cid.is_empty() {
            let _ = enc.add_string(FieldId::ClientId, cid);
        }
    }
    enc.into_bytes()
}

/// 构建删除操作的 TLV 请求体
fn build_delete_tlv(volume_id: u32, file_key: u64) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, volume_id as u64);
    enc.add_u64(FieldId::Name, file_key);
    enc.into_bytes()
}

/// 从 RequestResult 解析 JSON 响应（用于 MetadataProvider/VolumeProvider）
pub fn parse_response_from_result(
    result: crate::meta_shard_client::RequestResult,
) -> std::result::Result<FacadeResponse, PowerFsError> {
    let data = result
        .data
        .ok_or_else(|| PowerFsError::Internal("No response data".to_string()))?;
    serde_json::from_slice(&data)
        .map_err(|e| PowerFsError::Internal(format!("Failed to parse response: {}", e)))
}

/// 基于 FuseClientFacade 的 VolumeProvider 实现
pub struct FacadeVolumeProvider {
    facade: Arc<crate::fuse_client_facade::FuseClientFacade>,
}

impl FacadeVolumeProvider {
    pub fn new(facade: Arc<crate::fuse_client_facade::FuseClientFacade>) -> Self {
        Self { facade }
    }
}

#[async_trait]
impl VolumeProvider for FacadeVolumeProvider {
    async fn assign_volume(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<(Fid, Vec<Location>)> {
        // Route to Master via MsgType::Assign (Master allocates real volume_ids via Raft)
        let mut enc = powerfs_net::TlvEncoder::new();
        let _ = enc.add_string(powerfs_net::FieldId::Name, collection);
        let _ = enc.add_string(powerfs_net::FieldId::Backend, replication);
        let payload = enc.into_bytes();

        let resp = self
            .facade
            .submit_master_request(powerfs_net::MsgType::Assign, payload)
            .await
            .map_err(|e| PowerFsError::Internal(format!("Master assign_volume failed: {}", e)))?;

        if !resp.is_ok() {
            return Err(PowerFsError::Internal(format!(
                "Master assign_volume returned status: {}",
                resp.header.status
            )));
        }

        // Parse TLV response: VolumeId, Cookie, FileKey, Owner (volume server URL)
        let body = if !resp.body.is_empty() {
            &resp.body
        } else {
            &resp.data
        };
        log::debug!(
            "assign_volume: parsing {} bytes of Master TLV response (hex): {}",
            body.len(),
            hex_dump(body)
        );
        let mut dec = powerfs_net::TlvDecoder::new(body);
        let volume_id = dec.next_u64(powerfs_net::FieldId::VolumeId).unwrap_or(0) as u32;
        let cookie = dec.next_u64(powerfs_net::FieldId::Cookie).unwrap_or(0);
        let file_key = dec.next_u64(powerfs_net::FieldId::FileKey).unwrap_or(0);
        let owner_url = dec
            .next_string(powerfs_net::FieldId::Owner)
            .unwrap_or_default();

        log::info!(
            "assign_volume: Master assigned volume_id={}, cookie={}, file_key={}, url={}",
            volume_id,
            cookie,
            file_key,
            owner_url
        );

        let fid = Fid {
            volume_id: VolumeId(volume_id),
            cookie,
            file_key,
        };

        // Use the volume server URL from Master's response
        let locations = if !owner_url.is_empty() {
            vec![Location {
                url: owner_url.clone(),
                public_url: owner_url,
                grpc_port: 0,
                data_center: String::new(),
            }]
        } else {
            vec![Location {
                url: self.facade.net_client().filer_leader_addr(),
                public_url: String::new(),
                grpc_port: 0,
                data_center: String::new(),
            }]
        };

        Ok((fid, locations))
    }

    async fn lookup_volume(&self, volume_id: VolumeId) -> Result<Vec<Location>> {
        // Route to Master via MsgType::LookupVolume (Master knows volume → server mapping)
        let mut enc = powerfs_net::TlvEncoder::new();
        let _ = enc.add_string(powerfs_net::FieldId::Name, &volume_id.0.to_string());
        let payload = enc.into_bytes();

        let resp = self
            .facade
            .submit_master_request(powerfs_net::MsgType::LookupVolume, payload)
            .await
            .map_err(|e| PowerFsError::Internal(format!("Master lookup_volume failed: {}", e)))?;

        if !resp.is_ok() {
            return Err(PowerFsError::Internal(format!(
                "Volume {} not found (Master status: {})",
                volume_id.0, resp.header.status
            )));
        }

        // Parse TLV response: Limit(count), Owner(url), Backend(dc)
        let mut dec = powerfs_net::TlvDecoder::new(&resp.body);
        let _count = dec.next_u64(powerfs_net::FieldId::Limit).unwrap_or(0);
        let url = dec
            .next_string(powerfs_net::FieldId::Owner)
            .unwrap_or_default();

        if url.is_empty() {
            return Ok(Vec::new());
        }

        log::debug!(
            "lookup_volume: Master returned url={} for volume_id={}",
            url,
            volume_id.0
        );
        Ok(vec![Location {
            public_url: url.clone(),
            url,
            grpc_port: 0,
            data_center: String::new(),
        }])
    }

    async fn heartbeat(&self, _node_id: &NodeId, _stats: &NodeStats) -> Result<()> {
        Ok(())
    }

    async fn list_volumes(&self, _filters: &VolumeFilters) -> Result<Vec<VolumeInfo>> {
        Ok(Vec::new())
    }
}

/// 基于 FuseClientFacade 的 MetadataProvider 实现
pub struct FacadeMetadataProvider {
    facade: Arc<crate::fuse_client_facade::FuseClientFacade>,
}

impl FacadeMetadataProvider {
    pub fn new(facade: Arc<crate::fuse_client_facade::FuseClientFacade>) -> Self {
        Self { facade }
    }
}

#[async_trait]
impl MetadataProvider for FacadeMetadataProvider {
    async fn get_entry(&self, path: &str) -> Result<Option<Entry>> {
        let (parent_ino, name) = parse_path_to_parent_name(path);

        let payload = build_lookup_tlv(parent_ino, &name);

        let result = self
            .facade
            .submit_metadata_request_with_type(
                crate::request_state::RequestKind::Metadata,
                parent_ino,
                payload,
                powerfs_net::MsgType::Lookup,
            )
            .await
            .map_err(|e| PowerFsError::Internal(format!("Facade get_entry failed: {}", e)))?;

        // TLV response is in payload field (not data field which is for JSON)
        let response_data = result
            .payload
            .as_deref()
            .filter(|d| !d.is_empty())
            .or(result.data.as_deref());

        match response_data {
            Some(data) if !data.is_empty() => Ok(parse_entry_from_tlv(data, path)),
            _ => {
                // Check if error
                if let Some(data) = &result.data {
                    if let Ok(facade_resp) = serde_json::from_slice::<FacadeResponse>(data) {
                        if !facade_resp.success {
                            return Ok(None);
                        }
                    }
                }
                Ok(None)
            }
        }
    }

    async fn get_entry_by_inode(&self, _inode: u64) -> Result<Option<(Entry, String)>> {
        // 需要服务端支持 inode -> path 映射
        Ok(None)
    }

    async fn create_entry(&self, entry: &Entry, _client_id: &str) -> Result<u64> {
        let parent_ino = 1;
        let name = entry.name.clone();
        let mode = entry
            .attributes
            .as_ref()
            .map(|a| a.mode as u64)
            .unwrap_or(0o644);
        let uid = entry.attributes.as_ref().map(|a| a.uid as u64).unwrap_or(0);
        let gid = entry.attributes.as_ref().map(|a| a.gid as u64).unwrap_or(0);
        let is_dir = mode & 0o170000 == 0o040000;

        let payload = build_create_tlv(parent_ino, &name, mode, uid, gid);

        let msg_type = if is_dir {
            powerfs_net::MsgType::Mkdir
        } else {
            powerfs_net::MsgType::Create
        };

        let result = self
            .facade
            .submit_metadata_request_with_type(
                crate::request_state::RequestKind::Metadata,
                parent_ino,
                payload,
                msg_type,
            )
            .await
            .map_err(|e| PowerFsError::Internal(format!("Facade create_entry failed: {}", e)))?;

        // Parse TLV response to get ino
        log::debug!(
            "create_entry: result.payload={:?}, result.data={:?}",
            result.payload.as_ref().map(|v| v.len()),
            result.data.as_ref().map(|v| v.len())
        );
        // Prefer payload if non-empty, otherwise use data
        let response_data = result
            .payload
            .as_deref()
            .filter(|d| !d.is_empty())
            .or(result.data.as_deref());

        match response_data {
            Some(data) if !data.is_empty() => {
                log::debug!("create_entry: parsing {} bytes of TLV response", data.len());
                Ok(parse_create_response_tlv(data))
            }
            _ => Err(PowerFsError::Internal(
                "Create returned empty response".to_string(),
            )),
        }
    }

    async fn update_entry(
        &self,
        entry: &Entry,
        _client_id: &str,
        _old_size: u64,
        _is_truncate: bool,
    ) -> Result<u64> {
        let ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
        let new_size = entry.content_size;
        let mode = entry.attributes.as_ref().map(|a| a.mode as u64);
        let uid = entry.attributes.as_ref().map(|a| a.uid as u64);
        let gid = entry.attributes.as_ref().map(|a| a.gid as u64);

        let payload = build_setattr_tlv(ino, new_size, mode, uid, gid);

        let result = self
            .facade
            .submit_metadata_request_with_type(
                crate::request_state::RequestKind::Metadata,
                ino,
                payload,
                powerfs_net::MsgType::SetAttr,
            )
            .await
            .map_err(|e| PowerFsError::Internal(format!("Facade update_entry failed: {}", e)))?;

        // If we got a response (payload or data), the operation succeeded
        if result.payload.is_some() || result.data.is_some() {
            Ok(new_size)
        } else {
            Err(PowerFsError::Internal(
                "SetAttr returned empty response".to_string(),
            ))
        }
    }

    async fn delete_entry(&self, inode: u64, is_dir: bool, _client_id: &str) -> Result<()> {
        let msg_type = if is_dir {
            powerfs_net::MsgType::Rmdir
        } else {
            powerfs_net::MsgType::Unlink
        };

        let name = String::new();
        let payload = build_metadata_delete_tlv(inode, &name);

        let result = self
            .facade
            .submit_metadata_request_with_type(
                crate::request_state::RequestKind::Metadata,
                inode,
                payload,
                msg_type,
            )
            .await
            .map_err(|e| PowerFsError::Internal(format!("Facade delete_entry failed: {}", e)))?;

        // Check for error in response
        if let Some(data) = &result.data {
            if let Ok(facade_resp) = serde_json::from_slice::<FacadeResponse>(data) {
                if !facade_resp.success {
                    return Err(PowerFsError::Internal(
                        facade_resp
                            .error
                            .unwrap_or_else(|| "Delete failed".to_string()),
                    ));
                }
            }
        }

        Ok(())
    }

    async fn list_entries(&self, inode: u64, _limit: u32, _client_id: &str) -> Result<Vec<Entry>> {
        let payload = build_readdir_tlv(inode, 0);

        let result = self
            .facade
            .submit_metadata_request_with_type(
                crate::request_state::RequestKind::Metadata,
                inode,
                payload,
                powerfs_net::MsgType::ReadDir,
            )
            .await
            .map_err(|e| PowerFsError::Internal(format!("Facade list_entries failed: {}", e)))?;

        let response_data = result
            .payload
            .as_deref()
            .filter(|d| !d.is_empty())
            .or(result.data.as_deref());

        match response_data {
            Some(data) if !data.is_empty() => Ok(parse_readdir_response_tlv(data)),
            _ => Ok(Vec::new()),
        }
    }
}

/// 基于 FuseClientFacade 的 StorageProvider 实现
pub struct FacadeStorageProvider {
    facade: Arc<crate::fuse_client_facade::FuseClientFacade>,
}

impl FacadeStorageProvider {
    pub fn new(facade: Arc<crate::fuse_client_facade::FuseClientFacade>) -> Self {
        Self { facade }
    }
}

/// 构建 RangeLease 请求 TLV
fn build_range_lease_tlv(
    file_key: u64,
    stripe_start: u64,
    stripe_count: u64,
    client_id: &str,
    exclusive: bool,
    duration_ms: u64,
) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    let _ = enc.add_u64(FieldId::Ino, file_key);
    let _ = enc.add_u64(FieldId::Offset, stripe_start);
    let _ = enc.add_u64(FieldId::Limit, stripe_count);
    if !client_id.is_empty() {
        let _ = enc.add_string(FieldId::ClientId, client_id);
    }
    let _ = enc.add_u64(FieldId::Mode, if exclusive { 1 } else { 0 });
    let _ = enc.add_u64(FieldId::LeaseDuration, duration_ms);
    enc.into_bytes()
}

/// 解析 RangeLease 响应 TLV → (lease_token, epoch)
fn parse_range_lease_response(payload: &[u8]) -> Option<(String, u64)> {
    let mut dec = TlvDecoder::new(payload);
    let token = dec.next_string(FieldId::LeaseId).ok()?;
    let epoch = dec.next_u64(FieldId::LeaseEpoch).unwrap_or(0);
    Some((token, epoch))
}

impl FacadeStorageProvider {
    /// 获取/续期指定 (volume, file_key) 的有效 lease 并返回 token。
    /// 优先读缓存；若缓存未命中或已过期，向 Volume 发起 RangeLease 请求并更新缓存。
    async fn ensure_lease(
        &self,
        volume_id: u32,
        file_key: u64,
    ) -> powerfs_common::error::Result<String> {
        let vid = volume_id as u64;
        // Fast path: use cached valid lease token
        if let Some(tok) = self
            .facade
            .volume_client()
            .get_valid_lease_token(vid, file_key)
        {
            return Ok(tok);
        }

        let client_id = self.facade.client_id();
        let duration_ms = 60_000; // 1 min default exclusive lease
        let payload = build_range_lease_tlv(file_key, 0, 1, &client_id, true, duration_ms);

        log::debug!(
            "ensure_lease: acquiring for volume={} file_key={} client={}",
            volume_id,
            file_key,
            client_id
        );

        let result = self
            .facade
            .submit_lease_request(vid, payload)
            .await
            .map_err(|e| PowerFsError::Internal(format!("Lease request failed: {}", e)))?;

        // Lease response TLV can be in either data or payload field depending on handler
        let resp_payload = result
            .data
            .clone()
            .or_else(|| result.payload.clone())
            .ok_or_else(|| PowerFsError::Internal("Lease response has empty body".into()))?;

        log::debug!(
            "ensure_lease: lease resp bytes={}, data={:?}, payload={:?}",
            resp_payload.len(),
            result.data.as_ref().map(Vec::len),
            result.payload.as_ref().map(Vec::len)
        );

        let (lease_token, _epoch) = parse_range_lease_response(&resp_payload).ok_or_else(|| {
            PowerFsError::Internal(format!(
                "Failed to parse RangeLease response ({} bytes)",
                resp_payload.len()
            ))
        })?;

        // Store in VolumeClient lease cache
        self.facade.volume_client().update_lease(
            vid,
            file_key,
            lease_token.clone(),
            Duration::from_millis(duration_ms),
        );

        log::debug!(
            "ensure_lease: acquired token={:.16}... for volume={} file_key={}",
            lease_token,
            volume_id,
            file_key
        );

        Ok(lease_token)
    }
}

#[async_trait]
impl StorageProvider for FacadeStorageProvider {
    async fn write_blob(
        &self,
        volume_id: u32,
        file_key: u64,
        _offset: i64,
        _size: i32,
        data: &[u8],
    ) -> Result<()> {
        // Acquire a valid lease BEFORE submitting the write
        let lease_token = self
            .ensure_lease(volume_id, file_key)
            .await
            .map_err(|e| PowerFsError::Internal(format!("Failed to acquire lease: {}", e)))?;
        let client_id = self.facade.client_id();

        let payload = build_write_tlv(
            volume_id,
            file_key,
            data,
            Some(&lease_token),
            Some(&client_id),
        );

        let _result = self
            .facade
            .submit_data_request_with_type(
                crate::request_state::RequestKind::Write,
                volume_id as u64,
                payload,
                powerfs_net::MsgType::WriteNeedle,
            )
            .await
            .map_err(|e| PowerFsError::Internal(format!("Facade write_blob failed: {}", e)))?;

        Ok(())
    }

    async fn batch_write_blob(
        &self,
        volume_id: u32,
        file_key: u64,
        entries: &[(i64, i32, Vec<u8>, u32)],
    ) -> Result<()> {
        let mut combined_data = Vec::new();
        for (_offset, _size, data, _cookie) in entries {
            combined_data.extend_from_slice(data);
        }

        // Acquire a valid lease BEFORE submitting the write
        let lease_token = self
            .ensure_lease(volume_id, file_key)
            .await
            .map_err(|e| PowerFsError::Internal(format!("Failed to acquire lease: {}", e)))?;
        let client_id = self.facade.client_id();

        let payload = build_batch_write_tlv(
            volume_id,
            file_key,
            entries.len(),
            &combined_data,
            Some(&lease_token),
            Some(&client_id),
        );

        let _result = self
            .facade
            .submit_data_request_with_type(
                crate::request_state::RequestKind::Write,
                volume_id as u64,
                payload,
                powerfs_net::MsgType::BatchWriteNeedle,
            )
            .await
            .map_err(|e| {
                PowerFsError::Internal(format!("Facade batch_write_blob failed: {}", e))
            })?;

        Ok(())
    }

    async fn read_blob(
        &self,
        volume_id: u32,
        file_key: u64,
        offset: i64,
        size: i32,
    ) -> Result<Vec<u8>> {
        let payload = build_read_tlv(volume_id, file_key, offset, size);

        let result = self
            .facade
            .submit_data_request_with_type(
                crate::request_state::RequestKind::Read,
                volume_id as u64,
                payload,
                powerfs_net::MsgType::ReadNeedleBlob,
            )
            .await
            .map_err(|e| PowerFsError::Internal(format!("Facade read_blob failed: {}", e)))?;

        let data = result.payload.unwrap_or_default();
        Ok(data)
    }

    async fn delete_blob(&self, volume_id: u32, file_key: u64) -> Result<()> {
        let payload = build_delete_tlv(volume_id, file_key);

        let _result = self
            .facade
            .submit_mgmt_request_with_type(
                volume_id as u64,
                payload,
                powerfs_net::MsgType::DeleteNeedle,
            )
            .await
            .map_err(|e| PowerFsError::Internal(format!("Facade delete_blob failed: {}", e)))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_response_success() {
        let json = r#"{"success": true, "data": {"value": 42}}"#;
        let result: FacadeResponse = serde_json::from_str(json).unwrap();
        assert!(result.success);
        assert!(result.data.is_some());
        assert!(result.error.is_none());
    }

    #[test]
    fn test_parse_response_failure() {
        let json = r#"{"success": false, "error": "Something went wrong"}"#;
        let result: FacadeResponse = serde_json::from_str(json).unwrap();
        assert!(!result.success);
        assert_eq!(result.error, Some("Something went wrong".to_string()));
    }

    #[test]
    fn test_parse_response_without_data() {
        let json = r#"{"success": true}"#;
        let result: FacadeResponse = serde_json::from_str(json).unwrap();
        assert!(result.success);
        assert!(result.data.is_none());
    }

    #[test]
    fn test_parse_path_to_parent_name() {
        let (parent, name) = parse_path_to_parent_name("/");
        assert_eq!(parent, 1);
        assert_eq!(name, "");

        let (parent, name) = parse_path_to_parent_name("/file.txt");
        assert_eq!(parent, 1);
        assert_eq!(name, "file.txt");

        let (parent, name) = parse_path_to_parent_name("/dir/file.txt");
        assert_eq!(parent, 1); // simplified implementation
        assert_eq!(name, "file.txt");

        let (parent, name) = parse_path_to_parent_name("no_slash");
        assert_eq!(parent, 1);
        assert_eq!(name, "no_slash");
    }

    #[test]
    fn test_parse_volume_url() {
        let (host, port) = parse_volume_url("host:1234", 8080);
        assert_eq!(host, "host");
        assert_eq!(port, 1234);

        let (host, port) = parse_volume_url("host", 8080);
        assert_eq!(host, "host");
        assert_eq!(port, 8080);

        let (host, port) = parse_volume_url("host:invalid", 9999);
        assert_eq!(host, "host");
        assert_eq!(port, 9999); // fallback to default
    }
}
