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
use powerfs_layout::codec::{decode_file_layout, encode_file_layout, FEATURE_CHUNK_LAYOUT_V2};
use powerfs_layout::encoding::ChunkEncoding;
use powerfs_layout::layout::FileLayout;
use powerfs_layout::placement::Placement;
use powerfs_layout::reliability::{CompressionState, Reliability, ReliabilityState};
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::FieldId;
use std::sync::Arc;
use std::time::Duration;

/// P1-3: Lease proactive renew threshold.
///
/// When a cached lease's remaining time drops below this threshold,
/// `ensure_lease` proactively renews it before returning the token to the
/// caller. This prevents lease expiry mid-write for long operations.
///
/// Set to 10s: with a 60s default lease duration, this gives the renew
/// RPC ample time to complete while leaving 50s of "fresh" validity.
const RENEW_THRESHOLD: Duration = Duration::from_secs(10);

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

/// Encode a GetAttr request body in TLV format
fn build_getattr_tlv(ino: u64) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    let _ = enc.add_u64(FieldId::Ino, ino);
    enc.into_bytes()
}

/// Encode a Create/Mkdir request body in TLV format
#[allow(dead_code)]
fn build_create_tlv(parent_ino: u64, name: &str, mode: u64, uid: u64, gid: u64) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    let _ = enc.add_u64(FieldId::ParentIno, parent_ino);
    let _ = enc.add_string(FieldId::Name, name);
    let _ = enc.add_u64(FieldId::Mode, mode);
    let _ = enc.add_u64(FieldId::Uid, uid);
    let _ = enc.add_u64(FieldId::Gid, gid);
    enc.into_bytes()
}

/// Encode a Create/Mkdir request body in TLV format, including chunk/fid info
fn build_create_tlv_with_chunks(
    parent_ino: u64,
    name: &str,
    mode: u64,
    uid: u64,
    gid: u64,
    chunks: &[powerfs_common::traits::FileChunk],
) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    let _ = enc.add_u64(FieldId::ParentIno, parent_ino);
    let _ = enc.add_string(FieldId::Name, name);
    let _ = enc.add_u64(FieldId::Mode, mode);
    let _ = enc.add_u64(FieldId::Uid, uid);
    let _ = enc.add_u64(FieldId::Gid, gid);

    // Encode chunk info as binary FileLayout TLV (replaces JSON Chunks).
    if !chunks.is_empty() {
        let chunk_refs: Vec<powerfs_layout::encoding::ChunkRef> = chunks
            .iter()
            .map(|c| powerfs_layout::encoding::ChunkRef {
                offset: c.offset,
                size: c.size,
                needle_id: c.needle_id,
                volume_id: c.volume_id,
                crc32: c.crc32,
                mtime: c.mtime,
            })
            .collect();
        let layout = FileLayout {
            placement: Placement::Flat,
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::default(),
            compression: CompressionState::default(),
            encoding: ChunkEncoding::PerChunk { chunks: chunk_refs },
        };
        let _ = encode_file_layout(&mut enc, &layout, FEATURE_CHUNK_LAYOUT_V2);
    }

    enc.into_bytes()
}

/// Encode a SetAttr request body in TLV format (legacy unified path, kept for backward compatibility)
#[allow(dead_code)]
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

/// Encode a SetAttrData request body in TLV format (strong consistency path for size/chunks)
fn build_setattr_data_tlv(ino: u64, size: u64) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    let _ = enc.add_u64(FieldId::Ino, ino);
    let _ = enc.add_u64(FieldId::Size, size);
    enc.into_bytes()
}

/// Encode a SetAttrMeta request body in TLV format (eventual consistency path for mode/uid/gid/timestamps)
#[allow(clippy::too_many_arguments)]
fn build_setattr_meta_tlv(
    ino: u64,
    mode: Option<u64>,
    uid: Option<u64>,
    gid: Option<u64>,
    mtime: Option<u64>,
    atime: Option<u64>,
    client_id: &str,
    timestamp: u64,
) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    let _ = enc.add_u64(FieldId::Ino, ino);
    if let Some(m) = mode {
        let _ = enc.add_u64(FieldId::Mode, m);
    }
    if let Some(u) = uid {
        let _ = enc.add_u64(FieldId::Uid, u);
    }
    if let Some(g) = gid {
        let _ = enc.add_u64(FieldId::Gid, g);
    }
    if let Some(mt) = mtime {
        let _ = enc.add_u64(FieldId::Mtime, mt);
    }
    if let Some(at) = atime {
        let _ = enc.add_u64(FieldId::Atime, at);
    }
    let _ = enc.add_string(FieldId::ClientId, client_id);
    let _ = enc.add_u64(FieldId::Seq, timestamp);
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
    let mode = dec.next_u32(FieldId::Mode).unwrap_or(0o644);
    let uid = dec.next_u32(FieldId::Uid).unwrap_or(0);
    let gid = dec.next_u32(FieldId::Gid).unwrap_or(0);
    let size = dec.next_u64(FieldId::Size).unwrap_or(0);
    let _nlink = dec.next_u32(FieldId::Nlink).unwrap_or(1);
    let mtime = dec.next_u64(FieldId::Mtime).unwrap_or(0);
    let atime = dec.next_u64(FieldId::Atime).unwrap_or(0);
    let ctime = dec.next_u64(FieldId::Ctime).unwrap_or(0);
    let name = dec.next_string(FieldId::Name).unwrap_or_default();

    log::debug!(
        "parse_entry_from_tlv: path={}, ino={}, mode={:o}, name={}",
        path,
        ino,
        mode,
        name
    );

    // Decode FileLayout from remaining TLV fields (binary TLV, replaces JSON Chunks).
    // If no layout fields are present, decode_file_layout returns a default
    // FileLayout with empty ChunkEncoding::PerChunk.
    // P4: Save remaining bytes BEFORE decode_file_layout, because its while-loop
    // consumes ALL remaining fields (including ReplicaChunks via the _ => skip branch).
    let remaining_before_layout = dec.remaining_slice().to_vec();
    let layout = decode_file_layout(&mut dec).unwrap_or(FileLayout {
        placement: Placement::Flat,
        reliability: Reliability::SingleReplica,
        reliability_state: ReliabilityState::default(),
        compression: CompressionState::default(),
        encoding: ChunkEncoding::PerChunk { chunks: Vec::new() },
    });

    let chunks: Vec<powerfs_common::traits::FileChunk> = match &layout.encoding {
        ChunkEncoding::PerChunk { chunks } => chunks
            .iter()
            .map(|c| powerfs_common::traits::FileChunk {
                offset: c.offset,
                size: c.size,
                needle_id: c.needle_id,
                volume_id: c.volume_id,
                crc32: c.crc32,
                mtime: c.mtime,
            })
            .collect(),
        ChunkEncoding::Paginated { chunks, .. } => chunks
            .iter()
            .map(|c| powerfs_common::traits::FileChunk {
                offset: c.offset,
                size: c.size,
                needle_id: c.needle_id,
                volume_id: c.volume_id,
                crc32: c.crc32,
                mtime: c.mtime,
            })
            .collect(),
        // InlineData / StripeDescriptor: no per-chunk list to extract
        _ => Vec::new(),
    };

    // P4: 解析 FieldId::ReplicaChunks (副本 chunk 列表, 读路径 failover 使用).
    // 格式: [count u32 LE] [ChunkRef * count] (每个 44 字节).
    // Scan the saved bytes (before decode_file_layout consumed them) because
    // decode_file_layout's while-loop skips unknown fields including ReplicaChunks.
    let replica_chunks: Vec<powerfs_common::traits::FileChunk> =
        scan_tlv_for_field(&remaining_before_layout, powerfs_net::FieldId::ReplicaChunks)
            .map(|bytes| parse_replica_chunks(&bytes))
            .unwrap_or_default();

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
        chunks,
        replica_chunks,
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

/// P4: Scan raw TLV bytes for a specific field, returning its payload.
/// This is needed because TlvDecoder::next_field() skips unknown fields,
/// so decode_file_layout consumes ReplicaChunks before we can read it.
fn scan_tlv_for_field(buf: &[u8], target: powerfs_net::FieldId) -> Option<Vec<u8>> {
    let target_byte = target as u8;
    let mut pos = 0;
    while pos + 5 <= buf.len() {
        let field_id = buf[pos];
        let length = u32::from_be_bytes([
            buf[pos + 1],
            buf[pos + 2],
            buf[pos + 3],
            buf[pos + 4],
        ]) as usize;
        pos += 5;
        if pos + length > buf.len() {
            break; // malformed
        }
        if field_id == target_byte {
            return Some(buf[pos..pos + length].to_vec());
        }
        pos += length;
    }
    None
}

/// P4: 解析 FieldId::ReplicaChunks 的 payload.
/// 格式: [count u32 LE] [ChunkRef * count] (每个 44 字节, 与 codec 格式一致).
fn parse_replica_chunks(bytes: &[u8]) -> Vec<powerfs_common::traits::FileChunk> {
    if bytes.len() < 4 {
        return Vec::new();
    }
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap_or([0; 4])) as usize;
    const CHUNK_REF_SIZE: usize = 44;
    let needed = count * CHUNK_REF_SIZE;
    if bytes.len() < 4 + needed {
        log::warn!(
            "parse_replica_chunks: truncated, need {} bytes, have {}",
            4 + needed,
            bytes.len()
        );
        return Vec::new();
    }
    let mut chunks = Vec::with_capacity(count);
    for i in 0..count {
        let base = 4 + i * CHUNK_REF_SIZE;
        chunks.push(powerfs_common::traits::FileChunk {
            offset: u64::from_le_bytes(bytes[base..base + 8].try_into().unwrap()),
            size: u64::from_le_bytes(bytes[base + 8..base + 16].try_into().unwrap()),
            needle_id: u64::from_le_bytes(bytes[base + 16..base + 24].try_into().unwrap()),
            volume_id: u64::from_le_bytes(bytes[base + 24..base + 32].try_into().unwrap()),
            crc32: u32::from_le_bytes(bytes[base + 32..base + 36].try_into().unwrap()),
            mtime: u64::from_le_bytes(bytes[base + 36..base + 44].try_into().unwrap()),
        });
    }
    chunks
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
/// Note: This is a simplified helper; the full implementation uses iterative lookup
#[allow(dead_code)]
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
    volume_id: u64,
    file_key: u64,
    data: &[u8],
    lease_token: Option<&str>,
    client_id: Option<&str>,
) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, volume_id);
    enc.add_u64(FieldId::FileKey, file_key);
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

/// 构建写操作的 TLV 请求体（带 inode 用于 lease 校验）
///
/// P3 fix: chunk data is NOT included in the TLV body. It is sent in the
/// frame's DATA segment (up to 2MB) via `send_write_needle_direct(data)`.
/// The server's R5 protocol validation rejects bodies > 256KB (MAX_BODY_SIZE),
/// so putting 1MB+ chunk data in the body causes "body_len exceeds
/// MAX_BODY_SIZE" errors and connection drops. The server-side handler
/// prefers `msg.data` and falls back to the DataLen TLV field for backward
/// compatibility — since we always send data in the DATA segment now, the
/// fallback path is never taken.
fn build_write_tlv_with_inode(
    volume_id: u64,
    file_key: u64,
    inode: u64,
    lease_token: Option<&str>,
    client_id: Option<&str>,
) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, volume_id);
    enc.add_u64(FieldId::FileKey, file_key);
    enc.add_u64(FieldId::Inode, inode); // inode for lease validation
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
fn build_read_tlv(volume_id: u64, file_key: u64, offset: i64, size: i32) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, volume_id);
    enc.add_u64(FieldId::FileKey, file_key);
    enc.add_u64(FieldId::Offset, offset as u64);
    enc.add_u64(FieldId::Size, size as u64);
    enc.into_bytes()
}

/// 构建批量写操作的 TLV 请求体
fn build_batch_write_tlv(
    volume_id: u64,
    file_key: u64,
    entries_count: usize,
    data: &[u8],
    lease_token: Option<&str>,
    client_id: Option<&str>,
) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, volume_id);
    enc.add_u64(FieldId::FileKey, file_key);
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
fn build_delete_tlv(volume_id: u64, file_key: u64) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, volume_id);
    enc.add_u64(FieldId::FileKey, file_key);
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
        let volume_id = dec.next_u64(powerfs_net::FieldId::VolumeId).unwrap_or(0);
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
                url: self.facade.filer_addr(),
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
        // Some transports (e.g. MockServer) place the payload in the data
        // segment rather than the body segment, so fall back to resp.data
        // when resp.body is empty — same pattern as assign_volume.
        let body = if !resp.body.is_empty() {
            &resp.body
        } else {
            &resp.data
        };
        let mut dec = powerfs_net::TlvDecoder::new(body);
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

    /// Resolve parent_ino from a directory path by walking the path from root
    async fn resolve_parent_ino(&self, dir_path: &str) -> Result<u64> {
        let dir_path = dir_path.trim_end_matches('/');
        if dir_path.is_empty() || dir_path == "/" {
            return Ok(1); // Root inode
        }

        // Walk path: split into components, resolve each one from root
        let parts: Vec<&str> = dir_path.split('/').filter(|p| !p.is_empty()).collect();

        let mut current_ino: u64 = 1; // Start from root
        for part in &parts {
            match self.get_entry_by_parent(current_ino, part).await? {
                Some(entry) => {
                    current_ino = entry.attributes.as_ref().map(|a| a.ino).ok_or_else(|| {
                        PowerFsError::Internal(format!("Entry '{}' has no inode", part))
                    })?;
                }
                None => {
                    return Err(PowerFsError::Internal(format!(
                        "Directory component '{}' not found in path '{}'",
                        part, dir_path
                    )));
                }
            }
        }
        Ok(current_ino)
    }

    /// Create an entry using a known parent inode, avoiding path resolution.
    /// This is used by FUSE create/mkdir callbacks which already know the
    /// parent inode, eliminating a round-trip lookup that can fail due to
    /// Raft apply lag on the Filer.
    pub async fn create_entry_with_parent_ino(
        &self,
        entry: &Entry,
        parent_ino: u64,
        _client_id: &str,
    ) -> Result<u64> {
        let name = entry.name.clone();
        let mode = entry
            .attributes
            .as_ref()
            .map(|a| a.mode as u64)
            .unwrap_or(0o644);
        let uid = entry.attributes.as_ref().map(|a| a.uid as u64).unwrap_or(0);
        let gid = entry.attributes.as_ref().map(|a| a.gid as u64).unwrap_or(0);
        let is_dir = mode & 0o170000 == 0o040000;

        let payload =
            build_create_tlv_with_chunks(parent_ino, &name, mode, uid, gid, &entry.chunks);

        let msg_type = if is_dir {
            powerfs_net::MsgType::Mkdir
        } else {
            powerfs_net::MsgType::Create
        };

        let result = self
            .facade
            .submit_metadata_request_with_type(
                crate::request_state::RequestKind::Metadata,
                self.facade
                    .meta_shard_client()
                    .calculate_shard_id(parent_ino),
                payload,
                msg_type,
            )
            .await
            .map_err(|e| PowerFsError::Internal(format!("Facade create_entry failed: {}", e)))?;

        let response_data = result
            .payload
            .as_deref()
            .filter(|d| !d.is_empty())
            .or(result.data.as_deref());

        match response_data {
            Some(data) if !data.is_empty() => Ok(parse_create_response_tlv(data)),
            _ => Err(PowerFsError::Internal(
                "Create returned empty response".to_string(),
            )),
        }
    }
}

#[async_trait]
impl MetadataProvider for FacadeMetadataProvider {
    async fn get_entry(&self, path: &str) -> Result<Option<Entry>> {
        let path = path.trim_start_matches('/');
        if path.is_empty() {
            return Ok(None);
        }

        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if parts.is_empty() {
            return Ok(None);
        }

        let mut current_ino: u64 = 1;
        for (i, part) in parts.iter().enumerate() {
            match self.get_entry_by_parent(current_ino, part).await? {
                Some(entry) => {
                    if i == parts.len() - 1 {
                        return Ok(Some(entry));
                    }
                    let ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
                    if ino == 0 {
                        return Ok(None);
                    }
                    current_ino = ino;
                }
                None => return Ok(None),
            }
        }
        Ok(None)
    }

    async fn get_entry_by_parent(&self, parent_ino: u64, name: &str) -> Result<Option<Entry>> {
        let payload = build_lookup_tlv(parent_ino, name);

        let result = self
            .facade
            .submit_metadata_request_with_type(
                crate::request_state::RequestKind::Metadata,
                self.facade
                    .meta_shard_client()
                    .calculate_shard_id(parent_ino),
                payload,
                powerfs_net::MsgType::Lookup,
            )
            .await
            .map_err(|e| {
                PowerFsError::Internal(format!("Facade get_entry_by_parent failed: {}", e))
            })?;

        let response_data = result
            .payload
            .as_deref()
            .filter(|d| !d.is_empty())
            .or(result.data.as_deref());

        match response_data {
            Some(data) if !data.is_empty() => {
                let path = if parent_ino == 1 {
                    format!("/{}", name)
                } else {
                    name.to_string()
                };
                Ok(parse_entry_from_tlv(data, &path))
            }
            _ => {
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

    async fn get_entry_by_inode(&self, inode: u64) -> Result<Option<(Entry, String)>> {
        log::debug!(
            "FacadeMetadataProvider::get_entry_by_inode called: inode={}",
            inode
        );

        let payload = build_getattr_tlv(inode);

        let shard_id = self.facade.meta_shard_client().calculate_shard_id(inode);
        let result = self
            .facade
            .submit_metadata_request_with_type(
                crate::request_state::RequestKind::Metadata,
                shard_id,
                payload,
                powerfs_net::MsgType::GetAttr,
            )
            .await
            .map_err(|e| {
                PowerFsError::Internal(format!("Facade get_entry_by_inode failed: {}", e))
            })?;

        let response_data = result
            .payload
            .as_deref()
            .filter(|d| !d.is_empty())
            .or(result.data.as_deref());

        match response_data {
            Some(data) if !data.is_empty() => match parse_entry_from_tlv(data, "") {
                Some(entry) => {
                    log::debug!(
                        "get_entry_by_inode: parsed entry for inode={}, name={}",
                        inode,
                        entry.name
                    );
                    Ok(Some((entry, "".to_string())))
                }
                None => {
                    log::debug!(
                        "get_entry_by_inode: parse_entry_from_tlv returned None for inode={}",
                        inode
                    );
                    Ok(None)
                }
            },
            _ => {
                log::debug!(
                    "get_entry_by_inode: no response data for inode={}, result.data={:?}",
                    inode,
                    result.data
                );
                if let Some(data) = &result.data {
                    if let Ok(facade_resp) = serde_json::from_slice::<FacadeResponse>(data) {
                        log::debug!(
                            "get_entry_by_inode: facade_resp success={}",
                            facade_resp.success
                        );
                        if !facade_resp.success {
                            return Ok(None);
                        }
                    }
                }
                Ok(None)
            }
        }
    }

    async fn create_entry(&self, entry: &Entry, _client_id: &str) -> Result<u64> {
        // 从 entry.directory 解析 parent_ino
        let parent_ino = self.resolve_parent_ino(&entry.directory).await?;
        let name = entry.name.clone();
        let mode = entry
            .attributes
            .as_ref()
            .map(|a| a.mode as u64)
            .unwrap_or(0o644);
        let uid = entry.attributes.as_ref().map(|a| a.uid as u64).unwrap_or(0);
        let gid = entry.attributes.as_ref().map(|a| a.gid as u64).unwrap_or(0);
        let is_dir = mode & 0o170000 == 0o040000;

        let payload =
            build_create_tlv_with_chunks(parent_ino, &name, mode, uid, gid, &entry.chunks);

        let msg_type = if is_dir {
            powerfs_net::MsgType::Mkdir
        } else {
            powerfs_net::MsgType::Create
        };

        let result = self
            .facade
            .submit_metadata_request_with_type(
                crate::request_state::RequestKind::Metadata,
                self.facade
                    .meta_shard_client()
                    .calculate_shard_id(parent_ino),
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
        client_id: &str,
        old_size: u64,
        is_truncate: bool,
    ) -> Result<u64> {
        let ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
        let new_size = entry.content_size;
        let mode = entry.attributes.as_ref().map(|a| a.mode as u64);
        let uid = entry.attributes.as_ref().map(|a| a.uid as u64);
        let gid = entry.attributes.as_ref().map(|a| a.gid as u64);
        let mtime = entry
            .attributes
            .as_ref()
            .map(|a| a.mtime.timestamp() as u64);
        let atime = entry
            .attributes
            .as_ref()
            .map(|a| a.atime.timestamp() as u64);

        // Determine what changed
        let size_changed = new_size != old_size;
        let has_meta = mode.is_some() || uid.is_some() || gid.is_some();

        // Split into two paths:
        // 1. SetAttrData (strong consistency via Lease/Raft) - for size changes
        // 2. SetAttrMeta (eventual consistency via CRDT) - for mode/uid/gid changes
        if size_changed || is_truncate {
            let data_payload = build_setattr_data_tlv(ino, new_size);
            let result = self
                .facade
                .submit_metadata_request_with_type(
                    crate::request_state::RequestKind::Metadata,
                    self.facade.meta_shard_client().calculate_shard_id(ino),
                    data_payload,
                    powerfs_net::MsgType::SetAttrData,
                )
                .await
                .map_err(|e| PowerFsError::Internal(format!("Facade SetAttrData failed: {}", e)))?;

            if result.payload.is_none() && result.data.is_none() {
                return Err(PowerFsError::Internal(
                    "SetAttrData returned empty response".to_string(),
                ));
            }
        }

        if has_meta {
            let timestamp = Utc::now().timestamp() as u64;
            let meta_payload =
                build_setattr_meta_tlv(ino, mode, uid, gid, mtime, atime, client_id, timestamp);
            let _result = self
                .facade
                .submit_metadata_request_with_type(
                    crate::request_state::RequestKind::Metadata,
                    self.facade.meta_shard_client().calculate_shard_id(ino),
                    meta_payload,
                    powerfs_net::MsgType::SetAttrMeta,
                )
                .await
                .map_err(|e| PowerFsError::Internal(format!("Facade SetAttrMeta failed: {}", e)))?;
        }

        Ok(new_size)
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
                self.facade.meta_shard_client().calculate_shard_id(inode),
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
                self.facade.meta_shard_client().calculate_shard_id(inode),
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
    /// 获取/续期指定 (volume, inode, stripe) 的有效 lease 并返回 token。
    /// 优先读缓存；若缓存未命中或已过期，向 Volume 发起 RangeLease 请求并更新缓存。
    /// inode 用于 Volume Server 端的 lease 校验（lease 按 inode + stripe 注册）。
    ///
    /// P1-3: 当缓存 lease 有效但剩余时间低于 RENEW_THRESHOLD 时，主动续租，
    /// 避免 lease 在长写操作中途过期导致服务端 validate_token 失败。
    ///
    /// Phase 1: stripe_start 由 offset 计算，确保大文件写 stripe 1+ 时获取
    /// 正确 stripe 的 lease，而非总是获取 stripe 0。
    async fn ensure_lease(
        &self,
        volume_id: u64,
        file_key: u64,
        inode: u64,
        offset: u64,
    ) -> powerfs_common::error::Result<String> {
        // 方案 A: inode metadata lease (Filer-managed).
        //
        // When lease_mode == "inode", the Volume Server backend doesn't
        // support range lease (e.g., NVMe-oF target). We acquire a per-inode
        // exclusive lease from the Filer instead. The token is still passed
        // to write_needle, but Volume Server skips validation because
        // lease_enabled=false. Consistency is guaranteed by Filer's
        // UpdateInodeSizeChunks Raft commit.
        if self.facade.is_inode_lease_mode() {
            return self.ensure_inode_lease(inode).await;
        }

        // 方案 D: range lease (Volume Server-managed) — existing logic
        let vid = volume_id;
        let stripe_start = crate::volume_client::stripe_start_from_offset(offset);

        // Fast path: use cached valid lease token via facade
        // Cache key is (volume_id, inode, stripe_start) to match server-side
        // StripeKey { inode, stripe_start, stripe_count } registration.
        if let Some(tok) = self.facade.get_valid_lease_token(vid, inode, stripe_start) {
            // P1-3: proactively renew if remaining time is below threshold.
            // This prevents lease expiry mid-write for long operations.
            if let Some(remaining) = self.facade.get_lease_remaining(vid, inode, stripe_start) {
                if remaining < RENEW_THRESHOLD {
                    log::debug!(
                        "ensure_lease: proactive renew for volume={} inode={} stripe_start={} remaining_ms={} < threshold_ms={}",
                        vid,
                        inode,
                        stripe_start,
                        remaining.as_millis(),
                        RENEW_THRESHOLD.as_millis()
                    );
                    // Best-effort renew: if it fails, fall back to the
                    // existing valid token (server-side grace period covers
                    // brief overruns). A failed renew is not fatal.
                    if let Err(e) = self
                        .facade
                        .renew_lease(vid, inode, stripe_start, &tok)
                        .await
                    {
                        log::warn!(
                            "ensure_lease: proactive renew failed for volume={} inode={} stripe_start={} err={} (falling back to existing token)",
                            vid,
                            inode,
                            stripe_start,
                            e
                        );
                    }
                }
            }
            return Ok(tok);
        }

        let client_id = self.facade.client_id();
        let duration_ms = 60_000; // 1 min default exclusive lease
                                  // Register lease with real inode (not file_key) so server-side
                                  // validate_token_with_grace_period(token, holder, inode) matches
        let payload = build_range_lease_tlv(inode, stripe_start, 1, &client_id, true, duration_ms);

        log::debug!(
            "ensure_lease: acquiring for volume={} file_key={} inode={} stripe_start={} client={}",
            volume_id,
            file_key,
            inode,
            stripe_start,
            client_id
        );

        // P3: If the volume is not in the local router (e.g., topology sync
        // is incomplete or the Filer allocated a volume the FUSE client hasn't
        // discovered yet), resolve it from the Master before submitting the
        // lease request. Without this, process_lease_request_internal returns
        // VolumeNotFound and the flush fails silently.
        if self.facade.volume_client().get_volume(vid).is_none() {
            log::debug!(
                "ensure_lease: volume {} not in router, resolving from Master",
                vid
            );
            let vol_provider = FacadeVolumeProvider::new(self.facade.clone());
            use powerfs_common::traits::VolumeProvider;
            match vol_provider.lookup_volume(VolumeId(vid)).await {
                Ok(locations) if !locations.is_empty() => {
                    self.facade
                        .volume_client()
                        .resolve_and_set_volume_route(vid, &locations);
                    log::info!(
                        "ensure_lease: resolved volume {} -> {} locations, added to router",
                        vid,
                        locations.len()
                    );
                }
                Ok(_) => {
                    return Err(PowerFsError::Internal(format!(
                        "Volume {} not found in Master topology (empty locations)",
                        vid
                    )));
                }
                Err(e) => {
                    return Err(PowerFsError::Internal(format!(
                        "Failed to look up volume {} from Master: {}",
                        vid, e
                    )));
                }
            }
        }

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

        // Store lease via facade with (inode, stripe_start) as cache key
        self.facade.update_lease(
            vid,
            inode,
            stripe_start,
            lease_token.clone(),
            Duration::from_millis(duration_ms),
        );

        log::debug!(
            "ensure_lease: acquired token={:.16}... for volume={} inode={} stripe_start={}",
            lease_token,
            volume_id,
            inode,
            stripe_start
        );

        Ok(lease_token)
    }

    /// Ensure an inode metadata lease is held (方案 A, Phase 2).
    ///
    /// Filer-managed per-inode exclusive lease. Used when the Volume Server
    /// backend doesn't support range lease (e.g., NVMe-oF target).
    ///
    /// Flow:
    /// 1. Fast path: if a cached lease is still valid, return it. Proactively
    ///    renew if remaining time < RENEW_THRESHOLD.
    /// 2. Slow path: acquire a new lease from the Filer, cache it, return token.
    ///
    /// The returned token is passed to `write_needle` as the `LeaseToken`
    /// field. Volume Server skips validation because `lease_enabled=false`.
    async fn ensure_inode_lease(&self, inode: u64) -> powerfs_common::error::Result<String> {
        // Fast path: use cached valid inode lease token
        if let Some((token, remaining)) = self.facade.get_valid_inode_lease_token(inode) {
            // P1-3: proactively renew if remaining time is below threshold.
            if remaining < RENEW_THRESHOLD {
                let client_id = self.facade.client_id();
                let duration_ms = self.facade.lease_duration_ms();
                log::debug!(
                    "ensure_inode_lease: proactive renew for inode={} remaining_ms={} < threshold_ms={}",
                    inode,
                    remaining.as_millis(),
                    RENEW_THRESHOLD.as_millis()
                );
                // Best-effort renew: if it fails, fall back to existing token.
                // The Filer's grace period covers brief overruns.
                // renew_inode_lease auto-updates cache on success.
                if let Err(e) = self
                    .facade
                    .renew_inode_lease(inode, &client_id, &token, duration_ms)
                    .await
                {
                    log::warn!(
                        "ensure_inode_lease: proactive renew failed for inode={} err={} (falling back to existing token)",
                        inode,
                        e
                    );
                }
            }
            return Ok(token);
        }

        // Slow path: acquire new inode lease from Filer
        // acquire_inode_lease auto-caches the token on success.
        let client_id = self.facade.client_id();
        let duration_ms = self.facade.lease_duration_ms();

        log::debug!(
            "ensure_inode_lease: acquiring for inode={} client={}",
            inode,
            client_id
        );

        let (token, _expire_at_ms) = self
            .facade
            .acquire_inode_lease(inode, &client_id, duration_ms)
            .await
            .map_err(|e| PowerFsError::Internal(format!("AcquireInodeLease failed: {}", e)))?;

        log::debug!(
            "ensure_inode_lease: acquired token={:.16}... for inode={}",
            token,
            inode
        );

        Ok(token)
    }

    /// 写入数据时使用指定的 lease token（若提供），否则自动获取。
    /// 用于 write->flush 路径传递已获取的 lease，避免重复获取。
    /// inode 用于 Volume Server 端的 lease 校验（lease 按 inode 注册，非 file_key）。
    #[allow(clippy::too_many_arguments)]
    pub async fn write_blob_with_lease(
        &self,
        volume_id: u64,
        file_key: u64,
        inode: u64,
        offset: i64,
        _size: i32,
        data: &[u8],
        lease_token: Option<&str>,
    ) -> Result<()> {
        // Use provided lease token if available, otherwise acquire one
        let token = if let Some(t) = lease_token {
            if !t.is_empty() {
                t.to_string()
            } else {
                self.ensure_lease(volume_id, file_key, inode, offset as u64)
                    .await
                    .map_err(|e| {
                        PowerFsError::Internal(format!("Failed to acquire lease: {}", e))
                    })?
            }
        } else {
            self.ensure_lease(volume_id, file_key, inode, offset as u64)
                .await
                .map_err(|e| PowerFsError::Internal(format!("Failed to acquire lease: {}", e)))?
        };

        let client_id = self.facade.client_id();

        // P3 fix: chunk data goes in the frame's DATA segment, not the TLV
        // body. The server's R5 check rejects bodies > 256KB; 1MB chunks in
        // the body caused "body_len exceeds MAX_BODY_SIZE" + connection drops.
        let payload =
            build_write_tlv_with_inode(volume_id, file_key, inode, Some(&token), Some(&client_id));

        // 阶段1.5 临时方案：直接发送 WriteNeedle，绕过 data_queue。
        //
        // 根因：data_queue 的 response 路由有 2 层间接
        //   (pending_requests → process_data_request_internal → response_waiters → submit_data_request_and_wait)
        // process_data_request_internal 的 spawn 任务在 80 核环境下仍出现调度延迟，
        // 导致 response 卡在中间层，10s 超时。
        //
        // send_write_needle_direct 只有 1 层间接 (pending_requests → recv_loop → oneshot)，
        // 实测 64K write 从 54s 降到 1.72s。
        //
        // TODO: 修复 data_queue 的 response 路由（去掉 response_waiters 中间层），
        //       让 process_data_request_internal 直接返回结果给 submit_data_request_and_wait，
        //       然后恢复走 data_queue 以获得并发控制能力。
        self.facade
            .send_write_needle_direct(volume_id, payload, data)
            .await
            .map_err(|e| {
                PowerFsError::Internal(format!("Facade write_blob_with_lease failed: {}", e))
            })?;

        Ok(())
    }
}

#[async_trait]
impl StorageProvider for FacadeStorageProvider {
    async fn write_blob(
        &self,
        volume_id: u64,
        file_key: u64,
        offset: i64,
        _size: i32,
        data: &[u8],
    ) -> Result<()> {
        // Acquire a valid lease BEFORE submitting the write
        // write_blob (trait method) uses build_write_tlv which does NOT include
        // FieldId::FileKey, so server-side falls back to file_key as inode.
        // Register lease with file_key as inode to match.
        let lease_token = self
            .ensure_lease(volume_id, file_key, file_key, offset as u64)
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
                volume_id,
                payload,
                powerfs_net::MsgType::WriteNeedle,
            )
            .await
            .map_err(|e| PowerFsError::Internal(format!("Facade write_blob failed: {}", e)))?;

        Ok(())
    }

    async fn batch_write_blob(
        &self,
        volume_id: u64,
        file_key: u64,
        entries: &[(i64, i32, Vec<u8>, u32)],
    ) -> Result<()> {
        let mut combined_data = Vec::new();
        for (_offset, _size, data, _cookie) in entries {
            combined_data.extend_from_slice(data);
        }

        // Use the first entry's offset for stripe calculation. If entries
        // is empty, default to offset 0 (stripe 0).
        let first_offset = entries.first().map(|(off, _, _, _)| *off).unwrap_or(0) as u64;

        // Acquire a valid lease BEFORE submitting the write
        // batch_write_blob uses build_batch_write_tlv which does NOT include
        // FieldId::FileKey, so server-side falls back to file_key as inode.
        // Register lease with file_key as inode to match.
        let lease_token = self
            .ensure_lease(volume_id, file_key, file_key, first_offset)
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
                volume_id,
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
        volume_id: u64,
        file_key: u64,
        offset: i64,
        size: i32,
    ) -> Result<Vec<u8>> {
        let payload = build_read_tlv(volume_id, file_key, offset, size);

        let result = self
            .facade
            .submit_data_request_with_type(
                crate::request_state::RequestKind::Read,
                volume_id,
                payload,
                powerfs_net::MsgType::ReadNeedleBlob,
            )
            .await
            .map_err(|e| PowerFsError::Internal(format!("Facade read_blob failed: {}", e)))?;

        // The net client concatenates body+data into resp.body (see client.rs recv_response_locked).
        // success_with_payload maps resp.body -> result.data and resp.data -> result.payload.
        // Since the volume server puts file content in the `data` field of NetMessage but the
        // client reads everything into `body`, the actual content is in result.data.
        // Merge both to be robust against future protocol changes.
        let mut data = result.data.unwrap_or_default();
        if let Some(payload) = result.payload {
            if !payload.is_empty() {
                data.extend_from_slice(&payload);
            }
        }
        Ok(data)
    }

    async fn delete_blob(&self, volume_id: u64, file_key: u64) -> Result<()> {
        let payload = build_delete_tlv(volume_id, file_key);

        let _result = self
            .facade
            .submit_mgmt_request_with_type(volume_id, payload, powerfs_net::MsgType::DeleteNeedle)
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
