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
use powerfs_net::serialize::TlvEncoder;
use powerfs_net::FieldId;
use std::sync::Arc;

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

/// 将 FacadeResponse 中的 Entry 数据解析为 Entry
fn parse_entry_from_json(json: &serde_json::Value, path: &str) -> Option<Entry> {
    let obj = json.as_object()?;

    let attributes = obj.get("attributes").and_then(|a| {
        Some(EntryAttributes {
            ino: a.get("ino")?.as_u64()?,
            mode: a.get("mode")?.as_u64()? as u32,
            uid: a.get("uid")?.as_u64()? as u32,
            gid: a.get("gid")?.as_u64()? as u32,
            atime: parse_datetime(&a["atime"]),
            mtime: parse_datetime(&a["mtime"]),
            ctime: parse_datetime(&a["ctime"]),
            crtime: parse_datetime(&a["crtime"]),
        })
    });

    Some(Entry {
        name: obj
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or(path)
            .to_string(),
        directory: obj
            .get("directory")
            .and_then(|d| d.as_str())
            .unwrap_or("")
            .to_string(),
        attributes,
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        extended: std::collections::HashMap::new(),
        content_size: obj
            .get("content_size")
            .and_then(|s| s.as_u64())
            .unwrap_or(0),
        disk_size: obj.get("disk_size").and_then(|s| s.as_u64()).unwrap_or(0),
        ttl: String::new(),
        symlink_target: obj
            .get("symlink_target")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string(),
        owner: String::new(),
        generation: 0,
    })
}

/// 解析 DateTime 从 JSON 字段
fn parse_datetime(value: &serde_json::Value) -> chrono::DateTime<Utc> {
    if let Some(ts) = value.as_i64() {
        chrono::DateTime::from_timestamp(ts, 0).unwrap_or_else(Utc::now)
    } else {
        Utc::now()
    }
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
) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, volume_id as u64);
    enc.add_u64(FieldId::Name, file_key);
    enc.add_u64(FieldId::Entries, entries_count as u64);
    let _ = enc.add_bytes(FieldId::DataLen, data);
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
        let payload = serde_json::to_vec(&serde_json::json!({
            "collection": collection,
            "replication": replication,
        }))
        .map_err(|e| PowerFsError::Internal(e.to_string()))?;

        let result = self
            .facade
            .submit_metadata_request_with_type(
                crate::request_state::RequestKind::Metadata,
                0,
                payload,
                powerfs_net::MsgType::AssignVolumeV2,
            )
            .await
            .map_err(|e| PowerFsError::Internal(format!("Facade assign_volume failed: {}", e)))?;

        let response = parse_response_from_result(result)?;

        if !response.success {
            return Err(PowerFsError::Internal(
                response
                    .error
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ));
        }

        // 解析 Fid
        let fid = if let Some(data) = &response.data {
            let volume_id = data.get("volume_id").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
            let cookie = data.get("cookie").and_then(|v| v.as_u64()).unwrap_or(0);
            let file_key = data.get("file_key").and_then(|v| v.as_u64()).unwrap_or(0);
            Fid {
                volume_id: VolumeId(volume_id),
                cookie,
                file_key,
            }
        } else {
            Fid {
                volume_id: VolumeId(0),
                cookie: 0,
                file_key: 0,
            }
        };

        // 解析 Locations
        let locations = if let Some(data) = &response.data {
            data.get("locations")
                .and_then(|l| l.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|loc| {
                            let url = loc.get("url")?.as_str()?.to_string();
                            Some(Location {
                                public_url: loc
                                    .get("public_url")
                                    .and_then(|u| u.as_str())
                                    .unwrap_or(&url)
                                    .to_string(),
                                url,
                                grpc_port: 0,
                                data_center: String::new(),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        Ok((fid, locations))
    }

    async fn lookup_volume(&self, volume_id: VolumeId) -> Result<Vec<Location>> {
        let result = self
            .facade
            .submit_mgmt_request_with_type(
                volume_id.0 as u64,
                vec![],
                powerfs_net::MsgType::LookupVolume,
            )
            .await
            .map_err(|e| PowerFsError::Internal(format!("Facade lookup_volume failed: {}", e)))?;

        let response = parse_response_from_result(result)?;

        if !response.success {
            return Err(PowerFsError::Internal(
                response
                    .error
                    .unwrap_or_else(|| "Unknown error".to_string()),
            ));
        }

        let locations = if let Some(data) = &response.data {
            data.get("locations")
                .and_then(|l| l.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|loc| {
                            let url = loc.get("url").and_then(|u| u.as_str())?.to_string();
                            Some(Location {
                                public_url: url.clone(),
                                url,
                                grpc_port: 0,
                                data_center: String::new(),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        Ok(locations)
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

        let payload = serde_json::to_vec(&serde_json::json!({
            "parent_ino": parent_ino,
            "name": name,
        }))
        .map_err(|e| PowerFsError::Internal(e.to_string()))?;

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

        let response = parse_response_from_result(result)?;

        if !response.success {
            return Ok(None);
        }

        if let Some(data) = &response.data {
            Ok(parse_entry_from_json(data, path))
        } else {
            Ok(None)
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

        let payload = serde_json::to_vec(&serde_json::json!({
            "parent_ino": parent_ino,
            "name": name,
            "mode": mode,
            "uid": uid,
            "gid": gid,
        }))
        .map_err(|e| PowerFsError::Internal(e.to_string()))?;

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

        let response = parse_response_from_result(result)?;

        if !response.success {
            return Err(PowerFsError::Internal(
                response
                    .error
                    .unwrap_or_else(|| "Create failed".to_string()),
            ));
        }

        // 解析新创建的 inode
        let ino = response
            .data
            .as_ref()
            .and_then(|d| d.get("ino"))
            .and_then(|i| i.as_u64())
            .unwrap_or(0);
        Ok(ino)
    }

    async fn update_entry(
        &self,
        entry: &Entry,
        _client_id: &str,
        old_size: u64,
        is_truncate: bool,
    ) -> Result<u64> {
        let ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
        let new_size = entry.content_size;
        let mode = entry.attributes.as_ref().map(|a| a.mode as u64);
        let uid = entry.attributes.as_ref().map(|a| a.uid as u64);
        let gid = entry.attributes.as_ref().map(|a| a.gid as u64);

        let payload = serde_json::to_vec(&serde_json::json!({
            "ino": ino,
            "size": new_size,
            "old_size": old_size,
            "is_truncate": is_truncate,
            "mode": mode,
            "uid": uid,
            "gid": gid,
        }))
        .map_err(|e| PowerFsError::Internal(e.to_string()))?;

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

        let response = parse_response_from_result(result)?;

        if !response.success {
            return Err(PowerFsError::Internal(
                response
                    .error
                    .unwrap_or_else(|| "Update failed".to_string()),
            ));
        }

        Ok(new_size)
    }

    async fn delete_entry(&self, inode: u64, is_dir: bool, _client_id: &str) -> Result<()> {
        let msg_type = if is_dir {
            powerfs_net::MsgType::Rmdir
        } else {
            powerfs_net::MsgType::Unlink
        };

        let payload = serde_json::to_vec(&serde_json::json!({
            "ino": inode,
            "is_dir": is_dir,
        }))
        .map_err(|e| PowerFsError::Internal(e.to_string()))?;

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

        let response = parse_response_from_result(result)?;

        if !response.success {
            return Err(PowerFsError::Internal(
                response
                    .error
                    .unwrap_or_else(|| "Delete failed".to_string()),
            ));
        }

        Ok(())
    }

    async fn list_entries(&self, inode: u64, limit: u32, _client_id: &str) -> Result<Vec<Entry>> {
        let payload = serde_json::to_vec(&serde_json::json!({
            "ino": inode,
            "limit": limit,
        }))
        .map_err(|e| PowerFsError::Internal(e.to_string()))?;

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

        let response = parse_response_from_result(result)?;

        if !response.success {
            return Ok(Vec::new());
        }

        let entries = response
            .data
            .as_ref()
            .and_then(|d| d.get("entries"))
            .and_then(|e| e.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|entry_json| {
                        let name = entry_json.get("name").and_then(|n| n.as_str())?;
                        parse_entry_from_json(entry_json, name)
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(entries)
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
        let payload = build_write_tlv(volume_id, file_key, data, None, None);

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

        let payload = build_batch_write_tlv(volume_id, file_key, entries.len(), &combined_data);

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
    fn test_parse_entry_from_json() {
        let json = serde_json::json!({
            "name": "test.txt",
            "directory": "/dir",
            "content_size": 1024,
            "disk_size": 2048,
            "symlink_target": "",
            "attributes": {
                "ino": 12345,
                "mode": 33188,
                "uid": 1000,
                "gid": 1000,
                "atime": 1700000000,
                "mtime": 1700000000,
                "ctime": 1700000000,
                "crtime": 1700000000
            }
        });

        let entry = parse_entry_from_json(&json, "test.txt").unwrap();
        assert_eq!(entry.name, "test.txt");
        assert_eq!(entry.content_size, 1024);
        assert_eq!(entry.disk_size, 2048);

        let attrs = entry.attributes.unwrap();
        assert_eq!(attrs.ino, 12345);
        assert_eq!(attrs.mode, 33188);
        assert_eq!(attrs.uid, 1000);
        assert_eq!(attrs.gid, 1000);
    }

    #[test]
    fn test_parse_entry_minimal() {
        let json = serde_json::json!({});
        let entry = parse_entry_from_json(&json, "empty.txt").unwrap();
        assert_eq!(entry.name, "empty.txt");
        assert_eq!(entry.content_size, 0);
        assert!(entry.attributes.is_none());
    }

    #[test]
    fn test_parse_entry_invalid() {
        let json = serde_json::Value::String("invalid".to_string());
        let result = parse_entry_from_json(&json, "test");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_datetime() {
        let ts = serde_json::json!(1700000000);
        let dt = parse_datetime(&ts);
        assert!(dt.timestamp() > 0);

        let invalid = serde_json::json!("not_a_timestamp");
        let dt2 = parse_datetime(&invalid);
        assert!(dt2.timestamp() > 0); // falls back to now
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
