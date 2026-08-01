//! Filer Net Handler - Implements powerfs-net protocol for Filer metadata operations
//!
//! This module provides FilerNetHandler that processes powerfs-net metadata messages
//! using MetaShardManager, which is the authoritative metadata manager with sharded
//! storage, Raft consensus, and CRDT support.

use crate::inode_notifier::InodeNotifier;
use crate::meta_shard_manager::{MetaShardManager, POSIX_ROOT_INODE};
use crate::raft_group_manager::ShardId;
use crate::shard_store::{FileType, InodeInfo};
use crate::shard_strategy::ShardStrategy;
use log::{debug, info, warn};
use powerfs_coherence::ChunkWire;
use powerfs_net::serialize::{EntryInfo, TlvDecoder, TlvEncoder};
use powerfs_net::{
    ClientType, FieldId, FrameFlags, MsgType, NetError, NetMessage, NetResult, PowerFsNetHandler,
    RequestContext, ServerRequestHandler, STATUS_ERR_NOT_FOUND, STATUS_ERR_REDIRECT,
    STATUS_ERR_SERVER_ERROR, STATUS_OK,
};
use std::sync::Arc;

/// Filer Net Handler implementation
pub struct FilerNetHandler {
    pub meta_shard_manager: Arc<MetaShardManager>,
    pub shard_strategy: Arc<ShardStrategy>,
    /// Net port for powerfs-net protocol (used to construct redirect addresses)
    pub net_port: u16,
    /// Inode notification broadcaster (optional, for cache invalidation)
    pub inode_notifier: Option<Arc<InodeNotifier>>,
}

impl FilerNetHandler {
    pub fn new(
        meta_shard_manager: Arc<MetaShardManager>,
        shard_strategy: Arc<ShardStrategy>,
        net_port: u16,
    ) -> Self {
        Self {
            meta_shard_manager,
            shard_strategy,
            net_port,
            inode_notifier: None,
        }
    }

    /// Create a new FilerNetHandler with InodeNotifier support
    pub fn with_notifier(
        meta_shard_manager: Arc<MetaShardManager>,
        shard_strategy: Arc<ShardStrategy>,
        net_port: u16,
        inode_notifier: Arc<InodeNotifier>,
    ) -> Self {
        Self {
            meta_shard_manager,
            shard_strategy,
            net_port,
            inode_notifier: Some(inode_notifier),
        }
    }

    /// Notify subscribers that an inode's metadata has changed.
    /// This is called after successful metadata mutations.
    fn notify_inode_change(&self, inode: u64, version: u64) {
        if let Some(ref notifier) = self.inode_notifier {
            let notifier = notifier.clone();
            tokio::spawn(async move {
                let count = notifier.notify(inode, version).await;
                if count > 0 {
                    debug!(
                        "FILER_NET: notified {} clients about inode {} change (v={})",
                        count, inode, version
                    );
                }
            });
        }
    }

    /// Build a response message
    fn build_response(msg: &NetMessage, status: u16, body: Vec<u8>) -> NetMessage {
        let flags = FrameFlags::new(FrameFlags::RESPONSE);
        let header = powerfs_net::FrameHeader::new(
            msg.header.msg_type,
            flags,
            msg.header.seq,
            body.len() as u32,
        )
        .with_status(status);
        NetMessage::new(header).with_body(body)
    }

    /// Check if current node is the leader for the given shard.
    /// Returns Ok(()) if leader, or Err(redirect_response) if not.
    async fn check_leader(&self, msg: &NetMessage, shard_id: ShardId) -> Result<(), NetMessage> {
        match self
            .meta_shard_manager
            .get_shard_leader_status(shard_id)
            .await
        {
            Some((true, _)) => Ok(()),
            Some((false, leader_addr)) if !leader_addr.is_empty() => {
                // Convert gRPC address to net address for client redirect
                // leader_addr is in format "ip:grpc_port" (e.g., "172.21.0.33:8889")
                // Need to return net address (e.g., "172.21.0.33:8890")
                let net_addr = Self::grpc_addr_to_net_addr(&leader_addr, self.net_port);

                // Return redirect response with leader net address
                let mut enc = TlvEncoder::new();
                let _ = enc.add_string(FieldId::Owner, &net_addr);
                Err(Self::build_response(
                    msg,
                    STATUS_ERR_REDIRECT,
                    enc.into_bytes(),
                ))
            }
            _ => {
                // Unknown leader status - cluster not ready, reject write
                warn!(
                    "Leader status unknown for shard {}, rejecting write",
                    shard_id.0
                );
                Err(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Convert gRPC address to net address by replacing the port.
    /// gRPC address format: "ip:grpc_port" (e.g., "172.21.0.33:8889")
    /// Net address format: "ip:net_port" (e.g., "172.21.0.33:8890")
    fn grpc_addr_to_net_addr(grpc_addr: &str, net_port: u16) -> String {
        if let Some(colon_pos) = grpc_addr.rfind(':') {
            let ip_part = &grpc_addr[..colon_pos];
            format!("{}:{}", ip_part, net_port)
        } else {
            grpc_addr.to_string()
        }
    }

    /// Convert InodeInfo to EntryInfo for powerfs-net response
    fn inode_to_entry_info(info: &InodeInfo) -> EntryInfo {
        let is_dir = matches!(info.file_type, FileType::Directory);
        EntryInfo {
            ino: info.inode,
            mode: info.mode,
            uid: info.uid,
            gid: info.gid,
            size: info.size,
            nlink: if is_dir { 2 } else { 1 },
            mtime: info.mtime,
            atime: info.atime,
            ctime: info.ctime,
            name: info.name.clone(),
            is_dir,
            symlink_target: if matches!(info.file_type, FileType::Symlink) {
                Some(info.name.clone())
            } else {
                None
            },
            version: info.version,
        }
    }

    /// 将 InodeInfo 的 chunks 列表 + fid/volume_id 序列化到 TLV encoder。
    ///
    /// 同时输出两套字段以保证兼容性：
    /// 1. **完整列表**（新协议）：`FieldId::Chunks` = JSON 序列化的 `Vec<ChunkWire>`，
    ///    fuse 端优先解析此字段，可获取多 chunk 文件的完整数据布局。
    /// 2. **首 chunk 字段**（旧协议）：`Fid`/`VolumeId`/`Cookie`/`FileKey`/`Size`，
    ///    仅包含首 chunk 信息，供旧客户端兼容读取单 chunk 文件。
    ///
    /// 修复历史 bug：`handle_getattr` 之前完全缺失 chunks 序列化，
    /// 导致 `get_entry_by_inode` 拿到的 chunks 列表恒为空，跨客户端读文件时
    /// 因 chunks 为空而触发 I/O error。
    fn encode_chunks_fields(enc: &mut TlvEncoder, info: &InodeInfo) -> Result<(), NetError> {
        // 完整 chunks 列表（JSON）
        if !info.chunks.is_empty() {
            let wire_list: Vec<ChunkWire> = info
                .chunks
                .iter()
                .map(|c| ChunkWire {
                    offset: c.offset,
                    size: c.size,
                    mtime: c.mtime,
                    fid: c.fid.clone(),
                    cookie: c.cookie,
                    crc32: c.crc32,
                })
                .collect();
            if let Ok(json) = serde_json::to_vec(&wire_list) {
                enc.add_bytes(FieldId::Chunks, &json)?;
            }
        }
        // 兼容旧字段：首 chunk + 全局 fid/volume_id
        if let Some(ref fid) = info.fid {
            enc.add_string(FieldId::Fid, fid)?;
        }
        if let Some(volume_id) = info.volume_id {
            enc.add_u64(FieldId::VolumeId, volume_id);
        }
        if let Some(chunk) = info.chunks.first() {
            enc.add_u64(FieldId::Cookie, chunk.cookie as u64);
            enc.add_u64(FieldId::FileKey, chunk.offset);
            enc.add_u64(FieldId::Size, chunk.size);
        }
        Ok(())
    }

    /// Handle Lookup request
    async fn handle_lookup(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();

        info!(
            "FILER_NET_LOOKUP: seq={}, parent_ino={}, name={}",
            msg.header.seq, parent_ino, name
        );

        // Check leadership for the correct shard before reading
        let shard_id = self.shard_strategy.calculate_shard(parent_ino);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        // Root lookup: when client looks up the root directory itself
        // (parent_ino == root and name is empty or "."), return the actual
        // root inode data from the database instead of hardcoded values
        if parent_ino == POSIX_ROOT_INODE && (name.is_empty() || name == ".") {
            info!("FILER_NET_LOOKUP: root lookup, fetching root inode from database");
            match self.meta_shard_manager.get_inode(POSIX_ROOT_INODE) {
                Some(info) => {
                    let entry_info = Self::inode_to_entry_info(&info);
                    info!(
                        "FILER_NET_LOOKUP: root ino={}, mode={:o}, is_dir={}",
                        entry_info.ino, entry_info.mode, entry_info.is_dir
                    );
                    let mut enc = TlvEncoder::new();
                    enc.add_u64(FieldId::Ino, entry_info.ino);
                    enc.add_u32(FieldId::Mode, entry_info.mode);
                    enc.add_u32(FieldId::Uid, entry_info.uid);
                    enc.add_u32(FieldId::Gid, entry_info.gid);
                    enc.add_u64(FieldId::Size, entry_info.size);
                    enc.add_u32(FieldId::Nlink, entry_info.nlink);
                    enc.add_u64(FieldId::Mtime, entry_info.mtime);
                    enc.add_u64(FieldId::Atime, entry_info.atime);
                    enc.add_u64(FieldId::Ctime, entry_info.ctime);
                    enc.add_string(FieldId::Name, &entry_info.name)?;
                    return Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()));
                }
                None => {
                    warn!("FILER_NET_LOOKUP: root inode not found in database - init may not have been run");
                    return Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new()));
                }
            }
        }

        match self.meta_shard_manager.lookup(parent_ino, name.as_str()) {
            Some(info) => {
                let entry_info = Self::inode_to_entry_info(&info);
                info!(
                    "FILER_NET_LOOKUP: returning ino={}, mode={:o}, is_dir={}, name={}",
                    entry_info.ino, entry_info.mode, entry_info.is_dir, entry_info.name
                );
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, entry_info.ino);
                enc.add_u32(FieldId::Mode, entry_info.mode);
                enc.add_u32(FieldId::Uid, entry_info.uid);
                enc.add_u32(FieldId::Gid, entry_info.gid);
                enc.add_u64(FieldId::Size, entry_info.size);
                enc.add_u32(FieldId::Nlink, entry_info.nlink);
                enc.add_u64(FieldId::Mtime, entry_info.mtime);
                enc.add_u64(FieldId::Atime, entry_info.atime);
                enc.add_u64(FieldId::Ctime, entry_info.ctime);
                enc.add_string(FieldId::Name, &entry_info.name)?;

                // 完整 chunks 列表 + 兼容旧单 chunk 字段
                Self::encode_chunks_fields(&mut enc, &info)?;

                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            None => Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new())),
        }
    }

    /// Handle GetAttr request
    async fn handle_getattr(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);

        info!("FILER_NET_GETATTR: ino={}", ino);

        // Check leadership for the correct shard before reading
        let shard_id = self.shard_strategy.calculate_shard(ino);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self.meta_shard_manager.get_inode(ino) {
            Some(info) => {
                let entry_info = Self::inode_to_entry_info(&info);
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, entry_info.ino);
                enc.add_u32(FieldId::Mode, entry_info.mode);
                enc.add_u32(FieldId::Uid, entry_info.uid);
                enc.add_u32(FieldId::Gid, entry_info.gid);
                enc.add_u64(FieldId::Size, entry_info.size);
                enc.add_u32(FieldId::Nlink, entry_info.nlink);
                enc.add_u64(FieldId::Mtime, entry_info.mtime);
                enc.add_u64(FieldId::Atime, entry_info.atime);
                enc.add_u64(FieldId::Ctime, entry_info.ctime);
                enc.add_string(FieldId::Name, &entry_info.name)?;
                // 完整 chunks 列表 + 兼容旧单 chunk 字段。
                // 修复历史 bug：此前 GetAttr 完全缺失 chunks 序列化，
                // 导致 fuse 端 get_entry_by_inode 拿到的 chunks 恒为空，
                // open() 时无法刷新账本，跨客户端读文件触发 I/O error。
                Self::encode_chunks_fields(&mut enc, &info)?;
                info!(
                    "FILER_NET_GETATTR: returned info for ino={}, name={}, chunks={}",
                    ino,
                    entry_info.name,
                    info.chunks.len()
                );
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            None => {
                warn!(
                    "FILER_NET_GETATTR: ino={} not found in meta_shard_manager",
                    ino
                );
                Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new()))
            }
        }
    }

    /// Handle SetAttr request (legacy unified path)
    async fn handle_setattr(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let size = dec.next_u64(FieldId::Size).ok();
        let mode = dec.next_u64(FieldId::Mode).ok();
        let uid = dec.next_u64(FieldId::Uid).ok();
        let gid = dec.next_u64(FieldId::Gid).ok();

        info!(
            "FILER_NET_SETATTR: ino={}, size={:?}, mode={:?}, uid={:?}, gid={:?}",
            ino, size, mode, uid, gid
        );

        let shard_id = self.shard_strategy.calculate_shard(ino);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .setattr(ino, shard_id, size, mode, uid, gid)
            .await
        {
            Ok(_) => Ok(Self::build_response(msg, STATUS_OK, Vec::new())),
            Err(e) => {
                warn!("FILER_NET_SETATTR failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle SetAttrData request (strong consistency path for size/chunks)
    async fn handle_setattr_data(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let size = dec.next_u64(FieldId::Size).ok();

        info!("FILER_NET_SETATTR_DATA: ino={}, size={:?}", ino, size);

        let shard_id = self.shard_strategy.calculate_shard(ino);

        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_SETATTR_DATA: not leader for shard {}, redirecting",
                shard_id.0
            );
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .setattr_data(ino, shard_id, size)
            .await
        {
            Ok(_) => {
                // Notify other clients that this inode's data (size/chunks) changed
                let now = chrono::Utc::now().timestamp() as u64;
                self.notify_inode_change(ino, now);
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!("FILER_NET_SETATTR_DATA failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle SetAttrMeta request (eventual consistency path for mode/uid/gid/timestamps)
    async fn handle_setattr_meta(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let mode = dec.next_u64(FieldId::Mode).ok();
        let uid = dec.next_u64(FieldId::Uid).ok();
        let gid = dec.next_u64(FieldId::Gid).ok();
        let mtime = dec.next_u64(FieldId::Mtime).ok();
        let atime = dec.next_u64(FieldId::Atime).ok();
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let timestamp = dec.next_u64(FieldId::Seq).unwrap_or(0);

        info!(
            "FILER_NET_SETATTR_META: ino={}, mode={:?}, uid={:?}, gid={:?}, mtime={:?}, client={}, ts={}",
            ino, mode, uid, gid, mtime, client_id, timestamp
        );

        let shard_id = self.shard_strategy.calculate_shard(ino);

        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_SETATTR_META: not leader for shard {}, redirecting",
                shard_id.0
            );
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .setattr_meta(
                ino, shard_id, mode, uid, gid, mtime, atime, &client_id, timestamp,
            )
            .await
        {
            Ok(_) => {
                // Notify other clients that this inode's metadata changed
                self.notify_inode_change(ino, timestamp);
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!("FILER_NET_SETATTR_META failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Create request (create file)
    async fn handle_create(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();
        let mode = dec.next_u64(FieldId::Mode).unwrap_or(0o644);
        let uid = dec.next_u64(FieldId::Uid).unwrap_or(0);
        let gid = dec.next_u64(FieldId::Gid).unwrap_or(0);

        // Parse optional chunk/fid info
        let fid = dec.next_string(FieldId::Fid).ok();
        let cookie = dec.next_u64(FieldId::Cookie).ok();
        let offset = dec.next_u64(FieldId::FileKey).ok();
        let chunk_size = dec.next_u64(FieldId::Size).ok();

        info!(
            "FILER_NET_CREATE: parent_ino={}, name={}, mode={:o}, uid={}, gid={}, has_fid={}",
            parent_ino,
            name,
            mode,
            uid,
            gid,
            fid.is_some()
        );

        let shard_id = self.shard_strategy.calculate_shard(parent_ino);

        // Check leader - redirect write requests to the correct leader
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_CREATE: not leader for shard {}, redirecting",
                shard_id.0
            );
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .create_file_with_shard(parent_ino, &name, shard_id)
            .await
        {
            Ok(ino) => {
                // Apply mode/uid/gid via setattr
                let _ = self
                    .meta_shard_manager
                    .setattr(ino, shard_id, None, Some(mode), Some(uid), Some(gid))
                    .await;

                // Store chunk/fid info if provided
                if let (Some(fid_str), Some(c), Some(o)) = (fid.clone(), cookie, offset) {
                    let sz = chunk_size.unwrap_or(0);
                    // Parse volume_id from Fid string (format: "volume_id,cookie,file_key")
                    let volume_id = fid_str
                        .split(',')
                        .next()
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0);
                    let _ = self
                        .meta_shard_manager
                        .set_chunks(ino, shard_id, fid_str, volume_id, c as u32, o, sz)
                        .await;
                }

                // B5: notify 目录条目变更（parent readdir 缓存 + 新 inode）
                let now = crate::shard_store::ShardStore::current_time();
                self.notify_inode_change(parent_ino, now);
                self.notify_inode_change(ino, now);

                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, ino);
                enc.add_u32(FieldId::Mode, mode as u32);
                enc.add_string(FieldId::Name, &name)?;
                // Return chunk/fid info in response
                if let Some(fid_str) = fid {
                    enc.add_string(FieldId::Fid, &fid_str)?;
                }
                if let Some(c) = cookie {
                    enc.add_u64(FieldId::Cookie, c);
                }
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_CREATE failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Mkdir request
    async fn handle_mkdir(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();
        let mode = dec.next_u64(FieldId::Mode).unwrap_or(0o755);
        let uid = dec.next_u64(FieldId::Uid).unwrap_or(0);
        let gid = dec.next_u64(FieldId::Gid).unwrap_or(0);

        info!(
            "FILER_NET_MKDIR: parent_ino={}, name={}, mode={:o}",
            parent_ino, name, mode
        );

        let shard_id = self.shard_strategy.calculate_shard(parent_ino);

        // Check leader - redirect write requests to the correct leader
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_MKDIR: not leader for shard {}, redirecting",
                shard_id.0
            );
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .create_directory(parent_ino, &name)
            .await
        {
            Ok(info) => {
                let shard_id = self.shard_strategy.calculate_shard(info.inode);
                let _ = self
                    .meta_shard_manager
                    .setattr(info.inode, shard_id, None, Some(mode), Some(uid), Some(gid))
                    .await;

                // B5: notify 目录条目变更（parent readdir 缓存 + 新目录 inode）
                let now = crate::shard_store::ShardStore::current_time();
                self.notify_inode_change(parent_ino, now);
                self.notify_inode_change(info.inode, now);

                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, info.inode);
                enc.add_u32(FieldId::Mode, (mode | 0o040000) as u32);
                enc.add_string(FieldId::Name, &name)?;
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_MKDIR failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Unlink request
    async fn handle_unlink(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();

        info!("FILER_NET_UNLINK: parent_ino={}, name={}", parent_ino, name);

        match self.meta_shard_manager.lookup(parent_ino, name.as_str()) {
            Some(info) => {
                let shard_id = self.shard_strategy.calculate_shard(info.inode);

                // Check leader - redirect write requests to the correct leader
                if let Err(redirect) = self.check_leader(msg, shard_id).await {
                    warn!(
                        "FILER_NET_UNLINK: not leader for shard {}, redirecting",
                        shard_id.0
                    );
                    return Ok(redirect);
                }

                match self
                    .meta_shard_manager
                    .delete_file_by_inode(info.inode, shard_id)
                    .await
                {
                    Ok(_) => {
                        // B5: notify 目录条目变更（parent readdir 缓存 + 被删 inode 失效）
                        let now = crate::shard_store::ShardStore::current_time();
                        self.notify_inode_change(parent_ino, now);
                        self.notify_inode_change(info.inode, now);
                        Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
                    }
                    Err(e) => {
                        warn!("FILER_NET_UNLINK failed: {}", e);
                        Ok(Self::build_response(
                            msg,
                            STATUS_ERR_SERVER_ERROR,
                            Vec::new(),
                        ))
                    }
                }
            }
            None => Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new())),
        }
    }

    /// Handle Rmdir request
    async fn handle_rmdir(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();

        info!("FILER_NET_RMDIR: parent_ino={}, name={}", parent_ino, name);

        let shard_id = self.shard_strategy.calculate_shard(parent_ino);

        // Check leader - redirect write requests to the correct leader
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_RMDIR: not leader for shard {}, redirecting",
                shard_id.0
            );
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .delete_directory(parent_ino, &name)
            .await
        {
            Ok(_) => {
                // B5: notify 目录条目变更
                let now = crate::shard_store::ShardStore::current_time();
                self.notify_inode_change(parent_ino, now);
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!("FILER_NET_RMDIR failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Rename request
    async fn handle_rename(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let old_parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let old_name = dec.next_string(FieldId::Name).unwrap_or_default();
        let new_parent_ino = dec.next_u64(FieldId::NewParentIno).unwrap_or(0);
        let new_name = dec.next_string(FieldId::NewName).unwrap_or_default();

        info!(
            "FILER_NET_RENAME: old_parent={}, old_name={}, new_parent={}, new_name={}",
            old_parent_ino, old_name, new_parent_ino, new_name
        );

        match self
            .meta_shard_manager
            .rename(old_parent_ino, &old_name, new_parent_ino, &new_name)
            .await
        {
            Ok(_) => {
                // B5: notify 两个目录条目变更
                let now = crate::shard_store::ShardStore::current_time();
                self.notify_inode_change(old_parent_ino, now);
                self.notify_inode_change(new_parent_ino, now);
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!("FILER_NET_RENAME failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle ReadDir request
    async fn handle_readdir(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let limit = dec.next_u64(FieldId::Limit).unwrap_or(1000);
        let last_name = dec.next_string(FieldId::LastName).unwrap_or_default();

        info!(
            "FILER_NET_READDIR: parent_ino={}, limit={}, last_name={}",
            parent_ino, limit, last_name
        );

        // Check leadership for the correct shard before reading
        let shard_id = self.shard_strategy.calculate_shard(parent_ino);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        let entries = self.meta_shard_manager.list_directory(parent_ino);

        // Filter by last_name for pagination
        let filtered: Vec<&InodeInfo> = if last_name.is_empty() {
            entries.iter().collect()
        } else {
            entries
                .iter()
                .filter(|e| e.name.as_str() > last_name.as_str())
                .collect()
        };

        let limited: Vec<&InodeInfo> = filtered.into_iter().take(limit as usize).collect();
        let has_more = (limited.len() as u64) < limit && !entries.is_empty();

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Count, limited.len() as u64);
        enc.add_u64(FieldId::HasMore, if has_more { 1 } else { 0 });

        for entry in &limited {
            let mut entry_enc = TlvEncoder::new();
            entry_enc.add_u64(FieldId::Ino, entry.inode);
            entry_enc.add_string(FieldId::Name, &entry.name)?;
            entry_enc.add_u32(FieldId::Mode, entry.mode);
            entry_enc.add_u64(FieldId::Uid, entry.uid as u64);
            entry_enc.add_u64(FieldId::Gid, entry.gid as u64);
            entry_enc.add_u64(FieldId::Size, entry.size);
            entry_enc.add_u64(FieldId::Atime, entry.atime);
            entry_enc.add_u64(FieldId::Mtime, entry.mtime);
            entry_enc.add_u64(FieldId::Ctime, entry.ctime);
            entry_enc.add_u64(FieldId::Nlink, entry.nlink as u64);
            // 完整 chunks 列表 + 兼容旧单 chunk 字段
            Self::encode_chunks_fields(&mut entry_enc, entry)?;
            enc.add_bytes(FieldId::Entry, &entry_enc.into_bytes())?;
        }

        Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
    }

    /// Handle StatFs request
    async fn handle_statfs(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Size, 1024 * 1024 * 1024); // 1TB
        enc.add_u64(FieldId::Blksize, 4096);
        Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
    }

    /// Handle Symlink request
    async fn handle_symlink(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();
        let target = dec.next_string(FieldId::SymlinkTarget).unwrap_or_default();

        info!(
            "FILER_NET_SYMLINK: parent_ino={}, name={}, target={}",
            parent_ino, name, target
        );

        match self
            .meta_shard_manager
            .create_symlink(parent_ino, &name, &target)
            .await
        {
            Ok(info) => {
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, info.inode);
                enc.add_string(FieldId::Name, &name)?;
                enc.add_string(FieldId::SymlinkTarget, &target)?;
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_SYMLINK failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Readlink request
    async fn handle_readlink(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);

        info!("FILER_NET_READLINK: ino={}", ino);

        match self.meta_shard_manager.get_inode(ino) {
            Some(info) => {
                let target = info.symlink_target.unwrap_or_default();
                let mut enc = TlvEncoder::new();
                enc.add_string(FieldId::SymlinkTarget, &target)?;
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            None => Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new())),
        }
    }

    /// Handle Link request (hard link)
    async fn handle_link(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let new_parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let new_name = dec.next_string(FieldId::Name).unwrap_or_default();

        info!(
            "FILER_NET_LINK: ino={}, new_parent={}, new_name={}",
            ino, new_parent_ino, new_name
        );

        match self
            .meta_shard_manager
            .create_hard_link(ino, new_parent_ino, &new_name)
            .await
        {
            Ok(_) => {
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, ino);
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_LINK failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2: CRDT delta sync handlers（fuse→filer 走 net 层）
    // -----------------------------------------------------------------------

    /// handle_push_delta：接收客户端 push 的 delta，merge 到 filer OR-Set
    async fn handle_push_delta(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let req: powerfs_coherence::PushDeltaRequest = match serde_json::from_slice(&msg.body) {
            Ok(r) => r,
            Err(e) => {
                warn!("FILER_NET_PUSH_DELTA: decode failed: {}", e);
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ));
            }
        };

        // fuse 端传 dir_ino 作为 shard_id，重映射到正确的 shard
        // push_delta 不需要单独 dir_ino（每个 delta 自带 parent_ino）
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(req.shard_id);
        // B4: leader 校验
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        // DeltaWire → proto DeltaOp
        let proto_deltas: Vec<crate::powerfs::DeltaOp> =
            req.deltas.iter().filter_map(wire_to_proto_delta).collect();
        let proto_vclock = req.client_vclock.as_ref().map(wire_to_proto_vclock);

        match self
            .meta_shard_manager
            .push_delta(shard_id, &req.client_id, &proto_deltas, &proto_vclock)
            .await
        {
            Ok(server_vclock) => {
                let resp = powerfs_coherence::PushDeltaResponse {
                    success: true,
                    error: String::new(),
                    server_vclock: proto_to_wire_vclock(&server_vclock),
                };
                let body = serde_json::to_vec(&resp).unwrap_or_default();
                Ok(Self::build_response(msg, STATUS_OK, body))
            }
            Err(e) => {
                warn!("FILER_NET_PUSH_DELTA failed: {}", e);
                let resp = powerfs_coherence::PushDeltaResponse {
                    success: false,
                    error: e,
                    server_vclock: VectorClockWire::default(),
                };
                let body = serde_json::to_vec(&resp).unwrap_or_default();
                Ok(Self::build_response(msg, STATUS_ERR_SERVER_ERROR, body))
            }
        }
    }

    /// handle_pull_delta：返回客户端尚未看到的 delta
    async fn handle_pull_delta(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let req: powerfs_coherence::PullDeltaRequest = match serde_json::from_slice(&msg.body) {
            Ok(r) => r,
            Err(e) => {
                warn!("FILER_NET_PULL_DELTA: decode failed: {}", e);
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ));
            }
        };

        // fuse 端传 dir_ino 作为 shard_id，重映射到正确的 shard
        // dir_ino 保留用于过滤（避免跨目录 delta 污染）
        let dir_ino = req.shard_id;
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(req.shard_id);
        // B4: leader 校验（pull 也走 leader，确保 delta 一致性）
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        let proto_vclock = req.client_vclock.as_ref().map(wire_to_proto_vclock);

        match self
            .meta_shard_manager
            .pull_delta(shard_id, dir_ino, &req.client_id, &proto_vclock)
            .await
        {
            Ok((proto_deltas, server_vclock)) => {
                let server_vc_wire = proto_to_wire_vclock(&server_vclock);
                let wire_deltas: Vec<_> = proto_deltas
                    .iter()
                    .map(|d| {
                        let mut wire = proto_to_wire_delta(d);
                        // 补全 vclock：用 server_vclock 确保客户端 apply 后 vclock 推进。
                        // proto DeltaOp 不携带 vclock，proto_to_wire_delta 默认为空，
                        // 导致客户端 vclock 不推进、puller 反复收到相同 delta。
                        if wire.vclock.entries.is_empty() && !server_vc_wire.entries.is_empty() {
                            wire.vclock = server_vc_wire.clone();
                        }
                        wire
                    })
                    .collect();
                let resp = powerfs_coherence::PullDeltaResponse {
                    deltas: wire_deltas,
                    server_vclock: server_vc_wire,
                };
                let body = serde_json::to_vec(&resp).unwrap_or_default();
                Ok(Self::build_response(msg, STATUS_OK, body))
            }
            Err(e) => {
                warn!("FILER_NET_PULL_DELTA failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// handle_alloc_inode_batch：批量授权 inode 预留段
    async fn handle_alloc_inode_batch(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let req: powerfs_coherence::AllocInodeBatchRequest = match serde_json::from_slice(&msg.body)
        {
            Ok(r) => r,
            Err(e) => {
                warn!("FILER_NET_ALLOC_INODE: decode failed: {}", e);
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ));
            }
        };

        // fuse 端传 dir_ino/parent 作为 shard_id，重映射到正确的 shard
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(req.shard_id);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .alloc_inode_batch(shard_id, req.count)
            .await
        {
            Ok((start, end)) => {
                let resp = powerfs_coherence::AllocInodeBatchResponse {
                    success: true,
                    error: String::new(),
                    start_inode: start,
                    end_inode: end,
                };
                let body = serde_json::to_vec(&resp).unwrap_or_default();
                Ok(Self::build_response(msg, STATUS_OK, body))
            }
            Err(e) => {
                warn!("FILER_NET_ALLOC_INODE failed: {}", e);
                let resp = powerfs_coherence::AllocInodeBatchResponse {
                    success: false,
                    error: e,
                    start_inode: 0,
                    end_inode: 0,
                };
                let body = serde_json::to_vec(&resp).unwrap_or_default();
                Ok(Self::build_response(msg, STATUS_ERR_SERVER_ERROR, body))
            }
        }
    }

    /// handle_update_inode_size_chunks：close 时强一致 sync 账本
    async fn handle_update_inode_size_chunks(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let req: powerfs_coherence::UpdateInodeSizeChunksRequest =
            match serde_json::from_slice(&msg.body) {
                Ok(r) => r,
                Err(e) => {
                    warn!("FILER_NET_UPDATE_SIZE_CHUNKS: decode failed: {}", e);
                    return Ok(Self::build_response(
                        msg,
                        STATUS_ERR_SERVER_ERROR,
                        Vec::new(),
                    ));
                }
            };

        // fuse 端传 dir_ino 作为 shard_id，重映射到正确的 shard
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(req.shard_id);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        let chunks: Vec<crate::shard_store::StoredFileChunk> = req
            .chunks
            .iter()
            .map(|c| crate::shard_store::StoredFileChunk {
                offset: c.offset,
                size: c.size,
                mtime: c.mtime,
                fid: c.fid.clone(),
                cookie: c.cookie,
                crc32: c.crc32,
            })
            .collect();

        match self
            .meta_shard_manager
            .update_inode_size_chunks_atomic(shard_id, req.inode, req.size, chunks)
            .await
        {
            Ok(_) => {
                let resp = powerfs_coherence::UpdateInodeSizeChunksResponse {
                    success: true,
                    error: String::new(),
                };
                let body = serde_json::to_vec(&resp).unwrap_or_default();
                Ok(Self::build_response(msg, STATUS_OK, body))
            }
            Err(e) => {
                warn!("FILER_NET_UPDATE_SIZE_CHUNKS failed: {}", e);
                let resp = powerfs_coherence::UpdateInodeSizeChunksResponse {
                    success: false,
                    error: e,
                };
                let body = serde_json::to_vec(&resp).unwrap_or_default();
                Ok(Self::build_response(msg, STATUS_ERR_SERVER_ERROR, body))
            }
        }
    }

    /// Phase 3.5.3: 处理 fuse 端 open 时上报的 open_count 递增请求。
    async fn handle_open_count_inc(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let req: powerfs_coherence::OpenCountRequest = match serde_json::from_slice(&msg.body) {
            Ok(r) => r,
            Err(e) => {
                warn!("FILER_NET_OPEN_COUNT_INC: decode failed: {}", e);
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ));
            }
        };

        // fuse 端传 dir_ino/parent 作为 shard_id，重映射到正确的 shard
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(req.shard_id);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self.meta_shard_manager.increment_open_count(req.inode) {
            Ok(count) => {
                let resp = powerfs_coherence::OpenCountResponse {
                    success: true,
                    open_count: count,
                    error: String::new(),
                };
                let body = serde_json::to_vec(&resp).unwrap_or_default();
                Ok(Self::build_response(msg, STATUS_OK, body))
            }
            Err(e) => {
                warn!("FILER_NET_OPEN_COUNT_INC failed: {}", e);
                let resp = powerfs_coherence::OpenCountResponse {
                    success: false,
                    open_count: 0,
                    error: e,
                };
                let body = serde_json::to_vec(&resp).unwrap_or_default();
                Ok(Self::build_response(msg, STATUS_ERR_SERVER_ERROR, body))
            }
        }
    }

    /// Phase 3.5.3: 处理 fuse 端 release/close 时上报的 open_count 递减请求。
    async fn handle_open_count_dec(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let req: powerfs_coherence::OpenCountRequest = match serde_json::from_slice(&msg.body) {
            Ok(r) => r,
            Err(e) => {
                warn!("FILER_NET_OPEN_COUNT_DEC: decode failed: {}", e);
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ));
            }
        };

        // fuse 端传 dir_ino/parent 作为 shard_id，重映射到正确的 shard
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(req.shard_id);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self.meta_shard_manager.decrement_open_count(req.inode) {
            Ok(count) => {
                let resp = powerfs_coherence::OpenCountResponse {
                    success: true,
                    open_count: count,
                    error: String::new(),
                };
                let body = serde_json::to_vec(&resp).unwrap_or_default();
                Ok(Self::build_response(msg, STATUS_OK, body))
            }
            Err(e) => {
                warn!("FILER_NET_OPEN_COUNT_DEC failed: {}", e);
                let resp = powerfs_coherence::OpenCountResponse {
                    success: false,
                    open_count: 0,
                    error: e,
                };
                let body = serde_json::to_vec(&resp).unwrap_or_default();
                Ok(Self::build_response(msg, STATUS_ERR_SERVER_ERROR, body))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// DeltaWire ↔ proto DeltaOp 转换（net_handler 专用）
// ---------------------------------------------------------------------------

use powerfs_coherence::{
    DeltaOpType, DeltaWire, DirEntryWire, EntryIdWire, SetAttrWire, VectorClockWire,
};

/// proto VectorClock → wire VectorClock
fn proto_to_wire_vclock(vc: &crate::powerfs::VectorClock) -> VectorClockWire {
    VectorClockWire {
        entries: vc
            .entries
            .iter()
            .map(|e| powerfs_coherence::VectorClockEntryWire {
                client_id: e.client_id,
                seq: e.seq,
            })
            .collect(),
    }
}

/// wire VectorClock → proto VectorClock
fn wire_to_proto_vclock(w: &VectorClockWire) -> crate::powerfs::VectorClock {
    crate::powerfs::VectorClock {
        entries: w
            .entries
            .iter()
            .map(|e| crate::powerfs::VectorClockEntry {
                client_id: e.client_id,
                seq: e.seq,
            })
            .collect(),
    }
}

/// proto DeltaOp → wire DeltaWire
fn proto_to_wire_delta(d: &crate::powerfs::DeltaOp) -> DeltaWire {
    use crate::powerfs::delta_op::Op;
    let op = match &d.op {
        Some(op) => op,
        None => {
            return DeltaWire {
                op_type: DeltaOpType::Add,
                vclock: VectorClockWire::default(),
                entry: None,
                entry_id: None,
                old_entry_id: None,
                setattr: None,
            }
        }
    };
    match op {
        Op::Add(e) => DeltaWire {
            op_type: DeltaOpType::Add,
            // 补全 vclock：用 entry 的 (client_id, seq) 构建，
            // 客户端 apply 后 vclock 推进，避免 puller 反复收到相同 delta。
            vclock: VectorClockWire {
                entries: if e.client_id != 0 {
                    vec![powerfs_coherence::VectorClockEntryWire {
                        client_id: e.client_id,
                        seq: e.seq,
                    }]
                } else {
                    Vec::new()
                },
            },
            entry: Some(DirEntryWire {
                name: e.name.clone(),
                client_id: e.client_id,
                seq: e.seq,
                inode: e.inode,
                generation: 0,
                file_type: 0,
                mode: e.mode,
                uid: 0,
                gid: 0,
                size: 0,
                mtime: 0,
                atime: 0,
                ctime: 0,
                nlink: 1,
                rdev: 0,
                parent_ino: e.parent_ino,
                chunks: vec![],
                symlink_target: None,
            }),
            entry_id: None,
            old_entry_id: None,
            setattr: None,
        },
        Op::Remove(id) => DeltaWire {
            op_type: DeltaOpType::Remove,
            vclock: VectorClockWire::default(),
            entry: None,
            entry_id: Some(EntryIdWire {
                name: id.name.clone(),
                client_id: 0,
                seq: 0,
                parent_ino: id.parent_ino,
            }),
            old_entry_id: None,
            setattr: None,
        },
        Op::Rename(r) => DeltaWire {
            op_type: DeltaOpType::Rename,
            vclock: VectorClockWire::default(),
            entry: Some(DirEntryWire {
                name: r.new_name.clone(),
                client_id: 0,
                seq: 0,
                inode: 0,
                generation: 0,
                file_type: 0,
                mode: 0,
                uid: 0,
                gid: 0,
                size: 0,
                mtime: 0,
                atime: 0,
                ctime: 0,
                nlink: 1,
                rdev: 0,
                parent_ino: r.new_parent_ino,
                chunks: vec![],
                symlink_target: None,
            }),
            entry_id: None,
            old_entry_id: Some(EntryIdWire {
                name: r.old_name.clone(),
                client_id: 0,
                seq: 0,
                parent_ino: r.old_parent_ino,
            }),
            setattr: None,
        },
        Op::SetAttr(s) => DeltaWire {
            op_type: DeltaOpType::SetAttr,
            vclock: VectorClockWire::default(),
            entry: None,
            entry_id: None,
            old_entry_id: None,
            setattr: Some(SetAttrWire {
                inode: s.inode,
                mode: None,
                uid: None,
                gid: None,
                size: if s.size > 0 { Some(s.size) } else { None },
                mtime: if s.mtime > 0 { Some(s.mtime) } else { None },
                nlink: None,
                chunks: s
                    .chunks
                    .iter()
                    .map(|c| ChunkWire {
                        offset: c.offset,
                        size: c.size,
                        mtime: c.mtime,
                        fid: c.fid.clone(),
                        cookie: c.cookie,
                        crc32: c.crc32,
                    })
                    .collect(),
            }),
        },
    }
}

/// wire DeltaWire → proto DeltaOp
fn wire_to_proto_delta(w: &DeltaWire) -> Option<crate::powerfs::DeltaOp> {
    use crate::powerfs::{
        delta_op::Op, DirEntryOrset, EntryId as ProtoEntryId, RenameOp, SetAttrOp,
    };
    let op = match w.op_type {
        DeltaOpType::Add => {
            let e = w.entry.as_ref()?;
            Op::Add(DirEntryOrset {
                parent_ino: e.parent_ino,
                name: e.name.clone(),
                inode: e.inode,
                mode: e.mode,
                seq: e.seq,
                client_id: e.client_id,
            })
        }
        DeltaOpType::Remove => {
            let id = w.entry_id.as_ref()?;
            Op::Remove(ProtoEntryId {
                parent_ino: id.parent_ino,
                name: id.name.clone(),
            })
        }
        DeltaOpType::Rename => {
            let old = w.old_entry_id.as_ref()?;
            let new = w.entry.as_ref()?;
            Op::Rename(RenameOp {
                old_parent_ino: old.parent_ino,
                old_name: old.name.clone(),
                new_parent_ino: new.parent_ino,
                new_name: new.name.clone(),
            })
        }
        DeltaOpType::SetAttr => {
            let s = w.setattr.as_ref()?;
            Op::SetAttr(SetAttrOp {
                inode: s.inode,
                size: s.size.unwrap_or(0),
                mtime: s.mtime.unwrap_or(0),
                chunks: s
                    .chunks
                    .iter()
                    .map(|c| crate::powerfs::FileChunk {
                        offset: c.offset,
                        size: c.size,
                        mtime: c.mtime,
                        fid: c.fid.clone(),
                        cookie: c.cookie,
                        crc32: c.crc32,
                    })
                    .collect(),
                extended: std::collections::HashMap::new(),
            })
        }
    };
    Some(crate::powerfs::DeltaOp { op: Some(op) })
}

#[async_trait::async_trait]
impl PowerFsNetHandler for FilerNetHandler {
    async fn handle_request(&self, client_id: u64, msg: &NetMessage) -> NetResult<NetMessage> {
        let msg_type = msg
            .msg_type()
            .ok_or_else(|| powerfs_net::NetError::Protocol("unknown message type".into()))?;

        debug!(
            "FILER_NET: handling request {:?}, client_id={}, seq={}",
            msg_type, client_id, msg.header.seq
        );

        match msg_type {
            MsgType::Lookup => self.handle_lookup(msg).await,
            MsgType::GetAttr => self.handle_getattr(msg).await,
            MsgType::SetAttr => self.handle_setattr(msg).await,
            MsgType::SetAttrData => self.handle_setattr_data(msg).await,
            MsgType::SetAttrMeta => self.handle_setattr_meta(msg).await,
            MsgType::Create => self.handle_create(msg).await,
            MsgType::Mkdir => self.handle_mkdir(msg).await,
            MsgType::Unlink => self.handle_unlink(msg).await,
            MsgType::Rmdir => self.handle_rmdir(msg).await,
            MsgType::Rename => self.handle_rename(msg).await,
            MsgType::ReadDir => self.handle_readdir(msg).await,
            MsgType::StatFs => self.handle_statfs(msg).await,
            MsgType::Symlink => self.handle_symlink(msg).await,
            MsgType::Readlink => self.handle_readlink(msg).await,
            MsgType::Link => self.handle_link(msg).await,
            MsgType::PushDelta => self.handle_push_delta(msg).await,
            MsgType::PullDelta => self.handle_pull_delta(msg).await,
            MsgType::AllocInodeBatch => self.handle_alloc_inode_batch(msg).await,
            MsgType::UpdateInodeSizeChunks => self.handle_update_inode_size_chunks(msg).await,
            MsgType::OpenCountInc => self.handle_open_count_inc(msg).await,
            MsgType::OpenCountDec => self.handle_open_count_dec(msg).await,
            // AssignVolumeV2 removed - volume assignment is handled by Master via MsgType::Assign
            MsgType::Ping => {
                let flags = FrameFlags::new(FrameFlags::RESPONSE);
                let header =
                    powerfs_net::FrameHeader::new(msg.header.msg_type, flags, msg.header.seq, 0)
                        .with_status(STATUS_OK);
                Ok(NetMessage::new(header))
            }
            _ => {
                warn!("FILER_NET: unsupported message type {:?}", msg_type);
                Err(powerfs_net::NetError::UnknownMsgType(msg_type.as_u16()))
            }
        }
    }

    async fn on_connect(&self, _client_id: u64, _client_type: ClientType) {
        info!(
            "FILER_NET: client connected, id={}, type={:?}",
            _client_id, _client_type
        );
    }
}

#[async_trait::async_trait]
impl ServerRequestHandler for FilerNetHandler {
    async fn handle(&self, ctx: &mut RequestContext, msg: &NetMessage) -> NetResult<NetMessage> {
        let msg_type = msg
            .msg_type()
            .ok_or_else(|| powerfs_net::NetError::Protocol("unknown message type".into()))?;

        debug!(
            "FILER_NET: handling request {:?}, trace={}, client_id={}, seq={}",
            msg_type,
            ctx.trace_id(),
            ctx.client.client_id,
            msg.header.seq
        );

        match msg_type {
            MsgType::Lookup => self.handle_lookup(msg).await,
            MsgType::GetAttr => self.handle_getattr(msg).await,
            MsgType::SetAttr => self.handle_setattr(msg).await,
            MsgType::SetAttrData => self.handle_setattr_data(msg).await,
            MsgType::SetAttrMeta => self.handle_setattr_meta(msg).await,
            MsgType::Create => self.handle_create(msg).await,
            MsgType::Mkdir => self.handle_mkdir(msg).await,
            MsgType::Unlink => self.handle_unlink(msg).await,
            MsgType::Rmdir => self.handle_rmdir(msg).await,
            MsgType::Rename => self.handle_rename(msg).await,
            MsgType::ReadDir => self.handle_readdir(msg).await,
            MsgType::StatFs => self.handle_statfs(msg).await,
            MsgType::Symlink => self.handle_symlink(msg).await,
            MsgType::Readlink => self.handle_readlink(msg).await,
            MsgType::Link => self.handle_link(msg).await,
            MsgType::PushDelta => self.handle_push_delta(msg).await,
            MsgType::PullDelta => self.handle_pull_delta(msg).await,
            MsgType::AllocInodeBatch => self.handle_alloc_inode_batch(msg).await,
            MsgType::UpdateInodeSizeChunks => self.handle_update_inode_size_chunks(msg).await,
            MsgType::OpenCountInc => self.handle_open_count_inc(msg).await,
            MsgType::OpenCountDec => self.handle_open_count_dec(msg).await,
            // AssignVolumeV2 removed - volume assignment is handled by Master via MsgType::Assign
            MsgType::Ping => {
                let flags = FrameFlags::new(FrameFlags::RESPONSE);
                let header =
                    powerfs_net::FrameHeader::new(msg.header.msg_type, flags, msg.header.seq, 0)
                        .with_status(STATUS_OK);
                Ok(NetMessage::new(header))
            }
            _ => {
                warn!("FILER_NET: unsupported message type {:?}", msg_type);
                Err(powerfs_net::NetError::UnknownMsgType(msg_type.as_u16()))
            }
        }
    }
}
