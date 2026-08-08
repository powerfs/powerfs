//! Filer Net Handler - Implements powerfs-net protocol for Filer metadata operations
//!
//! This module provides FilerNetHandler that processes powerfs-net metadata messages
//! using MetaShardManager, which is the authoritative metadata manager with sharded
//! storage, Raft consensus, and strong consistency metadata operations.

use crate::inode_lease_manager::InodeLeaseManager;
use crate::inode_notifier::InodeNotifier;
use crate::meta_shard_manager::{MetaShardManager, POSIX_ROOT_INODE};
use crate::raft_group_manager::ShardId;
use crate::shard_store::{FileType, InodeInfo};
use crate::shard_strategy::ShardStrategy;
use log::{debug, info, warn};
use powerfs_layout::codec::{encode_file_layout, FEATURE_CHUNK_LAYOUT_V2};
use powerfs_layout::encoding::{ChunkEncoding, ChunkRef};
use powerfs_layout::layout::FileLayout;
use powerfs_layout::placement::Placement;
use powerfs_layout::reliability::{CompressionState, Reliability, ReliabilityState};
use powerfs_net::serialize::{decode_setattr_req, EntryInfo, TlvDecoder, TlvEncoder};
use powerfs_net::{
    ClientType, FieldId, MsgType, NetError, NetHandler, NetMessage, NetResult, RequestContext,
    STATUS_ERR_NOT_FOUND, STATUS_ERR_REDIRECT, STATUS_ERR_SERVER_ERROR, STATUS_OK,
};
use protobuf::Message as ProtoMessage;
use std::sync::Arc;

/// Zone 运行时状态 (Filer 持有, 每个 Zone 独立 counter + volume 列表)
struct ZoneState {
    zone_id: u32,
    counter: std::sync::atomic::AtomicU64,
    volumes: Vec<powerfs_common::types::ZoneVolume>,
}

/// Filer Net Handler implementation
pub struct FilerNetHandler {
    pub meta_shard_manager: Arc<MetaShardManager>,
    pub shard_strategy: Arc<ShardStrategy>,
    /// Net port for powerfs-net protocol (used to construct redirect addresses)
    pub net_port: u16,
    /// Inode notification broadcaster (optional, for cache invalidation)
    pub inode_notifier: Option<Arc<InodeNotifier>>,
    /// Inode metadata lease manager (方案 A, Phase 2)
    pub inode_lease_mgr: Arc<InodeLeaseManager>,
    /// 该 Filer 拥有的所有 Zone (多 Zone 设计: 旧 + 新)
    /// 空 Vec = 未注册, 无法分配 needle_id
    zones: std::sync::RwLock<Vec<ZoneState>>,
    /// round-robin 选 Zone 的索引 (fetch_add % zones.len())
    zone_rr: std::sync::atomic::AtomicU32,
    /// P2.5: Inline 小文件全局阈值 (字节). 0 = 禁用 (默认, 保持 Flat 行为).
    /// 大于 0 时, handle_create 对新文件返回 Placement::Inline + 该 max_size,
    /// 跳过 Volume Server 分配. 父目录 `powerfs.inline` xattr 可覆盖此值.
    /// 上限 8KB (Placement::Inline 硬上限).
    inline_max_size: std::sync::atomic::AtomicU32,
}

/// P2.5: Inline 模式硬上限 (Placement::Inline 的 max_size 不可超过此值)
pub const INLINE_HARD_LIMIT: u32 = 8 * 1024;
/// P2.5: `powerfs.inline` xattr key (存于父目录 InodeInfo.extended)
pub const INLINE_XATTR_KEY: &str = "powerfs.inline";

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
            inode_lease_mgr: Arc::new(InodeLeaseManager::new()),
            zones: std::sync::RwLock::new(Vec::new()),
            zone_rr: std::sync::atomic::AtomicU32::new(0),
            inline_max_size: std::sync::atomic::AtomicU32::new(0),
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
            inode_lease_mgr: Arc::new(InodeLeaseManager::new()),
            zones: std::sync::RwLock::new(Vec::new()),
            zone_rr: std::sync::atomic::AtomicU32::new(0),
            inline_max_size: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// P2.5: 设置 Inline 小文件全局阈值 (字节). 0 = 禁用.
    /// 超过 INLINE_HARD_LIMIT (8KB) 的值会被截断到 8KB.
    /// 启用后, handle_create 对新文件返回 Placement::Inline, 跳过 Volume 分配.
    pub fn set_inline_max_size(&self, max_size: u32) {
        let capped = max_size.min(INLINE_HARD_LIMIT);
        self.inline_max_size
            .store(capped, std::sync::atomic::Ordering::SeqCst);
        info!(
            "FILER_INLINE: set inline_max_size={} (requested={}, hard_limit={})",
            capped, max_size, INLINE_HARD_LIMIT
        );
    }

    /// P2.5: 读取当前 Inline 全局阈值
    pub fn inline_max_size(&self) -> u32 {
        self.inline_max_size
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// P2.5: 决定新文件是否使用 Inline 模式及阈值.
    /// 优先级: 父目录 `powerfs.inline` xattr > 全局 inline_max_size 配置.
    /// 返回 Some(max_size) 表示 Inline 模式; None 表示 Flat (当前默认行为).
    ///
    /// xattr 值解析:
    /// - 4 字节 LE u32 → 该值作为 max_size (上限 8KB)
    /// - 空值 → 使用全局 inline_max_size (若 >0), 否则禁用
    /// - 其它长度 → 忽略 (容错)
    fn resolve_inline_max_size(&self, parent_ino: u64) -> Option<u32> {
        // 1. 父目录 xattr 覆盖
        if let Some(parent) = self.meta_shard_manager.get_inode(parent_ino) {
            if let Some(val) = parent.extended.get(INLINE_XATTR_KEY) {
                let parsed = match val.len() {
                    0 => self.inline_max_size(),
                    4 => u32::from_le_bytes([val[0], val[1], val[2], val[3]]),
                    _ => {
                        warn!(
                            "FILER_INLINE: parent {} xattr {} has unexpected len {}, ignoring",
                            parent_ino,
                            INLINE_XATTR_KEY,
                            val.len()
                        );
                        return None;
                    }
                };
                return if parsed == 0 {
                    None // 显式禁用
                } else {
                    Some(parsed.min(INLINE_HARD_LIMIT))
                };
            }
        }
        // 2. 全局配置
        let global = self.inline_max_size();
        if global > 0 {
            Some(global)
        } else {
            None
        }
    }

    /// 设置 Zone 列表 (从 Master RegisterFiler 获取后调用)
    /// 替换所有已有 Zone, 保留已有 Zone 的 counter (若 zone_id 匹配)
    pub fn set_zones(&self, new_zones: Vec<powerfs_common::types::ZoneInfo>) {
        let mut zones = self.zones.write().unwrap();

        // 保留已有 Zone 的 counter (若 zone_id 匹配), 避免 set_zones 重置 counter
        let mut updated = Vec::with_capacity(new_zones.len());
        for zi in new_zones {
            let preserved_counter = zones
                .iter()
                .find(|z| z.zone_id == zi.zone_id)
                .map(|z| z.counter.load(std::sync::atomic::Ordering::SeqCst))
                .unwrap_or(0);

            updated.push(ZoneState {
                zone_id: zi.zone_id,
                counter: std::sync::atomic::AtomicU64::new(preserved_counter),
                volumes: zi.physical_volumes.clone(),
            });
        }

        let zone_ids: Vec<u32> = updated.iter().map(|z| z.zone_id).collect();
        let total_vols: usize = updated.iter().map(|z| z.volumes.len()).sum();
        info!(
            "FILER_ZONE: set_zones count={}, zone_ids={:?}, total_volumes={}",
            updated.len(),
            zone_ids,
            total_vols
        );

        *zones = updated;
    }

    /// 设置指定 Zone 的 needle_id counter (用于 recover_counter)
    pub fn set_zone_counter(&self, zone_id: u32, counter: u64) {
        let zones = self.zones.read().unwrap();
        if let Some(z) = zones.iter().find(|z| z.zone_id == zone_id) {
            z.counter
                .store(counter, std::sync::atomic::Ordering::SeqCst);
            info!("FILER_ZONE: set zone_id={} counter={}", zone_id, counter);
        } else {
            warn!(
                "FILER_ZONE: set_zone_counter zone_id={} not found (zones={})",
                zone_id,
                zones.len()
            );
        }
    }

    /// 返回所有 Zone 的 zone_id 列表 (用于 recover_counter, P2.5)
    pub fn get_zones(&self) -> Vec<u32> {
        let zones = self.zones.read().unwrap();
        zones.iter().map(|z| z.zone_id).collect()
    }

    /// 为新文件分配 needle_id + 选 volume (多 Zone round-robin)
    /// 返回 (volume_id, needle_id)
    fn alloc_for_new_file(&self) -> Option<(u64, u64)> {
        let zones = self.zones.read().unwrap();
        if zones.is_empty() {
            warn!("FILER_ZONE: no zones registered, cannot allocate needle_id");
            return None;
        }

        // round-robin 选 Zone: zone_rr.fetch_add(1) % zones.len()
        let rr = self
            .zone_rr
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let idx = (rr as usize) % zones.len();
        let zone = &zones[idx];

        // 从选中 Zone 的 counter 分配 needle_id
        let needle_id = crate::zone_client::alloc_needle_id(zone.zone_id, &zone.counter);

        // 从该 Zone 的 volumes 中选空闲比例最大的
        let vol = crate::zone_client::select_volume(&zone.volumes)?;
        let volume_id = vol.volume_id;

        info!(
            "FILER_ZONE: allocated needle_id={:#x} (zone={}, counter={}, rr_idx={}) volume_id={}",
            needle_id,
            zone.zone_id,
            powerfs_common::types::needle_counter(needle_id),
            idx,
            volume_id
        );

        Some((volume_id, needle_id))
    }

    /// Notify subscribers that an inode's metadata has changed.
    /// This is called after successful metadata mutations.
    fn notify_inode_change(&self, inode: u64, version: u64) {
        if let Some(ref notifier) = self.inode_notifier {
            let notifier = notifier.clone();
            let sub_count = notifier.subscriber_count(inode);
            info!(
                "FILER_NET_NOTIFY: inode={}, version={}, subscribers={}",
                inode, version, sub_count
            );
            tokio::spawn(async move {
                let count = notifier.notify(inode, version);
                info!(
                    "FILER_NET_NOTIFY: notified {} clients about inode {} change (v={})",
                    count, inode, version
                );
            });
        }
    }

    /// Build a response message
    fn build_response(msg: &NetMessage, status: u16, body: Vec<u8>) -> NetMessage {
        NetMessage::response(msg, status, body, Vec::new())
    }

    // MsgType::Read (0x0020) and MsgType::Write (0x0021) handlers removed.
    //
    // 数据读写不再经过 Filer — 客户端 (FUSE/内核) 直连 Volume Server:
    //   - 写: WriteNeedle (0x0062) / BatchWriteNeedle (0x0065) → Volume Server
    //   - 读: ReadNeedle (0x0063) / ReadNeedleBlob (0x0066) → Volume Server
    //
    // Filer 只负责元数据 (Lookup/GetAttr/Create/SetAttr 等) 和 chunk 映射管理.
    // 客户端从 GetAttr 响应的 Chunks 字段获取 (volume_id, needle_id) 后直连 volume.
    //
    // 旧的 handle_write/handle_read 将数据存入 Filer Raft 日志 (write_file_data),
    // 违反了 Filer=元数据 / Volume=数据 的架构分离, 已删除.

    /// Check if current node is the leader for the given shard.
    /// Returns Ok(()) if leader, or Err(redirect_response) if not.
    ///
    /// When leader status is unknown (e.g., Raft election in progress after
    /// filer restart), returns REDIRECT to the current node instead of
    /// SERVER_ERROR. This maps to -EAGAIN (retryable) in the kernel, allowing
    /// the VFS layer to retry the operation after election completes.
    /// Returning SERVER_ERROR would map to -EREMOTEIO (permanent), causing
    /// write failures during the brief election window.
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
                // Leader status unknown (Raft election in progress).
                // Return REDIRECT to self so the kernel retries with -EAGAIN
                // instead of -EREMOTEIO (permanent error). The disconnect +
                // reconnect cycle in the kernel's redirect handling provides
                // natural backoff; after election completes, the next request
                // will succeed or redirect to the real leader.
                let self_grpc_addr = self.meta_shard_manager.get_node_grpc_address();
                let net_addr = Self::grpc_addr_to_net_addr(&self_grpc_addr, self.net_port);
                warn!(
                    "Leader status unknown for shard {}, returning redirect to self {} (election in progress)",
                    shard_id.0, net_addr
                );
                let mut enc = TlvEncoder::new();
                let _ = enc.add_string(FieldId::Owner, &net_addr);
                Err(Self::build_response(
                    msg,
                    STATUS_ERR_REDIRECT,
                    enc.into_bytes(),
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

    /// 将 InodeInfo 的数据布局序列化为二进制 FileLayout TLV 字段。
    ///
    /// 使用 `powerfs_layout::encode_file_layout` 编码 (FEATURE_CHUNK_LAYOUT_V2),
    /// 输出 Placement + Reliability + ChunkEncoding 的二进制 TLV 字段,
    /// 替代旧版 JSON `FieldId::Chunks` 编码 (已废弃, 不再向后兼容).
    ///
    /// 构建 FileLayout 策略 (P2.5 Inline 支持):
    /// - **Inline 模式** (info.inline_data = Some): Placement::Inline + ChunkEncoding::InlineData
    ///   数据直接在响应中, 客户端一次 RPC 拿全 (GETATTR/LOOKUP), 无需 Volume Server.
    /// - **Flat 模式** (info.inline_data = None): Placement::Flat + ChunkEncoding::PerChunk
    ///   常规文件, 完整 chunk 列表, 客户端按 chunk 直连 Volume Server 读写.
    fn encode_chunks_fields(enc: &mut TlvEncoder, info: &InodeInfo) -> Result<(), NetError> {
        // P2.5: Inline 模式 — 数据直接存 Filer 元数据, 响应携带 inline_data
        if let Some(data) = &info.inline_data {
            let max_size = (INLINE_HARD_LIMIT).max(data.len() as u32);
            let layout = FileLayout {
                placement: Placement::Inline { max_size },
                reliability: Reliability::SingleReplica,
                reliability_state: ReliabilityState::default(),
                compression: CompressionState::default(),
                encoding: ChunkEncoding::InlineData { data: data.clone() },
            };
            encode_file_layout(enc, &layout, FEATURE_CHUNK_LAYOUT_V2)
                .map_err(|e| NetError::Protocol(format!("encode_file_layout failed: {}", e)))?;
            return Ok(());
        }

        // Flat 模式 — chunk 列表
        let chunks: Vec<ChunkRef> = info
            .chunks
            .iter()
            .map(|c| ChunkRef {
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
            encoding: ChunkEncoding::PerChunk { chunks },
        };

        encode_file_layout(enc, &layout, FEATURE_CHUNK_LAYOUT_V2)
            .map_err(|e| NetError::Protocol(format!("encode_file_layout failed: {}", e)))?;
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
                    "FILER_NET_LOOKUP: returning ino={}, mode={:o}, is_dir={}, name={}, size={}, chunks={}",
                    entry_info.ino, entry_info.mode, entry_info.is_dir, entry_info.name, entry_info.size, info.chunks.len()
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
                    "FILER_NET_GETATTR: returned info for ino={}, name={}, size={}, chunks={}",
                    ino,
                    entry_info.name,
                    entry_info.size,
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
        // Use decode_setattr_req which correctly handles optional fields via
        // while-loop parsing. Previously used fixed-order next_u64 which
        // desynced the decoder (encoder uses add_u32 for Mode/Uid/Gid, and
        // optional fields may be absent).
        let (ino, mode, uid, gid, size) = match decode_setattr_req(&msg.body) {
            Ok(v) => v,
            Err(e) => {
                warn!("FILER_NET_SETATTR: decode failed: {}", e);
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ));
            }
        };
        let mode = mode.map(|m| m as u64);
        let uid = uid.map(|u| u as u64);
        let gid = gid.map(|g| g as u64);

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
            Ok(_) => {
                // File data truncate is handled in ShardStore::setattr
                // (Raft-replicated), so no need to truncate inline_data here.
                // Notify other clients that this inode's metadata (and
                // possibly size) changed. Without this, truncate operations
                // via SetAttr are invisible to other clients' cached metadata
                // until TTL expiry, causing stale reads.
                let now = chrono::Utc::now().timestamp() as u64;
                self.notify_inode_change(ino, now);
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
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
        // Use while-loop parsing since fields are optional (encoder skips
        // None values). Previously used fixed-order next_u64 which desynced
        // the decoder when optional fields were absent.
        let mut dec = TlvDecoder::new(&msg.body);
        let mut ino = 0u64;
        let mut mode: Option<u64> = None;
        let mut uid: Option<u64> = None;
        let mut gid: Option<u64> = None;
        let mut mtime: Option<u64> = None;
        let mut atime: Option<u64> = None;
        let mut client_id = String::new();
        let mut timestamp = 0u64;

        while let Some((field, length)) = dec.next_field() {
            match field {
                FieldId::Ino => ino = dec.read_u64(length).unwrap_or(0),
                FieldId::Mode => mode = Some(dec.read_u64(length).unwrap_or(0)),
                FieldId::Uid => uid = Some(dec.read_u64(length).unwrap_or(0)),
                FieldId::Gid => gid = Some(dec.read_u64(length).unwrap_or(0)),
                FieldId::Mtime => mtime = Some(dec.read_u64(length).unwrap_or(0)),
                FieldId::Atime => atime = Some(dec.read_u64(length).unwrap_or(0)),
                FieldId::ClientId => {
                    client_id = dec.read_string(length).unwrap_or_default().to_string()
                }
                FieldId::Seq => timestamp = dec.read_u64(length).unwrap_or(0),
                _ => {
                    let _ = dec.skip(length);
                }
            }
        }

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
    ///
    /// Zone-based needle_id allocation (P2.4):
    ///   - Filer 自分配 needle_id = (zone_id << 40) | counter++
    ///   - Filer 自选物理 volume (从 zone 映射列表中选空闲比例最大的)
    ///   - 客户端不再传入 fid/cookie/offset, 由 Filer 完全自治
    ///   - 通过 set_chunks 持久化 chunk 映射 (Raft 强一致)
    ///   - 响应返回 VolumeId + FileKey 给客户端, 客户端直连 volume 读写
    async fn handle_create(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();
        let mode = dec.next_u32(FieldId::Mode).unwrap_or(0o644) as u64;
        let uid = dec.next_u32(FieldId::Uid).unwrap_or(0) as u64;
        let gid = dec.next_u32(FieldId::Gid).unwrap_or(0) as u64;

        // 客户端可能仍传入旧的 fid/cookie/offset 字段 (向后兼容), 但 Filer 忽略它们,
        // 改由 Zone 自分配. 读取后丢弃, 避免影响后续字段解析.
        let _ = dec.next_string(FieldId::Fid).ok();
        let _ = dec.next_u64(FieldId::Cookie).ok();
        let _ = dec.next_u64(FieldId::FileKey).ok();
        let _ = dec.next_u64(FieldId::Size).ok();

        info!(
            "FILER_NET_CREATE: parent_ino={}, name={}, mode={:o}, uid={}, gid={}",
            parent_ino, name, mode, uid, gid
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

        // === P2.5: 决定 Inline vs Flat ===
        // Inline: 数据直接存 Filer 元数据, 绕过 Volume Server (微小文件 < max_size).
        // Flat: 走原 Volume 分配路径.
        let inline_max = self.resolve_inline_max_size(parent_ino);

        // === Flat 模式: Zone 自分配 needle_id + volume_id (P2.4 核心) ===
        // 在创建 inode 之前分配, 这样若分配失败 (zone 未注册) 可直接返回错误,
        // 不会留下空 inode. 若分配成功但后续 create_file 失败, needle_id 会被
        // "泄漏" (counter 已自增但无 chunk 映射), 这是可接受的:
        //   - needle_id 空间巨大 (40 bits/zone, 1 万亿个)
        //   - counter 单调递增, 不会重复
        //
        // Inline 模式跳过分配 (无 Volume Server 参与).
        let flat_alloc = if inline_max.is_none() {
            match self.alloc_for_new_file() {
                Some(v) => Some(v),
                None => {
                    warn!(
                        "FILER_NET_CREATE: zone not registered (zone_id=0), cannot allocate needle_id"
                    );
                    return Ok(Self::build_response(
                        msg,
                        STATUS_ERR_SERVER_ERROR,
                        Vec::new(),
                    ));
                }
            }
        } else {
            None
        };

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

                // B5: notify 目录条目变更（parent readdir 缓存 + 新 inode）
                let now = crate::shard_store::ShardStore::current_time();
                self.notify_inode_change(parent_ino, now);
                self.notify_inode_change(ino, now);

                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, ino);
                enc.add_u32(FieldId::Mode, mode as u32);
                enc.add_string(FieldId::Name, &name)?;

                if let Some(max_size) = inline_max {
                    // === P2.5 Inline 模式 ===
                    // 不分配 volume/needle, 不持久化 chunk 映射. 客户端 CLOSE 时
                    // 把 inline_data 发 Filer (handle_update_inode_size_chunks),
                    // 由 Filer 单次 Raft 提交 (数据 + 元数据).
                    info!(
                        "FILER_NET_CREATE: inline mode inode={} max_size={}",
                        ino, max_size
                    );
                    let layout = FileLayout {
                        placement: Placement::Inline { max_size },
                        reliability: Reliability::SingleReplica,
                        reliability_state: ReliabilityState::default(),
                        compression: CompressionState::default(),
                        // 创建时尚无数据, 客户端 CLOSE 时携带 inline_data
                        encoding: ChunkEncoding::InlineData { data: Vec::new() },
                    };
                    encode_file_layout(&mut enc, &layout, FEATURE_CHUNK_LAYOUT_V2).map_err(
                        |e| NetError::Protocol(format!("encode_file_layout failed: {}", e)),
                    )?;
                } else {
                    // === Flat 模式: 持久化 chunk 映射 (volume_id, needle_id) via Raft ===
                    // fid 格式 "volume_id,cookie,needle_id": set_chunks 从第 3 字段解析 needle_id
                    // offset=0: 新文件首 chunk 的字节偏移 (chunk.offset, 非 needle_id)
                    // size=0: 初始无数据, 后续 write 时更新
                    let (volume_id, needle_id) = flat_alloc.unwrap();
                    let fid_str = format!("{},0,{}", volume_id, needle_id);
                    if let Err(e) = self
                        .meta_shard_manager
                        .set_chunks(ino, shard_id, fid_str, volume_id, 0, 0, 0)
                        .await
                    {
                        warn!(
                            "FILER_NET_CREATE: set_chunks failed for inode {} (needle_id={:#x}): {}",
                            ino, needle_id, e
                        );
                        // set_chunks 失败不回滚 inode 创建 (inode 已 Raft 持久化).
                        // needle_id 已分配但 chunk 映射缺失, 客户端写入会失败并重试.
                        // 后续 GC 会清理无 chunk 的 inode.
                    }
                    // === 返回 volume_id + needle_id 给客户端 (直连 volume 读写) ===
                    enc.add_u64(FieldId::VolumeId, volume_id);
                    enc.add_u64(FieldId::FileKey, needle_id);
                }

                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!(
                    "FILER_NET_CREATE failed: {}{}",
                    e,
                    flat_alloc
                        .map(|(_, n)| format!(" (needle_id={:#x} leaked, acceptable)", n))
                        .unwrap_or_default()
                );
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
        // encode_mkdir_req uses add_u64 for Mode/Uid/Gid (unlike
        // encode_create_req which uses add_u32). Decoder must match.
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
                enc.add_u8(FieldId::IsDir, 1);
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
                // Directory entries live in the shard of parent_inode.
                // Use parent_ino (not info.inode) for shard calculation so
                // the delete targets the correct shard. For hardlinks,
                // info.inode's stored parent/name may differ from the actual
                // entry being unlinked, so we must pass parent_ino/name
                // directly to delete_file instead of using delete_file_by_inode.
                let shard_id = self.shard_strategy.calculate_shard(parent_ino);

                // Check leader - redirect write requests to the correct leader
                if let Err(redirect) = self.check_leader(msg, shard_id).await {
                    warn!(
                        "FILER_NET_UNLINK: not leader for shard {}, redirecting",
                        shard_id.0
                    );
                    return Ok(redirect);
                }

                match self.meta_shard_manager.delete_file(parent_ino, &name).await {
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

        // Check leader for the source shard (rename operates on old_parent's shard).
        // Cross-shard rename returns an error in meta_shard_manager, so only
        // old_parent's shard leader is checked here.
        let shard_id = self.shard_strategy.calculate_shard(old_parent_ino);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_RENAME: not leader for shard {}, redirecting",
                shard_id.0
            );
            return Ok(redirect);
        }

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

        info!(
            "FILER_NET_READDIR: parent_ino={}, entries={}",
            parent_ino,
            entries
                .iter()
                .map(|e| e.name.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );

        // Filter by last_name for pagination
        let filtered: Vec<&InodeInfo> = if last_name.is_empty() {
            entries.iter().collect()
        } else {
            entries
                .iter()
                .filter(|e| e.name.as_str() > last_name.as_str())
                .collect()
        };

        // has_more is true only when there are entries beyond the `limit` window.
        // Previous logic `(limited.len() < limit) && !entries.is_empty()` was always
        // true for non-empty dirs (since limited.len() <= limit always), causing the
        // kernel to loop forever sending READDIR with the same last_name cursor.
        let has_more = filtered.len() > limit as usize;
        let limited: Vec<&InodeInfo> = filtered.into_iter().take(limit as usize).collect();

        let mut enc = TlvEncoder::new();
        enc.add_u32(FieldId::Count, limited.len() as u32);
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

        let shard_id = self.shard_strategy.calculate_shard(parent_ino);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_SYMLINK: not leader for shard {}, redirecting",
                shard_id.0
            );
            return Ok(redirect);
        }

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

        let shard_id = self.shard_strategy.calculate_shard(ino);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_READLINK: not leader for shard {}, redirecting",
                shard_id.0
            );
            return Ok(redirect);
        }

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

        // Hard link creates a directory entry in new_parent's shard, so check
        // leader for new_parent's shard.
        let shard_id = self.shard_strategy.calculate_shard(new_parent_ino);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_LINK: not leader for shard {}, redirecting",
                shard_id.0
            );
            return Ok(redirect);
        }

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

    /// handle_alloc_inode_batch：批量授权 inode 预留段
    ///
    /// TLV 编码: Request = ShardId + Count + ClientId
    ///           Response = StartInode + EndInode (成功) / Name=error (失败)
    async fn handle_alloc_inode_batch(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let shard_id_raw = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let count = dec.next_u32(FieldId::Count).unwrap_or(0);
        let _client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();

        // fuse 端传 dir_ino/parent 作为 shard_id，重映射到正确的 shard
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(shard_id_raw);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .alloc_inode_batch(shard_id, count)
            .await
        {
            Ok((start, end)) => {
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::StartInode, start);
                enc.add_u64(FieldId::EndInode, end);
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_ALLOC_INODE failed: {}", e);
                let mut enc = TlvEncoder::new();
                let _ = enc.add_string(FieldId::Name, &e);
                Ok(Self::build_response(msg, STATUS_ERR_SERVER_ERROR, enc.into_bytes()))
            }
        }
    }

    /// handle_update_inode_size_chunks：close 时强一致 sync 账本
    ///
    /// TLV 编码 (替代 JSON):
    /// - Request body: ShardId + Ino + Size + ClientId + FileLayout (chunks 二进制 TLV)
    /// - Response: STATUS_OK + 空 body (成功) / STATUS_ERR + FieldId::Name=error (失败)
    async fn handle_update_inode_size_chunks(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);

        let shard_id_raw = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let size = dec.next_u64(FieldId::Size).unwrap_or(0);
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();

        // 解码 FileLayout (chunks), 无 layout 字段时返回空 chunks
        let layout = powerfs_layout::codec::decode_file_layout(&mut dec).unwrap_or(FileLayout {
            placement: Placement::Flat,
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::default(),
            compression: CompressionState::default(),
            encoding: ChunkEncoding::PerChunk { chunks: Vec::new() },
        });

        let chunks: Vec<crate::shard_store::StoredFileChunk> = match &layout.encoding {
            ChunkEncoding::PerChunk { chunks } => chunks
                .iter()
                .map(|c| crate::shard_store::StoredFileChunk {
                    offset: c.offset,
                    size: c.size,
                    mtime: c.mtime,
                    needle_id: c.needle_id,
                    volume_id: c.volume_id,
                    crc32: c.crc32,
                })
                .collect(),
            _ => Vec::new(),
        };

        // P2.5: Inline 小文件 close 路径. 客户端在 CLOSE 时把 inline_data
        // (≤ 8KB) 直接发 Filer, 由 Filer 单次 Raft 提交 (数据 + 元数据),
        // 绕过 Volume Server. 当 layout.encoding 为 InlineData 时提取数据;
        // 也兼容客户端把 InlineData 作为独立 FieldId::InlineData 字段发送.
        let inline_data: Option<Vec<u8>> = match &layout.encoding {
            ChunkEncoding::InlineData { data } => Some(data.clone()),
            _ => dec.next_bytes(FieldId::InlineData).ok(),
        };

        // 安全检查: inline_data 不应超过 8KB (Placement::Inline 的硬上限)
        if let Some(d) = &inline_data {
            if d.len() > 8 * 1024 {
                warn!(
                    "FILER_NET_UPDATE_SIZE_CHUNKS: inline_data too large ({} bytes > 8KB) for inode {}, rejecting",
                    d.len(), inode
                );
                let mut enc = TlvEncoder::new();
                let _ = enc.add_string(
                    FieldId::Name,
                    &format!("inline_data too large: {} bytes", d.len()),
                );
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    enc.into_bytes(),
                ));
            }
        }

        info!(
            "FILER_NET_UPDATE_SIZE_CHUNKS: shard_id={}, inode={}, size={}, chunks={}, inline={}, client={}",
            shard_id_raw, inode, size, chunks.len(), inline_data.as_ref().map(|d| d.len()).unwrap_or(0), client_id
        );

        // fuse 端传 dir_ino 作为 shard_id，重映射到正确的 shard
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(shard_id_raw);
        info!(
            "FILER_NET_UPDATE_SIZE_CHUNKS: calculated shard_id={}, is_leader_check",
            shard_id.0
        );
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .update_inode_size_chunks_atomic(shard_id, inode, size, chunks, inline_data)
            .await
        {
            Ok(_) => {
                // Phase 2: notify subscribers that this inode's content
                // (size/chunks) changed so they can evict stale cache
                // entries. Without this, a second client that previously
                // looked up the file would keep serving the old content
                // until the 30s TTL expires.
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                self.notify_inode_change(inode, now);
                // 成功: STATUS_OK + 空 body
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!("FILER_NET_UPDATE_SIZE_CHUNKS failed: {}", e);
                // 失败: STATUS_ERR + FieldId::Name = error string
                let mut enc = TlvEncoder::new();
                let _ = enc.add_string(FieldId::Name, &e);
                Ok(Self::build_response(msg, STATUS_ERR_SERVER_ERROR, enc.into_bytes()))
            }
        }
    }

    /// Phase 3.5.3: 处理 fuse 端 open 时上报的 open_count 递增请求。
    ///
    /// TLV 编码: Request = ShardId + Ino
    ///           Response = OpenCount (成功) / Name=error (失败)
    async fn handle_open_count_inc(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let shard_id_raw = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);

        // fuse 端传 dir_ino/parent 作为 shard_id，重映射到正确的 shard
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(shard_id_raw);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self.meta_shard_manager.increment_open_count(inode) {
            Ok(count) => {
                let mut enc = TlvEncoder::new();
                enc.add_u32(FieldId::OpenCount, count);
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_OPEN_COUNT_INC failed: {}", e);
                let mut enc = TlvEncoder::new();
                let _ = enc.add_string(FieldId::Name, &e);
                Ok(Self::build_response(msg, STATUS_ERR_SERVER_ERROR, enc.into_bytes()))
            }
        }
    }

    /// Phase 3.5.3: 处理 fuse 端 release/close 时上报的 open_count 递减请求。
    ///
    /// TLV 编码: Request = ShardId + Ino
    ///           Response = OpenCount (成功) / Name=error (失败)
    async fn handle_open_count_dec(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let shard_id_raw = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);

        // fuse 端传 dir_ino/parent 作为 shard_id，重映射到正确的 shard
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(shard_id_raw);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self.meta_shard_manager.decrement_open_count(inode) {
            Ok(count) => {
                let mut enc = TlvEncoder::new();
                enc.add_u32(FieldId::OpenCount, count);
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_OPEN_COUNT_DEC failed: {}", e);
                let mut enc = TlvEncoder::new();
                let _ = enc.add_string(FieldId::Name, &e);
                Ok(Self::build_response(msg, STATUS_ERR_SERVER_ERROR, enc.into_bytes()))
            }
        }
    }

    /// P2.5c: handle_migrate_inline_alloc — Inline → Flat 迁移分配.
    ///
    /// 客户端 write 累计超 max_size×1.5 时调用. Filer 仅分配 (volume_id,
    /// needle_id), **不修改 inode 元数据** (保留 inline_data 用于 crash
    /// safety). 客户端拿到分配后把数据放入 chunk_cache, close 时 flush +
    /// sync_size_chunks_on_close 原子完成切换 (inline_data=None + Flat chunks).
    ///
    /// crash safety: 若客户端在分配后崩溃, Filer 仍有 inline_data, 文件仍可
    /// 作为 Inline 读; 分配的 needle_id 泄漏 (可接受, 同 CREATE 失败).
    ///
    /// TLV 编码: Request = ShardId + Ino
    ///           Response = VolumeId + FileKey(needle_id) / Name=error
    async fn handle_migrate_inline_alloc(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let shard_id_raw = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);

        info!(
            "FILER_NET_MIGRATE_INLINE_ALLOC: shard_id={}, inode={}",
            shard_id_raw, inode
        );

        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(shard_id_raw);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        // best-effort: 校验 inode 存在且为 Inline 模式 (有 inline_data).
        // 不强制失败 — 若已被其他客户端迁移, 仍分配 (容错).
        if let Some(info) = self.meta_shard_manager.get_inode(inode) {
            if info.inline_data.is_none() && info.chunks.is_empty() {
                warn!(
                    "FILER_NET_MIGRATE_INLINE_ALLOC: inode {} has no inline_data and no chunks — \
                     may already be migrated or never written, allocating anyway",
                    inode
                );
            }
        } else {
            warn!(
                "FILER_NET_MIGRATE_INLINE_ALLOC: inode {} not found, returning error",
                inode
            );
            let mut enc = TlvEncoder::new();
            let _ = enc.add_string(FieldId::Name, &format!("inode {} not found", inode));
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_NOT_FOUND,
                enc.into_bytes(),
            ));
        }

        // 分配 (volume_id, needle_id) — 同 CREATE Flat 路径
        let (volume_id, needle_id) = match self.alloc_for_new_file() {
            Some(v) => v,
            None => {
                warn!(
                    "FILER_NET_MIGRATE_INLINE_ALLOC: zone not registered, cannot allocate"
                );
                let mut enc = TlvEncoder::new();
                let _ = enc.add_string(FieldId::Name, "zone not registered");
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    enc.into_bytes(),
                ));
            }
        };

        info!(
            "FILER_NET_MIGRATE_INLINE_ALLOC: inode={} allocated volume_id={}, needle_id={:#x} \
             (inode NOT modified, inline_data preserved for crash safety)",
            inode, volume_id, needle_id
        );

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::VolumeId, volume_id);
        enc.add_u64(FieldId::FileKey, needle_id);
        Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
    }

    /// Handle AcquireInodeLease request (方案 A, Phase 2).
    ///
    /// Request TLV: Ino, ClientId, LeaseDuration
    /// Response TLV: LeaseId (token), LeaseDuration
    async fn handle_acquire_inode_lease(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let duration_ms = dec.next_u64(FieldId::LeaseDuration).unwrap_or(30000);

        if inode == 0 || client_id.is_empty() {
            warn!(
                "FILER_NET_ACQUIRE_INODE_LEASE: missing inode={} or client_id={}",
                inode, client_id
            );
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                Vec::new(),
            ));
        }

        // Route to shard leader (inode lease is in-memory on the leader)
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(inode);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self.inode_lease_mgr.acquire(inode, &client_id, duration_ms) {
            Ok(result) => {
                let mut enc = TlvEncoder::new();
                let _ = enc.add_string(FieldId::LeaseId, &result.token);
                let _ = enc.add_u64(FieldId::LeaseDuration, result.expire_at_ms);
                info!(
                    "FILER_NET_ACQUIRE_INODE_LEASE: inode={} client={} duration_ms={}",
                    inode, client_id, duration_ms
                );
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!(
                    "FILER_NET_ACQUIRE_INODE_LEASE: failed inode={} client={}: {}",
                    inode, client_id, e
                );
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    e.into_bytes(),
                ))
            }
        }
    }

    /// Handle ReleaseInodeLease request (方案 A, Phase 2).
    ///
    /// Request TLV: Ino, ClientId, LeaseToken
    /// Response: STATUS_OK / STATUS_ERR_SERVER_ERROR
    async fn handle_release_inode_lease(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();

        if inode == 0 || client_id.is_empty() || token.is_empty() {
            warn!(
                "FILER_NET_RELEASE_INODE_LEASE: missing inode={} client_id={} token={}",
                inode,
                client_id,
                !token.is_empty()
            );
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                Vec::new(),
            ));
        }

        // Route to shard leader
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(inode);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self.inode_lease_mgr.release(inode, &client_id, &token) {
            Ok(()) => {
                debug!(
                    "FILER_NET_RELEASE_INODE_LEASE: inode={} client={}",
                    inode, client_id
                );
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!(
                    "FILER_NET_RELEASE_INODE_LEASE: failed inode={} client={}: {}",
                    inode, client_id, e
                );
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    e.into_bytes(),
                ))
            }
        }
    }

    /// Handle RenewInodeLease request (方案 A, Phase 2).
    ///
    /// Request TLV: Ino, ClientId, LeaseToken, LeaseDuration
    /// Response: STATUS_OK / STATUS_ERR_SERVER_ERROR
    async fn handle_renew_inode_lease(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();
        let duration_ms = dec.next_u64(FieldId::LeaseDuration).unwrap_or(30000);

        if inode == 0 || client_id.is_empty() || token.is_empty() {
            warn!(
                "FILER_NET_RENEW_INODE_LEASE: missing inode={} client_id={} token={}",
                inode,
                client_id,
                !token.is_empty()
            );
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                Vec::new(),
            ));
        }

        // Route to shard leader
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(inode);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self
            .inode_lease_mgr
            .renew(inode, &client_id, &token, duration_ms)
        {
            Ok(()) => {
                debug!(
                    "FILER_NET_RENEW_INODE_LEASE: inode={} client={} duration_ms={}",
                    inode, client_id, duration_ms
                );
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!(
                    "FILER_NET_RENEW_INODE_LEASE: failed inode={} client={}: {}",
                    inode, client_id, e
                );
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    e.into_bytes(),
                ))
            }
        }
    }

    /// Handle Raft inter-node message (MsgType::RaftMessage).
    ///
    /// Replaces the gRPC FilerMetaService::send_raft_message RPC.
    /// Decodes the TLV body (ShardId + RaftPayload), deserializes the
    /// protobuf eraftpb::Message, and steps it into the local Raft group.
    ///
    /// Request TLV:
    ///   ShardId    = shard id (u64)
    ///   RaftPayload = serialized eraftpb::Message (bytes)
    /// Response: STATUS_OK (empty body) or STATUS_ERR_SERVER_ERROR (error string).
    async fn handle_raft_message(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let shard_id_val = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let payload = dec.next_bytes(FieldId::RaftPayload).unwrap_or_default();

        if payload.is_empty() {
            warn!(
                "FILER_RAFT: received empty RaftPayload for shard {}",
                shard_id_val
            );
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                b"empty raft payload".to_vec(),
            ));
        }

        let mut raft_msg = raft::eraftpb::Message::new();
        if let Err(e) = ProtoMessage::merge_from_bytes(&mut raft_msg, &payload) {
            warn!("FILER_RAFT: failed to deserialize Raft message: {}", e);
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                format!("failed to deserialize: {}", e).into_bytes(),
            ));
        }

        let shard_id = ShardId(shard_id_val);
        debug!(
            "FILER_RAFT: received raft message for shard {} (type={:?})",
            shard_id.0, raft_msg.msg_type
        );

        match self
            .meta_shard_manager
            .step_raft_message(shard_id, raft_msg)
            .await
        {
            Ok(_) => {
                debug!("FILER_RAFT: stepped raft message for shard {}", shard_id.0);
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!(
                    "FILER_RAFT: step_raft_message failed for shard {}: {}",
                    shard_id.0, e
                );
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    e.into_bytes(),
                ))
            }
        }
    }
}

#[async_trait::async_trait]
impl NetHandler for FilerNetHandler {
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
            MsgType::Lookup => {
                let response = self.handle_lookup(msg).await?;
                if response.header.status == STATUS_OK {
                    if let Some(ref notifier) = self.inode_notifier {
                        let client_id = ctx.client.client_id;
                        let parent_ino = TlvDecoder::new(&msg.body)
                            .next_u64(FieldId::ParentIno)
                            .unwrap_or(0);
                        if parent_ino != 0 {
                            notifier.subscribe(parent_ino, client_id);
                            info!(
                                "FILER_NET_SUBSCRIBE: client {} subscribed to dir inode {} (lookup)",
                                client_id, parent_ino
                            );
                        }
                        if let Ok(entry_ino) =
                            TlvDecoder::new(&response.body).next_u64(FieldId::Ino)
                        {
                            if entry_ino != 0 && entry_ino != parent_ino {
                                notifier.subscribe(entry_ino, client_id);
                                info!(
                                    "FILER_NET_SUBSCRIBE: client {} subscribed to entry inode {} (lookup)",
                                    client_id, entry_ino
                                );
                            }
                        }
                    }
                }
                Ok(response)
            }
            MsgType::ReadDir => {
                let response = self.handle_readdir(msg).await?;
                if response.header.status == STATUS_OK {
                    if let Some(ref notifier) = self.inode_notifier {
                        let client_id = ctx.client.client_id;
                        let parent_ino = TlvDecoder::new(&msg.body)
                            .next_u64(FieldId::ParentIno)
                            .unwrap_or(0);
                        if parent_ino != 0 {
                            notifier.subscribe(parent_ino, client_id);
                            info!(
                                "FILER_NET_SUBSCRIBE: client {} subscribed to dir inode {} (readdir)",
                                client_id, parent_ino
                            );
                        }
                    }
                }
                Ok(response)
            }
            MsgType::GetAttr => self.handle_getattr(msg).await,
            MsgType::SetAttr => self.handle_setattr(msg).await,
            MsgType::SetAttrData => self.handle_setattr_data(msg).await,
            MsgType::SetAttrMeta => self.handle_setattr_meta(msg).await,
            MsgType::Create => {
                let response = self.handle_create(msg).await?;
                // Subscribe the creating client to the parent directory and
                // the new inode so it receives subsequent Invalidate
                // notifications (e.g., another client truncating the file).
                if response.header.status == STATUS_OK {
                    if let Some(ref notifier) = self.inode_notifier {
                        let client_id = ctx.client.client_id;
                        let parent_ino = TlvDecoder::new(&msg.body)
                            .next_u64(FieldId::ParentIno)
                            .unwrap_or(0);
                        if parent_ino != 0 {
                            notifier.subscribe(parent_ino, client_id);
                            info!(
                                "FILER_NET_SUBSCRIBE: client {} subscribed to dir inode {} (create)",
                                client_id, parent_ino
                            );
                        }
                        if let Ok(entry_ino) =
                            TlvDecoder::new(&response.body).next_u64(FieldId::Ino)
                        {
                            if entry_ino != 0 && entry_ino != parent_ino {
                                notifier.subscribe(entry_ino, client_id);
                                info!(
                                    "FILER_NET_SUBSCRIBE: client {} subscribed to entry inode {} (create)",
                                    client_id, entry_ino
                                );
                            }
                        }
                    }
                }
                Ok(response)
            }
            MsgType::Mkdir => {
                let response = self.handle_mkdir(msg).await?;
                if response.header.status == STATUS_OK {
                    if let Some(ref notifier) = self.inode_notifier {
                        let client_id = ctx.client.client_id;
                        let parent_ino = TlvDecoder::new(&msg.body)
                            .next_u64(FieldId::ParentIno)
                            .unwrap_or(0);
                        if parent_ino != 0 {
                            notifier.subscribe(parent_ino, client_id);
                        }
                        if let Ok(entry_ino) =
                            TlvDecoder::new(&response.body).next_u64(FieldId::Ino)
                        {
                            if entry_ino != 0 && entry_ino != parent_ino {
                                notifier.subscribe(entry_ino, client_id);
                            }
                        }
                    }
                }
                Ok(response)
            }
            MsgType::Unlink => self.handle_unlink(msg).await,
            MsgType::Rmdir => self.handle_rmdir(msg).await,
            MsgType::Rename => self.handle_rename(msg).await,
            MsgType::StatFs => self.handle_statfs(msg).await,
            MsgType::Symlink => self.handle_symlink(msg).await,
            MsgType::Readlink => self.handle_readlink(msg).await,
            MsgType::Link => self.handle_link(msg).await,
            // MsgType::Read (0x0020) / MsgType::Write (0x0021) removed —
            // data operations go directly to Volume Server, not through Filer.
            MsgType::AllocInodeBatch => self.handle_alloc_inode_batch(msg).await,
            MsgType::UpdateInodeSizeChunks => self.handle_update_inode_size_chunks(msg).await,
            MsgType::OpenCountInc => self.handle_open_count_inc(msg).await,
            MsgType::OpenCountDec => self.handle_open_count_dec(msg).await,
            MsgType::MigrateInlineAlloc => self.handle_migrate_inline_alloc(msg).await,
            // Phase 2 / 方案 A: Inode metadata lease (Filer-managed)
            MsgType::AcquireInodeLease => self.handle_acquire_inode_lease(msg).await,
            MsgType::ReleaseInodeLease => self.handle_release_inode_lease(msg).await,
            MsgType::RenewInodeLease => self.handle_renew_inode_lease(msg).await,
            MsgType::RaftMessage => self.handle_raft_message(msg).await,
            // AssignVolumeV2 removed - volume assignment is handled by Master via MsgType::Assign
            MsgType::Ping => Ok(NetMessage::ok_response(msg, Vec::new(), Vec::new())),
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
