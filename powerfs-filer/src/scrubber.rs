//! P4: Scrubber Worker — 后台副本复制状态机
//!
//! 定期扫描 reliability_state == PendingReplicated 的文件,
//! 将 chunk 数据复制到另一个 volume (anti-affinity),
//! 然后通过 Raft 更新状态为 Replicated.
//!
//! 状态机: PendingReplicated → (scrubber 复制) → Replicated
//! 仅 Raft leader 对应 shard 的 Filer 执行复制 + propose.
//!
//! 使用 powerfs-net TLV 协议与 Volume Server 通信 (非 gRPC),
//! 因为内核客户端没有 gRPC, 所有业务通信统一走 TLV.

use crate::meta_shard_manager::MetaShardManager;
use crate::net_handler::FilerNetHandler;
use crate::shard_store::StoredFileChunk;
use crate::tlv_volume_client::TlvVolumeClient;
use log::{debug, error, info, warn};
use powerfs_layout::reliability::{Reliability, ReliabilityState};
use std::sync::Arc;
use tokio::time::{interval, Duration};

/// Scrubber 配置
pub struct ScrubberConfig {
    /// 扫描间隔 (秒), 默认 30
    pub scan_interval_secs: u64,
    /// 每次扫描最多处理的 inode 数, 默认 50
    pub max_inodes_per_scan: usize,
    /// 副本数 (含原始副本), 默认 2
    pub replica_count: u32,
    /// P6: EC 数据块数, 默认 4
    pub ec_data_shards: u32,
    /// P6: EC 校验块数, 默认 2
    pub ec_parity_shards: u32,
    /// P6: EC 转换最小文件大小 (字节), 默认 0 = 不限制
    pub ec_min_file_size: u64,
}

impl Default for ScrubberConfig {
    fn default() -> Self {
        Self {
            scan_interval_secs: 30,
            max_inodes_per_scan: 50,
            replica_count: 2,
            ec_data_shards: 4,
            ec_parity_shards: 2,
            ec_min_file_size: 0,
        }
    }
}

/// P4: Scrubber Worker
pub struct ScrubberWorker {
    meta_shard_manager: Arc<MetaShardManager>,
    volume_client: Arc<TlvVolumeClient>,
    net_handler: Arc<FilerNetHandler>,
    config: ScrubberConfig,
}

impl ScrubberWorker {
    pub fn new(
        meta_shard_manager: Arc<MetaShardManager>,
        volume_client: Arc<TlvVolumeClient>,
        net_handler: Arc<FilerNetHandler>,
        config: ScrubberConfig,
    ) -> Self {
        Self {
            meta_shard_manager,
            volume_client,
            net_handler,
            config,
        }
    }

    /// 启动后台 scrubber 循环
    pub async fn run(&self) {
        let mut tick = interval(Duration::from_secs(self.config.scan_interval_secs));

        info!(
            "P4_SCRUBBER: started, scan_interval={}s, max_inodes={}, replicas={}, ec={:?}+{:?}",
            self.config.scan_interval_secs,
            self.config.max_inodes_per_scan,
            self.config.replica_count,
            self.config.ec_data_shards,
            self.config.ec_parity_shards,
        );

        // 首次延迟 10 秒, 等待 Filer 完成启动 + Zone 注册
        tokio::time::sleep(Duration::from_secs(10)).await;

        loop {
            tick.tick().await;
            // P4: 副本复制
            if let Err(e) = self.scan_and_replicate().await {
                error!("P4_SCRUBBER: scan error: {}", e);
            }
            // P6: EC 转换 (Replicated → EC)
            if let Err(e) = self.scan_and_ec_convert().await {
                error!("P6_SCRUBBER: EC scan error: {}", e);
            }
        }
    }

    /// 扫描 PendingReplicated 文件, 执行副本复制
    async fn scan_and_replicate(&self) -> Result<(), String> {
        let pending = self.meta_shard_manager.list_pending_replicated();
        if pending.is_empty() {
            return Ok(());
        }

        info!(
            "P4_SCRUBBER: found {} PendingReplicated inodes",
            pending.len()
        );

        let volume_addrs = self.net_handler.get_all_volume_addrs();
        if volume_addrs.len() < 2 {
            warn!(
                "P4_SCRUBBER: only {} volumes available, need >= 2 for replication",
                volume_addrs.len()
            );
            return Ok(());
        }

        let addr_map: std::collections::HashMap<u64, String> =
            volume_addrs.iter().cloned().collect();

        let mut processed = 0usize;
        for (inode, chunks) in pending {
            if processed >= self.config.max_inodes_per_scan {
                info!(
                    "P4_SCRUBBER: reached max_inodes_per_scan={}, stopping",
                    self.config.max_inodes_per_scan
                );
                break;
            }

            match self.replicate_inode(inode, &chunks, &addr_map).await {
                Ok(replica_chunks) => {
                    // 通过 Raft 更新状态
                    let shard_id = self.meta_shard_manager.calculate_shard_id(inode);
                    match self
                        .meta_shard_manager
                        .update_reliability(
                            inode,
                            shard_id,
                            Reliability::Replicated {
                                count: self.config.replica_count,
                            },
                            ReliabilityState::Replicated,
                            replica_chunks,
                        )
                        .await
                    {
                        Ok(()) => {
                            info!(
                                "P4_SCRUBBER: inode {} replicated, state -> Replicated",
                                inode
                            );
                            processed += 1;
                        }
                        Err(e) => {
                            warn!("P4_SCRUBBER: inode {} Raft update failed: {}", inode, e);
                        }
                    }
                }
                Err(e) => {
                    warn!("P4_SCRUBBER: inode {} replication failed: {}", inode, e);
                }
            }
        }

        if processed > 0 {
            info!("P4_SCRUBBER: processed {} inodes this scan", processed);
        }

        Ok(())
    }

    /// 为单个 inode 的所有 chunk 创建副本
    /// 返回 replica_chunks (副本位置信息)
    async fn replicate_inode(
        &self,
        inode: u64,
        chunks: &[StoredFileChunk],
        addr_map: &std::collections::HashMap<u64, String>,
    ) -> Result<Vec<StoredFileChunk>, String> {
        let mut replica_chunks = Vec::with_capacity(chunks.len());

        for chunk in chunks {
            // 选择目标 volume (anti-affinity: 与源 volume 不同)
            let dst_volume_id = self
                .select_replica_volume(chunk.volume_id, addr_map)
                .ok_or_else(|| {
                    format!(
                        "no suitable replica volume for inode {} chunk at offset {} (src volume {})",
                        inode, chunk.offset, chunk.volume_id
                    )
                })?;

            let src_addr = addr_map
                .get(&chunk.volume_id)
                .ok_or_else(|| format!("src volume {} addr not found", chunk.volume_id))?
                .clone();
            let dst_addr = addr_map
                .get(&dst_volume_id)
                .ok_or_else(|| format!("dst volume {} addr not found", dst_volume_id))?
                .clone();

            // 1. 从源 volume 读取 chunk 数据 (TLV ReadNeedle)
            let data = self
                .volume_client
                .read_needle(&src_addr, chunk.volume_id, chunk.needle_id)
                .await
                .map_err(|e| {
                    format!(
                        "read_needle src vol={} needle={:#x} failed: {}",
                        chunk.volume_id, chunk.needle_id, e
                    )
                })?;

            // 1b. CRC32 校验: 防止复制损坏数据到副本 volume
            // (crc32==0 表示旧数据未计算 CRC, 跳过校验)
            if chunk.crc32 != 0 {
                let actual_crc = crc32fast::hash(&data);
                if actual_crc != chunk.crc32 {
                    return Err(format!(
                        "CRC32 mismatch during replication: inode={} offset={} src vol={} needle={:#x} expected={:#x} actual={:#x}",
                        inode, chunk.offset, chunk.volume_id, chunk.needle_id,
                        chunk.crc32, actual_crc
                    ));
                }
            }

            // 2. 写入目标 volume (TLV WriteNeedle, 数据放在 DATA 段)
            // 使用相同的 needle_id, 因为 needle_id 是全局唯一的
            self.volume_client
                .write_needle(&dst_addr, dst_volume_id, chunk.needle_id, &data)
                .await
                .map_err(|e| {
                    format!(
                        "write_needle dst vol={} needle={:#x} failed: {}",
                        dst_volume_id, chunk.needle_id, e
                    )
                })?;

            debug!(
                "P4_SCRUBBER: replicated inode={} chunk offset={} {} bytes: vol {} -> vol {}",
                inode,
                chunk.offset,
                data.len(),
                chunk.volume_id,
                dst_volume_id
            );

            replica_chunks.push(StoredFileChunk {
                offset: chunk.offset,
                size: chunk.size,
                needle_id: chunk.needle_id,
                volume_id: dst_volume_id,
                crc32: chunk.crc32,
                mtime: chunk.mtime,
            });
        }

        Ok(replica_chunks)
    }

    /// 选择副本目标 volume (anti-affinity: 与源 volume 不同)
    fn select_replica_volume(
        &self,
        src_volume_id: u64,
        addr_map: &std::collections::HashMap<u64, String>,
    ) -> Option<u64> {
        // 选择第一个与源 volume 不同的 volume
        for &vol_id in addr_map.keys() {
            if vol_id != src_volume_id {
                return Some(vol_id);
            }
        }
        None
    }

    // ========================================================================
    // P6: EC 转换 (Replicated → EC)
    // ========================================================================

    /// P6: 扫描 Replicated 文件, 执行 EC 转换
    async fn scan_and_ec_convert(&self) -> Result<(), String> {
        let data_shards = self.config.ec_data_shards as usize;
        let parity_shards = self.config.ec_parity_shards as usize;
        let total_shards = data_shards + parity_shards;

        let pending = self
            .meta_shard_manager
            .list_pending_ec(self.config.ec_min_file_size);
        if pending.is_empty() {
            return Ok(());
        }

        info!(
            "P6_SCRUBBER: found {} Replicated inodes eligible for EC",
            pending.len()
        );

        let volume_addrs = self.net_handler.get_all_volume_addrs();
        if (volume_addrs.len() as usize) < total_shards {
            warn!(
                "P6_SCRUBBER: only {} volumes available, need >= {} for EC({}+{})",
                volume_addrs.len(),
                total_shards,
                data_shards,
                parity_shards
            );
            return Ok(());
        }

        let addr_map: std::collections::HashMap<u64, String> =
            volume_addrs.iter().cloned().collect();

        let mut processed = 0usize;
        for (inode, chunks) in pending {
            if processed >= self.config.max_inodes_per_scan {
                info!(
                    "P6_SCRUBBER: reached max_inodes_per_scan={}, stopping",
                    self.config.max_inodes_per_scan
                );
                break;
            }

            match self.ec_convert_inode(inode, &chunks, &addr_map).await {
                Ok(ec_chunks) => {
                    let shard_size = ec_chunks.first().map(|c| c.size).unwrap_or(0);
                    let reliability = Reliability::EC {
                        data: self.config.ec_data_shards,
                        parity: self.config.ec_parity_shards,
                    };
                    let shard_id = self.meta_shard_manager.calculate_shard_id(inode);
                    match self
                        .meta_shard_manager
                        .update_to_ec(
                            inode,
                            shard_id,
                            reliability,
                            ReliabilityState::EC,
                            ec_chunks,
                        )
                        .await
                    {
                        Ok(()) => {
                            info!(
                                "P6_SCRUBBER: inode {} EC converted, {}+{} shards ({}B each), state -> EC",
                                inode, data_shards, parity_shards, shard_size
                            );
                            processed += 1;
                            // Only convert one file per scan to avoid overload
                            break;
                        }
                        Err(e) => {
                            warn!("P6_SCRUBBER: inode {} Raft update failed: {}", inode, e);
                        }
                    }
                }
                Err(e) => {
                    debug!("P6_SCRUBBER: inode {} EC conversion failed: {}", inode, e);
                }
            }
        }

        if processed > 0 {
            info!("P6_SCRUBBER: EC-converted {} inodes this scan", processed);
        }

        Ok(())
    }

    /// P6: 将单个 inode 的数据转换为 EC shards
    /// 1. 读取所有 chunks, 拼接成完整文件数据
    /// 2. EC 编码: data shards + parity shards
    /// 3. 分配 volumes + needle_ids (anti-affinity)
    /// 4. 写入每个 shard 到对应 volume
    /// 5. 返回 ec_chunks (data + parity shard 位置信息)
    async fn ec_convert_inode(
        &self,
        inode: u64,
        chunks: &[StoredFileChunk],
        addr_map: &std::collections::HashMap<u64, String>,
    ) -> Result<Vec<StoredFileChunk>, String> {
        let data_shards = self.config.ec_data_shards as usize;
        let parity_shards = self.config.ec_parity_shards as usize;
        let total_shards = data_shards + parity_shards;

        // 1. 读取所有 chunks, 拼接成完整文件数据
        let mut file_data = Vec::new();
        for chunk in chunks {
            let src_addr = addr_map
                .get(&chunk.volume_id)
                .ok_or_else(|| format!("vol {} addr not found", chunk.volume_id))?
                .clone();
            let data = self
                .volume_client
                .read_needle(&src_addr, chunk.volume_id, chunk.needle_id)
                .await
                .map_err(|e| {
                    format!(
                        "read_needle vol={} needle={:#x} failed: {}",
                        chunk.volume_id, chunk.needle_id, e
                    )
                })?;

            // CRC32 校验
            if chunk.crc32 != 0 {
                let actual_crc = crc32fast::hash(&data);
                if actual_crc != chunk.crc32 {
                    return Err(format!(
                        "CRC32 mismatch during EC read: inode={} offset={} expected={:#x} actual={:#x}",
                        inode, chunk.offset, chunk.crc32, actual_crc
                    ));
                }
            }

            file_data.extend_from_slice(&data);
        }

        info!(
            "P6_SCRUBBER: inode {} read {} bytes from {} chunks for EC encoding",
            inode,
            file_data.len(),
            chunks.len()
        );

        // 2. EC 编码
        let ec_config = powerfs_core::ec_thread::EcConfig {
            data_shards,
            parity_shards,
            parallel_encoding: true,
            ..Default::default()
        };
        let min_small_file_size = ec_config.min_small_file_size;
        let encoder = powerfs_core::ec_thread::EcEncoder::new(ec_config);

        if encoder.should_skip_ec(file_data.len()) {
            return Err(format!(
                "file too small for EC ({} bytes < min {})",
                file_data.len(),
                min_small_file_size
            ));
        }

        let shards = encoder.encode(&file_data);
        if shards.len() != total_shards {
            return Err(format!(
                "EC encode returned {} shards, expected {}",
                shards.len(),
                total_shards
            ));
        }

        let shard_size = shards[0].len();

        // 3. 分配 volumes + needle_ids (anti-affinity)
        let alloc = self
            .net_handler
            .alloc_for_stripe_file(total_shards as u32)
            .ok_or_else(|| "no volumes available for EC shard allocation".to_string())?;

        // 4. 写入每个 shard 到对应 volume
        let now = chrono::Utc::now().timestamp() as u64;
        let mut ec_chunks = Vec::with_capacity(total_shards);
        for (i, shard_data) in shards.iter().enumerate() {
            let (volume_id, needle_id) = alloc[i];
            let addr = addr_map
                .get(&volume_id)
                .ok_or_else(|| format!("EC shard vol {} addr not found", volume_id))?
                .clone();

            self.volume_client
                .write_needle(&addr, volume_id, needle_id, shard_data)
                .await
                .map_err(|e| {
                    format!(
                        "write_needle EC shard {} vol={} needle={:#x} failed: {}",
                        i, volume_id, needle_id, e
                    )
                })?;

            let crc = crc32fast::hash(shard_data);
            ec_chunks.push(StoredFileChunk {
                offset: (i as u64) * (shard_size as u64),
                size: shard_data.len() as u64,
                needle_id,
                volume_id,
                crc32: crc,
                mtime: now,
            });

            debug!(
                "P6_SCRUBBER: inode {} EC shard {}/{} written: vol={} needle={:#x} {}B crc={:#x}",
                inode,
                i + 1,
                total_shards,
                volume_id,
                needle_id,
                shard_data.len(),
                crc
            );
        }

        Ok(ec_chunks)
    }
}
