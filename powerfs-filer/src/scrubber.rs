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
use log::{debug, error, info, warn};
use powerfs_layout::reliability::{Reliability, ReliabilityState};
use powerfs_master_net::TlvMasterClient;
use powerfs_net::TlvVolumeClient;
use std::sync::Arc;
use tokio::time::{interval, Duration};

/// 分层 EC 配置项：按文件大小选择不同的 EC(data+parity) 配置
#[derive(Clone, Debug)]
pub struct EcTier {
    /// 该 tier 适用的文件大小上限 (bytes). 最后一个 tier 忽略此字段.
    pub max_file_size: u64,
    pub data_shards: u32,
    pub parity_shards: u32,
}

impl EcTier {
    pub fn total_shards(&self) -> usize {
        (self.data_shards + self.parity_shards) as usize
    }
}

/// P6: EC 分片固定大小 (1 MiB)。
///
/// 每个 shard 不超过 TLV 单帧 2 MiB (`MAX_DATA_SIZE`) 限制；整个文件按
/// stripe group 切分，每个 group = `data_shards × EC_SHARD_SIZE`，独立编码。
/// 该值同时作为 `EcLayout.shard_size` 暴露给读路径
/// (`powerfs-filer/src/grpc_service.rs`)，禁止在别处重复写 1 MiB 字面量。
pub(crate) const EC_SHARD_SIZE: usize = 1024 * 1024;

/// Scrubber 配置
pub struct ScrubberConfig {
    /// 扫描间隔 (秒), 默认 30
    pub scan_interval_secs: u64,
    /// 每次扫描最多处理的 inode 数, 默认 50
    pub max_inodes_per_scan: usize,
    /// 副本数 (含原始副本), 默认 2
    pub replica_count: u32,
    /// P6: EC 转换最小文件大小 (字节), 默认 0 = 不限制
    pub ec_min_file_size: u64,
    /// 分层 EC 配置表 (按文件大小升序). select_ec_config 按文件大小
    /// 匹配第一个 max_file_size > size 的 tier; 都不匹配则用最后一个.
    /// 默认: 8KB-1MB → EC(2+1), 1MB-1GB → EC(4+2),
    ///       1GB-100GB → EC(8+4), >100GB → EC(16+4)
    pub ec_tiers: Vec<EcTier>,
    /// P7: 是否启用后台压缩 (默认 false, 由 POWERFS_COMPRESS_ENABLED 控制)
    pub compress_enabled: bool,
    /// P7: 压缩最小文件大小 (字节), 默认 1MiB
    pub compress_min_file_size: u64,
    /// P7: 文件空闲秒数阈值 (max(atime,mtime) 距今), 默认 30 天
    pub compress_idle_secs: u64,
    /// P7: 每轮最多压缩的 inode 数, 默认 10
    pub compress_max_inodes: usize,
    /// P7: 压缩算法 (当前仅 zstd)
    pub compress_algo: powerfs_common::compression::CompressionAlgo,
}

impl Default for ScrubberConfig {
    fn default() -> Self {
        Self {
            scan_interval_secs: 30,
            max_inodes_per_scan: 50,
            replica_count: 2,
            ec_min_file_size: 0,
            ec_tiers: vec![
                EcTier {
                    max_file_size: 1 << 20,
                    data_shards: 2,
                    parity_shards: 1,
                }, // 8KB-1MB → EC(2+1)
                EcTier {
                    max_file_size: 1 << 30,
                    data_shards: 4,
                    parity_shards: 2,
                }, // 1MB-1GB → EC(4+2)
                EcTier {
                    max_file_size: 100 * (1 << 30),
                    data_shards: 8,
                    parity_shards: 4,
                }, // 1GB-100GB → EC(8+4)
                EcTier {
                    max_file_size: u64::MAX,
                    data_shards: 16,
                    parity_shards: 4,
                }, // >100GB → EC(16+4)
            ],
            // P7 压缩默认关闭，阈值按 spec FR-7
            compress_enabled: false,
            compress_min_file_size: 1024 * 1024,
            compress_idle_secs: 30 * 24 * 3600,
            compress_max_inodes: 10,
            compress_algo: powerfs_common::compression::CompressionAlgo::Zstd,
        }
    }
}

impl ScrubberConfig {
    /// 从环境变量构建完整的 ScrubberConfig（EC 分层配置用默认值，
    /// 其余字段均可被 POWERFS_* 覆盖）。集中解析，避免 main.rs 重复。
    pub fn from_env() -> Self {
        use powerfs_common::compression::CompressionAlgo;

        let compress_algo = match std::env::var("POWERFS_COMPRESS_ALGO")
            .ok()
            .as_deref()
            .map(|s| s.to_ascii_lowercase())
            .as_deref()
        {
            Some("zstd") => CompressionAlgo::Zstd,
            Some("none") | Some("off") => CompressionAlgo::None,
            _ => CompressionAlgo::Zstd,
        };

        Self {
            scan_interval_secs: parse_env_u64("POWERFS_SCRUBBER_SCAN_INTERVAL", 30),
            max_inodes_per_scan: parse_env_usize("POWERFS_SCRUBBER_MAX_INODES", 50),
            replica_count: 2,
            ec_min_file_size: parse_env_u64("POWERFS_EC_MIN_FILE_SIZE", 0),
            ec_tiers: Self::default().ec_tiers,
            compress_enabled: parse_env_bool("POWERFS_COMPRESS_ENABLED", false),
            compress_min_file_size: parse_env_u64("POWERFS_COMPRESS_MIN_FILE_SIZE", 1024 * 1024),
            compress_idle_secs: parse_env_u64("POWERFS_COMPRESS_IDLE_SECS", 30 * 24 * 3600),
            compress_max_inodes: parse_env_usize("POWERFS_COMPRESS_MAX_INODES", 10),
            compress_algo,
        }
    }

    /// 按文件大小选择 EC tier 配置 (data_shards, parity_shards)
    fn select_ec_config(&self, file_size: u64) -> (u32, u32) {
        for tier in &self.ec_tiers {
            if file_size < tier.max_file_size {
                return (tier.data_shards, tier.parity_shards);
            }
        }
        // 都不匹配 (理论上不会, 因为最后一个 tier 是 u64::MAX), 用最后一个
        let last = self.ec_tiers.last().expect("ec_tiers must not be empty");
        (last.data_shards, last.parity_shards)
    }

    /// 所有 tier 中最大的 total_shards, 用于判断 EC 是否可行
    /// (volume 数至少要满足最小 tier 的要求才能开始扫描)
    fn min_total_shards(&self) -> usize {
        self.ec_tiers
            .iter()
            .map(|t| t.total_shards())
            .min()
            .unwrap_or(6)
    }
}

/// P4: Scrubber Worker
pub struct ScrubberWorker {
    meta_shard_manager: Arc<MetaShardManager>,
    volume_client: Arc<TlvVolumeClient>,
    net_handler: Arc<FilerNetHandler>,
    /// Master TLV client: 副本复制时向 Master 申请目标卷的新 file_key
    /// (MsgType::AssignNeedle). needle_id 不能跨卷复用 — Master 的
    /// file_key 是按卷独立计数器, 同一数值会在不同卷重复分配.
    master_client: Arc<TlvMasterClient>,
    config: ScrubberConfig,
    /// P6: EC 不可行标记 (volume 数 < data+parity 时置 true, 跳过 EC 转换).
    /// 每 EC_RECHECK_CYCLES 轮重新检查一次, 支持扩容后自动恢复 EC.
    ec_infeasible: std::sync::atomic::AtomicBool,
    /// P6: EC 不可行后的扫描计数, 用于周期性重检
    ec_skip_count: std::sync::atomic::AtomicU32,
}

/// P6: EC 不可行时, 每隔多少轮扫描重新检查一次 volume 数量
const EC_RECHECK_CYCLES: u32 = 10;

/// P7: 环境变量解析辅助（解析失败时回退默认值，不 panic）。
fn parse_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key).ok().as_deref() {
        Some("1") | Some("true") | Some("TRUE") | Some("yes") => true,
        Some("0") | Some("false") | Some("FALSE") | Some("no") => false,
        _ => default,
    }
}

impl ScrubberWorker {
    pub fn new(
        meta_shard_manager: Arc<MetaShardManager>,
        volume_client: Arc<TlvVolumeClient>,
        net_handler: Arc<FilerNetHandler>,
        master_client: Arc<TlvMasterClient>,
        config: ScrubberConfig,
    ) -> Self {
        Self {
            meta_shard_manager,
            volume_client,
            net_handler,
            master_client,
            config,
            ec_infeasible: std::sync::atomic::AtomicBool::new(false),
            ec_skip_count: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// 启动后台 scrubber 循环
    pub async fn run(&self) {
        let mut tick = interval(Duration::from_secs(self.config.scan_interval_secs));

        info!(
            "P4_SCRUBBER: started, scan_interval={}s, max_inodes={}, replicas={}, ec_tiers={:?}",
            self.config.scan_interval_secs,
            self.config.max_inodes_per_scan,
            self.config.replica_count,
            self.config.ec_tiers,
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
            // C-0.7: Fingerprint tombstone GC
            if let Err(e) = self.scan_fingerprint_tombstones().await {
                error!("C0_SCRUBBER: fingerprint tombstone GC error: {}", e);
            }
            // P7: 后台压缩（仅 leader；未开启时直接跳过）
            if let Err(e) = self.scan_and_compress().await {
                error!("P7_SCRUBBER: compression scan error: {}", e);
            }
        }
    }

    /// 构建 volume_id → addr 映射, 供副本复制/EC 转换读取 chunk 使用.
    ///
    /// 以 Master 拓扑为准: Master 的 Assign 在**全部** volume 上分配
    /// (一个节点可承载多个 volume), 而 Filer 的 Zone 视图只含反亲和选出的
    /// 子集 — 仅用 Zone 视图会导致大量 chunk 的源卷 "addr not found",
    /// 复制/EC 永远无法执行. 拓扑查询失败时退回 Zone 视图.
    async fn volume_addr_map(&self) -> std::collections::HashMap<u64, String> {
        let map: std::collections::HashMap<u64, String> =
            match self.master_client.get_topology().await {
                Ok(topo) if !topo.volumes.is_empty() => topo
                    .volumes
                    .into_iter()
                    .map(|v| (v.volume_id, v.addr))
                    .collect(),
                Ok(_) => std::collections::HashMap::new(),
                Err(e) => {
                    warn!(
                        "SCRUBBER: get_topology failed, falling back to zone view: {}",
                        e
                    );
                    std::collections::HashMap::new()
                }
            };
        if !map.is_empty() {
            return map;
        }
        self.net_handler
            .get_all_volume_addrs()
            .into_iter()
            .collect()
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

        let addr_map = self.volume_addr_map().await;
        if addr_map.len() < 2 {
            warn!(
                "P4_SCRUBBER: only {} volumes available, need >= 2 for replication",
                addr_map.len()
            );
            return Ok(());
        }

        let mut processed = 0usize;
        for (inode, chunks) in pending {
            if processed >= self.config.max_inodes_per_scan {
                info!(
                    "P4_SCRUBBER: reached max_inodes_per_scan={}, stopping",
                    self.config.max_inodes_per_scan
                );
                break;
            }

            // 仅 shard leader 执行复制: 非 leader 的 propose_ff 会被静默丢弃,
            // 数据拷白做, 且新 file_key 方案下每轮重扫都会重复分配 key,
            // 在目标卷留下孤儿 needle.
            let shard_id = self.meta_shard_manager.calculate_shard_id(inode);
            if !self.meta_shard_manager.is_shard_leader(shard_id).await {
                continue;
            }

            match self.replicate_inode(inode, &chunks, &addr_map).await {
                Ok(replica_chunks) => {
                    // 通过 Raft 更新状态
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
    ///
    /// 副本使用目标卷上**新分配**的 file_key (向 Master 申请), 不复用源
    /// chunk 的 needle_id: Master 的 file_key 是按卷独立计数器, 同一数值
    /// 在不同卷上会被重复分配给不相关的文件, 跨卷复用会覆写目标卷上
    /// 已有数据 (P2d multipart resend 数据损坏事故根因).
    async fn replicate_inode(
        &self,
        inode: u64,
        chunks: &[StoredFileChunk],
        addr_map: &std::collections::HashMap<u64, String>,
    ) -> Result<Vec<StoredFileChunk>, String> {
        let mut replica_chunks = Vec::with_capacity(chunks.len());

        for chunk in chunks {
            match self.replicate_chunk(inode, chunk, addr_map).await {
                Ok(rc) => replica_chunks.push(rc),
                Err(e) => {
                    // 本次已写入的副本尚未通过 Raft 提交到元数据, 是确定
                    // 无主的孤儿 needle — 清理掉, 避免失败重扫时重复分配
                    // 导致孤儿 key 在目标卷上累积泄漏.
                    self.delete_orphan_replicas(&replica_chunks, addr_map).await;
                    return Err(e);
                }
            }
        }

        Ok(replica_chunks)
    }

    /// 复制单个 chunk 到 anti-affinity 目标卷, 返回副本位置信息.
    async fn replicate_chunk(
        &self,
        inode: u64,
        chunk: &StoredFileChunk,
        addr_map: &std::collections::HashMap<u64, String>,
    ) -> Result<StoredFileChunk, String> {
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

        // 2. 向 Master 申请目标卷上的新 file_key (TLV AssignNeedle).
        // needle_id 不能跨卷复用 — 见 replicate_inode 文档注释.
        let replica_needle_id = self
            .master_client
            .assign_needle(dst_volume_id)
            .await
            .map_err(|e| {
                format!(
                    "assign_needle dst vol={} failed (inode {} offset {}): {}",
                    dst_volume_id, inode, chunk.offset, e
                )
            })?;

        // 3. 写入目标 volume (TLV WriteNeedle, 数据放在 DATA 段)
        self.volume_client
            .write_needle(&dst_addr, dst_volume_id, replica_needle_id, &data)
            .await
            .map_err(|e| {
                format!(
                    "write_needle dst vol={} needle={:#x} failed: {}",
                    dst_volume_id, replica_needle_id, e
                )
            })?;

        debug!(
            "P4_SCRUBBER: replicated inode={} chunk offset={} {} bytes: vol {} needle={:#x} -> vol {} needle={:#x}",
            inode,
            chunk.offset,
            data.len(),
            chunk.volume_id,
            chunk.needle_id,
            dst_volume_id,
            replica_needle_id
        );

        Ok(StoredFileChunk {
            offset: chunk.offset,
            size: chunk.size,
            needle_id: replica_needle_id,
            volume_id: dst_volume_id,
            crc32: chunk.crc32,
            mtime: chunk.mtime,
            is_reference: false,
            // 副本继承源 chunk 的压缩算法（复制的是同一份存储字节）
            compression: chunk.compression,
        })
    }

    /// 清理复制中途失败时已写入的孤儿副本 needle (best-effort).
    ///
    /// 仅在 replicate_inode 内部失败路径调用 — 此时 replica_chunks 尚未
    /// 提交 Raft, 保证无任何元数据引用. update_reliability 失败后**不可**
    /// 调用本函数 (propose 可能实际已提交, 误删会破坏已生效的副本).
    async fn delete_orphan_replicas(
        &self,
        replica_chunks: &[StoredFileChunk],
        addr_map: &std::collections::HashMap<u64, String>,
    ) {
        for rc in replica_chunks {
            let Some(addr) = addr_map.get(&rc.volume_id) else {
                continue;
            };
            if let Err(e) = self
                .volume_client
                .delete_needle(addr, rc.volume_id, rc.needle_id)
                .await
            {
                warn!(
                    "P4_SCRUBBER: cleanup orphan replica vol={} needle={:#x} failed: {}",
                    rc.volume_id, rc.needle_id, e
                );
            }
        }
    }

    /// 选择副本目标 volume (anti-affinity: 与源 volume 不同)
    fn select_replica_volume(
        &self,
        src_volume_id: u64,
        addr_map: &std::collections::HashMap<u64, String>,
    ) -> Option<u64> {
        // 选择第一个与源 volume 不同的 volume
        addr_map
            .keys()
            .find(|&&vol_id| vol_id != src_volume_id)
            .copied()
    }

    // ========================================================================
    // P6: EC 转换 (Replicated → EC)
    // ========================================================================

    /// P6: 扫描 Replicated 文件, 执行分层 EC 转换
    ///
    /// 按文件大小选择 EC 配置 (select_ec_config):
    ///   8KB-1MB → EC(2+1), 1MB-1GB → EC(4+2),
    ///   1GB-100GB → EC(8+4), >100GB → EC(16+4)
    async fn scan_and_ec_convert(&self) -> Result<(), String> {
        // 用最小 tier 的 total_shards 做 EC 可行性判断 (volume 数至少满足最小 tier)
        let min_shards = self.config.min_total_shards();

        // 快速跳过: 如果 EC 之前被判定为不可行, 只在周期性重检时重新检查
        if self
            .ec_infeasible
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            let count = self
                .ec_skip_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if !count.is_multiple_of(EC_RECHECK_CYCLES) {
                return Ok(());
            }
            debug!(
                "P6_SCRUBBER: EC was infeasible, re-checking (cycle {})",
                count
            );
        }

        let pending = self
            .meta_shard_manager
            .list_pending_ec(self.config.ec_min_file_size);
        if pending.is_empty() {
            return Ok(());
        }

        // 与 P4 一致: 地址/统计以 Master 拓扑为准 (全部 volume), 否则
        // 非 Zone 子集上的 chunk 无法读取, EC 转换永远失败.
        let addr_map = self.volume_addr_map().await;

        if addr_map.len() < min_shards {
            // EC 不可行: volume 数不足. 标记并跳过, 避免每轮重复日志.
            if !self
                .ec_infeasible
                .swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                // 首次检测到不可行, 打印 warn 级别日志
                warn!(
                    "P6_SCRUBBER: EC disabled — only {} volumes available, need >= {} for smallest EC tier. \
                     Files will stay in Replicated state. Will re-check every {} scans.",
                    addr_map.len(),
                    min_shards,
                    EC_RECHECK_CYCLES,
                );
            } else {
                debug!(
                    "P6_SCRUBBER: EC still infeasible ({} < {} volumes)",
                    addr_map.len(),
                    min_shards
                );
            }
            return Ok(());
        }

        // EC 可行, 清除不可行标记
        if self
            .ec_infeasible
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            info!(
                "P6_SCRUBBER: EC re-enabled — {} volumes available (need >= {} for smallest tier)",
                addr_map.len(),
                min_shards
            );
        }
        self.ec_skip_count
            .store(0, std::sync::atomic::Ordering::Relaxed);

        info!(
            "P6_SCRUBBER: found {} Replicated inodes eligible for EC, {} volumes available",
            pending.len(),
            addr_map.len()
        );

        let mut processed = 0usize;
        for (inode, chunks) in pending {
            if processed >= self.config.max_inodes_per_scan {
                info!(
                    "P6_SCRUBBER: reached max_inodes_per_scan={}, stopping",
                    self.config.max_inodes_per_scan
                );
                break;
            }

            // 按文件大小选择 EC tier (data_shards, parity_shards)
            let file_size: u64 = chunks.iter().map(|c| c.size).sum();
            let (data_shards, parity_shards) = self.config.select_ec_config(file_size);
            let total_shards = (data_shards + parity_shards) as usize;

            // 该文件 tier 需要的 volume 数 > 当前可用数, 跳过 (不阻塞其他文件)
            if addr_map.len() < total_shards {
                debug!(
                    "P6_SCRUBBER: inode {} file_size={} needs EC({}+{}) = {} volumes, only {} available, skipping",
                    inode, file_size, data_shards, parity_shards, total_shards, addr_map.len()
                );
                continue;
            }

            match self
                .ec_convert_inode(inode, &chunks, &addr_map, data_shards, parity_shards)
                .await
            {
                Ok(ec_chunks) => {
                    // P6 CAS: 提交前重新检查 chunks 是否变化 (防止转换期间被写).
                    // 如果 chunks 变了, 说明文件被追加写/截断, Fix 1 已将状态
                    // 回退到 PendingReplicated. 此时提交旧数据的 EC shards 会覆盖
                    // 新数据, 必须放弃. 文件会在下次 scan 重新走完整管线.
                    let current_info = self.meta_shard_manager.get_inode(inode);
                    match current_info {
                        Some(ref info) if info.chunks != chunks => {
                            debug!(
                                "P6_SCRUBBER: inode {} chunks changed during EC conversion, \
                                 aborting (will retry next scan after re-replication)",
                                inode
                            );
                            continue;
                        }
                        None => {
                            debug!(
                                "P6_SCRUBBER: inode {} disappeared during EC conversion, skipping",
                                inode
                            );
                            continue;
                        }
                        _ => {} // chunks 未变, 安全提交
                    }

                    let shard_size = ec_chunks.first().map(|c| c.size).unwrap_or(0);
                    let total_needles = ec_chunks.len();
                    let num_groups = total_needles / total_shards;
                    // 仅记录 group 0 的 shard 位置 (避免大文件日志过长),
                    // 便于降级读测试定位并删除特定分片.
                    let g0_details: Vec<String> = ec_chunks
                        .iter()
                        .take(total_shards)
                        .enumerate()
                        .map(|(i, c)| {
                            let addr = addr_map.get(&c.volume_id).cloned().unwrap_or_default();
                            let kind = if i < data_shards as usize { "D" } else { "P" };
                            format!(
                                "{}[{}]:vol={} needle={:#x}@{}",
                                kind, i, c.volume_id, c.needle_id, addr
                            )
                        })
                        .collect();
                    let reliability = Reliability::EC {
                        data: data_shards,
                        parity: parity_shards,
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
                                "P6_SCRUBBER: inode {} EC({}+{}) converted, file_size={}B, {} groups, {}B shard_size, {} needles total, state -> EC | G0=[{}]",
                                inode, data_shards, parity_shards, file_size, num_groups, shard_size, total_needles, g0_details.join(", ")
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
    /// 2. EC 编码: data shards + parity shards (由调用方按文件大小选择)
    /// 3. 分配 volumes + needle_ids (anti-affinity)
    /// 4. 写入每个 shard 到对应 volume
    /// 5. 返回 ec_chunks (data + parity shard 位置信息)
    async fn ec_convert_inode(
        &self,
        inode: u64,
        chunks: &[StoredFileChunk],
        addr_map: &std::collections::HashMap<u64, String>,
        data_shards: u32,
        parity_shards: u32,
    ) -> Result<Vec<StoredFileChunk>, String> {
        let data_shards = data_shards as usize;
        let parity_shards = parity_shards as usize;
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

        // EC shard 固定 1MB (= chunk_size), 确保每个 shard 不超过 TLV 2MB MAX_DATA_SIZE 限制.
        // 整个文件按 stripe group 切分, 每个 group = data_shards × 1MB, 独立 EC 编码.
        let group_data_size = data_shards * EC_SHARD_SIZE;

        // 2. EC 编码器
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

        let num_groups = file_data.len().div_ceil(group_data_size);
        let now = chrono::Utc::now().timestamp() as u64;
        let mut ec_chunks = Vec::with_capacity(num_groups * total_shards);

        // 3. 按 stripe group 编码: 每个 group = data_shards × 1MB, 独立编码为
        // total_shards × 1MB, 每个 shard 写入独立 needle (不超过 TLV 2MB 限制).
        for group_idx in 0..num_groups {
            let group_start = group_idx * group_data_size;
            let group_end = std::cmp::min(group_start + group_data_size, file_data.len());
            let group_data = &file_data[group_start..group_end];

            // 最后一个 group 可能不足 group_data_size, 用零填充至完整 stripe.
            let mut padded_group = Vec::with_capacity(group_data_size);
            padded_group.extend_from_slice(group_data);
            while padded_group.len() < group_data_size {
                padded_group.push(0);
            }

            let shards = encoder.encode(&padded_group);
            if shards.len() != total_shards {
                return Err(format!(
                    "EC encode group {} returned {} shards, expected {}",
                    group_idx,
                    shards.len(),
                    total_shards
                ));
            }

            // 每个 group 独立分配 (volume_id, needle_id) 对 (anti-affinity)
            let alloc = self
                .net_handler
                .alloc_for_stripe_file(total_shards as u32)
                .ok_or_else(|| "no volumes available for EC shard allocation".to_string())?;

            // 写入每个 shard 到对应 volume
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
                            "write_needle EC shard g{}[{}/{}] vol={} needle={:#x} failed: {}",
                            group_idx, i, total_shards, volume_id, needle_id, e
                        )
                    })?;

                let crc = crc32fast::hash(shard_data);
                // data shard offset = group_start + i*EC_SHARD_SIZE;
                // parity shard offset = group_start (group base)
                let offset = if i < data_shards {
                    (group_start + i * EC_SHARD_SIZE) as u64
                } else {
                    group_start as u64
                };
                ec_chunks.push(StoredFileChunk {
                    offset,
                    size: shard_data.len() as u64,
                    needle_id,
                    volume_id,
                    crc32: crc,
                    mtime: now,
                    is_reference: false,
                    // EC 与压缩互斥：EC 分片永不压缩
                    compression: powerfs_common::compression::CompressionAlgo::None,
                });

                debug!(
                    "P6_SCRUBBER: inode {} EC shard g{}[{}/{}] written: vol={} needle={:#x} {}B crc={:#x}",
                    inode,
                    group_idx,
                    i,
                    total_shards,
                    volume_id,
                    needle_id,
                    shard_data.len(),
                    crc
                );
            }
        }

        info!(
            "P6_SCRUBBER: inode {} EC encoded: {} groups × {} shards = {} needles ({}B shard_size)",
            inode,
            num_groups,
            total_shards,
            ec_chunks.len(),
            EC_SHARD_SIZE
        );

        Ok(ec_chunks)
    }

    /// C-0.7: Scan fingerprint tombstones and expire those whose retention
    /// has elapsed. Expired entries are marked and will be cleaned up on
    /// the next lookup (or by an explicit `remove_by_needle` call after
    /// physical needle deletion).
    ///
    /// Tombstone lifecycle:
    ///   Active (refcount=0) → Tombstoned → [retention period] → Expired → cleaned
    async fn scan_fingerprint_tombstones(&self) -> Result<(), String> {
        let tombstoned = self.net_handler.fingerprint_index.get_tombstoned();
        if tombstoned.is_empty() {
            return Ok(());
        }

        let now = chrono::Utc::now().timestamp() as u64;
        // C-0.7: tombstone retention = 1 hour (configurable in future)
        const TOMBSTONE_RETENTION_SECS: u64 = 3600;

        let mut expired_count = 0usize;
        for (fp, nr) in &tombstoned {
            // We don't have an explicit tombstoned_at timestamp in NeedleRef
            // (it's tracked via the volume server's NeedleInfo.deleted_at).
            // For C-0, we use a simple heuristic: tombstoned entries are
            // expired after TOMBSTONE_RETENTION_SECS from the scrubber scan.
            // C-1 will add proper timestamp tracking via Raft.
            if self.net_handler.fingerprint_index.mark_expired(fp) {
                info!(
                    "C0_SCRUBBER: fingerprint tombstone expired fp={} needle={} vol={}",
                    fp.to_hex(),
                    nr.needle_id,
                    nr.volume_id
                );
                expired_count += 1;
            }
        }

        if expired_count > 0 {
            // Rebuild Bloom filter to remove stale bits
            self.net_handler.fingerprint_index.rebuild_bloom();
            info!(
                "C0_SCRUBBER: expired {} tombstoned fingerprints, bloom rebuilt",
                expired_count
            );
        }

        // Suppress unused-variable warning for `now` (will be used in C-1)
        let _ = now;
        let _ = TOMBSTONE_RETENTION_SECS;

        Ok(())
    }

    // ========================================================================
    // P7: 后台压缩 (Replicated 冷数据 → zstd)
    // ========================================================================

    /// P7: 扫描冷副本文件并执行压缩（仅 leader，未开启时跳过）。
    async fn scan_and_compress(&self) -> Result<(), String> {
        if !self.config.compress_enabled {
            return Ok(());
        }
        // 算法为 None 时无可执行压缩（配置误设防御）
        if self.config.compress_algo == powerfs_common::compression::CompressionAlgo::None {
            return Ok(());
        }

        let now = chrono::Utc::now().timestamp() as u64;
        let pending = self.meta_shard_manager.list_pending_compress(
            self.config.compress_min_file_size,
            self.config.ec_min_file_size, // EC 优先：max_size = EC 阈值
            self.config.compress_idle_secs,
            now,
            self.config.compress_max_inodes,
        );
        if pending.is_empty() {
            return Ok(());
        }

        let addr_map = self.volume_addr_map().await;

        info!(
            "P7_SCRUBBER: found {} compression candidates (min={}B idle={}s max_inodes={})",
            pending.len(),
            self.config.compress_min_file_size,
            self.config.compress_idle_secs,
            self.config.compress_max_inodes,
        );

        let mut processed = 0usize;
        for (inode, _chunks) in pending {
            if processed >= self.config.compress_max_inodes {
                break;
            }

            let shard_id = self.meta_shard_manager.calculate_shard_id(inode);
            if !self.meta_shard_manager.is_shard_leader(shard_id).await {
                continue;
            }

            // 取完整 inode（含 replica_chunks）用于压缩 + CAS
            let info = match self.meta_shard_manager.get_inode(inode) {
                Some(i) => i,
                None => continue,
            };
            if info.chunks.is_empty() {
                continue;
            }

            match self
                .compress_inode(inode, &info.chunks, &info.replica_chunks, &addr_map)
                .await
            {
                Ok(layout) => {
                    // CAS: 提交前再次比对 chunks / compression_state
                    let current = self.meta_shard_manager.get_inode(inode);
                    match current {
                        Some(ref cur)
                            if cur.chunks == info.chunks
                                && cur.compression_state
                                    == powerfs_layout::reliability::CompressionState::None =>
                        {
                            // 安全提交
                        }
                        _ => {
                            // 数据/状态变化 → 丢弃新 needle，下一轮重试
                            self.delete_compressed_orphans(
                                &layout.primary_chunks,
                                &layout.replica_chunks,
                                &addr_map,
                            )
                            .await;
                            debug!(
                                "P7_SCRUBBER: inode {} changed during compression, aborting",
                                inode
                            );
                            continue;
                        }
                    }

                    match self
                        .meta_shard_manager
                        .update_to_compressed(
                            inode,
                            shard_id,
                            layout.primary_chunks,
                            layout.replica_chunks,
                            self.config.compress_algo,
                            layout.ratio_x100,
                            layout.original_size,
                        )
                        .await
                    {
                        Ok(()) => {
                            // 提交成功后 best-effort 回收旧 needle
                            self.delete_compressed_orphans(
                                &info.chunks,
                                &info.replica_chunks,
                                &addr_map,
                            )
                            .await;
                            info!(
                                "P7_SCRUBBER: inode {} compressed (ratio_x100={}, original={}B)",
                                inode, layout.ratio_x100, layout.original_size
                            );
                            processed += 1;
                        }
                        Err(e) => {
                            warn!("P7_SCRUBBER: inode {} commit failed: {}", inode, e);
                        }
                    }
                }
                Err(CompressError::NotCompressible) => {
                    // 比率不达标：标记 skip 避免每轮重试
                    if let Err(e) = self
                        .meta_shard_manager
                        .set_xattr(
                            inode,
                            shard_id,
                            powerfs_common::constants::COMPRESS_SKIP_EXT_KEY,
                            b"1".to_vec(),
                        )
                        .await
                    {
                        warn!(
                            "P7_SCRUBBER: inode {} set compress-skip failed: {}",
                            inode, e
                        );
                    } else {
                        info!("P7_SCRUBBER: inode {} not compressible, marked skip", inode);
                    }
                }
                Err(CompressError::Other(e)) => {
                    warn!("P7_SCRUBBER: inode {} compression failed: {}", inode, e);
                }
            }
        }

        if processed > 0 {
            info!("P7_SCRUBBER: compressed {} inodes this scan", processed);
        }
        Ok(())
    }

    /// P7: 压缩单个 inode 的全部 chunk（主副本 + 各副本）。
    ///
    /// 流程（FR-4）：
    /// 1. 读主副本全部 chunk（CRC 校验，crc32==0 跳过）；
    /// 2. 逐 chunk 压缩到内存，整体比率 < 阈值 → `NotCompressible`（不写任何 needle）；
    /// 3. 主副本向 Master `assign_needle` 申请**同卷**新 key 并写压缩数据；
    ///    每个副本向目标卷申请新 key 并写同一份压缩数据；
    /// 4. 任一步失败 → 删除已写新 needle，返回错误。
    ///
    /// `size` 保持逻辑字节数不变；`crc32` 更新为压缩字节 CRC；
    /// `offset`/`mtime` 不变；`compression = Zstd`。
    async fn compress_inode(
        &self,
        inode: u64,
        chunks: &[StoredFileChunk],
        replica_chunks: &[StoredFileChunk],
        addr_map: &std::collections::HashMap<u64, String>,
    ) -> Result<CompressedLayout, CompressError> {
        use powerfs_common::compression::{compress, should_compress, DEFAULT_MIN_RATIO};

        let algo = self.config.compress_algo;

        // 1+2: 读取并压缩全部主副本 chunk 到内存，同时校验 CRC
        let mut compressed: Vec<Vec<u8>> = Vec::with_capacity(chunks.len());
        let mut total_original: usize = 0;
        let mut total_compressed: usize = 0;
        for chunk in chunks {
            let src_addr = addr_map
                .get(&chunk.volume_id)
                .ok_or_else(|| {
                    CompressError::Other(format!("src volume {} addr not found", chunk.volume_id))
                })?
                .clone();
            let data = self
                .volume_client
                .read_needle(&src_addr, chunk.volume_id, chunk.needle_id)
                .await
                .map_err(|e| {
                    CompressError::Other(format!(
                        "read_needle vol={} needle={:#x} failed: {}",
                        chunk.volume_id, chunk.needle_id, e
                    ))
                })?;

            if chunk.crc32 != 0 {
                let actual_crc = crc32fast::hash(&data);
                if actual_crc != chunk.crc32 {
                    return Err(CompressError::Other(format!(
                        "CRC32 mismatch during compression read: inode={} offset={} expected={:#x} actual={:#x}",
                        inode, chunk.offset, chunk.crc32, actual_crc
                    )));
                }
            }

            let cdata = compress(algo, &data).map_err(|e| {
                CompressError::Other(format!(
                    "compress inode={} offset={} failed: {}",
                    inode, chunk.offset, e
                ))
            })?;

            total_original += chunk.size as usize;
            total_compressed += cdata.len();
            compressed.push(cdata);
        }

        // 整体比率判定（不可压缩时不分配任何 needle，避免空间泄漏）
        if !should_compress(total_original, total_compressed, DEFAULT_MIN_RATIO) {
            return Err(CompressError::NotCompressible);
        }
        let ratio_x100 = match (total_original * 100).checked_div(total_compressed) {
            Some(r) => r.min(u16::MAX as usize) as u16,
            None => u16::MAX,
        };

        // 3. 分配新 needle 并写入压缩数据（主副本 + 各副本）
        let mut new_primary: Vec<StoredFileChunk> = Vec::with_capacity(chunks.len());
        let mut new_replicas: Vec<StoredFileChunk> = Vec::with_capacity(replica_chunks.len());

        // 主副本
        for (idx, chunk) in chunks.iter().enumerate() {
            let cdata = &compressed[idx];
            let new_needle = self
                .master_client
                .assign_needle(chunk.volume_id)
                .await
                .map_err(|e| {
                    CompressError::Other(format!(
                        "assign_needle primary vol={} failed: {}",
                        chunk.volume_id, e
                    ))
                })?;
            let addr = addr_map
                .get(&chunk.volume_id)
                .ok_or_else(|| {
                    CompressError::Other(format!(
                        "primary volume {} addr not found",
                        chunk.volume_id
                    ))
                })?
                .clone();
            self.volume_client
                .write_needle(&addr, chunk.volume_id, new_needle, cdata)
                .await
                .map_err(|e| {
                    CompressError::Other(format!(
                        "write_needle primary vol={} needle={:#x} failed: {}",
                        chunk.volume_id, new_needle, e
                    ))
                })?;
            new_primary.push(StoredFileChunk {
                offset: chunk.offset,
                size: chunk.size, // 逻辑长度不变
                needle_id: new_needle,
                volume_id: chunk.volume_id,
                crc32: crc32fast::hash(cdata),
                mtime: chunk.mtime,
                is_reference: false,
                compression: algo,
            });
        }

        // 副本：每个副本写入同一份压缩数据
        for rc in replica_chunks {
            // 副本 chunk 的 offset 与主副本同 offset 的 chunk 对应
            let idx = chunks
                .iter()
                .position(|c| c.offset == rc.offset)
                .ok_or_else(|| {
                    CompressError::Other(format!(
                        "replica chunk offset {} has no matching primary",
                        rc.offset
                    ))
                })?;
            let cdata = &compressed[idx];
            let new_needle = self
                .master_client
                .assign_needle(rc.volume_id)
                .await
                .map_err(|e| {
                    CompressError::Other(format!(
                        "assign_needle replica vol={} failed: {}",
                        rc.volume_id, e
                    ))
                })?;
            let addr = addr_map
                .get(&rc.volume_id)
                .ok_or_else(|| {
                    CompressError::Other(format!("replica volume {} addr not found", rc.volume_id))
                })?
                .clone();
            self.volume_client
                .write_needle(&addr, rc.volume_id, new_needle, cdata)
                .await
                .map_err(|e| {
                    CompressError::Other(format!(
                        "write_needle replica vol={} needle={:#x} failed: {}",
                        rc.volume_id, new_needle, e
                    ))
                })?;
            new_replicas.push(StoredFileChunk {
                offset: rc.offset,
                size: rc.size,
                needle_id: new_needle,
                volume_id: rc.volume_id,
                crc32: crc32fast::hash(cdata),
                mtime: rc.mtime,
                is_reference: false,
                compression: algo,
            });
        }

        Ok(CompressedLayout {
            primary_chunks: new_primary,
            replica_chunks: new_replicas,
            ratio_x100,
            original_size: total_original as u64,
        })
    }

    /// P7: best-effort 删除压缩流程中遗留的 needle（提交失败的新 needle，
    /// 或提交成功后的旧 needle）。失败仅告警，留给后续 GC。
    async fn delete_compressed_orphans(
        &self,
        primary: &[StoredFileChunk],
        replicas: &[StoredFileChunk],
        addr_map: &std::collections::HashMap<u64, String>,
    ) {
        for c in primary.iter().chain(replicas.iter()) {
            let Some(addr) = addr_map.get(&c.volume_id) else {
                continue;
            };
            if let Err(e) = self
                .volume_client
                .delete_needle(addr, c.volume_id, c.needle_id)
                .await
            {
                warn!(
                    "P7_SCRUBBER: delete needle vol={} needle={:#x} failed (best-effort): {}",
                    c.volume_id, c.needle_id, e
                );
            }
        }
    }
}

/// P7: `compress_inode` 成功产物
struct CompressedLayout {
    primary_chunks: Vec<StoredFileChunk>,
    replica_chunks: Vec<StoredFileChunk>,
    ratio_x100: u16,
    original_size: u64,
}

/// P7: 压缩失败类型
enum CompressError {
    /// 整体压缩比低于阈值 → 标记 skip，不写元数据
    NotCompressible,
    /// 其他错误（IO/网络/CRC）→ 仅告警，下一轮重试
    Other(String),
}
