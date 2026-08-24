//! Filer Net Handler - Implements powerfs-net protocol for Filer metadata operations
//!
//! This module provides FilerNetHandler that processes powerfs-net metadata messages
//! using MetaShardManager, which is the authoritative metadata manager with sharded
//! storage, Raft consensus, and strong consistency metadata operations.

use crate::cap_manager::{CapManager, CapRevoker, CapSet, RecallTimeoutPenalty};
use crate::inode_lease_manager::InodeLeaseManager;
use crate::inode_notifier::InodeNotifier;
use crate::meta_shard_manager::{MetaShardManager, POSIX_ROOT_INODE};
use crate::raft_group_manager_v2::ShardId;
use crate::shard_store::{FileType, InodeInfo};
use crate::shard_strategy::ShardStrategy;
use log::{debug, info, warn};
use powerfs_layout::codec::{encode_file_layout, FEATURE_CHUNK_LAYOUT_V2};
use powerfs_layout::encoding::{ChunkEncoding, ChunkRef};
use powerfs_layout::layout::FileLayout;
use powerfs_layout::placement::{Placement, PlacementSpec};
use powerfs_layout::reliability::{CompressionState, Reliability, ReliabilityState};
use powerfs_net::serialize::{decode_setattr_req, EntryInfo, TlvDecoder, TlvEncoder};
use powerfs_net::server_connection::ServerConnectionManager;
use powerfs_net::{
    ClientType, FieldId, MsgType, NetError, NetHandler, NetMessage, NetResult, RequestContext,
    STATUS_ERR_BAD_REQUEST, STATUS_ERR_NOT_FOUND, STATUS_ERR_REDIRECT, STATUS_ERR_SERVER_ERROR,
    STATUS_OK,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Zone 运行时状态 (Filer 持有, 每个 Zone 独立 counter + volume 列表)
struct ZoneState {
    zone_id: u32,
    counter: std::sync::atomic::AtomicU64,
    volumes: Vec<powerfs_common::types::ZoneVolume>,
}

/// §8.3.1 bridge: adapts the sync `RevokeTimeoutPenalty` trait to
/// `powerfs_lock_health::ClientHealth::record_revoke_ack_timeout`.
/// When the filer force-reclaims a lease because the holder didn't ACK
/// a revoke within 2s, this penalizes the holder's health score —
/// repeated violations escalate to quarantine then blacklist (§8.2
/// three-layer defense). Cheap: a single in-memory HashMap bump.
#[derive(Clone)]
struct HealthPenaltyBridge {
    health: Arc<powerfs_lock_health::ClientHealth>,
}

impl crate::early_grant::RevokeTimeoutPenalty for HealthPenaltyBridge {
    fn on_revoke_ack_timeout(&self, client_id: &str) {
        self.health.record_revoke_ack_timeout(client_id);
    }
}

/// §13 Cap model penalty bridge — mirrors `HealthPenaltyBridge` but for
/// the cap manager's `RecallTimeoutPenalty` trait. When the filer
/// force-reclaims caps because the holder didn't ACK a recall within 2s,
/// this penalizes the holder's health score.
#[derive(Clone)]
struct HealthCapPenaltyBridge {
    health: Arc<powerfs_lock_health::ClientHealth>,
}

impl RecallTimeoutPenalty for HealthCapPenaltyBridge {
    fn on_recall_ack_timeout(&self, client_id: &str) {
        self.health.record_revoke_ack_timeout(client_id);
    }
}

/// §13 Net-layer `CapRevoker` implementation. Pushes `CapRecallNotify`
/// messages to clients via `ServerConnectionManager::send_notification`.
///
/// Maintains a `String → u64` client_id mapping (populated on
/// `CapOpenGrant`) because the cap manager uses string client_ids but
/// the net layer uses u64 connection IDs.
struct NetCapRevoker {
    conn_mgr: Arc<ServerConnectionManager>,
    client_id_map: Arc<Mutex<HashMap<String, u64>>>,
}

impl CapRevoker for NetCapRevoker {
    fn recall(
        &self,
        inode: u64,
        holder: &str,
        token: &str,
        caps_to_recall: CapSet,
        retained_caps: CapSet,
        new_epoch: u64,
    ) -> Result<(), String> {
        let net_client_id = {
            let map = self.client_id_map.lock().unwrap();
            map.get(holder).copied()
        };
        let net_client_id = net_client_id
            .ok_or_else(|| format!("no net connection for cap holder '{}'", holder))?;

        // Build CapRecallNotify TLV: Ino + LeaseToken + CapSet(recall) +
        // CapSet(retained) + CapEpoch.
        let mut enc = TlvEncoder::new();
        let _ = enc.add_u64(FieldId::Ino, inode);
        let _ = enc.add_string(FieldId::LeaseToken, token);
        let _ = enc.add_u8(FieldId::CapSet, caps_to_recall.0);
        // Retained caps encoded as a second CapSet field — but TLV allows
        // duplicate field ids, so we use a distinct encoding: pack both
        // into a single byte (recall in high nibble, retained in low).
        // Simpler: use CapEpoch as separator and add retained as CapSet again.
        // Actually cleanest: encode retained in a separate field. We reuse
        // CapSet field id but the client decoder reads them in order.
        // To avoid ambiguity, pack recall+retained into one byte: recall
        // in bits 0-3, retained in bits 4-7.
        let packed = (caps_to_recall.0 & 0x0F) | ((retained_caps.0 & 0x0F) << 4);
        let _ = enc.add_u8(FieldId::IsWriteOpen, packed); // repurpose as packed caps
        let _ = enc.add_u64(FieldId::CapEpoch, new_epoch);

        let msg = NetMessage::notification(MsgType::CapRecallNotify, enc.into_bytes(), Vec::new());

        match self.conn_mgr.send_notification(net_client_id, msg) {
            Ok(true) => {
                log::info!(
                    "CapRecallNotify: pushed to client {} (inode={}, token={}, recall={:?}, retained={:?}, epoch={})",
                    holder,
                    inode,
                    token,
                    caps_to_recall,
                    retained_caps,
                    new_epoch
                );
                Ok(())
            }
            Ok(false) => Err(format!(
                "cap recall notification channel full for client {}",
                holder
            )),
            Err(e) => Err(format!(
                "cap recall notification failed for client {}: {}",
                holder, e
            )),
        }
    }
}

/// Filer Net Handler implementation
pub struct FilerNetHandler {
    pub meta_shard_manager: Arc<MetaShardManager>,
    pub shard_strategy: Arc<ShardStrategy>,
    /// Net port for powerfs-net protocol (used to construct redirect addresses)
    pub net_port: u16,
    /// Inode notification broadcaster (optional, for cache invalidation)
    pub inode_notifier: Option<Arc<InodeNotifier>>,
    /// Inode metadata lease manager (方案 A, Phase 2).
    ///
    /// Backed by `powerfs-lease::MemoryLeaseStore<InodeKey>`. Raft-backed
    /// `LeasePersistence` is wired in via `InodeLeaseManager::with_persistence`
    /// during the optimization phase (see `docs/lock-optimization-plan.md`
    /// §6.3 P1) — for now state lives only in memory and is lost on leader
    /// switch; clients retry acquire on the new leader.
    pub inode_lease_mgr: Arc<InodeLeaseManager>,
    /// §13 Capability model manager (replaces inode_lease_mgr for new
    /// open paths). When `cap_enabled` is true, `handle_cap_open_grant`
    /// routes through this manager instead of the legacy lease manager.
    /// The legacy manager is kept for backward compatibility during rollout.
    pub cap_mgr: Arc<CapManager>,
    /// String→u64 client_id mapping for cap recall push notifications.
    /// The cap manager uses string client_ids (e.g. "fuse-1"), but the
    /// net layer's `ServerConnectionManager::send_notification` uses u64.
    /// Populated on `CapOpenGrant` (from `ctx.client.client_id`), looked
    /// up on recall push (NetCapRevoker) and on `CapUpgradeNotify` push.
    cap_client_id_map: Arc<Mutex<HashMap<String, u64>>>,
    /// Reverse map: u64 net client_id → string cap client_id. Used by
    /// `on_disconnect` to resolve the string client_id for
    /// `evict_session_full`. Populated alongside `cap_client_id_map`
    /// on `CapOpenGrant`.
    cap_net_to_string: Arc<Mutex<HashMap<u64, String>>>,
    /// Optional `ServerConnectionManager` for pushing cap recall/upgrade
    /// notifications to clients. `None` in tests without a transport.
    server_conn_mgr: Option<Arc<ServerConnectionManager>>,
    /// 该 Filer 拥有的所有 Zone (多 Zone 设计: 旧 + 新)
    /// 空 Vec = 未注册, 无法分配 needle_id
    zones: std::sync::RwLock<Vec<ZoneState>>,
    /// P4: Filer-side allocation decision logic (extracted to powerfs-allocator).
    /// Owns the zone round-robin counter. The filer keeps needle_id execution.
    filer_allocator: powerfs_allocator::FilerAllocator,
    /// P2.5: Inline 小文件全局阈值 (字节). 0 = 禁用 (默认, 保持 Flat 行为).
    /// 大于 0 时, handle_create 对新文件返回 Placement::Inline + 该 max_size,
    /// 跳过 Volume Server 分配. 父目录 `powerfs.inline` xattr 可覆盖此值.
    /// 上限 8KB (Placement::Inline 硬上限).
    inline_max_size: std::sync::atomic::AtomicU32,
    /// L4.21 fix: Global monotonically increasing version counter for
    /// cache invalidation notifications. Initialized to current time in
    /// milliseconds to survive Filer restarts (new start time >= old time).
    ///
    /// Previously, notify_inode_change used SystemTime::now().as_secs()
    /// (1-second resolution). Multiple operations within the same second
    /// produced the same version, causing the client's is_duplicate check
    /// (version <= last_seen) to suppress subsequent notifications —
    /// clients only saw the first change per second, missing concurrent
    /// appends from other clients.
    version_counter: std::sync::atomic::AtomicU64,
}

/// P2.5: Inline 模式硬上限 (Placement::Inline 的 max_size 不可超过此值)
pub const INLINE_HARD_LIMIT: u32 = 8 * 1024;
/// P2.5: `powerfs.inline` xattr key (存于父目录 InodeInfo.extended)
pub const INLINE_XATTR_KEY: &str = "powerfs.inline";
/// P3: `powerfs.placement` xattr key (存于目录 InodeInfo.extended)
/// 值格式: `flat` | `stripe:<count>:<size>` | `wide_stripe:<count>:<size>`
pub const PLACEMENT_XATTR_KEY: &str = "powerfs.placement";

/// K3: 从 chunks 列表推断 Placement (用于 GETATTR/LOOKUP 响应编码).
///
/// InodeInfo 不持久化 placement 字段 (CREATE 时由父目录 xattr 决定,
/// 但不存入文件自身元数据). GETATTR 时需从 chunks 结构反推:
///
/// - **Flat**: 所有 chunk 同一 volume_id (单卷模型, chunk_size=1MB)
/// - **Stripe**: chunks 跨多个 volume_id (anti-affinity 分配,
///   每个 stripe unit 一个 chunk, stripe_size 从 offset 差值推断)
///
/// 边界情况:
/// - 0 chunks: Flat (新创建未写入的文件)
/// - 1 chunk: Flat (单 chunk 无法判断是否 Stripe)
/// - 所有 chunk 同 volume: Flat (即使 offset 间隔大, 也按 Flat 处理)
///
/// stripe_size 推断: chunks[1].offset - chunks[0].offset.
/// 若 chunks 未排序或 offset 不均匀, 兜底用 1MB (POWERFS_CHUNK_SIZE).
fn detect_placement_from_chunks(chunks: &[ChunkRef]) -> Placement {
    // K3-DBG: log chunks for stripe detection diagnosis
    let vid_list: Vec<u64> = chunks.iter().map(|c| c.volume_id).collect();
    let off_list: Vec<u64> = chunks.iter().map(|c| c.offset).collect();
    info!(
        "K3-DBG detect_placement: chunks={} volume_ids={:?} offsets={:?}",
        chunks.len(),
        vid_list,
        off_list
    );

    if chunks.len() < 2 {
        return Placement::Flat;
    }

    // 检查是否跨多个 volume (Stripe 的必要条件)
    let first_vid = chunks[0].volume_id;
    let multi_volume = chunks.iter().any(|c| c.volume_id != first_vid);
    if !multi_volume {
        return Placement::Flat;
    }

    // Stripe: 推断 stripe_size 从前两个 chunk 的 offset 差值
    let stripe_size = if chunks.len() >= 2 && chunks[0].offset < chunks[1].offset {
        chunks[1].offset - chunks[0].offset
    } else {
        // 兜底: 1MB (对齐 POWERFS_CHUNK_SIZE)
        1024 * 1024
    };

    // 收集 volume_ids (按 chunk 顺序, 每个 chunk 代表一个 stripe unit)
    let volume_ids: Vec<u64> = chunks.iter().map(|c| c.volume_id).collect();

    Placement::Stripe {
        stripe_size,
        stripe_count: volume_ids.len() as u32,
        start_volume_idx: 0,
        volume_ids,
    }
}

impl FilerNetHandler {
    pub fn new(
        meta_shard_manager: Arc<MetaShardManager>,
        shard_strategy: Arc<ShardStrategy>,
        net_port: u16,
    ) -> Self {
        // Phase 3: wire MetaCache into the lease manager so grant/
        // release automatically bumps/decrements inode refcounts.
        let mc = meta_shard_manager.meta_cache();
        Self {
            meta_shard_manager,
            shard_strategy,
            net_port,
            inode_notifier: None,
            inode_lease_mgr: Arc::new(InodeLeaseManager::new().with_meta_cache(mc.clone())),
            cap_mgr: Arc::new(CapManager::new().with_meta_cache(mc)),
            cap_client_id_map: Arc::new(Mutex::new(HashMap::new())),
            cap_net_to_string: Arc::new(Mutex::new(HashMap::new())),
            server_conn_mgr: None,
            zones: std::sync::RwLock::new(Vec::new()),
            filer_allocator: powerfs_allocator::FilerAllocator::new(),
            inline_max_size: std::sync::atomic::AtomicU32::new(0),
            version_counter: Self::init_version_counter(),
        }
    }

    /// Create a new FilerNetHandler with InodeNotifier support
    pub fn with_notifier(
        meta_shard_manager: Arc<MetaShardManager>,
        shard_strategy: Arc<ShardStrategy>,
        net_port: u16,
        inode_notifier: Arc<InodeNotifier>,
    ) -> Self {
        // Phase 3: wire MetaCache into the lease manager (same as new()).
        let mc = meta_shard_manager.meta_cache();
        Self {
            meta_shard_manager,
            shard_strategy,
            net_port,
            inode_notifier: Some(inode_notifier),
            inode_lease_mgr: Arc::new(InodeLeaseManager::new().with_meta_cache(mc.clone())),
            cap_mgr: Arc::new(CapManager::new().with_meta_cache(mc)),
            cap_client_id_map: Arc::new(Mutex::new(HashMap::new())),
            cap_net_to_string: Arc::new(Mutex::new(HashMap::new())),
            server_conn_mgr: None,
            zones: std::sync::RwLock::new(Vec::new()),
            filer_allocator: powerfs_allocator::FilerAllocator::new(),
            inline_max_size: std::sync::atomic::AtomicU32::new(0),
            version_counter: Self::init_version_counter(),
        }
    }

    /// Phase 5 §5.3: attach a Raft-backed `LeasePersistence` to the
    /// inode lease manager so lease state survives leader switches.
    ///
    /// This is the production wiring point. The persistence backend
    /// is `RaftLeasePersistence` (constructed by the filer main.rs
    /// from the `RaftGroupManagerV2` + shard store handles). When
    /// set, `acquire`/`renew`/`release` round-trip a
    /// `ShardCommand::LeasePut`/`Delete` through Raft so all replicas
    /// observe the same lease state, and `load_from_persistence` on
    /// leader takeover repopulates the in-memory store from
    /// `CF_LEASES`.
    ///
    /// Idempotent and must be called *before* any lease operation; we
    /// rebuild the manager with persistence attached, so any leases
    /// granted through the old non-persistent instance are orphaned
    /// (acceptable since this is a one-time filer-startup transition).
    #[must_use]
    pub fn with_lease_persistence<P>(mut self, backend: P) -> Self
    where
        P: powerfs_lease::persistence::LeasePersistence + 'static,
    {
        let rebuilt = (*self.inode_lease_mgr).clone().with_persistence(backend);
        self.inode_lease_mgr = Arc::new(rebuilt);
        self
    }

    /// Phase 5 §5.3: recover lease state from persistence after this
    /// node becomes the shard leader. Loads every non-expired lease
    /// into the in-memory store and reseeds the Fencer epoch counter.
    ///
    /// Returns the count of leases recovered (excluding expired ones
    /// that `decode_entry` skipped). On error, the in-memory store is
    /// left empty — clients re-acquire on next request, matching the
    /// pre-persistence behavior, so the cluster keeps making forward
    /// progress even if recovery fails.
    pub fn recover_leases_from_persistence(&self) -> Result<usize, String> {
        let count = self
            .inode_lease_mgr
            .load_from_persistence()
            .map_err(|e| format!("recover leases: {}", e))?;
        // Best-effort epoch reseed. A failure here just means the
        // Fencer epoch stays at its in-memory initial value; zombie
        // fencing degrades to "trust" until the next save_epoch lands.
        if let Err(e) = self.inode_lease_mgr.persist_epoch() {
            log::warn!("recover_leases: epoch reseed failed (non-fatal): {}", e);
        }
        log::info!(
            "recover_leases: loaded {} non-expired leases from persistence",
            count
        );
        Ok(count)
    }

    /// §8.3.1: attach a health-penalty recorder so force-reclaimed
    /// holders get their `ClientHealth` score penalized (feeding the
    /// §8.2 three-layer defense — quarantine / blacklist after
    /// repeated violations). Mirrors `with_lease_persistence`: rebuilds
    /// the manager with the penalty attached.
    #[must_use]
    pub fn with_revoke_timeout_penalty<P>(mut self, penalty: P) -> Self
    where
        P: crate::early_grant::RevokeTimeoutPenalty + 'static,
    {
        let rebuilt = (*self.inode_lease_mgr)
            .clone()
            .with_revoke_timeout_penalty(penalty);
        self.inode_lease_mgr = Arc::new(rebuilt);
        self
    }

    /// §8.3.1 convenience: wire the filer's shared `ClientHealth` store
    /// into the lease manager's force-reclaim penalty hook. Constructs
    /// the internal `HealthPenaltyBridge` (which delegates to
    /// `ClientHealth::record_revoke_ack_timeout`) so `main.rs` doesn't
    /// need to know about the bridge type.
    #[must_use]
    pub fn with_client_health(self, health: Arc<powerfs_lock_health::ClientHealth>) -> Self {
        self.with_revoke_timeout_penalty(HealthPenaltyBridge {
            health: health.clone(),
        })
        .with_cap_penalty(HealthCapPenaltyBridge { health })
    }

    /// §13: wire the `ServerConnectionManager` into the cap manager so
    /// `CapRecallNotify` / `CapUpgradeNotify` push notifications can be
    /// delivered to clients. Also rebuilds `cap_mgr` with a real
    /// `NetCapRevoker` (replacing the default `NoopCapRevoker`).
    #[must_use]
    pub fn with_server_connection(mut self, conn_mgr: Arc<ServerConnectionManager>) -> Self {
        let revoker = Arc::new(NetCapRevoker {
            conn_mgr: conn_mgr.clone(),
            client_id_map: self.cap_client_id_map.clone(),
        });
        let mc = self.meta_shard_manager.meta_cache();
        let rebuilt = (*self.cap_mgr)
            .clone()
            .with_revoker(revoker)
            .with_meta_cache(mc);
        self.cap_mgr = Arc::new(rebuilt);
        self.server_conn_mgr = Some(conn_mgr);
        self
    }

    /// §13: attach a recall-timeout penalty hook to the cap manager.
    /// When a holder fails to ACK recall within `recall_timeout`, the
    /// `RecallTimeoutPenalty` callback fires (feeds the health tracker).
    #[must_use]
    pub fn with_cap_penalty<P>(mut self, penalty: P) -> Self
    where
        P: RecallTimeoutPenalty + 'static,
    {
        let rebuilt = (*self.cap_mgr).clone().with_penalty(Arc::new(penalty));
        self.cap_mgr = Arc::new(rebuilt);
        self
    }

    /// §13 Stage 4: 推送 `CapUpgradeNotify` 给被升级为 LONER 的客户端.
    ///
    /// 供 `force_reclaim_expired_cap_recalls` (sweep loop) /
    /// `handle_cap_release` (主动 release 触发升级) / `on_disconnect`
    /// (会话销毁触发升级) 三处复用. 通过 `cap_client_id_map` (string→u64)
    /// 查找 net 连接 ID, 构造 TLV 推送 `CapUpgradeNotify` 通知.
    ///
    /// 返回 `true` 表示推送成功 (channel 接受); `false` 表示无 conn_mgr /
    /// 无映射 / channel 满 / 推送失败 (调用方仅做日志, 不重试).
    fn push_cap_upgrade_notify(
        &self,
        inode: u64,
        survivor: &str,
        new_sn: u64,
        caps: CapSet,
    ) -> bool {
        let Some(conn_mgr) = &self.server_conn_mgr else {
            debug!(
                "push_cap_upgrade_notify: no server_conn_mgr, skip inode={} survivor={}",
                inode, survivor
            );
            return false;
        };
        let net_cid = {
            let map = self.cap_client_id_map.lock().unwrap();
            map.get(survivor).copied()
        };
        let Some(net_cid) = net_cid else {
            warn!(
                "push_cap_upgrade_notify: no net conn for survivor={} inode={}",
                survivor, inode
            );
            return false;
        };
        let token = self.cap_mgr.make_token(inode, survivor, new_sn);
        let mut enc = TlvEncoder::new();
        let _ = enc.add_u64(FieldId::Ino, inode);
        let _ = enc.add_string(FieldId::LeaseToken, &token);
        let _ = enc.add_u8(FieldId::CapSet, caps.0);
        let _ = enc.add_u64(FieldId::CapEpoch, 0);
        let _ = enc.add_u64(FieldId::CapSn, new_sn);
        let notify_msg = NetMessage::notification(
            MsgType::CapUpgradeNotify,
            enc.into_bytes(),
            Vec::new(),
        );
        match conn_mgr.send_notification(net_cid, notify_msg) {
            Ok(true) => {
                info!(
                    "CapUpgradeNotify: pushed to survivor={} inode={} sn={} caps={:?}",
                    survivor, inode, new_sn, caps
                );
                true
            }
            Ok(false) => {
                warn!(
                    "CapUpgradeNotify: channel full for survivor={} inode={}",
                    survivor, inode
                );
                false
            }
            Err(e) => {
                warn!(
                    "CapUpgradeNotify: push failed survivor={} inode={}: {}",
                    survivor, inode, e
                );
                false
            }
        }
    }

    /// §13 Stage 4: run one pass of the cap recall force-reclaim sweep.
    ///
    /// 调用 `cap_mgr.drain_expired_recalls()` 遍历所有活跃 inode 调
    /// `arbiter.tick`, 处理 GATHER 超时 force-reclaim + Loner 升级.
    /// 返回的 promote_tasks 中, `LockType::File` 的 promote 下发
    /// `CapUpgradeNotify` (其余 LockType 是元数据锁, 客户端无 cap
    /// 状态需同步, 跳过).
    ///
    /// 返回 promote 总数 (含非 File 的, 供日志统计). 由 `main.rs`
    /// 的 500ms sweep loop 调度.
    pub fn force_reclaim_expired_cap_recalls(&self) -> usize {
        let promotes = self.cap_mgr.drain_expired_recalls();
        let count = promotes.len();
        for (inode, lt, survivor, new_sn, caps) in promotes {
            if lt == crate::lock_arbiter::LockType::File {
                self.push_cap_upgrade_notify(inode, &survivor, new_sn, caps);
            } else {
                debug!(
                    "force_reclaim_expired_cap_recalls: skip non-File promote inode={} lt={:?}",
                    inode, lt
                );
            }
        }
        count
    }

    /// §13 Stage 4: 获取排他锁 (`xlock_async`), GATHER 冲突时 dispatch
    /// recall 给旧 holder 并 await GATHER 完成. 供 `handle_setattr`
    /// (Auth 锁: mode/uid/gid) / `handle_setxattr` / `handle_remove_xattr`
    /// (Xattr 锁) 复用.
    ///
    /// 返回 `Some(sn)` 成功 (调用方完成操作后必须 `arbiter.unlock(lt, sn)`
    /// 释放锁), `None` 表示 GATHER 等待失败 (waiter 被 drop, 调用方返回
    /// STATUS_ERR_SERVER_ERROR).
    ///
    /// **client_id 用 `net-{u64}` 格式**: setattr/xattr 的锁 client_id 与
    /// open_grant 的 File cap client_id (body string) 不同, 但因为操作
    /// 不同的 LockType (Auth/Xattr vs File), 不会误冲突. 同 client 的
    /// net_cid 相同, 重入安全 (xlock 同 client 唯一 holder 直接升级).
    async fn acquire_xlock(
        &self,
        inode: u64,
        lock_type: crate::lock_arbiter::LockType,
        client_id: &str,
    ) -> Option<u64> {
        use crate::lock_arbiter::{LockAcquireResult, LockType};
        let _ = LockType::File; // silence unused import if any
        let acquire = self.cap_mgr.arbiter().xlock_async(inode, lock_type, client_id);
        match acquire {
            LockAcquireResult::Granted(g) => Some(g.sn),
            LockAcquireResult::Waiting { recall_tasks, rx } => {
                // dispatch recall 给旧 holder (CapRecallNotify 推送)
                for t in &recall_tasks {
                    let token = self.cap_mgr.make_token(inode, &t.client_id, t.sn);
                    if let Err(e) = self.cap_mgr.recall_holder(
                        inode,
                        &t.client_id,
                        &token,
                        t.caps_to_recall,
                        t.retained_caps,
                        t.new_epoch,
                    ) {
                        warn!(
                            "xlock recall dispatch failed holder={} inode={}: {}",
                            t.client_id, inode, e
                        );
                    }
                }
                // await GATHER 完成 (recall_ack 推进 + wake_waiters 唤醒)
                match rx.await {
                    Ok(g) => Some(g.sn),
                    Err(_) => {
                        warn!(
                            "xlock waiter dropped inode={} client={} lt={:?}",
                            inode, client_id, lock_type
                        );
                        None
                    }
                }
            }
        }
    }

    /// §8.3.1: run one pass of the force-reclaim sweep. For each
    /// pending Early Revoke whose 2-second timeout elapsed without a
    /// `RevokeAck`, force-reclaims the stuck holder's lease, grants
    /// the next queued waiter, and penalizes the holder's health
    /// score. Returns the number of leases force-reclaimed.
    ///
    /// Intended to be called from a periodic background task (the
    /// filer spawns a 500ms `tokio::time::interval` in `main.rs`).
    pub fn force_reclaim_expired_revokes(&self) -> usize {
        self.inode_lease_mgr.force_reclaim_expired_revokes()
    }

    /// L4.21 fix: Initialize the version counter to current time in
    /// milliseconds. This ensures that after a Filer restart, the counter
    /// starts from a value >= any previously-sent version (which were also
    /// time-based), so clients don't suppress new notifications as stale.
    fn init_version_counter() -> std::sync::atomic::AtomicU64 {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(1);
        std::sync::atomic::AtomicU64::new(now_ms)
    }

    /// L4.21 fix: Generate the next monotonically increasing version number.
    /// Each call returns a unique value, guaranteeing that no two
    /// notify_inode_change calls produce the same version — even if they
    /// happen in the same millisecond. This fixes the root cause of L4.21
    /// where the client's is_duplicate check (version <= last_seen)
    /// suppressed concurrent append notifications that shared the same
    /// second-resolution timestamp.
    fn next_version(&self) -> u64 {
        self.version_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1
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

    /// P3: 解析父目录的 `powerfs.placement` xattr, 返回 PlacementSpec.
    /// 用于 handle_create 继承父目录的 placement 策略 (Flat/Stripe/WideStripe).
    /// 返回 None 表示无显式策略, 使用默认 Flat.
    fn resolve_placement_spec(&self, parent_ino: u64) -> Option<PlacementSpec> {
        let parent = self.meta_shard_manager.get_inode(parent_ino)?;
        let val = parent.extended.get(PLACEMENT_XATTR_KEY)?;
        let s = std::str::from_utf8(val).ok()?;
        match powerfs_layout::xattr::parse_placement_xattr(s) {
            Ok(spec) => {
                debug!(
                    "FILER_P3: parent {} placement xattr = {:?}",
                    parent_ino, spec
                );
                Some(spec)
            }
            Err(e) => {
                warn!(
                    "FILER_P3: parent {} invalid placement xattr '{}': {}",
                    parent_ino, s, e
                );
                None
            }
        }
    }

    /// P3: 为 Stripe 文件分配多个 (volume_id, needle_id) 对.
    /// 节点级 anti-affinity: 尽量让每个 chunk 落在不同物理节点, 实现跨节点容错.
    /// 算法: 按 node_id 分组, round-robin 跨节点选取 volume.
    ///   - 节点数 >= count: 每个 shard 落不同节点 (完美反亲和, 停 1 节点最多丢 1 shard)
    ///   - 节点数 < count: 先每节点取 1 个, 剩余按节点 round-robin 补充
    ///
    /// 返回 Vec<(volume_id, needle_id)>, 长度 == count. 若无可用 volume, 返回 None.
    pub fn alloc_for_stripe_file(&self, count: u32) -> Option<Vec<(u64, u64)>> {
        let zones = self.zones.read().unwrap();
        if zones.is_empty() {
            warn!("FILER_P3: no zones registered, cannot allocate stripe chunks");
            return None;
        }

        // P4: Build a read-only ZoneView snapshot and delegate the volume
        // selection *decision* to FilerAllocator. The filer keeps the
        // needle_id *execution* (atomic counter increment).
        let zone_views: Vec<powerfs_allocator::ZoneView> = zones
            .iter()
            .map(|z| powerfs_allocator::ZoneView {
                zone_id: z.zone_id,
                volumes: z.volumes.clone(),
            })
            .collect();

        let total_volumes: usize = zone_views.iter().map(|z| z.volumes.len()).sum();
        if total_volumes == 0 {
            warn!("FILER_P3: no volumes available for stripe allocation");
            return None;
        }

        let picks = self
            .filer_allocator
            .pick_for_stripe_file(&zone_views, count as usize)?;
        if picks.is_empty() {
            return Some(Vec::new());
        }

        // Execute: allocate needle_id from each pick's zone counter.
        let mut result = Vec::with_capacity(picks.len());
        for pick in &picks {
            let zone = zones.iter().find(|z| z.zone_id == pick.zone_id);
            if let Some(zone) = zone {
                let needle_id = crate::zone_client::alloc_needle_id(zone.zone_id, &zone.counter);
                result.push((pick.volume_id, needle_id));
            }
        }

        let unique_volumes: std::collections::HashSet<u64> =
            result.iter().map(|(v, _)| *v).collect();
        let unique_nodes: std::collections::HashSet<&str> =
            picks.iter().map(|p| p.node_id.as_str()).collect();
        let num_nodes = {
            let mut s: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for zv in &zone_views {
                for v in &zv.volumes {
                    s.insert(v.node_id.as_str());
                }
            }
            s.len()
        };
        info!(
            "FILER_P3: allocated {} stripe chunks across {} unique volumes / {} unique nodes ({} vols / {} nodes available)",
            result.len(),
            unique_volumes.len(),
            unique_nodes.len(),
            total_volumes,
            num_nodes
        );
        Some(result)
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

    /// P4: 返回所有可用 volume 的 (volume_id, addr) 列表.
    /// 供 scrubber worker 选择副本目标 volume (anti-affinity).
    pub fn get_all_volume_addrs(&self) -> Vec<(u64, String)> {
        let zones = self.zones.read().unwrap();
        let mut result = Vec::new();
        for zone in zones.iter() {
            for vol in &zone.volumes {
                result.push((vol.volume_id, vol.addr.clone()));
            }
        }
        result
    }

    /// 为新文件分配 needle_id + 选 volume (多 Zone round-robin)
    /// 返回 (volume_id, needle_id)
    fn alloc_for_new_file(&self) -> Option<(u64, u64)> {
        let zones = self.zones.read().unwrap();
        if zones.is_empty() {
            warn!("FILER_ZONE: no zones registered, cannot allocate needle_id");
            return None;
        }

        // P4: Build a read-only ZoneView snapshot and delegate the volume
        // selection *decision* to FilerAllocator (round-robin zone +
        // most-free-space volume). The filer keeps the needle_id *execution*.
        let zone_views: Vec<powerfs_allocator::ZoneView> = zones
            .iter()
            .map(|z| powerfs_allocator::ZoneView {
                zone_id: z.zone_id,
                volumes: z.volumes.clone(),
            })
            .collect();

        let pick = self.filer_allocator.pick_for_new_file(&zone_views)?;

        // Execute: allocate file_key from the picked zone's counter (with stride).
        // 使用 alloc_file_key (步长=FILE_KEY_STRIDE) 而非 alloc_needle_id (步长=1),
        // 因为 Flat 文件用 file_key + chunk_idx 计算 needle_id, 需要预留区间.
        let zone = zones.iter().find(|z| z.zone_id == pick.zone_id)?;
        let needle_id = crate::zone_client::alloc_file_key(zone.zone_id, &zone.counter);

        info!(
            "FILER_ZONE: allocated file_key={:#x} (zone={}, counter={}, stride={}) volume_id={}",
            needle_id,
            pick.zone_id,
            powerfs_common::types::needle_counter(needle_id),
            crate::zone_client::FILE_KEY_STRIDE,
            pick.volume_id
        );

        Some((pick.volume_id, needle_id))
    }

    /// Notify subscribers that an inode's metadata has changed.
    /// This is called after successful metadata mutations.
    ///
    /// Async mode (default): no-op. In async mode, propose_meta returns
    /// before Raft apply completes, so notifying subscribers would cause
    /// them to re-fetch stale (pre-apply) data and cache it. Instead, let
    /// the natural TTL/lookup cycle handle cache invalidation after apply
    /// completes. This avoids useless RPC round-trips to subscribers.
    /// Notify connected FUSE clients that an inode's metadata has changed.
    ///
    /// Modeled after  MDS `MDCache::send_dentry_unlink` (Server.cc
    /// `_unlink_local_finish`), which **unconditionally** broadcasts to
    /// all clients holding the dentry after the journal entry is applied.
    ///
    /// # CRITICAL: do NOT skip in async_meta_persist mode
    ///
    /// Previously, this function returned early when async_meta_persist was
    /// true (the default). This caused T1.1's cross-client unlink
    /// invisibility: fuse-1 deletes the file, but fuse-2 never receives an
    /// Invalidate, so its userspace cache and kernel dcache continue to
    /// report the file as existing.
    ///
    /// # Why broadcast before Raft apply is safe
    ///
    /// `delete_file` / `batch_delete_files` calls `meta_cache.stage_delete()`
    /// BEFORE the Raft propose. This means the Filer's in-memory MetaCache
    /// already marks the dir entry as Deleted when the propose is submitted.
    /// Any client that receives the Invalidate and re-queries the Filer will
    /// hit the MetaCache Deleted marker and get ENOENT — without needing
    /// the Raft log to be applied to shard_store yet.
    ///
    /// This matches  projected state model: the MDS projects the
    /// unlink (updates in-memory dentry linkage) before journaling, so
    /// client re-queries see the projected state immediately.
    ///
    /// # Broadcast vs notify
    ///
    /// Uses `broadcast` (not `notify`) because the Filer's subscriber set
    /// only includes clients that explicitly subscribed via a prior
    /// lookup/readdir. A client that cached a dentry from a previous
    /// readdir may not be in the subscriber list for the parent inode.
    /// Broadcast ensures all connected clients receive the invalidation.
    fn notify_inode_change(&self, inode: u64, version: u64) {
        if let Some(ref notifier) = self.inode_notifier {
            let notifier = notifier.clone();
            info!(
                "FILER_NET_NOTIFY: inode={}, version={}, async={}",
                inode,
                version,
                self.meta_shard_manager.is_async_meta_persist()
            );
            tokio::spawn(async move {
                // broadcast: push to ALL connected clients, not just
                // subscribers. This matches  send_dentry_unlink
                // which notifies all replica MDS nodes regardless of
                // explicit subscription.
                let count = notifier.broadcast(inode, version);
                info!(
                    "FILER_NET_NOTIFY: broadcast Invalidate(inode={}, v={}) to {} clients",
                    inode, version, count
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
        // ============================================================
        // FINAL FIX: Bypass stale-metrics-based redirect entirely.
        //
        // Historical problem (T3 mkdir timeouts):  `get_shard_leader_status`
        // returns a SNAPSHOT of Raft metrics (persisted `current_leader`)
        // that may be STALE for seconds to minutes after cluster restart
        // until a successful write commit refreshes it.  The previous code
        // returned REDIRECT based on this stale snapshot, sending clients
        // into Filer-2 ↔ Filer-3 ↔ Filer-2 A→B→A cross-filer loops that
        // even 15s meta-timeout could not escape.
        //
        // Strategy (trust LIVE Raft state only):
        //   1. If metrics optimistically say WE are leader → pass through.
        //      If we're actually a follower, propose() below will return
        //      `"not_leader: shard X requires client redirect"` with the
        //      LIVE leader from the Raft library's internal state (not
        //      metrics).  `build_err_redirect_or_server` parses the REAL
        //      `actual_shard` from the error string and sends the client
        //      DIRECTLY to the correct leader — single redirect.
        //   2. In ALL other cases (metrics say we're follower with some
        //      leader_addr, leader unknown, election in progress, etc.)
        //      → also pass through.  Same guarantee as #1: propose() is
        //      the ground truth, never stale metrics.
        //
        // This guarantees that redirect responses are ONLY ever produced
        // from `build_err_redirect_or_server` (which uses propose()'s LIVE
        // not_leader error), never from the pre-check metrics snapshot.
        // No more stale-metrics A↔B ping-pong loops in the kernel.
        // ============================================================
        let _ = msg; // msg is unused now; kept for signature compatibility
        let _ = shard_id;
        Ok(())
    }

    /// Strict leader check for RPCs that bypass Raft propose().
    ///
    /// `check_leader` intentionally lets all requests pass through and relies
    /// on `propose()` returning `not_leader` as the ground-truth fallback.
    /// That works for write-path RPCs (create/mkdir/setattr/...) because they
    /// call `propose()`. But **cap RPCs** (`CapOpenGrant` / `CapRecallAck` /
    /// `CapRelease`) operate on the in-memory `LockArbiter` state and never
    /// call `propose()` — so the fallback never fires and a Follower would
    /// silently grant caps using its own (empty) local lock state, breaking
    /// the "request must not be served by a non-leader" invariant.
    ///
    /// Design principle (hard constraint): requests must not be forwarded
    /// between services; a non-leader must reject the request and let the
    /// client reconnect to the real leader. This method enforces that for
    /// cap RPCs by checking live Raft state (not stale metrics snapshot)
    /// and returning `STATUS_ERR_REDIRECT` with the leader net address.
    async fn check_leader_strict(
        &self,
        msg: &NetMessage,
        shard_id: ShardId,
    ) -> Result<(), NetMessage> {
        let (is_leader, leader_addr) = self
            .meta_shard_manager
            .get_shard_leader_status(shard_id)
            .await
            .unwrap_or((false, String::new()));
        if is_leader {
            return Ok(());
        }
        // Not leader: build redirect response. Prefer the real leader
        // address from live metrics; if unknown (election in progress),
        // fall back to self-redirect so the client's round-robin retry
        // (ROUTE_CHECKING) can still find the leader.
        let self_grpc = self.meta_shard_manager.get_node_grpc_address();
        let self_net = Self::grpc_addr_to_net_addr(&self_grpc, self.net_port);
        let owner_net_addr = if !leader_addr.is_empty() {
            Self::grpc_addr_to_net_addr(&leader_addr, self.net_port)
        } else {
            self_net
        };
        warn!(
            "check_leader_strict: not leader for shard {}, redirecting client to {}",
            shard_id.0, owner_net_addr
        );
        let mut enc = TlvEncoder::new();
        let _ = enc.add_string(FieldId::Owner, &owner_net_addr);
        Err(Self::build_response(msg, STATUS_ERR_REDIRECT, enc.into_bytes()))
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

    /// Build a response for `meta_shard_manager` write-path errors that
    /// preserves the "no inter-service forwarding" contract.
    ///
    /// The handler-level `check_leader` gate catches obvious non-leader cases
    /// at the top of each RPC, but between that check and the actual
    /// `propose`/`propose_ff` call inside `meta_shard_manager` the Raft
    /// state can flip (election, network blip, schedule skew). When that
    /// happens `RaftGroupManager::propose{,_ff,_many}` return a string
    /// starting with `"not_leader: shard N requires client redirect ..."`
    /// instead of committing. Returning a generic SERVER_ERROR here would be
    /// catastrophic: the client sees a permanent failure, retries the SAME
    /// follower node (no shard_router update) N times, then reports IO error
    /// to the kernel. Instead we return STATUS_ERR_REDIRECT with the real
    /// leader address in the response body (TLV FieldId::Owner), so the
    /// client retry path updates `shard_router` and fires the next request
    /// straight at the correct leader.
    ///
    /// For any other error (inode full, parent missing, quota, ...) we keep
    /// STATUS_ERR_SERVER_ERROR as before.
    async fn build_err_redirect_or_server(
        &self,
        msg: &NetMessage,
        shard_id: ShardId,
        err: &str,
    ) -> NetMessage {
        let is_redirect = err.contains("not_leader") || err.contains("redirect");
        if !is_redirect {
            return Self::build_response(msg, STATUS_ERR_SERVER_ERROR, Vec::new());
        }

        // ============================================================
        // FIX 1 (cross-shard target):  meta_shard_manager.create/mkdir/…
        // hashes the content (parent inode + name) to pick a shard — which
        // is often DIFFERENT from shard_id (the header-level routing hint
        // used by check_leader).  The `not_leader:` error explicitly says
        // which shard actually failed:
        //     "not_leader: shard 1 requires client redirect (current state: Follower)"
        // Parse the real target shard out of the err string so we can
        // redirect the client to the REAL leader of shard 1, NOT the
        // stale-metrics leader of the original header shard (shard 0).
        // Without this, every propose that landed on a non-header shard
        // re-used the header shard's stale metrics and bounced the client
        // back to the same follower → infinite redirect loop.
        // ============================================================
        let actual_shard = {
            let mut parsed: Option<ShardId> = None;
            if let Some(rest) = err.strip_prefix("not_leader:") {
                // grab the next token after whitespace, expect "shard"
                let mut tok = rest.trim().split_whitespace();
                if let (Some(k), Some(num)) = (tok.next(), tok.next()) {
                    if k == "shard" {
                        if let Ok(n) = num.parse::<u64>() {
                            parsed = Some(ShardId(n));
                        }
                    }
                }
            }
            parsed.unwrap_or(shard_id)
        };

        let self_grpc = self.meta_shard_manager.get_node_grpc_address();
        let self_net = Self::grpc_addr_to_net_addr(&self_grpc, self.net_port);

        // ============================================================
        // FINAL FIX V2 — Never cross-redirect based on stale metrics.
        //
        // Historical failure (T3 mkdir 15s timeout):
        //   After cluster restart, persisted Raft metrics snapshot is
        //   STALE for seconds to minutes until a write commit refreshes
        //   it.  So we get:
        //     S2.metrics.leader = S3  (but S2 proposes and fails not_leader)
        //     S3.metrics.leader = S2  (but S3 proposes and fails not_leader)
        //   The previous code cross-redirected kernel client:
        //     S2 → redirect → S3 → redirect → S2 → redirect → S3 → ...
        //   A perfect A↔B ping-pong loop that even 15s timeout cannot
        //   escape, producing ~6000 redirects/sec and returning ETIMEDOUT.
        //
        // Solution:
        //   Since propose() has ALREADY confirmed we are NOT the leader
        //   for actual_shard (this is why we're in build_err_redirect at
        //   all), we have ZERO ground-truth confidence about WHO is.
        //   Stale metrics snapshot is an untrustworthy hint — following
        //   it creates provable infinite loops.
        //
        //   Instead, ALWAYS redirect to SELF.  The kernel client has a
        //   robust ROUTE_CHECKING round-robin mechanism with:
        //     • last_tried_conn skipping (no immediate retry on same node)
        //     • self-redirect blacklisting (stale self-targets skipped)
        //     • exponential backoff (400ms → 800ms → 1600ms → 2000ms cap)
        //     • 15s META_TIMEOUT covering full Raft election settle
        //   Worst case: kernel rotates through 3 filers in ~2-3 RTTs and
        //   lands on the real leader — GUARANTEED to terminate, no
        //   possibility of A↔B ping-pong because the kernel NEVER trusts
        //   "go to X" from a follower that already said "not I".
        //
        // (cross-shard actual_shard parse is still preserved above for
        //  logging/diagnostics; the redirect target itself is self.)
        // ============================================================
        let _ = actual_shard; // keep value for potential future logging
        let owner_net_addr = self_net.clone();

        // Log diagnostic (use `log_msg` name to avoid shadowing the NetMessage `msg` arg)
        {
            let log_msg = format!(
                "build_err_redirect: propose() not_leader actual_shard={} (hdr_shard={}), returning SELF-redirect to {} to avoid stale-metrics A-B ping-pong (kernel ROUTE_CHECKING round-robins to real leader). err={}",
                actual_shard.0,
                shard_id.0,
                owner_net_addr,
                if err.len() > 180 { &err[..180] } else { err }
            );
            warn!("{}", log_msg);
        }
        let mut enc = TlvEncoder::new();
        let _ = enc.add_string(FieldId::Owner, &owner_net_addr);
        Self::build_response(msg, STATUS_ERR_REDIRECT, enc.into_bytes())
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
            nlink: info.nlink,
            mtime: info.mtime,
            atime: info.atime,
            ctime: info.ctime,
            name: info.name.clone(),
            is_dir,
            symlink_target: if matches!(info.file_type, FileType::Symlink) {
                info.symlink_target.clone()
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
        // Symlink: target stored in symlink_target field, encode as inline_data
        // so the client can read it via InlineData layout. Without this,
        // remount/lookup returns empty target (inline_data=None, chunks=[]).
        if let Some(target) = &info.symlink_target {
            let data = target.as_bytes().to_vec();
            let max_size = (INLINE_HARD_LIMIT).max(data.len() as u32);
            let layout = FileLayout {
                placement: Placement::Inline { max_size },
                reliability: info.reliability.clone(),
                reliability_state: info.reliability_state.clone(),
                compression: info.compression_state.clone(),
                encoding: ChunkEncoding::InlineData { data },
            };
            encode_file_layout(enc, &layout, FEATURE_CHUNK_LAYOUT_V2)
                .map_err(|e| NetError::Protocol(format!("encode_file_layout failed: {}", e)))?;
            return Ok(());
        }

        // P2.5: Inline 模式 — 数据直接存 Filer 元数据, 响应携带 inline_data
        if let Some(data) = &info.inline_data {
            let max_size = (INLINE_HARD_LIMIT).max(data.len() as u32);
            let layout = FileLayout {
                placement: Placement::Inline { max_size },
                reliability: info.reliability.clone(),
                reliability_state: info.reliability_state.clone(),
                compression: info.compression_state.clone(),
                encoding: ChunkEncoding::InlineData { data: data.clone() },
            };
            encode_file_layout(enc, &layout, FEATURE_CHUNK_LAYOUT_V2)
                .map_err(|e| NetError::Protocol(format!("encode_file_layout failed: {}", e)))?;
            return Ok(());
        }

        // Empty file (no inline_data, no chunks): default to Inline mode.
        // Without this, detect_placement_from_chunks([]) returns Flat, causing
        // the kernel client to set placement=FLAT. write_end then skips the
        // Inline path, writeback fails with -EINVAL (no volume_id/file_key),
        // and close skips chunk sync. Result: data lost on remount.
        if info.chunks.is_empty() {
            let layout = FileLayout {
                placement: Placement::Inline {
                    max_size: INLINE_HARD_LIMIT,
                },
                reliability: info.reliability.clone(),
                reliability_state: info.reliability_state.clone(),
                compression: info.compression_state.clone(),
                encoding: ChunkEncoding::InlineData { data: Vec::new() },
            };
            encode_file_layout(enc, &layout, FEATURE_CHUNK_LAYOUT_V2)
                .map_err(|e| NetError::Protocol(format!("encode_file_layout failed: {}", e)))?;
            return Ok(());
        }

        // Flat / Stripe 模式 — chunk 列表
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

        // K3: 检测 Stripe 模式 — chunks 跨多个 volume (anti-affinity 分配).
        // Flat: 所有 chunk 同一 volume_id; Stripe: 每个 stripe unit 不同 volume_id.
        // stripe_size 从 chunk offset 差值推断 (chunks[1].offset - chunks[0].offset).
        let placement = detect_placement_from_chunks(&chunks);

        let layout = FileLayout {
            placement: placement.clone(),
            reliability: info.reliability.clone(),
            reliability_state: info.reliability_state.clone(),
            compression: info.compression_state.clone(),
            encoding: ChunkEncoding::PerChunk {
                chunks: chunks.clone(),
            },
        };

        encode_file_layout(enc, &layout, FEATURE_CHUNK_LAYOUT_V2)
            .map_err(|e| NetError::Protocol(format!("encode_file_layout failed: {}", e)))?;

        // 兼容字段: 直接添加 VolumeId (0x92) + FileKey (0x94), 与 CREATE 响应一致.
        // 内核 powerfs_net_lookup/getattr 用 find_u64(0x92/0x94) 解析.
        // Flat: 从第一个 chunk 提取 (单卷模型).
        // Stripe: 内核 K3 通过 volume_ids[] 数组定位, VolumeId/FileKey 仅作为
        //         base needle_id 的兜底 (file_key = chunks[0].needle_id).
        if let Some(first) = chunks.first() {
            enc.add_u64(FieldId::VolumeId, first.volume_id);
            enc.add_u64(FieldId::FileKey, first.needle_id);
        }

        // P4: 副本 chunk 列表 — 编码到 FieldId::ReplicaChunks,
        // 客户端读路径 failover 使用 (主 volume 不可用时从副本 volume 读取).
        // 格式: [count u32 LE] [ChunkRef * count] (每个 44 字节, 与 codec 格式一致).
        if !info.replica_chunks.is_empty() {
            let mut buf = Vec::with_capacity(4 + info.replica_chunks.len() * 44);
            buf.extend_from_slice(&(info.replica_chunks.len() as u32).to_le_bytes());
            for rc in &info.replica_chunks {
                buf.extend_from_slice(&rc.offset.to_le_bytes());
                buf.extend_from_slice(&rc.size.to_le_bytes());
                buf.extend_from_slice(&rc.needle_id.to_le_bytes());
                buf.extend_from_slice(&rc.volume_id.to_le_bytes());
                buf.extend_from_slice(&rc.crc32.to_le_bytes());
                buf.extend_from_slice(&rc.mtime.to_le_bytes());
            }
            let _ = enc.add_bytes(FieldId::ReplicaChunks, &buf);
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

        // Check leadership for the correct shard before reading.
        //
        // P3: Use `check_leader_strict` (not the no-op `check_leader`) for
        // lookups. This ensures the shard LEADER serves the lookup, which is
        // critical for `async_meta_persist` mode:
        //
        // - The leader stages newly created inodes/direntries in MetaCache
        //   BEFORE Raft apply completes. A lookup on the leader hits the
        //   staging cache and returns immediately.
        // - A follower has NO staging entry. If the lookup arrives before
        //   Raft replication+apply completes (typically ~1-5ms but can be
        //   longer under load), the follower returns NOT FOUND → EIO to the
        //   client. This was the root cause of cross-client "file not found"
        //   immediately after create.
        //
        // Design principle: "非leader节点不能处理请求；客户端收到非leader
        // 错误后必须重连leader" — lookups are reads but must still be served
        // by the leader for strong consistency (linearizability) with the
        // staging cache.
        let shard_id = self.shard_strategy.calculate_shard(parent_ino);
        if let Err(redirect) = self.check_leader_strict(msg, shard_id).await {
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
                    // 方案 B: 返回 inode 所在 shard_id, 客户端缓存后直接使用
                    enc.add_u64(
                        FieldId::ShardId,
                        self.shard_strategy.calculate_shard(info.inode).0,
                    );
                    return Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()));
                }
                None => {
                    warn!("FILER_NET_LOOKUP: root inode not found in database - init may not have been run");
                    return Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new()));
                }
            }
        }

        // Lookup with retry for async_meta_persist mode.
        // In async mode, create returns before Raft apply completes.
        // If lookup misses, retry briefly to allow apply to catch up.
        let lookup_result = if self.meta_shard_manager.is_async_meta_persist() {
            let mut retries = 0;
            loop {
                if let Some(info) = self.meta_shard_manager.lookup(parent_ino, name.as_str()) {
                    break Some(info);
                }
                if retries >= 20 {
                    break None;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                retries += 1;
            }
        } else {
            self.meta_shard_manager.lookup(parent_ino, name.as_str())
        };

        match lookup_result {
            Some(info) => {
                let entry_info = Self::inode_to_entry_info(&info);
                // Fetch parent directory's version (shared_gen) for dentry lease.
                // Clients use this to detect stale dentries after lease expiry.
                let dir_version = self
                    .meta_shard_manager
                    .get_inode(parent_ino)
                    .map(|p| p.version)
                    .unwrap_or(0);
                // Dentry lease TTL: 30 seconds. The client may trust this
                // dentry (positive or negative) for this duration.
                const DENTRY_LEASE_TTL_MS: u64 = 30_000;

                info!(
                    "FILER_NET_LOOKUP: returning ino={}, mode={:o}, is_dir={}, name={}, size={}, chunks={}, dir_version={}, lease_ttl={}ms",
                    entry_info.ino, entry_info.mode, entry_info.is_dir, entry_info.name, entry_info.size, info.chunks.len(), dir_version, DENTRY_LEASE_TTL_MS
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
                // 方案 B: 返回 inode 所在 shard_id, 客户端缓存后直接使用
                enc.add_u64(
                    FieldId::ShardId,
                    self.shard_strategy.calculate_shard(info.inode).0,
                );
                // Dentry lease: dir_version + TTL
                enc.add_u64(FieldId::DirVersion, dir_version);
                enc.add_u64(FieldId::DentryLeaseTtl, DENTRY_LEASE_TTL_MS);

                // 完整 chunks 列表 + 兼容旧单 chunk 字段
                Self::encode_chunks_fields(&mut enc, &info)?;

                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            None => {
                // Lookup NOT FOUND: still return dir_version + lease TTL so
                // the client can cache the NEGATIVE dentry (file doesn't exist)
                // for the lease duration, avoiding repeated lookup RPCs.
                let dir_version = self
                    .meta_shard_manager
                    .get_inode(parent_ino)
                    .map(|p| p.version)
                    .unwrap_or(0);
                const DENTRY_LEASE_TTL_MS: u64 = 30_000;

                // 调试: LOOKUP 失败时打印 directory_entries 中的 key 用于对比
                let shard_id = self.shard_strategy.calculate_shard(parent_ino);
                let entries = self.meta_shard_manager.list_directory(parent_ino);
                let entry_names: Vec<String> = entries
                    .iter()
                    .map(|e| format!("'{}'(len={})", e.name, e.name.len()))
                    .collect();
                warn!(
                    "FILER_NET_LOOKUP: NOT FOUND parent_ino={}, name='{}'(len={}), shard={}, dir_version={}, dir_entries=[{}]",
                    parent_ino, name, name.len(), shard_id.0, dir_version, entry_names.join(", ")
                );
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::DirVersion, dir_version);
                enc.add_u64(FieldId::DentryLeaseTtl, DENTRY_LEASE_TTL_MS);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_NOT_FOUND,
                    enc.into_bytes(),
                ))
            }
        }
    }

    /// Handle GetAttr request
    async fn handle_getattr(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);

        info!("FILER_NET_GETATTR: ino={}", ino);

        // inode-level read → route by calculate_shard(inode)
        // P3: Use `check_leader_strict` for the same reason as handle_lookup —
        // the leader has MetaCache staging entries for newly created inodes,
        // followers don't. See handle_lookup comment for full rationale.
        let shard_id = self.shard_strategy.calculate_shard(ino);
        if let Err(redirect) = self.check_leader_strict(msg, shard_id).await {
            return Ok(redirect);
        }

        // GetAttr with retry for async_meta_persist mode.
        //
        // In async mode, CreateInode uses propose (wait commit) + MetaCache
        // staging, so the inode is immediately visible. But UpdateInodeSizeChunks
        // uses propose_ff (fire-and-forget) — the Raft log may not have been
        // applied yet when GetAttr arrives.
        //
        // For a FUSE client that just wrote data, the local cache is
        // authoritative (it has the correct size from the write path), so it
        // should NOT call getattr at all. This retry is only for:
        //   - A newly mounted FUSE client reading an existing file
        //   - Cross-client access after cache invalidation
        //
        // In those cases, the Filer's MetaCache (if backfilled) or ShardStore
        // (after Raft apply) provides the data. The retry handles the brief
        // window between propose_ff and Raft apply.
        let inode_result = if self.meta_shard_manager.is_async_meta_persist() {
            let mut retries = 0;
            loop {
                if let Some(info) = self.meta_shard_manager.get_inode(ino) {
                    break Some(info);
                }
                if retries >= 20 {
                    break None;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                retries += 1;
            }
        } else {
            self.meta_shard_manager.get_inode(ino)
        };

        match inode_result {
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
                // 方案 B: 返回 inode 所在 shard_id, 客户端缓存后直接使用
                enc.add_u64(FieldId::ShardId, shard_id.0);
                // 完整 chunks 列表 + 兼容旧单 chunk 字段。
                // 修复历史 bug：此前 GetAttr 完全缺失 chunks 序列化，
                // 导致 fuse 端 get_entry_by_inode 拿到的 chunks 恒为空，
                // open() 时无法刷新账本，跨客户端读文件触发 I/O error。
                Self::encode_chunks_fields(&mut enc, &info)?;

                // ===== P1-5: 目录 rstat (递归累计统计) =====
                // 字段定义对齐内核 powerfs_net.h 0xCD-0xD1。
                // 当前先编码 0 占位，待 Filer 写路径 UpdateChildSummary
                // 做祖先链增量聚合 (rbytes/rfiles/rsubdirs 持久化到 inode) 后，
                // 直接改成从 info.rbytes 等字段读取即可，客户端无需改动。
                // 对非目录 inode 不编码这些字段 (内核解析侧 S_ISDIR 才回填).
                // S_IFDIR = 0o040000 (POSIX 标准), 避免引入额外 libc 依赖.
                const S_IFDIR: u32 = 0o040000;
                if (entry_info.mode & 0xF000) == S_IFDIR {
                    let rctime = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default();
                    enc.add_u64(FieldId::RBytes, 0);
                    enc.add_u64(FieldId::RFiles, 0);
                    enc.add_u64(FieldId::RSubdirs, 0);
                    enc.add_u64(FieldId::RCtimeSec, rctime.as_secs());
                    enc.add_u32(FieldId::RCtimeNsec, rctime.subsec_nanos());
                }

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
    async fn handle_setattr(&self, ctx: &RequestContext, msg: &NetMessage) -> NetResult<NetMessage> {
        // Use decode_setattr_req which correctly handles optional fields via
        // while-loop parsing. Previously used fixed-order next_u64 which
        // desynced the decoder (encoder uses add_u32 for Mode/Uid/Gid, and
        // optional fields may be absent).
        let (ino, mode, uid, gid, size, mtime, atime) = match decode_setattr_req(&msg.body) {
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
            "FILER_NET_SETATTR: ino={}, size={:?}, mode={:?}, uid={:?}, gid={:?}, mtime={:?}, atime={:?}",
            ino, size, mode, uid, gid, mtime, atime
        );

        let shard_id = self.shard_strategy.calculate_shard(ino);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        // §13 Stage 4: setattr (mode/uid/gid/size) 涉及 Auth 锁 (IAUTH),
        // 并发 chmod/truncate 必须互斥. 用 xlock_async 拿排他锁,
        // GATHER 冲突时 dispatch recall 给旧 holder + await 完成.
        // client_id 用 net-{u64} 格式 (见 acquire_xlock 文档).
        let lock_client = format!("net-{}", ctx.client.client_id);
        let sn = match self
            .acquire_xlock(ino, crate::lock_arbiter::LockType::Auth, &lock_client)
            .await
        {
            Some(sn) => sn,
            None => {
                warn!(
                    "FILER_NET_SETATTR: xlock(Auth) acquire failed ino={} client={}",
                    ino, lock_client
                );
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    b"xlock acquire failed".to_vec(),
                ));
            }
        };

        let result = self
            .meta_shard_manager
            .setattr(ino, shard_id, size, mode, uid, gid, mtime, atime)
            .await;

        // 释放 Auth 锁 (无论 setattr 成功失败都释放, 避免锁泄漏)
        self.cap_mgr
            .arbiter()
            .unlock(ino, crate::lock_arbiter::LockType::Auth, sn);

        match result {
            Ok(_) => {
                // File data truncate is handled in ShardStore::setattr
                // (Raft-replicated), so no need to truncate inline_data here.
                // Notify other clients that this inode's metadata (and
                // possibly size) changed. Without this, truncate operations
                // via SetAttr are invisible to other clients' cached metadata
                // until TTL expiry, causing stale reads.
                self.notify_inode_change(ino, self.next_version());
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!("FILER_NET_SETATTR failed: {}", e);
                Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
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
                self.notify_inode_change(ino, self.next_version());
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!("FILER_NET_SETATTR_DATA failed: {}", e);
                Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
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

        // §13 Stage 4: mtime/atime 走 ScatterLock 的 Nest 锁 (INEST),
        // 多方共享写语义 — 允许多 client 并发 mtime 更新不互斥.
        // scatter_wrlock 非阻塞 (DSCATTER 不 GATHER); 若 Nest 锁恰在
        // GATHER (quiesce/scatter xlock 中), sn=0, 此时跳过加锁走原
        // CRDT 路径 (低频场景, 不破坏一致性).
        // client_id 用 body 解析的 string; 若为空用 "setattr-meta".
        let lock_client = if client_id.is_empty() {
            "setattr-meta".to_string()
        } else {
            client_id.clone()
        };
        let sn = {
            let g = self.cap_mgr.arbiter().scatter_wrlock(
                ino,
                crate::lock_arbiter::LockType::Nest,
                &lock_client,
            );
            if g.sn != 0 {
                Some(g.sn)
            } else {
                warn!(
                    "FILER_NET_SETATTR_META: scatter_wrlock(Nest) skipped ino={} (lock in GATHER/EXCL), proceeding unlocked",
                    ino
                );
                None
            }
        };

        let result = self
            .meta_shard_manager
            .setattr_meta(
                ino, shard_id, mode, uid, gid, mtime, atime, &client_id, timestamp,
            )
            .await;

        // 释放 Nest scatter 锁 (若拿到)
        if let Some(sn) = sn {
            self.cap_mgr
                .arbiter()
                .scatter_unlock(ino, crate::lock_arbiter::LockType::Nest, sn);
        }

        match result {
            Ok(_) => {
                // Notify other clients that this inode's metadata changed
                self.notify_inode_change(ino, self.next_version());
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!("FILER_NET_SETATTR_META failed: {}", e);
                Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
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

        // Special files (block/char devices, FIFOs, sockets) have no data
        // storage — skip volume/needle/inline allocation entirely.
        const S_IFMT: u64 = 0o170000;
        const S_IFBLK: u64 = 0o060000;
        const S_IFCHR: u64 = 0o020000;
        const S_IFIFO: u64 = 0o010000;
        const S_IFSOCK: u64 = 0o140000;
        let file_type = mode & S_IFMT;
        let is_special_file = file_type == S_IFBLK
            || file_type == S_IFCHR
            || file_type == S_IFIFO
            || file_type == S_IFSOCK;

        // === P2.5/P3: 决定 Inline vs Stripe vs Flat ===
        // 优先级: 显式 Stripe/WideStripe > Inline > Flat
        // - 若父目录设了 powerfs.placement=stripe:..., 使用 Stripe (忽略 Inline)
        // - 否则 Inline (微小文件 < max_size) + Flat (大文件) 共存
        let placement_spec = self.resolve_placement_spec(parent_ino);
        let is_explicit_stripe = matches!(
            placement_spec,
            Some(PlacementSpec::Stripe { .. }) | Some(PlacementSpec::WideStripe { .. })
        );

        // Inline 仅在未显式指定 Stripe 时生效; 特殊文件不分配.
        let inline_max = if is_special_file || is_explicit_stripe {
            None
        } else {
            self.resolve_inline_max_size(parent_ino)
        };

        // P3: Stripe 模式预分配 N 个 (volume_id, needle_id)
        let stripe_alloc: Option<Vec<(u64, u64)>> = if inline_max.is_none() {
            if let Some(PlacementSpec::Stripe { count, .. }) = &placement_spec {
                match self.alloc_for_stripe_file(*count) {
                    Some(v) => Some(v),
                    None => {
                        warn!("FILER_NET_CREATE: cannot allocate {} stripe volumes", count);
                        return Ok(Self::build_response(
                            msg,
                            STATUS_ERR_SERVER_ERROR,
                            Vec::new(),
                        ));
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        // === Flat 模式: Zone 自分配 needle_id + volume_id (P2.4 核心) ===
        // 在创建 inode 之前分配, 这样若分配失败 (zone 未注册) 可直接返回错误,
        // 不会留下空 inode. 若分配成功但后续 create_file 失败, needle_id 会被
        // "泄漏" (counter 已自增但无 chunk 映射), 这是可接受的:
        //   - needle_id 空间巨大 (40 bits/zone, 1 万亿个)
        //   - counter 单调递增, 不会重复
        //
        // Inline/Stripe 模式跳过单卷分配.
        let flat_alloc = if !is_special_file && inline_max.is_none() && stripe_alloc.is_none() {
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
            // P3.1: Pass mode/uid/gid directly into create_file_with_shard so
            // they're embedded in the CreateInode command. The old code ran a
            // separate setattr() propose for these fields, which added one
            // Raft submit per create. With mode/u32 cast (next_u32 already
            // returned u32 → cast to u64 by caller, so reverse here).
            .create_file_with_shard(
                parent_ino,
                &name,
                shard_id,
                mode as u32,
                uid as u32,
                gid as u32,
            )
            .await
        {
            Ok(ino) => {
                // P3.1: SetAttr propose for mode/uid/gid is ELIMINATED.
                // The values are already baked into the CreateInode Raft log
                // entry via InodeInfo { mode, uid, gid }. This saves ~40ms of
                // Raft quorum commit latency per create on the critical path.
                let setattr_shard = self.shard_strategy.calculate_shard(ino);

                // B5: notify 目录条目变更（parent readdir 缓存 + 新 inode）
                let v = self.next_version();
                self.notify_inode_change(parent_ino, v);
                self.notify_inode_change(ino, v);

                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, ino);
                enc.add_u32(FieldId::Mode, mode as u32);
                enc.add_string(FieldId::Name, &name)?;
                // 方案 B: 返回 inode 所在 shard_id (setattr_shard = calculate_shard(ino)),
                // 客户端缓存后直接用于后续 setattr/getattr 等路由
                enc.add_u64(FieldId::ShardId, setattr_shard.0);

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
                } else if let Some(allocs) = stripe_alloc {
                    // === P3 Stripe 模式: 多 volume 并行 ===
                    // 父目录有 powerfs.placement=stripe:N:size xattr.
                    // 预分配 N 个 (volume_id, needle_id), 客户端按 Placement::locate()
                    // 决定每个 stripe unit 写入哪个 volume. CLOSE 时 sync chunks.
                    let (stripe_size, stripe_count) = match &placement_spec {
                        Some(PlacementSpec::Stripe { count, stripe_size }) => {
                            (*stripe_size, *count)
                        }
                        _ => unreachable!("stripe_alloc implies PlacementSpec::Stripe"),
                    };
                    let volume_ids: Vec<u64> = allocs.iter().map(|(v, _)| *v).collect();
                    let chunks: Vec<ChunkRef> = allocs
                        .iter()
                        .enumerate()
                        .map(|(i, (vid, nid))| ChunkRef {
                            offset: (i as u64) * stripe_size,
                            size: 0,
                            needle_id: *nid,
                            volume_id: *vid,
                            crc32: 0,
                            mtime: 0,
                        })
                        .collect();
                    info!(
                        "FILER_NET_CREATE: stripe mode inode={} count={} stripe_size={} volumes={:?}",
                        ino, stripe_count, stripe_size, volume_ids
                    );
                    let layout = FileLayout {
                        placement: Placement::Stripe {
                            stripe_size,
                            stripe_count,
                            start_volume_idx: 0,
                            volume_ids: volume_ids.clone(),
                        },
                        reliability: Reliability::SingleReplica,
                        reliability_state: ReliabilityState::default(),
                        compression: CompressionState::default(),
                        encoding: ChunkEncoding::PerChunk { chunks },
                    };
                    encode_file_layout(&mut enc, &layout, FEATURE_CHUNK_LAYOUT_V2).map_err(
                        |e| NetError::Protocol(format!("encode_file_layout failed: {}", e)),
                    )?;
                } else if is_special_file {
                    // === Special file (device/fifo/socket): no data storage ===
                    // No volume/needle/inline allocation. The inode is created
                    // with mode (S_IFBLK/S_IFCHR/S_IFIFO/S_IFSOCK) and no chunks.
                    info!(
                        "FILER_NET_CREATE: special file inode={} mode={:o} (no allocation)",
                        ino, mode
                    );
                } else {
                    // === Flat 模式: 持久化 chunk 映射 (volume_id, needle_id) via Raft ===
                    // fid 格式 "volume_id,cookie,needle_id": set_chunks 从第 3 字段解析 needle_id
                    // offset=0: 新文件首 chunk 的字节偏移 (chunk.offset, 非 needle_id)
                    // size=0: 初始无数据, 后续 write 时更新
                    let (volume_id, needle_id) = flat_alloc.unwrap();
                    let fid_str = format!("{},0,{}", volume_id, needle_id);
                    // Route by inode's own shard: set_chunks mutates the inode
                    // record on calculate_shard(ino), not the parent's shard.
                    let setchunks_shard = self.shard_strategy.calculate_shard(ino);
                    if let Err(e) = self
                        .meta_shard_manager
                        .set_chunks(ino, setchunks_shard, fid_str, volume_id, 0, 0, 0)
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
                Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
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
            // P3.1: Pass mode/uid/gid directly into create_directory so the
            // CreateInode command already carries them. Eliminates the
            // separate setattr() follow-up propose that added one Raft round
            // per mkdir. The mode/u64 → u32 truncation is safe because POSIX
            // permission + type bits fit in u32.
            .create_directory(parent_ino, &name, mode as u32, uid as u32, gid as u32)
            .await
        {
            Ok(info) => {
                let shard_id = self.shard_strategy.calculate_shard(info.inode);
                // P3.1: SetAttr propose for mode/uid/gid is ELIMINATED.
                // Already embedded in the CreateInode Raft log entry.
                // The response is built from the InodeInfo returned (already
                // carries mode/u32 with S_IFDIR set).

                // B5: notify 目录条目变更（parent readdir 缓存 + 新目录 inode）
                let v = self.next_version();
                self.notify_inode_change(parent_ino, v);
                self.notify_inode_change(info.inode, v);

                // Return full attributes so the FUSE client can populate its
                // cache with correct nlink/size/uid/gid/timestamps. Previously
                // only Ino/Mode/IsDir/Name were sent, causing stat() to report
                // nlink=0, size=0, and epoch (1970) timestamps on new dirs.
                //
                // P3.1: info.mode already carries the final mode (with
                // S_IFDIR bit set by create_directory), and info.uid/gid
                // already carry the client-supplied values — no need to
                // recompute from the original request variables.
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, info.inode);
                enc.add_u32(FieldId::Mode, info.mode);
                enc.add_u32(FieldId::Uid, info.uid);
                enc.add_u32(FieldId::Gid, info.gid);
                enc.add_u64(FieldId::Size, info.size);
                enc.add_u32(FieldId::Nlink, info.nlink);
                enc.add_u64(FieldId::Mtime, info.mtime);
                enc.add_u64(FieldId::Atime, info.atime);
                enc.add_u64(FieldId::Ctime, info.ctime);
                enc.add_u8(FieldId::IsDir, 1);
                enc.add_string(FieldId::Name, &name)?;
                // 方案 B: 返回 inode 所在 shard_id (shard_id = calculate_shard(info.inode)),
                // 客户端缓存后直接用于后续 readdir/getattr 等路由
                enc.add_u64(FieldId::ShardId, shard_id.0);
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_MKDIR failed: {}", e);
                Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
            }
        }
    }

    /// Handle MkdirPhaseA request: CreateInode on target_shard (Phase A of
    /// client-routed two-phase mkdir).
    ///
    /// The client pre-allocated `ino` via AllocInodeBatch on target_shard,
    /// then routes this request to target_shard's leader. We create ONLY the
    /// inode record (no dir entry). The client will then send MkdirPhaseB to
    /// parent_shard's leader to add the dir entry.
    ///
    /// If we are not the leader for target_shard, return STATUS_ERR_REDIRECT
    /// so the client updates its shard_router and retries.
    ///
    /// See docs/shard-routing-no-forward-principle.md §3
    async fn handle_mkdir_phase_a(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let (shard_id, ino, parent_ino, name, mode, uid, gid) =
            match powerfs_net::serialize::decode_mkdir_phase_a_req(&msg.body) {
                Ok(v) => v,
                Err(e) => {
                    warn!("FILER_NET_MKDIR_PHASE_A: decode failed: {}", e);
                    return Ok(Self::build_response(
                        msg,
                        STATUS_ERR_BAD_REQUEST,
                        Vec::new(),
                    ));
                }
            };

        info!(
            "FILER_NET_MKDIR_PHASE_A: shard={} ino={} parent={} name={} mode={:o}",
            shard_id, ino, parent_ino, name, mode
        );

        let target_shard = ShardId(shard_id);

        // Check leader — redirect if we are not the leader for target_shard
        if let Err(redirect) = self.check_leader(msg, target_shard).await {
            warn!(
                "FILER_NET_MKDIR_PHASE_A: not leader for shard {}, redirecting",
                target_shard.0
            );
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .create_directory_phase_a(ino, parent_ino, &name, mode, uid, gid)
            .await
        {
            Ok(info) => {
                // Return full attributes (same format as handle_mkdir)
                let dir_mode = mode | 0o040000;
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, info.inode);
                enc.add_u32(FieldId::Mode, dir_mode);
                enc.add_u32(FieldId::Uid, uid);
                enc.add_u32(FieldId::Gid, gid);
                enc.add_u64(FieldId::Size, info.size);
                enc.add_u32(FieldId::Nlink, info.nlink);
                enc.add_u64(FieldId::Mtime, info.mtime);
                enc.add_u64(FieldId::Atime, info.atime);
                enc.add_u64(FieldId::Ctime, info.ctime);
                enc.add_u8(FieldId::IsDir, 1);
                enc.add_string(FieldId::Name, &name)?;
                enc.add_u64(FieldId::ShardId, target_shard.0);
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_MKDIR_PHASE_A failed: {}", e);
                Ok(self
                    .build_err_redirect_or_server(msg, target_shard, &e)
                    .await)
            }
        }
    }

    /// Handle MkdirPhaseB request: AddDirEntry on parent_shard (Phase B of
    /// client-routed two-phase mkdir).
    ///
    /// The client already completed Phase A (CreateInode on target_shard) and
    /// now routes this request to parent_shard's leader to add the dir entry
    /// pointing to the new inode. We add ONLY the dir entry (no inode record).
    ///
    /// If we are not the leader for parent_shard, return STATUS_ERR_REDIRECT.
    ///
    /// See docs/shard-routing-no-forward-principle.md §3
    async fn handle_mkdir_phase_b(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let (shard_id, parent_ino, name, ino, mode, uid, gid) =
            match powerfs_net::serialize::decode_mkdir_phase_b_req(&msg.body) {
                Ok(v) => v,
                Err(e) => {
                    warn!("FILER_NET_MKDIR_PHASE_B: decode failed: {}", e);
                    return Ok(Self::build_response(
                        msg,
                        STATUS_ERR_BAD_REQUEST,
                        Vec::new(),
                    ));
                }
            };

        info!(
            "FILER_NET_MKDIR_PHASE_B: shard={} parent={} name={} ino={}",
            shard_id, parent_ino, name, ino
        );

        let parent_shard = ShardId(shard_id);

        // Check leader — redirect if we are not the leader for parent_shard
        if let Err(redirect) = self.check_leader(msg, parent_shard).await {
            warn!(
                "FILER_NET_MKDIR_PHASE_B: not leader for shard {}, redirecting",
                parent_shard.0
            );
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .create_directory_phase_b(parent_ino, &name, ino, mode, uid, gid)
            .await
        {
            Ok(()) => {
                // Notify inode change (parent readdir cache + new dir inode)
                let v = self.next_version();
                self.notify_inode_change(parent_ino, v);
                self.notify_inode_change(ino, v);

                // Phase B response: status only (Phase A already returned attrs)
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, ino);
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_MKDIR_PHASE_B failed: {}", e);
                Ok(self
                    .build_err_redirect_or_server(msg, parent_shard, &e)
                    .await)
            }
        }
    }

    /// Handle Unlink request
    async fn handle_unlink(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();

        // Directory entries live in the shard of parent_inode.
        let shard_id = self.shard_strategy.calculate_shard(parent_ino);

        info!(
            "FILER_NET_UNLINK: parent_ino={}, name={}, shard={}",
            parent_ino, name, shard_id.0
        );

        // Check leader BEFORE lookup. If we are not the leader for this
        // shard, redirect immediately — do NOT return NOT_FOUND based on
        // stale follower state.  Previously the lookup was done first; if a
        // follower whose Raft log had not yet replicated the entry returned
        // None, it sent STATUS_ERR_NOT_FOUND without ever checking leader.
        // The FUSE client mapped that to ENOENT, rm -f silently ignored it,
        // and the file survived on the real leader (intermittent delete bug).
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_UNLINK: not leader for shard {}, redirecting (parent_ino={}, name={})",
                shard_id.0, parent_ino, name
            );
            return Ok(redirect);
        }

        // We are the leader — safe to read local state.
        match self.meta_shard_manager.lookup(parent_ino, name.as_str()) {
            Some(info) => {
                info!(
                    "FILER_NET_UNLINK: found entry inode={} for '{}/{}', deleting",
                    info.inode, parent_ino, name
                );

                // Use parent_ino (not info.inode) for shard calculation so
                // the delete targets the correct shard. For hardlinks,
                // info.inode's stored parent/name may differ from the actual
                // entry being unlinked, so we must pass parent_ino/name
                // directly to delete_file instead of using delete_file_by_inode.
                match self.meta_shard_manager.delete_file(parent_ino, &name).await {
                    Ok(_) => {
                        info!(
                            "FILER_NET_UNLINK: deleted inode={} ('{}/{}')",
                            info.inode, parent_ino, name
                        );
                        // B5: notify 目录条目变更（parent readdir 缓存 + 被删 inode 失效）
                        let v = self.next_version();
                        self.notify_inode_change(parent_ino, v);
                        self.notify_inode_change(info.inode, v);
                        Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
                    }
                    Err(e) => {
                        warn!(
                            "FILER_NET_UNLINK failed: parent_ino={}, name={}, inode={}, err={}",
                            parent_ino, name, info.inode, e
                        );
                        Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
                    }
                }
            }
            None => {
                // We are the leader and the entry genuinely doesn't exist.
                warn!(
                    "FILER_NET_UNLINK: entry not found (leader confirmed) parent_ino={}, name={}",
                    parent_ino, name
                );
                Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new()))
            }
        }
    }

    /// Handle BatchUnlink: remove multiple directory entries in one RPC.
    /// All entries must belong to the same shard (caller ensures this).
    /// Uses `batch_delete_files` → `propose_many` to submit all
    /// RemoveDirEntry commands in a single Raft replication cycle,
    /// and all DeleteInode commands in one cycle per inode shard.
    ///
    /// For N entries on the same shard, Raft commits = 2 (not 2N).
    async fn handle_batch_unlink(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let entries = match powerfs_net::serialize::decode_batch_unlink_req(&msg.body) {
            Ok(e) => e,
            Err(err) => {
                warn!("FILER_NET_BATCH_UNLINK: decode failed: {}", err);
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_BAD_REQUEST,
                    Vec::new(),
                ));
            }
        };

        if entries.is_empty() {
            return Ok(Self::build_response(msg, STATUS_OK, Vec::new()));
        }

        // All entries must be in the same shard (caller groups by shard).
        let shard_id = self.shard_strategy.calculate_shard(entries[0].0);

        info!(
            "FILER_NET_BATCH_UNLINK: {} entries, shard={}",
            entries.len(),
            shard_id.0
        );

        // Check leader once for the whole batch.
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_BATCH_UNLINK: not leader for shard {}, redirecting",
                shard_id.0
            );
            return Ok(redirect);
        }

        // Batch delete via propose_many: one Raft cycle for all RemoveDirEntry,
        // one per distinct inode shard for DeleteInode/DecrementNlink.
        let results = self.meta_shard_manager.batch_delete_files(&entries).await;

        let mut statuses: Vec<u32> = Vec::with_capacity(entries.len());
        let mut any_ok = false;
        for (i, result) in results.iter().enumerate() {
            match result {
                Ok(_) => {
                    let (parent_ino, name) = &entries[i];
                    info!("FILER_NET_BATCH_UNLINK: deleted '{}/{}'", parent_ino, name);
                    let v = self.next_version();
                    self.notify_inode_change(*parent_ino, v);
                    statuses.push(STATUS_OK as u32);
                    any_ok = true;
                }
                Err(e) => {
                    let (parent_ino, name) = &entries[i];
                    let status = if e.contains("not found") {
                        STATUS_ERR_NOT_FOUND
                    } else if e.contains("not_leader") || e.contains("redirect") {
                        STATUS_ERR_REDIRECT
                    } else {
                        STATUS_ERR_SERVER_ERROR
                    };
                    warn!(
                        "FILER_NET_BATCH_UNLINK: failed '{}/{}': {} -> status={}",
                        parent_ino, name, e, status
                    );
                    statuses.push(status as u32);
                }
            }
        }

        let resp_body =
            powerfs_net::serialize::encode_batch_unlink_resp(&statuses).unwrap_or_default();
        let overall_status = if any_ok {
            STATUS_OK
        } else {
            STATUS_ERR_NOT_FOUND
        };
        Ok(Self::build_response(msg, overall_status, resp_body))
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
                self.notify_inode_change(parent_ino, self.next_version());
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!("FILER_NET_RMDIR failed: {}", e);
                if e.contains("not_leader") || e.contains("redirect") {
                    Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
                } else {
                    // Encode the error string in the body (FieldId::Name) so the
                    // FUSE client can map "not empty" -> libc::ENOTEMPTY.
                    let mut enc = TlvEncoder::new();
                    let _ = enc.add_string(FieldId::Name, &e);
                    Ok(Self::build_response(
                        msg,
                        STATUS_ERR_SERVER_ERROR,
                        enc.into_bytes(),
                    ))
                }
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

        // Look up the source inode BEFORE renaming so we can notify
        // subscribers of the renamed inode itself. Without this, clients
        // that have the old dentry cached continue to serve stale entries
        // after a rename (L4.13 dentry cache invalidation failure).
        // This mirrors handle_unlink's pattern of notifying info.inode.
        let renamed_inode = self
            .meta_shard_manager
            .lookup(old_parent_ino, &old_name)
            .map(|info| info.inode);

        match self
            .meta_shard_manager
            .rename(old_parent_ino, &old_name, new_parent_ino, &new_name)
            .await
        {
            Ok(_) => {
                // B5: notify 两个目录条目变更 + 被重命名 inode 本身
                // Notifying the renamed inode is critical: it triggers
                // FUSE_NOTIFY_INVAL_ENTRY on clients so they drop the old
                // dentry cache, and FUSE_NOTIFY_INVAL_INODE to drop page
                // cache. Without it, cross-client stat/ls on the old name
                // continues to succeed after rename.
                let v = self.next_version();
                self.notify_inode_change(old_parent_ino, v);
                self.notify_inode_change(new_parent_ino, v);
                if let Some(inode) = renamed_inode {
                    self.notify_inode_change(inode, v);
                }
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!("FILER_NET_RENAME failed: {}", e);
                Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
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

        // Check leadership for the correct shard before reading.
        // P3: Use `check_leader_strict` — newly created dir entries may only
        // exist in the leader's MetaCache staging, not yet applied on followers.
        let shard_id = self.shard_strategy.calculate_shard(parent_ino);
        if let Err(redirect) = self.check_leader_strict(msg, shard_id).await {
            return Ok(redirect);
        }

        // Paginated listing: BTreeMap seek + lightweight DirEntry (no
        // chunks/inline_data clone). Entries are already sorted by name
        // (BTreeMap ordering) and paginated at the source.
        let (entries, has_more) = self.meta_shard_manager.list_directory_paginated(
            parent_ino,
            &last_name,
            limit as usize,
        );

        // Log entry count + first few names (avoid flooding for large dirs)
        let preview: Vec<&str> = entries.iter().take(10).map(|e| e.name.as_str()).collect();
        info!(
            "FILER_NET_READDIR: parent_ino={}, count={}, has_more={}, preview={}",
            parent_ino,
            entries.len(),
            has_more,
            preview.join(","),
        );

        let mut enc = TlvEncoder::new();
        enc.add_u32(FieldId::Count, entries.len() as u32);
        enc.add_u64(FieldId::HasMore, if has_more { 1 } else { 0 });

        for entry in &entries {
            let mut entry_enc = TlvEncoder::new();
            entry_enc.add_u64(FieldId::Ino, entry.inode);
            entry_enc.add_string(FieldId::Name, &entry.name)?;
            entry_enc.add_u32(FieldId::Mode, entry.mode);
            entry_enc.add_u32(FieldId::Uid, entry.uid);
            entry_enc.add_u32(FieldId::Gid, entry.gid);
            entry_enc.add_u64(FieldId::Size, entry.size);
            entry_enc.add_u64(FieldId::Atime, entry.atime);
            entry_enc.add_u64(FieldId::Mtime, entry.mtime);
            entry_enc.add_u64(FieldId::Ctime, entry.ctime);
            entry_enc.add_u32(FieldId::Nlink, entry.nlink);
            entry_enc.add_u64(
                FieldId::ShardId,
                self.shard_strategy.calculate_shard(entry.inode).0,
            );
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
                enc.add_u32(FieldId::Mode, info.mode);
                enc.add_string(FieldId::Name, &name)?;
                enc.add_string(FieldId::SymlinkTarget, &target)?;
                // 方案 B: 返回 inode 所在 shard_id
                enc.add_u64(
                    FieldId::ShardId,
                    self.shard_strategy.calculate_shard(info.inode).0,
                );
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_SYMLINK failed: {}", e);
                Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
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
                // 方案 B: 返回 inode 所在 shard_id (inode 已存在, 路由不变)
                enc.add_u64(FieldId::ShardId, self.shard_strategy.calculate_shard(ino).0);
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_LINK failed: {}", e);
                Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
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

        // Client sends the real shard_id (e.g. target_shard for two-phase mkdir).
        // Do NOT re-map via calculate_shard() — that would treat the shard_id as
        // an inode number and route to the wrong shard (e.g. shard_id=1 maps to
        // shard 0 because inode 1 ∈ [0, 1M)). The gRPC and POSIX handlers in
        // grpc_service.rs / posix_service.rs already use shard_id directly.
        let shard_id = ShardId(shard_id_raw);
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
                if e.contains("not_leader") || e.contains("redirect") {
                    Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
                } else {
                    let mut enc = TlvEncoder::new();
                    let _ = enc.add_string(FieldId::Name, &e);
                    Ok(Self::build_response(
                        msg,
                        STATUS_ERR_SERVER_ERROR,
                        enc.into_bytes(),
                    ))
                }
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

        // IsAppend flag: when 1, the Filer appends inline_data to the
        // existing data instead of overwriting (cross-client concurrent
        // append support). Absent → false (overwrite, backward compatible).
        let is_append = dec.next_u8(FieldId::IsAppend).unwrap_or(0) != 0;

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

        // Inode-level write: route by calculate_shard(inode). Inode records
        // are now stored on their own hash-derived shard (independent of the
        // parent dir entry's shard), so this is the authoritative location.
        let shard_id = self.shard_strategy.calculate_shard(inode);
        info!(
            "FILER_NET_UPDATE_SIZE_CHUNKS: calculated shard_id={}, is_leader_check",
            shard_id.0
        );
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .update_inode_size_chunks_atomic(
                shard_id,
                inode,
                size,
                chunks,
                inline_data.clone(),
                is_append,
            )
            .await
        {
            Ok(_) => {
                // Invalidate 策略（T1.3 修复, 2026-08-22）:
                //
                // 区分 inline 小文件和 chunk 大文件:
                //
                // 1. inline 小文件 (inline_data.is_some()):
                //    数据存在 Filer 元数据中 (inline_data)，必须广播
                //    Invalidate，否则其他客户端的 inline_buffer 会 stale
                //    （读到旧数据）。这和 T1.1 unlink 的无条件广播一致。
                //
                // 2. chunk 大文件 (inline_data.is_none()):
                //    数据在 Volume Server，Filer 只存 chunks 列表 (needle_id
                //    列表)。不广播 Invalidate，因为:
                //    a) 写入者自己 (fuse-1) 收到 Invalidate 会清除
                //       chunk_cache，导致后续读取需重新从 Volume Server
                //       拉 (T1.3 bug: md5sum 读到空数据/EIO);
                //    b) 其他客户端 (fuse-2) 通过 getattr (dentry_lease
                //       过期后, ~30s) 刷新 chunks 列表，新的 needle_id
                //       不会命中旧 chunk_cache → 自然从 Volume Server 读
                //       新数据。
                //
                // 这与  MDS 的 cap recall 模型对齐：写入者 close 后
                // 不主动驱逐自己的 page cache，其他客户端通过 cap recall
                // 机制延迟刷新。
                if inline_data.is_some() {
                    self.notify_inode_change(inode, self.next_version());
                }
                // 成功: STATUS_OK + 空 body
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!("FILER_NET_UPDATE_SIZE_CHUNKS failed: {}", e);
                if e.contains("not_leader") || e.contains("redirect") {
                    Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
                } else {
                    // 失败: STATUS_ERR + FieldId::Name = error string
                    let mut enc = TlvEncoder::new();
                    let _ = enc.add_string(FieldId::Name, &e);
                    Ok(Self::build_response(
                        msg,
                        STATUS_ERR_SERVER_ERROR,
                        enc.into_bytes(),
                    ))
                }
            }
        }
    }

    /// Phase 3.5.3: 处理 fuse 端 open 时上报的 open_count 递增请求。
    ///
    /// TLV 编码: Request = ShardId + Ino
    ///           Response = OpenCount (成功) / Name=error (失败)
    async fn handle_open_count_inc(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let _shard_id_raw = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);

        // inode-level state → route by calculate_shard(inode)
        let shard_id = self.shard_strategy.calculate_shard(inode);
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
                if e.contains("not_leader") || e.contains("redirect") {
                    Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
                } else {
                    let mut enc = TlvEncoder::new();
                    let _ = enc.add_string(FieldId::Name, &e);
                    Ok(Self::build_response(
                        msg,
                        STATUS_ERR_SERVER_ERROR,
                        enc.into_bytes(),
                    ))
                }
            }
        }
    }

    /// Phase 3.5.3: 处理 fuse 端 release/close 时上报的 open_count 递减请求。
    ///
    /// TLV 编码: Request = ShardId + Ino
    ///           Response = OpenCount (成功) / Name=error (失败)
    async fn handle_open_count_dec(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let _shard_id_raw = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);

        // inode-level state → route by calculate_shard(inode)
        let shard_id = self.shard_strategy.calculate_shard(inode);
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
                if e.contains("not_leader") || e.contains("redirect") {
                    Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
                } else {
                    let mut enc = TlvEncoder::new();
                    let _ = enc.add_string(FieldId::Name, &e);
                    Ok(Self::build_response(
                        msg,
                        STATUS_ERR_SERVER_ERROR,
                        enc.into_bytes(),
                    ))
                }
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
        let _shard_id_raw = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);

        info!(
            "FILER_NET_MIGRATE_INLINE_ALLOC: shard_id(raw)={}, inode={}",
            _shard_id_raw, inode
        );

        // inode-level write → route by calculate_shard(inode)
        let shard_id = self.shard_strategy.calculate_shard(inode);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        // A6 FIX: 幂等化迁移分配。
        // 如果 inode 已经有 chunks（已被另一客户端迁移过，且 release sync 成功写回），
        // 或已经设置了 fid，则直接返回现有 (volume_id, needle_id)，绝不重新分配。
        // 否则双客户端并发 migrate 会得到两套不同的 fid → 数据写到两个独立 Volume 文件，
        // 后 sync 的那个覆盖前一个的 chunks 元数据 → 先写的客户端数据永久丢失。
        if let Some(info) = self.meta_shard_manager.get_inode(inode) {
            // Case 1: chunks 非空 → 已完成一次迁移并 sync，复用首个 chunk 的位置
            if let Some(first) = info.chunks.first() {
                info!(
                    "FILER_NET_MIGRATE_INLINE_ALLOC: inode={} IDEMPOTENT REUSE — \
                     chunks already exist, returning volume_id={} needle_id={:#x} \
                     (chunks={}, fid={:?})",
                    inode,
                    first.volume_id,
                    first.needle_id,
                    info.chunks.len(),
                    info.fid
                );
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::VolumeId, first.volume_id);
                enc.add_u64(FieldId::FileKey, first.needle_id);
                return Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()));
            }
            // Case 2: fid 已设置（但 chunks 还空，罕见边界） → 从 fid 解析
            if let Some(fid) = &info.fid {
                let parts: Vec<&str> = fid.split(',').collect();
                if parts.len() >= 3 {
                    if let (Ok(vid), Ok(nid)) = (parts[0].parse::<u64>(), parts[2].parse::<u64>()) {
                        info!(
                            "FILER_NET_MIGRATE_INLINE_ALLOC: inode={} IDEMPOTENT REUSE — \
                             fid already set, returning volume_id={} needle_id={:#x}",
                            inode, vid, nid
                        );
                        let mut enc = TlvEncoder::new();
                        enc.add_u64(FieldId::VolumeId, vid);
                        enc.add_u64(FieldId::FileKey, nid);
                        return Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()));
                    }
                }
            }
            // Case 3: info.volume_id 存在但 fid 为空的极端边界
            if let (Some(vid), true) = (info.volume_id, info.chunks.is_empty()) {
                // volume_id 已知但 fid 字符串未设置，这种情况不应发生；
                // 为安全起见仍分配新值（极端退化场景）
                warn!(
                    "FILER_NET_MIGRATE_INLINE_ALLOC: inode={} has volume_id={} but no chunks/fid string — \
                     falling back to fresh allocation", inode, vid
                );
            }
            // 无 inline_data 且无 chunks/fid：通常是新建空文件，正常分配即可
            if info.inline_data.is_none() && info.chunks.is_empty() && info.fid.is_none() {
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
                warn!("FILER_NET_MIGRATE_INLINE_ALLOC: zone not registered, cannot allocate");
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
            "FILER_NET_MIGRATE_INLINE_ALLOC: inode={} FRESH allocation volume_id={}, needle_id={:#x} \
             (inode NOT modified, inline_data preserved for crash safety)",
            inode, volume_id, needle_id
        );

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::VolumeId, volume_id);
        enc.add_u64(FieldId::FileKey, needle_id);
        Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
    }

    /// P3: Handle SetXattr request — persist an extended attribute on an inode via Raft.
    ///
    /// Request TLV: ShardId + Ino + XattrKey (string) + XattrValue (bytes)
    /// Response: status only (STATUS_OK or STATUS_ERR_*)
    ///
    /// §13 Stage 4: setxattr 涉及 Xattr 锁 (IXATTR), 并发 setxattr/
    /// removexattr 必须互斥. 用 xlock_async 拿排他锁.
    async fn handle_setxattr(&self, ctx: &RequestContext, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let _shard_id_raw = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let key = dec.next_string(FieldId::XattrKey).unwrap_or_default();
        let value = dec.next_bytes(FieldId::XattrValue).unwrap_or_default();

        // inode-level write → route by calculate_shard(inode)
        let shard_id = self.shard_strategy.calculate_shard(inode);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        info!(
            "FILER_NET_SETXATTR: inode={} key={} value_len={}",
            inode,
            key,
            value.len()
        );

        // §13 Stage 4: xlock(Xattr) 排他锁, 防止并发 setxattr/removexattr 冲突
        let lock_client = format!("net-{}", ctx.client.client_id);
        let sn = match self
            .acquire_xlock(inode, crate::lock_arbiter::LockType::Xattr, &lock_client)
            .await
        {
            Some(sn) => sn,
            None => {
                warn!(
                    "FILER_NET_SETXATTR: xlock(Xattr) acquire failed ino={} client={}",
                    inode, lock_client
                );
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    b"xlock acquire failed".to_vec(),
                ));
            }
        };

        let result = self
            .meta_shard_manager
            .set_xattr(inode, shard_id, &key, value)
            .await;

        // 释放 Xattr 锁
        self.cap_mgr
            .arbiter()
            .unlock(inode, crate::lock_arbiter::LockType::Xattr, sn);

        match result {
            Ok(()) => {
                self.notify_inode_change(inode, self.next_version());
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!("FILER_NET_SETXATTR failed: {}", e);
                Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
            }
        }
    }

    /// P3: Handle GetXattr request — read an extended attribute from an inode.
    ///
    /// Request TLV: ShardId + Ino + XattrKey (string)
    /// Response TLV: XattrValue (bytes) or STATUS_ERR_NOT_FOUND
    async fn handle_getxattr(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let _shard_id_raw = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let key = dec.next_string(FieldId::XattrKey).unwrap_or_default();

        // inode-level read → route by calculate_shard(inode)
        let shard_id = self.shard_strategy.calculate_shard(inode);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self.meta_shard_manager.get_inode(inode) {
            Some(info) => match info.extended.get(&key) {
                Some(val) => {
                    let mut enc = TlvEncoder::new();
                    if let Err(e) = enc.add_bytes(FieldId::XattrValue, val) {
                        warn!(
                            "FILER_NET_GETXATTR: encode error for inode {}: {}",
                            inode, e
                        );
                        return Ok(Self::build_response(
                            msg,
                            STATUS_ERR_SERVER_ERROR,
                            Vec::new(),
                        ));
                    }
                    Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
                }
                None => {
                    debug!("FILER_NET_GETXATTR: inode {} key {} not found", inode, key);
                    Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new()))
                }
            },
            None => {
                warn!("FILER_NET_GETXATTR: inode {} not found", inode);
                Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new()))
            }
        }
    }

    /// Handle RemoveXattr request — remove an extended attribute via Raft.
    ///
    /// Request TLV: ShardId + Ino + XattrKey (string)
    /// Response: status only.
    ///
    /// §13 Stage 4: removexattr 与 setxattr 共用 Xattr 锁 (IXATTR),
    /// 互斥保证一致性.
    async fn handle_remove_xattr(&self, ctx: &RequestContext, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let _shard_id_raw = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let key = dec.next_string(FieldId::XattrKey).unwrap_or_default();

        let shard_id = self.shard_strategy.calculate_shard(inode);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        info!("FILER_NET_REMOVEXATTR: inode={} key={}", inode, key);

        // §13 Stage 4: xlock(Xattr) 排他锁
        let lock_client = format!("net-{}", ctx.client.client_id);
        let sn = match self
            .acquire_xlock(inode, crate::lock_arbiter::LockType::Xattr, &lock_client)
            .await
        {
            Some(sn) => sn,
            None => {
                warn!(
                    "FILER_NET_REMOVEXATTR: xlock(Xattr) acquire failed ino={} client={}",
                    inode, lock_client
                );
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    b"xlock acquire failed".to_vec(),
                ));
            }
        };

        let result = self
            .meta_shard_manager
            .remove_xattr(inode, shard_id, &key)
            .await;

        // 释放 Xattr 锁
        self.cap_mgr
            .arbiter()
            .unlock(inode, crate::lock_arbiter::LockType::Xattr, sn);

        match result {
            Ok(()) => {
                self.notify_inode_change(inode, self.next_version());
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!(
                    "FILER_NET_REMOVEXATTR: failed for inode {} key {}: {}",
                    inode, key, e
                );
                Ok(self.build_err_redirect_or_server(msg, shard_id, &e).await)
            }
        }
    }

    /// Handle ListXattr request — list all extended attribute keys on an inode.
    ///
    /// Request TLV: ShardId + Ino
    /// Response TLV: XattrKeys (bytes, NUL-separated keys) or empty.
    async fn handle_list_xattr(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let _shard_id_raw = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);

        let shard_id = self.shard_strategy.calculate_shard(inode);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self.meta_shard_manager.get_inode(inode) {
            Some(info) => {
                let mut buf = Vec::new();
                for key in info.extended.keys() {
                    buf.extend_from_slice(key.as_bytes());
                    buf.push(0); // NUL separator
                }
                let mut enc = TlvEncoder::new();
                if !buf.is_empty() {
                    let _ = enc.add_bytes(FieldId::XattrKeys, &buf);
                }
                debug!(
                    "FILER_NET_LISTXATTR: inode={} keys={}",
                    inode,
                    info.extended.keys().len()
                );
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            None => {
                warn!("FILER_NET_LISTXATTR: inode {} not found", inode);
                Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new()))
            }
        }
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

    /// Handle RevokeInodeLeaseAck (phase 4 §5.2 — Early Grant).
    ///
    /// The current lease holder sends this after receiving a pushed
    /// `Revoke` (Early Revoke) notification and flushing its dirty data.
    /// The Filer releases the holder's lease and immediately grants
    /// the next queued waiter (Early Grant), without waiting for the
    /// old holder's dirty-page writeback. The SN on the new grant
    /// preserves global IO ordering.
    ///
    /// Request TLV: Ino, ClientId, LeaseToken
    /// Response: STATUS_OK / STATUS_ERR_SERVER_ERROR
    async fn handle_revoke_ack_inode_lease(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();

        if inode == 0 || client_id.is_empty() || token.is_empty() {
            warn!(
                "FILER_NET_REVOKE_ACK_INODE_LEASE: missing inode={} client_id={} token={}",
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

        // Route to shard leader (lease state lives on the leader).
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(inode);
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            return Ok(redirect);
        }

        match self
            .inode_lease_mgr
            .handle_revoke_ack(inode, &token, &client_id)
        {
            Ok(()) => {
                debug!(
                    "FILER_NET_REVOKE_ACK_INODE_LEASE: inode={} client={} (early-grant triggered)",
                    inode, client_id
                );
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!(
                    "FILER_NET_REVOKE_ACK_INODE_LEASE: failed inode={} client={}: {}",
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

    /// Deprecated: Raft inter-node messaging is now handled by openraft's gRPC
    /// RaftService (MultiRaftServiceImpl). TLV MsgType::RaftMessage is no longer used.
    async fn handle_raft_message(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        warn!("FILER_RAFT: received deprecated TLV RaftMessage; openraft uses gRPC RaftService");
        Ok(Self::build_response(
            msg,
            STATUS_ERR_SERVER_ERROR,
            b"TLV raft transport deprecated; openraft uses gRPC RaftService".to_vec(),
        ))
    }

    // ===== §13 Capability model handlers =====

    /// Handle CapOpenGrant (§13 — open never blocks).
    ///
    /// Request TLV: Ino + ClientId(string) + IsWriteOpen(u8)
    /// Response TLV: LeaseToken + CapSet(u8) + CapEpoch(u64) + CapSn(u64)
    ///
    /// **Always returns STATUS_OK** — open never blocks in the Cap model.
    /// The response carries the granted caps (EXCLUSIVE for single writer,
    /// CAP_R for reader, NONE for SHARED_WRITE participant) plus any recall
    /// tasks that the net layer dispatches asynchronously.
    ///
    /// **Stage 4**: 填充 `cap_client_id_map` (string→u64) + `cap_net_to_string`
    /// (u64→string) 双向映射, 供后续 recall/upgrade push + on_disconnect 反查.
    async fn handle_cap_open_grant(
        &self,
        ctx: &RequestContext,
        msg: &NetMessage,
    ) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let is_write_open = dec.next_u8(FieldId::IsWriteOpen).unwrap_or(0) != 0;

        if inode == 0 || client_id.is_empty() {
            warn!("CAP_OPEN_GRANT: missing inode={} or client_id empty", inode);
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_BAD_REQUEST,
                Vec::new(),
            ));
        }

        // Route to shard leader (cap state lives on the leader).
        // Strict check: cap RPCs bypass propose(), so a Follower has no
        // not_leader fallback — it must reject and redirect the client.
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(inode);
        if let Err(redirect) = self.check_leader_strict(msg, shard_id).await {
            return Ok(redirect);
        }

        // Stage 4: 填充双向 client_id 映射 (string ↔ u64).
        // net 层用 u64 连接 ID (ctx.client.client_id), cap manager 用
        // string client_id (消息 body). recall/upgrade push 需 string→u64,
        // on_disconnect 需 u64→string 反查 evict_session_full.
        {
            let net_cid = ctx.client.client_id;
            if net_cid != 0 {
                self.cap_client_id_map
                    .lock()
                    .unwrap()
                    .insert(client_id.clone(), net_cid);
                self.cap_net_to_string
                    .lock()
                    .unwrap()
                    .insert(net_cid, client_id.clone());
                debug!(
                    "CAP_OPEN_GRANT: mapped string cid '{}' ↔ net cid {}",
                    client_id, net_cid
                );
            }
        }

        // **open_grant always succeeds** — this is the core Cap model
        // invariant. No blocking, no EAGAIN, no queueing.
        let result = self.cap_mgr.open_grant(inode, &client_id, is_write_open);

        // Dispatch recall tasks asynchronously (fire-and-forget). The
        // revoker pushes CapRecallNotify to the recalled holders; open
        // returns immediately without waiting for ACK.
        for task in &result.recall_tasks {
            if let Err(e) = self.cap_mgr.recall_holder(
                inode,
                &task.holder,
                &task.token,
                task.caps_to_recall,
                task.retained_caps,
                task.new_epoch,
            ) {
                warn!(
                    "CAP_OPEN_GRANT: recall push failed for holder={} inode={}: {}",
                    task.holder, inode, e
                );
            }
        }

        // Build response: token + caps + epoch + sn.
        let mut enc = TlvEncoder::new();
        let _ = enc.add_string(FieldId::LeaseToken, &result.token);
        let _ = enc.add_u8(FieldId::CapSet, result.granted_caps.0);
        let _ = enc.add_u64(FieldId::CapEpoch, result.epoch);
        let _ = enc.add_u64(FieldId::CapSn, result.sn);
        let _ = enc.add_u64(FieldId::LeaseDuration, result.duration_ms);

        info!(
            "CAP_OPEN_GRANT: inode={} client={} write_open={} caps={:?} epoch={} sn={} recalls={}",
            inode,
            client_id,
            is_write_open,
            result.granted_caps,
            result.epoch,
            result.sn,
            result.recall_tasks.len()
        );

        Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
    }

    /// Handle CapRecallAck (§13 — client flushed dirty data, releasing caps).
    ///
    /// Request TLV: Ino + ClientId + LeaseToken
    /// Response: STATUS_OK / STATUS_ERR_SERVER_ERROR
    async fn handle_cap_recall_ack(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();

        if inode == 0 || client_id.is_empty() || token.is_empty() {
            warn!(
                "CAP_RECALL_ACK: missing inode={} client_id={} token={}",
                inode,
                !client_id.is_empty(),
                !token.is_empty()
            );
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_BAD_REQUEST,
                Vec::new(),
            ));
        }

        // Route to shard leader (strict: cap state lives only on leader).
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(inode);
        if let Err(redirect) = self.check_leader_strict(msg, shard_id).await {
            return Ok(redirect);
        }

        match self.cap_mgr.recall_ack(inode, &client_id, &token) {
            Ok(Some(upgrade)) => {
                // H1 fix: gather_done → FileLock promote_to_loner returned an
                // upgrade task. Push the survivor's CapUpgradeNotify *inline*
                // with this ACK (same network turn), so the surviving writer
                // immediately gets CAP_W|X restored instead of waiting up to
                // one sweep tick (500 ms) — which was the cause of
                // SHARED_WRITE stalls, and indirectly of L4.17's md5
                // mismatch (writer B was stuck in SHARED_WRITE without
                // CAP_X, so its writes couldn't cache → size drifted).
                self.push_cap_upgrade_notify(
                    inode,
                    &upgrade.holder,
                    upgrade.sn,
                    upgrade.granted_caps,
                );
                info!(
                    "CAP_RECALL_ACK: inode={} client={} — survivor={} promoted to LONER, CapUpgradeNotify dispatched",
                    inode, client_id, upgrade.holder
                );
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Ok(None) => {
                info!(
                    "CAP_RECALL_ACK: inode={} client={} (no Loner promote)",
                    inode, client_id
                );
                Ok(Self::build_response(msg, STATUS_OK, Vec::new()))
            }
            Err(e) => {
                warn!(
                    "CAP_RECALL_ACK: failed inode={} client={}: {}",
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

    /// Handle CapRelease (§13 — client closing the file, release caps).
    ///
    /// Request TLV: Ino + ClientId + LeaseToken
    /// Response TLV: STATUS + HasUpgrade(u8) + [if upgrade: LeaseToken +
    ///               CapSet + CapEpoch + CapSn]
    ///
    /// **Upgrade detection (§13.4 场景 3):** if releasing this holder
    /// leaves exactly one SHARED_WRITE writer, that writer is upgraded
    /// back to EXCLUSIVE_WRITE. The upgrade notification is pushed to
    /// the surviving client asynchronously.
    async fn handle_cap_release(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();

        if inode == 0 || client_id.is_empty() || token.is_empty() {
            warn!(
                "CAP_RELEASE: missing inode={} client_id={} token={}",
                inode,
                !client_id.is_empty(),
                !token.is_empty()
            );
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_BAD_REQUEST,
                Vec::new(),
            ));
        }

        // Route to shard leader (strict: cap state lives only on leader).
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(inode);
        if let Err(redirect) = self.check_leader_strict(msg, shard_id).await {
            return Ok(redirect);
        }

        match self.cap_mgr.release_cap(inode, &client_id, &token) {
            Ok(Some(upgrade)) => {
                // Stage 4: 复用 push_cap_upgrade_notify 下发升级通知给 survivor.
                self.push_cap_upgrade_notify(
                    inode,
                    &upgrade.holder,
                    upgrade.sn,
                    upgrade.granted_caps,
                );

                // Build response with upgrade info.
                let mut enc = TlvEncoder::new();
                let _ = enc.add_u8(FieldId::HasUpgrade, 1);
                let _ = enc.add_string(FieldId::LeaseToken, &upgrade.token);
                let _ = enc.add_u8(FieldId::CapSet, upgrade.granted_caps.0);
                let _ = enc.add_u64(FieldId::CapEpoch, upgrade.epoch);
                let _ = enc.add_u64(FieldId::CapSn, upgrade.sn);

                info!(
                    "CAP_RELEASE: inode={} client={} upgrade survivor={} to EXCLUSIVE",
                    inode, client_id, upgrade.holder
                );
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Ok(None) => {
                let mut enc = TlvEncoder::new();
                let _ = enc.add_u8(FieldId::HasUpgrade, 0);
                debug!(
                    "CAP_RELEASE: inode={} client={} (no upgrade)",
                    inode, client_id
                );
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!(
                    "CAP_RELEASE: failed inode={} client={}: {}",
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
}

#[async_trait::async_trait]
impl NetHandler for FilerNetHandler {
    /// §13 Stage 4: 客户端断连时清理 cap 会话.
    ///
    /// 通过反向 map (`cap_net_to_string`: u64→string) 解析 string
    /// client_id, 调 `cap_mgr.evict_session_full` 清理该 client 持有的
    /// 所有锁 (File/Auth/Xattr/...), 并对 promote_tasks (剩 1 个
    /// holder 的 inode 升级为 LONER) 下发 `CapUpgradeNotify` 给被升级
    /// 的 survivor. 最后清理双向 client_id 映射.
    ///
    /// 未走 cap 路径的连接 (纯 lease 或无 cap 操作) 在反向 map 中无
    /// 条目, 直接返回 — 不会误清理.
    async fn on_disconnect(&self, net_client_id: u64) {
        if net_client_id == 0 {
            return;
        }
        // 反查 string client_id
        let string_cid = {
            let map = self.cap_net_to_string.lock().unwrap();
            map.get(&net_client_id).cloned()
        };
        let Some(string_cid) = string_cid else {
            // 该连接未走 cap 路径 (纯 lease 或无 cap 操作), 无需清理
            return;
        };
        info!(
            "CAP on_disconnect: net_cid={} string_cid={} — evicting cap session",
            net_client_id, string_cid
        );

        // evict 该 client 的所有 cap, 拿到 promote_tasks
        let (_changed, promote_tasks) = self.cap_mgr.evict_session_full(&string_cid);
        for (inode, lt, survivor, new_sn, caps) in promote_tasks {
            if lt == crate::lock_arbiter::LockType::File {
                self.push_cap_upgrade_notify(inode, &survivor, new_sn, caps);
            } else {
                debug!(
                    "on_disconnect: skip non-File promote inode={} lt={:?}",
                    inode, lt
                );
            }
        }

        // 清理双向 map
        {
            self.cap_client_id_map
                .lock()
                .unwrap()
                .remove(&string_cid);
        }
        {
            self.cap_net_to_string
                .lock()
                .unwrap()
                .remove(&net_client_id);
        }
    }

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
            MsgType::GetAttr => {
                let response = self.handle_getattr(msg).await?;
                // Subscribe the client to the inode's notifications so it
                // receives Invalidate messages when the file is modified by
                // another client. Without this, a client that accesses a file
                // via getattr (e.g., `cat`, `md5sum`, `stat`) won't be
                // notified of changes, causing stale reads.
                if response.header.status == STATUS_OK {
                    if let Some(ref notifier) = self.inode_notifier {
                        let client_id = ctx.client.client_id;
                        let ino = TlvDecoder::new(&msg.body)
                            .next_u64(FieldId::Ino)
                            .unwrap_or(0);
                        if ino != 0 {
                            notifier.subscribe(ino, client_id);
                            info!(
                                "FILER_NET_SUBSCRIBE: client {} subscribed to inode {} (getattr)",
                                client_id, ino
                            );
                        }
                    }
                }
                Ok(response)
            }
            MsgType::SetAttr => self.handle_setattr(ctx, msg).await,
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
            MsgType::SetXattr => self.handle_setxattr(ctx, msg).await,
            MsgType::GetXattr => self.handle_getxattr(msg).await,
            MsgType::RemoveXattr => self.handle_remove_xattr(ctx, msg).await,
            MsgType::ListXattr => self.handle_list_xattr(msg).await,
            // Two-phase Mkdir (client-routed, no server-to-server forwarding)
            // See docs/shard-routing-no-forward-principle.md §3
            MsgType::MkdirPhaseA => self.handle_mkdir_phase_a(msg).await,
            MsgType::MkdirPhaseB => self.handle_mkdir_phase_b(msg).await,
            MsgType::BatchUnlink => self.handle_batch_unlink(msg).await,
            // Phase 2 / 方案 A: Inode metadata lease (Filer-managed)
            MsgType::AcquireInodeLease => self.handle_acquire_inode_lease(msg).await,
            MsgType::ReleaseInodeLease => self.handle_release_inode_lease(msg).await,
            MsgType::RenewInodeLease => self.handle_renew_inode_lease(msg).await,
            MsgType::RevokeInodeLeaseAck => self.handle_revoke_ack_inode_lease(msg).await,
            MsgType::RaftMessage => self.handle_raft_message(msg).await,
            // §13 Capability model — open_grant 需要 ctx 填充双向 client_id 映射
            MsgType::CapOpenGrant => self.handle_cap_open_grant(ctx, msg).await,
            MsgType::CapRecallAck => self.handle_cap_recall_ack(msg).await,
            MsgType::CapRelease => self.handle_cap_release(msg).await,
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
