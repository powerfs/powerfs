//! Zone Client - Filer → Master Zone 注册客户端
//!
//! Filer 启动时向 Master 发送 RegisterFiler 请求，获取 Zone 分配。
//! Zone 内 needle_id 由 Filer 自管理，不需要跟 Master 频繁通信。
//!
//! Phase A1: 合并了 gRPC RegisterFiler 的节点发现功能 — TLV 请求现在同时
//! 携带 Zone 分配请求和 Filer 节点发现信息 (addr, net_port, shard_ids),
//! 替代了旧的 gRPC ResilientMasterClient::register_filer 循环。

use log::{debug, warn};
use powerfs_common::types::{make_needle_id, needle_counter, needle_zone_id, ZoneInfo, ZoneVolume};
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::{FieldId, STATUS_ERR_BAD_REQUEST, STATUS_ERR_REDIRECT, STATUS_OK};

/// Filer 节点发现信息 (随 RegisterFiler 请求一起发送, 替代 gRPC RegisterFiler)
#[derive(Debug, Clone)]
pub struct FilerNodeRegistration {
    /// Filer 标识 (如 "filer-1")
    pub filer_id: String,
    /// Filer 的可到达地址 "ip:net_port" (供 kernel ListFilers 使用)
    pub advertise_addr: String,
    /// powerfs-net 端口
    pub net_port: u32,
    /// S3 HTTP server port (hosts /admin/status, /admin/shards and S3 data).
    /// Reported to Master so it can proxy shard-introspection endpoints
    /// through `GetFilerStats`.
    pub http_port: u32,
    /// Metrics HTTP server port (hosts /metrics, /admin/meta-cache-stats,
    /// /admin/lease-stats). Reported to Master so it can proxy the
    /// observability endpoints through `GetFilerStats`.
    pub metrics_port: u32,
    /// 该 filer 持有的 shard 数量
    pub shard_count: u64,
    /// 该 filer 持有的 shard id 列表
    pub shard_ids: Vec<u64>,
    /// 强制注册标志：为 true 时跳过 master 的 `shard_count` 一致性校验。
    ///
    /// 仅用于运维场景（如集群升级、临时 mismatch 调试）；正常启动应保持 false，
    /// 让 master 拒绝配置不一致的 filer 进入集群，避免路由错位。
    pub force: bool,
    /// Registration token for master authentication. None = dev mode (no
    /// token sent, master must also be in dev mode to accept).
    pub registration_token: Option<String>,
}

/// 向 Master 发送 RegisterFiler 请求，获取 Zone 分配 (多 Zone)。
///
/// 返回该 filer 的所有 Zone (旧 + 新):
///   - 首次注册: 返回 Vec(1) (新建 1 个 Zone)
///   - 重启再注册: 返回 Vec(N) (该 filer 的所有已有 Zone)
///
/// 参数:
///   master_addr: Master 的 "ip:net_port" 地址
///   reg: Filer 节点注册信息 (filer_id + advertise_addr + net_port + shard_ids)
///
/// 注意: 使用循环处理 REDIRECT 而非递归, 避免深度重定向导致栈溢出。
pub async fn register_filer(
    master_addr: &str,
    reg: &FilerNodeRegistration,
) -> Result<Vec<ZoneInfo>, String> {
    let mut current_addr = master_addr.to_string();
    // 重定向深度限制: 防止 Master 持续返回 REDIRECT 导致无限循环
    // (旧实现使用 Box::pin 递归, 在 leader 未选举或指向自身时栈溢出)
    const MAX_REDIRECTS: usize = 5;

    // Pre-encode shard_ids as packed u64 LE blob (once, reused across redirects)
    let shard_ids_blob: Vec<u8> = reg
        .shard_ids
        .iter()
        .flat_map(|id| id.to_le_bytes())
        .collect();

    for depth in 0..MAX_REDIRECTS {
        debug!(
            "ZONE_CLIENT: register_filer attempt {} master={}, filer_id={}",
            depth, current_addr, reg.filer_id
        );

        // 构建 RegisterFiler 请求 — 同时包含 Zone 分配和节点发现信息
        let mut enc = TlvEncoder::new();
        let _ = enc.add_string(FieldId::Owner, &reg.filer_id);
        let _ = enc.add_string(FieldId::FilerAddress, &reg.advertise_addr);
        let _ = enc.add_u64(FieldId::Blksize, reg.net_port as u64);
        let _ = enc.add_u64(FieldId::FilerHttpPort, reg.http_port as u64);
        let _ = enc.add_u64(FieldId::FilerMetricsPort, reg.metrics_port as u64);
        let _ = enc.add_u64(FieldId::Limit, reg.shard_count);
        if !shard_ids_blob.is_empty() {
            let _ = enc.add_bytes(FieldId::ShardIdList, &shard_ids_blob);
        }
        // Force 标志：仅当 reg.force=true 时发送，让 master 跳过 shard_count 一致性校验。
        // 旧 master 不识别此字段会忽略，所以总是发送是安全的；但仅在 force=true 时
        // 显式置 1，避免日志噪音（master 端默认按 0 处理）。
        let _ = enc.add_u8(FieldId::Force, if reg.force { 1 } else { 0 });
        // Registration token for node authentication.
        if let Some(token) = &reg.registration_token {
            if !token.is_empty() {
                let _ = enc.add_string(FieldId::RegistrationToken, token);
            }
        }
        let body = enc.into_bytes();

        // 统一 RPC 客户端 (Layer A): connect → handshake → send → read
        let client_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1);
        let reply = match powerfs_net::call_once(
            &current_addr,
            powerfs_net::ClientType::Filer,
            client_id,
            powerfs_net::CHANNEL_DATA,
            powerfs_net::MsgType::RegisterFiler,
            &body,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                return Err(format!("register_filer to {} failed: {}", current_addr, e));
            }
        };

        // 处理 REDIRECT: 切换到 leader 地址, 继续下一轮循环 (而非递归)
        if reply.status == STATUS_ERR_REDIRECT {
            if !reply.body.is_empty() {
                let mut dec = TlvDecoder::new(&reply.body);
                if let Ok(leader_addr) = dec.next_string(FieldId::Owner) {
                    if leader_addr.is_empty() {
                        return Err("redirected to empty leader address".to_string());
                    }
                    if leader_addr == current_addr {
                        return Err(format!(
                            "redirect loop: master {} points to itself",
                            current_addr
                        ));
                    }
                    warn!(
                        "ZONE_CLIENT: redirected to leader: {} (depth={})",
                        leader_addr, depth
                    );
                    current_addr = leader_addr;
                    continue;
                }
            }
            return Err("redirected but no leader address".to_string());
        }

        if reply.status == STATUS_ERR_BAD_REQUEST {
            // Master 拒绝注册：通常是 shard_count 与集群现有 filer 不一致。
            // 此错误不会因重试而消失——重试只会刷屏日志并推迟崩溃。
            // 把 master 的 detail（body 中的可读消息）原样带回，由上层
            // 决定是退出（正常启动）还是继续（force 模式已传过 force=1，
            //   不应再走到这里；走到这里说明 master 是旧版本不识别 Force）。
            let detail = String::from_utf8_lossy(&reply.body).to_string();
            return Err(format!(
                "RegisterFiler rejected by master (BAD_REQUEST): {}",
                if detail.is_empty() {
                    "shard_count mismatch (master did not provide detail)".to_string()
                } else {
                    detail
                }
            ));
        }

        if reply.status != STATUS_OK {
            return Err(format!(
                "RegisterFiler failed: status={:#06x}",
                reply.status
            ));
        }

        if reply.body.is_empty() {
            return Err("empty response body".to_string());
        }

        return parse_zones_response(&reply.body, &reg.filer_id);
    }

    Err(format!(
        "exceeded {} redirects while registering filer",
        MAX_REDIRECTS
    ))
}

/// 解析 RegisterFiler 响应 body 为 Vec<ZoneInfo>
fn parse_zones_response(body: &[u8], filer_id: &str) -> Result<Vec<ZoneInfo>, String> {
    // 多 Zone TLV:
    //   Entries(zone_count) + [ZoneId + Limit(vol_count) + [VolumeId + Owner + Size + UsedSpace] × N] × M
    let mut dec = TlvDecoder::new(body);
    let zone_count = dec.next_u64(FieldId::Entries).unwrap_or(0) as usize;

    let mut zones = Vec::with_capacity(zone_count);
    for _ in 0..zone_count {
        let zone_id = dec.next_u32(FieldId::ZoneId).unwrap_or(0);
        let vol_count = dec.next_u64(FieldId::Limit).unwrap_or(0) as usize;

        let mut physical_volumes = Vec::with_capacity(vol_count);
        for _ in 0..vol_count {
            if let Ok(volume_id) = dec.next_u64(FieldId::VolumeId) {
                let addr = dec.next_string(FieldId::Owner).unwrap_or_default();
                let size = dec.next_u64(FieldId::Size).unwrap_or(0);
                let used = dec.next_u64(FieldId::UsedSpace).unwrap_or(0);
                // node_id (FieldId::Backend) — 旧版 Master 可能不发送，默认空字符串
                let node_id = dec.next_string(FieldId::Backend).unwrap_or_default();
                if !addr.is_empty() {
                    physical_volumes.push(ZoneVolume {
                        volume_id,
                        addr,
                        size,
                        used,
                        node_id,
                    });
                }
            }
        }

        zones.push(ZoneInfo {
            zone_id,
            owner_filer_id: filer_id.to_string(),
            physical_volumes,
        });
    }

    debug!(
        "ZONE_CLIENT: registered zones={}, total_volumes={}",
        zones.len(),
        zones
            .iter()
            .map(|z| z.physical_volumes.len())
            .sum::<usize>()
    );

    Ok(zones)
}

/// Filer → Master: notify that this filer gained/lost leadership of a
/// shard Raft group. Best-effort (fire-and-forget): the filer does not
/// block on the Master's response — if the notification is lost, the
/// fuse client's `check_leader_strict` redirect fallback still works.
///
/// The Master uses this to maintain a `shard_id → leader_addr` table
/// so that fuse clients route cap RPCs directly to the shard leader on
/// the very first request (zero-redirect fast path).
///
/// TLV: ShardId(u64) + Force(u8, 1=gained 0=lost)
///      + Owner(filer_id) + FilerAddress(leader_addr)
pub async fn notify_shard_leader_change(
    master_addr: &str,
    shard_id: u64,
    is_leader: bool,
    filer_id: &str,
    leader_addr: &str,
) {
    let mut current_addr = master_addr.to_string();
    // Retry on connection failure (e.g. Master still starting up during
    // filer boot — shard_leader_notifier fires before Master's net_port
    // is listening). 10 × 3s = 30s covers Master Raft election + boot.
    const MAX_ATTEMPTS: usize = 10;

    for depth in 0..MAX_ATTEMPTS {
        let mut enc = TlvEncoder::new();
        let _ = enc.add_u64(FieldId::ShardId, shard_id);
        let _ = enc.add_u8(FieldId::Force, if is_leader { 1 } else { 0 });
        let _ = enc.add_string(FieldId::Owner, filer_id);
        let _ = enc.add_string(FieldId::FilerAddress, leader_addr);
        let body = enc.into_bytes();

        let client_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1);
        let reply = match powerfs_net::call_once(
            &current_addr,
            powerfs_net::ClientType::Filer,
            client_id,
            powerfs_net::CHANNEL_DATA,
            powerfs_net::MsgType::ShardLeaderUpdate,
            &body,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                // Connection failure (e.g. Master still booting). Retry
                // after 3s — the shard_leader_notifier task is async so
                // this does not block the filer's main runtime.
                if depth + 1 < MAX_ATTEMPTS {
                    warn!(
                        "ZONE_CLIENT: notify_shard_leader_change to {} failed: {} (shard={}, is_leader={}, attempt={}/{}), retrying in 3s",
                        current_addr, e, shard_id, is_leader, depth + 1, MAX_ATTEMPTS
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    continue;
                }
                warn!(
                    "ZONE_CLIENT: notify_shard_leader_change to {} failed after {} attempts: {} (shard={}, is_leader={})",
                    current_addr, MAX_ATTEMPTS, e, shard_id, is_leader
                );
                return;
            }
        };

        if reply.status == STATUS_ERR_REDIRECT {
            if !reply.body.is_empty() {
                let mut dec = TlvDecoder::new(&reply.body);
                if let Ok(leader) = dec.next_string(FieldId::Owner) {
                    if !leader.is_empty() && leader != current_addr {
                        current_addr = leader;
                        continue;
                    }
                }
            }
            warn!(
                "ZONE_CLIENT: shard_leader_update redirected but no leader (shard={}, depth={})",
                shard_id, depth
            );
            return;
        }

        if reply.status == STATUS_OK {
            debug!(
                "ZONE_CLIENT: shard_leader_update OK shard={} is_leader={} filer_id={} addr={}",
                shard_id, is_leader, filer_id, leader_addr
            );
            return;
        }

        warn!(
            "ZONE_CLIENT: shard_leader_update unexpected status={} (shard={}, is_leader={})",
            reply.status, shard_id, is_leader
        );
        return;
    }

    warn!(
        "ZONE_CLIENT: shard_leader_update exceeded {} attempts (shard={})",
        MAX_ATTEMPTS, shard_id
    );
}

/// 从 chunk 映射恢复 needle_id counter。
/// File-key 分配步长.
///
/// needle_id = file_key + chunk_idx (Flat 模式), 每个文件的 chunks 占用
/// [file_key, file_key + N) 的 needle_id 区间. 若 file_key 顺序递增 (步长=1),
/// 后一个文件的 file_key 会落入前一个文件的 chunk 区间, 导致 needle_id
/// 碰撞和数据覆盖.
///
/// 步长 65536 (= 2^16): 每个文件预留 65536 个 needle_id (1MB chunk × 65536
/// = 64GB 最大文件). counter 有 40 bits (1万亿/zone), 步长 65536 可容纳
/// 2^24 = 1677 万个文件/zone.
pub const FILE_KEY_STRIDE: u64 = 65536;

///
/// 遍历所有 chunks，找到属于本 zone 的最大 counter，返回 next stride boundary.
///
/// 注意: Flat 文件仅存储 chunk 0 的 needle_id (= file_key) 在 Filer 元数据中,
/// 后续 chunks (file_key+1, ...) 不存储. 因此 max_counter 可能是某个文件的
/// file_key, 其后续 chunks 仍在使用中. 必须向上取整到下一个 stride boundary,
/// 避免恢复后的 counter 落入已有文件的 chunk 区间.
pub fn recover_counter(zone_id: u32, chunks: &[(u64, u64)]) -> u64 {
    let mut max_counter = 0u64;
    for &(_, needle_id) in chunks {
        if needle_zone_id(needle_id) == zone_id {
            let c = needle_counter(needle_id);
            if c > max_counter {
                max_counter = c;
            }
        }
    }
    // 向上取整到下一个 stride boundary, 避免与已有文件的 chunks 碰撞.
    // Stripe 文件的 per-chunk 分配 (步长=1) 也在 max_counter 之下, 不受影响.
    ((max_counter / FILE_KEY_STRIDE) + 1) * FILE_KEY_STRIDE
}

/// 分配 needle_id (zone_id << 40 | counter)
///
/// 用于 Stripe 文件的 per-chunk 分配, 步长=1.
pub fn alloc_needle_id(zone_id: u32, counter: &std::sync::atomic::AtomicU64) -> u64 {
    let c = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    make_needle_id(zone_id, c)
}

/// 分配 file_key (zone_id << 40 | counter), 步长=FILE_KEY_STRIDE.
///
/// 用于 Flat 文件的 file_key 分配 (CREATE + MIGRATE_INLINE_ALLOC).
/// 客户端用 file_key + chunk_idx 计算每个 chunk 的 needle_id,
/// 步长确保不同文件的 needle_id 区间不重叠.
pub fn alloc_file_key(zone_id: u32, counter: &std::sync::atomic::AtomicU64) -> u64 {
    let c = counter.fetch_add(FILE_KEY_STRIDE, std::sync::atomic::Ordering::SeqCst);
    make_needle_id(zone_id, c)
}
