//! Zone Client - Filer → Master Zone 注册客户端
//!
//! Filer 启动时向 Master 发送 RegisterFiler 请求，获取 Zone 分配。
//! Zone 内 needle_id 由 Filer 自管理，不需要跟 Master 频繁通信。

use log::{debug, warn};
use powerfs_common::types::{ZoneInfo, ZoneVolume, make_needle_id, needle_counter, needle_zone_id};
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::{
    build_frame, ClientType, FieldId, FrameFlags, FrameHeader, HandshakeRequest,
    HandshakeResponse, MsgType, STATUS_OK, STATUS_ERR_REDIRECT,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// 向 Master 发送 RegisterFiler 请求，获取 Zone 分配 (多 Zone)。
///
/// 返回该 filer 的所有 Zone (旧 + 新):
///   - 首次注册: 返回 Vec(1) (新建 1 个 Zone)
///   - 重启再注册: 返回 Vec(N) (该 filer 的所有已有 Zone)
///
/// 参数:
///   master_addr: Master 的 "ip:port" 地址
///   filer_id: Filer 标识 (如 "filer-1" 或地址)
///
/// 注意: 使用循环处理 REDIRECT 而非递归, 避免深度重定向导致栈溢出。
pub async fn register_filer(master_addr: &str, filer_id: &str) -> Result<Vec<ZoneInfo>, String> {
    let mut current_addr = master_addr.to_string();
    // 重定向深度限制: 防止 Master 持续返回 REDIRECT 导致无限循环
    // (旧实现使用 Box::pin 递归, 在 leader 未选举或指向自身时栈溢出)
    const MAX_REDIRECTS: usize = 5;

    for depth in 0..MAX_REDIRECTS {
        debug!(
            "ZONE_CLIENT: register_filer attempt {} master={}, filer_id={}",
            depth, current_addr, filer_id
        );

        let stream = tokio::time::timeout(
            Duration::from_secs(3),
            TcpStream::connect(&current_addr),
        )
        .await
        .map_err(|_| format!("connect timeout to master {}", current_addr))?
        .map_err(|e| format!("connect failed to master {}: {}", current_addr, e))?;

        let (mut reader, mut writer) = stream.into_split();

        // 握手: Master powerfs-net 服务器要求新连接先完成握手
        // 使用时间戳作为 client_id 避免多 Filer 同时注册时 session 冲突
        let client_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1);
        let hs_req = HandshakeRequest::new(ClientType::Fuse, client_id);
        let mut hs_buf = vec![0u8; HandshakeRequest::SIZE];
        hs_req.encode(&mut hs_buf);
        writer
            .write_all(&hs_buf)
            .await
            .map_err(|e| format!("send handshake failed: {}", e))?;

        let mut hs_resp_buf = vec![0u8; HandshakeResponse::SIZE];
        tokio::time::timeout(Duration::from_secs(5), reader.read_exact(&mut hs_resp_buf))
            .await
            .map_err(|_| "handshake response timeout".to_string())?
            .map_err(|e| format!("read handshake response failed: {}", e))?;
        let hs_resp = HandshakeResponse::decode(&hs_resp_buf)
            .ok_or_else(|| "invalid handshake response".to_string())?;
        if hs_resp.status != 0 {
            return Err(format!("handshake rejected by master {}", current_addr));
        }

        // 构建 RegisterFiler 请求
        let mut enc = TlvEncoder::new();
        let _ = enc.add_string(FieldId::Owner, filer_id);

        let frame = build_frame(
            MsgType::RegisterFiler as u16,
            FrameFlags::new(FrameFlags::REQUEST),
            1,
            &enc.into_bytes(),
            &[],
        );

        writer
            .write_all(&frame)
            .await
            .map_err(|e| format!("send failed: {}", e))?;

        // 读取响应头
        let mut hdr_buf = [0u8; FrameHeader::SIZE];
        tokio::time::timeout(Duration::from_secs(5), reader.read_exact(&mut hdr_buf))
            .await
            .map_err(|_| "read header timeout".to_string())?
            .map_err(|e| format!("read header failed: {}", e))?;

        let header = FrameHeader::decode(&hdr_buf)
            .ok_or_else(|| "invalid response header".to_string())?;

        // 处理 REDIRECT: 切换到 leader 地址, 继续下一轮循环 (而非递归)
        if header.status == STATUS_ERR_REDIRECT {
            let body_len = header.data_len as usize;
            if body_len > 0 {
                let mut body = vec![0u8; body_len];
                reader.read_exact(&mut body).await
                    .map_err(|e| format!("read redirect body: {}", e))?;
                let mut dec = TlvDecoder::new(&body);
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

        if header.status != STATUS_OK {
            return Err(format!("RegisterFiler failed: status={:#06x}", header.status));
        }

        // 读取响应 body
        let body_len = header.data_len as usize;
        if body_len == 0 {
            return Err("empty response body".to_string());
        }

        let mut body = vec![0u8; body_len];
        tokio::time::timeout(Duration::from_secs(5), reader.read_exact(&mut body))
            .await
            .map_err(|_| "read body timeout".to_string())?
            .map_err(|e| format!("read body failed: {}", e))?;

        return parse_zones_response(&body, filer_id);
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
                if !addr.is_empty() {
                    physical_volumes.push(ZoneVolume {
                        volume_id,
                        addr,
                        size,
                        used,
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
        zones.iter().map(|z| z.physical_volumes.len()).sum::<usize>()
    );

    Ok(zones)
}

/// 从 chunk 映射恢复 needle_id counter。
///
/// 遍历所有 chunks，找到属于本 zone 的最大 counter，返回 max + 1。
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
    max_counter + 1
}

/// 分配 needle_id (zone_id << 40 | counter)
pub fn alloc_needle_id(zone_id: u32, counter: &std::sync::atomic::AtomicU64) -> u64 {
    let c = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    make_needle_id(zone_id, c)
}

/// 选空闲比例最大的 volume
pub fn select_volume(volumes: &[ZoneVolume]) -> Option<&ZoneVolume> {
    volumes.iter().max_by(|a, b| {
        let free_a = if a.size > 0 { 1.0 - (a.used as f64 / a.size as f64) } else { 0.0 };
        let free_b = if b.size > 0 { 1.0 - (b.used as f64 / b.size as f64) } else { 0.0 };
        free_a.partial_cmp(&free_b).unwrap_or(std::cmp::Ordering::Equal)
    })
}
