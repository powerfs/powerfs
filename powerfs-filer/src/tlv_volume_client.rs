//! TLV-based Volume Client — 通过 powerfs-net TLV 协议与 Volume Server 通信.
//!
//! 替代 gRPC VolumeClientPool, 因为内核客户端没有 gRPC,
//! 所有业务通信统一走 TLV (powerfs-net).
//!
//! 提供 read_needle / write_needle / delete_needle 三个方法,
//! 签名与 gRPC VolumeClientPool 一致, 便于无缝替换.

use log::debug;
use powerfs_net::{ClientConnPool, FieldId, MsgType, NetMessage, TlvEncoder, STATUS_OK};
use std::sync::Arc;

/// TLV 协议 Volume 客户端, 封装 ClientConnPool.
///
/// 所有 Filer→Volume 业务通信 (scrubber 副本复制, GC 数据回收,
/// S3 读写) 统一使用此客户端, 走 powerfs-net TLV 协议 (非 gRPC).
pub struct TlvVolumeClient {
    conn_pool: Arc<ClientConnPool>,
}

impl TlvVolumeClient {
    pub fn new(conn_pool: Arc<ClientConnPool>) -> Self {
        Self { conn_pool }
    }

    /// 返回内部 ClientConnPool 的引用 (供 scrubber 等需要直接连接的场景使用).
    pub fn conn_pool(&self) -> &Arc<ClientConnPool> {
        &self.conn_pool
    }

    /// 通过 TLV 协议向 Volume Server 写入 needle 数据.
    ///
    /// chunk 数据放在帧的 DATA 段 (最大 2MB), 不放在 TLV body (最大 256KB),
    /// 与 FUSE 客户端 build_write_tlv_with_inode 路径一致.
    pub async fn write_needle(
        &self,
        address: &str,
        volume_id: u64,
        file_key: u64,
        data: &[u8],
    ) -> Result<(), String> {
        let body = build_needle_body(volume_id, file_key);

        let client = self
            .conn_pool
            .get_or_connect_addr(address)
            .await
            .map_err(|e| format!("connect to {} failed: {}", address, e))?;

        let resp: NetMessage = client
            .send_request(MsgType::WriteNeedle, &body, data)
            .await
            .map_err(|e| format!("send WriteNeedle to {} failed: {}", address, e))?;

        if resp.header.status != STATUS_OK {
            return Err(format!(
                "WriteNeedle vol={} needle={:#x} status={}",
                volume_id, file_key, resp.header.status
            ));
        }

        debug!(
            "TLV write_needle: vol={} needle={:#x} {} bytes -> {}",
            volume_id,
            file_key,
            data.len(),
            address
        );
        Ok(())
    }

    /// 通过 TLV 协议从 Volume Server 读取 needle 数据.
    ///
    /// 响应数据在帧的 DATA 段返回.
    pub async fn read_needle(
        &self,
        address: &str,
        volume_id: u64,
        file_key: u64,
    ) -> Result<Vec<u8>, String> {
        let body = build_needle_body(volume_id, file_key);

        let client = self
            .conn_pool
            .get_or_connect_addr(address)
            .await
            .map_err(|e| format!("connect to {} failed: {}", address, e))?;

        let resp: NetMessage = client
            .send_request(MsgType::ReadNeedle, &body, &[])
            .await
            .map_err(|e| format!("send ReadNeedle to {} failed: {}", address, e))?;

        if resp.header.status != STATUS_OK {
            return Err(format!(
                "ReadNeedle vol={} needle={:#x} status={}",
                volume_id, file_key, resp.header.status
            ));
        }

        debug!(
            "TLV read_needle: vol={} needle={:#x} {} bytes <- {}",
            volume_id,
            file_key,
            resp.data.len(),
            address
        );
        Ok(resp.data)
    }

    /// 通过 TLV 协议删除 Volume Server 上的 needle.
    ///
    /// NeedleNotFound 视为幂等删除成功 (与 gRPC 路径行为一致).
    pub async fn delete_needle(
        &self,
        address: &str,
        volume_id: u64,
        file_key: u64,
    ) -> Result<(), String> {
        let body = build_needle_body(volume_id, file_key);

        let client = self
            .conn_pool
            .get_or_connect_addr(address)
            .await
            .map_err(|e| format!("connect to {} failed: {}", address, e))?;

        let resp: NetMessage = client
            .send_request(MsgType::DeleteNeedle, &body, &[])
            .await
            .map_err(|e| format!("send DeleteNeedle to {} failed: {}", address, e))?;

        // STATUS_OK = 删除成功或 NeedleNotFound (幂等)
        if resp.header.status != STATUS_OK {
            return Err(format!(
                "DeleteNeedle vol={} needle={:#x} status={}",
                volume_id, file_key, resp.header.status
            ));
        }

        debug!(
            "TLV delete_needle: vol={} needle={:#x} -> {}",
            volume_id, file_key, address
        );
        Ok(())
    }
}

/// 构建 ReadNeedle / WriteNeedle / DeleteNeedle 的 TLV body.
///
/// 字段: Ino(u64) = volume_id, FileKey(u64) = needle_id.
/// WriteNeedle 的数据放在帧的 DATA 段, 不在此 body 中.
fn build_needle_body(volume_id: u64, file_key: u64) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, volume_id);
    enc.add_u64(FieldId::FileKey, file_key);
    enc.into_bytes()
}
