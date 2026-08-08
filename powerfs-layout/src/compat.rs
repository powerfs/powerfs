//! JSON 兼容编码 (旧客户端降级, 设计文档 S9.4)
//!
//! 旧格式:
//! - `FieldId::Chunks` (0x96) = JSON 序列化的 `Vec<ChunkRef>`
//! - 首 chunk 字段: `VolumeId`/`FileKey`/`Size` (兼容旧客户端单 chunk 读取)
//!
//! P8 (JSON 字段废弃) 时可删除此模块.

use powerfs_net::{FieldId, TlvEncoder};

use crate::encoding::{ChunkEncoding, ChunkRef};
use crate::error::{LayoutError, LayoutResult};

/// 旧格式编码: chunks 列表序列化为 JSON -> FieldId::Chunks (0x96)
/// + 首 chunk 的 VolumeId/FileKey/Size (兼容旧客户端)
///
/// TODO: P2 实现时完善, 当前为骨架
pub fn encode_chunks_json_compat(
    enc: &mut TlvEncoder,
    encoding: &ChunkEncoding,
) -> LayoutResult<()> {
    // 展开为 per-chunk 列表 (StripeDescriptor 需展开)
    let expanded = encoding.expand_to_perchunk()?;

    let chunks: &[ChunkRef] = match &expanded {
        ChunkEncoding::PerChunk { chunks } => chunks,
        ChunkEncoding::InlineData { data } => {
            // Inline 数据不编码到 Chunks 字段 (旧客户端不支持 inline)
            // 仅编码 InlineData 字段 (如果客户端支持)
            // TODO: 处理 inline 兼容性
            let _ = data;
            return Ok(());
        }
        _ => return Ok(()),
    };

    if chunks.is_empty() {
        return Ok(());
    }

    // 1. 完整列表: JSON -> FieldId::Chunks
    if let Ok(json) = serde_json::to_vec(chunks) {
        let _ = enc.add_bytes(FieldId::Chunks, &json);
    }

    // 2. 首 chunk 兼容字段: VolumeId / FileKey / Size
    let first = &chunks[0];
    let _ = enc.add_u64(FieldId::VolumeId, first.volume_id);
    let _ = enc.add_u64(FieldId::FileKey, first.needle_id);
    let _ = enc.add_u64(FieldId::Size, first.size);

    Ok(())
}

/// 旧格式解码: 从 FieldId::Chunks (JSON) 解析
///
/// TODO: P2 实现时完善
pub fn decode_chunks_json(_json: &[u8]) -> LayoutResult<ChunkEncoding> {
    let chunks: Vec<ChunkRef> = serde_json::from_slice(_json).map_err(|e| {
        LayoutError::TlvDecode(format!("JSON chunks decode failed: {}", e))
    })?;
    Ok(ChunkEncoding::PerChunk { chunks })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_json_basic() {
        let chunks = vec![ChunkRef {
            offset: 0,
            size: 1024,
            needle_id: 42,
            volume_id: 10,
            crc32: 0,
            mtime: 0,
        }];
        let json = serde_json::to_vec(&chunks).unwrap();
        let result = decode_chunks_json(&json).unwrap();
        match result {
            ChunkEncoding::PerChunk { chunks } => {
                assert_eq!(chunks.len(), 1);
                assert_eq!(chunks[0].needle_id, 42);
            }
            _ => panic!("expected PerChunk"),
        }
    }

    #[test]
    fn decode_json_invalid() {
        let result = decode_chunks_json(b"not json");
        assert!(result.is_err());
    }
}
