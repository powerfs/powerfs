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
/// InlineData 无法兼容旧客户端 (旧协议不支持 inline), 编码为空响应.
/// 旧客户端收到空 chunks 列表会触发 fallback 行为 (重新以新协议请求).
pub fn encode_chunks_json_compat(
    enc: &mut TlvEncoder,
    encoding: &ChunkEncoding,
) -> LayoutResult<()> {
    // 展开为 per-chunk 列表 (StripeDescriptor 需展开)
    let expanded = encoding.expand_to_perchunk()?;

    let chunks: &[ChunkRef] = match &expanded {
        ChunkEncoding::PerChunk { chunks } => chunks,
        ChunkEncoding::InlineData { .. } => {
            // Inline 数据无法兼容旧客户端 (旧协议不支持 inline).
            // 编码为空响应, 旧客户端会触发 fallback.
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
pub fn decode_chunks_json(json: &[u8]) -> LayoutResult<ChunkEncoding> {
    let chunks: Vec<ChunkRef> = serde_json::from_slice(json).map_err(|e| {
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

    #[test]
    fn encode_json_compat_perchunk() {
        let encoding = ChunkEncoding::PerChunk {
            chunks: vec![
                ChunkRef {
                    offset: 0,
                    size: 1024,
                    needle_id: 42,
                    volume_id: 10,
                    crc32: 0,
                    mtime: 0,
                },
                ChunkRef {
                    offset: 1024,
                    size: 2048,
                    needle_id: 43,
                    volume_id: 11,
                    crc32: 0,
                    mtime: 0,
                },
            ],
        };
        let mut enc = TlvEncoder::new();
        encode_chunks_json_compat(&mut enc, &encoding).unwrap();
        let bytes = enc.into_bytes();
        // 应包含 Chunks (JSON) + VolumeId + FileKey + Size 字段
        assert!(!bytes.is_empty());
    }

    #[test]
    fn encode_json_compat_inline_skips() {
        // InlineData 不编码任何字段 (旧客户端不支持 inline)
        let encoding = ChunkEncoding::InlineData { data: vec![1, 2, 3] };
        let mut enc = TlvEncoder::new();
        encode_chunks_json_compat(&mut enc, &encoding).unwrap();
        assert!(enc.into_bytes().is_empty());
    }

    #[test]
    fn encode_json_compat_empty_chunks() {
        let encoding = ChunkEncoding::PerChunk { chunks: vec![] };
        let mut enc = TlvEncoder::new();
        encode_chunks_json_compat(&mut enc, &encoding).unwrap();
        assert!(enc.into_bytes().is_empty());
    }

    #[test]
    fn encode_json_compat_stripe_expands() {
        // StripeDescriptor 应自动展开为 PerChunk 再编码为 JSON
        let encoding = ChunkEncoding::StripeDescriptor {
            start_needle_id: 100,
            chunk_size: 1024,
            chunk_count: 4,
            volume_ids: vec![10, 20],
            start_volume_idx: 0,
        };
        let mut enc = TlvEncoder::new();
        encode_chunks_json_compat(&mut enc, &encoding).unwrap();
        let bytes = enc.into_bytes();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn encode_decode_json_roundtrip() {
        let original = ChunkEncoding::PerChunk {
            chunks: vec![
                ChunkRef {
                    offset: 0,
                    size: 1024,
                    needle_id: 42,
                    volume_id: 10,
                    crc32: 0xdeadbeef,
                    mtime: 1234567890,
                },
            ],
        };
        // Encode to JSON compat format
        let mut enc = TlvEncoder::new();
        encode_chunks_json_compat(&mut enc, &original).unwrap();
        // Decode: 提取 Chunks 字段的 JSON 并反序列化
        let bytes = enc.into_bytes();
        // 手动查找 FieldId::Chunks (0x96) 字段
        let mut pos = 0;
        let mut json_bytes: Option<&[u8]> = None;
        while pos + 5 <= bytes.len() {
            let field_id = bytes[pos];
            let length = u32::from_be_bytes([
                bytes[pos + 1],
                bytes[pos + 2],
                bytes[pos + 3],
                bytes[pos + 4],
            ]) as usize;
            pos += 5;
            if field_id == FieldId::Chunks as u8 {
                json_bytes = Some(&bytes[pos..pos + length]);
                break;
            }
            pos += length;
        }
        let json = json_bytes.expect("Chunks field not found");
        let decoded = decode_chunks_json(json).unwrap();
        match decoded {
            ChunkEncoding::PerChunk { chunks } => {
                assert_eq!(chunks.len(), 1);
                assert_eq!(chunks[0].needle_id, 42);
                assert_eq!(chunks[0].volume_id, 10);
                assert_eq!(chunks[0].crc32, 0xdeadbeef);
                assert_eq!(chunks[0].mtime, 1234567890);
            }
            _ => panic!("expected PerChunk"),
        }
    }
}
