//! TLV 二进制 encode/decode (设计文档 S9)
//!
//! 使用 `powerfs-net` 的 FieldId / TlvEncoder / TlvDecoder.
//!
//! ## 编码策略
//!
//! | 维度 | 编码方式 | 说明 |
//! |------|---------|------|
//! | Placement | 独立 FieldId 字段 | tag u8 + InlineMaxSize/StripeSize/StripeCount/StartVolumeIdx/VolumeIds |
//! | Reliability | 复合 bytes | tag + params (无独立 count/data/parity FieldId) |
//! | ReliabilityState | u8 | 0=PendingReplicated ... 4=Degraded |
//! | CompressionState | u8 | 0=None, 1=Pending, 2=Compressed |
//! | ChunkEncoding | 复合 bytes | tag + 变体数据, ChunkRef 固定 44 字节 |
//!
//! ## ChunkRef 二进制布局 (44 字节, 小端)
//!
//! | 偏移 | 长度 | 字段 |
//! |------|------|------|
//! | 0  | 8 | offset (u64) |
//! | 8  | 8 | size (u64) |
//! | 16 | 8 | needle_id (u64) |
//! | 24 | 8 | volume_id (u64) |
//! | 32 | 4 | crc32 (u32) |
//! | 36 | 8 | mtime (u64) |

use powerfs_net::{FieldId, TlvDecoder, TlvEncoder};

use crate::encoding::{ChunkEncoding, ChunkRef};
use crate::error::{LayoutError, LayoutResult};
use crate::layout::FileLayout;
use crate::placement::Placement;
use crate::reliability::{CompressionState, Reliability, ReliabilityState};

// =========================================================================
// 客户端 feature flags (握手协商, 设计文档 S9.3)
// =========================================================================

/// 二进制 ChunkLayout (替代 JSON Chunks)
pub const FEATURE_CHUNK_LAYOUT_V2: u32 = 1 << 8;
/// Placement 字段 (0xA0-0xAB)
pub const FEATURE_PLACEMENT_V2: u32 = 1 << 9;
/// Reliability 字段 (0xA1-0xA3)
pub const FEATURE_RELIABILITY_V2: u32 = 1 << 10;
/// 压缩字段 (0xA3)
pub const FEATURE_COMPRESSION_V1: u32 = 1 << 11;

// =========================================================================
// 二进制 tag 常量
// =========================================================================

/// Placement tag (FieldId::Placement 的 u8 值)
mod placement_tag {
    pub const INLINE: u8 = 0;
    pub const FLAT: u8 = 1;
    pub const STRIPE: u8 = 2;
    pub const WIDE_STRIPE: u8 = 3;
}

/// Reliability tag (复合 bytes 首字节)
mod reliability_tag {
    pub const SINGLE_REPLICA: u8 = 0;
    pub const REPLICATED: u8 = 1;
    pub const EC: u8 = 2;
}

/// ReliabilityState tag (FieldId::ReliabilityState 的 u8 值)
mod state_tag {
    pub const PENDING_REPLICATED: u8 = 0;
    pub const REPLICATED: u8 = 1;
    pub const PENDING_EC: u8 = 2;
    pub const EC: u8 = 3;
    pub const DEGRADED: u8 = 4;
}

/// CompressionState tag (FieldId::Compression 的 u8 值)
mod compression_tag {
    pub const NONE: u8 = 0;
    pub const PENDING: u8 = 1;
    pub const COMPRESSED: u8 = 2;
}

/// ChunkEncoding tag (复合 bytes 首字节)
mod encoding_tag {
    pub const INLINE_DATA: u8 = 0;
    pub const PER_CHUNK: u8 = 1;
    pub const STRIPE_DESCRIPTOR: u8 = 2;
    pub const PAGINATED: u8 = 3;
}

/// ChunkRef 固定大小: offset(8) + size(8) + needle_id(8) + volume_id(8) + crc32(4) + mtime(8)
const CHUNK_REF_SIZE: usize = 44;

// =========================================================================
// encode_file_layout / decode_file_layout (顶层入口)
// =========================================================================

/// 编码 FileLayout 到 TLV (根据客户端 features 决定新格式/旧格式).
///
/// - 新格式 (FEATURE_CHUNK_LAYOUT_V2): 二进制 Placement + Reliability + ChunkLayout
/// - 旧格式: JSON Chunks + 首 chunk 兼容字段 (委托 compat.rs)
pub fn encode_file_layout(
    enc: &mut TlvEncoder,
    layout: &FileLayout,
    client_features: u32,
) -> LayoutResult<()> {
    if client_features & FEATURE_CHUNK_LAYOUT_V2 != 0 {
        // 新格式: 二进制
        encode_placement(enc, &layout.placement)?;
        encode_reliability(enc, &layout.reliability)?;
        enc.add_u8(FieldId::ReliabilityState, reliability_state_to_u8(&layout.reliability_state));
        enc.add_u8(FieldId::Compression, compression_state_to_u8(&layout.compression));
        encode_encoding(enc, &layout.encoding)?;
    } else {
        // 旧格式: JSON 兼容 (P8 废弃后删除此分支)
        crate::compat::encode_chunks_json_compat(enc, &layout.encoding)?;
    }
    Ok(())
}

/// 从 TLV 解码 FileLayout.
///
/// 使用 while 循环遍历所有字段, 支持乱序和可选字段.
/// 未知字段自动跳过 (前向兼容).
pub fn decode_file_layout(dec: &mut TlvDecoder) -> LayoutResult<FileLayout> {
    // 收集所有字段
    let mut placement_tag: Option<u8> = None;
    let mut inline_max_size: Option<u32> = None;
    let mut stripe_size: Option<u64> = None;
    let mut stripe_count: Option<u32> = None;
    let mut start_volume_idx: Option<u32> = None;
    let mut volume_ids: Option<Vec<u64>> = None;
    let mut reliability: Option<Reliability> = None;
    let mut reliability_state = ReliabilityState::PendingReplicated;
    let mut compression = CompressionState::None;
    let mut encoding: Option<ChunkEncoding> = None;
    let mut inline_data: Option<Vec<u8>> = None;

    while let Some((field, length)) = dec.next_field() {
        match field {
            // --- Placement ---
            FieldId::Placement => {
                placement_tag = Some(dec.read_u8(length)?);
            }
            FieldId::InlineMaxSize => {
                inline_max_size = Some(dec.read_u32(length)?);
            }
            FieldId::StripeSize => {
                stripe_size = Some(dec.read_u64(length)?);
            }
            FieldId::StripeCount => {
                stripe_count = Some(dec.read_u32(length)?);
            }
            FieldId::StartVolumeIdx => {
                start_volume_idx = Some(dec.read_u32(length)?);
            }
            FieldId::VolumeIds => {
                volume_ids = Some(decode_volume_ids(dec.read_bytes(length)?)?);
            }
            // --- Reliability ---
            FieldId::Reliability => {
                reliability = Some(decode_reliability(dec.read_bytes(length)?)?);
            }
            FieldId::ReliabilityState => {
                reliability_state = u8_to_reliability_state(dec.read_u8(length)?)?;
            }
            FieldId::Compression => {
                compression = u8_to_compression_state(dec.read_u8(length)?)?;
            }
            // --- ChunkEncoding ---
            FieldId::ChunkLayout => {
                encoding = Some(decode_encoding(dec.read_bytes(length)?)?);
            }
            FieldId::InlineData => {
                inline_data = Some(dec.read_bytes(length)?.to_vec());
            }
            // 未知字段: 跳过 (前向兼容)
            _ => dec.skip(length)?,
        }
    }

    // 组装 Placement
    let placement = assemble_placement(
        placement_tag,
        inline_max_size,
        stripe_size,
        stripe_count,
        start_volume_idx,
        volume_ids,
    )?;

    // 组装 ChunkEncoding
    let encoding = encoding.unwrap_or_else(|| {
        if let Some(data) = inline_data {
            ChunkEncoding::InlineData { data }
        } else {
            ChunkEncoding::PerChunk { chunks: Vec::new() }
        }
    });

    Ok(FileLayout {
        placement,
        reliability: reliability.unwrap_or(Reliability::SingleReplica),
        reliability_state,
        compression,
        encoding,
    })
}

// =========================================================================
// encode_placement / assemble_placement
// =========================================================================

/// 编码 Placement 到独立 FieldId 字段
fn encode_placement(enc: &mut TlvEncoder, placement: &Placement) -> LayoutResult<()> {
    match placement {
        Placement::Inline { max_size } => {
            enc.add_u8(FieldId::Placement, placement_tag::INLINE);
            enc.add_u32(FieldId::InlineMaxSize, *max_size);
        }
        Placement::Flat => {
            enc.add_u8(FieldId::Placement, placement_tag::FLAT);
        }
        Placement::Stripe {
            stripe_size,
            stripe_count,
            start_volume_idx,
            volume_ids,
        } => {
            enc.add_u8(FieldId::Placement, placement_tag::STRIPE);
            enc.add_u64(FieldId::StripeSize, *stripe_size);
            enc.add_u32(FieldId::StripeCount, *stripe_count);
            enc.add_u32(FieldId::StartVolumeIdx, *start_volume_idx);
            encode_volume_ids(enc, volume_ids)?;
        }
        Placement::WideStripe {
            stripe_size,
            stripe_count,
            start_volume_idx,
            volume_ids,
        } => {
            enc.add_u8(FieldId::Placement, placement_tag::WIDE_STRIPE);
            enc.add_u64(FieldId::StripeSize, *stripe_size);
            enc.add_u32(FieldId::StripeCount, *stripe_count);
            enc.add_u32(FieldId::StartVolumeIdx, *start_volume_idx);
            encode_volume_ids(enc, volume_ids)?;
        }
    }
    Ok(())
}

/// 从收集的字段组装 Placement
#[allow(clippy::too_many_arguments)]
fn assemble_placement(
    tag: Option<u8>,
    inline_max_size: Option<u32>,
    stripe_size: Option<u64>,
    stripe_count: Option<u32>,
    start_volume_idx: Option<u32>,
    volume_ids: Option<Vec<u64>>,
) -> LayoutResult<Placement> {
    let tag = tag.unwrap_or(placement_tag::FLAT);
    match tag {
        placement_tag::INLINE => {
            let max_size = inline_max_size.ok_or_else(|| {
                LayoutError::TlvDecode("Inline placement missing InlineMaxSize".into())
            })?;
            Ok(Placement::Inline { max_size })
        }
        placement_tag::FLAT => Ok(Placement::Flat),
        placement_tag::STRIPE => {
            let count = stripe_count.ok_or_else(|| {
                LayoutError::TlvDecode("Stripe placement missing StripeCount".into())
            })?;
            Ok(Placement::Stripe {
                stripe_size: stripe_size.unwrap_or(64 * 1024 * 1024),
                stripe_count: count,
                start_volume_idx: start_volume_idx.unwrap_or(0),
                volume_ids: volume_ids.unwrap_or_default(),
            })
        }
        placement_tag::WIDE_STRIPE => {
            let count = stripe_count.ok_or_else(|| {
                LayoutError::TlvDecode("WideStripe placement missing StripeCount".into())
            })?;
            Ok(Placement::WideStripe {
                stripe_size: stripe_size.unwrap_or(4 * 1024 * 1024),
                stripe_count: count,
                start_volume_idx: start_volume_idx.unwrap_or(0),
                volume_ids: volume_ids.unwrap_or_default(),
            })
        }
        other => Err(LayoutError::InvalidPlacement(format!(
            "unknown placement tag {}",
            other
        ))),
    }
}

// =========================================================================
// encode/decode Reliability (复合 bytes)
// =========================================================================

/// 编码 Reliability 为复合 bytes: [u8 tag, ...params]
fn encode_reliability(enc: &mut TlvEncoder, reliability: &Reliability) -> LayoutResult<()> {
    let mut buf = Vec::new();
    match reliability {
        Reliability::SingleReplica => {
            buf.push(reliability_tag::SINGLE_REPLICA);
        }
        Reliability::Replicated { count } => {
            buf.push(reliability_tag::REPLICATED);
            buf.extend_from_slice(&count.to_le_bytes());
        }
        Reliability::EC { data, parity } => {
            buf.push(reliability_tag::EC);
            buf.extend_from_slice(&data.to_le_bytes());
            buf.extend_from_slice(&parity.to_le_bytes());
        }
    }
    enc.add_bytes(FieldId::Reliability, &buf)?;
    Ok(())
}

/// 从复合 bytes 解码 Reliability
fn decode_reliability(bytes: &[u8]) -> LayoutResult<Reliability> {
    if bytes.is_empty() {
        return Err(LayoutError::InvalidReliability(
            "empty reliability field".into(),
        ));
    }
    let tag = bytes[0];
    match tag {
        reliability_tag::SINGLE_REPLICA => Ok(Reliability::SingleReplica),
        reliability_tag::REPLICATED => {
            let count = read_u32_at(bytes, 1)
                .ok_or_else(|| LayoutError::InvalidReliability("Replicated missing count".into()))?;
            Ok(Reliability::Replicated { count })
        }
        reliability_tag::EC => {
            let data = read_u32_at(bytes, 1)
                .ok_or_else(|| LayoutError::InvalidReliability("EC missing data".into()))?;
            let parity = read_u32_at(bytes, 5).ok_or_else(|| {
                LayoutError::InvalidReliability("EC missing parity".into())
            })?;
            Ok(Reliability::EC { data, parity })
        }
        other => Err(LayoutError::InvalidReliability(format!(
            "unknown reliability tag {}",
            other
        ))),
    }
}

// =========================================================================
// encode/decode ChunkEncoding (复合 bytes)
// =========================================================================

/// 编码 ChunkEncoding 为复合 bytes: [u8 tag, ...变体数据]
fn encode_encoding(enc: &mut TlvEncoder, encoding: &ChunkEncoding) -> LayoutResult<()> {
    let mut buf = Vec::new();
    match encoding {
        ChunkEncoding::InlineData { data } => {
            buf.push(encoding_tag::INLINE_DATA);
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(data);
        }
        ChunkEncoding::PerChunk { chunks } => {
            buf.push(encoding_tag::PER_CHUNK);
            buf.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
            for chunk in chunks {
                encode_chunk_ref(&mut buf, chunk);
            }
        }
        ChunkEncoding::StripeDescriptor {
            start_needle_id,
            chunk_size,
            chunk_count,
            volume_ids,
            start_volume_idx,
        } => {
            buf.push(encoding_tag::STRIPE_DESCRIPTOR);
            buf.extend_from_slice(&start_needle_id.to_le_bytes());
            buf.extend_from_slice(&chunk_size.to_le_bytes());
            buf.extend_from_slice(&chunk_count.to_le_bytes());
            buf.extend_from_slice(&start_volume_idx.to_le_bytes());
            buf.extend_from_slice(&(volume_ids.len() as u32).to_le_bytes());
            for vid in volume_ids {
                buf.extend_from_slice(&vid.to_le_bytes());
            }
        }
        ChunkEncoding::Paginated {
            chunks,
            total_count,
            has_more,
            next_offset,
        } => {
            buf.push(encoding_tag::PAGINATED);
            buf.extend_from_slice(&total_count.to_le_bytes());
            buf.push(if *has_more { 1 } else { 0 });
            buf.extend_from_slice(&next_offset.to_le_bytes());
            buf.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
            for chunk in chunks {
                encode_chunk_ref(&mut buf, chunk);
            }
        }
    }
    enc.add_bytes(FieldId::ChunkLayout, &buf)?;
    Ok(())
}

/// 从复合 bytes 解码 ChunkEncoding
fn decode_encoding(bytes: &[u8]) -> LayoutResult<ChunkEncoding> {
    if bytes.is_empty() {
        return Err(LayoutError::InvalidEncoding("empty encoding field".into()));
    }
    let tag = bytes[0];
    let rest = &bytes[1..];
    match tag {
        encoding_tag::INLINE_DATA => {
            let len = read_u32_at(rest, 0)
                .ok_or_else(|| LayoutError::InvalidEncoding("InlineData missing len".into()))?
                as usize;
            let data = rest
                .get(4..4 + len)
                .ok_or_else(|| LayoutError::InvalidEncoding("InlineData data truncated".into()))?
                .to_vec();
            Ok(ChunkEncoding::InlineData { data })
        }
        encoding_tag::PER_CHUNK => {
            let count = read_u32_at(rest, 0)
                .ok_or_else(|| LayoutError::InvalidEncoding("PerChunk missing count".into()))?
                as usize;
            let chunks = decode_chunk_list(rest, 4, count)?;
            Ok(ChunkEncoding::PerChunk { chunks })
        }
        encoding_tag::STRIPE_DESCRIPTOR => {
            let start_needle_id = read_u64_at(rest, 0).ok_or_else(|| {
                LayoutError::InvalidEncoding("StripeDescriptor missing start_needle_id".into())
            })?;
            let chunk_size = read_u32_at(rest, 8).ok_or_else(|| {
                LayoutError::InvalidEncoding("StripeDescriptor missing chunk_size".into())
            })?;
            let chunk_count = read_u32_at(rest, 12).ok_or_else(|| {
                LayoutError::InvalidEncoding("StripeDescriptor missing chunk_count".into())
            })?;
            let start_volume_idx = read_u32_at(rest, 16).ok_or_else(|| {
                LayoutError::InvalidEncoding("StripeDescriptor missing start_volume_idx".into())
            })?;
            let vol_count = read_u32_at(rest, 20)
                .ok_or_else(|| LayoutError::InvalidEncoding("StripeDescriptor missing vol_count".into()))?
                as usize;
            let volume_ids = decode_volume_ids_from(rest, 24, vol_count)?;
            Ok(ChunkEncoding::StripeDescriptor {
                start_needle_id,
                chunk_size,
                chunk_count,
                volume_ids,
                start_volume_idx,
            })
        }
        encoding_tag::PAGINATED => {
            let total_count = read_u32_at(rest, 0)
                .ok_or_else(|| LayoutError::InvalidEncoding("Paginated missing total_count".into()))?;
            let has_more = *rest
                .get(4)
                .ok_or_else(|| LayoutError::InvalidEncoding("Paginated missing has_more".into()))?
                != 0;
            let next_offset = read_u64_at(rest, 5)
                .ok_or_else(|| LayoutError::InvalidEncoding("Paginated missing next_offset".into()))?;
            let chunk_count = read_u32_at(rest, 13)
                .ok_or_else(|| LayoutError::InvalidEncoding("Paginated missing chunk_count".into()))?
                as usize;
            let chunks = decode_chunk_list(rest, 17, chunk_count)?;
            Ok(ChunkEncoding::Paginated {
                chunks,
                total_count,
                has_more,
                next_offset,
            })
        }
        other => Err(LayoutError::InvalidEncoding(format!(
            "unknown encoding tag {}",
            other
        ))),
    }
}

// =========================================================================
// 辅助函数: volume_ids / ChunkRef / 状态转换
// =========================================================================

/// 编码 volume_ids 为 bytes (FieldId::VolumeIds): u64 LE 数组
fn encode_volume_ids(enc: &mut TlvEncoder, volume_ids: &[u64]) -> LayoutResult<()> {
    let mut buf = Vec::with_capacity(volume_ids.len() * 8);
    for vid in volume_ids {
        buf.extend_from_slice(&vid.to_le_bytes());
    }
    enc.add_bytes(FieldId::VolumeIds, &buf)?;
    Ok(())
}

/// 从 bytes 解码 volume_ids (u64 LE 数组)
fn decode_volume_ids(bytes: &[u8]) -> LayoutResult<Vec<u64>> {
    if !bytes.len().is_multiple_of(8) {
        return Err(LayoutError::TlvDecode(format!(
            "volume_ids length {} not multiple of 8",
            bytes.len()
        )));
    }
    Ok(bytes
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
        .collect())
}

/// 从指定偏移解码 count 个 volume_id
fn decode_volume_ids_from(bytes: &[u8], offset: usize, count: usize) -> LayoutResult<Vec<u64>> {
    let needed = count * 8;
    let slice = bytes.get(offset..offset + needed).ok_or_else(|| {
        LayoutError::InvalidEncoding(format!(
            "volume_ids truncated: need {} bytes at offset {}, have {}",
            needed,
            offset,
            bytes.len()
        ))
    })?;
    Ok(slice
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
        .collect())
}

/// 编码单个 ChunkRef 到 buf (44 字节)
fn encode_chunk_ref(buf: &mut Vec<u8>, chunk: &ChunkRef) {
    buf.extend_from_slice(&chunk.offset.to_le_bytes());
    buf.extend_from_slice(&chunk.size.to_le_bytes());
    buf.extend_from_slice(&chunk.needle_id.to_le_bytes());
    buf.extend_from_slice(&chunk.volume_id.to_le_bytes());
    buf.extend_from_slice(&chunk.crc32.to_le_bytes());
    buf.extend_from_slice(&chunk.mtime.to_le_bytes());
}

/// 从指定偏移解码 chunk 列表
fn decode_chunk_list(bytes: &[u8], offset: usize, count: usize) -> LayoutResult<Vec<ChunkRef>> {
    let needed = count * CHUNK_REF_SIZE;
    let slice = bytes.get(offset..offset + needed).ok_or_else(|| {
        LayoutError::InvalidEncoding(format!(
            "chunk list truncated: need {} bytes at offset {}, have {}",
            needed,
            offset,
            bytes.len()
        ))
    })?;

    let mut chunks = Vec::with_capacity(count);
    for chunk_bytes in slice.chunks_exact(CHUNK_REF_SIZE) {
        chunks.push(ChunkRef {
            offset: u64::from_le_bytes(chunk_bytes[0..8].try_into().unwrap()),
            size: u64::from_le_bytes(chunk_bytes[8..16].try_into().unwrap()),
            needle_id: u64::from_le_bytes(chunk_bytes[16..24].try_into().unwrap()),
            volume_id: u64::from_le_bytes(chunk_bytes[24..32].try_into().unwrap()),
            crc32: u32::from_le_bytes(chunk_bytes[32..36].try_into().unwrap()),
            mtime: u64::from_le_bytes(chunk_bytes[36..44].try_into().unwrap()),
        });
    }
    Ok(chunks)
}

// ---- 状态/压缩 tag 转换 ----

fn reliability_state_to_u8(state: &ReliabilityState) -> u8 {
    match state {
        ReliabilityState::PendingReplicated => state_tag::PENDING_REPLICATED,
        ReliabilityState::Replicated => state_tag::REPLICATED,
        ReliabilityState::PendingEC => state_tag::PENDING_EC,
        ReliabilityState::EC => state_tag::EC,
        ReliabilityState::Degraded => state_tag::DEGRADED,
    }
}

fn u8_to_reliability_state(tag: u8) -> LayoutResult<ReliabilityState> {
    match tag {
        state_tag::PENDING_REPLICATED => Ok(ReliabilityState::PendingReplicated),
        state_tag::REPLICATED => Ok(ReliabilityState::Replicated),
        state_tag::PENDING_EC => Ok(ReliabilityState::PendingEC),
        state_tag::EC => Ok(ReliabilityState::EC),
        state_tag::DEGRADED => Ok(ReliabilityState::Degraded),
        other => Err(LayoutError::InvalidReliability(format!(
            "unknown reliability state tag {}",
            other
        ))),
    }
}

fn compression_state_to_u8(state: &CompressionState) -> u8 {
    match state {
        CompressionState::None => compression_tag::NONE,
        CompressionState::Pending => compression_tag::PENDING,
        CompressionState::Compressed => compression_tag::COMPRESSED,
    }
}

fn u8_to_compression_state(tag: u8) -> LayoutResult<CompressionState> {
    match tag {
        compression_tag::NONE => Ok(CompressionState::None),
        compression_tag::PENDING => Ok(CompressionState::Pending),
        compression_tag::COMPRESSED => Ok(CompressionState::Compressed),
        other => Err(LayoutError::InvalidReliability(format!(
            "unknown compression state tag {}",
            other
        ))),
    }
}

// ---- 小端整数读取辅助 ----

fn read_u32_at(bytes: &[u8], offset: usize) -> Option<u32> {
    bytes
        .get(offset..offset + 4)
        .map(|s| u32::from_le_bytes(s.try_into().unwrap()))
}

fn read_u64_at(bytes: &[u8], offset: usize) -> Option<u64> {
    bytes
        .get(offset..offset + 8)
        .map(|s| u64::from_le_bytes(s.try_into().unwrap()))
}

// =========================================================================
// 测试
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use powerfs_net::TlvEncoder;

    /// 编码 FileLayout 后解码, 验证往返一致
    fn round_trip(layout: &FileLayout) -> FileLayout {
        let mut enc = TlvEncoder::new();
        encode_file_layout(&mut enc, layout, FEATURE_CHUNK_LAYOUT_V2).unwrap();
        let bytes = enc.into_bytes();
        let mut dec = TlvDecoder::new(&bytes);
        decode_file_layout(&mut dec).unwrap()
    }

    fn make_chunk(offset: u64, size: u64, needle: u64, vol: u64) -> ChunkRef {
        ChunkRef {
            offset,
            size,
            needle_id: needle,
            volume_id: vol,
            crc32: 0xDEAD_BEEF,
            mtime: 1700000000,
        }
    }

    // ---- Placement 往返测试 ----

    #[test]
    fn round_trip_inline() {
        let layout = FileLayout {
            placement: Placement::Inline { max_size: 4096 },
            reliability: Reliability::Replicated { count: 3 },
            reliability_state: ReliabilityState::Replicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::InlineData {
                data: vec![1, 2, 3, 4],
            },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.placement, layout.placement);
        assert_eq!(decoded.reliability, layout.reliability);
        assert_eq!(decoded.encoding, layout.encoding);
    }

    #[test]
    fn round_trip_flat() {
        let layout = FileLayout {
            placement: Placement::Flat,
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::PendingReplicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::PerChunk {
                chunks: vec![make_chunk(0, 1024, 42, 10)],
            },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.placement, Placement::Flat);
        assert_eq!(decoded.encoding, layout.encoding);
    }

    #[test]
    fn round_trip_stripe() {
        let layout = FileLayout {
            placement: Placement::Stripe {
                stripe_size: 64 * 1024 * 1024,
                stripe_count: 4,
                start_volume_idx: 2,
                volume_ids: vec![10, 20, 30, 40],
            },
            reliability: Reliability::EC { data: 4, parity: 2 },
            reliability_state: ReliabilityState::EC,
            compression: CompressionState::Compressed,
            encoding: ChunkEncoding::PerChunk {
                chunks: vec![
                    make_chunk(0, 1024, 100, 10),
                    make_chunk(1024, 2048, 101, 20),
                ],
            },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.placement, layout.placement);
        assert_eq!(decoded.reliability, layout.reliability);
        assert_eq!(decoded.reliability_state, layout.reliability_state);
        assert_eq!(decoded.compression, layout.compression);
        assert_eq!(decoded.encoding, layout.encoding);
    }

    #[test]
    fn round_trip_wide_stripe() {
        let layout = FileLayout {
            placement: Placement::WideStripe {
                stripe_size: 4 * 1024 * 1024,
                stripe_count: 256,
                start_volume_idx: 0,
                volume_ids: (1..=256).collect(),
            },
            reliability: Reliability::EC { data: 8, parity: 4 },
            reliability_state: ReliabilityState::EC,
            compression: CompressionState::Compressed,
            encoding: ChunkEncoding::StripeDescriptor {
                start_needle_id: 1000,
                chunk_size: 2 * 1024 * 1024,
                chunk_count: 512,
                volume_ids: (1..=256).collect(),
                start_volume_idx: 0,
            },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.placement, layout.placement);
        assert_eq!(decoded.encoding, layout.encoding);
    }

    // ---- ChunkEncoding 各变体往返 ----

    #[test]
    fn round_trip_inline_data() {
        let layout = FileLayout {
            placement: Placement::Inline { max_size: 8192 },
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::PendingReplicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::InlineData {
                data: vec![0xAB; 4096],
            },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.encoding, layout.encoding);
    }

    #[test]
    fn round_trip_per_chunk_many() {
        let chunks: Vec<ChunkRef> = (0..100)
            .map(|i| make_chunk(i * 4096, 4096, 1000 + i, 10 + (i % 4)))
            .collect();
        let layout = FileLayout {
            placement: Placement::Stripe {
                stripe_size: 4096,
                stripe_count: 4,
                start_volume_idx: 0,
                volume_ids: vec![10, 20, 30, 40],
            },
            reliability: Reliability::Replicated { count: 3 },
            reliability_state: ReliabilityState::Replicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::PerChunk { chunks },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.encoding, layout.encoding);
    }

    #[test]
    fn round_trip_stripe_descriptor() {
        let layout = FileLayout {
            placement: Placement::Stripe {
                stripe_size: 64 * 1024 * 1024,
                stripe_count: 4,
                start_volume_idx: 0,
                volume_ids: vec![1, 2, 3, 4],
            },
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::PendingReplicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::StripeDescriptor {
                start_needle_id: 500,
                chunk_size: 2 * 1024 * 1024,
                chunk_count: 512,
                volume_ids: vec![1, 2, 3, 4],
                start_volume_idx: 0,
            },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.encoding, layout.encoding);
    }

    #[test]
    fn round_trip_paginated() {
        let chunks: Vec<ChunkRef> = (0..10)
            .map(|i| make_chunk(i * 1024, 1024, 2000 + i, 10))
            .collect();
        let layout = FileLayout {
            placement: Placement::Flat,
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::PendingReplicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::Paginated {
                chunks,
                total_count: 500,
                has_more: true,
                next_offset: 10240,
            },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.encoding, layout.encoding);
    }

    // ---- Reliability 各变体往返 ----

    #[test]
    fn round_trip_reliability_single() {
        let layout = FileLayout {
            placement: Placement::Flat,
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::PendingReplicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::PerChunk { chunks: vec![] },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.reliability, Reliability::SingleReplica);
    }

    #[test]
    fn round_trip_reliability_replicated() {
        let layout = FileLayout {
            placement: Placement::Flat,
            reliability: Reliability::Replicated { count: 3 },
            reliability_state: ReliabilityState::Replicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::PerChunk { chunks: vec![] },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.reliability, Reliability::Replicated { count: 3 });
    }

    #[test]
    fn round_trip_reliability_ec() {
        let layout = FileLayout {
            placement: Placement::Flat,
            reliability: Reliability::EC { data: 8, parity: 4 },
            reliability_state: ReliabilityState::Degraded,
            compression: CompressionState::Compressed,
            encoding: ChunkEncoding::PerChunk { chunks: vec![] },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.reliability, Reliability::EC { data: 8, parity: 4 });
        assert_eq!(decoded.reliability_state, ReliabilityState::Degraded);
        assert_eq!(decoded.compression, CompressionState::Compressed);
    }

    // ---- 状态/压缩 tag 转换 ----

    #[test]
    fn state_tag_round_trip() {
        let states = [
            ReliabilityState::PendingReplicated,
            ReliabilityState::Replicated,
            ReliabilityState::PendingEC,
            ReliabilityState::EC,
            ReliabilityState::Degraded,
        ];
        for state in &states {
            let tag = reliability_state_to_u8(state);
            let back = u8_to_reliability_state(tag).unwrap();
            assert_eq!(&back, state);
        }
    }

    #[test]
    fn compression_tag_round_trip() {
        let states = [
            CompressionState::None,
            CompressionState::Pending,
            CompressionState::Compressed,
        ];
        for state in &states {
            let tag = compression_state_to_u8(state);
            let back = u8_to_compression_state(tag).unwrap();
            assert_eq!(&back, state);
        }
    }

    // ---- 错误处理 ----

    #[test]
    fn decode_invalid_reliability_tag() {
        let bytes = [0xFF];
        assert!(decode_reliability(&bytes).is_err());
    }

    #[test]
    fn decode_invalid_encoding_tag() {
        let bytes = [0xFF];
        assert!(decode_encoding(&bytes).is_err());
    }

    #[test]
    fn decode_reliability_truncated() {
        // Replicated tag but missing count
        let bytes = [reliability_tag::REPLICATED];
        assert!(decode_reliability(&bytes).is_err());

        // EC tag but missing data/parity
        let bytes = [reliability_tag::EC, 0x01, 0x02];
        assert!(decode_reliability(&bytes).is_err());
    }

    #[test]
    fn decode_volume_ids_odd_length() {
        let bytes = [0x01, 0x02, 0x03]; // 3 bytes, not multiple of 8
        assert!(decode_volume_ids(&bytes).is_err());
    }

    // ---- 前向兼容: 未知字段跳过 ----

    #[test]
    fn decode_skips_unknown_fields() {
        let mut enc = TlvEncoder::new();
        // 写入 Placement Flat
        enc.add_u8(FieldId::Placement, placement_tag::FLAT);
        // 写入未知字段 (用 VolumeId 0x92 作为"未知"字段测试跳过)
        enc.add_u64(FieldId::VolumeId, 999);
        // 写入 Reliability SingleReplica
        let rel_buf = [reliability_tag::SINGLE_REPLICA];
        enc.add_bytes(FieldId::Reliability, &rel_buf).unwrap();

        let bytes = enc.into_bytes();
        let mut dec = TlvDecoder::new(&bytes);
        let layout = decode_file_layout(&mut dec).unwrap();

        assert_eq!(layout.placement, Placement::Flat);
        assert_eq!(layout.reliability, Reliability::SingleReplica);
    }

    // ---- 二进制大小验证 ----

    #[test]
    fn stripe_descriptor_compact_size() {
        // StripeDescriptor 应远小于等价的 JSON PerChunk
        let layout = FileLayout {
            placement: Placement::Stripe {
                stripe_size: 64 * 1024 * 1024,
                stripe_count: 4,
                start_volume_idx: 0,
                volume_ids: vec![1, 2, 3, 4],
            },
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::PendingReplicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::StripeDescriptor {
                start_needle_id: 1000,
                chunk_size: 2 * 1024 * 1024,
                chunk_count: 512,
                volume_ids: vec![1, 2, 3, 4],
                start_volume_idx: 0,
            },
        };

        let mut enc = TlvEncoder::new();
        encode_file_layout(&mut enc, &layout, FEATURE_CHUNK_LAYOUT_V2).unwrap();
        let binary_size = enc.into_bytes().len();

        // StripeDescriptor 编码: 1(tag) + 4(volume_ids) + 32(volume_ids data) +
        // 1(rel tag) + 1(state) + 1(compression) + 1(encoding tag) + 8+4+4+4+4+32 = ~96 bytes
        // JSON 等价: 512 chunks * ~100 bytes/chunk = ~50KB
        assert!(
            binary_size < 200,
            "StripeDescriptor binary size {} should be < 200 bytes",
            binary_size
        );
    }

    // =================================================================
    // 错误分支测试 (直接调用私有函数)
    // =================================================================

    // ---- decode_reliability 错误分支 ----

    #[test]
    fn decode_reliability_empty_bytes() {
        let bytes: [u8; 0] = [];
        assert!(decode_reliability(&bytes).is_err());
    }

    #[test]
    fn decode_reliability_unknown_tag() {
        let bytes = [0xFF];
        assert!(decode_reliability(&bytes).is_err());
    }

    #[test]
    fn decode_reliability_replicated_missing_count() {
        // Replicated tag 但 count 字段不足 4 字节
        let bytes = [reliability_tag::REPLICATED, 0x01, 0x02];
        assert!(decode_reliability(&bytes).is_err());
    }

    #[test]
    fn decode_reliability_ec_missing_parity() {
        // EC tag 但 data+parity 不足 8 字节
        let bytes = [reliability_tag::EC, 0x01, 0x02, 0x03, 0x04, 0x05];
        assert!(decode_reliability(&bytes).is_err());
    }

    // ---- decode_encoding 错误分支 ----

    #[test]
    fn decode_encoding_empty_bytes() {
        let bytes: [u8; 0] = [];
        assert!(decode_encoding(&bytes).is_err());
    }

    #[test]
    fn decode_encoding_unknown_tag() {
        let bytes = [0xFF];
        assert!(decode_encoding(&bytes).is_err());
    }

    #[test]
    fn decode_encoding_inline_data_truncated() {
        // InlineData: tag + len=100, 但无实际数据
        let bytes = [encoding_tag::INLINE_DATA, 100, 0, 0, 0];
        assert!(decode_encoding(&bytes).is_err());
    }

    #[test]
    fn decode_encoding_per_chunk_truncated() {
        // PerChunk: tag + count=10, 但无 chunk 数据
        let bytes = [encoding_tag::PER_CHUNK, 10, 0, 0, 0];
        assert!(decode_encoding(&bytes).is_err());
    }

    #[test]
    fn decode_encoding_stripe_desc_truncated() {
        // StripeDescriptor: tag 但数据不足
        let bytes = [encoding_tag::STRIPE_DESCRIPTOR, 0, 0, 0, 0, 0];
        assert!(decode_encoding(&bytes).is_err());
    }

    #[test]
    fn decode_encoding_paginated_truncated() {
        // Paginated: tag 但数据不足
        let bytes = [encoding_tag::PAGINATED, 0, 0, 0, 0];
        assert!(decode_encoding(&bytes).is_err());
    }

    // ---- assemble_placement 错误分支 ----

    #[test]
    fn assemble_inline_missing_max_size() {
        let result = assemble_placement(Some(placement_tag::INLINE), None, None, None, None, None);
        assert!(result.is_err());
    }

    #[test]
    fn assemble_stripe_missing_count() {
        let result = assemble_placement(
            Some(placement_tag::STRIPE), None, Some(64), None, None, None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn assemble_wide_stripe_missing_count() {
        let result = assemble_placement(
            Some(placement_tag::WIDE_STRIPE), None, None, None, None, None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn assemble_unknown_placement_tag() {
        let result = assemble_placement(Some(0xFF), None, None, None, None, None);
        assert!(result.is_err());
    }

    #[test]
    fn assemble_flat_ignores_extra_fields() {
        // Flat 不需要额外字段, 即使提供了也应成功
        let result = assemble_placement(
            Some(placement_tag::FLAT),
            Some(4096),  // 无意义但不应报错
            None,
            None,
            None,
            None,
        );
        assert_eq!(result.unwrap(), Placement::Flat);
    }

    #[test]
    fn assemble_stripe_uses_default_size() {
        // Stripe 有 count 但无 stripe_size → 使用默认 64MB
        let result = assemble_placement(
            Some(placement_tag::STRIPE), None, None, Some(4), Some(0), Some(vec![1, 2, 3, 4]),
        );
        let p = result.unwrap();
        match p {
            Placement::Stripe { stripe_size, stripe_count, .. } => {
                assert_eq!(stripe_size, 64 * 1024 * 1024);
                assert_eq!(stripe_count, 4);
            }
            _ => panic!("expected Stripe"),
        }
    }

    #[test]
    fn assemble_wide_stripe_uses_default_size() {
        // WideStripe 有 count 但无 stripe_size → 使用默认 4MB
        let result = assemble_placement(
            Some(placement_tag::WIDE_STRIPE), None, None, Some(256), None, None,
        );
        let p = result.unwrap();
        match p {
            Placement::WideStripe { stripe_size, stripe_count, .. } => {
                assert_eq!(stripe_size, 4 * 1024 * 1024);
                assert_eq!(stripe_count, 256);
            }
            _ => panic!("expected WideStripe"),
        }
    }

    #[test]
    fn assemble_no_tag_defaults_flat() {
        // 无 Placement tag → 默认 Flat
        let result = assemble_placement(None, None, None, None, None, None);
        assert_eq!(result.unwrap(), Placement::Flat);
    }

    // =================================================================
    // decode_file_layout 健壮性测试
    // =================================================================

    #[test]
    fn decode_completely_empty_tlv() {
        // 完全空的 TLV 数据 → 全部使用默认值
        let bytes: &[u8] = &[];
        let mut dec = TlvDecoder::new(bytes);
        let layout = decode_file_layout(&mut dec).unwrap();
        assert_eq!(layout.placement, Placement::Flat);
        assert_eq!(layout.reliability, Reliability::SingleReplica);
        assert_eq!(layout.reliability_state, ReliabilityState::PendingReplicated);
        assert_eq!(layout.compression, CompressionState::None);
        match layout.encoding {
            ChunkEncoding::PerChunk { chunks } => assert!(chunks.is_empty()),
            _ => panic!("expected empty PerChunk"),
        }
    }

    #[test]
    fn decode_only_placement_tag() {
        // 只有 Placement Flat, 其他字段使用默认值
        let mut enc = TlvEncoder::new();
        enc.add_u8(FieldId::Placement, placement_tag::FLAT);
        let bytes = enc.into_bytes();
        let mut dec = TlvDecoder::new(&bytes);
        let layout = decode_file_layout(&mut dec).unwrap();
        assert_eq!(layout.placement, Placement::Flat);
        assert_eq!(layout.reliability, Reliability::SingleReplica);
    }

    #[test]
    fn decode_field_order_independent() {
        // 故意打乱字段顺序: Reliability 先, Placement 后, State 最后
        let mut enc = TlvEncoder::new();
        let rel_buf = [reliability_tag::REPLICATED, 3, 0, 0, 0];
        enc.add_bytes(FieldId::Reliability, &rel_buf).unwrap();
        enc.add_u8(FieldId::Placement, placement_tag::STRIPE);
        enc.add_u64(FieldId::StripeSize, 64 * 1024 * 1024);
        enc.add_u32(FieldId::StripeCount, 4);
        enc.add_u8(FieldId::ReliabilityState, state_tag::REPLICATED);

        let bytes = enc.into_bytes();
        let mut dec = TlvDecoder::new(&bytes);
        let layout = decode_file_layout(&mut dec).unwrap();

        assert!(matches!(layout.placement, Placement::Stripe { stripe_count: 4, .. }));
        assert_eq!(layout.reliability, Reliability::Replicated { count: 3 });
        assert_eq!(layout.reliability_state, ReliabilityState::Replicated);
    }

    // =================================================================
    // 边界值测试
    // =================================================================

    #[test]
    fn round_trip_empty_per_chunk_list() {
        let layout = FileLayout {
            placement: Placement::Flat,
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::PendingReplicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::PerChunk { chunks: vec![] },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.encoding, layout.encoding);
    }

    #[test]
    fn round_trip_zero_length_inline_data() {
        let layout = FileLayout {
            placement: Placement::Inline { max_size: 4096 },
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::PendingReplicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::InlineData { data: vec![] },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.encoding, layout.encoding);
    }

    #[test]
    fn round_trip_256_volume_ids() {
        let vol_ids: Vec<u64> = (1..=256).collect();
        let layout = FileLayout {
            placement: Placement::WideStripe {
                stripe_size: 4 * 1024 * 1024,
                stripe_count: 256,
                start_volume_idx: 100,
                volume_ids: vol_ids.clone(),
            },
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::PendingReplicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::PerChunk { chunks: vec![] },
        };
        let decoded = round_trip(&layout);
        assert_eq!(decoded.placement, layout.placement);
    }

    // =================================================================
    // 二进制布局精确验证
    // =================================================================

    #[test]
    fn chunk_ref_exact_byte_layout() {
        // 验证 ChunkRef 44 字节布局, 每个字段的字节值精确匹配
        let chunk = ChunkRef {
            offset: 0x0102_0304_0506_0708,
            size: 0x1112_1314_1516_1718,
            needle_id: 0x2122_2324_2526_2728,
            volume_id: 0x3132_3334_3536_3738,
            crc32: 0x4142_4344,
            mtime: 0x5152_5354_5556_5758,
        };
        let mut buf = Vec::new();
        encode_chunk_ref(&mut buf, &chunk);

        assert_eq!(buf.len(), CHUNK_REF_SIZE);
        assert_eq!(&buf[0..8], &0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(&buf[8..16], &0x1112_1314_1516_1718u64.to_le_bytes());
        assert_eq!(&buf[16..24], &0x2122_2324_2526_2728u64.to_le_bytes());
        assert_eq!(&buf[24..32], &0x3132_3334_3536_3738u64.to_le_bytes());
        assert_eq!(&buf[32..36], &0x4142_4344u32.to_le_bytes());
        assert_eq!(&buf[36..44], &0x5152_5354_5556_5758u64.to_le_bytes());
    }

    #[test]
    fn chunk_ref_round_trip_precise() {
        let chunk = ChunkRef {
            offset: 123456789,
            size: 4096,
            needle_id: 999999,
            volume_id: 42,
            crc32: 0xCAFE_BABE,
            mtime: 1699999999,
        };
        let mut buf = Vec::new();
        encode_chunk_ref(&mut buf, &chunk);
        let decoded = decode_chunk_list(&buf, 0, 1).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], chunk);
    }

    #[test]
    fn volume_ids_exact_byte_layout() {
        let vol_ids = vec![1u64, 2, 3, 4];
        let mut enc = TlvEncoder::new();
        encode_volume_ids(&mut enc, &vol_ids).unwrap();
        let bytes = enc.into_bytes();

        let mut dec = TlvDecoder::new(&bytes);
        while let Some((field, length)) = dec.next_field() {
            if field == FieldId::VolumeIds {
                let data = dec.read_bytes(length).unwrap();
                assert_eq!(data.len(), 32);
                assert_eq!(&data[0..8], &1u64.to_le_bytes());
                assert_eq!(&data[8..16], &2u64.to_le_bytes());
                assert_eq!(&data[16..24], &3u64.to_le_bytes());
                assert_eq!(&data[24..32], &4u64.to_le_bytes());
                return;
            }
        }
        panic!("VolumeIds field not found");
    }

    #[test]
    fn reliability_ec_exact_byte_layout() {
        // EC: [tag u8, data u32 LE, parity u32 LE] = 9 bytes
        let mut enc = TlvEncoder::new();
        encode_reliability(&mut enc, &Reliability::EC { data: 4, parity: 2 }).unwrap();
        let bytes = enc.into_bytes();

        let mut dec = TlvDecoder::new(&bytes);
        while let Some((field, length)) = dec.next_field() {
            if field == FieldId::Reliability {
                let data = dec.read_bytes(length).unwrap();
                assert_eq!(data.len(), 9);
                assert_eq!(data[0], reliability_tag::EC);
                assert_eq!(&data[1..5], &4u32.to_le_bytes());
                assert_eq!(&data[5..9], &2u32.to_le_bytes());
                return;
            }
        }
        panic!("Reliability field not found");
    }

    #[test]
    fn reliability_replicated_exact_byte_layout() {
        // Replicated: [tag u8, count u32 LE] = 5 bytes
        let mut enc = TlvEncoder::new();
        encode_reliability(&mut enc, &Reliability::Replicated { count: 3 }).unwrap();
        let bytes = enc.into_bytes();

        let mut dec = TlvDecoder::new(&bytes);
        while let Some((field, length)) = dec.next_field() {
            if field == FieldId::Reliability {
                let data = dec.read_bytes(length).unwrap();
                assert_eq!(data.len(), 5);
                assert_eq!(data[0], reliability_tag::REPLICATED);
                assert_eq!(&data[1..5], &3u32.to_le_bytes());
                return;
            }
        }
        panic!("Reliability field not found");
    }

    #[test]
    fn reliability_single_exact_byte_layout() {
        // SingleReplica: [tag u8] = 1 byte
        let mut enc = TlvEncoder::new();
        encode_reliability(&mut enc, &Reliability::SingleReplica).unwrap();
        let bytes = enc.into_bytes();

        let mut dec = TlvDecoder::new(&bytes);
        while let Some((field, length)) = dec.next_field() {
            if field == FieldId::Reliability {
                let data = dec.read_bytes(length).unwrap();
                assert_eq!(data.len(), 1);
                assert_eq!(data[0], reliability_tag::SINGLE_REPLICA);
                return;
            }
        }
        panic!("Reliability field not found");
    }

    // =================================================================
    // 全状态枚举覆盖
    // =================================================================

    #[test]
    fn all_reliability_states_round_trip() {
        let states = [
            ReliabilityState::PendingReplicated,
            ReliabilityState::Replicated,
            ReliabilityState::PendingEC,
            ReliabilityState::EC,
            ReliabilityState::Degraded,
        ];
        for state in &states {
            let layout = FileLayout {
                placement: Placement::Flat,
                reliability: Reliability::SingleReplica,
                reliability_state: state.clone(),
                compression: CompressionState::None,
                encoding: ChunkEncoding::PerChunk { chunks: vec![] },
            };
            let decoded = round_trip(&layout);
            assert_eq!(&decoded.reliability_state, state, "state mismatch");
        }
    }

    #[test]
    fn all_compression_states_round_trip() {
        let states = [
            CompressionState::None,
            CompressionState::Pending,
            CompressionState::Compressed,
        ];
        for state in &states {
            let layout = FileLayout {
                placement: Placement::Flat,
                reliability: Reliability::SingleReplica,
                reliability_state: ReliabilityState::PendingReplicated,
                compression: state.clone(),
                encoding: ChunkEncoding::PerChunk { chunks: vec![] },
            };
            let decoded = round_trip(&layout);
            assert_eq!(&decoded.compression, state, "compression mismatch");
        }
    }

    #[test]
    fn unknown_state_tag_errors() {
        assert!(u8_to_reliability_state(0xFF).is_err());
    }

    #[test]
    fn unknown_compression_tag_errors() {
        assert!(u8_to_compression_state(0xFF).is_err());
    }

    // =================================================================
    // 旧客户端 JSON 兼容路径
    // =================================================================

    #[test]
    fn old_client_features_zero_uses_json() {
        // features=0 时走 JSON 兼容路径, 应包含 FieldId::Chunks
        let layout = FileLayout {
            placement: Placement::Flat,
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::PendingReplicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::PerChunk {
                chunks: vec![make_chunk(0, 1024, 42, 10)],
            },
        };
        let mut enc = TlvEncoder::new();
        encode_file_layout(&mut enc, &layout, 0).unwrap();
        let bytes = enc.into_bytes();

        let mut dec = TlvDecoder::new(&bytes);
        let mut found_chunks = false;
        let mut found_volume_id = false;
        while let Some((field, length)) = dec.next_field() {
            if field == FieldId::Chunks {
                found_chunks = true;
            }
            if field == FieldId::VolumeId {
                found_volume_id = true;
            }
            // 消费 value 字节, 使 pos 前进到下一字段头部
            dec.skip(length).unwrap();
        }
        assert!(found_chunks, "old format should contain FieldId::Chunks");
        assert!(found_volume_id, "old format should contain FieldId::VolumeId");
    }

    // =================================================================
    // PerChunk 顺序保持验证
    // =================================================================

    #[test]
    fn per_chunk_preserves_order() {
        let chunks: Vec<ChunkRef> = (0..50)
            .map(|i| ChunkRef {
                offset: i * 4096,
                size: 4096,
                needle_id: 1000 + i as u64,
                volume_id: 10 + (i % 4) as u64,
                crc32: i as u32,
                mtime: 1700000000 + i as u64,
            })
            .collect();
        let layout = FileLayout {
            placement: Placement::Flat,
            reliability: Reliability::SingleReplica,
            reliability_state: ReliabilityState::PendingReplicated,
            compression: CompressionState::None,
            encoding: ChunkEncoding::PerChunk { chunks },
        };
        let decoded = round_trip(&layout);
        match decoded.encoding {
            ChunkEncoding::PerChunk { chunks } => {
                assert_eq!(chunks.len(), 50);
                for (i, chunk) in chunks.iter().enumerate() {
                    assert_eq!(chunk.offset, i as u64 * 4096, "offset mismatch at {}", i);
                    assert_eq!(chunk.needle_id, 1000 + i as u64, "needle_id mismatch at {}", i);
                    assert_eq!(chunk.crc32, i as u32, "crc32 mismatch at {}", i);
                    assert_eq!(chunk.mtime, 1700000000 + i as u64, "mtime mismatch at {}", i);
                }
            }
            _ => panic!("expected PerChunk"),
        }
    }

    // =================================================================
    // 辅助函数测试
    // =================================================================

    #[test]
    fn read_u32_at_out_of_bounds() {
        let bytes = [0x01, 0x02];
        assert!(read_u32_at(&bytes, 0).is_none());
    }

    #[test]
    fn read_u64_at_out_of_bounds() {
        let bytes = [0x01, 0x02, 0x03, 0x04];
        assert!(read_u64_at(&bytes, 0).is_none());
    }

    #[test]
    fn decode_volume_ids_from_truncated() {
        // 需要 3*8=24 字节, 但只有 10
        let bytes = [0u8; 10];
        assert!(decode_volume_ids_from(&bytes, 0, 3).is_err());
    }

    #[test]
    fn decode_chunk_list_zero_count() {
        // count=0 应返回空列表
        let bytes: [u8; 0] = [];
        let result = decode_chunk_list(&bytes, 0, 0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn decode_chunk_list_truncated() {
        // 需要 1*44=44 字节, 但只有 10
        let bytes = [0u8; 10];
        assert!(decode_chunk_list(&bytes, 0, 1).is_err());
    }
}
