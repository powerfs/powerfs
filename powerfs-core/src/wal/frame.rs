//! WAL 记录帧编解码与哈希链校验（方案 §4.2 / §4.3）。
//!
//! 帧布局（26B 定长头 + payload，段内紧密连续追加）：
//!
//! ```text
//! offset  size  field
//! 0       8     prev_crc   u64  # 前一条记录的 crc 字段（段首记录填段头 crc 的 u64 扩展）
//! 8       4     crc        u32  # crc32c(rtype | flags | lsn | payload)
//! 12      4     len        u32  # payload 长度
//! 16      1     rtype      u8
//! 17      1     flags      u8   # bit0: F_SYNC_BARRIER（组提交 fsync 边界标记）
//! 18      8     lsn        u64
//! 26      len   payload
//! ```
//!
//! 哈希链：每条记录的 `prev_crc` 链接前一条记录的 `crc`，除单条内容损坏外
//! 还能检出"整条记录缺失/段内错位"。链按段封闭：段首记录以段头 CRC 的
//! u64 扩展作为种子。
//!
//! 撕裂语义：剩余空间不足以容纳完整帧头、或帧头声明长度超出可用字节，
//! 均视为尾部撕裂（tail tear），由恢复层（replayer）截断处理；扫描器只
//! 负责报告。全零头部视为预分配的空闲 slack（段尾逻辑数据结束）。

use bytes::Bytes;
use crc32c::crc32c;

/// 记录帧头大小（字节）。
pub const FRAME_HEADER_SIZE: usize = 26;

/// flags bit0：组提交 fsync 边界标记。
pub const FLAG_SYNC_BARRIER: u8 = 0x01;

pub const RT_DATA: u8 = 0x01;
pub const RT_DELETE: u8 = 0x02;
pub const RT_ATTR: u8 = 0x03;
pub const RT_SNAP_TAKE: u8 = 0x04;
pub const RT_SNAP_DROP: u8 = 0x05;
pub const RT_CKPT_ANCHOR: u8 = 0x06;
pub const RT_VOLUME_META: u8 = 0x07;
pub const RT_PAD: u8 = 0x7F;

/// 帧级错误。
#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone)]
pub enum FrameError {
    #[error("frame too small: need {need} bytes, got {got}")]
    TooSmall { need: usize, got: usize },
    #[error("crc mismatch at lsn {lsn}: expected {expected:#010x}, got {got:#010x}")]
    CrcMismatch { lsn: u64, expected: u32, got: u32 },
    #[error("chain broken at lsn {lsn}: prev_crc expected {expected:#018x}, got {got:#018x}")]
    ChainMismatch { lsn: u64, expected: u64, got: u64 },
    #[error("unknown record type {rtype:#04x} at offset {offset}")]
    UnknownType { rtype: u8, offset: u64 },
    #[error("invalid payload for record type {rtype:#04x}: {reason}")]
    InvalidPayload { rtype: u8, reason: String },
}

/// 记录类型（§4.3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecordType {
    /// 写入/覆写 needle：重放时建立/更新索引，同 id 取最后一条。
    Data,
    /// 删除 tombstone：重放时移入 tombstone 区（保留期内可 restore）。
    Delete,
    /// WORM 锁定等属性变更。
    Attr,
    /// 创建快照（O(1)）。group_id=0 为单卷快照；跨卷 EC 由协调器对 k+m 卷
    /// 下发同一 snapshot_id + group_id。
    SnapTake,
    /// 删除快照，释放其引用。
    SnapDrop,
    /// checkpoint 锚点：applied_lsn 之前的段在满足引用条件后可回收。
    CkptAnchor,
    /// volume 级元数据（collection、state 变更等）。
    VolumeMeta,
    /// 段尾填充，保证记录永不跨段。
    Pad,
}

impl RecordType {
    pub fn to_u8(self) -> u8 {
        match self {
            RecordType::Data => RT_DATA,
            RecordType::Delete => RT_DELETE,
            RecordType::Attr => RT_ATTR,
            RecordType::SnapTake => RT_SNAP_TAKE,
            RecordType::SnapDrop => RT_SNAP_DROP,
            RecordType::CkptAnchor => RT_CKPT_ANCHOR,
            RecordType::VolumeMeta => RT_VOLUME_META,
            RecordType::Pad => RT_PAD,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            RT_DATA => Some(RecordType::Data),
            RT_DELETE => Some(RecordType::Delete),
            RT_ATTR => Some(RecordType::Attr),
            RT_SNAP_TAKE => Some(RecordType::SnapTake),
            RT_SNAP_DROP => Some(RecordType::SnapDrop),
            RT_CKPT_ANCHOR => Some(RecordType::CkptAnchor),
            RT_VOLUME_META => Some(RecordType::VolumeMeta),
            RT_PAD => Some(RecordType::Pad),
            _ => None,
        }
    }
}

/// 记录帧头（解码后的视图）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub prev_crc: u64,
    pub crc: u32,
    pub len: u32,
    pub rtype: RecordType,
    pub flags: u8,
    pub lsn: u64,
}

impl FrameHeader {
    pub fn total_size(&self) -> usize {
        FRAME_HEADER_SIZE + self.len as usize
    }

    pub fn is_sync_barrier(&self) -> bool {
        self.flags & FLAG_SYNC_BARRIER != 0
    }
}

/// 计算帧 CRC：crc32c(rtype | flags | lsn(u64 LE) | payload)。
pub fn compute_frame_crc(rtype: u8, flags: u8, lsn: u64, payload: &[u8]) -> u32 {
    let mut input = Vec::with_capacity(10 + payload.len());
    input.push(rtype);
    input.push(flags);
    input.extend_from_slice(&lsn.to_le_bytes());
    input.extend_from_slice(payload);
    crc32c(&input)
}

/// 将帧头编码进 `buf`（长度必须恰好为 [`FRAME_HEADER_SIZE`]）。
pub fn encode_header_into(buf: &mut [u8], h: &FrameHeader) {
    assert_eq!(buf.len(), FRAME_HEADER_SIZE, "header buffer size");
    buf[0..8].copy_from_slice(&h.prev_crc.to_le_bytes());
    buf[8..12].copy_from_slice(&h.crc.to_le_bytes());
    buf[12..16].copy_from_slice(&h.len.to_le_bytes());
    buf[16] = h.rtype.to_u8();
    buf[17] = h.flags;
    buf[18..26].copy_from_slice(&h.lsn.to_le_bytes());
}

/// 编码一条完整记录帧（header + payload），返回 `(bytes, crc)`。
/// `crc` 用于链接下一条记录的 `prev_crc`。
pub fn encode_frame(
    prev_crc: u64,
    rtype: RecordType,
    flags: u8,
    lsn: u64,
    payload: &[u8],
) -> (Vec<u8>, u32) {
    let rt = rtype.to_u8();
    let crc = compute_frame_crc(rt, flags, lsn, payload);
    let header = FrameHeader {
        prev_crc,
        crc,
        len: payload.len() as u32,
        rtype,
        flags,
        lsn,
    };
    let mut out = Vec::with_capacity(FRAME_HEADER_SIZE + payload.len());
    let mut hb = [0u8; FRAME_HEADER_SIZE];
    encode_header_into(&mut hb, &header);
    out.extend_from_slice(&hb);
    out.extend_from_slice(payload);
    (out, crc)
}

/// 编码一条 PAD 帧填满 `total_len` 字节（`total_len >= FRAME_HEADER_SIZE`）。
/// `len` 字段即填充字节数（`total_len - 26`），payload 为等长零填充。
/// 返回 `(bytes, crc)`，`crc` 继续参与哈希链。
pub fn encode_pad_frame(prev_crc: u64, lsn: u64, total_len: usize) -> (Vec<u8>, u32) {
    assert!(
        total_len >= FRAME_HEADER_SIZE,
        "pad frame must be at least header size"
    );
    let pad_len = total_len - FRAME_HEADER_SIZE;
    let payload = vec![0u8; pad_len];
    encode_frame(prev_crc, RecordType::Pad, 0, lsn, &payload)
}

/// 解码帧头（不校验 CRC）。
pub fn decode_header(buf: &[u8], offset: u64) -> Result<FrameHeader, FrameError> {
    if buf.len() < FRAME_HEADER_SIZE {
        return Err(FrameError::TooSmall {
            need: FRAME_HEADER_SIZE,
            got: buf.len(),
        });
    }
    let prev_crc = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let crc = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    let len = u32::from_le_bytes(buf[12..16].try_into().unwrap());
    let rt_u8 = buf[16];
    let flags = buf[17];
    let lsn = u64::from_le_bytes(buf[18..26].try_into().unwrap());
    let rtype = RecordType::from_u8(rt_u8).ok_or(FrameError::UnknownType {
        rtype: rt_u8,
        offset,
    })?;
    Ok(FrameHeader {
        prev_crc,
        crc,
        len,
        rtype,
        flags,
        lsn,
    })
}

/// 扫描一条记录的结果。
#[derive(Debug)]
pub enum ScanOne<'a> {
    /// 完整解析出一条记录；`payload` 指向输入缓冲内的切片。
    Frame {
        header: FrameHeader,
        payload: &'a [u8],
        /// 本帧总字节数（header + payload）。
        consumed: usize,
    },
    /// 剩余字节不足一个完整帧头（含全零 slack），逻辑数据结束。
    EndOfData,
    /// 帧头存在但内容不完整（撕裂写），`need`/`got` 为帧声明总长与可用字节。
    Torn { need: usize, got: usize },
    /// CRC 或哈希链校验失败，或 payload 非法。
    Corrupt(FrameError),
}

/// 从 `buf` 起始处扫描一条记录。
///
/// `expected_prev_crc` 为哈希链期望值（段首为段头 CRC 的 u64 扩展）。
pub fn scan_one<'a>(buf: &'a [u8], expected_prev_crc: u64) -> ScanOne<'a> {
    if buf.len() < FRAME_HEADER_SIZE {
        // 尾部不足一个帧头：若全为零视为预分配 slack，否则视为撕裂。
        if buf.iter().all(|&b| b == 0) {
            return ScanOne::EndOfData;
        }
        return ScanOne::Torn {
            need: FRAME_HEADER_SIZE,
            got: buf.len(),
        };
    }

    let header = match decode_header(buf, 0) {
        Ok(h) => h,
        Err(e @ FrameError::UnknownType { .. }) => {
            // 全零头部（rtype=0）属于预分配 slack；其余未知类型为损坏。
            if buf[0..FRAME_HEADER_SIZE].iter().all(|&b| b == 0) {
                return ScanOne::EndOfData;
            }
            return ScanOne::Corrupt(e);
        }
        Err(e) => return ScanOne::Corrupt(e),
    };

    let total = header.total_size();
    if total > buf.len() {
        return ScanOne::Torn {
            need: total,
            got: buf.len(),
        };
    }

    let payload = &buf[FRAME_HEADER_SIZE..total];
    let got_crc = compute_frame_crc(header.rtype.to_u8(), header.flags, header.lsn, payload);
    if got_crc != header.crc {
        return ScanOne::Corrupt(FrameError::CrcMismatch {
            lsn: header.lsn,
            expected: header.crc,
            got: got_crc,
        });
    }
    if header.prev_crc != expected_prev_crc {
        return ScanOne::Corrupt(FrameError::ChainMismatch {
            lsn: header.lsn,
            expected: expected_prev_crc,
            got: header.prev_crc,
        });
    }

    ScanOne::Frame {
        header,
        payload,
        consumed: total,
    }
}

// ---------------------------------------------------------------------------
// payload 编解码（§4.3）
// ---------------------------------------------------------------------------

fn read_u64(buf: &[u8], pos: usize) -> Result<u64, FrameError> {
    buf.get(pos..pos + 8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .ok_or_else(|| too_short(8, buf.len() - pos))
}

fn read_u32(buf: &[u8], pos: usize) -> Result<u32, FrameError> {
    buf.get(pos..pos + 4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
        .ok_or_else(|| too_short(4, buf.len() - pos))
}

fn read_u16(buf: &[u8], pos: usize) -> Result<u16, FrameError> {
    buf.get(pos..pos + 2)
        .map(|b| u16::from_le_bytes(b.try_into().unwrap()))
        .ok_or_else(|| too_short(2, buf.len() - pos))
}

fn read_i64(buf: &[u8], pos: usize) -> Result<i64, FrameError> {
    Ok(read_u64(buf, pos)? as i64)
}

fn too_short(need: usize, got: usize) -> FrameError {
    FrameError::InvalidPayload {
        rtype: 0,
        reason: format!("payload too short: need {need} more bytes, got {got}"),
    }
}

fn payload_err(rtype: u8, reason: impl Into<String>) -> FrameError {
    FrameError::InvalidPayload {
        rtype,
        reason: reason.into(),
    }
}

/// DATA（0x01）payload：`needle_id u64 | data_len u32 | data`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataPayload {
    pub needle_id: u64,
    pub data: Bytes,
}

impl DataPayload {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.needle_id.to_le_bytes());
        out.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.data);
    }

    pub fn decode(buf: &[u8]) -> Result<Self, FrameError> {
        if buf.len() < 12 {
            return Err(payload_err(
                RT_DATA,
                format!("need >= 12 bytes, got {}", buf.len()),
            ));
        }
        let needle_id = read_u64(buf, 0)?;
        let data_len = read_u32(buf, 8)? as usize;
        if buf.len() != 12 + data_len {
            return Err(payload_err(
                RT_DATA,
                format!("data_len {data_len} != remaining {}", buf.len() - 12),
            ));
        }
        Ok(DataPayload {
            needle_id,
            data: Bytes::copy_from_slice(&buf[12..]),
        })
    }
}

/// DELETE（0x02）payload：`needle_id u64 | deleted_at i64 | retention_until i64`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletePayload {
    pub needle_id: u64,
    pub deleted_at: i64,
    pub retention_until: i64,
}

impl DeletePayload {
    pub const SIZE: usize = 24;

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.needle_id.to_le_bytes());
        out.extend_from_slice(&self.deleted_at.to_le_bytes());
        out.extend_from_slice(&self.retention_until.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Result<Self, FrameError> {
        if buf.len() != Self::SIZE {
            return Err(payload_err(
                RT_DELETE,
                format!("need exactly {} bytes, got {}", Self::SIZE, buf.len()),
            ));
        }
        Ok(DeletePayload {
            needle_id: read_u64(buf, 0)?,
            deleted_at: read_i64(buf, 8)?,
            retention_until: read_i64(buf, 16)?,
        })
    }
}

/// ATTR（0x03）payload：`needle_id u64 | attr_mask u32 | values`。
/// `values` 为按 mask 解释的属性字节（语义在 P2+ 定义），编码层不透明。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttrPayload {
    pub needle_id: u64,
    pub attr_mask: u32,
    pub values: Bytes,
}

impl AttrPayload {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.needle_id.to_le_bytes());
        out.extend_from_slice(&self.attr_mask.to_le_bytes());
        out.extend_from_slice(&self.values);
    }

    pub fn decode(buf: &[u8]) -> Result<Self, FrameError> {
        if buf.len() < 12 {
            return Err(payload_err(
                RT_ATTR,
                format!("need >= 12 bytes, got {}", buf.len()),
            ));
        }
        Ok(AttrPayload {
            needle_id: read_u64(buf, 0)?,
            attr_mask: read_u32(buf, 8)?,
            values: Bytes::copy_from_slice(&buf[12..]),
        })
    }
}

/// SNAP_TAKE（0x04）payload：
/// `snapshot_id u64 | group_id u64 | name_len u16 | name | ts i64`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapTakePayload {
    pub snapshot_id: u64,
    pub group_id: u64,
    pub name: String,
    pub ts: i64,
}

impl SnapTakePayload {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.snapshot_id.to_le_bytes());
        out.extend_from_slice(&self.group_id.to_le_bytes());
        out.extend_from_slice(&(self.name.len() as u16).to_le_bytes());
        out.extend_from_slice(self.name.as_bytes());
        out.extend_from_slice(&self.ts.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Result<Self, FrameError> {
        if buf.len() < 26 {
            return Err(payload_err(
                RT_SNAP_TAKE,
                format!("need >= 26 bytes, got {}", buf.len()),
            ));
        }
        let snapshot_id = read_u64(buf, 0)?;
        let group_id = read_u64(buf, 8)?;
        let name_len = read_u16(buf, 16)? as usize;
        if buf.len() < 18 + name_len + 8 {
            return Err(payload_err(
                RT_SNAP_TAKE,
                format!("name_len {name_len} exceeds payload {}", buf.len()),
            ));
        }
        let name = String::from_utf8(buf[18..18 + name_len].to_vec())
            .map_err(|_| payload_err(RT_SNAP_TAKE, "name is not valid utf-8"))?;
        let ts = read_i64(buf, 18 + name_len)?;
        Ok(SnapTakePayload {
            snapshot_id,
            group_id,
            name,
            ts,
        })
    }
}

/// SNAP_DROP（0x05）payload：`snapshot_id u64`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapDropPayload {
    pub snapshot_id: u64,
}

impl SnapDropPayload {
    pub const SIZE: usize = 8;

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.snapshot_id.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Result<Self, FrameError> {
        if buf.len() != Self::SIZE {
            return Err(payload_err(
                RT_SNAP_DROP,
                format!("need exactly {} bytes, got {}", Self::SIZE, buf.len()),
            ));
        }
        Ok(SnapDropPayload {
            snapshot_id: read_u64(buf, 0)?,
        })
    }
}

/// CKPT_ANCHOR（0x06）payload：`ckpt_seq u64 | applied_lsn u64`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CkptAnchorPayload {
    pub ckpt_seq: u64,
    pub applied_lsn: u64,
}

impl CkptAnchorPayload {
    pub const SIZE: usize = 16;

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.ckpt_seq.to_le_bytes());
        out.extend_from_slice(&self.applied_lsn.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Result<Self, FrameError> {
        if buf.len() != Self::SIZE {
            return Err(payload_err(
                RT_CKPT_ANCHOR,
                format!("need exactly {} bytes, got {}", Self::SIZE, buf.len()),
            ));
        }
        Ok(CkptAnchorPayload {
            ckpt_seq: read_u64(buf, 0)?,
            applied_lsn: read_u64(buf, 8)?,
        })
    }
}

/// VOLUME_META（0x07）payload：`field_mask u32 | values`。
/// `values` 为按 mask 解释的字节（collection、state 等），编码层不透明。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeMetaPayload {
    pub field_mask: u32,
    pub values: Bytes,
}

impl VolumeMetaPayload {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.field_mask.to_le_bytes());
        out.extend_from_slice(&self.values);
    }

    pub fn decode(buf: &[u8]) -> Result<Self, FrameError> {
        if buf.len() < 4 {
            return Err(payload_err(
                RT_VOLUME_META,
                format!("need >= 4 bytes, got {}", buf.len()),
            ));
        }
        Ok(VolumeMetaPayload {
            field_mask: read_u32(buf, 0)?,
            values: Bytes::copy_from_slice(&buf[4..]),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 段首种子：段头 CRC 的 u64 扩展。测试用固定值模拟。
    const SEED: u64 = 0x1234_5678_9abc_def0;

    /// 将一组 payload 编码为紧密连续的帧序列（模拟段内容），返回 (buffer, 各帧头)。
    fn build_segment(frames: &[(RecordType, u8, u64, Vec<u8>)]) -> (Vec<u8>, Vec<FrameHeader>) {
        let mut buf = Vec::new();
        let mut prev = SEED;
        let mut headers = Vec::new();
        for (rtype, flags, lsn, payload) in frames {
            let (bytes, crc) = encode_frame(prev, *rtype, *flags, *lsn, payload);
            headers.push(FrameHeader {
                prev_crc: prev,
                crc,
                len: payload.len() as u32,
                rtype: *rtype,
                flags: *flags,
                lsn: *lsn,
            });
            buf.extend_from_slice(&bytes);
            prev = crc as u64;
        }
        (buf, headers)
    }

    /// 顺序扫描整个缓冲，断言全部帧完整且链校验通过。
    fn scan_all(buf: &[u8]) -> Vec<(FrameHeader, Vec<u8>)> {
        let mut out = Vec::new();
        let mut prev = SEED;
        let mut pos = 0usize;
        loop {
            match scan_one(&buf[pos..], prev) {
                ScanOne::Frame {
                    header,
                    payload,
                    consumed,
                } => {
                    pos += consumed;
                    prev = header.crc as u64;
                    out.push((header, payload.to_vec()));
                }
                ScanOne::EndOfData => break,
                other => panic!("unexpected scan outcome at pos {pos}: {other:?}"),
            }
        }
        assert!(
            buf[pos..].iter().all(|&b| b == 0),
            "scanner must consume all non-slack bytes (pos {pos}, len {})",
            buf.len()
        );
        out
    }

    #[test]
    fn frame_roundtrip_all_rtypes() {
        let payloads: Vec<(RecordType, Vec<u8>)> = vec![
            (RecordType::Data, {
                let p = DataPayload {
                    needle_id: 0xdead_beef,
                    data: Bytes::from_static(b"hello wal"),
                };
                let mut v = Vec::new();
                p.encode(&mut v);
                v
            }),
            (RecordType::Delete, {
                let p = DeletePayload {
                    needle_id: 7,
                    deleted_at: 1000,
                    retention_until: 2000,
                };
                let mut v = Vec::new();
                p.encode(&mut v);
                v
            }),
            (RecordType::Attr, {
                let p = AttrPayload {
                    needle_id: 9,
                    attr_mask: 0b101,
                    values: Bytes::from_static(b"worm"),
                };
                let mut v = Vec::new();
                p.encode(&mut v);
                v
            }),
            (RecordType::SnapTake, {
                let p = SnapTakePayload {
                    snapshot_id: 42,
                    group_id: 100,
                    name: "snap-1".to_string(),
                    ts: 123456,
                };
                let mut v = Vec::new();
                p.encode(&mut v);
                v
            }),
            (RecordType::SnapDrop, {
                let p = SnapDropPayload { snapshot_id: 42 };
                let mut v = Vec::new();
                p.encode(&mut v);
                v
            }),
            (RecordType::CkptAnchor, {
                let p = CkptAnchorPayload {
                    ckpt_seq: 3,
                    applied_lsn: 999,
                };
                let mut v = Vec::new();
                p.encode(&mut v);
                v
            }),
            (RecordType::VolumeMeta, {
                let p = VolumeMetaPayload {
                    field_mask: 0x7,
                    values: Bytes::from_static(b"collection-a"),
                };
                let mut v = Vec::new();
                p.encode(&mut v);
                v
            }),
        ];

        let frames: Vec<(RecordType, u8, u64, Vec<u8>)> = payloads
            .iter()
            .enumerate()
            .map(|(i, (rt, p))| {
                (
                    *rt,
                    if i == 1 { FLAG_SYNC_BARRIER } else { 0 },
                    i as u64,
                    p.clone(),
                )
            })
            .collect();
        let (buf, headers) = build_segment(&frames);
        let scanned = scan_all(&buf);

        assert_eq!(scanned.len(), frames.len());
        for (i, ((header, payload), want)) in scanned.iter().zip(&frames).enumerate() {
            assert_eq!(header.rtype, want.0, "frame {i} rtype");
            assert_eq!(header.lsn, want.2, "frame {i} lsn");
            assert_eq!(header.flags, want.1, "frame {i} flags");
            assert_eq!(&payload[..], &want.3[..], "frame {i} payload");
            assert_eq!(header.total_size(), FRAME_HEADER_SIZE + want.3.len());
            assert_eq!(header.is_sync_barrier(), i == 1, "frame {i} barrier flag");
        }
        // 帧头解码独立于扫描路径也应一致。
        for (i, h) in headers.iter().enumerate() {
            let off = scanned[..i]
                .iter()
                .map(|(_, p)| FRAME_HEADER_SIZE + p.len())
                .sum::<usize>();
            assert_eq!(&decode_header(&buf[off..], off as u64).unwrap(), h);
        }
    }

    #[test]
    fn payload_roundtrip() {
        let data = DataPayload {
            needle_id: u64::MAX,
            data: Bytes::from(vec![0xab; 4096]),
        };
        let mut v = Vec::new();
        data.encode(&mut v);
        assert_eq!(v.len(), 12 + 4096);
        assert_eq!(DataPayload::decode(&v).unwrap(), data);

        let del = DeletePayload {
            needle_id: 1,
            deleted_at: -5,
            retention_until: i64::MAX,
        };
        let mut v = Vec::new();
        del.encode(&mut v);
        assert_eq!(v.len(), DeletePayload::SIZE);
        assert_eq!(DeletePayload::decode(&v).unwrap(), del);

        let attr = AttrPayload {
            needle_id: 2,
            attr_mask: u32::MAX,
            values: Bytes::new(),
        };
        let mut v = Vec::new();
        attr.encode(&mut v);
        assert_eq!(v.len(), 12);
        assert_eq!(AttrPayload::decode(&v).unwrap(), attr);

        let snap = SnapTakePayload {
            snapshot_id: 3,
            group_id: 0,
            name: String::from_utf8(vec![b'x'; 300]).unwrap(),
            ts: -1,
        };
        let mut v = Vec::new();
        snap.encode(&mut v);
        assert_eq!(SnapTakePayload::decode(&v).unwrap(), snap);

        let drop = SnapDropPayload {
            snapshot_id: u64::MAX,
        };
        let mut v = Vec::new();
        drop.encode(&mut v);
        assert_eq!(v.len(), 8);
        assert_eq!(SnapDropPayload::decode(&v).unwrap(), drop);

        let anchor = CkptAnchorPayload {
            ckpt_seq: 9,
            applied_lsn: u64::MAX,
        };
        let mut v = Vec::new();
        anchor.encode(&mut v);
        assert_eq!(v.len(), 16);
        assert_eq!(CkptAnchorPayload::decode(&v).unwrap(), anchor);

        let meta = VolumeMetaPayload {
            field_mask: 1,
            values: Bytes::from_static(b"abc"),
        };
        let mut v = Vec::new();
        meta.encode(&mut v);
        assert_eq!(VolumeMetaPayload::decode(&v).unwrap(), meta);
    }

    #[test]
    fn payload_rejects_malformed() {
        // DATA：声明长度与实际不符。
        let mut v = Vec::new();
        DataPayload {
            needle_id: 1,
            data: Bytes::from_static(b"abc"),
        }
        .encode(&mut v);
        v.truncate(v.len() - 1);
        assert!(matches!(
            DataPayload::decode(&v),
            Err(FrameError::InvalidPayload { .. })
        ));
        assert!(matches!(
            DataPayload::decode(&[0u8; 5]),
            Err(FrameError::InvalidPayload { .. })
        ));

        // DELETE：定长 24B。
        assert!(matches!(
            DeletePayload::decode(&[0u8; 23]),
            Err(FrameError::InvalidPayload { .. })
        ));

        // SNAP_TAKE：name_len 越界 / 非 UTF-8。
        let mut v = Vec::new();
        SnapTakePayload {
            snapshot_id: 1,
            group_id: 1,
            name: "nm".into(),
            ts: 1,
        }
        .encode(&mut v);
        v[16] = 0xff;
        v[17] = 0xff;
        assert!(matches!(
            SnapTakePayload::decode(&v),
            Err(FrameError::InvalidPayload { .. })
        ));

        // 未知 rtype 解不出 RecordType。
        assert!(RecordType::from_u8(0x00).is_none());
        assert!(RecordType::from_u8(0x7e).is_none());
    }

    #[test]
    fn hash_chain_detects_missing_frame() {
        // 三条记录，扫描时跳过第二条（模拟记录缺失/段内错位）。
        let frames = vec![
            (RecordType::Data, 0u8, 0u64, b"aaa".to_vec()),
            (RecordType::Data, 0, 1, b"bbb".to_vec()),
            (RecordType::Data, 0, 2, b"ccc".to_vec()),
        ];
        let (buf, headers) = build_segment(&frames);

        // 每条 Data 帧总长 26 + 3 = 29B；第三条起始偏移 = 29 * 2。
        let third_off = (FRAME_HEADER_SIZE + 3) * 2;
        // 从第三条开始扫，期望 prev_crc 与第二条 crc 不匹配 → 链断裂。
        match scan_one(&buf[third_off..], headers[0].crc as u64) {
            ScanOne::Corrupt(FrameError::ChainMismatch { expected, got, .. }) => {
                assert_eq!(expected, headers[0].crc as u64);
                assert_eq!(got, headers[1].crc as u64);
            }
            other => panic!("expected chain mismatch, got {other:?}"),
        }
    }

    #[test]
    fn crc_corruption_detected() {
        let (mut buf, _) = build_segment(&[(RecordType::Data, 0, 1, vec![7u8; 64])]);
        // 翻转 payload 中间一个字节。
        let mid = FRAME_HEADER_SIZE + 32;
        buf[mid] ^= 0x01;
        match scan_one(&buf, SEED) {
            ScanOne::Corrupt(FrameError::CrcMismatch { .. }) => {}
            other => panic!("expected crc mismatch, got {other:?}"),
        }
    }

    #[test]
    fn torn_tail_detection() {
        // 完整帧截断到一半：头部声明长度超出可用字节。
        let (buf, _) = build_segment(&[(RecordType::Data, 0, 1, vec![9u8; 100])]);
        for cut in [FRAME_HEADER_SIZE + 50, FRAME_HEADER_SIZE + 1] {
            match scan_one(&buf[..cut], SEED) {
                ScanOne::Torn { need, got } => {
                    assert_eq!(need, FRAME_HEADER_SIZE + 100);
                    assert_eq!(got, cut);
                }
                other => panic!("cut {cut}: expected torn, got {other:?}"),
            }
        }
        // 头部只写了一半且非零 → 撕裂。
        match scan_one(&buf[..10], SEED) {
            ScanOne::Torn { need, got } => {
                assert_eq!(need, FRAME_HEADER_SIZE);
                assert_eq!(got, 10);
            }
            other => panic!("expected torn partial header, got {other:?}"),
        }
    }

    #[test]
    fn zero_slack_is_end_of_data() {
        let (mut buf, _) = build_segment(&[(RecordType::Data, 0, 1, b"data".to_vec())]);
        // 模拟 fallocate 预分配的零填充尾部（不足一个帧头 / 多个帧头两种）。
        buf.extend_from_slice(&[0u8; 10]);
        assert!(matches!(scan_all(&buf).len(), 1));
        buf.extend_from_slice(&[0u8; FRAME_HEADER_SIZE + 5]);
        assert!(matches!(scan_all(&buf).len(), 1));

        // 纯零缓冲 → EndOfData。
        match scan_one(&[0u8; 64], SEED) {
            ScanOne::EndOfData => {}
            other => panic!("expected end of data, got {other:?}"),
        }
        match scan_one(&[0u8; 5], SEED) {
            ScanOne::EndOfData => {}
            other => panic!("expected end of data, got {other:?}"),
        }
        // 空缓冲 → EndOfData。
        assert!(matches!(scan_one(&[], SEED), ScanOne::EndOfData));
    }

    #[test]
    fn pad_fill_and_skip() {
        // 用 PAD 恰好填满"剩余空间"，扫描器应跳过 PAD 且链连续。
        let mut buf = Vec::new();
        let mut prev = SEED;
        let (bytes, crc) = encode_frame(prev, RecordType::Data, 0, 0, b"payload");
        buf.extend_from_slice(&bytes);
        prev = crc as u64;

        let remaining = 137usize; // 模拟段尾剩余空间
        let (pad, pad_crc) = encode_pad_frame(prev, 1, remaining);
        assert_eq!(pad.len(), remaining);
        buf.extend_from_slice(&pad);
        prev = pad_crc as u64;

        // PAD 之后仍有空间则继续写记录（新段场景下链重新播种；此处验证链字段）。
        let (bytes, _) = encode_frame(prev, RecordType::Data, 0, 1, b"next");
        buf.extend_from_slice(&bytes);

        let scanned = scan_all(&buf);
        assert_eq!(scanned.len(), 3);
        assert_eq!(scanned[1].0.rtype, RecordType::Pad);
        assert_eq!(scanned[1].1.len(), remaining - FRAME_HEADER_SIZE);
        assert!(scanned[1].1.iter().all(|&b| b == 0));

        // PAD 最小形态：len=0。
        let (pad0, _) = encode_pad_frame(SEED, 0, FRAME_HEADER_SIZE);
        assert_eq!(pad0.len(), FRAME_HEADER_SIZE);
        assert!(matches!(scan_one(&pad0, SEED), ScanOne::Frame { .. }));
        assert!(std::panic::catch_unwind(|| encode_pad_frame(SEED, 0, 5)).is_err());
    }

    #[test]
    fn lsn_and_flags_survive_roundtrip() {
        let (buf, _) =
            build_segment(&[(RecordType::Data, FLAG_SYNC_BARRIER, u64::MAX, b"x".to_vec())]);
        let scanned = scan_all(&buf);
        assert_eq!(scanned[0].0.lsn, u64::MAX);
        assert!(scanned[0].0.is_sync_barrier());
    }
}
