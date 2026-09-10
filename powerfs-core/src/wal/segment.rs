//! WAL 段文件读写（方案 §4.1）。
//!
//! 段文件 = 段头（64B 字段区 + 4B CRC，共 68B）+ 记录帧序列。段由
//! [`SegWriter`] 独占追加（配合卷级 flock 单写者），写满或显式 seal 后
//! 不可变，只能通过 [`SegReader`] 扫描。
//!
//! 空间语义：
//! - `append` 时若剩余空间容不下完整记录帧，先写 PAD 占满剩余空间，再
//!   返回 [`SegmentError::SegmentFull`]，由上层封段换段。剩余空间不足一
//!   个帧头（< 26B）时无法写 PAD，留作死缝（读取侧视为段尾）。
//! - 可选 `fallocate` 预分配：未写区域为 unwritten extent，读出为零，
//!   与帧扫描的零 slack 语义一致。

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use crate::wal::frame::{
    encode_frame, encode_pad_frame, scan_one, FrameError, RecordType, ScanOne,
    FLAG_SYNC_BARRIER, FRAME_HEADER_SIZE,
};

/// 段文件 magic："PFWLSEG\0"。
pub const SEG_MAGIC: [u8; 8] = *b"PFWLSEG\0";
/// 段头字段区大小（不含 CRC）。
pub const SEG_HEADER_FIELD_SIZE: usize = 64;
/// 段头总大小（字段区 + CRC）。
pub const SEG_HEADER_SIZE: usize = SEG_HEADER_FIELD_SIZE + 4;
/// 段头 CRC 覆盖 [0..64)。
const SEG_HEADER_CRC_INPUT: usize = SEG_HEADER_FIELD_SIZE;
/// 段头 flags：bit0 = sealed（封段后置位并回写）。
pub const SEG_FLAG_SEALED: u32 = 0x01;
/// 扫描预读缓冲大小。
const READ_CHUNK: usize = 256 * 1024;

/// 段级错误。
#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error("io error on {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("bad segment magic in {path}: got {got:#018x}")]
    BadMagic { path: PathBuf, got: u64 },
    #[error("segment header crc mismatch in {path}: expected {expected:#010x}, got {got:#010x}")]
    HeaderCrcMismatch {
        path: PathBuf,
        expected: u32,
        got: u32,
    },
    #[error("unsupported segment format version {got} in {path}")]
    FormatVersion { path: PathBuf, got: u16 },
    #[error("segment {path} full: need {needed} bytes, remaining {remaining}")]
    SegmentFull {
        path: PathBuf,
        needed: usize,
        remaining: u64,
    },
    #[error("segment {path} too small for header: {got} bytes")]
    HeaderTooSmall { path: PathBuf, got: usize },
    #[error("scan error: {0}")]
    Scan(PathBuf, #[source] ScanError),
    #[error("segment file name not parseable: {0}")]
    BadFileName(String),
    #[error("segment already sealed: {0}")]
    Sealed(PathBuf),
}

impl SegmentError {
    pub(crate) fn io(path: &Path, e: std::io::Error) -> Self {
        SegmentError::Io {
            path: path.to_path_buf(),
            source: e,
        }
    }
}

/// 段扫描错误（帧层问题的段级表达）。
#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    #[error("torn tail at offset {at}: need {need} bytes, got {got}")]
    TornTail { at: u64, need: usize, got: usize },
    #[error("corrupt frame: {0}")]
    Corrupt(#[from] FrameError),
    #[error("io error during scan: {0}")]
    Io(#[from] std::io::Error),
}

/// 段头（§4.1）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub seg_id: u64,
    pub volume_id: u64,
    /// 本段首条记录 LSN。
    pub base_lsn: u64,
    pub created_ts: i64,
    pub flags: u32,
}

impl SegmentHeader {
    pub fn is_sealed(&self) -> bool {
        self.flags & SEG_FLAG_SEALED != 0
    }

    /// 编码 68B 段头（64B 字段区 + 4B CRC）。
    pub fn encode(&self) -> [u8; SEG_HEADER_SIZE] {
        let mut buf = [0u8; SEG_HEADER_SIZE];
        buf[0..8].copy_from_slice(&SEG_MAGIC);
        buf[8..10].copy_from_slice(&1u16.to_le_bytes()); // format_ver
        buf[10..12].copy_from_slice(&(SEG_HEADER_FIELD_SIZE as u16).to_le_bytes());
        buf[12..20].copy_from_slice(&self.seg_id.to_le_bytes());
        buf[20..28].copy_from_slice(&self.volume_id.to_le_bytes());
        buf[28..36].copy_from_slice(&self.base_lsn.to_le_bytes());
        buf[36..44].copy_from_slice(&self.created_ts.to_le_bytes());
        buf[44..48].copy_from_slice(&self.flags.to_le_bytes());
        // [48..64) reserved 零填充
        let crc = crc32c::crc32c(&buf[0..SEG_HEADER_CRC_INPUT]);
        buf[SEG_HEADER_FIELD_SIZE..SEG_HEADER_SIZE].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// 从 68B 字节解码并校验 magic / version / CRC。
    pub fn decode(path: &Path, buf: &[u8]) -> Result<Self, SegmentError> {
        if buf.len() < SEG_HEADER_SIZE {
            return Err(SegmentError::HeaderTooSmall {
                path: path.to_path_buf(),
                got: buf.len(),
            });
        }
        if buf[0..8] != SEG_MAGIC {
            let mut got = [0u8; 8];
            got.copy_from_slice(&buf[0..8]);
            return Err(SegmentError::BadMagic {
                path: path.to_path_buf(),
                got: u64::from_le_bytes(got),
            });
        }
        let ver = u16::from_le_bytes(buf[8..10].try_into().unwrap());
        if ver != 1 {
            return Err(SegmentError::FormatVersion {
                path: path.to_path_buf(),
                got: ver,
            });
        }
        let stored_crc = u32::from_le_bytes(
            buf[SEG_HEADER_FIELD_SIZE..SEG_HEADER_SIZE].try_into().unwrap(),
        );
        let got_crc = crc32c::crc32c(&buf[0..SEG_HEADER_CRC_INPUT]);
        if stored_crc != got_crc {
            return Err(SegmentError::HeaderCrcMismatch {
                path: path.to_path_buf(),
                expected: stored_crc,
                got: got_crc,
            });
        }
        Ok(SegmentHeader {
            seg_id: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
            volume_id: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
            base_lsn: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
            created_ts: i64::from_le_bytes(buf[36..44].try_into().unwrap()),
            flags: u32::from_le_bytes(buf[44..48].try_into().unwrap()),
        })
    }

    /// 哈希链种子：对段头不可变字段区（编码后 [0..44)，即 magic/format_ver/
    /// header_len/seg_id/volume_id/base_lsn/created_ts）的 crc32c 的 u64 扩展。
    ///
    /// 封段仅置位 flags 并回写段头，不改写不可变字段区，因此种子在封段
    /// 前后保持一致（若以整段头 CRC 作种子，置位 flags 会改变种子，链校验
    /// 将失败）。
    pub fn chain_seed(&self) -> u64 {
        const IMMUTABLE_PREFIX: usize = 44;
        let enc = self.encode();
        crc32c::crc32c(&enc[0..IMMUTABLE_PREFIX]) as u64
    }
}

/// 段文件名：`seg_<seg_id:016x>.log`。
pub fn seg_file_name(seg_id: u64) -> String {
    format!("seg_{seg_id:016x}.log")
}

/// 从文件名解析 seg_id（严格匹配段文件名格式）。
pub fn parse_seg_id(file_name: &str) -> Option<u64> {
    let rest = file_name.strip_prefix("seg_")?;
    let rest = rest.strip_suffix(".log")?;
    if rest.len() != 16 || !rest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    u64::from_str_radix(rest, 16).ok()
}

/// 段内一帧的元信息。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameMeta {
    pub lsn: u64,
    pub rtype: RecordType,
    pub flags: u8,
    /// 帧起始在段文件内的偏移（含段头前缀）。
    pub offset: u64,
    pub payload_len: u32,
    pub crc: u32,
}

impl FrameMeta {
    pub fn total_size(&self) -> usize {
        FRAME_HEADER_SIZE + self.payload_len as usize
    }

    pub fn is_sync_barrier(&self) -> bool {
        self.flags & FLAG_SYNC_BARRIER != 0
    }
}

/// 扫描出的一帧（元信息 + payload 拷贝）。
#[derive(Debug, Clone)]
pub struct ScannedFrame {
    pub meta: FrameMeta,
    pub payload: Vec<u8>,
}

/// 重扫摘要：恢复/续写定位所需的最小信息。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScanSummary {
    pub frame_count: u64,
    /// 最后一个完整帧的结束偏移（== 下一次 append 的写入位置）。
    pub valid_end: u64,
    pub last_lsn: Option<u64>,
    pub last_crc: Option<u32>,
    /// 尾部撕裂信息（valid_end 之后存在不完整帧）。
    pub torn_tail: bool,
}

// ---------------------------------------------------------------------------
// SegReader：流式顺序扫描
// ---------------------------------------------------------------------------

/// 段只读扫描器。流式读取，内存占用与最大单帧 payload 同阶。
pub struct SegReader {
    file: File,
    pub header: SegmentHeader,
    path: PathBuf,
    file_size: u64,
    /// buf[0] 在文件内的偏移。
    buf_start: u64,
    buf: Vec<u8>,
    prev_crc: u64,
    eof: bool,
    frame_count: u64,
    valid_end: u64,
    last_meta: Option<FrameMeta>,
}

impl SegReader {
    /// 打开段文件并校验段头。
    pub fn open(path: &Path) -> Result<Self, SegmentError> {
        let mut file = File::open(path).map_err(|e| SegmentError::io(path, e))?;
        let mut hdr = [0u8; SEG_HEADER_SIZE];
        file.read_exact(&mut hdr)
            .map_err(|e| SegmentError::io(path, e))?;
        let header = SegmentHeader::decode(path, &hdr)?;
        let file_size = file.metadata().map_err(|e| SegmentError::io(path, e))?.len();
        Ok(SegReader {
            file,
            header,
            path: path.to_path_buf(),
            file_size,
            buf_start: SEG_HEADER_SIZE as u64,
            buf: Vec::new(),
            prev_crc: header.chain_seed(),
            eof: false,
            frame_count: 0,
            valid_end: SEG_HEADER_SIZE as u64,
            last_meta: None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 已扫描验证的数据末尾（下一个完整帧的起始偏移；撕裂时为最后
    /// 完整帧的结束位置）。
    pub fn valid_end(&self) -> u64 {
        self.valid_end
    }

    /// 扫描下一条完整记录。段尾撕裂返回 [`ScanError::TornTail`]；
    /// CRC/链校验失败返回 [`ScanError::Corrupt`]；正常结束返回 `Ok(None)`。
    pub fn next_frame(&mut self) -> Result<Option<ScannedFrame>, ScanError> {
        loop {
            if !self.buf.is_empty() {
                match scan_one(&self.buf, self.prev_crc) {
                    ScanOne::Frame {
                        header,
                        payload,
                        consumed,
                    } => {
                        // 先复制 payload 再更新游标（借用在此结束）。
                        let payload = payload.to_vec();
                        let buf_start = self.buf_start;
                        self.buf_start += consumed as u64;
                        self.buf.drain(..consumed);
                        let meta = FrameMeta {
                            lsn: header.lsn,
                            rtype: header.rtype,
                            flags: header.flags,
                            offset: buf_start,
                            payload_len: header.len,
                            crc: header.crc,
                        };
                        self.prev_crc = header.crc as u64;
                        self.frame_count += 1;
                        self.valid_end = self.buf_start;
                        self.last_meta = Some(meta);
                        return Ok(Some(ScannedFrame { meta, payload }));
                    }
                    ScanOne::EndOfData => {
                        // 零 slack：文件已尽则干净结束，否则继续读（防御性）。
                        if self.eof {
                            return Ok(None);
                        }
                    }
                    ScanOne::Torn { need, got } => {
                        if self.eof {
                            return Err(ScanError::TornTail {
                                at: self.buf_start,
                                need,
                                got,
                            });
                        }
                    }
                    ScanOne::Corrupt(e) => {
                        // 完整帧字节已在手，校验失败是确定性损坏。
                        return Err(ScanError::Corrupt(e));
                    }
                }
            } else if self.eof {
                return Ok(None);
            }

            self.refill()?;
        }
    }

    fn refill(&mut self) -> Result<(), ScanError> {
        debug_assert!(!self.eof);
        let file_pos = self.buf_start + self.buf.len() as u64;
        self.file
            .seek(SeekFrom::Start(file_pos))
            .map_err(ScanError::Io)?;
        let start_len = self.buf.len();
        self.buf.resize(start_len + READ_CHUNK, 0);
        let n = read_up_to(&mut self.file, &mut self.buf[start_len..])?;
        if n == 0 {
            self.eof = true;
        }
        self.buf.truncate(start_len + n);
        Ok(())
    }

    /// 扫描至段尾（或首个错误），返回摘要。
    pub fn scan_summary(&mut self) -> Result<ScanSummary, ScanError> {
        loop {
            match self.next_frame() {
                Ok(Some(_)) => continue,
                Ok(None) => {
                    return Ok(ScanSummary {
                        frame_count: self.frame_count,
                        valid_end: self.valid_end,
                        last_lsn: self.last_meta.map(|m| m.lsn),
                        last_crc: self.last_meta.map(|m| m.crc),
                        torn_tail: false,
                    })
                }
                Err(ScanError::TornTail { .. }) => {
                    return Ok(ScanSummary {
                        frame_count: self.frame_count,
                        valid_end: self.valid_end,
                        last_lsn: self.last_meta.map(|m| m.lsn),
                        last_crc: self.last_meta.map(|m| m.crc),
                        torn_tail: true,
                    })
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }
}

/// 读取至多填满 `buf`（read 不保证填满）。
fn read_up_to(file: &mut File, buf: &mut [u8]) -> std::io::Result<usize> {
    loop {
        match file.read(buf) {
            Ok(0) => return Ok(0),
            Ok(n) => return Ok(n),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}

// ---------------------------------------------------------------------------
// SegWriter：独占追加
// ---------------------------------------------------------------------------

/// 段追加器。同一时刻一个段只允许一个 SegWriter（卷级 flock 保证）。
pub struct SegWriter {
    file: File,
    header: SegmentHeader,
    path: PathBuf,
    seg_size: u64,
    write_pos: u64,
    /// 上一帧 crc（哈希链），初始为段头 CRC 种子。
    prev_crc: u64,
    frame_count: u64,
    last_meta: Option<FrameMeta>,
}

impl SegWriter {
    /// 创建新段文件并写入段头。
    ///
    /// `preallocate` 为 true 时按 `seg_size` 预分配（unwritten extent，
    /// 读出为零，与扫描语义一致）。
    pub fn create(
        path: &Path,
        seg_id: u64,
        volume_id: u64,
        base_lsn: u64,
        seg_size: u64,
        preallocate: bool,
    ) -> Result<Self, SegmentError> {
        if seg_size < (SEG_HEADER_SIZE + FRAME_HEADER_SIZE) as u64 {
            return Err(SegmentError::SegmentFull {
                path: path.to_path_buf(),
                needed: SEG_HEADER_SIZE + FRAME_HEADER_SIZE,
                remaining: seg_size,
            });
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| SegmentError::io(path, e))?;

        if preallocate {
            fallocate_len(&file, seg_size).map_err(|e| SegmentError::io(path, e))?;
        }

        let header = SegmentHeader {
            seg_id,
            volume_id,
            base_lsn,
            created_ts: chrono::Utc::now().timestamp(),
            flags: 0,
        };
        let enc = header.encode();
        file.seek(SeekFrom::Start(0))
            .map_err(|e| SegmentError::io(path, e))?;
        file.write_all(&enc).map_err(|e| SegmentError::io(path, e))?;
        file.sync_data().map_err(|e| SegmentError::io(path, e))?;

        Ok(SegWriter {
            file,
            header,
            path: path.to_path_buf(),
            seg_size,
            write_pos: SEG_HEADER_SIZE as u64,
            prev_crc: header.chain_seed(),
            frame_count: 0,
            last_meta: None,
        })
    }

    /// 重开既有段文件：扫描定位合法尾部；若存在撕裂尾则截断（tolerate_tail），
    /// 恢复哈希链后可继续追加。
    pub fn reopen(path: &Path, seg_size: u64) -> Result<(Self, ScanSummary), SegmentError> {
        let mut reader = SegReader::open(path)?;
        let summary = reader.scan_summary().map_err(|e| {
            SegmentError::Scan(path.to_path_buf(), e)
        })?;
        drop(reader);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| SegmentError::io(path, e))?;
        let meta = file.metadata().map_err(|e| SegmentError::io(path, e))?;
        let header = read_header(&mut file, path)?;

        if summary.torn_tail || meta.len() > summary.valid_end {
            // 截断撕裂尾/多余字节到合法末尾。
            file.set_len(summary.valid_end)
                .map_err(|e| SegmentError::io(path, e))?;
            file.sync_data().map_err(|e| SegmentError::io(path, e))?;
        }

        let writer = SegWriter {
            file,
            header,
            path: path.to_path_buf(),
            seg_size,
            write_pos: summary.valid_end,
            prev_crc: summary.last_crc.map(|c| c as u64).unwrap_or_else(|| header.chain_seed()),
            frame_count: summary.frame_count,
            last_meta: None,
        };
        Ok((writer, summary))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn seg_id(&self) -> u64 {
        self.header.seg_id
    }

    pub fn base_lsn(&self) -> u64 {
        self.header.base_lsn
    }

    pub fn is_sealed(&self) -> bool {
        self.header.is_sealed()
    }

    pub fn write_pos(&self) -> u64 {
        self.write_pos
    }

    pub fn remaining(&self) -> u64 {
        self.seg_size - self.write_pos
    }

    pub fn frame_count(&self) -> u64 {
        self.frame_count
    }

    pub fn last_crc(&self) -> u64 {
        self.prev_crc
    }

    /// 测试/工具用途：底层文件句柄（写入撕裂模拟等）。
    #[cfg(test)]
    pub(crate) fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    /// 追加一条记录。
    ///
    /// 剩余空间容不下完整帧时：先写 PAD 占满剩余空间（若剩余 ≥ 26B），
    /// 再返回 [`SegmentError::SegmentFull`]，由上层封段换段后重试。
    /// `flags` 任意；同步屏障标记使用 [`crate::wal::frame::FLAG_SYNC_BARRIER`]。
    pub fn append(
        &mut self,
        rtype: RecordType,
        flags: u8,
        lsn: u64,
        payload: &[u8],
    ) -> Result<FrameMeta, SegmentError> {
        if self.header.is_sealed() {
            return Err(SegmentError::Sealed(self.path.clone()));
        }
        let total = FRAME_HEADER_SIZE + payload.len();
        let remaining = self.remaining() as usize;
        if total > remaining {
            // 尽力 PAD 填满（剩余 >= 26B 时），失败不影响 SegmentFull 语义。
            if remaining >= FRAME_HEADER_SIZE {
                self.pad_to_end(lsn)?;
            }
            return Err(SegmentError::SegmentFull {
                path: self.path.clone(),
                needed: total,
                remaining: remaining as u64,
            });
        }

        let (bytes, crc) = encode_frame(self.prev_crc, rtype, flags, lsn, payload);
        debug_assert_eq!(bytes.len(), total);
        self.file
            .seek(SeekFrom::Start(self.write_pos))
            .map_err(|e| SegmentError::io(&self.path, e))?;
        self.file
            .write_all(&bytes)
            .map_err(|e| SegmentError::io(&self.path, e))?;

        let meta = FrameMeta {
            lsn,
            rtype,
            flags,
            offset: self.write_pos,
            payload_len: payload.len() as u32,
            crc,
        };
        self.write_pos += total as u64;
        self.prev_crc = crc as u64;
        self.frame_count += 1;
        self.last_meta = Some(meta);
        Ok(meta)
    }

    /// 写 PAD 帧恰好占满剩余空间。剩余 < 26B 时为 no-op（死缝）。
    pub fn pad_to_end(&mut self, lsn: u64) -> Result<Option<FrameMeta>, SegmentError> {
        let remaining = self.remaining() as usize;
        if remaining < FRAME_HEADER_SIZE {
            return Ok(None);
        }
        let (bytes, crc) = encode_pad_frame(self.prev_crc, lsn, remaining);
        self.file
            .seek(SeekFrom::Start(self.write_pos))
            .map_err(|e| SegmentError::io(&self.path, e))?;
        self.file
            .write_all(&bytes)
            .map_err(|e| SegmentError::io(&self.path, e))?;
        let meta = FrameMeta {
            lsn,
            rtype: RecordType::Pad,
            flags: 0,
            offset: self.write_pos,
            payload_len: (remaining - FRAME_HEADER_SIZE) as u32,
            crc,
        };
        self.write_pos = self.seg_size;
        self.prev_crc = crc as u64;
        self.frame_count += 1;
        self.last_meta = Some(meta);
        Ok(Some(meta))
    }

    /// 段尾是否还能容纳一条 `payload_len` 大小的记录。
    pub fn can_fit(&self, payload_len: usize) -> bool {
        FRAME_HEADER_SIZE + payload_len <= self.remaining() as usize
    }

    /// 刷写并封段：置位段头 sealed 标志并回写（barrier write：先 fsync 数据，
    /// 再改段头，再 fsync 段头），封段后段不可变。
    pub fn seal(&mut self) -> Result<(), SegmentError> {
        if self.header.is_sealed() {
            return Ok(());
        }
        self.file
            .seek(SeekFrom::Start(self.write_pos))
            .map_err(|e| SegmentError::io(&self.path, e))?;
        self.file
            .flush()
            .map_err(|e| SegmentError::io(&self.path, e))?;
        self.file
            .sync_data()
            .map_err(|e| SegmentError::io(&self.path, e))?;

        self.header.flags |= SEG_FLAG_SEALED;
        let enc = self.header.encode();
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|e| SegmentError::io(&self.path, e))?;
        self.file
            .write_all(&enc)
            .map_err(|e| SegmentError::io(&self.path, e))?;
        self.file
            .sync_data()
            .map_err(|e| SegmentError::io(&self.path, e))?;
        Ok(())
    }

    /// fsync 段数据（组提交/屏障由上层决定调用时机）。
    pub fn sync(&self) -> std::io::Result<()> {
        self.file.sync_data()
    }
}

fn read_header(file: &mut File, path: &Path) -> Result<SegmentHeader, SegmentError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|e| SegmentError::io(path, e))?;
    let mut hdr = [0u8; SEG_HEADER_SIZE];
    file.read_exact(&mut hdr)
        .map_err(|e| SegmentError::io(path, e))?;
    SegmentHeader::decode(path, &hdr)
}

fn fallocate_len(file: &File, len: u64) -> std::io::Result<()> {
    nix::fcntl::fallocate(
        file.as_raw_fd(),
        nix::fcntl::FallocateFlags::empty(),
        0,
        len as nix::libc::off_t,
    )
    .map_err(std::convert::Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::manifest::{SegmentState, SegManifest};

    /// 追加一条 Data 记录的辅助。
    fn append_data(w: &mut SegWriter, lsn: u64, payload: &[u8]) -> FrameMeta {
        w.append(RecordType::Data, 0, lsn, payload)
            .expect("append should succeed")
    }

    #[test]
    fn create_and_read_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(seg_file_name(1));
        let w = SegWriter::create(&path, 1, 42, 100, 4096, false).unwrap();
        assert_eq!(w.seg_id(), 1);
        assert_eq!(w.base_lsn(), 100);
        assert_eq!(w.write_pos(), SEG_HEADER_SIZE as u64);
        assert_eq!(w.remaining(), 4096 - SEG_HEADER_SIZE as u64);
        assert!(!w.is_sealed());
        drop(w);

        let r = SegReader::open(&path).unwrap();
        assert_eq!(r.header.seg_id, 1);
        assert_eq!(r.header.volume_id, 42);
        assert_eq!(r.header.base_lsn, 100);
        assert_eq!(r.header.created_ts, r.header.created_ts);
        assert!(!r.header.is_sealed());
        assert_eq!(r.file_size(), SEG_HEADER_SIZE as u64);
    }

    #[test]
    fn header_corruption_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(seg_file_name(1));
        drop(SegWriter::create(&path, 1, 1, 0, 4096, false).unwrap());

        // 破坏 seg_id 字段 → 段头 CRC 不匹配。
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[12] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        match SegReader::open(&path) {
            Err(SegmentError::HeaderCrcMismatch { .. }) => {}
            Err(e) => panic!("expected header crc mismatch, got {e}"),
            Ok(_) => panic!("expected header crc mismatch, got Ok"),
        }

        // 破坏 magic。
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] = b'X';
        std::fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            SegReader::open(&path),
            Err(SegmentError::BadMagic { .. })
        ));
    }

    #[test]
    fn append_scan_roundtrip_and_chain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(seg_file_name(1));
        let mut w = SegWriter::create(&path, 1, 1, 0, 4096, false).unwrap();

        let mut metas = Vec::new();
        for i in 0..20u64 {
            let payload = vec![i as u8; 16 + i as usize];
            let flags = if i == 19 { FLAG_SYNC_BARRIER } else { 0 };
            let m = w
                .append(RecordType::Data, flags, i, &payload)
                .unwrap_or_else(|e| panic!("append {i} failed: {e}"));
            assert_eq!(m.payload_len as usize, 16 + i as usize);
            metas.push(m);
        }
        // 帧偏移连续。
        let mut expect_off = SEG_HEADER_SIZE as u64;
        for (m, i) in metas.iter().zip(0..) {
            assert_eq!(m.offset, expect_off, "frame {i} offset");
            expect_off += m.total_size() as u64;
            assert_eq!(m.lsn, i);
        }
        assert!(metas.last().unwrap().is_sync_barrier());
        drop(w);

        // 重扫一致：帧数、lsn、payload、链校验全部还原。
        let mut r = SegReader::open(&path).unwrap();
        let summary = r.scan_summary().unwrap();
        assert_eq!(summary.frame_count, 20);
        assert_eq!(summary.valid_end, expect_off);
        assert_eq!(summary.last_lsn, Some(19));
        assert!(!summary.torn_tail);

        let mut r = SegReader::open(&path).unwrap();
        for i in 0..20u64 {
            let f = r.next_frame().unwrap().expect("frame");
            assert_eq!(f.meta.lsn, i);
            assert_eq!(f.payload, vec![i as u8; 16 + i as usize]);
        }
        assert!(r.next_frame().unwrap().is_none(), "clean end");
    }

    #[test]
    fn tail_boundary_pad_and_roll() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(seg_file_name(1));
        // 段可用空间：4096 - 68 = 4028。
        let mut w = SegWriter::create(&path, 1, 1, 0, 4096, false).unwrap();

        // 持续追加直到段满，记录导致 SegmentFull 的那一笔。
        let mut lsn = 0u64;
        loop {
            match w.append(RecordType::Data, 0, lsn, &[lsn as u8; 100]) {
                Ok(_) => lsn += 1,
                Err(SegmentError::SegmentFull { .. }) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        // PAD 已写满剩余空间 → write_pos == seg_size。
        assert_eq!(w.remaining(), 0);
        assert_eq!(w.write_pos(), 4096);
        w.seal().unwrap();
        drop(w);

        // 重扫：PAD 帧存在且跳过后干净结束。
        let mut r = SegReader::open(&path).unwrap();
        let mut count = 0;
        let mut pad_seen = false;
        while let Some(f) = r.next_frame().unwrap() {
            if f.meta.rtype == RecordType::Pad {
                pad_seen = true;
                assert_eq!(
                    f.meta.offset as usize + f.meta.total_size(),
                    4096,
                    "PAD must fill exactly to segment end"
                );
            } else {
                assert_eq!(f.payload, vec![f.meta.lsn as u8; 100]);
            }
            count += 1;
        }
        assert!(pad_seen, "tail pad frame must exist");
        assert_eq!(count, lsn + 1); // 数据帧 + 1 个 PAD

        // 清单视角：sealed 段后需换新段。
        let mut mf = SegManifest::load(dir.path(), 4096).unwrap();
        assert_eq!(mf.segments().len(), 1);
        assert_eq!(mf.segments()[0].state, SegmentState::Sealed);

        // 换段：新 seg_id 追加，链重新播种。
        let next_id = mf.next_seg_id();
        assert_eq!(next_id, 2);
        let path2 = dir.path().join(seg_file_name(next_id));
        let mut w2 = SegWriter::create(&path2, next_id, 1, lsn + 1, 4096, false).unwrap();
        append_data(&mut w2, lsn + 1, b"fresh");
        drop(w2);

        mf.register(next_id, lsn + 1, 1);
        assert_eq!(mf.segments().len(), 2);
        assert_eq!(mf.segments()[0].state, SegmentState::Sealed);
        assert_eq!(mf.segments()[1].state, SegmentState::Active);
        assert_eq!(mf.active().unwrap().seg_id, 2);
    }

    #[test]
    fn reopen_continues_chain_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(seg_file_name(1));
        {
            let mut w = SegWriter::create(&path, 1, 1, 0, 8192, false).unwrap();
            for i in 0..5u64 {
                append_data(&mut w, i, format!("first-{i}").as_bytes());
            }
        }

        // 跨重启重开：链恢复，继续追加。
        let (mut w, summary) = SegWriter::reopen(&path, 8192).unwrap();
        assert_eq!(summary.frame_count, 5);
        assert!(!summary.torn_tail);
        for i in 5..8u64 {
            append_data(&mut w, i, format!("second-{i}").as_bytes());
        }
        drop(w);

        let mut r = SegReader::open(&path).unwrap();
        for i in 0..8u64 {
            let f = r.next_frame().unwrap().expect("frame");
            assert_eq!(
                f.payload,
                if i < 5 {
                    format!("first-{i}").into_bytes()
                } else {
                    format!("second-{i}").into_bytes()
                }
            );
        }
        assert!(r.next_frame().unwrap().is_none());
    }

    #[test]
    fn torn_tail_located_and_truncated_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(seg_file_name(1));
        let valid_end;
        {
            let mut w = SegWriter::create(&path, 1, 1, 0, 8192, false).unwrap();
            append_data(&mut w, 0, b"good-0");
            append_data(&mut w, 1, b"good-1");
            let (bytes, _) = encode_frame(w.last_crc(), RecordType::Data, 0, 2, b"torn");
            valid_end = w.write_pos();
            // 模拟撕裂写：只落盘半个帧。
            w.file.seek(SeekFrom::Start(valid_end)).unwrap();
            w.file.write_all(&bytes[..bytes.len() / 2]).unwrap();
        }

        // 扫描定位撕裂。
        let mut r = SegReader::open(&path).unwrap();
        let summary = r.scan_summary().unwrap();
        assert_eq!(summary.frame_count, 2);
        assert_eq!(summary.valid_end, valid_end);
        assert!(summary.torn_tail);

        // reopen 截断撕裂尾并可继续追加。
        let (mut w, s) = SegWriter::reopen(&path, 8192).unwrap();
        assert!(s.torn_tail);
        assert_eq!(w.write_pos(), summary.valid_end);
        append_data(&mut w, 2, b"good-2");
        drop(w);

        let mut r = SegReader::open(&path).unwrap();
        for i in 0..3u64 {
            let f = r.next_frame().unwrap().expect("frame");
            assert_eq!(f.meta.lsn, i);
        }
        assert!(r.next_frame().unwrap().is_none());
        assert!(!r.scan_summary().unwrap().torn_tail);
    }

    #[test]
    fn preallocate_reads_zero_and_scan_clean() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(seg_file_name(1));
        let mut w = SegWriter::create(&path, 1, 1, 0, 1 << 20, true).unwrap();
        append_data(&mut w, 0, b"only");
        w.seal().unwrap();
        drop(w);

        let mut r = SegReader::open(&path).unwrap();
        assert_eq!(r.file_size(), 1 << 20, "fallocate preallocated");
        let f = r.next_frame().unwrap().unwrap();
        assert_eq!(f.payload, b"only");
        assert!(r.next_frame().unwrap().is_none(), "unwritten extents read as zeros");
    }

    #[test]
    fn manifest_normalizes_successor_sealed() {
        let dir = tempfile::tempdir().unwrap();
        // seg 1 sealed、seg 2 未 sealed 但有后继 seg 3 → 归一化为 Sealed。
        let p1 = dir.path().join(seg_file_name(1));
        let mut w1 = SegWriter::create(&p1, 1, 1, 0, 4096, false).unwrap();
        append_data(&mut w1, 0, b"a");
        w1.seal().unwrap();
        drop(w1);

        let p2 = dir.path().join(seg_file_name(2));
        let mut w2 = SegWriter::create(&p2, 2, 1, 1, 4096, false).unwrap();
        append_data(&mut w2, 1, b"b");
        drop(w2); // 未 seal（模拟崩溃时序）

        let p3 = dir.path().join(seg_file_name(3));
        let mut w3 = SegWriter::create(&p3, 3, 1, 2, 4096, false).unwrap();
        append_data(&mut w3, 2, b"c");
        drop(w3);

        let mf = SegManifest::load(dir.path(), 4096).unwrap();
        assert_eq!(mf.segments().len(), 3);
        assert_eq!(mf.segments()[0].state, SegmentState::Sealed);
        assert_eq!(mf.segments()[1].state, SegmentState::Sealed, "有后继段必须归一化为 Sealed");
        assert_eq!(mf.segments()[2].state, SegmentState::Active);
        assert_eq!(mf.active().unwrap().seg_id, 3);
        assert_eq!(mf.next_seg_id(), 4);

        // seg_id 与文件名不一致 → 报错。
        let p4 = dir.path().join(seg_file_name(9));
        drop(SegWriter::create(&p4, 7, 1, 0, 4096, false).unwrap());
        assert!(matches!(
            SegManifest::load(dir.path(), 4096),
            Err(SegmentError::BadFileName(_))
        ));

        // 非段文件被忽略。
        let dir2 = tempfile::tempdir().unwrap();
        std::fs::write(dir2.path().join("superblock.a"), b"junk").unwrap();
        std::fs::write(dir2.path().join("ckpt_0000000000000001.bin"), b"junk").unwrap();
        let mf2 = SegManifest::load(dir2.path(), 4096).unwrap();
        assert!(mf2.is_empty());
    }

    #[test]
    fn append_after_seal_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(seg_file_name(1));
        let mut w = SegWriter::create(&path, 1, 1, 0, 4096, false).unwrap();
        append_data(&mut w, 0, b"x");
        w.seal().unwrap();
        assert!(matches!(
            w.append(RecordType::Data, 0, 1, b"y"),
            Err(SegmentError::Sealed(_))
        ));
        assert!(w.seal().is_ok(), "seal idempotent");
    }

    #[test]
    fn file_name_codec() {
        assert_eq!(seg_file_name(0x1234), "seg_0000000000001234.log");
        assert_eq!(parse_seg_id("seg_0000000000001234.log"), Some(0x1234));
        assert_eq!(parse_seg_id("seg_0000000000001234.tmp"), None);
        assert_eq!(parse_seg_id("seg_1234.log"), None);
        assert_eq!(parse_seg_id("superblock.a"), None);
    }
}

