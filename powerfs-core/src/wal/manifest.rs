//! 段清单（SegManifest）：卷目录内段文件的内存索引。
//!
//! 磁盘上的段头是唯一事实来源；manifest 只是启动时扫描目录构建的内存
//! 索引，按 seg_id 升序维护。归一化规则：**除最后一个段外，其余段一律
//! 视为 Sealed**（崩溃可能发生在"创建新段之后、封旧段之前"，此时旧段
//! 段头 sealed 位未置位，但有后继段即不可再追加）。

use std::path::{Path, PathBuf};

use crate::wal::segment::{
    parse_seg_id, seg_file_name, SegmentError, SegmentHeader, SEG_HEADER_SIZE,
};

/// 段状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentState {
    Sealed,
    Active,
}

/// 段清单条目。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegEntry {
    pub seg_id: u64,
    pub state: SegmentState,
    pub base_lsn: u64,
    pub volume_id: u64,
}

/// 段清单。
#[derive(Debug, Clone)]
pub struct SegManifest {
    dir: PathBuf,
    seg_size: u64,
    /// 按 seg_id 升序。
    entries: Vec<SegEntry>,
}

impl SegManifest {
    /// 扫描 `dir` 下全部段文件构建清单。目录不存在时返回空清单。
    pub fn load(dir: &Path, seg_size: u64) -> Result<Self, SegmentError> {
        let mut entries = Vec::new();
        let read_dir = match std::fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(SegManifest {
                    dir: dir.to_path_buf(),
                    seg_size,
                    entries,
                })
            }
            Err(e) => return Err(SegmentError::io(dir, e)),
        };

        for entry in read_dir {
            let entry = entry.map_err(|e| SegmentError::io(dir, e))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(seg_id) = parse_seg_id(&name) else {
                continue; // 非段文件（superblock/ckpt/tmp 等）忽略
            };
            let path = entry.path();
            let header = read_segment_header(&path)?;
            if header.seg_id != seg_id {
                return Err(SegmentError::BadFileName(format!(
                    "file name seg_id {seg_id} != header seg_id {} in {}",
                    header.seg_id,
                    path.display()
                )));
            }
            entries.push(SegEntry {
                seg_id,
                state: if header.is_sealed() {
                    SegmentState::Sealed
                } else {
                    SegmentState::Active
                },
                base_lsn: header.base_lsn,
                volume_id: header.volume_id,
            });
        }

        entries.sort_by_key(|e| e.seg_id);
        // 归一化：有后继段的段一律 Sealed。
        for i in 0..entries.len().saturating_sub(1) {
            entries[i].state = SegmentState::Sealed;
        }

        Ok(SegManifest {
            dir: dir.to_path_buf(),
            seg_size,
            entries,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn segments(&self) -> &[SegEntry] {
        &self.entries
    }

    /// 当前活跃段（清单中最后一个未封段；归一化后即最后一个段）。
    pub fn active(&self) -> Option<&SegEntry> {
        self.entries
            .last()
            .filter(|e| e.state == SegmentState::Active)
    }

    /// 下一个可用 seg_id。
    pub fn next_seg_id(&self) -> u64 {
        self.entries.last().map(|e| e.seg_id + 1).unwrap_or(1)
    }

    pub fn seg_path(&self, seg_id: u64) -> PathBuf {
        self.dir.join(seg_file_name(seg_id))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn seg_size(&self) -> u64 {
        self.seg_size
    }

    /// 新段登记为 Active；若已有 Active 段（应为换段场景），先置为 Sealed。
    pub fn register(&mut self, seg_id: u64, base_lsn: u64, volume_id: u64) {
        if let Some(last) = self.entries.last_mut() {
            if last.state == SegmentState::Active {
                last.state = SegmentState::Sealed;
            }
        }
        self.entries.push(SegEntry {
            seg_id,
            state: SegmentState::Active,
            base_lsn,
            volume_id,
        });
    }

    /// 标记指定段为 Sealed（磁盘段头的回写由 SegWriter::seal 负责）。
    pub fn mark_sealed(&mut self, seg_id: u64) {
        if let Some(e) = self.entries.iter_mut().find(|e| e.seg_id == seg_id) {
            e.state = SegmentState::Sealed;
        }
    }
}

fn read_segment_header(path: &Path) -> Result<SegmentHeader, SegmentError> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).map_err(|e| SegmentError::io(path, e))?;
    let mut hdr = [0u8; SEG_HEADER_SIZE];
    file.read_exact(&mut hdr)
        .map_err(|e| SegmentError::io(path, e))?;
    SegmentHeader::decode(path, &hdr)
}
