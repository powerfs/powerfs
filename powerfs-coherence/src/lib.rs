//! powerfs-coherence: 模块化元数据缓存一致性 (CRDT 主线实现)
//!
//! 对外 trait：
//! - [`CacheCoherence`]（fuse 客户端侧）
//! - [`CoherenceAuthority`]（filer 服务端侧）
//!
//! 中性传输类型 [`DeltaWire`] / [`VectorClockWire`]：fuse 端用 `powerfs_orset::DeltaOp`，
//! filer 端用 proto `crate::powerfs::DeltaOp`，两侧各自与 wire 类型互转，互不依赖。

pub mod crdt_client;
pub mod crdt_server;
pub mod mock;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// 对外 trait
// ---------------------------------------------------------------------------

/// 客户端侧缓存一致性接口（fuse 用）
#[async_trait::async_trait]
pub trait CacheCoherence: Send + Sync {
    /// 本地写操作完成后触发（记录变更，用于后续 delta push）
    fn on_local_write(&self, parent_ino: u64, op: &WriteOp);

    /// 校验缓存是否有效。CRDT 实现恒返回 Valid（合法副本无需验证）。
    fn validate_cache(&self, kind: CacheKind) -> ValidationResult {
        let _ = kind;
        ValidationResult::Valid
    }

    /// 远端 delta 到达时触发（merge 到本地副本 + 联动失效）
    async fn on_remote_delta(&self, parent_ino: u64, delta: DeltaWire);

    /// 记录版本号（CRDT 模式下空实现，预留 epoch 模式）
    fn record_version(&self, kind: CacheKind, version: u64) {
        let _ = (kind, version);
    }
}

/// 服务端侧权威接口（filer 用）
#[async_trait::async_trait]
pub trait CoherenceAuthority: Send + Sync {
    /// 写操作提交后触发，返回本次 delta 的版本（广播给其他客户端）
    fn on_write_committed(&self, parent_ino: u64, op: &WriteOp) -> u64;

    /// 客户端 pull delta：返回 client_vclock 之后的新增 delta
    async fn pull_delta(
        &self,
        dir_ino: u64,
        client_vclock: &VectorClockWire,
    ) -> Result<(Vec<DeltaWire>, VectorClockWire), String>;
}

/// delta 同步通道（fuse 端实现，封装 meta_shard_client 的 push/pull_delta 调用）。
///
/// powerfs-coherence 不依赖 powerfs-fuse-core（避免循环依赖），
/// fuse 端在 meta_shard_client.rs 中实现此 trait 并注入 CrdtReplicaCoherence。
#[async_trait::async_trait]
pub trait DeltaSyncChannel: Send + Sync {
    async fn push_delta(&self, req: &PushDeltaRequest) -> Result<PushDeltaResponse, String>;
    async fn pull_delta(&self, req: &PullDeltaRequest) -> Result<PullDeltaResponse, String>;
    async fn alloc_inode_batch(
        &self,
        req: &AllocInodeBatchRequest,
    ) -> Result<AllocInodeBatchResponse, String>;
    async fn update_inode_size_chunks(
        &self,
        req: &UpdateInodeSizeChunksRequest,
    ) -> Result<UpdateInodeSizeChunksResponse, String>;
}

/// MetadataCache 联动失效接口（fuse 端实现，注入 CrdtReplicaCoherence）。
///
/// delta merge 后调 invalidate_inode / invalidate_dir 联动失效 MetadataCache。
/// size/chunks 不被失效（强一致，仅 lease 刷新）。
pub trait MetadataCacheInvalidator: Send + Sync {
    fn invalidate_inode(&self, inode: u64);
    fn invalidate_dir(&self, parent_inode: u64);
}

// ---------------------------------------------------------------------------
// 公共枚举类型
// ---------------------------------------------------------------------------

/// 缓存类别（区分目录条目 / inode 属性 / 数据账本）
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheKind {
    DirEntry,
    InodeAttr,
    DataLedger,
}

/// 校验结果
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValidationResult {
    /// 缓存有效，可直接用
    Valid,
    /// 缓存过期，需 pull
    Stale,
    /// 缓存缺失，需 lookup/pull
    Miss,
}

/// 本地写操作描述（用于触发 delta 计算）
#[derive(Clone, Debug)]
pub struct WriteOp {
    pub kind: WriteOpKind,
    pub dir_ino: u64,
    pub name: String,
    pub inode: u64,
    pub parent_ino: u64,
}

/// 写操作类型
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteOpKind {
    Create,
    Remove,
    Rename,
    SetAttr,
}

// ---------------------------------------------------------------------------
// 中性传输类型（wire format，JSON 序列化）
// ---------------------------------------------------------------------------

/// Delta 操作类型
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaOpType {
    Add,
    Remove,
    Rename,
    SetAttr,
}

/// 中性 VectorClock（wire 格式）
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VectorClockWire {
    /// (client_id, seq) 列表
    pub entries: Vec<VectorClockEntryWire>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct VectorClockEntryWire {
    pub client_id: u64,
    pub seq: u64,
}

/// 中性 DirEntry（wire 格式）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirEntryWire {
    pub name: String,
    pub client_id: u64,
    pub seq: u64,
    pub inode: u64,
    pub generation: u64,
    pub file_type: u8,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub mtime: u64,
    pub atime: u64,
    pub ctime: u64,
    pub nlink: u32,
    pub rdev: u64,
    pub parent_ino: u64,
    pub chunks: Vec<ChunkWire>,
    pub symlink_target: Option<String>,
}

/// 中性 EntryId（wire 格式）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntryIdWire {
    pub name: String,
    pub client_id: u64,
    pub seq: u64,
    pub parent_ino: u64,
}

/// 中性 SetAttr（wire 格式）
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SetAttrWire {
    pub inode: u64,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub mtime: Option<u64>,
    pub nlink: Option<u32>,
    pub chunks: Vec<ChunkWire>,
}

/// 中性 Chunk（wire 格式）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkWire {
    pub offset: u64,
    pub size: u64,
    pub mtime: u64,
    pub fid: String,
    pub cookie: u32,
    pub crc32: u32,
}

/// 中性 Delta（wire 格式）— net 层传输的顶层类型
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaWire {
    pub op_type: DeltaOpType,
    pub vclock: VectorClockWire,
    /// Add / Rename(new) 用
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry: Option<DirEntryWire>,
    /// Remove / Rename(old) 用
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<EntryIdWire>,
    /// Rename(old) 用
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_entry_id: Option<EntryIdWire>,
    /// SetAttr 用
    #[serde(skip_serializing_if = "Option::is_none")]
    pub setattr: Option<SetAttrWire>,
}

/// push_delta 请求体（net 层 body 的 JSON 格式）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PushDeltaRequest {
    pub shard_id: u64,
    pub client_id: String,
    pub deltas: Vec<DeltaWire>,
    pub client_vclock: Option<VectorClockWire>,
}

/// push_delta 响应体
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PushDeltaResponse {
    pub success: bool,
    pub error: String,
    pub server_vclock: VectorClockWire,
}

/// pull_delta 请求体
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullDeltaRequest {
    pub shard_id: u64,
    pub client_id: String,
    pub client_vclock: Option<VectorClockWire>,
}

/// pull_delta 响应体
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullDeltaResponse {
    pub deltas: Vec<DeltaWire>,
    pub server_vclock: VectorClockWire,
}

/// alloc_inode_batch 请求体
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AllocInodeBatchRequest {
    pub shard_id: u64,
    pub count: u32,
    pub client_id: String,
}

/// alloc_inode_batch 响应体
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AllocInodeBatchResponse {
    pub success: bool,
    pub error: String,
    pub start_inode: u64,
    pub end_inode: u64,
}

/// update_inode_size_chunks 请求体
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdateInodeSizeChunksRequest {
    pub shard_id: u64,
    pub inode: u64,
    pub size: u64,
    pub chunks: Vec<ChunkWire>,
    pub client_id: String,
}

/// update_inode_size_chunks 响应体
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdateInodeSizeChunksResponse {
    pub success: bool,
    pub error: String,
}

/// open_count 增减请求体（Phase 3.5.3: GC 第三条件 open_count==0 追踪）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenCountRequest {
    pub shard_id: u64,
    pub inode: u64,
}

/// open_count 增减响应体
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenCountResponse {
    pub success: bool,
    pub open_count: u32,
    pub error: String,
}

// ---------------------------------------------------------------------------
// powerfs_orset <-> wire 转换（fuse 端用）
// ---------------------------------------------------------------------------

impl From<&powerfs_orset::VectorClock> for VectorClockWire {
    fn from(vc: &powerfs_orset::VectorClock) -> Self {
        let entries: Vec<_> = vc
            .iter()
            .map(|(cid, seq)| VectorClockEntryWire {
                client_id: *cid,
                seq: *seq,
            })
            .collect();
        Self { entries }
    }
}

impl From<&VectorClockWire> for powerfs_orset::VectorClock {
    fn from(w: &VectorClockWire) -> Self {
        let mut vc = powerfs_orset::VectorClock::new();
        for e in &w.entries {
            vc.observe(e.client_id, e.seq);
        }
        vc
    }
}

impl From<&powerfs_orset::CachedFileChunk> for ChunkWire {
    fn from(c: &powerfs_orset::CachedFileChunk) -> Self {
        Self {
            offset: c.offset,
            size: c.size,
            mtime: c.mtime,
            fid: c.fid.clone(),
            cookie: c.cookie,
            crc32: c.crc32,
        }
    }
}

impl From<&ChunkWire> for powerfs_orset::CachedFileChunk {
    fn from(w: &ChunkWire) -> Self {
        Self {
            offset: w.offset,
            size: w.size,
            mtime: w.mtime,
            fid: w.fid.clone(),
            cookie: w.cookie,
            crc32: w.crc32,
        }
    }
}

/// FileType → u8 编码
fn file_type_to_u8(ft: &powerfs_orset::FileType) -> u8 {
    use powerfs_orset::FileType::*;
    match ft {
        RegularFile => 0,
        Directory => 1,
        Symlink => 2,
        CharDevice => 3,
        BlockDevice => 4,
        Fifo => 5,
        Socket => 6,
    }
}

/// u8 → FileType 解码
fn u8_to_file_type(v: u8) -> powerfs_orset::FileType {
    use powerfs_orset::FileType::*;
    match v {
        0 => RegularFile,
        1 => Directory,
        2 => Symlink,
        3 => CharDevice,
        4 => BlockDevice,
        5 => Fifo,
        6 => Socket,
        _ => RegularFile,
    }
}

impl From<&powerfs_orset::DirEntry> for DirEntryWire {
    fn from(e: &powerfs_orset::DirEntry) -> Self {
        Self {
            name: e.id.name.clone(),
            client_id: e.id.client_id,
            seq: e.id.seq,
            inode: e.inode,
            generation: e.generation,
            file_type: file_type_to_u8(&e.file_type),
            mode: e.mode,
            uid: e.uid,
            gid: e.gid,
            size: e.size,
            mtime: e.mtime,
            atime: e.atime,
            ctime: e.ctime,
            nlink: e.nlink,
            rdev: e.rdev,
            parent_ino: e.parent_ino,
            chunks: e.chunks.iter().map(ChunkWire::from).collect(),
            symlink_target: e.symlink_target.clone(),
        }
    }
}

impl From<&DirEntryWire> for powerfs_orset::DirEntry {
    fn from(w: &DirEntryWire) -> Self {
        let id = powerfs_orset::EntryId::new(&w.name, w.client_id, w.seq);
        Self {
            id,
            inode: w.inode,
            generation: w.generation,
            file_type: u8_to_file_type(w.file_type),
            mode: w.mode,
            uid: w.uid,
            gid: w.gid,
            size: w.size,
            mtime: w.mtime,
            atime: w.atime,
            ctime: w.ctime,
            nlink: w.nlink,
            rdev: w.rdev,
            parent_ino: w.parent_ino,
            chunks: w
                .chunks
                .iter()
                .map(powerfs_orset::CachedFileChunk::from)
                .collect(),
            symlink_target: w.symlink_target.clone(),
            extended: std::collections::HashMap::new(),
        }
    }
}

impl From<&powerfs_orset::EntryId> for EntryIdWire {
    fn from(id: &powerfs_orset::EntryId) -> Self {
        // EntryId 本身没有 parent_ino，由上层 DirEntry 补充
        Self {
            name: id.name.clone(),
            client_id: id.client_id,
            seq: id.seq,
            parent_ino: 0,
        }
    }
}

/// powerfs_orset::DeltaOp → DeltaWire（fuse 端发送时用）
impl From<&powerfs_orset::DeltaOp> for DeltaWire {
    fn from(op: &powerfs_orset::DeltaOp) -> Self {
        use powerfs_orset::DeltaOp::*;
        match op {
            Add { entry, vclock } => Self {
                op_type: DeltaOpType::Add,
                vclock: VectorClockWire::from(vclock),
                entry: Some(DirEntryWire::from(entry)),
                entry_id: None,
                old_entry_id: None,
                setattr: None,
            },
            Remove { id, vclock } => Self {
                op_type: DeltaOpType::Remove,
                vclock: VectorClockWire::from(vclock),
                entry: None,
                entry_id: Some(EntryIdWire {
                    name: id.name.clone(),
                    client_id: id.client_id,
                    seq: id.seq,
                    parent_ino: 0,
                }),
                old_entry_id: None,
                setattr: None,
            },
            Rename {
                old_id,
                new_entry,
                vclock,
            } => Self {
                op_type: DeltaOpType::Rename,
                vclock: VectorClockWire::from(vclock),
                entry: Some(DirEntryWire::from(new_entry)),
                entry_id: None,
                old_entry_id: Some(EntryIdWire {
                    name: old_id.name.clone(),
                    client_id: old_id.client_id,
                    seq: old_id.seq,
                    parent_ino: new_entry.parent_ino,
                }),
                setattr: None,
            },
            SetAttr {
                inode,
                mode,
                uid,
                gid,
                size,
                mtime,
                nlink,
                vclock,
            } => Self {
                op_type: DeltaOpType::SetAttr,
                vclock: VectorClockWire::from(vclock),
                entry: None,
                entry_id: None,
                old_entry_id: None,
                setattr: Some(SetAttrWire {
                    inode: *inode,
                    mode: *mode,
                    uid: *uid,
                    gid: *gid,
                    size: *size,
                    mtime: *mtime,
                    nlink: *nlink,
                    chunks: vec![],
                }),
            },
        }
    }
}

/// DeltaWire → powerfs_orset::DeltaOp（fuse 端接收时用）
impl TryFrom<&DeltaWire> for powerfs_orset::DeltaOp {
    type Error = String;

    fn try_from(w: &DeltaWire) -> Result<Self, Self::Error> {
        use powerfs_orset::{DeltaOp, DirEntry, EntryId};
        let vclock = powerfs_orset::VectorClock::from(&w.vclock);
        match w.op_type {
            DeltaOpType::Add => {
                let ew = w
                    .entry
                    .as_ref()
                    .ok_or_else(|| "Add missing entry".to_string())?;
                Ok(DeltaOp::Add {
                    entry: DirEntry::from(ew),
                    vclock,
                })
            }
            DeltaOpType::Remove => {
                let idw = w
                    .entry_id
                    .as_ref()
                    .ok_or_else(|| "Remove missing entry_id".to_string())?;
                Ok(DeltaOp::Remove {
                    id: EntryId::new(&idw.name, idw.client_id, idw.seq),
                    vclock,
                })
            }
            DeltaOpType::Rename => {
                let old = w
                    .old_entry_id
                    .as_ref()
                    .ok_or_else(|| "Rename missing old_entry_id".to_string())?;
                let new = w
                    .entry
                    .as_ref()
                    .ok_or_else(|| "Rename missing entry".to_string())?;
                Ok(DeltaOp::Rename {
                    old_id: EntryId::new(&old.name, old.client_id, old.seq),
                    new_entry: DirEntry::from(new),
                    vclock,
                })
            }
            DeltaOpType::SetAttr => {
                let s = w
                    .setattr
                    .as_ref()
                    .ok_or_else(|| "SetAttr missing setattr".to_string())?;
                Ok(DeltaOp::SetAttr {
                    inode: s.inode,
                    mode: s.mode,
                    uid: s.uid,
                    gid: s.gid,
                    size: s.size,
                    mtime: s.mtime,
                    nlink: s.nlink,
                    vclock,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vclock_roundtrip() {
        let mut vc = powerfs_orset::VectorClock::new();
        vc.increment(1);
        vc.increment(1);
        vc.observe(2, 5);
        let wire = VectorClockWire::from(&vc);
        let vc2 = powerfs_orset::VectorClock::from(&wire);
        assert_eq!(vc2.get(1), 2);
        assert_eq!(vc2.get(2), 5);
    }

    #[test]
    fn test_delta_wire_add_roundtrip() {
        let entry = powerfs_orset::DirEntry::new_file(
            powerfs_orset::EntryId::new("test.txt", 1, 1),
            100,
            1,
            0o644,
            0,
            0,
        );
        let mut vc = powerfs_orset::VectorClock::new();
        vc.increment(1);
        let op = powerfs_orset::DeltaOp::Add {
            entry: entry.clone(),
            vclock: vc.clone(),
        };
        let wire = DeltaWire::from(&op);
        let json = serde_json::to_string(&wire).unwrap();
        let wire2: DeltaWire = serde_json::from_str(&json).unwrap();
        let op2 = powerfs_orset::DeltaOp::try_from(&wire2).unwrap();
        match op2 {
            powerfs_orset::DeltaOp::Add {
                entry: e2,
                vclock: vc2,
            } => {
                assert_eq!(e2.inode, 100);
                assert_eq!(e2.id.name, "test.txt");
                assert_eq!(vc2.get(1), 1);
            }
            _ => panic!("expected Add"),
        }
    }

    #[test]
    fn test_delta_wire_remove_roundtrip() {
        let id = powerfs_orset::EntryId::new("foo", 2, 3);
        let mut vc = powerfs_orset::VectorClock::new();
        vc.increment(2);
        let op = powerfs_orset::DeltaOp::Remove {
            id: id.clone(),
            vclock: vc,
        };
        let wire = DeltaWire::from(&op);
        let json = serde_json::to_string(&wire).unwrap();
        let wire2: DeltaWire = serde_json::from_str(&json).unwrap();
        let op2 = powerfs_orset::DeltaOp::try_from(&wire2).unwrap();
        match op2 {
            powerfs_orset::DeltaOp::Remove { id: id2, .. } => {
                assert_eq!(id2, id);
            }
            _ => panic!("expected Remove"),
        }
    }
}
