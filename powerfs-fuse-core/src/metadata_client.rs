//! MetadataClient trait — 强一致元数据操作的统一接口。
//!
//! 所有元数据修改操作（mkdir/create/unlink/rmdir/rename/symlink/link/setattr）
//! 必须通过此 trait 调用 Filer Raft leader，保证强一致。
//! 读操作（lookup/readdir/getattr/readlink/statfs）也通过此 trait，
//! 走 Leader Lease Read（不经 read index，避免额外 RTT）。
//!
//! 设计要点：
//! - shard_id 由调用方传入（按 bucket 分片，见方案 3.9）
//! - 返回 MetadataAttr 统一属性结构
//! - trait 方法异步，由 MetaShardClient 实现
//! - 取代废弃的 MetadataProvider trait（仅支持 read，不支持 write）

use powerfs_common::error::Result;
use powerfs_layout::placement::Placement;

/// 元数据属性（FUSE 回调需要的字段子集）
#[derive(Clone, Debug)]
pub struct MetadataAttr {
    pub inode: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub mtime: u64,
    pub atime: u64,
    pub ctime: u64,
    pub nlink: u32,
    pub rdev: u64,
    pub file_type: u8, // FileType::to_d_type()
    pub symlink_target: Option<String>,
    /// Filer create 响应返回的 volume_id（Zone 自分配）。
    /// 仅 create 响应填充，lookup/getattr 响应可能为 None（由 chunks 字段单独编码）。
    pub volume_id: Option<u64>,
    /// Filer create 响应返回的 needle_id（file_key）。
    pub file_key: Option<u64>,
    /// P2.5: 数据分布策略 (Inline/Flat/Stripe...), 来自 FileLayout TLV.
    /// None 表示响应未携带 FileLayout (如 mkdir).
    pub placement: Option<Placement>,
    /// P2.5: Inline 数据 (来自 GETATTR/LOOKUP 响应的 ChunkEncoding::InlineData).
    /// 文件以 Inline 模式存储时, 数据直接在 Filer 元数据中, 客户端一次 RPC 拿全.
    pub inline_data: Option<Vec<u8>>,
    /// P2.5: Inline 阈值 (来自 CREATE 响应 Placement::Inline.max_size).
    /// 客户端据此判断累计写入是否超阈值 (需迁移到 Flat).
    pub inline_max_size: Option<u32>,
    /// P3: Chunk 列表 (来自 CREATE/GETATTR 响应的 ChunkEncoding::PerChunk).
    /// Stripe 模式下, 每个 chunk 对应一个 stripe unit (volume_id + needle_id).
    /// Flat 模式下, 通常为单个 chunk.
    pub chunks: Vec<powerfs_layout::encoding::ChunkRef>,
}

impl MetadataAttr {
    pub fn is_dir(&self) -> bool {
        self.file_type == libc::DT_DIR
    }

    /// P2.5: 文件是否以 Inline 模式存储 (数据在 Filer 元数据中).
    /// create 响应携带 `Placement::Inline` 时为 true; lookup/getattr 响应
    /// 携带 `Placement::Inline` (已关闭的 inline 文件) 时也为 true.
    pub fn is_inline(&self) -> bool {
        matches!(
            self.placement.as_ref(),
            Some(Placement::Inline { .. })
        )
    }

    /// P3: 文件是否以 Stripe 模式存储 (多 volume 并行).
    pub fn is_stripe(&self) -> bool {
        matches!(
            self.placement.as_ref(),
            Some(Placement::Stripe { .. } | Placement::WideStripe { .. })
        )
    }
}

/// 目录条目（readdir 返回）
#[derive(Clone, Debug)]
pub struct MetadataDirEntry {
    pub inode: u64,
    pub name: String,
    pub file_type: u8,
    pub offset: u64,
}

/// setattr 操作参数（仅更新提供的字段，None 表示不修改）
#[derive(Clone, Debug, Default)]
pub struct SetattrParams {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<u64>,
    pub mtime: Option<u64>,
}

/// statfs 返回信息
#[derive(Clone, Debug, Default)]
pub struct MetadataStatfs {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub total_inodes: u64,
    pub free_inodes: u64,
    pub block_size: u32,
}

/// 强一致元数据操作接口。
///
/// 所有方法走 Filer Raft leader：
/// - 写操作：Leader 提交 Raft log 后返回
/// - 读操作：Leader Lease Read（不经 read index）
///
/// 调用方负责传入正确的 shard_id（按 bucket 分片）。
/// Filer leader 切换时由实现内部重试，调用方无感。
pub trait MetadataClient: Send + Sync {
    /// lookup：查询目录条目
    fn lookup(
        &self,
        parent_ino: u64,
        name: &str,
        shard_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<MetadataAttr>> + Send + '_>>;

    /// mkdir：创建目录
    fn mkdir(
        &self,
        parent_ino: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        shard_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<MetadataAttr>> + Send + '_>>;

    /// create：创建普通文件
    /// fid_info: Optional (volume_id, cookie, file_key) to persist chunk mapping
    /// at create time, preventing "has no fid" errors on cache miss + reopen.
    fn create(
        &self,
        parent_ino: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        shard_id: u64,
        fid_info: Option<(u64, u64, u64)>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<MetadataAttr>> + Send + '_>>;

    /// unlink：删除文件（仅文件，非目录）
    fn unlink(
        &self,
        parent_ino: u64,
        name: &str,
        shard_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>>;

    /// rmdir：删除空目录
    fn rmdir(
        &self,
        parent_ino: u64,
        name: &str,
        shard_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>>;

    /// rename：重命名/移动
    fn rename(
        &self,
        parent_ino: u64,
        name: &str,
        new_parent_ino: u64,
        new_name: &str,
        shard_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<MetadataAttr>> + Send + '_>>;

    /// symlink：创建符号链接
    fn symlink(
        &self,
        parent_ino: u64,
        name: &str,
        target: &str,
        shard_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<MetadataAttr>> + Send + '_>>;

    /// readlink：读取符号链接目标
    fn readlink(
        &self,
        ino: u64,
        shard_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String>> + Send + '_>>;

    /// link：创建硬链接
    fn link(
        &self,
        ino: u64,
        new_parent_ino: u64,
        new_name: &str,
        shard_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<MetadataAttr>> + Send + '_>>;

    /// readdir：列出目录条目
    fn readdir(
        &self,
        ino: u64,
        offset: u64,
        count: u32,
        shard_id: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<MetadataDirEntry>>> + Send + '_>,
    >;

    /// getattr：获取 inode 属性
    fn getattr(
        &self,
        ino: u64,
        shard_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<MetadataAttr>> + Send + '_>>;

    /// setattr：修改 inode 属性
    fn setattr(
        &self,
        ino: u64,
        params: &SetattrParams,
        shard_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<MetadataAttr>> + Send + '_>>;

    /// statfs：获取文件系统统计信息
    fn statfs(
        &self,
        shard_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<MetadataStatfs>> + Send + '_>>;
}
