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
}

impl MetadataAttr {
    pub fn is_dir(&self) -> bool {
        self.file_type == libc::DT_DIR
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
