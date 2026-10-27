//! 桶级操作：创建 / 查询 / 列举 / 删除。
//!
//! 桶是 Filer 根目录（inode 0）下的直属目录，名字即桶名；
//! 对象以 `name = key` 平铺在同一目录内，不创建中间目录。

use powerfs_master::filer_proto::powerfs::Entry;
use rustfs_ecstore::api::error::{Error, Result};
use rustfs_storage_api::{BucketInfo, DeleteBucketOptions, MakeBucketOptions};

use crate::backend::PowerFsStore;

/// 单次列举页大小。
pub(crate) const SCAN_PAGE: u64 = 1000;

/// 判断条目是否为目录（按 mode 的文件类型位）。
pub(crate) fn entry_is_dir(entry: &Entry) -> bool {
    entry
        .attributes
        .as_ref()
        .map(|attributes| attributes.mode & 0o170000 == 0o040000)
        .unwrap_or(false)
}

/// 把 Filer 的时间戳转成 `OffsetDateTime`。
///
/// Filer 不同写入路径分别使用秒/毫秒/纳秒（历史遗留），这里按数量级判定，
/// 避免把秒当成纳秒而得到 1970 年的时间。
pub(crate) fn unix_timestamp_to_datetime(value: u64) -> Option<time::OffsetDateTime> {
    if value == 0 {
        return None;
    }
    let value = i128::from(value);
    let nanos = if value >= 10_000_000_000_000_000 {
        value
    } else if value >= 100_000_000_000 {
        value * 1_000_000
    } else {
        value * 1_000_000_000
    };
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()
}

/// 由 Filer 的桶目录信息构造 RustFS 的 `BucketInfo`。
fn bucket_info(name: &str, mtime: u64) -> BucketInfo {
    BucketInfo {
        name: name.to_string(),
        created: unix_timestamp_to_datetime(mtime),
        deleted: None,
        // P1c 不支持版本化与对象锁。
        versioning: false,
        object_locking: false,
    }
}

impl PowerFsStore {
    /// 确认桶存在，否则返回 `BucketNotFound`。
    pub(crate) async fn ensure_bucket(&self, bucket: &str) -> Result<()> {
        if self.filer.get_bucket_root(bucket).await?.is_none() {
            return Err(Error::BucketNotFound(bucket.to_string()));
        }
        Ok(())
    }

    /// 创建桶。已存在且未指定 `force_create` 时返回 `BucketExists`。
    pub(crate) async fn create_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        let exists = self.filer.get_bucket_root(bucket).await?.is_some();
        if exists && !opts.force_create {
            return Err(Error::BucketExists(bucket.to_string()));
        }
        self.filer
            .ensure_bucket_root(bucket)
            .await
            .map(|_| ())
            .map_err(|error| crate::error::bucket_error(bucket, error))
    }

    /// 查询桶信息。
    pub(crate) async fn bucket_info(&self, bucket: &str) -> Result<BucketInfo> {
        let root = self
            .filer
            .get_bucket_root(bucket)
            .await?
            .ok_or_else(|| Error::BucketNotFound(bucket.to_string()))?;
        Ok(bucket_info(bucket, root.mtime))
    }

    /// 列举全部桶。
    pub(crate) async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let roots = self.filer.list_bucket_roots().await?;
        Ok(roots
            .into_iter()
            .map(|root| bucket_info(&root.bucket, root.mtime))
            .collect())
    }

    /// 删除桶。默认要求桶为空；`opts.force` 由服务端先清空扁平子项。
    pub(crate) async fn remove_bucket(
        &self,
        bucket: &str,
        opts: &DeleteBucketOptions,
    ) -> Result<()> {
        if self.filer.get_bucket_root(bucket).await?.is_none() {
            return Err(Error::BucketNotFound(bucket.to_string()));
        }
        self.filer
            .delete_bucket_root(bucket, opts.force)
            .await
            .map_err(|error| crate::error::bucket_error(bucket, error))
    }
}
