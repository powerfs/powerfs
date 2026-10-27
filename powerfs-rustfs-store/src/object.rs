//! 对象级操作：PUT / GET / HEAD / DELETE。
//!
//! 落盘顺序与 `powerfs-master/src/s3/server.rs` 保持一致：
//! `assign_volume` → `write_needle` → 提交 Filer 对象元数据。
//!
//! Filer 侧 `PutObjectMeta` 对已存在的 key 原地替换 size/chunks，
//! 因此覆盖写是单次 RPC，不存在「删旧建新」的窗口。

use std::collections::HashMap;
use std::sync::Arc;

use http::HeaderMap;
use md5::{Digest, Md5};
use powerfs_common::types::VolumeId;
use powerfs_master::filer_proto::powerfs::{Entry, FileChunk, FuseAttributes};
use powerfs_net::protocol::MAX_DATA_SIZE;
use rustfs_ecstore::api::error::{Error, Result};
use rustfs_ecstore::api::object::{
    GetObjectBodySource, GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader,
};
use rustfs_storage_api::HTTPRangeSpec;
use tokio::io::AsyncReadExt;

use crate::backend::PowerFsStore;
use crate::bucket::{entry_is_dir, unix_timestamp_to_datetime};
use crate::filer::ObjectMetaWrite;

/// 对象元数据键：内容摘要（单段对象为 MD5 hex，与 S3 协议一致）。
pub(crate) const EXT_ETAG: &str = "s3.etag";
/// 对象元数据键：content-type。
pub(crate) const EXT_CONTENT_TYPE: &str = "s3.content-type";
/// 对象元数据键：tagging（URL 编码 tag set）。与 native 一致，该键
/// 同时出现在 user_defined 中，由 S3 层渲染响应时过滤。
pub(crate) const EXT_TAGGING: &str = "s3.x-amz-tagging";

impl PowerFsStore {
    /// 读取对象元数据条目：先确认桶存在，再取对象条目。
    pub(crate) async fn load_entry(&self, bucket: &str, object: &str) -> Result<Entry> {
        self.ensure_bucket(bucket).await?;
        self.filer
            .get_object_meta(bucket, object)
            .await?
            .and_then(|response| response.entry)
            .ok_or_else(|| Error::ObjectNotFound(bucket.to_string(), object.to_string()))
    }

    /// 查询对象元数据。
    pub(crate) async fn object_info(
        &self,
        bucket: &str,
        object: &str,
        _opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let entry = self.load_entry(bucket, object).await?;
        Ok(object_info_from_entry(bucket, &entry))
    }

    /// 写入对象。
    ///
    /// P1c 允许把请求体一次性读入内存；后续阶段会改成流式写 needle。
    pub(crate) async fn write_object(
        &self,
        bucket: &str,
        object: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        self.ensure_bucket(bucket).await?;

        // P1c: 全量缓冲。后续改成边读边写 needle，避免大对象占用内存。
        let capacity = usize::try_from(data.size().max(0)).unwrap_or(0);
        let mut body = Vec::with_capacity(capacity);
        data.stream
            .read_to_end(&mut body)
            .await
            .map_err(Error::Io)?;

        // 条件写在正文读完之后判定：与上游「提交时检查」一致，并发写入
        // 在正文传输期间完成时，后者应看到新的 etag 并得到 412。
        self.check_write_preconditions(bucket, object, opts).await?;

        let size = u64::try_from(body.len()).unwrap_or(u64::MAX);
        let now = unix_secs_now();
        let etag = hex::encode(Md5::digest(&body));

        let mut extended: HashMap<String, Vec<u8>> = HashMap::new();
        extended.insert(EXT_ETAG.to_string(), etag.clone().into_bytes());
        // 用户元数据整体落盘；键统一小写，content-type 与 etag 因此可用固定键回读。
        for (key, value) in &opts.user_defined {
            let key = key.to_ascii_lowercase();
            if key == EXT_ETAG || key == EXT_CONTENT_TYPE {
                continue;
            }
            extended.insert(format!("s3.{key}"), value.clone().into_bytes());
        }

        // 0 字节对象不写 needle；其余按 2MB（TLV 单帧上限）分块，
        // 每块独立 assign_volume + write_needle。
        let chunks = if size == 0 {
            Vec::new()
        } else {
            let mut chunks = Vec::with_capacity(body.len().div_ceil(MAX_DATA_SIZE));
            for (index, slice) in body.chunks(MAX_DATA_SIZE).enumerate() {
                let (fid, nodes) = self
                    .master
                    .assign_volume(&self.config.replication, &self.config.collection)
                    .await
                    .map_err(crate::error::from_master)?;
                let node = nodes
                    .first()
                    .ok_or_else(|| Error::other("assign_volume 未返回任何数据节点"))?;
                // master 下发的 DataNodeInfo.grpc_port 实际承载 powerfs-net 数据端口
                // （net_port），needle 读写必须走 TLV 协议而非 gRPC。
                let node_address = format!("{}:{}", node.address, node.grpc_port);

                self.volumes
                    .write_needle(&node_address, fid.volume_id.0, fid.file_key, slice)
                    .await
                    .map_err(crate::error::from_volume)?;

                chunks.push(FileChunk {
                    offset: (index * MAX_DATA_SIZE) as u64,
                    size: slice.len() as u64,
                    mtime: now,
                    needle_id: fid.file_key,
                    volume_id: fid.volume_id.0,
                    crc32: 0,
                });
            }
            chunks
        };

        self.filer
            .put_object_meta(ObjectMetaWrite {
                bucket,
                key: object,
                size,
                chunks: &chunks,
                extended: &extended,
            })
            .await?;

        Ok(object_info_from_entry(
            bucket,
            &Entry {
                name: object.to_string(),
                directory: format!("/{bucket}"),
                attributes: Some(file_attributes(size, now)),
                chunks,
                extended,
                content_size: size,
                disk_size: size,
                ..Entry::default()
            },
        ))
    }

    /// 写路径条件请求判定（If-Match / If-None-Match）。
    ///
    /// 语义对齐上游 `set_disk::evaluate_write_precondition`：
    /// - 对象存在且条件不满足 → `PreconditionFailed`（412）；
    /// - 对象不存在：带 If-Match → `ObjectNotFound`（404 NoSuchKey），
    ///   仅带 If-None-Match → 放行（首次创建）。
    pub(crate) async fn check_write_preconditions(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<()> {
        let Some(preconditions) = &opts.http_preconditions else {
            return Ok(());
        };
        let if_match = preconditions.if_match_value();
        let if_none_match = preconditions.if_none_match_value();
        if if_match.is_none() && if_none_match.is_none() {
            return Ok(());
        }

        let existing = self
            .filer
            .get_object_meta(bucket, object)
            .await?
            .and_then(|response| response.entry);

        match existing {
            Some(entry) => {
                let etag = entry_etag(&entry);
                if should_prevent_write(etag.as_deref(), if_none_match, if_match) {
                    return Err(Error::PreconditionFailed);
                }
                Ok(())
            }
            None if if_match.is_some() => Err(Error::ObjectNotFound(
                bucket.to_string(),
                object.to_string(),
            )),
            None => Ok(()),
        }
    }

    /// 读取对象，支持单个 HTTP range。
    ///
    /// 多分块对象按 chunk 顺序拼接；带 range 时只读取与请求区间相交的
    /// chunk，避免整对象回源。
    pub(crate) async fn read_object(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        _h: HeaderMap,
        _opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        let entry = self.load_entry(bucket, object).await?;
        let info = object_info_from_entry(bucket, &entry);

        // 对象大小以元数据为准，避免 needle 长度漂移影响 HEAD/GET 一致。
        let total = i64::try_from(entry.content_size).unwrap_or(i64::MAX);

        // 计算需要返回的对象内区间 [want_start, want_end)。
        let (want_start, want_end) = match &range {
            None => (0u64, entry.content_size),
            Some(spec) => {
                let (offset, length) = spec
                    .get_offset_length(total)
                    .map_err(|error| Error::InvalidRangeSpec(error.to_string()))?;
                let start = (offset as u64).min(entry.content_size);
                let end = start
                    .saturating_add(u64::try_from(length).unwrap_or(u64::MAX))
                    .min(entry.content_size);
                (start, end)
            }
        };

        // 只拉取与 [want_start, want_end) 相交的 chunk，并裁掉区间外部分。
        let mut body = Vec::with_capacity(usize::try_from(want_end - want_start).unwrap_or(0));
        for chunk in &entry.chunks {
            let chunk_start = chunk.offset;
            let chunk_end = chunk.offset.saturating_add(chunk.size);
            if chunk_end <= want_start || chunk_start >= want_end {
                continue;
            }
            let address = self.volume_address(chunk.volume_id).await?;
            let needle = self
                .volumes
                .read_needle(&address, chunk.volume_id, chunk.needle_id)
                .await
                .map_err(crate::error::from_volume)?;
            // needle 数据即该 chunk 的完整内容，裁出与请求区间相交的部分。
            let from = usize::try_from(want_start.saturating_sub(chunk_start)).unwrap_or(0);
            let to = usize::try_from(want_end.min(chunk_end) - chunk_start)
                .unwrap_or(usize::MAX)
                .min(needle.len());
            if from < to {
                body.extend_from_slice(&needle[from..to]);
            }
        }

        Ok(GetObjectReader {
            stream: Box::new(std::io::Cursor::new(body)),
            object_info: info,
            // 外部后端不参与 RustFS 的对象数据缓存钩子。
            buffered_body: None,
            body_source: GetObjectBodySource::Unprobed,
        })
    }

    /// 删除对象：先删元数据，再尽力删 needle。
    pub(crate) async fn remove_object(
        &self,
        bucket: &str,
        object: &str,
        _opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        let entry = self.load_entry(bucket, object).await?;
        let info = object_info_from_entry(bucket, &entry);

        self.filer.delete_object_meta(bucket, object).await?;

        for chunk in &entry.chunks {
            match self.volume_address(chunk.volume_id).await {
                Ok(address) => {
                    if let Err(error) = self
                        .volumes
                        .delete_needle(&address, chunk.volume_id, chunk.needle_id)
                        .await
                    {
                        // 数据清理尽力而为：needle 删除失败不阻断元数据删除。
                        tracing::warn!(
                            bucket = bucket,
                            object = object,
                            volume_id = chunk.volume_id,
                            error = %error,
                            "删除 needle 失败，元数据已删除"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        bucket = bucket,
                        object = object,
                        volume_id = chunk.volume_id,
                        error = %error,
                        "定位 volume 失败，跳过 needle 删除"
                    );
                }
            }
        }

        Ok(info)
    }

    /// 服务端复制：深拷贝数据（逐 chunk 读源、写新 needle），目标与源
    /// 完全独立，删除互不影响。元数据以 `src_info` 为准——S3 处理器已按
    /// COPY/REPLACE 指令合并好 user_defined / user_tags / content_type，
    /// 且 ETag 继承源对象（simple copy 不改变内容）。
    #[allow(clippy::too_many_arguments)] // 参数个数由 S3 copy 契约决定
    pub(crate) async fn copy_object_impl(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        src_info: &mut ObjectInfo,
        _src_opts: &ObjectOptions,
        _dst_opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        self.ensure_bucket(dst_bucket).await?;
        let src_entry = self.load_entry(src_bucket, src_object).await?;
        let now = unix_secs_now();

        // 深拷贝：每个源 chunk 读出后写入新分配的 needle。
        let mut chunks = Vec::with_capacity(src_entry.chunks.len());
        for chunk in &src_entry.chunks {
            let read_address = self.volume_address(chunk.volume_id).await?;
            let data = self
                .volumes
                .read_needle(&read_address, chunk.volume_id, chunk.needle_id)
                .await
                .map_err(crate::error::from_volume)?;

            let (fid, nodes) = self
                .master
                .assign_volume(&self.config.replication, &self.config.collection)
                .await
                .map_err(crate::error::from_master)?;
            let node = nodes
                .first()
                .ok_or_else(|| Error::other("assign_volume 未返回任何数据节点"))?;
            // 与 write_object 一致：master 下发的 grpc_port 实为 net 数据端口。
            let write_address = format!("{}:{}", node.address, node.grpc_port);
            self.volumes
                .write_needle(&write_address, fid.volume_id.0, fid.file_key, &data)
                .await
                .map_err(crate::error::from_volume)?;

            chunks.push(FileChunk {
                offset: chunk.offset,
                size: data.len() as u64,
                mtime: now,
                needle_id: fid.file_key,
                volume_id: fid.volume_id.0,
                crc32: 0,
            });
        }

        let size = src_entry.content_size;
        let mut extended: HashMap<String, Vec<u8>> = HashMap::new();
        if let Some(etag) = &src_info.etag {
            extended.insert(EXT_ETAG.to_string(), etag.clone().into_bytes());
        }
        if let Some(content_type) = &src_info.content_type {
            extended.insert(
                EXT_CONTENT_TYPE.to_string(),
                content_type.clone().into_bytes(),
            );
        }
        // user_defined 已含 effective tags（x-amz-tagging 键），统一小写落盘。
        for (key, value) in src_info.user_defined.iter() {
            extended.insert(
                format!("s3.{}", key.to_ascii_lowercase()),
                value.clone().into_bytes(),
            );
        }

        self.filer
            .put_object_meta(ObjectMetaWrite {
                bucket: dst_bucket,
                key: dst_object,
                size,
                chunks: &chunks,
                extended: &extended,
            })
            .await?;

        Ok(object_info_from_entry(
            dst_bucket,
            &Entry {
                name: dst_object.to_string(),
                directory: format!("/{dst_bucket}"),
                attributes: Some(file_attributes(size, now)),
                chunks,
                extended,
                content_size: size,
                disk_size: size,
                ..Entry::default()
            },
        ))
    }

    /// 仅合并元数据（对象锁 legal hold / retention 的 eval_metadata），
    /// 不触碰数据。retention 的强制删除拦截语义留待对象锁阶段。
    pub(crate) async fn put_object_metadata_impl(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let mut entry = self.load_entry(bucket, object).await?;
        if let Some(eval_metadata) = &opts.eval_metadata {
            for (key, value) in eval_metadata {
                entry.extended.insert(
                    format!("s3.{}", key.to_ascii_lowercase()),
                    value.clone().into_bytes(),
                );
            }
        }
        self.filer
            .put_object_meta(ObjectMetaWrite {
                bucket,
                key: object,
                size: entry.content_size,
                chunks: &entry.chunks,
                extended: &entry.extended,
            })
            .await?;
        Ok(object_info_from_entry(bucket, &entry))
    }

    /// 覆盖对象 tag set。
    pub(crate) async fn put_object_tags_impl(
        &self,
        bucket: &str,
        object: &str,
        tags: &str,
        _opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let mut entry = self.load_entry(bucket, object).await?;
        entry
            .extended
            .insert(EXT_TAGGING.to_string(), tags.as_bytes().to_vec());
        self.filer
            .put_object_meta(ObjectMetaWrite {
                bucket,
                key: object,
                size: entry.content_size,
                chunks: &entry.chunks,
                extended: &entry.extended,
            })
            .await?;
        Ok(object_info_from_entry(bucket, &entry))
    }

    /// 读取对象 tag set（URL 编码字符串）；无标签时返回空串。
    pub(crate) async fn get_object_tags_impl(
        &self,
        bucket: &str,
        object: &str,
        _opts: &ObjectOptions,
    ) -> Result<String> {
        let entry = self.load_entry(bucket, object).await?;
        Ok(entry
            .extended
            .get(EXT_TAGGING)
            .map(|value| String::from_utf8_lossy(value).into_owned())
            .unwrap_or_default())
    }

    /// 清空对象 tag set。
    pub(crate) async fn delete_object_tags_impl(
        &self,
        bucket: &str,
        object: &str,
        _opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let mut entry = self.load_entry(bucket, object).await?;
        entry.extended.remove(EXT_TAGGING);
        self.filer
            .put_object_meta(ObjectMetaWrite {
                bucket,
                key: object,
                size: entry.content_size,
                chunks: &entry.chunks,
                extended: &entry.extended,
            })
            .await?;
        Ok(object_info_from_entry(bucket, &entry))
    }

    /// 由 volume id 解析出 volume server 的数据面（TLV）地址。
    async fn volume_address(&self, volume_id: u64) -> Result<String> {
        self.master
            .get_volume_data_endpoint(&VolumeId(volume_id))
            .await
            .ok_or(Error::VolumeNotFound)
    }
}

/// 当前时间的秒级时间戳（Filer 元数据的 mtime 统一使用秒）。
pub(crate) fn unix_secs_now() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp()).unwrap_or(0)
}

/// 普通文件属性。
fn file_attributes(size: u64, mtime: u64) -> FuseAttributes {
    FuseAttributes {
        ino: 0,
        mode: 0o100644,
        nlink: 1,
        uid: 0,
        gid: 0,
        rdev: 0,
        size,
        blksize: 4096,
        blocks: size.div_ceil(4096),
        atime: mtime,
        mtime,
        ctime: mtime,
        crtime: mtime,
        perm: 0o644,
    }
}

/// 由 Filer 条目构造对象元数据。
pub(crate) fn object_info_from_entry(bucket: &str, entry: &Entry) -> ObjectInfo {
    let mut user_defined = HashMap::new();
    let mut etag = None;
    let mut content_type = None;
    let mut user_tags = String::new();

    for (key, value) in &entry.extended {
        let lowered = key.to_ascii_lowercase();
        let text = String::from_utf8_lossy(value).into_owned();
        if lowered == EXT_ETAG {
            etag = Some(text);
        } else if lowered == EXT_CONTENT_TYPE {
            content_type = Some(text);
        } else if let Some(name) = lowered.strip_prefix("s3.") {
            // tagging 与 native 一样保留在 user_defined 里，同时填入 user_tags。
            if name == "x-amz-tagging" {
                user_tags = text.clone();
            }
            user_defined.insert(name.to_string(), text);
        } else {
            user_defined.insert(lowered, text);
        }
    }

    let size = i64::try_from(entry.content_size).unwrap_or(i64::MAX);

    ObjectInfo {
        bucket: bucket.to_string(),
        name: entry.name.clone(),
        size,
        actual_size: size,
        is_dir: entry_is_dir(entry),
        is_latest: true,
        num_versions: 1,
        mod_time: entry
            .attributes
            .as_ref()
            .and_then(|attributes| unix_timestamp_to_datetime(attributes.mtime)),
        etag,
        content_type,
        user_defined: Arc::new(user_defined),
        user_tags: Arc::new(user_tags),
        ..ObjectInfo::default()
    }
}

/// 从对象条目取 ETag（`s3.etag` 扩展键，大小写不敏感）。
fn entry_etag(entry: &Entry) -> Option<String> {
    entry
        .extended
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(EXT_ETAG))
        .map(|(_, value)| String::from_utf8_lossy(value).into_owned())
}

/// 写条件是否应阻止本次写入。语义对齐上游 `set_disk::should_prevent_write`：
/// If-None-Match 命中（含 `*`）阻止；If-Match 不含 `*` 且不匹配阻止。
fn should_prevent_write(
    etag: Option<&str>,
    if_none_match: Option<&str>,
    if_match: Option<&str>,
) -> bool {
    let if_none_match = non_empty_condition(if_none_match);
    let if_match = non_empty_condition(if_match);

    match etag {
        Some(etag) => {
            if if_none_match.is_some_and(|condition| weak_etag_matches(etag, condition)) {
                return true;
            }
            if if_match.is_some_and(|condition| !strong_etag_matches(etag, condition)) {
                return true;
            }
            false
        }
        // 无 ETag 时，任一条件都保守阻止（与上游一致）。
        None => if_none_match.is_some() || if_match.is_some(),
    }
}

fn non_empty_condition(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// 弱比较：忽略 `W/` 前缀与引号；`*` 视为命中。
fn weak_etag_matches(etag: &str, condition: &str) -> bool {
    if condition.trim() == "*" {
        return true;
    }
    canonicalize_etag(strip_weak(etag)) == canonicalize_etag(strip_weak(condition))
}

/// 强比较：`*` 匹配任意现有对象；否则忽略引号后严格相等。
fn strong_etag_matches(etag: &str, condition: &str) -> bool {
    if condition.trim() == "*" {
        return true;
    }
    canonicalize_etag(etag) == canonicalize_etag(condition)
}

fn strip_weak(value: &str) -> &str {
    let trimmed = value.trim();
    trimmed
        .strip_prefix("W/")
        .or_else(|| trimmed.strip_prefix("w/"))
        .unwrap_or(trimmed)
}

/// 去掉包裹的引号（对齐上游 `canonicalize_etag`）。
fn canonicalize_etag(value: &str) -> &str {
    value.trim().trim_matches('"')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn if_none_match_blocks_matching_etag() {
        assert!(should_prevent_write(Some("abc"), Some("\"abc\""), None));
        assert!(should_prevent_write(Some("abc"), Some("*"), None));
        assert!(should_prevent_write(Some("abc"), Some("W/\"abc\""), None));
        assert!(!should_prevent_write(Some("abc"), Some("def"), None));
    }

    #[test]
    fn if_match_blocks_only_non_matching_etag() {
        assert!(!should_prevent_write(Some("abc"), None, Some("\"abc\"")));
        assert!(!should_prevent_write(Some("abc"), None, Some("*")));
        assert!(should_prevent_write(Some("abc"), None, Some("def")));
    }

    #[test]
    fn missing_etag_blocks_any_condition() {
        assert!(should_prevent_write(None, Some("*"), None));
        assert!(should_prevent_write(None, None, Some("*")));
        assert!(!should_prevent_write(None, None, None));
    }

    #[test]
    fn blank_conditions_are_ignored() {
        assert!(!should_prevent_write(Some("abc"), Some("  "), Some("")));
    }
}
