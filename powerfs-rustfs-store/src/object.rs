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

    for (key, value) in &entry.extended {
        let lowered = key.to_ascii_lowercase();
        let text = String::from_utf8_lossy(value).into_owned();
        if lowered == EXT_ETAG {
            etag = Some(text);
        } else if lowered == EXT_CONTENT_TYPE {
            content_type = Some(text);
        } else if let Some(name) = lowered.strip_prefix("s3.") {
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
        ..ObjectInfo::default()
    }
}
