//! 分段上传（multipart）：会话与分块元数据存放在内部隐藏桶中。
//!
//! Filer 目前没有 multipart 专用 RPC，因此这里复用对象元数据 RPC：
//! - 会话条目：`<所属桶>/<upload_id>/__session`，记录所属桶/对象/发起时间与
//!   用户元数据；
//! - 分块条目：`<所属桶>/<upload_id>/<part:05>`，记录分块 ETag 与实际数据
//!   chunk（needle 已落 volume）。
//!
//! 完成时按请求顺序把各分块的 chunk 偏移量整体平移后拼接，不复制 needle
//! 数据；提交对象元数据成功后才清理分块与会话，避免「元数据成功、数据丢失」
//! 的窗口。

use std::collections::HashMap;

use md5::{Digest, Md5};
use powerfs_master::filer_proto::powerfs::{Entry, FileChunk};
use rustfs_ecstore::api::error::{Error, Result};
use rustfs_ecstore::api::object::{ObjectInfo, ObjectOptions, PutObjReader};
use rustfs_storage_api::multipart::{
    CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartUploadResult, PartInfo,
};
use tokio::io::AsyncReadExt;

use crate::backend::PowerFsStore;
use crate::bucket::{unix_timestamp_to_datetime, SCAN_PAGE};
use crate::filer::ObjectMetaWrite;
use crate::object::{file_attributes, object_info_from_entry, unix_secs_now, EXT_ETAG};

/// 存放会话与分块元数据的内部隐藏桶；不出现在 `ListBuckets` 中。
pub(crate) const MPU_BUCKET: &str = ".powerfs.multipart";
/// 对象元数据中 multipart 内部键的统一前缀（不向 S3 泄漏）。
pub(crate) const EXT_MPU_PREFIX: &str = "mpu.";
/// 完成后的对象元数据键：各分块大小，逗号分隔。
pub(crate) const EXT_PART_SIZES: &str = "mpu.part-sizes";
/// 完成后的对象元数据键：各分块 ETag，逗号分隔（用于完成重放校验）。
pub(crate) const EXT_PART_ETAGS: &str = "mpu.part-etags";
/// 完成后的对象元数据键：来源 upload id（用于完成重放校验）。
pub(crate) const EXT_UPLOAD_ID: &str = "mpu.upload-id";
/// 分块元数据键：分块 ETag。
pub(crate) const EXT_PART_ETAG: &str = "mpu.etag";
/// 会话元数据键：所属桶。
const EXT_MPU_BUCKET: &str = "mpu.bucket";
/// 会话元数据键：所属对象。
const EXT_MPU_OBJECT: &str = "mpu.object";
/// 会话元数据键：发起时间（秒）。
const EXT_MPU_INITIATED: &str = "mpu.initiated";
/// 会话条目在当前上传目录下的名字。
const SESSION_KEY: &str = "__session";
/// 非末块的最小分块大小（S3 协议固定 5 MiB）。
pub(crate) const MIN_PART_SIZE: i64 = 5 * 1024 * 1024;
/// S3 允许的最大分块号。
const MAX_PART_NUMBER: usize = 10000;
/// 默认 storage class。
const STANDARD_STORAGE_CLASS: &str = "STANDARD";

impl PowerFsStore {
    /// 新建分段上传会话，返回 upload id。
    pub(crate) async fn new_multipart_upload_impl(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartUploadResult> {
        self.ensure_bucket(bucket).await?;
        // upload id 只需在隐藏桶内唯一；UUID v4 足够且免去全局计数。
        let upload_id = uuid::Uuid::new_v4().to_string();

        let mut extended: HashMap<String, Vec<u8>> = HashMap::new();
        extended.insert(EXT_MPU_BUCKET.to_string(), bucket.as_bytes().to_vec());
        extended.insert(EXT_MPU_OBJECT.to_string(), object.as_bytes().to_vec());
        extended.insert(
            EXT_MPU_INITIATED.to_string(),
            unix_secs_now().to_string().into_bytes(),
        );
        for (key, value) in &opts.user_defined {
            let key = key.to_ascii_lowercase();
            // 内部键不允许由客户端注入。
            if key.starts_with(EXT_MPU_PREFIX) {
                continue;
            }
            extended.insert(format!("s3.{key}"), value.clone().into_bytes());
        }

        let key = Self::multipart_session_key(bucket, &upload_id);
        self.filer.ensure_bucket_root(MPU_BUCKET).await?;
        self.filer
            .put_object_meta(ObjectMetaWrite {
                bucket: MPU_BUCKET,
                key: &key,
                size: 0,
                chunks: &[],
                extended: &extended,
            })
            .await?;

        Ok(MultipartUploadResult {
            upload_id,
            // 校验和暂不支持，与 native 在未启用校验和时一致。
            checksum_algo: None,
            checksum_type: None,
        })
    }

    /// 上传单个分块（可覆盖同名分块）。
    pub(crate) async fn put_object_part_impl(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo> {
        if part_id == 0 || part_id > MAX_PART_NUMBER {
            return Err(Error::InvalidPartNumber(part_id));
        }
        // 先确认会话存在，避免为不存在（或已中止）的上传白写 needle。
        self.load_multipart_session(bucket, object, upload_id)
            .await?;

        // P2d 仍按分块全量缓冲：单个分块通常在 5–16 MiB。
        let capacity = usize::try_from(data.size().max(0)).unwrap_or(0);
        let mut body = Vec::with_capacity(capacity);
        data.stream
            .read_to_end(&mut body)
            .await
            .map_err(Error::Io)?;

        let size = body.len();
        let now = unix_secs_now();
        // 与 native 一致：复制/加密路径可指定 ETag，否则取内容 MD5。
        let etag = opts
            .preserve_etag
            .clone()
            .filter(|etag| !etag.is_empty())
            .unwrap_or_else(|| hex::encode(Md5::digest(&body)));

        let key = Self::multipart_part_key(bucket, upload_id, part_id);
        // 覆盖写：先记下旧分块，待新元数据可见后再清理旧 needle。
        let previous = self
            .filer
            .get_object_meta(MPU_BUCKET, &key)
            .await?
            .and_then(|response| response.entry);

        let chunks = self.write_body_chunks(&body, now).await?;
        let mut extended = HashMap::new();
        extended.insert(EXT_PART_ETAG.to_string(), etag.clone().into_bytes());
        // 分块最终会被完成流程按 chunk 顺序拼接，不能被 scrubber 转成 EC：
        // 打上豁免标记，filer 的 EC 待转换队列会跳过该 inode。
        extended.insert(
            powerfs_common::constants::EC_EXEMPT_EXT_KEY.to_string(),
            b"1".to_vec(),
        );
        self.filer
            .put_object_meta(ObjectMetaWrite {
                bucket: MPU_BUCKET,
                key: &key,
                size: size as u64,
                chunks: &chunks,
                extended: &extended,
            })
            .await?;

        if let Some(previous) = previous {
            self.delete_chunks(MPU_BUCKET, &key, &previous.chunks).await;
        }

        Ok(PartInfo {
            part_num: part_id,
            last_mod: unix_timestamp_to_datetime(now),
            size,
            etag: Some(etag),
            actual_size: size as i64,
        })
    }

    /// 查询会话信息。
    pub(crate) async fn get_multipart_info_impl(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        _opts: &ObjectOptions,
    ) -> Result<MultipartInfo> {
        let session = self
            .load_multipart_session(bucket, object, upload_id)
            .await?;
        Ok(MultipartInfo {
            bucket: bucket.to_string(),
            object: object.to_string(),
            upload_id: upload_id.to_string(),
            initiated: extended_str(&session, EXT_MPU_INITIATED)
                .and_then(|value| value.parse::<u64>().ok())
                .and_then(unix_timestamp_to_datetime),
            user_defined: session_user_defined(&session),
        })
    }

    /// 列举已上传分块。
    pub(crate) async fn list_object_parts_impl(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: Option<usize>,
        max_parts: usize,
        _opts: &ObjectOptions,
    ) -> Result<ListPartsInfo> {
        let session = self
            .load_multipart_session(bucket, object, upload_id)
            .await?;
        let user_defined = session_user_defined(&session);
        let storage_class = user_defined
            .get("x-amz-storage-class")
            .cloned()
            .unwrap_or_else(|| STANDARD_STORAGE_CLASS.to_string());

        let marker = part_number_marker.unwrap_or(0);
        let mut info = ListPartsInfo {
            bucket: bucket.to_string(),
            object: object.to_string(),
            upload_id: upload_id.to_string(),
            storage_class,
            part_number_marker: marker,
            max_parts,
            user_defined,
            ..ListPartsInfo::default()
        };
        if max_parts == 0 {
            return Ok(info);
        }

        let remaining: Vec<(usize, Entry)> = self
            .list_multipart_parts(bucket, upload_id)
            .await?
            .into_iter()
            .filter(|(part_num, _)| *part_num > marker)
            .collect();
        let take = remaining.len().min(max_parts);
        for (part_num, entry) in remaining.iter().take(take) {
            let size = entry.content_size;
            info.parts.push(PartInfo {
                part_num: *part_num,
                last_mod: entry
                    .attributes
                    .as_ref()
                    .and_then(|attributes| unix_timestamp_to_datetime(attributes.mtime)),
                size: usize::try_from(size).unwrap_or(usize::MAX),
                etag: part_etag(entry),
                actual_size: i64::try_from(size).unwrap_or(i64::MAX),
            });
        }
        if remaining.len() > take {
            info.is_truncated = true;
            info.next_part_number_marker = info.parts.last().map(|part| part.part_num);
        }
        Ok(info)
    }

    /// 中止分段上传：先删分块元数据与 needle，再删会话。
    pub(crate) async fn abort_multipart_upload_impl(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        _opts: &ObjectOptions,
    ) -> Result<()> {
        self.load_multipart_session(bucket, object, upload_id)
            .await?;

        for (_, entry) in self.list_multipart_parts(bucket, upload_id).await? {
            self.filer
                .delete_object_meta(MPU_BUCKET, &entry.name)
                .await?;
            self.delete_chunks(MPU_BUCKET, &entry.name, &entry.chunks)
                .await;
        }
        let session_key = Self::multipart_session_key(bucket, upload_id);
        self.filer
            .delete_object_meta(MPU_BUCKET, &session_key)
            .await
    }

    /// 完成分段上传：校验分块后零拷贝拼接，单次提交对象元数据。
    pub(crate) async fn complete_multipart_upload_impl(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let session = match self.load_multipart_session(bucket, object, upload_id).await {
            Ok(session) => session,
            // 会话可能在上一次 Complete 成功后已销毁；按已提交对象重放。
            Err(Error::InvalidUploadID(detail_bucket, detail_object, detail_id)) => {
                if let Some(info) = self
                    .replay_completed_upload(bucket, object, upload_id, &uploaded_parts)
                    .await?
                {
                    return Ok(info);
                }
                return Err(Error::InvalidUploadID(
                    detail_bucket,
                    detail_object,
                    detail_id,
                ));
            }
            Err(error) => return Err(error),
        };

        if uploaded_parts.is_empty() {
            return Err(Error::InvalidPart(0, String::new(), String::new()));
        }

        let stored = self.list_multipart_parts(bucket, upload_id).await?;
        let mut chunks: Vec<FileChunk> = Vec::new();
        let mut part_sizes: Vec<u64> = Vec::with_capacity(uploaded_parts.len());
        let mut part_etags: Vec<String> = Vec::with_capacity(uploaded_parts.len());
        // 已拼接的数据长度，即当前分块 chunk 偏移量的平移量。
        let mut offset = 0u64;

        for (index, requested) in uploaded_parts.iter().enumerate() {
            let entry = stored
                .iter()
                .find(|(part_num, _)| *part_num == requested.part_num)
                .map(|(_, entry)| entry)
                .ok_or_else(|| {
                    Error::InvalidPart(
                        requested.part_num,
                        String::new(),
                        requested.etag.clone().unwrap_or_default(),
                    )
                })?;

            let stored_etag = part_etag(entry).unwrap_or_default();
            let client_etag = requested
                .etag
                .as_deref()
                .unwrap_or_default()
                .trim_matches('"');
            if client_etag != stored_etag.trim_matches('"') {
                return Err(Error::InvalidPart(
                    requested.part_num,
                    stored_etag,
                    requested.etag.clone().unwrap_or_default(),
                ));
            }

            // 分块必须保持平铺 chunk 布局：EC 分片是 group-major 且校验分片
            // offset 与数据分片重叠，直接平移偏移拼接会写出错乱对象。
            // 正常链路已通过 `EC_EXEMPT_EXT_KEY` 阻止转换，这里兜底拒绝历史脏数据。
            if entry.ec_layout.is_some() {
                return Err(Error::other(format!(
                    "multipart part {}/{:05} was EC-converted and cannot be concatenated",
                    upload_id, requested.part_num
                )));
            }

            // 末块允许小于 5 MiB，其余分块必须满足最小大小。
            let size = entry.content_size;
            if index + 1 < uploaded_parts.len() {
                let size = i64::try_from(size).unwrap_or(i64::MAX);
                if size < MIN_PART_SIZE {
                    return Err(Error::EntityTooSmall(
                        requested.part_num,
                        size,
                        MIN_PART_SIZE,
                    ));
                }
            }

            for chunk in &entry.chunks {
                chunks.push(FileChunk {
                    offset: offset.saturating_add(chunk.offset),
                    ..chunk.clone()
                });
            }
            offset = offset.saturating_add(size);
            part_sizes.push(size);
            part_etags.push(stored_etag);
        }

        let final_etag = match opts.preserve_etag.as_ref().filter(|etag| !etag.is_empty()) {
            Some(etag) => etag.clone(),
            None => complete_multipart_etag(&part_etags),
        };

        let mut extended: HashMap<String, Vec<u8>> = session
            .extended
            .iter()
            .filter(|(key, _)| key.to_ascii_lowercase().starts_with("s3."))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        extended.insert(EXT_ETAG.to_string(), final_etag.clone().into_bytes());
        extended.insert(
            EXT_PART_SIZES.to_string(),
            join_numbers(&part_sizes).into_bytes(),
        );
        extended.insert(
            EXT_PART_ETAGS.to_string(),
            part_etags.join(",").into_bytes(),
        );
        extended.insert(EXT_UPLOAD_ID.to_string(), upload_id.as_bytes().to_vec());
        if let Some(eval_metadata) = &opts.eval_metadata {
            for (key, value) in eval_metadata {
                extended.insert(
                    format!("s3.{}", key.to_ascii_lowercase()),
                    value.clone().into_bytes(),
                );
            }
        }

        let now = unix_secs_now();
        self.filer
            .put_object_meta(ObjectMetaWrite {
                bucket,
                key: object,
                size: offset,
                chunks: &chunks,
                extended: &extended,
            })
            .await?;

        // 对象已提交，只回收分块**元数据**与会话。分块 needle 必须保留：
        // 完成后的对象 chunks 直接引用它们（零拷贝），删掉会立刻损坏刚提交
        // 的对象；这些 needle 的内存回收留给后续的引用计数/GC 阶段。
        for (_, entry) in stored {
            self.filer
                .delete_object_meta(MPU_BUCKET, &entry.name)
                .await?;
        }
        let session_key = Self::multipart_session_key(bucket, upload_id);
        self.filer
            .delete_object_meta(MPU_BUCKET, &session_key)
            .await?;

        Ok(object_info_from_entry(
            bucket,
            &Entry {
                name: object.to_string(),
                directory: format!("/{bucket}"),
                attributes: Some(file_attributes(offset, now)),
                chunks,
                extended,
                content_size: offset,
                disk_size: offset,
                ..Entry::default()
            },
        ))
    }

    /// 列举桶内进行中的分段上传。
    pub(crate) async fn list_multipart_uploads_impl(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        delimiter: Option<String>,
        max_uploads: usize,
    ) -> Result<ListMultipartsInfo> {
        let mut result = ListMultipartsInfo {
            key_marker,
            upload_id_marker,
            max_uploads,
            prefix: prefix.to_string(),
            delimiter: delimiter.clone(),
            ..ListMultipartsInfo::default()
        };
        // 隐藏桶还没创建时说明从未发起过分段上传。
        if self.filer.get_bucket_root(MPU_BUCKET).await?.is_none() {
            return Ok(result);
        }

        let key_marker = result.key_marker.clone();
        let upload_id_marker = result.upload_id_marker.clone();
        let mut uploads: Vec<MultipartInfo> = Vec::new();
        for session in self.list_multipart_sessions(bucket).await? {
            let Some(object) = extended_str(&session, EXT_MPU_OBJECT) else {
                continue;
            };
            if !object.starts_with(prefix) {
                continue;
            }
            let Some(upload_id) = session_upload_id(&session.name) else {
                continue;
            };
            if before_marker(
                &object,
                &upload_id,
                key_marker.as_deref(),
                upload_id_marker.as_deref(),
            ) {
                continue;
            }
            uploads.push(MultipartInfo {
                bucket: bucket.to_string(),
                object,
                upload_id,
                initiated: extended_str(&session, EXT_MPU_INITIATED)
                    .and_then(|value| value.parse::<u64>().ok())
                    .and_then(unix_timestamp_to_datetime),
                user_defined: session_user_defined(&session),
            });
        }
        uploads.sort_by(|left, right| {
            left.object
                .cmp(&right.object)
                .then_with(|| left.upload_id.cmp(&right.upload_id))
        });

        if max_uploads == 0 {
            return Ok(result);
        }

        let mut collected = 0usize;
        let mut truncated = false;
        for upload in uploads {
            if collected >= max_uploads {
                truncated = true;
                break;
            }
            match common_prefix(&upload.object, prefix, delimiter.as_deref()) {
                Some(common) => {
                    // 对象已按名字排序，同一公共前缀必然连续出现，只需去重。
                    if result.common_prefixes.last() != Some(&common) {
                        result.common_prefixes.push(common);
                        collected += 1;
                    }
                }
                None => {
                    result.uploads.push(upload);
                    collected += 1;
                }
            }
        }
        if truncated {
            result.is_truncated = true;
            // 分页游标指向最后一条已返回记录；`common_prefixes` 已整体返回，
            // 因此以最后返回的上传为准（没有上传时回到 marker 之后的第一条）。
            if let Some(last) = result.uploads.last() {
                result.next_key_marker = Some(last.object.clone());
                result.next_upload_id_marker = Some(last.upload_id.clone());
            } else if let Some(common) = result.common_prefixes.last() {
                result.next_key_marker = Some(common.clone());
            }
        }
        Ok(result)
    }

    // --- 内部辅助 -------------------------------------------------------

    /// 会话条目键：`<所属桶>/<upload_id>/__session`。
    fn multipart_session_key(bucket: &str, upload_id: &str) -> String {
        format!("{bucket}/{upload_id}/{SESSION_KEY}")
    }

    /// 分块条目键：`<所属桶>/<upload_id>/<part:05>`。
    fn multipart_part_key(bucket: &str, upload_id: &str, part_id: usize) -> String {
        format!("{bucket}/{upload_id}/{part_id:05}")
    }

    /// 当前上传目录的前缀：`<所属桶>/<upload_id>/`。
    fn multipart_prefix(bucket: &str, upload_id: &str) -> String {
        format!("{bucket}/{upload_id}/")
    }

    /// 读取会话并校验归属；不存在或归属不符一律返回 `InvalidUploadID`。
    async fn load_multipart_session(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
    ) -> Result<Entry> {
        let key = Self::multipart_session_key(bucket, upload_id);
        let entry = self
            .filer
            .get_object_meta(MPU_BUCKET, &key)
            .await?
            .and_then(|response| response.entry);
        let Some(entry) = entry else {
            return Err(Error::InvalidUploadID(
                bucket.to_string(),
                object.to_string(),
                upload_id.to_string(),
            ));
        };
        let owner_matches = extended_str(&entry, EXT_MPU_BUCKET).as_deref() == Some(bucket)
            && extended_str(&entry, EXT_MPU_OBJECT).as_deref() == Some(object);
        if !owner_matches {
            return Err(Error::InvalidUploadID(
                bucket.to_string(),
                object.to_string(),
                upload_id.to_string(),
            ));
        }
        Ok(entry)
    }

    /// 列举当前上传的全部已上传分块（按分块号升序）。
    async fn list_multipart_parts(
        &self,
        bucket: &str,
        upload_id: &str,
    ) -> Result<Vec<(usize, Entry)>> {
        let prefix = Self::multipart_prefix(bucket, upload_id);
        let mut parts = Vec::new();
        let mut last_key = prefix.clone();

        'scan: loop {
            let page = self
                .filer
                .list_object_metas(MPU_BUCKET, &last_key, SCAN_PAGE)
                .await?;
            if page.entries.is_empty() {
                break;
            }
            for entry in &page.entries {
                let name = entry.name.as_str();
                if !name.starts_with(&prefix) {
                    // 键有序：一旦越过本上传目录就不会再出现分块。
                    break 'scan;
                }
                let suffix = &name[prefix.len()..];
                if suffix == SESSION_KEY {
                    continue;
                }
                if let Ok(part_num) = suffix.parse::<usize>() {
                    parts.push((part_num, entry.clone()));
                }
            }
            if !page.has_more {
                break;
            }
            last_key = page
                .entries
                .last()
                .map(|entry| entry.name.clone())
                .unwrap_or(last_key);
        }

        parts.sort_by_key(|(part_num, _)| *part_num);
        Ok(parts)
    }

    /// 列举某桶的全部会话条目（扫描 `<桶>/` 前缀，只保留 `__session`）。
    async fn list_multipart_sessions(&self, bucket: &str) -> Result<Vec<Entry>> {
        let prefix = format!("{bucket}/");
        let session_suffix = format!("/{SESSION_KEY}");
        let mut sessions = Vec::new();
        let mut last_key = prefix.clone();

        'scan: loop {
            let page = self
                .filer
                .list_object_metas(MPU_BUCKET, &last_key, SCAN_PAGE)
                .await?;
            if page.entries.is_empty() {
                break;
            }
            for entry in &page.entries {
                let name = entry.name.as_str();
                if !name.starts_with(&prefix) {
                    break 'scan;
                }
                if name.ends_with(&session_suffix) {
                    sessions.push(entry.clone());
                }
            }
            if !page.has_more {
                break;
            }
            last_key = page
                .entries
                .last()
                .map(|entry| entry.name.clone())
                .unwrap_or(last_key);
        }
        Ok(sessions)
    }

    /// 会话/对象已销毁时，用已提交对象的 `mpu.upload-id` 与分块 ETag 证明
    /// 本次 Complete 是丢失响应的重试，返回既有对象。
    async fn replay_completed_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: &[CompletePart],
    ) -> Result<Option<ObjectInfo>> {
        let Some(entry) = self
            .filer
            .get_object_meta(bucket, object)
            .await?
            .and_then(|response| response.entry)
        else {
            return Ok(None);
        };
        if extended_str(&entry, EXT_UPLOAD_ID).as_deref() != Some(upload_id) {
            return Ok(None);
        }
        let Some(stored_etags) = extended_str(&entry, EXT_PART_ETAGS) else {
            return Ok(None);
        };
        let stored_etags: Vec<&str> = stored_etags.split(',').collect();
        if stored_etags.len() != uploaded_parts.len() {
            return Ok(None);
        }
        for (index, requested) in uploaded_parts.iter().enumerate() {
            let client_etag = requested
                .etag
                .as_deref()
                .unwrap_or_default()
                .trim_matches('"');
            if requested.part_num != index + 1
                || client_etag != stored_etags[index].trim_matches('"')
            {
                return Ok(None);
            }
        }

        let mut info = object_info_from_entry(bucket, &entry);
        info.multipart_completion_replayed = true;
        Ok(Some(info))
    }
}

/// 读取扩展键（大小写不敏感）为字符串。
fn extended_str(entry: &Entry, key: &str) -> Option<String> {
    entry
        .extended
        .iter()
        .find(|(stored, _)| stored.eq_ignore_ascii_case(key))
        .map(|(_, value)| String::from_utf8_lossy(value).into_owned())
}

/// 会话中保存的 `s3.*` 用户元数据还原为 S3 键（去掉前缀）。
fn session_user_defined(entry: &Entry) -> HashMap<String, String> {
    let mut user_defined = HashMap::new();
    for (key, value) in &entry.extended {
        let lowered = key.to_ascii_lowercase();
        if let Some(name) = lowered.strip_prefix("s3.") {
            user_defined.insert(
                name.to_string(),
                String::from_utf8_lossy(value).into_owned(),
            );
        }
    }
    user_defined
}

/// 分块 ETag（`mpu.etag`）。
fn part_etag(entry: &Entry) -> Option<String> {
    extended_str(entry, EXT_PART_ETAG)
}

/// 从会话键 `<桶>/<upload_id>/__session` 中取出 upload id。
fn session_upload_id(name: &str) -> Option<String> {
    let mut segments = name.split('/');
    segments.next()?;
    let upload_id = segments.next()?;
    (segments.next() == Some(SESSION_KEY)).then(|| upload_id.to_string())
}

/// 分页游标判定：`(object, upload_id)` 是否已越过 marker。
fn before_marker(
    object: &str,
    upload_id: &str,
    key_marker: Option<&str>,
    upload_id_marker: Option<&str>,
) -> bool {
    let Some(key_marker) = key_marker else {
        return false;
    };
    match object.cmp(key_marker) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Greater => false,
        std::cmp::Ordering::Equal => match upload_id_marker {
            // 未给 upload id marker 时，该 object 的上传都已返回。
            None => true,
            Some(marker) => upload_id <= marker,
        },
    }
}

/// 按 delimiter 归并出公共前缀（返回含 delimiter 的完整前缀）。
fn common_prefix(object: &str, prefix: &str, delimiter: Option<&str>) -> Option<String> {
    let delimiter = delimiter.filter(|delimiter| !delimiter.is_empty())?;
    let rest = object.strip_prefix(prefix)?;
    let index = rest.find(delimiter)?;
    Some(format!("{prefix}{}", &rest[..index + delimiter.len()]))
}

/// 把分块大小列表编码为逗号分隔字符串。
fn join_numbers(values: &[u64]) -> String {
    values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// S3 分段对象 ETag：`md5(concat(hex_decode(各分块 ETag)))-<分块数>`。
fn complete_multipart_etag(part_etags: &[String]) -> String {
    let mut concatenated = Vec::new();
    for etag in part_etags {
        match hex::decode(etag.trim_matches('"')) {
            Ok(bytes) => concatenated.extend_from_slice(&bytes),
            // 非十六进制 ETag 按原始字节参与（与 native 一致）。
            Err(_) => concatenated.extend_from_slice(etag.trim_matches('"').as_bytes()),
        }
    }
    format!(
        "{}-{}",
        hex::encode(Md5::digest(&concatenated)),
        part_etags.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_etag_matches_native_formula() {
        // 两个空内容的 MD5，拼接后再取 MD5（与 native get_complete_multipart_md5 一致）。
        let etags = vec![
            "d41d8cd98f00b204e9800998ecf8427e".to_string(),
            "d41d8cd98f00b204e9800998ecf8427e".to_string(),
        ];
        let etag = complete_multipart_etag(&etags);
        assert!(etag.ends_with("-2"));
        assert_eq!(etag.len(), 32 + 2);

        let single = complete_multipart_etag(&["\"abc\"".to_string()]);
        // 非十六进制 ETag 按原始字节参与。
        assert!(single.ends_with("-1"));
    }

    #[test]
    fn session_key_parsing_round_trip() {
        let key = PowerFsStore::multipart_session_key("bucket", "upload");
        assert_eq!(key, "bucket/upload/__session");
        assert_eq!(session_upload_id(&key).as_deref(), Some("upload"));
        assert_eq!(session_upload_id("bucket/upload/00001"), None);
    }

    #[test]
    fn part_key_is_zero_padded_for_lexicographic_order() {
        assert_eq!(PowerFsStore::multipart_part_key("b", "u", 9), "b/u/00009");
        assert!(
            PowerFsStore::multipart_part_key("b", "u", 9)
                < PowerFsStore::multipart_part_key("b", "u", 10)
        );
    }

    #[test]
    fn markers_and_prefixes() {
        assert!(before_marker("a", "u", Some("b"), None));
        // 只给 key marker 时，该 key 的上传都已返回。
        assert!(before_marker("b", "u", Some("b"), None));
        assert!(before_marker("b", "u", Some("b"), Some("u")));
        assert!(!before_marker("b", "v", Some("b"), Some("u")));
        assert!(!before_marker("c", "u", Some("b"), Some("u")));
        assert_eq!(
            common_prefix("dir/a.txt", "", Some("/")),
            Some("dir/".to_string())
        );
        assert_eq!(common_prefix("a.txt", "", Some("/")), None);
        assert_eq!(common_prefix("a.txt", "", None), None);
    }

    #[test]
    fn object_helpers_stay_available() {
        // 会话条目没有 attributes，不应被识别为目录。
        assert!(!crate::bucket::entry_is_dir(&Entry::default()));
        assert!(crate::object::entry_etag(&Entry::default()).is_none());
    }
}
