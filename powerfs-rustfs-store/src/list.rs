//! 列举：`ListObjectsV2` 与 `ListObjectVersions`（P1c 单版本）。
//!
//! Filer 把对象按 `name = key` 平铺存放在桶目录下，并在服务端按名字升序分页；
//! 这里在本地完成 prefix 过滤、delimiter 折叠与 `max_keys` 截断。
//! 分页游标是「最后一个已产出项的名字」，由 S3 层负责 base64 包装。

use rustfs_ecstore::api::error::Result;
use rustfs_ecstore::api::object::ObjectInfo;
use rustfs_storage_api::{
    ListObjectVersionsInfo as StorageListObjectVersionsInfo,
    ListObjectsV2Info as StorageListObjectsV2Info,
};

use crate::backend::PowerFsStore;
use crate::bucket::SCAN_PAGE;
use crate::object::object_info_from_entry;

/// S3 默认的 `max-keys`。
const DEFAULT_MAX_KEYS: usize = 1000;

impl PowerFsStore {
    /// `ListObjectsV2`。
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn list_v2(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        _fetch_owner: bool,
        start_after: Option<String>,
        _incl_deleted: bool,
    ) -> Result<StorageListObjectsV2Info<ObjectInfo>> {
        let (listed, _) = self
            .scan_list(
                bucket,
                prefix,
                continuation_token,
                delimiter,
                max_keys,
                start_after,
            )
            .await?;
        Ok(listed)
    }

    /// 列举主体：返回 V2 结果与最后一个已消费 key（供 V1 `NextMarker` 使用）。
    async fn scan_list(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        start_after: Option<String>,
    ) -> Result<(StorageListObjectsV2Info<ObjectInfo>, String)> {
        self.ensure_bucket(bucket).await?;

        // `max-keys=0` 是合法的「不返回任何键」，不能回退到默认 1000。
        let max_keys = match usize::try_from(max_keys) {
            Ok(limit) => limit,
            Err(_) => DEFAULT_MAX_KEYS,
        };

        let after = match continuation_token.as_deref() {
            Some(token) => decode_token(token)?,
            None => start_after.unwrap_or_default(),
        };
        let delimiter = delimiter.filter(|value: &String| !value.is_empty());
        // `max-keys=0` 合法且必须返回空列表、`IsTruncated=false`。
        if max_keys == 0 {
            return Ok((
                StorageListObjectsV2Info {
                    is_truncated: false,
                    continuation_token,
                    next_continuation_token: None,
                    objects: Vec::new(),
                    prefixes: Vec::new(),
                },
                after,
            ));
        }
        // 游标是上一页最后一个**产出项**（对象名或公共前缀）。若它以 delimiter
        // 结尾（公共前缀），续页必须跳过该前缀下的全部 key，否则会重复产出。
        let skip_group = delimiter
            .as_deref()
            .is_some_and(|delimiter| !after.is_empty() && after.ends_with(delimiter));

        let mut objects: Vec<ObjectInfo> = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        let mut produced = 0usize;
        let mut is_truncated = false;
        // Filer 的 last_key 是排他游标，同时充当分页 token。
        let mut cursor = after.clone();
        // 最后一个产出项，用于生成对外的分页游标。
        let mut last_produced = after;

        'scan: loop {
            let page = self
                .filer
                .list_object_metas(bucket, &cursor, SCAN_PAGE)
                .await?;
            let page_len = page.entries.len();

            for entry in page.entries {
                if !entry.name.starts_with(prefix) {
                    cursor = entry.name;
                    continue;
                }
                if entry.name <= last_produced
                    || (skip_group && entry.name.starts_with(last_produced.as_str()))
                {
                    cursor = entry.name;
                    continue;
                }

                // delimiter 折叠：把 `prefix/a/b` 折叠成 `prefix/a/`。
                if let Some(delimiter) = delimiter.as_deref() {
                    let rest = &entry.name[prefix.len()..];
                    if let Some(index) = rest.find(delimiter) {
                        let common_prefix = format!("{}{}{}", prefix, &rest[..index], delimiter);
                        // 同一前缀组的后续 key 只推进游标，不再产出。
                        if prefixes.last() != Some(&common_prefix) {
                            if produced == max_keys {
                                is_truncated = true;
                                break 'scan;
                            }
                            prefixes.push(common_prefix.clone());
                            last_produced = common_prefix;
                            produced += 1;
                        }
                        cursor = entry.name;
                        continue;
                    }
                }

                if produced == max_keys {
                    is_truncated = true;
                    break 'scan;
                }
                let info = object_info_from_entry(bucket, &entry);
                cursor = entry.name.clone();
                last_produced = entry.name;
                objects.push(info);
                produced += 1;
            }

            if !page.has_more || page_len == 0 {
                break;
            }
        }

        let next_continuation_token = is_truncated.then(|| encode_token(&last_produced));

        Ok((
            StorageListObjectsV2Info {
                is_truncated,
                continuation_token,
                next_continuation_token,
                objects,
                prefixes,
            },
            last_produced,
        ))
    }

    /// `ListObjectVersions`（同时承载 V1 `ListObjects`）。P1c 没有版本概念，
    /// 返回与 V2 相同的单版本列表，每个对象 `is_latest = true`。
    pub(crate) async fn list_versions(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        _version_marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
    ) -> Result<StorageListObjectVersionsInfo<ObjectInfo>> {
        let (listed, last_key) = self
            .scan_list(bucket, prefix, None, delimiter, max_keys, marker)
            .await?;

        Ok(StorageListObjectVersionsInfo {
            is_truncated: listed.is_truncated,
            // V1 的 `NextMarker` 是**明文 key**（客户端会原样回传），不能混用
            // 对 V2 不透明的十六进制 token。
            next_marker: listed.is_truncated.then_some(last_key),
            // 单版本模型下没有真实版本 id 游标，但必须回显与 key 游标等价的
            // 不透明 token：S3 客户端在 IsTruncated=true 时会把
            // NextVersionIdMarker 原样带回下一次请求；缺省会得到 None，
            // 直接在客户端参数校验阶段报错（s3-tests 清理逻辑因此级联失败）。
            // 服务端忽略 version_marker，回显不会产生语义副作用。
            next_version_idmarker: listed.next_continuation_token,
            objects: listed.objects,
            prefixes: listed.prefixes,
        })
    }
}

/// 把 key 编码成对客户端不透明的分页 token。
///
/// 注意：S3 层会再对这个值做 base64（`next_continuation_token` 在响应中
/// base64 编码，V1 `NextMarker` 则先 base64 解码回来），因此这里必须放
/// **原始 key**，不能自带 hex/base64，否则客户端拿到的 marker 是乱码。
fn encode_token(key: &str) -> String {
    key.to_string()
}

/// 解码分页 token；空 token 表示从头开始。
fn decode_token(token: &str) -> Result<String> {
    Ok(token.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trip() {
        let token = encode_token("a/b/c");
        assert_eq!(token, "a/b/c");
        assert_eq!(decode_token(&token).unwrap(), "a/b/c");
    }
}
