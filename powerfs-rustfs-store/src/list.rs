//! 列举：`ListObjectsV2` 与 `ListObjectVersions`（P1c 单版本）。
//!
//! Filer 把对象按 `name = key` 平铺存放在桶目录下，并在服务端按名字升序分页；
//! 这里在本地完成 prefix 过滤、delimiter 折叠与 `max_keys` 截断。
//! 分页游标用「最后一个已消费 key 的十六进制编码」，对客户端不透明。

use rustfs_ecstore::api::error::{Error, Result};
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
        self.ensure_bucket(bucket).await?;

        let max_keys = match usize::try_from(max_keys) {
            Ok(limit) if limit > 0 => limit,
            _ => DEFAULT_MAX_KEYS,
        };

        let after = match continuation_token.as_deref() {
            Some(token) => decode_token(token)?,
            None => start_after.unwrap_or_default(),
        };
        let delimiter = delimiter.filter(|value| !value.is_empty());

        let mut objects: Vec<ObjectInfo> = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        let mut produced = 0usize;
        let mut is_truncated = false;
        // Filer 的 last_key 是排他游标，同时充当分页 token。
        let mut cursor = after;

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
                            prefixes.push(common_prefix);
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
                cursor = entry.name;
                objects.push(info);
                produced += 1;
            }

            if !page.has_more || page_len == 0 {
                break;
            }
        }

        let next_continuation_token = is_truncated.then(|| encode_token(&cursor));

        Ok(StorageListObjectsV2Info {
            is_truncated,
            continuation_token,
            next_continuation_token,
            objects,
            prefixes,
        })
    }

    /// `ListObjectVersions`。P1c 没有版本概念，返回与 V2 相同的单版本列表，
    /// 每个对象 `is_latest = true`。
    pub(crate) async fn list_versions(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        _version_marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
    ) -> Result<StorageListObjectVersionsInfo<ObjectInfo>> {
        let listed = self
            .list_v2(
                bucket, prefix, None, delimiter, max_keys, false, marker, false,
            )
            .await?;

        Ok(StorageListObjectVersionsInfo {
            is_truncated: listed.is_truncated,
            next_marker: listed.next_continuation_token.clone(),
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
fn encode_token(key: &str) -> String {
    hex::encode(key.as_bytes())
}

/// 解码分页 token；非法输入返回错误而不是静默从头开始。
fn decode_token(token: &str) -> Result<String> {
    let bytes = hex::decode(token)
        .map_err(|_| Error::other("continuation token 不是合法的十六进制编码"))?;
    String::from_utf8(bytes).map_err(|_| Error::other("continuation token 不是合法的 UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trip() {
        let token = encode_token("a/b/c");
        assert_eq!(decode_token(&token).unwrap(), "a/b/c");
    }

    #[test]
    fn invalid_token_is_rejected() {
        assert!(decode_token("zz").is_err());
    }
}
