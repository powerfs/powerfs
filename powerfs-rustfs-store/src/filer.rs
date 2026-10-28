//! Filer 元数据客户端。
//!
//! master 的 `get_entry/create_entry/delete_entry/list_entries` 已全部返回
//! `UNIMPLEMENTED`，元数据唯一可用入口是 Filer 的 `FilerMetaService`。
//! 这里只封装 P1c 需要的 8 个 S3 元数据 RPC，并负责端点轮询与一次重试。

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

use powerfs_master::filer_proto::powerfs::filer_meta_service_client::FilerMetaServiceClient;
use powerfs_master::filer_proto::powerfs::{
    BucketRootInfo, DeleteBucketRootRequest, DeleteBucketRootResponse, DeleteObjectMetaRequest,
    DeleteObjectMetaResponse, EnsureBucketRootRequest, EnsureBucketRootResponse,
    FileChunk as ProtoFileChunk, GetBucketRootRequest, GetBucketRootResponse, GetObjectMetaRequest,
    GetObjectMetaResponse, ListBucketRootsRequest, ListObjectMetasRequest, ListObjectMetasResponse,
    PutObjectMetaRequest, PutObjectMetaResponse,
};
use rustfs_ecstore::api::error::{Error, Result};
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::config::StoreConfig;

/// 一次 `put_object_meta` 请求所需的字段。
pub(crate) struct ObjectMetaWrite<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub size: u64,
    pub chunks: &'a [ProtoFileChunk],
    pub extended: &'a std::collections::HashMap<String, Vec<u8>>,
}

/// 多端点 Filer 元数据客户端。
pub(crate) struct FilerClient {
    channels: Vec<Channel>,
    next: AtomicUsize,
    client_id: String,
    /// bucket → 最近一次写成功的端点索引。写成功即说明该端点持有该桶所在
    /// Raft shard 的 leader 身份；读请求优先命中它，避免落在 apply 滞后的
    /// follower 上返回旧版本条目。
    leader_hints: std::sync::RwLock<std::collections::HashMap<String, usize>>,
}

impl FilerClient {
    /// 按配置构造；端点格式错误时直接失败，不惰性报错。
    pub(crate) fn new(config: &StoreConfig) -> Result<Self> {
        if config.filer_endpoints.is_empty() {
            return Err(Error::other("Filer 端点列表为空"));
        }
        let mut channels = Vec::with_capacity(config.filer_endpoints.len());
        for endpoint in &config.filer_endpoints {
            let url = normalize_endpoint(endpoint);
            let channel = Endpoint::from_shared(url)
                .map_err(|error| Error::other(format!("无效的 Filer 端点 {endpoint}: {error}")))?
                .timeout(config.rpc_timeout)
                .connect_lazy();
            channels.push(channel);
        }
        Ok(Self {
            channels,
            next: AtomicUsize::new(0),
            client_id: config.client_id.clone(),
            leader_hints: std::sync::RwLock::new(std::collections::HashMap::new()),
        })
    }

    /// 确保 bucket 根目录存在，返回 `(inode, 是否新建)`。
    pub(crate) async fn ensure_bucket_root(&self, bucket: &str) -> Result<(u64, bool)> {
        let request = EnsureBucketRootRequest {
            bucket: bucket.to_string(),
        };
        let response = self
            .call_retry(
                Some(bucket),
                true,
                move |mut client| {
                    let request = request.clone();
                    async move { client.ensure_bucket_root(request).await }
                },
                |response: &tonic::Response<EnsureBucketRootResponse>| {
                    mutation_outcome(response.get_ref())
                },
                false,
            )
            .await?
            .into_inner();

        if response.success {
            Ok((response.inode, response.created))
        } else {
            Err(Error::other(response.error))
        }
    }

    /// 查询 bucket 根目录；不存在返回 `None`。
    pub(crate) async fn get_bucket_root(&self, bucket: &str) -> Result<Option<BucketRootInfo>> {
        let request = GetBucketRootRequest {
            bucket: bucket.to_string(),
        };
        let response = self
            .call_retry(
                Some(bucket),
                false,
                move |mut client| {
                    let request = request.clone();
                    async move { client.get_bucket_root(request).await }
                },
                // 桶刚在 leader 上创建时，follower 可能还没 apply；换端点再查。
                |response: &tonic::Response<GetBucketRootResponse>| {
                    read_outcome(response.get_ref(), |response| !response.found)
                },
                true,
            )
            .await?
            .into_inner();

        if !response.error.is_empty() {
            return Err(Error::other(response.error));
        }
        Ok(response.found.then(|| BucketRootInfo {
            bucket: bucket.to_string(),
            inode: response.inode,
            mtime: response.mtime,
        }))
    }

    /// 列出所有桶。
    pub(crate) async fn list_bucket_roots(&self) -> Result<Vec<BucketRootInfo>> {
        let response = self
            .call_retry(
                None,
                false,
                |mut client| async move { client.list_bucket_roots(ListBucketRootsRequest {}).await },
                |_| Outcome::Accept,
                true,
            )
            .await?
            .into_inner();

        if !response.error.is_empty() {
            return Err(Error::other(response.error));
        }
        Ok(response.buckets)
    }

    /// 删除桶根目录；`force` 时由服务端先清空扁平子项。
    pub(crate) async fn delete_bucket_root(&self, bucket: &str, force: bool) -> Result<()> {
        let request = DeleteBucketRootRequest {
            bucket: bucket.to_string(),
            force,
        };
        let response = self
            .call_retry(
                Some(bucket),
                true,
                move |mut client| {
                    let request = request.clone();
                    async move { client.delete_bucket_root(request).await }
                },
                |response: &tonic::Response<DeleteBucketRootResponse>| {
                    mutation_outcome(response.get_ref())
                },
                false,
            )
            .await?
            .into_inner();

        if response.success {
            Ok(())
        } else {
            Err(Error::other(response.error))
        }
    }

    /// 提交对象元数据（新建或原地覆盖），返回 inode。
    pub(crate) async fn put_object_meta(&self, write: ObjectMetaWrite<'_>) -> Result<u64> {
        let request = PutObjectMetaRequest {
            bucket: write.bucket.to_string(),
            key: write.key.to_string(),
            size: write.size,
            chunks: write.chunks.to_vec(),
            extended: write.extended.clone(),
            client_id: self.client_id.clone(),
        };
        // Filer 默认异步持久化：`put_object_meta` 在 Raft propose 后即返回，
        // 命令若在 leader 切换等窗口内未提交，本地状态会永久停留在
        // 「只创建、未更新」（size=0、无 extended）。因此写入后必须确认条目
        // 本进程可见；不可见时重新下发（该 RPC 幂等）而不是静默返回成功。
        let mut last_error = None;
        for attempt in 0..META_WRITE_ATTEMPTS {
            match self
                .call_retry(
                    Some(write.bucket),
                    true,
                    {
                        let request = request.clone();
                        move |mut client| {
                            let request = request.clone();
                            async move { client.put_object_meta(request).await }
                        }
                    },
                    |response: &tonic::Response<PutObjectMetaResponse>| {
                        mutation_outcome(response.get_ref())
                    },
                    false,
                )
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    if !response.success {
                        last_error = Some(Error::other(response.error));
                        continue;
                    }
                    match self.await_object_meta_visible(&write).await {
                        Ok(()) => return Ok(response.inode),
                        Err(error) => {
                            last_error = Some(error);
                            // 提示可能指向已被降级的 leader，清掉以重新发现。
                            self.forget_leader(write.bucket);
                            tracing::warn!(
                                bucket = write.bucket,
                                key = write.key,
                                attempt,
                                "对象元数据写入后不可见，重新下发"
                            );
                        }
                    }
                }
                Err(error) => {
                    last_error = Some(error);
                    self.forget_leader(write.bucket);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| Error::other("put_object_meta 未执行")))
    }

    /// 轮询直到写入的元数据在本进程可见。`get_object_meta` 读的是 shard
    /// store 的内存状态，只有 Raft apply 后才更新，因此可见即已提交。
    async fn await_object_meta_visible(&self, write: &ObjectMetaWrite<'_>) -> Result<()> {
        let deadline = std::time::Instant::now() + META_VISIBILITY_WAIT;
        loop {
            if let Ok(Some(response)) = self.get_object_meta(write.bucket, write.key).await {
                if meta_matches(&response, write) {
                    return Ok(());
                }
            }
            if std::time::Instant::now() >= deadline {
                return Err(Error::other(format!(
                    "对象元数据在 {:?} 内仍不可见：bucket={} key={}",
                    META_VISIBILITY_WAIT, write.bucket, write.key
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    /// 读取对象元数据；不存在返回 `None`。
    pub(crate) async fn get_object_meta(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<GetObjectMetaResponse>> {
        let request = GetObjectMetaRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
        };
        let response = self
            .call_retry(
                Some(bucket),
                false,
                move |mut client| {
                    let request = request.clone();
                    async move { client.get_object_meta(request).await }
                },
                // 刚写入的对象在 follower 上可能尚未可见；换端点再读。
                |response: &tonic::Response<GetObjectMetaResponse>| {
                    read_outcome(response.get_ref(), |response| !response.found)
                },
                true,
            )
            .await?
            .into_inner();

        if !response.error.is_empty() {
            return Err(Error::other(response.error));
        }
        Ok(response.found.then_some(response))
    }

    /// 删除对象元数据。
    ///
    /// 与 `put_object_meta` 同理：删除在 propose 后即返回，若立即删桶，服务端
    /// 的空桶检查会赶在 apply 之前看到子项（`BucketNotEmpty`）。因此删除后必须
    /// 确认条目在所有端点不可见；重发时条目可能已被前一次删除（apply 延迟），
    /// 服务端返回 `object not found` 属幂等语义，视为删除已生效。
    pub(crate) async fn delete_object_meta(&self, bucket: &str, key: &str) -> Result<()> {
        let request = DeleteObjectMetaRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
        };
        let mut last_error = None;
        for attempt in 0..META_WRITE_ATTEMPTS {
            match self
                .call_retry(
                    Some(bucket),
                    true,
                    {
                        let request = request.clone();
                        move |mut client| {
                            let request = request.clone();
                            async move { client.delete_object_meta(request).await }
                        }
                    },
                    |response: &tonic::Response<DeleteObjectMetaResponse>| {
                        mutation_outcome(response.get_ref())
                    },
                    false,
                )
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    if !response.success && !response.error.contains("object not found") {
                        last_error = Some(Error::other(response.error));
                        continue;
                    }
                    match self.await_object_meta_absent(bucket, key).await {
                        Ok(()) => return Ok(()),
                        Err(error) => {
                            last_error = Some(error);
                            self.forget_leader(bucket);
                            tracing::warn!(
                                bucket,
                                key,
                                attempt,
                                "对象元数据删除后仍可见，重新下发"
                            );
                        }
                    }
                }
                Err(error) => {
                    last_error = Some(error);
                    self.forget_leader(bucket);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| Error::other("delete_object_meta 未执行")))
    }

    /// 轮询直到条目在所有端点均不可见。`get_object_meta` 对「未找到」会换端点
    /// 重试，任一节点仍可见即返回 `Some`，因此只有全端点 apply 后才返回 `None`。
    async fn await_object_meta_absent(&self, bucket: &str, key: &str) -> Result<()> {
        let deadline = std::time::Instant::now() + META_VISIBILITY_WAIT;
        loop {
            if let Ok(None) = self.get_object_meta(bucket, key).await {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(Error::other(format!(
                    "对象元数据删除后 {:?} 内仍可见：bucket={} key={}",
                    META_VISIBILITY_WAIT, bucket, key
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    /// 列举桶内对象元数据的一页。
    pub(crate) async fn list_object_metas(
        &self,
        bucket: &str,
        last_key: &str,
        limit: u64,
    ) -> Result<ListObjectMetasResponse> {
        let request = ListObjectMetasRequest {
            bucket: bucket.to_string(),
            limit,
            last_key: last_key.to_string(),
        };
        let response = self
            .call_retry(
                Some(bucket),
                false,
                move |mut client| {
                    let request = request.clone();
                    async move { client.list_object_metas(request).await }
                },
                |_| Outcome::Accept,
                true,
            )
            .await?
            .into_inner();

        if !response.error.is_empty() {
            return Err(Error::other(response.error));
        }
        Ok(response)
    }

    /// 执行一次 RPC：优先命中该桶的 leader 提示端点，随后环形轮询其余端点。
    ///
    /// Filer 明确不做服务端转发（`propose`/`propose_ff` 在非 leader 上返回
    /// `not_leader`），因此写类 RPC 必须换端点重试直到命中 leader。
    /// 读类 RPC 在 follower 上可能返回**旧版本或 pre-apply** 元数据
    /// （`found=true` 不代表数据新鲜），因此读请求同样优先走 leader 提示；
    /// 「未找到」也换端点重试。`record_leader` 为真时把成功响应的端点记为
    /// leader 提示——只有写成功才能确定 leader 身份，读请求不写提示。
    ///
    /// `classify` 决定每个响应的处置：接受、换端点重试、或立即失败。
    /// 写类响应即使是业务失败也必须落入「重试」或「失败」，绝不能当作成功，
    /// 否则失败会被静默吞掉、元数据丢失。
    async fn call_retry<F, Fut, T, P>(
        &self,
        bucket: Option<&str>,
        record_leader: bool,
        f: F,
        classify: P,
        accept_last_value: bool,
    ) -> Result<T>
    where
        F: Fn(FilerMetaServiceClient<Channel>) -> Fut + Send + Sync,
        Fut: Future<Output = std::result::Result<T, Status>> + Send,
        T: Send,
        P: Fn(&T) -> Outcome,
    {
        let channels = self.channels.len().max(1);
        let fallback = self.next.fetch_add(1, Ordering::Relaxed) % channels;
        let hint = bucket.and_then(|bucket| {
            self.leader_hints
                .read()
                .ok()
                .and_then(|hints| hints.get(bucket).copied())
                .filter(|index| *index < channels)
        });
        let mut last_retry = None;
        let mut last_status = None;

        for (attempt, index) in attempt_order(hint, fallback, channels).enumerate() {
            let client = FilerMetaServiceClient::new(self.channels[index].clone());
            let has_next = attempt + 1 < channels;
            match f(client).await {
                Ok(value) => match classify(&value) {
                    Outcome::Accept => {
                        if record_leader {
                            self.remember_leader(bucket, index);
                        }
                        return Ok(value);
                    }
                    Outcome::Retry(reason) if has_next => last_retry = Some(reason),
                    Outcome::Retry(reason) => {
                        // 读类把最后一次响应交给调用方映射成「不存在」；
                        // 写类必须报错。
                        if accept_last_value {
                            return Ok(value);
                        }
                        tracing::warn!(
                            bucket = bucket.unwrap_or(""),
                            reason = %reason,
                            "Filer 写类 RPC 在所有端点均失败"
                        );
                        return Err(Error::other(reason));
                    }
                },
                Err(status) if is_retryable(&status) && has_next => last_status = Some(status),
                Err(status) => return Err(crate::error::from_status(status)),
            }
        }

        Err(match (last_retry, last_status) {
            (Some(reason), _) => Error::other(reason),
            (None, Some(status)) => crate::error::from_status(status),
            (None, None) => Error::other("Filer RPC 未执行"),
        })
    }

    /// 记录某桶的 leader 端点（仅写成功时调用）。
    fn remember_leader(&self, bucket: Option<&str>, index: usize) {
        if let (Some(bucket), Ok(mut hints)) = (bucket, self.leader_hints.write()) {
            hints.insert(bucket.to_string(), index);
        }
    }

    /// 丢弃某桶的 leader 提示（写失败或写入后不可见时调用）。
    fn forget_leader(&self, bucket: &str) {
        if let Ok(mut hints) = self.leader_hints.write() {
            hints.remove(bucket);
        }
    }
}

/// 端点尝试顺序：有 leader 提示时从提示开始，否则从轮询起点开始，环形遍历。
fn attempt_order(
    hint: Option<usize>,
    fallback: usize,
    channels: usize,
) -> impl Iterator<Item = usize> {
    let start = hint.unwrap_or(fallback) % channels;
    (0..channels).map(move |attempt| (start + attempt) % channels)
}

/// 对象元数据写入的最大下发次数（幂等 RPC，失败可在新 leader 上重发）。
const META_WRITE_ATTEMPTS: usize = 3;
/// 单次下发后等待条目可见的时长。
const META_VISIBILITY_WAIT: std::time::Duration = std::time::Duration::from_millis(500);

/// 单个响应的处置结果。
enum Outcome {
    /// 接受该响应。
    Accept,
    /// 换端点重试（follower 的过期预检 / not_leader / apply 超时）。
    Retry(String),
}

/// 写类 RPC 响应的处置：只有成功才是 `Accept`；任何业务失败都必须重试其余
/// 端点、最终报错——把失败当作成功会静默丢失元数据。
fn mutation_outcome<T: MutationReply>(response: &T) -> Outcome {
    match response.failure() {
        None => Outcome::Accept,
        Some(reason) => Outcome::Retry(reason.to_string()),
    }
}

/// 读类 RPC：`retry_when` 为真时换端点重试，末个端点仍不满足则接受该值
/// （调用方按「不存在」处理）。
fn read_outcome<T>(response: &T, retry_when: impl Fn(&T) -> bool) -> Outcome {
    if retry_when(response) {
        Outcome::Retry("读取到的元数据尚未在本节点可见".to_string())
    } else {
        Outcome::Accept
    }
}

/// 读到的条目是否已经反映这次写入（size、chunk 数量、全部 extended 键）。
fn meta_matches(response: &GetObjectMetaResponse, write: &ObjectMetaWrite<'_>) -> bool {
    let Some(entry) = &response.entry else {
        return false;
    };
    if !response.found
        || entry.content_size != write.size
        || entry.chunks.len() != write.chunks.len()
    {
        return false;
    }
    write
        .extended
        .iter()
        .all(|(key, value)| entry.extended.get(key) == Some(value))
}

/// 写类 RPC 响应中提取失败原因（成功返回 `None`）。
trait MutationReply {
    fn failure(&self) -> Option<&str>;
}

impl MutationReply for EnsureBucketRootResponse {
    fn failure(&self) -> Option<&str> {
        (!self.success).then_some(self.error.as_str())
    }
}

impl MutationReply for DeleteBucketRootResponse {
    fn failure(&self) -> Option<&str> {
        (!self.success).then_some(self.error.as_str())
    }
}

impl MutationReply for PutObjectMetaResponse {
    fn failure(&self) -> Option<&str> {
        (!self.success).then_some(self.error.as_str())
    }
}

impl MutationReply for DeleteObjectMetaResponse {
    fn failure(&self) -> Option<&str> {
        (!self.success).then_some(self.error.as_str())
    }
}

/// 补齐 scheme，供 tonic `Endpoint` 解析。
fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

/// 传输层/超时类错误可换端点重试。
fn is_retryable(status: &Status) -> bool {
    matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_scheme_is_added() {
        assert_eq!(normalize_endpoint("10.0.0.1:9000"), "http://10.0.0.1:9000");
        assert_eq!(
            normalize_endpoint("https://filer:9000"),
            "https://filer:9000"
        );
    }

    #[test]
    fn retryable_statuses() {
        assert!(is_retryable(&Status::unavailable("down")));
        assert!(is_retryable(&Status::deadline_exceeded("slow")));
        assert!(!is_retryable(&Status::internal("boom")));
    }

    #[test]
    fn mutation_failures_are_never_accepted() {
        // 任何业务失败都必须重试/报错，不能当作成功（否则静默丢元数据）。
        let failed = PutObjectMetaResponse {
            success: false,
            inode: 7,
            error: "invalid bucket name".to_string(),
        };
        assert!(matches!(mutation_outcome(&failed), Outcome::Retry(_)));

        let ok = PutObjectMetaResponse {
            success: true,
            inode: 7,
            error: String::new(),
        };
        assert!(matches!(mutation_outcome(&ok), Outcome::Accept));
    }

    #[test]
    fn attempt_order_starts_at_leader_hint() {
        assert_eq!(
            attempt_order(Some(2), 0, 3).collect::<Vec<_>>(),
            vec![2, 0, 1]
        );
        assert_eq!(
            attempt_order(Some(1), 2, 3).collect::<Vec<_>>(),
            vec![1, 2, 0]
        );
    }

    #[test]
    fn attempt_order_falls_back_to_round_robin() {
        assert_eq!(attempt_order(None, 1, 3).collect::<Vec<_>>(), vec![1, 2, 0]);
        assert_eq!(attempt_order(None, 5, 3).collect::<Vec<_>>(), vec![2, 0, 1]);
    }

    #[test]
    fn meta_matches_requires_size_chunks_and_extended() {
        let chunks = vec![powerfs_master::filer_proto::powerfs::FileChunk::default()];
        let mut extended = std::collections::HashMap::new();
        extended.insert("s3.etag".to_string(), b"abc".to_vec());
        let write = ObjectMetaWrite {
            bucket: "b",
            key: "k",
            size: 5,
            chunks: &chunks,
            extended: &extended,
        };

        let entry_with = |size: u64, chunk_count: usize, with_etag: bool| {
            let mut entry = powerfs_master::filer_proto::powerfs::Entry {
                content_size: size,
                chunks: vec![
                    powerfs_master::filer_proto::powerfs::FileChunk::default();
                    chunk_count
                ],
                ..Default::default()
            };
            if with_etag {
                entry
                    .extended
                    .insert("s3.etag".to_string(), b"abc".to_vec());
            }
            multi_bucket_response(entry, true)
        };

        assert!(meta_matches(&entry_with(5, 1, true), &write));
        // 只创建、未更新：size/chunks/extended 都还是空。
        assert!(!meta_matches(&entry_with(0, 0, false), &write));
        // size 或 chunk 数量未 apply。
        assert!(!meta_matches(&entry_with(0, 1, true), &write));
        assert!(!meta_matches(&entry_with(5, 0, true), &write));
        // extended 未 apply。
        assert!(!meta_matches(&entry_with(5, 1, false), &write));
    }

    fn multi_bucket_response(
        entry: powerfs_master::filer_proto::powerfs::Entry,
        found: bool,
    ) -> GetObjectMetaResponse {
        GetObjectMetaResponse {
            found,
            entry: Some(entry),
            error: String::new(),
        }
    }
}
