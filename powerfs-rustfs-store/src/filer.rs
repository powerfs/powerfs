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
        })
    }

    /// 确保 bucket 根目录存在，返回 `(inode, 是否新建)`。
    pub(crate) async fn ensure_bucket_root(&self, bucket: &str) -> Result<(u64, bool)> {
        let request = EnsureBucketRootRequest {
            bucket: bucket.to_string(),
        };
        let response = self
            .call_retry(
                move |mut client| {
                    let request = request.clone();
                    async move { client.ensure_bucket_root(request).await }
                },
                |response: &tonic::Response<EnsureBucketRootResponse>| {
                    mutation_retry(response.get_ref())
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
                move |mut client| {
                    let request = request.clone();
                    async move { client.get_bucket_root(request).await }
                },
                // 桶刚在 leader 上创建时，follower 可能还没 apply；换端点再查。
                |response: &tonic::Response<GetBucketRootResponse>| {
                    (!response.get_ref().found).then(|| "bucket root 尚未在本节点可见".to_string())
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
                |mut client| async move { client.list_bucket_roots(ListBucketRootsRequest {}).await },
                |_| None,
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
                move |mut client| {
                    let request = request.clone();
                    async move { client.delete_bucket_root(request).await }
                },
                |response: &tonic::Response<DeleteBucketRootResponse>| {
                    mutation_retry(response.get_ref())
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
        let response = self
            .call_retry(
                move |mut client| {
                    let request = request.clone();
                    async move { client.put_object_meta(request).await }
                },
                |response: &tonic::Response<PutObjectMetaResponse>| {
                    mutation_retry(response.get_ref())
                },
                false,
            )
            .await?
            .into_inner();

        if response.success {
            Ok(response.inode)
        } else {
            Err(Error::other(response.error))
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
                move |mut client| {
                    let request = request.clone();
                    async move { client.get_object_meta(request).await }
                },
                // 刚写入的对象在 follower 上可能尚未可见；换端点再读。
                |response: &tonic::Response<GetObjectMetaResponse>| {
                    (!response.get_ref().found).then(|| "object meta 尚未在本节点可见".to_string())
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
    pub(crate) async fn delete_object_meta(&self, bucket: &str, key: &str) -> Result<()> {
        let request = DeleteObjectMetaRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
        };
        let response = self
            .call_retry(
                move |mut client| {
                    let request = request.clone();
                    async move { client.delete_object_meta(request).await }
                },
                |response: &tonic::Response<DeleteObjectMetaResponse>| {
                    mutation_retry(response.get_ref())
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
                move |mut client| {
                    let request = request.clone();
                    async move { client.list_object_metas(request).await }
                },
                |_| None,
                true,
            )
            .await?
            .into_inner();

        if !response.error.is_empty() {
            return Err(Error::other(response.error));
        }
        Ok(response)
    }

    /// 执行一次 RPC：轮询所有端点，每次尝试一个端点。
    ///
    /// Filer 明确不做服务端转发（`propose`/`propose_ff` 在非 leader 上返回
    /// `not_leader`），因此写类 RPC 必须换端点重试直到命中 leader。
    /// 读类 RPC 在 follower 上可能暂时看不到刚提交的元数据，因此「未找到」
    /// 也换端点重试；`accept_last_value` 为真时把最后一次响应原样返回
    /// （调用方把它映射成「不存在」），为假时把最后失败原因作为错误返回。
    async fn call_retry<F, Fut, T, P>(
        &self,
        f: F,
        should_retry: P,
        accept_last_value: bool,
    ) -> Result<T>
    where
        F: Fn(FilerMetaServiceClient<Channel>) -> Fut + Send + Sync,
        Fut: Future<Output = std::result::Result<T, Status>> + Send,
        T: Send,
        P: Fn(&T) -> Option<String>,
    {
        let channels = self.channels.len().max(1);
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let mut last_retry = None;
        let mut last_status = None;

        for attempt in 0..channels {
            let index = (start + attempt) % channels;
            let client = FilerMetaServiceClient::new(self.channels[index].clone());
            let has_next = attempt + 1 < channels;
            match f(client).await {
                Ok(value) => match should_retry(&value) {
                    Some(reason) if has_next => last_retry = Some(reason),
                    _ if accept_last_value => return Ok(value),
                    Some(reason) => return Err(Error::other(reason)),
                    None => return Ok(value),
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
}

/// 写类 RPC 的业务级失败是否值得换端点重试。
fn mutation_retry<T: MutationReply>(response: &T) -> Option<String> {
    let reason = response.failure()?;
    is_freshness_retryable(reason).then(|| reason.to_string())
}

/// 写类 RPC 值得换端点重试的业务错误。
///
/// 两类来源：
/// 1. 服务端显式要求重定向到 leader（`not_leader`）或等待 apply 超时；
/// 2. 处理端在 propose **之前**的本地读预检（`find_bucket_root`、
///    `get_object_entry`、列子项）——follower 的 Raft apply 滞后时这些
///    预检会给出过期的 "bucket not found" / "bucket not empty" /
///    "object not found"，且不带 `not_leader`。leader 的本地状态永远
///    新鲜，换端点直到命中 leader 即得到正确结果。
fn is_freshness_retryable(message: &str) -> bool {
    message.contains("not_leader")
        || message.contains("timeout waiting for apply")
        || message.contains("bucket not found")
        || message.contains("bucket not empty")
        || message.contains("object not found")
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
        assert_eq!(
            normalize_endpoint("203.0.113.1:9000"),
            "http://203.0.113.1:9000"
        );
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
    fn leader_redirect_errors_are_retryable() {
        assert!(is_freshness_retryable(
            "not_leader: shard 0 requires client redirect (current state: Learner)"
        ));
        assert!(is_freshness_retryable(
            "failed to create bucket root: timeout waiting for apply"
        ));
        // follower 本地预检的过期失败同样重试（leader 视图永远新鲜）。
        assert!(is_freshness_retryable("bucket not empty"));
        assert!(is_freshness_retryable("bucket not found"));
        assert!(is_freshness_retryable("object not found"));
        // 真正的业务错误不重试。
        assert!(!is_freshness_retryable("invalid bucket name"));
    }
}
