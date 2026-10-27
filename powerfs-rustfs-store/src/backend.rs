//! [`PowerFsStore`]：RustFS `ExternalBackend` 契约的 PowerFS 实现。

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use http::HeaderMap;
use powerfs_master::s3::master_client::S3MasterClient;
use powerfs_net::{ClientConnPool, ClientPoolConfig, TlvVolumeClient};
use rustfs_ecstore::api::error::Result;
use rustfs_ecstore::api::object::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use rustfs_ecstore::storage_external::ExternalBackend;
use rustfs_storage_api::{
    BucketInfo, BucketOptions, DeleteBucketOptions, HTTPRangeSpec,
    ListObjectVersionsInfo as StorageListObjectVersionsInfo,
    ListObjectsV2Info as StorageListObjectsV2Info, MakeBucketOptions,
};

use crate::config::StoreConfig;

/// PowerFS 存储后端。
///
/// 组合三个 PowerFS 客户端：
/// - [`S3MasterClient`]：volume 分配与 volume 位置查询；
/// - [`TlvVolumeClient`]：needle 数据读写删（powerfs-net TLV 数据面）；
/// - [`crate::filer::FilerClient`]：桶/对象元数据（Filer Raft）。
pub struct PowerFsStore {
    pub(crate) config: StoreConfig,
    pub(crate) master: S3MasterClient,
    pub(crate) volumes: TlvVolumeClient,
    pub(crate) filer: Arc<crate::filer::FilerClient>,
}

impl PowerFsStore {
    /// 依据显式配置构造后端。
    pub fn new(config: StoreConfig) -> Result<Self> {
        let master = S3MasterClient::new(config.master_endpoints.clone())
            .map_err(crate::error::from_master)?;
        let filer = crate::filer::FilerClient::new(&config)?;
        // TLV 连接池需要 u64 客户端标识；由配置的 client_id 字符串派生，
        // 进程内稳定、跨进程随 UUID v4 随机。
        let pool_client_id = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            config.client_id.hash(&mut hasher);
            hasher.finish()
        };
        let conn_pool = Arc::new(ClientConnPool::new(
            pool_client_id,
            ClientPoolConfig::default(),
            None,
        ));
        Ok(Self {
            config,
            master,
            volumes: TlvVolumeClient::new(conn_pool),
            filer: Arc::new(filer),
        })
    }

    /// 依据环境变量构造后端，见 [`StoreConfig::from_env`]。
    pub fn from_env() -> Result<Self> {
        Self::new(StoreConfig::from_env()?)
    }

    /// 生效配置。
    pub fn config(&self) -> &StoreConfig {
        &self.config
    }

    /// 安装为进程级外部后端，供 RustFS 的 S3 层路由到 PowerFS。
    ///
    /// 只在启动阶段调用一次；重复安装会返回被拒绝的后端。
    pub fn install(self: Arc<Self>) -> std::result::Result<(), Arc<dyn ExternalBackend>> {
        rustfs_ecstore::storage_external::install(self)
    }
}

impl std::fmt::Debug for PowerFsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PowerFsStore")
            .field("master_endpoints", &self.config.master_endpoints)
            .field("collection", &self.config.collection)
            .field("replication", &self.config.replication)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ExternalBackend for PowerFsStore {
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        self.create_bucket(bucket, opts).await
    }

    async fn get_bucket_info(&self, bucket: &str, _opts: &BucketOptions) -> Result<BucketInfo> {
        self.bucket_info(bucket).await
    }

    async fn list_bucket(&self, _opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        self.list_buckets().await
    }

    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        self.remove_bucket(bucket, opts).await
    }

    async fn get_object_info(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        self.object_info(bucket, object, opts).await
    }

    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        self.read_object(bucket, object, range, h, opts).await
    }

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        self.write_object(bucket, object, data, opts).await
    }

    async fn delete_object(
        &self,
        bucket: &str,
        object: &str,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        self.remove_object(bucket, object, opts).await
    }

    async fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        fetch_owner: bool,
        start_after: Option<String>,
        incl_deleted: bool,
    ) -> Result<StorageListObjectsV2Info<ObjectInfo>> {
        self.list_v2(
            bucket,
            prefix,
            continuation_token,
            delimiter,
            max_keys,
            fetch_owner,
            start_after,
            incl_deleted,
        )
        .await
    }

    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        version_marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
    ) -> Result<StorageListObjectVersionsInfo<ObjectInfo>> {
        self.list_versions(bucket, prefix, marker, version_marker, delimiter, max_keys)
            .await
    }
}
