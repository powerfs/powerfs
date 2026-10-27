use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::meta_shard_manager::{InodeSizeChunksUpdate, MetaShardManager};
use crate::raft_group_manager_v2::ShardId;
use crate::shard_store::{FileType, ShardStore};
use crate::shard_strategy::ShardStrategy;

use super::powerfs::filer_meta_service_server::FilerMetaService;
use super::powerfs::{
    AddShardRequest, AddShardResponse, AllocInodeBatchRequest, AllocInodeBatchResponse,
    BucketRootInfo, CreateEntryRequest, CreateEntryResponse, DeleteBucketRootRequest,
    DeleteBucketRootResponse, DeleteEntryRequest, DeleteEntryResponse, DeleteObjectMetaRequest,
    DeleteObjectMetaResponse, DrainShardRequest, EnsureBucketRootRequest, EnsureBucketRootResponse,
    Entry as ProtoEntry, FileChunk as ProtoFileChunk, FindInodesByVolumeRequest,
    FindInodesByVolumeResponse, FuseAttributes, GetBucketRootRequest, GetBucketRootResponse,
    GetEntryByInodeRequest, GetEntryByInodeResponse, GetEntryRequest, GetEntryResponse,
    GetObjectMetaRequest, GetObjectMetaResponse, GetShardStatsRequest, GetShardStatsResponse,
    InodeChunkEntry, LeaseReleaseRequest, LeaseReleaseResponse, LeaseRenewRequest,
    LeaseRenewResponse, LeaseRequest, LeaseResponse, ListBucketRootsRequest,
    ListBucketRootsResponse, ListEntriesRequest, ListEntriesResponse, ListObjectMetasRequest,
    ListObjectMetasResponse, ListShardsRequest, ListShardsResponse, LookupDirectoryEntryRequest,
    LookupDirectoryEntryResponse, PullDeltaRequest, PullDeltaResponse, PushDeltaRequest,
    PushDeltaResponse, PutObjectMetaRequest, PutObjectMetaResponse, RaftMessageRequest,
    RaftMessageResponse, RemoveShardRequest, RenameEntryRequest, RenameEntryResponse,
    ShardControlResponse, UpdateEntryRequest, UpdateEntryResponse, UpdateInodeSizeChunksRequest,
    UpdateInodeSizeChunksResponse,
};

/// S3 元数据在 inode `extended` 中的保留前缀；带此前缀但不在请求里的键会被清理。
const S3_META_PREFIX: &str = "s3.";

pub struct FilerMetaServiceImpl {
    meta_shard_manager: Arc<MetaShardManager>,
    shard_strategy: Arc<ShardStrategy>,
}

impl FilerMetaServiceImpl {
    pub fn new(
        meta_shard_manager: Arc<MetaShardManager>,
        shard_strategy: Arc<ShardStrategy>,
    ) -> Self {
        Self {
            meta_shard_manager,
            shard_strategy,
        }
    }

    fn inode_to_shard_id(&self, inode: u64) -> ShardId {
        self.shard_strategy.calculate_shard(inode)
    }

    /// bucket 根目录的 mtime；读不到时返回 0（S3 侧回落为当前时间）。
    async fn bucket_mtime(&self, inode: u64) -> u64 {
        let shard_id = self.inode_to_shard_id(inode);
        self.meta_shard_manager
            .get_entry(inode, shard_id)
            .await
            .map(|info| info.mtime)
            .unwrap_or(0)
    }

    /// 强制删桶前清理子项；只支持扁平对象，遇到子目录直接报错（P1c 不建中间目录）。
    async fn remove_bucket_children(&self, bucket: &str, parent_inode: u64) -> Result<(), String> {
        loop {
            let (children, _) =
                self.meta_shard_manager
                    .list_directory_paginated(parent_inode, "", 1000);
            if children.is_empty() {
                return Ok(());
            }
            for child in children {
                if child.mode & 0o170000 == 0o040000 {
                    return Err(format!(
                        "bucket {bucket} contains directory {}, force delete unsupported",
                        child.name
                    ));
                }
                self.meta_shard_manager
                    .delete_object_entry(parent_inode, &child.name)
                    .await?;
            }
        }
    }
}

#[tonic::async_trait]
impl FilerMetaService for FilerMetaServiceImpl {
    async fn get_entry(
        &self,
        request: Request<GetEntryRequest>,
    ) -> Result<Response<GetEntryResponse>, Status> {
        let req = request.into_inner();
        let path = req.path;

        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        if parts.is_empty() {
            return Ok(Response::new(GetEntryResponse {
                found: false,
                entry: None,
                error: "empty path".to_string(),
            }));
        }

        let bucket = parts[0];
        let key = if parts.len() > 1 {
            parts[1..].join("/")
        } else {
            "".to_string()
        };

        let inode = match self
            .meta_shard_manager
            .resolve_path(&format!("{}/{}", bucket, key))
            .await
        {
            Ok(ino) => ino,
            Err(e) => {
                return Ok(Response::new(GetEntryResponse {
                    found: false,
                    entry: None,
                    error: e,
                }));
            }
        };

        let shard_id = self.inode_to_shard_id(inode);
        let entry = match self.meta_shard_manager.get_entry(inode, shard_id).await {
            Ok(e) => e,
            Err(e) => {
                return Ok(Response::new(GetEntryResponse {
                    found: false,
                    entry: None,
                    error: e,
                }));
            }
        };

        Ok(Response::new(GetEntryResponse {
            found: true,
            entry: Some(proto_entry_from_inode(&entry)),
            error: "".to_string(),
        }))
    }

    async fn get_entry_by_inode(
        &self,
        request: Request<GetEntryByInodeRequest>,
    ) -> Result<Response<GetEntryByInodeResponse>, Status> {
        let req = request.into_inner();
        let inode = req.inode;

        let shard_id = self.inode_to_shard_id(inode);
        let entry = match self.meta_shard_manager.get_entry(inode, shard_id).await {
            Ok(e) => e,
            Err(_) => {
                return Ok(Response::new(GetEntryByInodeResponse {
                    found: false,
                    entry: None,
                    path: "".to_string(),
                    error: "".to_string(),
                }));
            }
        };

        Ok(Response::new(GetEntryByInodeResponse {
            found: true,
            entry: Some(proto_entry_from_inode(&entry)),
            path: "".to_string(),
            error: "".to_string(),
        }))
    }

    async fn create_entry(
        &self,
        request: Request<CreateEntryRequest>,
    ) -> Result<Response<CreateEntryResponse>, Status> {
        let req = request.into_inner();
        let entry = req.entry;
        let _client_id = req.client_id;

        if entry.is_none() {
            return Ok(Response::new(CreateEntryResponse {
                success: false,
                error: "entry is required".to_string(),
                inode: 0,
            }));
        }

        let entry = entry.unwrap();
        let parent_path = entry.directory.clone();
        let name = entry.name.clone();
        // P3.1: Extract mode/uid/gid from gRPC FuseAttributes for direct
        // embedding in CreateInode. Defaults match POSIX expectations (0644
        // regular file, uid=0 gid=0).
        let (mode, uid, gid) = match &entry.attributes {
            Some(attrs) => (attrs.mode, attrs.uid, attrs.gid),
            None => (0o100644, 0, 0),
        };

        let parent_inode = match self.meta_shard_manager.resolve_path(&parent_path).await {
            Ok(ino) => ino,
            Err(_) => {
                return Ok(Response::new(CreateEntryResponse {
                    success: false,
                    error: "parent directory not found".to_string(),
                    inode: 0,
                }));
            }
        };

        let shard_id = self.inode_to_shard_id(parent_inode);
        let inode = match self
            .meta_shard_manager
            .create_file_with_shard(parent_inode, &name, shard_id, mode, uid, gid)
            .await
        {
            Ok((ino, _storage_mode)) => ino,
            Err(e) => {
                return Ok(Response::new(CreateEntryResponse {
                    success: false,
                    error: e,
                    inode: 0,
                }));
            }
        };

        Ok(Response::new(CreateEntryResponse {
            success: true,
            error: "".to_string(),
            inode,
        }))
    }

    async fn update_entry(
        &self,
        request: Request<UpdateEntryRequest>,
    ) -> Result<Response<UpdateEntryResponse>, Status> {
        let req = request.into_inner();
        let entry = req.entry;

        if entry.is_none() {
            return Ok(Response::new(UpdateEntryResponse {
                success: false,
                error: "entry is required".to_string(),
                actual_size: 0,
            }));
        }

        let entry = entry.unwrap();
        let ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);

        if ino == 0 {
            return Ok(Response::new(UpdateEntryResponse {
                success: false,
                error: "inode is required".to_string(),
                actual_size: 0,
            }));
        }

        let shard_id = self.inode_to_shard_id(ino);
        let new_size = entry.attributes.as_ref().map(|a| a.size).unwrap_or(0);

        let result = self
            .meta_shard_manager
            .update_entry(ino, shard_id, new_size)
            .await;

        match result {
            Ok(_) => Ok(Response::new(UpdateEntryResponse {
                success: true,
                error: "".to_string(),
                actual_size: new_size,
            })),
            Err(e) => Ok(Response::new(UpdateEntryResponse {
                success: false,
                error: e,
                actual_size: 0,
            })),
        }
    }

    async fn delete_entry(
        &self,
        request: Request<DeleteEntryRequest>,
    ) -> Result<Response<DeleteEntryResponse>, Status> {
        let req = request.into_inner();
        let ino = req.ino;
        let is_directory = req.is_directory;

        if ino == 0 {
            return Ok(Response::new(DeleteEntryResponse {
                success: false,
                error: "inode is required".to_string(),
            }));
        }

        let shard_id = self.inode_to_shard_id(ino);
        let result = if is_directory {
            self.meta_shard_manager
                .delete_directory_by_inode(ino, shard_id)
                .await
        } else {
            self.meta_shard_manager
                .delete_file_by_inode(ino, shard_id)
                .await
        };

        match result {
            Ok(_) => Ok(Response::new(DeleteEntryResponse {
                success: true,
                error: "".to_string(),
            })),
            Err(e) => Ok(Response::new(DeleteEntryResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn rename_entry(
        &self,
        request: Request<RenameEntryRequest>,
    ) -> Result<Response<RenameEntryResponse>, Status> {
        let req = request.into_inner();
        let old_parent_ino = req.old_parent_ino;
        let old_name = req.old_name;
        let new_parent_ino = req.new_parent_ino;
        let new_name = req.new_name;

        let old_shard_id = self.inode_to_shard_id(old_parent_ino);
        let new_shard_id = self.inode_to_shard_id(new_parent_ino);

        let result = self
            .meta_shard_manager
            .rename_entry(
                old_parent_ino,
                &old_name,
                new_parent_ino,
                &new_name,
                old_shard_id,
                new_shard_id,
            )
            .await;

        match result {
            Ok(_) => Ok(Response::new(RenameEntryResponse {
                success: true,
                error: "".to_string(),
            })),
            Err(e) => Ok(Response::new(RenameEntryResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn list_entries(
        &self,
        request: Request<ListEntriesRequest>,
    ) -> Result<Response<ListEntriesResponse>, Status> {
        let req = request.into_inner();
        let parent_ino = req.parent_ino;
        let limit = req.limit as usize;

        if parent_ino == 0 {
            return Ok(Response::new(ListEntriesResponse {
                entries: vec![],
                has_more: false,
                error: "parent inode is required".to_string(),
            }));
        }

        // 目录项由 `list_directory_paginated` 按名字升序返回，跨节点也保持有序；
        // 之前走 `list_entries` 是全量扫描且顺序取决于 shard 内部顺序。
        let limit = if limit == 0 { 1000 } else { limit };
        let (children, more) =
            self.meta_shard_manager
                .list_directory_paginated(parent_ino, &req.last_name, limit);
        let has_more = more || children.len() > limit;

        let mut proto_entries = Vec::with_capacity(children.len().min(limit));
        for child in children.into_iter().take(limit) {
            let shard_id = self.inode_to_shard_id(child.inode);
            if let Ok(info) = self
                .meta_shard_manager
                .get_entry(child.inode, shard_id)
                .await
            {
                let mut entry = proto_entry_from_inode(&info);
                entry.name = child.name;
                proto_entries.push(entry);
            }
        }

        Ok(Response::new(ListEntriesResponse {
            entries: proto_entries,
            has_more,
            error: "".to_string(),
        }))
    }

    async fn lookup_directory_entry(
        &self,
        request: Request<LookupDirectoryEntryRequest>,
    ) -> Result<Response<LookupDirectoryEntryResponse>, Status> {
        let req = request.into_inner();
        let parent_ino = req.parent_ino;
        let name = req.name;

        if parent_ino == 0 {
            return Ok(Response::new(LookupDirectoryEntryResponse {
                found: false,
                entry: None,
                error: "parent inode is required".to_string(),
            }));
        }

        let shard_id = self.inode_to_shard_id(parent_ino);
        let child_inode = match self
            .meta_shard_manager
            .lookup_entry(parent_ino, &name, shard_id)
            .await
        {
            Ok(ino) => ino,
            Err(_) => {
                return Ok(Response::new(LookupDirectoryEntryResponse {
                    found: false,
                    entry: None,
                    error: "entry not found".to_string(),
                }));
            }
        };

        // inode 记录位于 calculate_shard(inode)，与父目录所在 shard 不一定相同；
        // 之前误用父目录 shard，跨 shard 时查不到条目。
        let child_shard_id = self.inode_to_shard_id(child_inode);
        let entry = match self
            .meta_shard_manager
            .get_entry(child_inode, child_shard_id)
            .await
        {
            Ok(e) => e,
            Err(_) => {
                return Ok(Response::new(LookupDirectoryEntryResponse {
                    found: false,
                    entry: None,
                    error: "entry not found".to_string(),
                }));
            }
        };

        Ok(Response::new(LookupDirectoryEntryResponse {
            found: true,
            entry: Some(proto_entry_from_inode(&entry)),
            error: "".to_string(),
        }))
    }

    async fn get_shard_stats(
        &self,
        request: Request<GetShardStatsRequest>,
    ) -> Result<Response<GetShardStatsResponse>, Status> {
        let req = request.into_inner();
        let shard_id = ShardId(req.shard_id);

        let store = match self.meta_shard_manager.get_shard_store(shard_id).await {
            Ok(s) => s,
            Err(_) => {
                return Ok(Response::new(GetShardStatsResponse {
                    success: false,
                    error: "shard not found".to_string(),
                    shard_id: shard_id.0,
                    inode_count: 0,
                    file_count: 0,
                    dir_count: 0,
                }));
            }
        };

        let stats = store.get_stats();

        Ok(Response::new(GetShardStatsResponse {
            success: true,
            error: "".to_string(),
            shard_id: shard_id.0,
            inode_count: stats.inode_count,
            file_count: stats.file_count,
            dir_count: stats.dir_count,
        }))
    }

    async fn list_shards(
        &self,
        _request: Request<ListShardsRequest>,
    ) -> Result<Response<ListShardsResponse>, Status> {
        let shard_ids = self.meta_shard_manager.list_shards();
        let ids: Vec<u64> = shard_ids.into_iter().map(|s| s.0).collect();

        Ok(Response::new(ListShardsResponse {
            shard_ids: ids,
            error: "".to_string(),
        }))
    }

    async fn push_delta(
        &self,
        request: Request<PushDeltaRequest>,
    ) -> Result<Response<PushDeltaResponse>, Status> {
        let req = request.into_inner();
        // 与 net_handler 一致：req.shard_id 语义为 dir_ino，重映射到实际 shard
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(req.shard_id);

        // B4: delta sync 必须跟 leader（follower 无最新 commit，且写不被 Raft 复制）
        match self
            .meta_shard_manager
            .get_shard_leader_status(shard_id)
            .await
        {
            Some((true, _)) => {}
            Some((false, leader_addr)) => {
                return Ok(Response::new(PushDeltaResponse {
                    success: false,
                    error: format!("not leader; redirect to {}", leader_addr),
                    server_vclock: None,
                }));
            }
            None => {
                return Ok(Response::new(PushDeltaResponse {
                    success: false,
                    error: format!("shard {} leader status unknown", shard_id.0),
                    server_vclock: None,
                }));
            }
        }

        log::info!(
            "push_delta received: shard={}, client={}, deltas_count={}",
            shard_id.0,
            req.client_id,
            req.deltas.len()
        );

        let result = self
            .meta_shard_manager
            .push_delta(shard_id, &req.client_id, &req.deltas, &req.client_vclock)
            .await;

        match result {
            Ok(vclock) => {
                log::info!("push_delta succeeded for client {}", req.client_id);
                Ok(Response::new(PushDeltaResponse {
                    success: true,
                    error: "".to_string(),
                    server_vclock: Some(vclock),
                }))
            }
            Err(e) => {
                log::error!("push_delta failed for client {}: {}", req.client_id, e);
                Ok(Response::new(PushDeltaResponse {
                    success: false,
                    error: e,
                    server_vclock: None,
                }))
            }
        }
    }

    async fn pull_delta(
        &self,
        request: Request<PullDeltaRequest>,
    ) -> Result<Response<PullDeltaResponse>, Status> {
        let req = request.into_inner();
        // 与 net_handler 一致：req.shard_id 语义为 dir_ino，重映射到实际 shard
        let dir_ino = req.shard_id;
        let shard_id = self
            .meta_shard_manager
            .get_shard_strategy()
            .calculate_shard(dir_ino);

        // B4: pull_delta 也跟 leader，保证拿到最新已 commit 的 delta
        match self
            .meta_shard_manager
            .get_shard_leader_status(shard_id)
            .await
        {
            Some((true, _)) => {}
            Some((false, leader_addr)) => {
                return Ok(Response::new(PullDeltaResponse {
                    success: false,
                    error: format!("not leader; redirect to {}", leader_addr),
                    deltas: vec![],
                    server_vclock: None,
                }));
            }
            None => {
                return Ok(Response::new(PullDeltaResponse {
                    success: false,
                    error: format!("shard {} leader status unknown", shard_id.0),
                    deltas: vec![],
                    server_vclock: None,
                }));
            }
        }

        let result = self
            .meta_shard_manager
            .pull_delta(shard_id, dir_ino, &req.client_id, &req.client_vclock)
            .await;

        match result {
            Ok((deltas, vclock)) => Ok(Response::new(PullDeltaResponse {
                success: true,
                error: "".to_string(),
                deltas,
                server_vclock: Some(vclock),
            })),
            Err(e) => Ok(Response::new(PullDeltaResponse {
                success: false,
                error: e,
                deltas: vec![],
                server_vclock: None,
            })),
        }
    }

    async fn alloc_inode_batch(
        &self,
        request: Request<AllocInodeBatchRequest>,
    ) -> Result<Response<AllocInodeBatchResponse>, Status> {
        let req = request.into_inner();
        let shard_id = ShardId(req.shard_id);

        // B4: inode 分配由 leader 单点（follower redirect）
        match self
            .meta_shard_manager
            .get_shard_leader_status(shard_id)
            .await
        {
            Some((true, _)) => {}
            Some((false, leader_addr)) => {
                return Ok(Response::new(AllocInodeBatchResponse {
                    success: false,
                    error: format!("not leader; redirect to {}", leader_addr),
                    start_inode: 0,
                    end_inode: 0,
                }));
            }
            None => {
                return Ok(Response::new(AllocInodeBatchResponse {
                    success: false,
                    error: format!("shard {} leader status unknown", shard_id.0),
                    start_inode: 0,
                    end_inode: 0,
                }));
            }
        }

        match self
            .meta_shard_manager
            .alloc_inode_batch(shard_id, req.count)
            .await
        {
            Ok((start, end)) => Ok(Response::new(AllocInodeBatchResponse {
                success: true,
                error: "".to_string(),
                start_inode: start,
                end_inode: end,
            })),
            Err(e) => Ok(Response::new(AllocInodeBatchResponse {
                success: false,
                error: e,
                start_inode: 0,
                end_inode: 0,
            })),
        }
    }

    async fn update_inode_size_chunks(
        &self,
        request: Request<UpdateInodeSizeChunksRequest>,
    ) -> Result<Response<UpdateInodeSizeChunksResponse>, Status> {
        let req = request.into_inner();
        let shard_id = ShardId(req.shard_id);

        // B4: size/chunks 强一致写由 leader 处理
        match self
            .meta_shard_manager
            .get_shard_leader_status(shard_id)
            .await
        {
            Some((true, _)) => {}
            Some((false, leader_addr)) => {
                return Ok(Response::new(UpdateInodeSizeChunksResponse {
                    success: false,
                    error: format!("not leader; redirect to {}", leader_addr),
                }));
            }
            None => {
                return Ok(Response::new(UpdateInodeSizeChunksResponse {
                    success: false,
                    error: format!("shard {} leader status unknown", shard_id.0),
                }));
            }
        }

        // proto FileChunk -> StoredFileChunk
        let chunks: Vec<crate::shard_store::StoredFileChunk> = req
            .chunks
            .into_iter()
            .map(|c| crate::shard_store::StoredFileChunk {
                offset: c.offset,
                size: c.size,
                mtime: c.mtime,
                needle_id: c.needle_id,
                volume_id: c.volume_id,
                crc32: c.crc32,
                is_reference: false,
            })
            .collect();

        match self
            .meta_shard_manager
            .update_inode_size_chunks_atomic(
                shard_id,
                req.inode,
                InodeSizeChunksUpdate {
                    size: req.size,
                    chunks,
                    inline_data: None,
                    is_append: false,
                    /* gRPC 管理面无 inline 提交 */
                    inline_dirty_mask: None,
                },
            )
            .await
        {
            Ok(_) => Ok(Response::new(UpdateInodeSizeChunksResponse {
                success: true,
                error: "".to_string(),
            })),
            Err(e) => Ok(Response::new(UpdateInodeSizeChunksResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn acquire_lease(
        &self,
        request: Request<LeaseRequest>,
    ) -> Result<Response<LeaseResponse>, Status> {
        let req = request.into_inner();
        let inode = req.inode;

        if inode == 0 {
            return Ok(Response::new(LeaseResponse {
                success: false,
                error: "inode is required".to_string(),
                lease_id: "".to_string(),
                duration_ms: 0,
                epoch: 0,
            }));
        }

        let shard_id = self.inode_to_shard_id(inode);
        let result = self
            .meta_shard_manager
            .acquire_lease(inode, shard_id, &req.client_id, req.duration_ms)
            .await;

        match result {
            Ok((lease_id, epoch)) => Ok(Response::new(LeaseResponse {
                success: true,
                error: "".to_string(),
                lease_id,
                duration_ms: req.duration_ms,
                epoch,
            })),
            Err(e) => Ok(Response::new(LeaseResponse {
                success: false,
                error: e,
                lease_id: "".to_string(),
                duration_ms: 0,
                epoch: 0,
            })),
        }
    }

    async fn release_lease(
        &self,
        request: Request<LeaseReleaseRequest>,
    ) -> Result<Response<LeaseReleaseResponse>, Status> {
        let req = request.into_inner();
        let lease_id = req.lease_id;

        if lease_id.is_empty() {
            return Ok(Response::new(LeaseReleaseResponse {
                success: false,
                error: "lease_id is required".to_string(),
            }));
        }

        let result = self.meta_shard_manager.release_lease(&lease_id).await;

        match result {
            Ok(_) => Ok(Response::new(LeaseReleaseResponse {
                success: true,
                error: "".to_string(),
            })),
            Err(e) => Ok(Response::new(LeaseReleaseResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn renew_lease(
        &self,
        request: Request<LeaseRenewRequest>,
    ) -> Result<Response<LeaseRenewResponse>, Status> {
        let req = request.into_inner();
        let lease_id = req.lease_id;

        if lease_id.is_empty() {
            return Ok(Response::new(LeaseRenewResponse {
                success: false,
                error: "lease_id is required".to_string(),
                epoch: 0,
            }));
        }

        let result = self
            .meta_shard_manager
            .renew_lease(&lease_id, req.duration_ms)
            .await;

        match result {
            Ok(epoch) => Ok(Response::new(LeaseRenewResponse {
                success: true,
                error: "".to_string(),
                epoch,
            })),
            Err(e) => Ok(Response::new(LeaseRenewResponse {
                success: false,
                error: e,
                epoch: 0,
            })),
        }
    }

    async fn send_raft_message(
        &self,
        _request: Request<RaftMessageRequest>,
    ) -> Result<Response<RaftMessageResponse>, Status> {
        // Phase B4: Raft inter-node transport migrated to TLV (MsgType::RaftMessage).
        // This gRPC handler is retained for proto compatibility but is no longer
        // the active transport path. Return an error so any stale caller fails fast.
        log::warn!(
            "send_raft_message: gRPC Raft transport is deprecated; use TLV MsgType::RaftMessage instead"
        );
        Ok(Response::new(RaftMessageResponse {
            success: false,
            error: "gRPC raft transport deprecated; use TLV MsgType::RaftMessage".to_string(),
        }))
    }

    async fn add_shard(
        &self,
        request: Request<AddShardRequest>,
    ) -> Result<Response<AddShardResponse>, Status> {
        let req = request.into_inner();
        let split_from = if req.split_from == 0 {
            None
        } else {
            Some(ShardId(req.split_from))
        };
        match self.shard_strategy.add_shard_auto(split_from, req.dry_run) {
            Ok(plan) => Ok(Response::new(AddShardResponse {
                success: true,
                error: String::new(),
                new_shard_id: plan.new_shard_id.0,
                split_from: plan.split_from.0,
                split_point: plan.split_point,
                new_range_start: plan.new_range.0,
                new_range_end: plan.new_range.1,
                affected_future_allocations: plan.affected_future_allocations,
            })),
            Err(e) => Ok(Response::new(AddShardResponse {
                success: false,
                error: e,
                new_shard_id: 0,
                split_from: 0,
                split_point: 0,
                new_range_start: 0,
                new_range_end: 0,
                affected_future_allocations: 0,
            })),
        }
    }

    async fn drain_shard(
        &self,
        request: Request<DrainShardRequest>,
    ) -> Result<Response<ShardControlResponse>, Status> {
        let req = request.into_inner();
        match self.shard_strategy.drain_shard(ShardId(req.shard_id)) {
            Ok(()) => Ok(Response::new(ShardControlResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(ShardControlResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn remove_shard(
        &self,
        request: Request<RemoveShardRequest>,
    ) -> Result<Response<ShardControlResponse>, Status> {
        let req = request.into_inner();
        match self.shard_strategy.remove_shard(ShardId(req.shard_id)) {
            Ok(()) => Ok(Response::new(ShardControlResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(ShardControlResponse {
                success: false,
                error: e,
            })),
        }
    }

    async fn find_inodes_by_volume(
        &self,
        request: Request<FindInodesByVolumeRequest>,
    ) -> Result<Response<FindInodesByVolumeResponse>, Status> {
        let req = request.into_inner();
        let entries = self
            .meta_shard_manager
            .find_inodes_by_volume(req.volume_id, &req.needle_ids);
        let proto_entries: Vec<InodeChunkEntry> = entries
            .into_iter()
            .map(
                |(inode, shard_id, needle_id, volume_id, offset, size, file_size)| {
                    InodeChunkEntry {
                        inode,
                        shard_id,
                        needle_id,
                        volume_id,
                        offset,
                        size,
                        file_size,
                    }
                },
            )
            .collect();
        Ok(Response::new(FindInodesByVolumeResponse {
            entries: proto_entries,
            error: String::new(),
        }))
    }

    async fn ensure_bucket_root(
        &self,
        request: Request<EnsureBucketRootRequest>,
    ) -> Result<Response<EnsureBucketRootResponse>, Status> {
        let bucket = request.into_inner().bucket;
        if let Err(error) = validate_bucket_name(&bucket) {
            return Ok(Response::new(EnsureBucketRootResponse {
                success: false,
                created: false,
                inode: 0,
                mtime: 0,
                error,
            }));
        }

        let existed = self.meta_shard_manager.find_bucket_root(&bucket).is_some();
        match self.meta_shard_manager.format_bucket_root(&bucket).await {
            Ok(inode) => Ok(Response::new(EnsureBucketRootResponse {
                success: true,
                created: !existed,
                inode,
                mtime: self.bucket_mtime(inode).await,
                error: String::new(),
            })),
            Err(error) => Ok(Response::new(EnsureBucketRootResponse {
                success: false,
                created: false,
                inode: 0,
                mtime: 0,
                error,
            })),
        }
    }

    async fn get_bucket_root(
        &self,
        request: Request<GetBucketRootRequest>,
    ) -> Result<Response<GetBucketRootResponse>, Status> {
        let bucket = request.into_inner().bucket;
        if let Err(error) = validate_bucket_name(&bucket) {
            return Ok(Response::new(GetBucketRootResponse {
                found: false,
                inode: 0,
                mtime: 0,
                error,
            }));
        }

        match self.meta_shard_manager.find_bucket_root(&bucket) {
            Some(inode) => Ok(Response::new(GetBucketRootResponse {
                found: true,
                inode,
                mtime: self.bucket_mtime(inode).await,
                error: String::new(),
            })),
            None => Ok(Response::new(GetBucketRootResponse {
                found: false,
                inode: 0,
                mtime: 0,
                error: String::new(),
            })),
        }
    }

    async fn list_bucket_roots(
        &self,
        _request: Request<ListBucketRootsRequest>,
    ) -> Result<Response<ListBucketRootsResponse>, Status> {
        let mut buckets = Vec::new();
        for bucket in self.meta_shard_manager.list_buckets() {
            if let Some(inode) = self.meta_shard_manager.find_bucket_root(&bucket) {
                buckets.push(BucketRootInfo {
                    bucket,
                    inode,
                    mtime: self.bucket_mtime(inode).await,
                });
            }
        }
        Ok(Response::new(ListBucketRootsResponse {
            buckets,
            error: String::new(),
        }))
    }

    async fn delete_bucket_root(
        &self,
        request: Request<DeleteBucketRootRequest>,
    ) -> Result<Response<DeleteBucketRootResponse>, Status> {
        let req = request.into_inner();
        let Some(inode) = self.meta_shard_manager.find_bucket_root(&req.bucket) else {
            return Ok(Response::new(DeleteBucketRootResponse {
                success: false,
                error: "bucket not found".to_string(),
            }));
        };

        if req.force {
            if let Err(error) = self.remove_bucket_children(&req.bucket, inode).await {
                return Ok(Response::new(DeleteBucketRootResponse {
                    success: false,
                    error,
                }));
            }
        } else {
            let (children, _) = self
                .meta_shard_manager
                .list_directory_paginated(inode, "", 1);
            if !children.is_empty() {
                return Ok(Response::new(DeleteBucketRootResponse {
                    success: false,
                    error: "bucket not empty".to_string(),
                }));
            }
        }

        if let Err(error) = self
            .meta_shard_manager
            .delete_directory_by_inode(inode, self.inode_to_shard_id(inode))
            .await
        {
            return Ok(Response::new(DeleteBucketRootResponse {
                success: false,
                error,
            }));
        }
        self.meta_shard_manager.unregister_root_inode(&req.bucket);
        Ok(Response::new(DeleteBucketRootResponse {
            success: true,
            error: String::new(),
        }))
    }

    async fn put_object_meta(
        &self,
        request: Request<PutObjectMetaRequest>,
    ) -> Result<Response<PutObjectMetaResponse>, Status> {
        let req = request.into_inner();
        // tonic 的 Status 较大，闭包返回值体积告警与本次改动无关。
        #[allow(clippy::result_large_err)]
        let failure = |error: String| {
            Ok(Response::new(PutObjectMetaResponse {
                success: false,
                inode: 0,
                error,
            }))
        };

        if let Err(error) = validate_bucket_name(&req.bucket) {
            return failure(error);
        }
        if let Err(error) = validate_object_key(&req.key) {
            return failure(error);
        }

        let parent_inode = match self.meta_shard_manager.find_bucket_root(&req.bucket) {
            Some(inode) => inode,
            None => match self
                .meta_shard_manager
                .format_bucket_root(&req.bucket)
                .await
            {
                Ok(inode) => inode,
                Err(error) => return failure(error),
            },
        };
        let parent_shard = self.inode_to_shard_id(parent_inode);
        match self
            .meta_shard_manager
            .get_entry(parent_inode, parent_shard)
            .await
        {
            Ok(info) if info.file_type == FileType::Directory => {}
            Ok(_) => return failure(format!("{} is not a directory", req.bucket)),
            Err(error) => return failure(error),
        }

        let inode = match self
            .meta_shard_manager
            .get_object_entry(parent_inode, &req.key)
        {
            Some(info) => info.inode,
            None => {
                match self
                    .meta_shard_manager
                    .create_file_with_shard(parent_inode, &req.key, parent_shard, 0o100644, 0, 0)
                    .await
                {
                    Ok((inode, _)) => inode,
                    Err(error) => return failure(error),
                }
            }
        };
        let shard_id = self.inode_to_shard_id(inode);

        let chunks: Vec<crate::shard_store::StoredFileChunk> =
            req.chunks.iter().map(stored_chunk_from_proto).collect();
        if let Err(error) = self
            .meta_shard_manager
            .update_inode_size_chunks_atomic(
                shard_id,
                inode,
                InodeSizeChunksUpdate {
                    size: req.size,
                    chunks,
                    inline_data: None,
                    is_append: false,
                    inline_dirty_mask: None,
                },
            )
            .await
        {
            return failure(error);
        }

        // 写入与覆盖写都刷新 mtime，保证 S3 Last-Modified 随内容更新。
        // Filer 元数据的 mtime 统一为秒级时间戳。
        // 显式带上传入 size：SetAttr 的 MetaCache 投影会用 store 里的值兜底，
        // 而异步持久化下 store 可能还是旧的 size（0），会把刚写入的 size 覆盖回去。
        let now = ShardStore::current_time();
        if let Err(error) = self
            .meta_shard_manager
            .setattr(
                inode,
                shard_id,
                Some(req.size),
                None,
                None,
                None,
                Some(now),
                Some(now),
            )
            .await
        {
            return failure(error);
        }

        let existing = self
            .meta_shard_manager
            .get_entry(inode, shard_id)
            .await
            .map(|info| info.extended)
            .unwrap_or_default();
        for (key, value) in &req.extended {
            if existing.get(key) != Some(value) {
                if let Err(error) = self
                    .meta_shard_manager
                    .set_xattr(inode, shard_id, key, value.clone())
                    .await
                {
                    return failure(error);
                }
            }
        }
        for key in existing.keys() {
            if key.starts_with(S3_META_PREFIX) && !req.extended.contains_key(key) {
                if let Err(error) = self
                    .meta_shard_manager
                    .remove_xattr(inode, shard_id, key)
                    .await
                {
                    return failure(error);
                }
            }
        }

        Ok(Response::new(PutObjectMetaResponse {
            success: true,
            inode,
            error: String::new(),
        }))
    }

    async fn get_object_meta(
        &self,
        request: Request<GetObjectMetaRequest>,
    ) -> Result<Response<GetObjectMetaResponse>, Status> {
        let req = request.into_inner();
        let Some(parent_inode) = self.meta_shard_manager.find_bucket_root(&req.bucket) else {
            return Ok(Response::new(GetObjectMetaResponse {
                found: false,
                entry: None,
                error: String::new(),
            }));
        };
        let Some(info) = self
            .meta_shard_manager
            .get_object_entry(parent_inode, &req.key)
        else {
            return Ok(Response::new(GetObjectMetaResponse {
                found: false,
                entry: None,
                error: String::new(),
            }));
        };

        let mut entry = proto_entry_from_inode(&info);
        entry.directory = format!("/{}", req.bucket);
        Ok(Response::new(GetObjectMetaResponse {
            found: true,
            entry: Some(entry),
            error: String::new(),
        }))
    }

    async fn delete_object_meta(
        &self,
        request: Request<DeleteObjectMetaRequest>,
    ) -> Result<Response<DeleteObjectMetaResponse>, Status> {
        let req = request.into_inner();
        let Some(parent_inode) = self.meta_shard_manager.find_bucket_root(&req.bucket) else {
            return Ok(Response::new(DeleteObjectMetaResponse {
                success: false,
                error: "bucket not found".to_string(),
            }));
        };
        if self
            .meta_shard_manager
            .get_object_entry(parent_inode, &req.key)
            .is_none()
        {
            return Ok(Response::new(DeleteObjectMetaResponse {
                success: false,
                error: "object not found".to_string(),
            }));
        }
        match self
            .meta_shard_manager
            .delete_object_entry(parent_inode, &req.key)
            .await
        {
            Ok(()) => Ok(Response::new(DeleteObjectMetaResponse {
                success: true,
                error: String::new(),
            })),
            Err(error) => Ok(Response::new(DeleteObjectMetaResponse {
                success: false,
                error,
            })),
        }
    }

    async fn list_object_metas(
        &self,
        request: Request<ListObjectMetasRequest>,
    ) -> Result<Response<ListObjectMetasResponse>, Status> {
        let req = request.into_inner();
        let Some(parent_inode) = self.meta_shard_manager.find_bucket_root(&req.bucket) else {
            return Ok(Response::new(ListObjectMetasResponse {
                entries: Vec::new(),
                has_more: false,
                error: "bucket not found".to_string(),
            }));
        };

        let limit = if req.limit == 0 {
            1000
        } else {
            req.limit as usize
        };
        let (children, more) =
            self.meta_shard_manager
                .list_directory_paginated(parent_inode, &req.last_key, limit);
        let has_more = more || children.len() > limit;
        let mut entries = Vec::new();
        for child in children.into_iter().take(limit) {
            if child.mode & 0o170000 != 0o100000 {
                continue;
            }
            let shard_id = self.inode_to_shard_id(child.inode);
            if let Ok(info) = self
                .meta_shard_manager
                .get_entry(child.inode, shard_id)
                .await
            {
                let mut entry = proto_entry_from_inode(&info);
                entry.directory = format!("/{}", req.bucket);
                entries.push(entry);
            }
        }

        Ok(Response::new(ListObjectMetasResponse {
            entries,
            has_more,
            error: String::new(),
        }))
    }
}

fn proto_entry_from_inode(inode: &crate::shard_store::InodeInfo) -> ProtoEntry {
    ProtoEntry {
        name: inode.name.clone(),
        directory: "/".to_string(),
        attributes: Some(FuseAttributes {
            ino: inode.inode,
            mode: inode.mode,
            nlink: 1,
            uid: inode.uid,
            gid: inode.gid,
            rdev: 0,
            size: inode.size,
            blksize: 4096,
            blocks: inode.blocks,
            atime: inode.atime,
            mtime: inode.mtime,
            ctime: inode.ctime,
            crtime: inode.ctime,
            perm: 0,
        }),
        chunks: inode.chunks.iter().map(proto_chunk_from_stored).collect(),
        hard_link_id: "".to_string(),
        hard_link_counter: 0,
        extended: inode.extended.clone(),
        content_size: inode.size,
        disk_size: inode.size,
        ttl: "".to_string(),
        symlink_target: "".to_string(),
        owner: "".to_string(),
        generation: 0,
        version: inode.version,
    }
}

fn proto_chunk_from_stored(chunk: &crate::shard_store::StoredFileChunk) -> ProtoFileChunk {
    ProtoFileChunk {
        offset: chunk.offset,
        size: chunk.size,
        mtime: chunk.mtime,
        needle_id: chunk.needle_id,
        volume_id: chunk.volume_id,
        crc32: chunk.crc32,
    }
}

fn stored_chunk_from_proto(chunk: &ProtoFileChunk) -> crate::shard_store::StoredFileChunk {
    crate::shard_store::StoredFileChunk {
        offset: chunk.offset,
        size: chunk.size,
        mtime: chunk.mtime,
        needle_id: chunk.needle_id,
        volume_id: chunk.volume_id,
        crc32: chunk.crc32,
        is_reference: false,
    }
}

/// S3 bucket 名合法性：非空、不含 `/`、不是 `.`/`..`。
fn validate_bucket_name(bucket: &str) -> Result<(), String> {
    if bucket.is_empty() {
        return Err("bucket name is empty".to_string());
    }
    if bucket == "." || bucket == ".." || bucket.contains('/') {
        return Err(format!("invalid bucket name: {bucket}"));
    }
    Ok(())
}

/// S3 object key 合法性：非空、不以 `/` 开头、不含 `..` 段。
fn validate_object_key(key: &str) -> Result<(), String> {
    if key.is_empty() {
        return Err("object key is empty".to_string());
    }
    if key.starts_with('/') {
        return Err(format!("invalid object key: {key}"));
    }
    if key.split('/').any(|segment| segment == "..") {
        return Err(format!("invalid object key: {key}"));
    }
    Ok(())
}
