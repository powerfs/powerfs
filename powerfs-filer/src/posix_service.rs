use std::sync::Arc;

use protobuf::Message;
use tonic::{Request, Response, Status};

use crate::meta_shard_manager::{MetaShardManager, POSIX_ROOT_INODE};
use crate::raft_group_manager::ShardId;
use crate::shard_strategy::ShardStrategy;

use super::powerfs::posix_meta_service_server::PosixMetaService;
use super::powerfs::{
    CreateDirectoryRequest, CreateDirectoryResponse, CreateEntryRequest, CreateEntryResponse,
    DeleteEntryRequest, DeleteEntryResponse, Entry as ProtoEntry, FuseAttributes,
    GetEntryByInodeRequest, GetEntryByInodeResponse, GetEntryRequest, GetEntryResponse,
    GetShardStatsRequest, GetShardStatsResponse, LeaseReleaseRequest, LeaseReleaseResponse,
    LeaseRenewRequest, LeaseRenewResponse, LeaseRequest, LeaseResponse, ListEntriesRequest,
    ListEntriesResponse, ListShardsRequest, ListShardsResponse, LookupDirectoryEntryRequest,
    LookupDirectoryEntryResponse, PullDeltaRequest, PullDeltaResponse, PushDeltaRequest,
    PushDeltaResponse, RaftMessageRequest, RaftMessageResponse, RenameEntryRequest,
    RenameEntryResponse, UpdateEntryRequest, UpdateEntryResponse,
};

const S_IFDIR: u32 = 0o170000;

pub struct PosixMetaServiceImpl {
    meta_shard_manager: Arc<MetaShardManager>,
    shard_strategy: Arc<ShardStrategy>,
}

impl PosixMetaServiceImpl {
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
}

#[tonic::async_trait]
impl PosixMetaService for PosixMetaServiceImpl {
    async fn get_entry(
        &self,
        request: Request<GetEntryRequest>,
    ) -> Result<Response<GetEntryResponse>, Status> {
        let req = request.into_inner();
        let path = req.path;

        // POSIX flat path: directly resolve from root (inode 1)
        let inode = match self.meta_shard_manager.resolve_flat_path(&path).await {
            Ok(ino) => ino,
            Err(e) => {
                return Ok(Response::new(GetEntryResponse {
                    found: false,
                    entry: None,
                    error: format!("path not found: {}", e),
                }));
            }
        };

        let shard_id = self.inode_to_shard_id(inode);
        let entry = match self.meta_shard_manager.get_entry(inode, shard_id).await {
            Ok(e) => e,
            Err(_) => {
                return Ok(Response::new(GetEntryResponse {
                    found: false,
                    entry: None,
                    error: "entry not found".to_string(),
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
                    error: "entry not found".to_string(),
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

        // POSIX flat path: resolve parent directory directly
        // If parent directory doesn't exist, auto-create it (like mkdir -p)
        let parent_inode = match self
            .meta_shard_manager
            .resolve_flat_path(&parent_path)
            .await
        {
            Ok(ino) => ino,
            Err(_) => {
                // Auto-create parent directories (mkdir -p behavior for CRDT async sync)
                match self
                    .meta_shard_manager
                    .create_directory_for_path(&parent_path)
                    .await
                {
                    Ok(ino) => ino,
                    Err(e) => {
                        return Ok(Response::new(CreateEntryResponse {
                            success: false,
                            error: format!("failed to create parent directory: {}", e),
                            inode: 0,
                        }));
                    }
                }
            }
        };

        // Check if parent is a directory
        let parent_shard_id = self.inode_to_shard_id(parent_inode);
        let parent_entry = match self
            .meta_shard_manager
            .get_entry(parent_inode, parent_shard_id)
            .await
        {
            Ok(e) => e,
            Err(_) => {
                return Ok(Response::new(CreateEntryResponse {
                    success: false,
                    error: "parent directory not found".to_string(),
                    inode: 0,
                }));
            }
        };

        if parent_entry.file_type != crate::shard_store::FileType::Directory {
            return Ok(Response::new(CreateEntryResponse {
                success: false,
                error: "parent is not a directory".to_string(),
                inode: 0,
            }));
        }

        // Determine if creating file or directory based on mode
        let mode = entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0);
        let is_directory = (mode & S_IFDIR) != 0;

        let inode = if is_directory {
            // Create directory
            match self
                .meta_shard_manager
                .create_directory(parent_inode, &name)
                .await
            {
                Ok(info) => info.inode,
                Err(e) => {
                    return Ok(Response::new(CreateEntryResponse {
                        success: false,
                        error: e,
                        inode: 0,
                    }));
                }
            }
        } else {
            // Create file
            let shard_id = self.inode_to_shard_id(parent_inode);
            match self
                .meta_shard_manager
                .create_file_with_shard(parent_inode, &name, shard_id)
                .await
            {
                Ok(ino) => ino,
                Err(e) => {
                    return Ok(Response::new(CreateEntryResponse {
                        success: false,
                        error: e,
                        inode: 0,
                    }));
                }
            }
        };

        Ok(Response::new(CreateEntryResponse {
            success: true,
            error: "".to_string(),
            inode,
        }))
    }

    async fn create_directory(
        &self,
        request: Request<CreateDirectoryRequest>,
    ) -> Result<Response<CreateDirectoryResponse>, Status> {
        let req = request.into_inner();
        let path = req.path.clone();
        let _mode = req.mode;
        let _client_id = req.client_id;

        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();

        if parts.is_empty() {
            return Ok(Response::new(CreateDirectoryResponse {
                success: false,
                error: "empty path".to_string(),
                inode: 0,
            }));
        }

        // Recursively create directories (like mkdir -p)
        let mut current_inode = POSIX_ROOT_INODE;

        for part in &parts {
            let shard_id = self.inode_to_shard_id(current_inode);

            // Check if directory already exists
            let shard_store = match self.meta_shard_manager.get_shard_store(shard_id).await {
                Ok(store) => store,
                Err(e) => {
                    return Ok(Response::new(CreateDirectoryResponse {
                        success: false,
                        error: format!("failed to get shard store: {}", e),
                        inode: 0,
                    }));
                }
            };

            let exists = shard_store.lookup(current_inode, part).is_some();

            if exists {
                // Directory already exists, get its inode
                let entry = match self
                    .meta_shard_manager
                    .lookup_entry(current_inode, part, shard_id)
                    .await
                {
                    Ok(ino) => ino,
                    Err(e) => {
                        return Ok(Response::new(CreateDirectoryResponse {
                            success: false,
                            error: format!("failed to lookup existing directory: {}", e),
                            inode: 0,
                        }));
                    }
                };
                current_inode = entry;
            } else {
                // Create directory
                let dir_info = match self
                    .meta_shard_manager
                    .create_directory(current_inode, part)
                    .await
                {
                    Ok(info) => info,
                    Err(e) => {
                        return Ok(Response::new(CreateDirectoryResponse {
                            success: false,
                            error: format!("failed to create directory '{}': {}", part, e),
                            inode: 0,
                        }));
                    }
                };
                current_inode = dir_info.inode;
            }
        }

        Ok(Response::new(CreateDirectoryResponse {
            success: true,
            error: "".to_string(),
            inode: current_inode,
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

        // Protect root inode
        if ino == POSIX_ROOT_INODE {
            return Ok(Response::new(DeleteEntryResponse {
                success: false,
                error: "cannot delete root directory".to_string(),
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

        let shard_id = self.inode_to_shard_id(parent_ino);
        let entries = match self
            .meta_shard_manager
            .list_entries(parent_ino, shard_id, limit)
            .await
        {
            Ok(e) => e,
            Err(_) => {
                return Ok(Response::new(ListEntriesResponse {
                    entries: vec![],
                    has_more: false,
                    error: "failed to list entries".to_string(),
                }));
            }
        };

        let proto_entries = entries
            .into_iter()
            .map(|e| proto_entry_from_inode(&e))
            .collect();

        Ok(Response::new(ListEntriesResponse {
            entries: proto_entries,
            has_more: false,
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

        let entry = match self
            .meta_shard_manager
            .get_entry(child_inode, shard_id)
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
        let shard_id = ShardId(req.shard_id);

        let result = self
            .meta_shard_manager
            .push_delta(shard_id, &req.client_id, &req.deltas, &req.client_vclock)
            .await;

        match result {
            Ok(vclock) => Ok(Response::new(PushDeltaResponse {
                success: true,
                error: "".to_string(),
                server_vclock: Some(vclock),
            })),
            Err(e) => Ok(Response::new(PushDeltaResponse {
                success: false,
                error: e,
                server_vclock: None,
            })),
        }
    }

    async fn pull_delta(
        &self,
        request: Request<PullDeltaRequest>,
    ) -> Result<Response<PullDeltaResponse>, Status> {
        let req = request.into_inner();
        let shard_id = ShardId(req.shard_id);

        let result = self
            .meta_shard_manager
            .pull_delta(shard_id, &req.client_id, &req.client_vclock)
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
        request: Request<RaftMessageRequest>,
    ) -> Result<Response<RaftMessageResponse>, Status> {
        let req = request.into_inner();
        let shard_id = ShardId(req.shard_id);
        let message_data = req.message;

        log::debug!("Received Raft message for shard {}", shard_id.0);

        // Deserialize the Raft message
        let mut msg = raft::eraftpb::Message::new();
        if let Err(e) = msg.merge_from_bytes(&message_data) {
            log::error!("Failed to deserialize Raft message: {}", e);
            return Ok(Response::new(RaftMessageResponse {
                success: false,
                error: format!("failed to deserialize message: {}", e),
            }));
        }

        // Pass the message to the Raft group manager
        let result = self
            .meta_shard_manager
            .step_raft_message(shard_id, msg)
            .await;

        match result {
            Ok(_) => Ok(Response::new(RaftMessageResponse {
                success: true,
                error: "".to_string(),
            })),
            Err(e) => Ok(Response::new(RaftMessageResponse {
                success: false,
                error: e,
            })),
        }
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
        chunks: vec![],
        hard_link_id: "".to_string(),
        hard_link_counter: 0,
        extended: std::collections::HashMap::new(),
        content_size: inode.size,
        disk_size: inode.size,
        ttl: "".to_string(),
        symlink_target: "".to_string(),
        owner: "".to_string(),
        generation: 0,
    }
}
