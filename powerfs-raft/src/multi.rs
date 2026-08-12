//! 多 Raft 组服务端路由（Filer 多分片场景）。
//!
//! `MultiRaftRouter<C>` 维护 `group_id -> Raft<C, RocksStateMachine>` 映射表，
//! `MultiRaftServiceImpl<C>` 实现 tonic `RaftService` trait，按请求中的 `group_id`
//! 字段路由到对应的 Raft 实例。
//!
//! 与单组 `RaftServiceImpl`（Master 用）的关系：
//! - Master 仍使用 `grpc::RaftServiceImpl`（单 Raft 实例，无需路由）。
//! - Filer 使用 `MultiRaftServiceImpl`（多 Raft 实例，按 shard_id 路由）。
//! - 两者共用同一份 proto（`raft.proto`）和 pb_impl 转换代码。
//!
//! 参考 `openraft/examples/multi-raft-kv/` 的服务端路由模式。

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use futures::StreamExt;
use openraft::entry::RaftEntry;
use openraft::storage::RaftStateMachine;
use openraft::storage::SnapshotMeta;
use openraft::vote::RaftLeaderId;
use openraft::vote::RaftVote;
use openraft::Membership;
use openraft::Raft;
use openraft::RaftTypeConfig;
use openraft::StoredMembership;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::RwLock;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tracing::debug;

use crate::pb_impl::pb_to_append_entries_request;
use crate::pb_impl::pb_to_log_id;
use crate::pb_impl::pb_to_membership;
use crate::pb_impl::pb_to_vote;
use crate::pb_impl::CommittedLidOf;
use crate::protobuf as pb;
use crate::protobuf::raft_service_server::RaftService;
use crate::store::RocksStateMachine;
use crate::BasicNode;
use crate::SnapshotData;

/// 多 Raft 组路由器：`group_id -> Raft` 映射表。
///
/// 线程安全（内部 `RwLock<HashMap>`）。`Raft` 句柄内部是 `Arc`，克隆廉价。
/// 注册/注销发生在分片创建/销毁时（低频），查询发生在每次 RPC（高频）。
pub struct MultiRaftRouter<C>
where
    C: RaftTypeConfig,
    RocksStateMachine: RaftStateMachine<C>,
{
    groups: RwLock<HashMap<String, Raft<C, RocksStateMachine>>>,
}

impl<C> MultiRaftRouter<C>
where
    C: RaftTypeConfig,
    RocksStateMachine: RaftStateMachine<C>,
{
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
        }
    }

    /// 注册一个 Raft 组。如果 `group_id` 已存在则覆盖。
    pub async fn register_group(
        &self,
        group_id: impl Into<String>,
        raft: Raft<C, RocksStateMachine>,
    ) {
        let id = group_id.into();
        self.groups.write().await.insert(id.clone(), raft);
        debug!("MultiRaftRouter: registered group '{}'", id);
    }

    /// 注销一个 Raft 组。
    pub async fn unregister_group(&self, group_id: &str) {
        self.groups.write().await.remove(group_id);
        debug!("MultiRaftRouter: unregistered group '{}'", group_id);
    }

    /// 查询 Raft 组（克隆 `Raft` 句柄）。
    pub async fn get_group(&self, group_id: &str) -> Option<Raft<C, RocksStateMachine>> {
        self.groups.read().await.get(group_id).cloned()
    }
}

impl<C> Default for MultiRaftRouter<C>
where
    C: RaftTypeConfig,
    RocksStateMachine: RaftStateMachine<C>,
{
    fn default() -> Self {
        Self::new()
    }
}

/// 多 Raft 组 gRPC 服务实现。
///
/// 持有 `Arc<MultiRaftRouter<C>>`，按请求中的 `group_id` 路由到对应 Raft 实例。
/// 用于 Filer 多分片场景（每个 shard 一个 Raft 组）。
pub struct MultiRaftServiceImpl<C>
where
    C: RaftTypeConfig,
    RocksStateMachine: RaftStateMachine<C>,
{
    router: Arc<MultiRaftRouter<C>>,
}

impl<C> MultiRaftServiceImpl<C>
where
    C: RaftTypeConfig,
    RocksStateMachine: RaftStateMachine<C>,
{
    pub fn new(router: Arc<MultiRaftRouter<C>>) -> Self {
        Self { router }
    }
}

#[tonic::async_trait]
impl<C> RaftService for MultiRaftServiceImpl<C>
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
    C::Term: From<u64> + Into<u64>,
    C::NodeId: From<String> + ToString + Ord,
    C::Node: From<BasicNode> + std::borrow::Borrow<BasicNode>,
    C::D: Serialize + DeserializeOwned,
    C::Entry: RaftEntry<D = C::D>,
    CommittedLidOf<C>: RaftLeaderId,
    <CommittedLidOf<C> as RaftLeaderId>::Term: Into<u64>,
    RocksStateMachine: RaftStateMachine<C, SnapshotData = SnapshotData>,
{
    async fn vote(
        &self,
        request: Request<pb::VoteRequest>,
    ) -> Result<Response<pb::VoteResponse>, Status> {
        let pb_req = request.into_inner();
        let group_id = pb_req.group_id.clone();

        let raft = self
            .router
            .get_group(&group_id)
            .await
            .ok_or_else(|| Status::not_found(format!("Raft group '{}' not found", group_id)))?;

        let req: openraft::raft::VoteRequest<C> = pb_req.into();
        let resp = raft
            .vote(req)
            .await
            .map_err(|e| Status::internal(format!("Vote failed: {}", e)))?;
        Ok(Response::new(resp.into()))
    }

    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        let pb_req = request.into_inner();
        let group_id = pb_req.group_id.clone();

        let raft = self
            .router
            .get_group(&group_id)
            .await
            .ok_or_else(|| Status::not_found(format!("Raft group '{}' not found", group_id)))?;

        let req = pb_to_append_entries_request::<C>(pb_req).map_err(|e| {
            Status::invalid_argument(format!("Invalid AppendEntriesRequest: {}", e))
        })?;

        let resp = raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(format!("Append entries failed: {}", e)))?;
        Ok(Response::new(resp.into()))
    }

    type StreamAppendStream =
        Pin<Box<dyn Stream<Item = Result<pb::AppendEntriesResponse, Status>> + Send>>;

    async fn stream_append(
        &self,
        request: Request<Streaming<pb::AppendEntriesRequest>>,
    ) -> Result<Response<Self::StreamAppendStream>, Status> {
        let mut input = request.into_inner();

        // 1) 取第一个 item，提取 group_id 路由
        let first_pb = input
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Empty stream_append stream"))?
            .map_err(|e| Status::internal(format!("Stream error: {}", e)))?;

        let group_id = first_pb.group_id.clone();
        let raft = self
            .router
            .get_group(&group_id)
            .await
            .ok_or_else(|| Status::not_found(format!("Raft group '{}' not found", group_id)))?;

        // 2) 转换首条 + 剩余流，拼接为 openraft AppendEntriesRequest 流
        let first_entry = pb_to_append_entries_request::<C>(first_pb).map_err(|e| {
            Status::invalid_argument(format!("Invalid first AppendEntriesRequest: {}", e))
        })?;

        let rest = input.filter_map(|r| async move {
            r.ok()
                .and_then(|pb_req| pb_to_append_entries_request::<C>(pb_req).ok())
        });

        let combined = futures::stream::once(async { first_entry }).chain(rest);

        // 3) 喂给 Raft::stream_append
        let output = raft.stream_append(combined);
        let output_stream = output.map(|result| match result {
            Ok(stream_result) => Ok(stream_result.into()),
            Err(fatal) => Err(Status::internal(format!("Fatal Raft error: {}", fatal))),
        });

        Ok(Response::new(Box::pin(output_stream)))
    }

    async fn snapshot(
        &self,
        request: Request<Streaming<pb::SnapshotRequest>>,
    ) -> Result<Response<pb::SnapshotResponse>, Status> {
        let mut stream = request.into_inner();

        // 1) 第一个 chunk 必须是 meta，含 group_id
        let first_chunk = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Empty snapshot stream"))?
            .map_err(|e| Status::internal(format!("Snapshot stream error: {}", e)))?;

        let meta = first_chunk
            .into_meta()
            .ok_or_else(|| Status::invalid_argument("First snapshot chunk must be metadata"))?;

        let group_id = meta.group_id.clone();
        let raft = self
            .router
            .get_group(&group_id)
            .await
            .ok_or_else(|| Status::not_found(format!("Raft group '{}' not found", group_id)))?;

        // 2) 解析快照元数据
        let vote = pb_to_vote::<C>(
            meta.vote
                .ok_or_else(|| Status::invalid_argument("Snapshot meta missing vote"))?,
        );

        let snapshot_meta = SnapshotMeta {
            last_log_id: pb_to_log_id::<C>(meta.last_log_id),
            last_membership: StoredMembership::new(
                pb_to_log_id::<C>(meta.last_membership_log_id),
                meta.last_membership
                    .map(|m| pb_to_membership::<C>(m))
                    .unwrap_or_else(Membership::default),
            ),
        };

        // 3) 收集数据 chunk
        let mut snapshot_bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|e| Status::internal(format!("Snapshot stream error: {}", e)))?;
            let data = chunk
                .into_data_chunk()
                .ok_or_else(|| Status::invalid_argument("Snapshot chunk must be data"))?;
            snapshot_bytes.extend_from_slice(&data);
        }

        // 4) 安装快照
        let snapshot = openraft::storage::Snapshot {
            meta: snapshot_meta,
            snapshot: SnapshotData::new(snapshot_bytes),
        };

        let resp = raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| Status::internal(format!("Snapshot installation failed: {}", e)))?;

        Ok(Response::new(pb::SnapshotResponse {
            vote: Some(crate::pb_impl::vote_to_pb::<C>(&resp.vote)),
        }))
    }
}
