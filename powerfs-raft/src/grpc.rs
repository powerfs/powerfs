//! gRPC 传输层：RaftService 服务端实现。
//!
//! 参考 `openraft/examples/raft-kv-memstore-grpc/src/grpc/raft_service.rs`，
//! 适配 PowerFS：泛型于 `C: RaftTypeConfig`（Master + Filer 共用一份）。
//!
//! 本模块仅实现"入站"方向：本节点作为服务端接收对端节点的 RPC，
//! 转发给本地 `Raft<C, RocksStateMachine>` 实例处理。
//! "出站"方向（本节点作为客户端调用对端 RPC）见 `network` 模块。
//!
//! # 安全提示
//!
//! RaftService 承载共识协议的内部通信，只能暴露给受信任的集群节点，
//! 绝不能对外开放。生产部署应通过 mTLS 或网络隔离限制访问。

use std::pin::Pin;

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

/// 内部 gRPC 服务实现，处理 Raft 节点间的共识协议通信。
///
/// 泛型于 `C`（Master/Filer 共用），状态机固定为 `RocksStateMachine`
/// （PowerFS 唯一的 RaftStateMachine 实现）。
///
/// 持有一个 `Raft<C, RocksStateMachine>` 句柄（内部 `Arc`，可廉价克隆）。
pub struct RaftServiceImpl<C>
where
    C: RaftTypeConfig,
    RocksStateMachine: RaftStateMachine<C>,
{
    raft: Raft<C, RocksStateMachine>,
}

impl<C> RaftServiceImpl<C>
where
    C: RaftTypeConfig,
    RocksStateMachine: RaftStateMachine<C>,
{
    /// 创建新的服务实例。
    ///
    /// `raft` 是本地 Raft 节点句柄，所有入站 RPC 都转发给它处理。
    pub fn new(raft: Raft<C, RocksStateMachine>) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl<C> RaftService for RaftServiceImpl<C>
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
    /// 处理 VoteRequest（领导者选举投票）。
    async fn vote(
        &self,
        request: Request<pb::VoteRequest>,
    ) -> Result<Response<pb::VoteResponse>, Status> {
        debug!("Processing vote request");
        let req = request.into_inner().into();
        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|e| Status::internal(format!("Vote operation failed: {}", e)))?;
        Ok(Response::new(resp.into()))
    }

    /// 处理 AppendEntries（日志复制 + 心跳）。
    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        debug!("Processing append entries request");
        let req = pb_to_append_entries_request::<C>(request.into_inner()).map_err(|e| {
            Status::invalid_argument(format!("Invalid AppendEntriesRequest: {}", e))
        })?;
        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(format!("Append entries failed: {}", e)))?;
        Ok(Response::new(resp.into()))
    }

    type StreamAppendStream =
        Pin<Box<dyn Stream<Item = Result<pb::AppendEntriesResponse, Status>> + Send>>;

    /// 处理 StreamAppend（流水线日志复制）。
    ///
    /// 使用 gRPC 双向流：客户端流式发送 AppendEntriesRequest，服务端流式返回 AppendEntriesResponse。
    #[allow(clippy::result_large_err)] // tonic::Status 固有体积较大，无法避免
    async fn stream_append(
        &self,
        request: Request<Streaming<pb::AppendEntriesRequest>>,
    ) -> Result<Response<Self::StreamAppendStream>, Status> {
        debug!("Processing stream_append request");
        let input = request.into_inner();

        // pb::AppendEntriesRequest -> openraft AppendEntriesRequest<C>
        // 转换失败的 item 跳过（filter_map）；与 network.rs 客户端对称。
        let input_stream = input.filter_map(|r| async move {
            r.ok()
                .and_then(|pb_req| pb_to_append_entries_request::<C>(pb_req).ok())
        });

        let output = self.raft.stream_append(input_stream);

        // Result<StreamAppendResult<C>, Fatal<C>> -> Result<pb::AppendEntriesResponse, Status>
        let output_stream = output.map(|result| match result {
            Ok(stream_result) => Ok(stream_result.into()),
            Err(fatal) => Err(Status::internal(format!("Fatal Raft error: {}", fatal))),
        });

        Ok(Response::new(Box::pin(output_stream)))
    }

    /// 处理 Snapshot（快照安装）。
    ///
    /// 流式协议：第一个 chunk 必须是 meta（含 vote + snapshot 元数据），
    /// 后续 chunk 为快照数据分片。服务端收集完整快照后调用 `Raft::install_full_snapshot`。
    async fn snapshot(
        &self,
        request: Request<Streaming<pb::SnapshotRequest>>,
    ) -> Result<Response<pb::SnapshotResponse>, Status> {
        debug!("Processing snapshot installation request");
        let mut stream = request.into_inner();

        // 1) 第一个 chunk 必须是 meta
        let first_chunk = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Empty snapshot stream"))?
            .map_err(|e| Status::internal(format!("Snapshot stream error: {}", e)))?;

        let meta = first_chunk
            .into_meta()
            .ok_or_else(|| Status::invalid_argument("First snapshot chunk must be metadata"))?;

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

        // 2) 收集数据 chunk
        let mut snapshot_bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|e| Status::internal(format!("Snapshot stream error: {}", e)))?;
            let data = chunk
                .into_data_chunk()
                .ok_or_else(|| Status::invalid_argument("Snapshot chunk must be data"))?;
            snapshot_bytes.extend_from_slice(&data);
        }

        // 3) 组装 Snapshot 并安装
        let snapshot = openraft::storage::Snapshot {
            meta: snapshot_meta,
            snapshot: SnapshotData::new(snapshot_bytes),
        };

        let resp = self
            .raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| Status::internal(format!("Snapshot installation failed: {}", e)))?;

        Ok(Response::new(pb::SnapshotResponse {
            vote: Some(crate::pb_impl::vote_to_pb::<C>(&resp.vote)),
        }))
    }
}
