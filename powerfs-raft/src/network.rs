//! gRPC 传输层：RaftNetwork 客户端实现。
//!
//! 参考 `openraft/examples/raft-kv-memstore-grpc/src/network/mod.rs`，
//! 适配 PowerFS：泛型于 `C: RaftTypeConfig`（Master + Filer 共用一份）。
//!
//! 实现 `RaftNetworkV2`（openraft 推荐入口），其 blanket impl 自动派生
//! `NetVote`/`NetStreamAppend`/`NetSnapshot`/`NetBackoff`/`NetTransferLeader`/`NetAppend`。
//!
//! 本模块仅实现"出站"方向：本节点作为客户端调用对端节点的 `RaftService`。
//! "入站"方向（本节点作为服务端接收对端 RPC）见 `grpc` 模块。

use std::borrow::Borrow;
use std::future::Future;
use std::time::Duration;

use futures::channel::mpsc;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use openraft::base::BoxFuture;
use openraft::base::BoxStream;
use openraft::entry::RaftEntry;
use openraft::errors::NetworkError;
use openraft::errors::RPCError;
use openraft::errors::ReplicationClosed;
use openraft::errors::StreamingError;
use openraft::errors::Unreachable;
use openraft::network::Backoff;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::StreamAppendError;
use openraft::raft::StreamAppendResult;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::TransferLeaderResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::type_config::alias::SnapshotOf;
use openraft::type_config::alias::VoteOf;
use openraft::vote::RaftLeaderId;
use openraft::vote::RaftVote;
use openraft::AnyError;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use crate::pb_impl::append_entries_request_to_pb;
use crate::pb_impl::log_id_to_pb;
use crate::pb_impl::membership_to_pb;
use crate::pb_impl::pb_to_log_id;
use crate::pb_impl::pb_to_vote;
use crate::pb_impl::vote_to_pb;
use crate::pb_impl::CommittedLidOf;
use crate::protobuf as pb;
use crate::protobuf::raft_service_client::RaftServiceClient;
use crate::BasicNode;
use crate::SnapshotData;

/// PowerFS 的 gRPC 网络工厂。
///
/// 泛型于 `C`，使得 Master + Filer 共用同一份实现。
/// 通过 `RaftNetworkFactory::new_client` 为每个对端节点创建一个 [`NetworkConnection`]。
pub struct Network<C>
where
    C: openraft::RaftTypeConfig,
{
    _phantom: std::marker::PhantomData<C>,
}

impl<C> Network<C>
where
    C: openraft::RaftTypeConfig,
{
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<C> Default for Network<C>
where
    C: openraft::RaftTypeConfig,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<C> RaftNetworkFactory<C> for Network<C>
where
    C: openraft::RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
    C::Term: From<u64> + Into<u64>,
    C::NodeId: From<String> + ToString + Ord,
    C::Node: From<BasicNode> + Borrow<BasicNode>,
    C::D: Serialize + DeserializeOwned,
    C::Entry:
        RaftEntry<D = C::D> + AsRef<openraft::Entry<CommittedLidOf<C>, C::D, C::NodeId, C::Node>>,
    CommittedLidOf<C>: RaftLeaderId,
    <CommittedLidOf<C> as RaftLeaderId>::Term: Into<u64>,
{
    type Network = NetworkConnection<C>;

    async fn new_client(&mut self, _target: C::NodeId, node: &C::Node) -> Self::Network {
        NetworkConnection::new(node.borrow().addr.clone())
    }
}

/// 到单个对端节点的 gRPC 连接（延迟建立）。
///
/// 每次 RPC 调用前通过 [`make_client`] 建立 tonic Channel；
/// 不缓存连接以简化生命周期管理（阶段 2 足够；阶段 3 视性能需求再加连接池）。
pub struct NetworkConnection<C>
where
    C: openraft::RaftTypeConfig,
{
    /// 对端 gRPC 地址（如 "127.0.0.1:50051"）。
    rpc_addr: String,
    _phantom: std::marker::PhantomData<C>,
}

impl<C> NetworkConnection<C>
where
    C: openraft::RaftTypeConfig,
{
    pub fn new(rpc_addr: String) -> Self {
        Self {
            rpc_addr,
            _phantom: std::marker::PhantomData,
        }
    }

    /// 建立 tonic Channel 并返回 `RaftServiceClient`。
    async fn make_client(&self) -> Result<RaftServiceClient<Channel>, RPCError<C>> {
        let url = format!("http://{}", self.rpc_addr);
        let endpoint = Endpoint::from_shared(url)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .connect()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        Ok(RaftServiceClient::new(endpoint))
    }

    /// 把 `pb::AppendEntriesResponse` 转成 `StreamAppendResult`。
    ///
    /// StreamAppend 协议中，conflict 编码为 `conflict = true` 且必带 `last_log_id`（冲突 log id）。
    fn pb_to_stream_result(
        resp: pb::AppendEntriesResponse,
    ) -> Result<StreamAppendResult<C>, RPCError<C>>
    where
        C: openraft::RaftTypeConfig,
        C::LeaderId: RaftLeaderId,
        C::Term: From<u64>,
        C::NodeId: From<String>,
        C::Vote: RaftVote<LeaderId = C::LeaderId>,
    {
        if let Some(higher_vote) = resp.rejected_by {
            return Ok(Err(StreamAppendError::HigherVote(pb_to_vote::<C>(
                higher_vote,
            ))));
        }

        if resp.conflict {
            let conflict_log_id = pb_to_log_id::<C>(resp.last_log_id).ok_or_else(|| {
                RPCError::Network(NetworkError::new(&AnyError::error(
                    "Missing `last_log_id` in conflict stream-append response",
                )))
            })?;
            return Ok(Err(StreamAppendError::Conflict(conflict_log_id)));
        }

        Ok(Ok(pb_to_log_id::<C>(resp.last_log_id)))
    }

    /// 把 snapshot 数据按 1MB 分块发送到流。
    async fn send_snapshot_chunks(
        tx: &mut mpsc::Sender<pb::SnapshotRequest>,
        snapshot_data: &[u8],
    ) -> Result<(), NetworkError<C>> {
        const CHUNK_SIZE: usize = 1024 * 1024;
        for chunk in snapshot_data.chunks(CHUNK_SIZE) {
            let request = pb::SnapshotRequest {
                payload: Some(pb::snapshot_request::Payload::Chunk(chunk.to_vec())),
            };
            tx.send(request).await.map_err(|e| NetworkError::new(&e))?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RaftNetworkV2 — openraft 推荐的入口 trait。
// 实现 V2 后，openraft 的 blanket impl 自动派生 NetVote/NetSnapshot/NetStreamAppend/
// NetBackoff/NetTransferLeader/NetAppend，无需单独 impl 子 trait。
// ---------------------------------------------------------------------------

impl<C> RaftNetworkV2<C> for NetworkConnection<C>
where
    C: openraft::RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
    C::Term: From<u64> + Into<u64>,
    C::NodeId: From<String> + ToString + Ord,
    C::Node: From<BasicNode> + Borrow<BasicNode>,
    C::D: Serialize + DeserializeOwned,
    C::Entry:
        RaftEntry<D = C::D> + AsRef<openraft::Entry<CommittedLidOf<C>, C::D, C::NodeId, C::Node>>,
    CommittedLidOf<C>: RaftLeaderId,
    <CommittedLidOf<C> as RaftLeaderId>::Term: Into<u64>,
{
    type SnapshotData = SnapshotData;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        let mut client = self.make_client().await?;

        let pb_req = append_entries_request_to_pb::<C>(rpc)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let response = client
            .append_entries(pb_req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        // AppendEntriesResponse<C>: From<pb::AppendEntriesResponse>（见 pb_impl.rs）。
        Ok(response.into_inner().into())
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C>,
        _option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        let mut client = self.make_client().await?;

        let pb_req: pb::VoteRequest = rpc.into();
        let response = client
            .vote(pb_req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        Ok(response.into_inner().into())
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let mut client = self.make_client().await?;

        let (mut tx, rx) = mpsc::channel(1024);
        let response = client
            .snapshot(rx)
            .await
            .map_err(|e| NetworkError::new(&e))?;

        // 1) meta chunk
        let meta_request = pb::SnapshotRequest {
            payload: Some(pb::snapshot_request::Payload::Meta(
                pb::SnapshotRequestMeta {
                    vote: Some(vote_to_pb::<C>(&vote)),
                    last_log_id: snapshot.meta.last_log_id.as_ref().map(log_id_to_pb::<C>),
                    last_membership_log_id: snapshot
                        .meta
                        .last_membership
                        .log_id()
                        .as_ref()
                        .map(log_id_to_pb::<C>),
                    last_membership: Some(membership_to_pb::<C>(
                        snapshot.meta.last_membership.membership(),
                    )),
                    group_id: String::new(),
                },
            )),
        };
        tx.send(meta_request)
            .await
            .map_err(|e| NetworkError::new(&e))?;

        // 2) data chunks（SnapshotData = Cursor<Vec<u8>>，取 inner bytes）
        let snapshot_bytes = snapshot.snapshot.into_inner();
        Self::send_snapshot_chunks(&mut tx, &snapshot_bytes).await?;

        // 3) 等待对端响应
        let message = response.into_inner();
        let resp_vote = message.vote.ok_or_else(|| {
            NetworkError::new(&AnyError::error("Missing `vote` in snapshot response"))
        })?;

        Ok(SnapshotResponse {
            vote: pb_to_vote::<C>(resp_vote),
        })
    }

    /// 重写默认的顺序 stream_append，使用 gRPC 双向流。
    fn stream_append<'s, S>(
        &'s mut self,
        input: S,
        _option: RPCOption,
    ) -> BoxFuture<'s, Result<BoxStream<'s, Result<StreamAppendResult<C>, RPCError<C>>>, RPCError<C>>>
    where
        S: Stream<Item = AppendEntriesRequest<C>> + OptionalSend + Unpin + 'static,
    {
        let fu = async move {
            let mut client = self.make_client().await?;

            // 把 openraft AppendEntriesRequest 流映射成 pb::AppendEntriesRequest 流。
            // 转换失败的 item 跳过（filter_map）；阶段 2 简化处理。
            let input_pb =
                input.filter_map(|req| async move { append_entries_request_to_pb::<C>(req).ok() });

            let response = client
                .stream_append(input_pb)
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

            let output = response.into_inner().map(|result| {
                let resp = result.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
                Self::pb_to_stream_result(resp)
            });

            Ok(Box::pin(output) as BoxStream<'s, _>)
        };

        Box::pin(fu)
    }

    /// 固定 200ms 退避（覆盖默认的 None）。
    fn backoff(&self) -> Option<Backoff> {
        Some(Backoff::new(std::iter::repeat(Duration::from_millis(200))))
    }

    /// PowerFS 暂不支持 leadership transfer；返回 Unreachable 让 openraft 走默认失败路径。
    async fn transfer_leader(
        &mut self,
        _req: TransferLeaderRequest<C>,
        _option: RPCOption,
    ) -> Result<TransferLeaderResponse<C>, RPCError<C>> {
        Err(RPCError::Unreachable(Unreachable::new(&AnyError::error(
            "transfer_leader not implemented",
        ))))
    }
}
