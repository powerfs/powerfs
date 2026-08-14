//! 多 Raft 组客户端网络层（Filer 多分片场景）。
//!
//! 实现 `openraft_multi::GroupRouter<C, String>`，为每个 Raft 组提供出站 gRPC 通信。
//! 通过 `GroupNetworkAdapter`（openraft-multi 提供）自动获得 `RaftNetworkV2` impl。
//!
//! 连接共享：所有 Raft 组共用一组 tonic `Channel`（按对端地址缓存），
//! 避免每组分片各自建立连接。
//!
//! 节点地址表：`MultiGroupRouter` 维护 `NodeId -> gRPC addr` 映射，
//! 由 `RaftGroupManagerV2` 在 `register_peer` 时填充。

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::channel::mpsc;
use futures::SinkExt;
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
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::type_config::alias::SnapshotOf;
use openraft::type_config::alias::VoteOf;
use openraft::vote::RaftLeaderId;
use openraft::vote::RaftVote;
use openraft::AnyError;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::RaftTypeConfig;
use openraft_multi::GroupNetworkAdapter;
use openraft_multi::GroupRouter;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::borrow::Borrow;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

use crate::pb_impl::append_entries_request_to_pb;
use crate::pb_impl::log_id_to_pb;
use crate::pb_impl::membership_to_pb;
use crate::pb_impl::pb_to_vote;
use crate::pb_impl::vote_to_pb;
use crate::pb_impl::CommittedLidOf;
use crate::protobuf as pb;
use crate::protobuf::raft_service_client::RaftServiceClient;
use crate::BasicNode;
use crate::SnapshotData;

/// 多 Raft 组共享路由器。
///
/// 持有节点地址表和共享 gRPC 连接池，实现 `GroupRouter<C, String>`。
/// 被 `RaftGroupManagerV2` 创建后，对每个 shard 用 `MultiNetworkFactory` 包装
/// （绑定 `group_id = shard_id`），再传给 `Raft::new()`。
#[derive(Clone)]
pub struct MultiGroupRouter {
    /// `NodeId -> gRPC addr` 映射（由 `register_node` 填充）。
    node_addrs: Arc<tokio::sync::RwLock<HashMap<String, String>>>,
    /// `gRPC addr -> Channel` 连接池（所有组共享）。
    channels: Arc<Mutex<HashMap<String, Channel>>>,
}

impl MultiGroupRouter {
    pub fn new() -> Self {
        Self {
            node_addrs: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 注册节点 `node_id -> addr` 映射。
    pub async fn register_node(&self, node_id: impl Into<String>, addr: impl Into<String>) {
        self.node_addrs
            .write()
            .await
            .insert(node_id.into(), addr.into());
    }

    /// 获取（或建立）到指定节点的 gRPC `Channel`。
    ///
    /// 返回 `AnyError`（非泛型于 C），调用方按需转成 `RPCError<C>` 或 `StreamingError<C>`。
    async fn get_channel(&self, node_id: &str) -> Result<Channel, AnyError> {
        let addr = {
            let addrs = self.node_addrs.read().await;
            addrs
                .get(node_id)
                .cloned()
                .ok_or_else(|| AnyError::error(format!("Node '{}' address not found", node_id)))?
        };

        // 先尝试从缓存取
        {
            let channels = self.channels.lock().await;
            if let Some(ch) = channels.get(&addr) {
                return Ok(ch.clone());
            }
        }

        // 建立新连接
        let url = format!("http://{}", addr);
        let channel = Endpoint::from_shared(url)
            .map_err(AnyError::error)?
            .connect()
            .await
            .map_err(AnyError::error)?;

        let mut channels = self.channels.lock().await;
        channels.insert(addr, channel.clone());
        Ok(channel)
    }

    /// 把 snapshot 数据按 1MB 分块发送到流。
    async fn send_snapshot_chunks(
        tx: &mut mpsc::Sender<pb::SnapshotRequest>,
        snapshot_data: &[u8],
    ) -> Result<(), AnyError> {
        const CHUNK_SIZE: usize = 1024 * 1024;
        for chunk in snapshot_data.chunks(CHUNK_SIZE) {
            let request = pb::SnapshotRequest {
                payload: Some(pb::snapshot_request::Payload::Chunk(chunk.to_vec())),
            };
            tx.send(request).await.map_err(AnyError::error)?;
        }
        Ok(())
    }

    /// 转发 propose 请求到指定节点（leader）。
    ///
    /// 当本地节点不是 leader 时，通过此方法将 `client_write` 请求转发到 leader 节点。
    /// `payload` 是序列化后的 `C::D`（如 `FilerRequest`，使用 `serde_json`）。
    ///
    /// 返回 `(ok, log_index, forward_leader_id)`：
    /// - 如果 `ok=true`，`log_index` 是已提交的日志索引。
    /// - 如果 `ok=false` 且 `forward_leader_id` 非空，应转发到该节点。
    /// - 如果 `ok=false` 且 `forward_leader_id` 为空，表示其他错误。
    pub async fn propose_forward(
        &self,
        target_node_id: &str,
        group_id: &str,
        payload: Vec<u8>,
    ) -> Result<(bool, u64, String, String), String> {
        let channel = self
            .get_channel(target_node_id)
            .await
            .map_err(|e| format!("failed to get channel to '{}': {}", target_node_id, e))?;
        let mut client = RaftServiceClient::new(channel);

        let pb_req = pb::ProposeRequest {
            group_id: group_id.to_string(),
            payload,
        };

        let response = client
            .propose(pb_req)
            .await
            .map_err(|e| format!("propose RPC to '{}' failed: {}", target_node_id, e))?;

        let resp = response.into_inner();
        Ok((resp.ok, resp.log_index, resp.forward_leader_id, resp.error))
    }
}

impl Default for MultiGroupRouter {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// GroupRouter impl — 出站 RPC 携带 group_id 路由到对端
// ---------------------------------------------------------------------------

impl<C> GroupRouter<C, String> for MultiGroupRouter
where
    C: RaftTypeConfig,
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

    fn append_entries(
        &self,
        target: C::NodeId,
        group_id: String,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> impl Future<Output = Result<AppendEntriesResponse<C>, RPCError<C>>> + OptionalSend {
        let router = self.clone();
        async move {
            let node_id = target.to_string();
            let channel = router
                .get_channel(&node_id)
                .await
                .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
            let mut client = RaftServiceClient::new(channel);

            let mut pb_req = append_entries_request_to_pb::<C>(rpc)
                .map_err(|e| RPCError::Network(NetworkError::new(&AnyError::error(e))))?;
            pb_req.group_id = group_id;

            let response = client
                .append_entries(pb_req)
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&AnyError::error(e))))?;

            Ok(response.into_inner().into())
        }
    }

    fn vote(
        &self,
        target: C::NodeId,
        group_id: String,
        rpc: VoteRequest<C>,
        _option: RPCOption,
    ) -> impl Future<Output = Result<VoteResponse<C>, RPCError<C>>> + OptionalSend {
        let router = self.clone();
        async move {
            let node_id = target.to_string();
            let channel = router
                .get_channel(&node_id)
                .await
                .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
            let mut client = RaftServiceClient::new(channel);

            let mut pb_req: pb::VoteRequest = rpc.into();
            pb_req.group_id = group_id;

            let response = client
                .vote(pb_req)
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&AnyError::error(e))))?;

            Ok(response.into_inner().into())
        }
    }

    fn full_snapshot(
        &self,
        target: C::NodeId,
        group_id: String,
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> impl Future<Output = Result<SnapshotResponse<C>, StreamingError<C>>> + OptionalSend {
        let router = self.clone();
        async move {
            let node_id = target.to_string();
            let channel = router
                .get_channel(&node_id)
                .await
                .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;
            let mut client = RaftServiceClient::new(channel);

            let (mut tx, rx) = mpsc::channel(1024);
            let response = client
                .snapshot(rx)
                .await
                .map_err(|e| StreamingError::Network(NetworkError::new(&AnyError::error(e))))?;

            // meta chunk（含 group_id）
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
                        group_id,
                    },
                )),
            };
            tx.send(meta_request)
                .await
                .map_err(|e| StreamingError::Network(NetworkError::new(&AnyError::error(e))))?;

            // data chunks
            let snapshot_bytes = snapshot.snapshot.into_inner();
            Self::send_snapshot_chunks(&mut tx, &snapshot_bytes)
                .await
                .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;

            // 等待响应
            let message = response.into_inner();
            let resp_vote = message.vote.ok_or_else(|| {
                StreamingError::Network(NetworkError::new(&AnyError::error(
                    "Missing `vote` in snapshot response",
                )))
            })?;

            Ok(SnapshotResponse {
                vote: pb_to_vote::<C>(resp_vote),
            })
        }
    }

    fn backoff(&self) -> Option<Backoff> {
        Some(Backoff::new(std::iter::repeat(Duration::from_millis(200))))
    }
}

// ---------------------------------------------------------------------------
// MultiNetworkFactory — RaftNetworkFactory impl
// ---------------------------------------------------------------------------

/// 绑定 `MultiGroupRouter` + `group_id` 的网络工厂。
///
/// 对每个 Raft 组创建一个 `MultiNetworkFactory` 实例，
/// 传给 `Raft::new()`。`new_client` 返回 `GroupNetworkAdapter`，
/// 它自动 impl `RaftNetworkV2`。
pub struct MultiNetworkFactory {
    pub router: MultiGroupRouter,
    pub group_id: String,
}

impl MultiNetworkFactory {
    pub fn new(router: MultiGroupRouter, group_id: impl Into<String>) -> Self {
        Self {
            router,
            group_id: group_id.into(),
        }
    }
}

impl<C> RaftNetworkFactory<C> for MultiNetworkFactory
where
    C: RaftTypeConfig,
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
    MultiGroupRouter: GroupRouter<C, String, SnapshotData = SnapshotData>,
{
    type Network = GroupNetworkAdapter<C, String, MultiGroupRouter>;

    async fn new_client(&mut self, target: C::NodeId, _node: &C::Node) -> Self::Network {
        GroupNetworkAdapter::new(self.router.clone(), target, self.group_id.clone())
    }
}
