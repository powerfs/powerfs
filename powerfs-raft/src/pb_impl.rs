//! proto 类型 ↔ openraft 类型转换。
//!
//! 参考 `openraft/examples/raft-kv-memstore-grpc/src/pb_impl/`，
//! 适配 PowerFS：NodeId = String，Entry.app_data = bytes（serde 序列化 C::D）。
//!
//! 泛型于 `C: RaftTypeConfig`，同一份代码服务 Master + Filer。

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use openraft::entry::RaftEntry;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::StreamAppendError;
use openraft::raft::StreamAppendResult;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::type_config::alias::EntryOf;
use openraft::type_config::alias::LogIdOf;
use openraft::type_config::alias::VoteOf;
use openraft::vote::RaftLeaderId;
use openraft::vote::RaftVote;
use openraft::EntryPayload;
use openraft::Membership;
use openraft::RaftTypeConfig;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::protobuf as pb;
use crate::BasicNode;

// ---------------------------------------------------------------------------
// pb::SnapshotRequest 辅助方法（拆分 meta / data chunk）
// ---------------------------------------------------------------------------

impl pb::SnapshotRequest {
    /// 若本 item 是 meta chunk，返回 `Some(SnapshotRequestMeta)`；否则 `None`。
    pub fn into_meta(self) -> Option<pb::SnapshotRequestMeta> {
        match self.payload? {
            pb::snapshot_request::Payload::Meta(meta) => Some(meta),
            pb::snapshot_request::Payload::Chunk(_) => None,
        }
    }

    /// 若本 item 是 data chunk，返回 `Some(Vec<u8>)`；否则 `None`。
    pub fn into_data_chunk(self) -> Option<Vec<u8>> {
        match self.payload? {
            pb::snapshot_request::Payload::Meta(_) => None,
            pb::snapshot_request::Payload::Chunk(chunk) => Some(chunk),
        }
    }
}

/// PowerFS 中 `C::LeaderId::Committed` 也实现 `RaftLeaderId`（默认 `LeaderIdAdv` 满足此条件）。
///
/// 由于 `RaftLeaderId::Committed` 关联类型只声明了 `RaftCommittedLeaderId` bound，
/// 编译器无法自动推导 `Committed: RaftLeaderId`。PowerFS 使用 openraft 默认的
/// `LeaderIdAdv<Term, NodeId>`，其中 `Committed = Self`，所以此 bound 恒成立。
pub type CommittedLidOf<C> = <<C as RaftTypeConfig>::LeaderId as RaftLeaderId>::Committed;

// ---------------------------------------------------------------------------
// LeaderId / LogId / Vote
// ---------------------------------------------------------------------------

pub(crate) fn pb_to_leader_id<C>(pb: pb::LeaderId) -> C::LeaderId
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Term: From<u64>,
    C::NodeId: From<String>,
{
    C::LeaderId::new(pb.term.into(), pb.node_id.into())
}

/// 泛型于任何 `RaftLeaderId`（接受 `C::LeaderId` 或 `C::CommittedLeaderId`）。
pub(crate) fn leader_id_to_pb<LID>(leader_id: &LID) -> pb::LeaderId
where
    LID: RaftLeaderId,
    LID::Term: Into<u64>,
{
    pb::LeaderId {
        term: leader_id.term().into(),
        node_id: leader_id.node_id().to_string(),
    }
}

pub(crate) fn pb_to_log_id<C>(pb: Option<pb::LogId>) -> Option<LogIdOf<C>>
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Term: From<u64>,
    C::NodeId: From<String>,
{
    pb.map(|log_id| {
        let leader_id = pb_to_leader_id::<C>(log_id.leader_id.unwrap_or_default());
        LogIdOf::<C>::new(leader_id.to_committed(), log_id.index)
    })
}

pub(crate) fn log_id_to_pb<C>(log_id: &LogIdOf<C>) -> pb::LogId
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    // PowerFS 使用 LeaderIdAdv，其 Committed = Self，故也实现 RaftLeaderId。
    CommittedLidOf<C>: RaftLeaderId,
    <CommittedLidOf<C> as RaftLeaderId>::Term: Into<u64>,
{
    pb::LogId {
        leader_id: Some(leader_id_to_pb(log_id.committed_leader_id())),
        index: log_id.index(),
    }
}

pub(crate) fn pb_to_vote<C>(pb: pb::Vote) -> VoteOf<C>
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Term: From<u64>,
    C::NodeId: From<String>,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
{
    let leader_id = pb_to_leader_id::<C>(pb.leader_id.unwrap_or_default());
    VoteOf::<C>::from_leader_id(leader_id, pb.committed)
}

pub(crate) fn vote_to_pb<C>(vote: &VoteOf<C>) -> pb::Vote
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
    C::Term: Into<u64>,
{
    pb::Vote {
        leader_id: Some(leader_id_to_pb(vote.leader_id())),
        committed: vote.is_committed(),
    }
}

// ---------------------------------------------------------------------------
// Membership / Node
// ---------------------------------------------------------------------------

fn pb_to_node_id_set<NID>(pb: pb::NodeIdSet) -> BTreeSet<NID>
where
    NID: From<String> + Ord,
{
    pb.node_ids.keys().map(|id| id.clone().into()).collect()
}

fn node_id_set_to_pb<NID: ToString>(set: &BTreeSet<NID>) -> pb::NodeIdSet {
    pb::NodeIdSet {
        node_ids: set.iter().map(|id| (id.to_string(), true)).collect(),
    }
}

pub(crate) fn pb_to_membership<C>(pb: pb::Membership) -> Membership<C::NodeId, C::Node>
where
    C: RaftTypeConfig,
    C::NodeId: From<String> + Ord,
    C::Node: From<BasicNode>,
{
    let configs = pb
        .configs
        .into_iter()
        .map(pb_to_node_id_set::<C::NodeId>)
        .collect::<Vec<_>>();

    let nodes: BTreeMap<C::NodeId, C::Node> = pb
        .nodes
        .into_iter()
        .map(|(id, node)| {
            (
                id.into(),
                C::Node::from(BasicNode {
                    addr: node.rpc_addr,
                }),
            )
        })
        .collect();

    Membership::new(configs, nodes).expect("invalid membership from proto")
}

pub(crate) fn membership_to_pb<C>(membership: &Membership<C::NodeId, C::Node>) -> pb::Membership
where
    C: RaftTypeConfig,
    C::NodeId: ToString,
    C::Node: std::borrow::Borrow<BasicNode>,
{
    let configs = membership
        .get_joint_config()
        .iter()
        .map(node_id_set_to_pb::<C::NodeId>)
        .collect();

    let nodes = membership
        .nodes()
        .map(|(id, node)| {
            (
                id.to_string(),
                pb::Node {
                    node_id: String::new(),
                    rpc_addr: node.borrow().addr.clone(),
                },
            )
        })
        .collect();

    pb::Membership { configs, nodes }
}

// ---------------------------------------------------------------------------
// Entry（用 serde 序列化 D）
// ---------------------------------------------------------------------------

fn pb_to_entry<C>(pb: pb::Entry) -> Result<EntryOf<C>, std::io::Error>
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Term: From<u64>,
    C::NodeId: From<String> + Ord,
    C::Node: From<BasicNode>,
    C::D: DeserializeOwned,
    C::Entry: RaftEntry<D = C::D>,
{
    let leader_id = pb_to_leader_id::<C>(pb.leader_id.unwrap_or_default());
    let log_id = LogIdOf::<C>::new(leader_id.to_committed(), pb.index);

    let payload = if let Some(membership) = pb.membership {
        EntryPayload::Membership(pb_to_membership::<C>(membership))
    } else if !pb.app_data.is_empty() {
        let d: C::D = serde_json::from_slice(&pb.app_data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        EntryPayload::Normal(d)
    } else {
        EntryPayload::Blank
    };

    Ok(EntryOf::<C>::new(log_id, payload))
}

fn entry_to_pb<C>(entry: &EntryOf<C>) -> Result<pb::Entry, std::io::Error>
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
    C::Term: Into<u64>,
    C::NodeId: ToString,
    C::D: Serialize,
    C::Node: Borrow<BasicNode>,
    C::Entry: RaftEntry + AsRef<openraft::Entry<CommittedLidOf<C>, C::D, C::NodeId, C::Node>>,
    CommittedLidOf<C>: RaftLeaderId,
    <CommittedLidOf<C> as RaftLeaderId>::Term: Into<u64>,
{
    let log_id = entry.log_id();
    let leader_id = leader_id_to_pb(log_id.committed_leader_id());
    let index = log_id.index();

    let inner = entry.as_ref();

    let mut app_data = Vec::new();
    let mut membership = None;

    match &inner.payload {
        EntryPayload::Blank => {}
        EntryPayload::Normal(d) => {
            app_data = serde_json::to_vec(d)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        }
        EntryPayload::Membership(mem) => {
            membership = Some(membership_to_pb::<C>(mem));
        }
    }

    Ok(pb::Entry {
        leader_id: Some(leader_id),
        index,
        app_data,
        membership,
    })
}

// ---------------------------------------------------------------------------
// RPC 请求/响应转换
// ---------------------------------------------------------------------------

impl<C> From<pb::VoteRequest> for VoteRequest<C>
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Term: From<u64>,
    C::NodeId: From<String>,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
{
    fn from(pb: pb::VoteRequest) -> Self {
        VoteRequest {
            vote: pb_to_vote::<C>(pb.vote.unwrap_or_default()),
            last_log_id: pb_to_log_id::<C>(pb.last_log_id),
            leadership_transfer: pb.leadership_transfer,
        }
    }
}

impl<C> From<VoteRequest<C>> for pb::VoteRequest
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
    C::Term: Into<u64>,
    CommittedLidOf<C>: RaftLeaderId,
    <CommittedLidOf<C> as RaftLeaderId>::Term: Into<u64>,
{
    fn from(req: VoteRequest<C>) -> Self {
        pb::VoteRequest {
            vote: Some(vote_to_pb::<C>(&req.vote)),
            last_log_id: req.last_log_id.as_ref().map(log_id_to_pb::<C>),
            leadership_transfer: req.leadership_transfer,
            group_id: String::new(),
        }
    }
}

impl<C> From<pb::VoteResponse> for VoteResponse<C>
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Term: From<u64>,
    C::NodeId: From<String>,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
{
    fn from(pb: pb::VoteResponse) -> Self {
        VoteResponse {
            vote: pb_to_vote::<C>(pb.vote.unwrap_or_default()),
            vote_granted: pb.vote_granted,
            last_log_id: pb_to_log_id::<C>(pb.last_log_id),
        }
    }
}

impl<C> From<VoteResponse<C>> for pb::VoteResponse
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
    C::Term: Into<u64>,
    CommittedLidOf<C>: RaftLeaderId,
    <CommittedLidOf<C> as RaftLeaderId>::Term: Into<u64>,
{
    fn from(resp: VoteResponse<C>) -> Self {
        pb::VoteResponse {
            vote: Some(vote_to_pb::<C>(&resp.vote)),
            vote_granted: resp.vote_granted,
            last_log_id: resp.last_log_id.as_ref().map(log_id_to_pb::<C>),
        }
    }
}

pub fn pb_to_append_entries_request<C>(
    pb: pb::AppendEntriesRequest,
) -> Result<AppendEntriesRequest<C>, std::io::Error>
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Term: From<u64>,
    C::NodeId: From<String> + Ord,
    C::Node: From<BasicNode>,
    C::D: DeserializeOwned,
    C::Entry: RaftEntry<D = C::D>,
{
    let entries = pb
        .entries
        .into_iter()
        .map(|e| pb_to_entry::<C>(e))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(AppendEntriesRequest {
        vote: pb_to_vote::<C>(pb.vote.unwrap_or_default()),
        prev_log_id: pb_to_log_id::<C>(pb.prev_log_id),
        entries,
        leader_commit: pb_to_log_id::<C>(pb.leader_commit),
    })
}

pub fn append_entries_request_to_pb<C>(
    req: AppendEntriesRequest<C>,
) -> Result<pb::AppendEntriesRequest, std::io::Error>
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
    C::Term: Into<u64>,
    C::NodeId: ToString,
    C::D: Serialize,
    C::Node: Borrow<BasicNode>,
    C::Entry: RaftEntry + AsRef<openraft::Entry<CommittedLidOf<C>, C::D, C::NodeId, C::Node>>,
    CommittedLidOf<C>: RaftLeaderId,
    <CommittedLidOf<C> as RaftLeaderId>::Term: Into<u64>,
{
    let entries = req
        .entries
        .iter()
        .map(|e| entry_to_pb::<C>(e))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(pb::AppendEntriesRequest {
        vote: Some(vote_to_pb::<C>(&req.vote)),
        prev_log_id: req.prev_log_id.as_ref().map(log_id_to_pb::<C>),
        entries,
        leader_commit: req.leader_commit.as_ref().map(log_id_to_pb::<C>),
        group_id: String::new(),
    })
}

impl<C> From<AppendEntriesResponse<C>> for pb::AppendEntriesResponse
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
    C::Term: Into<u64>,
    CommittedLidOf<C>: RaftLeaderId,
    <CommittedLidOf<C> as RaftLeaderId>::Term: Into<u64>,
{
    fn from(resp: AppendEntriesResponse<C>) -> Self {
        match resp {
            AppendEntriesResponse::Success => pb::AppendEntriesResponse {
                rejected_by: None,
                conflict: false,
                last_log_id: None,
            },
            AppendEntriesResponse::PartialSuccess(log_id) => pb::AppendEntriesResponse {
                rejected_by: None,
                conflict: false,
                last_log_id: log_id.as_ref().map(log_id_to_pb::<C>),
            },
            AppendEntriesResponse::Conflict => pb::AppendEntriesResponse {
                rejected_by: None,
                conflict: true,
                last_log_id: None,
            },
            AppendEntriesResponse::HigherVote(vote) => pb::AppendEntriesResponse {
                rejected_by: Some(vote_to_pb::<C>(&vote)),
                conflict: false,
                last_log_id: None,
            },
        }
    }
}

impl<C> From<pb::AppendEntriesResponse> for AppendEntriesResponse<C>
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Term: From<u64>,
    C::NodeId: From<String>,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
{
    fn from(pb: pb::AppendEntriesResponse) -> Self {
        if let Some(rejected_by) = pb.rejected_by {
            return AppendEntriesResponse::HigherVote(pb_to_vote::<C>(rejected_by));
        }

        if pb.conflict {
            return AppendEntriesResponse::Conflict;
        }

        if let Some(last_log_id) = pb_to_log_id::<C>(pb.last_log_id) {
            return AppendEntriesResponse::PartialSuccess(Some(last_log_id));
        }

        AppendEntriesResponse::Success
    }
}

/// `StreamAppendResult -> pb::AppendEntriesResponse`：服务端把 `Raft::stream_append` 的输出
/// 转成 gRPC 流的 item。
///
/// 注意：StreamAppend 协议中，`Conflict` 必须把冲突 log id 放在 `last_log_id`，
/// 客户端 `network::pb_to_stream_result` 据此还原 `StreamAppendError::Conflict`。
impl<C> From<StreamAppendResult<C>> for pb::AppendEntriesResponse
where
    C: RaftTypeConfig,
    C::LeaderId: RaftLeaderId,
    C::Vote: RaftVote<LeaderId = C::LeaderId>,
    C::Term: Into<u64>,
    CommittedLidOf<C>: RaftLeaderId,
    <CommittedLidOf<C> as RaftLeaderId>::Term: Into<u64>,
{
    fn from(result: StreamAppendResult<C>) -> Self {
        match result {
            Ok(Some(log_id)) => pb::AppendEntriesResponse {
                rejected_by: None,
                conflict: false,
                last_log_id: Some(log_id_to_pb::<C>(&log_id)),
            },
            Ok(None) => pb::AppendEntriesResponse {
                rejected_by: None,
                conflict: false,
                last_log_id: None,
            },
            Err(StreamAppendError::Conflict(log_id)) => pb::AppendEntriesResponse {
                rejected_by: None,
                conflict: true,
                last_log_id: Some(log_id_to_pb::<C>(&log_id)),
            },
            Err(StreamAppendError::HigherVote(vote)) => pb::AppendEntriesResponse {
                rejected_by: Some(vote_to_pb::<C>(&vote)),
                conflict: false,
                last_log_id: None,
            },
        }
    }
}
