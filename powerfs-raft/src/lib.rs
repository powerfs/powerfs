//! PowerFS 共识层（基于 openraft 0.10）。
//!
//! 替代 raft-rs 0.7 的 `RawNode`/`Ready` 循环模型，改用 openraft 的异步自驱动
//! `Raft<C>` + `RaftLogStorage`/`RaftStateMachine`/`RaftNetwork` trait 体系。
//!
//! 参考：`openraft/examples/raft-kv-memstore-grpc/`（gRPC + proto 映射的范本）。
//!
//! 迁移阶段：
//! - 阶段 0（当前）：依赖接入 + TypeConfig 声明 + crate 骨架。
//! - 阶段 1：`store` 模块实现 `RaftLogStorage`/`RaftStateMachine`（RocksDB）。
//! - 阶段 2：`network`/`grpc`/`protobuf`/`pb_impl` 实现 gRPC 传输。
//! - 阶段 3：Master 接入。
//! - 阶段 4：Filer 多组接入（openraft-multi）。
//!
//! 详见 `docs/openraft-migration-plan.md`。

#![allow(clippy::uninlined_format_args)]

// ---- 阶段 2：proto 生成 ----
pub mod protobuf {
    #![allow(clippy::result_large_err)]
    tonic::include_proto!("powerfs_raftpb");
}

pub mod grpc;
pub mod multi;
pub mod multi_network;
pub mod network;
pub mod pb_impl;
pub mod store;

use std::fmt;
use std::io::Cursor;

// 重新导出 openraft 的公共类型，方便上层（含集成测试）引用。
pub use openraft::BasicNode;
pub use openraft::Raft;

// =============================================================================
// TypeConfig
// =============================================================================

// Master 共识组的类型配置。
//
// - `D`/`R`：业务写入请求/响应（zone/node 增删、配置变更等），阶段 3 定义具体结构。
// - `NodeId`：复用 `String`（与 powerfs 的 `NodeId(pub String)` 在边界处互转）。
// - `Node`：`BasicNode`（仅带 `addr`，足以路由 RPC）。
// - 其余（`Term`/`LeaderId`/`Vote`/`Entry`/`AsyncRuntime` 等）走 openraft 默认值
//   （`AsyncRuntime` 默认为 `TokioRuntime`，由 `tokio-rt` feature 提供）。
openraft::declare_raft_types!(
    pub MasterTypeConfig:
        D = MasterRequest,
        R = MasterResponse,
        NodeId = String,
        Node = BasicNode,
);

// Filer 共识组类型配置（阶段 4 启用，多组通过 openraft-multi 共享连接）。
//
// D/R 为分片元数据操作（inode/shard 增删等），与 Master 不同，故单独一份 TypeConfig。
openraft::declare_raft_types!(
    pub FilerTypeConfig:
        D = FilerRequest,
        R = FilerResponse,
        NodeId = String,
        Node = BasicNode,
);

// =============================================================================
// 业务数据类型（占位，阶段 3/4 替换为具体业务结构）
// =============================================================================

/// Master 业务写入请求占位类型。
///
/// TODO(阶段 3)：替换为实际的 Master 元数据操作枚举
/// （AddZone / RemoveZone / AddNode / RemoveNode / UpdateNodeConfig 等）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MasterRequest {
    /// 占位 payload，阶段 3 替换。
    pub payload: Vec<u8>,
}

impl fmt::Display for MasterRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MasterRequest({} bytes)", self.payload.len())
    }
}

/// Master 业务写入响应占位类型。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct MasterResponse {
    pub ok: bool,
    pub message: String,
}

/// Filer 业务写入请求占位类型。
///
/// TODO(阶段 4)：替换为分片元数据操作枚举
/// （CreateInode / DeleteInode / SetAttr / Link / Unlink / Xattr 等）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FilerRequest {
    pub payload: Vec<u8>,
}

impl fmt::Display for FilerRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FilerRequest({} bytes)", self.payload.len())
    }
}

/// Filer 业务写入响应占位类型。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct FilerResponse {
    pub ok: bool,
    pub message: String,
}

// =============================================================================
// 类型别名（参考 openraft/examples/utils/declare_types.rs）
// =============================================================================

/// 快照数据载体：内存 bytes 流。RocksDB 快照序列化到此。
pub type SnapshotData = Cursor<Vec<u8>>;

/// Master Raft 实例类型。
///
/// 第二个泛型 `SM` 是状态机句柄类型，阶段 1 落地 `RaftStateMachine` 后替换为具体类型。
pub type MasterRaft = Raft<MasterTypeConfig>;

/// Filer Raft 实例类型（阶段 4）。
pub type FilerRaft = Raft<FilerTypeConfig>;

/// Master 日志条目类型。
pub type MasterEntry = <MasterTypeConfig as openraft::RaftTypeConfig>::Entry;
/// Filer 日志条目类型。
pub type FilerEntry = <FilerTypeConfig as openraft::RaftTypeConfig>::Entry;

// =============================================================================
// 配置
// =============================================================================

/// 构建适用于 PowerFS 的 openraft `Config`。
///
/// 心跳/超时沿用 raft-rs 时期的默认量级，迁移期保持参数一致以便对比行为。
pub fn default_config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 250,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证 TypeConfig 声明能编译、类型别名可用。
    #[test]
    fn type_config_compiles() {
        let _cfg = default_config();
        // 仅验证类型存在，不实例化 Raft（需 store/network，阶段 1-2 才有）
        let _req = MasterRequest { payload: vec![] };
        let _resp = MasterResponse {
            ok: true,
            message: String::new(),
        };
        assert_eq!(_req.payload.len(), 0);
        assert!(_resp.ok);
    }
}
