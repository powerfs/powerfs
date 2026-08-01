//! filer 端 CrdtCoherenceAuthority 框架。
//!
//! 具体实现在 `powerfs-filer` crate 中（需访问 MetaShardManager），
//! 这里仅提供 trait 引导与 net_handler 复用的转换工具。

use crate::{DeltaWire, VectorClockWire};

/// DeltaWire → filer proto DeltaOp 的转换辅助（filer net_handler 调用）。
///
/// filer 端的 `crate::powerfs::DeltaOp` 是 prost 生成的 proto 类型，
/// 此函数将中性 DeltaWire 转换为 filer 可用的 proto DeltaOp 构造参数。
///
/// 注意：filer crate 自己实现 DeltaWire→proto 的转换（因为 proto 类型在 filer crate 内），
/// 这里只提供 dir_ino 提取工具。
pub fn extract_dir_ino_from_wire(delta: &DeltaWire) -> Option<u64> {
    use crate::DeltaOpType;
    match delta.op_type {
        DeltaOpType::Add | DeltaOpType::Rename => delta.entry.as_ref().map(|e| e.parent_ino),
        DeltaOpType::Remove => delta.entry_id.as_ref().map(|e| e.parent_ino),
        DeltaOpType::SetAttr => delta.setattr.as_ref().map(|s| s.inode),
    }
}

/// 从 wire vclock 提取 (client_id, seq) 列表（filer 端构造 proto VectorClock 用）
pub fn vclock_entries(wire: &VectorClockWire) -> Vec<(u64, u64)> {
    wire.entries.iter().map(|e| (e.client_id, e.seq)).collect()
}
