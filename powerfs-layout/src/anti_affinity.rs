//! 跨节点 anti-affinity 约束 (设计文档 S4.5)
//!
//! 强制要求: 副本和分条数据必须分布在不同物理节点的 volume 上.
//!
//! Volume Server 启动时向 Master 注册 node_id (物理节点标识),
//! Master 维护 volume_id -> node_id 映射,
//! Filer 选 volume 时调用 [`select_volumes_with_anti_affinity`].

use crate::error::LayoutError;
use std::collections::HashSet;

/// 物理节点标识
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NodeId(pub u32);

/// Volume 拓扑信息 (由 Master 提供)
#[derive(Clone, Debug)]
pub struct VolumeInfo {
    /// Volume ID
    pub volume_id: u64,
    /// 所在物理节点
    pub node_id: NodeId,
    /// 空闲字节
    pub free_bytes: u64,
    /// 总字节
    pub total_bytes: u64,
}

impl VolumeInfo {
    /// 空闲比例 [0.0, 1.0]
    pub fn free_ratio(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            self.free_bytes as f64 / self.total_bytes as f64
        }
    }
}

/// 选 count 个 volume, 强制分布在不同 node (anti-affinity).
///
/// 算法 (设计文档 S4.5):
/// 1. 按 node 分组, 每节点选空闲比例最大的 volume
/// 2. 每选一个 volume, 标记其 node 为已用
/// 3. 若可用 node 数 < count, 返回 InsufficientNodes 错误
///
/// `exclude_nodes` 可排除已用节点 (如副本的 data 块节点不能复用给 parity 块).
pub fn select_volumes_with_anti_affinity(
    volumes: &[VolumeInfo],
    count: usize,
    exclude_nodes: &HashSet<NodeId>,
) -> Result<Vec<u64>, LayoutError> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let mut used_nodes: HashSet<NodeId> = exclude_nodes.clone();
    let mut selected: Vec<u64> = Vec::with_capacity(count);

    for _ in 0..count {
        // 在未用 node 中选空闲比例最大的 volume
        let best = volumes
            .iter()
            .filter(|v| !used_nodes.contains(&v.node_id))
            .max_by(|a, b| {
                a.free_ratio()
                    .partial_cmp(&b.free_ratio())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

        match best {
            Some(v) => {
                selected.push(v.volume_id);
                used_nodes.insert(v.node_id.clone());
            }
            None => {
                return Err(LayoutError::InsufficientNodes {
                    need: count,
                    have: selected.len(),
                });
            }
        }
    }

    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vol(id: u64, node: u32, free: u64, total: u64) -> VolumeInfo {
        VolumeInfo {
            volume_id: id,
            node_id: NodeId(node),
            free_bytes: free,
            total_bytes: total,
        }
    }

    #[test]
    fn select_basic() {
        let vols = vec![
            vol(1, 1, 100, 200),
            vol(2, 2, 80, 200),
            vol(3, 3, 60, 200),
            vol(4, 1, 50, 200), // 同 node 1
        ];
        let result = select_volumes_with_anti_affinity(&vols, 3, &HashSet::new()).unwrap();
        assert_eq!(result.len(), 3);
        // 每个在不同 node
        let nodes: HashSet<u32> = result
            .iter()
            .map(|vid| {
                vols.iter().find(|v| v.volume_id == *vid).unwrap().node_id.0
            })
            .collect();
        assert_eq!(nodes.len(), 3);
    }

    #[test]
    fn select_prefers_higher_free() {
        let vols = vec![
            vol(1, 1, 100, 200),
            vol(2, 2, 80, 200),
        ];
        let result = select_volumes_with_anti_affinity(&vols, 2, &HashSet::new()).unwrap();
        // vol 1 空闲更多, 应该先选
        assert_eq!(result[0], 1);
        assert_eq!(result[1], 2);
    }

    #[test]
    fn select_insufficient_nodes() {
        let vols = vec![
            vol(1, 1, 100, 200),
            vol(2, 1, 80, 200), // 同 node 1
        ];
        let result = select_volumes_with_anti_affinity(&vols, 2, &HashSet::new());
        assert!(result.is_err());
        match result.unwrap_err() {
            LayoutError::InsufficientNodes { need, have } => {
                assert_eq!(need, 2);
                assert_eq!(have, 1);
            }
            _ => panic!("expected InsufficientNodes"),
        }
    }

    #[test]
    fn select_with_exclude() {
        let vols = vec![
            vol(1, 1, 100, 200),
            vol(2, 2, 80, 200),
            vol(3, 3, 60, 200),
        ];
        let mut exclude = HashSet::new();
        exclude.insert(NodeId(1));
        let result = select_volumes_with_anti_affinity(&vols, 2, &exclude).unwrap();
        // 不应包含 node 1 的 vol 1
        assert!(!result.contains(&1));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn select_zero_count() {
        let vols = vec![vol(1, 1, 100, 200)];
        let result = select_volumes_with_anti_affinity(&vols, 0, &HashSet::new()).unwrap();
        assert!(result.is_empty());
    }
}
