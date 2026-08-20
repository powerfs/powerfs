//! Centralized debug configuration store.
//!
//! Master 维护全局 + 节点级调试配置，通过 HTTP `/admin/debug` 端点接收
//! 配置变更，通过 TLV `GetDebugConfig` 协议下发到各节点。
//!
//! ## 配置合并规则
//!
//! 1. "all" 节点的配置作为全局默认
//! 2. 具体节点的配置覆盖 "all"
//! 3. 未设置的字段（None）不覆盖
//!
//! ## 使用示例
//!
//! ```ignore
//! // 全局开 debug
//! curl -X PUT master:9300/admin/debug -d '{"node":"all","level":"debug"}'
//!
//! // 只看 fuse-1 的 create 耗时
//! curl -X PUT master:9300/admin/debug -d '{"node":"fuse-1","flag":"fuse_create_timing","on":true}'
//!
//! // 查看所有配置
//! curl master:9300/admin/debug
//! ```

use dashmap::DashMap;
use log::info;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// 单个节点的调试配置（存储视图，字段都是 Option 表示是否显式设置）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeDebugConfig {
    /// 日志级别: "off"|"error"|"warn"|"info"|"debug"|"trace"，None 表示不修改
    pub log_level: Option<String>,
    /// Target 过滤: "powerfs_fuse::fuse" 等，None 表示不修改，Some("") 表示清除
    pub target_filter: Option<String>,
    /// 子系统调试开关: name → on/off
    pub flags: std::collections::HashMap<String, bool>,
}

/// HTTP 请求 body: PUT /admin/debug
#[derive(Debug, Deserialize)]
pub struct DebugConfigUpdate {
    /// 目标节点: "all" | "fuse-1" | "filer-2" | ...
    pub node: String,
    /// 设置日志级别（可选，不传则不修改）
    pub level: Option<String>,
    /// 设置 target 过滤（可选）
    pub target_filter: Option<String>,
    /// 设置单个开关（可选，配合 on）
    pub flag: Option<String>,
    /// 开关值（可选，配合 flag）
    pub on: Option<bool>,
}

/// 集中式调试配置存储。
///
/// 线程安全（DashMap），所有节点共享同一个 Arc 实例。
#[derive(Clone)]
pub struct DebugConfigStore {
    /// node_id → config。"all" 是全局默认。
    configs: Arc<DashMap<String, NodeDebugConfig>>,
}

impl DebugConfigStore {
    pub fn new() -> Self {
        Self {
            configs: Arc::new(DashMap::new()),
        }
    }

    /// 应用一个更新请求（来自 HTTP PUT /admin/debug）
    pub fn apply_update(&self, update: DebugConfigUpdate) -> NodeDebugConfig {
        let mut entry = self
            .configs
            .entry(update.node.clone())
            .or_default()
            .clone();

        if let Some(level) = update.level {
            entry.log_level = Some(level);
        }
        if let Some(filter) = update.target_filter {
            entry.target_filter = Some(filter);
        }
        if let (Some(flag), Some(on)) = (update.flag, update.on) {
            entry.flags.insert(flag, on);
        }

        self.configs.insert(update.node.clone(), entry.clone());
        info!(
            "DEBUG_CONFIG: updated node='{}' level={:?} filter={:?} flags={}",
            update.node,
            entry.log_level,
            entry.target_filter,
            entry.flags.len()
        );
        entry
    }

    /// 获取节点的有效配置（合并 "all" 默认 + 节点覆盖）
    ///
    /// 用于 GetDebugConfig 响应。返回 powerfs_net::serialize::DebugConfig。
    pub fn effective_config(&self, node_id: &str) -> powerfs_net::serialize::DebugConfig {
        let mut level = None;
        let mut filter = None;
        let mut flags: std::collections::HashMap<String, bool> =
            std::collections::HashMap::new();

        // 先应用 "all" 默认
        if let Some(all_cfg) = self.configs.get("all") {
            level = all_cfg.log_level.clone();
            filter = all_cfg.target_filter.clone();
            flags = all_cfg.flags.clone();
        }

        // 再用节点特定配置覆盖
        if node_id != "all" {
            if let Some(node_cfg) = self.configs.get(node_id) {
                if node_cfg.log_level.is_some() {
                    level = node_cfg.log_level.clone();
                }
                if node_cfg.target_filter.is_some() {
                    filter = node_cfg.target_filter.clone();
                }
                for (k, v) in &node_cfg.flags {
                    flags.insert(k.clone(), *v);
                }
            }
        }

        let flags_vec: Vec<(String, bool)> = flags.into_iter().collect();
        powerfs_net::serialize::DebugConfig {
            log_level: level,
            target_filter: filter,
            flags: flags_vec,
        }
    }

    /// 列出所有节点的配置（用于 HTTP GET /admin/debug）
    pub fn list_all(&self) -> Vec<(String, NodeDebugConfig)> {
        let mut result: Vec<(String, NodeDebugConfig)> = self
            .configs
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        result.sort_by(|a, b| {
            // "all" 排最前
            if a.0 == "all" {
                std::cmp::Ordering::Less
            } else if b.0 == "all" {
                std::cmp::Ordering::Greater
            } else {
                a.0.cmp(&b.0)
            }
        });
        result
    }

    /// 清除指定节点的配置（HTTP DELETE /admin/debug?node=xxx）
    pub fn clear(&self, node_id: &str) -> bool {
        self.configs.remove(node_id).is_some()
    }
}

impl Default for DebugConfigStore {
    fn default() -> Self {
        Self::new()
    }
}
