//! 流控 HTTP API 数据层 (Phase 1 S4)
//!
//! 提供 JSON 序列化的流控快照, 供服务层 axum handler 调用.
//! 不依赖 axum, 仅依赖 serde_json, 可被所有服务 (volume/master/filer) 复用.
//!
//! # 端点设计
//!
//! | 方法 | 路径 | 说明 |
//! |------|------|------|
//! | GET | /admin/flow/overview | 总览 (policy + global + counts) |
//! | GET | /admin/flow/connections | 所有连接统计 |
//! | GET | /admin/flow/global | 全局统计 (含延迟直方图) |
//! | GET | /admin/flow/slow | 慢连接列表 |
//! | GET | /admin/flow/policy | 策略信息 (name + load_factor) |
//!
//! # 服务层集成示例
//!
//! ```ignore
//! use axum::{routing::get, Router};
//! use powerfs_net::flow_admin;
//!
//! let flow_app = Router::new()
//!     .route("/admin/flow/overview", get(|| async {
//!         axum::Json(flow_admin::overview_json(&fc))
//!     }))
//!     .with_state(fc);
//! ```

use serde_json::{json, Value};

use crate::flow_control::FlowController;

/// 流控总览 (policy + global + counts)
///
/// 适合 dashboard 首页, 一次请求获取关键指标.
pub fn overview_json(fc: &FlowController) -> Value {
    let conns = fc.snapshot_connections();
    let slow_count = conns.iter().filter(|c| c.slow).count();
    json!({
        "policy": {
            "name": fc.policy_name(),
            "load_factor": fc.current_load_factor(),
        },
        "global": fc.snapshot_global(),
        "connections_count": conns.len(),
        "slow_count": slow_count,
    })
}

/// 所有连接的流控统计 (按 conn_id 排序)
pub fn connections_json(fc: &FlowController) -> Value {
    let conns = fc.snapshot_connections();
    json!({
        "count": conns.len(),
        "connections": conns,
    })
}

/// 全局流控统计 (含 6 桶延迟直方图 + slow_conns 计数)
pub fn global_json(fc: &FlowController) -> Value {
    json!(fc.snapshot_global())
}

/// 慢连接列表 (只返回 slow=true 的连接)
pub fn slow_connections_json(fc: &FlowController) -> Value {
    let slow: Vec<_> = fc
        .snapshot_connections()
        .into_iter()
        .filter(|c| c.slow)
        .collect();
    json!({
        "count": slow.len(),
        "slow_connections": slow,
    })
}

/// 策略信息 (name + load_factor)
///
/// load_factor (0-3) 反映服务器当前负载, 基于 global active_reqs / max_active_global
/// 比率计算. Worker 在发送响应时将此值 stamp 到帧头 flags bits 6-7.
pub fn policy_json(fc: &FlowController) -> Value {
    json!({
        "name": fc.policy_name(),
        "load_factor": fc.current_load_factor(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow_control::{Channel, FlowController};

    #[test]
    fn test_overview_json_empty() {
        let fc = FlowController::with_defaults();
        let v = overview_json(&fc);
        assert_eq!(v["policy"]["name"], "none");
        assert_eq!(v["policy"]["load_factor"], 0);
        assert_eq!(v["connections_count"], 0);
        assert_eq!(v["slow_count"], 0);
        assert!(v["global"].is_object());
    }

    #[test]
    fn test_overview_json_with_conn() {
        let fc = FlowController::with_defaults();
        fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);
        fc.register_conn(2, "127.0.0.1:1002".into(), Channel::Meta);
        let v = overview_json(&fc);
        assert_eq!(v["connections_count"], 2);
        assert_eq!(v["slow_count"], 0);
    }

    #[test]
    fn test_connections_json() {
        let fc = FlowController::with_defaults();
        fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);
        fc.register_conn(2, "127.0.0.1:1002".into(), Channel::Meta);
        let v = connections_json(&fc);
        assert_eq!(v["count"], 2);
        assert!(v["connections"].is_array());
        let arr = v["connections"].as_array().unwrap();
        assert_eq!(arr.len(), 2);
        // snapshot_connections 按 conn_id 排序
        assert_eq!(arr[0]["conn_id"], 1);
        assert_eq!(arr[1]["conn_id"], 2);
        assert_eq!(arr[0]["channel"], "data");
        assert_eq!(arr[1]["channel"], "meta");
    }

    #[test]
    fn test_global_json() {
        let fc = FlowController::with_defaults();
        let v = global_json(&fc);
        assert!(v.is_object());
        assert_eq!(v["total_reqs"], 0);
        assert_eq!(v["active_reqs"], 0);
        assert_eq!(v["slow_conns"], 0);
        assert!(v["lat_buckets"].is_array());
        assert_eq!(v["lat_buckets"].as_array().unwrap().len(), 6);
    }

    #[test]
    fn test_slow_connections_json_empty() {
        let fc = FlowController::with_defaults();
        fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);
        let v = slow_connections_json(&fc);
        assert_eq!(v["count"], 0);
        assert!(v["slow_connections"].is_array());
    }

    #[test]
    fn test_slow_connections_json_with_slow() {
        let fc = FlowController::with_defaults();
        let stats = fc.register_conn(1, "127.0.0.1:1001".into(), Channel::Data);
        stats.slow.store(true, std::sync::atomic::Ordering::Relaxed);
        let v = slow_connections_json(&fc);
        assert_eq!(v["count"], 1);
        let arr = v["slow_connections"].as_array().unwrap();
        assert_eq!(arr[0]["conn_id"], 1);
        assert_eq!(arr[0]["slow"], true);
    }

    #[test]
    fn test_policy_json_no_policy() {
        let fc = FlowController::with_defaults();
        let v = policy_json(&fc);
        assert_eq!(v["name"], "none");
        assert_eq!(v["load_factor"], 0);
    }

    #[test]
    fn test_policy_json_with_default_policy() {
        let fc = FlowController::with_defaults();
        fc.set_default_policy();
        let v = policy_json(&fc);
        assert_eq!(v["name"], "adaptive-concurrency");
        // 0 active_reqs → load_factor=0 (idle)
        assert_eq!(v["load_factor"], 0);
    }
}
