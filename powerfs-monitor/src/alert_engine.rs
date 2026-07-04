use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use crate::event::{AlertCondition, AlertInfo, AlertRule, NotificationConfig};
use crate::metric_store::MetricStoreRef;

pub struct AlertEngine {
    rules: RwLock<HashMap<String, AlertRule>>,
    alerts: RwLock<HashMap<String, AlertInfo>>,
    metric_store: MetricStoreRef,
    pending_alerts: RwLock<HashMap<String, Instant>>,
}

impl AlertEngine {
    pub fn new(metric_store: MetricStoreRef) -> Self {
        Self {
            rules: RwLock::new(HashMap::new()),
            alerts: RwLock::new(HashMap::new()),
            metric_store,
            pending_alerts: RwLock::new(HashMap::new()),
        }
    }

    pub async fn add_rule(&self, rule: AlertRule) {
        let mut rules = self.rules.write().await;
        rules.insert(rule.id.clone(), rule);
    }

    pub async fn update_rule(&self, rule: AlertRule) {
        let mut rules = self.rules.write().await;
        rules.insert(rule.id.clone(), rule);
    }

    pub async fn remove_rule(&self, rule_id: &str) {
        let mut rules = self.rules.write().await;
        rules.remove(rule_id);
    }

    pub async fn get_rules(&self) -> Vec<AlertRule> {
        self.rules.read().await.values().cloned().collect()
    }

    pub async fn get_rule(&self, rule_id: &str) -> Option<AlertRule> {
        self.rules.read().await.get(rule_id).cloned()
    }

    pub async fn get_alerts(&self) -> Vec<AlertInfo> {
        self.alerts.read().await.values().cloned().collect()
    }

    pub async fn get_alert(&self, alert_id: &str) -> Option<AlertInfo> {
        self.alerts.read().await.get(alert_id).cloned()
    }

    pub async fn acknowledge_alert(&self, alert_id: &str) {
        let mut alerts = self.alerts.write().await;
        if let Some(alert) = alerts.get_mut(alert_id) {
            alert.status = "resolved".to_string();
            alert.resolved_at = Some(chrono::Utc::now());
        }
    }

    pub async fn evaluate_rules(&self) -> Vec<AlertInfo> {
        let rules = self.rules.read().await;
        let mut new_alerts = Vec::new();
        let now = Instant::now();

        for rule in rules.values().filter(|r| r.enabled) {
            let condition = &rule.condition;
            let value = self.get_metric_value(&condition.metric).await;

            if self.evaluate_condition(value, &condition.operator, condition.value) {
                let pending_key = format!("{}-{}", rule.id, condition.metric);
                let mut pending = self.pending_alerts.write().await;

                match pending.get(&pending_key) {
                    Some(start_time) => {
                        if now.duration_since(*start_time).as_secs() >= condition.duration {
                            let alert = self.trigger_alert(rule).await;
                            new_alerts.push(alert);
                            pending.remove(&pending_key);
                        }
                    }
                    None => {
                        pending.insert(pending_key, now);
                    }
                }
            } else {
                let pending_key = format!("{}-{}", rule.id, condition.metric);
                let mut pending = self.pending_alerts.write().await;
                pending.remove(&pending_key);

                self.resolve_alert_if_exists(&rule.id).await;
            }
        }

        new_alerts
    }

    async fn get_metric_value(&self, metric_name: &str) -> f64 {
        match metric_name {
            "powerfs_node_cpu_usage" => {
                let nodes = self.metric_store.get_nodes().await;
                if nodes.is_empty() { 0.0 } else { nodes.iter().map(|n| n.cpu_usage).sum::<f64>() / nodes.len() as f64 }
            }
            "powerfs_node_mem_usage" => {
                let nodes = self.metric_store.get_nodes().await;
                if nodes.is_empty() { 0.0 } else { nodes.iter().map(|n| n.mem_usage).sum::<f64>() / nodes.len() as f64 }
            }
            "powerfs_node_disk_usage" => {
                let nodes = self.metric_store.get_nodes().await;
                if nodes.is_empty() { 0.0 } else { nodes.iter().map(|n| n.disk_usage).sum::<f64>() / nodes.len() as f64 }
            }
            "powerfs_kv_hit_ratio" => {
                self.metric_store.get_kv_metrics().await.hit_ratio
            }
            "powerfs_cluster_uptime" => {
                self.metric_store.get_cluster_metrics().await.uptime as f64
            }
            _ => 0.0,
        }
    }

    fn evaluate_condition(&self, value: f64, operator: &str, threshold: f64) -> bool {
        match operator {
            ">" => value > threshold,
            "<" => value < threshold,
            ">=" => value >= threshold,
            "<=" => value <= threshold,
            _ => false,
        }
    }

    async fn trigger_alert(&self, rule: &AlertRule) -> AlertInfo {
        let alert_id = uuid::Uuid::new_v4().to_string();
        let alert = AlertInfo {
            id: alert_id.clone(),
            name: rule.name.clone(),
            severity: rule.severity.clone(),
            status: "firing".to_string(),
            source: "monitor".to_string(),
            message: format!(
                "告警触发: {} - 指标 {} 满足条件 {}",
                rule.name, rule.condition.metric, rule.condition.operator
            ),
            created_at: chrono::Utc::now(),
            resolved_at: None,
        };

        let mut alerts = self.alerts.write().await;
        alerts.insert(alert_id, alert.clone());

        alert
    }

    async fn resolve_alert_if_exists(&self, rule_id: &str) {
        let mut alerts = self.alerts.write().await;
        for alert in alerts.values_mut() {
            if alert.status == "firing" {
                alert.status = "resolved".to_string();
                alert.resolved_at = Some(chrono::Utc::now());
            }
        }
    }

    pub async fn load_default_rules(&self) {
        let rules = vec![
            AlertRule {
                id: "rule-1".to_string(),
                name: "节点CPU使用率过高".to_string(),
                description: "当节点CPU使用率超过80%时触发告警".to_string(),
                enabled: true,
                severity: "warning".to_string(),
                condition: AlertCondition {
                    metric: "powerfs_node_cpu_usage".to_string(),
                    operator: ">".to_string(),
                    value: 80.0,
                    duration: 30,
                },
                notifications: Vec::new(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
            AlertRule {
                id: "rule-2".to_string(),
                name: "节点磁盘使用率过高".to_string(),
                description: "当节点磁盘使用率超过90%时触发告警".to_string(),
                enabled: true,
                severity: "critical".to_string(),
                condition: AlertCondition {
                    metric: "powerfs_node_disk_usage".to_string(),
                    operator: ">".to_string(),
                    value: 90.0,
                    duration: 60,
                },
                notifications: Vec::new(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
            AlertRule {
                id: "rule-3".to_string(),
                name: "KV命中率过低".to_string(),
                description: "当KV命中率低于50%时触发告警".to_string(),
                enabled: true,
                severity: "warning".to_string(),
                condition: AlertCondition {
                    metric: "powerfs_kv_hit_ratio".to_string(),
                    operator: "<".to_string(),
                    value: 50.0,
                    duration: 60,
                },
                notifications: Vec::new(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        ];

        let mut rules_map = self.rules.write().await;
        for rule in rules {
            rules_map.insert(rule.id.clone(), rule);
        }
    }
}

pub type AlertEngineRef = Arc<AlertEngine>;