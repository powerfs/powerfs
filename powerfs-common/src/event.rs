use chrono::{DateTime, Utc};
use redis::{AsyncCommands, Client, RedisResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type")]
pub enum Event {
    #[serde(rename = "node_status")]
    NodeStatus(NodeStatusEvent),
    #[serde(rename = "volume_status")]
    VolumeStatus(VolumeStatusEvent),
    #[serde(rename = "kv_session")]
    KVSession(KVSessionEvent),
    #[serde(rename = "kv_block")]
    KVBlock(KVBlockEvent),
    #[serde(rename = "metric_update")]
    MetricUpdate(MetricUpdateEvent),
    #[serde(rename = "alert_trigger")]
    AlertTrigger(AlertTriggerEvent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: String,
    #[serde(flatten)]
    pub event: Event,
    pub source: String,
    pub source_id: String,
    pub timestamp: DateTime<Utc>,
    pub version: String,
}

impl EventEnvelope {
    pub fn new(event: Event, source: &str, source_id: &str) -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            event,
            source: source.to_string(),
            source_id: source_id.to_string(),
            timestamp: Utc::now(),
            version: "1.0".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatusEvent {
    pub node_id: String,
    pub address: String,
    pub grpc_port: u32,
    pub http_port: u32,
    pub status: String,
    pub cpu_usage: f64,
    pub mem_usage: f64,
    pub disk_usage: f64,
    pub network_rx: u64,
    pub network_tx: u64,
    pub uptime: u64,
    pub volume_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeStatusEvent {
    pub volume_id: u32,
    pub node_id: String,
    pub size: u64,
    pub used: u64,
    pub file_count: u64,
    pub status: String,
    pub collection: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KVSessionEvent {
    pub session_id: String,
    pub model_name: String,
    pub layer_count: u32,
    pub block_count: u64,
    pub memory_used: u64,
    pub hit_ratio: f64,
    pub eviction_count: u64,
    pub event_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KVBlockEvent {
    pub block_id: u64,
    pub session_id: String,
    pub layer_id: u32,
    pub event_type: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricUpdateEvent {
    pub metric_name: String,
    pub metric_type: String,
    pub value: f64,
    pub labels: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertTriggerEvent {
    pub alert_id: String,
    pub rule_id: String,
    pub name: String,
    pub severity: String,
    pub status: String,
    pub message: String,
    pub source: String,
}

#[derive(Clone)]
pub struct EventPublisher {
    client: redis::Client,
    stream_key: String,
    source: String,
}

impl EventPublisher {
    pub fn new(redis_url: &str, stream_key: &str, source: &str) -> Self {
        let client = Client::open(redis_url).expect("Failed to create Redis client");
        Self {
            client,
            stream_key: stream_key.to_string(),
            source: source.to_string(),
        }
    }

    pub async fn publish(&self, event: Event, source_id: &str) -> RedisResult<()> {
        let envelope = EventEnvelope::new(event, &self.source, source_id);
        let mut conn = self.client.get_async_connection().await?;
        let payload: Vec<(String, String)> = vec![
            ("event_id".to_string(), envelope.event_id.clone()),
            ("source".to_string(), envelope.source.clone()),
            ("source_id".to_string(), envelope.source_id.clone()),
            ("timestamp".to_string(), envelope.timestamp.to_rfc3339()),
            ("version".to_string(), envelope.version.clone()),
            ("payload".to_string(), serde_json::to_string(&envelope.event).unwrap()),
        ];
        let _: () = conn.xadd(&self.stream_key, "*", &payload).await?;
        Ok(())
    }
}