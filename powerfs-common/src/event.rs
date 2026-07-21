use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::traits::{EventProvider, EventStream};

#[cfg(feature = "redis-event")]
use redis::streams::StreamRangeReply;
#[cfg(feature = "redis-event")]
use redis::{AsyncCommands, Client, RedisResult, Value};

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
    #[serde(default = "default_node_type")]
    pub node_type: String,
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
    #[serde(default)]
    pub is_leader: bool,
    #[serde(default)]
    pub raft_term: u64,
}

fn default_node_type() -> String {
    "volume".to_string()
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

#[cfg(feature = "redis-event")]
#[derive(Clone)]
pub struct EventPublisher {
    client: redis::Client,
    stream_key: String,
    source: String,
}

#[cfg(feature = "redis-event")]
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
            (
                "payload".to_string(),
                serde_json::to_string(&envelope.event).unwrap(),
            ),
        ];
        let _: () = conn.xadd(&self.stream_key, "*", &payload).await?;
        Ok(())
    }
}

#[cfg(feature = "redis-event")]
pub struct RedisEventProvider {
    client: Arc<Client>,
    stream_key: String,
    source: String,
}

#[cfg(feature = "redis-event")]
impl RedisEventProvider {
    pub fn new(redis_url: &str, stream_key: &str, source: &str) -> Self {
        let client = Client::open(redis_url).expect("Failed to create Redis client");
        Self {
            client: Arc::new(client),
            stream_key: stream_key.to_string(),
            source: source.to_string(),
        }
    }
}

#[cfg(feature = "redis-event")]
#[async_trait]
impl EventProvider for RedisEventProvider {
    async fn publish(&self, event: Event, source_id: &str) -> crate::error::Result<()> {
        let envelope = EventEnvelope::new(event, &self.source, source_id);
        let mut conn = self.client.get_async_connection().await.map_err(|e| {
            crate::error::PowerFsError::Internal(format!("redis connection error: {}", e))
        })?;
        let payload: Vec<(String, String)> = vec![
            ("event_id".to_string(), envelope.event_id.clone()),
            ("source".to_string(), envelope.source.clone()),
            ("source_id".to_string(), envelope.source_id.clone()),
            ("timestamp".to_string(), envelope.timestamp.to_rfc3339()),
            ("version".to_string(), envelope.version.clone()),
            (
                "payload".to_string(),
                serde_json::to_string(&envelope.event).unwrap(),
            ),
        ];
        let _: () = conn
            .xadd(&self.stream_key, "*", &payload)
            .await
            .map_err(|e| {
                crate::error::PowerFsError::Internal(format!("redis publish error: {}", e))
            })?;
        Ok(())
    }

    async fn subscribe(&self, stream_key: &str) -> crate::error::Result<EventStream> {
        let (sender, receiver) = mpsc::channel(100);
        let client = self.client.clone();
        let stream_key_clone = stream_key.to_string();

        tokio::spawn(async move {
            let mut last_id = "0".to_string();
            let opts = redis::streams::StreamReadOptions::default().count(10);

            loop {
                let mut conn = match client.get_async_connection().await {
                    Ok(c) => c,
                    Err(_) => {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        continue;
                    }
                };

                let reply: redis::streams::StreamReadReply = match conn
                    .xread_options(&[&stream_key_clone], &[&last_id], &opts)
                    .await
                {
                    Ok(r) => r,
                    Err(_) => {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        continue;
                    }
                };

                for stream in reply.keys {
                    for entry in stream.ids {
                        last_id = entry.id.clone();

                        let mut event_id = String::new();
                        let mut source = String::new();
                        let mut source_id = String::new();
                        let mut payload_str = String::new();

                        for (key, value) in entry.map {
                            let value_str = match value {
                                Value::Data(data) => String::from_utf8_lossy(&data).to_string(),
                                Value::Status(s) => s,
                                _ => continue,
                            };
                            match key.as_str() {
                                "event_id" => event_id = value_str,
                                "source" => source = value_str,
                                "source_id" => source_id = value_str,
                                "payload" => payload_str = value_str,
                                _ => {}
                            }
                        }

                        if !payload_str.is_empty() {
                            if let Ok(event) = serde_json::from_str(&payload_str) {
                                let envelope = EventEnvelope {
                                    event_id,
                                    source,
                                    source_id,
                                    timestamp: chrono::Utc::now(),
                                    version: "1.0".to_string(),
                                    event,
                                };
                                let _ = sender.send(envelope).await;
                            }
                        }
                    }
                }
            }
        });

        Ok(EventStream { receiver })
    }

    async fn read_history(
        &self,
        stream_key: &str,
        start: &str,
        count: usize,
    ) -> crate::error::Result<Vec<EventEnvelope>> {
        let mut conn = self.client.get_async_connection().await.map_err(|e| {
            crate::error::PowerFsError::Internal(format!("redis connection error: {}", e))
        })?;

        let reply: StreamRangeReply = conn.xrange(stream_key, start, "+").await.map_err(|e| {
            crate::error::PowerFsError::Internal(format!("redis read error: {}", e))
        })?;

        let mut events = Vec::new();

        for entry in reply.ids.into_iter().take(count) {
            let mut event_id = String::new();
            let mut source = String::new();
            let mut source_id = String::new();
            let mut payload_str = String::new();

            for (key, value) in entry.map {
                let value_str = match value {
                    Value::Data(data) => String::from_utf8_lossy(&data).to_string(),
                    Value::Status(s) => s,
                    _ => continue,
                };
                match key.as_str() {
                    "event_id" => event_id = value_str,
                    "source" => source = value_str,
                    "source_id" => source_id = value_str,
                    "payload" => payload_str = value_str,
                    _ => {}
                }
            }

            if !payload_str.is_empty() {
                if let Ok(event) = serde_json::from_str(&payload_str) {
                    events.push(EventEnvelope {
                        event_id,
                        source,
                        source_id,
                        timestamp: chrono::Utc::now(),
                        version: "1.0".to_string(),
                        event,
                    });
                }
            }
        }

        Ok(events)
    }
}

pub struct NullEventProvider;

#[async_trait]
impl EventProvider for NullEventProvider {
    async fn publish(&self, _event: Event, _source_id: &str) -> crate::error::Result<()> {
        Ok(())
    }

    async fn subscribe(&self, _stream_key: &str) -> crate::error::Result<EventStream> {
        let (_, receiver) = mpsc::channel(100);
        Ok(EventStream { receiver })
    }

    async fn read_history(
        &self,
        _stream_key: &str,
        _start: &str,
        _count: usize,
    ) -> crate::error::Result<Vec<EventEnvelope>> {
        Ok(Vec::new())
    }
}
