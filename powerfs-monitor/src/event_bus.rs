use std::sync::Arc;

use redis::streams::{StreamRangeReply, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client, RedisResult, Value};

use crate::event::EventEnvelope;

pub struct EventBus {
    client: Arc<Client>,
    stream_key: String,
}

impl EventBus {
    pub fn new(redis_url: &str, stream_key: &str) -> Self {
        let client = Client::open(redis_url).expect("Failed to create Redis client");
        Self {
            client: Arc::new(client),
            stream_key: stream_key.to_string(),
        }
    }

    pub async fn subscribe(&self) -> EventStream {
        EventStream {
            client: self.client.clone(),
            stream_key: self.stream_key.clone(),
            last_id: "0".to_string(),
        }
    }

    pub async fn read_history(&self) -> RedisResult<Vec<EventEnvelope>> {
        let mut conn = self.client.get_async_connection().await?;

        let reply: StreamRangeReply = conn.xrange(&self.stream_key, "-", "+").await?;

        let mut events = Vec::new();

        for entry in reply.ids {
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

pub struct EventStream {
    client: Arc<Client>,
    stream_key: String,
    last_id: String,
}

impl EventStream {
    pub async fn read(&mut self) -> RedisResult<Vec<EventEnvelope>> {
        let mut conn = self.client.get_async_connection().await?;

        let opts = StreamReadOptions::default().count(10);

        let reply: StreamReadReply = conn
            .xread_options(&[&self.stream_key], &[&self.last_id], &opts)
            .await?;

        let mut events = Vec::new();

        for stream in reply.keys {
            for entry in stream.ids {
                self.last_id = entry.id.clone();

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
        }

        Ok(events)
    }
}
