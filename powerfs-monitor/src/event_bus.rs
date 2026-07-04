use std::sync::Arc;

use redis::{AsyncCommands, Client, RedisResult};

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

    pub async fn publish(&self, event: EventEnvelope) -> RedisResult<()> {
        let mut conn = self.client.get_async_connection().await?;
        let payload = serde_json::to_string(&event).unwrap();
        let _: () = conn.lpush(&self.stream_key, &payload).await?;
        Ok(())
    }

    pub async fn subscribe(&self) -> EventStream {
        EventStream {
            client: self.client.clone(),
            stream_key: self.stream_key.clone(),
        }
    }
}

pub struct EventStream {
    client: Arc<Client>,
    stream_key: String,
}

impl EventStream {
    pub async fn read(&self) -> RedisResult<Vec<EventEnvelope>> {
        let mut conn = self.client.get_async_connection().await?;
        let payload: Option<String> = conn.brpoplpush(&self.stream_key, &self.stream_key, 1).await?;
        
        match payload {
            Some(p) => {
                if let Ok(event) = serde_json::from_str(&p) {
                    Ok(vec![event])
                } else {
                    Ok(vec![])
                }
            }
            None => Ok(vec![]),
        }
    }

    pub async fn ack(&self, _event_id: &str) -> RedisResult<()> {
        Ok(())
    }
}