use tonic::transport::Channel;

pub use powerfs_master::proto::powerfs::kv_cache_service_client::KvCacheServiceClient;
pub use powerfs_master::proto::powerfs::{
    CreateSessionRequest, DeleteSessionRequest, GetBlockRequest, GetSessionRequest,
    GetStatsRequest, ListSessionsRequest, PutBlockRequest,
};

pub struct KvCacheClient {
    channel: Option<Channel>,
    pub address: String,
}

impl KvCacheClient {
    pub fn new(address: &str) -> Self {
        Self {
            channel: None,
            address: address.to_string(),
        }
    }

    pub async fn connect(&mut self) -> Result<Channel, Box<dyn std::error::Error>> {
        let addr = format!("http://{}", self.address);
        let channel = Channel::from_shared(addr)
            .map_err(|e| format!("Invalid URI: {}", e))?
            .connect()
            .await
            .map_err(|e| format!("Connection failed: {}", e))?;
        self.channel = Some(channel.clone());
        Ok(channel)
    }

    pub async fn channel(&mut self) -> Result<Channel, Box<dyn std::error::Error>> {
        if let Some(ch) = &self.channel {
            Ok(ch.clone())
        } else {
            self.connect().await
        }
    }

    pub async fn service(
        &mut self,
    ) -> Result<KvCacheServiceClient<Channel>, Box<dyn std::error::Error>> {
        let channel = self.channel().await?;
        Ok(KvCacheServiceClient::new(channel))
    }
}

impl Clone for KvCacheClient {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            address: self.address.clone(),
        }
    }
}
