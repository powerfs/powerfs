use tonic::transport::Channel;

/// Re-export master proto types and client
pub use powerfs_master::proto::powerfs::master_service_client::MasterServiceClient;

/// Master gRPC client wrapper
pub struct MasterClient {
    channel: Option<Channel>,
    pub address: String,  // Public for status display
}

impl MasterClient {
    /// Create a new client connecting to the master
    pub fn new(address: &str) -> Self {
        Self {
            channel: None,
            address: address.to_string(),
        }
    }

    /// Connect to the master (lazy connection)
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

    /// Get or create channel
    pub async fn channel(&mut self) -> Result<Channel, Box<dyn std::error::Error>> {
        if let Some(ch) = &self.channel {
            Ok(ch.clone())
        } else {
            self.connect().await
        }
    }

    /// Create the gRPC client
    pub async fn service(&mut self) -> Result<MasterServiceClient<Channel>, Box<dyn std::error::Error>> {
        let channel = self.channel().await?;
        Ok(MasterServiceClient::new(channel))
    }
}

impl Clone for MasterClient {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            address: self.address.clone(),
        }
    }
}