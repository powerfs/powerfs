use tonic::transport::Channel;

pub use powerfs_master::proto::powerfs::master_service_client::MasterServiceClient;
pub use powerfs_master::proto::powerfs::raft_service_client::RaftServiceClient;
pub use powerfs_master::proto::powerfs::{
    AddNodeRequest, AddNodeResponse, RemoveNodeRequest, RemoveNodeResponse, TransferLeaderRequest,
    TransferLeaderResponse,
};

pub struct MasterClient {
    channel: Option<Channel>,
    pub address: String,
}

impl MasterClient {
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
    ) -> Result<MasterServiceClient<Channel>, Box<dyn std::error::Error>> {
        let channel = self.channel().await?;
        Ok(MasterServiceClient::new(channel))
    }

    pub async fn raft_service(
        &mut self,
    ) -> Result<RaftServiceClient<Channel>, Box<dyn std::error::Error>> {
        let channel = self.channel().await?;
        Ok(RaftServiceClient::new(channel))
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
