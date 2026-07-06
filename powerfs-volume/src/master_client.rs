use log::{debug, error, warn};
use powerfs_common::types::NodeId;
use powerfs_master::proto::{
    powerfs::master_service_client::MasterServiceClient, Heartbeat, VolumeGrowRequest,
    VolumeGrowResponse, VolumeShortInfo,
};
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct MasterClient {
    master_address: String,
    node_id: NodeId,
    grpc_port: u32,
    http_port: u32,
    data_center: String,
    rack: String,
    public_url: String,
    ip: String,
    sender: Option<mpsc::Sender<Heartbeat>>,
}

#[derive(Clone)]
pub struct NewMasterClientParams<'a> {
    pub master_address: &'a str,
    pub node_id: NodeId,
    pub grpc_port: u32,
    pub http_port: u32,
    pub data_center: &'a str,
    pub rack: &'a str,
    pub public_url: &'a str,
    pub ip: &'a str,
}

impl MasterClient {
    pub fn new(params: NewMasterClientParams<'_>) -> Self {
        MasterClient {
            master_address: params.master_address.to_string(),
            node_id: params.node_id,
            grpc_port: params.grpc_port,
            http_port: params.http_port,
            data_center: params.data_center.to_string(),
            rack: params.rack.to_string(),
            public_url: params.public_url.to_string(),
            ip: params.ip.to_string(),
            sender: None,
        }
    }

    pub async fn start_heartbeat(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let address = format!("http://{}", self.master_address);
        let channel = Channel::from_shared(address)?.connect().await?;
        let mut client = MasterServiceClient::new(channel);

        let (tx, rx) = mpsc::channel(10);
        self.sender = Some(tx);

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let response_stream = client.send_heartbeat(tonic::Request::new(stream)).await?;
        let mut responses = response_stream.into_inner();

        tokio::spawn(async move {
            while let Some(response) = responses.next().await {
                match response {
                    Ok(resp) => {
                        debug!(
                            "Heartbeat response: leader={}, volume_size_limit={}",
                            resp.leader, resp.volume_size_limit
                        );
                    }
                    Err(e) => {
                        warn!("Heartbeat error: {}", e);
                        break;
                    }
                }
            }
            error!("Heartbeat stream ended");
        });

        Ok(())
    }

    pub async fn send_heartbeat(
        &self,
        volumes: Vec<VolumeShortInfo>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(sender) = &self.sender {
            let heartbeat = Heartbeat {
                ip: self.ip.clone(),
                port: self.http_port,
                public_url: self.public_url.clone(),
                max_file_key: 0,
                data_center: self.data_center.clone(),
                rack: self.rack.clone(),
                admin_port: 0,
                volumes: volumes.clone(),
                new_volumes: Vec::new(),
                deleted_volumes: Vec::new(),
                has_no_volumes: volumes.is_empty(),
                grpc_port: self.grpc_port,
                id: self.node_id.0.clone(),
            };

            sender.send(heartbeat).await.map_err(|e| e.into())
        } else {
            Err("heartbeat not started".into())
        }
    }

    pub async fn grow(
        &self,
        replication: &str,
        collection: &str,
        count: u32,
    ) -> Result<VolumeGrowResponse, Box<dyn std::error::Error>> {
        let address = format!("http://{}", self.master_address);
        let channel = Channel::from_shared(address)?.connect().await?;
        let mut client = MasterServiceClient::new(channel);

        let request = VolumeGrowRequest {
            replication: replication.to_string(),
            collection: collection.to_string(),
            ttl: String::new(),
            data_center: self.data_center.clone(),
            rack: self.rack.clone(),
            data_node: self.node_id.0.clone(),
            disk_type: String::new(),
            count,
        };

        let response = client.volume_grow(tonic::Request::new(request)).await?;
        Ok(response.into_inner())
    }
}
