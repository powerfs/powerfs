use crate::proto::powerfs::{
    master_service_client::MasterServiceClient, AssignRequest, LookupVolumeRequest,
};
use chrono::Utc;
use powerfs_common::{
    error::{PowerFsError, Result},
    types::{
        Collection, DataCenterId, DataNodeInfo, DiskType, Fid, NodeId, NodeState, RackId, Ttl,
        VolumeId, VolumeInfo, VolumeState,
    },
};
use std::sync::Arc;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct S3MasterClient {
    master_address: String,
    channel: Arc<tokio::sync::Mutex<Option<Channel>>>,
}

impl S3MasterClient {
    pub fn new(master_address: &str) -> Self {
        Self {
            master_address: master_address.to_string(),
            channel: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    async fn get_client(&self) -> Result<MasterServiceClient<Channel>> {
        let mut channel_guard = self.channel.lock().await;
        let channel = if let Some(ch) = &*channel_guard {
            ch.clone()
        } else {
            let addr = format!("http://{}", self.master_address);
            let ch = Channel::from_shared(addr)
                .map_err(|e| PowerFsError::Internal(format!("Invalid address: {}", e)))?
                .connect()
                .await
                .map_err(|e| {
                    PowerFsError::Internal(format!("Failed to connect to master: {}", e))
                })?;
            *channel_guard = Some(ch.clone());
            ch
        };
        Ok(MasterServiceClient::new(channel))
    }

    pub async fn assign_volume(
        &self,
        replication: &str,
        collection: &str,
    ) -> Result<(Fid, Vec<DataNodeInfo>)> {
        let mut client = self.get_client().await?;
        let request = AssignRequest {
            count: 1,
            replication: replication.to_string(),
            collection: collection.to_string(),
            ttl: String::new(),
            data_center: String::new(),
            rack: String::new(),
            data_node: String::new(),
            disk_type: String::new(),
            stripe_count: 0,
            stripe_size: 0,
        };
        let response = client
            .assign(tonic::Request::new(request))
            .await
            .map_err(|e| PowerFsError::Internal(format!("Assign failed: {}", e)))?;
        let response = response.into_inner();
        if !response.error.is_empty() {
            return Err(PowerFsError::Internal(response.error));
        }
        let fid = Fid::from_string(&response.fid)
            .map_err(|e| PowerFsError::Internal(format!("Invalid fid format: {}", e)))?;
        let nodes: Vec<DataNodeInfo> = response
            .replicas
            .into_iter()
            .map(|loc| {
                let mut addr = loc.url.strip_prefix("http://").unwrap_or(&loc.url);
                addr = addr.strip_prefix("https://").unwrap_or(addr);
                addr = addr.split('/').next().unwrap_or(addr);
                let ip: String = if let Some(colon_idx) = addr.rfind(':') {
                    addr[..colon_idx].to_string()
                } else {
                    addr.to_string()
                };
                DataNodeInfo {
                    id: NodeId(loc.url.clone()),
                    address: ip,
                    rack_id: RackId(String::new()),
                    data_center_id: DataCenterId(loc.data_center),
                    total_space: 0,
                    used_space: 0,
                    volume_count: 0,
                    state: NodeState::Healthy,
                    last_heartbeat: Utc::now(),
                    grpc_port: loc.grpc_port,
                    http_port: 8080,
                    public_url: loc.public_url,
                    maintenance_mode: false,
                }
            })
            .collect();
        Ok((fid, nodes))
    }

    pub async fn get_volume_info(&self, volume_id: &VolumeId) -> Option<VolumeInfo> {
        let mut client = match self.get_client().await {
            Ok(c) => c,
            Err(_) => return None,
        };
        let request = LookupVolumeRequest {
            volume_or_file_ids: vec![volume_id.0.to_string()],
            collection: String::new(),
        };
        match client.lookup_volume(tonic::Request::new(request)).await {
            Ok(response) => {
                let response = response.into_inner();
                for vol_loc in response.volume_id_locations {
                    if !vol_loc.error.is_empty() {
                        continue;
                    }
                    if let Some(loc) = vol_loc.locations.first() {
                        return Some(VolumeInfo {
                            id: *volume_id,
                            node_id: NodeId(loc.url.clone()),
                            collection: Collection(String::new()),
                            size: 0,
                            used: 0,
                            replica_count: vol_loc.locations.len() as u32,
                            ttl: Ttl::default(),
                            disk_type: DiskType::default(),
                            state: VolumeState::Available,
                            created_at: Utc::now(),
                            modified_at: Utc::now(),
                            next_file_key: 0,
                        });
                    }
                }
                None
            }
            Err(_) => None,
        }
    }

    pub fn get_node_info(&self, node_id: &str) -> Option<DataNodeInfo> {
        let mut addr = node_id.strip_prefix("http://").unwrap_or(node_id);
        addr = addr.strip_prefix("https://").unwrap_or(addr);
        addr = addr.split('/').next().unwrap_or(addr);
        let ip: String = if let Some(colon_idx) = addr.rfind(':') {
            addr[..colon_idx].to_string()
        } else {
            addr.to_string()
        };
        let grpc_port = if ip.starts_with("172.20.0.2") {
            8080
        } else {
            9333
        };
        Some(DataNodeInfo {
            id: NodeId(node_id.to_string()),
            address: ip,
            rack_id: RackId(String::new()),
            data_center_id: DataCenterId(String::new()),
            total_space: 0,
            used_space: 0,
            volume_count: 0,
            state: NodeState::Healthy,
            last_heartbeat: Utc::now(),
            grpc_port,
            http_port: 8080,
            public_url: String::new(),
            maintenance_mode: false,
        })
    }
}
