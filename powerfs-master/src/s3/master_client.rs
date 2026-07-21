use crate::proto::powerfs::{
    master_service_client::MasterServiceClient, AssignRequest, LookupVolumeRequest,
};
use chrono::Utc;
use log::info;
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
    master_address: Arc<tokio::sync::Mutex<String>>,
    channel: Arc<tokio::sync::Mutex<Option<Channel>>>,
}

impl S3MasterClient {
    pub fn new(master_address: &str) -> Self {
        Self {
            master_address: Arc::new(tokio::sync::Mutex::new(master_address.to_string())),
            channel: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    async fn get_client(&self) -> Result<MasterServiceClient<Channel>> {
        let mut channel_guard = self.channel.lock().await;
        let channel = if let Some(ch) = &*channel_guard {
            ch.clone()
        } else {
            let addr = format!("http://{}", self.master_address.lock().await);
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

        let response = match self.try_assign_with_retry(request).await {
            Ok(r) => r,
            Err(e) => return Err(e),
        };

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
                    soft_error_type: None,
                    degrade_type: None,
                    degrade_severity: 0,
                    state_since: 0,
                }
            })
            .collect();
        Ok((fid, nodes))
    }

    async fn try_assign_with_retry(
        &self,
        request: AssignRequest,
    ) -> Result<crate::proto::powerfs::AssignResponse> {
        let mut client = self.get_client().await?;
        let response = client.assign(tonic::Request::new(request.clone())).await;

        match response {
            Ok(r) => {
                let inner = r.into_inner();
                if inner.error.contains("not leader") {
                    if let Some(leader_addr) = extract_leader_addr(&inner.error) {
                        info!("Detected leader change, switching to: {}", leader_addr);
                        self.update_master_address(&leader_addr).await;
                        let mut client = self.get_client().await?;
                        let response = client
                            .assign(tonic::Request::new(request))
                            .await
                            .map_err(|e| PowerFsError::Internal(format!("Assign failed: {}", e)))?;
                        let inner = response.into_inner();
                        if !inner.error.is_empty() {
                            return Err(PowerFsError::Internal(inner.error));
                        }
                        Ok(inner)
                    } else {
                        Err(PowerFsError::Internal(inner.error))
                    }
                } else if !inner.error.is_empty() {
                    Err(PowerFsError::Internal(inner.error))
                } else {
                    Ok(inner)
                }
            }
            Err(e) => {
                let error_str = format!("{}", e);
                if error_str.contains("not leader") {
                    if let Some(leader_addr) = extract_leader_addr(&error_str) {
                        info!("Detected leader change, switching to: {}", leader_addr);
                        self.update_master_address(&leader_addr).await;
                        let mut client = self.get_client().await?;
                        let response = client
                            .assign(tonic::Request::new(request))
                            .await
                            .map_err(|e| PowerFsError::Internal(format!("Assign failed: {}", e)))?;
                        let inner = response.into_inner();
                        if !inner.error.is_empty() {
                            return Err(PowerFsError::Internal(inner.error));
                        }
                        Ok(inner)
                    } else {
                        Err(PowerFsError::Internal(format!("Assign failed: {}", e)))
                    }
                } else {
                    Err(PowerFsError::Internal(format!("Assign failed: {}", e)))
                }
            }
        }
    }

    async fn update_master_address(&self, new_addr: &str) {
        let addr = new_addr.strip_prefix("http://").unwrap_or(new_addr);
        let addr = addr.strip_prefix("https://").unwrap_or(addr);
        *self.channel.lock().await = None;
        *self.master_address.lock().await = addr.to_string();
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
            soft_error_type: None,
            degrade_type: None,
            degrade_severity: 0,
            state_since: 0,
        })
    }
}

fn extract_leader_addr(error_msg: &str) -> Option<String> {
    let patterns = [
        "current leader is ",
        "leader is ",
        "leader: ",
        "redirect to ",
    ];
    for pattern in patterns {
        if let Some(start) = error_msg.find(pattern) {
            let start = start + pattern.len();
            let end = error_msg[start..]
                .find(|c| [',', '\n', ')'].contains(&c))
                .map(|e| start + e)
                .unwrap_or(error_msg.len());
            // Strip surrounding quotes (tonic wraps the message in quotes) and
            // trim whitespace. An empty/quoted-only value means the leader is
            // unknown — return None so the caller does not switch to a bogus address.
            let addr = error_msg[start..end]
                .trim()
                .trim_matches('"')
                .trim()
                .to_string();
            // Validate that the address looks like host:port (contains ':' and
            // no spaces) before accepting it.
            if !addr.is_empty() && addr.contains(':') && !addr.contains(' ') {
                return Some(addr);
            }
        }
    }
    None
}
